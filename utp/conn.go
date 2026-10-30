package utp

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

const (
	// maxPayload keeps every packet under the smallest common path MTU
	// (1280 bytes for IPv6), so uTP never relies on IP fragmentation.
	maxPayload  = 1200
	minWindow   = maxPayload
	maxWindow   = 1 << 20 // our congestion window never grows past 1 MiB
	recvBufSize = 1 << 20 // what we advertise and buffer per connection
	maxOutQueue = 2048    // packets in flight; far below half the 16-bit sequence space

	// LEDBAT: aim for 100 ms of extra queuing delay, and grow the window by
	// at most 3000 bytes per round trip when there is room (BEP 29).
	targetDelay   = 100 * time.Millisecond
	maxGainPerRTT = 3000

	initialRTO     = time.Second
	minRTO         = 500 * time.Millisecond
	maxRTO         = 8 * time.Second
	maxRetransmits = 7
	keepAlive      = 29 * time.Second
	closeLinger    = 5 * time.Second // how long Close waits for our FIN to be acked
	tick           = 25 * time.Millisecond
)

const (
	stateSynSent = iota
	stateSynRecv
	stateConnected
	stateClosed
)

var (
	errReset    = errors.New("uTP connection reset by peer")
	errTimedOut = timeoutError("uTP connection timed out (no acks)")
	errDeadline = os.ErrDeadlineExceeded // a net.Error with Timeout() == true
)

type timeoutError string

func (e timeoutError) Error() string   { return string(e) }
func (e timeoutError) Timeout() bool   { return true }
func (e timeoutError) Temporary() bool { return true }

type outPacket struct {
	typ           uint8
	seq           uint16
	payload       []byte
	sentAt        time.Time
	transmissions int
	sacked        bool // the receiver reported it in a selective ack
}

type inPacket struct {
	typ     uint8
	payload []byte
}

// Conn is one uTP connection. It implements net.Conn.
type Conn struct {
	s              *Socket
	raddr          net.Addr
	recvID, sendID uint16 // IDs in packets we receive / send

	established chan struct{}
	dead        chan struct{}
	onceEst     sync.Once
	onceDead    sync.Once

	mu          sync.Mutex
	cond        *sync.Cond
	state       int
	err         error // why the connection ended
	localClosed bool
	finSent     bool
	closedAt    time.Time

	seq uint16 // next sequence number we send
	ack uint16 // last sequence number received in order

	// Sending.
	outq     []*outPacket // sent and not yet acked, oldest first
	inFlight int          // payload bytes in outq not acked yet
	cwnd     float64      // congestion window (LEDBAT)
	ssthresh float64
	peerWnd  int
	rtt      time.Duration
	rttVar   time.Duration
	rto      time.Duration
	dupAcks  int
	lastAck  uint16
	lastLoss time.Time
	delays   delayHistory

	// Receiving.
	inbuf        []byte
	reorder      map[uint16]inPacket
	reorderBytes int
	eof          bool // FIN received and everything before it delivered
	lastWnd      int  // window we last advertised

	replyMicro    uint32 // our clock minus the peer's timestamp on its last packet
	lastSend      time.Time
	readDeadline  time.Time
	writeDeadline time.Time
}

func newConn(s *Socket, raddr net.Addr, recvID, sendID uint16) *Conn {
	c := &Conn{
		s: s, raddr: raddr, recvID: recvID, sendID: sendID,
		established: make(chan struct{}),
		dead:        make(chan struct{}),
		cwnd:        2 * maxPayload,
		ssthresh:    maxWindow,
		peerWnd:     recvBufSize,
		rto:         initialRTO,
		reorder:     map[uint16]inPacket{},
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func now() uint32 { return uint32(time.Now().UnixMicro()) }

// setPeer records the timing and window fields of a packet from the peer.
func (c *Conn) setPeer(h *header) {
	if h.ts != 0 {
		c.replyMicro = now() - h.ts
	}
	c.peerWnd = int(h.wnd)
}

func (c *Conn) recvWindow() int {
	w := recvBufSize - len(c.inbuf) - c.reorderBytes
	if w < 0 {
		return 0
	}
	return w
}

// queue assigns the next sequence number to a packet, keeps it until it is
// acked, and sends it.
func (c *Conn) queue(typ uint8, payload []byte) {
	p := &outPacket{typ: typ, seq: c.seq, payload: payload}
	c.seq++
	c.outq = append(c.outq, p)
	c.inFlight += len(payload)
	c.transmit(p)
}

func (c *Conn) transmit(p *outPacket) {
	id := c.sendID
	if p.typ == stSyn {
		id = c.recvID // the SYN carries the ID we want to receive on
	}
	c.lastWnd = c.recvWindow()
	h := &header{typ: p.typ, connID: id, ts: now(), tsDiff: c.replyMicro, wnd: uint32(c.lastWnd), seq: p.seq, ack: c.ack}
	p.sentAt = time.Now()
	p.transmissions++
	c.lastSend = p.sentAt
	c.s.send(h.marshal(p.payload), c.raddr)
}

// sendState sends an ack. STATE packets carry the next sequence number but
// don't use it up, and aren't acked themselves.
func (c *Conn) sendState() {
	c.lastWnd = c.recvWindow()
	h := &header{typ: stState, connID: c.sendID, ts: now(), tsDiff: c.replyMicro,
		wnd: uint32(c.lastWnd), seq: c.seq, ack: c.ack, sack: c.sackMask()}
	c.lastSend = time.Now()
	c.s.send(h.marshal(nil), c.raddr)
}

// sackMask reports out-of-order packets we hold: bit j of byte i stands for
// ack_nr + 2 + 8i + j (least significant bit first). The mask is a multiple
// of 4 bytes, as BEP 29 requires, and covers up to 256 packets.
func (c *Conn) sackMask() []byte {
	if len(c.reorder) == 0 {
		return nil
	}
	const maxBits = 256
	mask := make([]byte, maxBits/8)
	used := 0
	for i := 0; i < maxBits; i++ {
		if _, ok := c.reorder[c.ack+2+uint16(i)]; ok {
			mask[i/8] |= 1 << (i % 8)
			used = i/8 + 1
		}
	}
	if used == 0 {
		return nil
	}
	return mask[:(used+3)/4*4]
}

// handle processes one packet from the peer (called by the socket).
func (c *Conn) handle(h *header, payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.cond.Broadcast()
	if c.state == stateClosed {
		return
	}
	c.setPeer(h)

	switch h.typ {
	case stReset:
		c.fail(errReset)
		return
	case stSyn:
		if c.state == stateSynRecv {
			c.sendState() // the peer didn't get our reply
		}
		return
	}
	if c.state == stateSynSent {
		if h.typ != stState {
			return
		}
		c.ack = h.seq - 1 // the reply carries the seq_nr of its first data packet
		c.connected()
	}
	c.processAck(h, len(payload))

	if h.typ == stData || h.typ == stFin {
		if c.state == stateSynRecv {
			c.connected()
		}
		c.receive(h, payload)
		c.sendState()
		if c.localClosed && c.eof && c.finSent && len(c.outq) == 0 {
			c.finish()
		}
	}
}

func (c *Conn) connected() {
	c.state = stateConnected
	c.onceEst.Do(func() { close(c.established) })
}

// processAck drops everything the peer has acked, measures the round trip,
// feeds LEDBAT and triggers fast retransmit on three duplicate acks.
func (c *Conn) processAck(h *header, payloadLen int) {
	defer func() { c.lastAck = h.ack }()
	if len(c.outq) == 0 {
		return
	}
	first, last := c.outq[0].seq, c.seq-1
	if seqLess(h.ack, first-1) || seqLess(last, h.ack) {
		return // stale or bogus ack
	}
	t := time.Now()
	acked := 0
	progressed := false
	for len(c.outq) > 0 && !seqLess(h.ack, c.outq[0].seq) {
		p := c.outq[0]
		c.outq[0] = nil
		c.outq = c.outq[1:]
		progressed = true
		if !p.sacked {
			acked += len(p.payload)
			c.inFlight -= len(p.payload)
		}
		if p.transmissions == 1 { // Karn: retransmitted packets give ambiguous samples
			c.updateRTT(t.Sub(p.sentAt))
		}
	}
	for i, b := range h.sack {
		for j := 0; j < 8; j++ {
			if b>>j&1 == 0 {
				continue
			}
			seq := h.ack + 2 + uint16(8*i+j)
			for _, p := range c.outq {
				if p.seq == seq && !p.sacked {
					p.sacked = true
					acked += len(p.payload)
					c.inFlight -= len(p.payload)
				}
			}
		}
	}

	if len(h.sack) > 0 {
		c.sackRecovery(t)
	}

	switch {
	case progressed:
		c.dupAcks = 0
	case h.typ == stState && payloadLen == 0 && h.ack == c.lastAck && len(c.outq) > 0:
		c.dupAcks++
		if c.dupAcks == 3 { // the packet after h.ack was most likely lost
			if p := c.firstUnacked(); p != nil {
				c.onLoss()
				c.transmit(p)
			}
		}
	}
	if acked > 0 {
		c.congestionControl(h.tsDiff, acked)
	}
}

// sackRecovery resends packets the receiver evidently lost: ones with at
// least 3 later packets selectively acked (the same rule as TCP's three
// duplicate acks). Each is resent at most once per round trip, and the
// window is halved at most once per round trip.
func (c *Conn) sackRecovery(t time.Time) {
	later := 0
	lost := false
	for i := len(c.outq) - 1; i >= 0; i-- {
		p := c.outq[i]
		if p.sacked {
			later++
			continue
		}
		if later >= 3 && t.Sub(p.sentAt) > c.rtt {
			c.transmit(p)
			lost = true
		}
	}
	if lost && t.Sub(c.lastLoss) > c.rtt {
		c.lastLoss = t
		c.onLoss()
	}
}

func (c *Conn) firstUnacked() *outPacket {
	for _, p := range c.outq {
		if !p.sacked {
			return p
		}
	}
	return nil
}

// updateRTT follows BEP 29 (the same estimator as TCP).
func (c *Conn) updateRTT(sample time.Duration) {
	if c.rtt == 0 {
		c.rtt, c.rttVar = sample, sample/2
	} else {
		delta := c.rtt - sample
		if delta < 0 {
			delta = -delta
		}
		c.rttVar += (delta - c.rttVar) / 4
		c.rtt += (sample - c.rtt) / 8
	}
	c.rto = c.rtt + 4*c.rttVar
	if c.rto < minRTO {
		c.rto = minRTO
	}
	if c.rto > maxRTO {
		c.rto = maxRTO
	}
}

// congestionControl is LEDBAT. The peer's timestamp_difference is the
// one-way delay of our packets plus an unknown clock offset; subtracting
// the smallest value seen recently (the "base delay") cancels the offset and
// leaves the queuing delay we are causing. Below the 100 ms target the
// window grows, above it the window shrinks, in proportion to the distance.
func (c *Conn) congestionControl(tsDiff uint32, acked int) {
	var queuing time.Duration
	if tsDiff != 0 {
		c.delays.add(tsDiff, time.Now())
		d := tsDiff - c.delays.base()
		if d < 10_000_000 { // ignore nonsense from clock jumps
			queuing = time.Duration(d) * time.Microsecond
		}
	}
	if c.cwnd < c.ssthresh && queuing < targetDelay/2 {
		c.cwnd += float64(acked) // slow start: double per round trip
	} else {
		offTarget := float64(targetDelay-queuing) / float64(targetDelay)
		windowFactor := float64(acked) / c.cwnd
		if windowFactor > 1 {
			windowFactor = 1
		}
		c.cwnd += maxGainPerRTT * offTarget * windowFactor
	}
	c.clampWindow()
}

func (c *Conn) onLoss() {
	c.ssthresh = c.cwnd / 2
	c.cwnd /= 2
	c.clampWindow()
}

func (c *Conn) clampWindow() {
	if c.cwnd < minWindow {
		c.cwnd = minWindow
	}
	if c.cwnd > maxWindow {
		c.cwnd = maxWindow
	}
	if c.ssthresh < 2*minWindow {
		c.ssthresh = 2 * minWindow
	}
}

// receive puts a DATA or FIN packet in order, buffering ones that arrive early.
func (c *Conn) receive(h *header, payload []byte) {
	next := c.ack + 1
	switch {
	case h.seq == next:
		c.deliver(h.typ, payload)
		c.ack = next
		for {
			p, ok := c.reorder[c.ack+1]
			if !ok {
				break
			}
			delete(c.reorder, c.ack+1)
			c.reorderBytes -= len(p.payload)
			c.deliver(p.typ, p.payload)
			c.ack++
		}
	case seqLess(h.seq, next):
		// A duplicate of something we already have: the ack we send will say so.
	default:
		if _, have := c.reorder[h.seq]; !have && h.seq-next < maxOutQueue && c.reorderBytes+len(payload) <= recvBufSize {
			c.reorder[h.seq] = inPacket{h.typ, payload}
			c.reorderBytes += len(payload)
		}
	}
}

func (c *Conn) deliver(typ uint8, payload []byte) {
	if c.eof {
		return
	}
	if typ == stFin {
		c.eof = true
		return
	}
	if !c.localClosed {
		c.inbuf = append(c.inbuf, payload...)
	}
}

// loop drives timers: retransmission, keep-alives, closing, deadlines.
func (c *Conn) loop() {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-c.dead:
			return
		case <-t.C:
		}
		c.mu.Lock()
		c.onTick(time.Now())
		c.cond.Broadcast() // wake readers and writers so they check their deadlines
		c.mu.Unlock()
	}
}

func (c *Conn) onTick(t time.Time) {
	if c.state == stateClosed {
		return
	}
	if p := c.firstUnacked(); p != nil && t.Sub(p.sentAt) > c.rto {
		if p.transmissions > maxRetransmits {
			c.fail(errTimedOut)
			return
		}
		// Timeout: the path is congested or gone. Shrink to one packet and back off.
		c.ssthresh = c.cwnd / 2
		c.cwnd = minWindow
		c.clampWindow()
		c.rto *= 2
		if c.rto > maxRTO {
			c.rto = maxRTO
		}
		c.transmit(p)
	}
	if c.state == stateConnected && t.Sub(c.lastSend) > keepAlive {
		c.sendState()
	}
	if c.localClosed && ((c.finSent && len(c.outq) == 0) || t.Sub(c.closedAt) > closeLinger) {
		c.finish()
	}
}

// fail ends the connection with err.
func (c *Conn) fail(err error) {
	if c.state == stateClosed {
		return
	}
	c.state = stateClosed
	c.err = err
	c.onceDead.Do(func() { close(c.dead) })
	c.s.forget(c)
	c.cond.Broadcast()
}

func (c *Conn) finish() { c.fail(net.ErrClosed) }

// abort ends the connection immediately, optionally telling the peer.
func (c *Conn) abort(err error, reset bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reset && c.state != stateClosed {
		h := &header{typ: stReset, connID: c.sendID, ts: now(), seq: c.seq, ack: c.ack}
		c.s.send(h.marshal(nil), c.raddr)
	}
	c.fail(err)
}

func (c *Conn) waitLocked(deadline time.Time) error {
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return errDeadline
	}
	c.cond.Wait()
	return nil
}

// Read reads data in order. After the peer's FIN it returns io.EOF.
func (c *Conn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if len(c.inbuf) > 0 {
			n := copy(b, c.inbuf)
			c.inbuf = c.inbuf[n:]
			if len(c.inbuf) == 0 {
				c.inbuf = nil
			}
			// If we had told the peer our buffer was nearly full, tell it there's room again.
			if c.state == stateConnected && c.lastWnd < recvBufSize/4 && c.recvWindow() >= recvBufSize/2 {
				c.sendState()
			}
			return n, nil
		}
		switch {
		case c.localClosed:
			return 0, net.ErrClosed
		case c.eof:
			return 0, io.EOF
		case c.err != nil:
			return 0, c.err
		}
		if err := c.waitLocked(c.readDeadline); err != nil {
			return 0, err
		}
	}
}

// Write splits b into packets, waiting while the send window is full.
func (c *Conn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0
	for len(b) > 0 {
		n := len(b)
		if n > maxPayload {
			n = maxPayload
		}
		for {
			switch {
			case c.localClosed:
				return total, net.ErrClosed
			case c.err != nil:
				return total, c.err
			}
			if c.state == stateConnected && c.canSend(n) {
				break
			}
			if err := c.waitLocked(c.writeDeadline); err != nil {
				return total, err
			}
			continue
		}
		c.queue(stData, append([]byte(nil), b[:n]...))
		b = b[n:]
		total += n
	}
	return total, nil
}

// canSend reports whether n more bytes fit in the window: the smaller of
// our congestion window and the peer's receive window. With nothing in
// flight one packet always goes, which also probes a peer whose window was 0.
func (c *Conn) canSend(n int) bool {
	if len(c.outq) >= maxOutQueue {
		return false
	}
	if c.inFlight == 0 {
		return true
	}
	window := int(c.cwnd)
	if c.peerWnd < window {
		window = c.peerWnd
	}
	return c.inFlight+n <= window
}

// Close sends FIN after any data still queued. Unsent data keeps being
// retransmitted in the background for a few seconds; Close doesn't wait.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.localClosed || c.state == stateClosed {
		return nil
	}
	c.localClosed = true
	c.closedAt = time.Now()
	c.inbuf = nil
	if c.state == stateConnected {
		c.queue(stFin, nil)
		c.finSent = true
	} else {
		h := &header{typ: stReset, connID: c.sendID, ts: now(), seq: c.seq, ack: c.ack}
		c.s.send(h.marshal(nil), c.raddr)
		c.fail(net.ErrClosed)
	}
	c.cond.Broadcast()
	return nil
}

func (c *Conn) LocalAddr() net.Addr  { return c.s.Addr() }
func (c *Conn) RemoteAddr() net.Addr { return c.raddr }

func (c *Conn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline, c.writeDeadline = t, t
	c.cond.Broadcast()
	c.mu.Unlock()
	return nil
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.cond.Broadcast()
	c.mu.Unlock()
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.cond.Broadcast()
	c.mu.Unlock()
	return nil
}

// delayHistory keeps the minimum delay sample of the current and previous
// minute, so the base delay adapts within two minutes if the route changes.
type delayHistory struct {
	cur, prev uint32
	start     time.Time
	set       bool
}

func (d *delayHistory) add(v uint32, t time.Time) {
	switch {
	case !d.set:
		d.cur, d.prev, d.start, d.set = v, v, t, true
	case t.Sub(d.start) > time.Minute:
		d.prev, d.cur, d.start = d.cur, v, t
	case int32(v-d.cur) < 0:
		d.cur = v
	}
}

func (d *delayHistory) base() uint32 {
	if int32(d.prev-d.cur) < 0 {
		return d.prev
	}
	return d.cur
}
