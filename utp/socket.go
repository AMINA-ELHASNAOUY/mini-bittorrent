package utp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"
)

// Socket is one UDP port carrying any number of uTP connections.
type Socket struct {
	pc net.PacketConn

	mu        sync.Mutex
	conns     map[connKey]*Conn
	listening bool
	accept    chan *Conn
	closed    bool
	done      chan struct{}
	other     chan packet // non-uTP packets, for PacketConn
	otherOn   bool
}

// connKey identifies a connection: the remote address plus the
// connection ID that the remote puts in packets addressed to us.
type connKey struct {
	addr string
	id   uint16
}

type packet struct {
	b    []byte
	from net.Addr
}

// Listen opens a UDP socket on addr (e.g. ":6881") that accepts incoming uTP connections.
func Listen(addr string) (*Socket, error) {
	s, err := NewSocket(addr)
	if err != nil {
		return nil, err
	}
	s.listening = true
	return s, nil
}

// NewSocket opens a UDP socket for outgoing connections only.
func NewSocket(addr string) (*Socket, error) {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}
	return NewSocketOn(pc, false), nil
}

// NewSocketOn runs uTP on an existing packet connection (useful in tests,
// e.g. to simulate packet loss). The Socket takes ownership of pc.
func NewSocketOn(pc net.PacketConn, listen bool) *Socket {
	if u, ok := pc.(*net.UDPConn); ok {
		// Bursts at the start of a transfer overflow the default buffers
		// (packets are then dropped and have to be resent).
		u.SetReadBuffer(4 << 20)
		u.SetWriteBuffer(4 << 20)
	}
	s := &Socket{
		pc:        pc,
		conns:     map[connKey]*Conn{},
		listening: listen,
		accept:    make(chan *Conn, 32),
		done:      make(chan struct{}),
		other:     make(chan packet, 256),
	}
	go s.readLoop()
	return s
}

// Addr is the local UDP address.
func (s *Socket) Addr() net.Addr { return s.pc.LocalAddr() }

// Accept waits for an incoming connection (net.Listener).
func (s *Socket) Accept() (net.Conn, error) {
	select {
	case c := <-s.accept:
		return c, nil
	case <-s.done:
		return nil, net.ErrClosed
	}
}

// Close shuts the socket down and aborts every connection on it.
func (s *Socket) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.done)
	var all []*Conn
	for _, c := range s.conns {
		all = append(all, c)
	}
	s.mu.Unlock()
	for _, c := range all {
		c.abort(net.ErrClosed, true)
	}
	return s.pc.Close()
}

// Dial opens a uTP connection to addr, waiting at most timeout for the reply.
func (s *Socket) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.DialContext(ctx, addr)
}

// DialContext opens a uTP connection to addr. The SYN is resent with a
// doubling interval, since UDP packets can be lost.
func (s *Socket) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	raddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, net.ErrClosed
	}
	var c *Conn
	for {
		id := randomID()
		key := connKey{raddr.String(), id}
		if _, taken := s.conns[key]; !taken {
			c = newConn(s, raddr, id, id+1)
			s.conns[key] = c
			break
		}
	}
	s.mu.Unlock()

	c.mu.Lock()
	c.state = stateSynSent
	c.seq = 1
	c.queue(stSyn, nil)
	c.mu.Unlock()
	go c.loop()

	select {
	case <-c.established:
		return c, nil
	case <-ctx.Done():
		c.abort(ctx.Err(), true)
		return nil, &net.OpError{Op: "dial", Net: "utp", Addr: raddr, Err: errDialTimeout}
	case <-c.dead:
		c.mu.Lock()
		err := c.err
		c.mu.Unlock()
		return nil, &net.OpError{Op: "dial", Net: "utp", Addr: raddr, Err: err}
	}
}

var errDialTimeout = timeoutError("uTP connection timed out")

func randomID() uint16 {
	var b [2]byte
	rand.Read(b[:])
	return binary.BigEndian.Uint16(b[:])
}

func (s *Socket) readLoop() {
	buf := make([]byte, 65536)
	for {
		n, from, err := s.pc.ReadFrom(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			s.Close()
			return
		}
		b := append([]byte(nil), buf[:n]...)
		h, payload, err := unmarshal(b)
		if err != nil {
			s.mu.Lock()
			on := s.otherOn
			s.mu.Unlock()
			if on {
				select {
				case s.other <- packet{b, from}:
				default: // nobody keeping up: drop, like a full UDP buffer would
				}
			}
			continue
		}
		s.dispatch(h, payload, from)
	}
}

func (s *Socket) dispatch(h *header, payload []byte, from net.Addr) {
	addr := from.String()
	s.mu.Lock()
	if h.typ == stSyn {
		key := connKey{addr, h.connID + 1}
		if c, ok := s.conns[key]; ok {
			s.mu.Unlock()
			c.handle(h, payload) // our STATE reply was lost: answer again
			return
		}
		if !s.listening || s.closed {
			s.mu.Unlock()
			s.sendReset(from, h)
			return
		}
		c := newConn(s, from, h.connID+1, h.connID)
		c.state = stateSynRecv
		c.seq = randomID()
		c.ack = h.seq
		c.setPeer(h)
		s.conns[key] = c
		s.mu.Unlock()

		select {
		case s.accept <- c:
		default:
			s.forget(c)
			s.sendReset(from, h)
			return
		}
		c.mu.Lock()
		c.sendState()
		c.mu.Unlock()
		go c.loop()
		return
	}
	c := s.conns[connKey{addr, h.connID}]
	if c == nil && h.typ == stReset {
		// A RESET may carry either of the connection's two IDs.
		for _, cand := range s.conns {
			if cand.sendID == h.connID && cand.raddr.String() == addr {
				c = cand
				break
			}
		}
	}
	s.mu.Unlock()
	if c == nil {
		if h.typ != stReset {
			s.sendReset(from, h)
		}
		return
	}
	c.handle(h, payload)
}

// sendReset answers a packet that belongs to no connection we know.
func (s *Socket) sendReset(to net.Addr, h *header) {
	r := &header{typ: stReset, connID: h.connID, ts: now(), seq: randomID(), ack: h.seq}
	s.pc.WriteTo(r.marshal(nil), to)
}

func (s *Socket) forget(c *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := connKey{c.raddr.String(), c.recvID}
	if s.conns[key] == c {
		delete(s.conns, key)
	}
}

func (s *Socket) send(b []byte, to net.Addr) error {
	_, err := s.pc.WriteTo(b, to)
	return err
}

// PacketConn returns a view of the socket that reads every packet which is
// not uTP and writes raw datagrams. It lets the DHT share the uTP port, as
// real clients do. Closing it does not close the Socket.
func (s *Socket) PacketConn() net.PacketConn {
	s.mu.Lock()
	s.otherOn = true
	s.mu.Unlock()
	return &otherConn{s: s, closed: make(chan struct{})}
}

type otherConn struct {
	s        *Socket
	closed   chan struct{}
	once     sync.Once
	mu       sync.Mutex
	deadline time.Time
}

func (o *otherConn) ReadFrom(b []byte) (int, net.Addr, error) {
	o.mu.Lock()
	d := o.deadline
	o.mu.Unlock()
	var timeout <-chan time.Time
	if !d.IsZero() {
		t := time.NewTimer(time.Until(d))
		defer t.Stop()
		timeout = t.C
	}
	select {
	case p := <-o.s.other:
		return copy(b, p.b), p.from, nil
	case <-o.closed:
		return 0, nil, net.ErrClosed
	case <-o.s.done:
		return 0, nil, net.ErrClosed
	case <-timeout:
		return 0, nil, errDeadline
	}
}

func (o *otherConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	select {
	case <-o.closed:
		return 0, net.ErrClosed
	default:
	}
	return o.s.pc.WriteTo(b, addr)
}

func (o *otherConn) Close() error {
	o.once.Do(func() { close(o.closed) })
	return nil
}

func (o *otherConn) LocalAddr() net.Addr { return o.s.pc.LocalAddr() }
func (o *otherConn) SetDeadline(t time.Time) error {
	return o.SetReadDeadline(t)
}
func (o *otherConn) SetReadDeadline(t time.Time) error {
	o.mu.Lock()
	o.deadline = t
	o.mu.Unlock()
	return nil
}
func (o *otherConn) SetWriteDeadline(time.Time) error { return nil }
