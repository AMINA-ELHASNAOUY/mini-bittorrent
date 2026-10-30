package utp

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	mrand "math/rand"
	"net"
	"sync"
	"testing"
	"time"
)

func TestHeaderRoundTrip(t *testing.T) {
	h := &header{typ: stData, connID: 0xbeef, ts: 1, tsDiff: 2, wnd: 3, seq: 65535, ack: 7, sack: []byte{1, 2, 3, 4}}
	b := h.marshal([]byte("hello"))
	got, payload, err := unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.typ != h.typ || got.connID != h.connID || got.ts != 1 || got.tsDiff != 2 || got.wnd != 3 ||
		got.seq != 65535 || got.ack != 7 || !bytes.Equal(got.sack, h.sack) || string(payload) != "hello" {
		t.Fatalf("round trip mismatch: %+v %q", got, payload)
	}
	if b[0] != stData<<4|1 {
		t.Fatalf("first byte %#x, want type<<4|version", b[0])
	}
}

func TestRejectsNonUTP(t *testing.T) {
	for _, b := range [][]byte{
		[]byte("d1:ad2:id20:aaaaaaaaaaaaaaaaaaaae1:q4:ping1:t2:aa1:y1:qe"), // a DHT ping
		make([]byte, 10),
		append([]byte{0x41, 9}, make([]byte, 18)...), // extension claims 9 bytes but none follow
	} {
		if _, _, err := unmarshal(b); err == nil {
			t.Errorf("accepted %q", b)
		}
	}
}

func TestSeqLessWraps(t *testing.T) {
	if !seqLess(65535, 0) || seqLess(0, 65535) || !seqLess(1, 2) || seqLess(5, 5) {
		t.Fatal("wrap-around comparison is wrong")
	}
}

// lossyConn simulates a bad network: it drops, duplicates and reorders packets.
type lossyConn struct {
	net.PacketConn
	mu        sync.Mutex
	rng       *mrand.Rand
	drop, dup float64
	maxDelay  time.Duration
}

func (l *lossyConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	l.mu.Lock()
	drop := l.rng.Float64() < l.drop
	dup := l.rng.Float64() < l.dup
	delay := time.Duration(0)
	if l.maxDelay > 0 {
		delay = time.Duration(l.rng.Int63n(int64(l.maxDelay)))
	}
	l.mu.Unlock()
	if drop {
		return len(b), nil
	}
	pkt := append([]byte(nil), b...)
	send := func() { l.PacketConn.WriteTo(pkt, addr) }
	if delay > 0 {
		time.AfterFunc(delay, send) // later packets may overtake this one
	} else {
		send()
	}
	if dup {
		time.AfterFunc(delay+time.Millisecond, send)
	}
	return len(b), nil
}

func pair(t *testing.T, wrap func(net.PacketConn) net.PacketConn) (client, server *Socket) {
	t.Helper()
	open := func(listen bool) *Socket {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		if wrap != nil {
			pc = wrap(pc)
		}
		return NewSocketOn(pc, listen)
	}
	client, server = open(false), open(true)
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server
}

// transfer sends size random bytes each way at the same time and checks them.
func transfer(t *testing.T, client, server *Socket, size int) {
	t.Helper()
	up, down := make([]byte, size), make([]byte, size)
	rand.Read(up)
	rand.Read(down)

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := server.Accept()
		if err != nil {
			t.Error(err)
			close(accepted)
			return
		}
		accepted <- c
	}()
	cc, err := client.Dial(server.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	// The receiving side learns the connection is established from the
	// first data packet, as in TCP-less BitTorrent handshakes.
	go cc.Write(up)
	sc := <-accepted
	if sc == nil {
		t.FailNow()
	}
	defer sc.Close()
	go sc.Write(down)

	deadline := time.Now().Add(60 * time.Second)
	cc.SetReadDeadline(deadline)
	sc.SetReadDeadline(deadline)
	var wg sync.WaitGroup
	var gotUp, gotDown []byte
	var errUp, errDown error
	wg.Add(2)
	go func() { defer wg.Done(); gotUp, errUp = readN(sc, size) }()
	go func() { defer wg.Done(); gotDown, errDown = readN(cc, size) }()
	wg.Wait()
	if errUp != nil || errDown != nil {
		t.Fatalf("read errors: up %v, down %v", errUp, errDown)
	}
	if !bytes.Equal(gotUp, up) || !bytes.Equal(gotDown, down) {
		t.Fatal("data corrupted in transit")
	}
}

func readN(c net.Conn, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(c, buf)
	return buf, err
}

func TestTransferBothWays(t *testing.T) {
	client, server := pair(t, nil)
	transfer(t, client, server, 4<<20)
}

func TestTransferOverLossyNetwork(t *testing.T) {
	// 10% loss, 5% duplicates, up to 20 ms of jitter that reorders packets.
	seed := int64(1)
	var mu sync.Mutex
	wrap := func(pc net.PacketConn) net.PacketConn {
		mu.Lock()
		defer mu.Unlock()
		seed++
		return &lossyConn{PacketConn: pc, rng: mrand.New(mrand.NewSource(seed)), drop: 0.10, dup: 0.05, maxDelay: 20 * time.Millisecond}
	}
	client, server := pair(t, wrap)
	transfer(t, client, server, 512<<10)
}

func TestCloseGivesEOFAfterAllData(t *testing.T) {
	client, server := pair(t, nil)
	go func() {
		c, err := client.Dial(server.Addr().String(), 5*time.Second)
		if err != nil {
			t.Error(err)
			return
		}
		c.Write(bytes.Repeat([]byte("x"), 100_000))
		c.Close() // FIN goes after the data
	}()
	sc, err := server.Accept()
	if err != nil {
		t.Fatal(err)
	}
	sc.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 100_000 {
		t.Fatalf("got %d bytes before EOF, want 100000", len(got))
	}
}

func TestReadDeadlineIsATimeoutAndRecoverable(t *testing.T) {
	client, server := pair(t, nil)
	go func() {
		c, _ := server.Accept()
		if c == nil {
			return
		}
		buf := make([]byte, 1)
		io.ReadFull(c, buf) // wait until the client is ready, then answer
		time.Sleep(200 * time.Millisecond)
		c.Write([]byte("late"))
	}()
	c, err := client.Dial(server.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.Write([]byte{1})
	c.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	_, err = c.Read(make([]byte, 4))
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("got %v, want a timeout net.Error", err)
	}
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, err := readN(c, 4)
	if err != nil || string(got) != "late" {
		t.Fatalf("after the timeout: %q, %v", got, err)
	}
}

func TestDialWithoutListenerIsRefused(t *testing.T) {
	client, _ := pair(t, nil)
	other, err := NewSocket("127.0.0.1:0") // not listening: answers SYN with RESET
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	start := time.Now()
	if _, err := client.Dial(other.Addr().String(), 5*time.Second); err == nil {
		t.Fatal("dial succeeded")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("a refused connection should fail fast, not time out")
	}
}

func TestDialTimesOutWhenNobodyAnswers(t *testing.T) {
	client, _ := pair(t, nil)
	silent, err := net.ListenPacket("udp", "127.0.0.1:0") // reads nothing, answers nothing
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	_, err = client.Dial(silent.LocalAddr().String(), 300*time.Millisecond)
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("got %v, want a timeout", err)
	}
}

func TestOtherTrafficSharesThePort(t *testing.T) {
	client, server := pair(t, nil)
	pc := server.PacketConn()
	defer pc.Close()

	// A DHT-style message to the uTP port reaches the PacketConn, while a
	// uTP connection on the same port keeps working.
	raw, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	msg := []byte("d1:y1:qe")
	raw.WriteTo(msg, server.Addr())
	pc.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 100)
	n, from, err := pc.ReadFrom(buf)
	if err != nil || !bytes.Equal(buf[:n], msg) || from.String() != raw.LocalAddr().String() {
		t.Fatalf("got %q from %v (%v)", buf[:n], from, err)
	}
	transfer(t, client, server, 64<<10)
}

func TestManyConnectionsOnOneSocket(t *testing.T) {
	client, server := pair(t, nil)
	const n = 8
	go func() {
		for i := 0; i < n; i++ {
			c, err := server.Accept()
			if err != nil {
				return
			}
			go io.Copy(c, c) // echo
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := client.Dial(server.Addr().String(), 5*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			msg := bytes.Repeat([]byte{byte(i)}, 50_000)
			go c.Write(msg)
			c.SetReadDeadline(time.Now().Add(10 * time.Second))
			got, err := readN(c, len(msg))
			if err != nil || !bytes.Equal(got, msg) {
				t.Errorf("connection %d: echo mismatch (%v)", i, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestLEDBATBacksOffWhenDelayRises(t *testing.T) {
	c := newConn(nil, nil, 1, 2)
	c.cwnd, c.ssthresh = 50_000, 10_000 // past slow start
	// Base delay 5 ms, then no extra delay: the window grows.
	c.congestionControl(5_000, 10_000)
	grown := c.cwnd
	if grown <= 50_000 {
		t.Fatalf("window didn't grow below the target: %v", grown)
	}
	// 300 ms of queuing (3x the target): the window shrinks.
	c.congestionControl(305_000, 10_000)
	if c.cwnd >= grown {
		t.Fatalf("window didn't shrink above the target: %v -> %v", grown, c.cwnd)
	}
}

// The accepting side may speak first (it doesn't wait for the dialer's data).
func TestAcceptorCanWriteFirst(t *testing.T) {
	client, server := pair(t, nil)
	go func() {
		c, err := server.Accept()
		if err == nil {
			c.Write([]byte("welcome"))
		}
	}()
	c, err := client.Dial(server.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	got, err := readN(c, 7)
	if err != nil || string(got) != "welcome" {
		t.Fatalf("got %q, %v", got, err)
	}
}
