package mse

import (
	"bytes"
	"crypto/rand"
	"io"
	"math/big"
	"net"
	"testing"
)

var infoHash = [20]byte{1, 2, 3, 4, 5}

// tcpPair returns both ends of a loopback TCP connection.
func tcpPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan net.Conn)
	go func() {
		c, _ := ln.Accept()
		done <- c
	}()
	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	server = <-done
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server
}

type result struct {
	c   net.Conn
	err error
}

func handshake(t *testing.T, provide, allow uint32, serverHash [20]byte) (a, b net.Conn, errA, errB error) {
	t.Helper()
	ca, cb := tcpPair(t)
	resB := make(chan result)
	go func() {
		c, err := Accept(cb, serverHash, policyFor(allow))
		if err != nil {
			cb.Close()
		}
		resB <- result{c, err}
	}()
	a, errA = Initiate(ca, infoHash, provide)
	if errA != nil {
		ca.Close() // as callers do; unblocks the other side
	}
	r := <-resB
	return a, r.c, errA, r.err
}

func policyFor(allow uint32) Policy {
	if allow == MethodRC4 {
		return Require
	}
	return Prefer
}

// exchange checks that bytes flow both ways after the handshake.
func exchange(t *testing.T, a, b net.Conn) {
	t.Helper()
	msgA, msgB := make([]byte, 100_000), make([]byte, 50_000)
	rand.Read(msgA)
	rand.Read(msgB)
	go a.Write(msgA)
	go b.Write(msgB)
	gotA, gotB := make([]byte, len(msgA)), make([]byte, len(msgB))
	if _, err := io.ReadFull(b, gotA); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(a, gotB); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotA, msgA) || !bytes.Equal(gotB, msgB) {
		t.Fatal("data garbled after the handshake")
	}
}

func TestRC4Stream(t *testing.T) {
	a, b, errA, errB := handshake(t, MethodRC4|MethodPlaintext, MethodRC4|MethodPlaintext, infoHash)
	if errA != nil || errB != nil {
		t.Fatalf("A: %v, B: %v", errA, errB)
	}
	if _, ok := a.(*rc4Conn); !ok {
		t.Fatalf("RC4 offered and allowed on both sides but not selected (%T)", a)
	}
	exchange(t, a, b)
}

func TestEncryptedBytesAreNotPlaintext(t *testing.T) {
	// What travels on the wire after the handshake must not be the plaintext.
	ca, cb := tcpPair(t)
	go func() {
		c, err := Initiate(ca, infoHash, MethodRC4)
		if err == nil {
			c.Write(bytes.Repeat([]byte("BitTorrent protocol"), 100))
		}
	}()
	b, err := Accept(cb, infoHash, Require)
	if err != nil {
		t.Fatal(err)
	}
	raw := b.(*rc4Conn).Conn // read below the decryption layer
	wire := make([]byte, 1900)
	if _, err := io.ReadFull(raw, wire); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(wire, []byte("BitTorrent")) {
		t.Fatal("plaintext visible on the wire")
	}
}

func TestPlaintextMethod(t *testing.T) {
	// Only plaintext offered: the handshake is encrypted, the stream is not.
	a, b, errA, errB := handshake(t, MethodPlaintext, MethodRC4|MethodPlaintext, infoHash)
	if errA != nil || errB != nil {
		t.Fatalf("A: %v, B: %v", errA, errB)
	}
	if _, ok := a.(*rc4Conn); ok {
		t.Fatal("RC4 selected although only plaintext was offered")
	}
	exchange(t, a, b)
}

func TestWrongTorrentIsRejected(t *testing.T) {
	_, _, errA, errB := handshake(t, MethodRC4, MethodRC4|MethodPlaintext, [20]byte{9, 9, 9})
	if errB == nil || errA == nil {
		t.Fatalf("handshake for another torrent succeeded (A: %v, B: %v)", errA, errB)
	}
}

func TestRequireRefusesPlaintextMethod(t *testing.T) {
	_, _, errA, errB := handshake(t, MethodPlaintext, MethodRC4, infoHash)
	if errB == nil || errA == nil {
		t.Fatal("a plaintext-only offer was accepted by a peer requiring RC4")
	}
}

func TestAcceptDetectsPlaintextHandshake(t *testing.T) {
	ca, cb := tcpPair(t)
	hs := append([]byte("\x13BitTorrent protocol"), make([]byte, 48)...)
	go ca.Write(hs)
	b, err := Accept(cb, infoHash, Prefer)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(hs))
	if _, err := io.ReadFull(b, got); err != nil || !bytes.Equal(got, hs) {
		t.Fatalf("plaintext handshake not passed through intact: %v", err)
	}

	ca2, cb2 := tcpPair(t)
	go ca2.Write(hs)
	if _, err := Accept(cb2, infoHash, Require); err == nil {
		t.Fatal("plaintext accepted with encryption required")
	}
}

func TestPreferFallsBackToPlaintextPeer(t *testing.T) {
	// A peer that only speaks plaintext hangs up on our DH key; Dial must redial.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for i := 0; ; i++ {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			buf := make([]byte, 20)
			io.ReadFull(c, buf)
			if bytes.Equal(buf, plainHeader) {
				c.Write([]byte("hello"))
			}
			c.Close()
		}
	}()
	dial := func() (net.Conn, error) { return net.Dial("tcp", ln.Addr().String()) }
	c, err := Dial(dial, infoHash, Prefer)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.Write(plainHeader)
	got := make([]byte, 5)
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "hello" {
		t.Fatalf("fallback connection: %q, %v", got, err)
	}
	if _, err := Dial(dial, infoHash, Require); err == nil {
		t.Fatal("Require fell back to plaintext")
	}
}

func TestInitialPayloadIsDelivered(t *testing.T) {
	// Other clients (libtorrent) send the BitTorrent handshake as IA inside
	// the encrypted handshake; Respond must hand it back through Read.
	ca, cb := tcpPair(t)
	go func() {
		kp, _ := newKeyPair()
		ca.Write(kp.public)
		yb := make([]byte, keyLen)
		io.ReadFull(ca, yb)
		s, _ := kp.secret(yb)
		req2 := hash([]byte("req2"), infoHash[:])
		req3 := hash([]byte("req3"), s)
		for i := range req2 {
			req2[i] ^= req3[i]
		}
		ia := []byte("initial payload")
		plain := make([]byte, 16)
		plain[11] = MethodRC4
		plain[15] = byte(len(ia))
		plain = append(plain, ia...)
		enc := newRC4("keyA", s, infoHash[:])
		enc.XORKeyStream(plain, plain)
		ca.Write(append(append(hash([]byte("req1"), s), req2...), plain...))
	}()
	b, err := Accept(cb, infoHash, Prefer)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("initial payload"))
	if _, err := io.ReadFull(b, got); err != nil || string(got) != "initial payload" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestRejectsDegenerateKeys(t *testing.T) {
	kp, _ := newKeyPair()
	pMinus1 := pad96(new(big.Int).Sub(prime, big.NewInt(1)))
	for _, y := range [][]byte{make([]byte, keyLen), pad96(big.NewInt(1)), pMinus1} {
		if _, err := kp.secret(y); err == nil {
			t.Errorf("accepted public key %x…", y[keyLen-4:])
		}
	}
}
