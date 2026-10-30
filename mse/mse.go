// Package mse implements BitTorrent's Message Stream Encryption (MSE, also
// called PE): a Diffie-Hellman key exchange followed by an RC4-encrypted
// stream. It was designed to stop ISPs from recognising and throttling
// BitTorrent traffic, not to protect the data (RC4 and 768-bit DH are weak
// by today's standards; the content is public anyway and verified by
// SHA-1). Some peers refuse unencrypted connections, which is the reason to
// speak it.
//
// The exchange, with A the side that opened the connection:
//
//	A -> B: Ya, PadA
//	B -> A: Yb, PadB
//	A -> B: HASH("req1", S), HASH("req2", SKEY) xor HASH("req3", S),
//	        ENCRYPT(VC, crypto_provide, len(PadC), PadC, len(IA)), ENCRYPT(IA)
//	B -> A: ENCRYPT(VC, crypto_select, len(PadD), PadD), ENCRYPT2(payload...)
//
// S is the shared DH secret, SKEY the torrent's info hash (so a listener
// can tell which torrent is wanted without it ever crossing the wire in
// the clear), VC eight zero bytes used to find where the random padding
// ends, and IA optional initial payload.
package mse

import (
	"bytes"
	"crypto/rand"
	"crypto/rc4"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"time"
)

// Crypto methods offered in crypto_provide / chosen in crypto_select.
const (
	MethodPlaintext = 0x01 // only the handshake is encrypted
	MethodRC4       = 0x02 // the whole stream is encrypted
)

const (
	keyLen     = 96  // 768-bit DH public keys, big-endian
	maxPad     = 512 // padding is 0-512 random bytes
	handshakeT = 15 * time.Second
	keyReplyT  = 5 * time.Second
)

var (
	prime, _ = new(big.Int).SetString("FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74"+
		"020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563", 16)
	generator = big.NewInt(2)
	vc        = make([]byte, 8)
)

func hash(parts ...[]byte) []byte {
	h := sha1.New()
	for _, p := range parts {
		h.Write(p)
	}
	return h.Sum(nil)
}

type keyPair struct {
	private *big.Int
	public  []byte
}

func newKeyPair() (*keyPair, error) {
	x := make([]byte, 20) // a 160-bit private key, as the spec recommends
	if _, err := rand.Read(x); err != nil {
		return nil, err
	}
	priv := new(big.Int).SetBytes(x)
	pub := new(big.Int).Exp(generator, priv, prime)
	return &keyPair{priv, pad96(pub)}, nil
}

func (k *keyPair) secret(peerPublic []byte) ([]byte, error) {
	y := new(big.Int).SetBytes(peerPublic)
	if y.Cmp(big.NewInt(1)) <= 0 || y.Cmp(new(big.Int).Sub(prime, big.NewInt(1))) >= 0 {
		return nil, errors.New("mse: invalid public key")
	}
	return pad96(new(big.Int).Exp(y, k.private, prime)), nil
}

func pad96(n *big.Int) []byte {
	b := n.Bytes()
	out := make([]byte, keyLen)
	copy(out[keyLen-len(b):], b)
	return out
}

// newRC4 is RC4 keyed with HASH(name, S, SKEY), with the first 1024 bytes
// of keystream thrown away as the spec requires (RC4's start is biased).
func newRC4(name string, s, skey []byte) *rc4.Cipher {
	c, _ := rc4.NewCipher(hash([]byte(name), s, skey))
	discard := make([]byte, 1024)
	c.XORKeyStream(discard, discard)
	return c
}

func randomPad() ([]byte, error) {
	var n [2]byte
	if _, err := rand.Read(n[:]); err != nil {
		return nil, err
	}
	pad := make([]byte, int(binary.BigEndian.Uint16(n[:]))%(maxPad+1))
	_, err := rand.Read(pad)
	return pad, err
}

// Initiate runs side A of the handshake on an outgoing connection.
// provide is a bitmask of acceptable methods (MethodRC4, MethodPlaintext).
// The returned conn carries the rest of the stream (encrypted or not, as
// the peer selected); on error the caller should close conn.
func Initiate(conn net.Conn, infoHash [20]byte, provide uint32) (net.Conn, error) {
	conn.SetDeadline(time.Now().Add(handshakeT))
	defer conn.SetDeadline(time.Time{})

	kp, err := newKeyPair()
	if err != nil {
		return nil, err
	}
	padA, err := randomPad()
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(append(append([]byte(nil), kp.public...), padA...)); err != nil {
		return nil, err
	}
	// An MSE peer answers with its key at once. A plaintext-only peer may
	// instead sit waiting for the rest of what it takes for a handshake:
	// don't let that hold up the fallback for the whole handshake timeout.
	conn.SetReadDeadline(time.Now().Add(keyReplyT))
	yb := make([]byte, keyLen)
	if _, err := io.ReadFull(conn, yb); err != nil {
		return nil, fmt.Errorf("mse: reading key: %w", err)
	}
	conn.SetDeadline(time.Now().Add(handshakeT))
	s, err := kp.secret(yb)
	if err != nil {
		return nil, err
	}
	skey := infoHash[:]
	encA := newRC4("keyA", s, skey)
	decB := newRC4("keyB", s, skey)

	req2 := hash([]byte("req2"), skey)
	req3 := hash([]byte("req3"), s)
	for i := range req2 {
		req2[i] ^= req3[i]
	}
	msg := append(hash([]byte("req1"), s), req2...)
	plain := make([]byte, 8+4+2+2)
	binary.BigEndian.PutUint32(plain[8:], provide)
	// len(PadC) = 0 and len(IA) = 0: the BitTorrent handshake follows in the stream.
	enc := make([]byte, len(plain))
	encA.XORKeyStream(enc, plain)
	if _, err := conn.Write(append(msg, enc...)); err != nil {
		return nil, err
	}

	// B's reply starts with ENCRYPT(VC) somewhere after its padding: find it.
	want := make([]byte, 8)
	decB.XORKeyStream(want, vc)
	if err := syncTo(conn, want, maxPad+8); err != nil {
		return nil, err
	}
	head := make([]byte, 6)
	if _, err := io.ReadFull(conn, head); err != nil {
		return nil, err
	}
	decB.XORKeyStream(head, head)
	selected := binary.BigEndian.Uint32(head[:4])
	padD := make([]byte, binary.BigEndian.Uint16(head[4:]))
	if len(padD) > maxPad {
		return nil, errors.New("mse: padding too long")
	}
	if _, err := io.ReadFull(conn, padD); err != nil {
		return nil, err
	}
	decB.XORKeyStream(padD, padD)

	switch {
	case selected == MethodRC4 && provide&MethodRC4 != 0:
		return &rc4Conn{Conn: conn, enc: encA, dec: decB}, nil
	case selected == MethodPlaintext && provide&MethodPlaintext != 0:
		return conn, nil
	default:
		return nil, fmt.Errorf("mse: peer selected method %#x, which we didn't offer", selected)
	}
}

// syncTo reads until the bytes just read equal want, giving up after limit bytes.
func syncTo(r io.Reader, want []byte, limit int) error {
	buf := make([]byte, 0, limit)
	one := make([]byte, 1)
	for len(buf) < limit {
		if _, err := io.ReadFull(r, one); err != nil {
			return fmt.Errorf("mse: %w", err)
		}
		buf = append(buf, one[0])
		if len(buf) >= len(want) && bytes.Equal(buf[len(buf)-len(want):], want) {
			return nil
		}
	}
	return errors.New("mse: handshake marker not found")
}

// Respond runs side B of the handshake on an incoming connection whose
// first bytes (Ya) have not been read yet. allow is a bitmask of methods
// we accept; RC4 is chosen whenever both sides allow it. Any initial
// payload (IA) the peer sent is returned first by Read.
func Respond(conn net.Conn, infoHash [20]byte, allow uint32) (net.Conn, error) {
	conn.SetDeadline(time.Now().Add(handshakeT))
	defer conn.SetDeadline(time.Time{})

	ya := make([]byte, keyLen)
	if _, err := io.ReadFull(conn, ya); err != nil {
		return nil, fmt.Errorf("mse: reading key: %w", err)
	}
	kp, err := newKeyPair()
	if err != nil {
		return nil, err
	}
	s, err := kp.secret(ya)
	if err != nil {
		return nil, err
	}
	padB, err := randomPad()
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(append(append([]byte(nil), kp.public...), padB...)); err != nil {
		return nil, err
	}

	if err := syncTo(conn, hash([]byte("req1"), s), maxPad+20); err != nil {
		return nil, err
	}
	got := make([]byte, 20)
	if _, err := io.ReadFull(conn, got); err != nil {
		return nil, err
	}
	req3 := hash([]byte("req3"), s)
	for i := range got {
		got[i] ^= req3[i]
	}
	skey := infoHash[:]
	if !bytes.Equal(got, hash([]byte("req2"), skey)) {
		return nil, errors.New("mse: peer wants a torrent we don't serve")
	}
	decA := newRC4("keyA", s, skey)
	encB := newRC4("keyB", s, skey)

	head := make([]byte, 8+4+2)
	if _, err := io.ReadFull(conn, head); err != nil {
		return nil, err
	}
	decA.XORKeyStream(head, head)
	if !bytes.Equal(head[:8], vc) {
		return nil, errors.New("mse: bad verification constant")
	}
	provide := binary.BigEndian.Uint32(head[8:12])
	padC := make([]byte, binary.BigEndian.Uint16(head[12:14]))
	if len(padC) > maxPad {
		return nil, errors.New("mse: padding too long")
	}
	if _, err := io.ReadFull(conn, padC); err != nil {
		return nil, err
	}
	decA.XORKeyStream(padC, padC)
	var iaLen [2]byte
	if _, err := io.ReadFull(conn, iaLen[:]); err != nil {
		return nil, err
	}
	decA.XORKeyStream(iaLen[:], iaLen[:])
	ia := make([]byte, binary.BigEndian.Uint16(iaLen[:]))
	if _, err := io.ReadFull(conn, ia); err != nil {
		return nil, err
	}
	decA.XORKeyStream(ia, ia)

	var selected uint32
	switch {
	case provide&allow&MethodRC4 != 0:
		selected = MethodRC4
	case provide&allow&MethodPlaintext != 0:
		selected = MethodPlaintext
	default:
		return nil, fmt.Errorf("mse: no common method (peer offers %#x)", provide)
	}
	reply := make([]byte, 8+4+2) // VC, crypto_select, len(PadD) = 0
	binary.BigEndian.PutUint32(reply[8:], selected)
	encB.XORKeyStream(reply, reply)
	if _, err := conn.Write(reply); err != nil {
		return nil, err
	}

	var out net.Conn = conn
	if selected == MethodRC4 {
		out = &rc4Conn{Conn: conn, enc: encB, dec: decA}
	}
	if len(ia) > 0 {
		out = &PrefixConn{Conn: out, Prefix: ia}
	}
	return out, nil
}

// rc4Conn encrypts everything written and decrypts everything read.
type rc4Conn struct {
	net.Conn
	rmu, wmu sync.Mutex
	enc, dec *rc4.Cipher
}

func (c *rc4Conn) Read(b []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	n, err := c.Conn.Read(b)
	c.dec.XORKeyStream(b[:n], b[:n])
	return n, err
}

func (c *rc4Conn) Write(b []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	out := make([]byte, len(b))
	c.enc.XORKeyStream(out, b)
	// Keystream was used for all of b, so a short write can't be retried:
	// treat it as fatal, which net.Conn writes are anyway.
	return c.Conn.Write(out)
}

// PrefixConn returns Prefix before reading from Conn.
type PrefixConn struct {
	net.Conn
	Prefix []byte
}

func (c *PrefixConn) Read(b []byte) (int, error) {
	if len(c.Prefix) > 0 {
		n := copy(b, c.Prefix)
		c.Prefix = c.Prefix[n:]
		return n, nil
	}
	return c.Conn.Read(b)
}
