package mse

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"time"
)

// Policy says when to use encryption.
type Policy int

const (
	// Off: plaintext only; encrypted incoming connections are refused.
	Off Policy = iota
	// Prefer: try encryption on outgoing connections and fall back to
	// plaintext if the peer doesn't speak it; accept both incoming.
	Prefer
	// Require: encrypted (RC4) connections only, both ways.
	Require
)

// ParsePolicy reads "off", "prefer" or "require".
func ParsePolicy(s string) (Policy, error) {
	switch s {
	case "off":
		return Off, nil
	case "prefer":
		return Prefer, nil
	case "require":
		return Require, nil
	}
	return Off, fmt.Errorf("encryption must be off, prefer or require, not %q", s)
}

func (p Policy) String() string {
	return [...]string{"off", "prefer", "require"}[p]
}

// Dial opens an outgoing connection with dial and applies the policy. With
// Prefer, a peer that fails the encrypted handshake is dialed again in
// plaintext (a plaintext-only client just hangs up on our DH key).
func Dial(dial func() (net.Conn, error), infoHash [20]byte, p Policy) (net.Conn, error) {
	conn, err := dial()
	if err != nil || p == Off {
		return conn, err
	}
	provide := uint32(MethodRC4)
	if p == Prefer {
		provide |= MethodPlaintext
	}
	enc, err := Initiate(conn, infoHash, provide)
	if err == nil {
		return enc, nil
	}
	conn.Close()
	if p == Require {
		return nil, err
	}
	return dial()
}

// plainHeader is how every plaintext BitTorrent handshake begins.
var plainHeader = []byte("\x13BitTorrent protocol")

// Accept looks at the first 20 bytes of an incoming connection to tell a
// plaintext BitTorrent handshake from an encrypted one (which starts with a
// 96-byte random DH key, so reading 20 bytes is safe either way), and
// applies the policy.
func Accept(conn net.Conn, infoHash [20]byte, p Policy) (net.Conn, error) {
	conn.SetReadDeadline(time.Now().Add(handshakeT))
	first := make([]byte, len(plainHeader))
	_, err := io.ReadFull(conn, first)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		return nil, err
	}
	pc := &PrefixConn{Conn: conn, Prefix: first}
	if bytes.Equal(first, plainHeader) {
		if p == Require {
			return nil, fmt.Errorf("mse: plaintext connection refused (encryption required)")
		}
		return pc, nil
	}
	if p == Off {
		return nil, fmt.Errorf("mse: encrypted connection refused (encryption off)")
	}
	allow := uint32(MethodRC4)
	if p == Prefer {
		allow |= MethodPlaintext
	}
	return Respond(pc, infoHash, allow)
}
