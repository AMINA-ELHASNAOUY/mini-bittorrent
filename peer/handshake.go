package peer

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"time"
)

const pstr = "BitTorrent protocol"

// Handshake represents the fixed-format BitTorrent handshake message.
type Handshake struct {
	Pstr     string
	InfoHash [20]byte
	PeerID   [20]byte
}

// NewHandshake creates a new handshake with the standard BitTorrent protocol string.
func NewHandshake(infoHash, peerID [20]byte) *Handshake {
	return &Handshake{
		Pstr:     pstr,
		InfoHash: infoHash,
		PeerID:   peerID,
	}
}

// Serialize converts the handshake into the wire format:
// <pstrlen><pstr><reserved 8 bytes><info_hash><peer_id>
func (h *Handshake) Serialize() []byte {
	buf := make([]byte, len(h.Pstr)+49)
	buf[0] = byte(len(h.Pstr))
	curr := 1
	curr += copy(buf[curr:], h.Pstr)
	curr += copy(buf[curr:], make([]byte, 8)) // reserved bytes
	curr += copy(buf[curr:], h.InfoHash[:])
	curr += copy(buf[curr:], h.PeerID[:])
	return buf
}

// ReadHandshake reads and parses a handshake from a stream (e.g. net.Conn).
func ReadHandshake(r io.Reader) (*Handshake, error) {
	lengthBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	pstrlen := int(lengthBuf[0])
	if pstrlen == 0 {
		return nil, fmt.Errorf("pstrlen cannot be 0")
	}

	handshakeBuf := make([]byte, 48+pstrlen)
	if _, err := io.ReadFull(r, handshakeBuf); err != nil {
		return nil, err
	}

	var infoHash [20]byte
	var peerID [20]byte
	copy(infoHash[:], handshakeBuf[pstrlen+8:pstrlen+8+20])
	copy(peerID[:], handshakeBuf[pstrlen+8+20:])

	return &Handshake{
		Pstr:     string(handshakeBuf[0:pstrlen]),
		InfoHash: infoHash,
		PeerID:   peerID,
	}, nil
}

// DoHandshake connects to a peer, exchanges handshakes, and validates the info hash.
func DoHandshake(conn net.Conn, infoHash, peerID [20]byte) (*Handshake, error) {
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	defer conn.SetDeadline(time.Time{})

	req := NewHandshake(infoHash, peerID)
	if _, err := conn.Write(req.Serialize()); err != nil {
		return nil, err
	}

	res, err := ReadHandshake(conn)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(res.InfoHash[:], infoHash[:]) {
		return nil, fmt.Errorf("expected info hash %x but got %x", infoHash, res.InfoHash)
	}

	return res, nil
}
