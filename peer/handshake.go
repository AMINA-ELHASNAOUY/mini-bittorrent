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
	Reserved [8]byte // feature flags; see SupportsExtensions
	InfoHash [20]byte
	PeerID   [20]byte
}

// extensionBit is reserved byte 5, bit 0x10: the peer speaks the extension protocol (BEP 10).
const extensionByte, extensionBit = 5, 0x10

// SupportsExtensions reports whether the extension protocol bit is set.
func (h *Handshake) SupportsExtensions() bool {
	return h.Reserved[extensionByte]&extensionBit != 0
}

// SetExtensions advertises support for the extension protocol.
func (h *Handshake) SetExtensions() {
	h.Reserved[extensionByte] |= extensionBit
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
	curr += copy(buf[curr:], h.Reserved[:])
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
	var reserved [8]byte
	copy(reserved[:], handshakeBuf[pstrlen:pstrlen+8])
	copy(infoHash[:], handshakeBuf[pstrlen+8:pstrlen+8+20])
	copy(peerID[:], handshakeBuf[pstrlen+8+20:])

	return &Handshake{
		Pstr:     string(handshakeBuf[0:pstrlen]),
		Reserved: reserved,
		InfoHash: infoHash,
		PeerID:   peerID,
	}, nil
}

// DoHandshake connects to a peer, exchanges handshakes, and validates the info hash.
func DoHandshake(conn net.Conn, infoHash, peerID [20]byte) (*Handshake, error) {
	return doHandshake(conn, NewHandshake(infoHash, peerID))
}

// DoHandshakeExt is DoHandshake while advertising the extension protocol (BEP 10).
func DoHandshakeExt(conn net.Conn, infoHash, peerID [20]byte) (*Handshake, error) {
	req := NewHandshake(infoHash, peerID)
	req.SetExtensions()
	return doHandshake(conn, req)
}

func doHandshake(conn net.Conn, req *Handshake) (*Handshake, error) {
	infoHash := req.InfoHash
	conn.SetDeadline(time.Now().Add(3 * time.Second))
	defer conn.SetDeadline(time.Time{})

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
