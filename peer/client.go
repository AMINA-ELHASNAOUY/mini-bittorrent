package peer

import (
	"fmt"
	"net"
	"time"
)

// Client is an open, handshaken connection to one peer.
type Client struct {
	Conn     net.Conn
	Choked   bool
	Bitfield Bitfield
	Addr     string
}

// Dial connects to addr, performs the handshake and reads the peer's
// initial piece announcement. numPieces sizes the bitfield for peers that
// announce pieces with HAVE messages instead of a BITFIELD.
func Dial(addr string, infoHash, peerID [20]byte, numPieces int) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	if _, err := DoHandshake(conn, infoHash, peerID); err != nil {
		conn.Close()
		return nil, fmt.Errorf("handshake: %w", err)
	}

	bf, err := readInitialBitfield(conn, numPieces)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Client{Conn: conn, Choked: true, Bitfield: bf, Addr: addr}, nil
}

// readInitialBitfield waits for the peer's BITFIELD. Peers that skip it and
// send HAVE messages (or go straight to UNCHOKE) are handled too.
func readInitialBitfield(conn net.Conn, numPieces int) (Bitfield, error) {
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetDeadline(time.Time{})

	msg, err := ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("reading bitfield: %w", err)
	}
	if msg != nil && msg.ID == MsgBitfield {
		if len(msg.Payload) < (numPieces+7)/8 {
			return nil, fmt.Errorf("bitfield too short: %d bytes for %d pieces", len(msg.Payload), numPieces)
		}
		return Bitfield(msg.Payload), nil
	}

	bf := make(Bitfield, (numPieces+7)/8)
	if msg != nil && msg.ID == MsgHave {
		if idx, err := ParseHave(msg); err == nil {
			bf.SetPiece(idx)
		}
	}
	return bf, nil
}

// Read reads the next message from the peer (nil means keep-alive).
func (c *Client) Read() (*Message, error) {
	return ReadMessage(c.Conn)
}

func (c *Client) send(m *Message) error {
	_, err := c.Conn.Write(m.Serialize())
	return err
}

// SendInterested tells the peer we want to download from it.
func (c *Client) SendInterested() error { return c.send(&Message{ID: MsgInterested}) }

// SendUnchoke tells the peer it may request from us.
func (c *Client) SendUnchoke() error { return c.send(&Message{ID: MsgUnchoke}) }

// SendRequest asks for one block of a piece.
func (c *Client) SendRequest(index, begin, length int) error {
	return c.send(FormatRequest(index, begin, length))
}

// SendHave announces that we finished a piece.
func (c *Client) SendHave(index int) error { return c.send(FormatHave(index)) }

// Close closes the connection.
func (c *Client) Close() error { return c.Conn.Close() }
