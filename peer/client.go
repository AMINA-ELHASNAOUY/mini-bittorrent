package peer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// Client is an open, handshaken connection to one peer.
type Client struct {
	Conn     net.Conn
	Choked   bool // whether the peer chokes US
	Bitfield Bitfield
	Addr     string
	PeerID   [20]byte

	writeMu sync.Mutex
	pending *Message // first message read while looking for a bitfield, replayed by Read
	rbuf    []byte   // bytes received but not yet returned as a whole message
}

// MaxMessageLen caps an incoming message, so a lying length prefix can't
// make us allocate gigabytes (the largest real one is a 128 KiB block or a
// bitfield for a huge torrent).
const MaxMessageLen = 4 << 20

// Dial connects to addr over TCP, performs the handshake and reads the
// peer's initial piece announcement. numPieces sizes the bitfield for peers
// that announce pieces with HAVE messages instead of a BITFIELD.
func Dial(addr string, infoHash, peerID [20]byte, numPieces int) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	return Start(conn, infoHash, peerID, numPieces, nil)
}

// Start runs the outgoing side of the handshake on an open connection (TCP
// or uTP), sends our bitfield if ours has any piece, and reads the peer's.
// The connection is closed if anything fails.
func Start(conn net.Conn, infoHash, peerID [20]byte, numPieces int, ours Bitfield) (*Client, error) {
	hs, err := DoHandshake(conn, infoHash, peerID)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("handshake: %w", err)
	}
	return finish(conn, hs, numPieces, ours)
}

// Accept runs the receiving side of the handshake on an incoming connection.
func Accept(conn net.Conn, infoHash, peerID [20]byte, numPieces int, ours Bitfield) (*Client, error) {
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	hs, err := ReadHandshake(conn)
	if err == nil && hs.InfoHash != infoHash {
		err = errors.New("handshake for a torrent we don't serve")
	}
	if err == nil {
		_, err = conn.Write(NewHandshake(infoHash, peerID).Serialize())
	}
	conn.SetDeadline(time.Time{})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("handshake: %w", err)
	}
	return finish(conn, hs, numPieces, ours)
}

func finish(conn net.Conn, hs *Handshake, numPieces int, ours Bitfield) (*Client, error) {
	c := &Client{Conn: conn, Choked: true, Addr: conn.RemoteAddr().String(), PeerID: hs.PeerID}
	// Both sides send their bitfield first and read second, so two clients
	// like this one never wait on each other.
	if len(ours) > 0 && !bytes.Equal(ours, make(Bitfield, len(ours))) {
		if err := c.send(&Message{ID: MsgBitfield, Payload: ours}); err != nil {
			conn.Close()
			return nil, err
		}
	}
	if err := c.readInitialBitfield(numPieces); err != nil {
		conn.Close()
		return nil, err
	}
	return c, nil
}

// readInitialBitfield waits for the peer's BITFIELD. A peer with no pieces
// may skip it: then the first message it sends (often INTERESTED or HAVE)
// is kept and handed out by the next Read, and silence just means "nothing yet".
func (c *Client) readInitialBitfield(numPieces int) error {
	c.Bitfield = make(Bitfield, (numPieces+7)/8)
	// Peers normally send it right after the handshake; one that is late
	// is still handled, since BITFIELD is also accepted later on.
	c.Conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	defer c.Conn.SetReadDeadline(time.Time{})

	msg, err := c.Read()
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return nil
		}
		return fmt.Errorf("reading bitfield: %w", err)
	}
	if msg != nil && msg.ID == MsgBitfield {
		if len(msg.Payload) < len(c.Bitfield) {
			return fmt.Errorf("bitfield too short: %d bytes for %d pieces", len(msg.Payload), numPieces)
		}
		copy(c.Bitfield, msg.Payload)
		return nil
	}
	c.pending = msg
	return nil
}

// Read reads the next message from the peer (nil means keep-alive).
//
// It is safe to use with read deadlines: if the deadline passes halfway
// through a message, the bytes received so far are kept and the next Read
// carries on from there. (Reading straight from the connection would lose
// them and every later message would be misread.)
func (c *Client) Read() (*Message, error) {
	if m := c.pending; m != nil {
		c.pending = nil
		return m, nil
	}
	for {
		if len(c.rbuf) >= 4 {
			length := int(binary.BigEndian.Uint32(c.rbuf[:4]))
			if length > MaxMessageLen {
				return nil, fmt.Errorf("message of %d bytes is too large", length)
			}
			if len(c.rbuf) >= 4+length {
				var m *Message
				if length > 0 {
					m = &Message{ID: messageID(c.rbuf[4]), Payload: append([]byte(nil), c.rbuf[5:4+length]...)}
				}
				c.rbuf = c.rbuf[4+length:]
				if len(c.rbuf) == 0 {
					c.rbuf = nil
				}
				return m, nil
			}
		}
		var tmp [32 << 10]byte
		n, err := c.Conn.Read(tmp[:])
		c.rbuf = append(c.rbuf, tmp[:n]...)
		if err != nil && n == 0 {
			return nil, err
		}
	}
}

// Send writes one message. It is safe to call from several goroutines.
func (c *Client) Send(m *Message) error { return c.send(m) }

func (c *Client) send(m *Message) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.Conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	_, err := c.Conn.Write(m.Serialize())
	return err
}

// SendInterested tells the peer we want to download from it.
func (c *Client) SendInterested() error { return c.send(&Message{ID: MsgInterested}) }

// SendUnchoke tells the peer it may request from us.
func (c *Client) SendUnchoke() error { return c.send(&Message{ID: MsgUnchoke}) }

// SendChoke tells the peer we won't serve its requests for now.
func (c *Client) SendChoke() error { return c.send(&Message{ID: MsgChoke}) }

// SendRequest asks for one block of a piece.
func (c *Client) SendRequest(index, begin, length int) error {
	return c.send(FormatRequest(index, begin, length))
}

// SendCancel withdraws a block request.
func (c *Client) SendCancel(index, begin, length int) error {
	return c.send(FormatCancel(index, begin, length))
}

// SendHave announces that we finished a piece.
func (c *Client) SendHave(index int) error { return c.send(FormatHave(index)) }

// Close closes the connection.
func (c *Client) Close() error { return c.Conn.Close() }
