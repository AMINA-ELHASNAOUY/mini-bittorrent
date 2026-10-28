package peer

import (
	"encoding/binary"
	"fmt"
	"io"
)

type messageID uint8

const (
	MsgChoke         messageID = 0
	MsgUnchoke       messageID = 1
	MsgInterested    messageID = 2
	MsgNotInterested messageID = 3
	MsgHave          messageID = 4
	MsgBitfield      messageID = 5
	MsgRequest       messageID = 6
	MsgPiece         messageID = 7
	MsgCancel        messageID = 8
)

// Message is a peer wire protocol message: <length><ID><payload>.
type Message struct {
	ID      messageID
	Payload []byte
}

// Serialize turns a Message into the wire format. A nil Message is a keep-alive (zero-length).
func (m *Message) Serialize() []byte {
	if m == nil {
		return make([]byte, 4) // keep-alive: length 0
	}
	length := uint32(len(m.Payload) + 1) // +1 for ID byte
	buf := make([]byte, 4+length)
	binary.BigEndian.PutUint32(buf[0:4], length)
	buf[4] = byte(m.ID)
	copy(buf[5:], m.Payload)
	return buf
}

// ReadMessage reads a single message from the stream. Returns nil, nil on keep-alive.
func ReadMessage(r io.Reader) (*Message, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf)

	if length == 0 {
		return nil, nil // keep-alive
	}

	messageBuf := make([]byte, length)
	if _, err := io.ReadFull(r, messageBuf); err != nil {
		return nil, err
	}

	return &Message{
		ID:      messageID(messageBuf[0]),
		Payload: messageBuf[1:],
	}, nil
}

func (m *Message) String() string {
	if m == nil {
		return "KeepAlive"
	}
	return fmt.Sprintf("ID: %d, len: %d", m.ID, len(m.Payload))
}

// FormatRequest builds a REQUEST message for length bytes at begin inside piece index.
func FormatRequest(index, begin, length int) *Message {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], uint32(index))
	binary.BigEndian.PutUint32(payload[4:8], uint32(begin))
	binary.BigEndian.PutUint32(payload[8:12], uint32(length))
	return &Message{ID: MsgRequest, Payload: payload}
}

// FormatHave builds a HAVE message announcing that we now own piece index.
func FormatHave(index int) *Message {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(index))
	return &Message{ID: MsgHave, Payload: payload}
}

// ParseHave returns the piece index announced in a HAVE message.
func ParseHave(m *Message) (int, error) {
	if m.ID != MsgHave {
		return 0, fmt.Errorf("expected HAVE (ID %d), got ID %d", MsgHave, m.ID)
	}
	if len(m.Payload) != 4 {
		return 0, fmt.Errorf("HAVE payload must be 4 bytes, got %d", len(m.Payload))
	}
	return int(binary.BigEndian.Uint32(m.Payload)), nil
}

// ParsePiece copies the block carried by a PIECE message into buf (the whole
// piece being assembled) and returns the number of bytes copied.
func ParsePiece(index int, buf []byte, m *Message) (int, error) {
	if m.ID != MsgPiece {
		return 0, fmt.Errorf("expected PIECE (ID %d), got ID %d", MsgPiece, m.ID)
	}
	if len(m.Payload) < 8 {
		return 0, fmt.Errorf("PIECE payload too short (%d bytes)", len(m.Payload))
	}
	if got := int(binary.BigEndian.Uint32(m.Payload[0:4])); got != index {
		return 0, fmt.Errorf("expected piece %d, got %d", index, got)
	}
	begin := int(binary.BigEndian.Uint32(m.Payload[4:8]))
	data := m.Payload[8:]
	if begin >= len(buf) || begin+len(data) > len(buf) {
		return 0, fmt.Errorf("block [%d, %d) does not fit in piece of %d bytes", begin, begin+len(data), len(buf))
	}
	return copy(buf[begin:], data), nil
}
