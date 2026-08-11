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
