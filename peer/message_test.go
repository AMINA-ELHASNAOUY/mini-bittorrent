package peer

import (
	"bytes"
	"testing"
)

func TestMessageSerialize(t *testing.T) {
	m := &Message{ID: MsgHave, Payload: []byte{0, 0, 0, 5}}
	buf := m.Serialize()

	expectedLen := 4 + 1 + len(m.Payload)
	if len(buf) != expectedLen {
		t.Fatalf("expected length %d, got %d", expectedLen, len(buf))
	}
	if buf[4] != byte(MsgHave) {
		t.Errorf("expected ID byte %d, got %d", MsgHave, buf[4])
	}
}

func TestReadMessage(t *testing.T) {
	m := &Message{ID: MsgBitfield, Payload: []byte{1, 2, 3}}
	buf := m.Serialize()

	parsed, err := ReadMessage(bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.ID != MsgBitfield {
		t.Errorf("expected ID %d, got %d", MsgBitfield, parsed.ID)
	}
	if !bytes.Equal(parsed.Payload, m.Payload) {
		t.Errorf("expected payload %v, got %v", m.Payload, parsed.Payload)
	}
}

func TestReadMessageKeepAlive(t *testing.T) {
	keepAlive := make([]byte, 4) // length 0
	parsed, err := ReadMessage(bytes.NewReader(keepAlive))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed != nil {
		t.Errorf("expected nil for keep-alive, got %v", parsed)
	}
}

func TestSerializeNilMessage(t *testing.T) {
	var m *Message
	buf := m.Serialize()
	if len(buf) != 4 {
		t.Fatalf("expected length 4, got %d", len(buf))
	}
}
