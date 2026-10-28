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

func TestRequestAndHave(t *testing.T) {
	req := FormatRequest(4, 16384, 16384)
	if req.ID != MsgRequest || !bytes.Equal(req.Payload, []byte{0, 0, 0, 4, 0, 0, 0x40, 0, 0, 0, 0x40, 0}) {
		t.Errorf("bad request payload %v", req.Payload)
	}
	idx, err := ParseHave(FormatHave(300))
	if err != nil || idx != 300 {
		t.Errorf("ParseHave = %d, %v", idx, err)
	}
	if _, err := ParseHave(&Message{ID: MsgHave, Payload: []byte{1}}); err == nil {
		t.Error("expected error for short HAVE")
	}
}

func TestParsePiece(t *testing.T) {
	buf := make([]byte, 10)
	m := &Message{ID: MsgPiece, Payload: []byte{0, 0, 0, 2, 0, 0, 0, 3, 'a', 'b', 'c'}}
	n, err := ParsePiece(2, buf, m)
	if err != nil || n != 3 || string(buf[3:6]) != "abc" {
		t.Fatalf("n=%d err=%v buf=%q", n, err, buf)
	}
	if _, err := ParsePiece(1, buf, m); err == nil {
		t.Error("expected error for wrong piece index")
	}
	tooFar := &Message{ID: MsgPiece, Payload: []byte{0, 0, 0, 2, 0, 0, 0, 9, 'a', 'b'}}
	if _, err := ParsePiece(2, buf, tooFar); err == nil {
		t.Error("expected error for block past end of piece")
	}
}
