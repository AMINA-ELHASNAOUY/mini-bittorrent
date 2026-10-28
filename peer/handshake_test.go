package peer

import (
	"bytes"
	"testing"
)

func TestHandshakeSerialize(t *testing.T) {
	infoHash := [20]byte{1, 2, 3}
	peerID := [20]byte{4, 5, 6}

	h := NewHandshake(infoHash, peerID)
	serialized := h.Serialize()

	expectedLen := 49 + len(pstr)
	if len(serialized) != expectedLen {
		t.Fatalf("expected length %d, got %d", expectedLen, len(serialized))
	}

	if serialized[0] != byte(len(pstr)) {
		t.Errorf("expected pstrlen byte %d, got %d", len(pstr), serialized[0])
	}
}

func TestReadHandshake(t *testing.T) {
	infoHash := [20]byte{1, 2, 3}
	peerID := [20]byte{4, 5, 6}

	h := NewHandshake(infoHash, peerID)
	serialized := h.Serialize()

	buf := bytes.NewReader(serialized)
	parsed, err := ReadHandshake(buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.Pstr != pstr {
		t.Errorf("expected pstr %q, got %q", pstr, parsed.Pstr)
	}
	if parsed.InfoHash != infoHash {
		t.Errorf("expected info hash %x, got %x", infoHash, parsed.InfoHash)
	}
	if parsed.PeerID != peerID {
		t.Errorf("expected peer id %x, got %x", peerID, parsed.PeerID)
	}
}

func TestReadHandshakeEmptyPstrlen(t *testing.T) {
	buf := bytes.NewReader([]byte{0})
	_, err := ReadHandshake(buf)
	if err == nil {
		t.Fatal("expected error for pstrlen=0, got nil")
	}
}

func TestHandshakeExtensionBit(t *testing.T) {
	h := NewHandshake([20]byte{1}, [20]byte{2})
	if h.SupportsExtensions() {
		t.Fatal("extensions should be off by default")
	}
	h.SetExtensions()
	parsed, err := ReadHandshake(bytes.NewReader(h.Serialize()))
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.SupportsExtensions() || parsed.Reserved[5] != 0x10 {
		t.Fatalf("reserved = %v", parsed.Reserved)
	}
}

func TestExtendedMessageRoundTrip(t *testing.T) {
	m, err := FormatExtended(3, map[string]interface{}{"msg_type": int64(1), "piece": int64(0)}, []byte("raw-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := ReadMessage(bytes.NewReader(m.Serialize()))
	id, dict, trailing, err := ParseExtended(parsed)
	if err != nil || id != 3 || dict["piece"] != int64(0) || string(trailing) != "raw-bytes" {
		t.Fatalf("got id=%d dict=%v trailing=%q err=%v", id, dict, trailing, err)
	}
}
