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
