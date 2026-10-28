package metadata

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"strings"
	"testing"
	"time"

	"mini-bittorrent/bencode"
	"mini-bittorrent/seed"
)

// startSeeder serves a torrent whose info dict is big enough to span several
// metadata pieces (lots of piece hashes), with metadata exchange enabled.
func startSeeder(t *testing.T, withInfo bool) (*seed.Server, []byte) {
	t.Helper()
	data := make([]byte, 2500*256) // 2500 pieces of 256 bytes → ~50 KB of hashes
	rand.Read(data)
	var pieces []byte
	var hashes [][20]byte
	for off := 0; off < len(data); off += 256 {
		h := sha1.Sum(data[off : off+256])
		hashes = append(hashes, h)
		pieces = append(pieces, h[:]...)
	}
	info, _ := bencode.Encode(map[string]interface{}{
		"name": "big.bin", "length": int64(len(data)), "piece length": int64(256), "pieces": string(pieces),
	})
	s := &seed.Server{InfoHash: sha1.Sum(info), PieceHashes: hashes, PieceLength: 256,
		Length: int64(len(data)), Data: bytes.NewReader(data)}
	if withInfo {
		s.Info = info
	}
	if err := s.Verify(); err != nil {
		t.Fatal(err)
	}
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s, info
}

func TestFetchMultiPieceMetadata(t *testing.T) {
	s, info := startSeeder(t, true)
	if len(info) < 3*BlockSize {
		t.Fatalf("test metadata only %d bytes, want several pieces", len(info))
	}
	got, err := Fetch(context.Background(), s.Addr().String(), s.InfoHash, [20]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, info) {
		t.Fatal("metadata differs")
	}
}

func TestFetchFromPeerWithoutMetadata(t *testing.T) {
	s, _ := startSeeder(t, false)
	_, err := Fetch(context.Background(), s.Addr().String(), s.InfoHash, [20]byte{9})
	if err == nil || !strings.Contains(err.Error(), "extension") {
		t.Fatalf("expected an extension-protocol error, got %v", err)
	}
}

func TestFetchAnySkipsBadPeers(t *testing.T) {
	good, info := startSeeder(t, true)
	bad, _ := startSeeder(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// "127.0.0.1:1" refuses connections, "bad" has no metadata support.
	got, err := FetchAny(ctx, []string{"127.0.0.1:1", bad.Addr().String(), good.Addr().String()}, good.InfoHash, [20]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, info) {
		t.Fatal("metadata differs")
	}
}
