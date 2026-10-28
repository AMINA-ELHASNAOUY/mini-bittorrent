package torrentfile

import (
	"bytes"
	"crypto/sha1"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mini-bittorrent/bencode"
)

func TestCreateAndParse(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefghij"), 10) // 100 bytes -> 4 pieces of 32
	raw, err := Create("http://tracker.test/announce", "hello.txt", 32, data)
	if err != nil {
		t.Fatal(err)
	}

	tf, err := Parse(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if tf.Announce != "http://tracker.test/announce" || tf.Name != "hello.txt" {
		t.Errorf("got announce=%q name=%q", tf.Announce, tf.Name)
	}
	if tf.Length != 100 || tf.PieceLength != 32 || len(tf.PieceHashes) != 4 {
		t.Fatalf("got length=%d pieceLength=%d pieces=%d", tf.Length, tf.PieceLength, len(tf.PieceHashes))
	}
	if tf.PieceHashes[3] != sha1.Sum(data[96:]) {
		t.Errorf("last piece hash mismatch")
	}
}

func TestInfoHashMatchesRawInfoBytes(t *testing.T) {
	// The info hash must be the SHA-1 of the exact bencoded info dict.
	info := map[string]interface{}{
		"length": int64(5), "name": "a", "piece length": int64(16384), "pieces": strings.Repeat("x", 20),
	}
	infoBytes, _ := bencode.Encode(info)
	raw := append([]byte("d8:announce14:http://t.test/4:info"), infoBytes...)
	raw = append(raw, 'e')

	tf, err := Parse(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if tf.InfoHash != sha1.Sum(infoBytes) {
		t.Errorf("info hash mismatch")
	}
}

func TestOpenFromDisk(t *testing.T) {
	raw, _ := Create("http://tracker.test/announce", "f.bin", 16, []byte("0123456789abcdefXYZ"))
	path := filepath.Join(t.TempDir(), "f.torrent")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	tf, err := Open(path)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	if len(tf.PieceHashes) != 2 {
		t.Errorf("want 2 pieces, got %d", len(tf.PieceHashes))
	}
}

func TestParseRejectsMultiFile(t *testing.T) {
	raw, _ := bencode.Encode(map[string]interface{}{
		"announce": "http://t.test/",
		"info": map[string]interface{}{
			"name": "dir", "piece length": int64(16), "pieces": strings.Repeat("x", 20),
			"files": []interface{}{},
		},
	})
	if _, err := Parse(bytes.NewReader(raw)); err == nil {
		t.Fatal("expected an error for a multi-file torrent")
	}
}

func TestParseRejectsWrongPieceCount(t *testing.T) {
	raw, _ := bencode.Encode(map[string]interface{}{
		"announce": "http://t.test/",
		"info": map[string]interface{}{
			"name": "f", "length": int64(100), "piece length": int64(16), "pieces": strings.Repeat("x", 20),
		},
	})
	if _, err := Parse(bytes.NewReader(raw)); err == nil {
		t.Fatal("expected an error when piece hashes don't cover the file")
	}
}
