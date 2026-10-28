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

func TestParseMultiFile(t *testing.T) {
	raw, err := CreateMulti("http://t.test/announce", "album", 16, []FileData{
		{Path: []string{"cd1", "01.flac"}, Data: []byte("0123456789")},
		{Path: []string{"empty.txt"}, Data: nil},
		{Path: []string{"cover.jpg"}, Data: []byte("abcdefghijklmnopqrstuv")},
	})
	if err != nil {
		t.Fatal(err)
	}
	tf, err := Parse(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if !tf.MultiFile || tf.Name != "album" || len(tf.Files) != 3 || tf.Length != 32 || len(tf.PieceHashes) != 2 {
		t.Fatalf("got %+v", tf)
	}
	if tf.Files[0].Path[1] != "01.flac" || tf.Files[2].Offset != 10 || tf.Files[2].Length != 22 {
		t.Errorf("bad file layout: %+v", tf.Files)
	}
}

func TestParseRejectsUnsafePaths(t *testing.T) {
	for _, path := range [][]string{{"..", "etc", "passwd"}, {"a/b"}, {""}, {"ok", ".."}} {
		raw, _ := CreateMulti("http://t.test/", "root", 16, []FileData{{Path: path, Data: []byte("x")}})
		if _, err := Parse(bytes.NewReader(raw)); err == nil {
			t.Errorf("path %q: expected an error", path)
		}
	}
	raw, _ := Create("http://t.test/", "../evil", 16, []byte("x"))
	if _, err := Parse(bytes.NewReader(raw)); err == nil {
		t.Error("expected an error for a name with a separator")
	}
}

func TestTrackersFromAnnounceList(t *testing.T) {
	raw, _ := bencode.Encode(map[string]interface{}{
		"announce": "http://a.test/announce",
		"announce-list": []interface{}{
			[]interface{}{"udp://b.test:80", "http://a.test/announce"},
			[]interface{}{},
			[]interface{}{"http://c.test/announce"},
		},
		"info": map[string]interface{}{
			"name": "f", "length": int64(1), "piece length": int64(16), "pieces": strings.Repeat("x", 20),
		},
	})
	tf, err := Parse(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(tf.Trackers(), " ")
	want := "udp://b.test:80 http://a.test/announce http://c.test/announce"
	if got != want {
		t.Errorf("Trackers() = %q, want %q", got, want)
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

func TestFromInfo(t *testing.T) {
	info := map[string]interface{}{
		"name": "a.bin", "length": int64(20), "piece length": int64(16), "pieces": strings.Repeat("y", 40),
	}
	infoBytes, _ := bencode.Encode(info)
	hash := sha1.Sum(infoBytes)

	tf, raw, err := FromInfo(infoBytes, hash, []string{"udp://t.test:80", "http://t2.test/"})
	if err != nil {
		t.Fatal(err)
	}
	if tf.InfoHash != hash || tf.Name != "a.bin" || len(tf.Trackers()) != 2 {
		t.Fatalf("got %+v", tf)
	}
	again, err := Parse(bytes.NewReader(raw))
	if err != nil || again.InfoHash != hash {
		t.Fatalf("saved .torrent doesn't round-trip: %v", err)
	}
	if _, _, err := FromInfo(infoBytes, [20]byte{1}, nil); err == nil {
		t.Error("expected an error for metadata that doesn't match the hash")
	}
	if tf, _, err := FromInfo(infoBytes, hash, nil); err != nil || len(tf.Trackers()) != 0 {
		t.Errorf("trackerless metadata should parse: %v", err)
	}
}
