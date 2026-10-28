package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"mini-bittorrent/torrentfile"
)

func multiTorrent(t *testing.T) (*torrentfile.TorrentFile, []byte) {
	t.Helper()
	files := []torrentfile.FileData{
		{Path: []string{"a.txt"}, Data: []byte("AAAAA")},
		{Path: []string{"sub", "empty"}, Data: nil},
		{Path: []string{"sub", "b.txt"}, Data: []byte("BBB")},
		{Path: []string{"c.txt"}, Data: []byte("CCCCCCCC")},
	}
	raw, err := torrentfile.CreateMulti("http://t.test/", "root", 4, files)
	if err != nil {
		t.Fatal(err)
	}
	tf, err := torrentfile.Parse(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	return tf, []byte("AAAAABBBCCCCCCCC")
}

func TestWriteAcrossFileBoundaries(t *testing.T) {
	tf, all := multiTorrent(t)
	root := filepath.Join(t.TempDir(), "root")
	s, err := Open(root, tf)
	if err != nil {
		t.Fatal(err)
	}
	// Write in 4-byte pieces, which straddle a.txt|b.txt and b.txt|c.txt.
	for off := 0; off < len(all); off += 4 {
		if _, err := s.WriteAt(all[off:off+4], int64(off)); err != nil {
			t.Fatal(err)
		}
	}
	got := make([]byte, 6)
	if _, err := s.ReadAt(got, 3); err != nil || string(got) != "AABBBC" {
		t.Fatalf("ReadAt across files = %q, %v", got, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	for path, want := range map[string]string{"a.txt": "AAAAA", "sub/empty": "", "sub/b.txt": "BBB", "c.txt": "CCCCCCCC"} {
		data, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || string(data) != want {
			t.Errorf("%s = %q, %v; want %q", path, data, err, want)
		}
	}
}

func TestOutOfRange(t *testing.T) {
	tf, _ := multiTorrent(t)
	s, err := Open(filepath.Join(t.TempDir(), "root"), tf)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.WriteAt([]byte("xx"), 15); err == nil {
		t.Error("expected an error writing past the end")
	}
}

func TestSingleFileKeepsExistingData(t *testing.T) {
	raw, _ := torrentfile.Create("http://t.test/", "f.bin", 4, []byte("0123456789"))
	tf, _ := torrentfile.Parse(bytes.NewReader(raw))
	path := filepath.Join(t.TempDir(), "f.bin")
	os.WriteFile(path, []byte("0123"), 0o644) // partial download from an earlier run

	s, err := Open(path, tf)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got := make([]byte, 10)
	if _, err := s.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got[:4]) != "0123" || !bytes.Equal(got[4:], make([]byte, 6)) {
		t.Errorf("got %q", got)
	}
}
