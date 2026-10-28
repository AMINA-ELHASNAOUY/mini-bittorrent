package main

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"mini-bittorrent/internal/localswarm"
	"mini-bittorrent/magnet"
	"mini-bittorrent/seed"
	"mini-bittorrent/torrentfile"
	"mini-bittorrent/tracker"
)

// startSeed serves data with metadata exchange enabled, like `mini-bittorrent seed` does.
func startSeed(t *testing.T, tf *torrentfile.TorrentFile, data []byte) *seed.Server {
	t.Helper()
	s := &seed.Server{InfoHash: tf.InfoHash, PieceHashes: tf.PieceHashes, PieceLength: tf.PieceLength,
		Length: tf.Length, Data: bytes.NewReader(data), Info: tf.InfoBytes}
	copy(s.PeerID[:], "-MB0001-testseeder00")
	if err := s.Verify(); err != nil {
		t.Fatal(err)
	}
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func inTempDir(t *testing.T) {
	t.Helper()
	wd, _ := os.Getwd()
	os.Chdir(t.TempDir())
	t.Cleanup(func() { os.Chdir(wd) })
}

func TestMagnetViaTracker(t *testing.T) {
	inTempDir(t)
	data := randomBytes(700_000)
	// A tracker with no built-in seeders; our seeder announces itself to it.
	sw, err := localswarm.Start("movie.mkv", data, 65536, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sw.Close()
	tf, _ := torrentfile.Parse(bytes.NewReader(sw.Torrent))
	s := startSeed(t, tf, data)
	if _, errs := tracker.AnnounceAll(tf.Trackers(), tracker.Request{InfoHash: tf.InfoHash, Port: s.Port()}, time.Second); len(errs) > 0 {
		t.Fatal(errs)
	}

	link := (&magnet.Link{InfoHash: tf.InfoHash, Name: "movie.mkv", Trackers: tf.Trackers()}).String()
	if err := runDownload([]string{"-nodht", link}); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile("movie.mkv")
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded file differs")
	}
	// The metadata is saved so the user can seed or resume without the magnet.
	saved, err := torrentfile.Open("movie.mkv.torrent")
	if err != nil || saved.InfoHash != tf.InfoHash {
		t.Fatalf("saved .torrent: %v", err)
	}
}

func TestMagnetViaDHTOnly(t *testing.T) {
	inTempDir(t)
	files := []torrentfile.FileData{
		{Path: []string{"a.txt"}, Data: randomBytes(1000)},
		{Path: []string{"sub", "b.bin"}, Data: randomBytes(200_000)},
	}
	raw, _ := torrentfile.CreateMulti("http://unused.invalid/announce", "folder", 32768, files)
	tf, _ := torrentfile.Parse(bytes.NewReader(raw))
	var all []byte
	for _, f := range files {
		all = append(all, f.Data...)
	}
	s := startSeed(t, tf, all)

	node, err := localswarm.StartDHTNode([]string{s.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	old := dhtBootstrap
	dhtBootstrap = []string{node.Addr()}
	defer func() { dhtBootstrap = old }()

	// No trackers at all: peers can only come from the DHT.
	link := (&magnet.Link{InfoHash: tf.InfoHash}).String()
	if err := runDownload([]string{"-o", "out", link}); err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		got, err := os.ReadFile(filepath.Join(append([]string{"out"}, f.Path...)...))
		if err != nil || !bytes.Equal(got, f.Data) {
			t.Fatalf("%v differs (%v)", f.Path, err)
		}
	}
}

func TestTorrentFallsBackToDHTWhenTrackersFail(t *testing.T) {
	inTempDir(t)
	data := randomBytes(100_000)
	raw, _ := torrentfile.Create("http://127.0.0.1:1/announce", "x.bin", 16384, data) // dead tracker
	os.WriteFile("x.torrent", raw, 0o644)
	tf, _ := torrentfile.Parse(bytes.NewReader(raw))
	s := startSeed(t, tf, data)

	node, _ := localswarm.StartDHTNode([]string{s.Addr().String()})
	defer node.Close()
	old := dhtBootstrap
	dhtBootstrap = []string{node.Addr()}
	defer func() { dhtBootstrap = old }()

	if err := runDownload([]string{"x.torrent"}); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile("x.bin")
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded file differs")
	}
}
