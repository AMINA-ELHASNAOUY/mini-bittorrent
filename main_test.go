package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"os"
	"path/filepath"
	"testing"
	"time"

	"mini-bittorrent/bencode"
	"mini-bittorrent/dht"
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

func TestSeedTrackerlessThenDownloadByMagnetOverDHT(t *testing.T) {
	inTempDir(t)
	// A small local DHT: 6 full nodes bootstrapped off the first one.
	var nodes []*dht.Client
	for i := 0; i < 6; i++ {
		n, err := dht.NewNode("127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer n.Close()
		nodes = append(nodes, n)
	}
	boot := []string{nodes[0].Addr().String()}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, n := range nodes[1:] {
		n.Bootstrap(ctx, boot)
	}
	old := dhtBootstrap
	dhtBootstrap = boot
	defer func() { dhtBootstrap = old }()

	// A torrent with no trackers at all, seeded with the real `seed` code path.
	data := randomBytes(300_000)
	os.WriteFile("song.flac", data, 0o644)
	info, _ := bencode.Encode(map[string]interface{}{
		"name": "song.flac", "length": int64(len(data)), "piece length": int64(65536),
		"pieces": piecesOf(data, 65536),
	})
	raw, _ := bencode.Encode(map[string]interface{}{"info": decode(t, info)})
	tf, err := torrentfile.Parse(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- seedUntil(tf, "song.flac", seedOptions{}, stop) }()
	defer func() { close(stop); <-done }()

	// Wait until the seeder's DHT announce has landed somewhere.
	reader, _ := dht.New("127.0.0.1:0")
	defer reader.Close()
	for {
		if peers, _ := reader.GetPeers(ctx, tf.InfoHash, boot, 1); len(peers) > 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("seeder never appeared in the DHT")
		}
		time.Sleep(100 * time.Millisecond)
	}

	os.Mkdir("dl", 0o755)
	link := (&magnet.Link{InfoHash: tf.InfoHash}).String()
	if err := runDownload([]string{"-o", "dl/song.flac", link}); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile("dl/song.flac")
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded file differs")
	}
}

func piecesOf(data []byte, n int) string {
	var out []byte
	for off := 0; off < len(data); off += n {
		end := off + n
		if end > len(data) {
			end = len(data)
		}
		h := sha1.Sum(data[off:end])
		out = append(out, h[:]...)
	}
	return string(out)
}

func decode(t *testing.T, b []byte) interface{} {
	t.Helper()
	v, err := bencode.Decode(bufio.NewReader(bytes.NewReader(b)))
	if err != nil {
		t.Fatal(err)
	}
	return v
}
