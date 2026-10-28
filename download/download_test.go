package download

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"mini-bittorrent/internal/localswarm"
	"mini-bittorrent/storage"
	"mini-bittorrent/torrentfile"
	"mini-bittorrent/tracker"
)

// memFile is an in-memory io.WriterAt / io.ReaderAt.
type memFile struct{ buf []byte }

func (m *memFile) WriteAt(p []byte, off int64) (int, error) { return copy(m.buf[off:], p), nil }
func (m *memFile) ReadAt(p []byte, off int64) (int, error)  { return copy(p, m.buf[off:]), nil }

func randomData(t *testing.T, n int) []byte {
	t.Helper()
	data := make([]byte, n)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	return data
}

// setup starts a local swarm and returns a Torrent wired to its peers via the tracker.
func setup(t *testing.T, data []byte, pieceLength int, specs []localswarm.SeederSpec) *Torrent {
	t.Helper()
	sw, err := localswarm.Start("file.bin", data, pieceLength, specs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sw.Close)

	tf, err := torrentfile.Parse(bytes.NewReader(sw.Torrent))
	if err != nil {
		t.Fatal(err)
	}
	var peerID [20]byte
	copy(peerID[:], "-MB0001-testtesttest")
	res, err := tracker.Announce(nil, tracker.Request{
		Announce: tf.Announce, InfoHash: tf.InfoHash, PeerID: peerID, Port: 6881, Left: tf.Length,
	})
	if err != nil {
		t.Fatal(err)
	}
	tor := &Torrent{PeerID: peerID, InfoHash: tf.InfoHash, PieceHashes: tf.PieceHashes,
		PieceLength: tf.PieceLength, Length: tf.Length}
	for _, p := range res.Peers {
		tor.Peers = append(tor.Peers, p.String())
	}
	return tor
}

func TestDownloadFromSingleSeeder(t *testing.T) {
	// Uneven size: the last piece and the last block are both short.
	data := randomData(t, 3*65536+12345)
	tor := setup(t, data, 65536, []localswarm.SeederSpec{{}})

	out := &memFile{buf: make([]byte, len(data))}
	var calls int32
	if err := tor.Download(out, func(Progress) { atomic.AddInt32(&calls, 1) }); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.buf, data) {
		t.Fatal("downloaded bytes differ from the original")
	}
	if calls != int32(len(tor.PieceHashes)) {
		t.Errorf("progress called %d times, want %d", calls, len(tor.PieceHashes))
	}
}

func TestDownloadSplitAcrossPartialSeeders(t *testing.T) {
	// Neither seeder has the whole file: even pieces on one, odd on the other.
	data := randomData(t, 10*32768)
	tor := setup(t, data, 32768, []localswarm.SeederSpec{
		{Has: func(i int) bool { return i%2 == 0 }},
		{Has: func(i int) bool { return i%2 == 1 }},
	})

	out := &memFile{buf: make([]byte, len(data))}
	if err := tor.Download(out, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.buf, data) {
		t.Fatal("downloaded bytes differ from the original")
	}
}

func TestCorruptPeerIsDroppedAndPiecesRetried(t *testing.T) {
	// One honest seeder and two that corrupt every block. Every bad piece must
	// fail the SHA-1 check, go back on the queue and be fetched from the honest peer.
	data := randomData(t, 20*16384)
	tor := setup(t, data, 16384, []localswarm.SeederSpec{{Corrupt: true}, {}, {Corrupt: true}})

	out := &memFile{buf: make([]byte, len(data))}
	if err := tor.Download(out, nil); err != nil {
		t.Fatal(err)
	}
	if err := tor.VerifyFile(out); err != nil {
		t.Fatal(err)
	}
}

func TestOnlyCorruptPeersFails(t *testing.T) {
	data := randomData(t, 8*16384)
	tor := setup(t, data, 16384, []localswarm.SeederSpec{{Corrupt: true}})

	err := tor.Download(&memFile{buf: make([]byte, len(data))}, nil)
	if err == nil {
		t.Fatal("expected an error when every peer sends corrupt data")
	}
}

func TestDownloadToDisk(t *testing.T) {
	data := randomData(t, 5*16384+1)
	tor := setup(t, data, 16384, []localswarm.SeederSpec{{}, {}})

	path := filepath.Join(t.TempDir(), "out.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := tor.Download(f, nil); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, data) {
		t.Fatal("file on disk differs from the original")
	}
}

func TestNoPeers(t *testing.T) {
	tor := &Torrent{PieceHashes: make([][20]byte, 1), PieceLength: 1, Length: 1}
	if err := tor.Download(&memFile{buf: make([]byte, 1)}, nil); err == nil {
		t.Fatal("expected an error with no peers")
	}
}

func TestResumeSkipsVerifiedPieces(t *testing.T) {
	data := randomData(t, 8*16384)
	tor := setup(t, data, 16384, []localswarm.SeederSpec{{}})

	// Simulate an interrupted run: first half on disk, one of those pieces damaged.
	out := &memFile{buf: make([]byte, len(data))}
	copy(out.buf, data[:4*16384])
	out.buf[2*16384+7] ^= 0xFF

	have, count, err := tor.CheckPieces(out)
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 || !have[0] || !have[1] || have[2] || !have[3] || have[4] {
		t.Fatalf("CheckPieces = %v (%d)", have, count)
	}

	tor.Completed = have
	var fetched int32
	if err := tor.Download(out, func(Progress) { atomic.AddInt32(&fetched, 1) }); err != nil {
		t.Fatal(err)
	}
	if fetched != 5 {
		t.Errorf("downloaded %d pieces, want 5 (3 were already verified)", fetched)
	}
	if !bytes.Equal(out.buf, data) {
		t.Fatal("resumed file differs from the original")
	}
}

func TestMultiFileDownload(t *testing.T) {
	files := []torrentfile.FileData{
		{Path: []string{"README.txt"}, Data: randomData(t, 1000)},
		{Path: []string{"data", "part1.bin"}, Data: randomData(t, 50000)},
		{Path: []string{"data", "part2.bin"}, Data: randomData(t, 30001)},
	}
	sw, err := localswarm.StartMulti("dataset", files, 16384, []localswarm.SeederSpec{{}, {}})
	if err != nil {
		t.Fatal(err)
	}
	defer sw.Close()
	tf, err := torrentfile.Parse(bytes.NewReader(sw.Torrent))
	if err != nil {
		t.Fatal(err)
	}

	root := filepath.Join(t.TempDir(), "dataset")
	st, err := storage.Open(root, tf)
	if err != nil {
		t.Fatal(err)
	}
	tor := &Torrent{InfoHash: tf.InfoHash, PieceHashes: tf.PieceHashes, PieceLength: tf.PieceLength, Length: tf.Length}
	for _, s := range sw.Seeders {
		tor.Peers = append(tor.Peers, s.Addr())
	}
	if err := tor.Download(st, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		got, err := os.ReadFile(filepath.Join(append([]string{root}, f.Path...)...))
		if err != nil || !bytes.Equal(got, f.Data) {
			t.Errorf("%v differs from the original (err %v)", f.Path, err)
		}
	}
}

func TestStallsWhenPiecesAreMissingFromSwarm(t *testing.T) {
	// The only seeder lacks piece 3, so the download can never finish.
	data := randomData(t, 6*16384)
	tor := setup(t, data, 16384, []localswarm.SeederSpec{{Has: func(i int) bool { return i != 3 }}})
	tor.StallTimeout = 300 * time.Millisecond

	start := time.Now()
	err := tor.Download(&memFile{buf: make([]byte, len(data))}, nil)
	if err == nil {
		t.Fatal("expected a stall error")
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("took %s to detect the stall", time.Since(start))
	}
}

func TestPeerThatChokesMidPiece(t *testing.T) {
	// The seeder chokes us every 3 blocks and drops our pending requests,
	// so the downloader must re-request them after each unchoke.
	data := randomData(t, 4*65536+5000)
	tor := setup(t, data, 65536, []localswarm.SeederSpec{{ChokeEvery: 3}})

	out := &memFile{buf: make([]byte, len(data))}
	if err := tor.Download(out, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.buf, data) {
		t.Fatal("downloaded bytes differ from the original")
	}
}

func TestEndgameFinishesDespiteSlowPeer(t *testing.T) {
	// One very slow seeder and one fast one. Without endgame the last piece
	// could sit on the slow peer for seconds; with it the fast peer takes over.
	data := randomData(t, 4*32768)
	tor := setup(t, data, 32768, []localswarm.SeederSpec{
		{Latency: 400 * time.Millisecond},
		{},
	})

	start := time.Now()
	out := &memFile{buf: make([]byte, len(data))}
	if err := tor.Download(out, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.buf, data) {
		t.Fatal("downloaded bytes differ from the original")
	}
	// The slow peer alone would need 2 blocks × 400ms for its piece.
	if elapsed := time.Since(start); elapsed > 700*time.Millisecond {
		t.Errorf("took %s; endgame should let the fast peer finish the slow peer's piece", elapsed)
	}
}
