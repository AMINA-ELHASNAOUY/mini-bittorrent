package download

import (
	"bytes"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mini-bittorrent/internal/localswarm"
	"mini-bittorrent/peer"
	"mini-bittorrent/torrentfile"
)

func TestSelectUnchokedRewardsFastestUploaders(t *testing.T) {
	slow := &candidate{rate: 10}
	fast := &candidate{rate: 900}
	medium := &candidate{rate: 400}
	idle := &candidate{rate: 0}
	cands := []*candidate{slow, fast, medium, idle}

	// 3 slots: 2 regular + 1 optimistic. The optimistic pick is forced to the
	// first choked candidate so the result is deterministic.
	set, opt := selectUnchoked(cands, 3, nil, true, false, func(int) int { return 0 })
	if !set[fast] || !set[medium] {
		t.Fatalf("the two fastest uploaders should get the regular slots: %v", set)
	}
	if opt != slow || !set[slow] {
		t.Fatalf("optimistic slot = %v, want the first choked peer", opt)
	}
	if set[idle] || len(set) != 3 {
		t.Fatalf("unchoked %d peers, want exactly 3 (idle peer must stay choked)", len(set))
	}
}

func TestSelectUnchokedSnubbedPeerLosesRegularSlot(t *testing.T) {
	// The fastest peer last round has since gone silent: anti-snubbing
	// takes its regular slot away even though its rate is the highest.
	snubber := &candidate{rate: 5000, snubbed: true}
	a := &candidate{rate: 20}
	b := &candidate{rate: 10}
	c := &candidate{rate: 5}
	pickLast := func(n int) int { return n - 1 }
	set, opt := selectUnchoked([]*candidate{snubber, a, b, c}, 3, nil, true, false, pickLast)
	if !set[a] || !set[b] {
		t.Fatalf("regular slots should go to the best non-snubbed peers: %v", set)
	}
	if opt == snubber {
		t.Fatal("random pick was forced to the last choked peer, not the snubber")
	}
	if set[snubber] {
		t.Fatal("a snubbed peer must not keep a regular slot")
	}
}

func TestSelectUnchokedOptimisticRotatesAndFillsFreeSlots(t *testing.T) {
	cands := make([]*candidate, 6)
	for i := range cands {
		cands[i] = &candidate{rate: int64(100 - i)}
	}
	rng := rand.New(rand.NewSource(1))
	set, opt := selectUnchoked(cands, 4, nil, true, false, rng.Intn)
	if len(set) != 4 {
		t.Fatalf("unchoked %d, want 4", len(set))
	}
	// Without rotation the optimistic peer survives a round, even if a
	// faster peer would rank above it.
	set2, opt2 := selectUnchoked(cands, 4, opt, true, false, rng.Intn)
	if opt2 != opt || !set2[opt] {
		t.Fatal("optimistic peer was dropped before its rotation")
	}
	// With rotation it is redrawn among the choked peers.
	seen := map[*candidate]bool{}
	for i := 0; i < 50; i++ {
		_, o := selectUnchoked(cands, 4, opt, true, true, rng.Intn)
		seen[o] = true
	}
	if len(seen) < 2 {
		t.Fatal("rotation never picked a different optimistic peer")
	}
	// Two interested peers and four slots: both get served right away.
	few := []*candidate{{}, {}}
	set3, _ := selectUnchoked(few, 4, nil, false, false, rng.Intn)
	if len(set3) != 2 {
		t.Fatalf("free slots not filled: %d of 2 unchoked", len(set3))
	}
}

// pipePeer returns a registered conn whose messages are read and dropped.
func pipePeer(t *testing.T, sw *swarm, name string) *conn {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() { a.Close(); b.Close() })
	go io.Copy(io.Discard, b)
	var id [20]byte
	copy(id[:], name)
	pc, err := sw.add(&peer.Client{Conn: a, Addr: name, PeerID: id}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pc.interested = true
	return pc
}

func TestRechokeIsTitForTat(t *testing.T) {
	tor := &Torrent{PieceHashes: make([][20]byte, 4), PieceLength: 16384, Length: 65536,
		Data: &memFile{buf: make([]byte, 65536)}, MaxUnchoked: 2}
	sw := newSwarm(tor, nil)
	generous := pipePeer(t, sw, "generous")
	stingy := pipePeer(t, sw, "stingy")
	silent := pipePeer(t, sw, "silent")

	generous.receivedBlock(10 * 16384)
	stingy.receivedBlock(16384)
	silent.lastBlock.Store(time.Now().Add(-2 * time.Minute).UnixNano()) // snubbing us

	// 1 regular slot + 1 optimistic: whatever the optimistic draw, the peer
	// that uploaded the most to us must be unchoked, every time.
	for i := 0; i < 20; i++ {
		generous.receivedBlock(16384)
		sw.rechoke(true)
		sw.mu.Lock()
		gotGenerous := !generous.choked
		unchoked := 0
		for pc := range sw.conns {
			if !pc.choked {
				unchoked++
			}
		}
		opt := sw.choker.optimistic
		sw.mu.Unlock()
		if !gotGenerous {
			t.Fatalf("round %d: the peer uploading to us fastest is choked", i)
		}
		if unchoked > 2 {
			t.Fatalf("round %d: %d peers unchoked, max is 2", i, unchoked)
		}
		if opt == generous {
			t.Fatalf("round %d: the top uploader was put in the optimistic slot", i)
		}
	}
}

// TestLeechersTradePieces is the case tit-for-tat exists for: two clients
// that are both still downloading, each connected to a seeder that has only
// half the file. Neither can finish alone; they only finish by uploading to
// each other while downloading (and whoever finishes first keeps serving
// the other until it is done: Linger). B never dials A: A's connection arrives on
// B's listener, so both the incoming and outgoing paths are covered.
func TestLeechersTradePieces(t *testing.T) {
	data := randomData(t, 20*16384+777)
	const pieceLen = 16384
	even := func(i int) bool { return i%2 == 0 }
	odd := func(i int) bool { return i%2 == 1 }
	sw, err := localswarm.Start("file.bin", data, pieceLen, []localswarm.SeederSpec{{Has: even}, {Has: odd}})
	if err != nil {
		t.Fatal(err)
	}
	defer sw.Close()
	tf, err := torrentfile.Parse(bytes.NewReader(sw.Torrent))
	if err != nil {
		t.Fatal(err)
	}

	newLeecher := func(name string) (*Torrent, *memFile) {
		out := &memFile{buf: make([]byte, len(data))}
		tor := &Torrent{InfoHash: tf.InfoHash, PieceHashes: tf.PieceHashes, PieceLength: tf.PieceLength,
			Length: tf.Length, Data: out, RechokeInterval: 100 * time.Millisecond, StallTimeout: 10 * time.Second, Linger: 10 * time.Second}
		copy(tor.PeerID[:], "-MB0001-"+name)
		return tor, out
	}
	a, outA := newLeecher("leecher-a---")
	b, outB := newLeecher("leecher-b---")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b.Listener = ln
	a.Peers = []string{sw.Seeders[0].Addr(), ln.Addr().String()}
	b.Peers = []string{sw.Seeders[1].Addr()}

	var wg sync.WaitGroup
	var errA, errB error
	var upA, upB atomic.Int64
	wg.Add(2)
	go func() {
		defer wg.Done()
		errA = a.Download(outA, func(p Progress) { upA.Store(p.Uploaded) })
	}()
	go func() {
		defer wg.Done()
		errB = b.Download(outB, func(p Progress) { upB.Store(p.Uploaded) })
	}()
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatalf("A: %v, B: %v", errA, errB)
	}
	if !bytes.Equal(outA.buf, data) || !bytes.Equal(outB.buf, data) {
		t.Fatal("a leecher ended up with the wrong data")
	}
	if upA.Load() == 0 || upB.Load() == 0 {
		t.Fatalf("both leechers must have uploaded (A %d bytes, B %d bytes)", upA.Load(), upB.Load())
	}
}

// Without Data the downloader stays download-only: it never unchokes anyone.
func TestNoUploadWithoutData(t *testing.T) {
	tor := &Torrent{PieceHashes: make([][20]byte, 4), PieceLength: 16384, Length: 65536}
	sw := newSwarm(tor, []bool{true, true, true, true})
	pc := pipePeer(t, sw, "someone")
	sw.rechoke(true)
	if !pc.choked {
		t.Fatal("download-only mode unchoked a peer")
	}
	if sw.ourBitfield() != nil {
		t.Fatal("download-only mode advertised pieces")
	}
}
