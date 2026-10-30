package seed

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	mrand "math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"mini-bittorrent/download"
	"mini-bittorrent/peer"
	"mini-bittorrent/utp"
)

type memFile struct{ buf []byte }

func (m *memFile) WriteAt(p []byte, off int64) (int, error) { return copy(m.buf[off:], p), nil }
func (m *memFile) ReadAt(p []byte, off int64) (int, error)  { return copy(p, m.buf[off:]), nil }

func newServer(t *testing.T, data []byte, pieceLen int64) *Server {
	t.Helper()
	var hashes [][20]byte
	for off := int64(0); off < int64(len(data)); off += pieceLen {
		end := off + pieceLen
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		hashes = append(hashes, sha1.Sum(data[off:end]))
	}
	s := &Server{PieceHashes: hashes, PieceLength: pieceLen, Length: int64(len(data)), Data: bytes.NewReader(data)}
	copy(s.InfoHash[:], "info-hash-for-tests!")
	copy(s.PeerID[:], "-MB0001-seedseedseed")
	return s
}

func TestDownloaderFetchesFromSeeder(t *testing.T) {
	data := make([]byte, 5*32768+777)
	rand.Read(data)
	s := newServer(t, data, 32768)
	if err := s.Verify(); err != nil {
		t.Fatal(err)
	}
	if s.Have() != 6 {
		t.Fatalf("Have = %d, want 6", s.Have())
	}
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tor := &download.Torrent{InfoHash: s.InfoHash, PieceHashes: s.PieceHashes, PieceLength: s.PieceLength,
		Length: s.Length, Peers: []string{s.Addr().String()}}
	copy(tor.PeerID[:], "-MB0001-leechleechle")
	out := &memFile{buf: make([]byte, len(data))}
	if err := tor.Download(out, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.buf, data) {
		t.Fatal("downloaded data differs")
	}
	if s.Uploaded() != int64(len(data)) {
		t.Errorf("Uploaded = %d, want %d", s.Uploaded(), len(data))
	}
}

func TestOnlyServesVerifiedPieces(t *testing.T) {
	data := make([]byte, 4*16384)
	rand.Read(data)
	s := newServer(t, data, 16384)
	damaged := append([]byte(nil), data...)
	damaged[16384+10] ^= 1 // piece 1 is corrupt on disk
	s.Data = bytes.NewReader(damaged)
	s.Verify()
	if s.Have() != 3 || s.bitfield.HasPiece(1) {
		t.Fatalf("bitfield %08b, want piece 1 missing", s.bitfield)
	}
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	c, err := peer.Dial(s.Addr().String(), s.InfoHash, [20]byte{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.Bitfield.HasPiece(1) || !c.Bitfield.HasPiece(0) {
		t.Fatalf("advertised bitfield %08b", c.Bitfield)
	}
	c.SendInterested()
	c.SendRequest(1, 0, 16384) // asking for a piece we never advertised
	c.Conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		msg, err := c.Read()
		if err != nil {
			return // the seeder hung up on us: correct
		}
		if msg != nil && msg.ID == peer.MsgPiece {
			t.Fatal("seeder served a piece it doesn't have")
		}
	}
}

func TestRejectsWrongInfoHash(t *testing.T) {
	s := newServer(t, []byte("hello world"), 16384)
	s.Verify()
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	conn, err := net.Dial("tcp", s.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var other [20]byte
	if _, err := peer.DoHandshake(conn, other, [20]byte{}); err == nil {
		t.Fatal("expected the seeder to refuse an unknown info hash")
	}
}

func TestListenRequiresVerify(t *testing.T) {
	s := newServer(t, []byte("x"), 16384)
	if err := s.Listen("127.0.0.1:0"); err == nil {
		t.Fatal("expected an error when Listen is called before Verify")
	}
}

// rawPeer connects, declares interest and records CHOKE/UNCHOKE messages.
type rawPeer struct {
	c        *peer.Client
	mu       sync.Mutex
	unchoked bool
	everUp   bool
}

func dialRaw(t *testing.T, s *Server) *rawPeer {
	t.Helper()
	c, err := peer.Dial(s.Addr().String(), s.InfoHash, [20]byte{byte(mrand.Int())}, len(s.PieceHashes))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	p := &rawPeer{c: c}
	c.SendInterested()
	go func() {
		for {
			msg, err := c.Read()
			if err != nil {
				return
			}
			if msg == nil {
				continue
			}
			p.mu.Lock()
			switch msg.ID {
			case peer.MsgUnchoke:
				p.unchoked, p.everUp = true, true
			case peer.MsgChoke:
				p.unchoked = false
			}
			p.mu.Unlock()
		}
	}()
	return p
}

func (p *rawPeer) state() (bool, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.unchoked, p.everUp
}

func TestChokerLimitsAndRotatesUnchokedPeers(t *testing.T) {
	data := make([]byte, 64*1024)
	rand.Read(data)
	s := newServer(t, data, 16384)
	s.MaxUnchoked = 2
	s.RechokeInterval = 30 * time.Millisecond
	s.Verify()
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var peers []*rawPeer
	for i := 0; i < 5; i++ {
		peers = append(peers, dialRaw(t, s))
	}

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		// Check the server's own view: the peers' views lag behind by however
		// long the CHOKE/UNCHOKE messages take to arrive.
		s.mu.Lock()
		up := 0
		for r := range s.conns {
			if !r.choked {
				up++
			}
		}
		s.mu.Unlock()
		if up > 2 {
			t.Fatalf("%d peers unchoked at once, limit is 2", up)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The optimistic slot rotates, so over time every peer gets a turn.
	for i, p := range peers {
		if _, ever := p.state(); !ever {
			t.Errorf("peer %d was never unchoked", i)
		}
	}
}

func TestNewPeerIsUnchokedWithoutWaitingForARound(t *testing.T) {
	s := newServer(t, []byte("hello world"), 16384)
	s.RechokeInterval = time.Hour // only event-driven rechokes can help
	s.Verify()
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p := dialRaw(t, s)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if u, _ := p.state(); u {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("an interested peer with a free slot should be unchoked right away")
}

func TestRateLimit(t *testing.T) {
	data := make([]byte, 600*1024)
	rand.Read(data)
	s := newServer(t, data, 65536)
	s.RateLimit = 400 * 1024 // 400 KiB/s: 600 KiB minus the burst takes > 1 s
	s.Verify()
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tor := &download.Torrent{InfoHash: s.InfoHash, PieceHashes: s.PieceHashes, PieceLength: s.PieceLength,
		Length: s.Length, Peers: []string{s.Addr().String()}}
	start := time.Now()
	if err := tor.Download(&memFile{buf: make([]byte, len(data))}, nil); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed < 1000*time.Millisecond {
		t.Errorf("600 KiB at 400 KiB/s took only %s", elapsed)
	}
	if elapsed > 4*time.Second {
		t.Errorf("rate limiting is far too slow: %s", elapsed)
	}
}

// The whole peer wire protocol over uTP: our seeder accepts on a uTP socket
// and our downloader dials over uTP only, with TCP never involved.
func TestDownloadOverUTP(t *testing.T) {
	data := make([]byte, 40*16384+999)
	rand.Read(data)
	s := newServer(t, data, 16384)
	if err := s.Verify(); err != nil {
		t.Fatal(err)
	}
	if err := s.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sock, err := utp.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.Serve(sock)

	client, err := utp.NewSocket("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	tor := &download.Torrent{InfoHash: s.InfoHash, PieceHashes: s.PieceHashes, PieceLength: s.PieceLength,
		Length: s.Length, Peers: []string{sock.Addr().String()},
		Dial: func(addr string) (net.Conn, error) { return client.Dial(addr, 3*time.Second) }}
	copy(tor.PeerID[:], "-MB0001-utputputputp")
	out := &memFile{buf: make([]byte, len(data))}
	if err := tor.Download(out, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.buf, data) {
		t.Fatal("downloaded data differs")
	}
	if s.Uploaded() < int64(len(data)) {
		t.Fatalf("seeder reports %d bytes uploaded, want at least %d", s.Uploaded(), len(data))
	}
}
