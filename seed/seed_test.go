package seed

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"net"
	"testing"
	"time"

	"mini-bittorrent/download"
	"mini-bittorrent/peer"
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
