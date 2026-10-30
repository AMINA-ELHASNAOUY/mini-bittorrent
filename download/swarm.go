package download

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"mini-bittorrent/internal/ratelimit"
	"mini-bittorrent/peer"
)

// maxRequestLen is the largest block we serve; bigger requests are a protocol violation.
const maxRequestLen = 128 << 10

// swarm tracks every connected peer during a download and, when uploading
// is enabled, serves our verified pieces back to them. Uploading while
// downloading is what makes tit-for-tat possible: peers that upload to us
// fastest get our upload slots in return (see choker.go).
type swarm struct {
	data        io.ReaderAt // nil: download only, never upload
	pieceLength int64
	length      int64
	numPieces   int
	limiter     *ratelimit.Limiter
	maxUnchoked int
	snubTimeout time.Duration
	uploaded    atomic.Int64

	mu         sync.Mutex
	have       peer.Bitfield // pieces written to disk and verified
	conns      map[*conn]struct{}
	ids        map[[20]byte]bool
	closed     bool
	choker     chokerState
	rechokeNow chan struct{}
}

// conn is one connected peer, as seen by the swarm.
type conn struct {
	c *peer.Client

	// Guarded by swarm.mu.
	interested bool // they want to download from us
	choked     bool // whether WE choke THEM
	lastDown   int64
	lastUp     int64

	downloaded atomic.Int64 // bytes of blocks they sent us
	uploaded   atomic.Int64 // bytes of blocks we sent them
	lastBlock  atomic.Int64 // unix nanos of the last block they sent us
}

func newSwarm(t *Torrent, completed []bool) *swarm {
	s := &swarm{
		data:        t.Data,
		pieceLength: t.PieceLength,
		length:      t.Length,
		numPieces:   len(t.PieceHashes),
		maxUnchoked: t.MaxUnchoked,
		snubTimeout: t.SnubTimeout,
		have:        make(peer.Bitfield, (len(t.PieceHashes)+7)/8),
		conns:       map[*conn]struct{}{},
		ids:         map[[20]byte]bool{t.PeerID: true}, // never connect to ourselves
		rechokeNow:  make(chan struct{}, 1),
	}
	if s.maxUnchoked == 0 {
		s.maxUnchoked = 4
	}
	if s.snubTimeout == 0 {
		s.snubTimeout = 60 * time.Second
	}
	if t.UploadLimit > 0 {
		s.limiter = ratelimit.New(t.UploadLimit)
	}
	for i, ok := range completed {
		if ok {
			s.have.SetPiece(i)
		}
	}
	return s
}

func (s *swarm) uploading() bool { return s.data != nil }

// ourBitfield is what we advertise in the handshake (nothing if we don't upload).
func (s *swarm) ourBitfield() peer.Bitfield {
	if !s.uploading() {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append(peer.Bitfield(nil), s.have...)
}

var errDuplicate = errors.New("already connected to this peer")

// add registers a peer; a second connection to the same peer ID is refused.
// sent is the bitfield we advertised in the handshake: pieces finished since
// then are announced with HAVE now, since gotPiece didn't know this peer yet.
func (s *swarm) add(c *peer.Client, sent peer.Bitfield) (*conn, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("download finished")
	}
	if s.ids[c.PeerID] {
		s.mu.Unlock()
		return nil, errDuplicate
	}
	s.ids[c.PeerID] = true
	pc := &conn{c: c, choked: true}
	pc.lastBlock.Store(time.Now().UnixNano())
	s.conns[pc] = struct{}{}
	var missed []int
	if s.uploading() {
		for i := 0; i < s.numPieces; i++ {
			if s.have.HasPiece(i) && !sent.HasPiece(i) {
				missed = append(missed, i)
			}
		}
	}
	s.mu.Unlock()
	for _, i := range missed {
		if err := c.SendHave(i); err != nil {
			s.remove(pc)
			return nil, err
		}
	}
	return pc, nil
}

func (s *swarm) remove(pc *conn) {
	s.mu.Lock()
	wasUnchoked := !pc.choked
	delete(s.conns, pc)
	delete(s.ids, pc.c.PeerID)
	s.mu.Unlock()
	if wasUnchoked {
		s.requestRechoke() // give its slot to someone else
	}
}

func (s *swarm) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// close disconnects everyone; workers notice and exit.
func (s *swarm) close() {
	s.mu.Lock()
	s.closed = true
	for pc := range s.conns {
		pc.c.Close()
	}
	s.mu.Unlock()
}

// gotPiece records a piece that is now on disk and tells every peer, so
// they can request it from us. HAVE is only sent once the data is written:
// announcing a piece we can't serve yet would be a lie.
func (s *swarm) gotPiece(index int) {
	s.mu.Lock()
	s.have.SetPiece(index)
	var all []*conn
	for pc := range s.conns {
		all = append(all, pc)
	}
	s.mu.Unlock()
	for _, pc := range all {
		go pc.c.SendHave(index) // a slow peer must not hold up the download
	}
}

// setInterested handles INTERESTED / NOT_INTERESTED from a peer.
func (s *swarm) setInterested(pc *conn, interested bool) {
	s.mu.Lock()
	changed := pc.interested != interested
	pc.interested = interested
	s.mu.Unlock()
	if changed {
		s.requestRechoke()
	}
}

// serveRequest answers a REQUEST. Requests from peers we choke are ignored,
// as the protocol says; malformed ones end the connection.
func (s *swarm) serveRequest(pc *conn, msg *peer.Message) error {
	if !s.uploading() {
		return nil
	}
	if len(msg.Payload) != 12 {
		return errors.New("malformed REQUEST")
	}
	index := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	begin := int64(binary.BigEndian.Uint32(msg.Payload[4:8]))
	length := int64(binary.BigEndian.Uint32(msg.Payload[8:12]))

	s.mu.Lock()
	choked, have := pc.choked, s.have.HasPiece(index)
	s.mu.Unlock()
	if choked {
		return nil
	}
	if !have {
		return fmt.Errorf("requested piece %d we don't have", index)
	}
	pieceStart := int64(index) * s.pieceLength
	pieceLen := s.pieceLength
	if pieceStart+pieceLen > s.length {
		pieceLen = s.length - pieceStart
	}
	if length <= 0 || length > maxRequestLen || begin+length > pieceLen {
		return fmt.Errorf("invalid request: piece %d [%d, +%d)", index, begin, length)
	}

	payload := make([]byte, 8+length)
	copy(payload[0:8], msg.Payload[0:8])
	if _, err := s.data.ReadAt(payload[8:], pieceStart+begin); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if s.limiter != nil {
		s.limiter.Wait(int(length))
	}
	if err := pc.c.Send(&peer.Message{ID: peer.MsgPiece, Payload: payload}); err != nil {
		return err
	}
	pc.uploaded.Add(length)
	s.uploaded.Add(length)
	return nil
}

// receivedBlock counts bytes a peer sent us: the tit-for-tat score.
func (pc *conn) receivedBlock(n int) {
	pc.downloaded.Add(int64(n))
	pc.lastBlock.Store(time.Now().UnixNano())
}

// finished tells every peer we no longer want anything from it.
func (s *swarm) finished() {
	s.mu.Lock()
	var all []*conn
	for pc := range s.conns {
		all = append(all, pc)
	}
	s.mu.Unlock()
	for _, pc := range all {
		pc.c.Send(&peer.Message{ID: peer.MsgNotInterested})
	}
}

// interestedPeers counts connected peers that still want our data.
func (s *swarm) interestedPeers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for pc := range s.conns {
		if pc.interested {
			n++
		}
	}
	return n
}

// linger keeps the workers serving until no connected peer is interested
// any more, or d passes. Hanging up the moment we finish would cut off
// peers that were halfway through downloading a piece from us.
func (s *swarm) linger(d time.Duration) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) && s.interestedPeers() > 0 {
		time.Sleep(50 * time.Millisecond)
	}
}
