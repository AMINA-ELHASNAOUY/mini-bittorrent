// Package download fetches all pieces of a torrent from many peers in parallel,
// verifies each piece against its SHA-1 hash and writes it to its place in the file.
package download

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"mini-bittorrent/peer"
)

const (
	// BlockSize is the largest block most clients will serve per request (16 KiB).
	BlockSize = 16384
	// MaxBacklog is how many block requests we keep in flight per peer (pipelining).
	MaxBacklog = 5
	// maxEndgameCopies caps how many peers may fetch the same piece in endgame.
	maxEndgameCopies = 3
	// maxBadPieces drops a peer after it sends this many pieces that fail the hash check.
	maxBadPieces = 3
	pieceTimeout = 30 * time.Second
)

// Torrent is everything the downloader needs.
type Torrent struct {
	Peers       []string // "ip:port"
	PeerID      [20]byte
	InfoHash    [20]byte
	PieceHashes [][20]byte
	PieceLength int64
	Length      int64
	Logger      *log.Logger // optional; nil discards per-peer logs
	Completed   []bool      // optional; pieces already on disk and verified (resume)

	// StallTimeout ends the download with an error when the remaining pieces
	// are unavailable from every connected peer for this long (default 30s).
	StallTimeout time.Duration

	// Dial opens a connection to a peer (default: TCP with a 3 s timeout).
	// The CLI swaps in one that tries TCP and uTP at the same time.
	Dial func(addr string) (net.Conn, error)

	// Uploading while downloading. With Data set, pieces we have verified
	// are served to peers using tit-for-tat choking (see choker.go); with
	// Data nil the download never uploads.
	Data            io.ReaderAt   // where verified pieces are read back from (usually the same file as out)
	Listener        net.Listener  // optional: incoming peers are accepted here; closed when Download returns
	MaxUnchoked     int           // peers uploaded to at once, incl. 1 optimistic slot (default 4)
	RechokeInterval time.Duration // how often the choker re-ranks peers (default 10s)
	SnubTimeout     time.Duration // a peer that sends us nothing for this long loses its regular slot (default 60s)
	UploadLimit     int64         // total upload bytes per second; 0 = unlimited
	// Linger keeps serving peers that are still downloading from us after
	// our own download completes, until they lose interest or this much
	// time passes (default 0: return as soon as the last piece is written).
	Linger time.Duration
}

// maxInbound caps how many incoming peers we accept during a download.
const maxInbound = 30

// Progress is reported after every verified piece.
type Progress struct {
	Done, Total int
	Peers       int   // peers currently connected
	Uploaded    int64 // bytes uploaded to other peers so far
}

type pieceResult struct {
	index int
	buf   []byte
}

// pieceBounds returns the byte range [begin, end) of piece index in the file.
func (t *Torrent) pieceBounds(index int) (begin, end int64) {
	begin = int64(index) * t.PieceLength
	end = begin + t.PieceLength
	if end > t.Length {
		end = t.Length
	}
	return begin, end
}

func (t *Torrent) logf(format string, args ...interface{}) {
	if t.Logger != nil {
		t.Logger.Printf(format, args...)
	}
}

// Download fetches every missing piece and writes it into out at its offset.
// onProgress (optional) is called after each verified piece.
func (t *Torrent) Download(out io.WriterAt, onProgress func(Progress)) error {
	if t.Listener != nil {
		defer t.Listener.Close()
	}
	numPieces := len(t.PieceHashes)
	pk := newPicker(numPieces, t.Completed)
	if pk.finished() {
		return nil
	}
	if len(t.Peers) == 0 && t.Listener == nil {
		return errors.New("no peers to download from")
	}
	stallTimeout := t.StallTimeout
	if stallTimeout == 0 {
		stallTimeout = 30 * time.Second
	}

	// Count pieces already on disk before any worker starts: workers mark
	// pieces complete in the picker before their results reach this loop.
	done := numPieces - pk.remainingCount()

	sw := newSwarm(t, t.Completed)
	results := make(chan *pieceResult)
	stop := make(chan struct{})
	defer sw.close()
	defer close(stop)
	if sw.uploading() {
		interval := t.RechokeInterval
		if interval == 0 {
			interval = 10 * time.Second
		}
		go sw.chokeLoop(interval, stop)
	}

	// The download gives up when every outgoing worker is gone and nobody
	// can connect to us; with a listener, new peers may still arrive.
	var wg sync.WaitGroup
	for _, addr := range t.Peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			sent := sw.ourBitfield()
			c, err := t.connect(addr, sent)
			if err != nil {
				t.logf("peer %s: could not connect: %v", addr, err)
				return
			}
			t.runWorker(c, sent, sw, pk, results, stop)
		}(addr)
	}
	if t.Listener != nil {
		go t.acceptLoop(sw, pk, results, stop)
	}
	allWorkersGone := make(chan struct{})
	go func() {
		wg.Wait()
		close(allWorkersGone)
	}()

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var stalledSince time.Time

	// Workers mark pieces complete in the picker as soon as they verify them,
	// so no other peer is sent after a piece that is already on its way here.
	for done < numPieces {
		select {
		case res := <-results:
			begin, _ := t.pieceBounds(res.index)
			if _, err := out.WriteAt(res.buf, begin); err != nil {
				return fmt.Errorf("writing piece %d: %w", res.index, err)
			}
			sw.gotPiece(res.index)
			done++
			if onProgress != nil {
				onProgress(Progress{Done: done, Total: numPieces, Peers: sw.count(), Uploaded: sw.uploaded.Load()})
			}
		case <-allWorkersGone:
			allWorkersGone = nil // report once; inbound peers may still help
			if t.Listener == nil {
				return fmt.Errorf("all peers disconnected with %d of %d pieces done", done, numPieces)
			}
		case <-ticker.C:
			if !pk.stalled() {
				stalledSince = time.Time{}
			} else if stalledSince.IsZero() {
				stalledSince = time.Now()
			} else if time.Since(stalledSince) > stallTimeout {
				return fmt.Errorf("stalled: no connected peer has the remaining pieces (%d of %d done)", done, numPieces)
			}
		}
	}
	if sw.uploading() {
		sw.finished()
		if t.Linger > 0 {
			sw.linger(t.Linger)
		}
	}
	return nil
}

// connect opens an outgoing connection and runs the handshake.
func (t *Torrent) connect(addr string, ours peer.Bitfield) (*peer.Client, error) {
	dial := t.Dial
	if dial == nil {
		dial = func(addr string) (net.Conn, error) { return net.DialTimeout("tcp", addr, 3*time.Second) }
	}
	conn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	return peer.Start(conn, t.InfoHash, t.PeerID, len(t.PieceHashes), ours)
}

// acceptLoop turns incoming connections into workers, exactly like the ones we dial.
func (t *Torrent) acceptLoop(sw *swarm, pk *picker, results chan<- *pieceResult, stop <-chan struct{}) {
	var inbound sync.WaitGroup
	defer inbound.Wait()
	var n int
	var mu sync.Mutex
	for {
		conn, err := t.Listener.Accept()
		if err != nil {
			return
		}
		mu.Lock()
		full := n >= maxInbound
		if !full {
			n++
		}
		mu.Unlock()
		if full {
			conn.Close()
			continue
		}
		inbound.Add(1)
		go func() {
			defer inbound.Done()
			defer func() { mu.Lock(); n--; mu.Unlock() }()
			sent := sw.ourBitfield()
			c, err := peer.Accept(conn, t.InfoHash, t.PeerID, len(t.PieceHashes), sent)
			if err != nil {
				t.logf("incoming %s: %v", conn.RemoteAddr(), err)
				return
			}
			t.runWorker(c, sent, sw, pk, results, stop)
		}()
	}
}

// errPieceTaken means another worker finished this piece first (endgame).
var errPieceTaken = errors.New("piece completed by another peer")

// worker is one connected peer: it downloads from it and, when uploading
// is on, answers its requests on the same connection.
type worker struct {
	t  *Torrent
	c  *peer.Client
	pc *conn
	sw *swarm
	pk *picker
}

// runWorker downloads pieces from a single peer until the download is done,
// the peer misbehaves, or the connection fails. A piece that isn't finished
// is released back to the picker so another peer can take it.
func (t *Torrent) runWorker(c *peer.Client, sent peer.Bitfield, sw *swarm, pk *picker, results chan<- *pieceResult, stop <-chan struct{}) {
	defer c.Close()
	addr := c.Addr
	pc, err := sw.add(c, sent)
	if err != nil {
		if !errors.Is(err, errDuplicate) {
			t.logf("peer %s: %v", addr, err)
		}
		return
	}
	defer sw.remove(pc)
	pk.addPeer(c.Bitfield)
	defer func() { pk.removePeer(c.Bitfield) }()
	t.logf("peer %s: connected", addr)
	w := &worker{t: t, c: c, pc: pc, sw: sw, pk: pk}

	c.SendInterested()

	badPieces := 0
	for {
		select {
		case <-stop:
			return
		default:
		}

		index := pk.next(c.Bitfield)
		if index < 0 {
			// Nothing to fetch from this peer right now: listen for HAVE
			// messages (and serve its requests) for a moment, then ask again.
			if err := w.idle(time.Second); err != nil {
				t.logf("peer %s: %v", addr, err)
				return
			}
			continue
		}

		buf, err := w.downloadPiece(index)
		if errors.Is(err, errPieceTaken) {
			pk.release(index)
			continue
		}
		if err != nil {
			pk.release(index)
			t.logf("peer %s: piece %d failed: %v", addr, index, err)
			return
		}
		if sha1.Sum(buf) != t.PieceHashes[index] {
			pk.release(index)
			badPieces++
			t.logf("peer %s: piece %d failed the hash check (%d/%d)", addr, index, badPieces, maxBadPieces)
			if badPieces >= maxBadPieces {
				t.logf("peer %s: dropped for sending corrupt data", addr)
				return
			}
			continue
		}

		if !pk.complete(index) {
			continue // endgame: another peer delivered this piece first
		}
		// HAVE goes out to every peer once the piece is on disk (swarm.gotPiece).
		select {
		case results <- &pieceResult{index: index, buf: buf}:
		case <-stop:
			return
		}
	}
}

// idle reads messages from a peer we have nothing to ask for, so we notice
// when it announces new pieces. A read timeout is normal and not an error.
func (w *worker) idle(d time.Duration) error {
	w.c.Conn.SetReadDeadline(time.Now().Add(d))
	defer w.c.Conn.SetReadDeadline(time.Time{})
	for {
		msg, err := w.c.Read()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return nil
			}
			return err
		}
		if _, err := w.handle(msg); err != nil {
			return err
		}
		if msg != nil && (msg.ID == peer.MsgHave || msg.ID == peer.MsgBitfield) {
			return nil // it may have something for us now: ask the picker again
		}
	}
}

// handle applies every message that isn't a PIECE: choke state, HAVE and
// BITFIELD updates, and (when uploading) the peer's interest and requests.
// It reports whether msg was handled.
func (w *worker) handle(msg *peer.Message) (bool, error) {
	c, pk := w.c, w.pk
	if msg == nil {
		return true, nil // keep-alive
	}
	switch msg.ID {
	case peer.MsgInterested, peer.MsgNotInterested:
		w.sw.setInterested(w.pc, msg.ID == peer.MsgInterested)
	case peer.MsgRequest:
		if err := w.sw.serveRequest(w.pc, msg); err != nil {
			return true, err
		}
	case peer.MsgCancel:
		// Requests are answered as they arrive, so there is nothing queued to cancel.
	case peer.MsgUnchoke:
		c.Choked = false
	case peer.MsgChoke:
		c.Choked = true
	case peer.MsgHave:
		if idx, err := peer.ParseHave(msg); err == nil && !c.Bitfield.HasPiece(idx) {
			c.Bitfield.SetPiece(idx)
			pk.have(idx)
		}
	case peer.MsgBitfield: // some peers send it late; replace what we assumed
		if len(msg.Payload) == len(c.Bitfield) {
			pk.removePeer(c.Bitfield)
			copy(c.Bitfield, msg.Payload)
			pk.addPeer(c.Bitfield)
		}
	default:
		return false, nil
	}
	return true, nil
}

// downloadPiece fetches one piece block by block, keeping up to MaxBacklog
// requests in flight. If the peer chokes us, its pending requests are
// dropped (as the protocol says) and re-sent after the next unchoke. In
// endgame it gives up as soon as another worker completes the same piece.
func (w *worker) downloadPiece(index int) ([]byte, error) {
	t, c, pk := w.t, w.c, w.pk
	begin, end := t.pieceBounds(index)
	length := int(end - begin)
	buf := make([]byte, length)

	numBlocks := (length + BlockSize - 1) / BlockSize
	const (
		pending = iota
		requested
		received
	)
	state := make([]int, numBlocks)
	blockLen := func(b int) int {
		if b == numBlocks-1 {
			return length - b*BlockSize
		}
		return BlockSize
	}
	inFlight, got := 0, 0

	c.Conn.SetDeadline(time.Now().Add(pieceTimeout))
	defer c.Conn.SetDeadline(time.Time{})

	for got < numBlocks {
		if !c.Choked {
			for b := 0; b < numBlocks && inFlight < MaxBacklog; b++ {
				if state[b] != pending {
					continue
				}
				if err := c.SendRequest(index, b*BlockSize, blockLen(b)); err != nil {
					return nil, err
				}
				state[b] = requested
				inFlight++
			}
		}

		msg, err := c.Read()
		if err != nil {
			return nil, err
		}
		wasChoked := c.Choked
		handled, err := w.handle(msg)
		if err != nil {
			return nil, err
		}
		if handled {
			if c.Choked && !wasChoked { // requests are discarded on choke
				for b := range state {
					if state[b] == requested {
						state[b] = pending
					}
				}
				inFlight = 0
			}
			continue
		}
		if msg.ID != peer.MsgPiece || len(msg.Payload) < 8 {
			continue
		}
		if int(binary.BigEndian.Uint32(msg.Payload[0:4])) != index {
			continue // late block from a piece we abandoned
		}
		off := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
		b := off / BlockSize
		if off%BlockSize != 0 || b >= numBlocks || state[b] != requested || len(msg.Payload)-8 != blockLen(b) {
			continue // unrequested, duplicate or malformed block
		}
		if _, err := peer.ParsePiece(index, buf, msg); err != nil {
			return nil, err
		}
		state[b] = received
		inFlight--
		got++
		w.pc.receivedBlock(blockLen(b))

		if got < numBlocks && pk.isDone(index) {
			for b := range state {
				if state[b] == requested {
					c.SendCancel(index, b*BlockSize, blockLen(b))
				}
			}
			return nil, errPieceTaken
		}
	}
	return buf, nil
}

// CheckPieces hashes what is already on disk and reports which pieces are
// complete and valid, so an interrupted download can skip them.
func (t *Torrent) CheckPieces(r io.ReaderAt) ([]bool, int, error) {
	have := make([]bool, len(t.PieceHashes))
	count := 0
	for i, want := range t.PieceHashes {
		begin, end := t.pieceBounds(i)
		buf := make([]byte, end-begin)
		if _, err := r.ReadAt(buf, begin); err != nil && !errors.Is(err, io.EOF) {
			return nil, 0, err
		}
		if got := sha1.Sum(buf); bytes.Equal(got[:], want[:]) {
			have[i] = true
			count++
		}
	}
	return have, count, nil
}

// VerifyFile re-reads a finished download and checks every piece hash.
func (t *Torrent) VerifyFile(r io.ReaderAt) error {
	have, _, err := t.CheckPieces(r)
	if err != nil {
		return err
	}
	for i, ok := range have {
		if !ok {
			return fmt.Errorf("piece %d does not match its hash", i)
		}
	}
	return nil
}
