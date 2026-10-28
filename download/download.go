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
}

// Progress is reported after every verified piece.
type Progress struct {
	Done, Total int
	Peers       int // peers currently connected
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
	numPieces := len(t.PieceHashes)
	pk := newPicker(numPieces, t.Completed)
	if pk.finished() {
		return nil
	}
	if len(t.Peers) == 0 {
		return errors.New("no peers to download from")
	}
	stallTimeout := t.StallTimeout
	if stallTimeout == 0 {
		stallTimeout = 30 * time.Second
	}

	// Count pieces already on disk before any worker starts: workers mark
	// pieces complete in the picker before their results reach this loop.
	done := numPieces - pk.remainingCount()

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		active int
	)
	results := make(chan *pieceResult)
	stop := make(chan struct{})
	defer close(stop)
	for _, addr := range t.Peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			t.runWorker(addr, pk, results, stop, func(delta int) {
				mu.Lock()
				active += delta
				mu.Unlock()
			})
		}(addr)
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
			done++
			if onProgress != nil {
				mu.Lock()
				peers := active
				mu.Unlock()
				onProgress(Progress{Done: done, Total: numPieces, Peers: peers})
			}
		case <-allWorkersGone:
			return fmt.Errorf("all peers disconnected with %d of %d pieces done", done, numPieces)
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
	return nil
}

// errPieceTaken means another worker finished this piece first (endgame).
var errPieceTaken = errors.New("piece completed by another peer")

// runWorker downloads pieces from a single peer until the download is done,
// the peer misbehaves, or the connection fails. A piece that isn't finished
// is released back to the picker so another peer can take it.
func (t *Torrent) runWorker(addr string, pk *picker, results chan<- *pieceResult, stop <-chan struct{}, track func(int)) {
	c, err := peer.Dial(addr, t.InfoHash, t.PeerID, len(t.PieceHashes))
	if err != nil {
		t.logf("peer %s: could not connect: %v", addr, err)
		return
	}
	defer c.Close()
	pk.addPeer(c.Bitfield)
	defer func() { pk.removePeer(c.Bitfield) }()
	track(+1)
	defer track(-1)
	t.logf("peer %s: connected", addr)

	c.SendUnchoke()
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
			// messages for a moment, then ask the picker again.
			if err := t.idle(c, pk, time.Second); err != nil {
				return
			}
			continue
		}

		buf, err := t.downloadPiece(c, pk, index)
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
		c.SendHave(index)
		select {
		case results <- &pieceResult{index: index, buf: buf}:
		case <-stop:
			return
		}
	}
}

// idle reads messages from a peer we have nothing to ask for, so we notice
// when it announces new pieces. A read timeout is normal and not an error.
func (t *Torrent) idle(c *peer.Client, pk *picker, d time.Duration) error {
	c.Conn.SetReadDeadline(time.Now().Add(d))
	defer c.Conn.SetReadDeadline(time.Time{})
	for {
		msg, err := c.Read()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return nil
			}
			return err
		}
		handleStateMessage(c, pk, msg)
	}
}

// handleStateMessage applies CHOKE/UNCHOKE/HAVE. It reports whether msg was one of them.
func handleStateMessage(c *peer.Client, pk *picker, msg *peer.Message) bool {
	if msg == nil {
		return true // keep-alive
	}
	switch msg.ID {
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
		return false
	}
	return true
}

// downloadPiece fetches one piece block by block, keeping up to MaxBacklog
// requests in flight. If the peer chokes us, its pending requests are
// dropped (as the protocol says) and re-sent after the next unchoke. In
// endgame it gives up as soon as another worker completes the same piece.
func (t *Torrent) downloadPiece(c *peer.Client, pk *picker, index int) ([]byte, error) {
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
		if handleStateMessage(c, pk, msg) {
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
