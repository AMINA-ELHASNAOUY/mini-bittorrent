// Package download fetches all pieces of a torrent from many peers in parallel,
// verifies each piece against its SHA-1 hash and writes it to its place in the file.
package download

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"mini-bittorrent/peer"
)

const (
	// BlockSize is the largest block most clients will serve per request (16 KiB).
	BlockSize = 16384
	// MaxBacklog is how many block requests we keep in flight per peer (pipelining).
	MaxBacklog = 5
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
}

// Progress is reported after every verified piece.
type Progress struct {
	Done, Total int
	Peers       int // peers currently connected
}

type pieceWork struct {
	index  int
	hash   [20]byte
	length int
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

// Download fetches every piece and writes it into out at its file offset.
// onProgress (optional) is called after each verified piece.
func (t *Torrent) Download(out io.WriterAt, onProgress func(Progress)) error {
	numPieces := len(t.PieceHashes)
	workQueue := make(chan *pieceWork, numPieces)
	results := make(chan *pieceResult)
	done := 0
	for i, hash := range t.PieceHashes {
		if i < len(t.Completed) && t.Completed[i] {
			done++
			continue
		}
		begin, end := t.pieceBounds(i)
		workQueue <- &pieceWork{index: i, hash: hash, length: int(end - begin)}
	}
	if done == numPieces {
		return nil
	}
	if len(t.Peers) == 0 {
		return errors.New("no peers to download from")
	}

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		active int
	)
	stop := make(chan struct{})
	for _, addr := range t.Peers {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			t.runWorker(addr, workQueue, results, stop, func(delta int) {
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

	for done < numPieces {
		select {
		case res := <-results:
			begin, _ := t.pieceBounds(res.index)
			if _, err := out.WriteAt(res.buf, begin); err != nil {
				close(stop)
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
		}
	}
	close(stop)
	return nil
}

// runWorker downloads pieces from a single peer until the queue is empty,
// the peer misbehaves, or the connection fails. Unfinished work is always
// put back on the queue so another peer can pick it up.
func (t *Torrent) runWorker(addr string, workQueue chan *pieceWork, results chan<- *pieceResult, stop <-chan struct{}, track func(int)) {
	c, err := peer.Dial(addr, t.InfoHash, t.PeerID, len(t.PieceHashes))
	if err != nil {
		t.logf("peer %s: could not connect: %v", addr, err)
		return
	}
	defer c.Close()
	track(+1)
	defer track(-1)
	t.logf("peer %s: connected", addr)

	c.SendUnchoke()
	c.SendInterested()

	badPieces, misses := 0, 0
	for {
		var pw *pieceWork
		select {
		case pw = <-workQueue:
		case <-stop:
			return
		}

		if !c.Bitfield.HasPiece(pw.index) {
			workQueue <- pw
			misses++
			if misses > len(t.PieceHashes) { // cycled through everything: nothing left for this peer
				return
			}
			continue
		}
		misses = 0

		buf, err := t.downloadPiece(c, pw)
		if err != nil {
			t.logf("peer %s: piece %d failed: %v", addr, pw.index, err)
			workQueue <- pw
			return
		}
		if sha1.Sum(buf) != pw.hash {
			badPieces++
			t.logf("peer %s: piece %d failed the hash check (%d/%d)", addr, pw.index, badPieces, maxBadPieces)
			workQueue <- pw
			if badPieces >= maxBadPieces {
				t.logf("peer %s: dropped for sending corrupt data", addr)
				return
			}
			continue
		}

		c.SendHave(pw.index)
		select {
		case results <- &pieceResult{index: pw.index, buf: buf}:
		case <-stop:
			return
		}
	}
}

// downloadPiece requests a piece block by block, keeping up to MaxBacklog
// requests in flight while the peer has us unchoked.
func (t *Torrent) downloadPiece(c *peer.Client, pw *pieceWork) ([]byte, error) {
	buf := make([]byte, pw.length)
	downloaded, requested, backlog := 0, 0, 0

	c.Conn.SetDeadline(time.Now().Add(pieceTimeout))
	defer c.Conn.SetDeadline(time.Time{})

	for downloaded < pw.length {
		if !c.Choked {
			for backlog < MaxBacklog && requested < pw.length {
				size := BlockSize
				if pw.length-requested < size {
					size = pw.length - requested
				}
				if err := c.SendRequest(pw.index, requested, size); err != nil {
					return nil, err
				}
				backlog++
				requested += size
			}
		}

		msg, err := c.Read()
		if err != nil {
			return nil, err
		}
		if msg == nil { // keep-alive
			continue
		}
		switch msg.ID {
		case peer.MsgUnchoke:
			c.Choked = false
		case peer.MsgChoke:
			c.Choked = true
		case peer.MsgHave:
			if idx, err := peer.ParseHave(msg); err == nil {
				c.Bitfield.SetPiece(idx)
			}
		case peer.MsgPiece:
			n, err := peer.ParsePiece(pw.index, buf, msg)
			if err != nil {
				return nil, err
			}
			downloaded += n
			backlog--
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
