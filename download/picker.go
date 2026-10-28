package download

import (
	"math/rand"
	"sync"

	"mini-bittorrent/peer"
)

// picker decides which piece each peer should fetch next.
//
// Rarest first: among the pieces a peer has, pick the one the fewest connected
// peers have. Rare pieces are the ones most likely to become unavailable if a
// peer leaves, so grabbing them early keeps the download from stalling.
//
// Endgame: once every missing piece is already being downloaded by someone,
// idle peers are allowed to fetch the same pieces in parallel. Whoever finishes
// first wins and the others cancel. This avoids waiting on one slow peer for
// the very last pieces.
type picker struct {
	mu           sync.Mutex
	done         []bool
	inProgress   []int // number of workers currently fetching each piece
	availability []int // number of connected peers that have each piece
	remaining    int
	rng          *rand.Rand
}

func newPicker(numPieces int, completed []bool) *picker {
	p := &picker{
		done:         make([]bool, numPieces),
		inProgress:   make([]int, numPieces),
		availability: make([]int, numPieces),
		remaining:    numPieces,
		rng:          rand.New(rand.NewSource(rand.Int63())),
	}
	for i := 0; i < numPieces && i < len(completed); i++ {
		if completed[i] {
			p.done[i] = true
			p.remaining--
		}
	}
	return p
}

// addPeer / removePeer keep the availability counts in step with connections.
func (p *picker) addPeer(bf peer.Bitfield)    { p.adjust(bf, +1) }
func (p *picker) removePeer(bf peer.Bitfield) { p.adjust(bf, -1) }

func (p *picker) adjust(bf peer.Bitfield, delta int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.availability {
		if bf.HasPiece(i) {
			p.availability[i] += delta
		}
	}
}

// have records that a connected peer announced a new piece.
func (p *picker) have(index int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if index >= 0 && index < len(p.availability) {
		p.availability[index]++
	}
}

// next returns the piece this peer should download, or -1 if there is
// nothing for it right now.
func (p *picker) next(bf peer.Bitfield) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	best, bestAvail, ties := -1, 0, 0
	for i, done := range p.done {
		if done || p.inProgress[i] > 0 || !bf.HasPiece(i) {
			continue
		}
		a := p.availability[i]
		switch {
		case best == -1 || a < bestAvail:
			best, bestAvail, ties = i, a, 1
		case a == bestAvail:
			// Random tie-break (reservoir sampling) so peers don't all start on piece 0.
			ties++
			if p.rng.Intn(ties) == 0 {
				best = i
			}
		}
	}
	if best >= 0 {
		p.inProgress[best]++
		return best
	}

	// Endgame: every missing piece this peer has is already in flight.
	// Duplicate the one with the fewest workers on it.
	best = -1
	for i, done := range p.done {
		if done || !bf.HasPiece(i) || p.inProgress[i] >= maxEndgameCopies {
			continue
		}
		if best == -1 || p.inProgress[i] < p.inProgress[best] {
			best = i
		}
	}
	if best >= 0 {
		p.inProgress[best]++
	}
	return best
}

// release is called when a worker stops working on a piece without finishing it.
func (p *picker) release(index int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inProgress[index] > 0 {
		p.inProgress[index]--
	}
}

// complete marks a piece as verified and written. It reports false if
// another worker already completed it (an endgame duplicate).
func (p *picker) complete(index int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inProgress[index] > 0 {
		p.inProgress[index]--
	}
	if p.done[index] {
		return false
	}
	p.done[index] = true
	p.remaining--
	return true
}

func (p *picker) isDone(index int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done[index]
}

func (p *picker) remainingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.remaining
}

func (p *picker) finished() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.remaining == 0
}

// stalled reports whether some missing piece is held by no connected peer
// and nobody is working on it, i.e. the download cannot finish as things stand.
func (p *picker) stalled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, done := range p.done {
		if !done && p.availability[i] == 0 && p.inProgress[i] == 0 {
			return true
		}
	}
	return false
}

// usefulPeer reports whether bf has any piece we still need.
func (p *picker) usefulPeer(bf peer.Bitfield) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, done := range p.done {
		if !done && bf.HasPiece(i) {
			return true
		}
	}
	return false
}
