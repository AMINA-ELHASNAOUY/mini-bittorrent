package download

import (
	"math/rand"
	"sort"
	"time"

	"mini-bittorrent/peer"
)

// Tit-for-tat choking while we download.
//
// We only upload to a few peers at a time (spreading upload thin helps
// nobody), and while downloading we choose them by reciprocity: every round
// the interested peers that sent us the most data in the last round get the
// regular upload slots. A peer that stops uploading to us loses its slot to
// one that does, so leeching without giving back gets you less.
//
// Two additions from the reference algorithm keep this from starving anyone:
//   - one optimistic slot rotates every 3 rounds to a random choked peer, so
//     a newcomer with nothing to offer yet can get its first pieces, and so
//     we discover peers that would upload to us faster than our current ones;
//   - anti-snubbing: a peer that hasn't sent us a block for SnubTimeout loses
//     its regular slot, and can only be served through the optimistic one.

type chokerState struct {
	round      int
	optimistic *conn
}

// candidate is one interested peer as the selection sees it.
type candidate struct {
	rate     int64 // bytes received from it during the last round
	snubbed  bool
	unchoked bool // currently unchoked by us
}

// selectUnchoked decides who gets upload slots. On a periodic round the
// regular slots go to the fastest non-snubbed uploaders; between rounds the
// current choices are kept and only empty slots are filled. rotate drops the
// optimistic peer so a new one is drawn. It returns the set to unchoke and
// the optimistic peer.
func selectUnchoked(cands []*candidate, maxUnchoked int, optimistic *candidate, periodic, rotate bool, rnd func(int) int) (map[*candidate]bool, *candidate) {
	regular := maxUnchoked - 1
	if regular < 1 {
		regular = 1
	}
	present := map[*candidate]bool{}
	for _, c := range cands {
		present[c] = true
	}
	if !present[optimistic] || rotate {
		optimistic = nil
	}

	unchoke := map[*candidate]bool{}
	if periodic {
		ranked := make([]*candidate, 0, len(cands))
		for _, c := range cands {
			if !c.snubbed && c != optimistic {
				ranked = append(ranked, c)
			}
		}
		sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].rate > ranked[j].rate })
		for _, c := range ranked {
			if len(unchoke) == regular {
				break
			}
			unchoke[c] = true
		}
	} else {
		for _, c := range cands {
			if c.unchoked && c != optimistic {
				unchoke[c] = true
			}
		}
	}

	if optimistic == nil {
		var choked []*candidate
		for _, c := range cands {
			if !unchoke[c] {
				choked = append(choked, c)
			}
		}
		if len(choked) > 0 {
			optimistic = choked[rnd(len(choked))]
		}
	}
	if optimistic != nil {
		unchoke[optimistic] = true
	}
	// Free slots (few interested peers): don't leave bandwidth unused.
	for _, snubbedPass := range []bool{false, true} {
		for _, c := range cands {
			if len(unchoke) >= maxUnchoked {
				break
			}
			if c.snubbed == snubbedPass {
				unchoke[c] = true
			}
		}
	}
	return unchoke, optimistic
}

func (s *swarm) requestRechoke() {
	if !s.uploading() {
		return
	}
	select {
	case s.rechokeNow <- struct{}{}:
	default:
	}
}

func (s *swarm) chokeLoop(interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			s.rechoke(true)
		case <-s.rechokeNow:
			s.rechoke(false)
		}
	}
}

// rechoke runs one round of the choking algorithm and sends CHOKE/UNCHOKE
// to the peers whose state changed.
func (s *swarm) rechoke(periodic bool) {
	if !s.uploading() {
		return // download-only: everyone stays choked
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if periodic {
		s.choker.round++
	}
	now := time.Now().UnixNano()
	var cands []*candidate
	byCand := map[*candidate]*conn{}
	var optimistic *candidate
	for pc := range s.conns {
		down, up := pc.downloaded.Load(), pc.uploaded.Load()
		rate := down - pc.lastDown
		if periodic {
			pc.lastDown, pc.lastUp = down, up
		}
		if !pc.interested {
			continue
		}
		cand := &candidate{
			rate:     rate,
			snubbed:  time.Duration(now-pc.lastBlock.Load()) > s.snubTimeout,
			unchoked: !pc.choked,
		}
		cands = append(cands, cand)
		byCand[cand] = pc
		if pc == s.choker.optimistic {
			optimistic = cand
		}
	}
	// Map iteration order is random; sort so ties are broken the same way every time.
	sort.SliceStable(cands, func(i, j int) bool { return byCand[cands[i]].c.Addr < byCand[cands[j]].c.Addr })

	rotate := periodic && s.choker.round%3 == 0
	set, opt := selectUnchoked(cands, s.maxUnchoked, optimistic, periodic, rotate, rand.Intn)
	s.choker.optimistic = byCand[opt]

	unchoke := map[*conn]bool{}
	for c := range set {
		unchoke[byCand[c]] = true
	}
	type change struct {
		pc     *conn
		choked bool
	}
	var changes []change
	for pc := range s.conns {
		choke := !unchoke[pc]
		if pc.choked != choke {
			pc.choked = choke
			changes = append(changes, change{pc, choke})
		}
	}
	s.mu.Unlock()

	// Send outside the lock so a slow peer can't stall everyone else.
	for _, ch := range changes {
		id := peer.MsgUnchoke
		if ch.choked {
			id = peer.MsgChoke
		}
		ch.pc.c.Send(&peer.Message{ID: id})
	}
}
