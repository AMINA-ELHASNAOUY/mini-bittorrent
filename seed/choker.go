package seed

import (
	"math/rand"
	"sort"
	"time"

	"mini-bittorrent/peer"
)

// The choking algorithm decides which peers we upload to.
//
// Uploading to every peer at once splits the bandwidth so thinly that nobody
// gets a useful speed, so a client "unchokes" only a few peers and keeps the
// rest choked. As a seeder (we have nothing to download), the reference rule
// is: every round, keep uploading to the peers we can send to fastest, plus
// one "optimistic" slot that rotates through the other interested peers so
// newcomers get a chance to show how fast they are.

type chokerState struct {
	round      int
	optimistic *remote
}

func (s *Server) requestRechoke() {
	select {
	case s.rechokeNow <- struct{}{}:
	default:
	}
}

func (s *Server) chokeLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.RechokeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.rechoke(true)
		case <-s.rechokeNow:
			s.rechoke(false)
		}
	}
}

// rechoke re-ranks interested peers. With periodic set (a timer round) the
// upload-rate window is reset and every third round the optimistic slot moves
// on; event-driven rounds (a peer became interested or left) only fill gaps.
func (s *Server) rechoke(periodic bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	var interested []*remote
	rate := map[*remote]int64{}
	for r := range s.conns {
		sent := r.sent.Load()
		rate[r] = sent - r.lastSent
		if periodic {
			r.lastSent = sent
		}
		if r.interested {
			interested = append(interested, r)
		}
	}

	if periodic {
		s.choker.round++
	}
	regularSlots := s.MaxUnchoked - 1
	if regularSlots < 1 {
		regularSlots = 1
	}

	unchoke := map[*remote]bool{}
	if !periodic {
		// Between rounds, keep the current choices and only fill empty slots,
		// so a new interested peer doesn't wait up to 10 s for its first byte.
		for _, r := range interested {
			if !r.choked {
				unchoke[r] = true
			}
		}
	} else {
		sort.SliceStable(interested, func(i, j int) bool { return rate[interested[i]] > rate[interested[j]] })
		for _, r := range interested {
			if len(unchoke) == regularSlots {
				break
			}
			if r != s.choker.optimistic {
				unchoke[r] = true
			}
		}
	}

	// Optimistic slot: keep it for 3 rounds, then give it to a random choked peer.
	opt := s.choker.optimistic
	if opt != nil && (!contains(s.conns, opt) || !opt.interested) {
		opt = nil
	}
	if periodic && s.choker.round%3 == 0 {
		opt = nil
	}
	if opt == nil {
		var choked []*remote
		for _, r := range interested {
			if !unchoke[r] {
				choked = append(choked, r)
			}
		}
		if len(choked) > 0 {
			opt = choked[rand.Intn(len(choked))]
		}
	}
	s.choker.optimistic = opt
	if opt != nil {
		unchoke[opt] = true
	}
	// Event-driven rounds may still have free slots: fill them.
	for _, r := range interested {
		if len(unchoke) >= s.MaxUnchoked {
			break
		}
		unchoke[r] = true
	}

	type change struct {
		r      *remote
		choked bool
	}
	var changes []change
	for r := range s.conns {
		choke := !unchoke[r]
		if r.choked != choke {
			r.choked = choke
			changes = append(changes, change{r, choke})
		}
	}
	s.mu.Unlock()

	// Send outside the lock so a slow peer can't stall the whole server.
	for _, c := range changes {
		id := peer.MsgUnchoke
		if c.choked {
			id = peer.MsgChoke
		}
		c.r.send(&peer.Message{ID: id})
	}
}

func contains(m map[*remote]struct{}, r *remote) bool {
	_, ok := m[r]
	return ok
}
