package download

import (
	"testing"

	"mini-bittorrent/peer"
)

func full(n int) peer.Bitfield {
	bf := make(peer.Bitfield, (n+7)/8)
	for i := 0; i < n; i++ {
		bf.SetPiece(i)
	}
	return bf
}

func TestPickerRarestFirst(t *testing.T) {
	pk := newPicker(4, nil)
	// Pieces 0,1,3 are on three peers; piece 2 is only on one.
	pk.addPeer(full(4))
	pk.addPeer(peer.Bitfield{0b11010000})
	pk.addPeer(peer.Bitfield{0b11010000})

	if got := pk.next(full(4)); got != 2 {
		t.Fatalf("next = %d, want the rarest piece 2", got)
	}
}

func TestPickerSkipsDoneAndRespectsBitfield(t *testing.T) {
	pk := newPicker(3, []bool{true, false, false})
	onlyPiece0 := peer.Bitfield{0b10000000}
	pk.addPeer(onlyPiece0)
	if got := pk.next(onlyPiece0); got != -1 {
		t.Fatalf("next = %d, want -1 (peer only has a finished piece)", got)
	}
	if pk.usefulPeer(onlyPiece0) {
		t.Error("peer with only finished pieces should not be useful")
	}
}

func TestPickerEndgameDuplicates(t *testing.T) {
	pk := newPicker(2, nil)
	a, b := full(2), full(2)
	pk.addPeer(a)
	pk.addPeer(b)

	first, second := pk.next(a), pk.next(a)
	if first == second || first < 0 || second < 0 {
		t.Fatalf("got %d and %d, want both pieces", first, second)
	}
	// Everything is in flight: peer b gets a duplicate instead of idling.
	dup := pk.next(b)
	if dup != first && dup != second {
		t.Fatalf("endgame next = %d, want a duplicate of %d or %d", dup, first, second)
	}
	if !pk.complete(dup) {
		t.Fatal("first completion should win")
	}
	if pk.complete(dup) {
		t.Fatal("second completion of the same piece should report a duplicate")
	}
}

func TestPickerStalled(t *testing.T) {
	pk := newPicker(2, nil)
	if !pk.stalled() {
		t.Fatal("no peers at all: should be stalled")
	}
	pk.addPeer(full(2))
	if pk.stalled() {
		t.Fatal("a peer has everything: should not be stalled")
	}
	pk.removePeer(full(2))
	pk.addPeer(peer.Bitfield{0b10000000})
	if !pk.stalled() {
		t.Fatal("nobody has piece 1: should be stalled")
	}
	pk.have(1)
	if pk.stalled() {
		t.Fatal("after a HAVE for piece 1 it should no longer be stalled")
	}
}
