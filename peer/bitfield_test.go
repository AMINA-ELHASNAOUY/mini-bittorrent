package peer

import "testing"

func TestBitfield(t *testing.T) {
	bf := Bitfield{0b10100000, 0b00000001}
	for i, want := range map[int]bool{0: true, 1: false, 2: true, 15: true, 14: false, 16: false, -1: false} {
		if bf.HasPiece(i) != want {
			t.Errorf("HasPiece(%d) = %v, want %v", i, !want, want)
		}
	}
	bf.SetPiece(1)
	bf.SetPiece(99) // ignored
	if !bf.HasPiece(1) || bf[0] != 0b11100000 {
		t.Errorf("SetPiece failed: %08b", bf[0])
	}
}
