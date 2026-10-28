package peer

// Bitfield records which pieces a peer has. Bit 0 of byte 0 (the high bit) is piece 0.
type Bitfield []byte

// HasPiece reports whether the bit for piece index is set.
func (bf Bitfield) HasPiece(index int) bool {
	byteIndex, offset := index/8, index%8
	if index < 0 || byteIndex >= len(bf) {
		return false
	}
	return bf[byteIndex]>>uint(7-offset)&1 != 0
}

// SetPiece sets the bit for piece index (out-of-range indexes are ignored).
func (bf Bitfield) SetPiece(index int) {
	byteIndex, offset := index/8, index%8
	if index < 0 || byteIndex >= len(bf) {
		return
	}
	bf[byteIndex] |= 1 << uint(7-offset)
}
