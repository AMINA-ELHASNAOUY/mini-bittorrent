package torrentfile

import (
	"fmt"
	"testing"
)

func TestOpen(t *testing.T) {
	tf, err := Open("../test.torrent")
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}

	fmt.Printf("Announce: %s\n", tf.Announce)
	fmt.Printf("Name: %s\n", tf.Name)
	fmt.Printf("Length: %d bytes (%.2f MB)\n", tf.Length, float64(tf.Length)/1024/1024)
	fmt.Printf("Piece Length: %d bytes\n", tf.PieceLength)
	fmt.Printf("Number of pieces: %d\n", len(tf.PieceHashes))
	fmt.Printf("InfoHash: %x\n", tf.InfoHash)

	if tf.Announce == "" {
		t.Error("expected non-empty announce URL")
	}
	if len(tf.PieceHashes) == 0 {
		t.Error("expected at least one piece hash")
	}
}
