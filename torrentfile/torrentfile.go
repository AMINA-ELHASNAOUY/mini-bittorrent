package torrentfile

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"os"

	"mini-bittorrent/bencode"
)

// TorrentFile holds the parsed, usable info from a .torrent file.
type TorrentFile struct {
	Announce    string     // tracker URL
	InfoHash    [20]byte   // SHA-1 hash of the info dict (identifies the torrent)
	PieceHashes [][20]byte // SHA-1 hash of each piece
	PieceLength int64      // bytes per piece
	Length      int64      // total file length in bytes
	Name        string     // suggested file name
}

// Open reads and parses a .torrent file from disk.
func Open(path string) (*TorrentFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	decoded, err := bencode.Decode(r)
	if err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	dict, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("top-level bencode value is not a dictionary")
	}

	announce, _ := dict["announce"].(string)

	info, ok := dict["info"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("missing or invalid info dict")
	}

	infoHash, err := hashInfoDict(info)
	if err != nil {
		return nil, err
	}

	pieceLength, _ := info["piece length"].(int64)
	length, _ := info["length"].(int64)
	name, _ := info["name"].(string)

	piecesStr, ok := info["pieces"].(string)
	if !ok {
		return nil, fmt.Errorf("missing pieces string")
	}
	pieceHashes, err := splitPieceHashes(piecesStr)
	if err != nil {
		return nil, err
	}

	return &TorrentFile{
		Announce:    announce,
		InfoHash:    infoHash,
		PieceHashes: pieceHashes,
		PieceLength: pieceLength,
		Length:      length,
		Name:        name,
	}, nil
}

// hashInfoDict re-encodes the info dict and returns its SHA-1 hash.
// The info hash is how peers/trackers uniquely identify a torrent.
func hashInfoDict(info map[string]interface{}) ([20]byte, error) {
	encoded, err := bencode.Encode(info)
	if err != nil {
		return [20]byte{}, err
	}
	return sha1.Sum(encoded), nil
}

// splitPieceHashes splits the concatenated 20-byte SHA-1 hashes
// into a slice of individual hashes.
func splitPieceHashes(pieces string) ([][20]byte, error) {
	const hashLen = 20
	buf := []byte(pieces)
	if len(buf)%hashLen != 0 {
		return nil, fmt.Errorf("malformed pieces string, length %d not divisible by %d", len(buf), hashLen)
	}
	numHashes := len(buf) / hashLen
	hashes := make([][20]byte, numHashes)
	for i := 0; i < numHashes; i++ {
		copy(hashes[i][:], buf[i*hashLen:(i+1)*hashLen])
	}
	return hashes, nil
}
