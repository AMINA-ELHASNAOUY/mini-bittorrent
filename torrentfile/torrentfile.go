package torrentfile

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"strings"

	"mini-bittorrent/bencode"
)

// File is one file inside the torrent, in the order its bytes appear in the piece stream.
type File struct {
	Path   []string // path components relative to the torrent's root
	Length int64
	Offset int64 // where this file starts in the concatenated byte stream
}

// TorrentFile holds the parsed, usable info from a .torrent file.
type TorrentFile struct {
	Announce     string     // primary tracker URL (may be empty if only AnnounceList is set)
	AnnounceList [][]string // tracker tiers (BEP 12), may be empty
	InfoHash     [20]byte   // SHA-1 hash of the info dict (identifies the torrent)
	PieceHashes  [][20]byte // SHA-1 hash of each piece
	PieceLength  int64      // bytes per piece
	Length       int64      // total length of all files in bytes
	Name         string     // file name (single-file) or root directory name (multi-file)
	Files        []File     // always at least one entry
	MultiFile    bool       // true when the torrent describes a directory
}

// Trackers returns every tracker URL to try, best first and without duplicates:
// announce-list tiers in order, then the plain announce URL.
func (t *TorrentFile) Trackers() []string {
	seen := map[string]bool{}
	var out []string
	add := func(u string) {
		if u != "" && !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
	}
	for _, tier := range t.AnnounceList {
		for _, u := range tier {
			add(u)
		}
	}
	add(t.Announce)
	return out
}

// Open reads and parses a .torrent file from disk.
func Open(path string) (*TorrentFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Parse(f)
}

// Parse reads a bencoded .torrent from r.
func Parse(r io.Reader) (*TorrentFile, error) {
	decoded, err := bencode.Decode(bufio.NewReader(r))
	if err != nil {
		return nil, fmt.Errorf("decode error: %w", err)
	}

	dict, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("top-level bencode value is not a dictionary")
	}

	tf := &TorrentFile{}
	tf.Announce, _ = dict["announce"].(string)
	if tiers, ok := dict["announce-list"].([]interface{}); ok {
		for _, tier := range tiers {
			list, _ := tier.([]interface{})
			var urls []string
			for _, u := range list {
				if s, ok := u.(string); ok && s != "" {
					urls = append(urls, s)
				}
			}
			if len(urls) > 0 {
				tf.AnnounceList = append(tf.AnnounceList, urls)
			}
		}
	}
	if len(tf.Trackers()) == 0 {
		return nil, fmt.Errorf("no tracker URL (trackerless/DHT torrents are not supported)")
	}

	info, ok := dict["info"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("missing or invalid info dict")
	}

	if tf.InfoHash, err = hashInfoDict(info); err != nil {
		return nil, err
	}

	tf.PieceLength, _ = info["piece length"].(int64)
	tf.Name, _ = info["name"].(string)
	if tf.PieceLength <= 0 {
		return nil, fmt.Errorf("invalid piece length %d", tf.PieceLength)
	}
	if err := checkPathComponent(tf.Name); err != nil {
		return nil, fmt.Errorf("unsafe torrent name: %w", err)
	}

	if files, multi := info["files"]; multi {
		tf.MultiFile = true
		if tf.Files, err = parseFiles(files); err != nil {
			return nil, err
		}
	} else {
		length, _ := info["length"].(int64)
		tf.Files = []File{{Path: []string{tf.Name}, Length: length}}
	}
	for i := range tf.Files {
		tf.Files[i].Offset = tf.Length
		tf.Length += tf.Files[i].Length
	}
	if tf.Length <= 0 {
		return nil, fmt.Errorf("torrent has no data (total length %d)", tf.Length)
	}

	piecesStr, ok := info["pieces"].(string)
	if !ok {
		return nil, fmt.Errorf("missing pieces string")
	}
	if tf.PieceHashes, err = splitPieceHashes(piecesStr); err != nil {
		return nil, err
	}
	wantPieces := (tf.Length + tf.PieceLength - 1) / tf.PieceLength
	if int64(len(tf.PieceHashes)) != wantPieces {
		return nil, fmt.Errorf("expected %d piece hashes for %d bytes, got %d", wantPieces, tf.Length, len(tf.PieceHashes))
	}
	return tf, nil
}

func parseFiles(v interface{}) ([]File, error) {
	list, ok := v.([]interface{})
	if !ok || len(list) == 0 {
		return nil, fmt.Errorf("invalid or empty files list")
	}
	files := make([]File, 0, len(list))
	for i, item := range list {
		fd, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("file %d is not a dictionary", i)
		}
		length, ok := fd["length"].(int64)
		if !ok || length < 0 {
			return nil, fmt.Errorf("file %d has an invalid length", i)
		}
		rawPath, ok := fd["path"].([]interface{})
		if !ok || len(rawPath) == 0 {
			return nil, fmt.Errorf("file %d has no path", i)
		}
		path := make([]string, len(rawPath))
		for j, p := range rawPath {
			s, _ := p.(string)
			if err := checkPathComponent(s); err != nil {
				return nil, fmt.Errorf("file %d has an unsafe path: %w", i, err)
			}
			path[j] = s
		}
		files = append(files, File{Path: path, Length: length})
	}
	return files, nil
}

// checkPathComponent rejects names that could write outside the download
// directory (a malicious torrent could otherwise use "../" or absolute paths).
func checkPathComponent(s string) error {
	switch {
	case s == "" || s == "." || s == "..":
		return fmt.Errorf("invalid path component %q", s)
	case strings.ContainsAny(s, "/\\\x00"):
		return fmt.Errorf("path component %q contains a separator", s)
	}
	return nil
}

// hashInfoDict re-encodes the info dict and returns its SHA-1 hash.
// The info hash is how peers/trackers uniquely identify a torrent.
// Bencoding is canonical (sorted keys), so re-encoding gives back the original bytes.
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

// pieceString hashes data into the concatenated "pieces" string.
func pieceString(pieceLength int64, data []byte) string {
	var pieces []byte
	for start := 0; start < len(data); start += int(pieceLength) {
		end := start + int(pieceLength)
		if end > len(data) {
			end = len(data)
		}
		h := sha1.Sum(data[start:end])
		pieces = append(pieces, h[:]...)
	}
	return string(pieces)
}

// Create builds the bencoded bytes of a single-file .torrent for data.
// It is used by the tests and by the local demo swarm.
func Create(announce, name string, pieceLength int64, data []byte) ([]byte, error) {
	return bencode.Encode(map[string]interface{}{
		"announce": announce,
		"info": map[string]interface{}{
			"name":         name,
			"length":       int64(len(data)),
			"piece length": pieceLength,
			"pieces":       pieceString(pieceLength, data),
		},
	})
}

// FileData is one file passed to CreateMulti.
type FileData struct {
	Path []string
	Data []byte
}

// CreateMulti builds a multi-file .torrent whose root directory is name.
func CreateMulti(announce, name string, pieceLength int64, files []FileData) ([]byte, error) {
	var all []byte
	list := make([]interface{}, len(files))
	for i, f := range files {
		path := make([]interface{}, len(f.Path))
		for j, p := range f.Path {
			path[j] = p
		}
		list[i] = map[string]interface{}{"length": int64(len(f.Data)), "path": path}
		all = append(all, f.Data...)
	}
	return bencode.Encode(map[string]interface{}{
		"announce": announce,
		"info": map[string]interface{}{
			"name":         name,
			"files":        list,
			"piece length": pieceLength,
			"pieces":       pieceString(pieceLength, all),
		},
	})
}
