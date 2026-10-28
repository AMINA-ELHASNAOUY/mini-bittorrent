// Package storage maps the torrent's single byte stream onto the files it
// describes. A piece can start in one file and end in the next, so reads and
// writes are split at file boundaries.
package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"mini-bittorrent/torrentfile"
)

type entry struct {
	file   *os.File
	offset int64 // start of this file in the byte stream
	length int64
}

// Storage implements io.WriterAt and io.ReaderAt over a set of files.
type Storage struct {
	entries []entry
	length  int64
}

// Open creates (or reopens, for resuming) every file of the torrent under
// root and returns a Storage spanning them. Existing data is kept.
// For a single-file torrent root is the file path itself.
func Open(root string, tf *torrentfile.TorrentFile) (*Storage, error) {
	s := &Storage{length: tf.Length}
	for _, f := range tf.Files {
		path := root
		if tf.MultiFile {
			path = filepath.Join(append([]string{root}, f.Path...)...)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			s.Close()
			return nil, err
		}
		fh, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			s.Close()
			return nil, err
		}
		// Bring every file to its final size (truncates anything larger).
		if err := fh.Truncate(f.Length); err != nil {
			fh.Close()
			s.Close()
			return nil, err
		}
		s.entries = append(s.entries, entry{file: fh, offset: f.Offset, length: f.Length})
	}
	return s, nil
}

// span calls fn for each file region overlapping [off, off+n).
func (s *Storage) span(off int64, n int, fn func(e entry, fileOff int64, lo, hi int) error) error {
	if off < 0 || off+int64(n) > s.length {
		return fmt.Errorf("range [%d, %d) is outside the torrent (%d bytes)", off, off+int64(n), s.length)
	}
	// First file whose end is past off.
	i := sort.Search(len(s.entries), func(i int) bool {
		return s.entries[i].offset+s.entries[i].length > off
	})
	done := 0
	for ; done < n && i < len(s.entries); i++ {
		e := s.entries[i]
		if e.length == 0 {
			continue
		}
		fileOff := off + int64(done) - e.offset
		chunk := int(min64(e.length-fileOff, int64(n-done)))
		if err := fn(e, fileOff, done, done+chunk); err != nil {
			return err
		}
		done += chunk
	}
	return nil
}

// WriteAt writes p at offset off of the combined byte stream.
func (s *Storage) WriteAt(p []byte, off int64) (int, error) {
	err := s.span(off, len(p), func(e entry, fileOff int64, lo, hi int) error {
		_, err := e.file.WriteAt(p[lo:hi], fileOff)
		return err
	})
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// ReadAt reads len(p) bytes at offset off of the combined byte stream.
func (s *Storage) ReadAt(p []byte, off int64) (int, error) {
	err := s.span(off, len(p), func(e entry, fileOff int64, lo, hi int) error {
		_, err := io.ReadFull(io.NewSectionReader(e.file, fileOff, int64(hi-lo)), p[lo:hi])
		return err
	})
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// Close flushes and closes every file.
func (s *Storage) Close() error {
	var errs []error
	for _, e := range s.entries {
		if err := e.file.Sync(); err != nil {
			errs = append(errs, err)
		}
		if err := e.file.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	s.entries = nil
	return errors.Join(errs...)
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
