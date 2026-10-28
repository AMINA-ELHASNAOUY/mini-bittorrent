package main

import (
	"bytes"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"mini-bittorrent/internal/localswarm"
	"mini-bittorrent/torrentfile"
)

// runDemo spins up a tracker and four seeders on localhost, one of which
// corrupts everything it sends, then downloads a random 8 MiB file from them.
func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	verbose := fs.Bool("v", true, "log per-peer events")
	if err := fs.Parse(args); err != nil {
		return err
	}

	data := make([]byte, 8<<20)
	if _, err := rand.Read(data); err != nil {
		return err
	}
	const pieceLength = 256 << 10
	const lag = 4 * time.Millisecond // simulated network delay per 16 KiB block
	sw, err := localswarm.Start("demo.bin", data, pieceLength, []localswarm.SeederSpec{
		{Latency: lag},
		{Has: func(i int) bool { return i%2 == 0 }, Latency: lag}, // only even pieces
		{Has: func(i int) bool { return i%2 == 1 }, Latency: lag}, // only odd pieces
		{Corrupt: true, Latency: lag},                             // sends garbage
	})
	if err != nil {
		return err
	}
	defer sw.Close()

	fmt.Println("demo: local tracker + 4 seeders (one full, two partial, one sending corrupt data)")
	tf, err := torrentfile.Parse(bytes.NewReader(sw.Torrent))
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "mini-bittorrent-demo")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	out := filepath.Join(dir, "demo.bin")
	if err := fetch(tf, out, fetchOptions{verbose: *verbose, noDHT: true}); err != nil {
		return err
	}
	got, err := os.ReadFile(out)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, data) {
		return errors.New("demo file differs from the original")
	}
	fmt.Println("✓ byte-for-byte identical to the original")
	return nil
}
