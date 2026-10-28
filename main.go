package main

import (
	"bytes"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"mini-bittorrent/download"
	"mini-bittorrent/internal/localswarm"
	"mini-bittorrent/storage"
	"mini-bittorrent/torrentfile"
	"mini-bittorrent/tracker"
)

const usage = `mini-bittorrent: a BitTorrent client written from the protocol spec, Go standard library only.

Usage:
  mini-bittorrent info     <file.torrent>                show what's inside a .torrent
  mini-bittorrent download [-o path] [-v] <file.torrent> download it (re-run the same command to resume)
  mini-bittorrent demo     [-v]                          run a local swarm and download from it (no internet needed)
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "info":
		err = runInfo(os.Args[2:])
	case "download":
		err = runDownload(os.Args[2:])
	case "demo":
		err = runDemo(os.Args[2:])
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func runInfo(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: mini-bittorrent info <file.torrent>")
	}
	tf, err := torrentfile.Open(args[0])
	if err != nil {
		return err
	}
	fmt.Printf("Name:       %s\n", tf.Name)
	fmt.Printf("Size:       %s (%d bytes)\n", humanBytes(tf.Length), tf.Length)
	fmt.Printf("Pieces:     %d × %s\n", len(tf.PieceHashes), humanBytes(tf.PieceLength))
	fmt.Printf("Info hash:  %x\n", tf.InfoHash)
	fmt.Println("Trackers:")
	for _, u := range tf.Trackers() {
		fmt.Println("  " + u)
	}
	if tf.MultiFile {
		fmt.Printf("Files (%d):\n", len(tf.Files))
		for i, f := range tf.Files {
			if i == 20 {
				fmt.Printf("  … and %d more\n", len(tf.Files)-20)
				break
			}
			fmt.Printf("  %10s  %s\n", humanBytes(f.Length), filepath.Join(f.Path...))
		}
	}
	return nil
}

func runDownload(args []string) error {
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	outPath := fs.String("o", "", "output file (default: the name inside the torrent)")
	verbose := fs.Bool("v", false, "log per-peer events")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: mini-bittorrent download [-o path] [-v] <file.torrent>")
	}
	tf, err := torrentfile.Open(fs.Arg(0))
	if err != nil {
		return err
	}
	if *outPath == "" {
		*outPath = filepath.Base(tf.Name)
	}
	return fetch(tf, *outPath, *verbose)
}

// fetch asks the trackers for peers, downloads every missing piece and
// writes the result to outPath (a file, or a directory for multi-file torrents).
// Data goes to outPath+".part" first, so an interrupted run can be resumed.
func fetch(tf *torrentfile.TorrentFile, outPath string, verbose bool) error {
	if _, err := os.Stat(outPath); err == nil {
		return fmt.Errorf("%s already exists", outPath)
	}
	peerID, err := newPeerID()
	if err != nil {
		return err
	}

	kind := "file"
	if tf.MultiFile {
		kind = fmt.Sprintf("%d files", len(tf.Files))
	}
	fmt.Printf("→ %s (%s, %s, %d pieces)\n", tf.Name, kind, humanBytes(tf.Length), len(tf.PieceHashes))

	partPath := outPath + ".part"
	st, err := storage.Open(partPath, tf)
	if err != nil {
		return err
	}
	defer st.Close()

	t := &download.Torrent{
		PeerID: peerID, InfoHash: tf.InfoHash, PieceHashes: tf.PieceHashes,
		PieceLength: tf.PieceLength, Length: tf.Length,
	}
	have, resumed, err := t.CheckPieces(st)
	if err != nil {
		return err
	}
	if resumed > 0 {
		fmt.Printf("→ resuming: %d of %d pieces already on disk and verified\n", resumed, len(tf.PieceHashes))
	}
	t.Completed = have

	if resumed < len(tf.PieceHashes) {
		trackers := tf.Trackers()
		fmt.Printf("→ asking %d tracker(s) for peers…\n", len(trackers))
		peers, errs := tracker.AnnounceAll(trackers, tracker.Request{
			InfoHash: tf.InfoHash, PeerID: peerID, Port: 6881, Left: tf.Length,
		}, 3*time.Second)
		if verbose {
			for _, e := range errs {
				fmt.Println("  tracker failed:", e)
			}
		}
		if len(peers) == 0 {
			if len(errs) > 0 {
				return fmt.Errorf("no tracker returned peers (first error: %v)", errs[0])
			}
			return errors.New("trackers returned no peers")
		}
		fmt.Printf("→ %d peers from %d tracker(s)\n", len(peers), len(trackers)-len(errs))
		for _, p := range peers {
			t.Peers = append(t.Peers, p.String())
		}
	}
	if verbose {
		t.Logger = log.New(clearLine{os.Stdout}, "  ", log.Ltime)
	}

	start := time.Now()
	err = t.Download(st, func(p download.Progress) {
		fmt.Printf("\r  %s %5.1f%%  %d/%d pieces  %d peers ", bar(p.Done, p.Total, 30),
			100*float64(p.Done)/float64(p.Total), p.Done, p.Total, p.Peers)
	})
	fmt.Println()
	if err != nil {
		return fmt.Errorf("%w (progress is kept in %s; run the same command again to resume)", err, partPath)
	}
	if err := t.VerifyFile(st); err != nil {
		return fmt.Errorf("final check failed: %w", err)
	}
	if err := st.Close(); err != nil {
		return err
	}
	if err := os.Rename(partPath, outPath); err != nil {
		return err
	}

	elapsed := time.Since(start)
	fetched := tf.Length * int64(len(tf.PieceHashes)-resumed) / int64(len(tf.PieceHashes))
	speed := float64(fetched) / elapsed.Seconds()
	fmt.Printf("✓ saved %s, all %d pieces SHA-1 verified (%s downloaded in %s, %s/s)\n",
		outPath, len(tf.PieceHashes), humanBytes(fetched), elapsed.Round(time.Millisecond), humanBytes(int64(speed)))
	return nil
}

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
	if err := fetch(tf, out, *verbose); err != nil {
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

// clearLine erases the progress bar before a log line is printed over it.
type clearLine struct{ w io.Writer }

func (c clearLine) Write(p []byte) (int, error) {
	if _, err := io.WriteString(c.w, "\r\033[K"); err != nil {
		return 0, err
	}
	return c.w.Write(p)
}

func newPeerID() ([20]byte, error) {
	var id [20]byte
	copy(id[:], "-MB0001-") // Azureus-style client prefix
	_, err := rand.Read(id[8:])
	return id, err
}

func bar(done, total, width int) string {
	filled := done * width / total
	return "[" + strings.Repeat("█", filled) + strings.Repeat("░", width-filled) + "]"
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
