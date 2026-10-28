// Command mini-bittorrent is a BitTorrent client written from the protocol
// specs using only the Go standard library.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"mini-bittorrent/torrentfile"
)

const usage = `mini-bittorrent: a BitTorrent client written from the protocol spec, Go standard library only.

Usage:
  mini-bittorrent info     <file.torrent>
        show what's inside a .torrent
  mini-bittorrent download [-o path] [-v] [-seed] [-nodht] <file.torrent | "magnet:?...">
        download it; re-run the same command to resume an interrupted download
  mini-bittorrent seed     [-port N] [-v] <file.torrent> <path>
        upload a finished download to other peers
  mini-bittorrent demo     [-v]
        run a local swarm and download from it (no internet needed)
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
	case "seed":
		err = runSeed(os.Args[2:])
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
	if trackers := tf.Trackers(); len(trackers) > 0 {
		fmt.Println("Trackers:")
		for _, u := range trackers {
			fmt.Println("  " + u)
		}
	} else {
		fmt.Println("Trackers:   none (peers are found through the DHT)")
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
