package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"mini-bittorrent/seed"
	"mini-bittorrent/storage"
	"mini-bittorrent/torrentfile"
	"mini-bittorrent/tracker"
)

func runSeed(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	port := fs.Int("port", 6881, "TCP port to accept peers on")
	verbose := fs.Bool("v", false, "log per-peer events")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: mini-bittorrent seed [-port N] [-v] <file.torrent> <path to the downloaded file or folder>")
	}
	tf, err := torrentfile.Open(fs.Arg(0))
	if err != nil {
		return err
	}
	return seedTorrent(tf, fs.Arg(1), *port, *verbose)
}

// seedTorrent verifies the data at path, then serves it to peers and keeps
// the trackers informed until Ctrl+C.
func seedTorrent(tf *torrentfile.TorrentFile, path string, port int, verbose bool) error {
	st, err := storage.OpenExisting(path, tf)
	if err != nil {
		return err
	}
	defer st.Close()

	peerID, err := newPeerID()
	if err != nil {
		return err
	}
	srv := &seed.Server{
		InfoHash: tf.InfoHash, PeerID: peerID, PieceHashes: tf.PieceHashes,
		PieceLength: tf.PieceLength, Length: tf.Length, Data: st, Info: tf.InfoBytes,
	}
	if verbose {
		srv.Logger = log.New(clearLine{os.Stdout}, "  ", log.Ltime)
	}
	fmt.Printf("→ checking %s…\n", path)
	if err := srv.Verify(); err != nil {
		return err
	}
	if srv.Have() == 0 {
		return fmt.Errorf("%s doesn't match the torrent (0 of %d pieces valid)", path, len(tf.PieceHashes))
	}
	if err := srv.Listen(fmt.Sprintf(":%d", port)); err != nil {
		fmt.Printf("  port %d unavailable (%v), using a random port\n", port, err)
		if err := srv.Listen(":0"); err != nil {
			return err
		}
	}
	defer srv.Close()
	fmt.Printf("→ seeding %d/%d pieces on port %d (peers behind your router can only reach you if this port is forwarded)\n",
		srv.Have(), len(tf.PieceHashes), srv.Port())

	announce := func() {
		left := tf.Length * int64(len(tf.PieceHashes)-srv.Have()) / int64(len(tf.PieceHashes))
		_, errs := tracker.AnnounceAll(tf.Trackers(), tracker.Request{
			InfoHash: tf.InfoHash, PeerID: peerID, Port: srv.Port(), Left: left,
		}, 3*time.Second)
		if verbose {
			for _, e := range errs {
				fmt.Println("  tracker failed:", e)
			}
		}
	}
	announce()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	defer signal.Stop(interrupt)
	status := time.NewTicker(time.Second)
	defer status.Stop()
	reannounce := time.NewTicker(30 * time.Minute)
	defer reannounce.Stop()
	for {
		select {
		case <-status.C:
			fmt.Printf("\r  ↑ %s uploaded · %d peers connected · Ctrl+C to stop ", humanBytes(srv.Uploaded()), srv.Peers())
		case <-reannounce.C:
			announce()
		case <-interrupt:
			fmt.Printf("\n✓ stopped seeding (%s uploaded)\n", humanBytes(srv.Uploaded()))
			return nil
		}
	}
}
