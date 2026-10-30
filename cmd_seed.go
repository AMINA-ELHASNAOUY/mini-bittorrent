package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"mini-bittorrent/dht"
	"mini-bittorrent/seed"
	"mini-bittorrent/storage"
	"mini-bittorrent/torrentfile"
	"mini-bittorrent/tracker"
	"mini-bittorrent/utp"
)

func runSeed(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ContinueOnError)
	port := fs.Int("port", 6881, "TCP port to accept peers on")
	verbose := fs.Bool("v", false, "log per-peer events")
	limit := fs.Int64("limit", 0, "upload limit in KiB/s (0 = unlimited)")
	noUTP := fs.Bool("noutp", false, "accept TCP only, no uTP")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return errors.New("usage: mini-bittorrent seed [-port N] [-limit KiB/s] [-noutp] [-v] <file.torrent> <path to the downloaded file or folder>")
	}
	tf, err := torrentfile.Open(fs.Arg(0))
	if err != nil {
		return err
	}
	return seedTorrent(tf, fs.Arg(1), seedOptions{port: *port, limitKiB: *limit, verbose: *verbose, noUTP: *noUTP})
}

type seedOptions struct {
	port     int
	limitKiB int64
	verbose  bool
	noUTP    bool
}

// seedTorrent verifies the data at path, then serves it to peers and keeps
// the trackers informed until Ctrl+C.
func seedTorrent(tf *torrentfile.TorrentFile, path string, o seedOptions) error {
	return seedUntil(tf, path, o, nil)
}

// seedUntil is seedTorrent that also stops when stop is closed (used by tests).
func seedUntil(tf *torrentfile.TorrentFile, path string, o seedOptions, stop <-chan struct{}) error {
	port, limitKiB, verbose := o.port, o.limitKiB, o.verbose
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
		RateLimit: limitKiB * 1024,
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
	transports := "TCP + uTP"
	if o.noUTP {
		transports = "TCP"
	}
	fmt.Printf("→ seeding %d/%d pieces on port %d, %s (peers behind your router can only reach you if this port is forwarded)\n",
		srv.Have(), len(tf.PieceHashes), srv.Port(), transports)
	if limitKiB > 0 {
		fmt.Printf("→ upload limited to %d KiB/s, uploading to %d peers at a time\n", limitKiB, 4)
	}

	// uTP and a full DHT node share the same port number over UDP, as other
	// clients expect: peers can reach us over uTP, find us without a
	// tracker, and we help route other people's DHT lookups.
	var node *dht.Client
	var sock *utp.Socket
	if !o.noUTP {
		if sock, err = utp.Listen(fmt.Sprintf(":%d", srv.Port())); err != nil {
			fmt.Printf("  uTP unavailable (%v); TCP only\n", err)
		} else {
			srv.Serve(sock) // closed by srv.Close
			node, err = dht.NewNodeOn(sock.PacketConn())
		}
	}
	if sock == nil {
		node, err = dht.NewNode(fmt.Sprintf(":%d", srv.Port()))
	}
	if err != nil {
		fmt.Printf("  DHT unavailable (%v); trackers only\n", err)
		node = nil
	} else if node != nil {
		defer node.Close()
	}

	announce := func() {
		if trackers := tf.Trackers(); len(trackers) > 0 {
			left := tf.Length * int64(len(tf.PieceHashes)-srv.Have()) / int64(len(tf.PieceHashes))
			_, errs := tracker.AnnounceAll(trackers, tracker.Request{
				InfoHash: tf.InfoHash, PeerID: peerID, Port: srv.Port(), Left: left,
			}, 3*time.Second)
			if verbose {
				for _, e := range errs {
					fmt.Println("  tracker failed:", e)
				}
			}
		}
		if node != nil {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), dhtTimeout)
				defer cancel()
				n, err := node.Announce(ctx, tf.InfoHash, int(srv.Port()), dhtBootstrap)
				switch {
				case err != nil && verbose:
					fmt.Print(clearLineSeq, "  DHT announce failed: ", err, "\n")
				case err == nil:
					fmt.Printf("%s→ announced to %d DHT nodes\n", clearLineSeq, n)
				}
			}()
		}
	}
	announce()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	defer signal.Stop(interrupt)
	status := time.NewTicker(time.Second)
	defer status.Stop()
	reannounce := time.NewTicker(15 * time.Minute) // DHT entries expire after 30 min
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
		case <-stop:
			return nil
		}
	}
}
