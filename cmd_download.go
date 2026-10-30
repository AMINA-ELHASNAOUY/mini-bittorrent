package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"mini-bittorrent/dht"
	"mini-bittorrent/download"
	"mini-bittorrent/magnet"
	"mini-bittorrent/metadata"
	"mini-bittorrent/mse"
	"mini-bittorrent/storage"
	"mini-bittorrent/torrentfile"
	"mini-bittorrent/tracker"
	"mini-bittorrent/utp"
)

// dhtBootstrap is a variable so tests can point the DHT at local nodes.
var dhtBootstrap = dht.DefaultBootstrap

const dhtTimeout = 20 * time.Second

type fetchOptions struct {
	verbose    bool
	noDHT      bool     // never use the DHT
	extraPeers []string // peers already known (e.g. from fetching magnet metadata)
	port       int      // port to accept peers on while downloading (announced to trackers)
	noUTP      bool     // TCP only
	noUpload   bool     // don't upload while downloading
	limitKiB   int64    // upload limit
	encryption mse.Policy
}

// lingerAfterDownload is how long we keep serving peers that are still
// downloading from us once our own download is complete.
const lingerAfterDownload = 15 * time.Second

func runDownload(args []string) error {
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	outPath := fs.String("o", "", "output file or folder (default: the name inside the torrent)")
	verbose := fs.Bool("v", false, "log per-peer events")
	seedAfter := fs.Bool("seed", false, "keep seeding after the download finishes (Ctrl+C to stop)")
	port := fs.Int("port", 6881, "TCP/uTP port to accept peers on")
	limit := fs.Int64("limit", 0, "upload limit in KiB/s (0 = unlimited)")
	noDHT := fs.Bool("nodht", false, "don't look for peers in the DHT")
	noUTP := fs.Bool("noutp", false, "use TCP only, no uTP")
	noUpload := fs.Bool("noupload", false, "don't upload to other peers while downloading")
	encryption := fs.String("encryption", "prefer", "protocol encryption (MSE/PE): prefer, require or off")
	if err := fs.Parse(args); err != nil {
		return err
	}
	policy, err := mse.ParsePolicy(*encryption)
	if err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New(`usage: mini-bittorrent download [-o path] [-v] [-seed] [-port N] [-limit KiB/s] [-nodht] [-noutp] [-noupload] [-encryption prefer|require|off] <file.torrent | "magnet:?...">`)
	}
	opts := fetchOptions{verbose: *verbose, noDHT: *noDHT, port: *port, noUTP: *noUTP, noUpload: *noUpload,
		limitKiB: *limit, encryption: policy}

	var tf *torrentfile.TorrentFile
	if arg := fs.Arg(0); strings.HasPrefix(arg, "magnet:") {
		var raw []byte
		tf, raw, opts.extraPeers, err = resolveMagnet(arg, opts)
		if err != nil {
			return err
		}
		if *outPath == "" {
			*outPath = filepath.Base(tf.Name)
		}
		// Save the metadata so the download can be resumed or seeded later
		// without fetching it from peers again.
		if err := os.WriteFile(*outPath+".torrent", raw, 0o644); err == nil {
			fmt.Printf("→ saved the torrent metadata to %s.torrent\n", *outPath)
		}
	} else {
		if tf, err = torrentfile.Open(arg); err != nil {
			return err
		}
	}
	if *outPath == "" {
		*outPath = filepath.Base(tf.Name)
	}

	if err := fetch(tf, *outPath, opts); err != nil {
		return err
	}
	if *seedAfter {
		return seedTorrent(tf, *outPath, seedOptions{port: *port, limitKiB: *limit, verbose: *verbose, noUTP: *noUTP, encryption: policy})
	}
	return nil
}

// resolveMagnet turns a magnet link into a full torrent: find peers (trackers
// in the link plus the DHT), then download the info dictionary from one of them.
func resolveMagnet(uri string, opts fetchOptions) (*torrentfile.TorrentFile, []byte, []string, error) {
	link, err := magnet.Parse(uri)
	if err != nil {
		return nil, nil, nil, err
	}
	name := link.Name
	if name == "" {
		name = fmt.Sprintf("%x", link.InfoHash)
	}
	fmt.Printf("→ magnet link: %s\n", name)

	peerID, err := newPeerID()
	if err != nil {
		return nil, nil, nil, err
	}
	peers, err := findPeers(link.InfoHash, peerID, link.Trackers, 1, true, opts)
	peers = append(link.Peers, peers...)
	if len(peers) == 0 {
		return nil, nil, nil, err
	}

	fmt.Printf("→ fetching torrent metadata from %d peers…\n", len(peers))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Metadata comes over TCP or uTP, whichever the peer answers first.
	rawDial := func(addr string) (net.Conn, error) { return net.DialTimeout("tcp", addr, dialTimeout) }
	if !opts.noUTP {
		if sock, err := utp.NewSocket(":0"); err == nil {
			defer sock.Close()
			rawDial = (&transport{utp: sock}).Dial
		}
	}
	dial := func(addr string) (net.Conn, error) {
		return mse.Dial(func() (net.Conn, error) { return rawDial(addr) }, link.InfoHash, opts.encryption)
	}
	info, err := metadata.FetchAnyDial(ctx, dial, peers, link.InfoHash, peerID)
	if err != nil {
		return nil, nil, nil, err
	}
	tf, raw, err := torrentfile.FromInfo(info, link.InfoHash, link.Trackers)
	if err != nil {
		return nil, nil, nil, err
	}
	fmt.Printf("→ got metadata: %s, %s\n", tf.Name, humanBytes(tf.Length))
	return tf, raw, peers, nil
}

// findPeers asks every tracker and, if allowed, the DHT. With alwaysDHT the
// DHT runs alongside the trackers; otherwise it is only a fallback for when
// trackers come back empty.
func findPeers(infoHash, peerID [20]byte, trackers []string, left int64, alwaysDHT bool, opts fetchOptions) ([]string, error) {
	useDHT := !opts.noDHT
	var dhtPeers chan []string
	lookupDHT := func() chan []string {
		ch := make(chan []string, 1)
		go func() {
			fmt.Println("→ searching the DHT for peers…")
			ctx, cancel := context.WithTimeout(context.Background(), dhtTimeout)
			defer cancel()
			client, err := dht.New(":0")
			if err != nil {
				ch <- nil
				return
			}
			defer client.Close()
			found, err := client.GetPeers(ctx, infoHash, dhtBootstrap, 200)
			if err != nil && opts.verbose {
				fmt.Println("  DHT:", err)
			}
			ch <- found
		}()
		return ch
	}
	if useDHT && alwaysDHT {
		dhtPeers = lookupDHT()
	}

	seen := map[string]bool{}
	var peers []string
	add := func(list []string) {
		for _, p := range list {
			if !seen[p] {
				seen[p] = true
				peers = append(peers, p)
			}
		}
	}

	var trackerErr error
	if len(trackers) > 0 {
		fmt.Printf("→ asking %d tracker(s) for peers…\n", len(trackers))
		found, errs := tracker.AnnounceAll(trackers, tracker.Request{
			InfoHash: infoHash, PeerID: peerID, Port: uint16(opts.port), Left: left,
		}, 3*time.Second)
		if opts.verbose {
			for _, e := range errs {
				fmt.Println("  tracker failed:", e)
			}
		}
		for _, p := range found {
			add([]string{p.String()})
		}
		fmt.Printf("  %d peers from trackers\n", len(peers))
		if len(found) == 0 && len(errs) > 0 {
			trackerErr = fmt.Errorf("no tracker returned peers (first error: %v)", errs[0])
		}
	}

	if useDHT && dhtPeers == nil && len(peers) == 0 {
		dhtPeers = lookupDHT()
	}
	if dhtPeers != nil {
		before := len(peers)
		add(<-dhtPeers)
		fmt.Printf("  %d peers from the DHT\n", len(peers)-before)
	}

	if len(peers) == 0 {
		switch {
		case trackerErr != nil:
			return nil, trackerErr
		case opts.noDHT:
			return nil, errors.New("no peers found (the DHT is disabled)")
		default:
			return nil, errors.New("no peers found from trackers or the DHT")
		}
	}
	return peers, nil
}

// fetch downloads every missing piece and writes the result to outPath (a
// file, or a directory for multi-file torrents). Data goes to outPath+".part"
// first, so an interrupted run can be resumed.
func fetch(tf *torrentfile.TorrentFile, outPath string, opts fetchOptions) error {
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

	// Accept peers while we download (TCP and uTP on one port number), so
	// we can upload pieces we already have: that is what earns us faster
	// uploads from others under tit-for-tat.
	tr, err := listenTransport(opts.port, !opts.noUTP)
	if err != nil {
		return err
	}
	defer tr.Close()
	opts.port = tr.port
	t.Dial = tr.Dial
	t.Encryption = opts.encryption
	if !opts.noUpload {
		t.Data = st
		t.Listener = tr.Listener()
		t.UploadLimit = opts.limitKiB * 1024
		t.Linger = lingerAfterDownload
		t.OnLinger = func(n int) {
			fmt.Printf("→ finishing uploads to %d peer(s) still downloading from us (up to %s)\n", n, lingerAfterDownload)
		}
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
		t.Peers = opts.extraPeers
		if len(t.Peers) == 0 {
			if t.Peers, err = findPeers(tf.InfoHash, peerID, tf.Trackers(), tf.Length, false, opts); err != nil {
				return err
			}
		}
		transports := "TCP + uTP"
		if opts.noUTP {
			transports = "TCP"
		}
		fmt.Printf("→ connecting to %d peers (%s), accepting peers on port %d\n", len(t.Peers), transports, tr.port)
	}
	if opts.verbose {
		t.Logger = log.New(clearLine{os.Stdout}, "  ", log.Ltime)
	}

	start := time.Now()
	err = t.Download(st, func(p download.Progress) {
		up := ""
		if p.Uploaded > 0 {
			up = "  ↑ " + humanBytes(p.Uploaded)
		}
		fmt.Printf("\r  %s %5.1f%%  %d/%d pieces  %d peers%s ", bar(p.Done, p.Total, 30),
			100*float64(p.Done)/float64(p.Total), p.Done, p.Total, p.Peers, up)
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
	if up := t.Uploaded(); up > 0 {
		fmt.Printf("  ↑ uploaded %s to other peers\n", humanBytes(up))
	}
	return nil
}
