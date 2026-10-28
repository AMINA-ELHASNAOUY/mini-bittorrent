package localswarm

import (
	"bytes"
	"encoding/binary"
	"net"
	"net/http"
	"strconv"
	"time"

	"mini-bittorrent/bencode"
	"mini-bittorrent/torrentfile"
)

// Swarm is a local tracker plus a set of seeders for one file.
type Swarm struct {
	Torrent []byte // bencoded .torrent pointing at the local tracker
	Seeders []*Seeder

	tracker *http.Server
	ln      net.Listener
}

// SeederSpec configures one seeder in the swarm.
type SeederSpec struct {
	Has     func(index int) bool // nil = every piece
	Corrupt bool
	Latency time.Duration // simulated delay per block served
}

// Start launches a tracker and one seeder per spec for a single file, all on 127.0.0.1.
func Start(name string, data []byte, pieceLength int, specs []SeederSpec) (*Swarm, error) {
	return start(data, pieceLength, specs, func(announce string) ([]byte, error) {
		return torrentfile.Create(announce, name, int64(pieceLength), data)
	})
}

// StartMulti is Start for a multi-file torrent whose root directory is name.
func StartMulti(name string, files []torrentfile.FileData, pieceLength int, specs []SeederSpec) (*Swarm, error) {
	var data []byte
	for _, f := range files {
		data = append(data, f.Data...)
	}
	return start(data, pieceLength, specs, func(announce string) ([]byte, error) {
		return torrentfile.CreateMulti(announce, name, int64(pieceLength), files)
	})
}

func start(data []byte, pieceLength int, specs []SeederSpec, makeTorrent func(announce string) ([]byte, error)) (*Swarm, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	sw := &Swarm{ln: ln}

	raw, err := makeTorrent("http://" + ln.Addr().String() + "/announce")
	if err != nil {
		ln.Close()
		return nil, err
	}
	sw.Torrent = raw
	tf, err := torrentfile.Parse(bytes.NewReader(raw))
	if err != nil {
		ln.Close()
		return nil, err
	}

	for i, spec := range specs {
		s := &Seeder{Data: data, PieceLength: pieceLength, InfoHash: tf.InfoHash, Has: spec.Has, Corrupt: spec.Corrupt, Latency: spec.Latency}
		copy(s.PeerID[:], "-LS0001-seeder-"+strconv.Itoa(i))
		if err := s.Start(); err != nil {
			sw.Close()
			return nil, err
		}
		sw.Seeders = append(sw.Seeders, s)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/announce", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("info_hash") != string(tf.InfoHash[:]) {
			b, _ := bencode.Encode(map[string]interface{}{"failure reason": "unknown torrent"})
			w.Write(b)
			return
		}
		var compact []byte
		for _, s := range sw.Seeders {
			host, portStr, _ := net.SplitHostPort(s.Addr())
			port, _ := strconv.Atoi(portStr)
			entry := make([]byte, 6)
			copy(entry, net.ParseIP(host).To4())
			binary.BigEndian.PutUint16(entry[4:], uint16(port))
			compact = append(compact, entry...)
		}
		b, _ := bencode.Encode(map[string]interface{}{"interval": int64(900), "peers": string(compact)})
		w.Write(b)
	})
	sw.tracker = &http.Server{Handler: mux}
	go sw.tracker.Serve(ln)
	return sw, nil
}

// Close shuts down the tracker and all seeders.
func (sw *Swarm) Close() {
	if sw.tracker != nil {
		sw.tracker.Close()
	} else {
		sw.ln.Close()
	}
	for _, s := range sw.Seeders {
		s.Close()
	}
}
