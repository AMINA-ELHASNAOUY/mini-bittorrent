// Package metadata downloads a torrent's info dictionary from peers
// (BEP 9, "ut_metadata", over the extension protocol of BEP 10).
// This is what turns a magnet link — just a hash — into a full torrent.
package metadata

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"net"
	"time"

	"mini-bittorrent/peer"
)

const (
	// BlockSize is the size of one metadata piece.
	BlockSize = 16384
	// MaxSize caps the metadata we accept, so a peer can't make us allocate gigabytes.
	MaxSize = 16 << 20
	// LocalID is the message ID we ask peers to use for ut_metadata messages to us.
	LocalID = 1

	msgRequest = 0
	msgData    = 1
	msgReject  = 2
)

// FetchAny tries peers (several at a time) until one delivers metadata that
// hashes to infoHash, and returns the raw bencoded info dictionary.
func FetchAny(ctx context.Context, peers []string, infoHash, peerID [20]byte) ([]byte, error) {
	return FetchAnyDial(ctx, nil, peers, infoHash, peerID)
}

// DialFunc opens a connection to a peer; nil means TCP.
type DialFunc func(addr string) (net.Conn, error)

// FetchAnyDial is FetchAny with a custom dialer (e.g. one that also tries uTP).
func FetchAnyDial(ctx context.Context, dial DialFunc, peers []string, infoHash, peerID [20]byte) ([]byte, error) {
	if len(peers) == 0 {
		return nil, errors.New("no peers to fetch metadata from")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		info []byte
		err  error
	}
	const parallel = 8
	results := make(chan result, len(peers))
	sem := make(chan struct{}, parallel)
	for _, addr := range peers {
		go func(addr string) {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results <- result{err: ctx.Err()}
				return
			}
			info, err := fetch(ctx, dial, addr, infoHash, peerID)
			if err != nil {
				err = fmt.Errorf("%s: %w", addr, err)
			}
			results <- result{info, err}
		}(addr)
	}

	var lastErr error
	for range peers {
		r := <-results
		if r.err == nil {
			return r.info, nil
		}
		lastErr = r.err
	}
	return nil, fmt.Errorf("no peer sent the metadata (last error: %w)", lastErr)
}

// Fetch downloads the metadata from a single peer.
func Fetch(ctx context.Context, addr string, infoHash, peerID [20]byte) ([]byte, error) {
	return fetch(ctx, nil, addr, infoHash, peerID)
}

func fetch(ctx context.Context, dial DialFunc, addr string, infoHash, peerID [20]byte) ([]byte, error) {
	var conn net.Conn
	var err error
	if dial != nil {
		conn, err = dial(addr)
	} else {
		var d net.Dialer
		dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err = d.DialContext(dialCtx, "tcp", addr)
		cancel()
	}
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	hs, err := peer.DoHandshakeExt(conn, infoHash, peerID)
	if err != nil {
		return nil, err
	}
	if !hs.SupportsExtensions() {
		return nil, errors.New("peer doesn't support the extension protocol")
	}
	conn.SetDeadline(time.Now().Add(20 * time.Second))

	hello, err := peer.FormatExtended(0, map[string]interface{}{
		"m": map[string]interface{}{"ut_metadata": int64(LocalID)},
		"v": "mini-bittorrent",
	}, nil)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(hello.Serialize()); err != nil {
		return nil, err
	}

	// Wait for the peer's extension handshake: it tells us which ID the
	// peer uses for ut_metadata and how big the metadata is.
	var remoteID byte
	var size int
	for remoteID == 0 {
		msg, err := peer.ReadMessage(conn)
		if err != nil {
			return nil, err
		}
		if msg == nil || msg.ID != peer.MsgExtended {
			continue // bitfield, have, etc. aren't relevant here
		}
		extID, dict, _, err := peer.ParseExtended(msg)
		if err != nil || extID != 0 {
			continue
		}
		m, _ := dict["m"].(map[string]interface{})
		id, _ := m["ut_metadata"].(int64)
		sz, _ := dict["metadata_size"].(int64)
		if id <= 0 || id > 255 {
			return nil, errors.New("peer doesn't support ut_metadata")
		}
		if sz <= 0 || sz > MaxSize {
			return nil, fmt.Errorf("peer reports an invalid metadata size %d", sz)
		}
		remoteID, size = byte(id), int(sz)
	}

	numPieces := (size + BlockSize - 1) / BlockSize
	for i := 0; i < numPieces; i++ {
		req, _ := peer.FormatExtended(remoteID, map[string]interface{}{"msg_type": int64(msgRequest), "piece": int64(i)}, nil)
		if _, err := conn.Write(req.Serialize()); err != nil {
			return nil, err
		}
	}

	buf := make([]byte, size)
	got := make([]bool, numPieces)
	for received := 0; received < numPieces; {
		msg, err := peer.ReadMessage(conn)
		if err != nil {
			return nil, err
		}
		if msg == nil || msg.ID != peer.MsgExtended {
			continue
		}
		extID, dict, data, err := peer.ParseExtended(msg)
		if err != nil || extID != LocalID {
			continue
		}
		msgType, _ := dict["msg_type"].(int64)
		piece, _ := dict["piece"].(int64)
		switch msgType {
		case msgReject:
			return nil, fmt.Errorf("peer rejected metadata piece %d", piece)
		case msgData:
		default:
			continue
		}
		if piece < 0 || int(piece) >= numPieces || got[piece] {
			continue
		}
		want := BlockSize
		if int(piece) == numPieces-1 {
			want = size - int(piece)*BlockSize
		}
		if len(data) != want {
			return nil, fmt.Errorf("metadata piece %d has %d bytes, want %d", piece, len(data), want)
		}
		copy(buf[int(piece)*BlockSize:], data)
		got[piece] = true
		received++
	}

	if sha1.Sum(buf) != infoHash {
		return nil, errors.New("metadata from peer does not match the info hash")
	}
	return buf, nil
}
