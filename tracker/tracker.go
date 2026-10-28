// Package tracker talks to an HTTP BitTorrent tracker to discover peers.
package tracker

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"mini-bittorrent/bencode"
)

// Peer is a single peer address returned by the tracker.
type Peer struct {
	IP   net.IP
	Port uint16
}

func (p Peer) String() string {
	return net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.Port)))
}

// Request holds what the tracker needs to know about us.
type Request struct {
	Announce string
	InfoHash [20]byte
	PeerID   [20]byte
	Port     uint16
	Left     int64 // bytes we still need
}

// Response is the parsed tracker reply.
type Response struct {
	Interval time.Duration
	Peers    []Peer
}

// BuildURL appends the announce query parameters to the tracker URL.
func BuildURL(req Request) (string, error) {
	base, err := url.Parse(req.Announce)
	if err != nil {
		return "", err
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return "", fmt.Errorf("unsupported tracker scheme %q (only http/https)", base.Scheme)
	}
	params := base.Query()
	params.Set("info_hash", string(req.InfoHash[:]))
	params.Set("peer_id", string(req.PeerID[:]))
	params.Set("port", strconv.Itoa(int(req.Port)))
	params.Set("uploaded", "0")
	params.Set("downloaded", "0")
	params.Set("left", strconv.FormatInt(req.Left, 10))
	params.Set("compact", "1")
	params.Set("event", "started")
	base.RawQuery = params.Encode()
	return base.String(), nil
}

// Announce contacts the tracker and returns the list of peers.
func Announce(client *http.Client, req Request) (*Response, error) {
	u, err := BuildURL(req)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := client.Get(u)
	if err != nil {
		return nil, fmt.Errorf("tracker request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tracker returned HTTP %d", resp.StatusCode)
	}
	return parseResponse(bufio.NewReader(resp.Body))
}

func parseResponse(r *bufio.Reader) (*Response, error) {
	decoded, err := bencode.Decode(r)
	if err != nil {
		return nil, fmt.Errorf("invalid tracker response: %w", err)
	}
	dict, ok := decoded.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("tracker response is not a dictionary")
	}
	if reason, ok := dict["failure reason"].(string); ok {
		return nil, fmt.Errorf("tracker failure: %s", reason)
	}

	interval, _ := dict["interval"].(int64)
	res := &Response{Interval: time.Duration(interval) * time.Second}

	switch peers := dict["peers"].(type) {
	case string: // compact format: 6 bytes per peer (4 IP + 2 port)
		res.Peers, err = ParseCompactPeers([]byte(peers))
		if err != nil {
			return nil, err
		}
	case []interface{}: // dictionary format
		for _, p := range peers {
			pd, ok := p.(map[string]interface{})
			if !ok {
				continue
			}
			ipStr, _ := pd["ip"].(string)
			port, _ := pd["port"].(int64)
			ip := net.ParseIP(ipStr)
			if ip == nil || port <= 0 || port > 65535 {
				continue
			}
			res.Peers = append(res.Peers, Peer{IP: ip, Port: uint16(port)})
		}
	default:
		return nil, fmt.Errorf("tracker response has no peers")
	}
	return res, nil
}

// ParseCompactPeers decodes the compact peer list (BEP 23).
func ParseCompactPeers(buf []byte) ([]Peer, error) {
	const peerSize = 6
	if len(buf)%peerSize != 0 {
		return nil, fmt.Errorf("malformed compact peer list of %d bytes", len(buf))
	}
	peers := make([]Peer, len(buf)/peerSize)
	for i := range peers {
		off := i * peerSize
		peers[i] = Peer{
			IP:   net.IP(append([]byte(nil), buf[off:off+4]...)),
			Port: binary.BigEndian.Uint16(buf[off+4 : off+6]),
		}
	}
	return peers, nil
}
