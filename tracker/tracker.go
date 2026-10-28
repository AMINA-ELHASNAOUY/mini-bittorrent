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
	"strings"
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

// Announce contacts one tracker (http, https or udp) and returns its peers.
// client is only used for HTTP trackers and may be nil.
func Announce(client *http.Client, req Request) (*Response, error) {
	if strings.HasPrefix(req.Announce, "udp://") {
		return announceUDP(req, udpTimeout)
	}
	return announceHTTP(client, req)
}

// AnnounceAll asks every tracker in parallel and merges the peers they return,
// along with the error of each tracker that failed. Once some tracker has
// answered, it waits at most `grace` for slower ones instead of stalling on
// dead trackers. An empty peer list means every tracker failed.
func AnnounceAll(trackers []string, req Request, grace time.Duration) ([]Peer, []error) {
	type result struct {
		url   string
		peers []Peer
		err   error
	}
	results := make(chan result, len(trackers))
	for _, u := range trackers {
		go func(u string) {
			r := req
			r.Announce = u
			res, err := Announce(nil, r)
			if err != nil {
				results <- result{url: u, err: err}
				return
			}
			results <- result{url: u, peers: res.Peers}
		}(u)
	}

	seen := map[string]bool{}
	var peers []Peer
	var errs []error
	var graceTimer <-chan time.Time
	for pending := len(trackers); pending > 0; pending-- {
		var r result
		select {
		case r = <-results:
		case <-graceTimer:
			return peers, errs
		}
		if r.err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.url, r.err))
			continue
		}
		for _, p := range r.peers {
			if key := p.String(); !seen[key] {
				seen[key] = true
				peers = append(peers, p)
			}
		}
		if graceTimer == nil && len(peers) > 0 {
			graceTimer = time.After(grace)
		}
	}
	return peers, errs
}

func announceHTTP(client *http.Client, req Request) (*Response, error) {
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
	case nil:
	default:
		return nil, fmt.Errorf("tracker response has a malformed peer list")
	}
	// IPv6 peers come separately in compact form, 18 bytes each (BEP 7).
	if peers6, ok := dict["peers6"].(string); ok {
		for off := 0; off+18 <= len(peers6); off += 18 {
			res.Peers = append(res.Peers, Peer{
				IP:   net.IP(append([]byte(nil), peers6[off:off+16]...)),
				Port: binary.BigEndian.Uint16([]byte(peers6[off+16 : off+18])),
			})
		}
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
