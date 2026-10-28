// Package dht finds peers for a torrent without a tracker, using the
// BitTorrent Mainline DHT (BEP 5).
//
// The DHT is a distributed hash table spread over millions of clients. Every
// node has a random 160-bit ID, and info hashes live in the same ID space.
// Nodes whose IDs are close to an info hash (by XOR distance) keep track of
// the peers for that torrent. To find them we start from well-known bootstrap
// nodes and repeatedly ask the closest nodes we know about for nodes even
// closer to the hash, until someone answers with peers.
//
// This is a read-only client (BEP 43): it looks things up but doesn't store
// data for others.
package dht

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"mini-bittorrent/bencode"
)

// DefaultBootstrap are public, long-running nodes used to join the DHT.
var DefaultBootstrap = []string{
	"router.bittorrent.com:6881",
	"dht.transmissionbt.com:6881",
	"router.utorrent.com:6881",
	"dht.libtorrent.org:25401",
}

const (
	alpha        = 8 // queries in flight per round
	kClosest     = 16
	queryTimeout = 2 * time.Second
	maxRounds    = 25
)

// Client sends KRPC queries over one UDP socket.
type Client struct {
	conn *net.UDPConn
	id   [20]byte

	mu      sync.Mutex
	nextTx  uint16
	pending map[string]chan map[string]interface{}
	closed  chan struct{}
}

// New opens a UDP socket (addr like ":0") and starts reading replies.
func New(addr string) (*Client, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	c := &Client{conn: conn, pending: map[string]chan map[string]interface{}{}, closed: make(chan struct{})}
	if _, err := rand.Read(c.id[:]); err != nil {
		conn.Close()
		return nil, err
	}
	go c.readLoop()
	return c, nil
}

// Close shuts the socket.
func (c *Client) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return c.conn.Close()
}

func (c *Client) readLoop() {
	buf := make([]byte, 65536)
	for {
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		v, err := bencode.Decode(bufioFrom(buf[:n]))
		if err != nil {
			continue
		}
		msg, ok := v.(map[string]interface{})
		if !ok {
			continue
		}
		tx, _ := msg["t"].(string)
		c.mu.Lock()
		ch := c.pending[tx]
		delete(c.pending, tx)
		c.mu.Unlock()
		if ch != nil {
			ch <- msg
		}
	}
}

// query sends one KRPC query and waits for the matching reply.
func (c *Client) query(ctx context.Context, addr *net.UDPAddr, method string, args map[string]interface{}) (map[string]interface{}, error) {
	c.mu.Lock()
	c.nextTx++
	tx := string([]byte{byte(c.nextTx >> 8), byte(c.nextTx)})
	ch := make(chan map[string]interface{}, 1)
	c.pending[tx] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, tx)
		c.mu.Unlock()
	}()

	args["id"] = string(c.id[:])
	pkt, err := bencode.Encode(map[string]interface{}{
		"t": tx, "y": "q", "q": method, "a": args, "ro": int64(1),
	})
	if err != nil {
		return nil, err
	}
	if _, err := c.conn.WriteToUDP(pkt, addr); err != nil {
		return nil, err
	}

	timer := time.NewTimer(queryTimeout)
	defer timer.Stop()
	select {
	case msg := <-ch:
		switch msg["y"] {
		case "r":
			r, ok := msg["r"].(map[string]interface{})
			if !ok {
				return nil, errors.New("malformed reply")
			}
			return r, nil
		case "e":
			return nil, fmt.Errorf("node error: %v", msg["e"])
		}
		return nil, errors.New("unexpected reply type")
	case <-timer.C:
		return nil, errors.New("timeout")
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, net.ErrClosed
	}
}

// node is a DHT node we know about. Bootstrap nodes start with an unknown ID.
type node struct {
	id      [20]byte
	knownID bool
	addr    *net.UDPAddr
	queried bool
}

// GetPeers walks the DHT towards infoHash and returns up to `want` peer
// addresses ("ip:port"). It stops early once it has enough peers, when no
// closer nodes turn up, or when ctx expires — whichever comes first.
func (c *Client) GetPeers(ctx context.Context, infoHash [20]byte, bootstrap []string, want int) ([]string, error) {
	nodes := map[string]*node{}
	for _, b := range bootstrap {
		addr, err := net.ResolveUDPAddr("udp4", b)
		if err != nil {
			continue
		}
		nodes[addr.String()] = &node{addr: addr}
	}
	if len(nodes) == 0 {
		return nil, errors.New("could not resolve any bootstrap node")
	}

	seen := map[string]bool{}
	var peers []string
	var mu sync.Mutex

	for round := 0; round < maxRounds && len(peers) < want; round++ {
		batch := closestUnqueried(nodes, infoHash)
		if len(batch) == 0 {
			break
		}
		var wg sync.WaitGroup
		for _, n := range batch {
			n.queried = true
			wg.Add(1)
			go func(n *node) {
				defer wg.Done()
				r, err := c.query(ctx, n.addr, "get_peers", map[string]interface{}{"info_hash": string(infoHash[:])})
				if err != nil {
					return
				}
				newPeers, newNodes := parseGetPeers(r)
				mu.Lock()
				defer mu.Unlock()
				if id, ok := r["id"].(string); ok && len(id) == 20 {
					copy(n.id[:], id)
					n.knownID = true
				}
				for _, p := range newPeers {
					if !seen[p] {
						seen[p] = true
						peers = append(peers, p)
					}
				}
				for _, nn := range newNodes {
					if _, ok := nodes[nn.addr.String()]; !ok {
						nodes[nn.addr.String()] = nn
					}
				}
			}(n)
		}
		wg.Wait()
		if ctx.Err() != nil {
			break
		}
	}
	if len(peers) == 0 {
		return nil, errors.New("DHT lookup found no peers")
	}
	if len(peers) > want {
		peers = peers[:want]
	}
	return peers, nil
}

// closestUnqueried returns up to alpha not-yet-queried nodes from the
// kClosest nodes we know, closest to target first. Nodes with unknown IDs
// (bootstrap) are tried first, since we can't rank them yet.
func closestUnqueried(nodes map[string]*node, target [20]byte) []*node {
	all := make([]*node, 0, len(nodes))
	for _, n := range nodes {
		all = append(all, n)
	}
	sort.Slice(all, func(i, j int) bool {
		a, b := all[i], all[j]
		if a.knownID != b.knownID {
			return !a.knownID
		}
		da, db := xor(a.id, target), xor(b.id, target)
		return bytes.Compare(da[:], db[:]) < 0
	})

	var out []*node
	ranked := 0
	for _, n := range all {
		if n.knownID {
			ranked++
			if ranked > kClosest {
				break
			}
		}
		if !n.queried {
			out = append(out, n)
			if len(out) == alpha {
				break
			}
		}
	}
	return out
}

func xor(a, b [20]byte) (d [20]byte) {
	for i := range a {
		d[i] = a[i] ^ b[i]
	}
	return d
}

// parseGetPeers extracts peers ("values", 6-byte compact) and closer nodes
// ("nodes", 26-byte compact: 20-byte ID + 4-byte IP + 2-byte port).
func parseGetPeers(r map[string]interface{}) ([]string, []*node) {
	var peers []string
	if values, ok := r["values"].([]interface{}); ok {
		for _, v := range values {
			s, ok := v.(string)
			if !ok || len(s) != 6 {
				continue
			}
			ip := net.IP([]byte(s[:4]))
			port := binary.BigEndian.Uint16([]byte(s[4:6]))
			if port == 0 {
				continue
			}
			peers = append(peers, net.JoinHostPort(ip.String(), fmt.Sprint(port)))
		}
	}
	var nodes []*node
	if compact, ok := r["nodes"].(string); ok {
		for off := 0; off+26 <= len(compact); off += 26 {
			n := &node{knownID: true}
			copy(n.id[:], compact[off:off+20])
			ip := net.IP([]byte(compact[off+20 : off+24]))
			port := binary.BigEndian.Uint16([]byte(compact[off+24 : off+26]))
			if port == 0 || ip.IsUnspecified() {
				continue
			}
			n.addr = &net.UDPAddr{IP: ip, Port: int(port)}
			nodes = append(nodes, n)
		}
	}
	return peers, nodes
}

func bufioFrom(b []byte) *bufio.Reader { return bufio.NewReader(bytes.NewReader(b)) }
