package dht

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"mini-bittorrent/bencode"
)

// A full DHT node (BEP 5) does what the read-only client does, plus:
//   - answers ping, find_node, get_peers and announce_peer from other nodes
//   - keeps a routing table of good nodes, organised in k-buckets by XOR distance
//   - remembers peers that announced themselves, and hands them out to lookups
//   - announces itself as a peer for torrents it seeds
// Write access is protected with tokens: a node may only announce_peer to us
// with a token we gave its IP address in a recent get_peers reply.

const (
	bucketSize     = 8
	peerTTL        = 30 * time.Minute
	maxPeersStored = 200 // per info hash
	tokenRotation  = 5 * time.Minute
)

type server struct {
	table *routingTable
	store *peerStore

	mu         sync.Mutex
	secret     [16]byte
	prevSecret [16]byte
	rotatedAt  time.Time
}

// NewNode opens a UDP socket (addr like ":6881") and runs a full DHT node on it.
func NewNode(addr string) (*Client, error) { return open(addr, true) }

// NewNodeOn runs a full node on an existing packet connection, e.g. the
// non-uTP side of a uTP socket, so the DHT and uTP share one port as other
// clients expect. Close closes pc.
func NewNodeOn(pc net.PacketConn) (*Client, error) { return openOn(pc, true) }

func newServer(self [20]byte) (*server, error) {
	srv := &server{table: newRoutingTable(self), store: newPeerStore(), rotatedAt: time.Now()}
	if _, err := rand.Read(srv.secret[:]); err != nil {
		return nil, err
	}
	srv.prevSecret = srv.secret
	return srv, nil
}

// Addr is the node's UDP address.
func (c *Client) Addr() *net.UDPAddr { return c.conn.LocalAddr().(*net.UDPAddr) }

// Bootstrap fills the routing table by looking up our own ID, which finds the
// nodes closest to us. It returns how many nodes the table knows afterwards.
func (c *Client) Bootstrap(ctx context.Context, bootstrap []string) (int, error) {
	if c.server == nil {
		return 0, errors.New("dht: Bootstrap needs a full node (NewNode)")
	}
	_, _, err := c.lookup(ctx, c.id, bootstrap, 1<<30)
	return c.server.table.size(), err
}

// Announce tells the DHT that we have infoHash and accept peers on port. It
// sends announce_peer to the closest nodes found by a lookup, using the tokens
// they gave us, and returns how many accepted.
func (c *Client) Announce(ctx context.Context, infoHash [20]byte, port int, bootstrap []string) (int, error) {
	if c.server == nil {
		return 0, errors.New("dht: Announce needs a full node (NewNode)")
	}
	_, closest, err := c.lookup(ctx, infoHash, bootstrap, 1<<30)
	if err != nil {
		return 0, err
	}
	accepted := 0
	for i, n := range closest {
		if i == bucketSize {
			break
		}
		if n.token == "" {
			continue
		}
		_, err := c.query(ctx, n.addr, "announce_peer", map[string]interface{}{
			"info_hash": string(infoHash[:]), "port": int64(port), "token": n.token, "implied_port": int64(0),
		})
		if err == nil {
			accepted++
		}
	}
	if accepted == 0 {
		return 0, errors.New("no DHT node accepted the announce")
	}
	return accepted, nil
}

// handleQuery answers a query from another node.
func (c *Client) handleQuery(msg map[string]interface{}, from *net.UDPAddr) {
	srv := c.server
	method, _ := msg["q"].(string)
	args, _ := msg["a"].(map[string]interface{})
	idStr, _ := args["id"].(string)
	if len(idStr) != 20 {
		c.replyError(msg, from, 203, "missing or invalid id")
		return
	}
	var id [20]byte
	copy(id[:], idStr)
	if ro, _ := msg["ro"].(int64); ro != 1 {
		srv.table.add(id, from) // read-only nodes ask not to be added
	}

	r := map[string]interface{}{"id": string(c.id[:])}
	switch method {
	case "ping":
	case "find_node":
		target, ok := hash20(args["target"])
		if !ok {
			c.replyError(msg, from, 203, "invalid target")
			return
		}
		r["nodes"] = compactNodes(srv.table.closest(target, bucketSize))
	case "get_peers":
		ih, ok := hash20(args["info_hash"])
		if !ok {
			c.replyError(msg, from, 203, "invalid info_hash")
			return
		}
		r["token"] = srv.token(from.IP)
		if peers := srv.store.get(ih); len(peers) > 0 {
			values := make([]interface{}, len(peers))
			for i, p := range peers {
				values[i] = p
			}
			r["values"] = values
		} else {
			r["nodes"] = compactNodes(srv.table.closest(ih, bucketSize))
		}
	case "announce_peer":
		ih, ok := hash20(args["info_hash"])
		token, _ := args["token"].(string)
		if !ok || !srv.validToken(token, from.IP) {
			c.replyError(msg, from, 203, "bad token")
			return
		}
		port, _ := args["port"].(int64)
		if implied, _ := args["implied_port"].(int64); implied == 1 {
			port = int64(from.Port)
		}
		if port <= 0 || port > 65535 {
			c.replyError(msg, from, 203, "bad port")
			return
		}
		srv.store.add(ih, from.IP, uint16(port))
	default:
		c.replyError(msg, from, 204, "method unknown")
		return
	}
	c.send(from, map[string]interface{}{"t": msg["t"], "y": "r", "r": r})
}

func (c *Client) replyError(msg map[string]interface{}, to *net.UDPAddr, code int64, text string) {
	c.send(to, map[string]interface{}{"t": msg["t"], "y": "e", "e": []interface{}{code, text}})
}

func (c *Client) send(to *net.UDPAddr, m map[string]interface{}) {
	if t, ok := m["t"].(string); !ok || t == "" {
		m["t"] = "aa"
	}
	pkt, err := bencode.Encode(m)
	if err == nil {
		c.conn.WriteTo(pkt, to)
	}
}

// -- tokens --------------------------------------------------------------

func (s *server) rotate() {
	if time.Since(s.rotatedAt) >= tokenRotation {
		s.prevSecret = s.secret
		rand.Read(s.secret[:])
		s.rotatedAt = time.Now()
	}
}

func makeToken(secret [16]byte, ip net.IP) string {
	h := sha1.Sum(append(secret[:], ip.To16()...))
	return string(h[:8])
}

func (s *server) token(ip net.IP) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rotate()
	return makeToken(s.secret, ip)
}

// validToken accepts tokens from the current or previous secret, so a token
// stays valid for 5-10 minutes.
func (s *server) validToken(tok string, ip net.IP) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rotate()
	return tok != "" && (tok == makeToken(s.secret, ip) || tok == makeToken(s.prevSecret, ip))
}

// -- routing table -------------------------------------------------------

type tableEntry struct {
	id       [20]byte
	addr     *net.UDPAddr
	lastSeen time.Time
}

// routingTable keeps up to bucketSize nodes per bucket, where bucket i holds
// nodes whose ID shares exactly i leading bits with ours. That gives many
// buckets for nodes near us and few for the far-away majority, so we know our
// own neighbourhood well and the rest of the ID space roughly.
type routingTable struct {
	mu      sync.Mutex
	self    [20]byte
	buckets [160][]tableEntry
}

func newRoutingTable(self [20]byte) *routingTable { return &routingTable{self: self} }

func commonPrefixLen(a, b [20]byte) int {
	for i := 0; i < 20; i++ {
		if x := a[i] ^ b[i]; x != 0 {
			n := 0
			for x&0x80 == 0 {
				x <<= 1
				n++
			}
			return i*8 + n
		}
	}
	return 160
}

func (t *routingTable) add(id [20]byte, addr *net.UDPAddr) {
	if id == t.self || addr == nil || addr.Port == 0 {
		return
	}
	i := commonPrefixLen(id, t.self)
	if i >= 160 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	b := t.buckets[i]
	for j := range b {
		if b[j].id == id {
			b[j].addr, b[j].lastSeen = addr, time.Now()
			return
		}
	}
	if len(b) < bucketSize {
		t.buckets[i] = append(b, tableEntry{id, addr, time.Now()})
		return
	}
	// Bucket full: replace the node we heard from least recently if it has
	// been quiet for a while. (The full protocol pings it first; we keep it simple.)
	oldest := 0
	for j := range b {
		if b[j].lastSeen.Before(b[oldest].lastSeen) {
			oldest = j
		}
	}
	if time.Since(b[oldest].lastSeen) > 15*time.Minute {
		b[oldest] = tableEntry{id, addr, time.Now()}
	}
}

func (t *routingTable) closest(target [20]byte, k int) []tableEntry {
	t.mu.Lock()
	var all []tableEntry
	for _, b := range t.buckets {
		all = append(all, b...)
	}
	t.mu.Unlock()
	sort.Slice(all, func(i, j int) bool {
		di, dj := xor(all[i].id, target), xor(all[j].id, target)
		return bytes.Compare(di[:], dj[:]) < 0
	})
	if len(all) > k {
		all = all[:k]
	}
	return all
}

func (t *routingTable) size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, b := range t.buckets {
		n += len(b)
	}
	return n
}

func compactNodes(entries []tableEntry) string {
	var out []byte
	for _, e := range entries {
		ip := e.addr.IP.To4()
		if ip == nil {
			continue // IPv6 nodes go in "nodes6", which we don't serve
		}
		out = append(out, e.id[:]...)
		out = append(out, ip...)
		out = binary.BigEndian.AppendUint16(out, uint16(e.addr.Port))
	}
	return string(out)
}

// -- peer store ----------------------------------------------------------

type storedPeer struct {
	compact string // 6-byte compact IPv4 address
	expires time.Time
}

type peerStore struct {
	mu    sync.Mutex
	peers map[[20]byte][]storedPeer
}

func newPeerStore() *peerStore { return &peerStore{peers: map[[20]byte][]storedPeer{}} }

func (s *peerStore) add(ih [20]byte, ip net.IP, port uint16) {
	ip4 := ip.To4()
	if ip4 == nil {
		return
	}
	c := string(binary.BigEndian.AppendUint16(append([]byte(nil), ip4...), port))
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.prune(ih)
	for i := range list {
		if list[i].compact == c {
			list[i].expires = time.Now().Add(peerTTL)
			return
		}
	}
	if len(list) >= maxPeersStored {
		list = list[1:]
	}
	s.peers[ih] = append(list, storedPeer{c, time.Now().Add(peerTTL)})
}

func (s *peerStore) get(ih [20]byte) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.prune(ih)
	out := make([]string, len(list))
	for i, p := range list {
		out[i] = p.compact
	}
	return out
}

// prune drops expired peers; s.mu must be held.
func (s *peerStore) prune(ih [20]byte) []storedPeer {
	list := s.peers[ih][:0]
	for _, p := range s.peers[ih] {
		if time.Now().Before(p.expires) {
			list = append(list, p)
		}
	}
	if len(list) == 0 {
		delete(s.peers, ih)
		return nil
	}
	s.peers[ih] = list
	return list
}

func hash20(v interface{}) ([20]byte, bool) {
	var h [20]byte
	s, ok := v.(string)
	if !ok || len(s) != 20 {
		return h, false
	}
	copy(h[:], s)
	return h, true
}

// PeerAddr formats a compact peer as "ip:port" (exported for tests and logs).
func PeerAddr(compact string) string {
	if len(compact) != 6 {
		return ""
	}
	return net.JoinHostPort(net.IP([]byte(compact[:4])).String(),
		strconv.Itoa(int(binary.BigEndian.Uint16([]byte(compact[4:])))))
}
