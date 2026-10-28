package dht

import (
	"context"
	"net"
	"testing"
	"time"
)

// startNetwork starts n full nodes on localhost that all know about node 0,
// and bootstraps each one so their routing tables fill in.
func startNetwork(t *testing.T, n int) []*Client {
	t.Helper()
	var nodes []*Client
	for i := 0; i < n; i++ {
		c, err := NewNode("127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		nodes = append(nodes, c)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	boot := []string{nodes[0].Addr().String()}
	for _, c := range nodes[1:] {
		if _, err := c.Bootstrap(ctx, boot); err != nil {
			t.Fatal(err)
		}
	}
	return nodes
}

func TestAnnounceThenFindThroughTheNetwork(t *testing.T) {
	nodes := startNetwork(t, 25)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	boot := []string{nodes[0].Addr().String()}

	var ih [20]byte
	copy(ih[:], "some-torrent-hash-20")
	seeder := nodes[7]
	accepted, err := seeder.Announce(ctx, ih, 51413, boot)
	if err != nil || accepted == 0 {
		t.Fatalf("announce: %d accepted, %v", accepted, err)
	}

	// A brand-new read-only client, which knows only the bootstrap node, finds the seeder.
	reader, _ := New("127.0.0.1:0")
	defer reader.Close()
	peers, err := reader.GetPeers(ctx, ih, boot, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := net.JoinHostPort("127.0.0.1", "51413")
	if len(peers) != 1 || peers[0] != want {
		t.Fatalf("got %v, want [%s]", peers, want)
	}
}

func TestRoutingTablesFillUp(t *testing.T) {
	nodes := startNetwork(t, 12)
	for i, c := range nodes {
		if c.server.table.size() == 0 {
			t.Errorf("node %d has an empty routing table", i)
		}
	}
}

func TestPingAndFindNode(t *testing.T) {
	nodes := startNetwork(t, 6)
	ctx := context.Background()
	r, err := nodes[1].query(ctx, nodes[0].Addr(), "ping", map[string]interface{}{})
	if err != nil || r["id"] != string(nodes[0].id[:]) {
		t.Fatalf("ping: %v %v", r, err)
	}
	r, err = nodes[1].query(ctx, nodes[0].Addr(), "find_node", map[string]interface{}{"target": string(nodes[3].id[:])})
	if err != nil {
		t.Fatal(err)
	}
	_, found := parseGetPeers(r)
	if len(found) == 0 || found[0].id != nodes[3].id {
		t.Fatalf("find_node should return the target itself first, got %d nodes", len(found))
	}
}

func TestAnnounceNeedsAValidToken(t *testing.T) {
	nodes := startNetwork(t, 2)
	ctx := context.Background()
	_, err := nodes[1].query(ctx, nodes[0].Addr(), "announce_peer", map[string]interface{}{
		"info_hash": string(make([]byte, 20)), "port": int64(6881), "token": "forged!!",
	})
	if err == nil {
		t.Fatal("announce_peer with a made-up token should be rejected")
	}
}

func TestUnknownMethodAndBadID(t *testing.T) {
	nodes := startNetwork(t, 2)
	ctx := context.Background()
	if _, err := nodes[1].query(ctx, nodes[0].Addr(), "frobnicate", map[string]interface{}{}); err == nil {
		t.Error("unknown method should return an error")
	}
}

func TestReadOnlyClientsStayOutOfRoutingTables(t *testing.T) {
	nodes := startNetwork(t, 2)
	before := nodes[0].server.table.size()
	ro, _ := New("127.0.0.1:0")
	defer ro.Close()
	ro.query(context.Background(), nodes[0].Addr(), "ping", map[string]interface{}{})
	if nodes[0].server.table.size() != before {
		t.Error("a read-only client was added to the routing table")
	}
}

func TestPeerStoreExpiry(t *testing.T) {
	s := newPeerStore()
	var ih [20]byte
	s.add(ih, net.IPv4(1, 2, 3, 4), 80)
	s.add(ih, net.IPv4(1, 2, 3, 4), 80) // refresh, not duplicate
	if got := s.get(ih); len(got) != 1 || PeerAddr(got[0]) != "1.2.3.4:80" {
		t.Fatalf("got %v", got)
	}
	s.peers[ih][0].expires = time.Now().Add(-time.Second)
	if len(s.get(ih)) != 0 {
		t.Error("expired peer still returned")
	}
}

func TestCommonPrefixLen(t *testing.T) {
	var a, b [20]byte
	if commonPrefixLen(a, b) != 160 {
		t.Error("identical IDs")
	}
	b[0] = 0x80
	if commonPrefixLen(a, b) != 0 {
		t.Error("first bit differs")
	}
	b[0], b[1] = 0, 0x01
	if commonPrefixLen(a, b) != 15 {
		t.Errorf("got %d, want 15", commonPrefixLen(a, b))
	}
}
