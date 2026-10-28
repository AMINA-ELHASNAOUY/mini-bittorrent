package dht

import (
	"context"
	"encoding/binary"
	"net"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"mini-bittorrent/bencode"
)

// fakeNode is a tiny DHT node: it answers get_peers with the nodes it knows,
// or with peers if it holds any for the info hash.
type fakeNode struct {
	id      [20]byte
	conn    net.PacketConn
	knows   []*fakeNode
	peers   []string // "ip:port"
	queries atomic.Int32
}

func newFakeNode(t *testing.T, firstByte byte) *fakeNode {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	n := &fakeNode{conn: pc}
	n.id[0] = firstByte
	return n
}

func (n *fakeNode) addr() *net.UDPAddr { return n.conn.LocalAddr().(*net.UDPAddr) }

func (n *fakeNode) serve() {
	buf := make([]byte, 2048)
	for {
		size, from, err := n.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		v, err := bencode.Decode(bufioFrom(buf[:size]))
		if err != nil {
			continue
		}
		msg := v.(map[string]interface{})
		if msg["y"] != "q" || msg["q"] != "get_peers" {
			continue
		}
		n.queries.Add(1)
		r := map[string]interface{}{"id": string(n.id[:]), "token": "tok"}
		if len(n.peers) > 0 {
			var values []interface{}
			for _, p := range n.peers {
				addr, _ := net.ResolveTCPAddr("tcp4", p)
				b := make([]byte, 6)
				copy(b, addr.IP.To4())
				binary.BigEndian.PutUint16(b[4:], uint16(addr.Port))
				values = append(values, string(b))
			}
			r["values"] = values
		} else {
			var compact []byte
			for _, k := range n.knows {
				compact = append(compact, k.id[:]...)
				compact = append(compact, k.addr().IP.To4()...)
				compact = binary.BigEndian.AppendUint16(compact, uint16(k.addr().Port))
			}
			r["nodes"] = string(compact)
		}
		reply, _ := bencode.Encode(map[string]interface{}{"t": msg["t"], "y": "r", "r": r})
		n.conn.WriteTo(reply, from)
	}
}

func TestGetPeersWalksTowardsTheHash(t *testing.T) {
	var infoHash [20]byte
	infoHash[0] = 0xF0

	// bootstrap → {far, mid} → mid knows close → close holds the peers.
	bootstrap := newFakeNode(t, 0x00)
	far := newFakeNode(t, 0x0F)
	mid := newFakeNode(t, 0xC0)
	close := newFakeNode(t, 0xF1)
	close.peers = []string{"10.1.2.3:6881", "10.9.9.9:51413"}
	bootstrap.knows = []*fakeNode{far, mid}
	mid.knows = []*fakeNode{close}
	far.knows = nil
	for _, n := range []*fakeNode{bootstrap, far, mid, close} {
		go n.serve()
	}

	c, err := New("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	peers, err := c.GetPeers(ctx, infoHash, []string{bootstrap.addr().String()}, 50)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(peers)
	if len(peers) != 2 || peers[0] != "10.1.2.3:6881" || peers[1] != "10.9.9.9:51413" {
		t.Fatalf("got %v", peers)
	}
	if close.queries.Load() == 0 {
		t.Error("the closest node was never asked")
	}
}

func TestGetPeersNoPeers(t *testing.T) {
	lonely := newFakeNode(t, 0x01)
	go lonely.serve()
	c, _ := New("127.0.0.1:0")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.GetPeers(ctx, [20]byte{0xAA}, []string{lonely.addr().String()}, 10); err == nil {
		t.Fatal("expected an error when the DHT has no peers")
	}
}

func TestQueryTimeoutOnSilentNode(t *testing.T) {
	silent, _ := net.ListenPacket("udp4", "127.0.0.1:0") // never answers
	defer silent.Close()
	c, _ := New("127.0.0.1:0")
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.GetPeers(ctx, [20]byte{1}, []string{silent.LocalAddr().String()}, 10); err == nil {
		t.Fatal("expected an error")
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("lookup ignored the context deadline (%s)", time.Since(start))
	}
}

func TestClosestUnqueriedOrdersByXOR(t *testing.T) {
	target := [20]byte{0x80}
	mk := func(b byte, port int) *node {
		n := &node{knownID: true, addr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}}
		n.id[0] = b
		return n
	}
	nodes := map[string]*node{"a": mk(0x00, 1), "b": mk(0x81, 2), "c": mk(0x90, 3)}
	got := closestUnqueried(nodes, target)
	if got[0].id[0] != 0x81 || got[1].id[0] != 0x90 || got[2].id[0] != 0x00 {
		t.Fatalf("order: %x %x %x", got[0].id[0], got[1].id[0], got[2].id[0])
	}
}
