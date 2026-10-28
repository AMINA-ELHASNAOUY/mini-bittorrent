package localswarm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"net"

	"mini-bittorrent/bencode"
)

// DHTNode is a one-node "DHT" on localhost that answers every get_peers
// query with a fixed list of peers. Pointing the DHT client's bootstrap list
// at it lets tests exercise a tracker-free download.
type DHTNode struct {
	conn  net.PacketConn
	peers []string
}

// StartDHTNode listens on a random localhost UDP port.
func StartDHTNode(peers []string) (*DHTNode, error) {
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	n := &DHTNode{conn: pc, peers: peers}
	go n.serve()
	return n, nil
}

// Addr is the node's "ip:port".
func (n *DHTNode) Addr() string { return n.conn.LocalAddr().String() }

// Close stops the node.
func (n *DHTNode) Close() { n.conn.Close() }

func (n *DHTNode) serve() {
	buf := make([]byte, 2048)
	for {
		size, from, err := n.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		v, err := bencode.Decode(bufio.NewReader(bytes.NewReader(buf[:size])))
		if err != nil {
			continue
		}
		msg, ok := v.(map[string]interface{})
		if !ok || msg["y"] != "q" || msg["q"] != "get_peers" {
			continue
		}
		var values []interface{}
		for _, p := range n.peers {
			addr, err := net.ResolveTCPAddr("tcp4", p)
			if err != nil {
				continue
			}
			b := make([]byte, 6)
			copy(b, addr.IP.To4())
			binary.BigEndian.PutUint16(b[4:], uint16(addr.Port))
			values = append(values, string(b))
		}
		reply, _ := bencode.Encode(map[string]interface{}{
			"t": msg["t"], "y": "r",
			"r": map[string]interface{}{"id": "localswarm-dht-node!", "token": "t", "values": values},
		})
		n.conn.WriteTo(reply, from)
	}
}
