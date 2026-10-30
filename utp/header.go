// Package utp implements uTP, the Micro Transport Protocol (BEP 29): a
// reliable, ordered byte stream over UDP that BitTorrent clients use instead
// of TCP. Its point is LEDBAT congestion control: it measures queuing delay
// on the path and backs off as soon as delay rises, so a torrent running in
// the background doesn't make the rest of your internet slow.
//
// A Socket owns one UDP port. It hands uTP packets to their connections and
// passes everything else (DHT messages share the port in practice) to the
// net.PacketConn returned by Socket.PacketConn. Conn implements net.Conn and
// Socket implements net.Listener, so the peer wire protocol runs over uTP
// unchanged.
package utp

import (
	"encoding/binary"
	"errors"
)

// Packet types.
const (
	stData  = 0 // carries payload
	stFin   = 1 // last packet of the stream
	stState = 2 // ack only; doesn't use a sequence number
	stReset = 3 // abort the connection
	stSyn   = 4 // open a connection
)

const (
	version   = 1
	headerLen = 20
	// extSack is the selective-ack extension: a bitmask of packets received
	// beyond ack_nr+1.
	extSack = 1
)

// header is the 20-byte uTP header:
//
//	0       4       8               16              24              32
//	+-------+-------+---------------+---------------+---------------+
//	| type  | ver   | extension     | connection_id                 |
//	+-------+-------+---------------+---------------+---------------+
//	| timestamp_microseconds                                        |
//	+---------------+---------------+---------------+---------------+
//	| timestamp_difference_microseconds                             |
//	+---------------+---------------+---------------+---------------+
//	| wnd_size                                                      |
//	+---------------+---------------+---------------+---------------+
//	| seq_nr                        | ack_nr                        |
//	+---------------+---------------+---------------+---------------+
type header struct {
	typ    uint8
	connID uint16
	ts     uint32 // sender's clock when sent, in microseconds
	tsDiff uint32 // sender's clock minus our ts from the last packet it got: the one-way delay sample
	wnd    uint32 // bytes the sender can still receive
	seq    uint16
	ack    uint16
	sack   []byte // selective ack bitmask, if present
}

func (h *header) marshal(payload []byte) []byte {
	n := headerLen
	if len(h.sack) > 0 {
		n += 2 + len(h.sack)
	}
	b := make([]byte, n+len(payload))
	b[0] = h.typ<<4 | version
	binary.BigEndian.PutUint16(b[2:], h.connID)
	binary.BigEndian.PutUint32(b[4:], h.ts)
	binary.BigEndian.PutUint32(b[8:], h.tsDiff)
	binary.BigEndian.PutUint32(b[12:], h.wnd)
	binary.BigEndian.PutUint16(b[16:], h.seq)
	binary.BigEndian.PutUint16(b[18:], h.ack)
	if len(h.sack) > 0 {
		b[1] = extSack
		b[20] = 0 // no further extension
		b[21] = byte(len(h.sack))
		copy(b[22:], h.sack)
	}
	copy(b[n:], payload)
	return b
}

var errNotUTP = errors.New("not a uTP packet")

// looksLikeUTP is a cheap test used to tell uTP packets from other traffic
// on a shared port: DHT messages are bencoded dictionaries and start with
// 'd' (0x64), which can't be mistaken for type<<4 | version 1.
func looksLikeUTP(b []byte) bool {
	return len(b) >= headerLen && b[0]&0x0f == version && b[0]>>4 <= stSyn
}

// unmarshal parses a packet, walking (and bounds-checking) the extension chain.
func unmarshal(b []byte) (*header, []byte, error) {
	if !looksLikeUTP(b) {
		return nil, nil, errNotUTP
	}
	h := &header{
		typ:    b[0] >> 4,
		connID: binary.BigEndian.Uint16(b[2:]),
		ts:     binary.BigEndian.Uint32(b[4:]),
		tsDiff: binary.BigEndian.Uint32(b[8:]),
		wnd:    binary.BigEndian.Uint32(b[12:]),
		seq:    binary.BigEndian.Uint16(b[16:]),
		ack:    binary.BigEndian.Uint16(b[18:]),
	}
	ext := b[1]
	rest := b[headerLen:]
	for ext != 0 {
		if len(rest) < 2 || len(rest) < 2+int(rest[1]) {
			return nil, nil, errors.New("truncated uTP extension")
		}
		next, l := rest[0], int(rest[1])
		if ext == extSack {
			h.sack = rest[2 : 2+l]
		}
		rest = rest[2+l:]
		ext = next
	}
	return h, rest, nil
}

// seqLess compares 16-bit sequence numbers that wrap around.
func seqLess(a, b uint16) bool { return int16(a-b) < 0 }
