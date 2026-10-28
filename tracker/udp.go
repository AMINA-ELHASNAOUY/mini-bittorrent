package tracker

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/url"
	"time"
)

// UDP tracker protocol (BEP 15): a two-step exchange over UDP.
//  1. connect:  we send a magic protocol ID, the tracker returns a connection ID
//  2. announce: we send the connection ID + torrent info, the tracker returns peers
// UDP can drop packets, so every request is retried with a growing timeout.

const (
	udpProtocolID   = 0x41727101980
	actionConnect   = 0
	actionAnnounce  = 1
	actionError     = 3
	udpEventStarted = 2
	udpRetries      = 3
)

// udpTimeout is the wait for the first reply; it doubles on each retry.
var udpTimeout = 3 * time.Second

func announceUDP(req Request, timeout time.Duration) (*Response, error) {
	u, err := url.Parse(req.Announce)
	if err != nil {
		return nil, err
	}
	if u.Port() == "" {
		return nil, fmt.Errorf("udp tracker URL %q has no port", req.Announce)
	}
	conn, err := net.Dial("udp", u.Host)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	connID, err := udpConnect(conn, timeout)
	if err != nil {
		return nil, fmt.Errorf("udp connect: %w", err)
	}
	return udpAnnounce(conn, connID, req, timeout)
}

func udpConnect(conn net.Conn, timeout time.Duration) (uint64, error) {
	txID, err := randomUint32()
	if err != nil {
		return 0, err
	}
	pkt := make([]byte, 16)
	binary.BigEndian.PutUint64(pkt[0:8], udpProtocolID)
	binary.BigEndian.PutUint32(pkt[8:12], actionConnect)
	binary.BigEndian.PutUint32(pkt[12:16], txID)

	resp, err := udpRoundTrip(conn, pkt, txID, actionConnect, 16, timeout)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(resp[8:16]), nil
}

func udpAnnounce(conn net.Conn, connID uint64, req Request, timeout time.Duration) (*Response, error) {
	txID, err := randomUint32()
	if err != nil {
		return nil, err
	}
	key, err := randomUint32()
	if err != nil {
		return nil, err
	}
	pkt := make([]byte, 98)
	binary.BigEndian.PutUint64(pkt[0:8], connID)
	binary.BigEndian.PutUint32(pkt[8:12], actionAnnounce)
	binary.BigEndian.PutUint32(pkt[12:16], txID)
	copy(pkt[16:36], req.InfoHash[:])
	copy(pkt[36:56], req.PeerID[:])
	binary.BigEndian.PutUint64(pkt[56:64], 0)                // downloaded
	binary.BigEndian.PutUint64(pkt[64:72], uint64(req.Left)) // left
	binary.BigEndian.PutUint64(pkt[72:80], 0)                // uploaded
	binary.BigEndian.PutUint32(pkt[80:84], udpEventStarted)
	binary.BigEndian.PutUint32(pkt[84:88], 0) // IP: let the tracker use the sender's
	binary.BigEndian.PutUint32(pkt[88:92], key)
	binary.BigEndian.PutUint32(pkt[92:96], 0xFFFFFFFF) // num_want: default
	binary.BigEndian.PutUint16(pkt[96:98], req.Port)

	resp, err := udpRoundTrip(conn, pkt, txID, actionAnnounce, 20, timeout)
	if err != nil {
		return nil, err
	}
	interval := binary.BigEndian.Uint32(resp[8:12])
	peers, err := ParseCompactPeers(resp[20 : 20+(len(resp)-20)/6*6])
	if err != nil {
		return nil, err
	}
	return &Response{Interval: time.Duration(interval) * time.Second, Peers: peers}, nil
}

// udpRoundTrip sends pkt and waits for a reply with the same transaction ID,
// resending with a doubled timeout when nothing arrives.
func udpRoundTrip(conn net.Conn, pkt []byte, txID, action uint32, minLen int, timeout time.Duration) ([]byte, error) {
	buf := make([]byte, 2048)
	for attempt := 0; attempt < udpRetries; attempt++ {
		if _, err := conn.Write(pkt); err != nil {
			return nil, err
		}
		deadline := time.Now().Add(timeout << uint(attempt))
		conn.SetReadDeadline(deadline)
		for {
			n, err := conn.Read(buf)
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				break // resend
			}
			if err != nil {
				return nil, err
			}
			resp := buf[:n]
			if n < 8 || binary.BigEndian.Uint32(resp[4:8]) != txID {
				continue // stray or stale packet
			}
			switch got := binary.BigEndian.Uint32(resp[0:4]); {
			case got == actionError:
				return nil, fmt.Errorf("tracker error: %s", bytes.TrimRight(resp[8:], "\x00"))
			case got != action || n < minLen:
				return nil, fmt.Errorf("unexpected reply (action %d, %d bytes)", got, n)
			}
			return append([]byte(nil), resp...), nil
		}
	}
	return nil, fmt.Errorf("no reply after %d attempts", udpRetries)
}

func randomUint32() (uint32, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}
