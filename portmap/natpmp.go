package portmap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"
)

// NAT-PMP (RFC 6886): a tiny UDP protocol spoken to the gateway on port 5351.
// Apple routers and many others support it (or its successor PCP, which
// still answers NAT-PMP requests).

const natpmpPort = 5351

// natpmp talks to one gateway.
type natpmp struct {
	gateway string // "ip:port"
	tries   int    // retransmissions; RFC 6886 starts at 250 ms and doubles
}

var natpmpResults = map[uint16]string{
	1: "unsupported version", 2: "not authorized/refused", 3: "network failure",
	4: "out of resources", 5: "unsupported opcode",
}

// call sends req and waits for a response to opcode op, resending with a
// doubling timeout since UDP packets can be lost.
func (n *natpmp) call(req []byte, op byte, minLen int) ([]byte, error) {
	conn, err := net.Dial("udp", n.gateway)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	wait := 250 * time.Millisecond
	buf := make([]byte, 64)
	for i := 0; i < n.tries; i++ {
		if _, err := conn.Write(req); err != nil {
			return nil, err
		}
		conn.SetReadDeadline(time.Now().Add(wait))
		for {
			k, err := conn.Read(buf)
			if err != nil {
				break // timeout (or ICMP refused): resend
			}
			if k < minLen || buf[0] != 0 || buf[1] != 128+op {
				continue
			}
			if code := binary.BigEndian.Uint16(buf[2:4]); code != 0 {
				msg := natpmpResults[code]
				if msg == "" {
					msg = fmt.Sprintf("result code %d", code)
				}
				return nil, fmt.Errorf("NAT-PMP: %s", msg)
			}
			return buf[:k], nil
		}
		wait *= 2
	}
	return nil, errors.New("NAT-PMP: no answer from the gateway")
}

// externalIP asks the gateway for its public address.
func (n *natpmp) externalIP() (net.IP, error) {
	resp, err := n.call([]byte{0, 0}, 0, 12)
	if err != nil {
		return nil, err
	}
	return net.IP(append([]byte(nil), resp[8:12]...)), nil
}

// addMapping maps external port -> our internal port for proto ("tcp" or
// "udp") for lifetime. The gateway may pick a different external port; the
// one it chose is returned. A lifetime of 0 deletes the mapping.
func (n *natpmp) addMapping(proto string, internal, external int, lifetime time.Duration) (int, error) {
	op := byte(1) // UDP
	if proto == "tcp" {
		op = 2
	}
	req := make([]byte, 12)
	req[1] = op
	binary.BigEndian.PutUint16(req[4:], uint16(internal))
	binary.BigEndian.PutUint16(req[6:], uint16(external))
	secs := uint32(lifetime / time.Second)
	if lifetime > 0 && secs == 0 {
		secs = 1 // 0 would mean "delete"
	}
	binary.BigEndian.PutUint32(req[8:], secs)
	resp, err := n.call(req, op, 16)
	if err != nil {
		return 0, err
	}
	if got := int(binary.BigEndian.Uint16(resp[8:10])); got != internal {
		return 0, fmt.Errorf("NAT-PMP: answer is for port %d, not %d", got, internal)
	}
	return int(binary.BigEndian.Uint16(resp[10:12])), nil
}
