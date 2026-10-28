package tracker

import (
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeUDPTracker answers BEP 15 connect/announce requests on localhost.
// dropFirst makes it ignore the first packet to exercise the retry path;
// fail makes it reply to announces with an error action.
func fakeUDPTracker(t *testing.T, dropFirst, fail bool) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	const connID = 0x1122334455667788

	go func() {
		buf := make([]byte, 2048)
		dropped := false
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if dropFirst && !dropped {
				dropped = true
				continue
			}
			pkt := buf[:n]
			action := binary.BigEndian.Uint32(pkt[8:12])
			txID := pkt[12:16]
			switch {
			case action == actionConnect && n == 16 && binary.BigEndian.Uint64(pkt[0:8]) == udpProtocolID:
				// A stray packet with the wrong transaction ID must be ignored by the client.
				pc.WriteTo([]byte{0, 0, 0, 0, 9, 9, 9, 9, 0, 0, 0, 0, 0, 0, 0, 0}, addr)
				resp := make([]byte, 16)
				binary.BigEndian.PutUint32(resp[0:4], actionConnect)
				copy(resp[4:8], txID)
				binary.BigEndian.PutUint64(resp[8:16], connID)
				pc.WriteTo(resp, addr)
			case action == actionAnnounce && n == 98 && binary.BigEndian.Uint64(pkt[0:8]) == connID:
				if fail {
					resp := append([]byte{0, 0, 0, 3}, txID...)
					pc.WriteTo(append(resp, "torrent not registered"...), addr)
					continue
				}
				resp := make([]byte, 20, 32)
				binary.BigEndian.PutUint32(resp[0:4], actionAnnounce)
				copy(resp[4:8], txID)
				binary.BigEndian.PutUint32(resp[8:12], 1800)
				resp = append(resp, 127, 0, 0, 1, 0x1A, 0xE1, 10, 0, 0, 7, 0x1F, 0x90)
				pc.WriteTo(resp, addr)
			}
		}
	}()
	return "udp://" + pc.LocalAddr().String() + "/announce"
}

func TestUDPAnnounce(t *testing.T) {
	url := fakeUDPTracker(t, false, false)
	res, err := Announce(nil, Request{Announce: url, Port: 6881, Left: 100})
	if err != nil {
		t.Fatal(err)
	}
	if res.Interval != 30*time.Minute || len(res.Peers) != 2 || res.Peers[1].String() != "10.0.0.7:8080" {
		t.Fatalf("got %+v", res)
	}
}

func TestUDPAnnounceRetriesLostPacket(t *testing.T) {
	url := fakeUDPTracker(t, true, false)
	res, err := announceUDP(Request{Announce: url}, 50*time.Millisecond)
	if err != nil || len(res.Peers) != 2 {
		t.Fatalf("got %+v, %v", res, err)
	}
}

func TestUDPAnnounceTrackerError(t *testing.T) {
	url := fakeUDPTracker(t, false, true)
	_, err := Announce(nil, Request{Announce: url})
	if err == nil || !strings.Contains(err.Error(), "torrent not registered") {
		t.Fatalf("expected tracker error, got %v", err)
	}
}

func TestUDPAnnounceNoReply(t *testing.T) {
	pc, _ := net.ListenPacket("udp", "127.0.0.1:0") // listens, never answers
	defer pc.Close()
	_, err := announceUDP(Request{Announce: "udp://" + pc.LocalAddr().String()}, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected an error when the tracker never answers")
	}
}

func TestAnnounceAllMergesAndSkipsDeadTrackers(t *testing.T) {
	udpURL := fakeUDPTracker(t, false, false)
	pc, _ := net.ListenPacket("udp", "127.0.0.1:0") // dead tracker
	defer pc.Close()

	start := time.Now()
	peers, errs := AnnounceAll([]string{udpURL, udpURL + "?dup", "udp://" + pc.LocalAddr().String(), "ftp://nope"},
		Request{Port: 6881}, 200*time.Millisecond)
	if len(peers) != 2 {
		t.Fatalf("want 2 unique peers, got %v", peers)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("waited %s on a dead tracker", time.Since(start))
	}
	if len(errs) == 0 {
		t.Error("expected the ftp:// tracker error to be reported")
	}
}
