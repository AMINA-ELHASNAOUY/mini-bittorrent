package tracker

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestParseCompactPeers(t *testing.T) {
	peers, err := ParseCompactPeers([]byte{127, 0, 0, 1, 0x1A, 0xE1, 10, 0, 0, 2, 0x00, 0x50})
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 2 || peers[0].String() != "127.0.0.1:6881" || peers[1].String() != "10.0.0.2:80" {
		t.Fatalf("got %v", peers)
	}
	if _, err := ParseCompactPeers([]byte{1, 2, 3}); err == nil {
		t.Error("expected error for truncated list")
	}
}

func TestBuildURL(t *testing.T) {
	req := Request{Announce: "http://tracker.test/announce?key=abc", Port: 6881, Left: 1000}
	req.InfoHash[0] = 0xff
	u, err := BuildURL(req)
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.Parse(u)
	q := parsed.Query()
	if q.Get("key") != "abc" || q.Get("port") != "6881" || q.Get("left") != "1000" || q.Get("compact") != "1" {
		t.Errorf("unexpected query: %s", parsed.RawQuery)
	}
	if q.Get("info_hash")[0] != 0xff {
		t.Error("info_hash not encoded as raw bytes")
	}
	if _, err := BuildURL(Request{Announce: "udp://tracker.test:80"}); err == nil {
		t.Error("expected error for udp tracker")
	}
}

func TestAnnounceCompact(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.URL.Query().Get("info_hash")) != 20 {
			t.Errorf("tracker got bad info_hash")
		}
		w.Write([]byte("d8:intervali900e5:peers6:\x7f\x00\x00\x01\x1a\xe1e"))
	}))
	defer srv.Close()

	res, err := Announce(srv.Client(), Request{Announce: srv.URL, Port: 6881})
	if err != nil {
		t.Fatal(err)
	}
	if res.Interval.Seconds() != 900 || len(res.Peers) != 1 || res.Peers[0].String() != "127.0.0.1:6881" {
		t.Fatalf("got %+v", res)
	}
}

func TestAnnounceDictPeersAndFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("left") == "0" {
			w.Write([]byte("d14:failure reason9:not founde"))
			return
		}
		w.Write([]byte("d8:intervali60e5:peersld2:ip9:127.0.0.14:porti7000eeee"))
	}))
	defer srv.Close()

	res, err := Announce(srv.Client(), Request{Announce: srv.URL, Left: 5})
	if err != nil || len(res.Peers) != 1 || res.Peers[0].Port != 7000 {
		t.Fatalf("got %+v, %v", res, err)
	}
	if _, err := Announce(srv.Client(), Request{Announce: srv.URL, Left: 0}); err == nil {
		t.Fatal("expected tracker failure error")
	}
}

func TestAnnounceIPv6Peers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v6 := "\x20\x01\x0d\xb8" + strings.Repeat("\x00", 11) + "\x01" + "\x1a\xe1"
		w.Write([]byte("d8:intervali60e5:peers0:6:peers618:" + v6 + "e"))
	}))
	defer srv.Close()
	res, err := Announce(srv.Client(), Request{Announce: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Peers) != 1 || res.Peers[0].String() != "[2001:db8::1]:6881" {
		t.Fatalf("got %v", res.Peers)
	}
}
