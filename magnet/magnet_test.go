package magnet

import (
	"encoding/hex"
	"testing"
)

func TestParseHex(t *testing.T) {
	l, err := Parse("magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a&dn=Some+File.iso" +
		"&tr=udp%3A%2F%2Ftracker.test%3A1337%2Fannounce&tr=http%3A%2F%2Ft2.test%2Fannounce&x.pe=10.0.0.1:6881")
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(l.InfoHash[:]) != "c12fe1c06bba254a9dc9f519b335aa7c1367a88a" {
		t.Errorf("info hash %x", l.InfoHash)
	}
	if l.Name != "Some File.iso" || len(l.Trackers) != 2 || l.Trackers[0] != "udp://tracker.test:1337/announce" || l.Peers[0] != "10.0.0.1:6881" {
		t.Errorf("got %+v", l)
	}
}

func TestParseBase32(t *testing.T) {
	l, err := Parse("magnet:?xt=urn:btih:YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK")
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(l.InfoHash[:]) != "c12fe1c06bba254a9dc9f519b335aa7c1367a88a" {
		t.Errorf("info hash %x", l.InfoHash)
	}
}

func TestParseErrors(t *testing.T) {
	for _, uri := range []string{
		"http://example.com",
		"magnet:?dn=no-hash",
		"magnet:?xt=urn:btih:1234",
		"magnet:?xt=urn:btih:zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
	} {
		if _, err := Parse(uri); err == nil {
			t.Errorf("Parse(%q): expected an error", uri)
		}
	}
}

func TestStringRoundTrip(t *testing.T) {
	in := &Link{Name: "x y", Trackers: []string{"udp://a.test:80"}}
	in.InfoHash[0] = 0xAB
	out, err := Parse(in.String())
	if err != nil || out.InfoHash != in.InfoHash || out.Name != "x y" || out.Trackers[0] != "udp://a.test:80" {
		t.Fatalf("round trip: %+v, %v", out, err)
	}
}
