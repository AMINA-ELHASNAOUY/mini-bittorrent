// Package magnet parses magnet links (BEP 9): a torrent identified only by its
// info hash, optionally with a display name, trackers and peer addresses.
package magnet

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
)

// Link is a parsed magnet URI.
type Link struct {
	InfoHash [20]byte
	Name     string   // dn: suggested display name (may be empty)
	Trackers []string // tr: tracker URLs
	Peers    []string // x.pe: "host:port" of peers to try directly
}

// Parse reads a "magnet:?xt=urn:btih:..." URI. The info hash may be 40 hex
// characters or 32 base32 characters.
func Parse(uri string) (*Link, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "magnet" {
		return nil, fmt.Errorf("not a magnet link (scheme %q)", u.Scheme)
	}
	q := u.Query()

	link := &Link{Name: q.Get("dn"), Trackers: q["tr"], Peers: q["x.pe"]}
	found := false
	for _, xt := range q["xt"] {
		const prefix = "urn:btih:"
		if !strings.HasPrefix(strings.ToLower(xt), prefix) {
			continue
		}
		h := xt[len(prefix):]
		var raw []byte
		switch len(h) {
		case 40:
			raw, err = hex.DecodeString(h)
		case 32:
			raw, err = base32.StdEncoding.DecodeString(strings.ToUpper(h))
		default:
			err = fmt.Errorf("info hash %q has %d characters, want 40 (hex) or 32 (base32)", h, len(h))
		}
		if err != nil {
			return nil, fmt.Errorf("invalid info hash: %w", err)
		}
		copy(link.InfoHash[:], raw)
		found = true
		break
	}
	if !found {
		return nil, fmt.Errorf("magnet link has no BitTorrent info hash (xt=urn:btih:...)")
	}
	return link, nil
}

// String rebuilds the magnet URI.
func (l *Link) String() string {
	v := url.Values{}
	v.Set("xt", "urn:btih:"+hex.EncodeToString(l.InfoHash[:]))
	if l.Name != "" {
		v.Set("dn", l.Name)
	}
	for _, tr := range l.Trackers {
		v.Add("tr", tr)
	}
	for _, p := range l.Peers {
		v.Add("x.pe", p)
	}
	return "magnet:?" + v.Encode()
}
