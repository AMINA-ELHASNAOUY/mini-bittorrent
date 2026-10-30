package main

import (
	"io"
	"net"
	"strconv"
	"testing"

	"mini-bittorrent/utp"
)

// Dial races TCP and uTP, so a peer that speaks only one of them is
// reached either way.
func TestDialReachesTCPOnlyAndUTPOnlyPeers(t *testing.T) {
	tr, err := listenTransport(0, true)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	tcpOnly, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer tcpOnly.Close()
	utpOnly, err := utp.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer utpOnly.Close()

	for name, ln := range map[string]net.Listener{"tcp": tcpOnly, "utp": utpOnly} {
		go func(ln net.Listener) {
			c, err := ln.Accept()
			if err == nil {
				c.Write([]byte("hi"))
				io.Copy(io.Discard, c)
			}
		}(ln)
		c, err := tr.Dial(ln.Addr().String())
		if err != nil {
			t.Fatalf("%s-only peer: %v", name, err)
		}
		buf := make([]byte, 2)
		if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "hi" {
			t.Fatalf("%s-only peer: read %q, %v", name, buf, err)
		}
		c.Close()
	}
}

func TestMergedListenerAcceptsBoth(t *testing.T) {
	tr, err := listenTransport(0, true)
	if err != nil {
		t.Fatal(err)
	}
	ln := tr.Listener()
	defer ln.Close()
	addr := "127.0.0.1:" + itoa(tr.port)

	client, err := utp.NewSocket("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	go func() {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Write([]byte("t"))
		}
		if c, err := client.Dial(addr, dialTimeout); err == nil {
			c.Write([]byte("u"))
		}
	}()
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		c, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		seen[c.LocalAddr().Network()] = true
		c.Close()
	}
	if !seen["tcp"] || !seen["udp"] {
		t.Fatalf("accepted %v, want one TCP and one uTP (UDP) connection", seen)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
