package main

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"mini-bittorrent/utp"
)

const dialTimeout = 3 * time.Second

// transport is how we reach peers and how they reach us: TCP plus, unless
// disabled, uTP on the same port number (UDP), as other clients expect.
type transport struct {
	tcp  net.Listener // nil when not accepting
	utp  *utp.Socket  // nil when uTP is off
	port int
}

// listenTransport opens TCP and uTP on port, falling back to a random port
// when it is taken. With useUTP false only TCP is used.
func listenTransport(port int, useUTP bool) (*transport, error) {
	for _, p := range []int{port, 0} {
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", p))
		if err != nil {
			continue
		}
		t := &transport{tcp: ln, port: ln.Addr().(*net.TCPAddr).Port}
		if useUTP {
			if t.utp, err = utp.Listen(fmt.Sprintf(":%d", t.port)); err != nil {
				ln.Close()
				continue // the UDP port is busy: try another pair
			}
		}
		return t, nil
	}
	return nil, fmt.Errorf("no free port for incoming peers (tried %d and a random one)", port)
}

// Listener merges incoming TCP and uTP connections. Closing it closes both.
func (t *transport) Listener() net.Listener {
	m := &mergedListener{tr: t, conns: make(chan net.Conn), done: make(chan struct{})}
	for _, ln := range t.listeners() {
		go m.feed(ln)
	}
	return m
}

func (t *transport) listeners() []net.Listener {
	ls := []net.Listener{t.tcp}
	if t.utp != nil {
		ls = append(ls, t.utp)
	}
	return ls
}

// Dial connects to a peer. With uTP on, TCP and uTP are tried at the same
// time and the first to connect wins ("happy eyeballs"): a peer that only
// speaks one of them is reached without waiting for the other to time out.
func (t *transport) Dial(addr string) (net.Conn, error) {
	if t.utp == nil {
		return net.DialTimeout("tcp", addr, dialTimeout)
	}
	type result struct {
		c   net.Conn
		err error
	}
	results := make(chan result, 2)
	go func() {
		c, err := net.DialTimeout("tcp", addr, dialTimeout)
		results <- result{c, err}
	}()
	go func() {
		c, err := t.utp.Dial(addr, dialTimeout)
		results <- result{c, err}
	}()
	first := <-results
	if first.err == nil {
		go func() { // close the loser, whenever it finishes
			if r := <-results; r.err == nil {
				r.c.Close()
			}
		}()
		return first.c, nil
	}
	second := <-results
	if second.err == nil {
		return second.c, nil
	}
	return nil, errors.Join(first.err, second.err)
}

func (t *transport) Close() {
	for _, ln := range t.listeners() {
		ln.Close()
	}
}

type mergedListener struct {
	tr    *transport
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func (m *mergedListener) feed(ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		select {
		case m.conns <- c:
		case <-m.done:
			c.Close()
			return
		}
	}
}

func (m *mergedListener) Accept() (net.Conn, error) {
	select {
	case c := <-m.conns:
		return c, nil
	case <-m.done:
		return nil, net.ErrClosed
	}
}

func (m *mergedListener) Close() error {
	m.once.Do(func() {
		close(m.done)
		m.tr.Close()
	})
	return nil
}

func (m *mergedListener) Addr() net.Addr { return m.tr.tcp.Addr() }
