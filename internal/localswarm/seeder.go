// Package localswarm runs a tiny BitTorrent swarm on localhost: an HTTP
// tracker plus seeders that serve a file over the peer wire protocol.
// It powers the end-to-end tests and the `demo` command, so the whole
// download path can be exercised without the internet.
package localswarm

import (
	"encoding/binary"
	"net"
	"sync"
	"time"

	"mini-bittorrent/peer"
)

// Seeder serves pieces of one file to any peer that connects.
type Seeder struct {
	Data        []byte
	PieceLength int
	InfoHash    [20]byte
	PeerID      [20]byte
	Has         func(index int) bool // nil = has every piece
	Corrupt     bool                 // flip a byte in every block it sends
	Latency     time.Duration        // simulated network delay per block

	ln     net.Listener
	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

// Start begins listening on a random localhost port.
func (s *Seeder) Start() error {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	s.ln = ln
	s.conns = make(map[net.Conn]struct{})
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				conn.Close()
				return
			}
			s.conns[conn] = struct{}{}
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(conn)
				s.mu.Lock()
				delete(s.conns, conn)
				s.mu.Unlock()
			}()
		}
	}()
	return nil
}

// Addr is the "ip:port" the seeder listens on.
func (s *Seeder) Addr() string { return s.ln.Addr().String() }

// Close stops the seeder and drops all connections.
func (s *Seeder) Close() {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.ln.Close()
	s.wg.Wait()
}

func (s *Seeder) numPieces() int {
	return (len(s.Data) + s.PieceLength - 1) / s.PieceLength
}

func (s *Seeder) serve(conn net.Conn) {
	defer conn.Close()

	hs, err := peer.ReadHandshake(conn)
	if err != nil || hs.InfoHash != s.InfoHash {
		return
	}
	if _, err := conn.Write(peer.NewHandshake(s.InfoHash, s.PeerID).Serialize()); err != nil {
		return
	}

	bf := make(peer.Bitfield, (s.numPieces()+7)/8)
	for i := 0; i < s.numPieces(); i++ {
		if s.Has == nil || s.Has(i) {
			bf.SetPiece(i)
		}
	}
	if _, err := conn.Write((&peer.Message{ID: peer.MsgBitfield, Payload: bf}).Serialize()); err != nil {
		return
	}

	for {
		msg, err := peer.ReadMessage(conn)
		if err != nil {
			return
		}
		if msg == nil {
			continue
		}
		switch msg.ID {
		case peer.MsgInterested:
			if _, err := conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize()); err != nil {
				return
			}
		case peer.MsgRequest:
			if len(msg.Payload) != 12 {
				return
			}
			index := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
			begin := int(binary.BigEndian.Uint32(msg.Payload[4:8]))
			length := int(binary.BigEndian.Uint32(msg.Payload[8:12]))
			start := index*s.PieceLength + begin
			if !bf.HasPiece(index) || length > 1<<17 || start+length > len(s.Data) {
				return
			}
			payload := make([]byte, 8+length)
			copy(payload[0:8], msg.Payload[0:8])
			copy(payload[8:], s.Data[start:start+length])
			if s.Latency > 0 {
				time.Sleep(s.Latency)
			}
			if s.Corrupt {
				payload[8] ^= 0xFF
			}
			if _, err := conn.Write((&peer.Message{ID: peer.MsgPiece, Payload: payload}).Serialize()); err != nil {
				return
			}
		}
	}
}
