// Package seed uploads pieces we have to other peers: it accepts incoming
// connections, answers the handshake, advertises our bitfield and serves
// REQUESTs straight from disk.
package seed

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"mini-bittorrent/peer"
)

const (
	// maxRequestLen is the largest block we serve; bigger requests are a protocol violation.
	maxRequestLen = 128 << 10
	idleTimeout   = 2 * time.Minute
)

// Server serves one torrent's data to any peer that connects.
type Server struct {
	InfoHash    [20]byte
	PeerID      [20]byte
	PieceHashes [][20]byte
	PieceLength int64
	Length      int64
	Data        io.ReaderAt // where piece data is read from
	MaxPeers    int         // connections served at once (default 20)
	Info        []byte      // optional raw info dict; enables ut_metadata so magnet-link peers can get it
	Logger      *log.Logger // optional

	bitfield peer.Bitfield
	uploaded atomic.Int64
	ln       net.Listener
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	closed   bool
}

// Uploaded reports the total bytes sent to peers so far.
func (s *Server) Uploaded() int64 { return s.uploaded.Load() }

// Peers reports how many peers are connected right now.
func (s *Server) Peers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

// Have returns the number of pieces we can serve.
func (s *Server) Have() int {
	n := 0
	for i := range s.PieceHashes {
		if s.bitfield.HasPiece(i) {
			n++
		}
	}
	return n
}

// Verify hashes Data and records which pieces are valid; only those are served.
func (s *Server) Verify() error {
	s.bitfield = make(peer.Bitfield, (len(s.PieceHashes)+7)/8)
	for i, want := range s.PieceHashes {
		begin := int64(i) * s.PieceLength
		end := begin + s.PieceLength
		if end > s.Length {
			end = s.Length
		}
		buf := make([]byte, end-begin)
		if _, err := s.Data.ReadAt(buf, begin); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if sha1.Sum(buf) == want {
			s.bitfield.SetPiece(i)
		}
	}
	return nil
}

// Listen starts accepting peers on addr (e.g. ":6881" or "127.0.0.1:0").
// Verify must have been called first.
func (s *Server) Listen(addr string) error {
	if s.bitfield == nil {
		return errors.New("seed: call Verify before Listen")
	}
	if s.MaxPeers == 0 {
		s.MaxPeers = 20
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.conns = make(map[net.Conn]struct{})
	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

// Addr is the address the server is listening on.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Port is the TCP port the server is listening on.
func (s *Server) Port() uint16 { return uint16(s.ln.Addr().(*net.TCPAddr).Port) }

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed || len(s.conns) >= s.MaxPeers {
			s.mu.Unlock()
			conn.Close()
			continue
		}
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			err := s.serve(conn)
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.logf("peer %s: %v", conn.RemoteAddr(), err)
			}
			conn.Close()
			s.mu.Lock()
			delete(s.conns, conn)
			s.mu.Unlock()
		}()
	}
}

// Close stops accepting peers and disconnects everyone.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	if s.ln != nil {
		s.ln.Close()
	}
	s.wg.Wait()
}

func (s *Server) logf(format string, args ...interface{}) {
	if s.Logger != nil {
		s.Logger.Printf(format, args...)
	}
}

func (s *Server) serve(conn net.Conn) error {
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	hs, err := peer.ReadHandshake(conn)
	if err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	if hs.InfoHash != s.InfoHash {
		return errors.New("handshake for a torrent we don't serve")
	}
	reply := peer.NewHandshake(s.InfoHash, s.PeerID)
	extensions := hs.SupportsExtensions() && s.Info != nil
	if extensions {
		reply.SetExtensions()
	}
	if _, err := conn.Write(reply.Serialize()); err != nil {
		return err
	}
	if _, err := conn.Write((&peer.Message{ID: peer.MsgBitfield, Payload: s.bitfield}).Serialize()); err != nil {
		return err
	}
	if extensions {
		hello, err := peer.FormatExtended(0, map[string]interface{}{
			"m":             map[string]interface{}{"ut_metadata": int64(metadataID)},
			"metadata_size": int64(len(s.Info)),
			"v":             "mini-bittorrent",
		}, nil)
		if err != nil {
			return err
		}
		if _, err := conn.Write(hello.Serialize()); err != nil {
			return err
		}
	}
	s.logf("peer %s: connected", conn.RemoteAddr())

	choked := true
	var remoteMetadataID byte
	for {
		conn.SetDeadline(time.Now().Add(idleTimeout))
		msg, err := peer.ReadMessage(conn)
		if err != nil {
			return err
		}
		if msg == nil {
			continue // keep-alive
		}
		switch msg.ID {
		case peer.MsgInterested:
			if choked {
				if _, err := conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize()); err != nil {
					return err
				}
				choked = false
			}
		case peer.MsgNotInterested:
			if !choked {
				if _, err := conn.Write((&peer.Message{ID: peer.MsgChoke}).Serialize()); err != nil {
					return err
				}
				choked = true
			}
		case peer.MsgRequest:
			if choked {
				continue // requests from choked peers are ignored
			}
			if err := s.serveRequest(conn, msg); err != nil {
				return err
			}
		case peer.MsgExtended:
			if !extensions {
				continue
			}
			extID, dict, _, err := peer.ParseExtended(msg)
			if err != nil {
				return err
			}
			switch extID {
			case 0: // their extension handshake: learn their ut_metadata ID
				m, _ := dict["m"].(map[string]interface{})
				if id, ok := m["ut_metadata"].(int64); ok && id > 0 && id < 256 {
					remoteMetadataID = byte(id)
				}
			case metadataID:
				if remoteMetadataID == 0 {
					continue
				}
				if err := s.serveMetadata(conn, remoteMetadataID, dict); err != nil {
					return err
				}
			}
		}
	}
}

// metadataID is the extended-message ID we ask peers to use for ut_metadata.
const metadataID = 2

// serveMetadata answers a ut_metadata request (BEP 9) with one 16 KiB piece
// of the info dictionary, or a reject if the piece doesn't exist.
func (s *Server) serveMetadata(conn net.Conn, remoteID byte, req map[string]interface{}) error {
	const block = 16384
	msgType, _ := req["msg_type"].(int64)
	if msgType != 0 {
		return nil
	}
	piece, _ := req["piece"].(int64)
	numPieces := int64((len(s.Info) + block - 1) / block)

	var reply *peer.Message
	var err error
	if piece < 0 || piece >= numPieces {
		reply, err = peer.FormatExtended(remoteID, map[string]interface{}{"msg_type": int64(2), "piece": piece}, nil)
	} else {
		start := piece * block
		end := start + block
		if end > int64(len(s.Info)) {
			end = int64(len(s.Info))
		}
		reply, err = peer.FormatExtended(remoteID, map[string]interface{}{
			"msg_type": int64(1), "piece": piece, "total_size": int64(len(s.Info)),
		}, s.Info[start:end])
	}
	if err != nil {
		return err
	}
	_, err = conn.Write(reply.Serialize())
	return err
}

func (s *Server) serveRequest(conn net.Conn, msg *peer.Message) error {
	if len(msg.Payload) != 12 {
		return errors.New("malformed REQUEST")
	}
	index := int(binary.BigEndian.Uint32(msg.Payload[0:4]))
	begin := int64(binary.BigEndian.Uint32(msg.Payload[4:8]))
	length := int64(binary.BigEndian.Uint32(msg.Payload[8:12]))

	if !s.bitfield.HasPiece(index) {
		return fmt.Errorf("requested piece %d we don't have", index)
	}
	pieceStart := int64(index) * s.PieceLength
	pieceLen := s.PieceLength
	if pieceStart+pieceLen > s.Length {
		pieceLen = s.Length - pieceStart
	}
	if length <= 0 || length > maxRequestLen || begin+length > pieceLen {
		return fmt.Errorf("invalid request: piece %d [%d, +%d)", index, begin, length)
	}

	payload := make([]byte, 8+length)
	copy(payload[0:8], msg.Payload[0:8])
	if _, err := s.Data.ReadAt(payload[8:], pieceStart+begin); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if _, err := conn.Write((&peer.Message{ID: peer.MsgPiece, Payload: payload}).Serialize()); err != nil {
		return err
	}
	s.uploaded.Add(length)
	return nil
}
