# ⚡ Mini BitTorrent Client

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.26-00ADD8?style=for-the-badge&logo=go&logoColor=white" />
  <img src="https://img.shields.io/badge/status-in--progress-F4A100?style=for-the-badge" />
  <img src="https://img.shields.io/badge/license-MIT-1565C0?style=for-the-badge" />
</p>

<p align="center">
  <i>A BitTorrent client built from the protocol spec up — no libtorrent, no shortcuts.</i>
</p>

---

## Why this exists

Most portfolios have five projects: a todo app, a weather app, a clone of something famous. This isn't one of those. It's peer-to-peer file sharing implemented from the raw protocol — no central server, no external torrent library doing the work for you.

If it works, it proves you understand:
- Binary protocol parsing (bencode)
- Distributed peer discovery
- TCP socket programming at the byte level
- Concurrent, fault-tolerant networking

## Architecture

┌─────────────┐ ┌──────────────┐ ┌─────────────┐
│ .torrent │────▶│ Tracker │────▶│ Peer List │
│ file │ │ Client │ │ │
└─────────────┘ └──────────────┘ └──────┬──────┘
│
┌─────────────────────────────┘
▼
┌──────────────────┐ ┌─────────────────┐
│ Peer Wire │────▶│ Piece Manager │
│ Protocol │ │ (verify + write)│
└──────────────────┘ └─────────────────┘

## Roadmap

| Milestone | Status | What it proves |
|---|---|---|
| Bencode encoder/decoder | 🔲 | Binary protocol parsing |
| `.torrent` file parser | 🔲 | Metadata extraction, SHA-1 hashing |
| Tracker HTTP client | 🔲 | Announce protocol, peer discovery |
| Peer wire handshake | 🔲 | Raw TCP protocol implementation |
| Piece download + verify | 🔲 | Chunked transfer, integrity checks |
| Multi-peer concurrency | 🔲 | Goroutines, channels, fan-out downloads |

## Tech stack

Go (standard library first — `net`, `crypto/sha1`, `encoding` — external deps only where it actually earns its place).

## Getting started

```bash
git clone https://github.com/AMINA-ELHASNAOUY/mini-bittorrent.git
cd mini-bittorrent
go run main.go
```

---

<p align="center"><sub>Built by <a href="https://github.com/AMINA-ELHASNAOUY">Amina El Hasnaouy</a></sub></p>