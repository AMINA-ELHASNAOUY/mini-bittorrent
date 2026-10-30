#!/usr/bin/env python3
"""Interoperability check against libtorrent (the engine behind qBittorrent
and Deluge), in two setups:

  uTP only: libtorrent runs with TCP turned off, so every byte has to go
  through our uTP implementation.
  Encryption required: libtorrent refuses any connection that isn't
  RC4-encrypted with MSE/PE.

In each setup:
  1. libtorrent seeds, mini-bittorrent downloads by magnet link
     (metadata + pieces)
  2. mini-bittorrent seeds, libtorrent downloads

Runs on 127.0.0.1, no internet needed.

  pip install libtorrent
  go build -o mini-bittorrent .
  python3 scripts/interop_libtorrent.py ./mini-bittorrent
"""
import os
import subprocess
import sys
import tempfile
import time
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)
import libtorrent as lt  # noqa: E402

BIN = os.path.abspath(sys.argv[1] if len(sys.argv) > 1 else "./mini-bittorrent")
COMMON = {
    "enable_dht": False, "enable_lsd": False, "enable_upnp": False, "enable_natpmp": False,
    "allow_multiple_connections_per_ip": True,
}
UTP_ONLY = dict(COMMON, **{
    "enable_outgoing_tcp": False, "enable_incoming_tcp": False,
    "enable_outgoing_utp": True, "enable_incoming_utp": True,
    "in_enc_policy": 1, "out_enc_policy": 1, "allowed_enc_level": 3,  # encryption allowed, not required
})
ENCRYPTED_ONLY = dict(COMMON, **{
    "in_enc_policy": 0, "out_enc_policy": 0,  # pe_forced: plaintext connections are refused
    "allowed_enc_level": 2,                   # pe_rc4: the whole stream must be RC4
})


def make_torrent(path, size):
    with open(path, "wb") as f:
        f.write(os.urandom(size))
    fs = lt.file_storage()
    lt.add_files(fs, path)
    ct = lt.create_torrent(fs, 256 * 1024, flags=lt.create_torrent.v1_only)
    lt.set_piece_hashes(ct, os.path.dirname(path))
    return lt.bencode(ct.generate())


def same(a, b):
    with open(a, "rb") as x, open(b, "rb") as y:
        return x.read() == y.read()


def libtorrent_seeds(work, settings, port):
    src = os.path.join(work, "lt-seed")
    os.makedirs(src)
    raw = make_torrent(os.path.join(src, "a.bin"), 5_000_000)
    ti = lt.torrent_info(lt.bdecode(raw))
    s = lt.session(dict(settings, listen_interfaces="127.0.0.1:%d" % port))
    h = s.add_torrent({"ti": ti, "save_path": src})
    while not h.status().is_seeding:
        time.sleep(0.1)
    magnet = "magnet:?xt=urn:btih:%s&x.pe=127.0.0.1:%d" % (ti.info_hashes().v1.to_bytes().hex(), port)
    dst = os.path.join(work, "ours")
    os.makedirs(dst)
    subprocess.run([BIN, "download", "-nodht", "-port", str(port + 1), magnet], cwd=dst, check=True,
                   timeout=60, stdout=subprocess.DEVNULL)
    ok = same(os.path.join(src, "a.bin"), os.path.join(dst, "a.bin"))
    time.sleep(1.5)  # libtorrent refreshes its counters about once a second
    print("  1. libtorrent seeds -> mini-bittorrent downloads:", "OK" if ok else "FAILED",
          "(libtorrent uploaded %d bytes)" % h.status().total_payload_upload)
    return ok


def libtorrent_downloads(work, settings, port):
    src = os.path.join(work, "our-seed")
    os.makedirs(src)
    raw = make_torrent(os.path.join(src, "b.bin"), 7_000_000)
    tpath = os.path.join(work, "b.torrent")
    with open(tpath, "wb") as f:
        f.write(raw)
    seeder = subprocess.Popen([BIN, "seed", "-port", str(port), tpath, os.path.join(src, "b.bin")],
                              stdout=subprocess.DEVNULL)
    try:
        time.sleep(1.5)
        dst = os.path.join(work, "lt-dl")
        os.makedirs(dst)
        s = lt.session(dict(settings, listen_interfaces="127.0.0.1:%d" % (port + 1)))
        h = s.add_torrent({"ti": lt.torrent_info(lt.bdecode(raw)), "save_path": dst})
        h.connect_peer(("127.0.0.1", port))
        deadline = time.time() + 60
        while not h.status().is_seeding and time.time() < deadline:
            time.sleep(0.1)
        ok = h.status().is_seeding and same(os.path.join(src, "b.bin"), os.path.join(dst, "b.bin"))
        print("  2. mini-bittorrent seeds -> libtorrent downloads:", "OK" if ok else "FAILED",
              "(libtorrent downloaded %d bytes)" % h.status().total_payload_download)
        return ok
    finally:
        seeder.terminate()


if __name__ == "__main__":
    print("libtorrent", lt.__version__)
    ok = True
    for name, settings, port in [("uTP only (TCP disabled)", UTP_ONLY, 7311),
                                 ("encryption required (MSE/PE, RC4)", ENCRYPTED_ONLY, 7331)]:
        print(name)
        with tempfile.TemporaryDirectory() as work:
            ok &= libtorrent_seeds(work, settings, port)
        with tempfile.TemporaryDirectory() as work:
            ok &= libtorrent_downloads(work, settings, port + 10)
    sys.exit(0 if ok else 1)
