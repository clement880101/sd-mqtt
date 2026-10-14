#!/usr/bin/env python3
"""Carry the Paho suite's plain-TCP client through TLS or a WebSocket.

The interoperability suite speaks MQTT over a plain socket and nothing
else, so to run it against a TLS or a WebSocket listener something has
to sit between: this. It listens on a plain port, and for every client
that connects it opens a connection to the broker — wrapped in TLS, or
upgraded to a WebSocket with the `mqtt` subprotocol — and carries bytes
both ways until either side closes. Standard library only, so the
runner needs nothing installed beyond Python 3.

    via.py tls <listen-port> <broker-host:port> <ca.pem>
    via.py ws  <listen-port> <broker-host:port>

The WebSocket half is a client per RFC 6455: an upgrade with a random
key, then masked binary frames outward and unmasked frames inward.
Fragmented frames are reassembled, pings are answered with pongs, and a
close frame ends the stream. The broker sends none of the first two,
but a forwarder that assumed so would be the wrong place to find out.
"""

import base64
import hashlib
import os
import selectors
import socket
import ssl
import struct
import sys
import threading

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def pump(source, sink, write, name):
    """Copy from `source` to `sink` through `write` until EOF."""
    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            write(sink, data)
    except OSError:
        pass
    finally:
        for s in (source, sink):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def plain_write(sink, data):
    sink.sendall(data)


# ---- TLS ---------------------------------------------------------------------
#
# One thread per direction is fine on a plain socket and is not on an
# SSL socket: an OpenSSL connection object is not safe to read and
# write from two threads at once, and the first version of this did
# exactly that, which worked in a probe by hand and hung the suite's
# first CONNECT on most runs. So the TLS bridge is one thread and a
# selector, with both sockets non-blocking and decrypted bytes that
# OpenSSL has already buffered (`pending`) drained before waiting on
# the socket again, because a selector cannot see them.


def tls_upstream(target, ca):
    host, port = target.rsplit(":", 1)
    context = ssl.create_default_context(cafile=ca)
    raw = socket.create_connection((host, int(port)))
    return context.wrap_socket(raw, server_hostname="localhost")


def bridge_tls(client, upstream):
    client.setblocking(False)
    upstream.setblocking(False)
    sel = selectors.DefaultSelector()
    sel.register(client, selectors.EVENT_READ)
    sel.register(upstream, selectors.EVENT_READ)
    to_up = bytearray()
    to_down = bytearray()
    alive = True

    def read_from(sock):
        """Everything readable now; b'' at the end; None for nothing yet."""
        got = bytearray()
        while True:
            try:
                data = sock.recv(65536)
            except (ssl.SSLWantReadError, ssl.SSLWantWriteError, BlockingIOError):
                break
            except OSError:
                return b""
            if not data:
                return b""
            got.extend(data)
            if not (isinstance(sock, ssl.SSLSocket) and sock.pending()):
                break
        return bytes(got) if got else None

    while alive:
        sel.modify(client, selectors.EVENT_READ | (selectors.EVENT_WRITE if to_down else 0))
        sel.modify(upstream, selectors.EVENT_READ | (selectors.EVENT_WRITE if to_up else 0))
        for key, events in sel.select(timeout=1.0):
            sock = key.fileobj
            if events & selectors.EVENT_READ:
                data = read_from(sock)
                if data == b"":
                    alive = False
                    break
                if data:
                    (to_up if sock is client else to_down).extend(data)
            if events & selectors.EVENT_WRITE:
                buf = to_up if sock is upstream else to_down
                try:
                    sent = sock.send(bytes(buf[:65536]))
                    del buf[:sent]
                except (ssl.SSLWantReadError, ssl.SSLWantWriteError, BlockingIOError):
                    pass
                except OSError:
                    alive = False
                    break
    # What one side had already said goes to the other before either is
    # closed: a DISCONNECT the broker sends just before closing is the
    # last thing on the stream, and the first version of this dropped it
    # with the close. Blocking sends, briefly; the sockets are ending.
    for sock, buf in ((client, to_down), (upstream, to_up)):
        if buf:
            try:
                sock.setblocking(True)
                sock.settimeout(2.0)
                sock.sendall(bytes(buf))
            except OSError:
                pass
    for sock in (client, upstream):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()


# ---- WebSocket ---------------------------------------------------------------


def ws_upstream(target):
    host, port = target.rsplit(":", 1)
    sock = socket.create_connection((host, int(port)))
    key = base64.b64encode(os.urandom(16)).decode()
    request = (
        f"GET /mqtt HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "Sec-WebSocket-Protocol: mqtt\r\n"
        "\r\n"
    )
    sock.sendall(request.encode())
    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            raise OSError("the upgrade response ended early")
        response += chunk
    head, rest = response.split(b"\r\n\r\n", 1)
    lines = head.decode("latin-1").split("\r\n")
    if not lines[0].startswith("HTTP/1.1 101"):
        raise OSError(f"the upgrade was refused: {lines[0]}")
    headers = {}
    for line in lines[1:]:
        if ":" in line:
            name, value = line.split(":", 1)
            headers[name.strip().lower()] = value.strip()
    expected = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
    if headers.get("sec-websocket-accept") != expected:
        raise OSError("the accept key does not match")
    if headers.get("sec-websocket-protocol", "").lower() != "mqtt":
        raise OSError("the broker did not confirm the mqtt subprotocol")
    return sock, rest


def ws_write(sink, data):
    """One masked binary frame."""
    mask = os.urandom(4)
    length = len(data)
    head = bytes([0x82])
    if length < 126:
        head += bytes([0x80 | length])
    elif length < 65536:
        head += bytes([0x80 | 126]) + struct.pack(">H", length)
    else:
        head += bytes([0x80 | 127]) + struct.pack(">Q", length)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
    sink.sendall(head + mask + masked)


def ws_read_frames(upstream, leftover, downstream):
    """Unwrap frames from the broker and write their bytes to the client."""
    buf = bytearray(leftover)
    fragments = bytearray()

    def need(n):
        while len(buf) < n:
            chunk = upstream.recv(65536)
            if not chunk:
                raise OSError("the broker closed")
            buf.extend(chunk)

    try:
        while True:
            need(2)
            first, second = buf[0], buf[1]
            fin = first & 0x80
            opcode = first & 0x0F
            masked = second & 0x80
            length = second & 0x7F
            offset = 2
            if length == 126:
                need(4)
                length = struct.unpack(">H", bytes(buf[2:4]))[0]
                offset = 4
            elif length == 127:
                need(10)
                length = struct.unpack(">Q", bytes(buf[2:10]))[0]
                offset = 10
            if masked:
                need(offset + 4)
                mask = bytes(buf[offset : offset + 4])
                offset += 4
            need(offset + length)
            payload = bytes(buf[offset : offset + length])
            del buf[: offset + length]
            if masked:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if opcode in (0x2, 0x0):  # binary, or a continuation
                fragments.extend(payload)
                if fin:
                    downstream.sendall(bytes(fragments))
                    fragments.clear()
            elif opcode == 0x9:  # ping
                pong_mask = os.urandom(4)
                pong = bytes([0x8A, 0x80 | len(payload)]) + pong_mask + bytes(
                    b ^ pong_mask[i % 4] for i, b in enumerate(payload)
                )
                upstream.sendall(pong)
            elif opcode == 0x8:  # close
                break
            elif opcode == 0x1:
                raise OSError("a text frame from the broker")
    except OSError:
        pass
    finally:
        for s in (upstream, downstream):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


# ---- the listener ------------------------------------------------------------


def serve(mode, listen_port, target, ca):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", listen_port))
    listener.listen(64)
    while True:
        client, _ = listener.accept()
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        threading.Thread(target=bridge, args=(mode, client, target, ca), daemon=True).start()


def bridge(mode, client, target, ca):
    try:
        if mode == "tls":
            bridge_tls(client, tls_upstream(target, ca))
        else:
            upstream, leftover = ws_upstream(target)
            threading.Thread(target=pump, args=(client, upstream, ws_write, "up"), daemon=True).start()
            ws_read_frames(upstream, leftover, client)
    except OSError as why:
        print(f"via: {why}", file=sys.stderr)
        try:
            client.close()
        except OSError:
            pass


def main():
    if len(sys.argv) < 4 or sys.argv[1] not in ("tls", "ws"):
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    mode, listen_port, target = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    ca = sys.argv[4] if mode == "tls" else None
    if mode == "tls" and ca is None:
        print("via: tls needs the CA file", file=sys.stderr)
        sys.exit(2)
    serve(mode, listen_port, target, ca)


if __name__ == "__main__":
    main()
