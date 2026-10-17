#!/usr/bin/env python3
"""A very small MQTT 3.1.1 client, for driving a broker by hand.

This exists because milestone 3's demonstration gave numbers that
nobody could reproduce without a client, and this repository has
none: the conformance suite in `services/broker/conformance` is a
conformance suite, not a tool, and it cannot be pointed at a port or
asked to hold four hundred connections open.

**It is a development tool and nothing else.** It ships with nothing,
no test depends on it, and it implements the smallest part of MQTT
3.1.1 that the demonstration needs: connect, publish, subscribe and
ping. It does not decode what the broker sends beyond the length of
an acknowledgement, it speaks no MQTT 5, and it is not a check of
anything — a broker that answered nonsense would satisfy it. Use the
conformance suite to find out whether the broker is right and this to
find out what the interface does when a broker is busy.

Usage, with the broker on its default port:

    scripts/mqtt-load.py retain sensors/kitchen/temp=21.5 status/x=on
    scripts/mqtt-load.py flood 400 demo        # 400 clients, held open
    scripts/mqtt-load.py traffic 20 300 350    # 20s, 300/s, 350 bytes
    scripts/mqtt-load.py subscribe 'sensors/#' 60

`flood` and `subscribe` hold their connections until interrupted.
Several hundred sockets in one process needs the file descriptor
limit raised: `ulimit -n 4096`.
"""

import socket
import struct
import sys
import time

HOST = "127.0.0.1"
PORT = 1883


def remaining_length(value: int) -> bytes:
    """MQTT's variable byte integer, for a length this tool can produce."""
    out = b""
    while True:
        byte = value % 128
        value //= 128
        if value:
            byte |= 0x80
        out += bytes([byte])
        if not value:
            return out


def string(text: str) -> bytes:
    """A UTF-8 string with its two-byte length, which is how MQTT writes one."""
    raw = text.encode()
    return struct.pack("!H", len(raw)) + raw


def connect(client_id: str, keep_alive: int = 60, clean: bool = True) -> socket.socket:
    """Open a connection and wait for the acknowledgement."""
    sock = socket.create_connection((HOST, PORT))
    sock.settimeout(10)
    flags = 0x02 if clean else 0x00
    body = string("MQTT") + bytes([4, flags]) + struct.pack("!H", keep_alive) + string(client_id)
    sock.sendall(b"\x10" + remaining_length(len(body)) + body)
    ack = sock.recv(4)
    if len(ack) < 4 or ack[0] != 0x20 or ack[3] != 0:
        raise RuntimeError(f"the broker refused {client_id}: {ack!r}")
    return sock


def publish(
    sock: socket.socket,
    topic: str,
    payload: str,
    retain: bool = False,
    qos: int = 0,
    packet_id: int = 1,
) -> None:
    """Publish, waiting for the acknowledgement at level one."""
    header = 0x30 | (1 if retain else 0) | (qos << 1)
    body = string(topic)
    if qos:
        body += struct.pack("!H", packet_id)
    body += payload.encode()
    sock.sendall(bytes([header]) + remaining_length(len(body)) + body)
    if qos == 1:
        sock.recv(4)


def subscribe(sock: socket.socket, topic_filter: str, qos: int = 0, packet_id: int = 1) -> None:
    """Subscribe to one filter."""
    body = struct.pack("!H", packet_id) + string(topic_filter) + bytes([qos])
    sock.sendall(b"\x82" + remaining_length(len(body)) + body)
    sock.recv(5)


def ping(sock: socket.socket) -> None:
    """Keep a connection alive without reading what comes back."""
    sock.sendall(b"\xc0\x00")
    try:
        sock.recv(2)
    except OSError:
        pass


def hold(socks: list[socket.socket]) -> None:
    """Keep connections open until interrupted."""
    print(f"connected {len(socks)}", flush=True)
    try:
        while True:
            time.sleep(20)
            for sock in socks:
                ping(sock)
    except KeyboardInterrupt:
        pass


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    command = sys.argv[1]

    if command == "retain":
        sock = connect("load-retainer")
        for pair in sys.argv[2:]:
            topic, _, payload = pair.partition("=")
            publish(sock, topic, payload, retain=True)
        time.sleep(0.3)
        sock.close()
        print(f"retained {len(sys.argv) - 2}", flush=True)

    elif command == "flood":
        count = int(sys.argv[2])
        prefix = sys.argv[3] if len(sys.argv) > 3 else "load"
        socks = []
        for index in range(count):
            try:
                socks.append(connect(f"{prefix}-{index:04d}"))
            except OSError as error:
                print(f"client {index} failed: {error}", flush=True)
                break
        hold(socks)

    elif command == "traffic":
        seconds, rate, size = int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
        sock = connect("load-publisher")
        payload = "x" * size
        end = time.time() + seconds
        sent = 0
        while time.time() < end:
            started = time.time()
            for index in range(rate):
                publish(sock, f"load/stream/{index % 8}", payload)
                sent += 1
            time.sleep(max(0.0, 1.0 - (time.time() - started)))
        sock.close()
        print(f"published {sent}", flush=True)

    elif command == "subscribe":
        topic_filter = sys.argv[2]
        seconds = float(sys.argv[3]) if len(sys.argv) > 3 else 60.0
        sock = connect("load-subscriber")
        subscribe(sock, topic_filter)
        print(f"subscribed to {topic_filter}", flush=True)
        time.sleep(seconds)
        sock.close()

    else:
        raise SystemExit(f"unknown command {command}\n{__doc__}")


main()
