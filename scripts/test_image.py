#!/usr/bin/env python3
"""Tests for the container image, `services/broker/Dockerfile`.

    python3 scripts/test_image.py            # builds it, then checks it
    IMAGE=sdmqtt:dev python3 scripts/test_image.py --no-build

Everything is asked of a running container, because an image is judged
by what it does, not by what its Dockerfile says:

- it starts read-only, as uid 65532, with only `/data` writable, and
  `/health/ready` answers ready on the admin port;
- an MQTT client connects;
- it has no shell, carries `LICENSE` and both notices files, and is
  under 25 MB;
- without `SDMQTT_ADMIN_TOKEN` it refuses to start, because its admin
  listener binds every interface;
- **a durable session survives `docker stop` and `docker start`**: the
  stop takes under ten seconds and exits zero, and a message queued for
  the session before the stop is delivered after it.

Exits non-zero naming each check that failed.
"""
import argparse
import json
import os
import socket
import struct
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IMAGE = os.environ.get("IMAGE", "sdmqtt:test")
TOKEN = "image-test-" + uuid.uuid4().hex  # a value made for this run alone
failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'ok  ' if ok else 'FAIL'}  {name}{(': ' + detail) if detail and not ok else ''}")
    if not ok:
        failures.append(name)


def docker(*args: str, check_status: bool = True) -> subprocess.CompletedProcess:
    done = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check_status and done.returncode != 0:
        raise SystemExit(f"docker {' '.join(args)} failed:\n{done.stderr}")
    return done


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def string(text: str) -> bytes:
    raw = text.encode()
    return struct.pack("!H", len(raw)) + raw


def packet(kind: int, body: bytes) -> bytes:
    out, length = bytes([kind]), len(body)
    while True:
        byte, length = length % 128, length // 128
        out += bytes([byte | (0x80 if length else 0)])
        if not length:
            return out + body


def read_packet(sock: socket.socket) -> tuple[int, bytes]:
    head = sock.recv(1)
    if not head:
        raise ConnectionError("closed")
    length, shift = 0, 0
    while True:
        byte = sock.recv(1)[0]
        length |= (byte & 127) << shift
        shift += 7
        if not byte & 128:
            break
    body = b""
    while len(body) < length:
        body += sock.recv(length - len(body))
    return head[0], body


def mqtt_connect(port: int, client: str, clean: bool) -> tuple[socket.socket, int, int]:
    """A 3.1.1 CONNECT; answers the socket, session present and the return code."""
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    flags = 0x02 if clean else 0x00
    sock.sendall(packet(0x10, string("MQTT") + bytes([4, flags]) + struct.pack("!H", 60) + string(client)))
    kind, body = read_packet(sock)
    assert kind == 0x20, f"expected CONNACK, got {kind:#x}"
    return sock, body[0], body[1]


def ready(admin: int, seconds: float = 30) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{admin}/health/ready", timeout=1) as answer:
                if b'"ready"' in answer.read():
                    return True
        except OSError:
            pass
        time.sleep(0.3)
    return False


def publish(admin: int, topic: str, payload: str) -> None:
    request = urllib.request.Request(
        f"http://127.0.0.1:{admin}/api/v1/publish",
        data=json.dumps({"topic": topic, "payload": payload, "qos": 1}).encode(),
        headers={"content-type": "application/json", "authorization": f"Bearer {TOKEN}"},
        method="POST",
    )
    urllib.request.urlopen(request, timeout=10).read()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()
    if not args.no_build:
        print(f"building {IMAGE}")
        subprocess.run(
            ["docker", "build", "-f", "services/broker/Dockerfile", "-t", IMAGE, "."],
            cwd=ROOT,
            check=True,
        )

    size = int(docker("image", "inspect", "--format", "{{.Size}}", IMAGE).stdout.strip())
    check("the image is under 25 MB", size < 25_000_000, f"{size / 1e6:.1f} MB")
    user = docker("image", "inspect", "--format", "{{.Config.User}}", IMAGE).stdout.strip()
    check("the image runs as uid 65532", user.split(":")[0] == "65532", user)
    shell = docker("run", "--rm", "--entrypoint", "/bin/sh", IMAGE, "-c", "true", check_status=False)
    check("the image has no shell", shell.returncode != 0)

    refused = docker("run", "--rm", IMAGE, check_status=False)
    check(
        "without an admin token it refuses to start",
        refused.returncode != 0 and "token" in (refused.stderr + refused.stdout).lower(),
        (refused.stderr + refused.stdout)[-300:],
    )

    volume = "sdmqtt-image-test-" + uuid.uuid4().hex[:8]
    name = "sdmqtt-image-test-" + uuid.uuid4().hex[:8]
    mqtt, admin = free_port(), free_port()
    docker("volume", "create", volume)
    try:
        docker(
            "run", "-d", "--name", name, "--read-only",
            "-v", f"{volume}:/data",
            "-e", f"SDMQTT_ADMIN_TOKEN={TOKEN}",
            "-p", f"127.0.0.1:{mqtt}:1883", "-p", f"127.0.0.1:{admin}:1884",
            IMAGE,
        )
        up = ready(admin)
        check("read-only, with only /data writable, it becomes ready", up,
              docker("logs", name, check_status=False).stderr[-500:])
        if not up:
            return 1
        top = docker("top", name, "-eo", "pid,uid,comm").stdout
        check("the process is uid 65532", "65532" in top, top)

        notices = docker("run", "--rm", "--entrypoint", "/sdmqtt", "-v", f"{volume}:/probe", IMAGE, "--version", check_status=False)
        check("the binary answers --version", notices.returncode == 0, notices.stderr)
        copied = subprocess.run(["docker", "cp", f"{name}:/usr/share/doc/sdmqtt/", "-"], capture_output=True)
        listing = copied.stdout
        for wanted in (b"LICENSE", b"THIRD-PARTY-NOTICES.txt", b"web-THIRD-PARTY-NOTICES.txt"):
            check(f"the image carries {wanted.decode()}", wanted in listing)

        # A durable session, made through the container: subscribe at
        # QoS 1 with a persistent session, leave, and have a message
        # queued for it while it is away.
        sock, _, code = mqtt_connect(mqtt, "durable-probe", clean=False)
        check("an MQTT client connects", code == 0, f"CONNACK {code}")
        sock.sendall(packet(0x82, struct.pack("!H", 1) + string("image/test") + b"\x01"))
        read_packet(sock)
        sock.sendall(packet(0xE0, b""))
        sock.close()
        publish(admin, "image/test", "kept across a stop")

        started = time.monotonic()
        docker("stop", name)
        took = time.monotonic() - started
        code = docker("inspect", "--format", "{{.State.ExitCode}}", name).stdout.strip()
        check("docker stop finishes inside its ten-second grace", took < 10, f"{took:.1f}s")
        check("the broker exits zero on SIGTERM", code == "0", f"exit {code}")
        docker("start", name)
        check("it comes back ready on the same volume", ready(admin))
        sock, present, code = mqtt_connect(mqtt, "durable-probe", clean=False)
        check("the session is still there", code == 0 and present == 1, f"present={present} code={code}")
        delivered = b""
        try:
            kind, body = read_packet(sock)
            delivered = body if kind & 0xF0 == 0x30 else b""
        except (OSError, ConnectionError):
            pass
        check("the message queued before the stop is delivered after it", b"kept across a stop" in delivered)
        sock.close()
    finally:
        docker("rm", "-f", name, check_status=False)
        docker("volume", "rm", "-f", volume, check_status=False)

    if failures:
        print(f"{len(failures)} failed: {', '.join(failures)}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
