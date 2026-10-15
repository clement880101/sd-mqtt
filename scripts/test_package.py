#!/usr/bin/env python3
"""Tests for a release archive, as `scripts/package.sh` writes it.

    python3 scripts/test_package.py dist/sdmqtt-0.7.0-aarch64-apple-darwin.tar.gz

An archive is judged by unpacking it and running what is inside, because
the failure worth catching is the one that builds and does not run: a
binary linked against something absent, built for the wrong machine, or
built before the interface existed (the plan's Review Focus 3).

- the `.sha256` beside it matches;
- it holds exactly the binary, `LICENSE`, `README.md` and both notices
  files, in one directory named for the archive;
- the binary is under section 15's 15 MB;
- `sdmqtt --version` says the version the archive is named for;
- `sdmqtt serve` becomes ready and serves the interface's page.

A binary this machine cannot run natively is run where it can be: an
Intel macOS binary under Rosetta, a Linux binary in a container of its
architecture (Docker runs the other architecture under emulation).

Exits non-zero naming each check that failed.
"""
import hashlib
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
from pathlib import Path

NAME = re.compile(r"^sdmqtt-(?P<version>[^-]+)-(?P<target>.+)\.tar\.gz$")
CONTENTS = {"sdmqtt", "LICENSE", "README.md", "THIRD-PARTY-NOTICES.txt", "web-THIRD-PARTY-NOTICES.txt"}
BUDGET_BYTES = 15_000_000
failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"  {'ok  ' if ok else 'FAIL'}  {name}{(': ' + detail) if detail and not ok else ''}")
    if not ok:
        failures.append(name)


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def commands(target: str, unpacked: Path, admin: int, mqtt: int, system: str, machine: str) -> tuple[list[str], list[str]]:
    """How to ask the binary its version, and how to serve with it, on this host.

    The admin listener ends up on `127.0.0.1:<admin>` of the host either
    way. In a container that is a published port with bridge networking,
    on every host: host networking would discard the publication, which
    is the bug `scripts/test_test_package.py` exists to keep out.
    """
    machine = machine.lower()
    binary = str(unpacked / "sdmqtt")
    native_serve = ["serve", "--address", f"127.0.0.1:{mqtt}", "--admin-address", f"127.0.0.1:{admin}"]
    if target.endswith("apple-darwin"):
        if system != "darwin":
            raise SystemExit(f"{target} can only be run on macOS")
        prefix = ["arch", "-x86_64"] if target.startswith("x86_64") and machine in ("arm64", "aarch64") else []
        return [*prefix, binary, "--version"], [*prefix, binary, *native_serve]
    arch = "amd64" if target.startswith("x86_64") else "arm64"
    native = system == "linux" and (
        (arch == "amd64" and machine in ("x86_64", "amd64")) or (arch == "arm64" and machine in ("aarch64", "arm64"))
    )
    if native:
        return [binary, "--version"], [binary, *native_serve]
    container = ["docker", "run", "--rm", "--platform", f"linux/{arch}", "-v", f"{unpacked}:/pkg:ro"]
    return (
        [*container, "alpine:3.20", "/pkg/sdmqtt", "--version"],
        [*container, "-i", "-p", f"127.0.0.1:{admin}:1884", "-e", "SDMQTT_ADMIN_TOKEN=package-test",
         "alpine:3.20", "/pkg/sdmqtt", "serve", "--admin-address", "0.0.0.0:1884"],
    )


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    archive = Path(sys.argv[1]).resolve()
    matched = NAME.match(archive.name)
    if not archive.is_file() or not matched:
        raise SystemExit(f"{archive} is not a release archive")
    version, target = matched["version"], matched["target"]
    print(f"{archive.name}: version {version}, target {target}")

    checksum = archive.with_name(archive.name + ".sha256")
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    recorded = checksum.read_text().split() if checksum.is_file() else []
    check("the .sha256 beside it matches", recorded[:2] == [digest, archive.name], " ".join(recorded))

    work = Path(tempfile.mkdtemp(prefix="sdmqtt-package-"))
    try:
        with tarfile.open(archive) as tar:
            names = tar.getnames()
            owners = {(member.uid, member.gid, member.uname, member.gname) for member in tar.getmembers()}
            foreign = sorted({key for member in tar.getmembers() for key in member.pax_headers})
            tar.extractall(work, filter="data")
        # Whoever extracts with sudo gets these owners, so they are root
        # and not the build runner's user.
        check("every entry is owned by root", owners == {(0, 0, "root", "root")}, str(owners))
        check("no entry carries the build machine's extended attributes", not foreign, str(foreign))
        top = f"sdmqtt-{version}-{target}"
        inside = {Path(name).name for name in names if name != top}
        check("one directory named for the archive", all(n == top or n.startswith(top + "/") for n in names), str(names))
        check("exactly the binary, LICENSE, README and both notices", inside == CONTENTS, str(sorted(inside)))
        unpacked = work / top
        binary = unpacked / "sdmqtt"
        size = binary.stat().st_size if binary.is_file() else BUDGET_BYTES
        check("the binary is under 15 MB", size < BUDGET_BYTES, f"{size / 1e6:.2f} MB")

        admin = free_port()
        version_command, serve = commands(target, unpacked, admin, free_port(), sys.platform, platform.machine())
        host = "127.0.0.1"
        said = subprocess.run(version_command, capture_output=True, text=True, timeout=120)
        check(f"--version says {version}", said.stdout.strip() == f"sdmqtt {version}", said.stdout + said.stderr)

        broker = subprocess.Popen(
            serve, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            env={**os.environ, "SDMQTT_CONFIG": "", "SDMQTT_STORAGE_DIRECTORY": ""},
        )
        try:
            page, ready = b"", False
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline and broker.poll() is None:
                try:
                    with urllib.request.urlopen(f"http://{host}:{admin}/health/ready", timeout=1) as answer:
                        ready = b'"ready"' in answer.read()
                    with urllib.request.urlopen(f"http://{host}:{admin}/", timeout=5) as answer:
                        page = answer.read()
                    break
                except OSError:
                    time.sleep(0.5)
            check("serve becomes ready", ready, (broker.stderr.read1(2000) if broker.poll() is not None else b"").decode(errors="replace"))
            check("the interface is embedded and served", b"<div id=\"app\"" in page, page[:200].decode(errors="replace"))
        finally:
            broker.terminate()
            try:
                broker.wait(timeout=15)
            except subprocess.TimeoutExpired:
                broker.kill()
    finally:
        shutil.rmtree(work, ignore_errors=True)

    if failures:
        print(f"{len(failures)} failed: {', '.join(failures)}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
