#!/usr/bin/env python3
"""Tests for how `scripts/test_package.py` runs an archive's binary.

    python3 scripts/test_test_package.py

`test_package.py` runs each binary where it can: natively, under
Rosetta, or in a container of its architecture. The container case is
the one that differs by host, and the first version of it used host
networking on Linux, which discards a published port: on a Linux runner
the test polled a port nothing listened on, and passed only on the Mac
it was written on. So the commands are built by a pure function and
checked here for every host a release runs on.
"""
import importlib.util
import sys
from pathlib import Path

spec = importlib.util.spec_from_file_location("test_package", Path(__file__).with_name("test_package.py"))
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)
failures = 0
unpacked = Path("/tmp/unpacked")


def expect(name, ok, detail=""):
    global failures
    print(f"  {'ok  ' if ok else 'FAIL'}  {name}{(': ' + str(detail)) if not ok else ''}")
    failures += 0 if ok else 1


for system, machine in (("linux", "x86_64"), ("darwin", "arm64")):
    version, serve = package.commands("aarch64-unknown-linux-musl", unpacked, 41884, 41883, system, machine)
    where = f"an aarch64 Linux archive on a {system} {machine} host"
    expect(f"{where} runs in an arm64 container", version[:2] == ["docker", "run"] and "linux/arm64" in version, version)
    expect(f"{where} publishes the admin port", "127.0.0.1:41884:1884" in serve, serve)
    expect(f"{where} does not use host networking", "host" not in serve, serve)
    expect(f"{where} gives the container a token", any(a.startswith("SDMQTT_ADMIN_TOKEN=") for a in serve), serve)

version, serve = package.commands("x86_64-unknown-linux-musl", unpacked, 41884, 41883, "linux", "x86_64")
expect("an x86_64 Linux archive on an x86_64 Linux host runs natively", version[0] == str(unpacked / "sdmqtt"), version)
expect("and its admin listener is on the polled port", "127.0.0.1:41884" in serve, serve)

version, serve = package.commands("x86_64-apple-darwin", unpacked, 41884, 41883, "darwin", "arm64")
expect("an Intel macOS archive on Apple silicon runs under Rosetta", version[:2] == ["arch", "-x86_64"], version)

version, _ = package.commands("aarch64-apple-darwin", unpacked, 41884, 41883, "darwin", "arm64")
expect("an Apple silicon archive on Apple silicon runs natively", version[0] == str(unpacked / "sdmqtt"), version)

sys.exit(1 if failures else 0)
