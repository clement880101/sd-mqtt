#!/usr/bin/env python3
"""Section 15's idle resident memory budget: under 30 MB.

    scripts/idle-memory.py services/broker/target/release/sdmqtt

Starts the given binary as one node with a store and no clients, waits
until `/health/ready` says ready, lets it settle, reads its resident
memory, stops it with SIGTERM and prints the figure against the budget
with its headroom, as `budgets.sh` does for the other three.

Exit 0 within budget, 1 over it, and **2 when it could not measure** —
a binary that is missing, never becomes ready, or exits early. A check
that quietly passes when it cannot measure is worse than no check;
`scripts/test_idle_memory.py` runs each of those paths.

Resident memory is `VmRSS` from `/proc/<pid>/status` on Linux and
`ps -o rss=` elsewhere, both in kibibytes. Measured with a store,
because a node that keeps nothing is not the node anyone runs, and the
store's cache is part of what idles.

Not a gate on every push: a process has to start and settle, so CI runs
it on the nightly schedule, as section 15 says.
"""
import argparse
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

BUDGET_BYTES = 30_000_000  # "under 30 MB": decimal, the stricter reading


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def resident_bytes(pid: int) -> int:
    status = Path(f"/proc/{pid}/status")
    if status.exists():
        for line in status.read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
        raise RuntimeError("no VmRSS line")
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
    return int(out.stdout.strip()) * 1024


def megabytes(count: int) -> str:
    return f"{count / 1_000_000:.2f} MB"


def cannot(why: str) -> "NoReturn":  # noqa: F821
    print(f"  idle resident memory cannot be measured: {why}", file=sys.stderr)
    sys.exit(2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("binary")
    parser.add_argument("--settle", type=float, default=10.0, help="seconds after ready")
    parser.add_argument("--ready-timeout", type=float, default=30.0)
    args = parser.parse_args()

    binary = Path(args.binary)
    if not (binary.is_file() and os.access(binary, os.X_OK)):
        cannot(f"{binary} is not an executable file")

    work = Path(tempfile.mkdtemp(prefix="sdmqtt-idle-"))
    admin = f"127.0.0.1:{free_port()}"
    env = {**os.environ, "SDMQTT_CONFIG": "", "SDMQTT_STORAGE_DIRECTORY": ""}
    log = (work / "broker.log").open("w")
    child = subprocess.Popen(
        [
            str(binary),
            "serve",
            "--address",
            f"127.0.0.1:{free_port()}",
            "--admin-address",
            admin,
            "--store-directory",
            str(work / "store"),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=log,
        env=env,
    )
    try:
        deadline = time.monotonic() + args.ready_timeout
        while True:
            if child.poll() is not None:
                cannot(f"the broker exited with {child.returncode} before it was ready")
            try:
                with urllib.request.urlopen(f"http://{admin}/health/ready", timeout=1) as answer:
                    if b'"ready"' in answer.read():
                        break
            except OSError:
                pass
            if time.monotonic() > deadline:
                cannot(f"the broker never became ready within {args.ready_timeout:g}s")
            time.sleep(0.2)
        time.sleep(args.settle)
        if child.poll() is not None:
            cannot(f"the broker exited with {child.returncode} while settling")
        measured = resident_bytes(child.pid)
    finally:
        if child.poll() is None:
            child.send_signal(signal.SIGTERM)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
        log.close()
        shutil.rmtree(work, ignore_errors=True)

    name = "idle resident memory"
    if measured >= BUDGET_BYTES:
        print(
            f"  {name:<32} {megabytes(measured):>13} of {megabytes(BUDGET_BYTES):<12} "
            f"OVER BUDGET by {megabytes(measured - BUDGET_BYTES)}"
        )
        sys.exit(1)
    headroom = 100 - measured * 100 // BUDGET_BYTES
    print(
        f"  {name:<32} {megabytes(measured):>13} of {megabytes(BUDGET_BYTES):<12} "
        f"{headroom:>3}% headroom   ok"
    )


if __name__ == "__main__":
    main()
