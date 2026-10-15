#!/usr/bin/env python3
"""Tests for `scripts/idle-memory.py`: that it fails when it should.

    python3 scripts/test_idle_memory.py

The budget scripts' rule, turned on this one: a check that quietly
passes when it cannot measure is worse than no check, so each way it
cannot measure is run and its exit code required. Exit codes are what a
CI job reads: 0 within budget, 1 over it, 2 not measured.

The broker is `fixtures/fake_broker.py`, which holds a known amount of
memory, because the case worth seeing fail is the one over budget and
the real broker is not.
"""
import os
import subprocess
import sys
from pathlib import Path

here = Path(__file__).resolve().parent
check = here / "idle-memory.py"
fake = here / "fixtures" / "fake_broker.py"
failures = 0


def expect(name, wanted, *args, env=None, contains=None):
    global failures
    done = subprocess.run(
        [sys.executable, str(check), *args],
        env={**os.environ, **(env or {})},
        capture_output=True,
        text=True,
        timeout=120,
    )
    said = done.stdout + done.stderr
    ok = done.returncode == wanted and (contains is None or contains in said)
    print(f"  {'ok  ' if ok else 'FAIL'}  {name} (exit {done.returncode})")
    if not ok:
        failures += 1
        print("        " + said.replace("\n", "\n        "))


expect("a binary that does not exist", 2, "/nonexistent/sdmqtt", contains="cannot be measured")
expect(
    "a broker that never becomes ready",
    2,
    str(fake),
    "--ready-timeout",
    "2",
    env={"FAKE_READY": "0"},
    contains="never became ready",
)
expect(
    "a broker over budget",
    1,
    str(fake),
    "--settle",
    "1",
    env={"FAKE_HOLD_MB": "40"},
    contains="OVER BUDGET",
)
expect(
    "a broker within budget",
    0,
    str(fake),
    "--settle",
    "1",
    env={"FAKE_HOLD_MB": "1"},
    contains="headroom",
)
sys.exit(1 if failures else 0)
