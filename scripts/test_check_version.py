#!/usr/bin/env python3
"""Tests for `scripts/check_version.py`: every disagreement is refused.

    python3 scripts/test_check_version.py

Each case builds a small tree — a workspace manifest, the interface's
package.json, a CHANGELOG and a stand-in binary — that agrees on one
version, then breaks exactly one of them and requires the check to fail
naming it. The release workflow runs the check before it builds
anything, so a tag the build disagrees with never becomes a release
(the plan's Review Focus 4).
"""
import os
import subprocess
import sys
import tempfile
from pathlib import Path

here = Path(__file__).resolve().parent
check = here / "check_version.py"
failures = 0


def tree(root: Path, cargo="0.7.0", web="0.7.0", heading="## 0.7.0 — 2026-10-15 — milestone 7: hardening", said="sdmqtt 0.7.0"):
    (root / "services/broker").mkdir(parents=True)
    (root / "services/web").mkdir(parents=True)
    (root / "services/broker/Cargo.toml").write_text(
        f'[workspace]\nmembers = ["crates/*"]\n\n[workspace.package]\nversion = "{cargo}"\nedition = "2021"\n'
    )
    (root / "services/web/package.json").write_text(f'{{\n  "name": "@sd-mqtt/web",\n  "version": "{web}"\n}}\n')
    (root / "CHANGELOG.md").write_text(
        f"# Changelog\n\nIntro.\n\n{heading}\n\n### Added\n\n- the thing\n\n## 0.6.0 — earlier\n\n- older\n"
    )
    binary = root / "sdmqtt"
    binary.write_text(f"#!/bin/sh\necho '{said}'\n")
    binary.chmod(0o755)
    return binary


def expect(name, wanted, tag, mention=None, **broken):
    global failures
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        binary = tree(root, **broken)
        notes = root / "notes.md"
        done = subprocess.run(
            [sys.executable, str(check), tag, "--root", str(root), "--binary", str(binary), "--notes", str(notes)],
            capture_output=True,
            text=True,
        )
        said = done.stdout + done.stderr
        ok = done.returncode == wanted and (mention is None or mention in said)
        if ok and wanted == 0:
            body = notes.read_text()
            ok = "- the thing" in body and "0.6.0" not in body and "## 0.7.0" not in body
            said += "\nnotes:\n" + body
        print(f"  {'ok  ' if ok else 'FAIL'}  {name} (exit {done.returncode})")
        if not ok:
            failures += 1
            print("        " + said.replace("\n", "\n        "))


expect("everything agrees, and the notes are that version's section alone", 0, "v0.7.0")
expect("a tag that is not v<major>.<minor>.<patch>", 1, "0.7.0", mention="tag")
expect("the workspace manifest disagrees", 1, "v0.7.0", mention="Cargo.toml", cargo="0.1.0")
expect("the interface's package.json disagrees", 1, "v0.7.0", mention="package.json", web="0.1.0")
expect("the CHANGELOG has no heading for it", 1, "v0.7.0", mention="CHANGELOG", heading="## Unreleased — milestone 7")
expect("the built binary says another version", 1, "v0.7.0", mention="--version", said="sdmqtt 0.1.0")
sys.exit(1 if failures else 0)
