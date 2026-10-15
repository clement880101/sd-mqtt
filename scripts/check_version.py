#!/usr/bin/env python3
"""Refuse a release whose version is not the same everywhere.

    scripts/check_version.py v0.7.0 [--binary PATH] [--notes FILE]

The tag, the workspace's `Cargo.toml`, the interface's `package.json`,
a `## 0.7.0 …` heading in `CHANGELOG.md` and, when given, the built
binary's `--version` must all say one version. Each disagreement is
named. With `--notes`, the CHANGELOG's section for that version — from
its heading to the next `## ` — is written to FILE as the release's
body.

The release workflow runs this first, so a tag the build disagrees with
fails before anything is built or published. `scripts/test_check_version.py`
breaks each source in turn.
"""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("tag")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--notes", type=Path)
    args = parser.parse_args()
    root = args.root
    problems: list[str] = []

    matched = re.fullmatch(r"v(\d+\.\d+\.\d+)", args.tag)
    if not matched:
        print(f"  the tag {args.tag!r} is not v<major>.<minor>.<patch>", file=sys.stderr)
        return 1
    version = matched[1]

    cargo = (root / "services/broker/Cargo.toml").read_text()
    section = cargo.split("[workspace.package]", 1)[-1]
    found = re.search(r'^version\s*=\s*"([^"]+)"', section, re.M)
    if not found or found[1] != version:
        problems.append(f"services/broker/Cargo.toml says {found[1] if found else 'nothing'}")

    web = json.loads((root / "services/web/package.json").read_text()).get("version")
    if web != version:
        problems.append(f"services/web/package.json says {web}")

    changelog = (root / "CHANGELOG.md").read_text().splitlines()
    starts = [i for i, line in enumerate(changelog) if re.match(rf"^## {re.escape(version)}(\s|$)", line)]
    if not starts:
        problems.append(f"CHANGELOG.md has no '## {version} …' heading")

    if args.binary is not None:
        said = subprocess.run([str(args.binary), "--version"], capture_output=True, text=True)
        if said.stdout.strip() != f"sdmqtt {version}":
            problems.append(f"the binary's --version says {said.stdout.strip() or said.stderr.strip()!r}")

    if problems:
        print(f"  {args.tag} does not agree with:", file=sys.stderr)
        for problem in problems:
            print(f"    {problem}", file=sys.stderr)
        return 1

    if args.notes is not None:
        start = starts[0] + 1
        end = next((i for i in range(start, len(changelog)) if changelog[i].startswith("## ")), len(changelog))
        args.notes.write_text("\n".join(changelog[start:end]).strip() + "\n")
    print(f"  {args.tag}: every source says {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
