#!/usr/bin/env python3
"""Generate the attribution for the Rust code that ships in the binary.

Section 11.2 says the browser half's attribution is generated from the
bundle and not from the package list. This is the same rule applied to
the other half, and it had no implementation at all: nothing in the
repository named a single crate, while eighty-odd of them are linked
into `sdmqtt`. Two of those are Apache-2.0 with no alternative branch
on our allow list, and Apache-2.0 asks for the licence text to travel
with the work.

# What "ships" means here, and how that is checked

The set is the packages reachable from `sdmqtt` along **normal**
dependency edges. Not dev-dependencies, which are not linked into it.
Not build-dependencies, which run and are thrown away. Not
procedural-macro crates, which run in the compiler: what reaches the
binary is the code they expand to, which is the expanding crate's own
output, and the macro crate itself is a build tool by any reading of
the two categories in docs/research/component-selection.md.

A dependency graph is still a manifest, though, and the point of
section 11.2's rule is to not trust one. So `--check` reads the
**built binary** and pulls every crate path out of it: rustc records
the source path of every panic site, so `tokio-1.53.1`,
`sync_wrapper-1.0.2` and the rest are in the bytes a user receives. A
crate whose path is in the binary and whose name is not in this file
fails the check.

That direction is the one that matters. The reverse does not hold and
must not be asserted: a crate can be linked in and leave no path
string at all — it has no panics, or the optimiser removed them — so
the binary names a subset. `ryu` and `sync_wrapper` are exactly that
case today, which is why the generated list is the graph and only the
audit is the binary.

# The standard library

The binary also carries the Rust standard library and the crates the
Rust project vendors into it, which arrive prebuilt from the sysroot
and are in no Cargo graph. The check finds them under `/rust/deps/`
and they are attributed in a section of their own; see
`STANDARD_LIBRARY_TEXT` and `STANDARD_LIBRARY_VENDORED` below.

# Why not cargo-about

docs/research/component-selection.md named `cargo-about`. It was
configured nowhere, and adopting it now would put a second tool in
every contributor's path and in three CI jobs to produce a file this
does in one script, with only `cargo` and `python3` — which is what
`scripts/budgets.sh` already needs. What it would not do is read the
binary, and reading the binary is the whole of section 11.2's rule.
The research is updated to say so rather than left naming a tool
nothing runs.

# Usage

    scripts/rust-notices.py            rewrite the file
    scripts/rust-notices.py --check    fail if it is not what this produces,
                                       or if the binary names a crate it omits

`--check` needs the release binary; it says what to run if there is
none.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
BROKER = ROOT / "services" / "broker"
MANIFEST = BROKER / "Cargo.toml"
OUTPUT = BROKER / "THIRD-PARTY-NOTICES.txt"
BINARY = BROKER / "target" / "release" / "sdmqtt"

# The crate whose dependencies ship. Not the workspace: the workspace
# holds test-only members and a fuzz target, and neither is
# distributed.
ROOT_PACKAGE = "sdmqtt"

# The targets this project is built for, unioned.
#
# **Explicit rather than "the host".** CI runs on Linux x86-64 and the
# development machines are macOS arm64, so a file generated for the
# host would differ between them and the drift check would fail on
# whichever machine did not write it. The two resolve identically
# today; the union is what keeps that from being luck.
TARGETS = (
    "x86_64-unknown-linux-gnu",
    "aarch64-apple-darwin",
)

# Which branch of a disjunction we take, in order of preference.
#
# Only branches on `deny.toml`'s allow list are candidates, which is
# read rather than restated below. The order is by how much the branch
# asks of us: a notice is cheaper to honour correctly than a licence
# text plus a NOTICE-propagation clause, and where a copyright holder
# has offered us the choice, taking the cheaper one is the choice they
# offered.
#
# This is what makes `ryu` an Apache-2.0 crate here. It offers
# "Apache-2.0 OR BSL-1.0", and BSL-1.0 is not on the allow list, so
# the branch we are actually on is Apache-2.0.
PREFERENCE = (
    "MIT",
    "ISC",
    "0BSD",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "Zlib",
    "Unlicense",
    "CC0-1.0",
    "Unicode-3.0",
    "CDLA-Permissive-2.0",
    "Apache-2.0 WITH LLVM-exception",
    "Apache-2.0",
)

# A file that is a licence, a notice, or a copyright statement.
LICENCE_FILE = re.compile(
    r"^(licen[cs]e|copying|notice|copyright|unlicense)([-_.][\w.+-]*)?$",
    re.IGNORECASE,
)
NOTICE_FILE = re.compile(r"^notice([-_.][\w.+-]*)?$", re.IGNORECASE)

# `<name>-<version>` as rustc spells it in a recorded source path.
IN_REGISTRY = re.compile(
    rb"registry/src/[^/\x00]{1,120}/([A-Za-z0-9_.+-]+?)-(\d+\.\d+\.\d+[A-Za-z0-9.+-]*)/"
)
IN_SYSROOT = re.compile(rb"/rust/deps/([A-Za-z0-9_.+-]+?)-(\d+\.\d+\.\d+[A-Za-z0-9.+-]*)/")

# The standard library, which is in no Cargo graph.
#
# It is linked from the sysroot as a prebuilt artifact, so nothing
# `cargo metadata` prints mentions it, and it is unambiguously in the
# binary: `--check` finds its vendored crates under `/rust/deps/`.
#
# The Rust project distributes all of it under "MIT OR Apache-2.0", so
# the MIT branch is the one taken, by the same preference as
# everything else. Its text is below rather than read from the
# sysroot, because the sysroot is a path that differs per machine and
# per toolchain and this file has to be reproducible.
STANDARD_LIBRARY_TEXT = """\
MIT License

Copyright (c) The Rust Project Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# The crates the Rust project vendors into the standard library and
# which this binary is therefore carrying. Checked against the binary
# by `--check`: one that appears under `/rust/deps/` and is not named
# here fails, so this list cannot quietly go stale.
#
# All of them are distributed by the Rust project under the same "MIT
# OR Apache-2.0" as the library itself, which is the licence the
# section above carries. The versions are the toolchain's and move
# with it, so they are not pinned here; the audit matches on name.
STANDARD_LIBRARY_VENDORED = (
    "addr2line",
    "adler2",
    "compiler_builtins",
    "gimli",
    "hashbrown",
    "libc",
    "memchr",
    "miniz_oxide",
    "object",
    "rustc-demangle",
    "std_detect",
    "unwind",
)


def run(command: list[str]) -> str:
    """Run a command in the broker workspace and return its output."""
    finished = subprocess.run(
        command,
        cwd=BROKER,
        capture_output=True,
        text=True,
        check=False,
    )
    if finished.returncode != 0:
        sys.stderr.write(finished.stderr)
        raise SystemExit(f"{command[0]} failed: {' '.join(command)}")
    return finished.stdout


def allowed_licences() -> set[str]:
    """The allow list from `deny.toml`, so there is one of them.

    Parsed rather than restated: a licence added to the Rust gate and
    not to this file would mean a crate whose branch we accept and
    whose obligation we do not record.
    """
    text = (ROOT / "deny.toml").read_text(encoding="utf-8")
    block = re.search(r"^allow\s*=\s*\[(.*?)\]", text, re.MULTILINE | re.DOTALL)
    if not block:
        raise SystemExit("deny.toml has no [licenses] allow list to read")
    return set(re.findall(r'"([^"]+)"', block.group(1)))


def effective(expression: str | None, allowed: set[str]) -> str:
    """Which licence we are actually under, given the allow list.

    A disjunction is a choice the copyright holder gave us, so the
    branch we take is the one we are obliged by, and the branches we
    did not take oblige us to nothing. A conjunction is not a choice;
    every part of it applies.
    """
    if not expression:
        raise SystemExit("a package declares no licence at all; deny.toml should have caught it")
    if "/" in expression:
        # The old separator, still on a few crates.
        expression = expression.replace("/", " OR ")
    if re.search(r"\bOR\b", expression):
        branches = [
            branch.strip(" ()")
            for branch in re.split(r"\s+OR\s+", expression)
            if branch.strip(" ()")
        ]
        candidates = [branch for branch in branches if branch in allowed]
        if not candidates:
            raise SystemExit(
                f'no branch of "{expression}" is on deny.toml\'s allow list'
            )
        for preferred in PREFERENCE:
            if preferred in candidates:
                return preferred
        return sorted(candidates)[0]
    for atom in re.split(r"\s+AND\s+", expression):
        if atom.strip(" ()") not in allowed:
            raise SystemExit(f'"{expression}" is not on deny.toml\'s allow list')
    return expression


def in_the_build(target: str) -> set[tuple[str, str]]:
    """What `cargo build --release --all-features -p sdmqtt` resolves to.

    **Asked of `cargo tree` and not of `cargo metadata`.** Metadata's
    `--all-features` turns on every feature of every workspace member,
    and the binary is built with one member's. The difference is not
    theoretical: it adds `embedded-io` twice, through a `postcard`
    feature the shipped binary does not enable, and a notices file
    naming code that does not ship is the same class of untruth as one
    omitting code that does.

    Proc macros are in here, because `cargo tree` in this toolchain
    has no way to drop them. They are dropped by the walk in
    [`shipped`], which can see which packages are macros; this set is
    only ever intersected with that walk's result.
    """
    listed = run(
        [
            "cargo",
            "tree",
            "--package",
            ROOT_PACKAGE,
            "--all-features",
            "--edges",
            "normal",
            "--target",
            target,
            "--prefix",
            "none",
            "--format",
            "{p}",
        ]
    )
    resolved: set[tuple[str, str]] = set()
    for line in listed.splitlines():
        # "name v1.2.3", then possibly " (/a/path)" or " (*)".
        match = re.match(r"^(\S+) v(\S+)", line.strip())
        if match:
            resolved.add((match.group(1), match.group(2)))
    if not resolved:
        raise SystemExit(f"cargo tree resolved nothing for {target}")
    return resolved


def shipped(allowed: set[str]) -> dict[tuple[str, str], dict]:
    """Every external package linked into the binary, by name and version."""
    found: dict[tuple[str, str], dict] = {}
    for target in TARGETS:
        resolved = in_the_build(target)
        metadata = json.loads(
            run(
                [
                    "cargo",
                    "metadata",
                    "--format-version",
                    "1",
                    "--all-features",
                    "--filter-platform",
                    target,
                    "--manifest-path",
                    str(MANIFEST),
                ]
            )
        )
        packages = {package["id"]: package for package in metadata["packages"]}
        nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
        members = set(metadata["workspace_members"])
        roots = [
            identifier
            for identifier in members
            if packages[identifier]["name"] == ROOT_PACKAGE
        ]
        if len(roots) != 1:
            raise SystemExit(f"expected exactly one {ROOT_PACKAGE} package, found {len(roots)}")

        seen: set[str] = set()
        queue = [roots[0]]
        while queue:
            identifier = queue.pop()
            if identifier in seen:
                continue
            seen.add(identifier)
            for edge in nodes[identifier]["deps"]:
                # `kind` is null for a normal dependency. A dev- or a
                # build-dependency is not linked into the binary.
                if not any(kind["kind"] is None for kind in edge["dep_kinds"]):
                    continue
                dependency = packages[edge["pkg"]]
                # Metadata resolved more features than the binary is
                # built with; see `in_the_build`.
                if (dependency["name"], dependency["version"]) not in resolved:
                    continue
                if any(
                    "proc-macro" in target_kind["kind"]
                    for target_kind in dependency["targets"]
                ):
                    # It runs in the compiler. What ships is the code
                    # it expanded to, which belongs to the crate that
                    # invoked it.
                    continue
                queue.append(edge["pkg"])
                if edge["pkg"] in members:
                    continue
                key = (dependency["name"], dependency["version"])
                if key in found:
                    continue
                found[key] = describe(dependency, allowed)
    return found


def describe(package: dict, allowed: set[str]) -> dict:
    """One package, with the texts it ships."""
    directory = Path(package["manifest_path"]).parent
    texts: list[tuple[str, str]] = []
    notices: list[tuple[str, str]] = []
    for entry in sorted(os.listdir(directory)):
        if not LICENCE_FILE.match(entry):
            continue
        path = directory / entry
        if not path.is_file():
            continue
        body = path.read_text(encoding="utf-8", errors="replace").strip()
        if not body:
            continue
        if NOTICE_FILE.match(entry):
            notices.append((entry, body))
        else:
            texts.append((entry, body))
    declared = package.get("license")
    chosen = effective(declared, allowed)
    if texts:
        # Keep only the files that belong to the branch we took, when
        # the names say which is which. A crate offering MIT and
        # Apache-2.0 ships both texts, and copying the one we did not
        # take would say we are under a licence we are not.
        named = [
            (entry, body)
            for entry, body in texts
            if branch_of(entry) is not None and branch_of(entry) == branch_of_spdx(chosen)
        ]
        if named:
            texts = named
    return {
        "name": package["name"],
        "version": package["version"],
        "declared": declared,
        "effective": chosen,
        "repository": package.get("repository"),
        "authors": package.get("authors") or [],
        "texts": texts,
        "notices": notices,
    }


def branch_of(filename: str) -> str | None:
    """Which licence a licence file's name says it is, if it says."""
    upper = filename.upper()
    for token, branch in (
        ("APACHE", "APACHE"),
        ("MIT", "MIT"),
        ("BSD", "BSD"),
        ("ISC", "ISC"),
        ("ZLIB", "ZLIB"),
        ("UNLICENSE", "UNLICENSE"),
        ("CC0", "CC0"),
        ("BOOST", "BSL"),
        ("BSL", "BSL"),
    ):
        if token in upper:
            return branch
    return None


def branch_of_spdx(identifier: str) -> str | None:
    """The same vocabulary, from an SPDX identifier."""
    return branch_of(identifier.replace("-", ""))


def render(packages: dict[tuple[str, str], dict]) -> str:
    """The file."""
    ordered = [packages[key] for key in sorted(packages, key=lambda key: (key[0].lower(), key[1]))]
    lines: list[str] = [
        "Third-party notices for the sd-mqtt broker (Rust)",
        "=================================================",
        "",
        "sd-mqtt itself is MIT licensed; see LICENSE at the root of the",
        "repository. This file covers the third-party Rust code compiled into",
        "the `sdmqtt` binary, which is not all MIT and is never claimed to be.",
        "",
        "Generated by scripts/rust-notices.py from the packages reachable from",
        "`sdmqtt` along normal dependency edges, for every target this project",
        "is built for. Dev-dependencies, build-dependencies and procedural",
        "macros are absent: none of them is linked into the binary, and running",
        "a tool to produce an artifact does not distribute the tool.",
        "",
        "The list is audited against the binary rather than trusted. rustc",
        "records the source path of every panic site, so the crates that ship",
        "are named in the bytes a user receives; `scripts/rust-notices.py",
        "--check` reads them out of the release binary and fails on any crate",
        "the binary names and this file does not. CI fails on any difference",
        "between the regenerated file and this one.",
        "",
        f"Packages: {len(ordered)}, plus the Rust standard library.",
        "",
    ]

    lines.append("Summary")
    lines.append("-------")
    lines.append("")
    for package in ordered:
        declared = package["declared"]
        chosen = package["effective"]
        suffix = "" if declared == chosen else f"  (of {declared})"
        lines.append(f"  {package['name']} {package['version']} — {chosen}{suffix}")
    lines.append("  the Rust standard library — MIT  (of MIT OR Apache-2.0)")
    lines.append("")

    # Apache-2.0 section 4(d) propagates a NOTICE file when the work
    # has one. Saying so out loud, either way, because "there was no
    # NOTICE file" and "nobody looked" are the same silence.
    with_notices = [package for package in ordered if package["notices"]]
    lines.append("NOTICE files")
    lines.append("------------")
    lines.append("")
    if with_notices:
        lines.append("Apache-2.0 section 4(d) propagates these:")
        lines.append("")
        for package in with_notices:
            for entry, body in package["notices"]:
                lines.append(f"  {package['name']} {package['version']} — {entry}")
                lines.append("")
                lines.extend(f"    {line}".rstrip() for line in body.splitlines())
                lines.append("")
    else:
        lines.append(
            "None of the packages above ships a NOTICE file, so Apache-2.0"
        )
        lines.append(
            "section 4(d) is not triggered by anything here. Checked, not assumed:"
        )
        lines.append(
            "this line is written by the generator from what it found in each"
        )
        lines.append("package's own directory.")
        lines.append("")

    lines.append("Licences")
    lines.append("--------")
    lines.append("")
    lines.append(
        "One block per distinct text. A text shipped verbatim by several"
    )
    lines.append(
        "packages appears once, naming all of them; a text that differs by so"
    )
    lines.append("much as a copyright line is its own block.")
    lines.append("")

    # Group by the exact text, so that the one Apache-2.0 text every
    # crate ships appears once and a per-crate MIT copyright line
    # keeps its own block.
    blocks: dict[str, list[str]] = {}
    order: list[str] = []
    for package in ordered:
        if not package["texts"]:
            continue
        for _, body in package["texts"]:
            who = f"{package['name']} {package['version']} ({package['effective']})"
            if body not in blocks:
                blocks[body] = []
                order.append(body)
            if who not in blocks[body]:
                blocks[body].append(who)
    # The Rust standard library, last and on its own: it is not a
    # package and has no manifest, and grouping it with one would
    # suggest it came from the same place.
    for body in order:
        lines.append("-" * 70)
        for who in blocks[body]:
            lines.append(who)
        lines.append("")
        lines.extend(line.rstrip() for line in body.splitlines())
        lines.append("")

    missing = [package for package in ordered if not package["texts"]]
    if missing:
        lines.append("-" * 70)
        lines.append("Packages shipping no licence file of their own")
        lines.append("")
        lines.append(
            "Their licence is the identifier their manifest declares, and the"
        )
        lines.append("text of that licence is above.")
        lines.append("")
        for package in missing:
            lines.append(
                f"  {package['name']} {package['version']} — {package['effective']}"
            )
        lines.append("")

    lines.append("-" * 70)
    lines.append("the Rust standard library (MIT)")
    lines.append("")
    lines.append(
        "Linked from the sysroot as a prebuilt artifact, so it is in no Cargo"
    )
    lines.append(
        "graph and no manifest names it. It is in the binary all the same, and"
    )
    lines.append(
        "so are the crates the Rust project vendors into it: "
    )
    lines.append("")
    lines.append("  " + ", ".join(STANDARD_LIBRARY_VENDORED))
    lines.append("")
    lines.append(
        "The Rust project distributes all of that under MIT OR Apache-2.0, and"
    )
    lines.append("the MIT branch is the one taken.")
    lines.append("")
    lines.extend(line.rstrip() for line in STANDARD_LIBRARY_TEXT.splitlines())
    lines.append("")
    return "\n".join(lines) + "\n"


def in_the_binary() -> tuple[set[tuple[str, str]], set[str]]:
    """Every crate the built binary names, from registry and sysroot."""
    if not BINARY.is_file():
        raise SystemExit(
            f"there is no release binary at {BINARY} to audit against.\n"
            "  build it first:  (cd services/broker && cargo build --release --all-features -p sdmqtt)"
        )
    blob = BINARY.read_bytes()
    registry = {
        (name.decode(), version.decode()) for name, version in IN_REGISTRY.findall(blob)
    }
    sysroot = {name.decode() for name, _ in IN_SYSROOT.findall(blob)}
    return registry, sysroot


def check(generated: str) -> int:
    """Fail on drift, and on a crate the binary names and the file omits."""
    problems: list[str] = []
    if not OUTPUT.is_file():
        problems.append(f"{OUTPUT.relative_to(ROOT)} does not exist")
    elif OUTPUT.read_text(encoding="utf-8") != generated:
        problems.append(
            f"{OUTPUT.relative_to(ROOT)} is not what the dependency graph produces.\n"
            "    Run scripts/rust-notices.py and commit the result."
        )

    registry, sysroot = in_the_binary()
    named = set(re.findall(r"^  (\S+) (\S+) — ", generated, re.MULTILINE))
    for name, version in sorted(registry):
        if (name, version) not in named:
            problems.append(
                f"the binary carries {name} {version} and this file does not name it"
            )
    vendored = set(STANDARD_LIBRARY_VENDORED)
    for name in sorted(sysroot):
        if name not in vendored:
            problems.append(
                f"the binary carries {name} from the sysroot and "
                "STANDARD_LIBRARY_VENDORED does not list it"
            )

    if problems:
        for problem in problems:
            print(f"  rust-notices: {problem}", file=sys.stderr)
        return 1
    print(
        f"  the notices name every one of the {len(registry)} crates the binary "
        f"names, and every one of the {len(sysroot)} it links from the sysroot"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if the file is stale or the binary names a crate it omits",
    )
    arguments = parser.parse_args()

    generated = render(shipped(allowed_licences()))
    if arguments.check:
        return check(generated)
    OUTPUT.write_text(generated, encoding="utf-8")
    print(f"  wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
