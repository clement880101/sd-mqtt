#!/usr/bin/env bash
#
# Build one release archive.
#
#     scripts/package.sh aarch64-apple-darwin
#     scripts/package.sh x86_64-unknown-linux-musl
#
# Writes dist/sdmqtt-<version>-<target>.tar.gz and its .sha256. The
# archive holds one directory, sdmqtt-<version>-<target>/, with the
# binary, LICENSE, README.md and both notices files (section 3: the
# attribution travels with what ships).
#
# **Linux targets are cut from the image's builder** (services/broker/
# Dockerfile, target `export`), so the binary in an archive and the
# binary in the image are one build: static musl, cross-compiled with
# cargo-zigbuild, audited against the notices inside the build. Needs
# Docker with buildx.
#
# **macOS targets are built here**, on a Mac, with cargo for the target,
# and audited the same way. Needs rustup's target and Node for the
# interface.
#
# The version is the workspace's. scripts/test_package.py is what checks
# an archive this writes; run it after.
#
# Section 10.4's measurement rides on the Linux path: CARGO_FLAGS is
# passed to the build, so `CARGO_FLAGS=--no-default-features` cuts a musl
# binary on the system allocator for comparison. Not for release.
set -euo pipefail

target="${1:?usage: scripts/package.sh <target>}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

version="$(sed -n 's/^version = "\(.*\)"/\1/p' services/broker/Cargo.toml | head -1)"
[ -n "$version" ] || { echo "no version in services/broker/Cargo.toml" >&2; exit 2; }
name="sdmqtt-$version-$target"
out="$root/dist"
stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
mkdir -p "$out" "$stage/$name"

case "$target" in
    x86_64-unknown-linux-musl | aarch64-unknown-linux-musl)
        case "$target" in
            x86_64-*) platform=linux/amd64 ;;
            aarch64-*) platform=linux/arm64 ;;
        esac
        docker buildx build -f services/broker/Dockerfile \
            --platform "$platform" \
            --build-arg CARGO_FLAGS="${CARGO_FLAGS:-}" \
            --target export \
            --output "type=local,dest=$stage/$name" .
        ;;
    x86_64-apple-darwin | aarch64-apple-darwin)
        [ "$(uname -s)" = Darwin ] || { echo "$target is built on macOS" >&2; exit 2; }
        if [ ! -f services/web/dist/index.html ]; then
            (cd services/web && npm ci && npm run build)
        fi
        # From the broker's directory, because its rust-toolchain.toml
        # pins the toolchain the target has to be added to.
        (cd services/broker && rustup target add "$target" >/dev/null)
        (cd services/broker && cargo build --release --locked -p sdmqtt --target "$target" ${CARGO_FLAGS:-})
        binary="services/broker/target/$target/release/sdmqtt"
        python3 scripts/rust-notices.py --check --binary "$binary"
        cp "$binary" "$stage/$name/sdmqtt"
        cp LICENSE README.md "$stage/$name/"
        cp services/broker/THIRD-PARTY-NOTICES.txt "$stage/$name/THIRD-PARTY-NOTICES.txt"
        cp services/web/THIRD-PARTY-NOTICES.txt "$stage/$name/web-THIRD-PARTY-NOTICES.txt"
        ;;
    *)
        echo "no release target $target; the four that ship are listed in docs/design.md, milestone 7" >&2
        exit 2
        ;;
esac

chmod 0755 "$stage/$name/sdmqtt"
# Written by Python's tarfile rather than whichever tar the host has: in
# a fixed order, owned by root, and carrying nothing of the machine it
# was built on. GNU tar on a CI runner wrote the runner's uid into every
# entry, and macOS's tar wrote AppleDouble files and its own extended
# attributes; scripts/test_package.py checks for all three.
python3 - "$stage" "$name" "$out/$name.tar.gz" <<'PACK'
import os, sys, tarfile
stage, name, destination = sys.argv[1:]
def owned(info):
    info.uid = info.gid = 0
    info.uname = info.gname = "root"
    info.pax_headers = {}
    return info
with tarfile.open(destination, "w:gz", format=tarfile.USTAR_FORMAT) as tar:
    tar.add(os.path.join(stage, name), arcname=name, recursive=False, filter=owned)
    for entry in sorted(os.listdir(os.path.join(stage, name))):
        tar.add(os.path.join(stage, name, entry), arcname=f"{name}/{entry}", filter=owned)
PACK
(cd "$out" && { command -v sha256sum >/dev/null && sha256sum "$name.tar.gz" || shasum -a 256 "$name.tar.gz"; } > "$name.tar.gz.sha256")
echo "wrote dist/$name.tar.gz"
