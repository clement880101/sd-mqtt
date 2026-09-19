# Foundation and MQTT Codec Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the sd-mqtt monorepo and build `mqtt-codec`, a pure library that encodes and decodes every MQTT 3.1.1 and 5.0 packet with no I/O.

**Architecture:** One Cargo workspace under `services/broker`. The codec crate owns wire format only: bytes in, typed packets out, errors as values. It never allocates a packet body before checking the declared size against a limit, and it returns "need more bytes" rather than blocking or panicking. A single set of packet types serves both protocol versions, with version passed in at call time, so there is no duplicated state machine later.

**Tech Stack:** Rust (edition 2021), `bytes` for buffers, `thiserror` for typed errors, `proptest` for round-trip properties, `cargo-fuzz` for the decoder, `cargo-nextest` as the runner, `cargo-deny` for the license gate.

**Spec:** `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`, sections 4 and 5.1.

**Order within a task.** Each task lists its module registration as a late
step, because that reads better. Perform it *first*. A new file is invisible
to the compiler until `lib.rs` declares it, so writing the test before the
declaration produces "no tests ran" rather than the compile failure the step
expects.

**Scope note:** This plan stops at the codec. The in-memory broker core (topic trie, sessions, delivery, listener, conformance suite) is the next plan and depends on this one.

---

## File Structure

| Path | Responsibility |
|---|---|
| `LICENSE` | MIT license text |
| `README.md` | What sd-mqtt is, how to build |
| `.gitignore` | Rust and Node ignores |
| `deny.toml` | License allow list for CI |
| `.github/workflows/ci.yml` | Format, lint, test, license gate |
| `.devcontainer/base.Dockerfile` | Shared image: Rust, Node, CLI tools |
| `.devcontainer/compose.yaml` | `root`, `broker`, `web` services |
| `.devcontainer/root/devcontainer.json` | Root orchestrator container |
| `services/web/.devcontainer/devcontainer.json` | Web container |
| `services/broker/Cargo.toml` | Workspace manifest, shared dependency versions |
| `services/broker/rust-toolchain.toml` | Pinned toolchain channel |
| `services/broker/.devcontainer/devcontainer.json` | Broker container |
| `services/broker/crates/mqtt-codec/Cargo.toml` | Codec manifest |
| `services/broker/crates/mqtt-codec/src/lib.rs` | Public surface, re-exports |
| `services/broker/crates/mqtt-codec/src/error.rs` | `CodecError`, reason-code mapping |
| `services/broker/crates/mqtt-codec/src/varint.rs` | Variable byte integers |
| `services/broker/crates/mqtt-codec/src/reader.rs` | Cursor over a frame body |
| `services/broker/crates/mqtt-codec/src/writer.rs` | Primitive encoders |
| `services/broker/crates/mqtt-codec/src/header.rs` | Fixed header, frame boundary |
| `services/broker/crates/mqtt-codec/src/topic.rs` | Topic name and filter validation |
| `services/broker/crates/mqtt-codec/src/properties.rs` | MQTT 5 properties |
| `services/broker/crates/mqtt-codec/src/packet/mod.rs` | `Packet` enum, top-level decode and encode |
| `services/broker/crates/mqtt-codec/src/packet/connect.rs` | CONNECT, CONNACK |
| `services/broker/crates/mqtt-codec/src/packet/publish.rs` | PUBLISH and the four acks |
| `services/broker/crates/mqtt-codec/src/packet/subscribe.rs` | SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK |
| `services/broker/crates/mqtt-codec/src/packet/control.rs` | PINGREQ, PINGRESP, DISCONNECT, AUTH |
| `services/broker/crates/mqtt-codec/tests/roundtrip.rs` | Property-based round trips |
| `services/broker/crates/mqtt-codec/tests/malformed.rs` | Corpus asserting exact errors |
| `services/broker/crates/mqtt-codec/fuzz/fuzz_targets/decode.rs` | Fuzz target |

Files split by packet family rather than by layer, so a change to PUBLISH touches one file. `reader.rs` and `writer.rs` are the only places that touch raw bytes.

---

## Task 1: Repository foundation

**Files:**
- Create: `LICENSE`, `README.md`, `.gitignore`, `services/broker/rust-toolchain.toml`

- [ ] **Step 1: Confirm the Rust toolchain is available**

Run:

```bash
rustc --version || curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
```

Expected: a version line such as `rustc 1.9x.x`. If rustup ran, open a new shell or run `source "$HOME/.cargo/env"` before continuing.

- [ ] **Step 2: Write the MIT license**

Create `LICENSE`:

```
MIT License

Copyright (c) 2026 Clement Chang

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
```

- [ ] **Step 3: Write the README**

Create `README.md`:

```markdown
# sd-mqtt

A distributed MQTT broker with a built-in admin UI and UI-configurable
integrations. One binary, no external dependencies.

Status: in development. See `docs/superpowers/specs/` for the design and
`docs/superpowers/plans/` for the implementation plans.

## Build

```bash
cd services/broker
cargo build --release
```

## Licensing

sd-mqtt is MIT licensed. It includes third-party components under other
permissive licenses; see `THIRD-PARTY-LICENSES.md` in a release artifact for
the full attribution.
```

- [ ] **Step 4: Write the ignore file**

Create `.gitignore`:

```
target/
node_modules/
dist/
*.profraw
.DS_Store
/data/
THIRD-PARTY-LICENSES.md
```

- [ ] **Step 5: Pin the toolchain**

Pin an exact version rather than `stable`. A floating channel lets a new
clippy lint break the build with no code change, and lets contributors
drift apart. Run `rustc --version` and use exactly what it reports; the
version below is what this plan was written against.

Create `services/broker/rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.98.1"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 6: Commit**

```bash
git add LICENSE README.md .gitignore services/broker/rust-toolchain.toml
git commit -m "chore: repository foundation"
```

---

## Task 2: Dev containers

**Files:**
- Create: `.devcontainer/base.Dockerfile`, `.devcontainer/compose.yaml`, `.devcontainer/root/devcontainer.json`, `services/broker/.devcontainer/devcontainer.json`

- [ ] **Step 1: Write the shared base image**

Create `.devcontainer/base.Dockerfile`:

```dockerfile
FROM rust:1.98.1-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl jq ripgrep fd-find ca-certificates \
    build-essential pkg-config clang \
    && rm -rf /var/lib/apt/lists/*

# Node for the admin UI workspace
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add rustfmt clippy \
    && cargo install cargo-nextest cargo-deny --locked

WORKDIR /workspaces/repo
```

- [ ] **Step 2: Write the compose file**

Create `.devcontainer/compose.yaml`:

```yaml
services:
  root:
    build:
      context: ..
      dockerfile: .devcontainer/base.Dockerfile
    volumes:
      - ..:/workspaces/repo:cached
      - cargo-registry:/usr/local/cargo/registry
    working_dir: /workspaces/repo
    command: sleep infinity

  broker:
    build:
      context: ..
      dockerfile: .devcontainer/base.Dockerfile
    volumes:
      - ..:/workspaces/repo:cached
      - cargo-registry:/usr/local/cargo/registry
    working_dir: /workspaces/repo/services/broker
    command: sleep infinity

  web:
    build:
      context: ..
      dockerfile: .devcontainer/base.Dockerfile
    volumes:
      - ..:/workspaces/repo:cached
    working_dir: /workspaces/repo/services/web
    command: sleep infinity

volumes:
  cargo-registry:
```

- [ ] **Step 3: Write the root devcontainer**

Create `.devcontainer/root/devcontainer.json`:

```json
{
  "name": "sd-mqtt (root)",
  "dockerComposeFile": ["../compose.yaml"],
  "service": "root",
  "runServices": ["root"],
  "workspaceFolder": "/workspaces/repo",
  "shutdownAction": "stopCompose"
}
```

- [ ] **Step 4: Write the broker devcontainer**

Create `services/broker/.devcontainer/devcontainer.json`:

```json
{
  "name": "sd-mqtt broker",
  "dockerComposeFile": ["../../../.devcontainer/compose.yaml"],
  "service": "broker",
  "runServices": ["broker"],
  "workspaceFolder": "/workspaces/repo/services/broker",
  "shutdownAction": "stopCompose"
}
```

- [ ] **Step 5: Write the web devcontainer**

Create `services/web/.devcontainer/devcontainer.json`:

```json
{
  "name": "sd-mqtt web",
  "dockerComposeFile": ["../../../.devcontainer/compose.yaml"],
  "service": "web",
  "runServices": ["web"],
  "workspaceFolder": "/workspaces/repo/services/web",
  "shutdownAction": "stopCompose"
}
```

- [ ] **Step 6: Reserve the web service directory**

The compose file names a `web` service whose working directory does not
exist until a later plan. Create a placeholder so the service can start.

```bash
mkdir -p services/web && touch services/web/.gitkeep
```

- [ ] **Step 7: Verify the compose file parses**

Run: `docker compose -f .devcontainer/compose.yaml config --quiet`
Expected: no output and exit code 0.

- [ ] **Step 8: Commit**

```bash
git add .devcontainer services/broker/.devcontainer services/web
git commit -m "chore: dev containers for root, broker and web"
```

---

## Task 3: Cargo workspace, license gate and CI

**Files:**
- Create: `services/broker/Cargo.toml`, `services/broker/crates/mqtt-codec/Cargo.toml`, `services/broker/crates/mqtt-codec/src/lib.rs`, `deny.toml`, `.github/workflows/ci.yml`

- [ ] **Step 1: Write the workspace manifest**

Two notes on what is deliberately absent. The release profile does **not**
set `panic = "abort"`. This broker runs one task per connection, and with
unwinding a panic in a connection handler stays contained to that task and
surfaces as an error on its join handle; with abort, one malformed packet
would take down every connected client. Aborting also breaks
`#[should_panic]` tests and `cargo test --release`. The safety lints live in
`[workspace.lints.rust]` rather than in each crate's source, so that every
crate added later inherits them by declaring `[lints] workspace = true`.

Create `services/broker/Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = ["crates/*"]

[workspace.package]
version = "0.1.0"
edition = "2021"
rust-version = "1.90"
license = "MIT"
repository = "https://github.com/clementchang/sd-mqtt"

[workspace.dependencies]
bytes = "1"
thiserror = "2"
proptest = "1"

[workspace.lints.rust]
unsafe_code = "forbid"
missing_docs = "warn"

[profile.release]
lto = true
codegen-units = 1
strip = true
```

- [ ] **Step 2: Write the codec manifest**

Create `services/broker/crates/mqtt-codec/Cargo.toml`:

```toml
[package]
name = "mqtt-codec"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
description = "MQTT 3.1.1 and 5.0 packet encoding and decoding"

[dependencies]
bytes.workspace = true
thiserror.workspace = true

[dev-dependencies]
proptest.workspace = true

[lints]
workspace = true
```

- [ ] **Step 3: Write a placeholder library root so the workspace builds**

Create `services/broker/crates/mqtt-codec/src/lib.rs`:

```rust
//! MQTT 3.1.1 and 5.0 packet encoding and decoding.
//!
//! This crate performs no I/O. It turns bytes into typed packets and back,
//! and reports every failure as a value rather than a panic.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// The MQTT protocol version a packet is encoded for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Version {
    /// MQTT 3.1.1, protocol level 4.
    V311,
    /// MQTT 5.0, protocol level 5.
    V5,
}

impl Version {
    /// The protocol level byte carried in CONNECT.
    pub fn level(self) -> u8 {
        match self {
            Version::V311 => 4,
            Version::V5 => 5,
        }
    }
}
```

- [ ] **Step 4: Verify the workspace builds**

Run: `cd services/broker && cargo build`
Expected: `Finished` with no errors.

- [ ] **Step 5: Write the license gate**

Create `deny.toml`:

```toml
[graph]
all-features = true

[licenses]
version = 2
allow = [
    "MIT",
    "Apache-2.0",
    "Apache-2.0 WITH LLVM-exception",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "Zlib",
    "Unicode-3.0",
    "CDLA-Permissive-2.0",
    "CC0-1.0",
    "Unlicense",
]
confidence-threshold = 0.8

[bans]
multiple-versions = "warn"
wildcards = "deny"

[advisories]
version = 2
yanked = "deny"

[sources]
unknown-registry = "deny"
unknown-git = "deny"
```

- [ ] **Step 6: Verify the license gate passes**

Run: `cd services/broker && cargo deny --manifest-path Cargo.toml --config ../../deny.toml check licenses bans sources`
Expected: `licenses ok`, `bans ok`, `sources ok`. If `cargo deny` is missing, run `cargo install cargo-deny --locked` first.

- [ ] **Step 7: Write the CI workflow**

Create `.github/workflows/ci.yml`:

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:

env:
  CARGO_TERM_COLOR: always

jobs:
  broker:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: services/broker
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: services/broker
      - name: Format
        run: cargo fmt --all --check
      - name: Lint
        run: cargo clippy --all-targets --all-features -- -D warnings
      - uses: taiki-e/install-action@nextest
      - name: Test
        run: cargo nextest run --all-features
      - name: Doc tests
        run: cargo test --doc --all-features

  licenses:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
      - uses: taiki-e/install-action@cargo-deny
      - name: License and advisory gate
        run: cargo deny --manifest-path services/broker/Cargo.toml --config deny.toml check
```

- [ ] **Step 8: Verify formatting and lints are clean locally**

Run: `cd services/broker && cargo fmt --all --check && cargo clippy --all-targets -- -D warnings`
Expected: no output from fmt, and clippy finishes with no warnings.

- [ ] **Step 9: Commit**

```bash
git add services/broker/Cargo.toml services/broker/crates deny.toml .github
git commit -m "chore: cargo workspace, license gate and CI"
```

---

## Task 4: Variable byte integers

MQTT encodes lengths in one to four bytes, seven bits each, with the high bit meaning "another byte follows". Decoding must report "need more bytes" distinctly from "malformed", because the decoder sees partial reads.

**Files:**
- Create: `services/broker/crates/mqtt-codec/src/error.rs`, `services/broker/crates/mqtt-codec/src/varint.rs`
- Modify: `services/broker/crates/mqtt-codec/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/src/varint.rs` with tests only:

```rust
//! Variable byte integers, MQTT's length encoding.

#[cfg(test)]
mod tests {
    use super::*;

    /// Boundary values from the protocol's encoding table.
    const CASES: &[(u32, &[u8])] = &[
        (0, &[0x00]),
        (127, &[0x7F]),
        (128, &[0x80, 0x01]),
        (16_383, &[0xFF, 0x7F]),
        (16_384, &[0x80, 0x80, 0x01]),
        (2_097_151, &[0xFF, 0xFF, 0x7F]),
        (2_097_152, &[0x80, 0x80, 0x80, 0x01]),
        (268_435_455, &[0xFF, 0xFF, 0xFF, 0x7F]),
    ];

    #[test]
    fn decodes_boundary_values() {
        for (value, bytes) in CASES {
            let got = decode(bytes).expect("well formed");
            assert_eq!(got, Some((*value, bytes.len())), "decoding {value}");
        }
    }

    #[test]
    fn encodes_boundary_values() {
        for (value, bytes) in CASES {
            let mut out = Vec::new();
            encode(*value, &mut out).expect("in range");
            assert_eq!(out.as_slice(), *bytes, "encoding {value}");
        }
    }

    #[test]
    fn reports_need_more_bytes_on_truncation() {
        assert_eq!(decode(&[]).expect("not malformed"), None);
        assert_eq!(decode(&[0x80]).expect("not malformed"), None);
        assert_eq!(decode(&[0x80, 0x80, 0x80]).expect("not malformed"), None);
    }

    #[test]
    fn rejects_a_fifth_continuation_byte() {
        let err = decode(&[0x80, 0x80, 0x80, 0x80]).expect_err("malformed");
        assert!(matches!(err, crate::error::CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_values_above_the_maximum() {
        let mut out = Vec::new();
        let err = encode(268_435_456, &mut out).expect_err("out of range");
        assert!(matches!(err, crate::error::CodecError::MalformedPacket(_)));
    }

    #[test]
    fn encoded_len_matches_encode() {
        for (value, bytes) in CASES {
            assert_eq!(encoded_len(*value), bytes.len());
        }
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec varint`
Expected: FAIL to compile, with errors naming `decode`, `encode`, `encoded_len` and `crate::error` as not found.

- [ ] **Step 3: Write the error type**

Create `services/broker/crates/mqtt-codec/src/error.rs`:

```rust
//! Errors produced while encoding or decoding.

use thiserror::Error;

/// Everything that can go wrong on the wire.
///
/// Every variant carries a static description so that logs can identify the
/// exact check that failed without allocating.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CodecError {
    /// The bytes do not form a valid packet.
    #[error("malformed packet: {0}")]
    MalformedPacket(&'static str),

    /// The packet is well formed but violates a protocol rule.
    #[error("protocol error: {0}")]
    ProtocolError(&'static str),

    /// The declared size exceeds the configured maximum.
    #[error("packet too large")]
    PacketTooLarge,

    /// The CONNECT packet named a protocol level this broker does not serve.
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    /// A string field was not valid UTF-8, or contained a null character.
    #[error("invalid UTF-8 string")]
    InvalidUtf8,

    /// A publish topic was empty, or contained a wildcard.
    #[error("invalid topic name")]
    InvalidTopicName,

    /// A subscription filter violated the wildcard placement rules.
    #[error("invalid topic filter")]
    InvalidTopicFilter,

    /// The topic had more levels than the configured maximum.
    #[error("topic too deep")]
    TopicTooDeep,
}
```

- [ ] **Step 4: Write the implementation**

Insert at the top of `services/broker/crates/mqtt-codec/src/varint.rs`, above the `#[cfg(test)]` module:

```rust
use crate::error::CodecError;

/// The largest value a variable byte integer can carry.
pub const MAX: u32 = 268_435_455;

/// Decode a variable byte integer from the front of `buf`.
///
/// Returns `Ok(None)` when `buf` is a prefix of a valid encoding and more
/// bytes are needed. Returns the value and the number of bytes consumed on
/// success.
pub fn decode(buf: &[u8]) -> Result<Option<(u32, usize)>, CodecError> {
    let mut value: u32 = 0;
    let mut multiplier: u32 = 1;

    for (index, &byte) in buf.iter().take(4).enumerate() {
        value += u32::from(byte & 0x7F) * multiplier;
        if byte & 0x80 == 0 {
            return Ok(Some((value, index + 1)));
        }
        multiplier *= 128;
    }

    if buf.len() >= 4 {
        Err(CodecError::MalformedPacket(
            "variable byte integer longer than four bytes",
        ))
    } else {
        Ok(None)
    }
}

/// Append the encoding of `value` to `out`.
pub fn encode(mut value: u32, out: &mut Vec<u8>) -> Result<(), CodecError> {
    if value > MAX {
        return Err(CodecError::MalformedPacket(
            "variable byte integer above the maximum",
        ));
    }
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return Ok(());
        }
    }
}

/// How many bytes `value` occupies once encoded.
///
/// Values above [`MAX`] report four, which is the largest legal width; the
/// caller is expected to reject them before encoding.
pub fn encoded_len(value: u32) -> usize {
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}
```

- [ ] **Step 5: Register the modules**

Add to `services/broker/crates/mqtt-codec/src/lib.rs`, directly below the `#![warn(missing_docs)]` line:

```rust
pub mod error;
pub mod varint;

pub use error::CodecError;
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd services/broker && cargo test -p mqtt-codec varint`
Expected: PASS, six tests.

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): variable byte integers"
```

---

## Task 5: The reader

A reader is a cursor over one frame body. Every accessor either returns a value or an error; none can panic or read past the end.

**Files:**
- Create: `services/broker/crates/mqtt-codec/src/reader.rs`
- Modify: `services/broker/crates/mqtt-codec/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/src/reader.rs` with tests only:

```rust
//! A bounds-checked cursor over a single frame body.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;

    #[test]
    fn reads_integers_in_network_order() {
        let mut r = Reader::new(&[0x01, 0x02, 0x03, 0x00, 0x00, 0x04, 0xD2]);
        assert_eq!(r.u8().unwrap(), 0x01);
        assert_eq!(r.u16().unwrap(), 0x0203);
        assert_eq!(r.u32().unwrap(), 1234);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn reads_a_utf8_string() {
        let mut r = Reader::new(&[0x00, 0x04, b'M', b'Q', b'T', b'T']);
        assert_eq!(r.string().unwrap(), "MQTT");
    }

    #[test]
    fn reads_an_empty_string() {
        let mut r = Reader::new(&[0x00, 0x00]);
        assert_eq!(r.string().unwrap(), "");
    }

    #[test]
    fn rejects_a_string_containing_a_null_character() {
        let mut r = Reader::new(&[0x00, 0x02, b'a', 0x00]);
        assert_eq!(r.string().unwrap_err(), CodecError::InvalidUtf8);
    }

    #[test]
    fn rejects_invalid_utf8() {
        let mut r = Reader::new(&[0x00, 0x02, 0xFF, 0xFE]);
        assert_eq!(r.string().unwrap_err(), CodecError::InvalidUtf8);
    }

    #[test]
    fn reads_binary_data() {
        let mut r = Reader::new(&[0x00, 0x03, 0xDE, 0xAD, 0xBE]);
        assert_eq!(&r.binary().unwrap()[..], &[0xDE, 0xAD, 0xBE]);
    }

    #[test]
    fn reports_truncation_as_malformed_not_a_panic() {
        assert!(Reader::new(&[]).u8().is_err());
        assert!(Reader::new(&[0x00]).u16().is_err());
        assert!(Reader::new(&[0x00, 0x00, 0x00]).u32().is_err());
        assert!(Reader::new(&[0x00, 0x05, b'a']).string().is_err());
        assert!(Reader::new(&[0x00, 0x05, b'a']).binary().is_err());
    }

    #[test]
    fn reads_a_varint_through_the_cursor() {
        let mut r = Reader::new(&[0x80, 0x01, 0xAA]);
        assert_eq!(r.varint().unwrap(), 128);
        assert_eq!(r.u8().unwrap(), 0xAA);
    }

    #[test]
    fn take_rest_consumes_everything_left() {
        let mut r = Reader::new(&[0x01, 0x02, 0x03]);
        assert_eq!(r.u8().unwrap(), 0x01);
        assert_eq!(&r.take_rest()[..], &[0x02, 0x03]);
        assert_eq!(r.remaining(), 0);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec reader`
Expected: FAIL to compile, with `Reader` not found.

- [ ] **Step 3: Write the implementation**

Insert at the top of `services/broker/crates/mqtt-codec/src/reader.rs`, above the `#[cfg(test)]` module:

```rust
use bytes::Bytes;

use crate::error::CodecError;
use crate::varint;

/// A cursor over a frame body that cannot read past its end.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// Wrap a frame body.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// How many bytes are left.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Whether every byte has been consumed.
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn need(&self, n: usize) -> Result<(), CodecError> {
        if self.remaining() < n {
            Err(CodecError::MalformedPacket("packet body ended early"))
        } else {
            Ok(())
        }
    }

    /// Read one byte.
    pub fn u8(&mut self) -> Result<u8, CodecError> {
        self.need(1)?;
        let value = self.buf[self.pos];
        self.pos += 1;
        Ok(value)
    }

    /// Read a two-byte big-endian integer.
    pub fn u16(&mut self) -> Result<u16, CodecError> {
        self.need(2)?;
        let value = u16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(value)
    }

    /// Read a four-byte big-endian integer.
    pub fn u32(&mut self) -> Result<u32, CodecError> {
        self.need(4)?;
        let value = u32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(value)
    }

    /// Read a variable byte integer.
    pub fn varint(&mut self) -> Result<u32, CodecError> {
        match varint::decode(&self.buf[self.pos..])? {
            Some((value, consumed)) => {
                self.pos += consumed;
                Ok(value)
            }
            None => Err(CodecError::MalformedPacket(
                "variable byte integer truncated",
            )),
        }
    }

    /// Read exactly `n` bytes.
    pub fn slice(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        self.need(n)?;
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    /// Read a length-prefixed UTF-8 string.
    ///
    /// The protocol forbids a null character inside such a string, so that
    /// check lives here rather than at every call site.
    pub fn string(&mut self) -> Result<String, CodecError> {
        let len = self.u16()? as usize;
        let raw = self.slice(len)?;
        let text = core::str::from_utf8(raw).map_err(|_| CodecError::InvalidUtf8)?;
        if text.contains('\0') {
            return Err(CodecError::InvalidUtf8);
        }
        Ok(text.to_owned())
    }

    /// Read a length-prefixed byte string.
    pub fn binary(&mut self) -> Result<Bytes, CodecError> {
        let len = self.u16()? as usize;
        Ok(Bytes::copy_from_slice(self.slice(len)?))
    }

    /// Consume and return everything that is left.
    pub fn take_rest(&mut self) -> Bytes {
        let out = Bytes::copy_from_slice(&self.buf[self.pos..]);
        self.pos = self.buf.len();
        out
    }
}
```

- [ ] **Step 4: Register the module**

Add to `services/broker/crates/mqtt-codec/src/lib.rs`, below `pub mod error;`:

```rust
pub mod reader;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd services/broker && cargo test -p mqtt-codec reader`
Expected: PASS, nine tests.

- [ ] **Step 6: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): bounds-checked frame reader"
```

---

## Task 6: The writer

**Files:**
- Create: `services/broker/crates/mqtt-codec/src/writer.rs`
- Modify: `services/broker/crates/mqtt-codec/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/src/writer.rs` with tests only:

```rust
//! Primitive encoders that append to a growable buffer.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_integers_in_network_order() {
        let mut out = Vec::new();
        put_u8(&mut out, 0x01);
        put_u16(&mut out, 0x0203);
        put_u32(&mut out, 1234);
        assert_eq!(out, vec![0x01, 0x02, 0x03, 0x00, 0x00, 0x04, 0xD2]);
    }

    #[test]
    fn writes_a_length_prefixed_string() {
        let mut out = Vec::new();
        put_string(&mut out, "MQTT").unwrap();
        assert_eq!(out, vec![0x00, 0x04, b'M', b'Q', b'T', b'T']);
    }

    #[test]
    fn writes_an_empty_string() {
        let mut out = Vec::new();
        put_string(&mut out, "").unwrap();
        assert_eq!(out, vec![0x00, 0x00]);
    }

    #[test]
    fn rejects_a_string_longer_than_the_length_prefix_allows() {
        let long = "a".repeat(65_536);
        let mut out = Vec::new();
        assert!(put_string(&mut out, &long).is_err());
    }

    #[test]
    fn writes_length_prefixed_binary() {
        let mut out = Vec::new();
        put_binary(&mut out, &[0xDE, 0xAD]).unwrap();
        assert_eq!(out, vec![0x00, 0x02, 0xDE, 0xAD]);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec writer`
Expected: FAIL to compile, with `put_u8` and friends not found.

- [ ] **Step 3: Write the implementation**

Insert at the top of `services/broker/crates/mqtt-codec/src/writer.rs`, above the `#[cfg(test)]` module:

```rust
use crate::error::CodecError;

/// Append one byte.
pub fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

/// Append a two-byte big-endian integer.
pub fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Append a four-byte big-endian integer.
pub fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Append a length-prefixed UTF-8 string.
pub fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), CodecError> {
    put_binary(out, value.as_bytes())
}

/// Append a length-prefixed byte string.
pub fn put_binary(out: &mut Vec<u8>, value: &[u8]) -> Result<(), CodecError> {
    let len = u16::try_from(value.len())
        .map_err(|_| CodecError::MalformedPacket("length-prefixed field above 65535 bytes"))?;
    put_u16(out, len);
    out.extend_from_slice(value);
    Ok(())
}
```

- [ ] **Step 4: Register the module**

Add to `services/broker/crates/mqtt-codec/src/lib.rs`, below `pub mod reader;`:

```rust
pub mod writer;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd services/broker && cargo test -p mqtt-codec writer`
Expected: PASS, five tests.

- [ ] **Step 6: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): primitive encoders"
```

---

## Task 7: Fixed header and frame boundary detection

This is the security-critical task. The decoder must decide whether a whole frame has arrived, and reject an oversized frame from its declared length alone, before any body buffer is reserved.

**Files:**
- Create: `services/broker/crates/mqtt-codec/src/header.rs`
- Modify: `services/broker/crates/mqtt-codec/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/src/header.rs` with tests only:

```rust
//! The two-part fixed header, and deciding when a whole frame has arrived.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;

    fn limits(max: u32) -> Limits {
        Limits {
            max_packet_size: max,
            ..Limits::default()
        }
    }

    #[test]
    fn reads_a_complete_small_frame() {
        // PINGREQ: type 12, no flags, zero-length body.
        let buf = [0xC0, 0x00];
        let header = peek(&buf, &limits(1024)).unwrap().expect("complete");
        assert_eq!(header.kind, PacketKind::PingReq);
        assert_eq!(header.flags, 0);
        assert_eq!(header.body_len, 0);
        assert_eq!(header.total_len(), 2);
    }

    #[test]
    fn reports_need_more_bytes_when_the_header_is_partial() {
        assert!(peek(&[], &limits(1024)).unwrap().is_none());
        assert!(peek(&[0xC0], &limits(1024)).unwrap().is_none());
        // Length prefix says 2 more bytes follow but none have arrived.
        assert!(peek(&[0x30, 0x02], &limits(1024)).unwrap().is_none());
    }

    #[test]
    fn rejects_an_oversized_frame_from_its_declared_length_alone() {
        // Declared body of 300 bytes against a 100-byte limit, with no body
        // present at all. The rejection must not wait for the body.
        let buf = [0x30, 0xAC, 0x02];
        let err = peek(&buf, &limits(100)).unwrap_err();
        assert_eq!(err, CodecError::PacketTooLarge);
    }

    #[test]
    fn rejects_the_one_unassigned_packet_type() {
        // Nibble 0 is the only unassigned value; 1 through 15 are all real
        // packet types, with 15 being AUTH.
        let err = peek(&[0x00, 0x00], &limits(1024)).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
        assert!(peek(&[0xF0, 0x00], &limits(1024)).unwrap().is_some());
    }

    #[test]
    fn rejects_wrong_reserved_flags() {
        // PUBREL, SUBSCRIBE, UNSUBSCRIBE must carry flags 0b0010.
        for first in [0x60u8, 0x80, 0xA0] {
            let err = peek(&[first, 0x00], &limits(1024)).unwrap_err();
            assert!(matches!(err, CodecError::MalformedPacket(_)), "{first:#04x}");
        }
        // CONNECT must carry flags 0b0000.
        let err = peek(&[0x11, 0x00], &limits(1024)).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn accepts_correct_reserved_flags() {
        assert!(peek(&[0x62, 0x00], &limits(1024)).unwrap().is_some());
        assert!(peek(&[0x82, 0x00], &limits(1024)).unwrap().is_some());
        assert!(peek(&[0xA2, 0x00], &limits(1024)).unwrap().is_some());
    }

    #[test]
    fn publish_flags_are_carried_through_not_rejected() {
        // PUBLISH with DUP set, QoS 2, retain set.
        let header = peek(&[0x3D, 0x00], &limits(1024)).unwrap().expect("complete");
        assert_eq!(header.kind, PacketKind::Publish);
        assert_eq!(header.flags, 0x0D);
    }

    #[test]
    fn total_len_accounts_for_a_multibyte_length_prefix() {
        // Body of 128 bytes needs a two-byte length prefix.
        let mut buf = vec![0x30, 0x80, 0x01];
        buf.extend(std::iter::repeat_n(0u8, 128));
        let header = peek(&buf, &limits(1024)).unwrap().expect("complete");
        assert_eq!(header.body_len, 128);
        assert_eq!(header.header_len, 3);
        assert_eq!(header.total_len(), 131);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec header`
Expected: FAIL to compile, with `Limits`, `peek`, `PacketKind` not found.

- [ ] **Step 3: Write the implementation**

Insert at the top of `services/broker/crates/mqtt-codec/src/header.rs`, above the `#[cfg(test)]` module:

```rust
use crate::error::CodecError;
use crate::varint;

/// Bounds the decoder enforces, supplied by configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest whole packet, header included, in bytes.
    pub max_packet_size: u32,
    /// Largest number of levels in a topic name or filter.
    pub max_topic_depth: u16,
    /// Largest client identifier length in bytes.
    pub max_client_id_len: u16,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_packet_size: 2_000_000,
            max_topic_depth: 16,
            max_client_id_len: 128,
        }
    }
}

/// The packet type carried in the high nibble of the first byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum PacketKind {
    Connect,
    ConnAck,
    Publish,
    PubAck,
    PubRec,
    PubRel,
    PubComp,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    PingReq,
    PingResp,
    Disconnect,
    Auth,
}

impl PacketKind {
    fn from_nibble(value: u8) -> Option<Self> {
        Some(match value {
            1 => Self::Connect,
            2 => Self::ConnAck,
            3 => Self::Publish,
            4 => Self::PubAck,
            5 => Self::PubRec,
            6 => Self::PubRel,
            7 => Self::PubComp,
            8 => Self::Subscribe,
            9 => Self::SubAck,
            10 => Self::Unsubscribe,
            11 => Self::UnsubAck,
            12 => Self::PingReq,
            13 => Self::PingResp,
            14 => Self::Disconnect,
            15 => Self::Auth,
            _ => return None,
        })
    }

    /// The high nibble this kind occupies.
    pub fn nibble(self) -> u8 {
        match self {
            Self::Connect => 1,
            Self::ConnAck => 2,
            Self::Publish => 3,
            Self::PubAck => 4,
            Self::PubRec => 5,
            Self::PubRel => 6,
            Self::PubComp => 7,
            Self::Subscribe => 8,
            Self::SubAck => 9,
            Self::Unsubscribe => 10,
            Self::UnsubAck => 11,
            Self::PingReq => 12,
            Self::PingResp => 13,
            Self::Disconnect => 14,
            Self::Auth => 15,
        }
    }

    /// The flags the low nibble must carry, or `None` when the packet defines
    /// its own flags. Only PUBLISH defines its own.
    fn required_flags(self) -> Option<u8> {
        match self {
            Self::Publish => None,
            Self::PubRel | Self::Subscribe | Self::Unsubscribe => Some(0b0010),
            _ => Some(0b0000),
        }
    }
}

/// A decoded fixed header, describing a frame that has fully arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedHeader {
    /// Which packet this is.
    pub kind: PacketKind,
    /// The low nibble of the first byte, meaningful only for PUBLISH.
    pub flags: u8,
    /// Bytes of fixed header, between one and five.
    pub header_len: usize,
    /// Bytes of body following the header.
    pub body_len: usize,
}

impl FixedHeader {
    /// Header plus body.
    pub fn total_len(&self) -> usize {
        self.header_len + self.body_len
    }
}

/// Inspect the front of `buf` without consuming it.
///
/// Returns `Ok(None)` when the frame has not fully arrived. Returns
/// [`CodecError::PacketTooLarge`] as soon as the declared length exceeds the
/// limit, which happens before the body is present, so an oversized frame
/// costs no memory.
pub fn peek(buf: &[u8], limits: &Limits) -> Result<Option<FixedHeader>, CodecError> {
    if buf.is_empty() {
        return Ok(None);
    }

    let first = buf[0];
    let kind = PacketKind::from_nibble(first >> 4)
        .ok_or(CodecError::MalformedPacket("unknown packet type"))?;
    let flags = first & 0x0F;

    if let Some(required) = kind.required_flags() {
        if flags != required {
            return Err(CodecError::MalformedPacket(
                "reserved flags in the fixed header are wrong",
            ));
        }
    }

    let Some((body_len, len_bytes)) = varint::decode(&buf[1..])? else {
        return Ok(None);
    };
    let header_len = 1 + len_bytes;

    // Check the declared size before deciding whether the body has arrived,
    // so an oversized declaration is rejected without waiting for bytes.
    let declared_total = u64::from(body_len) + header_len as u64;
    if declared_total > u64::from(limits.max_packet_size) {
        return Err(CodecError::PacketTooLarge);
    }

    if buf.len() < header_len + body_len as usize {
        return Ok(None);
    }

    Ok(Some(FixedHeader {
        kind,
        flags,
        header_len,
        body_len: body_len as usize,
    }))
}
```

- [ ] **Step 4: Register the module**

Add to `services/broker/crates/mqtt-codec/src/lib.rs`, below `pub mod error;`:

```rust
pub mod header;
```

And extend the re-exports at the bottom of the same file:

```rust
pub use header::{FixedHeader, Limits, PacketKind};
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd services/broker && cargo test -p mqtt-codec header`
Expected: PASS, eight tests.

- [ ] **Step 6: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): fixed header with early oversize rejection"
```

---
## Task 8: Topic name and filter validation

**Files:**
- Create: `services/broker/crates/mqtt-codec/src/topic.rs`
- Modify: `services/broker/crates/mqtt-codec/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/src/topic.rs` with tests only:

```rust
//! Validation of publish topic names and subscription filters.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;

    fn depth(n: u16) -> Limits {
        Limits {
            max_topic_depth: n,
            ..Limits::default()
        }
    }

    #[test]
    fn accepts_ordinary_topic_names() {
        for name in ["a", "a/b/c", "/", "sport/tennis/player1", " "] {
            assert!(validate_name(name, &depth(16)).is_ok(), "{name:?}");
        }
    }

    #[test]
    fn rejects_an_empty_topic_name() {
        assert_eq!(
            validate_name("", &depth(16)).unwrap_err(),
            CodecError::InvalidTopicName
        );
    }

    #[test]
    fn rejects_wildcards_in_a_topic_name() {
        for name in ["a/+", "a/#", "+", "#", "a/+/b"] {
            assert_eq!(
                validate_name(name, &depth(16)).unwrap_err(),
                CodecError::InvalidTopicName,
                "{name:?}"
            );
        }
    }

    #[test]
    fn rejects_a_topic_name_deeper_than_the_limit() {
        assert!(validate_name("a/b/c", &depth(3)).is_ok());
        assert_eq!(
            validate_name("a/b/c/d", &depth(3)).unwrap_err(),
            CodecError::TopicTooDeep
        );
    }

    #[test]
    fn accepts_well_formed_filters() {
        for filter in ["a", "a/b", "+", "#", "a/#", "a/+/b", "+/+/#", "/+"] {
            assert!(validate_filter(filter, &depth(16)).is_ok(), "{filter:?}");
        }
    }

    #[test]
    fn rejects_a_multi_level_wildcard_that_is_not_last() {
        for filter in ["#/a", "a/#/b"] {
            assert_eq!(
                validate_filter(filter, &depth(16)).unwrap_err(),
                CodecError::InvalidTopicFilter,
                "{filter:?}"
            );
        }
    }

    #[test]
    fn rejects_a_wildcard_sharing_a_level_with_other_characters() {
        for filter in ["a+", "+a", "a#", "#a", "sport+"] {
            assert_eq!(
                validate_filter(filter, &depth(16)).unwrap_err(),
                CodecError::InvalidTopicFilter,
                "{filter:?}"
            );
        }
    }

    #[test]
    fn rejects_an_empty_filter() {
        assert_eq!(
            validate_filter("", &depth(16)).unwrap_err(),
            CodecError::InvalidTopicFilter
        );
    }

    #[test]
    fn recognises_a_shared_subscription_and_splits_it() {
        let parsed = parse_shared("$share/group-a/sport/#").expect("valid shared filter");
        assert_eq!(parsed, Some(("group-a", "sport/#")));
        assert_eq!(parse_shared("sport/#").unwrap(), None);
    }

    #[test]
    fn rejects_a_shared_subscription_with_a_bad_group() {
        // Empty group, and a group containing a wildcard.
        assert!(parse_shared("$share//sport").is_err());
        assert!(parse_shared("$share/gr+up/sport").is_err());
        // Missing the filter after the group.
        assert!(parse_shared("$share/group").is_err());
        assert!(parse_shared("$share/group/").is_err());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec topic`
Expected: FAIL to compile, with `validate_name`, `validate_filter`, `parse_shared` not found.

- [ ] **Step 3: Write the implementation**

Insert at the top of `services/broker/crates/mqtt-codec/src/topic.rs`, above the `#[cfg(test)]` module:

```rust
use crate::error::CodecError;
use crate::header::Limits;

/// The prefix marking a shared subscription.
pub const SHARE_PREFIX: &str = "$share/";

fn level_count(topic: &str) -> usize {
    topic.split('/').count()
}

/// Validate a topic name as used in PUBLISH.
///
/// A topic name must be non-empty and must contain no wildcard character.
pub fn validate_name(name: &str, limits: &Limits) -> Result<(), CodecError> {
    if name.is_empty() {
        return Err(CodecError::InvalidTopicName);
    }
    if name.contains('+') || name.contains('#') {
        return Err(CodecError::InvalidTopicName);
    }
    if level_count(name) > usize::from(limits.max_topic_depth) {
        return Err(CodecError::TopicTooDeep);
    }
    Ok(())
}

/// Validate a subscription filter.
///
/// A single-level wildcard must occupy an entire level. A multi-level
/// wildcard must occupy an entire level and be the final one.
pub fn validate_filter(filter: &str, limits: &Limits) -> Result<(), CodecError> {
    if filter.is_empty() {
        return Err(CodecError::InvalidTopicFilter);
    }

    let levels: Vec<&str> = filter.split('/').collect();
    if levels.len() > usize::from(limits.max_topic_depth) {
        return Err(CodecError::TopicTooDeep);
    }

    let last = levels.len() - 1;
    for (index, level) in levels.iter().enumerate() {
        let has_plus = level.contains('+');
        let has_hash = level.contains('#');

        if has_plus && *level != "+" {
            return Err(CodecError::InvalidTopicFilter);
        }
        if has_hash {
            if *level != "#" {
                return Err(CodecError::InvalidTopicFilter);
            }
            if index != last {
                return Err(CodecError::InvalidTopicFilter);
            }
        }
    }

    Ok(())
}

/// Split a shared subscription filter into its group and inner filter.
///
/// Returns `Ok(None)` for an ordinary filter. The group must be non-empty
/// and must contain no wildcard or separator, and an inner filter must
/// follow it.
pub fn parse_shared(filter: &str) -> Result<Option<(&str, &str)>, CodecError> {
    let Some(rest) = filter.strip_prefix(SHARE_PREFIX) else {
        return Ok(None);
    };

    let Some((group, inner)) = rest.split_once('/') else {
        return Err(CodecError::InvalidTopicFilter);
    };

    if group.is_empty() || group.contains('+') || group.contains('#') {
        return Err(CodecError::InvalidTopicFilter);
    }
    if inner.is_empty() {
        return Err(CodecError::InvalidTopicFilter);
    }

    Ok(Some((group, inner)))
}
```

- [ ] **Step 4: Register the module**

Add to `services/broker/crates/mqtt-codec/src/lib.rs`, below `pub mod reader;`:

```rust
pub mod topic;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd services/broker && cargo test -p mqtt-codec topic`
Expected: PASS, ten tests.

- [ ] **Step 6: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): topic name and filter validation"
```

---

## Task 9: Reason codes

One enum spans the whole system. MQTT 5 carries these values directly; MQTT 3.1.1 has a narrower set in CONNACK and SUBACK, so the enum knows how to narrow itself.

**Files:**
- Create: `services/broker/crates/mqtt-codec/src/reason.rs`
- Modify: `services/broker/crates/mqtt-codec/src/lib.rs`, `services/broker/crates/mqtt-codec/src/error.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/src/reason.rs` with tests only:

```rust
//! Reason codes, shared by every acknowledgement path.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;

    #[test]
    fn round_trips_through_its_byte_value() {
        for code in [
            ReasonCode::Success,
            ReasonCode::GrantedQos1,
            ReasonCode::GrantedQos2,
            ReasonCode::NoMatchingSubscribers,
            ReasonCode::ContinueAuthentication,
            ReasonCode::UnspecifiedError,
            ReasonCode::MalformedPacket,
            ReasonCode::BadAuthenticationMethod,
            ReasonCode::SessionTakenOver,
            ReasonCode::PacketTooLarge,
            ReasonCode::QuotaExceeded,
            ReasonCode::SharedSubscriptionsNotSupported,
        ] {
            assert_eq!(ReasonCode::from_byte(code as u8), Some(code));
        }
    }

    #[test]
    fn rejects_an_unassigned_byte() {
        assert_eq!(ReasonCode::from_byte(0x7F), None);
    }

    #[test]
    fn knows_which_codes_are_failures() {
        assert!(!ReasonCode::Success.is_failure());
        assert!(!ReasonCode::GrantedQos2.is_failure());
        assert!(ReasonCode::NotAuthorized.is_failure());
        assert!(ReasonCode::MalformedPacket.is_failure());
    }

    #[test]
    fn narrows_to_the_legacy_connack_return_codes() {
        assert_eq!(ReasonCode::Success.to_v311_connack(), 0);
        assert_eq!(ReasonCode::UnsupportedProtocolVersion.to_v311_connack(), 1);
        assert_eq!(ReasonCode::ClientIdentifierNotValid.to_v311_connack(), 2);
        assert_eq!(ReasonCode::ServerUnavailable.to_v311_connack(), 3);
        assert_eq!(ReasonCode::BadUserNameOrPassword.to_v311_connack(), 4);
        assert_eq!(ReasonCode::NotAuthorized.to_v311_connack(), 5);
        // Anything with no legacy equivalent reports the broadest failure.
        assert_eq!(ReasonCode::QuotaExceeded.to_v311_connack(), 3);
    }

    #[test]
    fn narrows_to_the_legacy_suback_codes() {
        assert_eq!(ReasonCode::Success.to_v311_suback(), 0x00);
        assert_eq!(ReasonCode::GrantedQos1.to_v311_suback(), 0x01);
        assert_eq!(ReasonCode::GrantedQos2.to_v311_suback(), 0x02);
        assert_eq!(ReasonCode::NotAuthorized.to_v311_suback(), 0x80);
        assert_eq!(ReasonCode::TopicFilterInvalid.to_v311_suback(), 0x80);
    }

    #[test]
    fn maps_every_codec_error_to_a_reason_code() {
        assert_eq!(
            ReasonCode::from(CodecError::MalformedPacket("x")),
            ReasonCode::MalformedPacket
        );
        assert_eq!(
            ReasonCode::from(CodecError::ProtocolError("x")),
            ReasonCode::ProtocolError
        );
        assert_eq!(
            ReasonCode::from(CodecError::PacketTooLarge),
            ReasonCode::PacketTooLarge
        );
        assert_eq!(
            ReasonCode::from(CodecError::UnsupportedVersion(3)),
            ReasonCode::UnsupportedProtocolVersion
        );
        assert_eq!(
            ReasonCode::from(CodecError::InvalidUtf8),
            ReasonCode::MalformedPacket
        );
        assert_eq!(
            ReasonCode::from(CodecError::InvalidTopicName),
            ReasonCode::TopicNameInvalid
        );
        assert_eq!(
            ReasonCode::from(CodecError::InvalidTopicFilter),
            ReasonCode::TopicFilterInvalid
        );
        assert_eq!(
            ReasonCode::from(CodecError::TopicTooDeep),
            ReasonCode::TopicNameInvalid
        );
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec reason`
Expected: FAIL to compile, with `ReasonCode` not found.

- [ ] **Step 3: Write the implementation**

Insert at the top of `services/broker/crates/mqtt-codec/src/reason.rs`, above the `#[cfg(test)]` module:

```rust
use crate::error::CodecError;

/// A reason code, as carried by MQTT 5 acknowledgements.
///
/// The discriminant is the byte that appears on the wire. MQTT 3.1.1 carries
/// a narrower set, so [`ReasonCode::to_v311_connack`] and
/// [`ReasonCode::to_v311_suback`] project onto the legacy values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[allow(missing_docs)]
pub enum ReasonCode {
    /// Also granted QoS 0, and normal disconnection.
    Success = 0x00,
    GrantedQos1 = 0x01,
    GrantedQos2 = 0x02,
    DisconnectWithWillMessage = 0x04,
    NoMatchingSubscribers = 0x10,
    NoSubscriptionExisted = 0x11,
    ContinueAuthentication = 0x18,
    ReAuthenticate = 0x19,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    ClientIdentifierNotValid = 0x85,
    BadUserNameOrPassword = 0x86,
    NotAuthorized = 0x87,
    ServerUnavailable = 0x88,
    ServerBusy = 0x89,
    Banned = 0x8A,
    ServerShuttingDown = 0x8B,
    BadAuthenticationMethod = 0x8C,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    PacketIdentifierInUse = 0x91,
    PacketIdentifierNotFound = 0x92,
    ReceiveMaximumExceeded = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    MessageRateTooHigh = 0x96,
    QuotaExceeded = 0x97,
    AdministrativeAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QosNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    SharedSubscriptionsNotSupported = 0x9E,
    ConnectionRateExceeded = 0x9F,
    MaximumConnectTime = 0xA0,
    SubscriptionIdentifiersNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl ReasonCode {
    /// Interpret a wire byte, rejecting unassigned values.
    pub fn from_byte(value: u8) -> Option<Self> {
        use ReasonCode::*;
        Some(match value {
            0x00 => Success,
            0x01 => GrantedQos1,
            0x02 => GrantedQos2,
            0x04 => DisconnectWithWillMessage,
            0x10 => NoMatchingSubscribers,
            0x11 => NoSubscriptionExisted,
            0x18 => ContinueAuthentication,
            0x19 => ReAuthenticate,
            0x80 => UnspecifiedError,
            0x81 => MalformedPacket,
            0x82 => ProtocolError,
            0x83 => ImplementationSpecificError,
            0x84 => UnsupportedProtocolVersion,
            0x85 => ClientIdentifierNotValid,
            0x86 => BadUserNameOrPassword,
            0x87 => NotAuthorized,
            0x88 => ServerUnavailable,
            0x89 => ServerBusy,
            0x8A => Banned,
            0x8B => ServerShuttingDown,
            0x8C => BadAuthenticationMethod,
            0x8D => KeepAliveTimeout,
            0x8E => SessionTakenOver,
            0x8F => TopicFilterInvalid,
            0x90 => TopicNameInvalid,
            0x91 => PacketIdentifierInUse,
            0x92 => PacketIdentifierNotFound,
            0x93 => ReceiveMaximumExceeded,
            0x94 => TopicAliasInvalid,
            0x95 => PacketTooLarge,
            0x96 => MessageRateTooHigh,
            0x97 => QuotaExceeded,
            0x98 => AdministrativeAction,
            0x99 => PayloadFormatInvalid,
            0x9A => RetainNotSupported,
            0x9B => QosNotSupported,
            0x9C => UseAnotherServer,
            0x9D => ServerMoved,
            0x9E => SharedSubscriptionsNotSupported,
            0x9F => ConnectionRateExceeded,
            0xA0 => MaximumConnectTime,
            0xA1 => SubscriptionIdentifiersNotSupported,
            0xA2 => WildcardSubscriptionsNotSupported,
            _ => return None,
        })
    }

    /// Whether this code denotes a failure. Every value at or above 0x80 does.
    pub fn is_failure(self) -> bool {
        (self as u8) >= 0x80
    }

    /// Project onto the MQTT 3.1.1 CONNACK return codes.
    ///
    /// Codes with no legacy equivalent become "server unavailable", which is
    /// the broadest failure a 3.1.1 client understands.
    pub fn to_v311_connack(self) -> u8 {
        use ReasonCode::*;
        match self {
            Success => 0,
            UnsupportedProtocolVersion => 1,
            ClientIdentifierNotValid => 2,
            ServerUnavailable => 3,
            BadUserNameOrPassword => 4,
            NotAuthorized | Banned => 5,
            _ => 3,
        }
    }

    /// Project onto the MQTT 3.1.1 SUBACK return codes.
    pub fn to_v311_suback(self) -> u8 {
        use ReasonCode::*;
        match self {
            Success => 0x00,
            GrantedQos1 => 0x01,
            GrantedQos2 => 0x02,
            _ => 0x80,
        }
    }
}

impl From<CodecError> for ReasonCode {
    fn from(error: CodecError) -> Self {
        match error {
            CodecError::MalformedPacket(_) | CodecError::InvalidUtf8 => ReasonCode::MalformedPacket,
            CodecError::ProtocolError(_) => ReasonCode::ProtocolError,
            CodecError::PacketTooLarge => ReasonCode::PacketTooLarge,
            CodecError::UnsupportedVersion(_) => ReasonCode::UnsupportedProtocolVersion,
            CodecError::InvalidTopicName | CodecError::TopicTooDeep => ReasonCode::TopicNameInvalid,
            CodecError::InvalidTopicFilter => ReasonCode::TopicFilterInvalid,
        }
    }
}
```

- [ ] **Step 4: Register the module**

Add to `services/broker/crates/mqtt-codec/src/lib.rs`, below `pub mod reader;`:

```rust
pub mod reason;
```

And extend the re-exports:

```rust
pub use reason::ReasonCode;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd services/broker && cargo test -p mqtt-codec reason`
Expected: PASS, six tests.

- [ ] **Step 6: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): reason codes with legacy projections"
```

---

## Task 10: MQTT 5 properties

Properties are a length-prefixed sequence of identifier-and-value pairs. Most may appear at most once; a second occurrence is a protocol error. User properties and subscription identifiers may repeat.

**Files:**
- Create: `services/broker/crates/mqtt-codec/src/properties.rs`
- Modify: `services/broker/crates/mqtt-codec/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/src/properties.rs` with tests only:

```rust
//! MQTT 5 properties.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;
    use crate::reader::Reader;
    use bytes::Bytes;

    fn round_trip(props: &Properties) -> Properties {
        let mut out = Vec::new();
        props.encode(&mut out).expect("encodes");
        let mut reader = Reader::new(&out);
        Properties::decode(&mut reader).expect("decodes")
    }

    #[test]
    fn an_empty_set_encodes_as_a_single_zero_byte() {
        let mut out = Vec::new();
        Properties::default().encode(&mut out).unwrap();
        assert_eq!(out, vec![0x00]);
    }

    #[test]
    fn round_trips_every_scalar_kind() {
        let props = Properties {
            payload_format_indicator: Some(1),
            message_expiry_interval: Some(3600),
            server_keep_alive: Some(60),
            content_type: Some("application/json".into()),
            correlation_data: Some(Bytes::from_static(b"abc")),
            subscription_identifiers: vec![1, 268_435_455],
            user_properties: vec![("a".into(), "1".into()), ("b".into(), "2".into())],
            ..Properties::default()
        };
        assert_eq!(round_trip(&props), props);
    }

    #[test]
    fn preserves_the_order_of_repeated_properties() {
        let props = Properties {
            user_properties: vec![
                ("k".into(), "first".into()),
                ("k".into(), "second".into()),
            ],
            ..Properties::default()
        };
        assert_eq!(round_trip(&props).user_properties, props.user_properties);
    }

    #[test]
    fn rejects_a_duplicated_single_valued_property() {
        // Length 4, then session expiry (0x11) twice with truncated values.
        let bytes = [0x0A, 0x11, 0, 0, 0, 1, 0x11, 0, 0, 0, 2];
        let mut reader = Reader::new(&bytes);
        let err = Properties::decode(&mut reader).unwrap_err();
        assert!(matches!(err, CodecError::ProtocolError(_)));
    }

    #[test]
    fn rejects_an_unknown_property_identifier() {
        let bytes = [0x02, 0x7E, 0x00];
        let mut reader = Reader::new(&bytes);
        let err = Properties::decode(&mut reader).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_a_property_block_that_overruns_its_declared_length() {
        // Declares 10 bytes of properties but supplies 2.
        let bytes = [0x0A, 0x01, 0x01];
        let mut reader = Reader::new(&bytes);
        assert!(Properties::decode(&mut reader).is_err());
    }

    #[test]
    fn rejects_a_zero_subscription_identifier() {
        let bytes = [0x02, 0x0B, 0x00];
        let mut reader = Reader::new(&bytes);
        let err = Properties::decode(&mut reader).unwrap_err();
        assert!(matches!(err, CodecError::ProtocolError(_)));
    }

    #[test]
    fn stops_at_the_end_of_the_property_block() {
        // One property, then a trailing byte belonging to the packet body.
        let bytes = [0x02, 0x01, 0x01, 0xAA];
        let mut reader = Reader::new(&bytes);
        let props = Properties::decode(&mut reader).unwrap();
        assert_eq!(props.payload_format_indicator, Some(1));
        assert_eq!(reader.u8().unwrap(), 0xAA);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec properties`
Expected: FAIL to compile, with `Properties` not found.

- [ ] **Step 3: Write the implementation**

Insert at the top of `services/broker/crates/mqtt-codec/src/properties.rs`, above the `#[cfg(test)]` module:

```rust
use bytes::Bytes;

use crate::error::CodecError;
use crate::reader::Reader;
use crate::varint;
use crate::writer::{put_binary, put_string, put_u16, put_u32, put_u8};

/// Every MQTT 5 property, gathered into one structure.
///
/// A given packet type permits only a subset. Enforcing which subset belongs
/// to which packet is the job of the packet decoders, not of this type.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct Properties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Bytes>,
    pub subscription_identifiers: Vec<u32>,
    pub session_expiry_interval: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub server_keep_alive: Option<u16>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Bytes>,
    pub request_problem_information: Option<u8>,
    pub will_delay_interval: Option<u32>,
    pub request_response_information: Option<u8>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub reason_string: Option<String>,
    pub receive_maximum: Option<u16>,
    pub topic_alias_maximum: Option<u16>,
    pub topic_alias: Option<u16>,
    pub maximum_qos: Option<u8>,
    pub retain_available: Option<u8>,
    pub user_properties: Vec<(String, String)>,
    pub maximum_packet_size: Option<u32>,
    pub wildcard_subscription_available: Option<u8>,
    pub subscription_identifier_available: Option<u8>,
    pub shared_subscription_available: Option<u8>,
}

/// Reject a second occurrence of a property that may appear only once.
fn once<T>(slot: &mut Option<T>, value: T) -> Result<(), CodecError> {
    if slot.is_some() {
        return Err(CodecError::ProtocolError(
            "property appears more than once",
        ));
    }
    *slot = Some(value);
    Ok(())
}

impl Properties {
    /// Whether nothing is set, in which case the block is a single zero byte.
    pub fn is_empty(&self) -> bool {
        *self == Properties::default()
    }

    /// Decode a property block from the current position.
    ///
    /// Reads the block length, then exactly that many bytes, leaving the
    /// reader positioned at the first byte after the block.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let len = reader.varint()? as usize;
        let body = reader.slice(len)?;
        let mut inner = Reader::new(body);
        let mut props = Properties::default();

        while !inner.is_empty() {
            let id = inner.varint()?;
            match id {
                0x01 => once(&mut props.payload_format_indicator, inner.u8()?)?,
                0x02 => once(&mut props.message_expiry_interval, inner.u32()?)?,
                0x03 => once(&mut props.content_type, inner.string()?)?,
                0x08 => once(&mut props.response_topic, inner.string()?)?,
                0x09 => once(&mut props.correlation_data, inner.binary()?)?,
                0x0B => {
                    let value = inner.varint()?;
                    if value == 0 {
                        return Err(CodecError::ProtocolError(
                            "subscription identifier must not be zero",
                        ));
                    }
                    props.subscription_identifiers.push(value);
                }
                0x11 => once(&mut props.session_expiry_interval, inner.u32()?)?,
                0x12 => once(&mut props.assigned_client_identifier, inner.string()?)?,
                0x13 => once(&mut props.server_keep_alive, inner.u16()?)?,
                0x15 => once(&mut props.authentication_method, inner.string()?)?,
                0x16 => once(&mut props.authentication_data, inner.binary()?)?,
                0x17 => once(&mut props.request_problem_information, inner.u8()?)?,
                0x18 => once(&mut props.will_delay_interval, inner.u32()?)?,
                0x19 => once(&mut props.request_response_information, inner.u8()?)?,
                0x1A => once(&mut props.response_information, inner.string()?)?,
                0x1C => once(&mut props.server_reference, inner.string()?)?,
                0x1F => once(&mut props.reason_string, inner.string()?)?,
                0x21 => once(&mut props.receive_maximum, inner.u16()?)?,
                0x22 => once(&mut props.topic_alias_maximum, inner.u16()?)?,
                0x23 => once(&mut props.topic_alias, inner.u16()?)?,
                0x24 => once(&mut props.maximum_qos, inner.u8()?)?,
                0x25 => once(&mut props.retain_available, inner.u8()?)?,
                0x26 => {
                    let key = inner.string()?;
                    let value = inner.string()?;
                    props.user_properties.push((key, value));
                }
                0x27 => once(&mut props.maximum_packet_size, inner.u32()?)?,
                0x28 => once(&mut props.wildcard_subscription_available, inner.u8()?)?,
                0x29 => once(&mut props.subscription_identifier_available, inner.u8()?)?,
                0x2A => once(&mut props.shared_subscription_available, inner.u8()?)?,
                _ => {
                    return Err(CodecError::MalformedPacket(
                        "unknown property identifier",
                    ))
                }
            }
        }

        Ok(props)
    }

    /// Append the property block, including its length prefix.
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        let mut body = Vec::new();

        if let Some(v) = self.payload_format_indicator {
            body.push(0x01);
            put_u8(&mut body, v);
        }
        if let Some(v) = self.message_expiry_interval {
            body.push(0x02);
            put_u32(&mut body, v);
        }
        if let Some(v) = &self.content_type {
            body.push(0x03);
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.response_topic {
            body.push(0x08);
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.correlation_data {
            body.push(0x09);
            put_binary(&mut body, v)?;
        }
        for v in &self.subscription_identifiers {
            body.push(0x0B);
            varint::encode(*v, &mut body)?;
        }
        if let Some(v) = self.session_expiry_interval {
            body.push(0x11);
            put_u32(&mut body, v);
        }
        if let Some(v) = &self.assigned_client_identifier {
            body.push(0x12);
            put_string(&mut body, v)?;
        }
        if let Some(v) = self.server_keep_alive {
            body.push(0x13);
            put_u16(&mut body, v);
        }
        if let Some(v) = &self.authentication_method {
            body.push(0x15);
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.authentication_data {
            body.push(0x16);
            put_binary(&mut body, v)?;
        }
        if let Some(v) = self.request_problem_information {
            body.push(0x17);
            put_u8(&mut body, v);
        }
        if let Some(v) = self.will_delay_interval {
            body.push(0x18);
            put_u32(&mut body, v);
        }
        if let Some(v) = self.request_response_information {
            body.push(0x19);
            put_u8(&mut body, v);
        }
        if let Some(v) = &self.response_information {
            body.push(0x1A);
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.server_reference {
            body.push(0x1C);
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.reason_string {
            body.push(0x1F);
            put_string(&mut body, v)?;
        }
        if let Some(v) = self.receive_maximum {
            body.push(0x21);
            put_u16(&mut body, v);
        }
        if let Some(v) = self.topic_alias_maximum {
            body.push(0x22);
            put_u16(&mut body, v);
        }
        if let Some(v) = self.topic_alias {
            body.push(0x23);
            put_u16(&mut body, v);
        }
        if let Some(v) = self.maximum_qos {
            body.push(0x24);
            put_u8(&mut body, v);
        }
        if let Some(v) = self.retain_available {
            body.push(0x25);
            put_u8(&mut body, v);
        }
        for (key, value) in &self.user_properties {
            body.push(0x26);
            put_string(&mut body, key)?;
            put_string(&mut body, value)?;
        }
        if let Some(v) = self.maximum_packet_size {
            body.push(0x27);
            put_u32(&mut body, v);
        }
        if let Some(v) = self.wildcard_subscription_available {
            body.push(0x28);
            put_u8(&mut body, v);
        }
        if let Some(v) = self.subscription_identifier_available {
            body.push(0x29);
            put_u8(&mut body, v);
        }
        if let Some(v) = self.shared_subscription_available {
            body.push(0x2A);
            put_u8(&mut body, v);
        }

        let len = u32::try_from(body.len())
            .map_err(|_| CodecError::MalformedPacket("property block too large"))?;
        varint::encode(len, out)?;
        out.extend_from_slice(&body);
        Ok(())
    }
}
```

- [ ] **Step 4: Register the module**

Add to `services/broker/crates/mqtt-codec/src/lib.rs`, below `pub mod reason;`:

```rust
pub mod properties;
```

And extend the re-exports:

```rust
pub use properties::Properties;
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd services/broker && cargo test -p mqtt-codec properties`
Expected: PASS, eight tests.

- [ ] **Step 6: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): MQTT 5 properties"
```

---
## Task 11: QoS, CONNECT and CONNACK

The codec enforces structural rules only: the protocol name, the protocol
level, reserved bits, field presence and length limits. Semantic rules such
as "an empty client identifier requires a clean start" belong to the session
layer and are not checked here.

**Files:**
- Create: `services/broker/crates/mqtt-codec/src/qos.rs`, `services/broker/crates/mqtt-codec/src/packet/mod.rs`, `services/broker/crates/mqtt-codec/src/packet/connect.rs`
- Modify: `services/broker/crates/mqtt-codec/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/src/packet/connect.rs` with tests only:

```rust
//! CONNECT and CONNACK.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;
    use crate::header::Limits;
    use crate::reader::Reader;
    use crate::reason::ReasonCode;
    use crate::qos::QoS;
    use crate::Version;
    use bytes::Bytes;

    fn round_trip_connect(packet: &Connect, version: Version) -> Connect {
        let mut out = Vec::new();
        packet.encode_body(version, &mut out).expect("encodes");
        let mut reader = Reader::new(&out);
        Connect::decode_body(&mut reader, &Limits::default()).expect("decodes")
    }

    fn minimal(version: Version) -> Connect {
        Connect {
            version,
            client_id: "client-1".into(),
            clean_start: true,
            keep_alive: 60,
            username: None,
            password: None,
            will: None,
            properties: Default::default(),
        }
    }

    #[test]
    fn round_trips_a_minimal_v311_connect() {
        let packet = minimal(Version::V311);
        assert_eq!(round_trip_connect(&packet, Version::V311), packet);
    }

    #[test]
    fn round_trips_a_v5_connect_with_credentials_and_a_will() {
        let packet = Connect {
            version: Version::V5,
            client_id: "client-2".into(),
            clean_start: false,
            keep_alive: 30,
            username: Some("alice".into()),
            password: Some(Bytes::from_static(b"secret")),
            will: Some(Will {
                topic: "status/client-2".into(),
                payload: Bytes::from_static(b"offline"),
                qos: QoS::AtLeastOnce,
                retain: true,
                properties: crate::Properties {
                    will_delay_interval: Some(10),
                    ..Default::default()
                },
            }),
            properties: crate::Properties {
                session_expiry_interval: Some(300),
                receive_maximum: Some(32),
                ..Default::default()
            },
        };
        assert_eq!(round_trip_connect(&packet, Version::V5), packet);
    }

    #[test]
    fn rejects_a_wrong_protocol_name() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "MQIsdp").unwrap();
        out.push(4);
        out.push(0x02);
        crate::writer::put_u16(&mut out, 60);
        crate::writer::put_string(&mut out, "c").unwrap();
        let mut reader = Reader::new(&out);
        let err = Connect::decode_body(&mut reader, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_an_unsupported_protocol_level() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "MQTT").unwrap();
        out.push(3);
        out.push(0x02);
        crate::writer::put_u16(&mut out, 60);
        crate::writer::put_string(&mut out, "c").unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(
            Connect::decode_body(&mut reader, &Limits::default()).unwrap_err(),
            CodecError::UnsupportedVersion(3)
        );
    }

    #[test]
    fn rejects_a_set_reserved_flag_bit() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "MQTT").unwrap();
        out.push(4);
        out.push(0x03); // reserved bit 0 set
        crate::writer::put_u16(&mut out, 60);
        crate::writer::put_string(&mut out, "c").unwrap();
        let mut reader = Reader::new(&out);
        let err = Connect::decode_body(&mut reader, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_will_qos_three() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "MQTT").unwrap();
        out.push(4);
        out.push(0x02 | 0x04 | 0x18); // clean, will flag, will QoS 3
        crate::writer::put_u16(&mut out, 60);
        crate::writer::put_string(&mut out, "c").unwrap();
        let mut reader = Reader::new(&out);
        let err = Connect::decode_body(&mut reader, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_a_client_identifier_longer_than_the_limit() {
        let limits = Limits {
            max_client_id_len: 4,
            ..Limits::default()
        };
        let packet = Connect {
            client_id: "toolong".into(),
            ..minimal(Version::V311)
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        let err = Connect::decode_body(&mut reader, &limits).unwrap_err();
        assert!(matches!(err, CodecError::ProtocolError(_)));
    }

    #[test]
    fn round_trips_connack_in_both_versions() {
        for (version, reason) in [
            (Version::V311, ReasonCode::Success),
            (Version::V311, ReasonCode::NotAuthorized),
            (Version::V5, ReasonCode::Success),
            (Version::V5, ReasonCode::BadAuthenticationMethod),
        ] {
            let packet = ConnAck {
                session_present: reason == ReasonCode::Success,
                reason,
                properties: Default::default(),
            };
            let mut out = Vec::new();
            packet.encode_body(version, &mut out).unwrap();
            let mut reader = Reader::new(&out);
            let decoded = ConnAck::decode_body(&mut reader, version).unwrap();
            assert_eq!(decoded.session_present, packet.session_present);
            match version {
                Version::V5 => assert_eq!(decoded.reason, reason),
                // The legacy projection is lossy, so only success survives
                // exactly; failures widen to a legacy code.
                Version::V311 => assert_eq!(decoded.reason.is_failure(), reason.is_failure()),
            }
        }
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec connect`
Expected: FAIL to compile, with `Connect`, `ConnAck`, `Will`, `QoS` not found.

- [ ] **Step 3: Write the quality-of-service type**

Create `services/broker/crates/mqtt-codec/src/qos.rs`:

```rust
//! Quality of service levels.

use crate::error::CodecError;

/// How hard the broker works to deliver a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum QoS {
    /// Fire and forget.
    #[default]
    AtMostOnce,
    /// Acknowledged, may be delivered more than once.
    AtLeastOnce,
    /// Two-phase, delivered exactly once.
    ExactlyOnce,
}

impl QoS {
    /// Interpret the two-bit wire encoding. Three is not a level.
    pub fn from_u8(value: u8) -> Result<Self, CodecError> {
        match value {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            _ => Err(CodecError::MalformedPacket("quality of service must be 0, 1 or 2")),
        }
    }

    /// The two-bit wire encoding.
    pub fn as_u8(self) -> u8 {
        match self {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        }
    }
}
```

- [ ] **Step 4: Write the packet module root**

Create `services/broker/crates/mqtt-codec/src/packet/mod.rs`:

```rust
//! Packet types and their bodies.

pub mod connect;

pub use connect::{ConnAck, Connect, Will};
```

- [ ] **Step 5: Write the CONNECT and CONNACK implementation**

Insert at the top of `services/broker/crates/mqtt-codec/src/packet/connect.rs`, above the `#[cfg(test)]` module:

```rust
use bytes::Bytes;

use crate::error::CodecError;
use crate::header::Limits;
use crate::properties::Properties;
use crate::qos::QoS;
use crate::reader::Reader;
use crate::reason::ReasonCode;
use crate::writer::{put_binary, put_string, put_u16, put_u8};
use crate::Version;

/// The message the broker publishes if a client disappears ungracefully.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Will {
    /// Where to publish it.
    pub topic: String,
    /// What to publish.
    pub payload: Bytes,
    /// At what quality of service.
    pub qos: QoS,
    /// Whether to retain it.
    pub retain: bool,
    /// MQTT 5 properties, including the will delay.
    pub properties: Properties,
}

/// The first packet a client sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    /// Which protocol version the client speaks.
    pub version: Version,
    /// The client's identifier, possibly empty.
    pub client_id: String,
    /// Whether to discard any existing session.
    pub clean_start: bool,
    /// The client's proposed keep-alive interval in seconds.
    pub keep_alive: u16,
    /// The offered user name.
    pub username: Option<String>,
    /// The offered password, which is binary rather than text.
    pub password: Option<Bytes>,
    /// The will, if the client set one.
    pub will: Option<Will>,
    /// MQTT 5 properties.
    pub properties: Properties,
}

const FLAG_CLEAN_START: u8 = 0x02;
const FLAG_WILL: u8 = 0x04;
const FLAG_WILL_RETAIN: u8 = 0x20;
const FLAG_PASSWORD: u8 = 0x40;
const FLAG_USERNAME: u8 = 0x80;

impl Connect {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(reader: &mut Reader<'_>, limits: &Limits) -> Result<Self, CodecError> {
        let name = reader.string()?;
        if name != "MQTT" {
            return Err(CodecError::MalformedPacket("protocol name is not MQTT"));
        }

        let level = reader.u8()?;
        let version = match level {
            4 => Version::V311,
            5 => Version::V5,
            other => return Err(CodecError::UnsupportedVersion(other)),
        };

        let flags = reader.u8()?;
        if flags & 0x01 != 0 {
            return Err(CodecError::MalformedPacket(
                "reserved bit in the connect flags is set",
            ));
        }

        let clean_start = flags & FLAG_CLEAN_START != 0;
        let has_will = flags & FLAG_WILL != 0;
        let will_qos = QoS::from_u8((flags & 0x18) >> 3)?;
        let will_retain = flags & FLAG_WILL_RETAIN != 0;
        let has_password = flags & FLAG_PASSWORD != 0;
        let has_username = flags & FLAG_USERNAME != 0;

        if !has_will && (will_qos != QoS::AtMostOnce || will_retain) {
            return Err(CodecError::MalformedPacket(
                "will flags set without a will",
            ));
        }
        if version == Version::V311 && has_password && !has_username {
            return Err(CodecError::MalformedPacket(
                "password without a user name is not permitted in 3.1.1",
            ));
        }

        let keep_alive = reader.u16()?;

        let properties = if version == Version::V5 {
            Properties::decode(reader)?
        } else {
            Properties::default()
        };

        let client_id = reader.string()?;
        if client_id.len() > usize::from(limits.max_client_id_len) {
            return Err(CodecError::ProtocolError(
                "client identifier longer than the configured maximum",
            ));
        }

        let will = if has_will {
            let will_properties = if version == Version::V5 {
                Properties::decode(reader)?
            } else {
                Properties::default()
            };
            let topic = reader.string()?;
            let payload = reader.binary()?;
            crate::topic::validate_name(&topic, limits)?;
            Some(Will {
                topic,
                payload,
                qos: will_qos,
                retain: will_retain,
                properties: will_properties,
            })
        } else {
            None
        };

        let username = if has_username {
            Some(reader.string()?)
        } else {
            None
        };
        let password = if has_password {
            Some(reader.binary()?)
        } else {
            None
        };

        Ok(Connect {
            version,
            client_id,
            clean_start,
            keep_alive,
            username,
            password,
            will,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    ///
    /// `version` selects whether property blocks are written. It should match
    /// `self.version`; the argument exists so that every packet type shares
    /// one encoding signature.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_string(out, "MQTT")?;
        put_u8(out, version.level());

        let mut flags = 0u8;
        if self.clean_start {
            flags |= FLAG_CLEAN_START;
        }
        if let Some(will) = &self.will {
            flags |= FLAG_WILL;
            flags |= will.qos.as_u8() << 3;
            if will.retain {
                flags |= FLAG_WILL_RETAIN;
            }
        }
        if self.username.is_some() {
            flags |= FLAG_USERNAME;
        }
        if self.password.is_some() {
            flags |= FLAG_PASSWORD;
        }
        put_u8(out, flags);
        put_u16(out, self.keep_alive);

        if version == Version::V5 {
            self.properties.encode(out)?;
        }

        put_string(out, &self.client_id)?;

        if let Some(will) = &self.will {
            if version == Version::V5 {
                will.properties.encode(out)?;
            }
            put_string(out, &will.topic)?;
            put_binary(out, &will.payload)?;
        }
        if let Some(username) = &self.username {
            put_string(out, username)?;
        }
        if let Some(password) = &self.password {
            put_binary(out, password)?;
        }

        Ok(())
    }
}

/// The broker's answer to CONNECT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    /// Whether the broker resumed an existing session.
    pub session_present: bool,
    /// Why the connection was accepted or refused.
    pub reason: ReasonCode,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl ConnAck {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(reader: &mut Reader<'_>, version: Version) -> Result<Self, CodecError> {
        let flags = reader.u8()?;
        if flags & 0xFE != 0 {
            return Err(CodecError::MalformedPacket(
                "reserved bits in the connack flags are set",
            ));
        }
        let session_present = flags & 0x01 != 0;
        let code = reader.u8()?;

        let reason = match version {
            Version::V5 => ReasonCode::from_byte(code)
                .ok_or(CodecError::MalformedPacket("unknown reason code"))?,
            Version::V311 => match code {
                0 => ReasonCode::Success,
                1 => ReasonCode::UnsupportedProtocolVersion,
                2 => ReasonCode::ClientIdentifierNotValid,
                3 => ReasonCode::ServerUnavailable,
                4 => ReasonCode::BadUserNameOrPassword,
                5 => ReasonCode::NotAuthorized,
                _ => return Err(CodecError::MalformedPacket("unknown connack return code")),
            },
        };

        let properties = if version == Version::V5 {
            Properties::decode(reader)?
        } else {
            Properties::default()
        };

        Ok(ConnAck {
            session_present,
            reason,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_u8(out, u8::from(self.session_present));
        match version {
            Version::V5 => {
                put_u8(out, self.reason as u8);
                self.properties.encode(out)?;
            }
            Version::V311 => put_u8(out, self.reason.to_v311_connack()),
        }
        Ok(())
    }
}
```

- [ ] **Step 6: Register the modules**

Add to `services/broker/crates/mqtt-codec/src/lib.rs`, below `pub mod properties;`:

```rust
pub mod packet;
pub mod qos;
```

And extend the re-exports:

```rust
pub use qos::QoS;
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd services/broker && cargo test -p mqtt-codec connect`
Expected: PASS, eight tests.

- [ ] **Step 8: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): CONNECT and CONNACK"
```

---

## Task 12: PUBLISH and the four acknowledgements

PUBACK, PUBREC, PUBREL and PUBCOMP share a body shape, so one type serves
all four. In MQTT 5 the reason code and properties are both optional: a
two-byte body means success with no properties.

**Files:**
- Create: `services/broker/crates/mqtt-codec/src/packet/publish.rs`
- Modify: `services/broker/crates/mqtt-codec/src/packet/mod.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/src/packet/publish.rs` with tests only:

```rust
//! PUBLISH and the acknowledgements that follow it.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;
    use crate::header::Limits;
    use crate::qos::QoS;
    use crate::reader::Reader;
    use crate::reason::ReasonCode;
    use crate::Version;
    use bytes::Bytes;

    fn round_trip(packet: &Publish, version: Version) -> Publish {
        let mut out = Vec::new();
        packet.encode_body(version, &mut out).expect("encodes");
        let mut reader = Reader::new(&out);
        Publish::decode_body(&mut reader, packet.flags(), version, &Limits::default())
            .expect("decodes")
    }

    #[test]
    fn round_trips_a_qos_zero_publish() {
        let packet = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "sensors/1/temp".into(),
            packet_id: None,
            payload: Bytes::from_static(b"21.5"),
            properties: Default::default(),
        };
        assert_eq!(round_trip(&packet, Version::V311), packet);
    }

    #[test]
    fn round_trips_a_qos_two_publish_with_properties() {
        let packet = Publish {
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: true,
            topic: "sensors/1/temp".into(),
            packet_id: Some(42),
            payload: Bytes::from_static(b"21.5"),
            properties: crate::Properties {
                message_expiry_interval: Some(60),
                content_type: Some("text/plain".into()),
                ..Default::default()
            },
        };
        assert_eq!(round_trip(&packet, Version::V5), packet);
    }

    #[test]
    fn an_empty_payload_is_legal() {
        let packet = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: "a".into(),
            packet_id: None,
            payload: Bytes::new(),
            properties: Default::default(),
        };
        assert_eq!(round_trip(&packet, Version::V311), packet);
    }

    #[test]
    fn rejects_a_wildcard_in_a_publish_topic() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "a/+").unwrap();
        let mut reader = Reader::new(&out);
        let err =
            Publish::decode_body(&mut reader, 0x00, Version::V311, &Limits::default()).unwrap_err();
        assert_eq!(err, CodecError::InvalidTopicName);
    }

    #[test]
    fn rejects_the_duplicate_flag_on_a_qos_zero_publish() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "a").unwrap();
        let mut reader = Reader::new(&out);
        // 0x08 is the duplicate flag with QoS left at zero.
        let err =
            Publish::decode_body(&mut reader, 0x08, Version::V311, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_quality_of_service_three() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "a").unwrap();
        let mut reader = Reader::new(&out);
        let err =
            Publish::decode_body(&mut reader, 0x06, Version::V311, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn a_v311_acknowledgement_is_just_a_packet_identifier() {
        let ack = Ack {
            packet_id: 7,
            reason: ReasonCode::Success,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        ack.encode_body(Version::V311, &mut out).unwrap();
        assert_eq!(out, vec![0x00, 0x07]);

        let mut reader = Reader::new(&out);
        assert_eq!(Ack::decode_body(&mut reader, Version::V311).unwrap(), ack);
    }

    #[test]
    fn a_v5_acknowledgement_omits_a_successful_reason_code() {
        let ack = Ack {
            packet_id: 7,
            reason: ReasonCode::Success,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        ack.encode_body(Version::V5, &mut out).unwrap();
        assert_eq!(out, vec![0x00, 0x07]);

        let mut reader = Reader::new(&out);
        assert_eq!(Ack::decode_body(&mut reader, Version::V5).unwrap(), ack);
    }

    #[test]
    fn a_v5_acknowledgement_carries_a_failure_reason_and_properties() {
        let ack = Ack {
            packet_id: 9,
            reason: ReasonCode::NoMatchingSubscribers,
            properties: crate::Properties {
                reason_string: Some("nobody listening".into()),
                ..Default::default()
            },
        };
        let mut out = Vec::new();
        ack.encode_body(Version::V5, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(Ack::decode_body(&mut reader, Version::V5).unwrap(), ack);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec publish`
Expected: FAIL to compile, with `Publish` and `Ack` not found.

- [ ] **Step 3: Write the implementation**

Insert at the top of `services/broker/crates/mqtt-codec/src/packet/publish.rs`, above the `#[cfg(test)]` module:

```rust
use bytes::Bytes;

use crate::error::CodecError;
use crate::header::Limits;
use crate::properties::Properties;
use crate::qos::QoS;
use crate::reader::Reader;
use crate::reason::ReasonCode;
use crate::writer::{put_string, put_u16, put_u8};
use crate::Version;

/// An application message travelling in either direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    /// Whether this is a redelivery.
    pub dup: bool,
    /// The quality of service it was published at.
    pub qos: QoS,
    /// Whether the broker should retain it.
    pub retain: bool,
    /// Where it was published.
    pub topic: String,
    /// Present exactly when the quality of service is above zero.
    pub packet_id: Option<u16>,
    /// The application payload, which the broker never interprets.
    pub payload: Bytes,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl Publish {
    /// The low nibble of the fixed header for this packet.
    pub fn flags(&self) -> u8 {
        let mut flags = self.qos.as_u8() << 1;
        if self.dup {
            flags |= 0x08;
        }
        if self.retain {
            flags |= 0x01;
        }
        flags
    }

    /// Decode the body, given the flags taken from the fixed header.
    pub fn decode_body(
        reader: &mut Reader<'_>,
        flags: u8,
        version: Version,
        limits: &Limits,
    ) -> Result<Self, CodecError> {
        let dup = flags & 0x08 != 0;
        let qos = QoS::from_u8((flags & 0x06) >> 1)?;
        let retain = flags & 0x01 != 0;

        if dup && qos == QoS::AtMostOnce {
            return Err(CodecError::MalformedPacket(
                "duplicate flag set on a quality of service zero publish",
            ));
        }

        let topic = reader.string()?;
        crate::topic::validate_name(&topic, limits)?;

        let packet_id = if qos == QoS::AtMostOnce {
            None
        } else {
            let id = reader.u16()?;
            if id == 0 {
                return Err(CodecError::ProtocolError(
                    "packet identifier must not be zero",
                ));
            }
            Some(id)
        };

        let properties = if version == Version::V5 {
            Properties::decode(reader)?
        } else {
            Properties::default()
        };

        let payload = reader.take_rest();

        Ok(Publish {
            dup,
            qos,
            retain,
            topic,
            packet_id,
            payload,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_string(out, &self.topic)?;
        if self.qos != QoS::AtMostOnce {
            let id = self.packet_id.ok_or(CodecError::ProtocolError(
                "a publish above quality of service zero needs a packet identifier",
            ))?;
            put_u16(out, id);
        }
        if version == Version::V5 {
            self.properties.encode(out)?;
        }
        out.extend_from_slice(&self.payload);
        Ok(())
    }
}

/// The body shared by PUBACK, PUBREC, PUBREL and PUBCOMP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    /// Which publish this answers.
    pub packet_id: u16,
    /// The outcome.
    pub reason: ReasonCode,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl Ack {
    /// Decode the body that follows the fixed header.
    ///
    /// A two-byte body means success with no properties, which is the common
    /// case and the only legal shape in 3.1.1.
    pub fn decode_body(reader: &mut Reader<'_>, version: Version) -> Result<Self, CodecError> {
        let packet_id = reader.u16()?;
        if packet_id == 0 {
            return Err(CodecError::ProtocolError(
                "packet identifier must not be zero",
            ));
        }

        if version == Version::V311 || reader.is_empty() {
            return Ok(Ack {
                packet_id,
                reason: ReasonCode::Success,
                properties: Properties::default(),
            });
        }

        let code = reader.u8()?;
        let reason = ReasonCode::from_byte(code)
            .ok_or(CodecError::MalformedPacket("unknown reason code"))?;

        let properties = if reader.is_empty() {
            Properties::default()
        } else {
            Properties::decode(reader)?
        };

        Ok(Ack {
            packet_id,
            reason,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    ///
    /// A successful acknowledgement with no properties is written in its
    /// short form, which every version understands.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_u16(out, self.packet_id);
        if version == Version::V311 {
            return Ok(());
        }
        if self.reason == ReasonCode::Success && self.properties.is_empty() {
            return Ok(());
        }
        put_u8(out, self.reason as u8);
        self.properties.encode(out)?;
        Ok(())
    }
}
```

- [ ] **Step 4: Register the module**

Replace the contents of `services/broker/crates/mqtt-codec/src/packet/mod.rs`:

```rust
//! Packet types and their bodies.

pub mod connect;
pub mod publish;

pub use connect::{ConnAck, Connect, Will};
pub use publish::{Ack, Publish};
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd services/broker && cargo test -p mqtt-codec publish`
Expected: PASS, nine tests.

- [ ] **Step 6: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): PUBLISH and the acknowledgement packets"
```

---
## Task 13: SUBSCRIBE, SUBACK, UNSUBSCRIBE and UNSUBACK

**Files:**
- Create: `services/broker/crates/mqtt-codec/src/packet/subscribe.rs`
- Modify: `services/broker/crates/mqtt-codec/src/packet/mod.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/src/packet/subscribe.rs` with tests only:

```rust
//! SUBSCRIBE, SUBACK, UNSUBSCRIBE and UNSUBACK.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;
    use crate::header::Limits;
    use crate::qos::QoS;
    use crate::reader::Reader;
    use crate::reason::ReasonCode;
    use crate::Version;

    fn option(filter: &str, qos: QoS) -> SubscriptionOptions {
        SubscriptionOptions {
            filter: filter.into(),
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendAlways,
        }
    }

    #[test]
    fn round_trips_a_v311_subscribe() {
        let packet = Subscribe {
            packet_id: 1,
            filters: vec![option("a/b", QoS::AtLeastOnce), option("c/#", QoS::AtMostOnce)],
            properties: Default::default(),
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        let decoded =
            Subscribe::decode_body(&mut reader, Version::V311, &Limits::default()).unwrap();
        assert_eq!(decoded, packet);
    }

    #[test]
    fn round_trips_a_v5_subscribe_with_every_option_set() {
        let packet = Subscribe {
            packet_id: 2,
            filters: vec![SubscriptionOptions {
                filter: "a/+".into(),
                qos: QoS::ExactlyOnce,
                no_local: true,
                retain_as_published: true,
                retain_handling: RetainHandling::DoNotSend,
            }],
            properties: crate::Properties {
                subscription_identifiers: vec![7],
                ..Default::default()
            },
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        let decoded = Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).unwrap();
        assert_eq!(decoded, packet);
    }

    #[test]
    fn rejects_a_subscribe_with_no_filters() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        let mut reader = Reader::new(&out);
        let err =
            Subscribe::decode_body(&mut reader, Version::V311, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::ProtocolError(_)));
    }

    #[test]
    fn rejects_reserved_bits_in_the_options_byte() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        crate::writer::put_string(&mut out, "a").unwrap();
        out.push(0xC0); // both reserved high bits set
        let mut reader = Reader::new(&out);
        let err = Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_retain_handling_three() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        crate::writer::put_string(&mut out, "a").unwrap();
        out.push(0x30); // retain handling bits both set
        let mut reader = Reader::new(&out);
        let err = Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_no_local_on_a_shared_subscription() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        crate::writer::put_string(&mut out, "$share/g/a").unwrap();
        out.push(0x04); // no local
        let mut reader = Reader::new(&out);
        let err = Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::ProtocolError(_)));
    }

    #[test]
    fn rejects_an_invalid_filter() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        crate::writer::put_string(&mut out, "a/#/b").unwrap();
        out.push(0x00);
        let mut reader = Reader::new(&out);
        assert_eq!(
            Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).unwrap_err(),
            CodecError::InvalidTopicFilter
        );
    }

    #[test]
    fn round_trips_suback_in_both_versions() {
        let packet = SubAck {
            packet_id: 3,
            reasons: vec![ReasonCode::GrantedQos1, ReasonCode::NotAuthorized],
            properties: Default::default(),
        };
        for version in [Version::V311, Version::V5] {
            let mut out = Vec::new();
            packet.encode_body(version, &mut out).unwrap();
            let mut reader = Reader::new(&out);
            let decoded = SubAck::decode_body(&mut reader, version).unwrap();
            assert_eq!(decoded.packet_id, 3);
            assert_eq!(decoded.reasons.len(), 2);
            assert_eq!(decoded.reasons[0], ReasonCode::GrantedQos1);
            assert!(decoded.reasons[1].is_failure());
        }
    }

    #[test]
    fn round_trips_unsubscribe() {
        let packet = Unsubscribe {
            packet_id: 4,
            filters: vec!["a/b".into(), "c/#".into()],
            properties: Default::default(),
        };
        for version in [Version::V311, Version::V5] {
            let mut out = Vec::new();
            packet.encode_body(version, &mut out).unwrap();
            let mut reader = Reader::new(&out);
            let decoded =
                Unsubscribe::decode_body(&mut reader, version, &Limits::default()).unwrap();
            assert_eq!(decoded, packet);
        }
    }

    #[test]
    fn a_v311_unsuback_carries_no_reason_codes() {
        let packet = UnsubAck {
            packet_id: 5,
            reasons: vec![ReasonCode::Success],
            properties: Default::default(),
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();
        assert_eq!(out, vec![0x00, 0x05]);

        let mut reader = Reader::new(&out);
        let decoded = UnsubAck::decode_body(&mut reader, Version::V311).unwrap();
        assert_eq!(decoded.packet_id, 5);
        assert!(decoded.reasons.is_empty());
    }

    #[test]
    fn a_v5_unsuback_carries_one_reason_per_filter() {
        let packet = UnsubAck {
            packet_id: 6,
            reasons: vec![ReasonCode::Success, ReasonCode::NoSubscriptionExisted],
            properties: Default::default(),
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(UnsubAck::decode_body(&mut reader, Version::V5).unwrap(), packet);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec subscribe`
Expected: FAIL to compile, with `Subscribe`, `SubAck`, `Unsubscribe`, `UnsubAck`, `SubscriptionOptions`, `RetainHandling` not found.

- [ ] **Step 3: Write the implementation**

Insert at the top of `services/broker/crates/mqtt-codec/src/packet/subscribe.rs`, above the `#[cfg(test)]` module:

```rust
use crate::error::CodecError;
use crate::header::Limits;
use crate::properties::Properties;
use crate::qos::QoS;
use crate::reader::Reader;
use crate::reason::ReasonCode;
use crate::writer::{put_string, put_u16, put_u8};
use crate::Version;

/// When the broker should send retained messages for a new subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RetainHandling {
    /// Always send them.
    #[default]
    SendAlways,
    /// Send them only if this subscription did not already exist.
    SendIfNew,
    /// Never send them.
    DoNotSend,
}

impl RetainHandling {
    fn from_u8(value: u8) -> Result<Self, CodecError> {
        match value {
            0 => Ok(RetainHandling::SendAlways),
            1 => Ok(RetainHandling::SendIfNew),
            2 => Ok(RetainHandling::DoNotSend),
            _ => Err(CodecError::MalformedPacket("retain handling must be 0, 1 or 2")),
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            RetainHandling::SendAlways => 0,
            RetainHandling::SendIfNew => 1,
            RetainHandling::DoNotSend => 2,
        }
    }
}

/// One filter and the options attached to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionOptions {
    /// The topic filter.
    pub filter: String,
    /// The maximum quality of service to deliver at.
    pub qos: QoS,
    /// Suppress delivery of the subscriber's own publications.
    pub no_local: bool,
    /// Forward the retain flag as published rather than clearing it.
    pub retain_as_published: bool,
    /// When to send retained messages.
    pub retain_handling: RetainHandling,
}

/// A request to subscribe to one or more filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    /// Identifies the request so SUBACK can answer it.
    pub packet_id: u16,
    /// At least one filter.
    pub filters: Vec<SubscriptionOptions>,
    /// MQTT 5 properties, including the subscription identifier.
    pub properties: Properties,
}

fn read_packet_id(reader: &mut Reader<'_>) -> Result<u16, CodecError> {
    let id = reader.u16()?;
    if id == 0 {
        return Err(CodecError::ProtocolError(
            "packet identifier must not be zero",
        ));
    }
    Ok(id)
}

impl Subscribe {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(
        reader: &mut Reader<'_>,
        version: Version,
        limits: &Limits,
    ) -> Result<Self, CodecError> {
        let packet_id = read_packet_id(reader)?;
        let properties = if version == Version::V5 {
            Properties::decode(reader)?
        } else {
            Properties::default()
        };

        let mut filters = Vec::new();
        while !reader.is_empty() {
            let filter = reader.string()?;
            let options = reader.u8()?;

            let reserved_mask = match version {
                Version::V5 => 0xC0,
                Version::V311 => 0xFC,
            };
            if options & reserved_mask != 0 {
                return Err(CodecError::MalformedPacket(
                    "reserved bits in the subscription options are set",
                ));
            }

            let qos = QoS::from_u8(options & 0x03)?;
            let no_local = options & 0x04 != 0;
            let retain_as_published = options & 0x08 != 0;
            let retain_handling = RetainHandling::from_u8((options & 0x30) >> 4)?;

            let shared = crate::topic::parse_shared(&filter)?;
            if no_local && shared.is_some() {
                return Err(CodecError::ProtocolError(
                    "no local is not permitted on a shared subscription",
                ));
            }
            let to_validate = shared.map(|(_, inner)| inner).unwrap_or(filter.as_str());
            crate::topic::validate_filter(to_validate, limits)?;

            filters.push(SubscriptionOptions {
                filter,
                qos,
                no_local,
                retain_as_published,
                retain_handling,
            });
        }

        if filters.is_empty() {
            return Err(CodecError::ProtocolError(
                "subscribe must carry at least one filter",
            ));
        }

        Ok(Subscribe {
            packet_id,
            filters,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_u16(out, self.packet_id);
        if version == Version::V5 {
            self.properties.encode(out)?;
        }
        for entry in &self.filters {
            put_string(out, &entry.filter)?;
            let mut options = entry.qos.as_u8();
            if version == Version::V5 {
                if entry.no_local {
                    options |= 0x04;
                }
                if entry.retain_as_published {
                    options |= 0x08;
                }
                options |= entry.retain_handling.as_u8() << 4;
            }
            put_u8(out, options);
        }
        Ok(())
    }
}

/// The answer to SUBSCRIBE, one reason code per requested filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    /// Which request this answers.
    pub packet_id: u16,
    /// One outcome per filter, in the order requested.
    pub reasons: Vec<ReasonCode>,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl SubAck {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(reader: &mut Reader<'_>, version: Version) -> Result<Self, CodecError> {
        let packet_id = read_packet_id(reader)?;
        let properties = if version == Version::V5 {
            Properties::decode(reader)?
        } else {
            Properties::default()
        };

        let mut reasons = Vec::new();
        while !reader.is_empty() {
            let code = reader.u8()?;
            let reason = match version {
                Version::V5 => ReasonCode::from_byte(code)
                    .ok_or(CodecError::MalformedPacket("unknown reason code"))?,
                Version::V311 => match code {
                    0x00 => ReasonCode::Success,
                    0x01 => ReasonCode::GrantedQos1,
                    0x02 => ReasonCode::GrantedQos2,
                    0x80 => ReasonCode::UnspecifiedError,
                    _ => return Err(CodecError::MalformedPacket("unknown suback return code")),
                },
            };
            reasons.push(reason);
        }

        if reasons.is_empty() {
            return Err(CodecError::ProtocolError(
                "suback must carry at least one reason code",
            ));
        }

        Ok(SubAck {
            packet_id,
            reasons,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_u16(out, self.packet_id);
        if version == Version::V5 {
            self.properties.encode(out)?;
        }
        for reason in &self.reasons {
            let byte = match version {
                Version::V5 => *reason as u8,
                Version::V311 => reason.to_v311_suback(),
            };
            put_u8(out, byte);
        }
        Ok(())
    }
}

/// A request to remove one or more subscriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    /// Identifies the request.
    pub packet_id: u16,
    /// At least one filter.
    pub filters: Vec<String>,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl Unsubscribe {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(
        reader: &mut Reader<'_>,
        version: Version,
        limits: &Limits,
    ) -> Result<Self, CodecError> {
        let packet_id = read_packet_id(reader)?;
        let properties = if version == Version::V5 {
            Properties::decode(reader)?
        } else {
            Properties::default()
        };

        let mut filters = Vec::new();
        while !reader.is_empty() {
            let filter = reader.string()?;
            let shared = crate::topic::parse_shared(&filter)?;
            let to_validate = shared.map(|(_, inner)| inner).unwrap_or(filter.as_str());
            crate::topic::validate_filter(to_validate, limits)?;
            filters.push(filter);
        }

        if filters.is_empty() {
            return Err(CodecError::ProtocolError(
                "unsubscribe must carry at least one filter",
            ));
        }

        Ok(Unsubscribe {
            packet_id,
            filters,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_u16(out, self.packet_id);
        if version == Version::V5 {
            self.properties.encode(out)?;
        }
        for filter in &self.filters {
            put_string(out, filter)?;
        }
        Ok(())
    }
}

/// The answer to UNSUBSCRIBE.
///
/// MQTT 3.1.1 carries no reason codes, so `reasons` is empty when decoding a
/// 3.1.1 packet and is ignored when encoding one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAck {
    /// Which request this answers.
    pub packet_id: u16,
    /// One outcome per filter, in the order requested. MQTT 5 only.
    pub reasons: Vec<ReasonCode>,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl UnsubAck {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(reader: &mut Reader<'_>, version: Version) -> Result<Self, CodecError> {
        let packet_id = read_packet_id(reader)?;
        if version == Version::V311 {
            return Ok(UnsubAck {
                packet_id,
                reasons: Vec::new(),
                properties: Properties::default(),
            });
        }

        let properties = Properties::decode(reader)?;
        let mut reasons = Vec::new();
        while !reader.is_empty() {
            let code = reader.u8()?;
            reasons.push(
                ReasonCode::from_byte(code)
                    .ok_or(CodecError::MalformedPacket("unknown reason code"))?,
            );
        }

        Ok(UnsubAck {
            packet_id,
            reasons,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_u16(out, self.packet_id);
        if version == Version::V311 {
            return Ok(());
        }
        self.properties.encode(out)?;
        for reason in &self.reasons {
            put_u8(out, *reason as u8);
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Register the module**

Replace the contents of `services/broker/crates/mqtt-codec/src/packet/mod.rs`:

```rust
//! Packet types and their bodies.

pub mod connect;
pub mod publish;
pub mod subscribe;

pub use connect::{ConnAck, Connect, Will};
pub use publish::{Ack, Publish};
pub use subscribe::{RetainHandling, SubAck, Subscribe, SubscriptionOptions, UnsubAck, Unsubscribe};
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd services/broker && cargo test -p mqtt-codec subscribe`
Expected: PASS, eleven tests.

- [ ] **Step 6: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): subscription packets"
```

---

## Task 14: PINGREQ, PINGRESP, DISCONNECT and AUTH

**Files:**
- Create: `services/broker/crates/mqtt-codec/src/packet/control.rs`
- Modify: `services/broker/crates/mqtt-codec/src/packet/mod.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/src/packet/control.rs` with tests only:

```rust
//! DISCONNECT and AUTH. PINGREQ and PINGRESP have no body at all.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;
    use crate::reader::Reader;
    use crate::reason::ReasonCode;
    use crate::Version;

    #[test]
    fn a_v311_disconnect_has_an_empty_body() {
        let packet = Disconnect {
            reason: ReasonCode::Success,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();
        assert!(out.is_empty());

        let mut reader = Reader::new(&out);
        assert_eq!(
            Disconnect::decode_body(&mut reader, Version::V311).unwrap(),
            packet
        );
    }

    #[test]
    fn a_v5_normal_disconnect_also_has_an_empty_body() {
        let packet = Disconnect {
            reason: ReasonCode::Success,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn a_v5_disconnect_carries_a_reason_and_properties() {
        let packet = Disconnect {
            reason: ReasonCode::SessionTakenOver,
            properties: crate::Properties {
                reason_string: Some("another connection claimed this identifier".into()),
                ..Default::default()
            },
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(
            Disconnect::decode_body(&mut reader, Version::V5).unwrap(),
            packet
        );
    }

    #[test]
    fn a_v5_disconnect_may_carry_a_reason_with_no_properties() {
        let bytes = [ReasonCode::KeepAliveTimeout as u8];
        let mut reader = Reader::new(&bytes);
        let decoded = Disconnect::decode_body(&mut reader, Version::V5).unwrap();
        assert_eq!(decoded.reason, ReasonCode::KeepAliveTimeout);
        assert!(decoded.properties.is_empty());
    }

    #[test]
    fn rejects_an_unknown_disconnect_reason() {
        let bytes = [0x7F];
        let mut reader = Reader::new(&bytes);
        let err = Disconnect::decode_body(&mut reader, Version::V5).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn round_trips_an_auth_exchange() {
        let packet = Auth {
            reason: ReasonCode::ContinueAuthentication,
            properties: crate::Properties {
                authentication_method: Some("SCRAM-SHA-256".into()),
                authentication_data: Some(bytes::Bytes::from_static(b"r=nonce")),
                ..Default::default()
            },
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(Auth::decode_body(&mut reader).unwrap(), packet);
    }

    #[test]
    fn an_empty_auth_body_means_success() {
        let mut reader = Reader::new(&[]);
        let decoded = Auth::decode_body(&mut reader).unwrap();
        assert_eq!(decoded.reason, ReasonCode::Success);
        assert!(decoded.properties.is_empty());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec control`
Expected: FAIL to compile, with `Disconnect` and `Auth` not found.

- [ ] **Step 3: Write the implementation**

Insert at the top of `services/broker/crates/mqtt-codec/src/packet/control.rs`, above the `#[cfg(test)]` module:

```rust
use crate::error::CodecError;
use crate::properties::Properties;
use crate::reader::Reader;
use crate::reason::ReasonCode;
use crate::writer::put_u8;
use crate::Version;

/// A deliberate end to the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disconnect {
    /// Why. [`ReasonCode::Success`] is a normal disconnection.
    pub reason: ReasonCode,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl Disconnect {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(reader: &mut Reader<'_>, version: Version) -> Result<Self, CodecError> {
        if version == Version::V311 || reader.is_empty() {
            return Ok(Disconnect {
                reason: ReasonCode::Success,
                properties: Properties::default(),
            });
        }

        let code = reader.u8()?;
        let reason = ReasonCode::from_byte(code)
            .ok_or(CodecError::MalformedPacket("unknown reason code"))?;
        let properties = if reader.is_empty() {
            Properties::default()
        } else {
            Properties::decode(reader)?
        };

        Ok(Disconnect { reason, properties })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        if version == Version::V311 {
            return Ok(());
        }
        if self.reason == ReasonCode::Success && self.properties.is_empty() {
            return Ok(());
        }
        put_u8(out, self.reason as u8);
        self.properties.encode(out)?;
        Ok(())
    }
}

/// A step in the MQTT 5 enhanced authentication exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auth {
    /// Whether to continue, re-authenticate, or that it succeeded.
    pub reason: ReasonCode,
    /// MQTT 5 properties, carrying the method and its data.
    pub properties: Properties,
}

impl Auth {
    /// Decode the body that follows the fixed header.
    ///
    /// AUTH exists only in MQTT 5, so no version argument is needed. The
    /// caller rejects the packet outright on a 3.1.1 connection.
    pub fn decode_body(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        if reader.is_empty() {
            return Ok(Auth {
                reason: ReasonCode::Success,
                properties: Properties::default(),
            });
        }

        let code = reader.u8()?;
        let reason = ReasonCode::from_byte(code)
            .ok_or(CodecError::MalformedPacket("unknown reason code"))?;
        let properties = if reader.is_empty() {
            Properties::default()
        } else {
            Properties::decode(reader)?
        };

        Ok(Auth { reason, properties })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, _version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        if self.reason == ReasonCode::Success && self.properties.is_empty() {
            return Ok(());
        }
        put_u8(out, self.reason as u8);
        self.properties.encode(out)?;
        Ok(())
    }
}
```

- [ ] **Step 4: Register the module**

Replace the contents of `services/broker/crates/mqtt-codec/src/packet/mod.rs`:

```rust
//! Packet types and their bodies.

pub mod connect;
pub mod control;
pub mod publish;
pub mod subscribe;

pub use connect::{ConnAck, Connect, Will};
pub use control::{Auth, Disconnect};
pub use publish::{Ack, Publish};
pub use subscribe::{RetainHandling, SubAck, Subscribe, SubscriptionOptions, UnsubAck, Unsubscribe};
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd services/broker && cargo test -p mqtt-codec control`
Expected: PASS, seven tests.

- [ ] **Step 6: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): disconnect and authentication packets"
```

---
## Task 15: The Packet enum and the streaming codec

This is the public entry point. `decode` takes the connection's read buffer,
consumes exactly one frame if a whole one is present, and leaves anything
after it untouched.

**Files:**
- Modify: `services/broker/crates/mqtt-codec/src/packet/mod.rs`, `services/broker/crates/mqtt-codec/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Append to `services/broker/crates/mqtt-codec/src/packet/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;
    use crate::header::Limits;
    use crate::qos::QoS;
    use crate::reason::ReasonCode;
    use crate::Version;
    use bytes::{Bytes, BytesMut};

    fn publish() -> Packet {
        Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "a/b".into(),
            packet_id: Some(1),
            payload: Bytes::from_static(b"hello"),
            properties: Default::default(),
        })
    }

    #[test]
    fn round_trips_through_the_public_entry_points() {
        let original = publish();
        let mut buf = BytesMut::new();
        encode(&original, Version::V311, &mut buf).unwrap();

        let decoded = decode(&mut buf, Version::V311, &Limits::default())
            .unwrap()
            .expect("a whole frame");
        assert_eq!(decoded, original);
        assert!(buf.is_empty(), "the frame should have been consumed");
    }

    #[test]
    fn decodes_one_frame_and_leaves_the_next_alone() {
        let mut buf = BytesMut::new();
        encode(&publish(), Version::V311, &mut buf).unwrap();
        encode(&Packet::PingReq, Version::V311, &mut buf).unwrap();

        let first = decode(&mut buf, Version::V311, &Limits::default())
            .unwrap()
            .expect("first frame");
        assert!(matches!(first, Packet::Publish(_)));

        let second = decode(&mut buf, Version::V311, &Limits::default())
            .unwrap()
            .expect("second frame");
        assert_eq!(second, Packet::PingReq);
        assert!(buf.is_empty());
    }

    #[test]
    fn reports_need_more_bytes_for_every_prefix_of_a_frame() {
        let mut whole = BytesMut::new();
        encode(&publish(), Version::V311, &mut whole).unwrap();

        for take in 0..whole.len() {
            let mut partial = BytesMut::from(&whole[..take]);
            let outcome = decode(&mut partial, Version::V311, &Limits::default()).unwrap();
            assert!(outcome.is_none(), "should need more bytes at {take}");
            assert_eq!(partial.len(), take, "a partial frame must not be consumed");
        }
    }

    #[test]
    fn rejects_trailing_bytes_inside_a_declared_frame() {
        // A PINGREQ declaring a one-byte body, which PINGREQ never has.
        let mut buf = BytesMut::from(&[0xC0u8, 0x01, 0xFF][..]);
        let err = decode(&mut buf, Version::V311, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_auth_on_a_3_1_1_connection() {
        let mut buf = BytesMut::new();
        encode(
            &Packet::Auth(Auth {
                reason: ReasonCode::Success,
                properties: Default::default(),
            }),
            Version::V5,
            &mut buf,
        )
        .unwrap();
        let err = decode(&mut buf, Version::V311, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::ProtocolError(_)));
    }

    #[test]
    fn connect_is_decoded_without_knowing_the_version_first() {
        let original = Packet::Connect(Connect {
            version: Version::V5,
            client_id: "c".into(),
            clean_start: true,
            keep_alive: 10,
            username: None,
            password: None,
            will: None,
            properties: Default::default(),
        });
        let mut buf = BytesMut::new();
        encode(&original, Version::V5, &mut buf).unwrap();

        // The caller does not yet know the version, so it passes 3.1.1.
        let decoded = decode(&mut buf, Version::V311, &Limits::default())
            .unwrap()
            .expect("a whole frame");
        match decoded {
            Packet::Connect(connect) => assert_eq!(connect.version, Version::V5),
            other => panic!("expected a connect, got {other:?}"),
        }
    }

    #[test]
    fn rejects_an_oversized_frame_without_buffering_it() {
        let limits = Limits {
            max_packet_size: 16,
            ..Limits::default()
        };
        // Declares a 300-byte body; only the header is present.
        let mut buf = BytesMut::from(&[0x30u8, 0xAC, 0x02][..]);
        assert_eq!(
            decode(&mut buf, Version::V311, &limits).unwrap_err(),
            CodecError::PacketTooLarge
        );
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p mqtt-codec packet::tests`
Expected: FAIL to compile, with `Packet`, `decode`, `encode` not found.

- [ ] **Step 3: Write the implementation**

Insert into `services/broker/crates/mqtt-codec/src/packet/mod.rs`, after the `pub use` lines and before the `#[cfg(test)]` module:

```rust
use bytes::{Buf, BytesMut};

use crate::error::CodecError;
use crate::header::{peek, Limits, PacketKind};
use crate::reader::Reader;
use crate::varint;
use crate::Version;

/// Any MQTT control packet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Packet {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(Ack),
    PubRec(Ack),
    PubRel(Ack),
    PubComp(Ack),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    PingReq,
    PingResp,
    Disconnect(Disconnect),
    Auth(Auth),
}

impl Packet {
    /// Which packet type this is.
    pub fn kind(&self) -> PacketKind {
        match self {
            Packet::Connect(_) => PacketKind::Connect,
            Packet::ConnAck(_) => PacketKind::ConnAck,
            Packet::Publish(_) => PacketKind::Publish,
            Packet::PubAck(_) => PacketKind::PubAck,
            Packet::PubRec(_) => PacketKind::PubRec,
            Packet::PubRel(_) => PacketKind::PubRel,
            Packet::PubComp(_) => PacketKind::PubComp,
            Packet::Subscribe(_) => PacketKind::Subscribe,
            Packet::SubAck(_) => PacketKind::SubAck,
            Packet::Unsubscribe(_) => PacketKind::Unsubscribe,
            Packet::UnsubAck(_) => PacketKind::UnsubAck,
            Packet::PingReq => PacketKind::PingReq,
            Packet::PingResp => PacketKind::PingResp,
            Packet::Disconnect(_) => PacketKind::Disconnect,
            Packet::Auth(_) => PacketKind::Auth,
        }
    }

    /// The low nibble of the fixed header.
    pub fn flags(&self) -> u8 {
        match self {
            Packet::Publish(publish) => publish.flags(),
            Packet::PubRel(_) | Packet::Subscribe(_) | Packet::Unsubscribe(_) => 0b0010,
            _ => 0b0000,
        }
    }

    fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        match self {
            Packet::Connect(p) => p.encode_body(version, out),
            Packet::ConnAck(p) => p.encode_body(version, out),
            Packet::Publish(p) => p.encode_body(version, out),
            Packet::PubAck(p) | Packet::PubRec(p) | Packet::PubRel(p) | Packet::PubComp(p) => {
                p.encode_body(version, out)
            }
            Packet::Subscribe(p) => p.encode_body(version, out),
            Packet::SubAck(p) => p.encode_body(version, out),
            Packet::Unsubscribe(p) => p.encode_body(version, out),
            Packet::UnsubAck(p) => p.encode_body(version, out),
            Packet::PingReq | Packet::PingResp => Ok(()),
            Packet::Disconnect(p) => p.encode_body(version, out),
            Packet::Auth(p) => p.encode_body(version, out),
        }
    }
}

/// Decode one packet from the front of `buf`.
///
/// Returns `Ok(None)` when a whole frame has not yet arrived, leaving `buf`
/// untouched so the caller can read more and try again. On success the frame
/// is consumed and anything after it remains.
///
/// `version` is the version negotiated for this connection. CONNECT ignores
/// it and reports the version it carries, so a caller that has not yet seen
/// CONNECT may pass any value.
pub fn decode(
    buf: &mut BytesMut,
    version: Version,
    limits: &Limits,
) -> Result<Option<Packet>, CodecError> {
    let Some(header) = peek(buf, limits)? else {
        return Ok(None);
    };

    let frame = &buf[..header.total_len()];
    let body = &frame[header.header_len..];
    let mut reader = Reader::new(body);

    let packet = match header.kind {
        PacketKind::Connect => Packet::Connect(Connect::decode_body(&mut reader, limits)?),
        PacketKind::ConnAck => Packet::ConnAck(ConnAck::decode_body(&mut reader, version)?),
        PacketKind::Publish => Packet::Publish(Publish::decode_body(
            &mut reader,
            header.flags,
            version,
            limits,
        )?),
        PacketKind::PubAck => Packet::PubAck(Ack::decode_body(&mut reader, version)?),
        PacketKind::PubRec => Packet::PubRec(Ack::decode_body(&mut reader, version)?),
        PacketKind::PubRel => Packet::PubRel(Ack::decode_body(&mut reader, version)?),
        PacketKind::PubComp => Packet::PubComp(Ack::decode_body(&mut reader, version)?),
        PacketKind::Subscribe => {
            Packet::Subscribe(Subscribe::decode_body(&mut reader, version, limits)?)
        }
        PacketKind::SubAck => Packet::SubAck(SubAck::decode_body(&mut reader, version)?),
        PacketKind::Unsubscribe => {
            Packet::Unsubscribe(Unsubscribe::decode_body(&mut reader, version, limits)?)
        }
        PacketKind::UnsubAck => Packet::UnsubAck(UnsubAck::decode_body(&mut reader, version)?),
        PacketKind::PingReq => Packet::PingReq,
        PacketKind::PingResp => Packet::PingResp,
        PacketKind::Disconnect => {
            Packet::Disconnect(Disconnect::decode_body(&mut reader, version)?)
        }
        PacketKind::Auth => {
            if version == Version::V311 {
                return Err(CodecError::ProtocolError(
                    "authentication packets do not exist in 3.1.1",
                ));
            }
            Packet::Auth(Auth::decode_body(&mut reader)?)
        }
    };

    if !reader.is_empty() {
        return Err(CodecError::MalformedPacket(
            "trailing bytes after the packet body",
        ));
    }

    buf.advance(header.total_len());
    Ok(Some(packet))
}

/// Append one whole packet, fixed header included, to `out`.
pub fn encode(packet: &Packet, version: Version, out: &mut BytesMut) -> Result<(), CodecError> {
    let mut body = Vec::new();
    packet.encode_body(version, &mut body)?;

    let len = u32::try_from(body.len())
        .map_err(|_| CodecError::MalformedPacket("packet body too large"))?;
    if len > varint::MAX {
        return Err(CodecError::MalformedPacket("packet body too large"));
    }

    let mut header = Vec::with_capacity(5);
    header.push((packet.kind().nibble() << 4) | packet.flags());
    varint::encode(len, &mut header)?;

    out.extend_from_slice(&header);
    out.extend_from_slice(&body);
    Ok(())
}
```

- [ ] **Step 4: Re-export the entry points**

Extend the re-exports at the bottom of `services/broker/crates/mqtt-codec/src/lib.rs`:

```rust
pub use packet::{decode, encode, Packet};
```

- [ ] **Step 5: Run the whole suite to verify it passes**

Run: `cd services/broker && cargo test -p mqtt-codec`
Expected: PASS, every test in the crate.

- [ ] **Step 6: Verify lints are clean**

Run: `cd services/broker && cargo clippy --all-targets -- -D warnings && cargo fmt --all --check`
Expected: no warnings and no diff.

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/mqtt-codec/src
git commit -m "feat(codec): packet enum and streaming entry points"
```

---

## Task 16: Property-based round trips

Example tests prove the cases we thought of. Properties prove the cases we
did not.

**Files:**
- Create: `services/broker/crates/mqtt-codec/tests/roundtrip.rs`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/tests/roundtrip.rs`:

```rust
//! Round-trip properties over generated packets.

use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{
    Ack, ConnAck, Connect, Disconnect, Publish, RetainHandling, SubAck, Subscribe,
    SubscriptionOptions, UnsubAck, Unsubscribe, Will,
};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};
use proptest::prelude::*;

fn any_qos() -> impl Strategy<Value = QoS> {
    prop_oneof![
        Just(QoS::AtMostOnce),
        Just(QoS::AtLeastOnce),
        Just(QoS::ExactlyOnce),
    ]
}

/// Topic levels that are always valid in a name and a filter.
fn any_level() -> impl Strategy<Value = String> {
    "[a-z0-9]{1,8}"
}

fn any_topic() -> impl Strategy<Value = String> {
    prop::collection::vec(any_level(), 1..4).prop_map(|parts| parts.join("/"))
}

fn any_filter() -> impl Strategy<Value = String> {
    prop::collection::vec(prop_oneof![any_level(), Just("+".to_string())], 1..4)
        .prop_map(|parts| parts.join("/"))
}

fn any_payload() -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 0..64).prop_map(Bytes::from)
}

/// Properties that every packet type accepts, so one generator serves all.
fn any_properties() -> impl Strategy<Value = Properties> {
    (
        prop::option::of(any::<u32>()),
        prop::option::of("[a-z ]{0,16}"),
        prop::collection::vec(("[a-z]{1,4}", "[a-z]{0,8}"), 0..3),
    )
        .prop_map(|(expiry, reason, user)| Properties {
            message_expiry_interval: expiry,
            reason_string: reason,
            user_properties: user,
            ..Properties::default()
        })
}

/// Properties are only encoded for MQTT 5, so 3.1.1 packets carry none.
fn properties_for(version: Version) -> BoxedStrategy<Properties> {
    match version {
        Version::V5 => any_properties().boxed(),
        Version::V311 => Just(Properties::default()).boxed(),
    }
}

fn any_packet_id() -> impl Strategy<Value = u16> {
    1u16..=u16::MAX
}

fn any_publish(version: Version) -> impl Strategy<Value = Packet> {
    (
        any_qos(),
        any::<bool>(),
        any_topic(),
        any_packet_id(),
        any_payload(),
        properties_for(version),
    )
        .prop_map(|(qos, retain, topic, id, payload, properties)| {
            Packet::Publish(Publish {
                // The duplicate flag is illegal at quality of service zero.
                dup: false,
                qos,
                retain,
                topic,
                packet_id: if qos == QoS::AtMostOnce { None } else { Some(id) },
                payload,
                properties,
            })
        })
}

fn any_subscribe(version: Version) -> impl Strategy<Value = Packet> {
    let option = (
        any_filter(),
        any_qos(),
        any::<bool>(),
        any::<bool>(),
        prop_oneof![
            Just(RetainHandling::SendAlways),
            Just(RetainHandling::SendIfNew),
            Just(RetainHandling::DoNotSend),
        ],
    )
        .prop_map(move |(filter, qos, no_local, rap, handling)| SubscriptionOptions {
            filter,
            qos,
            // These three are only encoded for MQTT 5.
            no_local: version == Version::V5 && no_local,
            retain_as_published: version == Version::V5 && rap,
            retain_handling: if version == Version::V5 {
                handling
            } else {
                RetainHandling::SendAlways
            },
        });

    (
        any_packet_id(),
        prop::collection::vec(option, 1..4),
        properties_for(version),
    )
        .prop_map(|(packet_id, filters, properties)| {
            Packet::Subscribe(Subscribe {
                packet_id,
                filters,
                properties,
            })
        })
}

fn any_connect(version: Version) -> impl Strategy<Value = Packet> {
    (
        "[a-z0-9-]{0,24}",
        any::<bool>(),
        any::<u16>(),
        prop::option::of("[a-z]{1,8}"),
        prop::option::of(any_payload()),
        prop::option::of((any_topic(), any_payload(), any_qos(), any::<bool>())),
        properties_for(version),
    )
        .prop_map(
            move |(client_id, clean_start, keep_alive, username, password, will, properties)| {
                // MQTT 3.1.1 forbids a password without a user name.
                let password = if username.is_none() && version == Version::V311 {
                    None
                } else {
                    password
                };
                Packet::Connect(Connect {
                    version,
                    client_id,
                    clean_start,
                    keep_alive,
                    username,
                    password,
                    will: will.map(|(topic, payload, qos, retain)| Will {
                        topic,
                        payload,
                        qos,
                        retain,
                        properties: Properties::default(),
                    }),
                    properties,
                })
            },
        )
}

fn any_ack(version: Version) -> impl Strategy<Value = Packet> {
    (any_packet_id(), properties_for(version)).prop_map(move |(packet_id, properties)| {
        // Only success survives the 3.1.1 short form, which carries no code.
        let reason = ReasonCode::Success;
        Packet::PubAck(Ack {
            packet_id,
            reason,
            properties: if version == Version::V5 {
                properties
            } else {
                Properties::default()
            },
        })
    })
}

fn any_unsubscribe(version: Version) -> impl Strategy<Value = Packet> {
    (
        any_packet_id(),
        prop::collection::vec(any_filter(), 1..4),
        properties_for(version),
    )
        .prop_map(|(packet_id, filters, properties)| {
            Packet::Unsubscribe(Unsubscribe {
                packet_id,
                filters,
                properties,
            })
        })
}

fn any_packet(version: Version) -> BoxedStrategy<Packet> {
    prop_oneof![
        any_connect(version),
        any_publish(version),
        any_subscribe(version),
        any_unsubscribe(version),
        any_ack(version),
        Just(Packet::PingReq),
        Just(Packet::PingResp),
        Just(Packet::Disconnect(Disconnect {
            reason: ReasonCode::Success,
            properties: Properties::default(),
        })),
    ]
    .boxed()
}

fn round_trip(packet: &Packet, version: Version) -> Packet {
    let mut buf = BytesMut::new();
    encode(packet, version, &mut buf).expect("encodes");
    let decoded = decode(&mut buf, version, &Limits::default())
        .expect("decodes")
        .expect("a whole frame");
    assert!(buf.is_empty(), "the frame should have been consumed");
    decoded
}

proptest! {
    #[test]
    fn v5_packets_survive_a_round_trip(packet in any_packet(Version::V5)) {
        prop_assert_eq!(round_trip(&packet, Version::V5), packet);
    }

    #[test]
    fn v311_packets_survive_a_round_trip(packet in any_packet(Version::V311)) {
        prop_assert_eq!(round_trip(&packet, Version::V311), packet);
    }

    /// A frame delivered one byte at a time must decode exactly once, at the
    /// moment its final byte arrives, and never before.
    #[test]
    fn a_frame_split_across_reads_decodes_once_complete(
        packet in any_packet(Version::V5)
    ) {
        let mut whole = BytesMut::new();
        encode(&packet, Version::V5, &mut whole).expect("encodes");

        let mut fed = BytesMut::new();
        for (index, byte) in whole.iter().enumerate() {
            fed.extend_from_slice(&[*byte]);
            let outcome = decode(&mut fed, Version::V5, &Limits::default()).expect("no error");
            if index + 1 < whole.len() {
                prop_assert!(outcome.is_none(), "decoded early at byte {}", index);
            } else {
                prop_assert_eq!(outcome, Some(packet.clone()));
            }
        }
    }

    /// Arbitrary bytes must produce an error or a request for more, never a
    /// panic and never an infinite loop.
    #[test]
    fn arbitrary_bytes_never_panic(raw in prop::collection::vec(any::<u8>(), 0..512)) {
        let mut buf = BytesMut::from(&raw[..]);
        let before = buf.len();
        match decode(&mut buf, Version::V5, &Limits::default()) {
            Ok(Some(_)) => prop_assert!(buf.len() < before, "a decode must consume bytes"),
            Ok(None) => prop_assert_eq!(buf.len(), before, "a partial read must consume nothing"),
            Err(_) => {}
        }
    }

    /// Whatever the decoder accepts, the encoder must be able to write again.
    #[test]
    fn anything_decoded_can_be_encoded(raw in prop::collection::vec(any::<u8>(), 0..512)) {
        let mut buf = BytesMut::from(&raw[..]);
        if let Ok(Some(packet)) = decode(&mut buf, Version::V5, &Limits::default()) {
            let mut out = BytesMut::new();
            prop_assert!(encode(&packet, Version::V5, &mut out).is_ok());
        }
    }
}

/// These types are re-exported for downstream crates; touch them so an
/// accidental removal fails the build rather than a distant crate.
#[test]
fn public_surface_is_reachable() {
    let _ = ConnAck {
        session_present: false,
        reason: ReasonCode::Success,
        properties: Properties::default(),
    };
    let _ = SubAck {
        packet_id: 1,
        reasons: vec![ReasonCode::GrantedQos2],
        properties: Properties::default(),
    };
    let _ = UnsubAck {
        packet_id: 1,
        reasons: vec![ReasonCode::Success],
        properties: Properties::default(),
    };
}
```

- [ ] **Step 2: Run the test**

Run: `cd services/broker && cargo test -p mqtt-codec --test roundtrip`

This task adds tests over behavior that already exists, so there is no
failing-first step to perform. Two outcomes are possible and both are
useful. A compile error names a type the crate does not re-export, which
step 3 fixes. A test failure is a real defect in an earlier task: proptest
prints the shrunk input that broke it, and that input should become a case
in the malformed corpus in task 17 once the defect is fixed.

- [ ] **Step 3: Make the public surface match**

Confirm `services/broker/crates/mqtt-codec/src/lib.rs` ends with exactly these re-exports, adding any that are missing:

```rust
pub use error::CodecError;
pub use header::{FixedHeader, Limits, PacketKind};
pub use packet::{decode, encode, Packet};
pub use properties::Properties;
pub use qos::QoS;
pub use reason::ReasonCode;
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p mqtt-codec --test roundtrip`
Expected: PASS, six tests. Each property runs 256 generated cases by default.

- [ ] **Step 5: Commit**

```bash
git add services/broker/crates/mqtt-codec
git commit -m "test(codec): property-based round trips and panic freedom"
```

---

## Task 17: The malformed corpus and the fuzzer

The property test proves the decoder does not panic. This task proves it
returns the *right* error, which is what the broker turns into a reason code.

**Files:**
- Create: `services/broker/crates/mqtt-codec/tests/malformed.rs`, `services/broker/crates/mqtt-codec/fuzz/Cargo.toml`, `services/broker/crates/mqtt-codec/fuzz/fuzz_targets/decode.rs`, `services/broker/crates/mqtt-codec/fuzz/.gitignore`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Write the failing test**

Create `services/broker/crates/mqtt-codec/tests/malformed.rs`:

```rust
//! Malformed inputs and the exact error each must produce.

use bytes::BytesMut;
use mqtt_codec::{decode, CodecError, Limits, Version};

/// Each case is a name, the bytes, and the error the decoder must return.
fn cases() -> Vec<(&'static str, Vec<u8>, CodecError)> {
    vec![
        (
            "packet type zero is not assigned",
            vec![0x00, 0x00],
            CodecError::MalformedPacket("unknown packet type"),
        ),
        (
            "a fifth continuation byte in the length",
            vec![0x30, 0x80, 0x80, 0x80, 0x80],
            CodecError::MalformedPacket("variable byte integer longer than four bytes"),
        ),
        (
            "publish quality of service three",
            vec![0x36, 0x03, 0x00, 0x01, b'a'],
            CodecError::MalformedPacket("quality of service must be 0, 1 or 2"),
        ),
        (
            "duplicate flag at quality of service zero",
            vec![0x38, 0x03, 0x00, 0x01, b'a'],
            CodecError::MalformedPacket(
                "duplicate flag set on a quality of service zero publish",
            ),
        ),
        (
            "wildcard in a publish topic",
            vec![0x30, 0x03, 0x00, 0x01, b'#'],
            CodecError::InvalidTopicName,
        ),
        (
            "empty publish topic",
            vec![0x30, 0x02, 0x00, 0x00],
            CodecError::InvalidTopicName,
        ),
        (
            "subscribe without the required flag bits",
            vec![0x80, 0x00],
            CodecError::MalformedPacket("reserved flags in the fixed header are wrong"),
        ),
        (
            "packet identifier zero",
            vec![0x62, 0x02, 0x00, 0x00],
            CodecError::ProtocolError("packet identifier must not be zero"),
        ),
        (
            "trailing byte after a ping request",
            vec![0xC0, 0x01, 0xFF],
            CodecError::MalformedPacket("trailing bytes after the packet body"),
        ),
        (
            "a string that is not valid UTF-8",
            vec![0x30, 0x04, 0x00, 0x02, 0xFF, 0xFE],
            CodecError::InvalidUtf8,
        ),
        (
            "a string containing a null character",
            vec![0x30, 0x04, 0x00, 0x02, b'a', 0x00],
            CodecError::InvalidUtf8,
        ),
    ]
}

#[test]
fn every_malformed_input_produces_its_documented_error() {
    for (name, raw, expected) in cases() {
        let mut buf = BytesMut::from(&raw[..]);
        let actual = decode(&mut buf, Version::V5, &Limits::default());
        assert_eq!(
            actual,
            Err(expected),
            "case {name:?} produced the wrong outcome"
        );
    }
}

#[test]
fn an_oversized_declaration_is_rejected_before_the_body_arrives() {
    let limits = Limits {
        max_packet_size: 64,
        ..Limits::default()
    };
    // Declares a body of 300 bytes; supplies none of it.
    let mut buf = BytesMut::from(&[0x30u8, 0xAC, 0x02][..]);
    assert_eq!(
        decode(&mut buf, Version::V5, &limits),
        Err(CodecError::PacketTooLarge)
    );
    assert_eq!(buf.len(), 3, "nothing should have been consumed");
}

#[test]
fn a_topic_deeper_than_the_limit_is_rejected() {
    let limits = Limits {
        max_topic_depth: 2,
        ..Limits::default()
    };
    // PUBLISH to "a/b/c".
    let mut buf = BytesMut::from(&[0x30u8, 0x07, 0x00, 0x05, b'a', b'/', b'b', b'/', b'c'][..]);
    assert_eq!(
        decode(&mut buf, Version::V5, &limits),
        Err(CodecError::TopicTooDeep)
    );
}
```

- [ ] **Step 2: Run the test to verify it fails or passes**

Run: `cd services/broker && cargo test -p mqtt-codec --test malformed`
Expected: PASS if every earlier task produced the documented error strings.
If a case fails, the assertion prints the case name and both errors. Fix the
implementation to match the documented string, not the test to match the
implementation, unless the documented string is genuinely wrong.

- [ ] **Step 3: Write the fuzz target**

Create `services/broker/crates/mqtt-codec/fuzz/Cargo.toml`:

```toml
[package]
name = "mqtt-codec-fuzz"
version = "0.0.0"
edition = "2021"
publish = false
license = "MIT"

[package.metadata]
cargo-fuzz = true

[dependencies]
libfuzzer-sys = "0.4"
bytes = "1"
mqtt-codec = { path = ".." }

[[bin]]
name = "decode"
path = "fuzz_targets/decode.rs"
test = false
doc = false
bench = false

[workspace]
```

Create `services/broker/crates/mqtt-codec/fuzz/fuzz_targets/decode.rs`:

```rust
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use mqtt_codec::{decode, Limits, Version};

fuzz_target!(|data: &[u8]| {
    // Drain the buffer completely. Every iteration must either consume bytes
    // or stop, so a decoder that returns a packet without advancing shows up
    // as a hang rather than passing silently.
    for version in [Version::V311, Version::V5] {
        let mut buf = BytesMut::from(data);
        loop {
            let before = buf.len();
            match decode(&mut buf, version, &Limits::default()) {
                Ok(Some(_)) => {
                    assert!(buf.len() < before, "decode returned a packet without consuming");
                }
                Ok(None) | Err(_) => break,
            }
        }
    }
});
```

Create `services/broker/crates/mqtt-codec/fuzz/.gitignore`:

```
target/
corpus/
artifacts/
coverage/
```

- [ ] **Step 4: Verify the fuzz target builds and runs briefly**

Run:

```bash
cd services/broker/crates/mqtt-codec/fuzz && cargo +nightly fuzz run decode -- -max_total_time=60
```

Expected: the fuzzer runs for a minute and exits without a crash artifact.
If the nightly toolchain is absent, run `rustup toolchain install nightly`
and `cargo install cargo-fuzz --locked` first. This step is not part of the
pull-request gate; it runs on a schedule, so a missing nightly is a reason
to install it rather than to skip the step.

- [ ] **Step 5: Add the scheduled fuzz job to CI**

Append to `.github/workflows/ci.yml`:

```yaml
  fuzz:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@nightly
      - uses: taiki-e/install-action@cargo-fuzz
      - name: Fuzz the decoder
        working-directory: services/broker/crates/mqtt-codec/fuzz
        run: cargo fuzz run decode -- -max_total_time=600
      - name: Upload crash artifacts
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: fuzz-artifacts
          path: services/broker/crates/mqtt-codec/fuzz/artifacts/
```

And add the schedule trigger near the top of the same file, directly below
the `pull_request:` line:

```yaml
  schedule:
    - cron: "0 3 * * *"
```

- [ ] **Step 6: Run the whole suite one final time**

Run:

```bash
cd services/broker && cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test -p mqtt-codec
```

Expected: no formatting diff, no clippy warnings, every test passing.

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/mqtt-codec .github/workflows/ci.yml
git commit -m "test(codec): malformed corpus and a decoder fuzz target"
```

---

## Done when

- `cargo test -p mqtt-codec` passes, covering unit tests, the malformed
  corpus and the round-trip properties.
- `cargo clippy --all-targets -- -D warnings` is clean.
- `cargo deny check` passes against the allow list in `deny.toml`.
- The fuzz target runs for ten minutes without a crash.
- The codec crate depends only on `bytes` and `thiserror`.

## Next plan

`docs/superpowers/plans/` gains a second plan for the in-memory broker core:
the topic trie, the session actor, delivery and the quality-of-service state
machines, retained messages, wills, shared subscriptions, the keep-alive
ring, the TCP listener, and the conformance suite. It depends on this crate
and changes nothing in it.
