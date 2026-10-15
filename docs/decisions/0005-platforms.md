# 0005: Release artifacts for Linux and macOS, not Windows

**Status:** decided, milestone 7, 2026-10-15. Section 16's
"cross-compiled release artifacts".

## Decision

Each release ships four archives and one image:

| Target | Why |
|---|---|
| `x86_64-unknown-linux-musl` | servers; static, runs on any distribution |
| `aarch64-unknown-linux-musl` | ARM servers and boards; static |
| `x86_64-apple-darwin` | Intel Macs, for development |
| `aarch64-apple-darwin` | Apple silicon Macs, for development |
| container image, `linux/amd64` and `linux/arm64` | the musl binaries on `scratch` |

Linux builds are static musl, so the container needs no libc and a
binary copied to any distribution runs. Section 10.4's allocator is what
makes musl usable: its own allocator serialises on one lock.

**Windows is not built.** It has never been compiled or tested in this
project's CI, and section 10.4 already treats it as the platform without
mimalloc. Shipping a binary nobody has run would be a promise nobody
checked. Every artifact here is run before it is published, which is
what a Windows artifact would need first.

## What would reopen it

A request from someone who would run it, and a CI job that builds and
tests it on Windows on every push.
