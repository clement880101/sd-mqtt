# Component selection

Decisions for every third-party component in sd-mqtt, ranked on three
criteria in order: MIT-compatible permissive license, light dependency tree,
and capability at scale.

Facts were verified against crates.io, lib.rs, docs.rs, npm and GitHub on
2026-09-19. Where a fact could not be verified it is marked. Two areas
(UI charts/tables/styling and allocator/rate-limiting/timers) were decided
from first principles without independent verification; they are marked
UNVERIFIED and should be re-checked before the milestone that needs them.

## Summary table

| Area | Pick | License | Why |
|---|---|---|---|
| Async runtime | tokio | MIT | Only portable choice with the ecosystem; io_uring runtimes are Linux-only |
| Storage engine | fjall | MIT OR Apache-2.0 | LSM matches the queue workload; behind a `Storage` trait |
| Storage alternate | redb | MIT OR Apache-2.0 | 1 dependency; documented swap target |
| Cluster membership | own, over the TCP mesh | n/a | The mesh already exists for routing; zero new deps |
| Cluster metadata | own LWW + HLC anti-entropy | n/a | Gossip libraries cannot hold retained payloads |
| Cluster framing | postcard | MIT OR Apache-2.0 | 2 deps, published stable wire spec |
| HTTP server | axum, minimal features | MIT | Same hyper and tokio the broker already runs; SSE built in |
| HTTP client | hyper-util + hyper-rustls | MIT / Apache OR ISC OR MIT | Reuses the existing stack; we own retries anyway |
| TLS | rustls + tokio-rustls + aws-lc-rs | Apache OR ISC OR MIT | Fastest provider, no cmake for non-FIPS |
| WebSocket | tokio-tungstenite | MIT | Largest install base; deflate not needed for MQTT |
| JSON | serde_json | MIT OR Apache-2.0 | Admin payloads are small; SIMD parsers add unsafe and target gates |
| Buffers | bytes + tokio-util codec | MIT | Correct incremental framing with early size rejection |
| Hashing | foldhash | Zlib | Zero deps, seeded, DoS-resistant enough for client-controlled keys |
| Client registry | papaya | MIT | Read-dominated; lock-free reads, guards are Send |
| Topic trie | sharded parking_lot RwLock | MIT OR Apache-2.0 | Traversal needs consistency; per-node concurrent maps waste memory |
| IDs | uuid v7 | Apache-2.0 OR MIT | Sortable, zero required deps, universally parseable |
| Clocks | own HLC | n/a | ~80 lines; we control the wire encoding |
| CLI | clap, minimal features | MIT OR Apache-2.0 | Deliberate weight for usability; see note |
| Config | toml + serde + own env layering | MIT OR Apache-2.0 | ~30 lines beats a config framework |
| Logging | tracing + tracing-subscriber | MIT | Structured JSON; Targets filter, not env-filter |
| Metrics | prometheus-client | Apache-2.0 OR MIT | Official, light, no HTTP coupling |
| Passwords | argon2 + password-hash | MIT OR Apache-2.0 | OWASP recommendation, PHC strings |
| Asset embedding | build.rs + include_bytes! | n/a | Zero runtime deps, precompressed by Vite |
| UI runtime | preact + @preact/signals | MIT | ~8.3 KB gzipped, 1 transitive dep |
| UI router | preact-iso | MIT | 2.3 KB, zero deps |
| UI build | Vite 8 | MIT | Rolldown-based, 5 deps |
| UI live data | native EventSource | n/a | Auto-reconnect and Last-Event-ID for free |
| Charts | uPlot | MIT | UNVERIFIED. Smallest capable time-series canvas |
| Table windowing | own, ~100 lines | n/a | UNVERIFIED. Avoids needing preact/compat |
| Styling | plain CSS custom properties | n/a | UNVERIFIED. Zero deps |
| Allocator | mimalloc, feature-gated | MIT | UNVERIFIED. Required on musl; off elsewhere |
| Rate limiting | own token bucket | n/a | UNVERIFIED. ~40 lines beats governor's tree |
| Keep-alive timer | own 1-second bucket ring | n/a | UNVERIFIED. Per-connection sleeps do not scale to 100k |
| Errors | thiserror in libs, anyhow in binary | MIT OR Apache-2.0 | Typed at boundaries, ergonomic at the top |
| Tests | proptest, nextest, insta, criterion, cargo-fuzz | permissive | Standard set |
| Audit | cargo-deny, cargo-machete, cargo-bloat | permissive | Enforces the license policy in CI |
| Release | cargo-zigbuild, distroless static, cargo-dist | permissive | Static musl without Docker in CI |

## Notable findings that changed decisions

**bincode is discontinued.** Version 3.0.0 is a deliberate compile error and
the repository was archived in 2025. Its README points to postcard, which is
what we picked.

**foca and memberlist are MPL-2.0.** Both were leading candidates for cluster
membership and both are excluded by the license policy. chitchat is MIT and
production-proven at Quickwit, but it gossips over UDP with string values
sized to fit a datagram, and a node may only write its own namespace. That
makes it a poor home for retained messages and rules, which are global,
single-owner, and can be megabytes.

**We already have a TCP mesh.** Every node keeps a persistent link to every
other node for message forwarding. Failure detection over an existing TCP
connection is far simpler than SWIM over UDP: a closed socket is the signal,
and a heartbeat covers half-open connections. For the 10 to 50 node range
this design targets, a full mesh is 49 links per node at the top end. So
membership is our own code over the link we already maintain, and chitchat
is documented as the escape hatch if we ever need to pass roughly 100 nodes.

**rustls 0.24 will require an explicit crypto provider crate.** The provider
is exposed as a cargo feature so that migration is a one-line change.

**ring's unmaintained advisory was withdrawn** after the rustls team gained
commit access, but it has had no release in 18 months and lacks post-quantum
support. aws-lc-rs is the default; ring stays available as a feature for
exotic targets where a C compiler is a problem.

**musl's allocator is pathologically slow** under multithreaded async work,
with reported slowdowns of 7x and worse. Static musl builds therefore enable
mimalloc as the global allocator.

## Deliberate deviations from pure minimalism

**clap over lexopt.** lexopt is 37 KiB of binary against clap's 377 KiB in a
minimal configuration. The product's headline goal is ease of use, and the
broker has roughly fifteen subcommands whose help text, suggestions and
argument validation users will rely on. Hand-writing that is worse for users
and more code for us. The 340 KiB difference is about two percent of the
binary budget.

**fjall over redb.** redb's dependency tree is essentially libc, which is
lighter. fjall brings roughly forty transitive crates. The offline queue is
append-heavy with deletion from the head and values up to megabytes, which is
exactly the LSM shape: appends are journal writes with group commit, and
head deletions become tombstones that compaction drops in bulk. redb's
copy-on-write B-tree rewrites the path to the root on every commit and only
returns disk space on an explicit compaction call, which is awkward for a
process meant to run for months. Both sit behind the same `Storage` trait, so
a redb backend remains a supported build for anyone who wants the smaller
tree.

**aws-lc-rs over a pure-Rust provider.** It is a large C codebase. It is also
the rustls default, the fastest provider on every platform they benchmark,
and the only one with a FIPS path. Non-FIPS builds need a C compiler but not
cmake. graviola is the pure-Rust option worth revisiting once it matures.

## Rejected outright

| Component | Reason |
|---|---|
| sled | Stable line last released 2021; README still says beta; known space amplification |
| canopydb | README says do not trust it with production data |
| surrealkv | 21 required dependencies including tokio; single-vendor roadmap |
| rocksdb | ~480k lines of C++, minutes of build time, large binary and resident memory |
| flurry | README documents performance and memory problems under load |
| salvo | Apache-2.0 only |
| attohttpc, yawc | MPL-2.0 |
| foca, memberlist | MPL-2.0 |
| native-tls, openssl | Per-OS behavior differences; openssl-src bundles 27 MB of C and Perl |
| metrics-exporter-prometheus | Default features drag in hyper, tokio and a whole TLS stack |
| config-rs, figment | 13 and 8 direct dependencies for what 30 lines of env layering does |
| simd-json, sonic-rs | Unsafe and target-CPU gates for payloads that are never the bottleneck |
| rustc-hash | Not DoS-resistant, and client IDs and topics are attacker-controlled |
| pico-args, envy, speedy | Unmaintained |
| tiny_http, astra, may_minihttp | Dormant, or not tokio, or explicitly not production-grade |

## License policy

`cargo-deny` allows MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, Zlib,
Unicode-3.0, CDLA-Permissive-2.0 and Unlicense. Anything else fails CI.
`THIRD-PARTY-LICENSES.md` is generated at release time and shipped with the
binary, which satisfies the attribution obligations of Apache-2.0 and the
BSD family. The project itself is MIT.
