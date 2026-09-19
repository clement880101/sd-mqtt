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
| Charts | uPlot | MIT | 23.2 KB gzipped, zero deps, streaming is its design center |
| Sparklines | own SVG path | n/a | One canvas per table row does not scale |
| Table windowing | @tanstack/virtual-core | MIT | 7.1 KB, zero deps, genuinely framework-agnostic |
| Styling | plain CSS + Open Props tokens | MIT | 1 package and 2.2 MB against Tailwind's 32 and 45 MB |
| Form validation | valibot | MIT | 1.6 KB tree-shaken; plain zod costs 93 KB |
| UI tests | vitest + happy-dom + Playwright | MIT / Apache-2.0 | happy-dom has 7 direct deps against jsdom's 20 |
| Allocator | mimalloc, default on, all non-Windows | MIT | One build dependency; musl malloc is 7x to 700x slower |
| Rate limiting | own GCRA, ~130 lines | n/a | governor's keyed stores never evict on their own |
| Keep-alive timer | own 1-second bucket ring | n/a | A tokio Timeout is 896 bytes and the driver has a global mutex |
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

**musl's allocator is not merely slow, it is disqualifying.** On a six-core
machine the same workload takes 0.17s under glibc and 1.18s under musl, with
system time going from 0.37s to 6.13s and voluntary context switches from
1,196 to 199,786. The gap widens to roughly 700x on 48 cores. The cause is a
single global lock in musl's allocator, and a broker allocating per-packet
buffers across worker threads is the pathological case. mimalloc is chosen
over jemalloc on build friendliness rather than benchmarks: mimalloc needs
only a C compiler, so cargo-zigbuild compiles it with zig cc and nothing
else, while jemalloc runs an autoconf script that needs a shell, make and
correct host and target plumbing. mimalloc is also 23k lines of C against
jemalloc's 55k. It is default-on for every non-Windows target, not just
musl, so that development profiling reflects production. The musl release
profile adds mimalloc's local dynamic TLS flag, because musl does not
support initial-exec thread-local storage.

**governor does not solve the memory problem it appears to solve.** Its
keyed limiters expose retain_recent and shrink_to_fit, but the limiter never
calls them. A store keyed by IP address grows without bound until a janitor
task you wrote runs. Since we need that janitor either way, and since the
per-session publish cap needs no keying at all (the state is one 64-bit
field in a session struct we already have), governor would add roughly
fifteen crates including a concurrent map, a clock crate and three futures
crates to avoid about 130 lines. The per-IP map is a plain mutex-guarded
hash map swept by the same one-second tick task the keep-alive ring uses.

**Per-connection sleeps do not reach 100k connections.** A tokio Timeout
future measures 896 bytes against 2 bytes for the bare future, which is
about 90 MB of futures at 100k connections. The more serious problem is that
tokio's timer driver sits behind a global mutex and the sharding issue is
still open upstream, so rescheduling on every inbound packet means a
contended lock acquisition per packet. The ring avoids this entirely: a
packet arrival performs one relaxed atomic store of a coarse timestamp and
touches no timer at all. A single one-second interval task advances a cursor
over buckets, and when it reaches a connection it either disconnects it or
lazily re-arms it into a later bucket. That same task publishes a coarse
clock as an atomic, so neither the keep-alive sweep nor the rate limiters
ever ask the operating system for the time. Memory is 16 to 24 bytes per
connection, and a 60-second keep-alive sweeps about 1,100 entries per tick.

**The crate named timer-wheel does not exist.** An earlier draft listed it.
crates.io returns 404. The similarly named crates are different projects.

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

The project's own code is MIT. Third-party code is **not** all MIT, and we
never claim otherwise. The policy is MIT-compatible permissive, which is a
different and broader statement.

### Two categories with different obligations

**Distributed** code is anything that ends up in something a user receives:
Rust crates compiled into the binary, JavaScript libraries bundled into the
admin UI, and the small runtime helpers Vite injects into a production bundle
(module preload polyfill, dynamic import helpers), which are Vite's own MIT
code. This category carries real attribution obligations. Apache-2.0 requires
the license text and any NOTICE file to travel with the distribution.
BSD-2-Clause and BSD-3-Clause require the copyright notice and conditions.
ISC and Zlib require the notice. CC0-1.0 requires nothing but is recorded
anyway.

**Build-time only** code never reaches a user: Vite and its bundled
dependencies, vitest, Playwright, esbuild, cargo-deny, cargo-about, nextest,
criterion. Running a tool to produce an artifact does not distribute the
tool, so there is no attribution obligation here. We still run the license
check across this category, because a copyleft build tool can create
obligations in other ways and because a dependency can silently move from
one category to the other.

Vite itself illustrates the point. The published Vite package bundles
dependencies under Apache-2.0, BSD-2-Clause, CC0-1.0, ISC and MIT, and Vite
generates its own LICENSE file enumerating them for exactly this reason. None
of that reaches our users, because Vite is a devDependency and its bundled
tooling is not part of the build output. Only Vite's own MIT helper code is.

### Enforcement

`cargo-deny` gates the Rust tree in CI. The allow list is MIT, Apache-2.0,
Apache-2.0 WITH LLVM-exception, BSD-2-Clause, BSD-3-Clause, ISC, Zlib,
Unicode-3.0, CDLA-Permissive-2.0, CC0-1.0 and Unlicense. Anything outside it
fails the build. The same list is applied to the npm tree by a license check
in the UI package's CI job.

Known non-MIT entries in the current selection, all permissive and all
allowed: foldhash is Zlib, webpki-roots is CDLA-Permissive-2.0, subtle and
argh are BSD-3-Clause, hyper-rustls and rustls offer Apache-2.0 OR ISC OR
MIT, prometheus-client and Playwright are Apache-2.0, libfuzzer-sys carries
an NCSA component, and aws-lc-rs is ISC AND Apache-2.0 OR ISC.

### Attribution artifact

`THIRD-PARTY-LICENSES.md` is generated at release time, covering the
distributed category only, and ships alongside the binary and inside the
container image. `cargo-about` produces the Rust half from the actual
dependency graph of the release build, so it reflects enabled features
rather than a hand-maintained list. A small script produces the JavaScript
half from the UI's production bundle inputs. The file records each
component, its license identifier, its copyright line, and the full license
text once per distinct license. Build-time dependencies are listed in a
separate appendix marked as not distributed, for auditors who ask.

The README states that sd-mqtt is MIT licensed and that it includes
third-party components under other permissive licenses, with a pointer to
the attribution file. It does not say that every dependency is MIT.
