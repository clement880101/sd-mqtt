# Component selection

Decisions for every third-party component in sd-mqtt, ranked on three
criteria in order: MIT-compatible permissive license, light dependency tree,
and capability at scale.

Facts were verified against crates.io, lib.rs, docs.rs, npm, bundlephobia
and GitHub on 2026-09-19, including published tarballs where a repository's
license metadata was ambiguous. Every area has been verified. A few
individual figures remain estimates and are marked inline: mimalloc's exact
static binary cost, and snmalloc's musl support.

## Summary table

| Area | Pick | License | Why |
|---|---|---|---|
| Async runtime | tokio | MIT | Only portable choice with the ecosystem; io_uring runtimes are Linux-only |
| Storage engine | redb | MIT OR Apache-2.0 | One dependency; native head pop, flat as queues drain; behind a `Storage` trait |
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

**redb over fjall. This reverses the original choice, which was wrong.**

The original reasoning was that an offline queue is append-heavy with
deletion from the head and values up to megabytes, which is the shape a
log-structured merge tree is built for: appends are journal writes with
group commit, and head deletions become tombstones that compaction drops in
bulk. That last clause is true and irrelevant. Compaction does drop them in
bulk, eventually; **between compactions every head read walks all of them**,
because fjall's head read is a forward scan rather than a seek.

Measured on the same workload, a hundred thousand entries of 256 bytes
appended and then drained from the head:

| pops completed | fjall, first-key read | fjall, cursor read | redb `pop_first` |
|---:|---:|---:|---:|
| 10,000 | 1,357/s | 326,404/s | 1,479,199/s |
| 30,000 | 197/s | 286,455/s | 1,481,207/s |
| 100,000 | did not finish | 296,102/s | 1,467,934/s |

A persisted head cursor recovers most of fjall's loss, but redb needs no
workaround: it has a native remove-and-return-head, and it is flat.

The other three facts all point the same way. redb's dependency tree is
`libc` and nothing else, against forty-two for fjall. redb passes this
project's `cargo deny` unchanged, while fjall pulls in `xxhash-rust`
(BSL-1.0) and `varint-rs` (0BSD), both permissive and MIT-compatible but
both outside the allow list, so choosing it would mean amending the licence
policy. And redb spawns no background threads, where fjall's write path
contains unbounded loops of `std::thread::sleep` that must never run on an
async runtime's thread.

What redb gives up is key-value separation. It stores values in the tree, so
a multi-megabyte payload costs write amplification that fjall's separate
blob storage avoids. Message bodies are stored once and referenced by every
queue entry, so this is paid per message rather than per subscriber, and the
configured maximum packet size bounds it. It is recorded in the design's
open questions as a figure to measure rather than a risk to assume.

The concern that redb "only returns disk space on an explicit compaction
call" was overstated. Freed pages are reused, so the file is bounded by its
high-water mark; a drain-and-refill cycle held steady rather than growing.
Compaction reclaims the space and is a maintenance operation.

Both still sit behind the same `Storage` trait. Comparing two engines is
what found the head-read problem, so the trait is written as though a second
backend existed even though only one is built.

**aws-lc-rs over a pure-Rust provider.** It is a large C codebase. It is also
the rustls default, the fastest provider on every platform they benchmark,
and the only one with a FIPS path. Non-FIPS builds need a C compiler but not
cmake. graviola is the pure-Rust option worth revisiting once it matures.

## Verified again before milestone 3

Checked by building against them rather than by reading tables, because the
storage entry in this document was confidently wrong until it was measured.
Everything below was kept; what changed is what we know about it.

**axum is not nearly free, and this document implied it was.** The entry says
it reuses "the same hyper and tokio the broker already runs". The broker does
not run hyper: the lock file has no hyper, no http and no rustls, because TLS
is milestone 7. axum at milestone 3 pays for the whole stack up front, **22
new crates**, measured by diffing dependency trees. It is still the right
choice, because milestone 7 needs hyper regardless and the alternative is
writing HTTP by hand, but the reasoning was wrong. Measured cost to the
binary, with the project's release profile: **365 KB**, or 2.4% of the budget.

**Two new Apache-2.0 obligations enter the shipped binary with it.**
`sync_wrapper` is Apache-2.0 with no alternative in its expression. `ryu` is
`Apache-2.0 OR BSL-1.0`, and because BSL-1.0 is not on our allow list the
resolution is forced to Apache-2.0. Both are permissive and both pass
`cargo deny` unchanged, but the attribution file must carry Apache-2.0 for
them. This document previously listed `prometheus-client` as Apache-2.0; it
is `Apache-2.0 OR MIT` and may be taken as MIT.

**axum's default features cost two crates, not many.** The argument for
turning them off is therefore not weight. It is that `tower-log` routes
tower's internals through a second logging facade beside the tracing this
project standardised on, and that the form extractor widens the surface of a
JSON-only API. Keep the tracing feature; it costs nothing new.

**`prometheus-client`'s obvious update call takes a lock.** A metric family
is a read-write-locked map and the guard is held across the increment, so
incrementing on the packet path would put a shared lock acquisition on every
packet. The counter behind it is an atomic behind a reference count, so
resolving every label combination to an owned handle at startup makes the
increment a bare atomic add. The label cross-product is bounded and static,
so this costs nothing but saying so.

**Vite 8 is still current**, six months more mature, now bundling with
Rolldown by default. `build.rollupOptions` is renamed. Precompression is
still not built in, and is cheaper done in the build script that already
embeds the bundle than as a twelfth npm dependency.

**uPlot is the one thing to decide deliberately.** Its npm artifact is
eighteen months old while its repository had commits this week. It is not
abandoned; it has stopped publishing. We pin the published version and record
that any fix since is unreachable without pinning a commit, which is worse
for a shipped dependency because it has no version to audit.

**Preact 11 is at a release candidate.** We start on 10, which is stable and
what both the signals and routing packages resolve against.

**Attribution comes from `rollup-plugin-license` 3.7.1, MIT, pinned exactly.**
It reads the post-tree-shaking module graph, so a package installed but never
imported is correctly absent, which is the thing a node_modules walker cannot
do. Its declared peer range names Rollup and not Vite, so the compatibility
is empirical: pinned, with a build assertion that the notices file is
non-empty and names a package we know is in the bundle. The build fails on a
licence outside the allow list, and a separate step fails on any difference
between the regenerated file and the committed one.

Its one gap is that the bundler's own injected runtime helper has no package
to attribute, so a small companion emits the bundler's own notice when that
helper is present. Vite's full licence file is not copied, because most of it
covers Node-side tooling we do not distribute and copying it would
misrepresent what ships.

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

**This section described a release artifact that was never built.** It
said `THIRD-PARTY-LICENSES.md` was generated at release time by
`cargo-about`; that tool was configured nowhere, there is no release
workflow, the filename was in `.gitignore`, and the README pointed
readers at it. So the project shipped eighty-odd crates and five
browser libraries with no attribution of any kind, which milestone 3
made worse rather than better: `sync_wrapper` and `ryu` arrive through
axum, the only branch of either that this project's allow list accepts
is Apache-2.0, and Apache-2.0 asks for the license text to travel with
the work. Rewritten below to describe what exists.

**Nothing waits for a release.** The attribution is compiled into the
binary, because the binary is the artifact a user receives and a
release tarball is not the only way they receive it. `sdmqtt licenses`
prints it and the admin listener serves it at
`/third-party-notices.txt`; both are the same embedded bytes, put there
by `crates/admin/build.rs`.

Two halves, each committed so that a reader can see it without building
anything, and each checked in CI against the thing it was generated
from. They are **not** generated the same way, and the paragraph after
the two says which is which:

- **The crates.** `scripts/rust-notices.py` writes
  `services/broker/THIRD-PARTY-NOTICES.txt` from the packages reachable
  from `sdmqtt` along normal dependency edges, resolved per target and
  unioned over the targets this project builds for. Dev-dependencies,
  build-dependencies and procedural macros are absent: none of them is
  linked in. Each package's own license file is copied verbatim,
  deduplicated by text, so the one Apache-2.0 text appears once and a
  per-crate MIT copyright line keeps its own block. Where a package
  offers a choice, the branch taken is the cheapest one on `deny.toml`'s
  allow list — read from `deny.toml` rather than restated — which is
  what makes `ryu` an Apache-2.0 crate here.
- **The bundle.** `rollup-plugin-license` and the companion in
  `services/web/vite.config.ts`, unchanged in principle from what
  section 11.2 always asked for: generated from the post-tree-shaking
  module graph plus the stylesheets the bundler inlined.

**`cargo-about` is not used, and this is the deviation.** It would have
produced the first half from the same dependency graph, at the cost of a
second tool in every contributor's path and in three CI jobs. The script
needs only `cargo` and `python3`, which `scripts/budgets.sh` already
needs. What `cargo-about` would not do is read the binary at all:

**The list is generated from the graph and checked against the binary,
which is less than section 11.2 asks for.** Section 11.2's rule is
that the attribution comes from the artifact, and the bundle's half
obeys it: the module graph a bundler emits *is* the artifact. A Cargo
graph is not. So the crates half is generated from the manifest and
the binary is used for one assertion in one direction: rustc records
the source path of every panic site, so a crate that ships is usually
named in the bytes a user receives, and `scripts/rust-notices.py
--check` fails on any crate the binary names that the file does not.
The converse cannot be asserted — a crate can be linked in and leave
no path string, which `ryu` and `sync_wrapper` both do — so the check
finds an omission and never confirms the list is right. Calling that
"generated from the artifact" would be the flattering description of
it, and the script's own documentation has always said otherwise.

That one-way check is still what found the piece a manifest cannot:
the binary carries the Rust standard library, which arrives prebuilt
from the sysroot and appears in no Cargo graph. Five of its crates —
addr2line, gimli, object, rustc-demangle, hashbrown — are named in the
bytes. Nothing else the sysroot holds leaves a path string, whether it
is linked in or not, so the notices name the sysroot's whole contents,
read from the toolchain, and say plainly that this is an upper bound
on what the binary carries rather than a claim about each name. An
earlier version listed twelve crates as being in the binary, which
nothing had checked and five of which were all the bytes could show.
It is attributed in a section of its own, under the MIT branch of the
Rust project's MIT OR Apache-2.0.

Build-time dependencies are not listed. The category above says why:
running a tool to produce an artifact does not distribute the tool. An
appendix naming them was in the original of this section and is dropped
rather than written, because it would be a list nothing generates and
nothing checks, which is what the rest of this section is about.

The README states that sd-mqtt is MIT licensed and that it includes
third-party components under other permissive licenses, with a pointer
to both files and to the two ways of reading them out of the binary. It
does not say that every dependency is MIT.
