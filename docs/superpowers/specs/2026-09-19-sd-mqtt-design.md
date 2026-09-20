# sd-mqtt design

Status: approved 2026-09-19.

A distributed MQTT broker in Rust with a built-in admin UI and
UI-configurable integrations, distributed under the MIT license.

## 1. Goals

1. **Easy.** One binary, no external dependencies. `sdmqtt serve` with no
   configuration starts a working broker and opens an admin UI.
2. **Integrated.** Connect MQTT topics to HTTP services, and HTTP services to
   MQTT topics, by filling in a form. No code, no separate processes.
3. **Distributed.** Nodes join a cluster by naming a peer. Clients may
   connect to any node and reach subscribers on any other.
4. **Light.** Small binary, small resident memory, shallow dependency tree.
5. **Correct.** MQTT 3.1.1 and 5.0 conformance, verified against an
   independent interoperability suite.

## 2. Non-goals

- MQTT 3.1 (protocol level 3). It is rejected with an unacceptable protocol
  version response. Only 3.1.1 and 5.0 are served.
- Kafka-style consumer groups with replayable offsets. Shared subscriptions
  cover load balancing; message history is not a broker responsibility.
- Multi-region or wide-area clustering. The cluster targets one low-latency
  network. Bridges connect regions.
- A general plugin runtime in v1. Connectors are built in, on a hook
  interface designed so external plugins can be added later.
- Strong consistency for metadata. The cluster is eventually consistent by
  design, with the trade-offs stated in section 8.

## 3. Provenance and licensing

VerneMQ and Mosquitto were studied as reference implementations. Their
observable behavior and documented configuration are summarized in
`docs/research/vernemq-analysis.md` and
`docs/research/mosquitto-analysis.md`. No source code, comments, or
distinctive implementation structures from either project were copied or
translated. Where those analyses identify a behavior required by the MQTT
specification, that requirement is implemented independently from the
specification text.

The project is MIT licensed. Dependencies must be MIT-compatible permissive.
The full policy, including the distinction between distributed code and
build-time tooling, is in `docs/research/component-selection.md`.

## 4. Repository layout

```
sd-mqtt/
  .devcontainer/
    base.Dockerfile          Rust toolchain, Node, shared CLI tooling
    compose.yaml             root, broker, web services
    root/devcontainer.json
  .claude/skills/            shared repo skills
  .mcp.json
  services/
    broker/                  Cargo workspace
      crates/
        mqtt-codec/          packet types, encode, decode. No I/O.
        broker-core/         sessions, routing, retained, queues, hooks
        storage/             Storage trait, fjall backend, redb backend
        cluster/             membership, metadata replication, node link
        connectors/          rule engine and the four connector types
        api/                 HTTP API, SSE, metrics, health, UI serving
        sdmqtt/              binary: config, wiring, CLI
      .devcontainer/devcontainer.json
    web/                     Preact admin UI, built into the binary
      .devcontainer/devcontainer.json
  docs/
    research/                reference analyses, component selection
    superpowers/specs/       this document
    adr/                     decisions made during implementation
```

Crate dependencies point one way: `mqtt-codec` depends on nothing in the
workspace. `storage` depends on nothing in the workspace. `broker-core`
depends on both. `cluster` and `connectors` depend on `broker-core`. `api`
depends on all of them. `sdmqtt` wires them together.

## 5. Protocol and sessions

### 5.1 Codec

`mqtt-codec` is pure: bytes in, typed frames out, no I/O and no allocation
beyond what a frame owns. It exposes:

- `Frame`, an enum over the fifteen MQTT packet types, version-agnostic in
  shape, carrying v5 properties by value. An earlier draft said an
  `Option<Properties>`, which would shrink the common case where a packet
  has none; section 17 records why that was not done and what would decide
  it.
- `decode(buf: &mut BytesMut, version: Version, limits: &Limits) -> Result<Option<Frame>, CodecError>`,
  returning `Ok(None)` when more bytes are needed.
- `encode(frame: &Frame, version: Version, buf: &mut BytesMut) -> Result<(), CodecError>`.
- `CodecError`, a value type that maps to a reason code.

Decoding reads the fixed header, decodes the variable-length remaining
length incrementally across partial reads, and checks the total frame size
against the configured maximum **before** reserving the body buffer. Trailing
bytes inside a declared frame are a malformed-packet error. Strings are
validated as UTF-8 with no null characters. v5 properties are parsed by
identifier with per-property typing, and a property that is not permitted on
a given packet type is a protocol error.

One codec serves both versions. There is no second state machine for v5.

### 5.2 Session identity and lifecycle

A session is identified by `(mountpoint, client_id)`. Mountpoint is a
per-listener string, empty by default, that namespaces a listener's clients
into their own topic tree. This gives multi-tenancy at no structural cost.

Session state:

- subscriptions, each with QoS, no-local, retain-as-published and an
  optional subscription identifier. Retain handling is **not** kept: it
  says what to do with retained messages at the instant of subscribing,
  and once that instant has passed it has no further meaning.
- outbound inflight map, keyed by packet identifier
- outbound queue
- inbound QoS 2 packet identifiers awaiting release
- session expiry interval, will message and will delay
- an `epoch`, a monotonically increasing counter used for takeover

CONNECT is validated in this order: protocol version served by this build,
client identifier length and empty-identifier rules, authentication,
will topic authorized as an ordinary publish, then registration. Registration
is the point at which takeover happens.

**Takeover.** A CONNECT for a client identifier that already has a session
increments the epoch and claims the session on the new node. The previous
connection receives a v5 DISCONNECT with "session taken over" and is closed.
State transfers before the old socket dies. With clean start, the old session
is destroyed first. The old will fires only if the session was ending anyway.
Across the cluster this is coordinated through the session ownership record,
described in section 8.4.

**Keep-alive** is enforced at 1.5 times the negotiated interval. The
mechanism is a ring of one-second buckets, described in section 10.2.

### 5.3 Topic matching

Topic levels are interned into a `LevelId` on first use, so comparisons are
integer comparisons and nodes store fixed-size keys.

The subscription structure is a per-level trie. Matching visits literal,
single-level wildcard and multi-level wildcard children, in whatever order
the node finds cheapest. The order is deliberately unspecified: the result
is a set, the merge that consumes it is order-insensitive, and pinning an
order in prose invites a reader to treat an implementation detail as a
promise. A filter whose
first level is a wildcard never matches a topic whose first level begins with
a dollar sign, and a dollar-prefixed filter matches only dollar-prefixed
topics. Depth is bounded by configuration and checked during validation, so
traversal recursion is bounded.

The trie is sharded by the hash of the first topic level, each shard behind a
read-write lock. Publish takes read locks; subscribe and unsubscribe take
write locks and are comparatively rare.

Retained messages live in a separate trie with the same matching rules, plus
a periodic expiry sweep.

### 5.4 MQTT 5 features in scope for v1

Session expiry, message expiry, will delay, user properties, request and
response topics with correlation data, subscription identifiers, a
client's topic aliases, shared subscriptions, no-local,
retain-as-published, all three retain-handling values, server keep-alive,
receive maximum in both directions, maximum packet size, assigned client
identifier, and reason codes on every acknowledgement.

**Maximum QoS and Retain Availability move to milestone 7.** The broker
always supports quality of service 2 and always accepts a retained
message, advertising neither property, which is conformant. Capping either
is an operator policy and belongs with authorization rather than on its
own.

**Reason strings are not sent in v1.** The codec carries them and nothing
produces one. The reason code is the contract; a string is a convenience
for a human reading a packet capture, and it costs bytes on every
acknowledgement.

**The server does not itself use topic aliases outbound.** An earlier
draft of this section promised aliases "in both directions". 3.3.2.3.4
permits a server to use them and requires nothing, while the inbound half
is the one whose absence loses messages, so the inbound half is built and
the outbound half is not. A client that advertises a Topic Alias Maximum
is simply never sent an alias, which is conformant. Revisit it with the
packet-sizing question in section 17, since the two trade against each
other: an alias shortens every publish after the first on a topic.

Enhanced authentication (the AUTH exchange) is in scope, with SCRAM-SHA-256
as the shipped mechanism. See section 12.2.

## 6. Delivery, queues and ordering

Each session's outbound path has two stages.

**Inflight** holds messages sent and awaiting acknowledgement. Its size is
the lesser of the configured maximum and the peer's receive maximum. Default
is 20. Setting it to 1 gives strict ordering.

**Queue** holds messages waiting for an inflight slot, bounded by both a
message count and a byte total, both configurable. An earlier draft
specified separate bounds for the online and offline cases; one set is
built, because nobody stated the problem two sets solve and a bound an
operator cannot reason about is worse than a bound that is the same
everywhere. Revisit it when persistence makes the offline case cost disk
rather than memory.

**No retransmission on a live connection.** MQTT 5 forbids it and TCP makes
it pointless. On session resume, inflight messages are requeued with the
duplicate flag and **their original packet identifiers**, and queued
messages are promoted into inflight slots.

Keeping the identifier is not a detail. A peer that received the first copy
and lost its acknowledgement is waiting on that identifier; a redelivery
under a new one looks like an unrelated message and is delivered twice,
breaking exactly-once by an ordinary dropped connection. The old identifier
is also stranded on the peer forever, so when the broker's counter wraps
onto it, a genuinely new message is discarded as a duplicate. One mistake,
both duplication and loss.

Resume is also not uniform. A message whose receipt has already arrived is
resumed by resending the release, not the publish, because the peer has
taken ownership of it.

**Overflow** is `drop_newest` for every queue in v1. Both policies are
implemented and tested, and nothing selects between them yet; making it
configurable waits for someone to want the other one. Every drop carries a
reason, and from milestone 3 increments a counter labelled with it and
fires the message-drop hook. Nothing is discarded silently.

**Packet identifiers** are allocated per session from a monotonic counter
wrapping through 1 to 65535, skipping identifiers currently inflight.

**Message identifiers** are UUID v7, which sorts by time and is unique
across nodes without coordination.

**Queue sequence numbers.** Every queue operation for a session carries a
sequence number from a per-session monotonic counter. The queue is therefore
a log, which is what makes replication in section 8.6 tractable: a replica
reports the highest sequence it has applied, and a recovering owner can ask
which replica is furthest ahead. Sequence numbers exist from milestone 2 so
that replication is additive rather than a retrofit.

**Shared subscriptions** on `$share/<group>/<filter>` balance across the
cluster. Selection prefers a local online member, then the least loaded
remote node, round-robin within a node. A member whose queue is full is
skipped and the next is tried. Each member keeps its own queue; there is no
shared group queue.

**A retained publish with an empty payload removes the retained message**
for that topic. It is not stored, and it is not sent to future subscribers,
but it is delivered to subscribers matching at the time. This is easy to
miss because the codec correctly treats an empty payload as ordinary.

## 7. Persistence

### 7.1 The Storage trait

`storage` defines a trait covering:

- durable session records, subscriptions and inbound QoS 2 state
- queue append, head read, head delete, depth, and the highest applied
  sequence number per session
- replica queues for sessions this node does not own, with the same
  operations plus an apply-remote-operation path
- message bodies stored once, referenced by queue entries, with a persisted
  reference count
- retained messages
- cluster metadata records with their version stamps
- users with both an argon2 password hash and a SCRAM verifier, access
  control rules, rules, API keys, node identity

The default backend is fjall, chosen because the queue workload is appends
with deletion from the head and values up to megabytes, which is the shape a
log-structured merge tree is built for. A redb backend is maintained as a
supported alternative for anyone who wants the smaller dependency tree, and
its existence keeps the trait honest.

### 7.2 Durability

Writes are applied as they happen, not snapshotted periodically. A batching
writer coalesces operations and flushes on whichever comes first: a
configurable interval in the low milliseconds, a configurable operation
count, or a QoS 1 or 2 acknowledgement that must not be lost. The sync mode
is configurable so an operator can trade durability for throughput
deliberately.

Restore runs as the first startup phase, before any listener binds.

Reference counts are persisted alongside the bodies they protect, not
rebuilt by scanning at startup.

## 8. Clustering

### 8.1 Membership

Each node has a stable identity persisted on first start: a UUID v7 and a
human-readable name. A node joins by being given one or more seed addresses.

Nodes form a full TCP mesh. At the design ceiling of 50 nodes that is 49
links per node, which is negligible. The same links carry message forwarding,
metadata replication and heartbeats, so there is one transport, one
authentication step and one failure model.

Failure detection uses the link itself. A closed socket is an immediate
signal. A heartbeat every second covers half-open connections; a peer is
suspect after three missed beats and dead after ten seconds, both
configurable. Reconnection uses exponential backoff with jitter.

The link is authenticated by a shared secret and may be wrapped in TLS.

Membership beyond roughly 100 nodes would need gossip rather than a full
mesh. chitchat is the documented escape hatch. It is not needed now and
carries a UDP transport and a C compression dependency we would rather not
take.

### 8.2 Metadata replication

Replicated metadata comprises subscriptions, retained messages, rules, users,
access control rules, API keys and session ownership.

Each record is a last-writer-wins register stamped with a hybrid logical
clock reading and the writing node's identity, which breaks ties
deterministically. Writes broadcast immediately to all peers. Every few
seconds, with jitter, each node exchanges a digest of per-node version
counters with a random peer and pulls whatever it is missing, in batches.
This repairs anything a broadcast lost.

Deletions are tombstones, garbage collected after a grace period long enough
to exceed any plausible partition, tracked by a cluster-wide low watermark.

There is no consensus protocol and no external coordination service.

### 8.3 Routing

Each node holds the full subscription trie for local subscribers, plus a
separate map from filter to the set of remote nodes that have at least one
matching subscriber, with reference counts.

A publish matches locally, enqueues to local sessions, and forwards **one**
copy to each remote node that has any match. The receiving node matches
against its own local subscribers. Fan-out cost is therefore proportional to
the number of interested nodes, not the number of subscribers.

Frames on the link are a 32-bit little-endian length, a one-byte protocol
version, then a postcard-encoded message. Writes are batched and flushed on
a size or time threshold. The outgoing buffer per peer is bounded; beyond the
bound, messages to that peer are dropped and counted, and the condition is
surfaced in the UI rather than hidden.

### 8.4 Session ownership and migration

A session is owned by exactly one node, recorded as a metadata register
holding the owning node and the session epoch.

When a client connects to a node that does not own its session, that node
increments the epoch, writes itself as owner, broadcasts the change, asks the
previous owner to close the old connection and drain the offline queue to it,
and waits for the drain up to a bounded timeout before sending CONNACK.
Subscriptions follow the session.

### 8.5 Partitions

The default policy is availability: a partitioned node keeps serving its
clients. Per-operation flags allow an operator to refuse connects, publishes
or subscribes while the cluster is not whole, for deployments that prefer
that trade.

During a partition, two nodes can accept the same client identifier. On heal,
the higher epoch wins and the loser's connection is closed with "session
taken over". Retained messages and rules converge by last writer wins.
Messages published on either side during the partition are not retroactively
exchanged.

A node that accepted and acknowledged messages on the losing side of a
partition, and that never managed to replicate them, loses those messages
when it rejoins. Synchronous replication shrinks this window to nothing for
any publish that reached a replica, because such a publish is never
acknowledged until it did. Eliminating the window entirely would require
consensus, which this design deliberately does not take on.

### 8.6 Queue replication

Offline queues are replicated, so losing a node does not lose the durable
messages of the sessions it owned.

**Placement.** Each session has a replica set of size R, default 2 and
configurable from 1 to 3, chosen by rendezvous hashing of the session
identity across live members. The highest-weighted member is the owner and
the next R-1 are followers. Rendezvous hashing is deterministic, needs no
coordination, and moves only the sessions that must move when membership
changes. With fewer than R live members the set is as large as membership
allows, and is repaired when members return.

**Streaming.** The owner sends each queue operation, as an append, an
acknowledgement or a removal, to its followers over the existing node link,
tagged with the session sequence number. Followers apply operations in order
and report their high-water mark. Message bodies travel once and are
reference counted on the follower exactly as on the owner.

**Durability modes.** Configurable globally and overridable per listener.

- `async`, the default. The publisher is acknowledged as soon as the owner
  has the message durably. Replication proceeds in the background. The loss
  window is the replication lag, which is exposed as a metric and alerted on.
- `sync`. A QoS 1 or 2 publish to a durable session is not acknowledged
  until at least one follower confirms the append. A single node failure
  loses nothing. The cost is one local network round trip per durable
  publish.

The code path is the same in both modes. Synchronous mode awaits the
confirmation that asynchronous mode ignores.

**Failover.** When a session's owner is declared dead, or a client
reconnects elsewhere, the new owner asks every reachable member of the
replica set for its high-water mark, pulls from the most advanced one,
becomes authoritative, and resumes streaming to the remaining followers.
Ownership is recorded in the same epoch-stamped register that governs
takeover, so a stale owner returning from a partition loses and discards its
copy.

**Re-replication.** When a member leaves permanently, the replica sets that
included it are short. Repair is rate limited and runs in the background so
that losing one node does not saturate the cluster, and its progress is
visible in the UI.

**Cost.** With the default replication factor, each node holds roughly twice
the durable queue data it would otherwise, and durable queue writes generate
proportionate inter-node traffic. This is the price of the guarantee and is
stated in the budgets.

## 9. Sidecars: rules and connectors

### 9.1 The rule model

A rule has an identifier, a name, an enabled flag, a trigger and one action.

The trigger is a topic filter, optionally narrowed by minimum QoS and a
client identifier pattern.

Rules are replicated metadata, so a rule created in the UI on any node
applies everywhere. A message triggers a rule on the node where it enters the
cluster, exactly once.

### 9.2 Actions

**http_out** posts a JSON envelope to a URL. The envelope carries the topic,
the payload as a UTF-8 string when valid and base64 otherwise, QoS, the
retain flag, the publishing client identifier, a timestamp, and v5 user
properties. Configurable: method, headers, timeout, maximum concurrency,
retry count and backoff. Each rule owns a durable outbox, so a restart loses
nothing in flight. After the final attempt an envelope is published to
`$sd/rules/<id>/dlq` and surfaced in the UI.

**http_in** exposes `POST /ingest/<rule-id>`, guarded by a per-rule bearer
token. Which listener serves it is configuration, and it defaults to a
separate ingest listener rather than the admin listener, so that publishing
data does not require exposing the administrative surface. Running it on the
admin listener is permitted but must be chosen explicitly. The body is published to a configured topic at a
configured QoS and retain setting. The topic may instead be taken from a
named header or query parameter, restricted by a prefix so a caller cannot
publish anywhere.

**http_poll** issues a GET on an interval and publishes the response body to
a topic. It runs on exactly one node, selected as described below.

**mqtt_bridge** connects to a remote broker as a client. Topic rules carry a
direction of in, out or both, a QoS, and a topic rewrite. Rewriting supports
a prefix on either side, and for the outbound direction a template that
captures wildcard segments from the matched filter and substitutes them by
position, so `sensors/+/temp` can be published remotely as
`site-a/temp/{1}`. Inbound messages need no such facility, because a bridge
publishes them into the local broker where any rule can act on them. Reconnection uses exponential backoff with jitter. A state
topic reports connectivity, with a will supplying the offline value. Loop
prevention uses an origin identifier carried in a user property, and messages
that have already traversed this bridge are not sent back.

Bridges and pollers are singletons. The owner is the live member with the
lowest node identifier, recomputed on membership change. This is
deterministic, needs no election protocol, and at worst duplicates work for
the moments during which membership views disagree.

### 9.3 The hook interface

Connectors are implemented on an internal hook interface rather than being
special-cased in the publish path. Hooks fire on authenticate, authorize
publish, authorize subscribe, message received, message delivered, message
dropped, client connected, client disconnected and session expired.

An authorizing hook returns allow, deny or next, where next passes the
decision along the chain. If every hook returns next, the configured default
applies. Message hooks may rewrite topic, payload, QoS, retain and
properties, or drop the message.

This is deliberately the shape an external plugin transport would need, so
v2 can add out-of-process hooks without disturbing the core.

## 10. Runtime design

### 10.1 Task structure

Connection and session are separate. A connection task owns the socket and
the codec. A session actor owns subscriptions, the queue and the inflight
window, and outlives the connection when the session is durable. They
communicate over a bounded channel, so a slow consumer applies backpressure
rather than growing without limit.

The client registry maps client identity to session handle. It is read on
every delivery and written only on connect and disconnect, so it uses a
concurrent map with lock-free reads.

### 10.2 Time

A single one-second interval task drives three things: it advances the
keep-alive ring, it publishes a coarse monotonic clock as an atomic, and it
sweeps the rate-limiter maps.

The keep-alive ring is a vector of buckets sized to the maximum keep-alive
times 1.5, plus slack, with a cursor. A connection appears in one bucket. An
arriving packet performs a single relaxed atomic store of the coarse clock
into the connection's last-activity field and touches no timer at all. When
the cursor reaches a bucket, each entry is checked: if the client has been
silent past its limit it is disconnected, otherwise it is re-armed into a
later bucket. Dead entries are tombstoned and skipped.

This avoids both problems with per-connection timers: a tokio timeout future
is 896 bytes, which is roughly 90 MB at 100,000 connections, and the timer
driver serializes on a global lock that would be taken once per packet.

### 10.3 Rate limiting

Two limiters, both implementing the generic cell rate algorithm against the
coarse clock, so neither ever asks the operating system for the time.

The per-session inbound publish cap is a single 64-bit field in the session
struct. There is no map lookup on the hot path.

The per-address authentication failure limiter is a mutex-guarded map swept
by the one-second task, dropping entries older than the window. Authentication
failures are rare, so a single mutex is adequate.

### 10.4 Memory

mimalloc is the global allocator on every non-Windows target, enabled by
default and removable with a cargo feature. Static musl builds additionally
enable its local dynamic TLS mode. The reason is that musl's allocator
serializes on a single global lock and loses by a factor of seven on six
cores and far more on larger machines.

## 11. HTTP API, UI and observability

### 11.1 API

The admin listener serves a REST API, a server-sent events stream, the
Prometheus endpoint, health endpoints and the embedded UI.

Resources: nodes, listeners, clients, sessions, subscriptions, retained
messages, rules, rule runs and dead letters, users, access control rules,
API keys, and a publish endpoint. Mutating requests require an API key with
an appropriate scope, or a UI session cookie.

Health is tiered. Liveness answers whether the process is running. Readiness
answers whether listeners are bound and storage is open. A cluster endpoint
answers whether this node's view of membership is complete, so an operator
can choose which one gates traffic.

The events stream pushes counter deltas, connect and disconnect events, and
rule outcomes. Events carry identifiers so a reconnecting browser resumes
with the last-event header.

### 11.2 UI

Pages: Overview with live rate charts, Clients with a filterable virtualized
table, Topics with a retained browser and a live publish and subscribe tool,
Rules with a per-action form editor and per-rule statistics and dead letters,
Cluster with member and link health, and Settings for users, access control,
API keys and listeners.

Built with Preact and signals, routed by preact-iso, bundled by Vite,
charted by uPlot with hand-drawn SVG sparklines in table rows, virtualized
by the vanilla TanStack virtual core, validated by valibot, and styled with
hand-written CSS over Open Props tokens. Live data arrives over the events
stream.

The production bundle is precompressed at build time and embedded into the
binary by a build script, served with content negotiation. In development
the broker proxies to the Vite dev server.

### 11.3 Metrics

Counters for packets by type, version and reason code; queue events by
reason including drops and expiries; socket lifecycle; bytes in and out;
authentication outcomes; cluster bytes sent, received and dropped; and
router matches split local and remote. Gauges for connections, sessions,
subscriptions, retained messages, queue depth and cluster members.

Every discard path has both a metric and a hook. A message never disappears
without being counted.

## 12. Security

### 12.1 Authentication and access control

Authentication is anonymous, or username and password with argon2 hashing
and PHC-formatted storage, or SCRAM-SHA-256. Access control rules map a user or client pattern
to topic filters with publish and subscribe permissions, deny by default once
any rule exists. TLS and WebSocket listeners are supported, with certificate
hot reload through a certificate resolver so renewal needs no restart.

### 12.2 Enhanced authentication

SCRAM-SHA-256 is implemented per the SASL SCRAM specification and its
SHA-256 profile, carried over MQTT 5's authentication exchange. The client
names the mechanism in CONNECT and puts its first message in the
authentication data. The broker replies with an AUTH packet carrying the
server first message, the client answers with an AUTH packet carrying its
proof, and the broker completes with CONNACK carrying the server signature.
Re-authentication mid-session uses the same exchange initiated by the client.

Channel binding, the `-PLUS` variant, is not implemented in v1. The broker
handles the client's channel-binding flag correctly and advertises only the
unbound mechanism.

A user may hold both credential forms. They are computed together when a
password is set, because a SCRAM verifier cannot be derived from an argon2
hash: it requires the password itself. The verifier stores a salt, an
iteration count, and the derived stored and server keys, never the password.
The iteration count is configurable and defaults to a value chosen to balance
security against constrained clients, which must perform the same derivation.

A SCRAM verifier is a weaker secret at rest than an argon2 hash, because
possession of the stored key permits impersonating that client. Verifiers are
therefore treated as secrets equivalent to private keys, are never returned
by the API, and exist only for users explicitly enrolled in SCRAM.

A client requesting any other mechanism receives the bad authentication
method reason code.

### 12.3 Secret handling

Passwords and tokens are held in a wrapper that refuses to print itself, so
they cannot reach logs or crash output. Repeated authentication failures from
one address are slowed by the limiter in section 10.3. Dollar-prefixed topics
are not publishable by clients.

## 13. Error handling

One reason-code enum spans the whole system, encoded per protocol version.
A v3.1.1 client that would receive a reason code it cannot express gets the
closest legacy code, or a socket close where the specification provides
nothing.

Malformed packets before CONNACK close the socket. Afterwards they produce a
v5 DISCONNECT with a specific reason. Limits are enforced with per-client and
per-listener budgets that reject early with a reason code, rather than a
global memory ceiling that surfaces as unexplained failure.

Library crates expose typed errors. The binary uses a context-carrying error
type at the top level and converts at the boundary.

## 14. Testing

- **Codec**: property-based round-trip tests, plus a corpus of malformed
  inputs asserting the exact reason code produced. Fuzzed with a coverage
  fuzzer.
- **Core**: session, queue and routing tests over an in-memory transport,
  with no sockets involved.
- **Integration**: a test client built on our own codec drives a real
  listener. Using our own codec keeps the dependency tree clean and produces
  a client library as a by-product.
- **Cluster**: three nodes in one process over an in-memory link, covering
  takeover, migration, partition and heal.
- **Replication**: the same harness kills an owner mid-queue and asserts
  that synchronous mode loses nothing, that asynchronous mode loses only
  what the lag metric predicted, that failover selects the most advanced
  replica, and that a stale owner returning from a partition discards its
  copy. Re-replication is asserted to respect its rate limit.
- **Authentication**: the SCRAM exchange is tested against vectors from the
  specification, including a failed proof, a mid-session re-authentication,
  and a client requesting an unsupported mechanism.
- **Connectors**: a stub HTTP server asserts envelope shape, retry timing,
  outbox durability across restart, and dead-lettering.
- **Conformance**: an independent MQTT interoperability suite runs in CI.
- **UI**: component tests under vitest with happy-dom, and one end-to-end
  smoke test against a running binary.

Verification is evidence-based. A milestone is complete when its tests pass
in CI, not when the code is written.

## 15. Budgets

| Metric | Target |
|---|---|
| Release binary, all features, stripped, link-time optimized | under 15 MB |
| Idle resident memory, single node, no clients | under 30 MB |
| UI bundle, gzipped | under 150 KB |
| Direct dependencies across broker crates | under 35 |
| Concurrent connections per node, target hardware | 100,000 |

Durable queue storage and inter-node traffic scale with the replication
factor. At the default factor of 2, plan for roughly twice the queue disk a
single node would need, and inter-node bandwidth proportionate to durable
publish volume.

Budgets are checked in CI. Exceeding one fails the build and requires either
a fix or an explicit, recorded decision to move the budget.

## 16. Milestones

Each milestone gets its own implementation plan and is independently
shippable.

1. **Codec and core.** MQTT 3.1.1 and 5.0 over TCP, in memory only. Sessions,
   QoS 0, 1 and 2, retained messages, wills, shared subscriptions, keep-alive
   ring. Exit criteria: a single node passes the conformance suite over plain
   TCP, with the suite's TLS and WebSocket sections deferred to milestone 7.
   The suite is chosen and pinned during this milestone.
2. **Persistence.** The storage trait, the fjall backend, durable sessions and
   queues with per-session sequence numbers, restore on start. Exit criteria:
   a broker restart preserves durable sessions, their subscriptions and their
   queued messages, and queue operations carry gapless sequence numbers.
3. **API, metrics and UI shell.** HTTP API, events stream, Prometheus, health,
   and the Overview, Clients and Topics pages. Exit criteria: the UI shows
   live clients and retained messages against a running broker.
4. **Rules and connectors.** The rule engine, four connector types with
   outbound topic templates, the durable outbox, dead lettering, and the Rules
   page. Exit criteria: a message published to MQTT reaches an HTTP endpoint,
   and an HTTP post reaches an MQTT subscriber, both surviving a broker
   restart mid-flight.
5. **Clustering.** Membership, metadata replication, routing, session
   ownership and migration, and the Cluster page. Exit criteria: a three-node
   cluster delivers across nodes, migrates a session on reconnect, and
   converges after a partition heals.
6. **Queue replication.** Rendezvous placement, the replication stream,
   asynchronous and synchronous modes, failover to the most advanced replica,
   and rate-limited re-replication. Exit criteria: killing the owner of a
   session with a non-empty offline queue loses no messages in synchronous
   mode, and the replication lag metric bounds the loss in asynchronous mode.
   The UI shows replica health and re-replication progress.
7. **Hardening.** TLS with certificate hot reload, WebSocket, SCRAM-SHA-256,
   the authentication and access control UI, rate limits, a configurable
   maximum quality of service and retain availability, the container image,
   cross-compiled release artifacts and documentation. Exit criteria: all
   budgets met, the full conformance suite green including TLS and WebSocket,
   a SCRAM client authenticates and re-authenticates, and a release is
   published.

## 17. Open questions

- **A contradiction in the MQTT 5 specification, resolved permissively.**
  The reason-code table for DISCONNECT omits bad-authentication-method,
  while the cross-reference table listing which packets may carry each code
  includes it for DISCONNECT. Both published copies of the standard say
  this, and OASIS has issued no errata. The codec accepts it, because the
  re-authentication section tells a peer whose re-authentication fails to
  disconnect with an appropriate reason code and this is the only code that
  describes that failure. If a conformance suite ever rejects it, this is
  the line to revisit, and the stricter reading is a one-line change.

- **The single lock above the broker, measured.** The broker's methods take
  an exclusive reference, so the shell serialises every packet in the
  process through one mutex. Milestone 1 measured it rather than fixing it.
  The harness is `services/broker/crates/server/tests/throughput.rs`, three
  ignored tests run with
  `cargo test -p sdmqtt-server --release --test throughput -- --ignored
  --nocapture --test-threads 1`. Machine: Apple M2 Pro, 12 logical cores,
  32 GB, macOS 26.4.1, arm64, release profile, loopback TCP, MQTT 5,
  at-most-once, 32-byte payloads, one subscriber per publisher on its own
  topic, three-second window after a one-second warm-up.

  Closed loop, 512 messages outstanding per pair, nothing discarded:

  | publishers | total msg/s | per publisher |
  |---:|---:|---:|
  | 1 | 452,000 | 452,000 |
  | 2 | 349,000 | 175,000 |
  | 4 | 241,000 | 60,000 |
  | 8 | 183,000 | 23,000 |
  | 16 | 220,000 | 14,000 |

  **Throughput falls rather than flattens.** Sixteen publishers move less
  in total than one does. A thousand extra idle connections changed no
  figure by more than noise (447,000 / 354,000 / 245,000 / 183,000 /
  217,000), so an idle connection costs nothing per message.

  The same load with no sockets at all, one `Shared` for everyone versus
  one `Shared` per caller, which is the control that says whether the lock
  or the runtime is responsible:

  | callers | one broker, msg/s | one broker each, msg/s | ratio |
  |---:|---:|---:|---:|
  | 1 | 515,000 | 519,000 | 1.01 |
  | 2 | 400,000 | 901,000 | 2.26 |
  | 4 | 294,000 | 1,436,000 | 4.89 |
  | 8 | 289,000 | 1,318,000 | 4.56 |
  | 16 | 293,000 | 1,195,000 | 4.08 |

  Identical work, identical task counts, identical runtime; the only
  difference is whether the mutex is contended. Sharded, it scales to
  about 1.4 M msg/s and saturates on cores. Shared, it peaks at one caller
  and settles around 290,000.

  One tick with ten thousand sessions: 30 µs median, 27 µs minimum, 108 µs
  maximum over fifty ticks. `Broker::tick` scans the registry twice per
  tick, so this is linear in sessions and would be roughly 300 µs at the
  hundred thousand the design targets — a pause of that length, once a
  second, for every connection in the process.

  No conclusion is drawn here. The figures exist so that milestone 2
  decides from a measurement.

- **Contention in the matching layer.** The subscription trie is sharded
  by first topic level, but the retained store is one lock, so every
  retained publish serialises broker-wide and an expiry sweep holds that
  lock for the whole tree. Shared-subscription selection also takes one
  write lock across all groups, because it advances a per-group cursor;
  per-group atomic cursors under a read lock would fix it. Neither is worth
  designing before the session layer's access patterns exist, and neither
  is on a path a benchmark currently covers.

- **The action enum's size.** A session returns a list of actions, and
  the variant carrying a packet is around 400 bytes, which trips the
  compiler's large-variant lint. Boxing the packet silences it at the cost
  of one allocation per message sent, which is the same allocation the
  codec deliberately declined when it left the publish packet unboxed. The
  lint is suppressed with that reasoning recorded. Like the two questions
  below, the trade wants a benchmark rather than an argument.

- **Zero-copy payloads.** The codec's reader copies every payload out of
  the connection's read buffer. Since decoding already owns that buffer
  mutably, splitting and freezing it instead would remove one full payload
  copy per message on the hottest path, and the publish payload is already
  the right type for it, so the change is not breaking. Like the question
  below, it wants a benchmark rather than a guess.

- **Packet size on the hot path.** The MQTT 5 property block is one struct
  of 27 mostly-unset optional fields, around 344 bytes, carried by value
  inside every packet. Making it an optional boxed value would shrink the
  common no-properties case to a pointer, at the cost of an allocation when
  properties are present. The trade depends on how often real traffic
  carries properties, so it needs a benchmark rather than a guess. Decide
  during the broker core work, where benchmarks exist. The codec crate boxes
  only the connect packet today, on the reasoning that it arrives once per
  connection while a publish is the hot path and already owns two heap
  allocations.


None of these block milestone 1. Each is resolved by the milestone that
needs it, and the resolution is recorded as an architecture decision record.

- **Replication factor default.** The spec sets 2. Whether 3 should be the
  default for clusters above some size is a milestone 6 question, and it
  depends on how expensive re-replication turns out to be in practice.
- **Sync mode granularity.** Durability mode is specified as global with a
  per-listener override. Whether it should also be settable per session, via
  the authenticate hook, is a milestone 6 question. It is cheap to add and
  should only be added if a real deployment wants it.
- **SCRAM iteration count default.** The trade-off is between resistance to
  offline attack and the work a constrained client must perform, since the
  client runs the same derivation. Milestone 7 should pick a number against
  measurements on representative hardware rather than guessing now.
- **Channel binding.** SCRAM-SHA-256-PLUS defends against an attacker who
  terminates TLS in the middle. Whether that threat model applies depends on
  whether deployments terminate TLS at a proxy. Milestone 7 decides.
- **Conformance suite choice — settled.** Eclipse Paho's interoperability
  tests, `eclipse-paho/paho.mqtt.testing`, pinned at commit
  `9d7bb80bb8b9d9cfc0b52f8cb4c1916401281103`. It is maintained by the Paho
  project rather than by any broker, tests a broker purely over the wire,
  covers both protocol versions (27 tests for MQTT 5, 10 for 3.1.1), and
  needs nothing but Python 3 to run. Its licence is `EPL-2.0 OR
  BSD-3-Clause`.

  **The licence rule here is the weaker one.** A test tool run in CI is
  never linked into the shipped binary and never distributed, so what its
  licence must do is permit use. That is weaker than the dependency policy
  in `deny.toml`, which governs code that ships and is why EPL-2.0 is not
  on that allow list. The distinction happens not to change the answer,
  because the suite's BSD-3-Clause option is already allowed, but the rule
  being applied is the weaker one and `deny.toml` is unchanged.

  The runner is `services/broker/conformance/run.sh` and the CI job is
  `conformance`. The suite has no expected-failure mechanism, so the runner
  supplies one: it runs **every test on its own against a broker started
  for it**, and compares the failing set against
  `conformance/known-failures.txt`, failing the build on a new failure and
  equally on a listed test that has started passing. Per-test isolation is
  not tidiness: run whole, `test_subscribe_identifiers` and
  `test_will_message` fail only because of state an earlier test left
  behind, and a baseline taken from a whole-suite run would encode the
  running order as conformance.

  The first run found **12 failures of 37 tests**: two deferred, three
  suite assumptions, seven real defects and one test that races with
  itself. None was fixed at the time, because a conformance failure is
  worth looking at before it is patched and each fix belonged in a layer
  below the shell.

  All eight defects behind those seven tests have since been fixed. **Of
  37 tests, five now fail and 32 pass**, one of those 32 being the test
  whose result is ignored either way because it races with itself. The
  five are the two deferred and the three assumptions. No known defect
  remains on the list.

  The suite is not the whole story. A walk of sections 5 and 6 against
  the code, done because convention 8 demands it before a milestone
  closes, found five more promises with no implementation and five with
  no test that could fail. None of them is visible to a conformance
  suite, because a server that always supports quality of service 2 and
  always accepts a retained message is a legal server whatever it
  advertises. They are recorded in the milestone 1.5 plan.

- **What the final review found, after all of that.** Two defects worth
  naming. `Queue::would_refuse`, which the shared-subscription skip asks
  before choosing a member, modelled one of the three reasons
  `Queue::push` refuses, so an offline durable member answered "I have
  room" to a fire-and-forget message and then dropped it while an online
  member sat idle. And the writer's outbox stayed a hard 1024 after the
  queue bounds it drains became configurable, so a large configured
  queue could overflow it, discarding acknowledged deliveries whose
  window slots were already held. Both are fixed; the outbox is now
  derived from the bounds and a configuration that breaks the relation
  is refused at startup.

  Six further rules had no test that could fail, **including the
  takeover guard**, on which every takeover depends: deleting it lets a
  displaced connection tear down the session a newcomer has just taken
  over, and the whole suite stayed green. Its sibling rule was tested,
  but only by constructing the type by hand, so no test ever walked the
  connection. That is the lesson recorded in convention 5.

  The running total for milestone one is twenty-two defects found after
  the feature work was complete, by three methods in order of yield:
  an independent conformance suite, a walk of the spec against the code,
  and mutation testing during review. None of the three found what
  either of the others did.

- **Eight conformance defects, found by the suite and since fixed.**
  Kept here rather than deleted, because the finding is worth more than
  the fixes and a reader who meets the defect list in the history should
  meet its resolution in the same place.

  What was wrong: a client's topic aliases were neither resolved nor
  stripped, so a publish carrying only an alias was silently lost and the
  alias was forwarded to subscribers that never asked for one; a protocol
  error the decoder caught closed the socket with no DISCONNECT; a 3.1.1
  session with `CleanSession` clear was given an expiry of zero and
  discarded at disconnect; a DISCONNECT's Session Expiry Interval was
  ignored; the retained store held payload, quality of service and expiry
  and dropped the rest; a delivery reported the Message Expiry Interval it
  was published with rather than what remained; a client's Maximum Packet
  Size was not honoured outbound; and a will could fire up to a second
  early, because a deadline of `now + delay` on a whole-second clock
  rounds the wrong way.

  **Six of the eight were features this document lists in section 5.4 as
  in scope for v1, and no task in any of the five milestone-one plans
  built them.** Nothing in the process read the specification back. That
  is the finding, and it is recorded as convention 8.

  Sixteen mutations were made by hand against the fixes and **eight
  survived**, each one a test that could not fail. Two shapes recurred.
  Tests that assert around a boundary rather than on it: this plan
  predicted that a 200-byte publish would catch a size limit doubled from
  64, and it does not, because 200 exceeds both. And tests that reach the
  easy half of a rule: a discarded oversize publish must also complete its
  delivery, and every test that only checked the packet was absent passed
  against a broker that leaked the inflight slot forever.

  Two findings came from mutations no plan named. A publish carrying a
  Subscription Identifier from the client, which [MQTT-3.3.4-6] forbids,
  reached subscribers through the retained store; only the unconditional
  overwrite in the outgoing publish stops it, and `broker-core` still does
  not reject it on ingress. And a displaced connection's writer, draining
  actions handed to it before a takeover, could complete a delivery whose
  packet identifier the newcomer now owns; the discard path checks the
  session epoch as the detach path does.

  **Still open, deliberately.** The CONNACK is written before the write
  loop exists and is therefore not size-checked, so a client advertising a
  Maximum Packet Size below roughly thirteen bytes receives an oversize
  CONNACK. Suppressing it would leave that client with no reply at all,
  which is worse. Left as it is and recorded here.

  The three suite assumptions, recorded so they are not re-investigated:
  `test_server_keep_alive` asserts a CONNACK property 3.2.2.3.14 says the
  server MAY send; `test_server_topic_alias` asserts the server uses
  aliases, which 3.3.2.3.4 permits rather than requires and which section
  5.4 now says this broker does not do; and `test_flow_control2` is the
  suite's own client raising `BrokenPipeError` while writing another
  PUBREL, a raw probe confirming the broker sends exactly the DISCONNECT
  with reason 0x93 the test is looking for, after the 1024th
  unacknowledged QoS 2 publication.
