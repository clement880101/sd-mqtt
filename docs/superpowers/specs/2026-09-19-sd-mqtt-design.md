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
        storage/             Storage trait, redb backend, in-memory backend
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

**The backend is redb.** An earlier draft named fjall, reasoning that a
queue of appends with deletion from the head is the shape a log-structured
merge tree is built for. Measuring both before writing against either
reversed that.

The deciding facts:

- redb has a native remove-and-return-head. It measured 1.47M operations a
  second and stayed flat across a hundred thousand of them. fjall has no
  such primitive, and its obvious head read is a forward scan over every
  accumulated deletion marker: it collapsed from 1,357 a second to 197 as a
  queue drained, and a hundred thousand deletions never finished. A
  persisted head cursor recovers most of that, but the engine that needs no
  workaround is the better one to build on.
- redb has one dependency, `libc`. fjall has forty-two, two of which fall
  outside this project's licence allow list and would require amending it.
  redb passes `cargo deny` with no change at all.
- redb spawns no background threads. fjall runs a worker pool whose write
  path contains unbounded loops of `std::thread::sleep`, which must never
  run on an async runtime's thread.

What is given up is key-value separation: redb stores values in the tree,
so a multi-megabyte payload costs write amplification that fjall's separate
blob storage would avoid. Message bodies are stored once and referenced, so
the cost is paid once per message rather than once per subscriber. It was
recorded in section 17 as a thing to measure rather than a thing to assume,
and section 17 now carries the measurement: a body at the configured
maximum packet size costs less per byte than a small one, and nothing
follows from it.

Writes serialize on a single write transaction, which is what a single
batching writer does in any case. Freed pages are reused, so the file is
bounded by its high-water mark rather than growing without end; reclaiming
space needs an explicit compaction that excludes all transactions, which is
a maintenance operation rather than a background one.

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

### 7.3 How the broker says what changed

`broker-core` does no I/O, holds no locks and takes time as a parameter.
Persistence must not change that, so the broker does not write: it
**says what happened** and the shell writes.

`Broker` keeps a journal buffer beside the action buffer it already
fills. Every method that changes durable state appends a `Record` to it,
and the shell drains it under the same lock that produced it, so a
record and the actions it belongs with are always taken together and in
order. A buffer is state, not I/O, so the purity that makes the broker
testable survives.

The alternative, having the shell infer what to persist from the packet
it just handed in, was rejected: it would put a second copy of the
broker's rules in the shell, and the two would drift the first time one
of them was corrected.

**Ordering is the point, and it is not an optimisation.** For an
at-least-once or exactly-once publication, the acknowledgement says the
broker has taken responsibility for the message. Sending it before the
message is durable means a restart can lose a message the publisher was
told was safe. So the shell commits the journal, waits for the commit to
be durable, and only then delivers the actions. The wait is what the
batching writer exists to make cheap: many connections commit into one
flush and are woken together.

At-most-once publications are journalled where they touch durable state
and are never waited on, because nothing was promised.

### 7.4 Sequence numbers

Every queue entry carries a per-session sequence number, assigned when
the message is enqueued and monotonic within the session. It is the
queue's key, so reading in key order is reading in the order the
messages arrived, and the highest applied number is a single value
rather than a scan.

They exist from this milestone even though nothing reads them yet.
Replication in milestone 6 is a matter of shipping operations a replica
applies in order, and retrofitting an ordering onto a queue that never
had one would mean rewriting every stored queue in the field.

Gapless is a property of the assignment, not of what survives: a
delivered and deleted entry leaves a hole in the stored keys, and that
is expected. What must never happen is two entries sharing a number, or
a number going backwards.

### 7.5 Restore

Restore is the first startup phase and completes before any listener
binds, so no client can observe a half-restored broker.

It rebuilds, in order: sessions with their expiry and will; their
subscriptions, which re-enter the matching trie; inbound exactly-once
identifiers awaiting release; outbound inflight entries with the
identifiers they were sent under; and queues, read in sequence order.
The seams for the last three exist already and were written for this:
`Inbound::reserve`, `Outbound::restore` and `Queue::requeue`.

A session whose expiry has passed while the broker was down is not
restored. The clock the broker starts with is therefore read before
restore, not after.

### 7.6 What this milestone does not store

The trait in 7.1 lists what it will eventually cover. Milestone 2
implements sessions, subscriptions, inbound state, queues, message
bodies and retained messages. Replica queues wait for milestone 6,
cluster metadata for milestone 5, and users, access control rules,
rules and API keys for milestones 4 and 7. The trait may name them; the
backend need not answer them yet.

**One real backend is built.** What keeps the trait honest in this
milestone is an in-memory backend used by the tests, which is cheap and
catches the same class of mistake as a second engine would.

The trait is nonetheless written as though a second backend existed,
because comparing two engines is what found the head-read problem in the
first place. Concretely: no backend's types appear in a signature; the
head read is expressed as "read at or after this cursor" rather than as
"read the first key", so an engine without a head primitive can still
answer it; and queue depth is an explicit stored counter rather than a
call to the engine's own length, because at least one engine's answer
counts deleted entries.

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

**Until milestone 7 there are no API keys, and the admin listener is
protected by where it listens.** It binds the loopback address by default.
A static bearer token may be configured, and mutating requests need it when
one is set.

Binding a non-loopback address with no token configured is **refused at
startup**, not warned about. The endpoints include publishing and, later,
deleting; an open one on a network interface is not a degraded mode but a
hole, and this project already refuses rather than reinterprets a
configuration that cannot mean what it says. The refusal names both the
address and the missing key.

The token is an interim. It is one secret with no scopes, no rotation and no
per-user identity, which is precisely what milestone 7 replaces. It exists
so that milestone 3 can be used and demonstrated without shipping something
unprotected, and the configuration documents it as temporary.

The events stream pushes counter deltas, connect and disconnect events, and
rule outcomes. Events carry identifiers so a reconnecting browser resumes
with the last-event header.

**The header is not the mechanism; a ring of recent events is.** A broadcast
channel drops messages for a receiver that falls behind and reports the drop
as an error the idiomatic stream combinator discards, which would be a
message vanishing uncounted in the one section that forbids it. So recent
events are kept in a bounded ring keyed by identifier, a reconnecting browser
is replayed from the ring, and a browser asking for an identifier that has
fallen out of it is sent a resynchronise event telling it to refetch rather
than a quiet gap. Every dropped event is counted.

**A stream must end when the broker stops.** Graceful shutdown waits for
every open connection, and the body of an events stream never ends on its
own, so one browser tab left open on the Overview page would stop the broker
exiting. Since a clean stop is how sessions reach disk, that is not a
cosmetic bug. Each stream selects on the same shutdown signal the listener
does, and the whole admin server is bounded by a timeout as a backstop.

**The admin listener speaks HTTP/1.1, and the stream costs a connection.**
Browsers allow six per origin and an events stream holds one for the life of
the tab, so a seventh tab's ordinary requests would stall. HTTP/2 would fix
it and is deliberately not enabled: browsers negotiate it only over TLS, TLS
is milestone 7, and ten crates bought now would buy nothing until then. The
interface therefore opens exactly one stream for the whole application
rather than one per page, and the limit is revisited when TLS lands.

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
binary by a build script, served with content negotiation: brotli, gzip and
identity from three embedded blobs, chosen by a parsed `Accept-Encoding`
including its quality values, answered with a `Vary` header and a distinct
entity tag per encoding.

**In development Vite proxies to the broker, not the other way round.** An
earlier draft had the broker proxying to the Vite development server, which
would mean a streaming reverse proxy with WebSocket upgrade forwarding,
living in the shipped binary and existing only for development. Vite does it
natively in a few lines of its own configuration. The one thing the broker
owes is a header telling intermediaries not to buffer the events stream, and
never compressing it, which it owes to production proxies anyway.

**A build of the binary must not require Node.** The embedding is gated, so
a clone with no interface built compiles and serves a placeholder that says
so. Otherwise every Rust-only contributor and the whole test matrix depends
on an npm install.

**Attribution is generated from the bundle, not from the package list.**
What ships is what the bundler kept after tree-shaking, so the notice file is
produced by a plugin reading the built module graph, checked into the
repository, and regenerated in the build. The build fails on a licence
outside the allow list and on any difference between the regenerated file and
the committed one, so the notices cannot drift from the bundle. Build tooling
is excluded because it is not distributed; the small runtime helpers the
bundler injects are included, because they are.

### 11.3 Metrics

Counters for packets by type, version and reason code; queue events by
reason including drops and expiries; socket lifecycle; bytes in and out;
authentication outcomes; cluster bytes sent, received and dropped; and
router matches split local and remote. Gauges for connections, sessions,
subscriptions, retained messages, queue depth and cluster members.

Every discard path has both a metric and a hook. A message never disappears
without being counted.

Hooks are milestone 4. The metrics are milestone 3, and building them found
five discard paths nobody had listed, one of which was a live violation of
the sentence above: a fire-and-forget publication too large for its peer
returned before anything recorded it, so it left no action, no counter and no
log line. A new drop reason now fails to compile until it has a counter,
rather than failing a test somebody has to remember to extend.

**Router matches are deferred to milestone 5.** This section asks for them
split local and remote. The remote half needs a cluster, and counting the
local half needs the matching layer to report a number it does not currently
return, which would put counting inside the crate that deliberately counts
nothing. Both halves arrive together with clustering. Recorded here so that
it stays a decision rather than becoming an omission.

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

### 13.1 The HTTP API's error shape

The admin listener is one of those boundaries. **Every failure it returns
takes one shape and no other**, so that the interface has one thing to parse
and one thing to branch on:

```json
{
  "error": {
    "code": "invalid_request",
    "message": "limit must be between 1 and 1000, not 0",
    "field": "limit"
  }
}
```

`code` is a fixed string from a closed set and is the only part a client may
branch on. `message` is one English sentence for a person, may be reworded in
any release, and nothing may match on it — the alternative makes every
message a compatibility surface, and the first improvement to the wording
silently sends an interface down the wrong branch. `field` names the request
parameter at fault where one is at fault, and is absent otherwise, so a form
can mark an input without reading the sentence.

The codes today are `invalid_request`, `not_found`, `method_not_allowed`,
`unauthorized` and `internal`. Adding one is a compatible change and renaming
one is not; a client treats an unknown code as the class its HTTP status
implies. The status is **derived from the code** rather than sent beside it,
so the two cannot disagree.

The body is nested under `error` so that a client which forgot to look at the
status code still cannot confuse a failure with a success: no successful
response has a top-level `error` key and every failure has one.

Two things are deliberately outside it. The health endpoints answer their own
document either way, because an unready readiness probe is that endpoint
working rather than failing and a probe wants the same shape both times. The
metrics endpoint answers in the exposition format on success, and in this
shape on failure.

`crates/admin/src/error.rs` is where this lives, and is the authority if the
two ever disagree.

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

| Metric | Target | Today | Checked |
|---|---|---|---|
| Release binary, all features, stripped, link-time optimized | under 15 MB | 2.52 MB | every push |
| Idle resident memory, single node, no clients | under 30 MB | not yet measured | scheduled |
| UI bundle, gzipped | under 150 KB | no interface yet | reported as skipped |
| Direct dependencies across broker crates | under 35 | 14 | every push |
| Concurrent connections per node, target hardware | 100,000 | not yet measured | milestone 7 |

The "today" column is the trend, not the limit, and is what tells a
reader whether a budget is at 94% or at 4%. Recorded at milestone 3
with `scripts/budgets.sh`: the binary was 2,523,184 bytes on macOS
arm64, and the fourteen distinct direct dependencies were `bytes`,
`clap`, `foldhash`, `parking_lot`, `postcard`, `prometheus-client`,
`redb`, `serde`, `thiserror`, `tokio`, `toml`, `tracing`,
`tracing-subscriber` and `uuid`. The binary's size is platform
dependent; CI measures it on Linux x86-64 and its figure will differ.

Durable queue storage and inter-node traffic scale with the replication
factor. At the default factor of 2, plan for roughly twice the queue disk a
single node would need, and inter-node bandwidth proportionate to durable
publish volume.

Budgets are checked in CI. Exceeding one fails the build and requires either
a fix or an explicit, recorded decision to move the budget.

**This was not true when it was written and is being made true in milestone
3.** Milestones 1 and 2 shipped with no budget enforced, which convention 8's
walk found. Three of the five are cheap to check and are checked from
milestone 3: the binary's size, the count of direct dependencies, and the
gzipped interface bundle. Idle resident memory is measurable but needs a
process started and settled, so it is checked as a scheduled job rather than
on every push. The connection target is a load test, not a build gate, and is
milestone 7's to prove.

`scripts/budgets.sh` is what does it, from the `budgets` job. It prints
every budget's measurement and its headroom whether or not the budget
is met, because a green tick cannot tell 94% from 4%. The interface
bundle cannot be measured until there is an interface, so that line
reports **skipped, and why**, rather than passing: a check that
quietly passes when it cannot measure is worse than no check, and
would report three budgets met having looked at two.

## 16. Milestones

Each milestone gets its own implementation plan and is independently
shippable.

1. **Codec and core.** MQTT 3.1.1 and 5.0 over TCP, in memory only. Sessions,
   QoS 0, 1 and 2, retained messages, wills, shared subscriptions, keep-alive
   ring. Exit criteria: a single node passes the conformance suite over plain
   TCP, with the suite's TLS and WebSocket sections deferred to milestone 7.
   The suite is chosen and pinned during this milestone.
2. **Persistence.** The storage trait, the redb backend, durable sessions and
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

- **The ceiling on acknowledged delivery, measured.** Section 7.3 sends an
  acknowledgement only after the commit is durable, so whatever the rest
  of the broker can do, an acknowledged message costs a share of a flush.
  Nothing had measured it. The harness is
  `services/broker/crates/storage/tests/throughput.rs`, three ignored
  tests run with
  `cargo test -p storage --release --test throughput -- --ignored
  --nocapture --test-threads 1`. Machine: Apple M2 Pro, 12 logical cores,
  32 GB, macOS 26.4.1, arm64, release profile, internal APFS SSD, redb
  with `SyncMode::Immediate`, one small record per write, each waiter
  holding one write outstanding at a time, three-second window after a
  one-second warm-up.

  | concurrent waiters | durable writes/s | per waiter | coalescing off |
  |---:|---:|---:|---:|
  | 1 | 334 | 334 | 335 |
  | 10 | 3,343 | 334 | 339 |
  | 100 | 25,100 | 251 | 337 |

  The last column is the control: the same load with the writer's count
  trigger at zero, so every batch is flushed on its own. It is flat at
  about 337 a second however many waiters there are, because that is one
  `fsync` on this device and nothing is amortising it. With coalescing,
  a hundred waiters get 25,100, and each of them still completes about
  250 a second — roughly one flush per round trip, shared.

  **So the ceiling is set by how many publishers are waiting at once, not
  by the device.** A broker with one publisher awaiting an
  acknowledgement is held to about 340 messages a second whatever else it
  can do; a hundred of them get 75 times that between them, which is 75%
  of what a perfectly linear scaling would give. Beside
  the lock measurement above — around 290,000 at-most-once messages a
  second through one `Shared` — this says that for acknowledged traffic
  at these concurrencies the disk is the limit and the lock is not.

- **What a multi-megabyte payload costs the store, measured.** redb keeps
  values in the tree rather than apart from it, so a large payload is
  rewritten as pages split and merge, where an engine with key-value
  separation would write it once. The same harness, same machine. Each
  row stores about 32 MiB of payload as bodies of one size, one batch per
  body, with identifiers scattered across the key space as a real UUID
  would be, and compares the file size before and after with the file
  closed both times.

  | body | bodies | payload bytes | file grew by | bytes/byte | MiB/s |
  |---:|---:|---:|---:|---:|---:|
  | 1 KiB | 32,768 | 33,554,432 | 66,351,104 | 1.98 | 46 |
  | 64 KiB | 512 | 33,554,432 | 66,318,336 | 1.98 | 395 |
  | 1 MiB | 32 | 33,554,432 | 68,153,344 | 2.03 | 562 |
  | 2,000,000 B | 16 | 32,000,000 | 34,598,912 | 1.08 | 952 |

  **Large bodies are not disproportionately expensive, and no design
  change follows.** The store costs about twice its payload in file space
  at every body size up to a megabyte, and 1.08 at the configured maximum
  packet size, where it is also written twenty times faster per byte than
  a kilobyte body is. The answer this question reserved — keeping large
  bodies in files beside the store — is not needed, and bodies stay in
  the store.

  What the figure is and is not: bytes the file grew per byte of payload,
  which is **space** amplification. Write amplification, the bytes a
  copy-on-write engine spends rewriting pages, is necessarily larger and
  is not measured here, because counting device writes needs a tool the
  test cannot call. The throughput column is the check on it: a body
  rewritten many times over would show there, and the rate rises with
  body size rather than falling.

  One caveat for anyone reproducing it. The ratio is only stable once the
  store is tens of megabytes, which is why every row stores about 32 MiB:
  the same 64 KiB bodies measured over 8 MiB read 3.89, which is redb's
  file growth granularity rather than anything about the payload.

- **What a fan-out costs the store, measured.** Section 7.1 stores a body
  once and references it. That is a claim about **space**, and the table
  above is what measures it. The claim beside it that nothing measured is
  what the references *cost*: the backend held the reference count in the
  body's own row, so every put after the first was a read-modify-write of
  the whole row — deserialise the payload, reserialise it, write it back
  — and a fan-out emits one put per session for one message. The same
  harness, now three ignored tests, and the same machine as above. Each
  row queues one publication for that many durable offline subscribers as
  the broker emits it: one `BodyPut` and one `QueuePushed` per session,
  **all in one batch**, which is one write transaction. Eight
  publications a row, and only the apply is timed.

  | body | subscribers | ms/publication, count in the body | ms/publication, count apart |
  |---:|---:|---:|---:|
  | 256 KiB | 1,000 | 27.2 | 2.9 |
  | 256 KiB | 100 | 3.8 | 1.2 |
  | 256 B | 1,000 | 2.4 | 2.0 |

  **Nine times, and the change was kept.** A 256 KiB publication to a
  thousand durable offline subscribers cost 27 ms of the storage thread,
  inside one write transaction, with every waiter behind it blocked; with
  the count in a table of its own it costs 3. The two rows below it say
  what the saving is made of: at a hundred subscribers of the same
  payload it is three times rather than nine, and at a 256-byte payload
  it is 2.4 against 2.0, which is the shape of the batch rather than the
  payload. So what was being paid for was rewriting the body, once per
  subscriber, exactly as the reading predicted.

  The space claim is unchanged and still holds: one row holds the payload
  however many sessions name it, which `a_body_referenced_many_times_is_written_once`
  asserts. What this cost the on-disk format is one table and a version
  bump, because a version 1 store's body rows begin with eight bytes of
  count that this build would read as payload.

  The same shape one layer up was already fixed once: `broker-core`'s
  `SharedProperties` exists so that a property block is encoded once per
  message rather than once per subscriber. The backend reintroduced the
  cost underneath it, which is worth remembering the next time a
  per-subscriber loop meets a shared value.

- **What a store costs the broker, measured.** The two questions above
  measured the lock with no store and the store with no broker. This is
  the two together, on the same harness as the lock figures —
  `services/broker/crates/server/tests/throughput.rs`, now four ignored
  tests — run with
  `cargo test -p sdmqtt-server --release --test throughput -- --ignored
  --nocapture --test-threads 1`. Machine: Apple M2 Pro, 12 logical
  cores, 32 GB, macOS 26.4.1, arm64, release profile, internal APFS
  SSD, loopback TCP, MQTT 5, 32-byte payloads, one subscriber per
  publisher on its own topic, three-second window after a one-second
  warm-up. Both rows of each table were measured in the same run, so
  the comparison is not against a figure recorded on another day.

  At most once, paced at 512 outstanding per pair, nothing discarded:

  | publishers | no store, msg/s | redb, msg/s |
  |---:|---:|---:|
  | 1 | 429,000 | 428,000 |
  | 2 | 345,000 | 354,000 |
  | 4 | 230,000 | 231,000 |
  | 8 | 173,000 | 174,000 |
  | 16 | 214,000 | 217,000 |

  **A store costs at-most-once delivery nothing, and the reason is that
  nothing is written.** Section 7.3 journals such a publication only
  where it touches durable state, and one delivered straight to an
  attached subscriber touches none: `Outbound::offer` dispatches it
  without pushing it on to a queue, so the whole exchange produces no
  record. The column is here to show that rather than to find a cost.
  The no-store column also reproduces milestone one's curve on the same
  machine, 429,000 against 452,000 at one publisher, which is the check
  that nothing else moved underneath it.

  At least once, each publisher keeping exactly one publication
  outstanding, so the publisher count is the concurrent-waiter count
  the storage measurement is in:

  | publishers | no store, ack/s | redb, ack/s | per waiter | storage crate predicted |
  |---:|---:|---:|---:|---:|
  | 1 | 30,000 | 340 | 340 | 334 |
  | 2 | 41,500 | 365 | 182 | ~670 |
  | 4 | 53,200 | 1,125 | 281 | ~1,340 |
  | 8 | 58,700 | 2,630 | 329 | ~2,680 |
  | 16 | 62,500 | 4,900 | 306 | ~5,350 |
  | 100 | 66,000 | 11,100 | 111 | 25,100 |

  Every figure is the mean of two runs that agreed to within 5%.

  **The prediction holds at one waiter exactly and is optimistic
  everywhere above it.** One publisher awaiting an acknowledgement gets
  340 a second where the store alone gave 334, so the broker adds
  nothing measurable to a message that is waiting for a disk. Coalescing
  then works, but not as well through the broker as through the store:
  a hundred waiters get 11,100 rather than 25,100, which is 45% of the
  store's own ceiling and 33 times the single-waiter rate rather than
  75.

  **The shape differs, and the reason is that a message is not a
  record.** The storage measurement wrote one small record per durable
  write. An acknowledged publication through the broker journals a body,
  a queue push, a queue pop and an inflight entry before the
  acknowledgement is owed, and the body carries the payload; the
  subscriber's own acknowledgement then adds two more records to the
  next flush. So a hundred waiters are not a hundred records per flush
  but closer to six hundred, and the flush is correspondingly longer.
  The store's figure is the ceiling on durable **commits**, not on
  acknowledged messages, and the difference between the two is how much
  the broker writes per message.

  **Two waiters get no more between them than one gets alone** — 365 a
  second against 340, so 182 each. It is reproducible across runs and it
  is not noise. The writer flushes what has accumulated since the last
  flush, and a batch that arrives while a flush is in progress waits for
  the next one; two publishers in a strict round trip arrive almost
  exactly in each other's flush windows, so each pays a whole flush of
  its own. At four and above enough waiters accumulate behind each flush
  for the coalescing to pay. Nothing is broken here, but a deployment
  with two acknowledged publishers should not expect twice one.

  Beside the at-most-once row: the same broker moves 428,000 unpromised
  messages a second and 11,100 promised ones. **That factor of forty is
  the price of the promise**, and it is paid to the device rather than
  to the lock.

- **Encoding a property block once per subscriber, found and fixed.**
  `journal::body_record` built each subscriber's body record by
  encoding the message's property block again, though the store keeps
  one body per message however many sessions hold it. On the same
  machine, a publish to a fan-out of a thousand offline durable
  sessions cost 654 ns per subscriber with a realistic 95-byte block
  against 435 ns with an empty one, and a fan-out of a hundred cost 465
  against 248: a third to a half of the work of a fan-out was producing
  copies of one answer. The block itself encodes in 284 ns, which is
  the check that the difference is the encode and not something else
  that grew with the fan-out.

  `broker_core::properties::SharedProperties` now carries the block and
  its encoding together, filling the encoding on the first ask and
  sharing the buffer with every asker after it. Lazily, because an
  at-most-once publication to an attached client is journalled nowhere
  and encoding eagerly would charge the hot path for bytes no record
  wants; and with no way to change a block in place, because a cache is
  only honest while what it was taken from cannot change.

  | fan-out | before, ns/subscriber | after, ns/subscriber |
  |---:|---:|---:|
  | 1 | 1,730 / 1,950 | 1,740 / 2,030 |
  | 10 | 369 / 570 | 310 / 328 |
  | 100 | 248 / 465 | 193 / 185 |
  | 1,000 | 435 / 654 | 393 / 366 |

  Empty block first, realistic block second, each the mean of three
  runs. The two columns meet afterwards, and at a fan-out of one
  nothing changed — one encode is still one encode, and the 4% between
  1,950 and 2,030 is inside the spread of the runs it is the mean of. The measurement is
  `what_a_property_block_costs_a_fan_out` in the same harness, kept so
  that a build which re-encoded per subscriber would pull the two
  columns apart again.

- **Two fjall behaviours worth remembering if this is ever revisited.**
  Its length call counts deleted entries, so it cannot answer queue
  depth; and its keyspaces cost roughly thirty milliseconds each to
  create, so a design with one per session would take minutes to start.
  Both shaped the trait even though the engine was not chosen: depth is a
  stored counter and queues share one table with composite keys.

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
