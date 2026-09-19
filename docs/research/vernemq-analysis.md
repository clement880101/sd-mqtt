# VerneMQ: behavioral analysis

Purpose: inform an independent, clean-room Rust broker design. Paraphrased
description of documented and observable behavior from docs.vernemq.com and a
conceptual read of the public repository. No source code, comments, or
implementation structures are reproduced here.

## 1. Protocol handling

- Supports MQTT 3.1, 3.1.1, 5.0 and "bridge" variants. Allowed versions are a
  per-listener list.
- Two separate session state machines exist for 3.x and 5.0, sharing a small
  pre-init stage that reads the first frame to pick one. A compatibility shim
  lets old plugins see v5 sessions. This duplication is a known maintenance
  burden.
- Parsing is incremental: fixed header, remaining length, early size check
  against the max packet size before parsing the body, then either "need more
  bytes", a frame, or a typed error. Errors are values. v5 properties are
  parsed by identifier with per-property typing; UTF-8 is validated.
- Session states: awaiting CONNECT (connect timeout ~5 s) and connected, with
  sub-states for v5 enhanced auth.
- CONNECT validation order: version allowed, client ID length (default cap
  100) and empty-ID rules per version, credentials via auth chain, will
  authorized like a normal publish, then session registration.
- Keep-alive: one coarse timer per session rather than a reset per packet; on
  fire, disconnect if idle exceeds 1.5x keep-alive.
- v5 limits: separate receive-maximum per direction, topic alias maps per
  direction, negotiated max packet size (the broker strips optional properties
  before dropping an oversized outgoing frame), server keep-alive, capped will
  delay, subscription identifiers, retain-handling, request-problem-information.
- Global guards: max message size, topic depth (default 10; exceeding
  disconnects), max subscriptions per client, max inbound publish rate
  (throttle pauses), max connection lifetime (forces re-auth).

Adopt: value-typed parse errors with a need-more sentinel; early size
rejection; one coarse keep-alive timer; per-listener version list; typed limit
knobs.
Avoid: two parallel state machines; build one session core with a
version-aware codec.

## 2. Connection management

- Many named listeners per node (tcp/tls/ws/wss, admin http, cluster link).
  Config is hierarchical: global, per-transport, per-listener. Per-listener:
  max connections, acceptor count, TLS material, PROXY protocol, trusted
  proxies for X-Forwarded-For, auth chain, anonymous override, mountpoint.
- Mountpoints prefix each listener's clients into an isolated topic namespace;
  subscriber identity is (mountpoint, client ID). Simple multi-tenancy.
- Connection and session queue are distinct actors. The queue outlives the
  connection when the session is persistent.
- Backpressure: sockets read one packet at a time before returning to
  passive, so an overwhelmed session stops reading; inbound rate limit;
  outbound gated by max inflight (default 20) and v5 receive maximum.
- Takeover: a new CONNECT with an existing ID triggers a coordinated
  cluster-wide disconnect of the old session before the new one attaches
  (configurable to eventual). Will on takeover can be suppressed.
- WebSocket on a fixed path with header limits and max frame size.

Adopt: connection/session separation; mountpoints; per-listener auth chains;
PROXY/XFF with trusted list; connection lifetime cap.
Avoid: uncoordinated takeover leaving duplicate sessions for a window.

## 3. QoS behavior

- QoS 0 enqueued only if a session is attached. QoS 1 acked to the publisher
  on receipt; delivery waits for the subscriber's ack. QoS 2 two-phase both
  directions; inbound tracked by packet ID until PUBREL.
- Inflight window is a per-session map of unacked outbound messages; new QoS
  1/2 deliveries wait in the queue until the window has room.
- Retries: a single retry timer plus an ordered list of (timestamp, packet
  ID); on fire, entries older than the retry interval (default 20 s) are
  resent with DUP. This is 3.1.1-era behavior that MQTT 5 discourages.
- Ordering guaranteed only with inflight window = 1.
- A "backup" list of recently sent unacked messages is moved back to the
  offline store when the connection drops.

Adopt: single timer + ordered retry list; explicit backup-on-disconnect;
document ordering needs window = 1.
Avoid: DUP retransmission on a live connection as the default for v5.

## 4. Persistence

- Two embedded stores per node:
  - Metadata store (subscriptions, retained, membership, API keys): replicated
    cluster-wide by an eventually-consistent layer. Backed by an embedded KV.
  - Message store (offline QoS 1/2): node-local, not replicated. Pluggable
    engines.
- Message store: body stored once under a message reference; per-subscriber
  index entries (timestamp, QoS, DUP) point at it; refcount deletes the body
  when the last index is removed. Subscribers hashed across N engine buckets.
  Startup rebuilds refcounts by folding all index entries. FIFO from index
  key timestamps.
- Retained: in-memory ordered table for matching, with writes batched (~1 s)
  to the metadata store for replication; expiry swept periodically; cache
  rebuilt on restart/join.
- A persistent session's queue drains to another node on graceful leave, but
  offline messages on a crashed node are lost. The docs warn about this.

Adopt: deduplicated body + per-subscriber index with refcounting; bucketed
engines; batched retained writes; replicated metadata distinct from bulk data.
Avoid: unreplicated offline queues; refcount rebuild only via full scan.

## 5. Queueing

- Every session has a queue actor with states: offline, online, wait-for-
  offline (takeover/migration), drain (shipping to another node).
- Limits: max online messages (RAM, default 1000, held beyond the inflight
  window) and max offline messages (disk, default 1000).
- Overflow: FIFO drops the newest arrival; LIFO drops the oldest. Every drop
  fires a hook and increments a metric. Queue type overridable per client.
- Shared subscriptions `$share/group/topic` with policies: prefer_local
  (default), local_only, random, prefer_online_before_local. Candidates are
  shuffled; on full/unreachable, fall back to the next; each member keeps its
  own offline queue (no group queue).
- Migration on node leave: suspend listeners so clients reconnect elsewhere,
  then drain remaining offline queues round-robin to other nodes in batches,
  keeping the old queue alive for a window to absorb stragglers.
- Fanout sharding spreads high-fanout delivery across worker shards.

Adopt: explicit queue actor states; separate RAM vs disk caps with drop hook;
shared-sub policies with online awareness and fallback-on-full.
Avoid: drop-newest default surprises; random vs round-robin unevenness.

## 6. Clustering and distribution

- Membership: explicit join via a seed node, explicit leave; a shared secret
  gates the inter-node link; nodes with prior history cannot rejoin by
  default. Absent nodes are remembered and retried (a standing netsplit until
  removed).
- Metadata layer: eventually-consistent replicated KV using bounded node
  clocks and dotted version vectors. Each write gets a (node, counter) dot;
  updates broadcast immediately plus periodic anti-entropy (every ~5 s with
  jitter) with a random peer: compare clocks, pull missing dots in batches.
  Concurrent writes surface as siblings for the application to resolve;
  deletes are tombstones GC'd via a cluster-wide watermark. No consensus.
- Subscriptions persist in the metadata store and mirror into a per-node
  in-memory trie, plus a table mapping topics to remote nodes with refcounts.
- Routing: publish, match the trie, enqueue locally, forward one copy per
  remote node with any matching subscriber; the remote node re-matches.
  Dedicated inter-node TCP/TLS listener; framed batches; reconnect backoff;
  bounded outgoing buffer (64 MB) beyond which messages to that node are
  dropped with a metric and an error to the publisher.
- Netsplit: by default, when the cluster is not ready, CONNECT, PUBLISH,
  SUBSCRIBE, UNSUBSCRIBE are refused; four flags relax each independently.
  A window of uncertainty before detection can produce duplicate sessions and
  lost QoS 0 / will messages. After healing, version-vector sync converges.
- Session migration: registering on node B for a client whose queue lives on
  A remaps subscriptions to B and drains A's offline queue to B; registration
  blocks until migration completes (~5 s timeout).

Adopt: small replicated metadata plane (CRDT-style, anti-entropy with jitter,
tombstone watermarks) separate from a non-replicated data plane; local trie
mirrors with node-level routing (one copy per node); bounded cluster buffers
with drop metrics; explicit per-operation netsplit policy.
Avoid: refuse-everything default during a split; unreplicated offline queues;
membership tied to a runtime's built-in distribution.

## 7. Error handling

- Parser errors before CONNACK just close the connection; when connected, v5
  gets DISCONNECT with a specific reason (malformed, protocol error, packet
  too large, receive max exceeded, topic alias invalid, keep-alive timeout,
  session taken over, quota exceeded); v3 closes the socket.
- Auth failures: v3.1.1 gets a CONNACK return code; v5 uses reason codes.
  `$`-prefixed topics are blocked from client publishes.
- Overload: a monitor samples CPU into load levels; inbound rate limit; health
  endpoints go 503 when listeners are down or the cluster is not ready.
  Graceful shutdown suspends listeners first, then disconnects and drains.
- Passwords are obfuscated in memory so they never appear in logs or dumps.

Adopt: typed error to reason-code mapping; credential obfuscation; health
endpoints keyed to listener and cluster readiness; a drop hook for every
discard path.
Avoid: errors to publishers that are invisible to MQTT clients (already
acked).

## 8. Plugin and hook system

- Hooks are named behaviors run in plugin order. Three chain types:
  conditional (auth: first ok/error wins, `next` passes along, all-pass means
  reject), event (all run: register, publish, subscribe, unsubscribe, client
  wakeup/offline/gone, session expired, offline message, message drop, config
  change), single (one implementation, e.g. storage).
- Register hook can return per-client overrides: max inflight, max message
  size, retry interval, QoS upgrade, clean session, mountpoint/client ID,
  queue type. Publish hook can rewrite topic/payload/QoS/retain and v5
  properties. Subscribe hook can rewrite or reject individual filters.
  Deliver hook can rewrite outgoing topic/payload.
- Webhooks plugin: each hook becomes an HTTP POST with a hook-name header and
  a JSON body; response is ok / next / error plus optional modifiers.
  Multiple endpoints per hook chain. Auth results cached when the response
  carries a max-age; cache key is the request minus payload. Payload can be
  omitted or base64-encoded. Connection pool per hook; in-flight register
  requests cancelled on early disconnect.
- Bridge plugin: point-to-point to a remote broker with topic rules
  (in/out/both, QoS, prefixes), clean session off by default, restart
  timeout, bounded outbound buffer while disconnected; v5 messages downgrade.

Adopt: three chain semantics with `next`; per-client overrides from the
register hook; TTL-cached HTTP auth with a well-defined cache key; drop and
offline-message hooks; live re-authorize.
Avoid: cache without eviction; order defined implicitly by start order.

## 9. Operations and UI

- CLI groups: node, cluster (show/join/leave), session (show with filters,
  disconnect with cleanup, reauthorize), retain, listener (start/stop/
  suspend), plugin, metrics, api-key, trace (per-client live packet trace,
  rate-limited), webhooks, bridge, config (live set; resets on restart unless
  persisted).
- HTTP API mirrors the CLI one-to-one, secured by scoped API keys
  (mgmt/status/metrics/health, optional expiry). REST publish endpoint.
- Metrics: Prometheus with per-packet-type counters labelled by version and
  reason code, queue counters (setup/teardown/in/out/drop/expired), socket
  counters, bytes in/out, auth and netsplit error counters, cluster bytes
  sent/received/dropped, router local/remote matches, retained and
  subscription counts. Shipped alert-rule template.
- Status page with netsplit banner and per-node counts. Tiered health
  endpoints: ping, listeners, full cluster.

Adopt: CLI/HTTP API symmetry with scoped keys; rich session inspection;
per-client live tracing with rate limits; version/reason labelled metrics;
tiered health endpoints.
Avoid: live config that silently resets on restart.

## Cross-cutting takeaways

- Keep a replicated metadata plane (subscriptions, retained, membership,
  rules) distinct from a node-local message plane, but replicate offline
  queues since losing them on crash is VerneMQ's biggest documented weakness.
- Model session queue and connection as separate actors with explicit states
  and caps; expose every discard path as a hook and a metric.
- Unify v3 and v5 into one session core with a version-aware codec.
