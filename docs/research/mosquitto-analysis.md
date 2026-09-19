# Eclipse Mosquitto: behavioral analysis

Purpose: inform an independent, clean-room Rust broker design. This is a
paraphrased description of observable behavior and documented configuration,
drawn from the public man pages, release notes, and a conceptual read of the
repository. No source code, comments, or implementation structures are
reproduced here.

## 1. Protocol handling

- Supports MQTT 3.1, 3.1.1 and 5.0 on the same listener; newer versions can
  restrict accepted protocol versions per listener.
- Framing: read the fixed-header byte, decode the variable-byte remaining
  length incrementally across partial reads, check the total size against
  `max_packet_size` *before* allocating the body buffer. Trailing bytes after a
  fully parsed packet are a malformed-packet error.
- Default `max_packet_size` was lowered to ~2 MB in 2.1 and is advertised to
  v5 clients in CONNACK and enforced both directions. `message_size_limit`
  separately bounds payloads; an over-limit QoS 1/2 PUBLISH still completes its
  handshake with an error reason code and is not forwarded.
- Session state per client: subscription set, inbound/outbound in-flight lists,
  outbound queued list, last packet ID, session expiry interval, will.
- Keep-alive: client dropped after 1.5x keepalive of silence. Implemented as a
  time-indexed ring of client buckets so each tick inspects one slot.
  `max_keepalive` caps the value; v5 clients receive a Server Keep Alive.
- Limits enforced and advertised: Receive Maximum, topic alias maximum (both
  directions), per-listener max QoS, retain availability, zero-length client
  ID handling with an assigned-ID prefix.
- v5 properties: user properties, message expiry (stored as an absolute
  deadline, checked at send time), session expiry, request/response,
  subscription identifiers, topic aliases, will delay, content type, enhanced
  auth (AUTH exchange). Subscription options: no-local, retain-as-published,
  retain-handling 0/1/2. No-local on a shared filter is a protocol error.
- Retained delivery on SUBSCRIBE follows retain-handling; re-subscribing an
  existing filter re-sends only under v5 "send always".

Adopt: incremental length decoding with pre-allocation size check; per-listener
version allow-list; timer-wheel keepalive; advertise every enforced limit.
Avoid: accumulating non-standard escape hatches; make them explicit flags.

## 2. Connection management

- Single-threaded reactor: housekeeping (retained expiry, keepalive slot,
  session expiry, delayed wills, `$SYS`, autosave) then block in the
  multiplexer with a computed deadline. Write interest toggled only while a
  client has pending output.
- Listeners: multiple TCP/Unix/TLS/WebSocket listeners with per-listener
  settings. Separate per-listener and global connection caps; a distinct cap
  for connected + persistent sessions. TLS with cert or PSK; cert subject as
  username. Built-in WebSocket support and PROXY protocol in 2.1.
- Takeover: a CONNECT with an in-use client ID marks the old connection as
  superseded (v5 gets "session taken over"). With clean_start=false and an
  unexpired session, subscriptions, in-flight state and last packet ID move to
  the new connection. Clean start wipes the old session. The old will fires
  only if the session was ending anyway.
- Auth ordering on CONNECT: validate everything first, then enhanced auth if
  an auth method is present, else TLS-derived identity, else the basic-auth
  plugin chain; plugins may complete auth asynchronously.

Adopt: computed-deadline loop, takeover with state transfer before killing the
old socket, separate global vs per-listener caps.
Avoid: blocking the reactor on TLS handshakes or auth I/O.

## 3. QoS behavior

- Outbound per-client state splits into *in-flight* (sent, awaiting ack,
  bounded by `max_inflight_messages`, default 20; 1 gives strict ordering) and
  *queued* (waiting for a slot, bounded by count and bytes).
- When queue limits are hit, new messages for that client are dropped with a
  warning, counted separately for QoS 0 vs 1/2. QoS 0 to an offline persistent
  session is dropped unless explicitly enabled.
- Inbound QoS 2 tracked by packet ID; a DUP re-send just re-emits PUBREC.
  Inbound QoS 1 is at-least-once with no dedupe. On resume, inbound QoS 2 state
  survives; partial lower-QoS state is discarded.
- No timer-based retransmission on a live connection. MQTT 5 forbids it and
  TCP either delivers or dies. On reconnect, outbound messages are reset to
  their initial state and queued messages are promoted into in-flight slots.
- In-flight limit > 1 permits reordering across retries.
- Message-store IDs are 64-bit k-sortable: node bits + seconds + sub-second.

Adopt: in-flight/queued split with count and byte budgets; resend only on
session resume; explicit per-message state enum; k-sortable message IDs.
Avoid: silent drops; expose drop counters per client and reason.

## 4. Persistence

- Built-in: single snapshot file with magic, CRC, version, and typed chunks:
  config, message-store entries, client entries, client-message links
  (packet ID, direction, state, flags), subscriptions, retained references.
  Written to a temp file then renamed. Only persistent sessions are saved.
- Triggers: interval (default 30 min), change count, signal, clean shutdown.
  Everything between snapshots is lost on crash.
- `persistent_client_expiration` purges sessions that never return; a
  non-standard supplement to v5 session expiry.
- 2.1 SQLite plugin: write-as-you-go through persistence *events* (message
  add/delete, retained set/delete, client add/update/delete, subscription
  add/delete, client-message add/update/delete, will add/delete, restore).
  Tunables: sync level, flush period (batched writes).

Adopt: persistence as an event stream mapping onto a WAL/KV store; one base
message referenced by many client-message rows; restore as first startup phase.
Avoid: long snapshot pauses; a second non-standard expiry when v5 session
expiry can be persisted.

## 5. Queueing, retained, shared subscriptions, $SYS

- Retained messages live in their own topic tree; empty payload clears and
  prunes; expiry swept periodically; wildcard lookup walks the tree.
- Shared subscriptions `$share/<group>/<filter>` in a parallel tree; delivery
  rotates the group's subscriber list (round-robin). Recovered for durable
  clients.
- `$SYS/broker/...` published on an interval: client counts, message/byte
  counters, load averages, retained count, heap, uptime, version, bridge state.

Adopt: separate retained tree; rotate-on-deliver for shared groups; `$SYS` as
scheduled broker publications.
Avoid: per-level hash tables for millions of narrow topics; round-robin that
ignores subscriber backpressure.

## 6. Clustering and bridges

- No clustering, no shared session store, no leader election. The bridge is
  the only distribution primitive: an embedded MQTT client with per-topic
  direction (in/out/both), QoS, and local/remote prefix rewriting.
- Independent clean-session flags for the local and remote side; v5 bridges
  carry session expiry and receive maximum.
- Reconnect: automatic/lazy/once start types; constant or decorrelated-jitter
  backoff with a stability window; multiple addresses for failover or round
  robin; lazy bridges start on queue threshold and stop after idle timeout.
- State topic with a will for the offline value.
- Loop avoidance is weak (a protocol-byte flag understood only by peers of the
  same implementation). No session migration, no cross-path dedupe.

Adopt: bridges remain valuable for edge-to-cloud; keep prefix rewriting,
jittered backoff, lazy start, and state-topic-with-will.
Avoid: presenting bridges as HA; design a real inter-node protocol with session
ownership, subscription gossip, and origin-based loop prevention.

## 7. Error handling

- Handler failures map to v5 DISCONNECT reasons: protocol error, malformed
  packet, QoS/retain not supported, topic alias invalid, receive maximum
  exceeded, packet too large, unspecified. v3 clients simply lose the socket.
  Packets before CONNECT or a second CONNECT are protocol errors.
- CONNECT failures: bad credentials vs not authorized, unsupported version,
  identifier rejected, server busy, bad auth method.
- PUBACK/PUBREC codes: not authorized, quota exceeded, no matching
  subscribers, packet too large. SUBACK carries per-filter codes.
- No built-in rate limiting of auth failures. A global memory ceiling surfaces
  as drops and disconnects rather than graceful backpressure. Queue overflow
  is drop-newest for the affected client only.
- Logging to stderr/file/syslog and `$SYS/broker/log/#`. SIGHUP reloads config.

Adopt: one reason-code enum shared by all paths with a version-aware encoder;
per-filter SUBACK codes; `$SYS` log topics.
Avoid: a global allocator ceiling; prefer per-client/listener budgets that
reject early with a reason code.

## 8. Plugin system

- Plugin API v5 registers callbacks per event: reload, basic auth, extended
  auth, PSK, ACL check (read/write/subscribe/unsubscribe), message in/out,
  control, tick, connect, disconnect, client offline, subscribe/unsubscribe,
  and the persistence family.
- Chain semantics: global plugins then listener plugins; `ignore` = not mine,
  `defer` = looked, no decision; any success/failure ends the chain. All-defer
  on ACL means deny; no plugins means allow. Message callbacks may rewrite
  topic/payload/retain/properties; any non-success return drops the message.
- Helpers: publish as broker, kick by client/username, client accessors,
  async auth completion, `$CONTROL/<plugin>/v1` topics for runtime admin.
- Dynamic security: JSON-backed clients, groups, roles; six ACL types with
  priorities; deny-by-default publish/subscribe.

Adopt: typed event enum with explicit ignore/defer/allow/deny; async auth;
`$CONTROL` topics; persistence as a plugin boundary.
Avoid: in-process plugins with no isolation or timeouts.

## 9. Topic tree and matching

- Validation: non-empty, <= 65,535 bytes, valid UTF-8 without NUL, no
  wildcards in publish topics, `+` occupies a whole level, `#` is last and
  preceded by `/` or at start, `$share/` requires a non-empty wildcard-free
  group, depth limit.
- Matching: a filter starting with a wildcard never matches a `$`-prefixed
  topic; a `$`-filter only matches `$`-topics. Tree of levels; each leaf holds
  subscribers with QoS, no-local, retain-as-published, subscription ID.
  Publish walks literal, then `+`, then `#` children per level; nodes pruned
  when empty.
- Cost is proportional to depth times wildcard branching, not subscriber
  count. Optional dedupe pass for overlapping subscriptions.

Adopt: split levels once and intern them; keep the `$` rule in the matcher;
compact subscription options on the leaf.
Avoid: unbounded recursion; enforce depth early; consider arena allocation.
