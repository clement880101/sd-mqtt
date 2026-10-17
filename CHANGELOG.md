# Changelog

What changes between builds, and what an upgrade costs. The first
release was 0.7.0, numbered for the milestone it completed; the headings
below it are milestones, which is what this project shipped in before
it had releases.

**The one thing to read before upgrading is "Before you upgrade" under
the milestone you are moving to.** Everything else is description.

## Unreleased

### Changed

- **The documentation is split into features and a setup guide.** The
  README is the licence, a table of features, a quick start and the
  configuration reference. Each feature has a page under
  `docs/features/` saying what it does, how to configure it and what it
  reports; `docs/setup.md` goes from nothing to a three-node cluster;
  `docs/README.md` indexes it all. `scripts/test_docs_links.py` follows
  every relative link.

- **The working documents are gone from the repository.** The milestone
  plans, the reference-broker analyses and the milestone 3 demonstration
  were how it was built, not what ships, and the decision records'
  conclusions were already in the design's sections 15 and 17. The design
  is now `docs/design.md` and the component selection `docs/components.md`.

### Fixed

- **At the node a session is moving to**, a forward that arrives right
  behind the drain, before the session is installed, is held for up to two
  seconds and tried again, rather than dropped: `sdmqtt_forwards_held_total`.
  Only after a drain, so a new subscriber is never given a message published
  before it subscribed.

## 0.7.1 — 2026-10-16

### Fixed

- **A publication forwarded to a session's node while the session was
  moving could be lost.** Section 8.3 forwards a publication to the node
  whose register holds the subscriber; section 8.4 moves the session when
  its client reconnects elsewhere; the two arrive over different links.
  A forward that landed after the hand-over found no subscriber and was
  dropped with a debug line. The node now remembers, for thirty seconds,
  which filters each session it gave up held and where it went, and sends
  such a forward on to that node. `sdmqtt_forwards_redirected_total`
  counts those; `sdmqtt_forwards_unroutable_total` counts a forward
  nothing wanted and nothing could be done with, which was uncounted
  before. Found by a cluster test failing once on GitHub's runners; a new
  test makes the race happen on purpose by stalling the link.

- CI: the nightly fuzz job installed no fuzzer and used the wrong
  toolchain; the cluster tests' repair counts are measured from the kill;
  the cluster tests' ports are below Linux's ephemeral range and start
  apart under nextest; the development image has `procps`.

## 0.7.0 — 2026-10-15 — milestone 7: hardening

### Before you upgrade: a store written before this milestone is refused

**A broker built from this milestone will not open a store written by
milestone 6.** The format moved from 10 to 11 for the user records, and
the refusal says so:

```
store format version 10 is not readable by this build, which reads version 11.
```

What to do is what every version move asks: stop the broker, move or
delete the store directory, start the new build. What that throws away
is the milestone 5 list plus, from milestone 6, the replica copies and
stream positions. On a cluster, one store at a time, letting the node
rejoin and be repaired before the next.

### Added

- **A directory of users**, section 12.1, in a crate of its own. A user
  is durable metadata like a rule: written through `POST /api/v1/users`
  on any node, replicated as a register to every other, removed by a
  tombstone. The password is hashed with argon2id (PHC string) and,
  when the request enrols the user with `"scram": true`, derived into a
  SCRAM-SHA-256 verifier at the one moment the password is in hand,
  because a verifier cannot be made from an argon2 hash. Enrolment is
  opt-in because section 12.2 treats a verifier as a private key's
  equal. Neither form is ever returned by the API, and the record's `Debug` says
  `<redacted>`. The directory is a section 9.3 hook — `authenticate` —
  and the broker's connect path does not know users exist. A wrong
  password and an unknown name are one answer. `auth.anonymous` is an
  explicit switch, on by default, that creating a user does not flip;
  `auth.scram_iterations` sets the verifier's rounds and refuses fewer
  than a thousand. `ClientInfo` carries the authenticated username, for
  the access control rules that follow. `argon2` is the one new direct
  dependency.

- **SCRAM-SHA-256 over the AUTH exchange**, section 12.2, with
  re-authentication. The arithmetic is pure functions in the server
  crate, pinned by RFC 7677's one published exchange byte for byte;
  the connection shell runs it before the broker is asked to connect
  anybody, and the CONNACK carries the server's final message so the
  client can verify the server knew its key too. An unknown name gets a
  challenge with a salt of its own and fails at the proof, the same
  answer at the same step as a wrong password. `=2C` and `=3D` in a
  name are unescaped per RFC 5802. Channel binding is not offered; a
  client asking for it, or for any other method, is told 0x8C, and so
  is one on a node with no directory. Re-authentication is the same
  exchange in AUTH packets, answered with `Success` and the server's
  final; a wrong proof, or a proof as another user, disconnects with
  0x87. The user name a SCRAM client proves is what the session
  remembers, whatever the CONNECT's username field said.

- **Access control rules**, section 12.1, deny by default once one
  exists. `[[acl]]` tables in the file and `POST /api/v1/acl` through
  the API take the same shape — identifier, order, subject (`anyone`,
  `user:<name>`, `client:<id>`, where a name may end in `*` to cover a
  prefix and `user:*` is every authenticated connection), filter, and an opinion on publishing
  and on subscribing — and are consulted together by order, first
  match deciding; the API's replicate as registers, the file's are the
  node's own. Filters match the way subscriptions do, through the topic
  crate, and a shared subscription is judged by the filter it shares. A
  refused subscribe is 0x87 (0x80 on 3.1.1), a refused publish 0x87 on
  its acknowledgement, and an at-most-once publish that was refused —
  by a rule or by the reserved namespace — is now dropped and counted
  as `refused`, where before nothing counted it. Paho's
  `test_subscribe_failure` passes on 3.1.1 now that the runner
  configures the rule it needs; on version 5 it stays in the baseline
  as an assumption, because the suite asserts 0x80 where the protocol
  lists 0x87.

- **A client cannot publish into the dollar namespace**, section
  12.3's other half, deferred since milestone 3. Judged in the broker
  before the authorizing chain and told to the hooks as a `reserved`
  refusal; 0x87 on an acknowledged publish, dropped and counted
  otherwise. Only the first level is reserved.

- **A landing page**, `site/index.html`, published with GitHub Pages by
  `.github/workflows/pages.yml`. `scripts/test_site.py`, in CI and before
  every deployment, holds it to its sources: every version it names is
  the workspace's, every figure it quotes is one the spec, a decision or
  the README records, and every link names this repository.

- **Install documentation, checked.** The README opens with installing
  from a release archive, the image, and Compose, and carries a table of
  every configuration section and every `SDMQTT_*` variable;
  `tests/documented.rs` fails naming any variable the configuration reads
  or any section it accepts that the README does not mention, which found
  ten variables and two sections missing. The store-format refusal no
  longer says nothing is released: it says releases before 1.0 do not
  migrate a store.

- **Release archives** for `x86_64` and `aarch64` Linux (static musl,
  cut from the image's own builder) and Intel and Apple silicon macOS:
  `scripts/package.sh <target>` writes `dist/sdmqtt-<version>-<target>.tar.gz`
  with the binary, LICENSE, README and both notices, and its `.sha256`.
  `scripts/test_package.py` unpacks one and runs it — natively, under
  Rosetta, or in a container of its architecture — and requires the
  version, a ready broker and the embedded interface. It caught macOS
  `tar` shipping AppleDouble `._` files in the first archive.

- **A container image.** `services/broker/Dockerfile` cross-compiles the
  broker as a static musl binary with cargo-zigbuild and ships it on
  `scratch` with its licence and notices: about 10 MB, uid 65532, store
  in `/data`, read-only root works. The admin listener binds every
  interface in the image, so it refuses to start without
  `SDMQTT_ADMIN_TOKEN`. `scripts/test_image.py`, in CI, checks a running
  container, including that a durable session and a message queued for
  it survive `docker stop` and `docker start`. The notices generator now
  covers every release target and audits a given `--binary`.

- **Four decisions recorded** in `docs/decisions/`, which section 17
  always said they would be: the SCRAM iteration count stays 4,096 by
  measurement (0002); `SCRAM-SHA-256-PLUS` is not offered (0003); the
  admin listener stays HTTP/1.1 (0004); release artifacts are Linux musl
  and macOS, not Windows (0005).

- **The connection target is met.** 100,000 MQTT connections opened in
  2.5 seconds and held for a minute on one node, at about 32 KB each;
  `scripts/connection-target.sh` measures it in the development image
  with `sdmqtt-load`, a generator that is a workspace member and never
  ships. See `docs/decisions/0001-connection-target.md`. Running out of
  file descriptors is now counted: `sdmqtt_accept_pauses_total`.

- **mimalloc is the global allocator**, section 10.4, on every target
  but Windows, on by default and removable with
  `--no-default-features`; static musl builds use its local dynamic TLS
  mode. `sdmqtt_build_info{version, allocator}` names the build a node
  runs.

- **Two rate limiters**, section 10.3, both the generic cell rate
  algorithm on the broker's coarse clock. **Repeated authentication
  failures from one address are slowed**: after
  `auth.max_failures_per_minute` (10 by default) a connect from it is
  refused with 0x9F before its credentials are read, until the minute
  drains; IPv6 is counted by /64, and the tick sweeps the map. A
  listener's `max_publish_rate` and `publish_burst` cap one session's
  publishes, off by default; over it, version 5 is disconnected with
  0x96 and 3.1.1 closed. `sdmqtt_authentications_total{outcome}`, a
  series registered at zero since milestone 3, now moves;
  `sdmqtt_rate_limited_total{limiter}` and
  `sdmqtt_auth_failure_addresses` are new.

- **The Settings page**, section 11.2's last: paste a key, then manage
  users, access control rules and API keys, and read this node's
  listeners, publish cap included. The key is kept in session storage
  for the tab's life and sent with every request the interface makes.
  `GET /api/v1/listeners` gains `max_publish_rate` and `publish_burst`.

- **API keys**, section 11.1, with `read`, `write` and `admin` scopes.
  `POST /api/v1/keys` mints one and shows it once; the broker stores a
  SHA-256 digest; `DELETE /api/v1/keys/{id}` revokes, and keys and
  revocations replicate as registers. Every mutating endpoint names the
  scope it needs. The configured `admin.token` is now the bootstrap key
  with `admin` scope. **A `write` key cannot manage users, access
  control rules, keys or nodes**, and is answered with the new `forbidden`
  error code and `403`.

- **A listener's maximum QoS and retain availability**, section 5.4's
  two deferred properties. `listener.max_qos` (0, 1 or 2) is advertised
  to version 5 clients and enforced on every publish: above the cap is
  a protocol error, 0x9B on version 5 and a closed socket on 3.1.1,
  which has no code for it. A subscription above the cap is granted at
  the cap, as 3.8.4 allows. `listener.retain_available = false` refuses
  a retained publish with 0x9A and refuses the connect of a client whose
  will is retained, rather than accepting it and dropping the flag the
  client asked for. The defaults advertise nothing and refuse nothing,
  so the conformance baseline is unchanged.

- **A durability mode per listener**, section 8.6's override, deferred
  from milestone 6. `listener.durability = "sync"` makes publishes on
  that listener wait for a follower while the node's mode stays
  `async`, or the reverse. The timeout is the node's in both cases: a
  second timeout would be a second answer to "how long is too long" on
  one node. Refused at startup on a node that does not cluster, as the
  strict flag is, because it would wait for nobody on every publish.

- **Several listeners on one broker.** `[listener]` stays the default
  and is named `mqtt`; each `[[listeners]]` entry is one more, named,
  with its own address, mountpoint and policy over the same sessions,
  subscriptions and store. Two on one address are refused at startup
  by name, rather than the second failing to bind after the first is
  already serving. `GET /api/v1/listeners` lists each with its
  `max_qos`, `retain_available` and `durability`. **One clock:** the
  first listener bound drives the node's one-second tick and the rest
  do not, so a node with three listeners does not age its keep-alives
  three times as fast.

- **TLS listeners, with a certificate that renews without a restart.**
  `kind = "tls"` and a `tls` table naming the certificate and key. The
  pair is read at startup — wrong files refuse to start, naming the
  file — and re-read every `reload_ms`; a renewal is served by the next
  handshake, and a renewal half-written keeps the previous pair and
  logs once. The handshake is bounded by the connect timeout, failures
  are counted, and the two TLS series exist only on a node with a TLS
  listener. rustls with aws-lc-rs, which the rules crate's clients
  already linked; `rustls`, `rustls-pki-types` and `tokio-rustls`
  become direct dependencies of the server crate.

- **MQTT over WebSocket**, `kind = "ws"`, and over TLS, `"wss"`. The
  `mqtt` subprotocol is required and confirmed, every path upgrades,
  MQTT travels in binary frames and a text frame is closed with 1003.
  The connection shell never learns there was a frame: the adapter
  reads binary frames as bytes and writes each burst as one frame,
  flushing inside the write, because tungstenite queues a frame until
  flushed and the shell flushes only at the end of a burst. Refused
  upgrades are counted by reason, on a series that exists only on a
  node with a WebSocket listener. `tokio-tungstenite` and
  `futures-util` are new direct dependencies.

- **The conformance suite runs over TLS and WebSocket as well**, which
  is section 16's exit criterion for milestone 7's listeners. Paho's
  suite speaks plain TCP and nothing else, so `conformance/via.py` — a
  forwarder in the Python standard library, a TLS client on one side
  and an RFC 6455 client on the other — carries it through each layer,
  and the runner compares each pass against the baseline under its own
  label. The failures under TLS and WebSocket are exactly the plain
  pass's. Two defects in the forwarder itself were found by the suite
  and fixed: an SSL socket read and written from two threads at once,
  which hung most first connects, and a close that dropped the
  DISCONNECT the broker had just sent.

## Unreleased — milestone 6: queue replication

### Before you upgrade: a store written before this milestone is refused

**A broker built from this milestone will not open a store written by
milestone 5. It refuses to start, and says so:**

```
store format version 8 is not readable by this build, which reads version 10.
The store is not damaged; it was written by a different build. Nothing is
released yet, so there is no migration: see CHANGELOG.md for what moved and
what starting with a fresh store directory throws away.
```

**What to do.** The same as every other version move: stop the broker,
move or delete the store directory (`storage.directory`, holding
`sdmqtt.redb`), and start the new build, which creates a fresh store.
What that throws away is the list in the milestone 5 section —
durable sessions, queued messages, retained messages, wills, rules,
outboxes, this node's identity, its clock and its replicated
registers. **Throw away one store, not every store:** on a node
already in a cluster the registers come back from the next
anti-entropy exchange, and on the last node of a cluster they are the
cluster's metadata and do not.

**Why the version moved to 10.** Twice, within this one unreleased
milestone: to 9 when a node could hold a copy of another node's
session, and to 10 when the owner's own position in that stream was
stored beside it. The number moved the second time rather than version
9 being redefined, for the reason the check is exact in the first
place — redefining a number already written to somebody's disk is how
a silent misread happens.

A node can now hold a copy of a
session another node owns. The records that make up such a copy are
the same records as any other session's — the session, its
subscriptions, its queue — so the store gained a row saying *this one
is a copy, owned by that node, applied this far*. Without it a restore
would hand the broker another node's session: its filters would enter
this node's matching trie, it would be forwarded publications for
them, and it would deliver them to a client connected somewhere else.

The owner's position is stored rather than counted from zero at
startup because a follower refuses anything at or below its own mark:
an owner that restarted its count would have everything it sent
refused as a duplicate, silently, until the session had produced as
many operations as it had before. A follower that quietly stops
following is the failure replication exists to prevent.

A version 8 store holds neither, so nothing is lost by their absence
— but a version 8 *build* reading a later store would meet record
variants it has no arm for, and the check is exact in both directions
on purpose.

### Added

- **Which nodes hold a session's queue.** Rendezvous hashing over the
  live members picks a replica set per session:
  `cluster.replication_factor`, one to three, two by default, also
  `SDMQTT_CLUSTER_REPLICATION_FACTOR`. Nothing streams anything yet;
  this is the placement and its configuration.

  It is **advice, not authority**. Membership is local judgement, so
  two nodes on opposite sides of a partition compute different sets;
  ownership stays with the epoch-stamped register from milestone 5, so
  that disagreement costs nothing.

- **Queues are replicated.** An owner streams every change to a
  session's queue — an append, an acknowledgement, a removal — to the
  nodes rendezvous puts behind it, and those nodes hold a copy they do
  not serve. One frame per follower rather than a broadcast, which is
  the economy section 8.3 already makes for a publication: telling
  fifty nodes about a queue forty-eight of them hold nothing of would
  cost the cluster its own capacity.

  Nothing reaches a follower until the store has taken the batch,
  which is section 8.2's rule for registers applied to the same
  queue — one description of what happened, and both the store and
  the copy get it.

- **A copy is on the disk, with its payloads.** The first version of
  the stream shipped two defects the gauges could not see. A follower
  wrote queue records for a session its store did not hold, which a
  store refuses, so the copy existed only in the follower's in-memory
  table; and the entries travelled without their payloads, so even a
  stored copy was a queue of references to messages the follower had
  never seen. The first append an owner streams for a session now
  carries the session record, and every append carries the payload it
  names. The three-node test kills and restarts each follower and
  asserts the copies came back, because a restart is the only question
  a gauge cannot answer from memory.

  A third, smaller one beside them: a session that was handed over,
  expired or taken over never left the owner's stream table, so the
  node went on counting it as replicated and a client reconnecting
  under the same identifier would have had its new stream numbered
  from where the old one stopped — which a follower still holding the
  old copy would have refused as duplicates.

  And a fourth, which the migration test found on about half its
  runs and which is a race rather than a flake: a client reconnecting
  to the node that holds the copy of its session, at the moment that
  copy is arriving. The copy's write and the connect's write meet on
  one writer; when the copy lands first, the drain's queue entries
  carry sequence numbers the copy already used, the store refuses the
  whole connect batch as a reused sequence — correctly — and the
  subscription register in that batch is abandoned with it, so no
  other node ever learns the claiming node has the subscriber. The
  copy now yields to the drain, written ahead of the connect on the
  same writer. A test that waited for the copy before reconnecting
  passed every run, with and without the fix, because the wait took
  the race away; the test reconnects without waiting, as a client
  would.

- **A replica set that is short repairs itself**, and a node can be
  taken out of membership. Section 8.6's re-replication. A node that
  is down keeps its place in every replica set until
  `cluster.gone_after_ms` runs out — an hour by default, because a node
  that is down is usually restarting and copying the whole queue estate
  on every restart is what the grace prevents — and is then given up
  on: the sets that included it are repaired onto the nodes that
  remain, each session's queue sent to its new follower as one
  snapshot, at most `cluster.repair_sessions_per_second` a second.

  **`DELETE /api/v1/nodes/{id}`** forgets a node at once, on the node
  asked. It is the first thing in this design that makes membership
  smaller: until now a peer once met was remembered as `down` for
  ever, which is the hole repair could not fill, because a set is
  repaired *away* from a node and a node never forgotten is never
  repaired away from. A forgotten node that comes back is re-admitted.

  A snapshot does not consume a stream position, and that is the
  design: a new follower starts at zero while the owner's stream is at
  some position the other followers have reached, and re-sending the
  queue as ordinary appends would either put the others behind or
  leave the newcomer with a hole nothing fills. The snapshot carries
  the owner's position, the follower takes it whole, and the next live
  change is the next change for everybody.

  `sdmqtt_replica_repairs_pending` and `sdmqtt_replica_repairs_done_total`
  are section 8.6's "progress visible".

- **Losing the owner does not lose the queue.** Section 8.6's failover.
  When a drain comes up short — the previous owner cannot be reached,
  or does not finish in time — the claiming node asks every reachable
  member of the session's replica set how far its copy goes, weighs
  its own copy against the answers, and pulls from whichever is
  furthest ahead. **The drain still comes first**, because the owner
  holds the authoritative queue and asking it is the complete answer;
  the pull is what happens on the timeout that used to be counted as a
  shortfall, and the shortfall is now what is still missing *after* the
  pull. A drain that fell short by five and a pull that brought all
  five reports nothing lost, because nothing was.

  The copy furthest ahead, **not the nearest**. Under any partition
  worth failing over from, the nearest copy is the one that was cut
  off with the claiming node and is furthest behind, and it answers
  first because it is nearest. The three-node test holds one
  follower's link back, kills the owner, and reconnects the client to
  the laggard — which must pull from the other node anyway.

  Two counters beside the migration ones: `sdmqtt_sessions_pulled_total`
  and `sdmqtt_pulled_messages_total`. And one on the follower's side,
  `sdmqtt_replica_changes_applied_total`: the gauges say what a node
  holds, and a node that holds copies and has quietly stopped applying
  to them looks the same on all three. A flat line here on a node that
  is meant to be following is that node. A copy holds the queue and only
  the queue — no subscriptions, no inflight window — so a client that
  failed over has its messages and re-subscribes, which is what
  section 8.6 replicates and all it says it does.

- **`cluster.durability`, with `"sync"` available.** `async` is the
  default and is what the lag gauge is about. `sync` holds the
  acknowledgement until one follower has confirmed the append, so a
  single node failing loses nothing; the cost is one local round trip
  per durable publish, and it is one round trip rather than one per
  copy because section 8.6 asks for at least one.

  **With no follower to ask, it is acknowledged anyway and counted**
  as `sdmqtt_publishes_unconfirmed_total` — a cluster of one, a factor
  of one, every follower down. Refusing would turn the loss of one
  node into an outage for every durable publish, which is the trade
  section 8.5 already made the other way. `cluster.strict.unconfirmed`
  is there for a deployment that wants the refusal, and the refusal
  carries reason code 0x83 rather than a closed socket.

  `cluster.sync_timeout_ms` bounds the wait. A follower that has gone
  quiet is the case synchronous mode exists for, and it is also the
  case where waiting without a bound turns one slow node into every
  publisher's outage, so running out counts as unconfirmed rather than
  holding the client.

  **The mode applies to `POST /api/v1/publish` and to ingest rules as
  well.** The first version waited for the copy on that path and
  then threw the answer away, so an operator who set `sync` and
  published by hand to check it was told `durable: true` by the store
  alone. Now an unconfirmed answer is counted on the same series, and
  with the strict flag refused with `503` and the code `unavailable`
  (a new error code). The message is kept; the promise is refused.

  **A placed follower that has not reported yet is waited for.** Found
  by the exit-criteria test: "has a follower" meant "a follower has
  reported a mark", so the first few publishes to every fresh session
  were acknowledged unconfirmed, and counted, on a cluster with
  nothing wrong with it — three of eight. The owner now knows who
  placement names for each session and waits for one that can be
  reached; a dead one is nobody to wait for, as before.

  **Both modes describe the node that accepted the publish.** A
  publish accepted by one node for a session owned by another is
  forwarded under section 8.3, and the accepting node answers when it
  is done — before the owner has appended, let alone a copy. The
  exit-criteria tests publish on the owner for that reason, and the
  spec now says so where the modes are defined. It is written down as
  a gap rather than filled: filling it is an acknowledgement across
  the mesh, which is a design of its own.

- **Three gauges for it**: `sdmqtt_replication_lag`,
  `sdmqtt_replicated_sessions` and `sdmqtt_replica_sessions`. The
  first is section 8.6's loss window — in asynchronous mode what a
  sudden loss of a node would cost **is** the lag — and it is measured
  from the copy that is furthest behind, because a window measured
  from the closest one describes a failure that did not happen. Absent
  rather than zero on a node that does not cluster, like every other
  cluster series.

  The lag counts the followers placement names, and no others. Found
  on the Cluster page: a removed node's last mark stayed in the
  owner's table and the gauge grew by one with every publish, for
  ever. A node dead *within* the grace still counts, because its copy
  is still in the set and how far behind it is is the loss window; a
  node given up on or forgotten does not, and forgetting one purges
  its marks.

- **The Cluster page shows replica health and re-replication
  progress**, which is the last of milestone 6's exit criteria. Four
  gauges from `GET /health/cluster`'s new `replication` object: the
  durability mode; sessions streamed and copies held; the lag; and
  sessions short of a copy, with how many have been re-streamed so
  far. In synchronous mode a fifth appears when any acknowledgement
  went unconfirmed. Three columns join the membership table — copies
  of this node's sessions the peer has confirmed, how far behind it
  is, and copies of the peer's sessions held here — and the page says
  in words that these are this node's numbers, as every state on it
  is. Checked against three real brokers: a session queued, a follower
  killed and forgotten, and the repair watched happen.

- **`unavailable`, a new API error code**, answered with `503`: the
  request was well formed and taken, and the guarantee it asked for is
  what is not available right now. Only synchronous mode's strict
  refusal answers it so far.

- **A follower's half of the stream**, and what it keeps across a
  restart. Changes to a session's queue carry a stream position, and a
  follower applies them in order: a position after a hole is held
  rather than applied, and the high-water mark it reports is the
  highest *contiguous* position — a mark that skipped a hole would
  make failover prefer the copy with a message missing from the
  middle.

## Unreleased — milestone 5: the cluster mesh

### Before you upgrade: a store written before this milestone is refused

**A broker built from this milestone will not open a store written by
milestone 4. It refuses to start, and says so:**

```
store format version 4 is not readable by this build, which reads version 7.
The store is not damaged; it was written by a different build. Nothing is
released yet, so there is no migration: see CHANGELOG.md for what moved and
what starting with a fresh store directory throws away.
```

**The same applies to a version 5 or version 6 store**, either of which
is one written by an earlier build of this same unreleased milestone:
this milestone has moved the format three times — once for the node's
identity, once for its clock, and once for the replicated registers
themselves — and the check is exact.

The file is not damaged and the check is **exact rather than a
range**, for the reason the milestone 4 section below gives at length:
reading healthy bytes with the wrong layout restores nonsense.

**What to do.** Stop the broker, move or delete the store directory
(`storage.directory`, holding `sdmqtt.redb`), and start the new build,
which creates a fresh store. What that throws away is what the store
held, and the list is the one in the milestone 4 section — durable
sessions, queued messages, retained messages, wills, rules and
outboxes — **with three additions: this node's identity, its clock,
and the replicated registers it held.** A fresh store mints a fresh
UUID, so as far as the rest of a cluster is concerned the node that
comes back is a new one; and a fresh store holds no clock reading, so
the node starts its clock from the wall clock rather than resuming.
Starting a *fresh* store from zero is safe — there are no earlier
writes of this node's for a new stamp to collide with — and that is
exactly why throwing the store away is the instruction rather than
keeping half of it. The registers go with it, which on a node that is
already in a cluster costs nothing: it re-announces its filters as its
clients re-subscribe and the next anti-entropy exchange hands it back
everything the cluster holds. **On the last node of a cluster it costs
the cluster's metadata**, so the instruction is the same one it has
always been: throw away one store, not every store.

**Why the version moved to 5.** Section 8.1 gives each node a stable
identity — a UUID v7 minted on first start and a name beside it — and
it is stored, because a node that minted a new one on every restart
would stop being the node its peers know without ever saying so. That
added one record variant, a record to go in it, and one global row in
each backend. A version 4 store opened by this build holds no
identity, and the node reading it would found itself a second time.

**Why it moved again to 6.** Section 8.2 stamps every replicated
record with a hybrid logical clock reading, and **convergence is
conditional on no two writes ever sharing a stamp.** A clock cannot
issue a duplicate, but a clock that restarts behind itself can, so a
node resumes from the highest reading it has ever written, read back
from the store. That added one more record variant, a record to go in
it, and one more global row in each backend. A version 5 store opened
by this build holds no reading, so the node would resume its clock
from zero and re-issue stamps it had already used — and two values
under one key carrying one stamp is the single divergence no later
exchange repairs, because each side keeps its own and believes it
already holds the winner.

**Why it moved again to 7.** Section 8.2's registers are now stored,
which is what makes a restart come back knowing what the cluster was
told. That added one more record variant, the register and the three
types it is made of, and one more table in each backend. The failure a
version 6 store would produce is the quietest of the lot: **a node
would come up holding no registers at all and looking perfectly
healthy** — routing nothing to any peer, serving no replicated rule,
claiming no session it owned — and would then re-create what it held
locally under fresh stamps, which beat the tombstones every other node
still holds. A restart that resurrects everything this node ever
deleted.

### Added

- Section 8.2's hybrid logical clock, which is what makes two nodes'
  writes comparable: milliseconds from the wall clock, then a counter
  that moves when the millisecond does not. It is the first wall-clock
  read on a write path in this broker — the existing coarse clock is a
  count from an arbitrary origin, and two nodes' counts are not
  comparable. Two decisions in it reverse the obvious implementation,
  and both are there because convergence is conditional on no two
  writes ever sharing a stamp:
  - **A peer whose reading is further ahead than the configured bound
    is refused, not clamped.** Clamping still advances the local clock,
    so one node with a wrong year would poison every node it reached,
    permanently, and no later write anywhere could win a conflict
    against it. Refusing confines the damage to that node's own frames.
  - **A node resumes its clock from the highest reading its store
    holds, not from zero.** The reading rides in the same batch as the
    write it stamps, so a stamp that survived a crash has its mark
    beside it and a stamp that did not was shown to nobody.
- Section 8.2's last-writer-wins register, which is how two nodes
  resolve a conflict without talking about it: a value, a stamp, and
  whether the value is there at all. The order is the reading's
  milliseconds, then its counter, then the writing node's UUID bytes,
  and all three are load-bearing — milliseconds alone leave two nodes
  that wrote in the same millisecond each keeping their own, the
  counter is what stops a node being ambiguous with itself, and the
  identity is what breaks the tie that is left. **A deletion is a
  tombstone competing by that same comparison and not a special case**:
  "a delete always wins" diverges, because two nodes that saw a delete
  and a later write in different orders keep different things.
- **Section 8.2's four replicated kinds, and what each of them is
  keyed by**: that a node has at least one subscriber for a filter,
  retained messages, rules, and which node owns a session. The
  vocabulary is open, so milestone 7's users, access control rules and
  API keys are a new variant rather than a new mechanism. A deletion of
  any of them is a tombstone competing by the register's one
  comparison; there is no removal record, because a node that dropped
  the row would be handed its own older copy by the next peer it spoke
  to and would resurrect what it deleted.
- **Subscriptions replicate at node granularity, not per client.** What
  travels is that a node has a subscriber for a filter; which of its
  clients that is never leaves the node. A register per client
  subscription would be fifty nodes holding a hundred thousand
  registers each at the design ceiling, against an idle memory budget
  of thirty megabytes. So each node keeps a reference count per filter,
  local and unreplicated, and writes a register only when that count
  leaves or reaches zero — **a second subscriber to a filter broadcasts
  nothing, and the last unsubscribe broadcasts a tombstone.** The
  departure is emitted on all three paths a filter can go on, not just
  the one anybody thinks of: UNSUBSCRIBE, a connect that starts clean
  (including a clean-start takeover), and a session expiring. A
  departure missed on any of them leaves the node advertised for a
  filter nobody is subscribed to, and a restart does not repair it.
  A shared subscription counts under the filter a published topic is
  matched against, not under its group: `$share/one/a/b` and
  `$share/two/a/b` are two subscribers to `a/b`.
- Section 8.1's node identity: a UUID v7 and a name, minted on first
  start and read back unchanged afterwards. Renaming a node moves the
  name and not the UUID. See `[node]` in `README.md` and
  `SDMQTT_NODE_NAME`.
- Section 8.1's authenticated link: a mutual challenge over a
  length-prefixed transcript, keyed with a shared cluster secret that
  is never sent. **A node with no secret will refuse to cluster, and
  an empty secret is refused for being empty rather than read as
  absent** — there is no loopback exemption, because a link grants
  session takeover and unauthenticated writes to every replicated
  register. Nothing dials a peer yet, so there is nothing for an
  operator to configure until there is; the refusal is already the
  only way to build a usable configuration.
- The framing those links carry: a 32-bit little-endian length
  **covering the version byte and the body**, bounded by a configured
  maximum that is refused from the declared length alone, before
  anything is reserved for a frame that will never arrive. Writes are
  gathered and flushed on a size or a time threshold, and the queue to
  one peer is bounded: past the bound, messages to that peer are
  dropped and counted rather than blocking whoever sent them, and the
  link carries on.

- Section 8.1's mesh. A node given `cluster.address` listens for peers,
  dials its seeds, and forms a full mesh: every link authenticates
  before it carries anything, a heartbeat every second covers a
  connection that is open and silent, and reconnection backs off with
  jitter — harder after a peer refuses the secret, which is a
  configuration error that will not fix itself.
- **A node advertises the address peers should dial**, which is not its
  bind address. `cluster.advertise`, defaulting to the bind address and
  **required** when that address is a wildcard: a node bound to every
  interface has no address a peer can use, and nothing can guess which
  one was meant. It is what lets a node redial a peer it met but was
  never seeded with.
- **Two nodes dialling each other at once settle on one link.** The one
  that survives is the one dialled by the **lower identity**, which
  both ends work out from the two identities without another message.
  A link with no rival is never dropped for its role.
- Section 11.3's four cluster metrics, which were deferred to this
  milestone and are deferred no longer: `cluster_members`, and
  `cluster_bytes_sent`, `_received` and `_dropped`. They are registered
  **only on a node that clusters**, so a node that does not leaves them
  absent rather than reporting a zero that reads as "the links are
  quiet".
- Configuration: a whole `[cluster]` section, and
  `SDMQTT_CLUSTER_NAME`, `_SECRET`, `_ADDRESS`, `_ADVERTISE` and
  `_SEEDS` beside it. An empty `SDMQTT_CLUSTER_SECRET` is refused for
  being empty, as `SDMQTT_ADMIN_TOKEN` is.
- **Section 8.5's partition policy. A node cut off from the cluster
  keeps serving by default**, and there is nothing to set for that: a
  broker that refused its reachable clients would turn one node's
  network fault into every client's outage. `cluster.strict.connects`,
  `.publishes` and `.subscribes` refuse the one operation each names
  while the cluster is short of `cluster.expected_size`, for operators
  who want the other trade, with `SDMQTT_CLUSTER_EXPECTED_SIZE` and
  `SDMQTT_CLUSTER_STRICT_CONNECTS`, `_PUBLISHES` and `_SUBSCRIBES`
  beside them.

  A refusal carries a reason code rather than a closed socket wherever
  the protocol has one to put it in: 0x88 in a CONNACK, which a 3.1.1
  client reads as return code 3, and 0x83 in a SUBACK per filter and
  in a PUBACK or PUBREC. At at-most-once, and for a 3.1.1 publisher at
  any level, nothing can carry one — a 3.1.1 PUBACK means *accepted* —
  so the connection is closed, which is already what this broker does
  when it cannot acknowledge a publish. Those closes are counted as
  `sdmqtt_sockets_closed_total{outcome="not_whole"}`, a new value of
  that label.

  **A strict flag with no `cluster.expected_size` is refused at
  startup**, and so is one on a node with no `cluster.address`.
  Membership is learnt from seeds, so the nodes a node has met cannot
  say whether the cluster is whole — three nodes that never heard of a
  fourth look exactly like four with one down — and a cluster of one
  is never short of anything. A flag that can never fire is refused
  rather than left to mean nothing quietly.

- **The Cluster page**, which is section 11.2's sixth and the last
  piece of milestone 5's scope. It shows how many members are
  answering against how many are expected, whether that view is
  complete, and a row per node with its name, identity, advertised
  address, state and — for this node, which is the only one membership
  carries them for — its version and uptime.

  It says in words whose view it is showing. Membership is not
  replicated, so every state on it is **this node's own judgement**,
  and two nodes' pages disagreeing during a one-way partition is what
  a partition looks like rather than a fault in one of them. It also
  says which kind of expected size it is reporting, because
  `3 of 3, complete` means something quite different when the three is
  a number an operator wrote down than when it is the number of nodes
  this one happens to have met.

  It polls, and it is the only page that does: there is no membership
  event to follow, because a node's opinion about a peer is not news.

- **`GET /health/cluster` says whether its expected count was
  configured**, as `expected_configured`. Without it the document is
  reassuring rather than informative: a node that came up alone and
  never heard of the others reports a complete cluster of one, and
  nothing in the numbers distinguishes that from a healthy cluster of
  one that was configured as such. Additive, so an existing reader is
  unaffected.

- **Section 8.4's migration numbers are in `/metrics`.** Seven
  counters: `sdmqtt_sessions_claimed_total`,
  `sdmqtt_sessions_migrated_total`,
  `sdmqtt_sessions_migrated_incomplete_total`,
  `sdmqtt_sessions_migrated_unreachable_total`,
  `sdmqtt_migrated_messages_total`,
  `sdmqtt_migrated_messages_missing_total` and
  `sdmqtt_migrated_messages_late_total`. The third is the one the
  design has always promised to surface — the shortfall when a drain
  times out or the previous owner cannot be reached — and it was
  counted inside the process and exposed nowhere. Absent rather than
  zero on a node that does not cluster, like the other cluster series.

- **A split-brain client identifier resolves when the partition
  heals.** Two nodes that cannot see each other both accept the same
  client identifier — that is the availability above, working as
  intended — and until now both went on serving it afterwards, with
  the ownership register settled and nothing acting on it. Now the
  node whose claim lost the merge closes its connection with
  `0x8E Session taken over`, drops the session's subscriptions so it
  stops being forwarded copies, and says so in the log.

  **What that session's queue held is lost, and that is the
  documented behaviour rather than an oversight**: the node that won
  accepted its client with whatever reached it, which across a
  partition was nothing, and section 8.5 of the design says messages
  published on either side are not retroactively exchanged.

  Nothing is needed to turn this on and there is nothing to configure.
  A client that is disconnected this way has been told why and can
  reconnect, which is what the reason code is for.

### Changed

- **`GET /health/cluster` reports the configured expected size.** With
  `cluster.expected_size` set, `expected` is that number and `status`
  is `complete` once `members` has reached it. Unset, it reports the
  count of nodes this one has met, which is what it has always
  reported and what carries the blind spot above: it says `complete`
  on a node that came up alone. More nodes answering than expected is
  `complete` rather than a disagreement, so growing a cluster before
  editing the key does not page anybody.
- **Rules stop being node-local**, which is what section 9.1 and
  milestone 4 said would happen here. Installing, editing or removing
  one still writes this node's own copy and now writes the
  last-writer-wins register that carries it to every peer, in the same
  batch: a rule stored without its register would be a rule this node
  runs and nobody else has, and a removal writes a tombstone rather
  than dropping the register, because a register merely dropped comes
  back from the first peer this node speaks to and reinstalls the rule
  an operator deleted. Nothing broadcasts yet, so the register is
  written and held rather than sent; what changed is that a rule is in
  the replicated vocabulary and is stamped by the same clock as
  everything else.
- **The admin API reports this node's real identity.** `/api/v1/nodes`
  answered with the word `local`, hardcoded, on a binary that has had
  a UUID since the first task of this milestone. It now reports that
  UUID, the node's name and the address it advertises, and lists every
  peer beside it in identity order. Two fields changed shape: `version`
  and `uptime_seconds` are now `null` for a peer, because membership
  carries an identity and not a build, and reporting this node's
  version beside a peer's row would be wrong exactly during an upgrade.
  `/health/cluster` answers what this node can actually see, and says
  `incomplete` when a peer is missing or merely suspect. A node that
  does not cluster still answers a cluster of one.
- **Clustering requires a store, and is refused without one.** A
  node's identity is minted once and written down; a node with no
  `storage.directory` would be a new node to its peers after every
  restart, would own a singleton it had already given up, and would
  break the uniqueness conflict resolution depends on. Running with no
  store at all is still a supported mode — asking that node to cluster
  is not.
- The cluster greeting moved to version 2 and its transcript to `v2`,
  because the greeting now carries the advertised address and the
  transcript covers it. Nothing has shipped that speaks version 1.
- **Singleton ownership now orders nodes by that UUID.** It ordered
  them by a string whose meaning was this milestone's to decide, and
  the decision is the UUID: the name is for operators and is never
  compared, because two copied configurations share one. The type that
  orders now carries no name at all, so the comparison section 8.1
  forbids is one that does not compile. Nothing an operator can see
  changes while there is one node.

## Unreleased — milestone 4: the hook interface and four connectors

### Before you upgrade: a store written before this milestone is refused

**A broker built from this milestone will not open a store written by an
earlier one. It refuses to start, and says so:**

```
store format version 3 is not readable by this build, which reads version 4.
The store is not damaged; it was written by a different build. Nothing is
released yet, so there is no migration: see CHANGELOG.md for what moved and
what starting with a fresh store directory throws away.
```

This is not corruption and the file is not damaged. The stored format
version moved from 3 to 4, and the check on it is **exact rather than a
range**: a version this build does not know is refused rather than
guessed at, because reading healthy bytes with the wrong layout restores
nonsense from a file that is perfectly intact.

Nothing is released, so there is no migration and none is planned for
this step. The design says so in as many words — section 9.2: "Nothing
is released, so the cost is a line in the release notes rather than a
migration." This is that line, and it is here because it was missing:
an operator who upgrades and finds the broker refusing to start deserves
to have been told beforehand rather than to have to read an error
message and a source file.

**What to do.** Stop the broker, move or delete the store directory
(`storage.directory`, holding `sdmqtt.redb`), and start the new build,
which creates a fresh store. What that throws away is what the store
held: durable sessions and their subscriptions, messages queued for
offline sessions, retained messages, wills, the rules stored on this
node, and anything waiting in a rule's outbox. Clients reconnect and
resubscribe; retained messages are republished by whatever publishes
them; rules named in `sdmqtt.toml` are installed again from the file,
and rules created through the UI or the API on the old store are not —
export them first if you want them, from `GET /api/v1/rules`.

**Why the version moved.** Section 9.2's durable outbox is its own
record type in the store rather than a queue wearing a session's
clothes, which added three record variants, an outbox record to go in
the first of them, and a table and a counter row in each backend. A
version 3 store opened by this build would come up with every rule's
outbox empty — which is not an empty outbox but an undelivered message
nobody will ever look for again.

When migrations exist, the exact check becomes a range. Until then, this
is the deal: the format is allowed to move while nothing is released,
and the cost is this section.

### Added

- Section 9.3's hook interface: connectors sit on it rather than being
  special-cased in the publish path.
- Four connector types — `http_out`, `http_in`, `http_poll` and
  `mqtt_bridge` — each with a durable outbox where it needs one. See the
  configuration reference in `README.md`.
- The Rules page: a per-action form editor, per-rule statistics with
  sparklines, and the recent dead letters per rule.
- Per-rule metrics on `/metrics`, labelled by rule identifier and
  outcome, and a counter for publications a connector made that went
  nowhere. See section 11.3 of the design.

### Changed

- A message hook may lower a message's quality of service and may not
  raise it past what the publisher sent; a raise is clamped. Section 9.3
  permitted the rewrite without saying which directions were meant, and
  raising has protocol consequences the broker cannot honour: the
  promise a level makes was made on the way in, to a publisher that has
  already gone.
