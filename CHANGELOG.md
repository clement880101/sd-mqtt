# Changelog

What changes between builds, and what an upgrade costs. Nothing is
released yet, so there are no version numbers here; the headings are
milestones, which is what this project actually ships in.

**The one thing to read before upgrading is "Before you upgrade" under
the milestone you are moving to.** Everything else is description.

## Unreleased — milestone 5: the cluster mesh

### Before you upgrade: a store written before this milestone is refused

**A broker built from this milestone will not open a store written by
milestone 4. It refuses to start, and says so:**

```
store format version 4 is not readable by this build, which reads version 6.
The store is not damaged; it was written by a different build. Nothing is
released yet, so there is no migration: see CHANGELOG.md for what moved and
what starting with a fresh store directory throws away.
```

**The same applies to a version 5 store**, which is one written by an
earlier build of this same unreleased milestone: this milestone has
moved the format twice, once for the node's identity and once for its
clock, and the check is exact.

The file is not damaged and the check is **exact rather than a
range**, for the reason the milestone 4 section below gives at length:
reading healthy bytes with the wrong layout restores nonsense.

**What to do.** Stop the broker, move or delete the store directory
(`storage.directory`, holding `sdmqtt.redb`), and start the new build,
which creates a fresh store. What that throws away is what the store
held, and the list is the one in the milestone 4 section — durable
sessions, queued messages, retained messages, wills, rules and
outboxes — **with two additions: this node's identity, and its
clock.** A fresh store mints a fresh UUID, so as far as the rest of a
cluster is concerned the node that comes back is a new one; and a
fresh store holds no clock reading, so the node starts its clock from
the wall clock rather than resuming. Starting a *fresh* store from
zero is safe — there are no earlier writes of this node's for a new
stamp to collide with — and that is exactly why throwing the store
away is the instruction rather than keeping half of it.

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

### Changed

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
