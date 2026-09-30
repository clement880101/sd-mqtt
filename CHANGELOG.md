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
store format version 4 is not readable by this build, which reads version 5.
The store is not damaged; it was written by a different build. Nothing is
released yet, so there is no migration: see CHANGELOG.md for what moved and
what starting with a fresh store directory throws away.
```

The file is not damaged and the check is **exact rather than a
range**, for the reason the milestone 4 section below gives at length:
reading healthy bytes with the wrong layout restores nonsense.

**What to do.** Stop the broker, move or delete the store directory
(`storage.directory`, holding `sdmqtt.redb`), and start the new build,
which creates a fresh store. What that throws away is what the store
held, and the list is the one in the milestone 4 section — durable
sessions, queued messages, retained messages, wills, rules and
outboxes — **with one addition: this node's identity.** A fresh store
mints a fresh UUID, so as far as the rest of a cluster is concerned
the node that comes back is a new one. Nothing else in a cluster
depends on that yet, because nothing joins one yet; it will.

**Why the version moved.** Section 8.1 gives each node a stable
identity — a UUID v7 minted on first start and a name beside it — and
it is stored, because a node that minted a new one on every restart
would stop being the node its peers know without ever saying so. That
added one record variant, a record to go in it, and one global row in
each backend. A version 4 store opened by this build holds no
identity, and the node reading it would found itself a second time.

### Added

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

### Changed

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
