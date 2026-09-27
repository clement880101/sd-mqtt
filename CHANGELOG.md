# Changelog

What changes between builds, and what an upgrade costs. Nothing is
released yet, so there are no version numbers here; the headings are
milestones, which is what this project actually ships in.

**The one thing to read before upgrading is "Before you upgrade" under
the milestone you are moving to.** Everything else is description.

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
