# Poller and Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The remaining two connector types. A poller fetches a URL on an
interval and publishes the body. A bridge connects to a remote broker as a
client and moves messages in either direction without looping them back.

**Architecture:** Both are singletons, owned by the live member with the
lowest node identifier. With one member that is this node, but the selection
is written as a rule over the membership view, because a rule only ever
exercised in the one-member case is a rule nobody has tested.

The bridge is built on this project's own codec and framing reader, not a
client library. That decision is recorded with its reasons.

**Tech Stack:** The HTTP client from the outbound connector, `mqtt-codec`,
`sdmqtt-server`'s framing reader, `tokio-rustls` for a bridge over TLS.

---

## Read first

- `docs/conventions.md`, all eight.
- Section 9.2 of `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`,
  the `http_poll` and `mqtt_bridge` paragraphs, the singleton paragraph, and
  the three paragraphs on loop prevention added before these plans.
- The section "Verified again before milestone 4" in
  `docs/research/component-selection.md`, whose list of what a bridge needs
  beyond the codec is the task breakdown for task 3.
- `crates/rules/src/http_out.rs` for the client and the retry discipline.

## What is already decided

**No client library.** The one considered fails this project's advisory gate
today through a pin only its maintainer can move, would link a second
cryptography provider, a second root-trust mechanism, a second logging facade
and a second MQTT codec, and keeps its in-flight state in private in-memory
fields so the durable bridge would have to be written anyway.

**Loop prevention is a configuration-time rule first and a marker second.**
The marker is an MQTT 5 user property and 3.1.1 has none, so a bridge whose
inbound and outbound filters intersect after rewriting is refused outright,
on both versions. A bridge whose state topic matches its own outbound filter
is refused for the same reason.

**Connector-originated publications carry a hop count**, because the third
loop has no bridge in it: an outbound rule feeding a system that posts back
through an ingest rule.

## Conventions

Branch is `feat/rules`. From `services/broker`, before every commit:

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace && cargo deny check && ./scripts/budgets.sh
```

Mutation testing is not optional. `cp` backups, never `git checkout --`.

---

### Task 1: Who owns a singleton

**Files:**
- Create: `crates/rules/src/singleton.rs`
- Test: the same file

The owner is the live member with the lowest node identifier, recomputed on
membership change.

There is one member. Write the rule anyway, over a membership view that today
returns one entry, because the alternative is an assumption that this node is
alone, scattered through two connectors, that milestone five then has to find.

- [ ] **Step 1: Write the failing tests**

One member owns everything. Of three, the lowest owns. When the lowest
leaves, the next lowest takes over. When it returns, ownership moves back.

The last two are the ones that matter and the ones a single-member
implementation cannot express at all. If your membership view cannot be given
three members in a test, that is the design problem to solve first.

- [ ] **Step 2: Implement, run, confirm teeth**

Mutations: the highest identifier wins; ownership not recomputed on a change.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(rules): who owns a singleton, written as a rule"
```

---

### Task 2: The poller

**Files:**
- Create: `crates/rules/src/http_poll.rs`
- Modify: `crates/rules/src/lib.rs`, `crates/storage/src/record.rs`, `crates/sdmqtt/src/config.rs`
- Test: those files

A GET on an interval, publishing the response body to a topic.

The client, the timeout discipline and the refusal of a plain-text URL to a
non-loopback host all come from the outbound connector. Reuse them rather
than writing a second set.

**A poller is not an outbox.** It has no durable queue, because a fetch that
failed will be tried again on the next interval and a stale body is worse
than a missing one. Say that in the module, because the outbound connector
next door has an outbox and a reader will wonder.

**Decide what happens when a fetch takes longer than the interval**, and say
which you chose. Overlapping fetches against a slow endpoint is the same
amplification the outbound connector has a rule about.

- [ ] **Step 1: Write the failing tests**

The body is published on the interval and reaches a subscriber. A fetch that
fails skips that interval and the next one still happens, which is the test
that proves one failure does not stop the poller for good. A fetch slower
than the interval does whatever you decided, and the test says which.

- [ ] **Step 2: Implement, run, confirm teeth**

Mutations: the interval not honoured; a failure stopping the poller; the
publication not carrying its hop count.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(rules): a poller, on the interval it was given"
```

---

### Task 3: The bridge

**Files:**
- Create: `crates/rules/src/bridge/` with modules for the client session, the rewriting and the supervisor
- Modify: `crates/rules/src/lib.rs`, `crates/storage/src/record.rs`, `crates/sdmqtt/src/config.rs`
- Test: those files

The largest piece of this milestone. Split it across several commits; one
commit for a bridge is not reviewable.

The research lists what a bridge needs that the codec does not already
provide, and that list is the order to build in:

1. Connect and reconnect, with exponential backoff and jitter.
2. Composing a CONNECT: identifier, clean start or session expiry,
   keep-alive, credentials, the will that supplies the state topic's offline
   value, and the version 5 properties for receive maximum, maximum packet
   size and topic alias maximum.
3. **Interpreting a CONNACK and honouring what the server declares in it**:
   its keep-alive override, receive maximum, maximum packet size, topic alias
   maximum, retain availability, wildcard availability, and an assigned
   client identifier. Ignoring any of these is a protocol error the remote
   disconnects you for, and it is the part with no equivalent anywhere in
   this codebase, because the broker only ever decodes a CONNECT.
4. Client-side keep-alive: send a ping when idle, and treat a missing
   response inside a deadline as a dead link. The broker's own ring answers
   the opposite question and does not do this.
5. Outbound flow control against the remote's receive maximum, with
   redelivery keeping its original identifier on a resumed session.
6. Inbound exactly-once.
7. An inbound topic alias table, because a compliant server may use aliases
   once you have advertised a maximum.
8. Correlating subscribe acknowledgements, resubscribing when the remote
   reports no session, and surfacing a per-filter failure reason as a rule
   error rather than dropping it.

Items 5 and 6 exist in this codebase in mirror image, typed for the server
side. Read them before writing the client side; do not try to share them.

- [ ] **Step 1: Refuse a looping configuration, before anything connects**

The first task, not the last. A bridge whose inbound and outbound filters
intersect after rewriting is refused. So is one whose state topic matches its
outbound filter.

Test the intersection with filters that overlap only through a wildcard,
because two filters that are not equal can still both match one topic, and a
string comparison would let that through.

- [ ] **Step 2: The rewrite**

A prefix on either side, and for the outbound direction a template capturing
wildcard segments by position, so `sensors/+/temp` becomes
`site-a/temp/{1}`.

Test a template referring to a segment that does not exist, and one on a
filter with a multi-level wildcard, where the capture is several segments
rather than one. Decide what each does.

- [ ] **Step 3 onwards: the session, in the order above**

One commit per numbered item where it makes sense. Say in each what the
remote would do if we got it wrong, because that is the reason each one is in
the list.

- [ ] **Step 4: Test against a real broker**

This broker. Bridge one instance of `sdmqtt` to another, with real sockets,
and move messages in both directions. That is a better test than any mock and
this project happens to have the other end lying around.

Then test against the conformance suite's own broker if that is possible, or
say why not. A bridge that only works against itself has only proved the two
halves agree.

- [ ] **Step 5: Confirm teeth**

Mutations, at least: the remote's receive maximum ignored; the keep-alive
override ignored; a resumed publish given a new identifier; the loop marker
not set; the marker not checked on the way back.

The last two are the pair that matters, and each alone leaves the loop open.

---

### Task 4: Both survive a restart

**Files:**
- Modify: `crates/sdmqtt/tests/rules.rs`

A poller resumes on its interval after a restart. A bridge reconnects, and a
message in flight when the broker was killed is not lost.

Kill, do not stop.

- [ ] **Step 1: Write them, run them, commit**

```bash
git commit -m "test: a poller and a bridge come back"
```
