# Queue Replication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Losing a node does not lose the durable messages of the sessions it
owned. A session's queue lives on more than one node, a new owner can pick it
up from whichever copy is furthest ahead, and an operator can see how far
behind the copies are.

**Architecture:** Rendezvous hashing picks a replica set per session. The
owner streams every queue operation to its followers over the existing node
link. Section 8.4's claim still drains from the previous owner; the replica
pull is what happens when that drain comes up short.

**Tech Stack:** The `cluster` crate's link, metadata and migration; the
`storage` crate's records and restore; `broker-core`'s queue.

---

## Read first

- `docs/conventions.md`, all eight plus 7b and 7c.
- Section 8.6 of `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md` in
  full, **including the four paragraphs added before this plan** — they are
  what the tasks below are written against and each one reverses something an
  obvious implementation would do.
- Sections 8.2, 8.4 and 8.5, because this milestone sits on all three: the
  register, the drain, and what a partition is allowed to do.
- `crates/cluster/src/{migrate,metadata,mesh}.rs` and
  `crates/storage/src/{record,restore}.rs`.
- `crates/server/src/shared.rs`, specifically `with_broker_committed` and
  `promises_durability`: synchronous mode is a second wait beside the one
  that is already there, and the first thing to understand is why the
  existing one is where it is.

## Five things settled before you start

**Placement is advisory; the register decides ownership.** Rendezvous says
who *should* own a session and who its followers should be. Section 8.4's
epoch-stamped register says who *does*. Never write code that treats a
rendezvous result as authority over a session — two nodes on opposite sides
of a partition compute different live sets, and that disagreement is
survivable only because placement decides nothing.

**The weight is SHA-256 over a defined byte string, pinned by golden
vectors derived from the standard.** Not foldhash, not `DefaultHasher`, not
anything whose value a crate may change between versions: placement has to
agree across a rolling upgrade. Each node's digest and each session's digest
are computed once and mixed, because hashing every pair at the design
ceilings is five million digests per membership change.

**A synchronous publish with no reachable follower is acknowledged anyway,
and the unconfirmed acknowledgement is counted.** Not refused, unless an
operator sets the strict flag. This is the same trade section 8.5 made.

**Permanent means a configured grace with an operator override.** A node
that is down may be restarting. Nothing repairs on a missed heartbeat.

**The replica pull backs the drain; it does not replace it.** A client
reconnecting elsewhere still claims and still asks the previous owner. The
pull is what happens on the timeout that section 8.4 currently counts a
shortfall for. A plan task that deletes the drain has misread this.

## Conventions

Branch is `feat/replication`, from `main`. From `services/broker`, before
every commit:

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace && cargo deny check && ./scripts/budgets.sh
```

Mutation testing is not optional. Convention 7b has the three traps of
restoring a backup, and 7c is why you check free space before believing a
test is flaky and reclaim the build directory afterwards.

The stored format moves to version 8 in task 2. That needs a "Before you
upgrade" section in `CHANGELOG.md` saying what a fresh store throws away,
written in the same shape as the ones already there.

Every configuration key this milestone adds goes in `README.md` beside the
cluster keys already there, in the task that adds it rather than at the end:
`cluster.replication_factor`, `cluster.gone_after_ms`, the durability mode
and its strict flag. A key that works and is written down nowhere is a key
nobody will ever set.

---

### Task 1: Where a session's copies live

**Files:**
- Create: `crates/cluster/src/placement.rs`
- Modify: `crates/cluster/src/lib.rs`, `crates/sdmqtt/src/config.rs`
- Test: those files

Rendezvous hashing over the live members, returning the owner and R-1
followers in weight order. `cluster.replication_factor`, default 2, refused
outside 1 to 3.

Nothing streams anything yet. This task is a pure function and its
configuration, and it is first because every other task is written in terms
of its answer.

- [x] **Step 1: Write the failing tests**

The weight has golden vectors, and they are **derived from the SHA-256
standard rather than printed from this implementation**: compute the digest
of the defined byte string by hand or with a second tool, and write the
expected weight into the test. A vector captured from the code under test
pins the bug as firmly as the behaviour.

Then the properties, over proptest where they are properties:

- **Determinism.** The same session and the same member set give the same
  set, whatever order the members arrive in. Shuffle them.
- **Minimal movement.** Removing one member moves only the sessions that
  were placed on it. This is the whole reason rendezvous was chosen over
  anything simpler, and it is the property a hash used wrongly still passes
  every single-session test without.
- **Fewer members than R** gives as many as there are, not an error and not
  a set padded with repeats.
- **R of 1** is the owner alone, which is a supported configuration and is
  the one where a follower loop with an off-by-one writes to itself.

And the configuration: zero, four, and a value with the key absent.

- [x] **Step 2: Implement, confirm teeth, commit**

Mutations: the node digest not mixed into the weight, which places every
session on one node and which the determinism test passes; the set ordered by
node identity rather than by weight; R ignored so the set is always every
member; the golden vector recomputed from the implementation.

```bash
git commit -m "feat(cluster): which nodes hold a session's copies"
```

---

### Task 2: The stream, and what a follower keeps

**Files:**
- Create: `crates/cluster/src/replica.rs`
- Modify: `crates/storage/src/record.rs`, `crates/storage/src/restore.rs`,
  `crates/cluster/src/mesh.rs`, `crates/sdmqtt/src/replication.rs`
- Test: those files

The owner sends each queue operation — an append, an acknowledgement or a
removal — to its followers, tagged with the session sequence number. The
follower applies them in order and reports its high-water mark.

**A follower's copy is stored, and must not restore as a session this node
serves.** That is the trap in this task. The records are the same records,
so a restore that does not know the difference brings up another node's
session here, with its subscriptions in this node's trie and its queue being
delivered to nobody. Stored format version 8.

- [x] **Step 1: Write the failing tests**

- Operations arrive and are applied **in order**, and the high-water mark is
  what has been applied rather than what has arrived.
- **A gap is not applied.** An operation whose sequence is not the next one
  is held or refused, and the mark does not move past the hole. A follower
  that skipped would report itself more advanced than it is, which is
  exactly what failover reads to choose between replicas.
- **A restart restores a replica copy as a replica copy.** The clearest
  form: kill the process with the signal, restart, and assert the broker
  serves no session for that client while the replica state is still there.
  A test that only reads the store back has not asked the question.
- Bodies are reference counted on the follower exactly as on the owner: two
  queue entries for one body hold one body, and releasing one keeps it.
- **The lag is a metric**, because section 8.6 says the loss window in
  asynchronous mode *is* the replication lag and that it is exposed and
  alerted on. It is the owner's sequence minus the follower's high-water
  mark, per follower. Assert it from the exposition against a follower held
  deliberately behind — a gauge that only ever reads zero is the shape this
  project has already shipped twice and had to come back for. Task 6 reads
  this series to bound what a kill may lose, so a build without it has no
  exit criterion to meet.

- [x] **Step 2: Implement, confirm teeth, commit**

Mutations: the sequence not checked, so an out-of-order operation applies;
the high-water mark advanced on receipt rather than on apply; a restored
replica registered as a servable session, which must fail the restart test;
the body released on the first removal rather than the last; the lag
reported as zero whatever the marks are.

```bash
git commit -m "feat(cluster): a queue has a copy on another node"
```

Landed as three commits rather than one: the follower's half with the
stored format, the owner's position, and the wiring. The stored format
moved to **10**, not 9 — twice within this milestone, because a number
already written to a disk has to keep meaning one thing.

---

### Task 3: Acknowledged when, and by whom

**Files:**
- Modify: `crates/server/src/shared.rs`, `crates/server/src/connection.rs`,
  `crates/sdmqtt/src/config.rs`, `crates/server/src/metrics.rs`
- Test: those files

`durability = "async"` by default and `"sync"` available, globally and per
listener. Asynchronous acknowledges as soon as the owner has the message
durably. Synchronous waits for one follower to confirm the append.

The same code path in both: one awaits what the other ignores. Read
`with_broker_committed` before writing any of it.

- [ ] **Step 1: Write the failing tests**

- Asynchronous acknowledges **before** any follower has confirmed. Prove it
  with a follower that never confirms, not with a fast one.
- Synchronous does **not** acknowledge until one confirms, and then does.
  The same stalled follower, released.
- **One follower is enough**, with two followers configured and one of them
  silent. A build that waits for all of them passes every single-follower
  test.
- **No reachable follower acknowledges anyway**, and the unconfirmed
  acknowledgement is counted. Both halves asserted; the count is the one
  that makes the behaviour honest rather than silent.
- With the strict flag set, that case is refused with a reason code instead,
  and the flag with nothing to evaluate it against is refused at startup —
  the shape `cluster::policy` already has, so follow it rather than
  inventing a second one.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: synchronous awaiting nothing, which must fail the stalled
follower test rather than hang it; the wait requiring every follower; the
strict flag refusing in asynchronous mode too; the unconfirmed count not
incremented.

```bash
git commit -m "feat(cluster): a publish can wait for a second copy"
```

---

### Task 4: Picking the copy that is furthest ahead

**Files:**
- Modify: `crates/cluster/src/migrate.rs`, `crates/cluster/src/replica.rs`,
  `crates/server/src/shared.rs`
- Test: those files

When the drain times out or the previous owner cannot be reached, the
claiming node asks every reachable member of the replica set for its
high-water mark, pulls from the most advanced, and becomes authoritative. It
then streams to the remaining followers.

**The drain stays.** This is the fallback on the two cases section 8.4
already counts, and the count stays too: what is lost shrinks to what no
replica held.

- [ ] **Step 1: Write the failing tests**

- The owner is unreachable and the queue arrives from a replica anyway. The
  whole feature in one test.
- **The most advanced replica is chosen**, with two replicas at different
  high-water marks and the laggard reachable first. A build that takes the
  first answer passes every single-replica test, and this is the one
  assertion that catches it.
- A stale owner returning from a partition **discards its copy** rather than
  serving it, which is section 8.5's resolution reaching the queue as well
  as the connection.
- The shortfall is still counted when no replica held the rest, and is
  **zero** when one did. Both, because a counter that never moves and a
  counter that always moves look the same from one test.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: the first reachable replica used rather than the most advanced;
the pull replacing the drain rather than backing it, which must fail a test
asserting the previous owner is asked first; the stale owner keeping its
copy; the shortfall counted even when the pull filled it.

```bash
git commit -m "feat(cluster): a new owner takes the furthest copy"
```

---

### Task 5: Repairing a set that is short

**Files:**
- Create: `crates/cluster/src/repair.rs`
- Modify: `crates/cluster/src/membership.rs`, `crates/admin/src/resources.rs`,
  `crates/sdmqtt/src/config.rs`
- Test: those files

A member down longer than `cluster.gone_after_ms` is treated as gone and the
replica sets that included it are repaired, rate limited. `DELETE
/api/v1/nodes/{id}` removes one at once.

**This is the first thing in the design that takes a node out of
membership.** Until now the table could only grow. Removing a node that is
still alive has to do something defined — it will be met again on the next
heartbeat — so decide it, write it down, and test it.

- [ ] **Step 1: Write the failing tests**

- Down past the grace: the sets that included it are repaired onto the
  remaining members, and the new follower ends up holding the same queue.
- **Within the grace: nothing is copied.** The test that matters, because a
  build that repairs immediately passes the one above and copies the whole
  estate every time an operator restarts a node.
- Removal starts it at once, without waiting for the grace.
- The rate limit bounds what is in flight. Assert the bound, not that
  copying happened.
- A set that did not include the departed member is **not** touched.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: the grace ignored; the rate limit ignored; every set repaired
rather than the ones that are short; a removed node that comes back not
re-admitted.

```bash
git commit -m "feat(cluster): a short replica set repairs itself"
```

---

### Task 6: Three nodes, a kill, and what the operator sees

**Files:**
- Modify: `crates/sdmqtt/tests/cluster.rs`, `services/web/src/pages/cluster.tsx`,
  `services/web/src/api.ts`

Milestone six's exit criteria, against the real binary, plus the two things
section 11.2 asks the Cluster page to show.

- [ ] **Step 1: Write it, run it several times, commit**

Say how many times, and check free space first.

- **Synchronous mode loses nothing.** A session with a non-empty offline
  queue, its owner killed with the signal rather than asked politely, the
  client reconnecting elsewhere, and every message still there.
- **Asynchronous mode's loss is bounded by the lag metric.** Not "some
  messages arrive": read the lag, kill the owner, and assert what is missing
  is within what the metric said. A test that asserts nothing about the
  number is a test that would pass against no replication at all.
- The page shows replica health per session set and re-replication progress
  while it runs. Check it against real brokers rather than assuming it
  renders, the way the Cluster page itself was checked.

```bash
git commit -m "test: a killed owner loses nothing, and the page says so"
```
