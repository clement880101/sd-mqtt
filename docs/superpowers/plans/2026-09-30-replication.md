# Metadata Replication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A subscription, a retained message or a rule written on any node
appears on every node, converges after a partition, and survives a restart.
No consensus protocol and no coordination service.

**Architecture:** Each record is a last-writer-wins register stamped with a
hybrid logical clock reading and the writing node's identity. Writes
broadcast immediately. An anti-entropy exchange over a per-node operation log
repairs whatever a broadcast lost. Deletions are tombstones collected behind
a cluster-wide watermark.

**Tech Stack:** The `cluster` crate's link, the `storage` crate, `postcard`.
No new dependencies: the clock is ours, about a hundred and forty lines.

---

## Read first

- `docs/conventions.md`, all eight plus the one on tooling.
- Section 8.2 of `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md` in
  full, including the paragraphs added before this plan. They are not
  background; four of them are requirements that reverse an obvious
  implementation.
- The "Verified again before milestone 5" section of
  `docs/research/component-selection.md`.
- `crates/cluster/src/{identity,link,mesh}.rs`.

## Five things decided before you start

**The order is milliseconds, then counter, then the node's UUID bytes**, and
all three are load-bearing. Milliseconds alone do not converge: two nodes
writing in the same millisecond each keep their own and no exchange repairs
it. Dropping the counter makes one node ambiguous with itself, because two
writes inside one millisecond share a key.

**Convergence is conditional on no two writes sharing a stamp.** A property
test found that, not a person. A clock cannot issue a duplicate, but a
duplicated node identity can and so can a clock that restarts behind itself.
The handshake already refuses the first. The second is yours: a node resumes
its clock from the highest reading it has ever written, read from the store.

**A peer further ahead than the drift bound is refused, not clamped.**
Clamping still advances the local clock, so one node with a wrong year would
poison every node it reached, permanently, and no later write anywhere could
win a conflict against it.

**A tombstone is a value competing by the same comparison, not a special
case.** "A delete always wins" diverges, because two nodes that saw a delete
and a later write in different orders keep different things. One merge
function, and every path through it.

**The anti-entropy digest is a claim about an operation log, not about the
records.** A record's most recent writer changes whenever another node
overwrites it, so "everything after sequence N from node X" cannot be
answered by looking at records. Each node keeps a gapless log of what it
originated, truncated behind its own watermark, which is a second watermark
distinct from the tombstone one.

## Conventions

Branch is `feat/cluster`. From `services/broker`, before every commit:

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace && cargo deny check && ./scripts/budgets.sh
```

Mutation testing is not optional. Convention 7b's three traps have each cost
an agent an hour. Do not run two cargo invocations at once. A first run of a
freshly built test binary on this machine can stall a minute or two in the
dynamic loader; budget forty-five minutes for a cold full suite.

---

### Task 1: The clock

**Files:**
- Create: `crates/cluster/src/clock.rs`
- Modify: `crates/storage/src/record.rs` and both backends
- Test: those files

A reading is milliseconds and a counter. On a local event, take the larger of
the last reading and the wall clock, and increment the counter when the
millisecond did not move. On a remote event, take the largest of the last
reading, the peer's and the wall clock, and derive the counter from whichever
won.

The physical reading comes from the wall clock, which is the first wall-clock
read on a write path in this broker. The existing coarse clock is a monotonic
counter from an arbitrary origin and is deliberately not comparable across
nodes. Take the reading as an injected parameter the way the coarse clock is,
so drift is testable without sleeping.

A counter at its maximum advances the millisecond rather than wrapping or
panicking. Four billion events in one millisecond is unreachable; the branch
costs one comparison and monotonicity is the whole property.

- [ ] **Step 1: Write the failing tests, as properties where you can**

A reading never goes backwards. Two readings from one node are never equal. A
peer ahead within the bound moves the clock; one beyond it is refused and
leaves the clock untouched. A peer far behind changes nothing. The counter at
its maximum advances the millisecond.

Then the property that matters: **a node resuming from the store never
re-issues a stamp it has already used.** Write it as a property over a
restart, because the example you would think of is the one case it works for.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: the counter not incremented inside one millisecond; a far-ahead
peer clamped instead of refused; the clock resuming from zero.

The third must fail a test. If it does not, the resume property is not
reaching it.

```bash
git commit -m "feat(cluster): a clock two nodes can compare"
```

---

### Task 2: The register and the merge

**Files:**
- Create: `crates/cluster/src/register.rs`
- Test: the same file

A value, a stamp, and whether it is a tombstone. One merge function.

- [ ] **Step 1: Write the convergence property first**

Two nodes handed the same writes in any order agree. Generate the writes from
real clocks rather than by constructing stamps, because constructed stamps can
duplicate and the property is conditional on them not doing so. Say in a
comment that the condition exists and what guarantees it.

Then: a tombstone loses to a later write. A later write loses to a tombstone
written after it. Both orders of arrival agree. Three nodes gossiping in a
random order all end up the same.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: a delete always winning; the comparison dropping the counter; the
comparison dropping the node identity. Each must fail the convergence
property, and say which case each breaks.

```bash
git commit -m "feat(cluster): a register that converges, and why it must"
```

---

### Task 3: What is replicated

**Files:**
- Create: `crates/cluster/src/metadata.rs`
- Modify: `crates/storage/src/record.rs`, both backends, `crates/broker-core/`
- Test: those files

Four kinds this milestone: that a node has at least one subscriber for a
filter, retained messages, rules, and session ownership. The kind is an open
vocabulary so that milestone seven's three are a new variant rather than a
new mechanism.

**Subscriptions are replicated at node granularity.** A register per client
subscription would be fifty nodes holding a hundred thousand registers each
at the design ceiling, against a thirty megabyte idle budget. What is
replicated is that a node has a subscriber for a filter; which client it is
nothing outside that node needs.

That means a reference count per filter per node, incremented and decremented
locally, and a register written only when the count leaves or reaches zero.
**Test the edges**: the second subscriber to a filter writes nothing, and the
last unsubscribe writes a tombstone.

- [ ] **Step 1: Write the failing tests**

Each kind round trips through a register and survives a restart. A filter
gaining a second local subscriber broadcasts nothing. A filter losing its
last one broadcasts a tombstone. Rules stop being node-local, which is what
milestone four said would happen.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: a register written on every subscribe; the count never
decremented; a rule still node-local.

```bash
git commit -m "feat(cluster): what is replicated, at the granularity routing needs"
```

---

### Task 4: Broadcast, and the log behind it

**Files:**
- Create: `crates/cluster/src/replicate.rs`, `crates/cluster/src/oplog.rs`
- Test: those files

A write broadcasts immediately to every peer and appends to this node's own
operation log. The log is gapless and durable, because the digest exchange in
task 5 is a claim about it.

- [ ] **Step 1: Write the failing tests**

A write reaches two peers. A write to a node whose link is down still lands
in the log, so the exchange can carry it later, which is the whole reason the
log exists. The log is gapless across a restart. A peer that was down and
comes back is not sent the backlog by broadcast, because that is the
exchange's job and a broadcast that replays is a broadcast that duplicates.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: the log not written when a link is down; a sequence number reused
after a restart, which is the same defect as the clock resuming from zero and
must fail a test.

```bash
git commit -m "feat(cluster): a write is broadcast and written down"
```

---

### Task 5: Anti-entropy

**Files:**
- Modify: `crates/cluster/src/replicate.rs`
- Test: the same file

Every few seconds, with jitter, a node exchanges a digest of per-node
sequence numbers with a random peer and pulls what it is missing, in batches.

- [ ] **Step 1: Write the failing tests**

Two nodes that missed a broadcast converge without one. Three nodes where one
was partitioned converge after it returns. A node that has everything pulls
nothing, which is the test that the digest is a digest rather than a request
for everything.

The batch bound is honoured: a node a million writes behind does not receive
them in one frame.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: the digest ignored and everything always sent; the batch bound not
applied; the jitter removed, and say whether a test can see that.

```bash
git commit -m "feat(cluster): the exchange that repairs what a broadcast lost"
```

---

### Task 6: Tombstones go away

**Files:**
- Modify: `crates/cluster/src/replicate.rs`, `crates/cluster/src/metadata.rs`
- Test: those files

Collected after a grace period long enough to exceed any plausible partition,
tracked by a cluster-wide low watermark.

**A tombstone collected too early resurrects a deleted record**, because a
node that never saw the delete will re-broadcast its old value and nothing
contradicts it. That is the failure to write the test for.

- [ ] **Step 1: Write the failing tests**

A tombstone older than the grace period, with every node past it, is gone. A
tombstone whose grace has passed but which a node has not seen is **kept**.
A record deleted on one node does not come back when a node that missed the
delete rejoins.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: the watermark ignored and the grace period used alone, which the
resurrection test must catch; the watermark taken as the highest rather than
the lowest.

```bash
git commit -m "feat(cluster): tombstones go, once everyone has seen them"
```

---

### Task 7: Three nodes converge after a partition

**Files:**
- Modify: `crates/sdmqtt/tests/cluster.rs`

Against the real binary, through the gate.

1. Three nodes. A retained message published on one appears on all three.
2. The gate severs one node.
3. A retained message is published on each side of the partition, and a rule
   is created on each side.
4. The gate heals.
5. All three agree on everything, and the agreement is the last writer's
   value, not whichever node happened to ask last.

Also: **a node's name defaults to the same word on every node**, so a cluster
of three shows three identically named rows to an operator. Decide whether to
derive a better default, and say which.

- [ ] **Step 1: Write it, run it several times, commit**

Say how many times. A flaky convergence test is worse than none.

```bash
git commit -m "test: three nodes converge after a partition heals"
```
