# Session Migration and Partitions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A client reconnecting to a different node takes its session with it,
including what queued while it was away. And a cluster that splits keeps
serving, converges when it heals, and resolves two nodes that accepted the
same client identifier.

**Architecture:** Ownership is a replicated register holding the owning node
and the session epoch. A node a client connects to claims it, asks the
previous owner to close the old connection and drain the queue, and waits a
bounded time before acknowledging.

**Tech Stack:** The `cluster` crate's link, metadata and routing;
`broker-core`'s registry and session machinery.

---

## Read first

- `docs/conventions.md`, all eight plus 7b and 7c.
- Sections 8.4 and 8.5 of
  `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md` in full, including
  the paragraphs added before these plans.
- `crates/cluster/src/{metadata,routes,forward,mesh}.rs` and
  `crates/broker-core/src/{registry,session/state}.rs`.
- The takeover path already in `broker-core`, which does this within one
  node and is the shape to follow across nodes.

## Four things settled before you start

**Equal epochs are the expected outcome, not the rare one.** Two nodes on
opposite sides of a partition both read the same epoch and both increment to
the same number. The tie breaks the way every other register's does, on the
clock reading and then the node's identity, which is why the epoch lives in a
register rather than being compared on its own.

**On a drain timeout, or when the previous owner cannot be reached, the
connection is accepted with what arrived.** The session is reported present
and the shortfall is counted and surfaced. Refusing instead would turn an
unreachable node into an outage for a client that has done nothing wrong, and
the partition that made the owner unreachable is exactly when a client most
needs to connect somewhere.

**"Whole" means an operator-configured expected size**, because nothing else
can mean it: a node that has never heard of a fourth node cannot tell a
cluster of three from a cluster of four with one down. The health endpoint
has served an expected count since milestone 3 with nothing behind it. Unset,
the strict flags are refused rather than silently meaning nothing.

**Messages published on either side of a partition are not retroactively
exchanged.** That is deliberate and section 8.5 says so. Do not try.

## Conventions

Branch is `feat/cluster`. From `services/broker`, before every commit:

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace && cargo deny check && ./scripts/budgets.sh
```

Mutation testing is not optional. Convention 7b has the three traps of
restoring a backup, and 7c is why you check free space before believing a
test is flaky and reclaim the build directory afterwards.

---

### Task 1: Who owns a session

**Files:**
- Modify: `crates/cluster/src/metadata.rs`, `crates/broker-core/src/registry.rs`
- Test: those files

A register per session holding the owning node and the epoch. Written when a
node claims a session, read when a client connects.

The within-one-node takeover already increments an epoch and displaces the
incumbent. This is the same rule with the incumbent on another node.

- [ ] **Step 1: Write the failing tests**

A connect to the node that already owns the session is the existing local
takeover and writes no register it did not already own. A connect to another
node claims it, with a higher epoch. **Two nodes claiming at the same epoch
resolve the same way on both**, which is the case section 8.5 is about and
the one a test of a single claim cannot see.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: the epoch not incremented on a claim; the tie broken on the epoch
alone, which must fail the equal-epoch test; the tie broken on the node name
rather than its identity.

```bash
git commit -m "feat(cluster): a session has one owner and an epoch"
```

---

### Task 2: The drain

**Files:**
- Create: `crates/cluster/src/migrate.rs`
- Modify: `crates/cluster/src/forward.rs`, `crates/server/src/connection.rs`
- Test: those files

The claiming node asks the previous owner to close the old connection and
send what the session holds. It waits a bounded time, then acknowledges
either way.

Subscriptions follow the session, which the replicated metadata already
carries at node granularity, so the new owner's node must gain them and the
old owner's must lose them.

- [ ] **Step 1: Write the failing tests**

A client that subscribed on one node, disconnected, had messages published to
it, and reconnected to another node receives them. That is the whole feature
in one test.

The old connection is closed with "session taken over". The old node stops
being forwarded to for that session's filters and the new one starts.

Then the two that matter more:

- **The previous owner is unreachable.** The connection is accepted, the
  session is reported present, the shortfall is counted. Prove the count,
  not just the acceptance.
- **The drain times out partway.** Same, with what did arrive delivered.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: the acknowledgement sent before the drain; the drain unbounded,
which must fail a test rather than hang it; the shortfall not counted; the
old connection left open, which is two live connections for one session.

```bash
git commit -m "feat(cluster): a session moves, and what it holds moves with it"
```

---

### Task 3: Partitions

**Files:**
- Create: `crates/cluster/src/policy.rs`
- Modify: `crates/sdmqtt/src/config.rs`, `crates/admin/src/health.rs`
- Test: those files

Availability by default: a partitioned node keeps serving. Per-operation
flags to refuse connects, publishes or subscribes while the cluster is not
whole, for operators who prefer that trade.

The expected size supplies what "whole" means, and the health endpoint's
expected count finally has something behind it.

- [ ] **Step 1: Write the failing tests**

By default a node cut off from the others still accepts a connect, a publish
and a subscribe. With each flag set it refuses that one operation and not the
others. Unset expected size with a flag set is refused at startup.

The refusal carries a reason a client can act on, not a closed socket.

- [ ] **Step 2: Implement, confirm teeth, commit**

Mutations: a flag refusing when the cluster is whole; the expected size
ignored; one flag refusing all three operations.

```bash
git commit -m "feat(cluster): serve through a partition, or refuse on purpose"
```

---

### Task 4: Three nodes migrate and converge

**Files:**
- Modify: `crates/sdmqtt/tests/cluster.rs`

Milestone five's exit criterion, against the real binary through the gate.

1. Three nodes deliver across the cluster. (Already proved; keep it.)
2. **A session migrates on reconnect**: subscribe on one node, disconnect,
   have a message published, reconnect to another node, receive it.
3. **Converge after a partition heals**: cut one node off, write on both
   sides, heal, and all three agree on the last writer's value.
4. **Two nodes accept the same client identifier during a partition**, and on
   heal the loser's connection is closed with "session taken over" while the
   winner keeps serving.

The fourth is the one with the most ways to look right. Assert which side
won by the rule, not by which node you happened to ask.

- [ ] **Step 1: Write it, run it several times, commit**

Say how many times, and check free space first.

```bash
git commit -m "test: a session migrates and a cluster converges"
```
