# Durable Broker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A broker restart preserves durable sessions, their subscriptions and
their queued messages, and an acknowledgement is never sent before the message
it acknowledges is on disk.

**Architecture:** The broker says what happened and the shell writes it. A
journal buffer beside the action buffer, drained under the same lock; the
shell commits it, waits for durability when something is about to be
acknowledged, and only then delivers. Restore rebuilds the broker before any
listener binds.

**Tech Stack:** The `storage` crate built in the previous plan, `broker-core`,
`sdmqtt-server`, `sdmqtt`.

---

## Read first

- `docs/conventions.md`, all eight.
- Section 7 of `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`, in
  particular 7.3 on why the ordering here is a correctness requirement, 7.4 on
  sequence numbers and 7.5 on restore.
- The storage crate: `services/broker/crates/storage/src/`. Its `Record` enum
  is the whole vocabulary this plan has for saying what happened.

## What the previous plan established that changes this one

**Durable acknowledged writes measured at 334 a second with one waiter and
25,100 with a hundred.** The disk, not the broker's lock, is the limit for
acknowledged traffic. A connection that waits is waiting milliseconds, so the
wait must not be held under any lock, and the more connections that wait
together the better each does.

**A clean close of the store persists everything, flushed or not.** So a
restart test that shuts the broker down politely proves nothing about
durability. The end-to-end test in task 8 must kill the process.

**The writer counts batches the store refused and reports them to nobody.**
`Writer::rejected_batches()` exists because the storage crate has no logging
and one call site was not a reason to add it. This plan is that call site.

## Conventions

- **Mutation testing is not optional.** Two mutations survived in the writer
  alone, one of which silently abandoned every outstanding record after a
  failed flush. Use `cp` backups, not `git checkout --`; the working directory
  is shared with a live session.
- Branch is `feat/persistence`.

From `services/broker`, before every commit:

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace && cargo deny check
```

---

### Task 1: `broker-core` may depend on storage without depending on a database

**Files:**
- Modify: `crates/storage/Cargo.toml`, `crates/storage/src/lib.rs`
- Modify: `crates/broker-core/Cargo.toml`

`broker-core` needs the `Record` vocabulary. It must not gain redb: it is the
pure, no-I/O crate, and a database in its dependency tree would be a lie about
what it is, as well as a cost to every test that builds it.

Put the redb backend behind a feature, on by default, and have `broker-core`
depend on `storage` with default features off. The records, the trait and the
in-memory backend stay unconditional.

- [ ] **Step 1: Feature-gate the backend**

- [ ] **Step 2: Prove the tree**

```bash
cargo tree -p broker-core -e normal | grep -c redb
```

Expected: 0. Write this as a test if you can see how; otherwise put the
command in a comment in `broker-core`'s manifest, because a dependency that
creeps back in silently is exactly what this task exists to prevent.

- [ ] **Step 3: Commit**

```bash
git commit -m "build(storage): the backend is a feature, so the pure crate stays pure"
```

---

### Task 2: The journal

**Files:**
- Modify: `crates/broker-core/src/broker/mod.rs` and every module that changes durable state
- Test: a new `crates/broker-core/tests/journal.rs`

This is the task the milestone turns on, and its difficulty is entirely
**exhaustiveness**. A record type that is never emitted is a piece of state
that silently does not survive a restart, and no ordinary test looks for the
absence of a record.

`Broker` gains a journal buffer and `take_journal(&mut Vec<Record>)`, drained
by the shell under the same lock that filled it.

- [ ] **Step 1: Write the exhaustiveness test first, and make it the design**

Not a list of assertions. A differential test, in the same shape as the trie's
test against a naive matcher and the store's test against its in-memory model:

1. Drive a live `Broker` through a generated sequence of client operations:
   connects with and without clean start, subscribes, unsubscribes, publishes
   at all three levels, acknowledgements, disconnects, takeovers, ticks.
2. Collect every journalled record as you go.
3. Apply them all to an in-memory `Storage`.
4. Restore, and rebuild a second `Broker` from what comes back.
5. Assert the two brokers are the same.

Step 5 needs a way to compare two brokers, which does not exist. Add it:
something that reports durable state only, and say in its doc comment that it
reports what must survive, not everything. A comparison that included
connection state would fail for reasons that are correct.

**This test is the specification for the rest of the task.** Anything not
journalled makes it fail, which is precisely the property that a list of
hand-written assertions cannot give you.

- [ ] **Step 2: Watch it fail, and keep watching**

Before any record is emitted it fails immediately. Then emit records one kind
at a time and watch the failure move. The last failure to go is the one you
would have forgotten.

- [ ] **Step 3: Emit records from every path that changes durable state**

The paths, so that none is missed by accident: session creation, takeover and
clean start; expiry and removal; will set and cleared; subscribe and
unsubscribe, including the identifiers a subscription carries; inbound
exactly-once reserve and release; a message entering and leaving a queue; a
message entering and leaving the inflight window; a retained message set and
cleared.

Anything at most once that never touches a queue is journalled nowhere,
because nothing about it must survive.

- [ ] **Step 4: Say what the generator cannot produce**

The storage plan's generator produced none of the four cases it needed on its
first attempt and a coverage test caught it. Write the same coverage test
here: count the interesting cases across a run and fail by name on a zero. At
minimum it must produce a takeover, a clean start over an existing session, a
session that expires, a queue that is popped after a restart-shaped gap, and
two sessions holding one message.

- [ ] **Step 5: Confirm it has teeth**

Delete the emission of each record kind in turn. Every deletion must fail the
differential test. Any that does not is a hole in the generator, not a record
you can skip.

- [ ] **Step 6: Commit**

```bash
git commit -m "feat(core): the broker says what must survive it"
```

---

### Task 3: Sequence numbers

**Files:**
- Modify: `crates/broker-core/src/session/queue.rs`, `outbound.rs`, `state.rs`
- Test: those files, and `crates/broker-core/tests/journal.rs`

Every queue entry gets a per-session number, assigned on enqueue, monotonic
within the session, never reused. Section 7.4.

The store already refuses a reused sequence, so a mistake here surfaces as a
rejected batch rather than as corruption. Do not rely on that: a rejected
batch at runtime is a message lost.

- [ ] **Step 1: Write the failing tests**

Monotonic within a session; independent between sessions; **not reused after a
pop**, which is the one a naive "next = length + 1" gets wrong; and surviving
a restart, so a restored session continues from where it left off rather than
from one.

The last is the one the differential test in task 2 will not catch on its own
if the restored broker and the live broker are both wrong in the same way.

- [ ] **Step 2: Implement, run, and confirm teeth**

Mutations: assign from the queue's length; reset to one on restore; share one
counter across sessions.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(core): every queued message has a number"
```

---

### Task 4: Restore builds a broker

**Files:**
- Create: `crates/broker-core/src/restore.rs`
- Test: the same file

Turn a `Restored` into a `Broker`, using the seams milestone one left for it:
`Inbound::reserve`, `Outbound::restore` and `Queue::requeue`.

Two rules that are easy to miss:

- **A session whose expiry passed while the broker was down is not restored.**
  Section 7.5. So restore takes the current time, and the clock is read before
  restore rather than after.
- **Subscriptions must re-enter the matching trie**, not merely sit on the
  session. A restored session whose filters are on the session but not in the
  trie looks perfectly correct until somebody publishes.

- [ ] **Step 1: Write the failing tests**

A restored session is resumed rather than created, so a connect reports
session present. Its subscriptions receive a publication, which is the trie
half. An expired session is not restored. An inflight entry keeps the
identifier it was sent under.

- [ ] **Step 2: Implement, run, confirm teeth**

Mutations: skip the trie insertion; restore expired sessions; drop the
identifier from a restored inflight entry.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(core): rebuild a broker from what was stored"
```

---

### Task 5: The shell commits before it delivers

**Files:**
- Modify: `crates/server/src/shared.rs`, `crates/server/src/connection.rs`, `crates/server/src/ticker.rs`
- Test: those files

`Shared` gains a writer. Every place that calls `with_broker` and then
`deliver` now drains the journal too, and:

- If the batch must be waited on, await durability, then deliver.
- Otherwise hand it to the writer and deliver at once.

**A batch must be waited on when the actions it accompanies include an
acknowledgement of an at-least-once or exactly-once publication.** That is the
promise; everything else is not one.

**The wait must not be held under the broker lock.** It is milliseconds. The
lock is released when `with_broker` returns and the wait happens after, which
is already the shape of the code, but it is the single easiest thing to get
wrong here and the measurement says what it costs: one waiter alone gets 334 a
second.

The ticker's journal, expiries and wills, is not waited on. Nothing was
promised to anybody.

- [ ] **Step 1: Write the failing test**

The ordering, proved rather than assumed: a publication at least once, with a
store whose flush is held open, and an assertion that the acknowledgement has
**not** reached the client. Then release the flush and assert it does.

A test that only checks the acknowledgement eventually arrives passes against
a broker that sends it first.

- [ ] **Step 2: Write the failure test**

A store whose flush fails. The client must not be acknowledged. Decide what it
gets instead, do that, and write down why: closing the connection is
defensible, and so is a negative acknowledgement carrying an error reason.
What is not defensible is silence or a success.

- [ ] **Step 3: Implement**

- [ ] **Step 4: Report the writer's rejected batches**

`Writer::rejected_batches()` counts batches the store refused with nobody
waiting. Log the count, or surface it beside the other counters. A rejection
means the broker and the store disagree about what exists, which is a defect
somewhere else made visible here.

- [ ] **Step 5: Confirm teeth**

Mutations: deliver before committing; treat every batch as waiting; treat none
as waiting; report success when the flush failed.

- [ ] **Step 6: Commit**

```bash
git commit -m "feat(server): nothing is acknowledged before it is durable"
```

---

### Task 6: Restore runs before the listener binds

**Files:**
- Modify: `crates/sdmqtt/src/main.rs`, `crates/sdmqtt/src/config.rs`
- Test: `crates/sdmqtt/src/config.rs`

Open the store, restore, then bind. In that order, so no client can observe a
half-restored broker.

Configuration: the store's directory, the sync mode, the writer's flush
interval and its record count. Defaults that work with no configuration file
at all, because that is a property the binary already has and must keep.

**Decide and write down what happens with no configured directory.** Running
without a store, as milestone one did, is a legitimate mode and is what every
existing test expects. Refusing to start is also defensible. Pick one, make
the default explicit in the configuration's documentation, and make sure
`sdmqtt serve` with no arguments still does something sensible.

- [ ] **Step 1: Write the failing tests**

The keys parse and reach the store. The defaults are what the documentation
says. A directory that cannot be created is a startup failure naming the path,
not a panic.

- [ ] **Step 2: Implement, confirm teeth, commit**

```bash
git commit -m "feat(sdmqtt): restore, then serve"
```

---

### Task 7: What a store costs the broker

**Files:**
- Modify: `crates/server/tests/throughput.rs`

Milestone one measured throughput with no store. Add a row with one, so the
cost is a number rather than an argument.

At most once, which waits on nothing, and at least once, which waits on
everything. The second is the interesting one, and the storage measurement
predicts it is bounded by concurrent waiters rather than by the broker.

Record it in section 17 beside the existing figures.

- [ ] **Step 1: Measure, record, commit**

```bash
git commit -m "bench(server): what a store costs"
```

---

### Task 8: The exit criterion, proved by killing the process

**Files:**
- Create: `crates/sdmqtt/tests/restart.rs`

Milestone two's exit criterion: a broker restart preserves durable sessions,
their subscriptions and their queued messages.

**Kill the process; do not shut it down.** A clean close of the store persists
everything, flushed or not, so a polite shutdown proves nothing about
durability. Send the signal that does not run destructors.

The test, over a real socket against the real binary:

1. Start the broker with a store in a temporary directory.
2. Connect a durable session, subscribe, disconnect.
3. From a second client, publish to that filter. The subscriber is offline, so
   the message queues.
4. Kill the broker.
5. Start it again on the same directory.
6. Reconnect the first client. It must be told the session is present, and it
   must receive the message published while it was away and the broker was
   dead.

Then the negative, which is what makes the positive mean something: the same
sequence with a clean-start session, which must **not** be present and must
**not** receive anything.

- [ ] **Step 1: Write it, watch it fail against a broker with no store, then pass**

- [ ] **Step 2: Wire it into CI**

It needs a built binary and a temporary directory, like the conformance
runner. Follow that job.

- [ ] **Step 3: Confirm it has teeth**

Kill the broker before the writer's flush interval has elapsed, with the
message published a moment earlier. It must still arrive, because the
acknowledgement to the publisher was the promise. If it does not, the
publisher was lied to and that is the defect this milestone exists to prevent.

- [ ] **Step 4: Commit**

```bash
git commit -m "test: a restart keeps what was promised"
```
