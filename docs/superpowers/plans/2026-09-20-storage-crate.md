# Storage Crate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A durable store for sessions, subscriptions, queues, message bodies
and retained messages, complete and tested on its own, before anything in the
broker knows it exists.

**Architecture:** One crate, `storage`. A `Record` is something that happened
and must survive. A `Storage` implementation applies batches of records
atomically and can rebuild everything from what it has applied. Two
implementations: redb on disk, and a map in memory that the tests use. A
batching writer on its own thread turns many small batches into few disk
flushes.

**Tech Stack:** Rust, `redb` 4.3, `postcard` for the on-disk encoding,
`thiserror`, `proptest`.

---

## Read first

- `docs/conventions.md`, all eight.
- Section 7 of `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`, which is
  the design this implements, including the four sections added today on how
  persistence reaches the broker.
- The storage entry in `docs/research/component-selection.md`, which explains
  why the backend changed from fjall to redb and what was measured.

## Three things that decide the shape

**The on-disk encoding is a wire format.** Convention 1 says the codec's
golden vectors are derived from the specification rather than captured from
the encoder, because a round trip cannot see a consistent error. The same
applies here with a sharper edge: a round trip in one process cannot see an
encoding that changed between releases, and the consequence is a broker that
starts up and silently misreads every session it stored last week. Every
record type gets a golden byte vector, written by hand, and a version marker
that a future change must bump.

**The head read is expressed as a cursor, not as "the first key".** redb has
a native remove-and-return-head and does not need this. It is written this
way anyway because the engine that was originally chosen does need it, and
because the measurement that found that out is the reason the trait is worth
having. No backend type appears in a signature.

**Depth is a stored counter, not a question for the engine.** At least one
engine's length includes entries that have been deleted. Keep the number.

## Conventions that apply to every task

- **Mutation testing is not optional.** Name at least two mutations per task,
  make each by hand, confirm a test fails. Use `cp` backups rather than
  `git checkout --`; the working directory is shared with a live session.
- **A test that cannot fail is the failure mode this project actually has.**
  Twenty-two defects were found after milestone one's feature work was
  finished, and most were guards that could not fail. Ask of every test what
  would have to be true for it to fail.
- Branch is `feat/persistence`. Stay on it.

From `services/broker`, before every commit:

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace && cargo deny check
```

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/storage/Cargo.toml` | Dependencies, and nothing that is not needed |
| `crates/storage/src/lib.rs` | The `Storage` trait, `SyncMode`, `StorageError` |
| `crates/storage/src/record.rs` | `Record` and the record structs, with their encoding |
| `crates/storage/src/restore.rs` | `Restored`, what a rebuild needs |
| `crates/storage/src/memory.rs` | The in-memory backend |
| `crates/storage/src/redb.rs` | The redb backend |
| `crates/storage/src/writer.rs` | The batching writer and its thread |
| `crates/storage/tests/backends.rs` | One suite, run against both backends |
| `crates/storage/tests/restart.rs` | A property test over apply, reopen, compare |

---

### Task 1: The crate, and nothing in it

**Files:**
- Create: `crates/storage/Cargo.toml`, `crates/storage/src/lib.rs`
- Modify: `services/broker/Cargo.toml` (workspace members)

- [ ] **Step 1: Create the crate with its dependencies**

Runtime dependencies, and justify any you add beyond these in the commit
message: `redb = "4.3"`, `postcard` with `alloc`, `serde` with `derive`,
`thiserror`, `bytes`.

`postcard` and `serde` are already used elsewhere in the workspace; match the
versions the workspace already resolves rather than introducing a second.

- [ ] **Step 2: Confirm the licence gate is unmoved**

```bash
cargo deny check
```

Expected: all four ok, and `deny.toml` **unchanged**. redb and `libc` are
both dual MIT or Apache and already allowed. If anything is rejected, stop
and report it rather than editing the allow list: a dependency that needs a
policy change is a decision, not a step.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(storage): the crate, and its one dependency"
```

---

### Task 2: What a record is, and what it looks like on disk

**Files:**
- Create: `crates/storage/src/record.rs`
- Test: the same file, plus golden vectors

A `Record` is one durable fact. The set, which is the whole vocabulary the
broker will have for saying what happened:

```rust
pub enum Record {
    /// A session exists, or its durable fields changed.
    SessionPut(SessionRecord),
    /// A session is gone. Takes its subscriptions, queue and inflight with it.
    SessionRemoved { session: SessionKey },
    SubscriptionAdded { session: SessionKey, subscription: SubscriptionRecord },
    SubscriptionRemoved { session: SessionKey, filter: String },
    /// An exactly-once identifier the peer owns until it releases it.
    InboundReserved { session: SessionKey, packet_id: u16 },
    InboundReleased { session: SessionKey, packet_id: u16 },
    /// A message sent and awaiting acknowledgement, with the identifier it
    /// was sent under. Section 6: a resumed publish keeps that identifier.
    InflightPut { session: SessionKey, entry: InflightRecord },
    InflightRemoved { session: SessionKey, packet_id: u16 },
    /// A message joined a session's queue at this sequence number.
    QueuePushed { session: SessionKey, entry: QueueEntry },
    /// A message left it. Sequence numbers are not reused.
    QueuePopped { session: SessionKey, sequence: u64 },
    /// A body, stored once however many sessions reference it.
    BodyPut { id: MessageId, body: BodyRecord },
    /// One reference released. The body goes when the last one does.
    BodyReleased { id: MessageId },
    RetainedPut { topic: String, message: RetainedRecord },
    RetainedRemoved { topic: String },
}
```

`SessionKey` is the mountpoint and client identifier, which is what section
5.2 says identifies a session. Do not key on the runtime `SubscriberId`: it
is an index into a live registry and means nothing after a restart.

- [ ] **Step 1: Write the golden vectors first**

For each record type, a test asserting its exact bytes. Write the bytes by
hand from postcard's format rules, not by printing what the encoder produced.
A vector captured from the encoder agrees with the encoder by construction
and cannot see the encoder change.

If deriving a vector by hand for a large struct is impractical, say so and
write it for the field layout instead: the discriminant, the length prefixes
and the first field. A partial vector that is genuinely independent is worth
more than a complete one that is a photograph of the output.

- [ ] **Step 2: Write a format version, and a test that it is checked**

A single byte or short integer written once per database, not per record. A
reader that meets a version it does not know must refuse to start, loudly,
rather than misread. The test: a store written with a version marker one
higher than the current fails to open with an error naming the version.

This is the single most valuable test in this task. Everything else fails
noisily; this one fails silently and corrupts.

- [ ] **Step 3: Round trips, as a property**

`proptest` over every record type: encode, decode, compare. The round trip is
not the guard, the golden vectors are; this catches the cases the vectors do
not enumerate.

- [ ] **Step 4: Confirm the tests have teeth**

1. Swap two fields of the same type in a record struct. A golden vector must
   fail. If only the round trip runs, nothing will.
2. Change the format version constant without changing the vectors. Say what
   fails and whether that is what you want.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(storage): what a record is, and its bytes"
```

---

### Task 3: The trait

**Files:**
- Modify: `crates/storage/src/lib.rs`
- Create: `crates/storage/src/restore.rs`

```rust
pub trait Storage: Send + Sync + 'static {
    /// Apply a batch. Atomic: all of it or none of it. Not durable until
    /// `flush` returns, so a caller that promised something must flush.
    fn apply(&self, records: &[Record]) -> Result<(), StorageError>;

    /// Make everything applied so far durable.
    fn flush(&self) -> Result<(), StorageError>;

    /// Everything needed to rebuild the broker, read once at startup.
    fn restore(&self) -> Result<Restored, StorageError>;

    /// The head of a session's queue, at or after `cursor`.
    ///
    /// Expressed as a cursor rather than as "the first entry" because an
    /// engine without a head primitive answers the first form with a scan
    /// over everything already deleted. Measured at 197 reads a second
    /// against 1.47 million; see the storage entry in the component
    /// research.
    fn queue_head(&self, session: &SessionKey, cursor: u64)
        -> Result<Option<QueueEntry>, StorageError>;

    /// How many entries a session's queue holds.
    ///
    /// A stored counter, not the engine's own length: at least one engine
    /// counts entries that have been deleted.
    fn queue_depth(&self, session: &SessionKey) -> Result<u64, StorageError>;

    /// The highest sequence number ever assigned to this session, whether
    /// or not that entry is still held. Nothing may reuse it.
    fn highest_sequence(&self, session: &SessionKey) -> Result<u64, StorageError>;
}
```

`Restored` carries sessions, each with its subscriptions, inbound
identifiers, inflight entries and queue entries in sequence order, plus the
retained messages and the bodies the queue entries reference.

**Decide and write down** whether `Restored` is eager or an iterator. Eager
is simpler and the bound is the configured queue size times the session
count, which can be large. An iterator complicates the caller and bounds
memory. Pick one, say why in the doc comment, and note what would change
your mind.

- [ ] **Step 1: Write the trait and the error type**

`StorageError` distinguishes at least: the format version is not ours, the
store is corrupt, the store is locked by another process, and an underlying
I/O failure. A single opaque error would make the version check useless.

- [ ] **Step 2: Commit**

```bash
git commit -m "feat(storage): the trait, and what restoring returns"
```

---

### Task 4: The in-memory backend, and one suite for both

**Files:**
- Create: `crates/storage/src/memory.rs`, `crates/storage/tests/backends.rs`

The in-memory backend is what keeps the trait honest in place of a second
real engine. It is also what every other crate's tests will use, so it must
be correct rather than approximate.

- [ ] **Step 1: Write the shared suite first, against nothing**

A module of test functions generic over `Storage`, each taking a constructor.
Write them before either backend exists, so that the suite is written against
the trait's contract rather than against an implementation's behaviour.

At minimum: applying and restoring each record type; a batch is all or
nothing; a removed session takes its subscriptions, queue, inflight and
inbound state with it; sequence numbers do not reuse after a pop; depth
tracks pushes and pops; the head read at a cursor skips what was popped; a
body survives until its last reference is released and not one release
longer.

- [ ] **Step 2: Implement the in-memory backend until the suite passes**

- [ ] **Step 3: Confirm the suite has teeth**

Break the backend in three ways and confirm the suite catches each: a pop
that does not decrement depth; a body released once too often that is deleted
while still referenced; a removed session that leaves its queue behind.

If any of the three passes, the suite is the problem, not the backend.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(storage): a backend in memory, and the suite both must pass"
```

---

### Task 5: The redb backend

**Files:**
- Create: `crates/storage/src/redb.rs`
- Test: `crates/storage/tests/backends.rs`, now run twice

Tables, one set for the whole database rather than one per session:

| Table | Key | Value |
|---|---|---|
| `meta` | a short name | the format version, and anything else global |
| `sessions` | session key | the session record |
| `subscriptions` | session key, filter | the subscription record |
| `inbound` | session key, packet identifier | nothing |
| `inflight` | session key, packet identifier | the inflight record |
| `queues` | session key, sequence, big-endian | the queue entry |
| `queue_state` | session key | depth and highest sequence |
| `bodies` | message identifier | the body and its reference count |
| `retained` | topic | the retained record |

The sequence must be **big-endian** in the key so that byte order is numeric
order. A little-endian key sorts 256 before 2, and the queue comes back in
the wrong order after a restart, which no in-memory test would catch.

- [ ] **Step 1: Write the ordering test first**

Push entries with sequence numbers that differ in their high bytes, restore,
and assert the order. Include numbers that cross a byte boundary, such as
255, 256 and 257.

This test exists because it is the one mistake in this task that is invisible
to every other test and to the in-memory backend.

- [ ] **Step 2: Open, create and version**

`Database::create` on a path. On an existing database, read the format
version from `meta` and refuse a version that is not ours. On a new one,
write it. Report a lock held by another process as its own error: two brokers
on one directory is an operator mistake worth naming.

- [ ] **Step 3: Apply, in one write transaction**

One `begin_write` per batch, every record applied into it, one `commit`. That
is what makes a batch atomic.

Set durability to `Durability::None` on this commit. Durability is the
writer's job in task 7, and committing every batch immediately would make the
batching writer pointless.

- [ ] **Step 4: Flush**

A commit with `Durability::Immediate` is what makes everything before it
durable. An empty transaction committed immediately is the cheapest way to
say "make what I already wrote safe"; confirm that is true of redb rather
than assuming it, and say what you found.

- [ ] **Step 5: Restore, the head read, depth and the highest sequence**

Depth and highest sequence come from `queue_state`, maintained as records are
applied. The head read is a range from the cursor.

- [ ] **Step 6: Run the shared suite against it**

- [ ] **Step 7: Confirm it has teeth**

1. Write the sequence key little-endian. The ordering test must fail.
2. Commit with `Durability::Immediate` in `apply`. Nothing should fail, which
   tells you correctness does not depend on it; note that, because task 7's
   tests are what must catch the difference.
3. Skip the version check on open. The version test from task 2 must fail.

- [ ] **Step 8: Commit**

```bash
git commit -m "feat(storage): the redb backend"
```

---

### Task 6: Bodies are stored once

**Files:**
- Modify: `crates/storage/src/memory.rs`, `crates/storage/src/redb.rs`
- Test: `crates/storage/tests/backends.rs`

A message matching a thousand subscriptions is one body and a thousand queue
entries. The count is stored beside the body, because section 7.2 says it is
not rebuilt by scanning at startup, and a scan of every queue at startup is
exactly what a broker with a large store cannot afford.

Two failures to guard, in opposite directions. A body deleted while a queue
entry still names it is a message lost and, worse, a restore that cannot
complete. A body kept after its last reference is a leak that nothing ever
reclaims.

- [ ] **Step 1: Write both tests**

Two sessions queue the same message; one pops; the body is still readable.
The other pops; the body is gone.

- [ ] **Step 2: Implement in both backends**

- [ ] **Step 3: The count survives a restart**

Reopen between the two pops. The count must come back, not restart at one.
This is what "persisted alongside the bodies they protect" means, and without
this test the count could live only in memory and both tests above would
still pass.

- [ ] **Step 4: Confirm it has teeth**

1. Delete the body on the first release rather than the last. Test one fails.
2. Never delete. Test two fails.
3. Keep the count in memory rather than storing it. Only the restart test
   fails, which is why it exists.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(storage): a body is stored once and counted"
```

---

### Task 7: The batching writer

**Files:**
- Create: `crates/storage/src/writer.rs`
- Test: the same file

Measured, an uncoalesced durable write is roughly 360 a second on this
machine. Coalesced a thousand at a time it is over 300,000. The writer is
not an optimisation; without it the broker's acknowledged-delivery rate is
360 messages a second.

**It owns a dedicated operating system thread.** Not a tokio task and not
`spawn_blocking`. A durable commit blocks for milliseconds, and the engine
originally considered blocks for far longer; neither belongs on a runtime
worker, and the blocking pool is a shared resource that a stalled store
would exhaust.

The shape: a channel carries `(Vec<Record>, Option<oneshot::Sender<Result>>)`.
The thread applies each batch as it arrives. It flushes when the first of
these happens:

- a configurable interval, in the low milliseconds, has passed since the last
  flush;
- a configurable number of records have been applied since the last flush;
- a batch arrived with a waiter attached, meaning something is about to be
  acknowledged and must not be lost.

Every waiter queued since the last flush is woken when that flush returns,
with the same result. A waiter whose flush failed must learn that it failed:
telling a publisher its message is safe when the disk refused is the failure
this whole milestone exists to prevent.

- [ ] **Step 1: Write the failing tests**

- A batch with a waiter is durable when the waiter returns. Prove it by
  reopening the store, not by asking the writer.
- Batches without waiters are not flushed one by one. Count the flushes; a
  counter on a fake `Storage` is the way.
- Many waiters arriving together are woken by one flush. Count the flushes
  again.
- A flush that fails reaches every waiter it should have covered.
- The interval flushes a batch that nobody waited for.

The fourth is the one that will be missing if you write these quickly, and it
is the one that matters. A writer that swallows an error is worse than no
writer.

- [ ] **Step 2: Implement**

- [ ] **Step 3: Shutdown**

Closing the channel must flush what is outstanding and then stop, and the
handle's drop must wait for the thread. A writer that exits with records
applied but not flushed loses exactly the data this crate exists to keep.

Test it: apply, drop the handle, reopen, everything is there.

- [ ] **Step 4: Confirm the tests have teeth**

1. Flush on every batch. The coalescing tests must fail.
2. Never flush on a waiter, only on the interval. The first test must fail.
3. Report success to waiters whatever the flush returned. The error test must
   fail.
4. Drop the thread without a final flush. The shutdown test must fail.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(storage): the writer that makes durability affordable"
```

---

### Task 8: A restart is indistinguishable from not having stopped

**Files:**
- Create: `crates/storage/tests/restart.rs`

The property the milestone exists for, tested as a property rather than as
examples.

Generate a sequence of records that a broker could plausibly produce: a
session appears, gains subscriptions, queues messages, pops some, reserves and
releases identifiers, and sometimes vanishes. Apply them to the in-memory
backend and to redb. Flush, reopen redb, restore both, and compare.

The in-memory backend is the model. That is the same shape as the codec's
differential test against a naive matcher, and it works for the same reason:
two implementations of one contract disagree loudest where the contract is
least clear.

- [ ] **Step 1: Write the generator**

Say in a comment what it can and cannot produce. Convention 5's "a generator
that cannot produce a case" is how a property test quietly becomes decorative.
At minimum it must be able to produce: a pop of a queue that is already
empty, a release of a body that has one reference left, a session removed
while it still holds a queue, and two sessions sharing a body.

- [ ] **Step 2: Write the comparison**

Compare the restored state, not the internals. Two stores that agree on every
session, subscription, queue entry in order, inbound identifier, inflight
entry, body and retained message are the same store.

- [ ] **Step 3: Run it, and record how long it takes**

If it is slow, reduce the case count rather than the case size. A hundred
long sequences find more than a thousand short ones.

- [ ] **Step 4: Confirm it has teeth**

Introduce each of these into the redb backend and confirm the property fails:
a queue entry that restores out of order, a dropped subscription, an inbound
identifier that does not survive, and a depth that drifts by one after a
removed session.

- [ ] **Step 5: Commit**

```bash
git commit -m "test(storage): a restart is indistinguishable from not having stopped"
```

---

### Task 9: What it costs

**Files:**
- Create: `crates/storage/tests/throughput.rs`

Two numbers the design needs and does not have, as an ignored test that
prints a table, in the same shape as the listener's throughput harness.
Read `crates/server/tests/throughput.rs` first and follow it.

- **Durable acknowledged writes a second**, with the writer, at one, ten and
  a hundred concurrent waiters. This is the ceiling on acknowledged delivery
  and nothing has measured it.
- **What a large payload costs.** Section 17 records that redb stores values
  in the tree, so a multi-megabyte body is rewritten as pages split. Store
  bodies at 1 KiB, 64 KiB, 1 MiB and the configured maximum, and report
  bytes written to disk per byte of payload. Take the file size before and
  after; it does not need to be precise to be decisive.

Record both in section 17 of the design, with the machine, replacing the
open question rather than adding to it. If the large-payload figure is bad,
say so and stop: the answer is a change of design, and it is not yours to
make here.

- [ ] **Step 1: Write it, run it, record the figures, commit**

```bash
git commit -m "bench(storage): what durability and large payloads cost"
```
