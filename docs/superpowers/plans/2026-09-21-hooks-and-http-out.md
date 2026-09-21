# Hooks and the Outbound Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A message published to MQTT reaches an HTTPS endpoint, and survives
a broker restart mid-flight. That is half of milestone four's exit criterion
and the thing this project was asked for first.

**Architecture:** Connectors sit on a hook interface rather than being
special-cased in the publish path, because that is the shape an
out-of-process plugin transport would need later. A rule is durable
node-local metadata. Each rule owns a durable outbox in the store, so the
delivery that was accepted is the delivery that eventually happens.

**Tech Stack:** `hyper-util` client, `hyper-rustls`, `rustls`, `aws-lc-rs`,
`base64`, the storage crate from milestone two.

---

## Read first

- `docs/conventions.md`, all eight.
- Sections 9.1, 9.2 and 9.3 of
  `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`, including the four
  paragraphs added before this plan.
- The section "Verified again before milestone 4" in
  `docs/research/component-selection.md`. Several of its findings are
  requirements here, not background.

## What the verification settled, so do not re-decide it

The client is `hyper-util` plus `hyper-rustls` over `rustls` and `aws-lc-rs`.
It costs 2.75 MB, taking the binary from a quarter of its budget to nearly
half. That is recorded and accepted. `base64` goes in with default features
off, because its SIMD engine is `unsafe` and every crate here forbids that.

## Four traps this plan exists to avoid

**The notices generator is wrong in a way this dependency triggers.** It
reads any licence expression containing the word "or" as offering a choice,
and the new cryptography dependency requires four licences at once. Fixing
that is task 1 and comes before the dependency.

**A queue's pop does not release its body.** The storage crate documents this
and it is a real trap: an outbox that pops a delivered envelope without
releasing its payload leaks that payload forever and nothing reclaims it.

**Three settings on the client are bugs if omitted.** Without a pool timer
the idle timeout never fires and connections are held forever. Without a
connect timeout a black-holed host is bounded only by the request deadline,
and holds a blocking thread meanwhile. And the client retries a request it
thinks was cancelled, by default, which is a retry outside the rule's budget.

**Retrying is how you take down an endpoint that is already struggling.**
Attempt counts live in the stored record, not in memory, or a restart resets
every envelope to zero attempts and throws the whole backlog at a dead
endpoint at once.

## Conventions

Branch is `feat/rules`. From `services/broker`, before every commit:

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace && cargo deny check && ./scripts/budgets.sh
```

Mutation testing is not optional. `cp` backups, never `git checkout --`; the
working directory is shared with a live session.

---

### Task 1: The notices generator reads a licence expression correctly

**Files:**
- Modify: `scripts/rust-notices.py`
- Test: whatever that script's tests are, or add some

Before the dependency, not after.

The generator decides which licence a package ships under by searching the
expression for the word "or". An expression like
`ISC AND (Apache-2.0 OR ISC) AND Apache-2.0 AND MIT AND BSD-3-Clause`
requires every one of those at once, and contains the word "or" inside
parentheses, so it is read as a choice and resolves to one of them.

Parse it properly: a choice only counts at the top level, outside
parentheses. A conjunction means every branch applies and every branch's
obligations apply with it.

- [ ] **Step 1: Write the failing test**

The real expression from the dependency you are about to add, and a plain
choice, and a plain single licence. Assert what each resolves to. The
conjunction must resolve to all of its parts.

- [ ] **Step 2: Fix, run, and confirm teeth**

Mutations: treat a top-level choice as a conjunction; ignore parentheses
again. Each must fail a test.

- [ ] **Step 3: Regenerate the notices and commit**

The file should not change yet, because the dependency is not there. Confirm
that, and say so in the commit message.

```bash
git commit -m "fix(notices): a licence expression is parsed, not searched"
```

---

### Task 2: The hook interface

**Files:**
- Create: `crates/broker-core/src/hooks.rs`
- Modify: `crates/broker-core/src/broker/mod.rs` and the paths that fire hooks
- Test: `crates/broker-core/src/hooks.rs`, `crates/broker-core/tests/`

Section 9.3. Nine hook points: authenticate, authorize publish, authorize
subscribe, message received, message delivered, message dropped, client
connected, client disconnected, session expired.

An authorizing hook returns allow, deny or next. If every hook returns next,
the configured default applies. Message hooks may rewrite topic, payload,
quality of service, retain and properties, or drop the message.

**This must not make `broker-core` do I/O.** A hook is a synchronous call on
a trait object. Anything that needs to reach the network hands the work to
something else and returns; the outbound connector's hook writes to an outbox
and returns, and a separate task does the posting. Say this in the module
documentation, because the obvious implementation of an HTTP connector is an
`async fn` and that is the thing this design refuses.

**A hook must not be able to stall the broker.** It runs under the one lock.
Say what a hook may not do, and consider whether anything can enforce it.

- [ ] **Step 1: Write the failing tests**

For the authorizing chain: a hook that denies stops the message; one that
allows stops the chain; a chain of nexts falls through to the default; the
default applies both ways. Four tests, because a chain with one behaviour
tested passes against a chain that always does that.

For the rewriting hooks: a hook that changes the topic changes where the
message goes, which is the test that proves the rewrite happens before
matching rather than after. A hook that drops the message means no subscriber
receives it and the drop is counted.

For the observing hooks: each of the nine fires when it should. A test per
hook point, because a hook that never fires is a connector that never runs
and nothing else would notice.

- [ ] **Step 2: Implement**

- [ ] **Step 3: Confirm the tests have teeth**

Mutations: return allow where the chain said next; fire the received hook
after matching rather than before; skip the dropped hook. Each must fail.

Then the exhaustiveness question, which is the one that matters: **delete a
hook firing site and see whether anything fails.** If a hook point has no
test that reaches it through the broker, say which.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(core): the hook interface connectors sit on"
```

---

### Task 3: A rule, stored

**Files:**
- Create: `crates/rules/Cargo.toml`, `crates/rules/src/lib.rs`, `crates/rules/src/rule.rs`
- Modify: `crates/storage/src/record.rs` and both backends
- Test: those files

A rule has an identifier, a name, an enabled flag, a trigger and one action.
The trigger is a topic filter, optionally narrowed by a minimum quality of
service and a client identifier pattern.

Storing it extends the record vocabulary, which moves the stored format
version. The version is refused rather than migrated when it does not match,
so a broker built after this will not open a store written before it. That
is accepted and belongs in the commit message.

**The golden vectors rule applies.** The on-disk encoding is a wire format
and the storage crate derives its vectors by hand from the encoding's rules
rather than capturing them from the encoder. Do the same for every record you
add. A captured vector agrees with the encoder by construction.

- [ ] **Step 1: Write the golden vectors and the version test first**

- [ ] **Step 2: Add the records, the rule type, and the store round trip**

The contract suite in the storage crate runs against both backends. A new
record must pass it on both, and the restart property must cover it.

- [ ] **Step 3: Rules survive a restart**

The property this whole crate exists for. Extend the storage crate's restart
property rather than writing a separate test, so a rule is compared the same
way everything else is.

- [ ] **Step 4: Confirm teeth, commit**

Mutations: a rule's enabled flag not stored; the trigger's minimum quality of
service dropped; the format version not moved.

```bash
git commit -m "feat(rules): a rule, stored and restored"
```

---

### Task 4: The durable outbox

**Files:**
- Modify: `crates/storage/src/record.rs`, both backends, the contract suite
- Create: `crates/rules/src/outbox.rs`
- Test: those files

Each rule owns an outbox. An envelope enters it when the rule fires and
leaves when it is delivered or dead-lettered. **The attempt count and the
next attempt's time live in the stored record**, so a restart does not reset
them.

The operations: append, read the head, mark an attempt, delete the head, and
count what is waiting. Head reads are a cursor, as the storage trait already
does, and depth is a stored counter, for the reasons that trait records.

**Releasing the payload is yours.** The storage crate's own documentation
says a pop does not release a body, and an outbox that forgets leaks every
payload it ever sent.

- [ ] **Step 1: Write the failing tests**

An envelope survives a restart with its attempt count. Its payload is
reclaimed when it leaves, and not before. The head read skips what has been
delivered. Two rules' outboxes do not see each other.

- [ ] **Step 2: Implement, run, and confirm teeth**

Mutations: attempts kept in memory; the payload released on append rather
than on completion; the payload never released; one outbox reading another's
head.

The second and third are the leak in both directions and both must fail a
test.

- [ ] **Step 3: Commit**

```bash
git commit -m "feat(rules): a durable outbox per rule"
```

---

### Task 5: Posting the envelope

**Files:**
- Create: `crates/rules/src/http_out.rs`, `crates/rules/src/envelope.rs`
- Modify: `crates/rules/Cargo.toml`
- Test: those files

The envelope carries the topic, the payload as text when it is valid and
base64 otherwise, the quality of service, the retain flag, the publishing
client's identifier, a timestamp, and any version 5 user properties.

Configurable per rule: method, headers, timeout, maximum concurrency, retry
count and backoff.

- [ ] **Step 1: Add the dependencies and confirm the gates**

The client, the TLS stack and base64. Then `cargo deny check` with the allow
list unchanged, and the budget script. Report the binary size; the
verification predicts about 6.5 MB and a figure far from that is worth
understanding before going on.

- [ ] **Step 2: Build the client in one function**

With the pool timer, the connect timeout, and the cancelled-request retry
turned off. Each of those is a bug if omitted and the reasons are in the
research notes. Bound the response body; an endpoint answering forever must
not be able to grow this task's memory.

Refuse a plain-text URL. The design says HTTPS.

- [ ] **Step 3: Write the envelope, with golden bytes**

A payload that is valid text, and one that is not. Assert the exact JSON for
each. A test that parses its own output back cannot see a field renamed.

- [ ] **Step 4: Write the failing delivery tests against a real server**

Stand up a local HTTP server in the test and assert the envelope arrives.
Then: a server that fails twice and succeeds is retried and delivered once;
a server that always fails ends in the dead-letter topic after the configured
count; a server that never answers is bounded by the timeout rather than the
retry count.

The third is the one usually missing, and a hung endpoint is the most common
real failure.

- [ ] **Step 5: Do not amplify a failing endpoint**

Back off on the rule, not on each envelope. A per-envelope backoff against a
shared endpoint is a thundering herd with extra steps. Honour a retry-after
header where the endpoint sends one.

Test it: an endpoint that fails receives a bounded number of attempts in a
window, not one per queued envelope.

- [ ] **Step 6: Say that delivery is at least once**

A timeout cannot be distinguished from a failure, so a retry after one may
duplicate a delivered post. Put a stable identifier in the envelope so a
receiver can deduplicate, and write the guarantee down where an operator will
read it.

- [ ] **Step 7: Confirm teeth, commit**

Mutations: the timeout not applied; the retry count off by one; the dead
letter not published; the identifier regenerated per attempt, which would
break exactly the deduplication step 6 promises.

```bash
git commit -m "feat(rules): post the envelope, and keep trying sensibly"
```

---

### Task 6: A message reaches an endpoint, and survives a restart

**Files:**
- Create: `crates/sdmqtt/tests/rules.rs`

Half of milestone four's exit criterion, against the real binary.

1. Start the broker with a store and a rule posting to a local endpoint.
2. Publish over MQTT. The endpoint receives the envelope.
3. Then the part that matters: stop the endpoint, publish again, and **kill
   the broker** while the envelope is undelivered.
4. Start the broker and the endpoint. The envelope arrives.

**Kill, do not shut down.** A clean stop flushes everything, so a polite
shutdown proves nothing about durability. This is recorded in the milestone
two work and the restart test there is the pattern.

- [ ] **Step 1: Write it, watch it fail against a broker with no outbox, then pass**

- [ ] **Step 2: Confirm teeth**

Keep the attempt state in memory rather than the store and confirm the test
fails. That is the mutation the whole task exists for.

- [ ] **Step 3: Commit**

```bash
git commit -m "test: a message reaches an endpoint across a restart"
```
