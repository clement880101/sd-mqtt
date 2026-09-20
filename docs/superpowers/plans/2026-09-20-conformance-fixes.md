# Conformance Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the eight defects the Paho interoperability suite found against
milestone one, so that milestone two builds persistence on top of correct
session semantics rather than on top of them.

**Architecture:** Seven of the eight fixes are in `broker-core`, where the
state they concern lives. Two are in the shell, because they concern the
connection rather than the session: a client's maximum packet size and its
topic alias table are both per connection and are both re-established on
every reconnect. One adds a reason code mapping to the codec, which is spec
data of the kind the codec already owns.

**Tech Stack:** Rust, `tokio`, `bytes`, the project's own `mqtt-codec` and
`broker-core`.

---

## Why this plan exists, and what it is not

The conformance suite is `services/broker/conformance/run.sh`, pinned to a
commit of Eclipse Paho's interoperability tests. Run it before you start and
again when you finish:

```bash
services/broker/conformance/run.sh
```

It exits zero when the failing set matches `conformance/known-failures.txt`,
**and equally fails when a listed test starts passing**. So every task here
that fixes a listed test must also delete that test's line from the baseline,
in the same commit. A task that leaves the baseline stale breaks the build.

**This plan does not touch the broker lock.** The throughput measurement in
task 7 of the listener plan found that throughput falls as publishers are
added, and that is real, but the decision has been taken to scale
horizontally rather than redesign the lock now. Do not refactor the lock. Do
not add a lock acquisition to a hot path. Task 6 adds one to a cold path and
says why.

**Server-to-client topic aliases are out of scope.** Section 5.4 of the
design spec promises topic aliases "in both directions" in v1. Task 9 amends
that sentence, because the outbound half is optional under 3.3.2.3.4 and the
inbound half is the one whose absence loses messages. Do not implement the
outbound half here.

## Conventions that apply to every task

Read `docs/conventions.md` before you start. Three of its seven bear on this
work especially:

- **Mutation testing is not optional.** For each task, name at least two
  mutations of the code you wrote, make each one by hand, and confirm a test
  fails. Use `cp` to back the file up rather than `git checkout --`, because
  the working directory is shared.
- **Ask what a test cannot reach.** Several of these fixes are one line. A
  one-line fix is exactly where a test that cannot fail hides, because the
  round trip passes either way. For each test you add, state what it would
  catch that the existing suite would not.
- **Prefer a restructure that makes the mutation unwriteable** to a test per
  path, where one is available.

Run these before every commit, from `services/broker`:

```bash
cargo fmt --all --check && cargo clippy --all-targets -- -D warnings && cargo test --workspace
```

---

## File Structure

| File | Change |
|---|---|
| `crates/broker-core/src/broker/mod.rs` | Session expiry from the 3.1.1 clean flag; DISCONNECT's expiry property |
| `crates/broker-core/src/session/state.rs` | Deadlines must not fire early |
| `crates/broker-core/src/retained.rs` | The retained store keeps properties |
| `crates/broker-core/src/session/outbound.rs` | Message expiry decremented on delivery; oversize completion |
| `crates/mqtt-codec/src/error.rs` | A codec error's reason code |
| `crates/server/src/connection.rs` | Topic alias table; disconnect on a decode error; maximum packet size |
| `services/broker/conformance/known-failures.txt` | A line deleted per fix |
| `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md` | Section 5.4 amended; open questions closed |

---

### Task 1: A 3.1.1 session with `CleanSession` clear must survive its connection

**Files:**
- Modify: `crates/broker-core/src/broker/mod.rs` around line 183
- Test: the same file's test module

The session expiry interval is read from the MQTT 5 property and defaulted to
zero. A 3.1.1 connect carries no such property, so every 3.1.1 session is
given zero and is destroyed the moment the client disconnects.
[MQTT-3.1.2-4] requires that a session with `CleanSession` clear be stored
and resumed.

The mapping is: for version 5, the property, defaulting to zero. For 3.1.1,
zero when `clean_start` is set and an interval that does not expire when it
is clear. Use `u32::MAX`. Do not invent a configured maximum here; capping a
client's requested expiry is a policy question and belongs to milestone 7.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_three_one_one_session_with_clean_session_clear_outlives_its_connection() {
    let mut broker = Broker::new(BrokerConfig::default());
    let mut out = Vec::new();

    let mut connect = connect_v311("durable");
    connect.clean_start = false;
    let first = broker.connect(&connect, 0, &mut out).unwrap();
    broker.disconnect(first.subscriber, &mut out);

    // Far past any plausible deadline.
    broker.tick(1_000_000, &mut out);

    let second = broker.connect(&connect, 1_000_001, &mut out).unwrap();
    assert!(
        second.session_present,
        "[MQTT-3.1.2-4]: a 3.1.1 session with CleanSession clear is stored"
    );
}
```

Write `connect_v311` beside it if the test module has no such helper: a
`Connect` with `version: Version::V311`, the given client id, `clean_start`
as the caller sets it, and `Properties::default()`.

- [ ] **Step 2: Run it and watch it fail**

```bash
cargo test -p broker-core a_three_one_one_session_with_clean_session_clear
```

Expected: fails on the assertion, `session_present` being false.

- [ ] **Step 3: Write the mapping**

Replace the `set_expiry_interval` call with a call to a named function, so
that the rule is a thing with a name that can be tested and read:

```rust
/// How long a session outlives its connection, in seconds.
///
/// MQTT 5 states it as a property and defaults it to zero, 3.1.2.11.2.
/// MQTT 3.1.1 has only the clean flag, and [MQTT-3.1.2-4] requires a
/// session with it clear to be stored: there is no interval to read, so
/// the equivalent is one that does not elapse.
fn expiry_interval(connect: &Connect) -> u32 {
    match connect.version {
        Version::V5 => connect
            .properties
            .session_expiry_interval
            .unwrap_or_default(),
        _ if connect.clean_start => 0,
        _ => u32::MAX,
    }
}
```

- [ ] **Step 4: Run the test and the whole suite**

```bash
cargo test -p broker-core
```

- [ ] **Step 5: Confirm the test has teeth**

Make each of these by hand and confirm a test fails:
1. `_ => u32::MAX` becomes `_ => 0`. The new test must fail.
2. `Version::V5 => ...unwrap_or_default()` becomes `u32::MAX`. An existing
   session expiry test must fail. If none does, that is a gap: add a test
   that a version 5 connect with no expiry property does not outlive its
   connection.

- [ ] **Step 6: Delete the baseline line and commit**

Remove the `v311:test_offline_message_queueing` line from
`services/broker/conformance/known-failures.txt`, then:

```bash
services/broker/conformance/run.sh
git add crates/broker-core/src/broker/mod.rs conformance/known-failures.txt
git commit -m "fix(core): a 3.1.1 session with CleanSession clear is stored"
```

---

### Task 2: A DISCONNECT may change the session expiry interval

**Files:**
- Modify: `crates/broker-core/src/broker/mod.rs`, the `Packet::Disconnect` arm around line 417
- Test: the same file's test module

3.14.2.2.2 lets a client set a Session Expiry Interval on the way out. The
handler passes only the reason code to `detach`, so the property is ignored
and a client cannot extend its session as it leaves.

There is a second rule that comes with the first, and it is the easier one to
forget: [MQTT-3.14.4-3] makes it a protocol error to send a non-zero interval
on DISCONNECT when the CONNECT said zero. A session that was never going to
outlive its connection cannot be resurrected on the way out. Implement both;
they are one rule seen from two sides.

Order matters. The interval must be set **before** `detach` runs, because
`on_disconnected` computes `expires_at` from it.

- [ ] **Step 1: Write both failing tests**

```rust
#[test]
fn a_disconnect_may_extend_the_session() {
    let mut broker = Broker::new(BrokerConfig::default());
    let mut out = Vec::new();
    let mut connect = connect_v5("mover");
    connect.properties.session_expiry_interval = Some(1);
    let first = broker.connect(&connect, 0, &mut out).unwrap();

    let goodbye = Packet::Disconnect(Disconnect {
        reason: ReasonCode::Success,
        properties: Properties {
            session_expiry_interval: Some(300),
            ..Properties::default()
        },
    });
    broker.handle(first.subscriber, &goodbye, &mut out).unwrap();

    // Past the interval the CONNECT asked for, inside the one the
    // DISCONNECT asked for.
    broker.tick(60, &mut out);
    let second = broker.connect(&connect, 61, &mut out).unwrap();
    assert!(second.session_present, "3.14.2.2.2: the DISCONNECT's interval stands");
}

#[test]
fn a_disconnect_must_not_resurrect_a_session_that_asked_to_end() {
    let mut broker = Broker::new(BrokerConfig::default());
    let mut out = Vec::new();
    let mut connect = connect_v5("liar");
    connect.properties.session_expiry_interval = Some(0);
    let first = broker.connect(&connect, 0, &mut out).unwrap();

    let goodbye = Packet::Disconnect(Disconnect {
        reason: ReasonCode::Success,
        properties: Properties {
            session_expiry_interval: Some(300),
            ..Properties::default()
        },
    });
    assert_eq!(
        broker.handle(first.subscriber, &goodbye, &mut out),
        Err(ReasonCode::ProtocolError),
        "[MQTT-3.14.4-3]: zero on CONNECT cannot become non-zero on DISCONNECT"
    );
}
```

- [ ] **Step 2: Run them and watch both fail**

```bash
cargo test -p broker-core a_disconnect_may_extend
cargo test -p broker-core a_disconnect_must_not_resurrect
```

Expected: the first fails on `session_present`, the second on getting `Ok`.

- [ ] **Step 3: Implement both halves**

In the `Packet::Disconnect` arm, before `self.detach(...)`:

```rust
Packet::Disconnect(disconnect) => {
    // 3.14.2.2.2 lets a client change how long its session outlives
    // this connection. [MQTT-3.14.4-3] bars it from raising an
    // interval of zero: a session that asked to end with its
    // connection cannot be kept alive on the way out.
    if let Some(requested) = disconnect.properties.session_expiry_interval {
        let session = self
            .registry
            .get_mut(subscriber)
            .ok_or(ReasonCode::ProtocolError)?;
        if requested != 0 && session.expiry_interval() == 0 {
            return Err(ReasonCode::ProtocolError);
        }
        session.set_expiry_interval(requested);
    }
    self.detach(subscriber, Some(disconnect.reason), now);
    Ok(())
}
```

Add the reader this needs to `Session`, next to `set_expiry_interval` in
`crates/broker-core/src/session/state.rs`:

```rust
/// How long the session outlives its connection.
pub fn expiry_interval(&self) -> u32 {
    self.expiry_interval
}
```

Note that returning `Err` here closes the connection, and the shell already
turns a returned reason code into a DISCONNECT before it does. That is the
`close_with` path in `crates/server/src/connection.rs`.

- [ ] **Step 4: Run the tests and the whole suite**

- [ ] **Step 5: Confirm the tests have teeth**

1. Delete the `session.set_expiry_interval(requested);` line. The first test
   must fail.
2. Change `requested != 0 && session.expiry_interval() == 0` to
   `requested != 0`. The first test must fail, because a legitimate extension
   would now be refused.
3. Change it to `session.expiry_interval() == 0`. The second test must fail,
   because a DISCONNECT carrying an explicit zero would now be an error.

Mutation 3 is the one worth pausing on: an explicit `Some(0)` on a session
whose interval is already zero is legal and common. If no test fails, add
one.

- [ ] **Step 6: Delete the baseline line and commit**

Remove `v5:test_session_expiry`, then:

```bash
services/broker/conformance/run.sh
git add crates/broker-core/src/broker/mod.rs crates/broker-core/src/session/state.rs conformance/known-failures.txt
git commit -m "fix(core): honour a disconnect's session expiry interval"
```

---

### Task 3: A deadline must not fall due early

**Files:**
- Modify: `crates/broker-core/src/session/state.rs`, `on_disconnected` around line 113
- Test: the same file's test module

`now` is a coarse clock in whole seconds. A deadline set as `now + delay`
where `now` is the floor of the elapsed time falls due after anything from
`delay - 1` to `delay` real seconds. For a will delay this was measured
firing at 2.28 seconds against a three-second interval.

3.1.3.2.2 says the server delays publishing the will *until* the interval has
passed. Firing early breaks that; firing late does not. Every other deadline
in this broker errs late, which is why this one was the surprise.

The fix is to round the deadline up by one tick rather than down. A delay of
zero must still be owed immediately, so the rounding applies only to a
non-zero delay.

**Check the session expiry deadline on the line below it.** It has the same
shape and the same defect: [MQTT-3.1.2-11] ends the session after the
interval has passed, not up to a second before. Fix both, and say in the
commit that you did.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_will_delay_is_never_cut_short_by_the_coarse_clock() {
    // The disconnect lands part-way through a tick, so `now` reads one
    // less than the true elapsed time. A deadline of `now + delay` would
    // then fall due before `delay` seconds had actually passed.
    let mut session = a_session_with_a_will(3);
    session.set_expiry_interval(300);

    session.on_disconnected(false, 10);

    assert!(
        session.take_due_will(12).is_none(),
        "two ticks after a three second delay, the will is not yet owed"
    );
    assert!(
        session.take_due_will(13).is_none(),
        "3.1.3.2.2: at the tick the delay lands on, up to a second of it may remain"
    );
    assert!(
        session.take_due_will(14).is_some(),
        "the tick after, the delay has certainly passed"
    );
}
```

- [ ] **Step 2: Run it and watch it fail**

Expected: the second assertion fails. The will is taken at tick 13.

- [ ] **Step 3: Round both deadlines up**

```rust
} else if self.will.is_some() {
    let delay = if self.version == Version::V5 {
        self.delay_capped_by_expiry()
    } else {
        0
    };
    self.will_at = Some(deadline(now, delay));
}

self.expires_at = Some(deadline(now, self.expiry_interval));
```

With, as a free function in the same file:

```rust
/// A deadline that errs late.
///
/// `now` is a whole-second clock, so a disconnect part-way through a tick
/// reads as the tick that has already begun. A deadline of `now + delay`
/// would then fall due after as little as `delay - 1` real seconds:
/// 3.1.3.2.2 and [MQTT-3.1.2-11] both say a period elapses before the
/// thing happens, so the extra tick is the difference between late and
/// wrong. A delay of zero means at once and is not rounded.
fn deadline(now: u32, delay: u32) -> u32 {
    if delay == 0 {
        now
    } else {
        now.saturating_add(delay).saturating_add(1)
    }
}
```

Existing tests will move by one tick. Read each one before you change it and
satisfy yourself the new number is right, rather than sliding every literal
until the suite is green. If a test's intent was "expires at exactly T", its
intent was wrong and its comment needs rewriting too.

- [ ] **Step 4: Run the whole broker-core suite**

- [ ] **Step 5: Confirm the test has teeth**

1. `saturating_add(1)` becomes `saturating_add(0)`. The new test must fail.
2. `if delay == 0 { now }` becomes `if delay == 0 { now + 1 }`. An existing
   test that an ungraceful 3.1.1 disconnect owes its will at once must fail.
   There is one at `an_ungraceful_disconnect_owes_a_will_at_once_without_a_delay`.

- [ ] **Step 6: Delete the baseline line and commit**

Remove `v5:test_will_delay`, then:

```bash
services/broker/conformance/run.sh
git add crates/broker-core/src/session/state.rs conformance/known-failures.txt
git commit -m "fix(core): a will delay and a session expiry must not end early"
```

---

### Task 4: The retained store keeps the message's properties

**Files:**
- Modify: `crates/broker-core/src/retained.rs`
- Modify: wherever `RetainedMessage` is built and consumed; `rg 'RetainedMessage' crates/` finds them
- Test: `crates/broker-core/src/retained.rs` test module, plus a delivery test

`RetainedMessage` holds payload, quality of service and expiry. A retained
message's User Properties and Content Type are part of the stored Application
Message, and [MQTT-3.3.2-17] requires User Properties to be forwarded
unaltered. Live forwarding is already correct, so this is the store alone.

Add `properties: Arc<Properties>` to `RetainedMessage`, matching `Message`,
which shares them for the same reason: fan-out clones one retained message
per matching subscriber.

Two properties must **not** survive into a retained delivery, and both are
already handled elsewhere if you do this in the right order:

- **Subscription identifiers** are per subscriber and are set when the
  outgoing publish is built, in `outbound::publish`. Storing them would be
  wrong; check that whatever you store does not carry any.
- **The topic alias** is stripped on ingress by task 8. Until task 8 lands,
  a stored alias would be forwarded. Do not add a second strip here; add a
  test after task 8 that a retained message carries no alias, and let the
  ingress strip be the single place the rule lives.

**Message Expiry Interval is task 5's problem, not yours.** Store it as it
arrives. Task 5 rewrites it on the way out, for retained and queued
deliveries alike.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_retained_message_keeps_its_properties() {
    let mut store = Retained::new();
    let properties = Properties {
        content_type: Some("text/plain".to_string()),
        user_properties: vec![("a".to_string(), "2".to_string())],
        ..Properties::default()
    };
    store.store("retain/probe", retained_with(b"body", &properties), 0);

    let mut found = Vec::new();
    store.matching("retain/probe", 0, &mut found);

    let (_, message) = found.pop().expect("the message is there");
    assert_eq!(
        message.properties.user_properties,
        vec![("a".to_string(), "2".to_string())],
        "[MQTT-3.3.2-17]: user properties are forwarded unaltered"
    );
    assert_eq!(message.properties.content_type.as_deref(), Some("text/plain"));
}
```

Adapt the constructor calls to the store's real signature; `retained_with` is
a helper you write beside the test.

- [ ] **Step 2: Run it and watch it fail to compile**

A test that does not compile because the field does not exist is a failing
test. Do not skip this step; it is what proves the field was absent.

- [ ] **Step 3: Add the field and carry it through**

Add to the struct, set it where retained messages are built from a publish,
and read it where a retained message becomes a delivery. The compiler will
list every site.

- [ ] **Step 4: Write the delivery test**

The store keeping the properties is half the fix. The other half is that they
reach the subscriber. Add a test at the broker level: publish retained with a
content type, subscribe, and assert the PUBLISH the new subscriber receives
carries it.

This is the test that the round trip would otherwise not see, because a store
that keeps properties and a delivery path that drops them look identical from
inside `retained.rs`.

- [ ] **Step 5: Run the whole suite**

- [ ] **Step 6: Confirm the tests have teeth**

1. Set `properties: Arc::new(Properties::default())` at the store site. Both
   tests must fail.
2. Set it to default at the delivery site only. The store test must pass and
   the delivery test must fail. If both fail, your store test is reaching
   through the delivery path and is not testing what it claims.

- [ ] **Step 7: Delete the baseline line and commit**

Remove `v5:test_retained_message`, then:

```bash
services/broker/conformance/run.sh
git add crates/broker-core/src/retained.rs conformance/known-failures.txt
git commit -m "fix(core): a retained message keeps its properties"
```

---

### Task 5: Message Expiry Interval is decremented by the time waited

**Files:**
- Modify: `crates/broker-core/src/session/outbound.rs`, `publish` at line 395 and its two callers at 74 and 97
- Test: the same file's test module

[MQTT-3.3.2-6] requires the server to send the Message Expiry Interval it
received minus the time the message waited. A delivery that sat in a queue
for three seconds currently arrives with the interval it was published with,
so a client cannot tell how long it has left.

`Message` already carries `expires_at`, a coarse deadline, which is the
subtraction already done. The remaining interval is therefore
`expires_at - now`, and `publish` needs `now` to compute it.

Always write the property from `expires_at`, rather than only when the
message carries one. A message with no deadline must carry no interval, and
overwriting unconditionally means a stale interval in a cloned property block
cannot survive.

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn a_delivery_reports_the_expiry_that_is_left_rather_than_the_one_it_arrived_with() {
    let mut item = delivery(QoS::AtMostOnce, b"body");
    item.message.expires_at = Some(60);
    item.message.properties = Arc::new(Properties {
        message_expiry_interval: Some(60),
        ..Properties::default()
    });

    // Sent at tick 20, so forty seconds of the sixty remain.
    let Packet::Publish(sent) = publish(&item, None, 20) else {
        panic!("a publish");
    };

    assert_eq!(
        sent.properties.message_expiry_interval,
        Some(40),
        "[MQTT-3.3.2-6]: the interval sent is the one received less the wait"
    );
}

#[test]
fn a_delivery_with_no_deadline_carries_no_expiry() {
    let item = delivery(QoS::AtMostOnce, b"body");
    let Packet::Publish(sent) = publish(&item, None, 20) else {
        panic!("a publish");
    };
    assert_eq!(sent.properties.message_expiry_interval, None);
}
```

- [ ] **Step 2: Run them and watch the first fail**

Expected: it will not compile until `publish` takes `now`. Change the
signature first, leave the body alone, and watch the assertion fail with
`Some(60)`.

- [ ] **Step 3: Rewrite the property**

```rust
fn publish(item: &Delivery, packet_id: Option<u16>, now: u32) -> Packet {
    let mut properties = (*item.message.properties).clone();
    properties.subscription_identifiers = item.subscription_ids.clone();
    // [MQTT-3.3.2-6]: what the client is told is what is left, not what
    // was published. Written unconditionally rather than only when a
    // deadline exists, so that a cloned property block cannot carry a
    // stale interval past a message that has none.
    properties.message_expiry_interval = item
        .message
        .expires_at
        .map(|at| at.saturating_sub(now));

    // ...unchanged
}
```

Pass `now` at both call sites. Both are in methods that already have it; if
one does not, thread it rather than reaching for a clock.

- [ ] **Step 4: Run the whole suite**

- [ ] **Step 5: Confirm the tests have teeth**

1. `at.saturating_sub(now)` becomes `at`. The first test must fail.
2. The whole assignment is deleted. The first test must fail and the second
   must pass, which tells you the second test is not carrying its weight on
   its own. That is expected: it exists to catch mutation 3.
3. `.map(...)` becomes `.map(...).or(Some(0))`. The second test must fail.

- [ ] **Step 6: Delete the baseline line and commit**

Remove `v5:test_publication_expiry`, then:

```bash
services/broker/conformance/run.sh
git add crates/broker-core/src/session/outbound.rs conformance/known-failures.txt
git commit -m "fix(core): report the message expiry that is left"
```

---

### Task 6: A client's Maximum Packet Size is honoured on the way out

**Files:**
- Modify: `crates/server/src/connection.rs`, `write_loop` and `render`
- Modify: `crates/broker-core/src/broker/mod.rs` for the completion call
- Test: `crates/server/src/connection.rs` test module

[MQTT-3.1.2-24] forbids the server sending a packet larger than the client
advertised in its CONNECT. [MQTT-3.1.2-25] says what to do instead: discard
it and behave as though delivery had completed.

That second half is why this is not a check in the writer that drops bytes.
Dropping the bytes of a quality-of-service 1 or 2 publish leaks its inflight
slot and its packet identifier forever, which is the same class of bug as the
undispatched publish fixed during the broker plan.

**The design, decided:** check in the writer, where the packet has already
been encoded and its true length is known, and call back into the broker to
complete the delivery. One encode, an exact length, and correct state.

The alternative, computing a length in `broker-core` before building the
packet, needs either a second size calculation that will drift from the
encoder or a throwaway encode on the hot path. Neither is worth it.

This adds a broker lock acquisition to the write path. It is on the oversize
branch only, which is taken by no packet on a connection that advertised no
limit, and the lock is not touched otherwise. Say so in a comment, because a
future reader measuring the write path needs to know it was deliberate.

- [ ] **Step 1: Write the failing test**

An integration-style test in the connection module: connect with
`maximum_packet_size: Some(64)`, subscribe, have another client publish a
payload of 200 bytes and then a payload of 4 bytes, and assert the small one
arrives and the large one does not.

The second publish is the part that matters. A test that only asserts the
large one is absent is asserting an absence, and convention 5 says such a
test survives deleting the feature. The small message arriving afterwards is
what proves the connection is still alive and still delivering.

- [ ] **Step 2: Run it and watch it fail**

Expected: the 200-byte publish arrives.

- [ ] **Step 3: Carry the limit to the writer**

Read `connect.properties.maximum_packet_size` where `receive_maximum` is read
around line 159, and pass it to `write_loop`. A value of zero is a protocol
error under 3.1.2.11.4; refuse the connection the way a zero receive maximum
is refused just above.

- [ ] **Step 4: Discard and complete**

In `write_loop`, after `render` has filled the buffer and before the write:
if a limit is set and `buf.len()` exceeds it, do not write. If the action was
a publish with a packet identifier, call the broker to complete it.

Add to `Broker`:

```rust
/// A packet was too large to send and has been discarded.
///
/// [MQTT-3.1.2-25] says to behave as if delivery had completed, which
/// means releasing the identifier and the inflight slot rather than
/// leaving both held for an acknowledgement that cannot arrive.
pub fn discard_oversize(
    &mut self,
    subscriber: SubscriberId,
    packet_id: u16,
    now: u32,
    out: &mut Vec<Action>,
)
```

Find the existing completion path in `Outbound` and reuse it rather than
writing a second one. `on_puback` completes an at-least-once delivery;
quality of service 2 needs the equivalent for a message awaiting a receipt.
If no such method exists, add one beside `on_puback` and give it the same
shape. Do not open-code identifier release at the call site.

- [ ] **Step 5: Run the whole suite**

- [ ] **Step 6: Confirm the test has teeth**

1. Change `buf.len() > limit` to `buf.len() > limit * 2`. The test must fail.
2. Change it to `buf.len() >= limit`. Nothing may fail on the equal case
   unless you wrote a test for it; add one, because a packet of exactly the
   advertised size is legal and refusing it is a real bug that no other test
   would catch.
3. Delete the `discard_oversize` call. Write a test that a second large
   quality-of-service 1 message does not wedge the connection: without the
   completion, the inflight slot never frees and delivery stops. If your
   existing tests all pass with the call deleted, you have tested the drop
   and not the completion, which is the half that is hard to get right.

- [ ] **Step 7: Delete the baseline line and commit**

Remove `v5:test_maximum_packet_size`, then:

```bash
services/broker/conformance/run.sh
git add crates/server/src/connection.rs crates/broker-core/src/broker/mod.rs crates/broker-core/src/session/outbound.rs conformance/known-failures.txt
git commit -m "fix(server): never send a packet larger than the client allows"
```

---

### Task 7: A decode error tells the client why before the socket closes

**Files:**
- Modify: `crates/mqtt-codec/src/error.rs`
- Modify: `crates/server/src/connection.rs`, `read_loop` around line 457
- Test: both files

A protocol error the broker detects is already reported: `read_loop` calls
`close_with`, which queues a DISCONNECT carrying the reason. A protocol error
the **codec** detects is not, because `reader.next(...)?` propagates straight
out of the loop and the socket closes with nothing written. An MQTT 5 client
then has no way to learn what it did wrong.

Give `CodecError` a reason code. It belongs in the codec, next to the reason
code table the codec already owns, because the mapping is spec data rather
than a shell policy.

The mapping, from section 2.4 and the tables it feeds:

| Variant | Reason code |
|---|---|
| `MalformedPacket` | `MalformedPacket` (0x81) |
| `InvalidUtf8` | `MalformedPacket` (0x81) |
| `ProtocolError` | `ProtocolError` (0x82) |
| `PacketTooLarge` | `PacketTooLarge` (0x95) |
| `InvalidTopicName` | `TopicNameInvalid` (0x90) |
| `InvalidTopicFilter` | `TopicFilterInvalid` (0x8F) |
| `TopicNameTooDeep` | `TopicNameInvalid` (0x90) |
| `TopicFilterTooDeep` | `TopicFilterInvalid` (0x8F) |
| `UnsupportedVersion` | `UnsupportedProtocolVersion` (0x84) |

The last of these can only arise before a CONNACK, where the existing
`refuse` path handles it; map it anyway rather than leaving a hole, and say
in a comment that it is unreachable from `read_loop`.

- [ ] **Step 1: Write the failing test in the codec**

A table test over every variant, asserting the code above. Write the table
out by hand from the specification rather than from the implementation. This
is the same reasoning as the golden vectors in the codec: a test derived from
the code under test cannot see a transposition.

- [ ] **Step 2: Write the failing test in the shell**

Connect as version 5, send a PUBLISH whose topic alias is zero, and assert a
DISCONNECT with reason 0x82 arrives before the socket closes.

Alias zero is the case the conformance suite found. The codec already refuses
it; what is missing is the client being told.

- [ ] **Step 3: Run both and watch them fail**

- [ ] **Step 4: Implement**

```rust
impl CodecError {
    /// The reason code that reports this error to a peer.
    ///
    /// Section 2.4 and the tables under 3.14.2.1. The mapping lives here
    /// rather than in the shell because it is a property of the error,
    /// and a second shell would otherwise write it again and differently.
    pub fn reason_code(&self) -> ReasonCode {
        // ...
    }
}
```

In `read_loop`, replace the bare `?` on `reader.next(...)` with a match that
reports a protocol failure before returning:

```rust
next = reader.next(self.version) => match next {
    Ok(Some(packet)) => packet,
    Ok(None) => return Ok(()),
    // A malformed packet is the client's fault and it is owed the
    // reason: the decoder found it, so nothing above has had the
    // chance to report it. An I/O failure is not reportable,
    // because the socket is what failed.
    Err(FrameError::Protocol(error)) => {
        let reason = error.reason_code();
        self.close_with(reason);
        return Err(ConnectionError::Codec(error));
    }
    Err(other) => return Err(other.into()),
},
```

- [ ] **Step 5: Run the whole suite**

- [ ] **Step 6: Confirm the tests have teeth**

1. Swap the `MalformedPacket` and `ProtocolError` rows in the mapping. The
   codec table test must fail. If it does not, your table was copied from
   the implementation.
2. Delete the `self.close_with(reason)` line. The shell test must fail.
3. Change `close_with(reason)` to `close_with(ReasonCode::Success)`. The
   shell test must fail. If it only asserts that *a* disconnect arrived, it
   will not, and asserting the code is the point.

- [ ] **Step 7: Commit**

No baseline line is removed here; this defect has no test of its own in the
suite. Say so in the commit body.

```bash
git add crates/mqtt-codec/src/error.rs crates/server/src/connection.rs
git commit -m "fix: report a decode error before closing the connection"
```

---

### Task 8: Inbound topic aliases are resolved

**Files:**
- Modify: `crates/server/src/connection.rs`
- Modify: `crates/sdmqtt/src/config.rs` for the advertised maximum
- Test: `crates/server/src/connection.rs` test module

This is the defect that loses messages. A PUBLISH carrying a Topic Alias is
accepted, the alias is never recorded, and a later PUBLISH carrying an empty
topic name and only the alias is dropped in silence. The alias is then
forwarded verbatim to subscribers, which 3.1.2.11.5 forbids for any
subscriber that advertised no maximum of its own.

The table is per connection, not per session: 3.3.2.3.4 says aliases last
only for the network connection, and a resumed session starts again from
nothing. So it lives in the shell beside the receive maximum, and it is
rebuilt on every reconnect without anyone having to remember to clear it.

**The rules, from 3.3.2.3.4:**

- Advertise a maximum in the CONNACK. Without one, the client must send no
  aliases at all, which is today's behaviour and is why nothing crashed.
- An alias of zero, or greater than the advertised maximum, is a protocol
  error. Reason code `TopicAliasInvalid` (0x94).
- A PUBLISH with a non-empty topic and an alias sets the mapping, replacing
  any previous mapping for that alias.
- A PUBLISH with an empty topic and an alias uses the mapping. An unmapped
  alias is a protocol error.
- The alias is **stripped** before the message goes any further. Nothing
  below the shell should ever see one.

The strip is what makes the retained store and every subscriber correct
without any of them knowing aliases exist. Do it once, here, on ingress.

- [ ] **Step 1: Write the failing tests**

Four of them, because four rules can each be broken on their own:

```rust
// 1. The mapping is made and then used.
async fn a_publish_with_only_an_alias_is_routed_to_the_topic_it_named() { }

// 2. The alias does not reach a subscriber.
async fn a_subscriber_never_sees_the_publishers_topic_alias() { }

// 3. An alias past the advertised maximum is refused.
async fn an_alias_above_the_maximum_is_a_protocol_error() { }

// 4. An alias that was never mapped is refused.
async fn an_unmapped_alias_is_a_protocol_error() { }
```

Test 1 is the message-loss case: publish to `alias/topic` with alias 1, then
publish with an empty topic and alias 1, and assert the subscriber receives
**both**. Asserting only the second arrives would pass against a broker that
routed the second and lost the first.

- [ ] **Step 2: Run them and watch all four fail**

- [ ] **Step 3: Advertise a maximum**

Add `topic_alias_maximum: u16` to the connection config with a default of 16,
and set it in `acknowledgement`. Amend the doc comment above that function,
which currently states that the server accepts no topic aliases; it will no
longer be true.

Sixteen is a default and not a discovery. It is enough for the handful of
topics a device publishes to, and small enough that the table costs nothing
per connection. Put that sentence in the config's doc comment so the next
person knows it was chosen rather than measured.

- [ ] **Step 4: Resolve on ingress**

Hold the table in the connection state as a `Vec<Option<Arc<str>>>` sized to
the maximum, indexed by alias minus one. A vector rather than a map because
the maximum is small, known at connect time, and bounded by us.

Apply it to the packet before `broker.handle` sees it, which means
`read_loop` needs `mut packet`. Return `Err(ConnectionError::Protocol(...))`
by the same `close_with` path the second-connect check uses, so the client is
told.

- [ ] **Step 5: Run the whole suite**

- [ ] **Step 6: Confirm the tests have teeth**

1. Delete the strip. Test 2 must fail.
2. Record the mapping but never read it. Test 1 must fail on the second
   message.
3. Change the bounds check from `alias > maximum` to `alias > maximum + 1`.
   Test 3 must fail.
4. Treat an unmapped alias as an empty topic rather than an error. Test 4
   must fail.

If mutation 1 leaves everything green, your subscriber assertion is reading
the payload and not the properties.

- [ ] **Step 7: Delete the baseline line and commit**

Remove `v5:test_client_topic_alias`, then:

```bash
services/broker/conformance/run.sh
git add crates/server/src/connection.rs crates/sdmqtt/src/config.rs conformance/known-failures.txt
git commit -m "fix(server): resolve and strip a client's topic aliases"
```

---

### Task 9: Reconcile the specification with what was built

**Files:**
- Modify: `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`
- Modify: `services/broker/conformance/known-failures.txt`
- Modify: `docs/conventions.md`

Six of the eight defects were features section 5.4 lists as in scope for v1,
and no task in any of the five milestone-one plans built them. The
conformance suite caught what the plan review did not. That is the finding
worth recording, and it is worth more than the fixes.

- [ ] **Step 1: Amend section 5.4**

Change "topic aliases in both directions" to say that a client's aliases are
resolved in v1 and that the server does not itself use aliases outbound,
which 3.3.2.3.4 permits. State it as a decision with its reason, not as an
omission.

- [ ] **Step 2: Rewrite the open questions entry**

The eight defects are recorded in section 17. Replace that entry with what
was done, keeping the entry rather than deleting it: a reader who finds the
defect list in the git history should find its resolution in the same place.
Leave the three suite assumptions recorded exactly as they are; they will
come back if they are not.

- [ ] **Step 3: Add the convention**

`docs/conventions.md` has seven conventions, each with the failure that
earned it. This earned an eighth:

> **A spec section is not a task list.** Six features that section 5.4 of
> the design listed as in scope had no task in any plan, and every one of
> them shipped missing. Nothing in the plan-writing process reads the spec
> back. Before a milestone closes, walk its spec sections and name the task
> that implements each line, and where there is none, say whether it is
> deferred or forgotten.

Write it in the voice of the others. Read them first.

- [ ] **Step 4: Verify the baseline is down to five lines**

Six tests should now pass that did not. What must remain: two deferred
`test_subscribe_failure` lines, three suite assumptions, and the one flaky
line. `test_client_topic_alias` may also flip `v5:test_server_topic_alias`
to passing if the suite only checks that aliases work in the direction it
can see; if it does, delete that line too and move its explanation into the
commit body.

- [ ] **Step 5: Run everything**

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
cargo deny check
services/broker/conformance/run.sh
```

- [ ] **Step 6: Commit**

```bash
git add docs/ services/broker/conformance/known-failures.txt
git commit -m "docs: reconcile the spec with what milestone one built"
```
