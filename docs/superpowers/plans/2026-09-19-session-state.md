# Session State Machines Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the part of a broker that decides what to send and when: packet identifier allocation, the outbound queue, the inflight window, and the quality-of-service state machines in both directions.

**Architecture:** Everything here is a **pure state machine**. It performs no I/O, holds no lock, spawns nothing and never awaits. Inputs go in, a list of actions comes out, and the caller performs them. The async shell is the next plan and is deliberately thin.

**Tech Stack:** Rust, `mqtt-codec` for packet types, `broker-core`'s existing matching layer, `uuid` for k-sortable message identifiers, `proptest` for the conservation properties.

**Spec:** `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`, sections 5.2, 5.4 and 6.

---

## The idea that makes this plan work

Quality of service is where brokers lose and duplicate messages, and both failures are invisible in a unit test that sends one message and acknowledges it.

So the centrepiece is a **conservation property**. A generated sequence of publishes, acknowledgements, disconnects and resumes is driven through the session, and the test asserts what the protocol actually promises:

- At most once: a message is delivered zero or one times, never twice.
- At least once: every accepted message is delivered at least once, and redelivery carries the duplicate flag.
- Exactly once: every accepted message is delivered exactly once, no matter where the four-packet handshake was interrupted.
- A packet identifier in use is never handed out again.
- Nothing is discarded without a drop action naming why.

The pure design exists to make that property possible. A state machine that awaits cannot be driven a million times a second in a test; one that returns a list can.

## Conventions inherited from earlier milestones

Settled by review across two previous plans. They apply unchanged.

1. **The writer refuses what the reader rejects.** Symmetry between the two directions of any pair.
2. **Assert exact error strings, not error variants.** A test asserting only a variant passes for any other cause of the same variant, and this has twice hidden a real bug.
3. **Prove a test has teeth by mutation.** Break the implementation deliberately, confirm that test and only that test fails, restore. Back the file up with `cp`, not `git checkout --`, because the change being proven is usually uncommitted.
4. **Derive protocol rules from the specification, not from this plan.** The plan's tables have been wrong before. Download the OASIS HTML with `curl` and parse the raw markup; the fetch tool has mis-transcribed specification tables, and the Word-export markup splits words across tags so grep alone finds nothing.

A fifth, learned on the routing core: **a generator that cannot produce a case leaves that code untested while looking covered.** A bare multi-level wildcard was unreachable, and a deleted security guard survived twenty thousand cases. When you add a property, ask what its generator structurally cannot produce.

---

## File Structure

| Path | Responsibility |
|---|---|
| `services/broker/crates/broker-core/src/session/mod.rs` | Session module root, re-exports |
| `services/broker/crates/broker-core/src/session/message.rs` | The message that flows through the broker |
| `services/broker/crates/broker-core/src/session/packet_ids.rs` | Packet identifier allocation |
| `services/broker/crates/broker-core/src/session/queue.rs` | The outbound queue, bounds and overflow |
| `services/broker/crates/broker-core/src/session/inflight.rs` | The inflight window |
| `services/broker/crates/broker-core/src/session/outbound.rs` | Delivery state machine, broker to client |
| `services/broker/crates/broker-core/src/session/inbound.rs` | Receive state machine, client to broker |
| `services/broker/crates/broker-core/src/session/state.rs` | The session, assembling the above |
| `services/broker/crates/broker-core/tests/conservation.rs` | The conservation properties |

Split so that a change to overflow policy touches one file and a change to the exactly-once handshake touches another.

---

## Task 1: The message, and identifiers that sort by time

**Files:**
- Create: `services/broker/crates/broker-core/src/session/mod.rs`, `services/broker/crates/broker-core/src/session/message.rs`
- Modify: `services/broker/crates/broker-core/src/lib.rs`, `services/broker/crates/broker-core/Cargo.toml`

- [ ] **Step 1: Add the dependency and declare the module**

Add to `services/broker/crates/broker-core/Cargo.toml`, under `[dependencies]`:

```toml
uuid = { version = "1", default-features = false, features = ["v7", "std"] }
```

Default features are off deliberately: the crate pulls serialisation and formatting helpers nothing here needs.

Add `pub mod session;` to `src/lib.rs`, and create `src/session/mod.rs`:

```rust
//! Session state machines.
//!
//! Everything in this module is pure. It performs no I/O, holds no lock and
//! never awaits: inputs go in, a list of actions comes out, and the caller
//! performs them. That is what makes the quality-of-service handshakes
//! testable by driving millions of generated sequences through them.

pub mod message;

pub use message::{Message, MessageId};
```

- [ ] **Step 2: Write the failing test**

Create `src/session/message.rs` with tests only:

```rust
//! The message that flows from a publisher to each subscriber.

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mqtt_codec::QoS;

    fn message(topic: &str, payload: &'static [u8]) -> Message {
        Message::new(topic, Bytes::from_static(payload), QoS::AtMostOnce, false)
    }

    #[test]
    fn identifiers_are_unique() {
        let a = MessageId::new();
        let b = MessageId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn identifiers_sort_by_creation_order() {
        // A time-ordered identifier is what lets a queue restore in order
        // without storing a separate sequence alongside it.
        let mut ids: Vec<MessageId> = (0..64).map(|_| MessageId::new()).collect();
        let created = ids.clone();
        ids.sort();
        assert_eq!(ids, created, "identifiers should already be in order");
    }

    #[test]
    fn a_message_carries_what_delivery_needs() {
        let message = message("a/b", b"hello");
        assert_eq!(&*message.topic, "a/b");
        assert_eq!(&message.payload[..], b"hello");
        assert_eq!(message.qos, QoS::AtMostOnce);
        assert!(!message.retain);
        assert_eq!(message.expires_at, None);
    }

    #[test]
    fn cloning_shares_the_topic_and_payload_rather_than_copying() {
        // Fan-out clones a message once per subscriber, so the payload must
        // not be copied each time.
        let original = message("a/b", b"hello");
        let copy = original.clone();
        assert!(
            std::sync::Arc::ptr_eq(&original.topic, &copy.topic),
            "the topic should be shared"
        );
        assert_eq!(original.payload.as_ptr(), copy.payload.as_ptr());
    }

    #[test]
    fn size_in_queue_counts_the_payload_and_the_topic() {
        let small = message("a", b"x");
        let large = message("a", b"xxxxxxxxxx");
        assert!(large.size_in_queue() > small.size_in_queue());
        assert!(
            small.size_in_queue() >= 1,
            "an empty payload still occupies a slot"
        );
    }

    #[test]
    fn expiry_is_reported_against_a_supplied_clock() {
        let mut message = message("a", b"x");
        assert!(!message.is_expired(1_000), "no deadline never expires");

        message.expires_at = Some(100);
        assert!(!message.is_expired(99));
        assert!(!message.is_expired(100), "the deadline itself is not past");
        assert!(message.is_expired(101));
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core message`
Expected: FAIL to compile, with `Message` and `MessageId` not found.

- [ ] **Step 4: Write the implementation**

Insert at the top of `message.rs`:

```rust
use std::sync::Arc;

use bytes::Bytes;
use mqtt_codec::{Properties, QoS};
use uuid::Uuid;

/// A message identifier that sorts by the time it was created.
///
/// Version 7 identifiers put a millisecond timestamp in their high bits, so
/// ordering by identifier is ordering by creation. A queue can therefore be
/// restored in order without a separate sequence number beside it, and two
/// nodes can mint identifiers without coordinating.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageId(Uuid);

impl MessageId {
    /// Mint a new identifier.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// The identifier's bytes, for storage.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Rebuild an identifier from stored bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// An application message on its way from a publisher to a subscriber.
///
/// The topic and properties are shared rather than owned, because fan-out
/// clones one message per matching subscriber and copying a topic string
/// and a property block per subscriber is most of the cost of a publish.
#[derive(Debug, Clone)]
pub struct Message {
    /// Identifies this message for the life of the broker.
    pub id: MessageId,
    /// Where it was published.
    pub topic: Arc<str>,
    /// The application payload, which the broker never interprets.
    pub payload: Bytes,
    /// The quality of service it was published at. A subscriber may receive
    /// it at a lower one.
    pub qos: QoS,
    /// Whether the publisher asked for it to be retained.
    pub retain: bool,
    /// MQTT 5 properties, shared across every copy.
    pub properties: Arc<Properties>,
    /// A coarse deadline in seconds, past which it must not be delivered.
    pub expires_at: Option<u32>,
}

impl Message {
    /// A message with no properties and no deadline.
    pub fn new(topic: &str, payload: Bytes, qos: QoS, retain: bool) -> Self {
        Self {
            id: MessageId::new(),
            topic: Arc::from(topic),
            payload,
            qos,
            retain,
            properties: Arc::new(Properties::default()),
            expires_at: None,
        }
    }

    /// Roughly how much of a queue's byte budget this occupies.
    ///
    /// Approximate on purpose. The budget exists to bound memory, not to be
    /// exact, and an exact figure would have to track the shared topic and
    /// properties that several queued messages hold between them.
    pub fn size_in_queue(&self) -> usize {
        self.payload.len() + self.topic.len() + std::mem::size_of::<Self>()
    }

    /// Whether the deadline has passed. A message expires strictly after
    /// its deadline, matching the retained store.
    pub fn is_expired(&self, now: u32) -> bool {
        self.expires_at.is_some_and(|deadline| now > deadline)
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core message`
Expected: PASS, six tests.

- [ ] **Step 6: Confirm the licence gate still passes**

Run, from the repository root:

```bash
cargo deny --manifest-path services/broker/Cargo.toml --config deny.toml check licenses bans sources
```

`uuid` is Apache-2.0 OR MIT, already on the allow list, but it pulls `getrandom` for version 7. If anything in that tree falls outside the allow list, **stop and report rather than widening the list.**

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/broker-core services/broker/Cargo.lock
git commit -m "feat(session): the message and time-ordered identifiers"
```

---

## Task 2: Packet identifier allocation

A packet identifier is a sixteen-bit number the client and broker use to
match an acknowledgement to its publish. Reusing one that is still in
flight makes the peer acknowledge the wrong message, which is a silent
delivery fault rather than an error.

**Files:**
- Create: `services/broker/crates/broker-core/src/session/packet_ids.rs`
- Modify: `services/broker/crates/broker-core/src/session/mod.rs`

- [ ] **Step 1: Declare the module**

Add `pub mod packet_ids;` to `src/session/mod.rs` and re-export `PacketIds`.

- [ ] **Step 2: Write the failing test**

Create `packet_ids.rs` with tests only:

```rust
//! Allocation of the sixteen-bit identifiers that match an acknowledgement
//! to its publish.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_from_one_upwards() {
        let mut ids = PacketIds::default();
        assert_eq!(ids.allocate(), Some(1));
        assert_eq!(ids.allocate(), Some(2));
        assert_eq!(ids.allocate(), Some(3));
    }

    #[test]
    fn never_allocates_zero() {
        // Zero is not a valid packet identifier, so the wrap must skip it.
        let mut ids = PacketIds::default();
        for _ in 0..u16::MAX {
            let id = ids.allocate().expect("space remains");
            assert_ne!(id, 0);
            ids.release(id);
        }
        assert_eq!(ids.allocate(), Some(1), "allocation should wrap to one");
    }

    #[test]
    fn does_not_reuse_an_identifier_still_in_use() {
        let mut ids = PacketIds::default();
        let first = ids.allocate().unwrap();
        // Exhaust the space so the counter wraps back around to `first`.
        for _ in 1..u16::MAX {
            ids.allocate().unwrap();
        }
        assert_eq!(
            ids.allocate(),
            None,
            "with every identifier in use, allocation must fail"
        );
        assert!(ids.is_in_use(first));
    }

    #[test]
    fn reuses_an_identifier_once_released() {
        let mut ids = PacketIds::default();
        let id = ids.allocate().unwrap();
        assert!(ids.is_in_use(id));
        ids.release(id);
        assert!(!ids.is_in_use(id));
        assert_eq!(ids.in_use(), 0);
    }

    #[test]
    fn releasing_something_never_allocated_is_harmless() {
        let mut ids = PacketIds::default();
        ids.release(42);
        assert_eq!(ids.in_use(), 0);
    }

    #[test]
    fn skips_over_identifiers_that_are_still_held() {
        let mut ids = PacketIds::default();
        let one = ids.allocate().unwrap();
        let two = ids.allocate().unwrap();
        ids.release(two);
        // The counter has moved past both; releasing the second should let
        // it be found again on the wrap, while the first stays held.
        assert!(ids.is_in_use(one));
        assert!(!ids.is_in_use(two));
    }

    #[test]
    fn skips_past_held_identifiers_when_the_counter_wraps() {
        // The loop that steps over occupied slots only earns its keep when
        // the counter lands on one that is still held, which needs the
        // wrap. Without this the loop can be deleted and the suite passes.
        let mut ids = PacketIds::default();
        for _ in 0..u16::MAX {
            ids.allocate().expect("space remains");
        }
        ids.release(100);

        assert_eq!(
            ids.allocate(),
            Some(100),
            "allocation must skip every held identifier to reach the free one"
        );
        assert!(ids.is_in_use(1), "and must not have reused the first");
    }

    #[test]
    fn reports_how_many_are_outstanding() {
        let mut ids = PacketIds::default();
        assert_eq!(ids.in_use(), 0);
        let a = ids.allocate().unwrap();
        ids.allocate().unwrap();
        assert_eq!(ids.in_use(), 2);
        ids.release(a);
        assert_eq!(ids.in_use(), 1);
    }

    #[test]
    fn can_reserve_an_identifier_a_peer_chose() {
        // On resume, the identifiers already in flight were chosen before
        // the reconnect and must not be handed out again.
        let mut ids = PacketIds::default();
        assert!(ids.reserve(500));
        assert!(ids.is_in_use(500));
        assert!(!ids.reserve(500), "reserving twice should report the clash");
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core packet_ids`
Expected: FAIL to compile, with `PacketIds` not found.

- [ ] **Step 4: Write the implementation**

Insert at the top of `packet_ids.rs`:

```rust
/// Allocates the sixteen-bit identifiers that match an acknowledgement to
/// its publish.
///
/// The space is small, so the allocator tracks occupancy exactly rather
/// than hoping a counter does not lap an outstanding identifier. Handing
/// out one that is still in flight would make the peer acknowledge the
/// wrong message, which is a silent delivery fault rather than an error.
///
/// A bitmap costs eight kilobytes per session, which is too much when
/// sessions number in the hundreds of thousands, so occupancy is a set
/// sized by what is actually outstanding.
pub struct PacketIds {
    next: u16,
    in_use: std::collections::HashSet<u16, foldhash::fast::RandomState>,
}

impl Default for PacketIds {
    fn default() -> Self {
        Self {
            next: 1,
            in_use: std::collections::HashSet::with_hasher(Default::default()),
        }
    }
}

impl PacketIds {
    /// Take the next free identifier, or nothing when all are outstanding.
    pub fn allocate(&mut self) -> Option<u16> {
        if self.in_use.len() == usize::from(u16::MAX) {
            return None;
        }
        // At most one full lap, and the length check above guarantees a
        // free slot exists, so this terminates.
        loop {
            let candidate = self.next;
            self.next = if self.next == u16::MAX { 1 } else { self.next + 1 };
            if self.in_use.insert(candidate) {
                return Some(candidate);
            }
        }
    }

    /// Claim a specific identifier, for state restored on resume. Reports
    /// whether it was free.
    pub fn reserve(&mut self, id: u16) -> bool {
        id != 0 && self.in_use.insert(id)
    }

    /// Give an identifier back once its exchange is complete.
    pub fn release(&mut self, id: u16) {
        self.in_use.remove(&id);
    }

    /// Whether an identifier is outstanding.
    pub fn is_in_use(&self, id: u16) -> bool {
        self.in_use.contains(&id)
    }

    /// How many are outstanding.
    pub fn in_use(&self) -> usize {
        self.in_use.len()
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core packet_ids`
Expected: PASS, nine tests.

Note that `never_allocates_zero` and `does_not_reuse_an_identifier_still_in_use` each loop sixty-five thousand times. If either is slow enough to notice, say so rather than shrinking the loop, because the wrap is the interesting case.

- [ ] **Step 6: Prove the occupancy check is load-bearing**

Make `allocate` return the counter without consulting the set, confirm
`skips_past_held_identifiers_when_the_counter_wraps` fails, then restore
from a `cp` backup. Report what you saw.

Note which test this is, and which it is not. With no gaps in the
identifier space the length guard already handles exhaustion, so
`does_not_reuse_an_identifier_still_in_use` passes even with the skip loop
removed. Only the wrap makes the counter land on something still held. An
earlier draft of this plan named the wrong test and the mutation survived.

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/broker-core/src
git commit -m "feat(session): packet identifier allocation"
```

---
## Task 3: The outbound queue

What waits when the inflight window is full. Bounded by both a message
count and a byte total, because either alone lets a client exhaust memory:
a count bound permits a thousand megabyte payloads, and a byte bound
permits a million empty ones.

**Nothing is discarded silently.** Every drop reports why, so the caller
can count it. That is the specification's rule and it is the reason the
methods take an output list rather than returning a bare boolean.

**Files:**
- Create: `services/broker/crates/broker-core/src/session/queue.rs`
- Modify: `services/broker/crates/broker-core/src/session/mod.rs`

- [ ] **Step 1: Declare the module**

Add `pub mod queue;` to `src/session/mod.rs` and re-export `Delivery`, `DropReason`, `Overflow`, `Queue` and `QueueLimits`.

- [ ] **Step 2: Write the failing test**

Create `queue.rs` with tests only:

```rust
//! The outbound queue: what waits when the inflight window is full.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::message::Message;
    use bytes::Bytes;
    use mqtt_codec::QoS;

    fn delivery(qos: QoS, payload: &'static [u8]) -> Delivery {
        Delivery {
            message: Message::new("a/b", Bytes::from_static(payload), qos, false),
            qos,
            retain: false,
            subscription_ids: Vec::new(),
        }
    }

    fn limits(max_messages: usize, overflow: Overflow) -> QueueLimits {
        QueueLimits {
            max_messages,
            max_bytes: usize::MAX,
            overflow,
            queue_qos0_while_offline: false,
        }
    }

    #[test]
    fn holds_messages_in_order() {
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();
        for n in 0..3u8 {
            let mut item = delivery(QoS::AtLeastOnce, b"x");
            item.message.payload = Bytes::from(vec![n]);
            assert!(queue.push(item, 0, true, &mut dropped));
        }
        assert!(dropped.is_empty());
        assert_eq!(queue.len(), 3);

        for n in 0..3u8 {
            let item = queue.pop(0, &mut dropped).expect("in order");
            assert_eq!(&item.message.payload[..], &[n]);
        }
        assert!(queue.is_empty());
    }

    #[test]
    fn drop_newest_refuses_the_arrival_and_says_why() {
        let mut queue = Queue::new(limits(2, Overflow::DropNewest));
        let mut dropped = Vec::new();
        assert!(queue.push(delivery(QoS::AtLeastOnce, b"1"), 0, true, &mut dropped));
        assert!(queue.push(delivery(QoS::AtLeastOnce, b"2"), 0, true, &mut dropped));

        assert!(!queue.push(delivery(QoS::AtLeastOnce, b"3"), 0, true, &mut dropped));
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].1, DropReason::QueueFull);
        assert_eq!(&dropped[0].0.message.payload[..], b"3", "the arrival goes");
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn drop_oldest_evicts_the_head_and_accepts_the_arrival() {
        let mut queue = Queue::new(limits(2, Overflow::DropOldest));
        let mut dropped = Vec::new();
        queue.push(delivery(QoS::AtLeastOnce, b"1"), 0, true, &mut dropped);
        queue.push(delivery(QoS::AtLeastOnce, b"2"), 0, true, &mut dropped);

        assert!(queue.push(delivery(QoS::AtLeastOnce, b"3"), 0, true, &mut dropped));
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].1, DropReason::QueueFull);
        assert_eq!(&dropped[0].0.message.payload[..], b"1", "the head goes");
        assert_eq!(queue.len(), 2);

        let first = queue.pop(0, &mut dropped).unwrap();
        assert_eq!(&first.message.payload[..], b"2");
    }

    #[test]
    fn the_byte_budget_bounds_the_queue_independently_of_the_count() {
        let mut queue = Queue::new(QueueLimits {
            max_messages: 1000,
            max_bytes: delivery(QoS::AtLeastOnce, b"0123456789").size_in_queue() * 2,
            overflow: Overflow::DropNewest,
            queue_qos0_while_offline: false,
        });
        let mut dropped = Vec::new();

        assert!(queue.push(delivery(QoS::AtLeastOnce, b"0123456789"), 0, true, &mut dropped));
        assert!(queue.push(delivery(QoS::AtLeastOnce, b"0123456789"), 0, true, &mut dropped));
        assert!(!queue.push(delivery(QoS::AtLeastOnce, b"0123456789"), 0, true, &mut dropped));

        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].1, DropReason::ByteBudget);
        assert_eq!(queue.len(), 2, "the count limit was nowhere near");
    }

    #[test]
    fn popping_frees_the_byte_budget() {
        let mut queue = Queue::new(QueueLimits {
            max_messages: 1000,
            max_bytes: delivery(QoS::AtLeastOnce, b"0123456789").size_in_queue(),
            overflow: Overflow::DropNewest,
            queue_qos0_while_offline: false,
        });
        let mut dropped = Vec::new();
        assert!(queue.push(delivery(QoS::AtLeastOnce, b"0123456789"), 0, true, &mut dropped));
        assert!(!queue.push(delivery(QoS::AtLeastOnce, b"0123456789"), 0, true, &mut dropped));

        queue.pop(0, &mut dropped).unwrap();
        assert_eq!(queue.bytes(), 0);
        assert!(queue.push(delivery(QoS::AtLeastOnce, b"0123456789"), 0, true, &mut dropped));
    }

    #[test]
    fn a_fire_and_forget_message_is_discarded_while_offline_by_default() {
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();

        assert!(!queue.push(delivery(QoS::AtMostOnce, b"x"), 0, false, &mut dropped));
        assert_eq!(dropped[0].1, DropReason::QosZeroWhileOffline);
        assert!(queue.is_empty());

        // Online, it queues like anything else.
        dropped.clear();
        assert!(queue.push(delivery(QoS::AtMostOnce, b"x"), 0, true, &mut dropped));
        assert!(dropped.is_empty());
    }

    #[test]
    fn queueing_fire_and_forget_while_offline_can_be_turned_on() {
        let mut queue = Queue::new(QueueLimits {
            queue_qos0_while_offline: true,
            ..limits(10, Overflow::DropNewest)
        });
        let mut dropped = Vec::new();
        assert!(queue.push(delivery(QoS::AtMostOnce, b"x"), 0, false, &mut dropped));
        assert!(dropped.is_empty());
    }

    #[test]
    fn an_expired_message_is_discarded_on_the_way_out() {
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();

        let mut stale = delivery(QoS::AtLeastOnce, b"stale");
        stale.message.expires_at = Some(100);
        queue.push(stale, 0, true, &mut dropped);
        queue.push(delivery(QoS::AtLeastOnce, b"fresh"), 0, true, &mut dropped);
        assert!(dropped.is_empty(), "expiry is checked on the way out");

        let got = queue.pop(101, &mut dropped).expect("the fresh one");
        assert_eq!(&got.message.payload[..], b"fresh");
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].1, DropReason::Expired);
        assert!(queue.is_empty());
    }

    #[test]
    fn an_expired_message_is_not_admitted() {
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();
        let mut stale = delivery(QoS::AtLeastOnce, b"x");
        stale.message.expires_at = Some(100);

        assert!(!queue.push(stale, 101, true, &mut dropped));
        assert_eq!(dropped[0].1, DropReason::Expired);
    }

    #[test]
    fn clearing_reports_everything_it_discards() {
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();
        queue.push(delivery(QoS::AtLeastOnce, b"1"), 0, true, &mut dropped);
        queue.push(delivery(QoS::AtLeastOnce, b"2"), 0, true, &mut dropped);

        queue.clear(DropReason::SessionEnded, &mut dropped);
        assert_eq!(dropped.len(), 2);
        assert!(dropped.iter().all(|(_, r)| *r == DropReason::SessionEnded));
        assert!(queue.is_empty());
        assert_eq!(queue.bytes(), 0);
    }

    #[test]
    fn requeueing_puts_a_message_back_at_the_head() {
        // Resume puts unacknowledged messages back before anything newer.
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();
        queue.push(delivery(QoS::AtLeastOnce, b"new"), 0, true, &mut dropped);
        queue.requeue(delivery(QoS::AtLeastOnce, b"old"));

        let first = queue.pop(0, &mut dropped).unwrap();
        assert_eq!(&first.message.payload[..], b"old");
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core queue`
Expected: FAIL to compile, with `Queue`, `Delivery`, `QueueLimits`, `Overflow`, `DropReason` not found.

- [ ] **Step 4: Write the implementation**

Insert at the top of `queue.rs`:

```rust
use std::collections::VecDeque;

use mqtt_codec::QoS;

use crate::session::message::Message;

/// A message addressed to one subscriber, with everything delivery needs.
///
/// The quality of service here is the *delivery* level, the lesser of what
/// the publisher asked for and what the subscription granted, which is why
/// it is separate from the message's own.
#[derive(Debug, Clone)]
pub struct Delivery {
    /// The message itself, shared with every other subscriber's copy.
    pub message: Message,
    /// The level to deliver at.
    pub qos: QoS,
    /// The retain flag as it should appear on the wire, which depends on
    /// the subscription's retain-as-published option.
    pub retain: bool,
    /// MQTT 5 subscription identifiers to echo back, one per matching
    /// subscription that carried one.
    pub subscription_ids: Vec<u32>,
}

impl Delivery {
    /// Roughly how much of the queue's byte budget this occupies.
    pub fn size_in_queue(&self) -> usize {
        self.message.size_in_queue()
    }
}

/// Why a message was discarded.
///
/// Every discard carries one of these, because a message that disappears
/// without being counted is a support case nobody can answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DropReason {
    /// The queue was at its message limit.
    QueueFull,
    /// The queue was at its byte limit.
    ByteBudget,
    /// Fire-and-forget delivery to a session with nobody attached.
    QosZeroWhileOffline,
    /// The message's expiry deadline had passed.
    Expired,
    /// The session was destroyed.
    SessionEnded,
}

/// What to discard when the queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Overflow {
    /// Refuse the arrival. The default, because it preserves the older
    /// messages a subscriber has been waiting longest for.
    #[default]
    DropNewest,
    /// Evict the head to make room, which suits a consumer that only
    /// cares about recent state.
    DropOldest,
}

/// Bounds on one queue.
#[derive(Debug, Clone, Copy)]
pub struct QueueLimits {
    /// How many messages may wait.
    pub max_messages: usize,
    /// How many bytes they may occupy in total.
    ///
    /// Separate from the count because either bound alone is escapable: a
    /// count permits a thousand enormous payloads, a byte total permits a
    /// million empty ones.
    pub max_bytes: usize,
    /// What to discard when either bound is reached.
    pub overflow: Overflow,
    /// Whether to hold fire-and-forget messages for a session with nobody
    /// attached. Off by default, matching the protocol's intent that such
    /// a message is worth no effort.
    pub queue_qos0_while_offline: bool,
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            max_messages: 1000,
            max_bytes: 64 * 1024 * 1024,
            overflow: Overflow::DropNewest,
            queue_qos0_while_offline: false,
        }
    }
}

/// Messages waiting for room in the inflight window.
pub struct Queue {
    entries: VecDeque<Delivery>,
    bytes: usize,
    limits: QueueLimits,
}

impl Queue {
    /// An empty queue with the given bounds.
    pub fn new(limits: QueueLimits) -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            limits,
        }
    }

    /// Offer a message to the queue.
    ///
    /// Returns whether it was accepted. Anything discarded, including the
    /// arrival itself when it is refused, is appended to `dropped` with
    /// the reason, so the caller can count it.
    pub fn push(
        &mut self,
        item: Delivery,
        now: u32,
        online: bool,
        dropped: &mut Vec<(Delivery, DropReason)>,
    ) -> bool {
        if item.message.is_expired(now) {
            dropped.push((item, DropReason::Expired));
            return false;
        }
        if item.qos == QoS::AtMostOnce && !online && !self.limits.queue_qos0_while_offline {
            dropped.push((item, DropReason::QosZeroWhileOffline));
            return false;
        }

        let size = item.size_in_queue();
        // A message larger than the whole budget can never fit, whatever
        // is evicted, so refuse it rather than emptying the queue trying.
        if size > self.limits.max_bytes {
            dropped.push((item, DropReason::ByteBudget));
            return false;
        }

        while self.entries.len() >= self.limits.max_messages
            || self.bytes + size > self.limits.max_bytes
        {
            let reason = if self.entries.len() >= self.limits.max_messages {
                DropReason::QueueFull
            } else {
                DropReason::ByteBudget
            };
            match self.limits.overflow {
                Overflow::DropNewest => {
                    dropped.push((item, reason));
                    return false;
                }
                Overflow::DropOldest => match self.entries.pop_front() {
                    Some(evicted) => {
                        self.bytes -= evicted.size_in_queue();
                        dropped.push((evicted, reason));
                    }
                    // Nothing left to evict, which the size check above
                    // should already have prevented.
                    None => {
                        dropped.push((item, reason));
                        return false;
                    }
                },
            }
        }

        self.bytes += size;
        self.entries.push_back(item);
        true
    }

    /// Put a message back at the head, ahead of anything newer.
    ///
    /// Used on resume, where a message that was already sent but not
    /// acknowledged must go out before anything that arrived since. It
    /// bypasses the bounds deliberately: the message was already accepted,
    /// and refusing it now would lose something the peer may be expecting.
    pub fn requeue(&mut self, item: Delivery) {
        self.bytes += item.size_in_queue();
        self.entries.push_front(item);
    }

    /// Take the next message, discarding any that expired while waiting.
    pub fn pop(&mut self, now: u32, dropped: &mut Vec<(Delivery, DropReason)>) -> Option<Delivery> {
        while let Some(item) = self.entries.pop_front() {
            self.bytes -= item.size_in_queue();
            if item.message.is_expired(now) {
                dropped.push((item, DropReason::Expired));
                continue;
            }
            return Some(item);
        }
        None
    }

    /// Discard everything, reporting each message with the given reason.
    pub fn clear(&mut self, reason: DropReason, dropped: &mut Vec<(Delivery, DropReason)>) {
        for item in self.entries.drain(..) {
            dropped.push((item, reason));
        }
        self.bytes = 0;
    }

    /// How many messages are waiting.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// How many bytes they occupy.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Whether nothing is waiting.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The bounds in force.
    pub fn limits(&self) -> QueueLimits {
        self.limits
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core queue`
Expected: PASS, eleven tests.

- [ ] **Step 6: Prove the byte accounting cannot drift**

The byte total is maintained by hand on four paths, which is exactly the
shape of bug that shows up months later as a queue that refuses messages
while appearing empty. Write a scratch test that performs a few thousand
random pushes, pops, requeues and clears, and after every operation
asserts that `bytes()` equals the sum of `size_in_queue()` over the
entries. Run it, delete it, confirm a clean tree, and report what you saw.
If you can, note whether the property would have caught omitting the
subtraction in `pop`.

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/broker-core/src
git commit -m "feat(session): the outbound queue with bounds and drop reasons"
```

---

## Task 4: The inflight window

What has been sent and not yet acknowledged. Small by design: the default
is twenty, and setting it to one is the documented way to get strict
ordering, so a linear scan beats a hash map here.

**Files:**
- Create: `services/broker/crates/broker-core/src/session/inflight.rs`
- Modify: `services/broker/crates/broker-core/src/session/mod.rs`

- [ ] **Step 1: Declare the module**

Add `pub mod inflight;` to `src/session/mod.rs` and re-export `Inflight`, `InflightEntry` and `InflightState`.

- [ ] **Step 2: Write the failing test**

Create `inflight.rs` with tests only:

```rust
//! Messages sent and not yet acknowledged.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::message::Message;
    use crate::session::queue::Delivery;
    use bytes::Bytes;
    use mqtt_codec::QoS;

    fn entry(packet_id: u16, qos: QoS) -> InflightEntry {
        InflightEntry {
            packet_id,
            delivery: Delivery {
                message: Message::new("a/b", Bytes::from_static(b"x"), qos, false),
                qos,
                retain: false,
                subscription_ids: Vec::new(),
            },
            state: InflightState::AwaitingAck,
            sent_at: 0,
        }
    }

    #[test]
    fn accepts_up_to_its_limit() {
        let mut inflight = Inflight::new(2);
        assert!(inflight.has_room());
        inflight.push(entry(1, QoS::AtLeastOnce));
        assert!(inflight.has_room());
        inflight.push(entry(2, QoS::AtLeastOnce));
        assert!(!inflight.has_room());
        assert_eq!(inflight.len(), 2);
    }

    #[test]
    fn a_limit_of_zero_means_unbounded() {
        // The protocol has no way to say "no messages at all", and zero is
        // the conventional spelling of "no limit".
        let mut inflight = Inflight::new(0);
        for id in 1..=100 {
            assert!(inflight.has_room());
            inflight.push(entry(id, QoS::AtLeastOnce));
        }
        assert!(inflight.has_room());
    }

    #[test]
    fn finds_and_removes_by_packet_identifier() {
        let mut inflight = Inflight::new(10);
        inflight.push(entry(7, QoS::AtLeastOnce));
        inflight.push(entry(9, QoS::ExactlyOnce));

        assert!(inflight.get_mut(7).is_some());
        assert!(inflight.get_mut(8).is_none());

        let removed = inflight.remove(7).expect("present");
        assert_eq!(removed.packet_id, 7);
        assert!(inflight.get_mut(7).is_none());
        assert_eq!(inflight.len(), 1);
    }

    #[test]
    fn removing_something_absent_reports_nothing() {
        let mut inflight = Inflight::new(10);
        assert!(inflight.remove(1).is_none());
    }

    #[test]
    fn preserves_send_order() {
        // Resume resends in the order originally sent, not in identifier
        // order, which differ once the identifier space wraps.
        let mut inflight = Inflight::new(10);
        for id in [9u16, 3, 7] {
            inflight.push(entry(id, QoS::AtLeastOnce));
        }
        let order: Vec<u16> = inflight.iter().map(|e| e.packet_id).collect();
        assert_eq!(order, vec![9, 3, 7]);
    }

    #[test]
    fn draining_empties_in_send_order() {
        let mut inflight = Inflight::new(10);
        for id in [5u16, 1, 4] {
            inflight.push(entry(id, QoS::AtLeastOnce));
        }
        let drained: Vec<u16> = inflight.drain().map(|e| e.packet_id).collect();
        assert_eq!(drained, vec![5, 1, 4]);
        assert_eq!(inflight.len(), 0);
    }

    #[test]
    fn the_limit_can_be_lowered_below_what_is_already_outstanding() {
        // A peer may advertise a smaller receive maximum on resume. The
        // window must stop accepting, not discard what is already sent.
        let mut inflight = Inflight::new(10);
        for id in 1..=5 {
            inflight.push(entry(id, QoS::AtLeastOnce));
        }
        inflight.set_limit(2);
        assert_eq!(inflight.len(), 5, "nothing outstanding may be discarded");
        assert!(!inflight.has_room());
    }

    #[test]
    fn a_state_transition_is_visible_through_the_lookup() {
        let mut inflight = Inflight::new(10);
        inflight.push(entry(1, QoS::ExactlyOnce));
        inflight.get_mut(1).unwrap().state = InflightState::AwaitingComplete;
        assert_eq!(
            inflight.get_mut(1).unwrap().state,
            InflightState::AwaitingComplete
        );
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core inflight`
Expected: FAIL to compile, with `Inflight`, `InflightEntry`, `InflightState` not found.

- [ ] **Step 4: Write the implementation**

Insert at the top of `inflight.rs`:

```rust
use crate::session::queue::Delivery;

/// Where a message has reached in its acknowledgement exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InflightState {
    /// Sent at least-once, waiting for the acknowledgement.
    AwaitingAck,
    /// Sent exactly-once, waiting for the receipt.
    AwaitingReceived,
    /// Receipt seen and release sent, waiting for completion.
    ///
    /// The distinction matters on resume: a message in this state must be
    /// resumed by resending the release, not the original publish, because
    /// the peer has already taken ownership of it.
    AwaitingComplete,
}

/// One message sent and not yet acknowledged.
#[derive(Debug, Clone)]
pub struct InflightEntry {
    /// The identifier the peer will acknowledge.
    pub packet_id: u16,
    /// What was sent.
    pub delivery: Delivery,
    /// How far the exchange has got.
    pub state: InflightState,
    /// When it was sent, in coarse seconds.
    pub sent_at: u32,
}

/// The window of messages sent and not yet acknowledged.
///
/// Backed by a vector in send order rather than a map. The window is small
/// by design: the default is twenty, and one is the documented way to get
/// strict ordering, so a linear scan is faster than hashing and keeps
/// resume order correct for free.
pub struct Inflight {
    entries: Vec<InflightEntry>,
    limit: usize,
}

impl Inflight {
    /// A window holding at most `limit` messages. Zero means unbounded.
    pub fn new(limit: usize) -> Self {
        Self {
            entries: Vec::new(),
            limit,
        }
    }

    /// Whether another message may be sent.
    pub fn has_room(&self) -> bool {
        self.limit == 0 || self.entries.len() < self.limit
    }

    /// Record a message as sent. The caller checks [`Inflight::has_room`]
    /// first; this does not enforce the limit, because resume legitimately
    /// restores a window that a lowered limit would now refuse.
    pub fn push(&mut self, entry: InflightEntry) {
        self.entries.push(entry);
    }

    /// Find an outstanding message by identifier.
    pub fn get_mut(&mut self, packet_id: u16) -> Option<&mut InflightEntry> {
        self.entries.iter_mut().find(|e| e.packet_id == packet_id)
    }

    /// Take an outstanding message out, its exchange complete.
    pub fn remove(&mut self, packet_id: u16) -> Option<InflightEntry> {
        let index = self.entries.iter().position(|e| e.packet_id == packet_id)?;
        Some(self.entries.remove(index))
    }

    /// Every outstanding message, in the order sent.
    pub fn iter(&self) -> impl Iterator<Item = &InflightEntry> {
        self.entries.iter()
    }

    /// Empty the window, yielding everything in the order sent.
    pub fn drain(&mut self) -> impl Iterator<Item = InflightEntry> + '_ {
        self.entries.drain(..)
    }

    /// Change the limit.
    ///
    /// Lowering it below what is already outstanding stops new sends but
    /// never discards, because a peer advertising a smaller receive
    /// maximum on resume has not forgotten what it already holds.
    pub fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
    }

    /// How many are outstanding.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing is outstanding.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core inflight`
Expected: PASS, eight tests.

- [ ] **Step 6: Commit**

```bash
git add services/broker/crates/broker-core/src
git commit -m "feat(session): the inflight window"
```

---
## Task 5: Delivery, broker to client

The state machine that decides what goes on the wire. This is the task
where a mistake loses or duplicates a message, so two rules from the
design spec are worth restating before the code.

**There is no retransmission on a live connection.** MQTT 5 forbids it and
a connection either delivers a packet or dies, so a timer that resends
into a working socket only creates duplicates. Redelivery happens on
resume, and only then.

**Resume is not uniform.** A message whose receipt has already arrived,
and for which a release has been sent, must be resumed by resending the
release, not the original publish. The peer has taken ownership of it, and
sending the publish again would deliver it twice.

- [ ] **Step 1: Give a delivery a duplicate marker**

A requeued message must go out with the duplicate flag set. Add to
`Delivery` in `queue.rs`:

```rust
    /// Whether this is a redelivery, set when a message is requeued on
    /// resume. It becomes the duplicate flag on the wire.
    pub dup: bool,
```

Set it in `Queue::requeue`:

```rust
    pub fn requeue(&mut self, mut item: Delivery) {
        item.dup = true;
        self.bytes += item.size_in_queue();
        self.entries.push_front(item);
    }
```

Update the existing tests' `delivery` helper and the `inflight.rs` helper
to set `dup: false`, and add an assertion to
`requeueing_puts_a_message_back_at_the_head` that the requeued message
comes back with `dup` set.

**Files:**
- Create: `services/broker/crates/broker-core/src/session/outbound.rs`
- Modify: `services/broker/crates/broker-core/src/session/mod.rs`, `queue.rs`, `inflight.rs`

- [ ] **Step 2: Define the action type**

Add to `src/session/mod.rs`, above the re-exports:

```rust
use std::sync::Arc;

use mqtt_codec::{Packet, QoS, ReasonCode};

/// Something the caller must do as a result of an input.
///
/// The session never performs these itself. Returning them rather than
/// doing them is what keeps this module free of I/O and therefore
/// testable by driving millions of generated sequences through it.
#[derive(Debug)]
pub enum Action {
    /// Write this packet to the client.
    Send(Packet),
    /// A message was discarded. Count it and fire the hook; a message that
    /// disappears without being counted is a support case nobody can
    /// answer.
    Dropped {
        /// Which message went. Carried so that a caller, and the
        /// conservation property, can account for every message offered
        /// rather than only count how many vanished.
        id: message::MessageId,
        /// Why it went.
        reason: queue::DropReason,
        /// The level it would have been delivered at.
        qos: QoS,
        /// Where it was published, for the log line.
        topic: Arc<str>,
    },
    /// The session ended ungracefully; publish this will.
    PublishWill(message::Message),
    /// Close the connection with this reason.
    Close(ReasonCode),
}
```

Add `pub mod outbound;` and re-export `Outbound`.

- [ ] **Step 3: Write the failing test**

Create `outbound.rs` with tests only:

```rust
//! Deciding what goes on the wire, and when.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::message::Message;
    use crate::session::queue::{Delivery, DropReason, QueueLimits};
    use crate::session::Action;
    use bytes::Bytes;
    use mqtt_codec::packet::Publish;
    use mqtt_codec::{Packet, QoS, ReasonCode};

    fn delivery(qos: QoS, payload: &'static [u8]) -> Delivery {
        Delivery {
            message: Message::new("a/b", Bytes::from_static(payload), qos, false),
            qos,
            retain: false,
            dup: false,
            subscription_ids: Vec::new(),
        }
    }

    fn online(inflight_limit: usize) -> Outbound {
        let mut out = Outbound::new(QueueLimits::default(), inflight_limit);
        out.set_online(true);
        out
    }

    fn sent(actions: &[Action]) -> Vec<&Publish> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send(Packet::Publish(p)) => Some(p),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn fire_and_forget_goes_out_immediately_and_is_not_tracked() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtMostOnce, b"x"), 0, &mut actions);

        assert_eq!(sent(&actions).len(), 1);
        assert_eq!(sent(&actions)[0].packet_id, None);
        assert_eq!(out.inflight_len(), 0, "nothing to acknowledge");
    }

    #[test]
    fn at_least_once_is_tracked_until_acknowledged() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);

        let packet_id = sent(&actions)[0].packet_id.expect("an identifier");
        assert_eq!(out.inflight_len(), 1);

        actions.clear();
        out.on_puback(packet_id, 0, &mut actions).expect("known");
        assert_eq!(out.inflight_len(), 0);
    }

    #[test]
    fn exactly_once_runs_the_four_packet_exchange() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.expect("an identifier");

        actions.clear();
        out.on_pubrec(packet_id, ReasonCode::Success, 0, &mut actions)
            .expect("known");
        assert!(
            matches!(actions.first(), Some(Action::Send(Packet::PubRel(_)))),
            "a receipt is answered with a release"
        );
        assert_eq!(out.inflight_len(), 1, "still outstanding until completion");

        actions.clear();
        out.on_pubcomp(packet_id, 0, &mut actions).expect("known");
        assert_eq!(out.inflight_len(), 0);
    }

    #[test]
    fn a_failing_receipt_ends_the_exchange_without_a_release() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();

        actions.clear();
        out.on_pubrec(packet_id, ReasonCode::NotAuthorized, 0, &mut actions)
            .expect("known");
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::Send(Packet::PubRel(_)))),
            "a refused message is not released"
        );
        assert_eq!(out.inflight_len(), 0);
    }

    #[test]
    fn the_window_bounds_how_many_are_in_flight() {
        let mut out = online(2);
        let mut actions = Vec::new();
        for _ in 0..5 {
            out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        }
        assert_eq!(sent(&actions).len(), 2, "only the window goes out");
        assert_eq!(out.queued_len(), 3);
    }

    #[test]
    fn an_acknowledgement_lets_the_next_one_out() {
        let mut out = online(1);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"1"), 0, &mut actions);
        out.offer(delivery(QoS::AtLeastOnce, b"2"), 0, &mut actions);
        let first = sent(&actions)[0].packet_id.unwrap();

        actions.clear();
        out.on_puback(first, 0, &mut actions).expect("known");
        assert_eq!(sent(&actions).len(), 1, "the queue drains as room appears");
        assert_eq!(out.queued_len(), 0);
    }

    #[test]
    fn nothing_is_sent_while_offline() {
        let mut out = Outbound::new(QueueLimits::default(), 10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);

        assert!(sent(&actions).is_empty());
        assert_eq!(out.queued_len(), 1, "it waits instead");
    }

    #[test]
    fn resume_resends_unacknowledged_messages_marked_as_duplicates() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        assert!(!sent(&actions)[0].dup, "the first send is not a duplicate");

        out.set_online(false);
        actions.clear();
        out.on_resume(0, &mut actions);

        let resent = sent(&actions);
        assert_eq!(resent.len(), 1);
        assert!(resent[0].dup, "a redelivery carries the duplicate flag");
    }

    #[test]
    fn resume_resends_a_release_rather_than_the_publish() {
        // Once the peer has sent a receipt it owns the message. Sending
        // the publish again would deliver it twice.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();
        out.on_pubrec(packet_id, ReasonCode::Success, 0, &mut actions)
            .expect("known");

        out.set_online(false);
        actions.clear();
        out.on_resume(0, &mut actions);

        assert!(sent(&actions).is_empty(), "the publish must not be resent");
        assert!(
            matches!(actions.first(), Some(Action::Send(Packet::PubRel(_)))),
            "the release is resent instead"
        );
    }

    #[test]
    fn resume_preserves_the_order_messages_were_sent_in() {
        let mut out = online(3);
        let mut actions = Vec::new();
        for n in 0..3u8 {
            let mut item = delivery(QoS::AtLeastOnce, b"x");
            item.message.payload = Bytes::from(vec![n]);
            out.offer(item, 0, &mut actions);
        }

        out.set_online(false);
        actions.clear();
        out.on_resume(0, &mut actions);

        let order: Vec<u8> = sent(&actions).iter().map(|p| p.payload[0]).collect();
        assert_eq!(order, vec![0, 1, 2]);
    }

    #[test]
    fn a_drop_is_reported_with_its_reason_and_topic() {
        let mut out = Outbound::new(
            QueueLimits {
                max_messages: 1,
                ..QueueLimits::default()
            },
            10,
        );
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"1"), 0, &mut actions);
        out.offer(delivery(QoS::AtLeastOnce, b"2"), 0, &mut actions);

        let dropped: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Dropped { reason, topic, .. } => Some((*reason, topic.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].0, DropReason::QueueFull);
        assert_eq!(&*dropped[0].1, "a/b");
    }

    #[test]
    fn an_acknowledgement_for_an_unknown_identifier_is_reported() {
        let mut out = online(10);
        let mut actions = Vec::new();
        assert!(out.on_puback(99, 0, &mut actions).is_err());
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core outbound`
Expected: FAIL to compile, with `Outbound` not found.

- [ ] **Step 5: Decide the unknown-identifier rule from the specification**

Before writing the implementation, settle what happens when an
acknowledgement names an identifier that is not outstanding. Read MQTT 5
sections 3.4, 3.5, 3.6 and 3.7, and the reason code tables. Answer three
questions and report them:

- What must a receiver do with an acknowledgement for an unknown
  identifier? Is it a protocol error, or is it to be ignored?
- What must a release for an unknown identifier be answered with?
- Does the answer differ between the four acknowledgement packets?

Download the OASIS HTML with `curl` and strip the tags; the Word export
splits words across tags so grep alone finds nothing. The test above
asserts an error is reported; if the specification says otherwise, change
the test and say so, because the specification wins.

- [ ] **Step 6: Write the implementation**

Insert at the top of `outbound.rs`:

```rust
use std::sync::Arc;

use mqtt_codec::packet::{Ack, Publish};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode};

use crate::session::inflight::{Inflight, InflightEntry, InflightState};
use crate::session::packet_ids::PacketIds;
use crate::session::queue::{Delivery, DropReason, Queue, QueueLimits};
use crate::session::Action;

/// Decides what goes on the wire to one client, and when.
///
/// There is deliberately no retransmission timer. MQTT 5 forbids resending
/// on a live connection, and a connection either delivers a packet or
/// dies, so a timer would only manufacture duplicates. Redelivery happens
/// on resume and nowhere else.
pub struct Outbound {
    queue: Queue,
    inflight: Inflight,
    packet_ids: PacketIds,
    online: bool,
    /// Reused across calls so the hot path allocates nothing.
    scratch: Vec<(Delivery, DropReason)>,
}

impl Outbound {
    /// A new outbound path with the given queue bounds and window size.
    pub fn new(limits: QueueLimits, inflight_limit: usize) -> Self {
        Self {
            queue: Queue::new(limits),
            inflight: Inflight::new(inflight_limit),
            packet_ids: PacketIds::default(),
            online: false,
            scratch: Vec::new(),
        }
    }

    /// Whether a connection is attached.
    pub fn set_online(&mut self, online: bool) {
        self.online = online;
    }

    /// Narrow the window to what the peer said it can receive.
    pub fn set_inflight_limit(&mut self, limit: usize) {
        self.inflight.set_limit(limit);
    }

    /// Offer a message for delivery.
    pub fn offer(&mut self, item: Delivery, now: u32, out: &mut Vec<Action>) {
        self.scratch.clear();
        self.queue.push(item, now, self.online, &mut self.scratch);
        Self::report(&mut self.scratch, out);
        self.pump(now, out);
    }

    /// Send whatever the window has room for.
    fn pump(&mut self, now: u32, out: &mut Vec<Action>) {
        while self.online && self.inflight.has_room() {
            self.scratch.clear();
            let next = self.queue.pop(now, &mut self.scratch);
            Self::report(&mut self.scratch, out);
            let Some(item) = next else { break };
            if !self.dispatch(item, now, out) {
                break;
            }
        }
    }

    /// Put one message on the wire. Returns whether to keep going.
    fn dispatch(&mut self, item: Delivery, now: u32, out: &mut Vec<Action>) -> bool {
        if item.qos == QoS::AtMostOnce {
            out.push(Action::Send(publish(&item, None)));
            return true;
        }

        let Some(packet_id) = self.packet_ids.allocate() else {
            // Every identifier is outstanding. Put it back and wait for an
            // acknowledgement to free one; this is a stall, not a loss.
            self.queue.requeue(item);
            return false;
        };

        out.push(Action::Send(publish(&item, Some(packet_id))));
        let state = if item.qos == QoS::AtLeastOnce {
            InflightState::AwaitingAck
        } else {
            InflightState::AwaitingReceived
        };
        self.inflight.push(InflightEntry {
            packet_id,
            delivery: item,
            state,
            sent_at: now,
        });
        true
    }

    /// Handle an at-least-once acknowledgement.
    pub fn on_puback(
        &mut self,
        packet_id: u16,
        now: u32,
        out: &mut Vec<Action>,
    ) -> Result<(), ReasonCode> {
        self.complete(packet_id)?;
        self.pump(now, out);
        Ok(())
    }

    /// Handle an exactly-once receipt.
    ///
    /// A failure reason ends the exchange: the peer has refused the
    /// message, so there is nothing to release.
    pub fn on_pubrec(
        &mut self,
        packet_id: u16,
        reason: ReasonCode,
        now: u32,
        out: &mut Vec<Action>,
    ) -> Result<(), ReasonCode> {
        if reason.is_failure() {
            self.complete(packet_id)?;
            self.pump(now, out);
            return Ok(());
        }

        let entry = self
            .inflight
            .get_mut(packet_id)
            .ok_or(ReasonCode::PacketIdentifierNotFound)?;
        entry.state = InflightState::AwaitingComplete;
        out.push(Action::Send(Packet::PubRel(Ack {
            packet_id,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        })));
        Ok(())
    }

    /// Handle an exactly-once completion.
    pub fn on_pubcomp(
        &mut self,
        packet_id: u16,
        now: u32,
        out: &mut Vec<Action>,
    ) -> Result<(), ReasonCode> {
        self.complete(packet_id)?;
        self.pump(now, out);
        Ok(())
    }

    fn complete(&mut self, packet_id: u16) -> Result<(), ReasonCode> {
        match self.inflight.remove(packet_id) {
            Some(_) => {
                self.packet_ids.release(packet_id);
                Ok(())
            }
            None => Err(ReasonCode::PacketIdentifierNotFound),
        }
    }

    /// The connection went away. Nothing is discarded; the window is held
    /// until the session resumes or expires.
    pub fn on_disconnect(&mut self) {
        self.online = false;
    }

    /// A connection attached again.
    ///
    /// Anything whose receipt already arrived stays outstanding and has
    /// its release resent, because the peer owns it. Everything else goes
    /// back to the head of the queue, marked as a redelivery.
    pub fn on_resume(&mut self, now: u32, out: &mut Vec<Action>) {
        self.online = true;

        let drained: Vec<InflightEntry> = self.inflight.drain().collect();
        let (owned_by_peer, to_resend): (Vec<_>, Vec<_>) = drained
            .into_iter()
            .partition(|e| e.state == InflightState::AwaitingComplete);

        for entry in owned_by_peer {
            out.push(Action::Send(Packet::PubRel(Ack {
                packet_id: entry.packet_id,
                reason: ReasonCode::Success,
                properties: Properties::default(),
            })));
            self.inflight.push(entry);
        }

        // Reversed, because each requeue goes to the head, so the last one
        // pushed ends up first.
        for entry in to_resend.into_iter().rev() {
            self.packet_ids.release(entry.packet_id);
            self.queue.requeue(entry.delivery);
        }

        self.pump(now, out);
    }

    /// Discard everything, reporting each message.
    pub fn clear(&mut self, reason: DropReason, out: &mut Vec<Action>) {
        self.scratch.clear();
        self.queue.clear(reason, &mut self.scratch);
        Self::report(&mut self.scratch, out);
        for entry in self.inflight.drain() {
            out.push(Action::Dropped {
                id: entry.delivery.message.id,
                reason,
                qos: entry.delivery.qos,
                topic: Arc::clone(&entry.delivery.message.topic),
            });
        }
    }

    /// How many are outstanding.
    pub fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    /// How many are waiting.
    pub fn queued_len(&self) -> usize {
        self.queue.len()
    }

    fn report(scratch: &mut Vec<(Delivery, DropReason)>, out: &mut Vec<Action>) {
        for (item, reason) in scratch.drain(..) {
            out.push(Action::Dropped {
                id: item.message.id,
                reason,
                qos: item.qos,
                topic: Arc::clone(&item.message.topic),
            });
        }
    }
}

fn publish(item: &Delivery, packet_id: Option<u16>) -> Packet {
    // The property block is cloned per delivery because the subscription
    // identifiers differ per subscriber. That clone is the packet-sizing
    // question recorded in the design spec's open questions.
    let mut properties = (*item.message.properties).clone();
    properties.subscription_identifiers = item.subscription_ids.clone();

    Packet::Publish(Publish {
        dup: item.dup,
        qos: item.qos,
        retain: item.retain,
        topic: item.message.topic.to_string(),
        packet_id,
        payload: item.message.payload.clone(),
        properties,
    })
}
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core outbound`
Expected: PASS, twelve tests.

- [ ] **Step 8: Prove the resume rule is load-bearing**

Two mutations, each restored from a `cp` backup, each reported.

1. Make `on_resume` requeue everything, including messages awaiting
   completion. Confirm `resume_resends_a_release_rather_than_the_publish`
   fails. This is the mutation that would deliver a message twice.
2. Remove the `.rev()` from the requeue loop. Confirm
   `resume_preserves_the_order_messages_were_sent_in` fails.

- [ ] **Step 9: Commit**

```bash
git add services/broker/crates/broker-core/src
git commit -m "feat(session): the delivery state machine"
```

---
## Task 6: Receiving, client to broker

The other direction. Shorter, because the broker acknowledges rather than
tracks, with one exception: exactly-once delivery has to remember which
identifiers it has seen so a redelivery is not dispatched twice.

**Files:**
- Create: `services/broker/crates/broker-core/src/session/inbound.rs`
- Modify: `services/broker/crates/broker-core/src/session/mod.rs`

- [ ] **Step 1: Settle three rules from the specification**

Before writing anything, read MQTT 5 sections 3.3, 3.4, 3.5, 3.6 and
4.9, and answer these. Report the answers with the clause you relied on,
and if any contradicts what this task assumes, the specification wins.

- When a release names an identifier the receiver is not holding, what
  must the completion carry?
- Does the receive maximum count unacknowledged at-least-once messages,
  exactly-once messages, or both? What must happen when a peer exceeds it?
- May a receiver dispatch an exactly-once message when the publish
  arrives, or must it wait for the release? If both are permitted, say so.

This task assumes dispatch happens on the publish, with the identifier
remembered until the release so a redelivery is suppressed. That uses less
memory than holding the message, and still gives the subscriber exactly
one copy. If the specification forbids it, stop and tell me.

- [ ] **Step 2: Declare the module**

Add `pub mod inbound;` to `src/session/mod.rs` and re-export `Inbound`.

- [ ] **Step 3: Write the failing test**

Create `inbound.rs` with tests only:

```rust
//! Receiving messages from a client.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::Action;
    use bytes::Bytes;
    use mqtt_codec::packet::Publish;
    use mqtt_codec::{Packet, Properties, QoS, ReasonCode};

    fn publish(qos: QoS, packet_id: Option<u16>, dup: bool) -> Publish {
        Publish {
            dup,
            qos,
            retain: false,
            topic: "a/b".into(),
            packet_id,
            payload: Bytes::from_static(b"x"),
            properties: Properties::default(),
        }
    }

    fn acks(actions: &[Action]) -> Vec<&Packet> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send(p) => Some(p),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn fire_and_forget_is_dispatched_and_not_acknowledged() {
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();
        assert!(inbound
            .on_publish(&publish(QoS::AtMostOnce, None, false), &mut actions)
            .unwrap());
        assert!(acks(&actions).is_empty());
    }

    #[test]
    fn at_least_once_is_dispatched_and_acknowledged() {
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();
        assert!(inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(5), false), &mut actions)
            .unwrap());
        assert!(matches!(acks(&actions)[0], Packet::PubAck(a) if a.packet_id == 5));
    }

    #[test]
    fn at_least_once_is_dispatched_again_on_a_redelivery() {
        // At-least-once means exactly that: the broker does not dedupe,
        // because the publisher is entitled to a second attempt.
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();
        assert!(inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(5), false), &mut actions)
            .unwrap());
        assert!(inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(5), true), &mut actions)
            .unwrap());
    }

    #[test]
    fn exactly_once_is_dispatched_once_and_receipted_twice() {
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();

        assert!(inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(7), false), &mut actions)
            .unwrap());
        assert_eq!(inbound.outstanding(), 1);

        actions.clear();
        assert!(
            !inbound
                .on_publish(&publish(QoS::ExactlyOnce, Some(7), true), &mut actions)
                .unwrap(),
            "a redelivery must not reach the subscriber twice"
        );
        assert!(
            matches!(acks(&actions)[0], Packet::PubRec(a) if a.packet_id == 7),
            "but it is receipted again, because the first receipt may have been lost"
        );
    }

    #[test]
    fn a_release_completes_the_exchange_and_forgets_the_identifier() {
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();
        inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(7), false), &mut actions)
            .unwrap();

        actions.clear();
        inbound.on_pubrel(7, &mut actions);
        assert!(matches!(
            acks(&actions)[0],
            Packet::PubComp(a) if a.packet_id == 7 && a.reason == ReasonCode::Success
        ));
        assert_eq!(inbound.outstanding(), 0);

        // The identifier is free again, so the same one may be reused.
        actions.clear();
        assert!(inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(7), false), &mut actions)
            .unwrap());
    }

    #[test]
    fn a_release_for_an_identifier_never_seen_is_answered_not_ignored() {
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();
        inbound.on_pubrel(99, &mut actions);
        assert!(matches!(
            acks(&actions)[0],
            Packet::PubComp(a) if a.reason == ReasonCode::PacketIdentifierNotFound
        ));
    }

    #[test]
    fn exceeding_the_receive_maximum_is_refused() {
        let mut inbound = Inbound::new(2);
        let mut actions = Vec::new();
        inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(1), false), &mut actions)
            .unwrap();
        inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(2), false), &mut actions)
            .unwrap();

        assert_eq!(
            inbound.on_publish(&publish(QoS::ExactlyOnce, Some(3), false), &mut actions),
            Err(ReasonCode::ReceiveMaximumExceeded)
        );
    }

    #[test]
    fn a_redelivery_does_not_count_against_the_receive_maximum() {
        // It is already counted; refusing it would deadlock the exchange.
        let mut inbound = Inbound::new(1);
        let mut actions = Vec::new();
        inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(1), false), &mut actions)
            .unwrap();
        assert!(inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(1), true), &mut actions)
            .is_ok());
    }

    #[test]
    fn held_identifiers_survive_a_resume() {
        // Exactly-once state outlives the connection; discarding it would
        // let a redelivery through as a fresh message.
        let mut inbound = Inbound::new(10);
        inbound.reserve(7);
        let mut actions = Vec::new();
        assert!(!inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(7), true), &mut actions)
            .unwrap());
    }
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core inbound`
Expected: FAIL to compile, with `Inbound` not found.

- [ ] **Step 5: Write the implementation**

Insert at the top of `inbound.rs`:

```rust
use std::collections::HashSet;

use foldhash::fast::RandomState;
use mqtt_codec::packet::{Ack, Publish};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode};

use crate::session::Action;

/// Tracks what a client has sent that is not yet complete.
///
/// Only exactly-once delivery needs memory here. At-least-once is
/// acknowledged and forgotten, because the publisher is entitled to send
/// again and the broker is not promising to notice.
pub struct Inbound {
    /// Identifiers whose publish has arrived and whose release has not.
    awaiting_release: HashSet<u16, RandomState>,
    receive_maximum: usize,
}

impl Inbound {
    /// A receiver permitting `receive_maximum` concurrent exactly-once
    /// exchanges. Zero means unbounded.
    pub fn new(receive_maximum: usize) -> Self {
        Self {
            awaiting_release: HashSet::with_hasher(RandomState::default()),
            receive_maximum,
        }
    }

    /// Handle a publish from the client.
    ///
    /// Returns whether to dispatch the message to subscribers. A
    /// redelivery of an exactly-once message already held returns false:
    /// it is receipted again, because the first receipt may have been
    /// lost, but it must not reach a subscriber twice.
    pub fn on_publish(
        &mut self,
        publish: &Publish,
        out: &mut Vec<Action>,
    ) -> Result<bool, ReasonCode> {
        match publish.qos {
            QoS::AtMostOnce => Ok(true),
            QoS::AtLeastOnce => {
                let packet_id = publish
                    .packet_id
                    .ok_or(ReasonCode::ProtocolError)?;
                out.push(Action::Send(Packet::PubAck(ack(packet_id, ReasonCode::Success))));
                Ok(true)
            }
            QoS::ExactlyOnce => {
                let packet_id = publish
                    .packet_id
                    .ok_or(ReasonCode::ProtocolError)?;

                if self.awaiting_release.contains(&packet_id) {
                    // Already held. Receipt it again and stop; it is
                    // already counted against the receive maximum, so
                    // refusing it here would deadlock the exchange.
                    out.push(Action::Send(Packet::PubRec(ack(
                        packet_id,
                        ReasonCode::Success,
                    ))));
                    return Ok(false);
                }

                if self.receive_maximum != 0
                    && self.awaiting_release.len() >= self.receive_maximum
                {
                    return Err(ReasonCode::ReceiveMaximumExceeded);
                }

                self.awaiting_release.insert(packet_id);
                out.push(Action::Send(Packet::PubRec(ack(
                    packet_id,
                    ReasonCode::Success,
                ))));
                Ok(true)
            }
        }
    }

    /// Handle a release, completing an exactly-once exchange.
    ///
    /// An identifier the receiver is not holding is answered rather than
    /// ignored, so a peer that lost the completion can make progress.
    pub fn on_pubrel(&mut self, packet_id: u16, out: &mut Vec<Action>) {
        let reason = if self.awaiting_release.remove(&packet_id) {
            ReasonCode::Success
        } else {
            ReasonCode::PacketIdentifierNotFound
        };
        out.push(Action::Send(Packet::PubComp(ack(packet_id, reason))));
    }

    /// Claim an identifier for state restored on resume.
    pub fn reserve(&mut self, packet_id: u16) {
        self.awaiting_release.insert(packet_id);
    }

    /// Every identifier currently held, for persistence.
    pub fn held(&self) -> impl Iterator<Item = u16> + '_ {
        self.awaiting_release.iter().copied()
    }

    /// How many exchanges are incomplete.
    pub fn outstanding(&self) -> usize {
        self.awaiting_release.len()
    }

    /// Forget everything, for a session that is ending.
    pub fn clear(&mut self) {
        self.awaiting_release.clear();
    }
}

fn ack(packet_id: u16, reason: ReasonCode) -> Ack {
    Ack {
        packet_id,
        reason,
        properties: Properties::default(),
    }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core inbound`
Expected: PASS, nine tests.

- [ ] **Step 7: Prove the deduplication is load-bearing**

Make `on_publish` always return true for exactly-once messages, so a
redelivery dispatches again. Confirm
`exactly_once_is_dispatched_once_and_receipted_twice` fails, then restore
from a `cp` backup. That mutation is the one that delivers a message
twice to every subscriber.

- [ ] **Step 8: Commit**

```bash
git add services/broker/crates/broker-core/src
git commit -m "feat(session): the receive state machine"
```

---

## Task 7: Conservation properties

The point of the whole plan. Unit tests prove the cases we thought of;
this proves the promise the protocol actually makes.

**Files:**
- Create: `services/broker/crates/broker-core/tests/conservation.rs`
- Modify: `services/broker/crates/broker-core/src/session/outbound.rs`

- [ ] **Step 1: Expose what the invariant needs**

The conservation law is that every message offered is accounted for: sent,
dropped, or still held. Counting the held ones needs to see inside, so add
to `Outbound`, with a doc comment saying it exists so the invariant can be
asserted rather than assumed, the same reasoning `RetainedStore::node_count`
and `SharedSubscriptions::synthetic_count` already carry:

```rust
    /// Every message still held, queued or outstanding.
    ///
    /// Exposed so the conservation property can assert that nothing is
    /// lost rather than assume it.
    pub fn pending_ids(&self) -> impl Iterator<Item = crate::session::MessageId> + '_ {
        self.queue
            .iter()
            .map(|d| d.message.id)
            .chain(self.inflight.iter().map(|e| e.delivery.message.id))
    }
```

This needs `Queue::iter`, so add that too:

```rust
    /// Every message waiting, in order.
    pub fn iter(&self) -> impl Iterator<Item = &Delivery> {
        self.entries.iter()
    }
```

- [ ] **Step 2: Write the properties**

Create `services/broker/crates/broker-core/tests/conservation.rs`:

```rust
//! What the protocol actually promises, asserted over generated histories.
//!
//! A unit test proves the case its author thought of. These properties
//! drive random sequences of offers, acknowledgements, disconnects and
//! resumes through the delivery state machine and assert the guarantees
//! themselves: nothing offered vanishes, nothing is delivered fresh twice,
//! and an identifier in use is never handed out again.

use broker_core::session::message::Message;
use broker_core::session::queue::{Delivery, Overflow, QueueLimits};
use broker_core::session::{Action, MessageId, Outbound};
use bytes::Bytes;
use mqtt_codec::{Packet, QoS, ReasonCode};
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
enum Op {
    Offer(QoS),
    Ack,
    Receipt(bool),
    Complete,
    Disconnect,
    Resume,
}

fn any_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => prop_oneof![
            Just(QoS::AtMostOnce),
            Just(QoS::AtLeastOnce),
            Just(QoS::ExactlyOnce),
        ].prop_map(Op::Offer),
        4 => Just(Op::Ack),
        3 => prop::bool::ANY.prop_map(Op::Receipt),
        3 => Just(Op::Complete),
        1 => Just(Op::Disconnect),
        2 => Just(Op::Resume),
    ]
}

/// Drives the state machine and records what it observed.
struct Run {
    out: Outbound,
    actions: Vec<Action>,
    /// Every message offered, keyed by the unique tag in its payload.
    offered: HashMap<u32, MessageId>,
    /// Tags sent without the duplicate flag, and how often.
    fresh: HashMap<u32, usize>,
    /// Messages the state machine reported discarding.
    dropped: HashSet<MessageId>,
    /// Packet identifiers outstanding right now, in send order.
    outstanding: Vec<u16>,
    /// The same set, used to notice one handed out while already in use.
    live: HashSet<u16>,
    /// Set if an identifier was reused while outstanding.
    reused: bool,
    next_tag: u32,
}

impl Run {
    fn new(limits: QueueLimits, window: usize) -> Self {
        let mut out = Outbound::new(limits, window);
        out.set_online(true);
        Self {
            out,
            actions: Vec::new(),
            offered: HashMap::new(),
            fresh: HashMap::new(),
            dropped: HashSet::new(),
            outstanding: Vec::new(),
            live: HashSet::new(),
            reused: false,
            next_tag: 0,
        }
    }

    /// A unique tag per offer, so a payload identifies exactly one message.
    fn offer(&mut self, qos: QoS) {
        let tag = self.next_tag;
        self.next_tag += 1;

        let message = Message::new(
            "t",
            Bytes::from(tag.to_be_bytes().to_vec()),
            qos,
            false,
        );
        self.offered.insert(tag, message.id);

        self.actions.clear();
        self.out.offer(
            Delivery {
                message,
                qos,
                retain: false,
                dup: false,
                subscription_ids: Vec::new(),
            },
            0,
            &mut self.actions,
        );
        self.absorb();
    }

    fn step(&mut self, op: &Op) {
        match op {
            Op::Offer(qos) => {
                self.offer(*qos);
                return;
            }
            Op::Ack => {
                let Some(id) = self.outstanding.first().copied() else {
                    return;
                };
                self.actions.clear();
                if self.out.on_puback(id, 0, &mut self.actions).is_ok() {
                    self.release(id);
                }
            }
            Op::Receipt(accepted) => {
                let Some(id) = self.outstanding.first().copied() else {
                    return;
                };
                let reason = if *accepted {
                    ReasonCode::Success
                } else {
                    ReasonCode::NotAuthorized
                };
                self.actions.clear();
                if self.out.on_pubrec(id, reason, 0, &mut self.actions).is_ok() && !*accepted {
                    // A refused message ends the exchange here; an accepted
                    // one stays outstanding until it completes.
                    self.release(id);
                }
            }
            Op::Complete => {
                let Some(id) = self.outstanding.first().copied() else {
                    return;
                };
                self.actions.clear();
                if self.out.on_pubcomp(id, 0, &mut self.actions).is_ok() {
                    self.release(id);
                }
            }
            Op::Disconnect => {
                self.out.on_disconnect();
                self.actions.clear();
            }
            Op::Resume => {
                // Resume releases the identifiers of everything it requeues
                // and allocates fresh ones on the way out, so reallocation
                // across a resume is correct rather than a fault. The
                // property therefore watches for reuse within a connection,
                // which is where it would be a bug.
                self.outstanding.clear();
                self.live.clear();
                self.actions.clear();
                self.out.on_resume(0, &mut self.actions);
            }
        }
        self.absorb();
    }

    fn release(&mut self, id: u16) {
        self.live.remove(&id);
        self.outstanding.retain(|x| *x != id);
    }

    fn absorb(&mut self) {
        for action in std::mem::take(&mut self.actions) {
            match action {
                Action::Send(Packet::Publish(publish)) => {
                    let tag = u32::from_be_bytes(
                        publish.payload[..4].try_into().expect("a four byte tag"),
                    );
                    if !publish.dup {
                        *self.fresh.entry(tag).or_default() += 1;
                    }
                    if let Some(id) = publish.packet_id {
                        if !self.live.insert(id) {
                            self.reused = true;
                        }
                        self.outstanding.push(id);
                    }
                }
                Action::Dropped { id, .. } => {
                    self.dropped.insert(id);
                }
                _ => {}
            }
        }
    }
}

fn drive(ops: &[Op], limits: QueueLimits, window: usize) -> Run {
    let mut run = Run::new(limits, window);
    for op in ops {
        run.step(op);
    }
    run
}

proptest! {
    /// An identifier already outstanding is never handed out again. Doing
    /// so makes the peer acknowledge the wrong message, which is a silent
    /// delivery fault rather than an error.
    #[test]
    fn a_packet_identifier_in_use_is_never_reallocated(
        ops in prop::collection::vec(any_op(), 1..80),
        window in 1usize..8,
    ) {
        let run = drive(&ops, QueueLimits::default(), window);
        prop_assert!(!run.reused);
    }

    /// No message goes out fresh more than once. A second copy must carry
    /// the duplicate flag, which is how a subscriber tells a redelivery
    /// from a new message.
    #[test]
    fn nothing_is_delivered_fresh_twice(
        ops in prop::collection::vec(any_op(), 1..80),
        window in 1usize..8,
    ) {
        let run = drive(&ops, QueueLimits::default(), window);
        for (tag, count) in &run.fresh {
            prop_assert!(*count <= 1, "message {} was sent fresh {} times", tag, count);
        }
    }

    /// Every message offered is accounted for: it was sent, it was
    /// reported as discarded, or it is still held. None may simply vanish.
    ///
    /// The bounds are tight on purpose, so that the discard paths are
    /// exercised rather than merely present.
    #[test]
    fn nothing_offered_is_lost(
        ops in prop::collection::vec(any_op(), 1..60),
        window in 1usize..4,
        overflow in prop_oneof![Just(Overflow::DropNewest), Just(Overflow::DropOldest)],
    ) {
        let limits = QueueLimits {
            max_messages: 4,
            overflow,
            ..QueueLimits::default()
        };
        let run = drive(&ops, limits, window);

        let held: HashSet<MessageId> = run.out.pending_ids().collect();
        let sent: HashSet<MessageId> = run
            .fresh
            .keys()
            .filter_map(|tag| run.offered.get(tag).copied())
            .collect();

        for (tag, id) in &run.offered {
            prop_assert!(
                sent.contains(id) || run.dropped.contains(id) || held.contains(id),
                "message {} was neither sent, dropped, nor held",
                tag
            );
        }
    }

    /// A run that stays connected and acknowledges everything ends with
    /// nothing outstanding. A window that never drains is a stall, which
    /// looks exactly like a working broker until a client stops receiving.
    #[test]
    fn acknowledging_everything_empties_the_window(
        count in 1usize..20,
        window in 1usize..5,
    ) {
        let mut run = Run::new(QueueLimits::default(), window);
        for _ in 0..count {
            run.offer(QoS::AtLeastOnce);
        }

        // Acknowledge the oldest repeatedly until nothing is outstanding.
        for _ in 0..(count * 4) {
            if run.outstanding.is_empty() {
                break;
            }
            run.step(&Op::Ack);
        }

        prop_assert_eq!(run.out.inflight_len(), 0);
        prop_assert_eq!(run.out.queued_len(), 0);
    }
}
```

- [ ] **Step 3: Run the properties**

Run: `cd services/broker && cargo test -p broker-core --test conservation`
Expected: PASS, four properties.

**If a property fails, that is a real defect in the state machine**, not a
bad test, unless you can show the property asserts something the protocol
does not promise. Report the shrunk counterexample and your analysis
before changing anything.

The third property as written is weak: it only checks that something was
accounted for. **Strengthen it** so it tracks each offered message by
identifier through to a send, a drop or a held slot, and asserts the sets
partition exactly. That requires the drop action to carry enough to
identify the message; if it does not, say so and propose what to add
rather than leaving the property weak.

- [ ] **Step 4: Prove the properties have teeth**

Three mutations, each restored from a `cp` backup, each reported.

1. Make `complete` release the identifier without removing the entry.
   Confirm a property fails.
2. Make `on_resume` clear the duplicate flag. Confirm
   `nothing_is_delivered_fresh_twice` fails.
3. Make `PacketIds::allocate` return `Some(1)` unconditionally. Confirm
   `a_packet_identifier_in_use_is_never_reallocated` fails. Note that
   merely skipping the occupancy check would *not* fail: the counter is
   still monotonic, so no identifier repeats inside sixty-five thousand
   allocations, which no property-sized run reaches. The wrap case is
   covered by the unit test in task 2 instead. This is the same lesson as
   the routing core's unreachable bare wildcard: ask what a generator
   structurally cannot produce.

If any mutation does not fail, the generators are not reaching that path.
Say so and widen them rather than moving on. A generator that structurally
cannot produce a case leaves that code untested while looking covered,
which happened on the routing core and cost a security guard its coverage.

- [ ] **Step 5: Commit**

```bash
git add services/broker/crates/broker-core
git commit -m "test(session): conservation properties over generated histories"
```

---

## Amendments after review

Recorded so the plan matches the code, and so the reasoning survives.

**A resumed publish keeps its original packet identifier.** This plan said
to release the identifier and requeue, which meant a redelivery went out
under a new one. Section 4.4 requires the original. A peer that received
the first copy and lost its acknowledgement waits on the old identifier,
sees an unrelated-looking message, and delivers the payload twice; the
stranded identifier then causes a genuinely new message to be discarded as
a duplicate once the counter wraps. One mistake, both duplication and
loss, triggered by an ordinary dropped connection. The design spec was the
root cause and has been corrected too.

**Acknowledgements are validated against the entry they name.** Matching
on the identifier alone let a completion settle a message that had never
been receipted, freeing the slot and counting it delivered with no drop
reported.

**Three mutations survived the whole suite** and now do not: deleting the
identifier release on completion, which stalls a session permanently after
sixty-five thousand messages; and inverting the resume partition, which
resends a release for a message the peer never received, so the peer
answers not-found and the broker counts it delivered. Silent loss,
invisible to a hundred tests and fifty thousand generated histories.

**Separating the acknowledgement from the dispatch decision** made a
failing receipt reachable for the first time, and with it the rule that a
later publish under that identifier is a new message. It also made the
receive maximum's accounting load-bearing: counting only exactly-once
exchanges was equivalent while every publish was answered in the same
call, and is not once a caller can defer.

**A known limitation of the property layer.** The conservation harness
treats any acknowledgement the state machine accepts as a legitimate
ending, so a bug in that acceptance is mirrored by the harness rather than
caught by it. Three unit tests cover the specific case. This is the same
circularity that let a transposed property identifier and a swapped flag
bit pass in the codec, and an unreachable wildcard pass in the routing
core, so it is worth revisiting when the shell adds state. Note also that
the three unit tests covering it are all on the send side; the receive
side's dispatch decision has the same circularity in the end-to-end loop
and is covered by one mutation rather than by an independent assertion.

## Done when

- `cargo test -p broker-core` passes, and the workspace suite still does.
- `cargo clippy --all-targets -- -D warnings` is clean and
  `cargo fmt --all --check` shows no diff.
- `cargo deny check` passes with the allow list unchanged.
- Every mutation listed in tasks 2, 3, 5, 6 and 7 produced the failure it
  should have, and the tree is clean afterwards.
- The three specification questions in task 6 step 1 and the one in task 5
  step 5 are answered with clauses, not assumptions.

## What this plan deliberately leaves out

- **The session itself.** Connect, disconnect, wills with their MQTT 5
  delay, and session expiry assemble these pieces and belong with the
  async shell, where a clock exists.
- **Retained delivery on subscribe** and its three retain-handling
  options, which need the retained store and the subscription options
  together.
- **Anything async.** No actor, no channel, no task. That is the next plan
  and it is deliberately thin, because everything hard is here.
- **Persistence.** These structures are shaped so a storage trait can
  restore them, which is why the inbound receiver can enumerate what it
  holds and the queue preserves order, but nothing writes to disk yet.

## Obligations the shell inherits

Three things this layer cannot enforce, which the next plan must.

**An acknowledgement owed must be paid.** Receiving a publish reserves a
receive-maximum slot that only the acknowledgement frees. If the shell
drops the result on an error path, or a task is cancelled between the two
calls, that slot never returns and the client is throttled to death with
nothing reporting it. This is the same shape as the resume defect found in
review, one level up: a split moved an obligation to the caller and
nothing enforces it. A guard that acknowledges when dropped is the usual
answer, though it sits awkwardly with a module that performs no I/O.

**Requeueing bypasses the queue's bounds.** That is deliberate, since a
message already accepted should not be refused on the way back in, but it
means a session resumed repeatedly can exceed its byte budget, and a
requeued message now also pins a packet identifier. The shell should bound
how often a session may resume, or the budget is advisory.

**A receipt for an unknown identifier is answered but not counted.**
Answering is correct, but a peer can make the broker emit packets
indefinitely for identifiers it never held while no counter moves. The
shell wants a metric there before it wants anything else.

## Next plan

The server: the session actor wrapping these state machines, the client
registry, takeover, the keep-alive ring and coarse clock, wills and
session expiry, retained delivery on subscribe, the TCP listener, and the
conformance suite that is milestone one's exit criterion.
