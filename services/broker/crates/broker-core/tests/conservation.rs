//! What the protocol actually promises, asserted over generated histories.
//!
//! A unit test proves the case its author thought of. These properties
//! drive random sequences of offers, acknowledgements, disconnects and
//! resumes through the delivery state machine and assert the guarantees
//! themselves: nothing offered vanishes, nothing is delivered fresh twice,
//! and an identifier in use is never handed out again.

use broker_core::session::message::Message;
use broker_core::session::queue::{Delivery, Overflow, QueueLimits};
use broker_core::session::{Action, Inbound, MessageId, Outbound};
use bytes::Bytes;
use mqtt_codec::packet::Publish;
use mqtt_codec::{Packet, QoS, ReasonCode};
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
enum Op {
    /// Offer a message, optionally with a deadline this many seconds out.
    Offer(QoS, Option<u32>),
    /// Acknowledge an outstanding entry, chosen by position rather than
    /// always the oldest, so removal from the middle of the window is
    /// reached.
    Ack(usize),
    Receipt(usize, bool),
    Complete(usize),
    Disconnect,
    Resume,
    /// Move the clock on, so deadlines can pass.
    Tick(u32),
}

fn any_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (
            prop_oneof![
                Just(QoS::AtMostOnce),
                Just(QoS::AtLeastOnce),
                Just(QoS::ExactlyOnce),
            ],
            // Mostly no deadline, and when there is one it is short
            // enough that the generated clock reaches it.
            prop_oneof![3 => Just(None), 2 => (0u32..4).prop_map(Some)],
        ).prop_map(|(qos, ttl)| Op::Offer(qos, ttl)),
        4 => (0usize..6).prop_map(Op::Ack),
        3 => (0usize..6, prop::bool::ANY).prop_map(|(i, ok)| Op::Receipt(i, ok)),
        3 => (0usize..6).prop_map(Op::Complete),
        1 => Just(Op::Disconnect),
        2 => Just(Op::Resume),
        2 => (1u32..3).prop_map(Op::Tick),
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
    /// Messages whose exchange is over: dispatched fire-and-forget, or
    /// removed from the window by an acknowledgement.
    done: HashSet<MessageId>,
    /// Which message each outstanding identifier is currently carrying,
    /// so an acknowledgement can be attributed to the message it ends.
    carrying: HashMap<u16, u32>,
    /// Packet identifiers outstanding right now, in send order.
    outstanding: Vec<u16>,
    /// The same set, used to notice one handed out while already in use.
    live: HashSet<u16>,
    /// Set if an identifier was reused while outstanding.
    reused: bool,
    /// The coarse clock the state machine is driven with.
    now: u32,
    /// The identifier each message was last sent under.
    last_id: HashMap<u32, u16>,
    /// Set if a redelivery went out under a different identifier from
    /// the send it repeats.
    renumbered: bool,
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
            done: HashSet::new(),
            carrying: HashMap::new(),
            outstanding: Vec::new(),
            live: HashSet::new(),
            reused: false,
            now: 0,
            last_id: HashMap::new(),
            renumbered: false,
            next_tag: 0,
        }
    }

    /// A unique tag per offer, so a payload identifies exactly one message.
    fn offer(&mut self, qos: QoS, ttl: Option<u32>) {
        let tag = self.next_tag;
        self.next_tag += 1;

        let mut message = Message::new("t", Bytes::from(tag.to_be_bytes().to_vec()), qos, false);
        message.expires_at = ttl.map(|ttl| self.now.saturating_add(ttl));
        self.offered.insert(tag, message.id);

        self.actions.clear();
        let now = self.now;
        self.out.offer(
            Delivery {
                message,
                qos,
                retain: false,
                dup: false,
                subscription_ids: Vec::new(),
                packet_id: None,
            },
            now,
            &mut self.actions,
        );
        self.absorb();
    }

    /// The identifier at `index`, wrapped into what is outstanding.
    fn pick(&self, index: usize) -> Option<u16> {
        if self.outstanding.is_empty() {
            return None;
        }
        Some(self.outstanding[index % self.outstanding.len()])
    }

    fn step(&mut self, op: &Op) {
        match op {
            Op::Offer(qos, ttl) => {
                self.offer(*qos, *ttl);
                return;
            }
            Op::Tick(delta) => {
                self.now = self.now.saturating_add(*delta);
                return;
            }
            Op::Ack(index) => {
                let Some(id) = self.pick(*index) else {
                    return;
                };
                self.actions.clear();
                if self.out.on_puback(id, self.now, &mut self.actions).is_ok() {
                    self.release(id);
                }
            }
            Op::Receipt(index, accepted) => {
                let Some(id) = self.pick(*index) else {
                    return;
                };
                let reason = if *accepted {
                    ReasonCode::Success
                } else {
                    ReasonCode::NotAuthorized
                };
                self.actions.clear();
                let now = self.now;
                if self
                    .out
                    .on_pubrec(id, reason, now, &mut self.actions)
                    .is_ok()
                    && !*accepted
                {
                    // A refused message ends the exchange here; an
                    // accepted one stays outstanding until it completes.
                    // Refusal is not delivery: the state machine reports
                    // it as a discard, and counting it as finished here
                    // as well would put it in two buckets at once and
                    // hide the difference between delivered and thrown
                    // away.
                    self.abandon(id);
                }
            }
            Op::Complete(index) => {
                let Some(id) = self.pick(*index) else {
                    return;
                };
                self.actions.clear();
                if self.out.on_pubcomp(id, self.now, &mut self.actions).is_ok() {
                    self.release(id);
                }
            }
            Op::Disconnect => {
                self.out.on_disconnect();
                self.actions.clear();
            }
            Op::Resume => {
                // A resume releases nothing. Every identifier outstanding
                // before it is outstanding after it: one whose receipt
                // arrived keeps it and has its release resent, and one
                // that did not keeps it and is republished under it.
                // Reallocating across a resume is a fault, not a
                // correction, so the watch carries straight through.
                self.actions.clear();
                let now = self.now;
                self.out.on_resume(now, &mut self.actions);
            }
        }
        self.absorb();
    }

    /// The peer refused the message. The identifier is free and the
    /// entry is gone, but the message was thrown away rather than
    /// delivered, and the drop action is what accounts for it.
    fn abandon(&mut self, id: u16) {
        self.live.remove(&id);
        self.outstanding.retain(|x| *x != id);
        self.carrying.remove(&id);
    }

    /// An exchange ended: the entry has left the window and the
    /// identifier is free again, so its message is accounted for.
    fn release(&mut self, id: u16) {
        self.live.remove(&id);
        self.outstanding.retain(|x| *x != id);
        if let Some(tag) = self.carrying.remove(&id) {
            if let Some(message_id) = self.offered.get(&tag) {
                self.done.insert(*message_id);
            }
        }
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
                    match publish.packet_id {
                        Some(id) => {
                            // An identifier still outstanding may only be
                            // used again by the very message it is
                            // already carrying, and only as a
                            // redelivery. Anything else means the peer
                            // will acknowledge the wrong message.
                            // Section 4.4: a redelivery keeps the
                            // identifier of the send it repeats.
                            if publish.dup && self.last_id.get(&tag) != Some(&id) {
                                self.renumbered = true;
                            }
                            self.last_id.insert(tag, id);

                            let carried = self.carrying.insert(id, tag);
                            if self.live.contains(&id) && (carried != Some(tag) || !publish.dup) {
                                self.reused = true;
                            }
                            if self.live.insert(id) {
                                self.outstanding.push(id);
                            }
                        }
                        // Fire-and-forget: there is no acknowledgement to
                        // wait for, so the exchange is over on the wire.
                        None => {
                            if let Some(message_id) = self.offered.get(&tag) {
                                self.done.insert(*message_id);
                            }
                        }
                    }
                }
                Action::Dropped { id, .. } => {
                    // A discarded message gives its identifier back
                    // inside the state machine, so the watch here has to
                    // let go of it too. A resumed message waits in the
                    // queue holding its identifier, and expiry or
                    // eviction can take it from there; leaving the entry
                    // behind would make the next message to be handed
                    // that identifier look like a reuse. It cannot
                    // happen today only because the allocator's counter
                    // is monotonic, and this should not depend on that.
                    let stranded = self
                        .carrying
                        .iter()
                        .find(|(_, tag)| self.offered.get(tag) == Some(&id))
                        .map(|(packet_id, _)| *packet_id);
                    if let Some(packet_id) = stranded {
                        self.abandon(packet_id);
                    }
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

    /// A redelivery carries the identifier of the send it repeats.
    /// Section 4.4 requires it, and a peer that received the first copy
    /// and lost its acknowledgement is waiting on exactly that value: a
    /// new one makes the redelivery look like an unrelated message, so
    /// the payload reaches the application a second time.
    #[test]
    fn a_redelivery_keeps_its_original_identifier(
        ops in prop::collection::vec(any_op(), 1..80),
        window in 1usize..8,
    ) {
        let run = drive(&ops, QueueLimits::default(), window);
        prop_assert!(!run.renumbered);
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

        // A send is not an ending: a message already on the wire can be
        // requeued by a resume and evicted afterwards, so it would count
        // as both sent and dropped. The three states that do partition
        // are finished, discarded, and still held.
        for (tag, id) in &run.offered {
            let places = usize::from(run.done.contains(id))
                + usize::from(run.dropped.contains(id))
                + usize::from(held.contains(id));
            prop_assert_eq!(
                places,
                1,
                "message {} is in {} of finished, dropped and held; \
                 exactly one is the only accounting that conserves it \
                 (finished {}, dropped {}, held {})",
                tag,
                places,
                run.done.contains(id),
                run.dropped.contains(id),
                held.contains(id)
            );
        }

        // And nothing appears in those sets that was never offered, which
        // the per-message check above cannot see.
        prop_assert_eq!(
            run.done.len() + run.dropped.len() + held.len(),
            run.offered.len(),
            "the three sets must cover the offered messages and nothing else"
        );
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
            run.offer(QoS::AtLeastOnce, None);
        }

        // Acknowledge the oldest repeatedly until nothing is outstanding.
        for _ in 0..(count * 4) {
            if run.outstanding.is_empty() {
                break;
            }
            run.step(&Op::Ack(0));
        }

        prop_assert_eq!(run.out.inflight_len(), 0);
        prop_assert_eq!(run.out.queued_len(), 0);
    }
}

// ---------------------------------------------------------------------
// The receive path, closed into a loop.
//
// The properties above drive one direction. The claim this subsystem
// actually exists to make spans both: a client publishes exactly once,
// and a subscriber's application sees it exactly once, however the two
// connections are interrupted. That needs the receive state machine at
// each end and the delivery state machine between them.
// ---------------------------------------------------------------------

/// One publishing client, the broker, and one subscribing client.
struct Loop {
    /// The broker receiving from the publishing client.
    from_publisher: Inbound,
    /// The broker sending to the subscribing client.
    to_subscriber: Outbound,
    /// The subscribing client receiving from the broker. Its state is
    /// session state: it survives the connection dropping, which is what
    /// lets it recognise a redelivery.
    at_subscriber: Inbound,

    actions: Vec<Action>,
    /// How many times each payload reached the subscriber's application.
    delivered: HashMap<u32, usize>,
    /// Identifiers the publishing client may still use.
    free_ids: Vec<u16>,
    /// What the publishing client has open, oldest first.
    publisher_open: Vec<u16>,
    /// The publish it sent last, so it can repeat it after a reconnect.
    last_publish: Option<Publish>,
    /// Receipts the subscriber has not handed to the broker yet.
    owed_receipts: Vec<u16>,
    /// Completions the subscriber has not handed to the broker yet.
    owed_completions: Vec<u16>,
    next_tag: u32,
}

#[derive(Debug, Clone)]
enum Hop {
    /// The publishing client sends a new message.
    Publish,
    /// It repeats its last one, as it would after its own reconnect.
    RepeatPublish,
    /// It releases its oldest open exchange.
    Release,
    /// The subscriber hands the broker one of its outstanding receipts.
    Receipt(usize),
    /// The subscriber hands the broker one of its outstanding completions.
    Complete(usize),
    /// The broker's link to the subscriber drops and comes back.
    Reconnect,
}

fn any_hop() -> impl Strategy<Value = Hop> {
    prop_oneof![
        5 => Just(Hop::Publish),
        2 => Just(Hop::RepeatPublish),
        3 => Just(Hop::Release),
        4 => (0usize..6).prop_map(Hop::Receipt),
        4 => (0usize..6).prop_map(Hop::Complete),
        2 => Just(Hop::Reconnect),
    ]
}

impl Loop {
    fn new(window: usize) -> Self {
        let mut to_subscriber = Outbound::new(QueueLimits::default(), window);
        to_subscriber.set_online(true);
        Self {
            // Small enough that the publishing client runs into it.
            from_publisher: Inbound::new(2),
            to_subscriber,
            at_subscriber: Inbound::new(0),
            actions: Vec::new(),
            delivered: HashMap::new(),
            free_ids: vec![1, 2, 3, 4],
            publisher_open: Vec::new(),
            last_publish: None,
            owed_receipts: Vec::new(),
            owed_completions: Vec::new(),
            next_tag: 0,
        }
    }

    fn step(&mut self, hop: &Hop) {
        match hop {
            Hop::Publish => {
                let Some(packet_id) = self.free_ids.pop() else {
                    return;
                };
                let tag = self.next_tag;
                self.next_tag += 1;
                let publish = Publish {
                    dup: false,
                    qos: QoS::ExactlyOnce,
                    retain: false,
                    topic: "t".into(),
                    packet_id: Some(packet_id),
                    payload: Bytes::from(tag.to_be_bytes().to_vec()),
                    properties: Default::default(),
                };
                if self.receive(&publish).is_none() {
                    // Refused for exceeding the receive maximum; the
                    // exchange never opened, so the identifier is free.
                    self.free_ids.push(packet_id);
                    return;
                }
                self.publisher_open.push(packet_id);
                self.last_publish = Some(publish);
            }
            Hop::RepeatPublish => {
                // The publisher's own resume. The broker must recognise
                // it and not offer the message to the subscriber twice.
                //
                // Only an exchange still open may be repeated.
                // [MQTT-4.4.0-1] permits resending an unacknowledged
                // publish and nothing else, and once the broker has sent
                // the completion [MQTT-4.3.3-12] *requires* it to treat
                // the same identifier as a new application message. A
                // client that repeats a completed exchange has published
                // twice, so a second delivery there is correct and this
                // property would be asserting something the protocol
                // does not promise.
                let Some(mut publish) = self.last_publish.clone() else {
                    return;
                };
                let packet_id = publish.packet_id.expect("an exactly-once publish has one");
                if !self.publisher_open.contains(&packet_id) {
                    return;
                }
                publish.dup = true;
                self.receive(&publish);
            }
            Hop::Release => {
                if self.publisher_open.is_empty() {
                    return;
                }
                let packet_id = self.publisher_open.remove(0);
                let mut sink = Vec::new();
                self.from_publisher.on_pubrel(packet_id, &mut sink);
                self.free_ids.push(packet_id);
            }
            Hop::Receipt(index) => {
                if self.owed_receipts.is_empty() {
                    return;
                }
                let packet_id = self.owed_receipts[index % self.owed_receipts.len()];
                self.actions.clear();
                if self
                    .to_subscriber
                    .on_pubrec(packet_id, ReasonCode::Success, 0, &mut self.actions)
                    .is_ok()
                {
                    self.owed_receipts.retain(|x| *x != packet_id);
                }
                self.absorb();
            }
            Hop::Complete(index) => {
                if self.owed_completions.is_empty() {
                    return;
                }
                let packet_id = self.owed_completions[index % self.owed_completions.len()];
                self.actions.clear();
                if self
                    .to_subscriber
                    .on_pubcomp(packet_id, 0, &mut self.actions)
                    .is_ok()
                {
                    self.owed_completions.retain(|x| *x != packet_id);
                }
                self.absorb();
            }
            Hop::Reconnect => {
                self.to_subscriber.on_disconnect();
                // The subscribing client sees the drop too. Its
                // exactly-once state survives it, which is what lets it
                // recognise what the broker resends.
                self.at_subscriber.on_disconnect();
                self.actions.clear();
                self.to_subscriber.on_resume(0, &mut self.actions);
                self.absorb();
            }
        }
    }

    /// The broker takes a publish from the client and, if it is not a
    /// repeat it already holds, offers it to the subscriber.
    ///
    /// Returns nothing when the client exceeded the receive maximum.
    fn receive(&mut self, publish: &Publish) -> Option<()> {
        let received = self.from_publisher.on_publish(publish).ok()?;
        let mut sink = Vec::new();
        if let Some(owed) = received.owed {
            self.from_publisher.acknowledge(
                owed.packet_id,
                owed.qos,
                ReasonCode::Success,
                &mut sink,
            );
        }
        if received.dispatch {
            let message = Message::new("t", publish.payload.clone(), QoS::ExactlyOnce, false);
            self.actions.clear();
            self.to_subscriber.offer(
                Delivery {
                    message,
                    qos: QoS::ExactlyOnce,
                    retain: false,
                    dup: false,
                    subscription_ids: Vec::new(),
                    packet_id: None,
                },
                0,
                &mut self.actions,
            );
            self.absorb();
        }
        Some(())
    }

    /// Everything the broker asked to be sent to the subscriber, put
    /// through the subscriber's own receive state machine.
    fn absorb(&mut self) {
        for action in std::mem::take(&mut self.actions) {
            match action {
                Action::Send(Packet::Publish(publish)) => {
                    let tag = u32::from_be_bytes(
                        publish.payload[..4].try_into().expect("a four byte tag"),
                    );
                    let received = self
                        .at_subscriber
                        .on_publish(&publish)
                        .expect("the subscriber's receive maximum is unbounded");
                    if received.dispatch {
                        *self.delivered.entry(tag).or_default() += 1;
                    }
                    if let Some(owed) = received.owed {
                        let mut sink = Vec::new();
                        self.at_subscriber.acknowledge(
                            owed.packet_id,
                            owed.qos,
                            ReasonCode::Success,
                            &mut sink,
                        );
                        if !self.owed_receipts.contains(&owed.packet_id) {
                            self.owed_receipts.push(owed.packet_id);
                        }
                    }
                }
                Action::Send(Packet::PubRel(ack)) => {
                    let mut sink = Vec::new();
                    self.at_subscriber.on_pubrel(ack.packet_id, &mut sink);
                    if !self.owed_completions.contains(&ack.packet_id) {
                        self.owed_completions.push(ack.packet_id);
                    }
                }
                _ => {}
            }
        }
    }
}

proptest! {
    /// End to end: a payload published exactly once reaches the
    /// subscriber's application at most once, however the connection
    /// between the broker and the subscriber is interrupted.
    ///
    /// This is the claim the whole subsystem exists to make, and it is
    /// the only property that involves both directions. A redelivery
    /// under a new identifier, a resume that republishes something the
    /// subscriber already holds, or a receive path that forgets what it
    /// is holding all show up here as a second delivery.
    #[test]
    fn a_message_never_reaches_the_subscriber_twice(
        hops in prop::collection::vec(any_hop(), 1..80),
        window in 1usize..4,
    ) {
        let mut run = Loop::new(window);
        for hop in &hops {
            run.step(hop);
        }
        for (tag, count) in &run.delivered {
            prop_assert!(
                *count <= 1,
                "payload {} reached the subscriber's application {} times",
                tag,
                count
            );
        }
    }
}
