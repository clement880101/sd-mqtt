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
            last_id: HashMap::new(),
            renumbered: false,
            next_tag: 0,
        }
    }

    /// A unique tag per offer, so a payload identifies exactly one message.
    fn offer(&mut self, qos: QoS) {
        let tag = self.next_tag;
        self.next_tag += 1;

        let message = Message::new("t", Bytes::from(tag.to_be_bytes().to_vec()), qos, false);
        self.offered.insert(tag, message.id);

        self.actions.clear();
        self.out.offer(
            Delivery {
                message,
                qos,
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
                // A resume releases nothing. Every identifier outstanding
                // before it is outstanding after it: one whose receipt
                // arrived keeps it and has its release resent, and one
                // that did not keeps it and is republished under it.
                // Reallocating across a resume is a fault, not a
                // correction, so the watch carries straight through.
                self.actions.clear();
                self.out.on_resume(0, &mut self.actions);
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
