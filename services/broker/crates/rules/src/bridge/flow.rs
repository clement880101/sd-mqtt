//! What this bridge has sent the remote and not had answered.
//!
//! Item 5 on the research's list, and the client-side mirror of
//! `broker_core::session::outbound`. **Read that one first and do not
//! try to share it**: it is typed for the server throughout — a session
//! key, a journal of durable records, a queue with per-session bounds,
//! an `Action` a connection hands back — and the two have in common a
//! rule, not a structure.
//!
//! The rule is [MQTT-3.3.4-7] read from the sending side, which
//! section 4.9 states as a quota: a client may not have more than the
//! server's Receive Maximum "QoS 1 and QoS 2 PUBLISH packets for which
//! it has not received PUBACK or PUBCOMP". Both levels count, the
//! quota is released on PUBACK for one and PUBCOMP for the other, and
//! exceeding it is a protocol error the remote **closes the
//! connection** for rather than refusing a packet.
//!
//! # A resumed publish keeps its identifier
//!
//! Convention 3 records this as the worst instance of deriving a rule
//! from a plan rather than from the specification, and it is the same
//! rule here:
//!
//! > The design spec said a resumed message is "requeued with the
//! > duplicate flag" and omitted that it must keep its original packet
//! > identifier. […] The result delivers a message twice to a peer
//! > still waiting on the old identifier, and later discards a
//! > genuinely new message when the counter wraps onto the stranded
//! > one.
//!
//! [MQTT-4.4.0-1] is the clause: a client resending an unacknowledged
//! PUBLISH uses the original Packet Identifier, with the DUP flag set.
//! [`Flow::resume`] does exactly that, in the order the messages were
//! sent, which is also what [MQTT-4.6.0-1] requires of a resend.
//!
//! # An exactly-once exchange half finished resumes as a release
//!
//! A message the remote has already taken ownership of — one it sent
//! PUBREC for — must not be sent again as a PUBLISH. [MQTT-4.3.3-x]:
//! the sender resends the PUBREL, not the publish, because the
//! receiver has the message and is waiting to be told to release it.
//! Sending the publish again would deliver it twice to a peer that had
//! already promised exactly once.
//!
//! # Across a process restart there is nothing to resume
//!
//! This window lives in memory. What makes that safe is not durability
//! here but the clean start the supervisor forces on the **first**
//! connection of a process: a session the remote discards holds no
//! identifier this end has forgotten, so the outbox can re-send from
//! the beginning with fresh identifiers and nothing is stranded. See
//! [`crate::bridge::supervisor`], which is where that decision is
//! written down.

use mqtt_codec::packet::{Ack, Publish};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode};

/// How far one exchange has got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// Sent at least-once, waiting for the acknowledgement.
    AwaitingAck,
    /// Sent exactly-once, waiting for the receipt.
    AwaitingReceived,
    /// Receipt seen and release sent, waiting for completion.
    ///
    /// The distinction is the whole reason this is an enum: a message
    /// here must be resumed by resending the release, because the
    /// remote already holds it.
    AwaitingComplete,
}

/// One message sent and not yet answered.
#[derive(Debug, Clone)]
struct Sent {
    /// The identifier the remote will answer, and the one a resend
    /// must keep.
    packet_id: u16,
    /// The publish exactly as it went out, so that a resend is the
    /// same message rather than a reconstruction of it.
    publish: Publish,
    /// How far the exchange has got.
    stage: Stage,
    /// What the caller wants back when this finishes, so it can retire
    /// whatever it was holding.
    token: u64,
}

/// Why an acknowledgement could not be matched.
///
/// Reported rather than ignored. An acknowledgement for something that
/// was never sent means this end and the remote disagree about what is
/// outstanding, and carrying on would leak a window slot for the life
/// of the connection.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FlowError {
    /// The remote answered an identifier nothing is waiting on.
    #[error("the remote sent a {packet} for packet {packet_id}, which is not outstanding")]
    Unknown {
        /// Which acknowledgement.
        packet: &'static str,
        /// The identifier it named.
        packet_id: u16,
    },
    /// The remote answered with the wrong acknowledgement for the
    /// stage the exchange had reached.
    #[error(
        "the remote sent a {packet} for packet {packet_id}, which is {stage:?}; that is \
         not the answer this stage expects"
    )]
    OutOfStep {
        /// Which acknowledgement.
        packet: &'static str,
        /// The identifier it named.
        packet_id: u16,
        /// Where the exchange actually is.
        stage: Stage,
    },
}

/// What finishing an exchange produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finished {
    /// What the caller handed in with the publish, so it can retire
    /// the outbox entry that produced it.
    pub token: u64,
    /// Whether the remote accepted the message.
    ///
    /// A reason code of 0x80 or above is a refusal, and [MQTT-4.3.3-x]
    /// says the publish is then treated as acknowledged and must not
    /// be retransmitted. So the slot is released either way, and this
    /// is what tells the caller whether to count a delivery or a
    /// rejection.
    pub accepted: bool,
    /// What the remote said, for a log line.
    pub reason: ReasonCode,
}

/// The window of messages sent to the remote and not yet answered.
pub struct Flow {
    /// In send order, which is the order a resume must use.
    ///
    /// A vector rather than a map, for the reason
    /// `broker_core::session::inflight` gives: the limit comes from a
    /// peer's receive maximum and is small in every realistic setting,
    /// so a linear scan beats hashing and keeps resume order right for
    /// free.
    sent: Vec<Sent>,
    /// The ceiling the remote declared.
    limit: usize,
    /// Where to start looking for the next free identifier.
    next_id: u16,
}

impl Flow {
    /// A window holding at most `limit` exchanges.
    pub fn new(limit: u16) -> Self {
        Self {
            sent: Vec::new(),
            limit: usize::from(limit),
            next_id: 1,
        }
    }

    /// Narrow or widen the window to what the remote declared.
    ///
    /// Lowering it below what is already outstanding stops new sends
    /// and discards nothing: a remote that advertises a smaller receive
    /// maximum on a resumed session has not forgotten what it already
    /// holds.
    pub fn set_limit(&mut self, limit: u16) {
        self.limit = usize::from(limit);
    }

    /// Whether another message may be sent.
    pub fn has_room(&self) -> bool {
        self.sent.len() < self.limit
    }

    /// The largest token outstanding, if anything is.
    ///
    /// The bridge passes its outbox sequence as a token, so this is
    /// how far the outbox has been read on a session that is being
    /// resumed: everything at or below it is already on the wire under
    /// an identifier the remote knows, and re-reading it would send it
    /// twice.
    pub fn highest_token(&self) -> Option<u64> {
        self.sent.iter().map(|sent| sent.token).max()
    }

    /// How many exchanges are outstanding.
    pub fn len(&self) -> usize {
        self.sent.len()
    }

    /// Whether nothing is outstanding.
    pub fn is_empty(&self) -> bool {
        self.sent.is_empty()
    }

    /// Record a fire-and-forget publish, which occupies no slot.
    ///
    /// Section 4.9 counts "QoS 1 and QoS 2 PUBLISH packets", so an
    /// at-most-once message is not in the quota at all. Its own method
    /// so that a caller cannot reach [`Flow::send`] with one and have
    /// the window silently grow on messages nothing will ever answer.
    pub fn unacknowledged(publish: Publish) -> Packet {
        debug_assert_eq!(publish.qos, QoS::AtMostOnce);
        Packet::Publish(publish)
    }

    /// Take a slot and an identifier for `publish`.
    ///
    /// `token` is whatever the caller needs back when the exchange
    /// finishes; the bridge passes its outbox sequence number, so that
    /// an entry is retired exactly when the remote has taken
    /// responsibility for it.
    ///
    /// `None` when the window is full or the identifier space is
    /// exhausted — the caller waits, which is what applying the
    /// remote's backpressure means.
    pub fn send(&mut self, mut publish: Publish, token: u64) -> Option<Packet> {
        debug_assert_ne!(publish.qos, QoS::AtMostOnce);
        if !self.has_room() {
            return None;
        }
        let packet_id = self.take_id()?;
        publish.packet_id = Some(packet_id);
        publish.dup = false;
        self.sent.push(Sent {
            packet_id,
            publish: publish.clone(),
            stage: match publish.qos {
                QoS::ExactlyOnce => Stage::AwaitingReceived,
                _ => Stage::AwaitingAck,
            },
            token,
        });
        Some(Packet::Publish(publish))
    }

    /// The lowest free identifier at or after the cursor.
    ///
    /// The loop that skips identifiers still in use is reachable here
    /// and is exercised: convention 5 records that the broker's own
    /// allocator had this path untested because reaching it needs the
    /// counter to wrap onto something held, and the test below wraps.
    fn take_id(&mut self) -> Option<u16> {
        for _ in 0..u16::MAX {
            let candidate = self.next_id;
            self.next_id = if self.next_id == u16::MAX {
                1
            } else {
                self.next_id + 1
            };
            if !self.sent.iter().any(|sent| sent.packet_id == candidate) {
                return Some(candidate);
            }
        }
        None
    }

    /// The remote acknowledged an at-least-once publish.
    ///
    /// # Errors
    ///
    /// [`FlowError`] when nothing is waiting on that identifier, or
    /// when the exchange is past the stage a PUBACK answers.
    pub fn on_puback(&mut self, ack: &Ack) -> Result<Finished, FlowError> {
        let index = self.find("puback", ack.packet_id)?;
        if self.sent[index].stage != Stage::AwaitingAck {
            return Err(FlowError::OutOfStep {
                packet: "puback",
                packet_id: ack.packet_id,
                stage: self.sent[index].stage,
            });
        }
        let sent = self.sent.remove(index);
        Ok(Finished {
            token: sent.token,
            accepted: !ack.reason.is_failure(),
            reason: ack.reason,
        })
    }

    /// The remote took ownership of an exactly-once publish.
    ///
    /// Answers with the PUBREL to send, or with the exchange having
    /// ended: [MQTT-4.3.3-x] says a PUBREC carrying 0x80 or above
    /// means the publish is treated as acknowledged and must not be
    /// retransmitted, so the slot is released and no release goes out.
    ///
    /// # Errors
    ///
    /// [`FlowError`] when nothing is waiting on that identifier, or
    /// when the exchange is not at the stage a PUBREC answers.
    pub fn on_pubrec(
        &mut self,
        ack: &Ack,
    ) -> Result<(Option<Packet>, Option<Finished>), FlowError> {
        let index = self.find("pubrec", ack.packet_id)?;
        if self.sent[index].stage != Stage::AwaitingReceived {
            return Err(FlowError::OutOfStep {
                packet: "pubrec",
                packet_id: ack.packet_id,
                stage: self.sent[index].stage,
            });
        }
        if ack.reason.is_failure() {
            let sent = self.sent.remove(index);
            return Ok((
                None,
                Some(Finished {
                    token: sent.token,
                    accepted: false,
                    reason: ack.reason,
                }),
            ));
        }
        self.sent[index].stage = Stage::AwaitingComplete;
        // The payload is not kept past here: the remote holds the
        // message now, and everything left to say about it is the
        // identifier. Dropping it is what stops a bridge against a slow
        // remote holding every payload it has ever half-delivered.
        self.sent[index].publish.payload = bytes::Bytes::new();
        Ok((Some(release(ack.packet_id)), None))
    }

    /// The remote completed an exactly-once exchange.
    ///
    /// # Errors
    ///
    /// [`FlowError`] when nothing is waiting on that identifier, or
    /// when the exchange never reached the stage a PUBCOMP answers.
    pub fn on_pubcomp(&mut self, ack: &Ack) -> Result<Finished, FlowError> {
        let index = self.find("pubcomp", ack.packet_id)?;
        if self.sent[index].stage != Stage::AwaitingComplete {
            return Err(FlowError::OutOfStep {
                packet: "pubcomp",
                packet_id: ack.packet_id,
                stage: self.sent[index].stage,
            });
        }
        let sent = self.sent.remove(index);
        Ok(Finished {
            token: sent.token,
            accepted: !ack.reason.is_failure(),
            reason: ack.reason,
        })
    }

    /// Everything outstanding, to be sent again on a resumed session.
    ///
    /// **In send order, with the original identifiers, with the
    /// duplicate flag set** — and a half-finished exactly-once exchange
    /// resumes as a release rather than as the publish, because the
    /// remote already holds the message.
    pub fn resume(&mut self) -> Vec<Packet> {
        self.sent
            .iter_mut()
            .map(|sent| match sent.stage {
                Stage::AwaitingComplete => release(sent.packet_id),
                _ => {
                    sent.publish.dup = true;
                    // Belt and braces against the failure convention 3
                    // names: the identifier on the wire is the one this
                    // entry is filed under, whatever else happened to
                    // the publish in between.
                    sent.publish.packet_id = Some(sent.packet_id);
                    Packet::Publish(sent.publish.clone())
                }
            })
            .collect()
    }

    /// The remote has no session for us, so nothing is outstanding.
    ///
    /// Everything this held is still in the outbox — nothing is
    /// acknowledged until the remote says so — and will be sent again
    /// from there, with fresh identifiers, which is correct precisely
    /// because the remote has discarded whatever it knew.
    ///
    /// Answers with the tokens that were dropped, so the caller can put
    /// them back rather than losing track of them.
    pub fn forget(&mut self) -> Vec<u64> {
        self.next_id = 1;
        self.sent.drain(..).map(|sent| sent.token).collect()
    }

    /// Where an identifier sits, or why it does not.
    fn find(&self, packet: &'static str, packet_id: u16) -> Result<usize, FlowError> {
        self.sent
            .iter()
            .position(|sent| sent.packet_id == packet_id)
            .ok_or(FlowError::Unknown { packet, packet_id })
    }
}

/// The release for an exchange the remote has taken ownership of.
fn release(packet_id: u16) -> Packet {
    Packet::PubRel(Ack {
        packet_id,
        reason: ReasonCode::Success,
        properties: Properties::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn publish(qos: QoS) -> Publish {
        Publish {
            dup: false,
            qos,
            retain: false,
            topic: "a/b".into(),
            packet_id: None,
            payload: Bytes::from_static(b"hello"),
            properties: Properties::default(),
        }
    }

    fn ack(packet_id: u16) -> Ack {
        Ack {
            packet_id,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        }
    }

    fn sent_id(packet: &Packet) -> u16 {
        match packet {
            Packet::Publish(publish) => publish.packet_id.expect("an acknowledged level"),
            Packet::PubRel(ack) => ack.packet_id,
            other => panic!("not a sent packet: {other:?}"),
        }
    }

    /// The rule the whole module exists for. Exceeding it is a
    /// protocol error the remote closes the connection for.
    #[test]
    fn the_window_stops_at_the_remote_s_receive_maximum() {
        let mut flow = Flow::new(2);
        assert!(flow.send(publish(QoS::AtLeastOnce), 1).is_some());
        assert!(flow.send(publish(QoS::AtLeastOnce), 2).is_some());
        assert!(
            flow.send(publish(QoS::AtLeastOnce), 3).is_none(),
            "a third would be one more than the remote said it would take"
        );

        // A slot comes back when the exchange ends, and not before.
        flow.on_puback(&ack(1)).expect("outstanding");
        assert!(flow.send(publish(QoS::AtLeastOnce), 3).is_some());
    }

    /// Both levels count. Section 4.9 says the quota covers "QoS 1 and
    /// QoS 2 PUBLISH packets", and a window that counted only the
    /// exactly-once ones would let a bridge sending at-least-once
    /// exceed it without noticing.
    #[test]
    fn both_acknowledged_levels_take_a_slot() {
        let mut flow = Flow::new(2);
        assert!(flow.send(publish(QoS::ExactlyOnce), 1).is_some());
        assert!(flow.send(publish(QoS::AtLeastOnce), 2).is_some());
        assert!(flow.send(publish(QoS::AtLeastOnce), 3).is_none());
    }

    /// And an exactly-once slot is held until the *completion*, not
    /// until the receipt: section 4.9 replenishes on PUBCOMP.
    #[test]
    fn an_exactly_once_slot_is_held_until_the_completion() {
        let mut flow = Flow::new(1);
        flow.send(publish(QoS::ExactlyOnce), 1).expect("room");
        let (release, finished) = flow.on_pubrec(&ack(1)).expect("outstanding");
        assert!(matches!(release, Some(Packet::PubRel(_))));
        assert!(finished.is_none());
        assert!(
            !flow.has_room(),
            "the receipt is not the end of the exchange"
        );

        let done = flow.on_pubcomp(&ack(1)).expect("outstanding");
        assert_eq!(done.token, 1);
        assert!(flow.has_room());
    }

    /// Convention 3's worst instance, guarded from the client side.
    /// A resend that invented a new identifier would deliver the
    /// message twice to a remote still waiting on the old one, and
    /// would later discard a genuinely new message when the counter
    /// wrapped onto the stranded identifier.
    #[test]
    fn a_resumed_publish_keeps_its_original_identifier() {
        let mut flow = Flow::new(8);
        let first = sent_id(&flow.send(publish(QoS::AtLeastOnce), 10).expect("room"));
        let second = sent_id(&flow.send(publish(QoS::ExactlyOnce), 11).expect("room"));

        let resumed = flow.resume();
        assert_eq!(resumed.len(), 2);
        assert_eq!(
            resumed.iter().map(sent_id).collect::<Vec<_>>(),
            vec![first, second],
            "the original identifiers, in the order they were sent"
        );
        for packet in &resumed {
            let Packet::Publish(publish) = packet else {
                panic!("both are still publishes");
            };
            assert!(publish.dup, "MQTT-4.4.0-1 wants the duplicate flag too");
        }

        // And the acknowledgements still land, which they would not if
        // the resend had renumbered them.
        flow.on_puback(&ack(first)).expect("still outstanding");
        flow.on_pubrec(&ack(second)).expect("still outstanding");
    }

    /// [MQTT-4.3.3-x]: the remote already holds the message, so the
    /// resend is the release. Sending the publish again would deliver
    /// it twice to a peer that had been promised exactly once.
    #[test]
    fn a_half_finished_exactly_once_exchange_resumes_as_a_release() {
        let mut flow = Flow::new(8);
        let id = sent_id(&flow.send(publish(QoS::ExactlyOnce), 1).expect("room"));
        flow.on_pubrec(&ack(id)).expect("outstanding");

        let resumed = flow.resume();
        assert_eq!(resumed.len(), 1);
        assert!(
            matches!(&resumed[0], Packet::PubRel(ack) if ack.packet_id == id),
            "got {:?}",
            resumed[0]
        );
    }

    /// Convention 5: the broker's own allocator had this path
    /// unexercised because reaching it needs the counter to wrap onto
    /// something still held. This one wraps.
    #[test]
    fn the_allocator_skips_an_identifier_still_in_use() {
        let mut flow = Flow::new(u16::MAX);
        // Hold identifier 1 for the whole test.
        let held = sent_id(&flow.send(publish(QoS::ExactlyOnce), 0).expect("room"));
        assert_eq!(held, 1);

        // Spend the rest of the space and give every one of them back,
        // so the cursor wraps round onto the held one.
        for token in 1..u32::from(u16::MAX) {
            let packet = flow
                .send(publish(QoS::AtLeastOnce), u64::from(token))
                .expect("room");
            flow.on_puback(&ack(sent_id(&packet))).expect("outstanding");
        }

        let next = sent_id(&flow.send(publish(QoS::AtLeastOnce), 99).expect("room"));
        assert_ne!(next, held, "the cursor wrapped onto an identifier in use");
        assert_eq!(next, 2);
    }

    /// An acknowledgement for something never sent means the two ends
    /// disagree about what is outstanding. Carrying on would leak the
    /// slot for the life of the connection.
    #[test]
    fn an_acknowledgement_for_nothing_outstanding_is_an_error() {
        let mut flow = Flow::new(4);
        assert_eq!(
            flow.on_puback(&ack(7)),
            Err(FlowError::Unknown {
                packet: "puback",
                packet_id: 7
            })
        );
        flow.send(publish(QoS::AtLeastOnce), 1).expect("room");
        assert!(matches!(
            flow.on_pubcomp(&ack(1)),
            Err(FlowError::OutOfStep {
                stage: Stage::AwaitingAck,
                ..
            })
        ));
    }

    /// [MQTT-4.3.3-x]: a receipt carrying 0x80 or above ends the
    /// exchange. The slot comes back, no release goes out, and the
    /// caller is told the remote refused it rather than left to
    /// believe it was delivered.
    #[test]
    fn a_refused_receipt_ends_the_exchange_without_a_release() {
        let mut flow = Flow::new(1);
        flow.send(publish(QoS::ExactlyOnce), 42).expect("room");
        let refusal = Ack {
            packet_id: 1,
            reason: ReasonCode::NotAuthorized,
            properties: Properties::default(),
        };
        let (release, finished) = flow.on_pubrec(&refusal).expect("outstanding");
        assert!(release.is_none(), "it must not be retransmitted");
        let finished = finished.expect("the exchange is over");
        assert_eq!(finished.token, 42);
        assert!(!finished.accepted);
        assert!(flow.has_room());
    }

    #[test]
    fn a_refused_acknowledgement_still_releases_the_slot() {
        let mut flow = Flow::new(1);
        flow.send(publish(QoS::AtLeastOnce), 5).expect("room");
        let finished = flow
            .on_puback(&Ack {
                packet_id: 1,
                reason: ReasonCode::NoMatchingSubscribers,
                properties: Properties::default(),
            })
            .expect("outstanding");
        assert_eq!(finished.token, 5);
        assert!(
            finished.accepted,
            "no matching subscribers is a success; the remote took it"
        );
        assert!(flow.has_room());
    }

    #[test]
    fn a_narrowed_window_discards_nothing_already_outstanding() {
        let mut flow = Flow::new(4);
        for token in 0..4 {
            flow.send(publish(QoS::AtLeastOnce), token).expect("room");
        }
        flow.set_limit(1);
        assert_eq!(flow.len(), 4, "the remote has not forgotten what it holds");
        assert!(!flow.has_room());
    }

    /// A resumed session must not re-read the outbox from the
    /// beginning: everything at or below this is already on the wire
    /// under an identifier the remote knows.
    #[test]
    fn the_highest_token_outstanding_is_how_far_the_outbox_has_been_read() {
        let mut flow = Flow::new(8);
        assert_eq!(flow.highest_token(), None);
        flow.send(publish(QoS::AtLeastOnce), 5).expect("room");
        flow.send(publish(QoS::ExactlyOnce), 7).expect("room");
        flow.send(publish(QoS::AtLeastOnce), 6).expect("room");
        assert_eq!(flow.highest_token(), Some(7), "the largest, not the last");

        let packet = flow.send(publish(QoS::AtLeastOnce), 9).expect("room");
        flow.on_puback(&ack(sent_id(&packet))).expect("outstanding");
        assert_eq!(
            flow.highest_token(),
            Some(7),
            "an exchange that finished is no longer on the wire"
        );
    }

    #[test]
    fn forgetting_a_session_gives_the_tokens_back() {
        let mut flow = Flow::new(4);
        flow.send(publish(QoS::AtLeastOnce), 7).expect("room");
        flow.send(publish(QoS::ExactlyOnce), 9).expect("room");
        assert_eq!(flow.forget(), vec![7, 9]);
        assert!(flow.is_empty());
    }
}
