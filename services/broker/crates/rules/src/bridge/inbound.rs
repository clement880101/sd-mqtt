//! What the remote has sent this bridge and not had answered.
//!
//! Item 6, and the client-side mirror of
//! `broker_core::session::inbound`. **Read that one first and do not
//! try to share it**: it is written in session vocabulary — a session
//! key, a durable journal, an acknowledgement that is *owed* rather
//! than sent so that a reason code can say what happened to the
//! message once the broker has tried to dispatch it. A bridge has none
//! of that. It knows the answer immediately, because "dispatch" here
//! is a publish into the local broker that cannot fail for a reason
//! the remote would want to hear about.
//!
//! What the two share is the rule, and it is the one that matters:
//!
//! **Only exactly-once needs memory.** At-least-once is acknowledged
//! and forgotten, because [MQTT-4.3.2-5] requires a publish carrying an
//! identifier already acknowledged to be treated as a new message
//! whatever its duplicate flag says. Remembering it and suppressing
//! the repeat would be *wrong*, not merely wasteful.
//!
//! **An exactly-once repeat is answered again and delivered once.**
//! The remote resends a publish it has had no receipt for; this end has
//! it, so it sends the receipt again and does not hand the message on.
//! An implementation that delivered it twice has broken the one
//! promise the level exists to make, and it looks correct from the
//! outside until somebody counts.
//!
//! # The receive maximum here is the one this end advertised
//!
//! The other direction's ceiling is the remote's and lives in
//! [`super::flow`]. This one is ours, sent in the CONNECT, and
//! [MQTT-3.3.4-9] makes a remote that exceeds it something to
//! **disconnect** rather than to refuse a packet from. Both levels
//! count toward it, for the reason `broker_core::session::inbound`
//! gives at length: section 4.9 replenishes on the PUBACK for one and
//! the PUBCOMP for the other, so the count is the sum.
//!
//! # A network connection is where this ends
//!
//! Section 4.9 says the quota "is not preserved across Network
//! Connections", and MQTT 5 makes the exactly-once half session state
//! that does survive. This bridge keeps neither across a process
//! restart, for the reason [`super::flow`] gives: the supervisor
//! forces a clean start on the first connection of a process, so there
//! is no session for a forgotten identifier to strand.

use std::collections::HashSet;

use mqtt_codec::packet::{Ack, Publish};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode};

/// What to do with a publish the remote sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Received {
    /// Whether the message is published into the local broker.
    ///
    /// False for the repeat of an exactly-once message already held,
    /// which must be answered again and must not be delivered twice.
    pub deliver: bool,
    /// What goes back to the remote, if anything.
    pub answer: Option<Packet>,
}

/// What the remote has sent and this end has not finished answering.
pub struct Inbound {
    /// Identifiers whose at-least-once publish has arrived and whose
    /// acknowledgement has not gone out.
    ///
    /// Held only so that the quota is honest. A bridge answers inside
    /// one call, so this is almost always empty; it exists because the
    /// count is defined over "not yet acknowledged" and a count that
    /// quietly meant something else would be wrong the first time an
    /// answer was deferred.
    awaiting_ack: HashSet<u16>,
    /// Identifiers whose exactly-once publish has arrived and whose
    /// release has not.
    awaiting_release: HashSet<u16>,
    /// What this end advertised it would take at once. Zero is
    /// unbounded, which is what an absent receive maximum means.
    receive_maximum: usize,
}

impl Inbound {
    /// A receiver permitting `receive_maximum` unanswered publishes.
    pub fn new(receive_maximum: u16) -> Self {
        Self {
            awaiting_ack: HashSet::new(),
            awaiting_release: HashSet::new(),
            receive_maximum: usize::from(receive_maximum),
        }
    }

    /// How many are unanswered.
    pub fn outstanding(&self) -> usize {
        self.awaiting_ack.len() + self.awaiting_release.len()
    }

    /// Forget everything, because the network connection ended.
    pub fn reset(&mut self) {
        self.awaiting_ack.clear();
        self.awaiting_release.clear();
    }

    /// Decide what to do with a publish from the remote.
    ///
    /// # Errors
    ///
    /// The reason code to **close the connection** with.
    /// [MQTT-3.3.4-9] names a disconnect, not a refusal packet, as the
    /// answer to a peer that exceeds the receive maximum, and
    /// [MQTT-2.2.1-2] makes an acknowledged publish with no identifier
    /// a protocol error.
    pub fn on_publish(&mut self, publish: &Publish) -> Result<Received, ReasonCode> {
        match publish.qos {
            QoS::AtMostOnce => Ok(Received {
                deliver: true,
                answer: None,
            }),
            QoS::AtLeastOnce => {
                let packet_id = publish.packet_id.ok_or(ReasonCode::ProtocolError)?;
                // A repeat of one still unanswered is already counted,
                // so it does not take a second slot.
                if !self.awaiting_ack.contains(&packet_id) {
                    self.admit()?;
                    self.awaiting_ack.insert(packet_id);
                }
                // [MQTT-4.3.2-5]: whatever the duplicate flag says,
                // this is a new message. Delivered.
                Ok(Received {
                    deliver: true,
                    answer: Some(self.acknowledge(packet_id)),
                })
            }
            QoS::ExactlyOnce => {
                let packet_id = publish.packet_id.ok_or(ReasonCode::ProtocolError)?;
                let held = self.awaiting_release.contains(&packet_id);
                if !held {
                    self.admit()?;
                    self.awaiting_release.insert(packet_id);
                }
                Ok(Received {
                    // The one rule the level exists for. A repeat is
                    // answered again and handed on no second time.
                    deliver: !held,
                    answer: Some(Packet::PubRec(Ack {
                        packet_id,
                        reason: ReasonCode::Success,
                        properties: Properties::default(),
                    })),
                })
            }
        }
    }

    /// The remote released an exactly-once message.
    ///
    /// The completion goes back whether or not this end still holds
    /// the identifier: [MQTT-4.3.3-x] requires a PUBCOMP for every
    /// PUBREL, and a release for something already completed is the
    /// remote resending a release whose completion went missing. A
    /// bridge that answered only the first would leave the remote
    /// resending for ever.
    pub fn on_pubrel(&mut self, ack: &Ack) -> Packet {
        let known = self.awaiting_release.remove(&ack.packet_id);
        Packet::PubComp(Ack {
            packet_id: ack.packet_id,
            reason: if known {
                ReasonCode::Success
            } else {
                // [MQTT-3.6.2-x]: the reason a receiver gives for a
                // release naming something it is not holding.
                ReasonCode::PacketIdentifierNotFound
            },
            properties: Properties::default(),
        })
    }

    /// The acknowledgement for an at-least-once publish, releasing its
    /// slot.
    fn acknowledge(&mut self, packet_id: u16) -> Packet {
        self.awaiting_ack.remove(&packet_id);
        Packet::PubAck(Ack {
            packet_id,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        })
    }

    /// Whether there is room for one more.
    fn admit(&self) -> Result<(), ReasonCode> {
        if self.receive_maximum > 0 && self.outstanding() >= self.receive_maximum {
            return Err(ReasonCode::ReceiveMaximumExceeded);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

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

    fn ack(packet_id: u16) -> Ack {
        Ack {
            packet_id,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        }
    }

    #[test]
    fn a_fire_and_forget_publish_is_delivered_and_not_answered() {
        let mut inbound = Inbound::new(4);
        let received = inbound
            .on_publish(&publish(QoS::AtMostOnce, None, false))
            .expect("accepted");
        assert_eq!(
            received,
            Received {
                deliver: true,
                answer: None
            }
        );
        assert_eq!(inbound.outstanding(), 0);
    }

    /// [MQTT-4.3.2-5]. A repeat at at-least-once is a **new message**,
    /// whatever its duplicate flag says. Suppressing it would be wrong
    /// rather than merely wasteful: the remote is entitled to send
    /// again and this end is not promising to notice.
    #[test]
    fn an_at_least_once_repeat_is_a_new_message() {
        let mut inbound = Inbound::new(4);
        for dup in [false, true] {
            let received = inbound
                .on_publish(&publish(QoS::AtLeastOnce, Some(1), dup))
                .expect("accepted");
            assert!(received.deliver, "dup = {dup}");
            assert!(matches!(received.answer, Some(Packet::PubAck(_))));
        }
        assert_eq!(inbound.outstanding(), 0, "each was answered and forgotten");
    }

    /// The one promise the level makes. A bridge that delivered the
    /// repeat would publish the message into the local broker twice,
    /// which looks correct from the outside until somebody counts.
    #[test]
    fn an_exactly_once_repeat_is_answered_again_and_delivered_once() {
        let mut inbound = Inbound::new(4);
        let first = inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(9), false))
            .expect("accepted");
        assert!(first.deliver);
        assert!(matches!(first.answer, Some(Packet::PubRec(_))));

        let repeat = inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(9), true))
            .expect("accepted");
        assert!(!repeat.deliver, "this end already has it");
        assert!(
            matches!(repeat.answer, Some(Packet::PubRec(_))),
            "and must say so again, or the remote resends for ever"
        );
        assert_eq!(inbound.outstanding(), 1, "the repeat took no second slot");

        let completed = inbound.on_pubrel(&ack(9));
        assert!(matches!(&completed, Packet::PubComp(a) if a.reason == ReasonCode::Success));
        assert_eq!(inbound.outstanding(), 0);

        // And once released, the same identifier is a fresh message.
        let again = inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(9), false))
            .expect("accepted");
        assert!(again.deliver);
    }

    /// A release whose completion went missing is resent. Answering
    /// only the first would leave the remote resending for ever.
    #[test]
    fn a_release_for_nothing_held_is_still_completed() {
        let mut inbound = Inbound::new(4);
        let answer = inbound.on_pubrel(&ack(3));
        let Packet::PubComp(ack) = answer else {
            panic!("a release is answered with a completion");
        };
        assert_eq!(ack.packet_id, 3);
        assert_eq!(ack.reason, ReasonCode::PacketIdentifierNotFound);
    }

    /// [MQTT-3.3.4-9]: a remote that exceeds the number this end
    /// advertised is disconnected, not answered.
    #[test]
    fn a_remote_that_exceeds_what_we_advertised_is_disconnected() {
        let mut inbound = Inbound::new(2);
        inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(1), false))
            .expect("first");
        inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(2), false))
            .expect("second");
        assert_eq!(
            inbound.on_publish(&publish(QoS::ExactlyOnce, Some(3), false)),
            Err(ReasonCode::ReceiveMaximumExceeded)
        );

        // A repeat of one already held is not a third.
        inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(1), true))
            .expect("already counted");
    }

    /// Both levels count toward the quota. A count that watched only
    /// the exactly-once exchanges would agree with this test's first
    /// half and let a remote sending at-least-once past the limit.
    #[test]
    fn both_acknowledged_levels_count_toward_the_quota() {
        let mut inbound = Inbound::new(1);
        // Nothing is held after an at-least-once publish is answered,
        // so the way to see it occupy a slot is to fill the window
        // with an exactly-once exchange first.
        inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(1), false))
            .expect("first");
        assert_eq!(
            inbound.on_publish(&publish(QoS::AtLeastOnce, Some(2), false)),
            Err(ReasonCode::ReceiveMaximumExceeded),
            "an at-least-once publish needs a slot like any other"
        );
    }

    #[test]
    fn an_acknowledged_publish_with_no_identifier_is_a_protocol_error() {
        let mut inbound = Inbound::new(4);
        for qos in [QoS::AtLeastOnce, QoS::ExactlyOnce] {
            assert_eq!(
                inbound.on_publish(&publish(qos, None, false)),
                Err(ReasonCode::ProtocolError)
            );
        }
    }

    #[test]
    fn a_receive_maximum_of_zero_is_unbounded() {
        let mut inbound = Inbound::new(0);
        for id in 1..500 {
            inbound
                .on_publish(&publish(QoS::ExactlyOnce, Some(id), false))
                .expect("unbounded");
        }
    }

    #[test]
    fn a_new_connection_starts_with_nothing_outstanding() {
        let mut inbound = Inbound::new(4);
        inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(1), false))
            .expect("accepted");
        inbound.reset();
        assert_eq!(inbound.outstanding(), 0);
    }
}
