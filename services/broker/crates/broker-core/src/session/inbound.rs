//! Receiving messages from a client.

use std::collections::HashSet;

use foldhash::fast::RandomState;
use mqtt_codec::packet::{Ack, Publish};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode};

use crate::session::Action;

/// An acknowledgement a client is waiting for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Acknowledgement {
    /// The identifier to answer.
    pub packet_id: u16,
    /// Which acknowledgement the level calls for.
    pub qos: QoS,
}

/// What the caller must do with a publish that has arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Received {
    /// Whether the message goes to subscribers. False for a redelivery
    /// of an exactly-once message already held, which must be answered
    /// again but must not reach a subscriber twice.
    pub dispatch: bool,
    /// What the client is owed once the caller knows the outcome. It is
    /// returned rather than sent so that the reason code can say what
    /// actually happened; see [`Inbound::acknowledge`].
    pub owed: Option<Acknowledgement>,
}

/// Tracks what a client has sent that is not yet complete.
///
/// Only exactly-once delivery needs memory here. At-least-once is
/// acknowledged and forgotten, because the publisher is entitled to send
/// again and the broker is not promising to notice: [MQTT-4.3.2-5]
/// requires a publish carrying an identifier already acknowledged to be
/// treated as a new message whatever its duplicate flag says.
///
/// # What the receive maximum counts
///
/// Both levels, which is what [MQTT-3.3.4-9] requires: a peer may not
/// exceed the receive maximum in "QoS 1 and QoS 2 PUBLISH packets where
/// it has not sent a PUBACK or PUBCOMP in response". Section 4.9 says
/// the same from the sender's side, replenishing the quota on a PUBACK
/// for at-least-once and a PUBCOMP for exactly-once.
///
/// So the count is the sum of `awaiting_ack` and `awaiting_release`. An
/// at-least-once publish occupies a slot from the moment it arrives
/// until [`Inbound::acknowledge`] emits its packet, and an exactly-once
/// one until the completion goes out.
///
/// Counting only the exactly-once exchanges was equivalent while every
/// publish was answered inside `on_publish`. It stopped being so when
/// `acknowledge` became a separate call: a caller that defers would let
/// a client exceed its limit with nothing noticing.
///
/// The two sets differ in lifetime. Section 4.9 says the quota "is not
/// preserved across Network Connections", so `awaiting_ack` is dropped
/// by [`Inbound::on_disconnect`], while `awaiting_release` is session
/// state and survives.
pub struct Inbound {
    /// Identifiers whose at-least-once publish has arrived and whose
    /// acknowledgement has not gone out. Per connection, not session
    /// state: section 4.9 re-initialises the quota on each one.
    awaiting_ack: HashSet<u16, RandomState>,
    /// Identifiers whose publish has arrived and whose release has not.
    awaiting_release: HashSet<u16, RandomState>,
    receive_maximum: usize,
}

impl Inbound {
    /// A receiver permitting `receive_maximum` concurrent exactly-once
    /// exchanges. Zero means unbounded.
    pub fn new(receive_maximum: usize) -> Self {
        Self {
            awaiting_ack: HashSet::with_hasher(RandomState::default()),
            awaiting_release: HashSet::with_hasher(RandomState::default()),
            receive_maximum,
        }
    }

    /// Decide what to do with a publish from the client.
    ///
    /// This emits nothing. The acknowledgement is owed rather than sent,
    /// because its reason code depends on what happens to the message:
    /// the useful answers, and 0x10 no-matching-subscribers above all,
    /// are not knowable until the caller has tried to dispatch it. The
    /// caller acts on [`Received::dispatch`] and then calls
    /// [`Inbound::acknowledge`] with what it learned.
    ///
    /// An error is the reason code the caller must close the connection
    /// with; [MQTT-3.3.4-9] names a disconnect, not a refusal packet, as
    /// the answer to a peer that exceeds the receive maximum.
    pub fn on_publish(&mut self, publish: &Publish) -> Result<Received, ReasonCode> {
        match publish.qos {
            QoS::AtMostOnce => Ok(Received {
                dispatch: true,
                owed: None,
            }),
            QoS::AtLeastOnce => {
                let packet_id = publish.packet_id.ok_or(ReasonCode::ProtocolError)?;
                // A repeat of one still unanswered is already counted,
                // so refusing it here would deadlock the exchange in
                // the same way it would for exactly-once.
                if !self.awaiting_ack.contains(&packet_id) {
                    self.check_quota()?;
                    self.awaiting_ack.insert(packet_id);
                }
                Ok(Received {
                    dispatch: true,
                    owed: Some(Acknowledgement {
                        packet_id,
                        qos: QoS::AtLeastOnce,
                    }),
                })
            }
            QoS::ExactlyOnce => {
                let packet_id = publish.packet_id.ok_or(ReasonCode::ProtocolError)?;
                let owed = Some(Acknowledgement {
                    packet_id,
                    qos: QoS::ExactlyOnce,
                });

                if self.awaiting_release.contains(&packet_id) {
                    // Already held. It is receipted again, because the
                    // first receipt may have been lost, but it must not
                    // reach a subscriber twice: [MQTT-4.3.3-10] requires
                    // exactly that pair. It is also already counted
                    // against the receive maximum, so refusing it here
                    // would deadlock the exchange.
                    return Ok(Received {
                        dispatch: false,
                        owed,
                    });
                }

                self.check_quota()?;
                self.awaiting_release.insert(packet_id);
                Ok(Received {
                    dispatch: true,
                    owed,
                })
            }
        }
    }

    /// Answer a publish, once the caller knows how it went.
    ///
    /// A failing receipt forgets the identifier: [MQTT-4.3.3-9] requires
    /// a later publish carrying it to be treated as a new message rather
    /// than a redelivery, because the broker has said it never took this
    /// one.
    pub fn acknowledge(
        &mut self,
        packet_id: u16,
        qos: QoS,
        reason: ReasonCode,
        out: &mut Vec<Action>,
    ) {
        match qos {
            // Nothing is owed, and nothing may be sent.
            QoS::AtMostOnce => {}
            QoS::AtLeastOnce => {
                // The slot is occupied until the acknowledgement is on
                // the wire, which is this moment and not before.
                self.awaiting_ack.remove(&packet_id);
                out.push(Action::Send(Packet::PubAck(ack(packet_id, reason))));
            }
            QoS::ExactlyOnce => {
                if reason.is_failure() {
                    self.awaiting_release.remove(&packet_id);
                }
                out.push(Action::Send(Packet::PubRec(ack(packet_id, reason))));
            }
        }
    }

    /// Handle a release, completing an exactly-once exchange.
    ///
    /// An identifier the receiver is not holding is answered rather than
    /// ignored, so a peer that lost the completion can make progress.
    /// [MQTT-4.3.3-11] requires a completion in answer to every release,
    /// and the completion reason table holds exactly two values, so 0x92
    /// exists for this case and nothing else.
    pub fn on_pubrel(&mut self, packet_id: u16, out: &mut Vec<Action>) {
        let reason = if self.awaiting_release.remove(&packet_id) {
            ReasonCode::Success
        } else {
            ReasonCode::PacketIdentifierNotFound
        };
        out.push(Action::Send(Packet::PubComp(ack(packet_id, reason))));
    }

    /// Whether another publish would exceed the receive maximum.
    ///
    /// Zero means unbounded. The error is the reason code the caller
    /// must close the connection with.
    fn check_quota(&self) -> Result<(), ReasonCode> {
        if self.receive_maximum != 0 && self.quota_used() >= self.receive_maximum {
            return Err(ReasonCode::ReceiveMaximumExceeded);
        }
        Ok(())
    }

    /// The connection went away.
    ///
    /// At-least-once publishes left unanswered stop counting: section
    /// 4.9 says the quota "is not preserved across Network Connections,
    /// and \[is\] re-initialized with each new Network Connection", and
    /// a client that gets no acknowledgement resends the publish on the
    /// next one. Exactly-once exchanges are session state and are kept,
    /// because a redelivery must still be recognised as one.
    pub fn on_disconnect(&mut self) {
        self.awaiting_ack.clear();
    }

    /// Claim an identifier for state restored on resume.
    pub fn reserve(&mut self, packet_id: u16) {
        self.awaiting_release.insert(packet_id);
    }

    /// Every identifier currently held, for persistence.
    pub fn held(&self) -> impl Iterator<Item = u16> + '_ {
        self.awaiting_release.iter().copied()
    }

    /// How many exactly-once exchanges are incomplete.
    pub fn outstanding(&self) -> usize {
        self.awaiting_release.len()
    }

    /// How many publishes occupy the receive maximum right now: every
    /// at-least-once one not yet answered, and every exactly-once one
    /// not yet completed.
    ///
    /// Exposed so the limit can be asserted rather than assumed.
    pub fn quota_used(&self) -> usize {
        self.awaiting_ack.len() + self.awaiting_release.len()
    }

    /// Forget everything, for a session that is ending.
    pub fn clear(&mut self) {
        self.awaiting_ack.clear();
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

    /// The whole of what a caller does: decide, then answer with success.
    fn accept(inbound: &mut Inbound, publish: &Publish, out: &mut Vec<Action>) -> bool {
        let received = inbound.on_publish(publish).expect("accepted");
        if let Some(owed) = received.owed {
            inbound.acknowledge(owed.packet_id, owed.qos, ReasonCode::Success, out);
        }
        received.dispatch
    }

    #[test]
    fn fire_and_forget_is_dispatched_and_not_acknowledged() {
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();
        assert!(accept(
            &mut inbound,
            &publish(QoS::AtMostOnce, None, false),
            &mut actions
        ));
        assert!(acks(&actions).is_empty());
    }

    #[test]
    fn at_least_once_is_dispatched_and_acknowledged() {
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();
        assert!(accept(
            &mut inbound,
            &publish(QoS::AtLeastOnce, Some(5), false),
            &mut actions
        ));
        assert!(matches!(acks(&actions)[0], Packet::PubAck(a) if a.packet_id == 5));
    }

    #[test]
    fn at_least_once_is_dispatched_again_on_a_redelivery() {
        // At-least-once means exactly that: the broker does not dedupe,
        // because the publisher is entitled to a second attempt.
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();
        assert!(accept(
            &mut inbound,
            &publish(QoS::AtLeastOnce, Some(5), false),
            &mut actions
        ));
        assert!(accept(
            &mut inbound,
            &publish(QoS::AtLeastOnce, Some(5), true),
            &mut actions
        ));
    }

    #[test]
    fn exactly_once_is_dispatched_once_and_receipted_twice() {
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();

        assert!(accept(
            &mut inbound,
            &publish(QoS::ExactlyOnce, Some(7), false),
            &mut actions
        ));
        assert_eq!(inbound.outstanding(), 1);

        actions.clear();
        assert!(
            !accept(
                &mut inbound,
                &publish(QoS::ExactlyOnce, Some(7), true),
                &mut actions
            ),
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
        accept(
            &mut inbound,
            &publish(QoS::ExactlyOnce, Some(7), false),
            &mut actions,
        );

        actions.clear();
        inbound.on_pubrel(7, &mut actions);
        assert!(matches!(
            acks(&actions)[0],
            Packet::PubComp(a) if a.packet_id == 7 && a.reason == ReasonCode::Success
        ));
        assert_eq!(inbound.outstanding(), 0);

        // The identifier is free again, so the same one may be reused.
        actions.clear();
        assert!(accept(
            &mut inbound,
            &publish(QoS::ExactlyOnce, Some(7), false),
            &mut actions
        ));
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
        accept(
            &mut inbound,
            &publish(QoS::ExactlyOnce, Some(1), false),
            &mut actions,
        );
        accept(
            &mut inbound,
            &publish(QoS::ExactlyOnce, Some(2), false),
            &mut actions,
        );

        assert_eq!(
            inbound.on_publish(&publish(QoS::ExactlyOnce, Some(3), false)),
            Err(ReasonCode::ReceiveMaximumExceeded)
        );
    }

    #[test]
    fn a_deferred_at_least_once_acknowledgement_counts_against_the_receive_maximum() {
        // The case the split made reachable. The caller has not answered
        // either publish yet, so both still occupy a slot; without this
        // the client could send without limit and nothing would notice.
        let mut inbound = Inbound::new(2);
        inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(1), false))
            .expect("the first fits");
        inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(2), false))
            .expect("the second fits");
        assert_eq!(inbound.quota_used(), 2);

        assert_eq!(
            inbound.on_publish(&publish(QoS::AtLeastOnce, Some(3), false)),
            Err(ReasonCode::ReceiveMaximumExceeded)
        );
    }

    #[test]
    fn the_receive_maximum_counts_the_two_levels_together() {
        // [MQTT-3.3.4-9] counts "QoS 1 and QoS 2 PUBLISH packets", not
        // one or the other.
        let mut inbound = Inbound::new(2);
        let mut actions = Vec::new();
        inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(1), false))
            .expect("the first fits");
        accept(
            &mut inbound,
            &publish(QoS::ExactlyOnce, Some(2), false),
            &mut actions,
        );
        assert_eq!(inbound.quota_used(), 2);

        assert_eq!(
            inbound.on_publish(&publish(QoS::ExactlyOnce, Some(3), false)),
            Err(ReasonCode::ReceiveMaximumExceeded)
        );
    }

    #[test]
    fn answering_an_at_least_once_publish_frees_its_slot() {
        let mut inbound = Inbound::new(1);
        let mut actions = Vec::new();
        let received = inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(1), false))
            .expect("it fits");
        assert_eq!(inbound.quota_used(), 1);

        let owed = received.owed.expect("an acknowledgement is owed");
        inbound.acknowledge(owed.packet_id, owed.qos, ReasonCode::Success, &mut actions);
        assert_eq!(
            inbound.quota_used(),
            0,
            "the slot is free once it is on the wire"
        );

        assert!(inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(2), false))
            .is_ok());
    }

    #[test]
    fn an_unanswered_at_least_once_redelivery_does_not_take_a_second_slot() {
        // It is already counted, and refusing it would deadlock the
        // exchange in the same way it would for exactly-once.
        let mut inbound = Inbound::new(1);
        inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(1), false))
            .expect("it fits");
        assert!(inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(1), true))
            .is_ok());
        assert_eq!(inbound.quota_used(), 1);
    }

    #[test]
    fn a_disconnect_frees_the_at_least_once_slots_but_keeps_the_exactly_once_ones() {
        // Section 4.9: the quota is not preserved across connections. A
        // client that got no acknowledgement resends on the next one.
        // Exactly-once state is session state and must survive, or a
        // redelivery comes through as a fresh message.
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();
        inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(1), false))
            .expect("it fits");
        accept(
            &mut inbound,
            &publish(QoS::ExactlyOnce, Some(2), false),
            &mut actions,
        );
        assert_eq!(inbound.quota_used(), 2);

        inbound.on_disconnect();

        assert_eq!(inbound.quota_used(), 1, "only the exactly-once one is left");
        assert_eq!(inbound.outstanding(), 1);
        actions.clear();
        assert!(
            !accept(
                &mut inbound,
                &publish(QoS::ExactlyOnce, Some(2), true),
                &mut actions
            ),
            "and it still recognises the redelivery"
        );
    }

    #[test]
    fn a_redelivery_does_not_count_against_the_receive_maximum() {
        // It is already counted; refusing it would deadlock the exchange.
        let mut inbound = Inbound::new(1);
        let mut actions = Vec::new();
        accept(
            &mut inbound,
            &publish(QoS::ExactlyOnce, Some(1), false),
            &mut actions,
        );
        assert!(inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(1), true))
            .is_ok());
    }

    #[test]
    fn held_identifiers_survive_a_resume() {
        // Exactly-once state outlives the connection; discarding it would
        // let a redelivery through as a fresh message.
        let mut inbound = Inbound::new(10);
        inbound.reserve(7);
        let mut actions = Vec::new();
        assert!(!accept(
            &mut inbound,
            &publish(QoS::ExactlyOnce, Some(7), true),
            &mut actions
        ));
    }

    #[test]
    fn the_acknowledgement_carries_the_reason_the_caller_supplies() {
        // The point of the split. A broker cannot know there are no
        // matching subscribers until it has tried to dispatch, so the
        // reason code cannot be decided in the same call that asks it to.
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();

        let received = inbound
            .on_publish(&publish(QoS::AtLeastOnce, Some(4), false))
            .expect("accepted");
        let owed = received.owed.expect("an acknowledgement is owed");
        inbound.acknowledge(
            owed.packet_id,
            owed.qos,
            ReasonCode::NoMatchingSubscribers,
            &mut actions,
        );

        assert!(matches!(
            acks(&actions)[0],
            Packet::PubAck(a) if a.reason == ReasonCode::NoMatchingSubscribers
        ));
    }

    #[test]
    fn a_failing_receipt_forgets_the_identifier() {
        // [MQTT-4.3.3-9]: once the broker has said it never took the
        // message, a later publish under that identifier is a new
        // message, not a redelivery, and must be dispatched.
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();

        let received = inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(7), false))
            .expect("accepted");
        let owed = received.owed.expect("an acknowledgement is owed");
        inbound.acknowledge(
            owed.packet_id,
            owed.qos,
            ReasonCode::QuotaExceeded,
            &mut actions,
        );
        assert_eq!(inbound.outstanding(), 0, "the exchange never started");

        actions.clear();
        assert!(
            accept(
                &mut inbound,
                &publish(QoS::ExactlyOnce, Some(7), true),
                &mut actions
            ),
            "a later publish under that identifier is a new message"
        );
    }

    #[test]
    fn a_successful_receipt_keeps_the_identifier() {
        let mut inbound = Inbound::new(10);
        let mut actions = Vec::new();
        let received = inbound
            .on_publish(&publish(QoS::ExactlyOnce, Some(7), false))
            .expect("accepted");
        let owed = received.owed.expect("an acknowledgement is owed");
        inbound.acknowledge(owed.packet_id, owed.qos, ReasonCode::Success, &mut actions);
        assert_eq!(inbound.outstanding(), 1);
    }
}
