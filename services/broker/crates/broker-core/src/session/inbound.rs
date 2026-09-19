//! Receiving messages from a client.

use std::collections::HashSet;

use foldhash::fast::RandomState;
use mqtt_codec::packet::{Ack, Publish};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode};

use crate::session::Action;

/// Tracks what a client has sent that is not yet complete.
///
/// Only exactly-once delivery needs memory here. At-least-once is
/// acknowledged and forgotten, because the publisher is entitled to send
/// again and the broker is not promising to notice: [MQTT-4.3.2-5]
/// requires a publish carrying an identifier already acknowledged to be
/// treated as a new message whatever its duplicate flag says.
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
    /// lost, but it must not reach a subscriber twice. [MQTT-4.3.3-10]
    /// requires exactly that pair.
    ///
    /// An error is the reason code the caller must close the connection
    /// with; [MQTT-3.3.4-9] names a disconnect, not a refusal packet, as
    /// the answer to a peer that exceeds the receive maximum.
    pub fn on_publish(
        &mut self,
        publish: &Publish,
        out: &mut Vec<Action>,
    ) -> Result<bool, ReasonCode> {
        match publish.qos {
            QoS::AtMostOnce => Ok(true),
            QoS::AtLeastOnce => {
                let packet_id = publish.packet_id.ok_or(ReasonCode::ProtocolError)?;
                out.push(Action::Send(Packet::PubAck(ack(
                    packet_id,
                    ReasonCode::Success,
                ))));
                Ok(true)
            }
            QoS::ExactlyOnce => {
                let packet_id = publish.packet_id.ok_or(ReasonCode::ProtocolError)?;

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

                if self.receive_maximum != 0 && self.awaiting_release.len() >= self.receive_maximum
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
