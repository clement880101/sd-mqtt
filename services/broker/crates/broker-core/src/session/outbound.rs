//! Deciding what goes on the wire, and when.

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
        self.report(out);
        self.pump(now, out);
    }

    /// Send whatever the window has room for.
    fn pump(&mut self, now: u32, out: &mut Vec<Action>) {
        while self.online && self.inflight.has_room() {
            self.scratch.clear();
            let next = self.queue.pop(now, &mut self.scratch);
            self.report(out);
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

        let packet_id = match item.packet_id {
            // A redelivery keeps the identifier it was first sent with,
            // which is still reserved because the resume did not release
            // it. Section 4.4 requires this: the peer is waiting on that
            // identifier, and a fresh one makes the redelivery look like
            // an unrelated message.
            Some(reserved) => reserved,
            None => {
                let Some(fresh) = self.packet_ids.allocate() else {
                    // Every identifier is outstanding. Put it back and
                    // wait for an acknowledgement to free one; this is a
                    // stall, not a loss.
                    self.queue.requeue(item);
                    return false;
                };
                fresh
            }
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
    ///
    /// A receipt for an identifier that is not outstanding is answered
    /// with a release carrying `PacketIdentifierNotFound`, not refused.
    /// [MQTT-4.3.3-4] requires a release in answer to any receipt whose
    /// reason is below 0x80, and the release reason table holds exactly
    /// two values, so 0x92 exists for this case and nothing else. Sending
    /// nothing would leave the peer waiting for a release that never
    /// comes, which is worse than a protocol error because nothing
    /// reports it.
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

        let Some(entry) = self.inflight.get_mut(packet_id) else {
            // Nothing to track: the exchange is over from our side the
            // moment this goes out. The completion the peer answers with
            // will name an identifier we do not hold either, and is
            // reported to the caller in the same way as any other.
            out.push(Action::Send(Packet::PubRel(Ack {
                packet_id,
                reason: ReasonCode::PacketIdentifierNotFound,
                properties: Properties::default(),
            })));
            return Ok(());
        };
        entry.state = InflightState::AwaitingComplete;
        out.push(Action::Send(Packet::PubRel(Ack {
            packet_id,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        })));
        Ok(())
    }

    /// Handle an exactly-once completion.
    ///
    /// A completion is the last packet of the exchange, so one naming an
    /// identifier that is not outstanding is answered with silence: the
    /// specification defines no response to it, and 0x92 may travel only
    /// on a release or a completion, neither of which follows. It is
    /// reported to the caller to be counted, and nothing more.
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
    /// back to the head of the queue, marked as a redelivery and still
    /// holding the identifier it was first sent with.
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
            // The identifier stays reserved and travels with the message.
            // Releasing it here and allocating a fresh one on the way out
            // would break exactly-once for any peer that received the
            // first copy and lost its receipt.
            let mut delivery = entry.delivery;
            delivery.packet_id = Some(entry.packet_id);
            delivery.dup = true;
            self.queue.requeue(delivery);
        }

        self.pump(now, out);
    }

    /// Discard everything, reporting each message.
    pub fn clear(&mut self, reason: DropReason, out: &mut Vec<Action>) {
        self.scratch.clear();
        self.queue.clear(reason, &mut self.scratch);
        self.report(out);
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

    /// Turn everything the queue discarded into drop actions.
    ///
    /// A discarded message that was holding an identifier gives it back.
    /// A resumed message carries its identifier in the queue, so without
    /// this an expiry or an eviction would strand that identifier for the
    /// life of the session.
    fn report(&mut self, out: &mut Vec<Action>) {
        // Taken out so the identifiers can be released while draining;
        // the buffer goes back afterwards, so the hot path still does not
        // allocate.
        let mut scratch = std::mem::take(&mut self.scratch);
        for (item, reason) in scratch.drain(..) {
            if let Some(packet_id) = item.packet_id {
                self.packet_ids.release(packet_id);
            }
            out.push(Action::Dropped {
                id: item.message.id,
                reason,
                qos: item.qos,
                topic: Arc::clone(&item.message.topic),
            });
        }
        self.scratch = scratch;
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
            packet_id: None,
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
    fn a_receipt_for_an_unknown_identifier_is_answered_with_a_release() {
        // The release reason table holds exactly two values, so 0x92
        // exists for this case and nothing else. Answering with an error
        // and no packet would leave the peer waiting for a release that
        // never comes, which is worse than a protocol error because
        // nothing reports it.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.on_pubrec(99, ReasonCode::Success, 0, &mut actions)
            .expect("an unknown identifier is answered, not refused");

        let Some(Action::Send(Packet::PubRel(ack))) = actions.first() else {
            panic!("a release should have been sent, got {actions:?}");
        };
        assert_eq!(ack.packet_id, 99);
        assert_eq!(ack.reason, ReasonCode::PacketIdentifierNotFound);
        assert_eq!(out.inflight_len(), 0, "there is nothing to track");
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
    fn resume_keeps_the_identifier_an_at_least_once_message_was_sent_with() {
        // Section 4.4 requires the resumed publish to carry its original
        // identifier. A fresh one leaves the peer holding the first one
        // forever, and the broker will eventually hand that same value to
        // an unrelated message which the peer then discards as a repeat.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        let first = sent(&actions)[0].packet_id.expect("an identifier");

        out.set_online(false);
        actions.clear();
        out.on_resume(0, &mut actions);

        assert_eq!(
            sent(&actions)[0].packet_id,
            Some(first),
            "the redelivery must name the identifier the peer is waiting on"
        );
    }

    #[test]
    fn resume_keeps_the_identifier_an_exactly_once_message_was_sent_with() {
        // The same, and worse: a peer that received the publish and lost
        // the receipt is holding this identifier awaiting a release. A
        // redelivery under a new one has no record on the peer, so the
        // payload reaches the application twice and exactly-once is
        // broken by an ordinary dropped connection.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let first = sent(&actions)[0].packet_id.expect("an identifier");

        out.set_online(false);
        actions.clear();
        out.on_resume(0, &mut actions);

        let resent = sent(&actions);
        assert_eq!(resent.len(), 1);
        assert_eq!(resent[0].packet_id, Some(first));
        assert!(resent[0].dup, "and it is still a redelivery");
    }

    #[test]
    fn an_identifier_held_for_a_redelivery_is_freed_if_the_message_is_dropped() {
        // The resumed message keeps its identifier while it waits in the
        // queue. If it never goes out, the identifier must come back:
        // otherwise every interrupted delivery leaks one, and a long
        // lived session runs out.
        let mut out = online(10);
        let mut actions = Vec::new();

        let mut item = delivery(QoS::AtLeastOnce, b"x");
        item.message.expires_at = Some(5);
        out.offer(item, 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.expect("an identifier");

        out.set_online(false);
        actions.clear();
        // Resume after the deadline: the message is requeued holding its
        // identifier, then discarded on the way out.
        out.on_resume(9, &mut actions);

        assert!(sent(&actions).is_empty(), "it expired instead of going out");
        assert!(matches!(
            actions.first(),
            Some(Action::Dropped {
                reason: DropReason::Expired,
                ..
            })
        ));

        assert!(
            !out.packet_ids.is_in_use(packet_id),
            "the identifier it was holding must have come back"
        );
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
    fn clearing_reports_every_message_it_discards() {
        // Both halves must be accounted for: what is waiting and what is
        // already on the wire. A message that disappears without a drop
        // action is a support case nobody can answer.
        let mut out = online(1);
        let mut actions = Vec::new();
        let sent_one = delivery(QoS::AtLeastOnce, b"1");
        let queued_one = delivery(QoS::AtLeastOnce, b"2");
        let expected = vec![sent_one.message.id, queued_one.message.id];
        out.offer(sent_one, 0, &mut actions);
        out.offer(queued_one, 0, &mut actions);
        assert_eq!(out.inflight_len(), 1);
        assert_eq!(out.queued_len(), 1);

        actions.clear();
        out.clear(DropReason::SessionEnded, &mut actions);

        let mut reported: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Dropped { id, reason, .. } => {
                    assert_eq!(*reason, DropReason::SessionEnded);
                    Some(*id)
                }
                _ => None,
            })
            .collect();
        reported.sort();
        let mut expected = expected;
        expected.sort();
        assert_eq!(reported, expected, "every message must be reported");
        assert_eq!(out.queued_len(), 0);
        assert_eq!(out.inflight_len(), 0);
    }

    #[test]
    fn disconnecting_stops_anything_further_going_out() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"1"), 0, &mut actions);
        assert_eq!(sent(&actions).len(), 1);

        out.on_disconnect();
        actions.clear();
        out.offer(delivery(QoS::AtLeastOnce, b"2"), 0, &mut actions);

        assert!(sent(&actions).is_empty(), "nobody is attached");
        assert_eq!(out.queued_len(), 1, "it waits instead");
        assert_eq!(out.inflight_len(), 1, "and the window is held, not lost");
    }

    #[test]
    fn the_subscription_identifiers_are_echoed_on_the_packet() {
        // This is how an MQTT 5 client learns which of its subscriptions
        // matched, and the identifiers differ per subscriber, which is
        // why the property block is cloned per delivery.
        let mut out = online(10);
        let mut actions = Vec::new();
        let mut item = delivery(QoS::AtLeastOnce, b"x");
        item.subscription_ids = vec![7, 9];
        out.offer(item, 0, &mut actions);

        let published = sent(&actions);
        assert_eq!(published.len(), 1);
        assert_eq!(
            published[0].properties.subscription_identifiers,
            vec![7, 9],
            "both identifiers, in order"
        );
    }

    #[test]
    fn a_message_that_expired_while_waiting_is_dropped_rather_than_sent() {
        let mut out = Outbound::new(QueueLimits::default(), 10);
        let mut actions = Vec::new();
        let mut item = delivery(QoS::AtLeastOnce, b"stale");
        item.message.expires_at = Some(100);
        let id = item.message.id;
        out.offer(item, 0, &mut actions);
        assert_eq!(out.queued_len(), 1, "it waits while nobody is attached");

        actions.clear();
        out.on_resume(101, &mut actions);

        assert!(sent(&actions).is_empty(), "the deadline has passed");
        assert!(
            actions.iter().any(|a| matches!(
                a,
                Action::Dropped {
                    id: dropped,
                    reason: DropReason::Expired,
                    ..
                } if *dropped == id
            )),
            "the drop must be reported with its reason, got {actions:?}"
        );
        assert_eq!(out.queued_len(), 0);
    }

    #[test]
    fn narrowing_the_window_lets_fewer_out() {
        // A peer may advertise a smaller receive maximum than the default.
        let mut out = online(10);
        out.set_inflight_limit(1);
        let mut actions = Vec::new();
        for _ in 0..3 {
            out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        }

        assert_eq!(sent(&actions).len(), 1, "only the narrowed window");
        assert_eq!(out.inflight_len(), 1);
        assert_eq!(out.queued_len(), 2);
    }

    #[test]
    fn an_acknowledgement_for_an_unknown_identifier_is_reported() {
        let mut out = online(10);
        let mut actions = Vec::new();
        assert!(out.on_puback(99, 0, &mut actions).is_err());
    }
}
