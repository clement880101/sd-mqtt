//! What the broker says changed.
//!
//! Section 7.3 of the design: `broker-core` does no I/O, so it does not
//! write. It **says what happened**, in the vocabulary of
//! [`storage::Record`], and the shell writes it.
//!
//! # Where the records are produced
//!
//! Beside the state they describe, not above it. A [`Journal`] sits in
//! [`Outbound`], in [`Inbound`] and in [`Session`], because those are the
//! three places durable state actually changes, and a record written from
//! anywhere else would be the shell's inference wearing the broker's
//! clothes. The assembly drains them into one buffer, in order, and
//! [`Broker::take_journal`] hands that buffer over.
//!
//! [`Outbound`]: crate::session::Outbound
//! [`Inbound`]: crate::session::Inbound
//! [`Session`]: crate::session::Session
//! [`Broker::take_journal`]: crate::broker::Broker::take_journal
//!
//! # Draining is not tidiness
//!
//! A record left in a piece's own buffer is a record the shell has not
//! committed, and section 7.3 has the shell committing before it
//! acknowledges. So every path that changes state drains what it changed,
//! and a piece that is destroyed is drained before it goes, or its records
//! die with it. Both failures are silent in an ordinary test and are
//! exactly what `tests/journal.rs` exists to catch.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use mqtt_codec::packet::RetainHandling as CodecRetainHandling;
use mqtt_codec::{Properties, QoS};
use storage::record::{
    BodyRecord, DeliveryRecord, InflightRecord, InflightState as StoredInflightState,
    MessageId as StoredMessageId, Qos as StoredQos, QueueEntry, Record,
    RetainHandling as StoredRetainHandling, RetainedRecord, SessionKey, SessionRecord,
    SubscriptionRecord,
};

use crate::registry::SessionId;
use crate::retained::RetainedMessage;
use crate::session::inflight::{InflightEntry, InflightState};
use crate::session::message::{Message, MessageId};
use crate::session::queue::Delivery;

/// Records produced and not yet handed over.
///
/// A buffer, which is state rather than I/O, which is what lets the
/// purity that makes this crate testable survive persistence.
#[derive(Debug, Default)]
pub struct Journal {
    records: Vec<Record>,
}

impl Journal {
    /// An empty journal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Say that something happened.
    pub fn push(&mut self, record: Record) {
        self.records.push(record);
    }

    /// Move everything said so far onto the end of `out`, in order.
    pub fn drain_into(&mut self, out: &mut Vec<Record>) {
        out.append(&mut self.records);
    }

    /// How many records are waiting to be handed over.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether nothing is waiting.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// The key a record names, from the identity the broker runs on.
///
/// Section 5.2 identifies a session by its mountpoint and client
/// identifier, which is what [`SessionKey`] holds. The runtime
/// [`crate::subscriptions::SubscriberId`] is deliberately not it: that is
/// an index into a live registry and means nothing once the process it
/// indexed into has gone.
pub fn key_of(id: &SessionId) -> SessionKey {
    SessionKey::new(&id.mountpoint, &id.client_id)
}

/// The stored spelling of a quality of service level.
pub fn stored_qos(qos: QoS) -> StoredQos {
    match qos {
        QoS::AtMostOnce => StoredQos::AtMostOnce,
        QoS::AtLeastOnce => StoredQos::AtLeastOnce,
        QoS::ExactlyOnce => StoredQos::ExactlyOnce,
    }
}

/// The live spelling of a stored quality of service level.
pub fn live_qos(qos: StoredQos) -> QoS {
    match qos {
        StoredQos::AtMostOnce => QoS::AtMostOnce,
        StoredQos::AtLeastOnce => QoS::AtLeastOnce,
        StoredQos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

/// The stored spelling of a retain-handling option.
pub fn stored_retain_handling(handling: CodecRetainHandling) -> StoredRetainHandling {
    match handling {
        CodecRetainHandling::SendAlways => StoredRetainHandling::SendAlways,
        CodecRetainHandling::SendIfNew => StoredRetainHandling::SendIfNew,
        CodecRetainHandling::DoNotSend => StoredRetainHandling::DoNotSend,
    }
}

/// The live spelling of a stored retain-handling option.
pub fn live_retain_handling(handling: StoredRetainHandling) -> CodecRetainHandling {
    match handling {
        StoredRetainHandling::SendAlways => CodecRetainHandling::SendAlways,
        StoredRetainHandling::SendIfNew => CodecRetainHandling::SendIfNew,
        StoredRetainHandling::DoNotSend => CodecRetainHandling::DoNotSend,
    }
}

/// The stored spelling of a message identifier.
pub fn stored_message_id(id: MessageId) -> StoredMessageId {
    StoredMessageId(*id.as_bytes())
}

/// The live spelling of a stored message identifier.
pub fn live_message_id(id: StoredMessageId) -> MessageId {
    MessageId::from_bytes(id.0)
}

/// A property block as the codec encodes it, which is how storage holds
/// one.
///
/// Opaque on the far side on purpose: properties are the codec's format,
/// already specified and already pinned by the codec's own golden
/// vectors, and a second description of them in the storage crate would
/// be a second thing to keep in step.
///
/// The encode cannot fail for a block that arrived through the codec,
/// because convention 1 has the encoder refusing exactly what the decoder
/// rejects, so anything decoded re-encodes. A block assembled by hand
/// that the codec would refuse is a caller's bug and says so.
pub fn stored_properties(properties: &Properties) -> Bytes {
    let mut bytes = Vec::new();
    properties
        .encode(&mut bytes)
        .expect("a property block that arrived through the codec re-encodes");
    Bytes::from(bytes)
}

/// Read back a property block stored by [`stored_properties`].
///
/// Returns the default block for bytes that are not one. Nothing in the
/// broker produces such bytes; a store that holds them is corrupt, which
/// is the storage crate's error to raise and not this function's.
pub fn live_properties(bytes: &Bytes) -> Properties {
    let mut reader = mqtt_codec::reader::Reader::new(bytes);
    Properties::decode(&mut reader).unwrap_or_default()
}

/// The stored spelling of how far an exchange has got.
pub fn stored_inflight_state(state: InflightState) -> StoredInflightState {
    match state {
        InflightState::AwaitingAck => StoredInflightState::AwaitingAck,
        InflightState::AwaitingReceived => StoredInflightState::AwaitingReceived,
        InflightState::AwaitingComplete => StoredInflightState::AwaitingComplete,
    }
}

/// The live spelling of how far an exchange has got.
pub fn live_inflight_state(state: StoredInflightState) -> InflightState {
    match state {
        StoredInflightState::AwaitingAck => InflightState::AwaitingAck,
        StoredInflightState::AwaitingReceived => InflightState::AwaitingReceived,
        StoredInflightState::AwaitingComplete => InflightState::AwaitingComplete,
    }
}

/// A message body as it is stored, once however many sessions hold it.
pub fn body_record(message: &Message) -> BodyRecord {
    BodyRecord {
        topic: message.topic.to_string(),
        payload: message.payload.clone(),
        qos: stored_qos(message.qos),
        retain: message.retain,
        expires_at: message.expires_at,
        properties: stored_properties(&message.properties),
    }
}

/// A stored body, back as the message it came from.
pub fn live_message(id: StoredMessageId, body: &BodyRecord) -> Message {
    Message {
        id: live_message_id(id),
        topic: Arc::from(body.topic.as_str()),
        payload: body.payload.clone(),
        qos: live_qos(body.qos),
        retain: body.retain,
        properties: Arc::new(live_properties(&body.properties)),
        expires_at: body.expires_at,
    }
}

/// What one session needs in order to send one message to one peer.
pub fn delivery_record(item: &Delivery) -> DeliveryRecord {
    DeliveryRecord {
        body: stored_message_id(item.message.id),
        qos: stored_qos(item.qos),
        retain: item.retain,
        subscription_ids: item.subscription_ids.clone(),
        dup: item.dup,
    }
}

/// A stored delivery, back as the one it came from, around `message`.
pub fn live_delivery(
    record: &DeliveryRecord,
    message: Message,
    sequence: u64,
    packet_id: Option<u16>,
) -> Delivery {
    Delivery {
        message,
        qos: live_qos(record.qos),
        retain: record.retain,
        subscription_ids: record.subscription_ids.clone(),
        dup: record.dup,
        packet_id,
        sequence,
    }
}

/// A queued delivery as it is stored.
pub fn queue_entry(item: &Delivery) -> QueueEntry {
    QueueEntry {
        sequence: item.sequence,
        packet_id: item.packet_id,
        delivery: delivery_record(item),
    }
}

/// An outstanding delivery as it is stored.
pub fn inflight_record(entry: &InflightEntry) -> InflightRecord {
    InflightRecord {
        packet_id: entry.packet_id,
        sequence: entry.delivery.sequence,
        state: stored_inflight_state(entry.state),
        sent_at: entry.sent_at,
        delivery: delivery_record(&entry.delivery),
    }
}

/// A stored outstanding delivery, back as the one it came from.
pub fn live_inflight(record: &InflightRecord, message: Message) -> InflightEntry {
    InflightEntry {
        packet_id: record.packet_id,
        delivery: live_delivery(
            &record.delivery,
            message,
            record.sequence,
            Some(record.packet_id),
        ),
        state: live_inflight_state(record.state),
        sent_at: record.sent_at,
    }
}

/// A retained message as it is stored.
pub fn retained_record(message: &RetainedMessage) -> RetainedRecord {
    RetainedRecord {
        payload: message.payload.clone(),
        qos: stored_qos(message.qos),
        expires_at: message.expires_at,
        properties: stored_properties(&message.properties),
    }
}

/// A stored retained message, back as the one it came from.
pub fn live_retained(record: &RetainedRecord) -> RetainedMessage {
    RetainedMessage {
        payload: record.payload.clone(),
        qos: live_qos(record.qos),
        expires_at: record.expires_at,
        properties: Arc::new(live_properties(&record.properties)),
    }
}

/// Everything about a broker that must survive it.
///
/// **Durable state only, and deliberately not everything.** Nothing here
/// says whether a connection is attached, which handle a session holds,
/// what its keep-alive is or how much of its receive quota is spent:
/// section 4.9 says a quota is re-initialised with each network
/// connection, and a handle is an index into a registry that a restart
/// destroys. A comparison that included any of them would report a
/// difference between a running broker and the one rebuilt from its
/// journal, and would be **right to**, which is precisely why they are
/// left out.
///
/// What is here is what section 7.5 says restore rebuilds, in the shape
/// [`storage::Restored`] hands it over in, so that the two can be
/// compared directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableState {
    /// Every session, by key.
    pub sessions: BTreeMap<SessionKey, DurableSession>,
    /// Retained messages, by topic, expired or not: the sweep that
    /// reclaims one is a durable change like any other, so a message
    /// still held here is one the store still holds.
    pub retained: BTreeMap<String, RetainedRecord>,
    /// The bodies the queue and inflight entries reference, one entry per
    /// distinct message however many sessions hold it.
    pub bodies: BTreeMap<StoredMessageId, BodyRecord>,
}

/// One session's durable state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableSession {
    /// Its own fields: version, expiry, will.
    pub session: SessionRecord,
    /// The filters it holds, by filter, which is what identifies one.
    pub subscriptions: BTreeMap<String, SubscriptionRecord>,
    /// Exactly-once identifiers the peer owns and has not released.
    pub inbound: Vec<u16>,
    /// What is outstanding, **in sequence order**, which is the order
    /// the messages arrived in.
    ///
    /// Not the order the live window holds them in, and that is not a
    /// convenience. A window is drained and refilled by a resume, which
    /// puts the entries the peer already owns back first and the ones
    /// being resent after them, so a window that was in arrival order
    /// is not afterwards. Nothing durable records the difference — the
    /// store keys an outstanding entry by the identifier it was sent
    /// under — and nothing depends on it either: what a later resume
    /// needs is the relative order of the entries it will resend, and
    /// [`InflightRecord::sequence`] is exactly that, which is why it is
    /// on the record at all.
    ///
    /// So both sides are put in that order before they are compared. A
    /// difference in the live send order is therefore invisible here,
    /// deliberately, because it is a difference between two brokers
    /// that will behave identically.
    pub inflight: Vec<InflightRecord>,
    /// What waits, in the order the queue holds it.
    pub queue: Vec<QueueEntry>,
    /// The highest sequence number ever assigned to it.
    pub highest_sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_journal_hands_over_what_it_was_told_in_order() {
        let mut journal = Journal::new();
        journal.push(Record::RetainedRemoved { topic: "a".into() });
        journal.push(Record::RetainedRemoved { topic: "b".into() });
        let mut out = vec![Record::RetainedRemoved {
            topic: "before".into(),
        }];
        journal.drain_into(&mut out);
        assert!(journal.is_empty(), "a drained journal holds nothing");
        assert_eq!(
            out,
            vec![
                Record::RetainedRemoved {
                    topic: "before".into()
                },
                Record::RetainedRemoved { topic: "a".into() },
                Record::RetainedRemoved { topic: "b".into() },
            ]
        );
    }

    #[test]
    fn every_quality_of_service_level_survives_the_two_spellings() {
        // The stored discriminants happen to match the wire values, and
        // nothing may rely on that: a mapping written the other way round
        // would round trip and be wrong against every stored file.
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
            assert_eq!(live_qos(stored_qos(qos)), qos);
        }
        assert_eq!(stored_qos(QoS::AtMostOnce), StoredQos::AtMostOnce);
        assert_eq!(stored_qos(QoS::ExactlyOnce), StoredQos::ExactlyOnce);
    }

    #[test]
    fn every_retain_handling_option_survives_the_two_spellings() {
        for handling in [
            CodecRetainHandling::SendAlways,
            CodecRetainHandling::SendIfNew,
            CodecRetainHandling::DoNotSend,
        ] {
            assert_eq!(
                live_retain_handling(stored_retain_handling(handling)),
                handling
            );
        }
        assert_eq!(
            stored_retain_handling(CodecRetainHandling::DoNotSend),
            StoredRetainHandling::DoNotSend
        );
    }

    #[test]
    fn every_exchange_state_survives_the_two_spellings() {
        // Convention 5: a round trip cannot see a consistent error, so
        // the mapping is pinned in one direction as well as round
        // tripped. Transposing awaiting-received and awaiting-complete
        // would round trip perfectly and resume every exactly-once
        // exchange the wrong way: a publish resent where a release was
        // owed delivers a message the peer already has.
        for state in [
            InflightState::AwaitingAck,
            InflightState::AwaitingReceived,
            InflightState::AwaitingComplete,
        ] {
            assert_eq!(live_inflight_state(stored_inflight_state(state)), state);
        }
        assert_eq!(
            stored_inflight_state(InflightState::AwaitingAck),
            StoredInflightState::AwaitingAck
        );
        assert_eq!(
            stored_inflight_state(InflightState::AwaitingReceived),
            StoredInflightState::AwaitingReceived
        );
        assert_eq!(
            stored_inflight_state(InflightState::AwaitingComplete),
            StoredInflightState::AwaitingComplete
        );
    }

    #[test]
    fn a_property_block_survives_the_round_trip_to_storage() {
        let properties = Properties {
            content_type: Some("text/plain".into()),
            user_properties: vec![("a".into(), "b".into())],
            message_expiry_interval: Some(30),
            ..Properties::default()
        };
        assert_eq!(live_properties(&stored_properties(&properties)), properties);
    }

    #[test]
    fn a_message_identifier_survives_the_round_trip_to_storage() {
        let id = MessageId::new();
        assert_eq!(live_message_id(stored_message_id(id)), id);
    }
}
