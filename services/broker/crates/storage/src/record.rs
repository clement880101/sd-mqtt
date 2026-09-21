//! What a durable fact is, and what it looks like on disk.
//!
//! # The encoding is a wire format
//!
//! These bytes outlive the process that wrote them. A round trip inside one
//! build cannot see that: an encoder and a decoder that agree on a wrong
//! layout agree with each other forever, and the failure surfaces as a
//! broker that starts cleanly and misreads every session it stored last
//! week. Convention 5 records the same hazard in the codec's property
//! identifiers, where every round-trip test passed against transposed
//! values.
//!
//! So the guard here is [`tests::golden`]: byte vectors written by hand from
//! postcard's format rules, never captured from the encoder. The round-trip
//! property beside them catches the cases the vectors do not enumerate; it
//! does not catch a consistently wrong layout, and is not asked to.
//!
//! [`FORMAT_VERSION`] is the second half of the guard. A reader that meets a
//! version it does not know refuses, loudly, rather than guessing.
//!
//! # postcard's rules, as this module relies on them
//!
//! Taken from `postcard` 1.1.3's serializer, not from memory:
//!
//! - `u8` and `bool` are one byte; `bool` is `0x00` or `0x01`.
//! - `u16`, `u32`, `u64` and `usize` are LEB128 varints: seven bits a byte,
//!   least significant group first, high bit set on every byte but the last.
//! - An enum is its variant index as a `u32` varint, then the variant's
//!   fields. A unit variant is the index alone.
//! - A struct is its fields in declaration order, with no length prefix and
//!   no names. **Field order is the format.**
//! - `Option` is `0x00` for `None`, or `0x01` followed by the value.
//! - `String`, `&str` and byte sequences are a `usize` varint length then
//!   the bytes. A `Vec<T>` is a varint count then the elements.
//! - A fixed-size array is its elements with no length prefix.
//!
//! [`tests::golden`]: self::tests::golden

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// The version of the on-disk encoding this build writes and reads.
///
/// Written once per database, not once per record, and checked before
/// anything else is read. Bump it in the same commit as any change to a
/// record type's fields, their order, their types, or the order of the
/// variants of [`Record`] or any enum below.
pub const FORMAT_VERSION: u16 = 1;

/// What every sd-mqtt store begins with, so a file that is not one is
/// refused as such rather than as a corrupt store.
pub const FORMAT_MAGIC: [u8; 8] = *b"sdmqttst";

/// The marker written once per database: what this file is, and which
/// encoding wrote it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatMarker {
    /// Always [`FORMAT_MAGIC`].
    pub magic: [u8; 8],
    /// The encoding the store was written with.
    pub version: u16,
}

impl FormatMarker {
    /// The marker this build writes.
    pub fn current() -> Self {
        Self {
            magic: FORMAT_MAGIC,
            version: FORMAT_VERSION,
        }
    }

    /// Encode the marker.
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("a marker is two fixed-size fields and cannot overflow")
    }

    /// Read a marker and decide whether this build may go on.
    ///
    /// Refuses three things, each distinctly: bytes that are not a marker,
    /// a file that is not one of ours, and a version this build does not
    /// know. The third is the one that matters: a store written by a later
    /// build must stop the broker, because reading it with the wrong layout
    /// would restore nonsense from a file that is not damaged at all.
    ///
    /// A version *lower* than ours is refused too. Nothing has been
    /// released yet, so there is no migration to be compatible with, and
    /// accepting an older version would mean claiming a compatibility that
    /// has never been tested. When migrations exist this becomes a range.
    pub fn read(bytes: &[u8]) -> Result<Self, FormatError> {
        let marker: Self = decode_exact(bytes)?;
        if marker.magic != FORMAT_MAGIC {
            return Err(FormatError::NotAStore);
        }
        if marker.version != FORMAT_VERSION {
            return Err(FormatError::UnknownVersion {
                found: marker.version,
                expected: FORMAT_VERSION,
            });
        }
        Ok(marker)
    }
}

/// What can be wrong with stored bytes.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FormatError {
    /// The store was written by an encoding this build does not know.
    ///
    /// Names both versions, because "refuses to start" is only useful to an
    /// operator who can see which build wrote the file.
    #[error("store format version {found} is not readable by this build, which reads version {expected}")]
    UnknownVersion {
        /// The version the store carries.
        found: u16,
        /// The version this build reads, [`FORMAT_VERSION`].
        expected: u16,
    },
    /// The bytes do not begin with [`FORMAT_MAGIC`].
    #[error("not an sd-mqtt store: the format marker is missing")]
    NotAStore,
    /// The bytes could not be decoded at all.
    #[error("stored bytes could not be decoded: {0}")]
    Undecodable(postcard::Error),
    /// The storage engine reported its own structures as damaged.
    ///
    /// Not something this module can produce: a backend raises it when
    /// the engine underneath refuses bytes it wrote itself. It is here
    /// rather than beside the backend so that damage found by the engine
    /// and damage found by this decoder reach an operator as the same
    /// thing, because the answer to both is the same: restore a backup.
    #[error("the storage engine reported damage: {0}")]
    Damaged(String),
    /// The bytes decoded, and then there were more of them.
    ///
    /// Rejected rather than ignored. postcard's `from_bytes` stops when the
    /// value is complete and says nothing about what follows, so a record
    /// written by a longer layout would decode as a shorter one and look
    /// perfectly healthy.
    #[error("{extra} trailing byte(s) after a complete value")]
    Trailing {
        /// How many bytes were left over.
        extra: usize,
    },
}

/// Decode a value, and require that it used every byte it was given.
pub(crate) fn decode_exact<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, FormatError> {
    let (value, rest) = postcard::take_from_bytes(bytes).map_err(FormatError::Undecodable)?;
    if !rest.is_empty() {
        return Err(FormatError::Trailing { extra: rest.len() });
    }
    Ok(value)
}

/// What identifies a session across a restart.
///
/// The mountpoint and the client identifier, which is what section 5.2 of
/// the design says identifies a session. Deliberately not the runtime
/// `SubscriberId`: that is an index into a live registry and means nothing
/// once the process it indexed into has gone.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SessionKey {
    /// The listener's namespace, empty for the default.
    pub mountpoint: String,
    /// The identifier the client chose, or one assigned to it.
    pub client_id: String,
}

impl SessionKey {
    /// Build a key.
    pub fn new(mountpoint: &str, client_id: &str) -> Self {
        Self {
            mountpoint: mountpoint.to_owned(),
            client_id: client_id.to_owned(),
        }
    }
}

/// Identifies a stored message body.
///
/// Sixteen opaque bytes, which is what `broker-core`'s `MessageId` already
/// exposes for exactly this purpose. This crate does not depend on the uuid
/// crate to say so, and does not interpret them.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default,
)]
pub struct MessageId(pub [u8; 16]);

/// A quality of service level.
///
/// An enum rather than a `u8` because convention 1 says the writer must
/// refuse what the reader rejects: a `u8` field would let the encoder store
/// a 3 that the decoder has no meaning for. The variant indices match the
/// MQTT wire values, which is a convenience and not a coincidence to rely
/// on: the encoding is pinned by the golden vectors either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Qos {
    /// Fire and forget.
    AtMostOnce,
    /// Acknowledged.
    AtLeastOnce,
    /// Acknowledged in four steps.
    ExactlyOnce,
}

/// When a subscription wants retained messages sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetainHandling {
    /// On every subscribe.
    SendAlways,
    /// Only when the subscription did not already exist.
    SendIfNew,
    /// Never.
    DoNotSend,
}

/// The protocol a session speaks.
///
/// The stored discriminants are 0 and 1, which are **not** the MQTT
/// protocol level bytes 4 and 5. Nothing may read one as the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolVersion {
    /// MQTT 3.1.1.
    V311,
    /// MQTT 5.
    V5,
}

/// How far an exactly-once or at-least-once exchange has got.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InflightState {
    /// Sent at least-once, waiting for the acknowledgement.
    AwaitingAck,
    /// Sent exactly-once, waiting for the receipt.
    AwaitingReceived,
    /// Receipt seen and release sent, waiting for completion. A message
    /// here is resumed by resending the release, not the publish.
    AwaitingComplete,
}

/// A message published on a client's behalf if it disappears.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WillRecord {
    /// Where to publish it.
    pub topic: String,
    /// What to publish.
    pub payload: Bytes,
    /// At what level.
    pub qos: Qos,
    /// Whether to retain it.
    pub retain: bool,
    /// Seconds to wait first, MQTT 5 only.
    pub delay: u32,
    /// The MQTT property block, as the codec encodes it.
    ///
    /// Opaque here on purpose. Properties are the codec's format, already
    /// specified and already pinned by the codec's own golden vectors, and
    /// a second description of them in this crate would be a second thing
    /// to keep in step. The delay above is deliberately *not* in here: a
    /// PUBLISH may not carry one and the encoder refuses to write one.
    pub properties: Bytes,
}

/// A session, and the durable fields that outlive its connection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecord {
    /// What identifies it.
    pub key: SessionKey,
    /// The protocol it last connected with.
    pub version: ProtocolVersion,
    /// The session expiry interval it asked for, in seconds.
    pub expiry_interval: u32,
    /// When it may be destroyed, once disconnected. Section 7.5: a session
    /// whose expiry passed while the broker was down is not restored, so
    /// this is read against a clock taken before restore, not after.
    pub expires_at: Option<u32>,
    /// The will, if it is still owed.
    pub will: Option<WillRecord>,
    /// When the will becomes due, if it is owed and delayed.
    pub will_at: Option<u32>,
}

/// One subscription a session holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionRecord {
    /// The topic filter, which is also what identifies the subscription
    /// within its session.
    pub filter: String,
    /// The maximum level to deliver at.
    pub qos: Qos,
    /// Suppress delivery of the subscriber's own publications.
    pub no_local: bool,
    /// Forward the retain flag as published rather than clearing it.
    pub retain_as_published: bool,
    /// When to send retained messages.
    pub retain_handling: RetainHandling,
    /// The MQTT 5 subscription identifier to echo on delivery, if any.
    pub subscription_id: Option<u32>,
}

/// What a session needs in order to send one message to one subscriber.
///
/// Held separately from the body, which several sessions share. This is the
/// per-subscriber part: the same message can be queued at different levels,
/// with different subscription identifiers, for different sessions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryRecord {
    /// The body, stored once and referenced from here.
    pub body: MessageId,
    /// The level to deliver at: the lesser of what the publisher asked for
    /// and what the subscription granted.
    pub qos: Qos,
    /// The retain flag as it should appear on the wire.
    pub retain: bool,
    /// MQTT 5 subscription identifiers to echo back.
    pub subscription_ids: Vec<u32>,
    /// Whether this is a redelivery, which becomes the duplicate flag.
    pub dup: bool,
}

/// A message waiting in a session's queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEntry {
    /// The per-session sequence number, assigned on enqueue and monotonic
    /// within the session. Section 7.4: it is the queue's key, so reading
    /// in key order is reading in arrival order, and it is never reused.
    pub sequence: u64,
    /// The identifier this was already sent under, when it is being
    /// requeued after a disconnection. Section 6 requires a resumed
    /// publish to keep it: a peer that received the first copy is waiting
    /// on that identifier.
    pub packet_id: Option<u16>,
    /// Everything else delivery needs.
    pub delivery: DeliveryRecord,
}

/// A message sent and not yet acknowledged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InflightRecord {
    /// The identifier the peer will acknowledge. Not optional here: a
    /// message in flight has been sent, and a sent message has one.
    pub packet_id: u16,
    /// The sequence number it held in the queue, kept so that resume
    /// restores the window in the order the messages arrived.
    pub sequence: u64,
    /// How far the exchange has got.
    pub state: InflightState,
    /// When it was sent, in coarse seconds.
    pub sent_at: u32,
    /// Everything else delivery needs.
    pub delivery: DeliveryRecord,
}

/// A message body, stored once however many sessions reference it.
///
/// The reference count is not here. It is the store's, incremented by a
/// [`Record::BodyPut`] and decremented by a [`Record::BodyReleased`], and
/// persisted beside the body as section 7.2 requires rather than rebuilt by
/// scanning at startup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BodyRecord {
    /// Where it was published.
    pub topic: String,
    /// The payload, which the broker never interprets.
    pub payload: Bytes,
    /// The level it was published at. A subscriber may receive it lower.
    pub qos: Qos,
    /// Whether the publisher asked for it to be retained.
    pub retain: bool,
    /// A coarse deadline in seconds, past which it must not be delivered.
    pub expires_at: Option<u32>,
    /// The MQTT property block, as the codec encodes it. Opaque here, for
    /// the reason given on [`WillRecord::properties`].
    pub properties: Bytes,
}

/// A message held for future subscribers to a topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetainedRecord {
    /// The payload. An empty payload is a deletion, and never stored.
    pub payload: Bytes,
    /// The level it was published at.
    pub qos: Qos,
    /// A coarse deadline in seconds, past which it is discarded.
    pub expires_at: Option<u32>,
    /// The MQTT property block, as the codec encodes it. Opaque here, for
    /// the reason given on [`WillRecord::properties`].
    pub properties: Bytes,
}

/// One durable fact: something that happened and must survive.
///
/// This is the whole vocabulary the broker has for saying what changed.
/// Section 7.3: the broker does no I/O, so it appends one of these to a
/// journal buffer and the shell writes it.
///
/// **Variant order is the format.** The index of each variant is its
/// discriminant on disk, so a new one goes at the end and a removed one
/// leaves its index spent, or [`FORMAT_VERSION`] is bumped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Record {
    /// A session exists, or its durable fields changed.
    SessionPut(SessionRecord),
    /// A session is gone. Takes its subscriptions, queue and inflight with
    /// it, so nothing else needs to be said about them.
    SessionRemoved {
        /// Whose.
        session: SessionKey,
    },
    /// A session subscribed, or changed the options on a filter it held.
    SubscriptionAdded {
        /// Whose.
        session: SessionKey,
        /// What.
        subscription: SubscriptionRecord,
    },
    /// A session unsubscribed.
    SubscriptionRemoved {
        /// Whose.
        session: SessionKey,
        /// Which filter.
        filter: String,
    },
    /// An exactly-once identifier the peer owns until it releases it.
    InboundReserved {
        /// Whose.
        session: SessionKey,
        /// Which identifier.
        packet_id: u16,
    },
    /// The peer released an exactly-once identifier.
    InboundReleased {
        /// Whose.
        session: SessionKey,
        /// Which identifier.
        packet_id: u16,
    },
    /// A message was sent and awaits acknowledgement, with the identifier
    /// it was sent under. Section 6: a resumed publish keeps that
    /// identifier.
    InflightPut {
        /// Whose.
        session: SessionKey,
        /// What.
        entry: InflightRecord,
    },
    /// An exchange finished, one way or another.
    InflightRemoved {
        /// Whose.
        session: SessionKey,
        /// Which identifier.
        packet_id: u16,
    },
    /// A message joined a session's queue at this sequence number.
    QueuePushed {
        /// Whose.
        session: SessionKey,
        /// What.
        entry: QueueEntry,
    },
    /// A message left it. Sequence numbers are not reused, so the hole
    /// this leaves in the stored keys is expected.
    QueuePopped {
        /// Whose.
        session: SessionKey,
        /// Which sequence number.
        sequence: u64,
    },
    /// A body, stored once however many sessions reference it. Each put is
    /// one reference taken.
    BodyPut {
        /// Which body.
        id: MessageId,
        /// The body itself.
        body: BodyRecord,
    },
    /// One reference released. The body goes when the last one does.
    BodyReleased {
        /// Which body.
        id: MessageId,
    },
    /// A retained message was set on a topic.
    RetainedPut {
        /// Which topic.
        topic: String,
        /// What is retained there.
        message: RetainedRecord,
    },
    /// A retained message was cleared from a topic.
    RetainedRemoved {
        /// Which topic.
        topic: String,
    },
}

impl Record {
    /// Encode a record.
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("encoding into a Vec cannot run out of room")
    }

    /// Decode a record, requiring that it uses every byte given.
    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        decode_exact(bytes)
    }

    /// Roughly how much memory this record pins while it waits to be
    /// written.
    ///
    /// For [`crate::WriterConfig::high_water_bytes`], which is a bound on
    /// the process's memory rather than an accounting of it. Everything a
    /// peer controls the size of is counted exactly — a payload, a
    /// property block, a topic, a filter, a client identifier — and the
    /// rest is the record's own footprint, which is a constant.
    ///
    /// **An estimate, deliberately.** The number it feeds is a choice of
    /// bound, and being out by the header of a `Vec` per record moves
    /// that bound by far less than choosing it differently would. What it
    /// must not do is miss something unbounded, which is why every field
    /// a peer can grow is added and no other field is.
    pub fn weight(&self) -> usize {
        let variable = match self {
            Self::SessionPut(session) => {
                weigh_key(&session.key)
                    + session.will.as_ref().map_or(0, |will| {
                        will.topic.len() + will.payload.len() + will.properties.len()
                    })
            }
            Self::SessionRemoved { session }
            | Self::InboundReserved { session, .. }
            | Self::InboundReleased { session, .. }
            | Self::InflightRemoved { session, .. }
            | Self::QueuePopped { session, .. } => weigh_key(session),
            Self::SubscriptionAdded {
                session,
                subscription,
            } => weigh_key(session) + subscription.filter.len(),
            Self::SubscriptionRemoved { session, filter } => weigh_key(session) + filter.len(),
            Self::InflightPut { session, entry } => {
                weigh_key(session) + weigh_delivery(&entry.delivery)
            }
            Self::QueuePushed { session, entry } => {
                weigh_key(session) + weigh_delivery(&entry.delivery)
            }
            // The one record that carries an application payload, and so
            // the one this whole estimate exists for.
            Self::BodyPut { body, .. } => {
                body.topic.len() + body.payload.len() + body.properties.len()
            }
            Self::BodyReleased { .. } => 0,
            Self::RetainedPut { topic, message } => {
                topic.len() + message.payload.len() + message.properties.len()
            }
            Self::RetainedRemoved { topic } => topic.len(),
        };
        std::mem::size_of::<Self>() + variable
    }
}

/// What a session key's two strings hold.
fn weigh_key(key: &SessionKey) -> usize {
    key.mountpoint.len() + key.client_id.len()
}

/// What a delivery holds beyond its own fields, which is the subscription
/// identifiers a client's own subscriptions put there.
fn weigh_delivery(delivery: &DeliveryRecord) -> usize {
    delivery.subscription_ids.len() * std::mem::size_of::<u32>()
}

#[cfg(test)]
mod tests;
