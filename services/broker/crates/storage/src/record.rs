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

use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// The version of the on-disk encoding this build writes and reads.
///
/// Written once per database, not once per record, and checked before
/// anything else is read. Bump it in the same commit as any change to a
/// record type's fields, their order, their types, or the order of the
/// variants of [`Record`] or any enum below — **and as any change to
/// how a backend lays out what it holds**, which is the same hazard
/// wearing a different hat: a build that reads a healthy file as
/// something other than what was written.
///
/// Version 2 moved the reference count of a body out of the body's own
/// row and into a table of its own. A version 1 store's body rows begin
/// with eight bytes of count, which this build would read as the start
/// of a payload, so they are refused as what they are: a store this
/// build does not read.
///
/// Version 3 added rules, section 9.1: two variants at the end of
/// [`Record`] and a table of their own in each backend. Nothing already
/// stored changes shape, and the version moves all the same, because
/// this check is exact rather than a range — a version 2 store opened by
/// this build would come up with no rules at all and look healthy, and a
/// version 3 store opened by a milestone 3 build would meet two record
/// discriminants it has no meaning for. Nothing is released, so the cost
/// is a line in the release notes rather than a migration.
///
/// Version 4 added the per-rule outbox, section 9.2: three variants at
/// the end of [`Record`], an [`OutboxRecord`] to go in the first of
/// them, and a table and a counter row in each backend. The same
/// argument as version 3 applies with one addition — a version 3 store
/// opened by this build would come up with every rule's outbox empty,
/// which is not an empty outbox but an undelivered message nobody will
/// ever look for again.
///
/// Version 5 added the node's own identity, section 8.1: one variant at
/// the end of [`Record`], a [`NodeIdentityRecord`] to go in it, and one
/// global row in each backend. The same argument again, and the sharpest
/// instance of it yet — **a version 4 store opened by this build holds
/// no identity, so this node would mint a new UUID and stop being the
/// node its peers already know.** Section 8.1 makes that identity the
/// thing singleton ownership and every conflict tie-break order by, so a
/// silent re-founding is not a node that lost a name; it is two answers
/// to "which node" separated by a restart.
///
/// Version 6 added the hybrid logical clock's high-water mark, section
/// 8.2: one variant at the end of [`Record`], a [`ClockRecord`] to go
/// in it, and one global row in each backend. The same argument again,
/// and it is the sharpest instance yet for the same reason version 5
/// was — **a version 5 store opened by this build holds no reading, so
/// this node's clock would resume from zero and re-issue stamps it has
/// already used.** Section 8.2 makes convergence conditional on no two
/// writes ever sharing a stamp, so a clock that restarts behind itself
/// is not a node that lost some counter; it is two values under one key
/// that no later exchange can ever separate.
///
/// Version 7 added the replicated registers themselves, section 8.2:
/// one variant at the end of [`Record`], a [`MetadataRecord`] to go in
/// it with the key, stamp and value types it is made of, and one table
/// in each backend. The same argument as every version above, and the
/// failure a version 6 store would produce is the quietest of the lot
/// — **a node would come up holding no registers at all and looking
/// perfectly healthy**, routing nothing to any peer, serving no
/// replicated rule, and claiming no session it owned. Worse, it would
/// then re-create what it had locally under fresh stamps, which beat
/// the tombstones every other node still holds: a restart that
/// resurrects everything this node ever deleted.
///
/// Version 8 added the session epoch to
/// [`MetadataValueRecord::SessionOwner`], section 8.4. **This is the
/// first version that changes the shape of something already stored**
/// rather than adding a variant at the end, and that is exactly why it
/// moves the version: a version 7 session-owner register is sixteen
/// bytes where this build expects sixteen and a varint, so a reader
/// would run off the end of one record and into the next. The epoch has
/// to be in the value because section 8.4 makes the claiming node write
/// "the epoch it read plus one", and an epoch the register does not
/// carry is one no node can read.
///
/// Version 9 added [`Record::ReplicaFollows`] and
/// [`Record::ReplicaDropped`], section 8.6: this node holding a copy of
/// a session another node owns. Two variants at the end rather than a
/// field on [`SessionRecord`], which would have changed the shape of
/// every session already stored — and which would also have made
/// becoming a session's owner a rewrite of its record rather than the
/// removal of one row.
///
/// **A version 8 store opened by this build holds no replicas**, which
/// is harmless in itself: a node that followed nothing has nothing to
/// restore. The version still moves, because a version 8 *build*
/// reading a version 9 store would meet a variant index it has no arm
/// for, and the check is exact in both directions on purpose.
///
/// Version 10 added [`Record::StreamAt`]: the owner's side of the same
/// stream, which is where it has got to in sending rather than where a
/// follower has got to in applying. **Version 9 existed for one commit
/// of this same unreleased milestone**, and the version moved again
/// rather than being redefined, for exactly the reason the check is
/// exact: a version 9 store is only harmless to this build because the
/// number says which one it is. Redefining a number already written to
/// somebody's disk is how a silent misread happens.
///
/// # Why the owner's position is stored at all
///
/// A follower refuses anything at or below its mark, which is what
/// makes a re-send safe. So an owner that restarted and began counting
/// from one again would have every change it sent refused as a
/// duplicate, silently, until the session had produced as many
/// operations as it had before the restart — a follower that quietly
/// stops following, which is the failure replication exists to
/// prevent.
pub const FORMAT_VERSION: u16 = 11;

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

/// When a rule fires, section 9.1.
///
/// The filter, optionally narrowed. An absent narrowing is not the same
/// as a permissive one: `min_qos: None` says the operator did not narrow
/// by level, and storing it as `Some(AtMostOnce)` would be this crate
/// deciding that for them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerRecord {
    /// The topic filter a message must match.
    pub filter: String,
    /// The lowest level that fires the rule, if it is narrowed.
    pub min_qos: Option<Qos>,
    /// The client identifier pattern that fires it, if it is narrowed.
    ///
    /// The pattern as written, not compiled. What its syntax means
    /// belongs to the crate that evaluates it; storing a compiled form
    /// would put a second description of that syntax on disk, where it
    /// would outlive every build that agreed with it.
    pub client_id: Option<String>,
}

/// How an `http_out` action posts, section 9.2.
///
/// Every field the design lists as configurable — method, headers,
/// timeout, maximum concurrency, retry count and backoff — and no field
/// it does not.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpOutRecord {
    /// Where to post.
    pub url: String,
    /// Which method to use.
    pub method: String,
    /// Headers to send, in the order they were configured.
    pub headers: Vec<(String, String)>,
    /// How long one attempt may take, in milliseconds.
    pub timeout_ms: u32,
    /// How many attempts may be in flight at once.
    pub max_concurrency: u16,
    /// How many times to retry after the first attempt fails.
    pub retries: u8,
    /// How long to wait before the first retry, in milliseconds.
    pub backoff_ms: u32,
}

/// A secret in a record, which will not print itself.
///
/// Section 12.3: passwords and tokens are held in a wrapper that
/// refuses to print itself, so they cannot reach logs or crash output.
/// A record is printed by the storage tests' failure messages and by
/// anything that logs what it is about to write, and an ingest rule's
/// bearer token is a secret that lives in one.
///
/// `transparent` for serde, so the stored bytes are the string's and
/// nothing else: the wrapper is about [`fmt::Debug`] and not about the
/// format.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    /// A secret from the text an operator configured.
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }

    /// The secret itself, for the one comparison that needs it.
    ///
    /// Deliberately not [`fmt::Display`] and deliberately not
    /// `AsRef<str>`: the only way to get the characters out is to name
    /// this method, so a secret cannot reach a formatter by accident.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Whether an operator wrote the key and left it blank.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Not the length either. A length is a real, if small, hint
        // about a secret, and nothing debugging a record needs it.
        f.write_str("Secret(<redacted>)")
    }
}

/// Where a request may name the topic it is publishing to.
///
/// Section 9.2: a named header **or** a query parameter. An enum
/// rather than two optional fields, because two fields can both be set
/// and nothing says which wins; the ambiguity is refused by not being
/// storable.
///
/// **Variant order is the format.**
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopicFromRecord {
    /// The named request header.
    Header(String),
    /// The named query parameter.
    Query(String),
}

/// A topic the request chose, and the prefix that bounds it.
///
/// **The prefix is not optional, and that is the whole safety of the
/// feature.** Section 9.2: the topic may be taken from a header or a
/// query parameter, "restricted by a prefix so a caller cannot publish
/// anywhere". Without it an ingest rule is a way for whoever holds one
/// rule's token to publish to any topic on the broker, including the
/// reserved namespace. Held together with the source rather than beside
/// it so that a source without a prefix cannot be written down at all,
/// here or anywhere that reads this back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicFromRequestRecord {
    /// Where the request says it.
    pub source: TopicFromRecord,
    /// The prefix the topic must be at or under, by topic level.
    pub prefix: String,
}

/// How an `http_in` action accepts a post, section 9.2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpInRecord {
    /// The bearer token a request must present.
    ///
    /// Section 9.2: **not optional, and a rule without one is
    /// refused.** The endpoint publishes, so an unguarded one is a hole
    /// rather than a degraded mode.
    pub token: Secret,
    /// Where the body is published when the request does not choose.
    pub topic: String,
    /// Where the request may choose instead, and what bounds it.
    pub topic_from: Option<TopicFromRequestRecord>,
    /// The level the body is published at.
    pub qos: Qos,
    /// Whether it is retained.
    pub retain: bool,
}

/// How an `http_poll` action fetches, section 9.2.
///
/// A GET on an interval, publishing the response body to a topic.
///
/// **There is no retry count and no backoff here**, and that is the
/// shape of the connector rather than a field forgotten: a poller has
/// no outbox, a fetch that failed is simply tried again on the next
/// interval, and a body fetched late is worse than one not fetched at
/// all. `crates/rules/src/http_poll.rs` has the argument.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpPollRecord {
    /// Where to fetch from.
    pub url: String,
    /// Headers to send, in the order they were configured.
    pub headers: Vec<(String, String)>,
    /// How long between the start of one fetch and the next, in
    /// milliseconds.
    pub interval_ms: u32,
    /// How long one fetch may take, in milliseconds.
    pub timeout_ms: u32,
    /// The most of a response body that will be read, in bytes.
    pub max_body_bytes: u32,
    /// Where the body is published.
    pub topic: String,
    /// The level it is published at.
    pub qos: Qos,
    /// Whether it is retained.
    pub retain: bool,
}

/// Which way a bridge's topic rule moves messages, section 9.2.
///
/// **Variant order is the format.**
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DirectionRecord {
    /// Remote to local.
    In,
    /// Local to remote.
    Out,
    /// Both, which is one entry standing for two.
    Both,
}

/// One topic rule on a bridge, section 9.2.
///
/// A filter, a direction, a level and a rewrite. The two prefixes are
/// the rewrite's ordinary half and the template is the outbound-only
/// half; `crates/rules/src/bridge/rewrite.rs` says what each does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BridgeTopicRecord {
    /// The filter, without either prefix on it.
    pub filter: String,
    /// Which way messages move.
    pub direction: DirectionRecord,
    /// The level they move at.
    pub qos: Qos,
    /// The prefix this broker's half of the topic carries.
    pub local_prefix: String,
    /// The prefix the remote's half carries.
    pub remote_prefix: String,
    /// The outbound template, if one was configured.
    pub template: Option<String>,
}

/// How an `mqtt_bridge` action connects, section 9.2.
///
/// **The password is a [`Secret`]**, for the reason the ingest token is
/// one: a record is printed by a failure message and by anything that
/// logs what it is about to write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MqttBridgeRecord {
    /// Where the remote broker is, as `host:port`.
    pub address: String,
    /// What this bridge calls itself to the remote.
    pub client_id: String,
    /// Which version it speaks to the remote.
    pub version: ProtocolVersion,
    /// The user name it offers, if any.
    pub username: Option<String>,
    /// The password it offers, if any.
    pub password: Option<Secret>,
    /// The keep-alive it proposes, in seconds.
    pub keep_alive_secs: u16,
    /// Whether it asks the remote to discard any existing session.
    pub clean_start: bool,
    /// How long the remote should keep the session after a disconnect.
    pub session_expiry_secs: u32,
    /// How many acknowledged publications it will accept at once.
    pub receive_maximum: u16,
    /// The largest packet it will accept, in bytes.
    pub maximum_packet_size: u32,
    /// The highest topic alias it will accept from the remote.
    pub topic_alias_maximum: u16,
    /// Where its connectivity is reported.
    pub state_topic: String,
    /// The first reconnection wait, in milliseconds.
    pub reconnect_min_ms: u32,
    /// The longest reconnection wait, in milliseconds.
    pub reconnect_max_ms: u32,
    /// The hop count above which a publication is dropped.
    pub max_hops: u32,
    /// What moves, and which way.
    pub topics: Vec<BridgeTopicRecord>,
}

/// What a rule does when it fires. One, section 9.1.
///
/// **Variant order is the format**, as it is for [`Record`]: the other
/// actions section 9.2 names are later milestones' variants, and each
/// goes at the end.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionRecord {
    /// Post a JSON envelope to a URL.
    HttpOut(HttpOutRecord),
    /// Accept a post and publish its body. Section 9.2's `http_in`.
    ///
    /// Appended, so every `HttpOut` record written before this build
    /// still reads back as one and [`FORMAT_VERSION`] does not move.
    HttpIn(HttpInRecord),
    /// Fetch a URL on an interval and publish the body. Section 9.2's
    /// `http_poll`.
    ///
    /// Appended for the same reason and with the same effect: a record
    /// written before this build still reads back as whichever of the
    /// two it was, so [`FORMAT_VERSION`] does not move. What would move
    /// it is a variant inserted anywhere but the end, because the index
    /// is the format.
    HttpPoll(HttpPollRecord),
    /// Connect to a remote broker as a client and move messages both
    /// ways. Section 9.2's `mqtt_bridge`.
    ///
    /// Appended, at the end, for the reason the two above give: the
    /// index is the format, so a variant added last leaves every
    /// record written before this build reading back as whatever it
    /// was and [`FORMAT_VERSION`] stays where it is.
    MqttBridge(MqttBridgeRecord),
}

/// A rule, section 9.1.
///
/// Node-local in milestone 4 and replicated in milestone 5. Nothing here
/// changes when replication arrives, which is why the record is written
/// now rather than left until it does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleRecord {
    /// What names it, for as long as it exists.
    pub id: String,
    /// What an operator calls it.
    pub name: String,
    /// Whether it fires at all.
    pub enabled: bool,
    /// When it fires.
    ///
    /// # An action that nothing publishes fires stores an empty filter
    ///
    /// Section 9.1 gives every rule a trigger, and section 9.2's
    /// `http_in` has nothing to trigger on: an ingest rule is fired by
    /// an HTTP request rather than by a message. Such a rule stores a
    /// trigger whose filter is the empty string.
    ///
    /// That is an absence written as a value the reader already
    /// rejects, which is the one shape of sentinel this project will
    /// take: `""` is not a topic filter — `validate_filter` refuses it
    /// — so it cannot be mistaken for a trigger somebody meant, and the
    /// crate that reads this back refuses a rule that pairs an empty
    /// filter with an action that fires on messages, or a non-empty one
    /// with an action that does not.
    ///
    /// `Option<TriggerRecord>` is the honest encoding and would move
    /// [`FORMAT_VERSION`], which makes every store written before it
    /// unreadable. That is a cost worth paying **the next time the
    /// version moves for a reason of its own**, and it is named here
    /// rather than left to be discovered, so that it is a deferral and
    /// not an oversight.
    pub trigger: TriggerRecord,
    /// What it does.
    pub action: ActionRecord,
}

/// One envelope waiting in a rule's outbox, section 9.2.
///
/// # The attempt count is here, and that is the point of the record
///
/// It would be cheaper to keep the count beside the task that is doing
/// the posting. **A restart would then reset every envelope to zero
/// attempts**, and a node coming back after an outage would throw its
/// whole backlog at a still-dead endpoint as though none of it had ever
/// been tried. The count and the time of the next attempt are durable
/// facts about the delivery, not state belonging to whichever process
/// happens to be running.
///
/// # The payload is not here
///
/// [`Self::body`] names a [`BodyRecord`], which is the same row the
/// broker's own queues reference and is stored once however many things
/// name it. So a message that fires three rules is one payload and three
/// envelopes.
///
/// The cost of that is the one [`crate::Storage::apply`] warns about:
/// **taking a reference is explicit and so is releasing it.** Removing
/// an envelope from an outbox does not release the body it named, for
/// exactly the reason a queue pop does not, and an outbox that forgets
/// the release leaks the payload for the life of the file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxRecord {
    /// The per-rule sequence number, assigned on append and monotonic
    /// within the rule. It is the outbox's key, so reading in key order
    /// is reading in the order the rule fired, and it is never reused.
    pub sequence: u64,
    /// What names this delivery, for as long as it exists.
    ///
    /// Stored rather than derived, and minted once when the envelope is
    /// appended. Delivery is at least once — a timeout cannot be told
    /// from a failure, so a retry after one may repeat a post that
    /// arrived — and this is what lets a receiver notice the repeat. An
    /// identifier minted per attempt would look like a new delivery each
    /// time and would break exactly the deduplication it exists for.
    pub id: MessageId,
    /// The payload, stored once and referenced from here.
    pub body: MessageId,
    /// The client that published the message, which the envelope
    /// carries and the body does not.
    pub client_id: String,
    /// When the rule fired, in coarse seconds.
    pub enqueued_at: u32,
    /// How many attempts have been made and have failed.
    ///
    /// Zero for an envelope that has never been tried.
    pub attempts: u32,
    /// The coarse second before which the next attempt must not be
    /// made.
    ///
    /// Zero means "now": an envelope that has never been tried is due
    /// immediately, and so is one whose backoff has been served.
    pub next_attempt_at: u32,
}

/// Who this node is, section 8.1.
///
/// Minted on first start and read back unchanged for the life of the
/// store. There is one of these per store, not one per anything else:
/// it names the process's whole identity in the cluster.
///
/// # The UUID is stored as its bytes, and the name beside it
///
/// **[`Self::uuid`] is the identity everything orders by** and
/// [`Self::name`] is for operators and is never compared. Section 8.1
/// gives the reason in one sentence: two copied configurations share a
/// name, and a comparison that is not antisymmetric leaves both sides
/// believing they won.
///
/// The sixteen bytes rather than the canonical text, because the text
/// is a rendering of them and storing a rendering invites a reader that
/// parses it differently. The bytes are in the order the specification
/// lays a UUID out, which is what `Uuid::as_bytes` gives and what
/// `Uuid::from_bytes` takes back, so the ordering the cluster relies on
/// is the ordering of these bytes.
///
/// An operator may rename a node; the UUID does not move when they do.
/// That is the whole reason the two are separate fields rather than one
/// string: a rename must not re-found the node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIdentityRecord {
    /// The UUID v7 that names this node, as its sixteen bytes.
    pub uuid: [u8; 16],
    /// What an operator calls it. Never compared.
    pub name: String,
}

/// The highest hybrid logical clock reading this node has ever issued.
///
/// Section 8.2: **a node resumes its clock from the highest reading it
/// has ever written, read back from the store, not from zero.** A clock
/// that restarts behind itself re-issues a stamp it has already used,
/// and two writes under one key carrying one stamp is the single thing
/// convergence cannot repair: each side keeps its own and re-gossiping
/// changes nothing, because each believes it already holds the winner.
///
/// One of these per store, like [`NodeIdentityRecord`] and for the same
/// reason: it is a fact about this node rather than about anything in
/// the store.
///
/// # The store holds it and does not police it
///
/// A put replaces the row with whatever it carries, including a lower
/// reading. That is the same decision [`Record::NodeIdentityPut`]
/// records at length: only the caller knows whether a reading is this
/// node's own next one or a peer's, and a store with an opinion about
/// it would be a second answer in the layer that cannot see the
/// question. `cluster::HybridClock` is the one place that decides, and
/// it only ever moves forwards.
///
/// # Both halves, because milliseconds alone are not a reading
///
/// The counter is what makes two writes inside one millisecond
/// distinguishable, so a resume that recovered [`Self::millis`] and
/// dropped [`Self::counter`] would be a clock resuming behind itself by
/// up to a millisecond's worth of writes. Section 8.2 again: dropping
/// the counter makes one node ambiguous with itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClockRecord {
    /// The physical half: milliseconds since the Unix epoch.
    pub millis: u64,
    /// The logical half: how many readings this build has issued inside
    /// [`Self::millis`] already.
    pub counter: u32,
}

/// What orders one replicated write against another, section 8.2.
///
/// A clock reading and the node that wrote it. **The comparison is the
/// milliseconds, then the counter, then the sixteen UUID bytes**, and it
/// lives in `cluster::register` rather than here: this crate holds the
/// bytes and does not resolve conflicts. The field order below is that
/// order all the same, so a hex dump reads in the order a comparison
/// does.
///
/// The node is its sixteen bytes rather than its canonical text, for the
/// reason [`NodeIdentityRecord::uuid`] gives: the text is a rendering of
/// the bytes, and storing a rendering invites a reader that parses it
/// differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StampRecord {
    /// The reading's physical half.
    pub millis: u64,
    /// The reading's logical half.
    pub counter: u32,
    /// Who wrote it.
    pub node: [u8; 16],
}

/// What a replicated register is about, section 8.2.
///
/// # An open vocabulary, deliberately
///
/// Section 8.2 lists seven kinds of replicated metadata and three of
/// them — users, access control rules, API keys — do not exist until
/// milestone 7 brings authentication. They are a variant here and a
/// variant of [`MetadataValueRecord`], and nothing else: no table, no
/// exchange, no merge rule. That is what "written to carry a kind they
/// do not yet know" means in practice.
///
/// **Variant order is the format**, as it is for [`Record`]: a new kind
/// goes at the end or [`FORMAT_VERSION`] moves. It is also the order a
/// restore hands these back in, and the redb backend's key encoding
/// puts the variant index in the leading byte precisely so that the two
/// backends agree on it.
///
/// # A subscription names a node and a filter, and not a client
///
/// Section 8.2: **subscriptions are replicated at node granularity.** A
/// register per client subscription would be fifty nodes holding a
/// hundred thousand each at the design ceiling, against a thirty
/// megabyte idle budget. What is replicated is that a node has at least
/// one subscriber for a filter, which is what routing needs; which of
/// its clients that is never leaves the node, so there is nothing here
/// to hold it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum MetadataKeyRecord {
    /// That a node has at least one subscriber for a filter.
    NodeSubscription {
        /// Whose subscriber. Its sixteen UUID bytes.
        node: [u8; 16],
        /// Which filter.
        filter: String,
    },
    /// The retained message on a topic.
    Retained {
        /// Which topic.
        topic: String,
    },
    /// A rule, section 9.1, which stops being node-local here.
    Rule {
        /// Which rule.
        id: String,
    },
    /// Which node owns a session, section 8.4.
    SessionOwner {
        /// Whose session.
        session: SessionKey,
    },
    /// That a node has at least one member of a shared group.
    ///
    /// **Separate from [`Self::NodeSubscription`], and section 8.3 says
    /// why.** A group spanning three nodes has a matching subscriber on
    /// each, so a routing map built from node subscriptions says all
    /// three want the topic and one copy to each delivers three times
    /// where the semantics demand one. A group needs its own map and a
    /// selection *before* forwarding, and a map needs a register of its
    /// own to be built from: the group's name is what a node subscription
    /// deliberately does not carry.
    ///
    /// **Last of the variants, which is what keeps the format.** The
    /// variant order is the format, as the type documentation says, and
    /// a new kind goes at the end or [`FORMAT_VERSION`] moves. This is
    /// the end.
    NodeGroup {
        /// Whose member. Its sixteen UUID bytes.
        node: [u8; 16],
        /// The share name, which is what makes two groups on one filter
        /// two groups.
        group: String,
        /// The inner filter, with the `$share/<name>/` prefix stripped,
        /// because that is what a published topic is matched against.
        filter: String,
    },
    /// A user, by name. Section 8.2 lists users among what replicates.
    User {
        /// The name.
        name: String,
    },
    /// An access control rule, by identifier.
    Acl {
        /// The identifier.
        id: String,
    },
}

/// What a replicated register holds, section 8.2.
///
/// One variant per [`MetadataKeyRecord`] variant, in the same order. A
/// stored register whose key and value name different kinds is refused
/// when it is read back, by the crate that knows what the pairs mean.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataValueRecord {
    /// Presence, and nothing else.
    ///
    /// A unit variant because the key carries the node and the filter,
    /// which is the whole fact. Anything here would be something about
    /// that node's own clients, and that is the part which does not fit
    /// in the budget.
    NodeSubscription,
    /// What is retained on the topic the key names.
    Retained(RetainedRecord),
    /// The rule the key names, whole.
    ///
    /// Boxed, which changes nothing on disk — serde writes a `Box<T>`
    /// exactly as it writes a `T`, and the golden vectors pin that — and
    /// keeps this variant from being three hundred bytes while the
    /// smallest is sixteen. The same reason `rules::rule::Action` boxes
    /// its bridge.
    Rule(Box<RuleRecord>),
    /// Which node owns the session the key names, and at what epoch.
    SessionOwner {
        /// Its sixteen UUID bytes.
        node: [u8; 16],
        /// How many times the session has been claimed, section 8.4.
        ///
        /// **Carried, and not compared.** Section 8.5 is explicit that
        /// equal epochs are the expected outcome of a partition rather
        /// than the rare one — two nodes on opposite sides both read
        /// the same epoch and both increment to the same number — so
        /// what resolves two claims is the register's own stamp, like
        /// every other kind. This is here so that the *next* claim has
        /// something to be one more than.
        epoch: u64,
    },
    /// Presence, and nothing else.
    ///
    /// A unit variant for [`MetadataKeyRecord::NodeGroup`]'s reason:
    /// the key carries the node, the group and the filter, which is the
    /// whole fact. How many of that node's clients are in the group is
    /// the part section 8.2 keeps on the node, as it does for a node
    /// subscription.
    NodeGroup,
    /// A user, whole. Boxed for the reason a rule is.
    User(Box<UserRecord>),
    /// An access control rule, whole.
    Acl(Box<AclRecord>),
}

/// One replicated register, as a store holds it.
///
/// # An absent value is a tombstone, and that is the whole of deletion
///
/// Section 8.2: **a tombstone is a value competing by the same
/// comparison, not a special case.** So there is no `MetadataRemoved`
/// record beside this one: a deletion is one of these with
/// [`Self::value`] absent, and it is stored, because a node that simply
/// dropped the row would be handed its own older copy by the next peer
/// it spoke to and would resurrect what it deleted. Collecting
/// tombstones behind a cluster-wide watermark is a later task, and it is
/// the only thing that ever removes one of these.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetadataRecord {
    /// What the register is about.
    pub key: MetadataKeyRecord,
    /// What orders it against another copy.
    pub stamp: StampRecord,
    /// What is held, or `None` for a tombstone.
    pub value: Option<MetadataValueRecord>,
}

/// One entry in a node's own operation log, section 8.2.
///
/// A sequence number and the register the node wrote at it. The log is
/// **gapless in the sequence**, which is what makes "everything after
/// sequence N from node X" a question with an answer, and it is the
/// claim the anti-entropy digest makes: a record's most recent writer
/// changes whenever another node overwrites it, so the question cannot
/// be answered by looking at the registers.
///
/// # The whole register, not a reference to one
///
/// [`Self::entry`] duplicates what a [`MetadataPut`] in the same batch
/// stored, and the alternative — a log of keys, read back through the
/// register table — was rejected. A key's register is replaced by
/// whatever wins later, including by a tombstone another node's
/// collection has since removed, so a log of keys has entries that
/// resolve to nothing and a peer asking for them gets silence where the
/// log promised an answer. Holding the write makes an entry
/// self-contained, and the duplication is bounded by the truncation the
/// log's own watermark drives.
///
/// [`MetadataPut`]: Record::MetadataPut
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoggedRecord {
    /// Where in this node's log it sits. Counted from one.
    pub sequence: u64,
    /// What was written.
    pub entry: MetadataRecord,
}

/// How far this node has read one other node's operation log, section
/// 8.2.
///
/// **Durable, and that is not an optimisation.** A node that came back
/// claiming to have seen nothing would be served every peer's log from
/// the beginning — and a peer whose log has been truncated behind its
/// watermark cannot serve that, because the entries are gone. Worse, the
/// claim drags every peer's watermark to nothing, so no node in the
/// cluster could truncate a log or collect a tombstone until it caught
/// up. So what a node has seen is written down beside the registers it
/// saw, in the same batch, and a restart resumes from it.
///
/// [`Self::through`] is the **highest contiguous** sequence, not the
/// highest seen: a broadcast that was dropped leaves a gap, and a node
/// that counted past it would never be sent the entry it is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeenRecord {
    /// Whose log. Its sixteen UUID bytes.
    pub node: [u8; 16],
    /// The highest sequence of that node's log this node holds, with
    /// nothing missing below it. Zero is a node nothing has been read
    /// from.
    pub through: u64,
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
    /// one reference taken, and a put for an identifier the store
    /// already holds takes the reference without rewriting the body;
    /// see [`crate::Storage::apply`].
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
    /// A rule was created, or one that exists was changed.
    ///
    /// One record for both, as [`Record::SessionPut`] is: a rule is
    /// whatever the last put said it was, so an edit needs no vocabulary
    /// of its own and cannot half-apply.
    RulePut {
        /// The rule, whole.
        rule: RuleRecord,
    },
    /// A rule was deleted.
    ///
    /// Takes its outbox with it, as [`Record::SessionRemoved`] takes a
    /// session's queue: an outbox filed under a rule nothing holds is
    /// reachable by nothing and would sit in the file forever. The
    /// bodies those envelopes named are **not** released, for the same
    /// reason a session's removal does not release its queue's: the
    /// reference belongs to whoever took it, and a caller that emits
    /// its own releases would otherwise release twice.
    RuleRemoved {
        /// Which rule.
        id: String,
    },
    /// An envelope joined a rule's outbox at this sequence number.
    ///
    /// Refused for a rule the store does not hold, for the reason
    /// [`Rejection::NoSuchRule`] gives.
    ///
    /// [`Rejection::NoSuchRule`]: crate::Rejection::NoSuchRule
    OutboxAppended {
        /// Whose outbox.
        rule: String,
        /// What went into it.
        entry: OutboxRecord,
    },
    /// An attempt was made on an envelope and did not succeed.
    ///
    /// The count and the time of the next attempt are replaced with what
    /// this record carries rather than incremented by the store, so that
    /// the backoff is the caller's policy and the store holds a fact.
    /// An attempt on a sequence the outbox no longer holds is a no-op:
    /// the envelope left, which is the one outcome that makes further
    /// attempts pointless.
    OutboxAttempted {
        /// Whose outbox.
        rule: String,
        /// Which envelope.
        sequence: u64,
        /// How many attempts have now been made and failed.
        attempts: u32,
        /// The coarse second before which the next must not be made.
        next_attempt_at: u32,
    },
    /// An envelope left an outbox, delivered or dead-lettered.
    ///
    /// **Does not release the body it named.** See [`OutboxRecord`] and
    /// [`crate::Storage::apply`]: the release is the caller's, and it is
    /// owed.
    OutboxRemoved {
        /// Whose outbox.
        rule: String,
        /// Which envelope.
        sequence: u64,
    },
    /// Who this node is, section 8.1.
    ///
    /// One record for minting and for renaming, as [`Record::RulePut`]
    /// is one record for creating and for editing: the identity is
    /// whatever the last put said it was.
    ///
    /// **The store does not decide when one of these is written.** It
    /// holds what it is given, and the caller is the one place that
    /// knows an identity already exists and must not be replaced with a
    /// fresh one; see `cluster::NodeIdentity`. A store that refused a
    /// put whose UUID differed would be a second opinion about the same
    /// rule, in the layer that cannot see why the caller is asking.
    NodeIdentityPut {
        /// Who this node is.
        identity: NodeIdentityRecord,
    },
    /// The highest clock reading this node has issued, section 8.2.
    ///
    /// Written **in the same batch as the write it stamps**, which is
    /// what makes the resume safe rather than nearly safe: a batch is
    /// all or nothing, so a stamp that survived a crash has its mark
    /// beside it, and a stamp that did not was never shown to anybody.
    /// A mark written in a batch of its own could be lost while the
    /// write it covers survived, which is a clock resuming behind
    /// itself with no sign that anything went wrong.
    ///
    /// Replaced whole, and the store does not decide whether the
    /// reading moved forwards; see [`ClockRecord`].
    ClockHighWaterPut {
        /// The reading.
        reading: ClockRecord,
    },
    /// One replicated register, section 8.2.
    ///
    /// **Replaced whole, and the store does not merge.** Which of two
    /// copies wins is `cluster::register`'s one comparison, and a store
    /// with an opinion about it would be a second answer in the layer
    /// that cannot see the question — the same decision
    /// [`Record::NodeIdentityPut`] and [`ClockRecord`] record at
    /// length. A caller stores what it has already decided to keep.
    ///
    /// A deletion is one of these carrying no value. There is no
    /// removal record, for the reason [`MetadataRecord`] gives.
    MetadataPut {
        /// The register, and what it is about.
        entry: MetadataRecord,
    },
    /// One replicated register is gone, section 8.2.
    ///
    /// **The only thing that ever removes one**, and it is tombstone
    /// collection rather than a deletion: a deletion is a
    /// [`Record::MetadataPut`] carrying no value, and it is *stored*,
    /// because a node that dropped the row would be handed its own older
    /// copy by the next peer it spoke to and would resurrect what it
    /// deleted. This is what happens afterwards, once every node has
    /// seen the tombstone and the grace period has passed, and the
    /// caller is the only thing that can know both of those.
    ///
    /// Removing a key the store does not hold is a no-op rather than a
    /// refusal. A sweep that ran twice, or on a node that had already
    /// collected the same tombstone through a restart, is not a
    /// caller's mistake.
    MetadataRemoved {
        /// Which register.
        key: MetadataKeyRecord,
    },
    /// How far this node has read one other node's log, section 8.2.
    ///
    /// Written in the **same batch as the registers it covers**, for the
    /// reason [`Record::ClockHighWaterPut`] is: a batch is all or
    /// nothing, so a node that survived a crash claiming to have read a
    /// peer's log through sequence N holds everything up to N, and one
    /// that claimed it without the registers beside it would never be
    /// sent them again.
    ///
    /// Replaced whole, and the store does not decide whether the number
    /// moved forwards. See [`SeenRecord`].
    MetadataSeenPut {
        /// Whose log, and how far.
        seen: SeenRecord,
    },
    /// One entry in this node's own operation log, section 8.2.
    ///
    /// Written in the same batch as the [`Record::MetadataPut`] it
    /// duplicates, so that a node cannot come back holding a register
    /// its log does not mention — which is a register no exchange would
    /// ever repair, because the exchange serves the origin's log and the
    /// origin would have forgotten.
    ///
    /// # Refused when the sequence is spent
    ///
    /// Section 7.4's rule, one table across: two entries sharing a
    /// number, or a number going backwards, is what must never happen,
    /// and the store holds the highest sequence ever appended precisely
    /// so it can say no. A log that restarted from one would hand a peer
    /// one node's second write where the peer was told to expect its
    /// first, which is the same defect as a clock resuming from zero.
    OpLogAppended {
        /// The entry, with the sequence it claims.
        entry: LoggedRecord,
    },
    /// This node's own log is collected up to and including `through`.
    ///
    /// Section 8.2's second watermark, the one distinct from the
    /// tombstone one: a node truncates its own log behind what every
    /// other node has read of it, because nothing will ever ask for an
    /// entry every node already holds.
    ///
    /// **The highest sequence ever appended is not touched**, so a
    /// restart after a truncation that emptied the log still knows which
    /// number comes next. Truncating past the end, or twice, is a no-op
    /// rather than a refusal.
    OpLogTruncated {
        /// The last sequence to remove.
        through: u64,
    },
    /// This node holds a copy of a session another node owns, and has
    /// applied its stream this far. Section 8.6.
    ///
    /// **At the end of the enum rather than beside the session records
    /// it belongs with**, because postcard writes a variant's index and
    /// nothing else: inserting one in the middle renumbers every
    /// variant after it, so a store written by the previous build
    /// decodes as the wrong records entirely. These two were written
    /// after `RulePut` first and the golden vectors caught it, which is
    /// what they are for.
    ///
    /// **What stops a restore serving another node's session.** The
    /// queue, the subscriptions and the session record of a followed
    /// session are the same records as any other, so without this a
    /// restore would hand the broker a session it does not own, put
    /// its filters in this node's trie, and deliver its messages to
    /// nobody. [`crate::restore`] reads this first and splits what it
    /// hands back in two.
    ///
    /// One row per followed session, **overwritten as the mark
    /// advances** rather than appended to: the mark is a position in a
    /// stream, not a history, and it rides the same batch as the queue
    /// record it is about so the two cannot disagree across a crash.
    ReplicaFollows {
        /// Whose queue this is a copy of.
        session: SessionKey,
        /// The node that owns it, by its sixteen identity bytes.
        ///
        /// Opaque here, as [`MessageId`] is: which node that is, is
        /// `cluster`'s question, and this crate does not depend on it.
        owner: [u8; 16],
        /// The highest contiguous stream position applied.
        through: u64,
    },
    /// This node no longer follows that session.
    ///
    /// Written when a replica set is repaired away from this node, and
    /// when this node becomes the session's owner — which is why the
    /// mark is its own row rather than a field on the session record.
    /// **It does not remove the session**: becoming the owner keeps
    /// every queue entry and changes only who it belongs to, and a
    /// variant that took the queue with it could not express that.
    ReplicaDropped {
        /// Which session.
        session: SessionKey,
    },
    /// How far this node has sent, for a session it owns. Section 8.6.
    ///
    /// The owner's counterpart to [`Record::ReplicaFollows`]: that one
    /// is where a copy has got to in applying, this one is where the
    /// original has got to in sending. One row per owned session,
    /// overwritten, in the same batch as the queue record it covers.
    ///
    /// Stored rather than counted from zero at startup, because a
    /// follower refuses anything at or below its own mark: an owner
    /// that restarted its count would have everything it sent refused
    /// as a duplicate, silently, until the session caught up with where
    /// it had been.
    ///
    /// Goes with the session, as the queue does.
    StreamAt {
        /// Which session.
        session: SessionKey,
        /// The position of the last change sent.
        at: u64,
    },
    /// A user exists, or its credentials changed. Section 12.1.
    ///
    /// Carries the argon2 string and the SCRAM verifier, never a
    /// password: both are derived when the password is set, because a
    /// verifier cannot be derived from an argon2 hash (section 12.2).
    /// Boxed for the reason a rule is: the record is large and the
    /// enum is sized by its largest variant.
    UserPut(Box<UserRecord>),
    /// A user is gone.
    UserDeleted {
        /// Its name.
        name: String,
    },
    /// An access control rule exists, or changed. Section 12.1.
    AclPut(Box<AclRecord>),
    /// An access control rule is gone.
    AclDeleted {
        /// Its identifier.
        id: String,
    },
}

/// One access control rule, as stored. Section 12.1.
///
/// Rules are consulted in `order`, then by identifier, and the first
/// one whose subject and filter match decides the operation it has an
/// answer for; a rule with `None` for an operation says nothing about
/// it and the next is asked. With no rule matching, and any rule at
/// all existing, the answer is no.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclRecord {
    /// What an operator calls it. Compared exactly.
    pub id: String,
    /// Lower is consulted first.
    pub order: u32,
    /// Whom it is about: `anyone`, `user:<name>` or `client:<id>`.
    pub subject: String,
    /// The topic filter it is about, with MQTT wildcards.
    pub filter: String,
    /// Whether publishing to a matching topic is allowed, when this rule
    /// has an opinion.
    pub publish: Option<bool>,
    /// Whether subscribing to a matching filter is allowed, when this
    /// rule has an opinion.
    pub subscribe: Option<bool>,
}

/// A user, as stored. Sections 12.1 and 12.2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRecord {
    /// The name a client presents. Compared exactly.
    pub name: String,
    /// The argon2id hash in PHC string form, when the user has a
    /// password. `None` for a user enrolled in SCRAM alone.
    pub password: Option<Secret>,
    /// The SCRAM-SHA-256 verifier, when the user is enrolled.
    pub scram: Option<ScramVerifierRecord>,
    /// Seconds since the epoch, for the page.
    pub created_at: u64,
}

/// What SCRAM-SHA-256 needs to verify a client without the password.
///
/// Section 12.2: "a weaker secret at rest than an argon2 hash, because
/// possession of the stored key permits impersonating that client", so
/// both keys are [`Secret`]s and the API never returns them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramVerifierRecord {
    /// Random, per user.
    pub salt: Vec<u8>,
    /// PBKDF2 rounds. Section 12.2: configurable, defaulting to a value
    /// a constrained client can afford.
    pub iterations: u32,
    /// `H(ClientKey)`, base64.
    pub stored_key: Secret,
    /// `HMAC(SaltedPassword, "Server Key")`, base64.
    pub server_key: Secret,
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
            | Self::QueuePopped { session, .. }
            // The owner's sixteen bytes and the mark are fixed, so the
            // key is the only part a peer can grow.
            | Self::ReplicaFollows { session, .. }
            | Self::ReplicaDropped { session }
            | Self::StreamAt { session, .. } => weigh_key(session),
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
            // An operator writes these, not a peer, so none of it is
            // unbounded in the sense this estimate is about. Counted
            // anyway, because the fields are strings and a bound that
            // ignores strings is a bound waiting to be wrong.
            Self::RulePut { rule } => {
                rule.id.len()
                    + rule.name.len()
                    + rule.trigger.filter.len()
                    + rule.trigger.client_id.as_ref().map_or(0, String::len)
                    + match &rule.action {
                        ActionRecord::HttpOut(http) => {
                            http.url.len()
                                + http.method.len()
                                + http
                                    .headers
                                    .iter()
                                    .map(|(name, value)| name.len() + value.len())
                                    .sum::<usize>()
                        }
                        ActionRecord::HttpIn(http) => {
                            http.token.expose().len()
                                + http.topic.len()
                                + http.topic_from.as_ref().map_or(0, |from| {
                                    from.prefix.len()
                                        + match &from.source {
                                            TopicFromRecord::Header(name)
                                            | TopicFromRecord::Query(name) => name.len(),
                                        }
                                })
                        }
                        ActionRecord::HttpPoll(http) => {
                            http.url.len()
                                + http.topic.len()
                                + http
                                    .headers
                                    .iter()
                                    .map(|(name, value)| name.len() + value.len())
                                    .sum::<usize>()
                        }
                        ActionRecord::MqttBridge(bridge) => {
                            bridge.address.len()
                                + bridge.client_id.len()
                                + bridge.username.as_ref().map_or(0, String::len)
                                + bridge.password.as_ref().map_or(0, |p| p.expose().len())
                                + bridge.state_topic.len()
                                + bridge
                                    .topics
                                    .iter()
                                    .map(|topic| {
                                        topic.filter.len()
                                            + topic.local_prefix.len()
                                            + topic.remote_prefix.len()
                                            + topic.template.as_ref().map_or(0, String::len)
                                    })
                                    .sum::<usize>()
                        }
                    }
            }
            Self::RuleRemoved { id } => id.len(),
            Self::UserPut(user) => weigh_user(user),
            Self::UserDeleted { name } => name.len(),
            Self::AclPut(rule) => weigh_acl(rule),
            Self::AclDeleted { id } => id.len(),
            // The client identifier is a peer's, and the rule
            // identifier an operator's. The payload is not counted
            // because it is not here: the `BodyPut` beside this record
            // carries it and is weighed on its own.
            Self::OutboxAppended { rule, entry } => rule.len() + entry.client_id.len(),
            Self::OutboxAttempted { rule, .. } | Self::OutboxRemoved { rule, .. } => rule.len(),
            // An operator's string, and the only variable thing here:
            // the UUID is sixteen bytes counted by the record's own
            // footprint.
            Self::NodeIdentityPut { identity } => identity.name.len(),
            // Two integers, both inside the record's own footprint.
            Self::ClockHighWaterPut { .. } => 0,
            // A peer controls the size of a filter and a topic, an
            // operator the size of a rule, and a client the size of a
            // session key. The stamp is two integers and sixteen bytes,
            // all inside the record's own footprint.
            Self::MetadataPut { entry } => weigh_metadata(entry),
            // The key alone: a removal carries no value, because what it
            // removes is a tombstone and a tombstone has none.
            Self::MetadataRemoved { key } => weigh_metadata_key(key),
            // Sixteen bytes and an integer, both inside the record's own
            // footprint.
            Self::MetadataSeenPut { .. } | Self::OpLogTruncated { .. } => 0,
            // The same accounting a `MetadataPut` gets, because it holds
            // the same register: see `LoggedRecord`.
            Self::OpLogAppended { entry } => weigh_metadata(&entry.entry),
        };
        std::mem::size_of::<Self>() + variable
    }
}

/// What a replicated register holds beyond the record's own footprint.
///
/// A peer controls the size of a filter and a topic, an operator the size
/// of a rule, and a client the size of a session key. The stamp is two
/// integers and sixteen bytes, all inside the record's own footprint.
fn weigh_metadata(entry: &MetadataRecord) -> usize {
    let value = match &entry.value {
        None
        | Some(
            MetadataValueRecord::NodeSubscription
            | MetadataValueRecord::SessionOwner { .. }
            | MetadataValueRecord::NodeGroup,
        ) => 0,
        Some(MetadataValueRecord::Retained(message)) => {
            message.payload.len() + message.properties.len()
        }
        // The same accounting a `RulePut` gets, and the one place this
        // estimate would otherwise miss a field an operator can grow.
        Some(MetadataValueRecord::Rule(rule)) => {
            Record::RulePut {
                rule: (**rule).clone(),
            }
            .weight()
                - std::mem::size_of::<Record>()
        }
        Some(MetadataValueRecord::User(user)) => weigh_user(user),
        Some(MetadataValueRecord::Acl(rule)) => weigh_acl(rule),
    };
    weigh_metadata_key(&entry.key) + value
}

/// What a register's key holds beyond the record's own footprint.
fn weigh_metadata_key(key: &MetadataKeyRecord) -> usize {
    match key {
        MetadataKeyRecord::NodeSubscription { filter, .. } => filter.len(),
        MetadataKeyRecord::Retained { topic } => topic.len(),
        MetadataKeyRecord::Rule { id } => id.len(),
        MetadataKeyRecord::SessionOwner { session } => weigh_key(session),
        MetadataKeyRecord::NodeGroup { group, filter, .. } => group.len() + filter.len(),
        MetadataKeyRecord::User { name } => name.len(),
        MetadataKeyRecord::Acl { id } => id.len(),
    }
}

/// What a rule's record holds beyond its own footprint.
fn weigh_acl(rule: &AclRecord) -> usize {
    rule.id.len() + rule.subject.len() + rule.filter.len()
}

/// What a user's record holds beyond its own footprint: the name, the
/// hash string and the verifier's three byte strings.
fn weigh_user(user: &UserRecord) -> usize {
    user.name.len()
        + user.password.as_ref().map_or(0, |hash| hash.expose().len())
        + user.scram.as_ref().map_or(0, |scram| {
            scram.salt.len() + scram.stored_key.expose().len() + scram.server_key.expose().len()
        })
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
