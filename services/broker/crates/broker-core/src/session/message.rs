//! The message that flows from a publisher to each subscriber.

use std::sync::Arc;

use bytes::Bytes;
use mqtt_codec::QoS;
use uuid::Uuid;

use crate::properties::SharedProperties;

/// A message identifier that sorts by the time it was created.
///
/// Version 7 identifiers put a millisecond timestamp in their high bits, so
/// ordering by identifier is ordering by creation. A queue can therefore be
/// restored in order without a separate sequence number beside it, and two
/// nodes can mint identifiers without coordinating.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageId(Uuid);

impl MessageId {
    /// Mint a new identifier.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// The identifier's bytes, for storage.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    /// Rebuild an identifier from stored bytes.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// An application message on its way from a publisher to a subscriber.
///
/// The topic and properties are shared rather than owned, because fan-out
/// clones one message per matching subscriber and copying a topic string
/// and a property block per subscriber is most of the cost of a publish.
#[derive(Debug, Clone)]
pub struct Message {
    /// Identifies this message for the life of the broker.
    pub id: MessageId,
    /// Where it was published.
    pub topic: Arc<str>,
    /// The application payload, which the broker never interprets.
    pub payload: Bytes,
    /// The quality of service it was published at. A subscriber may receive
    /// it at a lower one.
    pub qos: QoS,
    /// Whether the publisher asked for it to be retained.
    pub retain: bool,
    /// MQTT 5 properties, shared across every copy, and encoded at
    /// most once however many subscribers journal this message.
    pub properties: Arc<SharedProperties>,
    /// A coarse deadline in seconds, past which it must not be delivered.
    pub expires_at: Option<u32>,
    /// Whether this message reached this node from another node.
    ///
    /// # The mark is the difference between a mesh and an infinite loop
    ///
    /// Section 8.3: **a forwarded message is marked as forwarded, and a
    /// forwarded message is never forwarded again.** Without it, three
    /// nodes that each have a subscriber forward to each other for ever
    /// at the speed of the links between them. The original design
    /// stated it as an implication; it is stated outright there now, and
    /// this is it.
    ///
    /// It is set in exactly one place,
    /// [`Broker::accept_forwarded`], and read in exactly one,
    /// [`Broker::publish`]. Nothing else sets it and nothing outside
    /// this crate needs to: a publication a client made, a will, a dead
    /// letter and a message a connector originated are all unmarked,
    /// because all of them entered the cluster here.
    ///
    /// # It answers section 9's question about where a rule fires
    ///
    /// A rule fires on the node where a message entered the cluster,
    /// which is the node that received it unmarked. That falls out of
    /// this rather than needing a rule of its own: the hooks are fired
    /// by [`Broker::handle`] and [`Broker::originate`], and a forwarded
    /// message reaches neither.
    ///
    /// # Not stored, deliberately
    ///
    /// [`crate::journal::body_record`] does not carry it and
    /// [`crate::journal::live_message`] reads it back as `false`. A
    /// message in a store is one that has already been routed, sitting
    /// in some session's queue; a restart that resumed the mark would be
    /// resuming an answer to a question nothing is going to ask again.
    ///
    /// [`Broker::accept_forwarded`]: crate::broker::Broker::accept_forwarded
    /// [`Broker::publish`]: crate::broker::Broker::publish
    /// [`Broker::handle`]: crate::broker::Broker::handle
    /// [`Broker::originate`]: crate::broker::Broker::originate
    pub forwarded: bool,
}

impl Message {
    /// A message with no properties and no deadline.
    pub fn new(topic: &str, payload: Bytes, qos: QoS, retain: bool) -> Self {
        Self {
            id: MessageId::new(),
            topic: Arc::from(topic),
            payload,
            qos,
            retain,
            properties: Arc::new(SharedProperties::default()),
            expires_at: None,
            // A message built here is one that entered the cluster
            // here. Only `Broker::accept_forwarded` says otherwise.
            forwarded: false,
        }
    }

    /// Roughly how much of a queue's byte budget this occupies.
    ///
    /// Approximate on purpose. The budget exists to bound memory, not to be
    /// exact, and an exact figure would have to track the shared topic and
    /// properties that several queued messages hold between them.
    pub fn size_in_queue(&self) -> usize {
        self.payload.len() + self.topic.len() + std::mem::size_of::<Self>()
    }

    /// Whether the deadline has passed. A message expires strictly after
    /// its deadline, matching the retained store.
    pub fn is_expired(&self, now: u32) -> bool {
        self.expires_at.is_some_and(|deadline| now > deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mqtt_codec::QoS;

    fn message(topic: &str, payload: &'static [u8]) -> Message {
        Message::new(topic, Bytes::from_static(payload), QoS::AtMostOnce, false)
    }

    #[test]
    fn identifiers_are_unique() {
        let a = MessageId::new();
        let b = MessageId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn identifiers_sort_by_creation_order() {
        // A time-ordered identifier is what lets a queue restore in order
        // without storing a separate sequence alongside it.
        let mut ids: Vec<MessageId> = (0..64).map(|_| MessageId::new()).collect();
        let created = ids.clone();
        ids.sort();
        assert_eq!(ids, created, "identifiers should already be in order");
    }

    #[test]
    fn a_message_carries_what_delivery_needs() {
        let message = message("a/b", b"hello");
        assert_eq!(&*message.topic, "a/b");
        assert_eq!(&message.payload[..], b"hello");
        assert_eq!(message.qos, QoS::AtMostOnce);
        assert!(!message.retain);
        assert_eq!(message.expires_at, None);
    }

    #[test]
    fn cloning_shares_the_topic_and_payload_rather_than_copying() {
        // Fan-out clones a message once per subscriber, so the payload must
        // not be copied each time.
        let original = message("a/b", b"hello");
        let copy = original.clone();
        assert!(
            std::sync::Arc::ptr_eq(&original.topic, &copy.topic),
            "the topic should be shared"
        );
        assert_eq!(original.payload.as_ptr(), copy.payload.as_ptr());
    }

    #[test]
    fn size_in_queue_counts_the_payload_and_the_topic() {
        let small = message("a", b"x");
        let large = message("a", b"xxxxxxxxxx");
        assert!(large.size_in_queue() > small.size_in_queue());
        assert!(
            small.size_in_queue() >= 1,
            "an empty payload still occupies a slot"
        );
    }

    #[test]
    fn expiry_is_reported_against_a_supplied_clock() {
        let mut message = message("a", b"x");
        assert!(!message.is_expired(1_000), "no deadline never expires");

        message.expires_at = Some(100);
        assert!(!message.is_expired(99));
        assert!(!message.is_expired(100), "the deadline itself is not past");
        assert!(message.is_expired(101));
    }
}
