//! A property block, shared by every copy of a message, and the
//! encoding the store holds it in.

use std::ops::Deref;
use std::sync::OnceLock;

use bytes::Bytes;
use mqtt_codec::Properties;

/// MQTT 5 properties shared across a fan-out, encoded at most once.
///
/// # Why the encoding is cached rather than taken where it is needed
///
/// A message matching a thousand subscriptions is offered to a thousand
/// sessions, and every one of them journals a body record carrying the
/// property block as bytes. The store keeps **one** body per message
/// however many sessions hold it, so those thousand encodes produce one
/// row.
///
/// Measured before this existed, on a fan-out of a thousand offline
/// durable sessions with a realistic 95-byte block: 654 ns per
/// subscriber against 435 ns for the same publish with an empty block,
/// and 465 against 248 at a fan-out of a hundred. A third to a half of
/// the cost of a large fan-out was spent producing copies of one
/// answer. With this, the two figures meet — 366 and 393 at a thousand
/// — and a fan-out of one is unchanged. The harness that says so is
/// `what_a_property_block_costs_a_fan_out`, in `sdmqtt-server`'s
/// `tests/throughput.rs`.
///
/// # Why it is lazy
///
/// [`OnceLock`] rather than a field filled on construction, because an
/// at-most-once publication delivered straight to an attached client is
/// journalled nowhere — section 7.3, and `Outbound::offer`'s direct
/// path — and that is the hot path. Encoding eagerly would charge every
/// such message for bytes no record ever asks for.
///
/// # Why there is no way to change one in place
///
/// The cache is only honest while the block it was taken from cannot
/// change. So this derefs to the live block and offers no mutable
/// access to it: a message whose properties change gets a new
/// `SharedProperties`, which is what every caller that changes them
/// already did.
#[derive(Debug, Default)]
pub struct SharedProperties {
    /// The block as the broker reads it.
    live: Properties,
    /// The block as the store holds it, once anybody has asked.
    encoded: OnceLock<Bytes>,
}

impl SharedProperties {
    /// Share this block, encoding it only if somebody asks.
    pub fn new(live: Properties) -> Self {
        Self {
            live,
            encoded: OnceLock::new(),
        }
    }

    /// Share a block whose encoding is already in hand.
    ///
    /// For restore, which read the bytes out of the store and decoded
    /// them: encoding them again to get back what it started with would
    /// be work for nothing.
    ///
    /// The caller promises the two are the same block. Nothing checks
    /// it, because the only caller is the one that just decoded one
    /// from the other.
    pub fn from_encoded(live: Properties, encoded: Bytes) -> Self {
        let cache = OnceLock::new();
        let _ = cache.set(encoded);
        Self {
            live,
            encoded: cache,
        }
    }

    /// The block as the store holds it, encoded once however many
    /// sessions ask.
    ///
    /// Cheap after the first call: [`Bytes`] shares its buffer, so
    /// every subscriber after the first pays a reference count.
    pub fn encoded(&self) -> Bytes {
        self.encoded
            .get_or_init(|| crate::journal::stored_properties(&self.live))
            .clone()
    }

    /// The block as the broker reads it.
    ///
    /// [`Deref`] covers reading a field; this is for the callers that
    /// want the whole block, where `(*shared).clone()` would be
    /// ambiguous to a reader about which of the two is being cloned.
    pub fn live(&self) -> &Properties {
        &self.live
    }
}

/// Two blocks are equal when what they say is equal.
///
/// The cache is a derived value and is deliberately not compared: a
/// block somebody has journalled must not stop being equal to the same
/// block nobody has, or a comparison would report a difference that
/// reading it had caused.
impl PartialEq for SharedProperties {
    fn eq(&self, other: &Self) -> bool {
        self.live == other.live
    }
}

impl Eq for SharedProperties {}

impl Deref for SharedProperties {
    type Target = Properties;

    fn deref(&self) -> &Properties {
        &self.live
    }
}

impl From<Properties> for SharedProperties {
    fn from(live: Properties) -> Self {
        Self::new(live)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_block() -> Properties {
        Properties {
            content_type: Some("application/json".to_owned()),
            user_properties: vec![("tenant".to_owned(), "acme".to_owned())],
            ..Properties::default()
        }
    }

    #[test]
    fn the_encoding_is_what_the_codec_would_have_written() {
        // Convention 5, a round trip cannot see a consistent error: the
        // cache must hold what `stored_properties` produces, not merely
        // something that decodes back to the same block.
        let shared = SharedProperties::new(a_block());
        assert_eq!(
            shared.encoded(),
            crate::journal::stored_properties(&a_block())
        );
    }

    #[test]
    fn every_asker_after_the_first_gets_the_same_buffer() {
        // The whole point. Two calls that allocated twice would encode
        // twice, which is exactly what this type exists to stop, and
        // equal bytes cannot tell the difference: the pointer can.
        let shared = SharedProperties::new(a_block());
        let first = shared.encoded();
        let second = shared.encoded();
        assert!(
            std::ptr::eq(first.as_ptr(), second.as_ptr()),
            "the second asker re-encoded the block instead of sharing it"
        );
    }

    #[test]
    fn a_block_given_its_encoding_does_not_encode_it_again() {
        // The restore path hands both halves over. If this encoded
        // anyway it would be right and slow, which no other test here
        // can see, so this asserts the buffer it was given is the
        // buffer it hands out.
        let encoded = crate::journal::stored_properties(&a_block());
        let shared = SharedProperties::from_encoded(a_block(), encoded.clone());
        assert!(
            std::ptr::eq(shared.encoded().as_ptr(), encoded.as_ptr()),
            "the encoding handed in was thrown away and taken again"
        );
    }

    #[test]
    fn reading_a_property_goes_through_to_the_live_block() {
        let shared = SharedProperties::new(a_block());
        assert_eq!(shared.content_type.as_deref(), Some("application/json"));
        assert_eq!(shared.live(), &a_block());
    }
}
