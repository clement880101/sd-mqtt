//! What an `http_out` rule posts.
//!
//! Section 9.2: the topic, the payload as a UTF-8 string when it is valid
//! and base64 otherwise, the quality of service, the retain flag, the
//! publishing client's identifier, a timestamp, and the version 5 user
//! properties.
//!
//! # This is a wire format too
//!
//! The same argument the storage crate makes about its records applies
//! here with the blame moved outside: these bytes are parsed by somebody
//! else's program, and a field this crate renames is that program
//! breaking rather than ours. A test that serialises an envelope and
//! parses it back agrees with itself by construction and cannot see a
//! renamed field, a reordered one, or a number that became a string.
//!
//! So [`tests::golden`] asserts the exact JSON text for a payload that is
//! valid UTF-8 and for one that is not, written out by hand.
//!
//! # Exactly one of two payload fields
//!
//! `payload` carries the text and `payload_base64` carries the bytes, and
//! **an envelope has one of them and never both**. A single field holding
//! either would be ambiguous — a payload whose text happens to be valid
//! base64 is indistinguishable from an encoded one — and a receiver would
//! have to guess. Two fields make the question the receiver asks
//! structural: whichever one is present says what it is holding.
//!
//! # `user_properties` is an array of pairs, not an object
//!
//! MQTT 5 permits the same user property name more than once and the
//! order is significant ([MQTT-3.3.2-13] keeps them in the order they
//! were sent). A JSON object would silently collapse duplicates and has
//! no order, so the pairs are an array of two-element arrays.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use serde::Serialize;
use storage::record::{BodyRecord, MessageId, OutboxRecord, Qos};

/// The JSON document an `http_out` rule posts.
///
/// **Field order is the format**, in the same sense the storage records
/// mean it: `serde_json` writes a struct's fields in declaration order,
/// and the golden vectors pin that. A reader that parses JSON properly
/// does not care, and one that does not is not this crate's problem —
/// but a diff that moves a field should still have to be deliberate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Envelope {
    /// What names this delivery, as thirty-two lowercase hexadecimal
    /// characters.
    ///
    /// **Stable across every attempt at one envelope**, which is the
    /// whole of what makes it useful. Delivery is at least once: a
    /// timeout cannot be told from a failure, so a retry after one may
    /// repeat a post that in fact arrived. A receiver that remembers
    /// this identifier can tell the repeat from a new message. One
    /// minted per attempt would look like a new message every time,
    /// which is the same as not having one.
    pub id: String,
    /// The topic the message was published on.
    pub topic: String,
    /// The payload as text, when every byte of it is valid UTF-8.
    ///
    /// Absent from the document entirely when it is not; see
    /// [`Self::payload_base64`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    /// The payload base64-encoded, when it is not valid UTF-8.
    ///
    /// Absent from the document entirely when it is. Standard base64
    /// with padding, RFC 4648 section 4, which is what every language's
    /// standard library decodes without being told which alphabet.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_base64: Option<String>,
    /// The level it was published at, 0, 1 or 2.
    ///
    /// The wire number rather than a name, because that is what an MQTT
    /// receiver already speaks.
    pub qos: u8,
    /// Whether the publisher asked for it to be retained.
    pub retain: bool,
    /// The identifier of the client that published it.
    pub client_id: String,
    /// When the rule fired, in whole seconds since the epoch.
    pub timestamp: u32,
    /// The MQTT 5 user properties, in the order they were sent.
    pub user_properties: Vec<(String, String)>,
}

impl Envelope {
    /// Build the envelope for one waiting delivery.
    ///
    /// Everything but the client identifier and the time comes from the
    /// stored body, which is the same row the broker's own queues
    /// reference: one payload however many rules fired on it.
    pub fn build(entry: &OutboxRecord, body: &BodyRecord) -> Self {
        let (payload, payload_base64) = match std::str::from_utf8(&body.payload) {
            Ok(text) => (Some(text.to_owned()), None),
            Err(_) => (None, Some(BASE64.encode(&body.payload))),
        };
        Self {
            id: hexadecimal(&entry.id),
            topic: body.topic.clone(),
            payload,
            payload_base64,
            qos: match body.qos {
                Qos::AtMostOnce => 0,
                Qos::AtLeastOnce => 1,
                Qos::ExactlyOnce => 2,
            },
            retain: body.retain,
            client_id: entry.client_id.clone(),
            timestamp: entry.enqueued_at,
            user_properties: broker_core::journal::live_properties(&body.properties)
                .user_properties,
        }
    }

    /// The document, as the bytes that go on the wire.
    ///
    /// # Panics
    ///
    /// Never in practice: every field is a string, a number, a boolean or
    /// a sequence of those, and `serde_json` cannot fail on any of them.
    /// A map with non-string keys or a float that is not a number are the
    /// ways this can fail and neither is reachable from these types.
    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("an envelope is strings, numbers and booleans")
    }
}

/// Sixteen bytes as thirty-two lowercase hexadecimal characters.
///
/// Written out rather than taken from a crate, because the one dependency
/// that would supply it is `uuid`, and this identifier is deliberately
/// not promised to be a UUID: what a receiver needs is a stable opaque
/// string, and promising a shape is promising to keep it.
fn hexadecimal(id: &MessageId) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(32);
    for byte in id.0 {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mqtt_codec::Properties;

    /// An outbox entry whose identifier is legible in the golden JSON.
    fn entry(id: [u8; 16], client_id: &str, at: u32) -> OutboxRecord {
        OutboxRecord {
            sequence: 7,
            id: MessageId(id),
            body: MessageId([0x99; 16]),
            client_id: client_id.to_owned(),
            enqueued_at: at,
            attempts: 3,
            next_attempt_at: 1_700_000_100,
        }
    }

    fn body(topic: &str, payload: &'static [u8], qos: Qos, retain: bool) -> BodyRecord {
        BodyRecord {
            topic: topic.to_owned(),
            payload: Bytes::from_static(payload),
            qos,
            retain,
            expires_at: None,
            properties: Bytes::new(),
        }
    }

    /// A stored property block carrying these user properties.
    fn with_user_properties(mut record: BodyRecord, pairs: &[(&str, &str)]) -> BodyRecord {
        let properties = Properties {
            user_properties: pairs
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
            ..Properties::default()
        };
        record.properties = broker_core::journal::stored_properties(&properties);
        record
    }

    /// The exact bytes, for both shapes of payload.
    ///
    /// Written out by hand. A test that parsed its own output back would
    /// agree with the encoder by construction and could not see a field
    /// renamed, reordered, or turned from a number into a string —
    /// which is somebody else's program breaking rather than ours, and
    /// therefore the failure we would hear about last.
    mod golden {
        use super::*;

        #[track_caller]
        fn pins(envelope: &Envelope, expected: &str) {
            assert_eq!(
                String::from_utf8(envelope.to_json()).expect("the document is text"),
                expected
            );
        }

        #[test]
        fn a_payload_that_is_valid_text() {
            let envelope = Envelope::build(
                &entry(
                    [
                        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98,
                        0x76, 0x54, 0x32, 0x10,
                    ],
                    "sensor-1",
                    1_700_000_000,
                ),
                &with_user_properties(
                    body("sensor/1/temp", b"21.5", Qos::AtLeastOnce, false),
                    // The same name twice, which is what an object would
                    // have collapsed into one.
                    &[("unit", "celsius"), ("unit", "also-celsius")],
                ),
            );
            pins(
                &envelope,
                concat!(
                    r#"{"id":"0123456789abcdeffedcba9876543210","#,
                    r#""topic":"sensor/1/temp","#,
                    r#""payload":"21.5","#,
                    r#""qos":1,"#,
                    r#""retain":false,"#,
                    r#""client_id":"sensor-1","#,
                    r#""timestamp":1700000000,"#,
                    r#""user_properties":[["unit","celsius"],["unit","also-celsius"]]}"#,
                ),
            );
            assert!(
                !String::from_utf8(envelope.to_json())
                    .expect("text")
                    .contains("payload_base64"),
                "a text payload must not carry the encoded field at all"
            );
        }

        #[test]
        fn a_payload_that_is_not_text() {
            // 0xff is not valid UTF-8 anywhere, in any position.
            let envelope = Envelope::build(
                &entry([0xaa; 16], "sensor-2", 1_700_000_001),
                &body("sensor/2/raw", &[0x00, 0x01, 0xff], Qos::ExactlyOnce, true),
            );
            pins(
                &envelope,
                concat!(
                    r#"{"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","#,
                    r#""topic":"sensor/2/raw","#,
                    r#""payload_base64":"AAH/","#,
                    r#""qos":2,"#,
                    r#""retain":true,"#,
                    r#""client_id":"sensor-2","#,
                    r#""timestamp":1700000001,"#,
                    r#""user_properties":[]}"#,
                ),
            );
            assert!(
                !String::from_utf8(envelope.to_json())
                    .expect("text")
                    .contains(r#""payload""#),
                "a binary payload must not carry the text field at all"
            );
        }

        /// An empty payload is text, and the empty string is what it is.
        ///
        /// Worth pinning because the obvious implementation of "is this
        /// text" on an empty slice could go either way, and a receiver
        /// meeting `payload_base64: ""` for a message with no payload
        /// would be entitled to be confused.
        #[test]
        fn an_empty_payload_is_the_empty_string() {
            let envelope = Envelope::build(
                &entry([0x00; 16], "sensor-3", 1_700_000_002),
                &body("sensor/3", b"", Qos::AtMostOnce, false),
            );
            pins(
                &envelope,
                concat!(
                    r#"{"id":"00000000000000000000000000000000","#,
                    r#""topic":"sensor/3","#,
                    r#""payload":"","#,
                    r#""qos":0,"#,
                    r#""retain":false,"#,
                    r#""client_id":"sensor-3","#,
                    r#""timestamp":1700000002,"#,
                    r#""user_properties":[]}"#,
                ),
            );
        }
    }

    /// The identifier is the stored one, unchanged, however often the
    /// envelope is built.
    ///
    /// Building the envelope is what every attempt does, so an
    /// identifier minted here rather than read would be a new one per
    /// attempt — which is exactly the deduplication the at-least-once
    /// guarantee depends on, broken.
    #[test]
    fn the_identifier_is_the_stored_one_and_does_not_change_per_attempt() {
        let stored = entry([0x5a; 16], "sensor-1", 1_700_000_000);
        let record = body("sensor/1/temp", b"21.5", Qos::AtLeastOnce, false);

        let first = Envelope::build(&stored, &record);
        let second = Envelope::build(&stored, &record);
        assert_eq!(
            first.id, second.id,
            "two attempts at one envelope carried different identifiers"
        );
        assert_eq!(first.id, "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a");

        // A different envelope is a different identifier, so the
        // assertion above is not passing on a constant.
        let other = Envelope::build(&entry([0x5b; 16], "sensor-1", 1_700_000_000), &record);
        assert_ne!(first.id, other.id);
    }

    /// Every level survives the crossing, with the wire's own numbering.
    #[test]
    fn each_level_is_the_number_mqtt_uses_for_it() {
        for (stored, expected) in [
            (Qos::AtMostOnce, 0),
            (Qos::AtLeastOnce, 1),
            (Qos::ExactlyOnce, 2),
        ] {
            let envelope = Envelope::build(
                &entry([0x01; 16], "sensor", 1),
                &body("a", b"x", stored, false),
            );
            assert_eq!(envelope.qos, expected, "{stored:?} became {}", envelope.qos);
        }
    }
}
