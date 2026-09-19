//! MQTT 5 properties.

use bytes::Bytes;

use crate::error::CodecError;
use crate::reader::Reader;
use crate::varint;
use crate::writer::{put_binary, put_string, put_u16, put_u32, put_u8};

/// Which packet a property block belongs to.
///
/// MQTT 5 permits a different subset of properties on each packet, and a
/// property outside that subset is a protocol error rather than something to
/// ignore. The will has its own subset, distinct from the CONNECT that
/// carries it.
///
/// The subsets are taken from the OASIS MQTT 5.0 specification: the
/// cross-reference table in 2.2.2.2, confirmed against the per-packet
/// property subsections 3.1.2.11 (CONNECT), 3.1.3.2 (will), 3.2.2.3
/// (CONNACK), 3.3.2.3 (PUBLISH), 3.4.2.2 and its PUBREC, PUBREL and PUBCOMP
/// siblings, 3.8.2.1 (SUBSCRIBE), 3.9.2.1 (SUBACK), 3.10.2.1 (UNSUBSCRIBE),
/// 3.11.2.1 (UNSUBACK), 3.14.2.2 (DISCONNECT) and 3.15.2.2 (AUTH). PINGREQ
/// and PINGRESP carry no property block at all and so have no scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum PropertyScope {
    Connect,
    Will,
    ConnAck,
    Publish,
    /// PUBACK, PUBREC, PUBREL and PUBCOMP share one subset.
    Ack,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    Disconnect,
    Auth,
}

/// Build a membership mask from a list of property identifiers.
///
/// Identifiers run from 0x01 to 0x2A, so the whole set fits in 64 bits and
/// membership is one shift and one test.
const fn mask(ids: &[u8]) -> u64 {
    let mut bits = 0u64;
    let mut index = 0;
    while index < ids.len() {
        bits |= 1u64 << ids[index];
        index += 1;
    }
    bits
}

impl PropertyScope {
    fn allowed(self) -> u64 {
        match self {
            PropertyScope::Connect => mask(&[0x11, 0x15, 0x16, 0x17, 0x19, 0x21, 0x22, 0x26, 0x27]),
            PropertyScope::Will => mask(&[0x01, 0x02, 0x03, 0x08, 0x09, 0x18, 0x26]),
            PropertyScope::ConnAck => mask(&[
                0x11, 0x12, 0x13, 0x15, 0x16, 0x1A, 0x1C, 0x1F, 0x21, 0x22, 0x24, 0x25, 0x26, 0x27,
                0x28, 0x29, 0x2A,
            ]),
            PropertyScope::Publish => mask(&[0x01, 0x02, 0x03, 0x08, 0x09, 0x0B, 0x23, 0x26]),
            PropertyScope::Ack => mask(&[0x1F, 0x26]),
            PropertyScope::Subscribe => mask(&[0x0B, 0x26]),
            PropertyScope::SubAck => mask(&[0x1F, 0x26]),
            PropertyScope::Unsubscribe => mask(&[0x26]),
            PropertyScope::UnsubAck => mask(&[0x1F, 0x26]),
            PropertyScope::Disconnect => mask(&[0x11, 0x1C, 0x1F, 0x26]),
            PropertyScope::Auth => mask(&[0x15, 0x16, 0x1F, 0x26]),
        }
    }

    fn permits(self, id: u32) -> bool {
        id < 64 && self.allowed() & (1u64 << id) != 0
    }
}

/// Every MQTT 5 property, gathered into one structure.
///
/// A given packet type permits only a subset. [`PropertyScope`] holds those
/// subsets, and [`Properties::decode_for`] and [`Properties::encode_for`]
/// enforce them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct Properties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Bytes>,
    pub subscription_identifiers: Vec<u32>,
    pub session_expiry_interval: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub server_keep_alive: Option<u16>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Bytes>,
    pub request_problem_information: Option<u8>,
    pub will_delay_interval: Option<u32>,
    pub request_response_information: Option<u8>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub reason_string: Option<String>,
    pub receive_maximum: Option<u16>,
    pub topic_alias_maximum: Option<u16>,
    pub topic_alias: Option<u16>,
    pub maximum_qos: Option<u8>,
    pub retain_available: Option<u8>,
    pub user_properties: Vec<(String, String)>,
    pub maximum_packet_size: Option<u32>,
    pub wildcard_subscription_available: Option<u8>,
    pub subscription_identifier_available: Option<u8>,
    pub shared_subscription_available: Option<u8>,
}

/// Write a property identifier, refusing it when the scope does not permit it.
///
/// The check sits on the write itself so the identifier checked is literally
/// the identifier emitted, with no parallel list to drift out of step.
fn push_id(body: &mut Vec<u8>, id: u8, scope: Option<PropertyScope>) -> Result<(), CodecError> {
    if let Some(scope) = scope {
        if !scope.permits(u32::from(id)) {
            return Err(CodecError::ProtocolError(
                "property is not permitted on this packet type",
            ));
        }
    }
    body.push(id);
    Ok(())
}

/// Reject a second subscription identifier where only one may appear.
///
/// PUBLISH may carry several: MQTT-v5.0 section 3.3.2.3.8 says "Multiple
/// Subscription Identifiers will be included if the publication is the
/// result of a match to more than one subscription", which is why the field
/// is a vector. SUBSCRIBE may carry one: section 3.8.2.1.2 says "It is a
/// Protocol Error to include the Subscription Identifier more than once".
///
/// Scopes other than subscribe are left alone, and so is the unscoped form,
/// which by definition has no packet to judge against.
fn check_subscription_identifier_count(
    scope: Option<PropertyScope>,
    already: usize,
) -> Result<(), CodecError> {
    if scope == Some(PropertyScope::Subscribe) && already > 0 {
        return Err(CodecError::ProtocolError(
            "subscription identifier appears more than once",
        ));
    }
    Ok(())
}

/// Reject a second occurrence of a property that may appear only once.
fn once<T>(slot: &mut Option<T>, value: T) -> Result<(), CodecError> {
    if slot.is_some() {
        return Err(CodecError::ProtocolError("property appears more than once"));
    }
    *slot = Some(value);
    Ok(())
}

impl Properties {
    /// Whether nothing is set, in which case the block is a single zero byte.
    pub fn is_empty(&self) -> bool {
        *self == Properties::default()
    }

    /// Decode a property block without checking which packet permits what.
    ///
    /// Prefer [`Properties::decode_for`]. This exists for callers that have
    /// no packet context, such as a property block read in isolation by a
    /// test or a tool. Hidden from the documentation so that it does not
    /// read as the recommended entry point: it skips the per-packet subset
    /// check, and every caller that knows its packet should keep it.
    #[doc(hidden)]
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        Self::decode_inner(reader, None)
    }

    /// Decode a property block, rejecting anything the packet does not permit.
    pub fn decode_for(reader: &mut Reader<'_>, scope: PropertyScope) -> Result<Self, CodecError> {
        Self::decode_inner(reader, Some(scope))
    }

    /// Decode a property block from the current position.
    ///
    /// Reads the block length, then exactly that many bytes, leaving the
    /// reader positioned at the first byte after the block.
    fn decode_inner(
        reader: &mut Reader<'_>,
        scope: Option<PropertyScope>,
    ) -> Result<Self, CodecError> {
        let len = reader.varint()? as usize;
        let body = reader.slice(len)?;
        let mut inner = Reader::new(body);
        let mut props = Properties::default();

        while !inner.is_empty() {
            let id = inner.varint()?;
            if let Some(scope) = scope {
                if !scope.permits(id) {
                    return Err(CodecError::ProtocolError(
                        "property is not permitted on this packet type",
                    ));
                }
            }
            match id {
                0x01 => once(&mut props.payload_format_indicator, inner.u8()?)?,
                0x02 => once(&mut props.message_expiry_interval, inner.u32()?)?,
                0x03 => once(&mut props.content_type, inner.string()?)?,
                0x08 => once(&mut props.response_topic, inner.string()?)?,
                0x09 => once(&mut props.correlation_data, inner.binary()?)?,
                0x0B => {
                    let value = inner.varint()?;
                    if value == 0 {
                        return Err(CodecError::ProtocolError(
                            "subscription identifier must not be zero",
                        ));
                    }
                    check_subscription_identifier_count(
                        scope,
                        props.subscription_identifiers.len(),
                    )?;
                    props.subscription_identifiers.push(value);
                }
                0x11 => once(&mut props.session_expiry_interval, inner.u32()?)?,
                0x12 => once(&mut props.assigned_client_identifier, inner.string()?)?,
                0x13 => once(&mut props.server_keep_alive, inner.u16()?)?,
                0x15 => once(&mut props.authentication_method, inner.string()?)?,
                0x16 => once(&mut props.authentication_data, inner.binary()?)?,
                0x17 => once(&mut props.request_problem_information, inner.u8()?)?,
                0x18 => once(&mut props.will_delay_interval, inner.u32()?)?,
                0x19 => once(&mut props.request_response_information, inner.u8()?)?,
                0x1A => once(&mut props.response_information, inner.string()?)?,
                0x1C => once(&mut props.server_reference, inner.string()?)?,
                0x1F => once(&mut props.reason_string, inner.string()?)?,
                0x21 => once(&mut props.receive_maximum, inner.u16()?)?,
                0x22 => once(&mut props.topic_alias_maximum, inner.u16()?)?,
                0x23 => once(&mut props.topic_alias, inner.u16()?)?,
                0x24 => once(&mut props.maximum_qos, inner.u8()?)?,
                0x25 => once(&mut props.retain_available, inner.u8()?)?,
                0x26 => {
                    let key = inner.string()?;
                    let value = inner.string()?;
                    props.user_properties.push((key, value));
                }
                0x27 => once(&mut props.maximum_packet_size, inner.u32()?)?,
                0x28 => once(&mut props.wildcard_subscription_available, inner.u8()?)?,
                0x29 => once(&mut props.subscription_identifier_available, inner.u8()?)?,
                0x2A => once(&mut props.shared_subscription_available, inner.u8()?)?,
                _ => return Err(CodecError::MalformedPacket("unknown property identifier")),
            }
        }

        Ok(props)
    }

    /// Append the property block, including its length prefix.
    ///
    /// Prefer [`Properties::encode_for`]. This writes whatever is set,
    /// without checking which packet permits what. Hidden from the
    /// documentation for the same reason as [`Properties::decode`].
    #[doc(hidden)]
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        self.encode_inner(None, out)
    }

    /// Append the property block, refusing anything the packet does not permit.
    ///
    /// The encoder refuses what the decoder rejects: a packet this crate
    /// wrote must be one it would accept, so a property outside the packet's
    /// subset is an error here as well as on the way in.
    pub fn encode_for(&self, scope: PropertyScope, out: &mut Vec<u8>) -> Result<(), CodecError> {
        self.encode_inner(Some(scope), out)
    }

    fn encode_inner(
        &self,
        scope: Option<PropertyScope>,
        out: &mut Vec<u8>,
    ) -> Result<(), CodecError> {
        let mut body = Vec::new();

        if let Some(v) = self.payload_format_indicator {
            push_id(&mut body, 0x01, scope)?;
            put_u8(&mut body, v);
        }
        if let Some(v) = self.message_expiry_interval {
            push_id(&mut body, 0x02, scope)?;
            put_u32(&mut body, v);
        }
        if let Some(v) = &self.content_type {
            push_id(&mut body, 0x03, scope)?;
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.response_topic {
            push_id(&mut body, 0x08, scope)?;
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.correlation_data {
            push_id(&mut body, 0x09, scope)?;
            put_binary(&mut body, v)?;
        }
        for (index, v) in self.subscription_identifiers.iter().enumerate() {
            if *v == 0 {
                return Err(CodecError::ProtocolError(
                    "subscription identifier must not be zero",
                ));
            }
            check_subscription_identifier_count(scope, index)?;
            push_id(&mut body, 0x0B, scope)?;
            varint::encode(*v, &mut body)?;
        }
        if let Some(v) = self.session_expiry_interval {
            push_id(&mut body, 0x11, scope)?;
            put_u32(&mut body, v);
        }
        if let Some(v) = &self.assigned_client_identifier {
            push_id(&mut body, 0x12, scope)?;
            put_string(&mut body, v)?;
        }
        if let Some(v) = self.server_keep_alive {
            push_id(&mut body, 0x13, scope)?;
            put_u16(&mut body, v);
        }
        if let Some(v) = &self.authentication_method {
            push_id(&mut body, 0x15, scope)?;
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.authentication_data {
            push_id(&mut body, 0x16, scope)?;
            put_binary(&mut body, v)?;
        }
        if let Some(v) = self.request_problem_information {
            push_id(&mut body, 0x17, scope)?;
            put_u8(&mut body, v);
        }
        if let Some(v) = self.will_delay_interval {
            push_id(&mut body, 0x18, scope)?;
            put_u32(&mut body, v);
        }
        if let Some(v) = self.request_response_information {
            push_id(&mut body, 0x19, scope)?;
            put_u8(&mut body, v);
        }
        if let Some(v) = &self.response_information {
            push_id(&mut body, 0x1A, scope)?;
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.server_reference {
            push_id(&mut body, 0x1C, scope)?;
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.reason_string {
            push_id(&mut body, 0x1F, scope)?;
            put_string(&mut body, v)?;
        }
        if let Some(v) = self.receive_maximum {
            push_id(&mut body, 0x21, scope)?;
            put_u16(&mut body, v);
        }
        if let Some(v) = self.topic_alias_maximum {
            push_id(&mut body, 0x22, scope)?;
            put_u16(&mut body, v);
        }
        if let Some(v) = self.topic_alias {
            push_id(&mut body, 0x23, scope)?;
            put_u16(&mut body, v);
        }
        if let Some(v) = self.maximum_qos {
            push_id(&mut body, 0x24, scope)?;
            put_u8(&mut body, v);
        }
        if let Some(v) = self.retain_available {
            push_id(&mut body, 0x25, scope)?;
            put_u8(&mut body, v);
        }
        for (key, value) in &self.user_properties {
            push_id(&mut body, 0x26, scope)?;
            put_string(&mut body, key)?;
            put_string(&mut body, value)?;
        }
        if let Some(v) = self.maximum_packet_size {
            push_id(&mut body, 0x27, scope)?;
            put_u32(&mut body, v);
        }
        if let Some(v) = self.wildcard_subscription_available {
            push_id(&mut body, 0x28, scope)?;
            put_u8(&mut body, v);
        }
        if let Some(v) = self.subscription_identifier_available {
            push_id(&mut body, 0x29, scope)?;
            put_u8(&mut body, v);
        }
        if let Some(v) = self.shared_subscription_available {
            push_id(&mut body, 0x2A, scope)?;
            put_u8(&mut body, v);
        }

        let len = u32::try_from(body.len())
            .map_err(|_| CodecError::MalformedPacket("property block too large"))?;
        varint::encode(len, out)?;
        out.extend_from_slice(&body);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;
    use crate::reader::Reader;
    use bytes::Bytes;

    fn round_trip(props: &Properties) -> Properties {
        let mut out = Vec::new();
        props.encode(&mut out).expect("encodes");
        let mut reader = Reader::new(&out);
        Properties::decode(&mut reader).expect("decodes")
    }

    #[test]
    fn an_empty_set_encodes_as_a_single_zero_byte() {
        let mut out = Vec::new();
        Properties::default().encode(&mut out).unwrap();
        assert_eq!(out, vec![0x00]);
    }

    #[test]
    fn round_trips_every_scalar_kind() {
        let props = Properties {
            payload_format_indicator: Some(1),
            message_expiry_interval: Some(3600),
            server_keep_alive: Some(60),
            content_type: Some("application/json".into()),
            correlation_data: Some(Bytes::from_static(b"abc")),
            subscription_identifiers: vec![1, 268_435_455],
            user_properties: vec![("a".into(), "1".into()), ("b".into(), "2".into())],
            ..Properties::default()
        };
        assert_eq!(round_trip(&props), props);
    }

    #[test]
    fn preserves_the_order_of_repeated_properties() {
        let props = Properties {
            user_properties: vec![("k".into(), "first".into()), ("k".into(), "second".into())],
            ..Properties::default()
        };
        assert_eq!(round_trip(&props).user_properties, props.user_properties);
    }

    #[test]
    fn rejects_a_duplicated_single_valued_property() {
        // Length 10 (0x0A), then session expiry (0x11) twice with truncated values.
        let bytes = [0x0A, 0x11, 0, 0, 0, 1, 0x11, 0, 0, 0, 2];
        let mut reader = Reader::new(&bytes);
        let err = Properties::decode(&mut reader).unwrap_err();
        assert!(matches!(err, CodecError::ProtocolError(_)));
    }

    #[test]
    fn rejects_an_unknown_property_identifier() {
        let bytes = [0x02, 0x7E, 0x00];
        let mut reader = Reader::new(&bytes);
        let err = Properties::decode(&mut reader).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_a_property_block_that_overruns_its_declared_length() {
        // Declares 10 bytes of properties but supplies 2.
        let bytes = [0x0A, 0x01, 0x01];
        let mut reader = Reader::new(&bytes);
        assert!(Properties::decode(&mut reader).is_err());
    }

    #[test]
    fn rejects_a_zero_subscription_identifier() {
        let bytes = [0x02, 0x0B, 0x00];
        let mut reader = Reader::new(&bytes);
        let err = Properties::decode(&mut reader).unwrap_err();
        assert!(matches!(err, CodecError::ProtocolError(_)));
    }

    #[test]
    fn rejects_encoding_a_zero_subscription_identifier() {
        let props = Properties {
            subscription_identifiers: vec![0],
            ..Properties::default()
        };
        let mut out = Vec::new();
        let err = props.encode(&mut out).unwrap_err();
        assert!(matches!(err, CodecError::ProtocolError(_)));
    }

    #[test]
    fn stops_at_the_end_of_the_property_block() {
        // One property, then a trailing byte belonging to the packet body.
        let bytes = [0x02, 0x01, 0x01, 0xAA];
        let mut reader = Reader::new(&bytes);
        let props = Properties::decode(&mut reader).unwrap();
        assert_eq!(props.payload_format_indicator, Some(1));
        assert_eq!(reader.u8().unwrap(), 0xAA);
    }

    /// Every identifier has now been checked by hand against the OASIS
    /// MQTT 5.0 property table, so this locks in a verified-correct
    /// encoding rather than merely locking in whatever the code currently
    /// does. Encoding and decoding through the *same* identifier table (as
    /// `round_trip` above does) cannot catch a transposed identifier; this
    /// literal byte vector can.
    #[test]
    fn golden_vector_for_every_property() {
        let props = Properties {
            payload_format_indicator: Some(1),
            message_expiry_interval: Some(2),
            content_type: Some("s3".into()),
            response_topic: Some("s4".into()),
            correlation_data: Some(Bytes::from_static(&[5])),
            subscription_identifiers: vec![6],
            session_expiry_interval: Some(7),
            assigned_client_identifier: Some("s8".into()),
            server_keep_alive: Some(9),
            authentication_method: Some("s10".into()),
            authentication_data: Some(Bytes::from_static(&[11])),
            request_problem_information: Some(12),
            will_delay_interval: Some(13),
            request_response_information: Some(14),
            response_information: Some("s15".into()),
            server_reference: Some("s16".into()),
            reason_string: Some("s17".into()),
            receive_maximum: Some(18),
            topic_alias_maximum: Some(19),
            topic_alias: Some(20),
            maximum_qos: Some(21),
            retain_available: Some(22),
            user_properties: vec![("k23".into(), "v23".into())],
            maximum_packet_size: Some(24),
            wildcard_subscription_available: Some(25),
            subscription_identifier_available: Some(26),
            shared_subscription_available: Some(27),
        };
        let mut out = Vec::new();
        props.encode(&mut out).unwrap();
        // Identifier bytes below (1, 2, 3, 8, 9, 11, 17, 18, 19, 21, 22, 23,
        // 24, 25, 26, 28, 31, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42) are
        // 0x01..0x2A, ascending and matching the table in this file.
        assert_eq!(
            out,
            vec![
                108, 1, 1, 2, 0, 0, 0, 2, 3, 0, 2, 115, 51, 8, 0, 2, 115, 52, 9, 0, 1, 5, 11, 6,
                17, 0, 0, 0, 7, 18, 0, 2, 115, 56, 19, 0, 9, 21, 0, 3, 115, 49, 48, 22, 0, 1, 11,
                23, 12, 24, 0, 0, 0, 13, 25, 14, 26, 0, 3, 115, 49, 53, 28, 0, 3, 115, 49, 54, 31,
                0, 3, 115, 49, 55, 33, 0, 18, 34, 0, 19, 35, 0, 20, 36, 21, 37, 22, 38, 0, 3, 107,
                50, 51, 0, 3, 118, 50, 51, 39, 0, 0, 0, 24, 40, 25, 41, 26, 42, 27,
            ]
        );
    }

    #[test]
    fn rejects_a_property_the_packet_does_not_permit() {
        // A topic alias is legal on PUBLISH and illegal on CONNECT.
        let bytes = [0x03, 0x23, 0x00, 0x0A];

        let mut reader = Reader::new(&bytes);
        let ok = Properties::decode_for(&mut reader, PropertyScope::Publish);
        assert_eq!(ok.unwrap().topic_alias, Some(10));

        let mut reader = Reader::new(&bytes);
        let err = Properties::decode_for(&mut reader, PropertyScope::Connect).unwrap_err();
        assert_eq!(
            err,
            CodecError::ProtocolError("property is not permitted on this packet type")
        );
    }

    #[test]
    fn permits_user_properties_everywhere_they_are_allowed() {
        let bytes = [0x07, 0x26, 0x00, 0x01, b'k', 0x00, 0x01, b'v'];
        for scope in [
            PropertyScope::Connect,
            PropertyScope::Will,
            PropertyScope::ConnAck,
            PropertyScope::Publish,
            PropertyScope::Ack,
            PropertyScope::Subscribe,
            PropertyScope::SubAck,
            PropertyScope::Unsubscribe,
            PropertyScope::UnsubAck,
            PropertyScope::Disconnect,
            PropertyScope::Auth,
        ] {
            let mut reader = Reader::new(&bytes);
            let props = Properties::decode_for(&mut reader, scope).expect("allowed");
            assert_eq!(props.user_properties.len(), 1, "{scope:?}");
        }
    }

    #[test]
    fn the_unscoped_decoder_still_permits_everything() {
        let bytes = [0x03, 0x23, 0x00, 0x0A];
        let mut reader = Reader::new(&bytes);
        assert!(Properties::decode(&mut reader).is_ok());
    }

    /// The whole table, checked one identifier at a time against every scope.
    ///
    /// Each row is a scope and the identifiers the OASIS MQTT 5.0
    /// specification permits on it (2.2.2.2, confirmed against each packet's
    /// own property subsection). Every identifier from 0x01 to 0x2A is then
    /// offered to every scope, so an identifier wrongly *added* to a mask
    /// fails here just as loudly as one wrongly left out. The rows are
    /// written out again rather than read from `PropertyScope::allowed`, so
    /// this pins the table rather than restating it.
    #[test]
    fn the_table_matches_the_specification() {
        const TABLE: &[(PropertyScope, &[u8])] = &[
            (
                PropertyScope::Connect,
                &[0x11, 0x15, 0x16, 0x17, 0x19, 0x21, 0x22, 0x26, 0x27],
            ),
            (
                PropertyScope::Will,
                &[0x01, 0x02, 0x03, 0x08, 0x09, 0x18, 0x26],
            ),
            (
                PropertyScope::ConnAck,
                &[
                    0x11, 0x12, 0x13, 0x15, 0x16, 0x1A, 0x1C, 0x1F, 0x21, 0x22, 0x24, 0x25, 0x26,
                    0x27, 0x28, 0x29, 0x2A,
                ],
            ),
            (
                PropertyScope::Publish,
                &[0x01, 0x02, 0x03, 0x08, 0x09, 0x0B, 0x23, 0x26],
            ),
            (PropertyScope::Ack, &[0x1F, 0x26]),
            (PropertyScope::Subscribe, &[0x0B, 0x26]),
            (PropertyScope::SubAck, &[0x1F, 0x26]),
            (PropertyScope::Unsubscribe, &[0x26]),
            (PropertyScope::UnsubAck, &[0x1F, 0x26]),
            (PropertyScope::Disconnect, &[0x11, 0x1C, 0x1F, 0x26]),
            (PropertyScope::Auth, &[0x15, 0x16, 0x1F, 0x26]),
        ];

        for (scope, permitted) in TABLE {
            for id in 0x01u8..=0x2A {
                assert_eq!(
                    scope.permits(u32::from(id)),
                    permitted.contains(&id),
                    "{scope:?} and identifier {id:#04x}"
                );
            }
        }
    }

    #[test]
    fn the_scoped_encoder_refuses_what_the_scoped_decoder_would_reject() {
        let props = Properties {
            topic_alias: Some(10),
            ..Properties::default()
        };

        let mut out = Vec::new();
        props
            .encode_for(PropertyScope::Publish, &mut out)
            .expect("a topic alias is legal on PUBLISH");

        let mut out = Vec::new();
        let err = props
            .encode_for(PropertyScope::Connect, &mut out)
            .unwrap_err();
        assert_eq!(
            err,
            CodecError::ProtocolError("property is not permitted on this packet type")
        );
        assert!(out.is_empty(), "a refused block must write nothing");
    }

    /// MQTT-v5.0 section 3.8.2.1.2: "It is a Protocol Error to include the
    /// Subscription Identifier more than once." Section 3.3.2.3.8 says the
    /// opposite for PUBLISH, where several arrive when one publication
    /// matched several subscriptions, so the same bytes must be accepted
    /// there.
    #[test]
    fn rejects_a_second_subscription_identifier_on_a_subscribe_only() {
        // Two subscription identifiers, 1 and 2, in a five-byte block.
        let wire = [0x04u8, 0x0B, 0x01, 0x0B, 0x02];

        let mut reader = Reader::new(&wire);
        assert_eq!(
            Properties::decode_for(&mut reader, PropertyScope::Subscribe).unwrap_err(),
            CodecError::ProtocolError("subscription identifier appears more than once")
        );

        let mut reader = Reader::new(&wire);
        let decoded = Properties::decode_for(&mut reader, PropertyScope::Publish).expect("decodes");
        assert_eq!(decoded.subscription_identifiers, vec![1, 2]);

        // One is fine on a subscribe.
        let one = [0x02u8, 0x0B, 0x01];
        let mut reader = Reader::new(&one);
        let decoded =
            Properties::decode_for(&mut reader, PropertyScope::Subscribe).expect("decodes");
        assert_eq!(decoded.subscription_identifiers, vec![1]);
    }

    #[test]
    fn refuses_to_encode_a_second_subscription_identifier_on_a_subscribe() {
        let props = Properties {
            subscription_identifiers: vec![1, 2],
            ..Properties::default()
        };

        let mut out = Vec::new();
        assert_eq!(
            props
                .encode_for(PropertyScope::Subscribe, &mut out)
                .unwrap_err(),
            CodecError::ProtocolError("subscription identifier appears more than once")
        );

        // The same pair is legal on a publish and round trips.
        let mut out = Vec::new();
        props
            .encode_for(PropertyScope::Publish, &mut out)
            .expect("encodes");
        let mut reader = Reader::new(&out);
        assert_eq!(
            Properties::decode_for(&mut reader, PropertyScope::Publish).expect("decodes"),
            props
        );
    }
}
