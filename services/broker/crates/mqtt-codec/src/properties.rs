//! MQTT 5 properties.

use bytes::Bytes;

use crate::error::CodecError;
use crate::reader::Reader;
use crate::varint;
use crate::writer::{put_binary, put_string, put_u16, put_u32, put_u8};

/// Every MQTT 5 property, gathered into one structure.
///
/// A given packet type permits only a subset. Enforcing which subset belongs
/// to which packet is the job of the packet decoders, not of this type.
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

    /// Decode a property block from the current position.
    ///
    /// Reads the block length, then exactly that many bytes, leaving the
    /// reader positioned at the first byte after the block.
    pub fn decode(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        let len = reader.varint()? as usize;
        let body = reader.slice(len)?;
        let mut inner = Reader::new(body);
        let mut props = Properties::default();

        while !inner.is_empty() {
            let id = inner.varint()?;
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
    pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), CodecError> {
        let mut body = Vec::new();

        if let Some(v) = self.payload_format_indicator {
            body.push(0x01);
            put_u8(&mut body, v);
        }
        if let Some(v) = self.message_expiry_interval {
            body.push(0x02);
            put_u32(&mut body, v);
        }
        if let Some(v) = &self.content_type {
            body.push(0x03);
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.response_topic {
            body.push(0x08);
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.correlation_data {
            body.push(0x09);
            put_binary(&mut body, v)?;
        }
        for v in &self.subscription_identifiers {
            body.push(0x0B);
            varint::encode(*v, &mut body)?;
        }
        if let Some(v) = self.session_expiry_interval {
            body.push(0x11);
            put_u32(&mut body, v);
        }
        if let Some(v) = &self.assigned_client_identifier {
            body.push(0x12);
            put_string(&mut body, v)?;
        }
        if let Some(v) = self.server_keep_alive {
            body.push(0x13);
            put_u16(&mut body, v);
        }
        if let Some(v) = &self.authentication_method {
            body.push(0x15);
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.authentication_data {
            body.push(0x16);
            put_binary(&mut body, v)?;
        }
        if let Some(v) = self.request_problem_information {
            body.push(0x17);
            put_u8(&mut body, v);
        }
        if let Some(v) = self.will_delay_interval {
            body.push(0x18);
            put_u32(&mut body, v);
        }
        if let Some(v) = self.request_response_information {
            body.push(0x19);
            put_u8(&mut body, v);
        }
        if let Some(v) = &self.response_information {
            body.push(0x1A);
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.server_reference {
            body.push(0x1C);
            put_string(&mut body, v)?;
        }
        if let Some(v) = &self.reason_string {
            body.push(0x1F);
            put_string(&mut body, v)?;
        }
        if let Some(v) = self.receive_maximum {
            body.push(0x21);
            put_u16(&mut body, v);
        }
        if let Some(v) = self.topic_alias_maximum {
            body.push(0x22);
            put_u16(&mut body, v);
        }
        if let Some(v) = self.topic_alias {
            body.push(0x23);
            put_u16(&mut body, v);
        }
        if let Some(v) = self.maximum_qos {
            body.push(0x24);
            put_u8(&mut body, v);
        }
        if let Some(v) = self.retain_available {
            body.push(0x25);
            put_u8(&mut body, v);
        }
        for (key, value) in &self.user_properties {
            body.push(0x26);
            put_string(&mut body, key)?;
            put_string(&mut body, value)?;
        }
        if let Some(v) = self.maximum_packet_size {
            body.push(0x27);
            put_u32(&mut body, v);
        }
        if let Some(v) = self.wildcard_subscription_available {
            body.push(0x28);
            put_u8(&mut body, v);
        }
        if let Some(v) = self.subscription_identifier_available {
            body.push(0x29);
            put_u8(&mut body, v);
        }
        if let Some(v) = self.shared_subscription_available {
            body.push(0x2A);
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
        // Length 4, then session expiry (0x11) twice with truncated values.
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
    fn stops_at_the_end_of_the_property_block() {
        // One property, then a trailing byte belonging to the packet body.
        let bytes = [0x02, 0x01, 0x01, 0xAA];
        let mut reader = Reader::new(&bytes);
        let props = Properties::decode(&mut reader).unwrap();
        assert_eq!(props.payload_format_indicator, Some(1));
        assert_eq!(reader.u8().unwrap(), 0xAA);
    }
}
