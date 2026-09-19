//! PUBLISH and the acknowledgements that follow it.

use bytes::Bytes;

use crate::error::CodecError;
use crate::header::Limits;
use crate::properties::Properties;
use crate::qos::QoS;
use crate::reader::Reader;
use crate::reason::ReasonCode;
use crate::writer::{put_string, put_u16, put_u8};
use crate::Version;

/// An application message travelling in either direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    /// Whether this is a redelivery.
    pub dup: bool,
    /// The quality of service it was published at.
    pub qos: QoS,
    /// Whether the broker should retain it.
    pub retain: bool,
    /// Where it was published.
    pub topic: String,
    /// Present exactly when the quality of service is above zero.
    pub packet_id: Option<u16>,
    /// The application payload, which the broker never interprets.
    pub payload: Bytes,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl Publish {
    /// The low nibble of the fixed header for this packet.
    pub fn flags(&self) -> u8 {
        let mut flags = self.qos.as_u8() << 1;
        if self.dup {
            flags |= 0x08;
        }
        if self.retain {
            flags |= 0x01;
        }
        flags
    }

    /// Decode the body, given the flags taken from the fixed header.
    pub fn decode_body(
        reader: &mut Reader<'_>,
        flags: u8,
        version: Version,
        limits: &Limits,
    ) -> Result<Self, CodecError> {
        let dup = flags & 0x08 != 0;
        let qos = QoS::from_u8((flags & 0x06) >> 1)?;
        let retain = flags & 0x01 != 0;

        if dup && qos == QoS::AtMostOnce {
            return Err(CodecError::MalformedPacket(
                "duplicate flag set on a quality of service zero publish",
            ));
        }

        let topic = reader.string()?;
        crate::topic::validate_name(&topic, limits)?;

        let packet_id = if qos == QoS::AtMostOnce {
            None
        } else {
            let id = reader.u16()?;
            if id == 0 {
                return Err(CodecError::ProtocolError(
                    "packet identifier must not be zero",
                ));
            }
            Some(id)
        };

        let properties = if version == Version::V5 {
            Properties::decode(reader)?
        } else {
            Properties::default()
        };

        let payload = reader.take_rest();

        Ok(Publish {
            dup,
            qos,
            retain,
            topic,
            packet_id,
            payload,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_string(out, &self.topic)?;
        if self.qos != QoS::AtMostOnce {
            let id = self.packet_id.ok_or(CodecError::ProtocolError(
                "a publish above quality of service zero needs a packet identifier",
            ))?;
            put_u16(out, id);
        }
        if version == Version::V5 {
            self.properties.encode(out)?;
        }
        out.extend_from_slice(&self.payload);
        Ok(())
    }
}

/// The body shared by PUBACK, PUBREC, PUBREL and PUBCOMP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    /// Which publish this answers.
    pub packet_id: u16,
    /// The outcome.
    pub reason: ReasonCode,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl Ack {
    /// Decode the body that follows the fixed header.
    ///
    /// A two-byte body means success with no properties, which is the common
    /// case and the only legal shape in 3.1.1.
    pub fn decode_body(reader: &mut Reader<'_>, version: Version) -> Result<Self, CodecError> {
        let packet_id = reader.u16()?;
        if packet_id == 0 {
            return Err(CodecError::ProtocolError(
                "packet identifier must not be zero",
            ));
        }

        if version == Version::V311 || reader.is_empty() {
            return Ok(Ack {
                packet_id,
                reason: ReasonCode::Success,
                properties: Properties::default(),
            });
        }

        let code = reader.u8()?;
        let reason = ReasonCode::from_byte(code)
            .ok_or(CodecError::MalformedPacket("unknown reason code"))?;

        let properties = if reader.is_empty() {
            Properties::default()
        } else {
            Properties::decode(reader)?
        };

        Ok(Ack {
            packet_id,
            reason,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    ///
    /// A successful acknowledgement with no properties is written in its
    /// short form, which every version understands.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_u16(out, self.packet_id);
        if version == Version::V311 {
            return Ok(());
        }
        if self.reason == ReasonCode::Success && self.properties.is_empty() {
            return Ok(());
        }
        put_u8(out, self.reason as u8);
        self.properties.encode(out)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;
    use crate::header::Limits;
    use crate::qos::QoS;
    use crate::reader::Reader;
    use crate::reason::ReasonCode;
    use crate::Version;
    use bytes::Bytes;

    fn round_trip(packet: &Publish, version: Version) -> Publish {
        let mut out = Vec::new();
        packet.encode_body(version, &mut out).expect("encodes");
        let mut reader = Reader::new(&out);
        Publish::decode_body(&mut reader, packet.flags(), version, &Limits::default())
            .expect("decodes")
    }

    #[test]
    fn round_trips_a_qos_zero_publish() {
        let packet = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "sensors/1/temp".into(),
            packet_id: None,
            payload: Bytes::from_static(b"21.5"),
            properties: Default::default(),
        };
        assert_eq!(round_trip(&packet, Version::V311), packet);
    }

    #[test]
    fn round_trips_a_qos_two_publish_with_properties() {
        let packet = Publish {
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: true,
            topic: "sensors/1/temp".into(),
            packet_id: Some(42),
            payload: Bytes::from_static(b"21.5"),
            properties: crate::Properties {
                message_expiry_interval: Some(60),
                content_type: Some("text/plain".into()),
                ..Default::default()
            },
        };
        assert_eq!(round_trip(&packet, Version::V5), packet);
    }

    #[test]
    fn an_empty_payload_is_legal() {
        let packet = Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: "a".into(),
            packet_id: None,
            payload: Bytes::new(),
            properties: Default::default(),
        };
        assert_eq!(round_trip(&packet, Version::V311), packet);
    }

    #[test]
    fn rejects_a_wildcard_in_a_publish_topic() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "a/+").unwrap();
        let mut reader = Reader::new(&out);
        let err =
            Publish::decode_body(&mut reader, 0x00, Version::V311, &Limits::default()).unwrap_err();
        assert_eq!(err, CodecError::InvalidTopicName);
    }

    #[test]
    fn rejects_the_duplicate_flag_on_a_qos_zero_publish() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "a").unwrap();
        let mut reader = Reader::new(&out);
        // 0x08 is the duplicate flag with QoS left at zero.
        let err =
            Publish::decode_body(&mut reader, 0x08, Version::V311, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_quality_of_service_three() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "a").unwrap();
        let mut reader = Reader::new(&out);
        let err =
            Publish::decode_body(&mut reader, 0x06, Version::V311, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn a_v311_acknowledgement_is_just_a_packet_identifier() {
        let ack = Ack {
            packet_id: 7,
            reason: ReasonCode::Success,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        ack.encode_body(Version::V311, &mut out).unwrap();
        assert_eq!(out, vec![0x00, 0x07]);

        let mut reader = Reader::new(&out);
        assert_eq!(Ack::decode_body(&mut reader, Version::V311).unwrap(), ack);
    }

    #[test]
    fn a_v5_acknowledgement_omits_a_successful_reason_code() {
        let ack = Ack {
            packet_id: 7,
            reason: ReasonCode::Success,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        ack.encode_body(Version::V5, &mut out).unwrap();
        assert_eq!(out, vec![0x00, 0x07]);

        let mut reader = Reader::new(&out);
        assert_eq!(Ack::decode_body(&mut reader, Version::V5).unwrap(), ack);
    }

    #[test]
    fn a_v5_acknowledgement_carries_a_failure_reason_and_properties() {
        let ack = Ack {
            packet_id: 9,
            reason: ReasonCode::NoMatchingSubscribers,
            properties: crate::Properties {
                reason_string: Some("nobody listening".into()),
                ..Default::default()
            },
        };
        let mut out = Vec::new();
        ack.encode_body(Version::V5, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(Ack::decode_body(&mut reader, Version::V5).unwrap(), ack);
    }
}
