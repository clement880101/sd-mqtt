//! PUBLISH and the acknowledgements that follow it.

use bytes::Bytes;

use crate::error::CodecError;
use crate::header::{Limits, PacketKind};
use crate::properties::{Properties, PropertyScope};
use crate::qos::QoS;
use crate::reader::Reader;
use crate::reason::ReasonCode;
use crate::writer::{put_string, put_u16, put_u8};
use crate::Version;

/// The MQTT 5 rules tying an empty topic name to a topic alias.
///
/// MQTT-v5.0 section 3.3.2.3.4: a topic alias of zero is never permitted,
/// and a zero-length topic name is a protocol error unless an alias stands
/// in for it. Whether the alias has actually been mapped on this connection
/// is session state, so a later layer decides that; the codec only checks
/// that the shape is one the specification allows on the wire.
fn check_topic_alias(topic: &str, properties: &Properties) -> Result<(), CodecError> {
    if properties.topic_alias == Some(0) {
        return Err(CodecError::ProtocolError("topic alias must not be zero"));
    }
    if topic.is_empty() && properties.topic_alias.is_none() {
        return Err(CodecError::InvalidTopicName);
    }
    Ok(())
}

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
    ///
    /// In MQTT 5 an empty topic name is legal when a topic alias stands in
    /// for it, so that decision waits until the property block has been
    /// read; MQTT-v5.0 section 3.3.2.3.4. MQTT 3.1.1 has no aliases and
    /// rejects an empty topic outright.
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
        if version == Version::V311 || !topic.is_empty() {
            crate::topic::validate_name(&topic, limits)?;
        }

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
            Properties::decode_for(reader, PropertyScope::Publish)?
        } else {
            Properties::default()
        };

        if version == Version::V5 {
            check_topic_alias(&topic, &properties)?;
        }

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
    ///
    /// Refuses to write anything [`Publish::decode_body`] would refuse to
    /// read back, so an incoherent packet is an error rather than silent
    /// data loss. The depth limit is not checked here, because encoding
    /// takes no [`Limits`].
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        // [`Publish::flags`] would happily set the bit, and
        // [`Publish::decode_body`] rejects it; MQTT-v5.0 section 3.3.1.1.
        if self.dup && self.qos == QoS::AtMostOnce {
            return Err(CodecError::MalformedPacket(
                "duplicate flag set on a quality of service zero publish",
            ));
        }
        if self.qos == QoS::AtMostOnce && self.packet_id.is_some() {
            return Err(CodecError::ProtocolError(
                "a quality of service zero publish must not carry a packet identifier",
            ));
        }
        if self.topic.contains('+') || self.topic.contains('#') || self.topic.contains('\0') {
            return Err(CodecError::InvalidTopicName);
        }
        if version == Version::V5 {
            check_topic_alias(&self.topic, &self.properties)?;
        } else if self.topic.is_empty() {
            return Err(CodecError::InvalidTopicName);
        }

        let packet_id = if self.qos == QoS::AtMostOnce {
            None
        } else {
            let id = self.packet_id.ok_or(CodecError::ProtocolError(
                "a publish above quality of service zero needs a packet identifier",
            ))?;
            if id == 0 {
                return Err(CodecError::ProtocolError(
                    "packet identifier must not be zero",
                ));
            }
            Some(id)
        };

        put_string(out, &self.topic)?;
        if let Some(id) = packet_id {
            put_u16(out, id);
        }
        if version == Version::V5 {
            self.properties.encode_for(PropertyScope::Publish, out)?;
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
    ///
    /// `kind` says which of the four acknowledgements this is, because the
    /// body alone does not: the legal reason codes differ between them. It
    /// is passed in rather than stored, so it cannot disagree with the
    /// enum variant that holds the result.
    pub fn decode_body(
        reader: &mut Reader<'_>,
        version: Version,
        kind: PacketKind,
    ) -> Result<Self, CodecError> {
        let packet_id = reader.u16()?;
        if packet_id == 0 {
            return Err(CodecError::ProtocolError(
                "packet identifier must not be zero",
            ));
        }

        let (reason, properties) = if version == Version::V311 || reader.is_empty() {
            (ReasonCode::Success, Properties::default())
        } else {
            let code = reader.u8()?;
            let reason = ReasonCode::from_byte(code)
                .ok_or(CodecError::MalformedPacket("unknown reason code"))?;

            let properties = if reader.is_empty() {
                Properties::default()
            } else {
                Properties::decode_for(reader, PropertyScope::Ack)?
            };

            (reason, properties)
        };

        crate::reason::check_reason(kind, reason)?;

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
    ///
    /// `kind` says which of the four acknowledgements this is, so that the
    /// reason code can be checked against the set that packet permits.
    ///
    /// Encoding as 3.1.1 is lossy and silently so: that version has no
    /// reason code and no properties, so a failure reason and any
    /// properties are dropped and the peer sees a plain acknowledgement.
    /// The degradation is what the protocol requires, but a caller that
    /// needs to know a refusal was conveyed must check the version itself.
    pub fn encode_body(
        &self,
        version: Version,
        kind: PacketKind,
        out: &mut Vec<u8>,
    ) -> Result<(), CodecError> {
        if self.packet_id == 0 {
            return Err(CodecError::ProtocolError(
                "packet identifier must not be zero",
            ));
        }
        crate::reason::check_reason(kind, self.reason)?;

        put_u16(out, self.packet_id);
        if version == Version::V311 {
            return Ok(());
        }
        if self.reason == ReasonCode::Success && self.properties.is_empty() {
            return Ok(());
        }
        put_u8(out, self.reason as u8);
        self.properties.encode_for(PropertyScope::Ack, out)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;
    use crate::header::{Limits, PacketKind};
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
        ack.encode_body(Version::V311, PacketKind::PubAck, &mut out)
            .unwrap();
        assert_eq!(out, vec![0x00, 0x07]);

        let mut reader = Reader::new(&out);
        assert_eq!(
            Ack::decode_body(&mut reader, Version::V311, PacketKind::PubAck).unwrap(),
            ack
        );
    }

    #[test]
    fn a_v5_acknowledgement_omits_a_successful_reason_code() {
        let ack = Ack {
            packet_id: 7,
            reason: ReasonCode::Success,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        ack.encode_body(Version::V5, PacketKind::PubAck, &mut out)
            .unwrap();
        assert_eq!(out, vec![0x00, 0x07]);

        let mut reader = Reader::new(&out);
        assert_eq!(
            Ack::decode_body(&mut reader, Version::V5, PacketKind::PubAck).unwrap(),
            ack
        );
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
        ack.encode_body(Version::V5, PacketKind::PubAck, &mut out)
            .unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(
            Ack::decode_body(&mut reader, Version::V5, PacketKind::PubAck).unwrap(),
            ack
        );
    }

    /// A helper for the encode-side rejections below.
    fn encode_err(packet: &Publish, version: Version) -> CodecError {
        let mut out = Vec::new();
        packet.encode_body(version, &mut out).unwrap_err()
    }

    fn qos_zero_publish(topic: &str) -> Publish {
        Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.into(),
            packet_id: None,
            payload: Bytes::new(),
            properties: Default::default(),
        }
    }

    #[test]
    fn rejects_a_packet_identifier_on_a_qos_zero_publish_when_encoding() {
        let packet = Publish {
            packet_id: Some(1),
            ..qos_zero_publish("a")
        };
        assert_eq!(
            encode_err(&packet, Version::V311),
            CodecError::ProtocolError(
                "a quality of service zero publish must not carry a packet identifier",
            )
        );
    }

    #[test]
    fn rejects_a_zero_packet_identifier_when_encoding_a_publish() {
        let packet = Publish {
            qos: QoS::AtLeastOnce,
            packet_id: Some(0),
            ..qos_zero_publish("a")
        };
        assert_eq!(
            encode_err(&packet, Version::V311),
            CodecError::ProtocolError("packet identifier must not be zero")
        );
    }

    #[test]
    fn rejects_a_zero_packet_identifier_when_encoding_an_acknowledgement() {
        let ack = Ack {
            packet_id: 0,
            reason: ReasonCode::Success,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        assert_eq!(
            ack.encode_body(Version::V311, PacketKind::PubAck, &mut out)
                .unwrap_err(),
            CodecError::ProtocolError("packet identifier must not be zero")
        );
    }

    /// The mirror of `rejects_the_duplicate_flag_on_a_qos_zero_publish`.
    /// `flags()` sets the bit from `dup` without consulting the QoS, so
    /// without this check the encoder writes a frame its own decoder
    /// refuses.
    #[test]
    fn rejects_the_duplicate_flag_at_qos_zero_when_encoding() {
        let packet = Publish {
            dup: true,
            ..qos_zero_publish("a")
        };
        assert_eq!(
            encode_err(&packet, Version::V311),
            CodecError::MalformedPacket("duplicate flag set on a quality of service zero publish")
        );
        assert_eq!(
            encode_err(&packet, Version::V5),
            CodecError::MalformedPacket("duplicate flag set on a quality of service zero publish")
        );
    }

    #[test]
    fn rejects_a_wildcard_topic_when_encoding() {
        assert_eq!(
            encode_err(&qos_zero_publish("a/+"), Version::V311),
            CodecError::InvalidTopicName
        );
        assert_eq!(
            encode_err(&qos_zero_publish("a/#"), Version::V311),
            CodecError::InvalidTopicName
        );
    }

    #[test]
    fn rejects_a_null_character_in_a_topic_when_encoding() {
        assert_eq!(
            encode_err(&qos_zero_publish("a\0b"), Version::V311),
            CodecError::InvalidTopicName
        );
    }

    /// An MQTT 5 publish may leave the topic empty when an alias stands in
    /// for it; MQTT-v5.0 section 3.3.2.3.4.
    #[test]
    fn accepts_an_empty_topic_with_a_topic_alias() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "").unwrap();
        crate::writer::put_u8(&mut out, 3); // property block length
        crate::writer::put_u8(&mut out, 0x23); // topic alias
        crate::writer::put_u16(&mut out, 7);
        let mut reader = Reader::new(&out);

        let decoded =
            Publish::decode_body(&mut reader, 0x00, Version::V5, &Limits::default()).unwrap();

        assert_eq!(decoded.topic, "");
        assert_eq!(decoded.properties.topic_alias, Some(7));
    }

    #[test]
    fn rejects_an_empty_topic_without_a_topic_alias() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "").unwrap();
        crate::writer::put_u8(&mut out, 0); // empty property block
        let mut reader = Reader::new(&out);

        let err =
            Publish::decode_body(&mut reader, 0x00, Version::V5, &Limits::default()).unwrap_err();

        assert_eq!(err, CodecError::InvalidTopicName);
    }

    #[test]
    fn rejects_a_zero_topic_alias() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "a").unwrap();
        crate::writer::put_u8(&mut out, 3); // property block length
        crate::writer::put_u8(&mut out, 0x23); // topic alias
        crate::writer::put_u16(&mut out, 0);
        let mut reader = Reader::new(&out);

        let err =
            Publish::decode_body(&mut reader, 0x00, Version::V5, &Limits::default()).unwrap_err();

        assert_eq!(
            err,
            CodecError::ProtocolError("topic alias must not be zero")
        );
    }

    #[test]
    fn rejects_an_empty_topic_in_v311_which_has_no_aliases() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "").unwrap();
        let mut reader = Reader::new(&out);

        let err =
            Publish::decode_body(&mut reader, 0x00, Version::V311, &Limits::default()).unwrap_err();

        assert_eq!(err, CodecError::InvalidTopicName);
    }

    #[test]
    fn round_trips_an_empty_topic_with_a_topic_alias() {
        let packet = Publish {
            properties: crate::Properties {
                topic_alias: Some(7),
                ..Default::default()
            },
            ..qos_zero_publish("")
        };
        assert_eq!(round_trip(&packet, Version::V5), packet);
    }

    #[test]
    fn rejects_encoding_an_empty_topic_without_a_topic_alias() {
        assert_eq!(
            encode_err(&qos_zero_publish(""), Version::V5),
            CodecError::InvalidTopicName
        );
        assert_eq!(
            encode_err(&qos_zero_publish(""), Version::V311),
            CodecError::InvalidTopicName
        );
    }

    #[test]
    fn rejects_encoding_a_zero_topic_alias() {
        let packet = Publish {
            properties: crate::Properties {
                topic_alias: Some(0),
                ..Default::default()
            },
            ..qos_zero_publish("a")
        };
        assert_eq!(
            encode_err(&packet, Version::V5),
            CodecError::ProtocolError("topic alias must not be zero")
        );
    }

    /// The nine codes MQTT-v5.0 Table 3-4 and Table 3-5 permit on PUBACK
    /// and PUBREC, which share one set.
    const PUBACK_CODES: [ReasonCode; 9] = [
        ReasonCode::Success,
        ReasonCode::NoMatchingSubscribers,
        ReasonCode::UnspecifiedError,
        ReasonCode::ImplementationSpecificError,
        ReasonCode::NotAuthorized,
        ReasonCode::TopicNameInvalid,
        ReasonCode::PacketIdentifierInUse,
        ReasonCode::QuotaExceeded,
        ReasonCode::PayloadFormatInvalid,
    ];

    /// The two codes MQTT-v5.0 Table 3-6 and Table 3-7 permit on PUBREL
    /// and PUBCOMP.
    const PUBREL_CODES: [ReasonCode; 2] =
        [ReasonCode::Success, ReasonCode::PacketIdentifierNotFound];

    #[test]
    fn accepts_the_reason_codes_legal_for_each_acknowledgement() {
        let sets = [
            (PacketKind::PubAck, &PUBACK_CODES[..]),
            (PacketKind::PubRec, &PUBACK_CODES[..]),
            (PacketKind::PubRel, &PUBREL_CODES[..]),
            (PacketKind::PubComp, &PUBREL_CODES[..]),
        ];

        for (kind, codes) in sets {
            for &reason in codes {
                let ack = Ack {
                    packet_id: 4,
                    reason,
                    properties: Default::default(),
                };
                let mut out = Vec::new();
                ack.encode_body(Version::V5, kind, &mut out)
                    .expect("encodes");
                let mut reader = Reader::new(&out);
                assert_eq!(
                    Ack::decode_body(&mut reader, Version::V5, kind).expect("decodes"),
                    ack,
                    "{kind:?} should accept {reason:?}"
                );
            }
        }
    }

    #[test]
    fn rejects_a_pubrel_carrying_a_reason_only_legal_on_a_puback() {
        let ack = Ack {
            packet_id: 3,
            reason: ReasonCode::NotAuthorized,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        assert_eq!(
            ack.encode_body(Version::V5, PacketKind::PubRel, &mut out)
                .unwrap_err(),
            CodecError::ProtocolError("reason code is not permitted on this packet type")
        );

        // Hand-built: identifier 3, reason 0x87 (not authorized), no
        // properties. Legal on a PUBACK, not on a PUBREL.
        let wire = [0x00, 0x03, 0x87, 0x00];
        let mut reader = Reader::new(&wire);
        assert_eq!(
            Ack::decode_body(&mut reader, Version::V5, PacketKind::PubRel).unwrap_err(),
            CodecError::ProtocolError("reason code is not permitted on this packet type")
        );

        // The same bytes are fine as a PUBACK.
        let mut reader = Reader::new(&wire);
        assert_eq!(
            Ack::decode_body(&mut reader, Version::V5, PacketKind::PubAck)
                .unwrap()
                .reason,
            ReasonCode::NotAuthorized
        );
    }

    #[test]
    fn rejects_a_pubcomp_carrying_no_matching_subscribers() {
        let ack = Ack {
            packet_id: 3,
            reason: ReasonCode::NoMatchingSubscribers,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        assert_eq!(
            ack.encode_body(Version::V5, PacketKind::PubComp, &mut out)
                .unwrap_err(),
            CodecError::ProtocolError("reason code is not permitted on this packet type")
        );
    }

    #[test]
    fn rejects_a_kind_that_is_not_an_acknowledgement() {
        let ack = Ack {
            packet_id: 3,
            reason: ReasonCode::Success,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        assert_eq!(
            ack.encode_body(Version::V5, PacketKind::Connect, &mut out)
                .unwrap_err(),
            CodecError::ProtocolError("reason code is not permitted on this packet type")
        );

        let wire = [0x00, 0x03];
        let mut reader = Reader::new(&wire);
        assert_eq!(
            Ack::decode_body(&mut reader, Version::V311, PacketKind::Connect).unwrap_err(),
            CodecError::ProtocolError("reason code is not permitted on this packet type")
        );
    }
}
