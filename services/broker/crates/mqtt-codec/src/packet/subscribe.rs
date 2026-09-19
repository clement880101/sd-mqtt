//! SUBSCRIBE, SUBACK, UNSUBSCRIBE and UNSUBACK.

use crate::error::CodecError;
use crate::header::Limits;
use crate::properties::{Properties, PropertyScope};
use crate::qos::QoS;
use crate::reader::Reader;
use crate::reason::ReasonCode;
use crate::writer::{put_string, put_u16, put_u8};
use crate::Version;

/// When the broker should send retained messages for a new subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RetainHandling {
    /// Always send them.
    #[default]
    SendAlways,
    /// Send them only if this subscription did not already exist.
    SendIfNew,
    /// Never send them.
    DoNotSend,
}

impl RetainHandling {
    fn from_u8(value: u8) -> Result<Self, CodecError> {
        match value {
            0 => Ok(RetainHandling::SendAlways),
            1 => Ok(RetainHandling::SendIfNew),
            2 => Ok(RetainHandling::DoNotSend),
            _ => Err(CodecError::MalformedPacket(
                "retain handling must be 0, 1 or 2",
            )),
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            RetainHandling::SendAlways => 0,
            RetainHandling::SendIfNew => 1,
            RetainHandling::DoNotSend => 2,
        }
    }
}

/// One filter and the options attached to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionOptions {
    /// The topic filter.
    pub filter: String,
    /// The maximum quality of service to deliver at.
    pub qos: QoS,
    /// Suppress delivery of the subscriber's own publications.
    pub no_local: bool,
    /// Forward the retain flag as published rather than clearing it.
    pub retain_as_published: bool,
    /// When to send retained messages.
    pub retain_handling: RetainHandling,
}

/// A request to subscribe to one or more filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    /// Identifies the request so SUBACK can answer it.
    pub packet_id: u16,
    /// At least one filter.
    pub filters: Vec<SubscriptionOptions>,
    /// MQTT 5 properties, including the subscription identifier.
    pub properties: Properties,
}

fn read_packet_id(reader: &mut Reader<'_>) -> Result<u16, CodecError> {
    let id = reader.u16()?;
    if id == 0 {
        return Err(CodecError::ProtocolError(
            "packet identifier must not be zero",
        ));
    }
    Ok(id)
}

impl Subscribe {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(
        reader: &mut Reader<'_>,
        version: Version,
        limits: &Limits,
    ) -> Result<Self, CodecError> {
        let packet_id = read_packet_id(reader)?;
        let properties = if version == Version::V5 {
            Properties::decode_for(reader, PropertyScope::Subscribe)?
        } else {
            Properties::default()
        };

        let mut filters = Vec::new();
        while !reader.is_empty() {
            let filter = reader.string()?;
            let options = reader.u8()?;

            let reserved_mask = match version {
                Version::V5 => 0xC0,
                Version::V311 => 0xFC,
            };
            if options & reserved_mask != 0 {
                return Err(CodecError::MalformedPacket(
                    "reserved bits in the subscription options are set",
                ));
            }

            let qos = QoS::from_u8(options & 0x03)?;
            let no_local = options & 0x04 != 0;
            let retain_as_published = options & 0x08 != 0;
            let retain_handling = RetainHandling::from_u8((options & 0x30) >> 4)?;

            let shared = crate::topic::validate_subscription(&filter, limits)?;
            if no_local && shared.is_some() {
                return Err(CodecError::ProtocolError(
                    "no local is not permitted on a shared subscription",
                ));
            }

            filters.push(SubscriptionOptions {
                filter,
                qos,
                no_local,
                retain_as_published,
                retain_handling,
            });
        }

        if filters.is_empty() {
            return Err(CodecError::ProtocolError(
                "subscribe must carry at least one filter",
            ));
        }

        Ok(Subscribe {
            packet_id,
            filters,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    ///
    /// Encoding as 3.1.1 silently drops the no-local, retain-as-published
    /// and retain-handling options, because that version has no way to
    /// express them. A caller bridging a v5 subscription down to 3.1.1 gets
    /// a plain subscription, which is the best that version allows.
    ///
    /// Filter validity is the caller's responsibility; use
    /// [`crate::topic::validate_subscription`]. This method has no access to
    /// [`Limits`](crate::header::Limits) and so cannot check topic depth.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        if self.packet_id == 0 {
            return Err(CodecError::ProtocolError(
                "packet identifier must not be zero",
            ));
        }
        if self.filters.is_empty() {
            return Err(CodecError::ProtocolError(
                "subscribe must carry at least one filter",
            ));
        }
        put_u16(out, self.packet_id);
        if version == Version::V5 {
            self.properties.encode_for(PropertyScope::Subscribe, out)?;
        }
        for entry in &self.filters {
            put_string(out, &entry.filter)?;
            let mut options = entry.qos.as_u8();
            if version == Version::V5 {
                if entry.no_local {
                    options |= 0x04;
                }
                if entry.retain_as_published {
                    options |= 0x08;
                }
                options |= entry.retain_handling.as_u8() << 4;
            }
            put_u8(out, options);
        }
        Ok(())
    }
}

/// The answer to SUBSCRIBE, one reason code per requested filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    /// Which request this answers.
    pub packet_id: u16,
    /// One outcome per filter, in the order requested.
    pub reasons: Vec<ReasonCode>,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl SubAck {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(reader: &mut Reader<'_>, version: Version) -> Result<Self, CodecError> {
        let packet_id = read_packet_id(reader)?;
        let properties = if version == Version::V5 {
            Properties::decode_for(reader, PropertyScope::SubAck)?
        } else {
            Properties::default()
        };

        let mut reasons = Vec::new();
        while !reader.is_empty() {
            let code = reader.u8()?;
            let reason = match version {
                Version::V5 => ReasonCode::from_byte(code)
                    .ok_or(CodecError::MalformedPacket("unknown reason code"))?,
                Version::V311 => match code {
                    0x00 => ReasonCode::Success,
                    0x01 => ReasonCode::GrantedQos1,
                    0x02 => ReasonCode::GrantedQos2,
                    0x80 => ReasonCode::UnspecifiedError,
                    _ => return Err(CodecError::MalformedPacket("unknown suback return code")),
                },
            };
            reasons.push(reason);
        }

        if reasons.is_empty() {
            return Err(CodecError::ProtocolError(
                "suback must carry at least one reason code",
            ));
        }

        Ok(SubAck {
            packet_id,
            reasons,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        if self.packet_id == 0 {
            return Err(CodecError::ProtocolError(
                "packet identifier must not be zero",
            ));
        }
        if self.reasons.is_empty() {
            return Err(CodecError::ProtocolError(
                "suback must carry at least one reason code",
            ));
        }
        put_u16(out, self.packet_id);
        if version == Version::V5 {
            self.properties.encode_for(PropertyScope::SubAck, out)?;
        }
        for reason in &self.reasons {
            let byte = match version {
                Version::V5 => *reason as u8,
                Version::V311 => reason.to_v311_suback(),
            };
            put_u8(out, byte);
        }
        Ok(())
    }
}

/// A request to remove one or more subscriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    /// Identifies the request.
    pub packet_id: u16,
    /// At least one filter.
    pub filters: Vec<String>,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl Unsubscribe {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(
        reader: &mut Reader<'_>,
        version: Version,
        limits: &Limits,
    ) -> Result<Self, CodecError> {
        let packet_id = read_packet_id(reader)?;
        let properties = if version == Version::V5 {
            Properties::decode_for(reader, PropertyScope::Unsubscribe)?
        } else {
            Properties::default()
        };

        let mut filters = Vec::new();
        while !reader.is_empty() {
            let filter = reader.string()?;
            crate::topic::validate_subscription(&filter, limits)?;
            filters.push(filter);
        }

        if filters.is_empty() {
            return Err(CodecError::ProtocolError(
                "unsubscribe must carry at least one filter",
            ));
        }

        Ok(Unsubscribe {
            packet_id,
            filters,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    ///
    /// Filter validity is the caller's responsibility; use
    /// [`crate::topic::validate_subscription`].
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        if self.packet_id == 0 {
            return Err(CodecError::ProtocolError(
                "packet identifier must not be zero",
            ));
        }
        if self.filters.is_empty() {
            return Err(CodecError::ProtocolError(
                "unsubscribe must carry at least one filter",
            ));
        }
        put_u16(out, self.packet_id);
        if version == Version::V5 {
            self.properties
                .encode_for(PropertyScope::Unsubscribe, out)?;
        }
        for filter in &self.filters {
            put_string(out, filter)?;
        }
        Ok(())
    }
}

/// The answer to UNSUBSCRIBE.
///
/// MQTT 3.1.1 carries no reason codes, so `reasons` is empty when decoding a
/// 3.1.1 packet and is ignored when encoding one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAck {
    /// Which request this answers.
    pub packet_id: u16,
    /// One outcome per filter, in the order requested. MQTT 5 only.
    pub reasons: Vec<ReasonCode>,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl UnsubAck {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(reader: &mut Reader<'_>, version: Version) -> Result<Self, CodecError> {
        let packet_id = read_packet_id(reader)?;
        if version == Version::V311 {
            return Ok(UnsubAck {
                packet_id,
                reasons: Vec::new(),
                properties: Properties::default(),
            });
        }

        let properties = Properties::decode_for(reader, PropertyScope::UnsubAck)?;
        let mut reasons = Vec::new();
        while !reader.is_empty() {
            let code = reader.u8()?;
            reasons.push(
                ReasonCode::from_byte(code)
                    .ok_or(CodecError::MalformedPacket("unknown reason code"))?,
            );
        }

        Ok(UnsubAck {
            packet_id,
            reasons,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    ///
    /// Encoding as 3.1.1 drops every reason code, because that version's
    /// UNSUBACK carries none. The drop is silent and correct.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        if self.packet_id == 0 {
            return Err(CodecError::ProtocolError(
                "packet identifier must not be zero",
            ));
        }
        put_u16(out, self.packet_id);
        if version == Version::V311 {
            return Ok(());
        }
        self.properties.encode_for(PropertyScope::UnsubAck, out)?;
        for reason in &self.reasons {
            put_u8(out, *reason as u8);
        }
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

    fn option(filter: &str, qos: QoS) -> SubscriptionOptions {
        SubscriptionOptions {
            filter: filter.into(),
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendAlways,
        }
    }

    #[test]
    fn round_trips_a_v311_subscribe() {
        let packet = Subscribe {
            packet_id: 1,
            filters: vec![
                option("a/b", QoS::AtLeastOnce),
                option("c/#", QoS::AtMostOnce),
            ],
            properties: Default::default(),
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        let decoded =
            Subscribe::decode_body(&mut reader, Version::V311, &Limits::default()).unwrap();
        assert_eq!(decoded, packet);
    }

    #[test]
    fn round_trips_a_v5_subscribe_with_every_option_set() {
        let packet = Subscribe {
            packet_id: 2,
            filters: vec![SubscriptionOptions {
                filter: "a/+".into(),
                qos: QoS::ExactlyOnce,
                no_local: true,
                retain_as_published: true,
                retain_handling: RetainHandling::DoNotSend,
            }],
            properties: crate::Properties {
                subscription_identifiers: vec![7],
                ..Default::default()
            },
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        let decoded = Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).unwrap();
        assert_eq!(decoded, packet);
    }

    #[test]
    fn rejects_a_subscribe_with_no_filters() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        let mut reader = Reader::new(&out);
        assert_eq!(
            Subscribe::decode_body(&mut reader, Version::V311, &Limits::default()).unwrap_err(),
            CodecError::ProtocolError("subscribe must carry at least one filter")
        );
    }

    #[test]
    fn rejects_reserved_bits_in_the_options_byte() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        out.push(0x00); // empty property block
        crate::writer::put_string(&mut out, "a").unwrap();
        out.push(0xC0); // both reserved high bits set
        let mut reader = Reader::new(&out);
        assert_eq!(
            Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).unwrap_err(),
            CodecError::MalformedPacket("reserved bits in the subscription options are set")
        );
    }

    #[test]
    fn rejects_retain_handling_three() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        out.push(0x00); // empty property block
        crate::writer::put_string(&mut out, "a").unwrap();
        out.push(0x30); // retain handling bits both set
        let mut reader = Reader::new(&out);
        assert_eq!(
            Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).unwrap_err(),
            CodecError::MalformedPacket("retain handling must be 0, 1 or 2")
        );
    }

    #[test]
    fn rejects_no_local_on_a_shared_subscription() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        out.push(0x00); // empty property block
        crate::writer::put_string(&mut out, "$share/g/a").unwrap();
        out.push(0x04); // no local
        let mut reader = Reader::new(&out);
        assert_eq!(
            Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).unwrap_err(),
            CodecError::ProtocolError("no local is not permitted on a shared subscription")
        );
    }

    #[test]
    fn rejects_an_invalid_filter() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        out.push(0x00); // empty property block
        crate::writer::put_string(&mut out, "a/#/b").unwrap();
        out.push(0x00);
        let mut reader = Reader::new(&out);
        assert_eq!(
            Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).unwrap_err(),
            CodecError::InvalidTopicFilter
        );
    }

    #[test]
    fn round_trips_suback_in_both_versions() {
        let packet = SubAck {
            packet_id: 3,
            reasons: vec![ReasonCode::GrantedQos1, ReasonCode::NotAuthorized],
            properties: Default::default(),
        };
        for version in [Version::V311, Version::V5] {
            let mut out = Vec::new();
            packet.encode_body(version, &mut out).unwrap();
            let mut reader = Reader::new(&out);
            let decoded = SubAck::decode_body(&mut reader, version).unwrap();
            assert_eq!(decoded.packet_id, 3);
            assert_eq!(decoded.reasons.len(), 2);
            assert_eq!(decoded.reasons[0], ReasonCode::GrantedQos1);
            assert!(decoded.reasons[1].is_failure());
        }
    }

    #[test]
    fn round_trips_unsubscribe() {
        let packet = Unsubscribe {
            packet_id: 4,
            filters: vec!["a/b".into(), "c/#".into()],
            properties: Default::default(),
        };
        for version in [Version::V311, Version::V5] {
            let mut out = Vec::new();
            packet.encode_body(version, &mut out).unwrap();
            let mut reader = Reader::new(&out);
            let decoded =
                Unsubscribe::decode_body(&mut reader, version, &Limits::default()).unwrap();
            assert_eq!(decoded, packet);
        }
    }

    #[test]
    fn a_v311_unsuback_carries_no_reason_codes() {
        let packet = UnsubAck {
            packet_id: 5,
            reasons: vec![ReasonCode::Success],
            properties: Default::default(),
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();
        assert_eq!(out, vec![0x00, 0x05]);

        let mut reader = Reader::new(&out);
        let decoded = UnsubAck::decode_body(&mut reader, Version::V311).unwrap();
        assert_eq!(decoded.packet_id, 5);
        assert!(decoded.reasons.is_empty());
    }

    #[test]
    fn a_v5_unsuback_carries_one_reason_per_filter() {
        let packet = UnsubAck {
            packet_id: 6,
            reasons: vec![ReasonCode::Success, ReasonCode::NoSubscriptionExisted],
            properties: Default::default(),
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(
            UnsubAck::decode_body(&mut reader, Version::V5).unwrap(),
            packet
        );
    }
}
