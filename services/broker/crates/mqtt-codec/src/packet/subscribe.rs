//! SUBSCRIBE, SUBACK, UNSUBSCRIBE and UNSUBACK.

use crate::error::CodecError;
use crate::header::{Limits, PacketKind};
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
    /// Interpret bits 4 and 5 of a subscription options byte.
    ///
    /// MQTT-v5.0 section 3.8.3.1: "It is a Protocol Error to send a Retain
    /// Handling value of 3." A protocol error rather than a malformed
    /// packet, the same classification the same clause gives a maximum
    /// quality of service of 3, and unlike the reserved bits 6 and 7 which
    /// that clause does call malformed, [MQTT-3.8.3-5].
    fn from_u8(value: u8) -> Result<Self, CodecError> {
        match value {
            0 => Ok(RetainHandling::SendAlways),
            1 => Ok(RetainHandling::SendIfNew),
            2 => Ok(RetainHandling::DoNotSend),
            _ => Err(CodecError::ProtocolError("retain handling must not be 3")),
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

/// Reject the no-local option on a shared subscription.
///
/// MQTT-v5.0 section 3.8.3.1, [MQTT-3.8.3-4]: "It is a Protocol Error to set
/// the No Local bit to 1 on a Shared Subscription." This is a rule tying two
/// fields together rather than a statement about filter validity, so it is
/// not covered by the note on [`Subscribe::encode_body`] that filter validity
/// belongs to the caller. Both sides call this so they cannot drift.
///
/// Testing the prefix alone needs no [`Limits`] and agrees with
/// [`crate::topic::validate_subscription`], which the broker asks of each
/// filter: a filter carrying the prefix but no well-formed group is
/// refused there with 0x8F, and here only when it also sets no-local.
fn check_no_local(filter: &str, no_local: bool) -> Result<(), CodecError> {
    if no_local && filter.starts_with(crate::topic::SHARE_PREFIX) {
        return Err(CodecError::ProtocolError(
            "no local is not permitted on a shared subscription",
        ));
    }
    Ok(())
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
    ///
    /// Filters are carried as written, not judged. [MQTT-3.8.4-5] has a
    /// subscribe of several filters handled as a sequence of
    /// single-filter subscribes with their answers combined, so an
    /// invalid filter earns 0x8F in its slot of the SUBACK and the
    /// filters beside it are granted; the broker asks
    /// [`crate::topic::validate_subscription`] of each. Refusing the
    /// packet here would close the connection over one filter, which
    /// 4.13 permits but which punishes the ones that were fine. The
    /// no-local rule stays, because it ties two fields together rather
    /// than judging a filter and has no per-filter answer. `Limits`
    /// is taken so the signature matches the other bodies the frame
    /// decoder dispatches to; the depth limit is the broker's to apply
    /// with the same value.
    pub fn decode_body(
        reader: &mut Reader<'_>,
        version: Version,
        _limits: &Limits,
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

            let qos = QoS::from_subscription_options(options & 0x03)?;
            let no_local = options & 0x04 != 0;
            let retain_as_published = options & 0x08 != 0;
            let retain_handling = RetainHandling::from_u8((options & 0x30) >> 4)?;

            check_no_local(&filter, no_local)?;

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
    /// [`Limits`] and so cannot check topic depth. The no-local rule for
    /// shared subscriptions is not filter validity but a relation between
    /// two fields, so it is checked here.
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
                check_no_local(&entry.filter, entry.no_local)?;
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
            crate::reason::check_reason(PacketKind::SubAck, reason)?;
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
        for reason in &self.reasons {
            crate::reason::check_reason(PacketKind::SubAck, *reason)?;
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
            let reason = ReasonCode::from_byte(code)
                .ok_or(CodecError::MalformedPacket("unknown reason code"))?;
            crate::reason::check_reason(PacketKind::UnsubAck, reason)?;
            reasons.push(reason);
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
        for reason in &self.reasons {
            crate::reason::check_reason(PacketKind::UnsubAck, *reason)?;
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

    /// MQTT-v5.0 section 3.8.3.1 calls a maximum quality of service of 3 a
    /// protocol error, unlike the same value in a PUBLISH fixed header,
    /// which section 3.3.1.2 calls malformed.
    #[test]
    fn rejects_a_maximum_quality_of_service_of_three() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        out.push(0x00); // empty property block
        crate::writer::put_string(&mut out, "a").unwrap();
        out.push(0x03); // both quality of service bits set
        let mut reader = Reader::new(&out);
        assert_eq!(
            Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).unwrap_err(),
            CodecError::ProtocolError(
                "maximum quality of service in the subscription options must not be 3"
            )
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
            CodecError::ProtocolError("retain handling must not be 3")
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

    /// The mirror of the decode-side check above. Without it the encoder
    /// writes a SUBSCRIBE its own decoder refuses to read back.
    #[test]
    fn rejects_no_local_on_a_shared_subscription_when_encoding() {
        let packet = Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: "$share/g/a".into(),
                qos: QoS::AtMostOnce,
                no_local: true,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAlways,
            }],
            properties: Default::default(),
        };
        let mut out = Vec::new();
        assert_eq!(
            packet.encode_body(Version::V5, &mut out).unwrap_err(),
            CodecError::ProtocolError("no local is not permitted on a shared subscription")
        );

        // The same subscription without no-local is fine.
        let allowed = Subscribe {
            filters: vec![SubscriptionOptions {
                no_local: false,
                ..packet.filters[0].clone()
            }],
            ..packet
        };
        let mut out = Vec::new();
        allowed.encode_body(Version::V5, &mut out).expect("encodes");
    }

    /// An invalid filter is carried to the broker as written, because
    /// [MQTT-3.8.4-5] answers a subscribe per filter and the broker
    /// refuses the bad one with 0x8F in its SUBACK. Refusing the packet
    /// here would close the connection over one filter and punish the
    /// ones beside it that were fine.
    #[test]
    fn decodes_an_invalid_filter_for_the_broker_to_refuse() {
        let mut out = Vec::new();
        crate::writer::put_u16(&mut out, 1);
        out.push(0x00); // empty property block
        crate::writer::put_string(&mut out, "a/#/b").unwrap();
        out.push(0x00);
        crate::writer::put_string(&mut out, "ok/+").unwrap();
        out.push(0x00);
        let mut reader = Reader::new(&out);
        let decoded =
            Subscribe::decode_body(&mut reader, Version::V5, &Limits::default()).expect("decodes");
        assert_eq!(
            decoded
                .filters
                .iter()
                .map(|options| options.filter.as_str())
                .collect::<Vec<_>>(),
            vec!["a/#/b", "ok/+"]
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

    /// MQTT-v5.0 Table 3-8 lists the 12 codes a SUBACK may carry. Session
    /// taken over, 0x8E, is a DISCONNECT code; before this check a SUBACK
    /// carrying it encoded and decoded cleanly.
    #[test]
    fn rejects_a_suback_reason_code_outside_table_3_8() {
        let packet = SubAck {
            packet_id: 1,
            reasons: vec![ReasonCode::SessionTakenOver],
            properties: Default::default(),
        };
        let mut out = Vec::new();
        assert_eq!(
            packet.encode_body(Version::V5, &mut out).unwrap_err(),
            CodecError::ProtocolError("reason code is not permitted on this packet type")
        );

        // Hand-built: identifier 1, empty property block, reason 0x8E.
        let wire = [0x00, 0x01, 0x00, 0x8E];
        let mut reader = Reader::new(&wire);
        assert_eq!(
            SubAck::decode_body(&mut reader, Version::V5).unwrap_err(),
            CodecError::ProtocolError("reason code is not permitted on this packet type")
        );
    }

    /// Every code Table 3-8 does list, including the three granted
    /// quality-of-service values, survives a round trip.
    #[test]
    fn accepts_every_suback_reason_code_table_3_8_lists() {
        let codes = [
            ReasonCode::Success,
            ReasonCode::GrantedQos1,
            ReasonCode::GrantedQos2,
            ReasonCode::UnspecifiedError,
            ReasonCode::ImplementationSpecificError,
            ReasonCode::NotAuthorized,
            ReasonCode::TopicFilterInvalid,
            ReasonCode::PacketIdentifierInUse,
            ReasonCode::QuotaExceeded,
            ReasonCode::SharedSubscriptionsNotSupported,
            ReasonCode::SubscriptionIdentifiersNotSupported,
            ReasonCode::WildcardSubscriptionsNotSupported,
        ];
        for reason in codes {
            let packet = SubAck {
                packet_id: 1,
                reasons: vec![reason],
                properties: Default::default(),
            };
            let mut out = Vec::new();
            packet.encode_body(Version::V5, &mut out).expect("encodes");
            let mut reader = Reader::new(&out);
            assert_eq!(
                SubAck::decode_body(&mut reader, Version::V5).expect("decodes"),
                packet,
                "{reason:?}"
            );
        }
    }

    /// The four 3.1.1 return codes all sit inside the MQTT 5 table, so the
    /// narrower legacy encoding is untouched.
    #[test]
    fn the_legacy_suback_return_codes_still_round_trip() {
        let packet = SubAck {
            packet_id: 1,
            reasons: vec![
                ReasonCode::Success,
                ReasonCode::GrantedQos1,
                ReasonCode::GrantedQos2,
                ReasonCode::UnspecifiedError,
            ],
            properties: Default::default(),
        };
        let mut out = Vec::new();
        packet
            .encode_body(Version::V311, &mut out)
            .expect("encodes");
        assert_eq!(out, vec![0x00, 0x01, 0x00, 0x01, 0x02, 0x80]);
        let mut reader = Reader::new(&out);
        assert_eq!(
            SubAck::decode_body(&mut reader, Version::V311).expect("decodes"),
            packet
        );
    }

    /// MQTT-v5.0 Table 3-9 lists 7 codes for UNSUBACK. Quota exceeded,
    /// 0x97, is legal on a SUBACK but not here.
    #[test]
    fn rejects_an_unsuback_reason_code_outside_table_3_9() {
        let packet = UnsubAck {
            packet_id: 1,
            reasons: vec![ReasonCode::QuotaExceeded],
            properties: Default::default(),
        };
        let mut out = Vec::new();
        assert_eq!(
            packet.encode_body(Version::V5, &mut out).unwrap_err(),
            CodecError::ProtocolError("reason code is not permitted on this packet type")
        );

        // Hand-built: identifier 1, empty property block, reason 0x97.
        let wire = [0x00, 0x01, 0x00, 0x97];
        let mut reader = Reader::new(&wire);
        assert_eq!(
            UnsubAck::decode_body(&mut reader, Version::V5).unwrap_err(),
            CodecError::ProtocolError("reason code is not permitted on this packet type")
        );
    }
}
