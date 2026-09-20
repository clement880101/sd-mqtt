//! Errors produced while encoding or decoding.

use thiserror::Error;

use crate::reason::ReasonCode;

/// Everything that can go wrong on the wire.
///
/// Every variant carries a static description so that logs can identify the
/// exact check that failed without allocating.
///
/// Marked `#[non_exhaustive]`: MQTT 5 has checks this crate does not yet
/// make, and adding a variant for one of them must not be a breaking change
/// for a released crate. Downstream matches need a wildcard arm.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum CodecError {
    /// The bytes do not form a valid packet.
    #[error("malformed packet: {0}")]
    MalformedPacket(&'static str),

    /// The packet is well formed but violates a protocol rule.
    #[error("protocol error: {0}")]
    ProtocolError(&'static str),

    /// The declared size exceeds the configured maximum.
    #[error("packet too large")]
    PacketTooLarge,

    /// The CONNECT packet named a protocol level this broker does not serve.
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    /// A string field was not valid UTF-8, or contained a null character.
    #[error("invalid UTF-8 string")]
    InvalidUtf8,

    /// A publish topic was empty, or contained a wildcard.
    #[error("invalid topic name")]
    InvalidTopicName,

    /// A subscription filter violated the wildcard placement rules.
    #[error("invalid topic filter")]
    InvalidTopicFilter,

    /// A publish topic name had more levels than the configured maximum.
    ///
    /// Separate from [`CodecError::TopicFilterTooDeep`] because the two
    /// convert to different reason codes: 0x90 is not a legal SUBACK code,
    /// so an over-deep filter reported as an over-deep name would produce
    /// an off-spec SUBACK through the obvious `ReasonCode::from` idiom.
    #[error("topic name too deep")]
    TopicNameTooDeep,

    /// A subscription filter had more levels than the configured maximum.
    #[error("topic filter too deep")]
    TopicFilterTooDeep,
}

impl CodecError {
    /// The reason code that reports this error to a peer.
    ///
    /// MQTT-v5.0 section 2.4 and the tables it feeds, chiefly Table
    /// 3-10 under 3.14.2.1. The mapping lives here rather than in a
    /// shell because it is a property of the error, and a second shell
    /// would otherwise write it again and differently.
    ///
    /// Two of these need saying out loud:
    ///
    /// - An over-deep topic filter reports 0x8F rather than 0x90,
    ///   because 0x90 is not a legal SUBACK code; see the note on
    ///   [`CodecError::TopicNameTooDeep`].
    /// - [`CodecError::UnsupportedVersion`] reports 0x84, which is a
    ///   CONNACK code and appears in no other packet's table. A caller
    ///   that answers with a DISCONNECT must narrow it, because Table
    ///   3-10 does not list it; the mapping here is what the
    ///   specification says the error is, not where it may be sent.
    pub fn reason_code(&self) -> ReasonCode {
        match self {
            Self::MalformedPacket(_) | Self::InvalidUtf8 => ReasonCode::MalformedPacket,
            Self::ProtocolError(_) => ReasonCode::ProtocolError,
            Self::PacketTooLarge => ReasonCode::PacketTooLarge,
            Self::UnsupportedVersion(_) => ReasonCode::UnsupportedProtocolVersion,
            Self::InvalidTopicName | Self::TopicNameTooDeep => ReasonCode::TopicNameInvalid,
            Self::InvalidTopicFilter | Self::TopicFilterTooDeep => ReasonCode::TopicFilterInvalid,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reason::ReasonCode;

    /// Every variant, with the wire byte of the reason code that
    /// reports it to a peer.
    ///
    /// Transcribed from the master table of reason codes in MQTT-v5.0
    /// section 2.4, cross-checked against Table 3-10 under 3.14.2.1,
    /// which is where all but one of these go out. Written from the
    /// specification rather than from [`CodecError::reason_code`]:
    /// convention 5 records that a table read off the code under test
    /// cannot see a transposition, which is why this crate's property
    /// identifiers are pinned to literal bytes too.
    ///
    /// Bytes rather than variant names, for the same reason: two
    /// variants whose discriminants were swapped round trip perfectly
    /// through every name-level assertion in this crate.
    const REPORTED_AS: &[(CodecError, u8)] = &[
        (CodecError::MalformedPacket("any"), 0x81),
        (CodecError::InvalidUtf8, 0x81),
        (CodecError::ProtocolError("any"), 0x82),
        (CodecError::PacketTooLarge, 0x95),
        (CodecError::InvalidTopicName, 0x90),
        (CodecError::InvalidTopicFilter, 0x8F),
        (CodecError::TopicNameTooDeep, 0x90),
        (CodecError::TopicFilterTooDeep, 0x8F),
        (CodecError::UnsupportedVersion(6), 0x84),
    ];

    #[test]
    fn reports_each_error_with_the_reason_code_the_specification_gives() {
        // Exhaustive: `reason_code` matches without a wildcard, so a
        // new variant fails to compile there, and this count fails
        // here if one is added without a row.
        assert_eq!(REPORTED_AS.len(), 9, "one row per variant");
        for (error, byte) in REPORTED_AS {
            assert_eq!(
                error.reason_code() as u8,
                *byte,
                "{error:?} reports 0x{byte:02X}"
            );
        }
    }

    #[test]
    fn the_conversion_and_the_method_are_one_mapping() {
        // Two mappings drift. The `From` impl exists for the ergonomics
        // of `?`, and must stay this method rather than a copy of it.
        for (error, _) in REPORTED_AS {
            assert_eq!(ReasonCode::from(error.clone()), error.reason_code());
        }
    }
}
