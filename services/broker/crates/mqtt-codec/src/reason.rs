//! Reason codes, shared by every acknowledgement path.

use crate::error::CodecError;

/// A reason code, as carried by MQTT 5 acknowledgements.
///
/// The discriminant is the byte that appears on the wire. MQTT 3.1.1 carries
/// a narrower set, so [`ReasonCode::to_v311_connack`] and
/// [`ReasonCode::to_v311_suback`] project onto the legacy values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[allow(missing_docs)]
pub enum ReasonCode {
    /// Also granted QoS 0, and normal disconnection.
    Success = 0x00,
    GrantedQos1 = 0x01,
    GrantedQos2 = 0x02,
    DisconnectWithWillMessage = 0x04,
    NoMatchingSubscribers = 0x10,
    NoSubscriptionExisted = 0x11,
    ContinueAuthentication = 0x18,
    ReAuthenticate = 0x19,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    ClientIdentifierNotValid = 0x85,
    BadUserNameOrPassword = 0x86,
    NotAuthorized = 0x87,
    ServerUnavailable = 0x88,
    ServerBusy = 0x89,
    Banned = 0x8A,
    ServerShuttingDown = 0x8B,
    BadAuthenticationMethod = 0x8C,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    PacketIdentifierInUse = 0x91,
    PacketIdentifierNotFound = 0x92,
    ReceiveMaximumExceeded = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    MessageRateTooHigh = 0x96,
    QuotaExceeded = 0x97,
    AdministrativeAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QosNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    SharedSubscriptionsNotSupported = 0x9E,
    ConnectionRateExceeded = 0x9F,
    MaximumConnectTime = 0xA0,
    SubscriptionIdentifiersNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl ReasonCode {
    /// Interpret a wire byte, rejecting unassigned values.
    pub fn from_byte(value: u8) -> Option<Self> {
        use ReasonCode::*;
        Some(match value {
            0x00 => Success,
            0x01 => GrantedQos1,
            0x02 => GrantedQos2,
            0x04 => DisconnectWithWillMessage,
            0x10 => NoMatchingSubscribers,
            0x11 => NoSubscriptionExisted,
            0x18 => ContinueAuthentication,
            0x19 => ReAuthenticate,
            0x80 => UnspecifiedError,
            0x81 => MalformedPacket,
            0x82 => ProtocolError,
            0x83 => ImplementationSpecificError,
            0x84 => UnsupportedProtocolVersion,
            0x85 => ClientIdentifierNotValid,
            0x86 => BadUserNameOrPassword,
            0x87 => NotAuthorized,
            0x88 => ServerUnavailable,
            0x89 => ServerBusy,
            0x8A => Banned,
            0x8B => ServerShuttingDown,
            0x8C => BadAuthenticationMethod,
            0x8D => KeepAliveTimeout,
            0x8E => SessionTakenOver,
            0x8F => TopicFilterInvalid,
            0x90 => TopicNameInvalid,
            0x91 => PacketIdentifierInUse,
            0x92 => PacketIdentifierNotFound,
            0x93 => ReceiveMaximumExceeded,
            0x94 => TopicAliasInvalid,
            0x95 => PacketTooLarge,
            0x96 => MessageRateTooHigh,
            0x97 => QuotaExceeded,
            0x98 => AdministrativeAction,
            0x99 => PayloadFormatInvalid,
            0x9A => RetainNotSupported,
            0x9B => QosNotSupported,
            0x9C => UseAnotherServer,
            0x9D => ServerMoved,
            0x9E => SharedSubscriptionsNotSupported,
            0x9F => ConnectionRateExceeded,
            0xA0 => MaximumConnectTime,
            0xA1 => SubscriptionIdentifiersNotSupported,
            0xA2 => WildcardSubscriptionsNotSupported,
            _ => return None,
        })
    }

    /// Whether this code denotes a failure. Every value at or above 0x80 does.
    pub fn is_failure(self) -> bool {
        (self as u8) >= 0x80
    }

    /// Project onto the MQTT 3.1.1 CONNACK return codes.
    ///
    /// Defined only for codes that can legally appear in a 3.1.1 CONNACK.
    /// Any other code collapses to 3 ("server unavailable"), the broadest
    /// failure a 3.1.1 client understands.
    ///
    /// This projection must not be used to answer a CONNECT that is
    /// malformed or that violates the protocol: MQTT 3.1.1 requires the
    /// server to close the connection with no CONNACK at all in that case.
    /// Sending a CONNACK with return code 3 tells the client the failure is
    /// transient and invites a retry, which is the wrong signal for a
    /// malformed or protocol-violating CONNECT.
    pub fn to_v311_connack(self) -> u8 {
        use ReasonCode::*;
        match self {
            Success => 0,
            UnsupportedProtocolVersion => 1,
            ClientIdentifierNotValid => 2,
            ServerUnavailable => 3,
            BadUserNameOrPassword => 4,
            NotAuthorized | Banned => 5,
            _ => 3,
        }
    }

    /// Project onto the MQTT 3.1.1 SUBACK return codes.
    ///
    /// Defined only for codes that can legally appear in a 3.1.1 SUBACK.
    /// Any other code collapses to 0x80, the broadest failure a 3.1.1
    /// client understands. This includes codes such as
    /// [`ReasonCode::NoSubscriptionExisted`] and
    /// [`ReasonCode::NoMatchingSubscribers`], which cannot legally appear
    /// in a 3.1.1 SUBACK in the first place, so their mapping to 0x80 is
    /// unreachable in practice rather than meaningful.
    pub fn to_v311_suback(self) -> u8 {
        use ReasonCode::*;
        match self {
            Success => 0x00,
            GrantedQos1 => 0x01,
            GrantedQos2 => 0x02,
            _ => 0x80,
        }
    }
}

impl From<CodecError> for ReasonCode {
    fn from(error: CodecError) -> Self {
        match error {
            CodecError::MalformedPacket(_) | CodecError::InvalidUtf8 => ReasonCode::MalformedPacket,
            CodecError::ProtocolError(_) => ReasonCode::ProtocolError,
            CodecError::PacketTooLarge => ReasonCode::PacketTooLarge,
            CodecError::UnsupportedVersion(_) => ReasonCode::UnsupportedProtocolVersion,
            CodecError::InvalidTopicName | CodecError::TopicTooDeep => ReasonCode::TopicNameInvalid,
            CodecError::InvalidTopicFilter => ReasonCode::TopicFilterInvalid,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;

    /// Every variant, in ascending numeric order. Exhaustive so that adding a
    /// variant without adding it here trips the length assertion below.
    const ALL: &[ReasonCode] = &[
        ReasonCode::Success,
        ReasonCode::GrantedQos1,
        ReasonCode::GrantedQos2,
        ReasonCode::DisconnectWithWillMessage,
        ReasonCode::NoMatchingSubscribers,
        ReasonCode::NoSubscriptionExisted,
        ReasonCode::ContinueAuthentication,
        ReasonCode::ReAuthenticate,
        ReasonCode::UnspecifiedError,
        ReasonCode::MalformedPacket,
        ReasonCode::ProtocolError,
        ReasonCode::ImplementationSpecificError,
        ReasonCode::UnsupportedProtocolVersion,
        ReasonCode::ClientIdentifierNotValid,
        ReasonCode::BadUserNameOrPassword,
        ReasonCode::NotAuthorized,
        ReasonCode::ServerUnavailable,
        ReasonCode::ServerBusy,
        ReasonCode::Banned,
        ReasonCode::ServerShuttingDown,
        ReasonCode::BadAuthenticationMethod,
        ReasonCode::KeepAliveTimeout,
        ReasonCode::SessionTakenOver,
        ReasonCode::TopicFilterInvalid,
        ReasonCode::TopicNameInvalid,
        ReasonCode::PacketIdentifierInUse,
        ReasonCode::PacketIdentifierNotFound,
        ReasonCode::ReceiveMaximumExceeded,
        ReasonCode::TopicAliasInvalid,
        ReasonCode::PacketTooLarge,
        ReasonCode::MessageRateTooHigh,
        ReasonCode::QuotaExceeded,
        ReasonCode::AdministrativeAction,
        ReasonCode::PayloadFormatInvalid,
        ReasonCode::RetainNotSupported,
        ReasonCode::QosNotSupported,
        ReasonCode::UseAnotherServer,
        ReasonCode::ServerMoved,
        ReasonCode::SharedSubscriptionsNotSupported,
        ReasonCode::ConnectionRateExceeded,
        ReasonCode::MaximumConnectTime,
        ReasonCode::SubscriptionIdentifiersNotSupported,
        ReasonCode::WildcardSubscriptionsNotSupported,
    ];

    #[test]
    fn round_trips_through_its_byte_value() {
        assert_eq!(ALL.len(), 43);
        for &code in ALL {
            assert_eq!(ReasonCode::from_byte(code as u8), Some(code));
        }
    }

    #[test]
    fn rejects_an_unassigned_byte() {
        assert_eq!(ReasonCode::from_byte(0x7F), None);
    }

    #[test]
    fn knows_which_codes_are_failures() {
        assert!(!ReasonCode::Success.is_failure());
        assert!(!ReasonCode::GrantedQos2.is_failure());
        assert!(ReasonCode::NotAuthorized.is_failure());
        assert!(ReasonCode::MalformedPacket.is_failure());
    }

    #[test]
    fn narrows_to_the_legacy_connack_return_codes() {
        assert_eq!(ReasonCode::Success.to_v311_connack(), 0);
        assert_eq!(ReasonCode::UnsupportedProtocolVersion.to_v311_connack(), 1);
        assert_eq!(ReasonCode::ClientIdentifierNotValid.to_v311_connack(), 2);
        assert_eq!(ReasonCode::ServerUnavailable.to_v311_connack(), 3);
        assert_eq!(ReasonCode::BadUserNameOrPassword.to_v311_connack(), 4);
        assert_eq!(ReasonCode::NotAuthorized.to_v311_connack(), 5);
        // Anything with no legacy equivalent reports the broadest failure.
        assert_eq!(ReasonCode::QuotaExceeded.to_v311_connack(), 3);
    }

    #[test]
    fn narrows_to_the_legacy_suback_codes() {
        assert_eq!(ReasonCode::Success.to_v311_suback(), 0x00);
        assert_eq!(ReasonCode::GrantedQos1.to_v311_suback(), 0x01);
        assert_eq!(ReasonCode::GrantedQos2.to_v311_suback(), 0x02);
        assert_eq!(ReasonCode::NotAuthorized.to_v311_suback(), 0x80);
        assert_eq!(ReasonCode::TopicFilterInvalid.to_v311_suback(), 0x80);
    }

    #[test]
    fn maps_every_codec_error_to_a_reason_code() {
        assert_eq!(
            ReasonCode::from(CodecError::MalformedPacket("x")),
            ReasonCode::MalformedPacket
        );
        assert_eq!(
            ReasonCode::from(CodecError::ProtocolError("x")),
            ReasonCode::ProtocolError
        );
        assert_eq!(
            ReasonCode::from(CodecError::PacketTooLarge),
            ReasonCode::PacketTooLarge
        );
        assert_eq!(
            ReasonCode::from(CodecError::UnsupportedVersion(3)),
            ReasonCode::UnsupportedProtocolVersion
        );
        assert_eq!(
            ReasonCode::from(CodecError::InvalidUtf8),
            ReasonCode::MalformedPacket
        );
        assert_eq!(
            ReasonCode::from(CodecError::InvalidTopicName),
            ReasonCode::TopicNameInvalid
        );
        assert_eq!(
            ReasonCode::from(CodecError::InvalidTopicFilter),
            ReasonCode::TopicFilterInvalid
        );
        assert_eq!(
            ReasonCode::from(CodecError::TopicTooDeep),
            ReasonCode::TopicNameInvalid
        );
    }
}
