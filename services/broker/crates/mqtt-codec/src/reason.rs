//! Reason codes, shared by every acknowledgement path.

use crate::error::CodecError;
use crate::header::PacketKind;

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

/// Whether the specification permits `reason` on `kind`.
///
/// Every MQTT 5 packet that carries a reason code carries its own table of
/// the codes it may carry, and a code outside that table is off-spec even
/// though [`ReasonCode::from_byte`] recognises it. The sets below are
/// transcribed from the OASIS MQTT 5.0 specification and cross-checked
/// against the master table in section 2.4, which lists for each code the
/// packets that may send it:
///
/// - CONNACK: Table 3-1, section 3.2.2.2. 22 codes.
/// - PUBACK and PUBREC: Tables 3-4 and 3-5, sections 3.4.2.1 and 3.5.2.1.
///   One set of 9, shared.
/// - PUBREL and PUBCOMP: Tables 3-6 and 3-7. One set of 2, shared.
/// - SUBACK: Table 3-8, section 3.9.3. 12 codes, which include the three
///   granted-quality-of-service values 0x00, 0x01 and 0x02.
/// - UNSUBACK: Table 3-9, section 3.11.3. 7 codes.
/// - DISCONNECT: Table 3-10, section 3.14.2.1. 30 codes; see the note below.
/// - AUTH: Table 3-11, section 3.15.2.1. 3 codes.
///
/// CONNECT, PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PINGREQ and PINGRESP carry no
/// reason code at all, so every code is refused on them; passing one of
/// those kinds is a caller mistake and is answered with an error rather
/// than a panic.
///
/// **One deliberate divergence.** Table 3-10 omits 0x8C, bad authentication
/// method, jumping from 139 to 141, while the master table in section 2.4
/// lists 0x8C as sent by CONNACK *and* DISCONNECT. The published
/// specification contradicts itself and OASIS has issued no errata for
/// MQTT 5.0. Section 4.12.1 tells a peer whose re-authentication fails to
/// send DISCONNECT with an appropriate reason code, and bad authentication
/// method is the only code that describes that failure, so the permissive
/// reading is taken and 0x8C is accepted on DISCONNECT.
fn permitted(kind: PacketKind, reason: ReasonCode) -> bool {
    use ReasonCode::*;
    match kind {
        PacketKind::ConnAck => matches!(
            reason,
            Success
                | UnspecifiedError
                | MalformedPacket
                | ProtocolError
                | ImplementationSpecificError
                | UnsupportedProtocolVersion
                | ClientIdentifierNotValid
                | BadUserNameOrPassword
                | NotAuthorized
                | ServerUnavailable
                | ServerBusy
                | Banned
                | BadAuthenticationMethod
                | TopicNameInvalid
                | PacketTooLarge
                | QuotaExceeded
                | PayloadFormatInvalid
                | RetainNotSupported
                | QosNotSupported
                | UseAnotherServer
                | ServerMoved
                | ConnectionRateExceeded
        ),
        PacketKind::PubAck | PacketKind::PubRec => matches!(
            reason,
            Success
                | NoMatchingSubscribers
                | UnspecifiedError
                | ImplementationSpecificError
                | NotAuthorized
                | TopicNameInvalid
                | PacketIdentifierInUse
                | QuotaExceeded
                | PayloadFormatInvalid
        ),
        PacketKind::PubRel | PacketKind::PubComp => {
            matches!(reason, Success | PacketIdentifierNotFound)
        }
        // 0x00 is "granted QoS 0" here rather than plain success.
        PacketKind::SubAck => matches!(
            reason,
            Success
                | GrantedQos1
                | GrantedQos2
                | UnspecifiedError
                | ImplementationSpecificError
                | NotAuthorized
                | TopicFilterInvalid
                | PacketIdentifierInUse
                | QuotaExceeded
                | SharedSubscriptionsNotSupported
                | SubscriptionIdentifiersNotSupported
                | WildcardSubscriptionsNotSupported
        ),
        PacketKind::UnsubAck => matches!(
            reason,
            Success
                | NoSubscriptionExisted
                | UnspecifiedError
                | ImplementationSpecificError
                | NotAuthorized
                | TopicFilterInvalid
                | PacketIdentifierInUse
        ),
        // 0x00 is "normal disconnection" here rather than plain success.
        PacketKind::Disconnect => matches!(
            reason,
            Success
                | DisconnectWithWillMessage
                | UnspecifiedError
                | MalformedPacket
                | ProtocolError
                | ImplementationSpecificError
                | NotAuthorized
                | ServerBusy
                | ServerShuttingDown
                | BadAuthenticationMethod
                | KeepAliveTimeout
                | SessionTakenOver
                | TopicFilterInvalid
                | TopicNameInvalid
                | ReceiveMaximumExceeded
                | TopicAliasInvalid
                | PacketTooLarge
                | MessageRateTooHigh
                | QuotaExceeded
                | AdministrativeAction
                | PayloadFormatInvalid
                | RetainNotSupported
                | QosNotSupported
                | UseAnotherServer
                | ServerMoved
                | SharedSubscriptionsNotSupported
                | ConnectionRateExceeded
                | MaximumConnectTime
                | SubscriptionIdentifiersNotSupported
                | WildcardSubscriptionsNotSupported
        ),
        PacketKind::Auth => matches!(reason, Success | ContinueAuthentication | ReAuthenticate),
        PacketKind::Connect
        | PacketKind::Publish
        | PacketKind::Subscribe
        | PacketKind::Unsubscribe
        | PacketKind::PingReq
        | PacketKind::PingResp => false,
    }
}

/// The error every packet reports for a reason code outside its own table.
///
/// One string for every packet type, because the failure is the same one:
/// the code is recognised but does not belong here.
pub(crate) fn check_reason(kind: PacketKind, reason: ReasonCode) -> Result<(), CodecError> {
    if permitted(kind, reason) {
        Ok(())
    } else {
        Err(CodecError::ProtocolError(
            "reason code is not permitted on this packet type",
        ))
    }
}

impl From<CodecError> for ReasonCode {
    fn from(error: CodecError) -> Self {
        match error {
            CodecError::MalformedPacket(_) | CodecError::InvalidUtf8 => ReasonCode::MalformedPacket,
            CodecError::ProtocolError(_) => ReasonCode::ProtocolError,
            CodecError::PacketTooLarge => ReasonCode::PacketTooLarge,
            CodecError::UnsupportedVersion(_) => ReasonCode::UnsupportedProtocolVersion,
            CodecError::InvalidTopicName | CodecError::TopicNameTooDeep => {
                ReasonCode::TopicNameInvalid
            }
            CodecError::InvalidTopicFilter | CodecError::TopicFilterTooDeep => {
                ReasonCode::TopicFilterInvalid
            }
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

    /// Every packet that carries a reason code, with the exact set the
    /// specification permits on it, written out as wire bytes rather than
    /// variant names so that the table can be read straight off the
    /// specification's own tables. Checked in both directions below: no
    /// code outside the set is accepted, and no code inside it is refused.
    const SETS: &[(PacketKind, &[u8])] = &[
        // Table 3-1, section 3.2.2.2.
        (
            PacketKind::ConnAck,
            &[
                0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8C, 0x90,
                0x95, 0x97, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9F,
            ],
        ),
        // Tables 3-4 and 3-5.
        (
            PacketKind::PubAck,
            &[0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99],
        ),
        (
            PacketKind::PubRec,
            &[0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99],
        ),
        // Tables 3-6 and 3-7.
        (PacketKind::PubRel, &[0x00, 0x92]),
        (PacketKind::PubComp, &[0x00, 0x92]),
        // Table 3-8, section 3.9.3.
        (
            PacketKind::SubAck,
            &[
                0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2,
            ],
        ),
        // Table 3-9, section 3.11.3.
        (
            PacketKind::UnsubAck,
            &[0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91],
        ),
        // Table 3-10, section 3.14.2.1, plus 0x8C from the section 2.4
        // master table; see the note on `permitted`.
        (
            PacketKind::Disconnect,
            &[
                0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8C, 0x8D, 0x8E, 0x8F, 0x90,
                0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0,
                0xA1, 0xA2,
            ],
        ),
        // Table 3-11, section 3.15.2.1.
        (PacketKind::Auth, &[0x00, 0x18, 0x19]),
    ];

    #[test]
    fn permits_exactly_the_codes_each_packet_may_carry() {
        // The counts each specification table shows, asserted separately so
        // that a code dropped from a list above is caught even if it is
        // dropped from `permitted` at the same time.
        let sizes: Vec<usize> = SETS.iter().map(|(_, codes)| codes.len()).collect();
        assert_eq!(sizes, vec![22, 9, 9, 2, 2, 12, 7, 30, 3]);

        for (kind, codes) in SETS {
            for &code in ALL {
                let expected = codes.contains(&(code as u8));
                assert_eq!(
                    permitted(*kind, code),
                    expected,
                    "{kind:?} and {code:?} (0x{:02X})",
                    code as u8
                );
            }
        }
    }

    #[test]
    fn permits_no_code_on_a_packet_that_carries_none() {
        for kind in [
            PacketKind::Connect,
            PacketKind::Publish,
            PacketKind::Subscribe,
            PacketKind::Unsubscribe,
            PacketKind::PingReq,
            PacketKind::PingResp,
        ] {
            for &code in ALL {
                assert!(!permitted(kind, code), "{kind:?} should carry no {code:?}");
            }
        }
    }

    #[test]
    fn reports_one_error_for_a_code_outside_the_table() {
        assert_eq!(
            check_reason(PacketKind::SubAck, ReasonCode::SessionTakenOver).unwrap_err(),
            CodecError::ProtocolError("reason code is not permitted on this packet type")
        );
        assert_eq!(
            check_reason(PacketKind::SubAck, ReasonCode::GrantedQos2),
            Ok(())
        );
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
            ReasonCode::from(CodecError::TopicNameTooDeep),
            ReasonCode::TopicNameInvalid
        );
        // 0x90 is not a legal SUBACK code, so an over-deep filter must not
        // convert to it; MQTT-v5.0 Table 3-8.
        assert_eq!(
            ReasonCode::from(CodecError::TopicFilterTooDeep),
            ReasonCode::TopicFilterInvalid
        );
        assert!(permitted(
            PacketKind::SubAck,
            ReasonCode::from(CodecError::TopicFilterTooDeep)
        ));
    }
}
