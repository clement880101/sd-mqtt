//! DISCONNECT and AUTH. PINGREQ and PINGRESP have no body at all.

use crate::error::CodecError;
use crate::properties::Properties;
use crate::reader::Reader;
use crate::reason::ReasonCode;
use crate::writer::put_u8;
use crate::Version;

/// A deliberate end to the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Disconnect {
    /// Why. [`ReasonCode::Success`] is a normal disconnection.
    pub reason: ReasonCode,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl Disconnect {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(reader: &mut Reader<'_>, version: Version) -> Result<Self, CodecError> {
        if version == Version::V311 || reader.is_empty() {
            return Ok(Disconnect {
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

        Ok(Disconnect { reason, properties })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
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

/// A step in the MQTT 5 enhanced authentication exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auth {
    /// Whether to continue, re-authenticate, or that it succeeded.
    pub reason: ReasonCode,
    /// MQTT 5 properties, carrying the method and its data.
    pub properties: Properties,
}

impl Auth {
    /// Decode the body that follows the fixed header.
    ///
    /// AUTH exists only in MQTT 5, so no version argument is needed. The
    /// caller rejects the packet outright on a 3.1.1 connection.
    pub fn decode_body(reader: &mut Reader<'_>) -> Result<Self, CodecError> {
        if reader.is_empty() {
            return Ok(Auth {
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

        Ok(Auth { reason, properties })
    }

    /// Append the body, excluding the fixed header.
    ///
    /// AUTH exists only in MQTT 5, so encoding one as 3.1.1 is refused
    /// rather than silently producing a packet that version cannot carry.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        if version == Version::V311 {
            return Err(CodecError::ProtocolError(
                "authentication packets do not exist in 3.1.1",
            ));
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
    use crate::reader::Reader;
    use crate::reason::ReasonCode;
    use crate::Version;

    #[test]
    fn a_v311_disconnect_has_an_empty_body() {
        let packet = Disconnect {
            reason: ReasonCode::Success,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();
        assert!(out.is_empty());

        let mut reader = Reader::new(&out);
        assert_eq!(
            Disconnect::decode_body(&mut reader, Version::V311).unwrap(),
            packet
        );
    }

    #[test]
    fn a_v5_normal_disconnect_also_has_an_empty_body() {
        let packet = Disconnect {
            reason: ReasonCode::Success,
            properties: Default::default(),
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn a_v5_disconnect_carries_a_reason_and_properties() {
        let packet = Disconnect {
            reason: ReasonCode::SessionTakenOver,
            properties: crate::Properties {
                reason_string: Some("another connection claimed this identifier".into()),
                ..Default::default()
            },
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(
            Disconnect::decode_body(&mut reader, Version::V5).unwrap(),
            packet
        );
    }

    #[test]
    fn a_v5_disconnect_may_carry_a_reason_with_no_properties() {
        let bytes = [ReasonCode::KeepAliveTimeout as u8];
        let mut reader = Reader::new(&bytes);
        let decoded = Disconnect::decode_body(&mut reader, Version::V5).unwrap();
        assert_eq!(decoded.reason, ReasonCode::KeepAliveTimeout);
        assert!(decoded.properties.is_empty());
    }

    #[test]
    fn rejects_an_unknown_disconnect_reason() {
        let bytes = [0x7F];
        let mut reader = Reader::new(&bytes);
        assert_eq!(
            Disconnect::decode_body(&mut reader, Version::V5).unwrap_err(),
            CodecError::MalformedPacket("unknown reason code")
        );
    }

    #[test]
    fn round_trips_an_auth_exchange() {
        let packet = Auth {
            reason: ReasonCode::ContinueAuthentication,
            properties: crate::Properties {
                authentication_method: Some("SCRAM-SHA-256".into()),
                authentication_data: Some(bytes::Bytes::from_static(b"r=nonce")),
                ..Default::default()
            },
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V5, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(Auth::decode_body(&mut reader).unwrap(), packet);
    }

    #[test]
    fn an_empty_auth_body_means_success() {
        let mut reader = Reader::new(&[]);
        let decoded = Auth::decode_body(&mut reader).unwrap();
        assert_eq!(decoded.reason, ReasonCode::Success);
        assert!(decoded.properties.is_empty());
    }

    #[test]
    fn refuses_to_encode_an_auth_as_v311() {
        let packet = Auth {
            reason: ReasonCode::ContinueAuthentication,
            properties: crate::Properties {
                authentication_method: Some("SCRAM-SHA-256".into()),
                ..Default::default()
            },
        };
        let mut out = Vec::new();
        assert_eq!(
            packet.encode_body(Version::V311, &mut out).unwrap_err(),
            CodecError::ProtocolError("authentication packets do not exist in 3.1.1")
        );
    }
}
