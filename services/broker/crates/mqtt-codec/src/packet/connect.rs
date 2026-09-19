//! CONNECT and CONNACK.

use bytes::Bytes;

use crate::error::CodecError;
use crate::header::Limits;
use crate::properties::Properties;
use crate::qos::QoS;
use crate::reader::Reader;
use crate::reason::ReasonCode;
use crate::writer::{put_binary, put_string, put_u16, put_u8};
use crate::Version;

/// The message the broker publishes if a client disappears ungracefully.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Will {
    /// Where to publish it.
    pub topic: String,
    /// What to publish.
    pub payload: Bytes,
    /// At what quality of service.
    pub qos: QoS,
    /// Whether to retain it.
    pub retain: bool,
    /// MQTT 5 properties, including the will delay.
    pub properties: Properties,
}

/// The first packet a client sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    /// Which protocol version the client speaks.
    pub version: Version,
    /// The client's identifier, possibly empty.
    pub client_id: String,
    /// Whether to discard any existing session.
    pub clean_start: bool,
    /// The client's proposed keep-alive interval in seconds.
    pub keep_alive: u16,
    /// The offered user name.
    pub username: Option<String>,
    /// The offered password, which is binary rather than text.
    pub password: Option<Bytes>,
    /// The will, if the client set one.
    pub will: Option<Will>,
    /// MQTT 5 properties.
    pub properties: Properties,
}

const FLAG_CLEAN_START: u8 = 0x02;
const FLAG_WILL: u8 = 0x04;
const FLAG_WILL_RETAIN: u8 = 0x20;
const FLAG_PASSWORD: u8 = 0x40;
const FLAG_USERNAME: u8 = 0x80;

impl Connect {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(reader: &mut Reader<'_>, limits: &Limits) -> Result<Self, CodecError> {
        let name = reader.string()?;
        if name != "MQTT" {
            return Err(CodecError::MalformedPacket("protocol name is not MQTT"));
        }

        let level = reader.u8()?;
        let version = match level {
            4 => Version::V311,
            5 => Version::V5,
            other => return Err(CodecError::UnsupportedVersion(other)),
        };

        let flags = reader.u8()?;
        if flags & 0x01 != 0 {
            return Err(CodecError::MalformedPacket(
                "reserved bit in the connect flags is set",
            ));
        }

        let clean_start = flags & FLAG_CLEAN_START != 0;
        let has_will = flags & FLAG_WILL != 0;
        let will_qos = QoS::from_u8((flags & 0x18) >> 3)?;
        let will_retain = flags & FLAG_WILL_RETAIN != 0;
        let has_password = flags & FLAG_PASSWORD != 0;
        let has_username = flags & FLAG_USERNAME != 0;

        if !has_will && (will_qos != QoS::AtMostOnce || will_retain) {
            return Err(CodecError::MalformedPacket("will flags set without a will"));
        }
        if version == Version::V311 && has_password && !has_username {
            return Err(CodecError::MalformedPacket(
                "password without a user name is not permitted in 3.1.1",
            ));
        }

        let keep_alive = reader.u16()?;

        let properties = if version == Version::V5 {
            Properties::decode(reader)?
        } else {
            Properties::default()
        };

        let client_id = reader.string()?;
        if client_id.len() > usize::from(limits.max_client_id_len) {
            return Err(CodecError::ProtocolError(
                "client identifier longer than the configured maximum",
            ));
        }

        let will = if has_will {
            let will_properties = if version == Version::V5 {
                Properties::decode(reader)?
            } else {
                Properties::default()
            };
            let topic = reader.string()?;
            let payload = reader.binary()?;
            crate::topic::validate_name(&topic, limits)?;
            Some(Will {
                topic,
                payload,
                qos: will_qos,
                retain: will_retain,
                properties: will_properties,
            })
        } else {
            None
        };

        let username = if has_username {
            Some(reader.string()?)
        } else {
            None
        };
        let password = if has_password {
            Some(reader.binary()?)
        } else {
            None
        };

        Ok(Connect {
            version,
            client_id,
            clean_start,
            keep_alive,
            username,
            password,
            will,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    ///
    /// `version` selects whether property blocks are written. It should match
    /// `self.version`; the argument exists so that every packet type shares
    /// one encoding signature.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_string(out, "MQTT")?;
        put_u8(out, version.level());

        let mut flags = 0u8;
        if self.clean_start {
            flags |= FLAG_CLEAN_START;
        }
        if let Some(will) = &self.will {
            flags |= FLAG_WILL;
            flags |= will.qos.as_u8() << 3;
            if will.retain {
                flags |= FLAG_WILL_RETAIN;
            }
        }
        if self.username.is_some() {
            flags |= FLAG_USERNAME;
        }
        if self.password.is_some() {
            flags |= FLAG_PASSWORD;
        }
        put_u8(out, flags);
        put_u16(out, self.keep_alive);

        if version == Version::V5 {
            self.properties.encode(out)?;
        }

        put_string(out, &self.client_id)?;

        if let Some(will) = &self.will {
            if version == Version::V5 {
                will.properties.encode(out)?;
            }
            put_string(out, &will.topic)?;
            put_binary(out, &will.payload)?;
        }
        if let Some(username) = &self.username {
            put_string(out, username)?;
        }
        if let Some(password) = &self.password {
            put_binary(out, password)?;
        }

        Ok(())
    }
}

/// The broker's answer to CONNECT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    /// Whether the broker resumed an existing session.
    pub session_present: bool,
    /// Why the connection was accepted or refused.
    pub reason: ReasonCode,
    /// MQTT 5 properties.
    pub properties: Properties,
}

impl ConnAck {
    /// Decode the body that follows the fixed header.
    pub fn decode_body(reader: &mut Reader<'_>, version: Version) -> Result<Self, CodecError> {
        let flags = reader.u8()?;
        if flags & 0xFE != 0 {
            return Err(CodecError::MalformedPacket(
                "reserved bits in the connack flags are set",
            ));
        }
        let session_present = flags & 0x01 != 0;
        let code = reader.u8()?;

        let reason = match version {
            Version::V5 => ReasonCode::from_byte(code)
                .ok_or(CodecError::MalformedPacket("unknown reason code"))?,
            Version::V311 => match code {
                0 => ReasonCode::Success,
                1 => ReasonCode::UnsupportedProtocolVersion,
                2 => ReasonCode::ClientIdentifierNotValid,
                3 => ReasonCode::ServerUnavailable,
                4 => ReasonCode::BadUserNameOrPassword,
                5 => ReasonCode::NotAuthorized,
                _ => return Err(CodecError::MalformedPacket("unknown connack return code")),
            },
        };

        let properties = if version == Version::V5 {
            Properties::decode(reader)?
        } else {
            Properties::default()
        };

        Ok(ConnAck {
            session_present,
            reason,
            properties,
        })
    }

    /// Append the body, excluding the fixed header.
    pub fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        put_u8(out, u8::from(self.session_present));
        match version {
            Version::V5 => {
                put_u8(out, self.reason as u8);
                self.properties.encode(out)?;
            }
            Version::V311 => put_u8(out, self.reason.to_v311_connack()),
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
    use bytes::Bytes;

    fn round_trip_connect(packet: &Connect, version: Version) -> Connect {
        let mut out = Vec::new();
        packet.encode_body(version, &mut out).expect("encodes");
        let mut reader = Reader::new(&out);
        Connect::decode_body(&mut reader, &Limits::default()).expect("decodes")
    }

    fn minimal(version: Version) -> Connect {
        Connect {
            version,
            client_id: "client-1".into(),
            clean_start: true,
            keep_alive: 60,
            username: None,
            password: None,
            will: None,
            properties: Default::default(),
        }
    }

    #[test]
    fn round_trips_a_minimal_v311_connect() {
        let packet = minimal(Version::V311);
        assert_eq!(round_trip_connect(&packet, Version::V311), packet);
    }

    #[test]
    fn round_trips_a_v5_connect_with_credentials_and_a_will() {
        let packet = Connect {
            version: Version::V5,
            client_id: "client-2".into(),
            clean_start: false,
            keep_alive: 30,
            username: Some("alice".into()),
            password: Some(Bytes::from_static(b"secret")),
            will: Some(Will {
                topic: "status/client-2".into(),
                payload: Bytes::from_static(b"offline"),
                qos: QoS::AtLeastOnce,
                retain: true,
                properties: crate::Properties {
                    will_delay_interval: Some(10),
                    ..Default::default()
                },
            }),
            properties: crate::Properties {
                session_expiry_interval: Some(300),
                receive_maximum: Some(32),
                ..Default::default()
            },
        };
        assert_eq!(round_trip_connect(&packet, Version::V5), packet);
    }

    #[test]
    fn rejects_a_wrong_protocol_name() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "MQIsdp").unwrap();
        out.push(4);
        out.push(0x02);
        crate::writer::put_u16(&mut out, 60);
        crate::writer::put_string(&mut out, "c").unwrap();
        let mut reader = Reader::new(&out);
        let err = Connect::decode_body(&mut reader, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_an_unsupported_protocol_level() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "MQTT").unwrap();
        out.push(3);
        out.push(0x02);
        crate::writer::put_u16(&mut out, 60);
        crate::writer::put_string(&mut out, "c").unwrap();
        let mut reader = Reader::new(&out);
        assert_eq!(
            Connect::decode_body(&mut reader, &Limits::default()).unwrap_err(),
            CodecError::UnsupportedVersion(3)
        );
    }

    #[test]
    fn rejects_a_set_reserved_flag_bit() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "MQTT").unwrap();
        out.push(4);
        out.push(0x03); // reserved bit 0 set
        crate::writer::put_u16(&mut out, 60);
        crate::writer::put_string(&mut out, "c").unwrap();
        let mut reader = Reader::new(&out);
        let err = Connect::decode_body(&mut reader, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_will_qos_three() {
        let mut out = Vec::new();
        crate::writer::put_string(&mut out, "MQTT").unwrap();
        out.push(4);
        out.push(0x02 | 0x04 | 0x18); // clean, will flag, will QoS 3
        crate::writer::put_u16(&mut out, 60);
        crate::writer::put_string(&mut out, "c").unwrap();
        let mut reader = Reader::new(&out);
        let err = Connect::decode_body(&mut reader, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_a_client_identifier_longer_than_the_limit() {
        let limits = Limits {
            max_client_id_len: 4,
            ..Limits::default()
        };
        let packet = Connect {
            client_id: "toolong".into(),
            ..minimal(Version::V311)
        };
        let mut out = Vec::new();
        packet.encode_body(Version::V311, &mut out).unwrap();
        let mut reader = Reader::new(&out);
        let err = Connect::decode_body(&mut reader, &limits).unwrap_err();
        assert!(matches!(err, CodecError::ProtocolError(_)));
    }

    #[test]
    fn round_trips_connack_in_both_versions() {
        for (version, reason) in [
            (Version::V311, ReasonCode::Success),
            (Version::V311, ReasonCode::NotAuthorized),
            (Version::V5, ReasonCode::Success),
            (Version::V5, ReasonCode::BadAuthenticationMethod),
        ] {
            let packet = ConnAck {
                session_present: reason == ReasonCode::Success,
                reason,
                properties: Default::default(),
            };
            let mut out = Vec::new();
            packet.encode_body(version, &mut out).unwrap();
            let mut reader = Reader::new(&out);
            let decoded = ConnAck::decode_body(&mut reader, version).unwrap();
            assert_eq!(decoded.session_present, packet.session_present);
            match version {
                Version::V5 => assert_eq!(decoded.reason, reason),
                // The legacy projection is lossy, so only success survives
                // exactly; failures widen to a legacy code.
                Version::V311 => assert_eq!(decoded.reason.is_failure(), reason.is_failure()),
            }
        }
    }
}
