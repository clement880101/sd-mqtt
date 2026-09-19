//! Packet types and their bodies.

pub mod connect;
pub mod control;
pub mod publish;
pub mod subscribe;

pub use connect::{ConnAck, Connect, Will};
pub use control::{Auth, Disconnect};
pub use publish::{Ack, Publish};
pub use subscribe::{
    RetainHandling, SubAck, Subscribe, SubscriptionOptions, UnsubAck, Unsubscribe,
};

use bytes::{Buf, BytesMut};

use crate::error::CodecError;
use crate::header::{peek, Limits, PacketKind};
use crate::reader::Reader;
use crate::varint;
use crate::Version;

/// Any MQTT control packet.
///
/// The variants differ widely in size: CONNECT carries a client identifier,
/// a will and a property block, while PINGREQ carries nothing. Boxing the
/// large variants would make the common path allocate and would push a
/// `Box::new` onto every construction site, so the enum is held inline and
/// callers move it by value rather than copying it about.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
#[allow(clippy::large_enum_variant)]
pub enum Packet {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(Ack),
    PubRec(Ack),
    PubRel(Ack),
    PubComp(Ack),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    PingReq,
    PingResp,
    Disconnect(Disconnect),
    Auth(Auth),
}

impl Packet {
    /// Which packet type this is.
    pub fn kind(&self) -> PacketKind {
        match self {
            Packet::Connect(_) => PacketKind::Connect,
            Packet::ConnAck(_) => PacketKind::ConnAck,
            Packet::Publish(_) => PacketKind::Publish,
            Packet::PubAck(_) => PacketKind::PubAck,
            Packet::PubRec(_) => PacketKind::PubRec,
            Packet::PubRel(_) => PacketKind::PubRel,
            Packet::PubComp(_) => PacketKind::PubComp,
            Packet::Subscribe(_) => PacketKind::Subscribe,
            Packet::SubAck(_) => PacketKind::SubAck,
            Packet::Unsubscribe(_) => PacketKind::Unsubscribe,
            Packet::UnsubAck(_) => PacketKind::UnsubAck,
            Packet::PingReq => PacketKind::PingReq,
            Packet::PingResp => PacketKind::PingResp,
            Packet::Disconnect(_) => PacketKind::Disconnect,
            Packet::Auth(_) => PacketKind::Auth,
        }
    }

    /// The low nibble of the fixed header.
    pub fn flags(&self) -> u8 {
        match self {
            Packet::Publish(publish) => publish.flags(),
            Packet::PubRel(_) | Packet::Subscribe(_) | Packet::Unsubscribe(_) => 0b0010,
            _ => 0b0000,
        }
    }

    fn encode_body(&self, version: Version, out: &mut Vec<u8>) -> Result<(), CodecError> {
        match self {
            Packet::Connect(p) => p.encode_body(version, out),
            Packet::ConnAck(p) => p.encode_body(version, out),
            Packet::Publish(p) => p.encode_body(version, out),
            Packet::PubAck(p) => p.encode_body(version, PacketKind::PubAck, out),
            Packet::PubRec(p) => p.encode_body(version, PacketKind::PubRec, out),
            Packet::PubRel(p) => p.encode_body(version, PacketKind::PubRel, out),
            Packet::PubComp(p) => p.encode_body(version, PacketKind::PubComp, out),
            Packet::Subscribe(p) => p.encode_body(version, out),
            Packet::SubAck(p) => p.encode_body(version, out),
            Packet::Unsubscribe(p) => p.encode_body(version, out),
            Packet::UnsubAck(p) => p.encode_body(version, out),
            Packet::PingReq | Packet::PingResp => Ok(()),
            Packet::Disconnect(p) => p.encode_body(version, out),
            Packet::Auth(p) => p.encode_body(version, out),
        }
    }
}

/// Decode one packet from the front of `buf`.
///
/// Returns `Ok(None)` when a whole frame has not yet arrived, leaving `buf`
/// untouched so the caller can read more and try again. On success the frame
/// is consumed and anything after it remains.
///
/// `version` is the version negotiated for this connection. CONNECT ignores
/// it and reports the version it carries, so a caller that has not yet seen
/// CONNECT may pass any value.
pub fn decode(
    buf: &mut BytesMut,
    version: Version,
    limits: &Limits,
) -> Result<Option<Packet>, CodecError> {
    let Some(header) = peek(buf, limits)? else {
        return Ok(None);
    };

    let frame = &buf[..header.total_len()];
    let body = &frame[header.header_len..];
    let mut reader = Reader::new(body);

    let packet = match header.kind {
        PacketKind::Connect => Packet::Connect(Connect::decode_body(&mut reader, limits)?),
        PacketKind::ConnAck => Packet::ConnAck(ConnAck::decode_body(&mut reader, version)?),
        PacketKind::Publish => Packet::Publish(Publish::decode_body(
            &mut reader,
            header.flags,
            version,
            limits,
        )?),
        PacketKind::PubAck => {
            Packet::PubAck(Ack::decode_body(&mut reader, version, PacketKind::PubAck)?)
        }
        PacketKind::PubRec => {
            Packet::PubRec(Ack::decode_body(&mut reader, version, PacketKind::PubRec)?)
        }
        PacketKind::PubRel => {
            Packet::PubRel(Ack::decode_body(&mut reader, version, PacketKind::PubRel)?)
        }
        PacketKind::PubComp => {
            Packet::PubComp(Ack::decode_body(&mut reader, version, PacketKind::PubComp)?)
        }
        PacketKind::Subscribe => {
            Packet::Subscribe(Subscribe::decode_body(&mut reader, version, limits)?)
        }
        PacketKind::SubAck => Packet::SubAck(SubAck::decode_body(&mut reader, version)?),
        PacketKind::Unsubscribe => {
            Packet::Unsubscribe(Unsubscribe::decode_body(&mut reader, version, limits)?)
        }
        PacketKind::UnsubAck => Packet::UnsubAck(UnsubAck::decode_body(&mut reader, version)?),
        PacketKind::PingReq => Packet::PingReq,
        PacketKind::PingResp => Packet::PingResp,
        PacketKind::Disconnect => {
            Packet::Disconnect(Disconnect::decode_body(&mut reader, version)?)
        }
        PacketKind::Auth => {
            if version == Version::V311 {
                return Err(CodecError::ProtocolError(
                    "authentication packets do not exist in 3.1.1",
                ));
            }
            Packet::Auth(Auth::decode_body(&mut reader)?)
        }
    };

    if !reader.is_empty() {
        return Err(CodecError::MalformedPacket(
            "trailing bytes after the packet body",
        ));
    }

    buf.advance(header.total_len());
    Ok(Some(packet))
}

/// Append one whole packet, fixed header included, to `out`.
pub fn encode(packet: &Packet, version: Version, out: &mut BytesMut) -> Result<(), CodecError> {
    let mut body = Vec::new();
    packet.encode_body(version, &mut body)?;

    let len = u32::try_from(body.len())
        .map_err(|_| CodecError::MalformedPacket("packet body too large"))?;
    if len > varint::MAX {
        return Err(CodecError::MalformedPacket("packet body too large"));
    }

    let mut header = Vec::with_capacity(5);
    header.push((packet.kind().nibble() << 4) | packet.flags());
    varint::encode(len, &mut header)?;

    out.extend_from_slice(&header);
    out.extend_from_slice(&body);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;
    use crate::header::Limits;
    use crate::qos::QoS;
    use crate::reason::ReasonCode;
    use crate::Version;
    use bytes::{Bytes, BytesMut};

    fn publish() -> Packet {
        Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "a/b".into(),
            packet_id: Some(1),
            payload: Bytes::from_static(b"hello"),
            properties: Default::default(),
        })
    }

    #[test]
    fn round_trips_through_the_public_entry_points() {
        let original = publish();
        let mut buf = BytesMut::new();
        encode(&original, Version::V311, &mut buf).unwrap();

        let decoded = decode(&mut buf, Version::V311, &Limits::default())
            .unwrap()
            .expect("a whole frame");
        assert_eq!(decoded, original);
        assert!(buf.is_empty(), "the frame should have been consumed");
    }

    #[test]
    fn decodes_one_frame_and_leaves_the_next_alone() {
        let mut buf = BytesMut::new();
        encode(&publish(), Version::V311, &mut buf).unwrap();
        encode(&Packet::PingReq, Version::V311, &mut buf).unwrap();

        let first = decode(&mut buf, Version::V311, &Limits::default())
            .unwrap()
            .expect("first frame");
        assert!(matches!(first, Packet::Publish(_)));

        let second = decode(&mut buf, Version::V311, &Limits::default())
            .unwrap()
            .expect("second frame");
        assert_eq!(second, Packet::PingReq);
        assert!(buf.is_empty());
    }

    #[test]
    fn reports_need_more_bytes_for_every_prefix_of_a_frame() {
        let mut whole = BytesMut::new();
        encode(&publish(), Version::V311, &mut whole).unwrap();

        for take in 0..whole.len() {
            let mut partial = BytesMut::from(&whole[..take]);
            let outcome = decode(&mut partial, Version::V311, &Limits::default()).unwrap();
            assert!(outcome.is_none(), "should need more bytes at {take}");
            assert_eq!(partial.len(), take, "a partial frame must not be consumed");
        }
    }

    #[test]
    fn rejects_trailing_bytes_inside_a_declared_frame() {
        // A PINGREQ declaring a one-byte body, which PINGREQ never has.
        let mut buf = BytesMut::from(&[0xC0u8, 0x01, 0xFF][..]);
        let err = decode(&mut buf, Version::V311, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_auth_on_a_3_1_1_connection() {
        let mut buf = BytesMut::new();
        encode(
            &Packet::Auth(Auth {
                reason: ReasonCode::Success,
                properties: Default::default(),
            }),
            Version::V5,
            &mut buf,
        )
        .unwrap();
        let err = decode(&mut buf, Version::V311, &Limits::default()).unwrap_err();
        assert!(matches!(err, CodecError::ProtocolError(_)));
    }

    #[test]
    fn connect_is_decoded_without_knowing_the_version_first() {
        let original = Packet::Connect(Connect {
            version: Version::V5,
            client_id: "c".into(),
            clean_start: true,
            keep_alive: 10,
            username: None,
            password: None,
            will: None,
            properties: Default::default(),
        });
        let mut buf = BytesMut::new();
        encode(&original, Version::V5, &mut buf).unwrap();

        // The caller does not yet know the version, so it passes 3.1.1.
        let decoded = decode(&mut buf, Version::V311, &Limits::default())
            .unwrap()
            .expect("a whole frame");
        match decoded {
            Packet::Connect(connect) => assert_eq!(connect.version, Version::V5),
            other => panic!("expected a connect, got {other:?}"),
        }
    }

    #[test]
    fn rejects_an_oversized_frame_without_buffering_it() {
        let limits = Limits {
            max_packet_size: 16,
            ..Limits::default()
        };
        // Declares a 300-byte body; only the header is present.
        let mut buf = BytesMut::from(&[0x30u8, 0xAC, 0x02][..]);
        assert_eq!(
            decode(&mut buf, Version::V311, &limits).unwrap_err(),
            CodecError::PacketTooLarge
        );
    }
}
