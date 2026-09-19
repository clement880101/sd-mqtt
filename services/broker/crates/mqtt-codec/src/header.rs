//! The two-part fixed header, and deciding when a whole frame has arrived.

use crate::error::CodecError;
use crate::varint;

/// Bounds the decoder enforces, supplied by configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest whole packet, header included, in bytes.
    pub max_packet_size: u32,
    /// Largest number of levels in a topic name or filter.
    pub max_topic_depth: u16,
    /// Largest client identifier length in bytes.
    pub max_client_id_len: u16,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_packet_size: 2_000_000,
            max_topic_depth: 16,
            max_client_id_len: 128,
        }
    }
}

/// The packet type carried in the high nibble of the first byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum PacketKind {
    Connect,
    ConnAck,
    Publish,
    PubAck,
    PubRec,
    PubRel,
    PubComp,
    Subscribe,
    SubAck,
    Unsubscribe,
    UnsubAck,
    PingReq,
    PingResp,
    Disconnect,
    Auth,
}

impl PacketKind {
    fn from_nibble(value: u8) -> Option<Self> {
        Some(match value {
            1 => Self::Connect,
            2 => Self::ConnAck,
            3 => Self::Publish,
            4 => Self::PubAck,
            5 => Self::PubRec,
            6 => Self::PubRel,
            7 => Self::PubComp,
            8 => Self::Subscribe,
            9 => Self::SubAck,
            10 => Self::Unsubscribe,
            11 => Self::UnsubAck,
            12 => Self::PingReq,
            13 => Self::PingResp,
            14 => Self::Disconnect,
            15 => Self::Auth,
            _ => return None,
        })
    }

    /// The high nibble this kind occupies.
    pub fn nibble(self) -> u8 {
        match self {
            Self::Connect => 1,
            Self::ConnAck => 2,
            Self::Publish => 3,
            Self::PubAck => 4,
            Self::PubRec => 5,
            Self::PubRel => 6,
            Self::PubComp => 7,
            Self::Subscribe => 8,
            Self::SubAck => 9,
            Self::Unsubscribe => 10,
            Self::UnsubAck => 11,
            Self::PingReq => 12,
            Self::PingResp => 13,
            Self::Disconnect => 14,
            Self::Auth => 15,
        }
    }

    /// The flags the low nibble must carry, or `None` when the packet defines
    /// its own flags. Only PUBLISH defines its own.
    fn required_flags(self) -> Option<u8> {
        match self {
            Self::Publish => None,
            Self::PubRel | Self::Subscribe | Self::Unsubscribe => Some(0b0010),
            _ => Some(0b0000),
        }
    }
}

/// A decoded fixed header, describing a frame that has fully arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedHeader {
    /// Which packet this is.
    pub kind: PacketKind,
    /// The low nibble of the first byte, meaningful only for PUBLISH.
    pub flags: u8,
    /// Bytes of fixed header, between one and five.
    pub header_len: usize,
    /// Bytes of body following the header.
    pub body_len: usize,
}

impl FixedHeader {
    /// Header plus body.
    pub fn total_len(&self) -> usize {
        self.header_len + self.body_len
    }
}

/// Inspect the front of `buf` without consuming it.
///
/// Returns `Ok(None)` when the frame has not fully arrived. Returns
/// [`CodecError::PacketTooLarge`] as soon as the declared length exceeds the
/// limit, which happens before the body is present, so an oversized frame
/// costs no memory.
pub fn peek(buf: &[u8], limits: &Limits) -> Result<Option<FixedHeader>, CodecError> {
    if buf.is_empty() {
        return Ok(None);
    }

    let first = buf[0];
    let kind = PacketKind::from_nibble(first >> 4)
        .ok_or(CodecError::MalformedPacket("unknown packet type"))?;
    let flags = first & 0x0F;

    if let Some(required) = kind.required_flags() {
        if flags != required {
            return Err(CodecError::MalformedPacket(
                "reserved flags in the fixed header are wrong",
            ));
        }
    }

    let Some((body_len, len_bytes)) = varint::decode(&buf[1..])? else {
        return Ok(None);
    };
    let header_len = 1 + len_bytes;

    // Check the declared size before deciding whether the body has arrived,
    // so an oversized declaration is rejected without waiting for bytes.
    let declared_total = u64::from(body_len) + header_len as u64;
    if declared_total > u64::from(limits.max_packet_size) {
        return Err(CodecError::PacketTooLarge);
    }

    if buf.len() < header_len + body_len as usize {
        return Ok(None);
    }

    Ok(Some(FixedHeader {
        kind,
        flags,
        header_len,
        body_len: body_len as usize,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;

    fn limits(max: u32) -> Limits {
        Limits {
            max_packet_size: max,
            ..Limits::default()
        }
    }

    #[test]
    fn reads_a_complete_small_frame() {
        // PINGREQ: type 12, no flags, zero-length body.
        let buf = [0xC0, 0x00];
        let header = peek(&buf, &limits(1024)).unwrap().expect("complete");
        assert_eq!(header.kind, PacketKind::PingReq);
        assert_eq!(header.flags, 0);
        assert_eq!(header.body_len, 0);
        assert_eq!(header.total_len(), 2);
    }

    #[test]
    fn reports_need_more_bytes_when_the_header_is_partial() {
        assert!(peek(&[], &limits(1024)).unwrap().is_none());
        assert!(peek(&[0xC0], &limits(1024)).unwrap().is_none());
        // Length prefix says 2 more bytes follow but none have arrived.
        assert!(peek(&[0x30, 0x02], &limits(1024)).unwrap().is_none());
    }

    #[test]
    fn rejects_an_oversized_frame_from_its_declared_length_alone() {
        // Declared body of 300 bytes against a 100-byte limit, with no body
        // present at all. The rejection must not wait for the body.
        let buf = [0x30, 0xAC, 0x02];
        let err = peek(&buf, &limits(100)).unwrap_err();
        assert_eq!(err, CodecError::PacketTooLarge);
    }

    #[test]
    fn accepts_a_frame_exactly_at_the_limit() {
        // A two-byte header plus a six-byte body is exactly eight.
        let mut buf = vec![0x30, 0x06];
        buf.extend(std::iter::repeat_n(0u8, 6));
        let header = peek(&buf, &limits(8)).unwrap().expect("complete");
        assert_eq!(header.total_len(), 8);

        // One byte over the limit is rejected.
        let mut buf = vec![0x30, 0x07];
        buf.extend(std::iter::repeat_n(0u8, 7));
        assert_eq!(
            peek(&buf, &limits(8)).unwrap_err(),
            CodecError::PacketTooLarge
        );
    }

    #[test]
    fn rejects_the_one_unassigned_packet_type() {
        // Nibble 0 is the only unassigned value; 1 through 15 are all real
        // packet types, with 15 being AUTH.
        let err = peek(&[0x00, 0x00], &limits(1024)).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
        assert!(peek(&[0xF0, 0x00], &limits(1024)).unwrap().is_some());
    }

    #[test]
    fn rejects_wrong_reserved_flags() {
        // PUBREL, SUBSCRIBE, UNSUBSCRIBE must carry flags 0b0010.
        for first in [0x60u8, 0x80, 0xA0] {
            let err = peek(&[first, 0x00], &limits(1024)).unwrap_err();
            assert!(
                matches!(err, CodecError::MalformedPacket(_)),
                "{first:#04x}"
            );
        }
        // CONNECT must carry flags 0b0000.
        let err = peek(&[0x11, 0x00], &limits(1024)).unwrap_err();
        assert!(matches!(err, CodecError::MalformedPacket(_)));
    }

    #[test]
    fn accepts_correct_reserved_flags() {
        assert!(peek(&[0x62, 0x00], &limits(1024)).unwrap().is_some());
        assert!(peek(&[0x82, 0x00], &limits(1024)).unwrap().is_some());
        assert!(peek(&[0xA2, 0x00], &limits(1024)).unwrap().is_some());
    }

    #[test]
    fn publish_flags_are_carried_through_not_rejected() {
        // PUBLISH with DUP set, QoS 2, retain set.
        let header = peek(&[0x3D, 0x00], &limits(1024))
            .unwrap()
            .expect("complete");
        assert_eq!(header.kind, PacketKind::Publish);
        assert_eq!(header.flags, 0x0D);
    }

    #[test]
    fn total_len_accounts_for_a_multibyte_length_prefix() {
        // Body of 128 bytes needs a two-byte length prefix.
        let mut buf = vec![0x30, 0x80, 0x01];
        buf.extend(std::iter::repeat_n(0u8, 128));
        let header = peek(&buf, &limits(1024)).unwrap().expect("complete");
        assert_eq!(header.body_len, 128);
        assert_eq!(header.header_len, 3);
        assert_eq!(header.total_len(), 131);
    }
}
