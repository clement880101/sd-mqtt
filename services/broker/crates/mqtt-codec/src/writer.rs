//! Primitive encoders that append to a growable buffer.

use crate::error::CodecError;

/// Append one byte.
pub fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

/// Append a two-byte big-endian integer.
pub fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Append a four-byte big-endian integer.
pub fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

/// Append a length-prefixed UTF-8 string.
pub fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), CodecError> {
    put_binary(out, value.as_bytes())
}

/// Append a length-prefixed byte string.
pub fn put_binary(out: &mut Vec<u8>, value: &[u8]) -> Result<(), CodecError> {
    let len = u16::try_from(value.len())
        .map_err(|_| CodecError::MalformedPacket("length-prefixed field above 65535 bytes"))?;
    put_u16(out, len);
    out.extend_from_slice(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_integers_in_network_order() {
        let mut out = Vec::new();
        put_u8(&mut out, 0x01);
        put_u16(&mut out, 0x0203);
        put_u32(&mut out, 1234);
        assert_eq!(out, vec![0x01, 0x02, 0x03, 0x00, 0x00, 0x04, 0xD2]);
    }

    #[test]
    fn writes_a_length_prefixed_string() {
        let mut out = Vec::new();
        put_string(&mut out, "MQTT").unwrap();
        assert_eq!(out, vec![0x00, 0x04, b'M', b'Q', b'T', b'T']);
    }

    #[test]
    fn writes_an_empty_string() {
        let mut out = Vec::new();
        put_string(&mut out, "").unwrap();
        assert_eq!(out, vec![0x00, 0x00]);
    }

    #[test]
    fn rejects_a_string_longer_than_the_length_prefix_allows() {
        let long = "a".repeat(65_536);
        let mut out = Vec::new();
        assert!(put_string(&mut out, &long).is_err());
    }

    #[test]
    fn writes_length_prefixed_binary() {
        let mut out = Vec::new();
        put_binary(&mut out, &[0xDE, 0xAD]).unwrap();
        assert_eq!(out, vec![0x00, 0x02, 0xDE, 0xAD]);
    }
}
