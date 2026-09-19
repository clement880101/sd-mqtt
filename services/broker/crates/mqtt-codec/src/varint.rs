//! Variable byte integers, MQTT's length encoding.

use crate::error::CodecError;

/// The largest value a variable byte integer can carry.
pub const MAX: u32 = 268_435_455;

/// Decode a variable byte integer from the front of `buf`.
///
/// Returns `Ok(None)` when `buf` is a prefix of a valid encoding and more
/// bytes are needed. Returns the value and the number of bytes consumed on
/// success.
pub fn decode(buf: &[u8]) -> Result<Option<(u32, usize)>, CodecError> {
    let mut value: u32 = 0;
    let mut multiplier: u32 = 1;

    for (index, &byte) in buf.iter().take(4).enumerate() {
        value += u32::from(byte & 0x7F) * multiplier;
        if byte & 0x80 == 0 {
            if byte == 0 && index != 0 {
                return Err(CodecError::MalformedPacket(
                    "variable byte integer is not minimally encoded",
                ));
            }
            return Ok(Some((value, index + 1)));
        }
        multiplier *= 128;
    }

    if buf.len() >= 4 {
        Err(CodecError::MalformedPacket(
            "variable byte integer longer than four bytes",
        ))
    } else {
        Ok(None)
    }
}

/// Append the encoding of `value` to `out`.
pub fn encode(mut value: u32, out: &mut Vec<u8>) -> Result<(), CodecError> {
    if value > MAX {
        return Err(CodecError::MalformedPacket(
            "variable byte integer above the maximum",
        ));
    }
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return Ok(());
        }
    }
}

/// How many bytes `value` occupies once encoded.
///
/// Values above [`MAX`] report four, which is the largest legal width; the
/// caller is expected to reject them before encoding.
pub fn encoded_len(value: u32) -> usize {
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Boundary values from the protocol's encoding table.
    const CASES: &[(u32, &[u8])] = &[
        (0, &[0x00]),
        (127, &[0x7F]),
        (128, &[0x80, 0x01]),
        (16_383, &[0xFF, 0x7F]),
        (16_384, &[0x80, 0x80, 0x01]),
        (2_097_151, &[0xFF, 0xFF, 0x7F]),
        (2_097_152, &[0x80, 0x80, 0x80, 0x01]),
        (268_435_455, &[0xFF, 0xFF, 0xFF, 0x7F]),
    ];

    #[test]
    fn decodes_boundary_values() {
        for (value, bytes) in CASES {
            let got = decode(bytes).expect("well formed");
            assert_eq!(got, Some((*value, bytes.len())), "decoding {value}");
        }
    }

    #[test]
    fn encodes_boundary_values() {
        for (value, bytes) in CASES {
            let mut out = Vec::new();
            encode(*value, &mut out).expect("in range");
            assert_eq!(out.as_slice(), *bytes, "encoding {value}");
        }
    }

    #[test]
    fn reports_need_more_bytes_on_truncation() {
        assert_eq!(decode(&[]).expect("not malformed"), None);
        assert_eq!(decode(&[0x80]).expect("not malformed"), None);
        assert_eq!(decode(&[0x80, 0x80, 0x80]).expect("not malformed"), None);
    }

    #[test]
    fn rejects_a_fifth_continuation_byte() {
        let err = decode(&[0x80, 0x80, 0x80, 0x80]).expect_err("malformed");
        assert!(matches!(err, crate::error::CodecError::MalformedPacket(_)));
    }

    #[test]
    fn rejects_values_above_the_maximum() {
        let mut out = Vec::new();
        let err = encode(268_435_456, &mut out).expect_err("out of range");
        assert!(matches!(err, crate::error::CodecError::MalformedPacket(_)));
    }

    #[test]
    fn encoded_len_matches_encode() {
        for (value, bytes) in CASES {
            assert_eq!(encoded_len(*value), bytes.len());
        }
    }

    #[test]
    fn rejects_non_minimal_encodings() {
        // 1 spelled in two bytes, 0 spelled in two, and a longer padding.
        for bytes in [
            &[0x81u8, 0x00][..],
            &[0x80, 0x00][..],
            &[0x81, 0x80, 0x00][..],
        ] {
            let err = decode(bytes).expect_err("non-minimal");
            assert!(
                matches!(err, crate::error::CodecError::MalformedPacket(_)),
                "{bytes:?}"
            );
        }
    }

    #[test]
    fn still_accepts_a_single_zero_byte() {
        assert_eq!(decode(&[0x00]).unwrap(), Some((0, 1)));
    }
}
