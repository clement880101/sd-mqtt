//! A bounds-checked cursor over a single frame body.

use bytes::Bytes;

use crate::error::CodecError;
use crate::varint;

/// A cursor over a frame body that cannot read past its end.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    /// Wrap a frame body.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// How many bytes are left.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Whether every byte has been consumed.
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn need(&self, n: usize) -> Result<(), CodecError> {
        if self.remaining() < n {
            Err(CodecError::MalformedPacket("packet body ended early"))
        } else {
            Ok(())
        }
    }

    /// Read one byte.
    pub fn u8(&mut self) -> Result<u8, CodecError> {
        self.need(1)?;
        let value = self.buf[self.pos];
        self.pos += 1;
        Ok(value)
    }

    /// Read a two-byte big-endian integer.
    pub fn u16(&mut self) -> Result<u16, CodecError> {
        self.need(2)?;
        let value = u16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(value)
    }

    /// Read a four-byte big-endian integer.
    pub fn u32(&mut self) -> Result<u32, CodecError> {
        self.need(4)?;
        let value = u32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(value)
    }

    /// Read a variable byte integer.
    pub fn varint(&mut self) -> Result<u32, CodecError> {
        match varint::decode(&self.buf[self.pos..])? {
            Some((value, consumed)) => {
                self.pos += consumed;
                Ok(value)
            }
            None => Err(CodecError::MalformedPacket(
                "variable byte integer truncated",
            )),
        }
    }

    /// Read exactly `n` bytes.
    pub fn slice(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        self.need(n)?;
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    /// Read a length-prefixed UTF-8 string.
    ///
    /// The protocol forbids a null character inside such a string, so that
    /// check lives here rather than at every call site.
    pub fn string(&mut self) -> Result<String, CodecError> {
        let len = self.u16()? as usize;
        let raw = self.slice(len)?;
        let text = core::str::from_utf8(raw).map_err(|_| CodecError::InvalidUtf8)?;
        if text.contains('\0') {
            return Err(CodecError::InvalidUtf8);
        }
        Ok(text.to_owned())
    }

    /// Read a length-prefixed byte string.
    pub fn binary(&mut self) -> Result<Bytes, CodecError> {
        let len = self.u16()? as usize;
        Ok(Bytes::copy_from_slice(self.slice(len)?))
    }

    /// Consume and return everything that is left.
    pub fn take_rest(&mut self) -> Bytes {
        let out = Bytes::copy_from_slice(&self.buf[self.pos..]);
        self.pos = self.buf.len();
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CodecError;

    #[test]
    fn reads_integers_in_network_order() {
        let mut r = Reader::new(&[0x01, 0x02, 0x03, 0x00, 0x00, 0x04, 0xD2]);
        assert_eq!(r.u8().unwrap(), 0x01);
        assert_eq!(r.u16().unwrap(), 0x0203);
        assert_eq!(r.u32().unwrap(), 1234);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn reads_a_utf8_string() {
        let mut r = Reader::new(&[0x00, 0x04, b'M', b'Q', b'T', b'T']);
        assert_eq!(r.string().unwrap(), "MQTT");
    }

    #[test]
    fn reads_an_empty_string() {
        let mut r = Reader::new(&[0x00, 0x00]);
        assert_eq!(r.string().unwrap(), "");
    }

    #[test]
    fn rejects_a_string_containing_a_null_character() {
        let mut r = Reader::new(&[0x00, 0x02, b'a', 0x00]);
        assert_eq!(r.string().unwrap_err(), CodecError::InvalidUtf8);
    }

    #[test]
    fn rejects_invalid_utf8() {
        let mut r = Reader::new(&[0x00, 0x02, 0xFF, 0xFE]);
        assert_eq!(r.string().unwrap_err(), CodecError::InvalidUtf8);
    }

    #[test]
    fn reads_binary_data() {
        let mut r = Reader::new(&[0x00, 0x03, 0xDE, 0xAD, 0xBE]);
        assert_eq!(&r.binary().unwrap()[..], &[0xDE, 0xAD, 0xBE]);
    }

    #[test]
    fn reports_truncation_as_malformed_not_a_panic() {
        assert!(Reader::new(&[]).u8().is_err());
        assert!(Reader::new(&[0x00]).u16().is_err());
        assert!(Reader::new(&[0x00, 0x00, 0x00]).u32().is_err());
        assert!(Reader::new(&[0x00, 0x05, b'a']).string().is_err());
        assert!(Reader::new(&[0x00, 0x05, b'a']).binary().is_err());
    }

    #[test]
    fn reads_a_varint_through_the_cursor() {
        let mut r = Reader::new(&[0x80, 0x01, 0xAA]);
        assert_eq!(r.varint().unwrap(), 128);
        assert_eq!(r.u8().unwrap(), 0xAA);
    }

    #[test]
    fn take_rest_consumes_everything_left() {
        let mut r = Reader::new(&[0x01, 0x02, 0x03]);
        assert_eq!(r.u8().unwrap(), 0x01);
        assert_eq!(&r.take_rest()[..], &[0x02, 0x03]);
        assert_eq!(r.remaining(), 0);
    }
}
