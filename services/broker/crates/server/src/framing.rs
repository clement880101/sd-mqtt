//! Turning a byte stream into packets.

use bytes::BytesMut;
use mqtt_codec::{decode, CodecError, Limits, Packet, Version};
use prometheus_client::metrics::counter::Counter;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};

/// Why reading a packet failed.
#[derive(Debug, Error)]
pub enum FrameError {
    /// The peer sent something that is not a valid packet.
    #[error("protocol error: {0}")]
    Protocol(#[from] CodecError),
    /// The connection failed, or ended partway through a packet.
    #[error("connection error: {0}")]
    Io(#[from] std::io::Error),
}

/// How much to read from the socket at once.
///
/// Large enough that a burst of small packets costs one syscall, small
/// enough that a hundred thousand idle connections do not each hold a
/// page they are not using.
const READ_CHUNK: usize = 4096;

/// Reads whole packets from a byte stream.
///
/// The codec does the hard part: it consumes nothing when a frame is
/// incomplete, and rejects an oversized one from its declared length
/// before any body arrives. This loop must not defeat either property by
/// buffering first and asking later.
pub struct FrameReader<R> {
    inner: R,
    buffer: BytesMut,
    limits: Limits,
    /// Bytes are counted here, where the socket hands them over.
    ///
    /// Not by adding up what the packets that came out should have
    /// weighed: the whole use of a byte counter is that the two can
    /// differ, by a half-received frame, by bytes read and then
    /// rejected, or by a framing bug. An owned handle, so the
    /// increment is an atomic add and not a lock; see
    /// [`crate::metrics`].
    bytes: Counter,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    /// A reader over `inner`, counting what it reads into `bytes`.
    pub fn new(inner: R, limits: Limits, bytes: Counter) -> Self {
        Self {
            inner,
            buffer: BytesMut::with_capacity(READ_CHUNK),
            limits,
            bytes,
        }
    }

    /// The next packet, or nothing when the peer closed cleanly.
    ///
    /// A close partway through a packet is an error rather than an end:
    /// reporting a truncated stream as a clean one would hide a fault.
    pub async fn next(&mut self, version: Version) -> Result<Option<Packet>, FrameError> {
        loop {
            // Try the buffer first, so several packets in one read are
            // all returned without touching the socket again.
            if let Some(packet) = decode(&mut self.buffer, version, &self.limits)? {
                return Ok(Some(packet));
            }

            self.buffer.reserve(READ_CHUNK);
            let read = self.inner.read_buf(&mut self.buffer).await?;
            // Counted before anything is decided about it. A frame
            // that turns out to be malformed still cost the bytes it
            // arrived in, and a counter that only counts what parsed
            // cannot be compared against what the interface saw.
            self.bytes.inc_by(read as u64);
            if read == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                return Err(FrameError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "the connection ended partway through a packet",
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use mqtt_codec::{encode, Limits, Packet, Version};
    use tokio::io::duplex;

    fn ping() -> Packet {
        Packet::PingReq
    }

    #[tokio::test]
    async fn reads_a_whole_packet() {
        let (mut client, server) = duplex(1024);
        let mut buf = BytesMut::new();
        encode(&ping(), Version::V311, &mut buf).unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut client, &buf)
            .await
            .unwrap();

        let mut reader = FrameReader::new(server, Limits::default(), Counter::default());
        let packet = reader.next(Version::V311).await.unwrap().expect("a packet");
        assert_eq!(packet, Packet::PingReq);
    }

    #[tokio::test]
    async fn reassembles_a_packet_split_across_writes() {
        // The whole reason this loop exists.
        let (mut client, server) = duplex(1024);
        let mut whole = BytesMut::new();
        for _ in 0..3 {
            encode(&ping(), Version::V311, &mut whole).unwrap();
        }

        let mut reader = FrameReader::new(server, Limits::default(), Counter::default());
        let handle = tokio::spawn(async move {
            for byte in whole.iter().copied() {
                tokio::io::AsyncWriteExt::write_all(&mut client, &[byte])
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        });

        for _ in 0..3 {
            assert_eq!(
                reader.next(Version::V311).await.unwrap(),
                Some(Packet::PingReq)
            );
        }
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn a_clean_close_ends_the_stream() {
        let (client, server) = duplex(1024);
        drop(client);

        let mut reader = FrameReader::new(server, Limits::default(), Counter::default());
        assert_eq!(reader.next(Version::V311).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_close_midway_through_a_packet_is_an_error_not_an_end() {
        // Half a packet then silence is a truncated stream, and reporting
        // it as a clean end would hide a fault.
        let (mut client, server) = duplex(1024);
        tokio::io::AsyncWriteExt::write_all(&mut client, &[0x30, 0x10])
            .await
            .unwrap();
        drop(client);

        let mut reader = FrameReader::new(server, Limits::default(), Counter::default());
        assert!(reader.next(Version::V311).await.is_err());
    }

    #[tokio::test]
    async fn a_malformed_packet_is_reported_with_its_reason() {
        let (mut client, server) = duplex(1024);
        // Packet type zero is the one unassigned value.
        tokio::io::AsyncWriteExt::write_all(&mut client, &[0x00, 0x00])
            .await
            .unwrap();

        let mut reader = FrameReader::new(server, Limits::default(), Counter::default());
        let error = reader.next(Version::V311).await.unwrap_err();
        assert!(matches!(error, FrameError::Protocol(_)));
    }

    #[tokio::test]
    async fn an_oversized_frame_is_refused_before_its_body_arrives() {
        // The codec rejects from the declared length alone. Confirm the
        // loop does not defeat that by waiting for bytes first.
        let limits = Limits {
            max_packet_size: 64,
            ..Limits::default()
        };
        let (mut client, server) = duplex(1024);
        // Declares a body of 300 bytes and sends none of it.
        tokio::io::AsyncWriteExt::write_all(&mut client, &[0x30, 0xAC, 0x02])
            .await
            .unwrap();

        let mut reader = FrameReader::new(server, limits, Counter::default());
        let error = reader.next(Version::V311).await.unwrap_err();
        assert!(matches!(error, FrameError::Protocol(_)));
    }

    #[tokio::test]
    async fn every_byte_the_socket_hands_over_is_counted() {
        // The exact number, and read off the socket rather than off
        // the packets: a counter that added up what the decoded
        // packets should have weighed would agree with a
        // greater-than-zero assertion and disagree with this one the
        // moment a byte arrives that does not decode.
        let (mut client, server) = duplex(1024);
        let mut whole = BytesMut::new();
        for _ in 0..4 {
            encode(&ping(), Version::V311, &mut whole).unwrap();
        }
        // A fifth packet's first byte and nothing more, so the count
        // is one greater than the four whole packets weigh.
        whole.extend_from_slice(&[0xC0]);
        let expected = whole.len() as u64;
        tokio::io::AsyncWriteExt::write_all(&mut client, &whole)
            .await
            .unwrap();

        let bytes = Counter::default();
        let mut reader = FrameReader::new(server, Limits::default(), bytes.clone());
        for _ in 0..4 {
            assert_eq!(
                reader.next(Version::V311).await.unwrap(),
                Some(Packet::PingReq)
            );
        }
        assert_eq!(
            bytes.get(),
            expected,
            "the half packet's byte was read and must be counted"
        );
    }

    #[tokio::test]
    async fn several_packets_in_one_write_are_all_read() {
        let (mut client, server) = duplex(1024);
        let mut buf = BytesMut::new();
        for _ in 0..5 {
            encode(&ping(), Version::V311, &mut buf).unwrap();
        }
        tokio::io::AsyncWriteExt::write_all(&mut client, &buf)
            .await
            .unwrap();
        drop(client);

        let mut reader = FrameReader::new(server, Limits::default(), Counter::default());
        for _ in 0..5 {
            assert_eq!(
                reader.next(Version::V311).await.unwrap(),
                Some(Packet::PingReq)
            );
        }
        assert_eq!(reader.next(Version::V311).await.unwrap(), None);
    }
}
