//! Errors produced while encoding or decoding.

use thiserror::Error;

/// Everything that can go wrong on the wire.
///
/// Every variant carries a static description so that logs can identify the
/// exact check that failed without allocating.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CodecError {
    /// The bytes do not form a valid packet.
    #[error("malformed packet: {0}")]
    MalformedPacket(&'static str),

    /// The packet is well formed but violates a protocol rule.
    #[error("protocol error: {0}")]
    ProtocolError(&'static str),

    /// The declared size exceeds the configured maximum.
    #[error("packet too large")]
    PacketTooLarge,

    /// The CONNECT packet named a protocol level this broker does not serve.
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    /// A string field was not valid UTF-8, or contained a null character.
    #[error("invalid UTF-8 string")]
    InvalidUtf8,

    /// A publish topic was empty, or contained a wildcard.
    #[error("invalid topic name")]
    InvalidTopicName,

    /// A subscription filter violated the wildcard placement rules.
    #[error("invalid topic filter")]
    InvalidTopicFilter,

    /// The topic had more levels than the configured maximum.
    #[error("topic too deep")]
    TopicTooDeep,
}
