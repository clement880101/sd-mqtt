//! MQTT 3.1.1 and 5.0 packet encoding and decoding.
//!
//! This crate performs no I/O. It turns bytes into typed packets and back,
//! and reports every failure as a value rather than a panic.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod error;
pub mod header;
pub mod reader;
pub mod topic;
pub mod varint;
pub mod writer;

pub use error::CodecError;
pub use header::{FixedHeader, Limits, PacketKind};

/// The MQTT protocol version a packet is encoded for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Version {
    /// MQTT 3.1.1, protocol level 4.
    V311,
    /// MQTT 5.0, protocol level 5.
    V5,
}

impl Version {
    /// The protocol level byte carried in CONNECT.
    pub fn level(self) -> u8 {
        match self {
            Version::V311 => 4,
            Version::V5 => 5,
        }
    }
}
