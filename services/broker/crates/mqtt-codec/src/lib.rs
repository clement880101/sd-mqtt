//! MQTT 3.1.1 and 5.0 packet encoding and decoding.
//!
//! This crate performs no I/O. It turns bytes into typed packets and back,
//! and reports every failure as a value rather than a panic.
//!
//! # The read loop
//!
//! A connection handler owns a buffer, appends whatever the socket gives
//! it, and asks [`decode`] for a packet. [`decode`] answers `Ok(None)`
//! while a frame is still incomplete and leaves the buffer untouched, so
//! the next read appends rather than reassembling anything. One read can
//! also complete more than one frame, so the loop drains until it is told
//! to wait.
//!
//! The protocol version is not known until CONNECT has been decoded.
//! CONNECT ignores the version passed to it and reports the one it carries,
//! so the loop can start on either and switch once.
//!
//! ```
//! use bytes::BytesMut;
//! use mqtt_codec::packet::Connect;
//! use mqtt_codec::{decode, encode, Limits, Packet, Version};
//!
//! # fn main() -> Result<(), mqtt_codec::CodecError> {
//! let limits = Limits::default();
//!
//! // Stands in for the socket: a CONNECT announcing MQTT 5, then a
//! // PINGREQ, arriving in four-byte pieces that fall on no frame boundary.
//! let mut wire = BytesMut::new();
//! encode(
//!     &Packet::Connect(Box::new(Connect {
//!         version: Version::V5,
//!         client_id: "sensor-7".into(),
//!         clean_start: true,
//!         keep_alive: 60,
//!         username: None,
//!         password: None,
//!         will: None,
//!         properties: Default::default(),
//!     })),
//!     Version::V5,
//!     &mut wire,
//! )?;
//! encode(&Packet::PingReq, Version::V5, &mut wire)?;
//!
//! let mut version = Version::V311;
//! let mut buf = BytesMut::new();
//! let mut received = Vec::new();
//!
//! for chunk in wire.chunks(4) {
//!     buf.extend_from_slice(chunk);
//!
//!     // Drain every whole frame this read completed. The loop ends on
//!     // `Ok(None)`, which means the bytes left in `buf` are the start of
//!     // a frame that has not finished arriving; none of them are lost.
//!     while let Some(packet) = decode(&mut buf, version, &limits)? {
//!         if let Packet::Connect(connect) = &packet {
//!             version = connect.version;
//!         }
//!         received.push(packet);
//!     }
//! }
//!
//! assert_eq!(version, Version::V5);
//! assert_eq!(received.len(), 2);
//! assert!(matches!(received[0], Packet::Connect(_)));
//! assert_eq!(received[1], Packet::PingReq);
//! assert!(buf.is_empty(), "both frames were consumed whole");
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod error;
pub mod header;
pub mod packet;
pub mod properties;
pub mod qos;
pub mod reader;
pub mod reason;
pub mod topic;
pub mod varint;
pub(crate) mod writer;

pub use error::CodecError;
pub use header::{peek, FixedHeader, Limits, PacketKind};
pub use packet::{decode, encode, Packet};
pub use properties::{Properties, PropertyScope};
pub use qos::QoS;
pub use reason::ReasonCode;

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
