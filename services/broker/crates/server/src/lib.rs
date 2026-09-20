//! Putting the broker on a socket.
//!
//! This layer is deliberately thin. Everything hard lives in
//! `broker-core` and is tested without any I/O at all; here we read
//! bytes, hand them over, and write back what comes out. A protocol
//! decision made in this crate is a decision in the wrong place.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod connection;
pub mod framing;
pub mod listener;
pub mod shared;
pub mod ticker;
