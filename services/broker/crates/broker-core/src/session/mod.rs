//! Session state machines.
//!
//! Everything in this module is pure. It performs no I/O, holds no lock and
//! never awaits: inputs go in, a list of actions comes out, and the caller
//! performs them. That is what makes the quality-of-service handshakes
//! testable by driving millions of generated sequences through them.

pub mod message;

pub use message::{Message, MessageId};
