//! Session state machines.
//!
//! Everything in this module is pure. It performs no I/O, holds no lock and
//! never awaits: inputs go in, a list of actions comes out, and the caller
//! performs them. That is what makes the quality-of-service handshakes
//! testable by driving millions of generated sequences through them.

pub mod inflight;
pub mod message;
pub mod packet_ids;
pub mod queue;

pub use inflight::{Inflight, InflightEntry, InflightState};
pub use message::{Message, MessageId};
pub use packet_ids::PacketIds;
pub use queue::{Delivery, DropReason, Overflow, Queue, QueueLimits};
