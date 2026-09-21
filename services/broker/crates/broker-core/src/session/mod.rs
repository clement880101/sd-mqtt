//! Session state machines.
//!
//! Everything in this module is pure. It performs no I/O, holds no lock and
//! never awaits: inputs go in, a list of actions comes out, and the caller
//! performs them. That is what makes the quality-of-service handshakes
//! testable by driving millions of generated sequences through them.

use std::sync::Arc;

use mqtt_codec::{Packet, QoS, ReasonCode};

pub mod inbound;
pub mod inflight;
pub mod message;
pub mod outbound;
pub mod packet_ids;
pub mod queue;
pub mod state;

/// Something the caller must do as a result of an input.
///
/// The session never performs these itself. Returning them rather than
/// doing them is what keeps this module free of I/O and therefore
/// testable by driving millions of generated sequences through it.
// The packet variant is much the largest, and boxing it is deliberately
// refused: `Packet::Publish` is itself left unboxed because it is on the
// hot path and already owns a heap-allocated topic and payload, so a box
// here would reintroduce exactly the per-message allocation the codec
// declined to pay.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Action {
    /// Write this packet to the client.
    Send(Packet),
    /// A message was discarded. Count it and fire the hook; a message that
    /// disappears without being counted is a support case nobody can
    /// answer.
    Dropped {
        /// Which message went. Carried so that a caller, and the
        /// conservation property, can account for every message offered
        /// rather than only count how many vanished.
        id: message::MessageId,
        /// Why it went.
        reason: queue::DropReason,
        /// The level it would have been delivered at.
        qos: QoS,
        /// Where it was published, for the log line.
        topic: Arc<str>,
    },
    /// Close the connection with this reason.
    Close(ReasonCode),
}

pub use inbound::{Acknowledgement, Inbound, Received};
pub use inflight::{Inflight, InflightEntry, InflightState};
pub use message::{Message, MessageId};
pub use outbound::Outbound;
pub use packet_ids::PacketIds;
pub use queue::{Delivery, DropReason, Overflow, Queue, QueueLimits, QueueTotal};
pub use state::{AttachedSessions, Session, StoredWill};
