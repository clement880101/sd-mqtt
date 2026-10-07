//! What moves when a session moves from one node to another.
//!
//! Section 8.4: a client that connects to a node which does not own its
//! session makes that node claim it, ask the previous owner to close the
//! old connection and **drain the offline queue to it**, and wait a
//! bounded time before acknowledging. This is the shape of what the
//! previous owner hands over, and it is in `broker-core` because both
//! halves of the move are this crate's: one session is taken apart and
//! another is built from the pieces.
//!
//! # It is the records the store already holds, and that is deliberate
//!
//! Every field here is a `storage` record rather than a live type, for
//! the reason `cluster::forward` gives about a forwarded publication:
//! the conversion pair in [`crate::journal`] is already exercised by
//! every restart, and a second spelling of "a session outside the
//! broker" would drift from this one the first time either was
//! corrected. It also makes the frame that carries this across a link
//! the frame the store would have written, so `cluster` adds no
//! vocabulary of its own.
//!
//! No `serde` derive, and that is not an oversight. `broker-core`
//! performs no I/O and names no encoder; the crate that puts this on a
//! wire is `cluster`, which has one. The fields are all types that
//! crate can already encode.
//!
//! # The window travels as a window, not as a queue
//!
//! A message that was *outstanding* on the previous owner — sent, not
//! yet acknowledged — crosses as an [`InflightRecord`] and is restored
//! into the new owner's window, exactly as a restart restores it. Two
//! things depend on that rather than on flattening it into the queue.
//!
//! Section 6 requires a resumed publish to keep its original
//! identifier, and convention 3 records what the alternative cost: a
//! peer still waiting on the old identifier is sent the message twice,
//! and a later message is discarded when the counter wraps onto the
//! stranded one. And an exchange that has reached `AwaitingComplete`
//! must be resumed by resending the **release**, because the peer has
//! already taken ownership of the message; re-sending the publish is a
//! second copy of a message the client has already accepted. The state
//! is what says which, and only the window carries it.

use storage::record::{BodyRecord, InflightRecord, QueueEntry, SubscriptionRecord};

/// Everything one session holds that another node needs.
///
/// Produced by `Broker::hand_over` and consumed by `Broker::take_over`.
/// Empty is a legitimate value: a session with no subscriptions and
/// nothing queued still moves, because the move is about who owns the
/// identity.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Handover {
    /// Every filter the session held, with the options it held them
    /// under.
    ///
    /// **Subscriptions follow the session**, section 8.4, and the
    /// options follow with them: the subscription identifier above all,
    /// because [MQTT-3.8.4-3] has it echoed on every delivery the
    /// subscription causes and a client that lost it could no longer
    /// route what it received.
    pub subscriptions: Vec<SubscriptionRecord>,
    /// What was sent and not acknowledged, each with the body it names.
    ///
    /// See the module note: this is a window and is restored as one.
    pub inflight: Vec<(InflightRecord, BodyRecord)>,
    /// What was waiting, oldest first, each with the body it names.
    ///
    /// The body travels with the entry rather than being asked for
    /// afterwards: the previous owner is the only node that holds it,
    /// and a second round trip to fetch it is a second thing the
    /// bounded wait can run out of.
    pub queued: Vec<(QueueEntry, BodyRecord)>,
    /// The exactly-once identifiers the client owns until it releases
    /// them.
    ///
    /// Without these the new owner treats a PUBREL for an identifier it
    /// never saw as a protocol error, and the client's half-finished
    /// exchange cannot be completed anywhere.
    pub inbound: Vec<u16>,
    /// The highest sequence number the session ever assigned.
    ///
    /// Section 7.4: a session that began again from one would hand out
    /// numbers the store has already spent. It crosses the link for the
    /// same reason it survives a restart.
    pub highest_sequence: u64,
}

impl Handover {
    /// How many messages this carries, outstanding and waiting.
    #[must_use]
    pub fn messages(&self) -> usize {
        self.inflight.len() + self.queued.len()
    }

    /// Whether it carries nothing at all.
    ///
    /// A session with no subscriptions, nothing queued and no
    /// outstanding identifiers. Still a handover: see the type's note.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.subscriptions.is_empty() && self.messages() == 0 && self.inbound.is_empty()
    }
}

/// Proof that the authenticate chain has already been asked, and said
/// yes.
///
/// Section 8.4 puts the claim — a replicated register, a close on
/// another node and a queue moved across a link — **before** the
/// registration, because the epoch the registration takes is the one the
/// claim wrote. A connect that is going to be refused must not do any of
/// that, so the chain is asked first, by `Broker::authenticates`, and
/// `Broker::connect_claimed` is handed this rather than asking again.
///
/// It cannot be constructed anywhere else: the field is private and this
/// module hands none out. A caller cannot write `true` into the connect
/// path by mistake, which is what a plain `bool` parameter in that
/// position would let it do, and what it would then do is admit every
/// client on the listener.
#[derive(Debug, Clone, Copy)]
pub struct Authenticated(pub(crate) ());
