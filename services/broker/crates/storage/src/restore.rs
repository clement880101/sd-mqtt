//! What a rebuild needs.
//!
//! Section 7.5 of the design: restore is the first startup phase and
//! completes before any listener binds, so no client can observe a
//! half-restored broker. This module is the shape of what it hands over.

use std::collections::HashMap;

use crate::record::{
    BodyRecord, InflightRecord, MessageId, NodeIdentityRecord, QueueEntry, RetainedRecord,
    RuleRecord, SessionRecord, SubscriptionRecord,
};

/// Everything needed to rebuild the broker, read once at startup.
///
/// # Eager, not an iterator
///
/// This is the whole state, in memory, before the caller sees any of it.
/// The bound is the configured queue size times the session count, which
/// at this node's budget of 100,000 connections and a default queue of
/// 1,000 messages is a number nobody would want to hold. That argument
/// looks decisive and is not, for one reason:
///
/// **The broker holds the same queues in memory anyway.** Milestone 2's
/// `Outbound` keeps every queued delivery resident, so the steady state
/// after restore already carries this bound. Streaming would not lower
/// it; it would only avoid holding a second copy of it during startup.
/// And the second copy is smaller than it looks, because a body is stored
/// once and handed over as [`bytes::Bytes`]: the payloads are shared with
/// the structures the broker builds from them, not duplicated. What is
/// duplicated is the per-entry metadata, for as long as the caller takes
/// to consume it.
///
/// Against that, an iterator is not free. It cannot be expressed over
/// `&self` on a `dyn Storage` without either a boxed item per entry or a
/// lifetime that ties the caller to a live read transaction, and every
/// caller pays for the complication, including the in-memory backend and
/// every test that uses it. Buying a smaller transient peak with that,
/// for a bound the broker has already accepted, is the wrong trade today.
///
/// # What would change this
///
/// Any one of these, and streaming becomes the right answer:
///
/// - **The broker stops holding whole queues in memory.** The cursor-based
///   [`Storage::queue_head`] exists so a queue *can* be read incrementally,
///   and if the broker ever keeps only a head window resident then the
///   steady-state bound disappears and restore becomes the only place the
///   whole queue is materialised. This is the likeliest of the three.
/// - **A measurement shows the transient copy matters.** The idle memory
///   budget is 30 MB and restore is not idle, but a restore that doubles
///   peak resident memory on a full node is a real operational limit and
///   should be measured rather than assumed, as section 17 says of the
///   other storage trade.
/// - **Restore stops being all-or-nothing.** If a later milestone wants to
///   bind listeners before every session is back, restore has to yield
///   sessions one at a time by definition.
///
/// [`Storage::queue_head`]: crate::Storage::queue_head
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Restored {
    /// Every session that survived, each with everything that belongs to
    /// it. Section 7.5 restores them in this order: the session, its
    /// subscriptions, its inbound identifiers, its inflight entries, then
    /// its queue.
    ///
    /// A session whose expiry has passed while the broker was down is not
    /// restored, and that is the caller's decision, not the store's: the
    /// clock it is judged against must be read before restore rather than
    /// after, so the store hands over what it holds and says nothing about
    /// the time.
    pub sessions: Vec<RestoredSession>,
    /// Retained messages, by topic.
    pub retained: Vec<(String, RetainedRecord)>,
    /// The bodies the queue and inflight entries reference.
    ///
    /// Stored once however many sessions reference them, so this is one
    /// entry per distinct message rather than one per delivery. Every
    /// [`crate::record::DeliveryRecord::body`] in [`Self::sessions`]
    /// appears here; a body with no reference left has been deleted and
    /// does not.
    pub bodies: HashMap<MessageId, BodyRecord>,
    /// Every rule the node holds, in identifier order.
    ///
    /// Section 9.1: a rule is durable node-local metadata in milestone 4,
    /// so it comes back with everything else rather than being read on
    /// demand. Ordered, so that two backends restoring the same store
    /// hand back the same value and the restart property can compare them
    /// as one equality.
    pub rules: Vec<RuleRecord>,
    /// Who this node is, if the store has ever been told.
    ///
    /// `None` for a store that has never held an identity, which is a
    /// store starting for the first time. Section 8.1 mints one then;
    /// **the store does not mint it**, because minting is a decision
    /// about a cluster and this crate knows nothing about one.
    ///
    /// Read back here rather than through a call of its own so that
    /// startup reads the store once. It is one row, and it is wanted at
    /// exactly the moment everything else is.
    pub node: Option<NodeIdentityRecord>,
}

/// One session and everything that belongs to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredSession {
    /// The session's own durable fields, including its key.
    pub session: SessionRecord,
    /// The filters it held, which re-enter the matching trie.
    pub subscriptions: Vec<SubscriptionRecord>,
    /// Exactly-once identifiers the peer owns and has not released.
    pub inbound: Vec<u16>,
    /// Messages sent and not acknowledged, with the identifiers they were
    /// sent under. Section 6: a resumed publish keeps its identifier.
    pub inflight: Vec<InflightRecord>,
    /// What waits, in sequence order, which is arrival order.
    pub queue: Vec<QueueEntry>,
    /// The highest sequence number ever assigned to this session, whether
    /// or not that entry is still held.
    ///
    /// Carried here as well as on [`Storage::highest_sequence`] so that
    /// restoring a node does not need one call per session to find the
    /// number it must not reuse. Section 7.4: two entries sharing a number,
    /// or a number going backwards, is what must never happen.
    ///
    /// [`Storage::highest_sequence`]: crate::Storage::highest_sequence
    pub highest_sequence: u64,
}
