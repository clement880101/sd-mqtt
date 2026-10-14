//! What a rebuild needs.
//!
//! Section 7.5 of the design: restore is the first startup phase and
//! completes before any listener binds, so no client can observe a
//! half-restored broker. This module is the shape of what it hands over.

use std::collections::HashMap;

use crate::record::{
    AclRecord, ApiKeyRecord, BodyRecord, ClockRecord, InflightRecord, LoggedRecord, MessageId,
    MetadataRecord, NodeIdentityRecord, QueueEntry, RetainedRecord, RuleRecord, SeenRecord,
    SessionRecord, SubscriptionRecord, UserRecord,
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
    /// Sessions this node holds a **copy** of rather than owns, section
    /// 8.6, each with the node that owns it and how far its stream was
    /// applied.
    ///
    /// **A separate list, and that is the whole safety of it.** Every
    /// record a followed session is made of is the same record an owned
    /// one is made of, so a restore that handed these back in
    /// [`Self::sessions`] would have the broker serve another node's
    /// session: its filters would enter this node's matching trie, it
    /// would be forwarded publications for them, and it would deliver
    /// them to a client that is connected somewhere else. Two lists
    /// make that mistake something a caller has to write on purpose
    /// rather than something it gets by not knowing.
    pub replicas: Vec<RestoredReplica>,
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
    /// Section 9.1: a rule is durable metadata, so it comes back with
    /// everything else rather than being read on demand. It was
    /// node-local in milestone 4 and is replicated from milestone 5, and
    /// what changed is how many nodes hold one rather than its shape;
    /// the register that carries it between them is
    /// [`Self::metadata`]. Ordered, so that two backends restoring the same store
    /// hand back the same value and the restart property can compare them
    /// as one equality.
    pub rules: Vec<RuleRecord>,
    /// Every user the node holds, in name order. Section 12.1, durable
    /// metadata like a rule, and ordered for the same reason.
    pub users: Vec<UserRecord>,
    /// Every access control rule the node holds, in identifier order.
    pub acl: Vec<AclRecord>,
    /// Every API key the node holds, in identifier order. Section 11.1.
    pub api_keys: Vec<ApiKeyRecord>,
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
    /// The highest clock reading this node has ever issued, if it has
    /// ever issued one.
    ///
    /// `None` for a store that has never been written to by a clock,
    /// which is a node whose first write is still to come. Section 8.2:
    /// **the clock resumes from this and not from zero**, because a
    /// clock that restarts behind itself re-issues a stamp, and two
    /// writes sharing a stamp is the one divergence no exchange
    /// repairs.
    ///
    /// Read back here rather than through a call of its own, for the
    /// reason [`Self::node`] gives: it is one row, and it is wanted at
    /// exactly the moment everything else is.
    pub clock: Option<ClockRecord>,
    /// Every replicated register the node holds, in key order.
    ///
    /// Section 8.2's records: that a node has a subscriber for a filter,
    /// retained messages, rules and session ownership, each a
    /// last-writer-wins register with the stamp that orders it.
    /// **Tombstones are here too**, because a deletion is a register
    /// with no value and a node that dropped one would be handed an
    /// older copy by the next peer it spoke to.
    ///
    /// Ordered, so that two backends restoring the same store hand back
    /// the same value and the restart property can compare them as one
    /// equality — the same reason [`Self::rules`] is ordered.
    pub metadata: Vec<MetadataRecord>,
    /// This node's own operation log, in sequence order.
    ///
    /// Section 8.2: the anti-entropy digest is a claim about this and not
    /// about [`Self::metadata`], because a register's most recent writer
    /// changes whenever another node overwrites it. Truncated behind what
    /// every other node has read of it, so a healthy cluster's log is
    /// short; a cluster with a node that has been away holds everything
    /// that node has not seen.
    pub oplog: Vec<LoggedRecord>,
    /// The last sequence collected out of this node's log.
    ///
    /// Zero for a node that has collected nothing. Section 8.2's second
    /// watermark, as the store last saw it: everything at or below it has
    /// been read by every node and is gone, so nothing will ever ask for
    /// it and nothing may ever be numbered there again.
    ///
    /// **This and [`Self::oplog`] are what a resuming log needs**, and
    /// between them they say which number comes next: the entries run from
    /// here to the top, so the highest sequence ever used is this or the
    /// highest entry, whichever is larger. A node that resumed from one
    /// would re-issue a sequence every peer had already been served, which
    /// is the same defect as a clock resuming from zero, in the other of
    /// section 8.2's two counters — and the case that makes this field
    /// rather than the entries the answer is a log collected down to
    /// nothing, which is what a healthy cluster's log almost always is.
    ///
    /// A third field holding the highest sequence outright was here first
    /// and was deleted: it is this or the highest entry by construction,
    /// so no test could tell an implementation that ignored it from one
    /// that did not, which is convention 5's test that cannot fail.
    pub oplog_collected_through: u64,
    /// How far this node has read each other node's log, in node order.
    ///
    /// Section 8.2, and durable for the reason [`SeenRecord`] gives: a
    /// node that came back claiming to have seen nothing would be served
    /// logs that have been truncated, and would hold every peer's
    /// watermark down until it caught up.
    pub seen: Vec<SeenRecord>,
}

/// One session this node holds a copy of, and who owns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoredReplica {
    /// Everything the copy holds, in the same shape an owned session
    /// comes back in.
    pub session: RestoredSession,
    /// The node that owns it, by its sixteen identity bytes.
    ///
    /// Opaque here, as a message identifier is: which node that is, is
    /// `cluster`'s question.
    pub owner: [u8; 16],
    /// The highest contiguous stream position applied before the
    /// restart, which is where the follower resumes.
    pub through: u64,
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
    /// How far this node had sent this session's changes to its
    /// followers, section 8.6.
    ///
    /// Zero for a session that has never been streamed, which is the
    /// same reading as one whose stream has sent nothing: a node
    /// resuming either starts at one.
    ///
    /// On the session rather than in a list of its own, because it is
    /// a fact about the session and a caller holding one should not
    /// have to go and look it up — and because the owner's position
    /// and the queue it counts must not be able to arrive separately.
    pub stream_at: u64,
}
