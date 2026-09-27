//! A restart is indistinguishable from not having stopped.
//!
//! The property the milestone exists for. The in-memory backend is the
//! model and redb is the implementation, which is the shape of
//! `broker-core`'s differential test of the subscription trie against a
//! naive matcher, and it works for the same reason: two implementations
//! of one contract disagree loudest where the contract is least clear.
//!
//! # The generator is where this test lives or dies
//!
//! Convention 5 records a differential suite that could not generate a
//! bare multi-level wildcard, so deleting a guard survived twenty
//! thousand cases. The same hazard is sharper here, because the single
//! mistake this backend can make that nothing else would catch — a
//! little-endian queue key — is invisible unless the generated queue
//! holds two entries that a little-endian key would reorder. Sequence
//! numbers one to ten do not: they agree byte for byte in either order.
//!
//! So [`the_generator_produces_every_case_the_property_needs`] is not
//! decoration. It runs the generator over a fixed number of plans with a
//! deterministic runner and counts how often each case that matters
//! appears, and it fails if any of them is zero.
//!
//! ## What it can produce
//!
//! - A pop of a queue that is already empty, and a pop of a sequence
//!   number that was assigned and popped already (a hole).
//! - A release of a body that has one reference left, so the body goes,
//!   and a release of a body that is not held at all, which is refused.
//! - A session removed while it still holds a queue, and a session
//!   recreated under the same key afterwards, which must inherit nothing.
//! - Two sessions holding entries that name the same body.
//! - A queue holding entries that a little-endian key would put in the
//!   wrong order, because the sequence gaps jump across byte boundaries.
//! - A batch that the store refuses after earlier records in it were
//!   applied, which is the atomicity promise.
//! - Session keys that are prefixes of one another, an empty client
//!   identifier, an empty mountpoint, and a client identifier with an
//!   embedded NUL, which is what the key escape exists for.
//! - Packet identifiers either side of a byte boundary, so the inbound
//!   and inflight keys are read in the same order by both backends.
//! - An outbox appended to, attempted on and drained: an append for a
//!   rule the store does not hold and one at a number already spent,
//!   both refused; an attempt and a removal naming an envelope that has
//!   left, both no-ops; a rule replaced while its outbox held
//!   envelopes, which must keep them, and one deleted while it did,
//!   which must take them; and an outbox whose sequence numbers a
//!   little-endian key would reorder.
//!
//! The outbox is here because the previous plan added it to the store
//! without adding it to this test, so it was covered by focused tests
//! and not by the property that catches what nobody thought of.
//!
//! ## What it cannot produce
//!
//! - **Concurrency.** One thread applies every batch to both stores in
//!   turn. What two concurrent batches do to each other is not specified
//!   by the trait, so there is nothing here to compare.
//! - **An unclean exit.** Every store here is closed cleanly, and a clean
//!   close of a redb store writes out everything, flushed or not. This
//!   property therefore says nothing about *when* a write became durable,
//!   and it is not asked to: it is about what was applied. That a flush
//!   is what survives a process that exits without running a destructor
//!   is proved in `tests/redb.rs`, in a child process, which is the only
//!   shape of test that can tell the difference.
//! - **A damaged or foreign store.** Corruption, a format marker from a
//!   later build, and a locked path are `tests/redb.rs`.
//! - **Large payloads.** Bodies here are a few bytes, so the store stays
//!   small enough to run hundreds of times. What a multi-megabyte body
//!   costs is `tests/throughput.rs`.
//! - **A wide universe of names.** Sessions, filters and topics are drawn
//!   from small fixed sets, chosen for the shapes above rather than for
//!   variety, so that sequences collide on the same session often rather
//!   than by luck. Arbitrary Unicode, long identifiers and deep topic
//!   hierarchies are the codec's problem and are tested there.
//! - **A sequence number near [`u64::MAX`].** The gaps are bounded, so a
//!   plan cannot push the high-water mark far enough to overflow.
//!
//! # Why the comparison is one equality
//!
//! Both backends hand back [`Restored`] in the same total order —
//! sessions by mountpoint and client identifier, subscriptions by filter,
//! inbound identifiers and inflight entries by packet identifier, queue
//! entries by sequence, retained messages by topic — so the two can be
//! compared as values. The trait does not promise that; it falls out of
//! the in-memory backend's `BTreeMap`s and of redb's order-preserving key
//! encoding, and if a backend ever stops doing it, this test says so
//! rather than the comparison being quietly loosened to sets.
//!
//! [`Restored`] does not carry the queue depth, so the depth, the
//! high-water mark and the cursor read are compared separately, over
//! every session in the universe including ones no plan created. A depth
//! that drifts is otherwise invisible to a restore.
//!
//! **[`Restored`] does not carry an outbox at all**, so the whole of it
//! is compared that way: the depth, the high-water mark, the head at
//! cursors either side of the byte boundaries the key has to get right,
//! and every envelope, walked by cursor from the start. Over every rule
//! in the universe including one no plan creates.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{TestCaseResult, TestRunner};
use storage::{
    ActionRecord, BodyRecord, DeliveryRecord, HttpOutRecord, InflightRecord, InflightState,
    MemoryStorage, MessageId, OutboxRecord, ProtocolVersion, Qos, QueueEntry, Record, RedbStorage,
    RetainHandling, RetainedRecord, RuleRecord, SessionKey, SessionRecord, Storage,
    SubscriptionRecord, TriggerRecord, WillRecord,
};

// ---- the universe the generator draws from ------------------------------

/// The session keys a plan may name.
///
/// Chosen rather than generated. `""`/`"a"`/`"ab"` are the pair a naive
/// key concatenation gets wrong, `"a\0"` is what the escape exists for,
/// and the second mountpoint is there so that the mountpoint is part of
/// what identifies a session rather than a constant.
fn sessions() -> Vec<SessionKey> {
    vec![
        SessionKey::new("", ""),
        SessionKey::new("", "a"),
        SessionKey::new("", "ab"),
        SessionKey::new("", "a\u{0}"),
        SessionKey::new("edge", "a"),
    ]
}

/// A session key that no plan ever creates, so that "not held" is
/// compared as well as "held".
fn a_session_no_plan_creates() -> SessionKey {
    SessionKey::new("edge", "never")
}

/// The filters a plan may subscribe to.
const FILTERS: [&str; 4] = ["#", "a", "a/b", "sensor/+/temp"];

/// The topics a plan may retain on.
const TOPICS: [&str; 3] = ["a", "a/b", "sensor/1/temp"];

/// The rule identifiers a plan may use.
///
/// Chosen for the same reason the session keys are: `"a"` and `"ab"` are
/// the pair a backend that keyed rules by a prefix would confuse, and a
/// rule's key is the identifier as text with nothing around it.
const RULES: [&str; 3] = ["a", "ab", "telemetry"];

/// A rule identifier that no plan ever creates, so that an outbox
/// nobody filled is compared as well as one somebody did.
const A_RULE_NO_PLAN_CREATES: &str = "never";

/// How many distinct bodies a plan may store.
const BODIES: usize = 4;

/// The largest number of records in one batch, when no plan boundary
/// closes it first.
///
/// Small, so that a plan is many batches rather than one: a batch is the
/// unit of atomicity, and a plan applied as a single batch would never
/// exercise a refusal discarding part of itself.
const BATCH: usize = 8;

// ---- what a plan is -----------------------------------------------------

/// One thing a broker might do. Indices into the universes above, so that
/// a plan collides on the same session and the same body often.
#[derive(Debug, Clone)]
enum Op {
    /// A session appears, or its durable fields change.
    SessionPut {
        /// Which session.
        session: usize,
        /// How long it asked to live for.
        expiry: u32,
        /// Whether it left a will.
        will: bool,
        /// Which protocol it connected with.
        five: bool,
    },
    /// A session vanishes, taking everything with it.
    SessionRemoved {
        /// Which session.
        session: usize,
    },
    /// A session subscribes, or changes the options on a filter.
    Subscribe {
        /// Whose.
        session: usize,
        /// Which filter.
        filter: usize,
        /// At what level.
        qos: u8,
        /// The MQTT 5 subscription identifier, if any.
        id: Option<u32>,
    },
    /// A session unsubscribes.
    Unsubscribe {
        /// Whose.
        session: usize,
        /// Which filter.
        filter: usize,
    },
    /// The peer takes an exactly-once identifier.
    InboundReserved {
        /// Whose.
        session: usize,
        /// Which identifier.
        packet_id: u16,
    },
    /// The peer releases one.
    InboundReleased {
        /// Whose.
        session: usize,
        /// Which identifier.
        packet_id: u16,
    },
    /// A body is stored, taking one reference.
    BodyPut {
        /// Which body.
        body: usize,
        /// Something to make the payload distinguishable.
        payload: u8,
    },
    /// One reference is released.
    BodyReleased {
        /// Which body.
        body: usize,
    },
    /// A message joins a queue, at the next free sequence plus a gap.
    QueuePushed {
        /// Whose.
        session: usize,
        /// What it carries.
        body: usize,
        /// How far past the high-water mark to jump.
        gap: u64,
        /// The identifier it was already sent under, if it is a requeue.
        packet_id: Option<u16>,
    },
    /// The head of a queue is popped, or a pop is made of a queue that is
    /// already empty.
    QueuePoppedHead {
        /// Whose.
        session: usize,
    },
    /// A pop of a particular sequence, usually one that is already gone.
    QueuePoppedAt {
        /// Whose.
        session: usize,
        /// Which sequence.
        sequence: u64,
    },
    /// A message is sent and awaits acknowledgement.
    InflightPut {
        /// Whose.
        session: usize,
        /// Under which identifier.
        packet_id: u16,
        /// What it carries.
        body: usize,
        /// How far the exchange has got.
        state: u8,
    },
    /// An exchange finishes.
    InflightRemoved {
        /// Whose.
        session: usize,
        /// Which identifier.
        packet_id: u16,
    },
    /// A retained message is set.
    RetainedPut {
        /// On which topic.
        topic: usize,
        /// Something to make the payload distinguishable.
        payload: u8,
    },
    /// A retained message is cleared.
    RetainedRemoved {
        /// Which topic.
        topic: usize,
    },
    /// A rule is created, or one that exists is changed.
    RulePut {
        /// Which rule.
        rule: usize,
        /// Whether it is switched on.
        enabled: bool,
        /// Something to make the rest of it distinguishable.
        shape: u8,
    },
    /// A rule is deleted.
    RuleRemoved {
        /// Which rule.
        rule: usize,
    },
    /// An envelope joins a rule's outbox, at the next free sequence
    /// plus a gap.
    OutboxAppended {
        /// Whose outbox.
        rule: usize,
        /// The payload it names.
        body: usize,
        /// How far past the high-water mark to jump.
        gap: u64,
        /// Something to make the rest of the envelope distinguishable.
        shape: u8,
        /// Append at a number this outbox has already spent, which is
        /// refused. Rare, and generated rather than left to chance:
        /// aiming an append at the next free number is what keeps a
        /// plan from being all refusals, and it also makes the one
        /// refusal an append has unreachable.
        reuse: bool,
    },
    /// An attempt is recorded against an envelope.
    OutboxAttempted {
        /// Whose outbox.
        rule: usize,
        /// Which envelope. `None` is the head, which is the one a pump
        /// actually attempts; a number is usually one that has left,
        /// which must be a no-op rather than a resurrection.
        at: Option<u64>,
        /// How many attempts have now been made and failed.
        attempts: u32,
        /// The coarse second before which the next must not be made.
        next_attempt_at: u32,
    },
    /// An envelope leaves an outbox, delivered or dead-lettered.
    OutboxRemoved {
        /// Whose outbox.
        rule: usize,
        /// Which envelope. `None` is the head.
        at: Option<u64>,
    },
    /// Close the current batch here.
    ///
    /// Generated rather than fixed, so that the boundary falls between
    /// records in different places in different plans and a refusal
    /// lands at a different offset each time.
    EndBatch,
}

/// A packet identifier either side of a byte boundary.
///
/// The inbound and inflight keys put this in big-endian bytes, and a
/// little-endian one would reorder 255 and 256. A generator drawing from
/// 1..=8 could not tell the two apart.
fn any_packet_id() -> impl Strategy<Value = u16> {
    prop_oneof![
        Just(1u16),
        Just(2),
        Just(7),
        Just(255),
        Just(256),
        Just(4_097),
        Just(65_535),
    ]
}

/// How far past the high-water mark the next push lands.
///
/// The large gaps are the point: a queue whose sequences all share a high
/// byte sorts identically under either byte order, so a generator that
/// only ever incremented by one could not see the one mistake this
/// backend can make that nothing else would catch.
fn any_gap() -> impl Strategy<Value = u64> {
    prop_oneof![
        6 => Just(0u64),
        3 => Just(1),
        3 => Just(7),
        3 => Just(250),
        2 => Just(1_000),
        1 => Just(60_000),
    ]
}

/// Which envelope an attempt or a removal names.
///
/// Mostly the head, because that is what a pump touches and because an
/// outbox that is never drained never produces a removal of something
/// held. The numbers are the same ones a queue pop draws from: either
/// side of the byte boundaries the key has to get right, and mostly
/// naming an envelope that has already left.
fn any_outbox_at() -> impl Strategy<Value = Option<u64>> {
    prop_oneof![
        6 => Just(None),
        1 => Just(Some(0u64)),
        1 => Just(Some(1)),
        1 => Just(Some(2)),
        1 => Just(Some(258)),
        1 => Just(Some(60_001)),
    ]
}

/// One operation.
#[allow(clippy::too_many_lines)]
fn any_op(session_count: usize) -> impl Strategy<Value = Op> {
    let session = 0..session_count;
    let body = 0..BODIES;
    prop_oneof![
        5 => (session.clone(), 0u32..3, any::<bool>(), any::<bool>()).prop_map(
            |(session, expiry, will, five)| Op::SessionPut { session, expiry: expiry * 60, will, five }
        ),
        1 => session.clone().prop_map(|session| Op::SessionRemoved { session }),
        4 => (session.clone(), 0..FILTERS.len(), 0u8..3, proptest::option::of(0u32..4)).prop_map(
            |(session, filter, qos, id)| Op::Subscribe { session, filter, qos, id }
        ),
        2 => (session.clone(), 0..FILTERS.len())
            .prop_map(|(session, filter)| Op::Unsubscribe { session, filter }),
        3 => (session.clone(), any_packet_id())
            .prop_map(|(session, packet_id)| Op::InboundReserved { session, packet_id }),
        2 => (session.clone(), any_packet_id())
            .prop_map(|(session, packet_id)| Op::InboundReleased { session, packet_id }),
        6 => (body.clone(), any::<u8>()).prop_map(|(body, payload)| Op::BodyPut { body, payload }),
        4 => body.clone().prop_map(|body| Op::BodyReleased { body }),
        8 => (
            session.clone(),
            body.clone(),
            any_gap(),
            proptest::option::of(any_packet_id()),
        )
            .prop_map(|(session, body, gap, packet_id)| Op::QueuePushed {
                session,
                body,
                gap,
                packet_id,
            }),
        5 => session.clone().prop_map(|session| Op::QueuePoppedHead { session }),
        2 => (session.clone(), prop_oneof![Just(0u64), Just(1), Just(2), Just(258), Just(60_001)])
            .prop_map(|(session, sequence)| Op::QueuePoppedAt { session, sequence }),
        5 => (session.clone(), any_packet_id(), body.clone(), 0u8..3).prop_map(
            |(session, packet_id, body, state)| Op::InflightPut { session, packet_id, body, state }
        ),
        3 => (session, any_packet_id())
            .prop_map(|(session, packet_id)| Op::InflightRemoved { session, packet_id }),
        3 => (0..TOPICS.len(), any::<u8>())
            .prop_map(|(topic, payload)| Op::RetainedPut { topic, payload }),
        2 => (0..TOPICS.len()).prop_map(|topic| Op::RetainedRemoved { topic }),
        3 => (0..RULES.len(), any::<bool>(), any::<u8>())
            .prop_map(|(rule, enabled, shape)| Op::RulePut { rule, enabled, shape }),
        2 => (0..RULES.len()).prop_map(|rule| Op::RuleRemoved { rule }),
        6 => (
            0..RULES.len(),
            body.clone(),
            any_gap(),
            any::<u8>(),
            proptest::bool::weighted(0.1),
        )
            .prop_map(|(rule, body, gap, shape, reuse)| Op::OutboxAppended {
                rule,
                body,
                gap,
                shape,
                reuse,
            }),
        4 => (0..RULES.len(), any_outbox_at(), 0u32..8, 0u32..4).prop_map(
            |(rule, at, attempts, step)| Op::OutboxAttempted {
                rule,
                at,
                attempts,
                // Zero is "now", so one of the four is the envelope
                // that is due immediately and the rest are waiting.
                next_attempt_at: if step == 0 { 0 } else { 1_700_000_000 + step * 30 },
            },
        ),
        4 => (0..RULES.len(), any_outbox_at())
            .prop_map(|(rule, at)| Op::OutboxRemoved { rule, at }),
        3 => Just(Op::EndBatch),
    ]
}

/// One operation, and whether it is allowed to name something the store
/// does not hold.
///
/// The two are separate because a plan of uniformly random indices is
/// almost all refusals: a record naming a session that was never created
/// is refused, a refusal discards its whole batch, and nothing ever
/// accumulates. The first version of this generator did exactly that,
/// and produced none of the cases that need history — no session removed
/// while it held a queue, no queue wide enough to cross a byte boundary.
/// So an operation names something the store holds unless it is
/// deliberately stray, and stray is rare.
#[derive(Debug, Clone)]
struct Step {
    /// Whether to leave the operation's indices alone even when they
    /// name nothing.
    stray: bool,
    /// What to do.
    op: Op,
}

/// How often an operation is allowed to name something that is not held.
///
/// Low, but not zero: a record for a session the store does not hold,
/// and a release of a body it does not hold, are both refusals the two
/// backends must agree on, and both are counted.
const STRAY: f64 = 0.04;

/// A plan: a long sequence of operations.
///
/// Long rather than numerous, as the plan for this task says: a hundred
/// long sequences find more than a thousand short ones, because the cases
/// that matter here — a body down to its last reference, a session
/// removed while it holds a queue, a queue wide enough to cross a byte
/// boundary — need history to have accumulated first.
fn any_plan() -> impl Strategy<Value = Vec<Step>> {
    proptest::collection::vec(
        (proptest::bool::weighted(STRAY), any_op(sessions().len()))
            .prop_map(|(stray, op)| Step { stray, op }),
        20..240,
    )
}

// ---- what the compiler has to know --------------------------------------

/// The cases this test would be worthless without, counted.
///
/// Every one of these is something a plausible generator silently fails
/// to produce. They are counted rather than asserted per case, because a
/// property that required each case in every plan would be a different,
/// much narrower test.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Coverage {
    /// A pop of a session's queue when that queue holds nothing.
    pop_of_an_already_empty_queue: u64,
    /// A pop of a sequence that was assigned and is already gone.
    pop_of_a_hole: u64,
    /// A release that takes a body's last reference, deleting it.
    release_of_the_last_reference: u64,
    /// A release of a body the store does not hold, which is refused.
    release_of_a_body_not_held: u64,
    /// A session removed while its queue still holds entries.
    session_removed_holding_a_queue: u64,
    /// A session put back under a key whose session was removed while it
    /// had assigned sequence numbers. It must inherit none of them.
    session_recreated_after_removal: u64,
    /// Two sessions holding live entries that name one body.
    two_sessions_holding_one_body: u64,
    /// A queue holding two entries that a little-endian key would sort
    /// in the wrong order.
    a_queue_a_little_endian_key_would_reorder: u64,
    /// A batch refused by a record that is not its first, so records
    /// before it had already been applied within the transaction.
    a_batch_refused_after_earlier_records: u64,
    /// A record naming a session the store does not hold.
    a_record_for_a_session_not_held: u64,
    /// A rule put over an identifier the store already holds, which
    /// must replace it rather than leave the first one there.
    a_rule_put_over_one_already_held: u64,
    /// A rule removed while it was held, as against a removal of one
    /// that was not, which is a no-op.
    a_rule_removed_while_held: u64,
    /// An outbox record naming a rule the store does not hold, which is
    /// refused: an outbox is reached through the rule that owns it, so
    /// one filed under a rule that is not there is reachable by
    /// nothing.
    an_outbox_record_for_a_rule_not_held: u64,
    /// An append at a number this outbox has already spent, which is
    /// refused. The number is never reused.
    an_append_at_a_number_already_spent: u64,
    /// An attempt recorded against an envelope that is still there,
    /// which replaces its count and its deadline.
    an_attempt_on_an_envelope_still_there: u64,
    /// An attempt naming an envelope that has already left, which is a
    /// no-op rather than a row nothing would ever remove.
    an_attempt_on_an_envelope_that_has_left: u64,
    /// An envelope removed while it was there, which is a delivery.
    a_removal_of_an_envelope_still_there: u64,
    /// A removal naming a sequence the outbox does not hold, which must
    /// leave the depth where it is.
    a_removal_of_a_sequence_not_held: u64,
    /// A rule replaced by a put while its outbox held envelopes. They
    /// must survive: an edit is how a connector is turned off and on
    /// again, and a put that emptied the outbox would discard messages
    /// somebody was told had been accepted.
    a_rule_replaced_while_its_outbox_held_envelopes: u64,
    /// A rule deleted while its outbox held envelopes. They must go
    /// with it, because nothing could reach them afterwards.
    a_rule_removed_while_its_outbox_held_envelopes: u64,
    /// A rule put back after a removal that left a high-water mark
    /// behind. Its outbox must inherit none of it.
    a_rule_recreated_after_its_outbox_was_removed: u64,
    /// An outbox holding two envelopes that a little-endian key would
    /// sort in the wrong order, which is the one mistake this key can
    /// make that nothing else would catch.
    an_outbox_a_little_endian_key_would_reorder: u64,
}

impl Coverage {
    /// Add another run's counts.
    fn add(&mut self, other: Self) {
        self.pop_of_an_already_empty_queue += other.pop_of_an_already_empty_queue;
        self.pop_of_a_hole += other.pop_of_a_hole;
        self.release_of_the_last_reference += other.release_of_the_last_reference;
        self.release_of_a_body_not_held += other.release_of_a_body_not_held;
        self.session_removed_holding_a_queue += other.session_removed_holding_a_queue;
        self.session_recreated_after_removal += other.session_recreated_after_removal;
        self.two_sessions_holding_one_body += other.two_sessions_holding_one_body;
        self.a_queue_a_little_endian_key_would_reorder +=
            other.a_queue_a_little_endian_key_would_reorder;
        self.a_batch_refused_after_earlier_records += other.a_batch_refused_after_earlier_records;
        self.a_record_for_a_session_not_held += other.a_record_for_a_session_not_held;
        self.a_rule_put_over_one_already_held += other.a_rule_put_over_one_already_held;
        self.a_rule_removed_while_held += other.a_rule_removed_while_held;
        self.an_outbox_record_for_a_rule_not_held += other.an_outbox_record_for_a_rule_not_held;
        self.an_append_at_a_number_already_spent += other.an_append_at_a_number_already_spent;
        self.an_attempt_on_an_envelope_still_there += other.an_attempt_on_an_envelope_still_there;
        self.an_attempt_on_an_envelope_that_has_left +=
            other.an_attempt_on_an_envelope_that_has_left;
        self.a_removal_of_an_envelope_still_there += other.a_removal_of_an_envelope_still_there;
        self.a_removal_of_a_sequence_not_held += other.a_removal_of_a_sequence_not_held;
        self.a_rule_replaced_while_its_outbox_held_envelopes +=
            other.a_rule_replaced_while_its_outbox_held_envelopes;
        self.a_rule_removed_while_its_outbox_held_envelopes +=
            other.a_rule_removed_while_its_outbox_held_envelopes;
        self.a_rule_recreated_after_its_outbox_was_removed +=
            other.a_rule_recreated_after_its_outbox_was_removed;
        self.an_outbox_a_little_endian_key_would_reorder +=
            other.an_outbox_a_little_endian_key_would_reorder;
    }

    /// Every counter, named, so that a zero can be reported as the name
    /// of the case the generator cannot reach rather than as a number.
    fn named(&self) -> [(&'static str, u64); 22] {
        [
            (
                "a pop of an already empty queue",
                self.pop_of_an_already_empty_queue,
            ),
            ("a pop of a hole", self.pop_of_a_hole),
            (
                "a release of a body with one reference left",
                self.release_of_the_last_reference,
            ),
            (
                "a release of a body that is not held",
                self.release_of_a_body_not_held,
            ),
            (
                "a session removed while it holds a queue",
                self.session_removed_holding_a_queue,
            ),
            (
                "a session recreated after removal",
                self.session_recreated_after_removal,
            ),
            (
                "two sessions holding one body",
                self.two_sessions_holding_one_body,
            ),
            (
                "a queue a little-endian key would reorder",
                self.a_queue_a_little_endian_key_would_reorder,
            ),
            (
                "a batch refused after earlier records",
                self.a_batch_refused_after_earlier_records,
            ),
            (
                "a record for a session not held",
                self.a_record_for_a_session_not_held,
            ),
            (
                "a rule put over one already held",
                self.a_rule_put_over_one_already_held,
            ),
            (
                "a rule removed while it was held",
                self.a_rule_removed_while_held,
            ),
            (
                "an outbox record for a rule that is not held",
                self.an_outbox_record_for_a_rule_not_held,
            ),
            (
                "an append at a number already spent",
                self.an_append_at_a_number_already_spent,
            ),
            (
                "an attempt on an envelope still there",
                self.an_attempt_on_an_envelope_still_there,
            ),
            (
                "an attempt on an envelope that has left",
                self.an_attempt_on_an_envelope_that_has_left,
            ),
            (
                "a removal of an envelope still there",
                self.a_removal_of_an_envelope_still_there,
            ),
            (
                "a removal of a sequence not held",
                self.a_removal_of_a_sequence_not_held,
            ),
            (
                "a rule replaced while its outbox held envelopes",
                self.a_rule_replaced_while_its_outbox_held_envelopes,
            ),
            (
                "a rule removed while its outbox held envelopes",
                self.a_rule_removed_while_its_outbox_held_envelopes,
            ),
            (
                "a rule recreated after its outbox was removed",
                self.a_rule_recreated_after_its_outbox_was_removed,
            ),
            (
                "an outbox a little-endian key would reorder",
                self.an_outbox_a_little_endian_key_would_reorder,
            ),
        ]
    }
}

/// One session, as much of it as choosing the next record needs.
///
/// Not a second in-memory backend: there is no room here for
/// subscriptions or for what a body contains, because nothing the
/// compiler decides depends on them. What it needs is the high-water
/// mark, which sequence numbers are held and what each of them names,
/// which is what says whether a pop is a no-op and whether two sessions
/// share a body.
#[derive(Debug, Clone, Default)]
struct SessionModel {
    /// Held sequence numbers, and the body each entry names.
    queue: BTreeMap<u64, usize>,
    /// Inflight entries by identifier, and the body each names.
    inflight: BTreeMap<u16, usize>,
    /// The highest sequence ever assigned.
    highest: u64,
}

impl SessionModel {
    /// Whether this session holds a live entry naming `body`.
    fn holds(&self, body: usize) -> bool {
        self.queue.values().any(|held| *held == body)
            || self.inflight.values().any(|held| *held == body)
    }
}

/// One rule's outbox, as much of it as choosing the next record needs.
///
/// The bodies its envelopes name are not here, because removing an
/// envelope does not release one: the reference belongs to whoever took
/// it, so nothing the outbox does can be the last reference to a body.
#[derive(Debug, Clone, Default)]
struct OutboxModel {
    /// Held sequence numbers.
    entries: BTreeSet<u64>,
    /// The highest sequence ever appended, whether or not that envelope
    /// is still held. Never reused.
    highest: u64,
}

/// Why the store will refuse a record.
///
/// Named rather than sniffed out of the record's shape in [`compile`],
/// because an append can be refused for either of two reasons and only
/// the model knows which: the rule is not held, or the number is spent.
/// Each one is a case the two backends have to agree on, and a coverage
/// counter that could not tell them apart would report the pair as one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refused {
    /// A record naming a session the store does not hold.
    NoSuchSession,
    /// A release of a body the store does not hold.
    BodyNotHeld,
    /// An outbox record naming a rule the store does not hold.
    NoSuchRule,
    /// An append at a sequence this outbox has already spent.
    OutboxSequenceReused,
}

/// Enough of the store to choose the next record and to say which case it
/// is an instance of.
///
/// This is **not** an oracle. Nothing is ever asserted against it; the
/// two backends are compared with each other. Convention 6 is the reason:
/// a harness that mirrors the implementation reflects a wrong acceptance
/// rather than catching it. If this model drifts from the real semantics
/// the only consequences are a coverage count that is slightly wrong and
/// a sequence number that provokes a refusal, and a refusal is compared
/// between the backends like anything else.
#[derive(Debug, Clone, Default)]
struct Model {
    /// Sessions the store holds, by index into the universe.
    sessions: BTreeMap<usize, SessionModel>,
    /// Bodies the store holds, by index, with their reference counts.
    bodies: BTreeMap<usize, u64>,
    /// Sessions whose removal left a high-water mark behind, so that a
    /// recreation under the same key can be recognised.
    removed_with_a_mark: BTreeMap<usize, u64>,
    /// Which rules the store holds, by index into the universe.
    rules: BTreeSet<usize>,
    /// Each rule's outbox, by the same index.
    ///
    /// Separate from [`Self::rules`] because the two do not have the
    /// same lifetime: a rule replaced by a put keeps the outbox it had,
    /// and only a delete takes it.
    outboxes: BTreeMap<usize, OutboxModel>,
    /// Rules whose deletion left a high-water mark behind, so that a
    /// rule put back under the same identifier can be recognised. Its
    /// outbox must inherit none of it.
    rules_removed_with_a_mark: BTreeMap<usize, u64>,
}

/// Which universe an operation's index has to name something held in.
///
/// Matched exhaustively, so that an operation added without a decision
/// about what it names does not compile. That is the whole reason this
/// is an enum rather than two matches with a catch-all in each: the
/// hazard convention 5 records is an operation nobody aimed, which
/// produces a plan of refusals and a case that never happens.
enum Aimed<'a> {
    /// It names a session.
    Session(&'a mut usize),
    /// It names a rule.
    Rule(&'a mut usize),
    /// It names neither, or it is what creates one.
    Nothing,
}

impl<'a> Aimed<'a> {
    /// What this operation has to be pointed at.
    fn of(op: &'a mut Op) -> Self {
        match op {
            Op::SessionRemoved { session }
            | Op::Subscribe { session, .. }
            | Op::Unsubscribe { session, .. }
            | Op::InboundReserved { session, .. }
            | Op::InboundReleased { session, .. }
            | Op::QueuePushed { session, .. }
            | Op::QueuePoppedHead { session }
            | Op::QueuePoppedAt { session, .. }
            | Op::InflightPut { session, .. }
            | Op::InflightRemoved { session, .. } => Self::Session(session),
            Op::OutboxAppended { rule, .. }
            | Op::OutboxAttempted { rule, .. }
            | Op::OutboxRemoved { rule, .. } => Self::Rule(rule),
            // `SessionPut` and `RulePut` are what create the thing the
            // others are aimed at; aiming them at one that exists would
            // mean no plan ever created the rest.
            Op::SessionPut { .. }
            | Op::BodyPut { .. }
            | Op::BodyReleased { .. }
            | Op::RetainedPut { .. }
            | Op::RetainedRemoved { .. }
            | Op::RulePut { .. }
            | Op::RuleRemoved { .. }
            | Op::EndBatch => Self::Nothing,
        }
    }
}

/// A plan compiled into batches, with what the compiler saw on the way.
struct Compiled {
    /// The batches, in the order they are applied.
    batches: Vec<Vec<Record>>,
    /// What the plan turned out to contain.
    coverage: Coverage,
}

impl Model {
    /// Point an operation at something the store actually holds, unless
    /// it is deliberately stray.
    ///
    /// This is what keeps a plan from being almost entirely refusals;
    /// see [`Step`]. [`Op::SessionPut`] is left alone, because it is what
    /// creates a session and aiming it at an existing one would mean no
    /// plan ever created the others.
    fn aim(&self, op: &mut Op, stray: bool) {
        match Aimed::of(op) {
            Aimed::Session(session) => {
                *session = self.nearest(*session, sessions().len(), stray, |index| {
                    self.sessions.contains_key(&index)
                });
            }
            // An outbox is reached through the rule that owns it, so an
            // operation aimed at a rule the store does not hold is a
            // refusal in the same way a record for a missing session
            // is, and is rare for the same reason.
            Aimed::Rule(rule) => {
                *rule = self.nearest(*rule, RULES.len(), stray, |index| {
                    self.rules.contains(&index)
                });
            }
            Aimed::Nothing => {}
        }
        if let Op::BodyReleased { body } = op {
            *body = self.nearest(*body, BODIES, stray, |index| {
                self.bodies.contains_key(&index)
            });
        }
    }

    /// The first index at or after `index`, wrapping within `count`,
    /// that `held` accepts; or `index` itself when it is stray or
    /// nothing is held.
    fn nearest(
        &self,
        index: usize,
        count: usize,
        stray: bool,
        held: impl Fn(usize) -> bool,
    ) -> usize {
        if stray || held(index) {
            return index;
        }
        (0..count)
            .map(|offset| (index + offset) % count)
            .find(|candidate| held(*candidate))
            .unwrap_or(index)
    }

    /// Build the record for one operation, and apply it to the model.
    ///
    /// `apply` is false once something earlier in the batch has been
    /// refused: the store stops at the first refusal and discards the
    /// transaction, so nothing after it takes effect and nothing after it
    /// is a case this plan produced. The record is still built, because
    /// the batch is still sent.
    ///
    /// Returns the record and whether the store will accept it.
    #[allow(clippy::too_many_lines)]
    fn step(
        &mut self,
        step: &Step,
        apply: bool,
        coverage: &mut Coverage,
    ) -> (Record, Option<Refused>) {
        let keys = sessions();
        let mut op = step.op.clone();
        self.aim(&mut op, step.stray);
        match op {
            Op::SessionPut {
                session,
                expiry,
                will,
                five,
            } => {
                if apply {
                    if let Some(mark) = self.removed_with_a_mark.remove(&session) {
                        if mark > 0 && !self.sessions.contains_key(&session) {
                            coverage.session_recreated_after_removal += 1;
                        }
                    }
                    self.sessions.entry(session).or_default();
                }
                (
                    Record::SessionPut(session_record(&keys[session], expiry, will, five)),
                    None,
                )
            }
            Op::SessionRemoved { session } => {
                if apply {
                    if let Some(held) = self.sessions.remove(&session) {
                        if !held.queue.is_empty() {
                            coverage.session_removed_holding_a_queue += 1;
                        }
                        self.removed_with_a_mark.insert(session, held.highest);
                    }
                }
                (
                    Record::SessionRemoved {
                        session: keys[session].clone(),
                    },
                    None,
                )
            }
            Op::Subscribe {
                session,
                filter,
                qos,
                id,
            } => {
                let refusal = self.session_refusal(session);
                (
                    Record::SubscriptionAdded {
                        session: keys[session].clone(),
                        subscription: SubscriptionRecord {
                            filter: FILTERS[filter].to_owned(),
                            qos: qos_of(qos),
                            no_local: qos == 1,
                            retain_as_published: filter % 2 == 0,
                            retain_handling: match filter % 3 {
                                0 => RetainHandling::SendAlways,
                                1 => RetainHandling::SendIfNew,
                                _ => RetainHandling::DoNotSend,
                            },
                            subscription_id: id,
                        },
                    },
                    refusal,
                )
            }
            Op::Unsubscribe { session, filter } => {
                let refusal = self.session_refusal(session);
                (
                    Record::SubscriptionRemoved {
                        session: keys[session].clone(),
                        filter: FILTERS[filter].to_owned(),
                    },
                    refusal,
                )
            }
            Op::InboundReserved { session, packet_id } => {
                let refusal = self.session_refusal(session);
                (
                    Record::InboundReserved {
                        session: keys[session].clone(),
                        packet_id,
                    },
                    refusal,
                )
            }
            Op::InboundReleased { session, packet_id } => {
                let refusal = self.session_refusal(session);
                (
                    Record::InboundReleased {
                        session: keys[session].clone(),
                        packet_id,
                    },
                    refusal,
                )
            }
            Op::BodyPut { body, payload } => {
                if apply {
                    *self.bodies.entry(body).or_default() += 1;
                }
                (
                    Record::BodyPut {
                        id: body_id(body),
                        body: body_record(body, payload),
                    },
                    None,
                )
            }
            Op::BodyReleased { body } => {
                let held = self.bodies.get(&body).copied().unwrap_or_default();
                let refusal = (held == 0).then_some(Refused::BodyNotHeld);
                let accepted = refusal.is_none();
                if apply && accepted {
                    if held == 1 {
                        coverage.release_of_the_last_reference += 1;
                        self.bodies.remove(&body);
                    } else {
                        self.bodies.insert(body, held - 1);
                    }
                }
                (Record::BodyReleased { id: body_id(body) }, refusal)
            }
            Op::QueuePushed {
                session,
                body,
                gap,
                packet_id,
            } => {
                let refusal = self.session_refusal(session);
                let accepted = refusal.is_none();
                let sequence = self
                    .sessions
                    .get(&session)
                    .map_or(1, |held| held.highest + 1 + gap);
                if apply && accepted {
                    let sharing = self
                        .sessions
                        .iter()
                        .any(|(index, other)| *index != session && other.holds(body));
                    let held = self.sessions.entry(session).or_default();
                    held.queue.insert(sequence, body);
                    held.highest = sequence;
                    if a_little_endian_key_would_reorder(held.queue.keys().copied()) {
                        coverage.a_queue_a_little_endian_key_would_reorder += 1;
                    }
                    if sharing {
                        coverage.two_sessions_holding_one_body += 1;
                    }
                }
                (
                    Record::QueuePushed {
                        session: keys[session].clone(),
                        entry: QueueEntry {
                            sequence,
                            packet_id,
                            delivery: delivery(body, sequence),
                        },
                    },
                    refusal,
                )
            }
            Op::QueuePoppedHead { session } => {
                let refusal = self.session_refusal(session);
                let accepted = refusal.is_none();
                let held = self.sessions.get(&session);
                let head = held.and_then(|held| held.queue.keys().next().copied());
                // A queue that holds nothing is popped at one past its
                // high-water mark, which is a sequence it has never
                // assigned. That is the "pop of a queue that is already
                // empty" the plan names, and it must leave the depth
                // where it is rather than taking it below zero.
                let sequence =
                    head.unwrap_or_else(|| held.map_or(1, |held| held.highest.saturating_add(1)));
                if apply && accepted {
                    match head {
                        Some(head) => {
                            if let Some(held) = self.sessions.get_mut(&session) {
                                held.queue.remove(&head);
                            }
                        }
                        None => coverage.pop_of_an_already_empty_queue += 1,
                    }
                }
                (
                    Record::QueuePopped {
                        session: keys[session].clone(),
                        sequence,
                    },
                    refusal,
                )
            }
            Op::QueuePoppedAt { session, sequence } => {
                let refusal = self.session_refusal(session);
                let accepted = refusal.is_none();
                if apply && accepted {
                    if let Some(held) = self.sessions.get_mut(&session) {
                        if held.queue.remove(&sequence).is_none() && sequence <= held.highest {
                            coverage.pop_of_a_hole += 1;
                        }
                    }
                }
                (
                    Record::QueuePopped {
                        session: keys[session].clone(),
                        sequence,
                    },
                    refusal,
                )
            }
            Op::InflightPut {
                session,
                packet_id,
                body,
                state,
            } => {
                let refusal = self.session_refusal(session);
                let accepted = refusal.is_none();
                let sequence = self.sessions.get(&session).map_or(0, |held| held.highest);
                if apply && accepted {
                    let sharing = self
                        .sessions
                        .iter()
                        .any(|(index, other)| *index != session && other.holds(body));
                    let held = self.sessions.entry(session).or_default();
                    held.inflight.insert(packet_id, body);
                    if sharing {
                        coverage.two_sessions_holding_one_body += 1;
                    }
                }
                (
                    Record::InflightPut {
                        session: keys[session].clone(),
                        entry: InflightRecord {
                            packet_id,
                            sequence,
                            state: match state {
                                0 => InflightState::AwaitingAck,
                                1 => InflightState::AwaitingReceived,
                                _ => InflightState::AwaitingComplete,
                            },
                            sent_at: 1_700_000_000 + u32::from(packet_id),
                            delivery: delivery(body, sequence),
                        },
                    },
                    refusal,
                )
            }
            Op::InflightRemoved { session, packet_id } => {
                let refusal = self.session_refusal(session);
                let accepted = refusal.is_none();
                if apply && accepted {
                    if let Some(held) = self.sessions.get_mut(&session) {
                        held.inflight.remove(&packet_id);
                    }
                }
                (
                    Record::InflightRemoved {
                        session: keys[session].clone(),
                        packet_id,
                    },
                    refusal,
                )
            }
            Op::RetainedPut { topic, payload } => (
                Record::RetainedPut {
                    topic: TOPICS[topic].to_owned(),
                    message: RetainedRecord {
                        payload: vec![payload; 1 + usize::from(payload % 4)].into(),
                        qos: qos_of(payload % 3),
                        expires_at: (payload % 2 == 0).then_some(1_700_000_500),
                        properties: bytes::Bytes::from_static(&[0x01, 0x00]),
                    },
                },
                None,
            ),
            Op::RetainedRemoved { topic } => (
                Record::RetainedRemoved {
                    topic: TOPICS[topic].to_owned(),
                },
                None,
            ),
            Op::RulePut {
                rule,
                enabled,
                shape,
            } => {
                if apply {
                    if !self.rules.insert(rule) {
                        coverage.a_rule_put_over_one_already_held += 1;
                        // A put replaces the rule whole and leaves the
                        // outbox alone, which is what makes turning a
                        // connector off and on again safe.
                        if !self.outbox(rule).entries.is_empty() {
                            coverage.a_rule_replaced_while_its_outbox_held_envelopes += 1;
                        }
                    } else if let Some(mark) = self.rules_removed_with_a_mark.remove(&rule) {
                        if mark > 0 {
                            coverage.a_rule_recreated_after_its_outbox_was_removed += 1;
                        }
                    }
                }
                (
                    Record::RulePut {
                        rule: rule_record(rule, enabled, shape),
                    },
                    None,
                )
            }
            Op::RuleRemoved { rule } => {
                if apply && self.rules.remove(&rule) {
                    coverage.a_rule_removed_while_held += 1;
                    // The outbox goes with the rule, counters included.
                    let held = self.outboxes.remove(&rule).unwrap_or_default();
                    if !held.entries.is_empty() {
                        coverage.a_rule_removed_while_its_outbox_held_envelopes += 1;
                    }
                    self.rules_removed_with_a_mark.insert(rule, held.highest);
                }
                (
                    Record::RuleRemoved {
                        id: RULES[rule].to_owned(),
                    },
                    None,
                )
            }
            Op::OutboxAppended {
                rule,
                body,
                gap,
                shape,
                reuse,
            } => {
                let outbox = self.outbox(rule);
                // A reuse names the highest number this outbox has ever
                // assigned, which is zero for one that has never been
                // appended to and is not a number an append may take
                // either. Both are refused, and both are the same
                // refusal.
                let sequence = if reuse {
                    outbox.highest
                } else {
                    outbox.highest + 1 + gap
                };
                let refusal = if self.rules.contains(&rule) {
                    (sequence <= outbox.highest).then_some(Refused::OutboxSequenceReused)
                } else {
                    Some(Refused::NoSuchRule)
                };
                if apply && refusal.is_none() {
                    let held = self.outboxes.entry(rule).or_default();
                    held.entries.insert(sequence);
                    held.highest = sequence;
                    if a_little_endian_key_would_reorder(held.entries.iter().copied()) {
                        coverage.an_outbox_a_little_endian_key_would_reorder += 1;
                    }
                }
                (
                    Record::OutboxAppended {
                        rule: RULES[rule].to_owned(),
                        entry: outbox_record(sequence, body, shape),
                    },
                    refusal,
                )
            }
            Op::OutboxAttempted {
                rule,
                at,
                attempts,
                next_attempt_at,
            } => {
                let refusal = (!self.rules.contains(&rule)).then_some(Refused::NoSuchRule);
                let sequence = self.outbox_sequence(rule, at);
                if apply && refusal.is_none() {
                    // Nothing in the model moves: the attempt count and
                    // the deadline are fields of the stored envelope,
                    // and what the model needs is which sequences are
                    // held. What is counted is which of the two cases
                    // this was, because the no-op is the one a backend
                    // could get wrong by putting the envelope back.
                    if self.outbox(rule).entries.contains(&sequence) {
                        coverage.an_attempt_on_an_envelope_still_there += 1;
                    } else {
                        coverage.an_attempt_on_an_envelope_that_has_left += 1;
                    }
                }
                (
                    Record::OutboxAttempted {
                        rule: RULES[rule].to_owned(),
                        sequence,
                        attempts,
                        next_attempt_at,
                    },
                    refusal,
                )
            }
            Op::OutboxRemoved { rule, at } => {
                let refusal = (!self.rules.contains(&rule)).then_some(Refused::NoSuchRule);
                let sequence = self.outbox_sequence(rule, at);
                if apply && refusal.is_none() {
                    if self
                        .outboxes
                        .entry(rule)
                        .or_default()
                        .entries
                        .remove(&sequence)
                    {
                        coverage.a_removal_of_an_envelope_still_there += 1;
                    } else {
                        // The depth must not move. A backend that
                        // subtracted anyway would report an outbox
                        // empty while it still held envelopes, and it
                        // would stop draining.
                        coverage.a_removal_of_a_sequence_not_held += 1;
                    }
                }
                (
                    Record::OutboxRemoved {
                        rule: RULES[rule].to_owned(),
                        sequence,
                    },
                    refusal,
                )
            }
            Op::EndBatch => unreachable!("a batch boundary is not a record"),
        }
    }

    /// Whether a record naming this session will be accepted.
    ///
    /// A record for a session the store does not hold is refused, which
    /// is what makes filing state under a missing session impossible
    /// rather than merely unwise.
    fn holds(&self, session: usize) -> bool {
        self.sessions.contains_key(&session)
    }

    /// Why a record naming this session would be refused, if it would.
    fn session_refusal(&self, session: usize) -> Option<Refused> {
        (!self.holds(session)).then_some(Refused::NoSuchSession)
    }

    /// This rule's outbox as the model has it, or an empty one.
    ///
    /// An outbox exists for a rule the store holds whether or not
    /// anything was ever appended to it, which is why a rule with no
    /// entry still answers a depth of zero rather than a refusal.
    fn outbox(&self, rule: usize) -> OutboxModel {
        self.outboxes.get(&rule).cloned().unwrap_or_default()
    }

    /// The sequence an attempt or a removal names.
    ///
    /// `None` is the head, which is what a pump touches. An outbox with
    /// nothing in it is named at one past its high-water mark, which is
    /// a number it has never assigned: that is the no-op the two
    /// backends have to agree about, and it must not move the depth.
    fn outbox_sequence(&self, rule: usize, at: Option<u64>) -> u64 {
        let outbox = self.outbox(rule);
        match at {
            Some(sequence) => sequence,
            None => outbox
                .entries
                .iter()
                .next()
                .copied()
                .unwrap_or_else(|| outbox.highest.saturating_add(1)),
        }
    }
}

/// Whether a little-endian key would put these sequence numbers in a
/// different order from the one they are in.
///
/// This is the detectability condition for the single mistake the redb
/// backend can make that nothing else would catch. Two sequences that
/// share every byte but the lowest sort the same either way, so it is not
/// enough for a queue to be long or for its numbers to be large: the
/// numbers have to cross a byte boundary *and* invert.
fn a_little_endian_key_would_reorder(sequences: impl Iterator<Item = u64>) -> bool {
    let ordered: Vec<u64> = sequences.collect();
    let mut little_endian = ordered.clone();
    little_endian.sort_by_key(|sequence| sequence.to_le_bytes());
    little_endian != ordered
}

// ---- building the records -----------------------------------------------

/// A quality of service level from a small number.
fn qos_of(n: u8) -> Qos {
    match n % 3 {
        0 => Qos::AtMostOnce,
        1 => Qos::AtLeastOnce,
        _ => Qos::ExactlyOnce,
    }
}

/// A message identifier that differs from every other in its first byte.
fn body_id(body: usize) -> MessageId {
    let mut bytes = [0u8; 16];
    bytes[0] = u8::try_from(body).expect("the body universe is small");
    MessageId(bytes)
}

/// A body whose payload names it, so a swapped body is legible.
fn body_record(body: usize, payload: u8) -> BodyRecord {
    BodyRecord {
        topic: format!("sensor/{body}"),
        payload: format!("body-{body}-{payload}").into_bytes().into(),
        qos: qos_of(payload),
        retain: payload.is_multiple_of(2),
        expires_at: payload
            .is_multiple_of(3)
            .then_some(1_700_000_100 + u32::from(payload)),
        properties: bytes::Bytes::from_static(&[0x01, 0x00]),
    }
}

/// The per-subscriber part of a delivery.
fn delivery(body: usize, sequence: u64) -> DeliveryRecord {
    DeliveryRecord {
        body: body_id(body),
        qos: qos_of(u8::try_from(sequence % 3).expect("a remainder of three fits")),
        retain: sequence.is_multiple_of(2),
        subscription_ids: if sequence.is_multiple_of(4) {
            Vec::new()
        } else {
            vec![u32::try_from(sequence % 7).expect("a remainder of seven fits") + 1]
        },
        dup: sequence.is_multiple_of(5),
    }
}

/// A rule whose every field depends on the operation that made it, so
/// that a backend keeping a stale one is visible.
fn rule_record(rule: usize, enabled: bool, shape: u8) -> RuleRecord {
    RuleRecord {
        id: RULES[rule].to_owned(),
        name: format!("rule {rule} shape {shape}"),
        enabled,
        trigger: TriggerRecord {
            filter: FILTERS[usize::from(shape) % FILTERS.len()].to_owned(),
            min_qos: shape.is_multiple_of(2).then(|| qos_of(shape % 3)),
            client_id: shape.is_multiple_of(3).then(|| format!("client-{shape}-*")),
        },
        action: ActionRecord::HttpOut(HttpOutRecord {
            url: format!("https://example.invalid/{rule}/{shape}"),
            method: if shape.is_multiple_of(2) {
                "POST"
            } else {
                "PUT"
            }
            .to_owned(),
            headers: (0..usize::from(shape) % 3)
                .map(|n| (format!("x-{n}"), format!("v-{shape}-{n}")))
                .collect(),
            timeout_ms: 1_000 + u32::from(shape),
            max_concurrency: u16::from(shape) % 9 + 1,
            retries: shape % 7,
            backoff_ms: 250 + u32::from(shape) * 3,
        }),
    }
}

/// An envelope whose every field depends on the operation that made it.
///
/// The identifier is derived from the sequence rather than minted, so
/// that the two backends are given the same envelope and a difference
/// between them is theirs. It is distinct from every body identifier
/// because its first byte is the sequence and its second says which of
/// the two kinds of identifier it is; a store that handed back a
/// delivery identifier where an envelope's belongs would otherwise be
/// invisible.
fn outbox_record(sequence: u64, body: usize, shape: u8) -> OutboxRecord {
    let mut id = [0u8; 16];
    id[0] = u8::try_from(sequence % 251).expect("a remainder of 251 fits");
    id[1] = 0xE1;
    id[2] = shape;
    OutboxRecord {
        sequence,
        id: MessageId(id),
        body: body_id(body),
        client_id: format!("sensor-{shape}"),
        enqueued_at: 1_700_000_000 + u32::from(shape),
        // Zero for an envelope that has never been tried, which is what
        // an append always is: the count and the deadline only move on
        // an attempt, and an append that carried them would be this
        // test agreeing with a backend that ignored the distinction.
        attempts: 0,
        next_attempt_at: 0,
    }
}

/// A session record with fields that are distinguishable from the
/// defaults, so that an implementation dropping one is visible.
fn session_record(key: &SessionKey, expiry: u32, will: bool, five: bool) -> SessionRecord {
    SessionRecord {
        key: key.clone(),
        version: if five {
            ProtocolVersion::V5
        } else {
            ProtocolVersion::V311
        },
        expiry_interval: expiry,
        expires_at: (expiry > 0).then_some(1_700_000_000 + expiry),
        will: will.then(|| WillRecord {
            topic: format!("will/{}", key.client_id),
            payload: bytes::Bytes::from_static(b"gone"),
            qos: Qos::AtLeastOnce,
            retain: true,
            delay: expiry,
            properties: bytes::Bytes::from_static(&[0x01, 0x00]),
        }),
        will_at: will.then_some(1_700_000_030),
    }
}

/// Turn a plan into the batches it means.
///
/// Batches are closed at an [`Op::EndBatch`] and at [`BATCH`] records,
/// whichever comes first. A batch containing a record the store will
/// refuse is still emitted in full: the store stops at the refusal and
/// discards the transaction, and that both backends discard exactly the
/// same thing is part of what is being tested.
fn compile(plan: &[Step]) -> Compiled {
    let mut committed = Model::default();
    let mut coverage = Coverage::default();
    let mut batches: Vec<Vec<Record>> = Vec::new();

    let mut staged = committed.clone();
    let mut staged_coverage = coverage;
    let mut batch: Vec<Record> = Vec::new();
    let mut refused = false;

    for step in plan {
        if !matches!(step.op, Op::EndBatch) {
            let (record, refusal) = staged.step(step, !refused, &mut staged_coverage);
            if let (Some(refusal), false) = (refusal, refused) {
                refused = true;
                // Counted here rather than in the model, and into the
                // run's own counters rather than the staged ones: a
                // refusal is exactly the case whose staged counters are
                // about to be thrown away with the batch.
                match refusal {
                    Refused::NoSuchSession => coverage.a_record_for_a_session_not_held += 1,
                    Refused::BodyNotHeld => coverage.release_of_a_body_not_held += 1,
                    Refused::NoSuchRule => coverage.an_outbox_record_for_a_rule_not_held += 1,
                    Refused::OutboxSequenceReused => {
                        coverage.an_append_at_a_number_already_spent += 1;
                    }
                }
                if !batch.is_empty() {
                    coverage.a_batch_refused_after_earlier_records += 1;
                }
            }
            batch.push(record);
        }
        if matches!(step.op, Op::EndBatch) || batch.len() >= BATCH {
            if !batch.is_empty() {
                batches.push(std::mem::take(&mut batch));
            }
            if !refused {
                committed = staged.clone();
                coverage = staged_coverage;
            }
            staged = committed.clone();
            staged_coverage = coverage;
            refused = false;
        }
    }
    if !batch.is_empty() {
        batches.push(batch);
        if !refused {
            coverage = staged_coverage;
        }
    }

    Compiled { batches, coverage }
}

// ---- the comparison -----------------------------------------------------

/// Everything two stores must agree on.
///
/// [`Restored`] first: field by field for the failure message, and then
/// as one equality, which is what says the field-wise list is complete
/// and that both backends produce one total order rather than five sets.
///
/// Then the three accessors, which a restore cannot see. The depth in
/// particular is a stored counter rather than part of [`Restored`], so a
/// backend that let it drift would restore every entry correctly and
/// still report a queue of the wrong length — which is exactly what the
/// fourth injected fault did.
///
/// [`Restored`]: storage::Restored
fn agree(model: &dyn Storage, disk: &dyn Storage, when: &str) -> TestCaseResult {
    let expected = model.restore().expect("the model restores");
    let found = disk.restore().expect("the store restores");

    prop_assert_eq!(
        expected.sessions.len(),
        found.sessions.len(),
        "{}: the two stores restored different numbers of sessions",
        when
    );
    for (expected, found) in expected.sessions.iter().zip(&found.sessions) {
        prop_assert_eq!(
            &expected.session,
            &found.session,
            "{}: a session's own fields differ",
            when
        );
        prop_assert_eq!(
            &expected.subscriptions,
            &found.subscriptions,
            "{}: the subscriptions of {:?} differ",
            when,
            expected.session.key
        );
        prop_assert_eq!(
            &expected.inbound,
            &found.inbound,
            "{}: the inbound identifiers of {:?} differ",
            when,
            expected.session.key
        );
        prop_assert_eq!(
            &expected.inflight,
            &found.inflight,
            "{}: the inflight entries of {:?} differ",
            when,
            expected.session.key
        );
        prop_assert_eq!(
            &expected.queue,
            &found.queue,
            "{}: the queue of {:?} differs, in contents or in order",
            when,
            expected.session.key
        );
        prop_assert_eq!(
            expected.highest_sequence,
            found.highest_sequence,
            "{}: the high-water mark of {:?} differs",
            when,
            expected.session.key
        );
    }
    prop_assert_eq!(
        &expected.retained,
        &found.retained,
        "{}: the retained messages differ",
        when
    );
    prop_assert_eq!(
        &expected.bodies,
        &found.bodies,
        "{}: the bodies differ",
        when
    );
    prop_assert_eq!(&expected.rules, &found.rules, "{}: the rules differ", when);
    // Nothing above may have been forgotten. The field-wise assertions
    // exist for the failure message; this one is what says the list of
    // them is complete, and it is also the claim that both backends
    // produce one total order rather than five sets.
    prop_assert_eq!(&expected, &found, "{}: the restored stores differ", when);

    let mut keys = sessions();
    keys.push(a_session_no_plan_creates());
    for key in &keys {
        prop_assert_eq!(
            model.queue_depth(key).expect("a depth"),
            disk.queue_depth(key).expect("a depth"),
            "{}: the depth of {:?} differs",
            when,
            key
        );
        prop_assert_eq!(
            model.highest_sequence(key).expect("a mark"),
            disk.highest_sequence(key).expect("a mark"),
            "{}: the high-water mark of {:?} differs",
            when,
            key
        );
        // Cursors either side of the byte boundaries the queue key has
        // to get right, and one past everything.
        for cursor in [0u64, 1, 2, 255, 256, 1_000, 65_536, u64::MAX] {
            prop_assert_eq!(
                model.queue_head(key, cursor).expect("a head"),
                disk.queue_head(key, cursor).expect("a head"),
                "{}: the head of {:?} at cursor {} differs",
                when,
                key,
                cursor
            );
        }
    }

    // The outboxes, which `Restored` does not carry at all, so all of
    // this is reachable only through the accessors. Over a rule no plan
    // ever created as well, so that an outbox nobody filled is compared
    // like one somebody did.
    let mut rules: Vec<&str> = RULES.to_vec();
    rules.push(A_RULE_NO_PLAN_CREATES);
    for rule in rules {
        prop_assert_eq!(
            model.outbox_depth(rule).expect("a depth"),
            disk.outbox_depth(rule).expect("a depth"),
            "{}: the depth of {:?}'s outbox differs",
            when,
            rule
        );
        prop_assert_eq!(
            model.outbox_highest_sequence(rule).expect("a mark"),
            disk.outbox_highest_sequence(rule).expect("a mark"),
            "{}: the high-water mark of {:?}'s outbox differs",
            when,
            rule
        );
        for cursor in [0u64, 1, 2, 255, 256, 1_000, 65_536, u64::MAX] {
            prop_assert_eq!(
                model.outbox_head(rule, cursor).expect("a head"),
                disk.outbox_head(rule, cursor).expect("a head"),
                "{}: the head of {:?}'s outbox at cursor {} differs",
                when,
                rule,
                cursor
            );
        }
        // And every envelope, in the order the key puts them in. The
        // fixed cursors above are the byte boundaries; this is the
        // whole outbox, and it is the only comparison here that would
        // see an envelope the two backends disagree about the contents
        // of.
        prop_assert_eq!(
            outbox_entries(model, rule),
            outbox_entries(disk, rule),
            "{}: the envelopes in {:?}'s outbox differ, in contents or in order",
            when,
            rule
        );
    }
    Ok(())
}

/// Every envelope in a rule's outbox, walked from the start by cursor.
///
/// [`Storage::outbox_head`] is the only way to read one, so a whole
/// outbox is a walk: the head at zero, then the head past the sequence
/// that came back, and so on. Bounded and checked rather than trusted,
/// because a backend that handed back a sequence at or below the cursor
/// it was given would otherwise be an infinite loop instead of a
/// failure.
fn outbox_entries(store: &dyn Storage, rule: &str) -> Vec<OutboxRecord> {
    let mut entries: Vec<OutboxRecord> = Vec::new();
    let mut cursor = 0u64;
    while let Some(entry) = store.outbox_head(rule, cursor).expect("a head") {
        assert!(
            entry.sequence >= cursor,
            "{rule}'s outbox answered the cursor {cursor} with the earlier sequence {}",
            entry.sequence
        );
        assert!(
            entries.len() < 1_000,
            "{rule}'s outbox did not end: a plan cannot append a thousand envelopes"
        );
        let Some(next) = entry.sequence.checked_add(1) else {
            entries.push(entry);
            break;
        };
        cursor = next;
        entries.push(entry);
    }
    entries
}

// ---- the property -------------------------------------------------------

proptest! {
    // Fewer, longer cases. The history is what produces the cases that
    // matter, and a store on disk is opened, written and reopened once
    // per case, which is the cost that decides this number.
    #![proptest_config(ProptestConfig {
        cases: 128,
        ..ProptestConfig::default()
    })]

    /// Apply the same records to both backends, then restart the one that
    /// can be restarted. Neither the restart nor the engine may make any
    /// difference to what comes back.
    #[test]
    fn a_restart_is_indistinguishable_from_not_having_stopped(plan in any_plan()) {
        let compiled = compile(&plan);
        let directory = tempfile::TempDir::new().expect("a temporary directory");
        let path = directory.path().join("store.redb");

        let model = MemoryStorage::new();
        let disk = RedbStorage::open(&path).expect("a store opens");

        for (index, batch) in compiled.batches.iter().enumerate() {
            // The refusal itself is compared, not merely whether there
            // was one: convention 2, a category is not the check.
            let in_memory = model.apply(batch).err().map(|error| error.to_string());
            let on_disk = disk.apply(batch).err().map(|error| error.to_string());
            prop_assert_eq!(
                &in_memory,
                &on_disk,
                "batch {} of {} was not treated the same way by both backends: {:?}",
                index,
                compiled.batches.len(),
                batch
            );
        }

        disk.flush().expect("a flush succeeds");
        agree(&model, &disk, "with the store still open")?;

        // And again through the file. A clean close writes out everything
        // redb holds, flushed or not, so this proves nothing about *when*
        // a write became durable — see the module documentation. What it
        // does prove is that everything came back from bytes on disk, in
        // the order the key encoding put them in, rather than from
        // whatever the open store had in hand.
        drop(disk);
        let reopened = RedbStorage::open(&path).expect("the store opens again");
        agree(&model, &reopened, "after closing and reopening")?;
    }
}

/// The generator can produce every case this test would be worthless
/// without.
///
/// Convention 5: a generator that cannot produce a case is how a property
/// test quietly becomes decorative, and it passed for a year once. This
/// runs the generator with a deterministic runner — the same plans every
/// time, so a failure here is a defect and never a bad seed — and fails
/// naming whichever case came out zero.
#[test]
fn the_generator_produces_every_case_the_property_needs() {
    const PLANS: usize = 128;

    let mut runner = TestRunner::deterministic();
    let strategy = any_plan();
    let mut total = Coverage::default();
    let mut records = 0usize;
    let mut batches = 0usize;

    for _ in 0..PLANS {
        let plan = strategy
            .new_tree(&mut runner)
            .expect("the generator produces a plan")
            .current();
        let compiled = compile(&plan);
        batches += compiled.batches.len();
        records += compiled.batches.iter().map(Vec::len).sum::<usize>();
        total.add(compiled.coverage);
    }

    println!("{PLANS} plans, {batches} batches, {records} records");
    for (case, count) in total.named() {
        println!("  {count:>6}  {case}");
    }
    for (case, count) in total.named() {
        assert!(
            count > 0,
            "the generator cannot produce {case}, so the property does not test it"
        );
    }
}
