//! Tests for this node's own operation log.
//!
//! What the log owes the exchange is one sentence: **a peer that asks for
//! everything after a sequence it was told about must receive every entry
//! between the two.** Everything here is that sentence from a different
//! angle — a hole held back, a reserved write invisible, a discarded
//! number reused, a resumed log that does not hand out a number twice.
//!
//! Nothing here sends a frame. The exchange that uses this is
//! [`crate::replicate`]'s, and the digest it builds is a claim about what
//! these methods return.

use super::*;

use crate::clock::{HybridClock, DEFAULT_MAX_DRIFT_MILLIS};
use crate::identity::NodeId;
use crate::metadata::{MetadataKey, MetadataValue};
use crate::register::{Register, Stamp};

/// The node every write here is stamped by.
fn me() -> NodeId {
    NodeId::from_bytes([7; 16])
}

/// A clock nothing here drifts past.
fn clock() -> HybridClock {
    HybridClock::resume(None, DEFAULT_MAX_DRIFT_MILLIS)
}

/// A write of a rule named after `id`, stamped by `clock`.
///
/// Stamped from a real clock rather than by hand, for the reason
/// `crate::register`'s tests give: two writes sharing a stamp is a
/// condition this design depends on and a constructed stamp can break it.
fn write(clock: &HybridClock, id: &str) -> MetadataWrite {
    MetadataWrite::new(
        MetadataKey::Rule { id: id.to_owned() },
        Register::written(
            MetadataValue::Rule(Box::new(rule(id))),
            Stamp::new(clock.local(1_700_000_000_000), me()),
        ),
    )
}

/// A tombstone for `id`, stamped by `clock`.
fn tombstone(clock: &HybridClock, id: &str) -> MetadataWrite {
    MetadataWrite::new(
        MetadataKey::Rule { id: id.to_owned() },
        Register::deleted(Stamp::new(clock.local(1_700_000_000_000), me())),
    )
}

fn rule(id: &str) -> storage::RuleRecord {
    storage::RuleRecord {
        id: id.to_owned(),
        name: id.to_owned(),
        enabled: true,
        trigger: storage::TriggerRecord {
            filter: "a/b".to_owned(),
            min_qos: None,
            client_id: None,
        },
        action: storage::ActionRecord::HttpOut(storage::HttpOutRecord {
            url: "https://example.invalid/hook".to_owned(),
            method: "POST".to_owned(),
            headers: Vec::new(),
            timeout_ms: 1_000,
            max_concurrency: 1,
            retries: 0,
            backoff_ms: 10,
        }),
    }
}

/// Reserve, publish, and hand back the sequence.
fn logged(log: &mut OpLog, write: MetadataWrite) -> u64 {
    let sequence = log.reserve(write);
    log.publish(sequence).expect("it was reserved");
    sequence
}

/// Which sequences a peer at `from` would be served.
fn served(log: &OpLog, from: u64, limit: usize) -> Vec<u64> {
    log.after(from, limit)
        .iter()
        .map(Logged::sequence)
        .collect()
}

// ---- numbering -----------------------------------------------------------

/// A fresh log numbers from one and hands each number out once.
///
/// One rather than zero because zero is "nothing of yours" in a digest;
/// see [`FIRST_SEQUENCE`].
#[test]
fn a_fresh_log_numbers_from_one_and_never_repeats_a_number() {
    let clock = clock();
    let mut log = OpLog::default();
    assert_eq!(log.published_through(), 0, "a fresh log has published none");
    assert_eq!(log.high_water(), 0);

    let first = logged(&mut log, write(&clock, "a"));
    let second = logged(&mut log, write(&clock, "b"));
    let third = logged(&mut log, write(&clock, "c"));
    assert_eq!(
        (first, second, third),
        (FIRST_SEQUENCE, FIRST_SEQUENCE + 1, FIRST_SEQUENCE + 2)
    );
    assert_eq!(log.published_through(), 3);
    assert_eq!(log.high_water(), 3);
    assert_eq!(served(&log, 0, 10), vec![1, 2, 3]);
}

/// A reserved write is served to nobody until it is published.
///
/// The two phases, from the peer's side. A broadcast is irrevocable and an
/// exchange is a promise about what this node holds, so neither may happen
/// before the store has taken the batch: a peer told about a register
/// whose batch was refused holds a value this node will never hold again,
/// and the exchange cannot repair it because the exchange serves this log
/// and this log lost the entry in the same refused batch.
#[test]
fn a_reserved_write_is_served_to_nobody_until_it_is_published() {
    let clock = clock();
    let mut log = OpLog::default();
    let sequence = log.reserve(write(&clock, "a"));

    assert_eq!(
        sequence, FIRST_SEQUENCE,
        "it is numbered when it is reserved"
    );
    assert_eq!(
        log.published_through(),
        0,
        "a reserved write was offered to a peer before the store had taken it"
    );
    assert_eq!(served(&log, 0, 10), Vec::<u64>::new());
    assert!(log.is_empty());
    assert_eq!(
        log.high_water(),
        1,
        "the number is spent the moment it is handed out, or two writes would share it"
    );

    let published = log.publish(sequence).expect("it was reserved");
    assert_eq!(published.sequence(), sequence);
    assert_eq!(log.published_through(), 1);
    assert_eq!(served(&log, 0, 10), vec![1]);
    assert_eq!(
        log.publish(sequence),
        None,
        "publishing the same sequence twice reported something happened"
    );
}

/// A discarded number is used again, and nothing above it is stranded.
///
/// The refusal path, and the reason a discarded number is not simply left
/// spent: a hole stops [`OpLog::published_through`], so every entry above
/// it would be unreachable by any peer until a later write happened to
/// land on exactly that number.
#[test]
fn a_discarded_number_is_used_again_and_nothing_is_stranded() {
    let clock = clock();
    let mut log = OpLog::default();
    let first = log.reserve(write(&clock, "a"));
    let second = log.reserve(write(&clock, "b"));

    // The later batch lands and the earlier one is refused, which is the
    // order two writes from two locks can reach a store in.
    log.publish(second).expect("it was reserved");
    assert!(log.discard(first), "it was reserved");
    assert_eq!(
        log.published_through(),
        0,
        "an entry above a hole was offered to a peer, which would set its watermark \
         past an entry it never received"
    );
    assert_eq!(served(&log, 0, 10), Vec::<u64>::new());

    // And the next write fills the hole rather than going on top of it.
    let third = log.reserve(write(&clock, "c"));
    assert_eq!(third, first, "the discarded number was left spent");
    log.publish(third).expect("it was reserved");
    assert_eq!(log.published_through(), 2);
    assert_eq!(served(&log, 0, 10), vec![1, 2]);

    assert!(
        !log.discard(first),
        "discarding something published reported something happened"
    );
}

// ---- what a peer is served ----------------------------------------------

/// A batch is bounded, and the next one carries on where it stopped.
#[test]
fn a_batch_is_bounded_and_the_next_one_carries_on() {
    let clock = clock();
    let mut log = OpLog::default();
    for which in 0..10 {
        logged(&mut log, write(&clock, &format!("rule-{which}")));
    }
    assert_eq!(served(&log, 0, 4), vec![1, 2, 3, 4]);
    assert_eq!(served(&log, 4, 4), vec![5, 6, 7, 8]);
    assert_eq!(served(&log, 8, 4), vec![9, 10]);
    assert_eq!(served(&log, 10, 4), Vec::<u64>::new());
    assert_eq!(
        served(&log, 0, 0),
        Vec::<u64>::new(),
        "a bound of nothing serves nothing rather than everything"
    );
}

/// A peer that has everything is served nothing.
///
/// The claim the digest exists for. A node that answered a digest it had
/// nothing to add to with its whole log would make the exchange a
/// periodic full resynchronisation.
#[test]
fn a_peer_that_has_everything_is_served_nothing() {
    let clock = clock();
    let mut log = OpLog::default();
    logged(&mut log, write(&clock, "a"));
    logged(&mut log, write(&clock, "b"));
    assert_eq!(
        served(&log, log.published_through(), 100),
        Vec::<u64>::new()
    );
}

/// The reading at a sequence is the stamp the write carried.
///
/// What section 8.2's tombstone watermark is expressed in: a peer reports
/// how far along this log it has read, and this is what turns that number
/// into the reading below which this node's writes have reached
/// everybody.
#[test]
fn the_reading_at_a_sequence_is_the_stamp_that_write_carried() {
    let clock = clock();
    let mut log = OpLog::default();
    let one = write(&clock, "a");
    let two = tombstone(&clock, "a");
    let first = logged(&mut log, one.clone());
    let second = logged(&mut log, two.clone());

    assert_eq!(log.reading_at(first), Some(one.stamp().reading()));
    assert_eq!(log.reading_at(second), Some(two.stamp().reading()));
    assert!(log.reading_at(second).unwrap() > log.reading_at(first).unwrap());
    assert_eq!(log.reading_at(99), None);
    let reserved = log.reserve(write(&clock, "c"));
    assert_eq!(
        log.reading_at(reserved),
        None,
        "a reserved write's reading was readable, so an unpublished stamp is visible"
    );
}

// ---- collection ----------------------------------------------------------

/// Collection takes what it names and leaves the numbers spent.
#[test]
fn collection_takes_what_it_names_and_leaves_the_numbers_spent() {
    let clock = clock();
    let mut log = OpLog::default();
    for which in 0..4 {
        logged(&mut log, write(&clock, &format!("rule-{which}")));
    }

    assert_eq!(log.collect_through(2), 2);
    assert_eq!(served(&log, 0, 10), vec![3, 4]);
    assert_eq!(
        log.published_through(),
        4,
        "collecting what every node holds moved what a peer is told it may ask past"
    );
    assert_eq!(log.high_water(), 4);

    // Twice, and past the end, are both no-ops on the numbers.
    assert_eq!(log.collect_through(2), 0);
    assert_eq!(log.collect_through(99), 2);
    assert!(log.is_empty());
    assert_eq!(
        log.high_water(),
        4,
        "an emptied log forgot what it had spent"
    );
    let next = log.reserve(write(&clock, "after"));
    assert_eq!(
        next, 5,
        "the next number must be past everything this log has ever numbered, and no \
         further: a watermark past the high-water mark is a peer's arithmetic rather \
         than a fact about this log"
    );
}

// ---- resuming ------------------------------------------------------------

/// A resumed log never hands out a number it has already used.
///
/// The same defect as a clock resuming from zero, in the other of section
/// 8.2's two counters: a log that numbered its next write one would tell a
/// peer "this is my first write" about a write the peer already has under
/// that number, and the peer would never ask for either of them again.
#[test]
fn a_resumed_log_never_hands_out_a_number_it_has_already_used() {
    let clock = clock();
    let mut before = OpLog::default();
    for which in 0..3 {
        logged(&mut before, write(&clock, &format!("rule-{which}")));
    }
    let stored: Vec<Logged> = before.after(0, usize::MAX);
    assert_eq!(stored.len(), 3);

    let mut after = OpLog::resume(stored, 0);
    assert_eq!(after.published_through(), 3);
    assert_eq!(after.high_water(), 3);
    assert_eq!(served(&after, 0, 10), vec![1, 2, 3]);
    assert_eq!(
        after.reserve(write(&clock, "next")),
        4,
        "a resumed log handed out a number it had already used"
    );
}

/// A log collected down to nothing still resumes past what it spent.
///
/// The case the stored high-water mark exists for, and the one a log that
/// counted its own entries would get wrong: in a healthy cluster almost
/// every entry has been collected, so the table a restart reads is empty
/// and says nothing about the numbers already served.
#[test]
fn a_log_collected_down_to_nothing_still_resumes_past_what_it_spent() {
    let clock = clock();
    let mut before = OpLog::default();
    for which in 0..5 {
        logged(&mut before, write(&clock, &format!("rule-{which}")));
    }
    before.collect_through(5);
    assert!(before.is_empty());

    let mut after = OpLog::resume(Vec::new(), 5);
    assert_eq!(after.high_water(), 5);
    assert_eq!(
        after.published_through(),
        5,
        "a resumed empty log must still tell a peer it may ask past what was collected"
    );
    assert_eq!(after.reserve(write(&clock, "next")), 6);
}

/// A hole a refused batch left behind holds a peer back and then closes.
///
/// A store can hold a hole: two writes numbered from two locks can reach
/// it in either order, and the later one's batch can be taken while the
/// earlier one's is refused. What must not happen is a resumed log
/// offering a peer the entry above the hole, because the peer would
/// advance its watermark past an entry nobody will ever send it.
#[test]
fn a_hole_a_refused_batch_left_behind_holds_a_peer_back_and_then_closes() {
    let clock = clock();
    let stored = vec![
        Logged::new(1, write(&clock, "a")),
        // Two is missing: its batch was refused.
        Logged::new(3, write(&clock, "c")),
    ];
    let mut log = OpLog::resume(stored, 0);
    assert_eq!(
        log.published_through(),
        1,
        "a resumed log offered a peer the entry above a hole"
    );
    assert_eq!(served(&log, 0, 10), vec![1]);
    assert_eq!(log.high_water(), 3);

    // The next write lands in the hole, and the entry above it becomes
    // reachable in the same moment.
    assert_eq!(
        log.reserve(write(&clock, "b")),
        2,
        "the next write did not fill the hole"
    );
    log.publish(2).expect("it was reserved");
    assert_eq!(log.published_through(), 3);
    assert_eq!(served(&log, 0, 10), vec![1, 2, 3]);
}
