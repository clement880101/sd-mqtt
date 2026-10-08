//! Tests for the clock two nodes compare.
//!
//! # The one that matters is the resume
//!
//! Everything else here is an example. [`resume`] is a property over a
//! restart, and it is a property deliberately: the example anybody would
//! reach for — stop a node, start it again a second later, check the next
//! stamp is bigger — is the one case a clock resuming from zero gets
//! right, because the wall clock moved on while it was down and rescued
//! it. The hazard is the restart where the wall clock did not: the same
//! millisecond, or a machine whose clock went backwards. A property over
//! generated wall readings reaches both, and
//! [`resume::the_generator_reaches_the_restart_the_wall_clock_does_not_rescue`]
//! is what says it does rather than hoping.

use super::*;

use storage::{MemoryStorage, Record, Storage};

/// The bound these tests use, in milliseconds.
///
/// Small and not [`DEFAULT_MAX_DRIFT_MILLIS`], so that a test reaching
/// past it does not have to write out a minute and so that an
/// implementation which had hard-coded the default would fail here.
const BOUND: u64 = 1_000;

/// A clock that has never issued anything.
fn fresh() -> HybridClock {
    HybridClock::resume(None, BOUND)
}

// ---- the reading ---------------------------------------------------------

/// The order is the milliseconds, then the counter.
///
/// Written with two readings whose counters sort the opposite way from
/// their milliseconds, so a comparison that reached for the counter
/// first would be caught. Section 8.2 names this order and the type's
/// field order is what implements it.
#[test]
fn a_reading_orders_by_milliseconds_and_then_by_the_counter() {
    let earlier = Reading::new(5, 900);
    let later = Reading::new(6, 0);
    assert!(
        earlier < later,
        "the millisecond decides first, whatever the counters say"
    );

    assert!(
        Reading::new(5, 1) < Reading::new(5, 2),
        "and inside one millisecond the counter decides"
    );
    assert_eq!(Reading::new(5, 2), Reading::new(5, 2));
}

/// A reading goes to a store and comes back unchanged, both halves.
///
/// Both halves asserted separately: a conversion that dropped the
/// counter would leave a resumed clock up to a millisecond's worth of
/// writes behind itself, and an equality on the pair would say only that
/// something was wrong.
#[test]
fn a_reading_round_trips_through_the_record_a_store_holds() {
    let reading = Reading::new(1_700_000_000_123, 7);
    let record = reading.to_record();
    assert_eq!(record.millis, 1_700_000_000_123);
    assert_eq!(record.counter, 7);
    assert_eq!(Reading::from_record(record), reading);
}

// ---- local events --------------------------------------------------------

/// The wall clock moving pulls the reading with it and restarts the
/// counter.
#[test]
fn the_wall_clock_moving_forward_moves_the_reading_and_resets_the_counter() {
    let clock = fresh();
    assert_eq!(clock.local(10), Reading::new(10, 0));
    assert_eq!(clock.local(11), Reading::new(11, 0));
}

/// Inside one millisecond, the counter is what moves.
///
/// The mutation this guards is "the counter not incremented inside one
/// millisecond", which makes every write in a busy millisecond share a
/// stamp. Three readings rather than two, because an implementation that
/// incremented once and then stopped would pass on two.
#[test]
fn the_counter_moves_inside_one_millisecond() {
    let clock = fresh();
    assert_eq!(clock.local(10), Reading::new(10, 0));
    assert_eq!(clock.local(10), Reading::new(10, 1));
    assert_eq!(clock.local(10), Reading::new(10, 2));
}

/// A wall clock that moves backwards does not move the reading
/// backwards.
///
/// An NTP step, an operator's correction, a virtual machine resumed from
/// a snapshot. The reading stands and the counter does the work.
#[test]
fn a_wall_clock_that_goes_backwards_does_not_take_the_reading_with_it() {
    let clock = fresh();
    assert_eq!(clock.local(1_000), Reading::new(1_000, 0));
    assert_eq!(
        clock.local(5),
        Reading::new(1_000, 1),
        "the millisecond stands and the counter moves"
    );
    assert_eq!(clock.local(5), Reading::new(1_000, 2));
}

/// A counter at its maximum advances the millisecond.
///
/// Four billion events inside one millisecond is unreachable, which is
/// exactly why this needs a test: nothing else will ever walk the
/// branch, and the alternatives are a wrapped counter — two writes
/// sharing a stamp — or a panic on a write path.
///
/// The clock is put there by resuming from a store, because that is the
/// only way in without four billion calls.
#[test]
fn the_counter_at_its_maximum_advances_the_millisecond() {
    let clock = HybridClock::resume(Some(Reading::new(10, u32::MAX).to_record()), BOUND);
    assert_eq!(
        clock.local(5),
        Reading::new(11, 0),
        "the millisecond advances rather than the counter wrapping"
    );
    assert_eq!(clock.local(5), Reading::new(11, 1));
}

/// The high-water mark is the last reading issued, and it is what a
/// batch carries.
#[test]
fn the_mark_is_the_highest_reading_issued() {
    let clock = fresh();
    assert_eq!(
        clock.high_water(),
        Reading::ZERO,
        "a clock that has issued nothing sits at zero, which it never issues"
    );
    let issued = clock.local(42);
    assert_eq!(clock.high_water(), issued);
    assert_eq!(clock.mark(), issued.to_record());
}

// ---- remote events -------------------------------------------------------

/// A peer ahead of us, but inside the bound, moves the clock.
///
/// And the reading that comes back is **greater than the peer's**, not
/// equal to it: the point of taking a peer's reading is that this node's
/// next write wins against the one that arrived.
#[test]
fn a_peer_ahead_within_the_bound_moves_the_clock_past_it() {
    let clock = fresh();
    clock.local(100);
    let peer = Reading::new(600, 4);

    let ours = clock.observe(100, peer).expect("inside the bound");
    assert!(
        ours > peer,
        "a reading taken from a peer must beat it: {ours:?} against {peer:?}"
    );
    assert_eq!(ours, Reading::new(600, 5));
    assert_eq!(clock.high_water(), ours, "and the clock kept it");
}

/// A peer beyond the bound is refused, and the clock is untouched.
///
/// The refusal is the half that is easy to get right. The untouched
/// clock is the half section 8.2 exists for: a clamping implementation
/// would also return something, and would have moved this node's clock
/// to a wrong year on the way, from which no later write anywhere could
/// win a conflict.
///
/// Asserted by comparing against a second clock that never saw the peer
/// at all, so "untouched" is a whole reading rather than one field.
#[test]
fn a_peer_beyond_the_bound_is_refused_and_leaves_the_clock_untouched() {
    let clock = fresh();
    let untouched = fresh();
    clock.local(100);
    untouched.local(100);

    // A wrong year, which is the mistake this bound is for.
    let wrong = Reading::new(100 + BOUND + 1, 0);
    let error = clock
        .observe(100, wrong)
        .expect_err("a peer past the bound must be refused");

    assert_eq!(
        error,
        ClockError::PeerTooFarAhead {
            peer: wrong,
            wall_millis: 100,
            max_drift_millis: BOUND,
        }
    );
    assert_eq!(
        clock.high_water(),
        untouched.high_water(),
        "the refused peer moved the clock, which is clamping wearing a refusal's name"
    );
    assert_eq!(
        clock.local(100),
        untouched.local(100),
        "and the next reading is the one it would have been"
    );
}

/// Exactly the bound is accepted; one past it is not.
///
/// A bound is a decision about a boundary, and a test either side of it
/// is the only thing that says which side the boundary is on.
#[test]
fn the_bound_itself_is_accepted_and_one_millisecond_past_it_is_not() {
    let clock = fresh();
    assert!(clock.observe(100, Reading::new(100 + BOUND, 0)).is_ok());
    assert!(fresh()
        .observe(100, Reading::new(100 + BOUND + 1, 0))
        .is_err());
}

/// The refusal names what the peer said, what we said and what we allow.
///
/// Three numbers, because the question an operator has is which of the
/// two machines is wrong, and a message naming only one of them cannot
/// answer it.
#[test]
fn the_refusal_names_both_clocks_and_the_bound() {
    let error = fresh()
        .observe(1_000, Reading::new(100_000, 0))
        .expect_err("past the bound");
    let said = error.to_string();
    for expected in ["100000", "1000", "99000"] {
        assert!(
            said.contains(expected),
            "the refusal must name {expected}: {said}"
        );
    }
}

/// A peer far behind changes nothing.
///
/// "Nothing" compared against a clock that saw no peer at all, rather
/// than against one field: a reading is two numbers and a test that
/// checked the millisecond would miss a counter jumped to the peer's.
#[test]
fn a_peer_far_behind_changes_nothing() {
    let clock = fresh();
    let untouched = fresh();
    clock.local(1_000);
    untouched.local(1_000);

    let behind = Reading::new(3, 9);
    let ours = clock
        .observe(1_000, behind)
        .expect("a peer behind is no danger");
    assert_eq!(
        ours,
        untouched.local(1_000),
        "seeing a peer behind us did something a local event would not have"
    );
    assert_eq!(clock.high_water(), untouched.high_water());
}

// ---- the properties ------------------------------------------------------

mod properties {
    use super::*;

    use proptest::prelude::*;

    /// Wall-clock readings a node might be handed, including repeats and
    /// steps backwards.
    ///
    /// A small range on purpose: the interesting cases are two events in
    /// one millisecond and a clock that goes backwards, and a wide range
    /// produces neither.
    fn walls() -> impl Strategy<Value = Vec<u64>> {
        proptest::collection::vec(0u64..8, 1..8)
    }

    proptest! {
        /// A reading never goes backwards, and two from one node are
        /// never equal.
        ///
        /// One assertion for both: strictly increasing says the readings
        /// are ordered *and* distinct, and distinctness is what section
        /// 8.2's convergence is conditional on.
        #[test]
        fn a_reading_never_goes_backwards_and_no_two_are_ever_equal(walls in walls()) {
            let clock = fresh();
            let mut issued = Vec::new();
            for wall in walls {
                issued.push(clock.local(wall));
            }
            for pair in issued.windows(2) {
                prop_assert!(
                    pair[0] < pair[1],
                    "two readings from one node were not strictly increasing: \
                     {:?} then {:?}",
                    pair[0],
                    pair[1]
                );
            }
        }

        /// The same, with a peer's readings mixed in.
        ///
        /// Including peers whose readings are behind, level with and
        /// ahead of ours, and some past the bound, which must be refused
        /// without disturbing the sequence. The local-only property
        /// above cannot see a remote event that moved the clock
        /// backwards.
        #[test]
        fn a_remote_event_never_moves_the_reading_backwards_either(
            events in proptest::collection::vec(
                (0u64..8, proptest::option::of((0u64..3_000, 0u32..4))),
                1..10,
            ),
        ) {
            let clock = fresh();
            let mut issued = Vec::new();
            for (wall, peer) in events {
                match peer {
                    None => issued.push(clock.local(wall)),
                    Some((millis, counter)) => {
                        let peer = Reading::new(millis, counter);
                        match clock.observe(wall, peer) {
                            Ok(ours) => {
                                prop_assert!(
                                    ours > peer,
                                    "a reading taken from a peer must beat it"
                                );
                                issued.push(ours);
                            }
                            Err(refused) => {
                                // Refused rather than clamped, so the
                                // clock must be where it was. The next
                                // local reading proves it, because it is
                                // compared against the sequence.
                                prop_assert!(
                                    peer.millis() > wall + BOUND,
                                    "refused a peer inside the bound: {refused}"
                                );
                            }
                        }
                    }
                }
            }
            for pair in issued.windows(2) {
                prop_assert!(pair[0] < pair[1], "{:?} then {:?}", pair[0], pair[1]);
            }
        }
    }
}

// ---- the resume ----------------------------------------------------------

/// A node resuming from the store never re-issues a stamp.
///
/// Section 8.2's operational requirement, and the reason the reading is
/// in the store at all. See this module's own documentation for why it is
/// a property rather than an example.
mod resume {
    use super::*;

    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::{TestCaseResult, TestRunner};

    /// Wall readings either side of a restart.
    ///
    /// Both drawn from the same small range, which is the point: the
    /// range is what lets the second list repeat the first's readings or
    /// fall below them. Two ranges chosen so the second was always
    /// higher would be the example this is deliberately not.
    fn restarts() -> impl Strategy<Value = (Vec<u64>, Vec<u64>)> {
        (
            proptest::collection::vec(0u64..8, 1..6),
            proptest::collection::vec(0u64..8, 1..6),
        )
    }

    /// Run a node across a restart and collect every reading it issued.
    ///
    /// The mark goes to the store **after every reading**, which is the
    /// contract: it rides in the batch of the write it stamps. The
    /// restart is a fresh clock built from what the store hands back, so
    /// nothing of the first clock survives except the row.
    fn readings_across_a_restart(before: &[u64], after: &[u64]) -> Vec<Reading> {
        let store = MemoryStorage::new();
        let mut issued = Vec::new();

        let clock = HybridClock::resume(None, BOUND);
        assert_eq!(
            store.restore().expect("a restore").clock,
            None,
            "a store nothing has written to holds no reading"
        );
        for wall in before {
            issued.push(clock.local(*wall));
            store
                .apply(&[Record::ClockHighWaterPut {
                    reading: clock.mark(),
                }])
                .expect("the mark must be storable");
        }

        // The restart. Everything the first clock knew is gone but the
        // row.
        let stored = store.restore().expect("a restore").clock;
        let clock = HybridClock::resume(stored, BOUND);
        for wall in after {
            issued.push(clock.local(*wall));
            store
                .apply(&[Record::ClockHighWaterPut {
                    reading: clock.mark(),
                }])
                .expect("the mark must be storable");
        }
        issued
    }

    /// What the property asserts, factored out so that the generator
    /// coverage test below can drive it with the same strategy.
    fn no_reading_is_ever_re_issued((before, after): &(Vec<u64>, Vec<u64>)) -> TestCaseResult {
        let issued = readings_across_a_restart(before, after);
        for pair in issued.windows(2) {
            prop_assert!(
                pair[0] < pair[1],
                "a restart re-issued or went behind a reading: {:?} then {:?}, \
                 from walls {:?} and then {:?}",
                pair[0],
                pair[1],
                before,
                after
            );
        }
        Ok(())
    }

    proptest! {
        /// The property. A clock resuming from zero fails it.
        #[test]
        fn a_clock_resuming_from_the_store_never_re_issues_a_reading(plan in restarts()) {
            no_reading_is_ever_re_issued(&plan)?;
        }
    }

    /// The generator reaches the restart the wall clock does not rescue.
    ///
    /// Convention 5's "a generator that cannot produce a case", pointed
    /// at the one case this property exists for. A clock resuming from
    /// zero survives a restart across which the wall clock moved
    /// strictly forward, because the millisecond it starts from is one
    /// it never used. It is caught only by a restart where the wall
    /// clock repeated a millisecond or went backwards, and if the
    /// generator stopped producing those the property would pass against
    /// a clock that resumed from nothing at all.
    ///
    /// So all three are counted, including the forward case: a generator
    /// that produced only hostile restarts would be testing something
    /// narrower than a restart.
    #[test]
    fn the_generator_reaches_the_restart_the_wall_clock_does_not_rescue() {
        let mut runner = TestRunner::deterministic();
        let strategy = restarts();

        let (mut backwards, mut level, mut forwards) = (0, 0, 0);
        for _ in 0..512 {
            let plan = strategy
                .new_tree(&mut runner)
                .expect("the strategy must produce a value")
                .current();
            let (before, after) = &plan;
            let highest = before.iter().copied().max().expect("at least one");
            let lowest = after.iter().copied().min().expect("at least one");
            if lowest < highest {
                backwards += 1;
            }
            if after.iter().any(|wall| before.contains(wall)) {
                level += 1;
            }
            if lowest > highest {
                forwards += 1;
            }
        }

        assert!(
            backwards > 0,
            "the generator never produced a restart across which the wall clock \
             went backwards, which is one of the two cases a clock resuming from \
             zero is caught by"
        );
        assert!(
            level > 0,
            "the generator never produced a restart across which the wall clock \
             repeated a millisecond, which is the other"
        );
        assert!(
            forwards > 0,
            "the generator only produced hostile restarts, so it is not testing \
             a restart"
        );
    }

    /// A clock resumes across a restart of the process, through a file.
    ///
    /// redb, opened twice with the store closed in between, which is the
    /// only shape of test that can fail if the row is written somewhere
    /// a reopen does not look. The property above runs against the
    /// in-memory backend, which cannot be reopened and so cannot answer
    /// this.
    ///
    /// **The wall clock is held still across the restart**, which is
    /// what makes the assertion mean something: a clock resuming from
    /// zero would issue the same millisecond with the counter back at
    /// nothing.
    #[test]
    fn a_clock_resumes_across_a_restart_of_the_store() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("sdmqtt.redb");
        let wall = 1_700_000_000_000;

        let before = {
            let store = storage::RedbStorage::open(&path).expect("the store opens");
            let clock = HybridClock::resume(
                store.restore().expect("a restore").clock,
                DEFAULT_MAX_DRIFT_MILLIS,
            );
            let mut issued = Vec::new();
            for _ in 0..3 {
                issued.push(clock.local(wall));
                store
                    .apply(&[Record::ClockHighWaterPut {
                        reading: clock.mark(),
                    }])
                    .expect("the mark must be storable");
            }
            store.flush().expect("a flush must succeed");
            issued
        };
        assert_eq!(
            before,
            vec![
                Reading::new(wall, 0),
                Reading::new(wall, 1),
                Reading::new(wall, 2)
            ]
        );

        // The store is closed, which is what makes the next open a
        // restart rather than a second read.
        let store = storage::RedbStorage::open(&path).expect("the store reopens");
        let clock = HybridClock::resume(
            store.restore().expect("a restore").clock,
            DEFAULT_MAX_DRIFT_MILLIS,
        );
        let after = clock.local(wall);
        assert_eq!(
            after,
            Reading::new(wall, 3),
            "the resumed clock went behind itself; a clock starting from zero \
             would have re-issued {:?}",
            before[0]
        );
        assert!(
            before.iter().all(|earlier| *earlier < after),
            "the resumed clock re-issued a reading it had already used"
        );
    }
}
