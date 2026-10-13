//! A follower's half of section 8.6, and the hole it must not skip.

use super::*;

use storage::record::{DeliveryRecord, MessageId, Qos};

fn entry(sequence: u64) -> QueueEntry {
    QueueEntry {
        sequence,
        packet_id: None,
        delivery: DeliveryRecord {
            body: MessageId([u8::try_from(sequence % 251).unwrap_or(0); 16]),
            qos: Qos::AtLeastOnce,
            retain: false,
            subscription_ids: Vec::new(),
            dup: false,
        },
    }
}

fn appended(sequence: u64) -> Change {
    Change::Appended {
        entry: Box::new(entry(sequence)),
        body: None,
    }
}

#[test]
fn operations_in_order_are_taken_and_the_mark_follows_them() {
    let mut follower = Follower::new();
    assert_eq!(follower.through(), 0, "nothing applied is zero, not one");
    for at in 1..=5 {
        assert_eq!(follower.offer(at, appended(at)), Applied::Take);
        assert!(follower.release().is_empty(), "nothing was waiting");
        assert_eq!(follower.through(), at);
    }
}

#[test]
fn the_mark_does_not_move_past_a_hole() {
    // **The assertion this module exists for.** A follower that
    // applied position three while two never arrived would hold a
    // queue with a message missing from the middle and report a mark
    // saying it held everything — and failover reads that mark to
    // choose which copy to take, so it would prefer the broken one.
    let mut follower = Follower::new();
    assert_eq!(follower.offer(1, appended(1)), Applied::Take);
    assert_eq!(follower.offer(3, appended(3)), Applied::Held);
    assert_eq!(
        follower.through(),
        1,
        "three arrived, but two did not, so this copy is still through one"
    );
    assert_eq!(follower.offer(4, appended(4)), Applied::Held);
    assert_eq!(follower.through(), 1);
}

#[test]
fn filling_a_hole_releases_what_was_waiting_behind_it_in_order() {
    let mut follower = Follower::new();
    assert_eq!(follower.offer(1, appended(1)), Applied::Take);
    assert_eq!(follower.offer(4, appended(4)), Applied::Held);
    assert_eq!(follower.offer(3, appended(3)), Applied::Held);
    assert_eq!(follower.through(), 1);

    // Two arrives and the stream catches up to four in one step.
    assert_eq!(follower.offer(2, appended(2)), Applied::Take);
    let released = follower.release();
    assert_eq!(
        released.iter().map(|(at, _)| *at).collect::<Vec<_>>(),
        vec![3, 4],
        "released in order, because the caller writes them in this order"
    );
    assert_eq!(follower.through(), 4);
    assert!(
        follower.release().is_empty(),
        "and nothing is released twice"
    );
}

#[test]
fn a_position_already_applied_is_a_duplicate_rather_than_a_hole() {
    // An owner re-sending from a follower's mark re-sends the mark
    // itself when the two cross. Refusing that rather than ignoring it
    // would stall the stream at the moment it was being repaired.
    let mut follower = Follower::new();
    assert_eq!(follower.offer(1, appended(1)), Applied::Take);
    assert_eq!(follower.offer(2, appended(2)), Applied::Take);
    assert_eq!(follower.offer(1, appended(1)), Applied::Duplicate);
    assert_eq!(follower.offer(2, appended(2)), Applied::Duplicate);
    assert_eq!(follower.through(), 2, "and the mark did not move back");
}

#[test]
fn the_holding_area_is_bounded_and_says_when_it_overflowed() {
    // A hole that never fills must not become an unbounded copy of
    // another node's queue held in memory, which is the failure this
    // is supposed to prevent rather than cause.
    let mut follower = Follower::new();
    for at in 2..=(Follower::HELD_MAX as u64 + 1) {
        assert_eq!(follower.offer(at, appended(at)), Applied::Held);
    }
    assert_eq!(follower.overflowed(), 0, "that is exactly the bound");

    assert_eq!(
        follower.offer(Follower::HELD_MAX as u64 + 2, appended(1)),
        Applied::Held
    );
    assert_eq!(
        follower.overflowed(),
        1,
        "the one past the bound is dropped, and counted rather than silent"
    );
    assert_eq!(follower.through(), 0, "and the mark never moved");
}

#[test]
fn a_resumed_follower_starts_from_the_mark_it_was_given() {
    // A restart does not re-apply what the store already holds, and
    // does not treat the next operation as a hole either.
    let mut follower = Follower::resuming(9);
    assert_eq!(follower.through(), 9);
    assert_eq!(follower.offer(9, appended(9)), Applied::Duplicate);
    assert_eq!(follower.offer(10, appended(10)), Applied::Take);
    assert_eq!(follower.through(), 10);
}

#[test]
fn the_lag_is_what_the_owner_has_that_this_copy_does_not() {
    let follower = Follower::resuming(7);
    assert_eq!(follower.lag(10), 3);
    assert_eq!(follower.lag(7), 0, "caught up is zero");
    // An owner reading behind its own follower cannot be true, and a
    // momentary disagreement must read as nothing rather than as
    // eighteen quintillion on somebody's dashboard.
    assert_eq!(follower.lag(3), 0);
}

#[test]
fn an_acknowledgement_and_a_removal_order_by_position_not_by_sequence() {
    // **The two numbers.** These operations name queue entry 1, which
    // is older than everything around it; if the stream were ordered
    // by the entry's own sequence they would look like messages from
    // the past and be refused or applied twice.
    let mut follower = Follower::new();
    assert_eq!(follower.offer(1, appended(100)), Applied::Take);
    assert_eq!(
        follower.offer(2, Change::Acknowledged { sequence: 1 }),
        Applied::Take
    );
    assert_eq!(
        follower.offer(3, Change::Removed { sequence: 1 }),
        Applied::Take
    );
    assert_eq!(follower.through(), 3);
}

#[test]
fn an_owner_numbers_from_one_and_never_repeats() {
    let mut owner = Owner::new();
    assert_eq!(owner.at(), 0, "nothing sent is zero");
    let key = SessionKey::new("", "a-client");
    for expected in 1..=4 {
        assert_eq!(owner.next(&key, appended(expected)).at, expected);
    }
    assert_eq!(owner.at(), 4);
}

#[test]
fn an_owner_resuming_carries_on_rather_than_starting_again() {
    // **The reason the position is stored.** A follower refuses
    // anything at or below its mark, so an owner that began again at
    // one after a restart would have everything it sent refused as a
    // duplicate — silently, until the session had produced as many
    // operations as it had before.
    let mut owner = Owner::resuming(40);
    let key = SessionKey::new("", "a-client");
    assert_eq!(owner.next(&key, appended(1)).at, 41);

    // And the follower on the other side takes it, which is the half
    // that makes the point: it would have refused a 1.
    let mut follower = Follower::resuming(40);
    assert_eq!(follower.offer(41, appended(1)), Applied::Take);
    assert_eq!(follower.offer(1, appended(1)), Applied::Duplicate);
}

#[test]
fn a_queue_record_becomes_a_change_and_everything_else_becomes_nothing() {
    use storage::record::SessionRecord;

    let key = SessionKey::new("", "a-client");
    let (session, change) = change_for(&storage::Record::QueuePushed {
        session: key.clone(),
        entry: entry(7),
    })
    .expect("a push is a change");
    assert_eq!(session, key);
    assert!(matches!(change, Change::Appended { .. }));

    let (_, change) = change_for(&storage::Record::QueuePopped {
        session: key.clone(),
        sequence: 7,
    })
    .expect("a pop is a change");
    assert_eq!(change, Change::Removed { sequence: 7 });

    // Everything a queue is not. A subscription and a retained
    // message are replicated as registers by section 8.2 and have no
    // business in this stream; a build that streamed them would send
    // every retained payload to every follower of every session.
    assert!(change_for(&storage::Record::SubscriptionRemoved {
        session: key.clone(),
        filter: "a/#".to_owned(),
    })
    .is_none());
    assert!(change_for(&storage::Record::RetainedRemoved {
        topic: "a/b".to_owned(),
    })
    .is_none());
    assert!(change_for(&storage::Record::SessionRemoved {
        session: key.clone()
    })
    .is_none());
    assert!(change_for(&storage::Record::SessionPut(SessionRecord {
        key,
        version: storage::record::ProtocolVersion::V5,
        expiry_interval: 0,
        expires_at: None,
        will: None,
        will_at: None,
    }))
    .is_none());
}

// -----------------------------------------------------------
// The table both halves live in.
// -----------------------------------------------------------

fn key(client: &str) -> SessionKey {
    SessionKey::new("", client)
}

const ALPHA: [u8; 16] = [0xaa; 16];
const BETA: [u8; 16] = [0xbb; 16];

#[test]
fn a_stream_for_a_session_this_node_has_never_heard_of_starts_following_it() {
    // What makes a new replica set begin without a handshake of its
    // own: the first change that arrives is the introduction.
    let copies = Copies::new();
    let session = key("rover");
    assert!(!copies.follows(&session));

    let (taken, through) = copies.arrived(&session, ALPHA, 1, appended(1));
    assert_eq!(taken.len(), 1);
    assert_eq!(through, 1);
    assert!(copies.follows(&session));
    assert_eq!(copies.through(&session), Some(1));
}

#[test]
fn a_change_behind_a_hole_takes_nothing_and_still_reports_the_mark() {
    // **Still reports.** A follower that said nothing while it waited
    // would look to the owner exactly like one that had stopped, and
    // the owner's repair is driven by the mark.
    let copies = Copies::new();
    let session = key("rover");
    copies.arrived(&session, ALPHA, 1, appended(1));

    let (taken, through) = copies.arrived(&session, ALPHA, 3, appended(3));
    assert!(taken.is_empty(), "three is not next");
    assert_eq!(
        through, 1,
        "and the mark says so rather than saying nothing"
    );

    // Two arrives and both come out, in order.
    let (taken, through) = copies.arrived(&session, ALPHA, 2, appended(2));
    assert_eq!(
        taken.iter().map(|(at, _)| *at).collect::<Vec<_>>(),
        vec![2, 3],
        "the one offered, then what was waiting behind it"
    );
    assert_eq!(through, 3);
}

#[test]
fn a_session_this_node_holds_no_copy_of_is_not_zero() {
    // Zero is a copy that has applied nothing. No copy is a different
    // answer, and a caller choosing between replicas has to be able to
    // tell them apart.
    let copies = Copies::new();
    assert_eq!(copies.through(&key("nobody")), None);
    copies.arrived(&key("rover"), ALPHA, 1, appended(1));
    copies.unfollow(&key("rover"));
    assert_eq!(copies.through(&key("rover")), None);
    assert!(!copies.unfollow(&key("rover")), "and twice is not an error");
}

#[test]
fn the_lag_is_measured_from_the_follower_that_is_furthest_behind() {
    // **The furthest, not the nearest.** Section 8.6 makes the lag the
    // loss window: what a sudden loss of this node would cost. Measured
    // from the closest copy it would describe a failure that did not
    // happen.
    let copies = Copies::new();
    let session = key("rover");
    for at in 1..=10 {
        copies.send(&session, appended(at));
    }
    copies.marked(session.clone(), ALPHA, 9);
    copies.marked(session.clone(), BETA, 4);
    assert_eq!(copies.lag(&session), Some(6), "ten sent, four applied");
    assert_eq!(copies.worst_lag(), 6);

    // A session nobody has reported on has no lag rather than a lag of
    // zero: no copy is not perfect health.
    assert_eq!(copies.lag(&key("alone")), None);
}

#[test]
fn an_owned_session_resumes_from_the_stored_position_only_once() {
    // The store's number seeds the stream at startup; after that the
    // table is what counts. A resume consulted on every change would
    // put a long-running session back where it began.
    let copies = Copies::new();
    let session = key("rover");
    copies.resume(session.clone(), 40);
    assert_eq!(copies.send(&session, appended(1)).at, 41);
    assert_eq!(
        copies.send(&session, appended(2)).at,
        42,
        "the second change carries on rather than resuming again"
    );

    // And a session nobody resumed starts at one.
    assert_eq!(copies.send(&key("fresh"), appended(1)).at, 1);
}

#[test]
fn a_session_that_stops_being_streamed_takes_its_marks_with_it() {
    // Or a session recreated under the same key would inherit a lag
    // measured against a stream that no longer exists.
    let copies = Copies::new();
    let session = key("rover");
    copies.send(&session, appended(1));
    copies.marked(session.clone(), ALPHA, 1);
    assert_eq!(copies.streaming(), 1);

    copies.stop(&session);
    assert_eq!(copies.streaming(), 0);
    assert_eq!(copies.lag(&session), None);
    assert_eq!(copies.worst_lag(), 0);
}

// -----------------------------------------------------------
// Section 8.6's synchronous mode: waiting for a copy.
// -----------------------------------------------------------

/// Patience a test does not spend when it is not meant to.
const BRIEF: Duration = Duration::from_millis(100);

#[tokio::test]
async fn a_position_a_follower_already_holds_needs_no_wait() {
    // The ordinary case under any load: the mark arrives while the
    // store is still being waited on, so the publisher waits for
    // nothing it was not already waiting for.
    let copies = Copies::new();
    let session = key("rover");
    copies.send(&session, appended(1));
    copies.marked(session.clone(), ALPHA, 1);

    assert!(matches!(
        copies.confirming(&session, 1),
        Confirming::Already
    ));
    assert_eq!(
        copies.confirming(&session, 1).within(BRIEF).await,
        Confirmed::Yes
    );
}

#[tokio::test]
async fn a_session_with_no_follower_is_answered_rather_than_waited_on() {
    // **A cluster of one, a factor of one, every follower down.**
    // Section 8.6 acknowledges anyway and counts it, so this has to be
    // an answer: a publisher left waiting for a confirmation that
    // cannot come is the outage the acknowledgement avoids.
    let copies = Copies::new();
    let session = key("alone");
    copies.send(&session, appended(1));

    assert!(matches!(copies.confirming(&session, 1), Confirming::Nobody));
    let started = std::time::Instant::now();
    assert_eq!(
        copies
            .confirming(&session, 1)
            .within(Duration::from_secs(30))
            .await,
        Confirmed::No
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "it answered rather than waiting: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_mark_that_reaches_the_position_releases_the_publisher() {
    let copies = std::sync::Arc::new(Copies::new());
    let session = key("rover");
    for at in 1..=3 {
        copies.send(&session, appended(at));
    }
    // A follower exists but is behind, so this is a real wait.
    copies.marked(session.clone(), ALPHA, 1);

    let waiting = copies.confirming(&session, 3);
    assert!(matches!(waiting, Confirming::Waiting(_)));

    let releasing = std::sync::Arc::clone(&copies);
    let told = session.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        releasing.marked(told, ALPHA, 3);
    });
    assert_eq!(waiting.within(Duration::from_secs(5)).await, Confirmed::Yes);
}

#[tokio::test]
async fn one_follower_confirming_is_enough() {
    // **The mutation this exists for is a wait that needs every
    // follower.** Section 8.6 says at least one; waiting for all of
    // them makes the slowest copy the publisher's latency and stalls
    // every durable publish the moment one node goes quiet. A test
    // with a single follower passes either way.
    let copies = std::sync::Arc::new(Copies::new());
    let session = key("rover");
    for at in 1..=3 {
        copies.send(&session, appended(at));
    }
    // Two followers, and one of them is never going to answer.
    copies.marked(session.clone(), ALPHA, 0);
    copies.marked(session.clone(), BETA, 0);

    let waiting = copies.confirming(&session, 3);
    let releasing = std::sync::Arc::clone(&copies);
    let told = session.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        releasing.marked(told, BETA, 3);
    });
    assert_eq!(
        waiting.within(Duration::from_secs(5)).await,
        Confirmed::Yes,
        "beta confirmed; alpha never did and must not have been needed"
    );
}

#[tokio::test]
async fn a_follower_that_goes_quiet_does_not_hold_the_publisher_for_ever() {
    // The case synchronous mode exists for is also the case where
    // waiting without a bound turns one slow node into every
    // publisher's outage.
    let copies = Copies::new();
    let session = key("rover");
    copies.send(&session, appended(1));
    copies.marked(session.clone(), ALPHA, 0);

    let started = std::time::Instant::now();
    assert_eq!(
        copies.confirming(&session, 1).within(BRIEF).await,
        Confirmed::No
    );
    assert!(started.elapsed() >= BRIEF, "it waited");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "and then stopped: {:?}",
        started.elapsed()
    );
}
