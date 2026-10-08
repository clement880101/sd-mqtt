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
