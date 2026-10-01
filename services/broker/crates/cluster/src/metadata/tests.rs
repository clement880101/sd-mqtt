//! What is replicated, and at what granularity.
//!
//! Two groups. The first is that every kind section 8.2 names this
//! milestone round trips through a register and survives a restart,
//! through a real store rather than through a clone of the map: the
//! restart is the thing a format version exists for, and a test that
//! compared one `Replicated` with another would pass on a backend that
//! wrote nothing.
//!
//! The second is the granularity, which is where the two easy-to-miss
//! edges are. Section 8.2 replicates **that a node has a subscriber for
//! a filter**, so a second subscriber says nothing new and the last
//! unsubscribe says a tombstone rather than nothing. Both are asserted
//! on the count *and* on what comes back, because a count that is right
//! and a register that is not is the half of this that routes.

use super::*;

use storage::{MemoryStorage, Qos, Restored, Storage, TriggerRecord};

use crate::clock::{HybridClock, Reading, DEFAULT_MAX_DRIFT_MILLIS};

/// The drift bound these tests do not vary.
const BOUND: u64 = DEFAULT_MAX_DRIFT_MILLIS;

/// A node identity whose bytes a failure can be read off.
fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index; 16])
}

/// This node's state, with nothing in it.
fn fresh(me: NodeId) -> Replicated {
    Replicated::open(me, HybridClock::resume(None, BOUND), &Restored::default())
        .expect("an empty store holds nothing to refuse")
}

/// A retained message, for the kind that carries one.
fn message(payload: &'static str) -> RetainedRecord {
    RetainedRecord {
        payload: bytes::Bytes::from_static(payload.as_bytes()),
        qos: Qos::AtLeastOnce,
        expires_at: None,
        properties: bytes::Bytes::new(),
    }
}

/// A rule, for the kind that carries one.
fn rule(id: &str) -> RuleRecord {
    RuleRecord {
        id: id.to_owned(),
        name: id.to_owned(),
        enabled: true,
        trigger: TriggerRecord {
            filter: "sensor/#".to_owned(),
            min_qos: None,
            client_id: None,
        },
        action: storage::ActionRecord::HttpOut(storage::HttpOutRecord {
            url: "https://example.invalid/hook".to_owned(),
            method: "POST".to_owned(),
            headers: Vec::new(),
            timeout_ms: 5_000,
            max_concurrency: 1,
            retries: 2,
            backoff_ms: 100,
        }),
    }
}

/// A session key, for the kind that carries one.
fn session(client: &str) -> SessionKey {
    SessionKey {
        mountpoint: String::new(),
        client_id: client.to_owned(),
    }
}

/// Every key this milestone replicates, paired with a value for it.
///
/// One list, used by the round-trip test and by the restart test, so
/// that a kind added to the vocabulary and not to this list is a kind
/// neither of them covers.
fn every_kind(me: NodeId) -> Vec<(MetadataKey, MetadataValue)> {
    vec![
        (
            MetadataKey::NodeSubscription {
                node: me,
                filter: "sensor/+/temp".to_owned(),
            },
            MetadataValue::NodeSubscription,
        ),
        (
            MetadataKey::Retained {
                topic: "sensor/1/temp".to_owned(),
            },
            MetadataValue::Retained(message("warm")),
        ),
        (
            MetadataKey::Rule {
                id: "telemetry".to_owned(),
            },
            MetadataValue::Rule(Box::new(rule("telemetry"))),
        ),
        (
            MetadataKey::SessionOwner {
                session: session("gauge"),
            },
            MetadataValue::SessionOwner { node: node(9) },
        ),
    ]
}

// ---- the vocabulary ----------------------------------------------------

/// Every kind round trips through a register and through a store.
///
/// The value out is compared with the value in, and the stamp with the
/// stamp: a record that lost its stamp would converge against nothing,
/// and a record that lost its node would attribute every write to
/// whoever read it.
#[test]
fn every_kind_round_trips_through_a_register() {
    let me = node(1);
    for (key, value) in every_kind(me) {
        let stamp = Stamp::new(Reading::new(7, 3), me);
        let write = MetadataWrite::new(key.clone(), Register::written(value.clone(), stamp));
        let back = MetadataWrite::from_record(write.to_record())
            .expect("a record this build wrote is a record it reads");
        assert_eq!(back.key(), &key, "the key did not survive the record");
        assert_eq!(
            back.register().value(),
            Some(&value),
            "the value did not survive the record"
        );
        assert_eq!(back.stamp(), stamp, "the stamp did not survive the record");
        assert_eq!(back, write, "the write did not survive the record");
    }
}

/// A tombstone of every kind round trips as a tombstone.
///
/// One byte distinguishes it, and losing that byte turns every delete in
/// the cluster back into a write of whatever it deleted.
#[test]
fn a_tombstone_of_every_kind_round_trips_as_a_tombstone() {
    let me = node(1);
    for (key, _) in every_kind(me) {
        let write = MetadataWrite::new(
            key.clone(),
            Register::<MetadataValue>::deleted(Stamp::new(Reading::new(7, 3), me)),
        );
        let back =
            MetadataWrite::from_record(write.to_record()).expect("a tombstone is readable back");
        assert!(
            back.is_tombstone(),
            "a tombstone for {key:?} came back carrying a value"
        );
        assert_eq!(back, write);
    }
}

/// A stored register whose key and value name different kinds is refused.
///
/// Convention 1 read the other way round: nothing this build writes can
/// produce the pair, so one in a store is a store written by something
/// else. Guessing which half to believe would route by one and answer by
/// the other.
#[test]
fn a_key_and_a_value_naming_different_kinds_are_refused() {
    let record = MetadataRecord {
        key: MetadataKeyRecord::Rule {
            id: "telemetry".to_owned(),
        },
        stamp: StampRecord {
            millis: 1,
            counter: 0,
            node: [1; 16],
        },
        value: Some(MetadataValueRecord::NodeSubscription),
    };
    assert_eq!(
        MetadataWrite::from_record(record),
        Err(MetadataError::KindMismatch {
            key: "a rule",
            value: "a node subscription",
        })
    );
}

/// Every kind survives a restart, through a real store.
///
/// What comes back is compared as **one equality** against what the node
/// held, in key order, which is the comparison
/// `storage::Restored::metadata` is ordered for. A tombstone is among
/// them, because a node that dropped one on restart would be handed its
/// own older copy by the next peer it spoke to and would resurrect what
/// it had deleted.
#[test]
fn every_kind_survives_a_restart() {
    let me = node(1);
    let store = MemoryStorage::new();
    let before = fresh(me);

    let mut writes = Vec::new();
    for (key, value) in every_kind(me) {
        writes.push(match value {
            MetadataValue::NodeSubscription => before
                .subscriber_arrived("sensor/+/temp")
                .expect("the first subscriber writes a register"),
            MetadataValue::Retained(held) => before.retained("sensor/1/temp", held),
            MetadataValue::Rule(held) => before.rule(*held),
            MetadataValue::SessionOwner { node: owner } => {
                let MetadataKey::SessionOwner { session } = key else {
                    unreachable!("the pair came from one list")
                };
                before.session_owned(session, owner)
            }
        });
    }
    // And a tombstone, which is the last unsubscribe: the register that
    // was written first is now a deletion, so the restore has to carry
    // one of those too.
    writes.push(
        before
            .subscriber_departed("sensor/+/temp")
            .expect("the last subscriber writes a tombstone"),
    );

    for write in &writes {
        store
            .apply(&before.records(write))
            .expect("a store must accept a register and the mark beside it");
    }

    let restored = store.restore().expect("a restore");
    assert_eq!(
        restored.metadata,
        before.records_held(),
        "the store and the node disagree about what is held"
    );
    let after = Replicated::open(me, HybridClock::resume(restored.clock, BOUND), &restored)
        .expect("what this build wrote, it reads");
    assert_eq!(
        after.records_held(),
        before.records_held(),
        "a restart did not come back holding what it held"
    );

    // The tombstone is a tombstone and not an absence, which is the one
    // thing an equality over records could satisfy by holding neither.
    let key = MetadataKey::NodeSubscription {
        node: me,
        filter: "sensor/+/temp".to_owned(),
    };
    assert!(
        after
            .register(&key)
            .expect("the tombstone is held, not dropped")
            .is_tombstone(),
        "the deletion came back as a value or not at all"
    );
    assert_eq!(
        after.nodes_subscribed("sensor/+/temp"),
        Vec::new(),
        "a tombstone is not a subscriber"
    );
}

/// A register write is stored with the clock mark beside it.
///
/// Section 8.2's resume depends on the mark riding in the batch of the
/// write it stamps. A pair from one call is what makes taking the
/// register alone impossible; this is what says the pair is the right
/// one — the mark is at least the stamp the register carries, so a
/// restart cannot resume behind a stamp it has already shown somebody.
#[test]
fn a_write_carries_the_clock_mark_that_covers_its_stamp() {
    let me = node(1);
    let held = fresh(me);
    let write = held.rule(rule("telemetry"));
    let records = held.records(&write);

    let Record::MetadataPut { entry } = &records[0] else {
        panic!("the first record must be the register: {records:?}")
    };
    let Record::ClockHighWaterPut { reading } = &records[1] else {
        panic!("the second record must be the clock mark: {records:?}")
    };
    assert_eq!(entry, &write.to_record());
    assert!(
        Reading::from_record(*reading) >= write.stamp().reading(),
        "the mark {reading:?} does not cover the stamp {:?} it was written beside",
        write.stamp().reading()
    );
}

/// Every write this node makes is stamped with this node and ordered.
///
/// One clock and one identity across all four kinds, so that a conflict
/// between a rule and a rule and a conflict between a retained message
/// and a retained message resolve by the same comparison.
#[test]
fn every_kind_is_stamped_by_one_clock_and_one_identity() {
    let me = node(1);
    let held = fresh(me);
    let writes = vec![
        held.subscriber_arrived("a/b")
            .expect("the first subscriber"),
        held.retained("a/b", message("warm")),
        held.rule(rule("telemetry")),
        held.session_owned(session("gauge"), me),
        held.retained_cleared("a/b"),
        held.rule_removed("telemetry"),
        held.session_released(session("gauge")),
        held.subscriber_departed("a/b")
            .expect("the last subscriber"),
    ];
    for write in &writes {
        assert_eq!(
            write.stamp().node(),
            me,
            "a write was attributed to another node"
        );
    }
    for pair in writes.windows(2) {
        assert!(
            pair[0].stamp() < pair[1].stamp(),
            "two writes are out of order or share a stamp: {:?} then {:?}",
            pair[0].stamp(),
            pair[1].stamp()
        );
    }
}

/// A deletion of every kind is a tombstone rather than a removal.
///
/// There is no removal record in the vocabulary, and this is what says
/// so: each of the four clearing methods produces a register with no
/// value, and the register is still held afterwards.
#[test]
fn clearing_anything_writes_a_tombstone_and_keeps_it() {
    let me = node(1);
    let held = fresh(me);
    held.retained("a/b", message("warm"));
    held.rule(rule("telemetry"));
    held.session_owned(session("gauge"), me);
    held.subscriber_arrived("a/b");

    let cleared = vec![
        held.retained_cleared("a/b"),
        held.rule_removed("telemetry"),
        held.session_released(session("gauge")),
        held.subscriber_departed("a/b")
            .expect("the last subscriber writes a tombstone"),
    ];
    for write in &cleared {
        assert!(
            write.is_tombstone(),
            "clearing {:?} did not write a tombstone",
            write.key()
        );
        assert!(
            held.register(write.key())
                .expect("the tombstone is held")
                .is_tombstone(),
            "the tombstone for {:?} was not kept",
            write.key()
        );
        assert_eq!(held.value(write.key()), None);
    }
}

// ---- the granularity ---------------------------------------------------

/// **A filter gaining a second local subscriber writes nothing.**
///
/// Section 8.2's node granularity, as the first of its two edges. Every
/// peer already believes this node has a subscriber for the filter, so
/// there is nothing new to say, and saying it anyway is a register and a
/// frame per client rather than per filter — which is the fifty nodes
/// holding a hundred thousand registers each that this design exists to
/// avoid.
///
/// The count is asserted beside it, because a register not written and a
/// count not kept look identical until the unsubscribe.
#[test]
fn a_second_local_subscriber_to_a_filter_writes_nothing() {
    let me = node(1);
    let held = fresh(me);

    let first = held
        .subscriber_arrived("sensor/+/temp")
        .expect("the first subscriber writes a register");
    assert_eq!(held.local_subscribers("sensor/+/temp"), 1);
    assert_eq!(
        first.key(),
        &MetadataKey::NodeSubscription {
            node: me,
            filter: "sensor/+/temp".to_owned()
        }
    );
    assert_eq!(
        first.register().value(),
        Some(&MetadataValue::NodeSubscription)
    );

    assert_eq!(
        held.subscriber_arrived("sensor/+/temp"),
        None,
        "a second subscriber to a filter had something to broadcast"
    );
    assert_eq!(
        held.local_subscribers("sensor/+/temp"),
        2,
        "the second subscriber was not counted"
    );
    assert_eq!(
        held.subscriber_arrived("sensor/+/temp"),
        None,
        "a third subscriber to a filter had something to broadcast"
    );
    assert_eq!(held.local_subscribers("sensor/+/temp"), 3);

    // And the register still says what the first subscriber said, with
    // the first subscriber's stamp: a later write of the same value
    // would be a frame per client wearing a different disguise.
    assert_eq!(held.register(first.key()), Some(first.register().clone()));
}

/// **A filter losing its last local subscriber writes a tombstone.**
///
/// The second edge, and the one that cannot be "nothing": a peer that
/// was told this node had a subscriber has to be told it no longer does,
/// and a register silently dropped leaves that peer routing here for
/// ever. The departures before the last one write nothing, which is the
/// same rule as the arrivals.
#[test]
fn the_last_local_unsubscribe_from_a_filter_writes_a_tombstone() {
    let me = node(1);
    let held = fresh(me);
    held.subscriber_arrived("sensor/+/temp");
    held.subscriber_arrived("sensor/+/temp");
    held.subscriber_arrived("sensor/+/temp");
    assert_eq!(held.local_subscribers("sensor/+/temp"), 3);

    assert_eq!(
        held.subscriber_departed("sensor/+/temp"),
        None,
        "a departure that left two subscribers had something to broadcast"
    );
    assert_eq!(held.local_subscribers("sensor/+/temp"), 2);
    assert_eq!(
        held.subscriber_departed("sensor/+/temp"),
        None,
        "a departure that left one subscriber had something to broadcast"
    );
    assert_eq!(held.local_subscribers("sensor/+/temp"), 1);

    let last = held
        .subscriber_departed("sensor/+/temp")
        .expect("the last unsubscribe must broadcast a tombstone");
    assert!(
        last.is_tombstone(),
        "the last unsubscribe broadcast a value rather than a tombstone"
    );
    assert_eq!(
        last.key(),
        &MetadataKey::NodeSubscription {
            node: me,
            filter: "sensor/+/temp".to_owned()
        }
    );
    assert_eq!(held.local_subscribers("sensor/+/temp"), 0);
    assert_eq!(
        held.nodes_subscribed("sensor/+/temp"),
        Vec::new(),
        "the node is still advertised as a subscriber after its last one left"
    );
    // The tombstone is kept, which is the whole of why it exists.
    assert!(held
        .register(last.key())
        .expect("the tombstone is held rather than the row dropped")
        .is_tombstone());
}

/// A filter subscribed, dropped and subscribed again writes each time.
///
/// The count leaving zero is what writes, so a filter that reached zero
/// and came back is a new arrival and not a second subscriber. Without
/// this a node that emptied a filter and refilled it would stay
/// tombstoned for the rest of the cluster's life.
#[test]
fn a_filter_that_reached_zero_and_came_back_writes_again() {
    let me = node(1);
    let held = fresh(me);
    let first = held
        .subscriber_arrived("a/b")
        .expect("the first subscriber");
    let gone = held
        .subscriber_departed("a/b")
        .expect("the last subscriber");
    let again = held.subscriber_arrived("a/b").expect("a fresh first one");

    assert!(!first.is_tombstone());
    assert!(gone.is_tombstone());
    assert!(!again.is_tombstone());
    assert!(
        again.stamp() > gone.stamp(),
        "the re-subscribe does not beat the tombstone it has to overwrite"
    );
    assert_eq!(held.nodes_subscribed("a/b"), vec![me]);
}

/// Two filters are counted separately.
///
/// A count kept per node rather than per filter would make the second
/// filter's first subscriber look like a second subscriber, and nothing
/// would ever be advertised for it.
#[test]
fn two_filters_are_counted_separately() {
    let me = node(1);
    let held = fresh(me);
    assert!(held.subscriber_arrived("a/b").is_some());
    assert!(
        held.subscriber_arrived("c/d").is_some(),
        "a different filter's first subscriber wrote nothing"
    );
    assert_eq!(held.local_subscribers("a/b"), 1);
    assert_eq!(held.local_subscribers("c/d"), 1);
    assert_eq!(held.nodes_subscribed("a/b"), vec![me]);
    assert_eq!(held.nodes_subscribed("c/d"), vec![me]);
}

/// A departure from a filter nothing is subscribed to changes nothing.
///
/// Not a tombstone and not a count below zero. A count that went
/// negative would never reach zero again, so the next genuine last
/// unsubscribe would broadcast nothing and the node would stay
/// advertised for a filter it has no subscriber for.
#[test]
fn a_departure_from_an_unsubscribed_filter_changes_nothing() {
    let me = node(1);
    let held = fresh(me);
    assert_eq!(held.subscriber_departed("a/b"), None);
    assert_eq!(held.local_subscribers("a/b"), 0);
    assert_eq!(
        held.register(&MetadataKey::NodeSubscription {
            node: me,
            filter: "a/b".to_owned()
        }),
        None,
        "a departure from nothing wrote a tombstone"
    );

    // And the count is still honest afterwards.
    assert!(held.subscriber_arrived("a/b").is_some());
    assert!(held
        .subscriber_departed("a/b")
        .expect("the last unsubscribe still broadcasts")
        .is_tombstone());
}

/// The count is this node's own, and is neither replicated nor stored.
///
/// A peer's register arriving must not touch it: the count answers "do
/// *my* clients want this filter", and a peer's answer about its own
/// clients is a different question. If a peer's frame moved this count,
/// one peer subscribing would stop this node ever tombstoning its own
/// filter.
#[test]
fn a_peers_register_does_not_touch_this_nodes_count() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    held.subscriber_arrived("a/b");

    let theirs = MetadataWrite::new(
        MetadataKey::NodeSubscription {
            node: peer,
            filter: "a/b".to_owned(),
        },
        Register::written(
            MetadataValue::NodeSubscription,
            Stamp::new(Reading::new(100, 0), peer),
        ),
    );
    assert!(held.observed(theirs), "a register nothing held was refused");
    assert_eq!(
        held.local_subscribers("a/b"),
        1,
        "a peer's register changed this node's own count"
    );
    assert_eq!(
        held.nodes_subscribed("a/b"),
        vec![me, peer],
        "two nodes subscribed to one filter are two registers"
    );

    // And this node's own last unsubscribe still tombstones only its own
    // register.
    assert!(held
        .subscriber_departed("a/b")
        .expect("the last local subscriber")
        .is_tombstone());
    assert_eq!(
        held.nodes_subscribed("a/b"),
        vec![peer],
        "this node's tombstone took the peer's register with it"
    );
}

// ---- merging, which is the register's and not this module's -------------

/// An arriving copy is taken only when it is later.
///
/// One comparison, and it is [`Register::merge`]'s. This is here to say
/// that nothing in this module added a second one — in particular that a
/// tombstone is not special on the way in either.
#[test]
fn an_arriving_copy_is_taken_only_when_it_is_later() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    let key = MetadataKey::Retained {
        topic: "a/b".to_owned(),
    };

    let early = MetadataWrite::new(
        key.clone(),
        Register::written(
            MetadataValue::Retained(message("cold")),
            Stamp::new(Reading::new(10, 0), peer),
        ),
    );
    let late = MetadataWrite::new(
        key.clone(),
        Register::written(
            MetadataValue::Retained(message("hot")),
            Stamp::new(Reading::new(20, 0), peer),
        ),
    );
    let tombstone = MetadataWrite::new(
        key.clone(),
        Register::<MetadataValue>::deleted(Stamp::new(Reading::new(15, 0), peer)),
    );

    assert!(held.observed(late.clone()), "the first copy was refused");
    assert!(
        !held.observed(early),
        "an earlier copy was taken over a later one"
    );
    assert!(
        !held.observed(tombstone),
        "a tombstone older than the value beat it, which is the divergence \
         section 8.2 refuses"
    );
    assert_eq!(
        held.value(&key),
        Some(MetadataValue::Retained(message("hot")))
    );
}

/// A local write beats a peer's copy this node has already taken.
///
/// Which is causality: this node saw the peer's register, so the stamp it
/// issues afterwards has to be greater. Without it an operator's edit on
/// this node would silently lose to the value it was editing.
#[test]
fn a_local_write_after_a_peers_copy_beats_it() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    let key = MetadataKey::Rule {
        id: "telemetry".to_owned(),
    };

    // A peer whose clock is far ahead of this node's, but inside the
    // drift bound this node accepts.
    let wall = crate::clock::wall_millis();
    let theirs = Stamp::new(Reading::new(wall + BOUND / 2, 0), peer);
    assert!(held.observed(MetadataWrite::new(
        key.clone(),
        Register::written(MetadataValue::Rule(Box::new(rule("telemetry"))), theirs),
    )));
    held.clock()
        .observe(wall, theirs.reading())
        .expect("a peer inside the bound is taken, not refused");

    let mine = held.rule(rule("telemetry"));
    assert!(
        mine.stamp() > theirs,
        "a local write after a peer's copy did not beat it: {:?} against {:?}",
        mine.stamp(),
        theirs
    );
    assert_eq!(held.register(&key).expect("held").stamp(), mine.stamp());
}
