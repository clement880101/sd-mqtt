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
            MetadataValue::SessionOwner {
                node: node(9),
                epoch: 3,
            },
        ),
        (
            MetadataKey::NodeGroup {
                node: me,
                group: "workers".to_owned(),
                filter: "jobs/+".to_owned(),
            },
            MetadataValue::NodeGroup,
        ),
        (
            MetadataKey::User {
                name: "alice".to_owned(),
            },
            MetadataValue::User(Box::new(storage::record::UserRecord {
                name: "alice".to_owned(),
                password: Some(storage::record::Secret::new(
                    "$argon2id$v=19$m=19456,t=2,p=1$x$y",
                )),
                scram: None,
                created_at: 1,
            })),
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
            MetadataValue::SessionOwner { node: owner, epoch } => {
                let MetadataKey::SessionOwner { session } = key else {
                    unreachable!("the pair came from one list")
                };
                before.session_owned(session, owner, epoch)
            }
            MetadataValue::NodeGroup => before
                .group_joined("workers", "jobs/+")
                .expect("the first member writes a register"),
            MetadataValue::User(held) => before.user(*held),
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
        held.session_owned(session("gauge"), me, 1),
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
    held.session_owned(session("gauge"), me, 1);
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

// ---- the routing map, which is an index over the registers -------------
//
// Section 8.3's map is fed by [`index`] from the four paths that change a
// register, and all four are in this file. These assert it through the
// registers rather than through `Routes`' own methods — `routes.rs` tests
// the map and this tests the wiring, which is the half that can be missed
// by a path nobody thought of.

/// A peer's register for `filter`, stamped later than anything local.
fn theirs(peer: NodeId, filter: &str, millis: u64) -> MetadataWrite {
    MetadataWrite::new(
        MetadataKey::NodeSubscription {
            node: peer,
            filter: filter.to_owned(),
        },
        Register::written(
            MetadataValue::NodeSubscription,
            Stamp::new(Reading::new(millis, 0), peer),
        ),
    )
}

/// And its tombstone, later still.
fn theirs_gone(peer: NodeId, filter: &str, millis: u64) -> MetadataWrite {
    MetadataWrite::new(
        MetadataKey::NodeSubscription {
            node: peer,
            filter: filter.to_owned(),
        },
        Register::deleted(Stamp::new(Reading::new(millis, 0), peer)),
    )
}

/// A register arriving puts its node on the route for a matching topic.
#[test]
fn a_peers_register_adds_it_to_the_routing_map() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    assert!(held.nodes_for("sensor/1/temp").is_empty());

    assert!(held.observed(theirs(peer, "sensor/+/temp", 100)));
    assert_eq!(
        held.nodes_for("sensor/1/temp"),
        vec![peer],
        "the register is held and the map does not know about it"
    );
    assert!(
        held.nodes_for("sensor/1/humidity").is_empty(),
        "a filter that does not match is not a route"
    );
}

/// A tombstone takes it off again.
#[test]
fn a_peers_tombstone_removes_it_from_the_routing_map() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    held.observed(theirs(peer, "a/b", 100));
    assert_eq!(held.nodes_for("a/b"), vec![peer]);

    assert!(held.observed(theirs_gone(peer, "a/b", 200)));
    assert!(
        held.nodes_for("a/b").is_empty(),
        "a tombstone is not a subscriber, in the map as in the registers"
    );
    assert_eq!(held.routes().filters_held(), 0, "and the row went with it");
}

/// A copy that lost does not move the map.
///
/// The register table already decided which write won, and an index fed
/// the loser would answer by the earlier of the two while the registers
/// answered by the later — which is the shape of every index that drifts.
#[test]
fn a_register_that_lost_does_not_move_the_routing_map() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    held.observed(theirs_gone(peer, "a/b", 200));
    assert!(held.nodes_for("a/b").is_empty());

    assert!(
        !held.observed(theirs(peer, "a/b", 100)),
        "an earlier copy must lose"
    );
    assert!(
        held.nodes_for("a/b").is_empty(),
        "the map took a write the registers refused"
    );
}

/// This node's own subscription is in the map like anybody else's.
///
/// Leaving this node out here would be a map whose answer depended on
/// who was asking; `cluster::forward` is where the subtraction belongs.
#[test]
fn this_nodes_own_subscription_is_in_the_routing_map() {
    let me = node(1);
    let held = fresh(me);
    held.subscriber_arrived("a/+")
        .expect("the first subscriber");
    assert_eq!(held.nodes_for("a/b"), vec![me]);

    held.subscriber_arrived("a/+");
    assert_eq!(
        held.nodes_for("a/b"),
        vec![me],
        "a second local subscriber writes no register and changes nothing"
    );
    held.subscriber_departed("a/+");
    assert_eq!(
        held.nodes_for("a/b"),
        vec![me],
        "and one departure of two leaves the node on the route"
    );
    held.subscriber_departed("a/+")
        .expect("the last subscriber tombstones");
    assert!(held.nodes_for("a/b").is_empty());
}

/// Two nodes wanting one filter are both forwarded to.
#[test]
fn two_nodes_wanting_one_filter_are_both_on_the_route() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    held.subscriber_arrived("a/b");
    held.observed(theirs(peer, "a/b", 100));
    assert_eq!(held.nodes_for("a/b"), vec![me, peer]);
}

/// A restart comes back with the map, not only with the registers.
///
/// The path that is easiest to leave out, and the one whose absence no
/// test about a running cluster can see: a node that came back holding
/// every register and an empty routing map would forward nothing to
/// anybody until every peer happened to write again, which for a quiet
/// cluster is never.
#[test]
fn a_restart_rebuilds_the_routing_map_from_the_registers() {
    let me = node(1);
    let peer = node(2);
    let store = MemoryStorage::new();
    let before = fresh(me);
    let mine = before
        .subscriber_arrived("sensor/+/temp")
        .expect("the first subscriber");
    store
        .apply(&before.records(&mine))
        .expect("the store takes it");
    let yours = theirs(peer, "site/#", 100);
    assert!(before.observed(yours.clone()));
    store
        .apply(&[Record::MetadataPut {
            entry: yours.to_record(),
        }])
        .expect("and the peer's");

    let restored = store.restore().expect("the store reads back");
    let after = Replicated::open(me, HybridClock::resume(restored.clock, BOUND), &restored)
        .expect("what this build wrote, it reads");
    assert_eq!(
        after.nodes_for("sensor/9/temp"),
        vec![me],
        "this node's own filter did not come back in the map"
    );
    assert_eq!(
        after.nodes_for("site/one/two"),
        vec![peer],
        "the peer's filter did not come back in the map"
    );
}

/// Collecting a tombstone leaves the map holding nothing for it.
///
/// A collected tombstone and a tombstone mean the same thing to routing
/// — this node does not believe that node has a subscriber — so the
/// answer does not change. What this guards is the row: an index that
/// kept one would grow with every filter the cluster ever held.
#[test]
fn collecting_a_tombstone_leaves_no_row_behind() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    held.observed(theirs(peer, "a/b", 100));
    held.observed(theirs_gone(peer, "a/b", 200));
    let key = MetadataKey::NodeSubscription {
        node: peer,
        filter: "a/b".to_owned(),
    };
    assert!(!held.collect(&key).is_empty(), "the tombstone is collected");
    assert!(held.nodes_for("a/b").is_empty());
    assert_eq!(held.routes().filters_held(), 0);
    assert_eq!(held.routes().filters_matched(), 0);
}

/// A node this one has judged dead stops being forwarded to.
///
/// Membership is not replicated, so nothing was written down. The
/// register is still held — this node's judgement is local and may be
/// the wrong one — and the route is withheld.
#[test]
fn a_node_judged_dead_is_not_on_the_route_although_its_register_is_held() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    held.observed(theirs(peer, "a/b", 100));

    held.died(peer);
    assert!(held.nodes_for("a/b").is_empty());
    assert_eq!(
        held.nodes_subscribed("a/b"),
        vec![peer],
        "the register is the truth and is kept"
    );

    held.revived(peer);
    assert_eq!(
        held.nodes_for("a/b"),
        vec![peer],
        "nothing was replayed, because nothing was thrown away"
    );
}

/// And a register taken from a peer's log moves the map too.
///
/// [`Replicated::observed_from`] is the exchange's door and
/// [`Replicated::observed`] is the broadcast's. They are two call sites
/// of one merge, so an index wired into one of them is an index that is
/// right until a broadcast is dropped — which is the case the exchange
/// exists for, and the one nobody tests by hand.
#[test]
fn a_register_taken_from_a_peers_log_moves_the_routing_map() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    let taken = held.observed_from(
        peer,
        vec![
            Logged::new(1, theirs(peer, "a/+", 100)),
            Logged::new(2, theirs_gone(peer, "a/+", 300)),
            Logged::new(3, theirs(peer, "c/#", 200)),
        ],
    );
    assert_eq!(taken.through, 3);
    assert!(
        held.nodes_for("a/b").is_empty(),
        "the later tombstone wins, in the map as in the registers"
    );
    assert_eq!(held.nodes_for("c/d/e"), vec![peer]);
}

// ---- the group map, which is the one the ordinary map cannot do -------

/// A second local member of a group writes nothing.
///
/// Section 8.2's granularity rule, applied to the kind section 8.3
/// added: what is replicated is that a node has **a** member, so a
/// second one says nothing new and saying it again would be a register
/// and a frame per client rather than per group.
#[test]
fn a_second_local_member_of_a_group_writes_nothing() {
    let me = node(1);
    let held = fresh(me);
    let first = held
        .group_joined("workers", "jobs/+")
        .expect("the first member writes a register");
    assert_eq!(
        first.key(),
        &MetadataKey::NodeGroup {
            node: me,
            group: "workers".to_owned(),
            filter: "jobs/+".to_owned()
        }
    );
    assert_eq!(first.register().value(), Some(&MetadataValue::NodeGroup));
    assert_eq!(held.group_joined("workers", "jobs/+"), None);
    assert_eq!(held.local_members("workers", "jobs/+"), 2);
}

/// The last local member to leave writes a tombstone.
///
/// Dropping the register instead would leave every peer choosing this
/// node for a group it has nobody in — and a publication chosen for it
/// is delivered to nobody at all, because the group's other members were
/// passed over for it. That is worse than an ordinary filter going
/// stale, which merely wastes a copy.
#[test]
fn the_last_local_member_of_a_group_writes_a_tombstone() {
    let me = node(1);
    let held = fresh(me);
    held.group_joined("workers", "jobs/+");
    held.group_joined("workers", "jobs/+");
    assert_eq!(held.group_left("workers", "jobs/+"), None, "one of two");
    let last = held
        .group_left("workers", "jobs/+")
        .expect("the last member writes a register");
    assert!(last.is_tombstone());
    assert_eq!(held.local_members("workers", "jobs/+"), 0);
    assert!(held.groups().members_of("workers", "jobs/+").is_empty());
}

/// Two groups on one filter are counted and routed apart.
#[test]
fn two_groups_on_one_filter_are_two_groups() {
    let me = node(1);
    let held = fresh(me);
    held.group_joined("one", "jobs/+");
    held.group_joined("two", "jobs/+");
    assert_eq!(held.local_members("one", "jobs/+"), 1);
    assert_eq!(held.local_members("two", "jobs/+"), 1);
    assert_eq!(held.groups().groups_held(), 2);
    assert_eq!(
        held.groups().chosen_for("jobs/x", me).len(),
        2,
        "each group is owed its own delivery"
    );
}

/// A group membership is not an ordinary subscription, in either map.
///
/// **The separation is the whole of section 8.3's answer**: a group
/// membership indexed into the routing map would put every member node
/// on the ordinary route, and one copy to each would be one delivery per
/// node.
#[test]
fn a_group_membership_is_in_the_group_map_and_not_the_routing_map() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    held.observed(MetadataWrite::new(
        MetadataKey::NodeGroup {
            node: peer,
            group: "workers".to_owned(),
            filter: "jobs/+".to_owned(),
        },
        Register::written(
            MetadataValue::NodeGroup,
            Stamp::new(Reading::new(100, 0), peer),
        ),
    ));
    assert!(
        held.nodes_for("jobs/one").is_empty(),
        "a group member must not be on the ordinary route"
    );
    assert_eq!(held.groups().members_of("workers", "jobs/+"), vec![peer]);
    assert_eq!(
        held.groups().chosen_for("jobs/one", me),
        vec![crate::groups::Chosen {
            group: "workers".to_owned(),
            filter: "jobs/+".to_owned(),
            node: peer
        }]
    );
}

/// And a peer's tombstone takes it out of the group map.
#[test]
fn a_peers_group_tombstone_removes_it_from_the_group_map() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    let key = MetadataKey::NodeGroup {
        node: peer,
        group: "workers".to_owned(),
        filter: "jobs/+".to_owned(),
    };
    held.observed(MetadataWrite::new(
        key.clone(),
        Register::written(
            MetadataValue::NodeGroup,
            Stamp::new(Reading::new(100, 0), peer),
        ),
    ));
    held.observed(MetadataWrite::new(
        key.clone(),
        Register::deleted(Stamp::new(Reading::new(200, 0), peer)),
    ));
    assert!(held.groups().chosen_for("jobs/one", me).is_empty());
    assert_eq!(held.groups().groups_held(), 0, "and the row went with it");

    assert!(!held.collect(&key).is_empty(), "the tombstone is collected");
    assert_eq!(held.groups().groups_held(), 0);
    assert_eq!(held.groups().groups_matched(), 0);
}

/// A restart rebuilds the group map as well as the routing map.
#[test]
fn a_restart_rebuilds_the_group_map_from_the_registers() {
    let me = node(1);
    let store = MemoryStorage::new();
    let before = fresh(me);
    let mine = before
        .group_joined("workers", "jobs/+")
        .expect("the first member");
    store
        .apply(&before.records(&mine))
        .expect("the store takes it");

    let restored = store.restore().expect("the store reads back");
    let after = Replicated::open(me, HybridClock::resume(restored.clock, BOUND), &restored)
        .expect("what this build wrote, it reads");
    assert_eq!(after.groups().members_of("workers", "jobs/+"), vec![me]);
    assert!(
        after.nodes_for("jobs/one").is_empty(),
        "and it did not come back as an ordinary route"
    );
}

// ---- who owns a session, section 8.4 ----------------------------------

/// A connect to the node that already owns the session writes nothing.
///
/// Section 8.4 is about a connect to a node that does **not** own the
/// session; a connect to the one that does is the within-one-node
/// takeover `broker_core::registry` has always done, and that takeover
/// is not a cluster event. A claim that wrote a register anyway would
/// be a frame per reconnect from every client on every node, and — the
/// part that is not merely waste — a fresh stamp on the losing side of
/// a partition every time the client retried, which is a node voting
/// repeatedly in an election it has already lost.
///
/// The register is compared whole, stamp included, because a rewrite
/// carrying the same owner and the same epoch is exactly the write
/// this is about and nothing else would see it.
#[test]
fn a_claim_by_the_node_that_already_owns_it_writes_nothing() {
    let me = node(1);
    let held = fresh(me);
    let key = session("gauge");

    let first = held.session_claimed(&key);
    assert_eq!(first.epoch, 1, "nothing held means nobody has claimed it");
    assert_eq!(first.previous, None);
    let write = first.write.expect("the first claim writes the register");
    let before = held
        .register(&MetadataKey::SessionOwner {
            session: key.clone(),
        })
        .expect("the claim is held");

    let again = held.session_claimed(&key);
    assert!(
        again.write.is_none(),
        "a claim by the owner wrote {:?}",
        again.write
    );
    assert_eq!(
        again.epoch, 1,
        "and it did not move the epoch it already holds"
    );
    assert_eq!(
        again.previous,
        Some(Ownership { node: me, epoch: 1 }),
        "the owner it found was itself"
    );
    assert_eq!(
        write.stamp(),
        before.stamp(),
        "the stamp the first claim reported is the one it holds"
    );
    assert_eq!(
        held.register(&MetadataKey::SessionOwner { session: key }),
        Some(before),
        "and the register is still that one, stamp and all"
    );
}

/// A connect to another node claims it, with a higher epoch.
///
/// The epoch is read from the register and incremented, so it is one
/// number for the session rather than one per node: the previous
/// owner's epoch is what the new owner's is one more than.
#[test]
fn a_claim_by_another_node_takes_it_at_a_higher_epoch() {
    let me = node(1);
    let peer = node(2);
    let held = fresh(me);
    let key = session("gauge");

    // The peer owns it at epoch 4, replicated to this node.
    held.observed(MetadataWrite::new(
        MetadataKey::SessionOwner {
            session: key.clone(),
        },
        Register::written(
            MetadataValue::SessionOwner {
                node: peer,
                epoch: 4,
            },
            Stamp::new(Reading::new(100, 0), peer),
        ),
    ));
    assert_eq!(
        held.session_owner(&key),
        Some(Ownership {
            node: peer,
            epoch: 4
        })
    );

    let claim = held.session_claimed(&key);
    assert_eq!(
        claim.previous,
        Some(Ownership {
            node: peer,
            epoch: 4
        }),
        "the claim reports who it took it from, which is who to ask for the queue"
    );
    assert_eq!(claim.epoch, 5, "the epoch is the one it read plus one");
    let write = claim.write.expect("a claim from another node is a write");
    assert_eq!(
        write.register().value(),
        Some(&MetadataValue::SessionOwner { node: me, epoch: 5 }),
        "the register names this node as owner and carries the epoch"
    );
    assert_eq!(
        write.stamp().node(),
        me,
        "stamped by whoever claimed it, like every other register"
    );
    assert_eq!(
        held.session_owner(&key),
        Some(Ownership { node: me, epoch: 5 }),
        "and this node holds it immediately, before the broadcast"
    );
}

/// Two nodes claiming at the same epoch resolve the same way on both.
///
/// **This is the expected outcome of a partition, not the rare one**,
/// and section 8.5 says so: two nodes on opposite sides both read the
/// same epoch and both increment to the same number. A test of a
/// single claim cannot see it, because a single claim has nothing to
/// tie with.
///
/// What resolves it is the register's own comparison — the clock
/// reading, then the counter, then the node's UUID — which is why the
/// epoch is *carried* in the register rather than compared on its own.
/// Both nodes are told both writes, in opposite orders, and what this
/// asserts is that they agree: not which one wins, which is arbitrary,
/// but that there is one answer and both hold it. A comparison on the
/// epoch alone leaves each side holding itself, and no later exchange
/// repairs it.
#[test]
fn two_nodes_claiming_at_the_same_epoch_agree_on_which_one_won() {
    let one = node(1);
    let two = node(2);
    let first = fresh(one);
    let second = fresh(two);
    let key = session("gauge");

    // Each side of the partition reads nothing held and claims it.
    let mine = first
        .session_claimed(&key)
        .write
        .expect("the first side claims it");
    let theirs = second
        .session_claimed(&key)
        .write
        .expect("the second side claims it, knowing nothing of the first");
    // The equal epoch this test is about: both sides read nothing held
    // and both wrote one. Spelled out as two values rather than as a
    // comparison, so that a build where neither side incremented would
    // not satisfy it by being equally wrong.
    assert_eq!(
        mine.register().value(),
        Some(&MetadataValue::SessionOwner {
            node: one,
            epoch: 1
        })
    );
    assert_eq!(
        theirs.register().value(),
        Some(&MetadataValue::SessionOwner {
            node: two,
            epoch: 1
        })
    );

    // The partition heals and each is told the other, in the order it
    // would hear it.
    first.observed(theirs.clone());
    second.observed(mine.clone());

    let agreed = first.session_owner(&key);
    assert_eq!(
        agreed,
        second.session_owner(&key),
        "the two sides of a healed partition hold different owners for one session"
    );
    let agreed = agreed.expect("somebody owns it");
    assert_eq!(agreed.epoch, 1, "the epoch both sides claimed at");
    let expected = if mine.stamp() > theirs.stamp() {
        one
    } else {
        two
    };
    assert_eq!(
        agreed.node, expected,
        "the winner is the later stamp, which is what every other register does"
    );

    // And it stays resolved: hearing the loser again changes nothing,
    // which is what makes the exchange safe to repeat.
    assert!(
        !first.observed(if expected == one {
            theirs.clone()
        } else {
            mine.clone()
        }),
        "the losing write won on a second hearing"
    );
    assert_eq!(first.session_owner(&key), Some(agreed));
    assert_eq!(second.session_owner(&key), Some(agreed));
}

/// A released session is nobody's, and the next claim starts again.
///
/// A tombstone is an absent owner rather than an owner of nothing, so
/// [`Replicated::session_owner`] answers `None` for it, which is the
/// same answer a register nobody ever wrote gives. What a claim does
/// after one is what it does from nothing: epoch one.
#[test]
fn a_released_session_has_no_owner_and_no_epoch() {
    let me = node(1);
    let held = fresh(me);
    let key = session("gauge");

    held.session_claimed(&key);
    held.session_released(key.clone());
    assert_eq!(
        held.session_owner(&key),
        None,
        "a tombstone is an absent owner"
    );
    assert_eq!(
        held.session_claimed(&key).epoch,
        1,
        "and the epoch starts again rather than carrying on from a deletion"
    );
}
