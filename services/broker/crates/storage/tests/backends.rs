//! One suite, run against every backend.
//!
//! These tests are the definition of what a [`Storage`] implementation
//! must do. They were written against the trait and its documentation,
//! before any implementation existed, for the reason the plan gives: a
//! suite written after an implementation describes that implementation,
//! and the second backend then has to match an accident rather than a
//! contract.
//!
//! Every test is a function generic over the trait, taking a constructor
//! for a fresh, empty store. The `every_contract_test!` list at the bottom
//! names each one once; adding a backend adds one line there and runs the
//! identical set.
//!
//! # What this suite deliberately does not cover
//!
//! - **Durability.** Nothing here reopens a store, because an in-memory
//!   backend has nothing to reopen. That a flush survives a restart, and
//!   that a store written by a later format refuses to open, belong to the
//!   redb backend, which is the only thing that can fail them.
//! - **Ordering the trait does not promise.** Queue entries restore in
//!   sequence order and that is asserted. Sessions, subscriptions, inbound
//!   identifiers, inflight entries and retained messages are compared as
//!   sets, because no backend has promised an order for them and pinning
//!   one here would be pinning whichever order the first backend happened
//!   to produce.
//! - **Concurrency.** The trait is `Send + Sync` and the type-level proof
//!   of that lives beside it in `lib.rs`. What two concurrent batches do to
//!   each other is not specified yet, so there is nothing to assert.

use std::collections::BTreeSet;

use storage::{
    BodyRecord, DeliveryRecord, InflightRecord, InflightState, MessageId, ProtocolVersion, Qos,
    QueueEntry, Record, Rejection, Restored, RestoredSession, RetainHandling, RetainedRecord,
    SessionKey, SessionRecord, Storage, StorageError, SubscriptionRecord,
};

/// The contract, as functions over any backend.
mod contract {
    use super::*;

    // ---- building blocks ------------------------------------------------

    /// A session key in the default mountpoint.
    fn key(client_id: &str) -> SessionKey {
        SessionKey::new("", client_id)
    }

    /// A session record with fields that are distinguishable from the
    /// defaults, so that an implementation dropping one is visible.
    fn session(client_id: &str) -> SessionRecord {
        SessionRecord {
            key: key(client_id),
            version: ProtocolVersion::V5,
            expiry_interval: 3_600,
            expires_at: Some(1_700_000_000),
            will: None,
            will_at: None,
        }
    }

    /// A subscription with non-default options, for the same reason.
    fn subscription(filter: &str) -> SubscriptionRecord {
        SubscriptionRecord {
            filter: filter.to_owned(),
            qos: Qos::ExactlyOnce,
            no_local: true,
            retain_as_published: true,
            retain_handling: RetainHandling::SendIfNew,
            subscription_id: Some(9),
        }
    }

    /// A message identifier that differs from every other in its first
    /// byte, so that a mix-up is not hidden by fifteen shared zeroes.
    fn body_id(n: u8) -> MessageId {
        let mut bytes = [0u8; 16];
        bytes[0] = n;
        MessageId(bytes)
    }

    /// A body whose payload names it, so a swapped body is legible in a
    /// failure message.
    fn body(n: u8) -> BodyRecord {
        BodyRecord {
            topic: format!("sensor/{n}"),
            payload: format!("body-{n}").into_bytes().into(),
            qos: Qos::AtLeastOnce,
            retain: false,
            expires_at: Some(1_700_000_100),
            properties: bytes::Bytes::from_static(&[0x01, 0x00]),
        }
    }

    fn delivery(body: MessageId) -> DeliveryRecord {
        DeliveryRecord {
            body,
            qos: Qos::AtLeastOnce,
            retain: true,
            subscription_ids: vec![9],
            dup: false,
        }
    }

    fn queue_entry(sequence: u64, body: MessageId) -> QueueEntry {
        QueueEntry {
            sequence,
            packet_id: Some(u16::try_from(sequence % 1_000).expect("in range") + 1),
            delivery: delivery(body),
        }
    }

    fn inflight(packet_id: u16, sequence: u64, body: MessageId) -> InflightRecord {
        InflightRecord {
            packet_id,
            sequence,
            state: InflightState::AwaitingReceived,
            sent_at: 1_700_000_050,
            delivery: delivery(body),
        }
    }

    fn retained(payload: &'static str) -> RetainedRecord {
        RetainedRecord {
            payload: bytes::Bytes::from_static(payload.as_bytes()),
            qos: Qos::AtLeastOnce,
            expires_at: None,
            properties: bytes::Bytes::new(),
        }
    }

    // ---- reading what came back ----------------------------------------

    /// Apply a batch that is expected to be accepted.
    #[track_caller]
    fn apply(store: &impl Storage, records: &[Record]) {
        store
            .apply(records)
            .unwrap_or_else(|error| panic!("the store refused a batch it must accept: {error}"));
    }

    /// Restore, requiring success.
    #[track_caller]
    fn restore(store: &impl Storage) -> Restored {
        store.restore().expect("restoring must succeed")
    }

    /// The one refusal a batch produced, as a value to compare exactly.
    ///
    /// Convention 2: asserting `Err(_)`, or even `Err(Rejected(_))`, would
    /// pass for a refusal with an entirely different cause, so every
    /// caller compares the whole [`Rejection`].
    #[track_caller]
    fn refusal(outcome: Result<(), StorageError>) -> Rejection {
        match outcome {
            Err(StorageError::Rejected(rejection)) => rejection,
            Err(other) => panic!("expected a refusal, and the store failed with {other:?}"),
            Ok(()) => panic!("expected a refusal, and the batch was accepted"),
        }
    }

    /// The restored session under this key, or a failure naming what was
    /// there instead.
    #[track_caller]
    fn session_of<'a>(restored: &'a Restored, client_id: &str) -> &'a RestoredSession {
        let wanted = key(client_id);
        restored
            .sessions
            .iter()
            .find(|session| session.session.key == wanted)
            .unwrap_or_else(|| {
                let held: Vec<_> = restored
                    .sessions
                    .iter()
                    .map(|session| session.session.key.client_id.clone())
                    .collect();
                panic!("no session {client_id:?} was restored; the store held {held:?}")
            })
    }

    fn holds_session(restored: &Restored, client_id: &str) -> bool {
        let wanted = key(client_id);
        restored
            .sessions
            .iter()
            .any(|session| session.session.key == wanted)
    }

    /// Filters held, as a set: the trait promises no order for these.
    fn filters(session: &RestoredSession) -> BTreeSet<&str> {
        session
            .subscriptions
            .iter()
            .map(|subscription| subscription.filter.as_str())
            .collect()
    }

    /// Sequence numbers of the queue, in the order they came back.
    fn sequences(session: &RestoredSession) -> Vec<u64> {
        session.queue.iter().map(|entry| entry.sequence).collect()
    }

    /// A store holding one session, which most tests need before anything
    /// else is interesting.
    #[track_caller]
    fn with_session<S: Storage>(new: impl Fn() -> S, client_id: &str) -> S {
        let store = new();
        apply(&store, &[Record::SessionPut(session(client_id))]);
        store
    }

    // ---- each record type, stored and restored --------------------------

    /// A store nobody has written to restores to nothing, rather than to
    /// an error or to a phantom session.
    pub fn an_empty_store_restores_to_nothing<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        let restored = restore(&store);
        assert_eq!(
            restored,
            Restored::default(),
            "an untouched store held {restored:?}"
        );
    }

    /// Reading a session that does not exist answers emptily rather than
    /// failing, because the broker asks before it knows.
    pub fn an_unknown_session_reads_as_empty<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        let absent = key("never-connected");
        assert_eq!(store.queue_depth(&absent).expect("depth must answer"), 0);
        assert_eq!(
            store
                .highest_sequence(&absent)
                .expect("the high-water mark must answer"),
            0
        );
        assert_eq!(
            store
                .queue_head(&absent, 0)
                .expect("the head read must answer"),
            None
        );
    }

    /// Every durable field of a session comes back, not just its key.
    pub fn a_session_restores_with_every_field_it_was_given<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        let mut record = session("sensor");
        record.version = ProtocolVersion::V311;
        record.expiry_interval = 120;
        record.expires_at = Some(1_700_000_500);
        record.will = Some(storage::WillRecord {
            topic: "gone".to_owned(),
            payload: bytes::Bytes::from_static(b"bye"),
            qos: Qos::ExactlyOnce,
            retain: true,
            delay: 30,
            properties: bytes::Bytes::from_static(&[0x02]),
        });
        record.will_at = Some(1_700_000_530);

        apply(&store, &[Record::SessionPut(record.clone())]);

        let restored = restore(&store);
        assert_eq!(session_of(&restored, "sensor").session, record);
    }

    /// A second put changes the session's own fields and leaves everything
    /// that hangs off it alone.
    ///
    /// The broker writes one of these whenever an expiry or a will
    /// changes, which is in the middle of a live session. An
    /// implementation that inserted a fresh empty session would lose a
    /// queue on a reconnect and nothing else here would see it.
    pub fn a_second_session_put_updates_without_discarding_what_it_holds<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = with_session(new, "sensor");
        apply(
            &store,
            &[
                Record::SubscriptionAdded {
                    session: key("sensor"),
                    subscription: subscription("a/b"),
                },
                Record::InboundReserved {
                    session: key("sensor"),
                    packet_id: 7,
                },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::InflightPut {
                    session: key("sensor"),
                    entry: inflight(4, 1, body_id(1)),
                },
            ],
        );

        let mut changed = session("sensor");
        changed.expiry_interval = 60;
        changed.expires_at = None;
        apply(&store, &[Record::SessionPut(changed.clone())]);

        let restored = restore(&store);
        let held = session_of(&restored, "sensor");
        assert_eq!(held.session, changed, "the put did not take effect");
        assert_eq!(filters(held), BTreeSet::from(["a/b"]));
        assert_eq!(held.inbound, vec![7]);
        assert_eq!(sequences(held), vec![1]);
        assert_eq!(held.inflight.len(), 1, "inflight: {:?}", held.inflight);
        assert_eq!(
            store.queue_depth(&key("sensor")).expect("depth"),
            1,
            "the put reset the depth"
        );
        assert_eq!(
            store
                .highest_sequence(&key("sensor"))
                .expect("the high-water mark"),
            1,
            "the put reset the high-water mark, so a sequence could be reused"
        );
    }

    /// Subscriptions are held, replaced by filter, and removed.
    pub fn subscriptions_are_held_replaced_and_removed<S: Storage>(new: impl Fn() -> S) {
        let store = with_session(new, "sensor");
        apply(
            &store,
            &[
                Record::SubscriptionAdded {
                    session: key("sensor"),
                    subscription: subscription("a/b"),
                },
                Record::SubscriptionAdded {
                    session: key("sensor"),
                    subscription: subscription("c/#"),
                },
            ],
        );

        // Re-subscribing to a held filter replaces its options rather than
        // adding a second subscription to the same filter: section 5.2's
        // subscription is identified by its filter.
        let mut relaxed = subscription("a/b");
        relaxed.qos = Qos::AtMostOnce;
        relaxed.no_local = false;
        relaxed.subscription_id = None;
        apply(
            &store,
            &[Record::SubscriptionAdded {
                session: key("sensor"),
                subscription: relaxed.clone(),
            }],
        );

        let restored = restore(&store);
        let held = session_of(&restored, "sensor");
        assert_eq!(filters(held), BTreeSet::from(["a/b", "c/#"]));
        assert!(
            held.subscriptions.contains(&relaxed),
            "the replacement did not take: {:?}",
            held.subscriptions
        );

        apply(
            &store,
            &[Record::SubscriptionRemoved {
                session: key("sensor"),
                filter: "a/b".to_owned(),
            }],
        );
        let restored = restore(&store);
        assert_eq!(
            filters(session_of(&restored, "sensor")),
            BTreeSet::from(["c/#"])
        );

        // Unsubscribing from a filter that is not held changes nothing.
        apply(
            &store,
            &[Record::SubscriptionRemoved {
                session: key("sensor"),
                filter: "a/b".to_owned(),
            }],
        );
        let restored = restore(&store);
        assert_eq!(
            filters(session_of(&restored, "sensor")),
            BTreeSet::from(["c/#"])
        );
    }

    /// Inbound identifiers are a set: reserved, held once however often
    /// reserved, and released.
    ///
    /// Held once matters because the peer sends one release. A store
    /// counting reservations would keep an identifier the peer believes it
    /// has given back, and the next publish under that identifier would be
    /// silently dropped as a duplicate for the life of the session.
    pub fn inbound_identifiers_are_reserved_once_and_released<S: Storage>(new: impl Fn() -> S) {
        let store = with_session(new, "sensor");
        apply(
            &store,
            &[
                Record::InboundReserved {
                    session: key("sensor"),
                    packet_id: 9,
                },
                Record::InboundReserved {
                    session: key("sensor"),
                    packet_id: 3,
                },
                Record::InboundReserved {
                    session: key("sensor"),
                    packet_id: 9,
                },
            ],
        );

        let restored = restore(&store);
        let mut held = session_of(&restored, "sensor").inbound.clone();
        held.sort_unstable();
        assert_eq!(held, vec![3, 9]);

        apply(
            &store,
            &[
                Record::InboundReleased {
                    session: key("sensor"),
                    packet_id: 9,
                },
                // A release of something never reserved is a no-op, not a
                // refusal: the postcondition already holds.
                Record::InboundReleased {
                    session: key("sensor"),
                    packet_id: 40,
                },
            ],
        );
        let restored = restore(&store);
        assert_eq!(session_of(&restored, "sensor").inbound, vec![3]);
    }

    /// Inflight entries are held by packet identifier, updated in place as
    /// the exchange advances, and removed when it finishes.
    pub fn inflight_entries_are_held_updated_and_removed<S: Storage>(new: impl Fn() -> S) {
        let store = with_session(new, "sensor");
        let first = inflight(4, 11, body_id(1));
        let second = inflight(5, 12, body_id(2));
        apply(
            &store,
            &[
                Record::InflightPut {
                    session: key("sensor"),
                    entry: first.clone(),
                },
                Record::InflightPut {
                    session: key("sensor"),
                    entry: second.clone(),
                },
            ],
        );

        let restored = restore(&store);
        let held = session_of(&restored, "sensor");
        assert!(held.inflight.contains(&first), "held {:?}", held.inflight);
        assert!(held.inflight.contains(&second));
        assert_eq!(held.inflight.len(), 2);

        // Section 6: the receipt moves the exchange on, keeping the same
        // identifier, and a resumed message is released rather than
        // republished. That is an update under the identifier already
        // there, not a second entry.
        let mut advanced = first.clone();
        advanced.state = InflightState::AwaitingComplete;
        apply(
            &store,
            &[Record::InflightPut {
                session: key("sensor"),
                entry: advanced.clone(),
            }],
        );
        let restored = restore(&store);
        let held = session_of(&restored, "sensor");
        assert_eq!(held.inflight.len(), 2, "held {:?}", held.inflight);
        assert!(held.inflight.contains(&advanced));

        apply(
            &store,
            &[
                Record::InflightRemoved {
                    session: key("sensor"),
                    packet_id: 4,
                },
                // Acknowledging something already finished is a no-op.
                Record::InflightRemoved {
                    session: key("sensor"),
                    packet_id: 4,
                },
            ],
        );
        let restored = restore(&store);
        assert_eq!(session_of(&restored, "sensor").inflight, vec![second]);
    }

    /// A queue entry comes back carrying everything it was given,
    /// including the identifier a resumed publish must keep.
    ///
    /// Convention 3's worst instance was exactly this field going missing
    /// between a spec sentence and the code.
    pub fn a_queue_entry_restores_with_everything_it_carries<S: Storage>(new: impl Fn() -> S) {
        let store = with_session(new, "sensor");
        let entry = QueueEntry {
            sequence: 1,
            packet_id: Some(1234),
            delivery: DeliveryRecord {
                body: body_id(1),
                qos: Qos::ExactlyOnce,
                retain: true,
                subscription_ids: vec![3, 4],
                dup: true,
            },
        };
        apply(
            &store,
            &[
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::QueuePushed {
                    session: key("sensor"),
                    entry: entry.clone(),
                },
            ],
        );

        let restored = restore(&store);
        assert_eq!(session_of(&restored, "sensor").queue, vec![entry.clone()]);
        assert_eq!(
            store
                .queue_head(&key("sensor"), 0)
                .expect("the head read must answer"),
            Some(entry)
        );
    }

    /// Retained messages are held by topic, replaced, and removed.
    pub fn retained_messages_are_held_replaced_and_removed<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        apply(
            &store,
            &[
                Record::RetainedPut {
                    topic: "a/b".to_owned(),
                    message: retained("first"),
                },
                Record::RetainedPut {
                    topic: "c/d".to_owned(),
                    message: retained("other"),
                },
                Record::RetainedPut {
                    topic: "a/b".to_owned(),
                    message: retained("second"),
                },
            ],
        );

        let mut held = restore(&store).retained;
        held.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            held,
            vec![
                ("a/b".to_owned(), retained("second")),
                ("c/d".to_owned(), retained("other")),
            ]
        );

        apply(
            &store,
            &[
                Record::RetainedRemoved {
                    topic: "a/b".to_owned(),
                },
                // Clearing a topic that holds nothing is a no-op.
                Record::RetainedRemoved {
                    topic: "z/z".to_owned(),
                },
            ],
        );
        assert_eq!(
            restore(&store).retained,
            vec![("c/d".to_owned(), retained("other"))]
        );
    }

    /// A body comes back with every field, and is held once however many
    /// entries name it.
    pub fn a_body_restores_once_with_every_field<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        apply(
            &store,
            &[
                Record::SessionPut(session("one")),
                Record::SessionPut(session("two")),
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::QueuePushed {
                    session: key("one"),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::QueuePushed {
                    session: key("two"),
                    entry: queue_entry(1, body_id(1)),
                },
            ],
        );

        let restored = restore(&store);
        assert_eq!(
            restored.bodies.len(),
            1,
            "held {:?}",
            restored.bodies.keys()
        );
        assert_eq!(restored.bodies.get(&body_id(1)), Some(&body(1)));
    }

    // ---- a batch is all or nothing --------------------------------------

    /// Records inside one batch see each other, in order.
    ///
    /// Without this a caller could not create a session and fill it in one
    /// batch, which is what a connecting client's first write is.
    pub fn a_batch_applies_all_of_itself_in_order<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        apply(
            &store,
            &[
                Record::SessionPut(session("sensor")),
                Record::SubscriptionAdded {
                    session: key("sensor"),
                    subscription: subscription("a/b"),
                },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::QueuePopped {
                    session: key("sensor"),
                    sequence: 1,
                },
            ],
        );

        let restored = restore(&store);
        let held = session_of(&restored, "sensor");
        assert_eq!(filters(held), BTreeSet::from(["a/b"]));
        assert_eq!(sequences(held), Vec::<u64>::new());
        assert_eq!(store.queue_depth(&key("sensor")).expect("depth"), 0);
        assert_eq!(
            store
                .highest_sequence(&key("sensor"))
                .expect("the high-water mark"),
            1
        );
    }

    /// A batch that fails leaves nothing of itself behind, including the
    /// records that came before the one that failed.
    ///
    /// This is the test the whole atomicity claim rests on. Asserting that
    /// a batch which succeeds took effect would pass against an
    /// implementation with no atomicity at all, so the batch here does
    /// four things that are each individually legal and then one that
    /// cannot be, and every one of the four is checked to have left no
    /// trace.
    pub fn a_batch_that_fails_leaves_nothing_of_itself_behind<S: Storage>(new: impl Fn() -> S) {
        let store = with_session(new, "sensor");
        apply(
            &store,
            &[
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(1, body_id(1)),
                },
            ],
        );
        let before = restore(&store);

        let rejection = refusal(store.apply(&[
            Record::SessionPut(session("newcomer")),
            Record::SubscriptionAdded {
                session: key("sensor"),
                subscription: subscription("a/b"),
            },
            Record::InboundReserved {
                session: key("sensor"),
                packet_id: 8,
            },
            Record::BodyPut {
                id: body_id(2),
                body: body(2),
            },
            Record::RetainedPut {
                topic: "a/b".to_owned(),
                message: retained("first"),
            },
            // Sequence 1 was assigned above. Reusing it is the one thing
            // section 7.4 says must never happen.
            Record::QueuePushed {
                session: key("sensor"),
                entry: queue_entry(1, body_id(2)),
            },
        ]));
        assert_eq!(
            rejection,
            Rejection::SequenceReused {
                session: key("sensor"),
                sequence: 1,
                highest: 1,
            }
        );

        let after = restore(&store);
        assert!(
            !holds_session(&after, "newcomer"),
            "a session created in a failed batch survived it"
        );
        let held = session_of(&after, "sensor");
        assert_eq!(filters(held), BTreeSet::new(), "a subscription survived");
        assert_eq!(
            held.inbound,
            Vec::<u16>::new(),
            "an inbound identifier survived"
        );
        assert_eq!(
            after.bodies.keys().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([body_id(1)]),
            "a body stored in a failed batch survived it"
        );
        assert_eq!(after.retained, Vec::new(), "a retained message survived");
        assert_eq!(after, before, "the failed batch changed the store");
        assert_eq!(store.queue_depth(&key("sensor")).expect("depth"), 1);
        assert_eq!(
            store
                .highest_sequence(&key("sensor"))
                .expect("the high-water mark"),
            1
        );

        // And the store is still usable: a refusal is not a poisoning.
        apply(
            &store,
            &[Record::QueuePushed {
                session: key("sensor"),
                entry: queue_entry(2, body_id(1)),
            }],
        );
        assert_eq!(store.queue_depth(&key("sensor")).expect("depth"), 2);
    }

    /// A record naming a session the store does not hold is refused, by
    /// name.
    ///
    /// Silently accepting it would file a queue entry where restore does
    /// not look; silently dropping it would lose a message and say
    /// nothing.
    pub fn a_record_for_an_unknown_session_is_refused<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        assert_eq!(
            refusal(store.apply(&[Record::SubscriptionAdded {
                session: key("ghost"),
                subscription: subscription("a/b"),
            }])),
            Rejection::NoSuchSession {
                session: key("ghost")
            }
        );
        assert_eq!(
            refusal(store.apply(&[Record::QueuePushed {
                session: key("ghost"),
                entry: queue_entry(1, body_id(1)),
            }])),
            Rejection::NoSuchSession {
                session: key("ghost")
            }
        );
        assert_eq!(
            refusal(store.apply(&[Record::InboundReserved {
                session: key("ghost"),
                packet_id: 1,
            }])),
            Rejection::NoSuchSession {
                session: key("ghost")
            }
        );
        assert_eq!(
            refusal(store.apply(&[Record::InflightPut {
                session: key("ghost"),
                entry: inflight(1, 1, body_id(1)),
            }])),
            Rejection::NoSuchSession {
                session: key("ghost")
            }
        );

        // Removing a session that is not there is idempotent, not a
        // refusal: the caller asked for it to be gone and it is.
        apply(
            &store,
            &[Record::SessionRemoved {
                session: key("ghost"),
            }],
        );
        assert_eq!(restore(&store), Restored::default());
    }

    // ---- a removed session takes everything with it ----------------------

    /// Removing a session removes its subscriptions, queue, inflight and
    /// inbound state, and its counters with them.
    ///
    /// Every part is checked separately because a partial cleanup is the
    /// realistic bug: whatever is left behind is inherited by the next
    /// session under the same client identifier, which is a live client
    /// receiving another's messages.
    pub fn a_removed_session_takes_everything_with_it<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        apply(
            &store,
            &[
                Record::SessionPut(session("sensor")),
                Record::SessionPut(session("bystander")),
                Record::SubscriptionAdded {
                    session: key("sensor"),
                    subscription: subscription("a/b"),
                },
                Record::InboundReserved {
                    session: key("sensor"),
                    packet_id: 7,
                },
                Record::InflightPut {
                    session: key("sensor"),
                    entry: inflight(4, 1, body_id(1)),
                },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(2, body_id(1)),
                },
                // The bystander holds the same shapes of state, so a
                // removal that takes too much is visible too.
                Record::SubscriptionAdded {
                    session: key("bystander"),
                    subscription: subscription("a/b"),
                },
                Record::InboundReserved {
                    session: key("bystander"),
                    packet_id: 7,
                },
                Record::QueuePushed {
                    session: key("bystander"),
                    entry: queue_entry(1, body_id(1)),
                },
            ],
        );

        apply(
            &store,
            &[Record::SessionRemoved {
                session: key("sensor"),
            }],
        );

        let restored = restore(&store);
        assert!(
            !holds_session(&restored, "sensor"),
            "the removed session is still restored"
        );
        assert_eq!(
            store.queue_depth(&key("sensor")).expect("depth"),
            0,
            "the removed session left its depth behind"
        );
        assert_eq!(
            store
                .highest_sequence(&key("sensor"))
                .expect("the high-water mark"),
            0,
            "the removed session left its high-water mark behind"
        );
        assert_eq!(
            store
                .queue_head(&key("sensor"), 0)
                .expect("the head read must answer"),
            None,
            "the removed session left its queue behind"
        );

        let bystander = session_of(&restored, "bystander");
        assert_eq!(filters(bystander), BTreeSet::from(["a/b"]));
        assert_eq!(bystander.inbound, vec![7]);
        assert_eq!(sequences(bystander), vec![1]);
    }

    /// A session created again under the same key inherits nothing from
    /// the one that was removed.
    ///
    /// The test above asks the store what it holds; this one asks through
    /// the door a reconnecting client comes in by, because the state that
    /// matters is the state the new session can see. A store that removed
    /// the session record and kept the rest passes nothing here.
    pub fn a_session_recreated_under_the_same_key_inherits_nothing<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        apply(
            &store,
            &[
                Record::SessionPut(session("sensor")),
                Record::SubscriptionAdded {
                    session: key("sensor"),
                    subscription: subscription("secret/#"),
                },
                Record::InboundReserved {
                    session: key("sensor"),
                    packet_id: 7,
                },
                Record::InflightPut {
                    session: key("sensor"),
                    entry: inflight(4, 1, body_id(1)),
                },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::SessionRemoved {
                    session: key("sensor"),
                },
                Record::SessionPut(session("sensor")),
            ],
        );

        let restored = restore(&store);
        let fresh = session_of(&restored, "sensor");
        assert_eq!(
            filters(fresh),
            BTreeSet::new(),
            "it inherited a subscription"
        );
        assert_eq!(
            fresh.inbound,
            Vec::<u16>::new(),
            "it inherited an inbound identifier"
        );
        assert_eq!(fresh.inflight, Vec::new(), "it inherited an inflight entry");
        assert_eq!(
            sequences(fresh),
            Vec::<u64>::new(),
            "it inherited a queue entry"
        );
        assert_eq!(fresh.highest_sequence, 0);
        assert_eq!(store.queue_depth(&key("sensor")).expect("depth"), 0);

        // And the new session may start from sequence one, which it could
        // not if the mark had survived.
        apply(
            &store,
            &[Record::QueuePushed {
                session: key("sensor"),
                entry: queue_entry(1, body_id(1)),
            }],
        );
        assert_eq!(store.queue_depth(&key("sensor")).expect("depth"), 1);
    }

    // ---- depth, sequence and the head read -------------------------------

    /// Depth counts what is held, through pushes, pops and a pop of
    /// something already gone.
    pub fn depth_tracks_pushes_and_pops<S: Storage>(new: impl Fn() -> S) {
        let store = with_session(new, "sensor");
        let session_key = key("sensor");
        assert_eq!(store.queue_depth(&session_key).expect("depth"), 0);

        for sequence in 1..=3 {
            apply(
                &store,
                &[Record::QueuePushed {
                    session: session_key.clone(),
                    entry: queue_entry(sequence, body_id(1)),
                }],
            );
            assert_eq!(
                store.queue_depth(&session_key).expect("depth"),
                sequence,
                "depth did not follow the push of {sequence}"
            );
        }

        apply(
            &store,
            &[Record::QueuePopped {
                session: session_key.clone(),
                sequence: 2,
            }],
        );
        assert_eq!(store.queue_depth(&session_key).expect("depth"), 2);

        // Popping the same entry again must not take the depth below what
        // is held: the entry is already gone, so there is nothing to
        // subtract. A depth that drifts down is a queue that reports empty
        // while still holding messages.
        apply(
            &store,
            &[Record::QueuePopped {
                session: session_key.clone(),
                sequence: 2,
            }],
        );
        assert_eq!(
            store.queue_depth(&session_key).expect("depth"),
            2,
            "a second pop of the same entry moved the depth"
        );

        // Nor a pop of something never pushed.
        apply(
            &store,
            &[Record::QueuePopped {
                session: session_key.clone(),
                sequence: 99,
            }],
        );
        assert_eq!(store.queue_depth(&session_key).expect("depth"), 2);

        apply(
            &store,
            &[
                Record::QueuePopped {
                    session: session_key.clone(),
                    sequence: 1,
                },
                Record::QueuePopped {
                    session: session_key.clone(),
                    sequence: 3,
                },
            ],
        );
        assert_eq!(store.queue_depth(&session_key).expect("depth"), 0);
        assert_eq!(
            restore(&store).sessions[0].queue,
            Vec::new(),
            "the queue is empty but the entries came back"
        );
    }

    /// Depth is per session.
    pub fn queues_are_counted_per_session<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        apply(
            &store,
            &[
                Record::SessionPut(session("one")),
                Record::SessionPut(session("two")),
                Record::QueuePushed {
                    session: key("one"),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::QueuePushed {
                    session: key("one"),
                    entry: queue_entry(2, body_id(1)),
                },
                Record::QueuePushed {
                    session: key("two"),
                    entry: queue_entry(1, body_id(1)),
                },
            ],
        );
        assert_eq!(store.queue_depth(&key("one")).expect("depth"), 2);
        assert_eq!(store.queue_depth(&key("two")).expect("depth"), 1);
        assert_eq!(store.highest_sequence(&key("one")).expect("mark"), 2);
        assert_eq!(store.highest_sequence(&key("two")).expect("mark"), 1);
    }

    /// A sequence number is never reused, whether or not its entry is
    /// still held.
    ///
    /// The high-water mark is the whole point: after a queue drains it is
    /// empty and the next number must still be higher than every number
    /// that ever was. A store answering from what it holds would restart
    /// at one, and the reused number would collide with an inflight entry
    /// that kept it.
    pub fn a_sequence_number_is_never_reused_after_a_pop<S: Storage>(new: impl Fn() -> S) {
        let store = with_session(new, "sensor");
        let session_key = key("sensor");
        apply(
            &store,
            &[
                Record::QueuePushed {
                    session: session_key.clone(),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::QueuePushed {
                    session: session_key.clone(),
                    entry: queue_entry(2, body_id(1)),
                },
                Record::QueuePopped {
                    session: session_key.clone(),
                    sequence: 1,
                },
                Record::QueuePopped {
                    session: session_key.clone(),
                    sequence: 2,
                },
            ],
        );

        assert_eq!(store.queue_depth(&session_key).expect("depth"), 0);
        assert_eq!(
            store.highest_sequence(&session_key).expect("the mark"),
            2,
            "the mark fell back to what is held"
        );
        assert_eq!(restore(&store).sessions[0].highest_sequence, 2);

        for reused in [1, 2] {
            assert_eq!(
                refusal(store.apply(&[Record::QueuePushed {
                    session: session_key.clone(),
                    entry: queue_entry(reused, body_id(1)),
                }])),
                Rejection::SequenceReused {
                    session: session_key.clone(),
                    sequence: reused,
                    highest: 2,
                }
            );
        }

        apply(
            &store,
            &[Record::QueuePushed {
                session: session_key.clone(),
                entry: queue_entry(3, body_id(1)),
            }],
        );
        assert_eq!(store.highest_sequence(&session_key).expect("the mark"), 3);
    }

    /// The head read at a cursor answers with the first entry still held
    /// at or after it, skipping the holes that pops leave.
    ///
    /// Asserting only that it returns something would pass against a store
    /// that ignored the cursor entirely, so every read below names the
    /// entry it must get and at least one of them would be answered wrongly
    /// by a store returning the lowest key it holds.
    pub fn the_head_read_at_a_cursor_skips_what_was_popped<S: Storage>(new: impl Fn() -> S) {
        let store = with_session(new, "sensor");
        let session_key = key("sensor");
        for sequence in 1..=5 {
            apply(
                &store,
                &[Record::QueuePushed {
                    session: session_key.clone(),
                    entry: queue_entry(sequence, body_id(1)),
                }],
            );
        }
        apply(
            &store,
            &[
                Record::QueuePopped {
                    session: session_key.clone(),
                    sequence: 1,
                },
                Record::QueuePopped {
                    session: session_key.clone(),
                    sequence: 2,
                },
                Record::QueuePopped {
                    session: session_key.clone(),
                    sequence: 4,
                },
            ],
        );

        let head = |cursor: u64| {
            store
                .queue_head(&session_key, cursor)
                .expect("the head read must answer")
                .map(|entry| entry.sequence)
        };

        // From the beginning, the two popped entries are skipped.
        assert_eq!(head(0), Some(3), "the head read returned a popped entry");
        assert_eq!(head(1), Some(3));
        assert_eq!(head(3), Some(3), "the cursor is inclusive");
        // Across the hole four leaves.
        assert_eq!(head(4), Some(5), "the head read stopped at a hole");
        assert_eq!(head(5), Some(5));
        // Past everything held.
        assert_eq!(head(6), None);
        assert_eq!(head(u64::MAX), None);

        // And it is the whole entry, not just a sequence number.
        assert_eq!(
            store
                .queue_head(&session_key, 0)
                .expect("the head read must answer"),
            Some(queue_entry(3, body_id(1)))
        );

        // Draining the rest empties it rather than wrapping round.
        apply(
            &store,
            &[
                Record::QueuePopped {
                    session: session_key.clone(),
                    sequence: 3,
                },
                Record::QueuePopped {
                    session: session_key.clone(),
                    sequence: 5,
                },
            ],
        );
        assert_eq!(head(0), None);
    }

    /// The queue restores in sequence order, including across the byte
    /// boundaries a naively encoded key sorts wrongly.
    ///
    /// 255, 256 and 257 are here because a key written in the machine's
    /// own byte order sorts 256 before 2, and the queue comes back in an
    /// order that is only wrong for large queues, which is to say only in
    /// production.
    pub fn the_queue_restores_in_sequence_order<S: Storage>(new: impl Fn() -> S) {
        let store = with_session(new, "sensor");
        let sequences_pushed = [1u64, 2, 255, 256, 257, 65_535, 65_536, 1_000_000];
        for sequence in sequences_pushed {
            apply(
                &store,
                &[Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(sequence, body_id(1)),
                }],
            );
        }

        let restored = restore(&store);
        assert_eq!(sequences(session_of(&restored, "sensor")), sequences_pushed);
        assert_eq!(
            store
                .queue_head(&key("sensor"), 3)
                .expect("the head read must answer")
                .map(|entry| entry.sequence),
            Some(255)
        );
    }

    // ---- a body lives exactly as long as its references ------------------

    /// A body outlives every release but its last.
    ///
    /// The losing direction: a body deleted while a queue entry still
    /// names it is a message gone, and a restore that cannot rebuild the
    /// entry that named it.
    pub fn a_body_survives_while_a_reference_remains<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        apply(
            &store,
            &[
                Record::SessionPut(session("one")),
                Record::SessionPut(session("two")),
                Record::SessionPut(session("three")),
                // Three references, one per queue entry.
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::QueuePushed {
                    session: key("one"),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::QueuePushed {
                    session: key("two"),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::QueuePushed {
                    session: key("three"),
                    entry: queue_entry(1, body_id(1)),
                },
            ],
        );

        for released in 1..=2 {
            apply(
                &store,
                &[
                    Record::QueuePopped {
                        session: key(if released == 1 { "one" } else { "two" }),
                        sequence: 1,
                    },
                    Record::BodyReleased { id: body_id(1) },
                ],
            );
            let restored = restore(&store);
            assert_eq!(
                restored.bodies.get(&body_id(1)),
                Some(&body(1)),
                "the body went after {released} of three references were released"
            );
        }
    }

    /// And goes on the release after its last, rather than lingering.
    ///
    /// The leaking direction: a body kept past its last reference is never
    /// reclaimed by anything, because nothing will ever release it again.
    pub fn a_body_goes_when_its_last_reference_is_released<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        apply(
            &store,
            &[
                Record::SessionPut(session("one")),
                Record::SessionPut(session("two")),
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                // A second body, to catch a release that empties the lot.
                Record::BodyPut {
                    id: body_id(2),
                    body: body(2),
                },
                Record::QueuePushed {
                    session: key("one"),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::QueuePushed {
                    session: key("two"),
                    entry: queue_entry(1, body_id(1)),
                },
            ],
        );

        apply(
            &store,
            &[
                Record::QueuePopped {
                    session: key("one"),
                    sequence: 1,
                },
                Record::BodyReleased { id: body_id(1) },
                Record::QueuePopped {
                    session: key("two"),
                    sequence: 1,
                },
                Record::BodyReleased { id: body_id(1) },
            ],
        );

        let restored = restore(&store);
        assert_eq!(
            restored.bodies.keys().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([body_id(2)]),
            "the body survived its last reference"
        );
    }

    /// Storing a body again after it was released is a new first
    /// reference, not a resurrection of the old count.
    ///
    /// The same message identifier can be published again, and a store
    /// that remembered a count of zero as "still here" would keep the new
    /// body alive one release too long, or delete it immediately.
    pub fn a_body_stored_again_after_release_starts_from_one<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        apply(
            &store,
            &[
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::BodyReleased { id: body_id(1) },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(2),
                },
            ],
        );
        assert_eq!(restore(&store).bodies.get(&body_id(1)), Some(&body(2)));

        apply(&store, &[Record::BodyReleased { id: body_id(1) }]);
        assert!(
            restore(&store).bodies.is_empty(),
            "the second put's reference outlived its release"
        );
    }

    /// Releasing a body the store does not hold is refused rather than
    /// clamped.
    ///
    /// Clamping at zero would turn a caller's double release into a body
    /// deleted under a live reference, which is the loss the count exists
    /// to prevent, and it would be silent.
    pub fn releasing_a_body_that_is_not_held_is_refused<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        assert_eq!(
            refusal(store.apply(&[Record::BodyReleased { id: body_id(1) }])),
            Rejection::BodyNotHeld { id: body_id(1) }
        );

        apply(
            &store,
            &[
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::BodyReleased { id: body_id(1) },
            ],
        );
        assert_eq!(
            refusal(store.apply(&[Record::BodyReleased { id: body_id(1) }])),
            Rejection::BodyNotHeld { id: body_id(1) },
            "a release past the last one was accepted"
        );
    }

    /// Nothing but a release drops a reference.
    ///
    /// Popping an entry does not release the body it named, because for
    /// anything above at-most-once the inflight entry that follows the pop
    /// names the same body: a store that released on pop would delete it
    /// between the two records. This is a decision the trait documents on
    /// `Storage::apply` rather than an accident of the first backend, and
    /// it is asserted here so that both backends make it.
    pub fn a_pop_does_not_release_the_body_it_named<S: Storage>(new: impl Fn() -> S) {
        let store = with_session(new, "sensor");
        apply(
            &store,
            &[
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(1, body_id(1)),
                },
            ],
        );

        // The message leaves the queue and enters the inflight window
        // under the identifier it will be acknowledged by. One reference,
        // held throughout.
        apply(
            &store,
            &[
                Record::QueuePopped {
                    session: key("sensor"),
                    sequence: 1,
                },
                Record::InflightPut {
                    session: key("sensor"),
                    entry: inflight(4, 1, body_id(1)),
                },
            ],
        );

        let restored = restore(&store);
        assert_eq!(
            restored.bodies.get(&body_id(1)),
            Some(&body(1)),
            "the pop released the body the inflight entry still names"
        );
        assert_eq!(
            session_of(&restored, "sensor").inflight,
            vec![inflight(4, 1, body_id(1))]
        );

        // Removing a session does not release them either: the caller owns
        // the reference it took.
        apply(
            &store,
            &[Record::SessionRemoved {
                session: key("sensor"),
            }],
        );
        assert_eq!(
            restore(&store).bodies.get(&body_id(1)),
            Some(&body(1)),
            "removing a session released a body the store did not take"
        );
    }

    // ---- flush ------------------------------------------------------------

    /// A flush changes nothing about what the store holds, and a store
    /// goes on working after one.
    ///
    /// What a flush is actually for — that what it covered survives a
    /// restart — cannot be asked of a backend with nothing to restart, and
    /// is tested where it can fail, against redb.
    pub fn a_flush_keeps_everything_and_leaves_the_store_usable<S: Storage>(new: impl Fn() -> S) {
        let store = with_session(new, "sensor");
        apply(
            &store,
            &[Record::QueuePushed {
                session: key("sensor"),
                entry: queue_entry(1, body_id(1)),
            }],
        );
        let before = restore(&store);

        store.flush().expect("a flush must succeed");
        assert_eq!(restore(&store), before, "the flush changed what was held");

        apply(
            &store,
            &[Record::QueuePushed {
                session: key("sensor"),
                entry: queue_entry(2, body_id(1)),
            }],
        );
        store.flush().expect("a second flush must succeed");
        assert_eq!(store.queue_depth(&key("sensor")).expect("depth"), 2);
    }
}

/// Every contract test, named once.
///
/// The list lives here rather than beside each backend so that adding a
/// backend cannot quietly skip a case: a backend is one `every_contract_test!`
/// line and gets all of them or none.
macro_rules! every_contract_test {
    ($apply:ident, $($prefix:tt)*) => {
        $apply! {
            $($prefix)* ;
            an_empty_store_restores_to_nothing,
            an_unknown_session_reads_as_empty,
            a_session_restores_with_every_field_it_was_given,
            a_second_session_put_updates_without_discarding_what_it_holds,
            subscriptions_are_held_replaced_and_removed,
            inbound_identifiers_are_reserved_once_and_released,
            inflight_entries_are_held_updated_and_removed,
            a_queue_entry_restores_with_everything_it_carries,
            retained_messages_are_held_replaced_and_removed,
            a_body_restores_once_with_every_field,
            a_batch_applies_all_of_itself_in_order,
            a_batch_that_fails_leaves_nothing_of_itself_behind,
            a_record_for_an_unknown_session_is_refused,
            a_removed_session_takes_everything_with_it,
            a_session_recreated_under_the_same_key_inherits_nothing,
            depth_tracks_pushes_and_pops,
            queues_are_counted_per_session,
            a_sequence_number_is_never_reused_after_a_pop,
            the_head_read_at_a_cursor_skips_what_was_popped,
            the_queue_restores_in_sequence_order,
            a_body_survives_while_a_reference_remains,
            a_body_goes_when_its_last_reference_is_released,
            a_body_stored_again_after_release_starts_from_one,
            releasing_a_body_that_is_not_held_is_refused,
            a_pop_does_not_release_the_body_it_named,
            a_flush_keeps_everything_and_leaves_the_store_usable,
        }
    };
}

/// Run the whole contract against one backend, in a module named for it.
macro_rules! run_against {
    ($backend:ident, $new:expr ; $($case:ident),+ $(,)?) => {
        mod $backend {
            $(
                #[test]
                fn $case() {
                    super::contract::$case($new);
                }
            )+
        }
    };
}

every_contract_test!(run_against, memory, storage::MemoryStorage::new);
