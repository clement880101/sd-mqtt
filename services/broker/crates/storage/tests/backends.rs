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
    ActionRecord, BodyRecord, ClockRecord, DeliveryRecord, HttpOutRecord, InflightRecord,
    InflightState, LoggedRecord, MessageId, MetadataKeyRecord, MetadataRecord, MetadataValueRecord,
    NodeIdentityRecord, OutboxRecord, ProtocolVersion, Qos, QueueEntry, Record, Rejection,
    Restored, RestoredSession, RetainHandling, RetainedRecord, RuleRecord, SeenRecord, SessionKey,
    SessionRecord, StampRecord, Storage, StorageError, SubscriptionRecord, TriggerRecord,
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

    /// A rule with no field left at its default, for the same reason as
    /// the session above: a backend that drops one must be visible.
    ///
    /// `enabled` is false, because the flag a store can silently fail to
    /// keep is the one whose absence looks like the ordinary case.
    fn rule(id: &str) -> RuleRecord {
        RuleRecord {
            id: id.to_owned(),
            name: format!("rule {id}"),
            enabled: false,
            trigger: TriggerRecord {
                filter: "sensor/+/temp".to_owned(),
                min_qos: Some(Qos::ExactlyOnce),
                client_id: Some("sensor-*".to_owned()),
            },
            action: ActionRecord::HttpOut(HttpOutRecord {
                url: format!("https://example.invalid/{id}"),
                method: "PUT".to_owned(),
                headers: vec![("x-token".to_owned(), "secret".to_owned())],
                timeout_ms: 2_500,
                max_concurrency: 8,
                retries: 5,
                backoff_ms: 750,
            }),
        }
    }

    /// An outbox envelope with no field left at its default, so that a
    /// backend dropping one is visible.
    ///
    /// The attempt count and the next attempt's time are both non-zero
    /// and different from each other, which is the pair this record
    /// exists for: a backend that kept only one of them, or swapped
    /// them, must not be able to pass.
    fn envelope(sequence: u64, body: MessageId) -> OutboxRecord {
        OutboxRecord {
            sequence,
            id: body_id(200 + u8::try_from(sequence % 50).expect("in range")),
            body,
            client_id: format!("sensor-{sequence}"),
            enqueued_at: 1_700_000_000,
            attempts: 2,
            next_attempt_at: 1_700_000_017,
        }
    }

    /// A store holding one rule, which every outbox test needs first.
    #[track_caller]
    fn with_rule<S: Storage>(new: impl Fn() -> S, id: &str) -> S {
        let store = new();
        apply(&store, &[Record::RulePut { rule: rule(id) }]);
        store
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

    /// A put for a body the store already holds takes a reference and
    /// leaves the body alone.
    ///
    /// `Storage::apply` says an identifier names one publication for as
    /// long as it exists, so the body a second put carries is the body
    /// already held. It is written down as a rule because a backend
    /// relies on it: the redb one answers the second and thousandth
    /// reference without reading, decoding or rewriting the payload,
    /// which is what a fan-out to a thousand durable subscribers costs
    /// otherwise.
    ///
    /// Two bodies that differ is not something the broker can produce.
    /// The point is that both backends answer it the same way, because
    /// the suite compares them as values and "whatever each happens to
    /// do" is not a contract.
    pub fn a_body_put_again_keeps_the_body_it_already_holds<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        let mut second = body(1);
        second.payload = bytes::Bytes::from_static(b"a different body under the same name");
        apply(
            &store,
            &[
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::BodyPut {
                    id: body_id(1),
                    body: second,
                },
            ],
        );

        assert_eq!(
            restore(&store).bodies.get(&body_id(1)),
            Some(&body(1)),
            "the second put rewrote the body instead of only counting"
        );
        // And it did count: one release leaves the body, the second
        // takes it.
        apply(&store, &[Record::BodyReleased { id: body_id(1) }]);
        assert_eq!(restore(&store).bodies.get(&body_id(1)), Some(&body(1)));
        apply(&store, &[Record::BodyReleased { id: body_id(1) }]);
        assert!(restore(&store).bodies.is_empty());
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

    // ---- this node's own identity ----------------------------------------

    /// Section 8.1's node identity: absent until something says, then
    /// held, and a rename moves the name and nothing else.
    ///
    /// The last part is the one worth having. An operator renaming a
    /// node must not re-found it, and a store that replaced the row
    /// with whatever the put carried would be fine here — which is
    /// why the put below carries the *same* UUID and a different name,
    /// and the assertion is on both fields rather than on the name.
    /// What stops a caller minting a second UUID is `cluster`, and its
    /// own tests say so.
    pub fn the_node_identity_is_held_and_renamed_without_re_founding<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        assert_eq!(
            restore(&store).node,
            None,
            "a store that has never been told who it is says so, rather than \
             guessing"
        );

        let minted = NodeIdentityRecord {
            uuid: [
                0x01, 0x93, 0x4e, 0x3a, 0x7c, 0x10, 0x70, 0x00, 0x80, 0x00, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f,
            ],
            name: "edge-1".to_owned(),
        };
        apply(
            &store,
            &[Record::NodeIdentityPut {
                identity: minted.clone(),
            }],
        );
        assert_eq!(restore(&store).node, Some(minted.clone()));

        let renamed = NodeIdentityRecord {
            name: "edge-one".to_owned(),
            ..minted.clone()
        };
        apply(
            &store,
            &[Record::NodeIdentityPut {
                identity: renamed.clone(),
            }],
        );
        let held = restore(&store).node.expect("the store still knows");
        assert_eq!(held.name, "edge-one", "the operator's rename took");
        assert_eq!(held.uuid, minted.uuid, "and it did not re-found the node");
    }

    // ---- this node's clock -----------------------------------------------

    /// Section 8.2's clock mark: absent until something writes one,
    /// then held, both halves of it.
    ///
    /// **The second put carries a lower reading and the store takes
    /// it.** That is not an oversight dressed up: `ClockRecord` says
    /// the store holds what it is given and the clock is the one thing
    /// that decides whether a reading moved forwards, for the same
    /// reason `Record::NodeIdentityPut` gives about a UUID. A backend
    /// that quietly kept the higher of the two would be a second
    /// opinion in the layer that cannot see the question, and would
    /// hide a clock that had started going backwards instead of letting
    /// `cluster`'s own tests catch it.
    ///
    /// The counter is asserted separately from the milliseconds,
    /// because a backend that stored only the milliseconds would leave
    /// a resumed clock up to a millisecond's worth of writes behind
    /// itself, which is the one thing section 8.2 says must not happen
    /// and the one thing a single equality on the millisecond would
    /// miss.
    pub fn the_clock_mark_is_held_with_both_of_its_halves<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        assert_eq!(
            restore(&store).clock,
            None,
            "a store no clock has written to says so, rather than answering zero, \
             which is a reading a clock could have issued"
        );

        let issued = ClockRecord {
            millis: 1_700_000_000_123,
            counter: 7,
        };
        apply(&store, &[Record::ClockHighWaterPut { reading: issued }]);
        let held = restore(&store).clock.expect("the store holds the mark");
        assert_eq!(held.millis, 1_700_000_000_123);
        assert_eq!(
            held.counter, 7,
            "the counter is half the reading and a store that dropped it would \
             resume a clock behind itself"
        );

        let later = ClockRecord {
            millis: 1_700_000_000_124,
            counter: 0,
        };
        apply(&store, &[Record::ClockHighWaterPut { reading: later }]);
        assert_eq!(restore(&store).clock, Some(later));

        let lower = ClockRecord {
            millis: 1,
            counter: 0,
        };
        apply(&store, &[Record::ClockHighWaterPut { reading: lower }]);
        assert_eq!(
            restore(&store).clock,
            Some(lower),
            "the store holds what it was given; whether a reading moved forwards \
             is the clock's question"
        );
    }

    // ---- the replicated registers ----------------------------------------

    /// A register, for the metadata tests below.
    fn register(
        key: MetadataKeyRecord,
        millis: u64,
        value: Option<MetadataValueRecord>,
    ) -> MetadataRecord {
        MetadataRecord {
            key,
            stamp: StampRecord {
                millis,
                counter: 3,
                node: [0x07; 16],
            },
            value,
        }
    }

    /// Section 8.2's registers: held, replaced whole, and restored **in
    /// key order**, which both backends must agree on.
    ///
    /// # The keys are chosen to catch a length-prefixed encoding
    ///
    /// `"a"`, `"ab"` and `"b"` sort as `a`, `ab`, `b` by content and as
    /// `a`, `b`, `ab` by length-then-content. A backend keying on a
    /// postcard encoding of the key would produce the second, because
    /// postcard writes a string as a length and then its bytes — and
    /// nothing else here would notice, because every register would
    /// still be present and whole. The restart property compares two
    /// backends' restores as one equality, so an order that differs is
    /// an equality that fails somewhere far from its cause.
    ///
    /// All four kinds appear, in a scrambled insertion order, against a
    /// hand-written expected order. Sorting a clone to build the
    /// expectation would be convention 6's harness that mirrors the
    /// implementation: it would accept whatever order the keys happened
    /// to have.
    pub fn metadata_registers_are_held_replaced_and_restored_in_key_order<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        assert!(
            restore(&store).metadata.is_empty(),
            "an empty store holds no registers"
        );

        let subscription = |node: [u8; 16], filter: &str| MetadataKeyRecord::NodeSubscription {
            node,
            filter: filter.to_owned(),
        };
        let expected = vec![
            register(
                subscription([1; 16], "a"),
                10,
                Some(MetadataValueRecord::NodeSubscription),
            ),
            register(
                subscription([1; 16], "ab"),
                11,
                Some(MetadataValueRecord::NodeSubscription),
            ),
            // A tombstone, so the order is proved over one too: a
            // backend that filed deletions elsewhere would pass a test
            // whose every register carried a value.
            register(subscription([1; 16], "b"), 12, None),
            register(
                subscription([2; 16], "a"),
                13,
                Some(MetadataValueRecord::NodeSubscription),
            ),
            register(
                MetadataKeyRecord::Retained {
                    topic: "a".to_owned(),
                },
                14,
                Some(MetadataValueRecord::Retained(retained("warm"))),
            ),
            register(
                MetadataKeyRecord::Rule {
                    id: "telemetry".to_owned(),
                },
                15,
                Some(MetadataValueRecord::Rule(Box::new(rule("telemetry")))),
            ),
            register(
                MetadataKeyRecord::SessionOwner { session: key("a") },
                16,
                Some(MetadataValueRecord::SessionOwner {
                    node: [9; 16],
                    epoch: 3,
                }),
            ),
            register(
                MetadataKeyRecord::SessionOwner { session: key("ab") },
                17,
                Some(MetadataValueRecord::SessionOwner {
                    node: [9; 16],
                    epoch: 3,
                }),
            ),
        ];

        // Applied in an order that is not the key order, so that a
        // backend restoring in insertion order fails rather than
        // passing by coincidence.
        let scrambled = [7usize, 2, 5, 0, 6, 3, 1, 4];
        apply(
            &store,
            &scrambled
                .iter()
                .map(|index| Record::MetadataPut {
                    entry: expected[*index].clone(),
                })
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            restore(&store).metadata,
            expected,
            "the registers did not come back in key order"
        );

        // Replaced whole, including by a register whose stamp is
        // *older*: the store holds what it is given, and which of two
        // copies wins is the caller's one comparison. A backend with an
        // opinion about it would be a second answer in the layer that
        // cannot see the question.
        let older = register(subscription([1; 16], "a"), 1, None);
        apply(
            &store,
            &[Record::MetadataPut {
                entry: older.clone(),
            }],
        );
        let held = restore(&store).metadata;
        assert_eq!(
            held[0], older,
            "the store refused or merged a register, which is not its decision"
        );
        assert_eq!(
            held.len(),
            expected.len(),
            "replacing a register added one instead"
        );
    }

    /// Two nodes' subscriptions to one filter are two registers.
    ///
    /// The node is part of the key, because each node answers for its
    /// own subscribers. A backend that keyed on the filter alone would
    /// hold one register for the whole cluster, and the last node to
    /// subscribe would silently un-route every other.
    pub fn two_nodes_subscriptions_to_one_filter_are_two_registers<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        let filter = |node: [u8; 16]| MetadataKeyRecord::NodeSubscription {
            node,
            filter: "sensor/+/temp".to_owned(),
        };
        apply(
            &store,
            &[
                Record::MetadataPut {
                    entry: register(
                        filter([1; 16]),
                        10,
                        Some(MetadataValueRecord::NodeSubscription),
                    ),
                },
                Record::MetadataPut {
                    entry: register(
                        filter([2; 16]),
                        11,
                        Some(MetadataValueRecord::NodeSubscription),
                    ),
                },
            ],
        );
        let held = restore(&store).metadata;
        assert_eq!(held.len(), 2, "one filter, two nodes, two registers");
        assert_eq!(held[0].stamp.millis, 10);
        assert_eq!(held[1].stamp.millis, 11);
    }

    /// A register survives the session and the rule its key names.
    ///
    /// A session's removal takes everything filed under the session with
    /// it, and a rule's takes its outbox. Neither may take a register:
    /// the register is the cluster's record of who owned that session
    /// and of a rule every node holds, and a node that dropped it on a
    /// local removal would have the deletion travel nowhere while every
    /// peer went on holding the value.
    pub fn a_register_survives_the_session_and_the_rule_its_key_names<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        apply(
            &store,
            &[
                Record::SessionPut(session("a")),
                Record::RulePut {
                    rule: rule("telemetry"),
                },
                Record::MetadataPut {
                    entry: register(
                        MetadataKeyRecord::SessionOwner { session: key("a") },
                        10,
                        Some(MetadataValueRecord::SessionOwner {
                            node: [9; 16],
                            epoch: 3,
                        }),
                    ),
                },
                Record::MetadataPut {
                    entry: register(
                        MetadataKeyRecord::Rule {
                            id: "telemetry".to_owned(),
                        },
                        11,
                        Some(MetadataValueRecord::Rule(Box::new(rule("telemetry")))),
                    ),
                },
            ],
        );
        apply(
            &store,
            &[
                Record::SessionRemoved { session: key("a") },
                Record::RuleRemoved {
                    id: "telemetry".to_owned(),
                },
            ],
        );
        assert_eq!(
            restore(&store).metadata.len(),
            2,
            "a local removal took a replicated register with it"
        );
    }

    // ---- the operation log, and what has been read of other nodes' ------

    /// Section 8.2's log: appended in order, restored in order, and a
    /// sequence is never used twice.
    ///
    /// The order is the whole value of the table. An exchange asks for
    /// everything after a sequence, so a backend that restored these in
    /// any other order would serve a peer entries it already had and
    /// withhold the one it was missing — and every entry would still be
    /// present and whole, so nothing else here would notice.
    ///
    /// The sequences below are chosen to catch a native-endian key:
    /// 1, 2 and 256 sort as 1, 2, 256 big-endian and as 256, 1, 2 on a
    /// little-endian machine, which is every machine this runs on.
    pub fn the_operation_log_is_appended_in_order_and_refuses_a_reused_sequence<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        let restored = restore(&store);
        assert!(restored.oplog.is_empty(), "an empty store holds no log");
        assert_eq!(
            restored.oplog_collected_through, 0,
            "a node that has never originated a write has collected nothing"
        );

        for (sequence, millis) in [(1_u64, 10_u64), (2, 20), (256, 30)] {
            apply(
                &store,
                &[Record::OpLogAppended {
                    entry: LoggedRecord {
                        sequence,
                        entry: register(
                            MetadataKeyRecord::Retained {
                                topic: format!("t/{sequence}"),
                            },
                            millis,
                            None,
                        ),
                    },
                }],
            );
        }
        let restored = restore(&store);
        assert_eq!(
            restored
                .oplog
                .iter()
                .map(|logged| logged.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 256],
            "the log did not come back in sequence order"
        );
        assert_eq!(
            restored.oplog[2].entry.stamp.millis, 30,
            "an entry came back under the wrong sequence"
        );
        assert_eq!(
            restored.oplog_collected_through, 0,
            "appending does not collect anything"
        );

        // And a sequence at or below the highest is refused, which is
        // section 7.4's rule one table across: a log that restarted from
        // one would tell a peer "this is my seventh write" about two
        // different writes.
        for reused in [1_u64, 256] {
            let outcome = store.apply(&[Record::OpLogAppended {
                entry: LoggedRecord {
                    sequence: reused,
                    entry: register(
                        MetadataKeyRecord::Retained {
                            topic: "again".to_owned(),
                        },
                        40,
                        None,
                    ),
                },
            }]);
            match refusal(outcome) {
                Rejection::OpLogSequenceReused { sequence, highest } => {
                    assert_eq!(sequence, reused);
                    assert_eq!(highest, 256);
                }
                other => panic!("refused as {other:?}"),
            }
        }
        assert_eq!(
            restore(&store).oplog.len(),
            3,
            "a refused append left something behind"
        );
    }

    /// A truncated log loses its entries and keeps the numbers it spent.
    ///
    /// The second half is the one that matters. Truncation is driven by
    /// what every other node has read, so a healthy cluster's log is
    /// almost always empty — and a node that resumed from one because its
    /// table was empty would re-issue a sequence every peer had already
    /// been served, which is the same defect as a clock resuming from
    /// zero.
    pub fn a_truncated_operation_log_keeps_the_sequence_it_has_spent<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        for sequence in 1_u64..=4 {
            apply(
                &store,
                &[Record::OpLogAppended {
                    entry: LoggedRecord {
                        sequence,
                        entry: register(
                            MetadataKeyRecord::Retained {
                                topic: format!("t/{sequence}"),
                            },
                            sequence,
                            None,
                        ),
                    },
                }],
            );
        }

        // Inclusive of the sequence named, which is what a watermark
        // means: everybody has read through here.
        apply(&store, &[Record::OpLogTruncated { through: 2 }]);
        let restored = restore(&store);
        assert_eq!(
            restored
                .oplog
                .iter()
                .map(|logged| logged.sequence)
                .collect::<Vec<_>>(),
            vec![3, 4],
            "truncation took the wrong entries"
        );
        assert_eq!(
            restored.oplog_collected_through, 2,
            "the watermark is what a resuming log reads, and it did not move"
        );

        // Past the end empties it and still spends nothing, and twice is
        // a no-op rather than a refusal.
        apply(&store, &[Record::OpLogTruncated { through: 99 }]);
        apply(&store, &[Record::OpLogTruncated { through: 99 }]);
        let restored = restore(&store);
        assert!(restored.oplog.is_empty(), "the log was not emptied");
        assert_eq!(
            restored.oplog_collected_through, 99,
            "an empty log forgot the sequences it had spent, so this node would reuse one"
        );
        let outcome = store.apply(&[Record::OpLogAppended {
            entry: LoggedRecord {
                sequence: 4,
                entry: register(
                    MetadataKeyRecord::Retained {
                        topic: "again".to_owned(),
                    },
                    9,
                    None,
                ),
            },
        }]);
        assert!(
            matches!(
                refusal(outcome),
                Rejection::OpLogSequenceReused { sequence: 4, .. }
            ),
            "a sequence spent before the truncation was accepted again"
        );
    }

    /// A register is removed only by a collection, and only that one.
    ///
    /// Section 8.2: a deletion is a register carrying no value and is
    /// *stored*, because a node that dropped the row would be handed its
    /// own older copy by the next peer it spoke to. This record is what
    /// happens afterwards, once every node has seen the tombstone, and it
    /// must take exactly the key it names.
    pub fn a_collected_register_is_the_only_thing_that_removes_one<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        let gone = MetadataKeyRecord::Retained {
            topic: "a".to_owned(),
        };
        let kept = MetadataKeyRecord::Retained {
            topic: "ab".to_owned(),
        };
        apply(
            &store,
            &[
                Record::MetadataPut {
                    entry: register(gone.clone(), 1, None),
                },
                Record::MetadataPut {
                    entry: register(kept.clone(), 2, None),
                },
            ],
        );
        apply(&store, &[Record::MetadataRemoved { key: gone.clone() }]);
        let held = restore(&store).metadata;
        assert_eq!(
            held.iter().map(|entry| &entry.key).collect::<Vec<_>>(),
            vec![&kept],
            "a collection took the wrong register, or more than one"
        );

        // Twice, and for a key nothing holds, are both no-ops: a sweep
        // that ran again after a restart is not a caller's mistake.
        apply(&store, &[Record::MetadataRemoved { key: gone }]);
        apply(
            &store,
            &[Record::MetadataRemoved {
                key: MetadataKeyRecord::Rule {
                    id: "never-held".to_owned(),
                },
            }],
        );
        assert_eq!(restore(&store).metadata.len(), 1);
    }

    /// What this node has read of each other node's log is held, replaced
    /// and restored in node order.
    ///
    /// Durable because a node that came back claiming to have read
    /// nothing would be served logs that have been truncated, and would
    /// hold every peer's watermark down until it caught up. Replaced
    /// whole and not compared: whether a watermark moved forwards is the
    /// caller's question, as it is for the clock mark beside it.
    pub fn what_has_been_read_of_each_peers_log_is_held_in_node_order<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        assert!(
            restore(&store).seen.is_empty(),
            "an empty store has read nobody's log"
        );
        apply(
            &store,
            &[
                Record::MetadataSeenPut {
                    seen: SeenRecord {
                        node: [0x20; 16],
                        through: 7,
                    },
                },
                Record::MetadataSeenPut {
                    seen: SeenRecord {
                        node: [0x10; 16],
                        through: 3,
                    },
                },
            ],
        );
        assert_eq!(
            restore(&store).seen,
            vec![
                SeenRecord {
                    node: [0x10; 16],
                    through: 3
                },
                SeenRecord {
                    node: [0x20; 16],
                    through: 7
                },
            ],
            "the watermarks did not come back in node order"
        );

        // Replaced whole, backwards included: the store holds what it was
        // given.
        apply(
            &store,
            &[Record::MetadataSeenPut {
                seen: SeenRecord {
                    node: [0x10; 16],
                    through: 1,
                },
            }],
        );
        assert_eq!(restore(&store).seen[0].through, 1);
    }

    // ---- rules -----------------------------------------------------------

    /// Rules are held, replaced whole, and removed. Section 9.1.
    pub fn rules_are_held_replaced_and_removed<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        assert!(
            restore(&store).rules.is_empty(),
            "an empty store holds no rules"
        );

        apply(
            &store,
            &[
                Record::RulePut { rule: rule("b") },
                Record::RulePut { rule: rule("a") },
            ],
        );
        assert_eq!(
            restore(&store).rules,
            vec![rule("a"), rule("b")],
            "both rules come back, in identifier order"
        );

        // A put over an identifier already held replaces it whole. The
        // enabled flag above all: turning a connector on and off is the
        // ordinary edit, and a store that kept the first value would
        // leave an operator's switch doing nothing after a restart.
        let mut changed = rule("a");
        changed.enabled = true;
        changed.name = "renamed".to_owned();
        changed.trigger.min_qos = None;
        apply(
            &store,
            &[Record::RulePut {
                rule: changed.clone(),
            }],
        );
        assert_eq!(restore(&store).rules, vec![changed.clone(), rule("b")]);

        apply(&store, &[Record::RuleRemoved { id: "a".to_owned() }]);
        assert_eq!(restore(&store).rules, vec![rule("b")]);

        // Removing one that is not held is a no-op rather than a
        // refusal, like every other removal in this vocabulary: the
        // postcondition it asks for already holds.
        apply(
            &store,
            &[Record::RuleRemoved {
                id: "never".to_owned(),
            }],
        );
        assert_eq!(restore(&store).rules, vec![rule("b")]);
    }

    /// A rule belongs to the node, not to a session.
    ///
    /// The mistake this guards is filing rules under whatever structure
    /// was nearest: a session removed takes its queue, its subscriptions
    /// and its inflight with it, and must take nothing of a rule's.
    pub fn a_rule_survives_the_session_that_was_there_when_it_was_made<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = with_session(new, "sensor");
        apply(&store, &[Record::RulePut { rule: rule("a") }]);
        apply(
            &store,
            &[Record::SessionRemoved {
                session: key("sensor"),
            }],
        );

        let restored = restore(&store);
        assert!(restored.sessions.is_empty(), "the session went");
        assert_eq!(restored.rules, vec![rule("a")], "the rule did not");
    }

    // ---- outboxes ---------------------------------------------------------

    /// An envelope is held with every field it was given, and the
    /// counters move with it. Section 9.2.
    pub fn an_outbox_holds_an_envelope_with_every_field_and_counts_it<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = with_rule(new, "a");
        assert_eq!(store.outbox_depth("a").expect("depth"), 0);
        assert_eq!(store.outbox_highest_sequence("a").expect("mark"), 0);
        assert_eq!(store.outbox_head("a", 0).expect("head"), None);

        apply(
            &store,
            &[
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::OutboxAppended {
                    rule: "a".to_owned(),
                    entry: envelope(1, body_id(1)),
                },
            ],
        );
        assert_eq!(
            store.outbox_head("a", 0).expect("head"),
            Some(envelope(1, body_id(1))),
            "the envelope came back changed"
        );
        assert_eq!(store.outbox_depth("a").expect("depth"), 1);
        assert_eq!(store.outbox_highest_sequence("a").expect("mark"), 1);
    }

    /// The attempt count and the next attempt's time are stored, and an
    /// attempt replaces them.
    ///
    /// This is the record's whole reason for existing: a count kept
    /// anywhere else resets when the process does, and a node coming
    /// back after an outage throws its entire backlog at a still-dead
    /// endpoint at once.
    pub fn an_attempt_moves_the_stored_count_and_the_next_time<S: Storage>(new: impl Fn() -> S) {
        let store = with_rule(new, "a");
        let mut fresh = envelope(1, body_id(1));
        fresh.attempts = 0;
        fresh.next_attempt_at = 0;
        apply(
            &store,
            &[
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::OutboxAppended {
                    rule: "a".to_owned(),
                    entry: fresh.clone(),
                },
            ],
        );
        assert_eq!(store.outbox_head("a", 0).expect("head"), Some(fresh));

        apply(
            &store,
            &[Record::OutboxAttempted {
                rule: "a".to_owned(),
                sequence: 1,
                attempts: 3,
                next_attempt_at: 1_700_000_031,
            }],
        );
        let held = store
            .outbox_head("a", 0)
            .expect("head")
            .expect("the envelope is still waiting");
        assert_eq!(held.attempts, 3, "the attempt count was not stored");
        assert_eq!(
            held.next_attempt_at, 1_700_000_031,
            "the next attempt's time was not stored"
        );
        // Nothing else moved: an attempt is not a rewrite of the
        // envelope, and a backend that rebuilt the record would lose
        // whichever field it forgot.
        assert_eq!(held.id, envelope(1, body_id(1)).id);
        assert_eq!(held.body, body_id(1));
        assert_eq!(held.client_id, "sensor-1");
        assert_eq!(held.enqueued_at, 1_700_000_000);

        // An attempt on an envelope that has left is a no-op rather than
        // a refusal or a resurrection.
        apply(
            &store,
            &[Record::OutboxRemoved {
                rule: "a".to_owned(),
                sequence: 1,
            }],
        );
        apply(
            &store,
            &[Record::OutboxAttempted {
                rule: "a".to_owned(),
                sequence: 1,
                attempts: 9,
                next_attempt_at: 1_700_000_099,
            }],
        );
        assert_eq!(
            store.outbox_head("a", 0).expect("head"),
            None,
            "an attempt on a delivered envelope put it back"
        );
        assert_eq!(store.outbox_depth("a").expect("depth"), 0);
    }

    /// Two rules' outboxes do not see each other.
    ///
    /// The identifiers are `"a"` and `"ab"`, which is the pair a backend
    /// gluing the identifier to the sequence number would confuse: one
    /// rule would post another rule's messages.
    pub fn two_rules_outboxes_do_not_see_each_other<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        apply(
            &store,
            &[
                Record::RulePut { rule: rule("a") },
                Record::RulePut { rule: rule("ab") },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::BodyPut {
                    id: body_id(2),
                    body: body(2),
                },
                Record::OutboxAppended {
                    rule: "a".to_owned(),
                    entry: envelope(1, body_id(1)),
                },
                Record::OutboxAppended {
                    rule: "ab".to_owned(),
                    entry: envelope(1, body_id(2)),
                },
            ],
        );

        assert_eq!(
            store.outbox_head("a", 0).expect("head").map(|e| e.body),
            Some(body_id(1)),
            "rule a read somebody else's head"
        );
        assert_eq!(
            store.outbox_head("ab", 0).expect("head").map(|e| e.body),
            Some(body_id(2)),
            "rule ab read somebody else's head"
        );
        assert_eq!(store.outbox_depth("a").expect("depth"), 1);
        assert_eq!(store.outbox_depth("ab").expect("depth"), 1);

        // Draining one does not drain the other, and the counters stay
        // apart.
        apply(
            &store,
            &[Record::OutboxRemoved {
                rule: "a".to_owned(),
                sequence: 1,
            }],
        );
        assert_eq!(store.outbox_depth("a").expect("depth"), 0);
        assert_eq!(store.outbox_depth("ab").expect("depth"), 1);
        assert_eq!(
            store.outbox_head("ab", 0).expect("head").map(|e| e.body),
            Some(body_id(2))
        );
        // **And the drained outbox reads as empty.** This is the
        // assertion the prefix-free key encoding is actually for, and
        // the one an earlier version of this test was missing: while
        // rule `a` still held something of its own, a backend keying by
        // the identifier glued to the sequence answered correctly by
        // luck, because `a`'s own rows happened to sort first. Ask it
        // once `a` is empty and it hands over rule `ab`'s envelope,
        // which is one rule posting another rule's messages to its own
        // endpoint.
        assert_eq!(
            store.outbox_head("a", 0).expect("head"),
            None,
            "a drained outbox handed over another rule's envelope"
        );

        // Deleting one rule takes its own outbox and nobody else's.
        apply(&store, &[Record::RuleRemoved { id: "a".to_owned() }]);
        assert_eq!(
            store.outbox_head("ab", 0).expect("head").map(|e| e.body),
            Some(body_id(2)),
            "deleting one rule emptied another rule's outbox"
        );
        assert_eq!(store.outbox_depth("ab").expect("depth"), 1);

        // A rule nothing ever wrote to reads as empty rather than as an
        // error, which is what lets a pump ask about every rule it holds.
        assert_eq!(store.outbox_depth("never").expect("depth"), 0);
        assert_eq!(store.outbox_head("never", 0).expect("head"), None);
        assert_eq!(store.outbox_highest_sequence("never").expect("mark"), 0);
    }

    /// The head read at a cursor skips what has been delivered, and the
    /// numbers it skipped are never handed out again.
    pub fn the_outbox_head_at_a_cursor_skips_what_was_delivered<S: Storage>(new: impl Fn() -> S) {
        let store = with_rule(new, "a");
        let mut records = vec![Record::BodyPut {
            id: body_id(1),
            body: body(1),
        }];
        // Sequence numbers either side of a byte boundary, because a
        // little-endian key sorts 256 before 2 and nothing else here
        // would see it.
        for sequence in [1_u64, 2, 300, 301] {
            records.push(Record::BodyPut {
                id: body_id(1),
                body: body(1),
            });
            records.push(Record::OutboxAppended {
                rule: "a".to_owned(),
                entry: envelope(sequence, body_id(1)),
            });
        }
        apply(&store, &records);
        assert_eq!(store.outbox_depth("a").expect("depth"), 4);
        assert_eq!(store.outbox_highest_sequence("a").expect("mark"), 301);

        apply(
            &store,
            &[
                Record::OutboxRemoved {
                    rule: "a".to_owned(),
                    sequence: 1,
                },
                Record::OutboxRemoved {
                    rule: "a".to_owned(),
                    sequence: 2,
                },
            ],
        );
        assert_eq!(
            store
                .outbox_head("a", 0)
                .expect("head")
                .map(|entry| entry.sequence),
            Some(300),
            "the head read did not skip what had been delivered"
        );
        assert_eq!(
            store
                .outbox_head("a", 301)
                .expect("head")
                .map(|entry| entry.sequence),
            Some(301),
            "a cursor past a hole must land on what follows it"
        );
        assert_eq!(store.outbox_head("a", 302).expect("head"), None);

        // The mark outlives the envelopes, so a restarted pump cannot
        // reuse a number: appending at one already spent is refused.
        apply(
            &store,
            &[
                Record::OutboxRemoved {
                    rule: "a".to_owned(),
                    sequence: 300,
                },
                Record::OutboxRemoved {
                    rule: "a".to_owned(),
                    sequence: 301,
                },
            ],
        );
        assert_eq!(store.outbox_depth("a").expect("depth"), 0);
        assert_eq!(store.outbox_highest_sequence("a").expect("mark"), 301);
        assert_eq!(
            refusal(store.apply(&[Record::OutboxAppended {
                rule: "a".to_owned(),
                entry: envelope(301, body_id(1)),
            }])),
            Rejection::OutboxSequenceReused {
                id: "a".to_owned(),
                sequence: 301,
                highest: 301,
            }
        );
    }

    /// An envelope for a rule the store does not hold is refused.
    ///
    /// The same argument as a record for an unknown session: an outbox
    /// is read through the rule that owns it, so one filed under a rule
    /// that is not there would be neither stored usefully nor reported
    /// missing.
    pub fn an_envelope_for_an_unknown_rule_is_refused<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        assert_eq!(
            refusal(store.apply(&[Record::OutboxAppended {
                rule: "ghost".to_owned(),
                entry: envelope(1, body_id(1)),
            }])),
            Rejection::NoSuchRule {
                id: "ghost".to_owned()
            }
        );
        assert_eq!(
            refusal(store.apply(&[Record::OutboxAttempted {
                rule: "ghost".to_owned(),
                sequence: 1,
                attempts: 1,
                next_attempt_at: 5,
            }])),
            Rejection::NoSuchRule {
                id: "ghost".to_owned()
            }
        );
        assert_eq!(
            refusal(store.apply(&[Record::OutboxRemoved {
                rule: "ghost".to_owned(),
                sequence: 1,
            }])),
            Rejection::NoSuchRule {
                id: "ghost".to_owned()
            }
        );
    }

    /// An outbox survives its rule being edited, and goes with its rule
    /// being deleted.
    ///
    /// Both halves matter and they pull in opposite directions. A rule
    /// is replaced whole by a put — turning a connector off and on again
    /// is the ordinary edit — and a put that emptied the outbox would
    /// silently destroy every message waiting to go. A delete is the
    /// operator saying the rule is finished, and an outbox left behind
    /// would sit in the file with nothing able to reach it.
    pub fn an_outbox_survives_an_edit_and_goes_with_a_delete<S: Storage>(new: impl Fn() -> S) {
        let store = with_rule(new, "a");
        apply(
            &store,
            &[
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::OutboxAppended {
                    rule: "a".to_owned(),
                    entry: envelope(1, body_id(1)),
                },
            ],
        );

        let mut changed = rule("a");
        changed.enabled = true;
        changed.name = "renamed".to_owned();
        apply(&store, &[Record::RulePut { rule: changed }]);
        assert_eq!(
            store.outbox_head("a", 0).expect("head"),
            Some(envelope(1, body_id(1))),
            "editing the rule threw away what was waiting to be delivered"
        );
        assert_eq!(store.outbox_depth("a").expect("depth"), 1);

        apply(&store, &[Record::RuleRemoved { id: "a".to_owned() }]);
        assert_eq!(store.outbox_head("a", 0).expect("head"), None);
        assert_eq!(store.outbox_depth("a").expect("depth"), 0);
        assert_eq!(
            store.outbox_highest_sequence("a").expect("mark"),
            0,
            "the counters went with the rule"
        );
        // The body is still held: the reference belongs to whoever took
        // it, and the store cannot tell a caller that releases its own
        // from one that does not.
        assert_eq!(restore(&store).bodies.get(&body_id(1)), Some(&body(1)));
    }

    /// Removing an envelope does not release the body it named.
    ///
    /// The same rule as a queue pop, and the reason this suite states it
    /// twice: the caller owes the release, and a store that made it for
    /// them would delete a payload out from under a rule that fired on
    /// the same message.
    pub fn removing_an_envelope_does_not_release_the_body_it_named<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        apply(
            &store,
            &[
                Record::RulePut { rule: rule("a") },
                Record::RulePut { rule: rule("ab") },
                // One message, two rules, two references.
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::OutboxAppended {
                    rule: "a".to_owned(),
                    entry: envelope(1, body_id(1)),
                },
                Record::OutboxAppended {
                    rule: "ab".to_owned(),
                    entry: envelope(1, body_id(1)),
                },
            ],
        );

        // The first rule delivers and releases its own reference. The
        // body must still be there for the second.
        apply(
            &store,
            &[
                Record::OutboxRemoved {
                    rule: "a".to_owned(),
                    sequence: 1,
                },
                Record::BodyReleased { id: body_id(1) },
            ],
        );
        assert_eq!(
            restore(&store).bodies.get(&body_id(1)),
            Some(&body(1)),
            "one rule's delivery deleted the payload another rule still had to send"
        );

        // And when the second releases, it goes: a store that released
        // on removal would have been asked to release twice, which is
        // the refusal that would have hidden this.
        apply(
            &store,
            &[
                Record::OutboxRemoved {
                    rule: "ab".to_owned(),
                    sequence: 1,
                },
                Record::BodyReleased { id: body_id(1) },
            ],
        );
        assert!(
            restore(&store).bodies.is_empty(),
            "the payload was not reclaimed when the last rule had sent it"
        );
    }

    /// A body can be read back one at a time, and reading one the store
    /// does not hold is `None` rather than an error.
    ///
    /// [`Storage::restore`] answers this at startup for everything at
    /// once; an outbox filled while the broker is running needs the
    /// payload of an envelope nobody has read since, and the answer has
    /// to come from the store rather than from a memory of it.
    pub fn a_body_can_be_read_back_on_its_own<S: Storage>(new: impl Fn() -> S) {
        let store = new();
        assert_eq!(
            store.body(&body_id(1)).expect("the read must answer"),
            None,
            "a body the store does not hold must read as absent, not as an error"
        );

        apply(
            &store,
            &[Record::BodyPut {
                id: body_id(1),
                body: body(1),
            }],
        );
        assert_eq!(
            store.body(&body_id(1)).expect("the read must answer"),
            Some(body(1)),
            "the body came back changed"
        );
        // And not somebody else's.
        assert_eq!(store.body(&body_id(2)).expect("the read must answer"), None);

        apply(&store, &[Record::BodyReleased { id: body_id(1) }]);
        assert_eq!(
            store.body(&body_id(1)).expect("the read must answer"),
            None,
            "a released body was still readable"
        );
    }

    // ---- flush ------------------------------------------------------------

    /// The owner's own position in the stream comes back with the
    /// session, and goes when the session goes.
    ///
    /// **Why it is stored at all**: a follower refuses anything at or
    /// below its mark, so an owner that restarted its count from one
    /// would have everything it sent refused as a duplicate —
    /// silently, until the session had produced as many operations as
    /// it had before. A follower that quietly stops following is the
    /// failure replication exists to prevent.
    pub fn the_owners_stream_position_survives_and_goes_with_the_session<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        apply(&store, &[Record::SessionPut(session("mine"))]);
        assert_eq!(
            session_of(&restore(&store), "mine").stream_at,
            0,
            "a session that has never been streamed reads as zero, not as absent"
        );

        apply(
            &store,
            &[Record::StreamAt {
                session: key("mine"),
                at: 4,
            }],
        );
        apply(
            &store,
            &[Record::StreamAt {
                session: key("mine"),
                at: 9,
            }],
        );
        assert_eq!(
            session_of(&restore(&store), "mine").stream_at,
            9,
            "one row, overwritten, not a history"
        );

        // And a client reconnecting under the same identifier inherits
        // nothing, this included: a position left behind would have
        // the next stream start part way through a session that has
        // none of it.
        apply(
            &store,
            &[
                Record::SessionRemoved {
                    session: key("mine"),
                },
                Record::SessionPut(session("mine")),
            ],
        );
        assert_eq!(session_of(&restore(&store), "mine").stream_at, 0);
    }

    /// A session this node merely holds a copy of comes back as a
    /// copy, and **not in the list the broker is handed**.
    ///
    /// Section 8.6. Every record a followed session is made of is the
    /// same record an owned one is made of — the session, its
    /// subscriptions, its queue — so the only thing that can tell them
    /// apart is the replica row. A restore that lost it would hand the
    /// broker another node's session: its filters would enter this
    /// node's matching trie, it would be forwarded publications for
    /// them, and it would deliver them to a client connected
    /// somewhere else.
    pub fn a_followed_session_restores_as_a_copy_and_not_as_a_session<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        apply(
            &store,
            &[
                Record::SessionPut(session("mine")),
                Record::SessionPut(session("theirs")),
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::QueuePushed {
                    session: key("theirs"),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::ReplicaFollows {
                    session: key("theirs"),
                    owner: [0xbb; 16],
                    through: 7,
                },
            ],
        );

        let restored = restore(&store);
        assert_eq!(
            restored
                .sessions
                .iter()
                .map(|held| held.session.key.client_id.clone())
                .collect::<Vec<_>>(),
            vec!["mine".to_owned()],
            "the followed session must not be in the list the broker is handed"
        );
        assert_eq!(restored.replicas.len(), 1);
        let copy = &restored.replicas[0];
        assert_eq!(copy.session.session.key, key("theirs"));
        assert_eq!(copy.owner, [0xbb; 16]);
        assert_eq!(
            copy.through, 7,
            "the mark comes back, or the follower re-applies a stream it already has"
        );
        assert_eq!(
            copy.session.queue.len(),
            1,
            "and the copy is a whole session: the queue came with it"
        );
    }

    /// The mark is overwritten rather than appended to, and dropping
    /// the row keeps the session.
    ///
    /// Both halves matter. A mark that accumulated rows would grow a
    /// row per replicated operation for the life of the session. And
    /// **becoming a session's owner is the removal of one row**: the
    /// queue, the subscriptions and the session itself stay exactly
    /// where they are, which is what makes failover keep what it
    /// pulled.
    pub fn a_replica_mark_is_overwritten_and_dropping_it_keeps_the_session<S: Storage>(
        new: impl Fn() -> S,
    ) {
        let store = new();
        apply(
            &store,
            &[
                Record::SessionPut(session("theirs")),
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::QueuePushed {
                    session: key("theirs"),
                    entry: queue_entry(1, body_id(1)),
                },
                Record::ReplicaFollows {
                    session: key("theirs"),
                    owner: [0xbb; 16],
                    through: 1,
                },
            ],
        );
        apply(
            &store,
            &[Record::ReplicaFollows {
                session: key("theirs"),
                owner: [0xbb; 16],
                through: 2,
            }],
        );

        let restored = restore(&store);
        assert_eq!(restored.replicas.len(), 1, "one row, not two");
        assert_eq!(restored.replicas[0].through, 2);

        // And now this node becomes the owner.
        apply(
            &store,
            &[Record::ReplicaDropped {
                session: key("theirs"),
            }],
        );
        let restored = restore(&store);
        assert!(restored.replicas.is_empty());
        let now_ours = session_of(&restored, "theirs");
        assert_eq!(
            now_ours.queue.len(),
            1,
            "the queue stayed: becoming the owner keeps everything and changes whose it is"
        );
    }

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
            a_body_put_again_keeps_the_body_it_already_holds,
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
            rules_are_held_replaced_and_removed,
            a_rule_survives_the_session_that_was_there_when_it_was_made,
            an_outbox_holds_an_envelope_with_every_field_and_counts_it,
            an_attempt_moves_the_stored_count_and_the_next_time,
            two_rules_outboxes_do_not_see_each_other,
            the_outbox_head_at_a_cursor_skips_what_was_delivered,
            an_envelope_for_an_unknown_rule_is_refused,
            an_outbox_survives_an_edit_and_goes_with_a_delete,
            removing_an_envelope_does_not_release_the_body_it_named,
            a_body_can_be_read_back_on_its_own,
            the_node_identity_is_held_and_renamed_without_re_founding,
            the_clock_mark_is_held_with_both_of_its_halves,
            metadata_registers_are_held_replaced_and_restored_in_key_order,
            two_nodes_subscriptions_to_one_filter_are_two_registers,
            a_register_survives_the_session_and_the_rule_its_key_names,
            the_operation_log_is_appended_in_order_and_refuses_a_reused_sequence,
            a_truncated_operation_log_keeps_the_sequence_it_has_spent,
            a_collected_register_is_the_only_thing_that_removes_one,
            what_has_been_read_of_each_peers_log_is_held_in_node_order,
            a_flush_keeps_everything_and_leaves_the_store_usable,
            a_followed_session_restores_as_a_copy_and_not_as_a_session,
            a_replica_mark_is_overwritten_and_dropping_it_keeps_the_session,
            the_owners_stream_position_survives_and_goes_with_the_session,
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
every_contract_test!(run_against, redb, crate::on_disk::fresh);

/// A redb store on a directory that goes when the store does.
///
/// The contract's constructor hands back a store and nothing else, so the
/// directory has to be owned by the store; a `TempDir` dropped at the end
/// of the constructor would delete the file out from under it. Everything
/// here is delegation: the backend under test is `storage::RedbStorage`,
/// unchanged.
mod on_disk {
    use storage::{
        BodyRecord, MessageId, OutboxRecord, QueueEntry, Record, RedbStorage, Restored, SessionKey,
        Storage, StorageError,
    };

    /// A store, and the directory it lives in.
    pub struct OnDisk {
        store: RedbStorage,
        /// Dropped after the store, deleting the file it used.
        _directory: tempfile::TempDir,
    }

    /// An empty store on a directory of its own.
    pub fn fresh() -> OnDisk {
        let directory = tempfile::tempdir().expect("a temporary directory must be available");
        let store = RedbStorage::open(directory.path().join("store.redb"))
            .expect("a fresh store must open");
        OnDisk {
            store,
            _directory: directory,
        }
    }

    impl Storage for OnDisk {
        fn apply(&self, records: &[Record]) -> Result<(), StorageError> {
            self.store.apply(records)
        }

        fn flush(&self) -> Result<(), StorageError> {
            self.store.flush()
        }

        fn restore(&self) -> Result<Restored, StorageError> {
            self.store.restore()
        }

        fn queue_head(
            &self,
            session: &SessionKey,
            cursor: u64,
        ) -> Result<Option<QueueEntry>, StorageError> {
            self.store.queue_head(session, cursor)
        }

        fn queue_depth(&self, session: &SessionKey) -> Result<u64, StorageError> {
            self.store.queue_depth(session)
        }

        fn highest_sequence(&self, session: &SessionKey) -> Result<u64, StorageError> {
            self.store.highest_sequence(session)
        }

        fn outbox_head(
            &self,
            rule: &str,
            cursor: u64,
        ) -> Result<Option<OutboxRecord>, StorageError> {
            self.store.outbox_head(rule, cursor)
        }

        fn outbox_depth(&self, rule: &str) -> Result<u64, StorageError> {
            self.store.outbox_depth(rule)
        }

        fn outbox_highest_sequence(&self, rule: &str) -> Result<u64, StorageError> {
            self.store.outbox_highest_sequence(rule)
        }

        fn body(&self, id: &MessageId) -> Result<Option<BodyRecord>, StorageError> {
            self.store.body(id)
        }
    }
}
