//! What only the redb backend can fail.
//!
//! The contract suite in `backends.rs` runs against both backends and says
//! what a store must do. Everything here needs a store that can be closed
//! and opened again, which is to say a store on a disk, so none of it
//! could live there.
//!
//! # The trap this file exists for
//!
//! A restart test that closes the store cleanly proves less than it looks
//! like it does. Measured against redb 4.3: a clean close writes out
//! everything, flushed or not, so a test that applies a batch, drops the
//! store and reopens it passes whether or not `flush` was ever called, and
//! whether or not `flush` does anything. That is convention 5's failure
//! mode exactly — a test that cannot fail — and
//! [`a_clean_close_persists_even_what_was_never_flushed`] pins the
//! behaviour so the next reader does not write one.
//!
//! What distinguishes a flushed store from an unflushed one is an
//! **unclean** exit, so the durability tests here run a child process that
//! calls [`std::process::exit`] and therefore runs no destructor. Anything
//! only the destructor would have written is gone, which is the case the
//! whole milestone is about.

use std::path::{Path, PathBuf};
use std::process::Command;

use storage::{
    ActionRecord, BodyRecord, DeliveryRecord, FormatMarker, HttpOutRecord, MessageId, OutboxRecord,
    ProtocolVersion, Qos, QueueEntry, Record, RedbStorage, RuleRecord, SessionKey, SessionRecord,
    Storage, StorageError, SyncMode, TriggerRecord, FORMAT_MAGIC, FORMAT_VERSION,
};

// ---- building blocks ----------------------------------------------------

fn key(client_id: &str) -> SessionKey {
    SessionKey::new("", client_id)
}

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

fn body_id(n: u8) -> MessageId {
    let mut bytes = [0u8; 16];
    bytes[0] = n;
    MessageId(bytes)
}

fn body(n: u8) -> BodyRecord {
    BodyRecord {
        topic: format!("sensor/{n}"),
        payload: format!("body-{n}").into_bytes().into(),
        qos: Qos::AtLeastOnce,
        retain: false,
        expires_at: None,
        properties: bytes::Bytes::new(),
    }
}

fn queue_entry(sequence: u64, body: MessageId) -> QueueEntry {
    QueueEntry {
        sequence,
        packet_id: None,
        delivery: DeliveryRecord {
            body,
            qos: Qos::AtLeastOnce,
            retain: false,
            subscription_ids: Vec::new(),
            dup: false,
        },
    }
}

/// A directory that outlives the stores opened in it.
fn workspace() -> tempfile::TempDir {
    tempfile::tempdir().expect("a temporary directory must be available")
}

fn store_path(workspace: &tempfile::TempDir) -> PathBuf {
    workspace.path().join("store.redb")
}

#[track_caller]
fn open(path: &Path) -> RedbStorage {
    RedbStorage::open(path).unwrap_or_else(|error| panic!("the store must open: {error}"))
}

#[track_caller]
fn apply(store: &RedbStorage, records: &[Record]) {
    store
        .apply(records)
        .unwrap_or_else(|error| panic!("the store refused a batch it must accept: {error}"));
}

// ---- the format version, at the store rather than the marker ------------

/// The marker a build one version ahead of this one would write.
fn marker_from_a_later_build() -> Vec<u8> {
    FormatMarker {
        magic: FORMAT_MAGIC,
        version: FORMAT_VERSION + 1,
    }
    .encode()
}

/// Overwrite the format marker in a closed store, as a later build would
/// have written it.
///
/// Reaches into the file with redb directly rather than through any
/// door this crate offers, because the case being simulated is a file this
/// build did not write. The table and key names are repeated here on
/// purpose: if the store moves its marker somewhere else, the insert below
/// stops replacing anything, and the assertion catches it rather than the
/// test passing against a store it never tampered with.
fn write_a_later_format_marker(path: &Path) {
    const META: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("meta");
    let database = redb::Database::create(path).expect("the store must open as a redb database");
    let transaction = database.begin_write().expect("a write must begin");
    {
        let mut meta = transaction.open_table(META).expect("meta must exist");
        let previous = meta
            .insert("format", marker_from_a_later_build().as_slice())
            .expect("the marker must be writable");
        assert!(
            previous.is_some(),
            "no marker was there to replace: the store does not keep its format version at \
             meta/format any more, so this test is tampering with nothing"
        );
    }
    transaction.commit().expect("the tampering must commit");
}

/// A store written by a later build refuses to open, and says so.
///
/// This is the one failure that is otherwise silent. Every other way a
/// store can be wrong announces itself; a layout this build does not know
/// restores nonsense from a file that is not damaged at all, and the
/// broker comes up looking healthy with every session misread.
#[test]
fn a_store_written_by_a_later_format_refuses_to_open() {
    let workspace = workspace();
    let path = store_path(&workspace);
    drop(open(&path));
    write_a_later_format_marker(&path);

    let error = RedbStorage::open(&path).expect_err("the store must refuse");
    match error {
        StorageError::UnsupportedVersion { found, expected } => {
            assert_eq!(found, FORMAT_VERSION + 1);
            assert_eq!(expected, FORMAT_VERSION);
        }
        other => panic!("a later format was reported as {other:?}"),
    }
}

/// And it refuses before it reads anything, rather than after.
///
/// A store that opened, restored what it could and failed later would have
/// already handed the broker misread sessions.
#[test]
fn a_later_format_is_refused_rather_than_read() {
    let workspace = workspace();
    let path = store_path(&workspace);
    {
        let store = open(&path);
        apply(&store, &[Record::SessionPut(session("sensor"))]);
        store.flush().expect("a flush must succeed");
    }
    write_a_later_format_marker(&path);

    assert!(
        matches!(
            RedbStorage::open(&path),
            Err(StorageError::UnsupportedVersion { .. })
        ),
        "a store with readable-looking content was opened despite its version"
    );
}

/// A store this build wrote opens again, which is what makes the refusal
/// above mean something.
#[test]
fn a_store_this_build_wrote_opens_again() {
    let workspace = workspace();
    let path = store_path(&workspace);
    drop(open(&path));
    drop(open(&path));
}

/// Two stores on one path is an operator mistake with its own name.
#[test]
fn a_second_store_on_one_path_is_refused_as_a_lock() {
    let workspace = workspace();
    let path = store_path(&workspace);
    let held = open(&path);
    match RedbStorage::open(&path) {
        Err(StorageError::Locked { path: reported }) => assert_eq!(reported, path),
        Err(other) => panic!("a held store was reported as {other:?}"),
        Ok(_) => panic!("two stores opened the same path"),
    }
    drop(held);
    // And the path is usable again once the first store has gone.
    drop(open(&path));
}

// ---- the queue key is big-endian ----------------------------------------

/// The queue comes back in sequence order after a restart.
///
/// redb orders by bytes, so a sequence number written in the machine's own
/// byte order sorts 256 before 2. The contract suite asserts the order
/// within one process, where a backend could get it right by accident of
/// how it iterates; this asks the file, which is the only place the key
/// encoding is the answer.
#[test]
fn the_queue_comes_back_in_sequence_order_after_a_restart() {
    let workspace = workspace();
    let path = store_path(&workspace);
    let pushed = [1u64, 2, 255, 256, 257, 511, 65_535, 65_536, 16_777_216];
    {
        let store = open(&path);
        apply(&store, &[Record::SessionPut(session("sensor"))]);
        for sequence in pushed {
            apply(
                &store,
                &[Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(sequence, body_id(1)),
                }],
            );
        }
        store.flush().expect("a flush must succeed");
    }

    let store = open(&path);
    let restored = store.restore().expect("restoring must succeed");
    let sequences: Vec<u64> = restored.sessions[0]
        .queue
        .iter()
        .map(|entry| entry.sequence)
        .collect();
    assert_eq!(sequences, pushed, "the queue came back out of order");

    // The head read walks the same key order, so it is asked too.
    assert_eq!(
        store
            .queue_head(&key("sensor"), 3)
            .expect("the head read must answer")
            .map(|entry| entry.sequence),
        Some(255)
    );
    assert_eq!(
        store
            .queue_head(&key("sensor"), 258)
            .expect("the head read must answer")
            .map(|entry| entry.sequence),
        Some(511)
    );
}

/// Two sessions whose keys are prefixes of each other keep their own
/// queues.
///
/// A key built by gluing the session's bytes to the sequence lets client
/// `a`'s entry at some sequence collide with client `ab`'s, because the
/// bytes of the sequence can spell the rest of the longer name. The result
/// is one client reading another's messages, which is the worst outcome in
/// this crate.
#[test]
fn sessions_whose_keys_are_prefixes_of_each_other_do_not_share_a_queue() {
    let workspace = workspace();
    let path = store_path(&workspace);
    {
        let store = open(&path);
        apply(
            &store,
            &[
                Record::SessionPut(session("a")),
                Record::SessionPut(session("ab")),
                Record::SessionPut(SessionRecord {
                    key: SessionKey::new("a", "b"),
                    ..session("ignored")
                }),
            ],
        );
        for sequence in 1..=4u64 {
            apply(
                &store,
                &[Record::QueuePushed {
                    session: key("ab"),
                    entry: queue_entry(sequence, body_id(1)),
                }],
            );
        }
        apply(
            &store,
            &[Record::QueuePushed {
                session: SessionKey::new("a", "b"),
                entry: queue_entry(9, body_id(1)),
            }],
        );
        store.flush().expect("a flush must succeed");
    }

    let store = open(&path);
    assert_eq!(
        store.queue_depth(&key("a")).expect("depth"),
        0,
        "client a is holding client ab's queue"
    );
    assert_eq!(store.queue_depth(&key("ab")).expect("depth"), 4);
    assert_eq!(
        store
            .queue_depth(&SessionKey::new("a", "b"))
            .expect("depth"),
        1,
        "the mountpoint and the client identifier ran together"
    );
    assert_eq!(
        store.queue_head(&key("a"), 0).expect("the head read"),
        None,
        "client a read an entry belonging to client ab"
    );
}

// ---- the outbox across a restart ----------------------------------------

/// A rule to hang an outbox from.
fn rule(id: &str) -> RuleRecord {
    RuleRecord {
        id: id.to_owned(),
        name: format!("rule {id}"),
        enabled: true,
        trigger: TriggerRecord {
            filter: "sensor/+/temp".to_owned(),
            min_qos: None,
            client_id: None,
        },
        action: ActionRecord::HttpOut(HttpOutRecord {
            url: "https://example.invalid/hook".to_owned(),
            method: "POST".to_owned(),
            headers: Vec::new(),
            timeout_ms: 1_000,
            max_concurrency: 1,
            retries: 3,
            backoff_ms: 500,
        }),
    }
}

/// An envelope comes back from the file with the attempts it had already
/// cost, and the outbox drains in the order the rule fired.
///
/// **This is the reason the count is in the record.** Keeping it beside
/// the task that posts is cheaper and works perfectly until the process
/// restarts, at which point every envelope is back at zero attempts and
/// a node returning from an outage throws its whole backlog at a
/// still-dead endpoint as though none of it had ever been tried. Nothing
/// inside one process can fail that; this reopens the file.
///
/// The sequence numbers cross byte boundaries for the reason the queue
/// test above gives: redb orders by bytes, and a little-endian key sorts
/// 256 before 2.
#[test]
fn an_envelope_comes_back_with_the_attempts_it_already_cost() {
    let workspace = workspace();
    let path = store_path(&workspace);
    let appended = [1u64, 2, 255, 256, 257, 65_536];
    {
        let store = open(&path);
        apply(&store, &[Record::RulePut { rule: rule("a") }]);
        for sequence in appended {
            apply(
                &store,
                &[
                    Record::BodyPut {
                        id: body_id(1),
                        body: body(1),
                    },
                    Record::OutboxAppended {
                        rule: "a".to_owned(),
                        entry: OutboxRecord {
                            sequence,
                            id: body_id(9),
                            body: body_id(1),
                            client_id: "sensor".to_owned(),
                            enqueued_at: 1_700_000_000,
                            attempts: 0,
                            next_attempt_at: 0,
                        },
                    },
                ],
            );
        }
        // The head has been tried twice and is not due again until a
        // known second. Both numbers have to survive: a restart that
        // kept the count and lost the deadline would still post
        // immediately, which is the behaviour the count exists to stop.
        apply(
            &store,
            &[Record::OutboxAttempted {
                rule: "a".to_owned(),
                sequence: 1,
                attempts: 2,
                next_attempt_at: 1_700_000_061,
            }],
        );
        store.flush().expect("a flush must succeed");
    }

    let store = open(&path);
    let head = store
        .outbox_head("a", 0)
        .expect("the head read must answer")
        .expect("the envelope is still waiting");
    assert_eq!(head.sequence, 1);
    assert_eq!(
        head.attempts, 2,
        "the attempt count was reset by the restart"
    );
    assert_eq!(
        head.next_attempt_at, 1_700_000_061,
        "the next attempt's deadline was reset by the restart"
    );
    assert_eq!(head.id, body_id(9), "the envelope lost its identifier");
    assert_eq!(store.outbox_depth("a").expect("depth"), 6);
    assert_eq!(store.outbox_highest_sequence("a").expect("mark"), 65_536);

    // And the rest of the outbox is in the order the rule fired, which
    // is the key encoding's answer and nothing else's.
    let mut cursor = 0;
    let mut seen = Vec::new();
    while let Some(entry) = store.outbox_head("a", cursor).expect("the head read") {
        seen.push(entry.sequence);
        cursor = entry.sequence + 1;
    }
    assert_eq!(seen, appended, "the outbox came back out of order");
}

/// Two rules whose identifiers are prefixes of each other keep their own
/// outboxes across a restart.
///
/// The same hazard as two sessions whose keys are prefixes of each other,
/// and the worse outcome here: a rule posting another rule's messages
/// sends somebody's data to the wrong endpoint.
#[test]
fn rules_whose_identifiers_are_prefixes_of_each_other_do_not_share_an_outbox() {
    let workspace = workspace();
    let path = store_path(&workspace);
    {
        let store = open(&path);
        apply(
            &store,
            &[
                Record::RulePut { rule: rule("a") },
                Record::RulePut { rule: rule("ab") },
            ],
        );
        for (id, sequence, body) in [("a", 25_185_u64, 1_u8), ("ab", 1, 2)] {
            apply(
                &store,
                &[
                    Record::BodyPut {
                        id: body_id(body),
                        body: crate::body(body),
                    },
                    Record::OutboxAppended {
                        rule: id.to_owned(),
                        entry: OutboxRecord {
                            sequence,
                            id: body_id(body),
                            body: body_id(body),
                            client_id: id.to_owned(),
                            enqueued_at: 1_700_000_000,
                            attempts: 0,
                            next_attempt_at: 0,
                        },
                    },
                ],
            );
        }
        store.flush().expect("a flush must succeed");
    }

    let store = open(&path);
    assert_eq!(
        store
            .outbox_head("a", 0)
            .expect("head")
            .map(|entry| (entry.sequence, entry.body)),
        Some((25_185, body_id(1))),
        "rule a read the wrong outbox"
    );
    assert_eq!(
        store
            .outbox_head("ab", 0)
            .expect("head")
            .map(|entry| (entry.sequence, entry.body)),
        Some((1, body_id(2))),
        "rule ab read the wrong outbox"
    );
    assert_eq!(store.outbox_depth("a").expect("depth"), 1);
    assert_eq!(store.outbox_depth("ab").expect("depth"), 1);

    // The sharper half, across the file: once one rule's outbox is
    // empty, it must read as empty rather than falling through into the
    // rows of the rule whose identifier begins with the same bytes.
    apply(
        &store,
        &[Record::OutboxRemoved {
            rule: "a".to_owned(),
            sequence: 25_185,
        }],
    );
    assert_eq!(
        store.outbox_head("a", 0).expect("head"),
        None,
        "a drained outbox handed over another rule's envelope"
    );
    assert_eq!(
        store
            .outbox_head("ab", 0)
            .expect("head")
            .map(|entry| entry.sequence),
        Some(1)
    );
}

// ---- what a flush is for ------------------------------------------------

/// The store a crashing child process leaves behind.
///
/// The child writes the same batch every time and differs only in how it
/// was told to flush. It exits with [`std::process::exit`], which runs no
/// destructor, so nothing reaches the file that the flush did not put
/// there.
fn crash_a_child_writing_to(path: &Path, sync: &str, flush: bool) {
    let binary = std::env::current_exe().expect("the test binary must know its own path");
    let output = Command::new(binary)
        .args(["--exact", "--ignored", "child_writes_then_exits_abruptly"])
        .env("SD_MQTT_TEST_STORE", path)
        .env("SD_MQTT_TEST_SYNC", sync)
        .env("SD_MQTT_TEST_FLUSH", if flush { "yes" } else { "no" })
        .output()
        .expect("the child must run");
    assert!(
        output.status.success(),
        "the child failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Run by [`crash_a_child_writing_to`] in a child process, never as part
/// of the suite: without the environment it does nothing.
#[test]
#[ignore = "a helper the durability tests run as a child process"]
fn child_writes_then_exits_abruptly() {
    let Ok(path) = std::env::var("SD_MQTT_TEST_STORE") else {
        return;
    };
    let sync = match std::env::var("SD_MQTT_TEST_SYNC").as_deref() {
        Ok("relaxed") => SyncMode::Relaxed,
        _ => SyncMode::Immediate,
    };
    let store = RedbStorage::open_with(&path, sync).expect("the child's store must open");
    store
        .apply(&[
            Record::SessionPut(session("sensor")),
            Record::BodyPut {
                id: body_id(1),
                body: body(1),
            },
            Record::QueuePushed {
                session: key("sensor"),
                entry: queue_entry(1, body_id(1)),
            },
        ])
        .expect("the child's batch must be accepted");
    if std::env::var("SD_MQTT_TEST_FLUSH").as_deref() == Ok("yes") {
        store.flush().expect("the child's flush must succeed");
    }
    // No destructor runs from here, which is the point.
    std::process::exit(0);
}

/// What a flush returned for is still there after the process died
/// without closing the store.
#[test]
fn a_flush_is_what_survives_an_unclean_exit() {
    let workspace = workspace();
    let path = store_path(&workspace);
    crash_a_child_writing_to(&path, "immediate", true);

    let store = open(&path);
    let restored = store.restore().expect("restoring must succeed");
    assert_eq!(
        restored.sessions.len(),
        1,
        "the flushed session did not survive"
    );
    assert_eq!(
        restored.sessions[0].queue,
        vec![queue_entry(1, body_id(1))],
        "the flushed queue entry did not survive"
    );
    assert_eq!(restored.bodies.get(&body_id(1)), Some(&body(1)));
    assert_eq!(store.queue_depth(&key("sensor")).expect("depth"), 1);
}

/// And a batch that was never flushed is not, which is what makes the
/// flush above worth calling.
///
/// This is the promise the whole milestone rests on being able to break:
/// if an unflushed batch survived anyway, `apply` would already be durable
/// and section 7.3's rule about acknowledging only after a flush would be
/// guarding nothing.
#[test]
fn a_batch_that_was_never_flushed_does_not_survive_an_unclean_exit() {
    let workspace = workspace();
    let path = store_path(&workspace);
    crash_a_child_writing_to(&path, "immediate", false);

    let store = open(&path);
    let restored = store.restore().expect("restoring must succeed");
    assert_eq!(
        restored.sessions,
        Vec::new(),
        "an unflushed batch reached the disk, so nothing here can prove a flush does anything"
    );
}

/// A relaxed flush does not reach the device, and the batch it covered is
/// gone after an unclean exit.
///
/// This is what stops [`SyncMode`] being decoration. The two modes differ
/// in exactly one observable way and this is it: everything a clean close
/// touches looks identical under both.
#[test]
fn a_relaxed_flush_does_not_survive_an_unclean_exit() {
    let workspace = workspace();
    let path = store_path(&workspace);
    crash_a_child_writing_to(&path, "relaxed", true);

    let store = open(&path);
    assert_eq!(
        store.restore().expect("restoring must succeed").sessions,
        Vec::new(),
        "a relaxed flush reached the device, so the mode is not being honoured"
    );
}

/// A clean close writes out everything, flushed or not.
///
/// Recorded rather than relied on. It is the reason the three tests above
/// go to the trouble of a child process: a restart test that drops the
/// store and reopens it passes with `flush` deleted entirely, so it proves
/// the store is a store and nothing about durability.
#[test]
fn a_clean_close_persists_even_what_was_never_flushed() {
    let workspace = workspace();
    let path = store_path(&workspace);
    {
        let store = open(&path);
        apply(&store, &[Record::SessionPut(session("sensor"))]);
        // No flush.
    }

    let store = open(&path);
    assert_eq!(
        store
            .restore()
            .expect("restoring must succeed")
            .sessions
            .len(),
        1,
        "redb 4.3 changed: a clean close no longer writes out unflushed commits, and the \
         comment on this module should be corrected"
    );
}

// ---- everything else survives a restart ---------------------------------

/// Every record type survives being closed and opened again.
///
/// The contract suite asks whether a store holds what it was given; this
/// asks whether the file does. A field that is written but never read back
/// is a whole class of defect that only a reopen can see.
#[test]
fn every_kind_of_state_survives_a_restart() {
    let workspace = workspace();
    let path = store_path(&workspace);
    let before = {
        let store = open(&path);
        apply(
            &store,
            &[
                Record::SessionPut(session("sensor")),
                Record::SubscriptionAdded {
                    session: key("sensor"),
                    subscription: storage::SubscriptionRecord {
                        filter: "a/b".to_owned(),
                        qos: Qos::ExactlyOnce,
                        no_local: true,
                        retain_as_published: true,
                        retain_handling: storage::RetainHandling::SendIfNew,
                        subscription_id: Some(9),
                    },
                },
                Record::InboundReserved {
                    session: key("sensor"),
                    packet_id: 7,
                },
                Record::BodyPut {
                    id: body_id(1),
                    body: body(1),
                },
                Record::InflightPut {
                    session: key("sensor"),
                    entry: storage::InflightRecord {
                        packet_id: 4,
                        sequence: 1,
                        state: storage::InflightState::AwaitingComplete,
                        sent_at: 1_700_000_050,
                        delivery: queue_entry(1, body_id(1)).delivery,
                    },
                },
                Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(2, body_id(1)),
                },
                Record::QueuePopped {
                    session: key("sensor"),
                    sequence: 2,
                },
                Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(3, body_id(1)),
                },
                Record::RetainedPut {
                    topic: "a/b".to_owned(),
                    message: storage::RetainedRecord {
                        payload: bytes::Bytes::from_static(b"kept"),
                        qos: Qos::ExactlyOnce,
                        expires_at: Some(1_700_000_900),
                        properties: bytes::Bytes::from_static(&[0x01, 0x00]),
                    },
                },
                // Section 8.2's clock mark. A counter that is not zero,
                // because zero is what a store that held nothing would
                // also produce.
                Record::ClockHighWaterPut {
                    reading: storage::ClockRecord {
                        millis: 1_700_000_000_123,
                        counter: 7,
                    },
                },
            ],
        );
        store.flush().expect("a flush must succeed");
        store.restore().expect("restoring must succeed")
    };

    let store = open(&path);
    assert_eq!(
        store.restore().expect("restoring must succeed"),
        before,
        "the store came back different from how it went in"
    );
    assert_eq!(
        store.queue_depth(&key("sensor")).expect("depth"),
        1,
        "the depth did not survive, or was recounted from what is held"
    );
    assert_eq!(
        store.highest_sequence(&key("sensor")).expect("the mark"),
        3,
        "the high-water mark did not survive, so a sequence could be reused"
    );
    assert_eq!(
        store
            .restore()
            .expect("restoring must succeed")
            .clock
            .expect("the clock mark survived the restart"),
        storage::ClockRecord {
            millis: 1_700_000_000_123,
            counter: 7,
        },
        "the clock mark did not survive, so this node's clock would resume \
         behind itself and re-issue a stamp"
    );
}

/// The depth and the high-water mark come back from the file rather than
/// from a count of what is held.
///
/// A queue that was fully drained holds nothing, so a store that recounted
/// at startup would come back with a mark of zero and hand the next
/// message a number that an inflight entry still owns.
#[test]
fn a_drained_queue_restores_its_mark_and_not_its_length() {
    let workspace = workspace();
    let path = store_path(&workspace);
    {
        let store = open(&path);
        apply(&store, &[Record::SessionPut(session("sensor"))]);
        for sequence in 1..=3u64 {
            apply(
                &store,
                &[Record::QueuePushed {
                    session: key("sensor"),
                    entry: queue_entry(sequence, body_id(1)),
                }],
            );
        }
        for sequence in 1..=3u64 {
            apply(
                &store,
                &[Record::QueuePopped {
                    session: key("sensor"),
                    sequence,
                }],
            );
        }
        store.flush().expect("a flush must succeed");
    }

    let store = open(&path);
    assert_eq!(store.queue_depth(&key("sensor")).expect("depth"), 0);
    assert_eq!(
        store.highest_sequence(&key("sensor")).expect("the mark"),
        3,
        "the mark came back from what is held rather than from the file"
    );
    assert_eq!(
        store.restore().expect("restoring must succeed").sessions[0].highest_sequence,
        3
    );
}

// ---- a body's reference count is stored, not remembered -----------------

/// The count of references to a body survives a restart.
///
/// Section 7.2 says the count is persisted beside the body rather than
/// rebuilt by scanning at startup, and a broker with a large store cannot
/// afford that scan. Without this test the count could live only in
/// memory: both of the contract suite's body tests would still pass,
/// because neither of them closes the store.
///
/// The restart is placed between the two releases on purpose. A count that
/// came back as one would delete the body on the first release, under a
/// queue entry that still names it.
#[test]
fn a_body_reference_count_survives_a_restart() {
    let workspace = workspace();
    let path = store_path(&workspace);
    {
        let store = open(&path);
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
        store.flush().expect("a flush must succeed");
    }

    // One of the two sessions takes its copy and releases its reference.
    {
        let store = open(&path);
        apply(
            &store,
            &[
                Record::QueuePopped {
                    session: key("one"),
                    sequence: 1,
                },
                Record::BodyReleased { id: body_id(1) },
            ],
        );
        assert_eq!(
            store.restore().expect("restoring").bodies.get(&body_id(1)),
            Some(&body(1)),
            "the body went on the first of two releases, and session two's queue entry now \
             names nothing"
        );
        store.flush().expect("a flush must succeed");
    }

    // And the second, after another restart.
    {
        let store = open(&path);
        assert_eq!(
            store.restore().expect("restoring").bodies.get(&body_id(1)),
            Some(&body(1)),
            "the body did not survive the restart"
        );
        apply(
            &store,
            &[
                Record::QueuePopped {
                    session: key("two"),
                    sequence: 1,
                },
                Record::BodyReleased { id: body_id(1) },
            ],
        );
        assert!(
            store.restore().expect("restoring").bodies.is_empty(),
            "the body outlived its last reference, and nothing will ever release it again"
        );
        store.flush().expect("a flush must succeed");
    }

    // A release past the last one is still refused after a restart, which
    // a count that came back too high would accept.
    let store = open(&path);
    assert!(
        matches!(
            store.apply(&[Record::BodyReleased { id: body_id(1) }]),
            Err(StorageError::Rejected(
                storage::Rejection::BodyNotHeld { .. }
            ))
        ),
        "the body is still held after its last reference was released"
    );
}

/// A body stored once and referenced many times is stored once on disk.
///
/// The count is what makes that safe, so it is worth asserting that the
/// saving is real rather than assumed: a thousand references to a
/// megabyte body must not be a gigabyte of file.
#[test]
fn a_body_referenced_many_times_is_written_once() {
    let workspace = workspace();
    let path = store_path(&workspace);
    let payload = vec![7u8; 64 * 1024];
    {
        let store = open(&path);
        apply(&store, &[Record::SessionPut(session("sensor"))]);
        for sequence in 1..=64u64 {
            apply(
                &store,
                &[
                    Record::BodyPut {
                        id: body_id(1),
                        body: BodyRecord {
                            topic: "sensor/1".to_owned(),
                            payload: payload.clone().into(),
                            qos: Qos::AtLeastOnce,
                            retain: false,
                            expires_at: None,
                            properties: bytes::Bytes::new(),
                        },
                    },
                    Record::QueuePushed {
                        session: key("sensor"),
                        entry: queue_entry(sequence, body_id(1)),
                    },
                ],
            );
        }
        store.flush().expect("a flush must succeed");
    }

    let written = std::fs::metadata(&path)
        .expect("the store file must exist")
        .len();
    let sixty_four_copies = 64 * payload.len() as u64;
    assert!(
        written < sixty_four_copies / 2,
        "the store is {written} bytes, which is the size of storing the body more than once \
         ({sixty_four_copies} bytes would be all sixty-four)"
    );

    let store = open(&path);
    assert_eq!(store.restore().expect("restoring").bodies.len(), 1);
}
