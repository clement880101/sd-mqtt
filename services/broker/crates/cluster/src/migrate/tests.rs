//! Section 8.4's bounded wait, and what it counts.
//!
//! What is here is the claiming node's half on its own: the cutting of a
//! handover into frames, and what a wait returns when the far node
//! answers, answers partly, or never answers. The whole feature — a
//! client that subscribed on one node and reconnects to another —
//! is `crates/cluster/tests/migration.rs`, because convention 5's last
//! entry is a guard that built its type by hand and so never reached the
//! caller it was supposed to be about.

use super::*;

use storage::record::{DeliveryRecord, InflightState, MessageId, Qos, RetainHandling};

fn key() -> SessionKey {
    SessionKey::new("", "a-client")
}

fn body(topic: &str) -> BodyRecord {
    BodyRecord {
        topic: topic.to_owned(),
        payload: bytes::Bytes::from_static(b"21.5"),
        qos: Qos::AtLeastOnce,
        retain: false,
        expires_at: None,
        properties: bytes::Bytes::new(),
    }
}

fn delivery(nth: u8) -> DeliveryRecord {
    DeliveryRecord {
        body: MessageId([nth; 16]),
        qos: Qos::AtLeastOnce,
        retain: false,
        subscription_ids: Vec::new(),
        dup: false,
    }
}

fn queued(nth: u8) -> (QueueEntry, BodyRecord) {
    (
        QueueEntry {
            sequence: u64::from(nth),
            packet_id: None,
            delivery: delivery(nth),
        },
        body("site/1/temp"),
    )
}

fn outstanding(nth: u8) -> (InflightRecord, BodyRecord) {
    (
        InflightRecord {
            packet_id: u16::from(nth) + 1,
            sequence: u64::from(nth),
            state: InflightState::AwaitingAck,
            sent_at: 0,
            delivery: delivery(nth),
        },
        body("site/1/temp"),
    )
}

fn subscription(filter: &str) -> SubscriptionRecord {
    SubscriptionRecord {
        filter: filter.to_owned(),
        qos: Qos::AtLeastOnce,
        no_local: false,
        retain_as_published: false,
        retain_handling: RetainHandling::SendAlways,
        subscription_id: None,
    }
}

/// A handover of `messages` queued messages and one filter.
/// Await a drain under a deadline of the test harness's own.
///
/// **Every wait on a drain in this file goes through here, and that is
/// the point.** The drain's own patience is the thing under test, so a
/// build whose wait is not bounded leaves a test with nothing to make
/// it return: it hangs, takes the binary's remaining tests with it,
/// and reports nothing. That is a worse failure than a failing
/// assertion and it is the one the "drain unbounded" mutation
/// produces, which is how it was found — the mutation hung the suite
/// three times before every call site was wrapped rather than two.
///
/// Ten seconds is far beyond any patience a test here passes in, so
/// reaching it is never a slow machine; it is a missing bound.
async fn bounded<F: std::future::Future<Output = Drain>>(drain: F) -> Drain {
    tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .expect("a bounded wait returns; an unbounded one is what this catches")
}

fn handover(messages: u8) -> Handover {
    Handover {
        subscriptions: vec![subscription("site/+/temp")],
        inflight: Vec::new(),
        queued: (0..messages).map(queued).collect(),
        inbound: vec![7],
        highest_sequence: u64::from(messages),
    }
}

#[test]
fn a_session_holding_nothing_is_still_one_chunk_and_it_says_last() {
    // A drain that sent no frame at all is indistinguishable at the far
    // end from a node that never answered, and the two must not be: one
    // is a session with an empty queue and the other is the shortfall
    // section 8.4 counts.
    let chunks = Drained::chunks(&key(), 3, Handover::default());
    assert_eq!(chunks.len(), 1);
    assert!(chunks[0].last, "the one chunk is the last chunk");
    assert_eq!(chunks[0].announced, 0);
    assert_eq!(chunks[0].epoch, 3);
}

#[test]
fn a_queue_larger_than_one_frame_is_cut_up_and_only_the_last_says_so() {
    let messages = u8::try_from(CHUNK + 5).expect("the chunk is small enough to count in a byte");
    let chunks = Drained::chunks(&key(), 1, handover(messages));
    assert_eq!(
        chunks.len(),
        2,
        "sixty-nine messages in frames of sixty-four"
    );
    assert_eq!(chunks[0].queued.len(), CHUNK);
    assert_eq!(chunks[1].queued.len(), 5);
    assert!(!chunks[0].last);
    assert!(chunks[1].last);
    for chunk in &chunks {
        assert_eq!(
            chunk.announced,
            u64::from(messages),
            "every chunk says the whole figure, so a node that missed the first \
             still knows what it is short by"
        );
    }
}

#[test]
fn the_window_crosses_before_the_queue_and_in_order() {
    // The far end puts the window back in order and a chunk boundary
    // must not reorder it.
    let held = Handover {
        subscriptions: Vec::new(),
        inflight: (0..3).map(outstanding).collect(),
        queued: (3..6).map(queued).collect(),
        inbound: Vec::new(),
        highest_sequence: 6,
    };
    let chunks = Drained::chunks(&key(), 1, held);
    assert_eq!(chunks.len(), 1);
    assert_eq!(
        chunks[0]
            .inflight
            .iter()
            .map(|(entry, _)| entry.sequence)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        chunks[0]
            .queued
            .iter()
            .map(|(entry, _)| entry.sequence)
            .collect::<Vec<_>>(),
        vec![3, 4, 5]
    );
}

#[test]
fn the_filters_ride_the_first_chunk() {
    // So that a drain which stops partway has still moved the
    // subscriptions, which is what makes section 8.3 forward to the new
    // node at all.
    let messages = u8::try_from(CHUNK + 1).expect("the chunk is small enough to count in a byte");
    let chunks = Drained::chunks(&key(), 1, handover(messages));
    assert_eq!(chunks[0].subscriptions.len(), 1);
    assert_eq!(chunks[0].inbound, vec![7]);
    assert!(
        chunks[1].subscriptions.is_empty(),
        "and nowhere else, or the far node installs them twice"
    );
}

#[tokio::test]
async fn a_drain_that_finishes_is_complete_and_short_by_nothing() {
    let migrations = Arc::new(Migrations::default());
    let awaiting = migrations.begin(&key(), 4);
    for chunk in Drained::chunks(&key(), 4, handover(3)) {
        migrations.arrived(chunk);
    }
    let drain = bounded(awaiting.collect(Duration::from_secs(5))).await;
    assert!(drain.complete);
    assert_eq!(drain.missing, 0);
    assert_eq!(drain.handover.queued.len(), 3);
    assert_eq!(drain.handover.subscriptions.len(), 1);
    assert_eq!(drain.handover.inbound, vec![7]);
    assert_eq!(drain.handover.highest_sequence, 3);
    assert_eq!(migrations.stats().drained(), 1);
    assert_eq!(migrations.stats().incomplete(), 0);
    assert_eq!(migrations.stats().messages(), 3);
    assert_eq!(migrations.stats().missing(), 0);
}

#[tokio::test]
async fn a_drain_that_stops_partway_keeps_what_arrived_and_counts_the_rest() {
    // Section 8.4: the connection is accepted with what arrived, the
    // session is reported present, and **the shortfall is counted**.
    // The count is what this asserts; the acceptance is the connection
    // test's.
    let migrations = Arc::new(Migrations::default());
    let awaiting = migrations.begin(&key(), 4);
    let messages = u8::try_from(CHUNK + 5).expect("the chunk is small enough to count in a byte");
    let mut chunks = Drained::chunks(&key(), 4, handover(messages));
    chunks.truncate(1);
    for chunk in chunks {
        migrations.arrived(chunk);
    }

    // Bounded from the outside as well as from within, for the reason
    // the next test gives: the mutation this guards against is "the
    // drain unbounded", and a test that hangs under it reports nothing
    // and takes the whole binary with it. The patience is 150ms, so a
    // second is a generous ceiling and a failure here is a real one.
    let drain = bounded(awaiting.collect(Duration::from_millis(150))).await;

    assert!(!drain.complete, "the far node never said it had finished");
    assert_eq!(drain.handover.queued.len(), CHUNK, "what did arrive");
    assert_eq!(
        drain.handover.subscriptions.len(),
        1,
        "and the filters with it, because they rode the first chunk"
    );
    assert_eq!(drain.missing, 5);
    assert_eq!(migrations.stats().missing(), 5);
    assert_eq!(migrations.stats().incomplete(), 1);
    assert_eq!(migrations.stats().drained(), 0);
    assert_eq!(migrations.stats().messages(), CHUNK as u64);
}

#[tokio::test]
async fn the_wait_is_bounded_over_the_whole_drain_and_not_over_each_chunk() {
    // **The mutation this exists for is "the drain unbounded", and it
    // must fail rather than hang.** A deadline recomputed per chunk is a
    // bound a sender can walk past for ever by sending one message at a
    // time, so the far node here never stops and never says `last`. The
    // collect is wrapped in a timeout of its own: a build whose wait is
    // not bounded fails this assertion instead of running until the test
    // harness is killed.
    let migrations = Arc::new(Migrations::default());
    let awaiting = migrations.begin(&key(), 9);
    let sending = Arc::clone(&migrations);
    let dribbling = tokio::spawn(async move {
        loop {
            let mut chunk = Drained::chunks(&key(), 9, handover(1))
                .pop()
                .expect("one chunk");
            chunk.last = false;
            chunk.announced = u64::MAX;
            sending.arrived(chunk);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });

    let started = std::time::Instant::now();
    let drain = bounded(awaiting.collect(PATIENCE)).await;
    dribbling.abort();

    assert!(!drain.complete);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the wait ran to {:?}, which is not the patience it was given",
        started.elapsed()
    );
    assert_eq!(migrations.stats().incomplete(), 1);
}

#[tokio::test]
async fn a_chunk_answering_another_claim_is_not_taken_for_this_one() {
    // Two claims of one session can be in flight at once — that is what
    // a partition does — and a chunk answering the earlier must not
    // acknowledge the later on a queue drained to somebody else.
    let migrations = Arc::new(Migrations::default());
    let awaiting = migrations.begin(&key(), 8);
    for chunk in Drained::chunks(&key(), 7, handover(3)) {
        migrations.arrived(chunk);
    }
    let drain = bounded(awaiting.collect(Duration::from_millis(150))).await;
    assert!(!drain.complete, "nothing answering epoch eight arrived");
    assert_eq!(drain.handover.messages(), 0);
    assert_eq!(migrations.stats().late(), 1, "and the stray is counted");
}

#[tokio::test]
async fn a_waiter_leaves_the_table_when_the_wait_is_over() {
    // Or a node that migrated a million sessions holds a million
    // channels nothing will ever send to.
    let migrations = Arc::new(Migrations::default());
    let awaiting = migrations.begin(&key(), 2);
    assert_eq!(locked(&migrations.waiting).len(), 1);
    let _ = bounded(awaiting.collect(Duration::from_millis(50))).await;
    assert!(locked(&migrations.waiting).is_empty());
    migrations.arrived(
        Drained::chunks(&key(), 2, Handover::default())
            .pop()
            .expect("one chunk"),
    );
    assert_eq!(migrations.stats().late(), 1);
}

/// Long enough that a loaded machine does not mistake scheduling for a
/// bound, short enough that the test is not a wait.
const PATIENCE: Duration = Duration::from_millis(300);
