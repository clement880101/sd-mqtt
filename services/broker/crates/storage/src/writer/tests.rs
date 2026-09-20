//! What the writer must do.
//!
//! # How these avoid being decorative
//!
//! Three hazards, each of which has a shape here.
//!
//! **Counting flushes needs a store that can be asked.** Neither real
//! backend can say how often it was flushed, so [`Fake`] records what it
//! was asked to do, in order. Every coalescing claim is a statement about
//! that log, not about elapsed time.
//!
//! **Coalescing is a race unless the thread is held still.** A test that
//! sends several batches and hopes they arrive together is a flaky test
//! that passes for the wrong reason most of the time. [`Fake`] can be
//! held inside its first `apply`, so the batches that must coalesce are
//! provably all in the channel before the thread can look at any of them.
//! Nothing here sleeps in the hope that something happened.
//!
//! **A clean close persists everything, flushed or not.** Measured
//! against redb 4.3 and pinned by
//! `a_clean_close_persists_even_what_was_never_flushed` in `tests/redb.rs`.
//! So "apply, drop the handle, reopen, everything is there" passes
//! against a writer that never flushes at all, and is not a shutdown
//! test. The shutdown claim is made twice here instead, in two ways that
//! cannot be fooled by a destructor: against [`Fake`], where the flush is
//! an entry in the log and its absence is visible; and against redb in a
//! child process that ends with [`std::process::exit`], so nothing
//! reaches the file that the writer did not put there. The second has the
//! control that makes it mean something —
//! [`a_batch_nobody_waited_for_is_not_yet_durable`] — without which both
//! would pass against a store that was durable all along.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Condvar, Mutex, PoisonError};

use super::*;
use crate::record::{
    BodyRecord, DeliveryRecord, MessageId, ProtocolVersion, Qos, QueueEntry, RetainHandling,
    SessionKey, SessionRecord, SubscriptionRecord,
};
use crate::restore::Restored;
use crate::{MemoryStorage, RedbStorage, Rejection};

// ---- records to write ---------------------------------------------------

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

fn queue_entry(sequence: u64, id: MessageId) -> QueueEntry {
    QueueEntry {
        sequence,
        packet_id: None,
        delivery: DeliveryRecord {
            body: id,
            qos: Qos::AtLeastOnce,
            retain: false,
            subscription_ids: Vec::new(),
            dup: false,
        },
    }
}

/// One record that any store accepts, for tests that only care that a
/// batch is a batch.
fn something(n: u8) -> Record {
    Record::RetainedPut {
        topic: format!("topic/{n}"),
        message: crate::RetainedRecord {
            payload: format!("retained-{n}").into_bytes().into(),
            qos: Qos::AtLeastOnce,
            expires_at: None,
            properties: bytes::Bytes::new(),
        },
    }
}

/// A batch the memory backend refuses: a subscription for a session that
/// was never stored, which is [`Rejection::NoSuchSession`].
fn a_batch_the_store_refuses() -> Vec<Record> {
    vec![Record::SubscriptionAdded {
        session: key("never-connected"),
        subscription: SubscriptionRecord {
            filter: "sensor/#".to_owned(),
            qos: Qos::AtLeastOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendAlways,
            subscription_id: None,
        },
    }]
}

// ---- a store that can be counted and held -------------------------------

/// What the thread asked the store to do, in the order it asked.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    /// A batch of this many records was applied.
    Applied(usize),
    /// A flush was attempted.
    Flushed,
}

/// A store that records what it was asked, can be held still, and can be
/// told to fail.
///
/// It stores nothing. Everything about what a store must *hold* is proved
/// in `tests/backends.rs` against both real backends; what is proved here
/// is only when the writer calls them and who it tells.
#[derive(Debug, Default)]
struct Fake {
    /// Every call, in order.
    events: Mutex<Vec<Event>>,
    /// While true, `apply` blocks.
    held: Mutex<bool>,
    /// Signalled when `held` changes.
    released: Condvar,
    /// Set by `apply` when it finds itself held, so a test can know the
    /// thread is parked rather than guessing.
    caught: AtomicBool,
    /// When set, `flush` fails with [`StorageError::Engine`] carrying it.
    flush_fails_with: Mutex<Option<String>>,
    /// When set, `apply` fails with [`StorageError::Engine`] carrying it.
    apply_fails_with: Mutex<Option<String>>,
    /// Flushes, so a test can wait for one without locking the log.
    flushes: AtomicU64,
}

impl Fake {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Hold the next `apply` until [`Fake::release`].
    fn hold(&self) {
        *self.held.lock().unwrap_or_else(PoisonError::into_inner) = true;
    }

    /// Whether the thread is parked inside a held `apply`.
    fn has_caught_the_thread(&self) -> bool {
        self.caught.load(Ordering::SeqCst)
    }

    /// Let it go.
    fn release(&self) {
        *self.held.lock().unwrap_or_else(PoisonError::into_inner) = false;
        self.released.notify_all();
    }

    /// Make every later flush fail with this message.
    fn fail_flushes_with(&self, reason: &str) {
        *self
            .flush_fails_with
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(reason.to_owned());
    }

    /// Make every later apply fail with this message.
    fn fail_applies_with(&self, reason: &str) {
        *self
            .apply_fails_with
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(reason.to_owned());
    }

    /// Let applies succeed again.
    fn stop_failing_applies(&self) {
        *self
            .apply_fails_with
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }

    /// Let flushes succeed again.
    fn stop_failing(&self) {
        *self
            .flush_fails_with
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = None;
    }

    fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn flushes(&self) -> u64 {
        self.flushes.load(Ordering::SeqCst)
    }

    fn record(&self, event: Event) {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event);
    }
}

impl Storage for Fake {
    fn apply(&self, records: &[Record]) -> Result<(), StorageError> {
        let mut held = self.held.lock().unwrap_or_else(PoisonError::into_inner);
        if *held {
            self.caught.store(true, Ordering::SeqCst);
        }
        while *held {
            held = self
                .released
                .wait(held)
                .unwrap_or_else(PoisonError::into_inner);
        }
        drop(held);
        self.record(Event::Applied(records.len()));
        match self
            .apply_fails_with
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        {
            Some(reason) => Err(StorageError::Engine(reason)),
            None => Ok(()),
        }
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.record(Event::Flushed);
        self.flushes.fetch_add(1, Ordering::SeqCst);
        match self
            .flush_fails_with
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
        {
            Some(reason) => Err(StorageError::Engine(reason)),
            None => Ok(()),
        }
    }

    fn restore(&self) -> Result<Restored, StorageError> {
        Ok(Restored::default())
    }

    fn queue_head(&self, _: &SessionKey, _: u64) -> Result<Option<QueueEntry>, StorageError> {
        Ok(None)
    }

    fn queue_depth(&self, _: &SessionKey) -> Result<u64, StorageError> {
        Ok(0)
    }

    fn highest_sequence(&self, _: &SessionKey) -> Result<u64, StorageError> {
        Ok(0)
    }
}

/// A store that dies rather than answering, for the one path where the
/// thread stops with waiters still queued.
#[derive(Debug)]
struct Unavailable;

impl Storage for Unavailable {
    fn apply(&self, _: &[Record]) -> Result<(), StorageError> {
        panic!("deliberate: the storage writer thread is being killed on purpose");
    }

    fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }

    fn restore(&self) -> Result<Restored, StorageError> {
        Ok(Restored::default())
    }

    fn queue_head(&self, _: &SessionKey, _: u64) -> Result<Option<QueueEntry>, StorageError> {
        Ok(None)
    }

    fn queue_depth(&self, _: &SessionKey) -> Result<u64, StorageError> {
        Ok(0)
    }

    fn highest_sequence(&self, _: &SessionKey) -> Result<u64, StorageError> {
        Ok(0)
    }
}

// ---- configurations -----------------------------------------------------

/// Neither the interval nor the count can fire, so every flush a test
/// sees was caused by a waiter or by shutdown.
///
/// An hour rather than a long-but-plausible figure: if a test ever became
/// slow enough for the interval to fire, the assertion it makes about
/// flush counts would start failing for a reason that has nothing to do
/// with the writer.
fn only_waiters_flush() -> WriterConfig {
    WriterConfig {
        interval: Duration::from_secs(3_600),
        records: u64::MAX,
    }
}

// ---- waiting, without sleeping and hoping -------------------------------

/// Poll `ready` until it is true, failing rather than hanging.
///
/// Used only where the thing being waited for is a trigger that fires on
/// its own — the interval and the record count. Everywhere else the
/// thread is held still instead, because a test that waits for a
/// coalesced flush cannot tell a slow writer from a correct one.
async fn eventually(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Await a waiter, failing rather than hanging.
///
/// Found by mutation, not by design. A writer that never flushes because
/// of a waiter leaves every one of these unanswered, and a bare `.await`
/// turns that into a suite that hangs. A suite that hangs says nothing:
/// the mutation it was supposed to kill survives as far as any report is
/// concerned, because there is no report.
async fn answered(what: &str, waiter: Durable) -> Result<(), NotDurable> {
    tokio::time::timeout(Duration::from_secs(10), waiter)
        .await
        .unwrap_or_else(|_| panic!("{what}: the waiter was never answered"))
}

// ---- coalescing ---------------------------------------------------------

/// Five batches nobody waited on and one that was waited on cost one
/// flush, not six.
///
/// The claim the module exists for. At 360 uncoalesced durable writes a
/// second, six flushes for six batches is the rate that makes the broker
/// useless.
#[tokio::test]
async fn batches_without_waiters_are_not_flushed_one_by_one() {
    let store = Fake::new();
    let writer = Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, only_waiters_flush())
        .expect("the writer must start");

    for n in 0..5 {
        writer
            .apply(vec![something(n)])
            .expect("the writer must accept a batch");
    }
    let waiter = writer
        .apply_durable(vec![something(5)])
        .expect("the writer must accept a batch");
    answered("the sixth batch", waiter)
        .await
        .expect("the flush must succeed");

    assert_eq!(
        store.flushes(),
        1,
        "six batches cost {} flushes; the log was {:?}",
        store.flushes(),
        store.events()
    );
}

/// Waiters that arrive while the thread is busy are woken by one flush.
///
/// The thread is held inside its first `apply`, so all eight waiters are
/// provably in the channel before it can look at any of them. Without
/// that the test would be a race, and a race that usually passes is
/// worse than no test.
#[tokio::test]
async fn many_waiters_arriving_together_are_woken_by_one_flush() {
    let store = Fake::new();
    store.hold();
    let writer = Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, only_waiters_flush())
        .expect("the writer must start");

    writer
        .apply(vec![something(0)])
        .expect("the writer must accept a batch");
    eventually("the thread to be held inside apply", || {
        store.has_caught_the_thread()
    })
    .await;

    let waiters: Vec<_> = (1..9)
        .map(|n| {
            writer
                .apply_durable(vec![something(n)])
                .expect("the writer must accept a batch")
        })
        .collect();
    store.release();

    for (index, waiter) in waiters.into_iter().enumerate() {
        answered(&format!("waiter {index}"), waiter)
            .await
            .unwrap_or_else(|error| panic!("waiter {index} was told {error}"));
    }
    assert_eq!(
        store.flushes(),
        1,
        "eight waiters cost {} flushes; the log was {:?}",
        store.flushes(),
        store.events()
    );
}

// ---- a failed flush -----------------------------------------------------

/// A flush that fails reaches every waiter it should have covered.
///
/// This is the one that matters. Telling a publisher its message is safe
/// when the device refused is precisely the failure this milestone exists
/// to prevent, and a writer that reported success here would be worse
/// than no writer: the broker would acknowledge, the publisher would
/// forget, and the message would be gone.
///
/// Set up exactly like the coalescing test above, so the eight waiters
/// are the ones a single flush covers. Every one of them must be told,
/// and told the cause: convention 2 asks for the exact error, because a
/// test that accepts any failure would pass against a writer that
/// invented one.
#[tokio::test]
async fn a_flush_that_fails_reaches_every_waiter_it_should_have_covered() {
    let store = Fake::new();
    store.hold();
    let writer = Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, only_waiters_flush())
        .expect("the writer must start");

    writer
        .apply(vec![something(0)])
        .expect("the writer must accept a batch");
    eventually("the thread to be held inside apply", || {
        store.has_caught_the_thread()
    })
    .await;

    let waiters: Vec<_> = (1..9)
        .map(|n| {
            writer
                .apply_durable(vec![something(n)])
                .expect("the writer must accept a batch")
        })
        .collect();
    store.fail_flushes_with("the device refused the write");
    store.release();

    for (index, waiter) in waiters.into_iter().enumerate() {
        let error = answered(&format!("waiter {index}"), waiter)
            .await
            .expect_err(&format!("waiter {index} was told the write was durable"));
        match error.store_error() {
            Some(StorageError::Engine(reason)) => {
                assert_eq!(reason, "the device refused the write", "waiter {index}");
            }
            other => panic!("waiter {index} was told {other:?}"),
        }
    }
    assert_eq!(
        store.flushes(),
        1,
        "the eight waiters were not covered by one flush, so the test is not asserting what \
         it claims; the log was {:?}",
        store.events()
    );
}

/// A failed flush does not become a permanent one.
///
/// The counters reset on the attempt, so the writer goes on coalescing;
/// what must not reset is the knowledge that those records have not been
/// through a successful flush, which is why shutdown flushes again.
#[tokio::test]
async fn a_writer_goes_on_after_a_flush_fails() {
    let store = Fake::new();
    store.fail_flushes_with("the device refused the write");
    let writer = Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, only_waiters_flush())
        .expect("the writer must start");

    for n in 0..2 {
        let waiter = writer
            .apply_durable(vec![something(n)])
            .expect("the writer must accept a batch");
        answered(&format!("waiter {n}"), waiter)
            .await
            .expect_err("the flush was told to fail");
    }

    assert_eq!(
        store.events(),
        vec![
            Event::Applied(1),
            Event::Flushed,
            Event::Applied(1),
            Event::Flushed
        ],
        "a writer that gave up after one failure would stop answering waiters at all"
    );
}

/// Records a failed flush did not cover are flushed again at shutdown.
///
/// Found by mutation: clearing the "not yet through a successful flush"
/// flag on a *failed* flush left every test green. It is the difference
/// between a transient write error costing nothing and costing every
/// record that was outstanding when it happened, and no test walked the
/// path — the failed-flush test stops at the waiter, and the shutdown
/// test never fails a flush.
///
/// The counter that decides when to flush is reset by the attempt, so
/// that one bad flush does not leave the writer permanently
/// uncoalesced. What must survive the attempt is the knowledge that
/// those records are still only in memory.
#[tokio::test]
async fn records_a_failed_flush_did_not_cover_are_flushed_again_at_shutdown() {
    let store = Fake::new();
    store.fail_flushes_with("the device refused the write");
    let writer = Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, only_waiters_flush())
        .expect("the writer must start");

    let waiter = writer
        .apply_durable(vec![something(0)])
        .expect("the writer must accept a batch");
    answered("the batch whose flush fails", waiter)
        .await
        .expect_err("the flush was told to fail");
    assert_eq!(store.events(), vec![Event::Applied(1), Event::Flushed]);

    store.stop_failing();
    drop(writer);

    assert_eq!(
        store.events(),
        vec![Event::Applied(1), Event::Flushed, Event::Flushed],
        "the record the failed flush did not cover was forgotten, and left the process unwritten"
    );
}

// ---- the triggers nobody waits on ---------------------------------------

/// The interval flushes a batch nobody waited for.
///
/// Section 7.3: at-most-once publications touch durable state and are
/// never waited on, because nothing was promised. Without this trigger
/// they would sit applied and unflushed until something else happened to
/// want a flush, which on a quiet broker is never.
///
/// The exact log matters as much as the flush. A writer that flushed
/// every interval whether or not anything was outstanding would pass a
/// test that only counted to one, and would fsync a hundred times a
/// second on an idle broker.
#[tokio::test]
async fn the_interval_flushes_a_batch_nobody_waited_for() {
    let store = Fake::new();
    let writer = Writer::spawn(
        Arc::clone(&store) as Arc<dyn Storage>,
        WriterConfig {
            interval: Duration::from_millis(2),
            records: u64::MAX,
        },
    )
    .expect("the writer must start");

    writer
        .apply(vec![something(0)])
        .expect("the writer must accept a batch");
    eventually("the interval to flush", || store.flushes() >= 1).await;

    // Long enough for a hundred intervals, so an idle writer that kept
    // flushing would be caught.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        store.events(),
        vec![Event::Applied(1), Event::Flushed],
        "the interval fired with nothing outstanding"
    );
    drop(writer);
}

/// The record count flushes a batch nobody waited for.
///
/// This is what bounds how much work one flush has to do. Without it a
/// broker taking at-most-once traffic faster than the interval would
/// build an arbitrarily large unflushed batch, and the flush that
/// eventually covered it would be the stall the interval was sized to
/// avoid.
#[tokio::test]
async fn the_record_count_flushes_a_batch_nobody_waited_for() {
    let store = Fake::new();
    let writer = Writer::spawn(
        Arc::clone(&store) as Arc<dyn Storage>,
        WriterConfig {
            interval: Duration::from_secs(3_600),
            records: 3,
        },
    )
    .expect("the writer must start");

    writer
        .apply(vec![something(0), something(1)])
        .expect("the writer must accept a batch");
    eventually("the first batch to be applied", || {
        store.events() == vec![Event::Applied(2)]
    })
    .await;
    assert_eq!(
        store.flushes(),
        0,
        "two records reached the threshold of three"
    );

    writer
        .apply(vec![something(2)])
        .expect("the writer must accept a batch");
    eventually("the count to flush", || store.flushes() >= 1).await;
    assert_eq!(
        store.events(),
        vec![Event::Applied(2), Event::Applied(1), Event::Flushed],
        "the third record did not trigger the flush the count promised"
    );
    drop(writer);
}

/// A batch the store refused is not followed by a write to the device.
///
/// Found by mutation: dropping the "is anything outstanding" guard from
/// the flush decision left every other test green, because the thread
/// blocks rather than ticking when there is nothing to write and so
/// rarely reaches the decision empty-handed. It reaches it here.
///
/// A count of zero is the uncoalesced configuration the throughput
/// measurement needs: every batch that changes something is flushed on
/// its own. One that changed nothing — [`Storage::apply`] is atomic, so
/// a refusal wrote nothing — must not cost a write anyway. A store whose
/// device has gone will otherwise be asked to flush once per refusal for
/// as long as the caller keeps trying.
#[tokio::test]
async fn a_batch_the_store_refused_is_not_followed_by_a_write_to_the_device() {
    let store = Fake::new();
    store.fail_applies_with("the device refused the write");
    let writer = Writer::spawn(
        Arc::clone(&store) as Arc<dyn Storage>,
        WriterConfig {
            interval: Duration::from_secs(3_600),
            records: 0,
        },
    )
    .expect("the writer must start");

    writer
        .apply(vec![something(0)])
        .expect("the writer must accept a batch");
    eventually("the refusal to be counted", || {
        writer.rejected_batches() == 1
    })
    .await;

    // Ordered behind the refusal in the same channel, so the log below
    // is fixed whatever the timing: this batch cannot be applied before
    // any flush the refusal caused.
    store.stop_failing_applies();
    let waiter = writer
        .apply_durable(vec![something(1)])
        .expect("the writer must accept a batch");
    answered("the batch behind the refusal", waiter)
        .await
        .expect("the flush must succeed");

    assert_eq!(
        store.events(),
        vec![Event::Applied(1), Event::Applied(1), Event::Flushed],
        "a refusal, which wrote nothing, was followed by a flush"
    );
}

// ---- what the store refuses ---------------------------------------------

/// A batch the store refuses reaches its own waiter, and nobody else's.
///
/// Told against the memory backend rather than a fake, because the
/// refusal has to be a real one: a fake that fails on command would
/// prove the writer passes on an error it was handed, not that it passes
/// on the error a store actually produces.
#[tokio::test]
async fn a_refused_batch_reaches_its_own_waiter_and_no_other() {
    let store = Arc::new(MemoryStorage::new());
    let writer = Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, only_waiters_flush())
        .expect("the writer must start");

    let refused = writer
        .apply_durable(a_batch_the_store_refuses())
        .expect("the writer must accept a batch");
    let accepted = writer
        .apply_durable(vec![Record::SessionPut(session("sensor"))])
        .expect("the writer must accept a batch");

    match answered("the refused batch", refused)
        .await
        .expect_err("a refused batch must not be reported as durable")
        .store_error()
    {
        Some(StorageError::Rejected(Rejection::NoSuchSession { session })) => {
            assert_eq!(session, &key("never-connected"));
        }
        other => panic!("the refusal was reported as {other:?}"),
    }
    answered("the accepted batch", accepted)
        .await
        .expect("one caller's mistake must not fail another caller's write");

    assert_eq!(
        store
            .restore()
            .expect("restoring must succeed")
            .sessions
            .len(),
        1,
        "the accepted batch was not applied"
    );
}

/// A refusal with nobody waiting is counted rather than dropped.
#[tokio::test]
async fn a_refused_batch_with_nobody_waiting_is_counted() {
    let store = Arc::new(MemoryStorage::new());
    let writer = Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, only_waiters_flush())
        .expect("the writer must start");

    writer
        .apply(a_batch_the_store_refuses())
        .expect("the writer must accept a batch");
    // Ordered behind it in the same channel, so when this returns the
    // refusal has been dealt with. No sleeping.
    let behind_it = writer
        .apply_durable(Vec::new())
        .expect("the writer must accept a batch");
    answered("the batch behind the refusal", behind_it)
        .await
        .expect("the flush must succeed");

    assert_eq!(writer.rejected_batches(), 1);
}

/// A waiter whose writer died learns that, rather than waiting forever.
///
/// The thread panics on purpose here, so a panic message and a backtrace
/// on stderr during this test are expected. What is being guarded is that
/// a connection holding a [`Durable`] is woken: a waiter that hung would
/// hold the publisher's acknowledgement open until the connection timed
/// out, and would do it silently.
#[tokio::test]
async fn a_waiter_learns_when_the_writer_has_died() {
    let writer =
        Writer::spawn(Arc::new(Unavailable), only_waiters_flush()).expect("the writer must start");

    let waiter = writer
        .apply_durable(vec![something(0)])
        .expect("the writer must accept a batch");
    assert!(
        matches!(
            answered("the waiter on a dead writer", waiter)
                .await
                .expect_err("the thread panicked"),
            NotDurable::WriterStopped
        ),
        "a dead writer must not look like a durable write"
    );
}

// ---- shutdown, against a store that can see it --------------------------

/// Dropping the handle applies what is outstanding and flushes it.
///
/// The honest half of the shutdown claim. "Apply, drop, reopen,
/// everything is there" would pass against a writer that never flushed,
/// because a clean close of redb writes out everything anyway. A log with
/// a flush in it cannot be produced by a writer that did not flush.
///
/// Three batches, all of them sent before the drop and none of them
/// triggering any of the three flush conditions, so the flush in the log
/// can only be the one shutdown made.
#[test]
fn dropping_the_handle_applies_what_is_outstanding_and_flushes_it() {
    let store = Fake::new();
    let writer = Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, only_waiters_flush())
        .expect("the writer must start");

    for n in 0..3 {
        writer
            .apply(vec![something(n)])
            .expect("the writer must accept a batch");
    }
    drop(writer);

    assert_eq!(
        store.events(),
        vec![
            Event::Applied(1),
            Event::Applied(1),
            Event::Applied(1),
            Event::Flushed
        ],
        "the writer stopped with records applied and not flushed"
    );
}

/// A writer with nothing outstanding does not flush on the way out.
///
/// Not a performance point. A flush that covers nothing is a write to the
/// device for no reason, and a writer that made one would also be one
/// whose shutdown flush proves nothing, because it would be there whether
/// or not anything had been applied.
#[test]
fn a_writer_with_nothing_outstanding_does_not_flush_on_the_way_out() {
    let store = Fake::new();
    let writer = Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, only_waiters_flush())
        .expect("the writer must start");
    drop(writer);

    assert_eq!(store.events(), Vec::new());
}

// ---- shutdown and durability, against a disk and a dying process --------

/// Somewhere to put a store.
fn workspace() -> tempfile::TempDir {
    tempfile::tempdir().expect("a temporary directory must be available")
}

fn store_path(workspace: &tempfile::TempDir) -> PathBuf {
    workspace.path().join("store.redb")
}

/// Run the child, which writes and then dies without running a
/// destructor.
///
/// `mode` says what it does before it dies; see
/// [`child_writes_then_exits_abruptly`].
fn crash_a_child_writing_to(path: &Path, mode: &str) {
    let binary = std::env::current_exe().expect("the test binary must know its own path");
    let output = Command::new(binary)
        .args([
            "--exact",
            "--ignored",
            "writer::tests::child_writes_then_exits_abruptly",
        ])
        .env("SD_MQTT_WRITER_STORE", path)
        .env("SD_MQTT_WRITER_MODE", mode)
        .output()
        .expect("the child must run");
    assert!(
        output.status.success(),
        "the child failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// What the child left on the disk.
fn reopen(path: &Path) -> Restored {
    RedbStorage::open(path)
        .expect("the store must reopen")
        .restore()
        .expect("restoring must succeed")
}

/// Run by [`crash_a_child_writing_to`] in a child process, never as part
/// of the suite: without the environment it does nothing.
///
/// It ends with [`std::process::exit`], which runs no destructor, so
/// neither the writer nor the database is closed and nothing reaches the
/// file that was not deliberately put there. The store is held in a
/// binding that outlives everything else for the same reason a clean
/// close is avoided: a dropped [`RedbStorage`] would persist the lot and
/// make all three cases below identical.
#[test]
#[ignore = "a helper the durability tests run as a child process"]
fn child_writes_then_exits_abruptly() {
    let Ok(path) = std::env::var("SD_MQTT_WRITER_STORE") else {
        return;
    };
    let mode = std::env::var("SD_MQTT_WRITER_MODE").unwrap_or_default();
    let store = Arc::new(RedbStorage::open(&path).expect("the child's store must open"));
    let writer = Writer::spawn(
        Arc::clone(&store) as Arc<dyn Storage>,
        // Neither timed nor counted triggers can fire, so whatever
        // survives was put there by a waiter or by shutdown.
        only_waiters_flush(),
    )
    .expect("the child's writer must start");

    let records = vec![
        Record::SessionPut(session("sensor")),
        Record::BodyPut {
            id: body_id(1),
            body: body(1),
        },
        Record::QueuePushed {
            session: key("sensor"),
            entry: queue_entry(1, body_id(1)),
        },
    ];

    match mode.as_str() {
        // A waiter is what makes it durable.
        "waiter" => {
            let waiter = writer
                .apply_durable(records)
                .expect("the child's writer must accept the batch");
            tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("a runtime must be available")
                .block_on(async {
                    // Bounded for the same reason as `answered`: a child
                    // that hangs makes the parent hang, and the suite
                    // reports nothing at all.
                    tokio::time::timeout(Duration::from_secs(10), waiter)
                        .await
                        .expect("the child's waiter was never answered")
                        .expect("the child's write must be durable");
                });
        }
        // Dropping the handle is what makes it durable.
        "shutdown" => {
            writer
                .apply(records)
                .expect("the child's writer must accept the batch");
            drop(writer);
        }
        // Nothing does, which is what makes the other two mean
        // something. The wait is long enough that the batch has
        // certainly been applied; it is the flush that is missing.
        _ => {
            writer
                .apply(records)
                .expect("the child's writer must accept the batch");
            std::thread::sleep(Duration::from_millis(200));
            std::mem::forget(writer);
        }
    }

    // Keeps the database alive past everything above. A dropped
    // `RedbStorage` persists whatever it is holding, flushed or not, which
    // would make all three modes identical.
    std::mem::forget(store);
    std::process::exit(0);
}

/// A batch with a waiter is durable when the waiter returns.
///
/// Proved by reopening the store rather than by asking the writer, and
/// proved across a process that died without closing anything, because a
/// clean close of redb persists whatever it is holding and would make
/// this pass against a writer that never flushed.
#[test]
fn a_batch_is_durable_when_its_waiter_returns() {
    let workspace = workspace();
    let path = store_path(&workspace);
    crash_a_child_writing_to(&path, "waiter");

    let restored = reopen(&path);
    assert_eq!(
        restored.sessions.len(),
        1,
        "the waiter returned before the session was durable"
    );
    assert_eq!(
        restored.sessions[0].queue,
        vec![queue_entry(1, body_id(1))],
        "the waiter returned before the queue entry was durable"
    );
    assert_eq!(restored.bodies.get(&body_id(1)), Some(&body(1)));
}

/// Dropping the handle makes what was outstanding durable.
///
/// The other half of the shutdown claim, against a real disk. The child
/// never waits on anything and no trigger can fire, so the only thing
/// that can have flushed is the drop.
#[test]
fn dropping_the_handle_makes_what_was_outstanding_durable() {
    let workspace = workspace();
    let path = store_path(&workspace);
    crash_a_child_writing_to(&path, "shutdown");

    let restored = reopen(&path);
    assert_eq!(
        restored.sessions.len(),
        1,
        "the writer stopped with records applied and not flushed"
    );
    assert_eq!(restored.sessions[0].queue, vec![queue_entry(1, body_id(1))]);
}

/// And a batch nobody waited for, in a writer nobody shut down, is not
/// durable.
///
/// The control. Without it the two tests above would pass against a store
/// that was durable all along, and would be saying nothing about the
/// writer at all.
#[test]
fn a_batch_nobody_waited_for_is_not_yet_durable() {
    let workspace = workspace();
    let path = store_path(&workspace);
    crash_a_child_writing_to(&path, "neither");

    assert_eq!(
        reopen(&path).sessions,
        Vec::new(),
        "an unflushed batch reached the disk, so nothing here can prove a flush does anything"
    );
}

// ---- the handle ---------------------------------------------------------

/// The thread is named, so a stack dump or a profile says which one it
/// is.
///
/// Small, and here because this thread is the whole point of the module:
/// an unnamed one is indistinguishable from a runtime worker in exactly
/// the situation — a stalled broker — where telling them apart is what
/// the investigation turns on.
#[test]
fn the_thread_is_named() {
    /// A store that reports whose thread it was called on.
    #[derive(Debug)]
    struct Naming(Mutex<Option<String>>);

    impl Storage for Naming {
        fn apply(&self, _: &[Record]) -> Result<(), StorageError> {
            *self.0.lock().unwrap_or_else(PoisonError::into_inner) =
                std::thread::current().name().map(str::to_owned);
            Ok(())
        }
        fn flush(&self) -> Result<(), StorageError> {
            Ok(())
        }
        fn restore(&self) -> Result<Restored, StorageError> {
            Ok(Restored::default())
        }
        fn queue_head(&self, _: &SessionKey, _: u64) -> Result<Option<QueueEntry>, StorageError> {
            Ok(None)
        }
        fn queue_depth(&self, _: &SessionKey) -> Result<u64, StorageError> {
            Ok(0)
        }
        fn highest_sequence(&self, _: &SessionKey) -> Result<u64, StorageError> {
            Ok(0)
        }
    }

    let store = Arc::new(Naming(Mutex::new(None)));
    let writer = Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, only_waiters_flush())
        .expect("the writer must start");
    writer
        .apply(vec![something(0)])
        .expect("the writer must accept a batch");
    drop(writer);

    assert_eq!(
        store
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_deref(),
        Some("sd-mqtt-storage")
    );
}
