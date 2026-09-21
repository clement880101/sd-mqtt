//! The writer that makes durability affordable.
//!
//! Measured on the machine this was written on, an uncoalesced durable
//! write is roughly 360 a second. Coalesced a thousand at a time it is
//! over 300,000. Without this module the broker's acknowledged-delivery
//! rate is 360 messages a second, so it is not an optimisation; it is the
//! difference between a broker and a demonstration.
//!
//! # What it is for
//!
//! Section 7.3: an acknowledgement tells a publisher the broker has taken
//! responsibility for its message. Sending one before the message is
//! durable means a restart can lose a message the publisher was told was
//! safe. So the shell commits its journal, waits for the commit to be
//! durable, and only then delivers the actions. The waiter is how it
//! waits for exactly that and no longer: many connections commit into one
//! flush and are woken together.
//!
//! # It owns a dedicated operating system thread
//!
//! **Chosen, not defaulted to.** A tokio task would be wrong because a
//! durable commit blocks, and blocking a runtime worker stalls every
//! other connection that worker was driving. `spawn_blocking` would be
//! wrong for a subtler reason: the blocking pool is a shared, bounded
//! resource, and a store whose device has stopped answering would occupy
//! a slot in it for as long as the stall lasts, so a failing disk would
//! starve every unrelated blocking call in the process. The engine
//! originally considered for this backend blocks far longer than redb
//! does — its write path contains unbounded loops of `std::thread::sleep`
//! — and the shape here must survive a change of backend.
//!
//! One thread, owned by this module, is a resource nothing else competes
//! for. When it stalls, the connections that asked for durability wait,
//! which is the correct behaviour: they were promised nothing yet. What
//! bounds the channel in front of it while that happens is the next
//! section.
//!
//! # What bounds the backlog
//!
//! The channel is unbounded, and the counting in front of it is what
//! keeps that from being a way to exhaust the process's memory.
//!
//! A bounded channel is not available here. Blocking the sender parks an
//! async connection task on a disk, which is the thing this module exists
//! to avoid, and dropping records loses exactly what the crate is for. So
//! [`Writer::apply`] always accepts, and **says** when the backlog has
//! passed [`WriterConfig::high_water_records`] or
//! [`WriterConfig::high_water_bytes`]. A caller that is told this waits
//! on [`Writer::relief`] before it produces more, and a caller that
//! ignores it is no worse off than it was.
//!
//! That is the shape `Shared::deliver` in `sdmqtt-server` already uses
//! one layer down: a send that never blocks, a signal back, and the
//! policy decided by the shell rather than here. The *decision* differs
//! from that one and has to. A discarded action is recovered when its
//! peer reconnects and the session resumes; a discarded record is a fact
//! the store never learns, and this milestone exists to stop that. So
//! nothing here is dropped — the producer is slowed instead.
//!
//! **Why a bound is needed at all**, since the comment this replaces
//! said it was not: the receive maximum bounds unacknowledged
//! publications, and an acknowledgement is what waits. That covers
//! at-least-once and exactly-once and nothing else. A retained
//! at-most-once publication journals a record carrying the whole payload,
//! touches no quota and is never waited on, so one client publishing in a
//! loop grows the channel by a payload at a time for as long as it
//! outruns the device. A subscribe and unsubscribe loop does the same
//! with smaller records, which is why the count is bounded beside the
//! bytes.
//!
//! # Shutdown
//!
//! Closing the channel is what asks the thread to stop, and it applies
//! and flushes whatever is still outstanding before it does. Dropping
//! [`Writer`] closes the channel and then joins, so "the handle is gone"
//! means "the flush happened". A writer that exited with records applied
//! and not flushed would lose exactly the data this crate exists to keep.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::sync::{oneshot, Notify};

use crate::record::Record;
use crate::{Storage, StorageError};

/// When the thread stops coalescing and flushes.
///
/// Section 7.2 names these: an interval in the low milliseconds, an
/// operation count, or an acknowledgement that must not be lost. The
/// third is not configurable, because it is a correctness requirement
/// rather than a tuning choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterConfig {
    /// How long after a flush the next one becomes due, if anything is
    /// outstanding.
    ///
    /// This bounds how long an at-most-once publication — which nobody
    /// waits on, because nothing was promised — can sit applied and
    /// unflushed. It does not bound anything a waiter cares about; a
    /// waiter flushes immediately.
    pub interval: Duration,
    /// How many records may be applied before a flush becomes due.
    ///
    /// This bounds how much work one flush has to do, which is what keeps
    /// the flush itself from becoming the stall. Zero means every batch
    /// is flushed on its own, which is the uncoalesced rate and is only
    /// useful for measuring it.
    pub records: u64,
    /// How many records may be outstanding before [`Writer::apply`]
    /// reports [`Applied::Backlogged`].
    ///
    /// Outstanding means handed over and not yet applied, which is what
    /// the channel is holding. See the module documentation for why the
    /// answer is a report rather than a block or a drop.
    ///
    /// Not configurable by an operator, for the reason the third flush
    /// trigger is not: this is a bound on the process's memory rather
    /// than a tuning choice, and the number that matters to a deployment
    /// is the one beside it in bytes.
    pub high_water_records: u64,
    /// Roughly how many bytes of records may be outstanding before
    /// [`Writer::apply`] reports [`Applied::Backlogged`].
    ///
    /// Roughly because [`Record::weight`] is an estimate; see it for what
    /// it counts exactly and what it approximates.
    ///
    /// The count above cannot do this on its own. One record may carry a
    /// payload of the configured maximum packet size, so a mark in
    /// records alone permits that size times the count, which is
    /// gigabytes at any count large enough to be worth coalescing.
    pub high_water_bytes: u64,
}

impl Default for WriterConfig {
    /// Two milliseconds, a thousand records, and a backlog of eight
    /// thousand records or 64 MiB.
    ///
    /// The thousand is where the measurement was taken: coalesced a
    /// thousand at a time, the durable rate is over 300,000 a second. The
    /// two milliseconds is the low-millisecond interval section 7.2 asks
    /// for, and it only ever applies to writes nobody is waiting on.
    ///
    /// The two marks are eight flushes' worth of records and 64 MiB of
    /// them, whichever comes first. Both are chosen rather than measured,
    /// and the reasoning is the same for each: it has to be far enough
    /// above one flush that ordinary coalescing never reaches it — a
    /// producer paced at every batch would be the uncoalesced rate — and
    /// low enough that the memory it permits is a number an operator
    /// would accept losing to a stalled disk.
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(2),
            records: 1_000,
            high_water_records: 8_000,
            high_water_bytes: 64 * 1_024 * 1_024,
        }
    }
}

/// What a batch nobody is waiting on left behind it.
///
/// Returned by [`Writer::apply`], which always accepts the batch: this
/// says whether the caller should slow down, not whether it was heard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "a caller told the writer is behind should wait for it rather than produce more"]
pub enum Applied {
    /// The backlog is under both marks. Carry on.
    Accepted,
    /// The batch was applied to the channel and the backlog is now at or
    /// over one of the marks.
    ///
    /// The records are safe; nothing was dropped. What this asks of a
    /// caller is to wait on [`Writer::relief`] before it produces the
    /// next batch, which is what turns a peer that outruns the device
    /// into a peer that is paced by it.
    Backlogged,
}

/// What a flush told the waiters it covered.
///
/// `Arc` because one flush answers every waiter queued behind it and
/// [`StorageError`] is not cloneable — it carries an [`std::io::Error`].
/// Sharing the error keeps the exact cause rather than flattening it to a
/// string, which convention 2 needs: a waiter that can only see a
/// category cannot assert the check it is named after.
type Outcome = Result<(), Arc<StorageError>>;

/// A batch on its way to the thread.
struct Batch {
    /// What to apply. May be empty, which means "flush and tell me": a
    /// connection acknowledging something another batch already
    /// journalled still has to wait for it to reach the device.
    records: Vec<Record>,
    /// Who to wake when it is durable, if anyone is waiting.
    waiter: Option<oneshot::Sender<Outcome>>,
    /// What this batch added to the backlog, to be taken off it again
    /// once the batch has been applied.
    ///
    /// Carried rather than recomputed, so that the two arithmetic
    /// operations are provably the same size however [`Record::weight`]
    /// changes.
    weight: u64,
}

/// What has been handed to the thread and not yet applied.
///
/// Shared between the handle, which adds, and the thread, which
/// subtracts. Two counts because one peer can exhaust memory with few
/// large records and another with many small ones; see
/// [`WriterConfig::high_water_bytes`].
#[derive(Debug, Default)]
struct Backlog {
    /// Records handed over and not yet applied.
    records: AtomicU64,
    /// What [`Record::weight`] said those records hold.
    bytes: AtomicU64,
    /// Woken when the backlog falls back under the marks, and when the
    /// thread stops.
    relief: Notify,
    /// Whether the thread has stopped, for any reason including a panic.
    ///
    /// Without it a caller waiting for relief from a thread that will
    /// never drain anything again waits forever, which turns one dead
    /// thread into every connection hanging.
    stopped: AtomicBool,
}

impl Backlog {
    /// Count a batch on its way in.
    fn added(&self, records: u64, weight: u64) {
        self.records.fetch_add(records, Ordering::Relaxed);
        self.bytes.fetch_add(weight, Ordering::Relaxed);
    }

    /// Count a batch that is applied, or one that was never sent.
    ///
    /// Saturating rather than wrapping: the two sides are paired per
    /// batch and cannot disagree, and if they ever did, a count stuck at
    /// zero paces nobody while a count that wrapped to `u64::MAX` paces
    /// everybody forever.
    fn removed(&self, records: u64, weight: u64) {
        subtract(&self.records, records);
        subtract(&self.bytes, weight);
    }

    /// Whether either mark has been reached.
    ///
    /// The two counts are read separately and so may disagree by a batch
    /// with a concurrent sender. That is a bound being approximate at its
    /// edge, which is what a bound is for.
    fn over(&self, config: WriterConfig) -> bool {
        self.records.load(Ordering::Relaxed) >= config.high_water_records
            || self.bytes.load(Ordering::Relaxed) >= config.high_water_bytes
    }

    /// Say that the backlog has room again, if it has.
    fn relieve(&self, config: WriterConfig) {
        if !self.over(config) {
            self.relief.notify_waiters();
        }
    }

    /// Say that nothing will ever drain again.
    fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.relief.notify_waiters();
    }
}

/// Take `by` off a counter without letting it wrap.
fn subtract(counter: &AtomicU64, by: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
        Some(held.saturating_sub(by))
    });
}

/// The batching writer.
///
/// Cloneable it is not: dropping it is what shuts the thread down, so a
/// second handle would make shutdown depend on which copy went last.
/// Share it behind an [`Arc`] instead; every method takes `&self`.
#[derive(Debug)]
pub struct Writer {
    /// The channel to the thread. `None` only while [`Drop`] is closing
    /// it, which is what asks the thread to flush and stop.
    ///
    /// Unbounded, deliberately, and **not unbounded in what it may
    /// hold**: a bounded channel would have to block the sender or drop
    /// records, and the module documentation has why neither is
    /// available. What keeps this from growing without end is
    /// [`Backlog`] and the two marks it is read against.
    batches: Option<Sender<Batch>>,
    /// The thread, joined on drop. `None` only inside [`Drop`].
    thread: Option<JoinHandle<()>>,
    /// See [`Writer::rejected_batches`].
    rejected: Arc<AtomicU64>,
    /// What is in the channel, and whether it is too much.
    backlog: Arc<Backlog>,
    /// Kept so that [`Writer::relief`] reads the backlog against the same
    /// marks [`Writer::apply`] does.
    config: WriterConfig,
}

/// The writer is no longer running.
///
/// Either the handle is being dropped or the thread has died, which in
/// practice means it panicked. Nothing sent after this is applied, so a
/// caller that sees it must not acknowledge anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the storage writer has stopped")]
pub struct WriterStopped;

/// Why a write this caller was waiting on is not durable.
///
/// Two causes, kept apart because they are different facts. The store
/// refusing is a disk or a caller problem and says which; the writer
/// having stopped says nothing about the write at all, only that nobody
/// is left to report on it. Both mean the same thing to a publisher —
/// **do not acknowledge** — and an operator needs to tell them apart.
#[derive(Debug, Clone, thiserror::Error)]
pub enum NotDurable {
    /// The store refused the batch, or the flush that covered it failed.
    ///
    /// Shared rather than owned because one failing flush answers every
    /// waiter it covered, and each of them gets the same cause.
    #[error("the store did not make the write durable: {0}")]
    Store(Arc<StorageError>),
    /// The writer stopped before the write was made durable, so whether
    /// it reached the device is unknown.
    #[error("the storage writer stopped before the write was made durable")]
    WriterStopped,
}

impl NotDurable {
    /// The store's own error, when there was one.
    ///
    /// Convention 2: a caller — or a test — asserting on the cause needs
    /// the cause, not the category.
    pub fn store_error(&self) -> Option<&StorageError> {
        match self {
            Self::Store(error) => Some(error),
            Self::WriterStopped => None,
        }
    }
}

/// A write that will be durable, or will say why it is not.
///
/// Awaiting it is what section 7.3 means by waiting for the commit before
/// delivering the actions. Dropping it without awaiting is allowed and
/// means the caller stopped caring — the write is still made, and the
/// thread notices nobody is listening and moves on.
#[derive(Debug)]
pub struct Durable(oneshot::Receiver<Outcome>);

impl Future for Durable {
    type Output = Result<(), NotDurable>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(context)
            .map(|received| match received {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(NotDurable::Store(error)),
                // The sender went with the thread, so the thread is gone.
                Err(_) => Err(NotDurable::WriterStopped),
            })
    }
}

impl Writer {
    /// Start a writer against `store`.
    ///
    /// # Errors
    ///
    /// [`std::io::Error`] if the operating system will not give us a
    /// thread. There is no fallback: see the module documentation for why
    /// a runtime worker is not one.
    pub fn spawn(store: Arc<dyn Storage>, config: WriterConfig) -> std::io::Result<Self> {
        let (sender, batches) = mpsc::channel();
        let rejected = Arc::new(AtomicU64::new(0));
        let backlog = Arc::new(Backlog::default());
        let thread = std::thread::Builder::new()
            .name("sd-mqtt-storage".to_owned())
            .spawn({
                let rejected = Arc::clone(&rejected);
                let backlog = Arc::clone(&backlog);
                move || {
                    Thread {
                        store,
                        batches,
                        config,
                        waiting: Vec::new(),
                        records_since_flush: 0,
                        unflushed: false,
                        next_flush: Instant::now() + config.interval,
                        rejected,
                        backlog,
                    }
                    .run();
                }
            })?;
        Ok(Self {
            batches: Some(sender),
            thread: Some(thread),
            rejected,
            backlog,
            config,
        })
    }

    /// Apply a batch, with nobody waiting on it.
    ///
    /// Section 7.3: at-most-once publications are journalled where they
    /// touch durable state and are never waited on, because nothing was
    /// promised. This is that path. The records are durable by the next
    /// flush, whichever trigger causes it, and by shutdown at the latest.
    ///
    /// The batch is always accepted. What comes back says whether the
    /// backlog behind it has reached a high-water mark, in which case the
    /// caller owes the writer a [`Writer::relief`] before it produces
    /// more; the module documentation has why the answer is that rather
    /// than a block or a refusal.
    ///
    /// # Errors
    ///
    /// [`WriterStopped`] if the thread is gone, in which case the records
    /// were not applied.
    pub fn apply(&self, records: Vec<Record>) -> Result<Applied, WriterStopped> {
        self.send(Batch {
            weight: weigh(&records),
            records,
            waiter: None,
        })?;
        Ok(if self.backlog.over(self.config) {
            Applied::Backlogged
        } else {
            Applied::Accepted
        })
    }

    /// Wait until the backlog is back under both marks.
    ///
    /// Returns at once when it already is, and when the writer has
    /// stopped: a caller waiting on a thread that will never drain
    /// anything again would otherwise wait forever, and one dead thread
    /// would become every connection hanging.
    ///
    /// This is the whole of what [`Applied::Backlogged`] asks for. It
    /// makes no promise about durability and cannot fail, because nothing
    /// was promised to anybody: a caller here is being paced, not
    /// answered.
    pub async fn relief(&self) {
        loop {
            // Registered before the backlog is read, which is the order
            // `Notify` documents: the other one drops a wakeup that
            // lands in between and waits for the next, which under a
            // writer that has just caught up may never come.
            let notified = self.backlog.relief.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.backlog.stopped.load(Ordering::Relaxed) || !self.backlog.over(self.config) {
                return;
            }
            notified.await;
        }
    }

    /// How many records are handed over and not yet applied.
    ///
    /// The quantity the first mark is read against. For a test, and for
    /// whatever reports on a broker's health later: a backlog that does
    /// not come down is the shape of a device that has stopped answering.
    pub fn outstanding_records(&self) -> u64 {
        self.backlog.records.load(Ordering::Relaxed)
    }

    /// Roughly how many bytes those records hold, by [`Record::weight`].
    pub fn outstanding_bytes(&self) -> u64 {
        self.backlog.bytes.load(Ordering::Relaxed)
    }

    /// Apply a batch and flush, and hand back something to wait on.
    ///
    /// A batch sent this way makes a flush due immediately, so the
    /// waiting is bounded by the flush rather than by the interval. Every
    /// other waiter queued in the meantime is woken by the same flush,
    /// which is what makes the wait cheap.
    ///
    /// An empty `records` is meaningful and means "flush what is already
    /// applied, and tell me when it is durable".
    ///
    /// These records count towards the backlog like any others, because
    /// they sit in the same channel. Nothing is reported about it here:
    /// a caller holding a [`Durable`] is already waiting for the device,
    /// which is what being paced would ask of it.
    ///
    /// # Errors
    ///
    /// [`WriterStopped`] if the thread is gone, in which case the records
    /// were not applied and nothing will ever be durable.
    pub fn apply_durable(&self, records: Vec<Record>) -> Result<Durable, WriterStopped> {
        let (waiter, waited_on) = oneshot::channel();
        self.send(Batch {
            weight: weigh(&records),
            records,
            waiter: Some(waiter),
        })?;
        Ok(Durable(waited_on))
    }

    /// How many batches the store refused with nobody waiting to be told.
    ///
    /// A batch that is refused with a waiter attached reports to that
    /// waiter, and a disk that has stopped answering reaches every waiter
    /// through the next flush. What is left is the case with no waiter
    /// and no flush to carry it: a [`crate::Rejection`], which is a
    /// caller's mistake rather than the device's, raised against a batch
    /// nobody asked about. That has nowhere to go, so it is counted
    /// rather than dropped.
    ///
    /// This crate has no logging dependency and one call site is not a
    /// reason to add one; the broker integration owns deciding what to do
    /// when this is not zero.
    pub fn rejected_batches(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    /// Hand a batch to the thread, counting it into the backlog.
    ///
    /// Counted **before** the send and taken off again if the send
    /// fails, because the thread subtracts as soon as it has the batch
    /// and counting afterwards would let it subtract something that had
    /// not yet been added.
    fn send(&self, batch: Batch) -> Result<(), WriterStopped> {
        let (records, weight) = (batch.records.len() as u64, batch.weight);
        self.backlog.added(records, weight);
        let sent = self
            .batches
            .as_ref()
            .ok_or(WriterStopped)
            .and_then(|batches| batches.send(batch).map_err(|_| WriterStopped));
        if sent.is_err() {
            self.backlog.removed(records, weight);
        }
        sent
    }
}

/// What a batch holds, by [`Record::weight`].
///
/// Saturating, because a backlog that wrapped to nearly nothing would
/// pace nobody at exactly the moment it should pace everybody. No real
/// batch comes near it.
fn weigh(records: &[Record]) -> u64 {
    records.iter().fold(0u64, |total, record| {
        total.saturating_add(record.weight() as u64)
    })
}

impl Drop for Writer {
    /// Close the channel, then wait for the thread.
    ///
    /// Both halves matter. Closing is what asks for the final flush;
    /// joining is what makes the flush have happened by the time this
    /// returns. Without the join, a process that drops the writer and
    /// exits races its own durability.
    fn drop(&mut self) {
        drop(self.batches.take());
        if let Some(thread) = self.thread.take() {
            // A panicked thread has already reported itself, and a panic
            // in a drop is not an improvement on one.
            let _ = thread.join();
        }
    }
}

/// The thread's own state. Nothing else touches it.
struct Thread {
    /// Where records go.
    store: Arc<dyn Storage>,
    /// Where batches come from.
    batches: Receiver<Batch>,
    /// When to flush.
    config: WriterConfig,
    /// Waiters queued since the last flush, woken together by the next
    /// one.
    waiting: Vec<oneshot::Sender<Outcome>>,
    /// Records applied since the last flush was attempted.
    records_since_flush: u64,
    /// Whether anything applied has not been through a *successful*
    /// flush.
    ///
    /// Separate from the counter because a failed flush resets the
    /// counter — the attempt was made, and flushing on every batch
    /// afterwards would turn one bad flush into a permanently
    /// uncoalesced writer — but must not let the records it failed to
    /// cover be forgotten at shutdown.
    unflushed: bool,
    /// When the interval trigger next fires.
    next_flush: Instant,
    /// See [`Writer::rejected_batches`].
    rejected: Arc<AtomicU64>,
    /// What is still in the channel. Subtracted from as batches are
    /// taken out of it.
    backlog: Arc<Backlog>,
}

impl Drop for Thread {
    /// Wake anything waiting for a backlog that will never drain.
    ///
    /// In [`Drop`] rather than at the end of [`Thread::run`] so that it
    /// happens on the way out of a panic too. A thread that died holding
    /// the backlog over its mark would otherwise park every connection
    /// that asked for relief, permanently and silently.
    fn drop(&mut self) {
        self.backlog.stop();
    }
}

impl Thread {
    /// Take batches until the channel closes, then flush and stop.
    ///
    /// The shape is take-everything-available, then decide. Deciding
    /// per batch would flush once per batch under load, which is the
    /// uncoalesced rate; deciding after a drain is what turns a burst of
    /// connections all wanting an acknowledgement into one write.
    fn run(&mut self) {
        loop {
            let received = if self.outstanding() {
                self.batches
                    .recv_timeout(self.next_flush.saturating_duration_since(Instant::now()))
            } else {
                // Nothing is waiting to be made durable, so there is no
                // deadline to keep: block until there is work rather
                // than waking every interval to find none. An idle
                // broker's writer costs nothing, and an interval
                // configured as zero cannot spin the thread.
                let blocked = self
                    .batches
                    .recv()
                    .map_err(|_| RecvTimeoutError::Disconnected);
                // The interval runs from the moment something becomes
                // outstanding, not from a flush that may be long past.
                // Otherwise the first batch after an idle spell would
                // arrive already overdue and be flushed on its own.
                self.next_flush = Instant::now() + self.config.interval;
                blocked
            };

            let promised = match received {
                Ok(batch) => {
                    let mut promised = self.take(batch);
                    // Everything already queued goes in with it. Bounded
                    // by the count trigger on purpose: an unbounded drain
                    // against a producer faster than the device would
                    // never reach the flush at all, and every waiter
                    // caught in it would wait for as long as the load
                    // lasted.
                    while self.records_since_flush < self.config.records {
                        let Ok(batch) = self.batches.try_recv() else {
                            break;
                        };
                        promised |= self.take(batch);
                    }
                    promised
                }
                Err(RecvTimeoutError::Timeout) => false,
                // Every sender is gone and nothing is left in the
                // channel, so no batch can be missed by leaving here.
                Err(RecvTimeoutError::Disconnected) => break,
            };

            let due = Instant::now() >= self.next_flush;
            // The [`Self::outstanding`] guard is what stops a flush that
            // would cover nothing: a batch the store refused changed
            // nothing, and writing to the device to say so is a write
            // for no reason.
            if self.outstanding() && (promised || due || self.reached_the_count()) {
                self.flush();
            }
        }

        // Section 7.2's point, and the one a writer is most likely to
        // get wrong: exiting with records applied and not flushed loses
        // exactly what this crate exists to keep.
        if self.outstanding() {
            self.flush();
        }
    }

    /// Whether anything is waiting to be made durable.
    fn outstanding(&self) -> bool {
        self.unflushed || !self.waiting.is_empty()
    }

    /// Whether enough records have been applied to be worth a flush.
    fn reached_the_count(&self) -> bool {
        self.records_since_flush >= self.config.records
    }

    /// Apply one batch, and say whether it must not wait for a trigger.
    ///
    /// A refused batch is not counted and does not make anything
    /// outstanding, because [`Storage::apply`] is atomic and a refusal
    /// changed nothing. Its waiter is told immediately rather than at the
    /// next flush: the answer is already known, and holding it back would
    /// make one caller's mistake wait on another caller's write.
    fn take(&mut self, batch: Batch) -> bool {
        let (records, weight) = (batch.records.len() as u64, batch.weight);
        let promised = self.apply_one(batch);
        // Off the backlog when the records have been applied and
        // dropped, **not** when they left the channel. A thread parked
        // in a store that has stopped answering is still holding that
        // memory, and a producer released the moment its batch was
        // picked up would be released by nothing having happened.
        //
        // However it ended, including a refusal: a refused batch is
        // gone too, and records that stayed counted would pace every
        // producer in the process forever.
        self.backlog.removed(records, weight);
        self.backlog.relieve(self.config);
        promised
    }

    /// Apply one batch and answer its waiter if it failed, without
    /// touching the backlog. [`Thread::take`] is the whole of it.
    fn apply_one(&mut self, batch: Batch) -> bool {
        if !batch.records.is_empty() {
            if let Err(error) = self.store.apply(&batch.records) {
                match batch.waiter {
                    Some(waiter) => {
                        let _ = waiter.send(Err(Arc::new(error)));
                    }
                    None => {
                        self.rejected.fetch_add(1, Ordering::Relaxed);
                    }
                }
                return false;
            }
            self.records_since_flush += batch.records.len() as u64;
            self.unflushed = true;
        }
        match batch.waiter {
            Some(waiter) => {
                self.waiting.push(waiter);
                true
            }
            None => false,
        }
    }

    /// Flush, and tell every waiter queued since the last one what
    /// happened.
    ///
    /// The same result to all of them, including a failure. A waiter
    /// whose flush failed must learn that it failed: telling a publisher
    /// its message is safe when the device refused is the failure this
    /// whole milestone exists to prevent.
    fn flush(&mut self) {
        let outcome = match self.store.flush() {
            Ok(()) => {
                self.unflushed = false;
                Ok(())
            }
            // `unflushed` deliberately survives, so shutdown tries again
            // for the records this flush did not cover.
            Err(error) => Err(Arc::new(error)),
        };
        // Reset on the attempt rather than on success, so that one bad
        // flush does not leave the writer permanently uncoalesced.
        self.records_since_flush = 0;
        self.next_flush = Instant::now() + self.config.interval;

        for waiter in self.waiting.drain(..) {
            // A receiver that is gone is a connection that left before
            // its acknowledgement could be sent, which is not this
            // thread's problem.
            let _ = waiter.send(outcome.clone());
        }
    }
}

#[cfg(test)]
mod tests;
