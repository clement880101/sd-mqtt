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
//! for. When it stalls, the channel in front of it grows and the
//! connections that asked for durability wait, which is the correct
//! behaviour: they were promised nothing yet.
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

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
}

impl Default for WriterConfig {
    /// Two milliseconds and a thousand records.
    ///
    /// The thousand is where the measurement was taken: coalesced a
    /// thousand at a time, the durable rate is over 300,000 a second. The
    /// two milliseconds is the low-millisecond interval section 7.2 asks
    /// for, and it only ever applies to writes nobody is waiting on.
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(2),
            records: 1_000,
        }
    }
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
    /// Unbounded, deliberately. A bounded channel would have to either
    /// block the sender — parking an async connection task on a disk,
    /// which is the thing this module exists to avoid — or drop records,
    /// which loses exactly what the crate is for. The back pressure that
    /// matters is the receive maximum the broker already enforces per
    /// connection: a publisher cannot have more unacknowledged messages
    /// in flight than that, and an acknowledgement is what waits here.
    batches: Option<Sender<Batch>>,
    /// The thread, joined on drop. `None` only inside [`Drop`].
    thread: Option<JoinHandle<()>>,
    /// See [`Writer::rejected_batches`].
    rejected: Arc<AtomicU64>,
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
        let thread = std::thread::Builder::new()
            .name("sd-mqtt-storage".to_owned())
            .spawn({
                let rejected = Arc::clone(&rejected);
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
                    }
                    .run();
                }
            })?;
        Ok(Self {
            batches: Some(sender),
            thread: Some(thread),
            rejected,
        })
    }

    /// Apply a batch, with nobody waiting on it.
    ///
    /// Section 7.3: at-most-once publications are journalled where they
    /// touch durable state and are never waited on, because nothing was
    /// promised. This is that path. The records are durable by the next
    /// flush, whichever trigger causes it, and by shutdown at the latest.
    ///
    /// # Errors
    ///
    /// [`WriterStopped`] if the thread is gone, in which case the records
    /// were not applied.
    pub fn apply(&self, records: Vec<Record>) -> Result<(), WriterStopped> {
        self.send(Batch {
            records,
            waiter: None,
        })
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
    /// # Errors
    ///
    /// [`WriterStopped`] if the thread is gone, in which case the records
    /// were not applied and nothing will ever be durable.
    pub fn apply_durable(&self, records: Vec<Record>) -> Result<Durable, WriterStopped> {
        let (waiter, waited_on) = oneshot::channel();
        self.send(Batch {
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

    /// Hand a batch to the thread.
    fn send(&self, batch: Batch) -> Result<(), WriterStopped> {
        self.batches
            .as_ref()
            .ok_or(WriterStopped)?
            .send(batch)
            .map_err(|_| WriterStopped)
    }
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
