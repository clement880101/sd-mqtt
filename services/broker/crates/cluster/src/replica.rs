//! What a follower holds, and how far through it is. Section 8.6.
//!
//! The owner of a session sends every change to its queue — an append,
//! an acknowledgement or a removal — to the nodes [`crate::placement`]
//! put behind it. A follower applies them in order and reports how far
//! it has got, which is the number [`crate::migrate`]'s failover reads
//! to choose between two copies.
//!
//! # Two numbers, and conflating them is the bug
//!
//! Every change carries both and they count different things.
//!
//! [`Streamed::at`] is the **stream position**: one per operation on
//! this session, from one, never reused and never skipped. It is what
//! "in order" is about and what the high-water mark counts.
//!
//! `sequence` is the **queue entry** the operation is about. Appends
//! hand out new ones and removals name old ones, so it does not
//! increase from one operation to the next and cannot order a stream. A
//! follower that used it as the stream position would treat every
//! removal as a message from the past and either apply it twice or
//! refuse it.
//!
//! # A gap is refused, not skipped
//!
//! The link is ordered, so most streams have no gaps. But
//! [`crate::link`] **drops rather than blocking** when a peer's queue
//! is full — that is what keeps one wedged follower from stalling the
//! node serving clients — so a hole is a thing that happens under load,
//! which is exactly when it matters.
//!
//! A follower that applied the operation after a hole would hold a
//! queue missing a message in the middle and report a mark saying it
//! held everything, and failover would then *prefer* it. So the mark is
//! the highest **contiguous** position applied, which is the same rule
//! and the same reason as [`crate::oplog`]'s `published_through`: a
//! reader served past a hole sets its watermark past something it never
//! received.
//!
//! # This module holds no lock and touches no disk
//!
//! A [`Follower`] is a decision about what to do with an operation.
//! Applying it to a store and telling a peer about the mark are the
//! caller's, for the reason the rest of this crate separates them:
//! everything here has to be testable without a transport or a disk.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use storage::record::{BodyRecord, QueueEntry};
use storage::SessionKey;

/// One change to a session's queue, on its way to a follower.
///
/// **Not called an operation**, although section 8.6 calls it one:
/// `policy::Operation` is already what a *client* asked to do, and two
/// unrelated things sharing a name in one crate is how a reader ends
/// up matching on the wrong one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Streamed {
    /// Which session's queue.
    pub session: SessionKey,
    /// Where this sits in the stream for that session, from one.
    pub at: u64,
    /// What happened.
    pub change: Change,
}

/// The change itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Change {
    /// A message joined the queue.
    ///
    /// The body travels **only when the follower may not have it**, and
    /// that is the owner's judgement rather than this type's: a body is
    /// stored once however many entries reference it, so a session
    /// receiving its second copy of one message needs the entry and not
    /// the payload again. `None` is the owner saying it has already
    /// sent this body; a follower that does not have it says so through
    /// its mark rather than guessing.
    Appended {
        /// The entry, including the sequence number it was given.
        entry: Box<QueueEntry>,
        /// The payload, when the follower may not hold it yet.
        body: Option<Box<BodyRecord>>,
    },
    /// The client acknowledged a delivery, so the entry is finished.
    Acknowledged {
        /// Which queue entry.
        sequence: u64,
    },
    /// The entry left the queue for a reason that is not an
    /// acknowledgement: it expired, or the queue was full.
    Removed {
        /// Which queue entry.
        sequence: u64,
    },
}

/// What a follower decided to do with an operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// Apply it: it is the next one.
    ///
    /// The caller writes it to the store and the mark has already
    /// moved.
    Take,
    /// Already applied, and this is a copy. Nothing to do.
    ///
    /// Not an error: an owner that re-sends from a follower's mark
    /// re-sends the mark itself when the two cross, and a resend a
    /// follower refused rather than ignored would stall the stream.
    Duplicate,
    /// Not the next one, and the hole before it has not been filled.
    ///
    /// Held, not applied, and the mark does not move. The owner
    /// notices the mark lagging and sends from there.
    Held,
}

/// How far one session's copy has got on this node.
///
/// One per session this node follows, held by whatever owns the
/// follower side of the stream.
#[derive(Debug, Default)]
pub struct Follower {
    /// The highest contiguous position applied. Zero before anything.
    through: u64,
    /// Positions that arrived early, waiting for the hole to fill.
    ///
    /// A map rather than a queue because they arrive out of order by
    /// definition. Bounded by [`Follower::HELD_MAX`]: a follower that
    /// kept every early operation while a hole never filled would hold
    /// an unbounded amount of another node's queue in memory, which is
    /// the failure this is supposed to prevent rather than cause.
    held: BTreeMap<u64, Change>,
    /// Operations dropped because the holding area was full.
    ///
    /// Counted rather than silent: it means this follower is behind by
    /// more than it can bridge, and the repair is the owner re-sending
    /// from the mark.
    overflowed: u64,
}

impl Follower {
    /// How many early operations one session may hold.
    ///
    /// Enough to bridge a burst that arrived out of order, and far
    /// short of a queue: the answer to a long hole is the owner
    /// re-sending from the mark, not this growing until the node runs
    /// out of memory.
    pub const HELD_MAX: usize = 256;

    /// A follower that has applied nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A follower resuming from a stored mark.
    #[must_use]
    pub fn resuming(through: u64) -> Self {
        Self {
            through,
            ..Self::default()
        }
    }

    /// The highest contiguous position applied.
    ///
    /// **What failover compares**, and what the owner is told. See the
    /// module documentation for why it is contiguous rather than the
    /// highest seen.
    #[must_use]
    pub fn through(&self) -> u64 {
        self.through
    }

    /// Operations dropped for want of room to hold them.
    #[must_use]
    pub fn overflowed(&self) -> u64 {
        self.overflowed
    }

    /// Decide what to do with `at`, and move the mark if it is next.
    ///
    /// Returns [`Applied::Take`] when the caller should apply it. A
    /// position that fills a hole also releases whatever was waiting
    /// behind it, which is what [`Follower::release`] hands back.
    pub fn offer(&mut self, at: u64, change: Change) -> Applied {
        if at <= self.through {
            return Applied::Duplicate;
        }
        if at == self.through + 1 {
            self.through = at;
            return Applied::Take;
        }
        if self.held.len() >= Self::HELD_MAX {
            self.overflowed += 1;
            return Applied::Held;
        }
        self.held.insert(at, change);
        Applied::Held
    }

    /// Whatever was waiting behind the hole that has just filled, in
    /// order, with the mark moved over them.
    ///
    /// Called after an [`Applied::Take`]. Empty when nothing was
    /// waiting, which is the ordinary case.
    pub fn release(&mut self) -> Vec<(u64, Change)> {
        let mut released = Vec::new();
        while let Some(change) = self.held.remove(&(self.through + 1)) {
            self.through += 1;
            released.push((self.through, change));
        }
        released
    }

    /// How far behind `owner` this copy is.
    ///
    /// **The loss window in asynchronous mode**, which section 8.6 says
    /// is exposed and alerted on: what the owner has acknowledged and
    /// this copy does not hold is what a sudden loss of the owner would
    /// lose. Zero when the copy is caught up, and never negative — an
    /// owner behind its own follower is not a thing that can be true,
    /// and saturating rather than wrapping means a momentary
    /// disagreement reads as nothing rather than as eighteen quintillion.
    #[must_use]
    pub fn lag(&self, owner: u64) -> u64 {
        owner.saturating_sub(self.through)
    }
}

/// The owner's side: what to send, and where it has got to.
///
/// Turns the records a batch wrote into the changes a follower needs,
/// numbering them. One per owned session, resumed from
/// [`storage::RestoredSession::stream_at`] across a restart — see
/// `Record::StreamAt` for why that number is stored rather than
/// counted from zero.
///
/// # The records are the source, not a second account of them
///
/// The broker already journals exactly what changed, in order, in one
/// batch. Deriving the stream from that rather than from a second set
/// of calls at the publish path is what keeps the copy and the
/// original from disagreeing: there is one description of what
/// happened and both the store and the follower get it.
#[derive(Debug, Default)]
pub struct Owner {
    /// The position of the last change sent.
    at: u64,
}

impl Owner {
    /// An owner that has sent nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An owner resuming from a stored position.
    #[must_use]
    pub fn resuming(at: u64) -> Self {
        Self { at }
    }

    /// The position of the last change sent.
    #[must_use]
    pub fn at(&self) -> u64 {
        self.at
    }

    /// Number `change` and hand it back ready to send.
    pub fn next(&mut self, session: &SessionKey, change: Change) -> Streamed {
        self.at += 1;
        Streamed {
            session: session.clone(),
            at: self.at,
            change,
        }
    }
}

/// What one record means to a follower, if anything.
///
/// `None` for every record that is not a change to a queue, which is
/// most of them: a subscription, a retained message and a rule are
/// replicated by section 8.2's registers and have nothing to do with
/// this stream.
///
/// **A pop is a removal and not an acknowledgement.** The store has one
/// record for both, because to a queue they are the same event, and
/// this cannot invent the difference: what it has is the record. The
/// distinction exists in [`Change`] because section 8.6 names both, and
/// it is the publish path that knows which — a caller that has that
/// knowledge uses [`Owner::next`] directly.
#[must_use]
pub fn change_for(record: &storage::Record) -> Option<(SessionKey, Change)> {
    match record {
        storage::Record::QueuePushed { session, entry } => Some((
            session.clone(),
            Change::Appended {
                entry: Box::new(entry.clone()),
                // The body travels separately: it is in the same batch
                // as its own record, and whether this follower already
                // holds it is the sender's question, not this one's.
                body: None,
            },
        )),
        storage::Record::QueuePopped { session, sequence } => Some((
            session.clone(),
            Change::Removed {
                sequence: *sequence,
            },
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
