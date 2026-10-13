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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::oneshot;

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

/// The lock, taken the way the rest of this crate takes one.
///
/// A poisoned lock is a panic that already happened somewhere else;
/// what is behind this one is a position and a map, and refusing to
/// read them because a different thread died would turn one fault into
/// a node that stops replicating.
fn locked<T>(what: &Mutex<T>) -> MutexGuard<'_, T> {
    what.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Every session this node holds a copy of, and every stream it owns.
///
/// One table rather than two, because a session moves between the two
/// halves — becoming a session's owner is exactly "stop following it
/// and start streaming it" — and two tables would be two places for
/// that to half-happen.
///
/// **Behind one lock, held only to read or move a number.** Nothing
/// here touches a disk or a socket: what comes out is what the caller
/// should write and send.
#[derive(Debug, Default)]
pub struct Copies {
    inner: Mutex<Tables>,
}

/// The two halves, under [`Copies`]'s lock.
#[derive(Debug, Default)]
struct Tables {
    /// Sessions this node follows, by the node that owns each.
    followed: HashMap<SessionKey, ([u8; 16], Follower)>,
    /// Sessions this node owns and streams, with how far it has sent.
    owned: HashMap<SessionKey, Owner>,
    /// Publishers waiting for a follower to confirm a position.
    ///
    /// Section 8.6's synchronous mode. Keyed by session, because that
    /// is what a mark is about; several publishers can wait on one
    /// session at once and each wants a different position.
    waiting: HashMap<SessionKey, Vec<(u64, oneshot::Sender<()>)>>,
    /// What each follower last said about a session this node owns.
    ///
    /// Keyed by session and follower, because one session has several
    /// followers and the lag is per link: a session whose nearest copy
    /// is caught up and whose furthest is a thousand behind is not
    /// described by either number alone.
    marks: HashMap<(SessionKey, [u8; 16]), u64>,
}

impl Copies {
    /// An empty table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Begin following `session`, owned by `owner`, from `through`.
    ///
    /// Called from a restore for every copy the store held, and when a
    /// stream for a session this node did not have arrives.
    pub fn follow(&self, session: SessionKey, owner: [u8; 16], through: u64) {
        locked(&self.inner)
            .followed
            .insert(session, (owner, Follower::resuming(through)));
    }

    /// Stop following `session`, whatever was known about it.
    ///
    /// Returns whether this node was following it, which is what the
    /// caller writes a `ReplicaDropped` for.
    pub fn unfollow(&self, session: &SessionKey) -> bool {
        locked(&self.inner).followed.remove(session).is_some()
    }

    /// Whether this node holds a copy of `session`.
    #[must_use]
    pub fn follows(&self, session: &SessionKey) -> bool {
        locked(&self.inner).followed.contains_key(session)
    }

    /// Offer an arriving change to the copy it is about.
    ///
    /// Returns the changes to apply, **in order**, with the position
    /// each is at — the one offered when it was next, plus whatever it
    /// released from behind a hole — and the mark to report. Empty when
    /// the change was a duplicate or is waiting for a hole to fill, and
    /// the mark is still returned in that case, because a follower that
    /// said nothing while it waited would look to the owner exactly
    /// like one that had stopped.
    ///
    /// `owner` is taken so that a stream arriving for a session this
    /// node has never heard of begins following it, which is what makes
    /// a new replica set start without a handshake of its own.
    pub fn arrived(
        &self,
        session: &SessionKey,
        owner: [u8; 16],
        at: u64,
        change: Change,
    ) -> (Vec<(u64, Change)>, u64) {
        let mut tables = locked(&self.inner);
        let (_, follower) = tables
            .followed
            .entry(session.clone())
            .or_insert_with(|| (owner, Follower::new()));
        let mut taken = Vec::new();
        if follower.offer(at, change.clone()) == Applied::Take {
            taken.push((at, change));
            taken.extend(follower.release());
        }
        (taken, follower.through())
    }

    /// How far a copy of `session` has got on this node.
    ///
    /// `None` when this node holds no copy of it, which is a different
    /// answer from zero: zero is a copy that has applied nothing.
    #[must_use]
    pub fn through(&self, session: &SessionKey) -> Option<u64> {
        locked(&self.inner)
            .followed
            .get(session)
            .map(|(_, follower)| follower.through())
    }

    /// Resume a session's stream from the position the store held.
    ///
    /// Called once per owned session at startup, from
    /// `storage::RestoredSession::stream_at`. **Not folded into
    /// [`Copies::send`] as a fallback**, because a fallback would be
    /// consulted on every change and would resume a session that had
    /// been streaming all along — the position it carries is only ever
    /// right the first time.
    pub fn resume(&self, session: SessionKey, at: u64) {
        locked(&self.inner)
            .owned
            .insert(session, Owner::resuming(at));
    }

    /// Number the next change for a session this node owns.
    pub fn send(&self, session: &SessionKey, change: Change) -> Streamed {
        let mut tables = locked(&self.inner);
        let owner = tables.owned.entry(session.clone()).or_default();
        owner.next(session, change)
    }

    /// Stop streaming `session`, because it has gone or moved.
    pub fn stop(&self, session: &SessionKey) {
        let mut tables = locked(&self.inner);
        tables.owned.remove(session);
        tables.marks.retain(|(held, _), _| held != session);
    }

    /// Record what a follower said about a session this node owns.
    pub fn marked(&self, session: SessionKey, follower: [u8; 16], through: u64) {
        let mut tables = locked(&self.inner);
        tables.marks.insert((session.clone(), follower), through);
        // **Any follower will do.** Section 8.6 says a synchronous
        // publish waits for *at least one* follower to confirm the
        // append; waiting for all of them would make the slowest copy
        // the publisher's latency and would stall every durable
        // publish the moment one node went quiet.
        let Some(waiting) = tables.waiting.get_mut(&session) else {
            return;
        };
        // What the retain removes are the senders whose position this
        // mark has reached, and dropping a sender is how its waiter
        // learns.
        waiting.retain(|(at, _)| *at > through);
        if waiting.is_empty() {
            tables.waiting.remove(&session);
        }
    }

    /// Wait for some follower of `session` to have applied through
    /// `at`, section 8.6's synchronous mode.
    ///
    /// [`Confirming::Already`] when one already has, which is the
    /// ordinary case under any load at all: the mark usually arrives
    /// while the store is still being waited on, so the publisher waits
    /// for nothing it was not already waiting for.
    ///
    /// **[`Confirming::Nobody`] when this node has no follower for the
    /// session** — a cluster of one, a replication factor of one, every
    /// follower down. That is an answer rather than a wait: section 8.6
    /// acknowledges anyway and counts it, and a publisher left waiting
    /// for a confirmation that cannot come is the outage the
    /// acknowledgement exists to avoid.
    pub fn confirming(&self, session: &SessionKey, at: u64) -> Confirming {
        let mut tables = locked(&self.inner);
        let mut followers = tables
            .marks
            .iter()
            .filter(|((held, _), _)| held == session)
            .map(|(_, through)| *through)
            .peekable();
        if followers.peek().is_none() {
            return Confirming::Nobody;
        }
        if followers.any(|through| through >= at) {
            return Confirming::Already;
        }
        let (tell, told) = oneshot::channel();
        tables
            .waiting
            .entry(session.clone())
            .or_default()
            .push((at, tell));
        Confirming::Waiting(told)
    }

    /// The furthest any follower of `session` is behind this node.
    ///
    /// **The furthest rather than the nearest**, because the lag is a
    /// statement about what a sudden loss would cost: section 8.6 makes
    /// it the loss window in asynchronous mode, and a window measured
    /// from the copy that happens to be closest describes a failure
    /// that did not happen. A session with no follower that has ever
    /// reported is `None` — no copy is not a long lag, it is a
    /// different thing, and a zero there would read as perfect health.
    #[must_use]
    pub fn lag(&self, session: &SessionKey) -> Option<u64> {
        let tables = locked(&self.inner);
        let sent = tables.owned.get(session)?.at();
        tables
            .marks
            .iter()
            .filter(|((held, _), _)| held == session)
            .map(|(_, through)| sent.saturating_sub(*through))
            .max()
    }

    /// The furthest any follower of any owned session is behind.
    ///
    /// What the exposition carries: one number for the node, because a
    /// gauge per session at the design's ceilings is a hundred thousand
    /// series.
    #[must_use]
    pub fn worst_lag(&self) -> u64 {
        let tables = locked(&self.inner);
        tables
            .marks
            .iter()
            .filter_map(|((session, _), through)| {
                Some(tables.owned.get(session)?.at().saturating_sub(*through))
            })
            .max()
            .unwrap_or(0)
    }

    /// How many sessions this node holds a copy of.
    #[must_use]
    pub fn following(&self) -> usize {
        locked(&self.inner).followed.len()
    }

    /// How many sessions this node streams to somebody.
    #[must_use]
    pub fn streaming(&self) -> usize {
        locked(&self.inner).owned.len()
    }
}

/// When a durable publish is acknowledged. Section 8.6.
///
/// Configurable globally and per listener, because the trade is a
/// deployment's: a listener carrying commands that must not be lost
/// can be synchronous while one carrying telemetry is not.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Durability {
    /// Acknowledge once this node has it durably. The default.
    ///
    /// Replication proceeds behind the acknowledgement, and the loss
    /// window is the replication lag — which is why the lag is a
    /// metric and why section 8.6 says it is alerted on.
    #[default]
    Async,
    /// Acknowledge once a follower has confirmed the append.
    ///
    /// A single node failure then loses nothing. The cost is one
    /// local network round trip per durable publish, and it is one
    /// round trip rather than one per follower: section 8.6 says at
    /// least one confirms.
    Sync,
}

impl Durability {
    /// Whether a publish waits for a copy before it is acknowledged.
    #[must_use]
    pub fn waits(self) -> bool {
        matches!(self, Self::Sync)
    }
}

/// Whether a follower has confirmed a position, or when it will.
#[derive(Debug)]
pub enum Confirming {
    /// One already had, before anybody asked.
    Already,
    /// This node has no follower for that session to ask.
    ///
    /// A cluster of one, a replication factor of one, or every
    /// follower down. Section 8.6 acknowledges anyway and counts it,
    /// so this is an answer rather than a wait.
    Nobody,
    /// Somebody is being waited for.
    Waiting(oneshot::Receiver<()>),
}

/// What a synchronous publish found out about its copies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confirmed {
    /// A follower holds it. The guarantee is kept.
    Yes,
    /// Nobody holds it, and nobody was going to.
    ///
    /// Counted and surfaced rather than silent: an operator who asked
    /// for synchronous mode and is getting this is not getting what
    /// they asked for, and section 8.6 says they must be able to see
    /// that.
    No,
}

impl Confirming {
    /// Wait for all of `these`, up to `patience` between them.
    ///
    /// **All of them, because one publish can queue for several
    /// sessions**: a message matching two offline subscribers appends
    /// to two queues in one batch, and a publisher told its message
    /// was replicated when only one of the two copies exists has been
    /// told something that is not true. Any one of them failing is a
    /// [`Confirmed::No`] for the batch.
    ///
    /// The patience covers the set rather than each, so a batch
    /// touching ten sessions does not wait ten times as long as one
    /// touching one.
    pub async fn all(these: Vec<Self>, patience: Duration) -> Confirmed {
        if these.is_empty() {
            return Confirmed::Yes;
        }
        let waiting = async {
            for one in these {
                if one.within(Duration::MAX).await == Confirmed::No {
                    return Confirmed::No;
                }
            }
            Confirmed::Yes
        };
        (tokio::time::timeout(patience, waiting).await).unwrap_or(Confirmed::No)
    }

    /// Wait, up to `patience`.
    ///
    /// **Bounded, and a timeout answers [`Confirmed::No`] rather than
    /// holding the publisher.** A follower that has gone quiet is the
    /// case synchronous mode exists for, and it is also the case where
    /// waiting for ever turns one slow node into every publisher's
    /// outage — which is the trade section 8.5 already made the other
    /// way round.
    pub async fn within(self, patience: Duration) -> Confirmed {
        match self {
            Self::Already => Confirmed::Yes,
            Self::Nobody => Confirmed::No,
            Self::Waiting(told) => {
                match tokio::time::timeout(patience, told).await {
                    // The sender was dropped by `marked`, which is how
                    // it says the position was reached.
                    Ok(_) => Confirmed::Yes,
                    Err(_) => Confirmed::No,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
