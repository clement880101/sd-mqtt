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
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use serde::{Deserialize, Serialize};
use storage::record::{BodyRecord, QueueEntry, SessionRecord};
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
        /// The session itself, on the first append the owner streams.
        ///
        /// **Without it the follower cannot store anything.** A store
        /// refuses a queue record for a session it does not hold —
        /// `Rejection::NoSuchSession`, which `crates/storage`'s
        /// contract tests pin — so a copy whose first record is a
        /// queue entry is a copy the store throws away, leaving the
        /// follower's table saying it holds something and the disk
        /// holding nothing. That is exactly what this did before the
        /// field existed, and nothing caught it: the gauge counts the
        /// table.
        ///
        /// Sent once per session per owner rather than with every
        /// append, because it is the same record every time and a
        /// queue of ten thousand would otherwise carry ten thousand
        /// copies of it.
        session: Option<Box<SessionRecord>>,
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
    /// The whole queue, for a follower that holds none of it. Section
    /// 8.6's repair.
    ///
    /// **Does not consume a stream position, and that is the design.**
    /// A follower rendezvous has just named for a session — because a
    /// node is gone, or was forgotten — starts with a mark of zero,
    /// while the owner's stream is at some position `M` that every
    /// other follower has reached. Re-sending the queue as ordinary
    /// appends at `M+1..` would make the existing followers' marks fall
    /// behind, or make them store entries they already hold; sending
    /// it as appends at `1..` would leave the new follower with a hole
    /// from there to `M` that no later change ever fills. So the
    /// snapshot carries the owner's current position, the follower
    /// takes it whole — replacing whatever copy it had — and sets its
    /// mark to that position. The next live change, at `M+1`, is then
    /// the next change for everybody.
    Snapshot {
        /// The session itself, because the follower may hold nothing.
        session: Box<SessionRecord>,
        /// Every entry in the queue, with its payload.
        entries: Vec<(QueueEntry, BodyRecord)>,
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
    /// The position a snapshot set this copy at, when one did.
    ///
    /// A frame at or below it is one the snapshot already holds. Frames
    /// are sent once the store has taken their batch and a repair reads
    /// the store, so a snapshot can reach the follower ahead of a frame
    /// of its own, and that frame is a duplicate rather than a stream
    /// starting over.
    snapshot_at: Option<u64>,
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

    /// A follower a snapshot has just set at `at`.
    #[must_use]
    pub fn from_snapshot(at: u64) -> Self {
        Self {
            through: at,
            snapshot_at: Some(at),
            ..Self::default()
        }
    }

    /// Whether a frame at `at` is one the snapshot that set this copy
    /// already holds.
    #[must_use]
    pub fn covered(&self, at: u64) -> bool {
        self.snapshot_at.is_some_and(|snapshot| at <= snapshot)
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
                // The same: whether this is the first append to this
                // session is what the owner's table knows and one
                // record does not. See `Change::Appended::session`.
                session: None,
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
    /// Changes this node has applied as a follower, ever.
    ///
    /// A counter rather than a mark, and for a reader outside the
    /// node: a follower's marks are per session and are not in the
    /// exposition, so without this nothing an operator — or a test —
    /// can read says whether a follower is still applying anything. A
    /// flat line here on a node that is meant to be following is a
    /// follower that has stopped.
    applied: std::sync::atomic::AtomicU64,
    /// Sessions this node owns whose replica set is short, as of the
    /// last sweep. Section 8.6's "progress visible".
    repairs_pending: std::sync::atomic::AtomicU64,
    /// Sessions this node has re-streamed to a new follower, ever.
    repairs_done: std::sync::atomic::AtomicU64,
    /// Who a session this node owns is streamed to, if this node knows.
    ///
    /// Installed by the node from its placement; see
    /// [`Copies::install_followers`]. Without it, the followers of a
    /// session are the ones that have reported a mark, which is what a
    /// unit test has and what a node that does not cluster has.
    followers: std::sync::OnceLock<Followers>,
}

/// How a node answers who follows a session. See
/// [`Copies::install_followers`].
struct Followers(Box<Placer>);

/// What answers who follows a session.
type Placer = dyn Fn(&SessionKey) -> Vec<Placed> + Send + Sync;

/// One follower placement names for a session, and whether it can be
/// reached right now. See [`Copies::install_followers`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placed {
    /// The follower.
    pub node: [u8; 16],
    /// Whether this node can reach it: alive or suspect, not dead.
    pub reachable: bool,
}

impl std::fmt::Debug for Followers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Followers(..)")
    }
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
    /// Followers seen behind this node's position, with the mark they
    /// were at and when that was first seen. See [`Copies::stalled`].
    behind: HashMap<(SessionKey, [u8; 16]), (u64, Instant)>,
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
    ///
    /// # A copy is of one stream, and a stream has one owner
    ///
    /// A mark of `None` is this node holding no copy of the stream
    /// that `change` belongs to, and the caller says nothing to the
    /// owner. Two things mean that. **A change from a node other than
    /// the recorded owner**: section 8.4 moved the session, and the
    /// new owner streams it from position one, so a copy kept from the
    /// old owner would read position one as a duplicate of its own
    /// mark, apply nothing, and answer a mark the new owner would take
    /// as a copy of *its* stream — which is a synchronous publish
    /// acknowledged against a copy frozen at the pre-move queue. **A
    /// change at position one for a session already followed**: a
    /// position is never reused, so one is only ever the start of a
    /// stream, and a stream starting over under the same owner is a
    /// session that ended and came back under its identifier. Position
    /// one is what tells the two apart: it is the new stream beginning,
    /// so the copy held is of one that is over and it goes; a change
    /// from a non-owner above one is a frame of the old stream arriving
    /// late over its own link, and the copy stands. In both the owner
    /// of the new stream hears nothing from this node, sees a follower
    /// with no mark, and sends the snapshot that starts it over — the
    /// same repair as for a follower placed after the queue was
    /// streamed. A snapshot from any node is taken as it always was: it
    /// carries the owner and the position both. And a session this node
    /// does not follow is begun only at position one or by a snapshot,
    /// never partway, for the same reason: see the body.
    pub fn arrived(
        &self,
        session: &SessionKey,
        owner: [u8; 16],
        at: u64,
        change: Change,
    ) -> (Vec<(u64, Change)>, Option<u64>) {
        let mut tables = locked(&self.inner);
        let mut taken = Vec::new();
        let stale_snapshot = matches!(change, Change::Snapshot { .. })
            && tables
                .followed
                .get(session)
                .is_some_and(|(held_by, follower)| *held_by == owner && follower.through() >= at);
        if stale_snapshot {
            // **A snapshot no newer than the copy, from the copy's own
            // owner, is not taken.** Repair can cross a mark on the
            // wire: the owner saw none yet and sent the session whole,
            // cut where its store was, while this copy applied the
            // change the mark was for. Taking it replaces a copy that
            // holds the newest message with one that lacks it; the mark
            // this copy has is the answer, and nothing is applied.
        } else if matches!(change, Change::Snapshot { .. }) {
            // A snapshot replaces the copy and sets the mark; see
            // `Change::Snapshot`. Whatever was held — nothing, or a
            // stale copy from before this node left the set — goes
            // with it, and anything waiting behind a hole with it.
            tables
                .followed
                .insert(session.clone(), (owner, Follower::from_snapshot(at)));
            taken.push((at, change));
        } else {
            if let Some((held_by, follower)) = tables.followed.get(session) {
                // See the method's documentation: a stream beginning
                // ends the copy held, and a stale frame of another
                // stream is ignored; nothing is said about either.
                //
                // **Except this stream's own beginning.** A snapshot is
                // stamped with the position its store's copy reflects,
                // which can be zero for a session whose first change is
                // still on its way; that change, at one and from the
                // same owner, is this copy's next one.
                let first_after_an_empty_copy = *held_by == owner && follower.through() == 0;
                // **And a frame the snapshot holds is a duplicate**, so it
                // is answered with the mark and nothing is applied; see
                // `Follower::covered`.
                if *held_by == owner && follower.covered(at) && follower.through() >= at {
                    let through = follower.through();
                    return (taken, Some(through));
                }
                if at == 1 && !first_after_an_empty_copy {
                    tables.followed.remove(session);
                    return (taken, None);
                }
                if *held_by != owner {
                    return (taken, None);
                }
            } else if at != 1 {
                // **A stream this node did not see begin is not picked up
                // partway.** Following it from here would hold a hole from
                // one to `at` that no later change fills, and the mark of
                // zero it answered with would tell the owner a copy exists
                // — which is exactly what stops `repairing` sending the
                // snapshot, because repair looks for a follower with no
                // mark. Silence is that follower. Reached by a copy dropped
                // above for a new stream, and by a node placement has just
                // named for a session already streaming.
                return (taken, None);
            }
            let (_, follower) = tables
                .followed
                .entry(session.clone())
                .or_insert_with(|| (owner, Follower::new()));
            if follower.offer(at, change.clone()) == Applied::Take {
                taken.push((at, change));
                taken.extend(follower.release());
            }
        }
        let through = tables
            .followed
            .get(session)
            .map(|(_, follower)| follower.through());
        self.applied
            .fetch_add(taken.len() as u64, std::sync::atomic::Ordering::Relaxed);
        (taken, through)
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

    /// Where a session's stream is now, for a snapshot to carry.
    ///
    /// Zero for a session this node has not streamed, which is a
    /// snapshot that says "you are at the beginning", and is right.
    #[must_use]
    pub fn position(&self, session: &SessionKey) -> u64 {
        locked(&self.inner).owned.get(session).map_or(0, Owner::at)
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

    /// Whether this node has streamed anything for `session` yet.
    ///
    /// What decides whether an append carries the session record with
    /// it; see `Change::Appended::session`.
    #[must_use]
    pub fn streams(&self, session: &SessionKey) -> bool {
        locked(&self.inner).owned.contains_key(session)
    }

    /// Number the next change for a session this node owns.
    pub fn send(&self, session: &SessionKey, change: Change) -> Streamed {
        let mut tables = locked(&self.inner);
        let owner = tables.owned.entry(session.clone()).or_default();
        owner.next(session, change)
    }

    /// Stop streaming `session`, because it has gone or moved.
    ///
    /// **Its waiters go too, and are told no.** A synchronous publish
    /// waiting on a session this node no longer streams is waiting for
    /// a mark nobody will send; left in the table it waited out its
    /// patience and stayed there after. Dropping its sender is the no:
    /// see [`Confirming::within`].
    pub fn stop(&self, session: &SessionKey) {
        let mut tables = locked(&self.inner);
        tables.owned.remove(session);
        tables.marks.retain(|(held, _), _| held != session);
        tables.behind.retain(|(held, _), _| held != session);
        tables.waiting.remove(session);
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
        // **Told yes by a send, not by a drop.** A drop is how a waiter
        // learns the session stopped being streamed, which is a no; see
        // `Copies::stop`. A waiter that has already given up has closed
        // its end, and the send to it is lost harmlessly.
        let (reached, still): (Vec<_>, Vec<_>) = std::mem::take(waiting)
            .into_iter()
            .partition(|(at, _)| *at <= through);
        *waiting = still;
        for (_, tell) in reached {
            let _ = tell.send(());
        }
        if waiting.is_empty() {
            tables.waiting.remove(&session);
        }
    }

    /// Whether `follower` has sat behind this node's position for
    /// `session` for the whole of `grace` without its mark moving.
    ///
    /// **Section 8.6's other repair.** A follower that missed one change
    /// reports a mark below the hole for ever: the link is ordered, the
    /// owner never re-sends, and [`Follower::HELD_MAX`] bounds what it
    /// can bridge. It has a mark, so the repair that looks for a
    /// follower with none passed it over, its copy stayed short, and a
    /// synchronous publish waiting on it waited out its patience every
    /// time. A stalled follower is sent the snapshot a new one is.
    ///
    /// Measured from when it was **first seen behind at that mark**,
    /// not from when it last moved: a session idle for an hour whose
    /// next append has not been answered yet is behind by one and
    /// moving, not an hour stalled. Caught up, or moved, clears it. A
    /// follower with no mark is never stalled; it is the other repair.
    ///
    /// `now` is injected, the way the clocks in this crate are, so that
    /// a grace is an argument rather than a wait.
    pub fn stalled(
        &self,
        session: &SessionKey,
        follower: [u8; 16],
        now: Instant,
        grace: Duration,
    ) -> bool {
        let mut tables = locked(&self.inner);
        let key = (session.clone(), follower);
        let Some(&mark) = tables.marks.get(&key) else {
            tables.behind.remove(&key);
            return false;
        };
        let position = tables.owned.get(session).map_or(0, Owner::at);
        if mark >= position {
            tables.behind.remove(&key);
            return false;
        }
        match tables.behind.get(&key) {
            Some(&(seen, since)) if seen == mark => now.saturating_duration_since(since) >= grace,
            _ => {
                tables.behind.insert(key, (mark, now));
                false
            }
        }
    }

    /// How many synchronous publishes are waiting on a follower, across
    /// every session.
    #[must_use]
    pub fn waiters(&self) -> usize {
        locked(&self.inner).waiting.values().map(Vec::len).sum()
    }

    /// Tell this table who a session it owns is streamed to.
    ///
    /// `followers` answers, for a session, the nodes this node sends
    /// that session's changes to — placement's choice, less this node
    /// — and whether each can be reached. It decides two things.
    ///
    /// **Whom a synchronous publish waits for.** Before this was
    /// installed, "has a follower" meant "a follower has reported a
    /// mark", and a follower that had been sent the first append and
    /// not yet answered looked like no follower at all — so the first
    /// few publishes to every new session were acknowledged
    /// unconfirmed in synchronous mode, and counted as such, on a
    /// cluster with nothing wrong with it. Three of eight, in the test
    /// that found it. A placed follower that can be reached is
    /// somebody to wait for; one that is dead is not.
    ///
    /// **Whose marks count toward the lag.** A follower placement no
    /// longer names — gone past the grace, or forgotten — leaves its
    /// last mark in the table, and the owner keeps appending past it.
    /// Without this the lag grew without bound after every node an
    /// operator removed, which was found by removing one and watching
    /// the page. Dead *within* the grace still counts: that copy is
    /// still in the set, and how far behind it is is exactly what the
    /// loss window is about.
    ///
    /// Once, like the rest of a node's wiring.
    pub fn install_followers(
        &self,
        followers: impl Fn(&SessionKey) -> Vec<Placed> + Send + Sync + 'static,
    ) {
        let _ = self.followers.set(Followers(Box::new(followers)));
    }

    /// Who placement names for `session`, if this node has said.
    ///
    /// `None` when nothing is installed, and then every mark counts
    /// and any mark is somebody: what a unit test has, and a node
    /// that does not cluster.
    fn placed(&self, session: &SessionKey) -> Option<Vec<Placed>> {
        self.followers.get().map(|followers| (followers.0)(session))
    }

    /// Whether `follower`'s marks for `session` count: it is placed,
    /// or nothing has been installed to say.
    fn counts(placed: Option<&[Placed]>, follower: [u8; 16]) -> bool {
        placed.is_none_or(|placed| placed.iter().any(|one| one.node == follower))
    }

    /// Whether `follower` is somebody a publish can wait for: placed
    /// and reachable, or nothing has been installed to say.
    ///
    /// A dead follower's last mark is a position it will not move
    /// past, and a publisher waiting on it waits for the patience and
    /// is then told no — which is the same answer as [`Confirming::Nobody`],
    /// a round trip later.
    fn answers(placed: Option<&[Placed]>, follower: [u8; 16]) -> bool {
        placed.is_none_or(|placed| {
            placed
                .iter()
                .any(|one| one.node == follower && one.reachable)
        })
    }

    /// Drop every mark `follower` has reported, because it is gone.
    ///
    /// Called when a node is forgotten. The table does not need it to
    /// read right — [`Copies::install_followers`] already leaves an
    /// unplaced follower out — but a mark for a node that will never
    /// report again is a row that would otherwise stay for the life
    /// of the process.
    pub fn forgot(&self, follower: [u8; 16]) {
        let mut tables = locked(&self.inner);
        tables.marks.retain(|(_, who), _| *who != follower);
        tables.behind.retain(|(_, who), _| *who != follower);
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
    /// acknowledgement exists to avoid. A follower is one placement
    /// names and can be reached, when the node has said so through
    /// [`Copies::install_followers`], **or** one that has reported a
    /// mark: a follower that has been sent the stream and has not yet
    /// answered is somebody, not nobody.
    pub fn confirming(&self, session: &SessionKey, at: u64) -> Confirming {
        let placed = self.placed(session);
        let somebody_placed = placed
            .as_deref()
            .is_some_and(|placed| placed.iter().any(|one| one.reachable));
        let mut tables = locked(&self.inner);
        let mut reported = tables
            .marks
            .iter()
            .filter(|((held, who), _)| held == session && Self::answers(placed.as_deref(), *who))
            .map(|(_, through)| *through)
            .peekable();
        if !somebody_placed && reported.peek().is_none() {
            return Confirming::Nobody;
        }
        if reported.any(|through| through >= at) {
            return Confirming::Already;
        }
        let (tell, told) = oneshot::channel();
        let waiting = tables.waiting.entry(session.clone()).or_default();
        // A waiter that timed out has closed its end and is waiting for
        // nothing; kept, the table grew by one for every publish that
        // ever gave up on a follower that did not answer.
        waiting.retain(|(_, tell)| !tell.is_closed());
        waiting.push((at, tell));
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
        let placed = self.placed(session);
        let tables = locked(&self.inner);
        let sent = tables.owned.get(session)?.at();
        tables
            .marks
            .iter()
            .filter(|((held, who), _)| held == session && Self::counts(placed.as_deref(), *who))
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
        // Placement asked per session, outside the lock, because the
        // answer is computed from the membership table and that is
        // not this table's lock to hold while asking.
        let placed: HashMap<SessionKey, Vec<Placed>> = self
            .owned_sessions()
            .into_iter()
            .filter_map(|session| Some((session.clone(), self.placed(&session)?)))
            .collect();
        let installed = self.followers.get().is_some();
        let tables = locked(&self.inner);
        tables
            .marks
            .iter()
            .filter(|((session, who), _)| {
                !installed || Self::counts(placed.get(session).map(Vec::as_slice), *who)
            })
            .filter_map(|((session, _), through)| {
                Some(tables.owned.get(session)?.at().saturating_sub(*through))
            })
            .max()
            .unwrap_or(0)
    }

    /// Every session this node owns and streams, for a sweep.
    ///
    /// A snapshot rather than an iterator over the lock, because what
    /// a sweep does per session — read a queue, send it — must not
    /// happen under the table's lock.
    #[must_use]
    pub fn owned_sessions(&self) -> Vec<SessionKey> {
        locked(&self.inner).owned.keys().cloned().collect()
    }

    /// Whether `follower` has ever reported a mark for `session`.
    ///
    /// What section 8.6's repair looks for: a follower rendezvous now
    /// names for a session, that has never said how far it is, holds
    /// nothing of it — it was not in the set when the queue was
    /// streamed, or it was and has since lost it.
    #[must_use]
    pub fn has_mark(&self, session: &SessionKey, follower: [u8; 16]) -> bool {
        locked(&self.inner)
            .marks
            .contains_key(&(session.clone(), follower))
    }

    /// Record the outcome of one sweep: how many sessions still need a
    /// copy sent somewhere, and how many were sent this time.
    pub fn swept(&self, pending: u64, done: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.repairs_pending.store(pending, Relaxed);
        self.repairs_done.fetch_add(done, Relaxed);
    }

    /// Sessions whose replica set was short at the last sweep.
    #[must_use]
    pub fn repairs_pending(&self) -> u64 {
        self.repairs_pending
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Sessions re-streamed to a new follower, ever.
    #[must_use]
    pub fn repairs_done(&self) -> u64 {
        self.repairs_done.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Changes this node has applied as a follower, ever.
    #[must_use]
    pub fn applied(&self) -> u64 {
        self.applied.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Section 8.6's copies, per peer: what each other node holds
    /// for this one and what this one holds for it.
    ///
    /// What the Cluster page's per-member columns are made of, and the
    /// one place the table is read by peer rather than by session. A
    /// peer appears when it has confirmed a copy of something this
    /// node owns, or this node holds a copy of something it owns;
    /// `behind` is the furthest of that peer's marks behind this
    /// node's position, which is the same reading [`Copies::lag`]
    /// gives per session. Sorted by identity, like everything else.
    #[must_use]
    pub fn by_peer(&self) -> Vec<PeerCopies> {
        let placed: HashMap<SessionKey, Vec<Placed>> = self
            .owned_sessions()
            .into_iter()
            .filter_map(|session| Some((session.clone(), self.placed(&session)?)))
            .collect();
        let installed = self.followers.get().is_some();
        let tables = locked(&self.inner);
        let mut peers: BTreeMap<[u8; 16], PeerCopies> = BTreeMap::new();
        for ((session, follower), through) in &tables.marks {
            let Some(owner) = tables.owned.get(session) else {
                continue;
            };
            if installed && !Self::counts(placed.get(session).map(Vec::as_slice), *follower) {
                continue;
            }
            let entry = peers
                .entry(*follower)
                .or_insert_with(|| PeerCopies::new(*follower));
            entry.confirmed += 1;
            entry.behind = entry.behind.max(owner.at().saturating_sub(*through));
        }
        for (owner, _) in tables.followed.values() {
            peers
                .entry(*owner)
                .or_insert_with(|| PeerCopies::new(*owner))
                .held += 1;
        }
        peers.into_values().collect()
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

/// One peer's share of this node's copies. See [`Copies::by_peer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerCopies {
    /// The peer.
    pub node: [u8; 16],
    /// Sessions this node owns that the peer has confirmed a copy of.
    pub confirmed: u64,
    /// The furthest any of those copies is behind this node.
    pub behind: u64,
    /// Sessions the peer owns that this node holds a copy of.
    pub held: u64,
}

impl PeerCopies {
    fn new(node: [u8; 16]) -> Self {
        Self {
            node,
            confirmed: 0,
            behind: 0,
            held: 0,
        }
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
                    // Sent by `marked`, which is how it says the
                    // position was reached.
                    Ok(Ok(())) => Confirmed::Yes,
                    // Dropped by `Copies::stop`: the session is no
                    // longer streamed from here, and nothing will ever
                    // confirm it.
                    Ok(Err(_)) | Err(_) => Confirmed::No,
                }
            }
        }
    }
}

/// What a stored copy of `session` holds, as a handover.
///
/// Section 8.6's failover reads this on the node whose copy is furthest
/// ahead — remotely, to answer a `PullCopy`, and locally, when the node
/// doing the claiming is itself the one holding the best copy. One
/// function for both, because the two must hand over the same thing.
///
/// **Only the queue, and that is the whole of what a copy is.** The
/// stream carries appends, acknowledgements and removals and nothing
/// else; a copy holds no subscriptions, no inflight window and no
/// inbound identifiers, so none come back from it. A client that
/// failed over has its queue and re-subscribes as it would after any
/// session loss — section 8.6 replicates offline queues, and says so.
///
/// Walked with `queue_head` from a cursor rather than restored whole,
/// for the reason that method exists: a restore reads every session
/// the node holds to answer about one.
///
/// # Errors
///
/// Whatever the store said. A copy that cannot be read is a copy that
/// cannot be pulled, and the caller counts it as nothing held.
pub fn copy_as_handover(
    store: &dyn storage::Storage,
    session: &SessionKey,
) -> Result<broker_core::handover::Handover, storage::StorageError> {
    let mut handover = broker_core::handover::Handover {
        highest_sequence: store.highest_sequence(session)?,
        ..Default::default()
    };
    let mut cursor = 0u64;
    while let Some(entry) = store.queue_head(session, cursor)? {
        // Past this one next time: sequence numbers are never reused,
        // so a cursor one past the head cannot miss anything.
        cursor = entry.sequence + 1;
        let Some(body) = store.body(&entry.delivery.body)? else {
            // An entry whose payload is gone is a message that cannot
            // be delivered, and handing it over would hand over a
            // reference to nothing. Left out rather than failing the
            // whole pull: the rest of the queue is still worth having.
            continue;
        };
        handover.queued.push((entry, body));
    }
    Ok(handover)
}

#[cfg(test)]
mod tests;
