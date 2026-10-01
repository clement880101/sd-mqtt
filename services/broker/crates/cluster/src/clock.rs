//! A clock two nodes can compare.
//!
//! Section 8.2: every replicated record is stamped with a hybrid logical
//! clock reading and the writing node's identity, and the order is the
//! reading's milliseconds, then its counter, then the node's UUID bytes.
//! This module is the first two of those three.
//!
//! # Why the existing clock cannot do this
//!
//! `broker-core`'s `CoarseClock` is a monotonic counter from an
//! arbitrary origin, advanced explicitly by one thread. That is exactly
//! right for a deadline and useless here: two nodes' coarse readings are
//! two counts from two origins, so comparing them is comparing nothing.
//! A stamp has to be comparable across nodes, so the physical half comes
//! from the wall clock, and this is the first wall-clock read on a write
//! path in this broker.
//!
//! **The reading is injected, not read here**, the way the coarse clock's
//! is. [`HybridClock::local`] and [`HybridClock::observe`] take the
//! millisecond reading as a parameter; [`wall_millis`] is what the one
//! production caller passes. That is what makes drift testable: a peer a
//! wrong year ahead is an argument rather than a machine with its clock
//! changed.
//!
//! # Three rules, and one function that keeps all of them
//!
//! - **A reading never goes backwards**, whatever the wall clock does.
//!   An operator correcting a clock, an NTP step, a virtual machine
//!   resumed from a snapshot: none of them may move a stamp back.
//! - **Two readings from one node are never equal.** Section 8.2 makes
//!   convergence conditional on no two writes ever sharing a stamp, and
//!   a stamp is a reading and a node identity, so a node that issued one
//!   reading twice would be the duplicate the handshake's refusal of a
//!   duplicated identity is there to prevent.
//! - **A peer's reading is taken but never trusted past a bound.**
//!
//! [`advance_past`] is all three at once: it returns the smallest
//! reading greater than a floor, with the millisecond pulled up to the
//! wall clock when the wall clock is ahead. A local event's floor is the
//! last reading; a remote event's floor is the greater of the last
//! reading and the peer's. So "a peer far behind changes nothing" is not
//! a case in this module — it is the same call with a floor the peer did
//! not raise.
//!
//! # A peer beyond the bound is refused, not clamped
//!
//! Section 8.2 is explicit and it reverses the obvious implementation.
//! Clamping sounds safer and is the opposite: clamping still advances the
//! local clock to the peer's reading, so one node with a wrong year
//! poisons every node it reaches, permanently, and no later write
//! anywhere can ever win a conflict against it. Refusing confines the
//! damage to that node's own frames, which is a peer nobody agrees with
//! rather than a cluster nobody can write to.
//!
//! What is deliberately *not* defended against is a peer far behind. Its
//! writes simply lose, which is the correct outcome, and no reading of
//! ours is harmed by having seen it. Section 8.2 notes that such a node
//! is worth counting, because its operator edits and sees nothing
//! happen; counting it belongs with the rest of the replication metrics
//! and is not here.
//!
//! # Resuming, which is the whole reason the reading is stored
//!
//! A clock that restarted from zero would re-issue readings it had
//! already used — the same millisecond, counting from zero again — and
//! two writes sharing one stamp is the single divergence no exchange
//! repairs: each side keeps its own value and believes it already holds
//! the winner. So [`HybridClock::resume`] starts from the highest
//! reading the store holds, and [`HybridClock::mark`] is what a write
//! batch carries so that the store holds it.
//!
//! **The mark goes in the same batch as the write it stamps.** A batch
//! is all or nothing, so a stamp that survived a crash has its mark
//! beside it and a stamp that did not was never shown to anybody.

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use storage::ClockRecord;

/// How far ahead of this node's wall clock a peer may be.
///
/// One minute. Two nodes with any time synchronisation at all are within
/// milliseconds of each other, so a minute is far past any real skew;
/// and the mistakes this bound exists for are not skew at all but a
/// wrong day, a wrong year or a clock never set, which are all orders of
/// magnitude beyond it. A tighter bound would start refusing honest
/// peers on a machine whose synchronisation had lapsed; a looser one
/// would accept a reading no correct node could have produced.
///
/// A constant here and a parameter on [`HybridClock::resume`], because
/// this is a property of a deployment and an operator will eventually
/// want to say so.
pub const DEFAULT_MAX_DRIFT_MILLIS: u64 = 60_000;

/// One reading of a hybrid logical clock.
///
/// Milliseconds since the Unix epoch and a counter of how many readings
/// have been issued inside that millisecond.
///
/// # Field order is the ordering, and both fields are load-bearing
///
/// `Ord` is derived, so it compares [`Self::millis`] and then
/// [`Self::counter`], which is the order section 8.2 names. Reordering
/// the fields would change what the cluster means by "later" without
/// changing a line of the comparison, which is why they are documented
/// here rather than at the comparison.
///
/// Milliseconds alone do not converge: two nodes writing in the same
/// millisecond would each keep their own and no exchange would repair
/// it. And a node without the counter is ambiguous with *itself*,
/// because two of its own writes inside one millisecond would share a
/// key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Reading {
    /// The physical half: milliseconds since the Unix epoch.
    millis: u64,
    /// The logical half: readings already issued inside [`Self::millis`].
    counter: u32,
}

impl Reading {
    /// The reading a clock that has never issued one sits at.
    ///
    /// **Never itself issued.** [`advance_past`] returns something
    /// strictly greater than its floor, so a clock starting here issues
    /// `(0, 1)` at the earliest. That matters: it makes "the store holds
    /// no reading" and "the store holds the first reading" different
    /// states rather than the same one, which is what lets
    /// [`HybridClock::resume`] take `None` honestly.
    pub const ZERO: Self = Self {
        millis: 0,
        counter: 0,
    };

    /// A reading from its two halves.
    #[must_use]
    pub const fn new(millis: u64, counter: u32) -> Self {
        Self { millis, counter }
    }

    /// The physical half.
    #[must_use]
    pub const fn millis(self) -> u64 {
        self.millis
    }

    /// The logical half.
    #[must_use]
    pub const fn counter(self) -> u32 {
        self.counter
    }

    /// The reading a store holds, as one of these.
    #[must_use]
    pub const fn from_record(record: ClockRecord) -> Self {
        Self::new(record.millis, record.counter)
    }

    /// This reading, as a store holds it.
    #[must_use]
    pub const fn to_record(self) -> ClockRecord {
        ClockRecord {
            millis: self.millis,
            counter: self.counter,
        }
    }
}

/// This machine's wall clock, in milliseconds since the Unix epoch.
///
/// The one production caller of [`HybridClock::local`] passes this. It
/// is a free function rather than something the clock reads for itself
/// so that every test can say what time it is; see the module
/// documentation.
///
/// A clock set before 1970 reads as zero rather than failing. Nothing
/// downstream can do anything useful with an error here, and a reading
/// of zero is not a hazard: the local clock refuses to go backwards, so
/// the only effect is that this node's counter does the work until the
/// machine's clock is corrected.
#[must_use]
pub fn wall_millis() -> u64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(since).unwrap_or(u64::MAX)
}

/// The smallest reading greater than `floor`, pulled up to `wall`.
///
/// The whole of this clock's arithmetic, in one place so that a local
/// event and a remote one cannot drift into two different rules.
///
/// Three cases, and the middle one is the one that is easy to leave out:
///
/// - The wall clock is ahead of the floor, so the reading moves to it
///   and the counter starts again.
/// - The counter is at its maximum, so the millisecond advances rather
///   than the counter wrapping or the process panicking. Four billion
///   events inside one millisecond is unreachable; the branch costs one
///   comparison and monotonicity is the entire property.
/// - Otherwise the millisecond stands and the counter moves, which is
///   what makes two writes inside one millisecond distinguishable.
///
/// The millisecond saturates. At [`u64::MAX`] milliseconds — some 584
/// million years after 1970 — a clock at its maximum counter would
/// return its floor and break monotonicity; the reading cannot get there
/// from [`wall_millis`] or from a store this build wrote.
fn advance_past(floor: Reading, wall: u64) -> Reading {
    if wall > floor.millis {
        Reading::new(wall, 0)
    } else if floor.counter == u32::MAX {
        Reading::new(floor.millis.saturating_add(1), 0)
    } else {
        Reading::new(floor.millis, floor.counter + 1)
    }
}

/// A hybrid logical clock, which is what stamps a replicated write.
///
/// # Behind a lock, and why that is enough
///
/// Issuing a reading is read-decide-write over twelve bytes, so a
/// [`Mutex`] rather than an atomic: packing the pair into one `u64` to
/// make it a compare-and-swap would cap the counter or the millisecond
/// at something, and "monotonic except at the cap" is the kind of
/// almost-true that this module exists to avoid. The contention is one
/// write path's worth, and every write behind it is already going to a
/// store.
#[derive(Debug)]
pub struct HybridClock {
    /// The highest reading issued so far, which is the floor for the
    /// next one.
    last: Mutex<Reading>,
    /// How far ahead of this node's wall clock a peer may be before
    /// [`HybridClock::observe`] refuses it.
    max_drift_millis: u64,
}

impl HybridClock {
    /// A clock resuming from what the store holds.
    ///
    /// `None` is a store no clock has ever written to, which is a node
    /// whose first write is still to come. Anything else is the highest
    /// reading this node has ever issued, and the next reading this
    /// clock issues is greater than it **whatever the wall clock now
    /// says** — which is the point, because a wall clock that moved
    /// backwards across the restart is exactly the case where a clock
    /// resuming from zero would re-issue a stamp it has already used.
    #[must_use]
    pub fn resume(stored: Option<ClockRecord>, max_drift_millis: u64) -> Self {
        Self {
            last: Mutex::new(stored.map_or(Reading::ZERO, Reading::from_record)),
            max_drift_millis,
        }
    }

    /// How far ahead of this node a peer may be.
    #[must_use]
    pub fn max_drift_millis(&self) -> u64 {
        self.max_drift_millis
    }

    /// The highest reading issued so far.
    ///
    /// What the store must hold for the resume above to work, and
    /// therefore what [`HybridClock::mark`] writes down.
    #[must_use]
    pub fn high_water(&self) -> Reading {
        *self.held()
    }

    /// The record a write batch carries so that a restart can resume.
    ///
    /// **In the same batch as the write it stamps.** A batch is all or
    /// nothing, so a stamp that survived a crash has its mark beside it;
    /// a mark written in a batch of its own could be lost while the
    /// write it covers survived, which is a clock resuming behind itself
    /// with nothing to say so.
    #[must_use]
    pub fn mark(&self) -> ClockRecord {
        self.high_water().to_record()
    }

    /// A reading for something that happened here.
    ///
    /// Strictly greater than every reading this clock has issued, and at
    /// least `wall_millis` in its physical half.
    pub fn local(&self, wall_millis: u64) -> Reading {
        let mut held = self.held();
        let next = advance_past(*held, wall_millis);
        *held = next;
        next
    }

    /// A reading for something that happened here, now.
    ///
    /// [`HybridClock::local`] against [`wall_millis`]. The one caller
    /// that does not get to say what time it is, which is the broker.
    pub fn now(&self) -> Reading {
        self.local(wall_millis())
    }

    /// A reading for a peer's frame, having taken what the peer knew.
    ///
    /// Strictly greater than both this clock's last reading and `peer`,
    /// so that anything this node writes afterwards wins against the
    /// write that arrived — which is what makes a conflict resolve the
    /// way causality says it should.
    ///
    /// A peer behind us raises nothing, so this is then exactly
    /// [`HybridClock::local`]. That is a consequence of the arithmetic
    /// rather than a case in it; see [`advance_past`].
    ///
    /// # Errors
    ///
    /// [`ClockError::PeerTooFarAhead`] when the peer's millisecond is
    /// further ahead of `wall_millis` than
    /// [`HybridClock::max_drift_millis`] allows. **The clock is left
    /// exactly as it was**, which is the whole difference between
    /// refusing and clamping: see the module documentation.
    pub fn observe(&self, wall_millis: u64, peer: Reading) -> Result<Reading, ClockError> {
        let bound = wall_millis.saturating_add(self.max_drift_millis);
        if peer.millis > bound {
            // Before the lock is taken, so there is no path on which a
            // refused peer has touched anything.
            return Err(ClockError::PeerTooFarAhead {
                peer,
                wall_millis,
                max_drift_millis: self.max_drift_millis,
            });
        }
        let mut held = self.held();
        let next = advance_past(std::cmp::max(*held, peer), wall_millis);
        *held = next;
        Ok(next)
    }

    /// The reading behind the lock, recovering from a poisoned one.
    ///
    /// A panic cannot leave this inconsistent: the only write is one
    /// assignment of a value computed before it. Poisoning would turn
    /// one unrelated failure into every later write on this node
    /// failing, which is a worse answer than the one it is protecting.
    fn held(&self) -> std::sync::MutexGuard<'_, Reading> {
        self.last
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Why a peer's reading was refused.
///
/// One variant, and it names every number an operator needs: what the
/// peer claimed, what this node thinks the time is, and what it was
/// willing to accept. A refusal that said only "too far ahead" would
/// leave the operator unable to tell which of the two machines is wrong,
/// which is the only question worth asking about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClockError {
    /// The peer's reading is further ahead than the bound allows.
    #[error(
        "a peer's clock reads {} ms, which is {} ms ahead of this node's {wall_millis} ms \
         and past the {max_drift_millis} ms this node accepts; the frame is refused rather \
         than the local clock advanced, because advancing it would spread one wrong clock \
         to every node this one reaches",
        .peer.millis(),
        .peer.millis().saturating_sub(*.wall_millis),
    )]
    PeerTooFarAhead {
        /// What the peer claimed.
        peer: Reading,
        /// What this node's wall clock said at the time.
        wall_millis: u64,
        /// What this node was willing to accept.
        max_drift_millis: u64,
    },
}

#[cfg(test)]
mod tests;
