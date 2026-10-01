//! A register that converges, and why it must.
//!
//! Section 8.2: each replicated record is a last-writer-wins register
//! stamped with a hybrid logical clock reading and the writing node's
//! identity. [`Stamp`] is that pair, [`Register`] is a value carrying
//! one, and [`Register::merge`] is the single function that decides which
//! of two copies survives.
//!
//! # The order is three parts and all three are load-bearing
//!
//! The reading's milliseconds, then its counter, then the node's UUID
//! bytes. [`Stamp`]'s own `Ord` spells all three out rather than
//! deriving two of them from [`Reading`], because the specification
//! names three and a reader checking the implementation against it
//! should be able to see three.
//!
//! - **Milliseconds alone do not converge.** Two nodes writing in the
//!   same millisecond each keep their own, and no later exchange repairs
//!   it, because neither side's stamp beats the other's.
//! - **Without the counter a node is ambiguous with itself**, because two
//!   of its own writes inside one millisecond share a key.
//! - **Without the identity two nodes tie**, which is the first case
//!   again: a tie is two sides each keeping what they have.
//!
//! # Convergence is conditional, and the condition is real
//!
//! **No two writes may ever share a stamp.** That is not a theoretical
//! caveat; a property test found it, phrased as "two nodes handed the
//! same writes in any order agree", and the shrunk counterexample was
//! two writes carrying one stamp with different values. Each side keeps
//! its own and re-gossiping changes nothing, because each believes it
//! already holds the winner. There is no merge rule that repairs it:
//! the two copies are indistinguishable, so whatever rule one side
//! applies the other applies identically to the mirror image.
//!
//! Two things can produce a duplicate stamp and a clock is not one of
//! them, because [`crate::clock::HybridClock`] is strictly increasing.
//! They are:
//!
//! - **A duplicated node identity**, which the handshake refuses: see
//!   [`crate::handshake`].
//! - **A clock that restarts behind itself**, which is why a node
//!   resumes from the highest reading its store holds rather than from
//!   zero: see [`crate::clock`].
//!
//! So [`Register::merge`] keeps the copy it already holds when two
//! stamps are equal. That choice is arbitrary and is *meant* to look
//! arbitrary: there is no right answer, which is the whole argument for
//! the two guarantees above.
//!
//! # A tombstone is a value, not a rule
//!
//! Section 8.2 is explicit, and it reverses the obvious implementation.
//! "A delete always wins" diverges: two nodes that saw a delete and a
//! later write in different orders keep different things, for exactly
//! the reason a tie does. So a deletion is a [`Register`] whose value is
//! absent, competing by the same comparison as any other, and there is
//! **one merge function with no path through it that asks whether
//! either side is a tombstone.**
//!
//! What a tombstone is for is the thing an absent register cannot say:
//! that something was deleted, and when, so that a node which never saw
//! the value does not resurrect it from an older copy. Collecting them
//! behind a cluster-wide watermark is a later task; nothing here expires
//! one.

use std::cmp::Ordering;

use crate::clock::Reading;
use crate::identity::NodeId;

/// What orders two writes: when, and by whom.
///
/// # Ordered by three things, in this order
///
/// The reading's milliseconds, the reading's counter, then the writing
/// node's sixteen UUID bytes. See the module documentation for what each
/// of the three is doing; none of them can be dropped.
///
/// The identity is last because it is the tie-break and not the
/// decision: two nodes do not have an order worth respecting, they have
/// an order worth *agreeing on*. Any total order over the identities
/// would do, and the bytes are the one that needs no locale, no
/// normalisation and no agreement beyond the bytes themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Stamp {
    /// When, as far as the writing node's clock was concerned.
    reading: Reading,
    /// Who wrote it.
    node: NodeId,
}

impl Stamp {
    /// A stamp from its two parts.
    #[must_use]
    pub const fn new(reading: Reading, node: NodeId) -> Self {
        Self { reading, node }
    }

    /// When.
    #[must_use]
    pub const fn reading(self) -> Reading {
        self.reading
    }

    /// Who.
    #[must_use]
    pub const fn node(self) -> NodeId {
        self.node
    }
}

impl Ord for Stamp {
    /// Section 8.2's order, written out as the three comparisons it is.
    ///
    /// Spelled out rather than derived from the field order, so that the
    /// three parts the specification names are three lines a reader can
    /// check against it. Deriving would give the same answer today and
    /// would make "the counter is part of the comparison" a fact about
    /// [`Reading`]'s field order, two files away from the comparison it
    /// decides.
    fn cmp(&self, other: &Self) -> Ordering {
        self.reading
            .millis()
            .cmp(&other.reading.millis())
            .then_with(|| self.reading.counter().cmp(&other.reading.counter()))
            .then_with(|| self.node.to_bytes().cmp(&other.node.to_bytes()))
    }
}

impl PartialOrd for Stamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A last-writer-wins register: a value, a stamp, and whether the value
/// is there at all.
///
/// An absent value is a tombstone, and a tombstone is a value as far as
/// [`Register::merge`] is concerned. See the module documentation for why
/// that is a requirement rather than a simplification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Register<V> {
    /// What orders this against another copy of the same record.
    stamp: Stamp,
    /// What is held, or `None` for a deletion.
    value: Option<V>,
}

impl<V> Register<V> {
    /// A written value.
    pub const fn written(value: V, stamp: Stamp) -> Self {
        Self {
            stamp,
            value: Some(value),
        }
    }

    /// A deletion: a tombstone, stamped like any other write.
    pub const fn deleted(stamp: Stamp) -> Self {
        Self { stamp, value: None }
    }

    /// What orders this copy against another.
    pub const fn stamp(&self) -> Stamp {
        self.stamp
    }

    /// What is held, or `None` for a tombstone.
    pub const fn value(&self) -> Option<&V> {
        self.value.as_ref()
    }

    /// Whether this is a deletion.
    pub const fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// Take `other` if it is later, and say whether it was taken.
    ///
    /// **The whole of the conflict resolution, and the only one.** There
    /// is no branch here on whether either side is a tombstone, and
    /// there must not be: see the module documentation for why "a delete
    /// always wins" diverges.
    ///
    /// Returns `true` when this register changed, which is what a caller
    /// uses to decide whether anything needs storing or passing on. A
    /// copy that lost is a copy that told us nothing.
    ///
    /// Equal stamps keep what is already held. That is arbitrary, and
    /// section 8.2 makes "no two writes share a stamp" an operational
    /// requirement precisely because no answer here would be better; see
    /// the module documentation for the two things that guarantee it.
    pub fn merge(&mut self, other: Self) -> bool {
        if other.stamp > self.stamp {
            *self = other;
            true
        } else {
            false
        }
    }

    /// The later of two copies, as a value.
    ///
    /// [`Register::merge`] for a caller holding both rather than one.
    /// The same comparison, in one place, so the two cannot disagree.
    #[must_use]
    pub fn merged(mut self, other: Self) -> Self {
        self.merge(other);
        self
    }
}

#[cfg(test)]
mod tests;
