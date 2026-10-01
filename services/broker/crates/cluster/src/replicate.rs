//! A write is broadcast and written down.
//!
//! Section 8.2: **writes broadcast immediately to all peers**, and every
//! few seconds a node exchanges a digest with a random peer and pulls
//! whatever it is missing. This module is the first half — what one node
//! says when something changes, and what it does with what a peer says —
//! and [`crate::oplog`] is the log the second half will ask about.
//!
//! # A broadcast that replays is a broadcast that duplicates
//!
//! There is no queue here and there must not be. A broadcast reaches the
//! peers this node has a link to **at the moment of the write**, and a
//! peer whose link has gone receives nothing: [`crate::link::Outgoing`]
//! drops and counts rather than blocking, and a link that is gone has no
//! queue at all. A peer that was down and comes back is therefore not
//! sent the backlog, and that is deliberate — repairing what a broadcast
//! lost is the exchange's job, and a broadcast that replayed a backlog
//! would send every entry twice to every peer that was there all along.
//!
//! What makes the omission safe is that the write is in this node's log
//! before it is broadcast, so there is always something for the exchange
//! to find. That is the whole reason the log exists.
//!
//! # Nothing is sent from here
//!
//! [`Replicator`] computes and never transmits. Every method returns what
//! to send and what to store, and the caller — the shell, which holds the
//! mesh and the storage writer — does both. Three reasons, and the last is
//! the one that decided it:
//!
//! - A sink held here would be the mesh, and the mesh would hold whatever
//!   delivers frames to this, which holds this. A cycle of [`std::sync::Arc`]
//!   is a leak, and breaking it with a [`std::sync::Weak`] would put "is
//!   the mesh still there" in a path that has nothing to say about it.
//! - Storing is the caller's already, because section 8.2's register has
//!   to go in the same batch as the broker state that caused it, and the
//!   broker's journal is the shell's.
//! - A module that returns frames is a module whose tests are a list of
//!   frames. There is no runtime in these tests, no clock and no socket.
//!
//! # The frames carry the storage vocabulary
//!
//! [`Replication`] holds [`storage::LoggedRecord`] rather than
//! [`crate::Logged`], which is deliberate rather than lazy: the record
//! types are the one encoding in this project with hand-derived golden
//! vectors, and a second encoding of the same registers would be a second
//! thing to keep in step and a second thing to get wrong. A peer from
//! another build is refused by the frame version before any of this is
//! decoded; see [`crate::frame`].

use std::time::Duration;

use serde::{Deserialize, Serialize};
use storage::{LoggedRecord, Record};

use crate::identity::NodeId;
use crate::metadata::{MetadataError, MetadataWrite, Replicated, Taken};
use crate::oplog::Logged;

/// How many log entries one batch of an exchange carries.
///
/// Section 8.2 asks for batches, and the number is a frame-size decision:
/// a retained message can be a quarter of a megabyte, and
/// [`crate::frame::FrameLimits`] refuses a frame larger than the link
/// carries — so a batch large enough to be refused would be a repair that
/// can never happen, on exactly the node that is furthest behind.
pub const DEFAULT_BATCH: usize = 64;

/// How often a node exchanges a digest with one random peer.
///
/// Section 8.2's "every few seconds". Two: often enough that a lost
/// broadcast is repaired before anybody notices, rare enough that fifty
/// nodes gossiping is not traffic worth measuring.
pub const DEFAULT_EXCHANGE: Duration = Duration::from_secs(2);

/// How long a tombstone is kept once every node has seen it.
///
/// Section 8.2: long enough to exceed any plausible partition. An hour,
/// and the watermark is the other half — see [`Replicator`] for why the
/// grace period alone resurrects a deleted record.
pub const DEFAULT_TOMBSTONE_GRACE: Duration = Duration::from_secs(3_600);

/// How a node replicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationSettings {
    /// How many log entries one batch carries.
    pub batch: usize,
    /// How often this node exchanges a digest with a random peer.
    pub exchange: Duration,
    /// How long a tombstone is kept once every node has seen it.
    pub tombstone_grace: Duration,
}

impl Default for ReplicationSettings {
    fn default() -> Self {
        Self {
            batch: DEFAULT_BATCH,
            exchange: DEFAULT_EXCHANGE,
            tombstone_grace: DEFAULT_TOMBSTONE_GRACE,
        }
    }
}

/// What one node says to another about section 8.2's registers.
///
/// Carried inside [`crate::mesh::Message`], so the link, the framing and
/// the authentication are the ones section 8.1 already built.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Replication {
    /// One write, as it happened, with where it sits in the sender's log.
    ///
    /// **The sequence travels with it** so that a peer which has
    /// everything below it can advance its watermark on the broadcast
    /// alone, and so that a peer which has not can tell that it cannot:
    /// a sequence that is not the next one is a broadcast that was lost,
    /// and the exchange is what fills the gap.
    ///
    /// Whose log it is comes from the link rather than from the frame.
    /// A node on the frame would be a node a peer could claim to be.
    Broadcast {
        /// The entry.
        entry: LoggedRecord,
    },
}

/// Why a frame from a peer was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReplicationError {
    /// A register in the frame is one this build does not understand.
    ///
    /// The same refusal a stored register gets, for the same reason and
    /// read the other way round: nothing this build writes can produce
    /// the pair, so a peer that sent one is a peer this build does not
    /// speak to, and guessing which half to believe would route by one
    /// and answer by the other.
    #[error("a peer sent a register this build does not understand: {0}")]
    Register(#[from] MetadataError),
    /// The entry claimed sequence zero.
    ///
    /// A log numbers from one, and zero is what a watermark means by
    /// "nothing of yours"; see [`crate::oplog::FIRST_SEQUENCE`]. An entry
    /// at zero would be an entry no watermark could ever be past.
    #[error("a peer sent a log entry numbered zero, and a log numbers from one")]
    SequenceZero,
}

/// What a peer's frame changed, and what to do about it.
///
/// Returned rather than acted on, for the reason the module documentation
/// gives. Empty in every field is a frame that told this node nothing,
/// which is the ordinary answer to a broadcast of something already held.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Handled {
    /// What to store, in one batch.
    ///
    /// **One batch**, because the watermark in it is a claim to hold the
    /// registers beside it: stored apart, a crash between the two leaves a
    /// node claiming to have read a peer's log through a sequence whose
    /// entries it does not have, and no peer will ever send them again.
    pub records: Vec<Record>,
    /// The writes that won, which the caller turns into local effect.
    pub applied: Vec<MetadataWrite>,
}

impl Handled {
    /// Whether this frame changed nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.applied.is_empty()
    }
}

/// What this node sends and what it does with what it is sent.
///
/// Holds no state of its own: everything is in [`Replicated`], behind the
/// one lock that also holds the registers and the log, because a write
/// touches all three and a second lock would be a second order to take
/// them in.
#[derive(Debug)]
pub struct Replicator {
    /// This node's registers, clock and log.
    replicated: std::sync::Arc<Replicated>,
    /// How this node replicates.
    settings: ReplicationSettings,
}

impl Replicator {
    /// A replicator over `replicated`.
    #[must_use]
    pub fn new(replicated: std::sync::Arc<Replicated>, settings: ReplicationSettings) -> Self {
        Self {
            replicated,
            settings,
        }
    }

    /// What this node replicates.
    #[must_use]
    pub fn replicated(&self) -> &std::sync::Arc<Replicated> {
        &self.replicated
    }

    /// How this node replicates.
    #[must_use]
    pub const fn settings(&self) -> &ReplicationSettings {
        &self.settings
    }

    /// The store took the batch carrying `write`: broadcast it.
    ///
    /// Returns the frame for every peer this node has a link to, or `None`
    /// for a write this node did not originate or has already resolved.
    ///
    /// **Called after the store, not before.** A peer told about a
    /// register whose batch the store refused holds a value this node will
    /// never hold again, and no exchange repairs it because an exchange
    /// serves the origin's log and the origin's log lost the entry in the
    /// same batch; see [`crate::oplog`].
    pub fn published(&self, write: &MetadataWrite) -> Option<Replication> {
        let entry = self.replicated.published(write)?;
        Some(Replication::Broadcast {
            entry: entry.to_record(),
        })
    }

    /// The store refused the batch carrying `write`: it never happened.
    ///
    /// Returns whether there was one to refuse.
    pub fn refused(&self, write: &MetadataWrite) -> bool {
        self.replicated.refused(write)
    }

    /// What a peer said, and what to do about it.
    ///
    /// # Errors
    ///
    /// [`ReplicationError`] for a frame this build will not read. The link
    /// is the caller's to close or keep; nothing here has been changed
    /// when this returns an error, because the refusal happens before the
    /// merge.
    pub fn received(&self, from: NodeId, frame: Replication) -> Result<Handled, ReplicationError> {
        match frame {
            Replication::Broadcast { entry } => self.take(from, vec![entry]),
        }
    }

    /// Take entries `origin` logged, having checked every one of them.
    ///
    /// Decoded and refused **before** anything is merged, so a batch whose
    /// third entry this build does not understand leaves the first two
    /// unapplied rather than half of it taken. Convention 1 read the other
    /// way round: a frame a reader rejects is a frame that changes nothing.
    fn take(
        &self,
        origin: NodeId,
        entries: Vec<LoggedRecord>,
    ) -> Result<Handled, ReplicationError> {
        let mut logged = Vec::with_capacity(entries.len());
        for record in entries {
            if record.sequence == 0 {
                return Err(ReplicationError::SequenceZero);
            }
            logged.push(Logged::from_record(record)?);
        }
        let Taken {
            applied, records, ..
        } = self.replicated.observed_from(origin, logged);
        Ok(Handled { records, applied })
    }
}

#[cfg(test)]
mod tests;
