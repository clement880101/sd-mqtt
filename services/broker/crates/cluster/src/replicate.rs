//! A write is broadcast and written down, and an exchange repairs what
//! the broadcast lost.
//!
//! Section 8.2: **writes broadcast immediately to all peers**, and every
//! few seconds a node exchanges a digest with a random peer and pulls
//! whatever it is missing. Both halves are here, and [`crate::oplog`] is
//! the log the second half asks about.
//!
//! # The exchange is a pull, one peer at a time, one direction at a time
//!
//! A round is three frames at most: this node tells one peer how far it
//! has read *that peer's* log, the peer answers with the next batch of it,
//! and this node asks again if there is more. Nothing in a round is about
//! this node's own log, and nothing in it is about a third node's.
//!
//! That is not a simplification, it is what "the digest is a claim about
//! an operation log" forces. A node holds its peers' **registers** and
//! only its own **log**, so it can serve entries it originated and
//! nothing else: asked for node C's log, node B has no entries to send
//! even where it holds every register C ever wrote. Symmetry comes from
//! every node running its own rounds against a random peer, which in a
//! full mesh reaches everybody.
//!
//! # A tombstone collected too early is a delete that never arrives
//!
//! Section 8.2 collects tombstones after a grace period long enough to
//! exceed any plausible partition, **tracked by a cluster-wide low
//! watermark**. The grace period alone is not enough and the plan says so;
//! what it gets wrong is only the shape of the damage, and the difference
//! is worth writing down because it is what the test asserts.
//!
//! The plan's worry is that a node which never saw a delete re-broadcasts
//! its old value and nothing contradicts it. That cannot happen here, for
//! a stronger reason than the watermark: **a node serves only the log it
//! originated**, so a register it merely holds is never offered to
//! anybody. A node that missed a delete does not spread the old value.
//!
//! What it does do is keep it, for ever. The node that deleted the record
//! has thrown away both the tombstone and the log entry carrying it, so
//! the next exchange has nothing to send; the absent node comes back
//! holding a retained message every other node has deleted, answers
//! subscribers with it, and no later exchange ever contradicts it. That is
//! the same outcome as a resurrection seen from a client, and it lasts
//! just as long.
//!
//! So the watermark is **the lowest sequence every node has read of the
//! deleting node's log, not the highest**. The highest is the node that
//! is up to date, which is the one node whose agreement proves nothing;
//! the lowest is the node that has been away, which is the only one the
//! tombstone is still for. A node that is permanently gone and still in
//! the member list holds the watermark at where it left off, so nothing is
//! collected until an operator removes it — which is the conservative
//! direction and is said out loud on [`Replicator::sweep`].
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

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use storage::{ClockRecord, LoggedRecord, Record};

use crate::clock::Reading;
use crate::identity::NodeId;
use crate::metadata::{MetadataError, MetadataKey, MetadataWrite, Replicated, Taken};
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

/// How long to wait before the next exchange, with section 8.2's jitter.
///
/// Between half the interval and all of it. **The jitter is not a
/// refinement**: without it fifty nodes started by one deployment ask each
/// other in the same millisecond for ever, which is a cluster whose
/// traffic arrives in spikes and whose spikes line up with every other
/// timer in the process. Section 8.1's reconnection backoff is jittered
/// for the same reason and by the same arithmetic, which is the one this
/// calls.
///
/// An interval of nothing stays nothing, so a configuration that turns the
/// exchange into a tight loop is not turned into something else by this.
/// No randomness is not a reason to synchronise a cluster, so the
/// unjittered interval is the fallback.
#[must_use]
pub fn jitter(interval: Duration) -> Duration {
    crate::mesh::jitter(interval)
}

/// One of `peers`, chosen at random.
///
/// Section 8.2 asks for a **random** peer rather than the next in a list,
/// and the reason is the shape of a repair: a ring would make every node's
/// exchange the same pair for ever, so a write that reached only one node
/// would travel one hop per interval around the whole cluster. A random
/// peer reaches everybody in a number of rounds that grows with the
/// logarithm of the cluster rather than with the cluster.
///
/// `None` for no peers, which is a cluster of one and a supported way to
/// run this broker.
///
/// The randomness is the one this crate already links for section 8.1's
/// nonces and its backoff jitter. If it fails there is still a peer to
/// ask — the first — because asking the same peer every time is worse than
/// asking nobody.
#[must_use]
pub fn choose(peers: &[NodeId]) -> Option<NodeId> {
    let first = *peers.first()?;
    let mut bytes = [0_u8; 8];
    if aws_lc_rs::rand::SecureRandom::fill(&aws_lc_rs::rand::SystemRandom::new(), &mut bytes)
        .is_err()
    {
        return Some(first);
    }
    let which = usize::try_from(u64::from_le_bytes(bytes) % peers.len() as u64).unwrap_or(0);
    peers.get(which).copied().or(Some(first))
}

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
    /// "I hold your log through `have`; send me the rest."
    ///
    /// Section 8.2's digest. **A claim about the receiver's log**, not
    /// about the sender's own and not about the records: a register's most
    /// recent writer changes whenever another node overwrites it, so there
    /// is no question about the records that this could ask.
    ///
    /// Zero is "nothing of yours", which is a node that has just met this
    /// peer or has never heard from it. See
    /// [`crate::oplog::FIRST_SEQUENCE`].
    ///
    /// It carries one thing about the **sender's own** writes, which is
    /// how the tombstone watermark travels: see [`Self::Digest::collected`].
    Digest {
        /// The highest contiguous sequence of the receiver's log the
        /// sender holds.
        have: u64,
        /// The highest reading of the sender's own writes that every node
        /// the sender knows of has read.
        ///
        /// **Only about the sender**, because only the sender knows how
        /// far every other node has read *its* log: a digest is the only
        /// thing that ever says so, and each node collects the minimum of
        /// the digests it has been sent. A receiver may collect a tombstone
        /// the sender wrote once the tombstone's reading is at or below
        /// this and the grace period has passed.
        ///
        /// Zero is a node that has not heard from every peer yet, which is
        /// a node whose tombstones nobody may collect. That is the safe
        /// direction: see the module documentation.
        collected: ClockRecord,
    },
    /// The answer: entries of the sender's own log, in order.
    ///
    /// Bounded, because a node a million writes behind must not be sent
    /// them in one frame — [`crate::frame`] would refuse it, which would
    /// make the repair impossible on exactly the node that needs it most.
    /// [`Self::Batch::more`] is what makes the next round carry on rather
    /// than the receiver having to guess.
    Batch {
        /// The entries, contiguous and starting just after what the asker
        /// said it had.
        entries: Vec<LoggedRecord>,
        /// Whether the sender has more past the last of these.
        more: bool,
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
    /// What to say back to the peer, if anything.
    ///
    /// A digest is answered with a batch, and a batch that says there is
    /// more is answered with another digest. `None` ends the round, which
    /// is what a peer that is up to date gets: see
    /// [`Replicator::received`].
    pub reply: Option<Replication>,
}

impl Handled {
    /// Whether this frame changed nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.applied.is_empty() && self.reply.is_none()
    }
}

/// What a sweep took.
///
/// Returned rather than acted on, for the reason the module documentation
/// gives about every other method here: the records go to the caller's
/// store, in one batch.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Swept {
    /// What to store: the registers that went and the log truncation.
    pub records: Vec<Record>,
    /// The tombstones that went.
    pub collected: Vec<MetadataKey>,
    /// How far this node's own log was truncated.
    pub log_through: u64,
}

impl Swept {
    /// Whether the sweep took nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
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
    /// What peers have said about this node's own log.
    gossip: Mutex<Gossip>,
}

/// What peers have said, which is not in the store.
///
/// **In memory only, and a lock of its own.** It is rebuilt from the next
/// round of digests, so storing it would be a durable copy of something
/// that is a few seconds stale by definition; and it is touched on the
/// exchange's path rather than on a write's, so putting it behind
/// [`Replicated`]'s lock would put every digest behind every subscribe.
#[derive(Debug, Default)]
struct Gossip {
    /// How far each peer says it has read **this node's** log.
    ///
    /// The other direction from [`Replicated::seen_through`], and the one
    /// no store holds: a peer tells this node where it is in a digest, and
    /// the minimum over every peer is what this node may collect behind.
    ours_at: BTreeMap<NodeId, u64>,
    /// What each peer says every node has read of *its* writes.
    ///
    /// The reading from that peer's last digest, which is the only thing
    /// that ever says it: only the writer knows how far every node has
    /// read its own log. A peer with no entry here has sent no digest yet,
    /// and nothing it wrote may be collected.
    theirs_collected: BTreeMap<NodeId, Reading>,
    /// The same number about this node's own writes.
    ///
    /// Remembered rather than recomputed, because computing it reads the
    /// reading at a sequence of this node's log and that entry is
    /// truncated moments later; it only ever moves forwards.
    ours_collected: Reading,
}

impl Replicator {
    /// A replicator over `replicated`.
    #[must_use]
    pub fn new(replicated: std::sync::Arc<Replicated>, settings: ReplicationSettings) -> Self {
        Self {
            replicated,
            settings,
            gossip: Mutex::new(Gossip::default()),
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

    /// What to ask `peer` for: a digest of how far this node has read it.
    ///
    /// One peer per round and chosen by the caller, because which peers
    /// there are is the mesh's answer and section 8.2 says the peer is
    /// random: see [`ReplicationSettings::exchange`] for the interval and
    /// [`jitter`] for why it is not the interval exactly.
    #[must_use]
    pub fn digest_for(&self, peer: NodeId) -> Replication {
        Replication::Digest {
            have: self.replicated.seen_through(peer),
            collected: self.gossip().ours_collected.to_record(),
        }
    }

    /// How far `peer` last said it had read this node's log.
    ///
    /// Zero for a peer that has never sent a digest. What section 8.2's
    /// watermarks are a minimum over.
    #[must_use]
    pub fn peer_holds_of_ours(&self, peer: NodeId) -> u64 {
        self.gossip().ours_at.get(&peer).copied().unwrap_or(0)
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
            Replication::Digest { have, collected } => Ok(self.answer(
                from,
                have,
                Reading::new(collected.millis, collected.counter),
            )),
            Replication::Batch { entries, more } => {
                let mut handled = self.take(from, entries)?;
                // Asked again only where the peer said there is more, so a
                // round against a peer that is up to date is one frame and
                // the answer to it, and never a third.
                if more {
                    handled.reply = Some(self.digest_for(from));
                }
                Ok(handled)
            }
        }
    }

    /// Answer `from`'s digest with the next batch of this node's log.
    ///
    /// **Bounded by [`ReplicationSettings::batch`]**, and `more` says
    /// whether there is another batch behind this one. No reply at all is
    /// a peer that has everything, which is the ordinary answer and the
    /// reason a digest is a digest rather than a request for everything: an
    /// implementation that answered it with its whole log would make the
    /// exchange a periodic full resynchronisation, and one that answered
    /// with an empty batch would be a frame per peer per interval for ever
    /// on an idle cluster.
    ///
    /// A peer claiming to hold more than this node has published is not an
    /// error. It is what a node restored from a copy of somebody else's
    /// store looks like, and what a node whose own store lost an unflushed
    /// batch looks like; there is nothing to send either way, and closing
    /// the link would stop the rest of the exchange working for no gain.
    fn answer(&self, from: NodeId, have: u64, collected: Reading) -> Handled {
        // Recorded whatever comes of it: a digest is the only thing that
        // ever says where a peer is in this node's log, or what the peer
        // says everybody has read of its own. Both only ever move
        // forwards; a digest that went backwards is one that overtook
        // another, and taking the larger is cheaper than reasoning about
        // which is newer.
        {
            let mut gossip = self.gossip();
            gossip
                .ours_at
                .entry(from)
                .and_modify(|held| *held = std::cmp::max(*held, have))
                .or_insert(have);
            gossip
                .theirs_collected
                .entry(from)
                .and_modify(|held| *held = std::cmp::max(*held, collected))
                .or_insert(collected);
        }

        let entries = self.replicated.log_after(have, self.settings.batch);
        if entries.is_empty() {
            return Handled::default();
        }
        let last = entries.last().map_or(have, Logged::sequence);
        let more = self.replicated.log_published_through() > last;
        Handled {
            reply: Some(Replication::Batch {
                entries: entries.iter().map(Logged::to_record).collect(),
                more,
            }),
            ..Handled::default()
        }
    }

    /// Collect what every node has read, and truncate this node's log.
    ///
    /// Section 8.2's two watermarks, in one pass because they come from one
    /// fact: how far every peer has read each node's log.
    ///
    /// `peers` is every node this one knows of, **dead ones included**.
    /// Membership is deliberately not replicated, so there is no cluster
    /// answer to ask; what this node has is its own member list, and a node
    /// in it that has said nothing counts as having read nothing. A node
    /// that is permanently gone therefore holds the watermark where it left
    /// off and nothing is collected until an operator removes it from the
    /// seeds and the member list. That is the conservative direction and it
    /// is the right one: the alternative is collecting a delete the absent
    /// node never saw, which it would then keep for ever.
    ///
    /// `now_millis` is the wall clock, injected the way
    /// [`crate::clock::HybridClock`]'s is, so that a grace period of an
    /// hour is an argument rather than an hour.
    ///
    /// **Both conditions, and the grace period is the weaker one.** A
    /// tombstone goes only when its reading is at or below what every node
    /// has read of the node that wrote it *and* the grace period has
    /// passed. The grace period on its own is what the module
    /// documentation is about.
    pub fn sweep(&self, now_millis: u64, peers: &[NodeId]) -> Swept {
        let mut swept = Swept::default();
        let me = self.replicated.me();

        // This node's own watermark first, because reading it needs the log
        // entry the truncation below removes.
        let ours_at = self.lowest_read_of_ours(peers);
        let collected = {
            let mut gossip = self.gossip();
            if let Some(reading) = self.replicated.log_reading_at(ours_at) {
                gossip.ours_collected = std::cmp::max(gossip.ours_collected, reading);
            }
            gossip.ours_collected
        };
        swept
            .records
            .extend(self.replicated.collect_log_through(ours_at));
        swept.log_through = ours_at;

        let grace = u64::try_from(self.settings.tombstone_grace.as_millis()).unwrap_or(u64::MAX);
        let gossip = self.gossip();
        for (key, stamp) in self.replicated.tombstones() {
            let reached = if stamp.node() == me {
                collected
            } else {
                match gossip.theirs_collected.get(&stamp.node()) {
                    Some(reading) => *reading,
                    // A node this one has never had a digest from. Nothing
                    // it wrote may be collected, because nothing says who
                    // has read it.
                    None => continue,
                }
            };
            if stamp.reading() > reached {
                continue;
            }
            // The grace period, measured on this node's own wall clock
            // against the writing node's reading. Comparable across nodes
            // because a peer further ahead than the drift bound is refused
            // rather than clamped, and that bound is a minute against a
            // grace period of an hour.
            if now_millis.saturating_sub(stamp.reading().millis()) < grace {
                continue;
            }
            let records = self.replicated.collect(&key);
            if !records.is_empty() {
                swept.records.extend(records);
                swept.collected.push(key);
            }
        }
        swept
    }

    /// The lowest sequence of **this node's** log any known peer has read.
    ///
    /// The whole of "the watermark is the lowest, not the highest". A peer
    /// that has said nothing has read nothing, so one silent member holds
    /// this at zero — and a cluster of one, with no peers at all, has had
    /// everything read by everybody, which is this node's own published
    /// end.
    fn lowest_read_of_ours(&self, peers: &[NodeId]) -> u64 {
        let published = self.replicated.log_published_through();
        let gossip = self.gossip();
        peers
            .iter()
            .filter(|peer| **peer != self.replicated.me())
            .map(|peer| gossip.ours_at.get(peer).copied().unwrap_or(0))
            .min()
            .map_or(published, |lowest| std::cmp::min(lowest, published))
    }

    /// What peers have said, recovering from a poisoned lock.
    ///
    /// The decision [`Replicated`] records: poisoning would turn one
    /// unrelated panic into every later exchange on this node failing,
    /// which is worse than what it protects.
    fn gossip(&self) -> MutexGuard<'_, Gossip> {
        self.gossip.lock().unwrap_or_else(PoisonError::into_inner)
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
        Ok(Handled {
            records,
            applied,
            reply: None,
        })
    }
}

#[cfg(test)]
mod tests;
