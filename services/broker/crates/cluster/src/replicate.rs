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
//! A round starts with two frames to one peer. The digest tells it how
//! far this node has read *that peer's* log; the peer answers with the
//! next batch of it, and this node asks again if there is more. The relay
//! tells it how far this node has read **every other** log, and is the
//! part about third nodes.
//!
//! The two are separate because "the digest is a claim about an
//! operation log" forces it. A node holds its peers' **registers** and
//! only its own **log**, so it can serve entries it originated and
//! nothing else: asked for node C's log, node B has no entries to send
//! even where it holds every register C ever wrote. Symmetry comes from
//! every node running its own rounds against a random peer, which in a
//! full mesh reaches everybody — **while C is alive**.
//!
//! # A dead origin's writes are relayed as registers
//!
//! Once C is dead nobody can serve its log, and a write of C's that
//! reached B and missed A was, before the relay, lost to A for good.
//! Section 8.2's version counters are per node and say nothing about who
//! may answer for them, so B answers for C: asked by A's relay, B sends
//! its registers — the pages of section 8.2's join snapshot, for the same
//! reason — and A, once the last page is in, moves its watermark for C to
//! what B had read. The claim is true because a register B holds for one
//! of C's entries is that entry or a write that beat it, and the merge
//! keeps whichever wins.
//!
//! **Only for an origin B is not hearing.** An origin B hears and A does
//! not is one A is behind on for as long as it writes, so relaying it
//! would be every register on every round of a one-way partition; A
//! reads that origin itself once it can, as it always did. An origin
//! nobody hears writes nothing more, which is what makes its relay a
//! repair that happens once rather than a resynchronisation that never
//! stops. See [`Replication::Relay`].
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
//! a stronger reason than the watermark: **a node broadcasts and batches
//! only the log it originated**, so a register it merely holds is offered
//! to nobody except in a snapshot — the join snapshot or the relay — and
//! a snapshot holds whatever tombstones the sender has not collected. A
//! sender collects a tombstone only once every node its writer knew of
//! had read past it, so a node that missed a delete is a node that holds
//! the watermark below it and nothing is collected.
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
use storage::{ClockRecord, LoggedRecord, MetadataKeyRecord, MetadataRecord, Record, SeenRecord};

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
    /// One page of the sender's registers, for an asker behind the
    /// sender's log truncation. **Section 8.2's join snapshot.**
    ///
    /// A log every known node has read is truncated, so a node that was
    /// not known when that happened — one joining the cluster, or one an
    /// operator removed and brought back — asks for entries that no
    /// longer exist. The answer to such a digest is the registers
    /// themselves rather than the log: every register the sender holds,
    /// whoever wrote it, tombstones included, in key order, a page at a
    /// time. The receiver merges them exactly as it merges a broadcast,
    /// and once the last page has arrived it moves its watermark to
    /// [`Self::Snapshot::through`] and reads the log above that like
    /// any other peer.
    ///
    /// A page rather than the lot, for [`Self::Batch`]'s reason: a
    /// retained message can be a quarter of a megabyte and the frame
    /// has a bound.
    ///
    /// **Added after 0.7.0.** A node from before it will not decode
    /// this frame and closes the link with a reason; it is only ever
    /// sent to a node whose watermark is behind a truncation, which an
    /// older node reaches only by joining an upgraded cluster fresh.
    Snapshot {
        /// The registers on this page, in key order.
        registers: Vec<MetadataRecord>,
        /// The last sequence of the sender's log that collection
        /// removed, which is where the receiver's watermark may move to
        /// once the last page has arrived.
        through: u64,
        /// Whether this is the last page.
        last: bool,
    },
    /// "Send me the registers after this key": the next page of a
    /// [`Self::Snapshot`].
    ///
    /// Sent by the receiver of a page that was not the last, naming the
    /// last key it was sent. The sender keeps no cursor and the receiver
    /// keeps none either: a page lost on the way ends the round, and the
    /// next scheduled exchange starts the snapshot again from the
    /// digest, which is what a repair that happens once per node per
    /// truncation can afford.
    SnapshotFrom {
        /// The last key already received.
        after: MetadataKeyRecord,
    },
    /// "This is how far I hold every log I know of; send me what you
    /// hold of the ones you cannot hear either." **Section 8.2's relay.**
    ///
    /// The digest above is about the receiver's own log, which is all a
    /// node used to be able to serve, so a write that reached one peer
    /// and missed another could be repaired only by the node that made
    /// it — and once that node was dead, never, although the first peer
    /// held it. Section 8.2's per-node version counters say any node may
    /// serve what it holds of any origin; this is how it is asked.
    ///
    /// Sent with every digest, to the same peer. **Answered only for an
    /// origin the receiver is not hearing from** and has read further of
    /// than the sender says it has; an origin the sender does not name
    /// counts as one it holds nothing of, which is what a node that
    /// joined after a member died looks like. Nothing to relay is no
    /// answer at all, which is the ordinary case.
    ///
    /// **Added after 0.8.0.** A node from before it will not decode this
    /// frame and closes the link, and this one is sent on every round,
    /// so every node of a cluster must run a build that has it.
    Relay {
        /// How far the sender has read each log it knows of, its own
        /// and the receiver's left out.
        have: Vec<SeenRecord>,
    },
    /// One page of the sender's registers, for an asker behind on
    /// origins neither of them can hear.
    ///
    /// The same pages as [`Self::Snapshot`] — every register the sender
    /// holds, whoever wrote it, tombstones included, in key order — and
    /// for the same reason: the sender holds the registers rather than
    /// the other node's log, and a register it holds for an entry is the
    /// entry or a write that beat it. What differs is what the last page
    /// lets the asker claim, which is [`Self::RelayPage::through`].
    ///
    /// **Added after 0.8.0**; see [`Self::Relay`].
    RelayPage {
        /// The registers on this page, in key order.
        registers: Vec<MetadataRecord>,
        /// How far the sender had read each relayed origin's log **when
        /// the relay began**, which is where the asker's watermarks may
        /// move once the last page has arrived.
        ///
        /// Fixed at the first page and carried through every page and
        /// every [`Self::RelayFrom`], because neither side keeps a
        /// cursor: an entry the sender took while the pages were going
        /// out may sit behind a key already sent, and a watermark read
        /// at the last page would claim it.
        through: Vec<SeenRecord>,
        /// Whether this is the last page.
        last: bool,
    },
    /// "Send me the registers after this key": the next page of a
    /// [`Self::RelayPage`], carrying the watermarks the first page gave.
    ///
    /// **Added after 0.8.0**; see [`Self::Relay`].
    RelayFrom {
        /// The last key already received.
        after: MetadataKeyRecord,
        /// What the first page said, handed back.
        through: Vec<SeenRecord>,
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
    /// An entry's stamp is further ahead of this node's clock than
    /// section 8.2's drift bound allows.
    ///
    /// Refused rather than clamped, and the whole frame with it: taking
    /// the reading would advance this node's clock to it, and every
    /// write this node made afterwards would carry the wrong clock on to
    /// every node it reached. See [`crate::clock`].
    #[error("a peer's write is refused for its clock: {0}")]
    Clock(#[from] crate::clock::ClockError),
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

    /// What to ask `peer` about the logs of every other node: section
    /// 8.2's relay. See [`Replication::Relay`].
    ///
    /// Every log this node has read anything of, its own and `peer`'s
    /// left out: `peer`'s is the digest's question, and nobody holds more
    /// of this node's than this node does.
    #[must_use]
    pub fn relay_for(&self, peer: NodeId) -> Replication {
        let me = self.replicated.me();
        Replication::Relay {
            have: self
                .replicated
                .seen()
                .into_iter()
                .filter(|(origin, _)| *origin != me && *origin != peer)
                .map(|(origin, through)| SeenRecord {
                    node: origin.to_bytes(),
                    through,
                })
                .collect(),
        }
    }

    /// One round of section 8.2's exchange with `peer`: the digest about
    /// its log, and the relay about everybody else's.
    ///
    /// Both to the same peer and in that order. The digest is answered
    /// from the peer's log and the relay from its registers, so neither
    /// answer depends on the other and a peer with nothing to say to
    /// either sends nothing back.
    #[must_use]
    pub fn asks_for(&self, peer: NodeId) -> Vec<Replication> {
        vec![self.digest_for(peer), self.relay_for(peer)]
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
                let before = self.replicated.seen_through(from);
                let mut handled = self.take(from, entries)?;
                // Asked again only where the peer said there is more, so a
                // round against a peer that is up to date is one frame and
                // the answer to it, and never a third. **And only where
                // the watermark moved.** A batch that begins above what
                // this node has read is kept for what it holds but moves
                // nothing, and a digest sent now would say the same thing
                // and bring the same batch back, at link speed, for as
                // long as that was true. The round ends instead and the
                // next scheduled exchange asks again.
                if more && self.replicated.seen_through(from) > before {
                    handled.reply = Some(self.digest_for(from));
                }
                Ok(handled)
            }
            Replication::Snapshot {
                registers,
                through,
                last,
            } => self.take_snapshot(from, registers, through, last),
            Replication::SnapshotFrom { after } => {
                Ok(self.snapshot_page(Some(&MetadataKey::from_record(after))))
            }
            Replication::Relay { have } => Ok(self.relay(from, &have)),
            Replication::RelayPage {
                registers,
                through,
                last,
            } => self.take_relayed(registers, through, last),
            Replication::RelayFrom { after, through } => Ok(Self::relay_page_reply(
                self.relay_page(Some(&MetadataKey::from_record(after)), through),
            )),
        }
    }

    /// Answer `from`'s relay: the first page of this node's registers,
    /// if there is an origin to relay, and nothing otherwise.
    ///
    /// **An origin is relayed only if this node is not hearing it**, and
    /// that is the bound on what the relay costs rather than a detail.
    /// A node that hears an origin the asker does not is ahead of the
    /// asker for as long as the origin keeps writing, so relaying it
    /// would be every register on every round of a one-way partition;
    /// the asker reads that origin itself once it can, as it always has.
    /// An origin nobody hears writes nothing more, so its relay happens
    /// once per node that missed something, and then the two agree.
    ///
    /// Never this node's own log, which the digest serves, and never the
    /// asker's, which nobody holds more of than the asker.
    fn relay(&self, from: NodeId, have: &[SeenRecord]) -> Handled {
        let me = self.replicated.me();
        let liveness = self.replicated.liveness();
        let asked: BTreeMap<NodeId, u64> = have
            .iter()
            .map(|seen| (NodeId::from_bytes(seen.node), seen.through))
            .collect();
        let through: Vec<SeenRecord> = self
            .replicated
            .seen()
            .into_iter()
            .filter(|(origin, held)| {
                *origin != me
                    && *origin != from
                    && !liveness.is_answering(*origin)
                    && *held > asked.get(origin).copied().unwrap_or(0)
            })
            .map(|(origin, through)| SeenRecord {
                node: origin.to_bytes(),
                through,
            })
            .collect();
        if through.is_empty() {
            return Handled::default();
        }
        Self::relay_page_reply(self.relay_page(None, through))
    }

    /// One page of this node's registers after `after`, relayed under
    /// `through`. See [`Replication::RelayPage`].
    fn relay_page(&self, after: Option<&MetadataKey>, through: Vec<SeenRecord>) -> Replication {
        let batch = self.settings.batch.max(1);
        // One more than the page, which is how the last page knows it is.
        let mut registers = self.replicated.registers_after(after, batch + 1);
        let last = registers.len() <= batch;
        registers.truncate(batch);
        Replication::RelayPage {
            registers: registers.iter().map(MetadataWrite::to_record).collect(),
            through,
            last,
        }
    }

    /// A page, as the reply to whatever asked for it.
    fn relay_page_reply(page: Replication) -> Handled {
        Handled {
            reply: Some(page),
            ..Handled::default()
        }
    }

    /// Take a relayed page, and ask for the next one.
    ///
    /// Decoded and refused before anything is merged, as
    /// [`Self::take_snapshot`] does and for the same reason. The last
    /// page ends the round: what the relay was about is now held, and
    /// the digest that went with it is answered on its own.
    fn take_relayed(
        &self,
        registers: Vec<MetadataRecord>,
        through: Vec<SeenRecord>,
        last: bool,
    ) -> Result<Handled, ReplicationError> {
        let wall = crate::clock::wall_millis();
        let clock = self.replicated.clock();
        let mut writes = Vec::with_capacity(registers.len());
        for record in registers {
            let write = MetadataWrite::from_record(record)?;
            clock.check(wall, write.stamp().reading())?;
            writes.push(write);
        }
        let continue_from = (!last)
            .then(|| writes.last().map(|write| write.key().to_record()))
            .flatten();
        let watermarks: Vec<(NodeId, u64)> = through
            .iter()
            .map(|seen| (NodeId::from_bytes(seen.node), seen.through))
            .collect();
        let Taken {
            applied, records, ..
        } = self
            .replicated
            .observed_relayed(writes, last.then_some(watermarks.as_slice()));
        Ok(Handled {
            records,
            applied,
            reply: continue_from.map(|after| Replication::RelayFrom { after, through }),
        })
    }

    /// One page of this node's registers, after `after`, for a peer
    /// behind this node's log truncation. See [`Replication::Snapshot`].
    fn snapshot_page(&self, after: Option<&MetadataKey>) -> Handled {
        let batch = self.settings.batch.max(1);
        // One more than the page, which is how the last page knows it is.
        let mut registers = self.replicated.registers_after(after, batch + 1);
        let last = registers.len() <= batch;
        registers.truncate(batch);
        Handled {
            reply: Some(Replication::Snapshot {
                registers: registers.iter().map(MetadataWrite::to_record).collect(),
                through: self.replicated.log_collected_through(),
                last,
            }),
            ..Handled::default()
        }
    }

    /// Take a page of `from`'s registers, and ask for the next or carry
    /// on with the log.
    ///
    /// Decoded and refused before anything is merged, as [`Self::take`]
    /// does for a batch and for the same reason.
    fn take_snapshot(
        &self,
        from: NodeId,
        registers: Vec<MetadataRecord>,
        through: u64,
        last: bool,
    ) -> Result<Handled, ReplicationError> {
        let wall = crate::clock::wall_millis();
        let clock = self.replicated.clock();
        let mut writes = Vec::with_capacity(registers.len());
        for record in registers {
            let write = MetadataWrite::from_record(record)?;
            clock.check(wall, write.stamp().reading())?;
            writes.push(write);
        }
        let continue_from = (!last)
            .then(|| writes.last().map(|write| write.key().to_record()))
            .flatten();
        let before = self.replicated.seen_through(from);
        let Taken {
            applied, records, ..
        } = self
            .replicated
            .observed_snapshot(from, writes, last.then_some(through));
        // The next page, or — once the last has landed and the watermark
        // has moved — the log above the truncation, asked for the
        // ordinary way. A last page that moved nothing ends the round,
        // for the reason a batch that moved nothing does: the same
        // digest would bring the same page back.
        let reply = match continue_from {
            Some(after) => Some(Replication::SnapshotFrom { after }),
            None if self.replicated.seen_through(from) > before => Some(self.digest_for(from)),
            None => None,
        };
        Ok(Handled {
            records,
            applied,
            reply,
        })
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

        // Section 8.2's join snapshot: a peer asking for a stretch of the
        // log that collection has removed cannot be answered from the
        // log. It is a node that was not known when the truncation
        // happened — joining, or removed and brought back — and before
        // this it was sent the entries above the truncation, which it
        // could not count past, or nothing at all; either way it never
        // held what the truncated entries said. See
        // [`Replication::Snapshot`].
        if have < self.replicated.log_collected_through() {
            return self.snapshot_page(None);
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
        // One wall-clock read for the batch, so that every entry is
        // judged against the same moment and the refusal names it.
        let wall = crate::clock::wall_millis();
        let clock = self.replicated.clock();
        for record in entries {
            if record.sequence == 0 {
                return Err(ReplicationError::SequenceZero);
            }
            let entry = Logged::from_record(record)?;
            // Section 8.2: a peer further ahead than the bound is
            // refused, not clamped. Checked here, before the merge,
            // for every entry, so that a batch is refused whole and the
            // clock has taken nothing from it; the merge below is what
            // then observes each reading.
            clock.check(wall, entry.write().stamp().reading())?;
            logged.push(entry);
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
