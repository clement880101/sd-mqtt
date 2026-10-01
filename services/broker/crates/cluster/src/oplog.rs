//! A gapless log of what this node originated.
//!
//! Section 8.2: **the anti-entropy digest is a claim about an operation
//! log, not about the records.** A register's most recent writer changes
//! every time another node overwrites it, so "everything after sequence N
//! from node X" cannot be answered by looking at the registers — the
//! register that was X's seventh write may now be Y's fourth. So each
//! node keeps a log of the writes it originated, numbered from one, and
//! that log is what a peer asks about.
//!
//! This is that log. It holds whole writes rather than keys, for the
//! reason [`storage::LoggedRecord`] gives: a log of keys has entries that
//! resolve to whatever won later, or to nothing once a tombstone has been
//! collected, and a peer asking for one gets silence where the log
//! promised an answer.
//!
//! # Gapless is a property of what a peer can see
//!
//! A peer may ask for everything after a sequence, so every number
//! between the one it has and the one it is told about must exist.
//! [`OpLog::published_through`] is what a peer is told, and it is the
//! **highest contiguous published sequence** rather than the highest
//! published one: an entry above a hole is held back until the hole is
//! filled, because a peer served past a hole would set its watermark past
//! an entry it never received and would never be sent it again.
//!
//! # Two phases, because a broadcast is irrevocable
//!
//! A write is stamped and numbered before the store has accepted the
//! batch that carries it. That was recorded as a known limitation in
//! [`crate::metadata`] while nothing showed the stamp to anybody, and the
//! argument for leaving it was explicit: closing it needs a two-phase
//! write, and that belongs with the broadcast that would expose it.
//!
//! **A broadcast is exactly what shows the stamp to somebody**, and the
//! exposure is not a window. A peer told about a register whose batch the
//! store refused holds a value this node will never hold again, and no
//! exchange repairs it: an exchange serves the origin's own log, and the
//! origin's log lost the entry in the same refused batch. Each side then
//! believes it holds the winner, which is the one divergence this design
//! has no answer to.
//!
//! So [`OpLog::reserve`] numbers a write and holds it where nothing can
//! see it; [`OpLog::publish`] is what the caller calls once the store has
//! taken the batch, and only then is the entry servable and the write
//! broadcast; [`OpLog::discard`] is the refusal. A discarded number goes
//! back in the pool, because a number that was never published was never
//! stored, never served and never mentioned to anybody — so reusing it is
//! invisible, and not reusing it would leave a hole that stalls everything
//! above it.
//!
//! What is deliberately *not* closed is the in-memory register table
//! being ahead of the store after a refusal. That is unobservable from
//! outside the node — the broker's own state went into the same refused
//! batch, so the node behaves consistently until it restarts — and
//! rolling it back would mean a second answer to which copy of a register
//! this node holds.

use std::collections::{BTreeMap, BTreeSet};

use storage::{LoggedRecord, Record};

use crate::clock::Reading;
use crate::metadata::{MetadataError, MetadataWrite};
use crate::register::Stamp;

/// The first sequence a node's log ever uses.
///
/// One, not zero, so that **zero means "nothing of yours"** in a digest
/// and in [`storage::SeenRecord::through`]. A log starting at zero would
/// make "I have your first write" and "I have nothing of yours" the same
/// claim.
pub const FIRST_SEQUENCE: u64 = 1;

/// One entry of a node's own log: where it sits, and what it wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Logged {
    /// Where in the originating node's log this sits.
    sequence: u64,
    /// What was written.
    write: MetadataWrite,
}

impl Logged {
    /// An entry from its two parts.
    #[must_use]
    pub const fn new(sequence: u64, write: MetadataWrite) -> Self {
        Self { sequence, write }
    }

    /// Where in the originating node's log this sits.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// What was written.
    #[must_use]
    pub const fn write(&self) -> &MetadataWrite {
        &self.write
    }

    /// What was written, taken.
    #[must_use]
    pub fn into_write(self) -> MetadataWrite {
        self.write
    }

    /// What a store holds this as.
    #[must_use]
    pub fn to_record(&self) -> LoggedRecord {
        LoggedRecord {
            sequence: self.sequence,
            entry: self.write.to_record(),
        }
    }

    /// An entry read back from a store.
    ///
    /// # Errors
    ///
    /// [`MetadataError`] for the reason [`MetadataWrite::from_record`]
    /// gives: a stored register whose key and value name different kinds
    /// is refused rather than guessed at.
    pub fn from_record(record: LoggedRecord) -> Result<Self, MetadataError> {
        Ok(Self {
            sequence: record.sequence,
            write: MetadataWrite::from_record(record.entry)?,
        })
    }
}

/// This node's own operation log.
///
/// Not behind a lock of its own: it lives inside [`crate::Replicated`]'s
/// state, because every change to it happens with a register's change and
/// the two must not be separable. See that type for why one lock.
#[derive(Debug, Default)]
pub struct OpLog {
    /// Published entries, by sequence. What a peer may be served.
    entries: BTreeMap<u64, MetadataWrite>,
    /// Numbered and not yet published: see the module documentation.
    held: BTreeMap<u64, MetadataWrite>,
    /// The highest sequence ever handed out, published or not.
    highest: u64,
    /// Numbers handed out and then discarded, free to be used again.
    ///
    /// A discarded number was never published and so was never stored,
    /// served or mentioned, which is what makes reuse invisible. Reusing
    /// it is not an optimisation: a number left spent is a hole, and
    /// [`Self::published_through`] stops below a hole, so every entry
    /// above it would be unreachable until another write happened to
    /// land on exactly that number.
    free: BTreeSet<u64>,
    /// The highest contiguous published sequence. What a peer is told.
    published_through: u64,
    /// The last sequence collection removed, so that the contiguous run
    /// is measured from the right place.
    collected_through: u64,
}

impl OpLog {
    /// A log resumed from what the store held.
    ///
    /// `collected_through` is
    /// [`storage::Restored::oplog_collected_through`] and is **not derived
    /// from `stored`**: collection can empty the log entirely, and a node
    /// that then numbered its next write one would re-issue a sequence
    /// every peer had already been served — which is the same defect as a
    /// clock resuming from zero, in the other of section 8.2's two
    /// counters. The entries run from there to the top, so the highest
    /// number ever used is that watermark or the highest entry, whichever
    /// is larger, and there is nothing else to read.
    ///
    /// A hole in `stored` is possible and is not repaired here. It is
    /// what a refused batch leaves behind when a later write's batch was
    /// taken, and [`Self::published_through`] stops below it, which is
    /// the honest answer: this node cannot serve what it does not have.
    /// The hole closes as soon as a write is numbered into it, which is
    /// the next write this node makes.
    #[must_use]
    pub fn resume(stored: Vec<Logged>, collected_through: u64) -> Self {
        let entries: BTreeMap<u64, MetadataWrite> = stored
            .into_iter()
            .map(|logged| (logged.sequence, logged.write))
            .filter(|(sequence, _)| *sequence > collected_through)
            .collect();
        let highest = std::cmp::max(
            collected_through,
            entries.keys().next_back().copied().unwrap_or_default(),
        );
        // Every number below the high-water mark that the store does not
        // hold and collection did not take is free: it was handed out and
        // its batch was refused, or it was never handed out at all.
        let free = (collected_through + 1..=highest)
            .filter(|sequence| !entries.contains_key(sequence))
            .collect();
        let mut log = Self {
            entries,
            held: BTreeMap::new(),
            highest,
            free,
            published_through: collected_through,
            collected_through,
        };
        log.advance();
        log
    }

    /// Number `write` and hold it where nothing can see it.
    ///
    /// The number is the lowest free one, which is the next unused number
    /// in the ordinary case. See the module documentation for the two
    /// phases and [`Self::free`] for why a discarded number comes back.
    pub fn reserve(&mut self, write: MetadataWrite) -> u64 {
        let sequence = match self.free.iter().next().copied() {
            Some(lowest) => {
                self.free.remove(&lowest);
                lowest
            }
            None => {
                self.highest += 1;
                self.highest
            }
        };
        self.held.insert(sequence, write);
        sequence
    }

    /// The number a reserved write is held under, by its stamp.
    ///
    /// A stamp identifies a write uniquely — it is a reading and the one
    /// node that issued it, and a clock cannot issue a reading twice — so
    /// this is how a caller that holds a write finds the sequence it was
    /// numbered with, without the write having to carry one.
    ///
    /// A scan, because what is held is what is in flight to the store:
    /// one write on the ordinary path and a handful under load, against a
    /// hash map that would have to be kept in step with the two others
    /// here.
    #[must_use]
    pub fn held_at(&self, stamp: Stamp) -> Option<u64> {
        self.held
            .iter()
            .find(|(_, write)| write.stamp() == stamp)
            .map(|(sequence, _)| *sequence)
    }

    /// The store took the batch: the entry is servable from now on.
    ///
    /// Returns the entry, or `None` for a sequence this log is not
    /// holding — which is a caller publishing something twice, and the
    /// answer that keeps the log honest is to say nothing happened.
    pub fn publish(&mut self, sequence: u64) -> Option<Logged> {
        let write = self.held.remove(&sequence)?;
        self.entries.insert(sequence, write.clone());
        self.advance();
        Some(Logged::new(sequence, write))
    }

    /// The store refused the batch: the entry never existed.
    ///
    /// Returns whether there was one. The number goes back in the pool;
    /// see [`Self::free`].
    pub fn discard(&mut self, sequence: u64) -> bool {
        if self.held.remove(&sequence).is_none() {
            return false;
        }
        if sequence > self.collected_through {
            self.free.insert(sequence);
        }
        true
    }

    /// The highest contiguous published sequence.
    ///
    /// **What a peer is told, and what a peer may ask past.** Zero is a
    /// node that has published nothing.
    #[must_use]
    pub const fn published_through(&self) -> u64 {
        self.published_through
    }

    /// The highest sequence ever handed out, published or not.
    ///
    /// What a store must hold so that [`Self::resume`] does not hand one
    /// out twice.
    #[must_use]
    pub const fn high_water(&self) -> u64 {
        self.highest
    }

    /// How many published entries are held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether any published entry is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Up to `limit` entries after `sequence`, in order.
    ///
    /// Never past [`Self::published_through`], which is what makes a
    /// batch something a peer can count on: it may advance its watermark
    /// to the last sequence it was sent, because everything below that is
    /// either in the batch or already its.
    ///
    /// An empty answer means one of three things and the caller cannot
    /// tell them apart, which is correct: the peer is up to date, the
    /// entries it is missing have been collected because every node had
    /// them, or `limit` was zero.
    #[must_use]
    pub fn after(&self, sequence: u64, limit: usize) -> Vec<Logged> {
        let from = sequence.saturating_add(1);
        if from > self.published_through {
            // Said before the range rather than inside it, because a
            // `BTreeMap` range whose start is past its end panics rather
            // than being empty, and a peer that is up to date asks for
            // exactly that range on every exchange.
            return Vec::new();
        }
        self.entries
            .range(from..=self.published_through)
            .take(limit)
            .map(|(sequence, write)| Logged::new(*sequence, write.clone()))
            .collect()
    }

    /// The clock reading the entry at `sequence` carries.
    ///
    /// What section 8.2's tombstone watermark is expressed in: a node
    /// knows how far along its own log every peer has read, and this
    /// turns that number into the reading below which its own writes have
    /// reached everybody.
    #[must_use]
    pub fn reading_at(&self, sequence: u64) -> Option<Reading> {
        self.entries
            .get(&sequence)
            .map(|write| write.stamp().reading())
    }

    /// Remove every entry up to and including `sequence`.
    ///
    /// Section 8.2's second watermark, the one distinct from the
    /// tombstone one: nothing will ever ask for an entry every node
    /// already holds. Returns how many went.
    ///
    /// **The high-water mark is untouched**, because the numbers are
    /// spent; see [`Self::resume`].
    pub fn collect_through(&mut self, sequence: u64) -> usize {
        let before = self.entries.len();
        self.entries.retain(|held, _| *held > sequence);
        // **Clamped to what this node has ever numbered.** A watermark is
        // the lowest sequence every node has read, and a node that has
        // read everything reports this node's own high-water mark; a
        // number past that is a peer's arithmetic rather than a fact about
        // this log, and taking it literally would make the next write
        // claim a sequence nothing had ever reached.
        let sequence = std::cmp::min(sequence, self.highest);
        if sequence > self.collected_through {
            self.collected_through = sequence;
            self.published_through = std::cmp::max(self.published_through, sequence);
            // A number below the collection point can never be asked
            // for, so holding it free would only let a later write land
            // where nothing would ever read it.
            self.free.retain(|free| *free > sequence);
            self.advance();
        }
        before - self.entries.len()
    }

    /// What a store must be given to append `entry`.
    #[must_use]
    pub fn record(entry: &Logged) -> Record {
        Record::OpLogAppended {
            entry: entry.to_record(),
        }
    }

    /// Move [`Self::published_through`] up over whatever is now
    /// contiguous.
    ///
    /// Stops at the first number this log does not hold, which is the
    /// whole of "gapless is a property of what a peer can see".
    fn advance(&mut self) {
        while self.entries.contains_key(&(self.published_through + 1)) {
            self.published_through += 1;
        }
    }
}

#[cfg(test)]
mod tests;
