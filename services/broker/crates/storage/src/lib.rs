//! Durable storage for the broker.
//!
//! Nothing in the broker knows this crate exists yet. That is deliberate:
//! section 7.6 of the design builds one real backend and keeps the trait
//! honest with an in-memory one, and both are proved on their own before
//! anything depends on them.
//!
//! The trait is written as though a second backend existed, because
//! comparing two engines is what found the head-read problem in the first
//! place. Concretely, and each of these is a decision rather than a style:
//! no backend's types appear in a signature; the head read is expressed as
//! "at or after this cursor" rather than "the first entry"; and queue depth
//! is a stored counter rather than the engine's own length.

use std::path::PathBuf;

pub mod memory;
pub mod record;
/// The redb backend, behind the `redb` feature.
///
/// Off for `broker-core`, which is the pure crate: it needs the record
/// vocabulary and nothing that touches a disk. On everywhere else.
#[cfg(feature = "redb")]
pub mod redb;
pub mod restore;
pub mod writer;

#[cfg(feature = "redb")]
pub use self::redb::RedbStorage;
pub use memory::MemoryStorage;

pub use record::{
    BodyRecord, DeliveryRecord, FormatError, FormatMarker, InflightRecord, InflightState,
    MessageId, ProtocolVersion, Qos, QueueEntry, Record, RetainHandling, RetainedRecord,
    SessionKey, SessionRecord, SubscriptionRecord, WillRecord, FORMAT_MAGIC, FORMAT_VERSION,
};
pub use restore::{Restored, RestoredSession};
pub use writer::{Applied, Durable, NotDurable, Writer, WriterConfig, WriterStopped};

/// How hard a flush tries.
///
/// Section 7.2 makes this configurable so that an operator can trade
/// durability for throughput deliberately, rather than discovering the
/// trade after a power cut.
///
/// # Where this is chosen
///
/// **On the store, at construction**, not on [`Storage::flush`]. Two
/// reasons, and the second is the one that decided it.
///
/// It is a property of a deployment rather than of a call. An operator
/// decides once whether this node's acknowledgements are worth a
/// synchronous write; a per-call argument would let any caller weaken that
/// for one batch, which is exactly the decision nobody should be making at
/// a call site.
///
/// And [`Storage::flush`] is what the batching writer calls, from one
/// place, on behalf of waiters that did not choose anything. There is no
/// caller who would pass a different value, so an argument would only be a
/// way to get it wrong.
///
/// A backend that cannot honour a mode must say so rather than accept it
/// silently; what redb does with [`SyncMode::Relaxed`] is on that variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// [`Storage::flush`] does not return until the writes are on the
    /// device. The default, because section 7.3 sends an acknowledgement
    /// only after the commit is durable, and anything weaker would make
    /// that promise to a publisher without keeping it.
    #[default]
    Immediate,
    /// [`Storage::flush`] returns without waiting for the device.
    ///
    /// What that is worth depends on the backend, and **on redb it is
    /// worth nothing at all**. redb has two durability levels and the
    /// weaker one keeps the commit in this process's memory: a flush made
    /// this way is atomic and visible to every later read, and it reaches
    /// no file. A clean shutdown writes it out, so a broker that exits
    /// normally loses nothing; a broker that is killed loses everything
    /// since its last immediate flush, however long ago that was, because
    /// under this mode there has never been one.
    ///
    /// So this is for tests and for measuring what the durable path costs,
    /// and not a durability trade an operator should be offered against
    /// this backend. A backend with a third level — written to the file,
    /// not yet synced — could keep the weaker promise honestly, which is
    /// why the mode exists rather than being deleted.
    Relaxed,
}

/// What can go wrong in a store.
///
/// The four causes are separate on purpose. An operator's response to each
/// is different: a version that is not ours means run the right build, a
/// corrupt store means restore a backup, a locked store means find the
/// other process, and an I/O failure means look at the disk. A single
/// opaque error would collapse them, and would make the version check
/// worthless, because "refuses to start" is only actionable if it says
/// which of these it is.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The store was written by a format this build does not know.
    ///
    /// Not a corruption. The file is intact and is being read by the wrong
    /// build, which is the case that would otherwise be silent: guessing
    /// at an unknown layout restores nonsense from healthy bytes.
    #[error("store format version {found} is not readable by this build, which reads version {expected}")]
    UnsupportedVersion {
        /// The version the store carries.
        found: u16,
        /// The version this build reads, [`FORMAT_VERSION`].
        expected: u16,
    },
    /// The path holds something that is not an sd-mqtt store at all.
    #[error("not an sd-mqtt store")]
    NotAStore,
    /// Stored bytes did not decode.
    #[error("the store is corrupt: {0}")]
    Corrupt(#[source] FormatError),
    /// Another process holds the store.
    #[error("the store at {path} is held by another process")]
    Locked {
        /// Where the store is.
        path: PathBuf,
    },
    /// A record contradicted what the store already holds, and the batch
    /// carrying it was applied in full or not at all, which means not at
    /// all.
    #[error("the store refused a record: {0}")]
    Rejected(#[from] Rejection),
    /// The underlying device or file system failed.
    #[error("storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// The storage engine failed for a reason that is none of the above.
    ///
    /// A catch-all, and deliberately the last resort rather than the first
    /// one. The five causes above are separated because an operator's
    /// response to each differs, and collapsing a sixth kind of failure
    /// into one of them would be worse than admitting it is a sixth: a
    /// poisoned lock inside the engine is not a disk that failed, and
    /// reporting it as one sends an operator to look at the wrong thing.
    #[error("the storage engine failed: {0}")]
    Engine(String),
}

/// Why a store refused a record.
///
/// Separate from the rest of [`StorageError`] because these are the
/// caller's mistakes rather than the disk's, and separate from each other
/// because convention 2 says a test asserting a category is not guarding
/// the check it is named after: `Rejected(_)` would pass for any of the
/// three.
///
/// Each of these is a fact the store cannot record without losing
/// something it already holds, so refusing is the only honest answer.
/// Everything a caller might plausibly repeat — reserving an inbound
/// identifier twice, popping a sequence already popped, removing a session
/// that is gone, unsubscribing from a filter it does not hold — is a
/// no-op instead, because for those the postcondition the record asks for
/// already holds.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Rejection {
    /// A record named a session the store does not hold.
    ///
    /// Accepting it would put a queue, a subscription or an inflight entry
    /// somewhere [`Storage::restore`] does not look, because restore walks
    /// sessions. The state would be neither stored nor reported missing,
    /// which is the one outcome worse than an error.
    #[error("no session is held for client {:?} at mountpoint {:?}", .session.client_id, .session.mountpoint)]
    NoSuchSession {
        /// Which session was named.
        session: SessionKey,
    },
    /// A queue push reused a sequence number.
    ///
    /// Section 7.4: two entries sharing a number, or a number going
    /// backwards, is what must never happen. The store holds the
    /// high-water mark precisely so it can say no.
    #[error(
        "sequence {sequence} was already assigned to client {:?} at mountpoint {:?}, whose highest is {highest}",
        .session.client_id,
        .session.mountpoint
    )]
    SequenceReused {
        /// Whose queue.
        session: SessionKey,
        /// The number the push asked for.
        sequence: u64,
        /// The highest ever assigned to that session.
        highest: u64,
    },
    /// A body was released more often than it was stored.
    ///
    /// The reference count is the only thing standing between a shared
    /// body and a queue entry that names nothing, so a release with no
    /// matching put is refused rather than clamped at zero. Clamping would
    /// turn a caller's double release into a body deleted underneath a
    /// live reference, which is a message lost and a restore that cannot
    /// complete.
    #[error("body {:?} was released more often than it was stored", .id.0)]
    BodyNotHeld {
        /// Which body.
        id: MessageId,
    },
}

impl From<FormatError> for StorageError {
    /// Keep the version case separate from the corruption cases.
    ///
    /// This is the whole point of having more than one variant: a store
    /// from a later build is healthy and must say so, or an operator
    /// reaches for a backup they do not need.
    fn from(error: FormatError) -> Self {
        match error {
            FormatError::UnknownVersion { found, expected } => {
                Self::UnsupportedVersion { found, expected }
            }
            FormatError::NotAStore => Self::NotAStore,
            other @ (FormatError::Undecodable(_)
            | FormatError::Trailing { .. }
            | FormatError::Damaged(_)) => Self::Corrupt(other),
        }
    }
}

/// A durable store for everything the broker must not lose.
///
/// Implementations are shared across threads: section 7.3 has the shell
/// committing a journal from whichever task filled it, and the batching
/// writer commits from a thread of its own.
pub trait Storage: Send + Sync + 'static {
    /// Apply a batch. Atomic: all of it or none of it.
    ///
    /// Not durable until [`Storage::flush`] returns, so a caller that
    /// promised something must flush. Section 7.3: an acknowledgement says
    /// the broker has taken responsibility for a message, so sending one
    /// before the flush returns means a restart can lose a message the
    /// publisher was told was safe.
    ///
    /// Records take effect in order, so a batch may create a session and
    /// then fill it. A batch that returns an error has changed nothing,
    /// including the records before the one that failed. [`Rejection`]
    /// lists what is refused and what is a no-op instead.
    ///
    /// # References to a body are taken and released explicitly
    ///
    /// [`Record::BodyPut`] takes one reference and
    /// [`Record::BodyReleased`] drops one; **no other record changes the
    /// count.** In particular a [`Record::QueuePopped`] does not release
    /// the body its entry named, because the usual reason to pop is to
    /// send the message, and for anything above at-most-once the
    /// [`Record::InflightPut`] that follows names the same body. A store
    /// that released on pop would delete it between the two. By the same
    /// argument a [`Record::SessionRemoved`] does not release the bodies
    /// of the entries it discards: a caller that already emits the
    /// releases would then release twice, and the store cannot tell the
    /// two callers apart.
    ///
    /// The cost of that choice is that a caller which drops a queue entry
    /// without releasing its body leaks the body, and nothing reclaims it.
    /// That is a leak rather than a loss, and it is the direction to err
    /// in.
    ///
    /// # A body is what the first put said it was
    ///
    /// An identifier names one publication for as long as it exists, so
    /// **a [`Record::BodyPut`] for an identifier the store already holds
    /// takes a reference and nothing else**: the body it carries is
    /// ignored, because it is the body already held. Both backends do
    /// this, and it is what lets the redb one answer the second and
    /// thousandth reference to a message without reading, decoding or
    /// rewriting its payload — the cost a fan-out pays once per
    /// subscriber otherwise, on the one storage thread, inside one write
    /// transaction.
    ///
    /// A caller that wants different content stores it under a different
    /// identifier, which is what minting one per publication already
    /// does.
    fn apply(&self, records: &[Record]) -> Result<(), StorageError>;

    /// Make everything applied so far durable, as hard as the configured
    /// [`SyncMode`] asks for.
    fn flush(&self) -> Result<(), StorageError>;

    /// Everything needed to rebuild the broker, read once at startup.
    ///
    /// Eager rather than streaming; the reasoning, and what would reverse
    /// it, are on [`Restored`].
    fn restore(&self) -> Result<Restored, StorageError>;

    /// The head of a session's queue, at or after `cursor`.
    ///
    /// Expressed as a cursor rather than as "the first entry" because an
    /// engine without a head primitive answers the first form with a scan
    /// over everything already deleted. Measured at 197 reads a second
    /// against 1.47 million; see the storage entry in the component
    /// research. redb needs no such workaround, and the signature is
    /// written this way anyway, because the comparison that found the
    /// problem is the reason the trait is worth having.
    ///
    /// Returns `None` when nothing is held at or after `cursor`. The
    /// entry's own sequence number may be greater than `cursor`, because
    /// delivered entries leave holes and section 7.4 says those holes are
    /// expected.
    fn queue_head(
        &self,
        session: &SessionKey,
        cursor: u64,
    ) -> Result<Option<QueueEntry>, StorageError>;

    /// How many entries a session's queue holds.
    ///
    /// A stored counter, not the engine's own length: at least one engine
    /// counts entries that have been deleted, so asking the engine would
    /// give an answer that grows as a queue drains.
    fn queue_depth(&self, session: &SessionKey) -> Result<u64, StorageError>;

    /// The highest sequence number ever assigned to this session, whether
    /// or not that entry is still held.
    ///
    /// Nothing may reuse it. Section 7.4: gapless is a property of the
    /// assignment and not of what survives, so this is the high-water mark
    /// rather than the largest key present. Zero for a session that has
    /// never been pushed to, which makes the first assigned number one.
    ///
    /// Zero again once the session is removed. The mark belongs to the
    /// session, not to the key, so a client that reconnects under the same
    /// identifier after its session was destroyed inherits nothing from
    /// it. A store that kept the mark would be the same bug as a store
    /// that kept the queue, only quieter.
    fn highest_sequence(&self, session: &SessionKey) -> Result<u64, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The trait must stay usable behind a pointer: the shell holds one
    /// store and does not know which backend it got. A change that took a
    /// generic parameter or returned an `impl Trait` would break that, and
    /// would do it at the call site rather than here.
    #[allow(dead_code)]
    fn a_store_can_be_held_as_a_trait_object(store: Box<dyn Storage>) -> Box<dyn Storage> {
        store
    }

    #[test]
    fn a_store_is_shareable_across_threads() {
        fn require<T: Send + Sync + ?Sized>() {}
        require::<dyn Storage>();
    }

    #[test]
    fn a_store_from_a_later_build_is_not_reported_as_corruption() {
        // An operator who reads "corrupt" reaches for a backup. The file
        // is not damaged; the build is wrong. Collapsing these is the
        // failure the error type exists to prevent.
        let error = StorageError::from(FormatError::UnknownVersion {
            found: 7,
            expected: FORMAT_VERSION,
        });
        match error {
            StorageError::UnsupportedVersion { found, expected } => {
                assert_eq!(found, 7);
                assert_eq!(expected, FORMAT_VERSION);
            }
            other => panic!("a version mismatch became {other:?}"),
        }
    }

    #[test]
    fn a_version_refusal_names_both_versions() {
        let message = StorageError::from(FormatError::UnknownVersion {
            found: 7,
            expected: FORMAT_VERSION,
        })
        .to_string();
        assert!(
            message.contains('7') && message.contains(&FORMAT_VERSION.to_string()),
            "said: {message}"
        );
    }

    #[test]
    fn undecodable_bytes_are_corruption_and_not_a_version_problem() {
        let inner = FormatError::Trailing { extra: 3 };
        match StorageError::from(inner.clone()) {
            StorageError::Corrupt(reported) => assert_eq!(reported, inner),
            other => panic!("corrupt bytes became {other:?}"),
        }
    }

    #[test]
    fn the_wrong_file_is_refused_as_the_wrong_file() {
        // Pointing the broker at some other file is an operator mistake
        // with an obvious fix, and is worth telling apart from a store of
        // ours that has been damaged.
        assert!(matches!(
            StorageError::from(FormatError::NotAStore),
            StorageError::NotAStore
        ));
    }

    #[test]
    fn immediate_is_the_default_sync_mode() {
        // Section 7.3 acknowledges only after a durable commit. A default
        // of anything weaker would keep the ordering and lose the promise.
        assert_eq!(SyncMode::default(), SyncMode::Immediate);
    }
}
