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

pub mod record;
pub mod restore;

pub use record::{
    BodyRecord, DeliveryRecord, FormatError, FormatMarker, InflightRecord, InflightState,
    MessageId, ProtocolVersion, Qos, QueueEntry, Record, RetainHandling, RetainedRecord,
    SessionKey, SessionRecord, SubscriptionRecord, WillRecord, FORMAT_MAGIC, FORMAT_VERSION,
};
pub use restore::{Restored, RestoredSession};

/// How hard a flush tries.
///
/// Section 7.2 makes this configurable so that an operator can trade
/// durability for throughput deliberately, rather than discovering the
/// trade after a power cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// [`Storage::flush`] does not return until the writes are on the
    /// device. The default, because section 7.3 sends an acknowledgement
    /// only after the commit is durable, and anything weaker would make
    /// that promise to a publisher without keeping it.
    #[default]
    Immediate,
    /// [`Storage::flush`] returns once the writes are handed to the
    /// engine, without waiting for the device.
    ///
    /// A commit made this way survives the broker exiting and may not
    /// survive the machine losing power: a backend is permitted to roll
    /// back to the last immediate flush. So this is for a deployment that
    /// has decided it would rather lose a second of at-least-once traffic
    /// than pay for a synchronous write, and for tests.
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
    /// The underlying device or file system failed.
    #[error("storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
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
            other @ (FormatError::Undecodable(_) | FormatError::Trailing { .. }) => {
                Self::Corrupt(other)
            }
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
