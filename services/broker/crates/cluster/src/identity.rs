//! Who this node is.
//!
//! Section 8.1: a node has a stable identity persisted on first start,
//! a UUID v7 and a human-readable name, and **the UUID is the identity
//! everything orders by**. The name is for operators and is never
//! compared.
//!
//! # The name is not in [`NodeId`], and that is the point
//!
//! The two places that must agree — the lowest identity owns a
//! singleton, and the highest identity breaks a tie between equal clock
//! readings — both order [`NodeId`], which carries the UUID and nothing
//! else. So "the name is never compared" is not a rule a test has to
//! guard: there is no name in the type that orders, and a comparison of
//! names in either of those places does not compile.
//!
//! That matters more than it looks. Two copied configurations share a
//! name, and an ordering that is not antisymmetric leaves both sides
//! believing they won — which is two nodes running one singleton, or
//! two nodes each discarding the other's write.
//!
//! # Minted once, and a rename does not re-found the node
//!
//! [`NodeIdentity::reconcile`] is the whole rule, and it is pure: given
//! what the store held and the name this start was configured with, it
//! says what this node is and what, if anything, must be written down.
//! A node that minted a fresh UUID on every start would stop being the
//! node its peers know, silently, on the first restart; a node that
//! refused to take a new name would make an operator's rename a lie.
//!
//! [`NodeIdentity::adopt`] is the same thing with the store attached,
//! which is what a startup calls.

use std::fmt;

use storage::{NodeIdentityRecord, Record, Storage, StorageError};
use uuid::Uuid;

/// What names a node, for as long as its store exists.
///
/// A UUID v7 and nothing else. **Ordered by its sixteen bytes**, which
/// is the one ordering that needs no locale, no normalisation and no
/// agreement beyond the bytes themselves — and, because version 7 is
/// time-ordered, is also the order the nodes were founded in.
///
/// `Copy`, because it is sixteen bytes and passing one about should not
/// read like an allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(Uuid);

impl NodeId {
    /// A fresh identifier.
    ///
    /// Version 7, so the canonical text sorts in the same order as the
    /// bytes and both sort in order of creation. Nothing relies on the
    /// last of those; it is what makes a listing of nodes readable.
    #[must_use]
    pub fn mint() -> Self {
        Self(Uuid::now_v7())
    }

    /// The identifier this UUID names.
    #[must_use]
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// The identifier these sixteen bytes name.
    ///
    /// In the order section 8.1's UUID is laid out, which is the order
    /// [`NodeId::to_bytes`] produces and the order a store holds.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// Its sixteen bytes, which are what every comparison is over.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 16] {
        *self.0.as_bytes()
    }

    /// The UUID itself.
    #[must_use]
    pub fn as_uuid(self) -> Uuid {
        self.0
    }

    /// The identifier this canonical text names.
    ///
    /// # Errors
    ///
    /// [`IdentityError::Malformed`], naming the text, because the only
    /// caller is reading something a person or a peer wrote and the
    /// answer to a refusal is to look at what they wrote.
    pub fn parse(text: &str) -> Result<Self, IdentityError> {
        Uuid::parse_str(text)
            .map(Self)
            .map_err(|source| IdentityError::Malformed {
                text: text.to_owned(),
                source,
            })
    }
}

impl fmt::Display for NodeId {
    /// The canonical lower-case hyphenated form.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.hyphenated())
    }
}

/// A node's identity: what it is, and what it is called.
///
/// The pair a peer is told about in the handshake. Deliberately not
/// `Ord`: the thing that orders is [`NodeId`], and an ordering on the
/// pair would be a second answer with a name in it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeIdentity {
    /// What it is.
    id: NodeId,
    /// What an operator calls it. Never compared.
    name: String,
}

impl NodeIdentity {
    /// An identity from its two parts.
    #[must_use]
    pub fn new(id: NodeId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
        }
    }

    /// A node founded now, under this name.
    #[must_use]
    pub fn mint(name: impl Into<String>) -> Self {
        Self::new(NodeId::mint(), name)
    }

    /// What it is.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// What an operator calls it.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The identity a store holds, as one of these.
    #[must_use]
    pub fn from_record(record: &NodeIdentityRecord) -> Self {
        Self::new(NodeId::from_bytes(record.uuid), record.name.clone())
    }

    /// This identity, as a store holds it.
    #[must_use]
    pub fn to_record(&self) -> NodeIdentityRecord {
        NodeIdentityRecord {
            uuid: self.id.to_bytes(),
            name: self.name.clone(),
        }
    }

    /// Who this node is, given what its store held and how this start
    /// was configured.
    ///
    /// Pure, and the whole rule:
    ///
    /// - **A store that holds an identity keeps its UUID.** Whatever
    ///   else changes, that does not: a node that minted a fresh one on
    ///   a restart would stop being the node its peers already know, and
    ///   would break the uniqueness conflict resolution depends on
    ///   without ever saying so.
    /// - **The configured name wins**, and is written down when it
    ///   differs from what was stored. An operator renaming a node is
    ///   renaming it, not founding a new one.
    /// - **A store that holds nothing founds a node**, which happens
    ///   once in the life of a store directory.
    ///
    /// [`Reconciled::write`] is `None` when there is nothing to say,
    /// which is the ordinary restart. Writing an identical record every
    /// start would be harmless and would also make "this store has been
    /// renamed" indistinguishable from "this store has been opened",
    /// in the one log line an operator reads about it.
    #[must_use]
    pub fn reconcile(stored: Option<&NodeIdentityRecord>, name: &str) -> Reconciled {
        match stored {
            Some(held) => {
                let identity = Self::new(NodeId::from_bytes(held.uuid), name);
                // Compared to decide whether to write, never to decide
                // which node this is.
                let renamed = held.name != name;
                Reconciled {
                    write: renamed.then(|| identity.to_record()),
                    identity,
                    founded: false,
                }
            }
            None => {
                let identity = Self::mint(name);
                Reconciled {
                    write: Some(identity.to_record()),
                    identity,
                    founded: true,
                }
            }
        }
    }

    /// The same, against a store that has already been read.
    ///
    /// What a startup calls: the store is read once, for everything, and
    /// the identity comes out of that read rather than out of a second
    /// one.
    ///
    /// **Flushed before it returns**, when anything was written. An
    /// identity that a peer has been told and the store has not is the
    /// one thing here that cannot be repaired by doing it again: the
    /// restart after the crash mints a different node.
    ///
    /// # Errors
    ///
    /// [`IdentityError::Store`] if the identity could not be written or
    /// made durable. A node that cannot record who it is must not start
    /// clustering, so this is an error rather than a warning.
    pub fn adopt(
        store: &dyn Storage,
        stored: Option<&NodeIdentityRecord>,
        name: &str,
    ) -> Result<Reconciled, IdentityError> {
        let reconciled = Self::reconcile(stored, name);
        if let Some(record) = &reconciled.write {
            store.apply(&[Record::NodeIdentityPut {
                identity: record.clone(),
            }])?;
            store.flush()?;
        }
        Ok(reconciled)
    }

    /// The same again, reading the store itself.
    ///
    /// For a caller that has no restore in hand, which in practice means
    /// a test.
    ///
    /// # Errors
    ///
    /// As [`NodeIdentity::adopt`], and [`IdentityError::Store`] if the
    /// store cannot be read.
    pub fn open(store: &dyn Storage, name: &str) -> Result<Reconciled, IdentityError> {
        let restored = store.restore()?;
        Self::adopt(store, restored.node.as_ref(), name)
    }
}

impl fmt::Display for NodeIdentity {
    /// The name and the identifier, in that order.
    ///
    /// Both, always. A log line with only the name cannot be matched
    /// against another node's view of the cluster, and one with only the
    /// identifier cannot be matched against the configuration an
    /// operator wrote.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.name, self.id)
    }
}

/// What [`NodeIdentity::reconcile`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reconciled {
    /// Who this node is.
    pub identity: NodeIdentity,
    /// What must be written down, if anything.
    pub write: Option<NodeIdentityRecord>,
    /// Whether this start founded the node.
    ///
    /// Worth a log line of its own: founding is supposed to happen once
    /// in the life of a store directory, so a node that reports it twice
    /// is a node whose store is being thrown away underneath it.
    pub founded: bool,
}

/// What can be wrong with an identity.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// The text is not a node identifier.
    #[error("{text:?} is not a node identifier: {source}")]
    Malformed {
        /// What was read.
        text: String,
        /// Why it is not one.
        source: uuid::Error,
    },
    /// The identity could not be read from or written to the store.
    #[error("this node's identity could not be stored: {0}")]
    Store(#[from] StorageError),
}

#[cfg(test)]
mod tests {
    use super::*;

    use storage::RedbStorage;

    /// A store on a directory that goes when it does.
    ///
    /// redb rather than the in-memory backend, and the tests below are
    /// the reason the crate takes a dev-dependency on it: "minted once
    /// and read back unchanged across a restart" is a claim about a
    /// file, and a backend that cannot be reopened answers it with
    /// nothing.
    struct OnDisk {
        directory: tempfile::TempDir,
    }

    impl OnDisk {
        fn new() -> Self {
            Self {
                directory: tempfile::tempdir().expect("a temporary directory"),
            }
        }

        /// Open the store. Dropping the returned value closes it, which
        /// is what makes the next call a restart.
        fn open(&self) -> RedbStorage {
            RedbStorage::open(self.directory.path().join("sdmqtt.redb")).expect("the store opens")
        }

        /// Start this node, as a startup does.
        fn start(&self, name: &str) -> Reconciled {
            let store = self.open();
            NodeIdentity::open(&store, name).expect("an identity")
        }
    }

    // -----------------------------------------------------------
    // Minted once
    // -----------------------------------------------------------

    /// The one that matters: an identity survives a restart unchanged.
    ///
    /// **The store is closed between the two starts**, so this is a
    /// restart rather than a second read of a live store. A node that
    /// minted afresh each start would pass every test that only ever
    /// looks at one process.
    #[test]
    fn an_identity_is_minted_once_and_read_back_unchanged_across_a_restart() {
        let on_disk = OnDisk::new();

        let first = on_disk.start("edge-1");
        assert!(first.founded, "the first start founds the node");
        assert!(
            first.write.is_some(),
            "and what it founded has to be written down"
        );

        let second = on_disk.start("edge-1");
        assert_eq!(
            second.identity, first.identity,
            "the second start is the same node"
        );
        assert!(!second.founded, "and it did not found it again");
        assert_eq!(second.write, None, "nothing changed, so nothing is written");

        // A third, because a node that re-founded itself every *other*
        // start would pass the two above.
        let third = on_disk.start("edge-1");
        assert_eq!(third.identity, first.identity);
        assert!(!third.founded);
    }

    /// Two store directories are two nodes.
    ///
    /// The other half of minting: an identity that was a constant, or
    /// derived from the name, or derived from anything the two share,
    /// would pass the restart test above and break the uniqueness every
    /// tie-break in section 8 depends on.
    #[test]
    fn two_nodes_with_separate_directories_get_different_identities() {
        let (one, two) = (OnDisk::new(), OnDisk::new());
        let first = one.start("edge").identity;
        let second = two.start("edge").identity;

        assert_ne!(
            first.id(),
            second.id(),
            "two nodes configured identically are still two nodes"
        );
        assert_eq!(
            first.name(),
            second.name(),
            "and they share the name, which is exactly why the name is \
             never what is compared"
        );
    }

    /// A rename moves the name and nothing else.
    ///
    /// An operator renaming a node must not re-found it. The assertion
    /// is on both fields: a store that ignored the new name would fail
    /// the first, and one that minted a fresh UUID for an unrecognised
    /// name would fail the second.
    #[test]
    fn a_rename_moves_the_name_without_moving_the_identifier() {
        let on_disk = OnDisk::new();
        let founded = on_disk.start("edge-1").identity;

        let renamed = on_disk.start("edge-one");
        assert_eq!(renamed.identity.name(), "edge-one", "the rename took");
        assert_eq!(
            renamed.identity.id(),
            founded.id(),
            "and it did not re-found the node"
        );
        assert!(!renamed.founded);
        assert_eq!(
            renamed.write,
            Some(renamed.identity.to_record()),
            "a rename is written down, or the next start undoes it"
        );

        // And it was written down: starting again under the new name
        // has nothing to say.
        let again = on_disk.start("edge-one");
        assert_eq!(again.identity, renamed.identity);
        assert_eq!(
            again.write, None,
            "the rename was stored, so this start is an ordinary one"
        );
    }

    // -----------------------------------------------------------
    // The ordering
    // -----------------------------------------------------------

    /// The order is over the UUID's bytes.
    ///
    /// Written with two identities whose names sort the opposite way,
    /// so an ordering that reached for the name would be caught — as
    /// far as it can be caught, which is not far, because [`NodeId`]
    /// holds no name for such an ordering to reach. That is the
    /// strongest version of this guard and it is why the type is
    /// shaped this way; see the module documentation.
    #[test]
    fn the_order_is_over_the_uuid_and_the_name_is_not_in_it() {
        let low = NodeIdentity::new(NodeId::from_bytes([0x01; 16]), "zulu");
        let high = NodeIdentity::new(NodeId::from_bytes([0x02; 16]), "alpha");

        assert!(low.id() < high.id());
        assert!(
            high.name() < low.name(),
            "the names sort the other way, which is the point"
        );

        let mut sorted = [high.id(), low.id()];
        sorted.sort_unstable();
        assert_eq!(sorted, [low.id(), high.id()]);
    }

    /// The bytes are the bytes, in both directions.
    ///
    /// A round trip over a value neither direction produced: these
    /// sixteen bytes are a version 7 UUID written out by hand, so a
    /// build that transposed or reordered them would be caught here
    /// rather than at a peer.
    #[test]
    fn the_sixteen_bytes_are_the_identifier() {
        let bytes = [
            0x01, 0x93, 0x4e, 0x3a, 0x7c, 0x10, 0x70, 0x00, 0x80, 0x00, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        let id = NodeId::from_bytes(bytes);
        assert_eq!(id.to_bytes(), bytes);
        assert_eq!(id.to_string(), "01934e3a-7c10-7000-8000-0a0b0c0d0e0f");
        assert_eq!(
            NodeId::parse("01934e3a-7c10-7000-8000-0a0b0c0d0e0f").expect("it parses"),
            id
        );
    }

    /// A minted identifier is a version 7 one.
    ///
    /// Section 8.1 says version 7, and the reason is in the ordering:
    /// the identity everything sorts by then sorts in order of
    /// founding. A version 4 identifier would satisfy every other test
    /// in this module.
    #[test]
    fn a_minted_identifier_is_version_seven_and_sorts_by_when_it_was_minted() {
        let first = NodeId::mint();
        // Version 7 puts the milliseconds in the leading bytes, so two
        // minted in the same millisecond may tie. Sleeping past one is
        // two milliseconds of a test's life for an assertion about the
        // ordering everything in section 8 depends on.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = NodeId::mint();

        assert_eq!(first.as_uuid().get_version_num(), 7);
        assert_eq!(second.as_uuid().get_version_num(), 7);
        assert_ne!(first, second);
        assert!(first < second, "version 7 sorts in order of creation");
    }

    /// Text that is not an identifier is refused, saying what it was.
    #[test]
    fn text_that_is_not_an_identifier_is_refused_by_name() {
        let error = NodeId::parse("edge-1").expect_err("a name is not an identifier");
        assert!(
            error.to_string().contains("edge-1"),
            "the refusal must name what it read: {error}"
        );
    }

    // -----------------------------------------------------------
    // Reconciling, without a store
    // -----------------------------------------------------------

    /// The pure rule, on its own.
    ///
    /// The store tests above go through a file and prove the whole
    /// path; this proves the decision, including the case the file
    /// tests cannot reach without corrupting a store: a stored identity
    /// whose name is already what was configured.
    #[test]
    fn reconciling_keeps_the_stored_identifier_whatever_the_name_says() {
        let held = NodeIdentityRecord {
            uuid: [0x07; 16],
            name: "edge-1".to_owned(),
        };

        let same = NodeIdentity::reconcile(Some(&held), "edge-1");
        assert_eq!(same.identity.id(), NodeId::from_bytes([0x07; 16]));
        assert_eq!(same.write, None);
        assert!(!same.founded);

        let renamed = NodeIdentity::reconcile(Some(&held), "edge-two");
        assert_eq!(
            renamed.identity.id(),
            NodeId::from_bytes([0x07; 16]),
            "a rename is not a founding"
        );
        assert_eq!(renamed.identity.name(), "edge-two");
        assert_eq!(
            renamed.write,
            Some(NodeIdentityRecord {
                uuid: [0x07; 16],
                name: "edge-two".to_owned(),
            })
        );

        let founded = NodeIdentity::reconcile(None, "edge-1");
        assert!(founded.founded);
        assert_eq!(founded.write, Some(founded.identity.to_record()));
    }

    /// An identity is displayed as both of its halves.
    #[test]
    fn an_identity_shows_its_name_and_its_identifier() {
        let identity = NodeIdentity::new(NodeId::from_bytes([0x01; 16]), "edge-1");
        let shown = identity.to_string();
        assert!(shown.contains("edge-1"), "{shown}");
        assert!(shown.contains(&identity.id().to_string()), "{shown}");
    }
}
