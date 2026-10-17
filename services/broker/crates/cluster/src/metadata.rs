//! What is replicated, at the granularity routing needs.
//!
//! Section 8.2 lists seven kinds of replicated metadata. Three of them —
//! users, access control rules, API keys — arrive with authentication in
//! milestone 7, so this milestone carries four: that a node has at least
//! one subscriber for a filter, retained messages, rules, and which node
//! owns a session. [`MetadataKey`] and [`MetadataValue`] are an **open
//! vocabulary**, so that the other three are a variant here rather than a
//! second mechanism.
//!
//! [`Replicated`] is this node's whole replicated state: a
//! [`Register`] per key, the node's own [`HybridClock`], and the one
//! thing that is *not* replicated — a reference count of this node's own
//! subscribers per filter. Every method that changes something returns
//! the [`MetadataWrite`] to store and broadcast, or `None` when there is
//! nothing to say.
//!
//! # Subscriptions are replicated at node granularity, and the count is why
//!
//! Section 8.2: **a register per client subscription would be fifty nodes
//! holding a hundred thousand registers each** at the design ceiling,
//! against an idle memory budget of thirty megabytes. What is replicated
//! is that a node has at least one subscriber for a filter, which is
//! exactly what section 8.3's routing needs; which of its clients that is
//! never leaves the node.
//!
//! So this holds a count per filter, incremented and decremented locally,
//! and writes a register **only when that count leaves or reaches zero**.
//! The two edges are the ones easy to miss and both are tested:
//!
//! - A filter gaining a *second* local subscriber writes nothing. There
//!   is nothing new to say: every peer already believes this node has a
//!   subscriber for it, and saying it again is a register and a frame per
//!   client rather than per filter, which is the budget this design is
//!   avoiding.
//! - A filter losing its *last* local subscriber writes a **tombstone**,
//!   not nothing. Dropping the register silently would leave every peer
//!   routing to a node with no subscriber for ever: a deletion has to be
//!   a stamped value that beats the write it supersedes, which is what
//!   [`Register`] is for.
//!
//! The count is deliberately not replicated and deliberately not stored.
//! It is rebuilt at startup by [`crate::metadata`]'s caller replaying this
//! node's own stored subscriptions, which is the same walk that rebuilds
//! the subscription trie; a count in the store would be a second answer
//! to a question the subscriptions already answer.
//!
//! # A deletion is a tombstone, on every kind
//!
//! There is no "removed" variant of anything here. A retained message
//! cleared, a rule deleted, a session released and a filter's last
//! subscriber leaving are all a [`Register`] with no value, competing by
//! [`Register::merge`]'s one comparison. See [`crate::register`] for why
//! "a delete always wins" diverges.
//!
//! # A write carries the clock mark beside it
//!
//! [`Replicated::records`] returns the register **and** the clock's
//! high-water mark, because section 8.2's resume depends on the mark
//! riding in the same batch as the write it stamps: a batch is all or
//! nothing, so a stamp that survived a crash has its mark beside it. A
//! caller holding the register alone would be a caller that can write a
//! stamp the store has no record of, which is a clock resuming behind
//! itself. One call returns both so that there is no way to take one.
//!
//! # A write is numbered before the store has taken it, and shown to
//! nobody until it has
//!
//! Every method below stamps the value, holds it, and **reserves a
//! sequence in this node's own operation log**; the caller then gives
//! [`Replicated::records`] to a store and says what the store did, with
//! [`Replicated::published`] or [`Replicated::refused`].
//!
//! That second call is the two-phase write this module recorded as a
//! known limitation while nothing showed a stamp to anybody, and the note
//! said the closure belonged with the broadcast that would expose it. It
//! does, and this is it. A peer told about a register whose batch the
//! store refused holds a value this node will never hold again, and **no
//! exchange repairs that**: an exchange serves the origin's own log, and
//! the origin's log lost the entry in the same refused batch. Each side
//! then believes it holds the winner, which is the one divergence section
//! 8.2 has no answer to. So nothing is broadcast and nothing is servable
//! until the store has taken the batch; see [`crate::oplog`].
//!
//! What is deliberately *not* closed is the register table here being
//! ahead of the store after a refusal. That is unobservable from outside
//! the node: the broker's own state — the subscription, the retained
//! message, the rule — went into the same refused batch, so this node
//! behaves consistently with itself until it restarts, and the caller
//! whose batch was refused has already failed whatever it was doing and
//! said so. Rolling it back would mean a second answer to which copy of a
//! register this node holds, in the layer whose whole purpose is that
//! there is one.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use storage::{
    AclRecord, ApiKeyRecord, MetadataKeyRecord, MetadataRecord, MetadataValueRecord, Record,
    Restored, RetainedRecord, RuleRecord, SeenRecord, SessionKey, StampRecord, UserRecord,
};

use crate::clock::HybridClock;
use crate::groups::Groups;
use crate::identity::NodeId;
use crate::oplog::{Logged, OpLog};
use crate::register::{Register, Stamp};
use crate::routes::{Liveness, Routes};

/// What a replicated register is about.
///
/// The mirror of [`MetadataKeyRecord`], in the same variant order, with
/// a [`NodeId`] where the record holds sixteen bytes. **The order is the
/// format**: see that type, whose redb key encoding puts the variant
/// index in its leading byte so that the two backends restore in one
/// order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetadataKey {
    /// That `node` has at least one subscriber for `filter`.
    ///
    /// **The node is part of the key**, because each node answers for its
    /// own subscribers. One register per filter for the whole cluster
    /// would mean the last node to subscribe silently un-routed every
    /// other.
    NodeSubscription {
        /// Whose subscriber.
        node: NodeId,
        /// Which filter. The *inner* filter for a shared subscription,
        /// because this is what a published topic is matched against.
        filter: String,
    },
    /// The retained message on a topic.
    Retained {
        /// Which topic.
        topic: String,
    },
    /// A rule, section 9.1, which stops being node-local here.
    Rule {
        /// Which rule.
        id: String,
    },
    /// Which node owns a session, section 8.4.
    SessionOwner {
        /// Whose session.
        session: SessionKey,
    },
    /// That `node` has at least one member of a shared group.
    ///
    /// **Not a [`Self::NodeSubscription`], and section 8.3 says why.** A
    /// group spanning three nodes has a matching subscriber on each, so
    /// a routing map built from node subscriptions says all three want
    /// the topic and one copy to each delivers three times where the
    /// semantics demand one. A group needs its own map, built from this,
    /// and a choice of **one** node before the forward rather than
    /// after; see [`crate::groups`].
    ///
    /// The two are exclusive: a shared subscription writes this and not
    /// a node subscription, so that a node whose only interest in a
    /// topic is a group member is not also on the ordinary route.
    NodeGroup {
        /// Whose member.
        node: NodeId,
        /// The share name, which is what makes two groups on one filter
        /// two groups.
        group: String,
        /// The inner filter, because that is what a published topic is
        /// matched against.
        filter: String,
    },
    /// A user, by name. Section 12.1; section 8.2 lists users among
    /// what replicates.
    User {
        /// The name.
        name: String,
    },
    /// An access control rule, by identifier. Section 12.1.
    Acl {
        /// The identifier.
        id: String,
    },
    /// An API key, by identifier. Section 11.1.
    ApiKey {
        /// The identifier.
        id: String,
    },
}

impl MetadataKey {
    /// What a store holds this as.
    #[must_use]
    pub fn to_record(&self) -> MetadataKeyRecord {
        match self {
            Self::NodeSubscription { node, filter } => MetadataKeyRecord::NodeSubscription {
                node: node.to_bytes(),
                filter: filter.clone(),
            },
            Self::Retained { topic } => MetadataKeyRecord::Retained {
                topic: topic.clone(),
            },
            Self::Rule { id } => MetadataKeyRecord::Rule { id: id.clone() },
            Self::SessionOwner { session } => MetadataKeyRecord::SessionOwner {
                session: session.clone(),
            },
            Self::NodeGroup {
                node,
                group,
                filter,
            } => MetadataKeyRecord::NodeGroup {
                node: node.to_bytes(),
                group: group.clone(),
                filter: filter.clone(),
            },
            Self::User { name } => MetadataKeyRecord::User { name: name.clone() },
            Self::Acl { id } => MetadataKeyRecord::Acl { id: id.clone() },
            Self::ApiKey { id } => MetadataKeyRecord::ApiKey { id: id.clone() },
        }
    }

    /// A key read back from a store.
    #[must_use]
    pub fn from_record(record: MetadataKeyRecord) -> Self {
        match record {
            MetadataKeyRecord::NodeSubscription { node, filter } => Self::NodeSubscription {
                node: NodeId::from_bytes(node),
                filter,
            },
            MetadataKeyRecord::Retained { topic } => Self::Retained { topic },
            MetadataKeyRecord::Rule { id } => Self::Rule { id },
            MetadataKeyRecord::SessionOwner { session } => Self::SessionOwner { session },
            MetadataKeyRecord::NodeGroup {
                node,
                group,
                filter,
            } => Self::NodeGroup {
                node: NodeId::from_bytes(node),
                group,
                filter,
            },
            MetadataKeyRecord::User { name } => Self::User { name },
            MetadataKeyRecord::Acl { id } => Self::Acl { id },
            MetadataKeyRecord::ApiKey { id } => Self::ApiKey { id },
        }
    }

    /// Which kind this is, for a refusal that has to name it.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::NodeSubscription { .. } => "a node subscription",
            Self::Retained { .. } => "a retained message",
            Self::Rule { .. } => "a rule",
            Self::SessionOwner { .. } => "a session owner",
            Self::NodeGroup { .. } => "a shared group membership",
            Self::User { .. } => "a user",
            Self::Acl { .. } => "an access control rule",
            Self::ApiKey { .. } => "an API key",
        }
    }
}

/// What a replicated register holds.
///
/// One variant per [`MetadataKey`] variant, in the same order. A stored
/// register whose key and value name different kinds is refused when it
/// is read back: see [`MetadataWrite::from_record`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataValue {
    /// Presence, and nothing else.
    ///
    /// A unit variant because the key carries the node and the filter,
    /// which is the whole fact. Anything here would be something about
    /// that node's own clients, and that is the part which does not fit
    /// in the budget.
    NodeSubscription,
    /// What is retained on the topic the key names.
    Retained(RetainedRecord),
    /// The rule the key names, whole.
    ///
    /// Boxed for the reason [`MetadataValueRecord::Rule`] is: a rule is
    /// three hundred bytes and the smallest variant here is sixteen, and
    /// a `Box<T>` is written to a store exactly as a `T` is.
    Rule(Box<RuleRecord>),
    /// Which node owns the session the key names, and at what epoch.
    SessionOwner {
        /// The owner.
        node: NodeId,
        /// How many times the session has been claimed, section 8.4.
        ///
        /// **Carried here, and not compared.** See [`Ownership`] for
        /// why the resolution is the register's own and what the epoch
        /// is for instead.
        epoch: u64,
    },
    /// Presence, and nothing else.
    ///
    /// A unit variant for [`Self::NodeSubscription`]'s reason: the key
    /// carries the node, the group and the filter, which is the whole
    /// fact. How many of that node's clients are in the group is the
    /// part that stays on the node.
    NodeGroup,
    /// A user, whole. Boxed for the reason a rule is.
    User(Box<UserRecord>),
    /// An access control rule, whole.
    Acl(Box<AclRecord>),
    /// An API key, whole.
    ApiKey(Box<ApiKeyRecord>),
}

impl MetadataValue {
    /// What a store holds this as.
    #[must_use]
    pub fn to_record(&self) -> MetadataValueRecord {
        match self {
            Self::NodeSubscription => MetadataValueRecord::NodeSubscription,
            Self::Retained(message) => MetadataValueRecord::Retained(message.clone()),
            Self::Rule(rule) => MetadataValueRecord::Rule(rule.clone()),
            Self::SessionOwner { node, epoch } => MetadataValueRecord::SessionOwner {
                node: node.to_bytes(),
                epoch: *epoch,
            },
            Self::NodeGroup => MetadataValueRecord::NodeGroup,
            Self::User(user) => MetadataValueRecord::User(user.clone()),
            Self::Acl(rule) => MetadataValueRecord::Acl(rule.clone()),
            Self::ApiKey(key) => MetadataValueRecord::ApiKey(key.clone()),
        }
    }

    /// A value read back from a store.
    #[must_use]
    pub fn from_record(record: MetadataValueRecord) -> Self {
        match record {
            MetadataValueRecord::NodeSubscription => Self::NodeSubscription,
            MetadataValueRecord::Retained(message) => Self::Retained(message),
            MetadataValueRecord::Rule(rule) => Self::Rule(rule),
            MetadataValueRecord::SessionOwner { node, epoch } => Self::SessionOwner {
                node: NodeId::from_bytes(node),
                epoch,
            },
            MetadataValueRecord::NodeGroup => Self::NodeGroup,
            MetadataValueRecord::User(user) => Self::User(user),
            MetadataValueRecord::Acl(rule) => Self::Acl(rule),
            MetadataValueRecord::ApiKey(key) => Self::ApiKey(key),
        }
    }

    /// Which kind this is, for a refusal that has to name it.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::NodeSubscription => "a node subscription",
            Self::Retained(_) => "a retained message",
            Self::Rule(_) => "a rule",
            Self::SessionOwner { .. } => "a session owner",
            Self::NodeGroup => "a shared group membership",
            Self::User(_) => "a user",
            Self::Acl(_) => "an access control rule",
            Self::ApiKey(_) => "an API key",
        }
    }

    /// Whether this value is about the same kind of thing as `key`.
    fn matches(&self, key: &MetadataKey) -> bool {
        matches!(
            (key, self),
            (MetadataKey::NodeSubscription { .. }, Self::NodeSubscription)
                | (MetadataKey::Retained { .. }, Self::Retained(_))
                | (MetadataKey::Rule { .. }, Self::Rule(_))
                | (MetadataKey::SessionOwner { .. }, Self::SessionOwner { .. })
                | (MetadataKey::NodeGroup { .. }, Self::NodeGroup)
                | (MetadataKey::User { .. }, Self::User(_))
                | (MetadataKey::Acl { .. }, Self::Acl(_))
                | (MetadataKey::ApiKey { .. }, Self::ApiKey(_))
        )
    }
}

/// One replicated write: which register, and what it now says.
///
/// What a caller stores and what a caller broadcasts, which are the same
/// thing. A deletion is one of these whose register is a tombstone; there
/// is no second variant for it, for the reason [`crate::register`] gives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataWrite {
    /// What the register is about.
    key: MetadataKey,
    /// What it holds, and what orders it.
    register: Register<MetadataValue>,
}

impl MetadataWrite {
    /// A write from its two parts.
    #[must_use]
    pub const fn new(key: MetadataKey, register: Register<MetadataValue>) -> Self {
        Self { key, register }
    }

    /// What the register is about.
    #[must_use]
    pub const fn key(&self) -> &MetadataKey {
        &self.key
    }

    /// What it holds, and what orders it.
    #[must_use]
    pub const fn register(&self) -> &Register<MetadataValue> {
        &self.register
    }

    /// What orders this against another copy.
    #[must_use]
    pub fn stamp(&self) -> Stamp {
        self.register.stamp()
    }

    /// Whether this is a deletion.
    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        self.register.is_tombstone()
    }

    /// What a store holds this as.
    #[must_use]
    pub fn to_record(&self) -> MetadataRecord {
        let stamp = self.register.stamp();
        MetadataRecord {
            key: self.key.to_record(),
            stamp: StampRecord {
                millis: stamp.reading().millis(),
                counter: stamp.reading().counter(),
                node: stamp.node().to_bytes(),
            },
            value: self.register.value().map(MetadataValue::to_record),
        }
    }

    /// A write read back from a store.
    ///
    /// # Errors
    ///
    /// [`MetadataError::KindMismatch`] when the key and the value name
    /// different kinds. **Refused rather than repaired**, for convention
    /// 1's reason read the other way round: nothing this build writes can
    /// produce such a pair, so one in a store is a store this build does
    /// not understand, and guessing which half to believe would route by
    /// one and answer by the other.
    pub fn from_record(record: MetadataRecord) -> Result<Self, MetadataError> {
        let key = MetadataKey::from_record(record.key);
        let value = record.value.map(MetadataValue::from_record);
        if let Some(value) = &value {
            if !value.matches(&key) {
                return Err(MetadataError::KindMismatch {
                    key: key.kind(),
                    value: value.kind(),
                });
            }
        }
        let stamp = Stamp::new(
            crate::clock::Reading::new(record.stamp.millis, record.stamp.counter),
            NodeId::from_bytes(record.stamp.node),
        );
        Ok(Self {
            key,
            register: match value {
                Some(value) => Register::written(value, stamp),
                None => Register::deleted(stamp),
            },
        })
    }
}

/// Who owns a session, and at what epoch. Section 8.4.
///
/// # The epoch is carried, not compared
///
/// Section 8.5 reverses the obvious implementation here, and says so
/// outright: **equal epochs are the expected outcome of a partition,
/// not the rare one.** Two nodes on opposite sides both read the same
/// epoch and both increment to the same number, so an ownership
/// decided by "the higher epoch wins" is a decision that is not taken
/// in precisely the case it exists for — each side keeps itself, and no
/// later exchange repairs it, for the reason [`crate::register`] gives
/// about a tie.
///
/// So what resolves two claims is the register's own comparison, which
/// is the clock reading, then its counter, then the writing node's
/// UUID. That is why the epoch lives in a [`Register`] rather than
/// being compared on its own, and it is why there is no ordering on
/// this type: there is nothing here to compare, and a derived `Ord`
/// would be an invitation to.
///
/// What the epoch is *for* is the next claim. A node that takes a
/// session over writes the epoch it read plus one, so the number counts
/// how many times the session has been claimed anywhere, and
/// `broker_core::registry` lifts its own session's epoch to it: one
/// number for the session, wherever the session is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ownership {
    /// Which node owns it.
    pub node: NodeId,
    /// How many times it has been claimed.
    pub epoch: u64,
}

/// What claiming a session produced.
///
/// Three answers from one call, for [`Taken`]'s reason: a caller needs
/// all three, and a second read to derive any of them would be a second
/// read of a register another node may have overwritten in between.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    /// Who held it a moment ago, if anybody.
    ///
    /// **Who to ask for the queue**, which is what section 8.4's drain
    /// is addressed to. `None` is a session no node has claimed, or one
    /// whose register is a tombstone, and there is nobody to ask.
    pub previous: Option<Ownership>,
    /// The epoch this node now holds it at.
    pub epoch: u64,
    /// The register to store and broadcast, or `None` when this node
    /// already owned it.
    ///
    /// **A claim by the owner writes nothing**, and that is not an
    /// optimisation. A connect to the node that already owns the
    /// session is the within-one-node takeover `broker_core::registry`
    /// has always done, which is not a cluster event; writing a
    /// register for it would be a frame per reconnect from every client
    /// on every node, and — the part that is not merely waste — a fresh
    /// stamp on the losing side of a partition every time a client
    /// retried, which is a node voting again in an election it has
    /// already lost.
    pub write: Option<MetadataWrite>,
}

/// Why a stored register was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MetadataError {
    /// The key and the value name different kinds.
    #[error(
        "a stored register's key is about {key} and its value is {value}; the two must name \
         one kind, and nothing this build writes can produce the pair, so the store was \
         written by something else and is refused rather than guessed at"
    )]
    KindMismatch {
        /// What the key was about.
        key: &'static str,
        /// What the value was.
        value: &'static str,
    },
}

/// What taking a peer's entries changed.
///
/// Three answers from one call, because a caller needs all three and
/// deriving any of them a second way would be a second answer: what to
/// apply locally, what to store, and how far this node has now read the
/// peer's log.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Taken {
    /// The writes that won, in sequence order.
    ///
    /// What a caller turns into local effect: a rule installed, a retained
    /// message set, a route to a peer added or dropped. A write that lost
    /// told this node nothing, so it is not here.
    pub applied: Vec<MetadataWrite>,
    /// What to store: the registers that won, and the watermark if it
    /// moved.
    ///
    /// **One batch**, because a watermark stored without the registers it
    /// covers is a claim to hold writes this node would never be sent
    /// again.
    pub records: Vec<Record>,
    /// How far this node has now read the log, contiguously.
    pub through: u64,
}

/// Who `state` says owns `session`, if anybody.
///
/// A free function rather than a method, because
/// [`Replicated::session_claimed`] reads it with the lock already held
/// and a second method taking the lock would deadlock on the recursion
/// the first one did not know it was making.
fn owner_of(state: &State, session: &SessionKey) -> Option<Ownership> {
    let register = state.registers.get(&MetadataKey::SessionOwner {
        session: session.clone(),
    })?;
    match register.value()? {
        MetadataValue::SessionOwner { node, epoch } => Some(Ownership {
            node: *node,
            epoch: *epoch,
        }),
        // A register filed under a session-owner key holding something
        // else is refused when it is read back from a store, so this is
        // unreachable rather than a case. `None` rather than a panic
        // because the one thing worse than a node that cannot say who
        // owns a session is a node that stops.
        _ => None,
    }
}

/// Take `write` into `state`'s registers, and say whether it won.
///
/// [`Register::merge`] and nothing else, in one place so that a broadcast,
/// an exchange and a test cannot reach three different answers.
fn merge(state: &mut State, write: &MetadataWrite) -> bool {
    match state.registers.get_mut(&write.key) {
        Some(held) => held.merge(write.register.clone()),
        None => {
            state
                .registers
                .insert(write.key.clone(), write.register.clone());
            true
        }
    }
}

/// Tell `routes` what a register now says, whatever it is about.
///
/// **One function, called from every path that changes a register**:
/// a restore, this node's own write, a peer's write that won, and a
/// tombstone collected. Section 8.3's map is an index over the
/// registers, and an index fed from three of the four places they
/// change is an index that is wrong in the fourth — which is convention
/// 5's "a path no test walks" waiting to happen, because the fourth is
/// always the one nobody thought of.
///
/// A kind the map is not about is nothing to tell it, which is most of
/// them: a retained message, a rule and a session owner are registers
/// routing does not read.
fn index(
    routes: &Routes,
    groups: &Groups,
    key: &MetadataKey,
    register: Option<&Register<MetadataValue>>,
) {
    // An absent register and a tombstone are the same fact: this node
    // does not believe that node has a subscriber there. A collected
    // tombstone is the first of the two and arrives here the same way,
    // which is why the argument is an `Option` rather than a `Register`.
    let held = matches!(register, Some(register) if !register.is_tombstone());
    match key {
        MetadataKey::NodeSubscription { node, filter } => {
            if held {
                routes.added(*node, filter);
            } else {
                routes.removed(*node, filter);
            }
        }
        // **The two go to two maps, and that separation is section
        // 8.3's whole answer to the shared-subscription trap.** A group
        // membership indexed into the routing map would put every
        // member node on the ordinary route, and one copy to each would
        // be one delivery per node.
        MetadataKey::NodeGroup {
            node,
            group,
            filter,
        } => {
            if held {
                groups.added(*node, group, filter);
            } else {
                groups.removed(*node, group, filter);
            }
        }
        // A kind neither map is about, which is most of them: a
        // retained message, a rule and a session owner are registers
        // routing does not read.
        MetadataKey::Retained { .. }
        | MetadataKey::Rule { .. }
        | MetadataKey::SessionOwner { .. }
        | MetadataKey::User { .. }
        | MetadataKey::Acl { .. }
        | MetadataKey::ApiKey { .. } => {}
    }
}

/// This node's replicated state, and the one thing that is not.
///
/// # Behind a lock, for the reason [`HybridClock`] is
///
/// Every method takes `&self` and the state is behind a [`Mutex`],
/// because the callers are the subscribe path, the rule registry and the
/// link that receives a peer's frames, and threading a `&mut` between
/// three owners means one of them holds it. The contention is one write
/// path's worth and every write behind it is already going to a store.
#[derive(Debug)]
pub struct Replicated {
    /// Who this node is, which is half of every stamp it issues.
    me: NodeId,
    /// What stamps a write, resumed from the store.
    clock: HybridClock,
    /// Everything behind one lock.
    state: Mutex<State>,
    /// Section 8.3's routing map, kept in step with the registers.
    ///
    /// **Beside the state rather than in it**, with a lock of its own.
    /// The registers are written on the subscribe path and read by an
    /// exchange; this is read on every publish, and one lock over both
    /// would put every publish behind every register write. See
    /// [`Routes`] for what that costs.
    ///
    /// It is held here rather than by the shell for the reason [`index`]
    /// gives: there are four paths that change a register and they are
    /// all in this file, so an index maintained here cannot be missed by
    /// one of them. A map the shell maintained would be fed by whichever
    /// of the four the shell happens to see.
    routes: Routes,
    /// Section 8.3's **other** map: which nodes have a member of each
    /// shared group.
    ///
    /// Separate from the one above because a group cannot use it, which
    /// is the one thing in this milestone's design that was wrong before
    /// the routing plan: see [`crate::groups`].
    groups: Groups,
    /// Which nodes this one has judged dead, shared by both maps.
    ///
    /// Membership is not replicated — section 8.2 — so this is a local
    /// judgement with no register behind it, and both maps have to
    /// honour it. One set rather than two, because two would be two
    /// answers to one question.
    liveness: Arc<Liveness>,
}

/// What [`Replicated`] holds.
#[derive(Debug, Default)]
struct State {
    /// Every register, by what it is about.
    ///
    /// Ordered, so that what this hands back is comparable with what a
    /// store hands back: see [`storage::Restored::metadata`].
    registers: BTreeMap<MetadataKey, Register<MetadataValue>>,
    /// How many of *this node's* clients are in each shared group.
    ///
    /// The same rule as [`Self::local_filters`] and for the same reason,
    /// keyed by share name and inner filter together: a register is
    /// written only when the count leaves or reaches zero, so a second
    /// local member of a group says nothing new and the last one to
    /// leave writes a tombstone.
    ///
    /// **A separate count from the one below**, because the two are
    /// separate registers: a client holding both `a/b` and
    /// `$share/g/a/b` is one ordinary subscriber and one group member,
    /// and a single count would tombstone neither when one of them went.
    local_groups: HashMap<(String, String), usize>,
    /// How many of *this node's* subscribers each filter has.
    ///
    /// **Not replicated and not stored.** An entry is removed when it
    /// reaches zero, so this grows with the filters this node's clients
    /// are actually subscribed to rather than with every filter it has
    /// ever seen.
    local_filters: HashMap<String, usize>,
    /// This node's own operation log, section 8.2.
    ///
    /// **In the same state as the registers, behind the same lock**,
    /// because a write reserves a sequence and holds a register in one
    /// step and the two must not be separable: a register held with no
    /// sequence is a write no exchange can ever carry.
    oplog: OpLog,
    /// How far this node has read each other node's log, section 8.2.
    ///
    /// The highest *contiguous* sequence, so a broadcast that was dropped
    /// leaves a gap rather than being counted past; see
    /// [`storage::SeenRecord`].
    seen: BTreeMap<NodeId, u64>,
}

impl Replicated {
    /// This node's replicated state, resumed from what the store held.
    ///
    /// `restored` is the whole of what the store held, and all four of the
    /// parts this reads come out of it together: the registers, tombstones
    /// included — a node that dropped a tombstone on restart would be
    /// handed its own older copy by the next peer it spoke to and would
    /// resurrect what it deleted — this node's own log and the highest
    /// sequence it has ever used, and how far it had read each peer's log.
    ///
    /// **One restore rather than four**, for the reason section 7.5 gives
    /// about reading the store once, and a sharper one: the clock, the log
    /// and the watermarks are written in one batch and a second read could
    /// see a different moment of any of them.
    ///
    /// The clock is resumed too, by the caller, and from the store: see
    /// [`HybridClock::resume`] for why not from zero.
    ///
    /// # Errors
    ///
    /// [`MetadataError`] from the first register this build does not
    /// understand. A startup that carried on would route by half of a
    /// record it had already decided was wrong.
    pub fn open(
        me: NodeId,
        clock: HybridClock,
        restored: &Restored,
    ) -> Result<Self, MetadataError> {
        let liveness = Arc::new(Liveness::new());
        let routes = Routes::new(Arc::clone(&liveness));
        let groups = Groups::new(Arc::clone(&liveness));
        let mut registers = BTreeMap::new();
        for record in &restored.metadata {
            let write = MetadataWrite::from_record(record.clone())?;
            // The first of [`index`]'s four paths. A node that came back
            // with the registers and an empty routing map would forward
            // nothing to anybody until every peer happened to write
            // again, which for a quiet cluster is never.
            index(&routes, &groups, &write.key, Some(&write.register));
            registers.insert(write.key, write.register);
        }
        let mut logged = Vec::with_capacity(restored.oplog.len());
        for record in &restored.oplog {
            logged.push(Logged::from_record(record.clone())?);
        }
        let seen = restored
            .seen
            .iter()
            .map(|seen| (NodeId::from_bytes(seen.node), seen.through))
            .collect();
        Ok(Self {
            me,
            clock,
            state: Mutex::new(State {
                registers,
                local_groups: HashMap::new(),
                local_filters: HashMap::new(),
                oplog: OpLog::resume(logged, restored.oplog_collected_through),
                seen,
            }),
            routes,
            groups,
            liveness,
        })
    }

    /// Who this node is.
    #[must_use]
    pub const fn me(&self) -> NodeId {
        self.me
    }

    /// What stamps this node's writes.
    #[must_use]
    pub const fn clock(&self) -> &HybridClock {
        &self.clock
    }

    /// What a store must be given for `write`.
    ///
    /// **The register, the clock's mark and this node's log entry,
    /// together**, because section 8.2's two resumes both depend on riding
    /// in the same batch as the write they cover: a batch is all or
    /// nothing, so a stamp that survived a crash has its mark beside it
    /// and a register that survived has its log entry beside it. Returned
    /// from one call so that there is no way for a caller to take one
    /// without the others.
    ///
    /// The log entry is absent for a write this node did not originate,
    /// which is one taken from a peer: a node logs what it wrote and not
    /// what it was told.
    #[must_use]
    pub fn records(&self, write: &MetadataWrite) -> Vec<Record> {
        let mut records = vec![
            Record::MetadataPut {
                entry: write.to_record(),
            },
            Record::ClockHighWaterPut {
                reading: self.clock.mark(),
            },
        ];
        if let Some(sequence) = self.held().oplog.held_at(write.stamp()) {
            records.push(Record::OpLogAppended {
                entry: Logged::new(sequence, write.clone()).to_record(),
            });
        }
        records
    }

    /// The store took the batch: the write may be shown to peers now.
    ///
    /// Returns the log entry, which is what a broadcast carries, or `None`
    /// for a write this node did not originate or has already resolved.
    /// Until this is called the entry is in no digest and in no batch an
    /// exchange serves; see [`crate::oplog`] for why that is the whole
    /// closure of the hole this module used to record.
    pub fn published(&self, write: &MetadataWrite) -> Option<Logged> {
        let mut state = self.held();
        let sequence = state.oplog.held_at(write.stamp())?;
        state.oplog.publish(sequence)
    }

    /// The store refused the batch: the write never happened.
    ///
    /// Returns whether there was one to refuse. The sequence goes back in
    /// the pool, because a number nothing was ever told about can be used
    /// again; see [`crate::oplog::OpLog::discard`].
    ///
    /// The register this node holds is **not** rolled back, and the module
    /// documentation says why.
    pub fn refused(&self, write: &MetadataWrite) -> bool {
        let mut state = self.held();
        match state.oplog.held_at(write.stamp()) {
            Some(sequence) => state.oplog.discard(sequence),
            None => false,
        }
    }

    /// The highest contiguous sequence of this node's log a peer may ask
    /// past.
    #[must_use]
    pub fn log_published_through(&self) -> u64 {
        self.held().oplog.published_through()
    }

    /// The highest sequence this node's log has ever used.
    #[must_use]
    pub fn log_high_water(&self) -> u64 {
        self.held().oplog.high_water()
    }

    /// How many entries of this node's log are still held.
    #[must_use]
    pub fn log_len(&self) -> usize {
        self.held().oplog.len()
    }

    /// Up to `limit` of this node's log entries after `sequence`.
    ///
    /// What an exchange serves a peer that asked. Never past
    /// [`Self::log_published_through`].
    #[must_use]
    pub fn log_after(&self, sequence: u64, limit: usize) -> Vec<Logged> {
        self.held().oplog.after(sequence, limit)
    }

    /// The reading the entry at `sequence` of this node's log carries.
    ///
    /// What turns "every peer has read my log through here" into "every
    /// peer has seen every write of mine up to this reading", which is
    /// what a tombstone may be collected behind.
    #[must_use]
    pub fn log_reading_at(&self, sequence: u64) -> Option<crate::clock::Reading> {
        self.held().oplog.reading_at(sequence)
    }

    /// Remove this node's own log entries up to and including `sequence`.
    ///
    /// Section 8.2's second watermark. Returns the records to store, which
    /// are empty when nothing went.
    pub fn collect_log_through(&self, sequence: u64) -> Vec<Record> {
        if self.held().oplog.collect_through(sequence) == 0 {
            return Vec::new();
        }
        vec![Record::OpLogTruncated { through: sequence }]
    }

    /// The last sequence of this node's own log that collection removed.
    ///
    /// A peer whose watermark is below this cannot be served from the
    /// log; see [`OpLog::collected_through`].
    #[must_use]
    pub fn log_collected_through(&self) -> u64 {
        self.held().oplog.collected_through()
    }

    /// Up to `limit` registers in key order, after `after`.
    ///
    /// **Section 8.2's join snapshot.** A peer that asks for a stretch
    /// of this node's log that collection has removed is sent the
    /// registers instead — every register this node holds, whoever
    /// wrote it, tombstones included — a page at a time in the order
    /// the keys sort, so that the peer can say where to continue from
    /// without either side keeping a cursor. One register more than
    /// `limit` tells the caller whether there is another page.
    ///
    /// Every register and not only this node's own, because the peer is
    /// behind on everybody's and the one register table is the only
    /// thing that holds what the truncated entries said. A register
    /// another node wrote is as good from here as from its writer: the
    /// merge is by stamp, and the stamp travels with it.
    #[must_use]
    pub fn registers_after(&self, after: Option<&MetadataKey>, limit: usize) -> Vec<MetadataWrite> {
        use std::ops::Bound::{Excluded, Unbounded};
        let state = self.held();
        let from: std::ops::Bound<&MetadataKey> = match after {
            Some(after) => Excluded(after),
            None => Unbounded,
        };
        state
            .registers
            .range((from, Unbounded))
            .take(limit)
            .map(|(key, register)| MetadataWrite::new(key.clone(), register.clone()))
            .collect()
    }

    /// Take a page of `origin`'s registers, sent because this node's
    /// watermark is behind what `origin` has truncated.
    ///
    /// Each register is merged as a broadcast's would be, and
    /// [`index`] is told about the ones that won. `through` is the last
    /// sequence `origin` truncated, given on the last page only: once
    /// every register has arrived, this node holds everything those
    /// entries said, so its watermark may move there and the exchange
    /// can go on from the log above it. **Not before the last page**,
    /// because a watermark is a claim to hold what is below it, and a
    /// page lost on the way would otherwise be a claim to hold registers
    /// this node was never sent.
    ///
    /// The watermark never moves backwards: a page from a peer whose
    /// truncation point is behind what this node has already read says
    /// nothing new about the log.
    pub fn observed_snapshot(
        &self,
        origin: NodeId,
        registers: Vec<MetadataWrite>,
        through: Option<u64>,
    ) -> Taken {
        let mut state = self.held();
        let mut taken = Taken::default();
        let wall = crate::clock::wall_millis();
        let mut refused = false;
        for write in registers {
            // Section 8.2's clock, as in `observed_from`; a reading past
            // the bound is refused by `Replicator` before it gets here.
            if self.clock.observe(wall, write.stamp().reading()).is_err() {
                refused = true;
                continue;
            }
            if merge(&mut state, &write) {
                index(
                    &self.routes,
                    &self.groups,
                    write.key(),
                    Some(write.register()),
                );
                taken.records.push(Record::MetadataPut {
                    entry: write.to_record(),
                });
                taken.applied.push(write);
            }
        }
        let before = state.seen.get(&origin).copied().unwrap_or(0);
        taken.through = before;
        if let Some(through) = through {
            // A register left out is a register this node does not hold,
            // so the claim the watermark makes would be false.
            if !refused && through > before {
                state.seen.insert(origin, through);
                taken.records.push(Record::MetadataSeenPut {
                    seen: SeenRecord {
                        node: origin.to_bytes(),
                        through,
                    },
                });
                taken.through = through;
            }
        }
        taken
    }

    /// How far this node has read `origin`'s log.
    ///
    /// Zero for a node nothing has been read from, which is why a log
    /// numbers from one; see [`crate::oplog::FIRST_SEQUENCE`].
    #[must_use]
    pub fn seen_through(&self, origin: NodeId) -> u64 {
        self.held().seen.get(&origin).copied().unwrap_or(0)
    }

    /// Every node this one has read anything of, with how far.
    ///
    /// What a digest carries, and what the tombstone watermark is a
    /// minimum over.
    #[must_use]
    pub fn seen(&self) -> Vec<(NodeId, u64)> {
        self.held()
            .seen
            .iter()
            .map(|(node, through)| (*node, *through))
            .collect()
    }

    /// Take entries `origin` logged, and say what happened.
    ///
    /// Every entry is merged by [`Register::merge`], whatever its
    /// sequence: a register that wins is worth keeping however it arrived.
    /// **The watermark only advances over a contiguous run**, because a
    /// peer told this node holds its log through sequence seven will never
    /// send five again — so a broadcast that was dropped must leave the
    /// watermark below the gap and let the exchange fill it.
    pub fn observed_from(&self, origin: NodeId, entries: Vec<Logged>) -> Taken {
        let mut state = self.held();
        let mut taken = Taken::default();
        let mut through = state.seen.get(&origin).copied().unwrap_or(0);
        // In sequence order, so that a batch delivered out of order still
        // advances the watermark over whatever run it holds.
        let mut entries = entries;
        entries.sort_by_key(Logged::sequence);
        let wall = crate::clock::wall_millis();
        for entry in entries {
            let sequence = entry.sequence();
            let write = entry.into_write();
            // Section 8.2: the peer's reading is taken into this node's
            // clock, so that whatever this node writes afterwards is
            // stamped later than what arrived and wins the conflict
            // causality says it should. Whether the write itself wins is
            // a separate question, answered by the merge below: a copy
            // that lost still said what time it is. A reading beyond
            // the drift bound is refused by `Replicator` before it gets
            // here; one that reaches here anyway is left out of the
            // merge and the watermark stops below it, so the frame is
            // asked for again rather than quietly taken.
            if self.clock.observe(wall, write.stamp().reading()).is_err() {
                break;
            }
            if merge(&mut state, &write) {
                // The third of [`index`]'s four paths, and only for a
                // write that won: a copy that lost told this node
                // nothing, and feeding it to the map would undo the
                // later write the map already holds.
                index(
                    &self.routes,
                    &self.groups,
                    write.key(),
                    Some(write.register()),
                );
                taken.records.push(Record::MetadataPut {
                    entry: write.to_record(),
                });
                taken.applied.push(write);
            }
            if sequence == through + 1 {
                through += 1;
            }
        }
        let before = state.seen.get(&origin).copied().unwrap_or(0);
        if through > before {
            state.seen.insert(origin, through);
            taken.records.push(Record::MetadataSeenPut {
                seen: SeenRecord {
                    node: origin.to_bytes(),
                    through,
                },
            });
        }
        taken.through = through;
        taken
    }

    /// What this node holds for `key`, if anything.
    #[must_use]
    pub fn register(&self, key: &MetadataKey) -> Option<Register<MetadataValue>> {
        self.held().registers.get(key).cloned()
    }

    /// What this node holds for `key`, or `None` for a tombstone or an
    /// absence.
    #[must_use]
    pub fn value(&self, key: &MetadataKey) -> Option<MetadataValue> {
        self.held().registers.get(key)?.value().cloned()
    }

    /// Every register, in key order, as a store holds them.
    ///
    /// The order [`storage::Restored::metadata`] comes back in, so that a
    /// restart is one equality.
    #[must_use]
    pub fn records_held(&self) -> Vec<MetadataRecord> {
        self.held()
            .registers
            .iter()
            .map(|(key, register)| MetadataWrite::new(key.clone(), register.clone()).to_record())
            .collect()
    }

    /// Section 8.3's routing map, as the publish path reads it.
    ///
    /// Every node with a matching subscriber for a topic, which is what
    /// a forward goes to — and what a dead node is taken out of, by
    /// [`Routes::died`], because membership is not replicated and no
    /// register covers it.
    #[must_use]
    pub const fn routes(&self) -> &Routes {
        &self.routes
    }

    /// Every node with a subscriber matching `topic`, in identity order.
    ///
    /// [`Routes::nodes_for`], which is the one that is an index rather
    /// than a walk. [`Self::nodes_subscribed`] below answers about one
    /// filter exactly and walks every register to do it; this answers
    /// about a topic and is what a publish asks.
    #[must_use]
    pub fn nodes_for(&self, topic: &str) -> Vec<NodeId> {
        self.routes.nodes_for(topic)
    }

    /// Which nodes have a subscriber for `filter`, in identity order.
    ///
    /// **Exactly `filter`, as a string**, which is not what routing
    /// asks: a publication on `sensor/1/temp` has to reach a node
    /// subscribed to `sensor/+/temp`, and that is [`Self::nodes_for`].
    /// This walks every register, so it is for a test and for an
    /// operator asking what a register says.
    ///
    /// A tombstone is not a subscriber, which is the whole point of
    /// keeping one.
    #[must_use]
    pub fn nodes_subscribed(&self, filter: &str) -> Vec<NodeId> {
        self.held()
            .registers
            .iter()
            .filter_map(|(key, register)| match key {
                MetadataKey::NodeSubscription { node, filter: held }
                    if held == filter && !register.is_tombstone() =>
                {
                    Some(*node)
                }
                _ => None,
            })
            .collect()
    }

    /// How many of this node's own subscribers `filter` has.
    ///
    /// Local, and never replicated: section 8.2 replicates that a node
    /// has a subscriber and not how many.
    #[must_use]
    pub fn local_subscribers(&self, filter: &str) -> usize {
        self.held().local_filters.get(filter).copied().unwrap_or(0)
    }

    /// A local subscriber arrived on `filter`.
    ///
    /// Returns a write **only when the count left zero**, because that is
    /// the only moment anything outside this node learns something new.
    /// A second subscriber to a filter returns `None`: see the module
    /// documentation for the budget that rule exists for.
    pub fn subscriber_arrived(&self, filter: &str) -> Option<MetadataWrite> {
        let mut state = self.held();
        let count = {
            let held = state.local_filters.entry(filter.to_owned()).or_insert(0);
            *held += 1;
            *held
        };
        if count > 1 {
            return None;
        }
        Some(self.write(
            &mut state,
            MetadataKey::NodeSubscription {
                node: self.me,
                filter: filter.to_owned(),
            },
            Some(MetadataValue::NodeSubscription),
        ))
    }

    /// A local subscriber departed from `filter`.
    ///
    /// Returns a **tombstone** only when the count reached zero, and
    /// `None` otherwise. Dropping the register instead of writing a
    /// tombstone would leave every peer routing to a node with no
    /// subscriber for ever.
    ///
    /// A departure from a filter this node has no count for returns
    /// `None` and changes nothing. That is a caller that unsubscribed
    /// something twice, and the answer that keeps the count honest is to
    /// ignore it rather than to go below zero and never reach it again.
    pub fn subscriber_departed(&self, filter: &str) -> Option<MetadataWrite> {
        let mut state = self.held();
        let count = {
            let held = state.local_filters.get_mut(filter)?;
            *held -= 1;
            *held
        };
        if count > 0 {
            return None;
        }
        state.local_filters.remove(filter);
        Some(self.write(
            &mut state,
            MetadataKey::NodeSubscription {
                node: self.me,
                filter: filter.to_owned(),
            },
            None,
        ))
    }

    /// Section 8.3's **group** map, as the publish path reads it.
    ///
    /// Which nodes have a member of each shared group, and which one is
    /// chosen to serve a publication. Separate from [`Self::routes`]
    /// because a group cannot use that map; see [`crate::groups`].
    #[must_use]
    pub const fn groups(&self) -> &Groups {
        &self.groups
    }

    /// Which nodes this one has judged dead.
    ///
    /// Membership is not replicated, so nothing is written down when a
    /// node dies and no register covers it. Both of section 8.3's maps
    /// honour this, and it is one set rather than two.
    #[must_use]
    pub const fn liveness(&self) -> &Arc<Liveness> {
        &self.liveness
    }

    /// This node has judged `node` dead: stop choosing it.
    pub fn died(&self, node: NodeId) {
        self.liveness.died(node);
    }

    /// And `node` is answering again.
    pub fn revived(&self, node: NodeId) {
        self.liveness.revived(node);
    }

    /// How many of this node's own clients are in `group` on `filter`.
    ///
    /// Local, and never replicated, like [`Self::local_subscribers`].
    #[must_use]
    pub fn local_members(&self, group: &str, filter: &str) -> usize {
        self.held()
            .local_groups
            .get(&(group.to_owned(), filter.to_owned()))
            .copied()
            .unwrap_or(0)
    }

    /// A local client joined `group` on `filter`.
    ///
    /// Returns a write **only when the count left zero**, for
    /// [`Self::subscriber_arrived`]'s reason: every peer already
    /// believes this node has a member, and saying it again is a
    /// register and a frame per client rather than per group.
    pub fn group_joined(&self, group: &str, filter: &str) -> Option<MetadataWrite> {
        let mut state = self.held();
        let count = {
            let held = state
                .local_groups
                .entry((group.to_owned(), filter.to_owned()))
                .or_insert(0);
            *held += 1;
            *held
        };
        if count > 1 {
            return None;
        }
        Some(self.write(
            &mut state,
            MetadataKey::NodeGroup {
                node: self.me,
                group: group.to_owned(),
                filter: filter.to_owned(),
            },
            Some(MetadataValue::NodeGroup),
        ))
    }

    /// A local client left `group` on `filter`.
    ///
    /// Returns a **tombstone** only when the count reached zero.
    /// Dropping the register instead would leave every peer choosing
    /// this node for a group it has nobody in, and a publication chosen
    /// for it would be delivered to nobody at all — which is worse than
    /// an ordinary filter going stale, because the group's other members
    /// were passed over for it.
    pub fn group_left(&self, group: &str, filter: &str) -> Option<MetadataWrite> {
        let key = (group.to_owned(), filter.to_owned());
        let mut state = self.held();
        let count = {
            let held = state.local_groups.get_mut(&key)?;
            *held -= 1;
            *held
        };
        if count > 0 {
            return None;
        }
        state.local_groups.remove(&key);
        Some(self.write(
            &mut state,
            MetadataKey::NodeGroup {
                node: self.me,
                group: group.to_owned(),
                filter: filter.to_owned(),
            },
            None,
        ))
    }

    /// A retained message was set on `topic`.
    pub fn retained(&self, topic: &str, message: RetainedRecord) -> MetadataWrite {
        let mut state = self.held();
        self.write(
            &mut state,
            MetadataKey::Retained {
                topic: topic.to_owned(),
            },
            Some(MetadataValue::Retained(message)),
        )
    }

    /// The retained message on `topic` was cleared.
    pub fn retained_cleared(&self, topic: &str) -> MetadataWrite {
        let mut state = self.held();
        self.write(
            &mut state,
            MetadataKey::Retained {
                topic: topic.to_owned(),
            },
            None,
        )
    }

    /// A rule was installed or edited.
    ///
    /// Section 9.1's rules stop being node-local here. The whole rule
    /// travels, because a peer that held only its identifier would have
    /// nothing to install.
    pub fn rule(&self, rule: RuleRecord) -> MetadataWrite {
        let mut state = self.held();
        let key = MetadataKey::Rule {
            id: rule.id.clone(),
        };
        self.write(&mut state, key, Some(MetadataValue::Rule(Box::new(rule))))
    }

    /// A rule was removed.
    pub fn rule_removed(&self, id: &str) -> MetadataWrite {
        let mut state = self.held();
        self.write(&mut state, MetadataKey::Rule { id: id.to_owned() }, None)
    }

    /// A user was written, whole. Section 12.1.
    pub fn user(&self, user: UserRecord) -> MetadataWrite {
        let mut state = self.held();
        let key = MetadataKey::User {
            name: user.name.clone(),
        };
        self.write(&mut state, key, Some(MetadataValue::User(Box::new(user))))
    }

    /// A user was removed.
    pub fn user_removed(&self, name: &str) -> MetadataWrite {
        let mut state = self.held();
        self.write(
            &mut state,
            MetadataKey::User {
                name: name.to_owned(),
            },
            None,
        )
    }

    /// An access control rule was written, whole. Section 12.1.
    pub fn acl(&self, rule: AclRecord) -> MetadataWrite {
        let mut state = self.held();
        let key = MetadataKey::Acl {
            id: rule.id.clone(),
        };
        self.write(&mut state, key, Some(MetadataValue::Acl(Box::new(rule))))
    }

    /// An access control rule was removed.
    pub fn acl_removed(&self, id: &str) -> MetadataWrite {
        let mut state = self.held();
        self.write(&mut state, MetadataKey::Acl { id: id.to_owned() }, None)
    }

    /// An API key was written, whole. Section 11.1.
    pub fn api_key(&self, key: ApiKeyRecord) -> MetadataWrite {
        let mut state = self.held();
        let metadata_key = MetadataKey::ApiKey { id: key.id.clone() };
        self.write(
            &mut state,
            metadata_key,
            Some(MetadataValue::ApiKey(Box::new(key))),
        )
    }

    /// An API key was revoked.
    pub fn api_key_removed(&self, id: &str) -> MetadataWrite {
        let mut state = self.held();
        self.write(&mut state, MetadataKey::ApiKey { id: id.to_owned() }, None)
    }

    /// Who owns `session`, and at what epoch. Section 8.4.
    ///
    /// `None` for a session no node has claimed and for one whose
    /// register is a tombstone: a released session is nobody's, which is
    /// the same answer as never having been claimed, and a caller that
    /// had to tell the two apart would be a caller that cared when a
    /// deletion gets collected.
    #[must_use]
    pub fn session_owner(&self, session: &SessionKey) -> Option<Ownership> {
        owner_of(&self.held(), session)
    }

    /// Claim `session` for this node, section 8.4.
    ///
    /// Reads the register, increments the epoch and writes this node as
    /// the owner — **under one acquisition of the lock**, because the
    /// epoch written is derived from the epoch read and two claims that
    /// read before either wrote would write one number twice. Two
    /// writes sharing a stamp is the condition [`crate::register`] says
    /// convergence depends on; two writes sharing an *epoch* is merely
    /// an election with no decision in it, and this node is the one
    /// place both halves are available at once.
    ///
    /// A claim by the node that already owns it writes nothing: see
    /// [`Claim::write`].
    pub fn session_claimed(&self, session: &SessionKey) -> Claim {
        let mut state = self.held();
        let previous = owner_of(&state, session);
        if let Some(held) = previous {
            if held.node == self.me {
                return Claim {
                    previous,
                    epoch: held.epoch,
                    write: None,
                };
            }
        }
        let epoch = previous.map_or(0, |held| held.epoch) + 1;
        let write = self.write(
            &mut state,
            MetadataKey::SessionOwner {
                session: session.clone(),
            },
            Some(MetadataValue::SessionOwner {
                node: self.me,
                epoch,
            }),
        );
        Claim {
            previous,
            epoch,
            write: Some(write),
        }
    }

    /// A session is owned by `owner` at `epoch`, section 8.4.
    ///
    /// The unconditional form, for a caller that already knows both —
    /// a restore, or a test. [`Replicated::session_claimed`] is what a
    /// connect goes through, because only it can read the epoch and
    /// write the next one without letting go of the lock in between.
    pub fn session_owned(&self, session: SessionKey, owner: NodeId, epoch: u64) -> MetadataWrite {
        let mut state = self.held();
        self.write(
            &mut state,
            MetadataKey::SessionOwner { session },
            Some(MetadataValue::SessionOwner { node: owner, epoch }),
        )
    }

    /// A session is owned by nobody.
    pub fn session_released(&self, session: SessionKey) -> MetadataWrite {
        let mut state = self.held();
        self.write(&mut state, MetadataKey::SessionOwner { session }, None)
    }

    /// Every tombstone this node holds, with the stamp that ordered it.
    ///
    /// What a sweep looks at. A value is not here: a register with a value
    /// is not a deletion and is never collected, because collecting it
    /// would be losing it.
    #[must_use]
    pub fn tombstones(&self) -> Vec<(MetadataKey, Stamp)> {
        self.held()
            .registers
            .iter()
            .filter(|(_, register)| register.is_tombstone())
            .map(|(key, register)| (key.clone(), register.stamp()))
            .collect()
    }

    /// Forget the tombstone under `key`, and say what to store.
    ///
    /// **Only a tombstone, and only by a caller that has decided every
    /// node has seen it.** A register with a value is left alone and an
    /// empty batch comes back: collecting one would be deleting a record
    /// nothing asked to delete, and the decision about what everybody has
    /// seen is not one this type can make.
    pub fn collect(&self, key: &MetadataKey) -> Vec<Record> {
        let mut state = self.held();
        match state.registers.get(key) {
            Some(register) if register.is_tombstone() => {}
            _ => return Vec::new(),
        }
        state.registers.remove(key);
        // The fourth of [`index`]'s four paths. Only a tombstone reaches
        // here, so the map is already answering without this node for
        // this filter and nothing changes — which is exactly why it has
        // to be said anyway: an index left holding a row for a register
        // that no longer exists is a leak, and the row is the only thing
        // that would notice.
        index(&self.routes, &self.groups, key, None);
        vec![Record::MetadataRemoved {
            key: key.to_record(),
        }]
    }

    /// Take a peer's copy of a register, and say whether anything changed.
    ///
    /// [`Register::merge`] and nothing else: there is one comparison and
    /// a tombstone is not a special case in it. A copy that lost is a copy
    /// that told us nothing, so `false` is what a caller uses to decide
    /// not to pass it on.
    pub fn observed(&self, write: MetadataWrite) -> bool {
        let mut state = self.held();
        // Section 8.2's clock, the same as in `observed_from`: a peer's
        // reading is taken before its write is weighed, and one beyond
        // the bound is refused and tells this node nothing.
        if self
            .clock
            .observe(crate::clock::wall_millis(), write.stamp().reading())
            .is_err()
        {
            return false;
        }
        let won = merge(&mut state, &write);
        if won {
            index(
                &self.routes,
                &self.groups,
                write.key(),
                Some(write.register()),
            );
        }
        won
    }

    /// Stamp a value, hold it, and hand back the write.
    ///
    /// **The one place a stamp is issued**, so that every kind is ordered
    /// by the same clock and written down the same way. `None` is a
    /// tombstone, and it goes through here like anything else: see the
    /// module documentation.
    fn write(
        &self,
        state: &mut State,
        key: MetadataKey,
        value: Option<MetadataValue>,
    ) -> MetadataWrite {
        let stamp = Stamp::new(self.clock.now(), self.me);
        let register = match value {
            Some(value) => Register::written(value, stamp),
            None => Register::deleted(stamp),
        };
        state.registers.insert(key.clone(), register.clone());
        // The second of [`index`]'s four paths: this node's own write.
        // Its own registers are in the map like any other node's, and
        // [`crate::forward`] is what leaves this node out of a forward —
        // see [`Routes::nodes_for`] for why the subtraction is there and
        // not here.
        index(&self.routes, &self.groups, &key, Some(&register));
        let write = MetadataWrite::new(key, register);
        // Numbered here and published later, which is the two-phase write
        // the module documentation is about: the number is spent now so
        // that two writes cannot share one, and the entry is servable only
        // once the store has taken the batch.
        state.oplog.reserve(write.clone());
        write
    }

    /// The state behind the lock, recovering from a poisoned one.
    ///
    /// The same decision [`HybridClock`] records: poisoning would turn
    /// one unrelated panic into every later write on this node failing,
    /// which is worse than what it protects.
    fn held(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests;
