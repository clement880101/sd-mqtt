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
//! # A known limitation: a write is held before the store has taken it
//!
//! Every method below stamps the value and holds it, and the caller then
//! gives [`Replicated::records`] to a store. **A store that refuses the
//! batch leaves this node holding a register the store does not have**,
//! and the restart after it resumes the clock from the older mark and
//! holds no such register.
//!
//! Recorded rather than papered over, because it is narrow and the two
//! cheap-looking repairs are both worse. Nothing is lost by it: the stamp
//! the refused write used was shown to nobody, which is exactly the case
//! [`crate::clock`] documents, and the next successful write of that key
//! supersedes the one held. What it does cost is a window in which an
//! exchange would offer a peer a register this node could not store — and
//! the caller whose batch was refused has already failed whatever it was
//! doing and said so. Closing it properly means a two-phase write, and
//! that belongs with the broadcast that would expose it rather than here.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, MutexGuard, PoisonError};

use storage::{
    MetadataKeyRecord, MetadataRecord, MetadataValueRecord, Record, RetainedRecord, RuleRecord,
    SessionKey, StampRecord,
};

use crate::clock::HybridClock;
use crate::identity::NodeId;
use crate::register::{Register, Stamp};

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
    /// Which node owns the session the key names.
    SessionOwner {
        /// The owner.
        node: NodeId,
    },
}

impl MetadataValue {
    /// What a store holds this as.
    #[must_use]
    pub fn to_record(&self) -> MetadataValueRecord {
        match self {
            Self::NodeSubscription => MetadataValueRecord::NodeSubscription,
            Self::Retained(message) => MetadataValueRecord::Retained(message.clone()),
            Self::Rule(rule) => MetadataValueRecord::Rule(rule.clone()),
            Self::SessionOwner { node } => MetadataValueRecord::SessionOwner {
                node: node.to_bytes(),
            },
        }
    }

    /// A value read back from a store.
    #[must_use]
    pub fn from_record(record: MetadataValueRecord) -> Self {
        match record {
            MetadataValueRecord::NodeSubscription => Self::NodeSubscription,
            MetadataValueRecord::Retained(message) => Self::Retained(message),
            MetadataValueRecord::Rule(rule) => Self::Rule(rule),
            MetadataValueRecord::SessionOwner { node } => Self::SessionOwner {
                node: NodeId::from_bytes(node),
            },
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
}

/// What [`Replicated`] holds.
#[derive(Debug, Default)]
struct State {
    /// Every register, by what it is about.
    ///
    /// Ordered, so that what this hands back is comparable with what a
    /// store hands back: see [`storage::Restored::metadata`].
    registers: BTreeMap<MetadataKey, Register<MetadataValue>>,
    /// How many of *this node's* subscribers each filter has.
    ///
    /// **Not replicated and not stored.** An entry is removed when it
    /// reaches zero, so this grows with the filters this node's clients
    /// are actually subscribed to rather than with every filter it has
    /// ever seen.
    local_filters: HashMap<String, usize>,
}

impl Replicated {
    /// This node's replicated state, resumed from what the store held.
    ///
    /// `stored` is [`storage::Restored::metadata`], tombstones included:
    /// a node that dropped a tombstone on restart would be handed its own
    /// older copy by the next peer it spoke to and would resurrect what
    /// it deleted.
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
        stored: &[MetadataRecord],
    ) -> Result<Self, MetadataError> {
        let mut registers = BTreeMap::new();
        for record in stored {
            let write = MetadataWrite::from_record(record.clone())?;
            registers.insert(write.key, write.register);
        }
        Ok(Self {
            me,
            clock,
            state: Mutex::new(State {
                registers,
                local_filters: HashMap::new(),
            }),
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
    /// **The register and the clock's mark, together**, because section
    /// 8.2's resume depends on the mark riding in the same batch as the
    /// write it stamps: a batch is all or nothing, so a stamp that
    /// survived a crash has its mark beside it and a stamp that did not
    /// was shown to nobody. Returned as a pair from one call so that
    /// there is no way for a caller to take the register alone.
    #[must_use]
    pub fn records(&self, write: &MetadataWrite) -> [Record; 2] {
        [
            Record::MetadataPut {
                entry: write.to_record(),
            },
            Record::ClockHighWaterPut {
                reading: self.clock.mark(),
            },
        ]
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

    /// Which nodes have a subscriber for `filter`, in identity order.
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

    /// A session is owned by `owner`, section 8.4.
    pub fn session_owned(&self, session: SessionKey, owner: NodeId) -> MetadataWrite {
        let mut state = self.held();
        self.write(
            &mut state,
            MetadataKey::SessionOwner { session },
            Some(MetadataValue::SessionOwner { node: owner }),
        )
    }

    /// A session is owned by nobody.
    pub fn session_released(&self, session: SessionKey) -> MetadataWrite {
        let mut state = self.held();
        self.write(&mut state, MetadataKey::SessionOwner { session }, None)
    }

    /// Take a peer's copy of a register, and say whether anything changed.
    ///
    /// [`Register::merge`] and nothing else: there is one comparison and
    /// a tombstone is not a special case in it. A copy that lost is a copy
    /// that told us nothing, so `false` is what a caller uses to decide
    /// not to pass it on.
    pub fn observed(&self, write: MetadataWrite) -> bool {
        let mut state = self.held();
        match state.registers.get_mut(&write.key) {
            Some(held) => held.merge(write.register),
            None => {
                state.registers.insert(write.key, write.register);
                true
            }
        }
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
        MetadataWrite::new(key, register)
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
