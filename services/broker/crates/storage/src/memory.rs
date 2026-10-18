//! A store in memory.
//!
//! This is not a toy. Section 7.6 builds one real backend and keeps the
//! trait honest with this one, so it stands in for the second engine that
//! would otherwise be the only thing stopping the trait from drifting into
//! a description of redb. It is also what every other crate's tests will
//! use, and in a later task it is the model the redb backend is compared
//! against in a property test, where an approximation would mean comparing
//! two wrong answers.
//!
//! So it is written to the contract rather than to what is convenient:
//!
//! - **Depth is a stored counter**, although `BTreeMap::len` is sitting
//!   right there. The trait says a counter because at least one engine's
//!   length includes entries it has deleted, and a backend that answered
//!   from its own length would make the contract untestable here: a depth
//!   that cannot drift is a depth no test can prove is maintained.
//! - **The high-water mark is stored**, for the same reason and a sharper
//!   one: after a queue drains there is nothing left to derive it from.
//! - **A batch is applied to a copy and swapped in.** Atomicity is a
//!   promise about failures, and the cheapest way to keep it exactly is to
//!   have nothing to undo.
//!
//! What it deliberately is not: durable. [`MemoryStorage::flush`] succeeds
//! and means nothing, because a process that exits takes the store with
//! it. Everything in this crate about surviving a restart is proved
//! against redb, which is the only backend that can fail it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Mutex, PoisonError};

use crate::record::{
    AclRecord, ApiKeyRecord, BodyRecord, ClockRecord, InflightRecord, LoggedRecord, MessageId,
    MetadataKeyRecord, MetadataRecord, NodeIdentityRecord, OutboxRecord, QueueEntry, Record,
    RetainedRecord, RuleRecord, SeenRecord, SessionKey, SessionRecord, SubscriptionRecord,
    UserRecord,
};
use crate::restore::{Restored, RestoredReplica, RestoredSession};
use crate::{Rejection, Storage, StorageError};

/// A [`Storage`] that keeps everything in maps and loses it on exit.
#[derive(Debug, Default)]
pub struct MemoryStorage {
    /// The whole store behind one lock.
    ///
    /// One lock rather than one per table because a batch is atomic across
    /// tables: a session and the queue entry that belongs to it must
    /// appear together or not at all, and two locks cannot promise that
    /// without an order to take them in.
    state: Mutex<State>,
}

impl MemoryStorage {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The state, recovering from a poisoned lock.
    ///
    /// A panic in a test holding this lock leaves the state consistent,
    /// because [`Storage::apply`] only ever publishes a batch that
    /// completed, so there is nothing for poisoning to protect against and
    /// a great deal for it to obscure: the first failure would be followed
    /// by every later test failing on the poison instead of its own
    /// assertion.
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Everything the store holds.
#[derive(Debug, Default, Clone)]
struct State {
    /// Sessions by key, ordered so that restore is deterministic.
    sessions: BTreeMap<SessionKey, SessionState>,
    /// Bodies, each with the number of references taken and not released.
    bodies: BTreeMap<MessageId, HeldBody>,
    /// Retained messages by topic.
    retained: BTreeMap<String, RetainedRecord>,
    /// Rules by identifier, ordered so that restore is deterministic.
    rules: BTreeMap<String, RuleRecord>,
    /// Users by name, ordered for the same reason. Section 12.1.
    users: BTreeMap<String, UserRecord>,
    /// Access control rules by identifier. Section 12.1.
    acl: BTreeMap<String, AclRecord>,
    /// API keys by identifier. Section 11.1.
    api_keys: BTreeMap<String, ApiKeyRecord>,
    /// Sessions this node holds a copy of rather than owns, section
    /// 8.6: the owning node's sixteen bytes and how far the stream has
    /// been applied.
    ///
    /// Beside the sessions rather than inside them, because a replica
    /// is a session plus a fact about who owns it — and becoming the
    /// owner is then the removal of a fact rather than a rewrite of
    /// the session.
    replicas: BTreeMap<SessionKey, ([u8; 16], u64)>,
    /// How far this node has sent, for each session it owns, section
    /// 8.6. The owner's counterpart to `replicas`.
    streams: BTreeMap<SessionKey, u64>,
    /// Who this node is, once something has said. Section 8.1.
    ///
    /// One row rather than a table: a store belongs to one node.
    node: Option<NodeIdentityRecord>,
    /// The highest clock reading this node has issued. Section 8.2.
    ///
    /// One row, beside the identity and for the same reason: a store
    /// belongs to one node, and this is that node's clock.
    clock: Option<ClockRecord>,
    /// Every replicated register, by what it is about. Section 8.2.
    ///
    /// Ordered by the key, which is what makes a restore from this
    /// backend comparable with one from redb: redb's key encoding puts
    /// the variant index in a leading byte and escapes each string
    /// component so that its byte order is this `BTreeMap`'s order.
    metadata: BTreeMap<MetadataKeyRecord, MetadataRecord>,
    /// This node's own operation log, by sequence. Section 8.2.
    ///
    /// Ordered, which is the whole point: an exchange asks for
    /// everything after a sequence, and that is a range over this.
    oplog: BTreeMap<u64, MetadataRecord>,
    /// The last sequence collection removed from [`State::oplog`].
    ///
    /// **The only number held about the log**, and the one that makes the
    /// highest sequence ever used derivable rather than stored: the table
    /// holds every entry from here to the top, so the highest is this or
    /// the last key, whichever is larger. A second row saying the same
    /// thing was there first and was deleted, because a mutation that
    /// ignored it could not be made to fail a test — nothing can tell the
    /// two answers apart, which is what convention 5 calls a test that
    /// cannot fail.
    ///
    /// It is also what lets a sequence at or below it be refused: once
    /// every node has read past a number the entry is gone, and nothing
    /// in the table can tell a reuse of it from a first use.
    oplog_collected_through: u64,
    /// How far this node has read each other node's log. Section 8.2.
    ///
    /// Ordered by the node's bytes, so that a restore from this backend
    /// is comparable with one from redb.
    seen: BTreeMap<[u8; 16], u64>,
    /// Each rule's outbox, by the same identifier.
    ///
    /// Beside the rules rather than inside a rule's own entry, because a
    /// rule is replaced whole by a [`Record::RulePut`] and an outbox must
    /// not be: turning a connector off and on again, or editing its URL,
    /// would otherwise throw away every envelope waiting to go.
    outboxes: BTreeMap<String, OutboxState>,
}

/// One rule's outbox and the counters it keeps.
///
/// Counters maintained rather than derived, for the reason the module
/// documentation gives about a queue's: a depth that cannot drift is a
/// depth no test can prove is maintained, and after an outbox drains
/// there is nothing left to derive the high-water mark from.
#[derive(Debug, Clone, Default)]
struct OutboxState {
    /// What waits, by sequence number, which is the order the rule
    /// fired.
    entries: BTreeMap<u64, OutboxRecord>,
    /// How many envelopes are held.
    depth: u64,
    /// The highest sequence ever appended, held or not.
    highest_sequence: u64,
}

/// A body and the count that keeps it alive.
#[derive(Debug, Clone)]
struct HeldBody {
    /// The body itself.
    body: BodyRecord,
    /// References taken by [`Record::BodyPut`] and not yet released.
    ///
    /// Never zero: a body that reaches zero is removed in the same step,
    /// so "present" and "referenced" are the same question.
    references: u64,
}

/// One session and everything that belongs to it.
#[derive(Debug, Clone)]
struct SessionState {
    /// Its durable fields.
    record: SessionRecord,
    /// Subscriptions by filter, which is what identifies one.
    subscriptions: BTreeMap<String, SubscriptionRecord>,
    /// Exactly-once identifiers the peer owns. A set: the peer releases
    /// each one once, however often it was reserved.
    inbound: BTreeSet<u16>,
    /// Sent and unacknowledged, by the identifier the peer will name.
    inflight: BTreeMap<u16, InflightRecord>,
    /// What waits, by sequence number, which is arrival order.
    queue: BTreeMap<u64, QueueEntry>,
    /// How many entries the queue holds.
    ///
    /// Maintained rather than derived; see the module documentation.
    depth: u64,
    /// The highest sequence number ever assigned, held or not.
    highest_sequence: u64,
}

impl SessionState {
    /// A session with nothing in it yet.
    fn new(record: SessionRecord) -> Self {
        Self {
            record,
            subscriptions: BTreeMap::new(),
            inbound: BTreeSet::new(),
            inflight: BTreeMap::new(),
            queue: BTreeMap::new(),
            depth: 0,
            highest_sequence: 0,
        }
    }
}

impl State {
    /// The session a record names, or a refusal saying it is not held.
    fn session_mut(&mut self, session: &SessionKey) -> Result<&mut SessionState, Rejection> {
        self.sessions
            .get_mut(session)
            .ok_or_else(|| Rejection::NoSuchSession {
                session: session.clone(),
            })
    }

    /// The same refusal, for a record that files its row beside the
    /// session rather than inside it.
    ///
    /// The stream and replica rows are keyed by session but held in
    /// their own maps, so they could be written for a session the store
    /// does not hold; a late mark after a removal would then be
    /// inherited by the next session under that key, as a position in
    /// a stream it has none of or as a replica row that makes restore
    /// hand the broker nothing. Held to the rule every other
    /// per-session record is held to.
    fn holds(&self, session: &SessionKey) -> Result<(), Rejection> {
        if self.sessions.contains_key(session) {
            Ok(())
        } else {
            Err(Rejection::NoSuchSession {
                session: session.clone(),
            })
        }
    }

    /// The outbox of the rule a record names, or a refusal saying the
    /// rule is not held.
    ///
    /// Created on first use rather than by [`Record::RulePut`], so that
    /// a rule replaced whole keeps the outbox it already had.
    fn outbox_mut(&mut self, id: &str) -> Result<&mut OutboxState, Rejection> {
        if !self.rules.contains_key(id) {
            return Err(Rejection::NoSuchRule { id: id.to_owned() });
        }
        Ok(self.outboxes.entry(id.to_owned()).or_default())
    }

    /// The same outbox, or nothing for a rule the store does not hold.
    ///
    /// For the records that take an envelope out or mark an attempt on
    /// it: a rule that has gone took its outbox with it, so the envelope
    /// those records name has already left, and what they ask for is
    /// already true. Refusing them would refuse the body release the
    /// rules engine sends in the same batch as the removal, and leak
    /// the payload for the life of the file.
    fn outbox_if_held(&mut self, id: &str) -> Option<&mut OutboxState> {
        self.rules
            .contains_key(id)
            .then(|| self.outboxes.entry(id.to_owned()).or_default())
    }

    /// Apply one record, or refuse it without having changed anything.
    ///
    /// Every arm either succeeds or returns before it writes. That is not
    /// what makes a batch atomic — the copy in [`Storage::apply`] does
    /// that — but it keeps this function readable as a statement of what
    /// each record means.
    fn apply_one(&mut self, record: &Record) -> Result<(), Rejection> {
        match record {
            Record::SessionPut(session) => {
                // An update, not a reset: the broker writes one of these
                // whenever an expiry or a will changes, in the middle of a
                // live session that holds a queue.
                match self.sessions.get_mut(&session.key) {
                    Some(held) => held.record = session.clone(),
                    None => {
                        self.sessions
                            .insert(session.key.clone(), SessionState::new(session.clone()));
                    }
                }
            }
            Record::SessionRemoved { session } => {
                // Everything hanging off the session goes with it,
                // including the counters: a client reconnecting under the
                // same identifier must inherit nothing. Removing a session
                // that is not held is a no-op, because what was asked for
                // is already true.
                //
                // The bodies its entries named are not released. The
                // reference belongs to whoever took it; see the note on
                // `Storage::apply`.
                self.sessions.remove(session);
                // Both stream rows go with it, for the reason the
                // queue does: a client reconnecting under the same
                // identifier must inherit nothing, and a position left
                // behind would have the next stream start part way
                // through a session that has none of it.
                self.replicas.remove(session);
                self.streams.remove(session);
            }
            Record::SubscriptionAdded {
                session,
                subscription,
            } => {
                self.session_mut(session)?
                    .subscriptions
                    .insert(subscription.filter.clone(), subscription.clone());
            }
            Record::SubscriptionRemoved { session, filter } => {
                self.session_mut(session)?.subscriptions.remove(filter);
            }
            Record::InboundReserved { session, packet_id } => {
                self.session_mut(session)?.inbound.insert(*packet_id);
            }
            Record::InboundReleased { session, packet_id } => {
                self.session_mut(session)?.inbound.remove(packet_id);
            }
            Record::InflightPut { session, entry } => {
                self.session_mut(session)?
                    .inflight
                    .insert(entry.packet_id, entry.clone());
            }
            Record::InflightRemoved { session, packet_id } => {
                self.session_mut(session)?.inflight.remove(packet_id);
            }
            Record::QueuePushed { session, entry } => {
                let held = self.session_mut(session)?;
                if entry.sequence <= held.highest_sequence {
                    // Section 7.4: two entries sharing a number, or a
                    // number going backwards, is what must never happen.
                    return Err(Rejection::SequenceReused {
                        session: session.clone(),
                        sequence: entry.sequence,
                        highest: held.highest_sequence,
                    });
                }
                held.queue.insert(entry.sequence, entry.clone());
                held.depth += 1;
                held.highest_sequence = entry.sequence;
            }
            Record::QueuePopped { session, sequence } => {
                let held = self.session_mut(session)?;
                // Only what was actually there is subtracted. A pop of an
                // entry already gone is a no-op, and must not take the
                // depth below what is held: a queue reporting empty while
                // holding messages stops draining.
                if held.queue.remove(sequence).is_some() {
                    held.depth -= 1;
                }
                // The high-water mark is untouched. The number is spent.
            }
            Record::BodyPut { id, body } => {
                // One put is one reference, and a put for a body already
                // held only counts: `Storage::apply` says an identifier
                // names one publication for as long as it exists, so the
                // body a second put carries is the body already held.
                // The redb backend depends on that to avoid rewriting
                // the payload once per subscriber, and the two backends
                // are compared as values, so this one does the same.
                self.bodies
                    .entry(*id)
                    .and_modify(|held| held.references += 1)
                    .or_insert_with(|| HeldBody {
                        body: body.clone(),
                        references: 1,
                    });
            }
            Record::BodyReleased { id } => {
                let Some(held) = self.bodies.get_mut(id) else {
                    // Refused rather than clamped: clamping turns a
                    // caller's double release into a body deleted under a
                    // live reference, silently.
                    return Err(Rejection::BodyNotHeld { id: *id });
                };
                held.references -= 1;
                if held.references == 0 {
                    self.bodies.remove(id);
                }
            }
            Record::RetainedPut { topic, message } => {
                self.retained.insert(topic.clone(), message.clone());
            }
            Record::RetainedRemoved { topic } => {
                self.retained.remove(topic);
            }
            Record::RulePut { rule } => {
                self.rules.insert(rule.id.clone(), rule.clone());
            }
            Record::UserPut(user) => {
                self.users.insert(user.name.clone(), (**user).clone());
            }
            Record::UserDeleted { name } => {
                self.users.remove(name);
            }
            Record::AclPut(rule) => {
                self.acl.insert(rule.id.clone(), (**rule).clone());
            }
            Record::AclDeleted { id } => {
                self.acl.remove(id);
            }
            Record::ApiKeyPut(key) => {
                self.api_keys.insert(key.id.clone(), (**key).clone());
            }
            Record::ApiKeyDeleted { id } => {
                self.api_keys.remove(id);
            }
            Record::ReplicaFollows {
                session,
                owner,
                through,
            } => {
                // Overwritten, not appended to: one row per followed
                // session holding where its stream has got to.
                self.holds(session)?;
                self.replicas.insert(session.clone(), (*owner, *through));
            }
            Record::ReplicaDropped { session } => {
                // **The session stays.** Becoming its owner keeps every
                // queue entry and changes only whose it is.
                self.holds(session)?;
                self.replicas.remove(session);
            }
            Record::StreamAt { session, at } => {
                self.holds(session)?;
                self.streams.insert(session.clone(), *at);
            }
            Record::RuleRemoved { id } => {
                // The outbox goes with the rule, as a session's queue
                // goes with the session. The bodies its envelopes named
                // are not released; the reference belongs to whoever
                // took it.
                self.rules.remove(id);
                self.outboxes.remove(id);
            }
            Record::OutboxAppended { rule, entry } => {
                let held = self.outbox_mut(rule)?;
                if entry.sequence <= held.highest_sequence {
                    return Err(Rejection::OutboxSequenceReused {
                        id: rule.clone(),
                        sequence: entry.sequence,
                        highest: held.highest_sequence,
                    });
                }
                held.entries.insert(entry.sequence, entry.clone());
                held.depth += 1;
                held.highest_sequence = entry.sequence;
            }
            Record::OutboxAttempted {
                rule,
                sequence,
                attempts,
                next_attempt_at,
            } => {
                // A no-op for an envelope that has left: it was
                // delivered or dead-lettered, and recording an attempt
                // on it would resurrect a row nothing would ever remove.
                // A rule that has gone is the same envelope having left
                // by another door.
                if let Some(entry) = self
                    .outbox_if_held(rule)
                    .and_then(|held| held.entries.get_mut(sequence))
                {
                    entry.attempts = *attempts;
                    entry.next_attempt_at = *next_attempt_at;
                }
            }
            Record::NodeIdentityPut { identity } => {
                // Replaced whole, as a rule is by a `RulePut`: the
                // identity is whatever the last put said it was. What
                // must not happen — a fresh UUID minted over one that
                // already exists — is the caller's to prevent, and
                // `Record::NodeIdentityPut` says why it is not the
                // store's.
                self.node = Some(identity.clone());
            }
            Record::ClockHighWaterPut { reading } => {
                // Replaced whole, and not compared with what is held:
                // whether a reading moved forwards is the clock's
                // question and `ClockRecord` says why it is not the
                // store's.
                self.clock = Some(*reading);
            }
            Record::MetadataPut { entry } => {
                // Replaced whole, and not compared with what is held:
                // which of two copies wins is the caller's one
                // comparison, and `Record::MetadataPut` says why it is
                // not the store's. A deletion arrives here as an entry
                // whose value is absent, and is stored like any other.
                self.metadata.insert(entry.key.clone(), entry.clone());
            }
            Record::MetadataRemoved { key } => {
                // A key the store does not hold is a no-op rather than a
                // refusal: see `Record::MetadataRemoved` for why a sweep
                // that ran twice is not a caller's mistake.
                self.metadata.remove(key);
            }
            Record::MetadataSeenPut { seen } => {
                // Replaced whole, and not compared with what is held:
                // whether a watermark moved forwards is the caller's
                // question, as it is for the clock beside it.
                self.seen.insert(seen.node, seen.through);
            }
            Record::OpLogAppended { entry } => {
                // **Already held, or already collected** — not "below the
                // high-water mark". Two of this node's own writes can be
                // numbered under two different locks and reach a store in
                // either order, so a refusal of anything below the highest
                // would refuse the earlier of two perfectly good writes.
                // What must never happen is one number carrying two
                // writes, and these two conditions are exactly that.
                if entry.sequence <= self.oplog_collected_through
                    || self.oplog.contains_key(&entry.sequence)
                {
                    return Err(Rejection::OpLogSequenceReused {
                        sequence: entry.sequence,
                        highest: self.oplog_highest(),
                    });
                }
                self.oplog.insert(entry.sequence, entry.entry.clone());
            }
            Record::OpLogTruncated { through } => {
                // The watermark moves even where it is past the last
                // entry: the numbers are spent whether or not anything was
                // held at them, and this is the only record of that.
                self.oplog.retain(|sequence, _| *sequence > *through);
                self.oplog_collected_through = self.oplog_collected_through.max(*through);
            }
            Record::OutboxRemoved { rule, sequence } => {
                // Only what was actually there is subtracted, for the
                // reason a queue pop gives: an outbox reporting empty
                // while holding envelopes stops draining. A rule that
                // has gone took the envelope with it, so there is
                // nothing to subtract and nothing to refuse.
                if let Some(held) = self.outbox_if_held(rule) {
                    if held.entries.remove(sequence).is_some() {
                        held.depth -= 1;
                    }
                }
                // The high-water mark is untouched. The number is spent.
            }
        }
        Ok(())
    }
}

impl State {
    /// The highest sequence this node's log has ever used.
    ///
    /// Derived rather than held: see [`State::oplog_collected_through`].
    fn oplog_highest(&self) -> u64 {
        std::cmp::max(
            self.oplog_collected_through,
            self.oplog.keys().next_back().copied().unwrap_or_default(),
        )
    }
}

impl Storage for MemoryStorage {
    fn apply(&self, records: &[Record]) -> Result<(), StorageError> {
        let mut state = self.state();
        // Applied to a copy and swapped in. A refusal anywhere in the
        // batch drops the copy and the store is untouched, which is
        // exactly the promise, with nothing to get wrong in an undo path.
        // The copy costs a clone of the whole store per batch, which is
        // the price of that exactness in a backend whose entire purpose is
        // being obviously right; redb pays nothing for it, because a write
        // transaction it does not commit has the same effect.
        let mut staged = state.clone();
        for record in records {
            staged.apply_one(record)?;
        }
        *state = staged;
        Ok(())
    }

    fn flush(&self) -> Result<(), StorageError> {
        // Nothing can be made more durable than memory already is not.
        // Succeeding rather than failing keeps callers on the same path
        // they take against a real store, which is the point of having
        // this backend at all.
        Ok(())
    }

    fn restore(&self) -> Result<Restored, StorageError> {
        let state = self.state();
        let restored = |session: &SessionState| RestoredSession {
            session: session.record.clone(),
            subscriptions: session.subscriptions.values().cloned().collect(),
            inbound: session.inbound.iter().copied().collect(),
            inflight: session.inflight.values().cloned().collect(),
            // In sequence order, which is arrival order, because the
            // queue is keyed by it.
            queue: session.queue.values().cloned().collect(),
            highest_sequence: session.highest_sequence,
            stream_at: state.streams.get(&session.record.key).copied().unwrap_or(0),
        };
        Ok(Restored {
            // **Split by the replica table**, so that a session this
            // node merely holds a copy of never reaches the list the
            // broker is handed. See `Restored::replicas`.
            sessions: state
                .sessions
                .iter()
                .filter(|(key, _)| !state.replicas.contains_key(*key))
                .map(|(_, session)| restored(session))
                .collect(),
            replicas: state
                .replicas
                .iter()
                .filter_map(|(key, (owner, through))| {
                    // A replica row for a session that is not there is
                    // not a thing a batch can write — the session
                    // record and the row ride together — so this is
                    // skipped rather than invented.
                    let session = state.sessions.get(key)?;
                    Some(RestoredReplica {
                        session: restored(session),
                        owner: *owner,
                        through: *through,
                    })
                })
                .collect(),
            retained: state
                .retained
                .iter()
                .map(|(topic, message)| (topic.clone(), message.clone()))
                .collect(),
            bodies: state
                .bodies
                .iter()
                .map(|(id, held)| (*id, held.body.clone()))
                .collect::<HashMap<_, _>>(),
            rules: state.rules.values().cloned().collect(),
            users: state.users.values().cloned().collect(),
            acl: state.acl.values().cloned().collect(),
            api_keys: state.api_keys.values().cloned().collect(),
            node: state.node.clone(),
            clock: state.clock,
            metadata: state.metadata.values().cloned().collect(),
            oplog: state
                .oplog
                .iter()
                .map(|(sequence, entry)| LoggedRecord {
                    sequence: *sequence,
                    entry: entry.clone(),
                })
                .collect(),
            oplog_collected_through: state.oplog_collected_through,
            seen: state
                .seen
                .iter()
                .map(|(node, through)| SeenRecord {
                    node: *node,
                    through: *through,
                })
                .collect(),
        })
    }

    fn queue_head(
        &self,
        session: &SessionKey,
        cursor: u64,
    ) -> Result<Option<QueueEntry>, StorageError> {
        Ok(self
            .state()
            .sessions
            .get(session)
            .and_then(|held| held.queue.range(cursor..).next())
            .map(|(_, entry)| entry.clone()))
    }

    fn queue_depth(&self, session: &SessionKey) -> Result<u64, StorageError> {
        Ok(self
            .state()
            .sessions
            .get(session)
            .map_or(0, |held| held.depth))
    }

    fn highest_sequence(&self, session: &SessionKey) -> Result<u64, StorageError> {
        Ok(self
            .state()
            .sessions
            .get(session)
            .map_or(0, |held| held.highest_sequence))
    }

    /// Under one lock, which is the memory store's one moment.
    fn session_copy(&self, session: &SessionKey) -> Result<crate::SessionCopy, StorageError> {
        let state = self.state();
        let Some(held) = state.sessions.get(session) else {
            return Ok(crate::SessionCopy {
                stream_at: Some(0),
                ..crate::SessionCopy::default()
            });
        };
        Ok(crate::SessionCopy {
            queued: held
                .queue
                .values()
                .filter_map(|entry| {
                    let body = state.bodies.get(&entry.delivery.body)?;
                    Some((entry.clone(), body.body.clone()))
                })
                .collect(),
            highest_sequence: held.highest_sequence,
            stream_at: Some(state.streams.get(session).copied().unwrap_or(0)),
        })
    }

    fn outbox_head(&self, rule: &str, cursor: u64) -> Result<Option<OutboxRecord>, StorageError> {
        Ok(self
            .state()
            .outboxes
            .get(rule)
            .and_then(|held| held.entries.range(cursor..).next())
            .map(|(_, entry)| entry.clone()))
    }

    fn outbox_depth(&self, rule: &str) -> Result<u64, StorageError> {
        Ok(self.state().outboxes.get(rule).map_or(0, |held| held.depth))
    }

    fn outbox_highest_sequence(&self, rule: &str) -> Result<u64, StorageError> {
        Ok(self
            .state()
            .outboxes
            .get(rule)
            .map_or(0, |held| held.highest_sequence))
    }

    fn body(&self, id: &MessageId) -> Result<Option<BodyRecord>, StorageError> {
        Ok(self.state().bodies.get(id).map(|held| held.body.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The contract itself is in `tests/backends.rs`, run against this
    /// backend and, from the next task, against redb. What is here is only
    /// what is true of this backend and not of the trait.
    #[test]
    fn a_memory_store_is_usable_behind_a_shared_reference_from_many_threads() {
        // The trait promises `Send + Sync`; this is the backend actually
        // keeping it, which the type-level assertion in `lib.rs` cannot
        // check for an implementation that does not exist yet.
        let store = std::sync::Arc::new(MemoryStorage::new());
        let mut threads = Vec::new();
        for index in 0..4u16 {
            let store = std::sync::Arc::clone(&store);
            threads.push(std::thread::spawn(move || {
                let key = SessionKey::new("", &format!("client-{index}"));
                store
                    .apply(&[Record::SessionPut(SessionRecord {
                        key: key.clone(),
                        version: crate::ProtocolVersion::V5,
                        expiry_interval: 0,
                        expires_at: None,
                        will: None,
                        will_at: None,
                    })])
                    .expect("a session must be storable");
                store.queue_depth(&key).expect("depth must answer")
            }));
        }
        for thread in threads {
            assert_eq!(thread.join().expect("no thread may panic"), 0);
        }
        assert_eq!(
            store
                .restore()
                .expect("restoring must succeed")
                .sessions
                .len(),
            4
        );
    }
}
