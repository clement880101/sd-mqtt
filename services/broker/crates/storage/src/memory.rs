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
    BodyRecord, ClockRecord, InflightRecord, MessageId, NodeIdentityRecord, OutboxRecord,
    QueueEntry, Record, RetainedRecord, RuleRecord, SessionKey, SessionRecord, SubscriptionRecord,
};
use crate::restore::{Restored, RestoredSession};
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
    /// Who this node is, once something has said. Section 8.1.
    ///
    /// One row rather than a table: a store belongs to one node.
    node: Option<NodeIdentityRecord>,
    /// The highest clock reading this node has issued. Section 8.2.
    ///
    /// One row, beside the identity and for the same reason: a store
    /// belongs to one node, and this is that node's clock.
    clock: Option<ClockRecord>,
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
                if let Some(entry) = self.outbox_mut(rule)?.entries.get_mut(sequence) {
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
            Record::OutboxRemoved { rule, sequence } => {
                let held = self.outbox_mut(rule)?;
                // Only what was actually there is subtracted, for the
                // reason a queue pop gives: an outbox reporting empty
                // while holding envelopes stops draining.
                if held.entries.remove(sequence).is_some() {
                    held.depth -= 1;
                }
                // The high-water mark is untouched. The number is spent.
            }
        }
        Ok(())
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
        Ok(Restored {
            sessions: state
                .sessions
                .values()
                .map(|session| RestoredSession {
                    session: session.record.clone(),
                    subscriptions: session.subscriptions.values().cloned().collect(),
                    inbound: session.inbound.iter().copied().collect(),
                    inflight: session.inflight.values().cloned().collect(),
                    // In sequence order, which is arrival order, because
                    // the queue is keyed by it.
                    queue: session.queue.values().cloned().collect(),
                    highest_sequence: session.highest_sequence,
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
            node: state.node.clone(),
            clock: state.clock,
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
