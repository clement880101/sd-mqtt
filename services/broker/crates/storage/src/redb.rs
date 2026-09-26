//! The store on disk.
//!
//! One redb database, one set of tables for the whole of it rather than
//! one set per session, and every key built by hand so that the byte order
//! redb sorts by is the order this crate means.
//!
//! # Three things the key encoding has to get right
//!
//! **The sequence number is big-endian.** redb orders by bytes. A sequence
//! written in the machine's own order sorts 256 before 2, so a queue comes
//! back in the wrong order after a restart, and only for queues long
//! enough to cross a byte boundary, which is to say only in production.
//! No in-memory backend can fail this and no round trip can see it.
//!
//! **A session's key is prefix-free.** Every per-session table keys on the
//! session followed by something else, so if one session's encoded key
//! could be a prefix of another's, client `a`'s sequence 25185 would be
//! client `ab`'s sequence 0: one client reading another's messages. The
//! mountpoint and the client identifier are therefore escaped and
//! terminated rather than glued together, by [`push_component`].
//!
//! **The escape keeps the order.** A length prefix would also be
//! prefix-free and would be cheaper, but it sorts by length before
//! content, so the sessions would come back in an order that is not the
//! order of their keys. Terminating instead costs a scan per key and buys
//! a restore whose every part comes back in an order the in-memory backend
//! produces too, which is what lets the two be compared as values.
//!
//! # Durability
//!
//! [`RedbStorage::apply`] commits with [`Durability::None`]. That is not a
//! shortcut: task 7's batching writer is what turns many batches into few
//! durable commits, and committing every batch immediately would make it
//! pointless while costing roughly 360 operations a second.
//! [`RedbStorage::flush`] is what makes everything before it durable, and
//! what that means in redb is recorded on it.

use std::path::{Path, PathBuf};

use ::redb::{
    Database, Durability, ReadOnlyTable, ReadableDatabase, ReadableTable, Table, TableDefinition,
    WriteTransaction,
};
use serde::Serialize;

use crate::record::{
    decode_exact, FormatError, FormatMarker, MessageId, OutboxRecord, QueueEntry, Record,
    SessionKey, SessionRecord,
};
use crate::restore::{Restored, RestoredSession};
use crate::{Rejection, Storage, StorageError, SyncMode};

/// Anything global to the database, of which there is currently one thing.
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
/// Where the format marker lives in [`META`].
const FORMAT_KEY: &str = "format";
/// Session key to [`SessionRecord`].
const SESSIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("sessions");
/// Session key and filter, to [`SubscriptionRecord`].
const SUBSCRIPTIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("subscriptions");
/// Session key and packet identifier, to nothing: this is a set.
const INBOUND: TableDefinition<&[u8], ()> = TableDefinition::new("inbound");
/// Session key and packet identifier, to [`InflightRecord`].
const INFLIGHT: TableDefinition<&[u8], &[u8]> = TableDefinition::new("inflight");
/// Session key and big-endian sequence, to [`QueueEntry`].
const QUEUES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("queues");
/// Session key to its depth and high-water mark.
const QUEUE_STATE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("queue_state");
/// Message identifier to its body.
const BODIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bodies");
/// Message identifier to how many references keep its body alive.
///
/// **Its own table rather than a prefix on the body.** The count changes
/// on every put and release; the body never changes at all. Holding the
/// two in one value made every put after the first a read-modify-write
/// of the whole row — deserialise the payload, reserialise it, write it
/// back — and a fan-out emits one put per session for one message. A
/// 256 KiB publication to a thousand durable offline subscribers is one
/// batch of two thousand records, so it paid that a thousand times, on
/// the one storage thread, inside one write transaction, with every
/// waiter behind it blocked. Measured at 27 ms a publication; apart, 3.
/// The figures are in section 17 of the design.
///
/// Section 7.1's claim was about **space** and was never in doubt: one
/// row holds the payload however many sessions name it, and that is
/// still true. What was wrong was the work, and nothing measured it.
const BODY_REFERENCES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("body_references");
/// Topic to [`RetainedRecord`].
const RETAINED: TableDefinition<&str, &[u8]> = TableDefinition::new("retained");
/// Rule identifier to [`crate::record::RuleRecord`].
///
/// Keyed by the identifier as text rather than by a composite, because a
/// rule belongs to the node rather than to a session: there is no prefix
/// to scope it under and nothing to escape.
const RULES: TableDefinition<&str, &[u8]> = TableDefinition::new("rules");
/// Rule identifier and big-endian sequence, to [`OutboxRecord`].
///
/// Keyed like a queue and for the same reasons. The identifier is
/// escaped and terminated by [`push_component`] rather than glued to
/// the sequence, so that rule `a`'s sequence 25185 cannot be rule
/// `ab`'s sequence 0 — one rule posting another's messages. And the
/// sequence is **big-endian**, because redb orders by bytes and this is
/// what decides whether an outbox drains in the order the rule fired.
const OUTBOXES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("outboxes");
/// Rule identifier to its outbox's depth and high-water mark.
const OUTBOX_STATE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("outbox_state");

/// A [`Storage`] backed by a redb database on a path.
#[derive(Debug)]
pub struct RedbStorage {
    /// The database. Its own lock makes two of these on one path
    /// impossible, which is reported as [`StorageError::Locked`].
    database: Database,
    /// Where it is, kept so that an error can say which store it means.
    path: PathBuf,
    /// How hard [`Storage::flush`] tries.
    ///
    /// On the store rather than on the call. Section 7.2 makes it an
    /// operator's choice, which is made once for a deployment, and
    /// [`Storage::flush`] takes no arguments precisely so that a caller
    /// cannot quietly weaken a store's durability for one batch.
    sync: SyncMode,
}

impl RedbStorage {
    /// Open or create a store, flushing as hard as [`SyncMode::Immediate`]
    /// asks.
    ///
    /// # Errors
    ///
    /// [`StorageError::UnsupportedVersion`] if the store was written by a
    /// build whose format this one does not read,
    /// [`StorageError::Locked`] if another store already holds the path,
    /// and [`StorageError::Io`] if the file cannot be opened or created.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open_with(path, SyncMode::default())
    }

    /// Open or create a store that flushes as `sync` asks.
    ///
    /// # Errors
    ///
    /// As [`RedbStorage::open`].
    pub fn open_with(path: impl AsRef<Path>, sync: SyncMode) -> Result<Self, StorageError> {
        let path = path.as_ref().to_path_buf();
        let database = Database::create(&path).map_err(|error| match error {
            // redb holds a lock on the file for as long as the database is
            // open, in this process or another. Two brokers on one
            // directory is an operator mistake worth naming, and it is the
            // one open failure with an obvious fix.
            ::redb::DatabaseError::DatabaseAlreadyOpen => {
                StorageError::Locked { path: path.clone() }
            }
            ::redb::DatabaseError::Storage(storage) => StorageError::from(storage),
            other => StorageError::Engine(other.to_string()),
        })?;

        // The version check happens in a transaction that is committed
        // only if this build may read the store. A refusal drops the
        // transaction, so a store from a later build is left exactly as it
        // was found.
        let transaction = database.begin_write().map_err(StorageError::from)?;
        {
            let mut meta = transaction.open_table(META).map_err(StorageError::from)?;
            let held = meta
                .get(FORMAT_KEY)
                .map_err(StorageError::from)?
                .map(|marker| marker.value().to_vec());
            match held {
                Some(marker) => {
                    FormatMarker::read(&marker)?;
                }
                None => {
                    meta.insert(FORMAT_KEY, FormatMarker::current().encode().as_slice())
                        .map_err(StorageError::from)?;
                }
            }
            // Every table is created here rather than on first use, so
            // that a read transaction never meets a table that does not
            // exist and no read path has to pretend a missing table is an
            // empty one.
            Tables::open(&transaction)?;
        }
        // Immediate: a store whose marker was not written is a store the
        // next open cannot tell from a foreign file.
        commit(transaction, Durability::Immediate)?;

        Ok(Self {
            database,
            path,
            sync,
        })
    }

    /// Where the store is.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// How hard this store's [`Storage::flush`] tries.
    pub fn sync_mode(&self) -> SyncMode {
        self.sync
    }
}

impl Storage for RedbStorage {
    fn apply(&self, records: &[Record]) -> Result<(), StorageError> {
        let transaction = self.database.begin_write().map_err(StorageError::from)?;
        {
            let mut tables = Tables::open(&transaction)?;
            for record in records {
                tables.apply_one(record)?;
            }
        }
        // Not durable. A batch that is refused returns here without
        // committing, and dropping a redb write transaction discards
        // everything in it, which is what makes a batch all or nothing
        // without an undo path to get wrong.
        commit(transaction, Durability::None)
    }

    /// Make everything applied so far durable.
    ///
    /// An empty transaction committed with [`Durability::Immediate`].
    /// Confirmed rather than assumed, in redb 4.3's source and then by
    /// measurement:
    ///
    /// - A non-durable commit updates the header's secondary slot in
    ///   memory and leaves its pages in redb's write buffer
    ///   (`Transaction::non_durable_commit`). Nothing has reached the
    ///   device.
    /// - A durable commit writes out the pages of every non-durable commit
    ///   before it — `durable_commit` begins by flushing the allocations
    ///   they left unpersisted — then writes the header and calls
    ///   `sync_data` on the file. It does this whether or not the
    ///   transaction itself changed anything, because there is no early
    ///   return for an empty one.
    /// - Measured by writing a batch in a child process that then exits
    ///   without running a destructor: with this flush the batch is there
    ///   afterwards, and without it the store is empty. That test is in
    ///   `tests/redb.rs` and it is the only shape of test that can tell
    ///   the difference, because a *clean* close writes out everything
    ///   regardless.
    ///
    /// Under [`SyncMode::Relaxed`] the commit is [`Durability::None`]
    /// instead, which reaches no device at all; see the note there.
    fn flush(&self) -> Result<(), StorageError> {
        let transaction = self.database.begin_write().map_err(StorageError::from)?;
        commit(
            transaction,
            match self.sync {
                SyncMode::Immediate => Durability::Immediate,
                SyncMode::Relaxed => Durability::None,
            },
        )
    }

    fn restore(&self) -> Result<Restored, StorageError> {
        let transaction = self.database.begin_read().map_err(StorageError::from)?;
        let sessions = open_read(&transaction, SESSIONS)?;
        let subscriptions = open_read(&transaction, SUBSCRIPTIONS)?;
        let inbound = transaction
            .open_table(INBOUND)
            .map_err(StorageError::from)?;
        let inflight = open_read(&transaction, INFLIGHT)?;
        let queues = open_read(&transaction, QUEUES)?;
        let queue_state = open_read(&transaction, QUEUE_STATE)?;
        let bodies = open_read(&transaction, BODIES)?;
        let retained = transaction
            .open_table(RETAINED)
            .map_err(StorageError::from)?;
        let rules = transaction.open_table(RULES).map_err(StorageError::from)?;

        let mut restored = Restored::default();
        for entry in sessions.iter().map_err(StorageError::from)? {
            let (key, value) = entry.map_err(StorageError::from)?;
            let record: SessionRecord = decode(value.value())?;
            let prefix = key.value().to_vec();
            let state = read_queue_state(&queue_state, &prefix)?;

            let mut session = RestoredSession {
                session: record,
                subscriptions: Vec::new(),
                inbound: Vec::new(),
                inflight: Vec::new(),
                queue: Vec::new(),
                highest_sequence: state.highest_sequence,
            };
            for entry in range(&subscriptions, &prefix)? {
                let (_, value) = entry.map_err(StorageError::from)?;
                session.subscriptions.push(decode(value.value())?);
            }
            for entry in range(&inbound, &prefix)? {
                let (key, _) = entry.map_err(StorageError::from)?;
                session.inbound.push(trailing_u16(key.value())?);
            }
            for entry in range(&inflight, &prefix)? {
                let (_, value) = entry.map_err(StorageError::from)?;
                session.inflight.push(decode(value.value())?);
            }
            for entry in range(&queues, &prefix)? {
                let (_, value) = entry.map_err(StorageError::from)?;
                session.queue.push(decode(value.value())?);
            }
            restored.sessions.push(session);
        }

        for entry in retained.iter().map_err(StorageError::from)? {
            let (topic, value) = entry.map_err(StorageError::from)?;
            restored
                .retained
                .push((topic.value().to_owned(), decode(value.value())?));
        }
        for entry in bodies.iter().map_err(StorageError::from)? {
            let (id, value) = entry.map_err(StorageError::from)?;
            restored
                .bodies
                .insert(message_id(id.value())?, decode(value.value())?);
        }
        // In key order, which is identifier order, which is what the
        // in-memory backend's `BTreeMap` also produces.
        for entry in rules.iter().map_err(StorageError::from)? {
            let (_, value) = entry.map_err(StorageError::from)?;
            restored.rules.push(decode(value.value())?);
        }
        Ok(restored)
    }

    fn queue_head(
        &self,
        session: &SessionKey,
        cursor: u64,
    ) -> Result<Option<QueueEntry>, StorageError> {
        let transaction = self.database.begin_read().map_err(StorageError::from)?;
        let queues = open_read(&transaction, QUEUES)?;
        let prefix = session_prefix(session);
        // From the cursor rather than from the start of the session, so
        // that a drained queue costs a seek rather than a walk over every
        // entry already gone. redb would answer the other form cheaply
        // too; the engine this was measured against would not.
        let from = queue_key(&prefix, cursor);
        let Some(entry) = range_from(&queues, &prefix, &from)?.next() else {
            return Ok(None);
        };
        let (_, value) = entry.map_err(StorageError::from)?;
        Ok(Some(decode(value.value())?))
    }

    fn queue_depth(&self, session: &SessionKey) -> Result<u64, StorageError> {
        Ok(self.state_of(session)?.depth)
    }

    fn highest_sequence(&self, session: &SessionKey) -> Result<u64, StorageError> {
        Ok(self.state_of(session)?.highest_sequence)
    }

    fn outbox_head(&self, rule: &str, cursor: u64) -> Result<Option<OutboxRecord>, StorageError> {
        let transaction = self.database.begin_read().map_err(StorageError::from)?;
        let outboxes = open_read(&transaction, OUTBOXES)?;
        let prefix = rule_prefix(rule);
        let from = queue_key(&prefix, cursor);
        let Some(entry) = range_from(&outboxes, &prefix, &from)?.next() else {
            return Ok(None);
        };
        let (_, value) = entry.map_err(StorageError::from)?;
        Ok(Some(decode(value.value())?))
    }

    fn outbox_depth(&self, rule: &str) -> Result<u64, StorageError> {
        Ok(self.outbox_state_of(rule)?.depth)
    }

    fn outbox_highest_sequence(&self, rule: &str) -> Result<u64, StorageError> {
        Ok(self.outbox_state_of(rule)?.highest_sequence)
    }
}

impl RedbStorage {
    /// A session's stored counters, or zeroes for a session that holds
    /// none.
    fn state_of(&self, session: &SessionKey) -> Result<QueueState, StorageError> {
        let transaction = self.database.begin_read().map_err(StorageError::from)?;
        let queue_state = open_read(&transaction, QUEUE_STATE)?;
        read_queue_state(&queue_state, &session_prefix(session))
    }

    /// A rule's outbox counters, or zeroes for a rule that holds none.
    fn outbox_state_of(&self, rule: &str) -> Result<QueueState, StorageError> {
        let transaction = self.database.begin_read().map_err(StorageError::from)?;
        let outbox_state = open_read(&transaction, OUTBOX_STATE)?;
        read_queue_state(&outbox_state, &rule_prefix(rule))
    }
}

/// Commit a transaction at a durability level.
fn commit(mut transaction: WriteTransaction, durability: Durability) -> Result<(), StorageError> {
    transaction
        .set_durability(durability)
        .map_err(|error| StorageError::Engine(error.to_string()))?;
    transaction.commit().map_err(StorageError::from)
}

// ---- the tables of one write transaction --------------------------------

/// A table of byte keys to byte values, which is all but two of them.
type ByteTable<'txn> = Table<'txn, &'static [u8], &'static [u8]>;

/// Every table, opened once for a batch.
///
/// Opened together rather than on demand because a batch is atomic across
/// them and the borrow checker is then the thing keeping the transaction
/// alive, rather than a rule someone has to remember.
struct Tables<'txn> {
    sessions: ByteTable<'txn>,
    subscriptions: ByteTable<'txn>,
    inbound: Table<'txn, &'static [u8], ()>,
    inflight: ByteTable<'txn>,
    queues: ByteTable<'txn>,
    queue_state: ByteTable<'txn>,
    bodies: ByteTable<'txn>,
    body_references: ByteTable<'txn>,
    retained: Table<'txn, &'static str, &'static [u8]>,
    rules: Table<'txn, &'static str, &'static [u8]>,
    outboxes: ByteTable<'txn>,
    outbox_state: ByteTable<'txn>,
}

impl<'txn> Tables<'txn> {
    /// Open them all, creating any that a new database does not have.
    fn open(transaction: &'txn WriteTransaction) -> Result<Self, StorageError> {
        Ok(Self {
            sessions: transaction
                .open_table(SESSIONS)
                .map_err(StorageError::from)?,
            subscriptions: transaction
                .open_table(SUBSCRIPTIONS)
                .map_err(StorageError::from)?,
            inbound: transaction
                .open_table(INBOUND)
                .map_err(StorageError::from)?,
            inflight: transaction
                .open_table(INFLIGHT)
                .map_err(StorageError::from)?,
            queues: transaction.open_table(QUEUES).map_err(StorageError::from)?,
            queue_state: transaction
                .open_table(QUEUE_STATE)
                .map_err(StorageError::from)?,
            bodies: transaction.open_table(BODIES).map_err(StorageError::from)?,
            body_references: transaction
                .open_table(BODY_REFERENCES)
                .map_err(StorageError::from)?,
            retained: transaction
                .open_table(RETAINED)
                .map_err(StorageError::from)?,
            rules: transaction.open_table(RULES).map_err(StorageError::from)?,
            outboxes: transaction
                .open_table(OUTBOXES)
                .map_err(StorageError::from)?,
            outbox_state: transaction
                .open_table(OUTBOX_STATE)
                .map_err(StorageError::from)?,
        })
    }

    /// The session a record names, as a key prefix, or a refusal.
    ///
    /// Every record but [`Record::SessionPut`] and
    /// [`Record::SessionRemoved`] goes through here: filing state under a
    /// session the store does not hold would put it somewhere
    /// [`Storage::restore`] never looks, because restore walks sessions.
    fn prefix_of(&self, session: &SessionKey) -> Result<Vec<u8>, StorageError> {
        let prefix = session_prefix(session);
        if self
            .sessions
            .get(prefix.as_slice())
            .map_err(StorageError::from)?
            .is_none()
        {
            return Err(Rejection::NoSuchSession {
                session: session.clone(),
            }
            .into());
        }
        Ok(prefix)
    }

    /// The rule a record names, as a key prefix, or a refusal.
    ///
    /// The same check [`Tables::prefix_of`] makes one level across: an
    /// outbox filed under a rule the store does not hold is reachable by
    /// nothing, because an outbox is read through the rule that owns it.
    fn rule_prefix_of(&self, id: &str) -> Result<Vec<u8>, StorageError> {
        if self.rules.get(id).map_err(StorageError::from)?.is_none() {
            return Err(Rejection::NoSuchRule { id: id.to_owned() }.into());
        }
        Ok(rule_prefix(id))
    }

    /// Apply one record.
    #[allow(clippy::too_many_lines)]
    fn apply_one(&mut self, record: &Record) -> Result<(), StorageError> {
        match record {
            Record::SessionPut(session) => {
                // An update, not a reset: the broker writes one of these
                // whenever an expiry or a will changes, in the middle of a
                // live session that holds a queue. Nothing but the
                // session's own row is touched.
                self.sessions
                    .insert(
                        session_prefix(&session.key).as_slice(),
                        encode(session).as_slice(),
                    )
                    .map_err(StorageError::from)?;
            }
            Record::SessionRemoved { session } => {
                // Everything hanging off the session goes with it,
                // counters included: a client reconnecting under the same
                // identifier must inherit nothing. Removing a session that
                // is not held is a no-op, because what was asked for is
                // already true.
                //
                // The bodies its entries named are not released; the
                // reference belongs to whoever took it. See `Storage::apply`.
                let prefix = session_prefix(session);
                self.sessions
                    .remove(prefix.as_slice())
                    .map_err(StorageError::from)?;
                self.queue_state
                    .remove(prefix.as_slice())
                    .map_err(StorageError::from)?;
                remove_prefixed(&mut self.subscriptions, &prefix)?;
                remove_prefixed(&mut self.inflight, &prefix)?;
                remove_prefixed(&mut self.queues, &prefix)?;
                let end = prefix_end(&prefix);
                self.inbound
                    .retain_in::<&[u8], _>(prefix_bounds(&prefix, end.as_deref()), |_, ()| false)
                    .map_err(StorageError::from)?;
            }
            Record::SubscriptionAdded {
                session,
                subscription,
            } => {
                let key = joined(&self.prefix_of(session)?, subscription.filter.as_bytes());
                self.subscriptions
                    .insert(key.as_slice(), encode(subscription).as_slice())
                    .map_err(StorageError::from)?;
            }
            Record::SubscriptionRemoved { session, filter } => {
                let key = joined(&self.prefix_of(session)?, filter.as_bytes());
                self.subscriptions
                    .remove(key.as_slice())
                    .map_err(StorageError::from)?;
            }
            Record::InboundReserved { session, packet_id } => {
                // A set: the peer releases each identifier once, however
                // often it was reserved, so a second reservation must not
                // be a second thing to release.
                let key = joined(&self.prefix_of(session)?, &packet_id.to_be_bytes());
                self.inbound
                    .insert(key.as_slice(), ())
                    .map_err(StorageError::from)?;
            }
            Record::InboundReleased { session, packet_id } => {
                let key = joined(&self.prefix_of(session)?, &packet_id.to_be_bytes());
                self.inbound
                    .remove(key.as_slice())
                    .map_err(StorageError::from)?;
            }
            Record::InflightPut { session, entry } => {
                let key = joined(&self.prefix_of(session)?, &entry.packet_id.to_be_bytes());
                self.inflight
                    .insert(key.as_slice(), encode(entry).as_slice())
                    .map_err(StorageError::from)?;
            }
            Record::InflightRemoved { session, packet_id } => {
                let key = joined(&self.prefix_of(session)?, &packet_id.to_be_bytes());
                self.inflight
                    .remove(key.as_slice())
                    .map_err(StorageError::from)?;
            }
            Record::QueuePushed { session, entry } => {
                let prefix = self.prefix_of(session)?;
                let mut state = read_queue_state(&self.queue_state, &prefix)?;
                if entry.sequence <= state.highest_sequence {
                    // Section 7.4: two entries sharing a number, or a
                    // number going backwards, is what must never happen.
                    return Err(Rejection::SequenceReused {
                        session: session.clone(),
                        sequence: entry.sequence,
                        highest: state.highest_sequence,
                    }
                    .into());
                }
                self.queues
                    .insert(
                        queue_key(&prefix, entry.sequence).as_slice(),
                        encode(entry).as_slice(),
                    )
                    .map_err(StorageError::from)?;
                state.depth += 1;
                state.highest_sequence = entry.sequence;
                self.write_queue_state(&prefix, state)?;
            }
            Record::QueuePopped { session, sequence } => {
                let prefix = self.prefix_of(session)?;
                // Only what was actually there is subtracted. A pop of an
                // entry already gone must not take the depth below what is
                // held: a queue reporting empty while holding messages
                // stops draining. The high-water mark is untouched either
                // way, because the number is spent.
                let removed = self
                    .queues
                    .remove(queue_key(&prefix, *sequence).as_slice())
                    .map_err(StorageError::from)?
                    .is_some();
                if removed {
                    let mut state = read_queue_state(&self.queue_state, &prefix)?;
                    state.depth -= 1;
                    self.write_queue_state(&prefix, state)?;
                }
            }
            Record::BodyPut { id, body } => {
                // One put is one reference, and the count is stored
                // rather than rebuilt by scanning every queue at
                // startup, which section 7.2 requires and a broker with
                // a large store could not afford.
                //
                // **A put for a body already held touches eight bytes
                // and never looks at the payload.** The body is written
                // once, by the put that takes the first reference; the
                // ones after it only count. `Storage::apply` says why
                // that is not a loss of anything: an identifier names
                // one publication for as long as it exists, so the body
                // a second put carries is the body already held.
                let references = read_references(&self.body_references, id)? + 1;
                self.body_references
                    .insert(&id.0[..], &references.to_be_bytes()[..])
                    .map_err(StorageError::from)?;
                if references == 1 {
                    self.bodies
                        .insert(&id.0[..], encode(body).as_slice())
                        .map_err(StorageError::from)?;
                }
            }
            Record::BodyReleased { id } => {
                let references = read_references(&self.body_references, id)?;
                if references == 0 {
                    // Refused rather than clamped: clamping turns a
                    // caller's double release into a body deleted under a
                    // live reference, silently.
                    return Err(Rejection::BodyNotHeld { id: *id }.into());
                }
                if references == 1 {
                    self.body_references
                        .remove(&id.0[..])
                        .map_err(StorageError::from)?;
                    self.bodies.remove(&id.0[..]).map_err(StorageError::from)?;
                } else {
                    self.body_references
                        .insert(&id.0[..], &(references - 1).to_be_bytes()[..])
                        .map_err(StorageError::from)?;
                }
            }
            Record::RetainedPut { topic, message } => {
                self.retained
                    .insert(topic.as_str(), encode(message).as_slice())
                    .map_err(StorageError::from)?;
            }
            Record::RetainedRemoved { topic } => {
                self.retained
                    .remove(topic.as_str())
                    .map_err(StorageError::from)?;
            }
            Record::RulePut { rule } => {
                self.rules
                    .insert(rule.id.as_str(), encode(rule).as_slice())
                    .map_err(StorageError::from)?;
            }
            Record::RuleRemoved { id } => {
                // The outbox goes with the rule, as a session's queue
                // goes with the session, counters included. The bodies
                // its envelopes named are not released; the reference
                // belongs to whoever took it.
                self.rules.remove(id.as_str()).map_err(StorageError::from)?;
                let prefix = rule_prefix(id);
                self.outbox_state
                    .remove(prefix.as_slice())
                    .map_err(StorageError::from)?;
                remove_prefixed(&mut self.outboxes, &prefix)?;
            }
            Record::OutboxAppended { rule, entry } => {
                let prefix = self.rule_prefix_of(rule)?;
                let mut state = read_queue_state(&self.outbox_state, &prefix)?;
                if entry.sequence <= state.highest_sequence {
                    return Err(Rejection::OutboxSequenceReused {
                        id: rule.clone(),
                        sequence: entry.sequence,
                        highest: state.highest_sequence,
                    }
                    .into());
                }
                self.outboxes
                    .insert(
                        queue_key(&prefix, entry.sequence).as_slice(),
                        encode(entry).as_slice(),
                    )
                    .map_err(StorageError::from)?;
                state.depth += 1;
                state.highest_sequence = entry.sequence;
                self.write_outbox_state(&prefix, state)?;
            }
            Record::OutboxAttempted {
                rule,
                sequence,
                attempts,
                next_attempt_at,
            } => {
                let prefix = self.rule_prefix_of(rule)?;
                let key = queue_key(&prefix, *sequence);
                // A no-op for an envelope that has left: recording an
                // attempt on it would write back a row nothing would
                // ever remove.
                let held = self
                    .outboxes
                    .get(key.as_slice())
                    .map_err(StorageError::from)?
                    .map(|value| value.value().to_vec());
                if let Some(bytes) = held {
                    let mut entry: OutboxRecord = decode(&bytes)?;
                    entry.attempts = *attempts;
                    entry.next_attempt_at = *next_attempt_at;
                    self.outboxes
                        .insert(key.as_slice(), encode(&entry).as_slice())
                        .map_err(StorageError::from)?;
                }
            }
            Record::OutboxRemoved { rule, sequence } => {
                let prefix = self.rule_prefix_of(rule)?;
                let mut state = read_queue_state(&self.outbox_state, &prefix)?;
                // Only what was actually there is subtracted, for the
                // reason a queue pop gives.
                if self
                    .outboxes
                    .remove(queue_key(&prefix, *sequence).as_slice())
                    .map_err(StorageError::from)?
                    .is_some()
                {
                    state.depth -= 1;
                    // The high-water mark is untouched. The number is
                    // spent.
                    self.write_outbox_state(&prefix, state)?;
                }
            }
        }
        Ok(())
    }

    /// Write a session's counters.
    ///
    /// **The row is never removed, including for a queue that has
    /// drained.** A depth of zero is not nothing left to say: the
    /// high-water mark beside it is the highest sequence ever assigned,
    /// and section 7.4 requires it to outlive every entry that carried
    /// one. Removing the row would read back as zero and hand out
    /// numbers the store has already spent, and every batch carrying
    /// one would be refused as [`Rejection::SequenceReused`].
    ///
    /// What does remove it is [`Record::SessionRemoved`], because then
    /// the mark is not owed to anybody: a client reconnecting under the
    /// same identifier inherits nothing.
    fn write_queue_state(&mut self, prefix: &[u8], state: QueueState) -> Result<(), StorageError> {
        self.queue_state
            .insert(prefix, state.encode().as_slice())
            .map_err(StorageError::from)?;
        Ok(())
    }

    /// Write a rule's outbox counters.
    ///
    /// The row is never removed by anything an outbox does, for the
    /// reason [`Tables::write_queue_state`] gives: the high-water mark
    /// outlives every envelope that carried one, and a row read back as
    /// zero would hand out numbers the store has already spent. What
    /// removes it is [`Record::RuleRemoved`].
    fn write_outbox_state(&mut self, prefix: &[u8], state: QueueState) -> Result<(), StorageError> {
        self.outbox_state
            .insert(prefix, state.encode().as_slice())
            .map_err(StorageError::from)?;
        Ok(())
    }
}

// ---- the counters a queue keeps -----------------------------------------

/// What a session's queue holds, and the highest number it ever assigned.
///
/// Stored rather than derived. The depth cannot come from the engine's own
/// length — at least one engine counts entries it has deleted — and the
/// high-water mark cannot come from the keys at all, because a drained
/// queue has none left.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct QueueState {
    /// How many entries are held.
    depth: u64,
    /// The highest sequence ever assigned, held or not.
    highest_sequence: u64,
}

impl QueueState {
    /// Sixteen bytes: the depth then the mark, both big-endian.
    ///
    /// Written by hand rather than through `postcard` so that the value is
    /// fixed width and legible in a hex dump, and so that this table needs
    /// no record type of its own.
    fn encode(self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.depth.to_be_bytes());
        bytes[8..].copy_from_slice(&self.highest_sequence.to_be_bytes());
        bytes
    }

    /// Read them back, refusing anything that is not sixteen bytes.
    fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let (depth, rest) = split_u64(bytes)?;
        let (highest_sequence, rest) = split_u64(rest)?;
        if !rest.is_empty() {
            return Err(StorageError::Corrupt(FormatError::Trailing {
                extra: rest.len(),
            }));
        }
        Ok(Self {
            depth,
            highest_sequence,
        })
    }
}

/// A session's counters, or zeroes when it holds none.
///
/// Zeroes rather than an error: an unknown session reads as empty, and a
/// session that has never been pushed to holds no row.
fn read_queue_state(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    prefix: &[u8],
) -> Result<QueueState, StorageError> {
    match table.get(prefix).map_err(StorageError::from)? {
        Some(value) => QueueState::decode(value.value()),
        None => Ok(QueueState::default()),
    }
}

// ---- values -------------------------------------------------------------

/// Encode a value with the crate's encoding.
fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("encoding into a Vec cannot run out of room")
}

/// Decode a stored value, requiring that it uses every byte.
fn decode<'a, T: serde::Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, StorageError> {
    decode_exact(bytes).map_err(StorageError::from)
}

/// How many references a body has, and zero for one the store does not
/// hold.
///
/// Eight big-endian bytes, written by hand for the reason
/// [`QueueState::encode`] gives: fixed width, legible in a hex dump, and
/// no record type of its own. Zero is never stored — the row goes with
/// the last reference — so reading zero and holding no row are the same
/// answer, which is what [`Rejection::BodyNotHeld`] is raised on.
fn read_references(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    id: &MessageId,
) -> Result<u64, StorageError> {
    let Some(held) = table.get(&id.0[..]).map_err(StorageError::from)? else {
        return Ok(0);
    };
    let (references, rest) = split_u64(held.value())?;
    if !rest.is_empty() {
        return Err(StorageError::Corrupt(FormatError::Trailing {
            extra: rest.len(),
        }));
    }
    Ok(references)
}

/// Take a big-endian `u64` off the front.
fn split_u64(bytes: &[u8]) -> Result<(u64, &[u8]), StorageError> {
    let (head, rest) =
        bytes
            .split_at_checked(8)
            .ok_or(StorageError::Corrupt(FormatError::Undecodable(
                postcard::Error::DeserializeUnexpectedEnd,
            )))?;
    let head: [u8; 8] = head.try_into().expect("split_at_checked gave eight bytes");
    Ok((u64::from_be_bytes(head), rest))
}

/// A message identifier from the sixteen bytes it is stored as.
fn message_id(bytes: &[u8]) -> Result<MessageId, StorageError> {
    bytes.try_into().map(MessageId).map_err(|_| {
        StorageError::Corrupt(FormatError::Trailing {
            extra: bytes.len().saturating_sub(16),
        })
    })
}

/// The packet identifier a key ends with.
fn trailing_u16(key: &[u8]) -> Result<u16, StorageError> {
    let tail = key.get(key.len().saturating_sub(2)..).unwrap_or_default();
    tail.try_into().map(u16::from_be_bytes).map_err(|_| {
        StorageError::Corrupt(FormatError::Undecodable(
            postcard::Error::DeserializeUnexpectedEnd,
        ))
    })
}

// ---- keys ---------------------------------------------------------------

/// The byte that ends a component of a session key, and is therefore
/// escaped inside one.
const TERMINATOR: u8 = 0x00;
/// What an escaped [`TERMINATOR`] is followed by.
///
/// `0xff` and not, say, `0x01`, so that the two-byte terminator `00 00`
/// sorts before any escaped byte and therefore before any continuation:
/// that is what makes a shorter component sort before a longer one that
/// begins with it.
const ESCAPE: u8 = 0xff;

/// Append one component of a session key, escaped and terminated.
///
/// The result is prefix-free, so no session's key can be a prefix of
/// another's, and order-preserving, so the keys sort in the order the
/// components do. Both matter: see the module documentation.
fn push_component(into: &mut Vec<u8>, component: &str) {
    for byte in component.as_bytes() {
        into.push(*byte);
        if *byte == TERMINATOR {
            into.push(ESCAPE);
        }
    }
    into.push(TERMINATOR);
    into.push(TERMINATOR);
}

/// The key prefix every per-session table uses.
fn session_prefix(session: &SessionKey) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(session.mountpoint.len() + session.client_id.len() + 4);
    push_component(&mut prefix, &session.mountpoint);
    push_component(&mut prefix, &session.client_id);
    prefix
}

/// The key prefix every per-rule table uses.
///
/// Escaped and terminated exactly as a session key's components are,
/// although a rule identifier is one component rather than two and this
/// crate's own [`crate::record::RuleRecord`] validator keeps separators
/// out of it. Written this way anyway: the store is handed bytes and
/// does not get to assume what validated them, and a prefix that is only
/// prefix-free by somebody else's rule is prefix-free by luck.
fn rule_prefix(id: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(id.len() + 2);
    push_component(&mut prefix, id);
    prefix
}

/// A session's prefix followed by something that identifies a row within
/// it.
fn joined(prefix: &[u8], tail: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + tail.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(tail);
    key
}

/// A queue key: the session, then the sequence **big-endian**.
///
/// Big-endian because redb orders by bytes and this is the only place that
/// decides whether a queue comes back in arrival order. Little-endian
/// sorts 256 before 2.
fn queue_key(prefix: &[u8], sequence: u64) -> Vec<u8> {
    joined(prefix, &sequence.to_be_bytes())
}

/// The first key after every key beginning with `prefix`, or `None` when
/// the prefix runs to the end of the table.
///
/// The prefix with its last byte that is not `0xff` raised by one. A
/// session prefix always ends in two zero bytes, so `None` cannot arise
/// here; it is handled anyway rather than left as a trap for a later key
/// that does not.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last == u8::MAX {
            end.pop();
        } else {
            *last += 1;
            return Some(end);
        }
    }
    None
}

/// The half-open range of keys from `from` up to `end`, which is what
/// [`prefix_end`] returned for the prefix in hand.
fn prefix_bounds<'a>(
    from: &'a [u8],
    end: Option<&'a [u8]>,
) -> (std::ops::Bound<&'a [u8]>, std::ops::Bound<&'a [u8]>) {
    use std::ops::Bound::{Excluded, Included, Unbounded};
    (Included(from), end.map_or(Unbounded, Excluded))
}

/// Every row of a table whose key begins with `prefix`, in key order.
fn range<'a, V: ::redb::Value + 'static>(
    table: &'a impl ReadableTable<&'static [u8], V>,
    prefix: &'a [u8],
) -> Result<::redb::Range<'a, &'static [u8], V>, StorageError> {
    range_from(table, prefix, prefix)
}

/// The same, starting at `from` rather than at the prefix itself.
fn range_from<'a, V: ::redb::Value + 'static>(
    table: &'a impl ReadableTable<&'static [u8], V>,
    prefix: &'a [u8],
    from: &'a [u8],
) -> Result<::redb::Range<'a, &'static [u8], V>, StorageError> {
    let end = prefix_end(prefix);
    table
        .range::<&[u8]>(prefix_bounds(from, end.as_deref()))
        .map_err(StorageError::from)
}

/// Delete every row of a table whose key begins with `prefix`.
fn remove_prefixed(table: &mut ByteTable<'_>, prefix: &[u8]) -> Result<(), StorageError> {
    let end = prefix_end(prefix);
    table
        .retain_in::<&[u8], _>(prefix_bounds(prefix, end.as_deref()), |_, _| false)
        .map_err(StorageError::from)?;
    Ok(())
}

/// Open a table for reading.
fn open_read<V: ::redb::Value + 'static>(
    transaction: &::redb::ReadTransaction,
    definition: TableDefinition<'_, &'static [u8], V>,
) -> Result<ReadOnlyTable<&'static [u8], V>, StorageError> {
    transaction
        .open_table(definition)
        .map_err(StorageError::from)
}

// ---- what redb's failures mean here -------------------------------------

impl From<::redb::StorageError> for StorageError {
    fn from(error: ::redb::StorageError) -> Self {
        match error {
            ::redb::StorageError::Io(io) => Self::Io(io),
            // The engine found its own structures damaged, which is a
            // different thing from a record that did not decode and the
            // same thing for an operator: reach for a backup.
            ::redb::StorageError::Corrupted(message) => {
                Self::Corrupt(FormatError::Damaged(message))
            }
            other => Self::Engine(other.to_string()),
        }
    }
}

impl From<::redb::TableError> for StorageError {
    fn from(error: ::redb::TableError) -> Self {
        match error {
            ::redb::TableError::Storage(storage) => Self::from(storage),
            other => Self::Engine(other.to_string()),
        }
    }
}

impl From<::redb::TransactionError> for StorageError {
    fn from(error: ::redb::TransactionError) -> Self {
        match error {
            ::redb::TransactionError::Storage(storage) => Self::from(storage),
            other => Self::Engine(other.to_string()),
        }
    }
}

impl From<::redb::CommitError> for StorageError {
    fn from(error: ::redb::CommitError) -> Self {
        match error {
            ::redb::CommitError::Storage(storage) => Self::from(storage),
            other => Self::Engine(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A session key's encoding is prefix-free.
    ///
    /// The property the queues depend on, asserted directly rather than
    /// only through the queue: `a`/`b` and `a\0`/`b` are the pair a naive
    /// concatenation gets wrong, and no realistic client identifier would
    /// produce them, so no test that goes through the store would reach
    /// this by accident.
    #[test]
    fn no_session_key_encodes_to_a_prefix_of_another() {
        let keys = [
            SessionKey::new("", ""),
            SessionKey::new("", "a"),
            SessionKey::new("", "ab"),
            SessionKey::new("a", ""),
            SessionKey::new("a", "b"),
            SessionKey::new("ab", ""),
            SessionKey::new("a\u{0}", "b"),
            SessionKey::new("a", "\u{0}b"),
            SessionKey::new("\u{0}", "\u{0}"),
        ];
        for left in &keys {
            for right in &keys {
                if left == right {
                    continue;
                }
                let (left, right) = (session_prefix(left), session_prefix(right));
                assert!(
                    !left.starts_with(&right) && !right.starts_with(&left),
                    "{left:?} and {right:?} share a prefix"
                );
            }
        }
    }

    /// And it sorts the way the keys themselves do.
    ///
    /// Not required for correctness: it is what makes `restore` hand back
    /// sessions in the same order as the in-memory backend, so the two can
    /// be compared as values. A length-prefixed encoding would be
    /// prefix-free and would fail this.
    #[test]
    fn session_keys_sort_in_the_order_of_their_keys() {
        let mut keys = [
            SessionKey::new("b", "a"),
            SessionKey::new("", "z"),
            SessionKey::new("ab", "a"),
            SessionKey::new("a", "b"),
            SessionKey::new("a", "ab"),
            SessionKey::new("a", "a"),
        ];
        keys.sort();
        let mut encoded: Vec<Vec<u8>> = keys.iter().map(session_prefix).collect();
        let expected = encoded.clone();
        encoded.sort();
        assert_eq!(encoded, expected, "the encoding reordered the sessions");
    }

    /// The queue key orders by the sequence numerically.
    ///
    /// The single mistake in this backend that nothing else would catch,
    /// asserted on the bytes themselves so that it fails here first and
    /// not only in a restore three layers away.
    #[test]
    fn a_queue_key_sorts_by_sequence_numerically() {
        let prefix = session_prefix(&SessionKey::new("", "sensor"));
        let sequences = [0u64, 1, 2, 255, 256, 257, 65_535, 65_536, u64::MAX];
        let mut keys: Vec<Vec<u8>> = sequences
            .iter()
            .map(|sequence| queue_key(&prefix, *sequence))
            .collect();
        let expected = keys.clone();
        keys.sort();
        assert_eq!(
            keys, expected,
            "the queue key does not sort in sequence order: it is not big-endian"
        );
    }

    /// The counters round trip, and a short row is refused rather than
    /// read as a smaller number.
    #[test]
    fn queue_state_round_trips_and_refuses_a_short_row() {
        let state = QueueState {
            depth: 3,
            highest_sequence: 1_000_000,
        };
        assert_eq!(
            QueueState::decode(&state.encode()).expect("it must decode"),
            state
        );
        assert!(QueueState::decode(&[0u8; 15]).is_err());
        assert!(QueueState::decode(&[0u8; 17]).is_err());
    }

    /// The prefix range stops before the next prefix along.
    #[test]
    fn a_prefix_range_ends_at_the_prefix_after_it() {
        assert_eq!(prefix_end(&[1, 2, 3]), Some(vec![1, 2, 4]));
        assert_eq!(prefix_end(&[1, 0xff, 0xff]), Some(vec![2]));
        assert_eq!(prefix_end(&[0xff, 0xff]), None);
    }
}
