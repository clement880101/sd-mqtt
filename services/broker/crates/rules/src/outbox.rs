//! A rule's durable outbox: what goes in, and what has to come out with
//! it.
//!
//! Section 9.2. An envelope enters when the rule fires and leaves when it
//! has been delivered or dead-lettered, and in between it survives
//! whatever happens to the process. The store holds the envelopes; this
//! module is the vocabulary for saying what happened to one, and it
//! exists mostly to hold two rules that are easy to state and easy to
//! get wrong.
//!
//! # A pop does not release the body
//!
//! [`storage::Storage::apply`] says so of a queue and the same is true
//! here: taking a reference to a payload and releasing it are separate
//! records, and **an envelope leaving its outbox releases nothing.**
//!
//! That is not an oversight in the store. One publication can fire
//! several rules, and each rule's envelope holds its own reference to the
//! one stored body. A store that released on removal would delete the
//! payload out from under every other rule that had not sent it yet. So
//! the release belongs to whoever took the reference, which is
//! [`Outbox::append`], and it is paid by [`Outbox::completed`].
//!
//! Both directions are mistakes and both are cheap to make:
//!
//! - Releasing **too early** — on append, or when an attempt fails —
//!   deletes a payload the outbox still has to send, and the message is
//!   lost.
//! - Releasing **never** leaks the payload for the life of the file, and
//!   nothing reclaims it: no scan rebuilds reference counts, because
//!   section 7.2 persists them rather than deriving them.
//!
//! [`Outbox::completed`] emits both records in **one batch** for the
//! reason `main.rs` gives about expired sessions: a batch is applied in
//! full or not at all, so the envelope cannot leave without its release
//! and the release cannot land without the envelope leaving.
//!
//! # The attempt count is not in this type
//!
//! [`Outbox`] holds one number, the next sequence to assign, and it is
//! read out of the store when the outbox is opened. Everything about how
//! far a delivery has got — how many attempts it has cost, when the next
//! is due — is in the stored [`OutboxRecord`], because a restart that
//! reset it would take a node coming back from an outage and have it
//! throw its entire backlog at a still-dead endpoint at once.

use std::sync::atomic::{AtomicU64, Ordering};

use storage::record::{BodyRecord, MessageId, OutboxRecord, Record};
use storage::{Storage, StorageError};

use crate::rule::RuleId;

/// One rule's outbox, over whatever store the node was given.
///
/// Cheap to hold and cheap to clone nothing: it owns no store and no
/// records. Every method that changes something hands back the records
/// that say so, and the caller decides when they are written, because
/// the caller is sometimes a hook that may not touch a disk.
#[derive(Debug)]
pub struct Outbox {
    /// Whose.
    rule: RuleId,
    /// The next sequence number to assign.
    ///
    /// Seeded from [`Storage::outbox_highest_sequence`] rather than from
    /// zero, so that a restarted node does not try to reuse a number the
    /// store has already spent — which the store refuses, meaning every
    /// envelope after a restart would be dropped on the floor.
    ///
    /// Atomic because [`Outbox::append`] is called from a hook, which
    /// the broker holds by shared reference.
    next_sequence: AtomicU64,
}

impl Outbox {
    /// Open a rule's outbox, continuing its sequence numbers.
    ///
    /// # Errors
    ///
    /// Whatever the store says when asked for the high-water mark.
    pub fn open(rule: RuleId, store: &dyn Storage) -> Result<Self, StorageError> {
        let highest = store.outbox_highest_sequence(rule.as_str())?;
        Ok(Self {
            rule,
            next_sequence: AtomicU64::new(highest + 1),
        })
    }

    /// Whose outbox this is.
    pub fn rule(&self) -> &RuleId {
        &self.rule
    }

    /// The records that put one envelope into this outbox.
    ///
    /// Two of them, in one batch: a [`Record::BodyPut`] that takes a
    /// reference to the payload, and the envelope that names it. The put
    /// is needed even for a message the broker has already stored for
    /// its own queues, because a reference is what keeps the body alive
    /// and this outbox owes one of its own. A put for a body the store
    /// already holds takes the reference without rewriting the payload;
    /// see [`storage::Storage::apply`].
    ///
    /// **No release is emitted here.** See the module documentation.
    ///
    /// `id` names the delivery and is minted once, here. It is not
    /// derived from the attempt, because an identifier that changed per
    /// attempt would look like a new delivery to a receiver trying to
    /// deduplicate the repeat that an at-least-once guarantee promises
    /// it will see.
    pub fn append(
        &self,
        id: MessageId,
        body_id: MessageId,
        body: BodyRecord,
        client_id: &str,
        now: u32,
    ) -> (OutboxRecord, Vec<Record>) {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let entry = OutboxRecord {
            sequence,
            id,
            body: body_id,
            client_id: client_id.to_owned(),
            enqueued_at: now,
            // Never tried, and due immediately.
            attempts: 0,
            next_attempt_at: 0,
        };
        let records = vec![
            Record::BodyPut { id: body_id, body },
            Record::OutboxAppended {
                rule: self.rule.as_str().to_owned(),
                entry: entry.clone(),
            },
        ];
        (entry, records)
    }

    /// The envelope waiting at or after `cursor`, if any.
    ///
    /// # Errors
    ///
    /// Whatever the store says.
    pub fn head(
        &self,
        store: &dyn Storage,
        cursor: u64,
    ) -> Result<Option<OutboxRecord>, StorageError> {
        store.outbox_head(self.rule.as_str(), cursor)
    }

    /// How many envelopes are waiting.
    ///
    /// # Errors
    ///
    /// Whatever the store says.
    pub fn depth(&self, store: &dyn Storage) -> Result<u64, StorageError> {
        store.outbox_depth(self.rule.as_str())
    }

    /// The record that says an attempt was made and failed.
    ///
    /// The count and the deadline are written out rather than
    /// incremented by the store, so that the backoff is this crate's
    /// policy and the store holds a fact. Nothing is released: the
    /// envelope is still owed a delivery.
    pub fn attempted(&self, entry: &OutboxRecord, next_attempt_at: u32) -> Record {
        Record::OutboxAttempted {
            rule: self.rule.as_str().to_owned(),
            sequence: entry.sequence,
            attempts: entry.attempts.saturating_add(1),
            next_attempt_at,
        }
    }

    /// The records that finish an envelope, delivered or dead-lettered.
    ///
    /// **Both of them, in one batch.** The envelope leaves and the
    /// reference its append took is released. Either one without the
    /// other is a bug with a name: without the release the payload leaks
    /// for the life of the file, and a release without the removal
    /// deletes a payload the outbox is still going to send.
    pub fn completed(&self, entry: &OutboxRecord) -> Vec<Record> {
        vec![
            Record::OutboxRemoved {
                rule: self.rule.as_str().to_owned(),
                sequence: entry.sequence,
            },
            Record::BodyReleased { id: entry.body },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use storage::record::{ActionRecord, HttpOutRecord, Qos, RuleRecord, TriggerRecord};
    use storage::MemoryStorage;

    fn rule_record(id: &str) -> RuleRecord {
        RuleRecord {
            id: id.to_owned(),
            name: format!("rule {id}"),
            enabled: true,
            trigger: TriggerRecord {
                filter: "sensor/+/temp".to_owned(),
                min_qos: None,
                client_id: None,
            },
            action: ActionRecord::HttpOut(HttpOutRecord {
                url: "https://example.invalid/hook".to_owned(),
                method: "POST".to_owned(),
                headers: Vec::new(),
                timeout_ms: 1_000,
                max_concurrency: 1,
                retries: 3,
                backoff_ms: 500,
            }),
        }
    }

    /// A store holding the named rules and nothing else.
    fn store_with(rules: &[&str]) -> MemoryStorage {
        let store = MemoryStorage::new();
        let records: Vec<Record> = rules
            .iter()
            .map(|id| Record::RulePut {
                rule: rule_record(id),
            })
            .collect();
        store.apply(&records).expect("a rule must be storable");
        store
    }

    fn body_id(n: u8) -> MessageId {
        let mut bytes = [0u8; 16];
        bytes[0] = n;
        MessageId(bytes)
    }

    fn body(n: u8) -> BodyRecord {
        BodyRecord {
            topic: format!("sensor/{n}/temp"),
            payload: Bytes::from(format!("payload-{n}")),
            qos: Qos::AtLeastOnce,
            retain: false,
            expires_at: None,
            properties: Bytes::new(),
        }
    }

    #[track_caller]
    fn apply(store: &MemoryStorage, records: &[Record]) {
        store
            .apply(records)
            .unwrap_or_else(|error| panic!("the store refused a batch: {error}"));
    }

    fn outbox(store: &MemoryStorage, id: &str) -> Outbox {
        Outbox::open(RuleId::new(id).expect("a valid identifier"), store)
            .expect("an outbox must open")
    }

    /// Whether the store still holds the payload.
    fn holds_body(store: &MemoryStorage, id: MessageId) -> bool {
        store
            .restore()
            .expect("restoring must succeed")
            .bodies
            .contains_key(&id)
    }

    #[test]
    fn an_envelope_is_appended_with_its_payload_and_a_sequence_of_its_own() {
        let store = store_with(&["a"]);
        let outbox = outbox(&store, "a");

        let (first, records) = outbox.append(body_id(9), body_id(1), body(1), "sensor-1", 1_700);
        apply(&store, &records);
        let (second, records) = outbox.append(body_id(8), body_id(2), body(2), "sensor-2", 1_701);
        apply(&store, &records);

        assert_eq!(first.sequence, 1, "the first envelope is sequence one");
        assert_eq!(second.sequence, 2);
        let head = outbox
            .head(&store, 0)
            .expect("the head read")
            .expect("an envelope is waiting");
        assert_eq!(head, first, "the envelope came back changed");
        assert_eq!(head.attempts, 0, "a fresh envelope has not been tried");
        assert_eq!(head.next_attempt_at, 0, "a fresh envelope is due now");
        assert_eq!(head.client_id, "sensor-1");
        assert_eq!(outbox.depth(&store).expect("depth"), 2);
    }

    /// An outbox opened against a store that already holds envelopes
    /// carries on from where the numbers stopped.
    ///
    /// Not cosmetic: the store refuses a sequence it has already
    /// assigned, so an outbox that restarted at one would have every
    /// envelope after a restart refused rather than delivered.
    #[test]
    fn sequences_continue_from_what_the_store_already_spent() {
        let store = store_with(&["a"]);
        {
            let outbox = outbox(&store, "a");
            for n in 1..=3u8 {
                let (_, records) = outbox.append(body_id(9), body_id(n), body(n), "sensor", 1_700);
                apply(&store, &records);
            }
        }

        // A new process, the same store.
        let reopened = outbox(&store, "a");
        let (entry, records) = reopened.append(body_id(9), body_id(4), body(4), "sensor", 1_800);
        assert_eq!(entry.sequence, 4, "a restart reused a spent number");
        apply(&store, &records);
        assert_eq!(reopened.depth(&store).expect("depth"), 4);
    }

    /// The payload is still there while the envelope is.
    ///
    /// The append takes a reference and nothing releases it until the
    /// envelope is finished. A release moved into [`Outbox::append`], or
    /// into [`Outbox::attempted`], deletes a payload that is still owed
    /// a delivery, and the message is lost rather than leaked.
    #[test]
    fn the_payload_is_not_released_while_the_envelope_is_still_owed() {
        let store = store_with(&["a"]);
        let outbox = outbox(&store, "a");
        let (entry, records) = outbox.append(body_id(9), body_id(1), body(1), "sensor", 1_700);
        apply(&store, &records);
        assert!(
            holds_body(&store, body_id(1)),
            "appending the envelope released the payload it has to send"
        );

        apply(&store, &[outbox.attempted(&entry, 1_760)]);
        assert!(
            holds_body(&store, body_id(1)),
            "a failed attempt released the payload the retry needs"
        );
        let head = outbox
            .head(&store, 0)
            .expect("the head read")
            .expect("the envelope is still waiting");
        assert_eq!(head.attempts, 1, "the attempt was not counted");
        assert_eq!(head.next_attempt_at, 1_760);
    }

    /// And it is reclaimed when the envelope leaves, in the same batch.
    ///
    /// The other direction of the same mistake: a payload nothing
    /// releases stays in the file forever, because reference counts are
    /// stored rather than rebuilt and no scan will ever notice.
    #[test]
    fn the_payload_is_reclaimed_when_the_envelope_leaves() {
        let store = store_with(&["a"]);
        let outbox = outbox(&store, "a");
        let (entry, records) = outbox.append(body_id(9), body_id(1), body(1), "sensor", 1_700);
        apply(&store, &records);

        let finishing = outbox.completed(&entry);
        assert_eq!(
            finishing.len(),
            2,
            "finishing an envelope is a removal and a release, together"
        );
        apply(&store, &finishing);

        assert!(
            !holds_body(&store, body_id(1)),
            "the payload was leaked when its envelope was delivered"
        );
        assert_eq!(outbox.depth(&store).expect("depth"), 0);
        assert_eq!(outbox.head(&store, 0).expect("head"), None);
    }

    /// One message firing two rules is two references, and the first
    /// rule to deliver does not take the payload away from the second.
    #[test]
    fn one_payload_survives_until_every_rule_has_sent_it() {
        let store = store_with(&["a", "ab"]);
        let first = outbox(&store, "a");
        let second = outbox(&store, "ab");

        let (one, records) = first.append(body_id(9), body_id(1), body(1), "sensor", 1_700);
        apply(&store, &records);
        let (two, records) = second.append(body_id(8), body_id(1), body(1), "sensor", 1_700);
        apply(&store, &records);

        apply(&store, &first.completed(&one));
        assert!(
            holds_body(&store, body_id(1)),
            "one rule's delivery deleted the payload the other still had to send"
        );

        apply(&store, &second.completed(&two));
        assert!(
            !holds_body(&store, body_id(1)),
            "the payload outlived the last rule that needed it"
        );
    }

    /// Two outboxes do not read each other's heads.
    ///
    /// `"a"` and `"ab"`, which is the pair a key built by concatenation
    /// confuses. The outcome it guards against is a rule posting another
    /// rule's messages to its own endpoint.
    #[test]
    fn two_outboxes_do_not_see_each_other() {
        let store = store_with(&["a", "ab"]);
        let first = outbox(&store, "a");
        let second = outbox(&store, "ab");

        let (_, records) = first.append(body_id(9), body_id(1), body(1), "from-a", 1_700);
        apply(&store, &records);
        let (_, records) = second.append(body_id(8), body_id(2), body(2), "from-ab", 1_700);
        apply(&store, &records);

        assert_eq!(
            first
                .head(&store, 0)
                .expect("head")
                .map(|entry| entry.client_id),
            Some("from-a".to_owned())
        );
        assert_eq!(
            second
                .head(&store, 0)
                .expect("head")
                .map(|entry| entry.client_id),
            Some("from-ab".to_owned())
        );
        assert_eq!(first.depth(&store).expect("depth"), 1);
        assert_eq!(second.depth(&store).expect("depth"), 1);
    }

    /// The head read skips what has been delivered, so a pump that walks
    /// forward never offers the same envelope twice.
    #[test]
    fn the_head_read_skips_what_has_been_delivered() {
        let store = store_with(&["a"]);
        let outbox = outbox(&store, "a");
        let mut entries = Vec::new();
        for n in 1..=3u8 {
            let (entry, records) = outbox.append(body_id(9), body_id(n), body(n), "sensor", 1_700);
            apply(&store, &records);
            entries.push(entry);
        }

        apply(&store, &outbox.completed(&entries[0]));
        assert_eq!(
            outbox
                .head(&store, 0)
                .expect("head")
                .map(|entry| entry.sequence),
            Some(2),
            "the head read offered an envelope that had already gone"
        );

        apply(&store, &outbox.completed(&entries[1]));
        apply(&store, &outbox.completed(&entries[2]));
        assert_eq!(outbox.head(&store, 0).expect("head"), None);
        assert_eq!(outbox.depth(&store).expect("depth"), 0);
    }
}
