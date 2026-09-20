//! The broker behind its lock, where to send what it produces, and
//! where to write what it says.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use broker_core::broker::{Addressed, Broker, BrokerConfig};
use broker_core::session::Action;
use broker_core::subscriptions::SubscriberId;
use foldhash::fast::RandomState;
use mqtt_codec::Packet;
use parking_lot::{Mutex, RwLock};
use storage::{NotDurable, Record, Writer};
use tokio::sync::mpsc;

/// Whether a batch of actions carries a promise that must be kept
/// before it is delivered.
///
/// **An acknowledgement of an at-least-once or exactly-once
/// publication, and nothing else.** Section 7.3: such an
/// acknowledgement tells a publisher the broker has taken
/// responsibility for its message, so sending one before the message
/// is durable means a restart can lose a message the publisher was
/// told was safe. Everything else promised nobody anything, and
/// section 7.3 says so in as many words for at-most-once.
///
/// The three packets are the three ways this broker says "yours is
/// mine now": PUBACK ends an at-least-once exchange, PUBREC accepts an
/// exactly-once one, and PUBCOMP ends it. A PUBREL travelling the same
/// direction is not one of them — it belongs to a delivery this broker
/// is making, not to a publication it is taking on.
///
/// # Why the reason code is not read
///
/// A refusal carries no promise, so narrowing this to the successful
/// codes would save a flush on a path that is already an error. It is
/// deliberately not done. Which reason codes mean "accepted" is the
/// broker's rule, and section 7.3 rejects the shell holding a second
/// copy of the broker's rules precisely because the two drift the
/// first time one of them is corrected. The cost of being wrong in
/// this direction is a flush nobody needed; the cost of being wrong in
/// the other is a lost message.
pub fn promises_durability(actions: &[Addressed]) -> bool {
    actions.iter().any(|(_, action)| {
        matches!(
            action,
            Action::Send(Packet::PubAck(_) | Packet::PubRec(_) | Packet::PubComp(_))
        )
    })
}

/// The broker, and where to send what it produces.
///
/// The broker's methods take an exclusive reference, so every packet in
/// the process passes through one lock. At the connection counts this
/// design targets that is the bottleneck, and it undoes the sharding the
/// subscription trie does internally. It is measured rather than guessed
/// at: see the benchmark in this crate. What matters until then is that
/// the lock is held for the shortest possible span and **never across an
/// await**, so the number measures the design rather than an accident.
///
/// # The journal leaves under the lock that filled it
///
/// Section 7.3: the broker says what changed into a buffer and the
/// shell writes it. Every path into the broker here goes through
/// [`Shared::with_broker`] or [`Shared::with_broker_journalled`], and
/// both drain that buffer before releasing the lock, so a record and
/// the actions it belongs with are always taken together and in order.
/// A path that took the lock and left the buffer alone would grow it
/// without bound and lose everything in it on a restart.
///
/// # The wait is not under the lock
///
/// [`Shared::commit_durable`] is an `async fn` and takes `&self`
/// rather than the guard, because the wait is a disk flush: measured
/// at 334 a second for one waiter alone and 25,100 for a hundred
/// waiting together. Holding the broker lock across it would serialise
/// every connection in the process behind one `fsync` and turn the
/// second number into the first.
pub struct Shared {
    broker: Mutex<Broker>,
    /// Where the journal goes, and `None` for a broker with no store.
    ///
    /// Running without one is a supported mode — it is what milestone
    /// one did, and what every test that does not care about
    /// durability still does — and it means exactly that nothing is
    /// written down and nothing is waited for. It does **not** mean
    /// the journal is left to grow: it is still drained, and then
    /// dropped.
    writer: Option<Arc<Writer>>,
    outboxes: RwLock<HashMap<SubscriberId, mpsc::Sender<Action>, RandomState>>,
    dropped_for_slow_reader: AtomicU64,
    /// The rejected-batch count as of the last line written about it,
    /// so that a standing count is reported once rather than every
    /// second. See [`Shared::report_rejections`].
    rejections_reported: AtomicU64,
}

impl Shared {
    /// A server with no connections and no store.
    pub fn new(config: BrokerConfig) -> Self {
        Self::from_parts(Broker::new(config), None)
    }

    /// A server whose broker is already built, writing to `writer`.
    ///
    /// The broker arrives whole rather than being built here, and that
    /// is section 7.5 rather than a convenience: restore is the first
    /// startup phase and completes before any listener binds. A
    /// listener cannot bind before this exists, and this cannot exist
    /// before the broker does, so a half-restored broker is not a
    /// state anything can be pointed at.
    pub fn with_store(broker: Broker, writer: Arc<Writer>) -> Self {
        Self::from_parts(broker, Some(writer))
    }

    fn from_parts(broker: Broker, writer: Option<Arc<Writer>>) -> Self {
        Self {
            broker: Mutex::new(broker),
            writer,
            outboxes: RwLock::new(HashMap::with_hasher(RandomState::default())),
            dropped_for_slow_reader: AtomicU64::new(0),
            rejections_reported: AtomicU64::new(0),
        }
    }

    /// Do something with the broker, holding the lock for exactly that,
    /// and commit what it said with nobody waiting on it.
    ///
    /// The closure must not await and must not block. Anything slow
    /// belongs outside it.
    ///
    /// **For everything that promises nothing**, which is every path
    /// into the broker except the one that answers a client's
    /// publication: connects, disconnects, the tick's expiries and
    /// wills, and a delivery discarded for size. Section 7.3: those are
    /// journalled where they touch durable state and are never waited
    /// on. A caller that might be about to acknowledge a publication
    /// wants [`Shared::with_broker_journalled`] instead, and
    /// [`promises_durability`] is how it tells.
    pub fn with_broker<T>(&self, f: impl FnOnce(&mut Broker) -> T) -> T {
        let (value, records) = self.with_broker_journalled(f);
        self.commit(records);
        value
    }

    /// The same, handing the journal back rather than committing it.
    ///
    /// For the one caller that has to look at what came out before it
    /// decides whether to wait. The records are still drained under the
    /// lock that filled them; what is left to the caller is only when
    /// they are written.
    ///
    /// This takes what the broker has handed over and does not go
    /// looking for records still sitting in a session's own buffer.
    /// That a broker call leaves none behind is `broker-core`'s
    /// property and is guarded there, by `Broker::sessions_holding_records`
    /// in its own tests; asserting it here would walk every session on
    /// the process's one lock, on every packet.
    pub fn with_broker_journalled<T>(&self, f: impl FnOnce(&mut Broker) -> T) -> (T, Vec<Record>) {
        let mut broker = self.broker.lock();
        let value = f(&mut broker);
        let mut records = Vec::new();
        broker.take_journal(&mut records);
        (value, records)
    }

    /// Write `records` down, with nobody waiting on them.
    ///
    /// They are durable by the next flush, whichever trigger causes it,
    /// and by the writer's shutdown at the latest.
    pub fn commit(&self, records: Vec<Record>) {
        if records.is_empty() {
            return;
        }
        let Some(writer) = &self.writer else {
            return;
        };
        if writer.apply(records).is_err() {
            // The thread is gone, so nothing sent from here on is
            // written and nothing will say so again. Loud, once per
            // batch: a broker still serving clients while writing
            // nothing down is the failure this whole milestone exists
            // to prevent.
            tracing::error!(
                "the storage writer has stopped; nothing the broker says is being written down"
            );
        }
    }

    /// Write `records` down and do not return until they are durable.
    ///
    /// **Never called with the broker lock held**, and it cannot be:
    /// the guard is not `Send`, so a caller that tried would not
    /// compile. That is a backstop rather than the rule. The rule is
    /// the measurement: one waiter alone gets 334 durable writes a
    /// second and a hundred waiting together get 25,100, so a lock
    /// held across this wait would serialise the whole broker behind
    /// one disk flush and cost seventy-five times the throughput it
    /// was protecting.
    ///
    /// Immediate when there is no store, because then nothing is
    /// promised to any device.
    ///
    /// # Errors
    ///
    /// [`NotDurable`] when the store refused the batch or the flush
    /// failed, and when the writer has stopped. The caller must not
    /// acknowledge anything.
    pub async fn commit_durable(&self, records: Vec<Record>) -> Result<(), NotDurable> {
        let Some(writer) = &self.writer else {
            return Ok(());
        };
        let durable = writer
            .apply_durable(records)
            .map_err(|_| NotDurable::WriterStopped)?;
        durable.await
    }

    /// Claim the outbox for a session, returning the receiving half.
    ///
    /// A handle names a session rather than a connection, so a takeover
    /// registers a second outbox under the same handle. The newcomer
    /// replaces the incumbent, whose receiver then sees the channel
    /// closed, which is how its task learns it has been displaced.
    pub fn register_outbox(
        &self,
        subscriber: SubscriberId,
        depth: usize,
    ) -> mpsc::Receiver<Action> {
        let (sender, receiver) = mpsc::channel(depth);
        self.outboxes.write().insert(subscriber, sender);
        receiver
    }

    /// Forget a session's outbox.
    pub fn remove_outbox(&self, subscriber: SubscriberId) {
        self.outboxes.write().remove(&subscriber);
    }

    /// Send each action to the session it is addressed to.
    ///
    /// Never blocks. Waiting here would hold the read half of the
    /// outboxes lock, which every connect and every teardown needs the
    /// write half of, and would block the caller: another connection
    /// with its own client to serve, or the one ticker task that drives
    /// every deadline in the process.
    ///
    /// **A full outbox should now be impossible.** Its depth is derived
    /// from the configured queue and window bounds, and a configuration
    /// that breaks the relation is refused at startup, so no drain can
    /// produce more actions at once than the channel holds. The branch
    /// is kept because the alternative to an unreachable branch is an
    /// unchecked assumption, and it shouts rather than counting quietly:
    /// a message discarded here carries no reason to the session that
    /// lost it, and if it was an acknowledged delivery its window slot
    /// is held for a receipt that can never come.
    pub fn deliver(&self, actions: Vec<Addressed>) {
        if actions.is_empty() {
            return;
        }
        let outboxes = self.outboxes.read();
        for (subscriber, action) in actions {
            let Some(sender) = outboxes.get(&subscriber) else {
                // The session went away between the broker producing this
                // and us delivering it. Ordinary, not an error.
                continue;
            };
            match sender.try_send(action) {
                Ok(()) => {}
                // The writer has stopped and its receiver is gone. That
                // is the same ordinary teardown as a missing outbox,
                // reached a moment later, and counting it as a slow
                // reader would make the counter report routine
                // disconnections as a capacity problem.
                Err(mpsc::error::TrySendError::Closed(_)) => {}
                Err(mpsc::error::TrySendError::Full(action)) => {
                    self.dropped_for_slow_reader.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        subscriber = subscriber.0,
                        kind = ?std::mem::discriminant(&action),
                        "an outbox was full, which its sizing should make \
                         impossible; an action was discarded with no reason \
                         delivered to the session"
                    );
                }
            }
        }
    }

    /// How many sessions have an outbox.
    pub fn outbox_count(&self) -> usize {
        self.outboxes.read().len()
    }

    /// How many actions were discarded because a reader could not keep up.
    pub fn dropped_for_slow_reader(&self) -> u64 {
        self.dropped_for_slow_reader.load(Ordering::Relaxed)
    }

    /// How many batches the store refused with nobody waiting to be
    /// told, and zero when there is no store.
    pub fn rejected_batches(&self) -> u64 {
        self.writer
            .as_ref()
            .map_or(0, |writer| writer.rejected_batches())
    }

    /// Say out loud that the store refused something, if it has since
    /// the last time this was asked.
    ///
    /// `Writer::rejected_batches` counts batches refused with no
    /// waiter attached and no flush to carry the failure, which is the
    /// one case the storage crate has nowhere to report: it has no
    /// logging dependency and one call site was not a reason to add
    /// one. This is that call site, and the ticker is where it is
    /// called from, because that is the one task in the process that
    /// runs whether or not anybody is connected.
    ///
    /// **An error rather than a warning.** A rejection is not a disk
    /// that is full or slow; it is the store saying the record
    /// contradicts what it already holds — a session that is not
    /// there, a sequence number used twice, a body released more often
    /// than it was stored. The broker and the store disagree about
    /// what exists, which is a defect somewhere else made visible
    /// here, and the batch carrying it was applied not at all. From
    /// then on the store is missing something the broker believes it
    /// has.
    ///
    /// Reported on the edge rather than every second, so that a
    /// standing count does not bury the line that says it moved.
    ///
    /// Returns how many refusals this call reported, which is zero
    /// when nothing has changed. The caller has no use for it; a test
    /// does, because a method whose only effect is a log line cannot
    /// be told from one that does nothing.
    pub fn report_rejections(&self) -> u64 {
        let rejected = self.rejected_batches();
        let reported = self.rejections_reported.swap(rejected, Ordering::Relaxed);
        let new = rejected.saturating_sub(reported);
        if new > 0 {
            tracing::error!(
                rejected,
                new,
                "the store refused what the broker said; the two now disagree about what exists, \
                 and the refused records are not written anywhere"
            );
        }
        new
    }
}

/// A store whose flush can be held open or made to fail.
///
/// The ordering section 7.3 asks for is only visible while a flush is
/// in progress: a test that lets the store answer immediately sees the
/// acknowledgement arrive either way, and so passes against a broker
/// that sends it first. Holding the flush open is what turns "the
/// acknowledgement waits" into something that can be asserted rather
/// than assumed.
///
/// Blocking in [`Storage::flush`] is safe because the writer owns a
/// dedicated operating system thread and calls it from there. Nothing
/// on the runtime is parked by it, which is the same reason the real
/// writer can afford a synchronous `fsync`.
#[cfg(test)]
pub(crate) mod gated {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use parking_lot::{Condvar, Mutex};
    use storage::record::{QueueEntry, Record, SessionKey};
    use storage::{MemoryStorage, Restored, Storage, StorageError};

    /// What the gate is currently doing to a flush.
    #[derive(Debug, Default)]
    struct Gate {
        /// Flushes block until this is cleared.
        held: bool,
        /// Flushes fail rather than succeed.
        failing: bool,
    }

    /// An in-memory store with a gate in front of its flush.
    #[derive(Debug, Default)]
    pub(crate) struct GatedStore {
        inner: MemoryStorage,
        gate: Mutex<Gate>,
        opened: Condvar,
        flushes: AtomicU64,
    }

    impl GatedStore {
        /// How long a held flush waits before giving up and going
        /// through anyway.
        ///
        /// Far longer than any assertion window here, so it never
        /// decides a test's answer, and finite so that a failing
        /// assertion ends in a report rather than in a hang.
        const LONGEST_HOLD: Duration = Duration::from_secs(20);

        /// A store that answers at once.
        pub(crate) fn new() -> Self {
            Self::default()
        }

        /// Hold every flush from now on, until [`GatedStore::release`].
        pub(crate) fn hold(&self) {
            self.gate.lock().held = true;
        }

        /// Let held flushes through, and the ones after them.
        pub(crate) fn release(&self) {
            self.gate.lock().held = false;
            self.opened.notify_all();
        }

        /// Fail every flush from now on.
        pub(crate) fn fail(&self) {
            self.gate.lock().failing = true;
        }

        /// How many flushes have been answered, held ones included
        /// once they were let through.
        pub(crate) fn flushes(&self) -> u64 {
            self.flushes.load(Ordering::Relaxed)
        }
    }

    impl Storage for GatedStore {
        fn apply(&self, records: &[Record]) -> Result<(), StorageError> {
            self.inner.apply(records)
        }

        fn flush(&self) -> Result<(), StorageError> {
            let mut gate = self.gate.lock();
            let deadline = std::time::Instant::now() + Self::LONGEST_HOLD;
            while gate.held {
                if self.opened.wait_until(&mut gate, deadline).timed_out() {
                    // Let go rather than wait forever. Dropping a
                    // `Writer` joins its thread, and the thread's last
                    // act is a flush, so a test whose assertion fails
                    // while the gate is shut would otherwise deadlock
                    // in its own unwind and report nothing at all.
                    break;
                }
            }
            let failing = gate.failing;
            drop(gate);
            self.flushes.fetch_add(1, Ordering::Relaxed);
            if failing {
                return Err(StorageError::Io(std::io::Error::other(
                    "the device stopped answering",
                )));
            }
            self.inner.flush()
        }

        fn restore(&self) -> Result<Restored, StorageError> {
            self.inner.restore()
        }

        fn queue_head(
            &self,
            session: &SessionKey,
            cursor: u64,
        ) -> Result<Option<QueueEntry>, StorageError> {
            self.inner.queue_head(session, cursor)
        }

        fn queue_depth(&self, session: &SessionKey) -> Result<u64, StorageError> {
            self.inner.queue_depth(session)
        }

        fn highest_sequence(&self, session: &SessionKey) -> Result<u64, StorageError> {
            self.inner.highest_sequence(session)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_core::subscriptions::SubscriberId;
    use mqtt_codec::ReasonCode;

    #[tokio::test]
    async fn an_outbox_receives_what_is_addressed_to_it() {
        let shared = Shared::new(Default::default());
        let mut inbox = shared.register_outbox(SubscriberId(1), 8);

        shared.deliver(vec![(
            SubscriberId(1),
            broker_core::session::Action::Close(ReasonCode::Success),
        )]);

        let action = inbox.recv().await.expect("delivered");
        assert!(matches!(
            action,
            broker_core::session::Action::Close(ReasonCode::Success)
        ));
    }

    #[tokio::test]
    async fn an_action_for_an_unknown_session_is_discarded_not_panicked() {
        // A session can be torn down between the broker producing an
        // action and the shell delivering it. That is ordinary, not an
        // error.
        let shared = Shared::new(Default::default());
        shared.deliver(vec![(
            SubscriberId(99),
            broker_core::session::Action::Close(ReasonCode::Success),
        )]);
        assert_eq!(shared.outbox_count(), 0);
    }

    #[tokio::test]
    async fn removing_an_outbox_stops_delivery() {
        let shared = Shared::new(Default::default());
        let mut inbox = shared.register_outbox(SubscriberId(1), 8);
        shared.remove_outbox(SubscriberId(1));

        shared.deliver(vec![(
            SubscriberId(1),
            broker_core::session::Action::Close(ReasonCode::Success),
        )]);
        assert!(inbox.try_recv().is_err());
        assert_eq!(shared.outbox_count(), 0);
    }

    #[tokio::test]
    async fn a_full_outbox_does_not_block_the_sender() {
        // Not the broker's mutex: it is released before anything is
        // delivered and this path never takes it. What waiting here
        // would hold is the read half of the outboxes lock, which
        // every connect and every teardown needs the write half of,
        // and the caller itself — another connection with its own
        // client to serve, or the one ticker task that drives every
        // deadline in the process.
        let shared = Shared::new(Default::default());
        let _inbox = shared.register_outbox(SubscriberId(1), 1);

        for _ in 0..100 {
            shared.deliver(vec![(
                SubscriberId(1),
                broker_core::session::Action::Close(ReasonCode::Success),
            )]);
        }
        assert_eq!(shared.dropped_for_slow_reader(), 99);
    }

    #[tokio::test]
    async fn a_closed_outbox_is_not_a_slow_reader() {
        // A writer that has stopped leaves its outbox registered for a
        // moment longer than its receiver. That is the ordinary end of
        // a connection, and counting it here would make the slow-reader
        // figure climb with routine disconnections, which is the one
        // thing it must not do if anyone is to act on it.
        let shared = Shared::new(Default::default());
        let inbox = shared.register_outbox(SubscriberId(1), 8);
        drop(inbox);

        shared.deliver(vec![(
            SubscriberId(1),
            broker_core::session::Action::Close(ReasonCode::Success),
        )]);

        assert_eq!(
            shared.dropped_for_slow_reader(),
            0,
            "a closed outbox is a departure, not a capacity problem"
        );
    }

    #[tokio::test]
    async fn the_broker_is_reachable_under_the_lock() {
        let shared = Shared::new(Default::default());
        assert_eq!(shared.with_broker(|broker| broker.session_count()), 0);
    }

    #[tokio::test]
    async fn a_takeover_replaces_the_outbox_for_the_same_handle() {
        // A handle names a session, not a connection. The newcomer's
        // outbox must replace the incumbent's, or the broker's
        // redeliveries go to a socket that is closing.
        let shared = Shared::new(Default::default());
        let mut first = shared.register_outbox(SubscriberId(1), 8);
        let mut second = shared.register_outbox(SubscriberId(1), 8);

        shared.deliver(vec![(
            SubscriberId(1),
            broker_core::session::Action::Close(ReasonCode::Success),
        )]);
        assert!(second.try_recv().is_ok());
        assert!(first.try_recv().is_err(), "the incumbent is disconnected");
        assert_eq!(shared.outbox_count(), 1);
    }

    // ----------------------------------------------------------------
    // Section 7.3: what the broker says leaves under the lock that
    // filled it, and what promises something waits.
    // ----------------------------------------------------------------

    #[tokio::test]
    async fn the_journal_does_not_pile_up_in_a_broker_with_no_store() {
        // Before this task the shell never drained the journal, so
        // `Broker`'s buffer grew for the life of the process: one
        // record per durable change, never handed to anybody, never
        // freed. Harmless on a branch with no store attached and a
        // leak the moment one is.
        //
        // Read by draining again: whatever `with_broker` left behind
        // comes out here, and nothing is what it must be.
        let shared = Shared::new(Default::default());
        for id in 0..8 {
            shared.with_broker(|broker| {
                let mut out = Vec::new();
                broker.connect(
                    "",
                    &mqtt_codec::packet::Connect {
                        version: mqtt_codec::Version::V5,
                        client_id: format!("c{id}"),
                        clean_start: false,
                        keep_alive: 0,
                        username: None,
                        password: None,
                        will: None,
                        properties: mqtt_codec::Properties::default(),
                    },
                    &mut out,
                );
            });
        }
        let left_behind = shared.with_broker(|broker| {
            let mut records = Vec::new();
            broker.take_journal(&mut records);
            records.len()
        });
        assert_eq!(
            left_behind, 0,
            "eight sessions were created and their records were left in the broker"
        );
    }

    #[test]
    fn only_an_acknowledgement_of_a_promised_publication_waits() {
        // Convention 2 and convention 5: the mapping is pinned packet
        // by packet rather than sampled, because a shell that waits
        // on everything and a shell that waits on nothing both pass a
        // test that only checks one packet.
        use mqtt_codec::packet::{Ack, Publish, SubAck};
        use mqtt_codec::{Properties, QoS};

        fn ack() -> Ack {
            Ack {
                packet_id: 1,
                reason: ReasonCode::Success,
                properties: Properties::default(),
            }
        }

        // `Action` is not `Clone`, so the description is taken before
        // the action is handed over.
        fn waits(action: Action) -> (bool, String) {
            let shown = format!("{action:?}");
            (promises_durability(&[(SubscriberId(1), action)]), shown)
        }

        // The three ways this broker says "yours is mine now".
        for promise in [
            Action::Send(Packet::PubAck(ack())),
            Action::Send(Packet::PubRec(ack())),
            Action::Send(Packet::PubComp(ack())),
        ] {
            let (waited, shown) = waits(promise);
            assert!(waited, "{shown} is a promise to a publisher");
        }

        // A release travels the same way and is not one: it belongs
        // to a delivery this broker is making, not to a publication
        // it is taking responsibility for. Nor is a publication, a
        // subscription acknowledgement, a ping or a close.
        for quiet in [
            Action::Send(Packet::PubRel(ack())),
            Action::Send(Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: "a/b".into(),
                packet_id: Some(1),
                payload: bytes::Bytes::new(),
                properties: Properties::default(),
            })),
            Action::Send(Packet::SubAck(SubAck {
                packet_id: 1,
                reasons: vec![ReasonCode::Success],
                properties: Properties::default(),
            })),
            Action::Send(Packet::PingResp),
            Action::Close(ReasonCode::Success),
        ] {
            let (waited, shown) = waits(quiet);
            assert!(!waited, "{shown} promises a publisher nothing");
        }

        assert!(!promises_durability(&[]), "nothing promises nothing");
    }

    #[tokio::test]
    async fn a_batch_the_store_refuses_is_reported_once_and_then_stays_quiet() {
        // `Writer::rejected_batches` counts what the store refused
        // with nobody waiting to be told. Before this task it
        // reported to nobody at all. The count is read on the edge,
        // so a standing figure does not bury the line that says it
        // moved.
        let store: Arc<dyn storage::Storage> = Arc::new(storage::MemoryStorage::new());
        let writer = Arc::new(
            Writer::spawn(store, storage::WriterConfig::default()).expect("a writer thread"),
        );
        let shared = Shared::with_store(Broker::new(Default::default()), Arc::clone(&writer));

        assert_eq!(shared.report_rejections(), 0, "nothing has been refused");

        // A queue entry for a session the store does not hold, which
        // `Rejection::NoSuchSession` refuses, committed with nobody
        // waiting: the one case the storage crate has nowhere to
        // report.
        shared.commit(vec![Record::QueuePushed {
            session: storage::record::SessionKey::new("", "never-existed"),
            entry: storage::record::QueueEntry {
                sequence: 1,
                packet_id: None,
                delivery: storage::record::DeliveryRecord {
                    body: storage::record::MessageId([0; 16]),
                    qos: storage::record::Qos::AtLeastOnce,
                    retain: false,
                    subscription_ids: Vec::new(),
                    dup: false,
                },
            },
        }]);

        let reported = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let reported = shared.report_rejections();
                if reported > 0 {
                    return reported;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the refusal is noticed");
        assert_eq!(reported, 1);
        assert_eq!(shared.rejected_batches(), 1);
        assert_eq!(
            shared.report_rejections(),
            0,
            "a count that has not moved is not news"
        );
    }

    #[tokio::test]
    async fn a_shell_with_no_store_waits_for_nothing() {
        // Running without a store is milestone one's mode and is
        // still supported. Nothing is written down, so nothing can be
        // waited for, and a commit that asked to wait returns rather
        // than hanging.
        let shared = Shared::new(Default::default());
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            shared.commit_durable(vec![Record::RetainedRemoved { topic: "a".into() }]),
        )
        .await
        .expect("a store-less commit does not wait")
        .expect("and does not fail");
        assert_eq!(shared.rejected_batches(), 0);
    }
}
