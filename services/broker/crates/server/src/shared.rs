//! The broker behind its lock, where to send what it produces, and
//! where to write what it says.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use broker_core::broker::{Addressed, Broker, BrokerConfig};
use broker_core::handover::Handover;
use broker_core::registry::SessionId;
use broker_core::session::Action;
use broker_core::subscriptions::{FilterEvent, SubscriberId};
use cluster::migrate::{Claimed, Migrator};
use cluster::{Gate, MetadataWrite, Operation, Replicated};
use foldhash::fast::RandomState;
use mqtt_codec::Packet;
use parking_lot::{Mutex, RwLock};
use storage::SessionKey;
use storage::{Applied, Durable, NotDurable, Record, Writer};
use tokio::sync::mpsc;

use crate::metrics::{Metrics, RecordDrop, ShellDrop};

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

/// Writes waiting for the store before anybody is told about them.
///
/// Section 8.2's two-phase write, as it crosses from the broker's lock to
/// whatever holds the mesh. [`Self::durable`] is the batch that carries
/// them; [`Self::writes`] is what to broadcast once it has landed and what
/// to abandon if it has not.
#[derive(Debug)]
#[must_use = "a write nobody waits for is a write no peer is ever told about"]
pub struct Broadcastable {
    /// The batch the writes rode in.
    durable: Durable,
    /// What they were, in the order the broker produced them.
    writes: Vec<MetadataWrite>,
}

impl Broadcastable {
    /// Wait for the batch, and say what to do with the writes.
    ///
    /// `Ok` is a batch the store took and a flush covered, so every write
    /// in it may be published and broadcast. `Err` is a batch that was
    /// refused or a flush that failed, so every write in it must be
    /// abandoned: see `cluster::oplog` for why telling a peer about one
    /// anyway is the single divergence no exchange repairs.
    ///
    /// # Errors
    ///
    /// [`NotDurable`], unchanged from the writer, so that whoever logs it
    /// can say which of the two causes it was.
    pub async fn landed(self) -> Result<Vec<MetadataWrite>, (NotDurable, Vec<MetadataWrite>)> {
        match self.durable.await {
            Ok(()) => Ok(self.writes),
            Err(why) => Err((why, self.writes)),
        }
    }
}

/// A journal already on its way to the store, and what is left to wait
/// for.
///
/// Handed out by [`Shared::with_broker_committed`] and awaited outside
/// the lock that produced it. It holds no lock and borrows nothing: by
/// the time a caller has one, the broker's guard has been dropped
/// inside the synchronous call that made it. That is what makes "the
/// wait is not under the lock" a property of the types rather than a
/// rule to remember.
#[derive(Debug)]
#[must_use = "a commit that promised a publisher something must be waited for"]
pub enum Committed {
    /// Nobody is waiting. Either nothing was promised, or there is no
    /// store and so nothing was promised to a device.
    ///
    /// The records are still on their way: durable by the next flush,
    /// whichever trigger causes it, and by the writer's shutdown at the
    /// latest.
    Nothing,
    /// A promise was made and the flush that keeps it is outstanding.
    Waiting(Durable),
    /// A promise was made and the writer was already gone, so it will
    /// never be kept and nobody is left to report on it.
    Stopped,
    /// Nothing was promised, and the writer is behind: the caller waits
    /// for it to catch up before it does anything else.
    ///
    /// **This is back pressure and not a promise.** The records are in
    /// the channel and will be written; what is waited for is the
    /// backlog coming down, which is why it cannot fail and why a caller
    /// that drops it is not breaking anything. See
    /// [`Shared::hand_over`] for what reaching this means and
    /// [`storage::Applied::Backlogged`] for what raises it.
    Paced(Arc<Writer>),
}

impl Committed {
    /// Wait for the promise to be kept, if one was made.
    ///
    /// Returns at once when nothing was promised, which is every path
    /// into the broker except the one that answers a client's
    /// publication — unless the writer is behind, in which case a caller
    /// that promised nothing still waits for it to catch up, and that
    /// wait succeeds.
    ///
    /// # Errors
    ///
    /// [`NotDurable`] when the store refused the batch, when the flush
    /// failed, and when the writer has stopped. The caller must not
    /// acknowledge anything.
    pub async fn wait(self) -> Result<(), NotDurable> {
        match self {
            Self::Nothing => Ok(()),
            Self::Waiting(durable) => durable.await,
            Self::Stopped => Err(NotDurable::WriterStopped),
            // Not an error however it ends. Nothing was promised, so
            // there is nothing to fail: this waited to pace a producer,
            // and the producer has been paced.
            Self::Paced(writer) => {
                writer.relief().await;
                Ok(())
            }
        }
    }
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
/// # The journal reaches the writer under the lock that filled it
///
/// Section 7.3: the broker says what changed into a buffer and the
/// shell writes it, and **a record and the actions it belongs with are
/// always taken together and in order**. Draining the buffer under the
/// lock is only half of that. The other half is the handoff: two tasks
/// that take this lock in order A then B can reach the writer's channel
/// in order B then A if nothing holds a position between the guard
/// dropping and the send, and `Storage::apply` is order-sensitive.
///
/// What that costs is not theoretical. One client reconnecting across
/// two connections is enough: the closing one journals a session record
/// carrying its expiry deadline and is preempted, the new one journals a
/// session record with no deadline and sends first, and the store ends
/// up holding the disconnect record for a session the broker believes is
/// live. Restore then reads a deadline already past and drops the
/// session, its subscriptions and its whole queue. Quieter shapes: a
/// retained removal applied after a retained put silently loses a
/// retained message, and a session removal applied after a session put
/// makes every later record for that session a rejection, which discards
/// the whole atomic batch, records for unrelated sessions included.
///
/// So [`Shared::with_broker_committed`] hands the batch to the writer
/// **while the guard is still held**, and hands back a [`Committed`] to
/// be awaited afterwards. It is the only way into the broker here; there
/// is no longer any way to take records out of a broker section and
/// write them later, because nothing hands them out.
///
/// # The wait is not under the lock
///
/// The handoff is a channel send and a `oneshot::channel()`, both of
/// which return without touching the device. The **wait** is the disk
/// flush: measured at 334 durable writes a second for one waiter alone
/// and 25,100 for a hundred waiting together. Holding the broker lock
/// across it would serialise every connection in the process behind one
/// `fsync` and turn the second number into the first.
///
/// That is a property of the types rather than a rule to remember.
/// [`Shared::with_broker_committed`] is a synchronous `fn`: the guard is
/// one of its locals, is never returned, never stored and never handed
/// to the caller, and there is no `.await` in it to hold it across. The
/// only awaitable thing a caller ever holds is [`Committed`], which by
/// then is a `oneshot` receiver and nothing else — it owns no lock, and
/// the guard that produced it was dropped before it existed.
///
/// The old backstop still stands behind that: [`parking_lot`]'s guard is
/// not [`Send`], so an `async fn` that held one across an await would
/// not compile into anything [`tokio::spawn`] accepts.
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
    /// What stamps a replicated write, and `None` on a node that has no
    /// store and therefore no identity and no clock.
    ///
    /// Section 8.2: the broker says which filters gained or lost a
    /// subscriber on this node, and this turns the ones that left or
    /// reached zero into registers. It is held here because **the
    /// register has to go in the same batch as the subscription that
    /// caused it** — one batch is all or nothing, so a node cannot end
    /// up with a subscription the cluster was never told about, or a
    /// register for a subscription it did not keep.
    ///
    /// `None` is milestone one's mode, which is still supported: a node
    /// with no store cannot cluster, so there is nothing to stamp and
    /// the events are drained and dropped. Drained, not ignored, for the
    /// reason [`Self::writer`] gives about the journal.
    replicated: Option<Arc<Replicated>>,
    /// Where writes go to wait for the store before they are broadcast.
    ///
    /// Section 8.2, and the order is the whole of it: **a peer told about
    /// a register whose batch the store refused holds a value this node
    /// will never hold again**, and no exchange repairs it, because an
    /// exchange serves the origin's own log and the origin's log lost the
    /// entry in the same batch. So a write is stamped and numbered here
    /// and shown to nobody until the batch carrying it is durable; what
    /// does the showing is a task of the binary's, draining
    /// [`Shared::waiting_to_broadcast`].
    ///
    /// Unbounded, and bounded in practice by what the writer will take:
    /// one entry per batch that carried a replicated write, held only
    /// until that batch's flush. A bound here would mean choosing between
    /// blocking the broker's lock and dropping a broadcast, and neither is
    /// a choice this has to make.
    broadcasts: Option<mpsc::UnboundedSender<Broadcastable>>,
    /// The other end of it, handed out once.
    waiting: Mutex<Option<mpsc::UnboundedReceiver<Broadcastable>>>,
    outboxes: RwLock<HashMap<SubscriberId, mpsc::Sender<Action>, RandomState>>,
    /// How often a caller was told the writer is behind. See
    /// [`Shared::report_pacing`].
    paced: AtomicU64,
    /// The paced count as of the last line written about it, for the
    /// same reason [`Shared::rejections_reported`] exists.
    pacing_reported: AtomicU64,
    /// The rejected-batch count as of the last line written about it,
    /// so that a standing count is reported once rather than every
    /// second. See [`Shared::report_rejections`].
    rejections_reported: AtomicU64,
    /// What this process counts, and what the metrics endpoint
    /// renders.
    ///
    /// It lives here because this is the one thing every connection,
    /// the ticker and the listener all already hold. The handles
    /// inside it are owned and lock-free; see [`crate::metrics`] for
    /// why that is not an optimisation to consider.
    metrics: Arc<Metrics>,
    /// What claims a session from another node, section 8.4.
    ///
    /// Installed after construction rather than passed to it, like the
    /// broker's forwarder and for the same reason: it holds the mesh,
    /// the mesh holds a handler, and the handler holds this. One of the
    /// three has to be joined up afterwards.
    ///
    /// `None` is a node that does not cluster, and a connect then costs
    /// one `OnceLock` read.
    migrator: OnceLock<Arc<Migrator>>,
    /// What a partition does to a connect, a publish and a subscribe,
    /// section 8.5.
    ///
    /// Installed after construction for the migrator's reason: it
    /// counts the mesh's members and the mesh is built later.
    ///
    /// **`None` is a node that serves everything**, which is both a
    /// node that does not cluster and a clustering node whose operator
    /// set no flags — the default, and section 8.5's availability. So
    /// the absent case and the permissive case are the same case, and
    /// the cost of the policy on a node that did not ask for it is one
    /// `OnceLock` read per operation.
    gate: OnceLock<Arc<Gate>>,
}

impl Shared {
    /// A server with no connections and no store.
    pub fn new(config: BrokerConfig) -> Self {
        Self::from_parts(Broker::new(config), None, None)
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
        Self::from_parts(broker, Some(writer), None)
    }

    /// The same, with section 8.2's replicated state beside it.
    ///
    /// What the binary builds. The `Replicated` comes out of the same
    /// restore as the broker, because the clock inside it resumes from
    /// the highest reading this node has written and the registers are
    /// the ones it held; see `Replicated::open`.
    pub fn with_replication(
        broker: Broker,
        writer: Arc<Writer>,
        replicated: Arc<Replicated>,
    ) -> Self {
        Self::from_parts(broker, Some(writer), Some(replicated))
    }

    fn from_parts(
        broker: Broker,
        writer: Option<Arc<Writer>>,
        replicated: Option<Arc<Replicated>>,
    ) -> Self {
        let (broadcasts, waiting) = match replicated {
            Some(_) => {
                let (sender, receiver) = mpsc::unbounded_channel();
                (Some(sender), Some(receiver))
            }
            None => (None, None),
        };
        Self {
            broker: Mutex::new(broker),
            writer,
            replicated,
            broadcasts,
            waiting: Mutex::new(waiting),
            outboxes: RwLock::new(HashMap::with_hasher(RandomState::default())),
            paced: AtomicU64::new(0),
            pacing_reported: AtomicU64::new(0),
            rejections_reported: AtomicU64::new(0),
            metrics: Arc::new(Metrics::new()),
            migrator: OnceLock::new(),
            gate: OnceLock::new(),
        }
    }

    /// Claim sessions through `migrator`, section 8.4.
    ///
    /// Once. A second call is ignored rather than refused: the binary
    /// installs one at startup and a second would be a second answer to
    /// which mesh a drain goes over.
    pub fn install_migrator(&self, migrator: Arc<Migrator>) {
        let _ = self.migrator.set(migrator);
    }

    /// What claims a session, for whatever reports on it.
    #[must_use]
    pub fn migrator(&self) -> Option<&Arc<Migrator>> {
        self.migrator.get()
    }

    /// Refuse operations through `gate` while the cluster is short,
    /// section 8.5.
    ///
    /// Once, for [`Shared::install_migrator`]'s reason: a second
    /// policy in one process is a second answer to whether a client
    /// may publish.
    pub fn install_gate(&self, gate: Arc<Gate>) {
        let _ = self.gate.set(gate);
    }

    /// Whether `operation` may proceed, section 8.5.
    ///
    /// **True on a node with no gate installed**, which is the default
    /// and is availability: a node whose operator asked for nothing
    /// refuses nothing. The gate re-reads the membership on every call
    /// rather than being told when it changes, because the thing it is
    /// about is a network fault and nothing sends a notification about
    /// one.
    #[must_use]
    pub fn allows(&self, operation: Operation) -> bool {
        self.gate.get().is_none_or(|gate| gate.allows(operation))
    }

    /// Claim `session` for this node and drain it from whoever held it.
    ///
    /// `None` on a node that does not cluster, which is the whole of
    /// what a non-clustering connect pays for section 8.4.
    ///
    /// **Returns once the drain has finished or the patience has run
    /// out**, and returns either way: see `cluster::migrate`. The
    /// register the claim wrote is stored and broadcast here, because
    /// the claim happens outside the broker's lock and nothing else is
    /// holding a batch for it to ride in.
    ///
    /// # The register is broadcast after the drain, and that order is
    /// load-bearing
    ///
    /// Not an artefact of the two statements below being in this order.
    /// [`Shared::lost_session`] is allowed to treat a register naming
    /// another node as proof that no drain is coming, and it is allowed
    /// to do that **only** because the claim reaches the previous owner
    /// before the register does — which is true exactly while the
    /// `await` here precedes the `originate`. Swap them and a previous
    /// owner can be told it has lost a session whose queue is still
    /// being asked for, and the client arrives at its new node with an
    /// empty session.
    pub async fn claim_session(&self, session: &SessionKey, clean: bool) -> Option<Claimed> {
        let migrator = self.migrator.get()?;
        let claimed = migrator.claim(session, clean).await;
        if let Some(write) = claimed.write.clone() {
            self.originate(write);
        }
        Some(claimed)
    }

    /// Store and broadcast one register this node originated, in a batch
    /// of its own.
    ///
    /// For a write made **outside** the broker's lock, which is section
    /// 8.4's claim and nothing else so far: everything the broker
    /// originates rides the batch its own journal is in, and a register
    /// in a batch of its own would be a register the store could take
    /// while the records that caused it were refused.
    ///
    /// Nothing waits for it here. Section 8.2's rule is that a peer is
    /// not told until the store has taken the batch, and that is kept by
    /// the broadcast waiting; a connect waiting as well would put a disk
    /// flush in front of every migrating client's acknowledgement for
    /// no further guarantee.
    ///
    /// **What is deliberately not closed** is the claim that is drained
    /// and then refused by the store. The far node has already given the
    /// session up, so a refusal here leaves ownership written nowhere
    /// until the next claim; the node that it happened on has also just
    /// failed to write everything else it was doing, which is the
    /// condition `cluster::metadata` describes at length and the one a
    /// restart resolves.
    fn originate(&self, write: MetadataWrite) {
        let (Some(replicated), Some(writer)) = (&self.replicated, &self.writer) else {
            return;
        };
        let records = replicated.records(&write);
        match writer.apply_durable(records) {
            Ok(durable) => self.broadcast_when_durable(durable, vec![write]),
            Err(_) => {
                self.metrics
                    .records_dropped(RecordDrop::WriterStopped)
                    .inc_by(1);
                tracing::error!(
                    "the storage writer has stopped, so a session claim was not written down"
                );
            }
        }
    }

    /// Give a session up to the node that claimed it, section 8.4.
    ///
    /// The previous owner's half: the old connection is closed, the
    /// subscriptions leave this node's trie — which is what makes
    /// section 8.3 stop forwarding here — and what the session held
    /// comes back to be sent to the claiming node.
    ///
    /// `None` is a session this node does not hold, which is not an
    /// error: a register naming this node as owner can be older than a
    /// restart.
    pub fn hand_over_session(&self, session: &SessionKey, clean: bool) -> Option<Handover> {
        let (handover, actions) = self.with_broker(|broker| {
            let now = broker.clock().now();
            let mut out = Vec::new();
            let identity = SessionId::new(&session.mountpoint, &session.client_id);
            let handover = broker.hand_over(&identity, clean, now, &mut out);
            (handover, out)
        });
        // The close for the displaced connection among them, which is
        // what stops one session having two live connections.
        self.deliver(actions);
        handover
    }

    /// Give a session up because the cluster says another node owns
    /// it, section 8.5's heal.
    ///
    /// Returns whether a session was given up, which the caller logs:
    /// this is a client being disconnected through no fault of its own
    /// and an operator is owed the line.
    ///
    /// # Why a register naming somebody else is enough, on its own
    ///
    /// There is no epoch comparison here and there must not be one. A
    /// claim is answered by [`Shared::hand_over_session`], and
    /// [`Shared::claim_session`] does not broadcast the register until
    /// the drain it asked for has finished or timed out — so by the
    /// time a register naming another node arrives, exactly one of two
    /// things is true. Either the claim reached this node, the session
    /// has already gone, and `broker.subscriber_of` below finds
    /// nothing; or the claim never arrived, because there was no link
    /// to carry it or the queue carrying it was full, and then **no
    /// drain is coming and none ever will** — the node that claimed it
    /// counted the shortfall and accepted its client anyway.
    ///
    /// A stale register cannot reach here either: what arrives has won
    /// the merge, and an older write loses it.
    ///
    /// The first draft compared the arriving epoch against the one this
    /// node's own session held, to tell a partition from a migration
    /// whose drain was still in flight. That comparison was unsound —
    /// the two numbers are different currencies, because a session's
    /// epoch is `max(held, claimed) + 1` and climbs on every local
    /// reconnect while the register's does not — and the case it was
    /// guarding against cannot happen.
    ///
    /// # What the session was holding is lost, on purpose
    ///
    /// The queue goes with the hand-over and nothing sends it anywhere:
    /// the node that won accepted its client with whatever reached it,
    /// which across a partition was nothing. Section 8.5 says this in
    /// as many words — messages published on either side are not
    /// retroactively exchanged, and a node that acknowledged messages
    /// on the losing side loses them when it rejoins. Keeping them
    /// would mean delivering, long afterwards, messages the other side
    /// has already answered for.
    pub fn lost_session(&self, session: &SessionKey) -> bool {
        let (given_up, actions) = self.with_broker(|broker| {
            let identity = SessionId::new(&session.mountpoint, &session.client_id);
            if broker.subscriber_of(&identity).is_none() {
                // Not held here, which is the ordinary state of almost
                // every register in a cluster — and is also the
                // migration that has already happened.
                return (false, Vec::new());
            }
            let now = broker.clock().now();
            let mut out = Vec::new();
            // The handover is dropped rather than sent; see above.
            let _ = broker.hand_over(&identity, false, now, &mut out);
            (true, out)
        });
        // The close for the losing connection is among them, which is
        // the thing section 8.5 promises.
        self.deliver(actions);
        given_up
    }

    /// What this process counts.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// What this node replicates, for whatever broadcasts it.
    ///
    /// `None` on a node with no store, which is a node that cannot
    /// cluster.
    pub fn replicated(&self) -> Option<&Arc<Replicated>> {
        self.replicated.as_ref()
    }

    /// The queue of writes waiting for the store, taken once.
    ///
    /// `None` on a node that does not replicate, and `None` on the second
    /// call: two drains on one queue would be two halves of the broadcast,
    /// each silently missing what the other took.
    pub fn waiting_to_broadcast(&self) -> Option<mpsc::UnboundedReceiver<Broadcastable>> {
        self.waiting.lock().take()
    }

    /// Hand `writes` to whatever broadcasts them, once `durable` lands.
    ///
    /// For a producer of replicated writes that is **not** the broker's
    /// journal, which is the rule registry: a rule is an operator's and
    /// goes through `rules::live`, not through a packet. Section 8.2's
    /// ordering is the same either way — nothing is shown to a peer until
    /// the store has taken the batch — so there is one queue and one task
    /// draining it rather than two answers to when a write may be sent.
    ///
    /// Never blocks and never fails: a queue nothing drains is a node that
    /// does not cluster, and the write is still stored.
    pub fn broadcast_when_durable(&self, durable: Durable, writes: Vec<MetadataWrite>) {
        if let Some(broadcasts) = &self.broadcasts {
            let _ = broadcasts.send(Broadcastable { durable, writes });
        }
    }

    /// Turn the filters that gained or lost a subscriber into registers.
    ///
    /// Section 8.2, and **appended to the same batch as the journal**:
    /// one batch is all or nothing, so a subscription cannot be stored
    /// without the register that tells the cluster about it, nor the
    /// register without the subscription. Drained under the same lock
    /// that filled it, so the order a filter was gained and lost in is
    /// the order the registers are written in.
    ///
    /// A node with no store drains and drops them, for the reason
    /// [`Self::writer`] gives about the journal: a buffer nobody reads
    /// is a buffer that grows for the life of the process.
    ///
    /// Most events produce nothing. A register is written only when the
    /// count leaves or reaches zero, which is what makes this a write
    /// per filter rather than per client subscription; see
    /// `cluster::metadata`.
    ///
    /// Several writes in one batch each carry the clock's mark, so the
    /// batch can hold the same row several times over. Harmless — the
    /// last one is the highest and the store simply replaces the row —
    /// and the alternative is a caller that holds a register without
    /// the mark that covers its stamp, which is what
    /// `Replicated::records` exists to make impossible.
    fn take_filter_events(
        &self,
        broker: &mut Broker,
        records: &mut Vec<Record>,
        originated: &mut Vec<MetadataWrite>,
    ) {
        let mut events = Vec::new();
        broker.take_filter_events(&mut events);
        let Some(replicated) = &self.replicated else {
            return;
        };
        for event in events {
            // **Four events and two counts.** A shared group is its own
            // register, because a group spanning three nodes reported as
            // three ordinary subscribers to one filter puts all three on
            // the ordinary route and is delivered to three times; see
            // `cluster::groups`. A match with no wildcard, so that an
            // event added to the vocabulary does not compile until it
            // has been given a register.
            let write = match &event {
                FilterEvent::Arrived(filter) => replicated.subscriber_arrived(filter),
                FilterEvent::Departed(filter) => replicated.subscriber_departed(filter),
                FilterEvent::GroupJoined { group, filter } => {
                    replicated.group_joined(group, filter)
                }
                FilterEvent::GroupLeft { group, filter } => replicated.group_left(group, filter),
            };
            if let Some(write) = write {
                records.extend(replicated.records(&write));
                originated.push(write);
            }
        }
    }

    /// Turn the retained messages the broker just changed into registers.
    ///
    /// Section 8.2 replicates retained messages, and **the broker's own
    /// journal is what says one changed.** A second signal beside it — an
    /// event buffer of its own, the way filters have one — was rejected
    /// for section 7.3's reason read the other way round: the journal
    /// already carries the fact, and a second copy of "a retained message
    /// was set" would drift from it the first time one of them was
    /// corrected. Filters need their own buffer because what is
    /// replicated is a count the journal says nothing about; a retained
    /// message is replicated whole and the record holds it.
    ///
    /// **Appended to the same batch**, so a node cannot end up holding a
    /// retained message the cluster was never told about, or a register
    /// for a message it did not keep.
    ///
    /// `peer` is the one case this must not run: a retained message that
    /// arrived *because* a peer replicated it would be stamped again by
    /// this node, which is a write with a later stamp and the same value
    /// bouncing between every pair of nodes for ever.
    fn take_retained(&self, records: &mut Vec<Record>, originated: &mut Vec<MetadataWrite>) {
        let Some(replicated) = &self.replicated else {
            return;
        };
        // Over what is in the batch now, and the registers are appended
        // after the loop: reading by index rather than over a copy, because
        // a fan-out to a thousand offline subscribers is a batch of two
        // thousand records and cloning it on every publish would be a
        // clone nobody asked for.
        let before = records.len();
        let mut made = Vec::new();
        for record in &records[..before] {
            match record {
                Record::RetainedPut { topic, message } => {
                    made.push(replicated.retained(topic, message.clone()));
                }
                Record::RetainedRemoved { topic } => {
                    made.push(replicated.retained_cleared(topic));
                }
                _ => {}
            }
        }
        for write in made {
            records.extend(replicated.records(&write));
            originated.push(write);
        }
    }

    /// Do something with the broker, holding the lock for exactly that,
    /// and hand what it said to the writer with nobody waiting on it.
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
    /// wants [`Shared::with_broker_committed`] instead, and
    /// [`promises_durability`] is how it tells.
    pub fn with_broker<T>(&self, f: impl FnOnce(&mut Broker) -> T) -> T {
        let (value, committed) = self.with_broker_committed(f, |_| false);
        // Nothing promised anything, so there is nothing to wait for.
        // The records are on their way regardless. A writer that is
        // behind would pace a caller here too, and this one declines to
        // be paced: see the last paragraph of [`Shared::hand_over`].
        drop(committed);
        value
    }

    /// The same, for a caller that may have to wait for what it said.
    ///
    /// `promised` is asked, under the lock, whether what came out of
    /// `f` carries a promise to a publisher; [`promises_durability`] is
    /// what answers it. It decides only whether anyone waits. The batch
    /// is handed to the writer either way, and either way it is handed
    /// over **before the guard is released**, which is what keeps the
    /// store's order the order the broker produced.
    ///
    /// What comes back is awaited once the caller has done whatever it
    /// wanted the value for, and before it delivers a single action.
    ///
    /// This takes what the broker has handed over and does not go
    /// looking for records still sitting in a session's own buffer.
    /// That a broker call leaves none behind is `broker-core`'s
    /// property and is guarded there, by `Broker::sessions_holding_records`
    /// in its own tests; asserting it here would walk every session on
    /// the process's one lock, on every packet.
    pub fn with_broker_committed<T>(
        &self,
        f: impl FnOnce(&mut Broker) -> T,
        promised: impl FnOnce(&T) -> bool,
    ) -> (T, Committed) {
        let mut broker = self.broker.lock();
        let value = f(&mut broker);
        // Under the guard, which is still alive until this function
        // returns. Everything it calls returns without touching the
        // device: a send on an unbounded channel, and at most the
        // allocation of a `oneshot`.
        let committed = self.hand_over(&mut broker, promised(&value));
        (value, committed)
    }

    /// Drain the journal and give it to the writer.
    ///
    /// Takes `&mut Broker` rather than the records, and that is the
    /// point: the only way to hold one is to hold the guard, so this
    /// cannot be called from a place that has already let go of it.
    /// It is private for the same reason — a `pub` entry point taking
    /// records would be the shape the ordering bug was.
    ///
    /// Nothing here blocks. [`Writer::apply`] and
    /// [`Writer::apply_durable`] are channel sends; only awaiting the
    /// [`Durable`] they hand back waits on a device.
    ///
    /// # What a backlogged writer does to the caller
    ///
    /// [`Writer::apply`] never blocks and never drops, and says when the
    /// backlog behind it has reached a high-water mark. **The answer
    /// here is to pace the caller**: it is handed a [`Committed::Paced`]
    /// and, because every caller awaits what this returns before it does
    /// anything else, the connection that produced the records stops
    /// reading its peer until the writer has caught up.
    ///
    /// Three answers were available and two are wrong.
    ///
    /// *Drop the records*, which is what [`Shared::deliver`] does to an
    /// action when an outbox is full. It is right there and wrong here:
    /// a discarded action is recovered when its peer reconnects and the
    /// session resumes, while a discarded record is a fact the store
    /// never learns — a retained message that vanishes at the next
    /// restart, a subscription that does not come back. This milestone
    /// exists to stop exactly that.
    ///
    /// *Close the connection*, which is honest but punishes a client for
    /// a slow device, and does not help: the records it already
    /// journalled are in the channel either way.
    ///
    /// *Pace the producer*, which costs the offending connection
    /// latency and costs everything else nothing. A client that outruns
    /// the device is held to the device's rate, which is the rate the
    /// broker can actually keep, and the backlog stops growing because
    /// nothing reads another packet from that socket while it is over
    /// the mark.
    ///
    /// The one caller this does not reach is [`Shared::with_broker`],
    /// which drops what this returns: the tick, a detach and an oversize
    /// discard are not producers a peer can drive in a loop, and putting
    /// the one task that drives every deadline in the process behind a
    /// disk would be the stall this is trying to avoid.
    fn hand_over(&self, broker: &mut Broker, promised: bool) -> Committed {
        self.hand_over_originating(broker, promised, true, Vec::new())
    }

    /// Apply what a peer replicated: the local effect and the registers
    /// that caused it, in one batch.
    ///
    /// `registers` is `cluster::Handled::records` — the registers that won
    /// and the watermark if it moved — and `f` is what they mean to this
    /// node: a retained message set or cleared. **One batch**, because a
    /// watermark stored without the effect it covers is a claim to hold a
    /// write this node would never be sent again, and an effect stored
    /// without the register is a retained message no peer can tell this
    /// node already has.
    ///
    /// Nothing is stamped again; see
    /// [`Shared::hand_over_originating`].
    ///
    /// What comes back is waited on by the caller the way every other
    /// commit is, and it promises nobody anything: a peer is not a
    /// publisher waiting for an acknowledgement.
    pub fn apply_replicated(
        &self,
        registers: Vec<Record>,
        f: impl FnOnce(&mut Broker),
    ) -> Committed {
        let mut broker = self.broker.lock();
        f(&mut broker);
        self.hand_over_originating(&mut broker, false, false, registers)
    }

    /// The same, saying whether this node is the author of what it is
    /// handing over.
    ///
    /// `originate` is false on exactly one path: applying what a peer
    /// replicated. The records still go to the store — this node holds a
    /// copy of every register, so its own retained store and its own
    /// journal carry the message like any other — but **nothing is stamped
    /// again**. A retained message that arrived because a peer replicated
    /// it, re-stamped here, is a later write with the same value, and two
    /// nodes doing that to each other is one message bouncing between them
    /// for ever.
    fn hand_over_originating(
        &self,
        broker: &mut Broker,
        promised: bool,
        originate: bool,
        extra: Vec<Record>,
    ) -> Committed {
        let mut records = Vec::new();
        broker.take_journal(&mut records);
        let mut originated = Vec::new();
        // **Always drained**, originating or not: a buffer nobody reads is
        // a buffer that grows for the life of the process, and a filter
        // event is this node's own whoever caused the call.
        self.take_filter_events(broker, &mut records, &mut originated);
        records.extend(extra);
        // The broker's own facts first and what the cluster is told about
        // them after, which is both the order a reader of the batch wants
        // and the order this has to happen in: the registers are derived
        // from the records, `extra` included. A peer's write arrives as an
        // `extra` record, so **this is the one place the rule that a peer's
        // write is not stamped again is kept** — stamp it and the two nodes
        // tell each other about the same retained message for ever.
        if originate {
            self.take_retained(&mut records, &mut originated);
        }

        let Some(writer) = &self.writer else {
            // No store: nothing is written down and nothing can be
            // promised to a device. Milestone one's mode, still
            // supported, and the journal is dropped rather than left
            // to grow.
            return Committed::Nothing;
        };
        if !originated.is_empty() {
            // Section 8.2's two-phase write. The batch is waited on twice:
            // once by whoever is about to broadcast, and once by this
            // caller if it promised a publisher anything. One batch and
            // one outcome, because whether the store took it is one fact.
            let Some(broadcasts) = &self.broadcasts else {
                // Unreachable: writes are only originated by a node that
                // has a `Replicated`, and the queue is made beside it.
                // Refusing to broadcast is the safe answer if it ever
                // happens, rather than broadcasting without the store.
                return Committed::Nothing;
            };
            return match writer.apply_durable_twice(records) {
                Ok((mine, theirs)) => {
                    // Never blocks: the queue is unbounded, and a send
                    // that fails is a binary with nothing draining it,
                    // which is a node that does not cluster.
                    let _ = broadcasts.send(Broadcastable {
                        durable: theirs,
                        writes: originated,
                    });
                    if promised {
                        Committed::Waiting(mine)
                    } else {
                        // Nothing was promised to a publisher, so nobody
                        // here waits. The broadcast still does.
                        drop(mine);
                        Committed::Nothing
                    }
                }
                Err(_) => Committed::Stopped,
            };
        }
        if !promised {
            if records.is_empty() {
                return Committed::Nothing;
            }
            // Read before the batch is handed over, because handing
            // it over moves it.
            let handed = records.len();
            return match writer.apply(records) {
                Ok(Applied::Accepted) => Committed::Nothing,
                // The writer is behind, so this caller waits for it
                // before it produces anything else. Counted here and
                // reported by the ticker, because the interesting fact
                // is that it happened at all rather than each time.
                Ok(Applied::Backlogged) => {
                    self.paced.fetch_add(1, Ordering::Relaxed);
                    Committed::Paced(Arc::clone(writer))
                }
                Err(_) => {
                    // The thread is gone, so nothing sent from here on
                    // is written and nothing will say so again. Loud,
                    // once per batch: a broker still serving clients
                    // while writing nothing down is the failure this
                    // whole milestone exists to prevent.
                    //
                    // Counted as well as logged, and counted in
                    // records rather than batches: what is lost is
                    // facts about durable state, and how many of them
                    // is the number an operator has to act on.
                    self.metrics
                        .records_dropped(RecordDrop::WriterStopped)
                        .inc_by(handed as u64);
                    tracing::error!(
                        "the storage writer has stopped; nothing the broker says is being written down"
                    );
                    Committed::Nothing
                }
            };
        }
        // Empty records are meaningful here and are not skipped: a
        // connection acknowledging something another batch journalled
        // still has to wait for that batch to reach the device.
        match writer.apply_durable(records) {
            Ok(durable) => Committed::Waiting(durable),
            Err(_) => Committed::Stopped,
        }
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
            // **Section 11.3, counted here and nowhere else.** Every
            // discard the broker reports passes through this loop,
            // and only some of them reach a writer: three of the
            // seven reasons happen precisely because nothing is
            // attached, so a session with no outbox is their normal
            // state. Counting where the bytes are written would
            // therefore report none of the drops that matter most.
            //
            // Before the send, so that a report discarded by a full
            // or missing outbox has still been counted as the drop it
            // reports. It is then counted a second time, under the
            // reason the shell lost it for — which is two facts about
            // two different things, not one thing counted twice.
            if let Action::Dropped { reason, .. } = &action {
                self.metrics.message_dropped(*reason).inc();
            }
            let Some(sender) = outboxes.get(&subscriber) else {
                // The session went away between the broker producing this
                // and us delivering it. Ordinary, not an error.
                self.metrics.shell_dropped(ShellDrop::NoReader).inc();
                continue;
            };
            match sender.try_send(action) {
                Ok(()) => {}
                // The writer has stopped and its receiver is gone. That
                // is the same ordinary teardown as a missing outbox,
                // reached a moment later, and counting it as a slow
                // reader would make the counter report routine
                // disconnections as a capacity problem.
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.metrics.shell_dropped(ShellDrop::NoReader).inc();
                }
                Err(mpsc::error::TrySendError::Full(action)) => {
                    self.metrics.shell_dropped(ShellDrop::OutboxFull).inc();
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
    ///
    /// Reads the metric rather than a counter of its own. This number
    /// and the exposition's are the same event, and two counts of one
    /// event are two numbers waiting to disagree.
    pub fn dropped_for_slow_reader(&self) -> u64 {
        self.metrics.shell_dropped(ShellDrop::OutboxFull).get()
    }

    /// How often a batch that promised nothing found the writer's
    /// backlog at its high-water mark, and so paced the caller that
    /// produced it.
    ///
    /// Zero on a broker whose store keeps up, which is every broker that
    /// is not being outrun. Non-zero is not a defect: it is the bound
    /// working, and it is the number that says a device cannot keep up
    /// with the load being put on it.
    pub fn paced_batches(&self) -> u64 {
        self.paced.load(Ordering::Relaxed)
    }

    /// Say out loud that the writer has been behind, if it has since the
    /// last time this was asked.
    ///
    /// A warning rather than an error, and the difference from
    /// [`Shared::report_rejections`] is the point: a rejection means the
    /// broker and the store disagree about what exists, which is a
    /// defect, while this means the device cannot keep up with the load,
    /// which is capacity. The same shape as the outbox's slow-reader
    /// count one layer down, and for the same reason.
    ///
    /// Reported on the edge rather than every second, so that a standing
    /// count does not bury the line that says it moved. Returns how many
    /// this call reported, which a test needs because a method whose
    /// only effect is a log line cannot be told from one that does
    /// nothing.
    pub fn report_pacing(&self) -> u64 {
        let paced = self.paced_batches();
        let reported = self.pacing_reported.swap(paced, Ordering::Relaxed);
        let new = paced.saturating_sub(reported);
        if new > 0 {
            tracing::warn!(
                paced,
                new,
                outstanding_records = self
                    .writer
                    .as_ref()
                    .map_or(0, |writer| writer.outstanding_records()),
                outstanding_bytes = self
                    .writer
                    .as_ref()
                    .map_or(0, |writer| writer.outstanding_bytes()),
                "the store is not keeping up with what the broker is telling it; \
                 the connections producing it are being held to the device's rate"
            );
        }
        new
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
/// **Test scaffolding**, compiled by this crate's own tests and, under
/// the `testing` feature, by another crate's. `sdmqtt-admin` needs it
/// to make the same assertion about its publish endpoint that
/// `connection` makes about a client's publish, and a second gated
/// store there would be a second set of assumptions about what a
/// device does. Nothing this crate ships turns the feature on.
#[cfg(any(test, feature = "testing"))]
pub mod gated {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use parking_lot::{Condvar, Mutex};
    use storage::record::{BodyRecord, MessageId, OutboxRecord, QueueEntry, Record, SessionKey};
    use storage::{MemoryStorage, Restored, Storage, StorageError};

    /// What the gate is currently doing to a flush.
    #[derive(Debug, Default)]
    struct Gate {
        /// Flushes block until this is cleared.
        held: bool,
        /// Applies block too, which is what a device that has stopped
        /// answering does to the writer's whole channel rather than to
        /// one flush.
        holding_applies: bool,
        /// When the hold gives up and lets everything through, set by
        /// the first call it catches. See [`GatedStore::LONGEST_HOLD`].
        held_until: Option<std::time::Instant>,
        /// Whether a hold is in force at all, which is what tells an
        /// unset deadline from an expired one.
        holding: bool,
        /// Flushes fail rather than succeed.
        failing: bool,
    }

    impl Gate {
        /// How long a call must keep waiting, and `None` when it may go.
        ///
        /// Starts the deadline if this is the first call to be caught.
        fn remaining(&mut self) -> Option<Duration> {
            if !self.holding {
                return None;
            }
            let deadline = *self
                .held_until
                .get_or_insert_with(|| std::time::Instant::now() + GatedStore::LONGEST_HOLD);
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            (!left.is_zero()).then_some(left)
        }
    }

    /// An in-memory store with a gate in front of its flush.
    #[derive(Debug, Default)]
    pub struct GatedStore {
        inner: MemoryStorage,
        gate: Mutex<Gate>,
        opened: Condvar,
        flushes: AtomicU64,
    }

    impl GatedStore {
        /// How long a hold lasts before it lets everything through.
        ///
        /// Far longer than any assertion window here, so it never
        /// decides a test's answer, and finite so that a failing
        /// assertion ends in a report rather than in a hang.
        ///
        /// **One deadline for the whole hold, started by the first
        /// call it catches.** Per call is no bound at all when a test
        /// has queued a thousand batches behind the held one: the
        /// writer's thread pays it once per batch, the join in
        /// `Writer::drop` waits for all of them, and the suite hangs
        /// anyway. Started by the call rather than by `hold` because a
        /// loaded test binary can take seconds to reach the store, and
        /// a budget spent before the test's own assertion window opens
        /// would make the assertion flaky.
        const LONGEST_HOLD: Duration = Duration::from_secs(20);

        /// A store that answers at once.
        pub fn new() -> Self {
            Self::default()
        }

        /// Hold every flush from now on, until [`GatedStore::release`].
        pub fn hold(&self) {
            let mut gate = self.gate.lock();
            gate.held = true;
            gate.holding = true;
            gate.held_until = None;
        }

        /// Hold every apply as well, until [`GatedStore::release`].
        ///
        /// What a stalled device does to the writer: the thread parks in
        /// the store and the channel in front of it stops draining,
        /// which is the only state in which a backlog can be observed at
        /// all.
        pub fn hold_applies(&self) {
            let mut gate = self.gate.lock();
            gate.held = true;
            gate.holding_applies = true;
            gate.holding = true;
            gate.held_until = None;
        }

        /// Let held flushes and applies through, and the ones after
        /// them.
        pub fn release(&self) {
            let mut gate = self.gate.lock();
            gate.held = false;
            gate.holding_applies = false;
            gate.holding = false;
            gate.held_until = None;
            drop(gate);
            self.opened.notify_all();
        }

        /// Fail every flush from now on.
        pub fn fail(&self) {
            self.gate.lock().failing = true;
        }

        /// How many flushes have been answered, held ones included
        /// once they were let through.
        pub fn flushes(&self) -> u64 {
            self.flushes.load(Ordering::Relaxed)
        }
    }

    impl Storage for GatedStore {
        fn apply(&self, records: &[Record]) -> Result<(), StorageError> {
            let mut gate = self.gate.lock();
            // Until released, or until the hold's own deadline, for the
            // reason on [`GatedStore::LONGEST_HOLD`].
            while gate.holding_applies {
                let Some(remaining) = gate.remaining() else {
                    break;
                };
                self.opened.wait_for(&mut gate, remaining);
            }
            drop(gate);
            self.inner.apply(records)
        }

        fn flush(&self) -> Result<(), StorageError> {
            let mut gate = self.gate.lock();
            while gate.held {
                // Let go rather than wait forever. Dropping a `Writer`
                // joins its thread, and the thread's last act is a
                // flush, so a test whose assertion fails while the gate
                // is shut would otherwise deadlock in its own unwind and
                // report nothing at all.
                let Some(remaining) = gate.remaining() else {
                    break;
                };
                self.opened.wait_for(&mut gate, remaining);
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

        fn outbox_head(
            &self,
            rule: &str,
            cursor: u64,
        ) -> Result<Option<OutboxRecord>, StorageError> {
            self.inner.outbox_head(rule, cursor)
        }

        fn outbox_depth(&self, rule: &str) -> Result<u64, StorageError> {
            self.inner.outbox_depth(rule)
        }

        fn outbox_highest_sequence(&self, rule: &str) -> Result<u64, StorageError> {
            self.inner.outbox_highest_sequence(rule)
        }

        fn body(&self, id: &MessageId) -> Result<Option<BodyRecord>, StorageError> {
            self.inner.body(id)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_core::session::{DropReason, MessageId};
    use broker_core::subscriptions::SubscriberId;
    use mqtt_codec::ReasonCode;
    use storage::Record;

    use crate::metrics::{drop_label, shell_label, ShellDrop, ALL_DROP_REASONS, ALL_SHELL_DROPS};

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

    // ----------------------------------------------------------------
    // Section 11.3: a message never disappears without being counted.
    // ----------------------------------------------------------------

    /// A discard report for `reason`, addressed to `subscriber`.
    fn discarded(reason: DropReason) -> Action {
        Action::Dropped {
            id: MessageId::new(),
            reason,
            qos: mqtt_codec::QoS::AtLeastOnce,
            topic: std::sync::Arc::from("a/b"),
        }
    }

    /// Every counter in the two message-drop tables, as a map from
    /// label to value.
    fn drop_counts(shared: &Shared) -> Vec<(&'static str, u64)> {
        let metrics = shared.metrics();
        ALL_DROP_REASONS
            .iter()
            .map(|reason| (drop_label(*reason), metrics.message_dropped(*reason).get()))
            .chain(
                ALL_SHELL_DROPS
                    .iter()
                    .map(|reason| (shell_label(*reason), metrics.shell_dropped(*reason).get())),
            )
            .collect()
    }

    #[tokio::test]
    async fn every_drop_reason_moves_a_counter_of_its_own() {
        // Not a list of assertions: the table is walked, so a reason
        // added to `DropReason` is carried into this test by the same
        // edit that makes the crate compile again. Two reasons mapped
        // onto one counter fails here, because every other counter is
        // asserted to have stayed still.
        //
        // Driven through `deliver` rather than through the metric,
        // because the thing that can be missing is the call. And
        // through `deliver` rather than through the writer loop for a
        // reason that is the whole shape of this: a session that is
        // offline has no outbox, so a report of `qos0_while_offline`
        // — which by definition only happens while it is offline —
        // never reaches a writer at all. Counted where every action
        // passes, or not counted for the cases that matter most.
        for reason in ALL_DROP_REASONS.iter().copied() {
            let shared = Shared::new(Default::default());
            // With a reader attached, so that the only counter that
            // can move is the reason's own: a session with nobody
            // attached also counts a `no_reader` discard, which is a
            // second true fact about a second thing and is asserted
            // on its own below.
            let _inbox = shared.register_outbox(SubscriberId(1), 8);
            shared.deliver(vec![(SubscriberId(1), discarded(reason))]);

            for (label, count) in drop_counts(&shared) {
                let expected = u64::from(label == drop_label(reason));
                assert_eq!(
                    count, expected,
                    "after one {reason:?}, the {label} counter reads {count}"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_discard_for_a_session_with_nobody_attached_is_still_counted() {
        // The case the writer loop cannot see. A session with no
        // outbox is the normal state for three of the seven reasons,
        // and a count taken where the bytes are written would report
        // none of them.
        let shared = Shared::new(Default::default());
        shared.deliver(vec![(
            SubscriberId(7),
            discarded(DropReason::QosZeroWhileOffline),
        )]);

        assert_eq!(
            shared
                .metrics()
                .message_dropped(DropReason::QosZeroWhileOffline)
                .get(),
            1,
            "nobody was attached, and the message still went"
        );
    }

    #[tokio::test]
    async fn an_action_for_a_session_with_no_reader_is_counted() {
        // Convention 5, a test asserting an absence: the existing
        // tests for this path assert that nothing panicked and that
        // the slow-reader figure did not move, which a path that
        // discarded everything silently would also satisfy.
        let shared = Shared::new(Default::default());
        shared.deliver(vec![(SubscriberId(99), Action::Close(ReasonCode::Success))]);

        let gone = shared.register_outbox(SubscriberId(1), 8);
        drop(gone);
        shared.deliver(vec![(SubscriberId(1), Action::Close(ReasonCode::Success))]);

        assert_eq!(
            shared.metrics().shell_dropped(ShellDrop::NoReader).get(),
            2,
            "a missing outbox and a closed one are both nobody reading"
        );
        assert_eq!(
            shared.metrics().shell_dropped(ShellDrop::OutboxFull).get(),
            0,
            "neither of them is a slow reader"
        );
    }

    #[tokio::test]
    async fn the_slow_reader_figure_is_the_metric_and_not_a_second_copy() {
        // One event, one number. The accessor existed before the
        // metric did, and keeping both would be two counts of the
        // same thing waiting to disagree.
        let shared = Shared::new(Default::default());
        let _inbox = shared.register_outbox(SubscriberId(1), 1);
        for _ in 0..5 {
            shared.deliver(vec![(SubscriberId(1), Action::Close(ReasonCode::Success))]);
        }
        assert_eq!(shared.dropped_for_slow_reader(), 4);
        assert_eq!(
            shared.metrics().shell_dropped(ShellDrop::OutboxFull).get(),
            shared.dropped_for_slow_reader(),
        );
    }

    #[tokio::test]
    async fn the_refused_batches_in_the_exposition_are_the_stores_own_count() {
        // Read at scrape time from the writer, which is the only
        // thing that can see a batch refused with nobody waiting to
        // be told. A counter of our own beside it would be a second
        // tally of one event, and the two would disagree the first
        // time a path forgot one of them.
        //
        // The refused batch is handed to the writer directly, for the
        // reason `a_batch_the_store_refuses_is_reported_once_and_then_stays_quiet`
        // gives: the shell's only door writes what the broker said,
        // and the broker will not say this.
        let store: Arc<dyn storage::Storage> = Arc::new(storage::MemoryStorage::new());
        let writer = Arc::new(
            Writer::spawn(store, storage::WriterConfig::default()).expect("a writer thread"),
        );
        let shared = Arc::new(Shared::with_store(
            Broker::new(Default::default()),
            Arc::clone(&writer),
        ));
        shared.metrics().watch(&shared);

        assert!(
            shared
                .metrics()
                .render()
                .contains("sdmqtt_store_batches_rejected_total 0"),
            "registered and zero before anything is refused"
        );

        let applied = writer.apply(vec![Record::QueuePushed {
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
        assert!(applied.is_ok(), "the writer took it");

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if shared.rejected_batches() > 0 {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the refusal is noticed");

        let rendered = shared.metrics().render();
        assert!(
            rendered.contains(&format!(
                "sdmqtt_store_batches_rejected_total {}",
                shared.rejected_batches()
            )),
            "the exposition reports the store's number, not one of ours: {rendered}"
        );
    }

    #[tokio::test]
    async fn the_labels_of_the_two_drop_tables_do_not_collide() {
        // Both tables feed one family, so a label used twice would
        // resolve to one counter and two reasons would silently share
        // it. The walk above would not see it: it compares by label.
        let mut labels: Vec<&str> = ALL_DROP_REASONS
            .iter()
            .map(|reason| drop_label(*reason))
            .chain(ALL_SHELL_DROPS.iter().map(|reason| shell_label(*reason)))
            .collect();
        let before = labels.len();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(before, labels.len(), "a drop reason's label is used twice");
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
                broker
                    .connect(
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
                    )
                    .expect("the default decision authenticates");
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
        // `Rejection::NoSuchSession` refuses, applied with nobody
        // waiting: the one case the storage crate has nowhere to
        // report.
        //
        // Given to the writer directly rather than through the shell,
        // because the shell's only door writes what the broker said
        // and the broker will not say this. The subject here is what
        // `Shared::report_rejections` does with a count, not how the
        // records got there.
        let applied = writer.apply(vec![Record::QueuePushed {
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
        assert!(applied.is_ok(), "the writer took it");

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

    // ----------------------------------------------------------------
    // The backlog: a peer that outruns the device is paced by it,
    // rather than growing the writer's channel until the process dies.
    // ----------------------------------------------------------------

    /// A shell whose store is held and whose writer paces after `mark`
    /// records, and the store and writer to drive it with.
    #[cfg(test)]
    fn a_stalled_shell(mark: u64) -> (Arc<Shared>, Arc<gated::GatedStore>, Arc<Writer>) {
        let store = Arc::new(gated::GatedStore::new());
        // Nothing drains while this holds, so the channel in front of
        // the writer is exactly what the test put there.
        store.hold_applies();
        let writer = Arc::new(
            Writer::spawn(
                Arc::clone(&store) as Arc<dyn storage::Storage>,
                storage::WriterConfig {
                    high_water_records: mark,
                    // One mark at a time: this is about the count.
                    high_water_bytes: u64::MAX,
                    ..storage::WriterConfig::default()
                },
            )
            .expect("a writer thread"),
        );
        let shared = Arc::new(Shared::with_store(
            Broker::new(Default::default()),
            Arc::clone(&writer),
        ));
        (shared, store, writer)
    }

    /// One at-most-once retained publication: one record carrying the
    /// whole payload, journalled for a client that is owed nothing and
    /// waits for nothing. Finding 3's reproduction.
    #[cfg(test)]
    fn a_retained_publication(shared: &Shared, n: usize) -> Committed {
        use mqtt_codec::packet::{Connect, Publish};
        use mqtt_codec::{Properties, QoS, Version};

        let (_, committed) = shared.with_broker_committed(
            |broker| {
                let mut out = Vec::new();
                let registration = broker
                    .connect(
                        "",
                        &Connect {
                            version: Version::V5,
                            client_id: format!("publisher-{n}"),
                            clean_start: true,
                            keep_alive: 0,
                            username: None,
                            password: None,
                            will: None,
                            properties: Properties::default(),
                        },
                        &mut out,
                    )
                    .expect("the default decision authenticates");
                let publish = Packet::Publish(Publish {
                    dup: false,
                    qos: QoS::AtMostOnce,
                    retain: true,
                    topic: format!("sensor/{n}"),
                    packet_id: None,
                    payload: bytes::Bytes::from(vec![0xa5u8; 256]),
                    properties: Properties::default(),
                });
                let _ = broker.handle(registration.subscriber, &publish, &mut out);
            },
            // A fire-and-forget publication promises nobody anything,
            // which is exactly why nothing used to bound this.
            |()| false,
        );
        committed
    }

    #[tokio::test]
    async fn a_producer_that_outruns_the_store_is_paced_by_it() {
        // Finding 3. An at-most-once retained publish touches no
        // receive quota and is never waited on, so before the marks
        // existed one client in a loop grew the writer's channel by a
        // payload at a time until the process died.
        let (shared, store, writer) = a_stalled_shell(4);

        let mut paced = None;
        for n in 0..64 {
            let committed = a_retained_publication(&shared, n);
            if matches!(committed, Committed::Paced(_)) {
                paced = Some(committed);
                break;
            }
        }
        let paced = paced.expect("a producer against a stalled store reaches the mark");
        assert_eq!(shared.paced_batches(), 1);

        // The wait is the pacing. A `Paced` that returned at once would
        // satisfy everything below it and bound nothing.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), paced.wait())
                .await
                .is_err(),
            "the caller was let go while the store was still holding every apply"
        );

        store.release();
        let relieved = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            a_retained_publication(&shared, 64).wait(),
        )
        .await
        .expect("relief comes once the store catches up");
        assert!(relieved.is_ok(), "being paced is not a failure");
        assert!(writer.outstanding_records() < 4 * 64);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_backlog_of_a_producer_that_waits_is_bounded() {
        // The same reproduction as a loop, which is the shape that
        // exhausts memory: a thousand publications against a store that
        // is answering nothing. What must be true afterwards is that
        // the channel holds a bound rather than a thousand payloads.
        //
        // A multi-threaded runtime because the producer parks on relief
        // and the storage thread is what wakes it.
        let (shared, store, writer) = a_stalled_shell(8);

        let producer = tokio::spawn({
            let shared = Arc::clone(&shared);
            async move {
                for n in 0..1_000 {
                    a_retained_publication(&shared, n)
                        .wait()
                        .await
                        .expect("being paced is not a failure");
                }
            }
        });

        // Long enough that an unbounded producer would have journalled
        // all thousand; it manages that in microseconds when nothing
        // stops it.
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let outstanding = writer.outstanding_records();
        assert!(
            !producer.is_finished(),
            "the producer ran to the end against a store that answered nothing"
        );
        assert!(
            outstanding < 64,
            "the backlog reached {outstanding} records against a mark of 8, \
             so nothing is pacing the producer"
        );

        store.release();
        tokio::time::timeout(std::time::Duration::from_secs(30), producer)
            .await
            .expect("the producer finishes once the store answers")
            .expect("it does not panic");
    }

    #[tokio::test]
    async fn pacing_is_reported_once_and_then_stays_quiet() {
        // The same edge-triggered shape as the rejection count beside
        // it, and for the same reason: a standing figure reported every
        // second buries the line that says it moved.
        let (shared, store, _writer) = a_stalled_shell(2);
        assert_eq!(shared.report_pacing(), 0, "nothing has been paced");

        let mut committed = Vec::new();
        for n in 0..32 {
            committed.push(a_retained_publication(&shared, n));
        }
        assert!(shared.paced_batches() > 0, "the mark was never reached");
        assert_eq!(shared.report_pacing(), shared.paced_batches());
        assert_eq!(
            shared.report_pacing(),
            0,
            "a count that has not moved is not news"
        );

        // Nothing here waits: the point is that dropping a `Paced` is
        // allowed, because it promised nothing.
        drop(committed);
        store.release();
    }

    #[tokio::test]
    async fn a_shell_with_no_store_waits_for_nothing() {
        // Running without a store is milestone one's mode and is
        // still supported. Nothing is written down, so nothing can be
        // waited for, and a commit that asked to wait returns rather
        // than hanging.
        //
        // The broker really does produce a record here — a durable
        // session is durable state — so what is being asserted is
        // that the records were dropped, not that there were none.
        let shared = Shared::new(Default::default());
        let (journalled, committed) = shared.with_broker_committed(
            |broker| {
                let mut out = Vec::new();
                broker
                    .connect(
                        "",
                        &mqtt_codec::packet::Connect {
                            version: mqtt_codec::Version::V5,
                            client_id: "durable".to_owned(),
                            clean_start: false,
                            keep_alive: 0,
                            username: None,
                            password: None,
                            will: None,
                            properties: mqtt_codec::Properties::default(),
                        },
                        &mut out,
                    )
                    .expect("the default decision authenticates");
                broker.sessions_holding_records() == 0
            },
            // Asked for, so that a shell with no store is taking the
            // path a promise takes and not the quiet one.
            |_| true,
        );
        assert!(journalled, "the broker left nothing in a session's buffer");
        tokio::time::timeout(std::time::Duration::from_secs(5), committed.wait())
            .await
            .expect("a store-less commit does not wait")
            .expect("and does not fail");
        assert_eq!(shared.rejected_batches(), 0);
    }
}
