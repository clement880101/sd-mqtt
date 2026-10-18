//! Section 8.2 on the wire: what this node says, and what it does with
//! what it is told.
//!
//! Everything below the shell computes and nothing transmits. `cluster`'s
//! [`Replicator`] turns a write into a frame and a frame into records;
//! `sdmqtt_server`'s `Shared` turns a frame's registers into local effect
//! and a local change into a write; `rules::live` does the same for a rule.
//! This module is the four pieces of wiring between them, and it is where
//! the mesh is held — which is why it is here rather than in any of them: a
//! crate that held the mesh and was held by it would be a cycle.
//!
//! # Four tasks, and each is one sentence
//!
//! - [`broadcasting`] waits for the store and then tells every peer. A
//!   write is stamped and numbered before the batch carrying it is durable
//!   and is shown to nobody until it is, because **a peer told about a
//!   register whose batch the store refused holds a value this node will
//!   never hold again** and no exchange repairs it.
//! - [`exchanging`] asks one random peer, every few seconds with jitter,
//!   for whatever this node is missing of that peer's log.
//! - [`sweeping`] collects tombstones every node has read and truncates
//!   this node's own log behind the same watermark.
//! - [`applying`] is what a peer's writes mean here: a retained message
//!   set or cleared, a rule installed or removed.
//!
//! # Applying is a queue of one, deliberately
//!
//! [`Replication::delivered`] is called from the task reading one link and
//! must not block or await, so it answers the frame and pushes the writes
//! to [`applying`]. **One task rather than one per frame**, because two
//! writes to one key applied out of order would leave this node running the
//! earlier of them: the register table already decided which won, and the
//! local effect has to follow in the same order.
//!
//! What is deliberately not applied *here* is a node subscription. That is
//! section 8.3's routing, and the thing that reads it is `cluster::Routes`
//! — which `cluster::Replicated` keeps in step itself, from the four paths
//! that change a register, so there is nothing for this task to do with
//! one.
//!
//! A session owner **is** applied here, and it is the one write in this
//! task that disconnects somebody. Section 8.5: two nodes that could not
//! see each other both accepted the same client identifier, and on heal
//! the one whose register lost the merge closes its connection with
//! "session taken over". It is applied after the batch rather than inside
//! it, because the close travels through the losing connection's own
//! outbox and nothing may be delivered under the broker's lock. The rule
//! What makes that safe to do from a register alone — rather than
//! tearing down a session whose drain is still on its way — is an
//! ordering `Shared::claim_session` keeps and `Shared::lost_session`
//! depends on: a claim is sent and waited for before the register that
//! records it is broadcast.
//!
//! # And a publication that a peer forwarded is not a write at all
//!
//! Section 8.3's [`Message::Forward`] arrives on the same link, through
//! the same handler, and goes nowhere near the register table: it is one
//! publication for this node's own subscribers. It is routed under the
//! broker's lock and delivered like any other, and it **fires no rule**,
//! because a rule fires on the node where the message entered the
//! cluster. See `broker_core::session::Message::forwarded`, which is
//! what makes both of those true.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use cluster::mesh::{Deliver, Mesh, Message};
use cluster::metadata::{MetadataKey, MetadataValue};
use cluster::migrate::{Drained, Migrations};
use cluster::MemberState;
use cluster::{MetadataWrite, NodeId, Placement, Replicator, Streamed};
use rules::live::Registry;
use sdmqtt_server::shared::{Broadcastable, Landed, Shared};
use storage::Writer;
use tokio::sync::mpsc;

/// How often tombstones are collected and the log truncated.
///
/// Far more often than the grace period, and far less often than the
/// exchange: the sweep is a walk of this node's tombstones, and what it is
/// waiting for is a watermark that only a round of digests moves.
const SWEEP: std::time::Duration = std::time::Duration::from_secs(30);

/// What this node does with what a peer says.
///
/// Answers the frame where an answer is owed, and queues what it means so
/// that the link's reader is never held: see the module documentation.
///
/// No `Debug`, because `Shared` has none and must not grow one: a
/// derived rendering of it would be every session and every subscription
/// in the process, printed under the broker's lock by whatever logged
/// it.
pub struct Replication {
    /// What decides what a frame changed.
    replicator: Arc<Replicator>,
    /// Where the writes that won go to be applied, in order.
    applying: mpsc::UnboundedSender<Applying>,
    /// The broker behind its lock, for section 8.3's forwarded
    /// publications.
    ///
    /// A publication is not a register and does not go through the
    /// queue above: it is routed and delivered where it arrives, in the
    /// order it arrived, which is what the link already guarantees. A
    /// queue in front of it would be a second ordering to reason about
    /// and a buffer with nothing bounding it but the link.
    shared: Arc<Shared>,
    /// Section 8.4's drains this node is waiting for.
    ///
    /// Made before the mesh and shared with the `Migrator` the shell
    /// installs into `Shared`, because a chunk arrives on a link —
    /// here — and the connection waiting for it is over there.
    migrations: Arc<Migrations>,
    /// Where this node's copies of other nodes' sessions are read
    /// from, to answer a `PullCopy`. Section 8.6.
    ///
    /// The store and not the broker, deliberately: a copy is not a
    /// session this node serves and is not in the broker at all. See
    /// `Shared::apply_aside`, which is how it got there.
    store: Arc<dyn storage::Storage>,
    /// The mesh, for the one thing this handler sends on its own
    /// account: see [`Replication::forwarded`]. Installed after the
    /// mesh exists, which is after this handler does.
    mesh: OnceLock<Arc<Mesh>>,
    /// Sessions this node handed over lately, and where to. Section
    /// 8.3's forward and section 8.4's claim arrive over different
    /// links, so a forward can land after the session it was for has
    /// gone; this is what says where to send it on.
    departed: Mutex<Vec<Departed>>,
    /// Forwards this node has sent on lately, by identifier, so that
    /// one is sent on at most once; see [`Replication::redirect`].
    redirected: Mutex<Vec<([u8; 16], Instant)>>,
    /// When a drain last arrived here: a session is moving to this
    /// node, and a forward for it may land before it is installed.
    last_drain: Mutex<Option<Instant>>,
}

/// A session this node gave up, as a forward arriving late needs it.
#[derive(Debug)]
struct Departed {
    /// The filters the session held, which a late forward's topic is
    /// matched against.
    filters: Vec<String>,
    /// The node that claimed it.
    to: NodeId,
    /// When this is forgotten. A forward is in flight for milliseconds;
    /// a stalled link holds one for as long as the link is stalled, and
    /// section 8.1 gives a link ten seconds before it is dead.
    until: Instant,
}

/// How long a hand-over is remembered for late forwards.
const DEPARTED_HOLDS_FOR: Duration = Duration::from_secs(30);

/// How many redirected forwards are remembered at once.
///
/// What bounds the memory [`Replication::redirect`] keeps when a burst
/// of late forwards arrives inside one window. Past it the oldest is
/// forgotten first, and a forgotten one can be redirected once more,
/// which is a hop rather than a loop.
const REDIRECTED_HELD: usize = 4_096;

/// How long a forward nobody here wants yet is held before it is given
/// up on, and how often it is tried again meanwhile. The hold covers
/// the moment between a claim's drain arriving and the connection that
/// asked for it installing the session, which is two tasks on one node
/// and takes milliseconds; two seconds is the loaded-machine version.
const HELD_FOR: Duration = Duration::from_secs(2);
const HELD_RETRY: Duration = Duration::from_millis(100);

/// Writes a peer sent that won, and the records that go with them.
#[derive(Debug)]
pub struct Applying {
    /// What to store: the registers that won, and the watermark if it
    /// moved.
    records: Vec<storage::Record>,
    /// What they mean here.
    writes: Vec<MetadataWrite>,
}

impl Replication {
    /// A handler over `replicator`, and the queue [`applying`] drains.
    #[must_use]
    pub fn new(
        replicator: Arc<Replicator>,
        shared: Arc<Shared>,
        migrations: Arc<Migrations>,
        store: Arc<dyn storage::Storage>,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<Applying>) {
        let (applying, waiting) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                replicator,
                applying,
                shared,
                migrations,
                store,
                mesh: OnceLock::new(),
                departed: Mutex::new(Vec::new()),
                redirected: Mutex::new(Vec::new()),
                last_drain: Mutex::new(None),
            }),
            waiting,
        )
    }

    /// Give this handler the mesh, once, so a late forward can be sent
    /// on. Without it a late forward is counted and dropped.
    pub fn install_mesh(&self, mesh: Arc<Mesh>) {
        let _ = self.mesh.set(mesh);
    }

    /// Remember where `session` went, for forwards that land late.
    fn departed(&self, filters: Vec<String>, to: NodeId) {
        if filters.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut departed = self.departed.lock().unwrap_or_else(PoisonError::into_inner);
        departed.retain(|entry| entry.until > now);
        departed.push(Departed {
            filters,
            to,
            until: now + DEPARTED_HOLDS_FOR,
        });
    }

    /// Where to send on a forward nobody here wanted, if anywhere.
    ///
    /// [`Self::gone_to`], with the hop bound section 8.4's redirect was
    /// missing. A session that moved B to C and back, then ended inside
    /// the window, leaves each node remembering it went to the other,
    /// and a forward for it went round between them at link speed until
    /// both forgot. Two rules bound it, and neither needs a word on the
    /// wire, so a node from before this is not broken by a frame it
    /// cannot read: **never back to `from`**, which is the two-node loop
    /// at once; and **at most once per forward identifier inside the
    /// window**, which bounds any longer cycle to one hop per node.
    fn redirect(&self, id: [u8; 16], from: NodeId, topic: &str) -> Option<NodeId> {
        let to = self.gone_to(topic)?;
        if to == from {
            return None;
        }
        let now = Instant::now();
        let mut redirected = self
            .redirected
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        redirected.retain(|(_, until)| *until > now);
        if redirected.iter().any(|(held, _)| *held == id) {
            return None;
        }
        if redirected.len() >= REDIRECTED_HELD {
            redirected.remove(0);
        }
        redirected.push((id, now + DEPARTED_HOLDS_FOR));
        Some(to)
    }

    /// The node a late forward for `topic` should go to, if a session
    /// that wanted it left here lately. The most recent hand-over wins,
    /// because a session that moved twice is where it moved last.
    fn gone_to(&self, topic: &str) -> Option<NodeId> {
        let now = Instant::now();
        let departed = self.departed.lock().unwrap_or_else(PoisonError::into_inner);
        departed
            .iter()
            .rev()
            .filter(|entry| entry.until > now)
            .find(|entry| {
                entry
                    .filters
                    .iter()
                    .any(|filter| broker_core::topic::reference::matches(filter, topic))
            })
            .map(|entry| entry.to)
    }
}

impl Replication {
    /// Apply one change to a queue this node holds a copy of, and
    /// say how far the copy has got.
    fn copied(&self, from: NodeId, change: cluster::Streamed) -> Vec<Message> {
        let Some(copies) = self.shared.copies() else {
            // A node that does not cluster cannot be anybody's
            // follower, and a change arriving at one is a peer that
            // has not noticed yet. Nothing to apply and nothing to
            // say.
            return Vec::new();
        };
        let session = change.session.clone();
        let (taken, through) = copies.arrived(&session, from.to_bytes(), change.at, change.change);
        let Some(through) = through else {
            // **Not this copy's stream, and nothing is said.** The
            // session has a new owner, or its owner has started it over,
            // and the copy this node held is of a stream that ended. A
            // mark answered here would be read against the new stream
            // and would make the owner believe a copy exists; silence is
            // a follower with no mark, which is what `repairing` sends
            // the whole queue to. See `Copies::arrived`.
            return Vec::new();
        };

        // **Written aside, not through the broker.** A copy is not a
        // session this node serves: putting it through the broker
        // would enter its filters in this node's trie and deliver its
        // messages to nobody. See `Shared::apply_aside`.
        let mut records = Vec::new();
        for (_, change) in taken {
            records.extend(records_for_change(&session, change));
        }
        if !records.is_empty() {
            records.push(storage::Record::ReplicaFollows {
                session: session.clone(),
                owner: from.to_bytes(),
                through,
            });
            self.shared.apply_aside(records);
        }

        // **Answered even when nothing was applied.** A follower that
        // said nothing while it waited for a hole to fill would look
        // to the owner exactly like one that had stopped, and the
        // repair the owner makes is driven by this number.
        vec![Message::Marked { session, through }]
    }
}

impl Deliver for Replication {
    fn delivered(&self, from: NodeId, message: Message) -> Vec<Message> {
        let frame = match message {
            Message::Replicate(frame) => frame,
            // Section 8.3. One publication, for this node's own
            // subscribers, and nothing is owed back: a publication is
            // not a question.
            Message::Forward(forwarded) => {
                self.forwarded(from, forwarded);
                return Vec::new();
            }
            // A heartbeat. Membership was updated before this was called;
            // there is nothing else it means.
            Message::Heartbeat { .. } => return Vec::new(),
            // Section 8.4. **The one frame that is a question**, and the
            // answer is what the session held — in chunks, so that a
            // drain which stops partway has still moved what it managed
            // to. The close for the old connection goes out inside
            // `hand_over_session`, before a single chunk is framed,
            // which is what stops one session having two live
            // connections.
            Message::Claim(claiming) => {
                let Some(handover) = self
                    .shared
                    .hand_over_session(&claiming.session, claiming.clean)
                else {
                    // **Not held here, and said so.** The register that
                    // named this node is older than a restart, or the
                    // session expired here. An empty complete drain was
                    // the answer before, and the claimer took it at its
                    // word: it never pulled, and the node holding the
                    // queue as a copy was torn down when the register
                    // arrived. See `Drained::not_held`.
                    return vec![Message::Drained(Drained::not_held(
                        &claiming.session,
                        claiming.epoch,
                    ))];
                };
                // Where the session went, for a forward still on its way
                // here; see `forwarded`.
                self.departed(
                    handover
                        .subscriptions
                        .iter()
                        .map(|subscription| subscription.filter.clone())
                        .collect(),
                    from,
                );
                return Drained::chunks(&claiming.session, claiming.epoch, handover)
                    .into_iter()
                    .map(Message::Drained)
                    .collect();
            }
            // One instalment of an answer. Routed to the connection
            // waiting for it, or counted as late and dropped.
            Message::Drained(chunk) => {
                *self
                    .last_drain
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
                self.migrations.arrived(chunk);
                return Vec::new();
            }
            // Section 8.6: one change to a queue this node holds a
            // copy of. Applied in order, and answered with how far
            // this copy has got.
            Message::Replicated(change) => {
                return self.copied(from, change);
            }
            // A follower saying how far it has applied one session.
            // Read by the lag, and by failover when it chooses between
            // copies. Nothing is owed back.
            Message::Marked { session, through } => {
                if let Some(copies) = self.shared.copies() {
                    copies.marked(session, from.to_bytes(), through);
                }
                return Vec::new();
            }
            // Section 8.6's failover, first question: how far does this
            // node's copy of the session go? **Answered even when the
            // answer is nothing**, because the asker is choosing
            // between copies by their marks under a bound, and a
            // silence would be a copy it waited the whole bound for.
            Message::HowFar { session } => {
                let through = self
                    .shared
                    .copies()
                    .and_then(|copies| copies.through(&session));
                return vec![Message::Holds { session, through }];
            }
            // The answer, routed to whoever is asking.
            Message::Holds { session, through } => {
                self.migrations.held(from, &session, through);
                return Vec::new();
            }
            // Section 8.6's failover, second question: send the copy.
            // Read out of the store, because a copy is not in the
            // broker, and cut into the same chunks a drain is cut into
            // — to the node collecting them the two are the same thing.
            Message::PullCopy { session, epoch } => {
                let handover = match cluster::copy_as_handover(self.store.as_ref(), &session) {
                    Ok(handover) => handover,
                    Err(why) => {
                        // Answered with an empty, complete handover
                        // rather than with silence: the asker would
                        // otherwise wait the whole bound for a copy
                        // this node has just failed to read.
                        tracing::error!(
                            peer = %from,
                            client = %session.client_id,
                            %why,
                            "a copy of a session could not be read to answer a pull"
                        );
                        Default::default()
                    }
                };
                return Drained::chunks(&session, epoch, handover)
                    .into_iter()
                    .map(Message::Drained)
                    .collect();
            }
        };
        let handled = match self.replicator.received(from, frame) {
            Ok(handled) => handled,
            Err(why) => {
                // **Logged and not answered.** Section 8.2's frames carry
                // the same records this build writes to its own store, so a
                // frame this build will not read is a peer from another
                // build or a peer misbehaving, and neither is repaired by
                // replying to it. The link is left alone: a peer sending
                // one unreadable register is still a peer whose other
                // registers are worth having.
                tracing::error!(peer = %from, %why, "a peer sent a register this node refused");
                return Vec::new();
            }
        };
        if !handled.records.is_empty() {
            // Never blocks: the queue is unbounded, and what bounds it is
            // the batch bound on the exchange that fills it.
            let _ = self.applying.send(Applying {
                records: handled.records,
                writes: handled.applied,
            });
        }
        handled.reply.map(Message::Replicate).into_iter().collect()
    }

    fn member_changed(&self, peer: NodeId, state: MemberState) {
        // Section 8.2: membership is not replicated, so this is the only
        // thing that can tell the routing map, and nothing is written
        // down. The register stays: this node's judgement is local and
        // may be the wrong half of a one-way partition, and the filters
        // are the truth either way — see `cluster::routes`. One
        // judgement, honoured by both of section 8.3's maps.
        let replicated = self.replicator.replicated();
        match state {
            MemberState::Dead => replicated.died(peer),
            MemberState::Alive => replicated.revived(peer),
            // **Suspect is not dead, and is deliberately nothing here.**
            // Section 8.1 gives failure detection two windows precisely
            // so that a peer that missed a beat is not treated as gone;
            // withholding its forwards at the first missed heartbeat
            // would make a cluster under load stop routing to a node
            // that is working.
            MemberState::Suspect => {}
        }
    }
}

impl Replication {
    /// Route one publication a peer forwarded, and deliver it.
    ///
    /// **Under the broker's lock and then outside it**, which is the
    /// division every other path into the broker makes: the routing and
    /// the journal happen under the guard, and the actions are delivered
    /// once it has been dropped. `Shared::with_broker` is what enforces
    /// it — the guard is its own local and there is no await in it.
    ///
    /// Nothing waits for the store. A forwarded publication promised
    /// nobody anything: there is no publisher here to acknowledge, and
    /// the node that did acknowledge one made its own batch durable
    /// before it answered its client.
    fn forwarded(&self, from: NodeId, forwarded: cluster::Forwarded) {
        let topic = forwarded.body.topic.clone();
        // **The groups the sending node chose this one for**, which is
        // the whole of section 8.3's answer to the shared-subscription
        // trap: the choice is made before the forward, so a group this
        // node has a member of and was not named for was served
        // elsewhere.
        let groups = forwarded.groups.clone();
        // Kept whole until it is known to have reached somebody, so it
        // can be sent on as it arrived; a frame is small and this is
        // the cold path.
        let frame = forwarded.clone();
        let (reached, actions) = self.shared.with_broker(|broker| {
            let now = broker.clock().now();
            let mut out = Vec::new();
            let reached = broker.accept_forwarded(forwarded.into_message(), &groups, now, &mut out);
            (reached, out)
        });
        self.shared.deliver(actions);
        if reached > 0 {
            return;
        }
        // Nobody here wanted it. Two reasons, told apart by what this
        // node remembers. **The session moved**: it was handed over to a
        // node that claimed it while this forward was on the link, which
        // the two links' independence allows at any load and a stalled
        // link makes certain. Then the forward goes on to where the
        // session went, as it arrived, and is counted. **Or the map is
        // behind**: the peer forwarded by its copy of this node's
        // registers, and a subscriber that left a moment ago is a copy
        // in flight. Counted too, where before it was a debug line: a
        // standing stream of these is a map that has stopped converging,
        // and section 11.3 says nothing disappears uncounted.
        let metrics = self.shared.metrics();
        if let (Some(to), Some(mesh)) = (self.redirect(frame.id, from, &topic), self.mesh.get()) {
            match mesh.send(to, &Message::Forward(frame.clone())) {
                Some(Ok(cluster::link::Sent::Queued)) => {
                    metrics.forward_redirected().inc();
                    tracing::info!(
                        topic,
                        %to,
                        "a forwarded publication arrived after its session moved and was sent on"
                    );
                    return;
                }
                other => tracing::warn!(
                    topic,
                    %to,
                    ?other,
                    "a forwarded publication arrived after its session moved and could not \
                     be sent on"
                ),
            }
        }
        // **Or the session is arriving.** This node may be the one a
        // session is moving to: the drain has landed and the connection
        // that claimed it has not yet installed it, which is two tasks
        // here and a window of milliseconds that a redirected forward
        // from the old owner falls into — the old owner answered the
        // claim and then sent the forward on, down the same link, so it
        // arrives right behind the drain. Held and tried again for a
        // bounded time; delivered late rather than lost, or given up on
        // and counted. **Only when a drain has just arrived**, because
        // a hold is also a window in which a subscriber made after the
        // publication would be given it; the moving session's
        // subscription is older than the publication, a new one is not.
        let drained_lately = self
            .last_drain
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some_and(|at| at.elapsed() < HELD_FOR);
        if !drained_lately {
            metrics.forward_unroutable().inc();
            tracing::debug!(
                topic,
                "a peer forwarded a publication this node has no subscriber for"
            );
            return;
        }
        let shared = Arc::clone(&self.shared);
        tokio::spawn(async move {
            let deadline = Instant::now() + HELD_FOR;
            while Instant::now() < deadline {
                tokio::time::sleep(HELD_RETRY).await;
                let (reached, actions) = shared.with_broker(|broker| {
                    let now = broker.clock().now();
                    let mut out = Vec::new();
                    let reached = broker.accept_forwarded(
                        frame.clone().into_message(),
                        &groups,
                        now,
                        &mut out,
                    );
                    (reached, out)
                });
                shared.deliver(actions);
                if reached > 0 {
                    shared.metrics().forward_held().inc();
                    tracing::info!(
                        topic,
                        "a forwarded publication nobody wanted on arrival was delivered after a hold"
                    );
                    return;
                }
            }
            shared.metrics().forward_unroutable().inc();
            tracing::debug!(
                topic,
                "a peer forwarded a publication this node has no subscriber for"
            );
        });
    }
}

/// Apply what peers replicated, in the order it arrived.
///
/// Returns when the queue closes, which is the process shutting down.
pub async fn applying(
    mut waiting: mpsc::UnboundedReceiver<Applying>,
    shared: Arc<Shared>,
    rules: Arc<Registry>,
    directory: Arc<directory::Directory>,
) {
    // Read once rather than per write. A node with no store cannot
    // cluster and so reaches none of this, but the type does not say
    // that, and a `None` here would quietly stop section 8.5's heal
    // instead of stopping the task — so it ends the task, loudly.
    let Some(me) = shared.replicated().map(|replicated| replicated.me()) else {
        tracing::error!(
            "the applying task was started on a node with nothing replicated, so no \
             peer's write will take effect here"
        );
        return;
    };
    while let Some(Applying { records, writes }) = waiting.recv().await {
        // The rules go to the registry and everything else to the broker.
        // A rule install opens an outbox and may bind a listener, neither
        // of which can happen under the broker's lock, so the registry
        // writes its own records — and the registers below are stored
        // first, so that what the registry writes is only its own copy.
        let (for_rules, rest): (Vec<MetadataWrite>, Vec<MetadataWrite>) = writes
            .into_iter()
            .partition(|write| matches!(write.key(), MetadataKey::Rule { .. }));
        // Section 12.1's users, its access control rules and section
        // 11.1's API keys go to the directory, which writes them the
        // way the rule registry writes a rule: the register and the
        // record in one batch, stamped by nobody here.
        let (for_directory, for_broker): (Vec<MetadataWrite>, Vec<MetadataWrite>) =
            rest.into_iter().partition(|write| {
                matches!(
                    write.key(),
                    MetadataKey::User { .. } | MetadataKey::Acl { .. } | MetadataKey::ApiKey { .. }
                )
            });

        // **The registers go to the store whatever they mean here**, and in
        // one batch with the local effect. A node holds every register,
        // including the ones it has nothing to do with yet — section 8.3's
        // routing and section 8.4's migration are what will read them — and
        // the watermark beside them is a claim to hold them, so storing it
        // apart would be a claim to hold writes this node would never be
        // sent again.
        //
        // The records the local effect needs are written here, because the
        // broker does not journal a retained message any way but through a
        // publish — and publishing it would deliver it to this node's
        // subscribers, which is section 8.3's job and not this milestone's.
        // A node that set the message and journalled nothing would hold it
        // until it restarted and would then hold the register without the
        // message, with nothing to reconcile the two.
        let mut batch = records;
        for write in &for_broker {
            batch.extend(records_for(write));
        }
        let committed = shared.apply_replicated(batch, |broker| {
            for write in &for_broker {
                apply_to_broker(broker, write);
            }
        });
        // Nothing was promised to anybody, so this is dropped rather than
        // awaited: the records are on their way, and the next exchange
        // repairs whatever a crash before the flush loses.
        drop(committed);

        // Section 8.5's heal, after the registers are stored and
        // **outside** the lock the batch was applied under: giving a
        // session up closes a connection, which is an action delivered
        // through an outbox.
        //
        // Not in `apply_to_broker` for that reason, although every
        // other local effect is: that closure runs under the lock and
        // has nowhere to put an action.
        for write in &for_broker {
            let MetadataKey::SessionOwner { session } = write.key() else {
                continue;
            };
            let Some(MetadataValue::SessionOwner { node, .. }) = write.register().value() else {
                // A tombstone, which is a session nobody owns: the
                // owner that wrote it gave it up, and a node that was
                // not serving it has nothing to do.
                continue;
            };
            if *node == me {
                // **A peer cannot have originated this.** A log holds
                // only what its own node wrote — `cluster::oplog`'s
                // first line — and what reaches here came off a peer's
                // log, so a register naming this node as the owner is
                // a peer replicating a write it did not make.
                //
                // So this is a broken invariant rather than a case to
                // handle, and it is said out loud rather than skipped
                // quietly. It is still not acted on: handing the
                // session over here would disconnect the client this
                // node has just accepted, on the word of a peer that
                // is already behaving in a way nothing explains.
                tracing::error!(
                    peer_write = %node,
                    client = %session.client_id,
                    "a peer replicated a session owner naming this node, which only its \
                     own log should carry; the register is held and nothing is done about it"
                );
                continue;
            }
            if shared.lost_session(session) {
                tracing::warn!(
                    client = %session.client_id,
                    mountpoint = %session.mountpoint,
                    owner = %node,
                    "another node accepted this client while the two could not see each \
                     other, and its claim won on heal; this node's connection is closed \
                     with session taken over and what its queue held is gone"
                );
            }
        }

        // The registers are already stored above, so the directory's
        // batch carries this node's own copy of the user and nothing
        // else, like the registry's below.
        for write in &for_directory {
            if let Err(why) = directory.apply_replicated(write).await {
                tracing::error!(
                    %why,
                    "a user a peer replicated could not be written here; the cluster and \
                     this node disagree about who may connect until it is fixed"
                );
            }
        }
        for write in &for_rules {
            // The registers are already stored above, so the registry's
            // batch carries this node's own copy of the rule and nothing
            // else.
            if let Err(why) = rules.apply_replicated(write, Vec::new()).await {
                tracing::error!(
                    %why,
                    "a rule a peer replicated is held as a register and is not running on \
                     this node; the cluster and this node disagree until it is fixed"
                );
            }
        }
    }
}

/// What a peer's write has to leave in the store.
///
/// The record the broker would have journalled had this node published the
/// message itself. Empty for a kind the broker does not hold.
fn records_for(write: &MetadataWrite) -> Vec<storage::Record> {
    match (write.key(), write.register().value()) {
        (MetadataKey::Retained { topic }, Some(MetadataValue::Retained(message))) => {
            vec![storage::Record::RetainedPut {
                topic: topic.clone(),
                message: message.clone(),
            }]
        }
        (MetadataKey::Retained { topic }, None) => {
            vec![storage::Record::RetainedRemoved {
                topic: topic.clone(),
            }]
        }
        _ => Vec::new(),
    }
}

/// What a peer's write means to the broker.
///
/// Only the kinds the broker holds. A node subscription and a session owner
/// are registers and nothing else until milestone 6; see the module
/// documentation.
fn apply_to_broker(broker: &mut broker_core::broker::Broker, write: &MetadataWrite) {
    match (write.key(), write.register().value()) {
        (MetadataKey::Retained { topic }, Some(MetadataValue::Retained(message))) => {
            // The same conversion a restore makes, which is the point:
            // a retained message from a peer and one from this node's own
            // store must become the same thing.
            broker
                .retained()
                .set(topic, broker_core::journal::live_retained(message));
        }
        (MetadataKey::Retained { topic }, None) => {
            broker.retained().remove(topic);
        }
        // A register this node holds and acts on later, or a kind whose
        // value does not match its key — which `cluster` has already
        // refused by the time this is reached.
        _ => {}
    }
}

/// Tell every peer about the writes the store has taken.
///
/// Returns when the queue closes, which is the process shutting down.
///
/// **Started on every node that has a store, mesh or no mesh.** A node
/// that does not cluster still stamps and numbers its writes — a store is
/// a node and a node has an identity, a clock and a log — so the queue
/// still fills, and a queue nothing drains is every retained payload this
/// node ever wrote held in memory for the life of the process. With no
/// mesh the writes are published into the log and told to nobody, which is
/// what a cluster of one is and what makes a node that is later given a
/// `cluster.address` have a log worth exchanging.
pub async fn broadcasting(
    mut waiting: mpsc::UnboundedReceiver<Broadcastable>,
    replicator: Arc<Replicator>,
    mesh: Option<Arc<Mesh>>,
    placement: Option<Arc<Placement>>,
) {
    while let Some(batch) = waiting.recv().await {
        match batch.landed().await {
            Ok(landed) => {
                let Landed { writes, streamed } = landed;
                // Section 8.6, after the same wait: a follower is not
                // told about a change the store has not taken, for the
                // reason a peer is not told about a register.
                if let (Some(mesh), Some(placement)) = (&mesh, &placement) {
                    stream_to_followers(mesh, placement, streamed);
                }
                for write in &writes {
                    let Some(frame) = replicator.published(write) else {
                        // Already resolved, which is a write published
                        // twice. Nothing to say and nothing wrong.
                        continue;
                    };
                    let Some(mesh) = &mesh else {
                        // No peers to tell, and nothing kept for peers
                        // there might be later: that is the exchange's.
                        continue;
                    };
                    let fanout = mesh.broadcast(&Message::Replicate(frame));
                    if fanout.dropped > 0 {
                        tracing::warn!(
                            peers = fanout.dropped,
                            "a replicated write was dropped for a peer whose queue was full; \
                             the exchange will repair it"
                        );
                    }
                }
            }
            Err((why, writes)) => {
                // **Not broadcast, and abandoned.** A peer told about a
                // register whose batch the store refused holds a value this
                // node will never hold again, and no exchange repairs it.
                tracing::error!(
                    %why,
                    writes = writes.len(),
                    "a batch carrying replicated writes was not made durable, so no peer is \
                     being told about them"
                );
                for write in &writes {
                    replicator.refused(write);
                }
            }
        }
    }
}

/// What one applied change leaves in the store.
///
/// The same records the owner wrote, which is the point: a copy that
/// stored something else would not be a copy. The body rides with the
/// append when the owner judged the follower might not hold it; a
/// follower that is sent one it already has stores it again, and the
/// store's reference count is what makes that cost nothing.
fn records_for_change(
    session: &storage::SessionKey,
    change: cluster::Change,
) -> Vec<storage::Record> {
    match change {
        cluster::Change::Appended {
            entry,
            body,
            session: record,
        } => {
            let mut records = Vec::new();
            // **The session first, and that ordering is the whole
            // point of it travelling.** A store refuses a queue record
            // for a session it does not hold, so without this every
            // record below is thrown away and the copy exists only in
            // the follower's table. The owner sends it on the first
            // append it streams for a session.
            if let Some(record) = record {
                records.push(storage::Record::SessionPut(*record));
            }
            if let Some(body) = body {
                records.push(storage::Record::BodyPut {
                    id: entry.delivery.body,
                    body: *body,
                });
            }
            records.push(storage::Record::QueuePushed {
                session: session.clone(),
                entry: *entry,
            });
            records
        }
        // An acknowledgement and a removal both take the entry out of
        // the queue; what differs is why, and the store has one record
        // for both because to a queue they are the same event.
        cluster::Change::Acknowledged { sequence } | cluster::Change::Removed { sequence } => {
            vec![storage::Record::QueuePopped {
                session: session.clone(),
                sequence,
            }]
        }
        // The whole queue, replacing whatever copy was held. **The
        // removal first**, so that entries a stale copy held and the
        // owner no longer does are gone, and so that the entries below
        // are not refused as reused sequences by a copy that already
        // had them.
        cluster::Change::Snapshot {
            session: record,
            entries,
        } => {
            let mut records = Vec::with_capacity(entries.len() * 2 + 2);
            records.push(storage::Record::SessionRemoved {
                session: session.clone(),
            });
            records.push(storage::Record::SessionPut(*record));
            for (entry, body) in entries {
                records.push(storage::Record::BodyPut {
                    id: entry.delivery.body,
                    body,
                });
                records.push(storage::Record::QueuePushed {
                    session: session.clone(),
                    entry,
                });
            }
            records
        }
    }
}

/// Repair the replica sets this node's sessions are short of, section
/// 8.6, at a bounded rate.
///
/// Every second: for each session this node streams, the followers
/// rendezvous names now; any of them that has never reported a mark
/// for the session holds no copy of it — it was named after the queue
/// was streamed, because a node is gone or was forgotten — and is sent
/// the whole queue as one [`cluster::Change::Snapshot`]. At most
/// `per_second` sessions a sweep, which is the rate limit: a node that
/// is gone leaves every session it held a copy of to be sent somewhere
/// else, and sending them all at once is what the limit prevents.
///
/// **The grace is not here.** It is in `cluster::Placement`, which
/// keeps a dead node in its sets until `cluster.gone_after_ms` runs
/// out; this task only ever acts on what placement says is short, so a
/// node that is merely restarting never becomes a repair.
///
/// **And any follower stalled behind the owner's position.** A
/// follower that missed one change reports a mark below the hole for
/// ever, and before this it had a mark and so was never repaired; see
/// `cluster::Copies::stalled`. It is sent the same snapshot, which
/// replaces its copy and lands its mark on the owner's position.
///
/// A snapshot that was sent but not yet answered must not be sent
/// again next second, or a slow follower would be sent its queue once
/// a second for as long as it took to apply the first one. So each
/// send is remembered for a bounded time, locally to this task.
pub async fn repairing(
    shared: Arc<Shared>,
    mesh: Arc<Mesh>,
    placement: Arc<Placement>,
    copies: Arc<cluster::Copies>,
    store: Arc<dyn storage::Storage>,
    per_second: u64,
) {
    /// How long a sent snapshot is given before it may be sent again.
    const SENT_HOLDS_FOR: Duration = Duration::from_secs(30);
    /// How long a follower may sit behind at one mark before it is
    /// treated as having missed something. Two sweeps: a follower that
    /// is applying moves its mark within a round trip, and one that has
    /// not moved in two seconds is waiting for a change that is not
    /// coming.
    const STALLED_FOR: Duration = Duration::from_secs(2);

    let mut sent: HashMap<(storage::SessionKey, [u8; 16]), Instant> = HashMap::new();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = Instant::now();
        sent.retain(|_, when| now.duration_since(*when) < SENT_HOLDS_FOR);

        let members = placement.members();
        let me = placement.me();
        let mut budget = per_second;
        let mut pending = 0u64;
        let mut done = 0u64;
        for session in copies.owned_sessions() {
            let followers: Vec<_> = cluster::place(&session, &members, placement.factor())
                .into_iter()
                .filter(|node| *node != me)
                .take(placement.factor().copies().saturating_sub(1))
                .filter(|node| {
                    !copies.has_mark(&session, node.to_bytes())
                        || copies.stalled(&session, node.to_bytes(), now, STALLED_FOR)
                })
                .collect();
            if followers.is_empty() {
                continue;
            }
            pending += 1;
            for follower in followers {
                let key = (session.clone(), follower.to_bytes());
                if sent.contains_key(&key) {
                    continue;
                }
                if budget == 0 {
                    break;
                }
                let Some(record) = shared.with_broker(|broker| broker.session_record(&session))
                else {
                    // Gone between the sweep's listing and now; the
                    // next sweep will not list it.
                    continue;
                };
                let copy = match store.session_copy(&session) {
                    Ok(copy) => copy,
                    Err(why) => {
                        tracing::error!(
                            client = %session.client_id,
                            %why,
                            "a session's queue could not be read to repair a copy of it"
                        );
                        continue;
                    }
                };
                // **Stamped with the position the store's queue reflects,
                // read with it**, not with this node's own position: the
                // store can be a batch behind, and a snapshot of an older
                // queue stamped as current told a follower it held a
                // message it did not. A follower already past the stamp
                // keeps its copy; one behind it takes this and is sent
                // the rest by the stream.
                let at = copy.stream_at.unwrap_or_else(|| copies.position(&session));
                let snapshot = cluster::Streamed {
                    session: session.clone(),
                    at,
                    change: cluster::Change::Snapshot {
                        session: Box::new(record),
                        entries: copy.queued,
                    },
                };
                match mesh.send(follower, &Message::Replicated(snapshot)) {
                    Some(Ok(cluster::link::Sent::Queued)) => {
                        sent.insert(key, now);
                        budget -= 1;
                        done += 1;
                        tracing::info!(
                            client = %session.client_id,
                            to = %follower,
                            "a copy of a session's queue was sent to a follower that held none"
                        );
                    }
                    _ => {
                        tracing::warn!(
                            client = %session.client_id,
                            to = %follower,
                            "a follower named for a session could not be sent its copy; tried \
                             again next sweep"
                        );
                    }
                }
            }
        }
        copies.swept(pending, done);
    }
}

/// Send each change to the nodes that hold a copy of its session.
///
/// **One frame per follower, not a broadcast.** Section 8.6 puts two
/// copies of a session on two nodes; telling fifty nodes about a queue
/// forty-eight of them hold nothing of would cost the cluster its own
/// capacity, and it is the same economy section 8.3 makes for a
/// publication.
///
/// A drop is counted and logged rather than retried here: the follower
/// reports a mark that stops moving, and repairing from that is the
/// owner's business and not this task's. See `cluster::replica`.
fn stream_to_followers(mesh: &Arc<Mesh>, placement: &Arc<Placement>, streamed: Vec<Streamed>) {
    if streamed.is_empty() {
        return;
    }
    let members = placement.members();
    for change in streamed {
        // **This node is the owner, whatever rendezvous thinks.**
        // Section 8.4 puts a session where its client connected, and
        // placement is advice; the two agree most of the time and need
        // not. So this node is skipped wherever it appears and the
        // first `factor - 1` of the rest are the followers, which keeps
        // the number of copies at `factor` either way. Taking the
        // placement set whole would give `factor + 1` copies for every
        // session whose client happened to connect somewhere else.
        let followers = cluster::place(&change.session, &members, placement.factor())
            .into_iter()
            .filter(|node| *node != placement.me())
            .take(placement.factor().copies().saturating_sub(1));
        for node in followers {
            match mesh.send(node, &Message::Replicated(change.clone())) {
                Some(Ok(cluster::link::Sent::Queued)) => {}
                _ => {
                    tracing::warn!(
                        peer = %node,
                        client = %change.session.client_id,
                        "a queue change was not sent to a node holding a copy of this \
                         session; its mark will stop moving until the copy is repaired"
                    );
                }
            }
        }
    }
}

/// Ask one random peer, every few seconds with jitter, for what is missing.
///
/// Never returns.
pub async fn exchanging(replicator: Arc<Replicator>, mesh: Arc<Mesh>) {
    loop {
        tokio::time::sleep(cluster::jitter(replicator.settings().exchange)).await;
        let Some(peer) = cluster::choose(&mesh.linked()) else {
            // A cluster of one, or every link gone. Nothing to ask.
            continue;
        };
        let digest = replicator.digest_for(peer);
        match mesh.send(peer, &Message::Replicate(digest)) {
            // The link went between choosing it and sending, which is the
            // next round's problem.
            None | Some(Ok(_)) => {}
            Some(Err(why)) => tracing::error!(%why, "a digest could not be framed"),
        }
    }
}

/// Collect tombstones every node has read, and truncate this node's log.
///
/// Never returns.
pub async fn sweeping(replicator: Arc<Replicator>, mesh: Arc<Mesh>, writer: Arc<Writer>) {
    loop {
        tokio::time::sleep(SWEEP).await;
        // **Every member, dead ones included**, which is what
        // `Replicator::sweep` asks for: a node that has said nothing has
        // read nothing, and collecting a delete it never saw would leave it
        // holding the deleted value for ever.
        let peers: Vec<NodeId> = mesh
            .members()
            .into_iter()
            .map(|member| member.identity.id())
            .collect();
        let swept = replicator.sweep(cluster::clock::wall_millis(), &peers);
        if swept.records.is_empty() {
            continue;
        }
        tracing::debug!(
            tombstones = swept.collected.len(),
            log_through = swept.log_through,
            "tombstones every node has read have been collected"
        );
        if writer.apply(swept.records).is_err() {
            tracing::error!("the storage writer stopped before a sweep could be written down");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::record::{
        BodyRecord, DeliveryRecord, MessageId, ProtocolVersion, Qos, QueueEntry, SessionRecord,
    };

    /// A snapshot wipes the copy before it writes it.
    ///
    /// **The removal first, and that is the whole test.** A copy that
    /// already held entry 1 — stale, from before this node left the
    /// set — would have the snapshot's entry 1 refused as a reused
    /// sequence, and the whole batch with it, and the follower's mark
    /// would say it held a queue the store had thrown away. That is
    /// the shape of the defect the stream shipped with, one layer up.
    #[test]
    fn a_snapshot_removes_the_old_copy_before_writing_the_new_one() {
        let session = storage::SessionKey::new("", "rover");
        let body = MessageId([3; 16]);
        let records = records_for_change(
            &session,
            cluster::Change::Snapshot {
                session: Box::new(SessionRecord {
                    key: session.clone(),
                    version: ProtocolVersion::V5,
                    expiry_interval: 300,
                    expires_at: None,
                    will: None,
                    will_at: None,
                }),
                entries: vec![(
                    QueueEntry {
                        sequence: 1,
                        packet_id: None,
                        delivery: DeliveryRecord {
                            body,
                            qos: Qos::AtLeastOnce,
                            retain: false,
                            subscription_ids: Vec::new(),
                            dup: false,
                        },
                    },
                    BodyRecord {
                        topic: "a/b".to_owned(),
                        payload: bytes::Bytes::from_static(b"x"),
                        qos: Qos::AtLeastOnce,
                        retain: false,
                        expires_at: None,
                        properties: bytes::Bytes::new(),
                    },
                )],
            },
        );
        let kinds: Vec<&str> = records
            .iter()
            .map(|record| match record {
                storage::Record::SessionRemoved { .. } => "removed",
                storage::Record::SessionPut(_) => "session",
                storage::Record::BodyPut { .. } => "body",
                storage::Record::QueuePushed { .. } => "pushed",
                other => panic!("a snapshot does not write {other:?}"),
            })
            .collect();
        assert_eq!(kinds, vec!["removed", "session", "body", "pushed"]);
    }

    /// A handler over a broker with nothing in it, and the broker.
    fn handler() -> (Arc<Replication>, Arc<Shared>) {
        let shared = Arc::new(Shared::new(Default::default()));
        let replicated = Arc::new(
            cluster::metadata::Replicated::open(
                NodeId::mint(),
                cluster::clock::HybridClock::resume(None, 500),
                &storage::Restored::default(),
            )
            .expect("an empty store opens"),
        );
        let replicator = Arc::new(Replicator::new(
            replicated,
            cluster::ReplicationSettings::default(),
        ));
        let store: Arc<dyn storage::Storage> = Arc::new(storage::MemoryStorage::new());
        let (handler, _applying) = Replication::new(
            replicator,
            Arc::clone(&shared),
            Arc::new(Migrations::default()),
            store,
        );
        (handler, shared)
    }

    /// The last chunk of a drain of an empty session.
    fn drain() -> Message {
        let chunk = Drained::chunks(
            &storage::SessionKey::new("", "rover"),
            1,
            broker_core::handover::Handover::default(),
        )
        .pop()
        .expect("a drain is at least one chunk");
        Message::Drained(chunk)
    }

    /// A forwarded publication on `topic`, for no shared group.
    fn forward(topic: &str) -> Message {
        Message::Forward(cluster::Forwarded {
            id: [9; 16],
            body: BodyRecord {
                topic: topic.to_owned(),
                payload: bytes::Bytes::from_static(b"behind the drain"),
                qos: Qos::AtLeastOnce,
                retain: false,
                expires_at: None,
                properties: bytes::Bytes::new(),
            },
            groups: Vec::new(),
        })
    }

    /// Connect "rover" to the broker and subscribe it to `filter`,
    /// returning the outbox its deliveries land in.
    fn subscribe(shared: &Shared, filter: &str) -> mpsc::Receiver<broker_core::session::Action> {
        use mqtt_codec::packet::{Connect, RetainHandling, Subscribe, SubscriptionOptions};
        use mqtt_codec::{Packet, Properties, QoS, Version};
        let subscriber = shared.with_broker(|broker| {
            let mut out = Vec::new();
            let registration = broker
                .connect(
                    "",
                    &Connect {
                        version: Version::V5,
                        client_id: "rover".to_owned(),
                        clean_start: false,
                        keep_alive: 0,
                        username: None,
                        password: None,
                        will: None,
                        properties: Properties::default(),
                    },
                    &mut out,
                )
                .expect("the default decision authenticates");
            registration.subscriber
        });
        let inbox = shared.register_outbox(subscriber, 8);
        shared.with_broker(|broker| {
            let mut out = Vec::new();
            broker
                .handle(
                    subscriber,
                    &Packet::Subscribe(Subscribe {
                        packet_id: 1,
                        filters: vec![SubscriptionOptions {
                            filter: filter.to_owned(),
                            qos: QoS::AtLeastOnce,
                            no_local: false,
                            retain_as_published: false,
                            retain_handling: RetainHandling::SendAlways,
                        }],
                        properties: Properties::default(),
                    }),
                    &mut out,
                )
                .expect("a subscribe is accepted");
        });
        inbox
    }

    /// Wait for `counter` to reach `value`, for a few holds' worth.
    async fn until(counter: impl Fn() -> u64, value: u64) -> u64 {
        let deadline = Instant::now() + HELD_FOR * 2;
        while counter() != value && Instant::now() < deadline {
            tokio::time::sleep(HELD_RETRY).await;
        }
        counter()
    }

    /// A claim for a session this node does not hold is answered with
    /// "not held", never with an empty complete drain.
    ///
    /// The register naming this node as owner can be older than a
    /// restart, or the session expired here. A complete drain told the
    /// claimer it had everything, so it never pulled from a copy, and
    /// the node holding the real queue as a copy was torn down when the
    /// register arrived.
    #[tokio::test]
    async fn a_claim_for_a_session_not_held_here_is_not_answered_complete() {
        let (handler, _shared) = handler();
        let session = storage::SessionKey::new("", "rover");
        let answer = handler.delivered(
            NodeId::mint(),
            Message::Claim(cluster::Claiming {
                session: session.clone(),
                epoch: 3,
                clean: false,
            }),
        );
        match answer.as_slice() {
            [Message::Drained(chunk)] => {
                assert!(!chunk.last, "a drain of nothing was declared complete");
                assert!(
                    chunk.is_not_held(),
                    "the answer says the session is not here"
                );
                assert_eq!((chunk.session.clone(), chunk.epoch), (session, 3));
            }
            other => panic!("a claim is answered with one chunk, not {other:?}"),
        }
    }

    /// A late forward is redirected once and never back to its sender,
    /// so a stale one cannot circle between two nodes.
    ///
    /// A session that moved B to C and back to B leaves each node
    /// remembering that it went to the other. When it then ends within
    /// the thirty seconds, a forward for it arriving at B was sent on to
    /// C, which sent it back to B, at link speed, until both forgot. The
    /// bound needs nothing on the wire, so an older node is not broken by
    /// it: a node redirects a given forward at most once inside the
    /// window, and not to the node that sent it.
    #[tokio::test]
    async fn a_late_forward_is_redirected_once_and_not_back_to_its_sender() {
        let (handler, _shared) = handler();
        let b = NodeId::mint();
        let c = NodeId::mint();
        handler.departed(vec!["fleet/+/telemetry".to_owned()], c);

        assert_eq!(
            handler.redirect([1; 16], b, "fleet/0043/telemetry"),
            Some(c),
            "a forward that arrived late goes where the session went"
        );
        assert_eq!(
            handler.redirect([1; 16], b, "fleet/0043/telemetry"),
            None,
            "the same forward arriving again has been round once already"
        );
        assert_eq!(
            handler.redirect([2; 16], c, "fleet/0043/telemetry"),
            None,
            "a forward from the node the session went to is not sent back to it"
        );
        assert_eq!(
            handler.redirect([3; 16], b, "fleet/0043/telemetry"),
            Some(c),
            "and another forward is redirected as the first was"
        );
    }

    /// A forward that lands behind a drain, before the session the
    /// drain is for is installed, is delivered once it is.
    ///
    /// Section 8.4's other window, made without a connection task: the
    /// drain arrives, the forward arrives, and nobody is here yet. The
    /// subscriber is installed a moment later, the way the connection
    /// that claimed the session installs it; the message must reach it
    /// and the hold must be counted. Without the hold the forward is
    /// counted unroutable on arrival and the subscriber never sees it.
    #[tokio::test]
    async fn a_forward_behind_a_drain_is_held_until_the_session_is_installed() {
        let (handler, shared) = handler();
        let peer = NodeId::mint();
        handler.delivered(peer, drain());
        handler.delivered(peer, forward("fleet/0043/telemetry"));
        let metrics = shared.metrics();
        assert_eq!(
            (
                metrics.forward_held().get(),
                metrics.forward_unroutable().get()
            ),
            (0, 0),
            "neither delivered nor dropped yet: held"
        );

        let mut inbox = subscribe(&shared, "fleet/+/telemetry");
        assert_eq!(
            until(|| metrics.forward_held().get(), 1).await,
            1,
            "the hold was counted"
        );
        assert_eq!(metrics.forward_unroutable().get(), 0);
        let topics: Vec<String> = std::iter::from_fn(|| inbox.try_recv().ok())
            .filter_map(|action| match action {
                broker_core::session::Action::Send(mqtt_codec::Packet::Publish(publish)) => {
                    Some(publish.topic)
                }
                _ => None,
            })
            .collect();
        assert_eq!(topics, vec!["fleet/0043/telemetry".to_owned()]);
    }

    /// A forward nobody wants is dropped and counted, unless a drain has
    /// just arrived: a hold is a window a later subscriber could fall
    /// into, and only a moving session has a subscription older than
    /// the publication.
    #[tokio::test]
    async fn a_forward_nobody_wants_is_not_held_without_a_drain() {
        let (handler, shared) = handler();
        handler.delivered(NodeId::mint(), forward("fleet/0043/telemetry"));
        let metrics = shared.metrics();
        assert_eq!(metrics.forward_unroutable().get(), 1, "dropped on arrival");

        // Even a subscriber arriving within the hold's span gets nothing.
        let mut inbox = subscribe(&shared, "fleet/+/telemetry");
        tokio::time::sleep(HELD_RETRY * 3).await;
        assert_eq!(metrics.forward_held().get(), 0);
        assert!(
            !std::iter::from_fn(|| inbox.try_recv().ok()).any(|action| matches!(
                action,
                broker_core::session::Action::Send(mqtt_codec::Packet::Publish(_))
            )),
            "a subscriber made after the publication was not given it"
        );
    }
}
