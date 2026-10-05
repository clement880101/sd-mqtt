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
//! What is deliberately not applied *here* is a node subscription or a
//! session owner. A node subscription is section 8.3's routing, and the
//! thing that reads it is `cluster::Routes` — which `cluster::Replicated`
//! keeps in step itself, from the four paths that change a register, so
//! there is nothing for this task to do with one. A session owner is
//! section 8.4's migration and is still a register with nothing acting on
//! it.
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

use std::sync::Arc;

use cluster::mesh::{Deliver, Mesh, Message};
use cluster::metadata::{MetadataKey, MetadataValue};
use cluster::MemberState;
use cluster::{MetadataWrite, NodeId, Replicator};
use rules::live::Registry;
use sdmqtt_server::shared::{Broadcastable, Shared};
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
}

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
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<Applying>) {
        let (applying, waiting) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                replicator,
                applying,
                shared,
            }),
            waiting,
        )
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
                self.forwarded(forwarded);
                return Vec::new();
            }
            // A heartbeat. Membership was updated before this was called;
            // there is nothing else it means.
            Message::Heartbeat { .. } => return Vec::new(),
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
        // are the truth either way — see `cluster::routes`.
        let routes = self.replicator.replicated().routes();
        match state {
            MemberState::Dead => routes.died(peer),
            MemberState::Alive => routes.revived(peer),
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
    fn forwarded(&self, forwarded: cluster::Forwarded) {
        let topic = forwarded.body.topic.clone();
        let actions = self.shared.with_broker(|broker| {
            let now = broker.clock().now();
            let mut out = Vec::new();
            let reached = broker.accept_forwarded(forwarded.into_message(), now, &mut out);
            if reached == 0 {
                // Not an error and not a defect: the routing map a peer
                // forwarded by is its copy of this node's registers, and
                // a subscriber that left a moment ago is a copy in
                // flight. Said at debug because a *standing* stream of
                // these is a map that has stopped converging.
                tracing::debug!(
                    topic,
                    "a peer forwarded a publication this node has no subscriber for"
                );
            }
            out
        });
        self.shared.deliver(actions);
    }
}

/// Apply what peers replicated, in the order it arrived.
///
/// Returns when the queue closes, which is the process shutting down.
pub async fn applying(
    mut waiting: mpsc::UnboundedReceiver<Applying>,
    shared: Arc<Shared>,
    rules: Arc<Registry>,
) {
    while let Some(Applying { records, writes }) = waiting.recv().await {
        // The rules go to the registry and everything else to the broker.
        // A rule install opens an outbox and may bind a listener, neither
        // of which can happen under the broker's lock, so the registry
        // writes its own records — and the registers below are stored
        // first, so that what the registry writes is only its own copy.
        let (for_rules, for_broker): (Vec<MetadataWrite>, Vec<MetadataWrite>) = writes
            .into_iter()
            .partition(|write| matches!(write.key(), MetadataKey::Rule { .. }));

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
) {
    while let Some(batch) = waiting.recv().await {
        match batch.landed().await {
            Ok(writes) => {
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
