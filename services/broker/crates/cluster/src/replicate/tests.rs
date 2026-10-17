//! Tests for the broadcast and the log behind it.
//!
//! # A cluster with no runtime, no clock and no socket
//!
//! [`Cluster`] is nodes, stores and a list of frames in flight. A link is
//! up or down per **ordered pair**, because that is where the interesting
//! failures are — a node that can be heard and cannot hear is two halves
//! of a cluster with two answers — and a frame sent over a link that is
//! down is counted and thrown away, which is what
//! [`crate::link::Outgoing`] does to a peer whose queue is full and what a
//! link that has gone does to everything.
//!
//! Nothing here is a mesh. What the mesh adds is sockets, a handshake and
//! which peers there are, and the three-process test in
//! `crates/sdmqtt/tests/cluster.rs` is where that is proved. What is
//! proved here is which frames there are and what they do.
//!
//! # Every write goes through a store
//!
//! [`Node::retain`] stamps the write, gives the batch to a real
//! [`MemoryStorage`], and only then publishes it. That is not ceremony:
//! the store refuses a log sequence it has already seen, so a numbering
//! mistake fails here rather than in a comment, and the two-phase write
//! this module exists to get right is only observable against something
//! that can refuse.

use super::*;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use storage::{
    MemoryStorage, MetadataKeyRecord, MetadataRecord, MetadataValueRecord, Restored,
    RetainedRecord, StampRecord, Storage,
};

use crate::clock::{HybridClock, DEFAULT_MAX_DRIFT_MILLIS};
use crate::metadata::{MetadataKey, MetadataValue};
use crate::register::Stamp;

/// How many nodes most of these tests use.
const NODES: usize = 3;

/// One node: its replicated state, the store under it, and its name.
struct Node {
    /// What a failure message calls it.
    name: &'static str,
    /// What this node writes and what it has been told.
    replicator: Replicator,
    /// The store under it, which can refuse.
    store: Arc<MemoryStorage>,
}

impl Node {
    /// A node with an empty store, identified by `index`.
    ///
    /// Sixteen identical bytes, so that the order of two identities is the
    /// order of the numbers that made them and a tie-break a test asserts
    /// on is one a reader can check.
    fn new(index: u8, name: &'static str) -> Self {
        let store = Arc::new(MemoryStorage::new());
        Self {
            name,
            replicator: replicator_over(NodeId::from_bytes([index; 16]), &Restored::default()),
            store,
        }
    }

    fn id(&self) -> NodeId {
        self.replicated().me()
    }

    fn replicated(&self) -> &Arc<Replicated> {
        self.replicator.replicated()
    }

    /// Retain `payload` on `topic`: stamp it, store it, publish it.
    ///
    /// Returns the frame to broadcast, which is what a caller hands to
    /// [`Cluster::broadcast`].
    fn retain(&self, topic: &str, payload: &'static str) -> Replication {
        let write = self.replicated().retained(topic, retained(payload));
        self.store
            .apply(&self.replicated().records(&write))
            .expect("a store must take what this node decided to write");
        self.replicator
            .published(&write)
            .expect("a write this node originated is publishable once")
    }

    /// Clear the retained message on `topic`: a tombstone, stored and
    /// published.
    fn clear(&self, topic: &str) -> Replication {
        let write = self.replicated().retained_cleared(topic);
        self.store
            .apply(&self.replicated().records(&write))
            .expect("a store must take what this node decided to write");
        self.replicator
            .published(&write)
            .expect("a write this node originated is publishable once")
    }

    /// The stamp on the tombstone this node holds for `topic`.
    fn tombstone_stamp(&self, topic: &str) -> Stamp {
        let key = MetadataKey::Retained {
            topic: topic.to_owned(),
        };
        self.replicated()
            .tombstones()
            .into_iter()
            .find(|(held, _)| *held == key)
            .unwrap_or_else(|| panic!("{} holds no tombstone for {topic}", self.name))
            .1
    }

    /// Whether this node holds any register at all for `topic`.
    ///
    /// A tombstone is one; a collected tombstone is not. The difference is
    /// the whole of what a sweep does.
    fn holds_anything(&self, topic: &str) -> bool {
        self.replicated()
            .register(&MetadataKey::Retained {
                topic: topic.to_owned(),
            })
            .is_some()
    }

    /// Retain `payload`, and have the store refuse the batch.
    ///
    /// The batch is never offered to the store at all, which is the same
    /// thing as far as this node is concerned: nothing was taken. What is
    /// asserted about it is that nothing reaches a peer.
    fn retain_and_be_refused(&self, topic: &str, payload: &'static str) -> MetadataWrite {
        let write = self.replicated().retained(topic, retained(payload));
        assert!(
            self.replicator.refused(&write),
            "a write this node originated must be refusable once"
        );
        write
    }

    /// What this node holds on `topic`, or `None` for a tombstone or an
    /// absence.
    fn held(&self, topic: &str) -> Option<RetainedRecord> {
        match self.replicated().value(&MetadataKey::Retained {
            topic: topic.to_owned(),
        }) {
            Some(MetadataValue::Retained(message)) => Some(message),
            Some(other) => panic!("{} holds {other:?} on {topic}", self.name),
            None => None,
        }
    }

    /// The payload this node holds on `topic`, as text.
    fn text(&self, topic: &str) -> Option<String> {
        self.held(topic)
            .map(|message| String::from_utf8_lossy(&message.payload).into_owned())
    }

    /// Restart from the store, as a process would.
    fn restart(&mut self) {
        let restored = self.store.restore().expect("a restore");
        self.replicator = replicator_over(self.id(), &restored);
    }
}

/// A replicator over a node resumed from `restored`.
fn replicator_over(me: NodeId, restored: &Restored) -> Replicator {
    Replicator::new(
        Arc::new(
            Replicated::open(
                me,
                HybridClock::resume(restored.clock, DEFAULT_MAX_DRIFT_MILLIS),
                restored,
            )
            .expect("what this build wrote, it reads"),
        ),
        ReplicationSettings::default(),
    )
}

fn retained(payload: &'static str) -> RetainedRecord {
    RetainedRecord {
        payload: bytes::Bytes::from_static(payload.as_bytes()),
        qos: storage::Qos::AtMostOnce,
        expires_at: None,
        properties: bytes::Bytes::new(),
    }
}

/// Nodes, the frames between them, and which directions are severed.
struct Cluster {
    nodes: Vec<Node>,
    /// Frames in flight, as `(from, to, frame)`, in the order they were
    /// sent.
    wire: Vec<(usize, usize, Replication)>,
    /// Ordered pairs that are not carrying anything.
    down: BTreeSet<(usize, usize)>,
    /// How many frames each ordered pair has delivered.
    delivered: BTreeMap<(usize, usize), usize>,
    /// And how many it threw away, which is what a link that has gone
    /// does.
    dropped: BTreeMap<(usize, usize), usize>,
    /// What every frame of the last `settle` was answered with.
    answered: Vec<Option<Replication>>,
}

impl Cluster {
    fn of(count: usize) -> Self {
        const NAMES: [&str; 3] = ["alpha", "beta", "gamma"];
        Self {
            nodes: (0..count)
                .map(|index| {
                    Node::new(
                        u8::try_from(index + 1).expect("a small index"),
                        NAMES[index],
                    )
                })
                .collect(),
            wire: Vec::new(),
            down: BTreeSet::new(),
            delivered: BTreeMap::new(),
            dropped: BTreeMap::new(),
            answered: Vec::new(),
        }
    }

    /// Sever what `from` says to `to`, and nothing else.
    fn cut(&mut self, from: usize, to: usize) {
        self.down.insert((from, to));
    }

    /// Let it through again.
    fn heal(&mut self, from: usize, to: usize) {
        self.down.remove(&(from, to));
    }

    /// Send `frame` from `from` to every other node, as a broadcast does.
    ///
    /// A pair that is down throws it away and counts it, because that is
    /// what happens on a real link: a queue that is full drops and counts,
    /// and a link that has gone has no queue to put anything in.
    fn broadcast(&mut self, from: usize, frame: &Replication) {
        for to in 0..self.nodes.len() {
            if to == from {
                continue;
            }
            if self.down.contains(&(from, to)) {
                *self.dropped.entry((from, to)).or_default() += 1;
            } else {
                self.wire.push((from, to, frame.clone()));
            }
        }
    }

    /// Deliver everything in flight, storing what each node decided.
    ///
    /// Returns how many frames were delivered. Replies go back to whoever
    /// sent the frame, which is what the mesh does with them, so a round
    /// of the exchange runs to its end here in one call.
    fn settle(&mut self) -> usize {
        let mut carried = 0;
        // Bounded, because a round that answered itself for ever would
        // hang a test rather than failing it. Far more than the longest
        // round any of these needs, which is two frames per batch.
        for _ in 0..1_000 {
            if self.wire.is_empty() {
                return carried;
            }
            for (from, to, frame) in std::mem::take(&mut self.wire) {
                let origin = self.nodes[from].id();
                let node = &self.nodes[to];
                let handled = node
                    .replicator
                    .received(origin, frame)
                    .expect("a frame this build wrote, it reads");
                if !handled.records.is_empty() {
                    node.store
                        .apply(&handled.records)
                        .expect("a store must take what a node decided to keep");
                }
                *self.delivered.entry((from, to)).or_default() += 1;
                carried += 1;
                self.answered.push(handled.reply.clone());
                if let Some(reply) = handled.reply {
                    self.say(to, from, reply);
                }
            }
        }
        panic!("a round of the exchange never ended");
    }

    /// Send one frame from `from` to `to`, if that direction carries.
    fn say(&mut self, from: usize, to: usize, frame: Replication) {
        if self.down.contains(&(from, to)) {
            *self.dropped.entry((from, to)).or_default() += 1;
        } else {
            self.wire.push((from, to, frame));
        }
    }

    /// One round of the exchange: `from` asks `to` for what it is missing.
    ///
    /// Returns how many frames the round took, which is what the batch
    /// bound is visible in: a peer far behind takes two frames per batch
    /// rather than two frames in total.
    fn exchange(&mut self, from: usize, to: usize) -> usize {
        self.answered.clear();
        let digest = self.nodes[from].replicator.digest_for(self.nodes[to].id());
        self.say(from, to, digest);
        self.settle()
    }

    /// Sweep node `which` against `now_millis`, storing what went.
    ///
    /// Every other node in the cluster is a peer, dead or not, which is
    /// what a node's own member list is.
    fn sweep(&mut self, which: usize, now_millis: u64) -> Swept {
        let peers: Vec<NodeId> = (0..self.nodes.len())
            .filter(|other| *other != which)
            .map(|other| self.nodes[other].id())
            .collect();
        let node = &self.nodes[which];
        let swept = node.replicator.sweep(now_millis, &peers);
        if !swept.records.is_empty() {
            node.store
                .apply(&swept.records)
                .expect("a store must take what a sweep decided");
        }
        swept
    }

    /// Every reply the last `settle` produced, `None` included.
    ///
    /// What "a node that has everything pulls nothing" is asserted on:
    /// the answer to its digest has to be no frame at all, rather than a
    /// frame carrying nothing.
    fn answers(&self) -> &[Option<Replication>] {
        &self.answered
    }

    /// How many frames `from` has delivered to `to`.
    fn carried(&self, from: usize, to: usize) -> usize {
        self.delivered.get(&(from, to)).copied().unwrap_or(0)
    }

    /// How many frames `from` threw away on the way to `to`.
    fn lost(&self, from: usize, to: usize) -> usize {
        self.dropped.get(&(from, to)).copied().unwrap_or(0)
    }
}

// ---- the broadcast -------------------------------------------------------

/// A write reaches two peers.
#[test]
fn a_write_reaches_two_peers() {
    let mut cluster = Cluster::of(NODES);
    let frame = cluster.nodes[0].retain("site/1/state", "warm");
    cluster.broadcast(0, &frame);
    assert_eq!(cluster.settle(), 2, "a broadcast reaches every peer, once");

    for which in 0..NODES {
        assert_eq!(
            cluster.nodes[which].text("site/1/state").as_deref(),
            Some("warm"),
            "{} does not hold the write",
            cluster.nodes[which].name
        );
    }

    // And each peer now knows how far it has read the writer's log, which
    // is what keeps the exchange from asking for it again.
    for which in 1..NODES {
        assert_eq!(
            cluster.nodes[which]
                .replicated()
                .seen_through(cluster.nodes[0].id()),
            1,
            "{} did not count the broadcast against the writer's log",
            cluster.nodes[which].name
        );
    }
}

/// A write to a node whose link is down still lands in the log.
///
/// **The whole reason the log exists.** The broadcast is gone — there is
/// nothing to replay it from and nothing will — so the only thing that can
/// ever repair the peer is the exchange, and the exchange is a claim about
/// this log.
#[test]
fn a_write_to_a_node_whose_link_is_down_still_lands_in_the_log() {
    let mut cluster = Cluster::of(NODES);
    cluster.cut(0, 1);

    let frame = cluster.nodes[0].retain("site/1/state", "warm");
    cluster.broadcast(0, &frame);
    cluster.settle();

    assert_eq!(cluster.lost(0, 1), 1, "the cut has to have bitten");
    assert_eq!(
        cluster.nodes[1].text("site/1/state"),
        None,
        "a severed peer was reached anyway"
    );
    assert_eq!(
        cluster.nodes[2].text("site/1/state").as_deref(),
        Some("warm"),
        "a peer that was there missed a broadcast"
    );

    // The write is in the writer's log, servable, which is what the
    // exchange will find.
    let log = cluster.nodes[0].replicated();
    assert_eq!(log.log_published_through(), 1);
    assert_eq!(log.log_len(), 1);
    assert_eq!(
        log.log_after(0, 10).len(),
        1,
        "the write a broadcast lost is not in the log, so nothing can repair it"
    );
    assert_eq!(
        cluster.nodes[1]
            .replicated()
            .seen_through(cluster.nodes[0].id()),
        0,
        "a peer that received nothing counted something"
    );
}

/// A peer that was down and comes back is not sent the backlog.
///
/// **A broadcast that replays is a broadcast that duplicates.** There is no
/// queue behind a broadcast and there must not be: an implementation that
/// kept what a severed peer missed and flushed it on reconnection would
/// send every one of those entries to every peer that was there all along,
/// because a broadcast is the same frame to everybody.
///
/// So what is asserted is a count rather than a state: after the link
/// returns, exactly **one** frame crosses it, carrying the one write made
/// after it returned. The three writes made while it was down are in the
/// writer's log and reach the peer only through the exchange, which is the
/// next task.
#[test]
fn a_peer_that_was_down_and_comes_back_is_not_sent_the_backlog() {
    let mut cluster = Cluster::of(NODES);

    // One write while everything is up, so that "nothing crossed" cannot
    // pass by the link never having worked.
    let frame = cluster.nodes[0].retain("before", "zero");
    cluster.broadcast(0, &frame);
    cluster.settle();
    assert_eq!(cluster.carried(0, 1), 1);

    cluster.cut(0, 1);
    for (topic, payload) in [("a", "one"), ("b", "two"), ("c", "three")] {
        let frame = cluster.nodes[0].retain(topic, payload);
        cluster.broadcast(0, &frame);
        cluster.settle();
    }
    assert_eq!(cluster.lost(0, 1), 3);
    assert_eq!(cluster.carried(0, 1), 1, "nothing crossed while it was cut");

    cluster.heal(0, 1);
    let frame = cluster.nodes[0].retain("d", "four");
    cluster.broadcast(0, &frame);
    cluster.settle();

    assert_eq!(
        cluster.carried(0, 1),
        2,
        "the peer was sent {} frames when the link returned, and a broadcast carries \
         one write: a backlog flushed by a reconnection is every entry in it sent \
         twice to every peer that never left",
        cluster.carried(0, 1) - 1
    );
    assert_eq!(cluster.nodes[1].text("d").as_deref(), Some("four"));
    for topic in ["a", "b", "c"] {
        assert_eq!(
            cluster.nodes[1].text(topic),
            None,
            "the backlog arrived by broadcast, which is the exchange's job"
        );
    }

    // And the peer's watermark is honest about it: it holds the writer's
    // first entry and the fifth, so it may not claim anything past the
    // first — or the exchange would never send it the three in between.
    assert_eq!(
        cluster.nodes[1]
            .replicated()
            .seen_through(cluster.nodes[0].id()),
        1,
        "a peer counted past a broadcast it never received"
    );
    assert_eq!(cluster.nodes[0].replicated().log_published_through(), 5);
}

/// A broadcast the store refused is sent to nobody and served to nobody.
///
/// The two-phase write, which this module's documentation and
/// [`crate::oplog`]'s explain at length. A peer told about a register whose
/// batch the store refused holds a value this node will never hold again,
/// and no exchange repairs it: the exchange serves this node's log, and
/// this node's log lost the entry in the same refused batch.
#[test]
fn a_write_the_store_refused_is_broadcast_to_nobody_and_served_to_nobody() {
    let mut cluster = Cluster::of(NODES);
    let refused = cluster.nodes[0].retain_and_be_refused("site/1/state", "never-stored");

    assert_eq!(
        cluster.nodes[0].replicator.published(&refused),
        None,
        "a refused write was publishable, so a broadcast could still carry it"
    );
    assert_eq!(
        cluster.nodes[0].replicated().log_published_through(),
        0,
        "a refused write is servable by the exchange"
    );
    assert_eq!(cluster.nodes[0].replicated().log_after(0, 10), Vec::new());

    // And the next write takes the number the refused one had, so the log
    // a peer sees has no hole in it.
    let frame = cluster.nodes[0].retain("site/1/state", "stored");
    cluster.broadcast(0, &frame);
    cluster.settle();
    assert_eq!(cluster.nodes[0].replicated().log_published_through(), 1);
    assert_eq!(
        cluster.nodes[1].text("site/1/state").as_deref(),
        Some("stored")
    );
}

// ---- the exchange that repairs what a broadcast lost ---------------------

/// Two nodes that missed a broadcast converge without one.
///
/// The claim the log exists for, end to end: the broadcast is gone and
/// nothing will ever replay it, so the only thing that can repair the peer
/// is the digest round — and what it carries is this node's log rather than
/// its registers.
#[test]
fn two_nodes_that_missed_a_broadcast_converge_without_one() {
    let mut cluster = Cluster::of(2);
    cluster.cut(0, 1);
    for (topic, payload) in [("a", "one"), ("b", "two"), ("c", "three")] {
        let frame = cluster.nodes[0].retain(topic, payload);
        cluster.broadcast(0, &frame);
        cluster.settle();
    }
    assert_eq!(cluster.lost(0, 1), 3, "the cut has to have bitten");
    assert_eq!(cluster.nodes[1].text("a"), None);

    // Healed, and then one round and nothing else. No broadcast happens
    // in between, which is what makes this the exchange's doing.
    cluster.heal(0, 1);
    cluster.exchange(1, 0);

    for (topic, payload) in [("a", "one"), ("b", "two"), ("c", "three")] {
        assert_eq!(
            cluster.nodes[1].text(topic).as_deref(),
            Some(payload),
            "the exchange did not carry {topic}"
        );
    }
    assert_eq!(
        cluster.nodes[1]
            .replicated()
            .seen_through(cluster.nodes[0].id()),
        3,
        "a node that has been served a batch must say so, or it is served it again"
    );
}

/// Three nodes where one was partitioned converge after it returns.
///
/// Both directions and both halves: the node that was away has writes to
/// learn and writes to teach, and each is a round of its own because a
/// round is a pull. The two that stayed up learn the absent node's writes
/// from the absent node, not from each other, which is what "the digest is
/// a claim about an operation log" means in a cluster of three.
#[test]
fn three_nodes_where_one_was_partitioned_converge_after_it_returns() {
    let mut cluster = Cluster::of(NODES);
    // Gamma is cut off from both others, in both directions.
    for other in 0..2 {
        cluster.cut(other, 2);
        cluster.cut(2, other);
    }

    let frame = cluster.nodes[0].retain("from/alpha", "alpha");
    cluster.broadcast(0, &frame);
    cluster.settle();
    let frame = cluster.nodes[1].retain("from/beta", "beta");
    cluster.broadcast(1, &frame);
    cluster.settle();
    let frame = cluster.nodes[2].retain("from/gamma", "gamma");
    cluster.broadcast(2, &frame);
    cluster.settle();

    assert_eq!(cluster.nodes[0].text("from/beta").as_deref(), Some("beta"));
    assert_eq!(cluster.nodes[2].text("from/alpha"), None);
    assert_eq!(cluster.nodes[0].text("from/gamma"), None);

    for other in 0..2 {
        cluster.heal(other, 2);
        cluster.heal(2, other);
    }
    // Every ordered pair that was severed gets one round. Gamma pulls from
    // both, and both pull from gamma.
    for other in 0..2 {
        cluster.exchange(2, other);
        cluster.exchange(other, 2);
    }

    for which in 0..NODES {
        for (topic, payload) in [
            ("from/alpha", "alpha"),
            ("from/beta", "beta"),
            ("from/gamma", "gamma"),
        ] {
            assert_eq!(
                cluster.nodes[which].text(topic).as_deref(),
                Some(payload),
                "{} does not hold {topic} after the partition healed",
                cluster.nodes[which].name
            );
        }
    }
}

/// A node that has everything pulls nothing.
///
/// **The test that the digest is a digest** rather than a request for
/// everything. The answer must be no frame at all: an implementation that
/// answered every digest with its whole log would make the exchange a
/// periodic full resynchronisation, and one that answered with an empty
/// batch would be a frame per peer per interval for ever on an idle
/// cluster.
#[test]
fn a_node_that_has_everything_pulls_nothing() {
    let mut cluster = Cluster::of(2);
    for (topic, payload) in [("a", "one"), ("b", "two")] {
        let frame = cluster.nodes[0].retain(topic, payload);
        cluster.broadcast(0, &frame);
        cluster.settle();
    }
    assert_eq!(cluster.nodes[1].text("b").as_deref(), Some("two"));

    let frames = cluster.exchange(1, 0);
    assert_eq!(frames, 1, "the round should be the digest and no answer");
    assert_eq!(
        cluster.answers(),
        &[None],
        "a peer that has everything was sent something"
    );

    // And the digest still told the holder where that peer is, which is
    // what the tombstone watermark is a minimum over.
    assert_eq!(
        cluster.nodes[0]
            .replicator
            .peer_holds_of_ours(cluster.nodes[1].id()),
        2,
        "a digest that carried nothing back still has to be heard"
    );
}

/// The batch bound is honoured: a node far behind is served in batches.
///
/// A retained message can be a quarter of a megabyte and the link refuses
/// a frame larger than it carries, so a batch that ignored its bound would
/// be a repair that cannot happen on exactly the node that needs it most.
/// What this asserts is the shape of the round rather than its length in
/// bytes: with a bound of two and six entries missing, the round is three
/// batches and the digests between them.
#[test]
fn the_batch_bound_is_honoured() {
    const BEHIND: usize = 6;
    const BOUND: usize = 2;

    let mut cluster = Cluster::of(2);
    cluster.nodes[0].replicator = Replicator::new(
        std::sync::Arc::clone(cluster.nodes[0].replicated()),
        ReplicationSettings {
            batch: BOUND,
            ..ReplicationSettings::default()
        },
    );
    cluster.cut(0, 1);
    for which in 0..BEHIND {
        let frame = cluster.nodes[0].retain(&format!("t/{which}"), "payload");
        cluster.broadcast(0, &frame);
        cluster.settle();
    }
    cluster.heal(0, 1);

    let frames = cluster.exchange(1, 0);
    let batches: Vec<usize> = cluster
        .answers()
        .iter()
        .filter_map(|answer| match answer {
            Some(Replication::Batch { entries, .. }) => Some(entries.len()),
            _ => None,
        })
        .collect();
    assert_eq!(
        batches,
        vec![BOUND, BOUND, BOUND],
        "the bound was not applied: {frames} frames carried {batches:?}"
    );
    assert!(
        batches.iter().all(|carried| *carried <= BOUND),
        "a batch carried more than the bound"
    );
    assert_eq!(
        cluster.nodes[1]
            .replicated()
            .seen_through(cluster.nodes[0].id()),
        BEHIND as u64,
        "the round did not finish"
    );
    for which in 0..BEHIND {
        assert!(
            cluster.nodes[1].text(&format!("t/{which}")).is_some(),
            "t/{which} never arrived"
        );
    }
}

/// A batch that says there is no more ends the round.
///
/// The other half of the bound: a peer that was sent everything must not
/// ask again, or an idle cluster is two frames per peer per interval for
/// ever.
#[test]
fn a_batch_that_says_there_is_no_more_ends_the_round() {
    let mut cluster = Cluster::of(2);
    cluster.cut(0, 1);
    let frame = cluster.nodes[0].retain("a", "one");
    cluster.broadcast(0, &frame);
    cluster.settle();
    cluster.heal(0, 1);

    let frames = cluster.exchange(1, 0);
    assert_eq!(
        frames,
        2,
        "the round should be a digest and one batch: {:?}",
        cluster.answers()
    );
    assert!(matches!(
        cluster.answers(),
        [Some(Replication::Batch { more: false, .. }), None]
    ));
}

/// The exchange interval is jittered, and a test can see it.
///
/// Section 8.2 asks for jitter and the plan asks whether a test can tell
/// it is there. It can, because the jitter is a range rather than a
/// distribution: every draw must land between half the interval and all of
/// it, and an implementation that returned the interval itself would sit
/// at the top of that range every time. Fifty draws all equal is a
/// probability of nothing, so a bare interval fails here and the mutation
/// is caught rather than merely argued about.
#[test]
fn the_exchange_interval_is_jittered() {
    let interval = ReplicationSettings::default().exchange;
    let draws: Vec<Duration> = (0..50).map(|_| jitter(interval)).collect();

    for drawn in &draws {
        assert!(
            *drawn >= interval / 2 && *drawn <= interval,
            "a jittered interval of {drawn:?} is outside half of {interval:?} and all of it; \
             a backoff must never become no backoff"
        );
    }
    assert!(
        draws.iter().collect::<BTreeSet<_>>().len() > 1,
        "fifty draws were all {:?}, so the interval is not jittered and fifty nodes \
         started together ask each other in the same millisecond for ever",
        draws[0]
    );

    // And an interval of nothing stays nothing, so a configuration that
    // asks for a tight loop is not quietly turned into something else.
    assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
}

// ---- tombstones go, once everyone has seen them --------------------------

/// How long after a tombstone's own reading a sweep is run.
///
/// The grace period and a second, so that the grace is never what keeps
/// one: every test below that wants it gone passes this.
fn well_past_the_grace(stamp: Stamp) -> u64 {
    stamp.reading().millis()
        + u64::try_from(ReplicationSettings::default().tombstone_grace.as_millis())
            .expect("an hour in milliseconds is a small number")
        + 1_000
}

/// Everybody has read it and the grace has passed, so it is gone.
///
/// Both watermarks in one: the tombstone leaves every node's register
/// table, and the log entry that carried it leaves the writer's log,
/// because nothing will ever ask for an entry every node already holds.
#[test]
fn a_tombstone_every_node_has_read_and_whose_grace_has_passed_is_gone() {
    let mut cluster = Cluster::of(NODES);
    let written = cluster.nodes[0].retain("site/1/state", "warm");
    cluster.broadcast(0, &written);
    cluster.settle();
    let deleted = cluster.nodes[0].clear("site/1/state");
    cluster.broadcast(0, &deleted);
    cluster.settle();

    let stamp = cluster.nodes[0].tombstone_stamp("site/1/state");
    for which in 0..NODES {
        assert!(
            cluster.nodes[which].holds_anything("site/1/state"),
            "{} holds no tombstone to collect",
            cluster.nodes[which].name
        );
        assert_eq!(cluster.nodes[which].text("site/1/state"), None);
    }

    // Both peers tell the writer where they are, which is the only thing
    // that ever says so.
    cluster.exchange(1, 0);
    cluster.exchange(2, 0);
    let swept = cluster.sweep(0, well_past_the_grace(stamp));
    assert_eq!(
        swept.collected,
        vec![MetadataKey::Retained {
            topic: "site/1/state".to_owned()
        }]
    );
    assert_eq!(
        swept.log_through, 2,
        "the writer's own log was not truncated"
    );
    assert!(!cluster.nodes[0].holds_anything("site/1/state"));
    assert_eq!(cluster.nodes[0].replicated().log_len(), 0);

    // And the peers collect it once the writer's digest has told them
    // everybody has read it. Before that they keep it, because nothing
    // they hold says who has seen it.
    assert!(cluster.sweep(1, well_past_the_grace(stamp)).is_empty());
    assert!(cluster.nodes[1].holds_anything("site/1/state"));
    cluster.exchange(0, 1);
    cluster.exchange(0, 2);
    for which in 1..NODES {
        cluster.sweep(which, well_past_the_grace(stamp));
        assert!(
            !cluster.nodes[which].holds_anything("site/1/state"),
            "{} kept a tombstone every node has read",
            cluster.nodes[which].name
        );
    }
}

/// A tombstone whose grace has passed but which a node has not read is
/// kept.
///
/// **The watermark, on its own.** The grace period is satisfied and the
/// tombstone stays, because one of the three has read nothing past the
/// value it is superseding.
#[test]
fn a_tombstone_whose_grace_has_passed_but_which_a_node_has_not_read_is_kept() {
    let mut cluster = Cluster::of(NODES);
    cluster.cut(0, 2);
    let written = cluster.nodes[0].retain("site/1/state", "warm");
    cluster.broadcast(0, &written);
    cluster.settle();
    let deleted = cluster.nodes[0].clear("site/1/state");
    cluster.broadcast(0, &deleted);
    cluster.settle();
    let stamp = cluster.nodes[0].tombstone_stamp("site/1/state");

    // Beta has both and says so; gamma has neither and says so. Gamma's
    // digest reaches the writer — the cut is the other direction — and the
    // batch answering it does not, which is exactly a node that is still
    // away.
    cluster.exchange(1, 0);
    assert_eq!(
        cluster.nodes[0]
            .replicator
            .peer_holds_of_ours(cluster.nodes[1].id()),
        2
    );
    cluster.exchange(2, 0);
    assert_eq!(
        cluster.nodes[0]
            .replicator
            .peer_holds_of_ours(cluster.nodes[2].id()),
        0,
        "gamma received nothing, so it must claim nothing"
    );

    let swept = cluster.sweep(0, well_past_the_grace(stamp));
    assert!(
        swept.collected.is_empty(),
        "a tombstone one node has never read was collected on the grace period alone, \
         so that node keeps the deleted value for ever"
    );
    assert!(cluster.nodes[0].holds_anything("site/1/state"));
    assert_eq!(
        swept.log_through, 0,
        "the log was truncated past what gamma has read, so the repair is gone too"
    );
    assert_eq!(cluster.nodes[0].replicated().log_len(), 2);
}

/// The watermark is the lowest a node has read, not the highest.
///
/// The mutation this is for is one character's worth: a maximum where the
/// minimum belongs. The node that is up to date is the one node whose
/// agreement proves nothing, so taking the highest is taking the answer of
/// whichever peer needs the tombstone least.
#[test]
fn the_watermark_is_the_lowest_a_node_has_read_and_not_the_highest() {
    let mut cluster = Cluster::of(NODES);
    // A batch of one, so that gamma's round tells the writer where it was
    // when it asked the second time: one entry read, which is more than
    // nothing and less than everything. Replaced before anything is
    // exchanged, because a replacement forgets what peers have said.
    cluster.nodes[0].replicator = Replicator::new(
        std::sync::Arc::clone(cluster.nodes[0].replicated()),
        ReplicationSettings {
            batch: 1,
            ..ReplicationSettings::default()
        },
    );
    cluster.cut(0, 2);
    let written = cluster.nodes[0].retain("a", "one");
    cluster.broadcast(0, &written);
    cluster.settle();
    let deleted = cluster.nodes[0].clear("a");
    cluster.broadcast(0, &deleted);
    cluster.settle();
    let stamp = cluster.nodes[0].tombstone_stamp("a");

    cluster.exchange(1, 0);
    cluster.heal(0, 2);
    cluster.exchange(2, 0);
    assert_eq!(
        cluster.nodes[0]
            .replicator
            .peer_holds_of_ours(cluster.nodes[2].id()),
        1,
        "gamma should have read exactly one entry"
    );
    assert_eq!(
        cluster.nodes[0]
            .replicator
            .peer_holds_of_ours(cluster.nodes[1].id()),
        2,
        "beta should have read both"
    );

    let swept = cluster.sweep(0, well_past_the_grace(stamp));
    assert!(
        swept.collected.is_empty(),
        "the highest of one and two was taken rather than the lowest, so a tombstone \
         gamma has not read was collected"
    );
    assert_eq!(
        swept.log_through, 1,
        "the log was truncated to the highest rather than the lowest"
    );
    assert_eq!(cluster.nodes[0].replicated().log_len(), 1);
}

/// A tombstone from a node nothing has been heard from is kept.
///
/// Zero is "nothing of yours" and a node that has sent no digest has said
/// nothing about who has read its writes. Collecting on no information is
/// collecting on a guess.
#[test]
fn a_tombstone_from_a_node_nothing_has_been_heard_from_is_kept() {
    let mut cluster = Cluster::of(2);
    let written = cluster.nodes[0].retain("a", "one");
    cluster.broadcast(0, &written);
    cluster.settle();
    let deleted = cluster.nodes[0].clear("a");
    cluster.broadcast(0, &deleted);
    cluster.settle();
    let stamp = cluster.nodes[1].tombstone_stamp("a");

    assert!(
        cluster
            .sweep(1, well_past_the_grace(stamp))
            .collected
            .is_empty(),
        "a tombstone was collected although the node that wrote it has said nothing \
         about who has read it"
    );
    assert!(cluster.nodes[1].holds_anything("a"));
}

/// A record deleted while a node was away is still deleted when it
/// returns.
///
/// **The test that matters in this task.** The node that was away comes
/// back holding the value, and what deletes it is the tombstone the
/// deleting node kept *because* that node had not read it. Collect on the
/// grace period alone and the deleting node has thrown away both the
/// tombstone and the log entry carrying it, so the exchange has nothing to
/// send: the absent node keeps a retained message every other node has
/// deleted, answers subscribers with it, and no later exchange ever
/// contradicts it.
///
/// That is the plan's resurrection seen from a client. It is not a value
/// spreading back out — a node serves only the log it originated, so a
/// register it merely holds is never offered to anybody — and the module
/// documentation says why the distinction does not make it any less
/// permanent.
#[test]
fn a_record_deleted_while_a_node_was_away_is_still_deleted_when_it_returns() {
    let mut cluster = Cluster::of(NODES);
    let written = cluster.nodes[0].retain("site/1/state", "warm");
    cluster.broadcast(0, &written);
    cluster.settle();
    for which in 1..NODES {
        assert_eq!(
            cluster.nodes[which].text("site/1/state").as_deref(),
            Some("warm")
        );
    }

    // Gamma is away for the delete.
    cluster.cut(0, 2);
    let deleted = cluster.nodes[0].clear("site/1/state");
    cluster.broadcast(0, &deleted);
    cluster.settle();
    assert_eq!(
        cluster.nodes[2].text("site/1/state").as_deref(),
        Some("warm"),
        "gamma was supposed to miss the delete"
    );

    // Everything else carries on for long enough that the grace period is
    // no longer what is keeping the tombstone. Beta tells the writer it has
    // read both; gamma, still cut off, tells it nothing.
    cluster.exchange(1, 0);
    let stamp = cluster.nodes[0].tombstone_stamp("site/1/state");
    let swept = cluster.sweep(0, well_past_the_grace(stamp));
    assert!(
        swept.collected.is_empty(),
        "the delete was thrown away while a node still held the value"
    );

    // And now gamma comes back and one round deletes it.
    cluster.heal(0, 2);
    cluster.exchange(2, 0);
    assert_eq!(
        cluster.nodes[2].text("site/1/state"),
        None,
        "a node that missed a delete came back holding the value, and nothing will \
         ever contradict it"
    );
    assert!(
        cluster.nodes[2].holds_anything("site/1/state"),
        "gamma must hold the tombstone rather than nothing, or the next peer it speaks \
         to hands it its own older copy"
    );

    // With every node past it, it goes everywhere.
    cluster.exchange(2, 0);
    let swept = cluster.sweep(0, well_past_the_grace(stamp));
    assert_eq!(
        swept.collected,
        vec![MetadataKey::Retained {
            topic: "site/1/state".to_owned()
        }],
        "once every node has read it there is nothing left to keep it for"
    );
}

/// A node with no peers collects behind its own published end.
///
/// A cluster of one is a supported way to run this broker, and a watermark
/// that is a minimum over an empty set must not be zero: every node that
/// exists has read everything this node wrote, because it is the only one.
#[test]
fn a_node_with_no_peers_collects_behind_its_own_published_end() {
    let mut cluster = Cluster::of(1);
    let _ = cluster.nodes[0].retain("a", "one");
    let _ = cluster.nodes[0].clear("a");
    let stamp = cluster.nodes[0].tombstone_stamp("a");

    let swept = cluster.sweep(0, well_past_the_grace(stamp));
    assert_eq!(swept.log_through, 2);
    assert_eq!(
        swept.collected,
        vec![MetadataKey::Retained {
            topic: "a".to_owned()
        }]
    );
    assert_eq!(cluster.nodes[0].replicated().log_len(), 0);
}

/// A tombstone whose grace has not passed is kept, however far everyone has
/// read.
///
/// The other half of the pair. The watermark says the delete has arrived
/// everywhere; the grace period is what covers a node that is not in the
/// member list at all yet — one being added, one whose process has not
/// started — and section 8.2 asks for both.
#[test]
fn a_tombstone_whose_grace_has_not_passed_is_kept() {
    let mut cluster = Cluster::of(2);
    let written = cluster.nodes[0].retain("a", "one");
    cluster.broadcast(0, &written);
    cluster.settle();
    let deleted = cluster.nodes[0].clear("a");
    cluster.broadcast(0, &deleted);
    cluster.settle();
    let stamp = cluster.nodes[0].tombstone_stamp("a");
    cluster.exchange(1, 0);

    let swept = cluster.sweep(0, stamp.reading().millis() + 1);
    assert!(
        swept.collected.is_empty(),
        "a tombstone was collected a millisecond after it was written"
    );
    assert_eq!(
        swept.log_through, 2,
        "the log is truncated behind what every node has read whatever the grace \
         period says: an entry everybody holds is an entry nobody will ask for"
    );
    assert!(cluster.nodes[0].holds_anything("a"));

    // And it goes once the hour has passed.
    assert!(!cluster.sweep(0, well_past_the_grace(stamp)).is_empty());
    assert!(!cluster.nodes[0].holds_anything("a"));
}

// ---- the log across a restart --------------------------------------------

/// The log is gapless across a restart, and numbering carries on.
///
/// The mutation this is for is the same defect as a clock resuming from
/// zero, in the other of section 8.2's two counters: a log that numbered
/// its next write one after a restart would tell a peer "this is my first
/// write" about a write the peer already holds under that number, and the
/// peer would never ask for either of them again.
#[test]
fn the_log_is_gapless_across_a_restart() {
    let mut cluster = Cluster::of(2);
    for (topic, payload) in [("a", "one"), ("b", "two"), ("c", "three")] {
        let frame = cluster.nodes[0].retain(topic, payload);
        cluster.broadcast(0, &frame);
        cluster.settle();
    }
    let before: Vec<u64> = cluster.nodes[0]
        .replicated()
        .log_after(0, usize::MAX)
        .iter()
        .map(Logged::sequence)
        .collect();
    assert_eq!(before, vec![1, 2, 3]);

    cluster.nodes[0].restart();
    let after: Vec<u64> = cluster.nodes[0]
        .replicated()
        .log_after(0, usize::MAX)
        .iter()
        .map(Logged::sequence)
        .collect();
    assert_eq!(after, before, "the log did not come back whole");
    assert_eq!(cluster.nodes[0].replicated().log_high_water(), 3);

    // And the next write is the fourth, which is what the store's own
    // refusal of a reused sequence would otherwise catch — and does, if
    // this is wrong: the batch below goes to the same store.
    let frame = cluster.nodes[0].retain("d", "four");
    cluster.broadcast(0, &frame);
    cluster.settle();
    assert_eq!(
        cluster.nodes[0]
            .replicated()
            .log_after(0, usize::MAX)
            .iter()
            .map(Logged::sequence)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4],
        "a restarted log reused a number a peer had already been served"
    );
    assert_eq!(cluster.nodes[1].text("d").as_deref(), Some("four"));
}

/// A peer's watermark survives a restart, so nothing is pulled twice.
///
/// Durable for the reason [`storage::SeenRecord`] gives: a node that came
/// back claiming to have read nothing would be served logs that have been
/// truncated behind their own watermark, and would hold every peer's
/// watermark down until it caught up.
#[test]
fn what_a_node_has_read_of_a_peers_log_survives_its_own_restart() {
    let mut cluster = Cluster::of(2);
    for (topic, payload) in [("a", "one"), ("b", "two")] {
        let frame = cluster.nodes[0].retain(topic, payload);
        cluster.broadcast(0, &frame);
        cluster.settle();
    }
    let writer = cluster.nodes[0].id();
    assert_eq!(cluster.nodes[1].replicated().seen_through(writer), 2);

    cluster.nodes[1].restart();
    assert_eq!(
        cluster.nodes[1].replicated().seen_through(writer),
        2,
        "a restarted node forgot how far it had read a peer's log"
    );
    assert_eq!(
        cluster.nodes[1].replicated().seen(),
        vec![(writer, 2)],
        "the watermarks did not come back"
    );
    assert_eq!(cluster.nodes[1].text("b").as_deref(), Some("two"));
}

// ---- what a peer may say -------------------------------------------------

/// An entry numbered zero is refused, and changes nothing.
///
/// Zero is what a watermark means by "nothing of yours", so an entry at
/// zero is an entry no watermark could ever be past: a peer would hold it
/// and go on asking for it for ever.
#[test]
fn an_entry_numbered_zero_is_refused() {
    let cluster = Cluster::of(2);
    let frame = Replication::Broadcast {
        entry: LoggedRecord {
            sequence: 0,
            entry: MetadataRecord {
                key: MetadataKeyRecord::Retained {
                    topic: "a".to_owned(),
                },
                stamp: StampRecord {
                    millis: 1,
                    counter: 0,
                    node: [1; 16],
                },
                value: Some(MetadataValueRecord::Retained(retained("nope"))),
            },
        },
    };
    let error = cluster.nodes[1]
        .replicator
        .received(cluster.nodes[0].id(), frame)
        .expect_err("a log numbers from one");
    assert_eq!(error, ReplicationError::SequenceZero);
    assert_eq!(
        cluster.nodes[1].text("a"),
        None,
        "a refused frame was applied anyway"
    );
}

/// A register whose key and value name different kinds is refused.
///
/// Nothing this build writes can produce the pair, so a peer that sent one
/// is a peer this build does not speak to. Convention 1 read the other way
/// round, and convention 2: the refusal names which kinds disagreed rather
/// than reporting a category.
#[test]
fn a_register_whose_key_and_value_disagree_is_refused() {
    let cluster = Cluster::of(2);
    let frame = Replication::Broadcast {
        entry: LoggedRecord {
            sequence: 1,
            entry: MetadataRecord {
                key: MetadataKeyRecord::Retained {
                    topic: "a".to_owned(),
                },
                stamp: StampRecord {
                    millis: 1,
                    counter: 0,
                    node: [1; 16],
                },
                value: Some(MetadataValueRecord::NodeSubscription),
            },
        },
    };
    let error = cluster.nodes[1]
        .replicator
        .received(cluster.nodes[0].id(), frame)
        .expect_err("a key and a value must name one kind");
    assert!(
        matches!(
            error,
            ReplicationError::Register(MetadataError::KindMismatch {
                key: "a retained message",
                value: "a node subscription",
            })
        ),
        "refused as {error:?}"
    );
    assert_eq!(cluster.nodes[1].text("a"), None);
}

/// A write already held is taken as nothing, and nothing is stored.
///
/// What a caller uses to decide not to pass something on, and what keeps a
/// broadcast of something everybody has from being a write to every store
/// in the cluster.
#[test]
fn a_write_already_held_changes_nothing() {
    let mut cluster = Cluster::of(2);
    let frame = cluster.nodes[0].retain("a", "one");
    cluster.broadcast(0, &frame);
    cluster.settle();

    let again = cluster.nodes[1]
        .replicator
        .received(cluster.nodes[0].id(), frame)
        .expect("it reads");
    assert!(
        again.is_empty(),
        "a register already held was stored and applied again: {again:?}"
    );
}

// ---- the clock, section 8.2 ----------------------------------------------

/// A broadcast of a retained message on `topic`, stamped `millis` by
/// node one of a cluster, numbered `sequence` in its log.
///
/// Built by hand rather than through [`Node::retain`] because the point
/// is the stamp: a node's own clock reads the wall and cannot be told
/// to be ahead, and what these tests are about is a peer that is.
fn stamped_at(topic: &str, millis: u64, sequence: u64) -> Replication {
    Replication::Broadcast {
        entry: LoggedRecord {
            sequence,
            entry: MetadataRecord {
                key: MetadataKeyRecord::Retained {
                    topic: topic.to_owned(),
                },
                stamp: StampRecord {
                    millis,
                    counter: 0,
                    node: [1; 16],
                },
                value: Some(MetadataValueRecord::Retained(retained("ahead"))),
            },
        },
    }
}

/// A peer's stamp advances this node's clock, so that what this node
/// writes afterwards wins against what arrived.
///
/// Section 8.2 orders every register by its stamp, so a node whose clock
/// never learnt from its peers would lose every conflict to a node a
/// few seconds ahead — including the session-owner register, which is
/// double ownership. The peer here is half a minute ahead, well inside
/// the bound, and the local write that follows must beat it.
#[test]
fn a_peers_stamp_within_the_bound_advances_the_local_clock() {
    let mut cluster = Cluster::of(2);
    let ahead = crate::clock::wall_millis() + 30_000;
    let frame = stamped_at("a", ahead, 1);
    cluster.say(0, 1, frame);
    cluster.settle();
    assert_eq!(cluster.nodes[1].text("a").as_deref(), Some("ahead"));
    assert!(
        cluster.nodes[1].replicated().clock().high_water().millis() >= ahead,
        "the local clock did not take the peer's reading"
    );

    let frame = cluster.nodes[1].retain("a", "later, locally");
    assert_eq!(
        cluster.nodes[1].text("a").as_deref(),
        Some("later, locally"),
        "a write made after the peer's arrived lost to it"
    );
    cluster.say(1, 0, frame);
    cluster.settle();
    assert_eq!(
        cluster.nodes[0].text("a").as_deref(),
        Some("later, locally"),
        "the peer did not take the later write"
    );
}

/// A peer further ahead than the drift bound is refused, not clamped.
///
/// Section 8.2: clamping would still advance the local clock to the
/// peer's reading, and one node with a wrong year would then poison
/// every node it reached. The frame changes nothing — not the register,
/// not the clock, not the watermark — and the refusal names the numbers.
#[test]
fn a_peer_further_ahead_than_the_drift_bound_is_refused() {
    let cluster = Cluster::of(2);
    let before = cluster.nodes[1].replicated().clock().high_water();
    let far = crate::clock::wall_millis() + DEFAULT_MAX_DRIFT_MILLIS + 60_000;
    let error = cluster.nodes[1]
        .replicator
        .received(cluster.nodes[0].id(), stamped_at("a", far, 1))
        .expect_err("a peer a year ahead is refused");
    assert!(
        matches!(
            error,
            ReplicationError::Clock(crate::clock::ClockError::PeerTooFarAhead { .. })
        ),
        "refused as {error:?}"
    );
    assert_eq!(
        cluster.nodes[1].text("a"),
        None,
        "a refused frame was applied"
    );
    assert_eq!(
        cluster.nodes[1].replicated().clock().high_water(),
        before,
        "a refused frame moved the clock"
    );
    assert_eq!(
        cluster.nodes[1]
            .replicated()
            .seen_through(cluster.nodes[0].id()),
        0,
        "a refused frame moved the watermark"
    );
}

/// A batch with one entry beyond the bound is refused whole, before any
/// of it is merged.
#[test]
fn a_batch_with_one_entry_beyond_the_bound_is_refused_whole() {
    let cluster = Cluster::of(2);
    let wall = crate::clock::wall_millis();
    let frame = Replication::Batch {
        entries: vec![
            match stamped_at("a", wall, 1) {
                Replication::Broadcast { entry } => entry,
                _ => unreachable!(),
            },
            match stamped_at("b", wall + DEFAULT_MAX_DRIFT_MILLIS + 60_000, 2) {
                Replication::Broadcast { entry } => entry,
                _ => unreachable!(),
            },
        ],
        more: false,
    };
    cluster.nodes[1]
        .replicator
        .received(cluster.nodes[0].id(), frame)
        .expect_err("one entry beyond the bound refuses the batch");
    assert_eq!(cluster.nodes[1].text("a"), None, "half the batch was taken");
    assert_eq!(cluster.nodes[1].text("b"), None);
}
