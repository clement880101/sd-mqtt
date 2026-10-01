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
    /// Returns how many frames were delivered.
    fn settle(&mut self) -> usize {
        let mut carried = 0;
        while !self.wire.is_empty() {
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
            }
        }
        carried
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
