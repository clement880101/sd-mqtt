//! Section 8.3, from a PUBLISH on one node to a delivery on another.
//!
//! Three brokers, three replicated states and three meshes over the
//! in-memory switch, in one process. What is proved here is what no unit
//! test of either half can be: a publication a client made on one node
//! comes out of another node's session, and the economy and the loop
//! prevention that make that affordable.
//!
//! # Why three and not two
//!
//! **Two nodes cannot tell a mesh from a line.** The rule that a
//! forwarded message is never forwarded again is invisible with two
//! nodes: A forwards to B, and B has nowhere to forward to that is not
//! A. With three, and a subscriber on the middle one, a build that
//! forwarded what it had been forwarded sends the message round the mesh
//! for ever — and the assertion that catches it is that the far node
//! received **one** copy rather than a stream of them.
//!
//! # What this harness is and is not
//!
//! It is the real mesh, the real frames, the real broker and the real
//! routing map. It is not the real shell: the registers are handed from
//! node to node directly rather than through `cluster::replicate`'s
//! broadcast and exchange, and the store is in memory. Section 8.2's own
//! tests cover the register getting there; what is under test here is
//! what a node does once it has one.
//!
//! The forwarded publication is delivered into the broker by a
//! [`Deliver`] of this file's own, which is what `sdmqtt::replication`
//! does in the binary: `Broker::accept_forwarded` under the broker's
//! lock, and the actions out afterwards. A test that called
//! `accept_forwarded` directly would be convention 5's last entry — a
//! guard that built its type by hand and never reached the caller
//! supposed to use it — because the thing being asserted is what comes
//! off a link.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use broker_core::broker::{Addressed, Broker, BrokerConfig};
use broker_core::hooks::{ClientInfo, Disposition, Hook};
use broker_core::session::message::Message as Publication;
use broker_core::session::Action;
use broker_core::subscriptions::{FilterEvent, SubscriberId};
use bytes::Bytes;
use cluster::membership::{Detection, MemberState};
use cluster::mesh::{Deliver, Mesh, MeshSettings, Message};
use cluster::{
    ClusterSettings, Forwarding, HybridClock, LinkConfig, MetadataWrite, NodeId, NodeIdentity,
    Replicated, DEFAULT_MAX_DRIFT_MILLIS,
};
use mqtt_codec::packet::{Connect, Publish, RetainHandling, Subscribe, SubscriptionOptions};
use mqtt_codec::{Packet, Properties, QoS, Version};
use storage::Secret;

/// Fast enough that a test is not a wait, slow enough that a loaded
/// machine does not declare a live peer dead.
const BEAT: Duration = Duration::from_millis(25);

/// How long to wait for something that should happen.
const PATIENCE: Duration = Duration::from_secs(10);

/// The secret every node here shares.
const SECRET: &str = "the-forwarding-secret";

// ---- one node ----------------------------------------------------------

/// Everything one node holds, as the binary holds it.
///
/// The broker behind a lock, the replicated state beside it, and what
/// each session was handed. The last of those is this file's stand-in
/// for a socket: `Shared::deliver` sends an action to the session's
/// outbox, and what a test wants to know is which topics came out of
/// which session.
struct Node {
    identity: NodeIdentity,
    broker: Mutex<Broker>,
    replicated: Arc<Replicated>,
    /// Every delivery any session on this node was handed, as topics.
    delivered: Mutex<Vec<(SubscriberId, String)>>,
    /// Every topic a rule on this node fired for.
    fired: Arc<Rules>,
}

/// A hook that writes down what it was asked about.
///
/// Section 9's rules, as far as this test needs them: what matters is
/// *where* the chain was asked, not what it did.
#[derive(Default)]
struct Rules {
    seen: Mutex<Vec<String>>,
}

impl Rules {
    fn topics(&self) -> Vec<String> {
        locked(&self.seen).clone()
    }
}

impl Hook for Rules {
    fn name(&self) -> &str {
        "the-rules"
    }

    fn message_received(&self, _client: &ClientInfo<'_>, message: &mut Publication) -> Disposition {
        locked(&self.seen).push(message.topic.to_string());
        Disposition::Deliver
    }
}

fn locked<T>(what: &Mutex<T>) -> MutexGuard<'_, T> {
    what.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Node {
    fn new(name: &str) -> Arc<Self> {
        let identity = NodeIdentity::mint(name);
        let fired = Arc::new(Rules::default());
        let mut broker = Broker::new(BrokerConfig::default());
        broker
            .hooks_mut()
            .install(Arc::clone(&fired) as Arc<dyn Hook>);
        Arc::new(Self {
            replicated: Arc::new(
                Replicated::open(
                    identity.id(),
                    HybridClock::resume(None, DEFAULT_MAX_DRIFT_MILLIS),
                    &storage::Restored::default(),
                )
                .expect("an empty store holds nothing to refuse"),
            ),
            identity,
            broker: Mutex::new(broker),
            delivered: Mutex::new(Vec::new()),
            fired,
        })
    }

    fn me(&self) -> NodeId {
        self.identity.id()
    }

    /// Start a mesh on `switch`, forwarding what this node publishes.
    ///
    /// The two halves of section 8.3 at once, as the binary does it: the
    /// mesh is given what to do with an arriving frame, and the broker
    /// is given what to do with a publication that entered here.
    fn join(
        self: &Arc<Self>,
        switch: &Arc<cluster::testing::memory::Switch>,
        seeds: &[&str],
    ) -> Arc<Mesh> {
        let transport = Arc::new(switch.listen(self.identity.name()));
        let settings = MeshSettings {
            advertise: self.identity.name().to_owned(),
            seeds: seeds.iter().map(|seed| (*seed).to_owned()).collect(),
            detection: Detection {
                heartbeat: BEAT,
                suspect_after: 3,
                dead_after: BEAT * 8,
            },
            reconnect_min: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(60),
            refused_backoff: Duration::from_millis(200),
            link: LinkConfig::default(),
        };
        let config = ClusterSettings {
            name: "production".to_owned(),
            secret: Some(Secret::new(SECRET)),
        }
        .check()
        .expect("a usable configuration");
        let mesh = Mesh::start_delivering(
            self.identity.clone(),
            config,
            settings,
            transport,
            Some(Arc::clone(self) as Arc<dyn Deliver>),
        );
        locked(&self.broker).install_forwarder(Arc::new(Forwarding::new(
            Arc::clone(&self.replicated),
            Arc::clone(&mesh),
        )));
        mesh
    }

    /// Join a shared group, and return the writes to tell the cluster
    /// about.
    ///
    /// Through a SUBSCRIBE packet carrying `$share/<group>/<filter>`,
    /// because the whole question is what the subscribe path reports and
    /// what the cluster is told about it. A test that put the group into
    /// the map by hand would be answering its own question.
    fn join_group(
        &self,
        client: &str,
        group: &str,
        filter: &str,
    ) -> (SubscriberId, Vec<MetadataWrite>) {
        self.subscribe(client, &format!("$share/{group}/{filter}"))
    }

    /// Connect a client, subscribe it, and return the writes to tell the
    /// cluster about.
    fn subscribe(&self, client: &str, filter: &str) -> (SubscriberId, Vec<MetadataWrite>) {
        let who = self.connect(client);
        let packet = Packet::Subscribe(Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: filter.to_owned(),
                qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAlways,
            }],
            properties: Properties::default(),
        });
        let mut out = Vec::new();
        locked(&self.broker)
            .handle(who, &packet, &mut out)
            .expect("the subscribe is accepted");
        (who, self.settle())
    }

    fn connect(&self, client: &str) -> SubscriberId {
        let connect = Connect {
            version: Version::V5,
            client_id: client.to_owned(),
            clean_start: false,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties {
                session_expiry_interval: Some(3_600),
                ..Properties::default()
            },
        };
        let mut out = Vec::new();
        locked(&self.broker)
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates")
            .subscriber
    }

    /// Turn the broker's filter events into registers, as a shell does.
    fn settle(&self) -> Vec<MetadataWrite> {
        let mut events = Vec::new();
        locked(&self.broker).take_filter_events(&mut events);
        let mut writes = Vec::new();
        for event in events {
            let write = match &event {
                FilterEvent::Arrived(filter) => self.replicated.subscriber_arrived(filter),
                FilterEvent::Departed(filter) => self.replicated.subscriber_departed(filter),
                FilterEvent::GroupJoined { group, filter } => {
                    self.replicated.group_joined(group, filter)
                }
                FilterEvent::GroupLeft { group, filter } => {
                    self.replicated.group_left(group, filter)
                }
            };
            if let Some(write) = write {
                writes.push(write);
            }
        }
        writes
    }

    /// Take a register a peer wrote.
    fn observed(&self, writes: &[MetadataWrite]) {
        for write in writes {
            self.replicated.observed(write.clone());
        }
    }

    /// Publish as a client of this node would, through the packet path.
    ///
    /// Through `Broker::handle` rather than `Broker::publish`, because
    /// the thing being asserted is what a client's publication does and
    /// the hooks that a rule hangs off are fired on that path.
    fn publish(&self, who: SubscriberId, topic: &str, payload: &'static [u8]) {
        let packet = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.to_owned(),
            packet_id: None,
            payload: Bytes::from_static(payload),
            properties: Properties::default(),
        });
        let mut out = Vec::new();
        locked(&self.broker)
            .handle(who, &packet, &mut out)
            .expect("the publish is accepted");
        self.record(out);
    }

    /// Publish, and say how many subscribers and nodes it reached.
    ///
    /// What becomes the acknowledgement's reason code: zero is
    /// [MQTT-3.3.5]'s "no matching subscribers".
    fn reached(&self, who: SubscriberId, topic: &str, payload: &'static [u8]) -> usize {
        let message = Publication::new(topic, Bytes::from_static(payload), QoS::AtMostOnce, false);
        let mut out = Vec::new();
        let reached = {
            let mut broker = locked(&self.broker);
            let now = broker.clock().now();
            broker.publish(message, Some(who), now, &mut out)
        };
        self.record(out);
        reached
    }

    /// Write down what the broker addressed to each session.
    fn record(&self, actions: Vec<Addressed>) {
        let mut delivered = locked(&self.delivered);
        for (who, action) in actions {
            if let Action::Send(Packet::Publish(publish)) = action {
                delivered.push((who, publish.topic));
            }
        }
    }

    /// Every topic `who` was handed.
    fn topics(&self, who: SubscriberId) -> Vec<String> {
        locked(&self.delivered)
            .iter()
            .filter(|(session, _)| *session == who)
            .map(|(_, topic)| topic.clone())
            .collect()
    }

    /// How many deliveries this node has made in total.
    fn deliveries(&self) -> usize {
        locked(&self.delivered).len()
    }

    /// Section 11.3's router matches, as this node counts them.
    fn matches(&self) -> broker_core::broker::RouterMatches {
        locked(&self.broker).router_matches()
    }

    /// How many retained messages this node holds.
    fn retained(&self) -> usize {
        locked(&self.broker).retained_count()
    }

    /// Publish with the retain flag set, as a client would.
    fn publish_retained(&self, who: SubscriberId, topic: &str, payload: &'static [u8]) {
        let packet = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: topic.to_owned(),
            packet_id: None,
            payload: Bytes::from_static(payload),
            properties: Properties::default(),
        });
        let mut out = Vec::new();
        locked(&self.broker)
            .handle(who, &packet, &mut out)
            .expect("the publish is accepted");
        self.record(out);
    }
}

/// What a peer's frame means here, which is `sdmqtt::replication`'s job
/// in the binary and this file's here.
impl Deliver for Node {
    fn delivered(&self, _from: NodeId, message: Message) -> Vec<Message> {
        let Message::Forward(forwarded) = message else {
            // A heartbeat. Nothing here replicates; section 8.2's own
            // tests do that.
            return Vec::new();
        };
        let actions = {
            let groups = forwarded.groups.clone();
            let mut broker = locked(&self.broker);
            let now = broker.clock().now();
            let mut out = Vec::new();
            broker.accept_forwarded(forwarded.into_message(), &groups, now, &mut out);
            out
        };
        self.record(actions);
        Vec::new()
    }

    fn member_changed(&self, peer: NodeId, state: MemberState) {
        match state {
            MemberState::Dead => self.replicated.died(peer),
            MemberState::Alive => self.replicated.revived(peer),
            MemberState::Suspect => {}
        }
    }
}

// ---- three of them -----------------------------------------------------

/// Three nodes on one switch, meshed, each holding every register.
struct Cluster {
    switch: Arc<cluster::testing::memory::Switch>,
    nodes: Vec<Arc<Node>>,
    meshes: Vec<Arc<Mesh>>,
}

impl Cluster {
    /// Three nodes, each seeded with the ones after it.
    ///
    /// **Only one end of each pair dials, deliberately.** Seeding every
    /// node with every other makes every pair produce two connections,
    /// and the rule in section 8.1 resolves that by dropping one —
    /// which aborts that link's writer and loses whatever was queued
    /// for it. `crate::mesh` says so in as many words. That is a real
    /// property of the mesh and a startup-only one, and it is not what
    /// this file is about: a publication made while a pair is still
    /// settling would make every assertion here a race. So the seeds
    /// make each pair's link unambiguous, and a single link is never
    /// dropped for having the wrong role.
    async fn start(names: [&str; 3]) -> Self {
        let switch = cluster::testing::memory::Switch::new();
        let nodes: Vec<Arc<Node>> = names.iter().map(|name| Node::new(name)).collect();
        let meshes: Vec<Arc<Mesh>> = (0..3)
            .map(|at| {
                let seeds: Vec<&str> = names[at + 1..].to_vec();
                nodes[at].join(&switch, &seeds)
            })
            .collect();
        let it = Self {
            switch,
            nodes,
            meshes,
        };
        it.linked().await;
        it
    }

    /// Wait until every node has a link to the other two.
    async fn linked(&self) {
        until("every node has two links", || {
            self.meshes.iter().all(|mesh| mesh.linked().len() == 2)
        })
        .await;
    }

    /// Tell every node about `writes`, except the node that wrote them.
    ///
    /// What section 8.2's broadcast does. Direct, because what is under
    /// test is the routing and not the replication.
    fn announce(&self, from: usize, writes: &[MetadataWrite]) {
        for (at, node) in self.nodes.iter().enumerate() {
            if at != from {
                node.observed(writes);
            }
        }
    }
}

async fn until(what: &str, mut holds: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while tokio::time::Instant::now() < deadline {
        if holds() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("{what} never happened within {PATIENCE:?}");
}

/// Assert something stays true for several beats.
///
/// Convention 5's "a test asserting an absence": a build that forwarded
/// what it had been forwarded would pass an assertion made once and fail
/// this one.
async fn keeps(what: &str, mut holds: impl FnMut() -> bool) {
    for _ in 0..40 {
        assert!(holds(), "{what} stopped being true");
        tokio::time::sleep(BEAT / 4).await;
    }
}

// ---- the criteria ------------------------------------------------------

/// A publication on one node reaches a subscriber on another.
///
/// The sentence milestone 5 exists for, and the one nothing before this
/// plan could make true.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publication_on_one_node_reaches_a_subscriber_on_another() {
    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    let (listener, writes) = it.nodes[2].subscribe("listener", "site/+/temp");
    it.announce(2, &writes);

    let publisher = it.nodes[0].connect("publisher");
    it.nodes[0].publish(publisher, "site/1/temp", b"21.5");

    until("the message crosses the cluster", || {
        !it.nodes[2].topics(listener).is_empty()
    })
    .await;
    assert_eq!(it.nodes[2].topics(listener), vec!["site/1/temp".to_owned()]);

    // Section 11.3's split, both halves: one copy left alpha and one
    // delivery happened on gamma.
    assert_eq!(it.nodes[0].matches().remote, 1);
    assert_eq!(it.nodes[0].matches().local, 0, "alpha has no subscriber");
    assert_eq!(it.nodes[2].matches().local, 1);
    assert_eq!(
        it.nodes[2].matches().remote,
        0,
        "a forwarded message is not forwarded on, so it counts no remote match"
    );
}

/// A node with no subscriber for a topic is not forwarded to at all.
///
/// **The economy the design is built on, and what a delivery test cannot
/// see.** Fan-out costs one frame per interested node rather than one
/// per node, so a build that broadcast every publication to every peer
/// and let the far end discard it would pass the test above and fail
/// this one. Asserted on what the link carried, because that is the cost
/// in question: a frame that was sent is bytes on a socket whatever the
/// far end did with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_with_no_subscriber_is_not_forwarded_to_at_all() {
    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    let (listener, writes) = it.nodes[2].subscribe("listener", "site/+/temp");
    it.announce(2, &writes);

    // What beta has read before anything is published, so that the
    // heartbeats it goes on receiving are not mistaken for a forward.
    let quiet_before = it.meshes[1].stats().bytes_received;

    let publisher = it.nodes[0].connect("publisher");
    it.nodes[0].publish(publisher, "site/1/temp", b"21.5");
    until("gamma has it", || !it.nodes[2].topics(listener).is_empty()).await;

    assert_eq!(
        it.nodes[0].matches().remote,
        1,
        "one copy for one interested node, not one per peer"
    );
    assert_eq!(
        it.nodes[1].deliveries(),
        0,
        "beta has no subscriber and must have been given nothing to deliver"
    );
    // One frame per interested node and no more: beta's traffic over the
    // whole of this is heartbeats, which are far smaller than the
    // publication. A broadcast would have put the payload on this link.
    let since = it.meshes[1].stats().bytes_received - quiet_before;
    assert!(
        since < 64,
        "beta read {since} bytes while a publication it has no subscriber for was \
         forwarded to gamma; the frame was broadcast rather than routed"
    );
}

/// A forwarded message is not forwarded again.
///
/// **Three nodes with the subscriber in the middle**, because two cannot
/// tell a mesh from a line. Alpha publishes, beta has the subscriber,
/// and gamma has nothing. A build that forwarded what it had been
/// forwarded would send the message from beta to alpha and gamma, from
/// those to each other, and round for ever — so what is asserted is that
/// beta's one subscriber was handed exactly one copy and goes on having
/// exactly one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarded_message_is_not_forwarded_again() {
    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    // Every node has a subscriber for the topic, which is what makes a
    // loop reachable at all: a node with nothing matching would not
    // forward even on a build with no mark.
    let (first, writes) = it.nodes[1].subscribe("in-the-middle", "loop/+");
    it.announce(1, &writes);
    let (second, writes) = it.nodes[2].subscribe("at-the-end", "loop/+");
    it.announce(2, &writes);
    let (third, writes) = it.nodes[0].subscribe("at-the-start", "loop/+");
    it.announce(0, &writes);

    let publisher = it.nodes[0].connect("publisher");
    it.nodes[0].publish(publisher, "loop/once", b"only once");

    until("both remote subscribers have it", || {
        !it.nodes[1].topics(first).is_empty() && !it.nodes[2].topics(second).is_empty()
    })
    .await;

    keeps("every subscriber has exactly one copy", || {
        it.nodes[1].topics(first).len() == 1
            && it.nodes[2].topics(second).len() == 1
            && it.nodes[0].topics(third).len() == 1
    })
    .await;

    // And the count says the same thing from the other side: alpha
    // forwarded twice, once per interested peer, and the two nodes that
    // received a forward forwarded nothing.
    assert_eq!(it.nodes[0].matches().remote, 2);
    assert_eq!(
        (it.nodes[1].matches().remote, it.nodes[2].matches().remote),
        (0, 0),
        "a node that received a forward sent one on"
    );
}

/// A rule fires where the message entered the cluster, and nowhere else.
///
/// Section 8.3: the mark answers section 9's deferred question about a
/// rule firing exactly once. A rule fires on the node that received the
/// message unmarked, which is the node a client published to — not on
/// every node that happened to have a subscriber, which would post one
/// publication to a webhook once per node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rule_fires_where_the_message_entered_and_not_where_it_arrived() {
    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    let (listener, writes) = it.nodes[1].subscribe("listener", "rules/+");
    it.announce(1, &writes);

    let publisher = it.nodes[0].connect("publisher");
    it.nodes[0].publish(publisher, "rules/fire", b"once");

    until("beta has the message", || {
        !it.nodes[1].topics(listener).is_empty()
    })
    .await;

    assert_eq!(
        it.nodes[0].fired.topics(),
        vec!["rules/fire".to_owned()],
        "the rule did not fire on the node the client published to"
    );
    keeps(
        "and it never fires on the node that received the forward",
        || it.nodes[1].fired.topics().is_empty() && it.nodes[2].fired.topics().is_empty(),
    )
    .await;
}

/// A node this one has judged dead stops being forwarded to.
///
/// Membership is not replicated, so nothing is written down when a node
/// dies: the register saying gamma wants the topic is still held by
/// alpha, and alpha must stop sending to it anyway. The cut is one-way
/// and over an open socket, which is the case section 8.1 built the
/// heartbeat for and the one a closed socket cannot express.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_node_stops_being_forwarded_to() {
    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    let (listener, writes) = it.nodes[2].subscribe("listener", "dead/+");
    it.announce(2, &writes);
    let publisher = it.nodes[0].connect("publisher");

    it.nodes[0].publish(publisher, "dead/before", b"while it was alive");
    until("gamma has the first one", || {
        it.nodes[2].topics(listener).len() == 1
    })
    .await;

    // Nothing gamma says reaches alpha, and the socket stays open: alpha
    // declares it dead on silence rather than on a close.
    it.switch.cut("gamma", "alpha");
    until("alpha has declared gamma dead", || {
        it.nodes[0].replicated.liveness().is_dead(it.nodes[2].me())
    })
    .await;
    assert_eq!(
        it.nodes[0].replicated.nodes_subscribed("dead/+"),
        vec![it.nodes[2].me()],
        "the register is the truth and is kept; only the route is withheld"
    );

    let forwarded = it.nodes[0].matches().remote;
    it.nodes[0].publish(publisher, "dead/after", b"and after");
    keeps("nothing is forwarded to a node believed dead", || {
        it.nodes[0].matches().remote == forwarded
    })
    .await;
    assert_eq!(
        it.nodes[2].topics(listener),
        vec!["dead/before".to_owned()],
        "gamma was sent a publication after alpha had declared it dead"
    );

    // And it heals on the connection it already had, without anything
    // being re-announced: `Routes` withholds rather than forgets.
    it.switch.heal("gamma", "alpha");
    until("alpha routes to gamma again", || {
        !it.nodes[0].replicated.liveness().is_dead(it.nodes[2].me())
    })
    .await;
    it.nodes[0].publish(publisher, "dead/healed", b"again");
    until("and gamma receives again", || {
        it.nodes[2].topics(listener).len() == 2
    })
    .await;
    assert_eq!(
        it.nodes[2].topics(listener),
        vec!["dead/before".to_owned(), "dead/healed".to_owned()]
    );
}

/// A forwarded retained publication is delivered and not retained.
///
/// **Retention travels as a register, not as a forward.** Section 8.2
/// replicates retained messages by last writer wins, stamped by the node
/// that wrote one. A node that retained what it was forwarded would
/// stamp the same value again, and two nodes doing that to each other is
/// one retained message bouncing between them for ever — the loop
/// `Shared::apply_replicated` already exists to prevent on the other
/// path.
///
/// The flag itself still travels, because [MQTT-3.3.1-13] is about the
/// delivery rather than about the store, and a subscription with
/// retain-as-published set is entitled to see it. Nothing here sets that
/// option, so what is asserted is that the message arrived and that the
/// receiving node's retained store stayed empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_forwarded_retained_publication_is_delivered_and_not_retained() {
    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    let (listener, writes) = it.nodes[2].subscribe("listener", "kept/+");
    it.announce(2, &writes);

    let publisher = it.nodes[0].connect("publisher");
    it.nodes[0].publish_retained(publisher, "kept/one", b"on alpha");

    until("gamma has the message", || {
        !it.nodes[2].topics(listener).is_empty()
    })
    .await;
    assert_eq!(
        it.nodes[0].retained(),
        1,
        "the node the client published to keeps it, and replicates the register"
    );
    keeps(
        "and the node that received the forward keeps nothing",
        || it.nodes[2].retained() == 0,
    )
    .await;
}

/// A shared group spanning three nodes receives one message, not three.
///
/// **The trap the whole plan is arranged around.** A group with a member
/// on each of three nodes has a matching subscriber on each, so the
/// routing map says all three want the topic — and forwarding one copy
/// to each interested node delivers three times where the semantics
/// demand one. Section 6: a shared subscription balances across the
/// cluster, and section 8.3: "shared subscriptions need their own map
/// and cannot use this one".
///
/// Counted across all three nodes, because which node delivers is
/// exactly what is not being asserted: what the group is owed is one
/// delivery somewhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shared_group_spanning_three_nodes_is_delivered_once() {
    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    for at in 0..3 {
        let (_, writes) = it.nodes[at].join_group(&format!("worker-{at}"), "workers", "jobs/+");
        it.announce(at, &writes);
    }

    // And a second group with a member on the two *other* nodes only,
    // so that both halves of the choice are under test: a publishing
    // node that has a member of its own, and one that does not. The
    // first is the pre-fix triple delivery; the second is what catches a
    // build that files a group as an ordinary subscription and then
    // refuses to serve it on arrival, which delivers nothing at all.
    for at in [1, 2] {
        let (_, writes) = it.nodes[at].join_group(&format!("remote-{at}"), "remotes", "work/+");
        it.announce(at, &writes);
    }

    let publisher = it.nodes[0].connect("publisher");
    it.nodes[0].publish(publisher, "jobs/one", b"do it once");

    // Held rather than read once: the local delivery is made before
    // `publish` returns and a forward arrives a beat later, so an
    // assertion made immediately would read one and pass against a
    // build that was about to deliver two more.
    keeps("exactly one member of the group is delivered to", || {
        let delivered: usize = (0..3).map(|at| it.nodes[at].deliveries()).sum();
        delivered == 1
    })
    .await;

    it.nodes[0].publish(publisher, "work/one", b"and this one too");
    until("the group with no local member is delivered to", || {
        let delivered: usize = (0..3).map(|at| it.nodes[at].deliveries()).sum();
        delivered == 2
    })
    .await;
    keeps(
        "exactly once, on one of the two nodes that have a member",
        || {
            let delivered: usize = (0..3).map(|at| it.nodes[at].deliveries()).sum();
            delivered == 2 && it.nodes[0].deliveries() == 1
        },
    )
    .await;
}

/// Over many publications every member node is chosen.
///
/// **What makes it a shared subscription rather than a fixed route.** A
/// cluster that always chose the same node would pass the test above and
/// would have balanced nothing: every message to the group would go to
/// one node's members and the others would idle.
///
/// Published from alpha, which has no member of its own, because a node
/// that has one serves the group itself — that is section 6's local
/// preference and it is the test below.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_member_node_is_chosen_over_many_publications() {
    const PUBLICATIONS: usize = 20;

    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    for at in [1, 2] {
        let (_, writes) = it.nodes[at].join_group(&format!("worker-{at}"), "workers", "jobs/+");
        it.announce(at, &writes);
    }

    let publisher = it.nodes[0].connect("publisher");
    for _ in 0..PUBLICATIONS {
        it.nodes[0].publish(publisher, "jobs/one", b"one of many");
    }

    until("every publication has been delivered once", || {
        it.nodes[1].deliveries() + it.nodes[2].deliveries() == PUBLICATIONS
    })
    .await;
    keeps("and not more than once", || {
        it.nodes[1].deliveries() + it.nodes[2].deliveries() == PUBLICATIONS
    })
    .await;
    assert!(
        it.nodes[1].deliveries() > 0 && it.nodes[2].deliveries() > 0,
        "one member node took everything: beta {} and gamma {}",
        it.nodes[1].deliveries(),
        it.nodes[2].deliveries()
    );
    assert_eq!(
        it.nodes[0].deliveries(),
        0,
        "alpha has no member and must have delivered nothing"
    );
}

/// A local member is preferred, which saves a hop and a network copy.
///
/// Section 6: selection "prefers a local online member". Every node has
/// a member here, so a cluster that rotated without looking would send
/// two publications out of three over a link for nothing — and would
/// also make the delivery arrive later than it had to.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_local_member_of_a_group_is_preferred() {
    const PUBLICATIONS: usize = 10;

    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    for at in 0..3 {
        let (_, writes) = it.nodes[at].join_group(&format!("worker-{at}"), "workers", "jobs/+");
        it.announce(at, &writes);
    }

    let publisher = it.nodes[0].connect("publisher");
    for _ in 0..PUBLICATIONS {
        it.nodes[0].publish(publisher, "jobs/one", b"stay at home");
    }

    assert_eq!(
        it.nodes[0].deliveries(),
        PUBLICATIONS,
        "every publication should have been served by alpha's own member"
    );
    keeps("and nothing left alpha for the group", || {
        it.nodes[1].deliveries() == 0 && it.nodes[2].deliveries() == 0
    })
    .await;
    assert_eq!(
        it.nodes[0].matches().remote,
        0,
        "a group served locally is not forwarded anywhere"
    );
}

/// An ordinary subscription and a shared one both deliver.
///
/// **The case the group map can easily swallow.** They are independent
/// mechanisms rather than alternatives: a client subscribed to `jobs/+`
/// is owed every publication, and the group is owed one. A node chosen
/// for neither must get nothing, and a node that is both on the ordinary
/// route and chosen for the group must get **one** copy and make two
/// deliveries from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_ordinary_subscription_and_a_shared_one_both_deliver() {
    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    // Beta holds both: an ordinary subscriber and the group's only
    // member. One copy reaches it, and both are owed a delivery.
    let (watching, writes) = it.nodes[1].subscribe("watcher", "jobs/+");
    it.announce(1, &writes);
    let (worker, writes) = it.nodes[1].join_group("worker", "workers", "jobs/+");
    it.announce(1, &writes);

    let publisher = it.nodes[0].connect("publisher");
    it.nodes[0].publish(publisher, "jobs/one", b"for both");

    until("both of beta's subscribers have it", || {
        !it.nodes[1].topics(watching).is_empty() && !it.nodes[1].topics(worker).is_empty()
    })
    .await;
    assert_eq!(
        it.nodes[0].matches().remote,
        1,
        "one copy to beta, not one for the subscriber and one for the group"
    );
    keeps(
        "and exactly two deliveries, on beta and nowhere else",
        || it.nodes[1].deliveries() == 2 && it.nodes[2].deliveries() == 0,
    )
    .await;
}

/// A node with a group member it was not chosen for delivers nothing.
///
/// **The triple delivery, from the other side.** Beta is on the ordinary
/// route because it has a subscriber for the topic, so it is forwarded a
/// copy whatever happens to the group. If that copy were delivered to
/// every group beta has a member of, beta would serve a group gamma had
/// been chosen for and the group would be delivered to twice.
///
/// So this is the test that the frame's list of groups is read: beta's
/// ordinary subscriber receives, beta's group member does not, and the
/// group is served once on gamma.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_another_node_was_chosen_for_is_not_served_here_as_well() {
    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    // Alpha has a member too, so it serves the group itself — section
    // 6's local preference — and beta is forwarded a copy only for its
    // ordinary subscriber.
    let (_, writes) = it.nodes[0].join_group("worker-alpha", "workers", "jobs/+");
    it.announce(0, &writes);
    let (watching, writes) = it.nodes[1].subscribe("watcher", "jobs/+");
    it.announce(1, &writes);
    let (worker, writes) = it.nodes[1].join_group("worker-beta", "workers", "jobs/+");
    it.announce(1, &writes);

    let publisher = it.nodes[0].connect("publisher");
    it.nodes[0].publish(publisher, "jobs/one", b"once for the group");

    until("beta's ordinary subscriber has it", || {
        !it.nodes[1].topics(watching).is_empty()
    })
    .await;
    keeps("and beta's group member never does", || {
        it.nodes[1].topics(worker).is_empty()
    })
    .await;
    assert_eq!(
        it.nodes[0].deliveries(),
        1,
        "alpha's own member served the group, once"
    );
    assert_eq!(
        it.nodes[1].deliveries(),
        1,
        "beta delivered to its ordinary subscriber and to nobody else"
    );
}

/// A publisher is not told nobody is listening when another node is.
///
/// **The one thing the delivery count decides.** `publish` answers how
/// many subscribers it reached, and an answer of zero becomes
/// [MQTT-3.3.5] "no matching subscribers" in the acknowledgement. On a
/// cluster the local count alone is the wrong answer: a client would be
/// told nobody is listening while another node's client received the
/// message, and the answer would depend on which node the publisher
/// happened to connect to — which is exactly what section 8 exists to
/// stop being visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_publication_forwarded_to_another_node_reached_somebody() {
    let it = Cluster::start(["alpha", "beta", "gamma"]).await;
    let (listener, writes) = it.nodes[2].subscribe("listener", "counted/+");
    it.announce(2, &writes);

    let publisher = it.nodes[0].connect("publisher");
    assert_eq!(
        it.nodes[0].reached(publisher, "counted/one", b"somebody wants this"),
        1,
        "a publication forwarded to a node that wants it reached somebody"
    );
    assert_eq!(
        it.nodes[0].reached(publisher, "wanted/by/nobody", b"and this one does not"),
        0,
        "and one nobody wants still reaches nobody"
    );
    until("the forwarded one arrives", || {
        !it.nodes[2].topics(listener).is_empty()
    })
    .await;
}
