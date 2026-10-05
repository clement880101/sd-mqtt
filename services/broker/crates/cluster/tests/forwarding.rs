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
            let mut broker = locked(&self.broker);
            let now = broker.clock().now();
            let mut out = Vec::new();
            broker.accept_forwarded(forwarded.into_message(), now, &mut out);
            out
        };
        self.record(actions);
        Vec::new()
    }

    fn member_changed(&self, peer: NodeId, state: MemberState) {
        match state {
            MemberState::Dead => self.replicated.routes().died(peer),
            MemberState::Alive => self.replicated.routes().revived(peer),
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
        it.nodes[0].replicated.routes().is_dead(it.nodes[2].me())
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
        !it.nodes[0].replicated.routes().is_dead(it.nodes[2].me())
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
