//! Section 8.2's node granularity, from a SUBSCRIBE packet to a register.
//!
//! The two edges the plan names are each a claim about two crates at
//! once: the broker has to report one arrival per session and filter on
//! every path, and the count here has to write a register only when that
//! report leaves or reaches zero. A test of either half alone passes on
//! a build where the other is missing, which is convention 5's last
//! entry — "a test that constructs the type instead of walking the path".
//!
//! So this walks the whole thing. Clients connect, subscribe and
//! unsubscribe through packets; every filter event the broker produces is
//! fed into a [`Replicated`]; and what is asserted is the registers that
//! come out and the records that go to the store.
//!
//! # What this is not
//!
//! Nothing here sends a frame. Broadcasting the writes and repairing what
//! a broadcast lost are later tasks; what is being proved is **which
//! writes there are**, which is the decision those tasks carry.

use std::sync::Arc;

use broker_core::broker::{Broker, BrokerConfig};
use broker_core::subscriptions::{FilterEvent, SubscriberId};
use cluster::metadata::{MetadataKey, MetadataValue, MetadataWrite};
use cluster::{HybridClock, NodeId, Replicated, DEFAULT_MAX_DRIFT_MILLIS};
use mqtt_codec::packet::{Connect, RetainHandling, Subscribe, SubscriptionOptions, Unsubscribe};
use mqtt_codec::{Properties, QoS, Version};
use storage::{MemoryStorage, Record, Storage};

/// One node: a broker, and the replicated state beside it.
///
/// The pairing a shell will make. Nothing else here knows how the two
/// are joined, which is the point: [`Node::settle`] is the whole of it,
/// and it is four lines.
struct Node {
    broker: Broker,
    replicated: Replicated,
    store: Arc<MemoryStorage>,
    /// Every write the node decided to make, in order. What a broadcast
    /// would carry.
    written: Vec<MetadataWrite>,
}

impl Node {
    fn new(me: NodeId) -> Self {
        Self {
            broker: Broker::new(BrokerConfig::default()),
            replicated: Replicated::open(
                me,
                HybridClock::resume(None, DEFAULT_MAX_DRIFT_MILLIS),
                &[],
            )
            .expect("an empty store holds nothing to refuse"),
            store: Arc::new(MemoryStorage::new()),
            written: Vec::new(),
        }
    }

    /// Drain everything the broker has said and write it down.
    ///
    /// **The one place the two halves meet**, and what a shell does: the
    /// journal and the filter events are taken together, the events
    /// become registers, and all of it goes to the store in one batch —
    /// which is what makes the subscriptions and the registers that
    /// describe them survive or not survive together.
    ///
    /// The events are handled in the order the broker produced them,
    /// because a filter gained and lost in one drain must be counted in
    /// that order: reversed, the node ends up advertising a filter it has
    /// just given up.
    fn settle(&mut self) -> Vec<MetadataWrite> {
        let mut records = Vec::new();
        self.broker.take_journal(&mut records);
        let mut events = Vec::new();
        self.broker.take_filter_events(&mut events);

        let mut writes = Vec::new();
        for event in events {
            let write = match &event {
                FilterEvent::Arrived(filter) => self.replicated.subscriber_arrived(filter),
                FilterEvent::Departed(filter) => self.replicated.subscriber_departed(filter),
            };
            if let Some(write) = write {
                // The register and the clock mark beside it, which is
                // what section 8.2's resume depends on.
                records.extend(self.replicated.records(&write));
                writes.push(write.clone());
                self.written.push(write);
            }
        }
        if !records.is_empty() {
            self.store
                .apply(&records)
                .expect("a store must accept what the broker said");
        }
        writes
    }

    /// Connect a client whose session outlives its connection.
    fn connect(&mut self, client: &str) -> SubscriberId {
        self.connect_with(client, false)
    }

    fn connect_with(&mut self, client: &str, clean_start: bool) -> SubscriberId {
        let connect = Connect {
            version: Version::V5,
            client_id: client.to_owned(),
            clean_start,
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
        self.broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates")
            .subscriber
    }

    fn subscribe(&mut self, who: SubscriberId, filter: &str) {
        let packet = mqtt_codec::Packet::Subscribe(Subscribe {
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
        self.broker
            .handle(who, &packet, &mut out)
            .expect("the subscribe is accepted");
    }

    fn unsubscribe(&mut self, who: SubscriberId, filter: &str) {
        let packet = mqtt_codec::Packet::Unsubscribe(Unsubscribe {
            packet_id: 2,
            filters: vec![filter.to_owned()],
            properties: Properties::default(),
        });
        let mut out = Vec::new();
        self.broker
            .handle(who, &packet, &mut out)
            .expect("the unsubscribe is accepted");
    }

    /// The key this node's register for `filter` is filed under.
    fn key(&self, filter: &str) -> MetadataKey {
        MetadataKey::NodeSubscription {
            node: self.replicated.me(),
            filter: filter.to_owned(),
        }
    }

    /// Every replicated register the store holds, in key order.
    fn stored(&self) -> Vec<storage::MetadataRecord> {
        self.store.restore().expect("a restore").metadata
    }
}

fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index; 16])
}

/// **A second subscriber to a filter broadcasts nothing.**
///
/// Section 8.2's first edge, walked from the packet. One register for the
/// filter, however many of this node's clients want it: a register per
/// client subscription is fifty nodes holding a hundred thousand each at
/// the design ceiling, against a thirty megabyte idle budget.
#[test]
fn a_second_subscriber_to_a_filter_broadcasts_nothing() {
    let mut it = Node::new(node(1));
    let first = it.connect("first");
    let second = it.connect("second");
    let third = it.connect("third");

    it.subscribe(first, "sensor/+/temp");
    let writes = it.settle();
    assert_eq!(
        writes.len(),
        1,
        "the first subscriber must write a register"
    );
    assert_eq!(writes[0].key(), &it.key("sensor/+/temp"));
    assert_eq!(
        writes[0].register().value(),
        Some(&MetadataValue::NodeSubscription)
    );

    it.subscribe(second, "sensor/+/temp");
    assert_eq!(
        it.settle(),
        Vec::new(),
        "a second subscriber to a filter broadcast something"
    );
    it.subscribe(third, "sensor/+/temp");
    assert_eq!(
        it.settle(),
        Vec::new(),
        "a third subscriber to a filter broadcast something"
    );

    assert_eq!(
        it.written.len(),
        1,
        "three clients on one filter produced {} writes, which is a register per \
         client subscription",
        it.written.len()
    );
    assert_eq!(
        it.stored().len(),
        1,
        "the store holds a register per client"
    );
    assert_eq!(it.replicated.local_subscribers("sensor/+/temp"), 3);
    assert_eq!(
        it.replicated.nodes_subscribed("sensor/+/temp"),
        vec![it.replicated.me()]
    );
}

/// **The last unsubscribe from a filter broadcasts a tombstone.**
///
/// Section 8.2's second edge, and the one that cannot be nothing: every
/// peer was told this node has a subscriber, and a register silently
/// dropped leaves them routing here for ever. The departures before the
/// last one broadcast nothing, which is the same rule as the arrivals.
#[test]
fn the_last_unsubscribe_from_a_filter_broadcasts_a_tombstone() {
    let mut it = Node::new(node(1));
    let first = it.connect("first");
    let second = it.connect("second");
    it.subscribe(first, "a/b");
    it.subscribe(second, "a/b");
    let written = it.settle();
    assert_eq!(written.len(), 1);

    it.unsubscribe(first, "a/b");
    assert_eq!(
        it.settle(),
        Vec::new(),
        "an unsubscribe that left a subscriber behind broadcast something"
    );
    assert_eq!(
        it.replicated.nodes_subscribed("a/b"),
        vec![it.replicated.me()],
        "the node stopped advertising a filter it still has a subscriber for"
    );

    it.unsubscribe(second, "a/b");
    let last = it.settle();
    assert_eq!(
        last.len(),
        1,
        "the last unsubscribe from a filter broadcast nothing"
    );
    assert!(
        last[0].is_tombstone(),
        "the last unsubscribe broadcast a value rather than a tombstone"
    );
    assert_eq!(last[0].key(), &it.key("a/b"));
    assert!(
        last[0].stamp() > written[0].stamp(),
        "the tombstone does not beat the write it has to supersede"
    );
    assert_eq!(it.replicated.nodes_subscribed("a/b"), Vec::new());

    // And the store holds the tombstone rather than nothing: a node that
    // dropped the row would be handed its own older copy by the next
    // peer it spoke to and would advertise the filter again.
    let stored = it.stored();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].value, None);
}

/// A session expiring tombstones the filters it was the last holder of.
///
/// Nobody unsubscribed from anything. The departure comes from the
/// broker's own tick, and without it the node advertises a filter no
/// client is subscribed to until the process restarts — and a restart
/// does not repair it either, because the subscription went from the
/// store with the session.
#[test]
fn a_session_expiring_tombstones_what_it_was_the_last_holder_of() {
    let mut it = Node::new(node(1));
    // One client that goes, and one that stays subscribed to `shared/x`.
    let staying = it.connect("staying");
    let going = {
        let connect = Connect {
            version: Version::V5,
            client_id: "going".to_owned(),
            clean_start: false,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties {
                session_expiry_interval: Some(1),
                ..Properties::default()
            },
        };
        let mut out = Vec::new();
        it.broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates")
            .subscriber
    };
    it.subscribe(staying, "shared/x");
    it.subscribe(going, "shared/x");
    it.subscribe(going, "only/mine");
    assert_eq!(it.settle().len(), 2, "two filters, two registers");

    let mut out = Vec::new();
    it.broker.disconnect(going, &mut out);
    while it.broker.clock().now() < 60 {
        it.broker.tick(&mut out);
    }
    assert_eq!(
        it.broker.session_count(),
        1,
        "the session must have expired"
    );

    let writes = it.settle();
    assert_eq!(
        writes.len(),
        1,
        "an expiring session tombstoned the wrong number of filters: {writes:?}"
    );
    assert_eq!(writes[0].key(), &it.key("only/mine"));
    assert!(writes[0].is_tombstone());
    assert_eq!(it.replicated.nodes_subscribed("only/mine"), Vec::new());
    assert_eq!(
        it.replicated.nodes_subscribed("shared/x"),
        vec![it.replicated.me()],
        "the filter the staying client still holds was tombstoned with the other one"
    );
}

/// A clean-start reconnect tombstones what the old session held.
///
/// The takeover path: the registry replaces the session under the same
/// handle, so the subscriptions are in the trie and nowhere else. A
/// departure missed here is a filter advertised for ever.
#[test]
fn a_clean_start_reconnect_tombstones_what_the_old_session_held() {
    let mut it = Node::new(node(1));
    let before = it.connect("same");
    it.subscribe(before, "a/b");
    assert_eq!(it.settle().len(), 1);

    let after = it.connect_with("same", true);
    let writes = it.settle();
    assert_eq!(
        writes.len(),
        1,
        "a clean start did not tombstone the filter its session held: {writes:?}"
    );
    assert!(writes[0].is_tombstone());
    assert_eq!(it.replicated.nodes_subscribed("a/b"), Vec::new());

    // And the new session can subscribe to it again, which has to write
    // a register that beats the tombstone.
    it.subscribe(after, "a/b");
    let again = it.settle();
    assert_eq!(again.len(), 1);
    assert!(!again[0].is_tombstone());
    assert!(again[0].stamp() > writes[0].stamp());
    assert_eq!(
        it.replicated.nodes_subscribed("a/b"),
        vec![it.replicated.me()]
    );
}

/// Two nodes subscribed to one filter are two registers, and merge.
///
/// Section 8.2 keys a subscription register by the node as well as the
/// filter, because each node answers for its own subscribers. This is
/// the two-node case: each writes its own register, the exchange gives
/// each the other's, and both end up listing both — and one node's last
/// unsubscribe takes only its own.
#[test]
fn two_nodes_on_one_filter_hold_two_registers_and_agree() {
    let mut left = Node::new(node(1));
    let mut right = Node::new(node(2));
    let here = left.connect("here");
    let there = right.connect("there");
    left.subscribe(here, "a/b");
    right.subscribe(there, "a/b");
    let from_left = left.settle();
    let from_right = right.settle();
    assert_eq!(from_left.len(), 1);
    assert_eq!(from_right.len(), 1);

    // The exchange, by hand: each takes the other's write.
    assert!(
        left.replicated.observed(from_right[0].clone()),
        "a register nothing held was refused"
    );
    assert!(right.replicated.observed(from_left[0].clone()));
    let both = vec![node(1), node(2)];
    assert_eq!(left.replicated.nodes_subscribed("a/b"), both);
    assert_eq!(right.replicated.nodes_subscribed("a/b"), both);

    // And one node's last unsubscribe takes only its own register.
    left.unsubscribe(here, "a/b");
    let gone = left.settle();
    assert_eq!(gone.len(), 1);
    assert!(gone[0].is_tombstone());
    assert!(right.replicated.observed(gone[0].clone()));
    assert_eq!(left.replicated.nodes_subscribed("a/b"), vec![node(2)]);
    assert_eq!(
        right.replicated.nodes_subscribed("a/b"),
        vec![node(2)],
        "one node's tombstone took the other's register with it"
    );
}

/// What the node held survives a restart, through the store.
///
/// The registers come back from the store and the count is rebuilt by the
/// replay of this node's own subscriptions — so a restarted node knows
/// both what the cluster was told and what its own clients want, and
/// writes nothing it does not have to.
#[test]
fn a_restart_comes_back_holding_the_registers_and_the_count() {
    let mut it = Node::new(node(1));
    let first = it.connect("first");
    let second = it.connect("second");
    it.subscribe(first, "a/b");
    it.subscribe(second, "a/b");
    it.subscribe(first, "c/d");
    it.unsubscribe(first, "c/d");
    it.settle();

    let restored = it.store.restore().expect("a restore");
    assert_eq!(
        restored.metadata.len(),
        2,
        "a tombstone is held beside the value, not instead of it"
    );
    let after = Replicated::open(
        it.replicated.me(),
        HybridClock::resume(restored.clock, DEFAULT_MAX_DRIFT_MILLIS),
        &restored.metadata,
    )
    .expect("what this build wrote, it reads");
    assert_eq!(after.records_held(), it.replicated.records_held());
    assert_eq!(
        after.nodes_subscribed("a/b"),
        vec![it.replicated.me()],
        "a restart came back advertising nothing"
    );
    assert_eq!(after.nodes_subscribed("c/d"), Vec::new());

    // The count is rebuilt by the replay rather than restored, and until
    // it is, this node has no local subscribers — which is exactly why a
    // restore replays the subscriptions it stored and why it keeps the
    // filter events that replay produces.
    assert_eq!(after.local_subscribers("a/b"), 0);
    assert!(
        after.subscriber_arrived("a/b").is_some(),
        "the first rebuilt subscriber is a filter leaving zero, like any other"
    );
    assert_eq!(
        after.subscriber_arrived("a/b"),
        None,
        "the second rebuilt subscriber had something to say"
    );
    assert_eq!(after.local_subscribers("a/b"), 2);

    // **And the replay is what rebuilds the count**, which is the whole
    // reason a restore keeps its filter events although it discards its
    // journal: the journal is state the store already holds, and the
    // count is in no store anywhere. A restore that dropped these would
    // come back with the trie full and the count at zero, so the first
    // genuine last-unsubscribe would tell the cluster nothing.
    let mut replaying = Broker::new(BrokerConfig::default());
    broker_core::restore::restore(&mut replaying, &restored, 0);
    let mut records: Vec<Record> = Vec::new();
    replaying.take_journal(&mut records);
    assert_eq!(records, Vec::new(), "a restore wrote what it had just read");

    let mut events = Vec::new();
    replaying.take_filter_events(&mut events);
    assert_eq!(
        events,
        vec![
            FilterEvent::Arrived("a/b".to_owned()),
            FilterEvent::Arrived("a/b".to_owned())
        ],
        "a restore did not replay the subscriptions the count is rebuilt from"
    );
}

/// The session identity a register never carries.
///
/// Section 8.2: what is replicated is that a node has a subscriber, and
/// what is not is which client it is. There is no field for one, so this
/// is not a rule a test can break — it is a type that cannot say it. What
/// this asserts is the consequence: two different clients on one filter
/// produce byte-identical registers but for the stamp, so nothing about
/// either of them is on the wire.
#[test]
fn a_register_says_nothing_about_which_client_subscribed() {
    let mut it = Node::new(node(1));
    let first = it.connect("a-very-distinctive-client-identifier");
    it.subscribe(first, "a/b");
    let from_first = it.settle().remove(0);
    it.unsubscribe(first, "a/b");
    it.settle();

    let second = it.connect("another-entirely");
    it.subscribe(second, "a/b");
    let from_second = it.settle().remove(0);

    assert_eq!(from_first.key(), from_second.key());
    assert_eq!(
        from_first.register().value(),
        from_second.register().value()
    );
    let bytes = postcard::to_allocvec(&from_second.to_record()).expect("a register encodes");
    for client in ["a-very-distinctive-client-identifier", "another-entirely"] {
        assert!(
            !bytes
                .windows(client.len())
                .any(|window| window == client.as_bytes()),
            "a register carried the client identifier {client}"
        );
    }
}
