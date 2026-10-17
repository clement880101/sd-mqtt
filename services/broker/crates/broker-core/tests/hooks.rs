//! Every hook point, reached through the broker.
//!
//! **Reached through the broker, and that is the whole point of this
//! file.** Convention 5's last entry: a unit test of a chain is not a
//! test of the caller that was supposed to walk it. `hooks.rs` already
//! proves that a chain of decisions resolves correctly; nothing in it
//! would notice if the broker never asked. And a hook point that never
//! fires is a connector that silently never runs — no metric moves, no
//! log line appears, and the only symptom is that nothing happens.
//!
//! So there is one test per firing site, and each drives the broker the
//! way a client would: connect, subscribe, publish, disconnect, tick.
//! Deleting any one firing site fails the test named after it.

use std::sync::Mutex;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use broker_core::broker::{Broker, BrokerConfig};
use broker_core::hooks::{ClientInfo, Credentials, Decision, Defaults, Disposition, Hook};
use broker_core::session::message::Message;
use broker_core::session::queue::DropReason;
use broker_core::session::Action;
use broker_core::subscriptions::SubscriberId;
use bytes::Bytes;
use mqtt_codec::packet::{Connect, Publish, Subscribe, SubscriptionOptions};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode, Version};

// ---------------------------------------------------------------------
// The clients this file drives the broker with.
// ---------------------------------------------------------------------

fn connect_packet(client: &str) -> Connect {
    Connect {
        version: Version::V5,
        client_id: client.to_string(),
        clean_start: true,
        keep_alive: 0,
        username: None,
        password: None,
        will: None,
        properties: Properties::default(),
    }
}

/// A session that outlives its connection, so it has an expiry to reach.
fn durable_connect(client: &str, expiry: u32) -> Connect {
    Connect {
        clean_start: false,
        properties: Properties {
            session_expiry_interval: Some(expiry),
            ..Properties::default()
        },
        ..connect_packet(client)
    }
}

fn subscribe_packet(filter: &str, qos: QoS) -> Packet {
    Packet::Subscribe(Subscribe {
        packet_id: 1,
        filters: vec![SubscriptionOptions {
            filter: filter.to_string(),
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: mqtt_codec::packet::RetainHandling::SendAlways,
        }],
        properties: Properties::default(),
    })
}

fn publish_packet(topic: &str, qos: QoS, packet_id: Option<u16>) -> Packet {
    Packet::Publish(Publish {
        dup: false,
        qos,
        retain: false,
        topic: topic.to_string(),
        packet_id,
        payload: Bytes::from_static(b"payload"),
        properties: Properties::default(),
    })
}

/// Connect a client and return its handle.
fn join(broker: &mut Broker, client: &str) -> SubscriberId {
    let mut out = Vec::new();
    broker
        .connect("", &connect_packet(client), &mut out)
        .expect("the default decision authenticates")
        .subscriber
}

/// Every publish in `out`, as topic and payload.
fn published(out: &[(SubscriberId, Action)]) -> Vec<(SubscriberId, String)> {
    out.iter()
        .filter_map(|(to, action)| match action {
            Action::Send(Packet::Publish(publish)) => Some((*to, publish.topic.clone())),
            _ => None,
        })
        .collect()
}

/// The level of every publish in `out`.
fn published_levels(out: &[(SubscriberId, Action)]) -> Vec<QoS> {
    out.iter()
        .filter_map(|(_, action)| match action {
            Action::Send(Packet::Publish(publish)) => Some(publish.qos),
            _ => None,
        })
        .collect()
}

/// Every acknowledgement reason in `out`.
fn ack_reasons(out: &[(SubscriberId, Action)]) -> Vec<ReasonCode> {
    out.iter()
        .filter_map(|(_, action)| match action {
            Action::Send(Packet::PubAck(ack)) => Some(ack.reason),
            Action::Send(Packet::PubRec(ack)) => Some(ack.reason),
            _ => None,
        })
        .collect()
}

fn suback_reasons(out: &[(SubscriberId, Action)]) -> Vec<ReasonCode> {
    out.iter()
        .find_map(|(_, action)| match action {
            Action::Send(Packet::SubAck(ack)) => Some(ack.reasons.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------
// The hooks.
// ---------------------------------------------------------------------

/// Writes down which points it was told about, in order.
///
/// A list of strings rather than counters: the order two hooks fire in
/// is part of what several of these tests are about, and a counter
/// cannot say it.
#[derive(Default)]
struct Recorder {
    seen: Mutex<Vec<String>>,
}

impl Recorder {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().expect("no test panics holding it").clone()
    }

    fn saw(&self, what: impl Into<String>) {
        self.seen
            .lock()
            .expect("no test panics holding it")
            .push(what.into());
    }

    fn count(&self, prefix: &str) -> usize {
        self.seen()
            .iter()
            .filter(|entry| entry.starts_with(prefix))
            .count()
    }
}

impl Hook for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }

    fn authenticate(&self, credentials: &Credentials<'_>) -> Decision {
        self.saw(format!("authenticate {}", credentials.client_id));
        Decision::Next
    }

    fn authorize_publish(&self, client: &ClientInfo<'_>, message: &Message) -> Decision {
        self.saw(format!(
            "authorize_publish {} {}",
            client.client_id, message.topic
        ));
        Decision::Next
    }

    fn authorize_subscribe(&self, client: &ClientInfo<'_>, filter: &str, _qos: QoS) -> Decision {
        self.saw(format!("authorize_subscribe {} {filter}", client.client_id));
        Decision::Next
    }

    fn message_received(&self, client: &ClientInfo<'_>, message: &mut Message) -> Disposition {
        self.saw(format!(
            "message_received {} {}",
            client.client_id, message.topic
        ));
        Disposition::Deliver
    }

    fn message_delivered(&self, to: &ClientInfo<'_>, publish: &Publish) {
        self.saw(format!(
            "message_delivered {} {}",
            to.client_id, publish.topic
        ));
    }

    fn message_dropped(&self, to: &ClientInfo<'_>, topic: &str, _qos: QoS, reason: DropReason) {
        self.saw(format!(
            "message_dropped {} {topic} {reason:?}",
            to.client_id
        ));
    }

    fn client_connected(&self, client: &ClientInfo<'_>) {
        self.saw(format!("client_connected {}", client.client_id));
    }

    fn client_disconnected(&self, client: &ClientInfo<'_>) {
        self.saw(format!("client_disconnected {}", client.client_id));
    }

    fn session_expired(&self, client: &ClientInfo<'_>) {
        self.saw(format!("session_expired {}", client.client_id));
    }
}

/// Answers one authorizing point with a canned decision and the rest
/// with `Next`.
struct Authorizer {
    authenticate: Decision,
    publish: Decision,
    subscribe: Decision,
    /// Set the moment this hook is asked anything, so a test can say
    /// that a chain stopped before reaching it.
    asked: AtomicBool,
}

impl Authorizer {
    fn new() -> Self {
        Self {
            authenticate: Decision::Next,
            publish: Decision::Next,
            subscribe: Decision::Next,
            asked: AtomicBool::new(false),
        }
    }
}

impl Hook for Authorizer {
    fn name(&self) -> &str {
        "authorizer"
    }

    fn authenticate(&self, _credentials: &Credentials<'_>) -> Decision {
        self.asked.store(true, Ordering::Relaxed);
        self.authenticate
    }

    fn authorize_publish(&self, _client: &ClientInfo<'_>, _message: &Message) -> Decision {
        self.asked.store(true, Ordering::Relaxed);
        self.publish
    }

    fn authorize_subscribe(&self, _client: &ClientInfo<'_>, _filter: &str, _qos: QoS) -> Decision {
        self.asked.store(true, Ordering::Relaxed);
        self.subscribe
    }
}

/// Moves every message from one topic to another.
struct Mover {
    from: &'static str,
    to: &'static str,
}

impl Hook for Mover {
    fn name(&self) -> &str {
        "mover"
    }

    fn message_received(&self, _client: &ClientInfo<'_>, message: &mut Message) -> Disposition {
        if &*message.topic == self.from {
            message.topic = Arc::from(self.to);
        }
        Disposition::Deliver
    }
}

/// Sets every message to one level, whatever the publisher sent.
struct Leveller {
    to: QoS,
}

impl Hook for Leveller {
    fn name(&self) -> &str {
        "leveller"
    }

    fn message_received(&self, _client: &ClientInfo<'_>, message: &mut Message) -> Disposition {
        message.qos = self.to;
        Disposition::Deliver
    }
}

struct Swallower;

impl Hook for Swallower {
    fn name(&self) -> &str {
        "swallower"
    }

    fn message_received(&self, _client: &ClientInfo<'_>, _message: &mut Message) -> Disposition {
        Disposition::Drop
    }
}

// ---------------------------------------------------------------------
// 1. authenticate
// ---------------------------------------------------------------------

#[test]
fn the_authenticate_hook_fires_on_a_connect() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    join(&mut broker, "c");
    assert_eq!(recorder.seen()[0], "authenticate c");
}

#[test]
fn a_denied_connect_registers_nothing() {
    let mut denier = Authorizer::new();
    denier.authenticate = Decision::Deny;
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(denier));

    let mut out = Vec::new();
    let refused = broker.connect("", &connect_packet("c"), &mut out);
    assert_eq!(refused, Err(ReasonCode::NotAuthorized));
    assert_eq!(broker.session_count(), 0, "nothing may be registered");
    assert!(out.is_empty(), "and nothing may be owed");
}

#[test]
fn a_denied_connect_does_not_displace_the_incumbent() {
    // The reason the chain runs before anything else happens. A
    // rejected connection that had already displaced the live one
    // would let an unauthenticated client hang up anybody it could
    // name.
    let holder = Arc::new(Authorizer::new());
    let mut broker = Broker::new(BrokerConfig::default());

    let incumbent = join(&mut broker, "c");
    broker.hooks_mut().install(holder);
    let denier = {
        let mut denier = Authorizer::new();
        denier.authenticate = Decision::Deny;
        Arc::new(denier)
    };
    broker.hooks_mut().install(denier);

    let mut out = Vec::new();
    assert_eq!(
        broker.connect("", &connect_packet("c"), &mut out),
        Err(ReasonCode::NotAuthorized)
    );
    assert!(out.is_empty(), "the incumbent was not told to close");
    assert!(
        broker
            .session(incumbent)
            .is_some_and(broker_core::session::Session::is_connected),
        "the incumbent is still attached"
    );
}

#[test]
fn a_chain_of_nexts_reaches_the_connect_default_both_ways() {
    // The default applied in the broker, not in the chain: a test of
    // `Hooks` alone cannot say the broker consults it.
    for (default, expected) in [(true, true), (false, false)] {
        let mut broker = Broker::new(BrokerConfig::default());
        broker.hooks_mut().install(Arc::new(Authorizer::new()));
        broker.hooks_mut().set_defaults(Defaults {
            authenticate: default,
            ..Defaults::default()
        });

        let mut out = Vec::new();
        let outcome = broker.connect("", &connect_packet("c"), &mut out);
        assert_eq!(outcome.is_ok(), expected, "default {default}");
    }
}

// ---------------------------------------------------------------------
// 2. authorize publish
// ---------------------------------------------------------------------

#[test]
fn the_authorize_publish_hook_fires_on_a_publish() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    let publisher = join(&mut broker, "p");
    let mut out = Vec::new();
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtMostOnce, None),
            &mut out,
        )
        .expect("a well formed publish");

    assert_eq!(recorder.count("authorize_publish p a/b"), 1);
}

#[test]
fn a_denied_publish_reaches_nobody_and_says_why() {
    let mut denier = Authorizer::new();
    denier.publish = Decision::Deny;
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(denier));

    let subscriber = join(&mut broker, "s");
    let publisher = join(&mut broker, "p");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");

    out.clear();
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtLeastOnce, Some(7)),
            &mut out,
        )
        .expect("a well formed publish");

    assert!(published(&out).is_empty(), "nobody may receive it");
    assert_eq!(
        ack_reasons(&out),
        vec![ReasonCode::NotAuthorized],
        "and the publisher is told why, rather than told nobody listened"
    );
}

// ---------------------------------------------------------------------
// 3. authorize subscribe
// ---------------------------------------------------------------------

#[test]
fn the_authorize_subscribe_hook_fires_on_a_subscribe() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    let subscriber = join(&mut broker, "s");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");

    assert_eq!(recorder.count("authorize_subscribe s a/b"), 1);
}

#[test]
fn a_denied_subscribe_leaves_nothing_behind() {
    let mut denier = Authorizer::new();
    denier.subscribe = Decision::Deny;
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(denier));

    let subscriber = join(&mut broker, "s");
    let publisher = join(&mut broker, "p");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");

    assert_eq!(suback_reasons(&out), vec![ReasonCode::NotAuthorized]);
    assert_eq!(broker.subscription_count(), 0, "nothing in the trie");

    out.clear();
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtMostOnce, None),
            &mut out,
        )
        .expect("a well formed publish");
    assert!(
        published(&out).is_empty(),
        "a refused filter must not deliver"
    );
}

// ---------------------------------------------------------------------
// 4 and 5. message received: rewriting, and dropping.
// ---------------------------------------------------------------------

#[test]
fn the_message_received_hook_fires_before_matching() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    let subscriber = join(&mut broker, "s");
    let publisher = join(&mut broker, "p");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtMostOnce, None),
            &mut out,
        )
        .expect("a well formed publish");

    let seen = recorder.seen();
    let received = seen
        .iter()
        .position(|entry| entry == "message_received p a/b")
        .expect("the hook should have fired");
    let delivered = seen
        .iter()
        .position(|entry| entry.starts_with("message_delivered"))
        .expect("and the message should have gone out");
    assert!(
        received < delivered,
        "the message hook runs before the message is matched: {seen:?}"
    );
}

#[test]
fn a_rewritten_topic_decides_where_the_message_goes() {
    // The test that proves the rewrite happens before matching rather
    // than after. A rewrite applied afterwards would route by the old
    // topic and deliver the new one, and only a subscriber on each
    // topic can tell the two apart.
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(Mover {
        from: "raw/telemetry",
        to: "cooked/telemetry",
    }));

    let on_old = join(&mut broker, "old");
    let on_new = join(&mut broker, "new");
    let publisher = join(&mut broker, "p");

    let mut out = Vec::new();
    broker
        .handle(
            on_old,
            &subscribe_packet("raw/telemetry", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    broker
        .handle(
            on_new,
            &subscribe_packet("cooked/telemetry", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");

    out.clear();
    broker
        .handle(
            publisher,
            &publish_packet("raw/telemetry", QoS::AtMostOnce, None),
            &mut out,
        )
        .expect("a well formed publish");

    assert_eq!(
        published(&out),
        vec![(on_new, "cooked/telemetry".to_string())],
        "the rewritten topic is what was matched and what was delivered"
    );
}

#[test]
fn a_dropped_message_reaches_nobody_and_is_counted() {
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(Swallower));

    let subscriber = join(&mut broker, "s");
    let publisher = join(&mut broker, "p");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");

    out.clear();
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtLeastOnce, Some(3)),
            &mut out,
        )
        .expect("a well formed publish");

    assert!(published(&out).is_empty(), "no subscriber receives it");
    assert_eq!(
        ack_reasons(&out),
        vec![ReasonCode::NoMatchingSubscribers],
        "and the count of who received it is zero, which is what the \
         acknowledgement carries"
    );
}

#[test]
fn a_dropped_message_is_not_retained() {
    // A hook refused it, so leaving it on the topic for the next
    // subscriber would deliver the thing that was refused, later.
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(Swallower));

    let publisher = join(&mut broker, "p");
    let mut out = Vec::new();
    broker
        .handle(
            publisher,
            &Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: true,
                topic: "a/b".to_string(),
                packet_id: None,
                payload: Bytes::from_static(b"payload"),
                properties: Properties::default(),
            }),
            &mut out,
        )
        .expect("a well formed publish");

    assert_eq!(broker.retained_count(), 0);
}

// ---------------------------------------------------------------------
// 6 and 7. delivered, and dropped.
// ---------------------------------------------------------------------

#[test]
fn the_message_delivered_hook_fires_on_a_delivery() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    let subscriber = join(&mut broker, "s");
    let publisher = join(&mut broker, "p");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtMostOnce, None),
            &mut out,
        )
        .expect("a well formed publish");

    assert_eq!(recorder.count("message_delivered s a/b"), 1);
}

#[test]
fn the_message_dropped_hook_fires_on_a_discard() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    // A fire-and-forget message to a session with nobody attached is
    // discarded by its queue rather than held, and the client never
    // learns of it: exactly the discard section 11.3 wants a hook on.
    let subscriber = join(&mut broker, "s");
    let publisher = join(&mut broker, "p");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    broker.disconnect(subscriber, &mut out);

    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtMostOnce, None),
            &mut out,
        )
        .expect("a well formed publish");

    assert_eq!(
        recorder.count("message_dropped s a/b QosZeroWhileOffline"),
        1
    );
}

// ---------------------------------------------------------------------
// 8, 9 and 10. connected, disconnected, expired.
// ---------------------------------------------------------------------

#[test]
fn the_client_connected_hook_fires_on_a_connect() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    join(&mut broker, "c");
    assert_eq!(recorder.count("client_connected c"), 1);
}

#[test]
fn the_client_disconnected_hook_fires_when_the_connection_goes() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    let client = join(&mut broker, "c");
    let mut out = Vec::new();
    broker.disconnect(client, &mut out);

    assert_eq!(recorder.count("client_disconnected c"), 1);

    // And not twice: a socket that closes after a disconnect packet
    // reaches the same path a second time.
    broker.disconnect(client, &mut out);
    assert_eq!(recorder.count("client_disconnected c"), 1);
}

#[test]
fn the_session_expired_hook_fires_when_the_session_goes() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    let mut out = Vec::new();
    let client = broker
        .connect("", &durable_connect("c", 1), &mut out)
        .expect("the default decision authenticates")
        .subscriber;
    broker.disconnect(client, &mut out);

    for _ in 0..4 {
        broker.tick(&mut out);
    }

    assert_eq!(recorder.count("session_expired c"), 1);
    assert_eq!(broker.session_count(), 0);
}

#[test]
fn a_session_destroyed_reports_what_it_was_holding_before_it_says_it_ended() {
    // The expiry path takes the session out of the registry before it
    // clears the queue, so the drops it owes have nothing left to look
    // their owner up by. Guarded here because the symptom of getting
    // it wrong is a connector that sees a session end and never learns
    // what ending it discarded.
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    let mut out = Vec::new();
    let subscriber = broker
        .connect("", &durable_connect("s", 1), &mut out)
        .expect("the default decision authenticates")
        .subscriber;
    let publisher = join(&mut broker, "p");
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtLeastOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    broker.disconnect(subscriber, &mut out);
    // Queued rather than sent: nobody is attached, and level one is
    // held for the session rather than discarded.
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtLeastOnce, Some(1)),
            &mut out,
        )
        .expect("a well formed publish");

    for _ in 0..4 {
        broker.tick(&mut out);
    }

    let seen = recorder.seen();
    let dropped = seen
        .iter()
        .position(|entry| entry.starts_with("message_dropped s a/b SessionEnded"))
        .expect("the queued message was discarded and must be reported");
    let expired = seen
        .iter()
        .position(|entry| entry == "session_expired s")
        .expect("and the session ended");
    assert!(dropped < expired, "in that order: {seen:?}");
}

// ---------------------------------------------------------------------
// All of them at once.
// ---------------------------------------------------------------------

#[test]
fn every_one_of_the_nine_points_fires_in_one_history() {
    // Section 9.3 lists nine. This drives a history that reaches all
    // of them, so a point added to the trait and never wired up is one
    // missing entry here rather than something nobody looks for.
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    let mut out = Vec::new();
    let subscriber = broker
        .connect("", &durable_connect("s", 1), &mut out)
        .expect("the default decision authenticates")
        .subscriber;
    let publisher = join(&mut broker, "p");
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtLeastOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtLeastOnce, Some(1)),
            &mut out,
        )
        .expect("a well formed publish");
    broker.disconnect(subscriber, &mut out);
    // A fire-and-forget message to the now-offline session, which its
    // queue discards.
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtMostOnce, None),
            &mut out,
        )
        .expect("a well formed publish");
    for _ in 0..4 {
        broker.tick(&mut out);
    }

    for point in [
        "authenticate",
        "authorize_publish",
        "authorize_subscribe",
        "message_received",
        "message_delivered",
        "message_dropped",
        "client_connected",
        "client_disconnected",
        "session_expired",
    ] {
        assert!(
            recorder.count(point) > 0,
            "{point} never fired: {:?}",
            recorder.seen()
        );
    }
}

#[test]
fn a_broker_with_no_hooks_behaves_exactly_as_it_did() {
    // The upgrade guarantee. Nothing installed means every default
    // applies and every path is the one that was there before.
    let mut broker = Broker::new(BrokerConfig::default());
    assert!(broker.hooks().is_empty());

    let subscriber = join(&mut broker, "s");
    let publisher = join(&mut broker, "p");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    assert_eq!(suback_reasons(&out), vec![ReasonCode::Success]);

    out.clear();
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtLeastOnce, Some(1)),
            &mut out,
        )
        .expect("a well formed publish");
    assert_eq!(published(&out), vec![(subscriber, "a/b".to_string())]);
    assert_eq!(ack_reasons(&out), vec![ReasonCode::Success]);
}

// ---------------------------------------------------------------------
// 10. A publication with no session behind it.
// ---------------------------------------------------------------------

/// A message as a connector hands one over: a topic, a payload and the
/// user properties it carries, which is where the hop count lives.
fn originated(topic: &str, user_properties: Vec<(String, String)>) -> Message {
    let mut message = Message::new(topic, Bytes::from_static(b"body"), QoS::AtMostOnce, false);
    message.properties = Arc::new(broker_core::properties::SharedProperties::new(Properties {
        user_properties,
        ..Properties::default()
    }));
    message
}

/// Section 9.2: a bridge publishes inbound messages into the local
/// broker "where any rule can act on them". This is the sentence.
#[test]
fn a_publication_with_no_session_fires_the_message_hook() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());
    let subscriber = join(&mut broker, "s");

    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("ingest/across", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    out.clear();

    let delivered = broker
        .originate(
            originated("ingest/across", Vec::new()),
            "gateway",
            0,
            &mut out,
        )
        .expect("one hop is under any maximum");

    assert_eq!(delivered, 1, "the subscriber still gets it");
    assert!(
        recorder
            .seen()
            .contains(&"message_received gateway ingest/across".to_owned()),
        "and the rule chain was asked, naming what originated it: {:?}",
        recorder.seen()
    );
}

/// The other half of the ruling. There is no client, so the chain that
/// judges what a client asked to publish is not asked: an access
/// control hook written about clients must not be able to switch off a
/// configured connector.
#[test]
fn a_publication_with_no_session_does_not_ask_the_authorizing_chain() {
    let mut denier = Authorizer::new();
    denier.publish = Decision::Deny;
    let denier = Arc::new(denier);
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(denier.clone());
    let subscriber = join(&mut broker, "s");

    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    out.clear();

    let delivered = broker
        .originate(originated("a/b", Vec::new()), "poller", 0, &mut out)
        .expect("one hop is under any maximum");
    assert_eq!(delivered, 1, "a denying chain does not reach it");
    assert_eq!(
        published(&out).len(),
        1,
        "and the subscriber was sent it: {out:?}"
    );
}

/// A hook may still take it, which is the power a connector chain needs
/// to be able to filter.
#[test]
fn a_hook_may_drop_a_publication_with_no_session() {
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(Swallower));
    let subscriber = join(&mut broker, "s");

    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    out.clear();

    let delivered = broker
        .originate(originated("a/b", Vec::new()), "ingest", 0, &mut out)
        .expect("one hop is under any maximum");
    assert_eq!(delivered, 0);
    assert!(published(&out).is_empty(), "nobody receives it: {out:?}");
}

/// And it may rewrite it before anything is matched, exactly as it may
/// for a client's publish.
#[test]
fn a_hook_rewrites_a_publication_with_no_session_before_matching() {
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(Mover {
        from: "from/here",
        to: "to/there",
    }));
    let subscriber = join(&mut broker, "s");

    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("to/there", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    out.clear();

    let delivered = broker
        .originate(originated("from/here", Vec::new()), "bridge", 0, &mut out)
        .expect("one hop is under any maximum");
    assert_eq!(delivered, 1);
    assert_eq!(
        published(&out),
        vec![(subscriber, "to/there".to_owned())],
        "routed by the rewritten topic, not the one it arrived on"
    );
}

/// Section 9.2's third loop, at the door. A publication already at the
/// maximum is not routed, not retained and fires no rule.
#[test]
fn a_publication_over_the_hop_limit_is_dropped_and_fires_nothing() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig {
        max_hops: 3,
        ..BrokerConfig::default()
    });
    broker.hooks_mut().install(recorder.clone());
    let subscriber = join(&mut broker, "s");

    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    out.clear();

    let hops = |count: u32| vec![(broker_core::hops::HOP_COUNT.to_owned(), count.to_string())];

    // At the maximum is still carried: the limit is what a publication
    // may have been through, not one less.
    let delivered = broker
        .originate(originated("a/b", hops(3)), "bridge", 0, &mut out)
        .expect("three hops is the maximum, not over it");
    assert_eq!(delivered, 1);

    out.clear();
    let refused = broker
        .originate(originated("a/b", hops(4)), "bridge", 0, &mut out)
        .expect_err("four hops is over a maximum of three");
    assert_eq!(refused.hops, 4);
    assert_eq!(refused.maximum, 3);
    assert!(out.is_empty(), "nothing is routed: {out:?}");
    assert_eq!(
        recorder.count("message_received"),
        1,
        "and no rule was fired by it: {:?}",
        recorder.seen()
    );
}

/// A retained publication over the limit leaves nothing behind either.
/// A message a limit refused must not sit on the topic waiting for the
/// next subscriber to find it.
#[test]
fn a_retained_publication_over_the_hop_limit_is_not_retained() {
    let mut broker = Broker::new(BrokerConfig {
        max_hops: 1,
        ..BrokerConfig::default()
    });
    let mut out = Vec::new();

    let mut message = originated(
        "a/b",
        vec![(broker_core::hops::HOP_COUNT.to_owned(), "9".to_owned())],
    );
    message.retain = true;
    broker
        .originate(message, "bridge", 0, &mut out)
        .expect_err("nine hops is over a maximum of one");

    let subscriber = join(&mut broker, "s");
    out.clear();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    assert!(
        published(&out).is_empty(),
        "there is no retained message to find: {out:?}"
    );
}

/// A message with no count has not been through a connector, so it is
/// never over a limit. Every publication the admin API makes is one.
#[test]
fn a_publication_with_no_hop_count_is_never_over_the_limit() {
    let mut broker = Broker::new(BrokerConfig {
        max_hops: 1,
        ..BrokerConfig::default()
    });
    let subscriber = join(&mut broker, "s");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    out.clear();

    let delivered = broker
        .originate(originated("a/b", Vec::new()), "admin", 0, &mut out)
        .expect("no count is not a count of anything");
    assert_eq!(delivered, 1);
}

// ---------------------------------------------------------------------
// 11. A will.
// ---------------------------------------------------------------------

/// A client that leaves a will and then vanishes.
fn connect_with_will(client: &str, topic: &str) -> Connect {
    Connect {
        will: Some(mqtt_codec::packet::Will {
            topic: topic.to_string(),
            payload: Bytes::from_static(b"offline"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Properties::default(),
        }),
        ..connect_packet(client)
    }
}

/// The ruling, driven through the broker. A rule that fires on a device
/// coming up must fire on it going down.
#[test]
fn a_will_fires_the_message_hook() {
    let recorder = Recorder::new();
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(recorder.clone());

    let subscriber = join(&mut broker, "s");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("device/1/status", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");

    let mut out = Vec::new();
    let publisher = broker
        .connect("", &connect_with_will("d", "device/1/status"), &mut out)
        .expect("the default decision authenticates")
        .subscriber;
    out.clear();
    // Gone without a disconnect packet, which is what owes a will.
    broker.disconnect(publisher, &mut out);
    broker.tick(&mut out);

    assert_eq!(
        published(&out),
        vec![(subscriber, "device/1/status".to_owned())],
        "the will still reaches the subscriber: {out:?}"
    );
    assert!(
        recorder
            .seen()
            .contains(&"message_received d device/1/status".to_owned()),
        "and the rule chain saw it, named after the client that left it: {:?}",
        recorder.seen()
    );
}

/// The other half of that ruling: the will is judged at CONNECT, where
/// there is still somebody to answer, and not again when it falls due.
/// The client is gone by then and cannot be told it was refused, so a
/// rule installed after the connect does not take a will the connect
/// admitted.
#[test]
fn a_will_does_not_ask_the_authorizing_chain() {
    let mut broker = Broker::new(BrokerConfig::default());

    let subscriber = join(&mut broker, "s");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("device/1/status", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");

    let mut out = Vec::new();
    let publisher = broker
        .connect("", &connect_with_will("d", "device/1/status"), &mut out)
        .expect("the default decision authenticates")
        .subscriber;
    // Installed once the connect has been answered, so the only place
    // it could be asked is when the will falls due.
    let mut denier = Authorizer::new();
    denier.publish = Decision::Deny;
    broker.hooks_mut().install(Arc::new(denier));
    out.clear();
    broker.disconnect(publisher, &mut out);
    broker.tick(&mut out);

    assert_eq!(
        published(&out),
        vec![(subscriber, "device/1/status".to_owned())],
        "[MQTT-3.1.2-8] owes this will whatever an authorizing hook thinks: {out:?}"
    );
}

/// Design section 5.2: the will topic is authorized as an ordinary
/// publish at CONNECT. A client that may not publish to a topic must
/// not reach it by naming it as its will and dropping the socket, so
/// the connect is refused with 3.2.2.2's "not authorized" and nothing
/// is registered.
#[test]
fn a_will_the_client_may_not_publish_refuses_the_connect() {
    let mut denier = Authorizer::new();
    denier.publish = Decision::Deny;
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(denier));

    let subscriber = join(&mut broker, "s");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("device/1/status", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");

    let mut out = Vec::new();
    let refused = broker.connect("", &connect_with_will("d", "device/1/status"), &mut out);
    assert_eq!(
        refused.map(|registration| registration.subscriber),
        Err(ReasonCode::NotAuthorized),
        "a will the client may not publish is a connect the broker will not serve"
    );
    assert_eq!(
        broker.session_count(),
        1,
        "nothing was registered for the refused client; only the subscriber remains"
    );

    broker.tick(&mut out);
    assert!(
        published(&out).is_empty(),
        "and no will is owed by a connection that was never accepted: {out:?}"
    );
}

/// And a hook may take a will, which is what suppressing the will of a
/// client being taken down deliberately looks like.
#[test]
fn a_hook_may_drop_a_will() {
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(Swallower));

    let subscriber = join(&mut broker, "s");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("device/1/status", QoS::AtMostOnce),
            &mut out,
        )
        .expect("a well formed subscribe");

    let mut out = Vec::new();
    let publisher = broker
        .connect("", &connect_with_will("d", "device/1/status"), &mut out)
        .expect("the default decision authenticates")
        .subscriber;
    out.clear();
    broker.disconnect(publisher, &mut out);
    broker.tick(&mut out);

    assert!(
        published(&out).is_empty(),
        "nobody receives a will a hook took: {out:?}"
    );
}

// ---------------------------------------------------------------------
// Section 9.3's quality of service, in the direction the spec did not say
// ---------------------------------------------------------------------

/// A hook may lower the level.
///
/// The half that works, asserted first so that the clamped half below
/// cannot pass by the rewrite doing nothing at all. Without this, a
/// `message.qos = published` that ignored the hook entirely would
/// satisfy every assertion in the next test.
#[test]
fn a_message_hook_may_lower_the_level() {
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(Leveller {
        to: QoS::AtMostOnce,
    }));

    let subscriber = join(&mut broker, "s");
    let publisher = join(&mut broker, "p");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::ExactlyOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    out.clear();
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::ExactlyOnce, Some(1)),
            &mut out,
        )
        .expect("a well formed publish");

    assert_eq!(
        published_levels(&out),
        vec![QoS::AtMostOnce],
        "a hook lowered the level and the subscriber got the publisher's"
    );
}

/// And may not raise it past what the publisher sent.
///
/// Section 9.3 permits a message hook to rewrite quality of service
/// and stopped there, which left the one direction with protocol
/// consequences unsaid and unexercised. What the design now says, and
/// what this asserts, is that a raise is clamped.
///
/// The reason is the promise. At-most-once is not acknowledged and is
/// not made durable, so a hook that raised one to exactly-once would
/// have the broker queue it durably and redeliver it on a resumed
/// session — a guarantee whose foundation, a publisher that will send
/// it again, does not exist. The subscriber would be told a message
/// cannot be lost by a message that a restart a moment later loses.
///
/// Convention 5's question: this fails if the clamp is removed, and
/// the input makes it fail because the subscriber holds the filter at
/// exactly-once, so nothing *else* between the hook and the wire would
/// bring the level back down. A subscriber at at-most-once would have
/// passed this test with no clamp at all, which is the shape of test
/// that guards nothing.
#[test]
fn a_message_hook_may_not_raise_the_level_past_what_the_publisher_sent() {
    let mut broker = Broker::new(BrokerConfig::default());
    broker.hooks_mut().install(Arc::new(Leveller {
        to: QoS::ExactlyOnce,
    }));

    let subscriber = join(&mut broker, "s");
    let publisher = join(&mut broker, "p");
    let mut out = Vec::new();
    broker
        .handle(
            subscriber,
            &subscribe_packet("a/b", QoS::ExactlyOnce),
            &mut out,
        )
        .expect("a well formed subscribe");
    out.clear();
    broker
        .handle(
            publisher,
            &publish_packet("a/b", QoS::AtMostOnce, None),
            &mut out,
        )
        .expect("a well formed publish");

    assert_eq!(
        published_levels(&out),
        vec![QoS::AtMostOnce],
        "a hook raised a fire-and-forget message to a level this broker \
         never took responsibility for"
    );
}
