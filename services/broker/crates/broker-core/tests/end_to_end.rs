//! Properties over whole broker histories.
//!
//! Every piece below has its own tests. These exist for the interactions
//! between them, which is where a broker's real bugs live and which no
//! unit test reaches. Time is a parameter, so a thirty second will delay
//! is exercised in a microsecond and nothing sleeps.
//!
//! # What these generators cannot produce
//!
//! The question convention 5 insists on. Closed here: no-local, which
//! [`a_no_local_subscriber_does_not_receive_its_own_publication`] sets
//! and which needs a publisher that is also a subscriber; takeover,
//! which [`a_takeover_closes_the_incumbent_and_clean_start_discards_its_subscriptions`]
//! drives by connecting the *same* client identifier twice without a
//! disconnect between; the retain flag and retain-as-published, and
//! levels above the lowest along with the window, the queue and the
//! acknowledgement handshakes, all of which
//! [`every_received_publish_is_answered_and_frees_its_slot`] and
//! [`a_resumed_session_redelivers_what_it_still_owes`] exercise
//! end to end.
//!
//! Left open, deliberately: shared subscriptions, mountpoints other than
//! the default, protocol version 3.1.1, subscription identifiers,
//! message expiry, an empty payload clearing a retained topic, dollar
//! topics, a queue driven past its bounds, and more than one packet
//! identifier outstanding per publisher. Each is covered by the tests of
//! the piece that owns it.
//!
//! Keep-alive expiry is left to the one test below rather than to a
//! generator, because the interaction worth guarding is a single point
//! rather than a range.

use broker_core::broker::{Broker, BrokerConfig};
use broker_core::session::message::Message;
use broker_core::session::{Action, DropReason};
use broker_core::subscriptions::SubscriberId;
use broker_core::topic::reference;
use bytes::Bytes;
use mqtt_codec::packet::{
    Ack, Connect, Disconnect, Publish, RetainHandling, Subscribe, SubscriptionOptions, Will,
};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode, Version};
use proptest::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

/// A small vocabulary, so collisions and overlaps are common.
fn any_topic() -> impl Strategy<Value = String> {
    prop::collection::vec(prop_oneof!["a", "b", "c"], 1..4).prop_map(|parts| parts.join("/"))
}

fn any_filter() -> impl Strategy<Value = String> {
    (
        prop::collection::vec(
            prop_oneof![3 => prop_oneof!["a", "b", "c"], 1 => Just("+".to_string())],
            1..4,
        ),
        prop::bool::weighted(0.3),
    )
        .prop_map(|(mut levels, trailing_hash)| {
            if trailing_hash {
                levels.push("#".to_string());
            }
            levels.join("/")
        })
}

fn any_qos() -> impl Strategy<Value = QoS> {
    prop_oneof![
        Just(QoS::AtMostOnce),
        Just(QoS::AtLeastOnce),
        Just(QoS::ExactlyOnce),
    ]
}

fn connect_packet(client: &str, keep_alive: u16) -> Connect {
    Connect {
        version: Version::V5,
        client_id: client.to_string(),
        clean_start: true,
        keep_alive,
        username: None,
        password: None,
        will: None,
        properties: Properties::default(),
    }
}

/// A session that outlives its connection, so that a will delay and a
/// resume have room to happen at all.
fn durable_connect(client: &str, seconds: u32) -> Connect {
    let mut connect = connect_packet(client, 0);
    connect.clean_start = false;
    connect.properties.session_expiry_interval = Some(seconds);
    connect
}

/// A will on the topic a watcher below listens to.
fn with_will(connect: &mut Connect, delay: Option<u32>) {
    connect.will = Some(Will {
        topic: "gone/device".into(),
        payload: Bytes::from_static(b"offline"),
        qos: QoS::AtMostOnce,
        retain: false,
        properties: Properties {
            will_delay_interval: delay,
            ..Properties::default()
        },
    });
}

/// A broker with one client watching `gone/+`, which is where every
/// will in these tests is published.
fn broker_with_a_watcher() -> (Broker, SubscriberId) {
    let mut broker = Broker::new(BrokerConfig::default());
    let mut out = Vec::new();
    let watcher = broker.connect("", &connect_packet("watcher", 0), &mut out);
    broker
        .handle(
            watcher.subscriber,
            &Packet::Subscribe(subscribe_packet("gone/+", QoS::AtMostOnce)),
            &mut out,
        )
        .expect("a valid subscribe");
    (broker, watcher.subscriber)
}

fn subscribe_packet(filter: &str, qos: QoS) -> Subscribe {
    Subscribe {
        packet_id: 1,
        filters: vec![SubscriptionOptions {
            filter: filter.to_string(),
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::DoNotSend,
        }],
        properties: Properties::default(),
    }
}

fn publish_packet(
    topic: &str,
    qos: QoS,
    packet_id: Option<u16>,
    retain: bool,
    dup: bool,
) -> Publish {
    Publish {
        dup,
        qos,
        retain,
        topic: topic.to_string(),
        packet_id,
        payload: Bytes::from_static(b"x"),
        properties: Properties::default(),
    }
}

fn release(packet_id: u16) -> Ack {
    Ack {
        packet_id,
        reason: ReasonCode::Success,
        properties: Properties::default(),
    }
}

/// Which subscribers received a publish for `topic`, and how many each.
fn received(out: &[(SubscriberId, Action)], topic: &str) -> HashMap<SubscriberId, usize> {
    let mut counts = HashMap::new();
    for (subscriber, action) in out {
        if let Action::Send(Packet::Publish(publish)) = action {
            if publish.topic == topic {
                *counts.entry(*subscriber).or_insert(0) += 1;
            }
        }
    }
    counts
}

/// How many copies of a publish for `topic` one subscriber received.
fn copies(out: &[(SubscriberId, Action)], topic: &str, subscriber: SubscriberId) -> usize {
    received(out, topic).get(&subscriber).copied().unwrap_or(0)
}

/// The retain flag on every publish for `topic` that reached `subscriber`.
fn retain_flags(
    out: &[(SubscriberId, Action)],
    topic: &str,
    subscriber: SubscriberId,
) -> Vec<bool> {
    out.iter()
        .filter(|(to, _)| *to == subscriber)
        .filter_map(|(_, action)| match action {
            Action::Send(Packet::Publish(publish)) if publish.topic == topic => {
                Some(publish.retain)
            }
            _ => None,
        })
        .collect()
}

/// The reason codes carried by the acknowledgements `qos` calls for, in
/// the order they were issued: a PUBACK at least-once, a PUBREC
/// exactly-once.
///
/// The reasons rather than a count, because a count cannot tell a
/// client that everyone wanted its message from one that nobody did.
fn answers(out: &[(SubscriberId, Action)], subscriber: SubscriberId, qos: QoS) -> Vec<ReasonCode> {
    out.iter()
        .filter(|(to, _)| *to == subscriber)
        .filter_map(|(_, action)| match (qos, action) {
            (QoS::AtLeastOnce, Action::Send(Packet::PubAck(ack))) => Some(ack.reason),
            (QoS::ExactlyOnce, Action::Send(Packet::PubRec(ack))) => Some(ack.reason),
            _ => None,
        })
        .collect()
}

/// How much of a publisher's receive maximum is still reserved.
fn reserved(broker: &Broker, subscriber: SubscriberId) -> usize {
    broker
        .session(subscriber)
        .expect("the session is still there")
        .inbound
        .quota_used()
}

proptest! {
    /// A publication reaches exactly the subscribers whose filters match,
    /// judged against the naive matcher rather than against the trie. The
    /// same technique found real bugs one layer down; this applies it
    /// through the whole broker.
    #[test]
    fn a_publication_reaches_exactly_the_matching_subscribers(
        filters in prop::collection::vec(any_filter(), 1..8),
        topic in any_topic(),
    ) {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let mut holders: Vec<(SubscriberId, String)> = Vec::new();

        for (index, filter) in filters.iter().enumerate() {
            let client = format!("c{index}");
            let registration = broker.connect("", &connect_packet(&client, 0), &mut out);
            broker
                .handle(
                    registration.subscriber,
                    &Packet::Subscribe(subscribe_packet(filter, QoS::AtMostOnce)),
                    &mut out,
                )
                .expect("a valid subscribe");
            holders.push((registration.subscriber, filter.clone()));
        }

        out.clear();
        let now = broker.clock().now();
        broker.publish(
            Message::new(&topic, Bytes::from_static(b"x"), QoS::AtMostOnce, false),
            None,
            now,
            &mut out,
        );
        let got = received(&out, &topic);

        for (subscriber, filter) in &holders {
            let expected = reference::matches(filter, &topic);
            prop_assert_eq!(
                got.contains_key(subscriber),
                expected,
                "filter {:?} against topic {:?}",
                filter,
                topic
            );
        }
    }

    /// One client holding several matching filters receives one copy, not
    /// one per filter. Getting this wrong looks like a duplication bug in
    /// the quality-of-service code and is not.
    #[test]
    fn no_subscriber_receives_a_publication_twice(
        filters in prop::collection::vec(any_filter(), 2..6),
        topic in any_topic(),
    ) {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let registration = broker.connect("", &connect_packet("c", 0), &mut out);
        for filter in &filters {
            broker
                .handle(
                    registration.subscriber,
                    &Packet::Subscribe(subscribe_packet(filter, QoS::AtMostOnce)),
                    &mut out,
                )
                .expect("a valid subscribe");
        }

        out.clear();
        let now = broker.clock().now();
        broker.publish(
            Message::new(&topic, Bytes::from_static(b"x"), QoS::AtMostOnce, false),
            None,
            now,
            &mut out,
        );

        for (subscriber, count) in received(&out, &topic) {
            prop_assert_eq!(count, 1, "subscriber {:?} received {} copies", subscriber, count);
        }
    }

    /// A will fires once, after its delay, and not at all if the client
    /// comes back first. A brief outage must not announce a device as
    /// gone, which is the entire reason the delay exists.
    ///
    /// The session outlives its connection by far more than the delay on
    /// purpose. A will delay is capped by the session expiry interval,
    /// because a will owed by a session that no longer exists falls due
    /// at once, so with the default interval of zero every delay would
    /// collapse to nothing and this property would pass without ever
    /// exercising one.
    #[test]
    fn a_will_fires_once_after_its_delay_unless_the_client_returns(
        delay in 0u32..20,
        returns_after in prop::option::of(1u32..25),
    ) {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        // A watcher subscribed to the will topic.
        let watcher = broker.connect("", &connect_packet("watcher", 0), &mut out);
        broker
            .handle(
                watcher.subscriber,
                &Packet::Subscribe(subscribe_packet("gone/+", QoS::AtMostOnce)),
                &mut out,
            )
            .expect("a valid subscribe");

        let mut connect = durable_connect("device", 600);
        connect.will = Some(Will {
            topic: "gone/device".into(),
            payload: Bytes::from_static(b"offline"),
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Properties {
                will_delay_interval: Some(delay),
                ..Properties::default()
            },
        });
        let device = broker.connect("", &connect, &mut out);

        out.clear();
        broker.disconnect(device.subscriber, &mut out);

        let mut fired = 0;
        for second in 1..=40u32 {
            if returns_after == Some(second) {
                broker.connect("", &connect_packet("device", 0), &mut out);
            }
            out.clear();
            broker.tick(&mut out);
            fired += received(&out, "gone/device").len();
        }

        // The disconnect happened at second zero and only a tick
        // publishes, so the earliest a will can go out is second one.
        let fires_at = delay.max(1);
        let returned_first = returns_after.is_some_and(|second| second <= fires_at);
        prop_assert_eq!(
            fired,
            usize::from(!returned_first),
            "delay {} return {:?}",
            delay,
            returns_after
        );
    }

    /// An expired session receives nothing and leaves no subscriptions
    /// behind. A leaked subscription routes traffic to a session that is
    /// gone, which wastes work and, once handles were reused, would
    /// deliver it to the wrong client.
    #[test]
    fn an_expired_session_leaves_nothing_behind(
        filters in prop::collection::vec(any_filter(), 1..4),
    ) {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let registration = broker.connect("", &durable_connect("c", 5), &mut out);

        for filter in &filters {
            broker
                .handle(
                    registration.subscriber,
                    &Packet::Subscribe(subscribe_packet(filter, QoS::AtMostOnce)),
                    &mut out,
                )
                .expect("a valid subscribe");
        }
        prop_assert!(broker.subscription_count() > 0);

        broker.disconnect(registration.subscriber, &mut out);
        for _ in 0..10 {
            out.clear();
            broker.tick(&mut out);
        }

        prop_assert_eq!(broker.session_count(), 0, "the session is gone");
        prop_assert_eq!(broker.subscription_count(), 0, "and so are its filters");
    }

    /// No-local withholds a message from its own author and from nobody
    /// else. This needs a publisher that is also a subscriber, which is
    /// why it cannot be a corner of the routing property above: there
    /// the publication has no author at all.
    #[test]
    fn a_no_local_subscriber_does_not_receive_its_own_publication(
        filter in any_filter(),
        topic in any_topic(),
        no_local in any::<bool>(),
    ) {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let author = broker.connect("", &connect_packet("author", 0), &mut out);
        let mut request = subscribe_packet(&filter, QoS::AtMostOnce);
        request.filters[0].no_local = no_local;
        broker
            .handle(author.subscriber, &Packet::Subscribe(request), &mut out)
            .expect("a valid subscribe");

        let bystander = broker.connect("", &connect_packet("bystander", 0), &mut out);
        broker
            .handle(
                bystander.subscriber,
                &Packet::Subscribe(subscribe_packet(&filter, QoS::AtMostOnce)),
                &mut out,
            )
            .expect("a valid subscribe");

        out.clear();
        broker
            .handle(
                author.subscriber,
                &Packet::Publish(publish_packet(&topic, QoS::AtMostOnce, None, false, false)),
                &mut out,
            )
            .expect("a valid publish");

        let got = received(&out, &topic);
        let matched = reference::matches(&filter, &topic);
        prop_assert_eq!(
            got.contains_key(&author.subscriber),
            matched && !no_local,
            "the author, filter {:?} topic {:?} no-local {}",
            filter,
            topic,
            no_local
        );
        prop_assert_eq!(
            got.contains_key(&bystander.subscriber),
            matched,
            "the bystander, filter {:?} topic {:?}",
            filter,
            topic
        );
    }

    /// A second connection under the same identifier displaces the first,
    /// and a clean start discards what the first held.
    ///
    /// Both clients use the *same* identifier deliberately. A takeover
    /// test written with two different ones cannot reach the case at all,
    /// which is how this class of gap has hidden before. The subscription
    /// half matters because a session's filters live in the trie keyed by
    /// a handle the registry hands back unchanged, so a clean start that
    /// forgets to sweep them leaves the newcomer receiving traffic it
    /// never asked for.
    #[test]
    fn a_takeover_closes_the_incumbent_and_clean_start_discards_its_subscriptions(
        filter in any_filter(),
        topic in any_topic(),
        clean_start in any::<bool>(),
    ) {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let first = broker.connect("", &durable_connect("same", 600), &mut out);
        broker
            .handle(
                first.subscriber,
                &Packet::Subscribe(subscribe_packet(&filter, QoS::AtMostOnce)),
                &mut out,
            )
            .expect("a valid subscribe");
        prop_assert_eq!(broker.subscription_count(), 1);

        // No disconnect: the incumbent is still live when the second
        // connection arrives.
        out.clear();
        let mut second = durable_connect("same", 600);
        second.clean_start = clean_start;
        let taken_over = broker.connect("", &second, &mut out);

        prop_assert!(taken_over.took_over, "the incumbent was live");
        prop_assert_eq!(
            taken_over.subscriber,
            first.subscriber,
            "one identity, one handle"
        );
        prop_assert_eq!(taken_over.session_present, !clean_start);
        prop_assert_eq!(
            out.iter()
                .filter(|(to, action)| *to == first.subscriber
                    && matches!(action, Action::Close(ReasonCode::SessionTakenOver)))
                .count(),
            1,
            "the incumbent is told exactly once why it is going"
        );
        prop_assert_eq!(
            broker.session_count(),
            1,
            "a takeover replaces a session rather than adding one"
        );
        prop_assert_eq!(
            broker.subscription_count(),
            usize::from(!clean_start),
            "a clean start starts clean"
        );

        out.clear();
        let now = broker.clock().now();
        broker.publish(
            Message::new(&topic, Bytes::from_static(b"x"), QoS::AtMostOnce, false),
            None,
            now,
            &mut out,
        );
        prop_assert_eq!(
            copies(&out, &topic, taken_over.subscriber) > 0,
            !clean_start && reference::matches(&filter, &topic),
            "filter {:?} topic {:?} clean start {}",
            filter,
            topic,
            clean_start
        );
    }

    /// Every publish the broker takes is answered, and the receive
    /// maximum slot it reserved comes back.
    ///
    /// This is the obligation the session plan left to this layer: a slot
    /// that the acknowledgement never frees throttles a client to death
    /// with nothing reporting it. The redelivery case is the one that
    /// bites, because an exactly-once message already held is not
    /// dispatched a second time and it is tempting to return early
    /// without answering it.
    ///
    /// The watcher's filter is generated rather than fixed, so that
    /// roughly half these histories have nobody wanting the message.
    /// With a watcher always subscribed, the reason code could only ever
    /// be success and the arm that says nobody wanted it would be
    /// unreachable: inverting the two would leave the suite green while
    /// telling a client its message reached no one.
    #[test]
    fn every_received_publish_is_answered_and_frees_its_slot(
        qos in any_qos(),
        filter in any_filter(),
        topic in any_topic(),
        retain in any::<bool>(),
        retain_as_published in any::<bool>(),
        redelivered in any::<bool>(),
    ) {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let watcher = broker.connect("", &connect_packet("watcher", 0), &mut out);
        let mut request = subscribe_packet(&filter, QoS::ExactlyOnce);
        request.filters[0].retain_as_published = retain_as_published;
        broker
            .handle(watcher.subscriber, &Packet::Subscribe(request), &mut out)
            .expect("a valid subscribe");

        let author = broker.connect("", &connect_packet("author", 0), &mut out);
        let packet_id = (qos != QoS::AtMostOnce).then_some(9);
        let wanted = reference::matches(&filter, &topic);

        out.clear();
        broker
            .handle(
                author.subscriber,
                &Packet::Publish(publish_packet(&topic, qos, packet_id, retain, false)),
                &mut out,
            )
            .expect("a valid publish");
        if redelivered {
            broker
                .handle(
                    author.subscriber,
                    &Packet::Publish(publish_packet(&topic, qos, packet_id, retain, true)),
                    &mut out,
                )
                .expect("a valid redelivery");
        }

        let attempts = 1 + usize::from(redelivered);
        // Only exactly-once recognises a redelivery. At-least-once means
        // exactly that, and the publisher is entitled to a second copy.
        let expected_copies = if qos == QoS::ExactlyOnce { 1 } else { attempts };

        // What a dispatch that happened is answered with.
        let dispatched = if wanted {
            ReasonCode::Success
        } else {
            ReasonCode::NoMatchingSubscribers
        };
        let mut expected_answers = Vec::new();
        if qos != QoS::AtMostOnce {
            expected_answers.push(dispatched);
            if redelivered {
                // An exactly-once redelivery is not dispatched again, so
                // the broker learns nothing new about who wanted it and
                // must not claim nobody did: the first copy may well
                // have reached a crowd.
                expected_answers.push(if qos == QoS::ExactlyOnce {
                    ReasonCode::Success
                } else {
                    dispatched
                });
            }
        }

        prop_assert_eq!(
            answers(&out, author.subscriber, qos),
            expected_answers,
            "filter {:?} topic {:?} wanted {}",
            filter,
            topic,
            wanted
        );
        prop_assert_eq!(
            copies(&out, &topic, watcher.subscriber),
            if wanted { expected_copies } else { 0 },
            "a redelivery already held must not reach a subscriber twice"
        );
        for flag in retain_flags(&out, &topic, watcher.subscriber) {
            prop_assert_eq!(
                flag,
                retain && retain_as_published,
                "the live copy carries the flag only when the subscriber asked for it"
            );
        }

        if qos == QoS::ExactlyOnce {
            prop_assert_eq!(
                reserved(&broker, author.subscriber),
                1,
                "an exactly-once slot is held until the release"
            );
            out.clear();
            broker
                .handle(author.subscriber, &Packet::PubRel(release(9)), &mut out)
                .expect("a valid release");
            prop_assert!(
                out.iter().any(|(to, action)| *to == author.subscriber
                    && matches!(action, Action::Send(Packet::PubComp(_)))),
                "the exchange is completed"
            );
        }
        prop_assert_eq!(
            reserved(&broker, author.subscriber),
            0,
            "no slot is left reserved"
        );

        // The retained copy, which is the only way to see that the flag
        // reached the store rather than only the wire. Subscribed to the
        // exact topic, so this half does not depend on the match above.
        let late = broker.connect("", &connect_packet("late", 0), &mut out);
        let mut late_request = subscribe_packet(&topic, QoS::AtMostOnce);
        late_request.filters[0].retain_handling = RetainHandling::SendAlways;
        out.clear();
        broker
            .handle(late.subscriber, &Packet::Subscribe(late_request), &mut out)
            .expect("a valid subscribe");
        prop_assert_eq!(
            copies(&out, &topic, late.subscriber),
            usize::from(retain),
            "a retained message is kept exactly when the flag was set"
        );
    }

    /// A session that comes back is handed everything it still owes.
    ///
    /// Messages published to an absent client wait in its queue, and
    /// nothing pumps that queue until the connection returns. This is the
    /// only property that drives the outbound window and queue end to
    /// end; every other one subscribes at the lowest level, where a
    /// message goes straight out and neither is touched.
    #[test]
    fn a_resumed_session_redelivers_what_it_still_owes(
        pending in 1usize..5,
    ) {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let connect = durable_connect("sub", 600);
        let subscriber = broker.connect("", &connect, &mut out);
        broker
            .handle(
                subscriber.subscriber,
                &Packet::Subscribe(subscribe_packet("a/b", QoS::AtLeastOnce)),
                &mut out,
            )
            .expect("a valid subscribe");
        broker.disconnect(subscriber.subscriber, &mut out);

        let author = broker.connect("", &connect_packet("author", 0), &mut out);
        out.clear();
        for attempt in 0..pending {
            let packet_id = u16::try_from(attempt).expect("a small count") + 1;
            broker
                .handle(
                    author.subscriber,
                    &Packet::Publish(publish_packet(
                        "a/b",
                        QoS::AtLeastOnce,
                        Some(packet_id),
                        false,
                        false,
                    )),
                    &mut out,
                )
                .expect("a valid publish");
        }
        prop_assert_eq!(
            copies(&out, "a/b", subscriber.subscriber),
            0,
            "nothing reaches a client that is not there"
        );

        out.clear();
        let resumed = broker.connect("", &connect, &mut out);
        prop_assert!(resumed.session_present, "the session was kept");
        prop_assert_eq!(
            copies(&out, "a/b", subscriber.subscriber),
            pending,
            "and everything it owes goes out on the resume"
        );
    }
}

/// A takeover disarms the incumbent's keep-alive.
///
/// The displaced connection never disconnected, so nothing else takes it
/// out of the ring. Left there it goes quiet, because it is gone, and
/// the ring expires it under the handle the newcomer now holds: the
/// replacement is closed for a silence that was never its own, and the
/// client reconnects into the same trap.
///
/// Asserted by *when* the close arrives rather than by its absence. The
/// newcomer is silent too and is legitimately closed at second sixteen;
/// the stale entry would have closed it at second two. An assertion that
/// no close ever arrives would pass just as well against a broker with
/// no keep-alive at all.
#[test]
fn a_takeover_disarms_the_incumbents_keep_alive() {
    let mut broker = Broker::new(BrokerConfig::default());
    let mut out = Vec::new();

    let mut incumbent = durable_connect("same", 600);
    incumbent.keep_alive = 1;
    broker.connect("", &incumbent, &mut out);

    let mut newcomer = durable_connect("same", 600);
    newcomer.keep_alive = TEN_SECOND_KEEP_ALIVE;
    broker.connect("", &newcomer, &mut out);

    assert_eq!(
        first_timeout(&mut broker, 40),
        Some(CLOSED_AT),
        "the newcomer was closed on the displaced connection's clock"
    );
}

/// A keep-alive of ten seconds, whose limit is one and a half times
/// that: fifteen seconds of silence are tolerated and the sixteenth is
/// not.
const TEN_SECOND_KEEP_ALIVE: u16 = 10;

/// The second at which a client last heard from at zero, holding
/// [`TEN_SECOND_KEEP_ALIVE`], is closed.
const CLOSED_AT: u32 = 16;

/// Tick up to `seconds` times, returning the second at which the first
/// keep-alive timeout was issued.
fn first_timeout(broker: &mut Broker, seconds: u32) -> Option<u32> {
    let mut out = Vec::new();
    for second in 1..=seconds {
        out.clear();
        broker.tick(&mut out);
        if out
            .iter()
            .any(|(_, action)| matches!(action, Action::Close(ReasonCode::KeepAliveTimeout)))
        {
            return Some(second);
        }
    }
    None
}

/// A client that keeps talking is never closed.
///
/// The positive half of the keep-alive: this is the test that fails when
/// the activity touch stops happening, because then the ring judges a
/// chatty client by a timestamp that never moves.
#[test]
fn a_client_that_pings_is_never_closed() {
    let mut broker = Broker::new(BrokerConfig::default());
    let mut out = Vec::new();

    let mut connect = durable_connect("chatty", 600);
    connect.keep_alive = TEN_SECOND_KEEP_ALIVE;
    let client = broker.connect("", &connect, &mut out);

    for _ in 1..=(CLOSED_AT * 4) {
        out.clear();
        broker
            .handle(client.subscriber, &Packet::PingReq, &mut out)
            .expect("a ping is always valid");
        broker.tick(&mut out);
        assert!(
            !out.iter()
                .any(|(_, action)| matches!(action, Action::Close(ReasonCode::KeepAliveTimeout))),
            "a client that answers is not silent"
        );
    }
    assert!(
        broker
            .session(client.subscriber)
            .is_some_and(|session| session.is_connected()),
        "and it is still attached"
    );
}

/// A client that stops talking is closed, on the second the limit says.
///
/// The second is asserted rather than the eventual outcome. A ring that
/// closed everyone immediately, or a sweep that found them a minute
/// late, would both satisfy "it is closed eventually".
#[test]
fn a_silent_client_is_closed_when_its_keep_alive_runs_out() {
    let mut broker = Broker::new(BrokerConfig::default());
    let mut out = Vec::new();

    let mut connect = durable_connect("silent", 600);
    connect.keep_alive = TEN_SECOND_KEEP_ALIVE;
    let client = broker.connect("", &connect, &mut out);

    assert_eq!(
        first_timeout(&mut broker, 60),
        Some(CLOSED_AT),
        "one and a half times ten is fifteen, and the sixteenth second is too late"
    );
    assert!(
        broker
            .session(client.subscriber)
            .is_some_and(|session| !session.is_connected()),
        "and the session knows its connection is gone"
    );
}

/// A client that vanishes leaves its will behind.
///
/// A keep-alive timeout is the specification's own example of a
/// situation that publishes one: the client "fails to communicate
/// within the Keep Alive time". So the departure must be the abrupt
/// kind. Closing the connection gracefully would discard the will and
/// nothing else in the suite would notice.
#[test]
fn a_silent_client_fires_its_will() {
    let (mut broker, watcher) = broker_with_a_watcher();
    let mut out = Vec::new();

    let mut connect = durable_connect("device", 600);
    connect.keep_alive = TEN_SECOND_KEEP_ALIVE;
    with_will(&mut connect, Some(0));
    broker.connect("", &connect, &mut out);

    let mut fired_at = None;
    for second in 1..=60u32 {
        out.clear();
        broker.tick(&mut out);
        if copies(&out, "gone/device", watcher) > 0 {
            fired_at = Some(second);
            break;
        }
    }

    assert_eq!(
        fired_at,
        Some(CLOSED_AT),
        "the will goes out on the tick that noticed the silence, not a tick later"
    );
}

/// A will owed by a session that expires on the same tick still goes
/// out.
///
/// This is what pins the sweep order in `tick`. A will falls due at the
/// session expiry interval when the delay is longer, because a will
/// outliving its session is pointless, so the two land on the same
/// second by construction rather than by luck. Sweep the expiries first
/// and the session is destroyed with its will still inside it, which is
/// silent: no action, no drop, no message.
#[test]
fn a_will_due_as_its_session_expires_still_goes_out() {
    let (mut broker, watcher) = broker_with_a_watcher();
    let mut out = Vec::new();

    // Five seconds of session, ten of delay. The delay is capped by the
    // expiry, so both fall due at second five.
    let mut connect = durable_connect("device", 5);
    with_will(&mut connect, Some(10));
    let device = broker.connect("", &connect, &mut out);
    broker.disconnect(device.subscriber, &mut out);

    let mut fired_at = None;
    for second in 1..=20u32 {
        out.clear();
        broker.tick(&mut out);
        if copies(&out, "gone/device", watcher) > 0 {
            fired_at = Some(second);
        }
    }

    assert_eq!(fired_at, Some(5), "the will went out as the session ended");
    assert_eq!(
        broker.session_count(),
        1,
        "and the session itself is gone, leaving only the watcher"
    );
}

/// A takeover closes the incumbent's connection without a normal
/// disconnection, which is one of the situations [MQTT-3.1.2-8] names,
/// so its will is armed. Whether it then goes out is decided by
/// [MQTT-3.1.3-9]: a new connection *to this session* before the delay
/// has passed suppresses it.
///
/// The four cases below are the whole of that rule. Each one is a point
/// rather than a range, so each is its own deterministic test: a
/// generator over the delay would only make the boundary harder to read.
#[test]
fn a_takeover_publishes_a_will_with_no_delay() {
    // The default delay of zero has already passed by the time the
    // connection closes, so nothing suppresses it.
    let (mut broker, watcher) = broker_with_a_watcher();
    let mut out = Vec::new();

    let mut incumbent = durable_connect("device", 600);
    with_will(&mut incumbent, Some(0));
    broker.connect("", &incumbent, &mut out);

    out.clear();
    broker.connect("", &durable_connect("device", 600), &mut out);

    assert_eq!(
        copies(&out, "gone/device", watcher),
        1,
        "a displaced connection with no delay publishes its will at once"
    );
}

#[test]
fn a_takeover_suppresses_a_will_that_is_still_delayed() {
    let (mut broker, watcher) = broker_with_a_watcher();
    let mut out = Vec::new();

    let mut incumbent = durable_connect("device", 600);
    with_will(&mut incumbent, Some(30));
    broker.connect("", &incumbent, &mut out);

    out.clear();
    broker.connect("", &durable_connect("device", 600), &mut out);
    assert_eq!(
        copies(&out, "gone/device", watcher),
        0,
        "[MQTT-3.1.3-9]: a new connection to the session before the delay"
    );

    // And it stays suppressed, rather than merely being deferred.
    for _ in 0..60 {
        out.clear();
        broker.tick(&mut out);
        assert_eq!(copies(&out, "gone/device", watcher), 0, "still suppressed");
    }
}

#[test]
fn a_takeover_publishes_a_will_under_protocol_3_1_1() {
    // 3.1.1 has no will delay and its [MQTT-3.1.2-8] is unconditional:
    // the will is published when the connection is closed, full stop.
    let (mut broker, watcher) = broker_with_a_watcher();
    let mut out = Vec::new();

    let mut incumbent = durable_connect("device", 600);
    incumbent.version = Version::V311;
    // A delay this long would suppress the will in MQTT 5. The version
    // is what must decide, not the number.
    with_will(&mut incumbent, Some(30));
    broker.connect("", &incumbent, &mut out);

    out.clear();
    broker.connect("", &durable_connect("device", 600), &mut out);

    assert_eq!(
        copies(&out, "gone/device", watcher),
        1,
        "3.1.1 knows no delay, so a takeover always publishes"
    );
}

#[test]
fn a_clean_start_takeover_publishes_a_will_whatever_its_delay() {
    // [MQTT-3.1.2-8] fires the will when the delay elapses *or the
    // session ends*, and a clean start ends the incumbent's session
    // rather than resuming it. [MQTT-3.1.3-9] does not hold it back,
    // because its subject is a new connection "to this Session" and
    // this one is not.
    let (mut broker, watcher) = broker_with_a_watcher();
    let mut out = Vec::new();

    let mut incumbent = durable_connect("device", 600);
    with_will(&mut incumbent, Some(30));
    broker.connect("", &incumbent, &mut out);

    let mut newcomer = durable_connect("device", 600);
    newcomer.clean_start = true;
    out.clear();
    broker.connect("", &newcomer, &mut out);

    assert_eq!(
        copies(&out, "gone/device", watcher),
        1,
        "the session ended, so the delay had nothing left to wait for"
    );
}

#[test]
fn a_takeover_does_not_publish_the_newcomers_will() {
    // The ordering this whole restructure exists for. The incumbent's
    // departure must be processed before `set_will` replaces it, or the
    // broker publishes the wrong message, or none at all.
    let (mut broker, watcher) = broker_with_a_watcher();
    let mut out = Vec::new();

    let mut incumbent = durable_connect("device", 600);
    with_will(&mut incumbent, Some(0));
    incumbent.will.as_mut().expect("just set").payload = Bytes::from_static(b"the first");
    broker.connect("", &incumbent, &mut out);

    let mut newcomer = durable_connect("device", 600);
    with_will(&mut newcomer, Some(0));
    newcomer.will.as_mut().expect("just set").payload = Bytes::from_static(b"the second");
    out.clear();
    broker.connect("", &newcomer, &mut out);

    let payloads: Vec<Bytes> = out
        .iter()
        .filter_map(|(to, action)| match action {
            Action::Send(Packet::Publish(publish))
                if *to == watcher && publish.topic == "gone/device" =>
            {
                Some(publish.payload.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        payloads,
        vec![Bytes::from_static(b"the first")],
        "the will that went out is the one being displaced"
    );
}

/// Queue `count` at-least-once messages for a session that is not
/// there, returning its handle. The session is durable so the messages
/// wait rather than being refused.
fn a_session_holding(broker: &mut Broker, expiry: u32, count: usize) -> SubscriberId {
    let mut out = Vec::new();
    let subscriber = broker.connect("", &durable_connect("holder", expiry), &mut out);
    broker
        .handle(
            subscriber.subscriber,
            &Packet::Subscribe(subscribe_packet("a/b", QoS::AtLeastOnce)),
            &mut out,
        )
        .expect("a valid subscribe");
    broker.disconnect(subscriber.subscriber, &mut out);

    let author = broker.connect("", &connect_packet("author", 0), &mut out);
    for attempt in 0..count {
        let packet_id = u16::try_from(attempt).expect("a small count") + 1;
        broker
            .handle(
                author.subscriber,
                &Packet::Publish(publish_packet(
                    "a/b",
                    QoS::AtLeastOnce,
                    Some(packet_id),
                    false,
                    false,
                )),
                &mut out,
            )
            .expect("a valid publish");
    }
    subscriber.subscriber
}

/// Every message discarded because a session ended, in order.
fn session_ended_drops(out: &[(SubscriberId, Action)], subscriber: SubscriberId) -> Vec<Arc<str>> {
    out.iter()
        .filter(|(to, _)| *to == subscriber)
        .filter_map(|(_, action)| match action {
            Action::Dropped {
                reason: DropReason::SessionEnded,
                topic,
                ..
            } => Some(Arc::clone(topic)),
            _ => None,
        })
        .collect()
}

/// A session destroyed by expiry says what it was still holding.
///
/// Three messages went into its queue and no client will ever see them.
/// That is correct. Letting them vanish with no action, so that nothing
/// counts them and no operator can answer where they went, is not.
#[test]
fn an_expiring_session_reports_what_it_discards() {
    let mut broker = Broker::new(BrokerConfig::default());
    let holder = a_session_holding(&mut broker, 5, 3);

    let mut out = Vec::new();
    let mut drops = Vec::new();
    for _ in 0..10 {
        out.clear();
        broker.tick(&mut out);
        drops.extend(session_ended_drops(&out, holder));
    }

    assert_eq!(
        drops.len(),
        3,
        "one report per message the destroyed session was holding"
    );
    assert!(
        drops.iter().all(|topic| &**topic == "a/b"),
        "and each says which message went"
    );
    assert_eq!(broker.session_count(), 1, "only the author is left");
}

/// A session replaced by a clean start says the same.
///
/// The registry swaps the session out from under the handle, which is
/// the same destruction by another route and just as silent.
#[test]
fn a_clean_start_reports_what_it_discards() {
    let mut broker = Broker::new(BrokerConfig::default());
    let holder = a_session_holding(&mut broker, 600, 3);

    let mut out = Vec::new();
    let mut fresh = durable_connect("holder", 600);
    fresh.clean_start = true;
    broker.connect("", &fresh, &mut out);

    assert_eq!(
        session_ended_drops(&out, holder).len(),
        3,
        "a clean start discards the old session's queue, and says so"
    );
    assert_eq!(
        copies(&out, "a/b", holder),
        0,
        "and certainly does not deliver it to the new session"
    );
}

/// A will carries the properties its client set on it.
///
/// Section 3.1.3.2: the will properties "define the Application Message
/// properties to be sent with the Will Message when it is published,
/// and properties which define when to publish". The first kind travel;
/// the second kind, which is the delay alone, must not, because a
/// publish does not permit a will delay and the encoder refuses to
/// write one.
#[test]
fn a_will_carries_its_properties_but_not_its_delay() {
    let (mut broker, watcher) = broker_with_a_watcher();
    let mut out = Vec::new();

    let mut connect = durable_connect("device", 600);
    connect.will = Some(Will {
        topic: "gone/device".into(),
        payload: Bytes::from_static(b"offline"),
        qos: QoS::AtMostOnce,
        retain: false,
        properties: Properties {
            will_delay_interval: Some(0),
            message_expiry_interval: Some(120),
            content_type: Some("text/plain".into()),
            response_topic: Some("gone/ack".into()),
            correlation_data: Some(Bytes::from_static(b"cid")),
            payload_format_indicator: Some(1),
            user_properties: vec![("why".into(), "it vanished".into())],
            ..Properties::default()
        },
    });
    let device = broker.connect("", &connect, &mut out);
    broker.disconnect(device.subscriber, &mut out);

    out.clear();
    broker.tick(&mut out);

    let published = out
        .iter()
        .find_map(|(to, action)| match action {
            Action::Send(Packet::Publish(publish))
                if *to == watcher && publish.topic == "gone/device" =>
            {
                Some(publish.clone())
            }
            _ => None,
        })
        .expect("the will went out");

    assert_eq!(
        published.properties.content_type.as_deref(),
        Some("text/plain")
    );
    assert_eq!(
        published.properties.response_topic.as_deref(),
        Some("gone/ack")
    );
    assert_eq!(
        published.properties.correlation_data,
        Some(Bytes::from_static(b"cid"))
    );
    assert_eq!(published.properties.payload_format_indicator, Some(1));
    assert_eq!(published.properties.message_expiry_interval, Some(120));
    assert_eq!(
        published.properties.user_properties,
        vec![("why".to_string(), "it vanished".to_string())]
    );
    assert_eq!(
        published.properties.will_delay_interval, None,
        "the delay says when to publish, not what to publish"
    );
}

/// A will's message expiry interval becomes a real deadline.
///
/// The half `will_message` cannot do, because turning an interval into
/// a deadline needs a clock and the session layer reads none. Asserted
/// through the retained store, where a deadline is observable: the will
/// is there before it and gone after.
#[test]
fn a_retained_will_expires_on_the_interval_it_carries() {
    let mut broker = Broker::new(BrokerConfig::default());
    let mut out = Vec::new();

    let mut connect = durable_connect("device", 600);
    connect.will = Some(Will {
        topic: "gone/device".into(),
        payload: Bytes::from_static(b"offline"),
        qos: QoS::AtMostOnce,
        retain: true,
        properties: Properties {
            will_delay_interval: Some(0),
            message_expiry_interval: Some(5),
            ..Properties::default()
        },
    });
    let device = broker.connect("", &connect, &mut out);
    broker.disconnect(device.subscriber, &mut out);
    broker.tick(&mut out);

    assert!(
        late_subscriber_sees_the_retained_will(&mut broker, "early"),
        "retained and within its five seconds"
    );
    for _ in 0..10 {
        out.clear();
        broker.tick(&mut out);
    }
    assert!(
        !late_subscriber_sees_the_retained_will(&mut broker, "late"),
        "the interval turned into a deadline and the deadline passed"
    );
}

/// Whether a client subscribing now receives the retained will.
fn late_subscriber_sees_the_retained_will(broker: &mut Broker, client: &str) -> bool {
    let mut out = Vec::new();
    let late = broker.connect("", &connect_packet(client, 0), &mut out);
    let mut request = subscribe_packet("gone/device", QoS::AtMostOnce);
    request.filters[0].retain_handling = RetainHandling::SendAlways;
    out.clear();
    broker
        .handle(late.subscriber, &Packet::Subscribe(request), &mut out)
        .expect("a valid subscribe");
    copies(&out, "gone/device", late.subscriber) > 0
}

/// Send a disconnect packet carrying `reason`, then tick `seconds`
/// times, reporting whether the will went out.
fn disconnects_with(reason: ReasonCode, version: Version) -> bool {
    let (mut broker, watcher) = broker_with_a_watcher();
    let mut out = Vec::new();

    let mut connect = durable_connect("device", 600);
    connect.version = version;
    with_will(&mut connect, Some(0));
    let device = broker.connect("", &connect, &mut out);

    broker
        .handle(
            device.subscriber,
            &Packet::Disconnect(Disconnect {
                reason,
                properties: Properties::default(),
            }),
            &mut out,
        )
        .expect("a disconnect is always accepted");

    for _ in 0..10 {
        out.clear();
        broker.tick(&mut out);
        if copies(&out, "gone/device", watcher) > 0 {
            return true;
        }
    }
    false
}

/// A normal disconnection discards the will.
///
/// [MQTT-3.14.4-3]: "On receipt of DISCONNECT with a Reason Code of
/// 0x00 (Success) the Server MUST discard any Will Message associated
/// with the current Connection without publishing it."
#[test]
fn a_normal_disconnection_discards_the_will() {
    assert!(!disconnects_with(ReasonCode::Success, Version::V5));
}

/// Any other reason leaves it standing, and 0x04 exists to ask for it.
///
/// Only 0x00 deletes the will, so a disconnect carrying anything else
/// is not the receipt that does. Section 3.1.2.5's own example of the
/// use is a client arranging to announce its own session expiry by
/// "sending DISCONNECT with Reason Code 0x04".
#[test]
fn a_disconnection_asking_for_the_will_publishes_it() {
    assert!(disconnects_with(
        ReasonCode::DisconnectWithWillMessage,
        Version::V5
    ));
}

/// MQTT 3.1.1's disconnect carries no reason at all, so its rule is
/// unconditional: every disconnect packet discards the will.
#[test]
fn a_3_1_1_disconnection_always_discards_the_will() {
    assert!(!disconnects_with(ReasonCode::Success, Version::V311));
    assert!(
        !disconnects_with(ReasonCode::DisconnectWithWillMessage, Version::V311),
        "3.1.1 has no reason code to read, whatever a caller passes"
    );
}
