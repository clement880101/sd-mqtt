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
use broker_core::session::Action;
use broker_core::subscriptions::SubscriberId;
use broker_core::topic::reference;
use bytes::Bytes;
use mqtt_codec::packet::{
    Ack, Connect, Publish, RetainHandling, Subscribe, SubscriptionOptions, Will,
};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode, Version};
use proptest::prelude::*;
use std::collections::HashMap;

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

/// How many acknowledgements of the kind `qos` calls for went to a
/// subscriber: a PUBACK at least-once, a PUBREC exactly-once.
fn acknowledgements(out: &[(SubscriberId, Action)], subscriber: SubscriberId, qos: QoS) -> usize {
    out.iter()
        .filter(|(to, _)| *to == subscriber)
        .filter(|(_, action)| {
            matches!(
                (qos, action),
                (QoS::AtLeastOnce, Action::Send(Packet::PubAck(_)))
                    | (QoS::ExactlyOnce, Action::Send(Packet::PubRec(_)))
            )
        })
        .count()
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
    #[test]
    fn every_received_publish_is_answered_and_frees_its_slot(
        qos in any_qos(),
        retain in any::<bool>(),
        retain_as_published in any::<bool>(),
        redelivered in any::<bool>(),
    ) {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let watcher = broker.connect("", &connect_packet("watcher", 0), &mut out);
        let mut request = subscribe_packet("a/b", QoS::ExactlyOnce);
        request.filters[0].retain_as_published = retain_as_published;
        broker
            .handle(watcher.subscriber, &Packet::Subscribe(request), &mut out)
            .expect("a valid subscribe");

        let author = broker.connect("", &connect_packet("author", 0), &mut out);
        let packet_id = (qos != QoS::AtMostOnce).then_some(9);

        out.clear();
        broker
            .handle(
                author.subscriber,
                &Packet::Publish(publish_packet("a/b", qos, packet_id, retain, false)),
                &mut out,
            )
            .expect("a valid publish");
        if redelivered {
            broker
                .handle(
                    author.subscriber,
                    &Packet::Publish(publish_packet("a/b", qos, packet_id, retain, true)),
                    &mut out,
                )
                .expect("a valid redelivery");
        }

        let attempts = 1 + usize::from(redelivered);
        let expected_answers = if qos == QoS::AtMostOnce { 0 } else { attempts };
        // Only exactly-once recognises a redelivery. At-least-once means
        // exactly that, and the publisher is entitled to a second copy.
        let expected_copies = if qos == QoS::ExactlyOnce { 1 } else { attempts };

        prop_assert_eq!(
            acknowledgements(&out, author.subscriber, qos),
            expected_answers,
            "every attempt is answered, dispatched or not"
        );
        prop_assert_eq!(
            copies(&out, "a/b", watcher.subscriber),
            expected_copies,
            "a redelivery already held must not reach a subscriber twice"
        );
        for flag in retain_flags(&out, "a/b", watcher.subscriber) {
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
        // reached the store rather than only the wire.
        let late = broker.connect("", &connect_packet("late", 0), &mut out);
        let mut late_request = subscribe_packet("a/b", QoS::AtMostOnce);
        late_request.filters[0].retain_handling = RetainHandling::SendAlways;
        out.clear();
        broker
            .handle(late.subscriber, &Packet::Subscribe(late_request), &mut out)
            .expect("a valid subscribe");
        prop_assert_eq!(
            copies(&out, "a/b", late.subscriber),
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
/// client reconnects into the same trap. Deterministic rather than
/// generated, because the case is one point and a range around it would
/// only bury it.
#[test]
fn a_takeover_disarms_the_incumbents_keep_alive() {
    let mut broker = Broker::new(BrokerConfig::default());
    let mut out = Vec::new();

    let mut incumbent = durable_connect("same", 600);
    incumbent.keep_alive = 1;
    let first = broker.connect("", &incumbent, &mut out);

    // The newcomer proposes no deadline at all, so nothing it does or
    // fails to do can legitimately expire it.
    broker.connect("", &durable_connect("same", 600), &mut out);

    out.clear();
    for _ in 0..10 {
        broker.tick(&mut out);
    }

    assert!(
        !out.iter()
            .any(|(_, action)| matches!(action, Action::Close(ReasonCode::KeepAliveTimeout))),
        "the newcomer was closed for the connection it replaced"
    );
    assert!(
        broker
            .session(first.subscriber)
            .is_some_and(|session| session.is_connected()),
        "and left disconnected by it"
    );
}
