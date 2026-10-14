//! Access control through the connection: what a client is refused.

use std::sync::Arc;

use broker_core::broker::{Broker, BrokerConfig};
use broker_core::session::queue::DropReason;
use bytes::BytesMut;
use cluster::{HybridClock, NodeId, Replicated, DEFAULT_MAX_DRIFT_MILLIS};
use directory::{AclRecord, Directory, RulesInForce, Settings};
use mqtt_codec::packet::{Connect, Publish, Subscribe, SubscriptionOptions};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};
use sdmqtt_server::connection::{serve, ConnectionConfig};
use sdmqtt_server::shared::Shared;
use storage::{MemoryStorage, Storage, Writer};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

fn rule(
    id: &str,
    order: u32,
    subject: &str,
    filter: &str,
    publish: Option<bool>,
    subscribe: Option<bool>,
) -> AclRecord {
    AclRecord {
        id: id.to_owned(),
        order,
        subject: subject.to_owned(),
        filter: filter.to_owned(),
        publish,
        subscribe,
    }
}

/// A broker with the directory and these configured rules in force.
fn broker_with(configured: Vec<AclRecord>) -> (Arc<Shared>, Arc<Directory>) {
    let store = Arc::new(MemoryStorage::new());
    let writer = Arc::new(
        Writer::spawn(
            Arc::clone(&store) as Arc<dyn Storage>,
            storage::WriterConfig::default(),
        )
        .expect("a writer thread"),
    );
    let replicated = Arc::new(
        Replicated::open(
            NodeId::mint(),
            HybridClock::resume(None, DEFAULT_MAX_DRIFT_MILLIS),
            &storage::Restored::default(),
        )
        .expect("an empty store holds nothing to refuse"),
    );
    let shared = Arc::new(Shared::with_replication(
        Broker::new(BrokerConfig::default()),
        Arc::clone(&writer),
        Arc::clone(&replicated),
    ));
    let directory = Directory::new(
        writer,
        Arc::clone(&shared),
        replicated,
        Settings::default(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let in_force = RulesInForce::new(configured, Some(Arc::clone(&directory)));
    shared.with_broker(|broker| {
        broker.hooks_mut().install(directory.hook());
        broker.hooks_mut().install(in_force.hook());
    });
    (shared, directory)
}

struct Client {
    stream: DuplexStream,
    buf: BytesMut,
    version: Version,
}

impl Client {
    async fn connect(
        shared: &Arc<Shared>,
        version: Version,
        client_id: &str,
        username: Option<&str>,
    ) -> Self {
        let (stream, server) = tokio::io::duplex(8192);
        let shared_for_server = Arc::clone(shared);
        tokio::spawn(async move {
            let _ = serve(server, shared_for_server, ConnectionConfig::default()).await;
        });
        let mut client = Self {
            stream,
            buf: BytesMut::new(),
            version,
        };
        client
            .send(&Packet::Connect(Box::new(Connect {
                version,
                client_id: client_id.into(),
                clean_start: true,
                keep_alive: 0,
                username: username.map(str::to_owned),
                password: username.map(|_| bytes::Bytes::from_static(b"hunter2")),
                will: None,
                properties: Properties::default(),
            })))
            .await;
        match client.next().await {
            Some(Packet::ConnAck(ack)) => assert_eq!(ack.reason, ReasonCode::Success),
            other => panic!("expected a connack, got {other:?}"),
        }
        client
    }

    async fn send(&mut self, packet: &Packet) {
        let mut out = BytesMut::new();
        encode(packet, self.version, &mut out).expect("encodes");
        self.stream.write_all(&out).await.expect("written");
    }

    async fn next(&mut self) -> Option<Packet> {
        loop {
            if let Some(packet) =
                decode(&mut self.buf, self.version, &Limits::default()).expect("valid")
            {
                return Some(packet);
            }
            let mut chunk = [0u8; 1024];
            let read = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.stream.read(&mut chunk),
            )
            .await
            .expect("an answer within five seconds")
            .expect("a read");
            if read == 0 {
                return None;
            }
            self.buf.extend_from_slice(&chunk[..read]);
        }
    }

    async fn subscribe(&mut self, filter: &str) -> Vec<ReasonCode> {
        self.send(&Packet::Subscribe(Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: filter.to_owned(),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: mqtt_codec::packet::RetainHandling::DoNotSend,
            }],
            properties: Properties::default(),
        }))
        .await;
        match self.next().await {
            Some(Packet::SubAck(ack)) => ack.reasons,
            other => panic!("expected a suback, got {other:?}"),
        }
    }

    async fn publish(&mut self, topic: &str, qos: QoS) -> Option<ReasonCode> {
        self.send(&Packet::Publish(Publish {
            dup: false,
            qos,
            retain: false,
            topic: topic.to_owned(),
            packet_id: (qos != QoS::AtMostOnce).then_some(7),
            payload: bytes::Bytes::from_static(b"x"),
            properties: Properties::default(),
        }))
        .await;
        if qos == QoS::AtMostOnce {
            return None;
        }
        loop {
            match self.next().await {
                Some(Packet::PubAck(ack)) => return Some(ack.reason),
                Some(Packet::PubRec(rec)) => return Some(rec.reason),
                // A delivery of our own publish, when we subscribed to a
                // filter it matches: acknowledged and skipped.
                Some(Packet::Publish(publish)) => {
                    if let Some(packet_id) = publish.packet_id {
                        self.send(&Packet::PubAck(mqtt_codec::packet::Ack {
                            packet_id,
                            reason: ReasonCode::Success,
                            properties: Properties::default(),
                        }))
                        .await;
                    }
                }
                other => panic!("expected an acknowledgement, got {other:?}"),
            }
        }
    }
}

/// With no rule, everything is allowed, as before this milestone.
#[tokio::test]
async fn with_no_rules_everything_is_allowed() {
    let (shared, _) = broker_with(Vec::new());
    let mut client = Client::connect(&shared, Version::V5, "c", None).await;
    assert_eq!(
        client.subscribe("anything/#").await,
        vec![ReasonCode::GrantedQos1]
    );
    // Delivered to itself, which is what the subscription above makes it.
    assert_eq!(
        client.publish("anything/goes", QoS::AtLeastOnce).await,
        Some(ReasonCode::Success)
    );
}

/// One configured rule denies all it does not allow: a subscribe is
/// 0x87 on version 5 and 0x80 on 3.1.1, a publish is 0x87 on its
/// acknowledgement, and an at-most-once publish is dropped and counted.
#[tokio::test]
async fn a_rule_refuses_what_it_does_not_allow_on_both_versions() {
    let (shared, _) = broker_with(vec![rule(
        "fleet",
        1,
        "anyone",
        "fleet/#",
        Some(true),
        Some(true),
    )]);
    let mut client = Client::connect(&shared, Version::V5, "c", None).await;
    assert_eq!(
        client.subscribe("fleet/+/temp").await,
        vec![ReasonCode::GrantedQos1]
    );
    assert_eq!(
        client.subscribe("office/#").await,
        vec![ReasonCode::NotAuthorized]
    );
    // A shared subscription is judged by the filter it shares, not by
    // the `$share/<group>/` it carries: a rule about `fleet/#` is a rule
    // about `$share/g/fleet/#` too, and `#` never covers a dollar topic.
    assert_eq!(
        client.subscribe("$share/g/fleet/+/temp").await,
        vec![ReasonCode::GrantedQos1]
    );
    assert_eq!(
        client.subscribe("$share/g/office/#").await,
        vec![ReasonCode::NotAuthorized]
    );
    assert_eq!(
        client.publish("fleet/7/temp", QoS::AtLeastOnce).await,
        Some(ReasonCode::Success)
    );
    assert_eq!(
        client.publish("office/light", QoS::AtLeastOnce).await,
        Some(ReasonCode::NotAuthorized)
    );
    assert_eq!(
        client.publish("office/light", QoS::ExactlyOnce).await,
        Some(ReasonCode::NotAuthorized)
    );

    let before = shared.metrics().message_dropped(DropReason::Refused).get();
    client.publish("office/light", QoS::AtMostOnce).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while shared.metrics().message_dropped(DropReason::Refused).get() == before {
        assert!(
            std::time::Instant::now() < deadline,
            "the refused QoS 0 publish was never counted"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let mut old = Client::connect(&shared, Version::V311, "old", None).await;
    assert_eq!(
        old.subscribe("office/#").await,
        vec![ReasonCode::UnspecifiedError],
        "3.1.1's one failure code, 0x80"
    );
    assert_eq!(
        old.subscribe("fleet/#").await,
        vec![ReasonCode::GrantedQos1]
    );
}

/// A rule for a user covers connections authenticated as that user and
/// nobody else; a rule written through the directory joins the file's.
#[tokio::test]
async fn a_users_rule_covers_that_user_and_a_directory_rule_takes_effect_at_once() {
    let (shared, directory) = broker_with(vec![rule(
        "all",
        100,
        "anyone",
        "#",
        Some(true),
        Some(true),
    )]);
    directory
        .put_user("alice", &storage::record::Secret::new("hunter2"), false)
        .await
        .expect("created");
    directory
        .put_acl(rule(
            "alice-only",
            10,
            "user:alice",
            "private/#",
            Some(true),
            Some(true),
        ))
        .await
        .expect("written");
    directory
        .put_acl(rule(
            "no-private",
            20,
            "anyone",
            "private/#",
            Some(false),
            Some(false),
        ))
        .await
        .expect("written");
    let mut alice = Client::connect(&shared, Version::V5, "a", Some("alice")).await;
    assert_eq!(
        alice.subscribe("private/diary").await,
        vec![ReasonCode::GrantedQos1]
    );
    let mut anon = Client::connect(&shared, Version::V5, "b", None).await;
    assert_eq!(
        anon.subscribe("private/diary").await,
        vec![ReasonCode::NotAuthorized]
    );
    assert_eq!(
        anon.subscribe("public/news").await,
        vec![ReasonCode::GrantedQos1]
    );
    directory.delete_acl("no-private").await.expect("removed");
    assert_eq!(
        anon.subscribe("private/diary").await,
        vec![ReasonCode::GrantedQos1],
        "the fallback allows it again"
    );
}

/// Section 12.3: a client never publishes into the dollar namespace,
/// rules or no rules.
#[tokio::test]
async fn a_dollar_topic_is_refused_to_a_client_whatever_the_rules_say() {
    let (shared, _) = broker_with(Vec::new());
    let mut client = Client::connect(&shared, Version::V5, "c", None).await;
    assert_eq!(
        client.publish("$SYS/anything", QoS::AtLeastOnce).await,
        Some(ReasonCode::NotAuthorized)
    );
    assert_eq!(
        client.publish("a/$b", QoS::AtLeastOnce).await,
        Some(ReasonCode::NoMatchingSubscribers),
        "only the first level is reserved"
    );
    let before = shared.metrics().message_dropped(DropReason::Refused).get();
    client.publish("$SYS/anything", QoS::AtMostOnce).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while shared.metrics().message_dropped(DropReason::Refused).get() == before {
        assert!(
            std::time::Instant::now() < deadline,
            "the refused QoS 0 publish was never counted"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let (open, _) = broker_with(vec![rule("all", 1, "anyone", "#", Some(true), Some(true))]);
    let mut client = Client::connect(&open, Version::V5, "c", None).await;
    assert_eq!(
        client.publish("$SYS/anything", QoS::AtLeastOnce).await,
        Some(ReasonCode::NotAuthorized),
        "a rule allowing everything does not reach the namespace"
    );
}
