//! The ingest endpoint, over real sockets, into a real broker.
//!
//! Section 9.2's `http_in`. What is tested here and not in the module's
//! own tests is everything that only exists once there is a socket: the
//! token as a header, the topic as a header or a query parameter, the
//! status codes, and — the one that matters most — that a post reaches
//! an **MQTT subscriber**, which is what says the body went through the
//! broker rather than somewhere parallel.
//!
//! # Why a subscriber and not the retained store
//!
//! Reading the message back out of the broker would prove that
//! something was written. A subscriber attached through the listener,
//! receiving a PUBLISH it did not ask this process for, proves that the
//! publication went through the matching, the per-session queue and the
//! write path a connection's publish goes through. Convention 5: a test
//! that constructs the type instead of walking the path is not a test
//! of the path.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use broker_core::broker::{Broker, BrokerConfig};
use bytes::BytesMut;
use mqtt_codec::packet::{Connect, Publish, RetainHandling, Subscribe, SubscriptionOptions};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};
use rules::http_in::{largest_body, routes};
use rules::rule::{Action, HttpIn, HttpOut, Rule, RuleId, TopicFromRequest, TopicSource, Trigger};
use rules::Ingest;
use sdmqtt_server::listener::{Running, Server, ServerConfig};
use sdmqtt_server::shared::gated::GatedStore;
use sdmqtt_server::shared::Shared;
use storage::record::Secret;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How long a socket round trip may take before this calls it a
/// failure.
const REPLY: Duration = Duration::from_secs(10);

/// The token every rule below is guarded by.
const TOKEN: &str = "a-secret-for-the-test";

/// Where a rule with one fixed topic publishes.
const FIXED: &str = "ingest/fixed";

/// The prefix a rule that lets a caller choose is bounded by.
const PREFIX: &str = "ingest/open";

// ---- the broker and the endpoint ----------------------------------------

/// A broker on a real MQTT listener, with the ingest routes beside it.
struct Node {
    mqtt: Running,
    ingest: SocketAddr,
    serving: tokio::task::JoinHandle<()>,
}

impl Node {
    /// A node serving `ingest`, over a broker with no store.
    async fn start(ingest: Ingest) -> Self {
        let mqtt = Server::bind(ServerConfig::ephemeral())
            .await
            .expect("the MQTT listener binds");
        let shared = Arc::clone(mqtt.shared());
        let (address, serving) = serve(shared, ingest).await;
        Self {
            mqtt,
            ingest: address,
            serving,
        }
    }

    async fn stop(self) {
        self.serving.abort();
        self.mqtt.shutdown().await;
    }
}

/// Serve the ingest route set on a loopback port, exactly as the binary
/// does: the same routes, the same fallbacks.
async fn serve(shared: Arc<Shared>, ingest: Ingest) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a port on the loopback");
    let address = listener.local_addr().expect("it has an address");
    let router: axum::Router = routes::<()>(shared, Arc::new(ingest))
        .fallback(rules::http_in::not_found)
        .method_not_allowed_fallback(rules::http_in::method_not_allowed);
    let serving = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (address, serving)
}

/// A rule publishing every body to one fixed topic.
fn fixed_topic_rule(qos: QoS) -> Ingest {
    let ingest = Ingest::new();
    ingest.register(
        &RuleId::new("fixed").expect("an identifier"),
        HttpIn {
            token: Secret::new(TOKEN),
            topic: FIXED.to_owned(),
            topic_from: None,
            qos,
            retain: false,
        },
    );
    ingest
}

/// A rule letting the request choose a topic under [`PREFIX`], from
/// `source`.
fn chosen_topic_rule(source: TopicSource) -> Ingest {
    let ingest = Ingest::new();
    ingest.register(
        &RuleId::new("open").expect("an identifier"),
        HttpIn {
            token: Secret::new(TOKEN),
            topic: format!("{PREFIX}/default"),
            topic_from: Some(TopicFromRequest {
                source,
                prefix: PREFIX.to_owned(),
            }),
            qos: QoS::AtMostOnce,
            retain: false,
        },
    );
    ingest
}

// ---- an HTTP client small enough to read --------------------------------

/// What came back.
#[derive(Debug)]
struct Response {
    status: u16,
    body: String,
}

/// One `POST`, with whatever headers and query string were asked for.
///
/// `Connection: close` on every request, so the response ends at end of
/// file and nothing here has to decide when a body is complete.
async fn post(
    address: SocketAddr,
    path: &str,
    token: Option<&str>,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Response {
    request(address, "POST", path, token, headers, body).await
}

/// The same, with a method of its own.
async fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Response {
    let mut stream = TcpStream::connect(address)
        .await
        .expect("the ingest listener accepts");
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\
         Content-Length: {}\r\n",
        body.len()
    );
    if let Some(token) = token {
        head.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream
        .write_all(head.as_bytes())
        .await
        .expect("the head is written");
    stream.write_all(body).await.expect("and the body");
    stream.flush().await.expect("and flushed");

    let mut raw = Vec::new();
    tokio::time::timeout(REPLY, stream.read_to_end(&mut raw))
        .await
        .expect("the response ends")
        .expect("the socket is readable");
    let text = String::from_utf8_lossy(&raw).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in {text:?}"));
    let body = text
        .split_once("\r\n\r\n")
        .map_or(String::new(), |(_, rest)| rest.to_owned());
    Response { status, body }
}

// ---- an MQTT subscriber over a socket -----------------------------------

/// One MQTT 5 client, attached through the listener.
struct Subscriber {
    stream: TcpStream,
    buf: BytesMut,
}

impl Subscriber {
    async fn connect(address: SocketAddr, client_id: &str) -> Self {
        let stream = TcpStream::connect(address)
            .await
            .expect("the broker accepts");
        let mut client = Self {
            stream,
            buf: BytesMut::new(),
        };
        client
            .send(&Packet::Connect(Box::new(Connect {
                version: Version::V5,
                client_id: client_id.into(),
                clean_start: true,
                keep_alive: 0,
                username: None,
                password: None,
                will: None,
                properties: Properties::default(),
            })))
            .await;
        match client.next().await {
            Some(Packet::ConnAck(ack)) => assert_eq!(ack.reason, ReasonCode::Success),
            other => panic!("expected a connect acknowledgement, got {other:?}"),
        }
        client
    }

    async fn subscribe(&mut self, filter: &str) {
        self.send(&Packet::Subscribe(Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: filter.into(),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAlways,
            }],
            properties: Properties::default(),
        }))
        .await;
        match self.next().await {
            Some(Packet::SubAck(ack)) => {
                assert_eq!(ack.reasons, vec![ReasonCode::GrantedQos1], "on {filter}");
            }
            other => panic!("expected a subscribe acknowledgement, got {other:?}"),
        }
    }

    /// The next delivery, or nothing within `patience`.
    async fn delivery_within(&mut self, patience: Duration) -> Option<Publish> {
        let deadline = tokio::time::Instant::now() + patience;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return None;
            }
            match tokio::time::timeout(left, self.next()).await {
                Err(_) => return None,
                Ok(Some(Packet::Publish(publish))) => return Some(publish),
                Ok(Some(_)) => continue,
                Ok(None) => return None,
            }
        }
    }

    async fn send(&mut self, packet: &Packet) {
        let mut out = BytesMut::new();
        encode(packet, Version::V5, &mut out).expect("the packet encodes");
        self.stream
            .write_all(&out)
            .await
            .expect("the write succeeds");
    }

    async fn next(&mut self) -> Option<Packet> {
        loop {
            if let Some(packet) = decode(&mut self.buf, Version::V5, &Limits::default())
                .expect("the broker sends valid packets")
            {
                return Some(packet);
            }
            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(read) => self.buf.extend_from_slice(&chunk[..read]),
            }
        }
    }
}

// ---- the tests ----------------------------------------------------------

/// The one that says the body went through the broker.
///
/// A post with the right token, and an MQTT subscriber attached through
/// the listener receives it on the configured topic with the bytes that
/// were posted.
#[tokio::test(flavor = "multi_thread")]
async fn a_post_with_the_right_token_reaches_an_mqtt_subscriber() {
    let node = Node::start(fixed_topic_rule(QoS::AtMostOnce)).await;
    let mut subscriber = Subscriber::connect(node.mqtt.address(), "watcher").await;
    subscriber.subscribe(FIXED).await;

    let answered = post(
        node.ingest,
        "/ingest/fixed",
        Some(TOKEN),
        &[],
        b"a reading from a webhook",
    )
    .await;
    assert_eq!(answered.status, 200, "{}", answered.body);
    assert!(
        answered.body.contains("\"delivered\":1"),
        "the answer says how many subscribers it reached: {}",
        answered.body
    );

    let delivered = subscriber
        .delivery_within(REPLY)
        .await
        .expect("the subscriber must receive what was posted");
    assert_eq!(delivered.topic, FIXED);
    assert_eq!(&delivered.payload[..], b"a reading from a webhook");
    assert_eq!(delivered.qos, QoS::AtMostOnce);

    node.stop().await;
}

/// The three cases the token rule has, together.
///
/// Convention 5: each on its own passes against a guard that refuses
/// everything and against one that refuses nothing, so all of them are
/// asserted at once. The prefix of the right token is the case an
/// early-returning comparison would be generous to in the one way that
/// matters.
#[tokio::test(flavor = "multi_thread")]
async fn no_token_and_a_wrong_token_are_refused_and_publish_nothing() {
    let node = Node::start(fixed_topic_rule(QoS::AtMostOnce)).await;
    let mut subscriber = Subscriber::connect(node.mqtt.address(), "watcher").await;
    subscriber.subscribe(FIXED).await;

    for token in [None, Some("wrong"), Some("a-secret-for-the-tes")] {
        let refused = post(node.ingest, "/ingest/fixed", token, &[], b"x").await;
        assert_eq!(refused.status, 401, "{token:?}: {}", refused.body);
        assert!(
            refused.body.contains("\"code\":\"unauthorized\""),
            "{token:?}: in the one shape every failure takes: {}",
            refused.body
        );
    }

    // And nothing was published. The refusal has to be before the
    // publish, not a different answer after it.
    assert!(
        subscriber
            .delivery_within(Duration::from_millis(300))
            .await
            .is_none(),
        "a refused post published anyway"
    );

    // The right one still works, so the test above is not passing
    // against an endpoint that refuses everything.
    let accepted = post(node.ingest, "/ingest/fixed", Some(TOKEN), &[], b"x").await;
    assert_eq!(accepted.status, 200, "{}", accepted.body);

    node.stop().await;
}

/// The scheme is matched the way RFC 7235 says: without regard to
/// case. `bearer <token>` presents the token; another scheme presents
/// something that is not it.
#[tokio::test(flavor = "multi_thread")]
async fn the_bearer_scheme_is_read_without_regard_to_case() {
    let node = Node::start(fixed_topic_rule(QoS::AtMostOnce)).await;

    for spelling in ["bearer", "BEARER", "bEaReR"] {
        let authorization = format!("{spelling} {TOKEN}");
        let accepted = post(
            node.ingest,
            "/ingest/fixed",
            None,
            &[("Authorization", &authorization)],
            b"x",
        )
        .await;
        assert_eq!(accepted.status, 200, "{spelling}: {}", accepted.body);
    }

    let basic = format!("Basic {TOKEN}");
    let refused = post(
        node.ingest,
        "/ingest/fixed",
        None,
        &[("Authorization", &basic)],
        b"x",
    )
    .await;
    assert_eq!(refused.status, 401, "{}", refused.body);

    node.stop().await;
}

/// A rule with no token has no route, and the path says so as though it
/// does not exist.
///
/// Not `401`. A tokenless rule must have no route at all rather than an
/// open one, and an identifier with no route answers exactly what an
/// identifier nobody configured answers — so the two cannot be told
/// apart from outside.
#[tokio::test(flavor = "multi_thread")]
async fn a_rule_with_no_token_answers_as_though_it_does_not_exist() {
    let ingest = Ingest::new();
    ingest.register(
        &RuleId::new("tokenless").expect("an identifier"),
        HttpIn {
            token: Secret::new(String::new()),
            topic: FIXED.to_owned(),
            topic_from: None,
            qos: QoS::AtMostOnce,
            retain: false,
        },
    );
    let node = Node::start(ingest).await;
    let mut subscriber = Subscriber::connect(node.mqtt.address(), "watcher").await;
    subscriber.subscribe(FIXED).await;

    // With no token, with a token, and with the empty token that was
    // configured: all the same answer.
    for token in [None, Some("anything"), Some("")] {
        let answered = post(node.ingest, "/ingest/tokenless", token, &[], b"x").await;
        assert_eq!(
            answered.status, 404,
            "{token:?}: a tokenless rule's path must answer as though it does not exist, \
             not as though it is unauthorised: {}",
            answered.body
        );
    }
    // Which is the same answer an identifier nobody configured gets.
    let unknown = post(node.ingest, "/ingest/never-configured", None, &[], b"x").await;
    assert_eq!(unknown.status, 404, "{}", unknown.body);

    assert!(
        subscriber
            .delivery_within(Duration::from_millis(300))
            .await
            .is_none(),
        "a rule with no route published anyway"
    );
    node.stop().await;
}

/// A rule that is not an ingest rule has no ingest route either.
#[tokio::test(flavor = "multi_thread")]
async fn a_rule_that_posts_out_has_no_ingest_route() {
    let out = Rule::new(
        RuleId::new("telemetry").expect("an identifier"),
        "Telemetry out",
        Some(Trigger::new("sensor/+/temp").expect("a filter")),
        Action::HttpOut(HttpOut {
            url: "https://example.invalid/hook".to_owned(),
            ..HttpOut::default()
        }),
    )
    .expect("a named rule");
    let node = Node::start(Ingest::from_rules([&out])).await;

    let answered = post(node.ingest, "/ingest/telemetry", Some(TOKEN), &[], b"x").await;
    assert_eq!(answered.status, 404, "{}", answered.body);
    node.stop().await;
}

/// A topic from a header is used, and one outside the prefix is
/// refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_topic_from_a_header_is_used_and_bounded_by_the_prefix() {
    let node = Node::start(chosen_topic_rule(TopicSource::Header("x-topic".to_owned()))).await;
    let mut subscriber = Subscriber::connect(node.mqtt.address(), "watcher").await;
    subscriber.subscribe("#").await;

    let chosen = format!("{PREFIX}/sensor/1");
    let accepted = post(
        node.ingest,
        "/ingest/open",
        Some(TOKEN),
        &[("x-topic", &chosen)],
        b"inside",
    )
    .await;
    assert_eq!(accepted.status, 200, "{}", accepted.body);
    let delivered = subscriber
        .delivery_within(REPLY)
        .await
        .expect("a topic inside the prefix is published");
    assert_eq!(delivered.topic, chosen);
    assert_eq!(&delivered.payload[..], b"inside");

    // And with no header at all, the configured topic.
    let accepted = post(node.ingest, "/ingest/open", Some(TOKEN), &[], b"default").await;
    assert_eq!(accepted.status, 200, "{}", accepted.body);
    let delivered = subscriber
        .delivery_within(REPLY)
        .await
        .expect("a request that chooses nothing publishes to the configured topic");
    assert_eq!(delivered.topic, format!("{PREFIX}/default"));

    node.stop().await;
}

/// Every spelling of an escape, through the endpoint, publishing
/// nothing.
///
/// The module's own test asserts what [`rules::http_in::is_under`]
/// answers. This asserts what the **endpoint** does with the same
/// spellings, which is the thing an attacker actually reaches: the unit
/// test would pass against a handler that computed the answer and
/// published anyway.
///
/// A subscriber on `#` watches the whole broker, so a topic that
/// escaped to anywhere at all is caught, not only one that escaped to a
/// place the test thought to look.
#[tokio::test(flavor = "multi_thread")]
async fn every_spelling_of_an_escape_is_refused_over_a_socket() {
    let node = Node::start(chosen_topic_rule(TopicSource::Header("x-topic".to_owned()))).await;
    let mut subscriber = Subscriber::connect(node.mqtt.address(), "watcher").await;
    subscriber.subscribe("#").await;

    // Two spellings from the module's own table cannot be posted
    // through a header, and the reason is worth writing down rather
    // than leaving as a gap.
    //
    // A header value may not hold a NUL or a fullwidth solidus: the
    // field is bytes, and hyper refuses a value that is not visible
    // ASCII before this endpoint sees it.
    //
    // And **a leading or trailing space cannot reach a header value at
    // all**, because RFC 9110 says the optional whitespace around a
    // field value is not part of it and every parser strips it. This
    // was found the way things like this are found: the test asserted
    // that `" ingest/open"` was refused, and it was accepted —
    // correctly, because what arrived was `"ingest/open"`, the prefix
    // itself. The endpoint trims nothing; the wire did.
    //
    // Both spellings are expressible through a query parameter, where
    // `%20` and `%00` mean exactly those bytes, and they are asserted
    // there instead. What is left here is what a header can carry.
    let escapes = [
        // The mutation's signature: a string prefix of the prefix.
        "ingest/openx",
        "ingest/openx/sensor/1",
        "ingest/open2",
        // A space makes a different level.
        "ingest/open 1",
        // Shorter than the prefix, and the empty topic.
        "ingest",
        "",
        // The prefix somewhere other than the start.
        "/ingest/open/a",
        "elsewhere/ingest/open/a",
        // Case, which MQTT does not fold.
        "Ingest/Open/a",
        "INGEST/OPEN",
        // An empty level in the middle is a level.
        "ingest//open/a",
        // There is no walking upward.
        "ingest/../ingest/open",
        "../ingest/open",
        // A percent-encoded separator nothing decodes is one level.
        "ingest%2Fopen/a",
        "ingest/open%2Fa",
        // Somewhere else entirely, including the reserved namespace.
        "somewhere/else",
        "$sd/rules/open/dlq",
        "$SYS/broker/uptime",
    ];
    for escape in escapes {
        let refused = post(
            node.ingest,
            "/ingest/open",
            Some(TOKEN),
            &[("x-topic", escape)],
            b"escaped",
        )
        .await;
        assert_eq!(
            refused.status, 400,
            "{escape:?} escaped this rule's prefix: {}",
            refused.body
        );
        assert!(
            refused.body.contains("\"code\":\"invalid_request\""),
            "{escape:?}: in the one shape: {}",
            refused.body
        );
    }

    // A wildcard is under the prefix by level and is still not a topic
    // a message can be published to, which is the publish path's
    // refusal rather than the prefix's. Asserted here because a reader
    // could reasonably believe the prefix covered it.
    for wildcard in ["ingest/open/#", "ingest/open/+", "ingest/open/+/x"] {
        let refused = post(
            node.ingest,
            "/ingest/open",
            Some(TOKEN),
            &[("x-topic", wildcard)],
            b"wild",
        )
        .await;
        assert_eq!(
            refused.status, 400,
            "{wildcard:?} is not a topic a message can be published to: {}",
            refused.body
        );
    }

    // Two headers is two answers to one question.
    let repeated = post(
        node.ingest,
        "/ingest/open",
        Some(TOKEN),
        &[("x-topic", "ingest/open/a"), ("x-topic", "somewhere/else")],
        b"two",
    )
    .await;
    assert_eq!(repeated.status, 400, "{}", repeated.body);

    // Nothing at all was published, anywhere on the broker.
    assert!(
        subscriber
            .delivery_within(Duration::from_millis(500))
            .await
            .is_none(),
        "a topic that escaped its prefix was published"
    );

    // And the endpoint is not simply refusing everything.
    let accepted = post(
        node.ingest,
        "/ingest/open",
        Some(TOKEN),
        &[("x-topic", "ingest/open")],
        b"the prefix itself is inside itself",
    )
    .await;
    assert_eq!(accepted.status, 200, "{}", accepted.body);
    let delivered = subscriber
        .delivery_within(REPLY)
        .await
        .expect("a topic equal to the prefix is inside it");
    assert_eq!(delivered.topic, PREFIX);

    node.stop().await;
}

/// A topic from a query parameter is used, and bounded the same way.
///
/// The decoded value is what is checked, which is what makes
/// `?topic=ingest%2Fopenx` a refusal rather than a way past the check:
/// it decodes to `ingest/openx`, which is a different level.
#[tokio::test(flavor = "multi_thread")]
async fn a_topic_from_a_query_parameter_is_used_and_bounded_by_the_prefix() {
    let node = Node::start(chosen_topic_rule(TopicSource::Query("topic".to_owned()))).await;
    let mut subscriber = Subscriber::connect(node.mqtt.address(), "watcher").await;
    subscriber.subscribe("#").await;

    let accepted = post(
        node.ingest,
        "/ingest/open?topic=ingest%2Fopen%2Fsensor%2F1",
        Some(TOKEN),
        &[],
        b"from a query",
    )
    .await;
    assert_eq!(accepted.status, 200, "{}", accepted.body);
    let delivered = subscriber
        .delivery_within(REPLY)
        .await
        .expect("a topic inside the prefix is published");
    assert_eq!(delivered.topic, format!("{PREFIX}/sensor/1"));

    // The percent-encoded escape, which is the spelling a check run on
    // the raw query string would let through.
    for escape in [
        "topic=ingest%2Fopenx",
        "topic=ingest%2Fopenx%2Fa",
        "topic=somewhere%2Felse",
        "topic=%24sd%2Frules%2Fopen%2Fdlq",
        // Double encoding: this decodes once to the literal level
        // `ingest%2Fopenx`, which is not under the prefix either.
        "topic=ingest%252Fopenx",
        // **Nothing is trimmed.** These are the two spellings a header
        // cannot carry, because HTTP strips the whitespace around a
        // field value; a percent sequence means the byte and nothing
        // else, so here they arrive intact and are refused.
        "topic=%20ingest%2Fopen",
        "topic=ingest%2Fopen%20",
        "topic=%09ingest%2Fopen",
        // And a NUL, which is not a character a topic may hold at all.
        "topic=ingest%2Fopen%00",
        // A fullwidth solidus is not a separator, so this is the two
        // levels `ingest` and `open\u{ff0f}a`.
        "topic=ingest%2Fopen%EF%BC%8Fa",
    ] {
        let refused = post(
            node.ingest,
            &format!("/ingest/open?{escape}"),
            Some(TOKEN),
            &[],
            b"escaped",
        )
        .await;
        assert_eq!(
            refused.status, 400,
            "{escape} escaped this rule's prefix: {}",
            refused.body
        );
    }

    // The parameter twice is two answers to one question.
    let repeated = post(
        node.ingest,
        "/ingest/open?topic=ingest%2Fopen%2Fa&topic=somewhere%2Felse",
        Some(TOKEN),
        &[],
        b"two",
    )
    .await;
    assert_eq!(repeated.status, 400, "{}", repeated.body);

    assert!(
        subscriber
            .delivery_within(Duration::from_millis(500))
            .await
            .is_none(),
        "a topic that escaped its prefix was published"
    );
    node.stop().await;
}

/// A body over the limit is refused, and the limit is the broker's.
///
/// The number is not written down twice. The test asks
/// [`largest_body`] what the broker's maximum packet size leaves for
/// this topic, posts exactly that — which must succeed and reach a
/// subscriber — and then posts one byte more, which must not.
///
/// Both halves are needed. A limit that refused everything would pass
/// the second assertion, and a limit that refused nothing would pass
/// the first.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_over_the_limit_is_refused_and_the_limit_is_the_brokers() {
    // Small, so the test does not push megabytes through a socket. The
    // point is that the two numbers agree, not how large they are.
    const MAX_PACKET: u32 = 4_096;

    let mut config = BrokerConfig::default();
    config.limits.max_packet_size = MAX_PACKET;
    let mqtt = Server::bind(ServerConfig {
        broker: config.clone(),
        ..ServerConfig::ephemeral()
    })
    .await
    .expect("the MQTT listener binds");
    let (address, serving) =
        serve(Arc::clone(mqtt.shared()), fixed_topic_rule(QoS::AtMostOnce)).await;

    let mut subscriber = Subscriber::connect(mqtt.address(), "watcher").await;
    subscriber.subscribe(FIXED).await;

    let largest = largest_body(MAX_PACKET, FIXED);
    assert!(largest > 0 && largest < MAX_PACKET as usize);

    let accepted = post(
        address,
        "/ingest/fixed",
        Some(TOKEN),
        &[],
        &vec![b'x'; largest],
    )
    .await;
    assert_eq!(
        accepted.status, 200,
        "a body of exactly the limit must be accepted: {}",
        accepted.body
    );
    let delivered = subscriber
        .delivery_within(REPLY)
        .await
        .expect("and must reach a subscriber");
    assert_eq!(delivered.payload.len(), largest);

    let refused = post(
        address,
        "/ingest/fixed",
        Some(TOKEN),
        &[],
        &vec![b'x'; largest + 1],
    )
    .await;
    assert_eq!(
        refused.status, 413,
        "one byte more than the broker can carry must be refused: {}",
        refused.body
    );
    assert!(
        refused.body.contains("max_packet_size"),
        "and the refusal must name the setting the limit comes from: {}",
        refused.body
    );
    assert!(
        subscriber
            .delivery_within(Duration::from_millis(300))
            .await
            .is_none(),
        "a body over the limit was published anyway"
    );

    serving.abort();
    mqtt.shutdown().await;
}

/// Section 7.3, through the ingest endpoint: the response is the
/// promise.
///
/// The same shape as the connection's test and the admin endpoint's,
/// and for the same reason. An assertion that the response eventually
/// arrives passes against a handler that answers first and writes
/// later, so the only thing that can tell the two apart is a store that
/// has not answered yet.
#[tokio::test(flavor = "multi_thread")]
async fn an_at_least_once_post_does_not_answer_until_it_is_durable() {
    let store = Arc::new(GatedStore::new());
    let writer = storage::Writer::spawn(
        Arc::clone(&store) as Arc<dyn storage::Storage>,
        storage::WriterConfig::default(),
    )
    .expect("a writer thread");
    let shared = Arc::new(Shared::with_store(
        Broker::new(BrokerConfig::default()),
        Arc::new(writer),
    ));
    let (address, serving) = serve(shared, fixed_topic_rule(QoS::AtLeastOnce)).await;

    store.hold();
    let mut asking =
        tokio::spawn(
            async move { post(address, "/ingest/fixed", Some(TOKEN), &[], b"promised").await },
        );

    // Long enough that a handler which answered without waiting would
    // have finished many times over.
    let early = tokio::time::timeout(Duration::from_millis(500), &mut asking).await;
    assert!(
        early.is_err(),
        "the post was answered before the store had taken it, so the answer promised \
         something that was not yet true"
    );

    store.release();
    let answered = tokio::time::timeout(REPLY, asking)
        .await
        .expect("the post is answered once the store has flushed")
        .expect("the task did not panic");
    assert_eq!(answered.status, 200, "{}", answered.body);
    assert!(
        answered.body.contains("\"durable\":true"),
        "and says it waited: {}",
        answered.body
    );

    serving.abort();
}

/// A path this listener does not serve, and a method it does not
/// accept, in the one shape.
#[tokio::test(flavor = "multi_thread")]
async fn a_path_that_is_not_served_answers_in_the_one_shape() {
    let node = Node::start(fixed_topic_rule(QoS::AtMostOnce)).await;

    // An empty body, because the fallback answers without reading one
    // and hyper is entitled to close the connection on a request body
    // nothing consumed.
    for path in ["/", "/api/v1/publish", "/ingest", "/ingest/fixed/more"] {
        let answered = post(node.ingest, path, Some(TOKEN), &[], b"").await;
        assert_eq!(answered.status, 404, "{path}: {}", answered.body);
        assert!(
            answered.body.contains("\"code\":\"not_found\""),
            "{path}: {}",
            answered.body
        );
    }

    // A method the path does not accept, in the same shape.
    let wrong = request(node.ingest, "GET", "/ingest/fixed", Some(TOKEN), &[], b"").await;
    assert_eq!(wrong.status, 405, "{}", wrong.body);
    assert!(
        wrong.body.contains("\"code\":\"method_not_allowed\""),
        "{}",
        wrong.body
    );

    // **What this test cannot reach**, said rather than left as a gap.
    // Convention 5. The handler answers a query string that will not
    // parse in this shape too, and nothing here exercises that branch
    // because no query string reaches it: the pairs are deserialised
    // into a `Vec<(String, String)>`, and `form_urlencoded` decodes a
    // malformed percent escape and a non-UTF-8 byte lossily rather than
    // failing. `?%ZZ=1` and `?%FF=1` are both ordinary pairs. The
    // branch is there against a decoder that one day is stricter, and
    // it is unreachable today.

    node.stop().await;
}

// ---- what the endpoint counts -------------------------------------------

/// An ingest rule reports what its endpoint has been asked for, and
/// each number means what its name says.
///
/// Convention 5: a counter that rose on every request would satisfy an
/// assertion that the accepted post moved `fired`, so the request that
/// does **not** authenticate is tried beside it and all four numbers
/// are read after each.
#[tokio::test]
async fn an_ingest_rule_counts_what_it_was_asked_and_what_it_published() {
    let stats = Arc::new(rules::stats::RuleStats::new());
    let ingest = Ingest::new();
    ingest.register_with(
        &RuleId::new("fixed").expect("an identifier"),
        HttpIn {
            token: Secret::new(TOKEN),
            topic: FIXED.to_owned(),
            topic_from: None,
            qos: QoS::AtMostOnce,
            retain: false,
        },
        Arc::clone(&stats),
    );
    let node = Node::start(ingest).await;

    // A request with no token never reached the rule, so nothing it
    // did is the rule's.
    let refused = post(node.ingest, "/ingest/fixed", None, &[], b"21.5").await;
    assert_eq!(refused.status, 401);
    assert_eq!(
        stats.totals(0),
        rules::stats::Totals::default(),
        "a request that did not authenticate was counted as the rule running"
    );

    // A request that did.
    let accepted = post(node.ingest, "/ingest/fixed", Some(TOKEN), &[], b"21.5").await;
    assert_eq!(accepted.status, 200, "{}", accepted.body);
    assert_eq!(
        stats.totals(0),
        rules::stats::Totals {
            fired: 1,
            delivered: 1,
            ..rules::stats::Totals::default()
        },
        "an accepted post moved something other than `fired` and `delivered`"
    );

    // A request this rule will not publish: the body is larger than
    // the broker carries. The rule ran and published nothing, which is
    // a failure and not a delivery.
    let largest = largest_body(
        node.mqtt
            .shared()
            .with_broker(|broker| broker.config().limits.max_packet_size),
        FIXED,
    );
    let refused = post(
        node.ingest,
        "/ingest/fixed",
        Some(TOKEN),
        &[],
        &vec![b'x'; largest + 1],
    )
    .await;
    assert_eq!(refused.status, 413, "{}", refused.body);
    assert_eq!(
        stats.totals(0),
        rules::stats::Totals {
            fired: 2,
            delivered: 1,
            failed: 1,
            ..rules::stats::Totals::default()
        },
        "a post the rule would not publish was counted as a delivery"
    );

    node.stop().await;
}
