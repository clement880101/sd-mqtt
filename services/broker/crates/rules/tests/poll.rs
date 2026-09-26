//! What an `http_poll` rule does against a server that is really there,
//! into a broker that is really there.
//!
//! Section 9.2's `http_poll`. Every test here stands up an HTTP server
//! on the loopback and runs the same [`Poller`] a broker runs, over the
//! same client, publishing through the same broker. Nothing is stubbed
//! between the response body and an MQTT subscriber.
//!
//! # Why a subscriber and not the poller's return value
//!
//! [`Poller::poll`] returning `Published` would prove that something
//! was handed to the publish path. A subscriber attached through the
//! listener, receiving a PUBLISH it did not ask this process for,
//! proves the body went through the matching, the per-session queue and
//! the write path a connection's publish goes through — and it is the
//! only thing that can see the hop count, which lives in a property
//! block that has to be encoded, carried and decoded to be read.
//! Convention 5: a test that constructs the type instead of walking the
//! path is not a test of the path.
//!
//! # The timings, and what is and is not asserted with a clock
//!
//! What a slow fetch *decides* is a pure function and is asserted
//! without a clock, in `http_poll`'s own tests. What is asserted here
//! is what a running poller does, which needs one.
//!
//! Every figure below is chosen so that the answer this code gives and
//! the answer a plausible mistake gives differ by a factor of several,
//! not by a few milliseconds: a test whose margin is the scheduler's
//! jitter is a test that fails on somebody else's laptop and teaches
//! nobody anything.
//!
//! # What these tests deliberately do not cover
//!
//! - **TLS**, for the reason `tests/delivery.rs` gives: what the TLS
//!   stack does with a certificate is rustls's problem, and which URLs
//!   may be fetched at all is a unit test beside the function that
//!   decides it.
//! - **A restart.** A poller coming back on its interval after the
//!   broker is killed is the binary's test, not this crate's.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use mqtt_codec::packet::{Connect, Publish, RetainHandling, Subscribe, SubscriptionOptions};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};
use rules::hops;
use rules::http_out::client;
use rules::http_poll::Poller;
use rules::rule::{HttpPoll, RuleId};
use rules::singleton::{Membership, NodeId};
use sdmqtt_server::listener::{Running, Server, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Where every poller below publishes.
const TOPIC: &str = "weather/now";

/// How long a delivery that should arrive is waited for.
///
/// Generous: what is being timed is a loopback socket and a broker in
/// this process, so anything slower than this did not happen.
const PATIENCE: Duration = Duration::from_secs(10);

// ---- an endpoint that is really there -----------------------------------

/// How the endpoint answers a GET.
#[derive(Debug, Clone)]
enum Behaviour {
    /// 200 with a numbered body, every time, at once.
    Counting,
    /// Answer 500 for the first `times` requests, then as `Counting`.
    RefuseTimes {
        /// How many more refusals are owed.
        times: usize,
    },
    /// Hold each request for `hold`, then answer as `Counting`.
    ///
    /// The slow endpoint the interval decision is about.
    Slow {
        /// How long each request is held.
        hold: Duration,
    },
    /// 200 with a body of `bytes` bytes, which is more than the rule
    /// will read.
    TooBig {
        /// How long the body is.
        bytes: usize,
    },
}

/// One request, as the endpoint saw it.
#[derive(Debug, Clone)]
struct Seen {
    /// The method on the request line.
    method: String,
    /// Header names lowercased, as a server reads them.
    headers: HashMap<String, String>,
    /// When the request line was read.
    started: Instant,
    /// When the answer was written. `None` while it is in flight.
    finished: Option<Instant>,
}

/// An HTTP server on the loopback which records what it was asked and
/// when.
struct Endpoint {
    address: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
    /// The most requests that were ever in flight at one moment.
    ///
    /// The whole of "one fetch at a time, ever": a poller that spawned
    /// a task per tick would put this above one against a slow
    /// endpoint, and no count of requests would show it.
    high_water: Arc<Mutex<usize>>,
    /// Kept so the accept loop is cancelled when the endpoint is
    /// dropped.
    _task: tokio::task::JoinHandle<()>,
}

impl Endpoint {
    async fn start(behaviour: Behaviour) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port on the loopback");
        let address = listener.local_addr().expect("it has an address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let high_water = Arc::new(Mutex::new(0usize));
        let behaviour = Arc::new(Mutex::new(behaviour));

        let task = tokio::spawn({
            let seen = Arc::clone(&seen);
            let high_water = Arc::clone(&high_water);
            let behaviour = Arc::clone(&behaviour);
            async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let seen = Arc::clone(&seen);
                    let high_water = Arc::clone(&high_water);
                    let behaviour = Arc::clone(&behaviour);
                    tokio::spawn(async move {
                        serve(stream, &seen, &high_water, &behaviour).await;
                    });
                }
            }
        });

        Self {
            address,
            seen,
            high_water,
            _task: task,
        }
    }

    fn url(&self) -> String {
        format!("http://{}/now", self.address)
    }

    fn requests(&self) -> Vec<Seen> {
        self.seen.lock().expect("not poisoned").clone()
    }

    fn count(&self) -> usize {
        self.seen.lock().expect("not poisoned").len()
    }

    fn most_at_once(&self) -> usize {
        *self.high_water.lock().expect("not poisoned")
    }

    /// The gaps between one request finishing and the next starting.
    ///
    /// What separates a schedule that drops the ticks a slow fetch
    /// covered from one that fires them in a burst: the first leaves a
    /// gap to the next interval boundary and the second leaves none.
    fn gaps(&self) -> Vec<Duration> {
        let seen = self.requests();
        seen.windows(2)
            .filter_map(|pair| {
                pair[0]
                    .finished
                    .map(|finished| pair[1].started.saturating_duration_since(finished))
            })
            .collect()
    }
}

/// Read one request, record it, and answer as the behaviour says.
///
/// A hand-rolled HTTP/1.1 server, for the reason `tests/delivery.rs`
/// gives: what is needed is a request line, some headers and a
/// response, and a server crate would be a second implementation of
/// HTTP in a workspace that already builds against one.
async fn serve(
    mut stream: TcpStream,
    seen: &Arc<Mutex<Vec<Seen>>>,
    high_water: &Arc<Mutex<usize>>,
    behaviour: &Arc<Mutex<Behaviour>>,
) {
    let mut buffer = Vec::new();
    let head_end = loop {
        if let Some(at) = find(&buffer, b"\r\n\r\n") {
            break at + 4;
        }
        let mut chunk = [0u8; 1024];
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
    };

    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default().to_owned();
    let method = request_line
        .split(' ')
        .next()
        .unwrap_or_default()
        .to_owned();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(": ") {
            headers.insert(name.to_ascii_lowercase(), value.to_owned());
        }
    }

    let index = {
        let mut held = seen.lock().expect("not poisoned");
        held.push(Seen {
            method,
            headers,
            started: Instant::now(),
            finished: None,
        });
        let in_flight = held.iter().filter(|one| one.finished.is_none()).count();
        let mut water = high_water.lock().expect("not poisoned");
        *water = (*water).max(in_flight);
        held.len() - 1
    };

    let (status, body) = {
        let mut held = behaviour.lock().expect("not poisoned");
        match &mut *held {
            Behaviour::Counting | Behaviour::Slow { .. } => (200, format!("reading {}", index + 1)),
            Behaviour::TooBig { bytes } => (200, "x".repeat(*bytes)),
            Behaviour::RefuseTimes { times } => {
                if *times > 0 {
                    *times -= 1;
                    (500, String::new())
                } else {
                    (200, format!("reading {}", index + 1))
                }
            }
        }
    };
    let hold = {
        let held = behaviour.lock().expect("not poisoned");
        match &*held {
            Behaviour::Slow { hold } => Some(*hold),
            _ => None,
        }
    };
    if let Some(hold) = hold {
        tokio::time::sleep(hold).await;
    }

    let response = format!(
        "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
    seen.lock().expect("not poisoned")[index].finished = Some(Instant::now());
}

/// The index of `needle` in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---- the broker and a subscriber on it ----------------------------------

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

    /// The next delivery, insisting there is one.
    async fn delivery(&mut self) -> Publish {
        self.delivery_within(PATIENCE)
            .await
            .unwrap_or_else(|| panic!("a delivery on {TOPIC} should have arrived"))
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

// ---- the poller under test ----------------------------------------------

/// A rule fetching `url` every `interval_ms`.
fn action(url: String, interval_ms: u32) -> HttpPoll {
    HttpPoll {
        url,
        interval_ms,
        // Shorter than the default so that a test about a failing
        // endpoint is bounded by the test's own patience rather than by
        // ten seconds of waiting.
        timeout_ms: 2_000,
        topic: TOPIC.to_owned(),
        ..HttpPoll::default()
    }
}

/// A broker with a poller running beside it.
///
/// The membership is handed in, because which member owns the poller is
/// what one of the tests below is about.
struct Node {
    broker: Running,
    poller: tokio::task::JoinHandle<()>,
}

impl Node {
    async fn stop(self) {
        self.poller.abort();
        self.broker.shutdown().await;
    }
}

async fn start(action: &HttpPoll, membership: &Membership) -> (Node, Subscriber) {
    let broker = Server::bind(ServerConfig::ephemeral())
        .await
        .expect("the MQTT listener binds");
    let mut subscriber = Subscriber::connect(broker.address(), "watcher").await;
    subscriber.subscribe(TOPIC).await;

    let poller = Poller::new(
        RuleId::new("weather").expect("an identifier"),
        client(),
        Arc::clone(broker.shared()),
        action,
    )
    .expect("the rule is usable");
    let running = tokio::spawn(poller.run(membership.ownership()));

    (
        Node {
            broker,
            poller: running,
        },
        subscriber,
    )
}

/// The one member milestone 4 has.
fn alone() -> Membership {
    Membership::alone(NodeId::new("local"))
}

// ---- the tests ----------------------------------------------------------

/// The one that says the body went through the broker.
///
/// A poller fetching a body, and an MQTT subscriber attached through
/// the listener receiving it on the configured topic with the bytes the
/// endpoint answered with — twice, a real interval apart.
#[tokio::test(flavor = "multi_thread")]
async fn the_body_is_published_on_the_interval_and_reaches_a_subscriber() {
    let endpoint = Endpoint::start(Behaviour::Counting).await;
    let interval = Duration::from_millis(400);
    let mut rule = action(endpoint.url(), 400);
    rule.headers = vec![("accept".to_owned(), "text/plain".to_owned())];
    let (running, mut subscriber) = start(&rule, &alone()).await;

    let first = subscriber.delivery().await;
    let at_first = Instant::now();
    assert_eq!(first.topic, TOPIC);
    assert_eq!(
        first.payload.as_ref(),
        b"reading 1",
        "the subscriber receives what the endpoint answered with, unchanged"
    );

    let second = subscriber.delivery().await;
    let between = at_first.elapsed();
    assert_eq!(
        second.payload.as_ref(),
        b"reading 2",
        "and the second interval is a second fetch, not a republication of the first"
    );

    // **The interval is honoured.** A poller that did not wait would
    // put these microseconds apart; the margin is a factor of four
    // rather than a few milliseconds, so the assertion is about the
    // interval rather than about the scheduler.
    assert!(
        between >= interval / 4,
        "two publications {between:?} apart, on an interval of {interval:?}: \
         the poller did not wait"
    );

    let asked = endpoint.requests();
    assert!(asked.len() >= 2, "the endpoint was asked twice");
    assert_eq!(asked[0].method, "GET", "section 9.2 says a GET");
    assert_eq!(
        asked[0].headers.get("accept").map(String::as_str),
        Some("text/plain"),
        "a configured header reaches the endpoint"
    );
    assert!(
        !asked[0].headers.contains_key("content-type"),
        "and a GET carries no body, so it does not announce the type of one: {:?}",
        asked[0].headers
    );

    running.stop().await;
}

/// What a poller publishes carries its hop count.
///
/// Section 9.2: every publication a connector originates carries one,
/// because the loop this broker cannot otherwise see is a cycle of
/// connectors with no bridge in it. Read off a subscriber's own
/// property block rather than the publication that was handed over, so
/// that a count lost in the encoding is a count this test misses too.
#[tokio::test(flavor = "multi_thread")]
async fn what_a_poller_publishes_carries_its_hop_count() {
    let endpoint = Endpoint::start(Behaviour::Counting).await;
    let (running, mut subscriber) = start(&action(endpoint.url(), 400), &alone()).await;

    let delivered = subscriber.delivery().await;
    assert_eq!(
        hops::count_of(&delivered.properties.user_properties),
        Some(1),
        "a poller originates, so what it publishes has hopped once; the properties were \
         {:?}",
        delivered.properties.user_properties
    );

    running.stop().await;
}

/// A fetch that fails skips that interval, and the next one happens.
///
/// **The test that proves one failure does not stop the poller for
/// good.** There is no outbox and no retry count here: what a failed
/// fetch gets is the next interval, which is what a successful one
/// gets.
#[tokio::test(flavor = "multi_thread")]
async fn a_fetch_that_fails_skips_that_interval_and_the_next_one_still_happens() {
    // Two refusals, so that the first failure is followed by a second
    // rather than by a success: a poller that stopped on the *second*
    // failure would pass a test with only one.
    let endpoint = Endpoint::start(Behaviour::RefuseTimes { times: 2 }).await;
    let (running, mut subscriber) = start(&action(endpoint.url(), 150), &alone()).await;

    let delivered = subscriber.delivery().await;
    assert_eq!(
        delivered.payload.as_ref(),
        b"reading 3",
        "the two refusals published nothing, and the third fetch did"
    );
    assert!(
        endpoint.count() >= 3,
        "the endpoint was asked three times: {} requests",
        endpoint.count()
    );

    running.stop().await;
}

/// Nothing is published when a fetch fails.
///
/// The other half of the test above, and worth saying on its own: a
/// poller that published an empty body, or the refusal's own body, on
/// a failed fetch would satisfy "the next interval still happens" and
/// would put nonsense on the topic meanwhile. A stale or empty reading
/// is worse than a missing one, because a subscriber cannot tell.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_fetch_publishes_nothing_at_all() {
    let endpoint = Endpoint::start(Behaviour::RefuseTimes { times: 3 }).await;
    let (running, mut subscriber) = start(&action(endpoint.url(), 100), &alone()).await;

    // Long enough for the three refusals to have happened and been
    // skipped, and short enough that the fourth fetch — which succeeds
    // — has not.
    let seen = subscriber.delivery_within(Duration::from_millis(250)).await;
    assert!(
        seen.is_none(),
        "a refused fetch must publish nothing, and this published {seen:?}"
    );
    assert!(
        endpoint.count() >= 2,
        "the poller did keep asking meanwhile: {} requests",
        endpoint.count()
    );

    running.stop().await;
}

/// A body past the limit is a failed fetch, not a truncated
/// publication.
///
/// **Found by a mutation that survived.** Replacing the refusal with a
/// truncated body changed nothing any test could see, and a poller that
/// published what it managed to read is the worst of the three
/// outcomes: a subscriber receiving half a document has no way to know
/// it is half, where a subscriber receiving nothing at least sees the
/// topic stop.
///
/// The endpoint answers with far more than the rule will read, so the
/// limit is reached in the body rather than in a header, and the poller
/// keeps asking afterwards — a body that did not fit is one failed
/// fetch, not the end of the poller.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_past_the_limit_publishes_nothing_and_the_poller_carries_on() {
    let endpoint = Endpoint::start(Behaviour::TooBig { bytes: 64 * 1024 }).await;
    let mut rule = action(endpoint.url(), 100);
    rule.max_body_bytes = 512;
    let (running, mut subscriber) = start(&rule, &alone()).await;

    assert!(
        subscriber
            .delivery_within(Duration::from_millis(600))
            .await
            .is_none(),
        "a body that did not fit must publish nothing at all, truncated or otherwise"
    );
    assert!(
        endpoint.count() >= 2,
        "and the poller keeps asking: {} requests",
        endpoint.count()
    );

    running.stop().await;
}

/// A fetch slower than the interval: one at a time, and the ticks it
/// covered are dropped.
///
/// Both halves of the decision, and they are different failures.
///
/// A poller that spawned a task per tick would put several requests to
/// one slow endpoint in flight at once — the amplification that
/// finishes off an endpoint which is already struggling — and
/// [`Endpoint::most_at_once`] is the only thing that can see it: a
/// count of requests looks the same either way.
///
/// A poller that queued the ticks a slow fetch passed, which is what
/// every runtime's interval timer does by default, would go again the
/// instant the slow fetch returned, with no gap at all.
/// [`Endpoint::gaps`] is what sees that, and the figures are chosen so
/// that the two answers differ by the whole of a gap rather than by
/// jitter: an interval of 100ms and a fetch held for 300ms puts the
/// next boundary 100ms after the fetch returns, against nothing at all.
#[tokio::test(flavor = "multi_thread")]
async fn a_fetch_slower_than_the_interval_never_overlaps_and_drops_the_ticks_it_covered() {
    let endpoint = Endpoint::start(Behaviour::Slow {
        hold: Duration::from_millis(300),
    })
    .await;
    let (running, _subscriber) = start(&action(endpoint.url(), 100), &alone()).await;

    // Long enough for three or four fetches at one per 400ms.
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    assert!(
        endpoint.count() >= 3,
        "the poller should have kept fetching: {} requests",
        endpoint.count()
    );
    assert_eq!(
        endpoint.most_at_once(),
        1,
        "one fetch at a time, ever: an endpoint that has become slow must not be sent \
         more requests because it has become slow"
    );

    let gaps = endpoint.gaps();
    assert!(!gaps.is_empty(), "there were at least two requests");
    for gap in &gaps {
        assert!(
            *gap >= Duration::from_millis(50),
            "a fetch that overran waits for the next interval boundary rather than going \
             again at once; the gaps were {gaps:?}"
        );
    }

    running.stop().await;
}

/// A poller fetches nothing while another member owns it, and starts
/// when it takes over.
///
/// Section 9.2 makes a poller a singleton. Two nodes running one is two
/// requests per interval to somebody else's endpoint and two
/// publications of one reading.
///
/// **Reached through the poller rather than through the membership
/// view.** `singleton`'s own tests say what the rule answers; this one
/// says the poller asks it. Convention 5: a unit test of a helper is
/// not a test of the caller that was supposed to use it.
#[tokio::test(flavor = "multi_thread")]
async fn a_poller_fetches_nothing_while_another_member_owns_it() {
    let endpoint = Endpoint::start(Behaviour::Counting).await;
    // This node is `b`, and `a` is live and lower, so `a` owns it.
    let membership = Membership::of(NodeId::new("b"), [NodeId::new("a")]);
    let (running, mut subscriber) = start(&action(endpoint.url(), 50), &membership).await;

    // Twenty intervals' worth. A poller that ignored ownership would
    // have fetched by now, many times over.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        endpoint.count(),
        0,
        "another member owns this poller, so this node fetches nothing"
    );
    assert!(
        subscriber
            .delivery_within(Duration::from_millis(100))
            .await
            .is_none(),
        "and publishes nothing"
    );

    // The lowest member leaves, so this one takes over.
    assert!(membership.left(&NodeId::new("a")));

    let delivered = subscriber.delivery().await;
    assert_eq!(delivered.payload.as_ref(), b"reading 1");
    assert!(
        endpoint.count() >= 1,
        "and it fetches as soon as it takes over rather than one interval later"
    );

    running.stop().await;
}
