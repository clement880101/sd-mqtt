//! What an `http_out` rule does against a server that is really there.
//!
//! Every test here stands up an HTTP server on the loopback and drives
//! the same [`Delivery`] a broker runs, over the same client, through the
//! same store. Nothing is stubbed between the envelope and the socket,
//! which is the point: the settings on the client that this milestone
//! exists to get right — a connect timeout, a pool timer, no retry of a
//! cancelled request — are invisible to any test that replaces the client
//! with something obliging.
//!
//! # The endpoint is plain HTTP, on the loopback
//!
//! Posting in the clear is refused for every URL that is not a loopback
//! address, and `crates/rules/src/http_out.rs` has the argument for the
//! exception. What it buys here is that these tests exercise the posting
//! path an operator's rule uses rather than a door held open for tests.
//!
//! # What these tests deliberately do not cover
//!
//! - **TLS itself.** Standing up a certificate the client would accept
//!   means generating one, which means a dependency and an expiry date.
//!   What the TLS stack does with a certificate is rustls's own problem
//!   and is tested there; what this crate decides is which URLs are
//!   allowed to be posted to at all, and that is a unit test beside the
//!   function that decides it.
//! - **Concurrency.** The pump posts one envelope at a time; see the
//!   note on `max_concurrency` in the crate documentation.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use rules::http_out::{client, DeadLetters, Delivery, Poster, Turn};
use rules::rule::{HttpOut, RuleId};
use rules::Outbox;
use storage::record::{
    ActionRecord, BodyRecord, HttpOutRecord, MessageId, Qos, Record, RuleRecord, TriggerRecord,
};
use storage::{MemoryStorage, Storage, Writer, WriterConfig};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---- a server that is really there --------------------------------------

/// How the endpoint answers.
#[derive(Debug, Clone)]
enum Behaviour {
    /// 204, every time.
    Accept,
    /// Answer `status` for the first `times` requests, then accept.
    RefuseTimes {
        /// How many more refusals are owed.
        times: usize,
        /// What to answer with while refusing.
        status: u16,
    },
    /// Answer `status` for ever.
    AlwaysRefuse {
        /// What to answer with.
        status: u16,
        /// A `Retry-After` in seconds to send with it.
        retry_after: Option<u64>,
    },
    /// Accept the connection, read the request, and never answer.
    ///
    /// The most common real failure and the one a retry count alone does
    /// nothing about.
    NeverAnswer,
}

/// One request, as the endpoint saw it.
#[derive(Debug, Clone)]
struct Received {
    method: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl Received {
    /// The envelope's `id` field, by a string search rather than a JSON
    /// parser, so that a renamed field is a failure here too.
    fn envelope_id(&self) -> String {
        let text = String::from_utf8(self.body.clone()).expect("the body is text");
        let marker = "\"id\":\"";
        let start = text.find(marker).expect("the envelope carries an id") + marker.len();
        let rest = &text[start..];
        rest[..rest.find('"').expect("the id is closed")].to_owned()
    }
}

/// An HTTP server on the loopback, which records what it was sent.
struct Endpoint {
    address: SocketAddr,
    received: Arc<Mutex<Vec<Received>>>,
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
        let received = Arc::new(Mutex::new(Vec::new()));
        let behaviour = Arc::new(Mutex::new(behaviour));

        let task = tokio::spawn({
            let received = Arc::clone(&received);
            let behaviour = Arc::clone(&behaviour);
            async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let received = Arc::clone(&received);
                    let behaviour = Arc::clone(&behaviour);
                    tokio::spawn(async move {
                        serve(stream, &received, &behaviour).await;
                    });
                }
            }
        });

        Self {
            address,
            received,
            _task: task,
        }
    }

    fn url(&self) -> String {
        format!("http://{}/hook", self.address)
    }

    fn requests(&self) -> Vec<Received> {
        self.received.lock().expect("not poisoned").clone()
    }

    fn count(&self) -> usize {
        self.received.lock().expect("not poisoned").len()
    }
}

/// Read one request, record it, and answer as the behaviour says.
///
/// A hand-rolled HTTP/1.1 server rather than a dependency: what is
/// needed is a request line, some headers, a body of a known length and
/// a response, and a server crate would be a second implementation of
/// HTTP in a workspace that already builds against one.
async fn serve(
    mut stream: TcpStream,
    received: &Arc<Mutex<Vec<Received>>>,
    behaviour: &Arc<Mutex<Behaviour>>,
) {
    let mut buffer = Vec::new();
    // Headers first: read until the blank line that ends them.
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

    let length: usize = headers
        .get("content-length")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let mut body = buffer[head_end..].to_vec();
    while body.len() < length {
        let mut chunk = [0u8; 1024];
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => body.extend_from_slice(&chunk[..read]),
        }
    }

    received.lock().expect("not poisoned").push(Received {
        method,
        headers,
        body,
    });

    let answer = {
        let mut held = behaviour.lock().expect("not poisoned");
        match &mut *held {
            Behaviour::Accept => Some((204, None)),
            Behaviour::RefuseTimes { times, status } => {
                if *times > 0 {
                    *times -= 1;
                    Some((*status, None))
                } else {
                    Some((204, None))
                }
            }
            Behaviour::AlwaysRefuse {
                status,
                retry_after,
            } => Some((*status, *retry_after)),
            Behaviour::NeverAnswer => None,
        }
    };

    let Some((status, retry_after)) = answer else {
        // Hold the connection open, answering nothing, until the client
        // gives up or the endpoint is dropped.
        tokio::time::sleep(Duration::from_secs(3_600)).await;
        return;
    };

    let mut response = format!("HTTP/1.1 {status} X\r\nContent-Length: 0\r\n");
    if let Some(seconds) = retry_after {
        response.push_str(&format!("Retry-After: {seconds}\r\n"));
    }
    response.push_str("Connection: keep-alive\r\n\r\n");
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

/// The index of `needle` in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---- the rule, the store and the pump -----------------------------------

/// A dead-letter sink that remembers.
#[derive(Debug, Default)]
struct Letters(Mutex<Vec<(String, Vec<u8>)>>);

impl DeadLetters for Letters {
    fn dead_letter(&self, topic: &str, payload: Vec<u8>) {
        self.0
            .lock()
            .expect("not poisoned")
            .push((topic.to_owned(), payload));
    }
}

impl Letters {
    fn published(&self) -> Vec<(String, Vec<u8>)> {
        self.0.lock().expect("not poisoned").clone()
    }
}

const RULE: &str = "telemetry";

fn rule_record(action: &HttpOut) -> RuleRecord {
    RuleRecord {
        id: RULE.to_owned(),
        name: "Telemetry out".to_owned(),
        enabled: true,
        trigger: TriggerRecord {
            filter: "sensor/+/temp".to_owned(),
            min_qos: None,
            client_id: None,
        },
        action: ActionRecord::HttpOut(HttpOutRecord {
            url: action.url.clone(),
            method: action.method.clone(),
            headers: action.headers.clone(),
            timeout_ms: action.timeout_ms,
            max_concurrency: action.max_concurrency,
            retries: action.retries,
            backoff_ms: action.backoff_ms,
        }),
    }
}

fn body_id(n: u8) -> MessageId {
    let mut bytes = [0u8; 16];
    bytes[0] = n;
    MessageId(bytes)
}

fn body(n: u8, payload: &[u8]) -> BodyRecord {
    BodyRecord {
        topic: format!("sensor/{n}/temp"),
        payload: Bytes::copy_from_slice(payload),
        qos: Qos::AtLeastOnce,
        retain: false,
        expires_at: None,
        properties: Bytes::new(),
    }
}

/// A writer whose only flush is the one a waiter asks for.
///
/// An hour between interval flushes and a billion records before a count
/// flush, so nothing a test sees happened because a timer went off.
fn writer_config() -> WriterConfig {
    WriterConfig {
        interval: Duration::from_secs(3_600),
        records: 1_000_000_000,
        ..WriterConfig::default()
    }
}

/// Everything a pump needs, with `count` envelopes already waiting.
struct Fixture {
    store: Arc<MemoryStorage>,
    writer: Arc<Writer>,
    outbox: Arc<Outbox>,
    letters: Arc<Letters>,
    action: HttpOut,
}

impl Fixture {
    fn new(action: HttpOut, envelopes: &[(&str, &[u8])]) -> Self {
        let store = Arc::new(MemoryStorage::new());
        store
            .apply(&[Record::RulePut {
                rule: rule_record(&action),
            }])
            .expect("the rule is storable");
        let writer = Arc::new(
            Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, writer_config())
                .expect("the writer starts"),
        );
        let outbox = Arc::new(
            Outbox::open(
                RuleId::new(RULE).expect("a valid identifier"),
                store.as_ref(),
            )
            .expect("the outbox opens"),
        );

        for (index, (client_id, payload)) in envelopes.iter().enumerate() {
            let n = u8::try_from(index + 1).expect("few enough envelopes");
            let (_, records) = outbox.append(
                body_id(100 + n),
                body_id(n),
                body(n, payload),
                client_id,
                1_700_000_000 + u32::from(n),
            );
            store.apply(&records).expect("the envelope is storable");
        }

        Self {
            store,
            writer,
            outbox,
            letters: Arc::new(Letters::default()),
            action,
        }
    }

    fn delivery(&self) -> Delivery {
        let poster = Poster::new(client(), &self.action).expect("the endpoint is configured");
        Delivery::new(
            Arc::clone(&self.outbox),
            Arc::clone(&self.store) as Arc<dyn Storage>,
            Arc::clone(&self.writer),
            poster,
            &self.action,
            Arc::clone(&self.letters) as Arc<dyn DeadLetters>,
        )
    }

    fn depth(&self) -> u64 {
        self.outbox
            .depth(self.store.as_ref())
            .expect("the depth answers")
    }

    fn holds_payload(&self, n: u8) -> bool {
        self.store
            .body(&body_id(n))
            .expect("the read answers")
            .is_some()
    }
}

fn action_for(url: String) -> HttpOut {
    HttpOut {
        url,
        method: "POST".to_owned(),
        headers: vec![("x-token".to_owned(), "secret".to_owned())],
        timeout_ms: 2_000,
        max_concurrency: 1,
        retries: 2,
        backoff_ms: 20,
    }
}

// ---- the tests ----------------------------------------------------------

/// The envelope arrives, with the rule's method and headers on it, and
/// the outbox empties.
#[tokio::test]
async fn an_envelope_reaches_the_endpoint() {
    let endpoint = Endpoint::start(Behaviour::Accept).await;
    let fixture = Fixture::new(
        action_for(endpoint.url()),
        &[("sensor-1", b"21.5" as &[u8])],
    );
    let delivery = fixture.delivery();

    assert_eq!(delivery.turn().await, Turn::Delivered);

    let requests = endpoint.requests();
    assert_eq!(requests.len(), 1, "exactly one request was made");
    assert_eq!(requests[0].method, "POST", "the rule's method was not used");
    assert_eq!(
        requests[0].headers.get("x-token").map(String::as_str),
        Some("secret"),
        "the rule's headers were not sent"
    );
    assert_eq!(
        requests[0].headers.get("content-type").map(String::as_str),
        Some("application/json")
    );
    let sent = String::from_utf8(requests[0].body.clone()).expect("the body is text");
    assert_eq!(
        sent,
        concat!(
            r#"{"id":"65000000000000000000000000000000","#,
            r#""topic":"sensor/1/temp","#,
            r#""payload":"21.5","#,
            r#""qos":1,"#,
            r#""retain":false,"#,
            r#""client_id":"sensor-1","#,
            r#""timestamp":1700000001,"#,
            r#""user_properties":[]}"#,
        ),
        "the endpoint was sent something other than the envelope"
    );

    assert_eq!(fixture.depth(), 0, "the envelope did not leave the outbox");
    assert!(
        !fixture.holds_payload(1),
        "the payload was leaked after it had been delivered"
    );
    assert!(
        fixture.letters.published().is_empty(),
        "a delivered envelope was dead-lettered"
    );
}

/// A server that fails twice and then succeeds is retried, and the
/// message is delivered once.
///
/// "Once" is the half that matters: the outbox must not be left holding
/// an envelope that has already arrived, and the payload must not be
/// released before it had.
#[tokio::test]
async fn a_server_that_fails_twice_and_then_succeeds_is_retried_and_delivered() {
    let endpoint = Endpoint::start(Behaviour::RefuseTimes {
        times: 2,
        status: 503,
    })
    .await;
    let fixture = Fixture::new(
        action_for(endpoint.url()),
        &[("sensor-1", b"21.5" as &[u8])],
    );
    let delivery = fixture.delivery();

    assert!(matches!(delivery.turn().await, Turn::BackedOff(_)));
    assert_eq!(fixture.depth(), 1, "a failed attempt lost the envelope");
    assert!(
        fixture.holds_payload(1),
        "a failed attempt released the payload the retry needs"
    );

    assert!(matches!(delivery.turn().await, Turn::BackedOff(_)));
    assert_eq!(delivery.turn().await, Turn::Delivered);

    assert_eq!(
        endpoint.count(),
        3,
        "the endpoint saw the wrong number of attempts"
    );
    assert_eq!(fixture.depth(), 0);
    assert!(!fixture.holds_payload(1), "the payload was leaked");
    assert!(fixture.letters.published().is_empty());

    // Every attempt carried the same identifier, which is the whole of
    // what lets the endpoint notice it has seen this message before.
    let ids: Vec<String> = endpoint
        .requests()
        .iter()
        .map(Received::envelope_id)
        .collect();
    assert_eq!(
        ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
        1,
        "the identifier changed between attempts, so a receiver cannot deduplicate: {ids:?}"
    );
}

/// A server that always fails ends in the dead-letter topic, after
/// exactly the configured number of attempts and not one more.
#[tokio::test]
async fn a_server_that_always_fails_ends_in_the_dead_letter_topic() {
    let endpoint = Endpoint::start(Behaviour::AlwaysRefuse {
        status: 500,
        retry_after: None,
    })
    .await;
    let mut action = action_for(endpoint.url());
    action.retries = 2;
    let fixture = Fixture::new(action, &[("sensor-1", b"21.5" as &[u8])]);
    let delivery = fixture.delivery();

    assert!(matches!(delivery.turn().await, Turn::BackedOff(_)));
    assert!(matches!(delivery.turn().await, Turn::BackedOff(_)));
    assert!(
        matches!(delivery.turn().await, Turn::DeadLettered(_)),
        "the third attempt is the last, because retries is two"
    );

    assert_eq!(
        endpoint.count(),
        3,
        "a retry count of two is three attempts, no more and no fewer"
    );
    assert_eq!(fixture.depth(), 0, "the envelope did not leave the outbox");
    assert!(
        !fixture.holds_payload(1),
        "a dead-lettered envelope leaked its payload"
    );

    let letters = fixture.letters.published();
    assert_eq!(letters.len(), 1, "nothing was dead-lettered");
    assert_eq!(letters[0].0, "$sd/rules/telemetry/dlq");
    let document = String::from_utf8(letters[0].1.clone()).expect("the dead letter is text");
    assert!(
        document.contains("\"attempts\":3"),
        "the dead letter must say how many attempts it cost: {document}"
    );
    assert!(
        document.contains("500"),
        "the dead letter must say why: {document}"
    );
    assert!(
        document.contains(r#""payload":"21.5""#),
        "the dead letter must carry the envelope that could not be sent: {document}"
    );

    // Nothing is tried again afterwards.
    assert_eq!(delivery.turn().await, Turn::Idle);
    assert_eq!(endpoint.count(), 3);
}

/// A server that never answers is bounded by the rule's timeout.
///
/// **This is the one usually missing.** A retry count bounds how many
/// times a request is made and says nothing about how long one takes,
/// and an endpoint that accepts the connection and then goes quiet is
/// the most common real failure there is. Without the timeout this test
/// does not fail; it hangs, which is the point.
#[tokio::test]
async fn a_server_that_never_answers_is_bounded_by_the_timeout() {
    let endpoint = Endpoint::start(Behaviour::NeverAnswer).await;
    let mut action = action_for(endpoint.url());
    action.timeout_ms = 300;
    action.retries = 1;
    action.backoff_ms = 10;
    let fixture = Fixture::new(action, &[("sensor-1", b"21.5" as &[u8])]);
    let delivery = fixture.delivery();

    let started = Instant::now();
    assert!(matches!(delivery.turn().await, Turn::BackedOff(_)));
    let one = started.elapsed();
    assert!(
        one >= Duration::from_millis(250),
        "the attempt gave up before the timeout: {one:?}"
    );
    assert!(
        one < Duration::from_secs(3),
        "the attempt was not bounded by the rule's timeout: {one:?}"
    );

    assert!(matches!(delivery.turn().await, Turn::DeadLettered(_)));
    let both = started.elapsed();
    assert!(
        both < Duration::from_secs(5),
        "two attempts at a silent endpoint took {both:?}, which is not two timeouts"
    );
    assert_eq!(endpoint.count(), 2, "the endpoint saw both attempts");

    let letters = fixture.letters.published();
    assert_eq!(letters.len(), 1);
    let document = String::from_utf8(letters[0].1.clone()).expect("text");
    assert!(
        document.contains("did not answer in time"),
        "a timeout must be dead-lettered as a timeout: {document}"
    );
}

/// An endpoint that is failing sees a bounded number of attempts in a
/// window, not one per envelope queued behind the first.
///
/// **The thundering herd, and the thing that makes it one.** A backoff
/// held per envelope expires on every waiting envelope at the same
/// moment, so a rule with a backlog answers an endpoint's first failure
/// by hitting it once per queued message as fast as the network allows —
/// which arrives exactly when the endpoint can least afford it. The
/// backoff here is held on the rule, so the queue behind the first
/// failure does not move at all until the wait has been served.
#[tokio::test]
async fn an_endpoint_that_fails_sees_one_attempt_per_window_not_one_per_envelope() {
    let endpoint = Endpoint::start(Behaviour::AlwaysRefuse {
        status: 503,
        retry_after: None,
    })
    .await;
    let mut action = action_for(endpoint.url());
    // Long enough that nothing in this test can serve one wait.
    action.backoff_ms = 30_000;
    action.retries = 5;
    let waiting: Vec<(&str, &[u8])> = (0..8).map(|_| ("sensor-1", b"21.5" as &[u8])).collect();
    let fixture = Fixture::new(action, &waiting);
    assert_eq!(fixture.depth(), 8, "eight envelopes are waiting");

    let delivery = fixture.delivery();
    let task = tokio::spawn(async move { delivery.run().await });
    tokio::time::sleep(Duration::from_millis(600)).await;
    task.abort();

    assert_eq!(
        endpoint.count(),
        1,
        "a failing endpoint was hit once per queued envelope rather than once per window"
    );
    assert_eq!(
        fixture.depth(),
        8,
        "nothing left the outbox, because nothing was delivered"
    );
}

/// And the endpoint's own `Retry-After` is honoured over the rule's
/// figure.
#[tokio::test]
async fn an_endpoint_that_asks_for_patience_is_given_it() {
    let endpoint = Endpoint::start(Behaviour::AlwaysRefuse {
        status: 429,
        retry_after: Some(120),
    })
    .await;
    let mut action = action_for(endpoint.url());
    // The rule would come back in a millisecond. The endpoint says two
    // minutes, and the endpoint wins.
    action.backoff_ms = 1;
    action.retries = 5;
    let fixture = Fixture::new(action, &[("sensor-1", b"21.5" as &[u8])]);
    let delivery = fixture.delivery();

    let Turn::BackedOff(wait) = delivery.turn().await else {
        panic!("a 429 is a failed attempt");
    };
    assert_eq!(
        wait,
        Duration::from_secs(120),
        "the endpoint asked for two minutes and was given the rule's millisecond"
    );
}

/// The attempt count in the store is what a restart comes back to.
///
/// The pump holds nothing about an envelope between turns, so this is
/// the store's answer rather than the pump's memory of it: a new
/// [`Delivery`] over the same outbox, which is what a restarted broker
/// builds, sees the attempts the old one made.
#[tokio::test]
async fn the_attempt_count_is_in_the_store_and_a_new_pump_sees_it() {
    let endpoint = Endpoint::start(Behaviour::AlwaysRefuse {
        status: 500,
        retry_after: None,
    })
    .await;
    let mut action = action_for(endpoint.url());
    action.retries = 2;
    action.backoff_ms = 1;
    let fixture = Fixture::new(action, &[("sensor-1", b"21.5" as &[u8])]);

    assert!(matches!(
        fixture.delivery().turn().await,
        Turn::BackedOff(_)
    ));
    assert_eq!(
        fixture
            .outbox
            .head(fixture.store.as_ref(), 0)
            .expect("head")
            .expect("still waiting")
            .attempts,
        1,
        "the attempt was not recorded in the store"
    );

    // A different pump, as a restarted broker would build. It must not
    // start the envelope's budget again.
    assert!(matches!(
        fixture.delivery().turn().await,
        Turn::BackedOff(_)
    ));
    assert!(matches!(
        fixture.delivery().turn().await,
        Turn::DeadLettered(_)
    ));
    assert_eq!(
        endpoint.count(),
        3,
        "three pumps made more than the rule's three attempts, so the count is not durable"
    );
}

/// An envelope whose payload has gone is dropped rather than retried for
/// ever.
#[tokio::test]
async fn an_envelope_whose_payload_is_gone_does_not_block_the_outbox() {
    let endpoint = Endpoint::start(Behaviour::Accept).await;
    let fixture = Fixture::new(
        action_for(endpoint.url()),
        &[("sensor-1", b"21.5" as &[u8]), ("sensor-2", b"22.5")],
    );
    // Somebody else released the first payload's last reference.
    fixture
        .store
        .apply(&[Record::BodyReleased { id: body_id(1) }])
        .expect("the release is accepted");

    let delivery = fixture.delivery();
    assert_eq!(delivery.turn().await, Turn::Orphaned);
    assert_eq!(delivery.turn().await, Turn::Delivered);
    assert_eq!(
        endpoint.count(),
        1,
        "the envelope behind the orphan was not delivered"
    );
    assert_eq!(fixture.depth(), 0);
}

/// Two rules posting to one endpoint share the client's pool.
///
/// Not an assertion about the pool's internals — there is nothing public
/// to assert on — but about the thing sharing it is for: a second rule
/// against the same endpoint works, over the same client, without
/// anything being rebuilt per rule.
#[tokio::test]
async fn one_client_serves_more_than_one_rule() {
    let endpoint = Endpoint::start(Behaviour::Accept).await;
    let shared = client();
    let action = action_for(endpoint.url());
    let first = Poster::new(shared.clone(), &action).expect("configured");
    let second = Poster::new(shared, &action).expect("configured");
    assert_eq!(first.uri(), second.uri());

    let fixture = Fixture::new(action, &[("sensor-1", b"21.5" as &[u8])]);
    assert_eq!(fixture.delivery().turn().await, Turn::Delivered);
    assert_eq!(endpoint.count(), 1);
}
