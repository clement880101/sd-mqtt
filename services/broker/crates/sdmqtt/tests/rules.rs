//! Half of milestone four's exit criterion: a message published to MQTT
//! reaches an HTTPS endpoint, and survives the broker being killed
//! mid-flight.
//!
//! Against the real binary, over real sockets, through a real store.
//! Everything below the shell is tested where it lives and none of those
//! tests can see a process die.
//!
//! # The broker is killed, not shut down
//!
//! **A clean close of the store persists everything, flushed or not.**
//! That was measured while the storage crate was written and it makes a
//! politely restarted broker worthless as evidence: a broker that never
//! made anything durable would pass such a test, because dropping the
//! writer flushes and dropping the store commits. So the broker here
//! dies of `SIGKILL`, which runs no destructor, and the test asserts
//! that is how it died — a kill that had quietly become an orderly
//! shutdown would otherwise go on passing and stop meaning anything.
//!
//! # The writer is configured never to flush on its own
//!
//! An hour between interval flushes and a billion records before a count
//! flush, so nothing that survives here survived because a timer went
//! off. What is left is section 7.3's third trigger — a batch somebody
//! is waiting on — and the only waiter in the rule path is the delivery
//! pump itself, which makes its own record of an attempt durable before
//! it makes another. That is the sharp form: if the envelope survives,
//! it survived because the pump's own write covered it.
//!
//! # The endpoint keeps its port across the restart
//!
//! A rule's URL is fixed in the configuration, so "the endpoint is down"
//! cannot mean "the listener is gone" without the port coming back at
//! the same number, which is a race. It means the listener is still
//! there and answering 503, which is what a real endpoint in trouble
//! does anyway.

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{Ack, Connect, Publish, RetainHandling, Subscribe, SubscriptionOptions};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};

/// The topic the rule watches.
const TOPIC: &str = "sensor/1/temp";

/// The rule's identifier, and so the middle of its dead-letter topic.
const RULE: &str = "telemetry";

/// Where a dead letter goes.
const DLQ: &str = "$sd/rules/telemetry/dlq";

/// What is published while the endpoint is up.
const FIRST: &[u8] = b"delivered while the endpoint was there";

/// What is published while it is not, and must arrive after the
/// restart.
const SECOND: &[u8] = b"in flight when the broker was killed";

/// How long a session outlives its connection, on the wall clock.
const EXPIRY: u32 = 3_600;

/// What the writer is told to wait before flushing on its own.
const NEVER: Duration = Duration::from_secs(3_600);

/// How long to wait for a freshly started broker to accept.
const STARTUP: Duration = Duration::from_secs(20);

/// How long to wait for something that should happen.
const PATIENCE: Duration = Duration::from_secs(20);

/// The admin listener's token, so a test can read what the broker
/// thinks it holds.
const ADMIN_TOKEN: &str = "a-secret-for-the-test";

/// The ingest rule's identifier, and so the last level of its path.
const INGEST_RULE: &str = "gateway";

/// The ingest rule's own bearer token, which is not the admin one: a
/// rule's token is per rule, and a test that used one secret for both
/// could not tell them apart.
const INGEST_TOKEN: &str = "a-secret-for-the-gateway";

/// Where the ingest rule publishes.
const INGEST_TOPIC: &str = "ingest/across";

/// What is posted before the broker is killed.
const POSTED: &[u8] = b"posted before the broker was killed";

/// The identifier of the rule this test creates over the API.
///
/// Nothing in any configuration file mentions it, which is the whole
/// point: it exists because a request made it, and it survives because
/// the request made it durable.
const API_RULE: &str = "made-over-the-api";

/// What that rule's trigger matches.
const API_FILTER: &str = "api/+/temp";

/// What is published to it.
const API_TOPIC: &str = "api/1/temp";

/// What is published before the broker is killed.
const BEFORE_THE_RESTART: &[u8] = b"published to a rule the file never mentioned";

/// What is published after it comes back.
const AFTER_THE_RESTART: &[u8] = b"published after the broker was restarted";

// ---- the endpoint -------------------------------------------------------

/// One request, as the endpoint saw it.
#[derive(Debug, Clone)]
struct Received {
    /// The request body, which is the envelope.
    body: Vec<u8>,
}

impl Received {
    /// A quoted string field's value, found by searching the text
    /// rather than by parsing it: a renamed field is then a failure
    /// here too, which a parser tolerant of unknown keys would not be.
    fn field(&self, name: &str) -> String {
        let text = String::from_utf8(self.body.clone()).expect("the envelope is text");
        let marker = format!("\"{name}\":\"");
        let start = text
            .find(&marker)
            .unwrap_or_else(|| panic!("the envelope has no {name}: {text}"))
            + marker.len();
        let rest = &text[start..];
        rest[..rest.find('"').expect("the value is closed")].to_owned()
    }
}

impl Received {
    /// The hop count the envelope carries among its user properties.
    ///
    /// Found by searching the text, for the reason [`Received::field`]
    /// gives: a renamed property or a changed shape is a failure here
    /// too, which a tolerant parser would let through.
    fn hops(&self) -> u32 {
        let text = String::from_utf8(self.body.clone()).expect("the envelope is text");
        let marker = "[\"sd-hops\",\"";
        let start = text
            .find(marker)
            .unwrap_or_else(|| panic!("the envelope carries no hop count: {text}"))
            + marker.len();
        let rest = &text[start..];
        rest[..rest.find('"').expect("the value is closed")]
            .parse()
            .expect("a hop count is a number")
    }
}

/// An HTTP endpoint on the loopback that a rule can post to.
///
/// It keeps its port for the whole test, because the rule's URL is
/// written into a configuration file before the broker starts. Being
/// "down" is answering 503 rather than going away.
struct Endpoint {
    address: SocketAddr,
    received: Arc<Mutex<Vec<Received>>>,
    accepting: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    /// What a request is answered with.
    ///
    /// Empty is the `204 No Content` this endpoint has always
    /// answered, which is what a webhook does. A poller fetches rather
    /// than posts and wants something back, so it sets this and gets a
    /// `200` with a body.
    body: Arc<Mutex<Vec<u8>>>,
}

impl Endpoint {
    /// Bind, and serve until dropped.
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a port on the loopback");
        let address = listener.local_addr().expect("it has an address");
        listener
            .set_nonblocking(true)
            .expect("the accept loop polls");

        let received = Arc::new(Mutex::new(Vec::new()));
        let accepting = Arc::new(AtomicBool::new(true));
        let stopped = Arc::new(AtomicBool::new(false));
        let body = Arc::new(Mutex::new(Vec::new()));

        std::thread::spawn({
            let received = Arc::clone(&received);
            let accepting = Arc::clone(&accepting);
            let stopped = Arc::clone(&stopped);
            let body = Arc::clone(&body);
            move || {
                while !stopped.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let received = Arc::clone(&received);
                            let accepting = Arc::clone(&accepting);
                            let body = Arc::clone(&body);
                            std::thread::spawn(move || {
                                serve(stream, &received, &accepting, &body);
                            });
                        }
                        Err(_) => std::thread::sleep(Duration::from_millis(5)),
                    }
                }
            }
        });

        Self {
            address,
            received,
            accepting,
            stopped,
            body,
        }
    }

    /// Answer every request with this body from now on.
    fn answers(&self, body: &[u8]) {
        *self.body.lock().expect("not poisoned") = body.to_vec();
    }

    fn url(&self) -> String {
        format!("http://{}/hook", self.address)
    }

    /// Answer 503 from now on.
    fn go_down(&self) {
        self.accepting.store(false, Ordering::SeqCst);
    }

    /// Answer 204 again.
    fn come_back(&self) {
        self.accepting.store(true, Ordering::SeqCst);
    }

    fn requests(&self) -> Vec<Received> {
        self.received.lock().expect("not poisoned").clone()
    }

    fn count(&self) -> usize {
        self.received.lock().expect("not poisoned").len()
    }

    /// Wait until at least `wanted` requests have arrived.
    #[track_caller]
    fn wait_for(&self, wanted: usize, what: &str) {
        let deadline = Instant::now() + PATIENCE;
        while Instant::now() < deadline {
            if self.count() >= wanted {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "{what}: the endpoint saw {} requests and was waiting for {wanted}",
            self.count()
        );
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
    }
}

/// Read one request, record it, and answer.
fn serve(
    mut stream: TcpStream,
    received: &Arc<Mutex<Vec<Received>>>,
    accepting: &AtomicBool,
    body_to_answer: &Arc<Mutex<Vec<u8>>>,
) {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("a read deadline");
    let mut buffer = Vec::new();
    let head_end = loop {
        if let Some(at) = find(&buffer, b"\r\n\r\n") {
            break at + 4;
        }
        let mut chunk = [0u8; 1024];
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
        }
    };

    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let mut headers = HashMap::new();
    for line in head.split("\r\n").skip(1) {
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
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(read) => body.extend_from_slice(&chunk[..read]),
        }
    }

    received
        .lock()
        .expect("not poisoned")
        .push(Received { body });

    let answer = body_to_answer.lock().expect("not poisoned").clone();
    let status = if !accepting.load(Ordering::SeqCst) {
        "503 Service Unavailable"
    } else if answer.is_empty() {
        "204 No Content"
    } else {
        "200 OK"
    };
    let _ = stream.write_all(
        format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            answer.len()
        )
        .as_bytes(),
    );
    if status.starts_with("200") {
        let _ = stream.write_all(&answer);
    }
    let _ = stream.flush();
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---- the broker under test ----------------------------------------------

/// What `--config` is pointed at.
///
/// A file rather than flags, because a rule has no flag and should not:
/// a command line is visible in `ps` to every user on the host, and a
/// rule's headers are usually somebody's bearer token.
fn write_config(
    path: &Path,
    store: &Path,
    mqtt: SocketAddr,
    admin: SocketAddr,
    ingest: SocketAddr,
    tables: &str,
) {
    let text = format!(
        r#"
[listener]
address = "{mqtt}"

[admin]
address = "{admin}"
token = "{ADMIN_TOKEN}"

[ingest]
address = "{ingest}"

[storage]
directory = "{store}"
sync = "immediate"
flush_interval_ms = {interval}
flush_records = 1000000000

[log]
level = "info"

{tables}
"#,
        store = store.display(),
        interval = NEVER.as_millis(),
    );
    std::fs::write(path, text).expect("the configuration file can be written");
}

/// A `[[rules]]` table accepting posts on one fixed topic.
///
/// At least once, which is the whole point: section 7.3 says an
/// acknowledgement at that level promises the message is durable, and
/// the response to the post is that acknowledgement.
fn rule_accepting_posts() -> String {
    format!(
        r#"
[[rules]]
id = "{INGEST_RULE}"
name = "Gateway in"

[rules.http_in]
token = "{INGEST_TOKEN}"
topic = "{INGEST_TOPIC}"
qos = 1
"#
    )
}

/// A `[[rules]]` table posting to `url`.
fn rule_posting_to(url: &str, retries: u8, backoff_ms: u32) -> String {
    format!(
        r#"
[[rules]]
id = "{RULE}"
name = "Telemetry out"
filter = "sensor/+/temp"

[rules.http_out]
url = "{url}"
timeout_ms = 2000
retries = {retries}
backoff_ms = {backoff_ms}
"#
    )
}

/// A running `sdmqtt` process.
struct BrokerProcess {
    child: Child,
    address: SocketAddr,
    log: PathBuf,
}

impl BrokerProcess {
    /// Start a broker against `config`, and wait until it accepts.
    fn start(config: &Path, store: &Path, address: SocketAddr, log: PathBuf) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_sdmqtt"));
        command
            .arg("serve")
            .arg("--config")
            .arg(config)
            // **On the command line as well as in the file.** The
            // environment of whoever runs the tests is emptied of both
            // keys below so that it cannot reach this broker, and an
            // emptied `SDMQTT_STORAGE_DIRECTORY` means "no store" and
            // overrides the file. A flag beats the environment, which
            // is what puts the directory back.
            .arg("--store-directory")
            .arg(store)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            // Emptied so that a configuration in the environment of
            // whoever is running the tests cannot reach this broker.
            .env("SDMQTT_CONFIG", "")
            .env("SDMQTT_STORAGE_DIRECTORY", "")
            .stderr(Stdio::from(
                File::create(&log).expect("a log file can be created"),
            ));
        let child = command.spawn().expect("the broker binary runs");
        let mut broker = Self {
            child,
            address,
            log: log.clone(),
        };
        if let Err(why) = broker.wait_until_listening() {
            panic!("the broker never started: {why}");
        }
        broker
    }

    fn wait_until_listening(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + STARTUP;
        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!("it exited with {status}: {}", self.logged()));
            }
            if TcpStream::connect_timeout(&self.address, Duration::from_millis(200)).is_ok() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(format!(
            "it did not accept on {} within {STARTUP:?}: {}",
            self.address,
            self.logged()
        ))
    }

    fn logged(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Kill it, running no destructor, and prove that is what happened.
    ///
    /// `Child::kill` is `SIGKILL` on unix, which cannot be caught,
    /// blocked or handled: no `Drop`, no final flush, no clean close of
    /// the store. The exit status is checked because a test that
    /// believed it was killing the process while something turned it
    /// into an orderly shutdown would go on passing and stop meaning
    /// anything.
    fn kill(mut self) {
        self.child
            .kill()
            .expect("the broker is still there to kill");
        let status = self.child.wait().expect("it can be waited for");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                status.signal(),
                Some(9),
                "the broker must die of SIGKILL, which runs no destructor; \
                 anything that lets it close its store proves nothing about \
                 durability"
            );
        }
        #[cfg(not(unix))]
        assert!(
            !status.success(),
            "the broker must be killed rather than allowed to stop"
        );
    }
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A port the operating system has just said is free.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("a port on the loopback")
        .local_addr()
        .expect("it has an address")
        .port()
}

fn loopback(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().expect("an address")
}

// ---- an HTTP client over the socket -------------------------------------

/// What an HTTP request came back with.
#[derive(Debug)]
struct Answered {
    status: u16,
    body: String,
}

/// One request, with `Connection: close` so the response ends at end of
/// file.
///
/// Small on purpose. The same argument the admin crate's own test
/// client makes: this project already builds its own MQTT client on its
/// own codec, and a second HTTP stack in the dev-dependencies would be
/// a larger thing to audit than the thing it tests.
fn http(
    address: SocketAddr,
    method: &str,
    path: &str,
    token: &str,
    body: Option<&[u8]>,
) -> Answered {
    http_with(address, method, path, token, &[], body)
}

/// The same, carrying headers of its own.
///
/// One caller, and it is the one that matters: section 9.2's hop count
/// crosses the HTTP leg of a cycle as a request header, so a test that
/// could not send one could not drive a cycle at all.
fn http_with(
    address: SocketAddr,
    method: &str,
    path: &str,
    token: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
) -> Answered {
    let mut stream = TcpStream::connect_timeout(&address, PATIENCE).expect("the listener accepts");
    stream
        .set_read_timeout(Some(PATIENCE))
        .expect("a read deadline");
    let body = body.unwrap_or(b"");
    let extra: String = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect();
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\
         Authorization: Bearer {token}\r\n{extra}Content-Length: {}\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .expect("the head is written");
    stream.write_all(body).expect("and the body");
    stream.flush().expect("and flushed");

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("the response ends");
    let text = String::from_utf8_lossy(&raw).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in {text:?}"));
    let body = text
        .split_once("\r\n\r\n")
        .map_or(String::new(), |(_, rest)| rest.to_owned());
    Answered { status, body }
}

/// Ask the admin API for `path` until `ready` is happy, or give up
/// loudly.
///
/// What this is for is the one race in the test below: dropping a
/// socket is not the same instant as the broker noticing. Posting
/// before it has noticed would hand the message to a connection that is
/// ending, and the test would be about something else.
#[track_caller]
fn until(admin: SocketAddr, path: &str, what: &str, ready: impl Fn(&str) -> bool) -> String {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let body = http(admin, "GET", path, ADMIN_TOKEN, None).body;
        if ready(&body) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: {path} never settled; the last answer was {body}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Wait until something is accepting on `address`.
#[track_caller]
fn accepting(address: SocketAddr, what: &str) {
    let deadline = Instant::now() + STARTUP;
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("{what} never accepted on {address}");
}

// ---- a client over the socket -------------------------------------------

/// One MQTT 5 client on a blocking socket.
struct Client {
    stream: TcpStream,
    buf: BytesMut,
}

impl Client {
    fn connect(address: SocketAddr, client_id: &str) -> Self {
        let stream = TcpStream::connect_timeout(&address, PATIENCE).expect("the broker accepts");
        stream.set_nodelay(true).expect("nodelay");
        stream
            .set_read_timeout(Some(PATIENCE))
            .expect("a read deadline");
        let mut client = Self {
            stream,
            buf: BytesMut::new(),
        };
        client.send(&Packet::Connect(Box::new(Connect {
            version: Version::V5,
            client_id: client_id.into(),
            clean_start: false,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties {
                session_expiry_interval: Some(EXPIRY),
                ..Properties::default()
            },
        })));
        match client.next() {
            Some(Packet::ConnAck(ack)) => {
                assert_eq!(ack.reason, ReasonCode::Success, "{client_id} was refused");
            }
            other => panic!("expected a connect acknowledgement, got {other:?}"),
        }
        client
    }

    fn subscribe(&mut self, filter: &str) {
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
        }));
        match self.next() {
            Some(Packet::SubAck(ack)) => {
                assert_eq!(ack.reasons, vec![ReasonCode::GrantedQos1], "on {filter}");
            }
            other => panic!("expected a subscribe acknowledgement, got {other:?}"),
        }
    }

    /// Publish at least once and wait to be told it is safe.
    fn publish_and_wait(&mut self, topic: &str, payload: &'static [u8]) {
        self.send(&Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: topic.into(),
            packet_id: Some(1),
            payload: Bytes::from_static(payload),
            properties: Properties::default(),
        }));
        match self.next() {
            Some(Packet::PubAck(ack)) => {
                assert_eq!(ack.packet_id, 1);
                assert!(
                    matches!(
                        ack.reason,
                        ReasonCode::Success | ReasonCode::NoMatchingSubscribers
                    ),
                    "the publication was refused: {:?}",
                    ack.reason
                );
            }
            other => panic!("expected a publish acknowledgement, got {other:?}"),
        }
    }

    /// Wait for a delivery on any subscription.
    fn delivery_within(&mut self, patience: Duration) -> Option<Publish> {
        self.stream
            .set_read_timeout(Some(patience))
            .expect("a read deadline");
        let deadline = Instant::now() + patience;
        while Instant::now() < deadline {
            match self.next() {
                Some(Packet::Publish(publish)) => {
                    if let Some(packet_id) = publish.packet_id {
                        self.send(&Packet::PubAck(Ack {
                            packet_id,
                            reason: ReasonCode::Success,
                            properties: Properties::default(),
                        }));
                    }
                    return Some(publish);
                }
                Some(_) => continue,
                None => return None,
            }
        }
        None
    }

    fn send(&mut self, packet: &Packet) {
        let mut out = BytesMut::new();
        encode(packet, Version::V5, &mut out).expect("the packet encodes");
        self.stream.write_all(&out).expect("the write succeeds");
    }

    fn next(&mut self) -> Option<Packet> {
        loop {
            if let Some(packet) = decode(&mut self.buf, Version::V5, &Limits::default())
                .expect("the broker sends valid packets")
            {
                return Some(packet);
            }
            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk) {
                Ok(0) | Err(_) => return None,
                Ok(read) => self.buf.extend_from_slice(&chunk[..read]),
            }
        }
    }
}

// ---- the exit criterion -------------------------------------------------

/// A message published to MQTT reaches an endpoint, and one in flight
/// when the broker is killed reaches it after the restart.
#[test]
fn a_message_reaches_an_endpoint_and_survives_the_broker_being_killed() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let endpoint = Endpoint::start();
    let mqtt = loopback(free_port());

    // Long enough that the envelope published while the endpoint is
    // down is still waiting when the broker is killed, rather than
    // having been retried into its dead letter in the meantime. Short
    // enough that the restarted broker serves out the rest of the same
    // wait — the deadline is in the stored record and the new process
    // honours it — inside this test's patience.
    write_config(
        &config,
        &store,
        mqtt,
        loopback(free_port()),
        loopback(free_port()),
        &rule_posting_to(&endpoint.url(), 5, 2_000),
    );

    // ---- the endpoint is there ------------------------------------
    let broker = BrokerProcess::start(&config, &store, mqtt, workspace.path().join("first.log"));
    {
        let mut client = Client::connect(mqtt, "publisher");
        client.publish_and_wait(TOPIC, FIRST);
        endpoint.wait_for(1, "the first message must reach the endpoint");
    }

    let first = endpoint.requests();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].field("topic"), TOPIC);
    assert_eq!(
        first[0].field("payload"),
        String::from_utf8_lossy(FIRST),
        "the endpoint was sent something other than what was published"
    );
    assert_eq!(
        first[0].field("client_id"),
        "publisher",
        "the envelope must say who published it"
    );
    let delivered_id = first[0].field("id");

    // ---- the endpoint goes down, and so does the broker ------------
    endpoint.go_down();
    {
        let mut client = Client::connect(mqtt, "publisher-again");
        client.publish_and_wait(TOPIC, SECOND);
    }
    // The attempt that fails is what makes the append durable: the pump
    // waits for its own record of the attempt before it makes another,
    // and that flush covers everything applied before it. Waiting for
    // the request to arrive is therefore waiting for the envelope to be
    // on the disk, without sleeping and hoping.
    endpoint.wait_for(2, "the second message must be attempted and refused");
    assert_eq!(
        endpoint.requests()[1].field("payload"),
        String::from_utf8_lossy(SECOND)
    );

    // **Killed, not stopped.** A clean close writes out everything,
    // flushed or not, so a polite shutdown here would prove nothing.
    broker.kill();

    // ---- both come back -------------------------------------------
    endpoint.come_back();
    let before = endpoint.count();
    let _restarted =
        BrokerProcess::start(&config, &store, mqtt, workspace.path().join("second.log"));

    endpoint.wait_for(
        before + 1,
        "the envelope in flight when the broker was killed must be delivered after the restart",
    );
    let after = endpoint.requests();
    let resumed = &after[before];
    assert_eq!(
        resumed.field("payload"),
        String::from_utf8_lossy(SECOND),
        "something other than the undelivered envelope arrived"
    );
    assert_eq!(
        resumed.field("id"),
        after[1].field("id"),
        "the retry after the restart must carry the identifier the first attempt did, \
         or a receiver cannot tell the repeat from a new message"
    );
    assert_ne!(
        resumed.field("id"),
        delivered_id,
        "two different messages must not share an identifier"
    );

    // And nothing was delivered twice: the endpoint has seen the first
    // message once and the second one twice, which is the attempt that
    // was refused and the attempt that succeeded.
    let payloads: Vec<String> = after
        .iter()
        .map(|request| request.field("payload"))
        .collect();
    assert_eq!(
        payloads
            .iter()
            .filter(|payload| *payload == &String::from_utf8_lossy(FIRST).to_string())
            .count(),
        1,
        "the message that was already delivered was sent again after the restart: {payloads:?}"
    );
}

/// The attempt count survives the kill too, so a node coming back after
/// an outage does not start every envelope's budget again.
///
/// **This is the half a durable outbox alone would pass.** An envelope
/// that comes back with its attempts reset is eventually delivered just
/// the same, so the first test cannot see the difference. What can see
/// it is the total number of attempts an endpoint that never recovers
/// receives before the envelope is given up on: with a durable count it
/// is the rule's, and with a count that restarts with the process it is
/// the rule's every time the process starts.
#[test]
fn the_attempt_count_survives_the_kill() {
    const RETRIES: u8 = 3;

    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let endpoint = Endpoint::start();
    let mqtt = loopback(free_port());
    write_config(
        &config,
        &store,
        mqtt,
        loopback(free_port()),
        loopback(free_port()),
        &rule_posting_to(&endpoint.url(), RETRIES, 50),
    );
    endpoint.go_down();

    let broker = BrokerProcess::start(&config, &store, mqtt, workspace.path().join("first.log"));
    {
        let mut client = Client::connect(mqtt, "publisher");
        client.publish_and_wait(TOPIC, SECOND);
    }
    // Two of the four attempts, and then the process dies. The pump
    // makes its record of each attempt durable before it makes the
    // next, so seeing the second request is seeing the first attempt on
    // the disk.
    endpoint.wait_for(2, "two attempts must be made before the broker is killed");
    broker.kill();

    let before_the_kill = endpoint.count();
    assert!(
        before_the_kill >= 2,
        "the endpoint saw {before_the_kill} attempts"
    );

    let _restarted =
        BrokerProcess::start(&config, &store, mqtt, workspace.path().join("second.log"));
    let mut watcher = Client::connect(mqtt, "dead-letter-watcher");
    watcher.subscribe(DLQ);

    let dead = watcher
        .delivery_within(PATIENCE)
        .expect("an envelope that fails every attempt must reach the dead-letter topic");
    assert_eq!(dead.topic, DLQ);
    let document = String::from_utf8(dead.payload.to_vec()).expect("a dead letter is text");
    assert!(
        document.contains(&format!("\"attempts\":{}", u32::from(RETRIES) + 1)),
        "the dead letter must say the rule's number of attempts was reached: {document}"
    );

    // **The assertion the durable count exists for.** The endpoint has
    // been refusing throughout and has never recovered, so the total
    // number of requests it has seen across both of the broker's lives
    // is the rule's attempt budget — once. A count that restarted with
    // the process would spend the whole budget again and the total
    // would be larger.
    let total = endpoint.count();
    assert_eq!(
        total,
        usize::from(RETRIES) + 1,
        "a failing endpoint received {total} attempts across a restart, and the rule allows {}; \
         an attempt count that does not survive the kill spends the whole budget again",
        u32::from(RETRIES) + 1
    );
}

/// A rule that cannot post stops the broker starting, by name.
///
/// The alternative is a broker that comes up perfectly and quietly
/// delivers nothing, which an operator finds out about from whoever was
/// expecting the messages.
#[test]
fn a_rule_that_cannot_post_stops_the_broker_starting() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let config = workspace.path().join("sdmqtt.toml");
    let mqtt = loopback(free_port());
    write_config(
        &config,
        &workspace.path().join("store"),
        mqtt,
        loopback(free_port()),
        loopback(free_port()),
        // Plain text to somewhere that is not this machine. The payload
        // and the rule's headers do not cross a network in the clear.
        &rule_posting_to("http://example.invalid/hook", 3, 50),
    );

    let output = Command::new(env!("CARGO_BIN_EXE_sdmqtt"))
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .arg("--store-directory")
        .arg(workspace.path().join("store"))
        .env("SDMQTT_CONFIG", "")
        .env("SDMQTT_STORAGE_DIRECTORY", "")
        .output()
        .expect("the broker binary runs");
    assert!(!output.status.success(), "the broker must refuse to start");
    let said = String::from_utf8_lossy(&output.stderr);
    assert!(
        said.contains(RULE),
        "the refusal must name the rule: {said}"
    );
    assert!(
        said.contains("clear") || said.contains("loopback"),
        "the refusal must say what is wrong with it: {said}"
    );
}

/// **The other half of milestone four's exit criterion.** An HTTP post
/// reaches an MQTT subscriber across a restart.
///
/// 1. A broker with a store, an ingest rule, and a subscriber whose
///    session outlives its connection.
/// 2. The subscriber disconnects, so what is published queues.
/// 3. A post at least once. It is acknowledged, so it is a promise.
/// 4. **The broker is killed**, running no destructor.
/// 5. It starts again, the subscriber reconnects, and the message is
///    there.
///
/// # Killed, not stopped
///
/// A clean close of the store writes out everything, flushed or not, so
/// a politely restarted broker would pass this test against a broker
/// that never made anything durable. `BrokerProcess::kill` sends
/// `SIGKILL` and asserts the process died of it.
///
/// # The writer is configured never to flush on its own
///
/// An hour between interval flushes and a billion records before a
/// count flush. So nothing here survives because a timer went off, and
/// the only flush that can have happened is section 7.3's third
/// trigger: a batch somebody is waiting on. The only waiter in this
/// path is the post itself, waiting for the durability its `200`
/// promises. That is the sharp form: **if the message survives, it
/// survived because answering the post is what made it durable.**
///
/// It is also the shape of the mutation this test exists for. Answering
/// before the wait leaves nothing to trigger a flush at all, so nothing
/// is on the disk when the process dies.
#[test]
fn an_http_post_reaches_a_subscriber_across_a_restart() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let mqtt = loopback(free_port());
    let admin = loopback(free_port());
    let ingest = loopback(free_port());
    write_config(
        &config,
        &store,
        mqtt,
        admin,
        ingest,
        &rule_accepting_posts(),
    );

    let broker = BrokerProcess::start(&config, &store, mqtt, workspace.path().join("first.log"));
    accepting(ingest, "the ingest listener");

    // ---- a subscriber whose session outlives its connection --------
    {
        let mut subscriber = Client::connect(mqtt, "durable-subscriber");
        subscriber.subscribe(INGEST_TOPIC);
    }
    // **Dropped, and then waited for.** Closing a socket is not the
    // same instant as the broker noticing; a post made in between would
    // be handed to a connection that is ending, and this test would be
    // about something else.
    until(
        admin,
        "/api/v1/sessions",
        "the subscriber must be seen to have gone before anything is posted",
        |body| body.contains("durable-subscriber") && body.contains("\"connected\":false"),
    );

    // ---- the post, which is a promise ------------------------------
    let answered = http(
        ingest,
        "POST",
        &format!("/ingest/{INGEST_RULE}"),
        INGEST_TOKEN,
        Some(POSTED),
    );
    assert_eq!(answered.status, 200, "{}", answered.body);
    // **This assertion is about what the answer says, not about what
    // happened.** The field reports the level's promise, so a handler
    // that answered before waiting would still set it; the mutation
    // that skips the wait passes this line and fails the reconnection
    // at the end. It is here because a caller reads it, not because it
    // guards anything.
    assert!(
        answered.body.contains("\"durable\":true"),
        "an at-least-once post says the answer is a promise: {}",
        answered.body
    );
    assert!(
        answered.body.contains("\"delivered\":1"),
        "and that it reached the session that is waiting for it: {}",
        answered.body
    );

    // The broker agrees that it is queued for a session that is not
    // there. Read before the kill, so a failure afterwards is about
    // durability rather than about the message never having existed.
    until(
        admin,
        "/api/v1/sessions",
        "the message must be queued for the disconnected session",
        |body| body.contains("\"queued\":1"),
    );

    // ---- killed, not stopped ---------------------------------------
    broker.kill();

    // ---- and it is still there -------------------------------------
    let _restarted =
        BrokerProcess::start(&config, &store, mqtt, workspace.path().join("second.log"));
    let mut subscriber = Client::connect(mqtt, "durable-subscriber");
    let delivered = subscriber
        .delivery_within(PATIENCE)
        .expect("a post acknowledged before the kill must reach the subscriber after it");
    assert_eq!(delivered.topic, INGEST_TOPIC);
    assert_eq!(
        &delivered.payload[..],
        POSTED,
        "something other than what was posted arrived"
    );
    assert_eq!(
        delivered.qos,
        QoS::AtLeastOnce,
        "at the level the rule publishes at, which is the level that promised anything"
    );
}

/// The ingest listener is its own, and answers nothing of the admin
/// listener's; and the admin listener answers no ingest route.
///
/// Section 9.2: it "defaults to a separate ingest listener rather than
/// the admin listener, so that publishing data does not require
/// exposing the administrative surface". Against the real binary,
/// because that sentence is about two sockets a process bound and
/// nothing below the shell can see them both.
///
/// The failure shape it rules out: an ingest listener that serves the
/// admin router as well would expose `/api/v1/publish` — which, on a
/// listener an operator has deliberately opened to webhook senders,
/// publishes to any topic with no token at all.
#[test]
fn the_ingest_listener_serves_ingest_and_nothing_else() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let mqtt = loopback(free_port());
    let admin = loopback(free_port());
    let ingest = loopback(free_port());
    write_config(
        &config,
        &store,
        mqtt,
        admin,
        ingest,
        &rule_accepting_posts(),
    );

    let _broker = BrokerProcess::start(&config, &store, mqtt, workspace.path().join("log"));
    accepting(ingest, "the ingest listener");

    // Nothing of the admin listener's is on the ingest listener.
    for path in ["/", "/api/v1/publish", "/api/v1/retained", "/metrics"] {
        let answered = http(ingest, "GET", path, ADMIN_TOKEN, None);
        assert_eq!(
            answered.status, 404,
            "{path} is the admin listener's and was served on the ingest listener: {}",
            answered.body
        );
    }

    // And the ingest route is not on the admin listener, because this
    // configuration did not ask for it there.
    let elsewhere = http(
        admin,
        "POST",
        &format!("/ingest/{INGEST_RULE}"),
        INGEST_TOKEN,
        Some(b"x"),
    );
    assert_eq!(
        elsewhere.status, 404,
        "the ingest route is served where the configuration asked for it: {}",
        elsewhere.body
    );

    // The two listeners answer a path neither serves in the same shape,
    // which is what says the ingest listener's error body is section
    // 13's and not a second description of it. The two are written in
    // different crates, because `rules` cannot depend on `sdmqtt-admin`,
    // and this is the only place both are visible at once.
    let here = http(ingest, "GET", "/no/such/path", ADMIN_TOKEN, None);
    let there = http(admin, "GET", "/api/v1/no/such/path", ADMIN_TOKEN, None);
    assert_eq!(here.status, there.status);
    assert!(
        here.body.contains("\"error\":{\"code\":\"not_found\"")
            && there.body.contains("\"error\":{\"code\":\"not_found\""),
        "both listeners answer in the one shape: {} against {}",
        here.body,
        there.body
    );

    // And the admin listener still works, so the assertions above are
    // not passing against a broker that answered 404 to everything.
    let alive = http(admin, "GET", "/health/live", ADMIN_TOKEN, None);
    assert_eq!(alive.status, 200, "{}", alive.body);
}

/// An ingest rule with no token stops the broker starting, by name.
///
/// Section 9.2: the token is not optional. The alternative is a broker
/// that comes up perfectly with an endpoint that publishes for anybody
/// who finds the port.
#[test]
fn an_ingest_rule_with_no_token_stops_the_broker_starting() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let config = workspace.path().join("sdmqtt.toml");
    let mqtt = loopback(free_port());
    write_config(
        &config,
        &workspace.path().join("store"),
        mqtt,
        loopback(free_port()),
        loopback(free_port()),
        &format!(
            r#"
[[rules]]
id = "{INGEST_RULE}"
name = "Gateway in"

[rules.http_in]
topic = "{INGEST_TOPIC}"
"#
        ),
    );

    let output = Command::new(env!("CARGO_BIN_EXE_sdmqtt"))
        .arg("serve")
        .arg("--config")
        .arg(&config)
        .arg("--store-directory")
        .arg(workspace.path().join("store"))
        .env("SDMQTT_CONFIG", "")
        .env("SDMQTT_STORAGE_DIRECTORY", "")
        .output()
        .expect("the broker binary runs");
    assert!(!output.status.success(), "the broker must refuse to start");
    let said = String::from_utf8_lossy(&output.stderr);
    assert!(
        said.contains(INGEST_RULE),
        "the refusal must name the rule: {said}"
    );
    assert!(said.contains("token"), "and say what is missing: {said}");
}

// ---- section 9.2's third loop, driven round until it stops ---------------

/// How many connectors a publication may go through in this test.
///
/// Small so that the cycle below is short enough to read in the
/// assertions, and not the default, so that a test which passed because
/// the default happened to be what the code used would not.
const CYCLE_MAX_HOPS: u32 = 3;

/// The rule identifier the cycle's ingest half has.
const CYCLE_INGEST: &str = "round";

/// The topic the cycle goes round on.
const CYCLE_TOPIC: &str = "loop/round";

/// The two rules that make a cycle with no bridge in it, and the hop
/// limit that bounds it.
///
/// Section 9.2: "an outbound rule feeding a system that posts back
/// through an ingest rule is a cycle with no bridge in it". This is
/// exactly that. The `[broker]` table rides along because
/// `write_config` appends whatever it is given to the file, and a TOML
/// table may appear anywhere in one.
fn rules_that_cycle_through(url: &str) -> String {
    format!(
        r#"
[broker]
max_hops = {CYCLE_MAX_HOPS}

[[rules]]
id = "{CYCLE_INGEST}"
name = "The way in"

[rules.http_in]
token = "{INGEST_TOKEN}"
topic = "{CYCLE_TOPIC}"
qos = 0

[[rules]]
id = "back-out"
name = "The way out"
filter = "{CYCLE_TOPIC}"

[rules.http_out]
url = "{url}"
timeout_ms = 2000
retries = 0
"#
    )
}

/// **The cycle, driven round a real broker until the limit stops it.**
///
/// A connector's publication now fires the message hooks, which is what
/// makes section 9.2's "where any rule can act on them" true — and it
/// is also what lets one connector's output be another's input, so it
/// is what makes a cycle of connectors possible. This drives one.
///
/// The circle is an ingest rule publishing to a topic, an `http_out`
/// rule firing on that topic, and a system on the other end of it that
/// posts back to the ingest rule. The system is this test, which is the
/// honest shape of it: the count has to cross a piece of software
/// neither end wrote, and it crosses because that software returns what
/// it was given. Nothing here trusts the arithmetic — the number in
/// each envelope is read off the wire and handed back on the next post.
///
/// What it asserts is the whole of the ruling: the chain grows by one
/// each time round, the broker carries it while it is under the limit,
/// it refuses the publication that would take it over with a `508`, and
/// the counter section 9.2 asks for has moved.
#[test]
fn a_cycle_of_connectors_is_driven_into_the_hop_limit() {
    let endpoint = Endpoint::start();
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let mqtt = loopback(free_port());
    let admin = loopback(free_port());
    let ingest = loopback(free_port());
    write_config(
        &config,
        &store,
        mqtt,
        admin,
        ingest,
        &rules_that_cycle_through(&endpoint.url()),
    );
    let broker = BrokerProcess::start(&config, &store, mqtt, workspace.path().join("broker.log"));
    accepting(ingest, "the ingest listener");

    let path = format!("/ingest/{CYCLE_INGEST}");
    let post = |hops: Option<u32>| {
        let value = hops.map(|count| count.to_string());
        let headers: Vec<(&str, &str)> = value
            .as_deref()
            .map(|value| vec![("sd-hops", value)])
            .unwrap_or_default();
        http_with(
            ingest,
            "POST",
            &path,
            INGEST_TOKEN,
            &headers,
            Some(b"round and round"),
        )
    };

    // **The first post carries no count**, which is every caller that
    // knows nothing of hop counts and is the ordinary case.
    let first = post(None);
    assert_eq!(
        first.status, 200,
        "the first post is an ordinary one: {}",
        first.body
    );

    // Round the circle: read what the rule posted out, hand the count
    // back in, and stop when the broker says the cycle ends here.
    let mut carried = Vec::new();
    let mut seen = 0usize;
    let last = loop {
        endpoint.wait_for(
            seen + 1,
            "the rule posts what the ingest endpoint published",
        );
        let envelope = endpoint.requests()[seen].clone();
        seen += 1;
        let hops = envelope.hops();
        carried.push(hops);
        let answer = post(Some(hops));
        if answer.status != 200 {
            break answer;
        }
        assert!(
            seen < 10,
            "the cycle is not stopping: the counts so far are {carried:?}"
        );
    };

    assert_eq!(
        carried,
        vec![1, 2, 3],
        "each turn of the cycle is one hop further round, and the last one \
         the broker carried was at the limit"
    );
    assert_eq!(
        last.status, 508,
        "the post that would have taken it over is refused, and 508 is \
         Loop Detected: {}",
        last.body
    );
    assert!(
        last.body.contains("loop_detected"),
        "in this project's one error shape: {}",
        last.body
    );
    assert_eq!(
        endpoint.count(),
        3,
        "and the rule fired three times, not four: the refused publication \
         reached no subscriber and no rule"
    );

    // Section 9.2 says dropped **and counted**, and the counter is the
    // whole of what an operator has to find a cycle with.
    let exposition = http(admin, "GET", "/metrics", ADMIN_TOKEN, None).body;
    assert!(
        exposition.contains("sdmqtt_publications_over_hop_limit_total 1"),
        "the drop is counted: {exposition}"
    );

    broker.kill();
}

// ---- task 4: a poller comes back -----------------------------------------

/// How long between the start of one fetch and the next, for the poller
/// below.
///
/// Short, because the test's evidence is a count of fetches over a
/// handful of intervals and a long one would make the test a sleep.
const POLL_INTERVAL: Duration = Duration::from_millis(300);

/// The polling rule's identifier.
const POLL_RULE: &str = "readings";

/// Where the poller publishes what it fetched.
const POLL_TOPIC: &str = "polled/readings";

/// What the endpoint answers a fetch with.
const READING: &[u8] = b"21.5";

/// A `[[rules]]` table fetching `url` on [`POLL_INTERVAL`].
fn rule_polling(url: &str) -> String {
    format!(
        r#"
[[rules]]
id = "{POLL_RULE}"
name = "Readings in"

[rules.http_poll]
url = "{url}"
interval_ms = {interval}
timeout_ms = 2000
topic = "{POLL_TOPIC}"
qos = 1
"#,
        interval = POLL_INTERVAL.as_millis(),
    )
}

/// **A poller resumes on its interval after a restart.** Task 4.
///
/// A poller has no outbox and nothing durable of its own — that is the
/// connector rather than an omission, because a fetch that failed has
/// lost nothing and is simply asked again. So what a restart has to
/// prove here is not that something survived but that the schedule
/// starts again by itself: the rule is read back out of the store, the
/// singleton is claimed again, and the interval runs.
///
/// **Killed, not stopped.** A clean close of the store persists
/// everything, so a politely restarted broker would prove nothing about
/// what was written down — and the rule this poller is restored from is
/// written down. [`BrokerProcess::kill`] asserts the signal.
///
/// The middle assertion is the one that makes this a restart test: the
/// endpoint is asked nothing at all while the process is dead. Without
/// it, an endpoint that had simply gone on being polled by a broker
/// nobody killed would pass.
#[test]
fn a_poller_resumes_on_its_interval_after_a_restart() {
    let endpoint = Endpoint::start();
    endpoint.answers(READING);

    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let mqtt = loopback(free_port());
    let admin = loopback(free_port());
    let ingest = loopback(free_port());
    write_config(
        &config,
        &store,
        mqtt,
        admin,
        ingest,
        &rule_polling(&endpoint.url()),
    );

    let broker = BrokerProcess::start(&config, &store, mqtt, workspace.path().join("first.log"));
    endpoint.wait_for(2, "the poller fetches on its interval before the kill");

    // A subscriber sees what was fetched, which is the half of the
    // connector a count of requests cannot see.
    let mut before = Client::connect(mqtt, "watcher-before");
    before.subscribe(POLL_TOPIC);
    let delivered = before
        .delivery_within(PATIENCE)
        .expect("the poller publishes what it fetched");
    assert_eq!(delivered.topic, POLL_TOPIC);
    assert_eq!(&delivered.payload[..], READING);
    drop(before);

    let fetched_by_the_kill = endpoint.count();
    broker.kill();

    // **Nothing is fetched while there is no broker.** Several
    // intervals of it, so this is a real absence rather than a race
    // with the next one.
    std::thread::sleep(POLL_INTERVAL * 5);
    assert_eq!(
        endpoint.count(),
        fetched_by_the_kill,
        "a killed broker polls nothing; if this grew, the test is watching \
         something it did not kill"
    );

    let broker = BrokerProcess::start(&config, &store, mqtt, workspace.path().join("again.log"));

    // And the schedule runs again. Three more, so this is an interval
    // rather than a single fetch at startup.
    endpoint.wait_for(
        fetched_by_the_kill + 3,
        "the poller resumes on its interval after the restart",
    );

    let mut after = Client::connect(mqtt, "watcher-after");
    after.subscribe(POLL_TOPIC);
    let delivered = after
        .delivery_within(PATIENCE)
        .expect("and publishes what it fetched, to a subscriber that was not there before");
    assert_eq!(delivered.topic, POLL_TOPIC);
    assert_eq!(&delivered.payload[..], READING);

    broker.kill();
}

/// Section 11.1, against the real binary: a rule written over the API
/// fires, and still fires after the process has been killed.
///
/// **The configuration file has no `[[rules]]` table at all.** That is
/// what makes this test about the API rather than about the file: the
/// only thing that ever said this rule exists is one HTTP request, and
/// the only thing that can make it fire again after a `SIGKILL` is the
/// store.
///
/// Convention 5, twice over. A rule appended to a list in memory
/// satisfies every assertion about what the API answered, and is
/// caught by the endpoint seeing a request; a rule installed but never
/// stored satisfies that too, and is caught by the restart. Neither
/// half is redundant, because each is the other's failure mode.
#[test]
fn a_rule_created_over_the_api_fires_and_still_fires_after_a_restart() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let endpoint = Endpoint::start();
    let mqtt = loopback(free_port());
    let admin = loopback(free_port());
    let ingest = loopback(free_port());

    // No rules. Everything this test is about arrives over HTTP.
    write_config(&config, &store, mqtt, admin, ingest, "");
    let broker = BrokerProcess::start(
        &config,
        &store,
        mqtt,
        workspace.path().join("before-the-restart.log"),
    );
    accepting(admin, "the admin listener");

    let listed = http(admin, "GET", "/api/v1/rules", ADMIN_TOKEN, None);
    assert_eq!(listed.status, 200, "listing rules: {}", listed.body);
    assert!(
        listed.body.contains("\"total\":0"),
        "this broker was started with no rules and says it has some: {}",
        listed.body
    );

    let written = format!(
        r#"{{"id":"{API_RULE}","name":"Made over the API","filter":"{API_FILTER}",
             "http_out":{{"url":"{url}","timeout_ms":2000,"retries":2,"backoff_ms":200}}}}"#,
        url = endpoint.url(),
    );
    let created = http_with(
        admin,
        "POST",
        "/api/v1/rules",
        ADMIN_TOKEN,
        // The one header axum's JSON extractor insists on, which the
        // ingest endpoint beside it deliberately does not.
        &[("Content-Type", "application/json")],
        Some(written.as_bytes()),
    );
    assert_eq!(created.status, 200, "creating a rule: {}", created.body);

    // At least once, so the publication is durable before the
    // acknowledgement — which is also what flushes the envelope the
    // hook put in the outbox, since this configuration tells the
    // writer never to flush on its own.
    let mut client = Client::connect(mqtt, "before-the-restart");
    client.publish_and_wait(API_TOPIC, BEFORE_THE_RESTART);
    endpoint.wait_for(
        1,
        "a rule created over the API did not fire; it went into a list rather than \
         into the broker",
    );
    assert_eq!(
        endpoint.requests()[0].field("topic"),
        API_TOPIC,
        "the endpoint was posted something other than the message that fired the rule"
    );

    // No destructor, no final flush, no clean close of the store.
    broker.kill();

    let broker = BrokerProcess::start(
        &config,
        &store,
        mqtt,
        workspace.path().join("after-the-restart.log"),
    );
    accepting(admin, "the admin listener after the restart");

    let read = http(
        admin,
        "GET",
        &format!("/api/v1/rules/{API_RULE}"),
        ADMIN_TOKEN,
        None,
    );
    assert_eq!(
        read.status, 200,
        "the rule created over the API did not survive the restart: {}",
        read.body
    );
    assert!(
        read.body.contains(API_FILTER),
        "the rule came back as something else: {}",
        read.body
    );

    let mut client = Client::connect(mqtt, "after-the-restart");
    client.publish_and_wait(API_TOPIC, AFTER_THE_RESTART);
    endpoint.wait_for(
        2,
        "the rule was read back after the restart but did not fire, so it was restored \
         as a row and not as a running rule",
    );
    let seen = endpoint.requests();
    assert_eq!(
        seen[1].field("topic"),
        API_TOPIC,
        "the second post was not the message published after the restart"
    );
    assert_ne!(
        seen[0].field("id"),
        seen[1].field("id"),
        "the endpoint saw the same envelope twice rather than two publications"
    );

    drop(broker);
}
