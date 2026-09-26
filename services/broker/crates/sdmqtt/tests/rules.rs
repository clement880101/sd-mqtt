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

        std::thread::spawn({
            let received = Arc::clone(&received);
            let accepting = Arc::clone(&accepting);
            let stopped = Arc::clone(&stopped);
            move || {
                while !stopped.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let received = Arc::clone(&received);
                            let accepting = Arc::clone(&accepting);
                            std::thread::spawn(move || {
                                serve(stream, &received, &accepting);
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
        }
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
fn serve(mut stream: TcpStream, received: &Arc<Mutex<Vec<Received>>>, accepting: &AtomicBool) {
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

    let status = if accepting.load(Ordering::SeqCst) {
        "204 No Content"
    } else {
        "503 Service Unavailable"
    };
    let _ = stream.write_all(
        format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes(),
    );
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
fn write_config(path: &Path, store: &Path, mqtt: SocketAddr, admin: SocketAddr, rule: &str) {
    let text = format!(
        r#"
[listener]
address = "{mqtt}"

[admin]
address = "{admin}"
token = "a-secret-for-the-test"

[storage]
directory = "{store}"
sync = "immediate"
flush_interval_ms = {interval}
flush_records = 1000000000

[log]
level = "info"

{rule}
"#,
        store = store.display(),
        interval = NEVER.as_millis(),
    );
    std::fs::write(path, text).expect("the configuration file can be written");
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
