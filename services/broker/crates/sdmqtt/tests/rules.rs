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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

/// How long the broker has to exit after a stop signal.
///
/// Longer than `sdmqtt_admin::SHUTDOWN_GRACE`, deliberately: a
/// listener that reached its own backstop and a listener that never
/// finished are different failures, and a deadline shorter than the
/// backstop could not tell them apart.
const TO_STOP: Duration = Duration::from_secs(30);

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
    /// Connections no request was read from.
    ///
    /// **A request thrown away used to be invisible**, and that is what
    /// made one cause look like three unrelated flaky tests: a read
    /// that had not completed yet was read as a peer that had closed,
    /// the request went nowhere, and the only trace was a test waiting
    /// for an attempt the broker had in fact made. Counted so that the
    /// next one says so — see [`Endpoint::wait_for`], which reports it.
    abandoned: Arc<AtomicUsize>,
    accepting: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    /// Which request to record and then never answer, counted from one.
    ///
    /// Zero is every request answered, which is what every test but one
    /// wants. See [`Endpoint::stall_request`] for the one.
    stall: Arc<AtomicUsize>,
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
        let abandoned = Arc::new(AtomicUsize::new(0));
        let accepting = Arc::new(AtomicBool::new(true));
        let stopped = Arc::new(AtomicBool::new(false));
        let body = Arc::new(Mutex::new(Vec::new()));
        let stall = Arc::new(AtomicUsize::new(0));

        std::thread::spawn({
            let received = Arc::clone(&received);
            let abandoned = Arc::clone(&abandoned);
            let accepting = Arc::clone(&accepting);
            let stopped = Arc::clone(&stopped);
            let body = Arc::clone(&body);
            let stall = Arc::clone(&stall);
            move || {
                while !stopped.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let received = Arc::clone(&received);
                            let abandoned = Arc::clone(&abandoned);
                            let accepting = Arc::clone(&accepting);
                            let body = Arc::clone(&body);
                            let stall = Arc::clone(&stall);
                            let stopped = Arc::clone(&stopped);
                            std::thread::spawn(move || {
                                serve(
                                    stream, &received, &abandoned, &accepting, &body, &stall,
                                    &stopped,
                                );
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
            abandoned,
            accepting,
            stopped,
            body,
            stall,
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

    /// Record the `which`th request, counted from one, and never answer
    /// it.
    ///
    /// **What makes a kill land in a known place.** A test that kills
    /// the broker while an attempt is in flight otherwise has to guess
    /// where the pump is, because an attempt against a refusing endpoint
    /// takes microseconds and the backoff between attempts takes
    /// milliseconds — so whether the kill lands between an attempt and
    /// the record of it is up to the scheduler. Held open, the pump
    /// cannot leave that point until the rule's own request deadline, so
    /// the state the kill interrupts is the one the test named.
    fn stall_request(&self, which: usize) {
        self.stall.store(which, Ordering::SeqCst);
    }

    /// Answer everything again, including whatever is being held.
    fn stall_nothing(&self) {
        self.stall.store(0, Ordering::SeqCst);
    }

    fn requests(&self) -> Vec<Received> {
        self.received.lock().expect("not poisoned").clone()
    }

    fn count(&self) -> usize {
        self.received.lock().expect("not poisoned").len()
    }

    /// How many connections it read no request from.
    fn abandoned(&self) -> usize {
        self.abandoned.load(Ordering::SeqCst)
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
            "{what}: the endpoint saw {} requests and was waiting for {wanted}, having read \
             no request at all from {} connection(s)",
            self.count(),
            self.abandoned()
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
    abandoned: &AtomicUsize,
    accepting: &AtomicBool,
    body_to_answer: &Arc<Mutex<Vec<u8>>>,
    stall: &AtomicUsize,
    stopped: &AtomicBool,
) {
    // **This socket inherited the listener's non-blocking flag**, and
    // putting it back is the whole of one bug. The listener is
    // non-blocking so that the accept loop can notice [`Endpoint`] being
    // dropped without a connection arriving to wake it, and `accept` on
    // this platform hands back a descriptor carrying that flag — which
    // POSIX does not require and Linux does not do, so it is not
    // something reading the code here would suggest.
    //
    // On a non-blocking socket the deadline below does nothing and the
    // first read returns `WouldBlock` whenever the request has not
    // landed yet, which is sometimes: `accept` returns when the
    // handshake completes and the bytes follow. Read as a peer that had
    // closed, that threw the request away and recorded nothing, and the
    // broker saw a connector that had closed on it mid-request. One
    // failure in twenty-five in each of three tests in this file, with
    // three different symptoms, which is why it looked like three bugs:
    // an attempt waited for that never arrived, an attempt made twice
    // where the second was a retry of the first, and an attempt spent
    // against a budget nothing counted.
    stream
        .set_nonblocking(false)
        .expect("the accepted socket goes back to blocking reads");
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
            // A signal is not an answer about the request; everything
            // else here is the connection ending before one arrived.
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Ok(0) | Err(_) => {
                abandoned.fetch_add(1, Ordering::SeqCst);
                return;
            }
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
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Ok(0) | Err(_) => {
                // **Abandoned rather than recorded short.** A body that
                // stopped early is not what the broker sent, and
                // recording it makes the lie surface somewhere else: as
                // an envelope with no `sd-hops` property, or no `topic`,
                // in a test about neither.
                abandoned.fetch_add(1, Ordering::SeqCst);
                return;
            }
            Ok(read) => body.extend_from_slice(&chunk[..read]),
        }
    }

    let which = {
        let mut held = received.lock().expect("not poisoned");
        held.push(Received { body });
        held.len()
    };

    // Recorded and then held, which is what a test waiting for this
    // request is waiting for: the attempt has reached the endpoint and
    // the pump is still inside it. Released when the test says so, or
    // when the endpoint shuts down, so nothing here outlives the test
    // that asked for it.
    while stall.load(Ordering::SeqCst) == which && !stopped.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(5));
    }

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
    write_config_with(path, store, mqtt, admin, ingest, "own", tables);
}

/// The same, saying which listener serves the ingest routes.
///
/// Section 9.2 allows either, and the difference matters to more than
/// which port answers: a listener of this node's own is bound while
/// something needs it, and the admin listener is bound regardless. A
/// rule created over the API has to be reachable on both.
fn write_config_with(
    path: &Path,
    store: &Path,
    mqtt: SocketAddr,
    admin: SocketAddr,
    ingest: SocketAddr,
    ingest_listener: &str,
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
listener = "{ingest_listener}"

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
    rule_posting_to_within(url, retries, backoff_ms, 2_000)
}

/// The same, saying how long one attempt may take.
///
/// One caller sets it, and it is the one that holds a request open
/// deliberately: see [`Endpoint::stall_request`]. The deadline there has
/// to outlast the test noticing the request and killing the process, or
/// the pump would give up on its own and the kill would land somewhere
/// else again.
fn rule_posting_to_within(url: &str, retries: u8, backoff_ms: u32, timeout_ms: u32) -> String {
    format!(
        r#"
[[rules]]
id = "{RULE}"
name = "Telemetry out"
filter = "sensor/+/temp"

[rules.http_out]
url = "{url}"
timeout_ms = {timeout_ms}
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

    /// Wait until the broker **answers MQTT**, not until its port
    /// accepts.
    ///
    /// Those are different instants and the gap between them is a
    /// real one: the listener is bound a moment before the process is
    /// warm, and on a machine running several of these tests at once,
    /// each spawning its own broker, that moment can be seconds. A
    /// readiness check that stops at the accept leaves the test body
    /// as the first thing to require an answer, so a slow start
    /// arrives as a failed assertion about the feature under test
    /// rather than as a slow readiness wait. That is what it was:
    /// one run in twenty-five failed inside `Client::connect`,
    /// expecting an acknowledgement, twenty seconds after a successful
    /// connect.
    ///
    /// So the probe completes a round trip. It uses a clean start and
    /// no session expiry, under an identifier no test uses, so it
    /// leaves nothing of itself behind for the test that follows.
    fn wait_until_listening(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + STARTUP;
        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!("it exited with {status}: {}", self.logged()));
            }
            if answers_mqtt(self.address) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(format!(
            "it did not answer MQTT on {} within {STARTUP:?}: {}",
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

    /// Ask it to stop the way a deployment does.
    ///
    /// `SIGTERM` rather than `Child::kill`, which is `SIGKILL` and
    /// runs no destructor: what a test using this wants to know is
    /// whether the **graceful** path terminates. Sent with `kill`
    /// rather than a signalling crate, because a dependency for one
    /// line of a test is a dependency to audit.
    #[cfg(unix)]
    fn terminate(&self) {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .expect("kill runs");
        assert!(status.success(), "the signal was delivered");
    }

    /// Wait for it to exit, and say how.
    ///
    /// Bounded, because the regression this guards is a process that
    /// never exits and a test that hangs stalls a build instead of
    /// reporting.
    #[cfg(unix)]
    fn wait_for_exit(&mut self) -> Result<std::process::ExitStatus, String> {
        let deadline = Instant::now() + TO_STOP;
        while Instant::now() < deadline {
            match self.child.try_wait() {
                Ok(Some(status)) => return Ok(status),
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(error) => return Err(format!("it could not be waited for: {error}")),
            }
        }
        Err(format!(
            "it was still running {TO_STOP:?} after the signal: {}",
            self.logged()
        ))
    }
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The lowest port these tests will use, and one past the highest.
///
/// **Below every platform's ephemeral range, deliberately.** Linux
/// draws automatic ports from 32768 upwards, macOS and Windows from
/// 49152, so nothing in this window is ever *assigned* by the kernel:
/// not a bind of port zero and not the source port of an outbound
/// connection. That is the whole difference between a reservation and a
/// hint.
///
/// A port in here can still be in use by something already running on
/// the machine, which is why one is taken by binding it and moving on if
/// that fails, rather than by assuming it is free.
const PORTS: std::ops::Range<u16> = 20_000..30_000;

/// Every port this test binary has taken, including the ones it has
/// already let go of.
///
/// **A port read from a bind of port zero is a hint and not a
/// reservation**, and that was the whole of one flaky test. The listener
/// that chose it is dropped before the caller can use the number, so the
/// port is free again at once and anything on the machine may be given
/// it next: another of these tests, a broker child process binding a
/// listener, or the kernel picking a source port for any outbound
/// connection any of them makes. Measured at one failure in six with the
/// machine loaded, none on a quiet one and none single-threaded, which
/// is the shape of exactly that.
///
/// Drawing from [`PORTS`] removes the kernel as a competitor. This
/// removes the siblings: a port is spent for the life of the process and
/// never handed out twice, so a test that has released an address can
/// still rely on nothing here having taken it.
static HANDED_OUT: Mutex<Vec<u16>> = Mutex::new(Vec::new());

/// A listener holding a port nothing will take from it, and that port.
///
/// For the one test that has to **hold** an address and then free it: it
/// keeps the listener while it wants the address taken and drops it to
/// free the address. Between the two the port is not something the
/// kernel can hand to anybody, because it is outside the range the
/// kernel assigns from, and not something a sibling can be given,
/// because it stays spent.
fn reserved_port() -> (TcpListener, u16) {
    // Started somewhere different in each process so that two runs in
    // quick succession do not queue up on the same first candidate.
    // Hashed rather than the id itself: under nextest each test is a
    // process, and consecutive ids one port apart collided in CI. See
    // `first_candidate` in tests/cluster.rs.
    let offset = usize::try_from(
        u64::from(std::process::id()).wrapping_mul(2_654_435_761) % PORTS.len() as u64,
    )
    .unwrap_or(0);
    for step in 0..PORTS.len() {
        let port = PORTS.start + u16::try_from((offset + step) % PORTS.len()).unwrap_or(0);
        if !first_time(port) {
            continue;
        }
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) {
            return (listener, port);
        }
        // Something else on the machine has it. Spent either way, so
        // nothing here tries it twice.
    }
    panic!(
        "no port between {} and {} could be bound",
        PORTS.start, PORTS.end
    );
}

/// Spend `port`, and say whether this binary had it already.
///
/// The decision the whole registry comes down to, in a function of its
/// own so that it can be tested without a second thread: see
/// [`a_port_this_binary_has_taken_is_never_handed_out_again`].
fn first_time(port: u16) -> bool {
    let mut handed = HANDED_OUT.lock().expect("not poisoned");
    if handed.contains(&port) {
        return false;
    }
    handed.push(port);
    true
}

/// A port is spent once, and a second claim on it is refused.
///
/// **The race itself is not something a test can schedule.** Two
/// parallel tests being handed one port depends on the kernel choosing
/// to do it, which is why the defect showed up as one failure in six
/// under load and never on a quiet machine. So what is guarded here is
/// the decision that race turns on, and the number is one outside
/// [`PORTS`] so that this test cannot be made to fail by whatever ports
/// its siblings took.
#[test]
fn a_port_this_binary_has_taken_is_never_handed_out_again() {
    let outside = PORTS.end;
    assert!(
        first_time(outside),
        "a port nothing had claimed was reported as already spent"
    );
    assert!(
        !first_time(outside),
        "a port already handed out was offered a second time, so a port here is a hint \
         rather than a reservation and two tests in parallel can be given one address"
    );
}

/// A request whose bytes arrive after its connection does is recorded.
///
/// **The one cause under three flaky tests in this file**, and it was
/// the harness rather than the broker: see the note at the top of
/// [`serve`] for what the socket does and why nothing in the code there
/// suggested it.
///
/// The race itself is the scheduler's — whether the request has landed
/// by the time `serve` first reads — so what is guarded here is the
/// condition it turns on, in the shape convention 5 asks for: the gap is
/// made deliberate, so a `serve` that cannot wait for the bytes records
/// nothing and this fails every time rather than once in twenty-five.
///
/// The head and the body are written apart, because the second read is
/// the one that used to record a short envelope rather than nothing, and
/// the two surface in different places: a missing attempt, and an
/// envelope with no fields in a test about neither.
#[test]
fn a_request_whose_bytes_arrive_after_its_connection_is_recorded() {
    const LATE_BODY: &[u8] = br#"{"topic":"written after the connection"}"#;
    /// Long enough that the connection has been accepted and whoever is
    /// serving it has reached a read with nothing yet to read.
    const GAP: Duration = Duration::from_millis(250);

    let endpoint = Endpoint::start();
    let mut stream = TcpStream::connect(endpoint.address).expect("the endpoint accepts");
    std::thread::sleep(GAP);
    write!(
        stream,
        "POST /hook HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
        LATE_BODY.len()
    )
    .expect("the head is written");
    stream.flush().expect("and sent");
    std::thread::sleep(GAP);
    stream.write_all(LATE_BODY).expect("the body is written");
    stream.flush().expect("and sent");

    endpoint.wait_for(1, "a request written after its connection was accepted");
    assert_eq!(
        endpoint.requests()[0].body,
        LATE_BODY,
        "the endpoint recorded a body other than the one it was sent"
    );
    // Beside the assertion above rather than instead of it: convention 5
    // on tests that assert an absence. What carries this test is the
    // body; this says which way it failed when it does.
    assert_eq!(
        endpoint.abandoned(),
        0,
        "the endpoint read no request from a connection that carried one"
    );
}

/// A request that stops partway is not recorded as if it had arrived.
///
/// The other half of the note at the top of [`serve`]. A body that
/// stopped early is not what the broker sent, and recording it short
/// moves the failure somewhere it makes no sense: an envelope with no
/// `topic` and no `sd-hops`, in a test about neither. It is counted as
/// abandoned instead, which is what [`Endpoint::wait_for`] reports when
/// it gives up — so the next time this happens the message says so.
#[test]
fn a_request_that_stops_partway_is_not_recorded() {
    let endpoint = Endpoint::start();
    let mut stream = TcpStream::connect(endpoint.address).expect("the endpoint accepts");
    // A length the body will not reach, and then a close.
    write!(stream, "POST /hook HTTP/1.1\r\nContent-Length: 64\r\n\r\n")
        .expect("the head is written");
    stream
        .write_all(b"the first few bytes and no more")
        .expect("and part of the body");
    stream.flush().expect("and sent");
    drop(stream);

    let deadline = Instant::now() + PATIENCE;
    while endpoint.abandoned() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        endpoint.count(),
        0,
        "a request that stopped partway was recorded as though it had all arrived: {:?}",
        endpoint.requests()
    );
    assert_eq!(
        endpoint.abandoned(),
        1,
        "and the endpoint has to say it threw one away, or the next flake is silent too"
    );
}

/// A port nothing else will be given, with nothing listening on it.
///
/// What a test hands to a broker it is about to start. The listener goes
/// immediately, because the broker has to bind it — and nothing can take
/// it in between: see [`PORTS`] and [`HANDED_OUT`].
fn free_port() -> u16 {
    let (listener, port) = reserved_port();
    drop(listener);
    port
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

/// What lies between `open` and the next `close`.
///
/// A search rather than a parse, for the reason [`Received::field`]
/// gives: a renamed key or a changed shape is a failure here too,
/// which a tolerant parser would let through.
#[track_caller]
fn between(text: &str, open: &str, close: &str) -> String {
    let start = text
        .find(open)
        .unwrap_or_else(|| panic!("no {open} in {text}"))
        + open.len();
    let rest = &text[start..];
    rest[..rest.find(close).expect("the value is closed")].to_owned()
}

/// Wait until nothing is accepting on `address`.
///
/// The other half of [`accepting`], and it waits for the same reason:
/// closing a listener is not the same instant as the request that
/// caused it being answered, so a single attempt would be a race. It
/// is an assertion about an absence, which convention 5 warns about —
/// it is here beside the assertions that something *was* reachable,
/// and it is those that carry the test.
#[track_caller]
fn refusing(address: SocketAddr, what: &str) {
    let deadline = Instant::now() + PATIENCE;
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_err() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("{what} is still accepting on {address}");
}

/// Which protocols the broker says its listeners serve.
///
/// A positive answer from the broker's own inventory, for assertions
/// that would otherwise have to probe a port and so would depend on
/// nothing else on the machine happening to hold it.
fn listener_protocols(admin: SocketAddr, token: &str) -> Vec<String> {
    let answered = http(admin, "GET", "/api/v1/listeners", token, None);
    assert_eq!(
        answered.status, 200,
        "the listeners resource answers: {}",
        answered.body
    );
    let page: serde_json::Value =
        serde_json::from_str(&answered.body).expect("the listeners resource answers JSON");
    page["items"]
        .as_array()
        .expect("a page of listeners has items")
        .iter()
        .map(|item| {
            item["protocol"]
                .as_str()
                .expect("a listener says what it serves")
                .to_owned()
        })
        .collect()
}

/// Whether a broker on `address` answers a connect with an
/// acknowledgement.
///
/// Every failure is a `false` rather than a panic: this is asked in a
/// loop while a process is starting, so a refused connection, a read
/// that times out and a half-written reply are all simply "not yet".
/// The one thing it must not do is report success for anything other
/// than an acknowledgement, which is the whole of what it is for.
fn answers_mqtt(address: SocketAddr) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(200)) else {
        return false;
    };
    if stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .is_err()
    {
        return false;
    }
    let connect = Packet::Connect(Box::new(Connect {
        version: Version::V5,
        client_id: "$sdmqtt-readiness-probe".into(),
        clean_start: true,
        keep_alive: 0,
        username: None,
        password: None,
        will: None,
        properties: Properties::default(),
    }));
    let mut out = BytesMut::new();
    if encode(&connect, Version::V5, &mut out).is_err() || stream.write_all(&out).is_err() {
        return false;
    }
    let mut buf = BytesMut::new();
    let mut chunk = [0u8; 512];
    loop {
        match decode(&mut buf, Version::V5, &Limits::default()) {
            Ok(Some(Packet::ConnAck(ack))) => return ack.reason == ReasonCode::Success,
            Ok(Some(_)) | Err(_) => return false,
            Ok(None) => {}
        }
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return false,
            Ok(read) => buf.extend_from_slice(&chunk[..read]),
        }
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
/// is the rule's plus the one that was in flight, and with a count that
/// restarts with the process it is the rule's every time the process
/// starts.
///
/// # Where the kill lands, and why that had to be said
///
/// The pump records an attempt **after** it has made it, and waits for
/// that record to be durable before it makes another. So an attempt that
/// is in flight when the process dies is an attempt the store never
/// heard of, and the node that comes back makes it again: the budget is
/// a budget per stored count, and a crash can cost it one extra attempt.
/// That is the design and it is not free to change — recording before
/// the request would cap the budget and would cost a second flush on
/// every *successful* delivery, which is the path that has to be fast.
///
/// This test used to leave that implicit and was flaky because of it. It
/// waited for two attempts and killed the broker, and whether the third
/// attempt had happened by then, and if so whether its record had
/// reached the disk, was up to the scheduler: a kill between an attempt
/// and its record gave a total of five where the test asserted four.
/// Three failures across the milestone, all of them that.
///
/// It then failed once more in twenty-five, and that one was something
/// else entirely: the endpoint threw a request away, so an attempt was
/// spent that nothing counted. See
/// [`a_request_whose_bytes_arrive_after_its_connection_is_recorded`].
///
/// So the kill is now put where the test means it to be. The endpoint
/// holds the third request open, which pins the pump inside an attempt
/// it cannot finish, and the state the kill interrupts is therefore
/// exact: two attempts recorded, one in flight and unrecorded. The total
/// below is five for a stated reason rather than four for a hopeful one,
/// and a count that did not survive at all would still be six.
#[test]
fn the_attempt_count_survives_the_kill() {
    const RETRIES: u8 = 3;
    /// Which attempt is held open when the broker is killed.
    const HELD: usize = 3;

    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let endpoint = Endpoint::start();
    let mqtt = loopback(free_port());
    let admin = loopback(free_port());
    write_config(
        &config,
        &store,
        mqtt,
        admin,
        loopback(free_port()),
        // A request deadline far longer than this test needs to notice
        // the held request and kill the process, so the pump is still
        // inside that attempt when it dies.
        &rule_posting_to_within(&endpoint.url(), RETRIES, 50, 60_000),
    );
    endpoint.go_down();
    endpoint.stall_request(HELD);

    let broker = BrokerProcess::start(&config, &store, mqtt, workspace.path().join("first.log"));
    {
        let mut client = Client::connect(mqtt, "publisher");
        client.publish_and_wait(TOPIC, SECOND);
    }
    // The first two attempts are answered and recorded; the third is
    // answered by nobody, so the pump is held inside it. The record of
    // an attempt is durable before the next is made, so a third request
    // having arrived means the first two are on the disk.
    endpoint.wait_for(
        HELD,
        "three attempts must be made before the broker is killed",
    );
    broker.kill();

    let before_the_kill = endpoint.count();
    assert_eq!(
        before_the_kill, HELD,
        "the endpoint saw {before_the_kill} attempts, and the third was being held open, \
         so the pump cannot have made a fourth"
    );
    // Whatever is still holding that request can answer now: the socket
    // it would answer on died with the process, and the attempts after
    // the restart must be refused like the first two.
    endpoint.stall_nothing();

    let _restarted =
        BrokerProcess::start(&config, &store, mqtt, workspace.path().join("second.log"));
    // **The admin listener binds last**, after the MQTT listener this
    // test's start already waited for and after the ingest one; a
    // request made before it is bound is a connection refused rather
    // than a retry.
    accepting(admin, "the admin listener after the restart");
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

    // **And the same letter is in the resource**, which is where
    // somebody who was not subscribed at the moment finds it. The two
    // are one event: the topic is section 9.2's and the resource is
    // section 11.1's, and a page showing one letter while a subscriber
    // saw another would be two answers to one question.
    //
    // Matched on the envelope's own identifier rather than on the
    // resource being non-empty, because non-empty is what a resource
    // that invented a letter would also be.
    let id = between(&document, "\"id\":\"", "\"");
    let kept = until(
        admin,
        &format!("/api/v1/rules/{RULE}/dead-letters"),
        "the dead letter reaches the resource",
        |body| body.contains(&id),
    );
    assert!(
        kept.contains("\\\"attempts\\\":4"),
        "the resource kept something other than the document that was published: {kept}"
    );
    assert!(
        kept.contains(DLQ),
        "the resource does not say which topic the letter went to: {kept}"
    );
    assert!(
        kept.contains("\"truncated\":false"),
        "a letter that fits was reported as cut: {kept}"
    );

    // **The assertion the durable count exists for.** The endpoint has
    // been refusing throughout and has never recovered, so the total
    // number of requests it has seen across both of the broker's lives
    // is the rule's attempt budget, plus the one attempt that was in
    // flight when the process died and that the store therefore never
    // heard of. Two of the four were recorded before the kill, so two
    // follow the restart: three requests, then two, which is five. A
    // count that restarted with the process would spend the whole budget
    // again and the total would be seven.
    let spent_before_the_kill = HELD - 1;
    let total = endpoint.count();
    assert_eq!(
        total,
        usize::from(RETRIES) + 1 + 1,
        "a failing endpoint received {total} attempts across a restart; the rule allows {}, \
         {spent_before_the_kill} of them were recorded before the kill and one more was in \
         flight and unrecorded, so {} is the whole of what this node may do",
        u32::from(RETRIES) + 1,
        usize::from(RETRIES) + 2
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
    // **The admin listener binds last**, after the MQTT listener this
    // test's start already waited for and after the ingest one; a
    // request made before it is bound is a connection refused rather
    // than a retry.
    accepting(admin, "the admin listener");

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
    // **The admin listener binds last**, after the MQTT listener this
    // test's start already waited for and after the ingest one; a
    // request made before it is bound is a connection refused rather
    // than a retry.
    accepting(admin, "the admin listener");

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
///
/// It failed once in twenty-five, and the cause was not in the cycle:
/// the endpoint threw a request away, and with `retries = 0` the
/// envelope was dead-lettered rather than tried again, so the turn of
/// the circle this waits for never came. See
/// [`a_request_whose_bytes_arrive_after_its_connection_is_recorded`].
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
    // The admin listener binds last; see the note in the test above.
    accepting(admin, "the admin listener");

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
        exposition.contains(r#"sdmqtt_publications_refused_total{reason="too_many_hops"} 1"#),
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

    // **Wait for the rule to be finished with it before killing the
    // broker.** The endpoint records a request when it reads one,
    // which is before it has answered and long before the pump has
    // made the envelope's removal durable; a kill in that window
    // leaves the envelope in the outbox, and the restarted broker
    // delivers it again. The second request would then be the first
    // envelope coming back rather than the second publication, which
    // is a different test passing under this one's name — and is what
    // the assertion on the two identifiers below caught.
    //
    // That wait is not the whole of it. This test failed once more in
    // twenty-five with the same symptom and a different cause: the
    // endpoint threw the first request away, the pump retried it, and
    // the two requests the assertion compares were one envelope twice.
    // See [`a_request_whose_bytes_arrive_after_its_connection_is_recorded`].
    until(
        admin,
        &format!("/api/v1/rules/{API_RULE}/stats"),
        "the first envelope is delivered and out of the outbox",
        |body| body.contains("\"delivered\":1") && body.contains("\"waiting\":0"),
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

    // Wait for the envelope carrying *this* publication, rather than
    // for a second request and then assuming it is this one.
    //
    // The two are not the same, and the difference is a guarantee
    // rather than a timing detail. An envelope's removal from the
    // outbox is durable at the next flush, not at the moment it was
    // delivered, so a broker killed just after a delivery redelivers
    // on the way back. That is at-least-once behaving as documented,
    // and it costs this test nothing — except that the redelivery can
    // be the second request to arrive, in which case asserting about
    // "the second" is asserting about the wrong one. It failed that
    // way once in thirty runs.
    let deadline = Instant::now() + PATIENCE;
    let fired = loop {
        if let Some(seen) = endpoint
            .requests()
            .into_iter()
            .find(|seen| seen.field("payload").as_bytes() == AFTER_THE_RESTART)
        {
            break seen;
        }
        assert!(
            Instant::now() < deadline,
            "the rule was read back after the restart but did not fire on a new \
             publication, so it was restored as a row and not as a running rule; the \
             endpoint saw {} request(s)",
            endpoint.count()
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        fired.field("topic"),
        API_TOPIC,
        "the envelope carrying the publication made after the restart names another topic"
    );

    drop(broker);
}

// ---- the ingest listener follows the rules -------------------------------

/// The identifier of the ingest rule these tests create over the API.
///
/// Nothing in any configuration file mentions it, and nothing in the
/// store holds it when the broker starts: it exists because a request
/// made it, which is what makes these tests about the listener rather
/// than about startup.
const LATE_RULE: &str = "made-over-the-api-in";

/// Its own bearer token, which is not the admin one.
const LATE_TOKEN: &str = "a-secret-for-the-late-rule";

/// Where it publishes.
const LATE_TOPIC: &str = "ingest/without-a-restart";

/// What is posted to it.
const POSTED_LATE: &[u8] = b"posted to a rule this broker did not start with";

/// What is posted after it is switched off and on again.
const POSTED_AGAIN: &[u8] = b"posted after the listener came back";

/// The body that creates [`LATE_RULE`] over the admin API.
fn late_ingest_rule() -> String {
    format!(
        r#"{{"id":"{LATE_RULE}","name":"Made over the API",
             "http_in":{{"token":"{LATE_TOKEN}","topic":"{LATE_TOPIC}","qos":1,
                         "retain":false}}}}"#
    )
}

/// Create a rule over the admin API, and say what came back.
fn create_rule(admin: SocketAddr, body: &str) -> Answered {
    http_with(
        admin,
        "POST",
        "/api/v1/rules",
        ADMIN_TOKEN,
        // The one header axum's JSON extractor insists on, which the
        // ingest endpoint beside it deliberately does not.
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
}

/// **The test milestone four's interface was failing.** An ingest rule
/// created over the API accepts a post without the broker being
/// restarted.
///
/// The broker starts with **no ingest rule at all**, so there is no
/// listener and nothing in the store to make one. Then one request
/// creates a rule and the very next request posts to it.
///
/// # What used to happen, and why it was worse than a refusal
///
/// The listener was bound during startup and only if this node held an
/// ingest rule then. So the create was stored, installed, listed,
/// counted and answered `200`, the Rules page drew it as live, and
/// every post to it was refused by a socket nobody was listening on
/// until the process was restarted. Every surface said it worked.
///
/// # Convention 5, and why the post is not the end of the test
///
/// A listener that answered `200` and published nothing would satisfy
/// a test that stopped at the status line, and so would one that
/// published to the wrong topic. The subscriber is attached before the
/// post and is what the payload has to reach, so what is asserted is
/// the thing an operator wanted rather than the answer they were
/// given.
#[test]
fn an_ingest_rule_created_over_the_api_is_reachable_without_a_restart() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let mqtt = loopback(free_port());
    let admin = loopback(free_port());
    let ingest = loopback(free_port());

    // No rules. Everything this test is about arrives over HTTP.
    write_config(&config, &store, mqtt, admin, ingest, "");
    let broker = BrokerProcess::start(
        &config,
        &store,
        mqtt,
        workspace.path().join("without-a-restart.log"),
    );
    accepting(admin, "the admin listener");

    // **Nothing on the ingest address yet**, which is the other half
    // of the decision this test records: a broker that does no ingest
    // binds no ingest port. The default port is a fixed number, so a
    // socket bound unconditionally would be one every broker on a
    // host contended for.
    assert!(
        TcpStream::connect_timeout(&ingest, Duration::from_millis(200)).is_err(),
        "a broker with no ingest rule is listening on {ingest}"
    );
    let listed = http(admin, "GET", "/api/v1/listeners", ADMIN_TOKEN, None);
    assert!(
        !listed.body.contains("\"ingest\""),
        "it reports an ingest listener it did not bind: {}",
        listed.body
    );

    // Somebody who will be waiting for what is posted.
    let mut subscriber = Client::connect(mqtt, "waiting-for-the-post");
    subscriber.subscribe(LATE_TOPIC);

    let created = create_rule(admin, &late_ingest_rule());
    assert_eq!(created.status, 200, "creating a rule: {}", created.body);

    // **No wait, no retry, no restart.** The answer to the create is
    // a promise about what can be reached, so the next thing here is
    // to reach it.
    assert!(
        TcpStream::connect_timeout(&ingest, PATIENCE).is_ok(),
        "the rule was created and answered 200, and nothing is listening on {ingest}: \
         a rule reported as live that no post can reach is the defect this test exists \
         for"
    );

    let answered = http(
        ingest,
        "POST",
        &format!("/ingest/{LATE_RULE}"),
        LATE_TOKEN,
        Some(POSTED_LATE),
    );
    assert_eq!(
        answered.status, 200,
        "posting to a rule created over the API: {}",
        answered.body
    );

    let delivered = subscriber
        .delivery_within(PATIENCE)
        .expect("what was posted must reach the subscriber, not only be answered 200");
    assert_eq!(
        delivered.topic, LATE_TOPIC,
        "it published somewhere other than where the rule says"
    );
    assert_eq!(
        &delivered.payload[..],
        POSTED_LATE,
        "something other than what was posted arrived"
    );

    // And the inventory says the port exists, which it did not have
    // to be asked about twice to get right.
    let listed = http(admin, "GET", "/api/v1/listeners", ADMIN_TOKEN, None);
    assert!(
        listed.body.contains(&format!("\"address\":\"{ingest}\"")),
        "the listener it bound for this rule is not in what it reports: {}",
        listed.body
    );

    drop(broker);
}

/// The listener is closed again when the last rule that needs it goes,
/// and bound again when one comes back.
///
/// The half of the decision that is not about reachability: a port
/// answering `404` to everything is one more thing for an operator to
/// explain, so a node that has stopped doing ingest stops holding the
/// socket for it. Disabling rather than deleting, because that is the
/// switch an operator reaches for and a listener that stayed up for a
/// rule that was switched off would be the one part of a connector
/// that ignored it.
///
/// **The rebind at the end is what makes this more than an assertion
/// about an absence.** A socket that closed and could never come back
/// would pass every line up to it.
#[test]
fn the_ingest_listener_comes_and_goes_with_the_rules_that_need_it() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let mqtt = loopback(free_port());
    let admin = loopback(free_port());
    let ingest = loopback(free_port());

    write_config(&config, &store, mqtt, admin, ingest, "");
    let broker = BrokerProcess::start(
        &config,
        &store,
        mqtt,
        workspace.path().join("comes-and-goes.log"),
    );
    accepting(admin, "the admin listener");

    let created = create_rule(admin, &late_ingest_rule());
    assert_eq!(created.status, 200, "creating a rule: {}", created.body);
    accepting(ingest, "the ingest listener the new rule needs");

    // Switched off: the route goes, and with it the only reason the
    // socket was open.
    let disabled = http(
        admin,
        "POST",
        &format!("/api/v1/rules/{LATE_RULE}/disable"),
        ADMIN_TOKEN,
        None,
    );
    assert_eq!(disabled.status, 200, "disabling it: {}", disabled.body);
    refusing(
        ingest,
        "the ingest listener of a node whose only ingest rule is off",
    );
    let listed = http(admin, "GET", "/api/v1/listeners", ADMIN_TOKEN, None);
    assert!(
        !listed.body.contains(&format!("\"address\":\"{ingest}\"")),
        "it still reports a listener it has closed: {}",
        listed.body
    );

    // And on again, which is the assertion an absence cannot make.
    let mut subscriber = Client::connect(mqtt, "waiting-after-the-rebind");
    subscriber.subscribe(LATE_TOPIC);
    let enabled = http(
        admin,
        "POST",
        &format!("/api/v1/rules/{LATE_RULE}/enable"),
        ADMIN_TOKEN,
        None,
    );
    assert_eq!(enabled.status, 200, "enabling it again: {}", enabled.body);

    let answered = http(
        ingest,
        "POST",
        &format!("/ingest/{LATE_RULE}"),
        LATE_TOKEN,
        Some(POSTED_AGAIN),
    );
    assert_eq!(
        answered.status, 200,
        "posting to a rule that was switched off and on again: {}",
        answered.body
    );
    let delivered = subscriber
        .delivery_within(PATIENCE)
        .expect("a rebound listener must publish what is posted to it, not only accept it");
    assert_eq!(&delivered.payload[..], POSTED_AGAIN);

    // And deleting the last one closes it too. A separate path from
    // disabling: a delete goes through the store and takes the rule
    // away entirely, and it would be perfectly possible to have taught
    // one of the two to close the socket and not the other.
    let deleted = http(
        admin,
        "DELETE",
        &format!("/api/v1/rules/{LATE_RULE}"),
        ADMIN_TOKEN,
        None,
    );
    assert_eq!(deleted.status, 200, "deleting it: {}", deleted.body);
    refusing(
        ingest,
        "the ingest listener of a node whose only ingest rule was deleted",
    );

    drop(broker);
}

/// A rule whose listener cannot be bound is refused, and this node is
/// left exactly as it was.
///
/// The failure a listener bound after startup makes possible: the
/// address may be taken by then. The answer is the one this project
/// gives everywhere else — refuse rather than reinterpret — and the
/// thing that would be worse than either is a rule that was created,
/// stored and reported as live with nothing in front of it, which is
/// the defect this listener had in the first place.
///
/// So the refusal has to leave **nothing** behind: the assertions
/// below are that the rule is not readable, not counted, and that the
/// same request succeeds once the address is free.
#[test]
fn an_ingest_rule_whose_listener_cannot_be_bound_is_refused_and_not_installed() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let mqtt = loopback(free_port());
    let admin = loopback(free_port());

    // Something else is already on the ingest address, and stays
    // there. A broker with no ingest rule starts perfectly happily
    // beside it, which is the point: the address is only needed when
    // a rule needs it.
    //
    // **The listener chooses the address rather than being pointed at
    // one**, because the two halves of this test are the only place a
    // port has to be held and then released: it asserts a refusal while
    // the address is taken and an acceptance once it is free, so a
    // sibling test given the same port in between would turn either
    // half into a failure about nothing. See [`HANDED_OUT`]: this
    // listener is the reservation, and dropping it below frees the
    // address without spending it again.
    let (squatter, ingest_port) = reserved_port();
    let ingest = loopback(ingest_port);

    write_config(&config, &store, mqtt, admin, ingest, "");
    let broker = BrokerProcess::start(
        &config,
        &store,
        mqtt,
        workspace.path().join("address-in-use.log"),
    );
    accepting(admin, "the admin listener");

    // **A rule that is switched off needs no listener**, so this one
    // is accepted although the address is taken: `enabled` is what
    // decides whether an ingest rule has a route at all, and a rule
    // with no route is a rule with nothing to serve. Created and
    // deleted here rather than in a test of its own, because what it
    // guards is the difference between the two requests below it.
    let off = create_rule(
        admin,
        &late_ingest_rule().replace("\"name\"", "\"enabled\":false,\"name\""),
    );
    assert_eq!(
        off.status, 200,
        "a switched-off ingest rule needs no listener and must not be refused for \
         wanting one: {}",
        off.body
    );
    let deleted = http(
        admin,
        "DELETE",
        &format!("/api/v1/rules/{LATE_RULE}"),
        ADMIN_TOKEN,
        None,
    );
    assert_eq!(deleted.status, 200, "deleting it again: {}", deleted.body);

    let refused = create_rule(admin, &late_ingest_rule());
    assert_eq!(
        refused.status, 500,
        "creating an ingest rule with nowhere to serve it: {}",
        refused.body
    );
    assert!(
        refused.body.contains("nowhere to serve it"),
        "the refusal must say what is wrong rather than name a generic failure: {}",
        refused.body
    );

    // Nothing was left behind by the refusal.
    let read = http(
        admin,
        "GET",
        &format!("/api/v1/rules/{LATE_RULE}"),
        ADMIN_TOKEN,
        None,
    );
    assert_eq!(
        read.status, 404,
        "a refused rule must not be readable afterwards: {}",
        read.body
    );
    let listed = http(admin, "GET", "/api/v1/rules", ADMIN_TOKEN, None);
    assert!(
        listed.body.contains("\"total\":0"),
        "a refused rule must not be counted: {}",
        listed.body
    );

    // And the same request succeeds once the address is free, which
    // is what says the refusal was about the address and not about
    // the rule.
    drop(squatter);
    let created = create_rule(admin, &late_ingest_rule());
    assert_eq!(
        created.status, 200,
        "the same rule, once the address is free: {}",
        created.body
    );
    let answered = http(
        ingest,
        "POST",
        &format!("/ingest/{LATE_RULE}"),
        LATE_TOKEN,
        Some(POSTED_LATE),
    );
    assert_eq!(
        answered.status, 200,
        "and it is reachable: {}",
        answered.body
    );

    drop(broker);
}

/// The same defect on the other configuration: with the routes on the
/// admin listener, a rule created over the API is reachable there too.
///
/// `ingest.listener = "admin"` binds no second socket, so there is no
/// listener to bind lazily — but the route set was merged into the
/// admin router only if this node held an ingest rule at startup, and
/// an axum router cannot grow a route once it is being served. A node
/// that started with none would therefore have answered `404` to every
/// post until it was restarted, which is the same lie on a different
/// port.
///
/// The route set is merged unconditionally instead, which costs a node
/// with no ingest rules nothing: an identifier nothing has registered
/// is answered exactly the way an identifier nobody configured is.
#[test]
fn an_ingest_rule_created_over_the_api_is_reachable_on_the_admin_listener() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let mqtt = loopback(free_port());
    let admin = loopback(free_port());
    let ingest = loopback(free_port());

    write_config_with(&config, &store, mqtt, admin, ingest, "admin", "");
    let broker = BrokerProcess::start(
        &config,
        &store,
        mqtt,
        workspace.path().join("on-the-admin-listener.log"),
    );
    accepting(admin, "the admin listener");

    let mut subscriber = Client::connect(mqtt, "waiting-on-the-admin-listener");
    subscriber.subscribe(LATE_TOPIC);

    let created = create_rule(admin, &late_ingest_rule());
    assert_eq!(created.status, 200, "creating a rule: {}", created.body);

    let answered = http(
        admin,
        "POST",
        &format!("/ingest/{LATE_RULE}"),
        LATE_TOKEN,
        Some(POSTED_LATE),
    );
    assert_eq!(
        answered.status, 200,
        "posting to a rule created over the API, on the admin listener: {}",
        answered.body
    );
    let delivered = subscriber
        .delivery_within(PATIENCE)
        .expect("what was posted must reach the subscriber");
    assert_eq!(&delivered.payload[..], POSTED_LATE);

    // And no second listener was bound, because this configuration
    // asked for one rather than two.
    //
    // Asked of the broker rather than of the port. Probing the port
    // was an assertion about an absence, which convention 5 warns
    // about, and worse: it is an absence on an address this test
    // never bound, so anything else on the machine taking it makes
    // the test fail for a reason that has nothing to do with the
    // broker. That is what it did, once in twenty-five runs. What
    // the test means is that this broker bound one ingest listener,
    // and the broker is the thing that knows.
    let mut bound = listener_protocols(admin, LATE_TOKEN);
    bound.sort();
    assert_eq!(
        bound,
        ["admin", "mqtt"],
        "the routes ride the admin listener, so ingest has no listener of its own \
         and nothing else was bound either"
    );

    drop(broker);
}

/// A listener bound after startup stops on a signal like the ones
/// bound during it.
///
/// **This project has been bitten here before.** Section 11.1's events
/// stream does not end on its own, and graceful shutdown waits for
/// every open connection, so one browser tab left open stopped the
/// broker exiting — which is a data bug, because a clean stop is how
/// sessions reach disk. A listener that now comes into existence
/// halfway through the process's life is a new chance to make the same
/// mistake: it is not in the list of things startup bound, so it is
/// easy for it to be in nobody's shutdown sequence either.
///
/// **A client is connected to it when the signal arrives**, kept alive
/// rather than closed, which is what makes this more than a test that
/// an idle broker stops. Bounded, because the regression is a process
/// that never exits and a test that hangs stalls a build instead of
/// reporting.
#[cfg(unix)]
#[test]
fn an_ingest_listener_bound_after_startup_stops_on_a_signal() {
    let workspace = tempfile::tempdir().expect("a temporary directory");
    let store = workspace.path().join("store");
    let config = workspace.path().join("sdmqtt.toml");
    let mqtt = loopback(free_port());
    let admin = loopback(free_port());
    let ingest = loopback(free_port());

    write_config(&config, &store, mqtt, admin, ingest, "");
    let mut broker = BrokerProcess::start(
        &config,
        &store,
        mqtt,
        workspace.path().join("stops-on-a-signal.log"),
    );
    accepting(admin, "the admin listener");

    let created = create_rule(admin, &late_ingest_rule());
    assert_eq!(created.status, 200, "creating a rule: {}", created.body);
    accepting(ingest, "the ingest listener this rule needed");

    // A caller that posted and has not gone away. `Connection:
    // close` is what every other request in this file sends, so this
    // one says the opposite deliberately: what is being tested is a
    // connection the broker is holding when the signal arrives.
    let mut posting = TcpStream::connect_timeout(&ingest, PATIENCE).expect("the listener accepts");
    posting
        .set_read_timeout(Some(PATIENCE))
        .expect("a read deadline");
    let head = format!(
        "POST /ingest/{LATE_RULE} HTTP/1.1\r\nHost: {ingest}\r\nConnection: keep-alive\r\n\
         Authorization: Bearer {LATE_TOKEN}\r\nContent-Length: {}\r\n\r\n",
        POSTED_LATE.len()
    );
    posting.write_all(head.as_bytes()).expect("the head");
    posting.write_all(POSTED_LATE).expect("and the body");
    posting.flush().expect("and flushed");
    let mut answer = [0u8; 15];
    posting
        .read_exact(&mut answer)
        .expect("the response head begins");
    assert!(
        answer.starts_with(b"HTTP/1.1 200"),
        "the post was refused: {}",
        String::from_utf8_lossy(&answer)
    );

    let signalled = Instant::now();
    broker.terminate();

    let status = broker.wait_for_exit().expect(
        "a signalled broker exits, with a lazily bound ingest listener and a \
                 client still connected to it",
    );
    assert!(
        status.success(),
        "an orderly stop is not a failure: {status}, {}",
        broker.logged()
    );

    // It stopped because the listener was told to, not because the
    // backstop gave up on it. Reaching the backstop on every shutdown
    // would be the defect the backstop exists to report.
    let took = signalled.elapsed();
    assert!(
        took < sdmqtt_admin::SHUTDOWN_GRACE,
        "it took {took:?}, which is the {:?} backstop rather than the signal: {}",
        sdmqtt_admin::SHUTDOWN_GRACE,
        broker.logged()
    );
    assert!(
        broker.logged().contains("the ingest listener has stopped"),
        "the listener this node bound after startup was in nobody's shutdown sequence: {}",
        broker.logged()
    );
}
