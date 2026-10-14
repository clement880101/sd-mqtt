//! Three real processes form a mesh and survive a one-way partition.
//!
//! Against the real binary, over real sockets, through
//! [`cluster::testing::gate`]. Everything here is a property of three
//! operating system processes that share nothing but loopback: three
//! stores, three identities minted independently, three handshakes and
//! three copies of every timer.
//!
//! # Why the gate, rather than killing something
//!
//! Killing a node is a symmetric failure. Both survivors see the
//! socket close, they agree instantly, and the heartbeat this
//! milestone built is not involved at all. The failure that finds bugs
//! is the one where **A cannot hear B while B still hears A**: two
//! halves of a cluster with two different answers, for as long as it
//! lasts. That needs the connection to stay established with the bytes
//! simply stopping, which is what the gate does and what a closed
//! socket cannot express.
//!
//! # One gate per ordered pair
//!
//! A node advertises one address, so two peers reaching a third
//! through one address cannot be severed from it independently. Each
//! ordered pair gets a listener of its own, and each node's seeds are
//! the gates that front its peers — which is also why every address in
//! these configurations is a gate's and none is a node's own. Section
//! 8.1 is explicit that the advertised address is not the bind
//! address, and a node behind a gate is the case it names.
//!
//! # What is asserted about healing
//!
//! That nothing reconnected. Every gate's count of accepted
//! connections is recorded once the mesh has formed, and must not have
//! moved by the end: the partition is imposed and lifted on the
//! connections the cluster already had.

#![cfg(unix)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use cluster::testing::gate::Gate;
use cluster::testing::Direction;
use mqtt_codec::packet::{
    Ack, ConnAck, Connect, Publish, RetainHandling, Subscribe, SubscriptionOptions,
};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};

/// How long to wait for a freshly started broker to accept.
///
/// Generous: this runs on shared hardware, and a flaky cluster test is
/// worse than none.
const STARTUP: Duration = Duration::from_secs(30);

/// How long to wait for the cluster to reach a state.
///
/// Comfortably more than the detection windows below, so that a slow
/// machine is not a failure and a broken mechanism still is.
const PATIENCE: Duration = Duration::from_secs(30);

/// How long a request may take.
const REPLY: Duration = Duration::from_secs(10);

/// How often each node beats, in milliseconds.
///
/// Faster than section 8.1's default second, because this test waits
/// for a peer to be declared dead and the default would make it a
/// ten-second wait. The mechanism is the same at any interval.
const HEARTBEAT_MS: u64 = 200;

/// How long a peer may be silent before it is dead, in milliseconds.
const DEAD_AFTER_MS: u64 = 1_500;

/// What each node is called, in the order they are started.
///
/// Written into each node's `node.name`, so the default derived from the
/// machine's hostname is not what three nodes on one machine would show —
/// which is the whole reason that default was changed, and the reason this
/// test says what it expects instead of relying on it.
const LABELS: [&str; 3] = ["alpha", "beta", "gamma"];

/// The secret all three share.
const SECRET: &str = "a-cluster-secret-for-the-test";

/// The admin token all three share.
const TOKEN: &str = "an-admin-token-for-the-test";

/// How many nodes.
const NODES: usize = 3;

// ---- the processes -------------------------------------------------

/// One running `sdmqtt`.
struct Node {
    child: Child,
    /// What it was started with, so a restart starts the same node.
    config: PathBuf,
    /// Where its store is. The point of a restart is that this
    /// survives it.
    store: PathBuf,
    /// What it was started with beyond the file, so a restart keeps it.
    knobs: Knobs,
    admin: SocketAddr,
    /// Where its clients connect, which section 8.3's routing is about.
    mqtt: SocketAddr,
    log: PathBuf,
    label: &'static str,
}

impl Node {
    fn logged(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn wait_until_listening(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + STARTUP;
        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!(
                    "{} exited with {status}: {}",
                    self.label,
                    self.logged()
                ));
            }
            if TcpStream::connect_timeout(&self.admin, Duration::from_millis(200)).is_ok() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(format!(
            "{} did not answer on {} within {STARTUP:?}: {}",
            self.label,
            self.admin,
            self.logged()
        ))
    }

    /// Kill this node, and say who it was.
    ///
    /// The identity is read **before** the kill, because afterwards
    /// there is nobody to ask: what a survivor is asked about is the
    /// identity, and a test that wanted it later would find the admin
    /// listener gone.
    ///
    /// Killed, not stopped. A clean shutdown could hand sessions over
    /// on the way out, and a test about losing an owner would then be
    /// a test about section 8.4 working rather than section 8.6.
    fn kill(&mut self) -> String {
        let identity = self.identity();
        let _ = self.child.kill();
        let _ = self.child.wait();
        identity
    }

    /// Stop this node and start it again against the same store.
    ///
    /// **The only way to ask whether something was really written
    /// down.** Everything else a test can read — a gauge, a count, an
    /// endpoint — is served from memory, so a copy that was never
    /// stored looks exactly like one that was until the process goes
    /// away and comes back.
    ///
    /// Killed rather than asked politely, for the reason every
    /// durability test in this project kills: a clean shutdown flushes,
    /// and a flush on the way out proves nothing about what was on the
    /// disk a moment earlier.
    fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.child = spawn(&self.config, &self.store, &self.log, self.knobs);
        if let Err(why) = self.wait_until_listening() {
            panic!("{} did not come back: {why}", self.label);
        }
    }

    /// This node's own identity, as it reports it.
    fn identity(&self) -> String {
        self.nodes()
            .into_iter()
            .find(|node| node.this_node)
            .expect("a node always reports itself")
            .id
    }

    /// Every node this one can see, itself included.
    fn nodes(&self) -> Vec<Reported> {
        let body = self.get("/api/v1/nodes");
        let document: serde_json::Value =
            serde_json::from_str(&body).unwrap_or_else(|why| panic!("{body:?} is not JSON: {why}"));
        document["items"]
            .as_array()
            .unwrap_or_else(|| panic!("no items in {document}"))
            .iter()
            .map(|item| Reported {
                id: item["id"].as_str().unwrap_or_default().to_owned(),
                name: item["name"].as_str().unwrap_or_default().to_owned(),
                address: item["address"].as_str().unwrap_or_default().to_owned(),
                state: item["state"].as_str().unwrap_or_default().to_owned(),
                this_node: item["this_node"].as_bool().unwrap_or(false),
            })
            .collect()
    }

    /// How this node says `peer` is doing, if it has met it.
    fn how(&self, peer: &str) -> Option<String> {
        self.nodes()
            .into_iter()
            .find(|node| node.id == peer)
            .map(|node| node.state)
    }

    /// Whether this node's view of membership is complete.
    fn view_is_complete(&self) -> bool {
        self.get("/health/cluster")
            .contains("\"status\":\"complete\"")
    }

    /// The body of a `GET`, without its head.
    fn get(&self, path: &str) -> String {
        let raw = raw_get(self.admin, path);
        match raw.split_once("\r\n\r\n") {
            Some((_head, body)) => body.to_owned(),
            None => panic!("no body in {raw:?}"),
        }
    }

    /// Publish a retained message through the admin API.
    ///
    /// At least once, so the answer does not come back until the store
    /// has the message — which is also the moment section 8.2 allows the
    /// register to be broadcast. A test that published at most once would
    /// be asserting about a race it had set up itself.
    fn retain(&self, topic: &str, payload: &str) {
        let body = format!(
            "{{\"topic\":\"{topic}\",\"payload\":\"{payload}\",\"qos\":1,\"retain\":true}}"
        );
        let answered = self.post("/api/v1/publish", &body);
        assert!(
            answered.contains("\"delivered\""),
            "{} would not publish to {topic}: {answered}",
            self.label
        );
    }

    /// Publish through the admin API and say how many it reached.
    ///
    /// Section 8.3's count: subscribers on that node plus nodes it was
    /// forwarded to. Nonzero is "somebody in this cluster wants it",
    /// which is what a test waiting for a subscription register to
    /// arrive is waiting for — and asking the broker beats sleeping,
    /// because a broadcast is milliseconds and a sleep long enough to
    /// cover a loaded machine is seconds.
    fn publish_reaching(&self, topic: &str, payload: &str) -> u64 {
        let body = format!("{{\"topic\":\"{topic}\",\"payload\":\"{payload}\",\"qos\":1}}");
        let answered = self.post("/api/v1/publish", &body);
        let document: serde_json::Value = serde_json::from_str(&answered)
            .unwrap_or_else(|why| panic!("{answered:?} is not JSON: {why}"));
        document["delivered"]
            .as_u64()
            .unwrap_or_else(|| panic!("no delivered count in {document}"))
    }

    /// What this node holds retained, as topic and text.
    fn retained(&self) -> BTreeMap<String, String> {
        let body = self.get("/api/v1/retained?limit=100");
        let document: serde_json::Value =
            serde_json::from_str(&body).unwrap_or_else(|why| panic!("{body:?} is not JSON: {why}"));
        document["items"]
            .as_array()
            .unwrap_or_else(|| panic!("no items in {document}"))
            .iter()
            .map(|item| {
                (
                    item["topic"].as_str().unwrap_or_default().to_owned(),
                    item["text"].as_str().unwrap_or_default().to_owned(),
                )
            })
            .collect()
    }

    /// One counter from this node's exposition.
    ///
    /// `None` for a series that is not there, which is a different
    /// answer from zero and is what section 11.3 relies on: a node
    /// that does not cluster renders no cluster numbers at all.
    fn counter(&self, name: &str) -> Option<u64> {
        let rendered = self.get("/metrics");
        let prefix = format!("{name} ");
        rendered
            .lines()
            .find(|line| line.starts_with(&prefix))
            .and_then(|line| line[prefix.len()..].trim().parse().ok())
    }

    /// Which sessions this node holds, by client identifier.
    ///
    /// Connected or not: what a migration moves is the session, and a
    /// listing that showed only connected ones could not tell a
    /// session that left from a client that went away.
    fn sessions(&self) -> BTreeSet<String> {
        let body = self.get("/api/v1/sessions?limit=100");
        let document: serde_json::Value =
            serde_json::from_str(&body).unwrap_or_else(|why| panic!("{body:?} is not JSON: {why}"));
        document["items"]
            .as_array()
            .unwrap_or_else(|| panic!("no items in {document}"))
            .iter()
            .map(|item| item["client_id"].as_str().unwrap_or_default().to_owned())
            .collect()
    }

    /// Create a rule through the admin API.
    fn create_rule(&self, id: &str) {
        let body = format!(
            "{{\"id\":\"{id}\",\"name\":\"{id}\",\"filter\":\"nothing/{id}\",\
               \"http_out\":{{\"url\":\"https://example.invalid/{id}\",\"timeout_ms\":1000,\
               \"retries\":0,\"backoff_ms\":1000}}}}"
        );
        let answered = self.post("/api/v1/rules", &body);
        assert!(
            answered.contains(id),
            "{} would not create the rule {id}: {answered}",
            self.label
        );
    }

    /// Which rules this node holds, by identifier.
    fn rules(&self) -> BTreeSet<String> {
        let body = self.get("/api/v1/rules?limit=100");
        let document: serde_json::Value =
            serde_json::from_str(&body).unwrap_or_else(|why| panic!("{body:?} is not JSON: {why}"));
        document["items"]
            .as_array()
            .unwrap_or_else(|| panic!("no items in {document}"))
            .iter()
            .map(|item| item["id"].as_str().unwrap_or_default().to_owned())
            .collect()
    }

    /// A `DELETE`, with the token: the status line's code and the body.
    ///
    /// The code is returned as well as the body because what this is
    /// used for is a request that may be refused, and a refusal's body
    /// is prose nobody should match on.
    fn delete(&self, path: &str) -> (u16, String) {
        let mut socket =
            TcpStream::connect_timeout(&self.admin, REPLY).expect("the listener accepts");
        socket
            .set_read_timeout(Some(REPLY))
            .expect("a read deadline");
        let head = format!(
            "DELETE {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {TOKEN}\r\n\
             Connection: close\r\n\r\n",
            self.admin
        );
        socket
            .write_all(head.as_bytes())
            .expect("the head is written");
        socket.flush().expect("and flushed");
        let mut raw = Vec::new();
        socket.read_to_end(&mut raw).expect("the response ends");
        let text = String::from_utf8_lossy(&raw).into_owned();
        let code = text
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .unwrap_or_else(|| panic!("no status in {text:?}"));
        match text.split_once("\r\n\r\n") {
            Some((_head, body)) => (code, body.to_owned()),
            None => panic!("no body in {text:?}"),
        }
    }

    /// The body of a `POST` of JSON, without its head.
    fn post(&self, path: &str, body: &str) -> String {
        let mut socket =
            TcpStream::connect_timeout(&self.admin, REPLY).expect("the listener accepts");
        socket
            .set_read_timeout(Some(REPLY))
            .expect("a read deadline");
        let head = format!(
            "POST {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {TOKEN}\r\n\
             Content-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n",
            self.admin,
            body.len()
        );
        socket
            .write_all(head.as_bytes())
            .expect("the head is written");
        socket.write_all(body.as_bytes()).expect("and the body");
        socket.flush().expect("and flushed");
        let mut raw = Vec::new();
        socket.read_to_end(&mut raw).expect("the response ends");
        let text = String::from_utf8_lossy(&raw).into_owned();
        match text.split_once("\r\n\r\n") {
            Some((_head, body)) => body.to_owned(),
            None => panic!("no body in {text:?}"),
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start one broker against a configuration and a store.
///
/// Factored out because a restart is the same command: a node that
/// came back differently would not be testing what a restart does.
/// What a test can turn on a node that the file does not carry.
///
/// Through the environment, so one harness serves every setting a test
/// wants and a restart starts with the same ones.
#[derive(Debug, Clone, Copy)]
struct Knobs {
    /// Copies of each session. Section 8.6.
    factor: usize,
    /// How long a dead node keeps its place in a replica set.
    gone_after_ms: u64,
    /// How many sessions a repair may send a second.
    repair_per_second: u64,
    /// `async` or `sync`: when a durable publish is acknowledged.
    durability: &'static str,
}

impl Default for Knobs {
    /// The broker's own defaults, so a test that sets nothing tests
    /// what an operator who set nothing would run.
    fn default() -> Self {
        Self {
            factor: cluster::DEFAULT_FACTOR,
            gone_after_ms: 3_600_000,
            repair_per_second: 50,
            durability: "async",
        }
    }
}

fn spawn(config: &Path, store: &Path, log: &Path, knobs: Knobs) -> Child {
    Command::new(env!("CARGO_BIN_EXE_sdmqtt"))
        .arg("serve")
        .arg("--config")
        .arg(config)
        // On the command line as well as in the file, because the
        // environment is emptied below and an empty
        // `SDMQTT_STORAGE_DIRECTORY` takes a store *away*. A flag
        // overrides both, which is what `tests/bridge.rs` does and
        // for the same reason.
        .arg("--store-directory")
        .arg(store)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        // Emptied so that a configuration in the environment of
        // whoever is running the tests cannot reach these brokers.
        .env("SDMQTT_CONFIG", "")
        .env("SDMQTT_STORAGE_DIRECTORY", "")
        // Section 8.6's copies per session. In the environment rather
        // than the file so that one harness serves every factor a test
        // wants, and so that a restart starts with the same one.
        .env(
            "SDMQTT_CLUSTER_REPLICATION_FACTOR",
            knobs.factor.to_string(),
        )
        .env(
            "SDMQTT_CLUSTER_GONE_AFTER_MS",
            knobs.gone_after_ms.to_string(),
        )
        .env(
            "SDMQTT_CLUSTER_REPAIR_SESSIONS_PER_SECOND",
            knobs.repair_per_second.to_string(),
        )
        .env("SDMQTT_CLUSTER_DURABILITY", knobs.durability)
        .stderr(Stdio::from(
            // Appended rather than truncated, so a restart does not
            // throw away the log of what happened before it.
            File::options()
                .create(true)
                .append(true)
                .open(log)
                .expect("a log file can be opened"),
        ))
        .spawn()
        .expect("the broker binary runs")
}

/// One node's row in another's listing.
#[derive(Debug, Clone)]
struct Reported {
    id: String,
    name: String,
    address: String,
    state: String,
    this_node: bool,
}

fn raw_get(address: SocketAddr, path: &str) -> String {
    let mut socket = TcpStream::connect_timeout(&address, REPLY).expect("the listener accepts");
    socket
        .set_read_timeout(Some(REPLY))
        .expect("a read deadline");
    socket
        .write_all(
            format!(
                "GET {path} HTTP/1.1\r\nHost: {address}\r\n\
                 Authorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .expect("the request is written");
    let mut raw = Vec::new();
    socket.read_to_end(&mut raw).expect("the response ends");
    String::from_utf8_lossy(&raw).into_owned()
}

/// The lowest port this test will use, and one past the highest.
///
/// **Below every platform's ephemeral range, deliberately.** Linux draws
/// automatic ports from 32768 upwards, macOS and Windows from 49152, so
/// nothing in this window is ever assigned by the kernel: not a bind of
/// port zero and not the source port of an outbound connection. A port
/// read from a bind of port zero is a hint and not a reservation — it is
/// free again before the caller has it — and this test hands nine of them
/// to three child processes that bind them milliseconds later. The same
/// defect was measured in `tests/rules.rs` at one failure in six under
/// load; this is the same fix.
const PORTS: std::ops::Range<u16> = 30_000..40_000;

/// Every port this test binary has taken.
static HANDED_OUT: std::sync::Mutex<Vec<u16>> = std::sync::Mutex::new(Vec::new());

/// A port nothing else will be given, with nothing listening on it.
///
/// Drawing from [`PORTS`] removes the kernel as a competitor; remembering
/// what has been handed out removes this binary's own tests. A port in
/// that range can still be in use by something already running, so one is
/// taken by binding it and moving on if that fails rather than by assuming
/// it is free.
fn free_port() -> u16 {
    // Started somewhere different in each process so that two runs in
    // quick succession do not queue up on the same first candidate.
    let offset = usize::from(std::process::id() as u16) % PORTS.len();
    for step in 0..PORTS.len() {
        let port = PORTS.start + u16::try_from((offset + step) % PORTS.len()).unwrap_or(0);
        let mut handed = HANDED_OUT.lock().expect("not poisoned");
        if handed.contains(&port) {
            continue;
        }
        handed.push(port);
        drop(handed);
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
        // Something else on the machine has it. Spent either way, so
        // nothing here tries it twice.
    }
    panic!(
        "no port between {} and {} could be bound",
        PORTS.start, PORTS.end
    );
}

// ---- a client over the socket --------------------------------------
//
// Section 8.3 is about what a *client* sees, so these tests have to be
// clients: the admin API can publish and can say what a node holds
// retained, and it cannot subscribe. The same shape as the client in
// `tests/restart.rs`, which is deliberate — what differs is that these
// connect to three different brokers.

/// One MQTT 5 client on a blocking socket.
struct Client {
    stream: TcpStream,
    buf: BytesMut,
    packets: u16,
    /// Deliveries that arrived while something else was being waited
    /// for.
    ///
    /// **A retained message overtakes its own SUBACK**, legitimately: a
    /// subscribe that matches a retained message produces the delivery
    /// and the acknowledgement in one batch, and nothing in the
    /// protocol orders the two. A client that insisted on the SUBACK
    /// first would be asserting an order the broker never promised, so
    /// this holds whatever arrives early until something asks for it.
    pending: Vec<(String, String)>,
}

/// How long a session outlives its connection, for the tests that need
/// it to.
///
/// Comfortably longer than any of them takes. A session that expired
/// mid-test would look exactly like one that failed to migrate.
const SESSION_EXPIRY: u32 = 300;

/// How a connection ended, as the client saw it.
///
/// Three outcomes rather than two, because section 8.5 promises a
/// *reason* and a socket that simply ends is the failure that promise
/// is about. Collapsing [`Ending::Silent`] into a missing reason would
/// make a test that cannot tell the two apart.
#[derive(Debug, PartialEq, Eq)]
enum Ending {
    /// The broker said why, which is what MQTT 5's DISCONNECT is for.
    Said(ReasonCode),
    /// The socket ended with nothing said.
    Silent,
    /// Nothing happened in the time allowed.
    StillOpen,
}

impl Client {
    /// Connect to `address` with a session that goes when it does.
    fn connect(address: SocketAddr, client_id: &str) -> Self {
        Self::opened(address, client_id, true, 0).0
    }

    /// CONNECT through section 12.2's exchange as `username` with
    /// `password`, and the acknowledgement's reason — with the server's
    /// final message verified when the reason is success, so a success
    /// here is mutual.
    fn connect_scram(
        address: SocketAddr,
        client_id: &str,
        username: &str,
        password: &str,
    ) -> Option<ReasonCode> {
        use sdmqtt_server::scram;
        let stream = TcpStream::connect_timeout(&address, REPLY).expect("the broker accepts");
        stream
            .set_read_timeout(Some(REPLY))
            .expect("a read deadline");
        let mut client = Self {
            stream,
            buf: BytesMut::new(),
            packets: 0,
            pending: Vec::new(),
        };
        let (exchange, first) = scram::Client::first(username, "a-test-nonce-0001");
        client.send(&Packet::Connect(Box::new(Connect {
            version: Version::V5,
            client_id: client_id.to_owned(),
            clean_start: true,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties {
                authentication_method: Some(scram::METHOD.to_owned()),
                authentication_data: Some(Bytes::from(first)),
                ..Properties::default()
            },
        })));
        let server_first = match client.next()? {
            Packet::Auth(auth) => auth.properties.authentication_data?,
            Packet::ConnAck(ack) => return Some(ack.reason),
            _ => return None,
        };
        let (exchange, client_final) = exchange
            .finish(&server_first, password.as_bytes())
            .expect("a well-formed server-first");
        client.send(&Packet::Auth(mqtt_codec::packet::Auth {
            reason: ReasonCode::ContinueAuthentication,
            properties: Properties {
                authentication_method: Some(scram::METHOD.to_owned()),
                authentication_data: Some(Bytes::from(client_final)),
                ..Properties::default()
            },
        }));
        match client.next()? {
            Packet::ConnAck(ack) if ack.reason == ReasonCode::Success => {
                let verified = ack
                    .properties
                    .authentication_data
                    .as_deref()
                    .is_some_and(|server_final| exchange.verify(server_final));
                assert!(verified, "the server's final message must verify");
                Some(ReasonCode::Success)
            }
            Packet::ConnAck(ack) => Some(ack.reason),
            _ => None,
        }
    }

    /// CONNECT as `username` with `password`, and the acknowledgement's
    /// reason: `None` when the broker closed without one.
    fn connect_with(
        address: SocketAddr,
        client_id: &str,
        username: &str,
        password: &str,
    ) -> Option<ReasonCode> {
        let stream = TcpStream::connect_timeout(&address, REPLY).expect("the broker accepts");
        stream
            .set_read_timeout(Some(REPLY))
            .expect("a read deadline");
        let mut client = Self {
            stream,
            buf: BytesMut::new(),
            packets: 0,
            pending: Vec::new(),
        };
        client.send(&Packet::Connect(Box::new(Connect {
            version: Version::V5,
            client_id: client_id.to_owned(),
            clean_start: true,
            keep_alive: 0,
            username: Some(username.to_owned()),
            password: Some(Bytes::from(password.to_owned())),
            will: None,
            properties: Properties::default(),
        })));
        match client.next() {
            Some(Packet::ConnAck(ack)) => Some(ack.reason),
            _ => None,
        }
    }

    /// Connect asking to resume, with a session that survives the
    /// connection going away.
    ///
    /// Returns whether the broker said it already had the session,
    /// which is the thing a migration is: `true` is a session that was
    /// found — here or, through section 8.4, on whichever node held it
    /// — and `false` is one that has just been made.
    fn resuming(address: SocketAddr, client_id: &str) -> (Self, bool) {
        let (client, ack) = Self::opened(address, client_id, false, SESSION_EXPIRY);
        (client, ack.session_present)
    }

    /// The connect both of the above are, and its acknowledgement.
    fn opened(
        address: SocketAddr,
        client_id: &str,
        clean_start: bool,
        expiry: u32,
    ) -> (Self, ConnAck) {
        let stream = TcpStream::connect_timeout(&address, REPLY).expect("the broker accepts");
        stream.set_nodelay(true).expect("nodelay");
        stream
            .set_read_timeout(Some(REPLY))
            .expect("a read deadline");
        let mut client = Self {
            stream,
            buf: BytesMut::new(),
            packets: 0,
            pending: Vec::new(),
        };
        client.send(&Packet::Connect(Box::new(Connect {
            version: Version::V5,
            client_id: client_id.to_owned(),
            clean_start,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties {
                session_expiry_interval: Some(expiry),
                ..Properties::default()
            },
        })));
        match client.next() {
            Some(Packet::ConnAck(ack)) => {
                assert_eq!(ack.reason, ReasonCode::Success, "{client_id} was refused");
                (client, ack)
            }
            other => panic!("expected a connect acknowledgement, got {other:?}"),
        }
    }

    /// Wait up to `patience` for the broker to end this connection, and
    /// say how it ended.
    ///
    /// Whatever arrives before the ending is acknowledged and dropped:
    /// a queued delivery is not what is being waited for, and leaving
    /// it unread would stall the connection being closed.
    fn closed_within(&mut self, patience: Duration) -> Ending {
        self.stream
            .set_read_timeout(Some(patience))
            .expect("a read deadline");
        let deadline = Instant::now() + patience;
        while Instant::now() < deadline {
            match self.next() {
                Some(Packet::Disconnect(said)) => return Ending::Said(said.reason),
                Some(packet) => {
                    let _ = self.take(packet);
                }
                None => return Ending::Silent,
            }
        }
        Ending::StillOpen
    }

    /// Subscribe at least once, and wait for the grant.
    fn subscribe(&mut self, filter: &str) {
        let packet_id = self.next_packet_id();
        self.send(&Packet::Subscribe(Subscribe {
            packet_id,
            filters: vec![SubscriptionOptions {
                filter: filter.to_owned(),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAlways,
            }],
            properties: Properties::default(),
        }));
        loop {
            match self.next() {
                Some(Packet::SubAck(ack)) => {
                    assert_eq!(
                        ack.reasons,
                        vec![ReasonCode::GrantedQos1],
                        "the subscribe to {filter} was refused"
                    );
                    return;
                }
                // A retained message this subscribe asked for, arriving
                // before the acknowledgement. Held rather than
                // discarded: it is the delivery a later assertion is
                // about.
                Some(packet) => {
                    if let Some(delivery) = self.take(packet) {
                        self.pending.push(delivery);
                    }
                }
                None => panic!("the broker closed before acknowledging {filter}"),
            }
        }
    }

    /// Publish at least once and wait to be told what it reached.
    ///
    /// The reason code is returned rather than asserted, because it
    /// answers the question a test about routing has to ask: whether
    /// anybody in the cluster wants the topic yet.
    fn publish(&mut self, topic: &str, payload: &str) -> ReasonCode {
        let packet_id = self.next_packet_id();
        self.send(&Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: topic.to_owned(),
            packet_id: Some(packet_id),
            payload: Bytes::from(payload.to_owned()),
            properties: Properties::default(),
        }));
        loop {
            match self.next() {
                Some(Packet::PubAck(ack)) => {
                    assert_eq!(ack.packet_id, packet_id);
                    return ack.reason;
                }
                // A delivery to this same client, which is ordinary:
                // these clients subscribe and publish on one connection.
                Some(packet) => {
                    if let Some(delivery) = self.take(packet) {
                        self.pending.push(delivery);
                    }
                }
                None => panic!("the broker closed before acknowledging a publication"),
            }
        }
    }

    /// A publication as a delivery, acknowledged, or `None` for
    /// anything else.
    fn take(&mut self, packet: Packet) -> Option<(String, String)> {
        let Packet::Publish(publish) = packet else {
            return None;
        };
        if let Some(packet_id) = publish.packet_id {
            self.send(&Packet::PubAck(Ack {
                packet_id,
                reason: ReasonCode::Success,
                properties: Properties::default(),
            }));
        }
        Some((
            publish.topic,
            String::from_utf8_lossy(&publish.payload).into_owned(),
        ))
    }

    /// Wait up to `patience` for a delivery, and acknowledge it.
    ///
    /// Returns the topic and the payload as text. `None` is nothing
    /// arriving in time, which several of these tests assert.
    fn delivered_within(&mut self, patience: Duration) -> Option<(String, String)> {
        if !self.pending.is_empty() {
            return Some(self.pending.remove(0));
        }
        self.stream
            .set_read_timeout(Some(patience))
            .expect("a read deadline");
        let deadline = Instant::now() + patience;
        while Instant::now() < deadline {
            // Anything that is not a publication is not what is being
            // waited for; a closed socket ends the wait.
            let packet = self.next()?;
            if let Some(delivery) = self.take(packet) {
                return Some(delivery);
            }
        }
        None
    }

    /// Every delivery already waiting, without waiting for more.
    fn drain(&mut self) -> Vec<(String, String)> {
        let mut seen = Vec::new();
        while let Some(delivery) = self.delivered_within(Duration::from_millis(100)) {
            seen.push(delivery);
        }
        seen
    }

    /// A packet identifier nothing else of this client's is using.
    ///
    /// Counted up rather than reused, because these clients have
    /// deliveries outstanding at the same time as their own
    /// publications and a fixed identifier would collide with one.
    fn next_packet_id(&mut self) -> u16 {
        self.packets = self.packets.wrapping_add(1).max(1);
        self.packets
    }

    fn send(&mut self, packet: &Packet) {
        let mut out = BytesMut::new();
        encode(packet, Version::V5, &mut out).expect("the packet encodes");
        self.stream.write_all(&out).expect("the write succeeds");
    }

    /// Read until one packet is whole, or the read deadline passes.
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

fn loopback(port: u16) -> SocketAddr {
    format!("127.0.0.1:{port}").parse().expect("an address")
}

#[allow(clippy::too_many_arguments)]
fn write_config(
    path: &Path,
    store: &Path,
    name: &str,
    mqtt: SocketAddr,
    admin: SocketAddr,
    cluster: SocketAddr,
    advertise: SocketAddr,
    seeds: &[SocketAddr],
) {
    let seeds = seeds
        .iter()
        .map(|seed| format!("\"{seed}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let text = format!(
        r#"
[node]
name = "{name}"

[listener]
address = "{mqtt}"

[admin]
address = "{admin}"
token = "{TOKEN}"

[storage]
directory = "{store}"
sync = "immediate"

[cluster]
name = "the-test-cluster"
secret = "{SECRET}"
address = "{cluster}"
advertise = "{advertise}"
seeds = [{seeds}]
heartbeat_ms = {HEARTBEAT_MS}
suspect_after = 3
dead_after_ms = {DEAD_AFTER_MS}
reconnect_min_ms = 100
reconnect_max_ms = 500

[log]
level = "info"
"#,
        store = store.display(),
    );
    std::fs::write(path, text).expect("the configuration file can be written");
}

/// Wait until `holds`, or fail saying what never happened.
fn until(what: &str, mut holds: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while Instant::now() < deadline {
        if holds() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{what} never happened within {PATIENCE:?}");
}

/// Assert something stays true for a while.
fn keeps(what: &str, for_how_long: Duration, mut holds: impl FnMut() -> bool) {
    let deadline = Instant::now() + for_how_long;
    while Instant::now() < deadline {
        assert!(holds(), "{what} stopped being true");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(holds(), "{what} stopped being true");
}

/// Three `sdmqtt` processes behind gates, meshed and settled.
///
/// Factored out because section 8.2's convergence test needs the same
/// three processes section 8.1's partition test does, and a second copy of
/// this would be a second answer to how a node behind a gate is
/// configured — which is the one thing in here that is easy to get subtly
/// wrong, because every address a node advertises has to be a gate's.
///
/// Returns the nodes, the gate per ordered pair, and what each node
/// advertises. Nothing here asserts; the caller does.
struct Cluster {
    nodes: Vec<Node>,
    /// `gates[&(from, to)]` is what `from` dials to reach `to`.
    gates: BTreeMap<(usize, usize), Gate>,
    advertised: Vec<SocketAddr>,
}

impl Cluster {
    /// Start three nodes under `directory`, each with a store of its own.
    async fn start(directory: &Path) -> Self {
        Self::start_with(directory, Knobs::default()).await
    }

    /// The same, keeping `factor` copies of each session.
    ///
    /// Three nodes with a factor of three put a copy of every session
    /// on every node, which is what a test about choosing *between*
    /// copies needs: two followers, one of them held back.
    async fn start_with_factor(directory: &Path, factor: usize) -> Self {
        Self::start_with(
            directory,
            Knobs {
                factor,
                ..Knobs::default()
            },
        )
        .await
    }

    /// The same, with every knob a test may turn.
    async fn start_with(directory: &Path, knobs: Knobs) -> Self {
        let labels = LABELS;

        // The three cluster ports, chosen before anything starts so that
        // the gates in front of them can be opened first and written into
        // the configurations.
        let ports: Vec<SocketAddr> = (0..NODES).map(|_| loopback(free_port())).collect();

        // One gate per ordered pair: `gates[&(from, to)]` is what `from`
        // dials to reach `to`. A flag on `to`'s own port would sever it
        // from everybody rather than from `from`.
        let mut gates: BTreeMap<(usize, usize), Gate> = BTreeMap::new();
        for from in 0..NODES {
            for (to, port) in ports.iter().enumerate() {
                if from != to {
                    gates.insert((from, to), Gate::open(*port).await.expect("a gate opens"));
                }
            }
        }

        // **Every advertised address is a gate's.** Behind a gate a node's
        // dialable address is the gate's and not its own, which is the
        // case section 8.1 names, and advertising the real port would give
        // a peer a way round the partition.
        let advertised: Vec<SocketAddr> = (0..NODES)
            .map(|to| gates[&((to + 1) % NODES, to)].address())
            .collect();

        let mut nodes = Vec::new();
        for at in 0..NODES {
            let label = labels[at];
            let config = directory.join(format!("{label}.toml"));
            let store = directory.join(format!("{label}-store"));
            let admin = loopback(free_port());
            let mqtt = loopback(free_port());
            let seeds: Vec<SocketAddr> = (0..NODES)
                .filter(|to| *to != at)
                .map(|to| gates[&(at, to)].address())
                .collect();
            write_config(
                &config,
                &store,
                label,
                mqtt,
                admin,
                ports[at],
                advertised[at],
                &seeds,
            );
            let log = directory.join(format!("{label}.log"));
            let mut node = Node {
                child: spawn(&config, &store, &log, knobs),
                admin,
                mqtt,
                log,
                label,
                config,
                store,
                knobs,
            };
            if let Err(why) = node.wait_until_listening() {
                panic!("{why}");
            }
            nodes.push(node);
        }
        Self {
            nodes,
            gates,
            advertised,
        }
    }

    /// How many connections each gate has carried.
    fn connections(&self) -> BTreeMap<(usize, usize), u64> {
        self.gates
            .iter()
            .map(|(pair, gate)| (*pair, gate.connections()))
            .collect()
    }

    /// Wait until every node sees the other two, and settle the dialling.
    ///
    /// The second half is not the same moment as the first: a node starts
    /// before its peers, so its first dials are refused, and a node that
    /// dialled a peer it already had a link to opens one connection and has
    /// it dropped by the rule that keeps the link the lower identity
    /// dialled. Both are startup, both are finite, and the count a
    /// partition is measured against has to be taken afterwards.
    fn settle(&self) -> BTreeMap<(usize, usize), u64> {
        until("every node sees the other two", || {
            self.nodes.iter().all(|node| {
                let seen = node.nodes();
                seen.len() == NODES && seen.iter().all(|row| row.state == "up")
            })
        });
        let mut established = self.connections();
        let settled = Duration::from_millis(HEARTBEAT_MS * 8);
        let deadline = Instant::now() + PATIENCE;
        loop {
            assert!(
                Instant::now() < deadline,
                "the cluster never stopped opening connections: {established:?}"
            );
            std::thread::sleep(settled);
            let now = self.connections();
            if now == established {
                break;
            }
            established = now;
        }
        assert!(
            established.values().all(|opened| *opened > 0),
            "every gate should have carried a dial by now: {established:?}"
        );
        established
    }
}

// ---- the test ------------------------------------------------------

/// Three processes form a mesh, survive a one-way partition, and agree
/// again when it heals.
///
/// Multi-threaded on purpose: the gates are tokio tasks and everything
/// else in here blocks a thread waiting on a socket, so a
/// single-threaded runtime would stop relaying bytes the moment the
/// test waited for anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_processes_form_a_mesh_and_survive_a_partition() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let Cluster {
        nodes,
        gates,
        advertised,
        ..
    } = Cluster::start(directory.path()).await;

    // ---- they all find each other --------------------------------
    until("every node sees the other two", || {
        nodes.iter().all(|node| {
            let seen = node.nodes();
            seen.len() == NODES && seen.iter().all(|row| row.state == "up")
        })
    });
    let identities: Vec<String> = nodes.iter().map(Node::identity).collect();
    assert_eq!(
        identities
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        NODES,
        "three stores, three identities: {identities:?}"
    );
    for node in &nodes {
        assert!(
            node.view_is_complete(),
            "{} does not have a complete view",
            node.label
        );
        // Section 11.3's gauge, which was deferred to this milestone.
        let metrics = node.get("/metrics");
        assert!(
            metrics.contains("cluster_members 3"),
            "{} reports the wrong membership: {metrics}",
            node.label
        );
        // And what every node publishes is the gate's address, not its
        // own bind address, which is what a peer actually dials.
        for row in node.nodes() {
            assert!(
                advertised
                    .iter()
                    .any(|gate| gate.to_string() == row.address),
                "{} reports {row:?}, which is not an address any peer can dial",
                node.label
            );
            assert!(
                LABELS.contains(&row.name.as_str()),
                "{} reports a node called {:?}",
                node.label,
                row.name
            );
        }
    }

    // What the cluster was built on. Nothing after this may add to it:
    // the partition is imposed and lifted on these connections.
    //
    // **Taken once the dialling has settled**, which is not the same
    // moment as the mesh being formed. A node starts before its peers,
    // so its first dials are refused; and a node that dialled a peer
    // it already had a link to opens one connection, has it dropped by
    // the rule that keeps the link the lower identity dialled, and
    // then stops. Both are startup, both are finite, and neither is
    // what this is watching for.
    let count = || -> BTreeMap<(usize, usize), u64> {
        gates
            .iter()
            .map(|(pair, gate)| (*pair, gate.connections()))
            .collect()
    };
    let mut established = count();
    let settled = Duration::from_millis(HEARTBEAT_MS * 8);
    let deadline = Instant::now() + PATIENCE;
    loop {
        assert!(
            Instant::now() < deadline,
            "the cluster never stopped opening connections: {established:?}"
        );
        std::thread::sleep(settled);
        let now = count();
        if now == established {
            break;
        }
        established = now;
    }
    assert!(
        established.values().all(|opened| *opened > 0),
        "every gate should have carried a dial by now: {established:?}"
    );

    // ---- one direction is severed --------------------------------
    //
    // Nothing alpha says stops; nothing beta says arrives. Both gates
    // between them are cut in the same direction, because which of the
    // two connections the mesh kept depends on which identity is lower
    // and that is minted at first start.
    gates[&(0, 1)].cut(Direction::Backward);
    gates[&(1, 0)].cut(Direction::Forward);

    until("alpha declares beta dead", || {
        nodes[0].how(&identities[1]).as_deref() == Some("down")
    });
    assert!(
        gates[&(0, 1)].valve(Direction::Backward).swallowed()
            + gates[&(1, 0)].valve(Direction::Forward).swallowed()
            > 0,
        "the cut has to have bitten: nothing of beta's was thrown away"
    );

    // **The disagreement is the point.** Beta still hears alpha, so
    // beta still believes alpha is there, and it goes on believing it
    // for as long as the cut lasts.
    keeps(
        "beta still sees alpha, and gamma still sees both",
        Duration::from_millis(DEAD_AFTER_MS * 2),
        || {
            nodes[1].how(&identities[0]).as_deref() == Some("up")
                && nodes[2].how(&identities[0]).as_deref() == Some("up")
                && nodes[2].how(&identities[1]).as_deref() == Some("up")
        },
    );
    assert!(
        !nodes[0].view_is_complete(),
        "alpha cannot see beta, so its view is not complete"
    );
    assert!(
        nodes[2].view_is_complete(),
        "gamma reaches both, so its view is"
    );

    // Nothing was closed. This is what a closed socket could not do.
    for (pair, gate) in &gates {
        assert_eq!(
            gate.connections(),
            established[pair],
            "the gate {pair:?} reconnected during the partition"
        );
    }

    // ---- and it heals, on the same connections -------------------
    gates[&(0, 1)].heal(Direction::Backward);
    gates[&(1, 0)].heal(Direction::Forward);

    until("all three agree again", || {
        nodes.iter().all(|node| {
            let seen = node.nodes();
            seen.len() == NODES && seen.iter().all(|row| row.state == "up")
        })
    });
    for node in &nodes {
        assert!(
            node.view_is_complete(),
            "{} still does not have a complete view: {}",
            node.label,
            node.logged()
        );
    }
    for (pair, gate) in &gates {
        assert_eq!(
            gate.connections(),
            established[pair],
            "the cluster healed by reconnecting through the gate {pair:?} rather \
             than on the connection it already had"
        );
    }
}

/// Three nodes converge on subscriptions, retained messages and rules
/// after a partition heals, and the agreement is the last writer's.
///
/// Section 8.2 against the real binary, over real sockets, through the
/// gate. Everything it asserts is a property of three operating system
/// processes that share nothing but loopback: three stores, three clocks,
/// three logs and three copies of every timer.
///
/// # Why a retained message and a rule
///
/// They are the two of this milestone's four kinds that an operator can
/// make happen and then read back, and they travel by different paths: a
/// retained message is the broker's own journal turned into a register by
/// the shell, and a rule is `rules::live`'s own write. A test of one would
/// leave the other's path unwalked, which is convention 5's "a path no
/// test walks" — and the rule's path is the one with an outbox and a
/// listener hanging off it.
///
/// The other two are not reachable from outside: a node subscription is
/// replicated at node granularity and is read by section 8.3's routing,
/// and a session owner is read by section 8.4's migration. Both are
/// milestone 6. What this milestone owes them is that the registers are
/// there, and `crates/cluster/tests/metadata.rs` is where that is proved
/// from a SUBSCRIBE packet.
///
/// # What the last writer means here
///
/// Two writes to one topic, one on each side of the partition, separated
/// by long enough that the later one's clock reading is certainly higher:
/// the two processes share a wall clock, so a hundred milliseconds apart is
/// not a tie. **Both orders are exercised** — once alpha first and once
/// gamma first — because a cluster that simply took whichever value
/// arrived last, or whichever node asked last, would get one of the two
/// right by accident.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_converge_after_a_partition_heals() {
    /// A topic alpha writes first and gamma writes later.
    const GAMMA_WINS: &str = "site/0001/state";
    /// And one the other way round.
    const ALPHA_WINS: &str = "site/0002/state";
    /// Long enough that two wall-clock readings cannot be the same
    /// millisecond, and short enough to be nothing in a test.
    const APART: Duration = Duration::from_millis(250);

    let directory = tempfile::tempdir().expect("a temporary directory");
    let cluster = Cluster::start(directory.path()).await;
    let established = cluster.settle();
    let nodes = &cluster.nodes;

    // ---- a retained message on one node reaches all three -----------
    //
    // **By broadcast, and only by broadcast.** An exchange is a pull: gamma
    // sends alpha a digest and alpha answers it, so cutting what gamma says
    // to alpha leaves gamma unable to ask for anything while alpha can still
    // tell it something. Both gates between the two are cut in that
    // direction, because which of the two connections the mesh kept depends
    // on which identity is lower and that is minted at first start.
    //
    // Without this the step proves nothing: the exchange runs every couple
    // of seconds, so a broadcast that never happened would be repaired
    // before any patience ran out, and deleting the broadcast entirely
    // passed this test until the cut was added.
    cluster.gates[&(0, 2)].cut(Direction::Backward);
    cluster.gates[&(2, 0)].cut(Direction::Forward);
    // **Waited for before anything is published.** A cut takes effect on
    // the relay's next decision, and what proves it has is a byte thrown
    // away — which is gamma's next heartbeat, within the interval. Asserted
    // after the write instead, it would be an assertion about a window of
    // milliseconds in which there was nothing yet to throw away.
    until("the cut bites, so gamma cannot ask for anything", || {
        cluster.gates[&(0, 2)]
            .valve(Direction::Backward)
            .swallowed()
            + cluster.gates[&(2, 0)].valve(Direction::Forward).swallowed()
            > 0
    });
    nodes[0].retain("before/the/partition", "everywhere");
    until("the write reaches all three", || {
        nodes.iter().all(|node| {
            node.retained()
                .get("before/the/partition")
                .map(String::as_str)
                == Some("everywhere")
        })
    });
    cluster.gates[&(0, 2)].heal(Direction::Backward);
    cluster.gates[&(2, 0)].heal(Direction::Forward);
    until("gamma is answering again", || {
        nodes[0].how(&nodes[2].identity()).as_deref() == Some("up")
    });

    // ---- gamma is severed, in both directions ------------------------
    //
    // Both directions, which is what makes this two halves of a cluster
    // rather than one node that can be heard and cannot hear: each half
    // takes writes, and neither sees the other's.
    for other in 0..NODES - 1 {
        cluster.gates[&(other, NODES - 1)].cut(Direction::Forward);
        cluster.gates[&(other, NODES - 1)].cut(Direction::Backward);
        cluster.gates[&(NODES - 1, other)].cut(Direction::Forward);
        cluster.gates[&(NODES - 1, other)].cut(Direction::Backward);
    }
    until("alpha declares gamma dead", || {
        let gamma = nodes[2].identity();
        nodes[0].how(&gamma).as_deref() == Some("down")
            && nodes[1].how(&gamma).as_deref() == Some("down")
    });

    // ---- each side writes -------------------------------------------
    nodes[0].retain(GAMMA_WINS, "from-alpha");
    nodes[2].retain(ALPHA_WINS, "from-gamma");
    nodes[0].create_rule("rule-from-alpha");
    nodes[2].create_rule("rule-from-gamma");
    std::thread::sleep(APART);
    nodes[2].retain(GAMMA_WINS, "from-gamma");
    nodes[0].retain(ALPHA_WINS, "from-alpha");

    // And the disagreement is real while it lasts, which is what makes
    // the convergence below something that had to happen rather than
    // something that was already true.
    assert_eq!(
        nodes[0].retained().get(GAMMA_WINS).map(String::as_str),
        Some("from-alpha"),
        "alpha should still hold its own write"
    );
    assert_eq!(
        nodes[2].retained().get(GAMMA_WINS).map(String::as_str),
        Some("from-gamma")
    );
    assert!(
        !nodes[0].rules().contains("rule-from-gamma"),
        "alpha cannot have gamma's rule yet"
    );
    assert!(
        !nodes[2].rules().contains("rule-from-alpha"),
        "gamma cannot have alpha's rule yet"
    );

    // ---- and it heals ------------------------------------------------
    for other in 0..NODES - 1 {
        cluster.gates[&(other, NODES - 1)].heal(Direction::Forward);
        cluster.gates[&(other, NODES - 1)].heal(Direction::Backward);
        cluster.gates[&(NODES - 1, other)].heal(Direction::Forward);
        cluster.gates[&(NODES - 1, other)].heal(Direction::Backward);
    }

    // **Nothing is published after the heal**, so everything below
    // arrives through the exchange: a broadcast reaches only the peers a
    // node has a link to at the moment of the write, and these writes
    // were made into a gate that threw them away.
    let expected: BTreeMap<&str, &str> = [
        ("before/the/partition", "everywhere"),
        (GAMMA_WINS, "from-gamma"),
        (ALPHA_WINS, "from-alpha"),
    ]
    .into_iter()
    .collect();
    let rules: BTreeSet<String> = ["rule-from-alpha", "rule-from-gamma"]
        .into_iter()
        .map(str::to_owned)
        .collect();

    until("all three agree on every retained message", || {
        nodes.iter().all(|node| {
            let held = node.retained();
            expected
                .iter()
                .all(|(topic, text)| held.get(*topic).map(String::as_str) == Some(*text))
        })
    });
    until("all three hold both rules", || {
        nodes.iter().all(|node| node.rules().is_superset(&rules))
    });

    // **The last writer's value, and not whichever arrived last.** One
    // topic resolves each way, which is what no rule about arrival order
    // could produce for both.
    for node in nodes {
        let held = node.retained();
        assert_eq!(
            held.get(GAMMA_WINS).map(String::as_str),
            Some("from-gamma"),
            "{} kept the earlier write on {GAMMA_WINS}",
            node.label
        );
        assert_eq!(
            held.get(ALPHA_WINS).map(String::as_str),
            Some("from-alpha"),
            "{} kept the earlier write on {ALPHA_WINS}",
            node.label
        );
    }

    // And it stays agreed, which is the answer to convention 5's test
    // asserting an absence: a cluster that converged and then started
    // overwriting itself would pass an assertion made once. An exchange
    // runs every couple of seconds, so this covers several rounds.
    keeps("and they go on agreeing", Duration::from_secs(6), || {
        nodes.iter().all(|node| {
            let held = node.retained();
            expected
                .iter()
                .all(|(topic, text)| held.get(*topic).map(String::as_str) == Some(*text))
        })
    });

    // Nothing reconnected: the partition was imposed and lifted on the
    // connections the cluster already had, which is what a gate can do and
    // a closed socket cannot.
    for (pair, gate) in &cluster.gates {
        assert_eq!(
            gate.connections(),
            established[pair],
            "the cluster healed by reconnecting through the gate {pair:?} rather than on \
             the connection it already had"
        );
    }

    // And every node still sees every other, by name: three nodes on one
    // machine would otherwise show an operator three identical rows, which
    // is what the derived default name was changed for.
    for node in nodes {
        let names: BTreeSet<String> = node.nodes().into_iter().map(|row| row.name).collect();
        assert_eq!(
            names,
            LABELS.iter().map(|label| (*label).to_owned()).collect(),
            "{} does not see three distinctly named nodes",
            node.label
        );
    }
}

/// Append whichever workers have `payload` waiting for them.
///
/// One short pass over all of them rather than a long wait on each:
/// every publication to a shared group is received by exactly one
/// member, so most of these find nothing and must find it quickly.
fn collect(workers: &mut [Client], payload: &str, got: &mut Vec<usize>) {
    for (which, worker) in workers.iter_mut().enumerate() {
        while let Some((topic, body)) = worker.delivered_within(Duration::from_millis(50)) {
            assert_eq!(topic, "jobs/one");
            assert_eq!(body, payload, "a member received a job out of order");
            got.push(which);
        }
    }
}

/// Three processes route a publication across the cluster.
///
/// Section 8.3 against the real binary, over real sockets, through the
/// gate. Four criteria in one test rather than four, because what each
/// of them costs is three operating system processes, three stores and
/// three handshakes, and the thing being asserted is a property of the
/// same three:
///
/// 1. a subscriber on the third node receives what a publisher on the
///    first one sends;
/// 2. a retained message published on one is delivered to a subscriber
///    that connects to another afterwards;
/// 3. a shared group with a member on each node receives **one** copy
///    per publication, and over enough of them every member is chosen;
/// 4. through the gate: two nodes severed from each other, and the two
///    that can still reach each other still deliver.
///
/// # How a test waits for a subscription to reach another node
///
/// Not by sleeping. A subscription becomes a register, the register is
/// broadcast, and the node that receives it puts it in its routing map
/// — milliseconds, but milliseconds nobody can bound on a loaded
/// machine. What *can* be asked is the publisher's own
/// acknowledgement: section 8.3 counts a node forwarded to as a
/// subscriber reached, so [MQTT-3.3.5]'s "no matching subscribers"
/// stops being the answer exactly when the register has arrived.
///
/// So these publish until the acknowledgement changes, drain whatever
/// those probes delivered, and only then assert about a publication of
/// their own. A test that slept would be slower and would still be a
/// race.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_deliver_across_the_cluster() {
    /// How long one delivery may take once the cluster has converged.
    ///
    /// A forward is one frame over loopback. Generous because this runs
    /// on shared hardware, and a flaky cluster test is worse than none.
    const DELIVERY: Duration = Duration::from_secs(10);

    let directory = tempfile::tempdir().expect("a temporary directory");
    let cluster = Cluster::start(directory.path()).await;
    let established = cluster.settle();
    let nodes = &cluster.nodes;

    // ---- 1. a publication crosses the cluster ----------------------
    let mut listener = Client::connect(nodes[2].mqtt, "the-listener");
    listener.subscribe("site/+/temp");

    until("alpha learns that gamma wants site/+/temp", || {
        nodes[0].publish_reaching("site/0/temp", "probe") > 0
    });
    // The probes above were delivered too, legitimately: they are
    // publications to a topic the subscriber asked for.
    listener.drain();

    let mut publisher = Client::connect(nodes[0].mqtt, "the-publisher");
    assert_eq!(
        publisher.publish("site/1/temp", "21.5"),
        ReasonCode::Success,
        "a publication a node in this cluster wants must not be answered \
         with no matching subscribers"
    );
    assert_eq!(
        listener.delivered_within(DELIVERY),
        Some(("site/1/temp".to_owned(), "21.5".to_owned())),
        "a publication on alpha did not reach a subscriber on gamma: {}",
        nodes[2].logged()
    );

    // ---- 2. a retained message is waiting on another node ----------
    //
    // Published on beta, read by a client that connects to gamma
    // *afterwards*: the register travels by section 8.2's replication
    // and the delivery is section 3.3.1.3's retained handling, so this
    // is the one case where the message reaches a client that was not
    // there when it was published.
    nodes[1].retain("site/9/state", "warm");
    until("gamma holds the retained message", || {
        nodes[2].retained().get("site/9/state").map(String::as_str) == Some("warm")
    });
    let mut late = Client::connect(nodes[2].mqtt, "the-latecomer");
    late.subscribe("site/9/state");
    assert_eq!(
        late.delivered_within(DELIVERY),
        Some(("site/9/state".to_owned(), "warm".to_owned())),
        "a retained message published on beta was not delivered on gamma"
    );

    // ---- 3. a shared group is delivered once -----------------------
    //
    // A member on each node, which is the arrangement that delivers
    // three times to a cluster that routes a group through the ordinary
    // map. Each publication goes through a different node's admin API,
    // so that section 6's local preference is what spreads the work —
    // a node with a member of its own serves the group itself, which is
    // the clause that is implementable before queue replication.
    const ROUNDS: usize = 3;
    let mut workers: Vec<Client> = (0..NODES)
        .map(|at| {
            let mut worker = Client::connect(nodes[at].mqtt, &format!("worker-{at}"));
            worker.subscribe("$share/workers/jobs/+");
            worker
        })
        .collect();
    until("every node knows the group has a member somewhere", || {
        (0..NODES).all(|at| nodes[at].publish_reaching("jobs/probe", "probe") > 0)
    });
    for worker in &mut workers {
        worker.drain();
    }

    let mut served = vec![0usize; NODES];
    for round in 0..ROUNDS {
        for (at, node) in nodes.iter().enumerate() {
            let payload = format!("job-{round}-{at}");
            assert!(
                node.publish_reaching("jobs/one", &payload) > 0,
                "{} would not publish to the group",
                node.label
            );
            // Exactly one member receives it, and which one is not what
            // is asserted: the whole point of a shared subscription is
            // that the cluster chooses.
            //
            // **Polled in short passes rather than waited for member by
            // member.** Two of the three are going to receive nothing,
            // and a long deadline each would be two long waits per
            // publication — the shape that turned this section into
            // forty seconds of a test that asserts twelve things.
            let mut got = Vec::new();
            let deadline = Instant::now() + DELIVERY;
            while got.is_empty() {
                assert!(
                    Instant::now() < deadline,
                    "{payload} reached no member of the group at all: {}",
                    node.logged()
                );
                collect(&mut workers, &payload, &mut got);
            }
            // And then one more pass, because a second delivery is the
            // defect this whole section exists for and it would arrive
            // a moment after the first.
            std::thread::sleep(Duration::from_millis(250));
            collect(&mut workers, &payload, &mut got);
            assert_eq!(
                got.len(),
                1,
                "{payload} was delivered to {} members of one group, not one: {}",
                got.len(),
                node.logged()
            );
            served[got[0]] += 1;
        }
    }
    assert_eq!(
        served.iter().sum::<usize>(),
        ROUNDS * NODES,
        "one delivery per publication, and no more: {served:?}"
    );
    assert!(
        served.iter().all(|count| *count > 0),
        "a member node was never chosen over {} publications: {served:?}",
        ROUNDS * NODES
    );

    // ---- 4. through the gate ---------------------------------------
    //
    // Alpha and beta are severed from each other in both directions,
    // and gamma reaches both. What is asserted is that the two nodes
    // that can still reach each other still deliver: a partition must
    // not stop the cluster routing where it can, which is section 8.5's
    // availability default.
    cluster.gates[&(0, 1)].cut(Direction::Forward);
    cluster.gates[&(0, 1)].cut(Direction::Backward);
    cluster.gates[&(1, 0)].cut(Direction::Forward);
    cluster.gates[&(1, 0)].cut(Direction::Backward);
    until("alpha and beta declare each other dead", || {
        nodes[0].how(&nodes[1].identity()).as_deref() == Some("down")
            && nodes[1].how(&nodes[0].identity()).as_deref() == Some("down")
    });

    let mut across = Client::connect(nodes[2].mqtt, "across-the-partition");
    across.subscribe("partitioned/+");
    until("alpha learns that gamma wants partitioned/+", || {
        nodes[0].publish_reaching("partitioned/probe", "probe") > 0
    });
    across.drain();
    assert_eq!(
        publisher.publish("partitioned/one", "still here"),
        ReasonCode::Success,
        "alpha should still reach gamma while it cannot reach beta"
    );
    assert_eq!(
        across.delivered_within(DELIVERY),
        Some(("partitioned/one".to_owned(), "still here".to_owned())),
        "the two reachable nodes stopped delivering to each other: {}",
        nodes[0].logged()
    );

    // And nothing reconnected: the partition was imposed on the
    // connections the cluster already had, which is what a gate can do
    // and a closed socket cannot.
    for (pair, gate) in &cluster.gates {
        assert_eq!(
            gate.connections(),
            established[pair],
            "the gate {pair:?} reconnected during the test"
        );
    }
}

/// A session moves to the node its client comes back to, and brings
/// what it was holding.
///
/// Section 8.4 end to end, against three processes: the subscription
/// is taken on one node, the message is published on a second while
/// the client is away, and the client reconnects to a third. Every
/// piece of that has been proved in isolation — the claim, the drain,
/// the chunking, the forwarding — and this is the one that proves they
/// were wired to each other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_session_moves_to_the_node_its_client_returns_to() {
    /// What the roaming client subscribes to.
    const FILTER: &str = "fleet/+/telemetry";
    /// And the topic published under it.
    const TOPIC: &str = "fleet/0007/telemetry";
    /// The message published while nobody is connected.
    const WHILE_AWAY: &str = "queued-while-away";

    let directory = tempfile::tempdir().expect("a temporary directory");
    let cluster = Cluster::start(directory.path()).await;
    cluster.settle();
    let nodes = &cluster.nodes;

    // ---- the session is made on alpha, and left there --------------
    let (mut rover, present) = Client::resuming(nodes[0].mqtt, "rover");
    assert!(
        !present,
        "a session nothing has ever held is not resumed, and a broker that said \
         otherwise would make every assertion below meaningless"
    );
    rover.subscribe(FILTER);
    assert!(
        nodes[0].sessions().contains("rover"),
        "alpha holds the session it just made"
    );
    // The socket goes and the session stays: `SESSION_EXPIRY` is what
    // makes this a session that outlives its connection rather than a
    // client that has finished.
    drop(rover);

    // ---- beta publishes to it while it is nowhere --------------
    //
    // Waited for rather than slept on: the register that says alpha has
    // a subscriber for the filter is a broadcast, and a publication made
    // before it lands reaches nobody. `publish_reaching` is what asks.
    // Every attempt before the last reached no node and so queued
    // nothing, which is what makes the single delivery below the one
    // this published.
    until(
        "beta learns that somebody in the cluster wants the filter",
        || nodes[1].publish_reaching(TOPIC, WHILE_AWAY) > 0,
    );

    // ---- and the client comes back to gamma --------------
    //
    // **Gamma by name, and without waiting for its copy to land.** A
    // version of this test that waited for the copy and reconnected to
    // whichever node held it passed every run — including runs with the
    // fix below removed — because the wait moved the copy's write ahead
    // of the connect and out of the race. The defect this guards is the
    // race itself, so the client comes back the moment the publish has
    // reached somebody, as a real client would.
    let holder = 2;
    let (mut rover, present) = Client::resuming(nodes[holder].mqtt, "rover");
    assert!(
        present,
        "the session was found, which is section 8.4: the node it returned to claimed \
         it and drained it from alpha rather than making a new one"
    );
    assert_eq!(
        rover.delivered_within(PATIENCE),
        Some((TOPIC.to_owned(), WHILE_AWAY.to_owned())),
        "what was queued on alpha was delivered where the client returned"
    );

    // The session is on the holder and **not on alpha**, which is the half
    // of a handover that a drain alone does not prove: a node that kept
    // its copy would go on being forwarded messages for those filters
    // and would deliver them all again next time the client came back.
    until("alpha has let the session go", || {
        !nodes[0].sessions().contains("rover")
    });
    assert!(
        nodes[holder].sessions().contains("rover"),
        "and the node it returned to has it: {:?}",
        nodes[holder].sessions()
    );

    // ---- and the subscription moved with it --------------
    //
    // Published until one arrives, because a migration moves two
    // registers independently — the holder gains a subscriber for the filter
    // and alpha loses one — and a publication made between the two is
    // forwarded only to the node that no longer holds the session.
    // Numbered, so the delivery says which attempt reached it rather
    // than being anonymous.
    let mut attempt = 0;
    // What beta said each publication reached, kept so a failure can
    // say whether beta forwarded to nobody, to the wrong node, or to
    // the right one that then delivered nothing. Three different
    // defects, one assertion, and the number tells them apart.
    let mut reached: Vec<u64> = Vec::new();
    let delivered = loop {
        attempt += 1;
        // The number beta reported is in the message because it tells
        // three defects apart: reaching nobody is a register that never
        // arrived, reaching one node that delivers nothing is a stale
        // route, and reaching the right node is a delivery fault. The
        // first of those shipped once, as a refused batch on the
        // claiming node — see the note above the reconnect.
        assert!(
            attempt <= 50,
            "no publication made after the move ever reached the session; beta reported \
             reaching, per attempt: {reached:?}"
        );
        reached.push(nodes[1].publish_reaching(TOPIC, &format!("after-the-move-{attempt}")));
        if let Some(delivery) = rover.delivered_within(Duration::from_millis(200)) {
            break delivery;
        }
    };
    assert_eq!(delivered.0, TOPIC);
    assert!(
        delivered.1.starts_with("after-the-move-"),
        "the delivery should be one of the publications made after the move, not \
         the one from before it: {delivered:?}"
    );

    // ---- and the migration is in the holder's exposition ----------
    //
    // Section 8.4 asks for the shortfall to be "counted **and
    // surfaced**", and it was counted from this milestone's second
    // task and surfaced nowhere until now. Read off the real process,
    // because the unit test behind these series reads a double and so
    // cannot see a counter wired to the wrong field.
    //
    // The zeroes are asserted beside the ones for the same reason the
    // double's values are all different: a counter reading a
    // neighbour's field is invisible while both hold the same number.
    for (series, expected) in [
        ("sdmqtt_sessions_claimed_total", 1),
        ("sdmqtt_sessions_migrated_total", 1),
        ("sdmqtt_sessions_migrated_incomplete_total", 0),
        ("sdmqtt_sessions_migrated_unreachable_total", 0),
        ("sdmqtt_migrated_messages_total", 1),
        ("sdmqtt_migrated_messages_missing_total", 0),
    ] {
        assert_eq!(
            nodes[holder].counter(series),
            Some(expected),
            "{series} on the claiming node, which drained one message"
        );
    }
    assert_eq!(
        nodes[0].counter("sdmqtt_sessions_claimed_total"),
        Some(0),
        "alpha claimed nothing: it was drained from, which is the other half"
    );
}

/// Two nodes accept the same client identifier while they cannot see
/// each other, and the heal resolves it.
///
/// Section 8.5. Both connects are accepted, because the default is
/// availability and a node that refused them would turn one node's
/// network fault into every client's outage. On heal the registers
/// merge, one claim wins, and **the loser's connection is closed with
/// "session taken over"** rather than being left as a second live
/// connection to one session.
///
/// # Which side wins is the rule's answer, not this test's
///
/// Both nodes read the same epoch and both wrote the same one, which
/// section 8.5 calls the expected outcome rather than the rare one. An
/// equal epoch breaks on the clock reading, so **the later claim wins**
/// — and that is what is asserted: the connection made first is the one
/// closed. The two are made [`APART`] so the tie cannot fall through to
/// the node UUID, which is a different rule and not this one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_that_accepted_one_client_resolve_on_heal() {
    /// Long enough that two claims cannot share a clock reading.
    const APART: Duration = Duration::from_millis(250);
    /// What the surviving client uses to prove it is still serving.
    const AFTERWARDS: &str = "twin/0009/afterwards";

    let directory = tempfile::tempdir().expect("a temporary directory");
    let cluster = Cluster::start(directory.path()).await;
    cluster.settle();
    let nodes = &cluster.nodes;

    // ---- gamma is severed, in both directions --------------
    //
    // Both, for the reason the convergence test gives: each half has to
    // take a claim of its own and neither may see the other's.
    for other in 0..NODES - 1 {
        cluster.gates[&(other, NODES - 1)].cut(Direction::Forward);
        cluster.gates[&(other, NODES - 1)].cut(Direction::Backward);
        cluster.gates[&(NODES - 1, other)].cut(Direction::Forward);
        cluster.gates[&(NODES - 1, other)].cut(Direction::Backward);
    }
    let gamma = nodes[2].identity();
    let alpha = nodes[0].identity();
    until("the other two declare gamma dead", || {
        nodes[0].how(&gamma).as_deref() == Some("down")
            && nodes[1].how(&gamma).as_deref() == Some("down")
    });
    // **And gamma declares them dead**, which is not the same moment
    // and is the half that matters here: gamma is about to accept a
    // claim for a session it must not be able to ask anybody about.
    until("gamma declares the other two dead", || {
        nodes[2].how(&alpha).as_deref() == Some("down")
    });

    // ---- both sides accept the same client --------------
    let (mut first, _) = Client::resuming(nodes[0].mqtt, "twin");
    first.subscribe("twin/+/first");
    std::thread::sleep(APART);
    let (mut second, _) = Client::resuming(nodes[2].mqtt, "twin");
    second.subscribe("twin/+/second");

    // The split is real while it lasts, which is what makes the
    // resolution below something that had to happen rather than
    // something that was already true.
    assert!(
        nodes[0].sessions().contains("twin"),
        "alpha accepted the client it could serve"
    );
    assert!(
        nodes[2].sessions().contains("twin"),
        "and so did gamma, which is section 8.5's availability and not a defect"
    );

    // ---- and it heals --------------
    for other in 0..NODES - 1 {
        cluster.gates[&(other, NODES - 1)].heal(Direction::Forward);
        cluster.gates[&(other, NODES - 1)].heal(Direction::Backward);
        cluster.gates[&(NODES - 1, other)].heal(Direction::Forward);
        cluster.gates[&(NODES - 1, other)].heal(Direction::Backward);
    }

    // The earlier claim loses, and is **told why**. A socket that
    // simply ended would leave a client to guess, which is the whole
    // difference section 8.5 is about; `Ending` can tell the two apart.
    assert_eq!(
        first.closed_within(PATIENCE),
        Ending::Said(ReasonCode::SessionTakenOver),
        "the connection whose claim was earlier is the one closed, and it is closed \
         with the reason section 8.5 names"
    );
    until("alpha has let the session go", || {
        !nodes[0].sessions().contains("twin")
    });
    // **And its stream of the session with it.** Section 8.6: a stale
    // owner returning from a partition discards its copy. The session
    // left alpha above; what this asserts is that alpha stopped
    // counting it as a session it replicates, which is the owner-side
    // table and is a separate thing from the session — it was left
    // behind once, and the gauge went on saying a session nobody held
    // was being streamed.
    until("and stopped streaming it", || {
        nodes[0].counter("sdmqtt_replicated_sessions") == Some(0)
    });

    // And the winner is still serving, asserted by using it rather
    // than by waiting for nothing to happen. Published and subscribed
    // on gamma, so this is about the connection and the session rather
    // than about a register reaching another node.
    second.subscribe(AFTERWARDS);
    assert_eq!(
        second.publish(AFTERWARDS, "still-here"),
        ReasonCode::Success,
        "the surviving connection still publishes"
    );
    assert_eq!(
        second.delivered_within(PATIENCE),
        Some((AFTERWARDS.to_owned(), "still-here".to_owned())),
        "and its session still holds subscriptions"
    );
    assert!(
        nodes[2].sessions().contains("twin"),
        "gamma kept the session it won"
    );
}

/// A session's queue exists on a second node, section 8.6.
///
/// The whole of this milestone's first half in one test, against three
/// real processes: a message queued for an offline session on the node
/// that owns it is streamed to the node rendezvous put behind it, and
/// that node holds a copy it does not serve.
///
/// # Which node holds each copy is not this test's to predict
///
/// Placement is a hash of the session identity against the live
/// members, and the members are three UUIDs minted at startup. So this
/// asserts what the design promises — **`replication_factor` copies in
/// the cluster, one of them not on the owner** — rather than naming a
/// node, which would be asserting the hash rather than the behaviour.
///
/// # Several sessions, and that is not padding
///
/// Every one of them is owned by alpha, because that is where their
/// clients connected, but rendezvous ranks the three nodes per session
/// and alpha is **not** in the top two for all of them. A build that
/// streamed the placement set whole, without removing itself, puts two
/// copies on other nodes for exactly the sessions where alpha is
/// ranked third — and one copy, correctly, for all the rest. With one
/// session that mutation survives on a coin flip; it was written with
/// one session first, and it did.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queued_message_is_copied_to_a_second_node() {
    /// How many sessions to leave queued.
    ///
    /// Enough that rendezvous ranks alpha outside the top two for some
    /// of them; see the note above. Six, so the test stays seconds
    /// rather than minutes.
    const SESSIONS: usize = 6;

    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut cluster = Cluster::start(directory.path()).await;
    cluster.settle();

    // Nothing is replicated before anything is queued, and saying so
    // is what makes the count below a change rather than a reading.
    for node in cluster.nodes.iter() {
        assert_eq!(
            node.counter("sdmqtt_replica_sessions"),
            Some(0),
            "{} should hold no copies before anything is queued",
            node.label
        );
    }

    // Sessions on alpha with queues and nobody connected to drain
    // them: the messages stay in the queues, which is what gets
    // copied.
    for nth in 0..SESSIONS {
        let filter = format!("fleet/{nth}/+");
        let topic = format!("fleet/{nth}/telemetry");
        let (mut rover, _) = Client::resuming(cluster.nodes[0].mqtt, &format!("rover-{nth}"));
        rover.subscribe(&filter);
        drop(rover);
        until("beta learns that somebody wants the filter", || {
            cluster.nodes[1].publish_reaching(&topic, "copy-me") > 0
        });
    }

    // **One copy each, and no more.** The default factor is two: the
    // owner and one follower. A build that streamed to every member
    // would put two copies of each session elsewhere, and one that
    // took the placement set whole without removing itself would do
    // the same for every session alpha is not ranked in the top two
    // for.
    let copies_elsewhere = |cluster: &Cluster| {
        cluster
            .nodes
            .iter()
            .skip(1)
            .filter_map(|node| node.counter("sdmqtt_replica_sessions"))
            .sum::<u64>()
    };
    until("every session has a copy on another node", || {
        copies_elsewhere(&cluster) == SESSIONS as u64
    });
    keeps(
        "and exactly one copy each, not two",
        Duration::from_secs(3),
        || copies_elsewhere(&cluster) == SESSIONS as u64,
    );

    // ---- and the copies are on a disk, not in a table --------------
    //
    // **The assertion the first version of this test did not make, and
    // the defect it did not catch.** A follower wrote queue records for
    // a session its store did not hold, which a store refuses; the
    // copy existed only in the in-memory table, and
    // `sdmqtt_replica_sessions` counts that table, so every gauge said
    // the right thing while the disk held nothing.
    //
    // A restart is the only way to ask. Each follower is killed and
    // started again against the same store, and what it reports
    // afterwards is what it had actually written down.
    let held_before: Vec<Option<u64>> = cluster
        .nodes
        .iter()
        .map(|node| node.counter("sdmqtt_replica_sessions"))
        .collect();
    for at in 1..NODES {
        cluster.nodes[at].restart();
    }
    for (at, expected) in held_before.iter().enumerate().skip(1) {
        until("the node comes back holding what it had", || {
            cluster.nodes[at].counter("sdmqtt_replica_sessions") == *expected
        });
    }
    assert_eq!(
        copies_elsewhere(&cluster),
        SESSIONS as u64,
        "every copy survived a restart, which means it was on a disk"
    );

    // The owner says it is streaming all of them, and says the copies
    // caught up. The lag is section 8.6's loss window: zero here is
    // the followers confirming they hold what the owner holds.
    assert_eq!(
        cluster.nodes[0].counter("sdmqtt_replicated_sessions"),
        Some(SESSIONS as u64),
        "alpha owns every session and streams every one"
    );
    until("the copies catch up", || {
        cluster.nodes[0].counter("sdmqtt_replication_lag") == Some(0)
    });
}

/// Killing the owner does not lose the queue, section 8.6's failover.
///
/// The exit criterion this milestone is for, in its simplest form: a
/// session with a queued message, its owner killed with the signal
/// rather than asked, and the client reconnecting somewhere else and
/// receiving the message — from the node that held the copy, over a
/// link, because the node it reconnected to is deliberately not that
/// one.
///
/// Killed, not stopped: a clean shutdown could hand the session over
/// on the way out, and that would be section 8.4 working rather than
/// section 8.6.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killing_the_owner_does_not_lose_the_queue() {
    const FILTER: &str = "fleet/+/telemetry";
    const TOPIC: &str = "fleet/0007/telemetry";
    const WHILE_AWAY: &str = "survives-the-owner";

    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut cluster = Cluster::start(directory.path()).await;
    cluster.settle();

    // A session on alpha with one queued message, copied to one of the
    // other two.
    let (mut rover, _) = Client::resuming(cluster.nodes[0].mqtt, "rover");
    rover.subscribe(FILTER);
    drop(rover);
    until("beta learns that somebody wants the filter", || {
        cluster.nodes[1].publish_reaching(TOPIC, WHILE_AWAY) > 0
    });
    until("a copy of the session exists on another node", || {
        cluster
            .nodes
            .iter()
            .skip(1)
            .any(|node| node.counter("sdmqtt_replica_sessions") == Some(1))
    });
    let holder = (1..NODES)
        .find(|at| cluster.nodes[*at].counter("sdmqtt_replica_sessions") == Some(1))
        .expect("the wait above found one");
    // The node the client comes back to is the one that holds nothing,
    // so the queue has to cross a link to reach it.
    let elsewhere = (1..NODES).find(|at| *at != holder).expect("three nodes");

    // ---- the owner dies --------------
    let alpha = cluster.nodes[0].kill();
    until("the survivors declare alpha dead", || {
        (1..NODES).all(|at| cluster.nodes[at].how(&alpha).as_deref() == Some("down"))
    });

    // ---- and the client comes back somewhere that held nothing ----
    let (mut rover, present) = Client::resuming(cluster.nodes[elsewhere].mqtt, "rover");
    assert!(present, "the session was found: pulled from the copy");
    assert_eq!(
        rover.delivered_within(PATIENCE),
        Some((TOPIC.to_owned(), WHILE_AWAY.to_owned())),
        "the message queued before the owner died was delivered after it"
    );

    // ---- and the exposition says how --------------
    //
    // Unreachable once, pulled once, one message pulled, and **nothing
    // missing**: the shortfall is what is still lost after the pull,
    // and nothing is.
    for (series, expected) in [
        ("sdmqtt_sessions_claimed_total", 1),
        ("sdmqtt_sessions_migrated_unreachable_total", 1),
        ("sdmqtt_sessions_migrated_incomplete_total", 1),
        ("sdmqtt_sessions_pulled_total", 1),
        ("sdmqtt_pulled_messages_total", 1),
        ("sdmqtt_migrated_messages_missing_total", 0),
    ] {
        assert_eq!(
            cluster.nodes[elsewhere].counter(series),
            Some(expected),
            "{series} on the node that pulled"
        );
    }
}

/// Of two copies, the one furthest ahead is the one taken.
///
/// **This is the test the chooser exists for.** With a factor of three
/// every node holds a copy, and the link carrying alpha's stream to
/// gamma is stalled so gamma's copy falls behind beta's. Alpha is
/// killed, and the client comes back to **gamma** — the laggard, whose
/// own copy answers first and costs no round trip — which must pull
/// from beta anyway, because beta's mark is higher. A chooser that
/// took the nearest copy would deliver a short queue here and look
/// correct on every test where the nearest copy happened to be whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_copy_furthest_ahead_is_the_one_pulled() {
    const FILTER: &str = "fleet/+/telemetry";
    const TOPIC: &str = "fleet/0042/telemetry";
    /// How many to queue. Enough that a short copy is unmistakable.
    const MESSAGES: usize = 5;

    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut cluster = Cluster::start_with_factor(directory.path(), 3).await;
    cluster.settle();

    let (mut rover, _) = Client::resuming(cluster.nodes[0].mqtt, "rover");
    rover.subscribe(FILTER);
    drop(rover);
    until("beta learns that somebody wants the filter", || {
        cluster.nodes[1].publish_reaching(TOPIC, "message-0") > 0
    });
    // Both followers have the first message: a factor of three is
    // every node.
    until("both other nodes hold a copy", || {
        (1..NODES).all(|at| cluster.nodes[at].counter("sdmqtt_replica_sessions") == Some(1))
    });

    // ---- gamma's copy is held back --------------
    //
    // Stalled rather than cut: the bytes alpha sends gamma are read by
    // nobody, so alpha's link queue to gamma fills and drops, and
    // gamma's mark stops. Beta's does not.
    cluster.gates[&(0, 2)].stall(Direction::Forward);
    for nth in 1..MESSAGES {
        assert!(
            cluster.nodes[1].publish_reaching(TOPIC, &format!("message-{nth}")) > 0,
            "the filter is known by now"
        );
    }
    // **Beta has every change and gamma has one, and both are waited
    // for before the owner is killed.** The first version of this test
    // waited only for alpha to report *a* copy behind, which is true
    // the moment gamma lags by one — and then killed alpha while it was
    // still streaming to beta. Beta had one message on one run and
    // three on the next, the chooser picked it correctly each time,
    // and the test failed for a reason that had nothing to do with
    // the chooser. The applied counter is what makes a follower's
    // progress something a test, or an operator, can wait on.
    until("beta has applied every change", || {
        cluster.nodes[1].counter("sdmqtt_replica_changes_applied_total") == Some(MESSAGES as u64)
    });
    assert_eq!(
        cluster.nodes[2].counter("sdmqtt_replica_changes_applied_total"),
        Some(1),
        "gamma's link is stalled, so it applied the first change and no more"
    );
    until("alpha reports a copy behind", || {
        cluster.nodes[0]
            .counter("sdmqtt_replication_lag")
            .is_some_and(|lag| lag > 0)
    });

    // ---- the owner dies, and the laggard is asked --------------
    let alpha = cluster.nodes[0].kill();
    until("the survivors declare alpha dead", || {
        (1..NODES).all(|at| cluster.nodes[at].how(&alpha).as_deref() == Some("down"))
    });
    let (mut rover, present) = Client::resuming(cluster.nodes[2].mqtt, "rover");
    assert!(present);

    // **The counter first, because it says which copy was taken.** A
    // short delivery below could be a pull from the wrong copy or a
    // delivery fault after the right one; the number of messages
    // pulled tells the two apart, and the node's own log says which
    // copy it chose and why.
    let pulled = cluster.nodes[2].counter("sdmqtt_pulled_messages_total");
    let lines_about = |node: &Node, needles: &[&str]| {
        node.logged()
            .lines()
            .filter(|line| needles.iter().any(|needle| line.contains(needle)))
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert_eq!(
        pulled,
        Some(MESSAGES as u64),
        "gamma should have pulled beta's whole copy; gamma's choice:\n{}\nalpha's stream \
         (its log outlives it):\n{}\nbeta's side:\n{}",
        lines_about(&cluster.nodes[2], &["choosing a copy", "copy answered"]),
        lines_about(
            &cluster.nodes[0],
            &["WARN", "ERROR", "copy", "stream", "follower"]
        ),
        lines_about(
            &cluster.nodes[1],
            &["WARN", "ERROR", "copy", "stream", "follower"]
        ),
    );

    // Every message, in order: the queue came from beta, not from the
    // copy gamma itself was holding.
    let mut got = Vec::new();
    while let Some((_, payload)) = rover.delivered_within(Duration::from_secs(5)) {
        got.push(payload);
        if got.len() == MESSAGES {
            break;
        }
    }
    assert_eq!(
        got,
        (0..MESSAGES)
            .map(|nth| format!("message-{nth}"))
            .collect::<Vec<_>>(),
        "the furthest copy was the one pulled, and all of it was delivered"
    );
    assert_eq!(
        cluster.nodes[2].counter("sdmqtt_migrated_messages_missing_total"),
        Some(0)
    );
}

/// Queue `count` sessions on alpha, each with one message, and wait for
/// every one to have a copy on another node. Returns how many copies
/// each of beta and gamma holds, in that order.
///
/// The split between the two is rendezvous's and varies by run, which
/// is why it is returned rather than assumed: the tests below assert
/// against what the hash actually did.
fn queued_and_copied(cluster: &Cluster, count: usize) -> [u64; 2] {
    for nth in 0..count {
        let filter = format!("fleet/{nth}/+");
        let topic = format!("fleet/{nth}/telemetry");
        let (mut rover, _) = Client::resuming(cluster.nodes[0].mqtt, &format!("rover-{nth}"));
        rover.subscribe(&filter);
        drop(rover);
        until("beta learns that somebody wants the filter", || {
            cluster.nodes[1].publish_reaching(&topic, "copy-me") > 0
        });
    }
    let held = |at: usize| {
        cluster.nodes[at]
            .counter("sdmqtt_replica_sessions")
            .unwrap_or(0)
    };
    until("every session has a copy on another node", || {
        held(1) + held(2) == count as u64
    });
    [held(1), held(2)]
}

/// A node gone past the grace is repaired away from.
///
/// Section 8.6's re-replication, with the grace short enough to wait
/// for: the node holding the copies is killed, the grace runs out, and
/// every session it held a copy of is sent to the node that remains.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_gone_past_the_grace_is_repaired_away_from() {
    const SESSIONS: usize = 4;
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut cluster = Cluster::start_with(
        directory.path(),
        Knobs {
            gone_after_ms: 2_000,
            ..Knobs::default()
        },
    )
    .await;
    cluster.settle();
    let [on_beta, on_gamma] = queued_and_copied(&cluster, SESSIONS);
    // Kill whichever holds more, so there is something to repair.
    let (victim, survivor, lost) = if on_beta >= on_gamma {
        (1, 2, on_beta)
    } else {
        (2, 1, on_gamma)
    };
    let before = cluster.nodes[survivor]
        .counter("sdmqtt_replica_sessions")
        .unwrap_or(0);
    cluster.nodes[victim].kill();

    until("the survivor holds every copy the victim held", || {
        cluster.nodes[survivor].counter("sdmqtt_replica_sessions") == Some(before + lost)
    });
    assert_eq!(
        cluster.nodes[0].counter("sdmqtt_replica_repairs_done_total"),
        Some(lost),
        "exactly the sessions the victim held were repaired, and no others"
    );
    until("and nothing is pending any more", || {
        cluster.nodes[0].counter("sdmqtt_replica_repairs_pending") == Some(0)
    });
}

/// Within the grace, nothing is copied.
///
/// **The test that matters**, because a build that repairs on a missed
/// heartbeat passes the one above and copies the whole queue estate
/// every time an operator restarts a node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn within_the_grace_nothing_is_copied() {
    const SESSIONS: usize = 4;
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut cluster = Cluster::start(directory.path()).await;
    cluster.settle();
    let [on_beta, on_gamma] = queued_and_copied(&cluster, SESSIONS);
    let (victim, survivor) = if on_beta >= on_gamma { (1, 2) } else { (2, 1) };
    let before = cluster.nodes[survivor]
        .counter("sdmqtt_replica_sessions")
        .unwrap_or(0);
    let gone = cluster.nodes[victim].kill();
    until("alpha declares the victim dead", || {
        cluster.nodes[0].how(&gone).as_deref() == Some("down")
    });

    // Dead, and still placed: the default grace is an hour.
    keeps(
        "no copy moves to the survivor",
        Duration::from_secs(5),
        || {
            cluster.nodes[survivor].counter("sdmqtt_replica_sessions") == Some(before)
                && cluster.nodes[0].counter("sdmqtt_replica_repairs_done_total") == Some(0)
        },
    );
    assert_eq!(
        cluster.nodes[0].counter("sdmqtt_replica_repairs_pending"),
        Some(0),
        "a node inside the grace keeps its place, so nothing is short"
    );
}

/// Forgetting a node starts the repair at once, and the two refusals.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forgetting_a_node_starts_the_repair_at_once() {
    const SESSIONS: usize = 4;
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut cluster = Cluster::start(directory.path()).await;
    cluster.settle();
    let [on_beta, on_gamma] = queued_and_copied(&cluster, SESSIONS);
    let (victim, survivor, lost) = if on_beta >= on_gamma {
        (1, 2, on_beta)
    } else {
        (2, 1, on_gamma)
    };
    let before = cluster.nodes[survivor]
        .counter("sdmqtt_replica_sessions")
        .unwrap_or(0);
    let gone = cluster.nodes[victim].kill();
    until("alpha declares the victim dead", || {
        cluster.nodes[0].how(&gone).as_deref() == Some("down")
    });

    // The two refusals first, because they are cheap and because a
    // handler that forgot *something* for any identifier would pass
    // the success below.
    let (code, _) = cluster.nodes[0].delete("/api/v1/nodes/not-an-identifier");
    assert_eq!(code, 404, "an identifier this node never met is not found");
    let me = cluster.nodes[0].identity();
    let (code, _) = cluster.nodes[0].delete(&format!("/api/v1/nodes/{me}"));
    assert_eq!(code, 400, "a node cannot forget itself");

    let (code, body) = cluster.nodes[0].delete(&format!("/api/v1/nodes/{gone}"));
    assert_eq!(code, 200, "{body}");
    assert!(
        body.contains(&gone),
        "the answer names what was forgotten: {body}"
    );
    assert!(
        !cluster.nodes[0].nodes().iter().any(|row| row.id == gone),
        "and it is out of alpha's membership, not marked down"
    );
    // With the grace still an hour away, this is the override doing
    // the work.
    until("the survivor holds every copy the victim held", || {
        cluster.nodes[survivor].counter("sdmqtt_replica_sessions") == Some(before + lost)
    });
    assert_eq!(
        cluster.nodes[0].counter("sdmqtt_replica_repairs_done_total"),
        Some(lost)
    );
    let (code, _) = cluster.nodes[0].delete(&format!("/api/v1/nodes/{gone}"));
    assert_eq!(code, 404, "forgetting twice finds nothing the second time");

    // Forgotten is not banned: the node coming back, on the same store
    // and so the same identity, is met again like any newcomer.
    cluster.nodes[victim].restart();
    until("the forgotten node is re-admitted when heard from", || {
        cluster.nodes[0].how(&gone).as_deref() == Some("up")
    });
}

/// Repair is rate limited.
///
/// One session a second, and sessions enough that the difference
/// between "one a second" and "all at once" is several seconds wide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_sends_no_more_sessions_a_second_than_it_is_allowed() {
    const SESSIONS: usize = 6;
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut cluster = Cluster::start_with(
        directory.path(),
        Knobs {
            gone_after_ms: 2_000,
            repair_per_second: 1,
            ..Knobs::default()
        },
    )
    .await;
    cluster.settle();
    let [on_beta, on_gamma] = queued_and_copied(&cluster, SESSIONS);
    let (victim, lost) = if on_beta >= on_gamma {
        (1, on_beta)
    } else {
        (2, on_gamma)
    };
    assert!(
        lost >= 3,
        "the hash gave the victim {lost}; six sessions should split 3+"
    );
    cluster.nodes[victim].kill();

    // The first repair marks the grace having run out. From there, at
    // one a second, the rest take `lost - 1` more seconds at least.
    // At least one, not exactly one: a build with no limit sends every
    // session in one sweep and would never show a count of one, and
    // the timing below is what this test is about, not the count.
    until("the first session is repaired", || {
        cluster.nodes[0]
            .counter("sdmqtt_replica_repairs_done_total")
            .is_some_and(|done| done >= 1)
    });
    let started = Instant::now();
    until("every session is repaired", || {
        cluster.nodes[0].counter("sdmqtt_replica_repairs_done_total") == Some(lost)
    });
    let took = started.elapsed();
    assert!(
        took >= Duration::from_millis((lost - 1) * 800),
        "{lost} sessions at one a second cannot finish in {took:?}: the limit was not \
         applied"
    );
}

// -------------------------------------------------------------------
// Milestone 6's exit criteria, against the real binary.
// -------------------------------------------------------------------

/// Everything `rover` is delivered, in order, giving up after five
/// seconds of silence.
fn everything_delivered_to(rover: &mut Client) -> Vec<String> {
    let mut got = Vec::new();
    while let Some((_, payload)) = rover.delivered_within(Duration::from_secs(5)) {
        got.push(payload);
    }
    got
}

/// Synchronous mode loses nothing when the owner is killed.
///
/// **No waiting on a gauge between the last publish and the kill.**
/// The test above this one waits for a copy to exist before it kills,
/// which is a test of the stream; this one is a test of the mode, and
/// the mode's whole claim is that the acknowledgement *is* the
/// evidence a copy exists. So each publish is acknowledged, the owner
/// is killed the moment the last one is, and every message has to be
/// on the node the client comes back to.
///
/// **Published on the owner.** Section 8.6 describes both modes in
/// terms of the owner having the message: that is what the node that
/// accepted the publish can promise about its own sessions. A publish
/// accepted on *another* node is forwarded under section 8.3, and its
/// acknowledgement is that node's — the owner has not appended yet
/// when it is sent. The first version of this test published on beta
/// and lost three of eight acknowledged messages, which is that gap
/// and not a fault in the mode; it is written down in the spec rather
/// than papered over here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synchronous_mode_loses_nothing_when_the_owner_is_killed() {
    const FILTER: &str = "fleet/+/telemetry";
    const TOPIC: &str = "fleet/0007/telemetry";
    const MESSAGES: usize = 8;

    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut cluster = Cluster::start_with(
        directory.path(),
        Knobs {
            durability: "sync",
            ..Knobs::default()
        },
    )
    .await;
    cluster.settle();

    let (mut rover, _) = Client::resuming(cluster.nodes[0].mqtt, "rover");
    rover.subscribe(FILTER);
    drop(rover);
    for nth in 0..MESSAGES {
        assert_eq!(
            cluster.nodes[0].publish_reaching(TOPIC, &format!("message-{nth}")),
            1,
            "the owner knows its own session's filter"
        );
    }
    // Acknowledged, so a copy exists: the mode says so, and nothing
    // else is consulted before the kill. The counter is read first
    // because the node is about to die, and a zero there is what
    // makes the acknowledgements evidence.
    assert_eq!(
        cluster.nodes[0].counter("sdmqtt_publishes_unconfirmed_total"),
        Some(0),
        "every acknowledgement was a confirmed one; otherwise the mode said nothing"
    );
    let alpha = cluster.nodes[0].kill();

    until("the survivors declare alpha dead", || {
        (1..NODES).all(|at| cluster.nodes[at].how(&alpha).as_deref() == Some("down"))
    });
    // Back on whichever node holds nothing, so the queue crosses a
    // link to get there. Read after the kill, which is the only place
    // a gauge is consulted and which cannot have helped the copy.
    let holder = (1..NODES)
        .find(|at| cluster.nodes[*at].counter("sdmqtt_replica_sessions") == Some(1))
        .expect("the mode put a copy somewhere");
    let elsewhere = (1..NODES).find(|at| *at != holder).expect("three nodes");
    let (mut rover, present) = Client::resuming(cluster.nodes[elsewhere].mqtt, "rover");
    assert!(present);

    assert_eq!(
        everything_delivered_to(&mut rover),
        (0..MESSAGES)
            .map(|nth| format!("message-{nth}"))
            .collect::<Vec<_>>(),
        "every acknowledged message, in order, from a node that never saw them"
    );
    assert_eq!(
        cluster.nodes[elsewhere].counter("sdmqtt_migrated_messages_missing_total"),
        Some(0)
    );
}

/// Asynchronous mode's loss is bounded by the lag metric.
///
/// **Asserted against the number, not against "some arrived".** The
/// follower is held back, the owner's lag is read, the owner is
/// killed, and what did not arrive has to be within what the metric
/// said — and the metric has to have said something, because a lag of
/// zero on a stalled follower is a metric reading nothing. A build
/// with no replication at all delivers nothing and reads a lag of
/// zero, and fails on the first assertion.
///
/// Published on the owner, for the reason the synchronous test gives:
/// the lag is the owner's position against its follower's, and a
/// publish forwarded from another node is not at the owner yet when
/// that node answers, so a count of answers overstates what the lag
/// could know about.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn asynchronous_loss_is_bounded_by_the_lag_metric() {
    const FILTER: &str = "fleet/+/telemetry";
    const TOPIC: &str = "fleet/0042/telemetry";
    /// Published while the follower is stalled.
    const BEHIND: usize = 6;

    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut cluster = Cluster::start(directory.path()).await;
    cluster.settle();

    let (mut rover, _) = Client::resuming(cluster.nodes[0].mqtt, "rover");
    rover.subscribe(FILTER);
    drop(rover);
    assert_eq!(cluster.nodes[0].publish_reaching(TOPIC, "message-0"), 1);
    until("the first message has a copy somewhere", || {
        (1..NODES)
            .any(|at| cluster.nodes[at].counter("sdmqtt_replica_changes_applied_total") == Some(1))
    });
    let holder = (1..NODES)
        .find(|at| cluster.nodes[*at].counter("sdmqtt_replica_changes_applied_total") == Some(1))
        .expect("the wait above found one");
    let elsewhere = (1..NODES).find(|at| *at != holder).expect("three nodes");

    // ---- the follower is held back, and the owner keeps taking ----
    cluster.gates[&(0, holder)].stall(Direction::Forward);
    for nth in 1..=BEHIND {
        assert_eq!(
            cluster.nodes[0].publish_reaching(TOPIC, &format!("message-{nth}")),
            1
        );
    }
    until("alpha reports the copy behind", || {
        cluster.nodes[0]
            .counter("sdmqtt_replication_lag")
            .is_some_and(|lag| lag > 0)
    });
    let lag = cluster.nodes[0]
        .counter("sdmqtt_replication_lag")
        .expect("read a moment ago");
    assert!(
        lag as usize <= BEHIND,
        "a lag of {lag} is more than was published while stalled; the metric overstates"
    );

    // ---- the owner dies with that lag outstanding ----
    let alpha = cluster.nodes[0].kill();
    until("the survivors declare alpha dead", || {
        (1..NODES).all(|at| cluster.nodes[at].how(&alpha).as_deref() == Some("down"))
    });
    let (mut rover, present) = Client::resuming(cluster.nodes[elsewhere].mqtt, "rover");
    assert!(present, "the session was found: pulled from the copy");
    let got = everything_delivered_to(&mut rover);

    let published = BEHIND + 1;
    let lost = published - got.len();
    assert!(
        lost <= lag as usize,
        "{lost} of {published} messages were lost and the lag metric said at most {lag}"
    );
    assert_eq!(
        got,
        (0..got.len())
            .map(|nth| format!("message-{nth}"))
            .collect::<Vec<_>>(),
        "what arrived is a prefix: the loss is the tail, not holes"
    );
    assert!(
        !got.is_empty(),
        "the message copied before the stall has to be there, or nothing was replicated"
    );
}

// -------------------------------------------------------------------
// Section 12.1: a user is replicated metadata.
// -------------------------------------------------------------------

/// A user created on one node connects on another, and a delete on one
/// refuses on another. Section 8.2 lists users among what replicates;
/// this is the sentence against three processes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_user_created_on_one_node_connects_on_another() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let cluster = Cluster::start(directory.path()).await;
    cluster.settle();

    let answered =
        cluster.nodes[0].post("/api/v1/users", r#"{"name":"alice","password":"hunter2"}"#);
    assert!(answered.contains("\"name\":\"alice\""), "{answered}");
    assert!(
        !answered.contains("hunter2"),
        "the answer carries no secret: {answered}"
    );

    // On the node it was made on, at once.
    assert_eq!(
        Client::connect_with(cluster.nodes[0].mqtt, "alice-at-alpha", "alice", "hunter2"),
        Some(ReasonCode::Success)
    );
    assert_eq!(
        Client::connect_with(cluster.nodes[0].mqtt, "alice-at-alpha", "alice", "wrong"),
        Some(ReasonCode::NotAuthorized)
    );
    // On the others, once the register has arrived.
    for at in 1..NODES {
        until("the user has replicated", || {
            Client::connect_with(
                cluster.nodes[at].mqtt,
                "alice-elsewhere",
                "alice",
                "hunter2",
            ) == Some(ReasonCode::Success)
        });
        let listed = cluster.nodes[at].get("/api/v1/users");
        assert!(listed.contains("\"name\":\"alice\""), "{listed}");
    }

    // And through section 12.2's exchange, on a node the user was not
    // made on: the verifier replicated with the hash.
    assert_eq!(
        Client::connect_scram(cluster.nodes[1].mqtt, "alice-scram", "alice", "hunter2"),
        Some(ReasonCode::Success)
    );
    assert_eq!(
        Client::connect_scram(cluster.nodes[1].mqtt, "alice-scram", "alice", "wrong"),
        Some(ReasonCode::NotAuthorized)
    );

    // A delete on gamma refuses on alpha, in time.
    let (code, _) = cluster.nodes[2].delete("/api/v1/users/alice");
    assert_eq!(code, 200);
    until("the delete has replicated", || {
        Client::connect_with(cluster.nodes[0].mqtt, "alice-at-alpha", "alice", "hunter2")
            == Some(ReasonCode::NotAuthorized)
    });
}
