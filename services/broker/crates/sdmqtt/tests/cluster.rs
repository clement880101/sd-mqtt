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

use cluster::testing::gate::Gate;
use cluster::testing::Direction;

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
    admin: SocketAddr,
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
            let seeds: Vec<SocketAddr> = (0..NODES)
                .filter(|to| *to != at)
                .map(|to| gates[&(at, to)].address())
                .collect();
            write_config(
                &config,
                &store,
                label,
                loopback(free_port()),
                admin,
                ports[at],
                advertised[at],
                &seeds,
            );
            let log = directory.join(format!("{label}.log"));
            let child = Command::new(env!("CARGO_BIN_EXE_sdmqtt"))
                .arg("serve")
                .arg("--config")
                .arg(&config)
                // On the command line as well as in the file, because the
                // environment is emptied below and an empty
                // `SDMQTT_STORAGE_DIRECTORY` takes a store *away*. A flag
                // overrides both, which is what `tests/bridge.rs` does and
                // for the same reason.
                .arg("--store-directory")
                .arg(&store)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                // Emptied so that a configuration in the environment of
                // whoever is running the tests cannot reach these brokers.
                .env("SDMQTT_CONFIG", "")
                .env("SDMQTT_STORAGE_DIRECTORY", "")
                .stderr(Stdio::from(
                    File::create(&log).expect("a log file can be created"),
                ))
                .spawn()
                .expect("the broker binary runs");
            let mut node = Node {
                child,
                admin,
                log,
                label,
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
