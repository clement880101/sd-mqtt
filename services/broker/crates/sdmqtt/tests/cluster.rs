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

use std::collections::BTreeMap;
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
    let labels = ["alpha", "beta", "gamma"];

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
        let config = directory.path().join(format!("{label}.toml"));
        let store = directory.path().join(format!("{label}-store"));
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
        let log = directory.path().join(format!("{label}.log"));
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
                labels.contains(&row.name.as_str()),
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
