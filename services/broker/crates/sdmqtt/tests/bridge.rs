//! Section 9.2's bridge, against a real broker, over real sockets.
//!
//! **The other end is this broker.** The plan says so and says why: a
//! bridge is a client, this project has a server lying around, and a
//! test that moves messages between two `sdmqtt` processes exercises
//! every layer the mock a unit test would use replaces.
//!
//! It is also not sufficient on its own, and the plan says that too: a
//! bridge that only works against itself has proved the two halves
//! agree rather than that either is right. What carries that weight is
//! the per-clause tests in `rules::bridge`, each of which quotes the
//! clause of MQTT-v5.0 it is about, and — where it can be reached — the
//! conformance suite. See the note at the end of this file.
//!
//! # Two processes, not two brokers in one process
//!
//! Two `Broker` values in one process would share a runtime, a clock
//! and an allocator, and would let a mistake about which is which pass
//! unnoticed. Two processes cannot: the only thing they share is a
//! loopback socket, which is the thing under test.

use std::fs::File;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{Ack, Connect, Publish, RetainHandling, Subscribe, SubscriptionOptions};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};

/// How long to wait for a freshly started broker to accept.
const STARTUP: Duration = Duration::from_secs(20);

/// How long to wait for something that should happen.
const PATIENCE: Duration = Duration::from_secs(20);

/// How long to wait to be sure something has **not** happened.
///
/// Shorter than [`PATIENCE`], because it is paid in full every time
/// and because the thing it is waiting out — a bridge deciding whether
/// to send a message it already has — takes no time at all.
const SILENCE: Duration = Duration::from_secs(3);

/// The bridge rule's identifier, and so the origin its marker carries.
const BRIDGE: &str = "to-b";

/// Where the bridge reports its connectivity, on the near broker.
const STATE_TOPIC: &str = "bridges/b/state";

// ---- the broker under test ----------------------------------------------

/// A running `sdmqtt` process.
struct BrokerProcess {
    child: Child,
    address: SocketAddr,
    log: PathBuf,
}

impl BrokerProcess {
    /// Start a broker against `config`, and wait until it accepts.
    fn start(config: &Path, store: &Path, address: SocketAddr, log: PathBuf) -> Self {
        let child = Self::spawn(config, store, &log);
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

    fn spawn(config: &Path, store: &Path, log: &Path) -> Child {
        Command::new(env!("CARGO_BIN_EXE_sdmqtt"))
            .arg("serve")
            .arg("--config")
            .arg(config)
            .arg("--store-directory")
            .arg(store)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            // Emptied so that a configuration in the environment of
            // whoever is running the tests cannot reach this broker.
            .env("SDMQTT_CONFIG", "")
            .env("SDMQTT_STORAGE_DIRECTORY", "")
            .stderr(Stdio::from(
                File::create(log).expect("a log file can be created"),
            ))
            .spawn()
            .expect("the broker binary runs")
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

    /// Wait until the log says something, so a test can wait for a
    /// bridge to be up rather than for a duration somebody guessed.
    fn wait_for_log(&self, needle: &str) {
        let deadline = Instant::now() + PATIENCE;
        while Instant::now() < deadline {
            if self.logged().contains(needle) {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("the log never said {needle:?}: {}", self.logged());
    }
}

impl BrokerProcess {
    /// Kill it, running no destructor, and prove that is what
    /// happened.
    ///
    /// The same assertion `tests/rules.rs` makes and for the same
    /// reason: a clean close of the store persists everything, so a
    /// politely stopped broker proves nothing about what was written
    /// down, and a kill that had quietly become an orderly shutdown
    /// would go on passing and stop meaning anything.
    fn kill_and_prove_it(&mut self) {
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

/// What `--config` is pointed at.
///
/// The admin and ingest listeners get ports of their own rather than
/// their defaults, because two brokers in one test and several tests at
/// once would otherwise all try to bind the same two.
fn write_config(path: &Path, store: &Path, mqtt: SocketAddr, rules: &str) {
    write_config_with_storage(path, store, mqtt, "", rules);
}

/// The same, with extra lines in the `[storage]` table.
///
/// One caller: the restart test below turns off both of the writer's
/// own flush triggers, so that anything which survives a kill survived
/// because somebody waited on it rather than because a timer went off.
/// `tests/rules.rs` has the argument in full.
fn write_config_with_storage(
    path: &Path,
    store: &Path,
    mqtt: SocketAddr,
    storage: &str,
    rules: &str,
) {
    let text = format!(
        r#"
[listener]
address = "{mqtt}"

[admin]
address = "{admin}"
token = "a-secret-for-the-test"

[ingest]
address = "{ingest}"

[storage]
directory = "{store}"
sync = "immediate"
{storage}

[log]
level = "info"

{rules}
"#,
        store = store.display(),
        admin = loopback(free_port()),
        ingest = loopback(free_port()),
    );
    std::fs::write(path, text).expect("the configuration file can be written");
}

/// The near broker's bridge to the far one.
///
/// Read the two topic entries against `rules::bridge::topics`: what
/// this publishes to the remote is `from-a/sensors/#` and what it
/// subscribes to there is `from-b/#` and `back/#`. Those do not meet,
/// which is why this configuration starts at all.
fn bridge_to(remote: SocketAddr) -> String {
    format!(
        r##"
[[rules]]
id = "{BRIDGE}"
name = "Bridge to the far broker"

[rules.mqtt_bridge]
address = "mqtt://{remote}"
client_id = "bridge-a"
protocol_level = 5
keep_alive_secs = 5
state_topic = "{STATE_TOPIC}"
reconnect_min_ms = 100
reconnect_max_ms = 1000

[[rules.mqtt_bridge.topics]]
filter = "sensors/#"
direction = "out"
qos = 1
remote_prefix = "from-a/"

[[rules.mqtt_bridge.topics]]
filter = "#"
direction = "in"
qos = 1
remote_prefix = "from-b/"
local_prefix = "remote/"

[[rules.mqtt_bridge.topics]]
filter = "#"
direction = "in"
qos = 1
remote_prefix = "back/"
local_prefix = "returned/"
"##
    )
}

/// Two brokers, the near one bridged to the far one.
struct Bridged {
    near: BrokerProcess,
    far: BrokerProcess,
    /// Kept so the far broker can be stopped and started again on the
    /// same port and the same store, which is what a reconnection
    /// needs to be more than a socket reopening.
    far_config: PathBuf,
    far_store: PathBuf,
    /// The same, for the near broker, so that the restart test can
    /// kill the broker the bridge belongs to and start it again on
    /// what it left behind.
    near_config: PathBuf,
    near_store: PathBuf,
    dir: tempfile::TempDir,
}

impl Bridged {
    fn start() -> Self {
        Self::start_with_storage("")
    }

    /// The same, with extra `[storage]` lines on the **near** broker,
    /// which is the one the restart test kills.
    fn start_with_storage(storage: &str) -> Self {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let near_address = loopback(free_port());
        let far_address = loopback(free_port());

        let far_config = dir.path().join("far.toml");
        let far_store = dir.path().join("far-store");
        write_config(&far_config, &far_store, far_address, "");
        let far = BrokerProcess::start(
            &far_config,
            &far_store,
            far_address,
            dir.path().join("far.log"),
        );

        let near_config = dir.path().join("near.toml");
        let near_store = dir.path().join("near-store");
        write_config_with_storage(
            &near_config,
            &near_store,
            near_address,
            storage,
            &bridge_to(far_address),
        );
        let near = BrokerProcess::start(
            &near_config,
            &near_store,
            near_address,
            dir.path().join("near.log"),
        );

        // Not a sleep: the bridge says when it is up, and says what the
        // remote declared while it was at it.
        near.wait_for_log("the bridge is connected");
        Self {
            near,
            far,
            far_config,
            far_store,
            near_config,
            near_store,
            dir,
        }
    }

    /// Kill the far broker and leave it dead, so that the bridge has
    /// nowhere to deliver and its outbox fills.
    fn kill_far(&mut self) {
        let _ = self.far.child.kill();
        let _ = self.far.child.wait();
    }

    /// Kill the near broker — the one the bridge belongs to.
    ///
    /// **Killed, not stopped**, and the signal is asserted: a clean
    /// close of the store persists everything, so a politely stopped
    /// broker would prove nothing about what was written down.
    ///
    /// Separate from starting it again, because what happens between
    /// the two is the whole of the test: a caller that killed and
    /// restarted in one step could not put a subscriber on the far
    /// broker while this one was dead, and the message would then be
    /// carried to a broker nobody was listening on.
    fn kill_near(&mut self) {
        self.near.kill_and_prove_it();
    }

    /// Start the near broker again on the port and store it had.
    fn start_near_again(&mut self, log: &str) {
        let address = self.near.address;
        // A fresh log, so an assertion about what the new process said
        // cannot be satisfied by what the old one said.
        self.near = BrokerProcess::start(
            &self.near_config,
            &self.near_store,
            address,
            self.dir.path().join(log),
        );
    }

    /// Start the far broker again on the port and store it had.
    fn start_far_again(&mut self, log: &str) {
        let address = self.far.address;
        self.far = BrokerProcess::start(
            &self.far_config,
            &self.far_store,
            address,
            self.dir.path().join(log),
        );
    }

    /// Stop the far broker and start it again on the same port, with
    /// the same store, so the session it held for this bridge is still
    /// there when the bridge comes back.
    fn restart_far(&mut self) {
        let address = self.far.address;
        let _ = self.far.child.kill();
        let _ = self.far.child.wait();
        // A fresh log, so `wait_for_log` cannot be satisfied by what
        // the previous connection said.
        let log = self.dir.path().join("far-again.log");
        self.far = BrokerProcess::start(&self.far_config, &self.far_store, address, log);
    }
}

// ---- a client over the socket -------------------------------------------

/// One MQTT 5 client on a blocking socket.
///
/// The same shape as `tests/rules.rs`'s, and deliberately its own copy:
/// an integration test that shared a client with another integration
/// test would make a change to one of them able to break the other for
/// reasons that have nothing to do with what either is testing.
struct Client {
    stream: TcpStream,
    buf: BytesMut,
    next_id: u16,
    /// Deliveries that arrived while something else was being waited
    /// for.
    ///
    /// **A server may start publishing before it sends the SUBACK** —
    /// MQTT-v5.0 3.8.4 and 3.1.1's §3.8.4 both permit it, and this
    /// broker takes them up on it for retained messages, which go out
    /// ahead of the acknowledgement. A client that discarded whatever
    /// it saw while waiting for the SUBACK would lose exactly the
    /// retained message it had just subscribed in order to see. This
    /// test found that out by asserting a state topic and being told
    /// there was none.
    pending: std::collections::VecDeque<Publish>,
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
            next_id: 1,
            pending: std::collections::VecDeque::new(),
        };
        client.send(&Packet::Connect(Box::new(Connect {
            version: Version::V5,
            client_id: client_id.into(),
            clean_start: true,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties::default(),
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
        let packet_id = self.take_id();
        self.send(&Packet::Subscribe(Subscribe {
            packet_id,
            filters: vec![SubscriptionOptions {
                filter: filter.into(),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAlways,
            }],
            properties: Properties::default(),
        }));
        loop {
            match self.next() {
                Some(Packet::SubAck(ack)) if ack.packet_id == packet_id => {
                    assert_eq!(ack.reasons, vec![ReasonCode::GrantedQos1], "on {filter}");
                    return;
                }
                Some(Packet::Publish(publish)) => self.hold(publish),
                Some(_) => continue,
                None => panic!("the broker closed while subscribing to {filter}"),
            }
        }
    }

    /// Publish at least once and wait to be told it is safe.
    fn publish_and_wait(&mut self, topic: &str, payload: &[u8], properties: Properties) {
        let packet_id = self.take_id();
        self.send(&Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: topic.into(),
            packet_id: Some(packet_id),
            payload: Bytes::copy_from_slice(payload),
            properties,
        }));
        loop {
            match self.next() {
                Some(Packet::PubAck(ack)) if ack.packet_id == packet_id => {
                    assert!(
                        matches!(
                            ack.reason,
                            ReasonCode::Success | ReasonCode::NoMatchingSubscribers
                        ),
                        "publishing to {topic} was refused: {:?}",
                        ack.reason
                    );
                    return;
                }
                Some(Packet::Publish(publish)) => self.hold(publish),
                Some(_) => continue,
                None => panic!("the broker closed while publishing to {topic}"),
            }
        }
    }

    /// Keep a delivery that arrived while something else was awaited,
    /// acknowledging it as it goes.
    fn hold(&mut self, publish: Publish) {
        if let Some(packet_id) = publish.packet_id {
            self.send(&Packet::PubAck(Ack {
                packet_id,
                reason: ReasonCode::Success,
                properties: Properties::default(),
            }));
        }
        self.pending.push_back(publish);
    }

    /// Wait for a delivery on any subscription.
    fn delivery_within(&mut self, patience: Duration) -> Option<Publish> {
        if let Some(held) = self.pending.pop_front() {
            return Some(held);
        }
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

    fn take_id(&mut self) -> u16 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        id
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

/// The value of one user property, if the message carries it.
fn property(publish: &Publish, name: &str) -> Option<String> {
    publish
        .properties
        .user_properties
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
}

// ---- the tests ----------------------------------------------------------

/// The whole of it: two brokers, real sockets, messages both ways, and
/// the topic rewritten on each crossing.
#[test]
fn two_brokers_move_messages_in_both_directions() {
    let bridged = Bridged::start();

    let mut far_watcher = Client::connect(bridged.far.address, "far-watcher");
    far_watcher.subscribe("from-a/#");
    let mut near_watcher = Client::connect(bridged.near.address, "near-watcher");
    near_watcher.subscribe("remote/#");

    // Near to far. The outbound entry's prefix is what makes the topic
    // change on the way.
    let mut near_publisher = Client::connect(bridged.near.address, "near-publisher");
    near_publisher.publish_and_wait(
        "sensors/kitchen/temp",
        b"21.5",
        Properties {
            user_properties: vec![("unit".to_owned(), "celsius".to_owned())],
            ..Properties::default()
        },
    );

    let arrived = far_watcher
        .delivery_within(PATIENCE)
        .expect("the message crossed to the far broker");
    assert_eq!(arrived.topic, "from-a/sensors/kitchen/temp");
    assert_eq!(&arrived.payload[..], b"21.5");
    assert_eq!(
        property(&arrived, "unit").as_deref(),
        Some("celsius"),
        "an application's own properties travel with the message"
    );
    assert_eq!(
        property(&arrived, "sd-bridge-origin").as_deref(),
        Some(BRIDGE),
        "the bridge marks what it carries"
    );
    assert_eq!(
        property(&arrived, "sd-hops").as_deref(),
        Some("1"),
        "and counts the hop"
    );

    // Far to near, which is the direction that needs a subscription on
    // the remote and an acknowledgement read back.
    let mut far_publisher = Client::connect(bridged.far.address, "far-publisher");
    far_publisher.publish_and_wait("from-b/alarm", b"smoke", Properties::default());

    let came_back = near_watcher
        .delivery_within(PATIENCE)
        .expect("the message crossed to the near broker");
    assert_eq!(came_back.topic, "remote/alarm");
    assert_eq!(&came_back.payload[..], b"smoke");
    assert_eq!(
        property(&came_back, "sd-bridge-origin").as_deref(),
        Some(BRIDGE)
    );
}

/// Section 9.2's state topic, with the will supplying the offline
/// value.
///
/// Retained, so somebody who subscribes after the fact still learns
/// what happened — which is what this test does.
#[test]
fn the_bridge_reports_that_it_is_connected() {
    let bridged = Bridged::start();

    let mut watcher = Client::connect(bridged.near.address, "state-watcher");
    watcher.subscribe(STATE_TOPIC);
    let state = watcher
        .delivery_within(PATIENCE)
        .unwrap_or_else(|| panic!("a retained state message: {}", bridged.near.logged()));
    assert_eq!(state.topic, STATE_TOPIC);
    assert_eq!(&state.payload[..], b"online");
    assert!(
        state.retain,
        "a state topic that is not retained tells only whoever was already listening"
    );
}

/// The marker, end to end, in the one shape a configuration check
/// cannot see: the far broker republishes what it was sent, onto a
/// topic nothing in this bridge's configuration relates to, and it
/// comes back.
///
/// **This is the pair the plan singles out.** The outbound half stamps
/// the message; the inbound half reads the stamp and refuses it. Either
/// half alone leaves the loop open, and with either half removed the
/// first assertion below fails.
///
/// The second half of the test is what stops it being vacuous: an
/// unmarked message on the very same path arrives. So "nothing came
/// back" is the marker working rather than the path being broken.
#[test]
fn a_message_this_bridge_sent_is_not_taken_back() {
    let bridged = Bridged::start();

    let mut returned = Client::connect(bridged.near.address, "returned-watcher");
    returned.subscribe("returned/#");

    // Stands in for a rule on the far broker: whatever arrives under
    // `from-a/`, republish under `back/`, properties and all. That is
    // the loop with a broker in the middle that this end does not
    // configure and cannot analyse.
    let mut mirror = Client::connect(bridged.far.address, "far-mirror");
    mirror.subscribe("from-a/#");

    let mut near_publisher = Client::connect(bridged.near.address, "near-publisher");
    near_publisher.publish_and_wait("sensors/loop/temp", b"round we go", Properties::default());

    let crossed = mirror
        .delivery_within(PATIENCE)
        .expect("the message crossed");
    assert_eq!(
        property(&crossed, "sd-bridge-origin").as_deref(),
        Some(BRIDGE),
        "the outbound half of the pair: without this there is nothing to find"
    );
    mirror.publish_and_wait(
        "back/sensors/loop/temp",
        &crossed.payload,
        Properties {
            user_properties: crossed.properties.user_properties.clone(),
            ..Properties::default()
        },
    );

    assert!(
        returned.delivery_within(SILENCE).is_none(),
        "the inbound half of the pair: this bridge sent that message, and taking it back \
         is the loop section 9.2's marker exists to stop"
    );

    // And the same path, with an unmarked message, does deliver — so
    // the silence above is the marker and not a broken bridge.
    let mut far_publisher = Client::connect(bridged.far.address, "far-publisher");
    far_publisher.publish_and_wait("back/fresh", b"never been here", Properties::default());
    let fresh = returned
        .delivery_within(PATIENCE)
        .expect("an unmarked message on the same path arrives");
    assert_eq!(fresh.topic, "returned/fresh");
    assert_eq!(&fresh.payload[..], b"never been here");
}

/// Item 1, end to end: the far broker goes away and comes back, and
/// the bridge reconnects to it on its own.
///
/// The far broker keeps its port and its store, so the session it held
/// for this bridge is still there — which means the CONNACK says
/// Session Present, the bridge does **not** resubscribe, and the
/// inbound direction has to work off the subscriptions the far broker
/// restored. A bridge that resubscribed regardless would pass this
/// test; one that relied on resubscribing and got no session would
/// hang here for ever, which is the failure item 8 is about.
#[test]
fn the_bridge_reconnects_when_the_remote_comes_back() {
    let mut bridged = Bridged::start();

    let mut far_watcher = Client::connect(bridged.far.address, "far-watcher");
    far_watcher.subscribe("from-a/#");
    let mut near_publisher = Client::connect(bridged.near.address, "near-publisher");
    near_publisher.publish_and_wait("sensors/before/temp", b"before", Properties::default());
    let before = far_watcher
        .delivery_within(PATIENCE)
        .expect("the bridge works before the restart");
    assert_eq!(before.topic, "from-a/sensors/before/temp");
    drop(far_watcher);

    bridged.restart_far();
    // The reconnection is the bridge's own: nothing here pokes it.
    bridged.near.wait_for_log("this connection is over");
    bridged.far.wait_for_log("listening");

    let mut far_watcher = Client::connect(bridged.far.address, "far-watcher-again");
    far_watcher.subscribe("from-a/#");
    // Outbound, which needs a live connection.
    let deadline = Instant::now() + PATIENCE;
    let after = loop {
        assert!(
            Instant::now() < deadline,
            "the bridge never reconnected: {}",
            bridged.near.logged()
        );
        near_publisher.publish_and_wait("sensors/after/temp", b"after", Properties::default());
        // **A resent "before" is passed over, not failed on.** The far
        // broker went away right after it took "before", and if its
        // acknowledgement never reached the bridge the outbox still
        // holds it and sends it again on the new connection: at least
        // once, which permits a duplicate and forbids a loss. The new
        // watcher subscribed after the reconnect and sees that resend
        // first.
        if let Some(publish) = far_watcher.delivery_within(SILENCE) {
            if publish.topic == "from-a/sensors/before/temp" {
                continue;
            }
            break publish;
        }
    };
    assert_eq!(after.topic, "from-a/sensors/after/temp");
    assert_eq!(&after.payload[..], b"after");

    // And inbound, which needs the subscriptions to be there — either
    // because the far broker restored them or because the bridge
    // noticed it had no session and sent them again.
    let mut near_watcher = Client::connect(bridged.near.address, "near-watcher");
    near_watcher.subscribe("remote/#");
    let mut far_publisher = Client::connect(bridged.far.address, "far-publisher");
    far_publisher.publish_and_wait("from-b/alarm", b"still here", Properties::default());
    let came_back = near_watcher.delivery_within(PATIENCE).unwrap_or_else(|| {
        panic!(
            "inbound is dead after the reconnection: {}",
            bridged.near.logged()
        )
    });
    assert_eq!(came_back.topic, "remote/alarm");
}

/// The first line of defence, in the real binary: a bridge whose
/// inbound and outbound filters intersect after rewriting does not
/// start.
///
/// The filters overlap **only through a wildcard**. They are not equal
/// and do not look alike, and a string comparison would let this
/// configuration through and find out at run time, on somebody else's
/// broker.
#[test]
fn a_bridge_that_loops_through_a_wildcard_does_not_start() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let config = dir.path().join("looping.toml");
    let store = dir.path().join("store");
    let log = dir.path().join("broker.log");
    write_config(
        &config,
        &store,
        loopback(free_port()),
        r##"
[[rules]]
id = "loops"
name = "A bridge that loops"

[rules.mqtt_bridge]
address = "mqtt://127.0.0.1:1883"
client_id = "bridge-a"

[[rules.mqtt_bridge.topics]]
filter = "sensors/+/temp"
direction = "out"

[[rules.mqtt_bridge.topics]]
filter = "sensors/kitchen/#"
direction = "in"
"##,
    );

    let mut child = BrokerProcess::spawn(&config, &store, &log);
    let status = child.wait().expect("the broker exits");
    assert!(
        !status.success(),
        "a looping bridge must stop the broker starting"
    );

    let logged = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        logged.contains("mqtt_bridge loops"),
        "the refusal must name itself: {logged}"
    );
    assert!(
        logged.contains("sensors/kitchen/temp"),
        "and must name a topic both filters match, so an operator can check it by hand: \
         {logged}"
    );
}

/// The second refusal section 9.2 asks for: a bridge republishing its
/// own liveness is a loop with a heartbeat.
#[test]
fn a_bridge_that_would_forward_its_own_state_topic_does_not_start() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let config = dir.path().join("heartbeat.toml");
    let store = dir.path().join("store");
    let log = dir.path().join("broker.log");
    write_config(
        &config,
        &store,
        loopback(free_port()),
        r##"
[[rules]]
id = "heartbeat"
name = "A bridge that forwards its own liveness"

[rules.mqtt_bridge]
address = "mqtt://127.0.0.1:1883"
client_id = "bridge-a"
state_topic = "sensors/bridge/state"

[[rules.mqtt_bridge.topics]]
filter = "sensors/#"
direction = "out"
"##,
    );

    let mut child = BrokerProcess::spawn(&config, &store, &log);
    let status = child.wait().expect("the broker exits");
    assert!(!status.success());
    let logged = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        logged.contains("loop with a heartbeat"),
        "the refusal must say what it is about: {logged}"
    );
}

/// Plain text to anywhere but this machine is refused, for the reason
/// `http_out` refuses a plain-text endpoint and with more at stake: a
/// bridge sends its password in the connect and then everything it
/// carries.
#[test]
fn a_plain_text_bridge_to_another_host_does_not_start() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let config = dir.path().join("cleartext.toml");
    let store = dir.path().join("store");
    let log = dir.path().join("broker.log");
    write_config(
        &config,
        &store,
        loopback(free_port()),
        r##"
[[rules]]
id = "cleartext"
name = "A bridge in the clear"

[rules.mqtt_bridge]
address = "mqtt://broker.example.com:1883"
client_id = "bridge-a"

[[rules.mqtt_bridge.topics]]
filter = "sensors/#"
direction = "out"
"##,
    );

    let mut child = BrokerProcess::spawn(&config, &store, &log);
    let status = child.wait().expect("the broker exits");
    assert!(!status.success());
    let logged = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        logged.contains("use mqtts://"),
        "the refusal must say what to write instead: {logged}"
    );
}

// ---- against a broker we did not write ----------------------------------

/// The same bridge, against the conformance suite's **own** broker.
///
/// The plan asks for this and says why: "A bridge that only works
/// against itself has only proved the two halves agree." Two `sdmqtt`
/// processes share every misunderstanding this project has. Eclipse
/// Paho's interoperability suite ships a pure-Python MQTT 5 broker
/// beside its tests, and that one does not.
///
/// **It is a deliberately strict broker**, which is the point. Its
/// defaults, from `mqtt/brokers/start.py`, are a receive maximum of
/// **2**, a maximum packet size of **256**, a topic alias maximum of
/// **2** and a server keep-alive it declares in the CONNACK. Every one
/// of those is a limit this bridge has to read out of the CONNACK and
/// obey, and every one of them is a disconnect if it does not. A bridge
/// that ignored them works against a permissive broker and fails
/// against this one.
///
/// # Why it is ignored by default
///
/// The suite is cloned by `conformance/run.sh` into `conformance/.work`
/// and is not in this repository. A test that needed it would fail on a
/// clean checkout, and one that skipped quietly when it was absent
/// would be a test that passes by doing nothing — which is the failure
/// convention 5 is about. So it is explicit:
///
/// ```text
/// services/broker/conformance/run.sh        # clones the suite
/// cargo test -p sdmqtt --test bridge -- --ignored
/// ```
#[test]
#[ignore = "needs the conformance suite cloned; conformance/run.sh does that"]
fn bridged_to_the_conformance_suites_own_broker() {
    let suite = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/.work/paho.mqtt.testing/interoperability");
    assert!(
        suite.join("startbroker.py").is_file(),
        "the suite is not cloned at {}; run conformance/run.sh first",
        suite.display()
    );

    let dir = tempfile::tempdir().expect("a temporary directory");
    let far_port = free_port();
    let far_config = dir.path().join("paho.conf");
    std::fs::write(
        &far_config,
        format!(
            "listener {far_port} 127.0.0.1
"
        ),
    )
    .expect("the suite's broker takes a config file");

    let mut far = Command::new("python3")
        .arg("startbroker.py")
        .arg("-c")
        .arg(&far_config)
        .current_dir(&suite)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            File::create(dir.path().join("paho.log")).expect("a log file"),
        ))
        .spawn()
        .expect("python3 runs the suite's broker");
    let far_address = loopback(far_port);
    let deadline = Instant::now() + STARTUP;
    while Instant::now() < deadline
        && TcpStream::connect_timeout(&far_address, Duration::from_millis(200)).is_err()
    {
        std::thread::sleep(Duration::from_millis(50));
    }

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let near_config = dir.path().join("near.toml");
        let near_store = dir.path().join("near-store");
        let near_address = loopback(free_port());
        write_config(
            &near_config,
            &near_store,
            near_address,
            &bridge_to(far_address),
        );
        let near = BrokerProcess::start(
            &near_config,
            &near_store,
            near_address,
            dir.path().join("near.log"),
        );
        near.wait_for_log("the bridge is connected");

        // The limits this broker declares are not ours, and the line
        // above carries them, so a reader of a failing run can see
        // what was negotiated rather than guess.
        let logged = near.logged();
        assert!(
            logged.contains("send_quota=2"),
            "the suite's broker declares a receive maximum of 2 and this bridge must \
             take it: {logged}"
        );

        let mut far_watcher = Client::connect(far_address, "far-watcher");
        far_watcher.subscribe("from-a/#");
        let mut near_watcher = Client::connect(near_address, "near-watcher");
        near_watcher.subscribe("remote/#");

        // Four messages, which is twice the window this broker allows,
        // so the flow control has to give a slot back before the third
        // can go. A bridge that ignored the declared receive maximum
        // would send all four at once and be disconnected for it.
        let mut near_publisher = Client::connect(near_address, "near-publisher");
        for index in 0..4u8 {
            near_publisher.publish_and_wait(
                &format!("sensors/{index}/temp"),
                &[b'0' + index],
                Properties::default(),
            );
        }
        let mut arrived: Vec<String> = Vec::new();
        for _ in 0..4 {
            let publish = far_watcher.delivery_within(PATIENCE).unwrap_or_else(|| {
                panic!("only {} of four crossed: {}", arrived.len(), near.logged())
            });
            arrived.push(publish.topic.clone());
        }
        arrived.sort();
        assert_eq!(
            arrived,
            vec![
                "from-a/sensors/0/temp",
                "from-a/sensors/1/temp",
                "from-a/sensors/2/temp",
                "from-a/sensors/3/temp",
            ]
        );

        let mut far_publisher = Client::connect(far_address, "far-publisher");
        far_publisher.publish_and_wait("from-b/alarm", b"smoke", Properties::default());
        let came_back = near_watcher
            .delivery_within(PATIENCE)
            .unwrap_or_else(|| panic!("nothing came back: {}", near.logged()));
        assert_eq!(came_back.topic, "remote/alarm");
        assert_eq!(&came_back.payload[..], b"smoke");
    }));

    let _ = far.kill();
    let _ = far.wait();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

// ---- task 4: the bridge comes back ---------------------------------------

/// **A bridge reconnects after the broker is killed, and the message
/// that was in flight arrives.** Task 4.
///
/// The sequence is the one that makes it a durability test rather than
/// a reconnection test:
///
/// 1. The bridge is up, so there is nothing clever about the first
///    connection.
/// 2. The far broker is killed, so the bridge has nowhere to deliver.
/// 3. A client publishes on the near broker, on the bridge's outbound
///    filter, at least once. The acknowledgement it gets back is
///    section 7.3's promise that the message is durable — and the
///    bridge cannot have sent it, because there is nothing to send it
///    to. **This is the message in flight.**
/// 4. The near broker is killed with `SIGKILL`, which runs no
///    destructor: no final flush, no clean close of the store. The
///    exit status is checked, because a kill that had quietly become an
///    orderly shutdown would go on passing and stop meaning anything.
/// 5. Both brokers come back on the ports and stores they had.
/// 6. The message arrives at the far broker.
///
/// The writer is told never to flush on its own — an hour between
/// interval flushes and a billion records before a count flush — so
/// nothing that survives here survived because a timer went off. What
/// is left is a batch somebody waited on, and the waiter is the
/// publishing client's own acknowledgement.
///
/// # And it asserts what the in-flight window's deferral costs
///
/// `rules::bridge::supervisor` rules that the window stays in memory,
/// and that the first connection of a process therefore asks for a
/// clean start whatever `mqtt_bridge.clean_start` says. That decision
/// is what makes the outbox the only thing carrying this message
/// across the restart, and it is asserted here rather than left as
/// prose: the restarted broker must say it is doing it, because what
/// it throws away — whatever the remote was holding for us — is a loss
/// this broker cannot see and will never count.
#[test]
fn a_bridge_reconnects_and_the_message_in_flight_arrives() {
    let mut bridged =
        Bridged::start_with_storage("flush_interval_ms = 3600000\nflush_records = 1000000000");
    let near_address = bridged.near.address;
    let far_address = bridged.far.address;

    // The far broker goes, so the bridge has nowhere to put anything.
    bridged.kill_far();
    bridged.near.wait_for_log("this connection is over");

    // In flight: accepted by the near broker, promised durable to its
    // publisher, and not yet carried.
    let mut publisher = Client::connect(near_address, "near-publisher");
    publisher.publish_and_wait("sensors/9/temp", b"in flight", Properties::default());
    drop(publisher);

    // **Killed, not stopped**, and killed **first**. A near broker
    // still running when the far one came back would reconnect and
    // deliver the message at once, to a far broker nobody is
    // subscribed on, and the restart would then be carrying nothing.
    // The first draft of this test had the two the other way round and
    // passed on whichever won the race.
    bridged.kill_near();

    bridged.start_far_again("far-again.log");
    let mut watcher = Client::connect(far_address, "far-watcher");
    watcher.subscribe("from-a/#");

    // And only now the broker that was killed, so that what crosses
    // crosses after the restart, into a subscription already there.
    bridged.start_near_again("near-again.log");

    let arrived = watcher
        .delivery_within(PATIENCE)
        .unwrap_or_else(|| panic!("the message never crossed: {}", bridged.near.logged()));
    assert_eq!(arrived.topic, "from-a/sensors/9/temp");
    assert_eq!(&arrived.payload[..], b"in flight");

    let logged = bridged.near.logged();
    assert!(
        logged.contains("this is the first connection of this process"),
        "the restarted bridge must say it is asking for a clean start, because what \
         that discards is a loss nothing here counts: {logged}"
    );
}
