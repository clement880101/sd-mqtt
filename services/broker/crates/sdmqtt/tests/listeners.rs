//! Several listeners on one broker, through the binary.
//!
//! One process, two listeners: a session subscribed on the default one
//! is reached from the second, which proves they share a broker, and
//! the second keeps a policy of its own, which proves they do not share
//! everything. Then the admin API describes both.
//!
//! The harness here is deliberately small: a configuration file, the
//! binary, and a client that speaks just enough MQTT 5 to connect,
//! subscribe, publish and read. `tests/cluster.rs` has the full one;
//! the day a third test wants it, the two move to a shared module.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{Connect, Publish, Subscribe, SubscriptionOptions};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};

const STARTUP: Duration = Duration::from_secs(30);
const REPLY: Duration = Duration::from_secs(10);
const TOKEN: &str = "an-admin-token-for-the-test";

/// A port nobody is listening on right now.
fn free_port() -> SocketAddr {
    let probe = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
    probe.local_addr().expect("an address")
}

/// The broker, started from a configuration file, killed on drop.
struct Broker {
    child: Child,
    log: std::path::PathBuf,
}

impl Broker {
    fn start(directory: &Path, config: &str) -> Self {
        let path = directory.join("sdmqtt.toml");
        std::fs::write(&path, config).expect("the configuration is written");
        let log = directory.join("broker.log");
        let child = Command::new(env!("CARGO_BIN_EXE_sdmqtt"))
            .arg("serve")
            .arg("--config")
            .arg(&path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(
                std::fs::File::create(&log).expect("a log file"),
            ))
            .env("SDMQTT_CONFIG", "")
            .env("SDMQTT_STORAGE_DIRECTORY", "")
            .spawn()
            .expect("the binary starts");
        Self { child, log }
    }

    fn logged(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Wait until `address` answers, or say what the broker said.
    fn wait_for(&mut self, address: SocketAddr) {
        let deadline = Instant::now() + STARTUP;
        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.child.try_wait() {
                panic!("the broker exited with {status}: {}", self.logged());
            }
            if TcpStream::connect_timeout(&address, Duration::from_millis(200)).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("{address} never answered: {}", self.logged());
    }

    /// `GET path` on the admin listener, the body.
    fn get(&self, admin: SocketAddr, path: &str) -> String {
        let mut socket = TcpStream::connect_timeout(&admin, REPLY).expect("the admin accepts");
        socket.set_read_timeout(Some(REPLY)).expect("a deadline");
        write!(
            socket,
            "GET {path} HTTP/1.1\r\nHost: {admin}\r\nAuthorization: Bearer {TOKEN}\r\n\
             Connection: close\r\n\r\n"
        )
        .expect("the request is written");
        let mut raw = Vec::new();
        socket.read_to_end(&mut raw).expect("the response ends");
        let text = String::from_utf8_lossy(&raw).into_owned();
        text.split_once("\r\n\r\n")
            .map(|(_, body)| body.to_owned())
            .unwrap_or_else(|| panic!("no body in {text:?}"))
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Just enough of a client.
struct Client {
    stream: TcpStream,
    buf: BytesMut,
}

impl Client {
    fn connect(address: SocketAddr, client_id: &str) -> Self {
        let stream = TcpStream::connect_timeout(&address, REPLY).expect("the listener accepts");
        stream.set_read_timeout(Some(REPLY)).expect("a deadline");
        let mut client = Self {
            stream,
            buf: BytesMut::new(),
        };
        client.send(&Packet::Connect(Box::new(Connect {
            version: Version::V5,
            client_id: client_id.to_owned(),
            clean_start: true,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties::default(),
        })));
        match client.next() {
            Some(Packet::ConnAck(ack)) => assert_eq!(ack.reason, ReasonCode::Success),
            other => panic!("expected a connack, got {other:?}"),
        }
        client
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

    fn subscribe(&mut self, filter: &str) -> Vec<ReasonCode> {
        self.send(&Packet::Subscribe(Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: filter.to_owned(),
                qos: QoS::ExactlyOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: mqtt_codec::packet::RetainHandling::DoNotSend,
            }],
            properties: Properties::default(),
        }));
        match self.next() {
            Some(Packet::SubAck(ack)) => ack.reasons,
            other => panic!("expected a suback, got {other:?}"),
        }
    }

    fn publish(&mut self, topic: &str, payload: &str, qos: QoS) {
        self.send(&Packet::Publish(Publish {
            dup: false,
            qos,
            retain: false,
            topic: topic.to_owned(),
            packet_id: (qos != QoS::AtMostOnce).then_some(7),
            payload: Bytes::from(payload.to_owned()),
            properties: Properties::default(),
        }));
    }
}

/// A second listener shares the broker and keeps its own policy.
#[test]
fn a_second_listener_shares_the_broker_and_keeps_its_own_policy() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let (mqtt, capped, admin) = (free_port(), free_port(), free_port());
    let mut broker = Broker::start(
        directory.path(),
        &format!(
            r#"
[listener]
address = "{mqtt}"

[[listeners]]
name = "capped"
address = "{capped}"
max_qos = 0

[admin]
address = "{admin}"
token = "{TOKEN}"

[log]
level = "info"
"#
        ),
    );
    broker.wait_for(admin);
    broker.wait_for(mqtt);
    broker.wait_for(capped);

    // Subscribed on one listener, reached from the other: one broker.
    let mut rover = Client::connect(mqtt, "rover");
    assert_eq!(
        rover.subscribe("fleet/+/telemetry"),
        vec![ReasonCode::GrantedQos2]
    );
    let mut probe = Client::connect(capped, "probe");
    probe.publish("fleet/1/telemetry", "across", QoS::AtMostOnce);
    match rover.next() {
        Some(Packet::Publish(publish)) => assert_eq!(&publish.payload[..], b"across"),
        other => panic!("the publish on the second listener should reach the first: {other:?}"),
    }

    // And its own policy: a QoS 1 publish on the capped listener is a
    // protocol error there, while the default listener granted two.
    probe.publish("fleet/1/telemetry", "too-high", QoS::AtLeastOnce);
    match probe.next() {
        Some(Packet::Disconnect(gone)) => assert_eq!(gone.reason, ReasonCode::QosNotSupported),
        other => panic!("expected a disconnect with 0x9B, got {other:?}"),
    }

    // The admin API describes both, with the policy on each.
    let body = broker.get(admin, "/api/v1/listeners");
    let document: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|why| panic!("{body:?}: {why}"));
    let rows = document["items"].as_array().expect("items");
    let mqtt_rows: Vec<&serde_json::Value> = rows
        .iter()
        .filter(|row| row["protocol"] == "mqtt")
        .collect();
    assert_eq!(mqtt_rows.len(), 2, "{body}");
    let by_name = |name: &str| {
        mqtt_rows
            .iter()
            .find(|row| row["name"] == name)
            .unwrap_or_else(|| panic!("no listener named {name} in {body}"))
    };
    assert_eq!(by_name("mqtt")["max_qos"], 2);
    assert_eq!(by_name("mqtt")["address"], mqtt.to_string());
    assert_eq!(by_name("capped")["max_qos"], 0);
    assert_eq!(by_name("capped")["address"], capped.to_string());
    assert_eq!(by_name("capped")["durability"], "node");
}

/// Two listeners on one address never start: the refusal names both.
#[test]
fn two_listeners_on_one_address_do_not_start() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let port = free_port();
    let mut broker = Broker::start(
        directory.path(),
        &format!(
            r#"
[listener]
address = "{port}"

[[listeners]]
name = "again"
address = "{port}"
"#
        ),
    );
    let deadline = Instant::now() + STARTUP;
    let status = loop {
        if let Ok(Some(status)) = broker.child.try_wait() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "the broker kept running: {}",
            broker.logged()
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(!status.success());
    let said = broker.logged();
    assert!(
        said.contains("listeners[0].address") && said.contains("listener"),
        "the refusal names both listeners: {said}"
    );
}
