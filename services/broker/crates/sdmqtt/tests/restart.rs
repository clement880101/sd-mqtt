//! Milestone two's exit criterion: a broker restart preserves durable
//! sessions, their subscriptions and their queued messages.
//!
//! Over a real socket, against the real binary, which is the only
//! shape that can prove it. Everything below the shell is tested where
//! it lives, and none of those tests can see a process die.
//!
//! # The broker is killed, not shut down
//!
//! **A clean close of the store persists everything, flushed or not.**
//! That was found and pinned while the storage crate was written, and
//! it makes a politely restarted broker worthless as evidence: a
//! broker that never flushed at all would pass such a test, because
//! dropping the writer flushes and dropping the store commits. So the
//! broker here is killed with `SIGKILL`, which runs no destructor,
//! closes no store and flushes nothing. The test asserts that is how
//! it died — a `kill` that had become a polite shutdown would
//! otherwise quietly stop testing anything.
//!
//! # The writer is configured never to flush on its own
//!
//! An hour between interval flushes and a billion records before a
//! count flush, so the only thing that can put a record on the device
//! is section 7.3's third trigger: a batch carrying an acknowledgement
//! the client must not be lied to about. The message is published, the
//! acknowledgement comes back, and the broker is killed milliseconds
//! later — inside the flush interval by three orders of magnitude,
//! which the test measures rather than assumes.
//!
//! That is the sharp form of the criterion. If the message survives,
//! it survived because the acknowledgement waited for it; there is no
//! other flush it could have caught.
//!
//! # What the negative half is for
//!
//! A broker that restored *everything* it had ever seen would pass the
//! positive half. So the same run holds a clean-start session
//! subscribed to the same filter, which must not come back and must
//! not be given anything, and both halves are asserted against one
//! restarted broker. The positive half is what says the negative half
//! is not simply a broker that restored nothing.

use std::fs::File;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{
    Ack, Connect, Disconnect, Publish, RetainHandling, Subscribe, SubscriptionOptions,
};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};

/// The filter the durable session holds across the restart.
const FILTER: &str = "restart/probe";

/// The filter the clean-start session holds, and must not hold after
/// the restart.
///
/// Its own, rather than the durable session's, so that the broker can
/// be asked whether anybody still subscribes to it. A publication to a
/// filter nobody holds is acknowledged with
/// [`ReasonCode::NoMatchingSubscribers`], which turns "the
/// subscription did not come back" from an absence nothing can observe
/// into an answer the broker gives.
const EPHEMERAL_FILTER: &str = "restart/ephemeral";

/// What is published while it is away.
const PAYLOAD: &[u8] = b"published to a broker that then died";

/// What is published to the restarted broker, to see whether the
/// restored subscription is in its matching trie.
const AFTERWARDS: &[u8] = b"published to the broker that came back";

/// How long a session outlives its connection.
///
/// An hour on the wall clock, which is the scale a stored expiry is
/// judged on: section 7.5 has restore comparing it against the time
/// the restarted process reads, so a session must be given long enough
/// to outlive a restart that takes a second.
const EXPIRY: u32 = 3_600;

/// How long the writer is told to wait before flushing on its own.
///
/// An hour. Nothing in a test that finishes in seconds can reach it,
/// which is the point: the only flush available is the one a durable
/// acknowledgement forces.
const NEVER: Duration = Duration::from_secs(3_600);

/// How long to wait for a freshly started broker to accept.
///
/// Generous, because this runs on shared CI hardware and the cost of
/// being wrong is a flaky test of the exit criterion, which is worse
/// than no test at all.
const STARTUP: Duration = Duration::from_secs(20);

/// How long to wait for a packet that should arrive.
const PATIENCE: Duration = Duration::from_secs(10);

/// How long to wait for a packet that should not arrive.
///
/// Shorter, because it is paid in full on every run. It is long enough
/// to be evidence only because the same restarted broker delivers the
/// durable session's message first, which is what says deliveries are
/// flowing at all.
const SILENCE: Duration = Duration::from_secs(2);

// ---- the broker under test ----------------------------------------------

/// A running `sdmqtt` process.
struct BrokerProcess {
    child: Child,
    address: SocketAddr,
    log: PathBuf,
}

impl BrokerProcess {
    /// Start a broker whose store is `directory`, or none if that is
    /// `None`, and wait until it accepts connections.
    ///
    /// The port is one the operating system has just handed back, so
    /// two of these in one test run do not collide. A port taken by
    /// somebody else between the probe and the bind is a failure to
    /// start, and is retried rather than reported: the alternative is
    /// a test that fails for a reason that has nothing to do with what
    /// it asserts.
    fn start(directory: Option<&Path>, log: PathBuf) -> Self {
        let mut last = String::new();
        for _ in 0..5 {
            let port = free_port();
            let address: SocketAddr = format!("127.0.0.1:{port}").parse().expect("an address");
            let mut command = Command::new(env!("CARGO_BIN_EXE_sdmqtt"));
            command
                .arg("serve")
                .arg("--address")
                .arg(address.to_string())
                // Its own, and not the default. Since milestone 3
                // the admin listener is started with the broker, so
                // two brokers on one host collide on it exactly as
                // they would on the MQTT port — and two of these run
                // at once. Nothing here reads it; `tests/admin.rs`
                // is where it is asserted on.
                .arg("--admin-address")
                .arg(format!("127.0.0.1:{}", free_port()))
                .arg("--log-level")
                .arg("info")
                // The writer flushes only when something must not be
                // lost. See the note at the top of this file.
                .env(
                    "SDMQTT_STORAGE_FLUSH_INTERVAL_MS",
                    NEVER.as_millis().to_string(),
                )
                .env("SDMQTT_STORAGE_FLUSH_RECORDS", "1000000000")
                .env("SDMQTT_STORAGE_SYNC", "immediate")
                // Emptied rather than left alone, so that a
                // configuration file in the environment of whoever is
                // running the tests cannot reach this broker. Both are
                // documented as taking an empty value to mean absence.
                .env("SDMQTT_CONFIG", "")
                .env("SDMQTT_STORAGE_DIRECTORY", "")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::from(
                    File::create(&log).expect("a log file can be created"),
                ));
            if let Some(directory) = directory {
                command.arg("--store-directory").arg(directory);
            }
            let child = command.spawn().expect("the broker binary runs");
            let mut broker = Self {
                child,
                address,
                log: log.clone(),
            };
            match broker.wait_until_listening() {
                Ok(()) => return broker,
                Err(why) => {
                    let _ = broker.child.kill();
                    let _ = broker.child.wait();
                    last = why;
                }
            }
        }
        panic!("the broker never started: {last}");
    }

    /// Connect to it until it answers, or say why it never did.
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

    /// What it wrote to standard error, for a failure message.
    fn logged(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Kill it, running no destructor in it, and prove that is what
    /// happened.
    ///
    /// `Child::kill` is `SIGKILL` on unix, which cannot be caught,
    /// blocked or handled: no `Drop`, no final flush, no clean close
    /// of the store. The exit status is checked because a test that
    /// believed it was killing the process while something turned it
    /// into an orderly shutdown would go on passing and stop meaning
    /// anything — a clean close persists everything, flushed or not.
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
    /// Never leave one behind, however the test ended.
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

// ---- a client over the socket -------------------------------------------

/// One MQTT 5 client on a blocking socket.
struct Client {
    stream: TcpStream,
    buf: BytesMut,
}

impl Client {
    /// Connect, and report whether the broker said the session was
    /// already there.
    fn connect(
        address: SocketAddr,
        client_id: &str,
        clean_start: bool,
        expiry: u32,
    ) -> (Self, bool) {
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
                (client, ack.session_present)
            }
            other => panic!("expected a connect acknowledgement, got {other:?}"),
        }
    }

    /// Subscribe at least once, and wait for the grant.
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
            Some(Packet::SubAck(ack)) => assert_eq!(ack.reasons, vec![ReasonCode::GrantedQos1]),
            other => panic!("expected a subscribe acknowledgement, got {other:?}"),
        }
    }

    /// Publish at least once, wait to be told it is safe, and report
    /// what the broker said.
    ///
    /// **This acknowledgement is the promise the whole milestone is
    /// about.** Section 7.3 sends it only after the commit is durable,
    /// so a message acknowledged here and missing after a restart
    /// means the publisher was lied to.
    ///
    /// The reason code is returned rather than asserted, because it
    /// also answers a question no other packet does: whether anybody
    /// at all subscribes to the topic.
    fn publish_and_wait(&mut self, topic: &str, payload: &'static [u8]) -> ReasonCode {
        // Reused rather than counted up. Each publication here is
        // acknowledged before the next is sent, so the identifier is
        // free again every time, and a fixed one makes a mismatch
        // obvious.
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
                ack.reason
            }
            other => panic!("expected a publish acknowledgement, got {other:?}"),
        }
    }

    /// Publish to `topic` until the broker says nobody subscribes to
    /// it.
    ///
    /// **This is how the control session's ending is waited for
    /// rather than slept through.** A session whose expiry interval is
    /// zero ends with its connection, but the sweep that ends it runs
    /// in `Broker::tick`, once a second, and until it has run the
    /// session is still there holding its subscription — legitimately,
    /// and restore is right to bring back a session whose deadline has
    /// not yet passed. So the test waits for the answer to change
    /// instead of guessing how long that takes.
    fn publish_until_nobody_wants_it(&mut self, topic: &str) {
        let deadline = Instant::now() + PATIENCE;
        while Instant::now() < deadline {
            if self.publish_and_wait(topic, b"probing for the control session")
                == ReasonCode::NoMatchingSubscribers
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("the clean-start session still held its subscription after {PATIENCE:?}");
    }

    /// Disconnect and wait for the broker to close the socket.
    ///
    /// Waiting matters. The session is detached when the broker
    /// processes this, and the test publishes next: a race here would
    /// sometimes dispatch the message to a session still thought to be
    /// online, which is a different path from the queue this is about.
    /// The far end closing is the broker saying it has finished with
    /// the connection.
    ///
    /// The session itself survives, because [MQTT-3.1.2-23] and
    /// section 4.1 keep a session whose expiry interval is not zero
    /// over a normal disconnection.
    fn disconnect(mut self) {
        self.send(&Packet::Disconnect(Disconnect {
            reason: ReasonCode::Success,
            properties: Properties::default(),
        }));
        let mut chunk = [0u8; 256];
        loop {
            match self.stream.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    }

    /// Acknowledge a delivery, freeing the slot it holds.
    fn acknowledge(&mut self, packet_id: u16) {
        self.send(&Packet::PubAck(Ack {
            packet_id,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        }));
    }

    /// Write one packet.
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

    /// Wait `patience` for a delivery, and say what arrived.
    fn delivery_within(&mut self, patience: Duration) -> Option<Publish> {
        self.stream
            .set_read_timeout(Some(patience))
            .expect("a read deadline");
        let deadline = Instant::now() + patience;
        while Instant::now() < deadline {
            match self.next() {
                Some(Packet::Publish(publish)) => return Some(publish),
                // Anything else the broker volunteers is not what is
                // being waited for.
                Some(_) => continue,
                None => return None,
            }
        }
        None
    }
}

// ---- the criterion ------------------------------------------------------

/// A durable session, its subscription and its queued message survive
/// a broker that was killed; a clean-start session does not.
///
/// The sequence the plan sets out, with both halves against one
/// restarted broker:
///
/// 1. a durable session subscribes and disconnects;
/// 2. a clean-start session subscribes to a filter of its own and
///    disconnects, which ends it;
/// 3. a third client publishes at least once and is acknowledged;
/// 4. the broker is killed before any flush interval could have
///    elapsed;
/// 5. it is started again on the same directory;
/// 6. the durable session is present, is given its message and still
///    receives what is published to its filter, and the clean-start
///    session is absent, holds no subscription and is given nothing.
#[test]
fn a_restart_keeps_the_durable_session_and_not_the_clean_one() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let broker = BrokerProcess::start(Some(directory.path()), directory.path().join("first.log"));
    let address = broker.address;

    let (mut durable, present) = Client::connect(address, "durable", false, EXPIRY);
    assert!(!present, "nothing was stored before this test ran");
    durable.subscribe(FILTER);
    durable.disconnect();

    // The control, on the same broker, with a filter of its own. Its
    // expiry is zero, which with a clean start is a session that ends
    // when the connection does.
    let (mut ephemeral, present) = Client::connect(address, "ephemeral", true, 0);
    assert!(!present, "a clean start never resumes anything");
    ephemeral.subscribe(EPHEMERAL_FILTER);
    ephemeral.disconnect();

    let (mut publisher, _) = Client::connect(address, "publisher", true, 0);
    // The control must be *gone* before the kill, not merely
    // disconnected: a session whose deadline has not yet passed is one
    // restore is right to bring back, so killing the broker in the
    // same second would test the clock rather than the rule.
    publisher.publish_until_nobody_wants_it(EPHEMERAL_FILTER);

    assert_eq!(
        publisher.publish_and_wait(FILTER, PAYLOAD),
        ReasonCode::Success,
        "the durable session should have been queued this message"
    );
    let acknowledged = Instant::now();

    broker.kill();

    // The teeth. Everything this test is about happened between the
    // acknowledgement and the kill, and the writer was told not to
    // flush for an hour, so nothing can have reached the device except
    // through the promise made to the publisher.
    let since = acknowledged.elapsed();
    assert!(
        since * 10 < NEVER,
        "the broker must be killed well inside the writer's flush interval, \
         or a message that survives may have survived by luck: {since:?} of \
         {NEVER:?}"
    );

    let restarted =
        BrokerProcess::start(Some(directory.path()), directory.path().join("second.log"));
    let address = restarted.address;

    let (mut durable, present) = Client::connect(address, "durable", false, EXPIRY);
    assert!(
        present,
        "the durable session did not survive the restart: {}",
        restarted.logged()
    );
    let queued = match durable.delivery_within(PATIENCE) {
        Some(publish) => publish,
        None => panic!(
            "the message the publisher was told was safe never arrived: {}",
            restarted.logged()
        ),
    };
    assert_eq!(&queued.topic[..], FILTER);
    assert_eq!(
        &queued.payload[..],
        PAYLOAD,
        "the message came back changed"
    );
    // Out of the window again, so the delivery below is not waiting
    // behind it.
    durable.acknowledge(
        queued
            .packet_id
            .expect("an at-least-once delivery is numbered"),
    );

    // The third of the criterion's three nouns, which the message
    // above does not prove. It was queued before the kill, so a
    // restore that rebuilt the queue and forgot to put the filter back
    // in the matching trie would have delivered it just the same. This
    // is a publication made *after* the restart: the reason code says
    // the broker found a subscriber, and the delivery says it was this
    // one.
    let (mut publisher, _) = Client::connect(address, "publisher", true, 0);
    assert_eq!(
        publisher.publish_and_wait(FILTER, AFTERWARDS),
        ReasonCode::Success,
        "the restored session's subscription is not in the matching trie: {}",
        restarted.logged()
    );
    let delivered = durable
        .delivery_within(PATIENCE)
        .expect("the restored subscription receives what is published to it");
    assert_eq!(&delivered.payload[..], AFTERWARDS);

    // And the half that makes the other half mean something. The same
    // broker has just resumed a session and delivered its message, so
    // what follows is the broker having nothing for this client rather
    // than the broker having nothing for anybody.
    let (mut ephemeral, present) = Client::connect(address, "ephemeral", false, EXPIRY);
    assert!(
        !present,
        "a clean-start session came back from the dead; the restore is \
         keeping what was never durable"
    );

    // Asked rather than waited for. The subscription is gone if the
    // broker says nobody holds the filter, and that is an answer in a
    // packet rather than an absence a slow machine could fake.
    assert_eq!(
        publisher.publish_and_wait(EPHEMERAL_FILTER, PAYLOAD),
        ReasonCode::NoMatchingSubscribers,
        "the clean-start session's subscription survived a restart it \
         should not have survived"
    );
    assert!(
        ephemeral.delivery_within(SILENCE).is_none(),
        "a session that asked for nothing to be kept was given a message \
         published while it did not exist"
    );
}

/// The same sequence against a broker with no store keeps nothing.
///
/// Convention 4, as a test rather than as an afternoon: the criterion
/// above is only evidence if it could fail, and the one thing that
/// would make it pass for the wrong reason is a broker that resumes a
/// session for some reason other than having stored it. This is that
/// broker — the same binary, the same sequence, no
/// `--store-directory` — and it must lose everything.
///
/// It also pins the mode the documentation promises: running without a
/// store is supported, and means exactly that nothing outlives the
/// process.
#[test]
fn a_restart_with_no_store_keeps_nothing() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let broker = BrokerProcess::start(None, directory.path().join("first.log"));
    let address = broker.address;

    let (mut durable, _) = Client::connect(address, "durable", false, EXPIRY);
    durable.subscribe(FILTER);
    durable.disconnect();

    let (mut publisher, _) = Client::connect(address, "publisher", true, 0);
    assert_eq!(
        publisher.publish_and_wait(FILTER, PAYLOAD),
        ReasonCode::Success,
        "the session was queued the message; a broker with no store \
         still has one until it dies"
    );

    broker.kill();

    let restarted = BrokerProcess::start(None, directory.path().join("second.log"));
    let address = restarted.address;
    let (mut durable, present) = Client::connect(address, "durable", false, EXPIRY);
    assert!(
        !present,
        "a broker with no store reported a session it cannot have stored"
    );
    assert!(
        durable.delivery_within(SILENCE).is_none(),
        "a broker with no store delivered a message published before it died"
    );
}
