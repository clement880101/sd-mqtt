//! What a bridge's outbound half tells an operator it did.
//!
//! Section 9.2's `mqtt_bridge`, driven the way it is really driven: a
//! [`session::run`] on one end of a pipe and a scripted remote on the
//! other, over the same codec, the same outbox and the same store a
//! broker gives it. Nothing between the envelope and the wire is
//! replaced.
//!
//! # Why this file exists at all
//!
//! Two reviews found the same hole from different directions, and both
//! found it by mutation: **nothing in the suite observed a bridge
//! rule's statistics**, so deleting the delivered increment from
//! `Session::retire` broke no test. The bridge's own integration tests
//! run two `sdmqtt` processes and assert what arrives at the far end,
//! which says nothing about what the near end counted; `crates/rules/
//! tests/ingest.rs` has the equivalent assertion for `http_in` and its
//! message — "a post the rule would not publish was counted as a
//! delivery" — names exactly the failure a bridge had no guard for.
//!
//! # Convention 5, applied here
//!
//! A counter that moved on every outcome would satisfy an assertion
//! that a delivery moved `delivered`. So every test below puts a
//! message that works beside one that does not **in the same
//! connection**, and reads all four numbers after each. The question
//! "what would have to be true for this to fail" has an answer that is
//! not "the feature would have to be absent": the two messages differ
//! only in what the remote said about them, or in whether their
//! payload is still there.
//!
//! # And a pipe rather than two brokers
//!
//! `crates/sdmqtt/tests/bridge.rs` has the two-process test and it is
//! the right shape for what it asserts. It cannot make a remote answer
//! "not authorized", because the remote is this broker and this broker
//! authorises everything; a scripted peer can, and a remote with
//! access control is the ordinary case this was getting wrong.

use std::sync::Arc;
use std::time::Duration;

use broker_core::broker::{Broker, BrokerConfig};
use broker_core::hooks::Refusal;
use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{Ack, ConnAck, Disconnect};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};
use rules::bridge::marker::Marker;
use rules::bridge::rewrite::Rewrite;
use rules::bridge::session::{self, Bridge, Carried, Ended};
use rules::bridge::Flow;
use rules::rule::{Action, BridgeTopic, Direction, MqttBridge, Rule, RuleId};
use rules::stats::{RuleStats, Totals};
use rules::Outbox;
use sdmqtt_server::metrics::RuleEvent;
use sdmqtt_server::shared::Shared;
use storage::record::{BodyRecord, MessageId, Qos, Record};
use storage::{MemoryStorage, Storage, Writer, WriterConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::Notify;

/// Nothing here should take longer than this; a test that hangs is a
/// test that tells nobody anything.
const PATIENCE: Duration = Duration::from_secs(10);

/// The bridge rule's identifier.
const RULE: &str = "to-the-hub";

/// The local filter the outbound entry carries.
const LOCAL: &str = "sensor/+/temp";

// ---- the remote ---------------------------------------------------------

/// The far end of the pipe, driven by hand.
struct Remote {
    stream: DuplexStream,
    buf: BytesMut,
    /// What the bridge published, in the order it arrived.
    published: Vec<(String, Bytes)>,
}

impl Remote {
    fn new(stream: DuplexStream) -> Self {
        Self {
            stream,
            buf: BytesMut::new(),
            published: Vec::new(),
        }
    }

    /// The next packet the bridge sent, or `None` when it closed.
    async fn next(&mut self) -> Option<Packet> {
        loop {
            if let Some(packet) = decode(&mut self.buf, Version::V5, &Limits::default())
                .expect("the bridge sends packets this codec accepts")
            {
                return Some(packet);
            }
            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(read) => self.buf.extend_from_slice(&chunk[..read]),
            }
        }
    }

    async fn send(&mut self, packet: &Packet) {
        let mut out = BytesMut::new();
        encode(packet, Version::V5, &mut out).expect("the remote composes valid packets");
        self.stream
            .write_all(&out)
            .await
            .expect("the pipe takes the bytes");
    }

    /// Read the CONNECT and accept it.
    ///
    /// [MQTT-3.1.4-5]: nothing may be sent before this, and the bridge
    /// does not — a packet arriving here that is not a CONNECT is the
    /// failure that assertion exists for.
    async fn accept(&mut self) {
        let first = self.next().await.expect("the bridge connects");
        assert!(
            matches!(first, Packet::Connect(_)),
            "the bridge's first packet was a {:?} and must be a connect",
            first.kind()
        );
        self.send(&Packet::ConnAck(ConnAck {
            session_present: false,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        }))
        .await;
    }

    /// Answer every publish with `reasons`, in order, and then go.
    ///
    /// The remote disconnects rather than dropping the pipe, so that
    /// the session ends for a reason the test names rather than for a
    /// broken socket: a connection that ended in an I/O failure would
    /// pass an assertion about the numbers for the wrong reason.
    async fn answer(&mut self, reasons: &[ReasonCode]) {
        let mut answered = 0usize;
        while answered < reasons.len() {
            let Some(packet) = self.next().await else {
                break;
            };
            match packet {
                Packet::Publish(publish) => {
                    self.published
                        .push((publish.topic.clone(), publish.payload.clone()));
                    let reason = reasons[answered];
                    answered += 1;
                    let Some(packet_id) = publish.packet_id else {
                        // At most once: nothing is answered, and
                        // nothing is owed.
                        continue;
                    };
                    self.send(&Packet::PubAck(Ack {
                        packet_id,
                        reason,
                        properties: Properties::default(),
                    }))
                    .await;
                }
                // The bridge pings while it waits; a remote that did
                // not answer would end the connection on the
                // keep-alive rather than on the disconnect below.
                Packet::PingReq => self.send(&Packet::PingResp).await,
                _ => {}
            }
        }
        self.disconnect().await;
    }

    /// Take nothing and go, after waiting long enough that anything
    /// the bridge meant to send would have arrived.
    ///
    /// The assertion this serves is an absence, which convention 5
    /// warns about, so it is never the only assertion in a test.
    async fn expect_nothing_then_go(&mut self, patience: Duration) {
        loop {
            match tokio::time::timeout(patience, self.next()).await {
                Err(_) | Ok(None) => break,
                Ok(Some(Packet::PingReq)) => self.send(&Packet::PingResp).await,
                Ok(Some(Packet::Publish(publish))) => {
                    self.published.push((publish.topic, publish.payload));
                    break;
                }
                Ok(Some(_)) => {}
            }
        }
        self.disconnect().await;
    }

    async fn disconnect(&mut self) {
        self.send(&Packet::Disconnect(Disconnect {
            reason: ReasonCode::Success,
            properties: Properties::default(),
        }))
        .await;
    }
}

// ---- the near end -------------------------------------------------------

/// A bridge over a memory store, with envelopes already waiting.
struct Fixture {
    store: Arc<MemoryStorage>,
    writer: Arc<Writer>,
    outbox: Arc<Outbox>,
    stats: Arc<RuleStats>,
    bridge: Bridge,
}

impl Fixture {
    /// `payloads` become one envelope each, in order, body `n + 1`.
    fn new(payloads: &[&[u8]]) -> Self {
        Self::with_bodies(
            payloads
                .iter()
                .enumerate()
                .map(|(index, payload)| {
                    body(
                        u8::try_from(index + 1).expect("few enough envelopes"),
                        payload,
                    )
                })
                .collect(),
        )
    }

    /// The same, for a test that needs a body carrying something in
    /// particular rather than only a payload.
    fn with_bodies(bodies: Vec<BodyRecord>) -> Self {
        let action = action();
        let store = Arc::new(MemoryStorage::new());
        let rule = Rule {
            id: RuleId::new(RULE).expect("a valid identifier"),
            name: "To the hub".to_owned(),
            enabled: true,
            trigger: None,
            action: Action::MqttBridge(Box::new(action.clone())),
        };
        store
            .apply(&[Record::RulePut {
                rule: rule.to_record(),
            }])
            .expect("the rule is storable");

        let writer = Arc::new(
            Writer::spawn(Arc::clone(&store) as Arc<dyn Storage>, writer_config())
                .expect("the writer starts"),
        );
        let outbox =
            Arc::new(Outbox::open(rule.id.clone(), store.as_ref()).expect("the outbox opens"));
        for (index, body) in bodies.into_iter().enumerate() {
            let n = u8::try_from(index + 1).expect("few enough envelopes");
            let (_, records) = outbox.append(
                id(100 + n),
                id(n),
                body,
                "sensor",
                1_700_000_000 + u32::from(n),
            );
            store.apply(&records).expect("the envelope is storable");
        }

        let shared = Arc::new(Shared::with_store(
            Broker::new(BrokerConfig::default()),
            Arc::clone(&writer),
        ));
        // **Exporting, rather than the bare `RuleStats::new` every
        // other test of a connector builds.** Five of this rule's nine
        // outcomes are exported and not sampled, so a `RuleStats` with
        // no broker behind it cannot see them at all — and a test that
        // cannot see a counter is a test a deleted increment passes.
        let stats = Arc::new(RuleStats::exporting(Some(shared.metrics().rule(RULE))));
        let bridge = Bridge {
            rule: rule.id.clone(),
            action: action.clone(),
            rewrites: action.topics.iter().map(Rewrite::new).collect(),
            marker: Marker::new(&rule.id, action.version),
            outbox: Arc::clone(&outbox),
            store: Arc::clone(&store) as Arc<dyn Storage>,
            writer: Arc::clone(&writer),
            shared: Arc::clone(&shared),
            wake: Arc::new(Notify::new()),
            stats: Arc::clone(&stats),
        };
        Self {
            store,
            writer,
            outbox,
            stats,
            bridge,
        }
    }

    /// One connection, with the remote scripted by `script`.
    async fn connect<F, Fut>(&self, flow: &mut Flow, script: F) -> (Carried, Ended, Remote)
    where
        F: FnOnce(Remote) -> Fut,
        Fut: std::future::Future<Output = Remote>,
    {
        let (near, far) = tokio::io::duplex(64 * 1024);
        let running = session::run(&self.bridge, near, RULE, true, flow);
        let scripted = script(Remote::new(far));
        let (ended, remote) =
            tokio::time::timeout(PATIENCE, async { tokio::join!(running, scripted) })
                .await
                .expect("the connection finishes");
        let (_, carried, ended) = ended;
        // Everything `retire` wrote went to the writer without waiting,
        // which is right for a bridge and means a test has to wait
        // here: an empty durable batch flushes what is already applied
        // and says when it is.
        self.settle().await;
        (carried, ended, remote)
    }

    /// Wait until everything handed to the writer is in the store.
    async fn settle(&self) {
        self.writer
            .apply_durable(Vec::new())
            .expect("the writer is running")
            .await
            .expect("the flush succeeds");
    }

    /// What the store says is still waiting.
    fn depth(&self) -> u64 {
        self.outbox
            .depth(self.store.as_ref())
            .expect("the depth answers")
    }

    /// What the rule reports, which is the maintained count rather than
    /// the store's.
    fn totals(&self) -> Totals {
        self.stats.totals(self.outbox.waiting())
    }

    /// What one of this rule's exported counters reads.
    ///
    /// Taken from the registry by label rather than from the handle
    /// the fixture holds, so that a counter which moved the wrong
    /// series cannot pass: `Metrics::rule` answers a handle on the
    /// same series, and a different series would read zero.
    fn exported(&self, event: RuleEvent) -> u64 {
        self.bridge.shared.metrics().rule(RULE).event(event).get()
    }

    fn holds_body(&self, n: u8) -> bool {
        self.store.body(&id(n)).expect("the read answers").is_some()
    }
}

/// A bridge that carries one filter outwards and reads nothing back.
///
/// Outbound only, so no SUBSCRIBE is sent and the scripts below have
/// one kind of packet to answer. What is being tested is what happens
/// to an outbox entry, which is the outbound half by definition.
fn action() -> MqttBridge {
    MqttBridge {
        address: "hub.invalid:1883".to_owned(),
        client_id: RULE.to_owned(),
        version: Version::V5,
        // No state topic: this bridge is not publishing about itself
        // here, and a publication into the broker would be a second
        // thing for these assertions to have to account for.
        state_topic: String::new(),
        topics: vec![BridgeTopic {
            filter: LOCAL.to_owned(),
            direction: Direction::Out,
            qos: QoS::AtLeastOnce,
            local_prefix: String::new(),
            remote_prefix: "from-here/".to_owned(),
            template: None,
        }],
        ..MqttBridge::default()
    }
}

fn id(n: u8) -> MessageId {
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

/// The same, carrying the user properties a message that had already
/// been through this bridge would carry.
fn marked_body(n: u8, payload: &[u8], user_properties: Vec<(String, String)>) -> BodyRecord {
    BodyRecord {
        properties: broker_core::journal::stored_properties(&Properties {
            user_properties,
            ..Properties::default()
        }),
        ..body(n, payload)
    }
}

/// A writer whose only flush is the one a waiter asks for, so that
/// nothing a test sees happened because a timer went off.
fn writer_config() -> WriterConfig {
    WriterConfig {
        interval: Duration::from_secs(3_600),
        records: 1_000_000_000,
        ..WriterConfig::default()
    }
}

// ---- the tests ----------------------------------------------------------

/// A remote that refuses a message must not have it counted as
/// delivered.
///
/// "Not authorized" and "quota exceeded" are the ordinary answers from
/// a broker with access control, and [MQTT-4.3.3-x] says a refused
/// publish is finished rather than retried — so the envelope leaves the
/// outbox either way and the **only** place the difference can show is
/// the numbers an operator reads. Both messages are in one connection
/// so that a counter which moved on every outcome cannot pass.
#[tokio::test(flavor = "multi_thread")]
async fn a_message_the_remote_refuses_is_counted_as_a_failure_and_not_a_delivery() {
    let fixture = Fixture::new(&[b"21.5", b"22.5"]);
    let mut flow = Flow::new(fixture.bridge.action.receive_maximum);

    let (carried, ended, remote) = fixture
        .connect(&mut flow, |mut remote| async move {
            remote.accept().await;
            remote
                .answer(&[ReasonCode::Success, ReasonCode::NotAuthorized])
                .await;
            remote
        })
        .await;

    assert_eq!(
        remote.published.len(),
        2,
        "the bridge sent something other than the two envelopes it held"
    );
    assert!(
        matches!(ended, Ended::Disconnected { .. }),
        "the connection ended some other way: {ended}"
    );
    assert_eq!(
        fixture.totals(),
        Totals {
            fired: 0,
            delivered: 1,
            failed: 1,
            dead_lettered: 0,
            waiting: 0,
        },
        "a message the remote refused was counted as a delivery"
    );
    assert_eq!(
        carried,
        Carried {
            sent: 1,
            received: 0,
            refused: 1,
        },
        "the connection's own summary disagrees with the rule's numbers"
    );
    // One connection, counted once at each end of it. Both were a log
    // line and nothing else, and a log line cannot be alerted on: a
    // bridge that reconnects every thirty seconds looks perfect by
    // every other number this broker publishes. They are asserted here
    // rather than in a test of their own because this test is a whole
    // connection from CONNECT to DISCONNECT, which is the only thing
    // either of them counts.
    assert_eq!(fixture.exported(RuleEvent::Connected), 1);
    assert_eq!(fixture.exported(RuleEvent::Disconnected), 1);
    // Both are finished with, in the store as well as in the counters:
    // a refusal must not be retried, and a delivery must not be either.
    assert_eq!(fixture.depth(), 0, "an answered envelope is still waiting");
    assert!(!fixture.holds_body(1), "a delivered payload was leaked");
    assert!(!fixture.holds_body(2), "a refused payload was leaked");
}

/// Section 9.2's second line of defence, counted.
///
/// A message that came in through this bridge and was queued to go
/// back out through it is refused by the marker. That much was already
/// true and already tested in `rules::bridge::marker`. What was not
/// true is section 11.3's sentence about it: the refusal was a
/// `tracing::debug!` and a field in a per-connection struct, so an
/// operator had **no number at all** for the defence that catches the
/// loop a configuration check cannot see — and a mutation that deleted
/// the check would have moved nothing an alert could read.
///
/// Convention 5's question: this fails if the counter is not moved,
/// and the input can make it fail because the envelope really does
/// carry this bridge's own origin. It is asserted through
/// `session::run` over a real socket rather than against
/// `marker::carry`, because a unit test of the helper is not a test of
/// the caller that was supposed to use it.
///
/// Two envelopes, one marked and one not, so that a counter which
/// moved on every entry cannot pass.
#[tokio::test(flavor = "multi_thread")]
async fn a_message_this_bridge_already_carried_is_refused_and_counted() {
    let marked = marked_body(
        1,
        b"21.5",
        vec![(rules::bridge::marker::ORIGIN.to_owned(), RULE.to_owned())],
    );
    let fixture = Fixture::with_bodies(vec![marked, body(2, b"22.5")]);
    let mut flow = Flow::new(fixture.bridge.action.receive_maximum);
    let metrics = Arc::clone(&fixture.bridge.shared);

    assert_eq!(
        metrics.metrics().publication_refused(Refusal::Looped).get(),
        0,
        "the counter had already moved before anything happened"
    );

    let (carried, _, remote) = fixture
        .connect(&mut flow, |mut remote| async move {
            remote.accept().await;
            remote.answer(&[ReasonCode::Success]).await;
            remote
        })
        .await;

    assert_eq!(
        remote
            .published
            .iter()
            .map(|(topic, _)| topic.as_str())
            .collect::<Vec<_>>(),
        vec!["from-here/sensor/2/temp"],
        "the message this bridge had already carried went back out"
    );
    assert_eq!(
        carried,
        Carried {
            sent: 1,
            received: 0,
            refused: 1,
        },
        "the connection's own summary disagrees with what went out"
    );
    assert_eq!(
        metrics.metrics().publication_refused(Refusal::Looped).get(),
        1,
        "the loop was stopped and nothing counted it"
    );
    // And it landed on its own series rather than on a neighbour's.
    for other in [
        Refusal::TooManyHops,
        Refusal::NotATopic,
        Refusal::Reserved,
        Refusal::NotDurable,
    ] {
        assert_eq!(
            metrics.metrics().publication_refused(other).get(),
            0,
            "a loop refusal moved the {other:?} counter as well"
        );
    }
}

/// An envelope whose payload has gone leaves the outbox, once.
///
/// The same case `http_out` has and the same answer: the store refuses
/// a release of a body it does not hold, and a batch is applied in full
/// or not at all, so an entry retired with a completion record leaves
/// **neither** record behind. What that looked like was a bridge that
/// re-read the same dead envelope on every reconnection for the life of
/// the process, counted a delivery each time, and a rule that could
/// never be deleted because its outbox never emptied.
///
/// So this connects twice. The first connection is what fixes it; the
/// second is what says it stayed fixed, and is the half a test of one
/// connection cannot see.
#[tokio::test(flavor = "multi_thread")]
async fn an_envelope_whose_payload_is_gone_leaves_the_outbox_and_is_not_counted_delivered() {
    let fixture = Fixture::new(&[b"21.5", b"22.5"]);
    // Somebody else released the first payload's last reference.
    fixture
        .store
        .apply(&[Record::BodyReleased { id: id(1) }])
        .expect("the release is accepted");
    let mut flow = Flow::new(fixture.bridge.action.receive_maximum);

    let (_, _, remote) = fixture
        .connect(&mut flow, |mut remote| async move {
            remote.accept().await;
            remote.answer(&[ReasonCode::Success]).await;
            remote
        })
        .await;

    assert_eq!(
        remote
            .published
            .iter()
            .map(|(topic, payload)| (topic.as_str(), payload.as_ref()))
            .collect::<Vec<_>>(),
        vec![("from-here/sensor/2/temp", b"22.5" as &[u8])],
        "the envelope behind the orphan was not sent, or the orphan was"
    );
    assert_eq!(
        fixture.totals(),
        Totals {
            fired: 0,
            delivered: 1,
            failed: 1,
            dead_lettered: 0,
            waiting: 0,
        },
        "an envelope with no payload was counted as a delivery"
    );
    // Both halves of the wedge. The store is empty, so a delete is no
    // longer refused; and the maintained count agrees with it, so the
    // page does not say a rule is idle while a delete says it is not.
    assert_eq!(fixture.depth(), 0, "the orphan is still in the outbox");
    assert_eq!(
        fixture.outbox.waiting(),
        fixture.depth(),
        "the maintained waiting count has drifted from the store's depth"
    );

    // A second connection, as a reconnection is. Nothing is left to
    // send and nothing is left to count.
    let mut flow = Flow::new(fixture.bridge.action.receive_maximum);
    let (_, _, again) = fixture
        .connect(&mut flow, |mut remote| async move {
            remote.accept().await;
            remote
                .expect_nothing_then_go(Duration::from_millis(400))
                .await;
            remote
        })
        .await;

    assert!(
        again.published.is_empty(),
        "a reconnection sent {} message(s) from an outbox that should be empty",
        again.published.len()
    );
    assert_eq!(
        fixture.totals(),
        Totals {
            fired: 0,
            delivered: 1,
            failed: 1,
            dead_lettered: 0,
            waiting: 0,
        },
        "reconnecting moved a counter for a message that does not exist"
    );
}
