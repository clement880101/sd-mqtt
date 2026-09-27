//! What the single lock costs.
//!
//! The broker's methods take an exclusive reference, so every packet in
//! the process passes through one mutex and the sharding the
//! subscription trie does internally is undone above it. This harness
//! produces the number, so that a redesign is decided from a
//! measurement. It draws no conclusion; twice on this project the
//! obvious optimisation was slower when measured, and a third guess is
//! not wanted.
//!
//! # Why not criterion
//!
//! Criterion is already a development dependency in `broker-core` and
//! is the right tool there, where the quantity is the latency of one
//! synchronous call that can be run a million times. The quantity here
//! is the aggregate rate of sixteen concurrent connections in steady
//! state, which is a different shape:
//!
//! - A sample would have to bind a listener, complete sixteen
//!   handshakes and tear them all down, so setup would dominate the
//!   measurement rather than the work.
//! - Criterion's outlier rejection and resampling are statistics over
//!   per-iteration times it did not produce here; a throughput figure
//!   over a window is one number, not a distribution of iterations.
//! - Its warm-up and resampling multiply wall time by a large factor
//!   for a measurement that already costs seconds per point.
//!
//! So these are ignored tests that print a table. They are run
//! deliberately and their output is read by a person:
//!
//! ```text
//! cargo test -p sdmqtt-server --release --test throughput -- \
//!     --ignored --nocapture --test-threads 1
//! ```
//!
//! `--release` matters: a debug build measures the codec's bounds
//! checks. `--test-threads 1` matters: two of these running at once
//! measure each other.
//!
//! # What each row can and cannot see
//!
//! The TCP rows measure the whole shell, which means they are capped by
//! whichever of the loopback sockets and the lock gives out first. A
//! flattening curve there is therefore ambiguous on its own, so the
//! last table repeats the shape with no sockets at all: the same
//! `Shared`, the same `handle` and `deliver` calls, the same number of
//! concurrent callers. If the socket curve flattens and the lock-only
//! curve flattens at a similar rate, the lock is the ceiling. If the
//! lock-only curve is far higher, the sockets are, and the lock is not
//! yet what limits this.
//!
//! # What a store costs
//!
//! Milestone one measured all of the above with no store at all.
//! [`what_a_store_costs`] measures the same broker with one, at both
//! qualities of service that differ in whether anything is promised:
//!
//! - **At most once promises nothing and waits on nothing.** Section
//!   7.3 journals such a publication only where it touches durable
//!   state, and one delivered straight to an attached subscriber
//!   touches none: `Outbound::offer` dispatches it without pushing it
//!   on to a queue, so the whole exchange writes no record at all. The
//!   row is here to show that, not to find a cost.
//! - **At least once promises, so it waits.** The acknowledgement is
//!   sent after the commit is durable, and the storage crate's own
//!   measurement says what that costs: 334 durable writes a second
//!   with one waiter and 25,100 with a hundred. Each publisher here
//!   keeps exactly one publication outstanding, so the publisher count
//!   *is* the waiter count and the two tables are in the same
//!   quantity.
//!
//! Both rows are measured with and without a store in the same run, so
//! the comparison is on one machine on one day rather than against a
//! figure recorded earlier.
//!
//! # What a property block costs a fan-out
//!
//! A message matching a thousand subscriptions is offered to a
//! thousand sessions, and each of them journals a body record carrying
//! the property block as bytes, though the store keeps one body per
//! message however many sessions hold it. Encoding it per subscriber
//! was a third of the cost of a large fan-out; `SharedProperties`
//! encodes it once and every subscriber after the first takes a
//! reference count.
//!
//! The last table is what said so and is what would say if it came
//! back. It publishes to a growing fan-out with an empty property
//! block and with a realistic one, and prints the encode's own cost
//! beside the per-subscriber cost of the whole publish. The two
//! property rows now meet; a build that encoded per subscriber again
//! would pull them apart by the encode, which the line above the table
//! measures on its own.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use broker_core::broker::{Broker, BrokerConfig};
use broker_core::journal;
use broker_core::session::Message;
use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{Ack, Connect, Publish, RetainHandling, Subscribe, SubscriptionOptions};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};
use sdmqtt_server::listener::{Server, ServerConfig};
use sdmqtt_server::shared::Shared;
use storage::{RedbStorage, Storage, SyncMode, Writer, WriterConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// How long the load runs before anything is counted.
///
/// The socket buffers fill during this, so the counters sampled after
/// it are a steady-state rate rather than a burst into an empty kernel
/// buffer.
const WARM_UP: Duration = Duration::from_secs(1);

/// The window the rate is measured over.
const WINDOW: Duration = Duration::from_secs(3);

/// How many publishes one publisher writes per `write_all`.
///
/// One syscall per message would measure the client's syscall cost, not
/// the broker's lock, and a real client that pipelines does not do that
/// either. The server still sees, and locks for, one packet at a time:
/// batching is on the wire, not in the broker.
const BATCH: usize = 32;

/// The publisher counts in the curve.
const PUBLISHERS: [usize; 5] = [1, 2, 4, 8, 16];

/// How many connections sit idle in the second curve.
const IDLE: usize = 1_000;

/// How many messages a paced pair may have outstanding end to end.
///
/// Below the outbox depth of 1024, so a paced run discards nothing and
/// its delivered figure is a rate the system can sustain. Far above the
/// batch, so the window itself is not what sets the rate.
const CREDIT: u64 = 512;

/// A payload the size of a small telemetry reading.
const PAYLOAD: &[u8] = b"0123456789abcdef0123456789abcdef";

fn connect_packet(client_id: &str) -> Packet {
    Packet::Connect(Box::new(Connect {
        version: Version::V5,
        client_id: client_id.into(),
        clean_start: true,
        // Disabled. A keep-alive here would measure the ring's sweep
        // alongside the lock and let an idle connection be reaped
        // mid-window.
        keep_alive: 0,
        username: None,
        password: None,
        will: None,
        properties: Properties::default(),
    }))
}

fn publish_packet(topic: &str) -> Packet {
    Packet::Publish(Publish {
        dup: false,
        // At-most-once, so one inbound packet is one lock span and no
        // acknowledgement travels back. Anything higher would measure
        // the flow control as well.
        qos: QoS::AtMostOnce,
        retain: false,
        topic: topic.into(),
        packet_id: None,
        payload: Bytes::from_static(PAYLOAD),
        properties: Properties::default(),
    })
}

fn subscribe_packet(topic: &str, qos: QoS) -> Packet {
    Packet::Subscribe(Subscribe {
        packet_id: 1,
        filters: vec![SubscriptionOptions {
            filter: topic.into(),
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendAlways,
        }],
        properties: Properties::default(),
    })
}

fn bytes_of(packet: &Packet) -> BytesMut {
    let mut buf = BytesMut::new();
    encode(packet, Version::V5, &mut buf).expect("the packet encodes");
    buf
}

/// Read until one packet is whole, or the peer closes.
async fn next(stream: &mut TcpStream, buf: &mut BytesMut) -> Option<Packet> {
    loop {
        if let Some(packet) = decode(buf, Version::V5, &Limits::default()).expect("a valid packet")
        {
            return Some(packet);
        }
        let mut chunk = [0u8; 4096];
        let read = stream
            .read(&mut chunk)
            .await
            .expect("the socket is readable");
        if read == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..read]);
    }
}

/// Connect and wait for the acknowledgement.
async fn handshake(address: SocketAddr, client_id: &str) -> (TcpStream, BytesMut) {
    let mut stream = TcpStream::connect(address).await.expect("the server binds");
    stream.set_nodelay(true).expect("nodelay");
    let mut buf = BytesMut::new();
    let out = bytes_of(&connect_packet(client_id));
    stream.write_all(&out).await.expect("the write succeeds");
    match next(&mut stream, &mut buf).await {
        Some(Packet::ConnAck(ack)) => assert_eq!(ack.reason, ReasonCode::Success),
        other => panic!("expected an acknowledgement, got {other:?}"),
    }
    (stream, buf)
}

/// One measured point: what arrived and what was lost.
struct Point {
    /// Publishes written by the clients during the window.
    offered: u64,
    /// Publishes that reached a subscriber during the window.
    delivered: u64,
    /// Actions discarded because a reader could not keep up.
    dropped: u64,
    /// How long the window actually was.
    elapsed: Duration,
}

impl Point {
    fn offered_per_second(&self) -> f64 {
        self.offered as f64 / self.elapsed.as_secs_f64()
    }

    fn delivered_per_second(&self) -> f64 {
        self.delivered as f64 / self.elapsed.as_secs_f64()
    }
}

/// How the publishers are driven.
#[derive(Clone, Copy)]
enum Load {
    /// Write as fast as the socket will take it. Backpressure then
    /// throttles each publisher to the rate the server's read loop
    /// consumes, so the offered figure is the rate packets pass
    /// through the lock. What the subscriber then receives is not a
    /// throughput: at-most-once has no flow control towards a
    /// subscriber, so anything the outbox cannot hold is discarded by
    /// policy and most of the offered load never reaches anybody.
    Saturating,
    /// Never let a pair have more than this many messages outstanding
    /// end to end. Below the outbox depth, so nothing is discarded and
    /// the delivered figure is a sustainable rate rather than a
    /// survival rate.
    Paced(u64),
}

impl Load {
    fn label(self) -> &'static str {
        match self {
            Load::Saturating => "saturating",
            Load::Paced(_) => "paced",
        }
    }
}

/// Whether the broker under load writes down what it says.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Store {
    /// None at all, which is the broker milestone one measured and the
    /// control for every row below.
    Absent,
    /// redb in a temporary directory, flushing to the device.
    Redb,
}

impl Store {
    fn label(self) -> &'static str {
        match self {
            Store::Absent => "no store",
            Store::Redb => "redb",
        }
    }
}

/// A `Shared` as `store` says, and the directory it lives in.
///
/// The directory comes back because it must outlive the server: a
/// [`tempfile::TempDir`] deletes its tree when it is dropped, and one
/// dropped here would take the store out from under the run.
///
/// `SyncMode::Immediate` is not a choice this makes. Section 7.3's
/// promise is kept only under it, and the relaxed mode against this
/// backend keeps a commit in the process's memory until a clean
/// shutdown — so a figure measured under it would be the throughput of
/// a broker that acknowledges what it has not written.
fn shared_for(store: Store, config: &ServerConfig) -> (Arc<Shared>, Option<tempfile::TempDir>) {
    match store {
        Store::Absent => (Arc::new(Shared::new(config.broker.clone())), None),
        Store::Redb => {
            let directory = tempfile::TempDir::new().expect("a temporary directory");
            let redb =
                RedbStorage::open_with(directory.path().join("store.redb"), SyncMode::Immediate)
                    .expect("a store opens");
            let writer = Writer::spawn(Arc::new(redb) as Arc<dyn Storage>, WriterConfig::default())
                .expect("the writer gets a thread");
            let shared = Shared::with_store(Broker::new(config.broker.clone()), Arc::new(writer));
            (Arc::new(shared), Some(directory))
        }
    }
}

/// Run `publishers` publisher/subscriber pairs, with `idle` connections
/// attached and silent, and report the steady-state rate.
async fn measure(publishers: usize, idle: usize, load: Load, store: Store) -> Point {
    let config = ServerConfig::ephemeral();
    let (shared, _directory) = shared_for(store, &config);
    let running = Server::bind_shared(config, shared)
        .await
        .expect("the server binds");
    let address = running.address();

    // Held so they stay attached. Nothing is ever written to them and
    // nothing is ever sent to them; they exist to make the registry,
    // the outbox map and the semaphore carry a realistic population.
    let mut idlers = Vec::with_capacity(idle);
    for n in 0..idle {
        idlers.push(handshake(address, &format!("idle-{n}")).await);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let offered = Arc::new(AtomicU64::new(0));
    let delivered = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();

    // One per pair, so a paced publisher waits on its own subscriber
    // rather than on the slowest in the run.
    let progress: Vec<(Arc<AtomicU64>, Arc<AtomicU64>)> = (0..publishers)
        .map(|_| (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0))))
        .collect();

    // Subscribers first, and each is acknowledged before any publisher
    // starts, or the first part of the window would be measuring a
    // publication nobody yet wants.
    for (n, (_, received)) in progress.iter().enumerate() {
        let (ready, is_ready) = tokio::sync::oneshot::channel();
        let topic = format!("bench/{n}");
        let (stop, delivered) = (Arc::clone(&stop), Arc::clone(&delivered));
        let mine = Arc::clone(received);
        tasks.push(tokio::spawn(async move {
            let (mut stream, mut buf) = handshake(address, &format!("sub-{n}")).await;
            let out = bytes_of(&subscribe_packet(&topic, QoS::AtMostOnce));
            stream.write_all(&out).await.expect("the write succeeds");
            match next(&mut stream, &mut buf).await {
                Some(Packet::SubAck(ack)) => assert_eq!(ack.reasons, vec![ReasonCode::Success]),
                other => panic!("expected a subscribe acknowledgement, got {other:?}"),
            }
            ready.send(()).expect("the measurement is still waiting");

            let mut chunk = vec![0u8; 64 * 1024];
            while !stop.load(Ordering::Relaxed) {
                // A timeout rather than a plain read, so the task
                // notices the stop flag even if the flow has ended.
                let read =
                    match tokio::time::timeout(Duration::from_millis(50), stream.read(&mut chunk))
                        .await
                    {
                        Ok(Ok(0)) | Err(_) => continue,
                        Ok(Ok(read)) => read,
                        Ok(Err(_)) => break,
                    };
                buf.extend_from_slice(&chunk[..read]);
                let mut seen = 0u64;
                while let Some(packet) =
                    decode(&mut buf, Version::V5, &Limits::default()).expect("a valid packet")
                {
                    if matches!(packet, Packet::Publish(_)) {
                        seen += 1;
                    }
                }
                delivered.fetch_add(seen, Ordering::Relaxed);
                mine.fetch_add(seen, Ordering::Relaxed);
            }
        }));
        is_ready.await.expect("the subscriber subscribed");
    }

    for (n, (sent, received)) in progress.iter().enumerate() {
        let topic = format!("bench/{n}");
        let (stop, offered) = (Arc::clone(&stop), Arc::clone(&offered));
        let (sent, received) = (Arc::clone(sent), Arc::clone(received));
        tasks.push(tokio::spawn(async move {
            let (mut stream, _) = handshake(address, &format!("pub-{n}")).await;
            let mut batch = BytesMut::new();
            for _ in 0..BATCH {
                encode(&publish_packet(&topic), Version::V5, &mut batch).expect("it encodes");
            }
            let batch = batch.freeze();
            while !stop.load(Ordering::Relaxed) {
                if let Load::Paced(credit) = load {
                    // Spin on the pair's own counters rather than on a
                    // timer: a sleep here would set the rate instead of
                    // measuring it. The yield is what keeps the wait
                    // from starving the runtime's other workers.
                    while sent.load(Ordering::Relaxed) - received.load(Ordering::Relaxed) > credit {
                        if stop.load(Ordering::Relaxed) {
                            return;
                        }
                        tokio::task::yield_now().await;
                    }
                }
                if stream.write_all(&batch).await.is_err() {
                    break;
                }
                sent.fetch_add(BATCH as u64, Ordering::Relaxed);
                offered.fetch_add(BATCH as u64, Ordering::Relaxed);
            }
        }));
    }

    tokio::time::sleep(WARM_UP).await;
    let start = Instant::now();
    let (offered_before, delivered_before) = (
        offered.load(Ordering::Relaxed),
        delivered.load(Ordering::Relaxed),
    );
    let dropped_before = running.shared().dropped_for_slow_reader();

    tokio::time::sleep(WINDOW).await;

    let elapsed = start.elapsed();
    let point = Point {
        offered: offered.load(Ordering::Relaxed) - offered_before,
        delivered: delivered.load(Ordering::Relaxed) - delivered_before,
        dropped: running.shared().dropped_for_slow_reader() - dropped_before,
        elapsed,
    };

    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    drop(idlers);
    running.shutdown().await;
    point
}

fn print_curve(heading: &str, rows: &[(usize, Point)]) {
    println!();
    println!("{heading}");
    println!(
        "{:>10}  {:>14}  {:>14}  {:>12}  {:>10}",
        "publishers", "offered msg/s", "delivered msg/s", "per publisher", "dropped"
    );
    for (publishers, point) in rows {
        println!(
            "{:>10}  {:>14.0}  {:>14.0}  {:>12.0}  {:>10}",
            publishers,
            point.offered_per_second(),
            point.delivered_per_second(),
            point.delivered_per_second() / *publishers as f64,
            point.dropped,
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not an assertion: run it deliberately with --nocapture"]
async fn the_publisher_curve() {
    println!("machine: {}", machine());

    for load in [Load::Saturating, Load::Paced(CREDIT)] {
        for idle in [0, IDLE] {
            let mut rows = Vec::new();
            for publishers in PUBLISHERS {
                rows.push((
                    publishers,
                    measure(publishers, idle, load, Store::Absent).await,
                ));
            }
            print_curve(
                &format!("{} pairs over TCP, {idle} idle connections", load.label()),
                &rows,
            );
        }
    }
}

/// The same shape with no sockets: `Shared`, `handle` and `deliver`
/// exactly as the connection calls them, and nothing else.
///
/// Two columns, because one of them on its own proves nothing.
///
/// - **shared** puts every caller behind one `Shared`, which is what
///   the server does today.
/// - **sharded** gives every caller its own `Shared`, so the work, the
///   task count and the runtime are identical and the only difference
///   is whether the mutex is contended.
///
/// Without the second column a falling first column could be blamed on
/// the runtime scheduling more tasks. With it, the fall has one
/// remaining explanation.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not an assertion: run it deliberately with --nocapture"]
async fn the_lock_without_any_sockets() {
    println!("machine: {}", machine());
    println!();
    println!("the same publisher counts with no sockets at all");
    println!(
        "{:>10}  {:>14}  {:>14}  {:>10}",
        "publishers", "shared msg/s", "sharded msg/s", "ratio"
    );

    for publishers in PUBLISHERS {
        let shared = in_process(publishers, true).await;
        let sharded = in_process(publishers, false).await;
        println!(
            "{:>10}  {:>14.0}  {:>14.0}  {:>10.2}",
            publishers,
            shared,
            sharded,
            sharded / shared
        );
    }
}

/// One publisher and one subscriber per caller, no sockets, for a
/// window. `one_broker` decides whether they contend.
async fn in_process(publishers: usize, one_broker: bool) -> f64 {
    let stop = Arc::new(AtomicBool::new(false));
    let handled = Arc::new(AtomicU64::new(0));
    let mut drains = Vec::new();
    let mut tasks = Vec::new();

    // Kept alive for the whole run: dropping a `Shared` would take its
    // sessions with it.
    let mut brokers: Vec<Arc<Shared>> = Vec::new();
    if one_broker {
        brokers.push(Arc::new(Shared::new(BrokerConfig::default())));
    } else {
        for _ in 0..publishers {
            brokers.push(Arc::new(Shared::new(BrokerConfig::default())));
        }
    }

    for n in 0..publishers {
        let shared = Arc::clone(&brokers[if one_broker { 0 } else { n }]);
        let topic = format!("bench/{n}");

        let subscriber = shared.with_broker(|broker| {
            let mut out = Vec::new();
            let Packet::Connect(connect) = connect_packet(&format!("sub-{n}")) else {
                unreachable!()
            };
            let registration = broker
                .connect("", &connect, &mut out)
                .expect("the default decision authenticates");
            broker
                .handle(
                    registration.subscriber,
                    &subscribe_packet(&topic, QoS::AtMostOnce),
                    &mut out,
                )
                .expect("the subscribe is accepted");
            registration.subscriber
        });
        let mut inbox = shared.register_outbox(subscriber, 1024);
        let stop_drain = Arc::clone(&stop);
        drains.push(tokio::spawn(async move {
            // The writer task's job, minus the socket. Without it the
            // outbox fills and `try_send` starts failing, which is
            // cheaper than succeeding and would flatter the figure.
            while !stop_drain.load(Ordering::Relaxed) {
                let _ = tokio::time::timeout(Duration::from_millis(50), inbox.recv()).await;
            }
        }));

        let publisher = shared.with_broker(|broker| {
            let mut out = Vec::new();
            let Packet::Connect(connect) = connect_packet(&format!("pub-{n}")) else {
                unreachable!()
            };
            broker
                .connect("", &connect, &mut out)
                .expect("the default decision authenticates")
                .subscriber
        });

        let (stop, handled) = (Arc::clone(&stop), Arc::clone(&handled));
        tasks.push(tokio::spawn(async move {
            let packet = publish_packet(&topic);
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..BATCH {
                    let actions = shared.with_broker(|broker| {
                        let mut out = Vec::new();
                        let _ = broker.handle(publisher, &packet, &mut out);
                        out
                    });
                    shared.deliver(actions);
                }
                handled.fetch_add(BATCH as u64, Ordering::Relaxed);
                // These tasks never await otherwise, so without this
                // they would starve the runtime's other workers and the
                // figure would be of a scheduler, not a lock.
                tokio::task::yield_now().await;
            }
        }));
    }

    tokio::time::sleep(WARM_UP).await;
    let before = handled.load(Ordering::Relaxed);
    let start = Instant::now();
    tokio::time::sleep(WINDOW).await;
    let rate = (handled.load(Ordering::Relaxed) - before) as f64 / start.elapsed().as_secs_f64();

    stop.store(true, Ordering::Relaxed);
    for task in tasks.into_iter().chain(drains) {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    rate
}

/// What one tick costs when the registry is full.
///
/// The tick is the one thing in the process that takes the lock on
/// behalf of every session at once, so its cost is the length of the
/// pause every connection sees once a second.
#[test]
#[ignore = "a measurement, not an assertion: run it deliberately with --nocapture"]
fn one_tick_with_ten_thousand_sessions() {
    const SESSIONS: usize = 10_000;
    const TICKS: usize = 50;

    println!("machine: {}", machine());

    let mut broker = Broker::new(BrokerConfig::default());
    for n in 0..SESSIONS {
        let mut out = Vec::new();
        let Packet::Connect(connect) = connect_packet(&format!("session-{n}")) else {
            unreachable!()
        };
        let registration = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");
        broker
            .handle(
                registration.subscriber,
                &subscribe_packet(
                    &format!("site/{}/device/{n}/telemetry", n % 100),
                    QoS::AtMostOnce,
                ),
                &mut out,
            )
            .expect("the subscribe is accepted");
    }
    assert_eq!(broker.session_count(), SESSIONS);

    let mut timings = Vec::with_capacity(TICKS);
    for _ in 0..TICKS {
        let mut out = Vec::new();
        let start = Instant::now();
        broker.tick(&mut out);
        timings.push(start.elapsed());
    }
    timings.sort_unstable();

    println!();
    println!("one tick with {SESSIONS} sessions, {TICKS} ticks");
    println!("  min    {:>10.1} us", timings[0].as_secs_f64() * 1e6);
    println!(
        "  median {:>10.1} us",
        timings[TICKS / 2].as_secs_f64() * 1e6
    );
    println!(
        "  max    {:>10.1} us",
        timings[TICKS - 1].as_secs_f64() * 1e6
    );
    // The figure that matters for the lock: a tick this long is a pause
    // of that length for every connection in the process.
    println!("  sessions still registered: {}", broker.session_count());
}

// ---- what a store costs -------------------------------------------------

/// A publish that must be acknowledged, and the identifier it is
/// acknowledged under.
fn publish_at_least_once(topic: &str, packet_id: u16) -> Packet {
    Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: topic.into(),
        packet_id: Some(packet_id),
        payload: Bytes::from_static(PAYLOAD),
        properties: Properties::default(),
    })
}

/// A PUBACK for a delivery, which is what keeps a subscriber's window
/// from filling.
fn puback(packet_id: u16) -> Packet {
    Packet::PubAck(Ack {
        packet_id,
        reason: ReasonCode::Success,
        properties: Properties::default(),
    })
}

/// Read until one packet is whole, treating every ending as an ending.
///
/// [`next`] panics on a broken socket, which is right during a
/// handshake and wrong in a loop that is still running when the server
/// is shut down: every publisher and subscriber would panic on the way
/// out and bury the table in backtraces.
async fn try_next(stream: &mut TcpStream, buf: &mut BytesMut) -> Option<Packet> {
    loop {
        if let Some(packet) = decode(buf, Version::V5, &Limits::default()).ok()? {
            return Some(packet);
        }
        let mut chunk = [0u8; 4096];
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..read]);
    }
}

/// One measured point on the acknowledged curve.
struct AckPoint {
    /// Publications the broker acknowledged during the window.
    acknowledged: u64,
    /// Deliveries that reached a subscriber during the window.
    delivered: u64,
    /// How long the window actually was.
    elapsed: Duration,
}

impl AckPoint {
    fn acknowledged_per_second(&self) -> f64 {
        self.acknowledged as f64 / self.elapsed.as_secs_f64()
    }

    fn delivered_per_second(&self) -> f64 {
        self.delivered as f64 / self.elapsed.as_secs_f64()
    }
}

/// Run `publishers` at-least-once pairs and report the acknowledged
/// rate.
///
/// **One publication outstanding per publisher**, deliberately. The
/// storage crate's curve is in concurrent waiters, and a publisher that
/// pipelined would be several of them at once, so the publisher count
/// here would no longer be the waiter count and the two tables could
/// not be read against each other. It is also what a client that cares
/// about the acknowledgement does.
///
/// The subscriber acknowledges every delivery it receives. Without that
/// its inflight window fills, deliveries start queueing, and the row
/// would measure a queue rather than a round trip.
async fn measure_acknowledged(publishers: usize, store: Store) -> AckPoint {
    let config = ServerConfig::ephemeral();
    let (shared, _directory) = shared_for(store, &config);
    let running = Server::bind_shared(config, shared)
        .await
        .expect("the server binds");
    let address = running.address();

    let stop = Arc::new(AtomicBool::new(false));
    let acknowledged = Arc::new(AtomicU64::new(0));
    let delivered = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();

    for n in 0..publishers {
        let (ready, is_ready) = tokio::sync::oneshot::channel();
        let topic = format!("bench/{n}");
        let (stop_sub, delivered_sub) = (Arc::clone(&stop), Arc::clone(&delivered));
        tasks.push(tokio::spawn(async move {
            let (mut stream, mut buf) = handshake(address, &format!("sub-{n}")).await;
            let out = bytes_of(&subscribe_packet(&topic, QoS::AtLeastOnce));
            stream.write_all(&out).await.expect("the write succeeds");
            match next(&mut stream, &mut buf).await {
                Some(Packet::SubAck(ack)) => assert_eq!(ack.reasons, vec![ReasonCode::GrantedQos1]),
                other => panic!("expected a subscribe acknowledgement, got {other:?}"),
            }
            ready.send(()).expect("the measurement is still waiting");

            let mut chunk = vec![0u8; 64 * 1024];
            while !stop_sub.load(Ordering::Relaxed) {
                let read =
                    match tokio::time::timeout(Duration::from_millis(50), stream.read(&mut chunk))
                        .await
                    {
                        Ok(Ok(0)) | Err(_) => continue,
                        Ok(Ok(read)) => read,
                        Ok(Err(_)) => break,
                    };
                buf.extend_from_slice(&chunk[..read]);
                let mut seen = 0u64;
                let mut acks = BytesMut::new();
                while let Ok(Some(packet)) = decode(&mut buf, Version::V5, &Limits::default()) {
                    if let Packet::Publish(publish) = packet {
                        seen += 1;
                        if let Some(packet_id) = publish.packet_id {
                            encode(&puback(packet_id), Version::V5, &mut acks)
                                .expect("the packet encodes");
                        }
                    }
                }
                if !acks.is_empty() && stream.write_all(&acks).await.is_err() {
                    break;
                }
                delivered_sub.fetch_add(seen, Ordering::Relaxed);
            }
        }));
        is_ready.await.expect("the subscriber subscribed");
    }

    for n in 0..publishers {
        let topic = format!("bench/{n}");
        let (stop_pub, acknowledged_pub) = (Arc::clone(&stop), Arc::clone(&acknowledged));
        tasks.push(tokio::spawn(async move {
            let (mut stream, mut buf) = handshake(address, &format!("pub-{n}")).await;
            let mut packet_id = 0u16;
            while !stop_pub.load(Ordering::Relaxed) {
                // Never zero, which the codec refuses, and wrapping
                // rather than growing: a three-second window at any
                // rate this broker reaches goes round several times.
                packet_id = packet_id % 65_535 + 1;
                let out = bytes_of(&publish_at_least_once(&topic, packet_id));
                if stream.write_all(&out).await.is_err() {
                    return;
                }
                loop {
                    // A timeout rather than a plain read, so a
                    // publisher waiting on an acknowledgement that will
                    // never come still notices the stop flag. It is far
                    // longer than any acknowledgement this measures,
                    // including the one-waiter case at a few
                    // milliseconds a message.
                    match tokio::time::timeout(
                        Duration::from_millis(500),
                        try_next(&mut stream, &mut buf),
                    )
                    .await
                    {
                        Ok(Some(Packet::PubAck(ack))) => {
                            assert_eq!(
                                ack.packet_id, packet_id,
                                "an acknowledgement for a publication that was not outstanding"
                            );
                            acknowledged_pub.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        // Anything else on the socket belongs to some
                        // other exchange; this publisher subscribes to
                        // nothing, so in practice there is nothing else.
                        Ok(Some(_)) => continue,
                        Ok(None) => return,
                        Err(_) => {
                            if stop_pub.load(Ordering::Relaxed) {
                                return;
                            }
                        }
                    }
                }
            }
        }));
    }

    tokio::time::sleep(WARM_UP).await;
    let start = Instant::now();
    let (acknowledged_before, delivered_before) = (
        acknowledged.load(Ordering::Relaxed),
        delivered.load(Ordering::Relaxed),
    );
    tokio::time::sleep(WINDOW).await;
    let point = AckPoint {
        acknowledged: acknowledged.load(Ordering::Relaxed) - acknowledged_before,
        delivered: delivered.load(Ordering::Relaxed) - delivered_before,
        elapsed: start.elapsed(),
    };

    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
    running.shutdown().await;
    point
}

/// The publisher counts on the acknowledged curve.
///
/// The first five are the at-most-once curve's, so the two can be read
/// against each other. The hundred is the storage crate's: it measured
/// 334 durable writes a second with one waiter and 25,100 with a
/// hundred, and a prediction that far apart at its ends is worth
/// testing at both.
const WAITERS: [usize; 6] = [1, 2, 4, 8, 16, 100];

/// What the store costs the broker, at both qualities of service.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not an assertion: run it deliberately with --nocapture"]
async fn what_a_store_costs() {
    println!("machine: {}", machine());

    println!();
    println!("at most once, paced pairs over TCP, no idle connections");
    println!(
        "{:>10}  {:>10}  {:>14}  {:>14}  {:>12}  {:>10}",
        "publishers", "store", "offered msg/s", "delivered msg/s", "per publisher", "dropped"
    );
    for publishers in PUBLISHERS {
        for store in [Store::Absent, Store::Redb] {
            let point = measure(publishers, 0, Load::Paced(CREDIT), store).await;
            println!(
                "{:>10}  {:>10}  {:>14.0}  {:>14.0}  {:>12.0}  {:>10}",
                publishers,
                store.label(),
                point.offered_per_second(),
                point.delivered_per_second(),
                point.delivered_per_second() / publishers as f64,
                point.dropped,
            );
        }
    }

    println!();
    println!("at least once, one publication outstanding per publisher");
    println!(
        "{:>10}  {:>10}  {:>16}  {:>16}  {:>12}",
        "publishers", "store", "acknowledged /s", "delivered /s", "per publisher"
    );
    for publishers in WAITERS {
        for store in [Store::Absent, Store::Redb] {
            let point = measure_acknowledged(publishers, store).await;
            println!(
                "{:>10}  {:>10}  {:>16.0}  {:>16.0}  {:>12.0}",
                publishers,
                store.label(),
                point.acknowledged_per_second(),
                point.delivered_per_second(),
                point.acknowledged_per_second() / publishers as f64,
            );
        }
    }
}

// ---- what the fan-out re-encodes ----------------------------------------

/// How many subscriptions one message matches, in the fan-out table.
const FAN_OUT: [usize; 4] = [1, 10, 100, 1_000];

/// How many messages each fan-out point publishes.
///
/// Comfortably below the queue's thousand-message bound, so every one
/// of them is accepted and journalled rather than refused at a bound,
/// which would measure a different path.
const FAN_OUT_MESSAGES: usize = 200;

/// A property block of the kind a real publish carries.
///
/// Every one of these is permitted on a PUBLISH, and together they are
/// about a hundred bytes: enough that encoding them is not free, and
/// not so much that the row is about an unrealistic message.
fn realistic_properties() -> Properties {
    Properties {
        payload_format_indicator: Some(1),
        message_expiry_interval: Some(3_600),
        content_type: Some("application/json".to_owned()),
        response_topic: Some("bench/reply".to_owned()),
        correlation_data: Some(Bytes::from_static(b"0123456789abcdef")),
        user_properties: vec![
            ("tenant".to_owned(), "acme".to_owned()),
            ("device".to_owned(), "sensor-01".to_owned()),
        ],
        ..Properties::default()
    }
}

/// A connect whose session outlives the connection, so that a delivery
/// to it queues.
fn durable_connect(client_id: &str) -> Packet {
    Packet::Connect(Box::new(Connect {
        version: Version::V5,
        client_id: client_id.into(),
        clean_start: false,
        keep_alive: 0,
        username: None,
        password: None,
        will: None,
        properties: Properties {
            session_expiry_interval: Some(3_600),
            ..Properties::default()
        },
    }))
}

/// Publish to `subscribers` offline durable sessions and report what
/// one publish cost and how many records it produced.
///
/// Offline on purpose: an offline session queues what it is offered, so
/// every subscriber journals a `BodyPut` and a `QueuePushed`, which is
/// the fan-out this is about.
///
/// Each message gets its own property block, exactly as
/// `Broker::handle` gives an arriving publish its own. Sharing one
/// across the two hundred would let a cache none of them has in
/// reality answer for all of them, and the row would be a measurement
/// of the harness.
///
/// No store, no sockets and no shell: the question is what
/// `Broker::publish` spends, and a device underneath it would bury the
/// difference being looked for.
fn fan_out(subscribers: usize, properties: &Properties) -> (Duration, usize) {
    let mut broker = Broker::new(BrokerConfig::default());
    let mut out = Vec::new();
    for n in 0..subscribers {
        let Packet::Connect(connect) = durable_connect(&format!("fan-{n}")) else {
            unreachable!()
        };
        let registration = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");
        broker
            .handle(
                registration.subscriber,
                &subscribe_packet("fan/out", QoS::AtLeastOnce),
                &mut out,
            )
            .expect("the subscribe is accepted");
        broker.disconnect(registration.subscriber, &mut out);
        out.clear();
    }
    // Everything above is setup, and what it journalled is not part of
    // what a publish costs.
    broker.discard_journal();

    let now = broker.clock().now();
    let mut records = Vec::new();
    let mut produced = 0usize;
    let start = Instant::now();
    for _ in 0..FAN_OUT_MESSAGES {
        let mut message = Message::new(
            "fan/out",
            Bytes::from_static(PAYLOAD),
            QoS::AtLeastOnce,
            false,
        );
        // What `Broker::handle` does to an arriving publish.
        message.properties = Arc::new(properties.clone().into());
        broker.publish(message, None, now, &mut out);
        // The shell drains the journal under the same lock that filled
        // it, so the drain is part of what a publish costs.
        broker.take_journal(&mut records);
        produced += records.len();
        records.clear();
        out.clear();
    }
    let elapsed = start.elapsed();
    assert_eq!(
        produced,
        // A `BodyPut` and a `QueuePushed` each, every time.
        2 * subscribers * FAN_OUT_MESSAGES,
        "every subscriber journals the body and the queue push, and a \
         fan-out that stopped doing so is measuring something else"
    );
    (
        elapsed / FAN_OUT_MESSAGES as u32,
        produced / FAN_OUT_MESSAGES,
    )
}

/// What one property block costs to encode, on its own.
fn one_encode(properties: &Properties) -> Duration {
    const ENCODES: u32 = 200_000;
    let start = Instant::now();
    for _ in 0..ENCODES {
        std::hint::black_box(journal::stored_properties(std::hint::black_box(properties)));
    }
    start.elapsed() / ENCODES
}

/// What carrying a property block costs a fan-out.
///
/// The decisive comparison is between the two property rows at the
/// same fan-out. Everything else about the publish is identical, so
/// the difference per subscriber is what the block costs, and the
/// encode measured on its own says whether that difference is the
/// encode or something else that grew with it.
///
/// This is the measurement `SharedProperties` was written from, and
/// the reason it exists rather than being argued for: twice on this
/// project the obvious optimisation was slower when measured. Before
/// it, on this machine, a fan-out of a thousand cost 647 ns per
/// subscriber with a realistic 95-byte block against 433 ns with an
/// empty one, and a fan-out of a hundred 465 against 262 — the same
/// block encoded once per subscriber, into records the store collapses
/// to one body. After it the two rows meet, at a fan-out of one
/// nothing changed, and nothing in between got worse.
#[test]
#[ignore = "a measurement, not an assertion: run it deliberately with --nocapture"]
fn what_a_property_block_costs_a_fan_out() {
    println!("machine: {}", machine());

    let empty = Properties::default();
    let realistic = realistic_properties();
    println!();
    println!(
        "one property block encoded on its own: {:.0} ns empty, {:.0} ns realistic ({} bytes)",
        one_encode(&empty).as_secs_f64() * 1e9,
        one_encode(&realistic).as_secs_f64() * 1e9,
        journal::stored_properties(&realistic).len(),
    );

    println!();
    println!("one publish to a fan-out of offline durable sessions");
    println!(
        "{:>8}  {:>10}  {:>14}  {:>16}  {:>16}  {:>12}",
        "fan-out", "properties", "records/publish", "us/publish", "ns/subscriber", "publishes/s"
    );
    for subscribers in FAN_OUT {
        for (label, properties) in [("empty", &empty), ("realistic", &realistic)] {
            let (per_publish, records) = fan_out(subscribers, properties);
            println!(
                "{:>8}  {:>10}  {:>14}  {:>16.1}  {:>16.0}  {:>12.0}",
                subscribers,
                label,
                records,
                per_publish.as_secs_f64() * 1e6,
                per_publish.as_secs_f64() * 1e9 / subscribers as f64,
                1.0 / per_publish.as_secs_f64(),
            );
        }
    }
}

/// Where the numbers came from. A throughput figure without this is
/// not reproducible.
fn machine() -> String {
    let profile = if cfg!(debug_assertions) {
        "debug (figures are not meaningful)"
    } else {
        "release"
    };
    format!(
        "{} {}, {} logical cores, {} build",
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        profile,
    )
}
