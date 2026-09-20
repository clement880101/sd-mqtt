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

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use broker_core::broker::{Broker, BrokerConfig};
use bytes::{Bytes, BytesMut};
use mqtt_codec::packet::{Connect, Publish, RetainHandling, Subscribe, SubscriptionOptions};
use mqtt_codec::{decode, encode, Limits, Packet, Properties, QoS, ReasonCode, Version};
use sdmqtt_server::listener::{Server, ServerConfig};
use sdmqtt_server::shared::Shared;
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

fn subscribe_packet(topic: &str) -> Packet {
    Packet::Subscribe(Subscribe {
        packet_id: 1,
        filters: vec![SubscriptionOptions {
            filter: topic.into(),
            qos: QoS::AtMostOnce,
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

/// Run `publishers` publisher/subscriber pairs, with `idle` connections
/// attached and silent, and report the steady-state rate.
async fn measure(publishers: usize, idle: usize, load: Load) -> Point {
    let running = Server::bind(ServerConfig::ephemeral())
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
            let out = bytes_of(&subscribe_packet(&topic));
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
                rows.push((publishers, measure(publishers, idle, load).await));
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
            let registration = broker.connect("", &connect, &mut out);
            broker
                .handle(registration.subscriber, &subscribe_packet(&topic), &mut out)
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
            broker.connect("", &connect, &mut out).subscriber
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
        let registration = broker.connect("", &connect, &mut out);
        broker
            .handle(
                registration.subscriber,
                &subscribe_packet(&format!("site/{}/device/{n}/telemetry", n % 100)),
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
