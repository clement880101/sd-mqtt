//! What durability costs, and what a large payload costs.
//!
//! Two numbers section 17 of the design records as unknown.
//!
//! - **Durable acknowledged writes a second.** Section 7.3 sends an
//!   acknowledgement only after the commit is durable, so this is the
//!   ceiling on the broker's acknowledged delivery rate whatever the rest
//!   of it can do. Nothing had measured it.
//! - **What a large payload costs.** redb stores values in the tree, so a
//!   multi-megabyte body sits among the pages that split and merge as
//!   other keys are inserted, where an engine with a separate value log
//!   would have written it once. Section 17 records the consequence —
//!   keeping large bodies in files beside the store — as the change of
//!   design this figure decides.
//!
//! # Why not criterion
//!
//! The same reasons as `sdmqtt-server`'s throughput harness, which this
//! follows: the quantity is an aggregate rate over a window with a fixed
//! setup cost, not the latency of one call that can be run a million
//! times. So these are ignored tests that print a table, run deliberately
//! and read by a person:
//!
//! ```text
//! cargo test -p storage --release --test throughput -- \
//!     --ignored --nocapture --test-threads 1
//! ```
//!
//! `--release` matters: a debug build measures postcard's bounds checks
//! alongside the disk. `--test-threads 1` matters twice over here,
//! because two of these at once share one device as well as one machine.
//!
//! # What each figure can and cannot see
//!
//! The write rate is what the writer's coalescing achieves against this
//! device's `fsync`. It is not a promise about any other device: an
//! `fsync` on the laptop SSD this was written on is not an `fsync` on a
//! network volume, and the ratio between the coalesced and uncoalesced
//! rows travels better than either row does.
//!
//! The large-payload figure is **space** amplification: bytes the file
//! grew per byte of payload stored. It is not write amplification, which
//! is what a copy-on-write engine actually spends rewriting pages, and
//! which is necessarily larger. Counting device writes needs a tool this
//! test cannot call, and the plan for this task says the file size is
//! decisive enough — a store that grows three times its payload has
//! already answered the question, and one that grows by a tenth over its
//! payload has too. The wall-clock rate beside it is the second half of
//! the answer: an engine that rewrote a megabyte many times over would
//! show it there.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use storage::{
    BodyRecord, DeliveryRecord, MessageId, ProtocolVersion, Qos, QueueEntry, Record, RedbStorage,
    RetainedRecord, SessionKey, SessionRecord, Storage, SyncMode, Writer, WriterConfig,
};

/// How long the load runs before anything is counted.
const WARM_UP: Duration = Duration::from_secs(1);

/// The window the rate is measured over.
const WINDOW: Duration = Duration::from_secs(3);

/// The concurrent waiter counts in the curve.
///
/// One is the uncoalesced rate: nothing else is ever queued behind the
/// flush, so each waiter pays for a whole `fsync` of its own. Ten and a
/// hundred are what coalescing is for.
const WAITERS: [usize; 3] = [1, 10, 100];

/// A payload the size of a small telemetry reading, as the listener's
/// harness uses, so the two tables are about the same message.
const PAYLOAD: &[u8] = b"0123456789abcdef0123456789abcdef";

/// The body sizes the second table walks, and how many of each to store.
///
/// The counts hold the total payload near 32 MiB at every size, so the
/// ratios are comparable and none of them is dominated by the empty
/// store's own pages.
///
/// **That total is load-bearing.** The ratio is stable in the count once
/// the store is large enough — 1 KiB bodies gave 1.88, 1.98 and 2.00 at
/// 8, 32 and 64 MiB of payload, and 1 MiB bodies gave 2.12, 2.03 and
/// 2.02 — but at 8 MiB of 64 KiB bodies it read 3.89, which is the
/// file's own growth granularity rather than anything about the payload.
/// A row measured on a store of a few megabytes would say more about
/// redb's allocator than about the question being asked.
///
/// The last size is the configured maximum packet size,
/// `mqtt_codec::Limits::default().max_packet_size`, repeated here as a
/// literal because this crate does not depend on the codec. A body at
/// that size is marginally larger than any real one, since the limit is
/// on the whole packet, header included.
const SIZES: [(usize, usize); 4] = [
    (1_024, 32_768),
    (64 * 1_024, 512),
    (1_024 * 1_024, 32),
    (2_000_000, 16),
];

// ---- durable acknowledged writes a second -------------------------------

/// What one window saw.
struct Point {
    /// Durable writes that completed.
    completed: u64,
    /// Writes that came back not durable, which must be zero.
    failed: u64,
    /// How long the window actually was.
    elapsed: Duration,
}

impl Point {
    /// Completions a second.
    fn per_second(&self) -> f64 {
        self.completed as f64 / self.elapsed.as_secs_f64()
    }
}

/// One acknowledged write.
///
/// A retained publish: one record, and it overwrites its own topic, so a
/// three-second run at any rate leaves a store of a few rows rather than
/// a store whose growth is what is being measured. Section 7.3's
/// acknowledged path journals more than this per message, but the
/// quantity here is durable commits a second and the flush is what it
/// costs; the record is small on purpose so that nothing else is in the
/// figure.
fn one_write(waiter: usize) -> Record {
    Record::RetainedPut {
        topic: format!("bench/{waiter}"),
        message: RetainedRecord {
            payload: bytes::Bytes::from_static(PAYLOAD),
            qos: Qos::AtLeastOnce,
            expires_at: None,
            properties: bytes::Bytes::new(),
        },
    }
}

/// Run `waiters` tasks, each waiting for its own write to be durable
/// before starting the next, and count what completes in the window.
async fn measure(waiters: usize, config: WriterConfig) -> Point {
    let directory = tempfile::TempDir::new().expect("a temporary directory");
    let store = RedbStorage::open_with(directory.path().join("store.redb"), SyncMode::Immediate)
        .expect("a store opens");
    let writer =
        Arc::new(Writer::spawn(Arc::new(store), config).expect("the writer gets a thread"));

    let completed = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let mut tasks = Vec::with_capacity(waiters);
    for waiter in 0..waiters {
        let writer = Arc::clone(&writer);
        let completed = Arc::clone(&completed);
        let failed = Arc::clone(&failed);
        let stop = Arc::clone(&stop);
        tasks.push(tokio::spawn(async move {
            let record = one_write(waiter);
            while !stop.load(Ordering::Relaxed) {
                let durable = writer
                    .apply_durable(vec![record.clone()])
                    .expect("the writer is running");
                match durable.await {
                    Ok(()) => completed.fetch_add(1, Ordering::Relaxed),
                    Err(_) => failed.fetch_add(1, Ordering::Relaxed),
                };
            }
        }));
    }

    tokio::time::sleep(WARM_UP).await;
    let start = Instant::now();
    let (completed_before, failed_before) = (
        completed.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed),
    );
    tokio::time::sleep(WINDOW).await;
    let point = Point {
        completed: completed.load(Ordering::Relaxed) - completed_before,
        failed: failed.load(Ordering::Relaxed) - failed_before,
        elapsed: start.elapsed(),
    };

    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
    }
    point
}

/// The ceiling on acknowledged delivery.
///
/// Two curves, because one of them alone says nothing. The first is the
/// writer as the broker configures it, coalescing up to a thousand
/// records. The second flushes every batch on its own, which is the rate
/// without coalescing, and is the control that says how much of the first
/// curve is the writer and how much is the device.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not an assertion: run it deliberately with --nocapture"]
async fn durable_acknowledged_writes_a_second() {
    println!("machine: {}", machine());
    println!();
    println!("durable acknowledged writes a second, redb, sync mode immediate");
    println!(
        "{:>8}  {:>16}  {:>16}  {:>12}  {:>8}",
        "waiters", "coalesced /s", "uncoalesced /s", "per waiter", "failed"
    );

    for waiters in WAITERS {
        let coalesced = measure(waiters, WriterConfig::default()).await;
        // A count trigger of zero flushes every batch on its own, which
        // is what `WriterConfig::records` documents as the uncoalesced
        // rate and the only thing it is useful for.
        let uncoalesced = measure(
            waiters,
            WriterConfig {
                records: 0,
                ..WriterConfig::default()
            },
        )
        .await;
        println!(
            "{:>8}  {:>16.0}  {:>16.0}  {:>12.0}  {:>8}",
            waiters,
            coalesced.per_second(),
            uncoalesced.per_second(),
            coalesced.per_second() / waiters as f64,
            coalesced.failed + uncoalesced.failed,
        );
    }
}

// ---- what a large payload costs -----------------------------------------

/// Store `count` bodies of `size` bytes each, and say what the file did.
///
/// One batch per body, which is what the broker does: a publish is a
/// batch. The identifiers are spread across the key space rather than
/// laid down in order, because a real [`MessageId`] is a UUID and an
/// ascending key is the one insertion order a B-tree never has to split
/// in the middle for.
fn store_bodies(size: usize, count: usize) -> (u64, u64, Duration) {
    let directory = tempfile::TempDir::new().expect("a temporary directory");
    let path = directory.path().join("store.redb");

    // The baseline is taken with the file closed, so that it is the same
    // kind of measurement as the one after.
    {
        let store = RedbStorage::open(&path).expect("a store opens");
        store.flush().expect("an empty flush succeeds");
    }
    let before = std::fs::metadata(&path).expect("the file exists").len();

    let payload = bytes::Bytes::from(vec![0xa5u8; size]);
    let elapsed = {
        let store = RedbStorage::open(&path).expect("the store opens again");
        let start = Instant::now();
        for n in 0..count {
            let body = BodyRecord {
                topic: "sensor/large".to_owned(),
                payload: payload.clone(),
                qos: Qos::AtLeastOnce,
                retain: false,
                expires_at: None,
                properties: bytes::Bytes::new(),
            };
            store
                .apply(&[Record::BodyPut {
                    id: scattered_id(n),
                    body,
                }])
                .expect("a body is storable");
        }
        store.flush().expect("the flush succeeds");
        start.elapsed()
    };
    let after = std::fs::metadata(&path).expect("the file exists").len();

    (before, after, elapsed)
}

/// An identifier whose bytes are spread across the key space.
///
/// A counter in the high bytes would insert in ascending order, which is
/// the friendliest possible order for a B-tree. A real [`MessageId`] is a
/// UUID, so this reverses the counter's bytes to get something that lands
/// all over the tree, which is what makes pages split and merge.
fn scattered_id(n: usize) -> MessageId {
    let mut bytes = [0u8; 16];
    let counter = (n as u64).to_be_bytes();
    for (index, byte) in counter.iter().enumerate() {
        bytes[index] = byte.reverse_bits();
        bytes[15 - index] = *byte;
    }
    MessageId(bytes)
}

/// What a large payload costs on disk.
#[test]
#[ignore = "a measurement, not an assertion: run it deliberately with --nocapture"]
fn what_a_large_payload_costs() {
    println!("machine: {}", machine());
    println!();
    println!("bytes written to disk per byte of payload, redb");
    println!(
        "{:>12}  {:>8}  {:>14}  {:>14}  {:>12}  {:>10}",
        "body", "bodies", "payload bytes", "file grew by", "bytes/byte", "MiB/s"
    );

    for (size, count) in SIZES {
        let (before, after, elapsed) = store_bodies(size, count);
        let payload = (size * count) as f64;
        let grew = after.saturating_sub(before) as f64;
        println!(
            "{:>12}  {:>8}  {:>14}  {:>14}  {:>12.2}  {:>10.1}",
            human(size),
            count,
            payload as u64,
            after.saturating_sub(before),
            grew / payload,
            payload / elapsed.as_secs_f64() / (1024.0 * 1024.0),
        );
    }
}

// ---- what a fan-out costs the storage thread ----------------------------

/// The fan-out rows: a payload size, and how many durable subscribers
/// one publication is queued for.
///
/// A thousand offline subscribers of a 256 KiB message is the shape
/// section 7.1's shared body was built for, and the size at which
/// rewriting the payload once per reference is worth a measurement. The
/// hundred beside it says whether the cost is linear in subscribers, and
/// the small payload says whether what is being measured is the payload
/// or the shape of the batch.
const FAN_OUT: [(usize, usize); 3] = [(256 * 1_024, 1_000), (256 * 1_024, 100), (256, 1_000)];

/// How many publications each row fans out, so that the figure is not
/// one sample.
const FAN_OUT_MESSAGES: usize = 8;

/// One publication queued for `sessions` durable offline subscribers, as
/// the broker emits it: one [`Record::BodyPut`] and one
/// [`Record::QueuePushed`] per session, **all in one batch**, which is
/// one write transaction.
///
/// That last part is the measurement. `tests/redb.rs` puts each
/// reference in a transaction of its own, which is not what a fan-out
/// does and is why the cost of one was never visible there.
fn fan_out(size: usize, sessions: usize, messages: usize) -> Duration {
    let directory = tempfile::TempDir::new().expect("a temporary directory");
    let store = RedbStorage::open_with(directory.path().join("store.redb"), SyncMode::Immediate)
        .expect("a store opens");

    let keys: Vec<SessionKey> = (0..sessions)
        .map(|n| SessionKey::new("", &format!("subscriber-{n}")))
        .collect();
    let sessions_batch: Vec<Record> = keys
        .iter()
        .map(|key| {
            Record::SessionPut(SessionRecord {
                key: key.clone(),
                version: ProtocolVersion::V5,
                expiry_interval: u32::MAX,
                expires_at: None,
                will: None,
                will_at: None,
            })
        })
        .collect();
    store
        .apply(&sessions_batch)
        .expect("the sessions are storable");
    store.flush().expect("the flush succeeds");

    let payload = bytes::Bytes::from(vec![0xa5u8; size]);
    let mut elapsed = Duration::ZERO;
    for message in 0..messages {
        let id = scattered_id(message);
        let body = BodyRecord {
            topic: "sensor/fan-out".to_owned(),
            payload: payload.clone(),
            qos: Qos::AtLeastOnce,
            retain: false,
            expires_at: None,
            properties: bytes::Bytes::new(),
        };
        let mut batch = Vec::with_capacity(sessions * 2);
        for key in &keys {
            batch.push(Record::BodyPut {
                id,
                body: body.clone(),
            });
            batch.push(Record::QueuePushed {
                session: key.clone(),
                entry: QueueEntry {
                    sequence: message as u64 + 1,
                    packet_id: None,
                    delivery: DeliveryRecord {
                        body: id,
                        qos: Qos::AtLeastOnce,
                        retain: false,
                        subscription_ids: Vec::new(),
                        dup: false,
                    },
                },
            });
        }
        // Only the apply is timed. Building the batch is the broker's
        // cost rather than the store's, and it is the same either way.
        let start = Instant::now();
        store.apply(&batch).expect("the fan-out is storable");
        elapsed += start.elapsed();
    }
    store.flush().expect("the flush succeeds");
    elapsed
}

/// What one publication to many durable subscribers costs the store.
///
/// Section 7.1 stores a body once and references it. That is a claim
/// about **space**, and the file-size table above is what measures it.
/// This is the claim beside it that nothing measured: what the
/// references *cost* in work, on the one storage thread, inside one
/// write transaction, with every waiter behind it blocked.
#[test]
#[ignore = "a measurement, not an assertion: run it deliberately with --nocapture"]
fn what_a_fan_out_costs() {
    println!("machine: {}", machine());
    println!();
    println!("one publication queued for many durable subscribers, redb");
    println!(
        "{:>12}  {:>12}  {:>10}  {:>16}  {:>16}",
        "body", "subscribers", "messages", "ms/publication", "µs/subscriber"
    );

    for (size, sessions) in FAN_OUT {
        let elapsed = fan_out(size, sessions, FAN_OUT_MESSAGES);
        let per_message = elapsed.as_secs_f64() / FAN_OUT_MESSAGES as f64;
        println!(
            "{:>12}  {:>12}  {:>10}  {:>16.1}  {:>16.1}",
            human(size),
            sessions,
            FAN_OUT_MESSAGES,
            per_message * 1_000.0,
            per_message * 1_000_000.0 / sessions as f64,
        );
    }
}

/// A size a person reads.
fn human(bytes: usize) -> String {
    if bytes >= 1_024 * 1_024 && bytes.is_multiple_of(1_024 * 1_024) {
        format!("{} MiB", bytes / (1_024 * 1_024))
    } else if bytes >= 1_024 && bytes.is_multiple_of(1_024) {
        format!("{} KiB", bytes / 1_024)
    } else {
        format!("{bytes} B")
    }
}

/// Where the numbers came from. A throughput figure without this is not
/// reproducible.
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
