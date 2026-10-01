//! A link between two nodes: frames in, batched frames out.
//!
//! Section 8.1 makes one transport carry message forwarding, metadata
//! replication and heartbeats, so there is one framing, one
//! authentication step and one failure model. This is that transport
//! once [`crate::handshake`] has succeeded.
//!
//! # Reading: incremental, and oversized is refused before the body
//!
//! [`FrameReader`] is `crates/server/src/framing.rs` in this crate's
//! vocabulary, and the precedent is exact: the decoder consumes nothing
//! when a frame is incomplete and refuses an oversized one from its
//! declared length before any body arrives, and this loop must not
//! defeat either property by buffering first and asking later. So it
//! decodes before it reads, every time round.
//!
//! A close partway through a frame is an error rather than an end, for
//! the reason the precedent gives: reporting a truncated stream as a
//! clean one would hide a fault.
//!
//! # Writing: batched, and bounded in the direction that can grow
//!
//! Fifty peers and a heartbeat a second each is a lot of very small
//! writes, so frames are gathered and flushed on **a size or a time
//! threshold**, whichever comes first. The time threshold is not an
//! optimisation: without it a single heartbeat would sit in a buffer
//! waiting for traffic that an idle link does not have, and the peer
//! would declare this node dead.
//!
//! **The queue to one peer is bounded, and beyond the bound messages
//! are dropped and counted rather than blocking whoever sent them.**
//! That is the whole reason [`Outgoing::send`] is not a future: a slow
//! or wedged peer must not be able to stop a broker's own delivery
//! path, and the strongest form of that is a sender that cannot await
//! anything. What replaces blocking is a count, because a drop nobody
//! counted is a message nobody will ever know was lost; see
//! [`Outgoing::dropped`].
//!
//! An idle link arms no timer. The pump waits for a first frame with
//! nothing else pending, and only then is a deadline set for the rest
//! of the batch.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{timeout_at, Instant};

use crate::frame::{decode, encode, FrameError, FrameLimits};

/// How much to read from the socket at once.
///
/// The precedent's number, for the precedent's reason: large enough
/// that a burst of heartbeats costs one syscall, small enough that a
/// full mesh does not hold a page per peer it is not using.
const READ_CHUNK: usize = 4096;

/// How many messages may be waiting for one peer.
///
/// Fifty peers at this depth is the ceiling on what a wedged mesh can
/// hold, which is the number this exists to bound.
pub const DEFAULT_OUTGOING_DEPTH: usize = 1024;

/// How many bytes of batch are enough to flush.
pub const DEFAULT_FLUSH_BYTES: usize = 8 * 1024;

/// How long a frame may wait for company.
///
/// Well under section 8.1's one-second heartbeat, so that batching
/// never costs a beat.
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(5);

/// What a link reads and writes by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkConfig {
    /// How large a frame this link will read or write.
    pub frames: FrameLimits,
    /// How many messages may be queued for the peer.
    pub outgoing_depth: usize,
    /// Flush once the batch is at least this many bytes.
    pub flush_bytes: usize,
    /// Flush at the latest this long after the first frame of a batch.
    pub flush_interval: Duration,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            frames: FrameLimits::default(),
            outgoing_depth: DEFAULT_OUTGOING_DEPTH,
            flush_bytes: DEFAULT_FLUSH_BYTES,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        }
    }
}

/// Why reading from a link failed.
#[derive(Debug, thiserror::Error)]
pub enum LinkError {
    /// The peer sent something this link will not read.
    ///
    /// Every one of these closes the link. See [`FrameError`] for which
    /// are a peer from another build and which are a peer misbehaving.
    #[error("framing error: {0}")]
    Frame(#[from] FrameError),
    /// The connection failed, or ended partway through a frame.
    #[error("connection error: {0}")]
    Io(#[from] std::io::Error),
}

/// Reads whole frames from a byte stream.
///
/// See the module documentation: it decodes before it reads, so the
/// decoder's two properties survive the loop around it.
pub struct FrameReader<R> {
    inner: R,
    buffer: BytesMut,
    limits: FrameLimits,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    /// A reader over `inner`, refusing frames larger than `limits`.
    pub fn new(inner: R, limits: FrameLimits) -> Self {
        Self {
            inner,
            buffer: BytesMut::with_capacity(READ_CHUNK),
            limits,
        }
    }

    /// The next message, or nothing when the peer closed cleanly.
    ///
    /// # Errors
    ///
    /// [`LinkError::Frame`] for anything the framing refuses, including
    /// an oversized frame refused **from its declared length alone**,
    /// before its body has arrived. [`LinkError::Io`] for a failed
    /// connection, and for a close partway through a frame: a truncated
    /// stream reported as a clean end would hide a fault.
    pub async fn next<M: DeserializeOwned>(&mut self) -> Result<Option<M>, LinkError> {
        loop {
            // The buffer first, so several frames in one read are all
            // returned without touching the socket again — and so that
            // an oversized declaration is refused on the strength of
            // the four bytes already in hand.
            if let Some(message) = decode(&mut self.buffer, &self.limits)? {
                return Ok(Some(message));
            }
            self.buffer.reserve(READ_CHUNK);
            let read = self.inner.read_buf(&mut self.buffer).await?;
            if read == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                return Err(LinkError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "the connection ended partway through a frame",
                )));
            }
        }
    }
}

/// What became of a message handed to [`Outgoing::send`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sent {
    /// It is in the queue for the peer.
    Queued,
    /// The queue for that peer was full, so it was dropped and counted.
    ///
    /// Returned rather than hidden, so that a caller which has
    /// somewhere better to put it can. Nothing has to look: the count
    /// is kept either way.
    Dropped,
}

/// The sending end of a link, held by whoever has something to say.
///
/// **Cloneable and never blocking.** One per peer; the bound is on the
/// queue behind it, and reaching the bound drops and counts. See the
/// module documentation for why this is not a future.
#[derive(Debug, Clone)]
pub struct Outgoing {
    queue: mpsc::Sender<Vec<u8>>,
    dropped: Arc<AtomicU64>,
    dropped_bytes: Arc<AtomicU64>,
    /// Bytes handed over and not yet written.
    ///
    /// Shared with [`Outbox`], which is what [`pump`] subtracts from once
    /// a batch has reached the stream. What it is for is the one loss
    /// nothing else counts: see [`Outgoing::pending_bytes`].
    pending_bytes: Arc<AtomicU64>,
    limits: FrameLimits,
}

/// The other end of that queue, which [`pump`] drains.
///
/// Its own type rather than a bare receiver, so that a caller cannot
/// accidentally hold two drains on one peer.
#[derive(Debug)]
pub struct Outbox {
    queue: mpsc::Receiver<Vec<u8>>,
    /// The other end of [`Outgoing::pending_bytes`].
    pending_bytes: Arc<AtomicU64>,
}

/// A queue to one peer, and the end that drains it.
#[must_use]
pub fn outgoing(config: &LinkConfig) -> (Outgoing, Outbox) {
    let (sender, receiver) = mpsc::channel(config.outgoing_depth);
    let pending_bytes = Arc::new(AtomicU64::new(0));
    (
        Outgoing {
            queue: sender,
            dropped: Arc::new(AtomicU64::new(0)),
            dropped_bytes: Arc::new(AtomicU64::new(0)),
            pending_bytes: Arc::clone(&pending_bytes),
            limits: config.frames,
        },
        Outbox {
            queue: receiver,
            pending_bytes,
        },
    )
}

impl Outgoing {
    /// Queue a message for the peer.
    ///
    /// Returns [`Sent::Dropped`] when the queue is full, having counted
    /// it. **It does not wait**, and it cannot: see the module
    /// documentation.
    ///
    /// A peer whose link has gone is the same answer as a peer whose
    /// queue is full, because it is the same thing to the caller: this
    /// message is not going to arrive, and something must say so.
    ///
    /// # Errors
    ///
    /// [`FrameError`] when the message cannot be framed at all, which
    /// is the writer refusing what a reader would reject rather than a
    /// peer's doing.
    pub fn send<M: Serialize>(&self, message: &M) -> Result<Sent, FrameError> {
        let frame = encode(message, &self.limits)?;
        // Weighed before it is handed over, because a frame that was
        // taken is a frame this no longer owns.
        let weight = frame.len() as u64;
        match self.queue.try_send(frame) {
            Ok(()) => {
                self.pending_bytes.fetch_add(weight, Ordering::Relaxed);
                Ok(Sent::Queued)
            }
            Err(_) => {
                // Counted before it is reported, so that a caller which
                // ignores the answer still leaves a trace. Relaxed: the
                // count is read by an operator and by the metrics, and
                // nothing branches on it.
                self.dropped.fetch_add(1, Ordering::Relaxed);
                // And the bytes beside the count, because section 11.3
                // asks for cluster bytes dropped and a count of
                // messages is not that: a heartbeat and a replicated
                // record are two very different losses.
                self.dropped_bytes.fetch_add(weight, Ordering::Relaxed);
                Ok(Sent::Dropped)
            }
        }
    }

    /// How many messages to this peer have been dropped.
    ///
    /// Section 8.1 counts what it drops rather than blocking, and this
    /// is the count. A drop nobody counted is a message nobody will
    /// ever know was lost.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// How many bytes to this peer have been dropped.
    ///
    /// Section 11.3's cluster bytes dropped, which is what the metrics
    /// report. Counted as the frame would have weighed on the wire,
    /// which is what a byte counter beside bytes sent and received has
    /// to mean for the three to be comparable.
    #[must_use]
    pub fn dropped_bytes(&self) -> u64 {
        self.dropped_bytes.load(Ordering::Relaxed)
    }

    /// How many bytes are queued for this peer and not yet written.
    ///
    /// **This is what a link replaced loses.** Two nodes dialling each
    /// other produce two links and one of them is dropped by
    /// [`crate::mesh::keeper`]'s rule; dropping it aborts its writer, so
    /// anything still in its queue is never written and nothing else would
    /// ever say so. A heartbeat lost that way is invisible — another
    /// follows within the interval — but section 8.2's replication puts
    /// real writes through the same queue, and section 8.1 is explicit
    /// that a drop nobody counted is a message nobody will know was lost.
    ///
    /// Counted as the frame would have weighed on the wire, which is what
    /// makes it comparable with [`Self::dropped_bytes`] and with the
    /// mesh's bytes sent.
    ///
    /// It is an upper bound by at most one batch: [`pump`] takes frames
    /// out of the queue before it writes them and subtracts once the write
    /// has landed, so a batch in flight is still counted here. That is the
    /// safe direction — a batch abandoned mid-write did not reach the peer
    /// either.
    #[must_use]
    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }
}

/// Write everything queued for the peer, in batches.
///
/// Flushes on a size or a time threshold, whichever comes first, and
/// returns when the last [`Outgoing`] for this peer has gone — having
/// written what was still queued.
///
/// # Errors
///
/// [`std::io::Error`] from the stream. A write that fails is a link
/// that has gone, which is the caller's to reconnect.
pub async fn pump<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut outbox: Outbox,
    config: &LinkConfig,
) -> std::io::Result<()> {
    let mut batch: Vec<u8> = Vec::with_capacity(config.flush_bytes);
    // An idle link arms no timer: this wait has no deadline, and the
    // deadline below is only for the rest of a batch that has started.
    while let Some(first) = outbox.queue.recv().await {
        batch.extend_from_slice(&first);
        let deadline = Instant::now() + config.flush_interval;
        while batch.len() < config.flush_bytes {
            match timeout_at(deadline, outbox.queue.recv()).await {
                Ok(Some(next)) => batch.extend_from_slice(&next),
                // Nobody will send again. What is in hand is written
                // below, and the outer loop then ends.
                Ok(None) => break,
                // The time threshold. A heartbeat must not wait for
                // traffic an idle link does not have.
                Err(_) => break,
            }
        }
        writer.write_all(&batch).await?;
        writer.flush().await?;
        // Subtracted once it has reached the stream, not when it left the
        // queue: see [`Outgoing::pending_bytes`] for what reads this and
        // why the count must not go down before the bytes have gone.
        outbox
            .pending_bytes
            .fetch_sub(batch.len() as u64, Ordering::Relaxed);
        batch.clear();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::Pin;
    use std::task::{Context, Poll};

    use serde::Deserialize;
    use tokio::io::duplex;

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Message {
        beat: u32,
        note: String,
    }

    fn message(beat: u32) -> Message {
        Message {
            beat,
            note: "beat".to_owned(),
        }
    }

    /// Long enough that a failure is a failure and not a slow machine.
    const PATIENCE: Duration = Duration::from_secs(5);

    async fn within<F: std::future::Future>(future: F) -> F::Output {
        tokio::time::timeout(PATIENCE, future)
            .await
            .expect("this should not have taken five seconds")
    }

    /// A stream that remembers each write it was given.
    ///
    /// The only way to see batching: what a reader gets back is the
    /// same bytes however they were written, so a test over a pair of
    /// ends cannot tell one write of ten frames from ten writes of one.
    #[derive(Debug, Default)]
    struct Recording {
        writes: Arc<std::sync::Mutex<Vec<usize>>>,
    }

    impl Recording {
        fn handle(&self) -> Arc<std::sync::Mutex<Vec<usize>>> {
            Arc::clone(&self.writes)
        }
    }

    impl AsyncWrite for Recording {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.writes.lock().expect("not poisoned").push(buf.len());
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    // -----------------------------------------------------------
    // Reading
    // -----------------------------------------------------------

    /// A frame round trips over a stream, through the pump.
    #[tokio::test]
    async fn frames_round_trip_over_a_stream() {
        let config = LinkConfig::default();
        let (writing, reading) = duplex(4096);
        let (out, outbox) = outgoing(&config);
        let pumping = tokio::spawn(async move { pump(writing, outbox, &config).await });

        for beat in 0..3 {
            assert_eq!(out.send(&message(beat)).expect("it frames"), Sent::Queued);
        }
        drop(out);

        let mut reader = FrameReader::new(reading, config.frames);
        for beat in 0..3 {
            let read: Message = within(reader.next())
                .await
                .expect("it reads")
                .expect("a frame");
            assert_eq!(read, message(beat));
        }
        assert_eq!(
            within(reader.next::<Message>())
                .await
                .expect("a clean close"),
            None
        );
        pumping.await.expect("no panic").expect("the writes landed");
    }

    /// A partial frame yields nothing and resumes when the rest
    /// arrives.
    ///
    /// A byte at a time, which is what a socket does and what the loop
    /// exists for. The reader is awaited while the frame is incomplete,
    /// so a loop that decided an incomplete frame was an end or an
    /// error would be caught rather than merely unexercised.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_partial_frame_yields_nothing_and_resumes_when_the_rest_arrives() {
        let config = LinkConfig::default();
        let whole = encode(&message(7), &config.frames).expect("it encodes");
        let (mut writing, reading) = duplex(4096);

        let mut reader = FrameReader::new(reading, config.frames);
        let feeding = tokio::spawn(async move {
            for byte in whole {
                writing.write_all(&[byte]).await.expect("the write lands");
                writing.flush().await.expect("the flush lands");
                tokio::task::yield_now().await;
            }
            writing
        });

        let read: Message = within(reader.next())
            .await
            .expect("it reads")
            .expect("the last byte completes it");
        assert_eq!(read, message(7));
        let writing = feeding.await.expect("no panic");
        drop(writing);
        assert_eq!(
            within(reader.next::<Message>())
                .await
                .expect("a clean close"),
            None
        );
    }

    /// An oversized frame is refused before its body arrives.
    ///
    /// **Four bytes and nothing else on the wire.** The reader must
    /// come back with the refusal rather than waiting for a body that
    /// is never coming, which is what the patience around it asserts: a
    /// loop that buffered first and asked later would sit here.
    #[tokio::test]
    async fn an_oversized_frame_is_refused_before_its_body_arrives() {
        let limits = FrameLimits {
            max_frame_bytes: 64,
        };
        let (mut writing, reading) = duplex(4096);
        writing
            .write_all(&4_000_000_000_u32.to_le_bytes())
            .await
            .expect("the length lands");
        writing.flush().await.expect("flushed");

        let mut reader = FrameReader::new(reading, limits);
        let error = within(reader.next::<Message>())
            .await
            .expect_err("a frame that size is not one this link carries");
        assert!(
            matches!(error, LinkError::Frame(FrameError::TooLarge { .. })),
            "{error:?}"
        );
    }

    /// An unknown version closes the link, with a reason.
    #[tokio::test]
    async fn an_unknown_version_closes_the_link_with_a_reason() {
        let config = LinkConfig::default();
        let mut frame = encode(&message(1), &config.frames).expect("it encodes");
        frame[crate::frame::LENGTH_BYTES] = crate::frame::FRAME_VERSION + 9;
        let (mut writing, reading) = duplex(4096);
        writing.write_all(&frame).await.expect("it lands");
        writing.flush().await.expect("flushed");

        let mut reader = FrameReader::new(reading, config.frames);
        let error = within(reader.next::<Message>())
            .await
            .expect_err("a layout this build does not speak");
        match error {
            LinkError::Frame(FrameError::UnknownVersion { found, expected }) => {
                assert_eq!(found, crate::frame::FRAME_VERSION + 9);
                assert_eq!(expected, crate::frame::FRAME_VERSION);
            }
            other => panic!("refused as {other:?}"),
        }
        assert!(
            error.to_string().contains("version"),
            "the reason has to say what was wrong: {error}"
        );
    }

    /// A close partway through a frame is an error, not an end.
    #[tokio::test]
    async fn a_close_partway_through_a_frame_is_an_error_not_an_end() {
        let config = LinkConfig::default();
        let whole = encode(&message(1), &config.frames).expect("it encodes");
        let (mut writing, reading) = duplex(4096);
        writing
            .write_all(&whole[..whole.len() - 1])
            .await
            .expect("it lands");
        drop(writing);

        let mut reader = FrameReader::new(reading, config.frames);
        let error = within(reader.next::<Message>())
            .await
            .expect_err("half a frame then silence is a truncated stream");
        assert!(matches!(error, LinkError::Io(_)), "{error:?}");
    }

    // -----------------------------------------------------------
    // Writing: the two thresholds
    // -----------------------------------------------------------

    /// A batch is flushed once it is large enough, in one write.
    ///
    /// Asserted on the writes themselves, because a reader cannot tell
    /// one write of ten frames from ten writes of one — convention 5, a
    /// test measuring the wrong dimension. The threshold here is two
    /// frames' worth, so a pump that wrote every frame as it came would
    /// record twice as many writes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_batch_is_flushed_on_its_size_threshold_in_one_write() {
        let one = encode(&message(0), &FrameLimits::default())
            .expect("it encodes")
            .len();
        let config = LinkConfig {
            // Four frames to a batch, and an interval long enough that
            // it is the size that ends this batch and not the clock.
            flush_bytes: one * 4,
            flush_interval: Duration::from_secs(30),
            ..LinkConfig::default()
        };
        let recording = Recording::default();
        let writes = recording.handle();
        let (out, outbox) = outgoing(&config);
        let pumping = tokio::spawn(async move { pump(recording, outbox, &config).await });

        for beat in 0..4 {
            assert_eq!(out.send(&message(beat)).expect("it frames"), Sent::Queued);
        }
        drop(out);
        within(pumping).await.expect("no panic").expect("it wrote");

        let writes = writes.lock().expect("not poisoned").clone();
        assert_eq!(
            writes,
            vec![one * 4],
            "four frames at the threshold are one write of four"
        );
    }

    /// One small frame is flushed on the time threshold.
    ///
    /// The half that keeps a heartbeat alive. The size threshold here
    /// is far larger than the frame, so without the clock this message
    /// would sit in the pump for ever and the read below would time
    /// out.
    #[tokio::test(flavor = "multi_thread")]
    async fn one_small_frame_is_flushed_on_the_time_threshold() {
        let config = LinkConfig {
            flush_bytes: 64 * 1024,
            flush_interval: Duration::from_millis(5),
            ..LinkConfig::default()
        };
        let (writing, reading) = duplex(4096);
        let (out, outbox) = outgoing(&config);
        let _pumping = tokio::spawn(async move { pump(writing, outbox, &config).await });

        assert_eq!(out.send(&message(1)).expect("it frames"), Sent::Queued);

        let mut reader = FrameReader::new(reading, config.frames);
        let read: Message = within(reader.next())
            .await
            .expect("it reads")
            .expect("the clock flushed it");
        assert_eq!(read, message(1));
    }

    // -----------------------------------------------------------
    // Writing: the bound
    // -----------------------------------------------------------

    /// A full queue drops and counts, and the link survives it.
    ///
    /// The surviving is the half that matters. A bound that dropped the
    /// overflow and then left the link unusable would have turned a
    /// slow peer into a dead one, which is the outage the bound exists
    /// to prevent.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_outgoing_buffer_drops_and_counts_and_the_link_survives() {
        let config = LinkConfig {
            outgoing_depth: 4,
            ..LinkConfig::default()
        };
        let (out, outbox) = outgoing(&config);
        assert_eq!(out.dropped(), 0);

        // Nothing is draining the queue yet, so the bound is reached.
        for beat in 0..4 {
            assert_eq!(
                out.send(&message(beat)).expect("it frames"),
                Sent::Queued,
                "the queue holds its configured depth"
            );
        }
        for beat in 4..10 {
            assert_eq!(
                out.send(&message(beat)).expect("it frames"),
                Sent::Dropped,
                "beyond the bound a message is dropped rather than queued"
            );
        }
        assert_eq!(
            out.dropped(),
            6,
            "every drop is counted, or a lost message is one nobody knows about"
        );

        // And now the link: what was queued arrives, and so does what
        // is sent afterwards.
        let (writing, reading) = duplex(4096);
        let _pumping = tokio::spawn(async move { pump(writing, outbox, &config).await });
        let mut reader = FrameReader::new(reading, config.frames);
        for beat in 0..4 {
            let read: Message = within(reader.next())
                .await
                .expect("it reads")
                .expect("a frame that was queued before the overflow");
            assert_eq!(read, message(beat));
        }
        assert_eq!(
            out.send(&message(99)).expect("it frames"),
            Sent::Queued,
            "the queue drained, so the link takes messages again"
        );
        let read: Message = within(reader.next())
            .await
            .expect("it reads")
            .expect("a frame sent after the overflow");
        assert_eq!(read, message(99), "the connection survived the drops");
        assert_eq!(out.dropped(), 6, "and nothing else was dropped");
    }

    /// A message too large to frame is refused at the sender.
    ///
    /// Convention 1: the writer refuses what the reader rejects, so
    /// this never reaches a queue and never reaches a peer.
    #[test]
    fn a_message_too_large_to_frame_never_reaches_the_queue() {
        let config = LinkConfig {
            // Smaller than this message, which frames to seven bytes.
            frames: FrameLimits { max_frame_bytes: 4 },
            ..LinkConfig::default()
        };
        let (out, _outbox) = outgoing(&config);
        let error = out
            .send(&message(1))
            .expect_err("a frame larger than the link carries is not sent");
        assert!(matches!(error, FrameError::TooLarge { .. }), "{error:?}");
        assert_eq!(
            out.dropped(),
            0,
            "refusing to frame something is not dropping it, and must not be counted as one"
        );
    }

    /// What is queued and not yet written is counted, and clears.
    ///
    /// The one loss nothing else could see: a link replaced by the rule
    /// that keeps one per pair aborts its writer, and whatever was still
    /// in its queue is never written. Section 8.1 is explicit that a drop
    /// nobody counted is a message nobody will know was lost, and this is
    /// what the mesh folds into its dropped bytes when it retires a link;
    /// see [`Outgoing::pending_bytes`].
    #[tokio::test(flavor = "multi_thread")]
    async fn what_is_queued_and_never_written_is_counted() {
        let config = LinkConfig::default();
        let one = encode(&message(0), &config.frames)
            .expect("it encodes")
            .len();
        let (out, outbox) = outgoing(&config);
        assert_eq!(out.pending_bytes(), 0, "nothing is queued yet");

        for beat in 0..3 {
            assert_eq!(out.send(&message(beat)).expect("it frames"), Sent::Queued);
        }
        assert_eq!(
            out.pending_bytes(),
            (one * 3) as u64,
            "a frame handed over and not written is not counted, so a link replaced \
             while it held one would report having lost nothing"
        );
        assert_eq!(
            out.dropped_bytes(),
            0,
            "queued is not dropped, and counting it as both would report one loss twice"
        );

        // And once the pump has written them it is back to nothing, so the
        // count is what is outstanding rather than what has ever been sent.
        let (writing, reading) = duplex(4096);
        let pumping = tokio::spawn(async move { pump(writing, outbox, &config).await });
        let mut reader = FrameReader::new(reading, config.frames);
        for beat in 0..3 {
            let read: Message = within(reader.next())
                .await
                .expect("it reads")
                .expect("a frame");
            assert_eq!(read, message(beat));
        }
        within(async {
            while out.pending_bytes() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        drop(out);
        pumping.await.expect("no panic").expect("the writes landed");
    }

    /// A send to a peer whose pump has gone is a drop, and counted.
    #[test]
    fn a_send_to_a_link_that_has_gone_is_counted_as_a_drop() {
        let config = LinkConfig::default();
        let (out, outbox) = outgoing(&config);
        drop(outbox);
        assert_eq!(out.send(&message(1)).expect("it frames"), Sent::Dropped);
        assert_eq!(out.dropped(), 1);
    }
}
