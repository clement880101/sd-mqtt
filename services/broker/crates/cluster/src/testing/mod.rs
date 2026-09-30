//! The two harnesses everything after the mesh depends on.
//!
//! They come before the mesh deliberately. A plan that builds the
//! cluster first and then asks how to partition it has already decided
//! it cannot: severing a link is not something that can be added to a
//! transport afterwards, because the place it has to happen is between
//! two ends that are already talking.
//!
//! - [`memory`] is section 14's in-memory link: three nodes in one
//!   process, connected through duplex pipes behind a switch. It is
//!   what the bulk of the matrix runs over, because it needs no port,
//!   no kernel and no clock.
//! - [`gate`] is the same severing in front of a **real socket**, for
//!   the cases an in-memory pipe cannot reach: a peer in another
//!   process, and a broker binary that knows nothing about any of this.
//!
//! # A cut must not close the socket
//!
//! This is the whole design and it is worth saying before the types.
//! Closing a socket is a symmetric failure both ends see at once, the
//! mesh detects it instantly, and it finds no bugs. The interesting
//! failure is the connection still established with the bytes simply
//! stopping — which is the case section 8.1's heartbeat exists for and
//! which a closed socket cannot reach at all.
//!
//! So a cut here is [`Flow::Swallow`] or [`Flow::Stall`], and neither
//! touches the connection. What tells them apart is what happens to
//! the sender:
//!
//! - **[`Flow::Swallow`] is a black hole.** The bytes are read and
//!   thrown away, so the sender never notices and never blocks. It is
//!   the closest thing to a router that has stopped forwarding.
//! - **[`Flow::Stall`] is an overloaded peer.** The bytes are not read
//!   at all, so they pile up in the sender's buffers until the sender
//!   blocks, and every one of them is delivered when the flow reopens.
//!
//! The two differ on healing, and that difference is real rather than
//! an artefact: a black hole loses what was in flight, so a framed
//! stream that resumes through one can resume mid-frame; a stall loses
//! nothing, so the stream continues exactly where it stopped.
//!
//! # One direction at a time
//!
//! Every valve is one direction of one ordered pair. That is where the
//! bugs are: a partition that severs both directions is a node that
//! has gone, and both ends agree about it immediately. A node that can
//! be heard but cannot hear is a node two halves of a cluster disagree
//! about, for as long as it lasts.

pub mod gate;
pub mod memory;

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;

/// How much is copied at once.
///
/// The link's own read size, so that a harness never becomes the thing
/// that decides how bytes are grouped.
const CHUNK: usize = 4096;

/// What one direction of a link is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Flow {
    /// Bytes move. The ordinary state.
    #[default]
    Open,
    /// Bytes are read and discarded. A black hole: the sender never
    /// notices, and what was in flight is gone for good.
    Swallow,
    /// Bytes are not read. An overloaded peer: the sender blocks once
    /// the buffers fill, and everything is delivered when this
    /// reopens.
    Stall,
}

/// Which way the bytes are going.
///
/// Named from the point of view of whoever dialled, because that is
/// the end a test holds: [`Direction::Forward`] is what the dialler
/// sends and [`Direction::Backward`] is what it hears back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// From the end that dialled, towards the end that accepted.
    Forward,
    /// From the end that accepted, back towards the end that dialled.
    Backward,
}

/// One direction of one link, and whether it is flowing.
///
/// A [`watch`] channel rather than a flag and a notification, because
/// the relay has to both read the current state and wait for the next
/// one, and doing that with two primitives is how a wakeup goes
/// missing and a stalled direction never reopens.
#[derive(Debug)]
pub struct Valve {
    flow: watch::Sender<Flow>,
    passed: AtomicU64,
    swallowed: AtomicU64,
}

impl Default for Valve {
    fn default() -> Self {
        Self::new()
    }
}

impl Valve {
    /// A valve that is open, having carried nothing.
    #[must_use]
    pub fn new() -> Self {
        Self {
            flow: watch::channel(Flow::Open).0,
            passed: AtomicU64::new(0),
            swallowed: AtomicU64::new(0),
        }
    }

    /// What it is doing now.
    #[must_use]
    pub fn flow(&self) -> Flow {
        *self.flow.borrow()
    }

    /// Make it do this instead.
    ///
    /// Takes effect on the relay's next decision, which is at the
    /// latest when the next byte arrives — and immediately for a
    /// relay that is waiting for this, which is what makes a heal
    /// deliver a stalled backlog without anything else happening on
    /// the link.
    pub fn set(&self, flow: Flow) {
        // `send_replace` rather than `send`: a valve with no relay
        // attached yet is the ordinary case when a test sets a flow
        // before a connection exists, and `send` reports that as an
        // error.
        self.flow.send_replace(flow);
    }

    /// Stop the bytes, without touching the connection.
    pub fn cut(&self) {
        self.set(Flow::Swallow);
    }

    /// Let them move again.
    pub fn heal(&self) {
        self.set(Flow::Open);
    }

    /// How many bytes this direction has delivered.
    #[must_use]
    pub fn passed(&self) -> u64 {
        self.passed.load(Ordering::Relaxed)
    }

    /// How many bytes this direction has thrown away.
    ///
    /// The count is what makes a test able to wait for a cut to have
    /// actually bitten rather than sleeping and hoping: a partition
    /// is in force once the bytes it is dropping have been counted.
    #[must_use]
    pub fn swallowed(&self) -> u64 {
        self.swallowed.load(Ordering::Relaxed)
    }
}

/// Copy `source` into `sink`, subject to `valve`.
///
/// **The one implementation of a cut**, used by both harnesses. Two
/// harnesses with two copies of this would be two answers to what
/// severing a link means, and the one that is wrong is whichever the
/// mesh is not being tested over.
///
/// Returns when the source ends or either side fails. The sink is shut
/// down on a clean end, so that an end propagates rather than becoming
/// a link that is open and silent — which is the very thing a cut here
/// simulates, and a harness that produced it by accident would be
/// testing nothing.
///
/// # Errors
///
/// [`std::io::Error`] from either side.
pub async fn relay<R, W>(mut source: R, mut sink: W, valve: Arc<Valve>) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut watcher = valve.flow.subscribe();
    let mut buffer = vec![0_u8; CHUNK];
    // Bytes read but not yet disposed of. Non-empty only while
    // stalled: a stall that arrives between the read and the write
    // must hold what is in hand rather than lose it, or a stall would
    // be a swallow of one chunk.
    let mut held: Vec<u8> = Vec::new();

    loop {
        if held.is_empty() {
            // **Not read at all while stalled**, which is what makes a
            // stall push back: the bytes stay in the sender's buffers
            // and the sender eventually blocks on them.
            //
            // Convention 7, reported rather than tidied away: deleting
            // this loop survives the suite. It has to, and the reason
            // is worth writing down. What actually holds the sender
            // back is that `held` is not emptied while stalled, so the
            // branch above is not entered again and no second read
            // happens; without this loop the relay drains exactly one
            // chunk more before it wedges, and a read already in
            // flight when a stall arrives drains that chunk anyway.
            // So this says "not read at all" precisely rather than
            // approximately, and the property it is about — that a
            // stall stops the sender — is guarded by
            // `memory::tests::a_stall_pushes_back_on_the_sender`,
            // which does fail when the drain is made unbounded.
            while *watcher.borrow_and_update() == Flow::Stall {
                if watcher.changed().await.is_err() {
                    return Ok(());
                }
            }
            let read = source.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            held.extend_from_slice(&buffer[..read]);
        }
        // Read out and dropped before the arms, because the borrow a
        // `watch` hands back is held for the whole expression and
        // `changed` needs the receiver back.
        let flow = *watcher.borrow_and_update();
        match flow {
            Flow::Open => {
                sink.write_all(&held).await?;
                sink.flush().await?;
                valve.passed.fetch_add(held.len() as u64, Ordering::Relaxed);
                held.clear();
            }
            Flow::Swallow => {
                valve
                    .swallowed
                    .fetch_add(held.len() as u64, Ordering::Relaxed);
                held.clear();
            }
            Flow::Stall => {
                if watcher.changed().await.is_err() {
                    return Ok(());
                }
            }
        }
    }
    sink.shutdown().await
}
