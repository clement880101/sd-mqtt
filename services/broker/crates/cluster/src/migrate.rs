//! A session moves, and what it holds moves with it. Section 8.4.
//!
//! A client that connects to a node which does not own its session makes
//! that node claim it — [`crate::metadata`]'s register — then **ask the
//! previous owner to close the old connection and send what the session
//! holds**, and wait a bounded time before acknowledging. This module is
//! the asking and the waiting; `broker_core::handover` is what is asked
//! for and `broker_core::Broker::hand_over` is what the far node does
//! about it.
//!
//! # A drain that does not arrive is not a refusal
//!
//! Section 8.4 reverses the obvious implementation and says so outright:
//! **on timeout, or when the previous owner cannot be reached, the
//! connection is accepted with what arrived**, the session is reported
//! present, and the shortfall is counted.
//!
//! Refusing instead makes an unreachable node into an outage for a
//! client that has done nothing wrong, and the partition that made the
//! owner unreachable is exactly the moment a client most needs to
//! connect somewhere. What is lost is bounded by what had not yet been
//! delivered, it is counted rather than silent, and section 8.6's queue
//! replication is what shrinks it to nothing.
//!
//! So every path out of [`Awaiting::collect`] returns a handover. What
//! differs is [`Drain::complete`] and [`Drain::missing`], and both are
//! [`Migrations::stats`] as well, because a number a connection returned
//! and nobody added up is a shortfall nobody can see.
//!
//! # It arrives in chunks, and that is what makes a partial drain a thing
//!
//! A single frame carrying a whole queue would make "the drain timed out
//! partway" unreachable: a frame either arrives or does not, so a
//! timeout would always mean nothing arrived and "with what did arrive
//! delivered" would be a sentence with no case behind it. The previous
//! owner sends at most [`CHUNK`] messages per frame and says in the
//! first how many there are altogether, so the claiming node can tell a
//! drain that finished from one that stopped — and by how much.
//!
//! # The waiter is keyed by the session and the epoch
//!
//! Not by the session alone. Two claims of one session can be in flight
//! at once — that is what a partition does — and a chunk answering the
//! earlier of them must not be taken for an answer to the later, or a
//! connection acknowledges on a queue that was drained to somebody else.
//! The epoch is what tells them apart, and it is in the request and
//! echoed in every chunk.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use broker_core::handover::Handover;
use serde::{Deserialize, Serialize};
use storage::record::{BodyRecord, InflightRecord, QueueEntry, SubscriptionRecord};
use storage::SessionKey;
use tokio::sync::mpsc;

use crate::identity::NodeId;
use crate::mesh::{self, Mesh};

/// How long a claiming node waits for a drain before carrying on.
///
/// Section 8.4's "bounded timeout". Two seconds is long enough for a
/// healthy link to deliver a queue across a datacentre and short enough
/// that a client whose previous owner is wedged is connected rather than
/// left hanging — which is the whole reason the wait is bounded and not
/// the reason it is two rather than three.
pub const DEFAULT_PATIENCE: Duration = Duration::from_secs(2);

/// How many messages one drain frame carries.
///
/// Small enough that a session holding thousands does not become one
/// frame larger than a link will pass, and large enough that an ordinary
/// queue of a handful crosses in one. See the module note on why the
/// drain is chunked at all.
pub const CHUNK: usize = 64;

/// "I have claimed this session; close its connection and send me what
/// it holds."
///
/// **It carries no node.** The link has already proved who is on the
/// other end of it, which is where [`mesh::Message`]'s other variants
/// take the same answer from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claiming {
    /// Which session.
    pub session: SessionKey,
    /// The epoch the claiming node now holds it at, echoed in every
    /// chunk so that two claims in flight cannot be confused.
    pub epoch: u64,
    /// Whether the client asked for a clean start.
    ///
    /// [MQTT-3.1.2-4] discards the session, so there is nothing to
    /// carry — but the old connection is still closed and the previous
    /// owner still lets the session go. A claiming node that simply did
    /// not ask would leave a session and a live socket behind on the
    /// node it took over from.
    pub clean: bool,
}

/// One instalment of what a session held.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Drained {
    /// Which session.
    pub session: SessionKey,
    /// Which claim this answers.
    pub epoch: u64,
    /// How many messages the session held in total when the drain
    /// began.
    ///
    /// The same number in every chunk, so that a claiming node that
    /// received the second and not the first still knows what it is
    /// short by.
    pub announced: u64,
    /// The filters, carried in the first chunk and empty after it.
    pub subscriptions: Vec<SubscriptionRecord>,
    /// Outstanding messages, with the bodies they name.
    pub inflight: Vec<(InflightRecord, BodyRecord)>,
    /// Waiting messages, with the bodies they name.
    pub queued: Vec<(QueueEntry, BodyRecord)>,
    /// Exactly-once identifiers the client owns, in the first chunk.
    pub inbound: Vec<u16>,
    /// The highest sequence the session ever assigned.
    pub highest_sequence: u64,
    /// Whether this is the last chunk.
    pub last: bool,
}

impl Drained {
    /// Cut `handover` into the frames that carry it.
    ///
    /// Always at least one, even for a session holding nothing: the last
    /// chunk is what says the drain finished, and a drain that sent
    /// nothing at all is indistinguishable at the far end from a node
    /// that never answered.
    #[must_use]
    pub fn chunks(session: &SessionKey, epoch: u64, handover: Handover) -> Vec<Self> {
        let announced = handover.messages() as u64;
        let Handover {
            subscriptions,
            inflight,
            queued,
            inbound,
            highest_sequence,
        } = handover;
        let mut chunks: Vec<Self> = Vec::new();
        let mut inflight = inflight.into_iter().peekable();
        let mut queued = queued.into_iter().peekable();
        loop {
            let mut chunk = Self {
                session: session.clone(),
                epoch,
                announced,
                subscriptions: Vec::new(),
                inflight: Vec::new(),
                queued: Vec::new(),
                inbound: Vec::new(),
                highest_sequence,
                last: false,
            };
            // The window first and in order, because the far end puts it
            // back in order and a chunk boundary must not reorder it.
            while chunk.inflight.len() + chunk.queued.len() < CHUNK {
                if let Some(entry) = inflight.next() {
                    chunk.inflight.push(entry);
                } else if let Some(entry) = queued.next() {
                    chunk.queued.push(entry);
                } else {
                    break;
                }
            }
            let done = inflight.peek().is_none() && queued.peek().is_none();
            chunk.last = done;
            chunks.push(chunk);
            if done {
                break;
            }
        }
        // The filters and the held identifiers ride the first chunk, so
        // that a drain which stops partway has still moved the
        // subscriptions — which is what makes section 8.3 forward to the
        // new node at all.
        let first = chunks.first_mut().expect("the loop pushes at least one");
        first.subscriptions = subscriptions;
        first.inbound = inbound;
        chunks
    }
}

/// What a bounded wait came back with.
///
/// A handover on every path. See the module note: a drain that did not
/// finish is a connection that is accepted anyway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drain {
    /// What arrived, merged in the order it was sent.
    pub handover: Handover,
    /// Whether the previous owner said it had sent everything.
    pub complete: bool,
    /// How many messages the previous owner said it held and this node
    /// did not receive.
    ///
    /// Zero when the drain completed, and zero when nothing arrived at
    /// all — there is nobody to have announced a figure, which is why
    /// [`Stats::incomplete`] is the count that proves an unreachable
    /// owner rather than this one.
    pub missing: u64,
}

/// What a node's migrations have added up to.
///
/// Read by the metrics endpoint and by the tests that prove the
/// shortfall is counted rather than merely tolerated. Plain atomics:
/// every one of these is incremented once per connect at most.
#[derive(Debug, Default)]
pub struct Stats {
    claimed: AtomicU64,
    drained: AtomicU64,
    incomplete: AtomicU64,
    unreachable: AtomicU64,
    messages: AtomicU64,
    missing: AtomicU64,
    late: AtomicU64,
}

impl Stats {
    /// Sessions this node claimed from another node.
    #[must_use]
    pub fn claimed(&self) -> u64 {
        self.claimed.load(Ordering::Relaxed)
    }

    /// Drains that finished: the previous owner said it had sent
    /// everything, and this node had it before the wait ran out.
    #[must_use]
    pub fn drained(&self) -> u64 {
        self.drained.load(Ordering::Relaxed)
    }

    /// Drains that did not finish, for any reason.
    ///
    /// **The count the spec means by "the shortfall is counted".** It
    /// covers the unreachable owner, where no figure can be known, and
    /// the drain that stopped partway, where [`Self::missing`] says by
    /// how much.
    #[must_use]
    pub fn incomplete(&self) -> u64 {
        self.incomplete.load(Ordering::Relaxed)
    }

    /// Of those, the ones where there was no link to ask over.
    #[must_use]
    pub fn unreachable(&self) -> u64 {
        self.unreachable.load(Ordering::Relaxed)
    }

    /// Messages that arrived in a drain.
    #[must_use]
    pub fn messages(&self) -> u64 {
        self.messages.load(Ordering::Relaxed)
    }

    /// Messages a previous owner said it held and this node never got.
    #[must_use]
    pub fn missing(&self) -> u64 {
        self.missing.load(Ordering::Relaxed)
    }

    /// Chunks that arrived with nobody waiting for them.
    ///
    /// A drain that finished after the wait did, or an answer to a claim
    /// some other connection has already superseded. Not an error and
    /// counted anyway: a standing stream of these is a patience set
    /// below what the links cost.
    #[must_use]
    pub fn late(&self) -> u64 {
        self.late.load(Ordering::Relaxed)
    }
}

/// The drains this node is waiting for.
///
/// One per node, held beside the mesh. A chunk arriving on any link is
/// handed here and routed to the connection waiting for it; a chunk with
/// nobody waiting is counted and dropped.
#[derive(Debug, Default)]
pub struct Migrations {
    waiting: Mutex<HashMap<(SessionKey, u64), mpsc::UnboundedSender<Drained>>>,
    stats: Stats,
}

fn locked<T>(what: &Mutex<T>) -> MutexGuard<'_, T> {
    what.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Migrations {
    /// What this node's migrations have added up to.
    #[must_use]
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Start waiting for the answer to a claim.
    ///
    /// **Registered before the request is sent**, which is the ordering
    /// the whole thing rests on: a far node that answers immediately
    /// answers into a queue that already exists, and a registration
    /// afterwards is a race whose loser waits the full patience for a
    /// frame it has already thrown away.
    #[must_use]
    pub fn begin(self: &Arc<Self>, session: &SessionKey, epoch: u64) -> Awaiting {
        let (sender, chunks) = mpsc::unbounded_channel();
        locked(&self.waiting).insert((session.clone(), epoch), sender);
        Awaiting {
            migrations: Arc::clone(self),
            session: session.clone(),
            epoch,
            chunks,
        }
    }

    /// A chunk arrived.
    ///
    /// Never blocks: the queue is unbounded and what bounds it is the
    /// far node having a finite session to send.
    pub fn arrived(&self, chunk: Drained) {
        let key = (chunk.session.clone(), chunk.epoch);
        let sender = locked(&self.waiting).get(&key).cloned();
        match sender {
            Some(sender) => {
                if sender.send(chunk).is_err() {
                    self.stats.late.fetch_add(1, Ordering::Relaxed);
                }
            }
            None => {
                self.stats.late.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// There was no link to the previous owner.
    ///
    /// Section 8.4's other half of the same sentence as a timeout, and
    /// counted as its own thing because an operator reading a standing
    /// count of these is reading a partition rather than a slow peer.
    fn no_owner_to_ask(&self) {
        self.stats.incomplete.fetch_add(1, Ordering::Relaxed);
        self.stats.unreachable.fetch_add(1, Ordering::Relaxed);
    }
}

/// A drain in flight.
///
/// Dropped when the wait is over, which is what takes the waiter out of
/// the table — including on a path that returns early, and including a
/// panic.
#[derive(Debug)]
pub struct Awaiting {
    migrations: Arc<Migrations>,
    session: SessionKey,
    epoch: u64,
    chunks: mpsc::UnboundedReceiver<Drained>,
}

impl Drop for Awaiting {
    fn drop(&mut self) {
        locked(&self.migrations.waiting).remove(&(self.session.clone(), self.epoch));
    }
}

impl Awaiting {
    /// Collect chunks until the drain finishes or `patience` runs out.
    ///
    /// **The deadline is over the whole drain and not over each chunk.**
    /// A per-chunk timeout is a bound a slow sender can walk past
    /// indefinitely by sending one message at a time, which is a drain
    /// that is not bounded at all — the mutation that removes the bound
    /// has to fail a test rather than hang it, and a deadline computed
    /// once is what makes that possible.
    pub async fn collect(mut self, patience: Duration) -> Drain {
        let deadline = tokio::time::Instant::now() + patience;
        let mut handover = Handover::default();
        let mut announced = 0u64;
        let mut complete = false;
        loop {
            let chunk = match tokio::time::timeout_at(deadline, self.chunks.recv()).await {
                Ok(Some(chunk)) => chunk,
                // The sender is gone, which cannot happen while this
                // holds the receiver: the table holds the only other
                // handle and this entry leaves it when `self` drops.
                // Treated as a drain that stopped rather than
                // asserted away.
                Ok(None) | Err(_) => break,
            };
            announced = announced.max(chunk.announced);
            handover.subscriptions.extend(chunk.subscriptions);
            handover.inbound.extend(chunk.inbound);
            handover.highest_sequence = handover.highest_sequence.max(chunk.highest_sequence);
            handover.inflight.extend(chunk.inflight);
            handover.queued.extend(chunk.queued);
            if chunk.last {
                complete = true;
                break;
            }
        }
        let received = handover.messages() as u64;
        let missing = announced.saturating_sub(received);
        let stats = &self.migrations.stats;
        stats.messages.fetch_add(received, Ordering::Relaxed);
        if complete {
            stats.drained.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.incomplete.fetch_add(1, Ordering::Relaxed);
            stats.missing.fetch_add(missing, Ordering::Relaxed);
        }
        Drain {
            handover,
            complete,
            // A completed drain is short by nothing, whatever arithmetic
            // on a figure the far node sent would say.
            missing: if complete { 0 } else { missing },
        }
    }
}

/// What a claim produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claimed {
    /// The epoch this node now holds the session at.
    pub epoch: u64,
    /// Who held it a moment ago, if anybody, and `None` when this node
    /// already did.
    pub previous: Option<NodeId>,
    /// The register to store and broadcast, `None` when this node
    /// already owned the session. See `cluster::metadata::Claim::write`.
    pub write: Option<crate::metadata::MetadataWrite>,
    /// What the previous owner sent, which is nothing when there was
    /// none to ask.
    pub drain: Option<Drain>,
}

impl Claimed {
    /// Whether the cluster says this session already existed.
    ///
    /// **What the acknowledgement's session-present flag reports on a
    /// migration**, and it is `true` even when the drain brought
    /// nothing: section 8.4 says the session is reported present when
    /// the previous owner could not be reached, because it *is* present
    /// — this node has taken ownership of it — and telling the client
    /// otherwise would make it resubscribe, which is the one thing that
    /// cannot be recovered from a queue.
    #[must_use]
    pub fn present(&self) -> bool {
        self.previous.is_some()
    }
}

/// What asks the previous owner, over a mesh.
///
/// Held beside the mesh for the reason `sdmqtt::replication` holds the
/// rest of the wiring: a crate that held the mesh and was held by it
/// would be a cycle, and this one is held by the shell rather than
/// holding it.
#[derive(Debug)]
pub struct Migrator {
    replicated: Arc<crate::metadata::Replicated>,
    mesh: Arc<Mesh>,
    migrations: Arc<Migrations>,
    patience: Duration,
}

impl Migrator {
    /// Claim sessions for `replicated`'s node, draining over `mesh`.
    ///
    /// `migrations` is made **before** the mesh and handed to both,
    /// because the handler the mesh is started with is what routes an
    /// arriving chunk into it: a table built here would be a second one
    /// that nothing ever puts a chunk in, and every drain would time
    /// out. See `sdmqtt::replication`.
    #[must_use]
    pub fn new(
        replicated: Arc<crate::metadata::Replicated>,
        mesh: Arc<Mesh>,
        migrations: Arc<Migrations>,
        patience: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            replicated,
            mesh,
            migrations,
            patience,
        })
    }

    /// The drains this node is waiting for, for whatever delivers
    /// frames into it.
    #[must_use]
    pub fn migrations(&self) -> &Arc<Migrations> {
        &self.migrations
    }

    /// What this node's migrations have added up to.
    #[must_use]
    pub fn stats(&self) -> &Stats {
        self.migrations.stats()
    }

    /// Claim `session` and drain it from whoever held it.
    ///
    /// Returns once the drain has finished or the patience has run out,
    /// whichever comes first, and **returns either way**: see the module
    /// note.
    pub async fn claim(&self, session: &SessionKey, clean: bool) -> Claimed {
        let claim = self.replicated.session_claimed(session);
        let Some(previous) = claim.previous.map(|held| held.node) else {
            // Nobody held it. A session this cluster has never seen, or
            // one every node has released.
            return Claimed {
                epoch: claim.epoch,
                previous: None,
                write: claim.write,
                drain: None,
            };
        };
        if previous == self.replicated.me() {
            // The within-one-node takeover `broker_core::registry` has
            // always done, which is not a cluster event and has nothing
            // to ask anybody. `Claim::write` is already `None` here.
            return Claimed {
                epoch: claim.epoch,
                previous: None,
                write: claim.write,
                drain: None,
            };
        }
        self.migrations
            .stats
            .claimed
            .fetch_add(1, Ordering::Relaxed);

        // Registered before the request goes out; see `Migrations::begin`.
        let awaiting = self.migrations.begin(session, claim.epoch);
        let asked = self.mesh.send(
            previous,
            &mesh::Message::Claim(Claiming {
                session: session.clone(),
                epoch: claim.epoch,
                clean,
            }),
        );
        let reached = match asked {
            Some(Ok(crate::link::Sent::Queued)) => true,
            // A peer whose queue is full, a peer with no link, and a
            // frame this build would not read back. All three are "the
            // previous owner cannot be reached", which section 8.4 makes
            // the same case as a timeout — with the difference that
            // there is nothing to wait for, so this does not wait.
            Some(Ok(crate::link::Sent::Dropped)) | Some(Err(_)) | None => false,
        };
        if !reached {
            drop(awaiting);
            self.migrations.no_owner_to_ask();
            tracing::warn!(
                client = %session.client_id,
                %previous,
                "the previous owner of a session could not be asked for it, so the \
                 connection is accepted with nothing and the shortfall is counted"
            );
            return Claimed {
                epoch: claim.epoch,
                previous: Some(previous),
                write: claim.write,
                drain: None,
            };
        }
        let drain = awaiting.collect(self.patience).await;
        if !drain.complete {
            tracing::warn!(
                client = %session.client_id,
                %previous,
                missing = drain.missing,
                "a session's drain did not finish, so the connection is accepted with \
                 what arrived"
            );
        }
        Claimed {
            epoch: claim.epoch,
            previous: Some(previous),
            write: claim.write,
            drain: Some(drain),
        }
    }
}

#[cfg(test)]
mod tests;
