//! Nodes find each other, authenticate, and know when a peer has gone.
//!
//! Section 8.1's full mesh. Every node dials the seeds it was given
//! and the peers it has met, every link is [`crate::handshake`]'s
//! mutual challenge before it is anything else, and the same link
//! carries the heartbeat that says the peer is still there. Nothing
//! routes and nothing replicates yet; this is what those will sit on.
//!
//! # Two nodes dialling each other settle on one link
//!
//! At startup every node dials every other, so every pair produces two
//! connections and both of them authenticate. Keeping both would
//! double the heartbeats, double the bytes, and — once anything is
//! forwarded over them — deliver it twice.
//!
//! The rule is [`keeper`]: **the link that survives is the one dialled
//! by the lower identity.** It is computed from the two identities and
//! nothing else, so both ends reach the same answer without exchanging
//! another message, and it is total because the two identities can
//! never be equal — a greeting carrying this node's own identity is
//! refused before it gets here.
//!
//! A single link is never dropped for having the wrong role. The rule
//! breaks a tie; it does not forbid a link that has no rival, which
//! matters whenever only one of the two ends has an address for the
//! other.
//!
//! # A member that dies keeps its link, and its link keeps its socket
//!
//! Section 8.1 gives failure detection two signals, and this module
//! keeps them apart on purpose:
//!
//! - **A closed socket removes the link** and marks the peer dead at
//!   once, and the dialler starts trying again with backoff.
//! - **Silence over an open socket does not touch the link.** The peer
//!   is marked suspect and then dead, and if it speaks again it is
//!   alive again *on the same connection*.
//!
//! The second is the half-open link, which is what the heartbeat is
//! for, and tearing the connection down on the way to declaring it
//! dead would turn every one of those into the first — a symmetric
//! failure both ends see at once, which is precisely the case that
//! finds no bugs. It is also what lets a one-way partition heal
//! without anything reconnecting.
//!
//! # What is not here
//!
//! Nothing gossips. A node learns of a peer by being seeded with its
//! address or by being dialled by it, and the address it dials it
//! again at is the one that peer **advertised**, which is not the
//! address it connected from: section 8.1 says so, and a node behind a
//! port mapping or behind a test's gate is the reason.
//!
//! There is also no way to say a node has left for good. See
//! [`crate::membership`]: a member is forgotten when this process
//! restarts, and section 8.5's departure is a later plan's.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::task::{AbortHandle, JoinHandle};

use crate::config::ClusterConfig;
use crate::frame::FrameError;
use crate::handshake::{self, HandshakeError, Role};
use crate::identity::{NodeId, NodeIdentity};
use crate::link::{outgoing, pump, FrameReader, LinkConfig, Sent};
use crate::membership::{Detection, Member, MemberState, Membership};
use crate::transport::{BoxStream, Transport};

/// How long to wait before trying a failed address again, at first.
pub const DEFAULT_RECONNECT_MIN: Duration = Duration::from_millis(200);

/// And at most.
pub const DEFAULT_RECONNECT_MAX: Duration = Duration::from_secs(30);

/// How long to wait after a peer refused this node's secret.
///
/// Section 8.1: **a mismatched secret is a configuration error that
/// will not fix itself**, so it backs off harder than a transport
/// failure. Five minutes rather than thirty seconds, because retrying
/// it is not going to work and the attempt costs the peer a handshake.
pub const DEFAULT_REFUSED_BACKOFF: Duration = Duration::from_secs(300);

/// What a cluster link carries.
///
/// Section 8.3's routing is a later plan and arrives as a variant here
/// rather than as a second transport, which is what section 8.1 asks
/// for: one framing, one authentication step and one failure model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// Section 8.1's heartbeat.
    ///
    /// Carries which beat it is, counted by the sender since it
    /// started. Nothing depends on the number; it is what makes a log
    /// line about a beat say which one, and what lets a test assert
    /// that beats are still being sent rather than that one once was.
    Heartbeat {
        /// Which beat this is.
        beat: u64,
    },
    /// Section 8.2's replication.
    ///
    /// **Which peer sent it is not in the frame.** A replicated entry is
    /// a claim about the sender's own operation log, and a node named in
    /// the frame would be a node any peer could claim to be; the link
    /// has already proved who is on the other end of it, so that is
    /// where the answer comes from.
    Replicate(crate::replicate::Replication),
}

/// What something on top of the mesh does with a peer's message.
///
/// The seam section 8.2's replication sits on. Implemented by whoever
/// holds the replicator and the storage writer, because what a frame
/// changes has to be stored and the mesh writes nothing.
///
/// **Synchronous, and it must not block.** It is called from the task
/// reading one link, so a handler that waited on a device would stop
/// that peer's heartbeats being noticed. Storing is a channel send and
/// taking the broker's lock is what this module already does, so there
/// is nothing here that needs to wait.
pub trait Deliver: Send + Sync {
    /// `from` sent `message`. Returns whatever to say back to it.
    ///
    /// Replies go back to the sender and nowhere else, which is what
    /// keeps this from needing the mesh: a handler that could send to
    /// anybody would have to hold the thing that holds it.
    fn delivered(&self, from: NodeId, message: Message) -> Vec<Message>;
}

/// Which end of a link between two nodes is the one to keep.
///
/// **The link dialled by the lower identity**, expressed as the role
/// *this* node has on it. Deterministic from the two identities, so
/// both ends drop the same connection without another message.
///
/// The name is the ordering section 8.1 already uses for singleton
/// ownership and for conflict tie-breaks, which is the point: a third
/// answer to "which node" would be a third thing to keep in step.
#[must_use]
pub fn keeper(me: NodeId, peer: NodeId) -> Role {
    if me < peer {
        Role::Dialler
    } else {
        Role::Listener
    }
}

/// How a mesh dials, beats and gives up.
#[derive(Debug, Clone)]
pub struct MeshSettings {
    /// The address peers should dial to reach this node.
    ///
    /// **Not the bind address**, and section 8.1 says why: a node
    /// bound to every interface, behind a port mapping, or behind a
    /// test's gate has a bind address no peer can use.
    pub advertise: String,
    /// The addresses this node dials to join.
    pub seeds: Vec<String>,
    /// The windows failure detection uses.
    pub detection: Detection,
    /// How long to wait before retrying an address, at first.
    pub reconnect_min: Duration,
    /// And at most.
    pub reconnect_max: Duration,
    /// How long to wait after a peer refused this node's secret.
    pub refused_backoff: Duration,
    /// What the link under this mesh reads and writes by.
    pub link: LinkConfig,
}

impl Default for MeshSettings {
    fn default() -> Self {
        Self {
            advertise: String::new(),
            seeds: Vec::new(),
            detection: Detection::default(),
            reconnect_min: DEFAULT_RECONNECT_MIN,
            reconnect_max: DEFAULT_RECONNECT_MAX,
            refused_backoff: DEFAULT_REFUSED_BACKOFF,
            link: LinkConfig::default(),
        }
    }
}

/// Section 11.3's cluster numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeshStats {
    /// Peers this node has met, dead ones included.
    pub members: usize,
    /// How many of them are answering.
    pub alive: usize,
    /// Links established right now.
    pub links: usize,
    /// Bytes written to peers.
    pub bytes_sent: u64,
    /// Bytes read from peers.
    pub bytes_received: u64,
    /// Bytes that were never written, because a peer's queue was full.
    ///
    /// Section 8.1 drops rather than blocking, and this is what says
    /// so: a drop nobody counted is a message nobody will know was
    /// lost.
    pub bytes_dropped: u64,
}

/// What became of one broadcast.
///
/// Three numbers rather than a count of peers, because the three are
/// different facts: a peer that took it, a peer whose queue was full,
/// and a message this build refused to frame at all. A caller that saw
/// only "three peers" could not tell a healthy fan-out from a wedged
/// one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Fanout {
    /// Peers the message was queued for.
    pub queued: usize,
    /// Peers whose queue was full, so it was dropped and counted.
    pub dropped: usize,
    /// Peers it could not be framed for, which is the same message every
    /// time and not the peer's doing.
    pub unframed: usize,
}

/// One established link to one peer.
#[derive(Debug)]
struct Link {
    /// Which link this is, so that a reader finishing after it has
    /// been replaced does not tear down its successor.
    epoch: u64,
    /// Which end of it this node is.
    role: Role,
    /// What to hand a message to.
    outgoing: crate::link::Outgoing,
    reader: AbortHandle,
    writer: AbortHandle,
}

impl Drop for Link {
    fn drop(&mut self) {
        self.reader.abort();
        self.writer.abort();
    }
}

/// What is known about one address this node dials.
#[derive(Debug)]
struct Attempt {
    /// When it may be tried again.
    next: Instant,
    /// How long the wait after the next failure will be.
    delay: Duration,
    /// Whether a dial to it is running now.
    running: bool,
    /// Who answered there, once anything has.
    peer: Option<NodeId>,
    /// Whether dialling it is pointless.
    ///
    /// Set when this node's own identity answered. Retrying that for
    /// ever would be a log line a second about a seed that is this
    /// node.
    hopeless: bool,
}

/// Everything a running mesh shares between its tasks.
struct State {
    identity: NodeIdentity,
    config: ClusterConfig,
    settings: MeshSettings,
    transport: Arc<dyn Transport>,
    members: Mutex<Membership>,
    links: Mutex<HashMap<NodeId, Link>>,
    dialling: Mutex<HashMap<String, Attempt>>,
    /// Addresses whose refusal has already been logged.
    ///
    /// Section 8.1: a mismatched secret is logged **once per peer**
    /// rather than once per attempt. Keyed by the address rather than
    /// by the identity, because a handshake that did not authenticate
    /// has not established one — taking the identity it offered would
    /// be letting a stranger choose which log line to suppress.
    refused: Mutex<HashSet<String>>,
    epochs: AtomicU64,
    beats: AtomicU64,
    sent: Arc<AtomicU64>,
    received: Arc<AtomicU64>,
    /// What links that have gone dropped, kept because their counters
    /// go with them.
    dropped_retired: AtomicU64,
    /// What to do with what a peer says, on a node that replicates.
    ///
    /// `None` is a mesh that reads frames and throws them away, which is
    /// what section 8.1's own tests want: a heartbeat changes membership
    /// and nothing else, and a handler would be a second thing to stand
    /// up to assert it.
    deliveries: Option<Arc<dyn Deliver>>,
}

fn locked<T>(what: &Mutex<T>) -> MutexGuard<'_, T> {
    what.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A running mesh.
///
/// Dropping it stops everything: every task is aborted and every link
/// is closed.
pub struct Mesh {
    state: Arc<State>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Mesh {
    /// Start dialling, accepting and beating.
    ///
    /// The transport is already bound; nothing here binds anything, so
    /// a caller that got as far as calling this cannot fail on an
    /// address in use afterwards.
    pub fn start(
        identity: NodeIdentity,
        config: ClusterConfig,
        settings: MeshSettings,
        transport: Arc<dyn Transport>,
    ) -> Arc<Self> {
        Self::start_delivering(identity, config, settings, transport, None)
    }

    /// The same, handing what peers say to `deliveries`.
    ///
    /// What the binary calls once section 8.2's replication is wired, and
    /// what [`Mesh::start`] is in terms of. Two entry points rather than
    /// one with an option everywhere, because every test of section 8.1
    /// wants the one without.
    pub fn start_delivering(
        identity: NodeIdentity,
        config: ClusterConfig,
        settings: MeshSettings,
        transport: Arc<dyn Transport>,
        deliveries: Option<Arc<dyn Deliver>>,
    ) -> Arc<Self> {
        let state = Arc::new(State {
            members: Mutex::new(Membership::new(settings.detection)),
            links: Mutex::new(HashMap::new()),
            dialling: Mutex::new(HashMap::new()),
            refused: Mutex::new(HashSet::new()),
            epochs: AtomicU64::new(0),
            beats: AtomicU64::new(0),
            sent: Arc::new(AtomicU64::new(0)),
            received: Arc::new(AtomicU64::new(0)),
            dropped_retired: AtomicU64::new(0),
            deliveries,
            identity,
            config,
            settings,
            transport,
        });
        let tasks = vec![
            tokio::spawn(accepting(Arc::clone(&state))),
            tokio::spawn(dialling(Arc::clone(&state))),
            tokio::spawn(beating(Arc::clone(&state))),
        ];
        Arc::new(Self {
            state,
            tasks: Mutex::new(tasks),
        })
    }

    /// Who this node is.
    #[must_use]
    pub fn identity(&self) -> &NodeIdentity {
        &self.state.identity
    }

    /// What this node tells peers to dial.
    #[must_use]
    pub fn advertise(&self) -> &str {
        &self.state.settings.advertise
    }

    /// What the transport under it actually bound.
    #[must_use]
    pub fn address(&self) -> String {
        self.state.transport.address()
    }

    /// Every peer this node has met, in identity order.
    #[must_use]
    pub fn members(&self) -> Vec<Member> {
        locked(&self.state.members).members().cloned().collect()
    }

    /// How many of them are answering.
    #[must_use]
    pub fn alive(&self) -> usize {
        locked(&self.state.members).alive()
    }

    /// Which end of the link to this peer this node is, if there is
    /// one.
    ///
    /// What [`keeper`] decides, after the fact. For a test, and for an
    /// operator asking why two nodes have one link rather than two.
    #[must_use]
    pub fn link_role(&self, peer: NodeId) -> Option<Role> {
        locked(&self.state.links).get(&peer).map(|link| link.role)
    }

    /// Every peer this node has a link to right now, in identity order.
    ///
    /// **Links, not members.** A member that is dead still has a row and
    /// may still have a socket; what a broadcast can reach is what there
    /// is a queue for, and that is this.
    #[must_use]
    pub fn linked(&self) -> Vec<NodeId> {
        let mut peers: Vec<NodeId> = locked(&self.state.links).keys().copied().collect();
        peers.sort();
        peers
    }

    /// Hand `message` to one peer, if there is a link to it.
    ///
    /// `None` is a peer with no link. Otherwise what the queue did with
    /// it, which may be [`Sent::Dropped`]: section 8.1 drops and counts
    /// rather than letting a wedged peer block whoever had something to
    /// say to it.
    ///
    /// # Errors
    ///
    /// [`crate::FrameError`] when the message cannot be framed, which is
    /// this build refusing to write what it would not read back rather
    /// than anything the peer did.
    pub fn send(&self, peer: NodeId, message: &Message) -> Option<Result<Sent, FrameError>> {
        let queue = locked(&self.state.links)
            .get(&peer)
            .map(|link| link.outgoing.clone())?;
        Some(queue.send(message))
    }

    /// Hand `message` to every peer there is a link to.
    ///
    /// **No queue of its own, and that is the point.** A peer whose link
    /// has gone is not in the table, so it receives nothing and nothing
    /// is kept for it: repairing what a broadcast lost is section 8.2's
    /// exchange, and a broadcast that replayed a backlog on reconnection
    /// would send every entry in it twice to every peer that never left.
    ///
    /// Collected under the lock and sent outside it, as the heartbeat is
    /// and for the same reason: `send` never waits, but holding the link
    /// table while touching every peer would put every accept and every
    /// loss behind one write.
    pub fn broadcast(&self, message: &Message) -> Fanout {
        let queues: Vec<crate::link::Outgoing> = locked(&self.state.links)
            .values()
            .map(|link| link.outgoing.clone())
            .collect();
        let mut fanout = Fanout::default();
        for queue in queues {
            match queue.send(message) {
                Ok(Sent::Queued) => fanout.queued += 1,
                Ok(Sent::Dropped) => fanout.dropped += 1,
                Err(why) => {
                    tracing::error!(%why, "a cluster message could not be framed");
                    fanout.unframed += 1;
                }
            }
        }
        fanout
    }

    /// Section 11.3's numbers.
    #[must_use]
    pub fn stats(&self) -> MeshStats {
        let links = locked(&self.state.links);
        let dropped = self.state.dropped_retired.load(Ordering::Relaxed)
            + links
                .values()
                .map(|link| link.outgoing.dropped_bytes())
                .sum::<u64>();
        let established = links.len();
        drop(links);
        let members = locked(&self.state.members);
        MeshStats {
            members: members.len(),
            alive: members.alive(),
            links: established,
            bytes_sent: self.state.sent.load(Ordering::Relaxed),
            bytes_received: self.state.received.load(Ordering::Relaxed),
            bytes_dropped: dropped,
        }
    }

    /// Stop dialling, accepting and beating, and close every link.
    pub fn shutdown(&self) {
        for task in locked(&self.tasks).drain(..) {
            task.abort();
        }
        locked(&self.state.links).clear();
    }
}

impl std::fmt::Debug for Mesh {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mesh")
            .field("identity", &self.state.identity)
            .field("advertise", &self.state.settings.advertise)
            .finish_non_exhaustive()
    }
}

impl Drop for Mesh {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ---------------------------------------------------------------
// The three tasks
// ---------------------------------------------------------------

/// Answer connections, authenticate them, and keep the ones worth
/// keeping.
async fn accepting(state: Arc<State>) {
    loop {
        let stream = match state.transport.accept().await {
            Ok(stream) => stream,
            Err(why) => {
                tracing::warn!(%why, "the cluster listener stopped accepting");
                return;
            }
        };
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let mut stream = stream;
            match handshake::accept(
                &mut stream,
                &state.config,
                &state.identity,
                &state.settings.advertise,
            )
            .await
            {
                Ok(peer) => {
                    install(&state, stream, peer, Role::Listener);
                }
                Err(why) => {
                    refuse(&state, None, &why);
                }
            }
        });
    }
}

/// Dial the seeds, and the peers whose links have gone.
async fn dialling(state: Arc<State>) {
    loop {
        for address in targets(&state) {
            consider(&state, address);
        }
        tokio::time::sleep(state.settings.detection.heartbeat).await;
    }
}

/// Every address worth dialling: the seeds, and the advertised address
/// of every peer this node has met and has no link to.
///
/// The second half is what the advertised address is *for*. A node
/// that accepted a link from a peer it was never seeded with has no
/// other way to reach it again, and the address that link arrived from
/// is the peer's ephemeral port rather than anything dialable.
fn targets(state: &Arc<State>) -> Vec<String> {
    let mut addresses = state.settings.seeds.clone();
    let linked: HashSet<NodeId> = locked(&state.links).keys().copied().collect();
    for member in locked(&state.members).members() {
        if !linked.contains(&member.identity.id()) && !member.advertised.is_empty() {
            addresses.push(member.advertised.clone());
        }
    }
    addresses.sort();
    addresses.dedup();
    addresses
}

/// Dial `address` if it is due, is not already being dialled, and is
/// not already answered by a peer this node is linked to.
fn consider(state: &Arc<State>, address: String) {
    let now = Instant::now();
    {
        let mut dialling = locked(&state.dialling);
        let attempt = dialling.entry(address.clone()).or_insert_with(|| Attempt {
            next: now,
            delay: state.settings.reconnect_min,
            running: false,
            peer: None,
            hopeless: false,
        });
        if attempt.running || attempt.hopeless || now < attempt.next {
            return;
        }
        if let Some(peer) = attempt.peer {
            if locked(&state.links).contains_key(&peer) {
                // Already linked to whoever is there. Nothing to do,
                // and the delay is forgotten so that the next loss is
                // retried promptly.
                attempt.delay = state.settings.reconnect_min;
                return;
            }
        }
        attempt.running = true;
    }

    let state = Arc::clone(state);
    tokio::spawn(async move {
        let outcome = attempt_dial(&state, &address).await;
        let mut dialling = locked(&state.dialling);
        let Some(attempt) = dialling.get_mut(&address) else {
            return;
        };
        attempt.running = false;
        match outcome {
            Ok(peer) => {
                attempt.peer = Some(peer);
                attempt.delay = state.settings.reconnect_min;
                attempt.next = Instant::now();
            }
            Err(Refusal::OurOwnIdentity) => {
                attempt.hopeless = true;
            }
            Err(Refusal::Secret) => {
                attempt.delay = state.settings.refused_backoff;
                attempt.next = Instant::now() + jitter(state.settings.refused_backoff);
            }
            Err(Refusal::Transport) => {
                attempt.next = Instant::now() + jitter(attempt.delay);
                attempt.delay = (attempt.delay * 2).min(state.settings.reconnect_max);
            }
        }
    });
}

/// Why a dial did not end in a link.
///
/// Three, because three different waits follow: none at all, the
/// backoff a transport failure earns, and the harder one section 8.1
/// gives a refused secret.
enum Refusal {
    /// The connection or the handshake failed.
    Transport,
    /// The peer would not accept this node's secret, or this node
    /// would not accept the peer's.
    Secret,
    /// This node answered its own dial.
    OurOwnIdentity,
}

async fn attempt_dial(state: &Arc<State>, address: &str) -> Result<NodeId, Refusal> {
    let mut stream = match state.transport.dial(address).await {
        Ok(stream) => stream,
        Err(why) => {
            tracing::debug!(address, %why, "a cluster peer could not be dialled");
            return Err(Refusal::Transport);
        }
    };
    match handshake::dial(
        &mut stream,
        &state.config,
        &state.identity,
        &state.settings.advertise,
    )
    .await
    {
        Ok(peer) => {
            let id = peer.identity.id();
            install(state, stream, peer, Role::Dialler);
            Ok(id)
        }
        Err(why) => {
            let refusal = refuse(state, Some(address), &why);
            Err(refusal)
        }
    }
}

/// Say why a handshake failed, once per peer where saying it every
/// time would be noise, and classify it for the caller's backoff.
fn refuse(state: &Arc<State>, address: Option<&str>, why: &HandshakeError) -> Refusal {
    match why {
        HandshakeError::Unauthenticated { role } => {
            // Once per peer rather than once per attempt, section 8.1.
            let key = address.unwrap_or("an inbound connection");
            let first = locked(&state.refused).insert(key.to_owned());
            if first {
                tracing::error!(
                    peer = key,
                    ?role,
                    "a cluster peer does not hold this cluster's secret, so no link will \
                     form; this is a configuration error and will not fix itself. Give \
                     every node the same cluster.secret and the same cluster.name."
                );
            }
            Refusal::Secret
        }
        HandshakeError::OurOwnIdentity { id } => {
            tracing::error!(
                %id,
                peer = address.unwrap_or("an inbound connection"),
                "this address answered with this node's own identity. If it is not this \
                 node's own seed, it is a copied store directory or a cloned image, and \
                 two nodes with one identity is what conflict resolution cannot survive."
            );
            Refusal::OurOwnIdentity
        }
        other => {
            tracing::debug!(
                peer = address.unwrap_or("an inbound connection"),
                why = %other,
                "a cluster handshake did not complete"
            );
            Refusal::Transport
        }
    }
}

/// Beat once per interval, and age what has not answered.
async fn beating(state: Arc<State>) {
    loop {
        tokio::time::sleep(state.settings.detection.heartbeat).await;
        let beat = state.beats.fetch_add(1, Ordering::Relaxed);
        let message = Message::Heartbeat { beat };
        // Collected under the lock and sent outside it: `send` never
        // waits, but holding the link table while touching every peer
        // would put every accept and every loss behind the beat.
        let queues: Vec<crate::link::Outgoing> = locked(&state.links)
            .values()
            .map(|link| link.outgoing.clone())
            .collect();
        for queue in queues {
            match queue.send(&message) {
                Ok(Sent::Queued) => {}
                Ok(Sent::Dropped) => {
                    tracing::warn!("a peer's queue was full, so its heartbeat was dropped");
                }
                Err(why) => tracing::error!(%why, "a heartbeat could not be framed"),
            }
        }
        for (identity, state) in locked(&state.members).age(Instant::now()) {
            match state {
                MemberState::Suspect => tracing::warn!(
                    peer = %identity,
                    "a cluster peer has missed its heartbeats and is suspect"
                ),
                MemberState::Dead => tracing::warn!(
                    peer = %identity,
                    "a cluster peer has been silent past the window and is dead"
                ),
                MemberState::Alive => tracing::info!(peer = %identity, "a cluster peer is back"),
            }
        }
    }
}

// ---------------------------------------------------------------
// Links
// ---------------------------------------------------------------

/// What happened to a link that authenticated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Installed {
    /// It is this pair's link now.
    Kept,
    /// The pair already had the link the rule says to keep.
    Dropped,
}

/// Take an authenticated connection, unless this pair already has the
/// one the rule says to keep.
fn install(state: &Arc<State>, stream: BoxStream, peer: handshake::Peer, role: Role) -> Installed {
    let id = peer.identity.id();
    let wanted = keeper(state.identity.id(), id);
    let mut links = locked(&state.links);
    if let Some(existing) = links.get(&id) {
        // A lone link is never dropped for its role; the rule breaks a
        // tie and nothing else. Here there are two, so it decides.
        if existing.role == wanted || role != wanted {
            tracing::debug!(
                peer = %peer.identity,
                ?role,
                "two links to one peer, so the one the lower identity dialled is kept"
            );
            drop(links);
            drop(stream);
            return Installed::Dropped;
        }
    }

    let epoch = state.epochs.fetch_add(1, Ordering::Relaxed) + 1;
    let (queue, outbox) = outgoing(&state.settings.link);
    let (reading, writing) = tokio::io::split(stream);
    let reading = Counted::new(reading, Arc::clone(&state.received));
    let writing = Counted::new(writing, Arc::clone(&state.sent));

    let link = state.settings.link;
    let writer = tokio::spawn(async move {
        if let Err(why) = pump(writing, outbox, &link).await {
            tracing::debug!(%why, "a cluster link's writer stopped");
        }
    });
    let reader = tokio::spawn(reading_from(
        Arc::clone(state),
        peer.identity.clone(),
        epoch,
        FrameReader::new(reading, state.settings.link.frames),
    ));

    if let Some(previous) = links.insert(
        id,
        Link {
            epoch,
            role,
            outgoing: queue,
            reader: reader.abort_handle(),
            writer: writer.abort_handle(),
        },
    ) {
        // **What it dropped and what it never got to write.** Replacing a
        // link aborts its writer, so whatever is still queued for it is
        // lost; counting only what the queue had refused would report a
        // simultaneous dial as having lost nothing. A heartbeat lost here
        // is invisible, but section 8.2's replication goes through the
        // same queue, and the first thing to notice this was a broadcast
        // that vanished because it was queued a millisecond before the
        // rule chose the other link.
        state.dropped_retired.fetch_add(
            previous.outgoing.dropped_bytes() + previous.outgoing.pending_bytes(),
            Ordering::Relaxed,
        );
    }
    drop(links);

    locked(&state.members).heard(&peer.identity, &peer.advertised, Instant::now());
    tracing::info!(peer = %peer.identity, advertised = %peer.advertised, ?role, "a cluster link is up");
    Installed::Kept
}

/// Read from one link until it ends, and then take it down.
async fn reading_from(
    state: Arc<State>,
    identity: NodeIdentity,
    epoch: u64,
    mut reader: FrameReader<Counted<tokio::io::ReadHalf<BoxStream>>>,
) {
    let advertised = locked(&state.members)
        .member(identity.id())
        .map(|member| member.advertised.clone())
        .unwrap_or_default();
    loop {
        match reader.next::<Message>().await {
            Ok(Some(message)) => {
                // **Anything, not only a beat.** A link carrying
                // traffic is a link whose peer is answering; the
                // heartbeat is what covers the links that are not.
                locked(&state.members).heard(&identity, &advertised, Instant::now());
                // Counted as a sign of life first and handled second, so
                // that a handler which refuses a frame has not also made
                // the peer look dead.
                if let Some(deliveries) = &state.deliveries {
                    for reply in deliveries.delivered(identity.id(), message) {
                        // Back to the sender and nowhere else. A reply to
                        // a peer whose link went in the meantime is a
                        // drop, counted like any other.
                        let queue = locked(&state.links)
                            .get(&identity.id())
                            .map(|link| link.outgoing.clone());
                        match queue.map(|queue| queue.send(&reply)) {
                            Some(Ok(Sent::Queued)) | None => {}
                            Some(Ok(Sent::Dropped)) => tracing::warn!(
                                peer = %identity,
                                "a peer's queue was full, so the answer to it was dropped"
                            ),
                            Some(Err(why)) => {
                                tracing::error!(%why, "an answer to a peer could not be framed");
                            }
                        }
                    }
                }
            }
            Ok(None) => {
                tracing::info!(peer = %identity, "a cluster peer closed its link");
                break;
            }
            Err(why) => {
                tracing::warn!(peer = %identity, %why, "a cluster link failed");
                break;
            }
        }
    }

    let id = identity.id();
    let mut links = locked(&state.links);
    // **The epoch, not merely the presence of a link.** This reader may
    // be the losing half of a simultaneous dial, whose peer is very
    // much there; removing the winner because the loser ended would
    // declare a live peer dead and then redial it.
    //
    // Convention 7, and convention 5's "ask what a test cannot reach":
    // replacing the comparison with `true` survives the suite, and it
    // has to. Installing a replacement aborts the loser's reader, so
    // the ordinary path never reaches here twice. What this guards is
    // the window between this reader seeing its stream end and taking
    // this lock: an abort only lands at an await point, and there is
    // none left between those two. A test cannot schedule that window,
    // so this is written down rather than guarded.
    let ours = links.get(&id).is_some_and(|link| link.epoch == epoch);
    if ours {
        if let Some(link) = links.remove(&id) {
            state.dropped_retired.fetch_add(
                link.outgoing.dropped_bytes() + link.outgoing.pending_bytes(),
                Ordering::Relaxed,
            );
        }
    }
    drop(links);
    if ours {
        // Only the link that is still this pair's may declare the peer
        // gone. A reader finishing after it has been replaced is the
        // losing half of a simultaneous dial, and its peer is very
        // much there.
        locked(&state.members).gone(id);
    }
}

// ---------------------------------------------------------------
// Odds and ends
// ---------------------------------------------------------------

/// How long to actually wait, given how long the backoff says.
///
/// Section 8.1 asks for jitter, and the reason is that a node coming
/// back finds every peer dialling it in the same millisecond
/// otherwise. Between half the delay and all of it, so a backoff never
/// becomes no backoff.
fn jitter(delay: Duration) -> Duration {
    let nanos = u64::try_from(delay.as_nanos()).unwrap_or(u64::MAX);
    let half = nanos / 2;
    if half == 0 {
        return delay;
    }
    let mut bytes = [0_u8; 8];
    if SystemRandom::new().fill(&mut bytes).is_err() {
        // No randomness is not a reason to hammer an address; the
        // unjittered delay is the safe answer.
        return delay;
    }
    Duration::from_nanos(half + u64::from_le_bytes(bytes) % half)
}

/// A stream that counts what passes through it.
///
/// Section 11.3's cluster bytes sent and received. Wrapped around the
/// halves rather than counted where frames are made, because what the
/// section asks for is bytes on the link and a frame's own length is
/// not the same number once the link batches them.
#[derive(Debug)]
pub struct Counted<T> {
    inner: T,
    tally: Arc<AtomicU64>,
}

impl<T> Counted<T> {
    /// Count what goes through `inner` into `tally`.
    pub fn new(inner: T, tally: Arc<AtomicU64>) -> Self {
        Self { inner, tally }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for Counted<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buffer.filled().len();
        let this = self.get_mut();
        let ready = Pin::new(&mut this.inner).poll_read(context, buffer);
        if ready.is_ready() {
            let read = buffer.filled().len().saturating_sub(before);
            this.tally.fetch_add(read as u64, Ordering::Relaxed);
        }
        ready
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Counted<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let ready = Pin::new(&mut this.inner).poll_write(context, buffer);
        if let Poll::Ready(Ok(written)) = &ready {
            this.tally.fetch_add(*written as u64, Ordering::Relaxed);
        }
        ready
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(context)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use storage::Secret;

    use crate::config::ClusterSettings;
    use crate::testing::memory::Switch;

    /// Fast enough that a test is not a wait, slow enough that a
    /// loaded machine does not declare a live peer dead.
    const BEAT: Duration = Duration::from_millis(25);

    /// How long to wait for something that should happen.
    const PATIENCE: Duration = Duration::from_secs(10);

    /// The secret every node in these tests shares.
    const SECRET: &str = "the-cluster-secret";

    fn config(secret: &str) -> ClusterConfig {
        ClusterSettings {
            name: "production".to_owned(),
            secret: Some(Secret::new(secret)),
        }
        .check()
        .expect("a usable configuration")
    }

    fn settings(advertise: &str, seeds: &[&str]) -> MeshSettings {
        MeshSettings {
            advertise: advertise.to_owned(),
            seeds: seeds.iter().map(|seed| (*seed).to_owned()).collect(),
            detection: Detection {
                heartbeat: BEAT,
                suspect_after: 3,
                dead_after: BEAT * 8,
            },
            reconnect_min: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(60),
            refused_backoff: Duration::from_millis(200),
            link: LinkConfig::default(),
        }
    }

    /// A node on the switch, listening under `name` and advertising it.
    fn node(
        switch: &Arc<Switch>,
        name: &str,
        seeds: &[&str],
        identity: &NodeIdentity,
        secret: &str,
    ) -> Arc<Mesh> {
        let transport = Arc::new(switch.listen(name));
        Mesh::start(
            identity.clone(),
            config(secret),
            settings(name, seeds),
            transport,
        )
    }

    async fn until(what: &str, mut holds: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + PATIENCE;
        while tokio::time::Instant::now() < deadline {
            if holds() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("{what} never happened within {PATIENCE:?}");
    }

    /// Assert that something stays true for several beats.
    ///
    /// The answer to convention 5's "a test asserting an absence": a
    /// mesh that settles on one link and then starts churning is a
    /// mesh that passes an assertion made once.
    async fn keeps(what: &str, beats: u32, mut holds: impl FnMut() -> bool) {
        for _ in 0..beats {
            assert!(holds(), "{what} stopped being true");
            tokio::time::sleep(BEAT).await;
        }
        assert!(holds(), "{what} stopped being true");
    }

    fn how_a_peer_looks(mesh: &Mesh, peer: NodeId) -> Option<MemberState> {
        mesh.members()
            .into_iter()
            .find(|member| member.identity.id() == peer)
            .map(|member| member.state)
    }

    /// Three nodes over the in-memory link all see each other.
    ///
    /// Section 14's commitment, and the shape everything after this
    /// runs over.
    #[tokio::test]
    async fn three_nodes_over_an_in_memory_link_all_see_each_other() {
        let switch = Switch::new();
        let a = node(&switch, "a", &["b", "c"], &NodeIdentity::mint("a"), SECRET);
        let b = node(&switch, "b", &["a", "c"], &NodeIdentity::mint("b"), SECRET);
        let c = node(&switch, "c", &["a", "b"], &NodeIdentity::mint("c"), SECRET);

        until("every node sees the other two", || {
            a.alive() == 2 && b.alive() == 2 && c.alive() == 2
        })
        .await;

        // One link per pair, and it stays that way: every node dialled
        // every other, so every pair authenticated twice.
        keeps("each node holds exactly two links", 6, || {
            a.stats().links == 2 && b.stats().links == 2 && c.stats().links == 2
        })
        .await;

        // And each of them knows the address the others advertise, so
        // it could dial them again without a seed.
        for member in a.members() {
            assert!(
                member.advertised == "b" || member.advertised == "c",
                "a peer advertised {:?}",
                member.advertised
            );
        }
    }

    /// A node that stops is seen dead by both others, and rejoins.
    #[tokio::test]
    async fn a_node_that_stops_is_seen_dead_by_both_others_and_rejoins() {
        let switch = Switch::new();
        let identity = NodeIdentity::mint("c");
        let a = node(&switch, "a", &["b", "c"], &NodeIdentity::mint("a"), SECRET);
        let b = node(&switch, "b", &["a", "c"], &NodeIdentity::mint("b"), SECRET);
        let c = node(&switch, "c", &["a", "b"], &identity, SECRET);
        until("every node sees the other two", || {
            a.alive() == 2 && b.alive() == 2 && c.alive() == 2
        })
        .await;

        drop(c);
        switch.forget("c");
        until("both others see it dead", || {
            a.alive() == 1
                && b.alive() == 1
                && how_a_peer_looks(&a, identity.id()) == Some(MemberState::Dead)
                && how_a_peer_looks(&b, identity.id()) == Some(MemberState::Dead)
        })
        .await;
        assert_eq!(
            a.members().len(),
            2,
            "a dead peer keeps its row, with the address to dial it at"
        );

        let c = node(&switch, "c", &["a", "b"], &identity, SECRET);
        until("it rejoins", || {
            a.alive() == 2 && b.alive() == 2 && c.alive() == 2
        })
        .await;
        assert_eq!(
            a.members().len(),
            2,
            "the node that came back is the node that went, not a third"
        );
    }

    /// Two nodes dialling each other at once settle on one link.
    #[tokio::test]
    async fn a_simultaneous_dial_settles_on_one_link() {
        let switch = Switch::new();
        let lower = NodeIdentity::mint("a");
        let higher = NodeIdentity::mint("b");
        assert!(lower.id() < higher.id(), "version 7 is time-ordered");

        let a = node(&switch, "a", &["b"], &lower, SECRET);
        let b = node(&switch, "b", &["a"], &higher, SECRET);
        until("both see each other", || a.alive() == 1 && b.alive() == 1).await;

        keeps(
            "exactly one link each, and both stay alive on it",
            8,
            || a.stats().links == 1 && b.stats().links == 1 && a.alive() == 1 && b.alive() == 1,
        )
        .await;

        // And it is the one the rule names: the lower identity dialled
        // it, so the lower identity is its dialler and the higher its
        // listener.
        assert_eq!(a.link_role(higher.id()), Some(Role::Dialler));
        assert_eq!(b.link_role(lower.id()), Some(Role::Listener));
    }

    /// The rule itself, as a function of the two identities.
    ///
    /// Total and antisymmetric, which is what makes both ends drop the
    /// same connection without another message.
    #[test]
    fn the_link_kept_is_the_one_the_lower_identity_dialled() {
        let lower = NodeId::mint();
        let higher = NodeId::mint();
        assert!(lower < higher);
        assert_eq!(keeper(lower, higher), Role::Dialler);
        assert_eq!(keeper(higher, lower), Role::Listener);
    }

    /// A lone link is kept whichever role it has.
    ///
    /// The rule breaks a tie; it does not forbid a link that has no
    /// rival. Here only the **higher** identity has an address to
    /// dial, so the one link between them has the role the rule would
    /// have discarded had there been two — and dropping it would leave
    /// two nodes that can reach each other believing they cannot.
    #[tokio::test]
    async fn a_lone_link_is_kept_although_the_rule_would_not_have_chosen_it() {
        let switch = Switch::new();
        let lower = NodeIdentity::mint("a");
        let higher = NodeIdentity::mint("b");
        assert!(lower.id() < higher.id());

        let a = node(&switch, "a", &[], &lower, SECRET);
        let b = node(&switch, "b", &["a"], &higher, SECRET);

        until("both see each other", || a.alive() == 1 && b.alive() == 1).await;
        assert_eq!(
            b.link_role(lower.id()),
            Some(Role::Dialler),
            "the higher identity dialled, which is not the role the rule prefers"
        );
        keeps("and the link is not dropped for it", 6, || {
            a.alive() == 1 && b.alive() == 1
        })
        .await;
    }

    /// A peer this node met without a seed is dialled again at the
    /// address it advertised.
    ///
    /// Section 8.1: seeds alone are not enough for a full mesh. Here
    /// `a` was never told where `b` is; it learnt that from the
    /// greeting, and it is the only way it can bring the link back.
    #[tokio::test]
    async fn a_peer_met_without_a_seed_is_redialled_at_the_address_it_advertised() {
        let switch = Switch::new();
        let identity = NodeIdentity::mint("b");
        let a = node(&switch, "a", &[], &NodeIdentity::mint("a"), SECRET);
        let b = node(&switch, "b", &["a"], &identity, SECRET);
        until("they meet", || a.alive() == 1 && b.alive() == 1).await;
        assert_eq!(
            a.members()[0].advertised,
            "b",
            "the address came from the greeting, not from the connection"
        );

        drop(b);
        switch.forget("b");
        until("a sees it go", || a.alive() == 0).await;

        // It comes back with no seeds at all, so nothing it does can
        // re-establish the link. Only `a` can, and only from what `b`
        // advertised.
        let b = node(&switch, "b", &[], &identity, SECRET);
        until("a dials it again at the advertised address", || {
            a.alive() == 1 && b.alive() == 1
        })
        .await;
    }

    /// A peer whose link is open and silent is declared dead by the
    /// heartbeat — and only at the end that cannot hear it.
    ///
    /// The case the heartbeat exists for, and the one a closed socket
    /// cannot reach: the connection is still established throughout,
    /// which the link count says, and the disagreement between the two
    /// ends lasts for as long as the cut does.
    #[tokio::test]
    async fn a_peer_that_is_open_and_silent_is_declared_dead_at_one_end_only() {
        let switch = Switch::new();
        let a_identity = NodeIdentity::mint("a");
        let b_identity = NodeIdentity::mint("b");
        let a = node(&switch, "a", &["b"], &a_identity, SECRET);
        let b = node(&switch, "b", &["a"], &b_identity, SECRET);
        until("they meet", || a.alive() == 1 && b.alive() == 1).await;

        // Nothing b says reaches a. The connection is untouched.
        switch.cut("b", "a");
        until("a declares b dead", || {
            how_a_peer_looks(&a, b_identity.id()) == Some(MemberState::Dead)
        })
        .await;
        assert!(
            switch.valve("b", "a").swallowed() > 0,
            "the cut has to have bitten: nothing of b's was thrown away"
        );
        assert_eq!(
            how_a_peer_looks(&b, a_identity.id()),
            Some(MemberState::Alive),
            "b still hears a, so b must still think a is there; that disagreement \
             is the whole of a one-way partition"
        );
        assert_eq!(
            a.stats().links,
            1,
            "the link is still open: a closed socket would be the other signal"
        );

        switch.heal("b", "a");
        until("a hears b again", || {
            how_a_peer_looks(&a, b_identity.id()) == Some(MemberState::Alive)
        })
        .await;
        assert_eq!(
            a.stats().links,
            1,
            "and it healed on the connection it was cut on"
        );
    }

    /// A node whose secret does not match forms no link, and one whose
    /// secret does forms one.
    ///
    /// Both halves in one test on purpose: an assertion that nothing
    /// happened cannot tell a refused handshake from a mesh that never
    /// dialled anything.
    #[tokio::test]
    async fn a_mismatched_secret_forms_no_link_and_a_matching_one_does() {
        let switch = Switch::new();
        let a = node(&switch, "a", &["b", "c"], &NodeIdentity::mint("a"), SECRET);
        let _b = node(
            &switch,
            "b",
            &["a"],
            &NodeIdentity::mint("b"),
            "another-secret",
        );
        let _c = node(&switch, "c", &["a"], &NodeIdentity::mint("c"), SECRET);

        until("the node that holds the secret is linked", || {
            a.alive() == 1
        })
        .await;
        keeps("and the one that does not never is", 8, || a.alive() == 1).await;
        assert_eq!(
            a.members().len(),
            1,
            "a peer that could not prove itself must not be a member"
        );
    }

    /// Section 11.3's cluster bytes move, and nothing is dropped on a
    /// link nobody is overrunning.
    #[tokio::test]
    async fn the_cluster_byte_counters_move_in_both_directions() {
        let switch = Switch::new();
        let a = node(&switch, "a", &["b"], &NodeIdentity::mint("a"), SECRET);
        let b = node(&switch, "b", &["a"], &NodeIdentity::mint("b"), SECRET);
        until("they meet", || a.alive() == 1 && b.alive() == 1).await;

        until("beats have been sent and heard", || {
            let stats = a.stats();
            stats.bytes_sent > 0 && stats.bytes_received > 0
        })
        .await;
        let stats = a.stats();
        assert_eq!(stats.members, 1);
        assert_eq!(stats.alive, 1);
        assert_eq!(
            stats.bytes_dropped, 0,
            "nothing is overrunning this link, so nothing may be counted as dropped"
        );
        assert!(b.stats().bytes_received > 0);
    }

    /// A node dialling itself says so once and stops.
    ///
    /// A seed list that names this node is an ordinary configuration —
    /// the same file on every node is how a cluster is deployed — and
    /// retrying it for ever would be a log line a second.
    #[tokio::test]
    async fn a_seed_that_is_this_node_is_given_up_on() {
        let switch = Switch::new();
        let a = node(&switch, "a", &["a", "b"], &NodeIdentity::mint("a"), SECRET);
        let b = node(&switch, "b", &["a"], &NodeIdentity::mint("b"), SECRET);
        until("it still meets the peer that is not itself", || {
            a.alive() == 1 && b.alive() == 1
        })
        .await;
        keeps("and never counts itself as a member", 6, || {
            a.members().len() == 1
        })
        .await;
    }
    // -----------------------------------------------------------
    // What the mesh carries for section 8.2
    // -----------------------------------------------------------

    /// Every message a node was handed, and what it answered with.
    ///
    /// A [`Deliver`] that records rather than replicates, because what is
    /// being proved here is the mesh's half: that a broadcast reaches
    /// every peer there is a link to, that it reaches **only** those, and
    /// that an answer goes back to whoever asked. What a replicated frame
    /// means is `crate::replicate`'s and is tested there, without a
    /// socket.
    #[derive(Debug, Default)]
    struct Recorder {
        heard: Mutex<Vec<(NodeId, Message)>>,
        /// What to answer the next message with, once.
        answer: Mutex<Option<Message>>,
    }

    impl Recorder {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn answering(answer: Message) -> Arc<Self> {
            Arc::new(Self {
                heard: Mutex::new(Vec::new()),
                answer: Mutex::new(Some(answer)),
            })
        }

        /// Every replicated frame this node was handed, heartbeats aside.
        fn replications(&self) -> Vec<Message> {
            locked(&self.heard)
                .iter()
                .filter(|(_, message)| matches!(message, Message::Replicate(_)))
                .map(|(_, message)| message.clone())
                .collect()
        }
    }

    impl Deliver for Recorder {
        fn delivered(&self, from: NodeId, message: Message) -> Vec<Message> {
            locked(&self.heard).push((from, message.clone()));
            // Only a replicated frame is answered, so that a heartbeat
            // does not consume the one answer. The guard is taken once and
            // dropped before anything else: `locked` recovers a poisoned
            // lock rather than failing, so taking it twice on one thread
            // is a deadlock and not a panic.
            let mut held = locked(&self.answer);
            match (&message, held.is_some()) {
                (Message::Replicate(_), true) => vec![held.take().expect("just checked")],
                _ => Vec::new(),
            }
        }
    }

    /// A frame to broadcast, carrying a tombstone nobody will apply.
    ///
    /// The contents are deliberately uninteresting: what is asserted here
    /// is which nodes were handed it.
    fn replication(sequence: u64) -> Message {
        Message::Replicate(crate::replicate::Replication::Broadcast {
            entry: storage::LoggedRecord {
                sequence,
                entry: storage::MetadataRecord {
                    key: storage::MetadataKeyRecord::Retained {
                        topic: "a/b".to_owned(),
                    },
                    stamp: storage::StampRecord {
                        millis: 1,
                        counter: 0,
                        node: [1; 16],
                    },
                    value: None,
                },
            },
        })
    }

    /// A node on the switch, handing what peers say to `deliveries`.
    fn node_delivering(
        switch: &Arc<Switch>,
        name: &str,
        seeds: &[&str],
        identity: &NodeIdentity,
        deliveries: Arc<dyn Deliver>,
    ) -> Arc<Mesh> {
        let transport = Arc::new(switch.listen(name));
        Mesh::start_delivering(
            identity.clone(),
            config(SECRET),
            settings(name, seeds),
            transport,
            Some(deliveries),
        )
    }

    /// A broadcast reaches every peer there is a link to, and one answers.
    ///
    /// Three claims in one, because standing three meshes up is the
    /// expensive part: the fan-out counts the peers it queued for, both
    /// peers are handed the frame exactly once, and the answer one of them
    /// returns goes back to the node that broadcast and to nobody else.
    #[tokio::test]
    async fn a_broadcast_reaches_every_peer_and_an_answer_comes_back() {
        let switch = Switch::new();
        let heard_by_a = Recorder::new();
        let heard_by_b = Recorder::answering(replication(99));
        let heard_by_c = Recorder::new();
        let a = node_delivering(
            &switch,
            "a",
            &["b", "c"],
            &NodeIdentity::mint("a"),
            Arc::clone(&heard_by_a) as Arc<dyn Deliver>,
        );
        let b = node_delivering(
            &switch,
            "b",
            &["a", "c"],
            &NodeIdentity::mint("b"),
            Arc::clone(&heard_by_b) as Arc<dyn Deliver>,
        );
        let c = node_delivering(
            &switch,
            "c",
            &["a", "b"],
            &NodeIdentity::mint("c"),
            Arc::clone(&heard_by_c) as Arc<dyn Deliver>,
        );
        until("every node sees the other two", || {
            a.alive() == 2 && b.alive() == 2 && c.alive() == 2
        })
        .await;
        // **Waited for, and this is the finding that put it here.** Two
        // nodes dialling each other produce two links and `keeper`'s rule
        // drops one of them, which aborts its writer — so a message
        // queued a moment before the rule chose is never written and the
        // peer never hears it. Three links and two live peers is true
        // while that is still settling, so broadcasting there loses the
        // frame, which is exactly what this test did until it waited.
        //
        // The loss is now counted rather than silent, which is what
        // section 8.1 asks of a drop; see `crate::link::Outgoing::pending_bytes`.
        // It is not prevented, and it does not need to be: section 8.2's
        // exchange repairs anything a broadcast lost, and handing one
        // link's queue to its replacement would mean deciding what a
        // half-written batch means to a peer that has already read part
        // of it.
        keeps("the links have settled on one per pair", 6, || {
            a.stats().links == 2 && b.stats().links == 2 && c.stats().links == 2
        })
        .await;
        assert_eq!(a.linked().len(), 2, "a broadcast can reach two peers");

        let frame = replication(1);
        let fanout = a.broadcast(&frame);
        assert_eq!(
            fanout,
            Fanout {
                queued: 2,
                dropped: 0,
                unframed: 0
            }
        );

        until("both peers were handed it", || {
            heard_by_b.replications().len() == 1 && heard_by_c.replications().len() == 1
        })
        .await;
        assert_eq!(heard_by_b.replications(), vec![frame.clone()]);
        assert_eq!(heard_by_c.replications(), vec![frame]);

        // The answer goes back to whoever asked, and to nobody else. `c`
        // answered nothing, so a frame reaching `c` twice would be a
        // broadcast of an answer rather than a reply to one.
        until("the answer came back", || {
            heard_by_a.replications() == vec![replication(99)]
        })
        .await;
        keeps("and nothing else moved", 4, || {
            heard_by_a.replications().len() == 1
                && heard_by_b.replications().len() == 1
                && heard_by_c.replications().len() == 1
        })
        .await;
    }

    /// A broadcast with no peers reaches nobody and says so.
    ///
    /// The shape a node coming up alone is in, and the one a broadcast
    /// must not treat as an error: a cluster of one is a supported way to
    /// run this broker.
    #[tokio::test]
    async fn a_broadcast_with_no_peers_reaches_nobody() {
        let switch = Switch::new();
        let heard = Recorder::new();
        let alone = node_delivering(
            &switch,
            "alone",
            &[],
            &NodeIdentity::mint("alone"),
            Arc::clone(&heard) as Arc<dyn Deliver>,
        );
        assert_eq!(alone.linked(), Vec::new());
        assert_eq!(alone.broadcast(&replication(1)), Fanout::default());
        assert!(
            alone
                .send(NodeId::from_bytes([9; 16]), &replication(1))
                .is_none(),
            "a peer with no link must be reported as such rather than silently dropped"
        );
        assert_eq!(heard.replications(), Vec::new());
    }
}
