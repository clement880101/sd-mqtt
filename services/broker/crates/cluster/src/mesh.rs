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
//! # A peer that was declared dead is arriving again, not racing
//!
//! The rule breaks a tie between two **live** connections, and the
//! link this node keeps for a peer it has declared dead is not one of
//! them. It is kept on purpose — see the next section — and nothing
//! about it says whether the socket under it still goes anywhere.
//!
//! So a connection carrying the identity of a peer this node has
//! declared dead **replaces** that link rather than being judged
//! against it. Deciding it with [`keeper`] is how a node that was seen
//! dead stops being able to come back: half the time the rule names
//! the corpse, the live connection is dropped, and the dialler on this
//! side does not try either, because [`targets`] asks for peers with
//! no link and this peer has one. A restart, a deploy or a crash would
//! be permanent.
//!
//! Nothing is lost on the race this guards. A simultaneous dial is two
//! connections to a peer that has just been heard from, which is a
//! peer that is alive, and the rule applies to it unchanged.
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
//! # Members are gossiped, and gossip only says where to dial
//!
//! Section 8.1: a node learnt through a seed is reached. A node learns
//! of a peer by being seeded with its address, by being dialled by it,
//! or by being told of it by a peer it has a link to; the address it
//! dials it at is the one that peer **advertised**, which is not the
//! address it connected from — section 8.1 says so, and a node behind a
//! port mapping or behind a test's gate is the reason. Before gossip a
//! mesh was full only when every node listed every other as a seed.
//!
//! So each node tells each peer, a beat after their link comes up, and
//! one random peer every few beats, which members it is hearing and the address
//! each gave in its own greeting: [`Message::Members`]. What it is told
//! is a **rumour**, and a rumour is an address to dial and nothing
//! more. Three rules keep it that way:
//!
//! - **It never vouches.** Every link is still the mutual challenge
//!   before it is anything else, so a rumour naming a node that does not
//!   hold the secret, or an address where some other node answers, is a
//!   dial that fails or meets whoever is really there. Membership is
//!   only ever what a handshake proved.
//! - **It never overrides a member's own word.** A member's advertised
//!   address is the one it gave this node, and a rumour about a node
//!   already met is ignored; the rumour is for the node not yet met.
//! - **It never resurrects a node an operator forgot.** See
//!   [`Mesh::forget`]: a forgotten node that comes back by itself is
//!   met again like any newcomer, but a peer that still remembers it is
//!   not a reason to dial it.
//!
//! A rumour is dialled under the same backoff as a seed, and not while
//! a seed is being dialled: a seed is the operator's word and a rumour
//! is a peer's, and until a seed has answered nobody knows which node
//! it is, so a rumour of the same node at another address would be a
//! second connection for the link rule to throw away.
//!
//! What gossip is not is a replicated member list. Section 8.2 declines
//! one, and this is not one: liveness is still each node's own
//! judgement of its own links, and a rumour says only where a peer heard
//! a member answering.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// The most rumours a node holds, and the most members one gossip names.
///
/// Five times section 15's fifty-node ceiling. A list longer than this is
/// a peer that has gone wrong rather than a cluster that has grown, and
/// a bound is what keeps a peer's mistake from becoming this node's
/// memory.
pub const MAX_RUMOURS: usize = 256;

/// How many beats apart the repair gossip to one random peer is.
///
/// Section 8.1's gossip has two halves, and this is the slow one. A new
/// link is told at the first beat after it is up, which is what makes a
/// join prompt and covers every way a node can arrive; the random peer
/// is only for a list that was dropped on a full queue. Ten beats keeps
/// that repair within seconds and keeps a cluster's idle traffic what
/// it was — heartbeats — on nine beats out of ten.
pub const GOSSIP_EVERY: u64 = 10;

/// What a cluster link carries.
///
/// Section 8.3's routing arrived as a variant here rather than as a
/// second transport, which is what section 8.1 asked for: one framing,
/// one authentication step and one failure model.
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
    /// Section 8.3's routing: one publication, for this peer's own
    /// subscribers.
    ///
    /// Sent to a peer because the routing map says that peer has a
    /// matching subscriber, and to no other peer. This is not a
    /// broadcast, and the whole economy of section 8.3 is that it is
    /// not: fan-out costs one frame per interested node rather than one
    /// per node, and a node with nothing matching is not written to at
    /// all. See [`crate::forward`].
    ///
    /// **A frame is never answered with another frame.** A publication
    /// is not a question and nothing on the far end owes the sender
    /// anything: a subscriber's acknowledgement belongs to the session
    /// it was delivered to, which is held by the node that received
    /// this.
    Forward(crate::forward::Forwarded),
    /// Section 8.4's migration: "I have claimed this session; close its
    /// connection and send me what it holds."
    ///
    /// **This one is a question**, which is what makes it unlike
    /// [`Self::Forward`]: the sender is waiting, with a bound on how
    /// long. The answer is one or more [`Self::Drained`] frames, and a
    /// node that sends none is a node the claiming node accepts the
    /// connection without — see `crate::migrate`.
    Claim(crate::migrate::Claiming),
    /// One instalment of what a claimed session held.
    ///
    /// Sent back to whoever asked and to nobody else. Several per
    /// claim, so that a drain which stops partway has still moved what
    /// it managed to; the last one says so.
    Drained(crate::migrate::Drained),
    /// Section 8.6: one change to a session's queue, for a node that
    /// holds a copy of it.
    ///
    /// Sent to the followers [`crate::placement`] names and to no other
    /// peer, which is [`Self::Forward`]'s economy for the same reason:
    /// a node that holds no copy of this session has nothing to do with
    /// its queue.
    ///
    /// **Answered with [`Self::Marked`]**, which makes this unlike a
    /// forward. The answer is not an acknowledgement that the change is
    /// safe — section 8.6's synchronous mode needs that and reads it
    /// from the mark — it is the follower saying how far it has got,
    /// which is also what the lag is measured from.
    Replicated(crate::replica::Streamed),
    /// How far a follower has applied one session's stream.
    ///
    /// The highest **contiguous** position, which is the whole of what
    /// makes it worth sending: a mark that had skipped a hole would
    /// make this node prefer a copy with a message missing from the
    /// middle. See `crate::replica`.
    Marked {
        /// Whose queue.
        session: storage::SessionKey,
        /// The highest contiguous position applied.
        through: u64,
    },
    /// Section 8.6's failover, first question: "how far does your copy
    /// of this session go?"
    ///
    /// Asked of every reachable member of the session's replica set by
    /// a node whose drain came up short — the previous owner could not
    /// be reached, or did not finish — and answered with
    /// [`Self::Holds`]. A question, like [`Self::Claim`]: the asker is
    /// waiting, with a bound.
    HowFar {
        /// Whose queue.
        session: storage::SessionKey,
    },
    /// The answer: the highest contiguous position this node's copy
    /// has applied, or nothing if it holds no copy.
    ///
    /// **`None` is an answer and not a silence.** A node that holds no
    /// copy says so, because the asker is choosing between copies by
    /// their marks and a silence would be a copy it waited the whole
    /// bound for. Zero is a copy that has applied nothing, which is a
    /// different thing and is `Some(0)`.
    Holds {
        /// Whose queue.
        session: storage::SessionKey,
        /// How far, if a copy is held at all.
        through: Option<u64>,
    },
    /// Section 8.6's failover, second question: "send me your copy."
    ///
    /// Sent to the one member whose answer to [`Self::HowFar`] was
    /// furthest ahead, and answered with [`Self::Drained`] chunks under
    /// this epoch — the same chunks and the same collection as a drain
    /// from an owner, because to the node receiving them the two are
    /// the same thing: what the session held, in instalments, with the
    /// last one saying so.
    PullCopy {
        /// Whose queue.
        session: storage::SessionKey,
        /// The epoch the asker claimed under, which is what routes the
        /// chunks to the connection waiting for them.
        epoch: u64,
    },
    /// Section 8.1's gossip: the members the sender is hearing, and the
    /// address each gave in its own greeting.
    ///
    /// Sent to a peer at the first beat after its link comes up and to
    /// one random peer every [`GOSSIP_EVERY`] beats, and handled by the
    /// mesh itself rather than handed to
    /// [`Deliver`]: it is about where to dial, which is the mesh's.
    /// **A rumour, not a vouch**; see the module documentation.
    ///
    /// **Added after 0.8.0.** A node from before it will not decode this
    /// frame and closes the link, and it is sent on every link a beat
    /// after it comes up, so every node of a cluster must run a build
    /// that has it.
    Members {
        /// Who, and where to dial them. The sender and the receiver are
        /// left out; neither needs telling.
        members: Vec<Rumour>,
    },
}

/// One member, as a peer reports it: who, and where it said to dial.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rumour {
    /// Its identity's sixteen bytes.
    pub node: [u8; 16],
    /// The address it advertised in its own greeting to the sender.
    pub advertised: String,
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

    /// This node now judges `peer` to be in `state`.
    ///
    /// Section 8.2: **membership is deliberately not replicated.**
    /// Liveness is each node's own judgement about its own links, so
    /// nothing is written down when a node dies and no register covers
    /// the case — and section 8.3's routing map still has to stop
    /// forwarding to it. This is how it is told.
    ///
    /// **Said rather than reported as a change**, so that a listener
    /// does not have to track transitions to be correct. It is called
    /// when a window ages a peer, when a link's reader ends, and when a
    /// link comes up; every call is "it is this now" and the same call
    /// twice means the same thing once.
    ///
    /// Synchronous and non-blocking, like [`Self::delivered`], and
    /// called from the three tasks named above rather than from one.
    ///
    /// The default does nothing, which is what section 8.1's own tests
    /// want: a mesh that reads frames and throws them away has no map to
    /// keep in step.
    fn member_changed(&self, _peer: NodeId, _state: MemberState) {}
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
    /// Whether the peer has been sent this node's members on this link.
    ///
    /// Section 8.1's gossip goes to a new link at the **first beat after
    /// it is up**, not as it is installed. A simultaneous dial installs
    /// one link and replaces it with the other a moment later, and a
    /// frame queued on the first before its writer had run would be
    /// thrown away and counted as dropped on a link nobody overran.
    told: bool,
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
    /// Members peers have told this node of and it has not met: where
    /// each said to dial. See the module documentation.
    ///
    /// Keyed by identity, so that a member's address moving replaces
    /// the rumour rather than adding a second one. Emptied of a node the
    /// moment a link to it is up, because from then on its own greeting
    /// is what says where it is.
    rumoured: Mutex<BTreeMap<NodeId, String>>,
    /// Nodes an operator forgot, which no rumour may bring back.
    ///
    /// Cleared for a node when it is met again — when it dials this
    /// node, or a configured seed answers as it — because a node that
    /// came back by itself is a member like any other, and its own
    /// address is in its greeting.
    forgotten: Mutex<HashSet<NodeId>>,
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
    /// Whether this mesh has been stopped.
    ///
    /// **Because a handshake is a task of its own.** Dialling and
    /// accepting each hand the connection to a spawned task for the
    /// length of the handshake, and those tasks are not the three
    /// [`Mesh::shutdown`] holds, so aborting the three does not reach
    /// them. One still in flight goes on to install a link on a mesh
    /// that has stopped, with a reader and a writer nobody will ever
    /// abort, holding its end of the connection open for as long as
    /// the process lives. The peer on the other end then has a link
    /// to a node that is gone and will never see it close — which is
    /// a corpse in its link table rather than the closed socket a
    /// stopped node is supposed to be.
    stopped: AtomicBool,
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
            rumoured: Mutex::new(BTreeMap::new()),
            forgotten: Mutex::new(HashSet::new()),
            epochs: AtomicU64::new(0),
            beats: AtomicU64::new(0),
            sent: Arc::new(AtomicU64::new(0)),
            received: Arc::new(AtomicU64::new(0)),
            dropped_retired: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
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

    /// How long `peer` has been silent. `None` for a peer never met.
    pub fn silent_for(&self, peer: NodeId) -> Option<Duration> {
        locked(&self.state.members).silent_for(peer, Instant::now())
    }

    /// Take `peer` out of membership altogether, and drop its link.
    ///
    /// Section 8.6's operator override: a node that is never coming
    /// back is forgotten at once rather than after the grace, so the
    /// replica sets that included it are repaired now. The link is
    /// aborted first, because a member forgotten while its link stayed
    /// up would be re-admitted by its very next heartbeat.
    ///
    /// **A forgotten peer that comes back is re-admitted.** Its
    /// handshake inserts it like any peer met for the first time, and
    /// a configured seed is still dialled; forgetting is about this
    /// node's table, not about the other node's right to exist.
    ///
    /// **A peer's word is not enough to bring it back.** Membership is
    /// each node's own, so the other members may well still remember
    /// it and gossip it; a node that dialled it on their say-so would
    /// undo the operator's forget on the next beat. So it is marked
    /// forgotten, and no rumour of it is taken until it is met again by
    /// a link of its own.
    ///
    /// Returns whether there was anything to forget.
    pub fn forget(&self, peer: NodeId) -> bool {
        if let Some(link) = locked(&self.state.links).remove(&peer) {
            link.reader.abort();
            link.writer.abort();
        }
        let forgot = locked(&self.state.members).forget(peer);
        let rumoured = locked(&self.state.rumoured).remove(&peer).is_some();
        if forgot || rumoured {
            locked(&self.state.forgotten).insert(peer);
        }
        forgot
    }

    /// Where peers have said members this node has not met are.
    ///
    /// For a test, and for an operator asking why a node is dialling an
    /// address nobody configured.
    #[must_use]
    pub fn rumours(&self) -> Vec<(NodeId, String)> {
        locked(&self.state.rumoured)
            .iter()
            .map(|(node, address)| (*node, address.clone()))
            .collect()
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
        // Before the aborts, so that a handshake already past them
        // cannot install what nothing would take down afterwards.
        self.state.stopped.store(true, Ordering::Relaxed);
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
            // **Bounded by the dead window.** Anything that opened a
            // connection and then said nothing held this task and the
            // socket for as long as it liked; a peer silent for as long
            // as it takes to be declared dead is dead by section 8.1's
            // own rule, whether or not it ever finished greeting.
            let accepted = tokio::time::timeout(
                state.settings.detection.dead_after,
                handshake::accept(
                    &mut stream,
                    &state.config,
                    &state.identity,
                    &state.settings.advertise,
                ),
            )
            .await;
            match accepted {
                Ok(Ok(peer)) => {
                    install(&state, stream, peer, Role::Listener);
                }
                Ok(Err(why)) => {
                    refuse(&state, None, &why);
                }
                Err(_) => {
                    tracing::debug!(
                        "an inbound cluster connection did not finish its handshake within the \
                         dead window and was closed"
                    );
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

/// Every address worth dialling: the seeds, the advertised address of
/// every peer this node has met and has no link to, and where peers
/// have said the members this node has not met are.
///
/// The second part is what the advertised address is *for*. A node
/// that accepted a link from a peer it was never seeded with has no
/// other way to reach it again, and the address that link arrived from
/// is the peer's ephemeral port rather than anything dialable. The third
/// is section 8.1's "a node learnt through a seed is reached"; see the
/// module documentation for why it waits for the seeds.
fn targets(state: &Arc<State>) -> Vec<String> {
    let mut addresses = state.settings.seeds.clone();
    let linked: HashSet<NodeId> = locked(&state.links).keys().copied().collect();
    let mut met = HashSet::new();
    for member in locked(&state.members).members() {
        met.insert(member.identity.id());
        if !linked.contains(&member.identity.id()) && !member.advertised.is_empty() {
            addresses.push(member.advertised.clone());
        }
    }
    let seeding = {
        let dialling = locked(&state.dialling);
        state
            .settings
            .seeds
            .iter()
            .any(|seed| dialling.get(seed).is_some_and(|attempt| attempt.running))
    };
    if !seeding {
        let forgotten = locked(&state.forgotten).clone();
        for (node, address) in locked(&state.rumoured).iter() {
            // A member's own word wins, and a forgotten node is not
            // brought back on a peer's: both checked here as well as when
            // the rumour arrived, because either can change in between.
            if !met.contains(node) && !linked.contains(node) && !forgotten.contains(node) {
                addresses.push(address.clone());
            }
        }
    }
    addresses.sort();
    addresses.dedup();
    addresses
}

/// What to tell `to` about the members this node is hearing.
///
/// **Alive members only.** A rumour is where a peer heard a member
/// answering, and a member this node has judged dead is one it has not
/// heard; passing that on would have every node that never met it dial
/// a corpse for ever. A dead member that comes back dials the nodes it
/// knows, and they gossip it from there.
fn gossip_for(state: &State, to: NodeId) -> Message {
    let members = locked(&state.members)
        .members()
        .filter(|member| {
            member.state == MemberState::Alive
                && member.identity.id() != to
                && !member.advertised.is_empty()
        })
        .take(MAX_RUMOURS)
        .map(|member| Rumour {
            node: member.identity.id().to_bytes(),
            advertised: member.advertised.clone(),
        })
        .collect();
    Message::Members { members }
}

/// Section 8.1's gossip, once a beat: every link that has not been
/// told yet, and on a repair beat one random peer as well.
///
/// The new links are what make a join prompt — a node that came in
/// through one seed learns the rest of the mesh a beat after its first
/// link, rather than whenever a random choice happens to land on it —
/// and they cover a member arriving later too, because its own first
/// link is new to whoever it reached. The random peer is what repairs a
/// list dropped on a full queue; see [`GOSSIP_EVERY`]. One rather than
/// all of them, for section 8.2's reason: a random peer reaches
/// everybody in a few rounds.
///
/// The queues are taken out under the link table and the lists made
/// outside it, because a list reads the member table and the two are
/// never held at once.
fn gossip(state: &State, repair: bool) {
    let (fresh, linked) = {
        let mut links = locked(&state.links);
        let linked: Vec<NodeId> = links.keys().copied().collect();
        let fresh: Vec<(NodeId, crate::link::Outgoing)> = links
            .iter_mut()
            .filter(|(_, link)| !link.told)
            .map(|(peer, link)| {
                link.told = true;
                (*peer, link.outgoing.clone())
            })
            .collect();
        (fresh, linked)
    };
    let mut told: Vec<(NodeId, crate::link::Outgoing)> = fresh;
    if let Some(peer) = repair.then(|| crate::replicate::choose(&linked)).flatten() {
        if !told.iter().any(|(already, _)| *already == peer) {
            let queue = locked(&state.links)
                .get(&peer)
                .map(|link| link.outgoing.clone());
            told.extend(queue.map(|queue| (peer, queue)));
        }
    }
    for (peer, queue) in told {
        match queue.send(&gossip_for(state, peer)) {
            Ok(Sent::Queued) => {}
            Ok(Sent::Dropped) => {
                tracing::debug!(%peer, "a peer's queue was full, so its member list was dropped");
            }
            Err(why) => tracing::error!(%why, "a member list could not be framed"),
        }
    }
}

/// Take what `from` said about the members it is hearing.
///
/// Only a node this one has not met, is not this one, and was not
/// forgotten, becomes a rumour; see the module documentation for the
/// three rules. Nothing here dials: [`targets`] reads the rumours on the
/// next pass, under the backoff every other address has.
fn learn(state: &State, from: NodeId, members: Vec<Rumour>) {
    let me = state.identity.id();
    let met: HashSet<NodeId> = locked(&state.members)
        .members()
        .map(|member| member.identity.id())
        .collect();
    let forgotten = locked(&state.forgotten).clone();
    let mut rumoured = locked(&state.rumoured);
    for rumour in members.into_iter().take(MAX_RUMOURS) {
        let node = NodeId::from_bytes(rumour.node);
        if node == me
            || node == from
            || rumour.advertised.is_empty()
            || met.contains(&node)
            || forgotten.contains(&node)
        {
            continue;
        }
        if rumoured.len() >= MAX_RUMOURS && !rumoured.contains_key(&node) {
            continue;
        }
        if rumoured.get(&node) != Some(&rumour.advertised) {
            tracing::debug!(
                peer = %node,
                advertised = %rumour.advertised,
                told_by = %from,
                "a cluster peer told this node of a member it has not met"
            );
            rumoured.insert(node, rumour.advertised);
        }
    }
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
    // **The connect and the handshake together are bounded by the dead
    // window.** Unbounded, a peer that accepted and then said nothing —
    // a wedged process, a half-open socket, an address that swallows
    // packets — held this attempt for ever, and `consider` never dials
    // an address whose attempt is still running, so the peer was never
    // dialled again. Timing out is a transport failure, and backs off
    // like one.
    let bound = state.settings.detection.dead_after;
    let attempt = async {
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
            Ok(peer) => Ok((stream, peer)),
            Err(why) => Err(refuse(state, Some(address), &why)),
        }
    };
    match tokio::time::timeout(bound, attempt).await {
        Ok(Ok((stream, peer))) => {
            let id = peer.identity.id();
            install(state, stream, peer, Role::Dialler);
            Ok(id)
        }
        Ok(Err(refusal)) => Err(refusal),
        Err(_) => {
            tracing::debug!(
                address,
                "a cluster peer did not finish its handshake within the dead window"
            );
            Err(Refusal::Transport)
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
        gossip(&state, beat.is_multiple_of(GOSSIP_EVERY));
        let aged = locked(&state.members).age(Instant::now());
        for (identity, verdict) in aged {
            match verdict {
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
            // Section 8.3's routing map is what wants this: a node that
            // is dead must stop receiving forwards, and nothing is
            // written down when one dies.
            if let Some(deliveries) = &state.deliveries {
                deliveries.member_changed(identity.id(), verdict);
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
    /// It was not taken: the pair already had the link the rule says
    /// to keep, or this mesh has stopped.
    Dropped,
}

/// Take an authenticated connection, unless this pair already has the
/// one the rule says to keep.
fn install(state: &Arc<State>, stream: BoxStream, peer: handshake::Peer, role: Role) -> Installed {
    if state.stopped.load(Ordering::Relaxed) {
        tracing::debug!(
            peer = %peer.identity,
            "a cluster handshake finished after this node stopped, so its connection is closed"
        );
        drop(stream);
        return Installed::Dropped;
    }
    let id = peer.identity.id();
    let wanted = keeper(state.identity.id(), id);
    // Read before the link table is locked, so that the two are never
    // held at once and no order between them has to be kept.
    let declared_dead = locked(&state.members)
        .member(id)
        .is_some_and(|member| member.state == MemberState::Dead);
    let mut links = locked(&state.links);
    if let Some(existing) = links.get(&id) {
        // A lone link is never dropped for its role; the rule breaks a
        // tie and nothing else. Here there are two, so it decides —
        // unless the one already here belongs to a peer this node has
        // declared dead, in which case they are not two. That link is
        // the corpse this node keeps on purpose, and this is the same
        // peer arriving again rather than a rival; judging it against
        // the corpse is how half of all rejoins become permanent.
        if !declared_dead && (existing.role == wanted || role != wanted) {
            tracing::debug!(
                peer = %peer.identity,
                ?role,
                "two links to one peer, so the one the lower identity dialled is kept"
            );
            drop(links);
            drop(stream);
            return Installed::Dropped;
        }
        if declared_dead {
            tracing::info!(
                peer = %peer.identity,
                ?role,
                "a peer this node had declared dead is back on a new connection, so the \
                 link it left behind is replaced rather than raced against"
            );
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
            told: false,
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

    // Met by a link of its own: its greeting says where it is from now
    // on, and a node an operator forgot that came back by itself is a
    // member again. See the module documentation.
    locked(&state.rumoured).remove(&id);
    locked(&state.forgotten).remove(&id);
    let now_alive = locked(&state.members).heard(&peer.identity, &peer.advertised, Instant::now());
    // A peer this node had judged dead is alive again, and its filters
    // are to be routed to again. `Routes` never threw them away, so
    // there is nothing to replay; see that module.
    if let (Some(deliveries), Some(verdict)) = (&state.deliveries, now_alive) {
        deliveries.member_changed(peer.identity.id(), verdict);
    }
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
                let now_alive =
                    locked(&state.members).heard(&identity, &advertised, Instant::now());
                // **Only when it changed.** A peer that was dead on a
                // link that never closed is alive again the moment it
                // speaks, and nothing else reports that: `age` sees a
                // row that is already alive. Told on the edge rather
                // than on every frame, because this is the inbound
                // publish path and the map is behind a lock.
                if let (Some(deliveries), Some(verdict)) = (&state.deliveries, now_alive) {
                    deliveries.member_changed(identity.id(), verdict);
                }
                // Section 8.1's gossip is the mesh's own: it is about
                // where to dial, and nothing above the mesh dials.
                let message = match message {
                    Message::Members { members } => {
                        learn(&state, identity.id(), members);
                        continue;
                    }
                    other => other,
                };
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
        // And the second of the two signals section 8.1 keeps apart: a
        // closed socket, which is dead at once rather than after a
        // window. Said here as well as in `beating` because the aging
        // loop never sees this one — the row is already dead by the
        // time it looks.
        if let Some(deliveries) = &state.deliveries {
            deliveries.member_changed(id, MemberState::Dead);
        }
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
pub(crate) fn jitter(delay: Duration) -> Duration {
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

    /// A peer declared dead on a link that never closed rejoins on a
    /// new connection.
    ///
    /// The half-open link, and the case [`keeper`] must not settle. A
    /// node whose host died, or one behind a router that stopped
    /// forwarding, leaves this one a socket that goes nowhere and says
    /// nothing. The window declares the peer dead and **the link is
    /// kept on purpose**, because that is what lets a one-way partition
    /// heal without anything reconnecting. What is asserted here is
    /// that the connection the peer makes when it comes back is the one
    /// that survives.
    ///
    /// Before the fix this hung whichever way the two identities
    /// ordered, because both connections are ones `a` accepted: with
    /// `wanted` the listener the corpse already has that role, and with
    /// `wanted` the dialler neither of them does. So the rule dropped
    /// the live connection every time, and nothing dialled either —
    /// [`targets`] asks for peers with no link, and this peer had one.
    ///
    /// **Why a cut and a second mesh rather than a restart.** The
    /// corpse has to be a link whose socket is still open, and a node
    /// that merely stops closes its end: [`crate::testing::relay`]
    /// propagates that, `a` removes the link, and the rejoin is the
    /// ordinary one the test above covers. So the node that is declared
    /// dead stays up behind a black hole on what it says, and the
    /// identity comes back on a connection of its own. Two meshes share
    /// an identity for a moment, which no deployment should do; what is
    /// under test is what `a` does with a connection carrying an
    /// identity it has declared dead, and this is the only way to hand
    /// it one.
    ///
    /// Only the peers have seeds, so `a` never dials. The assertion is
    /// about what it does with a connection it accepted, and a dial of
    /// its own would be a second route to the same link.
    #[tokio::test]
    async fn a_peer_dead_on_a_link_that_never_closed_rejoins_on_a_new_one() {
        let switch = Switch::new();
        let identity = NodeIdentity::mint("c");
        let a = node(&switch, "a", &[], &NodeIdentity::mint("a"), SECRET);
        let stopped = node(&switch, "c", &["a"], &identity, SECRET);
        until("both see each other", || {
            a.alive() == 1 && stopped.alive() == 1
        })
        .await;
        assert_eq!(a.link_role(identity.id()), Some(Role::Listener));

        switch.cut("c", "a");
        until("it is dead, on a link that is still here", || {
            how_a_peer_looks(&a, identity.id()) == Some(MemberState::Dead) && a.stats().links == 1
        })
        .await;

        let back = node(&switch, "c2", &["a"], &identity, SECRET);
        until("it rejoins", || a.alive() == 1 && back.alive() == 1).await;
        keeps("one link each, and both stay alive on it", 8, || {
            a.stats().links == 1 && back.stats().links == 1 && a.alive() == 1 && back.alive() == 1
        })
        .await;
        // The row it came back into is the row it left, carrying the
        // address the greeting on the new connection advertised.
        let members = a.members();
        assert_eq!(members.len(), 1, "the peer that came back is not a second");
        assert_eq!(members[0].advertised, "c2");
    }

    /// A mesh that has stopped takes no link from a handshake that was
    /// in flight when it stopped.
    ///
    /// **Because a handshake is a task of its own**, and not one of the
    /// three [`Mesh::shutdown`] holds. One past the aborts goes on to
    /// install a link on a mesh that has stopped, with a reader and a
    /// writer nothing will ever abort, holding its end of the connection
    /// open for as long as the process lives — so the peer at the other
    /// end has a link to a node that is gone and never sees it close.
    /// That is the corpse the test above has to be contrived to produce,
    /// and the first thing it did was make a stopped node look alive.
    ///
    /// The window is otherwise the scheduler's: at rest a mesh has no
    /// handshake in flight, and this one exists only between a
    /// connection being accepted and the greeting answering it. So the
    /// connection is driven by hand with what `a` says back held, which
    /// pins `a` inside [`handshake::accept`] — it cannot leave until it
    /// reads a proof that cannot be sent until its greeting arrives —
    /// and makes the moment it finishes this test's to choose.
    #[tokio::test]
    async fn a_mesh_that_has_stopped_takes_no_link_from_a_handshake_in_flight() {
        let switch = Switch::new();
        let a = node(&switch, "a", &[], &NodeIdentity::mint("a"), SECRET);

        // Before the dial, which is what the switch's valves are made on
        // demand for: the pair is severed before either end connects.
        switch.stall("a", "peer");
        let transport = switch.listen("peer");
        let stream = transport.dial("a").await.expect("a is listening");
        let dialling = tokio::spawn({
            let config = config(SECRET);
            let peer = NodeIdentity::mint("peer");
            async move {
                let mut stream = stream;
                handshake::dial(&mut stream, &config, &peer, "peer").await
            }
        });
        until("a is inside the handshake", || {
            switch.valve("peer", "a").passed() > 0
        })
        .await;

        a.shutdown();
        switch.heal("a", "peer");
        let settled = dialling.await.expect("the dial ran to completion");
        assert!(
            settled.is_ok(),
            "the handshake itself must succeed, or this test is about a refusal instead: \
             {settled:?}"
        );

        keeps(
            "a mesh that has stopped holds no link and no member",
            8,
            || a.stats().links == 0 && a.members().is_empty(),
        )
        .await;
    }

    /// A dial whose peer accepts and never answers is given up on and
    /// tried again.
    ///
    /// The handshake had no bound. A peer that accepted the connection
    /// and then said nothing — a wedged process, a half-open socket —
    /// held the dial inside `handshake::dial` for ever, and the attempt
    /// stayed marked running, so the address was never dialled again.
    /// Bounded by the dead window: a peer silent for that long is dead
    /// by section 8.1's own rule.
    #[tokio::test]
    async fn a_dial_whose_peer_never_answers_is_given_up_on_and_retried() {
        let switch = Switch::new();
        let silent = switch.listen("silent");
        let _a = node(&switch, "a", &["silent"], &NodeIdentity::mint("a"), SECRET);

        // The first connection, held open and never answered.
        let first = tokio::time::timeout(PATIENCE, silent.accept())
            .await
            .expect("a dials its seed")
            .expect("the switch delivers it");
        // Without a bound on the handshake there is never a second.
        let second = tokio::time::timeout(PATIENCE, silent.accept()).await;
        assert!(
            second.is_ok(),
            "a stayed inside a handshake with a peer that never answered, and never dialled again"
        );
        drop(first);
    }

    /// A connection that never greets is closed rather than held.
    ///
    /// The listener's half of the same bound: before it, anything that
    /// opened a connection to the cluster port and sent nothing held a
    /// task and a socket on this node for as long as it liked.
    #[tokio::test]
    async fn a_connection_that_never_greets_is_closed() {
        use tokio::io::AsyncReadExt;
        let switch = Switch::new();
        let _a = node(&switch, "a", &[], &NodeIdentity::mint("a"), SECRET);
        let mute = switch.listen("mute");
        let mut stream = mute.dial("a").await.expect("a is listening");

        // `a` greets first and then waits for ours. Read until it gives
        // up on us, which is the end of the stream.
        let mut buffer = [0_u8; 512];
        let closed = tokio::time::timeout(PATIENCE, async {
            loop {
                match stream.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
            }
        })
        .await;
        assert!(
            closed.is_ok(),
            "a held a connection that never greeted it for the whole patience"
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

    /// A node given one seed ends up linked to every member.
    ///
    /// Section 8.1: a node learnt through a seed is reached. Here `b`
    /// and `c` are each told only where `a` is, so before members were
    /// gossiped the mesh was two links and never three: `a` knew both,
    /// and neither knew the other.
    #[tokio::test]
    async fn a_node_given_one_seed_is_linked_to_every_member() {
        let switch = Switch::new();
        let a = node(&switch, "a", &[], &NodeIdentity::mint("a"), SECRET);
        let b = node(&switch, "b", &["a"], &NodeIdentity::mint("b"), SECRET);
        let c = node(&switch, "c", &["a"], &NodeIdentity::mint("c"), SECRET);
        until("every node sees the other two", || {
            a.alive() == 2 && b.alive() == 2 && c.alive() == 2
        })
        .await;
        keeps("each node holds exactly two links", 6, || {
            a.stats().links == 2 && b.stats().links == 2 && c.stats().links == 2
        })
        .await;
        // And `c` met `b` at the address `b` gave in its own greeting.
        let met = c
            .members()
            .into_iter()
            .find(|member| member.identity.id() == b.identity().id())
            .expect("c has met b");
        assert_eq!(met.advertised, "b");
    }

    /// A rumour of `who` at `address`, as a peer would send it.
    fn rumour(who: NodeId, address: &str) -> Vec<Rumour> {
        vec![Rumour {
            node: who.to_bytes(),
            advertised: address.to_owned(),
        }]
    }

    /// A node an operator forgot is not dialled again on a peer's word,
    /// and is a member again once it is met by a link of its own.
    ///
    /// Membership is each node's own, so the peers that still remember
    /// a forgotten node go on gossiping it; taking their word would undo
    /// the operator's forget on the next beat.
    #[tokio::test]
    async fn a_forgotten_node_is_not_dialled_on_a_peers_word() {
        let switch = Switch::new();
        let a = node(&switch, "a", &[], &NodeIdentity::mint("a"), SECRET);
        let x = NodeIdentity::mint("x");
        let teller = NodeIdentity::mint("b").id();

        learn(&a.state, teller, rumour(x.id(), "x"));
        assert_eq!(a.rumours(), vec![(x.id(), "x".to_owned())]);
        assert!(targets(&a.state).contains(&"x".to_owned()));

        a.forget(x.id());
        assert!(a.rumours().is_empty(), "forgetting drops the rumour");
        learn(&a.state, teller, rumour(x.id(), "x"));
        assert!(
            a.rumours().is_empty(),
            "a peer's word brought a forgotten node back"
        );
        assert!(!targets(&a.state).contains(&"x".to_owned()));

        // Forgotten is not banned: dialling in by itself, it is met.
        let _x = node(&switch, "x", &["a"], &x, SECRET);
        until("x is met again by a link of its own", || a.alive() == 1).await;
        assert!(
            locked(&a.state.forgotten).is_empty(),
            "a node met again is no longer forgotten"
        );
    }

    /// A rumour is where to dial and nothing more: the handshake still
    /// decides who is there, and a member's own word beats a peer's.
    #[tokio::test]
    async fn a_rumour_is_only_where_to_dial() {
        let switch = Switch::new();
        let a = node(&switch, "a", &[], &NodeIdentity::mint("a"), SECRET);

        // A node that does not hold the secret, rumoured: dialled, and
        // never a member.
        let stranger = NodeIdentity::mint("stranger");
        let _stranger = node(&switch, "stranger", &[], &stranger, "another-secret");
        // And an address where somebody other than the rumoured node
        // answers: whoever answers is who is met.
        let claimed = NodeIdentity::mint("claimed").id();
        let real = NodeIdentity::mint("real");
        let _real = node(&switch, "real", &[], &real, SECRET);
        let teller = NodeIdentity::mint("teller").id();
        learn(&a.state, teller, rumour(stranger.id(), "stranger"));
        learn(&a.state, teller, rumour(claimed, "real"));

        until("the node at the rumoured address is met", || a.alive() == 1).await;
        keeps("and nobody else ever is", 8, || {
            let members: Vec<NodeId> = a
                .members()
                .into_iter()
                .map(|member| member.identity.id())
                .collect();
            members == vec![real.id()]
        })
        .await;

        // A member's own address beats a rumour of it.
        learn(&a.state, teller, rumour(real.id(), "elsewhere"));
        assert!(
            !a.rumours().iter().any(|(node, _)| *node == real.id()),
            "a rumour overrode what a member said about itself"
        );
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
