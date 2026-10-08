//! The bottom of section 8: who a node is, and how two of them talk.
//!
//! Nothing here routes a message and nothing here replicates anything.
//! This is the transport those sit on, and it is built first because
//! every one of them assumes a link that is authenticated, framed and
//! bounded, and assumes both ends agree on which node is which.
//!
//! # One identity, and it is the UUID
//!
//! Section 8.1: **the UUID is the identity everything orders by**, and
//! the name is for operators and is never compared. [`NodeId`] is that
//! UUID and holds nothing else, which is the strongest form the rule can
//! take — a comparison of names is not something to be caught by a test
//! here, it is something that cannot be written. [`NodeIdentity`] is the
//! pair, and it is what a node tells a peer about itself.
//!
//! `rules::singleton` orders singleton ownership by this same type
//! rather than by one of its own, for the reason that module's
//! documentation gives: two answers to "which node" in one binary is a
//! defect waiting for the first cluster.
//!
//! # A link is authenticated before it is anything else
//!
//! [`handshake`] is section 8.1's mutual challenge: two nonces, a role
//! byte, and a keyed hash over a length-prefixed transcript. Nothing
//! above it has to ask whether a peer proved itself, because a
//! [`handshake::Peer`] is the only thing either end of it returns.
//!
//! [`config`] is what a cluster will not start without. A
//! [`config::ClusterConfig`] cannot be built without a secret, so "a
//! node with no secret refuses to cluster" is the absence of a way to
//! say otherwise rather than a check in a startup path.
//!
//! # And then it is bounded, in both directions
//!
//! [`frame`] is what a message looks like once a link is open: a
//! 32-bit length covering a version byte and a postcard body, refused
//! from that length alone when it is larger than the link carries.
//! [`link`] is the loop around it — reading incrementally, writing in
//! batches, and **dropping and counting** rather than letting a wedged
//! peer block whoever had something to say to it.

//! # A stamp is a reading and an identity, and the reading is new
//!
//! [`clock`] is section 8.2's hybrid logical clock: the first wall-clock
//! read on a write path in this broker, because `broker-core`'s coarse
//! clock is a count from an arbitrary origin and two nodes' counts are
//! not comparable. A peer further ahead than the configured bound is
//! **refused rather than clamped**, and a node resumes its clock from
//! the highest reading the store holds rather than from zero. Both of
//! those reverse the obvious implementation and both are there for the
//! same reason: convergence is conditional on no two writes ever sharing
//! a stamp.
//!
//! # One merge function, and a tombstone is not a special case
//!
//! [`register`] is section 8.2's last-writer-wins register: a value, a
//! [`Stamp`], and whether the value is there at all. An absent value is
//! a tombstone and competes by the same comparison as any other, because
//! "a delete always wins" diverges — two nodes that saw a delete and a
//! later write in different orders keep different things. So there is one
//! merge function and no path through it that asks whether either side
//! is a tombstone.
//!
//! # And what those registers are about, and at what granularity
//!
//! [`metadata`] is section 8.2's vocabulary: the four kinds this
//! milestone replicates — that a node has at least one subscriber for a
//! filter, retained messages, rules and which node owns a session — and
//! [`Replicated`], which holds this node's registers, its clock, and the
//! one thing that is deliberately *not* replicated.
//!
//! That one thing is a reference count of this node's own subscribers per
//! filter, because **subscriptions replicate at node granularity.** A
//! register per client subscription would be fifty nodes holding a
//! hundred thousand registers each at the design ceiling, against a
//! thirty megabyte idle budget. So a second subscriber to a filter writes
//! nothing and the last unsubscribe writes a **tombstone**, and what the
//! cluster is told is that a node has a subscriber and never which of its
//! clients it is.
//!
//! Rules stop being node-local here too, which is what section 9.1 said
//! in milestone 4 would happen: `rules::live::Registry` writes the
//! register that carries a rule in the same batch as the node's own copy.
//!
//! # And a session has an owner, and it can change
//!
//! [`migrate`] is section 8.4: a node that a client connects to claims
//! the session, asks whoever held it to close the old connection and
//! send what the session holds, and **waits a bounded time before
//! acknowledging — then acknowledges either way.** A drain that times
//! out, or a previous owner with no link to ask over, is a connection
//! accepted with what arrived and a shortfall counted, because refusing
//! turns an unreachable node into an outage for a client that has done
//! nothing wrong.
//!
//! # And it is opened over something, which is not always a socket
//!
//! [`transport`] is the seam. Section 14 committed to three nodes in
//! one process over an in-memory link, and the plan's gate puts a
//! listener in front of a real one; neither is possible if the mesh
//! names a socket. So it names a [`transport::Transport`], and
//! [`transport::TcpTransport`] is the one that is not a harness.
//!
//! # The harnesses ship with the crate, deliberately
//!
//! [`testing`] is compiled always rather than behind a feature. What
//! it holds is needed by this crate's own tests, by the mesh's, and by
//! `crates/sdmqtt/tests/cluster.rs`, which drives three real broker
//! processes through the gate — and a feature that every one of those
//! turns on is a feature with no off position, expressed in a way that
//! makes `cargo test` quietly skip the harness's own tests when
//! nobody remembers to name it. Nothing above reaches it: the release
//! binary links none of it, because nothing it links calls it.

//! # And when it cannot see them, it keeps serving
//!
//! [`policy`] is section 8.5. The default is availability: a node cut
//! off from the rest of the cluster goes on accepting connects,
//! publishes and subscribes from the clients that can still reach it,
//! because a broker that refuses them has turned one node's network
//! fault into every client's outage. Per-operation flags are there for
//! operators who want the other trade, and a refusal under one carries
//! a reason code rather than a closed socket.
//!
//! What "the rest of the cluster" is has to be **a number an operator
//! wrote down**. Membership is learnt, so a node that never met a
//! fourth node cannot tell a cluster of three from a cluster of four
//! with one node down; a flag evaluated against the nodes met would
//! refuse nothing in exactly the case it exists for. So a flag set
//! without `cluster.expected_size` is refused at startup, by
//! [`Policy::new`] rather than by a check in a startup path.

//! # And where a session's queue is kept
//!
//! [`placement`] is section 8.6's rendezvous hashing: which nodes hold
//! a copy of a session's queue, highest weight first, the owner and
//! then its followers. It decides nothing — section 8.4's register
//! still says who owns a session — because two nodes on either side of
//! a partition see different members and would otherwise place copies
//! differently *and* disagree about ownership, which is one problem
//! rather than two.
//!
//! The weight is SHA-256 over a defined byte string rather than a
//! crate's hash, because every node must compute the same number
//! across a rolling upgrade as well as a restart.

pub mod clock;
pub mod config;
pub mod forward;
pub mod frame;
pub mod groups;
pub mod handshake;
pub mod identity;
pub mod link;
pub mod membership;
pub mod mesh;
pub mod metadata;
pub mod migrate;
pub mod oplog;
pub mod placement;
pub mod policy;
pub mod register;
pub mod replica;
pub mod replicate;
pub mod routes;
pub mod testing;
pub mod transport;

pub use clock::{ClockError, HybridClock, Reading, DEFAULT_MAX_DRIFT_MILLIS};
pub use config::{ClusterConfig, ClusterSettings, ConfigError, DEFAULT_CLUSTER_NAME};
pub use forward::{targets, Forwarded, Forwarding};
pub use frame::{FrameError, FrameLimits, FRAME_VERSION};
pub use groups::{Chosen, Groups};
pub use handshake::{accept, dial, HandshakeError, Peer, Role};
pub use identity::{IdentityError, NodeId, NodeIdentity, Reconciled};
pub use link::{outgoing, pump, FrameReader, LinkConfig, LinkError, Outgoing, Sent};
pub use membership::{Member, MemberState, Membership};
pub use mesh::{keeper, Deliver, Fanout, Mesh, MeshSettings, MeshStats};
pub use metadata::{
    Claim, MetadataError, MetadataKey, MetadataValue, MetadataWrite, Ownership, Replicated,
};
pub use migrate::{
    Awaiting, Claimed, Claiming, Drain, Drained, Migrations, Migrator, DEFAULT_PATIENCE,
};
pub use oplog::{Logged, OpLog, FIRST_SEQUENCE};
pub use placement::{place, Factor, FactorError, Weighted, DEFAULT_FACTOR, MAX_FACTOR};
pub use policy::{Gate, Live, Operation, Policy, PolicyError, Strict};
pub use register::{Register, Stamp};
pub use replica::{Applied, Change, Follower, Streamed};
pub use replicate::{
    choose, jitter, Handled, Replication, ReplicationError, ReplicationSettings, Replicator, Swept,
    DEFAULT_BATCH, DEFAULT_EXCHANGE, DEFAULT_TOMBSTONE_GRACE,
};
pub use routes::{Liveness, Routes};
pub use transport::{BoxStream, TcpTransport, Transport};
