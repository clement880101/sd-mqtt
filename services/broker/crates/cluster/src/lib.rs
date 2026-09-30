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

pub mod config;
pub mod frame;
pub mod handshake;
pub mod identity;
pub mod link;
pub mod membership;
pub mod mesh;
pub mod testing;
pub mod transport;

pub use config::{ClusterConfig, ClusterSettings, ConfigError, DEFAULT_CLUSTER_NAME};
pub use frame::{FrameError, FrameLimits, FRAME_VERSION};
pub use handshake::{accept, dial, HandshakeError, Peer, Role};
pub use identity::{IdentityError, NodeId, NodeIdentity, Reconciled};
pub use link::{outgoing, pump, FrameReader, LinkConfig, LinkError, Outgoing, Sent};
pub use membership::{Member, MemberState, Membership};
pub use mesh::{keeper, Mesh, MeshSettings, MeshStats};
pub use transport::{BoxStream, TcpTransport, Transport};
