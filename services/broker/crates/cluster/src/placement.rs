//! Which nodes hold a session's queue, section 8.6.
//!
//! Rendezvous hashing: every node is weighted for every session, and the
//! highest weight owns it while the next `R - 1` follow. It needs no
//! coordination, every node computes the same answer from the same
//! inputs, and when a member leaves only the sessions that were on it
//! move — which is the whole reason it was chosen over dividing a ring
//! into ranges.
//!
//! # This decides nothing about who owns a session
//!
//! **[`place`] is advice.** Section 8.2 makes membership deliberately
//! local judgement, so two nodes on opposite sides of a partition have
//! different live sets and compute different replica sets from them. If
//! this function decided ownership, that disagreement would be a split
//! brain by construction and no amount of care here would fix it.
//!
//! It does not. Section 8.4's epoch-stamped register says who owns a
//! session; this says who *should*, which is a different question and a
//! safe one to disagree about. A node streaming to a follower that does
//! not think it is one is harmless, because a follower applies what it
//! is sent.
//!
//! # The weight is this project's hash, not a crate's
//!
//! Every node has to compute the same weights or they place copies on
//! different nodes, and that has to hold across a rolling upgrade as
//! well as a restart. A hash whose value a dependency is free to change
//! between versions cannot carry that: `foldhash`'s fixed state is
//! deterministic *within a version* and promises nothing between them,
//! and `DefaultHasher` promises less.
//!
//! So the weight is SHA-256 — from the crate already linked for the
//! handshake, so it costs no dependency — over a byte string defined
//! here, with the digests mixed by [`mix`]. The golden vectors in the
//! tests are computed with another tool against the standard rather
//! than printed from this code, because a vector captured from the
//! implementation pins a bug exactly as firmly as it pins the
//! behaviour.
//!
//! # Why the digests are mixed rather than hashed together
//!
//! True rendezvous hashes each session-and-node pair. At the design's
//! ceilings — a hundred thousand sessions and fifty nodes — that is five
//! million digests every time membership changes, which is seconds of
//! processor time for a question that has to be answered while a node
//! is already busy reacting to a peer going away.
//!
//! So each node's digest is computed once, when the member set changes,
//! and each session's digest once, and the weight is a mix of the two.
//! A membership change then costs one digest per session instead of
//! fifty. The mixer has to scramble well enough that a node's digest
//! cannot dominate the ordering for every session — which is what the
//! distribution test is about, and what the mutation that drops the
//! node digest breaks.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use aws_lc_rs::digest;
use storage::SessionKey;
use thiserror::Error;

use crate::membership::MemberState;
use crate::mesh::Mesh;
use crate::NodeId;

/// How many nodes hold a session's queue when nothing says otherwise.
///
/// Two: the owner and one follower, which is what makes losing a single
/// node lose nothing.
pub const DEFAULT_FACTOR: usize = 2;

/// The largest replication factor this broker will take.
///
/// Three, from section 8.6. Not a limit of the algorithm — rendezvous
/// would order fifty — but of what the design is willing to promise
/// about disk and inter-node traffic, both of which scale with it and
/// are stated in section 15's budgets.
pub const MAX_FACTOR: usize = 3;

/// Why a replication factor will not do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FactorError {
    /// Zero copies, which is no session anywhere.
    #[error(
        "cluster.replication_factor is 0, and a session is held by at least the node \
         serving it. Write 1 to keep one copy and replicate nothing, or 2 for the \
         default of an owner and one follower."
    )]
    None,
    /// More copies than the design will promise.
    #[error(
        "cluster.replication_factor is {asked}, and the most this broker will take is \
         {MAX_FACTOR}. Each copy costs a full copy of the durable queue data and \
         proportionate traffic between nodes; see section 15's budgets."
    )]
    TooMany {
        /// What was asked for.
        asked: usize,
    },
}

/// How many nodes hold a session's queue.
///
/// **Constructible only through [`Factor::new`]**, so holding one is the
/// proof that it is between one and [`MAX_FACTOR`]: the range check is
/// the absence of a way to say otherwise rather than a check in a
/// startup path that a later caller might route around. The same shape
/// as `ClusterConfig`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Factor(usize);

impl Default for Factor {
    fn default() -> Self {
        Self(DEFAULT_FACTOR)
    }
}

impl Factor {
    /// Take a factor, or say why not.
    ///
    /// # Errors
    ///
    /// [`FactorError::None`] for zero and [`FactorError::TooMany`] above
    /// [`MAX_FACTOR`]. Two causes and two messages: an operator's next
    /// move differs for each.
    pub fn new(copies: usize) -> Result<Self, FactorError> {
        if copies == 0 {
            return Err(FactorError::None);
        }
        if copies > MAX_FACTOR {
            return Err(FactorError::TooMany { asked: copies });
        }
        Ok(Self(copies))
    }

    /// How many copies.
    #[must_use]
    pub fn copies(self) -> usize {
        self.0
    }
}

/// A node, with the digest that weighs it, computed once.
///
/// Held by whatever watches membership and rebuilt when the member set
/// changes, because that is the only time any of these change. See the
/// module documentation for why this is not recomputed per session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Weighted {
    /// Which node.
    pub node: NodeId,
    /// SHA-256 of its sixteen identity bytes, first eight, big-endian.
    digest: u64,
}

impl Weighted {
    /// Weigh `node`, hashing it once.
    #[must_use]
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            digest: first_eight(&digest::digest(&digest::SHA256, &node.to_bytes())),
        }
    }
}

/// The first eight bytes of a digest, big-endian.
///
/// Big-endian because it is the order the digest is written in and the
/// order another tool prints it in, which is what makes a golden vector
/// checkable by eye against `shasum`.
fn first_eight(digest: &digest::Digest) -> u64 {
    let bytes = digest.as_ref();
    let mut eight = [0u8; 8];
    eight.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(eight)
}

/// The digest of a session key.
///
/// **Length-prefixed, and that is not decoration.** A key is two strings
/// and concatenating them is ambiguous: mountpoint `a` with client `bc`
/// and mountpoint `ab` with client `c` are different sessions and would
/// hash the same. This broker has already shipped that exact bug once,
/// in the stored sequence keys, and the fix there was the same one.
#[must_use]
pub fn session_digest(session: &SessionKey) -> u64 {
    let mut context = digest::Context::new(&digest::SHA256);
    context.update(&(session.mountpoint.len() as u64).to_be_bytes());
    context.update(session.mountpoint.as_bytes());
    context.update(&(session.client_id.len() as u64).to_be_bytes());
    context.update(session.client_id.as_bytes());
    first_eight(&context.finish())
}

/// Combine a session's digest with a node's into a weight.
///
/// SplitMix64's finalizer over the two, which is a published mixer with
/// published constants rather than one invented here — the same reason
/// the digest is SHA-256. What it has to do is destroy any structure
/// shared between the inputs, so that a node whose digest happens to be
/// large does not win every session; the distribution test is what
/// holds it to that.
#[must_use]
pub fn mix(session: u64, node: u64) -> u64 {
    let mut x = session ^ node.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Who should hold `session`, highest weight first.
///
/// The first is the owner and the rest are followers. Shorter than
/// `factor` when there are fewer members than that, which section 8.6
/// makes an ordinary state rather than an error: a cluster of one with
/// a factor of two is a cluster that has not finished being built, and
/// it is repaired when the members arrive.
///
/// **Advice, not authority.** See the module documentation.
#[must_use]
pub fn place(session: &SessionKey, members: &[Weighted], factor: Factor) -> Vec<NodeId> {
    let digest = session_digest(session);
    let mut weighed: Vec<(u64, NodeId)> = members
        .iter()
        .map(|member| (mix(digest, member.digest), member.node))
        .collect();
    // Highest weight first, and **the identity breaks a tie** rather
    // than the order the members arrived in. Two nodes whose weights
    // collide is a one-in-two-to-the-sixty-four event per session per
    // pair, and at that rate it will happen to somebody: left to the
    // input order it would make two nodes disagree about placement
    // while agreeing about everything else, which is the one failure
    // this function exists to make impossible.
    weighed.sort_unstable_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.to_bytes().cmp(&right.1.to_bytes()))
    });
    weighed
        .into_iter()
        .take(factor.copies())
        .map(|(_, node)| node)
        .collect()
}

/// This node, its replication factor, and the members to weigh.
///
/// What a caller holds to answer "who should hold this session". The
/// mesh is read for its live members; see [`Placement::members`] for
/// why the digests behind them are not recomputed every time.
#[derive(Debug)]
pub struct Placement {
    me: NodeId,
    factor: Factor,
    mesh: Arc<Mesh>,
    /// How long a dead member keeps its place. Section 8.6's grace.
    gone_after: Duration,
    /// The weighed members, and the identities they were built from.
    ///
    /// **A cache, because the digests are the expensive part.** The
    /// module documentation promises that a node's digest is computed
    /// once when the member set changes rather than per session, and
    /// this is where that is kept: a batch compares at most fifty
    /// identities instead of hashing fifty of them.
    weighed: Mutex<(Vec<NodeId>, Vec<Weighted>)>,
}

/// The lock, taken the way the rest of this crate takes one.
fn locked<T>(what: &Mutex<T>) -> MutexGuard<'_, T> {
    what.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Placement {
    /// Place sessions for `me` across `mesh`, keeping `factor` copies.
    #[must_use]
    pub fn new(me: NodeId, factor: Factor, mesh: Arc<Mesh>, gone_after: Duration) -> Arc<Self> {
        Arc::new(Self {
            me,
            factor,
            mesh,
            gone_after,
            weighed: Mutex::new((Vec::new(), Vec::new())),
        })
    }

    /// Which node this is.
    #[must_use]
    pub fn me(&self) -> NodeId {
        self.me
    }

    /// How many copies of each session.
    #[must_use]
    pub fn factor(&self) -> Factor {
        self.factor
    }

    /// The members that keep a place in a replica set, weighed,
    /// **this node included**.
    ///
    /// Itself included because a session has to be placeable on the
    /// node serving it — rendezvous puts the owner first, and a set
    /// computed without this node would make every session's owner
    /// somebody else.
    ///
    /// **A dead member keeps its place until the grace runs out.** The
    /// obvious rule, section 8.6's "live members" read literally, would
    /// drop a node the moment its heartbeat failed, and rendezvous
    /// would then name a replacement, and the replacement would be
    /// sent the queue — the whole estate, every time anybody restarted
    /// a node. So a member that is dead but silent for less than
    /// `gone_after` is still placed: the set is short by one copy
    /// while it is away, which the lag and the unconfirmed counter
    /// both show, and is repaired only once the node is given up on.
    /// A forgotten node is not in the table at all and so is not
    /// placed, which is the operator's way of not waiting.
    ///
    /// That this is this node's own judgement, and that another node
    /// may judge differently, is why placement decides nothing — see
    /// the module documentation.
    #[must_use]
    pub fn members(&self) -> Vec<Weighted> {
        let now = std::time::Instant::now();
        let gone_after = self.gone_after;
        let mut live: Vec<NodeId> = self
            .mesh
            .members()
            .into_iter()
            .filter(|member| {
                placed(
                    member.state,
                    now.saturating_duration_since(member.last_heard),
                    gone_after,
                )
            })
            .map(|member| member.identity.id())
            .collect();
        live.push(self.me);
        live.sort_unstable_by_key(|node| node.to_bytes());

        let mut weighed = locked(&self.weighed);
        if weighed.0 != live {
            weighed.1 = live.iter().copied().map(Weighted::new).collect();
            weighed.0 = live;
        }
        weighed.1.clone()
    }
}

/// Whether a member in `state`, silent for `silence`, keeps its place.
///
/// Alive and suspect always; dead only within the grace. Pulled out of
/// [`Placement::members`] so the rule can be tested without a mesh.
#[must_use]
pub fn placed(state: MemberState, silence: Duration, gone_after: Duration) -> bool {
    match state {
        MemberState::Alive | MemberState::Suspect => true,
        MemberState::Dead => silence < gone_after,
    }
}

#[cfg(test)]
mod tests;
