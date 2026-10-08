//! Who owns a singleton.
//!
//! Section 9.2: bridges and pollers are singletons, and **the owner is
//! the live member with the lowest node identifier, recomputed on
//! membership change**. That is deterministic, needs no election
//! protocol, and at worst duplicates work for the moments during which
//! two nodes' membership views disagree.
//!
//! # There is one member, and this is still written as a rule
//!
//! Milestone 4 has no clustering. This node is the whole cluster, so it
//! owns everything, and every line below could be replaced by `true`.
//!
//! It is not, and the reason is concrete rather than aesthetic. The
//! alternative to a rule is an assumption that this node is alone,
//! spelled out separately inside each connector that is a singleton —
//! today the poller, next the bridge — which milestone 5 then has to
//! find and unpick in two places that have drifted apart. Section 9.2
//! says the same thing in its own words: a rule that is only ever
//! exercised in the one-member case is a rule nobody has tested.
//!
//! So [`Membership`] is a view that can hold any number of members, and
//! the two tests that matter are the two a single-member implementation
//! cannot express at all: ownership moving when the lowest member
//! leaves, and moving back when it returns.
//!
//! # The order is over the identifier's bytes, and must be everywhere
//!
//! "Lowest" is only deterministic if every node sorts the same way.
//! [`NodeId`] orders by the bytes of the identifier, which is the one
//! ordering that needs no locale, no normalisation and no agreement
//! beyond the bytes themselves.
//!
//! **Which identity that is has been decided: it is the UUID.** This
//! module used to carry a [`NodeId`] of its own, holding a string,
//! with a note saying that section 8.1's milestone would decide
//! whether the string was the UUID or the operator's name. It is the
//! UUID, and the type now lives in `cluster` and is used here rather
//! than copied: one type, one ordering, one answer to "which node".
//!
//! The name is not in it at all. Section 8.1 says the name is never
//! compared, because two copied configurations share a name and an
//! ordering that is not antisymmetric leaves both sides believing they
//! won — and the strongest way to say "never compared" is a type with
//! no name in it, in which the comparison does not compile.
//!
//! # What this is not
//!
//! It is not failure detection. Section 8.1 decides when a peer is
//! suspect and when it is dead, over the cluster's own links, and that
//! is milestone 5's. This is the half that says what follows from a
//! view somebody else maintains, which is why it can be written and
//! tested now.

use std::collections::BTreeSet;
use std::fmt;

use tokio::sync::watch;

/// What names a node, for as long as its store exists.
///
/// `cluster`'s, not a second one: section 8.1's UUID, ordered by its
/// sixteen bytes. Re-exported here because this module's rule is
/// written in terms of it and a reader of "the lowest node identifier"
/// should be one hop from what an identifier is.
pub use cluster::NodeId;

/// Who is live, as this node sees it.
///
/// One view per node rather than one shared truth, because that is what
/// a cluster has: section 9.2 accepts that two nodes' views disagree
/// for a moment and that a singleton is briefly run twice, in exchange
/// for needing no election protocol at all.
///
/// **This node is always in its own view.** A node that could not see
/// itself would compute an owner it can never be, and would then either
/// run nothing or run everything depending on which way the comparison
/// fell. It is inserted by every constructor and [`Membership::left`]
/// will not take it out.
pub struct Membership {
    /// Whose view this is.
    this: NodeId,
    /// Who is live. The sender is the view; every [`Ownership`] holds a
    /// receiver, so a change reaches all of them at once.
    live: watch::Sender<BTreeSet<NodeId>>,
}

impl Membership {
    /// A cluster of one: this node, and nobody else.
    ///
    /// What milestone 4 has. It is a call to the same constructor
    /// everything else uses rather than a special case, so the
    /// one-member answer comes out of the rule rather than from beside
    /// it.
    pub fn alone(this: NodeId) -> Self {
        Self::of(this, [])
    }

    /// This node, and these others.
    pub fn of(this: NodeId, others: impl IntoIterator<Item = NodeId>) -> Self {
        let mut live: BTreeSet<NodeId> = others.into_iter().collect();
        live.insert(this);
        Self {
            this,
            live: watch::Sender::new(live),
        }
    }

    /// Whose view this is.
    pub fn this(&self) -> &NodeId {
        &self.this
    }

    /// Who is live.
    pub fn live(&self) -> BTreeSet<NodeId> {
        self.live.borrow().clone()
    }

    /// A member became live.
    ///
    /// Returns whether the view changed. Idempotent, because a
    /// membership signal may be repeated and rejoining a member already
    /// present is not an event.
    pub fn joined(&self, node: NodeId) -> bool {
        self.live.send_if_modified(|live| live.insert(node))
    }

    /// A member is no longer live.
    ///
    /// Returns whether the view changed. **This node cannot leave its
    /// own view**; see [`Membership`].
    pub fn left(&self, node: &NodeId) -> bool {
        if node == &self.this {
            return false;
        }
        self.live.send_if_modified(|live| live.remove(node))
    }

    /// Who owns every singleton, as this node sees it.
    ///
    /// The live member with the lowest identifier. Never absent: this
    /// node is always in its own view, so there is always a lowest one.
    pub fn owner(&self) -> NodeId {
        owner_of(&self.live.borrow())
    }

    /// Whether this node owns every singleton, as it sees it.
    pub fn owns(&self) -> bool {
        self.owner() == self.this
    }

    /// A handle that follows ownership as the view changes.
    ///
    /// One per singleton. What a connector holds; see [`Ownership`].
    pub fn ownership(&self) -> Ownership {
        // `subscribe` marks the current view as seen, so the handle's
        // first wait is for the next change rather than for this one.
        let live = self.live.subscribe();
        let reported = owner_of(&live.borrow()) == self.this;
        Ownership {
            this: self.this,
            live,
            reported,
        }
    }
}

impl fmt::Debug for Membership {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Membership")
            .field("this", &self.this)
            .field("live", &*self.live.borrow())
            .finish()
    }
}

/// Whether this node runs one singleton, and a way to wait for that to
/// change.
///
/// **[`Self::owns`] is computed from the view every time it is asked**,
/// so the answer a connector acts on is never a copy that went stale —
/// which is what "recomputed on membership change" is a warning about.
/// A connector handed a `bool` at startup would keep running the
/// singleton after the member that should have taken it over came back.
///
/// The one thing the handle does remember is what it last *reported*
/// through [`Self::changed`], and that is not an optimisation: see that
/// method for what goes wrong without it.
#[derive(Debug, Clone)]
pub struct Ownership {
    /// Whose ownership this is.
    this: NodeId,
    /// The view, as the membership publishes it.
    live: watch::Receiver<BTreeSet<NodeId>>,
    /// The answer this handle last gave its caller.
    ///
    /// **Not a cache of the current answer** — [`Self::owns`] never
    /// reads it. It is the other end of the edge [`Self::changed`]
    /// reports, and it is what makes a change that happens while the
    /// caller is away impossible to miss.
    reported: bool,
}

impl Ownership {
    /// Whether this node owns the singleton right now.
    pub fn owns(&self) -> bool {
        owner_of(&self.live.borrow()) == self.this
    }

    /// Who owns it right now.
    pub fn owner(&self) -> NodeId {
        owner_of(&self.live.borrow())
    }

    /// Wait until the answer to [`Self::owns`] is different from the
    /// one this handle last reported, and return it.
    ///
    /// **A membership change is not an ownership change.** A member
    /// joining or leaving above the lowest one moves nothing, and a
    /// caller woken for it would stop and restart a connector for no
    /// reason. So this waits for the answer to move rather than for the
    /// view to.
    ///
    /// # It looks before it waits, and that is the whole correctness of
    /// it
    ///
    /// A version that read the answer, then waited, then compared would
    /// be wrong in a way no amount of care at the call site can fix: a
    /// change that lands between the caller's last look and this call
    /// would be read as the baseline, and the wait would then be for a
    /// *second* change that may never come. Ownership would move and
    /// the connector would sit there. That is a race, so it is a test
    /// that passes until the day it does not.
    ///
    /// The baseline is therefore [`Self::reported`] — what this handle
    /// last told its caller — and the view is read before anything is
    /// awaited. An edge cannot fall between the two.
    ///
    /// If the view is gone — the membership was dropped — the answer
    /// can never change again, and this never returns. That is
    /// deliberate: a caller selecting on this in a loop must not be
    /// handed a change that did not happen, and must not spin.
    pub async fn changed(&mut self) -> bool {
        loop {
            // `borrow_and_update` rather than `borrow`, so that "what
            // this handle has seen" and "what it has reported" stay the
            // same thing.
            let now = owner_of(&self.live.borrow_and_update()) == self.this;
            if now != self.reported {
                self.reported = now;
                return now;
            }
            if self.live.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

/// The live member with the lowest identifier.
///
/// The whole rule, in one place, so that the answer a [`Membership`]
/// gives and the answer an [`Ownership`] gives cannot differ.
///
/// `expect` rather than an `Option`: every view holds the node whose
/// view it is, so an empty one is this module having been broken rather
/// than a state a caller can reach.
fn owner_of(live: &BTreeSet<NodeId>) -> NodeId {
    *live
        .iter()
        .next()
        .expect("a node is always in its own membership view")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A node identifier whose order is written on its face.
    ///
    /// The whole sixteen bytes set from one, so `node(1) < node(2)` is
    /// true of the bytes rather than of a rendering of them, and a test
    /// below can say which node owns a singleton without the reader
    /// having to parse a UUID to check.
    fn node(rank: u8) -> NodeId {
        NodeId::from_bytes([rank; 16])
    }

    /// How long a handover may take before this calls it a failure.
    ///
    /// Generous, because what is being timed is a notification between
    /// two tasks in one process; anything slower than this is a
    /// notification that did not happen.
    const HANDOVER: std::time::Duration = std::time::Duration::from_secs(5);

    async fn handover(ownership: &mut Ownership) -> bool {
        tokio::time::timeout(HANDOVER, ownership.changed())
            .await
            .expect("ownership should have moved")
    }

    // -----------------------------------------------------------
    // The one-member case, which is all milestone 4 has
    // -----------------------------------------------------------

    #[test]
    fn one_member_owns_everything() {
        let membership = Membership::alone(node(1));
        assert_eq!(membership.owner(), node(1));
        assert!(membership.owns());
        assert!(membership.ownership().owns());
    }

    // -----------------------------------------------------------
    // Three members, which is what the rule is for
    // -----------------------------------------------------------

    /// The lowest of three owns, and the other two do not.
    ///
    /// Written from all three points of view rather than from the
    /// winner's, because a rule that answered "yes" everywhere would
    /// pass the winner's half on its own and would run the singleton
    /// three times.
    #[test]
    fn of_three_the_lowest_owns_and_the_others_do_not() {
        let (a, b, c) = (node(1), node(2), node(3));
        for this in [&a, &b, &c] {
            let others = [a, b, c].into_iter().filter(|other| other != this);
            let membership = Membership::of(*this, others);
            assert_eq!(
                membership.live().len(),
                3,
                "a view of three members holds three"
            );
            assert_eq!(membership.owner(), a, "as {this} sees it");
            assert_eq!(
                membership.owns(),
                this == &a,
                "{this} should{} own the singleton",
                if this == &a { "" } else { " not" }
            );
        }
    }

    /// The order is over the identifier's bytes, from the first that
    /// differs.
    ///
    /// Asserted with three identifiers that share a leading byte and
    /// differ later, because an implementation comparing only the
    /// first byte, or the length, or the time half of a version 7
    /// identifier, would agree with the test above and disagree here.
    #[test]
    fn lowest_is_by_bytes() {
        let mut lowest = [0x09; 16];
        lowest[7] = 0x01;
        let mut middle = [0x09; 16];
        middle[7] = 0x02;
        let highest = [0x09; 16];

        let membership = Membership::of(
            NodeId::from_bytes(highest),
            [NodeId::from_bytes(lowest), NodeId::from_bytes(middle)],
        );
        assert_eq!(membership.owner(), NodeId::from_bytes(lowest));
        assert!(!membership.owns());
    }

    /// Ownership is decided by the identifier and cannot reach a name.
    ///
    /// Section 8.1: the name is for operators and is **never**
    /// compared, because two copied configurations share one. The
    /// guard is not this assertion — it is that [`NodeId`] carries no
    /// name, so an ordering that reached for one would not compile.
    /// This is here to say so in a place a reader of the rule will
    /// look, and to fail if the identity ever grows a name again.
    #[test]
    fn the_owner_is_the_lowest_identifier_whatever_the_nodes_are_called() {
        let low = cluster::NodeIdentity::new(node(1), "zulu");
        let high = cluster::NodeIdentity::new(node(2), "alpha");

        let membership = Membership::of(high.id(), [low.id()]);
        assert_eq!(
            membership.owner(),
            low.id(),
            "the lowest identifier owns it, although its name sorts last"
        );
        assert!(!membership.owns());
    }

    // -----------------------------------------------------------
    // The two that a single-member implementation cannot express
    // -----------------------------------------------------------

    /// When the lowest member leaves, the next lowest takes over.
    ///
    /// **The change lands before anything waits for it**, which is the
    /// order that catches [`Ownership::changed`] taking its baseline
    /// after the fact. See that method.
    #[tokio::test]
    async fn ownership_moves_when_the_lowest_member_leaves() {
        let (a, b, c) = (node(1), node(2), node(3));
        let membership = Membership::of(b, [a, c]);
        let mut ownership = membership.ownership();

        assert!(!membership.owns(), "a is live, so b owns nothing");
        assert!(!ownership.owns());

        assert!(membership.left(&a), "a leaves");

        assert!(
            handover(&mut ownership).await,
            "the change reported by the handle is b taking over"
        );
        assert!(ownership.owns());
        assert_eq!(membership.owner(), b, "b is now the lowest live member");
        assert!(membership.owns());
    }

    /// And when it comes back, ownership moves back.
    ///
    /// The other half, and the one that catches an implementation that
    /// only ever hands ownership *out*. A node that took over during an
    /// outage and kept the singleton when the rightful owner returned
    /// would be running it twice for as long as both were up.
    #[tokio::test]
    async fn ownership_moves_back_when_the_lowest_member_returns() {
        let (a, b, c) = (node(1), node(2), node(3));
        let membership = Membership::of(b, [c]);
        let mut ownership = membership.ownership();

        assert!(membership.owns(), "with only b and c live, b is the lowest");
        assert!(ownership.owns());

        assert!(membership.joined(a), "a returns");

        assert!(
            !handover(&mut ownership).await,
            "the change reported by the handle is b giving it up"
        );
        assert!(!ownership.owns());
        assert_eq!(membership.owner(), a);
        assert!(!membership.owns());
    }

    /// And when the waiter is already waiting, it is still woken.
    ///
    /// The other order, and both are worth having: the two tests above
    /// prove a change is not missed when it arrives first, and this one
    /// proves the handle is not merely polling something it read
    /// earlier. Convention 5 — ask what a test cannot reach. Neither
    /// ordering reaches the other.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_waiter_already_waiting_is_woken() {
        let (a, b) = (node(1), node(2));
        let membership = std::sync::Arc::new(Membership::of(b, [a]));
        let mut ownership = membership.ownership();
        assert!(!ownership.owns());

        let waiting = tokio::spawn(async move { handover(&mut ownership).await });
        // Nothing here can know the task has reached the await, so the
        // change is simply made and the handle is given the full
        // handover patience to report it. A handle that only looked
        // before waiting would hang; one that only waited would race.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(membership.left(&a));

        assert!(
            waiting.await.expect("the waiter did not panic"),
            "the waiter should have been told it now owns the singleton"
        );
    }

    /// A member joining or leaving above the owner moves nothing.
    ///
    /// The complement of the two above, and the reason
    /// [`Ownership::changed`] waits for the answer rather than for the
    /// view: a handle woken by every membership signal would stop and
    /// restart a connector each time any node in the cluster bounced.
    #[tokio::test]
    async fn a_change_that_does_not_move_ownership_wakes_nobody() {
        let (a, b, c) = (node(1), node(2), node(3));
        let membership = Membership::of(a, [b]);
        let mut ownership = membership.ownership();
        assert!(ownership.owns());

        assert!(membership.joined(c), "c joins, above a");
        assert!(membership.left(&b), "and b leaves, also above a");

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), ownership.changed())
                .await
                .is_err(),
            "a is still the lowest live member, so nothing has changed for it"
        );
        assert!(ownership.owns());
    }

    // -----------------------------------------------------------
    // The view itself
    // -----------------------------------------------------------

    /// A node is in its own view however it was built, and cannot be
    /// removed from it.
    ///
    /// Without this the rule has a state where it computes an owner
    /// this node can never be, and a singleton on a node that has
    /// somehow been told it is dead would stop for ever rather than
    /// carry on until something replaces it.
    #[test]
    fn a_node_is_always_in_its_own_view() {
        let membership = Membership::of(node(2), [node(1)]);
        assert!(membership.live().contains(&node(2)));

        assert!(!membership.left(&node(2)), "a node cannot see itself leave");
        assert!(membership.live().contains(&node(2)));

        // And with nobody else left, it owns everything again.
        assert!(membership.left(&node(1)));
        assert!(membership.owns());
    }

    #[test]
    fn joining_and_leaving_say_whether_the_view_moved() {
        let membership = Membership::of(node(1), []);
        assert!(membership.joined(node(2)));
        assert!(
            !membership.joined(node(2)),
            "a repeated signal is not an event"
        );
        assert!(membership.left(&node(2)));
        assert!(!membership.left(&node(2)), "nor is leaving twice");
    }
}
