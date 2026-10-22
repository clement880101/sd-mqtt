//! Which nodes want a topic.
//!
//! Section 8.3: each node holds the full subscription trie for its own
//! subscribers, **plus a separate map from filter to the set of nodes
//! that have at least one matching subscriber**. This is that map. A
//! publish matches locally, enqueues locally, and forwards one copy to
//! each node this answers with.
//!
//! # It is the subscription trie, in a second instance
//!
//! Matching a topic against other nodes' filters is the same problem
//! the local trie solves, and the trie is in a crate that must not
//! depend on this one. The three ways out were to reuse it, to write a
//! second matcher here, or to scan.
//!
//! **Reused**, through [`FilterIndex`], which is the trie with the
//! subscription taken out of it: a filter and an opaque slot in, the
//! slots whose filters matched out. The slot here is a filter's row in
//! [`Table::holders`], and the row holds the nodes.
//!
//! What that buys and what it costs:
//!
//! - A publish costs one trie walk — proportional to the published
//!   topic's depth and to how many wildcard branches it meets — and
//!   then one set union over the matched filters' rows. It does **not**
//!   grow with how many filters the cluster holds, which is the whole
//!   point: a linear scan comparing every remote filter against every
//!   published topic is the obvious implementation and it is the wrong
//!   one at the design's fifty-node ceiling.
//! - The memory is one trie node per distinct level of the distinct
//!   remote filters, plus a row per distinct filter holding a set of
//!   node identities. The registers section 8.2 already replicates hold
//!   the same facts at node granularity, so this is an index over them
//!   rather than a second copy of anything per client.
//! - A second matcher written here would have been a second answer to
//!   what `a/+` means, and the day the two disagreed the cluster would
//!   forward to a node whose own matching then delivered nothing — or,
//!   worse, deliver where the local trie would not have.
//!
//! # A dead node is withheld, not forgotten
//!
//! Section 8.2: **membership is deliberately not replicated.** Liveness
//! is each node's own judgement about its own links, so nothing is ever
//! written down when a node dies and no register covers the case. A
//! node that is dead must still stop receiving forwards, which is
//! [`Routes::died`].
//!
//! It takes the node out of every answer and **leaves its filters in the
//! map**, which is the one decision here worth arguing about. Removing
//! them is tempting and wrong: section 8.1 declares a peer dead on a
//! half-open link *without closing it*, and such a peer comes back on
//! the same connection having written nothing new, so a map that had
//! forgotten its filters would never route to it again. A node that
//! restarts does re-announce — see `Broker::discard_journal` — but a
//! node that was merely unheard from for a few seconds does not, and
//! that is the ordinary case rather than the exotic one.
//!
//! The cost of withholding instead is one set lookup per candidate node
//! per publish, over a set that is empty in a healthy cluster.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use broker_core::subscriptions::FilterIndex;

use crate::identity::NodeId;

/// How many shards the matcher inside a routing map uses.
///
/// Smaller than the broker's own trie, deliberately. This one holds a
/// filter per *filter the cluster has a subscriber for* rather than per
/// subscription, which is the granularity section 8.2 replicates at, and
/// every path into it is already behind one lock — so shards buy
/// nothing here beyond keeping a single hot first level from putting
/// every filter in one map.
const SHARDS: usize = 16;

/// The nodes that hold one filter.
#[derive(Debug)]
struct Holders {
    /// Which filter, so that the row can be taken out of the matcher
    /// when the last node lets go of it.
    filter: String,
    /// Who has a subscriber for it, dead ones included.
    ///
    /// Ordered, so that what [`Routes::nodes_for`] answers is in
    /// identity order whatever order the registers arrived in: a
    /// forward that went to two nodes in a different order on every
    /// publish would make a test about who was forwarded to a test
    /// about hashing.
    nodes: BTreeSet<NodeId>,
}

/// Everything behind the lock.
#[derive(Debug, Default)]
struct Table {
    /// A slot per distinct filter.
    slots: HashMap<String, u64>,
    /// What each slot holds.
    holders: HashMap<u64, Holders>,
    /// The next slot to hand out.
    ///
    /// Never reused. A slot is sixty-four bits and a filter that comes
    /// and goes a million times a second would take half a million
    /// years to exhaust it; reusing one would mean a matcher entry that
    /// outlived its row pointing at somebody else's.
    next: u64,
}

/// Which nodes this one has judged dead, and which it is hearing.
///
/// **One answer, shared by both maps.** Section 8.3's routing map and
/// section 8.3's group map both have to stop choosing a node that has
/// gone, and two sets would be two answers to one question — the kind
/// that disagree the first time a path updates one of them. So the
/// judgement is a thing of its own, held by [`crate::metadata::Replicated`]
/// and consulted by both.
///
/// See the [module documentation](crate::routes) for why a dead node's
/// filters are withheld rather than forgotten.
///
/// # Dead and answering are not each other's opposite
///
/// The two maps ask "has this node been judged dead?", and a node never
/// met has not been: routing to it is the mesh's problem, because there
/// is no link to send on. Section 8.2's relay asks the other question —
/// "is this node being heard from?" — and a node never met is not,
/// which is exactly the node a newcomer joining after it died needs
/// relayed. So both are kept, moved by the same two calls, and a node
/// this one has never met is neither.
#[derive(Debug, Default)]
pub struct Liveness {
    judged: Mutex<Judged>,
}

/// What [`Liveness`] holds.
#[derive(Debug, Default)]
struct Judged {
    /// Judged dead, and not heard from since.
    dead: BTreeSet<NodeId>,
    /// Heard from, and not judged dead since.
    answering: BTreeSet<NodeId>,
}

impl Liveness {
    /// Nothing is dead, and nobody has been heard from.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// This node has judged `node` dead.
    pub fn died(&self, node: NodeId) {
        let mut judged = self.held();
        judged.dead.insert(node);
        judged.answering.remove(&node);
    }

    /// And `node` is answering again, or for the first time.
    pub fn revived(&self, node: NodeId) {
        let mut judged = self.held();
        judged.dead.remove(&node);
        judged.answering.insert(node);
    }

    /// Whether `node` is one this node has judged dead.
    #[must_use]
    pub fn is_dead(&self, node: NodeId) -> bool {
        self.held().dead.contains(&node)
    }

    /// Whether `node` is being heard from: met, and not judged dead
    /// since.
    ///
    /// **Not `!is_dead`.** A node never met is not dead and is not
    /// answering either; see the type's documentation for which
    /// question each of section 8's paths asks.
    #[must_use]
    pub fn is_answering(&self, node: NodeId) -> bool {
        self.held().answering.contains(&node)
    }

    /// Whether anything is dead at all.
    ///
    /// The fast path: in a healthy cluster this is false, and a
    /// publication then pays one lock and one `is_empty` rather than a
    /// lookup per candidate node.
    #[must_use]
    pub fn all_alive(&self) -> bool {
        self.held().dead.is_empty()
    }

    fn held(&self) -> MutexGuard<'_, Judged> {
        self.judged.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Which nodes have a subscriber matching a topic.
///
/// # Behind a lock of its own, and not the register table's
///
/// [`crate::metadata::Replicated`] holds one of these and keeps it in
/// step with the registers. It is **beside** that type's state rather
/// than inside it, because the two are read on different paths: the
/// registers are written on the subscribe path and read by an exchange,
/// and this is read on the publish path. One lock over both would put
/// every publish behind every register write.
///
/// What that costs is that a reader can see this a moment before or
/// after the register that moved it, which is the same thing
/// `SubscriptionTrie` documents about itself and for the same reason. A
/// forward sent to a node whose last subscriber has just gone is
/// delivered to nobody there, and one not sent to a node whose first
/// subscriber has just arrived is a message that subscriber was never
/// going to see anyway.
#[derive(Debug)]
pub struct Routes {
    /// Which filters a topic matches, by slot.
    matcher: FilterIndex,
    table: Mutex<Table>,
    /// Which nodes this one has judged dead, shared with the group map.
    liveness: Arc<Liveness>,
}

impl Routes {
    /// An empty map: nobody wants anything.
    ///
    /// `liveness` is shared with [`crate::groups::Groups`], because both
    /// have to stop choosing a node that has gone and two sets would be
    /// two answers.
    #[must_use]
    pub fn new(liveness: Arc<Liveness>) -> Self {
        Self {
            matcher: FilterIndex::new(SHARDS),
            table: Mutex::new(Table::default()),
            liveness,
        }
    }

    /// `node` has at least one subscriber for `filter`.
    ///
    /// Idempotent: a register that says again what this already knew
    /// changes nothing, which matters because an anti-entropy exchange
    /// hands over whatever it is asked for rather than only what is new.
    pub fn added(&self, node: NodeId, filter: &str) {
        let mut table = self.held();
        if let Some(slot) = table.slots.get(filter).copied() {
            if let Some(holders) = table.holders.get_mut(&slot) {
                holders.nodes.insert(node);
                return;
            }
        }
        let slot = table.next;
        table.next += 1;
        table.slots.insert(filter.to_owned(), slot);
        table.holders.insert(
            slot,
            Holders {
                filter: filter.to_owned(),
                nodes: [node].into_iter().collect(),
            },
        );
        // Inserted into the matcher under the lock, so that a reader
        // cannot find a slot whose row is not there yet.
        self.matcher.insert(filter, slot);
    }

    /// `node` has no subscriber for `filter` any more.
    ///
    /// What a tombstone means here, and what a collected tombstone means
    /// too: both are this node no longer believing that node has a
    /// subscriber, and there is nothing else for either to mean.
    ///
    /// The filter's row goes when the last node lets go of it, matcher
    /// entry and all. Leaving it would be a map that grows with every
    /// filter the cluster has ever held rather than with the ones it
    /// holds, which at the ceiling is the difference between an index
    /// and a log.
    pub fn removed(&self, node: NodeId, filter: &str) {
        let mut table = self.held();
        let Some(slot) = table.slots.get(filter).copied() else {
            return;
        };
        let Some(holders) = table.holders.get_mut(&slot) else {
            return;
        };
        holders.nodes.remove(&node);
        if !holders.nodes.is_empty() {
            return;
        }
        let holders = table.holders.remove(&slot);
        table.slots.remove(filter);
        if let Some(holders) = holders {
            self.matcher.remove(&holders.filter, slot);
        }
    }

    /// Forget everything `node` ever said.
    ///
    /// For a node whose registers have been collected wholesale, which
    /// is not a thing anything does yet, and for a test that wants the
    /// difference between forgetting a node and judging it dead to be
    /// something it can write down.
    pub fn forgot(&self, node: NodeId) {
        let filters: Vec<String> = {
            let table = self.held();
            table
                .holders
                .values()
                .filter(|holders| holders.nodes.contains(&node))
                .map(|holders| holders.filter.clone())
                .collect()
        };
        for filter in filters {
            self.removed(node, &filter);
        }
    }

    /// Which nodes this one has judged dead.
    ///
    /// Shared with the group map. [`Liveness::died`] is what takes a
    /// node out of both.
    #[must_use]
    pub fn liveness(&self) -> &Arc<Liveness> {
        &self.liveness
    }

    /// Every node with a subscriber matching `topic`, in identity order.
    ///
    /// Nodes judged dead are not here, and this node is: it holds its
    /// own registers like any other, and which of the nodes that want a
    /// topic is *this* one is the caller's to know. See
    /// [`crate::forward`], which is where that subtraction belongs,
    /// because a map that quietly removed one node would be a map whose
    /// answer depended on who was asking.
    #[must_use]
    pub fn nodes_for(&self, topic: &str) -> Vec<NodeId> {
        let mut slots = Vec::new();
        self.matcher.matching(topic, &mut slots);
        if slots.is_empty() {
            return Vec::new();
        }
        let table = self.held();
        let mut nodes = BTreeSet::new();
        for slot in slots {
            if let Some(holders) = table.holders.get(&slot) {
                nodes.extend(holders.nodes.iter().copied());
            }
        }
        drop(table);
        if !self.liveness.all_alive() {
            nodes.retain(|node| !self.liveness.is_dead(*node));
        }
        nodes.into_iter().collect()
    }

    /// How many distinct filters the map holds.
    ///
    /// Exposed so that the pruning above can be asserted rather than
    /// assumed, for the reason `SubscriptionTrie::node_count` is: a map
    /// that answers correctly while never letting a filter go is a leak
    /// that no assertion about routing can see.
    #[must_use]
    pub fn filters_held(&self) -> usize {
        self.held().slots.len()
    }

    /// The same number as the matcher reads it.
    ///
    /// These must agree after every operation. They are two structures
    /// maintained by the same four methods, which is exactly the shape
    /// that drifts.
    #[must_use]
    pub fn filters_matched(&self) -> usize {
        self.matcher.len()
    }

    /// The table, recovering from a poisoned lock.
    ///
    /// The same decision [`crate::metadata::Replicated`] records: one
    /// unrelated panic must not turn into every later publish on this
    /// node forwarding nothing.
    fn held(&self) -> MutexGuard<'_, Table> {
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::NodeIdentity;

    /// A node identity, named so a failure says which one.
    fn node(name: &str) -> NodeId {
        NodeIdentity::mint(name).id()
    }

    /// An empty map with a liveness of its own.
    fn fresh() -> Routes {
        Routes::new(Arc::new(Liveness::new()))
    }

    /// A node never met is neither dead nor answering, and the two
    /// calls move both judgements together.
    #[test]
    fn a_node_never_met_is_neither_dead_nor_answering() {
        let liveness = Liveness::new();
        let a = node("a");
        assert!(!liveness.is_dead(a));
        assert!(
            !liveness.is_answering(a),
            "never heard from, so not answering"
        );
        liveness.revived(a);
        assert!(!liveness.is_dead(a));
        assert!(liveness.is_answering(a));
        liveness.died(a);
        assert!(liveness.is_dead(a));
        assert!(!liveness.is_answering(a), "judged dead, so not answering");
        assert!(!liveness.all_alive());
        liveness.revived(a);
        assert!(liveness.all_alive());
        assert!(liveness.is_answering(a));
    }

    #[test]
    fn a_node_that_said_it_wants_a_filter_is_returned_for_a_matching_topic() {
        let routes = fresh();
        let a = node("a");
        routes.added(a, "sensor/+/temp");
        assert_eq!(routes.nodes_for("sensor/1/temp"), vec![a]);
    }

    #[test]
    fn a_wildcard_filter_is_not_matched_as_a_literal() {
        // The mutation: a map keyed by string equality answers nothing
        // here, and every publication to a real topic is forwarded
        // nowhere while a node subscribed to the wildcard waits.
        let routes = fresh();
        let a = node("a");
        routes.added(a, "sensor/+/temp");
        routes.added(a, "site/#");
        assert_eq!(routes.nodes_for("sensor/9/temp"), vec![a]);
        assert_eq!(routes.nodes_for("site/one/two/three"), vec![a]);
        assert!(
            routes.nodes_for("sensor/1/humidity").is_empty(),
            "a filter that does not match must not be a route"
        );
    }

    #[test]
    fn two_nodes_subscribing_to_one_filter_are_both_returned() {
        let routes = fresh();
        let (a, b) = (node("a"), node("b"));
        routes.added(a, "a/b");
        routes.added(b, "a/b");
        let mut expected = vec![a, b];
        expected.sort();
        assert_eq!(routes.nodes_for("a/b"), expected);
        assert_eq!(routes.filters_held(), 1, "one filter, held by two nodes");
    }

    #[test]
    fn a_topic_nobody_wants_is_nobody() {
        let routes = fresh();
        routes.added(node("a"), "a/b");
        assert!(routes.nodes_for("c/d").is_empty());
    }

    #[test]
    fn a_tombstone_removes_the_node() {
        // What `Routes::removed` is: a register that says this node no
        // longer has a subscriber. A map that ignored it would forward
        // to that node for ever, which is section 8.2's reason for
        // writing a tombstone rather than dropping the register.
        let routes = fresh();
        let (a, b) = (node("a"), node("b"));
        routes.added(a, "a/b");
        routes.added(b, "a/b");
        routes.removed(a, "a/b");
        assert_eq!(routes.nodes_for("a/b"), vec![b]);
        routes.removed(b, "a/b");
        assert!(routes.nodes_for("a/b").is_empty());
    }

    #[test]
    fn the_last_node_to_leave_a_filter_takes_the_filter_with_it() {
        let routes = fresh();
        let a = node("a");
        routes.added(a, "a/b");
        routes.added(a, "c/#");
        assert_eq!(routes.filters_held(), 2);
        assert_eq!(routes.filters_matched(), 2);

        routes.removed(a, "a/b");
        assert_eq!(routes.filters_held(), 1, "the row went");
        assert_eq!(routes.filters_matched(), 1, "and so did the matcher entry");
        assert!(routes.nodes_for("a/b").is_empty());
        assert_eq!(routes.nodes_for("c/d/e"), vec![a]);
    }

    #[test]
    fn removing_something_that_was_never_added_changes_nothing() {
        let routes = fresh();
        let a = node("a");
        routes.added(a, "a/b");
        routes.removed(node("b"), "a/b");
        routes.removed(a, "nothing/held");
        assert_eq!(routes.nodes_for("a/b"), vec![a]);
        assert_eq!(routes.filters_held(), 1);
    }

    #[test]
    fn saying_the_same_thing_twice_is_one_subscriber() {
        // An exchange serves whatever it was asked for rather than only
        // what is new, so the same register arrives more than once as a
        // matter of course.
        let routes = fresh();
        let a = node("a");
        routes.added(a, "a/b");
        routes.added(a, "a/b");
        assert_eq!(routes.nodes_for("a/b"), vec![a]);
        assert_eq!(routes.filters_held(), 1);
        assert_eq!(routes.filters_matched(), 1);
        routes.removed(a, "a/b");
        assert!(
            routes.nodes_for("a/b").is_empty(),
            "one departure undoes it, because there was one arrival"
        );
    }

    #[test]
    fn a_node_judged_dead_stops_being_returned() {
        // The case **no register covers**: membership is not
        // replicated, so nothing was written down when this happened.
        let routes = fresh();
        let (a, b) = (node("a"), node("b"));
        routes.added(a, "a/b");
        routes.added(b, "a/b");
        routes.added(a, "only/a");

        routes.liveness().died(a);
        assert!(routes.liveness().is_dead(a));
        assert_eq!(
            routes.nodes_for("a/b"),
            vec![b],
            "a dead node must stop receiving forwards"
        );
        assert!(
            routes.nodes_for("only/a").is_empty(),
            "every one of its filters, not the ones it shares"
        );
    }

    #[test]
    fn a_node_that_comes_back_is_routed_to_again_without_re_announcing() {
        // Section 8.1 declares a peer dead on a link it never closed,
        // and such a peer comes back on that same connection having
        // written nothing new. A map that had forgotten its filters
        // would never route to it again.
        let routes = fresh();
        let a = node("a");
        routes.added(a, "a/b");
        routes.liveness().died(a);
        assert!(routes.nodes_for("a/b").is_empty());

        routes.liveness().revived(a);
        assert!(!routes.liveness().is_dead(a));
        assert_eq!(
            routes.nodes_for("a/b"),
            vec![a],
            "nothing was replayed, because nothing was thrown away"
        );
    }

    #[test]
    fn a_dead_node_is_still_told_about_and_still_remembered() {
        // A register that arrives for a node this one believes is dead
        // is held: the judgement is local and this node may be the one
        // that is wrong, and the register is the truth either way.
        let routes = fresh();
        let a = node("a");
        routes.liveness().died(a);
        routes.added(a, "a/b");
        assert!(routes.nodes_for("a/b").is_empty(), "still dead");
        routes.liveness().revived(a);
        assert_eq!(routes.nodes_for("a/b"), vec![a]);
    }

    #[test]
    fn forgetting_a_node_is_not_the_same_as_judging_it_dead() {
        let routes = fresh();
        let (a, b) = (node("a"), node("b"));
        routes.added(a, "a/b");
        routes.added(b, "a/b");
        routes.added(a, "only/a");

        routes.forgot(a);
        assert_eq!(routes.nodes_for("a/b"), vec![b]);
        assert_eq!(routes.filters_held(), 1, "its own filter went with it");
        assert_eq!(routes.filters_matched(), 1);
        assert!(!routes.liveness().is_dead(a), "forgotten is not dead");
    }

    #[test]
    fn several_filters_matching_one_topic_name_each_node_once() {
        let routes = fresh();
        let a = node("a");
        routes.added(a, "a/b");
        routes.added(a, "a/+");
        routes.added(a, "#");
        assert_eq!(
            routes.nodes_for("a/b"),
            vec![a],
            "three matching filters are one copy, not three"
        );
    }

    #[test]
    fn a_leading_wildcard_does_not_reach_a_dollar_topic() {
        // The trie's rule, inherited rather than reimplemented. A map
        // that answered otherwise would forward a broker status
        // publication to a node whose own matching then dropped it.
        let routes = fresh();
        let (a, b) = (node("a"), node("b"));
        routes.added(a, "#");
        routes.added(b, "$SYS/#");
        assert_eq!(
            routes.nodes_for("$SYS/broker/uptime"),
            vec![b],
            "the node holding `#` must not be routed a dollar topic"
        );
        assert_eq!(routes.nodes_for("ordinary/topic"), vec![a]);
    }
}
