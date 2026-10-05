//! Which node serves a shared group, chosen once.
//!
//! **This is the trap the routing plan was arranged around.** Section
//! 8.3: "shared subscriptions need their own map and cannot use this
//! one". A group with a member on each of three nodes has a matching
//! subscriber on each, so [`crate::routes`] says all three want the
//! topic — and one copy to each interested node is delivered three
//! times where the semantics demand one.
//!
//! So a group is replicated as a register of its own,
//! [`crate::metadata::MetadataKey::NodeGroup`], indexed here, and the
//! choice is made **before** forwarding rather than after: one node per
//! group per publication, and that node then chooses among its own
//! members.
//!
//! # A group is its name and its filter, together
//!
//! `$share/g1/a/b` and `$share/g2/a/b` are two groups and each is owed
//! its own delivery; `$share/g/a/b` and `$share/g/a/+` are two groups
//! too, because what a publication matches is a filter and these are two
//! filters. That is the same key `broker_core`'s own
//! `SharedSubscriptions` uses within a node, which is the point: the
//! pair is what the whole cluster agrees about, where the group
//! identifier a node hands out is a number it made up.
//!
//! # Section 6's preference, and the half of it that waits
//!
//! Section 6: selection "prefers a local online member, then the least
//! loaded remote node, round-robin within a node".
//!
//! - **The local preference is here**, and it is the cheap half as well
//!   as the required one: a group this node has a member of is served
//!   here, which saves a hop and a network copy, and the node's own
//!   `SharedSubscriptions` then rotates among its members.
//! - **The least loaded remote node waits for milestone 6**, and this is
//!   the deferral written into the code rather than left to be
//!   rediscovered. A load figure means a queue depth from another node,
//!   and no such number crosses a link yet: queue replication is what
//!   makes one exist and worth reading. Until then the choice among
//!   remote member nodes is a **rotation**, which is honest — it spreads
//!   the work evenly, which is what a shared subscription is for — and
//!   it is deliberately not dressed up as a load comparison against a
//!   figure that does not exist.
//!
//! A rotation also makes the thing a test can assert: over enough
//! publications every member node is chosen, which is what tells a
//! shared subscription from a fixed route.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use broker_core::subscriptions::FilterIndex;

use crate::identity::NodeId;
use crate::routes::Liveness;

/// How many shards the matcher inside a group map uses.
///
/// Fewer than [`crate::routes`]'s, because there are fewer groups than
/// filters: a group is something an operator's fleet of workers shares,
/// counted in tens rather than in the thousands a client population
/// subscribes to.
const SHARDS: usize = 8;

/// One group: which nodes have a member, and whose turn it is.
#[derive(Debug)]
struct Group {
    /// The share name.
    name: String,
    /// The inner filter, which is what a topic is matched against.
    filter: String,
    /// Which nodes have at least one member, dead ones included.
    ///
    /// Ordered, so that the rotation below is over a stable sequence: a
    /// cursor into a set that reordered itself would be a rotation that
    /// skipped and repeated.
    nodes: BTreeSet<NodeId>,
    /// Whose turn is next among the remote members.
    ///
    /// Only advanced when a remote node is chosen. A publication served
    /// locally spends nobody's turn, because nobody remote was asked:
    /// advancing it there would make the rotation depend on how much
    /// traffic the local member took, and two nodes each with a member
    /// would never agree about whose turn it was anyway.
    cursor: usize,
}

/// What the choice for one group came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chosen {
    /// The share name.
    pub group: String,
    /// The inner filter.
    pub filter: String,
    /// The node that serves this publication for this group.
    pub node: NodeId,
}

/// Everything behind the lock.
#[derive(Debug, Default)]
struct Table {
    /// A slot per group, by name and filter.
    slots: HashMap<(String, String), u64>,
    /// What each slot holds.
    groups: HashMap<u64, Group>,
    /// The next slot to hand out, never reused.
    next: u64,
}

/// Which nodes have a member of each shared group.
///
/// Built from the registers section 8.2 replicates and kept in step by
/// [`crate::metadata::Replicated`], the same way [`crate::routes::Routes`]
/// is and for the same reason: there are four paths that change a
/// register and an index fed from three of them is wrong in the fourth.
///
/// The matcher is `broker_core`'s subscription trie in a third instance;
/// see [`FilterIndex`] for what reusing it buys and costs.
#[derive(Debug)]
pub struct Groups {
    /// Which groups' filters a topic matches, by slot.
    matcher: FilterIndex,
    table: Mutex<Table>,
    /// Which nodes this one has judged dead, shared with the routing
    /// map: one answer to one question.
    liveness: Arc<Liveness>,
}

impl Groups {
    /// An empty map: no group has a member anywhere.
    #[must_use]
    pub fn new(liveness: Arc<Liveness>) -> Self {
        Self {
            matcher: FilterIndex::new(SHARDS),
            table: Mutex::new(Table::default()),
            liveness,
        }
    }

    /// `node` has at least one member of `group` on `filter`.
    ///
    /// Idempotent, for the reason [`crate::routes::Routes::added`] is: an
    /// exchange serves whatever it was asked for rather than only what
    /// is new.
    pub fn added(&self, node: NodeId, group: &str, filter: &str) {
        let key = (group.to_owned(), filter.to_owned());
        let mut table = self.held();
        if let Some(slot) = table.slots.get(&key).copied() {
            if let Some(held) = table.groups.get_mut(&slot) {
                held.nodes.insert(node);
                return;
            }
        }
        let slot = table.next;
        table.next += 1;
        table.slots.insert(key, slot);
        table.groups.insert(
            slot,
            Group {
                name: group.to_owned(),
                filter: filter.to_owned(),
                nodes: [node].into_iter().collect(),
                cursor: 0,
            },
        );
        // Under the lock, so that a reader cannot match a slot whose
        // group is not there yet.
        self.matcher.insert(filter, slot);
    }

    /// `node` has no member of `group` on `filter` any more.
    ///
    /// The group goes when the last node lets go of it, matcher entry
    /// and all, for the reason a filter's row does.
    pub fn removed(&self, node: NodeId, group: &str, filter: &str) {
        let key = (group.to_owned(), filter.to_owned());
        let mut table = self.held();
        let Some(slot) = table.slots.get(&key).copied() else {
            return;
        };
        let Some(held) = table.groups.get_mut(&slot) else {
            return;
        };
        held.nodes.remove(&node);
        if !held.nodes.is_empty() {
            return;
        }
        table.groups.remove(&slot);
        table.slots.remove(&key);
        self.matcher.remove(filter, slot);
    }

    /// Every group matching `topic`, with the node chosen to serve it.
    ///
    /// **One node per group**, which is the whole of this module. The
    /// choice prefers `me` when this node has a member — section 6's
    /// local preference, which saves a hop and a network copy — and
    /// otherwise rotates among the member nodes that are not dead.
    ///
    /// A group whose every member node is dead is **not** returned. It
    /// has nobody to deliver to, and choosing a node this one believes
    /// is gone would be a publication framed for a link that is not
    /// there. A member whose *queue* is full is a different matter and
    /// is `broker_core`'s to skip, within whichever node is chosen.
    ///
    /// The cursor advances only when a remote node is chosen; see
    /// [`Group::cursor`].
    #[must_use]
    pub fn chosen_for(&self, topic: &str, me: NodeId) -> Vec<Chosen> {
        let mut slots = Vec::new();
        self.matcher.matching(topic, &mut slots);
        if slots.is_empty() {
            return Vec::new();
        }
        // Sorted and deduplicated, because a node holding two filters of
        // one group is two slots and a topic matching both would
        // otherwise choose twice for what is, by the key, two groups —
        // which is correct — while a single group matched twice through
        // one slot would not be. The sort is what makes the answer
        // ordered rather than hash-ordered.
        slots.sort_unstable();
        slots.dedup();
        let mut table = self.held();
        let mut chosen = Vec::with_capacity(slots.len());
        for slot in slots {
            let Some(group) = table.groups.get_mut(&slot) else {
                continue;
            };
            let Some(node) = pick(group, me, &self.liveness) else {
                continue;
            };
            chosen.push(Chosen {
                group: group.name.clone(),
                filter: group.filter.clone(),
                node,
            });
        }
        chosen
    }

    /// Which nodes have a member of `group` on `filter`, in identity
    /// order, dead ones included.
    ///
    /// For a test and for an operator. What routing asks is
    /// [`Self::chosen_for`].
    #[must_use]
    pub fn members_of(&self, group: &str, filter: &str) -> Vec<NodeId> {
        let key = (group.to_owned(), filter.to_owned());
        let table = self.held();
        table
            .slots
            .get(&key)
            .and_then(|slot| table.groups.get(slot))
            .map(|group| group.nodes.iter().copied().collect())
            .unwrap_or_default()
    }

    /// How many groups the map holds.
    ///
    /// Exposed so that the pruning above can be asserted rather than
    /// assumed, for the reason [`crate::routes::Routes::filters_held`]
    /// is.
    #[must_use]
    pub fn groups_held(&self) -> usize {
        self.held().slots.len()
    }

    /// The same number as the matcher reads it. These must agree.
    #[must_use]
    pub fn groups_matched(&self) -> usize {
        self.matcher.len()
    }

    fn held(&self) -> MutexGuard<'_, Table> {
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Which node serves one group, and advance its rotation if remote.
///
/// Section 6's policy, with the one clause that is implementable now.
fn pick(group: &mut Group, me: NodeId, liveness: &Liveness) -> Option<NodeId> {
    // **The local preference, first and unconditionally.** A group this
    // node has a member of is served here: section 6 asks for it, and it
    // saves a hop and a network copy. A local member whose queue is full
    // is not a reason to go to another node — that skip is within a
    // node, among its own members, and is `broker_core`'s.
    if group.nodes.contains(&me) {
        return Some(me);
    }
    // Every other member, dead ones taken out. A node this one believes
    // is gone has nobody reachable to deliver to, so choosing it would
    // forward into a link that is not there.
    let alive: Vec<NodeId> = group
        .nodes
        .iter()
        .copied()
        .filter(|node| !liveness.is_dead(*node))
        .collect();
    if alive.is_empty() {
        return None;
    }
    let at = group.cursor % alive.len();
    group.cursor = group.cursor.wrapping_add(1);
    Some(alive[at])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::NodeIdentity;
    use std::collections::BTreeSet as Set;

    fn node(name: &str) -> NodeId {
        NodeIdentity::mint(name).id()
    }

    fn fresh() -> (Groups, Arc<Liveness>) {
        let liveness = Arc::new(Liveness::new());
        (Groups::new(Arc::clone(&liveness)), liveness)
    }

    /// The node chosen for the one group matching `topic`.
    fn one(groups: &Groups, topic: &str, me: NodeId) -> Option<NodeId> {
        let chosen = groups.chosen_for(topic, me);
        assert!(
            chosen.len() <= 1,
            "one group matches {topic}, and {} were chosen for",
            chosen.len()
        );
        chosen.first().map(|it| it.node)
    }

    #[test]
    fn one_node_is_chosen_for_a_group_spanning_three() {
        // The trap, at the layer that answers it. Three nodes have a
        // member and the answer is one of them, not three.
        let (groups, _) = fresh();
        let (a, b, c) = (node("a"), node("b"), node("c"));
        let watcher = node("watcher");
        for member in [a, b, c] {
            groups.added(member, "workers", "jobs/+");
        }
        let chosen = groups.chosen_for("jobs/one", watcher);
        assert_eq!(chosen.len(), 1, "one node per group per publication");
        assert!([a, b, c].contains(&chosen[0].node));
        assert_eq!(chosen[0].group, "workers");
        assert_eq!(chosen[0].filter, "jobs/+");
    }

    #[test]
    fn a_local_member_is_preferred() {
        // Section 6, and the cheap half as well as the required one: it
        // saves a hop and a network copy.
        let (groups, _) = fresh();
        let (me, other) = (node("me"), node("other"));
        groups.added(me, "workers", "jobs/+");
        groups.added(other, "workers", "jobs/+");
        for _ in 0..20 {
            assert_eq!(
                one(&groups, "jobs/one", me),
                Some(me),
                "a node with a member of its own must serve the group itself"
            );
        }
    }

    #[test]
    fn every_member_node_is_chosen_over_many_publications() {
        // What makes it a shared subscription rather than a fixed route.
        // Asked from a node with no member of its own, because a node
        // with one always answers itself.
        let (groups, _) = fresh();
        let members: Vec<NodeId> = ["a", "b", "c"].iter().map(|name| node(name)).collect();
        for member in &members {
            groups.added(*member, "workers", "jobs/+");
        }
        let watcher = node("watcher");
        let mut seen = Set::new();
        for _ in 0..30 {
            seen.insert(one(&groups, "jobs/one", watcher).expect("somebody is chosen"));
        }
        assert_eq!(
            seen,
            members.iter().copied().collect::<Set<_>>(),
            "the rotation never reached every member node"
        );
    }

    #[test]
    fn the_rotation_is_even_over_the_member_nodes() {
        // A rotation and not a random pick, which is what section 6's
        // "round-robin" asks for and what makes the deferred least-loaded
        // ordering a refinement rather than a replacement.
        let (groups, _) = fresh();
        let members: Vec<NodeId> = ["a", "b", "c"].iter().map(|name| node(name)).collect();
        for member in &members {
            groups.added(*member, "workers", "jobs/+");
        }
        let watcher = node("watcher");
        let mut counts: HashMap<NodeId, usize> = HashMap::new();
        for _ in 0..30 {
            *counts
                .entry(one(&groups, "jobs/one", watcher).expect("somebody is chosen"))
                .or_default() += 1;
        }
        for member in &members {
            assert_eq!(counts.get(member), Some(&10), "the rotation is not even");
        }
    }

    #[test]
    fn a_dead_member_node_is_not_chosen() {
        let (groups, liveness) = fresh();
        let (a, b) = (node("a"), node("b"));
        groups.added(a, "workers", "jobs/+");
        groups.added(b, "workers", "jobs/+");
        liveness.died(a);
        for _ in 0..10 {
            assert_eq!(one(&groups, "jobs/one", node("watcher")), Some(b));
        }
        assert_eq!(
            groups.members_of("workers", "jobs/+"),
            {
                let mut both = vec![a, b];
                both.sort();
                both
            },
            "the register is the truth and is kept; only the choice is withheld"
        );
    }

    #[test]
    fn a_group_whose_every_member_is_dead_is_not_chosen_for() {
        // Nobody to deliver to, so choosing anyone would frame a
        // publication for a link that is not there.
        let (groups, liveness) = fresh();
        let a = node("a");
        groups.added(a, "workers", "jobs/+");
        liveness.died(a);
        assert!(groups.chosen_for("jobs/one", node("watcher")).is_empty());
        liveness.revived(a);
        assert_eq!(one(&groups, "jobs/one", node("watcher")), Some(a));
    }

    #[test]
    fn a_wildcard_group_filter_matches_a_topic() {
        let (groups, _) = fresh();
        let a = node("a");
        groups.added(a, "workers", "jobs/#");
        assert_eq!(one(&groups, "jobs/one/two", node("watcher")), Some(a));
        assert!(groups.chosen_for("other/one", node("watcher")).is_empty());
    }

    #[test]
    fn the_name_and_the_filter_together_identify_a_group() {
        // Two groups on one filter are two deliveries, and one group on
        // two filters is two groups.
        let (groups, _) = fresh();
        let a = node("a");
        groups.added(a, "one", "jobs/+");
        groups.added(a, "two", "jobs/+");
        let chosen = groups.chosen_for("jobs/x", node("watcher"));
        let names: Set<String> = chosen.iter().map(|it| it.group.clone()).collect();
        assert_eq!(
            names,
            ["one".to_owned(), "two".to_owned()].into_iter().collect(),
            "each group is owed its own delivery"
        );
        assert_eq!(groups.groups_held(), 2);
    }

    #[test]
    fn the_last_node_to_leave_a_group_takes_the_group_with_it() {
        let (groups, _) = fresh();
        let (a, b) = (node("a"), node("b"));
        groups.added(a, "workers", "jobs/+");
        groups.added(b, "workers", "jobs/+");
        assert_eq!(groups.groups_held(), 1);

        groups.removed(a, "workers", "jobs/+");
        assert_eq!(one(&groups, "jobs/x", node("watcher")), Some(b));
        groups.removed(b, "workers", "jobs/+");
        assert_eq!(groups.groups_held(), 0, "the row went");
        assert_eq!(groups.groups_matched(), 0, "and the matcher entry with it");
        assert!(groups.chosen_for("jobs/x", node("watcher")).is_empty());
    }

    #[test]
    fn saying_the_same_thing_twice_is_one_member() {
        let (groups, _) = fresh();
        let a = node("a");
        groups.added(a, "workers", "jobs/+");
        groups.added(a, "workers", "jobs/+");
        assert_eq!(groups.members_of("workers", "jobs/+"), vec![a]);
        groups.removed(a, "workers", "jobs/+");
        assert!(groups.members_of("workers", "jobs/+").is_empty());
    }

    #[test]
    fn removing_something_that_was_never_added_changes_nothing() {
        let (groups, _) = fresh();
        let a = node("a");
        groups.added(a, "workers", "jobs/+");
        groups.removed(node("b"), "workers", "jobs/+");
        groups.removed(a, "others", "jobs/+");
        assert_eq!(groups.members_of("workers", "jobs/+"), vec![a]);
    }

    #[test]
    fn a_local_member_spends_nobody_elses_turn() {
        // The cursor advances only when a remote node is chosen. A
        // publication served locally that moved it would make the
        // rotation depend on how much traffic the local member took.
        let (groups, _) = fresh();
        let (me, a, b) = (node("me"), node("a"), node("b"));
        for member in [me, a, b] {
            groups.added(member, "workers", "jobs/+");
        }
        // Served here, twenty times, which must leave the rotation where
        // it started.
        for _ in 0..20 {
            assert_eq!(one(&groups, "jobs/x", me), Some(me));
        }
        // And a node with no member of its own then sees a rotation that
        // starts at the beginning.
        let watcher = node("watcher");
        let first = one(&groups, "jobs/x", watcher);
        let second = one(&groups, "jobs/x", watcher);
        assert_ne!(
            first, second,
            "the rotation did not move, or it had been spun by the local deliveries"
        );
    }
}
