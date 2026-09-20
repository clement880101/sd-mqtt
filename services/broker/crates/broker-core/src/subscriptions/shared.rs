//! Shared subscription groups.

use std::collections::HashMap;

use foldhash::fast::RandomState;
use parking_lot::RwLock;

use mqtt_codec::QoS;

use crate::subscriptions::node::{SubscriberId, Subscription};
use crate::subscriptions::trie::SubscriptionTrie;

/// Shared subscription groups, keyed by share name and filter together.
///
/// The trie holds one synthetic entry per group rather than one per
/// member, so matching a topic yields the matching groups directly instead
/// of requiring a scan over every group that exists.
///
/// Selection is round-robin within a group. The specification's fuller
/// policy, preferring a local online member and then the least loaded
/// remote node, needs session and cluster state that does not exist yet;
/// [`SharedSubscriptions::next_member`] is the seam where that policy
/// will go.
///
/// Nothing here can see a session, deliberately. The one clause of the
/// policy that is implemented, skipping a member whose queue is full,
/// lives in the caller for that reason: it asks for a member, tries it,
/// and asks again if that member cannot take the message.
pub struct SharedSubscriptions {
    /// Keyed by a synthetic subscriber identifier standing for a group.
    trie: SubscriptionTrie,
    groups: RwLock<Groups>,
}

#[derive(Default)]
struct Groups {
    by_key: HashMap<(Box<str>, Box<str>), u64, RandomState>,
    by_id: HashMap<u64, Group, RandomState>,
    next_id: u64,
}

struct Group {
    key: (Box<str>, Box<str>),
    members: Vec<Subscription>,
    cursor: usize,
}

/// One shared group, as the caller refers to it between asking which
/// groups matched and asking them for a member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GroupId(pub u64);

impl SharedSubscriptions {
    /// An empty set of groups, over a trie with `shards` shards.
    pub fn new(shards: usize) -> Self {
        Self {
            trie: SubscriptionTrie::new(shards),
            groups: RwLock::new(Groups {
                by_key: HashMap::with_hasher(RandomState::default()),
                by_id: HashMap::with_hasher(RandomState::default()),
                next_id: 0,
            }),
        }
    }

    /// Add a member to the group named by `group` on `filter`, replacing
    /// any subscription that member already had there. Returns whether it
    /// replaced one.
    ///
    /// `filter` is the inner filter, with the `$share/<name>/` prefix
    /// already stripped by the caller.
    pub fn insert(&self, group: &str, filter: &str, sub: Subscription) -> bool {
        let key = (Box::from(group), Box::from(filter));
        let mut groups = self.groups.write();

        // Read the lookup out before the match, so its borrow does not
        // live across the arm that mutates the same map.
        let existing = groups.by_key.get(&key).copied();
        let id = match existing {
            Some(id) => id,
            None => {
                let id = groups.next_id;
                groups.next_id += 1;
                groups.by_key.insert(key.clone(), id);
                groups.by_id.insert(
                    id,
                    Group {
                        key: key.clone(),
                        members: Vec::new(),
                        cursor: 0,
                    },
                );
                // One synthetic entry stands for the whole group. Only
                // its subscriber identifier is ever read, by
                // `matching`, to learn which groups matched; the options come from
                // the chosen member, never from here. They are spelled
                // out as defaults rather than copied from whichever
                // member happened to create the group, which made them
                // look as though they meant something.
                self.trie.insert(
                    filter,
                    Subscription {
                        subscriber: SubscriberId(id),
                        qos: QoS::AtMostOnce,
                        no_local: false,
                        retain_as_published: false,
                        id: None,
                    },
                );
                id
            }
        };

        let entry = groups.by_id.get_mut(&id).expect("group just created");
        if let Some(slot) = entry
            .members
            .iter_mut()
            .find(|m| m.subscriber == sub.subscriber)
        {
            *slot = sub;
            true
        } else {
            entry.members.push(sub);
            false
        }
    }

    /// Remove a member. Returns whether anything was removed. Removing the
    /// last member removes the group.
    pub fn remove(&self, group: &str, filter: &str, subscriber: SubscriberId) -> bool {
        let key = (Box::from(group), Box::from(filter));
        let mut groups = self.groups.write();

        let Some(id) = groups.by_key.get(&key).copied() else {
            return false;
        };
        let Some(entry) = groups.by_id.get_mut(&id) else {
            return false;
        };

        let before = entry.members.len();
        entry.members.retain(|m| m.subscriber != subscriber);
        if entry.members.len() == before {
            return false;
        }

        if entry.members.is_empty() {
            groups.by_id.remove(&id);
            groups.by_key.remove(&key);
            self.trie.remove(filter, SubscriberId(id));
        }
        true
    }

    /// Remove one subscriber from every group it belongs to. Returns how
    /// many memberships went.
    pub fn remove_subscriber(&self, subscriber: SubscriberId) -> usize {
        let mut groups = self.groups.write();
        let mut removed = 0;
        let mut emptied: Vec<u64> = Vec::new();

        for (id, entry) in groups.by_id.iter_mut() {
            let before = entry.members.len();
            entry.members.retain(|m| m.subscriber != subscriber);
            removed += before - entry.members.len();
            if entry.members.is_empty() {
                emptied.push(*id);
            }
        }

        for id in emptied {
            if let Some(entry) = groups.by_id.remove(&id) {
                self.trie.remove(&entry.key.1, SubscriberId(id));
                groups.by_key.remove(&entry.key);
            }
        }

        removed
    }

    /// Append every group whose filter matches `topic` to `out`.
    ///
    /// One entry per group, never per member: choosing among the members
    /// is [`SharedSubscriptions::next_member`]'s job, and the caller's to
    /// repeat.
    pub fn matching(&self, topic: &str, out: &mut Vec<GroupId>) {
        let mut matched = Vec::new();
        self.trie.matching(topic, &mut matched);
        out.extend(matched.into_iter().map(|s| GroupId(s.subscriber.0)));
    }

    /// How many members `group` has, or zero if it no longer exists.
    ///
    /// The bound on how many times a caller may ask for a member before
    /// giving up: one full turn of the rotation and no more.
    pub fn size_of(&self, group: GroupId) -> usize {
        self.groups
            .read()
            .by_id
            .get(&group.0)
            .map_or(0, |entry| entry.members.len())
    }

    /// Take the next member of `group` in the rotation.
    ///
    /// This is the seam for delivery policy. Today it is round-robin
    /// within the group; it will later prefer a local online member and
    /// then the least loaded remote node.
    ///
    /// **Every call is a turn.** The cursor advances whether or not the
    /// caller goes on to use the member it was given, so a member the
    /// caller refuses because its queue is full has spent its turn on
    /// the message it could not have taken anyway. It is neither moved
    /// to the back of the rotation nor held at the front of it: it keeps
    /// its place, and the next message that reaches it is the next one
    /// its turn comes round for, as soon as it has drained. The
    /// alternative, holding the cursor still on a refusal, would ask the
    /// same full member first on every message until it drained, paying
    /// a wasted ask each time to gain nothing: the member was never
    /// going to receive those messages either way.
    ///
    /// Returns `None` only when the group has gone, which a caller
    /// holding a [`GroupId`] from [`SharedSubscriptions::matching`] can
    /// still see if it was emptied in between.
    pub fn next_member(&self, group: GroupId) -> Option<Subscription> {
        let mut groups = self.groups.write();
        let entry = groups.by_id.get_mut(&group.0)?;
        if entry.members.is_empty() {
            return None;
        }
        let index = entry.cursor % entry.members.len();
        entry.cursor = entry.cursor.wrapping_add(1);
        Some(entry.members[index].clone())
    }

    /// How many groups exist.
    pub fn group_count(&self) -> usize {
        self.groups.read().by_id.len()
    }

    /// How many memberships exist across all groups.
    pub fn member_count(&self) -> usize {
        self.groups
            .read()
            .by_id
            .values()
            .map(|g| g.members.len())
            .sum()
    }

    /// How many synthetic entries the inner trie holds.
    ///
    /// The design rests on one entry per live group, which is what lets
    /// matching a topic yield the matching groups directly rather than
    /// scanning every group that exists. That invariant holds by
    /// construction, and a violation of it leaks quietly rather than
    /// returning a wrong answer: a stale entry names a group that
    /// [`next_member`] will simply decline to answer for. Exposed so
    /// the invariant can be
    /// asserted rather than assumed, for the same reason
    /// [`SubscriptionTrie::node_count`] and [`RetainedStore::node_count`]
    /// are.
    ///
    /// This should equal [`group_count`] after every operation.
    ///
    /// [`next_member`]: Self::next_member
    /// [`group_count`]: Self::group_count
    /// [`RetainedStore::node_count`]: crate::retained::RetainedStore::node_count
    pub fn synthetic_count(&self) -> usize {
        self.trie.len()
    }

    /// Whether no group exists.
    pub fn is_empty(&self) -> bool {
        self.group_count() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqtt_codec::QoS;

    fn sub(id: u64) -> Subscription {
        Subscription {
            subscriber: SubscriberId(id),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            id: None,
        }
    }

    /// One member from each group matching `topic`, the way a caller
    /// with room everywhere would take them.
    fn picked(shared: &SharedSubscriptions, topic: &str) -> Vec<u64> {
        let mut groups = Vec::new();
        shared.matching(topic, &mut groups);
        let mut ids: Vec<u64> = groups
            .iter()
            .filter_map(|group| shared.next_member(*group))
            .map(|s| s.subscriber.0)
            .collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn one_member_of_a_group_receives_each_message() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert("g", "a/b", sub(2));
        shared.insert("g", "a/b", sub(3));

        for _ in 0..10 {
            assert_eq!(picked(&shared, "a/b").len(), 1);
        }
    }

    #[test]
    fn selection_rotates_through_the_members() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert("g", "a/b", sub(2));
        shared.insert("g", "a/b", sub(3));

        let mut seen = std::collections::HashSet::new();
        for _ in 0..30 {
            seen.extend(picked(&shared, "a/b"));
        }
        assert_eq!(seen.len(), 3, "every member should be picked eventually");
    }

    #[test]
    fn every_ask_is_a_turn_whether_or_not_the_caller_uses_the_answer() {
        // The rotation's answer to starvation. A caller that refuses a
        // member because its queue is full spends that member's turn,
        // and the cursor moves on: the member keeps its place rather
        // than being held at the front or pushed to the back, so it is
        // picked again on its next turn as soon as it drains.
        //
        // Asserted by asking four times and using none of the answers,
        // which is exactly what a caller refusing every member does.
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert("g", "a/b", sub(2));

        let mut groups = Vec::new();
        shared.matching("a/b", &mut groups);
        let asked: Vec<u64> = (0..4)
            .filter_map(|_| shared.next_member(groups[0]))
            .map(|member| member.subscriber.0)
            .collect();
        assert_eq!(asked, vec![1, 2, 1, 2]);
    }

    #[test]
    fn a_groups_size_is_how_many_members_it_holds() {
        // The bound a caller retries within. One short and a member is
        // never asked; one long and a full group is asked twice about
        // somebody.
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        let mut groups = Vec::new();
        shared.matching("a/b", &mut groups);
        let group = groups[0];
        assert_eq!(shared.size_of(group), 1);

        shared.insert("g", "a/b", sub(2));
        assert_eq!(shared.size_of(group), 2);

        shared.remove("g", "a/b", SubscriberId(1));
        assert_eq!(shared.size_of(group), 1);

        shared.remove("g", "a/b", SubscriberId(2));
        assert_eq!(
            shared.size_of(group),
            0,
            "a group that is gone has no members to ask"
        );
        assert!(shared.next_member(group).is_none());
    }

    #[test]
    fn the_share_name_and_the_filter_together_identify_a_group() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert("g", "a/+", sub(2));
        shared.insert("h", "a/b", sub(3));

        // Three distinct groups all match, so all three receive one each.
        assert_eq!(picked(&shared, "a/b").len(), 3);
    }

    #[test]
    fn a_wildcard_group_filter_still_matches() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/#", sub(1));
        assert_eq!(picked(&shared, "a/b/c"), vec![1]);
    }

    #[test]
    fn removing_the_last_member_removes_the_group() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        assert_eq!(shared.group_count(), 1);

        assert!(shared.remove("g", "a/b", SubscriberId(1)));
        assert_eq!(shared.group_count(), 0);
        assert!(picked(&shared, "a/b").is_empty());
    }

    #[test]
    fn removing_one_member_leaves_the_group() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert("g", "a/b", sub(2));

        assert!(shared.remove("g", "a/b", SubscriberId(1)));
        assert_eq!(shared.group_count(), 1);
        assert_eq!(picked(&shared, "a/b"), vec![2]);
    }

    #[test]
    fn removing_a_subscriber_clears_it_from_every_group() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert("h", "c/d", sub(1));
        shared.insert("h", "c/d", sub(2));

        assert_eq!(shared.remove_subscriber(SubscriberId(1)), 2);
        assert_eq!(shared.group_count(), 1);
        assert_eq!(picked(&shared, "c/d"), vec![2]);
    }

    #[test]
    fn resubscribing_updates_options_rather_than_duplicating() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert(
            "g",
            "a/b",
            Subscription {
                qos: QoS::ExactlyOnce,
                ..sub(1)
            },
        );
        assert_eq!(shared.member_count(), 1);

        let mut groups = Vec::new();
        shared.matching("a/b", &mut groups);
        let member = shared
            .next_member(groups[0])
            .expect("the group still has its one member");
        assert_eq!(member.qos, QoS::ExactlyOnce);
    }
}
