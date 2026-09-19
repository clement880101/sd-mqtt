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
/// [`SharedSubscriptions::select`] is the seam where that policy will go.
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
                // its subscriber identifier is ever read, by `select`,
                // to learn which groups matched; the options come from
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

    /// Append one chosen member of each group matching `topic` to `out`.
    ///
    /// This is the seam for delivery policy. Today it is round-robin within
    /// each group; it will later prefer a local online member and then the
    /// least loaded remote node.
    pub fn select(&self, topic: &str, out: &mut Vec<Subscription>) {
        let mut matched = Vec::new();
        self.trie.matching(topic, &mut matched);
        if matched.is_empty() {
            return;
        }

        let mut groups = self.groups.write();
        for synthetic in matched {
            let Some(entry) = groups.by_id.get_mut(&synthetic.subscriber.0) else {
                continue;
            };
            if entry.members.is_empty() {
                continue;
            }
            let index = entry.cursor % entry.members.len();
            entry.cursor = entry.cursor.wrapping_add(1);
            out.push(entry.members[index].clone());
        }
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
    /// [`select`] will simply skip. Exposed so the invariant can be
    /// asserted rather than assumed, for the same reason
    /// [`SubscriptionTrie::node_count`] and [`RetainedStore::node_count`]
    /// are.
    ///
    /// This should equal [`group_count`] after every operation.
    ///
    /// [`select`]: Self::select
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

    fn picked(shared: &SharedSubscriptions, topic: &str) -> Vec<u64> {
        let mut out = Vec::new();
        shared.select(topic, &mut out);
        let mut ids: Vec<u64> = out.into_iter().map(|s| s.subscriber.0).collect();
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

        let mut out = Vec::new();
        shared.select("a/b", &mut out);
        assert_eq!(out[0].qos, QoS::ExactlyOnce);
    }
}
