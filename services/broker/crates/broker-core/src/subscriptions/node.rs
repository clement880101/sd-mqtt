//! One node of the subscription trie.

use std::collections::HashMap;

use foldhash::fast::RandomState;
use mqtt_codec::QoS;

use crate::topic::levels::{Interner, LevelId};

/// Identifies the session a subscription belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriberId(pub u64);

/// One subscriber's interest in one filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    /// Whose subscription this is.
    pub subscriber: SubscriberId,
    /// The maximum quality of service to deliver at.
    pub qos: QoS,
    /// Suppress delivery of this subscriber's own publications.
    pub no_local: bool,
    /// Forward the retain flag as published rather than clearing it.
    pub retain_as_published: bool,
    /// The MQTT 5 subscription identifier, echoed back on delivery.
    pub id: Option<u32>,
}

/// One level of the subscription trie.
///
/// Three kinds of edge leave a node: literal children keyed by interned
/// level, a single-level wildcard child, and a multi-level wildcard, which
/// is a terminal rather than an edge because nothing can follow it.
#[derive(Debug, Default)]
pub struct Node {
    children: HashMap<LevelId, Node, RandomState>,
    plus: Option<Box<Node>>,
    /// Subscribers whose filter ended exactly at this node.
    exact: Vec<Subscription>,
    /// Subscribers whose filter ended in a multi-level wildcard here.
    multi: Vec<Subscription>,
}

impl Node {
    /// Whether this node holds nothing and can be pruned.
    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
            && self.plus.is_none()
            && self.exact.is_empty()
            && self.multi.is_empty()
    }

    /// Whether this node or any descendant holds a subscription.
    ///
    /// Stops at the first one found, where `len` would count every
    /// subscription in the subtree. Correct pruning should make this
    /// agree with `is_empty` at a root, but this does not rely on that.
    pub fn holds_any(&self) -> bool {
        !self.exact.is_empty()
            || !self.multi.is_empty()
            || self.plus.as_ref().is_some_and(|child| child.holds_any())
            || self.children.values().any(Node::holds_any)
    }

    /// Add a subscription, replacing any the same subscriber already had on
    /// this filter. Returns whether it replaced one.
    ///
    /// Replacing rather than appending is the protocol's rule: subscribing
    /// again to a filter you already hold updates its options.
    pub fn insert(&mut self, interner: &Interner, filter: &str, sub: Subscription) -> bool {
        let levels: Vec<&str> = filter.split('/').collect();
        self.insert_at(interner, &levels, sub)
    }

    fn insert_at(&mut self, interner: &Interner, levels: &[&str], sub: Subscription) -> bool {
        let Some((level, rest)) = levels.split_first() else {
            return replace_or_push(&mut self.exact, sub);
        };

        match *level {
            "#" => replace_or_push(&mut self.multi, sub),
            "+" => {
                let child = self.plus.get_or_insert_with(|| Box::new(Node::default()));
                child.insert_at(interner, rest, sub)
            }
            literal => {
                // Interning here, on the write path, is what keeps the
                // table growing with vocabulary rather than with traffic.
                let id = interner.intern(literal);
                self.children
                    .entry(id)
                    .or_default()
                    .insert_at(interner, rest, sub)
            }
        }
    }

    /// Remove one subscriber's subscription to one filter, pruning any
    /// branch it leaves empty. Returns whether anything was removed.
    pub fn remove(&mut self, interner: &Interner, filter: &str, subscriber: SubscriberId) -> bool {
        let levels: Vec<&str> = filter.split('/').collect();
        self.remove_at(interner, &levels, subscriber)
    }

    fn remove_at(
        &mut self,
        interner: &Interner,
        levels: &[&str],
        subscriber: SubscriberId,
    ) -> bool {
        let Some((level, rest)) = levels.split_first() else {
            return drop_subscriber(&mut self.exact, subscriber);
        };

        match *level {
            "#" => drop_subscriber(&mut self.multi, subscriber),
            "+" => {
                let Some(child) = self.plus.as_mut() else {
                    return false;
                };
                let removed = if rest.is_empty() {
                    drop_subscriber(&mut child.exact, subscriber)
                } else {
                    child.remove_at(interner, rest, subscriber)
                };
                if removed && child.is_empty() {
                    self.plus = None;
                }
                removed
            }
            literal => {
                // A level never interned cannot be in the trie, and
                // interning it here would grow the table on a miss.
                let Some(id) = interner.lookup(literal) else {
                    return false;
                };
                let Some(child) = self.children.get_mut(&id) else {
                    return false;
                };
                let removed = if rest.is_empty() {
                    drop_subscriber(&mut child.exact, subscriber)
                } else {
                    child.remove_at(interner, rest, subscriber)
                };
                if removed && child.is_empty() {
                    self.children.remove(&id);
                }
                removed
            }
        }
    }

    /// Append every subscription matching `topic` to `out`.
    ///
    /// Appends rather than clears, so a caller can gather matches from
    /// several tries into one buffer and reuse it across publishes.
    pub fn matching(&self, interner: &Interner, topic: &str, out: &mut Vec<Subscription>) {
        let levels: Vec<&str> = topic.split('/').collect();
        self.match_at(interner, &levels, out);
    }

    fn match_at(&self, interner: &Interner, levels: &[&str], out: &mut Vec<Subscription>) {
        // A multi-level wildcard here matches this level and everything
        // below it, including the case where nothing is below.
        out.extend(self.multi.iter().cloned());

        let Some((level, rest)) = levels.split_first() else {
            out.extend(self.exact.iter().cloned());
            return;
        };

        if let Some(id) = interner.lookup(level) {
            if let Some(child) = self.children.get(&id) {
                child.match_at(interner, rest, out);
            }
        }
        if let Some(child) = self.plus.as_ref() {
            child.match_at(interner, rest, out);
        }
    }

    /// Remove every subscription belonging to one subscriber, wherever it
    /// is. Returns how many were removed.
    ///
    /// This walks the whole trie, so it is for session teardown rather than
    /// for an unsubscribe.
    pub fn remove_subscriber(&mut self, subscriber: SubscriberId) -> usize {
        let mut removed = 0;
        removed += usize::from(drop_subscriber(&mut self.exact, subscriber));
        removed += usize::from(drop_subscriber(&mut self.multi, subscriber));

        if let Some(child) = self.plus.as_mut() {
            removed += child.remove_subscriber(subscriber);
            if child.is_empty() {
                self.plus = None;
            }
        }
        self.children.retain(|_, child| {
            removed += child.remove_subscriber(subscriber);
            !child.is_empty()
        });

        removed
    }

    /// How many nodes this node and its descendants amount to, including
    /// itself.
    ///
    /// Exposed so that pruning can be tested rather than assumed: `len`
    /// counts subscriptions, so an orphaned empty branch left behind by a
    /// remove is invisible to it but not to this.
    pub fn node_count(&self) -> usize {
        1 + self.plus.as_ref().map_or(0, |child| child.node_count())
            + self.children.values().map(Node::node_count).sum::<usize>()
    }

    /// How many subscriptions this node and its descendants hold.
    pub fn len(&self) -> usize {
        self.exact.len()
            + self.multi.len()
            + self.plus.as_ref().map_or(0, |child| child.len())
            + self.children.values().map(Node::len).sum::<usize>()
    }
}

fn replace_or_push(list: &mut Vec<Subscription>, sub: Subscription) -> bool {
    if let Some(slot) = list.iter_mut().find(|s| s.subscriber == sub.subscriber) {
        *slot = sub;
        true
    } else {
        list.push(sub);
        false
    }
}

fn drop_subscriber(list: &mut Vec<Subscription>, subscriber: SubscriberId) -> bool {
    let before = list.len();
    list.retain(|s| s.subscriber != subscriber);
    list.len() != before
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::levels::Interner;
    use crate::topic::reference;
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

    /// Insert every filter with its index as the subscriber, then assert the
    /// node agrees with the reference matcher about which ones match.
    fn check(filters: &[&str], topic: &str) {
        let interner = Interner::new();
        let mut root = Node::default();
        for (index, filter) in filters.iter().enumerate() {
            root.insert(&interner, filter, sub(index as u64));
        }

        let mut got = Vec::new();
        root.matching(&interner, topic, &mut got);
        let mut got: Vec<u64> = got.into_iter().map(|s| s.subscriber.0).collect();
        got.sort_unstable();

        let mut expected: Vec<u64> = filters
            .iter()
            .enumerate()
            .filter(|(_, f)| reference::matches(f, topic))
            .map(|(index, _)| index as u64)
            .collect();
        expected.sort_unstable();

        assert_eq!(got, expected, "topic {topic:?} against {filters:?}");
    }

    const FILTERS: &[&str] = &[
        "a", "a/b", "a/b/c", "a/+", "a/+/c", "+/b", "+", "#", "a/#", "a/b/#", "+/+", "/a", "a/",
    ];

    #[test]
    fn agrees_with_the_reference_matcher() {
        for topic in [
            "a", "a/b", "a/b/c", "a/b/c/d", "b", "b/b", "/a", "a/", "a//c", "",
        ] {
            check(FILTERS, topic);
        }
    }

    #[test]
    fn a_multi_level_wildcard_matches_the_parent() {
        check(&["a/#"], "a");
        check(&["#"], "a/b/c");
    }

    #[test]
    fn removing_a_subscriber_removes_only_that_one() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a/b", sub(1));
        root.insert(&interner, "a/b", sub(2));

        assert!(root.remove(&interner, "a/b", SubscriberId(1)));

        let mut got = Vec::new();
        root.matching(&interner, "a/b", &mut got);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].subscriber, SubscriberId(2));
    }

    #[test]
    fn removing_the_last_subscriber_prunes_the_branch() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a/b/c", sub(1));
        assert!(!root.is_empty());
        assert!(root.remove(&interner, "a/b/c", SubscriberId(1)));
        assert!(root.is_empty(), "the whole branch should be gone");
    }

    #[test]
    fn removing_something_absent_reports_false() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a/b", sub(1));
        assert!(!root.remove(&interner, "a/b", SubscriberId(2)));
        assert!(!root.remove(&interner, "a/c", SubscriberId(1)));
        assert!(!root.remove(&interner, "x/y/z", SubscriberId(1)));
    }

    #[test]
    fn resubscribing_replaces_the_options_rather_than_duplicating() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a/b", sub(1));
        let replaced = root.insert(
            &interner,
            "a/b",
            Subscription {
                qos: QoS::ExactlyOnce,
                ..sub(1)
            },
        );
        assert!(replaced, "the second insert should report a replacement");

        let mut got = Vec::new();
        root.matching(&interner, "a/b", &mut got);
        assert_eq!(got.len(), 1, "a resubscribe must not duplicate");
        assert_eq!(got[0].qos, QoS::ExactlyOnce);
    }

    #[test]
    fn a_matching_call_appends_rather_than_clearing() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a", sub(1));

        let mut got = vec![sub(99)];
        root.matching(&interner, "a", &mut got);
        assert_eq!(got.len(), 2, "the caller's existing entries must survive");
    }

    #[test]
    fn matching_an_unseen_level_allocates_nothing_in_the_interner() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a", sub(1));
        let before = interner.len();

        let mut got = Vec::new();
        root.matching(&interner, "never/seen/before", &mut got);
        assert!(got.is_empty());
        assert_eq!(
            interner.len(),
            before,
            "matching must not grow the interner with traffic"
        );
    }
}
