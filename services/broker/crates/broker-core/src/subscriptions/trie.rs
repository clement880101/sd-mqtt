//! The sharded subscription trie.

use std::hash::BuildHasher;

use foldhash::fast::RandomState;
use parking_lot::RwLock;

use crate::subscriptions::node::{Node, SubscriberId, Subscription};
use crate::topic::levels::Interner;

/// A subscription trie sharded by first topic level.
///
/// Filters whose first level is literal live in the shard that level hashes
/// to. Filters whose first level is a wildcard can match a topic in any
/// shard, so they live in a separate root that every publish also consults.
///
/// # Concurrency
///
/// Locking is per shard, and no operation holds more than one lock at a
/// time. That makes a single insert or remove of one filter atomic, and
/// makes anything spanning shards not atomic:
///
/// - [`matching`] reads one shard, releases it, then reads the wildcard
///   root. A session being torn down concurrently can therefore be
///   observed half gone, with its shard subscriptions already removed and
///   its wildcard subscription still present, or the reverse.
/// - [`len`], [`node_count`], [`is_empty`] and [`remove_subscriber`] take
///   each lock in turn, so a total they return may correspond to no
///   single instant.
///
/// The session layer will reason about teardown ordering, so this is
/// stated rather than left to be discovered: if a caller needs a session
/// to become invisible atomically, it has to arrange that above this
/// type, for instance by marking the session dead where delivery checks
/// it, and treating [`remove_subscriber`] as reclamation afterwards.
///
/// [`matching`]: Self::matching
/// [`len`]: Self::len
/// [`node_count`]: Self::node_count
/// [`is_empty`]: Self::is_empty
/// [`remove_subscriber`]: Self::remove_subscriber
pub struct SubscriptionTrie {
    interner: Interner,
    shards: Box<[RwLock<Node>]>,
    /// Filters beginning with `+` or `#`, which belong to no shard.
    wildcard: RwLock<Node>,
    hasher: RandomState,
}

impl SubscriptionTrie {
    /// A trie with `shards` shards. Zero is treated as one.
    pub fn new(shards: usize) -> Self {
        let shards = shards.max(1);
        Self {
            interner: Interner::new(),
            shards: (0..shards).map(|_| RwLock::new(Node::default())).collect(),
            wildcard: RwLock::new(Node::default()),
            hasher: RandomState::default(),
        }
    }

    fn shard_of(&self, first_level: &str) -> &RwLock<Node> {
        let index = (self.hasher.hash_one(first_level) as usize) % self.shards.len();
        &self.shards[index]
    }

    fn first_level_is_wildcard(filter: &str) -> bool {
        matches!(filter.split('/').next(), Some("+") | Some("#"))
    }

    /// Add a subscription, replacing any the same subscriber held on this
    /// filter. Returns whether it replaced one.
    pub fn insert(&self, filter: &str, sub: Subscription) -> bool {
        if Self::first_level_is_wildcard(filter) {
            return self.wildcard.write().insert(&self.interner, filter, sub);
        }
        let first = filter.split('/').next().unwrap_or("");
        self.shard_of(first)
            .write()
            .insert(&self.interner, filter, sub)
    }

    /// Remove one subscriber's subscription to one filter. Returns whether
    /// anything was removed.
    pub fn remove(&self, filter: &str, subscriber: SubscriberId) -> bool {
        if Self::first_level_is_wildcard(filter) {
            return self
                .wildcard
                .write()
                .remove(&self.interner, filter, subscriber);
        }
        let first = filter.split('/').next().unwrap_or("");
        self.shard_of(first)
            .write()
            .remove(&self.interner, filter, subscriber)
    }

    /// Append every subscription matching `topic` to `out`.
    ///
    /// A topic whose first level begins with a dollar sign skips the
    /// wildcard root entirely, which is how the protocol's rule that a
    /// leading wildcard must not match a dollar topic is enforced.
    ///
    /// # One entry per matching filter, not per subscriber
    ///
    /// A subscriber holding both `a/#` and `a/+` appears twice in the
    /// results for `a/b`, once per filter it matched. That is deliberate:
    /// this answers which subscriptions matched, and only the caller
    /// knows what to do with several from one session.
    ///
    /// MQTT 5 requires that this become a single delivery carrying every
    /// matching subscription identifier, sent at the greatest quality of
    /// service among the matching subscriptions. Nothing here does that,
    /// so it is the caller's job, and a caller that forwards these
    /// straight to a session will duplicate messages.
    pub fn matching(&self, topic: &str, out: &mut Vec<Subscription>) {
        let first = topic.split('/').next().unwrap_or("");
        self.shard_of(first)
            .read()
            .matching(&self.interner, topic, out);

        if !topic.starts_with('$') {
            self.wildcard.read().matching(&self.interner, topic, out);
        }
    }

    /// Remove every subscription belonging to one subscriber. Returns how
    /// many were removed. For session teardown, not for unsubscribe.
    pub fn remove_subscriber(&self, subscriber: SubscriberId) -> usize {
        let mut removed = self.wildcard.write().remove_subscriber(subscriber);
        for shard in self.shards.iter() {
            removed += shard.write().remove_subscriber(subscriber);
        }
        removed
    }

    /// How many subscriptions the trie holds.
    pub fn len(&self) -> usize {
        self.wildcard.read().len()
            + self
                .shards
                .iter()
                .map(|shard| shard.read().len())
                .sum::<usize>()
    }

    /// Whether the trie holds nothing.
    ///
    /// Stops at the first subscription found rather than counting them
    /// all, because the name reads as cheap and callers will treat it
    /// that way. Like [`len`], it takes each lock in turn rather than all
    /// at once, so it is a question about no single instant.
    ///
    /// [`len`]: Self::len
    pub fn is_empty(&self) -> bool {
        !self.wildcard.read().holds_any()
            && !self.shards.iter().any(|shard| shard.read().holds_any())
    }

    /// How many trie nodes exist, across every shard and the wildcard
    /// root, each root included.
    ///
    /// Exposed so that pruning can be tested rather than assumed. [`len`]
    /// counts subscriptions, so a remove that leaves an orphaned empty
    /// branch behind is invisible to it; this notices. A fresh trie of
    /// `shards` shards starts at `shards + 1`.
    ///
    /// [`len`]: Self::len
    pub fn node_count(&self) -> usize {
        self.wildcard.read().node_count()
            + self
                .shards
                .iter()
                .map(|shard| shard.read().node_count())
                .sum::<usize>()
    }

    /// How many distinct topic levels have been interned.
    pub fn interned_levels(&self) -> usize {
        self.interner.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn ids(trie: &SubscriptionTrie, topic: &str) -> Vec<u64> {
        let mut out = Vec::new();
        trie.matching(topic, &mut out);
        let mut ids: Vec<u64> = out.into_iter().map(|s| s.subscriber.0).collect();
        ids.sort_unstable();
        ids
    }

    fn expected(filters: &[&str], topic: &str) -> Vec<u64> {
        let mut ids: Vec<u64> = filters
            .iter()
            .enumerate()
            .filter(|(_, f)| reference::matches(f, topic))
            .map(|(index, _)| index as u64)
            .collect();
        ids.sort_unstable();
        ids
    }

    const FILTERS: &[&str] = &[
        "a",
        "a/b",
        "a/+",
        "a/#",
        "+/b",
        "+",
        "#",
        "+/+/c",
        "$SYS/#",
        "$SYS/broker/+",
        "b/c",
    ];

    fn populated() -> SubscriptionTrie {
        let trie = SubscriptionTrie::new(8);
        for (index, filter) in FILTERS.iter().enumerate() {
            trie.insert(filter, sub(index as u64));
        }
        trie
    }

    #[test]
    fn agrees_with_the_reference_matcher_including_wildcard_roots() {
        let trie = populated();
        for topic in [
            "a",
            "a/b",
            "a/b/c",
            "b/b",
            "b/c",
            "x/y/c",
            "$SYS/broker/uptime",
            "$SYS/broker",
            "$other",
        ] {
            assert_eq!(
                ids(&trie, topic),
                expected(FILTERS, topic),
                "topic {topic:?}"
            );
        }
    }

    #[test]
    fn a_leading_wildcard_never_reaches_a_dollar_topic() {
        let trie = SubscriptionTrie::new(4);
        trie.insert("#", sub(1));
        trie.insert("+/broker/uptime", sub(2));
        trie.insert("$SYS/#", sub(3));

        assert_eq!(ids(&trie, "$SYS/broker/uptime"), vec![3]);
        assert_eq!(ids(&trie, "ordinary/topic"), vec![1]);
    }

    #[test]
    fn counts_subscriptions_across_shards_and_the_wildcard_root() {
        let trie = populated();
        assert_eq!(trie.len(), FILTERS.len());
    }

    #[test]
    fn removing_reports_whether_anything_went() {
        let trie = SubscriptionTrie::new(4);
        trie.insert("a/b", sub(1));
        assert!(trie.remove("a/b", SubscriberId(1)));
        assert!(!trie.remove("a/b", SubscriberId(1)));
        assert_eq!(trie.len(), 0);
    }

    #[test]
    fn removing_a_subscriber_clears_every_shard_and_the_wildcard_root() {
        let trie = SubscriptionTrie::new(4);
        trie.insert("a/b", sub(1));
        trie.insert("q/r", sub(1));
        trie.insert("#", sub(1));
        trie.insert("a/b", sub(2));
        assert_eq!(trie.len(), 4);

        assert_eq!(trie.remove_subscriber(SubscriberId(1)), 3);
        assert_eq!(trie.len(), 1);
        assert_eq!(ids(&trie, "a/b"), vec![2]);
    }

    #[test]
    fn a_single_shard_behaves_the_same_as_many() {
        for shards in [1usize, 2, 8, 64] {
            let trie = SubscriptionTrie::new(shards);
            for (index, filter) in FILTERS.iter().enumerate() {
                trie.insert(filter, sub(index as u64));
            }
            assert_eq!(
                ids(&trie, "a/b"),
                expected(FILTERS, "a/b"),
                "with {shards} shards"
            );
        }
    }

    #[test]
    fn zero_shards_is_treated_as_one() {
        let trie = SubscriptionTrie::new(0);
        trie.insert("a", sub(1));
        assert_eq!(ids(&trie, "a"), vec![1]);
    }
}
