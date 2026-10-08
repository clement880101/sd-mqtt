//! A set of filters, and which of them a topic matches.
//!
//! # Why this exists, and what it is not
//!
//! Section 8.3's routing map asks the same question the subscription
//! trie answers — *which filters does this topic match* — about filters
//! that belong to other nodes rather than to this node's sessions. The
//! trie is the answer to that question and it lives here, in a crate
//! that must not depend on `cluster`.
//!
//! The alternative was a linear scan over every remote filter on every
//! publish. At the design's ceiling of fifty nodes that is fifty nodes'
//! worth of filters compared one at a time against every published
//! topic, which is the cost this trie was built to avoid in the first
//! place: a second answer to one question, and the slow one.
//!
//! So this is [`SubscriptionTrie`] with the subscription taken out of
//! it. What goes in is a filter and an opaque `u64` **slot**; what
//! comes out is the slots whose filters matched. A slot means whatever
//! the caller says it means — `cluster::routes` makes it a filter's row
//! in a table of the nodes that hold it — and nothing here knows or
//! cares.
//!
//! Taking a slot rather than a [`Subscription`] is the whole of the
//! difference, and it is deliberate. A caller outside this crate would
//! otherwise have to name a quality of service, a no-local flag and a
//! retain-as-published flag for a thing that has none of them, and the
//! three would be read by nobody: `SharedSubscriptions` already holds a
//! synthetic entry per group that way and had to say in a comment that
//! its options mean nothing. One field cannot be misread.
//!
//! [`Subscription`]: crate::subscriptions::Subscription

use mqtt_codec::QoS;

use crate::subscriptions::node::{SubscriberId, Subscription};
use crate::subscriptions::trie::SubscriptionTrie;

/// A set of filters, each standing for a caller's `u64` slot.
///
/// The same matching the subscription trie does, including the rule that
/// a leading wildcard never reaches a dollar topic, because it **is**
/// the subscription trie: one instance, one filter per slot. Anything
/// this says about which filters match a topic is therefore the same
/// answer the local matching layer would give, which is the point —
/// a routing map that disagreed with the trie about `a/+` would forward
/// to a node whose own matching then delivered nothing, or worse.
pub struct FilterIndex {
    trie: SubscriptionTrie,
}

/// How many pairs it holds, and nothing about which.
///
/// Written by hand because [`SubscriptionTrie`] has no `Debug` of its
/// own: a derive would have to print every filter in the trie, which on
/// a routing map is every filter in the cluster. A caller holding one of
/// these inside a `#[derive(Debug)]` type of its own needs this to
/// exist and does not need it to be long.
impl std::fmt::Debug for FilterIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterIndex")
            .field("filters", &self.len())
            .finish_non_exhaustive()
    }
}

impl FilterIndex {
    /// An empty index over a trie with `shards` shards.
    pub fn new(shards: usize) -> Self {
        Self {
            trie: SubscriptionTrie::new(shards),
        }
    }

    /// Hold `filter` for `slot`. Returns whether it replaced one.
    ///
    /// One entry per filter and slot, so inserting the same pair twice
    /// is one entry: the trie's own rule that re-subscribing replaces
    /// rather than duplicates, which is what keeps a caller from having
    /// to remember whether it has already said this.
    pub fn insert(&self, filter: &str, slot: u64) -> bool {
        self.trie.insert(filter, Self::entry(slot))
    }

    /// Stop holding `filter` for `slot`. Returns whether anything went.
    pub fn remove(&self, filter: &str, slot: u64) -> bool {
        self.trie.remove(filter, SubscriberId(slot))
    }

    /// Append the slot of every filter matching `topic` to `out`.
    ///
    /// Appends rather than clears, as the trie does, so a caller may
    /// gather from several indexes into one buffer and reuse it.
    ///
    /// A slot appears once per matching filter it holds, so a caller
    /// that put two filters in one slot sees it twice. Which of those
    /// matched is not something this can say, and a caller that cares
    /// uses a slot per filter.
    pub fn matching(&self, topic: &str, out: &mut Vec<u64>) {
        let mut matched = Vec::new();
        self.trie.matching(topic, &mut matched);
        out.extend(matched.into_iter().map(|sub| sub.subscriber.0));
    }

    /// How many filter-and-slot pairs this holds.
    pub fn len(&self) -> usize {
        self.trie.len()
    }

    /// Whether it holds nothing.
    pub fn is_empty(&self) -> bool {
        self.trie.is_empty()
    }

    /// The entry one slot is held as.
    ///
    /// The options are spelled out as defaults rather than taken from a
    /// caller, because nothing reads them: [`Self::matching`] returns
    /// the slot and throws the rest away.
    fn entry(slot: u64) -> Subscription {
        Subscription {
            subscriber: SubscriberId(slot),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            id: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::reference;

    fn slots(index: &FilterIndex, topic: &str) -> Vec<u64> {
        let mut out = Vec::new();
        index.matching(topic, &mut out);
        out.sort_unstable();
        out
    }

    const FILTERS: &[&str] = &[
        "a", "a/b", "a/+", "a/#", "+/b", "#", "+/+/c", "$SYS/#", "b/c",
    ];

    fn populated() -> FilterIndex {
        let index = FilterIndex::new(4);
        for (at, filter) in FILTERS.iter().enumerate() {
            index.insert(filter, at as u64);
        }
        index
    }

    #[test]
    fn it_agrees_with_the_reference_matcher() {
        // The same property the trie's own tests assert, asserted again
        // through this door: the whole value of reusing the trie is
        // that the routing map and the local matching layer cannot
        // disagree about what `a/+` means.
        let index = populated();
        for topic in ["a", "a/b", "a/b/c", "b/b", "b/c", "x/y/c", "$SYS/broker"] {
            let expected: Vec<u64> = FILTERS
                .iter()
                .enumerate()
                .filter(|(_, filter)| reference::matches(filter, topic))
                .map(|(at, _)| at as u64)
                .collect();
            assert_eq!(slots(&index, topic), expected, "topic {topic:?}");
        }
    }

    #[test]
    fn a_wildcard_filter_is_not_matched_as_a_literal() {
        // The mutation this guards: a map that compared filters for
        // equality would answer nothing here, and would route a
        // publication on `sensor/1/temp` to nobody while a node
        // subscribed to `sensor/+/temp` waited.
        let index = FilterIndex::new(4);
        index.insert("sensor/+/temp", 7);
        assert_eq!(slots(&index, "sensor/1/temp"), vec![7]);
        assert_eq!(slots(&index, "sensor/+/temp"), vec![7], "and the literal");
        assert!(slots(&index, "sensor/1/humidity").is_empty());
    }

    #[test]
    fn removing_a_slot_leaves_the_others_holding_the_filter() {
        let index = FilterIndex::new(4);
        index.insert("a/b", 1);
        index.insert("a/b", 2);
        assert!(index.remove("a/b", 1));
        assert_eq!(slots(&index, "a/b"), vec![2]);
        assert!(!index.remove("a/b", 1), "it has already gone");
    }

    #[test]
    fn the_same_pair_twice_is_one_entry() {
        let index = FilterIndex::new(4);
        assert!(!index.insert("a/b", 1), "nothing to replace the first time");
        assert!(index.insert("a/b", 1), "and a replacement the second");
        assert_eq!(index.len(), 1);
        assert_eq!(slots(&index, "a/b"), vec![1]);
    }

    #[test]
    fn an_empty_index_matches_nothing() {
        let index = FilterIndex::new(4);
        assert!(index.is_empty());
        assert!(slots(&index, "a/b").is_empty());
    }
}
