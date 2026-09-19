//! The fast structures must agree with the naive matcher, always.
//!
//! A trie is easy to get subtly wrong and hard to review by reading, so
//! correctness here is established by comparison rather than by inspection:
//! for generated filters and topics, the sharded trie and the retained
//! store must return exactly what `reference::matches` says they should. A
//! bug becomes a shrunk counterexample instead of a delivery fault found in
//! production.

use broker_core::retained::{RetainedMessage, RetainedStore};
use broker_core::subscriptions::{SubscriberId, Subscription, SubscriptionTrie};
use broker_core::topic::reference;
use bytes::Bytes;
use mqtt_codec::QoS;
use proptest::prelude::*;

fn sub(id: u64) -> Subscription {
    Subscription {
        subscriber: SubscriberId(id),
        qos: QoS::AtMostOnce,
        no_local: false,
        retain_as_published: false,
        id: None,
    }
}

/// A small alphabet, so that collisions and shared prefixes are common
/// rather than vanishingly rare. Empty levels are legal and included.
fn any_level() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => "[ab]",
        2 => "[ab]{2}",
        1 => Just(String::new()),
    ]
}

fn any_filter() -> impl Strategy<Value = String> {
    (
        prop::collection::vec(
            prop_oneof![3 => any_level(), 1 => Just("+".to_string())],
            1..4,
        ),
        prop::bool::weighted(0.25),
        prop::bool::weighted(0.15),
    )
        .prop_map(|(mut levels, trailing_hash, dollar_root)| {
            if dollar_root {
                levels[0] = "$SYS".to_string();
            }
            if trailing_hash {
                levels.push("#".to_string());
            }
            levels.join("/")
        })
}

fn any_topic() -> impl Strategy<Value = String> {
    (
        prop::collection::vec(any_level(), 1..5),
        prop::bool::weighted(0.15),
    )
        .prop_map(|(mut levels, dollar_root)| {
            if dollar_root {
                levels[0] = "$SYS".to_string();
            }
            levels.join("/")
        })
}

fn oracle(filters: &[String], topic: &str) -> Vec<u64> {
    let mut ids: Vec<u64> = filters
        .iter()
        .enumerate()
        .filter(|(_, filter)| reference::matches(filter, topic))
        .map(|(index, _)| index as u64)
        .collect();
    ids.sort_unstable();
    ids
}

proptest! {
    /// The core claim: the sharded trie and the naive matcher never disagree.
    #[test]
    fn the_trie_agrees_with_the_reference_matcher(
        filters in prop::collection::vec(any_filter(), 1..12),
        topics in prop::collection::vec(any_topic(), 1..12),
        shards in 1usize..8,
    ) {
        let trie = SubscriptionTrie::new(shards);
        for (index, filter) in filters.iter().enumerate() {
            trie.insert(filter, sub(index as u64));
        }

        for topic in &topics {
            let mut matched = Vec::new();
            trie.matching(topic, &mut matched);
            let mut got: Vec<u64> = matched.into_iter().map(|s| s.subscriber.0).collect();
            got.sort_unstable();

            prop_assert_eq!(got, oracle(&filters, topic), "topic {:?}", topic);
        }
    }

    /// Whatever was inserted can be removed, leaving nothing behind. This
    /// catches pruning bugs that leave orphaned nodes holding subscriptions.
    #[test]
    fn inserting_then_removing_everything_empties_the_trie(
        filters in prop::collection::vec(any_filter(), 1..12),
    ) {
        let trie = SubscriptionTrie::new(4);
        for (index, filter) in filters.iter().enumerate() {
            trie.insert(filter, sub(index as u64));
        }
        for (index, filter) in filters.iter().enumerate() {
            trie.remove(filter, SubscriberId(index as u64));
        }
        prop_assert_eq!(trie.len(), 0);
    }

    /// Tearing down one session must not disturb another's subscriptions.
    #[test]
    fn removing_one_subscriber_leaves_the_others_intact(
        filters in prop::collection::vec(any_filter(), 1..8),
        topics in prop::collection::vec(any_topic(), 1..8),
    ) {
        let trie = SubscriptionTrie::new(4);
        // Two subscribers hold identical interests.
        for filter in &filters {
            trie.insert(filter, sub(1));
            trie.insert(filter, sub(2));
        }

        trie.remove_subscriber(SubscriberId(1));

        for topic in &topics {
            let mut matched = Vec::new();
            trie.matching(topic, &mut matched);
            let survivors: Vec<u64> = matched.into_iter().map(|s| s.subscriber.0).collect();
            prop_assert!(
                survivors.iter().all(|&id| id == 2),
                "subscriber 1 survived for {:?}",
                topic
            );

            let expected_any = filters.iter().any(|f| reference::matches(f, topic));
            prop_assert_eq!(!survivors.is_empty(), expected_any, "topic {:?}", topic);
        }
    }

    /// Insert order must not change what matches.
    #[test]
    fn matching_does_not_depend_on_insertion_order(
        filters in prop::collection::vec(any_filter(), 2..8),
        topic in any_topic(),
    ) {
        let forward = SubscriptionTrie::new(4);
        for (index, filter) in filters.iter().enumerate() {
            forward.insert(filter, sub(index as u64));
        }
        let backward = SubscriptionTrie::new(4);
        for (index, filter) in filters.iter().enumerate().rev() {
            backward.insert(filter, sub(index as u64));
        }

        let mut a = Vec::new();
        forward.matching(&topic, &mut a);
        let mut b = Vec::new();
        backward.matching(&topic, &mut b);

        let mut a: Vec<u64> = a.into_iter().map(|s| s.subscriber.0).collect();
        let mut b: Vec<u64> = b.into_iter().map(|s| s.subscriber.0).collect();
        a.sort_unstable();
        b.sort_unstable();
        prop_assert_eq!(a, b);
    }

    /// The retained store runs the traversal the other way round, so it
    /// gets its own comparison against the same oracle.
    #[test]
    fn the_retained_store_agrees_with_the_reference_matcher(
        stored in prop::collection::vec(any_topic(), 1..12),
        filters in prop::collection::vec(any_filter(), 1..12),
    ) {
        let store = RetainedStore::new();
        for topic in &stored {
            store.set(
                topic,
                RetainedMessage {
                    payload: Bytes::from_static(b"v"),
                    qos: QoS::AtMostOnce,
                    expires_at: None,
                },
            );
        }

        for filter in &filters {
            let mut matched = Vec::new();
            store.matching(filter, &mut matched);
            let mut got: Vec<String> = matched.into_iter().map(|(topic, _)| topic).collect();
            got.sort();
            got.dedup();

            let mut want: Vec<String> = stored
                .iter()
                .filter(|topic| reference::matches(filter, topic))
                .cloned()
                .collect();
            want.sort();
            want.dedup();

            prop_assert_eq!(got, want, "filter {:?}", filter);
        }
    }
}
