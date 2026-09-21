//! The fast structures must agree with the naive matcher, always.
//!
//! A trie is easy to get subtly wrong and hard to review by reading, so
//! correctness here is established by comparison rather than by inspection:
//! for generated filters and topics, the sharded trie and the retained
//! store must return exactly what `reference::matches` says they should. A
//! bug becomes a shrunk counterexample instead of a delivery fault found in
//! production.

use std::collections::BTreeMap;

use broker_core::retained::{RetainedMessage, RetainedStore};
use broker_core::subscriptions::{
    SharedSubscriptions, SubscriberId, Subscription, SubscriptionTrie,
};
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

/// Levels beginning with a dollar sign.
///
/// `$SYS` is the conventional one, but the protocol's rule is about the
/// sign rather than the name, so an implementation that special-cased the
/// string `$SYS` would be wrong and these catch it.
fn any_dollar_level() -> impl Strategy<Value = String> {
    prop_oneof![
        2 => Just("$SYS".to_string()),
        1 => Just("$share".to_string()),
        1 => Just("$a".to_string()),
    ]
}

/// A small alphabet, so that collisions and shared prefixes are common
/// rather than vanishingly rare. Empty levels are legal and included, and
/// so are dollar levels, which below the first position are ordinary
/// levels and must not be treated otherwise.
fn any_level() -> impl Strategy<Value = String> {
    prop_oneof![
        8 => "[ab]",
        4 => "[ab]{2}",
        2 => Just(String::new()),
        1 => any_dollar_level(),
    ]
}

fn any_filter() -> impl Strategy<Value = String> {
    prop_oneof![
        // A bare multi-level wildcard is the subscribe-to-everything
        // filter, and the structured strategy below cannot produce one:
        // it always joins `#` onto at least one preceding level. Some
        // code is reachable only through it, notably the retained store's
        // guard against a leading wildcard reaching a dollar topic, so it
        // gets its own arm rather than being left to chance.
        1 => Just("#".to_string()),
        1 => Just("+".to_string()),
        8 => structured_filter(),
    ]
}

fn structured_filter() -> impl Strategy<Value = String> {
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

/// One mutating call on [`SubscriptionTrie`].
#[derive(Debug, Clone)]
enum TrieOp {
    Insert { filter: String, subscriber: u64 },
    Remove { filter: String, subscriber: u64 },
    RemoveSubscriber { subscriber: u64 },
}

/// A small space of subscribers, so that a sequence collides on the same
/// filter and the same subscriber often rather than by luck. A replacement
/// and a remove of something absent are the two cases the count gets wrong
/// if it is adjusted before the structure is asked what actually happened.
fn any_trie_op() -> impl Strategy<Value = TrieOp> {
    prop_oneof![
        6 => (any_filter(), 0u64..4)
            .prop_map(|(filter, subscriber)| TrieOp::Insert { filter, subscriber }),
        3 => (any_filter(), 0u64..4)
            .prop_map(|(filter, subscriber)| TrieOp::Remove { filter, subscriber }),
        1 => (0u64..4).prop_map(|subscriber| TrieOp::RemoveSubscriber { subscriber }),
    ]
}

/// One mutating call on [`SharedSubscriptions`].
#[derive(Debug, Clone)]
enum Op {
    Insert {
        group: String,
        filter: String,
        subscriber: u64,
    },
    Remove {
        group: String,
        filter: String,
        subscriber: u64,
    },
    RemoveSubscriber {
        subscriber: u64,
    },
}

/// A tiny space of group names and subscribers, so that sequences collide
/// on the same group and the same member often rather than by luck.
fn any_op() -> impl Strategy<Value = Op> {
    let group = prop_oneof![Just("g".to_string()), Just("h".to_string())];
    prop_oneof![
        6 => (group.clone(), any_filter(), 0u64..4)
            .prop_map(|(group, filter, subscriber)| Op::Insert { group, filter, subscriber }),
        3 => (group, any_filter(), 0u64..4)
            .prop_map(|(group, filter, subscriber)| Op::Remove { group, filter, subscriber }),
        1 => (0u64..4).prop_map(|subscriber| Op::RemoveSubscriber { subscriber }),
    ]
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

    /// Whatever was inserted can be removed, leaving nothing behind.
    ///
    /// `len` counts subscriptions, so on its own it would pass for a trie
    /// that had pruned nothing and merely emptied its leaves. The node
    /// count has to return to its baseline too, which is what actually
    /// says the branches went.
    #[test]
    fn inserting_then_removing_everything_empties_the_trie(
        filters in prop::collection::vec(any_filter(), 1..12),
    ) {
        let trie = SubscriptionTrie::new(4);
        let baseline = trie.node_count();

        for (index, filter) in filters.iter().enumerate() {
            trie.insert(filter, sub(index as u64));
        }
        for (index, filter) in filters.iter().enumerate() {
            trie.remove(filter, SubscriberId(index as u64));
        }
        prop_assert_eq!(trie.len(), 0);
        prop_assert_eq!(trie.node_count(), baseline, "an empty branch was left behind");
    }

    /// The maintained subscription count, against the same number by
    /// walking the shards.
    ///
    /// `len` is section 11.3's subscriptions gauge and is read once a
    /// second by the events sampler and again on every metrics scrape.
    /// It used to add up every shard, which is a walk of the trie
    /// measured at 2.64 milliseconds with a hundred thousand
    /// subscriptions; it is now a counter adjusted at three places.
    /// That is the shape that drifts the first time a path forgets
    /// one, and a wrong gauge is silent — every other answer the trie
    /// gives is still right.
    ///
    /// The two cases to get wrong are here on purpose: an insert that
    /// replaces a subscription the same subscriber already held, which
    /// must not add, and a remove of something that was not there,
    /// which must not subtract. A count adjusted before asking the
    /// structure what actually happened gets both wrong, and neither
    /// shows up in a matching assertion.
    #[test]
    fn the_maintained_subscription_count_agrees_with_a_walk(
        ops in prop::collection::vec(any_trie_op(), 1..40),
        shards in 1usize..8,
    ) {
        let trie = SubscriptionTrie::new(shards);
        for (index, op) in ops.iter().enumerate() {
            match op {
                TrieOp::Insert { filter, subscriber } => {
                    trie.insert(filter, sub(*subscriber));
                }
                TrieOp::Remove { filter, subscriber } => {
                    trie.remove(filter, SubscriberId(*subscriber));
                }
                TrieOp::RemoveSubscriber { subscriber } => {
                    trie.remove_subscriber(SubscriberId(*subscriber));
                }
            }
            prop_assert_eq!(
                trie.len(),
                trie.len_by_walking(),
                "after operation {} ({:?})",
                index,
                op
            );
            prop_assert_eq!(trie.is_empty(), trie.is_empty_by_walking());
        }
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
                    properties: Default::default(),
                },
            );
        }

        for filter in &filters {
            let mut matched = Vec::new();
            store.matching(filter, 0, &mut matched);
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

    /// Shared subscriptions against a map model.
    ///
    /// The structure keeps two indexes and a synthetic trie entry per
    /// group in step with one another across three mutating operations,
    /// which is the kind of bookkeeping that drifts. The model is a plain
    /// map doing the obvious thing, so any drift shows up as a count that
    /// disagrees or a group that stops matching.
    #[test]
    fn shared_subscriptions_agree_with_a_map_model(
        ops in prop::collection::vec(any_op(), 1..40),
        topics in prop::collection::vec(any_topic(), 1..6),
    ) {
        let shared = SharedSubscriptions::new(4);
        let mut model: BTreeMap<(String, String), Vec<u64>> = BTreeMap::new();

        for op in &ops {
            match op {
                Op::Insert { group, filter, subscriber } => {
                    let got = shared.insert(group, filter, sub(*subscriber));

                    let members = model.entry((group.clone(), filter.clone())).or_default();
                    let want = members.contains(subscriber);
                    if !want {
                        members.push(*subscriber);
                    }
                    prop_assert_eq!(got, want, "insert reported the wrong replacement");
                }
                Op::Remove { group, filter, subscriber } => {
                    let got = shared.remove(group, filter, SubscriberId(*subscriber));

                    let key = (group.clone(), filter.clone());
                    let mut want = false;
                    if let Some(members) = model.get_mut(&key) {
                        let before = members.len();
                        members.retain(|m| m != subscriber);
                        want = members.len() != before;
                        if members.is_empty() {
                            model.remove(&key);
                        }
                    }
                    prop_assert_eq!(got, want, "remove reported the wrong outcome");
                }
                Op::RemoveSubscriber { subscriber } => {
                    let got = shared.remove_subscriber(SubscriberId(*subscriber));

                    let mut want = 0;
                    model.retain(|_, members| {
                        let before = members.len();
                        members.retain(|m| m != subscriber);
                        want += before - members.len();
                        !members.is_empty()
                    });
                    prop_assert_eq!(got, want, "remove_subscriber counted wrong");
                }
            }

            prop_assert_eq!(shared.group_count(), model.len());
            prop_assert_eq!(
                shared.member_count(),
                model.values().map(Vec::len).sum::<usize>()
            );
            prop_assert_eq!(shared.is_empty(), model.is_empty());

            // One synthetic trie entry per live group, always. This is
            // what makes matching yield groups directly, and a stale
            // entry is invisible to every other assertion here:
            // `next_member` answers nothing for a synthetic whose
            // group is gone, so the leak shows
            // up in nothing but the count itself.
            prop_assert_eq!(
                shared.synthetic_count(),
                shared.group_count(),
                "a synthetic entry outlived its group, or a group lost one"
            );
        }

        for topic in &topics {
            let wanted: Vec<&(String, String)> = model
                .keys()
                .filter(|(_, filter)| reference::matches(filter, topic))
                .collect();

            // One member from each matching group, the way a caller
            // with room for the message everywhere would take them.
            let mut groups = Vec::new();
            shared.matching(topic, &mut groups);
            let picked: Vec<u64> = groups
                .iter()
                .filter_map(|group| shared.next_member(*group))
                .map(|s| s.subscriber.0)
                .collect();

            // Exactly one member per matching group, no more and no less.
            prop_assert_eq!(picked.len(), wanted.len(), "topic {:?}", topic);

            // Every pick belongs to a group that matched, and every group
            // that matched is represented by some pick. Together with the
            // count, that is one member each.
            for id in &picked {
                prop_assert!(
                    wanted.iter().any(|key| model[*key].contains(id)),
                    "picked {} for {:?}, which no matching group holds",
                    id,
                    topic
                );
            }
            for key in &wanted {
                prop_assert!(
                    picked.iter().any(|id| model[*key].contains(id)),
                    "group {:?} matched {:?} but nothing was picked from it",
                    key,
                    topic
                );
            }
        }
    }
}
