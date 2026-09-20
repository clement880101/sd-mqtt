//! Turning one publication into deliveries.

use mqtt_codec::QoS;

use crate::session::message::Message;
use crate::subscriptions::shared::GroupId;
use crate::subscriptions::{SharedSubscriptions, SubscriberId, Subscription, SubscriptionTrie};

/// One message addressed to one subscriber.
#[derive(Debug, Clone)]
pub struct Routed {
    /// Who receives it.
    pub subscriber: SubscriberId,
    /// At what level, the lesser of published and granted.
    pub qos: QoS,
    /// The retain flag as it goes on the wire.
    pub retain: bool,
    /// Every matching subscription's identifier, for MQTT 5.
    ///
    /// One entry per matching subscription that carried one, which means
    /// repeats when a client used the same identifier for several
    /// overlapping filters. Section 3.3.4 says so explicitly.
    pub subscription_ids: Vec<u32>,
}

/// Turn one publication into deliveries.
///
/// A subscriber holding several matching filters receives **one**
/// message, at the highest level any of them granted, carrying every
/// matching subscription identifier. The matching layer returns one entry
/// per subscription, so merging them is this function's job.
/// [MQTT-3.3.4-2] requires the single delivery to respect the maximum
/// quality of service of all the matching subscriptions, and
/// [MQTT-3.3.4-4] requires it to carry the identifiers of all of them.
///
/// `publisher` withholds the message from any subscription that set
/// no-local, which is about who sent it rather than what it says.
///
/// `has_room` answers whether one subscriber could take this message,
/// and is asked of a shared group's members alone. Section 6: "A member
/// whose queue is full is skipped and the next is tried." The selector
/// cannot see a session, so the asking is done here, and bounded: one
/// turn of the rotation, no more. A group whose every member is full
/// still hands the message to one of them, so that the drop is reported
/// where every other drop is, rather than vanishing here uncounted.
///
/// An ordinary subscription is not asked. Its subscriber is the only
/// one there is, so a full queue there is a drop however it is reached.
pub fn route(
    trie: &SubscriptionTrie,
    shared: &SharedSubscriptions,
    message: &Message,
    publisher: Option<SubscriberId>,
    has_room: &mut dyn FnMut(SubscriberId) -> bool,
    out: &mut Vec<Routed>,
) {
    let mut matched = Vec::new();
    trie.matching(&message.topic[..], &mut matched);

    for subscription in matched {
        if is_withheld(&subscription, publisher) {
            continue;
        }
        merge(out, &subscription, message);
    }

    // A shared group contributes one member, and that member is merged
    // like any other: a client holding both an ordinary and a shared
    // subscription to the same topic still receives one message.
    let mut groups = Vec::new();
    shared.matching(&message.topic[..], &mut groups);
    for group in groups {
        if let Some(subscription) = choose(shared, group, publisher, has_room) {
            merge(out, &subscription, message);
        }
    }
}

/// Pick the member of `group` that will carry this message.
///
/// Asks for a member, tries it, and asks again if it cannot take the
/// message, up to the size of the group. The bound is what keeps a
/// saturated group from becoming a search: every member having refused
/// is an answer, not a reason to go round again.
///
/// When nobody can take it, the member whose turn it was carries it
/// anyway and its queue refuses it. That is deliberate: a dropped
/// message must be reported, and the session's own drop is the only
/// thing that reports one. Returning `None` here would lose it in
/// silence.
fn choose(
    shared: &SharedSubscriptions,
    group: GroupId,
    publisher: Option<SubscriberId>,
    has_room: &mut dyn FnMut(SubscriberId) -> bool,
) -> Option<Subscription> {
    let size = shared.size_of(group);
    let mut turn_holder = None;

    for _ in 0..size {
        let candidate = shared.next_member(group)?;
        // Unreachable in practice: [MQTT-3.8.3-4] forbids no-local on a
        // shared subscription and the codec refuses to decode one. Kept
        // because the rule belongs to whoever reads a subscription's
        // options, and a withheld member is not a candidate for the
        // drop either.
        if is_withheld(&candidate, publisher) {
            continue;
        }
        if has_room(candidate.subscriber) {
            return Some(candidate);
        }
        turn_holder.get_or_insert(candidate);
    }

    turn_holder
}

fn is_withheld(subscription: &Subscription, publisher: Option<SubscriberId>) -> bool {
    subscription.no_local && publisher == Some(subscription.subscriber)
}

fn merge(out: &mut Vec<Routed>, subscription: &Subscription, message: &Message) {
    // Capped by what was published before the maximum is taken across
    // subscriptions, [MQTT-3.8.4-8]: a subscription granted exactly-once
    // must not raise a message published at-least-once.
    let qos = subscription.qos.min(message.qos);
    let retain = subscription.retain_as_published && message.retain;

    if let Some(existing) = out
        .iter_mut()
        .find(|r| r.subscriber == subscription.subscriber)
    {
        existing.qos = existing.qos.max(qos);
        // Retain-as-published on any matching subscription is enough:
        // the flag describes the message, and withholding it would lose
        // information the subscriber asked for.
        existing.retain |= retain;
        // Appended without checking for a repeat. Section 3.3.4: a client
        // that used one identifier for several overlapping filters gets
        // "multiple identical Subscription Identifiers", because the count
        // is how it learns how many of its subscriptions matched.
        existing.subscription_ids.extend(subscription.id);
        return;
    }

    out.push(Routed {
        subscriber: subscription.subscriber,
        qos,
        retain,
        subscription_ids: subscription.id.into_iter().collect(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::message::Message;
    use crate::subscriptions::{SharedSubscriptions, SubscriberId, Subscription, SubscriptionTrie};
    use bytes::Bytes;
    use mqtt_codec::QoS;

    fn sub(id: u64, qos: QoS) -> Subscription {
        Subscription {
            subscriber: SubscriberId(id),
            qos,
            no_local: false,
            retain_as_published: false,
            id: None,
        }
    }

    fn message(topic: &str, qos: QoS) -> Message {
        Message::new(topic, Bytes::from_static(b"x"), qos, false)
    }

    /// Route with every subscriber able to take the message.
    ///
    /// These tests are about matching and merging rather than about a
    /// queue's capacity, and a subscriber with room is the ordinary
    /// case. The tests that are about capacity pass their own answer.
    fn route_all(
        trie: &SubscriptionTrie,
        shared: &SharedSubscriptions,
        message: &Message,
        publisher: Option<SubscriberId>,
        out: &mut Vec<Routed>,
    ) {
        route(trie, shared, message, publisher, &mut |_| true, out);
    }

    fn targets(out: &[Routed]) -> Vec<u64> {
        let mut ids: Vec<u64> = out.iter().map(|r| r.subscriber.0).collect();
        ids.sort_unstable();
        ids
    }

    /// The group this topic matches, which every shared test wants.
    fn group_of(shared: &SharedSubscriptions, topic: &str) -> crate::subscriptions::GroupId {
        let mut groups = Vec::new();
        shared.matching(topic, &mut groups);
        let [group] = groups[..] else {
            panic!("one group matches {topic}, got {}", groups.len());
        };
        group
    }

    #[test]
    fn a_shared_member_that_cannot_take_the_message_is_skipped_for_the_next() {
        // Section 6: "A member whose queue is full is skipped and the
        // next is tried." Subscriber 3 holds an ordinary subscription
        // and refuses too, to pin the other half of the rule: an
        // ordinary subscriber is the only one there is, so it is never
        // asked and never skipped. Skipping it would lose the message
        // with nothing reporting the loss.
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert("a/b", sub(3, QoS::AtLeastOnce));
        shared.insert("g", "a/b", sub(1, QoS::AtLeastOnce));
        shared.insert("g", "a/b", sub(2, QoS::AtLeastOnce));
        assert_eq!(
            group_of(&shared, "a/b"),
            group_of(&shared, "a/b"),
            "the two are one group, so the rotation is one rotation"
        );

        let mut out = Vec::new();
        route(
            &trie,
            &shared,
            &message("a/b", QoS::AtLeastOnce),
            None,
            &mut |subscriber| subscriber == SubscriberId(2),
            &mut out,
        );
        assert_eq!(
            targets(&out),
            vec![2, 3],
            "the member with room takes it, and the ordinary subscriber is not asked"
        );
    }

    #[test]
    fn a_shared_group_is_asked_once_per_member_and_no_more() {
        // The bound, which is what keeps the skip from becoming a
        // spin. Without it the rotation goes round for ever on a group
        // where nobody can take the message; the panic inside the
        // answer turns that hang into a failure.
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1, QoS::AtLeastOnce));
        shared.insert("g", "a/b", sub(2, QoS::AtLeastOnce));

        let mut asked: Vec<u64> = Vec::new();
        let mut out = Vec::new();
        route(
            &trie,
            &shared,
            &message("a/b", QoS::AtLeastOnce),
            None,
            &mut |subscriber| {
                asked.push(subscriber.0);
                assert!(
                    asked.len() <= 2,
                    "a group of two is asked twice at most, and was asked {asked:?}"
                );
                false
            },
            &mut out,
        );

        assert_eq!(asked, vec![1, 2], "each member once, in rotation order");
        assert_eq!(
            targets(&out),
            vec![1],
            "the member whose turn it was carries it, so its queue reports the drop"
        );
    }

    #[test]
    fn a_message_reaches_every_matching_subscriber() {
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert("a/+", sub(1, QoS::AtLeastOnce));
        trie.insert("a/b", sub(2, QoS::AtLeastOnce));
        trie.insert("z/#", sub(3, QoS::AtLeastOnce));

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a/b", QoS::AtLeastOnce),
            None,
            &mut out,
        );
        assert_eq!(targets(&out), vec![1, 2]);
    }

    #[test]
    fn a_subscriber_matching_twice_receives_one_message() {
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert("a/+", sub(1, QoS::AtMostOnce));
        trie.insert("a/b", sub(1, QoS::ExactlyOnce));

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a/b", QoS::ExactlyOnce),
            None,
            &mut out,
        );
        assert_eq!(out.len(), 1, "one delivery, not two");
        assert_eq!(out[0].qos, QoS::ExactlyOnce, "at the highest granted level");
    }

    #[test]
    fn a_merged_delivery_carries_every_matching_subscription_identifier() {
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert(
            "a/+",
            Subscription {
                id: Some(7),
                ..sub(1, QoS::AtMostOnce)
            },
        );
        trie.insert(
            "a/b",
            Subscription {
                id: Some(9),
                ..sub(1, QoS::AtMostOnce)
            },
        );

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a/b", QoS::AtMostOnce),
            None,
            &mut out,
        );
        let mut ids = out[0].subscription_ids.clone();
        ids.sort_unstable();
        assert_eq!(ids, vec![7, 9]);
    }

    #[test]
    fn two_overlapping_filters_sharing_an_identifier_carry_it_twice() {
        // One SUBSCRIBE packet gives every filter in it the same
        // identifier, so this is the ordinary case rather than a corner.
        // Section 3.3.4: "It is possible that the Client made several
        // subscriptions which match a publication and that it used the
        // same identifier for more than one of them. In this case the
        // PUBLISH packet will carry multiple identical Subscription
        // Identifiers." De-duplicating would tell the client one
        // subscription matched when two did.
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert(
            "a/+",
            Subscription {
                id: Some(5),
                ..sub(1, QoS::AtMostOnce)
            },
        );
        trie.insert(
            "a/b",
            Subscription {
                id: Some(5),
                ..sub(1, QoS::AtMostOnce)
            },
        );

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a/b", QoS::AtMostOnce),
            None,
            &mut out,
        );
        assert_eq!(out.len(), 1, "still one delivery");
        assert_eq!(out[0].subscription_ids, vec![5, 5]);
    }

    #[test]
    fn a_subscription_without_an_identifier_contributes_none() {
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert(
            "a/+",
            Subscription {
                id: Some(3),
                ..sub(1, QoS::AtMostOnce)
            },
        );
        trie.insert("a/b", sub(1, QoS::AtMostOnce));

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a/b", QoS::AtMostOnce),
            None,
            &mut out,
        );
        assert_eq!(out[0].subscription_ids, vec![3]);
    }

    #[test]
    fn the_delivery_level_is_the_lesser_of_published_and_granted() {
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert("a", sub(1, QoS::AtMostOnce));
        trie.insert("a", sub(2, QoS::ExactlyOnce));

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a", QoS::AtLeastOnce),
            None,
            &mut out,
        );
        let levels: Vec<QoS> = {
            let mut v: Vec<(u64, QoS)> = out.iter().map(|r| (r.subscriber.0, r.qos)).collect();
            v.sort_by_key(|(id, _)| *id);
            v.into_iter().map(|(_, q)| q).collect()
        };
        assert_eq!(levels, vec![QoS::AtMostOnce, QoS::AtLeastOnce]);
    }

    #[test]
    fn a_merged_level_is_capped_by_what_was_published() {
        // [MQTT-3.8.4-8] caps each subscription at the published level
        // before [MQTT-3.3.4-2] takes the maximum across them. Taking the
        // maximum of the granted levels first would deliver a QoS 1
        // message at QoS 2.
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert("a/+", sub(1, QoS::AtMostOnce));
        trie.insert("a/b", sub(1, QoS::ExactlyOnce));

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a/b", QoS::AtLeastOnce),
            None,
            &mut out,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].qos, QoS::AtLeastOnce);
    }

    #[test]
    fn no_local_withholds_a_publisher_from_its_own_message() {
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert(
            "a",
            Subscription {
                no_local: true,
                ..sub(1, QoS::AtMostOnce)
            },
        );
        trie.insert("a", sub(2, QoS::AtMostOnce));

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a", QoS::AtMostOnce),
            Some(SubscriberId(1)),
            &mut out,
        );
        assert_eq!(targets(&out), vec![2]);
    }

    #[test]
    fn no_local_does_not_withhold_from_anyone_else() {
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert(
            "a",
            Subscription {
                no_local: true,
                ..sub(1, QoS::AtMostOnce)
            },
        );

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a", QoS::AtMostOnce),
            Some(SubscriberId(2)),
            &mut out,
        );
        assert_eq!(targets(&out), vec![1]);
    }

    #[test]
    fn no_local_withholds_only_the_subscription_that_set_it() {
        // [MQTT-3.8.3-3] is per subscription, so a publisher holding a
        // plain overlapping filter still receives its own message
        // through that one.
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert(
            "a/+",
            Subscription {
                no_local: true,
                ..sub(1, QoS::AtMostOnce)
            },
        );
        trie.insert("a/b", sub(1, QoS::AtMostOnce));

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a/b", QoS::AtMostOnce),
            Some(SubscriberId(1)),
            &mut out,
        );
        assert_eq!(targets(&out), vec![1]);
    }

    #[test]
    fn retain_as_published_decides_the_flag_on_the_wire() {
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert(
            "a",
            Subscription {
                retain_as_published: true,
                ..sub(1, QoS::AtMostOnce)
            },
        );
        trie.insert("a", sub(2, QoS::AtMostOnce));

        let mut retained = message("a", QoS::AtMostOnce);
        retained.retain = true;

        let mut out = Vec::new();
        route_all(&trie, &shared, &retained, None, &mut out);
        let mut flags: Vec<(u64, bool)> = out.iter().map(|r| (r.subscriber.0, r.retain)).collect();
        flags.sort_by_key(|(id, _)| *id);
        assert_eq!(flags, vec![(1, true), (2, false)]);
    }

    #[test]
    fn retain_as_published_forwards_a_clear_flag_as_clear() {
        // [MQTT-3.3.1-13] sets the flag *equal to* the one received, so
        // the option must not manufacture a retain flag on a message
        // that was not published with one.
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert(
            "a",
            Subscription {
                retain_as_published: true,
                ..sub(1, QoS::AtMostOnce)
            },
        );

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a", QoS::AtMostOnce),
            None,
            &mut out,
        );
        assert!(!out[0].retain);
    }

    #[test]
    fn one_member_of_a_shared_group_receives_the_message() {
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a", sub(1, QoS::AtMostOnce));
        shared.insert("g", "a", sub(2, QoS::AtMostOnce));

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a", QoS::AtMostOnce),
            None,
            &mut out,
        );
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn an_ordinary_and_a_shared_subscription_both_receive() {
        // They are independent mechanisms, not alternatives.
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert("a", sub(1, QoS::AtMostOnce));
        shared.insert("g", "a", sub(2, QoS::AtMostOnce));

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a", QoS::AtMostOnce),
            None,
            &mut out,
        );
        assert_eq!(targets(&out), vec![1, 2]);
    }

    #[test]
    fn a_client_holding_both_an_ordinary_and_a_shared_match_receives_once() {
        // The shared selection is merged like any other, so the one
        // client still gets a single message.
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert("a", sub(1, QoS::AtMostOnce));
        shared.insert("g", "a", sub(1, QoS::ExactlyOnce));

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("a", QoS::ExactlyOnce),
            None,
            &mut out,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].qos, QoS::ExactlyOnce);
    }

    #[test]
    fn nothing_matching_produces_nothing() {
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert("a/b", sub(1, QoS::AtMostOnce));

        let mut out = Vec::new();
        route_all(
            &trie,
            &shared,
            &message("c/d", QoS::AtMostOnce),
            None,
            &mut out,
        );
        assert!(out.is_empty());
    }
}
