//! Turning one publication into deliveries.

use mqtt_codec::QoS;

use crate::session::message::Message;
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
pub fn route(
    trie: &SubscriptionTrie,
    shared: &SharedSubscriptions,
    message: &Message,
    publisher: Option<SubscriberId>,
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
    let mut chosen = Vec::new();
    shared.select(&message.topic[..], &mut chosen);
    for subscription in chosen {
        if is_withheld(&subscription, publisher) {
            continue;
        }
        merge(out, &subscription, message);
    }
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

    fn targets(out: &[Routed]) -> Vec<u64> {
        let mut ids: Vec<u64> = out.iter().map(|r| r.subscriber.0).collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn a_message_reaches_every_matching_subscriber() {
        let trie = SubscriptionTrie::new(4);
        let shared = SharedSubscriptions::new(4);
        trie.insert("a/+", sub(1, QoS::AtLeastOnce));
        trie.insert("a/b", sub(2, QoS::AtLeastOnce));
        trie.insert("z/#", sub(3, QoS::AtLeastOnce));

        let mut out = Vec::new();
        route(
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
        route(
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
        route(
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
        route(
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
        route(
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
        route(
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
        route(
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
        route(
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
        route(
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
        route(
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
        route(&trie, &shared, &retained, None, &mut out);
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
        route(
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
        route(
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
        route(
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
        route(
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
        route(
            &trie,
            &shared,
            &message("c/d", QoS::AtMostOnce),
            None,
            &mut out,
        );
        assert!(out.is_empty());
    }
}
