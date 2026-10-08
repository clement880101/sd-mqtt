//! Subscribing, unsubscribing, and what retained messages go out.

use mqtt_codec::packet::{RetainHandling, Subscribe};
use mqtt_codec::{Limits, QoS, ReasonCode};
use storage::record::SubscriptionRecord;

use crate::journal;
use crate::retained::RetainedStore;
use crate::session::message::Message;
use crate::session::queue::Delivery;
use crate::session::state::Session;
use crate::session::Action;
use crate::subscriptions::events::FilterEvent;
use crate::subscriptions::{SharedSubscriptions, Subscription, SubscriptionTrie};

/// Apply a subscribe request, returning one reason code per filter.
///
/// A bad filter refuses only itself: the protocol answers per filter, and
/// refusing the whole packet would punish the good ones. [MQTT-3.8.4-5]
/// requires a packet of several filters to be handled as a sequence of
/// single-filter packets with their answers combined.
// Eight parameters, one over clippy's threshold. Every one is a distinct
// collaborator the broker owns separately, and bundling them into a
// context struct would exist only to satisfy the count while task 7 is
// about to own the assembly.
#[allow(clippy::too_many_arguments)]
pub fn subscribe(
    trie: &SubscriptionTrie,
    shared: &SharedSubscriptions,
    retained: &RetainedStore,
    session: &mut Session,
    packet: &Subscribe,
    limits: &Limits,
    now: u32,
    authorize: &mut dyn FnMut(&str, QoS) -> bool,
    out: &mut Vec<Action>,
    events: &mut Vec<FilterEvent>,
) -> Vec<ReasonCode> {
    let subscription_id = packet.properties.subscription_identifiers.first().copied();
    let mut reasons = Vec::with_capacity(packet.filters.len());

    for options in &packet.filters {
        let group = match mqtt_codec::topic::validate_subscription(&options.filter, limits) {
            Ok(group) => group,
            Err(_) => {
                reasons.push(ReasonCode::TopicFilterInvalid);
                continue;
            }
        };

        // Section 9.3's authorize-subscribe chain, asked per filter
        // for the same reason a bad filter refuses only itself, and
        // before anything is inserted so that a refusal leaves
        // nothing behind: no entry in the trie, no record in the
        // session and no retained message on its way out.
        //
        // 3.9.3: "Not authorized" is one of the reason codes a
        // suback may carry per filter.
        if !authorize(&options.filter, options.qos) {
            reasons.push(ReasonCode::NotAuthorized);
            continue;
        }

        let existed = install(
            trie,
            shared,
            session,
            &options.filter,
            group,
            Subscription {
                subscriber: session.subscriber(),
                qos: options.qos,
                no_local: options.no_local,
                retain_as_published: options.retain_as_published,
                id: subscription_id,
            },
            SubscriptionRecord {
                filter: options.filter.clone(),
                qos: journal::stored_qos(options.qos),
                no_local: options.no_local,
                retain_as_published: options.retain_as_published,
                retain_handling: journal::stored_retain_handling(options.retain_handling),
                subscription_id,
            },
            events,
        );

        // A shared subscription has no single subscriber to hand retained
        // messages to, so it receives none. Section 3.3.1.3 scopes the
        // whole mechanism to "a new Non-shared Subscription", and 3.8.4
        // says so twice more for shared filters, new group or not.
        let wants_retained = group.is_none()
            && match options.retain_handling {
                RetainHandling::SendAlways => true,
                RetainHandling::SendIfNew => !existed,
                RetainHandling::DoNotSend => false,
            };

        if wants_retained {
            deliver_retained(
                retained,
                session,
                &options.filter,
                options,
                subscription_id,
                now,
                out,
            );
        }

        reasons.push(match options.qos {
            QoS::AtMostOnce => ReasonCode::Success,
            QoS::AtLeastOnce => ReasonCode::GrantedQos1,
            QoS::ExactlyOnce => ReasonCode::GrantedQos2,
        });
    }

    reasons
}

/// Put one subscription into the matching structures and the session.
///
/// The one place a subscription is inserted, so that a client's
/// SUBSCRIBE and a session arriving from another node cannot reach two
/// different answers about what the cluster is told. Returns whether the
/// handle already held an entry for the filter.
// Eight parameters, one over clippy's threshold, for the reason
// `subscribe` above it carries the same allowance: every one is a
// distinct collaborator the broker owns separately, and a context struct
// here would exist only to satisfy the count.
#[allow(clippy::too_many_arguments)]
fn install(
    trie: &SubscriptionTrie,
    shared: &SharedSubscriptions,
    session: &mut Session,
    filter: &str,
    group: Option<(&str, &str)>,
    subscription: Subscription,
    record: SubscriptionRecord,
    events: &mut Vec<FilterEvent>,
) -> bool {
    let existed = match group {
        Some((name, inner)) => shared.insert(name, inner, subscription),
        None => trie.insert(filter, subscription),
    };
    // Section 8.2: the cluster is told that this node has a
    // subscriber for the filter, and the filter is the one a
    // published topic is matched against — the inner filter for a
    // shared subscription.
    //
    // **The group travels with it**, and section 8.3 is why: a
    // group with a member on each of three nodes reported as three
    // ordinary subscribers to one filter is delivered to three
    // times. See [`crate::subscriptions::events`].
    //
    // **Only when the trie actually gained an entry.** A second
    // subscribe to a filter this session already holds changes the
    // options rather than adding a subscription, and an arrival for
    // it would be a count incremented twice that never comes back
    // down: the filter would stay advertised to the cluster after
    // its last subscriber left.
    if !existed {
        events.push(FilterEvent::joined(group.map(|(name, _)| name), filter));
    }
    // Everything the filter was taken under, not just its name. The
    // identifier above all: [MQTT-3.8.4-3] has it echoed on every
    // delivery the subscription causes, and a restart that dropped
    // it would deliver messages a client could no longer route.
    session.add_subscription(record);
    existed
}

/// Put back the subscriptions a session held on another node.
///
/// Section 8.4: **subscriptions follow the session.** The new owner
/// gains the filters the previous owner had, which is what makes
/// section 8.3 start forwarding here and stop forwarding there.
///
/// Three things this is not, each of which would be wrong:
///
/// - **No retained messages go out.** 3.3.1.3 scopes retained delivery
///   to "a new Non-shared Subscription", and this subscription is not
///   new — the client made it on the other node and was given the
///   retained messages there. Replaying them on a migration would hand
///   a client a second copy of every retained message under every
///   filter it holds, every time it moved. That is also why this is not
///   `subscribe` with a packet built by the caller: a packet carries a
///   retain-handling option, and the only honest value to give it would
///   be one that contradicts the one being restored.
/// - **No hook is asked.** The authorize-subscribe chain judged these
///   filters when the client asked for them. Asking again here would
///   make a migration able to silently drop a subscription that a
///   node's configuration happens to read differently, and the client
///   is not told, because there is no SUBACK to put a reason code in.
/// - **No reason codes come back.** There is nobody to answer.
///
/// Returns how many filters could not be put back, which is a filter
/// the far node accepted and this build will not parse. Counted rather
/// than silent: it is a disagreement between two builds and it loses a
/// subscription.
pub fn reinstate(
    trie: &SubscriptionTrie,
    shared: &SharedSubscriptions,
    session: &mut Session,
    held: &[SubscriptionRecord],
    limits: &Limits,
    events: &mut Vec<FilterEvent>,
) -> usize {
    let mut refused = 0;
    for record in held {
        let Ok(group) = mqtt_codec::topic::validate_subscription(&record.filter, limits) else {
            refused += 1;
            continue;
        };
        install(
            trie,
            shared,
            session,
            &record.filter,
            group,
            Subscription {
                subscriber: session.subscriber(),
                qos: journal::live_qos(record.qos),
                no_local: record.no_local,
                retain_as_published: record.retain_as_published,
                id: record.subscription_id,
            },
            record.clone(),
            events,
        );
    }
    refused
}

fn deliver_retained(
    retained: &RetainedStore,
    session: &mut Session,
    filter: &str,
    options: &mqtt_codec::packet::SubscriptionOptions,
    subscription_id: Option<u32>,
    now: u32,
    out: &mut Vec<Action>,
) {
    let mut matches = Vec::new();
    retained.matching(filter, now, &mut matches);

    for (topic, kept) in matches {
        let qos = options.qos.min(kept.qos);
        let mut message = Message::new(&topic, kept.payload, kept.qos, true);
        message.expires_at = kept.expires_at;
        // What was published is what is delivered. The Message Expiry
        // Interval inside is the publisher's and is stale; the outbound
        // path rewrites it from `expires_at` on the way out.
        message.properties = kept.properties;

        session.outbound.offer(
            Delivery {
                message,
                qos,
                // A retained delivery always says so, whatever the
                // retain-as-published option says: that option is about
                // live messages. Section 3.3.1.3, "These messages are
                // sent with the RETAIN flag set to 1", and 3.8.3.1,
                // "Retained messages sent when the subscription is
                // established have the RETAIN flag set to 1".
                retain: true,
                dup: false,
                packet_id: None,
                subscription_ids: subscription_id.into_iter().collect(),
                sequence: 0,
            },
            now,
            out,
        );
    }
}

/// Remove filters, returning one reason code each.
pub fn unsubscribe(
    trie: &SubscriptionTrie,
    shared: &SharedSubscriptions,
    session: &mut Session,
    filters: &[String],
    events: &mut Vec<FilterEvent>,
) -> Vec<ReasonCode> {
    filters
        .iter()
        .map(|filter| {
            let removed = match mqtt_codec::topic::parse_shared(filter) {
                Ok(Some((name, inner))) => shared.remove(name, inner, session.subscriber()),
                _ => trie.remove(filter, session.subscriber()),
            };
            session.remove_filter(filter);
            if removed {
                // Only what actually went, for the reason the arrival
                // above is only what actually came: a departure from a
                // filter nothing held would take the count below zero,
                // and a count that cannot reach zero again never
                // tombstones the filter.
                events.push(FilterEvent::left_filter(filter));
                ReasonCode::Success
            } else {
                ReasonCode::NoSubscriptionExisted
            }
        })
        .collect()
}

/// Remove every subscription a session holds, for teardown.
pub fn remove_all(
    trie: &SubscriptionTrie,
    shared: &SharedSubscriptions,
    session: &mut Session,
    events: &mut Vec<FilterEvent>,
) {
    let held: Vec<String> = session.filters().map(str::to_string).collect();
    remove_held(trie, shared, session, &held, events);
}

/// The same, told what the handle's subscriptions were.
///
/// # Two of section 8.2's three departure paths come through here
///
/// A clean-start connect and a session expiring in a tick, neither of
/// which is an UNSUBSCRIBE and neither of which any client asked for.
/// The departures are emitted **here rather than by those two callers**,
/// because a third teardown path added later gets them for free, and a
/// departure missed leaves this node advertised to the cluster for a
/// filter nobody is subscribed to, for ever — a restart does not repair
/// it, because the subscription is gone from the store too.
///
/// # Why `held` is a parameter and not read from `session`
///
/// A clean-start connect **replaces the session before the trie is
/// cleared**: the registry hands back a fresh one under the same handle,
/// so by the time this runs the session's own list is empty while the
/// trie still holds its entries. Reading the list here would report
/// nothing on exactly the path nobody thinks of, and would do it
/// silently, because the trie is cleared by handle and comes out right
/// either way. So the caller that knows which session it is taking away
/// says what it held.
pub fn remove_held(
    trie: &SubscriptionTrie,
    shared: &SharedSubscriptions,
    session: &mut Session,
    held: &[String],
    events: &mut Vec<FilterEvent>,
) {
    trie.remove_subscriber(session.subscriber());
    shared.remove_subscriber(session.subscriber());
    for filter in held {
        // The return is not consulted, for the reason above: on the
        // clean-start path the session being told is the replacement and
        // holds none of these. One departure per filter the handle held
        // is the claim, and `held` is one entry per filter.
        session.remove_filter(filter);
        events.push(FilterEvent::left_filter(filter));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The authorizing chain these tests are not about.
    ///
    /// A plain `|_, _| true` at every call site would be the same
    /// thing written fifteen times; that the chain itself is asked
    /// per filter, and what a refusal leaves behind, is tested
    /// through the broker in `broker/mod.rs` and in
    /// `tests/end_to_end.rs`, where a refusal can be seen to change
    /// where a message goes.
    #[allow(clippy::unnecessary_wraps)]
    fn allow_everything(_filter: &str, _qos: QoS) -> bool {
        true
    }

    use crate::registry::SessionId;
    use crate::retained::{RetainedMessage, RetainedStore};
    use crate::session::state::Session;
    use crate::session::Action;
    use crate::subscriptions::{SharedSubscriptions, SubscriberId, SubscriptionTrie};
    use bytes::Bytes;
    use mqtt_codec::packet::{RetainHandling, Subscribe, SubscriptionOptions};
    use mqtt_codec::{Limits, Packet, Properties, QoS, ReasonCode, Version};
    use std::sync::Arc;

    struct Fixture {
        trie: SubscriptionTrie,
        shared: SharedSubscriptions,
        retained: RetainedStore,
        session: Session,
    }

    fn fixture() -> Fixture {
        let retained = RetainedStore::new();
        retained.set(
            "a/b",
            RetainedMessage {
                payload: Bytes::from_static(b"kept"),
                qos: QoS::AtMostOnce,
                expires_at: None,
                properties: Arc::new(Properties::default().into()),
            },
        );
        Fixture {
            trie: SubscriptionTrie::new(4),
            shared: SharedSubscriptions::new(4),
            retained,
            session: Session::new(
                SessionId::new("", "c"),
                SubscriberId(1),
                Version::V5,
                1,
                0,
                crate::session::state::SessionBounds {
                    max_inflight: 20,
                    ..Default::default()
                },
            ),
        }
    }

    fn request(filter: &str, handling: RetainHandling) -> Subscribe {
        Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: filter.into(),
                qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: handling,
            }],
            properties: Properties::default(),
        }
    }

    fn sent_topics(actions: &[Action]) -> Vec<String> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send(Packet::Publish(p)) => Some(p.topic.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn subscribing_grants_the_level_requested() {
        let mut f = fixture();
        let mut out = Vec::new();
        let reasons = subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("x/y", RetainHandling::DoNotSend),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert_eq!(reasons, vec![ReasonCode::GrantedQos1]);
        assert_eq!(f.trie.len(), 1);
    }

    #[test]
    fn a_retained_message_goes_out_with_the_retain_flag_set() {
        let mut f = fixture();
        let mut out = Vec::new();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("a/+", RetainHandling::SendAlways),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert_eq!(sent_topics(&out), vec!["a/b".to_string()]);

        let retained_flag = out
            .iter()
            .any(|a| matches!(a, Action::Send(Packet::Publish(p)) if p.retain));
        assert!(retained_flag, "a retained delivery says so on the wire");
    }

    #[test]
    fn a_live_subscription_option_does_not_clear_a_retained_delivery_flag() {
        // [MQTT-3.3.1-12] clears the flag on a *forwarded* message when
        // retain-as-published is off. Section 3.8.3.1 says separately that
        // "Retained messages sent when the subscription is established
        // have the RETAIN flag set to 1", so the option must not reach
        // this path. The fixture's options have it off.
        let mut f = fixture();
        let mut out = Vec::new();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("a/+", RetainHandling::SendAlways),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        let flags: Vec<bool> = out
            .iter()
            .filter_map(|a| match a {
                Action::Send(Packet::Publish(p)) => Some(p.retain),
                _ => None,
            })
            .collect();
        assert_eq!(flags, vec![true]);
    }

    #[test]
    fn send_if_new_sends_the_first_time_and_not_the_second() {
        // This is the only place the difference between always and
        // if-new is visible, which is why it is easy to get wrong.
        let mut f = fixture();
        let mut out = Vec::new();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("a/+", RetainHandling::SendIfNew),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert_eq!(sent_topics(&out).len(), 1);

        out.clear();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("a/+", RetainHandling::SendIfNew),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert!(sent_topics(&out).is_empty(), "the subscription existed");
    }

    #[test]
    fn send_always_resends_on_a_re_subscribe() {
        // The other half of the distinction, [MQTT-3.8.4-4]: replacing a
        // subscription with retain handling 0 re-sends. Without this the
        // if-new test alone passes for an implementation that never sends
        // on a re-subscribe whatever the option says.
        let mut f = fixture();
        let mut out = Vec::new();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("a/+", RetainHandling::SendAlways),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert_eq!(sent_topics(&out).len(), 1);

        out.clear();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("a/+", RetainHandling::SendAlways),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert_eq!(sent_topics(&out).len(), 1, "sent again, [MQTT-3.3.1-9]");
    }

    #[test]
    fn do_not_send_never_sends_even_the_first_time() {
        let mut f = fixture();
        let mut out = Vec::new();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("a/+", RetainHandling::DoNotSend),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert!(sent_topics(&out).is_empty());
    }

    #[test]
    fn a_shared_subscription_goes_to_the_shared_structure() {
        let mut f = fixture();
        let mut out = Vec::new();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("$share/g/x/y", RetainHandling::DoNotSend),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert_eq!(f.shared.group_count(), 1);
        assert_eq!(f.trie.len(), 0, "not in the ordinary trie");
    }

    #[test]
    fn a_shared_subscription_receives_no_retained_messages() {
        // There is no single subscriber to send them to.
        let mut f = fixture();
        let mut out = Vec::new();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("$share/g/a/+", RetainHandling::SendAlways),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert!(sent_topics(&out).is_empty());
    }

    #[test]
    fn an_invalid_filter_is_refused_without_affecting_the_others() {
        let mut f = fixture();
        let mut out = Vec::new();
        let packet = Subscribe {
            packet_id: 1,
            filters: vec![
                SubscriptionOptions {
                    filter: "good/+".into(),
                    qos: QoS::AtMostOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: RetainHandling::DoNotSend,
                },
                SubscriptionOptions {
                    filter: "bad/#/x".into(),
                    qos: QoS::AtMostOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: RetainHandling::DoNotSend,
                },
            ],
            properties: Properties::default(),
        };
        let reasons = subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &packet,
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert_eq!(reasons.len(), 2);
        assert!(!reasons[0].is_failure());
        assert_eq!(reasons[1], ReasonCode::TopicFilterInvalid);
        assert_eq!(f.trie.len(), 1, "the good one still took");
    }

    #[test]
    fn a_refused_filter_is_not_recorded_against_the_session() {
        // Recording it would make teardown try to remove a subscription
        // that was never made, and would let an unsubscribe report
        // success for a filter the broker refused.
        let mut f = fixture();
        let mut out = Vec::new();
        let packet = Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: "bad/#/x".into(),
                qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::DoNotSend,
            }],
            properties: Properties::default(),
        };
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &packet,
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert_eq!(f.session.filters().count(), 0);
    }

    #[test]
    fn a_subscription_identifier_rides_along_on_a_retained_delivery() {
        let mut f = fixture();
        let mut out = Vec::new();
        let mut packet = request("a/+", RetainHandling::SendAlways);
        packet.properties.subscription_identifiers = vec![7];
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &packet,
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        let ids: Vec<Vec<u32>> = out
            .iter()
            .filter_map(|a| match a {
                Action::Send(Packet::Publish(p)) => {
                    Some(p.properties.subscription_identifiers.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(ids, vec![vec![7]]);
    }

    #[test]
    fn an_expired_retained_message_is_not_delivered() {
        // Expiry is checked on read, so the store needs the current time.
        let mut f = fixture();
        f.retained.set(
            "a/c",
            RetainedMessage {
                payload: Bytes::from_static(b"stale"),
                qos: QoS::AtMostOnce,
                expires_at: Some(5),
                properties: Arc::new(Properties::default().into()),
            },
        );
        let mut out = Vec::new();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("a/+", RetainHandling::SendAlways),
            &Limits::default(),
            9,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );
        assert_eq!(sent_topics(&out), vec!["a/b".to_string()]);
    }

    #[test]
    fn unsubscribing_reports_whether_anything_was_there() {
        let mut f = fixture();
        let mut out = Vec::new();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("x/y", RetainHandling::DoNotSend),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );

        let reasons = unsubscribe(
            &f.trie,
            &f.shared,
            &mut f.session,
            &["x/y".into(), "z".into()],
            &mut Vec::new(),
        );
        assert_eq!(reasons[0], ReasonCode::Success);
        assert_eq!(reasons[1], ReasonCode::NoSubscriptionExisted);
        assert_eq!(f.trie.len(), 0);
    }

    #[test]
    fn unsubscribing_a_shared_filter_leaves_the_group() {
        let mut f = fixture();
        let mut out = Vec::new();
        subscribe(
            &f.trie,
            &f.shared,
            &f.retained,
            &mut f.session,
            &request("$share/g/x/y", RetainHandling::DoNotSend),
            &Limits::default(),
            0,
            &mut allow_everything,
            &mut out,
            &mut Vec::new(),
        );

        let reasons = unsubscribe(
            &f.trie,
            &f.shared,
            &mut f.session,
            &["$share/g/x/y".into()],
            &mut Vec::new(),
        );
        assert_eq!(reasons[0], ReasonCode::Success);
        assert_eq!(f.shared.group_count(), 0);
        assert_eq!(f.session.filters().count(), 0);
    }

    #[test]
    fn tearing_down_a_session_removes_every_filter_it_held() {
        let mut f = fixture();
        let mut out = Vec::new();
        for filter in ["a/1", "a/2", "$share/g/a/3"] {
            subscribe(
                &f.trie,
                &f.shared,
                &f.retained,
                &mut f.session,
                &request(filter, RetainHandling::DoNotSend),
                &Limits::default(),
                0,
                &mut allow_everything,
                &mut out,
                &mut Vec::new(),
            );
        }
        assert_eq!(f.trie.len() + f.shared.group_count(), 3);

        remove_all(&f.trie, &f.shared, &mut f.session, &mut Vec::new());
        assert_eq!(f.trie.len(), 0);
        assert_eq!(f.shared.group_count(), 0);
        assert_eq!(f.session.filters().count(), 0);
    }
}
