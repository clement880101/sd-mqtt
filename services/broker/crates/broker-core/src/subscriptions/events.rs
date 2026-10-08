//! What the broker says about a filter, for whoever replicates it.
//!
//! Section 8.2 replicates **that a node has at least one subscriber for a
//! filter**, and not which of its clients that is. Deciding when that
//! fact changed needs a count per filter, and the count belongs with the
//! register it writes — in `cluster::metadata`, which this crate does not
//! and must not depend on. So this crate says what happened and says it
//! once per session and filter, the way it already says what happened
//! into [`crate::journal`], and the layer that holds both counts them.
//!
//! # Once per session and filter, on every path
//!
//! An event is emitted when the subscription trie actually gained or lost
//! an entry, never when a subscribe merely changed the options on a
//! filter the same session already held. That is what makes the count
//! above reach zero: a count fed by every SUBSCRIBE packet would be
//! incremented twice by a client that re-subscribed and would never come
//! back down.
//!
//! **Three paths lose a filter on this node** and all three must emit,
//! because a count that misses one leaves the node advertised for a
//! filter nobody is subscribed to, for ever:
//!
//! - UNSUBSCRIBE.
//! - A connect that starts clean, which includes a clean-start takeover
//!   of an existing session.
//! - A session expiring in a tick.
//!
//! Only the first is the one anybody thinks of, which is why
//! [`crate::broker::subscribe::remove_held`] — the function the other two
//! go through — emits them rather than its callers.
//!
//! # A restore replays them, and the broker keeps them
//!
//! [`crate::broker::Broker::discard_journal`] throws away everything a
//! restore said into the journal, because a restore is reading back what
//! the store already holds. It does **not** throw these away, and the
//! reason is on that function: the count these feed is in no store
//! anywhere, so the replay is the only thing that rebuilds it.
//!
//! # The filter named is the one a topic is matched against
//!
//! A shared subscription to `$share/g/a/b` means this node has a member
//! of the group `g` on the filter `a/b`, so [`inner_filter`] is applied
//! before the event is made: the inner filter is what a published topic
//! is matched against, and `$share/g/a/b` and `a/b` must mean the same
//! thing to a matcher.
//!
//! # A shared group is its own event, and section 8.3 says why
//!
//! The group name used to be dropped here, on the argument that it is
//! how this node shares a delivery among its own clients and is nothing
//! a peer can route by. **That argument was wrong, and it was the one
//! defect in this milestone's design.** A group with a member on each of
//! three nodes reported as three ordinary subscribers to one filter puts
//! all three on the ordinary route, and one copy to each interested node
//! delivers three times where the semantics demand one.
//!
//! So a shared subscription is [`FilterEvent::GroupJoined`] and not
//! [`FilterEvent::Arrived`]: four events rather than two, and the group
//! and the filter both travel. What reads them is
//! `cluster::metadata`, which keeps two maps — one for ordinary
//! subscribers and one for groups — and chooses **one** node per group
//! before forwarding rather than after.
//!
//! The two are exclusive. A shared subscription is not also an ordinary
//! subscriber for its inner filter, and must not be reported as one: a
//! node whose only interest in a topic is a group member would otherwise
//! be on the ordinary route as well, be forwarded a copy for that, and
//! deliver to a group member that had not been chosen.

/// A filter gained or lost a subscriber on this node.
///
/// Carries the filter, the group where there is one, and nothing else.
/// Which session it was is exactly the part section 8.2 says never
/// leaves the node, and a type that carried it would invite a caller to
/// replicate it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FilterEvent {
    /// One more of this node's sessions is subscribed to the filter.
    ///
    /// An ordinary subscription. A shared one is [`Self::GroupJoined`].
    Arrived(String),
    /// One fewer of this node's sessions is subscribed to the filter.
    Departed(String),
    /// One more of this node's sessions is a member of a shared group.
    GroupJoined {
        /// The share name.
        group: String,
        /// The inner filter, which is what a topic is matched against.
        filter: String,
    },
    /// One fewer of this node's sessions is a member of a shared group.
    GroupLeft {
        /// The share name.
        group: String,
        /// The inner filter.
        filter: String,
    },
}

impl FilterEvent {
    /// Which filter, whichever this is.
    ///
    /// The inner filter in every case, so that a caller asking what a
    /// topic would be matched against gets one answer.
    #[must_use]
    pub fn filter(&self) -> &str {
        match self {
            Self::Arrived(filter) | Self::Departed(filter) => filter,
            Self::GroupJoined { filter, .. } | Self::GroupLeft { filter, .. } => filter,
        }
    }

    /// The share name, for the two that have one.
    #[must_use]
    pub fn group(&self) -> Option<&str> {
        match self {
            Self::Arrived(_) | Self::Departed(_) => None,
            Self::GroupJoined { group, .. } | Self::GroupLeft { group, .. } => Some(group),
        }
    }

    /// The arrival or departure for one filter, shared or not.
    ///
    /// **One place that decides which of the four an event is**, because
    /// the subscribe path has three callers that must agree and the
    /// consequence of disagreeing is a node advertised for a filter it
    /// does not serve, for ever.
    #[must_use]
    pub fn joined(group: Option<&str>, filter: &str) -> Self {
        match group {
            Some(group) => Self::GroupJoined {
                group: group.to_owned(),
                filter: inner_filter(filter).to_owned(),
            },
            None => Self::Arrived(inner_filter(filter).to_owned()),
        }
    }

    /// And the departure, by the same decision.
    #[must_use]
    pub fn left(group: Option<&str>, filter: &str) -> Self {
        match group {
            Some(group) => Self::GroupLeft {
                group: group.to_owned(),
                filter: inner_filter(filter).to_owned(),
            },
            None => Self::Departed(inner_filter(filter).to_owned()),
        }
    }

    /// The departure for a filter as a client wrote it.
    ///
    /// The group is read out of the filter rather than passed in,
    /// because the teardown paths hold a list of filters and nothing
    /// else: a session's record of what it subscribed to is the filter
    /// as written, `$share/` prefix and all.
    #[must_use]
    pub fn left_filter(filter: &str) -> Self {
        match mqtt_codec::topic::parse_shared(filter) {
            Ok(Some((group, inner))) => Self::GroupLeft {
                group: group.to_owned(),
                filter: inner.to_owned(),
            },
            // A filter this cannot parse is itself, for the reason
            // [`inner_filter`] gives: dropping the event would leave
            // the node advertised for something in the trie.
            _ => Self::Departed(inner_filter(filter).to_owned()),
        }
    }
}

/// The filter a published topic is matched against.
///
/// `$share/<name>/<filter>` is `<filter>`; anything else is itself. A
/// filter this cannot parse is returned unchanged, which is the right
/// answer rather than a refusal: every caller here has already had the
/// filter validated, and the one thing not to do is drop an event for a
/// filter that is in the trie.
#[must_use]
pub fn inner_filter(filter: &str) -> &str {
    match mqtt_codec::topic::parse_shared(filter) {
        Ok(Some((_, inner))) => inner,
        _ => filter,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_shared_filter_is_named_by_its_inner_filter() {
        // What a peer routes by. The group is how this node's own
        // clients share the delivery and means nothing outside it.
        assert_eq!(inner_filter("$share/workers/a/b"), "a/b");
        assert_eq!(inner_filter("$share/g/#"), "#");
    }

    #[test]
    fn an_ordinary_filter_is_itself() {
        assert_eq!(inner_filter("a/b"), "a/b");
        assert_eq!(inner_filter("#"), "#");
        assert_eq!(inner_filter("$SYS/broker/uptime"), "$SYS/broker/uptime");
    }

    #[test]
    fn a_filter_that_cannot_be_parsed_is_itself() {
        // Returned rather than refused: dropping the event would leave
        // the trie holding a filter the count knows nothing about, and
        // the node advertised for it until it restarts.
        assert_eq!(inner_filter("$share/"), "$share/");
        assert_eq!(inner_filter("$share"), "$share");
    }

    #[test]
    fn an_event_names_its_filter_and_its_group_if_it_has_one() {
        assert_eq!(FilterEvent::Arrived("a/b".to_owned()).filter(), "a/b");
        assert_eq!(FilterEvent::Departed("a/b".to_owned()).filter(), "a/b");
        assert_eq!(FilterEvent::Arrived("a/b".to_owned()).group(), None);

        let joined = FilterEvent::joined(Some("workers"), "$share/workers/a/b");
        assert_eq!(joined.filter(), "a/b", "the inner filter, in every case");
        assert_eq!(joined.group(), Some("workers"));
    }

    #[test]
    fn a_shared_subscription_is_a_group_event_and_not_an_ordinary_one() {
        // Section 8.3's trap, at the layer that reports it. A node whose
        // only interest in a topic is a group member must not be on the
        // ordinary route as well: it would be forwarded a copy for that
        // and deliver to a member that had not been chosen.
        assert_eq!(
            FilterEvent::joined(Some("g"), "$share/g/a/b"),
            FilterEvent::GroupJoined {
                group: "g".to_owned(),
                filter: "a/b".to_owned()
            }
        );
        assert_eq!(
            FilterEvent::joined(None, "a/b"),
            FilterEvent::Arrived("a/b".to_owned())
        );
    }

    #[test]
    fn a_departure_read_from_a_written_filter_keeps_its_group() {
        // The teardown paths hold the filter as the client wrote it and
        // nothing else, so the group has to come back out of it. A
        // departure that lost the group would decrement the ordinary
        // count for a filter that never incremented it, and the group's
        // own count would never reach zero.
        assert_eq!(
            FilterEvent::left_filter("$share/workers/a/b"),
            FilterEvent::GroupLeft {
                group: "workers".to_owned(),
                filter: "a/b".to_owned()
            }
        );
        assert_eq!(
            FilterEvent::left_filter("a/b"),
            FilterEvent::Departed("a/b".to_owned())
        );
        assert_eq!(
            FilterEvent::left_filter("$share/"),
            FilterEvent::Departed("$share/".to_owned()),
            "a filter nothing can parse is itself, not a dropped event"
        );
    }
}
