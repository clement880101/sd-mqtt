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
//! A shared subscription to `$share/g/a/b` means this node has a
//! subscriber for `a/b`: the group name is how deliveries are shared
//! among this node's own clients and is nothing a peer routing a
//! publication can use. So [`inner_filter`] is applied before the event
//! is made, and `$share/g1/a/b` and `$share/g2/a/b` are two subscribers
//! to one filter rather than subscribers to two.

/// A filter gained or lost a subscriber on this node.
///
/// Carries the filter and nothing else. Which session it was is exactly
/// the part section 8.2 says never leaves the node, and a type that
/// carried it would invite a caller to replicate it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FilterEvent {
    /// One more of this node's sessions is subscribed to the filter.
    Arrived(String),
    /// One fewer of this node's sessions is subscribed to the filter.
    Departed(String),
}

impl FilterEvent {
    /// Which filter, whichever this is.
    #[must_use]
    pub fn filter(&self) -> &str {
        match self {
            Self::Arrived(filter) | Self::Departed(filter) => filter,
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
    fn an_event_names_its_filter_and_nothing_else() {
        assert_eq!(FilterEvent::Arrived("a/b".to_owned()).filter(), "a/b");
        assert_eq!(FilterEvent::Departed("a/b".to_owned()).filter(), "a/b");
    }
}
