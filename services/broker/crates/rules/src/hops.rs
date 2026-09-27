//! The hop count a connector-originated publication carries.
//!
//! Section 9.2, the third loop. The first two are a bridge sending back
//! what it received, and they are answered by refusing a configuration
//! whose inbound and outbound filters intersect and, second, by a
//! marker on the message. **The third has no bridge in it at all**: an
//! `http_out` rule feeding a system that posts back through an `http_in`
//! rule is a cycle in which every hop is a different connector, and
//! nothing either of them knows about itself can see it.
//!
//! So every publication a connector originates carries a hop count, and
//! one over the configured maximum is dropped and counted.
//!
//! # What is here, and where the dropping is
//!
//! Here: what a connector stamps on a message it originates. The
//! property and the reader are [`broker_core::hops`]'s and are
//! re-exported, so nothing has two answers to choose between.
//!
//! **There are two places a count is enforced and they are different
//! checks.**
//!
//! [`crate::bridge::marker::carry`] is a bridge's own, against that
//! rule's `mqtt_bridge.max_hops`. It runs before a message costs a
//! network round trip and it carries the loop marker with it, which is
//! the check no node-wide number can make.
//!
//! [`broker_core::broker::Broker::originate`] is the node's, against
//! `broker.max_hops`, and it is the one that closes the cycle this
//! module is about. It sits at the door every publication without a
//! session goes through — ingest, a poller, a bridge's inbound half,
//! the admin API — which is the door a connector's publication started
//! firing the message hooks at. Firing them is what lets one
//! connector's output be another's input, so the limit is at the same
//! place as the thing that needs limiting, and a connector written
//! later cannot be written without it.
//!
//! `http_out` still posts whatever fired it, and that half **is** still
//! deferred: an outbox envelope goes to a URL rather than to another
//! broker, so the thing that would refuse it is the endpoint's own
//! configuration rather than this count. What the endpoint *can* do,
//! and what closes the cycle, is send the count back: the envelope
//! carries it among its user properties, and `http_in` reads it from
//! the `sd-hops` request header. See [`crate::http_in`].
//!
//! # It is a user property, so 3.1.1 cannot carry it
//!
//! MQTT 3.1.1 has no user properties. A publication this broker
//! originates is carried to a 3.1.1 subscriber with the property
//! dropped, exactly as every other v5 property is, and the loop
//! prevention that does not depend on a property — the
//! configuration-time refusal section 9.2 puts first — is the one that
//! holds on both versions. The same reasoning, in section 9.2's own
//! words: the marker is the second line of defence, not the first.

pub use broker_core::hops::{count_of, HOP_COUNT};

/// The properties for a publication a connector originates.
///
/// One hop: this is the first. A connector that originates cannot
/// exceed any maximum worth configuring, so nothing here refuses
/// anything — the refusal belongs to whatever passes a message on, and
/// is named in the module documentation.
pub fn originated() -> Vec<(String, String)> {
    after(0)
}

/// The properties for a publication a connector makes on behalf of a
/// message that had already been through `hops` connectors.
///
/// [`originated`] is this with nothing behind it. Separate because a
/// connector that is passing somebody else's chain on must not restart
/// the count: section 9.2's third loop is closed only if the number
/// grows all the way round it, and a connector that minted a fresh one
/// would make every message one hop old for ever.
pub fn after(hops: u32) -> Vec<(String, String)> {
    vec![(HOP_COUNT.to_owned(), hops.saturating_add(1).to_string())]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_a_connector_originates_has_hopped_once() {
        assert_eq!(count_of(&originated()), Some(1));
    }

    /// The count a connector passes on is the one it was given plus
    /// its own hop, not a fresh one. A connector that restarted the
    /// count would leave the cycle this exists to bound unbounded.
    #[test]
    fn a_connector_passing_a_chain_on_adds_to_it() {
        assert_eq!(count_of(&after(0)), Some(1));
        assert_eq!(count_of(&after(4)), Some(5));
        assert_eq!(
            count_of(&originated()),
            count_of(&after(0)),
            "originating is the same as passing on a chain of nothing"
        );
    }

    /// Nothing here wraps. A count at the ceiling stays there, and the
    /// limit — which is a small number — refuses it long before.
    #[test]
    fn a_hop_count_at_the_ceiling_does_not_wrap_to_nothing() {
        assert_eq!(count_of(&after(u32::MAX)), Some(u32::MAX));
    }
}
