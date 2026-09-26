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
//! # What is here and what is not
//!
//! Here: the property, the count a connector stamps on a message it
//! originates, and reading one back.
//!
//! **Not here: the dropping.** A hop count is only useful to a
//! connector that reads one *in* and decides whether to pass it on, and
//! the connectors that do that are `http_out` — which today posts
//! whatever fired it — and the bridge, which does not exist yet. Adding
//! the maximum before there is anything to enforce it on would be a
//! number in a configuration file that nothing reads, which is the
//! shape of a feature people believe they have.
//!
//! This is a deferral and not an oversight, and it is written down here
//! because convention 8 says the two look identical in a codebase and
//! opposite in a review. The counting half lands with the connector
//! that consumes it.
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

/// The user property a connector-originated publication carries its hop
/// count in.
///
/// Not `$`-prefixed: the reserved namespace section 12.3 describes is a
/// namespace of *topics*, and a property name is not a topic. Prefixed
/// with `sd-` so that it does not collide with an application's own
/// property of the same short name, which is the only collision a free
/// string can have.
pub const HOP_COUNT: &str = "sd-hops";

/// The properties for a publication a connector originates.
///
/// One hop: this is the first. A connector that originates cannot
/// exceed any maximum worth configuring, so nothing here refuses
/// anything — the refusal belongs to whatever passes a message on, and
/// is not written yet. See the module documentation.
pub fn originated() -> Vec<(String, String)> {
    vec![(HOP_COUNT.to_owned(), "1".to_owned())]
}

/// The hop count a publication carries, if it carries one.
///
/// `None` for a publication with no hop count, which is every one a
/// client made: a message that has never been through a connector has
/// not hopped, and "none" and "zero" are different answers.
///
/// A property that is present and is not a number is `None` too. It was
/// not written by this broker, and guessing what somebody else's
/// `sd-hops` meant is worse than treating the message as one that has
/// not been through a connector — which, since ours always writes a
/// number, it has not.
pub fn count_of(properties: &[(String, String)]) -> Option<u32> {
    properties
        .iter()
        .find(|(name, _)| name == HOP_COUNT)
        .and_then(|(_, value)| value.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_a_connector_originates_has_hopped_once() {
        assert_eq!(count_of(&originated()), Some(1));
    }

    /// A message that has not been through a connector has no count,
    /// and that is not the same as a count of zero.
    #[test]
    fn a_publication_with_no_hop_count_has_none() {
        assert_eq!(count_of(&[]), None);
        assert_eq!(
            count_of(&[("sd-hops-and-more".to_owned(), "3".to_owned())]),
            None,
            "a property whose name merely starts the same way is a different property"
        );
        assert_eq!(
            count_of(&[("SD-HOPS".to_owned(), "3".to_owned())]),
            None,
            "a user property name is compared exactly, as MQTT compares one"
        );
    }

    /// A count that is not a number is not a count.
    ///
    /// `"+1"` is deliberately not in the list: Rust's integer parser
    /// accepts a leading plus, so it reads as one hop. This assertion
    /// said otherwise and was wrong about the parser rather than about
    /// the code — and one hop is the right reading of it anyway.
    #[test]
    fn a_hop_count_that_is_not_a_number_is_not_one() {
        for nonsense in ["", "many", "-1", "1.0", " 1", "1 ", "0x1", "4294967296"] {
            assert_eq!(
                count_of(&[(HOP_COUNT.to_owned(), nonsense.to_owned())]),
                None,
                "{nonsense:?} is not a hop count this broker wrote"
            );
        }
        assert_eq!(
            count_of(&[(HOP_COUNT.to_owned(), "4294967295".to_owned())]),
            Some(u32::MAX),
            "the largest one that fits does"
        );
        assert_eq!(
            count_of(&[(HOP_COUNT.to_owned(), "+1".to_owned())]),
            Some(1),
            "a leading plus is accepted by the parser and means what it says"
        );
    }
}
