//! The hop count a publication with no session behind it carries.
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
//! # Why this is here rather than beside the connectors
//!
//! It was in `rules::hops`, which is where a reader would look for it,
//! and it moved the day a connector's publication started firing the
//! message hooks. That is the change that lets one connector's output
//! be another connector's input, so it is the change that makes a cycle
//! possible — and the place it becomes possible is
//! [`Broker::originate`], which is this crate's. A limit enforced
//! anywhere else would be a limit the next connector could be written
//! without.
//!
//! `rules::hops` still exists, still documents the three loops, and
//! re-exports what is here, so nothing that reads a count has two
//! answers to choose between.
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
//!
//! [`Broker::originate`]: crate::broker::Broker::originate

/// The user property a publication carries its hop count in.
///
/// Not `$`-prefixed: the reserved namespace section 12.3 describes is a
/// namespace of *topics*, and a property name is not a topic. Prefixed
/// with `sd-` so that it does not collide with an application's own
/// property of the same short name, which is the only collision a free
/// string can have.
pub const HOP_COUNT: &str = "sd-hops";

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
    }

    /// The first of several properties is not privileged, and neither
    /// is the last: the count is found by name wherever it sits.
    #[test]
    fn a_hop_count_is_found_among_other_properties() {
        let properties = vec![
            ("unit".to_owned(), "celsius".to_owned()),
            (HOP_COUNT.to_owned(), "4".to_owned()),
            ("source".to_owned(), "gateway".to_owned()),
        ];
        assert_eq!(count_of(&properties), Some(4));
    }
}
