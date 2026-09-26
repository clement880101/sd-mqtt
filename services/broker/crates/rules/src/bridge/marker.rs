//! The second line of defence: a marker on the message itself.
//!
//! Section 9.2: "Loop prevention uses an origin identifier carried in a
//! user property, and messages that have already traversed this bridge
//! are not sent back."
//!
//! [`super::topics`] is the first line and refuses a configuration that
//! loops. This is the second, and it answers what a configuration check
//! cannot: a message that went out through this bridge, was published
//! by the remote onto some *other* topic by a rule of its own, and came
//! back on a filter that does not overlap anything. No static analysis
//! of two filters can see that, because the loop goes through a broker
//! this end does not configure.
//!
//! # Setting it and checking it are one feature with two halves
//!
//! The plan says so and says why: "the loop marker not set; the marker
//! not checked on the way back. The last two are the pair that matters,
//! and each alone leaves the loop open." A bridge that stamps every
//! message and reads none carries a marker nobody uses; a bridge that
//! reads one and stamps none never sees it. Both look like a working
//! feature from every angle except the one that matters, so the tests
//! below assert each half against a mutation of the other.
//!
//! # 3.1.1 cannot carry it, which is why it is second
//!
//! MQTT 3.1.1 has no user properties. A bridge to a 3.1.1 remote
//! therefore has the configuration refusal and nothing else, which is
//! exactly section 9.2's point:
//!
//! > **That mechanism does not exist in MQTT 3.1.1** […] So the marker
//! > is the second line of defence, not the first.
//!
//! [`stamp`] is a no-op on 3.1.1 rather than a failure, because the
//! alternative would be refusing to bridge to the version people
//! actually bridge to.
//!
//! # The origin is this bridge's, not this broker's
//!
//! Two bridges on one node to two different remotes must not read each
//! other's markers, or a message that legitimately came in through one
//! and should go out through the other would be refused as a loop. So
//! the origin is derived from the rule identifier, which is unique per
//! bridge by [`crate::rule::RuleId`]'s own rules.

use mqtt_codec::Version;

use crate::rule::RuleId;

/// The user property a bridge marks a message it has carried with.
///
/// Not `$`-prefixed, for the reason [`crate::hops::HOP_COUNT`] gives:
/// the reserved namespace section 12.3 describes is a namespace of
/// topics, and a property name is not a topic. Prefixed with `sd-` so
/// that it cannot collide with an application's own property of the
/// same short name.
pub const ORIGIN: &str = "sd-bridge-origin";

/// What one bridge marks its traffic with.
///
/// A value rather than a bare string so that the two halves of the
/// feature cannot be given different origins by accident: there is one
/// of these per bridge and both [`Marker::stamp`] and
/// [`Marker::traversed`] are on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Marker {
    /// The value carried in the property.
    origin: String,
    /// Which version the remote speaks, because 3.1.1 carries nothing.
    version: Version,
}

impl Marker {
    /// The marker for one bridge rule.
    pub fn new(rule: &RuleId, version: Version) -> Self {
        Self {
            origin: rule.as_str().to_owned(),
            version,
        }
    }

    /// What this bridge's traffic is marked with.
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Whether the marker can travel at all.
    ///
    /// False on 3.1.1, where there are no user properties. A caller
    /// that wants to log why a bridge is relying on the configuration
    /// refusal alone asks this.
    pub fn available(&self) -> bool {
        self.version == Version::V5
    }

    /// Mark a message as having been through this bridge.
    ///
    /// Idempotent: a message already carrying this bridge's mark is
    /// left alone rather than given a second copy, because a property
    /// list that grows by one on every pass is a message that gets
    /// bigger the longer a loop runs.
    ///
    /// A no-op on 3.1.1, where there is nothing to put it in.
    pub fn stamp(&self, properties: &mut Vec<(String, String)>) {
        if !self.available() || self.traversed(properties) {
            return;
        }
        properties.push((ORIGIN.to_owned(), self.origin.clone()));
    }

    /// Whether this message has already been through this bridge.
    ///
    /// Only **this** bridge's mark counts. A message marked by a
    /// different bridge on this node came in through that one and is
    /// entitled to go out through this one; refusing it would turn two
    /// bridges into a bridge that carries nothing.
    pub fn traversed(&self, properties: &[(String, String)]) -> bool {
        properties
            .iter()
            .any(|(name, value)| name == ORIGIN && value == &self.origin)
    }
}

/// Why a message a bridge was given is not carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// It has already been through this bridge.
    Looped,
    /// It has been through more connectors than the rule allows.
    ///
    /// Section 9.2's third loop, the one with no bridge in it: "an
    /// outbound rule feeding a system that posts back through an
    /// ingest rule is a cycle with no bridge in it. Every publication a
    /// connector originates carries a hop count, and one over the
    /// configured maximum is dropped and counted."
    TooManyHops {
        /// The count it arrived with.
        hops: u32,
        /// What the rule allows.
        maximum: u32,
    },
}

/// Whether this bridge carries a message, and the properties it
/// carries it with.
///
/// The one place both loop defences are applied, so that a caller
/// cannot apply one and forget the other. Answers the properties to
/// send rather than mutating in place, because the message the local
/// broker holds is somebody else's and must not grow a property
/// because a bridge looked at it.
///
/// # Errors
///
/// [`Refused`], which is the message not being carried and the reason
/// worth counting.
pub fn carry(
    marker: &Marker,
    maximum_hops: u32,
    properties: &[(String, String)],
) -> Result<Vec<(String, String)>, Refused> {
    if marker.traversed(properties) {
        return Err(Refused::Looped);
    }
    // A message that has never been through a connector has no count,
    // and that is not the same as a count of zero; `hops::count_of`
    // keeps the distinction and this treats an absent count as the
    // first hop.
    let hops = crate::hops::count_of(properties).unwrap_or(0);
    if hops >= maximum_hops {
        return Err(Refused::TooManyHops {
            hops,
            maximum: maximum_hops,
        });
    }

    let mut carried: Vec<(String, String)> = properties
        .iter()
        .filter(|(name, _)| name != crate::hops::HOP_COUNT)
        .cloned()
        .collect();
    carried.push((crate::hops::HOP_COUNT.to_owned(), (hops + 1).to_string()));
    marker.stamp(&mut carried);
    Ok(carried)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hops;

    fn marker(version: Version) -> Marker {
        Marker::new(
            &RuleId::new("bridge-a").expect("a plain word is an identifier"),
            version,
        )
    }

    /// The first half of the pair. Without it there is nothing for the
    /// other half to find.
    #[test]
    fn a_message_this_bridge_carries_is_marked() {
        let marker = marker(Version::V5);
        let carried = carry(&marker, 8, &[]).expect("carried");
        assert!(
            carried
                .iter()
                .any(|(name, value)| name == ORIGIN && value == "bridge-a"),
            "nothing marked it, so nothing downstream can refuse it: {carried:?}"
        );
    }

    /// The second half. Without it the mark is a property nobody reads.
    #[test]
    fn a_message_that_has_already_been_through_is_refused() {
        let marker = marker(Version::V5);
        let carried = carry(&marker, 8, &[]).expect("carried");
        assert_eq!(carry(&marker, 8, &carried), Err(Refused::Looped));
    }

    /// The pair, stated as one round trip. This is the test that fails
    /// if **either** half is removed, which is the point of writing it
    /// as a round trip rather than as two assertions about one
    /// function.
    #[test]
    fn what_goes_out_marked_does_not_come_back_in() {
        let marker = marker(Version::V5);
        // Out through the bridge.
        let on_the_wire = carry(&marker, 8, &[]).expect("carried out");
        // The remote hands it back, properties intact, on some other
        // topic that no configuration check could have predicted.
        assert_eq!(
            carry(&marker, 8, &on_the_wire),
            Err(Refused::Looped),
            "the loop the configuration refusal cannot see"
        );
    }

    /// Two bridges on one node must not read each other's marks, or a
    /// message that legitimately came in through one and should go out
    /// through the other is refused and the pair carries nothing.
    #[test]
    fn one_bridge_does_not_read_another_s_mark() {
        let first = marker(Version::V5);
        let second = Marker::new(
            &RuleId::new("bridge-b").expect("an identifier"),
            Version::V5,
        );

        let from_first = carry(&first, 8, &[]).expect("carried");
        let through_second = carry(&second, 8, &from_first).expect("a different bridge's mark");
        assert!(through_second
            .iter()
            .any(|(name, value)| name == ORIGIN && value == "bridge-a"));
        assert!(through_second
            .iter()
            .any(|(name, value)| name == ORIGIN && value == "bridge-b"));

        // And now neither will take it back.
        assert_eq!(carry(&first, 8, &through_second), Err(Refused::Looped));
        assert_eq!(carry(&second, 8, &through_second), Err(Refused::Looped));
    }

    /// A property list that grew by one on every pass would make a
    /// message get bigger the longer a loop ran, which is the one
    /// failure mode worse than the loop.
    #[test]
    fn stamping_twice_adds_one_mark() {
        let marker = marker(Version::V5);
        let mut properties = Vec::new();
        for _ in 0..5 {
            marker.stamp(&mut properties);
        }
        assert_eq!(
            properties.iter().filter(|(name, _)| name == ORIGIN).count(),
            1
        );
    }

    /// Section 9.2: the marker does not exist in 3.1.1, and a bridge
    /// to a 3.1.1 endpoint is exactly the bridge people build. The
    /// configuration refusal is what holds there.
    #[test]
    fn a_3_1_1_bridge_carries_no_marker_and_is_not_broken_by_that() {
        let marker = marker(Version::V311);
        assert!(!marker.available());

        let carried = carry(&marker, 8, &[]).expect("carried");
        assert!(
            !carried.iter().any(|(name, _)| name == ORIGIN),
            "there is nowhere to put it"
        );
        // And the message goes round again, because on 3.1.1 nothing
        // stops it but [`super::topics::check`].
        carry(&marker, 8, &carried).expect("3.1.1 has no marker to find");
    }

    /// Section 9.2's third loop, the one with no bridge in it.
    #[test]
    fn a_message_over_the_hop_limit_is_dropped() {
        let marker = marker(Version::V5);
        let mut properties = hops::originated();
        for hop in 1..4 {
            assert_eq!(hops::count_of(&properties), Some(hop));
            properties = carry(&marker, 4, &properties).expect("under the limit");
            // The mark is this bridge's, so pretend a different one
            // carried each hop.
            properties.retain(|(name, _)| name != ORIGIN);
        }
        assert_eq!(hops::count_of(&properties), Some(4));
        assert_eq!(
            carry(&marker, 4, &properties),
            Err(Refused::TooManyHops {
                hops: 4,
                maximum: 4
            })
        );
    }

    /// A message a client published has no hop count, which is not the
    /// same as a count of zero, and a bridge is its first hop.
    #[test]
    fn a_message_from_a_client_starts_at_one_hop() {
        let marker = marker(Version::V5);
        assert_eq!(hops::count_of(&[]), None);
        let carried = carry(&marker, 8, &[]).expect("carried");
        assert_eq!(hops::count_of(&carried), Some(1));
    }

    /// The count is replaced rather than appended to. Two `sd-hops`
    /// properties would make the count whichever one a reader found
    /// first.
    #[test]
    fn the_hop_count_is_replaced_and_not_repeated() {
        let marker = marker(Version::V5);
        let carried = carry(&marker, 8, &hops::originated()).expect("carried");
        assert_eq!(
            carried
                .iter()
                .filter(|(name, _)| name == hops::HOP_COUNT)
                .count(),
            1
        );
        assert_eq!(hops::count_of(&carried), Some(2));
    }

    /// An application's own properties travel with the message. A
    /// bridge that dropped them would silently change what a
    /// subscriber on the far side receives.
    #[test]
    fn an_application_s_own_properties_are_carried_through() {
        let marker = marker(Version::V5);
        let carried =
            carry(&marker, 8, &[("unit".to_owned(), "celsius".to_owned())]).expect("carried");
        assert!(carried
            .iter()
            .any(|(name, value)| name == "unit" && value == "celsius"));
    }
}
