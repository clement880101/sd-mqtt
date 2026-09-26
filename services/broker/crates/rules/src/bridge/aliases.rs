//! Topic aliases the remote sends this bridge.
//!
//! Item 7, and it exists **because of the CONNECT this bridge sends**.
//! Advertising a Topic Alias Maximum above zero licenses a compliant
//! server to start using aliases at any moment, and it does not have to
//! ask first: [MQTT-3.3.2-9] and the surrounding text make an alias a
//! unilateral saving the sender may take. So a bridge that advertises a
//! maximum and then cannot resolve an alias is a bridge that stops
//! understanding its own traffic the day it is pointed at a broker
//! which bothers to optimise.
//!
//! The shape is small and every part of it is a protocol error if it
//! is wrong:
//!
//! - A publish with a **topic and an alias** sets or replaces the
//!   mapping, and the topic is the one on the wire.
//! - A publish with an **empty topic and an alias** uses the mapping.
//!   An alias with no mapping is a protocol error, because there is
//!   nothing else it could mean.
//! - An alias of **zero, or above the maximum this end advertised**,
//!   is a protocol error. [MQTT-3.3.2-8]: "A sender MUST NOT send a
//!   Topic Alias […] greater than the Topic Alias Maximum value."
//! - The table **lasts exactly as long as the network connection**.
//!   [MQTT-3.3.2-7]: "Topic Alias mappings exist only within a Network
//!   Connection and last only for the lifetime of that Network
//!   Connection." Carrying one across a reconnection would resolve an
//!   alias to whatever the *previous* connection happened to have put
//!   there, and publish somebody's data to the wrong topic.
//!
//! # Outbound aliasing is deferred, and that is safe by construction
//!
//! The remote's Topic Alias Maximum licenses *this* end to use aliases
//! when sending, and this bridge does not: it sends every publish with
//! its full topic. That is always legal — an alias is an optimisation
//! a sender may decline — and it is a deferral rather than an
//! oversight, so convention 8 wants it said out loud here. What would
//! make it worth doing is a bridge whose topics are long and whose link
//! is narrow, and neither is measured yet.

use mqtt_codec::packet::Publish;
use mqtt_codec::ReasonCode;

/// The aliases the remote has established on this connection.
///
/// Empty at the start of every one of them.
pub struct Aliases {
    /// Indexed by alias minus one, because aliases start at one.
    /// `None` for an alias the remote has not bound.
    table: Vec<Option<String>>,
}

impl Aliases {
    /// A table for the maximum this end advertised.
    ///
    /// Allocated up front because the maximum is what this end chose
    /// and is small; a remote cannot make it grow.
    pub fn new(maximum: u16) -> Self {
        Self {
            table: vec![None; usize::from(maximum)],
        }
    }

    /// Forget every mapping, because the network connection ended.
    pub fn reset(&mut self) {
        self.table.fill(None);
    }

    /// How many aliases the remote has bound.
    pub fn bound(&self) -> usize {
        self.table.iter().filter(|slot| slot.is_some()).count()
    }

    /// The topic this publish is really on.
    ///
    /// Takes the alias out of the properties as it goes, because the
    /// alias belongs to the hop it arrived on: republishing it into the
    /// local broker would attach a number that means nothing there and
    /// would be carried on to a subscriber as though it did.
    ///
    /// # Errors
    ///
    /// The reason code to close the connection with. Every failure
    /// here is a protocol error rather than something to skip: an
    /// unresolvable alias is a message whose topic this end does not
    /// know, and guessing is worse than stopping.
    pub fn resolve(&mut self, publish: &mut Publish) -> Result<String, ReasonCode> {
        let Some(alias) = publish.properties.topic_alias.take() else {
            if publish.topic.is_empty() {
                // No alias and no topic is a publish that names
                // nowhere. [MQTT-3.3.2-1] requires a topic name unless
                // an alias supplies one.
                return Err(ReasonCode::ProtocolError);
            }
            return Ok(publish.topic.clone());
        };

        // [MQTT-3.3.2-8]. Zero is never valid, and above the maximum
        // this end advertised is the remote using a number it was
        // never given.
        if alias == 0 || usize::from(alias) > self.table.len() {
            return Err(ReasonCode::TopicAliasInvalid);
        }
        let slot = usize::from(alias) - 1;

        if publish.topic.is_empty() {
            return self.table[slot]
                .clone()
                // An alias the remote never bound. There is nothing
                // else it could mean, so it is a protocol error rather
                // than a message to drop.
                .ok_or(ReasonCode::ProtocolError);
        }

        // A topic and an alias together set the mapping, and replace
        // whatever was there: [MQTT-3.3.2-x] lets a sender reuse an
        // alias for a different topic at any time.
        self.table[slot] = Some(publish.topic.clone());
        Ok(publish.topic.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mqtt_codec::{Properties, QoS};

    fn publish(topic: &str, alias: Option<u16>) -> Publish {
        Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: topic.into(),
            packet_id: None,
            payload: Bytes::from_static(b"x"),
            properties: Properties {
                topic_alias: alias,
                ..Properties::default()
            },
        }
    }

    #[test]
    fn a_topic_with_an_alias_binds_it_and_a_bare_alias_uses_it() {
        let mut aliases = Aliases::new(4);
        let mut first = publish("sensors/kitchen/temp", Some(1));
        assert_eq!(
            aliases.resolve(&mut first).expect("bound"),
            "sensors/kitchen/temp"
        );
        assert_eq!(aliases.bound(), 1);

        let mut later = publish("", Some(1));
        assert_eq!(
            aliases.resolve(&mut later).expect("resolved"),
            "sensors/kitchen/temp",
            "this is the whole point: the remote sent no topic at all"
        );
    }

    /// The alias belongs to the hop it arrived on. Carrying it into
    /// the local broker would attach a number that means nothing there
    /// and hand it to a subscriber as though it did.
    #[test]
    fn the_alias_does_not_travel_on() {
        let mut aliases = Aliases::new(4);
        let mut publish = publish("a/b", Some(2));
        aliases.resolve(&mut publish).expect("bound");
        assert_eq!(publish.properties.topic_alias, None);
    }

    #[test]
    fn a_sender_may_rebind_an_alias_to_a_different_topic() {
        let mut aliases = Aliases::new(2);
        let mut first = publish("a/b", Some(1));
        aliases.resolve(&mut first).expect("bound");
        let mut second = publish("c/d", Some(1));
        assert_eq!(aliases.resolve(&mut second).expect("rebound"), "c/d");

        let mut bare = publish("", Some(1));
        assert_eq!(aliases.resolve(&mut bare).expect("resolved"), "c/d");
        assert_eq!(aliases.bound(), 1);
    }

    /// [MQTT-3.3.2-8]: a sender must not use an alias above the
    /// maximum this end advertised, and zero is never one.
    #[test]
    fn an_alias_outside_what_we_advertised_is_a_protocol_error() {
        let mut aliases = Aliases::new(2);
        for alias in [0u16, 3, u16::MAX] {
            let mut publish = publish("a/b", Some(alias));
            assert_eq!(
                aliases.resolve(&mut publish),
                Err(ReasonCode::TopicAliasInvalid),
                "alias {alias}"
            );
        }
    }

    /// A bridge that advertised no aliases must refuse every one,
    /// which is what makes advertising zero a safe configuration
    /// rather than a silent invitation.
    #[test]
    fn a_bridge_that_advertised_none_refuses_every_alias() {
        let mut aliases = Aliases::new(0);
        let mut publish = publish("a/b", Some(1));
        assert_eq!(
            aliases.resolve(&mut publish),
            Err(ReasonCode::TopicAliasInvalid)
        );
    }

    #[test]
    fn an_alias_the_remote_never_bound_is_a_protocol_error() {
        let mut aliases = Aliases::new(4);
        let mut publish = publish("", Some(2));
        assert_eq!(
            aliases.resolve(&mut publish),
            Err(ReasonCode::ProtocolError)
        );
    }

    #[test]
    fn a_publish_with_neither_a_topic_nor_an_alias_names_nowhere() {
        let mut aliases = Aliases::new(4);
        let mut publish = publish("", None);
        assert_eq!(
            aliases.resolve(&mut publish),
            Err(ReasonCode::ProtocolError)
        );
    }

    /// [MQTT-3.3.2-7]. Carrying a mapping across a reconnection would
    /// resolve an alias to whatever the *previous* connection put
    /// there, and publish somebody's data to the wrong topic — which
    /// no test on a single connection can see.
    #[test]
    fn a_mapping_does_not_survive_the_connection_it_was_made_on() {
        let mut aliases = Aliases::new(4);
        let mut bound = publish("a/b", Some(1));
        aliases.resolve(&mut bound).expect("bound");

        aliases.reset();
        assert_eq!(aliases.bound(), 0);

        let mut bare = publish("", Some(1));
        assert_eq!(
            aliases.resolve(&mut bare),
            Err(ReasonCode::ProtocolError),
            "the new connection has bound nothing"
        );
    }

    #[test]
    fn a_publish_with_a_topic_and_no_alias_is_left_alone() {
        let mut aliases = Aliases::new(4);
        let mut publish = publish("a/b", None);
        assert_eq!(aliases.resolve(&mut publish).expect("plain"), "a/b");
        assert_eq!(aliases.bound(), 0);
    }
}
