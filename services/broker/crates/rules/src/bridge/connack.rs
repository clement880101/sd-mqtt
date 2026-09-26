//! Composing a CONNECT, and honouring the CONNACK that answers it.
//!
//! **This is the part with no equivalent anywhere else in this
//! codebase.** The broker decodes a CONNECT and writes a CONNACK;
//! nothing in it has ever had to read one and obey it. The research
//! notes say so, and the plan puts it third on the list for the same
//! reason: every limit a remote declares here is one it will
//! disconnect for, and the symptom of getting it wrong is a bridge
//! that works against a permissive broker and fails against a strict
//! one — which is to say, one that passes every test we write and
//! fails at a customer.
//!
//! So each limit below carries the clause of MQTT-v5.0 that makes it a
//! requirement, and the module test for each one says what the remote
//! does if we ignore it.
//!
//! # What the remote declares, and what happens if we ignore it
//!
//! | Declared | Absent means | Ignoring it |
//! |---|---|---|
//! | Server Keep Alive, 0x13 | ours stands | [MQTT-3.1.2-21] makes theirs binding; ours is longer, so they time us out and close |
//! | Receive Maximum, 0x21 | 65535 | [MQTT-3.3.4-7]: exceeding it is a protocol error and a disconnect |
//! | Maximum Packet Size, 0x27 | no limit | [MQTT-3.2.2-15]: a larger packet is a protocol error and a disconnect |
//! | Maximum QoS, 0x24 | 2 | [MQTT-3.2.2-11]: a higher-level publish is a protocol error and a disconnect |
//! | Retain Available, 0x25 | available | [MQTT-3.2.2-14]: a retained publish closes the connection |
//! | Wildcard Available, 0x28 | available | [MQTT-3.2.2-22]: a wildcard subscribe closes the connection |
//! | Topic Alias Maximum, 0x22 | 0 | [MQTT-3.3.2-9]: an alias above it is a protocol error |
//! | Assigned Client Identifier, 0x12 | we keep ours | our session is somebody else's next time we connect |
//!
//! Every one of those is a **close**, not a refusal packet, which is
//! what makes them worth a module: a bridge that gets one wrong does
//! not fail visibly, it reconnects for ever.
//!
//! # 3.1.1 declares none of it
//!
//! A 3.1.1 CONNACK carries a session-present flag and a return code and
//! nothing else, so [`negotiate`] answers every question from the
//! defaults and from what was asked for. That is not a degraded path
//! to be apologised for; it is the protocol, and the one place it
//! bites is the client identifier, which 3.1.1 cannot assign — so an
//! empty one is refused when the configuration is read rather than
//! discovered on the wire.

use std::time::Duration;

use mqtt_codec::packet::{ConnAck, Connect, Will};
use mqtt_codec::{Properties, QoS, ReasonCode, Version};

use crate::rule::MqttBridge;

/// What the bridge asked for, so that the answer can be read against
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asked {
    /// The identifier that was sent, possibly empty.
    pub client_id: String,
    /// The keep-alive that was proposed, in seconds.
    pub keep_alive_secs: u16,
    /// The in-flight window this bridge is willing to hold open toward
    /// the remote.
    ///
    /// A bound of prudence rather than one the protocol asks for: the
    /// remote's declaration is a ceiling this end may not exceed, and
    /// this is a floor below it that keeps a bridge from holding
    /// sixty-five thousand messages in memory because a remote said it
    /// could. Lowering it is always safe; it can never raise the
    /// window above what the remote allowed.
    pub receive_maximum: u16,
    /// Which version was spoken.
    pub version: Version,
}

/// What the remote said, after the defaults have been applied.
///
/// Every field is a decision, not a copy: an absent property has a
/// default the specification names, and that default is applied here
/// once rather than at each of the places that would otherwise have to
/// remember it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Negotiated {
    /// Whether the remote resumed a session for us.
    ///
    /// False after a clean start, and false after a remote that lost
    /// ours. The second is why this is carried rather than assumed:
    /// [`crate::bridge::subs`] resubscribes when it is false, and a
    /// bridge that skipped that would sit on a live connection
    /// receiving nothing.
    pub session_present: bool,
    /// The identifier this connection is known by.
    ///
    /// The one the remote assigned if it assigned one, and ours
    /// otherwise. [MQTT-3.2.2-16]: a server that assigns an identifier
    /// must say so here, and the session it keeps for us is under that
    /// name — so a bridge that ignored it and reconnected with an
    /// empty identifier would be given a *new* session every time and
    /// would never resume anything.
    pub client_id: String,
    /// How often to ping when nothing else is going out.
    ///
    /// [MQTT-3.1.2-21]: a server that sends a Server Keep Alive
    /// replaces the value the client sent, and the client must use
    /// theirs. `None` means no keep-alive at all, which is what a
    /// value of zero means on either side.
    pub keep_alive: Option<Duration>,
    /// How many acknowledged publications may be outstanding toward
    /// the remote at once.
    ///
    /// Never zero: [MQTT-3.2.2-x] makes a declared zero a protocol
    /// error, and a window of zero would be a bridge that can send
    /// nothing.
    pub send_quota: u16,
    /// The largest packet the remote will accept, if it named one.
    pub maximum_packet_size: Option<u32>,
    /// The highest level the remote accepts on a publish.
    pub maximum_qos: QoS,
    /// Whether a publish may be retained.
    pub retain_available: bool,
    /// Whether a subscribe may carry a wildcard.
    pub wildcard_available: bool,
    /// The highest topic alias this end may use when sending.
    ///
    /// Zero means none, which is both the default and what this bridge
    /// uses: see [`crate::bridge::aliases`] for why outbound aliasing
    /// is deferred and inbound is not.
    pub outbound_alias_maximum: u16,
    /// How long the remote says it will keep the session, if it said.
    pub session_expiry_secs: Option<u32>,
}

/// Why a connection attempt did not produce a session.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConnAckError {
    /// The remote refused the connection.
    ///
    /// **Not retried faster for being a refusal.** A remote that says
    /// "not authorized" every second is a remote being asked a
    /// question it has answered, so the backoff applies to this exactly
    /// as it applies to a socket that would not open.
    #[error("the remote refused the connection: {reason:?}{}", server_reference.as_ref().map_or(String::new(), |r| format!(", referring us to {r}")))]
    Refused {
        /// What it said.
        reason: ReasonCode,
        /// Where it said to go instead, if it said.
        server_reference: Option<String>,
    },
    /// The remote assigned no identifier and we sent none.
    ///
    /// [MQTT-3.2.2-16]: a server that accepts a zero-length client
    /// identifier must assign one and return it. One that does not has
    /// left us with no name to reconnect under, and a bridge that
    /// carried on would get a fresh session on every reconnection and
    /// silently lose whatever the old one held.
    #[error(
        "the remote accepted a connection with no client identifier and assigned none, \
         which MQTT-3.2.2-16 requires it to do. There is no name to resume this session \
         under; set mqtt_bridge.client_id."
    )]
    NoAssignedIdentifier,
    /// The remote declared a limit the specification forbids.
    #[error("the remote's connack is not one MQTT 5 permits: {0}")]
    Malformed(&'static str),
    /// The remote will not accept the subscriptions this bridge needs.
    #[error(
        "the remote declares that wildcard subscriptions are not available, and this \
         bridge subscribes to {filter:?}. There is nothing to retry: either the remote's \
         configuration changes or this bridge's inbound filters lose their wildcards."
    )]
    NoWildcards {
        /// The first filter that cannot be sent.
        filter: String,
    },
}

/// The CONNECT this bridge sends.
///
/// Every field section 9.2 asks for: identifier, clean start or session
/// expiry, keep-alive, credentials, the will that supplies the state
/// topic's offline value, and the v5 properties for receive maximum,
/// maximum packet size and topic alias maximum.
///
/// `client_id` is passed rather than read from `action`, because a
/// reconnection uses the identifier the remote assigned rather than the
/// empty one that was configured. See [`Negotiated::client_id`].
pub fn connect(action: &MqttBridge, client_id: &str) -> Connect {
    let version = action.version;
    let mut properties = Properties::default();
    if version == Version::V5 {
        // **Clean start and session expiry are different questions in
        // 5 and the same one in 3.1.1.** A clean start with an expiry
        // is a session that begins empty and outlives the connection,
        // which is exactly what a bridge that has just been
        // reconfigured wants. Only the expiry is a property; clean
        // start is a flag on both versions.
        properties.session_expiry_interval = Some(action.session_expiry_secs);
        // What **this end** will accept at once, which is the mirror of
        // the number the remote declares back. Zero is a protocol
        // error, so a configuration of zero is refused before here;
        // see [`check`].
        properties.receive_maximum = Some(action.receive_maximum);
        properties.maximum_packet_size = Some(action.maximum_packet_size);
        // What **this end** will accept, which is what licenses the
        // remote to send an alias at all. Advertising it is what makes
        // [`crate::bridge::aliases`] necessary rather than optional:
        // a compliant server may use aliases the moment this number is
        // above zero.
        properties.topic_alias_maximum = Some(action.topic_alias_maximum);
    }

    Connect {
        version,
        client_id: client_id.to_owned(),
        clean_start: action.clean_start,
        keep_alive: action.keep_alive_secs,
        username: action.username.clone(),
        password: action
            .password
            .as_ref()
            .map(|secret| bytes::Bytes::copy_from_slice(secret.expose().as_bytes())),
        will: will(action),
        properties,
    }
}

/// The will that supplies the state topic's offline value.
///
/// Section 9.2. Retained, because the point of it is that somebody who
/// subscribes *after* the bridge died still learns that it did; a will
/// that is not retained tells only whoever was already listening.
///
/// At least-once, because a liveness message that the remote may drop
/// is a liveness message that can leave the far end believing a dead
/// bridge is alive for ever.
fn will(action: &MqttBridge) -> Option<Will> {
    if action.state_topic.is_empty() {
        return None;
    }
    Some(Will {
        topic: action.state_topic.clone(),
        payload: bytes::Bytes::from_static(OFFLINE),
        qos: QoS::AtLeastOnce,
        retain: true,
        properties: Properties::default(),
    })
}

/// What the state topic carries while the bridge is connected.
pub const ONLINE: &[u8] = b"online";
/// What it carries otherwise, and what the will publishes.
pub const OFFLINE: &[u8] = b"offline";

/// Read a CONNACK and decide what this connection may do.
///
/// # Errors
///
/// [`ConnAckError`]: the remote refused, or declared something MQTT 5
/// does not permit, or left us without an identifier, or will not take
/// the subscriptions this bridge needs.
pub fn negotiate(
    connack: &ConnAck,
    asked: &Asked,
    inbound_filters: &[String],
) -> Result<Negotiated, ConnAckError> {
    if connack.reason.is_failure() {
        return Err(ConnAckError::Refused {
            reason: connack.reason,
            server_reference: connack.properties.server_reference.clone(),
        });
    }

    // A 3.1.1 connack carries no property block at all, so the codec
    // hands over the empty one and every default below applies. That is
    // the protocol rather than a shortcut, and it is why this function
    // has no version arm: the defaults *are* 3.1.1's behaviour.
    let properties = &connack.properties;

    if properties.receive_maximum == Some(0) {
        return Err(ConnAckError::Malformed(
            "a receive maximum of zero is a protocol error, and would be a window \
             through which nothing could be sent",
        ));
    }
    if properties.maximum_packet_size == Some(0) {
        return Err(ConnAckError::Malformed(
            "a maximum packet size of zero is a protocol error, and would refuse every \
             packet including the disconnect saying so",
        ));
    }
    let maximum_qos = match properties.maximum_qos {
        None => QoS::ExactlyOnce,
        Some(0) => QoS::AtMostOnce,
        Some(1) => QoS::AtLeastOnce,
        // [MQTT-3.2.2-x]: the only values are 0 and 1. A server that
        // supports exactly-once omits the property rather than sending
        // 2, so a 2 here is a server this end cannot reason about.
        Some(_) => {
            return Err(ConnAckError::Malformed(
                "maximum qos may only be 0 or 1; a server that supports exactly-once \
                 omits the property",
            ))
        }
    };
    let flag = |value: Option<u8>, what: &'static str| match value {
        None | Some(1) => Ok(true),
        Some(0) => Ok(false),
        Some(_) => Err(ConnAckError::Malformed(what)),
    };
    let retain_available = flag(
        properties.retain_available,
        "retain available may only be 0 or 1",
    )?;
    let wildcard_available = flag(
        properties.wildcard_subscription_available,
        "wildcard subscription available may only be 0 or 1",
    )?;

    if !wildcard_available {
        if let Some(filter) = inbound_filters
            .iter()
            .find(|filter| filter.contains(['+', '#']))
        {
            return Err(ConnAckError::NoWildcards {
                filter: filter.clone(),
            });
        }
    }

    let client_id = match &properties.assigned_client_identifier {
        Some(assigned) => assigned.clone(),
        None if asked.client_id.is_empty() => {
            return Err(ConnAckError::NoAssignedIdentifier);
        }
        None => asked.client_id.clone(),
    };

    // [MQTT-3.1.2-21]. Theirs replaces ours outright rather than
    // bounding it: a server may legitimately lengthen a keep-alive as
    // well as shorten one, and a bridge that took the smaller of the
    // two would ping a server that had just asked it not to.
    let keep_alive_secs = properties
        .server_keep_alive
        .unwrap_or(asked.keep_alive_secs);
    let keep_alive = (keep_alive_secs > 0).then(|| Duration::from_secs(u64::from(keep_alive_secs)));

    // The specification's default when the property is absent, which is
    // also what a 3.1.1 server means by saying nothing.
    const DEFAULT_RECEIVE_MAXIMUM: u16 = 65_535;
    let declared = properties
        .receive_maximum
        .unwrap_or(DEFAULT_RECEIVE_MAXIMUM);
    let send_quota = declared.min(asked.receive_maximum.max(1));

    Ok(Negotiated {
        session_present: connack.session_present,
        client_id,
        keep_alive,
        send_quota,
        maximum_packet_size: properties.maximum_packet_size,
        maximum_qos,
        retain_available,
        wildcard_available,
        outbound_alias_maximum: properties.topic_alias_maximum.unwrap_or(0),
        session_expiry_secs: properties.session_expiry_interval,
    })
}

impl Negotiated {
    /// The level a publish may actually go out at.
    ///
    /// [MQTT-3.2.2-11]: a publish above the remote's maximum is a
    /// protocol error it disconnects for. Downgraded rather than
    /// dropped, because a message delivered with a weaker promise is
    /// closer to what the operator asked for than no message at all,
    /// and because the alternative is a bridge that silently carries
    /// nothing against a server which only does at-most-once.
    pub fn allowed_qos(&self, wanted: QoS) -> QoS {
        wanted.min(self.maximum_qos)
    }

    /// Whether a retained publish may keep its flag.
    ///
    /// [MQTT-3.2.2-14]: a retained publish to a server that declared
    /// retain unavailable closes the connection. The flag is cleared
    /// rather than the message dropped, for the reason
    /// [`Self::allowed_qos`] gives: the subscriber still wants the
    /// reading.
    pub fn allowed_retain(&self, wanted: bool) -> bool {
        wanted && self.retain_available
    }

    /// Whether a packet of this size may be sent.
    ///
    /// [MQTT-3.2.2-15]. There is nothing to downgrade here — a message
    /// cannot be made smaller — so the caller drops it and counts it,
    /// which is the whole reason this returns a decision rather than
    /// truncating anything.
    pub fn fits(&self, encoded_len: usize) -> bool {
        self.maximum_packet_size
            .is_none_or(|maximum| encoded_len <= maximum as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule::MqttBridge;
    use mqtt_codec::packet::ConnAck;

    fn asked() -> Asked {
        Asked {
            client_id: "bridge-a".to_owned(),
            keep_alive_secs: 60,
            receive_maximum: 32,
            version: Version::V5,
        }
    }

    fn accepted(properties: Properties) -> ConnAck {
        ConnAck {
            session_present: false,
            reason: ReasonCode::Success,
            properties,
        }
    }

    fn settled(properties: Properties) -> Negotiated {
        negotiate(&accepted(properties), &asked(), &[]).expect("accepted")
    }

    /// [MQTT-3.1.2-21]. If this is ignored the remote times us out on
    /// its own shorter interval and closes the connection, and the
    /// bridge reconnects for ever without ever saying why.
    #[test]
    fn the_remote_s_keep_alive_replaces_the_one_we_proposed() {
        let negotiated = settled(Properties {
            server_keep_alive: Some(10),
            ..Properties::default()
        });
        assert_eq!(negotiated.keep_alive, Some(Duration::from_secs(10)));

        // It may also lengthen it, and a bridge that took the smaller
        // of the two would ping a server that had asked it not to.
        let longer = settled(Properties {
            server_keep_alive: Some(300),
            ..Properties::default()
        });
        assert_eq!(longer.keep_alive, Some(Duration::from_secs(300)));

        // Absent, ours stands.
        assert_eq!(
            settled(Properties::default()).keep_alive,
            Some(Duration::from_secs(60))
        );

        // Zero, on either side, means no keep-alive at all.
        assert_eq!(
            settled(Properties {
                server_keep_alive: Some(0),
                ..Properties::default()
            })
            .keep_alive,
            None
        );
    }

    /// [MQTT-3.3.4-7]. Exceeding it is a protocol error and a
    /// disconnect, so the window this bridge opens is the remote's
    /// number and never its own.
    #[test]
    fn the_remote_s_receive_maximum_bounds_the_window() {
        let narrow = settled(Properties {
            receive_maximum: Some(2),
            ..Properties::default()
        });
        assert_eq!(narrow.send_quota, 2, "the remote's number, not ours");

        // A remote that allows more than this end wants to hold gets
        // this end's number: a ceiling can always be undercut.
        let wide = settled(Properties {
            receive_maximum: Some(10_000),
            ..Properties::default()
        });
        assert_eq!(wide.send_quota, 32);

        // Absent is 65535, which is the specification's default and
        // what a 3.1.1 server means by saying nothing.
        assert_eq!(settled(Properties::default()).send_quota, 32);
    }

    #[test]
    fn a_receive_maximum_of_zero_is_refused() {
        let error = negotiate(
            &accepted(Properties {
                receive_maximum: Some(0),
                ..Properties::default()
            }),
            &asked(),
            &[],
        )
        .expect_err("zero is a protocol error");
        assert!(matches!(error, ConnAckError::Malformed(_)), "{error:?}");
    }

    /// [MQTT-3.2.2-15]. A larger packet is a protocol error and a
    /// disconnect, and there is nothing to downgrade, so the message is
    /// dropped.
    #[test]
    fn a_packet_larger_than_the_remote_accepts_does_not_fit() {
        let bounded = settled(Properties {
            maximum_packet_size: Some(128),
            ..Properties::default()
        });
        assert!(bounded.fits(128), "the limit itself is allowed");
        assert!(!bounded.fits(129));

        let unbounded = settled(Properties::default());
        assert!(unbounded.fits(usize::MAX), "absent means no limit");
    }

    /// [MQTT-3.2.2-11]. A publish above the declared maximum is a
    /// protocol error the remote closes for.
    #[test]
    fn a_publish_is_downgraded_to_the_remote_s_maximum_level() {
        let at_most_once = settled(Properties {
            maximum_qos: Some(0),
            ..Properties::default()
        });
        assert_eq!(at_most_once.allowed_qos(QoS::ExactlyOnce), QoS::AtMostOnce);
        assert_eq!(at_most_once.allowed_qos(QoS::AtLeastOnce), QoS::AtMostOnce);

        let at_least_once = settled(Properties {
            maximum_qos: Some(1),
            ..Properties::default()
        });
        assert_eq!(
            at_least_once.allowed_qos(QoS::ExactlyOnce),
            QoS::AtLeastOnce
        );
        assert_eq!(at_least_once.allowed_qos(QoS::AtMostOnce), QoS::AtMostOnce);

        // Absent is exactly-once, and nothing is downgraded.
        let full = settled(Properties::default());
        assert_eq!(full.allowed_qos(QoS::ExactlyOnce), QoS::ExactlyOnce);
    }

    /// [MQTT-3.2.2-14]. A retained publish to a remote that declared
    /// retain unavailable closes the connection.
    #[test]
    fn a_retained_publish_loses_its_flag_when_retain_is_unavailable() {
        let without = settled(Properties {
            retain_available: Some(0),
            ..Properties::default()
        });
        assert!(!without.allowed_retain(true));
        assert!(!without.allowed_retain(false));

        let with = settled(Properties::default());
        assert!(with.allowed_retain(true), "absent means available");
        assert!(!with.allowed_retain(false));
    }

    /// [MQTT-3.2.2-22]. A subscribe carrying a wildcard to a remote
    /// that declared them unavailable closes the connection — so the
    /// bridge does not connect at all rather than connecting and being
    /// hung up on.
    #[test]
    fn a_remote_without_wildcards_refuses_a_bridge_that_needs_them() {
        let properties = Properties {
            wildcard_subscription_available: Some(0),
            ..Properties::default()
        };
        let error = negotiate(
            &accepted(properties.clone()),
            &asked(),
            &["site/+/temp".to_owned()],
        )
        .expect_err("this bridge cannot subscribe");
        assert!(
            matches!(error, ConnAckError::NoWildcards { ref filter } if filter == "site/+/temp"),
            "{error:?}"
        );

        // A bridge whose inbound filters are all literal is fine on the
        // same remote.
        negotiate(
            &accepted(properties),
            &asked(),
            &["site/kitchen/temp".to_owned()],
        )
        .expect("nothing here needs a wildcard");
    }

    /// [MQTT-3.2.2-16]. The session the remote keeps for us is under
    /// the name it assigned, so a bridge that ignored this would be
    /// given a fresh session on every reconnection and would never
    /// resume anything.
    #[test]
    fn an_assigned_identifier_replaces_the_one_we_sent() {
        let mut empty = asked();
        empty.client_id = String::new();
        let negotiated = negotiate(
            &accepted(Properties {
                assigned_client_identifier: Some("auto-4711".to_owned()),
                ..Properties::default()
            }),
            &empty,
            &[],
        )
        .expect("accepted");
        assert_eq!(negotiated.client_id, "auto-4711");

        // And it replaces one we did send, which a server is allowed
        // to do.
        let over = negotiate(
            &accepted(Properties {
                assigned_client_identifier: Some("auto-4711".to_owned()),
                ..Properties::default()
            }),
            &asked(),
            &[],
        )
        .expect("accepted");
        assert_eq!(over.client_id, "auto-4711");
    }

    #[test]
    fn an_empty_identifier_with_nothing_assigned_is_refused() {
        let mut empty = asked();
        empty.client_id = String::new();
        assert_eq!(
            negotiate(&accepted(Properties::default()), &empty, &[]),
            Err(ConnAckError::NoAssignedIdentifier)
        );
    }

    /// [MQTT-3.3.2-9]: an alias above the remote's maximum is a
    /// protocol error. Absent is zero, which is "none at all".
    #[test]
    fn the_remote_s_topic_alias_maximum_is_zero_unless_it_says_otherwise() {
        assert_eq!(settled(Properties::default()).outbound_alias_maximum, 0);
        assert_eq!(
            settled(Properties {
                topic_alias_maximum: Some(8),
                ..Properties::default()
            })
            .outbound_alias_maximum,
            8
        );
    }

    #[test]
    fn a_refusal_is_a_refusal_and_carries_what_the_remote_said() {
        let error = negotiate(
            &ConnAck {
                session_present: false,
                reason: ReasonCode::NotAuthorized,
                properties: Properties {
                    server_reference: Some("other.invalid:1883".to_owned()),
                    ..Properties::default()
                },
            },
            &asked(),
            &[],
        )
        .expect_err("refused");
        assert_eq!(
            error,
            ConnAckError::Refused {
                reason: ReasonCode::NotAuthorized,
                server_reference: Some("other.invalid:1883".to_owned()),
            }
        );
    }

    /// A 3.1.1 connack has no properties at all, so every default
    /// applies. This is the protocol, not a degraded path.
    #[test]
    fn a_3_1_1_connack_is_read_entirely_from_the_defaults() {
        let mut old = asked();
        old.version = Version::V311;
        let negotiated = negotiate(
            &ConnAck {
                session_present: true,
                reason: ReasonCode::Success,
                properties: Properties::default(),
            },
            &old,
            &["a/#".to_owned()],
        )
        .expect("accepted");

        assert!(negotiated.session_present);
        assert_eq!(negotiated.client_id, "bridge-a");
        assert_eq!(negotiated.keep_alive, Some(Duration::from_secs(60)));
        assert_eq!(negotiated.send_quota, 32);
        assert_eq!(negotiated.maximum_packet_size, None);
        assert_eq!(negotiated.maximum_qos, QoS::ExactlyOnce);
        assert!(negotiated.retain_available);
        assert!(negotiated.wildcard_available);
        assert_eq!(negotiated.outbound_alias_maximum, 0);
    }

    #[test]
    fn the_connect_carries_everything_section_9_2_lists() {
        let action = MqttBridge {
            address: "remote.invalid:1883".to_owned(),
            client_id: "bridge-a".to_owned(),
            username: Some("user".to_owned()),
            password: Some(storage::record::Secret::new("pw")),
            keep_alive_secs: 45,
            clean_start: false,
            session_expiry_secs: 600,
            receive_maximum: 16,
            maximum_packet_size: 65_536,
            topic_alias_maximum: 4,
            state_topic: "bridges/a/state".to_owned(),
            ..MqttBridge::default()
        };
        let connect = connect(&action, &action.client_id);

        assert_eq!(connect.client_id, "bridge-a");
        assert_eq!(connect.keep_alive, 45);
        assert!(!connect.clean_start);
        assert_eq!(connect.username.as_deref(), Some("user"));
        assert_eq!(connect.password.as_deref(), Some(&b"pw"[..]));
        assert_eq!(connect.properties.session_expiry_interval, Some(600));
        assert_eq!(connect.properties.receive_maximum, Some(16));
        assert_eq!(connect.properties.maximum_packet_size, Some(65_536));
        assert_eq!(connect.properties.topic_alias_maximum, Some(4));

        let will = connect.will.expect("a state topic means a will");
        assert_eq!(will.topic, "bridges/a/state");
        assert_eq!(&will.payload[..], OFFLINE);
        assert!(
            will.retain,
            "somebody who subscribes after the bridge died must still learn that it did"
        );
        assert_eq!(
            will.qos,
            QoS::AtLeastOnce,
            "a liveness message the remote may drop leaves the far end believing a \
             dead bridge is alive"
        );
    }

    /// The properties are v5's. A 3.1.1 connect carries none, and the
    /// codec would refuse to encode one that did.
    #[test]
    fn a_3_1_1_connect_carries_no_properties() {
        let action = MqttBridge {
            version: Version::V311,
            client_id: "bridge-a".to_owned(),
            session_expiry_secs: 600,
            state_topic: "bridges/a/state".to_owned(),
            ..MqttBridge::default()
        };
        let connect = connect(&action, "bridge-a");
        assert_eq!(connect.properties, Properties::default());

        let mut out = bytes::BytesMut::new();
        mqtt_codec::encode(
            &mqtt_codec::Packet::Connect(Box::new(connect)),
            Version::V311,
            &mut out,
        )
        .expect("a 3.1.1 connect with no properties encodes");
    }

    /// A connect this bridge composes must be one the codec will
    /// encode and one it reads back the same. Convention 1 from the
    /// inside: the peer's decoder is this decoder.
    #[test]
    fn what_the_bridge_composes_round_trips_through_the_codec() {
        for version in [Version::V311, Version::V5] {
            let action = MqttBridge {
                version,
                client_id: "bridge-a".to_owned(),
                username: Some("user".to_owned()),
                password: Some(storage::record::Secret::new("pw")),
                state_topic: "bridges/a/state".to_owned(),
                ..MqttBridge::default()
            };
            let composed = connect(&action, "bridge-a");
            let mut out = bytes::BytesMut::new();
            mqtt_codec::encode(
                &mqtt_codec::Packet::Connect(Box::new(composed.clone())),
                version,
                &mut out,
            )
            .expect("encodes");
            let back = mqtt_codec::decode(&mut out, version, &mqtt_codec::Limits::default())
                .expect("decodes")
                .expect("a whole frame");
            assert_eq!(back, mqtt_codec::Packet::Connect(Box::new(composed)));
        }
    }
}
