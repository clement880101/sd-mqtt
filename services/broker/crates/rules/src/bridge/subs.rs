//! Subscribing on the remote, and reading the answer.
//!
//! Item 8, and it is on the list because of what happens when it is
//! left out: a SUBACK whose reason codes are not read is a bridge
//! sitting on a live, healthy connection, pinging happily, receiving
//! nothing, with nothing anywhere saying why.
//!
//! Three things, and the second is the one that is easy to forget.
//!
//! **Correlate the acknowledgement.** A SUBACK names the packet
//! identifier of the SUBSCRIBE it answers, and carries one reason code
//! per filter **in the order requested** ([MQTT-3.9.3-1]). Reading
//! them without remembering what was asked gives a list of codes with
//! nothing to attach them to.
//!
//! **Resubscribe when the remote reports no session.** A CONNACK with
//! Session Present false means the remote has no record of us, and
//! therefore none of our subscriptions — [MQTT-3.2.2-x] — whatever we
//! asked for with clean start. This is the failure that looks like
//! nothing at all: the connection is up, the keep-alive is answered,
//! and no message ever arrives.
//!
//! **Surface a per-filter failure as a rule error.** A SUBACK can
//! grant some filters and refuse others, and a bridge that looked only
//! at the packet is a bridge half of whose configuration silently does
//! not apply. It can also grant a *lower* level than was asked for,
//! which is not a failure and is still something an operator wants to
//! know, because the promise their configuration makes has been
//! quietly weakened.

use mqtt_codec::packet::{SubAck, Subscribe, SubscriptionOptions};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode, Version};

/// What one filter's acknowledgement said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Granted {
    /// The filter, as it was asked for.
    pub filter: String,
    /// The level asked for.
    pub wanted: QoS,
    /// The level granted, for a filter that was granted one.
    pub granted: Option<QoS>,
    /// What the remote said.
    pub reason: ReasonCode,
}

impl Granted {
    /// Whether this filter is subscribed at all.
    pub fn accepted(&self) -> bool {
        self.granted.is_some()
    }

    /// Whether the remote granted less than was asked for.
    ///
    /// Not a failure, and still worth a line: the operator's
    /// configuration promises a level the link will not carry.
    pub fn downgraded(&self) -> bool {
        self.granted.is_some_and(|granted| granted < self.wanted)
    }
}

/// Why a subscribe could not be completed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SubscribeError {
    /// The remote acknowledged a request that was never sent.
    #[error("the remote sent a suback for packet {packet_id}, which was not outstanding")]
    Unknown {
        /// The identifier it named.
        packet_id: u16,
    },
    /// The remote answered a different number of filters than were
    /// asked about.
    ///
    /// [MQTT-3.9.3-2]: the count must match. Reading the codes
    /// positionally after a mismatch would attach the wrong outcome to
    /// the wrong filter, which is worse than not reading them.
    #[error(
        "the remote acknowledged {answered} filter(s) for a subscribe that asked about \
         {asked}; the codes cannot be matched to the filters"
    )]
    Mismatched {
        /// How many were asked about.
        asked: usize,
        /// How many were answered.
        answered: usize,
    },
}

/// The subscribe requests this bridge has outstanding.
pub struct Subscriptions {
    /// What this bridge wants, in configuration order.
    wanted: Vec<(String, QoS)>,
    /// What was asked for, against the identifier that will answer it.
    outstanding: Vec<(u16, Vec<(String, QoS)>)>,
    /// The next identifier to use.
    ///
    /// A space of its own from the publish window's: [MQTT-2.2.1-x]
    /// makes one identifier space per direction per connection, and a
    /// SUBSCRIBE and a PUBLISH going the same way may not share a live
    /// number. This bridge keeps them apart the simple way — subscribes
    /// count down from the top of the space while publishes count up
    /// from the bottom — because a bridge has a handful of
    /// subscriptions, made once, and the two cursors cannot meet in any
    /// realistic configuration.
    next_id: u16,
}

/// Where the subscribe cursor starts.
const SUBSCRIBE_ID_TOP: u16 = u16::MAX;

impl Subscriptions {
    /// A bridge's subscriptions, in configuration order.
    pub fn new(wanted: Vec<(String, QoS)>) -> Self {
        Self {
            wanted,
            outstanding: Vec::new(),
            next_id: SUBSCRIBE_ID_TOP,
        }
    }

    /// What this bridge subscribes to.
    pub fn filters(&self) -> Vec<String> {
        self.wanted
            .iter()
            .map(|(filter, _)| filter.clone())
            .collect()
    }

    /// Whether there is anything to subscribe to.
    pub fn is_empty(&self) -> bool {
        self.wanted.is_empty()
    }

    /// Forget what a previous connection had outstanding.
    pub fn reset(&mut self) {
        self.outstanding.clear();
        self.next_id = SUBSCRIBE_ID_TOP;
    }

    /// The subscribe to send on a new connection, if one is needed.
    ///
    /// `session_present` is the flag the remote sent in its CONNACK.
    /// **False means the remote has no record of us and therefore none
    /// of our subscriptions**, so they go again. True means they are
    /// still there and resending them would only cost a round trip and
    /// a fresh burst of retained messages.
    ///
    /// The whole set in one packet, so one acknowledgement answers the
    /// whole configuration and a partial failure is visible as a
    /// partial failure rather than as one rule that happened to fail.
    pub fn on_connected(&mut self, session_present: bool) -> Option<Packet> {
        self.reset();
        if self.wanted.is_empty() || session_present {
            return None;
        }
        let packet_id = self.take_id();
        let asked = self.wanted.clone();
        self.outstanding.push((packet_id, asked.clone()));
        Some(Packet::Subscribe(Subscribe {
            packet_id,
            filters: asked
                .into_iter()
                .map(|(filter, qos)| SubscriptionOptions {
                    filter,
                    qos,
                    // **Not set, and that is deliberate.** No-local
                    // would look like the loop prevention this bridge
                    // needs and is not: it suppresses a *session's own*
                    // publications, and the messages this bridge must
                    // not take back are ones it published through a
                    // different session on a different broker. Section
                    // 9.2's marker is the mechanism; see
                    // [`super::marker`].
                    no_local: false,
                    // The remote's retain flag is carried through
                    // rather than cleared, so a retained message on the
                    // far side arrives here still marked retained and
                    // can be retained here too. Without this a bridge
                    // turns every retained message into an ordinary
                    // one and the local broker's retained set never
                    // fills.
                    retain_as_published: true,
                    retain_handling: mqtt_codec::packet::RetainHandling::SendAlways,
                })
                .collect(),
            properties: Properties::default(),
        }))
    }

    /// Read a SUBACK against the request it answers.
    ///
    /// # Errors
    ///
    /// [`SubscribeError`] when nothing was waiting on that identifier,
    /// or when the remote answered a different number of filters than
    /// were asked about.
    pub fn on_suback(
        &mut self,
        suback: &SubAck,
        version: Version,
    ) -> Result<Vec<Granted>, SubscribeError> {
        let index = self
            .outstanding
            .iter()
            .position(|(packet_id, _)| *packet_id == suback.packet_id)
            .ok_or(SubscribeError::Unknown {
                packet_id: suback.packet_id,
            })?;
        let (_, asked) = self.outstanding.remove(index);

        if asked.len() != suback.reasons.len() {
            return Err(SubscribeError::Mismatched {
                asked: asked.len(),
                answered: suback.reasons.len(),
            });
        }

        Ok(asked
            .into_iter()
            .zip(suback.reasons.iter().copied())
            .map(|((filter, wanted), reason)| Granted {
                filter,
                wanted,
                granted: granted_qos(reason, version),
                reason,
            })
            .collect())
    }

    /// The next subscribe identifier, counting down.
    fn take_id(&mut self) -> u16 {
        let id = self.next_id;
        self.next_id = if self.next_id <= 1 {
            SUBSCRIBE_ID_TOP
        } else {
            self.next_id - 1
        };
        id
    }
}

/// The level a reason code grants, if it grants one.
///
/// The three success codes are the three levels, and everything at
/// 0x80 or above is a refusal. 3.1.1's codes are the same three plus
/// 0x80, and the codec has already mapped them onto these, so one arm
/// serves both versions; `version` is taken so that a caller cannot
/// forget which it is reading and so a later divergence has somewhere
/// to go.
fn granted_qos(reason: ReasonCode, version: Version) -> Option<QoS> {
    let _ = version;
    match reason {
        // 0x00 is both "success" and "granted QoS 0"; the codec names
        // the byte rather than the meaning, and a suback is the one
        // packet where that byte means a level.
        ReasonCode::Success => Some(QoS::AtMostOnce),
        ReasonCode::GrantedQos1 => Some(QoS::AtLeastOnce),
        ReasonCode::GrantedQos2 => Some(QoS::ExactlyOnce),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscriptions() -> Subscriptions {
        Subscriptions::new(vec![
            ("site/+/temp".to_owned(), QoS::AtLeastOnce),
            ("site/alarms".to_owned(), QoS::ExactlyOnce),
        ])
    }

    fn suback(packet_id: u16, reasons: Vec<ReasonCode>) -> SubAck {
        SubAck {
            packet_id,
            reasons,
            properties: Properties::default(),
        }
    }

    fn sent(packet: Packet) -> Subscribe {
        match packet {
            Packet::Subscribe(subscribe) => subscribe,
            other => panic!("expected a subscribe, got {other:?}"),
        }
    }

    /// The failure that looks like nothing at all: a live connection,
    /// an answered keep-alive, and no message ever arriving.
    #[test]
    fn a_remote_that_reports_no_session_is_subscribed_again() {
        let mut subs = subscriptions();
        let subscribe = sent(subs.on_connected(false).expect("no session, so subscribe"));
        assert_eq!(
            subscribe
                .filters
                .iter()
                .map(|f| f.filter.as_str())
                .collect::<Vec<_>>(),
            vec!["site/+/temp", "site/alarms"]
        );
        assert_eq!(subscribe.filters[0].qos, QoS::AtLeastOnce);
        assert_eq!(subscribe.filters[1].qos, QoS::ExactlyOnce);
    }

    #[test]
    fn a_resumed_session_still_holds_the_subscriptions() {
        let mut subs = subscriptions();
        assert!(
            subs.on_connected(true).is_none(),
            "they are still there; resending costs a round trip and a burst of retained \
             messages"
        );
    }

    #[test]
    fn a_bridge_with_no_inbound_filters_subscribes_to_nothing() {
        let mut subs = Subscriptions::new(Vec::new());
        assert!(subs.is_empty());
        assert!(subs.on_connected(false).is_none());
    }

    /// One acknowledgement, one outcome per filter, in the order
    /// asked. Reading the codes without remembering what was asked
    /// gives a list with nothing to attach it to.
    #[test]
    fn each_reason_code_is_matched_to_the_filter_it_answers() {
        let mut subs = subscriptions();
        let subscribe = sent(subs.on_connected(false).expect("subscribe"));
        let granted = subs
            .on_suback(
                &suback(
                    subscribe.packet_id,
                    vec![ReasonCode::GrantedQos1, ReasonCode::NotAuthorized],
                ),
                Version::V5,
            )
            .expect("matched");

        assert_eq!(granted.len(), 2);
        assert_eq!(granted[0].filter, "site/+/temp");
        assert_eq!(granted[0].granted, Some(QoS::AtLeastOnce));
        assert!(granted[0].accepted());
        assert!(!granted[0].downgraded());

        assert_eq!(granted[1].filter, "site/alarms");
        assert_eq!(granted[1].granted, None);
        assert_eq!(granted[1].reason, ReasonCode::NotAuthorized);
        assert!(
            !granted[1].accepted(),
            "half this bridge's configuration does not apply, and something must say so"
        );
    }

    /// Not a failure, and still something an operator wants: the
    /// promise their configuration makes has been quietly weakened.
    #[test]
    fn a_level_lower_than_asked_for_is_reported_as_a_downgrade() {
        let mut subs = subscriptions();
        let subscribe = sent(subs.on_connected(false).expect("subscribe"));
        let granted = subs
            .on_suback(
                &suback(
                    subscribe.packet_id,
                    vec![ReasonCode::Success, ReasonCode::GrantedQos2],
                ),
                Version::V5,
            )
            .expect("matched");

        assert!(granted[0].accepted());
        assert!(granted[0].downgraded(), "asked for 1 and given 0");
        assert!(granted[1].accepted());
        assert!(!granted[1].downgraded(), "asked for 2 and given 2");
    }

    #[test]
    fn an_acknowledgement_for_nothing_outstanding_is_an_error() {
        let mut subs = subscriptions();
        assert_eq!(
            subs.on_suback(&suback(7, vec![ReasonCode::Success]), Version::V5),
            Err(SubscribeError::Unknown { packet_id: 7 })
        );
    }

    /// [MQTT-3.9.3-2]. Reading the codes positionally after a mismatch
    /// would attach the wrong outcome to the wrong filter, which is
    /// worse than not reading them at all.
    #[test]
    fn an_answer_of_the_wrong_length_is_refused_rather_than_read() {
        let mut subs = subscriptions();
        let subscribe = sent(subs.on_connected(false).expect("subscribe"));
        assert_eq!(
            subs.on_suback(
                &suback(subscribe.packet_id, vec![ReasonCode::GrantedQos1]),
                Version::V5
            ),
            Err(SubscribeError::Mismatched {
                asked: 2,
                answered: 1
            })
        );
    }

    #[test]
    fn an_acknowledgement_is_matched_once_and_then_forgotten() {
        let mut subs = subscriptions();
        let subscribe = sent(subs.on_connected(false).expect("subscribe"));
        let answer = suback(
            subscribe.packet_id,
            vec![ReasonCode::GrantedQos1, ReasonCode::GrantedQos2],
        );
        subs.on_suback(&answer, Version::V5).expect("first");
        assert_eq!(
            subs.on_suback(&answer, Version::V5),
            Err(SubscribeError::Unknown {
                packet_id: subscribe.packet_id
            })
        );
    }

    /// 3.1.1's return codes are the same three grants and one failure,
    /// and the codec has already mapped them onto the v5 names.
    #[test]
    fn a_3_1_1_suback_reads_the_same_way() {
        let mut subs = subscriptions();
        let subscribe = sent(subs.on_connected(false).expect("subscribe"));
        let granted = subs
            .on_suback(
                &suback(
                    subscribe.packet_id,
                    vec![ReasonCode::Success, ReasonCode::UnspecifiedError],
                ),
                Version::V311,
            )
            .expect("matched");
        assert_eq!(granted[0].granted, Some(QoS::AtMostOnce));
        assert_eq!(granted[1].granted, None);
    }

    /// [MQTT-2.2.1-x]: one identifier space per direction. A subscribe
    /// and a publish going the same way must not share a live number,
    /// and the two cursors here start at opposite ends so they cannot
    /// meet in any configuration a bridge has.
    #[test]
    fn a_subscribe_identifier_does_not_collide_with_a_publish_one() {
        let mut subs = subscriptions();
        let first = sent(subs.on_connected(false).expect("subscribe")).packet_id;
        assert_eq!(
            first,
            u16::MAX,
            "the top of the space, where the publish window's cursor — which starts at \
             one and counts up — will not reach"
        );

        // A second subscribe on the same connection counts down. There
        // is only ever one today, because the whole configuration goes
        // in one packet; the cursor is written anyway so that a later
        // caller which sends two does not send them both under the
        // same number.
        subs.outstanding.clear();
        assert_eq!(subs.take_id(), u16::MAX - 1);

        // A **new connection** starts at the top again, because
        // identifiers belong to a connection and the previous one's
        // are gone with it.
        let reconnected = sent(subs.on_connected(false).expect("subscribe")).packet_id;
        assert_eq!(reconnected, u16::MAX);
    }

    /// The remote's retain flag is carried through rather than
    /// cleared. Without this a bridge turns every retained message
    /// into an ordinary one and the local broker's retained set never
    /// fills.
    #[test]
    fn a_retained_message_arrives_still_marked_retained() {
        let mut subs = subscriptions();
        let subscribe = sent(subs.on_connected(false).expect("subscribe"));
        assert!(subscribe.filters.iter().all(|f| f.retain_as_published));
    }

    /// **And 3.1.1 cannot ask for it.** The option does not exist
    /// there, so the codec drops it on the way to the wire and a
    /// bridge to a 3.1.1 remote receives every retained message with
    /// its flag already cleared. That is the protocol rather than a
    /// defect here, and it is asserted so that the limitation is
    /// written down somewhere a reader will meet it: a 3.1.1 bridge
    /// does not reproduce the far side's retained set.
    #[test]
    fn a_3_1_1_subscribe_cannot_ask_for_retain_as_published() {
        let mut subs = subscriptions();
        let packet = subs.on_connected(false).expect("subscribe");
        let mut out = bytes::BytesMut::new();
        mqtt_codec::encode(&packet, Version::V311, &mut out).expect("encodes");
        let Packet::Subscribe(back) =
            mqtt_codec::decode(&mut out, Version::V311, &mqtt_codec::Limits::default())
                .expect("decodes")
                .expect("a whole frame")
        else {
            panic!("a subscribe");
        };
        assert!(
            back.filters.iter().all(|f| !f.retain_as_published),
            "3.1.1 has no such option, so the flag does not survive the wire"
        );
    }

    /// No-local looks like the loop prevention a bridge needs and is
    /// not: it suppresses a session's own publications, and the
    /// messages this bridge must not take back were published through
    /// a different session on a different broker.
    #[test]
    fn no_local_is_not_set_because_it_is_not_the_loop_prevention() {
        let mut subs = subscriptions();
        let subscribe = sent(subs.on_connected(false).expect("subscribe"));
        assert!(subscribe.filters.iter().all(|f| !f.no_local));
    }

    /// Convention 1: what this bridge composes must be something the
    /// codec will write and read back unchanged.
    #[test]
    fn what_the_bridge_composes_round_trips_through_the_codec() {
        let mut subs = subscriptions();
        let packet = subs.on_connected(false).expect("subscribe");
        let mut out = bytes::BytesMut::new();
        mqtt_codec::encode(&packet, Version::V5, &mut out).expect("encodes");
        let back = mqtt_codec::decode(&mut out, Version::V5, &mqtt_codec::Limits::default())
            .expect("decodes")
            .expect("a whole frame");
        assert_eq!(back, packet);
    }
}
