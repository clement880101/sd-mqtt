//! A session's lifetime: connect, disconnect, expiry and the will.

use std::sync::Arc;

use bytes::Bytes;
use mqtt_codec::{Properties, QoS, ReasonCode, Version};
use storage::record::{ProtocolVersion, Record, SessionRecord, SubscriptionRecord, WillRecord};

use crate::journal::{self, Journal};
use crate::registry::{Presence, SessionId};
use crate::session::inbound::Inbound;
use crate::session::outbound::Outbound;
use crate::session::queue::{QueueLimits, QueueTotal};
use crate::subscriptions::SubscriberId;

/// The version assumed before a connect has been decoded.
pub const DEFAULT_VERSION: Version = Version::V311;

/// A message published on the client's behalf if it disappears.
#[derive(Debug, Clone)]
pub struct StoredWill {
    /// Where to publish it.
    pub topic: String,
    /// What to publish.
    pub payload: Bytes,
    /// At what level.
    pub qos: QoS,
    /// Whether to retain it.
    pub retain: bool,
    /// Seconds to wait first. MQTT 5 only; a brief outage should not
    /// announce a device as gone.
    pub delay: u32,
    /// The properties the published message carries.
    ///
    /// Section 3.1.3.2 splits the will properties in two: those that
    /// "define the Application Message properties to be sent with the
    /// Will Message when it is published", which are these, and "those
    /// which define when to publish", which is the delay above. The
    /// delay is held separately and must not appear here, because a
    /// PUBLISH does not permit it and the encoder refuses to write one.
    pub properties: Properties,
}

/// One client's session, connected or not.
pub struct Session {
    id: SessionId,
    subscriber: SubscriberId,
    version: Version,
    epoch: u64,
    connected: bool,
    /// The outbound path, which outlives the connection.
    pub outbound: Outbound,
    /// The inbound path.
    pub inbound: Inbound,
    /// Filters this session holds, with the options each was taken
    /// under: teardown needs the names, and a restart needs everything
    /// else, because a subscription rebuilt with default options is a
    /// subscription that silently changed.
    subscriptions: Vec<SubscriptionRecord>,
    will: Option<StoredWill>,
    /// When the will becomes due, if it is owed.
    will_at: Option<u32>,
    expiry_interval: u32,
    /// When the session may be destroyed, once disconnected.
    expires_at: Option<u32>,
    /// What changed durably about the session itself and has not been
    /// handed over. The queue, the window and the inbound identifiers
    /// keep their own; this one is for the fields above.
    journal: Journal,
    /// Where a gained or lost connection is written down.
    ///
    /// Held by the session rather than asked of it, for the reason
    /// the queue's total is: connectedness changes in three methods
    /// here and a caller that has to remember to report each of them
    /// is a caller that will forget one. Free until somebody is
    /// reading; see [`Presence`].
    presence: Presence,
}

/// The configured bounds every session is built with.
///
/// Together rather than one parameter each, because they travel
/// together: the registry holds one of these and hands it to every
/// session it makes, so a bound added later is added in one place.
#[derive(Clone, Debug, Default)]
pub struct SessionBounds {
    /// The configured inflight window, which section 6 makes one half
    /// of the lesser-of pair. The other half is the peer's receive
    /// maximum, which the shell narrows this to once it has read the
    /// connect packet; it is not known when a session is built, and a
    /// session exists before any connection has advertised anything.
    pub max_inflight: usize,
    /// What bounds the queue waiting for one of those slots. Section
    /// 6 calls both bounds configured, and this is the only path a
    /// configured value can reach a session by.
    pub queue: QueueLimits,
    /// The process-wide count of queued messages this session's queue
    /// counts into.
    pub total: QueueTotal,
    /// Where this session records gaining or losing a connection.
    pub presence: Presence,
}

impl Session {
    /// A fresh session, connected.
    pub fn new(
        id: SessionId,
        subscriber: SubscriberId,
        version: Version,
        epoch: u64,
        _now: u32,
        bounds: SessionBounds,
    ) -> Self {
        let SessionBounds {
            max_inflight,
            queue,
            total,
            presence,
        } = bounds;
        // A fresh session has a connection attached, so its outbound path
        // does too. Leaving it offline would make `is_connected` lie and
        // silently discard every at-most-once delivery, which such a path
        // does not queue for an absent client.
        let key = journal::key_of(&id);
        let mut outbound = Outbound::sharing(key.clone(), queue, max_inflight, total);
        outbound.set_online(true);

        presence.record(subscriber, &id, true);
        let mut session = Self {
            id,
            subscriber,
            version,
            epoch,
            connected: true,
            outbound,
            inbound: Inbound::new(key, 0),
            subscriptions: Vec::new(),
            will: None,
            will_at: None,
            expiry_interval: 0,
            expires_at: None,
            journal: Journal::new(),
            presence,
        };
        // A session exists the moment it is made, and nothing else will
        // say so: every record that follows names it, and the store
        // refuses one that names a session it does not hold.
        session.journal_state();
        session
    }

    /// Say what this session's durable fields are now.
    ///
    /// One record covers all of them, because [`Record::SessionPut`] is
    /// an update rather than a reset. Every setter below calls it rather
    /// than leaving the decision to a caller: a durable field changed by
    /// a path that forgot to say so is a field that silently does not
    /// survive a restart, and there is no ordinary test that looks for
    /// the absence of a record.
    pub fn journal_state(&mut self) {
        let record = self.record();
        self.journal.push(Record::SessionPut(record));
    }

    /// This session's durable fields, as they are stored.
    pub fn record(&self) -> SessionRecord {
        SessionRecord {
            key: journal::key_of(&self.id),
            version: match self.version {
                Version::V311 => ProtocolVersion::V311,
                Version::V5 => ProtocolVersion::V5,
            },
            expiry_interval: self.expiry_interval,
            expires_at: self.expires_at,
            will: self.will.as_ref().map(|will| WillRecord {
                topic: will.topic.clone(),
                payload: will.payload.clone(),
                qos: journal::stored_qos(will.qos),
                retain: will.retain,
                delay: will.delay,
                properties: journal::stored_properties(&will.properties),
            }),
            will_at: self.will_at,
        }
    }

    /// Move everything this session has said onto the end of `out`.
    ///
    /// Its own fields first, then what arrived and then what is leaving,
    /// because a record naming a session the store does not hold is
    /// refused and the session's own record is what makes it hold one.
    pub fn take_journal(&mut self, out: &mut Vec<Record>) {
        self.journal.drain_into(out);
        self.inbound.take_journal(out);
        self.outbound.take_journal(out);
    }

    /// Whether anything said has not been handed over.
    ///
    /// For the test that proves every path drains what it changed. A
    /// record left here is one the shell has not committed, and section
    /// 7.3 has the shell committing before it acknowledges.
    pub fn has_unsaid(&self) -> bool {
        !self.journal.is_empty() || self.inbound.has_unsaid() || self.outbound.has_unsaid()
    }

    /// Reclaim an existing session for a new connection.
    ///
    /// Cancels any owed will: the client came back, so it was never gone.
    pub fn claim(&mut self, epoch: u64, _now: u32) {
        self.epoch = epoch;
        self.connected = true;
        self.presence.record(self.subscriber, &self.id, true);
        self.will_at = None;
        self.expires_at = None;
        self.outbound.set_online(true);
        // Section 4.9: the receive quota "is not preserved across
        // Network Connections, and [is] re-initialized with each new
        // Network Connection". The departure resets it too, and every
        // path into here runs one first, so this is belt and braces
        // rather than the only guard. It stops being so the moment a
        // shell defers an acknowledgement across a reconnect.
        self.inbound.on_disconnect();
        self.journal_state();
    }

    /// The connection went away.
    ///
    /// A graceful departure discards the will; an abrupt one owes it,
    /// after the delay in MQTT 5 and at once in 3.1.1.
    pub fn on_disconnected(&mut self, graceful: bool, now: u32) {
        self.connected = false;
        self.presence.record(self.subscriber, &self.id, false);
        self.outbound.on_disconnect();
        self.inbound.on_disconnect();

        if graceful {
            self.will = None;
            self.will_at = None;
        } else if self.will.is_some() {
            let delay = if self.version == Version::V5 {
                self.delay_capped_by_expiry()
            } else {
                0
            };
            self.will_at = Some(deadline(now, delay));
        }

        self.expires_at = Some(deadline(now, self.expiry_interval));
        self.journal_state();
    }

    /// A disconnect packet arrived, which may leave the will standing
    /// despite being a graceful departure.
    ///
    /// Only reason code 0x00 discards it [MQTT-3.14.4-3]; 0x04 asks for it
    /// explicitly, and any other reason is simply not the receipt that
    /// deletes it. MQTT 3.1.1's disconnect carries no reason at all and
    /// its rule is unconditional, so there every packet discards.
    pub fn on_disconnect_packet(&mut self, reason: ReasonCode, now: u32) {
        let wants_will = self.version == Version::V5 && reason != ReasonCode::Success;
        self.on_disconnected(!wants_will, now);
    }

    /// A delay longer than the session lives is pointless, because the
    /// will falls due when the session ends however long the delay is.
    /// An interval of zero ends the session with its connection, so the
    /// will is owed at once.
    fn delay_capped_by_expiry(&self) -> u32 {
        match self.will.as_ref() {
            Some(will) => will.delay.min(self.expiry_interval),
            None => 0,
        }
    }

    /// Take the will if it is due, leaving nothing behind.
    pub fn take_due_will(&mut self, now: u32) -> Option<StoredWill> {
        match self.will_at {
            Some(due) if now >= due => {
                self.will_at = None;
                let will = self.will.take();
                // A will that has been published is no longer owed, and
                // a restart that resurrected it would announce a client
                // gone twice.
                self.journal_state();
                will
            }
            _ => None,
        }
    }

    /// Set or clear the will.
    pub fn set_will(&mut self, will: Option<StoredWill>) {
        self.will = will;
        self.journal_state();
    }

    /// When the will falls due, for a path rebuilding this from storage.
    pub fn set_will_at(&mut self, will_at: Option<u32>) {
        self.will_at = will_at;
        self.journal_state();
    }

    /// When the will falls due, if it is owed.
    pub fn will_at(&self) -> Option<u32> {
        self.will_at
    }

    /// The will, if one is still owed.
    pub fn will(&self) -> Option<&StoredWill> {
        self.will.as_ref()
    }

    /// When the session may be destroyed, once disconnected.
    pub fn expires_at(&self) -> Option<u32> {
        self.expires_at
    }

    /// When the session may be destroyed, for a path rebuilding this
    /// from storage.
    ///
    /// Section 7.5: a session whose expiry passed while the broker was
    /// down is not restored, so the deadline that survives is an
    /// absolute one and is judged against a clock read before restore.
    pub fn set_expires_at(&mut self, expires_at: Option<u32>) {
        self.expires_at = expires_at;
        self.journal_state();
    }

    /// How long the session outlives its connection.
    pub fn set_expiry_interval(&mut self, seconds: u32) {
        self.expiry_interval = seconds;
        self.journal_state();
    }

    /// How long the session outlives its connection.
    pub fn expiry_interval(&self) -> u32 {
        self.expiry_interval
    }

    /// Whether the session may be destroyed. A connected one never may.
    pub fn is_expired(&self, now: u32) -> bool {
        !self.connected && self.expires_at.is_some_and(|at| now >= at)
    }

    /// Record a subscription this session holds, replacing the options
    /// on a filter it already had.
    ///
    /// The filter is what identifies one, which is the protocol's rule
    /// and the store's: a second subscribe to the same filter changes
    /// the options rather than adding a subscription.
    pub fn add_subscription(&mut self, subscription: SubscriptionRecord) {
        self.journal.push(Record::SubscriptionAdded {
            session: journal::key_of(&self.id),
            subscription: subscription.clone(),
        });
        match self
            .subscriptions
            .iter_mut()
            .find(|held| held.filter == subscription.filter)
        {
            Some(held) => *held = subscription,
            None => self.subscriptions.push(subscription),
        }
    }

    /// Forget a filter.
    pub fn remove_filter(&mut self, filter: &str) -> bool {
        let before = self.subscriptions.len();
        self.subscriptions.retain(|held| held.filter != filter);
        let removed = self.subscriptions.len() != before;
        if removed {
            self.journal.push(Record::SubscriptionRemoved {
                session: journal::key_of(&self.id),
                filter: filter.to_string(),
            });
        }
        removed
    }

    /// Every filter this session holds.
    pub fn filters(&self) -> impl Iterator<Item = &str> {
        self.subscriptions.iter().map(|held| held.filter.as_str())
    }

    /// Every subscription this session holds, with its options.
    pub fn subscriptions(&self) -> &[SubscriptionRecord] {
        &self.subscriptions
    }

    /// Whether a connection is attached.
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Which protocol version the client speaks.
    pub fn version(&self) -> Version {
        self.version
    }

    /// Record the version once the connect has been decoded.
    pub fn set_version(&mut self, version: Version) {
        self.version = version;
        self.journal_state();
    }

    /// The claim's generation.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// This session's identity.
    pub fn id(&self) -> &SessionId {
        &self.id
    }

    /// The handle keying its subscriptions.
    pub fn subscriber(&self) -> SubscriberId {
        self.subscriber
    }
}

/// A deadline that errs late.
///
/// `now` is a whole-second clock, so a disconnect part-way through a tick
/// reads as the tick that has already begun. A deadline of `now + delay`
/// would then fall due after as little as `delay - 1` real seconds:
/// 3.1.3.2.2 and [MQTT-3.1.2-11] both say a period elapses before the
/// thing happens, so the extra tick is the difference between late and
/// wrong. A delay of zero means at once and is not rounded.
///
/// Restore reaches this too, for a session that was connected when the
/// broker died and so has no stored deadline to put back.
pub(crate) fn deadline(now: u32, delay: u32) -> u32 {
    if delay == 0 {
        now
    } else {
        now.saturating_add(delay).saturating_add(1)
    }
}

/// Turn a stored will into a message to publish.
///
/// The expiry deadline is left for the caller, because it is the only
/// part that needs a clock and this module reads none.
pub fn will_message(will: &StoredWill) -> crate::session::message::Message {
    let mut message = crate::session::message::Message::new(
        &will.topic,
        will.payload.clone(),
        will.qos,
        will.retain,
    );
    message.properties = Arc::new(will.properties.clone().into());
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::SessionId;
    use crate::session::message::Message;
    use crate::session::queue::Delivery;
    use crate::session::Action;
    use crate::subscriptions::SubscriberId;
    use bytes::Bytes;
    use mqtt_codec::{Packet, QoS, ReasonCode, Version};

    fn session(version: Version) -> Session {
        Session::new(
            SessionId::new("", "c"),
            SubscriberId(1),
            version,
            1,
            0,
            SessionBounds {
                max_inflight: 20,
                ..Default::default()
            },
        )
    }

    fn will() -> StoredWill {
        StoredWill {
            topic: "gone/c".into(),
            payload: Bytes::from_static(b"offline"),
            qos: QoS::AtLeastOnce,
            retain: false,
            delay: 0,
            properties: Properties::default(),
        }
    }

    #[test]
    fn a_session_records_the_version_it_speaks() {
        // Convention 5 again: the stored discriminants are 0 and 1 and
        // the protocol level bytes are 4 and 5, so a mapping written
        // from the wrong pair would round trip inside this build and
        // misread every session a released build had stored.
        let mut five = session(Version::V5);
        five.set_version(Version::V5);
        assert_eq!(five.record().version, ProtocolVersion::V5);

        let mut three = session(Version::V5);
        three.set_version(Version::V311);
        assert_eq!(three.record().version, ProtocolVersion::V311);
    }

    #[test]
    fn a_session_records_the_will_it_is_holding_and_stops_when_it_is_not() {
        let mut session = session(Version::V5);
        assert_eq!(session.record().will, None, "no will, nothing recorded");

        session.set_will(Some(will()));
        let recorded = session.record().will.expect("a will was set");
        assert_eq!(recorded.topic, "gone/c");
        assert_eq!(recorded.qos, storage::record::Qos::AtLeastOnce);

        // A graceful departure discards it, and the record must say so
        // or a restart announces a client gone that said goodbye.
        session.on_disconnected(true, 10);
        assert_eq!(session.record().will, None);
    }

    #[test]
    fn a_new_session_is_connected_and_has_no_deadline() {
        let session = session(Version::V5);
        assert!(session.is_connected());
        assert!(!session.is_expired(1_000_000));
    }

    #[test]
    fn a_new_session_delivers_a_fire_and_forget_message_rather_than_dropping_it() {
        // `is_connected` and the outbound path must agree. A session that
        // reports itself connected while its outbound path is offline
        // discards every at-most-once delivery, because such a message is
        // not queued for an absent client by default. The symptom is a
        // retained message that silently never arrives.
        let mut session = session(Version::V5);
        let mut out = Vec::new();
        session.outbound.offer(
            Delivery {
                message: Message::new("a/b", Bytes::from_static(b"x"), QoS::AtMostOnce, false),
                qos: QoS::AtMostOnce,
                retain: false,
                subscription_ids: Vec::new(),
                dup: false,
                packet_id: None,
                sequence: 0,
            },
            0,
            &mut out,
        );

        assert!(
            !out.iter().any(|a| matches!(a, Action::Dropped { .. })),
            "a connected session does not discard what it is offered"
        );
        assert!(
            out.iter()
                .any(|a| matches!(a, Action::Send(Packet::Publish(p)) if p.topic == "a/b")),
            "it goes out"
        );
    }

    /// A session with a will that waits `delay` seconds.
    fn a_session_with_a_will(delay: u32) -> Session {
        let mut session = session(Version::V5);
        session.set_will(Some(StoredWill {
            delay,
            properties: Properties::default(),
            ..will()
        }));
        session
    }

    #[test]
    fn a_will_delay_is_never_cut_short_by_the_coarse_clock() {
        // The disconnect lands part-way through a tick, so `now` reads one
        // less than the true elapsed time. A deadline of `now + delay` would
        // then fall due before `delay` seconds had actually passed.
        let mut session = a_session_with_a_will(3);
        session.set_expiry_interval(300);

        session.on_disconnected(false, 10);

        assert!(
            session.take_due_will(12).is_none(),
            "two ticks after a three second delay, the will is not yet owed"
        );
        assert!(
            session.take_due_will(13).is_none(),
            "3.1.3.2.2: at the tick the delay lands on, up to a second of it may remain"
        );
        assert!(
            session.take_due_will(14).is_some(),
            "the tick after, the delay has certainly passed"
        );
    }

    #[test]
    fn an_ungraceful_disconnect_owes_a_will_at_once_without_a_delay() {
        let mut session = session(Version::V5);
        session.set_will(Some(will()));
        session.on_disconnected(false, 100);

        assert_eq!(
            session.take_due_will(100).map(|w| w.topic),
            Some("gone/c".into())
        );
        assert!(session.take_due_will(100).is_none(), "only once");
    }

    #[test]
    fn a_graceful_disconnect_discards_the_will() {
        let mut session = session(Version::V5);
        session.set_will(Some(will()));
        session.on_disconnected(true, 100);
        assert!(session.take_due_will(1_000).is_none());
    }

    #[test]
    fn the_will_delay_holds_it_back() {
        let mut session = session(Version::V5);
        // The session must outlive the delay, or the will is due when the
        // session ends instead: MQTT-3.1.3-9.
        session.set_expiry_interval(300);
        session.set_will(Some(StoredWill {
            delay: 30,
            properties: Properties::default(),
            ..will()
        }));
        session.on_disconnected(false, 100);

        // The disconnect was seen at coarse tick 100, so the true time
        // was somewhere in [100, 101). At tick 130 as little as 29
        // seconds may have passed, which is short of the delay; at 131
        // at least 30 have.
        assert!(session.take_due_will(130).is_none(), "not yet");
        assert!(
            session.take_due_will(131).is_some(),
            "the delay has certainly passed"
        );
    }

    #[test]
    fn the_delay_is_capped_by_the_session_expiry() {
        // "The Server delays publishing the Client's Will Message until the
        // Will Delay Interval has passed or the Session ends, whichever
        // happens first" [MQTT-3.1.3-9]. A session whose expiry interval is
        // ten seconds ends at ten seconds, delay notwithstanding.
        let mut session = session(Version::V5);
        session.set_expiry_interval(10);
        session.set_will(Some(StoredWill {
            delay: 30,
            properties: Properties::default(),
            ..will()
        }));
        session.on_disconnected(false, 100);

        // Ten seconds from a disconnect seen at tick 100 is not
        // certainly past until tick 111, for the same reason the
        // uncapped delay is not: see `deadline`.
        assert!(session.take_due_will(110).is_none(), "the session lives");
        assert!(session.take_due_will(111).is_some(), "the session ended");
    }

    #[test]
    fn a_session_that_ends_with_its_connection_owes_the_will_at_once() {
        // An expiry interval of zero means "the Session ends when the
        // Network Connection is closed" (3.1.2.11.2), and a will is due
        // when the session ends however long the delay is.
        let mut session = session(Version::V5);
        session.set_expiry_interval(0);
        session.set_will(Some(StoredWill {
            delay: 30,
            properties: Properties::default(),
            ..will()
        }));
        session.on_disconnected(false, 100);

        assert!(session.take_due_will(100).is_some());
    }

    #[test]
    fn reconnecting_before_the_delay_elapses_cancels_the_will() {
        let mut session = session(Version::V5);
        session.set_expiry_interval(300);
        session.set_will(Some(StoredWill {
            delay: 30,
            properties: Properties::default(),
            ..will()
        }));
        session.on_disconnected(false, 100);

        session.claim(2, 110);
        assert!(
            session.take_due_will(1_000).is_none(),
            "the client came back"
        );
    }

    #[test]
    fn a_three_one_one_session_ignores_the_delay() {
        // The field does not exist in that version, so a delay would be
        // an invention rather than a behaviour.
        let mut session = session(Version::V311);
        session.set_will(Some(StoredWill {
            delay: 30,
            properties: Properties::default(),
            ..will()
        }));
        session.on_disconnected(false, 100);
        assert!(session.take_due_will(100).is_some());
    }

    #[test]
    fn a_session_expires_after_its_interval() {
        let mut session = session(Version::V5);
        session.set_expiry_interval(60);
        session.on_disconnected(true, 100);

        // The boundary exactly, not a gap either side of it. A
        // disconnect seen at tick 100 happened somewhere in [100, 101),
        // so sixty seconds are not certainly gone until tick 161:
        // [MQTT-3.1.2-11] ends the session after the interval has
        // passed, not up to a second before. Read loosely, this test
        // could not tell 160 from 161, which is the very error the
        // interval had.
        assert!(!session.is_expired(160), "up to a second of it may remain");
        assert!(session.is_expired(161), "and now it has certainly gone");
    }

    #[test]
    fn a_connected_session_never_expires() {
        let mut session = session(Version::V5);
        session.set_expiry_interval(1);
        assert!(!session.is_expired(1_000_000), "it is still connected");
    }

    #[test]
    fn an_interval_of_zero_expires_the_session_the_moment_it_disconnects() {
        let mut session = session(Version::V5);
        session.set_expiry_interval(0);
        session.on_disconnected(true, 100);
        assert!(session.is_expired(100));
    }

    #[test]
    fn a_disconnect_asking_for_the_will_sends_it_despite_being_graceful() {
        let mut session = session(Version::V5);
        session.set_will(Some(will()));
        session.on_disconnect_packet(ReasonCode::DisconnectWithWillMessage, 100);
        assert!(session.take_due_will(100).is_some());
    }

    #[test]
    fn only_a_normal_disconnection_discards_the_will_in_mqtt_five() {
        // "On receipt of DISCONNECT with a Reason Code of 0x00 the Server
        // MUST discard any Will Message" [MQTT-3.14.4-3]. A DISCONNECT
        // carrying any other reason is not that receipt, so the will
        // stands.
        let mut session = session(Version::V5);
        session.set_will(Some(will()));
        session.on_disconnect_packet(ReasonCode::UnspecifiedError, 100);
        assert!(session.take_due_will(100).is_some());
    }

    #[test]
    fn any_disconnect_packet_discards_the_will_in_three_one_one() {
        // That version's DISCONNECT carries no reason code, and its rule
        // is unconditional [MQTT-3.14.4-3].
        let mut session = session(Version::V311);
        session.set_will(Some(will()));
        session.on_disconnect_packet(ReasonCode::Success, 100);
        assert!(session.take_due_will(1_000).is_none());
    }
}
