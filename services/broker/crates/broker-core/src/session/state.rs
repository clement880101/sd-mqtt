//! A session's lifetime: connect, disconnect, expiry and the will.

use bytes::Bytes;
use mqtt_codec::{QoS, ReasonCode, Version};

use crate::registry::SessionId;
use crate::session::inbound::Inbound;
use crate::session::outbound::Outbound;
use crate::session::queue::QueueLimits;
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
    /// Filters this session holds, so teardown can undo them.
    filters: Vec<String>,
    will: Option<StoredWill>,
    /// When the will becomes due, if it is owed.
    will_at: Option<u32>,
    expiry_interval: u32,
    /// When the session may be destroyed, once disconnected.
    expires_at: Option<u32>,
}

impl Session {
    /// A fresh session, connected.
    pub fn new(
        id: SessionId,
        subscriber: SubscriberId,
        version: Version,
        epoch: u64,
        _now: u32,
    ) -> Self {
        Self {
            id,
            subscriber,
            version,
            epoch,
            connected: true,
            outbound: Outbound::new(QueueLimits::default(), 20),
            inbound: Inbound::new(0),
            filters: Vec::new(),
            will: None,
            will_at: None,
            expiry_interval: 0,
            expires_at: None,
        }
    }

    /// Reclaim an existing session for a new connection.
    ///
    /// Cancels any owed will: the client came back, so it was never gone.
    pub fn claim(&mut self, epoch: u64, _now: u32) {
        self.epoch = epoch;
        self.connected = true;
        self.will_at = None;
        self.expires_at = None;
        self.outbound.set_online(true);
    }

    /// The connection went away.
    ///
    /// A graceful departure discards the will; an abrupt one owes it,
    /// after the delay in MQTT 5 and at once in 3.1.1.
    pub fn on_disconnected(&mut self, graceful: bool, now: u32) {
        self.connected = false;
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
            self.will_at = Some(now.saturating_add(delay));
        }

        self.expires_at = Some(now.saturating_add(self.expiry_interval));
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
                self.will.take()
            }
            _ => None,
        }
    }

    /// Set or clear the will.
    pub fn set_will(&mut self, will: Option<StoredWill>) {
        self.will = will;
    }

    /// How long the session outlives its connection.
    pub fn set_expiry_interval(&mut self, seconds: u32) {
        self.expiry_interval = seconds;
    }

    /// Whether the session may be destroyed. A connected one never may.
    pub fn is_expired(&self, now: u32) -> bool {
        !self.connected && self.expires_at.is_some_and(|at| now >= at)
    }

    /// Record a filter this session holds.
    pub fn add_filter(&mut self, filter: &str) {
        if !self.filters.iter().any(|f| f == filter) {
            self.filters.push(filter.to_string());
        }
    }

    /// Forget a filter.
    pub fn remove_filter(&mut self, filter: &str) -> bool {
        let before = self.filters.len();
        self.filters.retain(|f| f != filter);
        self.filters.len() != before
    }

    /// Every filter this session holds.
    pub fn filters(&self) -> impl Iterator<Item = &str> {
        self.filters.iter().map(String::as_str)
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

/// Turn a stored will into a message to publish.
pub fn will_message(will: &StoredWill) -> crate::session::message::Message {
    crate::session::message::Message::new(&will.topic, will.payload.clone(), will.qos, will.retain)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::SessionId;
    use crate::subscriptions::SubscriberId;
    use bytes::Bytes;
    use mqtt_codec::{QoS, ReasonCode, Version};

    fn session(version: Version) -> Session {
        Session::new(SessionId::new("", "c"), SubscriberId(1), version, 1, 0)
    }

    fn will() -> StoredWill {
        StoredWill {
            topic: "gone/c".into(),
            payload: Bytes::from_static(b"offline"),
            qos: QoS::AtLeastOnce,
            retain: false,
            delay: 0,
        }
    }

    #[test]
    fn a_new_session_is_connected_and_has_no_deadline() {
        let session = session(Version::V5);
        assert!(session.is_connected());
        assert!(!session.is_expired(1_000_000));
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
            ..will()
        }));
        session.on_disconnected(false, 100);

        assert!(session.take_due_will(129).is_none(), "not yet");
        assert!(session.take_due_will(130).is_some(), "at the deadline");
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
            ..will()
        }));
        session.on_disconnected(false, 100);

        assert!(session.take_due_will(109).is_none(), "the session lives");
        assert!(session.take_due_will(110).is_some(), "the session ended");
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

        assert!(!session.is_expired(159));
        assert!(session.is_expired(161));
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
