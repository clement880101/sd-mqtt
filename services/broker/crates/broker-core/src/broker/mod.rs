//! The broker: sessions, subscriptions and routing, assembled.

pub mod route;
pub mod subscribe;

use std::collections::HashMap;
use std::sync::Arc;

use foldhash::fast::RandomState;
use mqtt_codec::packet::Connect;
use mqtt_codec::{Limits, Packet, Properties, ReasonCode, Version};

use crate::clock::CoarseClock;
use crate::keepalive::{Activity, KeepAliveRing};
use crate::registry::{Registration, Registry, SessionId};
use crate::retained::{RetainedMessage, RetainedStore};
use crate::session::message::Message;
use crate::session::queue::{Delivery, DropReason};
use crate::session::state::{will_message, Session, StoredWill};
use crate::session::Action;
use crate::subscriptions::{SharedSubscriptions, SubscriberId, SubscriptionTrie};

/// How the broker is configured.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Protocol limits handed to the codec and the validators.
    pub limits: Limits,
    /// How many shards the subscription trie uses.
    pub trie_shards: usize,
    /// The largest keep-alive interval the ring is sized for.
    pub max_keep_alive: u16,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            trie_shards: 32,
            max_keep_alive: 3600,
        }
    }
}

/// An action addressed to one session, for the caller to perform.
pub type Addressed = (SubscriberId, Action);

/// The broker.
///
/// It reads no clock and touches no socket. Time arrives through
/// [`Broker::tick`], and everything it wants done comes back as a list of
/// addressed actions. That is what makes a whole history driveable in a
/// test without a runtime.
///
/// Nothing here decides anything. Every protocol judgement belongs to a
/// piece that is tested on its own: the registry decides takeover, the
/// session decides whether a departure owes a will, the receive path
/// decides what an arriving publish is worth, and the matching layer
/// decides who wants it. This module only holds them and passes the
/// answers along.
pub struct Broker {
    clock: Arc<CoarseClock>,
    subscriptions: SubscriptionTrie,
    shared: SharedSubscriptions,
    retained: RetainedStore,
    registry: Registry,
    keepalive: KeepAliveRing,
    activities: HashMap<SubscriberId, Activity, RandomState>,
    config: BrokerConfig,
}

impl Broker {
    /// A broker with no sessions, its clock at zero.
    pub fn new(config: BrokerConfig) -> Self {
        Self {
            clock: Arc::new(CoarseClock::new(0)),
            subscriptions: SubscriptionTrie::new(config.trie_shards),
            shared: SharedSubscriptions::new(config.trie_shards),
            retained: RetainedStore::new(),
            registry: Registry::default(),
            keepalive: KeepAliveRing::new(config.max_keep_alive),
            activities: HashMap::with_hasher(RandomState::default()),
            config,
        }
    }

    /// The clock, so the shell can advance it and a test can read it.
    pub fn clock(&self) -> &Arc<CoarseClock> {
        &self.clock
    }

    /// The handle an identity currently holds, if any.
    ///
    /// For the one caller that must ask before it invents an identifier.
    /// [MQTT-3.2.2-16] requires an assigned client identifier to be "a
    /// new Client Identifier not used by any other Session currently in
    /// the Server", and the consequence of getting that wrong is not a
    /// duplicate name: [`Broker::connect`] would read the newcomer as
    /// the same client and take the session over, disconnecting an
    /// unrelated one and handing its state away.
    pub fn subscriber_of(&self, id: &SessionId) -> Option<SubscriberId> {
        self.registry.subscriber_of(id)
    }

    /// One session, mutably.
    ///
    /// For the two settings a connection owns and a session does not:
    /// the send window narrowed to the peer's receive maximum, and the
    /// receive maximum imposed on the peer. Section 4.9 says both are
    /// "not preserved across Network Connections", so neither can be
    /// decided here, where no connection is visible.
    pub fn session_mut(&mut self, subscriber: SubscriberId) -> Option<&mut Session> {
        self.registry.get_mut(subscriber)
    }

    /// One session, for a metric or an assertion.
    pub fn session(&self, subscriber: SubscriberId) -> Option<&Session> {
        self.registry.get(subscriber)
    }

    /// Accept a connection.
    ///
    /// Taking over a session that already has a live connection is not an
    /// error: the newcomer wins and the incumbent is told why. The
    /// incumbent's departure runs before the registry reclaims or
    /// replaces the session, because a departure is what arms a will and
    /// the newcomer's will overwrites it a few lines later.
    ///
    /// # What the caller must do with `out`
    ///
    /// **Drain it in order, and map the handle to a connection rather
    /// than to a socket.** A handle names a session, not a connection,
    /// and a takeover gives two connections the same one. So a single
    /// call can append the close for the connection being displaced and
    /// then the redeliveries for the one replacing it, both addressed
    /// to the identical [`SubscriberId`]. A caller that reorders them,
    /// or that keeps one socket per handle, will close the newcomer or
    /// send the old connection the new one's traffic.
    ///
    /// The rule that makes this tractable: everything before the close
    /// belongs to the incumbent, everything after it to the newcomer.
    pub fn connect(
        &mut self,
        mountpoint: &str,
        connect: &Connect,
        out: &mut Vec<Addressed>,
    ) -> Registration {
        let now = self.clock.now();
        let identity = SessionId::new(mountpoint, &connect.client_id);

        let displaced = self.displace_incumbent(&identity, connect.clean_start, now, out);
        if connect.clean_start {
            // The registry is about to replace this session wholesale,
            // taking its queue and its window with it. What it was
            // holding is reported before it goes.
            if let Some(existing) = self.registry.subscriber_of(&identity) {
                if let Some(session) = self.registry.get_mut(existing) {
                    Self::discard_held(session, existing, out);
                }
            }
        }
        let mut registration = self.registry.register(identity, connect.clean_start, now);
        // The registry judges takeover by whether the session was still
        // connected, and this layer disconnected it a moment ago on
        // purpose, so the registry now says no. The observation above is
        // the true one.
        registration.took_over = displaced;

        if !registration.session_present {
            // A clean start discards the session [MQTT-3.1.2-4], and the
            // registry did: the handle now names a new one. Its
            // subscriptions do not live in the session, though, they live
            // in the trie keyed by that same handle, so only this layer
            // can discard them. A no-op for a handle never seen before.
            if let Some(session) = self.registry.get_mut(registration.subscriber) {
                subscribe::remove_all(&self.subscriptions, &self.shared, session);
            }
        }

        let session = self
            .registry
            .get_mut(registration.subscriber)
            .expect("just registered");
        session.set_version(connect.version);
        session.set_expiry_interval(expiry_interval(connect));
        session.set_will(connect.will.as_ref().map(|will| {
            // Section 3.1.3.2 splits the will properties in two: the
            // application message's own, which travel with it, and the
            // delay, which says when to send it and must not. A publish
            // does not permit a will delay and the encoder refuses to
            // write one, so taking it out here is the same rule as
            // convention 1 seen from the other side.
            let mut properties = will.properties.clone();
            let delay = properties.will_delay_interval.take().unwrap_or_default();
            StoredWill {
                topic: will.topic.clone(),
                payload: will.payload.clone(),
                qos: will.qos,
                retain: will.retain,
                delay,
                properties,
            }
        }));

        let mut actions = Vec::new();
        session.outbound.on_resume(now, &mut actions);
        for action in actions {
            out.push((registration.subscriber, action));
        }

        let activity = Activity::new(now);
        self.activities
            .insert(registration.subscriber, activity.clone());
        self.keepalive
            .insert(registration.subscriber, connect.keep_alive, activity, now);

        registration
    }

    /// End a live connection that a newcomer is taking over from, and
    /// publish the will it leaves if one is due. Returns whether there
    /// was one to displace.
    ///
    /// # Which takeovers publish a will
    ///
    /// Closing a connection without having received a normal
    /// disconnection is one of the situations [MQTT-3.1.2-8] names, so
    /// the will is armed by the departure like any other. What differs
    /// is whether the newcomer suppresses it.
    ///
    /// [MQTT-3.1.3-9] forbids sending the will when "a new Network
    /// Connection to this Session is made before the Will Delay Interval
    /// has passed". Both halves of that matter:
    ///
    /// - A delay of zero, which is the default, has already passed by
    ///   the time the connection closes, so nothing is suppressed and
    ///   the will goes out.
    /// - A delay above zero is suppressed, because the takeover is
    ///   exactly the new connection the clause describes.
    /// - MQTT 3.1.1 has no delay at all and its [MQTT-3.1.2-8] is
    ///   unconditional, so a takeover always publishes. The session
    ///   already knows this and forces the delay to zero for that
    ///   version.
    ///
    /// # Clean start
    ///
    /// A clean-start takeover publishes the will whatever the delay
    /// says. [MQTT-3.1.2-8] fires the will when "the Will Delay Interval
    /// has elapsed **or the Session ends**", and clean start ends the
    /// incumbent's session rather than resuming it. Section 3.1.4's own
    /// commentary reads the pair the same way: with clean start set, or
    /// a session expiry interval of zero, the will "will be sent because
    /// the original Session is ended on the takeover".
    ///
    /// That does not contradict [MQTT-3.1.3-9], whose operative words
    /// are "to this Session". A clean start is not a new connection to
    /// the incumbent's session; it discards that session and begins
    /// another, so the clause never engages.
    ///
    /// It is expressed by ending the session's life here rather than by
    /// testing the delay again, so the rule that a will falls due when
    /// its session ends stays the session's, where it is tested. A
    /// session expiry interval of zero already reaches the same place by
    /// the same route.
    fn displace_incumbent(
        &mut self,
        identity: &SessionId,
        clean_start: bool,
        now: u32,
        out: &mut Vec<Addressed>,
    ) -> bool {
        let Some(incumbent) = self.registry.subscriber_of(identity) else {
            return false;
        };
        if !self
            .registry
            .get(incumbent)
            .is_some_and(Session::is_connected)
        {
            return false;
        }

        out.push((incumbent, Action::Close(ReasonCode::SessionTakenOver)));
        // The incumbent never disconnected, so nothing else disarms its
        // keep-alive. Left in the ring it would expire on the displaced
        // connection's silence and close the newcomer.
        self.keepalive.remove(incumbent);
        self.activities.remove(&incumbent);

        let due = {
            let session = self.registry.get_mut(incumbent).expect("just looked it up");
            if clean_start {
                // The session ends now, so it outlives its connection by
                // nothing and the delay has nothing left to cap.
                session.set_expiry_interval(0);
            }
            session.on_disconnected(false, now);
            session.take_due_will(now)
        };

        if let Some(will) = due {
            self.publish_will(incumbent, &will, now, out);
        }

        true
    }

    /// Publish a will on behalf of the session that left it.
    ///
    /// The one path, so the two callers cannot drift: a will owed to a
    /// takeover and one owed to a timeout or an expiry are the same
    /// message.
    fn publish_will(
        &mut self,
        publisher: SubscriberId,
        will: &StoredWill,
        now: u32,
        out: &mut Vec<Addressed>,
    ) {
        let mut message = will_message(will);
        message.expires_at = self.deadline(&message.properties);
        self.publish(message, Some(publisher), now, out);
    }

    /// When a message carrying these properties expires, if it does.
    fn deadline(&self, properties: &Properties) -> Option<u32> {
        properties
            .message_expiry_interval
            .map(|interval| self.clock.deadline(interval))
    }

    /// Handle a packet from a connected client.
    ///
    /// An error is the reason to close the connection with; the caller
    /// does the closing.
    ///
    /// **Closing the socket is not enough.** A caller that drops the
    /// connection without also calling [`Broker::disconnect`] leaves
    /// the session believing it is still attached, and a session that
    /// believes that never expires, never fires its will and holds its
    /// queue forever: [`Session::is_expired`] refuses to expire a
    /// connected session, deliberately, because that is what keeps a
    /// live client alive. The same applies to an
    /// [`Action::Close`] handed back from any other call.
    ///
    /// [`Session::is_expired`]: crate::session::state::Session::is_expired
    pub fn handle(
        &mut self,
        subscriber: SubscriberId,
        packet: &Packet,
        out: &mut Vec<Addressed>,
    ) -> Result<(), ReasonCode> {
        let now = self.clock.now();
        if let Some(activity) = self.activities.get(&subscriber) {
            // The only thing an arriving packet does to the keep-alive
            // machinery: one relaxed store, no timer, no lock.
            activity.touch(now);
        }

        match packet {
            Packet::Publish(publish) => self.on_publish(subscriber, publish, now, out),
            Packet::Subscribe(request) => {
                let mut actions = Vec::new();
                let session = self
                    .registry
                    .get_mut(subscriber)
                    .ok_or(ReasonCode::ProtocolError)?;
                let reasons = subscribe::subscribe(
                    &self.subscriptions,
                    &self.shared,
                    &self.retained,
                    session,
                    request,
                    &self.config.limits,
                    now,
                    &mut actions,
                );
                actions.push(Action::Send(Packet::SubAck(mqtt_codec::packet::SubAck {
                    packet_id: request.packet_id,
                    reasons,
                    properties: Default::default(),
                })));
                for action in actions {
                    out.push((subscriber, action));
                }
                Ok(())
            }
            Packet::Unsubscribe(request) => {
                let session = self
                    .registry
                    .get_mut(subscriber)
                    .ok_or(ReasonCode::ProtocolError)?;
                let reasons = subscribe::unsubscribe(
                    &self.subscriptions,
                    &self.shared,
                    session,
                    &request.filters,
                );
                out.push((
                    subscriber,
                    Action::Send(Packet::UnsubAck(mqtt_codec::packet::UnsubAck {
                        packet_id: request.packet_id,
                        reasons,
                        properties: Default::default(),
                    })),
                ));
                Ok(())
            }
            Packet::PingReq => {
                out.push((subscriber, Action::Send(Packet::PingResp)));
                Ok(())
            }
            Packet::Disconnect(disconnect) => {
                // 3.14.2.2.2 lets a client change how long its session
                // outlives this connection. [MQTT-3.14.4-3] bars it from
                // raising an interval of zero: a session that asked to
                // end with its connection cannot be kept alive on the
                // way out.
                //
                // This runs before `detach`, because `on_disconnected`
                // computes `expires_at` from the interval.
                if let Some(requested) = disconnect.properties.session_expiry_interval {
                    let session = self
                        .registry
                        .get_mut(subscriber)
                        .ok_or(ReasonCode::ProtocolError)?;
                    if requested != 0 && session.expiry_interval() == 0 {
                        return Err(ReasonCode::ProtocolError);
                    }
                    session.set_expiry_interval(requested);
                }
                // Whether a disconnect packet leaves the will standing
                // depends on the reason code and the protocol version,
                // and the session already holds that rule.
                self.detach(subscriber, Some(disconnect.reason), now);
                Ok(())
            }
            other => self.on_acknowledgement(subscriber, other, now, out),
        }
    }

    fn on_acknowledgement(
        &mut self,
        subscriber: SubscriberId,
        packet: &Packet,
        now: u32,
        out: &mut Vec<Addressed>,
    ) -> Result<(), ReasonCode> {
        let mut actions = Vec::new();
        let session = self
            .registry
            .get_mut(subscriber)
            .ok_or(ReasonCode::ProtocolError)?;

        let result = match packet {
            Packet::PubAck(ack) => {
                session
                    .outbound
                    .on_puback(ack.packet_id, ack.reason, now, &mut actions)
            }
            Packet::PubRec(ack) => {
                session
                    .outbound
                    .on_pubrec(ack.packet_id, ack.reason, now, &mut actions)
            }
            Packet::PubComp(ack) => session
                .outbound
                .on_pubcomp(ack.packet_id, now, &mut actions),
            Packet::PubRel(ack) => {
                session.inbound.on_pubrel(ack.packet_id, &mut actions);
                Ok(())
            }
            _ => Err(ReasonCode::ProtocolError),
        };

        // Drained before the result is returned, because a failing
        // acknowledgement may still have produced packets the peer is
        // waiting for.
        for action in actions {
            out.push((subscriber, action));
        }
        result
    }

    fn on_publish(
        &mut self,
        from: SubscriberId,
        publish: &mqtt_codec::packet::Publish,
        now: u32,
        out: &mut Vec<Addressed>,
    ) -> Result<(), ReasonCode> {
        let received = {
            let session = self
                .registry
                .get_mut(from)
                .ok_or(ReasonCode::ProtocolError)?;
            // Nothing is emitted here and nothing is reserved on the
            // error path: the receive path refuses before it counts, so
            // an error costs the client no receive-maximum slot.
            session.inbound.on_publish(publish)?
        };

        // `None` when the message was not dispatched, which is not the
        // same as reaching nobody.
        let delivered = if received.dispatch {
            let mut message = Message::new(
                &publish.topic,
                publish.payload.clone(),
                publish.qos,
                publish.retain,
            );
            message.properties = Arc::new(publish.properties.clone());
            message.expires_at = self.deadline(&publish.properties);

            Some(self.publish(message, Some(from), now, out))
        } else {
            None
        };

        // The acknowledgement is owed until now, because whether anyone
        // wanted the message is only known after routing. It is paid on
        // every path that got this far, including the one that dispatched
        // nothing: a slot the acknowledgement never frees throttles the
        // client to death with nothing reporting it.
        self.acknowledge(from, &received, delivered, out);
        Ok(())
    }

    /// Pay what the receive path said was owed.
    ///
    /// `delivered` is how many subscribers the message reached, or `None`
    /// when it was not dispatched at all. Only a dispatch that reached
    /// nobody may say so: a redelivery already held was dispatched once,
    /// and claiming nobody wanted it would be a lie about the first copy.
    fn acknowledge(
        &mut self,
        subscriber: SubscriberId,
        received: &crate::session::Received,
        delivered: Option<usize>,
        out: &mut Vec<Addressed>,
    ) {
        let Some(owed) = received.owed else {
            return;
        };
        let reason = match delivered {
            Some(0) => ReasonCode::NoMatchingSubscribers,
            _ => ReasonCode::Success,
        };

        let mut actions = Vec::new();
        if let Some(session) = self.registry.get_mut(subscriber) {
            session
                .inbound
                .acknowledge(owed.packet_id, owed.qos, reason, &mut actions);
        }
        for action in actions {
            out.push((subscriber, action));
        }
    }

    /// Route a message to every subscriber that wants it, updating the
    /// retained store first when the flag is set. Returns how many
    /// subscribers it reached.
    pub fn publish(
        &mut self,
        message: Message,
        publisher: Option<SubscriberId>,
        now: u32,
        out: &mut Vec<Addressed>,
    ) -> usize {
        if message.retain {
            // An empty payload clears the topic rather than storing one,
            // which the store itself knows.
            self.retained.set(
                &message.topic,
                RetainedMessage {
                    payload: message.payload.clone(),
                    qos: message.qos,
                    expires_at: message.expires_at,
                },
            );
        }

        let mut routed = Vec::new();
        route::route(
            &self.subscriptions,
            &self.shared,
            &message,
            publisher,
            &mut routed,
        );
        let reached = routed.len();

        for target in routed {
            let mut actions = Vec::new();
            if let Some(session) = self.registry.get_mut(target.subscriber) {
                session.outbound.offer(
                    Delivery {
                        message: message.clone(),
                        qos: target.qos,
                        retain: target.retain,
                        dup: false,
                        packet_id: None,
                        subscription_ids: target.subscription_ids,
                    },
                    now,
                    &mut actions,
                );
            }
            for action in actions {
                out.push((target.subscriber, action));
            }
        }

        reached
    }

    /// A connection went away without a disconnect packet.
    ///
    /// Nothing comes back from this. A will owed by an abrupt departure
    /// falls due on a later [`Broker::tick`], even when its delay is
    /// zero, so that one path publishes every will and a test cannot
    /// pass by watching the wrong one.
    pub fn disconnect(&mut self, subscriber: SubscriberId, _out: &mut Vec<Addressed>) {
        let now = self.clock.now();
        self.detach(subscriber, None, now);
    }

    /// Let go of a connection.
    ///
    /// `packet` carries the reason code when a disconnect packet arrived
    /// and is `None` when the connection simply went away. Which of the
    /// two owes a will is the session's rule, not this layer's.
    fn detach(&mut self, subscriber: SubscriberId, packet: Option<ReasonCode>, now: u32) {
        self.keepalive.remove(subscriber);
        self.release(subscriber, packet, now);
    }

    /// The part of a detach that a connection already out of the ring
    /// still needs: forget its activity cell and tell the session.
    ///
    /// Split out for the keep-alive path alone. The sweep takes an
    /// expired entry out of its bucket as it finds it, so asking the
    /// ring to remove it again is the one call there that buys nothing.
    fn release(&mut self, subscriber: SubscriberId, packet: Option<ReasonCode>, now: u32) {
        self.activities.remove(&subscriber);
        if let Some(session) = self.registry.get_mut(subscriber) {
            match packet {
                Some(reason) => session.on_disconnect_packet(reason, now),
                None => session.on_disconnected(false, now),
            }
        }
    }

    /// Report and discard everything a session still held.
    ///
    /// A session that is destroyed takes its queue and its window with
    /// it, and discarding them is correct: nothing will ever deliver
    /// them now. Discarding them *unreported* is not. The session layer
    /// keeps the rule that nothing disappears without an action saying
    /// so, because a message that vanishes with no counter moving is a
    /// support case nobody can answer, and [`Outbound::clear`] exists
    /// for exactly this moment. Only the assembly knows when it arrives.
    ///
    /// [`Outbound::clear`]: crate::session::Outbound::clear
    fn discard_held(session: &mut Session, subscriber: SubscriberId, out: &mut Vec<Addressed>) {
        let mut actions = Vec::new();
        session
            .outbound
            .clear(DropReason::SessionEnded, &mut actions);
        for action in actions {
            out.push((subscriber, action));
        }
    }

    /// Advance one second and do everything that falls due.
    pub fn tick(&mut self, out: &mut Vec<Addressed>) {
        let now = self.clock.advance(1);

        let mut expired = Vec::new();
        self.keepalive.tick(now, &mut expired);
        for subscriber in expired {
            out.push((subscriber, Action::Close(ReasonCode::KeepAliveTimeout)));
            // A connection that stopped talking never said goodbye, so
            // its will is owed. The sweep above already took it out of
            // the ring, so this does not ask again.
            self.release(subscriber, None, now);
        }

        // Wills first: a session that expires this tick may also owe one.
        let due: Vec<(SubscriberId, StoredWill)> = self
            .registry
            .iter_mut()
            .filter_map(|(id, session)| session.take_due_will(now).map(|will| (*id, will)))
            .collect();
        for (publisher, will) in due {
            self.publish_will(publisher, &will, now, out);
        }

        let gone: Vec<SubscriberId> = self
            .registry
            .iter_mut()
            .filter(|(_, session)| session.is_expired(now))
            .map(|(id, _)| *id)
            .collect();
        for subscriber in gone {
            if let Some(mut session) = self.registry.remove(subscriber) {
                subscribe::remove_all(&self.subscriptions, &self.shared, &mut session);
                Self::discard_held(&mut session, subscriber, out);
            }
            self.keepalive.remove(subscriber);
            self.activities.remove(&subscriber);
        }

        self.retained.sweep_expired(now);
    }

    /// How many sessions exist, connected or not.
    pub fn session_count(&self) -> usize {
        self.registry.len()
    }

    /// How many subscriptions are held, for a metric and for tests.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len() + self.shared.member_count()
    }
}

/// How long a session outlives its connection, in seconds.
///
/// MQTT 5 states it as a property and defaults it to zero, 3.1.2.11.2.
/// MQTT 3.1.1 has only the clean flag, and [MQTT-3.1.2-4] requires a
/// session with it clear to be stored: there is no interval to read, so
/// the equivalent is one that does not elapse.
fn expiry_interval(connect: &Connect) -> u32 {
    match connect.version {
        Version::V5 => connect
            .properties
            .session_expiry_interval
            .unwrap_or_default(),
        _ if connect.clean_start => 0,
        _ => u32::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mqtt_codec::packet::{Connect, Disconnect};

    /// A minimal MQTT 5 connect for the given client, resuming rather
    /// than discarding whatever session it already has.
    fn connect_v5(client: &str) -> Connect {
        Connect {
            version: Version::V5,
            client_id: client.to_string(),
            clean_start: false,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties::default(),
        }
    }

    /// The same over 3.1.1, which has no properties to speak of.
    fn connect_v311(client: &str) -> Connect {
        Connect {
            version: Version::V311,
            ..connect_v5(client)
        }
    }

    #[test]
    fn a_three_one_one_session_with_clean_session_clear_outlives_its_connection() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let mut connect = connect_v311("durable");
        connect.clean_start = false;
        let first = broker.connect("", &connect, &mut out);
        broker.disconnect(first.subscriber, &mut out);

        // Far past any plausible deadline.
        broker.clock().advance(1_000_000);
        broker.tick(&mut out);

        let second = broker.connect("", &connect, &mut out);
        assert!(
            second.session_present,
            "[MQTT-3.1.2-4]: a 3.1.1 session with CleanSession clear is stored"
        );
    }

    #[test]
    fn a_version_five_session_with_no_expiry_property_does_not_outlive_its_connection() {
        // The mirror of the test above, and the half no other test
        // reached: every existing session-expiry test names an interval
        // explicitly, so the default was unguarded and reading the
        // absent property as "never expires" survived the whole suite.
        // 3.1.2.11.2 says the absent Session Expiry Interval is zero,
        // and zero ends the session with the network connection.
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let connect = connect_v5("ephemeral");
        assert!(
            connect.properties.session_expiry_interval.is_none(),
            "the point of this test is the absent property"
        );
        let first = broker.connect("", &connect, &mut out);
        broker.disconnect(first.subscriber, &mut out);
        broker.tick(&mut out);

        let second = broker.connect("", &connect, &mut out);
        assert!(
            !second.session_present,
            "3.1.2.11.2: an absent Session Expiry Interval is zero, not forever"
        );
    }

    /// Advance the broker's clock to `seconds`, running every tick on
    /// the way so that nothing due is stepped over.
    fn run_to(broker: &mut Broker, seconds: u32, out: &mut Vec<Addressed>) {
        while broker.clock().now() < seconds {
            broker.tick(out);
        }
    }

    #[test]
    fn a_disconnect_may_extend_the_session() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let mut connect = connect_v5("mover");
        connect.properties.session_expiry_interval = Some(1);
        let first = broker.connect("", &connect, &mut out);

        let goodbye = Packet::Disconnect(Disconnect {
            reason: ReasonCode::Success,
            properties: Properties {
                session_expiry_interval: Some(300),
                ..Properties::default()
            },
        });
        broker.handle(first.subscriber, &goodbye, &mut out).unwrap();

        // Past the interval the CONNECT asked for, inside the one the
        // DISCONNECT asked for.
        run_to(&mut broker, 60, &mut out);
        let second = broker.connect("", &connect, &mut out);
        assert!(
            second.session_present,
            "3.14.2.2.2: the DISCONNECT's interval stands"
        );
    }

    #[test]
    fn a_disconnect_must_not_resurrect_a_session_that_asked_to_end() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let mut connect = connect_v5("liar");
        connect.properties.session_expiry_interval = Some(0);
        let first = broker.connect("", &connect, &mut out);

        let goodbye = Packet::Disconnect(Disconnect {
            reason: ReasonCode::Success,
            properties: Properties {
                session_expiry_interval: Some(300),
                ..Properties::default()
            },
        });
        assert_eq!(
            broker.handle(first.subscriber, &goodbye, &mut out),
            Err(ReasonCode::ProtocolError),
            "[MQTT-3.14.4-3]: zero on CONNECT cannot become non-zero on DISCONNECT"
        );
    }

    #[test]
    fn a_disconnect_repeating_an_interval_of_zero_is_not_an_error() {
        // [MQTT-3.14.4-3] forbids raising an interval of zero, not
        // restating it. An explicit `Some(0)` on a session already at
        // zero asks for nothing it did not have, and a client that
        // spells out its default is common; refusing it would close a
        // conforming connection with a protocol error.
        //
        // This is the half the two tests above cannot reach: both send
        // a non-zero interval, so a check that dropped the
        // `requested != 0` term and rejected every zero passed them.
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let mut connect = connect_v5("plainspoken");
        connect.properties.session_expiry_interval = Some(0);
        let first = broker.connect("", &connect, &mut out);

        let goodbye = Packet::Disconnect(Disconnect {
            reason: ReasonCode::Success,
            properties: Properties {
                session_expiry_interval: Some(0),
                ..Properties::default()
            },
        });
        assert_eq!(
            broker.handle(first.subscriber, &goodbye, &mut out),
            Ok(()),
            "3.14.2.2.2: restating an interval of zero is legal"
        );

        // And it meant it: the session still ends with the connection.
        broker.tick(&mut out);
        let second = broker.connect("", &connect, &mut out);
        assert!(
            !second.session_present,
            "zero still means the session ends with its connection"
        );
    }
}
