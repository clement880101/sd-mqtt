//! The broker: sessions, subscriptions and routing, assembled.

pub mod route;
pub mod subscribe;

use std::collections::HashMap;
use std::sync::Arc;

use foldhash::fast::RandomState;
use mqtt_codec::packet::Connect;
use mqtt_codec::{Limits, Packet, ReasonCode};

use crate::clock::CoarseClock;
use crate::keepalive::{Activity, KeepAliveRing};
use crate::registry::{Registration, Registry, SessionId};
use crate::retained::{RetainedMessage, RetainedStore};
use crate::session::message::Message;
use crate::session::queue::Delivery;
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

    /// One session, for a metric or an assertion.
    pub fn session(&self, subscriber: SubscriberId) -> Option<&Session> {
        self.registry.get(subscriber)
    }

    /// Accept a connection.
    ///
    /// Taking over a session that already has a live connection is not an
    /// error: the newcomer wins and the incumbent is told why.
    pub fn connect(
        &mut self,
        mountpoint: &str,
        connect: &Connect,
        out: &mut Vec<Addressed>,
    ) -> Registration {
        let now = self.clock.now();
        let identity = SessionId::new(mountpoint, &connect.client_id);
        let registration = self.registry.register(identity, connect.clean_start, now);

        if registration.took_over {
            out.push((
                registration.subscriber,
                Action::Close(ReasonCode::SessionTakenOver),
            ));
            // The incumbent never disconnected, so nothing disarmed its
            // keep-alive. Left in the ring it would expire on the
            // displaced connection's silence and close the newcomer.
            self.keepalive.remove(registration.subscriber);
        }

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
        session.set_expiry_interval(
            connect
                .properties
                .session_expiry_interval
                .unwrap_or_default(),
        );
        session.set_will(connect.will.as_ref().map(|will| StoredWill {
            topic: will.topic.clone(),
            payload: will.payload.clone(),
            qos: will.qos,
            retain: will.retain,
            delay: will.properties.will_delay_interval.unwrap_or_default(),
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

    /// Handle a packet from a connected client.
    ///
    /// An error is the reason to close the connection with; the caller
    /// does the closing.
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
            message.expires_at = publish
                .properties
                .message_expiry_interval
                .map(|interval| self.clock.deadline(interval));

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
        self.activities.remove(&subscriber);
        if let Some(session) = self.registry.get_mut(subscriber) {
            match packet {
                Some(reason) => session.on_disconnect_packet(reason, now),
                None => session.on_disconnected(false, now),
            }
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
            // its will is owed.
            self.detach(subscriber, None, now);
        }

        // Wills first: a session that expires this tick may also owe one.
        let due: Vec<(SubscriberId, StoredWill)> = self
            .registry
            .iter_mut()
            .filter_map(|(id, session)| session.take_due_will(now).map(|will| (*id, will)))
            .collect();
        for (publisher, will) in due {
            let message = will_message(&will);
            self.publish(message, Some(publisher), now, out);
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
