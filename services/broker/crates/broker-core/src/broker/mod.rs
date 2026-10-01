//! The broker: sessions, subscriptions and routing, assembled.

pub mod route;
pub mod subscribe;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use foldhash::fast::RandomState;
use mqtt_codec::packet::Connect;
use mqtt_codec::{Limits, Packet, Properties, QoS, ReasonCode, Version};
use storage::record::Record;

use crate::clock::CoarseClock;
use crate::hooks::{ClientInfo, Credentials, Disposition, Hooks};
use crate::journal::{self, DurableSession, DurableState};
use crate::keepalive::{Activity, KeepAliveRing};
use crate::registry::{Presence, Registration, Registry, SessionId};
use crate::retained::{RetainedMessage, RetainedStore};
use crate::session::message::Message;
use crate::session::queue::{Delivery, DropReason, QueueLimits};
use crate::session::state::{will_message, Session, StoredWill};
use crate::session::Action;
use crate::subscriptions::{FilterEvent, SharedSubscriptions, SubscriberId, SubscriptionTrie};

/// The inflight window a session has when nothing narrows it, from
/// section 6 of the design.
pub const DEFAULT_MAX_INFLIGHT: u16 = 20;

/// How the broker is configured.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Protocol limits handed to the codec and the validators.
    pub limits: Limits,
    /// How many shards the subscription trie uses.
    pub trie_shards: usize,
    /// The largest keep-alive interval a client may have.
    ///
    /// Section 5.4: a version 5 client proposing more than this is
    /// clamped to it and told so in the connack, because a clamp the
    /// client is not told about disconnects it while it believes it may
    /// still be silent. The shell applies both halves; the broker only
    /// arms whatever interval it is handed.
    ///
    /// Zero disables the ceiling and nothing else. It is not the size
    /// of the keep-alive ring: that has its own floor, because a
    /// ceiling of none says nothing about how many buckets a sweep
    /// should be spread over. See [`KeepAliveRing::new`].
    ///
    /// [`KeepAliveRing::new`]: crate::keepalive::KeepAliveRing::new
    pub max_keep_alive: u16,
    /// The largest inflight window a session may have.
    ///
    /// Section 6: a session's window is the lesser of this and the
    /// peer's receive maximum. One gives strict ordering, and is the
    /// only way to ask for it that does not depend on the client.
    pub max_inflight: u16,
    /// How many messages a session's queue may hold waiting for a
    /// window slot.
    pub max_queue_messages: usize,
    /// How many bytes those messages may occupy in total.
    ///
    /// Separate from the count because either bound alone is
    /// escapable: a count permits a thousand enormous payloads, a byte
    /// total permits a million empty ones.
    pub max_queue_bytes: usize,
    /// How many connectors a publication may have been through.
    ///
    /// Section 9.2's third loop, and [`Broker::originate`] is where it
    /// is applied: a publication arriving there having already been
    /// through this many connectors is dropped rather than routed, and
    /// counted.
    ///
    /// **Node-wide rather than per rule**, because a hop count is a
    /// property of a chain that crosses rules and a per-rule number can
    /// only say what one link of it allows. `mqtt_bridge.max_hops`
    /// stays what it is: a bridge's own check, made before a message
    /// costs a network round trip, and the one that carries the marker
    /// with it.
    ///
    /// Zero would drop everything a connector originates, so
    /// `Config::validate` refuses it, exactly as it refuses a bridge's
    /// zero.
    pub max_hops: u32,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            trie_shards: 32,
            max_keep_alive: 3600,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            // Taken from the queue's own defaults rather than restated,
            // so that the two cannot drift apart while both claim to be
            // the documented number.
            max_queue_messages: QueueLimits::default().max_messages,
            max_queue_bytes: QueueLimits::default().max_bytes,
            max_hops: DEFAULT_MAX_HOPS,
        }
    }
}

/// How many connectors a publication may have been through by default.
///
/// The same number `mqtt_bridge.max_hops` defaults to, and named once
/// so the two cannot drift while both claim to be the documented
/// figure. Eight is deep enough that no honest chain of connectors
/// reaches it and shallow enough that a cycle is stopped in under a
/// second.
pub const DEFAULT_MAX_HOPS: u32 = 8;

/// An action addressed to one session, for the caller to perform.
pub type Addressed = (SubscriberId, Action);

/// A publication that had been through too many connectors to make
/// another hop.
///
/// Section 9.2: "Every publication a connector originates carries a hop
/// count, and one over the configured maximum is dropped and counted."
/// This is the dropping; the counting is the caller's, because this
/// crate holds no metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "this publication has already been through {hops} connectors and the limit is \
     {maximum}, so it is dropped rather than published. Raise broker.max_hops, or find \
     the cycle: a rule feeding a system that publishes back through another rule is one \
     no single rule can see."
)]
pub struct Looped {
    /// How many connectors it had already been through.
    pub hops: u32,
    /// What [`BrokerConfig::max_hops`] allows.
    pub maximum: u32,
}

/// What the hooks said to do with an arriving message.
///
/// Three outcomes and not two, because a message a hook refused and a
/// message a hook consumed are acknowledged differently: the first is
/// "not authorized", which tells the client to stop, and the second is
/// "no matching subscribers", which is the truth about where it went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// Match it and deliver it.
    Route,
    /// A hook took it. Nobody else sees it.
    Dropped,
    /// The authorizing chain said no.
    Refused,
}

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
    /// The connectors, section 9.3, in the order they are asked.
    ///
    /// Held here rather than in [`BrokerConfig`] because installing
    /// one is not configuration a shell can clone and pass around: a
    /// hook is a live object with state of its own, and two brokers
    /// sharing a configuration must not silently share it.
    hooks: Hooks,
    /// Everything that changed durably and has not been handed over.
    ///
    /// Section 7.3: the broker does no I/O, so it says what happened
    /// into here and the shell writes it. The shell drains this under
    /// the same lock that filled it, so a record and the actions it
    /// belongs with are always taken together and in order.
    journal: Vec<Record>,
    /// Which filters gained or lost a subscriber on this node, and has
    /// not been handed over.
    ///
    /// Section 8.2 replicates that a node has at least one subscriber
    /// for a filter, so the shell has to know when that changed. This
    /// is beside [`Self::journal`] and not in it, and the reason is the
    /// stamp: a replicated write is ordered by a hybrid logical clock
    /// reading and this node's identity, and neither is something this
    /// crate has or should gain — it reads no clock, and the identity
    /// belongs to `cluster`, which rests on nothing of ours.
    ///
    /// So this says what happened and the shell decides what is
    /// written, which is exactly the division [`Self::journal`]'s
    /// comment describes. See [`crate::subscriptions::events`] for why
    /// it is one event per session and filter rather than one per
    /// packet, and for the three paths that must emit a departure.
    filter_events: Vec<FilterEvent>,
}

impl Broker {
    /// A broker with no sessions, its clock at zero.
    pub fn new(config: BrokerConfig) -> Self {
        Self {
            clock: Arc::new(CoarseClock::new(0)),
            subscriptions: SubscriptionTrie::new(config.trie_shards),
            shared: SharedSubscriptions::new(config.trie_shards),
            retained: RetainedStore::new(),
            registry: Registry::new(
                usize::from(config.max_inflight),
                QueueLimits {
                    max_messages: config.max_queue_messages,
                    max_bytes: config.max_queue_bytes,
                    ..QueueLimits::default()
                },
            ),
            keepalive: KeepAliveRing::new(config.max_keep_alive),
            activities: HashMap::with_hasher(RandomState::default()),
            config,
            hooks: Hooks::new(),
            journal: Vec::new(),
            filter_events: Vec::new(),
        }
    }

    /// The hooks this broker fires, to install one or to read the
    /// defaults.
    ///
    /// Section 9.3, and [`crate::hooks`] for what a hook may not do:
    /// it is called synchronously, under whatever lock the shell holds
    /// around this broker, and nothing enforces that it returns
    /// promptly.
    pub fn hooks_mut(&mut self) -> &mut Hooks {
        &mut self.hooks
    }

    /// The hooks this broker fires.
    pub fn hooks(&self) -> &Hooks {
        &self.hooks
    }

    /// Who a handle belongs to, for a hook that has to name it.
    ///
    /// `None` once the session is gone, which is the ordinary case for
    /// anything said about a session after it was removed.
    fn client_of(&self, subscriber: SubscriberId) -> Option<&SessionId> {
        self.registry.get(subscriber).map(Session::id)
    }

    /// Move a session's actions into the caller's list, telling the
    /// hooks about the ones they asked to hear.
    ///
    /// **The one place actions leave a session**, so that the delivery
    /// and discard hooks cannot be missed by a path that grew its own
    /// loop. A retained message handed out on subscribe, a redelivery
    /// on resume and a live fan-out all come through here.
    fn emit(&self, subscriber: SubscriberId, actions: Vec<Action>, out: &mut Vec<Addressed>) {
        if !self.hooks.is_empty() {
            if let Some(id) = self.client_of(subscriber) {
                let client = ClientInfo {
                    subscriber: Some(subscriber),
                    mountpoint: &id.mountpoint,
                    client_id: &id.client_id,
                };
                tell_hooks(&self.hooks, &client, &actions);
            }
        }
        for action in actions {
            out.push((subscriber, action));
        }
    }

    /// Move everything the broker has said onto the end of `out`.
    ///
    /// Drained by the shell under the same lock that filled it, which is
    /// what keeps a record and the actions it belongs with together and
    /// in order. Section 7.3: the shell commits this, waits for the
    /// commit to be durable when something is about to be acknowledged,
    /// and only then performs the actions.
    pub fn take_journal(&mut self, out: &mut Vec<Record>) {
        out.append(&mut self.journal);
    }

    /// Move every filter that gained or lost a subscriber onto `out`.
    ///
    /// Section 8.2, and [`Self::filter_events`] for why these are not
    /// records. Drained by the shell under the same lock that filled
    /// it, so that the order a filter was gained and lost in is the
    /// order the registers are written in — reversed, the node would
    /// end up advertising a filter it had just given up.
    pub fn take_filter_events(&mut self, out: &mut Vec<FilterEvent>) {
        out.append(&mut self.filter_events);
    }

    /// How many of this node's sessions are subscribed to `filter`.
    ///
    /// Walks the trie, so it is for a test and for an operator and not
    /// for a routing path. The count the replication layer keeps is its
    /// own, fed by [`Self::take_filter_events`]; this is what says the
    /// two agree.
    #[must_use]
    pub fn local_subscribers(&self, filter: &str) -> usize {
        let mut matched = 0;
        for (_, session) in self.registry.iter() {
            matched += session
                .filters()
                .filter(|held| crate::subscriptions::inner_filter(held) == filter)
                .count();
        }
        matched
    }

    /// Throw away everything said so far, everywhere it is held.
    ///
    /// For [`crate::restore`] and nothing else. Restore rebuilds a
    /// broker by walking the same paths a live client walks, so those
    /// paths say what they always say; but it is reading what was
    /// already written down, not deciding anything new, and writing it
    /// back would be a second copy of a state the store already holds.
    ///
    /// It reaches into every session as well as the broker's own
    /// buffer, because a record left in a session's buffer is not
    /// discarded by clearing this one: it would simply be handed over
    /// by whichever call next drained that session, long after the
    /// restore that produced it.
    ///
    /// # It does not discard the filter events, and that is the point
    ///
    /// [`Self::filter_events`] survives this, although the reasoning
    /// above would seem to apply to it just as well. It does not,
    /// because the thing those events feed is **not in the store at
    /// all**: section 8.2's replication keeps a reference count per
    /// filter, deliberately unreplicated and deliberately unstored, and
    /// a restore replaying this node's subscriptions is the only thing
    /// that rebuilds it. Discarding them would leave the count at zero
    /// with the trie full, so the first genuine last-unsubscribe would
    /// find nothing to decrement and would tell the cluster nothing —
    /// and the node would stay advertised for a filter it no longer
    /// serves until somebody noticed.
    ///
    /// The cost is that a node re-announces its filters as it comes
    /// back, one register per filter rather than per subscription. That
    /// is a cost worth paying twice over: a peer may have tombstoned
    /// this node while it was down, or may never have heard from it at
    /// all, and a node that came back silently would be routed nothing.
    pub fn discard_journal(&mut self) {
        for (_, session) in self.registry.iter_mut() {
            session.take_journal(&mut self.journal);
        }
        self.journal.clear();
    }

    /// Everything about this broker that must survive it.
    ///
    /// Durable state only. What it leaves out, and why leaving it out is
    /// the point rather than a simplification, is on [`DurableState`].
    pub fn durable_state(&self) -> DurableState {
        let mut bodies = BTreeMap::new();
        let mut sessions = BTreeMap::new();

        for (_, session) in self.registry.iter() {
            let record = session.record();
            let subscriptions = session
                .subscriptions()
                .iter()
                .map(|held| (held.filter.clone(), held.clone()))
                .collect();
            let mut inbound: Vec<u16> = session.inbound.held().collect();
            inbound.sort_unstable();

            let mut inflight = Vec::new();
            for entry in session.outbound.outstanding() {
                bodies.insert(
                    journal::stored_message_id(entry.delivery.message.id),
                    journal::body_record(&entry.delivery.message),
                );
                inflight.push(journal::inflight_record(entry));
            }
            // In arrival order rather than send order; see the field.
            inflight.sort_by_key(|entry| entry.sequence);
            let mut queue = Vec::new();
            for item in session.outbound.waiting() {
                bodies.insert(
                    journal::stored_message_id(item.message.id),
                    journal::body_record(&item.message),
                );
                queue.push(journal::queue_entry(item));
            }

            sessions.insert(
                record.key.clone(),
                DurableSession {
                    session: record,
                    subscriptions,
                    inbound,
                    inflight,
                    queue,
                    highest_sequence: session.outbound.highest_sequence(),
                },
            );
        }

        let mut held = Vec::new();
        self.retained.entries(&mut held);
        let retained = held
            .into_iter()
            .map(|(topic, message)| (topic, journal::retained_record(&message)))
            .collect();

        DurableState {
            sessions,
            retained,
            bodies,
        }
    }

    /// The retained store, so that a path rebuilding this broker from
    /// storage can put back what was held.
    pub fn retained(&self) -> &RetainedStore {
        &self.retained
    }

    /// How many sessions are holding records nothing has taken.
    ///
    /// Zero at the end of every call into the broker. A record left in a
    /// session's own buffer is one the shell has not committed, and
    /// section 7.3 has the shell committing before it acknowledges, so
    /// a late drain is a broken promise rather than an untidy one. It
    /// walks every session, which is why it is for tests and not for a
    /// metric.
    pub fn sessions_holding_records(&self) -> usize {
        self.registry
            .iter()
            .filter(|(_, session)| session.has_unsaid())
            .count()
    }

    /// Hand over everything one session has said.
    fn drain_session(&mut self, subscriber: SubscriberId) {
        if let Some(session) = self.registry.get_mut(subscriber) {
            session.take_journal(&mut self.journal);
        }
    }

    /// The clock, so the shell can advance it and a test can read it.
    pub fn clock(&self) -> &Arc<CoarseClock> {
        &self.clock
    }

    /// What this broker was configured with.
    ///
    /// The shell reads the inflight maximum from here rather than
    /// holding its own copy, so that the two cannot disagree about a
    /// number one of them narrows and the other enforces.
    pub fn config(&self) -> &BrokerConfig {
        &self.config
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

    /// Every session, connected or not, for a report.
    ///
    /// Immutable and in no particular order, because the registry is
    /// a hash map. A caller that needs an order must impose one, and
    /// the admin API does.
    ///
    /// **Whoever calls this is holding the process's one lock.** It
    /// walks every session, so a caller that does anything per
    /// session beyond reading a field is doing that work with every
    /// connection in the process waiting behind it. The admin API
    /// takes what it needs here and builds its answer afterwards; see
    /// `sdmqtt-admin`'s `resources` module for why the shape is what
    /// it is.
    pub fn sessions(&self) -> impl Iterator<Item = (SubscriberId, &Session)> {
        self.registry.iter().map(|(id, session)| (*id, session))
    }

    /// Every session in identity order, starting strictly after
    /// `after`.
    ///
    /// **Whoever calls this is holding the process's one lock**, as
    /// with [`Broker::sessions`] — but this one can be stopped. The
    /// iterator is lazy over an ordered index, so a caller taking a
    /// page of a hundred looks at a hundred sessions rather than at
    /// every session in the broker. That is what the admin API's
    /// pages are built on; see `sdmqtt-admin`'s `resources` module.
    pub fn sessions_after(
        &self,
        after: Option<&SessionId>,
    ) -> impl Iterator<Item = (SubscriberId, &Session)> {
        self.registry.iter_after(after)
    }

    /// The same, including the session `from` names rather than
    /// starting past it.
    ///
    /// For a reader resuming *inside* a session — the admin API's
    /// subscriptions page, whose cursor names a filter within one.
    /// See [`Registry::iter_from`].
    ///
    /// [`Registry::iter_from`]: crate::registry::Registry::iter_from
    pub fn sessions_from(
        &self,
        from: Option<&SessionId>,
    ) -> impl Iterator<Item = (SubscriberId, &Session)> {
        self.registry.iter_from(match from {
            Some(from) => std::ops::Bound::Included(from),
            None => std::ops::Bound::Unbounded,
        })
    }

    /// How many sessions have a connection attached.
    ///
    /// Read from a maintained count rather than by filtering every
    /// session, which measured 7.89 milliseconds with a hundred
    /// thousand of them under this broker's one lock. See
    /// [`AttachedSessions`].
    ///
    /// This is the broker's count and not the shell's. `Shared`'s
    /// outbox count is how many connections the server is writing
    /// to, which is a different question with a different answer
    /// during a takeover, and section 11.3's `connections` gauge is
    /// deliberately the shell's.
    ///
    /// [`AttachedSessions`]: crate::session::AttachedSessions
    pub fn connected_count(&self) -> usize {
        self.registry.attached()
    }

    /// The same number by asking every session, for the test that
    /// keeps the maintained one honest.
    pub fn connected_count_by_walking(&self) -> usize {
        self.registry.attached_by_walking()
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
    ///
    /// # When it refuses
    ///
    /// The authenticate hook chain runs first, and an error is the
    /// reason code to answer the connect with, the same shape
    /// [`Broker::handle`] uses. **Nothing at all has happened when it
    /// refuses**: no session is registered, no incumbent is displaced
    /// and no will is armed, because a rejected connection must not be
    /// able to end a session that is serving somebody else. That is
    /// what puts the chain before [`Broker::displace_incumbent`]
    /// rather than after it.
    ///
    /// [`Broker::displace_incumbent`]: Broker::displace_incumbent
    pub fn connect(
        &mut self,
        mountpoint: &str,
        connect: &Connect,
        out: &mut Vec<Addressed>,
    ) -> Result<Registration, ReasonCode> {
        let now = self.clock.now();
        let identity = SessionId::new(mountpoint, &connect.client_id);

        if !self.hooks.authenticate(&Credentials {
            mountpoint,
            client_id: &connect.client_id,
            username: connect.username.as_deref(),
            password: connect.password.as_deref(),
            version: connect.version,
            clean_start: connect.clean_start,
        }) {
            // 3.2.2.2: "Not authorized" is the answer to a connect the
            // server will not serve. [MQTT-3.1.4-6] requires the
            // network connection to be closed after it, which is the
            // caller's half.
            return Err(ReasonCode::NotAuthorized);
        }

        let displaced = self.displace_incumbent(&identity, connect.clean_start, now, out);
        // What the handle was subscribed to before any of this, read
        // while the session that held it still exists. Section 8.2
        // needs a departure per filter and the registry is about to
        // replace the session wholesale; see `subscribe::remove_held`.
        let held: Vec<String> = self
            .registry
            .subscriber_of(&identity)
            .and_then(|subscriber| self.registry.get(subscriber))
            .map(|session| session.filters().map(str::to_string).collect())
            .unwrap_or_default();
        if connect.clean_start {
            // The registry is about to replace this session wholesale,
            // taking its queue and its window with it. What it was
            // holding is reported before it goes.
            if let Some(existing) = self.registry.subscriber_of(&identity) {
                self.discard_held(existing, out);
                // Said before the session is replaced, because the
                // buffer holding it is part of the session and goes
                // when it does. Everything the discard released must
                // reach the shell, and it can only reach it from here.
                self.drain_session(existing);
            }
            // [MQTT-3.1.2-4]: a clean start discards the session. The
            // record takes its subscriptions, its queue, its window and
            // its inbound identifiers with it, so nothing else needs to
            // be said about them, and it must come before the record
            // that creates the replacement. A session the store does
            // not hold is a no-op, which is what this is for a client
            // that has never connected.
            self.journal.push(Record::SessionRemoved {
                session: journal::key_of(&identity),
            });
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
                subscribe::remove_held(
                    &self.subscriptions,
                    &self.shared,
                    session,
                    &held,
                    &mut self.filter_events,
                );
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
        self.emit(registration.subscriber, actions, out);

        let activity = Activity::new(now);
        self.activities
            .insert(registration.subscriber, activity.clone());
        self.keepalive
            .insert(registration.subscriber, connect.keep_alive, activity, now);

        self.drain_session(registration.subscriber);
        // Last, because a hook told about a connection must be able to
        // ask this broker's observers about it and find it there. A
        // resumed session's redeliveries are already on their way by
        // now, which is the same order a subscriber would see them in.
        self.tell_connected(registration.subscriber);
        Ok(registration)
    }

    /// Tell the hooks a connection was accepted.
    fn tell_connected(&self, subscriber: SubscriberId) {
        if let Some(id) = self.client_of(subscriber) {
            self.hooks.client_connected(&ClientInfo {
                subscriber: Some(subscriber),
                mountpoint: &id.mountpoint,
                client_id: &id.client_id,
            });
        }
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

        // Said before the will goes out, because publishing it routes
        // to other sessions and the order records are handed over in is
        // the order they happened in.
        self.drain_session(incumbent);

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
    ///
    /// # A will fires the message hooks, and that was a decision
    ///
    /// It did not, until the day a connector's publication started to.
    /// The argument that settles it: **a will is a message the client
    /// published**, in advance, in its CONNECT. To an application,
    /// `device/1/status = offline` arriving as a will is
    /// indistinguishable from the device having published it and should
    /// be. A rule that fires on a device coming up and not on it going
    /// down is broken in the one case anybody installed it for.
    ///
    /// The argument against is cost: a tick that owes a thousand wills
    /// would run the hook chain a thousand times inside a sweep. It
    /// does not hold. Routing a will is a fan-out, which is far more
    /// work than a hook call, so the sweep was never bounded in the
    /// number of wills due and this does not change its order.
    ///
    /// **[`Hook::authorize_publish`] is not asked**, and that half is
    /// deliberate too. [MQTT-3.1.2-8] makes publishing the will the
    /// server's obligation once the connection ended abnormally, and
    /// the client is gone and cannot be told it was refused. Judging a
    /// will belongs at CONNECT, where there is still somebody to answer;
    /// that is section 12.1's and milestone 7's.
    ///
    /// A hook may still **drop** a will, which is a power worth having:
    /// suppressing the will of a client being taken down deliberately.
    /// Nobody receives it and it is not retained, exactly as for a
    /// client's own publish.
    ///
    /// # What still fires nothing
    ///
    /// [`Broker::publish`] itself, which is now reached only by this
    /// and by a dead letter. A dead letter is the broker reporting that
    /// a rule's endpoint has stopped answering, and firing that rule
    /// with it would be a cycle with a guaranteed period.
    fn publish_will(
        &mut self,
        publisher: SubscriberId,
        will: &StoredWill,
        now: u32,
        out: &mut Vec<Addressed>,
    ) {
        let mut message = will_message(will);
        message.expires_at = self.deadline(&message.properties);
        if !self.hooks.is_empty() {
            // The session is still registered on both paths that reach
            // here — a will is taken off a session and nothing removes
            // one between taking it and this — so the `else` of this
            // `if let` is unreachable by construction. It publishes
            // anyway rather than returning, because the will is owed by
            // [MQTT-3.1.2-8] and an identity we have mislaid is our bug
            // rather than a reason to swallow somebody's message.
            //
            // Cloned because the hook chain is borrowed from `self`
            // beside it.
            if let Some(identity) = self.client_of(publisher).cloned() {
                let client = ClientInfo {
                    subscriber: Some(publisher),
                    mountpoint: &identity.mountpoint,
                    client_id: &identity.client_id,
                };
                if self.hooks.message_received(&client, &mut message) == Disposition::Drop {
                    return;
                }
            }
        }
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
        let outcome = self.handle_inner(subscriber, packet, out);
        // Drained on every path out, including the failing ones: a
        // packet that ends the connection may still have changed
        // durable state on its way to doing so, and an error is not a
        // reason to lose what it changed.
        self.drain_session(subscriber);
        outcome
    }

    fn handle_inner(
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
                // Cloned before the session is borrowed mutably, and
                // it is two atomic increments: the chain has to be
                // told who is asking, and who is asking lives in the
                // session the subscribe is about to change.
                let identity = self
                    .registry
                    .get(subscriber)
                    .map(Session::id)
                    .cloned()
                    .ok_or(ReasonCode::ProtocolError)?;
                let client = ClientInfo {
                    subscriber: Some(subscriber),
                    mountpoint: &identity.mountpoint,
                    client_id: &identity.client_id,
                };
                // Borrowed from the field rather than through `self`,
                // because the session below is a mutable borrow of a
                // different one.
                let hooks = &self.hooks;
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
                    &mut |filter: &str, qos| hooks.authorize_subscribe(&client, filter, qos),
                    &mut actions,
                    &mut self.filter_events,
                );
                actions.push(Action::Send(Packet::SubAck(mqtt_codec::packet::SubAck {
                    packet_id: request.packet_id,
                    reasons,
                    properties: Default::default(),
                })));
                self.emit(subscriber, actions, out);
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
                    &mut self.filter_events,
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
        self.emit(subscriber, actions, out);
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
        let mut refused = false;
        let delivered = if received.dispatch {
            let mut message = Message::new(
                &publish.topic,
                publish.payload.clone(),
                publish.qos,
                publish.retain,
            );
            message.properties = Arc::new(publish.properties.clone().into());
            message.expires_at = self.deadline(&publish.properties);

            match self.judge(from, &mut message) {
                Verdict::Route => Some(self.publish(message, Some(from), now, out)),
                // Nobody received it, which is what the
                // acknowledgement will say. The message is not
                // retained either, because retaining it would leave
                // the thing a hook refused sitting on the topic for
                // the next subscriber.
                Verdict::Dropped => Some(0),
                Verdict::Refused => {
                    refused = true;
                    None
                }
            }
        } else {
            None
        };

        // The acknowledgement is owed until now, because whether anyone
        // wanted the message is only known after routing. It is paid on
        // every path that got this far, including the one that dispatched
        // nothing: a slot the acknowledgement never frees throttles the
        // client to death with nothing reporting it.
        self.acknowledge(from, &received, delivered, refused, out);
        Ok(())
    }

    /// What the authorizing and message hooks say should happen to an
    /// arriving message.
    ///
    /// **Both before matching, and in that order.** Authorization
    /// judges what the client actually asked to publish, so it is
    /// asked first and sees the message unrewritten; a rewrite that
    /// ran first would have the chain approving a topic the client
    /// never sent. And a rewritten topic decides where the message
    /// goes, so both have to be over before anything is matched: a
    /// rewrite applied afterwards would route by the old topic and
    /// deliver the new one.
    fn judge(&mut self, from: SubscriberId, message: &mut Message) -> Verdict {
        if self.hooks.is_empty() {
            // The common case, and it must not pay for a clone of the
            // identity. With no hooks installed the only thing left
            // to consult is the default.
            return if self.hooks.defaults().publish {
                Verdict::Route
            } else {
                Verdict::Refused
            };
        }
        let Some(identity) = self.client_of(from).cloned() else {
            return Verdict::Refused;
        };
        let client = ClientInfo {
            subscriber: Some(from),
            mountpoint: &identity.mountpoint,
            client_id: &identity.client_id,
        };
        if !self.hooks.authorize_publish(&client, message) {
            return Verdict::Refused;
        }
        match self.hooks.message_received(&client, message) {
            Disposition::Deliver => Verdict::Route,
            Disposition::Drop => Verdict::Dropped,
        }
    }

    /// Pay what the receive path said was owed.
    ///
    /// `delivered` is how many subscribers the message reached, or `None`
    /// when it was not dispatched at all. Only a dispatch that reached
    /// nobody may say so: a redelivery already held was dispatched once,
    /// and claiming nobody wanted it would be a lie about the first copy.
    ///
    /// `refused` is the authorizing chain's answer and outranks both,
    /// because "you may not publish here" and "nobody was listening"
    /// are different things to tell a client and only one of them is
    /// worth changing its behaviour over.
    fn acknowledge(
        &mut self,
        subscriber: SubscriberId,
        received: &crate::session::Received,
        delivered: Option<usize>,
        refused: bool,
        out: &mut Vec<Addressed>,
    ) {
        let Some(owed) = received.owed else {
            return;
        };
        let reason = match (refused, delivered) {
            // 3.4.2.1: "Not authorized" is a puback and a pubrec
            // reason code. At level zero there is no acknowledgement
            // to carry it, so the message is simply not routed —
            // which is what [MQTT-3.3.5-2] permits, and less
            // aggressive than closing the connection over one packet.
            (true, _) => ReasonCode::NotAuthorized,
            (false, Some(0)) => ReasonCode::NoMatchingSubscribers,
            (false, _) => ReasonCode::Success,
        };

        let mut actions = Vec::new();
        if let Some(session) = self.registry.get_mut(subscriber) {
            session
                .inbound
                .acknowledge(owed.packet_id, owed.qos, reason, &mut actions);
        }
        self.emit(subscriber, actions, out);
    }

    /// Publish a message that has no session behind it, firing the
    /// message hooks on the way.
    ///
    /// # This is what makes section 9.2 true
    ///
    /// Section 9.2 says a bridge publishes inbound messages into the
    /// local broker "where any rule can act on them". It was not true
    /// while every caller without a session went straight to
    /// [`Broker::publish`], which is the route a will takes and fires
    /// no message hook: a message arriving over a bridge, through
    /// ingest or from a poller reached every subscriber and triggered
    /// no rule. The difference is whether connectors compose. A
    /// gateway posting to ingest whose message should then fan out to
    /// a webhook is the obvious thing somebody will try, and it did
    /// nothing.
    ///
    /// So this exists beside [`Broker::publish`] rather than replacing
    /// it. A will is the broker speaking for a client that is gone, a
    /// dead letter is the broker reporting on itself, and neither is a
    /// message a connector originated; they keep the route that fires
    /// nothing. See the ruling in [`crate::hooks`].
    ///
    /// # The authorizing hooks are not asked, deliberately
    ///
    /// [`Hook::authorize_publish`] judges what a client asked to
    /// publish, and there is no client here. What let this publication
    /// in was the ingest rule's bearer token, the admin listener's
    /// token, or a bridge somebody wrote into the configuration file —
    /// all of them decisions an operator already made. Asking the
    /// authorizing chain as well would let an access-control hook
    /// written about clients silently switch off a configured
    /// connector, which is not a power that hook was given.
    ///
    /// # The hop count, and what happens at the limit
    ///
    /// Here, because here is where a cycle becomes possible: a
    /// connector's publication firing the hooks is what lets one
    /// connector's output become another's input. A publication that
    /// has already been through [`BrokerConfig::max_hops`] connectors
    /// is **not routed, not retained and fires no rule**, and the
    /// caller is told so rather than answered — section 9.2's "dropped
    /// and counted".
    ///
    /// Above the hooks rather than below them, so the cycle stops
    /// instead of being noticed one hop after it was extended.
    ///
    /// # Errors
    ///
    /// [`Looped`], which is the publication being over the limit.
    /// Nothing was written and nothing was delivered.
    pub fn originate(
        &mut self,
        mut message: Message,
        origin: &str,
        now: u32,
        out: &mut Vec<Addressed>,
    ) -> Result<usize, Looped> {
        let hops = crate::hops::count_of(&message.properties.user_properties).unwrap_or(0);
        if hops > self.config.max_hops {
            return Err(Looped {
                hops,
                maximum: self.config.max_hops,
            });
        }
        if self.hooks.is_empty() {
            // The same shortcut the client path takes: with no hooks
            // installed there is nothing to ask and no identity worth
            // building. The default for publish is not consulted,
            // because that default answers the authorizing chain and
            // the authorizing chain is not asked here.
            return Ok(self.publish(message, None, now, out));
        }
        let client = ClientInfo {
            subscriber: None,
            mountpoint: "",
            client_id: origin,
        };
        match self.hooks.message_received(&client, &mut message) {
            // Nobody received it, and it is not retained either: a hook
            // that refused a message must not leave it sitting on the
            // topic for the next subscriber. The same rule the client
            // path applies.
            Disposition::Drop => Ok(0),
            Disposition::Deliver => Ok(self.publish(message, None, now, out)),
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
            // which the store itself knows. The record says the same
            // thing the store was just told, because the two must not
            // disagree about whether a topic still holds a message.
            if message.payload.is_empty() {
                self.journal.push(Record::RetainedRemoved {
                    topic: message.topic.to_string(),
                });
            } else {
                self.journal.push(Record::RetainedPut {
                    topic: message.topic.to_string(),
                    message: storage::record::RetainedRecord {
                        payload: message.payload.clone(),
                        qos: journal::stored_qos(message.qos),
                        expires_at: message.expires_at,
                        properties: message.properties.encoded(),
                    },
                });
            }
            self.retained.set(
                &message.topic,
                RetainedMessage {
                    payload: message.payload.clone(),
                    qos: message.qos,
                    expires_at: message.expires_at,
                    // The stored Application Message is the whole of it,
                    // properties included: [MQTT-3.3.2-17] requires User
                    // Properties to be forwarded unaltered, and a
                    // retained delivery is a forward. Shared rather than
                    // cloned, as the live fan-out below shares them.
                    properties: Arc::clone(&message.properties),
                },
            );
        }

        let mut routed = Vec::new();
        // Section 6's "a member whose queue is full is skipped". The
        // selector cannot see a session and is not given one; it is
        // asked again instead, and this answers whether the member it
        // offered could take the message.
        //
        // The whole message and the level this member would be
        // delivered at, not a byte count: a queue refuses a
        // fire-and-forget message to a session with nobody attached,
        // and an expired one, however empty it is. Asking only about
        // size let an offline member answer "room" for a publish it
        // then dropped, while an idle member received nothing.
        let registry = &self.registry;
        let mut has_room = |subscriber: SubscriberId, qos: QoS| {
            registry
                .get(subscriber)
                .is_some_and(|session| !session.outbound.would_refuse(&message, qos, now))
        };
        route::route(
            &self.subscriptions,
            &self.shared,
            &message,
            publisher,
            &mut has_room,
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
                        sequence: 0,
                    },
                    now,
                    &mut actions,
                );
            }
            self.emit(target.subscriber, actions, out);
            self.drain_session(target.subscriber);
        }

        reached
    }

    /// A packet was too large to send and has been discarded.
    ///
    /// [MQTT-3.1.2-24] forbids sending a packet larger than the
    /// Maximum Packet Size the client advertised, and [MQTT-3.1.2-25]
    /// says to behave as if delivery had completed. This is that
    /// second half: releasing the identifier and the inflight slot
    /// rather than leaving both held for an acknowledgement that
    /// cannot arrive.
    ///
    /// The check belongs to the writer, which is the only place the
    /// packet's true size is known without either a second size
    /// calculation that would drift from the encoder or a throwaway
    /// encode on the hot path. So this is the way back in.
    ///
    /// Nothing happens when the identifier names nothing outstanding;
    /// see [`Outbound::on_discarded`] for why the caller cannot
    /// promise that it does.
    ///
    /// [`Outbound::on_discarded`]: crate::session::Outbound::on_discarded
    pub fn discard_oversize(
        &mut self,
        subscriber: SubscriberId,
        packet_id: u16,
        now: u32,
        out: &mut Vec<Addressed>,
    ) {
        let mut actions = Vec::new();
        if let Some(session) = self.registry.get_mut(subscriber) {
            session.outbound.on_discarded(packet_id, now, &mut actions);
        }
        self.emit(subscriber, actions, out);
        self.drain_session(subscriber);
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
        let mut departed = false;
        if let Some(session) = self.registry.get_mut(subscriber) {
            // Only a session that was attached has a connection to
            // lose. Without this, a second call — and there are two
            // paths that can make one, a disconnect packet followed
            // by the socket closing — would tell the hooks the same
            // client left twice.
            departed = session.is_connected();
            match packet {
                Some(reason) => session.on_disconnect_packet(reason, now),
                None => session.on_disconnected(false, now),
            }
        }
        // A departure changes when the session expires and whether it
        // still owes a will, both of which must survive it.
        self.drain_session(subscriber);
        if departed {
            if let Some(id) = self.client_of(subscriber) {
                self.hooks.client_disconnected(&ClientInfo {
                    subscriber: Some(subscriber),
                    mountpoint: &id.mountpoint,
                    client_id: &id.client_id,
                });
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
    fn discard_held(&mut self, subscriber: SubscriberId, out: &mut Vec<Addressed>) {
        let mut actions = Vec::new();
        if let Some(session) = self.registry.get_mut(subscriber) {
            session
                .outbound
                .clear(DropReason::SessionEnded, &mut actions);
        }
        self.emit(subscriber, actions, out);
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
            // The session said the will is no longer owed as it handed
            // it over; that must be written down before the publication
            // it causes.
            self.drain_session(publisher);
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
                let identity = session.id().clone();
                let key = journal::key_of(&identity);
                let client = ClientInfo {
                    subscriber: Some(subscriber),
                    mountpoint: &identity.mountpoint,
                    client_id: &identity.client_id,
                };
                subscribe::remove_all(
                    &self.subscriptions,
                    &self.shared,
                    &mut session,
                    &mut self.filter_events,
                );
                // Not through `emit`: the session is already out of
                // the registry, so there is nothing left there to
                // look its identity up by. The drops it owes its
                // hooks are owed all the same.
                let mut actions = Vec::new();
                session
                    .outbound
                    .clear(DropReason::SessionEnded, &mut actions);
                tell_hooks(&self.hooks, &client, &actions);
                for action in actions {
                    out.push((subscriber, action));
                }
                // Taken out of the session before it is dropped, and
                // before the record that ends it: the removal is what
                // makes every one of these refer to a session the store
                // no longer holds.
                session.take_journal(&mut self.journal);
                self.journal.push(Record::SessionRemoved { session: key });
                // Last, and after the drops: a hook told a session
                // ended must already have been told what ending it
                // discarded.
                self.hooks.session_expired(&client);
            }
            self.keepalive.remove(subscriber);
            self.activities.remove(&subscriber);
        }

        // A retained message reclaimed here is gone from the store too,
        // or it stays on disk for the life of the file: every reader
        // skips an expired message, so nothing would ever notice.
        let mut reclaimed = Vec::new();
        self.retained.sweep_expired(now, &mut reclaimed);
        for topic in reclaimed {
            self.journal.push(Record::RetainedRemoved { topic });
        }
    }

    /// How many sessions exist, connected or not.
    pub fn session_count(&self) -> usize {
        self.registry.len()
    }

    /// Which sessions have gained or lost a connection since a reader
    /// last looked.
    ///
    /// For an observer that would otherwise walk every session to
    /// find out, which is what the events stream did: see
    /// [`Presence`]. Nothing is recorded until a reader arms it, and
    /// arming it and taking a first picture belong under one
    /// acquisition of this broker's lock.
    ///
    /// [`Presence`]: crate::registry::Presence
    pub fn presence(&self) -> &Presence {
        self.registry.presence()
    }

    /// How many subscriptions are held, for a metric and for tests.
    ///
    /// Both halves are read from a maintained number rather than
    /// walked. The trie's own walk measured 2.64 milliseconds with a
    /// hundred thousand subscriptions, and this is one of section
    /// 11.3's gauges: the events sampler reads it once a second and
    /// every metrics scrape reads it again, both under this broker's
    /// one lock. See [`QueueTotal`], which is the same answer to the
    /// same question one milestone earlier.
    ///
    /// [`QueueTotal`]: crate::session::QueueTotal
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len() + self.shared.member_count()
    }

    /// The same number by walking the trie and every group, for the
    /// test that keeps the maintained one honest.
    pub fn subscription_count_by_walking(&self) -> usize {
        self.subscriptions.len_by_walking() + self.shared.member_count_by_walking()
    }

    /// How many messages are waiting in every session's queue.
    ///
    /// Read from a maintained total rather than summed over the
    /// sessions, and the reason is on [`QueueTotal`]: the walk was
    /// measured at 405 microseconds with a hundred thousand sessions,
    /// and it would be held under this broker's one lock every time
    /// a scraper asked.
    ///
    /// [`QueueTotal`]: crate::session::QueueTotal
    pub fn queue_depth(&self) -> usize {
        self.registry.queued()
    }

    /// The same number by walking every session, for the test that
    /// keeps the maintained one honest.
    pub fn queue_depth_by_walking(&self) -> usize {
        self.registry.queued_by_walking()
    }

    /// How many messages the retained store holds, expired or not.
    ///
    /// It exists so that the expiry sweep in [`Broker::tick`] can be
    /// asserted, and there is no other way to see it. Every route into
    /// the store applies the deadline on read, so an expired message
    /// that no sweep has reclaimed is already invisible: deleting the
    /// sweep from the tick changes no answer the protocol can observe
    /// and leaks the memory instead. Counting what is stored rather
    /// than what is live is the one view that tells those two apart,
    /// which is also why this must not skip expired messages.
    pub fn retained_count(&self) -> usize {
        self.retained.len()
    }

    /// The same number by walking the tree, for the test that keeps
    /// the maintained one honest.
    ///
    /// [`RetainedStore::len`] reads the length of the ordered index
    /// the store keeps beside its tree, because the walk was measured
    /// at 2.66 milliseconds with a hundred thousand topics and this
    /// number is read once a second by the sampler and again on every
    /// scrape. An index that drifts from the tree is worse than a slow
    /// walk, so the two are compared after every history
    /// `tests/journal.rs` generates.
    ///
    /// [`RetainedStore::len`]: crate::retained::RetainedStore::len
    pub fn retained_count_by_walking(&self) -> usize {
        self.retained.count_by_walking()
    }
}

/// Tell the hooks what a batch of actions says happened.
///
/// A publish on its way to a session is a delivery; a discard is a
/// drop. Everything else — acknowledgements, ping responses, closes —
/// is protocol machinery no connector asked about.
///
/// Free rather than a method, because the one caller that matters most
/// runs after the session has already been taken out of the registry:
/// a session destroyed by expiry still owes its hooks the drops that
/// destroying it caused, and there is nothing left to look it up by.
fn tell_hooks(hooks: &Hooks, client: &ClientInfo<'_>, actions: &[Action]) {
    for action in actions {
        match action {
            Action::Send(Packet::Publish(publish)) => hooks.message_delivered(client, publish),
            Action::Dropped {
                reason, qos, topic, ..
            } => hooks.message_dropped(client, topic, *qos, *reason),
            _ => {}
        }
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
    use bytes::Bytes;
    use mqtt_codec::packet::{
        Connect, Disconnect, Publish, RetainHandling, Subscribe, SubscriptionOptions,
    };
    use mqtt_codec::QoS;

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
        let first = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");
        broker.disconnect(first.subscriber, &mut out);

        // Far past any plausible deadline.
        broker.clock().advance(1_000_000);
        broker.tick(&mut out);

        let second = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");
        assert!(
            second.session_present,
            "[MQTT-3.1.2-4]: a 3.1.1 session with CleanSession clear is stored"
        );
    }

    #[test]
    fn a_three_one_one_session_with_clean_session_set_does_not_outlive_its_connection() {
        // The other half of the test above, and the half that was
        // unguarded: that one was added when reading the clear flag as
        // "ends with the connection" was the bug, so only the clear
        // case has ever been asserted. Deleting the arm that reads the
        // set flag leaves every 3.1.1 session living for the maximum
        // interval, which is [MQTT-3.1.2-6] broken in the direction
        // nothing complains about: a peer-driven leak of sessions that
        // never expire, one per identifier it cares to invent.
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let mut connect = connect_v311("ephemeral");
        connect.clean_start = true;
        let first = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");
        broker.disconnect(first.subscriber, &mut out);
        broker.tick(&mut out);

        assert_eq!(
            broker.session_count(),
            0,
            "[MQTT-3.1.2-6]: the session must not outlast its connection"
        );

        // And asked for by name rather than inferred from the count: a
        // resuming connect is what would find it if it were still there.
        let mut resuming = connect_v311("ephemeral");
        resuming.clean_start = false;
        let second = broker
            .connect("", &resuming, &mut out)
            .expect("the default decision authenticates");
        assert!(
            !second.session_present,
            "[MQTT-3.1.2-6]: there is nothing left to resume"
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
        let first = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");
        broker.disconnect(first.subscriber, &mut out);
        broker.tick(&mut out);

        let second = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");
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
        let first = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");

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
        let second = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");
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
        let first = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");

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
        let first = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");

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
        let second = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates");
        assert!(
            !second.session_present,
            "zero still means the session ends with its connection"
        );
    }

    /// Retain a message carrying properties, then subscribe from a
    /// second client and read what it was sent.
    ///
    /// The store keeping the properties is half the fix; this is the
    /// other half. A store that keeps them and a delivery path that
    /// drops them look identical from inside `retained.rs`, so the
    /// store's own test cannot tell the two apart.
    #[test]
    fn a_retained_delivery_carries_the_properties_it_was_published_with() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let publisher = broker
            .connect("", &connect_v5("publisher"), &mut out)
            .expect("the default decision authenticates");
        let keepsake = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: "retain/probe".to_string(),
            packet_id: None,
            payload: Bytes::from_static(b"body"),
            properties: Properties {
                content_type: Some("text/plain".to_string()),
                user_properties: vec![("a".to_string(), "2".to_string())],
                ..Properties::default()
            },
        });
        broker
            .handle(publisher.subscriber, &keepsake, &mut out)
            .expect("the publish is accepted");

        let newcomer = broker
            .connect("", &connect_v5("newcomer"), &mut out)
            .expect("the default decision authenticates");
        out.clear();
        broker
            .handle(newcomer.subscriber, &subscribe_to("retain/probe"), &mut out)
            .expect("the subscribe is accepted");

        let sent = publishes_to(&out, newcomer.subscriber);
        let [delivered] = sent.as_slice() else {
            panic!("one retained delivery, got {}", sent.len());
        };
        assert_eq!(
            delivered.properties.user_properties,
            vec![("a".to_string(), "2".to_string())],
            "[MQTT-3.3.2-17]: user properties are forwarded unaltered"
        );
        assert_eq!(
            delivered.properties.content_type.as_deref(),
            Some("text/plain"),
            "the content type is part of the stored application message"
        );
    }

    /// The seam between the store and the delivery path, end to end.
    ///
    /// A retained message is the worst case for [MQTT-3.3.2-6]: it can
    /// sit for hours, and the interval it was published with is stale
    /// the moment it is stored. The store keeps the publisher's figure
    /// on purpose and the outbound path rewrites it from the deadline,
    /// so neither half can be checked from inside the other.
    ///
    /// This is also the only test that the deadline itself survives
    /// into the delivery. Dropping `expires_at` on the way out of the
    /// store passes every other test, because the store applies its
    /// own deadline on read and the delivery's is never read again.
    #[test]
    fn a_retained_delivery_reports_the_expiry_that_is_left_after_the_wait() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let publisher = broker
            .connect("", &connect_v5("publisher"), &mut out)
            .expect("the default decision authenticates");
        let keepsake = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic: "retain/perishable".to_string(),
            packet_id: None,
            payload: Bytes::from_static(b"body"),
            properties: Properties {
                message_expiry_interval: Some(600),
                ..Properties::default()
            },
        });
        broker
            .handle(publisher.subscriber, &keepsake, &mut out)
            .expect("the publish is accepted");

        // A hundred seconds pass with nobody subscribed.
        run_to(&mut broker, 100, &mut out);

        let newcomer = broker
            .connect("", &connect_v5("newcomer"), &mut out)
            .expect("the default decision authenticates");
        out.clear();
        broker
            .handle(
                newcomer.subscriber,
                &subscribe_to("retain/perishable"),
                &mut out,
            )
            .expect("the subscribe is accepted");

        let sent = publishes_to(&out, newcomer.subscriber);
        let [delivered] = sent.as_slice() else {
            panic!("one retained delivery, got {}", sent.len());
        };
        assert_eq!(
            delivered.properties.message_expiry_interval,
            Some(500),
            "[MQTT-3.3.2-6]: a hundred of the six hundred were spent waiting"
        );
    }

    /// One quality-of-service zero publication, with no properties.
    fn publication(topic: &str, payload: &'static [u8], retain: bool) -> Packet {
        Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain,
            topic: topic.to_string(),
            packet_id: None,
            payload: Bytes::from_static(payload),
            properties: Properties::default(),
        })
    }

    /// The same, carrying properties.
    fn publication_with(
        topic: &str,
        payload: &'static [u8],
        retain: bool,
        properties: Properties,
    ) -> Packet {
        let Packet::Publish(mut publish) = publication(topic, payload, retain) else {
            unreachable!("publication builds a publish");
        };
        publish.properties = properties;
        Packet::Publish(publish)
    }

    /// The topic of every publication addressed to one subscriber.
    fn topics_to(out: &[Addressed], subscriber: SubscriberId) -> Vec<&str> {
        publishes_to(out, subscriber)
            .into_iter()
            .map(|publish| publish.topic.as_str())
            .collect()
    }

    #[test]
    fn an_empty_retained_publish_still_reaches_the_subscribers_matching_at_the_time() {
        // 3.3.1.3: "If the Payload contains zero bytes it is processed
        // normally by the Server but any retained message with the same
        // topic name MUST be removed and any future subscribers for the
        // topic will not receive a retained message" [MQTT-3.3.1-6],
        // and it "MUST NOT be stored as a retained message"
        // [MQTT-3.3.1-7]. 3.1.1 spells out what "processed normally"
        // covers: "sent to Clients with a subscription matching the
        // topic name" [MQTT-3.3.1-10].
        //
        // The removal half is guarded inside `retained.rs`, which
        // cannot see a delivery; before this test nothing in the
        // workspace published an empty payload through a broker at
        // all, so the live half was guarded by nothing and discarding
        // the message outright looked identical.
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let publisher = broker
            .connect("", &connect_v5("publisher"), &mut out)
            .expect("the default decision authenticates");
        broker
            .handle(
                publisher.subscriber,
                &publication("retain/cleared", b"body", true),
                &mut out,
            )
            .expect("the publish is accepted");

        let sub = broker
            .connect("", &connect_v5("sub"), &mut out)
            .expect("the default decision authenticates");
        broker
            .handle(sub.subscriber, &subscribe_to("retain/+"), &mut out)
            .expect("the subscribe is accepted");
        assert_eq!(
            topics_to(&out, sub.subscriber),
            vec!["retain/cleared"],
            "the subscriber is caught up before the clear, or the next \
             assertion is about the wrong message"
        );
        out.clear();

        broker
            .handle(
                publisher.subscriber,
                &publication("retain/cleared", b"", true),
                &mut out,
            )
            .expect("the empty publish is accepted");

        let sent = publishes_to(&out, sub.subscriber);
        let [delivered] = sent.as_slice() else {
            panic!("one live delivery, got {}", sent.len());
        };
        assert_eq!(delivered.topic, "retain/cleared");
        assert!(
            delivered.payload.is_empty(),
            "the message delivered is the empty one that was published"
        );

        // The other half, and a positive assertion rather than an
        // absence: a newcomer is sent the sibling topic that still
        // holds a message and not the one that was cleared, so a
        // delivery path that had simply stopped working would fail
        // here too.
        broker
            .handle(
                publisher.subscriber,
                &publication("retain/kept", b"body", true),
                &mut out,
            )
            .expect("the publish is accepted");
        let newcomer = broker
            .connect("", &connect_v5("newcomer"), &mut out)
            .expect("the default decision authenticates");
        out.clear();
        broker
            .handle(newcomer.subscriber, &subscribe_to("retain/+"), &mut out)
            .expect("the subscribe is accepted");

        assert_eq!(
            topics_to(&out, newcomer.subscriber),
            vec!["retain/kept"],
            "the cleared topic is not stored and reaches no future subscriber"
        );
    }

    #[test]
    fn the_retained_sweep_reclaims_an_expired_message_nobody_has_read() {
        // `tick` calls the sweep, and deleting that call changes no
        // other test: the deadline is enforced on read as well, so an
        // unswept message is already absent from every answer and only
        // the memory stays. That makes the store's size the one
        // observation that can tell a sweep that ran from one that did
        // not, and it is why `retained_count` exists.
        //
        // On the boundary, not around it: a message expires strictly
        // after its deadline, so second ten still holds it.
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let publisher = broker
            .connect("", &connect_v5("publisher"), &mut out)
            .expect("the default decision authenticates");

        broker
            .handle(
                publisher.subscriber,
                &publication_with(
                    "retain/perishable",
                    b"body",
                    true,
                    Properties {
                        message_expiry_interval: Some(10),
                        ..Properties::default()
                    },
                ),
                &mut out,
            )
            .expect("the publish is accepted");
        assert_eq!(broker.retained_count(), 1, "it is retained to begin with");

        run_to(&mut broker, 10, &mut out);
        assert_eq!(
            broker.retained_count(),
            1,
            "the deadline itself is not past"
        );

        run_to(&mut broker, 11, &mut out);
        assert_eq!(
            broker.retained_count(),
            0,
            "the second after it, the sweep gives the memory back"
        );
    }

    #[test]
    fn a_publication_carries_its_response_topic_and_correlation_data() {
        // 4.10: the pair is how a responder learns where to reply and
        // how the requester matches the reply to its request, and
        // [MQTT-3.3.2-15] and [MQTT-3.3.2-16] make both the receiver's
        // to be sent unaltered. Guarded today only on the will path,
        // where a different piece of code assembles the message, so a
        // fan-out that dropped either one passed the whole suite.
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let sub = broker
            .connect("", &connect_v5("sub"), &mut out)
            .expect("the default decision authenticates");
        broker
            .handle(
                sub.subscriber,
                &subscribe_to("request/temperature"),
                &mut out,
            )
            .expect("the subscribe is accepted");
        let publisher = broker
            .connect("", &connect_v5("publisher"), &mut out)
            .expect("the default decision authenticates");
        out.clear();

        broker
            .handle(
                publisher.subscriber,
                &publication_with(
                    "request/temperature",
                    b"now?",
                    false,
                    Properties {
                        response_topic: Some("reply/17".to_string()),
                        correlation_data: Some(Bytes::from_static(b"\x00\xffcid")),
                        ..Properties::default()
                    },
                ),
                &mut out,
            )
            .expect("the publish is accepted");

        let sent = publishes_to(&out, sub.subscriber);
        let [delivered] = sent.as_slice() else {
            panic!("one delivery, got {}", sent.len());
        };
        assert_eq!(
            delivered.properties.response_topic.as_deref(),
            Some("reply/17"),
            "[MQTT-3.3.2-15]: the response topic reaches the receiver"
        );
        assert_eq!(
            delivered.properties.correlation_data,
            Some(Bytes::from_static(b"\x00\xffcid")),
            "[MQTT-3.3.2-16]: correlation data is binary and travels unaltered"
        );
    }

    /// A subscribe to one filter, asking for retained messages.
    fn subscribe_to(filter: &str) -> Packet {
        Packet::Subscribe(Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: filter.to_string(),
                qos: QoS::AtMostOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: RetainHandling::SendAlways,
            }],
            properties: Properties::default(),
        })
    }

    // ---- what is replicated, at node granularity: section 8.2 --------

    /// Unsubscribe from one filter.
    fn unsubscribe_from(filter: &str) -> Packet {
        Packet::Unsubscribe(mqtt_codec::packet::Unsubscribe {
            packet_id: 2,
            filters: vec![filter.to_string()],
            properties: Properties::default(),
        })
    }

    /// Everything the broker has said about filters, drained.
    fn filter_events(broker: &mut Broker) -> Vec<FilterEvent> {
        let mut events = Vec::new();
        broker.take_filter_events(&mut events);
        events
    }

    /// Connect a client that keeps its session for an hour.
    fn durable(broker: &mut Broker, client: &str, out: &mut Vec<Addressed>) -> SubscriberId {
        let mut connect = connect_v5(client);
        connect.properties.session_expiry_interval = Some(3_600);
        broker
            .connect("", &connect, out)
            .expect("the default decision authenticates")
            .subscriber
    }

    /// **A filter gaining a second local subscriber says nothing new.**
    ///
    /// Section 8.2's first easy-to-miss edge, walked through the broker
    /// rather than through the counting type: convention 5's last entry
    /// is a guard that built its type by hand and so never reached the
    /// caller that was supposed to use it. Two clients subscribe to one
    /// filter through SUBSCRIBE packets, and the second must produce
    /// exactly one arrival of its own — which the replication layer
    /// counts to two and broadcasts nothing for.
    ///
    /// The count the broker can see is asserted beside it, because an
    /// event emitted and a subscription inserted are two different
    /// claims and a test of one is not a test of the other.
    #[test]
    fn a_second_subscriber_to_a_filter_is_one_more_arrival_and_nothing_else() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let first = durable(&mut broker, "first", &mut out);
        let second = durable(&mut broker, "second", &mut out);
        assert_eq!(
            filter_events(&mut broker),
            Vec::new(),
            "a connect subscribes to nothing"
        );

        broker
            .handle(first, &subscribe_to("sensor/+/temp"), &mut out)
            .expect("the subscribe is accepted");
        assert_eq!(
            filter_events(&mut broker),
            vec![FilterEvent::Arrived("sensor/+/temp".to_owned())]
        );

        broker
            .handle(second, &subscribe_to("sensor/+/temp"), &mut out)
            .expect("the subscribe is accepted");
        assert_eq!(
            filter_events(&mut broker),
            vec![FilterEvent::Arrived("sensor/+/temp".to_owned())],
            "the second subscriber to a filter must be one arrival, no more and no less"
        );
        assert_eq!(broker.local_subscribers("sensor/+/temp"), 2);
    }

    /// A session re-subscribing to a filter it holds says nothing.
    ///
    /// The protocol's rule is that a second subscribe to the same filter
    /// changes the options rather than adding a subscription, and the
    /// count has to agree: an arrival per packet would be incremented
    /// twice by this client and would never come back down, so the
    /// filter would stay advertised after its only subscriber left.
    #[test]
    fn re_subscribing_to_a_held_filter_is_not_another_arrival() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let only = durable(&mut broker, "only", &mut out);

        broker
            .handle(only, &subscribe_to("a/b"), &mut out)
            .expect("the subscribe is accepted");
        assert_eq!(
            filter_events(&mut broker),
            vec![FilterEvent::Arrived("a/b".to_owned())]
        );

        // The same filter at a different quality of service, which is
        // the options changing and not a new subscription.
        broker
            .handle(only, &subscribe_at("a/b", QoS::ExactlyOnce), &mut out)
            .expect("the subscribe is accepted");
        assert_eq!(
            filter_events(&mut broker),
            Vec::new(),
            "changing a held filter's options was reported as a new subscriber"
        );
        assert_eq!(broker.local_subscribers("a/b"), 1);

        // And the one departure still arrives, which is what the count
        // above would have lost.
        broker
            .handle(only, &unsubscribe_from("a/b"), &mut out)
            .expect("the unsubscribe is accepted");
        assert_eq!(
            filter_events(&mut broker),
            vec![FilterEvent::Departed("a/b".to_owned())]
        );
    }

    /// **A filter losing its last local subscriber departs.**
    ///
    /// Section 8.2's second edge. The departure is what the replication
    /// layer turns into a tombstone; without it every peer goes on
    /// routing to this node for a filter it has no subscriber for.
    #[test]
    fn the_last_unsubscribe_from_a_filter_is_the_departure_that_tombstones_it() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let first = durable(&mut broker, "first", &mut out);
        let second = durable(&mut broker, "second", &mut out);
        broker
            .handle(first, &subscribe_to("a/b"), &mut out)
            .expect("the subscribe is accepted");
        broker
            .handle(second, &subscribe_to("a/b"), &mut out)
            .expect("the subscribe is accepted");
        drop(filter_events(&mut broker));

        broker
            .handle(first, &unsubscribe_from("a/b"), &mut out)
            .expect("the unsubscribe is accepted");
        assert_eq!(
            filter_events(&mut broker),
            vec![FilterEvent::Departed("a/b".to_owned())]
        );
        assert_eq!(broker.local_subscribers("a/b"), 1);

        broker
            .handle(second, &unsubscribe_from("a/b"), &mut out)
            .expect("the unsubscribe is accepted");
        assert_eq!(
            filter_events(&mut broker),
            vec![FilterEvent::Departed("a/b".to_owned())],
            "the last unsubscribe from a filter must depart"
        );
        assert_eq!(broker.local_subscribers("a/b"), 0);
    }

    /// Unsubscribing from a filter nothing held says nothing.
    ///
    /// The protocol answers "no subscription existed" and the count must
    /// agree: a departure here would take the count below zero, and a
    /// count that cannot reach zero again never tombstones the filter.
    #[test]
    fn unsubscribing_from_a_filter_nothing_held_is_not_a_departure() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let only = durable(&mut broker, "only", &mut out);
        broker
            .handle(only, &unsubscribe_from("a/b"), &mut out)
            .expect("the unsubscribe is accepted");
        assert_eq!(filter_events(&mut broker), Vec::new());
    }

    /// A session expiring departs from every filter it held.
    ///
    /// One of the two teardown paths nobody thinks of. The session is
    /// never unsubscribed from anything; it simply stops existing, and
    /// the trie entries go with it. A departure missed here leaves the
    /// node advertised for the filter until it restarts — and a restart
    /// would not fix it either, because the subscription is gone from
    /// the store too.
    #[test]
    fn a_session_expiring_departs_from_every_filter_it_held() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let mut connect = connect_v5("brief");
        connect.properties.session_expiry_interval = Some(1);
        let only = broker
            .connect("", &connect, &mut out)
            .expect("the default decision authenticates")
            .subscriber;
        broker
            .handle(only, &subscribe_to("a/b"), &mut out)
            .expect("the subscribe is accepted");
        broker
            .handle(only, &subscribe_to("c/d"), &mut out)
            .expect("the subscribe is accepted");
        drop(filter_events(&mut broker));

        broker.disconnect(only, &mut out);
        run_to(&mut broker, 60, &mut out);
        assert_eq!(broker.session_count(), 0, "the session must have expired");

        let mut departed: Vec<String> = filter_events(&mut broker)
            .into_iter()
            .map(|event| match event {
                FilterEvent::Departed(filter) => filter,
                FilterEvent::Arrived(filter) => {
                    panic!("an expiring session reported an arrival for {filter}")
                }
            })
            .collect();
        departed.sort();
        assert_eq!(departed, vec!["a/b".to_owned(), "c/d".to_owned()]);
        assert_eq!(broker.local_subscribers("a/b"), 0);
    }

    /// A clean-start connect departs from everything the old session held.
    ///
    /// The other teardown path, and the one that hides: a clean-start
    /// takeover replaces the session wholesale and keeps the handle, so
    /// only this layer can clear the trie — which is exactly why only
    /// this layer can report the departures.
    #[test]
    fn a_clean_start_connect_departs_from_what_the_old_session_held() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let first = durable(&mut broker, "same", &mut out);
        broker
            .handle(first, &subscribe_to("a/b"), &mut out)
            .expect("the subscribe is accepted");
        drop(filter_events(&mut broker));

        let mut again = connect_v5("same");
        again.clean_start = true;
        broker
            .connect("", &again, &mut out)
            .expect("the default decision authenticates");
        assert_eq!(
            filter_events(&mut broker),
            vec![FilterEvent::Departed("a/b".to_owned())],
            "a clean start kept the trie entry or kept it quiet"
        );
        assert_eq!(broker.local_subscribers("a/b"), 0);
    }

    /// A shared subscription is an arrival on the filter topics match.
    ///
    /// Section 8.3 routes by matching a published topic, and `$share/g/`
    /// is how this node shares a delivery among its own clients: a peer
    /// has nothing to do with the group. So two groups on one filter are
    /// two subscribers to that filter, and the last of them to go is what
    /// departs.
    #[test]
    fn a_shared_subscription_arrives_and_departs_on_its_inner_filter() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let only = durable(&mut broker, "worker", &mut out);

        broker
            .handle(only, &subscribe_to("$share/one/a/b"), &mut out)
            .expect("the subscribe is accepted");
        assert_eq!(
            filter_events(&mut broker),
            vec![FilterEvent::Arrived("a/b".to_owned())],
            "a shared subscription was reported under its group"
        );
        broker
            .handle(only, &subscribe_to("$share/two/a/b"), &mut out)
            .expect("the subscribe is accepted");
        assert_eq!(
            filter_events(&mut broker),
            vec![FilterEvent::Arrived("a/b".to_owned())]
        );
        assert_eq!(broker.local_subscribers("a/b"), 2);

        broker
            .handle(only, &unsubscribe_from("$share/one/a/b"), &mut out)
            .expect("the unsubscribe is accepted");
        broker
            .handle(only, &unsubscribe_from("$share/two/a/b"), &mut out)
            .expect("the unsubscribe is accepted");
        assert_eq!(
            filter_events(&mut broker),
            vec![
                FilterEvent::Departed("a/b".to_owned()),
                FilterEvent::Departed("a/b".to_owned())
            ]
        );
        assert_eq!(broker.local_subscribers("a/b"), 0);
    }

    /// A refused subscribe is not an arrival.
    ///
    /// Nothing went into the trie, so nothing may be advertised. A node
    /// that announced a filter its authorizing chain had refused would
    /// be asked for traffic it would then throw away.
    #[test]
    fn a_refused_subscribe_is_not_an_arrival() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let only = durable(&mut broker, "only", &mut out);
        broker.hooks_mut().set_defaults(crate::hooks::Defaults {
            subscribe: false,
            ..crate::hooks::Defaults::default()
        });
        broker
            .handle(only, &subscribe_to("a/b"), &mut out)
            .expect("the subscribe is answered, per filter");
        assert_eq!(
            filter_events(&mut broker),
            Vec::new(),
            "a filter the chain refused was advertised to the cluster"
        );
    }

    /// An invalid filter is not an arrival either.
    #[test]
    fn an_invalid_filter_is_not_an_arrival() {
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();
        let only = durable(&mut broker, "only", &mut out);
        broker
            .handle(only, &subscribe_to("a/#/b"), &mut out)
            .expect("the subscribe is answered, per filter");
        assert_eq!(filter_events(&mut broker), Vec::new());
    }

    /// A subscribe to one filter at the given quality of service.
    fn subscribe_at(filter: &str, qos: QoS) -> Packet {
        let Packet::Subscribe(mut subscribe) = subscribe_to(filter) else {
            unreachable!("subscribe_to builds a subscribe");
        };
        subscribe.filters[0].qos = qos;
        Packet::Subscribe(subscribe)
    }

    #[test]
    fn the_configured_window_bounds_what_a_session_holds_unacknowledged() {
        // The configured half of section 6's "lesser of", at the layer
        // that owns it: there is no shell here, so nothing narrows the
        // window to a peer's advertisement and what is left is the
        // configured number on its own. A window of one is how section
        // 6 says strict ordering is asked for, and before this task it
        // could only be had from a client advertising a receive
        // maximum of one.
        for max_inflight in [1u16, 3] {
            let mut broker = Broker::new(BrokerConfig {
                max_inflight,
                ..BrokerConfig::default()
            });
            let mut out = Vec::new();
            let sub = broker
                .connect("", &connect_v5("sub"), &mut out)
                .expect("the default decision authenticates");
            broker
                .handle(
                    sub.subscriber,
                    &subscribe_at("a/b", QoS::AtLeastOnce),
                    &mut out,
                )
                .expect("the subscribe is accepted");
            out.clear();

            // Two more than the window, so the count below is the
            // window rather than the supply.
            for _ in 0..usize::from(max_inflight) + 2 {
                broker.publish(
                    Message::new("a/b", Bytes::from_static(b"x"), QoS::AtLeastOnce, false),
                    None,
                    0,
                    &mut out,
                );
            }

            assert_eq!(
                publishes_to(&out, sub.subscriber).len(),
                usize::from(max_inflight),
                "a configured window of {max_inflight} sends that many and queues the rest"
            );
        }
    }

    /// Every message dropped on the way to one subscriber, with why.
    fn drops_to(out: &[Addressed], subscriber: SubscriberId) -> Vec<DropReason> {
        out.iter()
            .filter_map(|(target, action)| match action {
                Action::Dropped { reason, .. } if *target == subscriber => Some(*reason),
                _ => None,
            })
            .collect()
    }

    /// A subscriber with a window of one, so that everything after the
    /// first message it is offered is in the queue rather than in
    /// flight, and the queue's bounds are what decide its fate.
    fn subscriber_with_a_full_window(
        broker: &mut Broker,
        out: &mut Vec<Addressed>,
    ) -> SubscriberId {
        let sub = broker
            .connect("", &connect_v5("sub"), out)
            .expect("the default decision authenticates")
            .subscriber;
        broker
            .handle(sub, &subscribe_at("a/b", QoS::AtLeastOnce), out)
            .expect("the subscribe is accepted");
        out.clear();
        sub
    }

    /// One message of the size the tests below budget for.
    fn queued_message(payload: &[u8]) -> Message {
        Message::new(
            "a/b",
            Bytes::copy_from_slice(payload),
            QoS::AtLeastOnce,
            false,
        )
    }

    #[test]
    fn the_configured_message_bound_bounds_a_sessions_queue() {
        // Section 6 calls the queue bounds configured. They were two
        // literals in `QueueLimits::default`, and a test that only
        // read the number back out of the configuration would not have
        // noticed that nothing carried it to a session.
        //
        // On the boundary, not around it: with room for two, the
        // second is queued and the third is not.
        let mut broker = Broker::new(BrokerConfig {
            max_inflight: 1,
            max_queue_messages: 2,
            ..BrokerConfig::default()
        });
        let mut out = Vec::new();
        let sub = subscriber_with_a_full_window(&mut broker, &mut out);

        // One fills the window, two fill the queue, the fourth has
        // nowhere to go.
        for _ in 0..4 {
            broker.publish(queued_message(b"x"), None, 0, &mut out);
        }

        assert_eq!(publishes_to(&out, sub).len(), 1, "the window holds one");
        assert_eq!(
            drops_to(&out, sub),
            vec![DropReason::QueueFull],
            "two queued and exactly one refused"
        );
    }

    #[test]
    fn the_configured_byte_bound_bounds_a_sessions_queue() {
        // The other half of the pair. A count alone permits a thousand
        // enormous payloads, so this bound is the one that makes the
        // memory budget a budget, and it is the knob an operator
        // reaches for.
        let payload = [b'x'; 256];
        let size = queued_message(&payload).size_in_queue();
        let mut broker = Broker::new(BrokerConfig {
            max_inflight: 1,
            // Room for exactly two, measured rather than guessed: the
            // queue refuses a message that would take it past the
            // budget, so two of this size fit and a third does not.
            max_queue_bytes: size * 2,
            ..BrokerConfig::default()
        });
        let mut out = Vec::new();
        let sub = subscriber_with_a_full_window(&mut broker, &mut out);

        for _ in 0..4 {
            broker.publish(queued_message(&payload), None, 0, &mut out);
        }

        assert_eq!(publishes_to(&out, sub).len(), 1, "the window holds one");
        assert_eq!(
            drops_to(&out, sub),
            vec![DropReason::ByteBudget],
            "two of that size fit the budget and a third does not"
        );
    }

    /// Two sessions sharing a group on `a/b`, each also holding an
    /// ordinary subscription to a private filling topic so that its
    /// queue can be filled alone, without spending any of the group's
    /// rotation and without touching the other member.
    ///
    /// Returns them in the order they joined the group, which is the
    /// order the rotation asks them in.
    fn a_group_of_two(broker: &mut Broker, out: &mut Vec<Addressed>) -> [SubscriberId; 2] {
        let mut members = Vec::new();
        for name in ["first", "second"] {
            let member = broker
                .connect("", &connect_v5(name), out)
                .expect("the default decision authenticates")
                .subscriber;
            broker
                .handle(
                    member,
                    &subscribe_at(&fill_topic(name), QoS::AtLeastOnce),
                    out,
                )
                .expect("the subscribe is accepted");
            broker
                .handle(member, &subscribe_at("$share/g/a/b", QoS::AtLeastOnce), out)
                .expect("the shared subscribe is accepted");
            members.push(member);
        }
        out.clear();
        [members[0], members[1]]
    }

    /// The private topic one group member's queue is filled through.
    fn fill_topic(name: &str) -> String {
        format!("fill/{name}")
    }

    /// Fill one member's window and queue, so that the next message
    /// offered to it is refused.
    ///
    /// The broker here is configured with a window of one and a queue of
    /// one, so two messages are exactly full: one in flight, one waiting.
    fn fill(broker: &mut Broker, member: SubscriberId, name: &str, out: &mut Vec<Addressed>) {
        for _ in 0..2 {
            broker.publish(
                Message::new(
                    &fill_topic(name),
                    Bytes::from_static(b"x"),
                    QoS::AtLeastOnce,
                    false,
                ),
                None,
                0,
                out,
            );
        }
        assert!(
            drops_to(out, member).is_empty(),
            "the filling itself must not overflow, or the test is measuring that instead"
        );
        out.clear();
    }

    /// A broker whose sessions hold one message in flight and one queued.
    fn brimful() -> Broker {
        Broker::new(BrokerConfig {
            max_inflight: 1,
            max_queue_messages: 1,
            ..BrokerConfig::default()
        })
    }

    #[test]
    fn a_shared_member_whose_queue_is_full_is_skipped_for_one_with_room() {
        // Section 6: "A member whose queue is full is skipped and the
        // next is tried." The rotation asks `first` because it joined
        // first, so without the skip the message dies in its full queue
        // while `second` sits idle.
        let mut broker = brimful();
        let mut out = Vec::new();
        let [first, second] = a_group_of_two(&mut broker, &mut out);
        fill(&mut broker, first, "first", &mut out);

        broker.publish(
            Message::new("a/b", Bytes::from_static(b"x"), QoS::AtLeastOnce, false),
            None,
            0,
            &mut out,
        );

        assert_eq!(
            publishes_to(&out, second).len(),
            1,
            "the member with room receives it"
        );
        assert!(
            drops_to(&out, first).is_empty() && drops_to(&out, second).is_empty(),
            "nothing is dropped when a member of the group could take it"
        );
    }

    #[test]
    fn a_shared_group_with_every_member_full_drops_the_message_once() {
        // The other end of the same rule, and the one that keeps the
        // skip from becoming a search: when nobody can take it the
        // message is dropped, once, and reported. A retry that wrapped
        // past the group would either deliver it twice or drop it twice.
        let mut broker = brimful();
        let mut out = Vec::new();
        let [first, second] = a_group_of_two(&mut broker, &mut out);
        fill(&mut broker, first, "first", &mut out);
        fill(&mut broker, second, "second", &mut out);

        broker.publish(
            Message::new("a/b", Bytes::from_static(b"x"), QoS::AtLeastOnce, false),
            None,
            0,
            &mut out,
        );

        let dropped: Vec<DropReason> = [first, second]
            .iter()
            .flat_map(|member| drops_to(&out, *member))
            .collect();
        assert_eq!(
            dropped,
            vec![DropReason::QueueFull],
            "one member's queue reports it, and only one"
        );
        assert!(
            publishes_to(&out, first).is_empty() && publishes_to(&out, second).is_empty(),
            "nobody could take it, so nobody received it"
        );
    }

    #[test]
    fn a_shared_member_skipped_while_full_is_picked_again_once_it_drains() {
        // Starvation, which the skip is the obvious way to cause: a
        // member passed over must keep its place in the rotation rather
        // than lose it. `first` is asked first, refuses, and `second`
        // takes the message; once `first` drains it must be picked
        // again, and on its very next turn.
        let mut broker = brimful();
        let mut out = Vec::new();
        let [first, second] = a_group_of_two(&mut broker, &mut out);
        fill(&mut broker, first, "first", &mut out);

        broker.publish(
            Message::new("a/b", Bytes::from_static(b"x"), QoS::AtLeastOnce, false),
            None,
            0,
            &mut out,
        );
        assert_eq!(publishes_to(&out, second).len(), 1, "skipped while full");

        // `first` drains: the in-flight message is acknowledged, the
        // queued one goes out in its place, and that is acknowledged
        // too. Both the window and the queue are empty again.
        out.clear();
        for packet_id in [1, 2] {
            broker
                .handle(
                    first,
                    &Packet::PubAck(mqtt_codec::packet::Ack {
                        packet_id,
                        reason: ReasonCode::Success,
                        properties: Properties::default(),
                    }),
                    &mut out,
                )
                .expect("the acknowledgement is accepted");
        }
        out.clear();

        broker.publish(
            Message::new("a/b", Bytes::from_static(b"y"), QoS::AtLeastOnce, false),
            None,
            0,
            &mut out,
        );
        assert_eq!(
            publishes_to(&out, first).len(),
            1,
            "a member that has drained takes its next turn"
        );
        assert!(
            publishes_to(&out, second).is_empty(),
            "and takes it instead of the member that covered for it"
        );
    }

    #[test]
    fn a_shared_member_that_would_drop_a_fire_and_forget_message_is_skipped() {
        // The other two thirds of what `push` refuses. A queue turns a
        // message away for three reasons and only one of them is "full",
        // so a member asked about room alone answers yes to a publish it
        // is about to discard. Here the first member is offline and the
        // publish is at-most-once, which the queue drops on arrival: the
        // rotation hands it to the offline member, the message dies
        // there, and the online member with an empty queue receives
        // nothing. That is precisely the loss the skip exists to
        // prevent, one layer further down.
        //
        // Durable sessions, so that disconnecting the first leaves its
        // subscription in the group rather than removing it: a member
        // that is gone is not a member that is skipped, and the test
        // would then pass on a broker with no skip at all.
        let mut broker = Broker::new(BrokerConfig::default());
        let mut out = Vec::new();

        let mut members = Vec::new();
        for name in ["first", "second"] {
            let mut connect = connect_v5(name);
            connect.properties.session_expiry_interval = Some(300);
            let member = broker
                .connect("", &connect, &mut out)
                .expect("the default decision authenticates")
                .subscriber;
            broker
                .handle(
                    member,
                    &subscribe_at("$share/g/a/b", QoS::AtMostOnce),
                    &mut out,
                )
                .expect("the shared subscribe is accepted");
            members.push(member);
        }
        let [first, second] = [members[0], members[1]];
        broker.disconnect(first, &mut out);
        out.clear();

        broker.publish(
            Message::new("a/b", Bytes::from_static(b"x"), QoS::AtMostOnce, false),
            None,
            0,
            &mut out,
        );

        // Both halves. A broker that delivers to the online member and
        // *also* drops a copy at the offline one would satisfy the first
        // assertion alone, and so would one that asks every member in
        // turn until something sticks.
        assert_eq!(
            publishes_to(&out, second).len(),
            1,
            "the member that can take it receives it"
        );
        assert!(
            publishes_to(&out, first).is_empty(),
            "and the offline member is not sent one"
        );
        let dropped: Vec<DropReason> = [first, second]
            .iter()
            .flat_map(|member| drops_to(&out, *member))
            .collect();
        assert!(
            dropped.is_empty(),
            "nothing is discarded when a member of the group could take it, got {dropped:?}"
        );
    }

    /// Every publish addressed to one subscriber, in order.
    fn publishes_to(out: &[Addressed], subscriber: SubscriberId) -> Vec<&Publish> {
        out.iter()
            .filter_map(|(target, action)| match action {
                Action::Send(Packet::Publish(p)) if *target == subscriber => Some(p),
                _ => None,
            })
            .collect()
    }
}
