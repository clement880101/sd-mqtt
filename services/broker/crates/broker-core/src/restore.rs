//! Rebuilding a broker from what was stored.
//!
//! Section 7.5 of the design: restore is the first startup phase and
//! completes before any listener binds, so no client can observe a
//! half-restored broker. It rebuilds, in order, sessions with their
//! expiry and will; their subscriptions, **which re-enter the matching
//! trie**; inbound exactly-once identifiers awaiting release; outbound
//! inflight entries with the identifiers they were sent under; and
//! queues, read in sequence order.
//!
//! # It walks the paths a client walks
//!
//! A session is put back by connecting it, subscribing it and
//! disconnecting it, through the same three entry points a real client
//! reaches, rather than by assembling a [`Session`] and pushing it into
//! the registry. That is deliberate, and it is convention 5's last
//! example read the other way round: a rebuild that constructs the type
//! instead of walking the path is a second copy of the broker's rules,
//! and the two drift the first time one of them is corrected. The
//! clearest instance is the trie. A session's filters live on the
//! session *and* in the matching structures, and only the subscribe
//! path puts them in both; a rebuild that set the session's own list
//! would look perfectly correct until somebody published.
//!
//! What restore adds on top is only what a client cannot say: the
//! stored expiry and will, the identifiers already outstanding, the
//! queue, and the sequence number to carry on from.
//!
//! One of those is not simply put back. A session that was connected
//! when the broker stopped has no stored expiry deadline, because a
//! connected session cannot expire and so never had one written, and
//! the restored session is not connected. It is given one, counted
//! from the restore.
//!
//! # Two orderings that are not tidiness
//!
//! **Retained messages go in last, after every session.** Establishing
//! a subscription delivers the retained messages matching it, so a
//! rebuild that had already filled the retained store would hand a
//! session being restored a message the original never had, and the
//! restored broker would differ from the one it was rebuilt from by
//! exactly that message.
//!
//! **A queue is replayed newest first,** because [`Outbound::requeue`]
//! puts each entry at the head.
//!
//! # What it says, and why none of it is written down
//!
//! Every path it walks says what it always says, and all of it is
//! thrown away by [`Broker::discard_journal`]. Restore is reading what
//! was already written, not deciding anything new; writing it back
//! would be a second copy of a state the store already holds, and a
//! `QueuePushed` replaying a number the store has already spent would
//! be refused outright.
//!
//! [`Session`]: crate::session::Session
//! [`Outbound::requeue`]: crate::session::Outbound::requeue

use mqtt_codec::packet::{Connect, Subscribe, SubscriptionOptions};
use mqtt_codec::{Packet, Properties, Version};
use storage::record::{ProtocolVersion, SessionKey};
use storage::{Restored, RestoredSession};

use crate::broker::Broker;
use crate::journal;
use crate::session::state::{deadline, StoredWill};
use crate::subscriptions::SubscriberId;

/// What a restore found, for the log line the shell writes.
///
/// Counts rather than silence, because every number here is a
/// difference between what was stored and what came back, and a restore
/// that quietly dropped half a node's sessions looks exactly like a
/// restore of a node that had half as many.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Restoration {
    /// How many sessions came back.
    pub sessions: usize,
    /// How many subscriptions came back with them.
    pub subscriptions: usize,
    /// Sessions whose expiry passed while the broker was down, which
    /// section 7.5 does not restore.
    ///
    /// Named rather than counted, because the store still holds every
    /// one of them and nothing in this crate can tell it otherwise: the
    /// broker does no I/O. A shell that does not remove them leaves a
    /// session and its queue on disk for the life of the file, and the
    /// next restart skips them again. Handing back the keys is what
    /// lets the shell say [`Record::SessionRemoved`] for each.
    ///
    /// **A removal does not reclaim the bodies.** `Storage::apply` is
    /// explicit that a [`Record::SessionRemoved`] does not release the
    /// bodies of the queue and inflight entries it discards, so the
    /// shell owes one [`Record::BodyReleased`] per entry of every
    /// session named here, in the same batch and before the removal.
    /// [`Restored`] carries those entries, which is where the shell
    /// gets them; this list is only the keys. This is unlike a session
    /// that expires through [`Broker::tick`], where `Outbound::clear`
    /// emits a release for every entry as it discards it.
    ///
    /// [`Record::SessionRemoved`]: storage::record::Record::SessionRemoved
    /// [`Record::BodyReleased`]: storage::record::Record::BodyReleased
    pub expired: Vec<SessionKey>,
    /// Filters the store held that the broker would no longer accept.
    ///
    /// Zero unless the file is from a build with different limits, or
    /// is corrupt. Non-zero is a session that has silently stopped
    /// receiving something it asked for, which is worth a line.
    pub filters_refused: usize,
    /// Identifiers the stored state claimed twice.
    ///
    /// A clash means the stored state disagrees with itself. Nothing is
    /// dropped over it — losing a message is worse than holding a
    /// suspect identifier — but it is a defect somewhere else made
    /// visible here.
    pub identifier_clashes: usize,
}

/// Rebuild `broker` from what the store handed back.
///
/// `now` is the clock the broker starts with, and section 7.5 requires
/// it to be **read before restore rather than after**: it is what
/// decides which sessions outlived the outage, and a clock read
/// afterwards would count the restore itself against every session's
/// expiry. The broker's own clock is moved up to it here, so that the
/// deadlines this puts back and the deadlines it judges are on the same
/// scale.
///
/// The broker must be empty. Restoring into one that already holds
/// sessions is not meaningful: section 7.5 has this running before any
/// listener binds, precisely so that nothing else has happened yet.
pub fn restore(broker: &mut Broker, restored: &Restored, now: u32) -> Restoration {
    debug_assert_eq!(
        broker.session_count(),
        0,
        "restore runs before any listener binds, so nothing else has happened yet"
    );
    broker
        .clock()
        .advance(now.saturating_sub(broker.clock().now()));

    let mut report = Restoration::default();
    let mut actions = Vec::new();

    for held in &restored.sessions {
        // Section 7.5: a session whose expiry has passed while the
        // broker was down is not restored, judged against the clock
        // read before any of this began.
        //
        // Strictly past, not `Session::is_expired`'s `now >= at`, and
        // the difference is one second at the boundary. A broker that
        // never stopped still holds a session in the second its
        // deadline falls: the sweep runs in [`Broker::tick`], which
        // advances the clock before it looks, so the session goes at
        // `at + 1`. Skipping it here at `at` would make a restart
        // destroy a session a second earlier than not restarting,
        // which is the outage counting for more than the uptime it
        // replaced. The differential property in `tests/journal.rs`
        // is what says so: it compares a live broker against one
        // rebuilt from its records, and a session held by one and not
        // the other is a difference whichever way round it is.
        //
        // Nothing is left behind by the extra second. The first tick
        // after restore expires the session by the ordinary path, and
        // that path says `SessionRemoved`.
        if held.session.expires_at.is_some_and(|at| now > at) {
            report.expired.push(held.session.key.clone());
            continue;
        }
        restore_session(broker, held, restored, now, &mut actions, &mut report);
        report.sessions += 1;
    }

    // Last, so that no subscription established above was handed one.
    for (topic, message) in &restored.retained {
        broker
            .retained()
            .set(topic, journal::live_retained(message));
    }

    broker.discard_journal();
    report
}

/// Put one session back.
fn restore_session(
    broker: &mut Broker,
    held: &RestoredSession,
    restored: &Restored,
    now: u32,
    actions: &mut Vec<(SubscriberId, crate::session::Action)>,
    report: &mut Restoration,
) {
    let record = &held.session;
    let connect = Connect {
        version: match record.version {
            ProtocolVersion::V311 => Version::V311,
            ProtocolVersion::V5 => Version::V5,
        },
        client_id: record.key.client_id.clone(),
        // The session is being put back, not started over.
        clean_start: false,
        // Nothing is attached, so nothing is being kept alive. The
        // client's own interval arrives with its connect packet.
        keep_alive: 0,
        username: None,
        password: None,
        // The stored will goes on below, whole. Putting it here would
        // run it through the connect path's split of the delay out of
        // the property block, which storage has already done.
        will: None,
        properties: Properties::default(),
    };
    actions.clear();
    let handle = broker
        .connect(&record.key.mountpoint, &connect, actions)
        .subscriber;
    actions.clear();

    // The trie half. A filter that only reaches the session's own list
    // is a subscription that matches nothing.
    for subscription in &held.subscriptions {
        let packet = Packet::Subscribe(Subscribe {
            packet_id: 1,
            filters: vec![SubscriptionOptions {
                filter: subscription.filter.clone(),
                qos: journal::live_qos(subscription.qos),
                no_local: subscription.no_local,
                retain_as_published: subscription.retain_as_published,
                retain_handling: journal::live_retain_handling(subscription.retain_handling),
            }],
            properties: Properties {
                subscription_identifiers: subscription.subscription_id.into_iter().collect(),
                ..Properties::default()
            },
        });
        let _ = broker.handle(handle, &packet, actions);
        actions.clear();
    }

    // Nobody is attached to a session that has just been read off a
    // disk, and saying otherwise is not a detail: a connected session
    // never expires, its fire-and-forget deliveries go to a socket that
    // does not exist rather than being discarded, and its keep-alive
    // sits in the ring waiting to close a connection nobody made.
    //
    // The departure recomputes the expiry and the will from an
    // interval of zero and a will of none, which is why both are put
    // back afterwards rather than before.
    broker.disconnect(handle, actions);
    actions.clear();

    let Some(session) = broker.session_mut(handle) else {
        debug_assert!(false, "the session was registered a moment ago");
        return;
    };

    report.subscriptions += session.subscriptions().len();
    report.filters_refused += held.subscriptions.len() - session.subscriptions().len();

    session.set_expiry_interval(record.expiry_interval);
    session.set_will(record.will.as_ref().map(|will| StoredWill {
        topic: will.topic.clone(),
        payload: will.payload.clone(),
        qos: journal::live_qos(will.qos),
        retain: will.retain,
        delay: will.delay,
        properties: journal::live_properties(&will.properties),
    }));
    session.set_will_at(record.will_at);
    // A stored deadline is kept exactly, and a session that has none
    // is given one counted from the restore.
    //
    // Only a *disconnected* session has a stored deadline, because
    // only a disconnected session can expire; one that was connected
    // when the broker died never had one written. Restoring that
    // verbatim leaves it offline with no deadline, and
    // [`Session::is_expired`] never expires a session without one, so
    // the crash makes it immortal — it and its queue and its share of
    // every body it references stay in the file forever. A clean-start
    // client with an interval of zero, which asked to outlive nothing,
    // outlives everything.
    //
    // The crash disconnected it, so the deadline counts from a
    // disconnection whose time is not knowable. The restore's is, and
    // it is later, which is the direction [`deadline`] already argues
    // for: every other deadline in this broker errs late, which is
    // harmless, and only erring early is wrong. An interval of zero
    // lands on the restore itself and the session goes on the first
    // tick, which is what a running broker does with it too.
    //
    // A session that already had one keeps it. Time spent dead is time
    // the client was away, and extending those by the length of the
    // outage would be a second bug wearing the first one's clothes. It
    // is also what keeps this composing with section 7.5 above: a
    // stored deadline already past is dropped before reaching here,
    // and a fresh one would have resurrected it.
    session.set_expires_at(Some(
        record
            .expires_at
            .unwrap_or_else(|| deadline(now, record.expiry_interval)),
    ));

    for packet_id in &held.inbound {
        session.inbound.reserve(*packet_id);
    }

    // The window in the order the messages arrived, which is sequence
    // order. The store keys it by the identifier each entry was sent
    // under and so hands it back in a different one.
    let mut inflight: Vec<_> = held.inflight.iter().collect();
    inflight.sort_by_key(|entry| entry.sequence);
    for entry in inflight {
        let Some(body) = restored.bodies.get(&entry.delivery.body) else {
            debug_assert!(
                false,
                "an inflight entry names a body the store does not hold"
            );
            continue;
        };
        let message = journal::live_message(entry.delivery.body, body);
        if !session
            .outbound
            .restore(journal::live_inflight(entry, message))
        {
            report.identifier_clashes += 1;
        }
    }

    // Newest first, because each requeue goes to the head.
    for entry in held.queue.iter().rev() {
        let Some(body) = restored.bodies.get(&entry.delivery.body) else {
            debug_assert!(false, "a queued entry names a body the store does not hold");
            continue;
        };
        let message = journal::live_message(entry.delivery.body, body);
        let delivery =
            journal::live_delivery(&entry.delivery, message, entry.sequence, entry.packet_id);
        if !session.outbound.requeue(delivery) {
            report.identifier_clashes += 1;
        }
    }

    // Section 7.4: the mark is the highest ever assigned, so a session
    // that began again from one would hand out numbers the store has
    // already spent and every batch carrying one would be refused.
    session.outbound.set_highest_sequence(held.highest_sequence);
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use mqtt_codec::packet::{Publish, Will};
    use mqtt_codec::{QoS, ReasonCode};
    use storage::{MemoryStorage, Storage};

    use crate::broker::BrokerConfig;
    use crate::registry::SessionId;

    /// A small window and queue, so a full window and a queue behind it
    /// are reached by four messages rather than by forty.
    fn config() -> BrokerConfig {
        BrokerConfig {
            max_inflight: 2,
            max_queue_messages: 4,
            ..BrokerConfig::default()
        }
    }

    /// A broker and the store its journal goes into, driven together
    /// exactly as the shell drives them.
    struct Node {
        broker: Broker,
        store: MemoryStorage,
        actions: Vec<(SubscriberId, crate::session::Action)>,
    }

    impl Node {
        fn new() -> Self {
            Self {
                broker: Broker::new(config()),
                store: MemoryStorage::new(),
                actions: Vec::new(),
            }
        }

        /// Hand the journal to the store, which is what makes these
        /// tests run against records the broker really produced rather
        /// than against a `Restored` assembled by hand. A `Restored`
        /// built by the test would agree with whatever the test thought
        /// the broker stored.
        fn commit(&mut self) {
            let mut records = Vec::new();
            self.broker.take_journal(&mut records);
            self.store.apply(&records).expect("the store takes it");
            self.actions.clear();
        }

        fn connect_with(&mut self, client: &str, will: Option<Will>, expiry: u32) -> SubscriberId {
            let connect = Connect {
                version: Version::V5,
                client_id: client.to_string(),
                clean_start: false,
                keep_alive: 0,
                username: None,
                password: None,
                will,
                properties: Properties {
                    session_expiry_interval: Some(expiry),
                    ..Properties::default()
                },
            };
            let handle = self
                .broker
                .connect("", &connect, &mut self.actions)
                .subscriber;
            self.commit();
            handle
        }

        fn connect(&mut self, client: &str) -> SubscriberId {
            self.connect_with(client, None, u32::MAX)
        }

        fn subscribe(&mut self, handle: SubscriberId, filter: &str, qos: QoS) {
            let packet = Packet::Subscribe(Subscribe {
                packet_id: 1,
                filters: vec![SubscriptionOptions {
                    filter: filter.to_string(),
                    qos,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: mqtt_codec::packet::RetainHandling::SendAlways,
                }],
                properties: Properties::default(),
            });
            self.broker
                .handle(handle, &packet, &mut self.actions)
                .expect("a valid filter");
            self.commit();
        }

        fn publish(&mut self, handle: SubscriberId, topic: &str, qos: QoS, packet_id: u16) {
            let packet = Packet::Publish(Publish {
                dup: false,
                qos,
                retain: false,
                topic: topic.to_string(),
                packet_id: (qos != QoS::AtMostOnce).then_some(packet_id),
                payload: Bytes::from_static(b"payload"),
                properties: Properties::default(),
            });
            self.broker
                .handle(handle, &packet, &mut self.actions)
                .expect("a valid publish");
            self.commit();
        }

        fn vanish(&mut self, handle: SubscriberId) {
            self.broker.disconnect(handle, &mut self.actions);
            self.commit();
        }

        fn tick(&mut self, seconds: u32) {
            for _ in 0..seconds {
                self.broker.tick(&mut self.actions);
                self.commit();
            }
        }

        /// Restore a second broker from everything the first said.
        fn rebuild(&self) -> (Broker, Restoration) {
            let held = self.store.restore().expect("the store restores");
            let mut broker = Broker::new(config());
            let report = restore(&mut broker, &held, self.broker.clock().now());
            (broker, report)
        }
    }

    /// Everything a session still holds, waiting or outstanding.
    fn held_by(broker: &Broker, handle: SubscriberId) -> usize {
        let session = broker.session(handle).expect("the session exists");
        session.outbound.queued_len() + session.outbound.inflight_len()
    }

    fn handle_of(broker: &Broker, client: &str) -> Option<SubscriberId> {
        broker.subscriber_of(&SessionId::new("", client))
    }

    /// Connect a client to a rebuilt broker the way the shell would,
    /// and report whether it was told its session was present.
    fn reconnect(broker: &mut Broker, client: &str) -> bool {
        let connect = Connect {
            version: Version::V5,
            client_id: client.to_string(),
            clean_start: false,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties {
                session_expiry_interval: Some(u32::MAX),
                ..Properties::default()
            },
        };
        let mut actions = Vec::new();
        broker.connect("", &connect, &mut actions).session_present
    }

    #[test]
    fn a_restored_session_is_resumed_rather_than_created() {
        let mut node = Node::new();
        let sub = node.connect("sub");
        node.subscribe(sub, "a/+", QoS::AtLeastOnce);
        node.vanish(sub);

        let (mut rebuilt, report) = node.rebuild();
        assert_eq!(report.sessions, 1);
        assert_eq!(report.subscriptions, 1);
        assert!(report.expired.is_empty());

        assert!(
            reconnect(&mut rebuilt, "sub"),
            "[MQTT-3.2.2-2]: a session the broker still holds is present"
        );
    }

    #[test]
    fn a_restored_session_has_nobody_attached() {
        // A session read off a disk has no connection, and the
        // difference is not cosmetic: `Session::is_expired` refuses to
        // expire a connected session, deliberately, so a restore that
        // marked one connected would keep every abandoned session alive
        // until somebody happened to connect as it.
        let mut node = Node::new();
        let sub = node.connect("sub");
        node.vanish(sub);

        let (rebuilt, _) = node.rebuild();
        let handle = handle_of(&rebuilt, "sub").expect("it came back");
        assert!(
            !rebuilt
                .session(handle)
                .expect("it came back")
                .is_connected(),
            "nothing is attached to a session that has just been read back"
        );
    }

    #[test]
    fn a_restored_subscription_receives_a_publication() {
        // **The trie half**, and the one thing the journal's
        // differential test cannot see: `Broker::durable_state` reads a
        // session's filters from the session, which is also where the
        // records come from, so a rebuild that put nothing into the
        // matching structures compares perfectly equal and then matches
        // nothing.
        let mut node = Node::new();
        let sub = node.connect("sub");
        node.subscribe(sub, "a/+", QoS::AtLeastOnce);
        node.vanish(sub);

        let (mut rebuilt, _) = node.rebuild();
        let restored_sub = handle_of(&rebuilt, "sub").expect("it came back");

        // Somebody else publishes to a topic the restored filter
        // matches. The subscriber is offline, so it queues.
        let mut actions = Vec::new();
        let publisher = rebuilt
            .connect(
                "",
                &Connect {
                    version: Version::V5,
                    client_id: "pub".to_string(),
                    clean_start: true,
                    keep_alive: 0,
                    username: None,
                    password: None,
                    will: None,
                    properties: Properties::default(),
                },
                &mut actions,
            )
            .subscriber;
        actions.clear();
        rebuilt
            .handle(
                publisher,
                &Packet::Publish(Publish {
                    dup: false,
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    topic: "a/b".to_string(),
                    packet_id: Some(1),
                    payload: Bytes::from_static(b"after the restart"),
                    properties: Properties::default(),
                }),
                &mut actions,
            )
            .expect("a valid publish");

        assert_eq!(
            rebuilt
                .session(restored_sub)
                .expect("it came back")
                .outbound
                .queued_len(),
            1,
            "a restored filter that is not in the trie matches nothing"
        );
    }

    #[test]
    fn a_restored_shared_subscription_receives_a_publication() {
        // The other matching structure. A shared filter goes into the
        // shared table rather than the trie, and a rebuild that put
        // back only one of the two would pass the test above.
        let mut node = Node::new();
        let sub = node.connect("sub");
        node.subscribe(sub, "$share/g/a/+", QoS::AtLeastOnce);
        node.vanish(sub);

        let (mut rebuilt, _) = node.rebuild();
        let restored_sub = handle_of(&rebuilt, "sub").expect("it came back");

        let mut actions = Vec::new();
        let publisher = rebuilt
            .connect(
                "",
                &Connect {
                    version: Version::V5,
                    client_id: "pub".to_string(),
                    clean_start: true,
                    keep_alive: 0,
                    username: None,
                    password: None,
                    will: None,
                    properties: Properties::default(),
                },
                &mut actions,
            )
            .subscriber;
        actions.clear();
        rebuilt
            .handle(
                publisher,
                &Packet::Publish(Publish {
                    dup: false,
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    topic: "a/b".to_string(),
                    packet_id: Some(1),
                    payload: Bytes::from_static(b"to the group"),
                    properties: Properties::default(),
                }),
                &mut actions,
            )
            .expect("a valid publish");

        assert_eq!(
            rebuilt
                .session(restored_sub)
                .expect("it came back")
                .outbound
                .queued_len(),
            1,
            "the only member of the group must receive it"
        );
    }

    #[test]
    fn a_session_whose_expiry_passed_is_not_restored() {
        // Section 7.5. The clock is read before restore, so the outage
        // counts against the session exactly as the time it was up
        // would have.
        let mut node = Node::new();
        let short = node.connect_with("short", None, 5);
        node.subscribe(short, "a/+", QoS::AtLeastOnce);
        let long = node.connect_with("long", None, 600);
        node.vanish(short);
        node.vanish(long);

        let held = node.store.restore().expect("the store restores");
        assert_eq!(held.sessions.len(), 2, "the store holds both");

        // Both deadlines are relative to the departure, and the broker
        // was down for a minute.
        let mut rebuilt = Broker::new(config());
        let report = restore(&mut rebuilt, &held, node.broker.clock().now() + 60);

        assert_eq!(report.sessions, 1);
        assert_eq!(
            report.expired,
            vec![SessionKey::new("", "short")],
            "the one whose deadline passed is named, because the store still holds it"
        );
        assert!(handle_of(&rebuilt, "short").is_none());
        assert!(handle_of(&rebuilt, "long").is_some());
        assert_eq!(
            rebuilt.subscription_count(),
            0,
            "and it took its subscription with it"
        );
    }

    #[test]
    fn the_second_a_deadline_falls_in_is_the_second_it_is_still_owed() {
        // The boundary, which the test above cannot see: an off-by-one
        // here is invisible unless something lands either side of the
        // line, and the line is not where it first looks. A broker that
        // never stopped still holds a session in the second its
        // deadline falls, because the sweep advances the clock before
        // it looks, so restore must too or a restart destroys a session
        // a second earlier than no restart would have.
        let mut node = Node::new();
        let handle = node.connect_with("edge", None, 5);
        node.vanish(handle);

        let held = node.store.restore().expect("the store restores");
        let deadline = held.sessions[0]
            .session
            .expires_at
            .expect("a disconnected session has one");

        for (now, expected, why) in [
            (deadline - 1, 1, "a second before, plainly owed"),
            (
                deadline,
                1,
                "in the second it falls, as a running broker would",
            ),
            (deadline + 1, 0, "and past it, gone"),
        ] {
            let mut rebuilt = Broker::new(config());
            assert_eq!(
                restore(&mut rebuilt, &held, now).sessions,
                expected,
                "{why}"
            );
        }
    }

    #[test]
    fn a_session_restored_in_the_second_its_deadline_falls_goes_on_the_next_tick() {
        // The other half of the boundary above: the extra second is a
        // second, not a reprieve. Nothing is stranded in the store
        // either, because the session leaves by the ordinary expiry
        // path and that path says so.
        let mut node = Node::new();
        let handle = node.connect_with("edge", None, 5);
        node.vanish(handle);

        let held = node.store.restore().expect("the store restores");
        let deadline = held.sessions[0].session.expires_at.expect("it has one");
        let mut rebuilt = Broker::new(config());
        restore(&mut rebuilt, &held, deadline);
        assert_eq!(rebuilt.session_count(), 1);

        let mut actions = Vec::new();
        rebuilt.tick(&mut actions);
        assert_eq!(rebuilt.session_count(), 0, "one tick, and it is gone");
        let mut records = Vec::new();
        rebuilt.take_journal(&mut records);
        assert!(
            records.iter().any(|record| matches!(
                record,
                storage::record::Record::SessionRemoved { session }
                    if session == &SessionKey::new("", "edge")
            )),
            "and the store is told, got {records:?}"
        );
    }

    #[test]
    fn a_restored_session_expires_on_the_first_tick_past_its_deadline() {
        // The consequence of putting the deadline back rather than
        // recomputing it: a restored session carries on counting down
        // from where it was, instead of being granted its whole
        // interval again by the restart.
        let mut node = Node::new();
        let handle = node.connect_with("short", None, 5);
        node.vanish(handle);

        let (mut rebuilt, report) = node.rebuild();
        assert_eq!(report.sessions, 1);
        assert_eq!(rebuilt.session_count(), 1);

        let mut actions = Vec::new();
        for _ in 0..7 {
            rebuilt.tick(&mut actions);
        }
        assert_eq!(
            rebuilt.session_count(),
            0,
            "the deadline it left with is the deadline it keeps"
        );
    }

    #[test]
    fn a_session_connected_when_the_broker_died_is_given_a_deadline() {
        // A connected session has no stored deadline, because a
        // connected session cannot expire and so never had one set.
        // Restored verbatim it comes back offline with none, and
        // `Session::is_expired` refuses to expire a session that has
        // no deadline, so the crash makes it immortal: it holds its
        // queue and its share of every body it references for the
        // life of the file. The crash disconnected it, so it needs
        // one.
        let mut node = Node::new();
        let handle = node.connect_with("crashed", None, 5);
        node.subscribe(handle, "a/+", QoS::AtLeastOnce);
        // No `vanish`. The broker died with this session attached.
        let now = node.broker.clock().now();

        let held = node.store.restore().expect("the store restores");
        assert_eq!(
            held.sessions[0].session.expires_at, None,
            "nothing wrote a deadline for a session that was connected, \
             which is what leaves restore with nothing to put back"
        );

        let (mut rebuilt, report) = node.rebuild();
        assert_eq!(report.sessions, 1);
        let restored = handle_of(&rebuilt, "crashed").expect("it came back");
        assert_eq!(
            rebuilt
                .session(restored)
                .expect("it came back")
                .expires_at(),
            // Not merely "some deadline": the value, restated from the
            // rule. Five seconds from the restore, and the extra
            // second because the clock is whole seconds and a deadline
            // that errs late is harmless where one that errs early is
            // wrong.
            Some(now + 5 + 1),
            "a restored session is not connected, so its interval runs \
             from the restore"
        );

        let mut actions = Vec::new();
        for _ in 0..7 {
            rebuilt.tick(&mut actions);
        }
        assert_eq!(
            rebuilt.session_count(),
            0,
            "its interval counts from the restore, and the restore is when \
             it became a session nobody is attached to"
        );
        assert_eq!(
            rebuilt.subscription_count(),
            0,
            "and it took its subscription with it"
        );
    }

    #[test]
    fn a_session_that_asked_to_outlive_nothing_goes_on_the_first_tick() {
        // The interval the defect is worst for. Zero means the session
        // ends with its connection, so a restore that leaves it
        // deadlineless turns the one session that should never have
        // outlived its socket into the one that outlives the file.
        //
        // A deadline of the restore itself is not a reprieve: it is
        // the same boundary a running broker keeps, where the sweep
        // advances the clock before it looks.
        let mut node = Node::new();
        let handle = node.connect_with("clean", None, 0);
        node.subscribe(handle, "a/+", QoS::AtLeastOnce);
        let now = node.broker.clock().now();

        let (mut rebuilt, report) = node.rebuild();
        assert_eq!(
            report.sessions, 1,
            "its deadline is the restore, and the restore has not passed"
        );
        let restored = handle_of(&rebuilt, "clean").expect("it came back");
        assert_eq!(
            rebuilt
                .session(restored)
                .expect("it came back")
                .expires_at(),
            Some(now),
            "at once, which is not rounded up"
        );

        let mut actions = Vec::new();
        rebuilt.tick(&mut actions);
        assert_eq!(rebuilt.session_count(), 0, "one tick, and it is gone");
        assert_eq!(rebuilt.subscription_count(), 0);
    }

    #[test]
    fn a_stored_deadline_survives_restore_unchanged() {
        // The other half of the rule above, and the bug it must not
        // become: a session that was already disconnected when the
        // broker died has a deadline, and time spent dead is time the
        // client was away. Granting it a fresh one would extend every
        // abandoned session by the length of the outage.
        let mut node = Node::new();
        let handle = node.connect_with("away", None, 600);
        node.vanish(handle);
        let stored = node.store.restore().expect("the store restores").sessions[0]
            .session
            .expires_at
            .expect("a disconnected session has one");

        // The outage, on the clock restore is judged against.
        node.tick(5);

        let (rebuilt, _) = node.rebuild();
        let restored = handle_of(&rebuilt, "away").expect("it came back");
        assert_eq!(
            rebuilt
                .session(restored)
                .expect("it came back")
                .expires_at(),
            Some(stored),
            "the deadline it left with is the deadline it keeps"
        );
    }

    #[test]
    fn a_stored_deadline_already_past_is_still_not_restored() {
        // The two rules composing. Section 7.5 drops a session whose
        // expiry passed while the broker was down, and the rule above
        // hands a deadline to one that has none; a fix that gave every
        // session a fresh deadline would resurrect the dropped ones.
        let mut node = Node::new();
        let handle = node.connect_with("short", None, 5);
        node.vanish(handle);

        let held = node.store.restore().expect("the store restores");
        let mut rebuilt = Broker::new(config());
        let report = restore(&mut rebuilt, &held, node.broker.clock().now() + 60);

        assert_eq!(report.sessions, 0);
        assert_eq!(report.expired, vec![SessionKey::new("", "short")]);
        assert_eq!(rebuilt.session_count(), 0);
    }

    #[test]
    fn a_restored_inflight_entry_keeps_the_identifier_it_was_sent_under() {
        // Section 7.5, and section 6 behind it: the peer is waiting on
        // that identifier. A restored window under fresh identifiers
        // leaves the peer holding the old ones forever, and the
        // redelivery looks to it like an unrelated message.
        let mut node = Node::new();
        let sub = node.connect("sub");
        node.subscribe(sub, "a/+", QoS::ExactlyOnce);
        let publisher = node.connect("pub");
        node.publish(publisher, "a/b", QoS::ExactlyOnce, 1);

        let sent: Vec<u16> = node
            .broker
            .session(sub)
            .expect("it exists")
            .outbound
            .outstanding()
            .map(|entry| entry.packet_id)
            .collect();
        assert_eq!(sent.len(), 1, "one is outstanding to the subscriber");
        node.vanish(sub);

        let (rebuilt, report) = node.rebuild();
        assert_eq!(report.identifier_clashes, 0);
        let restored_sub = handle_of(&rebuilt, "sub").expect("it came back");
        let back: Vec<u16> = rebuilt
            .session(restored_sub)
            .expect("it came back")
            .outbound
            .outstanding()
            .map(|entry| entry.packet_id)
            .collect();
        assert_eq!(back, sent, "the identifier the peer is waiting on");
    }

    #[test]
    fn a_restored_window_resumes_in_the_order_the_messages_arrived() {
        // The store keys the window by identifier, so it hands it back
        // in identifier order rather than arrival order. Section 7.5
        // reads it in sequence order for exactly this reason, and
        // [MQTT-4.6.0-6] is what makes it matter: a resume resends in
        // the order the entries went out.
        let mut node = Node::new();
        let sub = node.connect("sub");
        node.subscribe(sub, "a/+", QoS::AtLeastOnce);
        let publisher = node.connect("pub");
        node.publish(publisher, "a/b", QoS::AtLeastOnce, 1);
        node.publish(publisher, "a/b", QoS::AtLeastOnce, 2);
        node.vanish(sub);

        let live: Vec<u64> = node
            .broker
            .session(sub)
            .expect("it exists")
            .outbound
            .outstanding()
            .map(|entry| entry.delivery.sequence)
            .collect();
        assert_eq!(live, vec![1, 2], "two went out, in arrival order");

        let (rebuilt, _) = node.rebuild();
        let restored_sub = handle_of(&rebuilt, "sub").expect("it came back");
        let back: Vec<u64> = rebuilt
            .session(restored_sub)
            .expect("it came back")
            .outbound
            .outstanding()
            .map(|entry| entry.delivery.sequence)
            .collect();
        assert_eq!(back, live);
    }

    #[test]
    fn a_restored_queue_is_in_the_order_it_will_be_delivered() {
        let mut node = Node::new();
        let sub = node.connect("sub");
        node.subscribe(sub, "a/+", QoS::AtLeastOnce);
        let publisher = node.connect("pub");
        // Two fill the window, two wait behind it.
        for packet_id in 1..=4 {
            node.publish(publisher, "a/b", QoS::AtLeastOnce, packet_id);
        }
        node.vanish(sub);

        let live: Vec<u64> = node
            .broker
            .session(sub)
            .expect("it exists")
            .outbound
            .waiting()
            .map(|item| item.sequence)
            .collect();
        assert_eq!(live, vec![3, 4]);

        let (rebuilt, _) = node.rebuild();
        let restored_sub = handle_of(&rebuilt, "sub").expect("it came back");
        let back: Vec<u64> = rebuilt
            .session(restored_sub)
            .expect("it came back")
            .outbound
            .waiting()
            .map(|item| item.sequence)
            .collect();
        assert_eq!(back, live, "newest first is how a requeue replays");
    }

    #[test]
    fn a_restored_inbound_identifier_is_still_owned_by_the_peer() {
        // The seam section 7.5 names first. An exactly-once identifier
        // the peer has published under and not released is the broker's
        // guard against delivering that message twice, and a restart
        // that forgot it delivers the redelivery as a new message.
        let mut node = Node::new();
        let publisher = node.connect("pub");
        node.publish(publisher, "a/b", QoS::ExactlyOnce, 9);
        node.vanish(publisher);

        let (rebuilt, _) = node.rebuild();
        let handle = handle_of(&rebuilt, "pub").expect("it came back");
        let held: Vec<u16> = rebuilt
            .session(handle)
            .expect("it came back")
            .inbound
            .held()
            .collect();
        assert_eq!(held, vec![9]);
    }

    #[test]
    fn a_restored_will_is_still_owed_and_still_falls_due() {
        // A will armed by an abrupt departure outlives the broker that
        // armed it: the client is still gone, and section 3.1.3.2 has
        // the delay running against the session rather than against any
        // process.
        let mut node = Node::new();
        let watcher = node.connect("watcher");
        node.subscribe(watcher, "gone/+", QoS::AtLeastOnce);
        let leaver = node.connect_with(
            "leaver",
            Some(Will {
                topic: "gone/leaver".to_string(),
                payload: Bytes::from_static(b"bye"),
                qos: QoS::AtLeastOnce,
                retain: false,
                properties: Properties {
                    will_delay_interval: Some(10),
                    ..Properties::default()
                },
            }),
            600,
        );
        node.vanish(leaver);
        node.vanish(watcher);

        let (mut rebuilt, _) = node.rebuild();
        let restored_watcher = handle_of(&rebuilt, "watcher").expect("it came back");
        let restored_leaver = handle_of(&rebuilt, "leaver").expect("it came back");
        assert!(
            rebuilt
                .session(restored_leaver)
                .expect("it came back")
                .will_at()
                .is_some(),
            "the will is still owed"
        );

        let mut actions = Vec::new();
        for _ in 0..12 {
            rebuilt.tick(&mut actions);
        }
        assert_eq!(
            rebuilt
                .session(restored_watcher)
                .expect("it came back")
                .outbound
                .queued_len(),
            1,
            "and it is published to the subscriber that was watching for it"
        );
    }

    #[test]
    fn a_restored_broker_holds_the_retained_messages() {
        let mut node = Node::new();
        let publisher = node.connect("pub");
        let packet = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: true,
            topic: "a/b".to_string(),
            packet_id: Some(1),
            payload: Bytes::from_static(b"kept"),
            properties: Properties::default(),
        });
        node.broker
            .handle(publisher, &packet, &mut node.actions)
            .expect("a valid publish");
        node.commit();

        let (rebuilt, _) = node.rebuild();
        assert_eq!(rebuilt.retained_count(), 1);
        assert_eq!(
            rebuilt.durable_state().retained,
            node.broker.durable_state().retained
        );
    }

    #[test]
    fn a_retained_message_is_not_handed_to_a_session_being_restored() {
        // The ordering rule, proved rather than asserted in a comment.
        // Establishing a subscription delivers what it matches, so a
        // rebuild that filled the retained store before it rebuilt the
        // sessions would put a message in a restored queue that the
        // original never held.
        let mut node = Node::new();
        let publisher = node.connect("pub");
        node.broker
            .handle(
                publisher,
                &Packet::Publish(Publish {
                    dup: false,
                    qos: QoS::AtLeastOnce,
                    retain: true,
                    topic: "a/b".to_string(),
                    packet_id: Some(1),
                    payload: Bytes::from_static(b"kept"),
                    properties: Properties::default(),
                }),
                &mut node.actions,
            )
            .expect("a valid publish");
        node.commit();

        // The subscriber took the retained message when it subscribed
        // and acknowledged it, so its queue and window are empty.
        let sub = node.connect("sub");
        node.subscribe(sub, "a/+", QoS::AtLeastOnce);
        let outstanding: Vec<u16> = node
            .broker
            .session(sub)
            .expect("it exists")
            .outbound
            .outstanding()
            .map(|entry| entry.packet_id)
            .collect();
        for packet_id in outstanding {
            node.broker
                .handle(
                    sub,
                    &Packet::PubAck(mqtt_codec::packet::Ack {
                        packet_id,
                        reason: ReasonCode::Success,
                        properties: Properties::default(),
                    }),
                    &mut node.actions,
                )
                .expect("it is outstanding");
        }
        node.commit();
        node.vanish(sub);
        assert_eq!(held_by(&node.broker, sub), 0, "it took it and settled it");

        let (rebuilt, _) = node.rebuild();
        let restored_sub = handle_of(&rebuilt, "sub").expect("it came back");
        // Queued **and** outstanding. A session being restored is
        // connected while its filters go back in, so a retained
        // message handed to it at that moment goes straight into the
        // window rather than into the queue: a test counting only the
        // queue watches the half this cannot reach, and let the
        // ordering above be reversed without complaint until the
        // differential property caught it.
        assert_eq!(
            held_by(&rebuilt, restored_sub),
            0,
            "a restored session must not be handed a message it already took"
        );
        assert_eq!(rebuilt.retained_count(), 1, "but the message is still kept");
    }

    #[test]
    fn a_restore_says_nothing_that_needs_writing_down() {
        // Restore reads what was already written. Anything left in a
        // journal afterwards would be written a second time, and a
        // `QueuePushed` replaying a number the store has already spent
        // would be refused outright, which the shell counts as a batch
        // the broker and the store disagree about.
        let mut node = Node::new();
        let sub = node.connect("sub");
        node.subscribe(sub, "a/+", QoS::AtLeastOnce);
        let publisher = node.connect("pub");
        for packet_id in 1..=4 {
            node.publish(publisher, "a/b", QoS::AtLeastOnce, packet_id);
        }
        node.publish(publisher, "a/b", QoS::ExactlyOnce, 5);
        node.vanish(sub);

        let (mut rebuilt, _) = node.rebuild();
        let mut records = Vec::new();
        rebuilt.take_journal(&mut records);
        assert!(
            records.is_empty(),
            "the broker's own buffer, got {records:?}"
        );
        assert_eq!(
            rebuilt.sessions_holding_records(),
            0,
            "and every session's, which clearing the broker's does not reach"
        );
    }

    #[test]
    fn what_was_stored_is_what_comes_back() {
        // The whole of it, in one comparison. `tests/journal.rs` drives
        // this over generated histories; here it is over a hand-built
        // one, so that a failure names a shape rather than a seed.
        let mut node = Node::new();
        let sub = node.connect_with(
            "sub",
            Some(Will {
                topic: "gone/sub".to_string(),
                payload: Bytes::from_static(b"bye"),
                qos: QoS::AtLeastOnce,
                retain: false,
                properties: Properties {
                    will_delay_interval: Some(30),
                    ..Properties::default()
                },
            }),
            600,
        );
        node.subscribe(sub, "a/+", QoS::ExactlyOnce);
        node.subscribe(sub, "$share/g/x/#", QoS::AtLeastOnce);
        let publisher = node.connect("pub");
        for packet_id in 1..=4 {
            node.publish(publisher, "a/b", QoS::AtLeastOnce, packet_id);
        }
        node.publish(publisher, "a/c", QoS::ExactlyOnce, 5);
        node.vanish(sub);
        node.tick(3);

        let (rebuilt, _) = node.rebuild();
        let mut live = node.broker.durable_state();
        // One field is expected to differ, and stating it is the point.
        // The publisher is still connected, and a connected session has
        // no deadline because it cannot expire; the rebuilt one is not,
        // because nothing is attached to a session read off a disk, so
        // restore gives it one counted from the restore. Its interval is
        // `u32::MAX`, which saturates there whatever the clock reads.
        // `tests/journal.rs` states the same rule over generated
        // histories.
        live.sessions
            .get_mut(&SessionKey::new("", "pub"))
            .expect("the publisher is held")
            .session
            .expires_at = Some(u32::MAX);
        assert_eq!(rebuilt.durable_state(), live);
    }
}
