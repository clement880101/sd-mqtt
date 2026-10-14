//! Publishing from something that is not a connection.
//!
//! # There is one publish, and this is it
//!
//! Retention, expiry, matching, shared-subscription selection, the
//! per-session queue and its bounds are all rules, and a second set of
//! them anywhere would drift from the first the day either was
//! corrected. So everything that publishes without a session behind it
//! — the admin API's `POST /api/v1/publish`, section 9.2's ingest
//! endpoint, and whatever comes next — builds the same [`Message`] a
//! connection's publish builds and hands it to the same
//! [`Broker::originate`], under the same [`Shared::with_broker_committed`],
//! and delivers what comes back with the same [`Shared::deliver`].
//!
//! This module was extracted from the admin API's handler when the
//! second caller arrived, which is the moment a copy would have been
//! made instead.
//!
//! # And it fires the message hooks, which it did not
//!
//! It went to [`Broker::publish`], the internal route that fires no
//! message hook. So a message arriving over a bridge,
//! through section 9.2's ingest endpoint or from a poller reached every
//! subscriber and triggered **no rule** — while section 9.2 says a
//! bridge publishes inbound messages into the local broker "where any
//! rule can act on them". It now goes to [`Broker::originate`], which
//! fires them. The difference is whether connectors compose at all: a
//! gateway posting to ingest whose message should then fan out to a
//! webhook is the obvious thing somebody will try, and until this it
//! did nothing.
//!
//! [`Broker::originate`] is also where section 9.2's hop limit is
//! applied, and that is not an aside. Firing the hooks is exactly what
//! makes a cycle of connectors possible, so the limit belongs at the
//! same door — see [`PublishRefused::TooManyHops`] for what a caller
//! does with one.
//!
//! # The answer is the acknowledgement
//!
//! Section 7.3: an acknowledgement of an at-least-once or exactly-once
//! publication tells a publisher the broker has taken responsibility,
//! so it must not be sent before the message is durable. A caller here
//! has no session and therefore no PUBACK to send; the promise it makes
//! is whatever it answers its own client with, so [`publish`] does not
//! return until the batch is durable when the level asks for it.
//!
//! [`promises_durability`] is not what decides that. That function
//! reads a batch's actions for a PUBACK, PUBREC or PUBCOMP, and there
//! is no such packet here because there is no publishing session to
//! send one to. So the level is asked directly, and at-most-once, which
//! promises nobody anything, waits for nothing.
//!
//! [`Broker::publish`]: broker_core::broker::Broker::publish
//! [`Broker::originate`]: broker_core::broker::Broker::originate
//! [`Message`]: broker_core::session::message::Message
//! [`promises_durability`]: crate::shared::promises_durability

use std::sync::Arc;

use broker_core::hooks::{ClientInfo, Refusal};
use broker_core::session::message::Message;
use bytes::Bytes;
use mqtt_codec::{Properties, QoS};

use crate::shared::Shared;

/// One publication, as something that is not a connection asks for it.
#[derive(Debug, Clone)]
pub struct Publication {
    /// Where it goes. A topic name, so no wildcards.
    pub topic: String,
    /// What it carries.
    pub payload: Bytes,
    /// The level to publish at.
    pub qos: QoS,
    /// Whether to retain it.
    pub retain: bool,
    /// How long it stays deliverable, in seconds.
    pub message_expiry_interval: Option<u32>,
    /// The v5 user properties it carries.
    ///
    /// Section 9.2's hop count travels in one of these, so a connector
    /// that originates a publication has something to put it in; see
    /// `rules::hops`. A caller with nothing to say passes none, and the
    /// message then carries the empty property block it carried before
    /// this field existed.
    ///
    /// **A 3.1.1 subscriber will not see them**, as it sees no other v5
    /// property. That is the codec's rule and not this one's.
    pub user_properties: Vec<(String, String)>,
    /// What made this publication, for the hooks it fires.
    ///
    /// There is no session here, so there is no client identifier to
    /// give a rule, and a rule's trigger can be narrowed by one. A
    /// connector names itself — its rule identifier — so that a rule
    /// watching a topic can say whether it means the messages a bridge
    /// brought in or the ones clients published, and so that a log line
    /// about a publication says what made it.
    ///
    /// It is a name and not a session: nothing is subscribed under it,
    /// nothing can be delivered to it, and two connectors that chose
    /// the same name would be indistinguishable to a hook and to
    /// nothing else.
    pub origin: String,
}

/// What a publication that was made comes back with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Published {
    /// How many subscribers on this node it reached, **plus how many
    /// other nodes it was forwarded to**.
    ///
    /// Zero is an ordinary answer: a retained publish to a topic nobody
    /// holds reaches nobody and is still kept.
    ///
    /// The two halves are added for the reason
    /// [`Broker::publish`] gives: a node forwarded to has a matching
    /// subscriber, so a caller reading this as "did anybody want it"
    /// gets the same answer wherever it published, which is what
    /// section 8's clustering is for. A caller wanting the two apart
    /// reads section 11.3's `router_matches`, where they are two series.
    pub delivered: usize,
    /// Whether this answer waited for the store.
    ///
    /// Reported rather than implied, because the promise a caller is
    /// entitled to read into an acknowledgement differs between the
    /// levels and an interface should not have to know the rule to say
    /// so.
    pub durable: bool,
}

/// Why a publication was not made.
#[derive(Debug, thiserror::Error)]
pub enum PublishRefused {
    /// The topic is not one a message can be published to.
    #[error("that is not a topic a message can be published to: {0}")]
    NotATopic(String),
    /// The topic is in the namespace the broker reserves for itself.
    ///
    /// Section 12.3: dollar-prefixed topics are not publishable.
    ///
    /// **Here, and not yet on a connection.** The rule is written about
    /// clients and the MQTT publish path does not enforce it; that is
    /// older than this module and is deferred to milestone 7, where the
    /// authorization work it belongs with lives. What is not deferred
    /// is this, because every caller of this module is a new way to
    /// write one — and a retained one, which stays until somebody finds
    /// it. A reserved namespace that can be filled from a form or a
    /// webhook is worse than one a client can fill, because those are
    /// the things that look sanctioned.
    ///
    /// Only the first level. `a/$b` is an ordinary topic and the
    /// matching layer already treats it as one.
    #[error(
        "the dollar-prefixed namespace is reserved for the broker and is not publishable, \
         so this endpoint will not write one. Publish to a topic outside it."
    )]
    Reserved,
    /// The batch could not be made durable, so nothing is acknowledged.
    #[error("the message could not be written to the store, so it is not acknowledged")]
    NotDurable,
    /// Durable here, confirmed by no copy, and the operator asked for
    /// that to be a refusal. See [`Refusal::NotConfirmed`].
    #[error(
        "the message was written here but no other node confirmed a copy of it within \
         cluster.sync_timeout_ms, and cluster.strict.unconfirmed refuses such a publication"
    )]
    NotConfirmed,
    /// It had been through more connectors than `broker.max_hops`
    /// allows.
    ///
    /// Section 9.2's third loop: "an outbound rule feeding a system
    /// that posts back through an ingest rule is a cycle with no bridge
    /// in it. Every publication a connector originates carries a hop
    /// count, and one over the configured maximum is dropped and
    /// counted."
    ///
    /// **Answered rather than swallowed.** A caller that had been told
    /// nothing would go on feeding the cycle at full speed and would
    /// have no way to learn why nothing arrived; an ingest endpoint
    /// that answers its caller with this is the only place the cycle is
    /// visible from outside.
    #[error(transparent)]
    TooManyHops(#[from] broker_core::broker::Looped),
}

impl PublishRefused {
    /// What this refusal is, in the vocabulary the metric and the hook
    /// share.
    ///
    /// **A match with no wildcard, and that is the whole mechanism.**
    /// Section 11.3 says every discard path has a metric and a hook,
    /// and milestone three made that true of the queue by having a new
    /// [`DropReason`] fail to compile until it had a counter. This is
    /// the same trick one layer out: a variant added to
    /// [`PublishRefused`] does not compile until it is given a
    /// [`Refusal`], and a variant added to [`Refusal`] does not
    /// compile until `crate::metrics` gives it a counter.
    ///
    /// And because [`publish`] is the one door every publication
    /// without a session goes through — section 9.2's ingest endpoint,
    /// a poller, the inbound half of a bridge, the admin API's publish
    /// — a connector written after this one is counted by calling the
    /// function it already had to call.
    ///
    /// [`DropReason`]: broker_core::session::DropReason
    pub fn refusal(&self) -> Refusal {
        match self {
            Self::NotATopic(_) => Refusal::NotATopic,
            Self::Reserved => Refusal::Reserved,
            Self::NotDurable => Refusal::NotDurable,
            Self::NotConfirmed => Refusal::NotConfirmed,
            Self::TooManyHops(_) => Refusal::TooManyHops,
        }
    }
}

/// Count a publication that went nowhere, and tell the hooks.
///
/// Section 11.3's two halves in one call, so that a caller cannot do
/// one and forget the other. [`publish`] calls it for every refusal of
/// its own; a connector that refuses a message *before* it reaches
/// that door — a bridge's loop marker is the one milestone four has —
/// calls it with the [`Refusal`] its own vocabulary maps to.
///
/// `origin` is the connector's rule identifier, never a session: there
/// is no session here, which is what this whole module is about, and
/// [`ClientInfo::subscriber`] is `None` for the reason it documents.
///
/// **The hook runs under the broker's lock**, as every hook does, and
/// the obligations in `broker_core::hooks` apply unchanged. Taking the
/// lock for a message that has already been lost is not a cost worth
/// designing around; taking it on the packet path would be.
pub fn refuse(shared: &Shared, origin: &str, topic: &str, qos: QoS, reason: Refusal) {
    shared.metrics().publication_refused(reason).inc();
    shared.with_broker(|broker| {
        broker.hooks().message_refused(
            &ClientInfo {
                subscriber: None,
                mountpoint: "",
                client_id: origin,
                username: None,
            },
            topic,
            qos,
            reason,
        );
    });
}

/// Publish `publication` through the path a connection publishes by.
///
/// # Errors
///
/// [`PublishRefused`], which is the topic being unusable, the topic
/// being reserved, or the store being unable to take the batch. The
/// first two are refusals made before anything is written; the third is
/// a publication that was made and then could not be promised, and
/// [`PublishRefused::NotDurable`] says the caller is told so rather
/// than answered.
pub async fn publish(
    shared: &Shared,
    publication: Publication,
) -> Result<Published, PublishRefused> {
    let Publication {
        topic,
        payload,
        qos,
        retain,
        message_expiry_interval,
        user_properties,
        origin,
    } = publication;
    // Only at-most-once promises nobody anything. Both acknowledged
    // levels are a promise, and both wait.
    let promised = qos != QoS::AtMostOnce;
    let properties = Properties {
        message_expiry_interval,
        user_properties,
        ..Properties::default()
    };

    let (outcome, committed) = shared.with_broker_committed(
        |broker| {
            // Validated inside the lock only because the limits are the
            // broker's: one acquisition rather than two, and the work
            // is a scan of a short string.
            mqtt_codec::topic::validate_name(&topic, &broker.config().limits)
                .map_err(|error| PublishRefused::NotATopic(error.to_string()))?;
            if topic.starts_with('$') {
                return Err(PublishRefused::Reserved);
            }

            let now = broker.clock().now();
            let mut message = Message::new(&topic, payload.clone(), qos, retain);
            // The same two lines a connection's publish uses, so the
            // properties a subscriber receives and the deadline the
            // broker enforces come from one rule.
            message.properties = Arc::new(properties.clone().into());
            message.expires_at = properties
                .message_expiry_interval
                .map(|interval| broker.clock().deadline(interval));

            let mut out = Vec::new();
            // The door every publication without a session goes
            // through: it fires the message hooks, so a rule can act on
            // what a connector brought in, and it applies section 9.2's
            // hop limit, because firing them is what lets a cycle
            // exist. There is no publishing session, so no-local cannot
            // apply and this is not any subscriber's own message;
            // `originate` passes `None` on for that reason.
            let delivered = broker.originate(message, &origin, now, &mut out)?;
            Ok((delivered, out))
        },
        // A batch that failed validation, or that the hop limit
        // refused, produced nothing and promised nothing.
        |outcome: &Result<(usize, Vec<_>), PublishRefused>| promised && outcome.is_ok(),
    );

    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(refused) => {
            // Section 9.2: dropped **and counted** — and, since this
            // milestone, hooked, because section 11.3 asks for both
            // and the hop limit returned before any action existed for
            // one to be attached to. Counted here rather than in the
            // broker, which holds no metrics.
            //
            // **Every refusal, not only the hop limit.** A topic the
            // broker will not take and a batch the store would not
            // hold are messages that disappeared just as completely,
            // and until this they were an error string handed back to
            // a caller and nothing else.
            refuse(shared, &origin, &topic, qos, refused.refusal());
            if let PublishRefused::TooManyHops(looped) = &refused {
                // Warned about because a cycle is a configuration
                // mistake an operator has to be told about: nothing
                // else in the system can see it, by construction,
                // since no single rule is in the whole of it.
                tracing::warn!(
                    %origin,
                    topic,
                    hops = looped.hops,
                    maximum = looped.maximum,
                    "a publication has been through more connectors than broker.max_hops                      allows and is dropped; there is a cycle of rules to find"
                );
            }
            return Err(refused);
        }
    };
    let (delivered, actions) = outcome;

    // Section 8.6, the same as on a connection: this path asked the
    // copies too, and an answer that said `durable: true` to a caller
    // in synchronous mode while no copy held the message would be the
    // mode not applying to the one publish the operator makes by hand.
    // Counted either way; refused when the operator said so.
    let (durable, confirmed) = committed.settled().await;
    if confirmed == cluster::Confirmed::No {
        shared.metrics().publish_unconfirmed().inc();
        if shared.refuses_unconfirmed() {
            tracing::warn!(
                %origin,
                topic,
                "a publication made without a session was not confirmed by any copy and \
                 cluster.strict.unconfirmed is set, so it is refused rather than answered"
            );
            // The subscribers were promised nothing and have the
            // delivery recorded against their windows; see the
            // durability failure below for why they are served.
            shared.deliver(actions);
            refuse(
                shared,
                &origin,
                &topic,
                qos,
                PublishRefused::NotConfirmed.refusal(),
            );
            return Err(PublishRefused::NotConfirmed);
        }
    }

    if let Err(error) = durable {
        // The same partition a connection makes, with the easy half:
        // nothing in this batch is addressed to the caller, because the
        // caller has no session. So every action goes, exactly as it
        // does for a publisher that is being closed — those subscribers
        // are alive, were promised nothing, and have already had the
        // delivery recorded against their windows. Leaving them
        // undelivered would strand a window slot until the peer
        // reconnected.
        tracing::error!(
            %error,
            "a publication made without a session could not be made durable; \
             the caller is told so rather than answered"
        );
        shared.deliver(actions);
        // Counted and hooked like every other refusal, and here rather
        // than only in the caller: this one is a publication that was
        // *made* and then could not be promised, which is the case a
        // caller is least able to describe and the one an operator
        // most wants a number for.
        refuse(
            shared,
            &origin,
            &topic,
            qos,
            PublishRefused::NotDurable.refusal(),
        );
        return Err(PublishRefused::NotDurable);
    }
    shared.deliver(actions);

    Ok(Published {
        delivered,
        durable: promised,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use broker_core::broker::BrokerConfig;
    use broker_core::hooks::{ClientInfo, Disposition, Hook};

    use super::*;

    /// Writes down what it was told about, so a test can say the door
    /// was opened rather than that a message arrived.
    #[derive(Default)]
    struct Recorder {
        seen: Mutex<Vec<String>>,
        refused: Mutex<Vec<(String, String, Refusal)>>,
    }

    impl Recorder {
        fn seen(&self) -> Vec<String> {
            self.seen.lock().expect("no test panics holding it").clone()
        }

        fn refusals(&self) -> Vec<(String, String, Refusal)> {
            self.refused
                .lock()
                .expect("no test panics holding it")
                .clone()
        }
    }

    impl Hook for Recorder {
        fn name(&self) -> &str {
            "recorder"
        }

        fn message_received(&self, client: &ClientInfo<'_>, message: &mut Message) -> Disposition {
            self.seen
                .lock()
                .expect("no test panics holding it")
                .push(format!("{} {}", client.client_id, message.topic));
            Disposition::Deliver
        }

        fn message_refused(
            &self,
            origin: &ClientInfo<'_>,
            topic: &str,
            _qos: QoS,
            reason: Refusal,
        ) {
            self.refused
                .lock()
                .expect("no test panics holding it")
                .push((origin.client_id.to_owned(), topic.to_owned(), reason));
        }
    }

    fn publication(topic: &str, user_properties: Vec<(String, String)>) -> Publication {
        Publication {
            topic: topic.to_owned(),
            payload: Bytes::from_static(b"body"),
            qos: QoS::AtMostOnce,
            retain: false,
            message_expiry_interval: None,
            user_properties,
            origin: "a-connector".to_owned(),
        }
    }

    fn hops(count: u32) -> Vec<(String, String)> {
        vec![(broker_core::hops::HOP_COUNT.to_owned(), count.to_string())]
    }

    /// Section 9.2's sentence, at the layer that made it false: a
    /// publication with no session behind it reaches the rules.
    #[tokio::test]
    async fn a_publication_here_fires_the_message_hooks() {
        let shared = Shared::new(BrokerConfig::default());
        let recorder = Arc::new(Recorder::default());
        shared.with_broker(|broker| {
            broker.hooks_mut().install(recorder.clone());
        });

        publish(&shared, publication("ingest/across", Vec::new()))
            .await
            .expect("a fresh publication is under every limit");

        assert_eq!(
            recorder.seen(),
            vec!["a-connector ingest/across".to_owned()],
            "the rule chain was asked, and told what originated it"
        );
    }

    /// And one over the limit is refused rather than published, so the
    /// caller can say so and the cycle stops here.
    #[tokio::test]
    async fn a_publication_over_the_hop_limit_is_refused_and_counted() {
        let shared = Shared::new(BrokerConfig {
            max_hops: 2,
            ..BrokerConfig::default()
        });
        let recorder = Arc::new(Recorder::default());
        shared.with_broker(|broker| {
            broker.hooks_mut().install(recorder.clone());
        });

        let refused = publish(&shared, publication("a/b", hops(3)))
            .await
            .expect_err("three hops is over a maximum of two");
        let PublishRefused::TooManyHops(looped) = refused else {
            panic!("the refusal must name the limit, not something else: {refused}");
        };
        assert_eq!(looped.hops, 3);
        assert_eq!(looped.maximum, 2);
        assert!(
            recorder.seen().is_empty(),
            "and no rule was fired by it: {:?}",
            recorder.seen()
        );
        assert_eq!(
            shared
                .metrics()
                .publication_refused(Refusal::TooManyHops)
                .get(),
            1,
            "section 9.2 says dropped *and counted*"
        );
    }

    /// The counter is the whole of what an operator has to find a
    /// cycle with, so it counts every one rather than the first.
    #[tokio::test]
    async fn every_publication_over_the_limit_is_counted() {
        let shared = Shared::new(BrokerConfig {
            max_hops: 1,
            ..BrokerConfig::default()
        });
        for _ in 0..3 {
            publish(&shared, publication("a/b", hops(2)))
                .await
                .expect_err("two hops is over a maximum of one");
        }
        assert_eq!(
            shared
                .metrics()
                .publication_refused(Refusal::TooManyHops)
                .get(),
            3
        );
    }

    /// A publication under the limit moves nothing, so an operator
    /// reading a non-zero counter is reading a real cycle.
    #[tokio::test]
    async fn a_publication_under_the_limit_moves_no_counter() {
        let shared = Shared::new(BrokerConfig {
            max_hops: 2,
            ..BrokerConfig::default()
        });
        publish(&shared, publication("a/b", hops(2)))
            .await
            .expect("two hops is the maximum, not over it");
        assert_eq!(
            shared
                .metrics()
                .publication_refused(Refusal::TooManyHops)
                .get(),
            0
        );
    }

    /// Every refusal moves its own counter, and no other.
    ///
    /// Section 11.3's rule applied to milestone four: a message never
    /// disappears without being counted. Four refusals, four series,
    /// and every one of the other three asserted to have stayed still
    /// — because a family whose reasons shared a slot would say a
    /// connector lost a message without saying which door it was lost
    /// at, and "the topic was reserved" and "the store would not take
    /// it" are different support cases.
    ///
    /// `NotDurable` is not here: it needs a store that fails, which is
    /// the gated backend `crates/rules/tests/delivery.rs` uses. It is
    /// on the same one call as the other three, which is the point of
    /// there being one call.
    #[tokio::test]
    async fn each_refusal_moves_its_own_counter() {
        // A publication, the refusal it should produce, and nothing
        // else.
        let cases: [(&str, Refusal); 3] = [
            // A wildcard is not a topic name.
            ("a/+/b", Refusal::NotATopic),
            // Section 12.3's reserved namespace.
            ("$sys/made-up", Refusal::Reserved),
            // Over the limit set below.
            ("a/b", Refusal::TooManyHops),
        ];

        for (topic, expected) in cases {
            let shared = Shared::new(BrokerConfig {
                max_hops: 1,
                ..BrokerConfig::default()
            });
            let hops = if expected == Refusal::TooManyHops {
                hops(2)
            } else {
                Vec::new()
            };
            publish(&shared, publication(topic, hops))
                .await
                .expect_err("every case here is a refusal");

            for reason in [
                Refusal::Looped,
                Refusal::TooManyHops,
                Refusal::NotATopic,
                Refusal::Reserved,
                Refusal::NotDurable,
                Refusal::NotConfirmed,
            ] {
                let moved = u64::from(reason == expected);
                assert_eq!(
                    shared.metrics().publication_refused(reason).get(),
                    moved,
                    "publishing to {topic} should have moved {expected:?} and \
                     {reason:?} reads otherwise"
                );
            }
        }
    }

    /// And every refusal reaches a hook.
    ///
    /// Section 11.3 asks for a metric **and a hook** on every discard
    /// path. The hop limit had the metric and not the hook, because it
    /// returns before any action exists for one to be attached to —
    /// and the other three had neither. The hook is fired from here
    /// rather than from inside the broker for the same reason the
    /// counter is: this is the one door, and a hook fired at three
    /// call sites is a hook missing from the fourth.
    #[tokio::test]
    async fn a_refused_publication_reaches_a_hook_that_can_say_which_and_why() {
        let shared = Shared::new(BrokerConfig {
            max_hops: 1,
            ..BrokerConfig::default()
        });
        let recorder = Arc::new(Recorder::default());
        shared.with_broker(|broker| {
            broker.hooks_mut().install(recorder.clone());
        });

        publish(&shared, publication("$sys/made-up", Vec::new()))
            .await
            .expect_err("the reserved namespace is not publishable");
        publish(&shared, publication("over/the/limit", hops(2)))
            .await
            .expect_err("two hops is over a maximum of one");

        assert_eq!(
            recorder.refusals(),
            vec![
                (
                    "a-connector".to_owned(),
                    "$sys/made-up".to_owned(),
                    Refusal::Reserved
                ),
                (
                    "a-connector".to_owned(),
                    "over/the/limit".to_owned(),
                    Refusal::TooManyHops
                ),
            ],
            "the hook was not told which connector, which topic, or why"
        );
        assert!(
            recorder.seen().is_empty(),
            "a refused publication fired a rule: {:?}",
            recorder.seen()
        );
    }

    // -----------------------------------------------------------
    // Section 8.6 on this path.
    // -----------------------------------------------------------

    /// A node replicating in `mode`, with a gated store behind it so
    /// the batch is durable at once and the only wait is the copies'.
    fn replicating(
        mode: cluster::Durability,
        patience: Duration,
    ) -> (Arc<Shared>, Arc<cluster::Copies>) {
        let store = Arc::new(crate::shared::gated::GatedStore::new());
        let writer = storage::Writer::spawn(
            Arc::clone(&store) as Arc<dyn storage::Storage>,
            storage::WriterConfig::default(),
        )
        .expect("a writer thread");
        let shared = Arc::new(Shared::with_store(
            broker_core::broker::Broker::new(BrokerConfig::default()),
            Arc::new(writer),
        ));
        let copies = Arc::new(cluster::Copies::new());
        shared.install_copies(Arc::clone(&copies));
        shared.install_durability(mode, patience);
        (shared, copies)
    }

    /// A durable session that is not connected, subscribed to
    /// `filter`, so a publication to it queues and is replicated.
    fn a_queued_session(shared: &Shared, client_id: &str, filter: &str) -> storage::SessionKey {
        shared.with_broker(|broker| {
            let mut out = Vec::new();
            let connect = mqtt_codec::packet::Connect {
                version: mqtt_codec::Version::V5,
                client_id: client_id.into(),
                clean_start: false,
                keep_alive: 0,
                username: None,
                password: None,
                will: None,
                properties: Properties {
                    session_expiry_interval: Some(300),
                    ..Properties::default()
                },
            };
            let registration = broker
                .connect("", &connect, &mut out)
                .expect("a fresh session is admitted");
            broker
                .handle(
                    registration.subscriber,
                    &mqtt_codec::Packet::Subscribe(mqtt_codec::packet::Subscribe {
                        packet_id: 1,
                        filters: vec![mqtt_codec::packet::SubscriptionOptions {
                            filter: filter.into(),
                            qos: QoS::AtLeastOnce,
                            no_local: false,
                            retain_as_published: false,
                            retain_handling: mqtt_codec::packet::RetainHandling::DoNotSend,
                        }],
                        properties: Properties::default(),
                    }),
                    &mut out,
                )
                .expect("the subscription is taken");
            broker.disconnect(registration.subscriber, &mut out);
        });
        storage::SessionKey::new("", client_id)
    }

    /// An at-least-once publication, which is the level that promises.
    fn acknowledged(topic: &str) -> Publication {
        Publication {
            qos: QoS::AtLeastOnce,
            ..publication(topic, Vec::new())
        }
    }

    /// Synchronous mode applies to a publication made here, as it does
    /// to one made on a connection.
    ///
    /// Until this test the answer said `durable: true` the moment the
    /// store had it, in every mode: an operator who set `sync` and
    /// published by hand to check it was being told the store's
    /// answer and not the copies'.
    #[tokio::test]
    async fn synchronous_mode_waits_for_a_follower_here_as_well() {
        let (shared, copies) = replicating(cluster::Durability::Sync, Duration::from_secs(30));
        let session = a_queued_session(&shared, "sensor", "site/+/temp");
        copies.marked(session.clone(), [0xaa; 16], 0);

        let releasing = Arc::clone(&copies);
        let told = session.clone();
        let confirmed_at = Arc::new(Mutex::new(None));
        let noted = Arc::clone(&confirmed_at);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            *noted.lock().expect("a lock") = Some(std::time::Instant::now());
            releasing.marked(told, [0xaa; 16], u64::MAX);
        });

        let published = publish(&shared, acknowledged("site/1/temp"))
            .await
            .expect("the follower confirms, late");
        let answered_at = std::time::Instant::now();
        assert!(published.durable);
        let confirmed = confirmed_at
            .lock()
            .expect("a lock")
            .expect("the answer came before the follower confirmed, so it did not wait");
        assert!(
            answered_at >= confirmed,
            "the answer must follow the confirmation"
        );
        assert_eq!(shared.metrics().publish_unconfirmed().get(), 0);
    }

    /// And one no copy confirms is counted, and with the flag refused.
    ///
    /// Both halves in one test, in that order, because the counter has
    /// to move whether or not the flag is set and a test of the
    /// refusal alone would pass against a build that only counted on
    /// refusing.
    #[tokio::test]
    async fn an_unconfirmed_publication_is_counted_and_with_the_flag_refused() {
        let (shared, copies) = replicating(cluster::Durability::Sync, Duration::from_millis(100));
        let session = a_queued_session(&shared, "sensor", "site/+/temp");
        // A follower that has reported once and will never catch up.
        copies.marked(session, [0xaa; 16], 0);

        let published = publish(&shared, acknowledged("site/1/temp"))
            .await
            .expect("without the flag an unconfirmed publication is answered");
        assert!(
            published.durable,
            "the store had it; the copies are what did not"
        );
        assert_eq!(shared.metrics().publish_unconfirmed().get(), 1);
        assert_eq!(
            shared
                .metrics()
                .publication_refused(Refusal::NotConfirmed)
                .get(),
            0,
            "counted as unconfirmed is not counted as refused"
        );

        shared.install_strict_unconfirmed(true);
        let refused = publish(&shared, acknowledged("site/2/temp"))
            .await
            .expect_err("with the flag it is refused");
        assert!(
            matches!(refused, PublishRefused::NotConfirmed),
            "refused for the right reason, not another: {refused}"
        );
        assert_eq!(shared.metrics().publish_unconfirmed().get(), 2);
        assert_eq!(
            shared
                .metrics()
                .publication_refused(Refusal::NotConfirmed)
                .get(),
            1
        );
    }
}
