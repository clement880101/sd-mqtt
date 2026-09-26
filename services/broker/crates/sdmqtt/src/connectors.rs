//! Wiring rules into a running broker.
//!
//! This is the shell half of section 9: the `rules` crate knows what a
//! rule is and what posting means, `broker-core` knows what a hook is,
//! and neither of them knows about the other. Here is where the two are
//! joined, because here is the only place that holds a store, a writer
//! and a broker at once.
//!
//! # Two halves, and the line between them is the broker's lock
//!
//! **The hook writes and returns.** [`RuleHook::message_received`] runs
//! on the thread handling a publish, under the one lock every packet in
//! the process passes through, so it does exactly two things: it decides
//! which rules the message fires, and it hands the writer a batch that
//! says so. Handing over is a send on an unbounded channel. Nothing here
//! awaits, blocks, or touches a socket, which is what
//! [`broker_core::hooks`] requires and does not enforce.
//!
//! **The pump posts.** One task per rule, spawned before any listener
//! binds, reading the outbox the hook filled. A rule whose endpoint is
//! down slows nothing down but itself.
//!
//! # Rules are written into the store at startup
//!
//! A rule declared in the configuration is put into the store before the
//! broker serves anything, because an outbox hangs off a rule and the
//! store refuses an envelope for a rule it does not hold. The put is
//! idempotent — a rule is whatever the last put said it was — so a
//! restart with an unchanged file changes nothing, and a restart with an
//! edited one applies the edit **without touching the outbox**, which is
//! what makes turning a connector off and on again safe.
//!
//! A rule the store holds that the configuration no longer names is left
//! alone, not run, and logged. Deleting it would take its outbox with
//! it, and anything still waiting in that outbox is a message somebody
//! was told had been accepted.
//!
//! # A singleton is started here, and asks whether it may run
//!
//! Section 9.2 makes a poller a singleton owned by the live member with
//! the lowest node identifier. Milestone 4 has one member, so the
//! [`Membership`] built below holds one node and that node owns
//! everything — but it is a membership view and the poller asks it,
//! rather than a poller written on the assumption that it is alone. The
//! reasoning is `rules::singleton`'s; what happens here is that
//! milestone 5 replaces one constructor call and nothing else.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use broker_core::hooks::{ClientInfo, Disposition, Hook};
use broker_core::session::message::Message;
use bytes::Bytes;
use mqtt_codec::QoS;
use rules::bridge::marker::Marker;
use rules::bridge::rewrite::Rewrite;
use rules::bridge::session::Bridge;
use rules::bridge::supervisor::Supervisor;
use rules::http_out::{client, DeadLetters, Delivery, Poster};
use rules::http_poll::Poller;
use rules::rule::{Action, Rule, Trigger};
use rules::singleton::{Membership, NodeId};
use rules::{Ingest, Outbox};
use sdmqtt_server::shared::Shared;
use storage::record::{BodyRecord, MessageId, Qos, Record};
use storage::{Storage, Writer};
use tokio::sync::Notify;

use crate::config::Config;
use crate::StartupError;

/// One rule and the outbox it fills.
struct Fired {
    /// What decides whether a message fires it.
    rule: Rule,
    /// Where an envelope goes when it does.
    outbox: Arc<Outbox>,
    /// The local filters a bridge's outbound entries fire on.
    ///
    /// Empty for every other kind of rule, which is fired by the
    /// rule's own trigger. A bridge has no rule-level trigger — what
    /// it carries is decided by its topic entries, each with a
    /// direction and a rewrite that a single filter has nowhere to put
    /// — so this is where its filters live instead.
    bridge_filters: Vec<String>,
    /// Woken when something has been put in this rule's outbox.
    ///
    /// `Some` for a bridge, whose pump is a live socket and should
    /// send a message the moment it exists rather than on the next
    /// turn of a poll. `None` for `http_out`, which polls: its pump
    /// sleeps on a backoff that belongs to the rule, and waking it
    /// early would mean posting inside a wait that exists to stop
    /// exactly that.
    wake: Option<Arc<Notify>>,
}

impl Fired {
    /// Whether this message fires this rule.
    fn fires(&self, topic: &str, qos: QoS, client_id: &str) -> bool {
        if self.bridge_filters.is_empty() {
            return self.rule.fires_on(topic, qos, client_id);
        }
        // A disabled rule fires on nothing, here as everywhere.
        self.rule.enabled
            && self
                .bridge_filters
                .iter()
                .any(|filter| broker_core::topic::reference::matches(filter, topic))
    }
}

/// The hook that turns a publication into envelopes.
///
/// Read [`broker_core::hooks`] before changing anything here. This runs
/// under the broker's lock: for as long as it is running, no other
/// connection in the process is served.
struct RuleHook {
    /// Every rule this node holds, in the order they were configured.
    rules: Vec<Fired>,
    /// Where the batch goes. A send on an unbounded channel, which is
    /// the only kind of hand-over a hook may make.
    writer: Arc<Writer>,
}

impl Hook for RuleHook {
    fn name(&self) -> &str {
        "rules"
    }

    /// Append an envelope to the outbox of every rule this fires.
    ///
    /// **Before matching**, which is what the hook point is for: a rule
    /// sees the topic the message was published on rather than a topic
    /// some later hook rewrote it to.
    ///
    /// # What fires this, which is more than it was
    ///
    /// A client's publish, a will, and — since section 9.2 was made
    /// true — every publication with no session behind it: the inbound
    /// half of a bridge, section 9.2's ingest endpoint, a poller and
    /// the admin API. That is what lets connectors chain, and it is
    /// also what makes a cycle of them possible; the hop count on the
    /// message is what bounds one, applied at
    /// [`broker_core::broker::Broker::originate`] before this is ever
    /// called.
    ///
    /// **`client.client_id` is not always a client.** For a publication
    /// with no session it is whatever originated it, which for all four
    /// of those is the rule identifier. A rule's trigger can be
    /// narrowed by a client identifier pattern, so that is how an
    /// operator says "the messages this bridge brought in" or "not the
    /// ones that came through ingest".
    ///
    /// The message is always delivered. A rule is a copy of a message
    /// sent somewhere else, not a filter on where it goes; dropping a
    /// publication because a connector wanted it would be a connector
    /// that quietly breaks subscribers.
    ///
    /// # What this does that costs anything
    ///
    /// A clone of the payload — [`Bytes`], so a reference count — a
    /// clone of the topic, the encoded property block, which the
    /// message caches and the broker's own journal asks for anyway, and
    /// one wall-clock read. Nothing proportional to anything a client
    /// controls beyond the payload it already sent, and nothing that can
    /// block.
    fn message_received(&self, client: &ClientInfo<'_>, message: &mut Message) -> Disposition {
        let now = now_seconds();
        let mut records = Vec::new();
        let mut wake = Vec::new();
        for fired in &self.rules {
            if !fired.fires(&message.topic, message.qos, client.client_id) {
                continue;
            }
            let body = BodyRecord {
                topic: message.topic.to_string(),
                payload: message.payload.clone(),
                qos: stored_qos(message.qos),
                retain: message.retain,
                expires_at: message.expires_at,
                properties: message.properties.encoded(),
            };
            // The body is filed under the broker's own identifier for
            // this publication, so a message that reaches a durable
            // subscriber *and* fires three rules is one stored payload
            // with four references, not four copies of it.
            let (_, more) = fired.outbox.append(
                MessageId(*broker_core::session::MessageId::new().as_bytes()),
                MessageId(*message.id.as_bytes()),
                body,
                client.client_id,
                now,
            );
            records.extend(more);
            if let Some(notify) = &fired.wake {
                wake.push(Arc::clone(notify));
            }
        }
        if !records.is_empty() && self.writer.apply(records).is_err() {
            // Nothing else can be done from inside the lock, and the
            // alternative — refusing the publication — would take
            // subscribers down with the connector.
            tracing::error!("the storage writer stopped; a rule could not record an envelope");
        }
        // **After the batch has been handed over**, so a woken pump
        // finds the entry rather than an empty outbox. A notify is an
        // atomic store on a flag and is one of the two things a hook
        // may do; see [`broker_core::hooks`].
        for notify in wake {
            notify.notify_one();
        }
        Disposition::Deliver
    }
}

/// Publishes a dead letter back into the broker.
///
/// Section 9.2 sends an envelope that ran out of attempts to
/// `$sd/rules/<id>/dlq`. This is called from a pump's task rather than
/// from a hook, so taking the broker's lock is allowed here exactly as
/// it is for a connection.
///
/// **It cannot loop.** [`broker_core::broker::Broker::publish`] is the
/// internal route that fires no message hook, so a dead letter cannot
/// fire the rule that produced it.
///
/// It is the only caller left on that route besides a will, and it
/// stayed there on purpose when everything else moved to
/// [`broker_core::broker::Broker::originate`]. A dead letter is the
/// broker saying a rule's endpoint has stopped answering; firing that
/// rule with it would be a cycle with a guaranteed period, and one
/// whose every turn writes another envelope into the outbox that is
/// already backed up.
struct BrokerDeadLetters {
    shared: Arc<Shared>,
}

impl DeadLetters for BrokerDeadLetters {
    fn dead_letter(&self, topic: &str, payload: Vec<u8>) {
        let message = Message::new(topic, Bytes::from(payload), QoS::AtMostOnce, false);
        let now = now_seconds();
        let mut out = Vec::new();
        self.shared.with_broker(|broker| {
            broker.publish(message, None, now, &mut out);
        });
        self.shared.deliver(out);
    }
}

/// The pumps this node is running.
///
/// Held so that they stop with the broker. A pump is an infinite loop
/// around a store read and a socket, and the process stopping is the
/// only thing that ends it.
pub struct Connectors {
    pumps: Vec<tokio::task::JoinHandle<()>>,
    /// Who is live, which is who owns the singletons.
    ///
    /// Held because dropping it would leave every [`rules::Ownership`]
    /// handle watching a view that can never change again. Nothing
    /// changes it in milestone 4, and something will in milestone 5.
    _membership: Arc<Membership>,
    /// How many rules were installed, of either kind.
    ///
    /// Not `pumps.len()`: an ingest rule has no pump, and a node whose
    /// only rule was an ingest one would otherwise report none.
    rules: usize,
    /// The ingest rules, for whoever serves their routes.
    ///
    /// Built here because this is where a rule is turned into the thing
    /// that runs it, and handed out rather than served here because
    /// which listener serves it is configuration; see section 9.2.
    ingest: Arc<Ingest>,
}

impl Connectors {
    /// How many rules are installed.
    pub fn len(&self) -> usize {
        self.rules
    }

    /// The ingest rules this node serves.
    pub fn ingest(&self) -> &Arc<Ingest> {
        &self.ingest
    }

    /// Stop every pump.
    ///
    /// Aborted rather than asked politely, and that is safe because of
    /// where the state is: everything about how far a delivery has got
    /// is in the store, written and made durable before the pump moves
    /// on. A pump killed between two awaits loses a request in flight,
    /// which the next start retries — the at-least-once guarantee
    /// already promises the endpoint a repeat.
    pub async fn shutdown(self) {
        for pump in &self.pumps {
            pump.abort();
        }
        for pump in self.pumps {
            // An abort is reported as a cancellation, which is what was
            // asked for. Anything else is a pump that panicked, which
            // is worth a line.
            if let Err(error) = pump.await {
                if !error.is_cancelled() {
                    tracing::error!(%error, "a rule's delivery task ended badly");
                }
            }
        }
    }
}

/// No rules, because there is no store to hold one.
///
/// Its own function rather than an `Option<Connectors>`, because what
/// the caller does with it afterwards is identical.
pub fn start_without_rules() -> Connectors {
    Connectors {
        pumps: Vec::new(),
        _membership: Arc::new(Membership::alone(NodeId::new(THIS_NODE))),
        rules: 0,
        ingest: Arc::new(Ingest::new()),
    }
}

/// What this node calls itself.
///
/// Section 8.1 gives a node a persisted identity and that is milestone
/// 5's; until then there is one node and this is what it is called.
/// The same string the admin API reports as the only row in its node
/// listing, so an operator reading one is reading the other.
const THIS_NODE: &str = "local";

/// Install the configured rules and start their pumps.
///
/// Called after the store has been read and before any listener binds,
/// which is the order that matters: a client cannot publish into a
/// broker whose rules are half installed.
///
/// # Errors
///
/// [`StartupError::Connector`] when a rule cannot be stored. A rule that
/// cannot be *used* was refused by `Config::validate` long before this.
pub async fn start(
    config: &Config,
    shared: &Arc<Shared>,
    store: &Arc<dyn Storage>,
    writer: &Arc<Writer>,
) -> Result<Connectors, StartupError> {
    if config.rules.is_empty() {
        return Ok(start_without_rules());
    }

    // One client for every rule: the connection pool is the thing worth
    // sharing, and two rules posting to one endpoint should reuse a
    // connection rather than each holding their own.
    let client = client();
    let dead_letters: Arc<dyn DeadLetters> = Arc::new(BrokerDeadLetters {
        shared: Arc::clone(shared),
    });

    // One member, so it owns every singleton. Written as a view
    // because a rule only ever exercised in the one-member case is a
    // rule nobody has tested; see `rules::singleton`.
    let membership = Arc::new(Membership::alone(NodeId::new(THIS_NODE)));

    let mut configured = BTreeSet::new();
    let mut fired = Vec::new();
    let mut pumps = Vec::new();
    let mut ingest = Ingest::new();

    for section in &config.rules {
        // `Config::validate` has already refused anything that cannot
        // be used, so this cannot fail. Going through the same function
        // rather than a second construction of the rule is what keeps
        // that true.
        let rule = section
            .to_rule()
            .map_err(|error| StartupError::Connector(error.to_string()))?;
        configured.insert(rule.id.as_str().to_owned());

        // Durable before anything hangs off it. The store refuses an
        // envelope for a rule it does not hold, and the first thing a
        // client publishes could be one.
        writer
            .apply_durable(vec![Record::RulePut {
                rule: rule.to_record(),
            }])
            .map_err(|error| StartupError::Connector(error.to_string()))?
            .await
            .map_err(|error| StartupError::Connector(error.to_string()))?;

        match &rule.action {
            Action::HttpOut(action) => {
                // An outbox hangs off an `http_out` rule and off no
                // other: an ingest rule has nothing to queue, because
                // what it does is finished by the time its caller is
                // answered.
                let outbox = Arc::new(
                    Outbox::open(rule.id.clone(), store.as_ref())
                        .map_err(|error| StartupError::Connector(error.to_string()))?,
                );
                let waiting = outbox
                    .depth(store.as_ref())
                    .map_err(|error| StartupError::Connector(error.to_string()))?;

                let poster = Poster::new(client.clone(), action)
                    .map_err(|error| StartupError::Connector(error.to_string()))?;
                let delivery = Delivery::new(
                    Arc::clone(&outbox),
                    Arc::clone(store),
                    Arc::clone(writer),
                    poster,
                    action,
                    Arc::clone(&dead_letters),
                );

                tracing::info!(
                    rule = rule.id.as_str(),
                    name = %rule.name,
                    enabled = rule.enabled,
                    filter = rule.trigger.as_ref().map_or("", Trigger::filter),
                    url = %action.url,
                    waiting,
                    "a rule is installed"
                );
                if waiting > 0 {
                    // The whole point of the milestone, said out loud:
                    // these are messages accepted before the last stop
                    // and not yet delivered.
                    tracing::info!(
                        rule = rule.id.as_str(),
                        waiting,
                        "envelopes were waiting in this rule's outbox and are being \
                         delivered now"
                    );
                }

                pumps.push(tokio::spawn(delivery.run()));
                fired.push(Fired {
                    rule,
                    outbox,
                    bridge_filters: Vec::new(),
                    wake: None,
                });
            }
            Action::HttpPoll(action) => {
                // **No outbox**, and that is the connector rather than
                // an omission: a poller is given nothing, so a fetch
                // that failed has lost nothing, and it is asked again
                // on the next interval. See `rules::http_poll`.
                let poller =
                    Poller::new(rule.id.clone(), client.clone(), Arc::clone(shared), action)
                        .map_err(|error| StartupError::Connector(error.to_string()))?;

                tracing::info!(
                    rule = rule.id.as_str(),
                    name = %rule.name,
                    enabled = rule.enabled,
                    url = %action.url,
                    topic = %action.topic,
                    interval_ms = action.interval_ms,
                    owner = %membership.owner(),
                    "a rule is installed"
                );

                // **A disabled rule fetches nothing.** The same rule an
                // ingest rule's route follows: `enabled` is what an
                // operator reaches for to turn a connector off, and a
                // poller that kept fetching would be the one connector
                // that ignored the switch. Nothing is spawned rather
                // than a task that starts and stops, because there is
                // no state to keep warm.
                if rule.enabled {
                    pumps.push(tokio::spawn(poller.run(membership.ownership())));
                } else {
                    tracing::info!(
                        rule = rule.id.as_str(),
                        "this polling rule is switched off and fetches nothing"
                    );
                }
            }
            Action::MqttBridge(action) => {
                // **A bridge has an outbox, and an `http_poll` does
                // not.** The difference is what the connector is
                // given: a poller is given nothing and fetches, so a
                // failed fetch has lost nothing, while a bridge is
                // handed somebody else's message and losing it in a
                // restart loses something this broker accepted.
                let outbox = Arc::new(
                    Outbox::open(rule.id.clone(), store.as_ref())
                        .map_err(|error| StartupError::Connector(error.to_string()))?,
                );
                let waiting = outbox
                    .depth(store.as_ref())
                    .map_err(|error| StartupError::Connector(error.to_string()))?;
                let wake = Arc::new(Notify::new());

                let bridge = Bridge {
                    rule: rule.id.clone(),
                    action: (**action).clone(),
                    rewrites: action.topics.iter().map(Rewrite::new).collect(),
                    marker: Marker::new(&rule.id, action.version),
                    outbox: Arc::clone(&outbox),
                    store: Arc::clone(store),
                    writer: Arc::clone(writer),
                    shared: Arc::clone(shared),
                    wake: Arc::clone(&wake),
                };
                let outbound: Vec<String> = bridge
                    .rewrites
                    .iter()
                    .filter(|rewrite| rewrite.direction().outbound())
                    .map(|rewrite| rewrite.local_filter().to_owned())
                    .collect();
                let supervisor = Supervisor::new(bridge, action)
                    .map_err(|error| StartupError::Connector(error.to_string()))?;

                tracing::info!(
                    rule = rule.id.as_str(),
                    name = %rule.name,
                    enabled = rule.enabled,
                    address = %supervisor.address().authority(),
                    tls = supervisor.address().tls,
                    protocol_level = action.version.level(),
                    topics = action.topics.len(),
                    state_topic = %action.state_topic,
                    owner = %membership.owner(),
                    waiting,
                    "a rule is installed"
                );
                if !supervisor.address().tls {
                    // Only a loopback address gets here; `address`
                    // refuses a plain-text link to anywhere else. Said
                    // out loud anyway, because an operator reading a
                    // startup log should be able to see that a link
                    // carrying their data is in the clear.
                    tracing::warn!(
                        rule = rule.id.as_str(),
                        address = %supervisor.address().authority(),
                        "this bridge is plain text; it is permitted because the address \
                         is on this machine"
                    );
                }
                if !Marker::new(&rule.id, action.version).available() {
                    tracing::warn!(
                        rule = rule.id.as_str(),
                        "this bridge speaks MQTT 3.1.1, which has no user properties, so \
                         the loop marker cannot travel. Loop prevention here is the \
                         configuration refusal alone, which has already been made."
                    );
                }

                // **A disabled rule connects to nothing.** The same
                // rule the other three follow: `enabled` is what an
                // operator reaches for to turn a connector off.
                if rule.enabled {
                    pumps.push(tokio::spawn(supervisor.run(membership.ownership())));
                } else {
                    tracing::info!(
                        rule = rule.id.as_str(),
                        "this bridge is switched off and connects to nothing"
                    );
                }
                fired.push(Fired {
                    rule,
                    outbox,
                    bridge_filters: outbound,
                    wake: Some(wake),
                });
            }
            Action::HttpIn(action) => {
                // **A disabled rule gets no route.** `enabled` is what
                // an operator reaches for to turn a connector off, and
                // an ingest rule left serving while it was switched off
                // would be the one connector that ignored the switch.
                if rule.enabled {
                    ingest.register(&rule.id, action.clone());
                } else {
                    tracing::info!(
                        rule = rule.id.as_str(),
                        "this ingest rule is switched off and has no route"
                    );
                }
                tracing::info!(
                    rule = rule.id.as_str(),
                    name = %rule.name,
                    enabled = rule.enabled,
                    path = format!("/ingest/{}", rule.id),
                    topic = %action.topic,
                    // What a caller may choose, and what bounds it.
                    // Logged because the prefix is the whole safety of
                    // the feature and an operator should be able to see
                    // from the startup line what this endpoint can
                    // reach.
                    topic_from = action
                        .topic_from
                        .as_ref()
                        .map_or_else(String::new, |from| from.source.name().to_owned()),
                    topic_prefix = action
                        .topic_from
                        .as_ref()
                        .map_or("", |from| from.prefix.as_str()),
                    "a rule is installed"
                );
            }
        }
    }

    // A rule in the store that nothing names any more. Kept, because
    // deleting it would take its outbox with it, and not run, because
    // nothing here knows where it was supposed to post.
    let held = store
        .restore()
        .map_err(|error| StartupError::Connector(error.to_string()))?
        .rules;
    for orphan in held.iter().filter(|held| !configured.contains(&held.id)) {
        let waiting = store.outbox_depth(&orphan.id).unwrap_or(0);
        tracing::warn!(
            rule = %orphan.id,
            name = %orphan.name,
            waiting,
            "the store holds a rule this configuration does not name; \
             it is not running, and it is kept rather than deleted because \
             deleting it would discard whatever is still waiting in its outbox"
        );
    }

    shared.with_broker(|broker| {
        broker.hooks_mut().install(Arc::new(RuleHook {
            rules: fired,
            writer: Arc::clone(writer),
        }));
    });

    Ok(Connectors {
        pumps,
        _membership: membership,
        rules: config.rules.len(),
        ingest: Arc::new(ingest),
    })
}

/// A live level as the store spells it.
fn stored_qos(qos: QoS) -> Qos {
    match qos {
        QoS::AtMostOnce => Qos::AtMostOnce,
        QoS::AtLeastOnce => Qos::AtLeastOnce,
        QoS::ExactlyOnce => Qos::ExactlyOnce,
    }
}

/// The wall clock in whole seconds, as the stored records count time.
fn now_seconds() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u32::try_from(since.as_secs()).unwrap_or(u32::MAX)
        })
}
