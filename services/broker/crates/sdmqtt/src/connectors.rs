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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use broker_core::hooks::{ClientInfo, Disposition, Hook};
use broker_core::session::message::Message;
use bytes::Bytes;
use mqtt_codec::QoS;
use rules::http_out::{client, DeadLetters, Delivery, Poster};
use rules::rule::{Action, Rule};
use rules::Outbox;
use sdmqtt_server::shared::Shared;
use storage::record::{BodyRecord, MessageId, Qos, Record};
use storage::{Storage, Writer};

use crate::config::Config;
use crate::StartupError;

/// One rule and the outbox it fills.
struct Fired {
    /// What decides whether a message fires it.
    rule: Rule,
    /// Where an envelope goes when it does.
    outbox: Arc<Outbox>,
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
        for fired in &self.rules {
            if !fired
                .rule
                .fires_on(&message.topic, message.qos, client.client_id)
            {
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
        }
        if !records.is_empty() && self.writer.apply(records).is_err() {
            // Nothing else can be done from inside the lock, and the
            // alternative — refusing the publication — would take
            // subscribers down with the connector.
            tracing::error!("the storage writer stopped; a rule could not record an envelope");
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
/// internal route a will already takes and does not fire the message
/// hooks, so a dead letter cannot fire the rule that produced it.
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
}

impl Connectors {
    /// How many rules are running.
    pub fn len(&self) -> usize {
        self.pumps.len()
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
    Connectors { pumps: Vec::new() }
}

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
        return Ok(Connectors { pumps: Vec::new() });
    }

    // One client for every rule: the connection pool is the thing worth
    // sharing, and two rules posting to one endpoint should reuse a
    // connection rather than each holding their own.
    let client = client();
    let dead_letters: Arc<dyn DeadLetters> = Arc::new(BrokerDeadLetters {
        shared: Arc::clone(shared),
    });

    let mut configured = BTreeSet::new();
    let mut fired = Vec::new();
    let mut pumps = Vec::new();

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

        let outbox = Arc::new(
            Outbox::open(rule.id.clone(), store.as_ref())
                .map_err(|error| StartupError::Connector(error.to_string()))?,
        );
        let waiting = outbox
            .depth(store.as_ref())
            .map_err(|error| StartupError::Connector(error.to_string()))?;

        let Action::HttpOut(action) = &rule.action;
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
            filter = rule.trigger.filter(),
            url = %action.url,
            waiting,
            "a rule is installed"
        );
        if waiting > 0 {
            // The whole point of the milestone, said out loud: these
            // are messages accepted before the last stop and not yet
            // delivered.
            tracing::info!(
                rule = rule.id.as_str(),
                waiting,
                "envelopes were waiting in this rule's outbox and are being delivered now"
            );
        }

        pumps.push(tokio::spawn(delivery.run()));
        fired.push(Fired { rule, outbox });
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

    Ok(Connectors { pumps })
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
