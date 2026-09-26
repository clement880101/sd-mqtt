//! The rules this node is running, and what it takes to change them.
//!
//! This is the shell half of section 9 that used to live in the binary:
//! the [`rule`](crate::rule) module knows what a rule is, [`http_out`]
//! and the rest know what a connector does, `broker-core` knows what a
//! hook is, and here is where a rule becomes a thing that runs.
//!
//! # Why it moved, and why it is a registry rather than a list
//!
//! It was a list, built once from the configuration file before any
//! listener bound, because a configuration file was the only way to
//! write a rule. Section 11.1 makes the admin API a second way, and a
//! rule created over the API has to fire without the broker being
//! restarted — which a list built at startup cannot do, and which is
//! exactly the difference between a rule that went all the way and one
//! that went into a list in memory.
//!
//! So the installed rules are behind a lock, the hook reads them
//! through it, and installing one is an insert.
//!
//! # Two halves, and the line between them is the broker's lock
//!
//! **The hook writes and returns.** [`RuleHook::message_received`] runs
//! on the thread handling a publish, under the one lock every packet in
//! the process passes through, so it does exactly two things: it
//! decides which rules the message fires, and it hands the writer a
//! batch that says so. Handing over is a send on an unbounded channel.
//! Nothing there awaits, blocks, or touches a socket, which is what
//! [`broker_core::hooks`] requires and does not enforce.
//!
//! **The pump posts.** One task per rule, reading the outbox the hook
//! filled. A rule whose endpoint is down slows nothing down but itself.
//!
//! # What the lock costs the hook, and what it may not do
//!
//! The hook takes a **read** lock on a map, which is what it would have
//! paid to read a `Vec` behind a pointer anyway; the write side runs
//! when an operator changes a rule and at no other time.
//!
//! The rule that keeps that true is that **nothing holds the write lock
//! across an `await`**. Installing a rule makes a durable write and
//! opens an outbox, and both of those happen with no lock held; the
//! lock is taken to swap one entry and released. A write lock held
//! across a disk flush would stop every publish in the process for the
//! length of it, which is the same mistake the resources module
//! documents at length about the broker's own lock.
//!
//! # A rule is durable before anything hangs off it
//!
//! The store refuses an envelope for a rule it does not hold, and the
//! first thing a client publishes could be one. So the [`Record::RulePut`]
//! is made durable and waited for **before** the rule is in the map,
//! and a rule that could not be stored is not installed at all.
//!
//! # Deleting a rule takes its outbox with it
//!
//! That is the store's behaviour and it is the right one — an outbox
//! with no rule is a queue nothing will ever drain — but it means a
//! delete can discard messages this broker told somebody it had
//! accepted. [`Registry::remove`] therefore **refuses while anything is
//! waiting**, and says how much. An outbox drains on its own, to the
//! endpoint or to a dead letter, so the refusal is a wait rather than a
//! rule that can never be deleted; and an operator who wants a rule to
//! stop *now* wants [`Registry::set_enabled`], which is what that is
//! for.
//!
//! [`http_out`]: crate::http_out
//! [`Record::RulePut`]: storage::record::Record::RulePut

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use broker_core::hooks::{ClientInfo, Disposition, Hook};
use broker_core::session::message::Message;
use bytes::Bytes;
use http_body_util::Full;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use mqtt_codec::QoS;
use parking_lot::{Mutex, RwLock};
use sdmqtt_server::shared::Shared;
use storage::record::{BodyRecord, MessageId, Qos, Record};
use storage::{Storage, Writer};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::bridge::marker::Marker;
use crate::bridge::rewrite::Rewrite;
use crate::bridge::session::Bridge;
use crate::bridge::supervisor::Supervisor;
use crate::http_out::{client, DeadLetters, Delivery, Poster};
use crate::http_poll::Poller;
use crate::rule::{Action, Rule, RuleId};
use crate::singleton::Membership;
use crate::stats::{RuleStats, Sample, Totals, SAMPLE_INTERVAL};
use crate::{Ingest, Outbox};

/// The HTTP client every rule on this node shares.
///
/// One, because the connection pool is the thing worth sharing: two
/// rules posting to one endpoint should reuse a connection rather than
/// each holding their own.
pub type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<hyper::body::Bytes>>;

/// Why a rule could not be installed, or could not be taken away.
///
/// Not the same thing as [`crate::spec::SpecError`], which is a rule
/// that cannot mean what it says. Everything here is about this node:
/// a store that would not take the record, an outbox that would not
/// open, or a delete that would throw away messages.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InstallError {
    /// The store would not hold it, or could not be read.
    #[error("the rule {id} could not be stored: {why}")]
    Storage {
        /// Which rule.
        id: String,
        /// What the store said.
        why: String,
    },
    /// There is no rule by that name on this node.
    #[error("there is no rule called {id:?} on this node")]
    NoSuchRule {
        /// What was asked for.
        id: String,
    },
    /// The rule still has envelopes nobody has been given.
    ///
    /// See the module note: deleting a rule deletes its outbox, and
    /// the contents of that outbox are messages this broker accepted.
    #[error(
        "the rule {id:?} still has {waiting} envelope(s) waiting in its outbox, and \
         deleting a rule deletes its outbox; disable it instead, or wait for the outbox \
         to drain — every envelope in it ends at the endpoint or in a dead letter"
    )]
    OutboxNotEmpty {
        /// Which rule.
        id: String,
        /// How many envelopes are still waiting.
        waiting: u64,
    },
}

impl InstallError {
    /// A store failure about `id`.
    fn storage(id: &RuleId, why: impl ToString) -> Self {
        Self::Storage {
            id: id.as_str().to_owned(),
            why: why.to_string(),
        }
    }
}

/// One installed rule and everything running for it.
struct Installed {
    /// What decides whether a message fires it.
    rule: Rule,
    /// Where an envelope goes when it does.
    ///
    /// `None` for an action with nothing to queue: an ingest rule is
    /// finished by the time its caller is answered, and a poller is
    /// given nothing, so a fetch that failed has lost nothing.
    outbox: Option<Arc<Outbox>>,
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
    /// The task doing the posting, fetching or bridging.
    ///
    /// `None` for an ingest rule, which has no pump, and for a
    /// singleton that is switched off.
    pump: Option<JoinHandle<()>>,
    /// What this rule has been doing, and the series of it.
    ///
    /// **Kept across an update**, which is why it is taken from the
    /// old installation rather than made fresh in [`Registry::start`]:
    /// an operator who disables a rule and enables it again has not
    /// asked for its chart to start over, and a series that reset on
    /// every edit would be one nobody could read a trend from.
    stats: Arc<RuleStats>,
}

impl Installed {
    /// How many envelopes are waiting for this rule.
    ///
    /// Zero for a rule with no outbox, which is the true answer:
    /// section 9.2 gives an ingest rule nothing to queue and a poller
    /// nothing at all.
    fn waiting(&self) -> u64 {
        self.outbox.as_ref().map_or(0, |outbox| outbox.waiting())
    }
}

impl Installed {
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

    /// Stop whatever this rule was running.
    ///
    /// Aborted rather than asked politely, and that is safe because of
    /// where the state is: everything about how far a delivery has got
    /// is in the store, written and made durable before the pump moved
    /// on. A pump killed between two awaits loses a request in flight,
    /// which the next start retries — the at-least-once guarantee
    /// already promises the endpoint a repeat.
    fn stop(&mut self) {
        if let Some(pump) = self.pump.take() {
            pump.abort();
        }
    }
}

/// The rules this node is running.
///
/// Read the module documentation before changing anything here: what
/// makes this safe is where the lock is taken and what is not done
/// while holding it.
pub struct Registry {
    /// The broker to publish into, for a poller, a bridge and a dead
    /// letter.
    shared: Arc<Shared>,
    /// Where outboxes live.
    store: Arc<dyn Storage>,
    /// How anything is written down.
    writer: Arc<Writer>,
    /// Where an exhausted envelope goes.
    dead_letters: Arc<dyn DeadLetters>,
    /// Who is live, which is who owns the singletons.
    ///
    /// Held because dropping it would leave every
    /// [`crate::Ownership`] handle watching a view that can never
    /// change again. Nothing changes it in milestone 4, and something
    /// will in milestone 5.
    membership: Arc<Membership>,
    /// The one HTTP client.
    client: HttpClient,
    /// The ingest routes, which this installs into and withdraws from.
    ingest: Arc<Ingest>,
    /// What is installed, by identifier.
    installed: RwLock<BTreeMap<RuleId, Installed>>,
    /// The task that closes one interval of every rule's series and
    /// starts the next. See [`crate::stats`].
    sampler: Mutex<Option<JoinHandle<()>>>,
}

impl Registry {
    /// A registry over this node's broker and store, holding nothing.
    pub fn new(
        shared: Arc<Shared>,
        store: Arc<dyn Storage>,
        writer: Arc<Writer>,
        membership: Arc<Membership>,
    ) -> Arc<Self> {
        let dead_letters: Arc<dyn DeadLetters> = Arc::new(BrokerDeadLetters {
            shared: Arc::clone(&shared),
        });
        let registry = Arc::new(Self {
            shared,
            store,
            writer,
            membership,
            dead_letters,
            client: client(),
            ingest: Arc::new(Ingest::new()),
            installed: RwLock::new(BTreeMap::new()),
            sampler: Mutex::new(None),
        });
        *registry.sampler.lock() = Some(tokio::spawn(sample_forever(Arc::downgrade(&registry))));
        registry
    }

    /// The ingest routes this node serves.
    ///
    /// Handed out rather than served here, because which listener
    /// serves them is configuration; see section 9.2.
    pub fn ingest(&self) -> &Arc<Ingest> {
        &self.ingest
    }

    /// The hook that turns a publication into envelopes.
    ///
    /// Installed into the broker once. It holds a [`Weak`] reference
    /// rather than an [`Arc`], because the registry holds the broker
    /// and the broker would then hold the registry: the same cycle
    /// [`sdmqtt_server::metrics`] breaks the same way. A hook whose
    /// registry has gone does nothing, which is correct — there are no
    /// rules left to fire.
    pub fn hook(self: &Arc<Self>) -> Arc<dyn Hook> {
        Arc::new(RuleHook {
            registry: Arc::downgrade(self),
            writer: Arc::clone(&self.writer),
        })
    }

    /// How many rules are installed.
    pub fn len(&self) -> usize {
        self.installed.read().len()
    }

    /// Whether none is.
    pub fn is_empty(&self) -> bool {
        self.installed.read().is_empty()
    }

    /// Every installed rule, in identifier order.
    ///
    /// Cloned out under the lock and read outside it, which is the
    /// shape every read resource on the admin listener has: nothing is
    /// serialised while a lock is held.
    pub fn list(&self) -> Vec<Rule> {
        self.installed
            .read()
            .values()
            .map(|installed| installed.rule.clone())
            .collect()
    }

    /// One installed rule.
    pub fn get(&self, id: &RuleId) -> Option<Rule> {
        self.installed
            .read()
            .get(id)
            .map(|installed| installed.rule.clone())
    }

    /// Store `rule` and start whatever it runs, replacing any rule of
    /// the same name.
    ///
    /// Idempotent in the way the configuration file's put is: a rule
    /// is whatever the last put said it was, and replacing one applies
    /// the change **without touching its outbox**, which is what makes
    /// turning a connector off and on again safe.
    ///
    /// # Errors
    ///
    /// [`InstallError::Storage`] when the store would not hold the
    /// rule or would not open its outbox. Nothing is installed in that
    /// case, and whatever was installed before is left running.
    pub async fn install(&self, rule: Rule) -> Result<(), InstallError> {
        // **Durable before anything hangs off it**, and before the old
        // one is taken away: a store that refuses the record leaves
        // the node exactly as it was.
        self.writer
            .apply_durable(vec![Record::RulePut {
                rule: rule.to_record(),
            }])
            .map_err(|error| InstallError::storage(&rule.id, error))?
            .await
            .map_err(|error| InstallError::storage(&rule.id, error))?;

        // The outbox is opened before the old installation is stopped,
        // so that a store that will not answer is a failed install
        // rather than a rule that has been switched off and not
        // switched back on.
        let outbox = match &rule.action {
            // An outbox hangs off `http_out` and off a bridge. An
            // ingest rule has nothing to queue, and a poller is given
            // nothing; see `http_poll`.
            Action::HttpOut(_) | Action::MqttBridge(_) => Some(Arc::new(
                Outbox::open(rule.id.clone(), self.store.as_ref())
                    .map_err(|error| InstallError::storage(&rule.id, error))?,
            )),
            Action::HttpIn(_) | Action::HttpPoll(_) => None,
        };

        // **Kept across the replacement.** A rule that is being
        // edited, disabled or enabled is the same rule, and an
        // operator watching its chart has not asked for it to start
        // over. A rule this node has not held before starts at zero.
        let stats = self.installed.read().get(&rule.id).map_or_else(
            || {
                // The same four numbers also reach Prometheus, and
                // the handles are taken here — once, when the rule is
                // installed — rather than looked up when something
                // happens. See `sdmqtt_server::metrics`.
                Arc::new(RuleStats::exporting(Some(
                    self.shared.metrics().rule(rule.id.as_str()),
                )))
            },
            |held| Arc::clone(&held.stats),
        );

        // Out of the map, and stopped, before the replacement starts:
        // two supervisors for one bridge would be two connections to
        // the remote under one client identifier, which the remote
        // resolves by closing one of them.
        self.uninstall(&rule.id);

        let installed = self.start(rule, outbox, stats);
        self.installed
            .write()
            .insert(installed.rule.id.clone(), installed);
        Ok(())
    }

    /// Turn a rule on or off without deleting it.
    ///
    /// **Not the same as deleting it.** Turning a connector off while
    /// an endpoint is down and on again afterwards is the ordinary
    /// thing to want, and deleting the rule would take its outbox with
    /// it.
    ///
    /// An `http_out` rule's pump keeps running while it is off, and
    /// that is deliberate: what `enabled` decides is whether new
    /// messages fire the rule, and what is already in the outbox was
    /// accepted while it was on.
    ///
    /// # Errors
    ///
    /// [`InstallError::NoSuchRule`] when this node holds no such rule,
    /// and [`InstallError::Storage`] when the change could not be
    /// stored — in which case nothing changes, because a switch that
    /// does not survive a restart is a switch that lies.
    pub async fn set_enabled(&self, id: &RuleId, enabled: bool) -> Result<Rule, InstallError> {
        let Some(mut rule) = self.get(id) else {
            return Err(InstallError::NoSuchRule {
                id: id.as_str().to_owned(),
            });
        };
        if rule.enabled == enabled {
            return Ok(rule);
        }
        rule.enabled = enabled;
        self.install(rule.clone()).await?;
        Ok(rule)
    }

    /// Delete a rule and everything running for it.
    ///
    /// # Errors
    ///
    /// [`InstallError::NoSuchRule`] when there is none,
    /// [`InstallError::OutboxNotEmpty`] when deleting it would throw
    /// away envelopes this broker accepted — see the module note — and
    /// [`InstallError::Storage`] when the store would not take the
    /// removal.
    pub async fn remove(&self, id: &RuleId) -> Result<(), InstallError> {
        if !self.installed.read().contains_key(id) {
            return Err(InstallError::NoSuchRule {
                id: id.as_str().to_owned(),
            });
        }
        // A maintained number in the store rather than a walk of the
        // outbox: see `Storage::outbox_depth`.
        let waiting = self
            .store
            .outbox_depth(id.as_str())
            .map_err(|error| InstallError::storage(id, error))?;
        if waiting > 0 {
            return Err(InstallError::OutboxNotEmpty {
                id: id.as_str().to_owned(),
                waiting,
            });
        }

        self.writer
            .apply_durable(vec![Record::RuleRemoved {
                id: id.as_str().to_owned(),
            }])
            .map_err(|error| InstallError::storage(id, error))?
            .await
            .map_err(|error| InstallError::storage(id, error))?;

        self.uninstall(id);
        Ok(())
    }

    /// What every rule has done since this process started, and the
    /// series of it, in identifier order.
    ///
    /// Copied out under the read lock and read outside it, which is
    /// the shape every resource on the admin listener has. Nothing
    /// here asks the store anything: the four counters are atomics and
    /// the depth is [`Outbox::waiting`], which is maintained.
    pub fn statistics(&self) -> Vec<(RuleId, Totals, Vec<Sample>)> {
        self.installed
            .read()
            .iter()
            .map(|(id, held)| {
                (
                    id.clone(),
                    held.stats.totals(held.waiting()),
                    held.stats.series(),
                )
            })
            .collect()
    }

    /// What one rule has done, and the series of it.
    pub fn statistics_of(&self, id: &RuleId) -> Option<(Totals, Vec<Sample>)> {
        self.installed
            .read()
            .get(id)
            .map(|held| (held.stats.totals(held.waiting()), held.stats.series()))
    }

    /// Close one interval of every rule's series.
    ///
    /// The handles and the depths are copied out under the read lock
    /// and the ring is written outside it, so the hook is never behind
    /// a sampler holding the map while it takes sixty locks.
    fn sample(&self) {
        let taking: Vec<(Arc<RuleStats>, u64)> = self
            .installed
            .read()
            .values()
            .map(|held| (Arc::clone(&held.stats), held.waiting()))
            .collect();
        for (stats, waiting) in taking {
            stats.sample(waiting);
        }
    }

    /// Stop everything every rule is running.
    ///
    /// The process stopping is the only thing that ends a pump, so
    /// this is what a graceful shutdown calls.
    pub async fn shutdown(&self) {
        if let Some(sampler) = self.sampler.lock().take() {
            sampler.abort();
        }
        let stopped: Vec<JoinHandle<()>> = {
            let mut installed = self.installed.write();
            installed
                .values_mut()
                .filter_map(|held| held.pump.take())
                .collect()
        };
        for pump in &stopped {
            pump.abort();
        }
        for pump in stopped {
            // An abort is reported as a cancellation, which is what
            // was asked for. Anything else is a pump that panicked,
            // which is worth a line.
            if let Err(error) = pump.await {
                if !error.is_cancelled() {
                    tracing::error!(%error, "a rule's task ended badly");
                }
            }
        }
    }

    /// Take `id` out of the map, stop its pump and withdraw its route.
    ///
    /// The entry is taken **under** the lock and stopped **outside**
    /// it, because aborting a task can wake a scheduler and nothing
    /// that could take time belongs inside a lock a hook reads.
    fn uninstall(&self, id: &RuleId) {
        let held = self.installed.write().remove(id);
        if let Some(mut held) = held {
            held.stop();
        }
        self.ingest.withdraw(id.as_str());
    }

    /// Start whatever this rule runs, and describe what was started.
    ///
    /// Takes no lock and is called with none held. Everything it does
    /// is a spawn or a map insert on the ingest routes.
    fn start(&self, rule: Rule, outbox: Option<Arc<Outbox>>, stats: Arc<RuleStats>) -> Installed {
        match &rule.action {
            Action::HttpOut(action) => {
                let outbox = outbox.expect("an http_out rule is opened with an outbox");
                // A poster is built from an action `Rule::new` and
                // `http_out::check` have both already accepted, so
                // this cannot fail; a rule that reached here with a
                // URL nobody will post to would be a refusal that had
                // been skipped, and the pump is where that would show
                // up rather than be hidden.
                let pump = match Poster::new(self.client.clone(), action) {
                    Ok(poster) => {
                        let delivery = Delivery::new(
                            Arc::clone(&outbox),
                            Arc::clone(&self.store),
                            Arc::clone(&self.writer),
                            poster,
                            action,
                            Arc::clone(&self.dead_letters),
                        )
                        .with_stats(Arc::clone(&stats));
                        Some(tokio::spawn(delivery.run()))
                    }
                    Err(error) => {
                        tracing::error!(
                            rule = rule.id.as_str(),
                            %error,
                            "this rule was accepted and its endpoint was then refused, which \
                             is a defect in this broker rather than in the rule; nothing \
                             will be posted for it"
                        );
                        None
                    }
                };
                Installed {
                    rule,
                    outbox: Some(outbox),
                    bridge_filters: Vec::new(),
                    wake: None,
                    pump,
                    stats,
                }
            }
            Action::HttpPoll(action) => {
                // **A disabled rule fetches nothing.** The same rule
                // an ingest rule's route follows: `enabled` is what an
                // operator reaches for to turn a connector off, and a
                // poller that kept fetching would be the one connector
                // that ignored the switch. Nothing is spawned rather
                // than a task that starts and stops, because there is
                // no state to keep warm.
                let pump = if rule.enabled {
                    match Poller::new(
                        rule.id.clone(),
                        self.client.clone(),
                        Arc::clone(&self.shared),
                        action,
                    ) {
                        Ok(poller) => Some(tokio::spawn(
                            poller
                                .with_stats(Arc::clone(&stats))
                                .run(self.membership.ownership()),
                        )),
                        Err(error) => {
                            tracing::error!(
                                rule = rule.id.as_str(),
                                %error,
                                "this rule was accepted and its endpoint was then refused, \
                                 which is a defect in this broker rather than in the rule; \
                                 nothing will be fetched for it"
                            );
                            None
                        }
                    }
                } else {
                    tracing::info!(
                        rule = rule.id.as_str(),
                        "this polling rule is switched off and fetches nothing"
                    );
                    None
                };
                Installed {
                    rule,
                    outbox: None,
                    bridge_filters: Vec::new(),
                    wake: None,
                    pump,
                    stats,
                }
            }
            Action::MqttBridge(action) => {
                let outbox = outbox.expect("a bridge is opened with an outbox");
                let wake = Arc::new(Notify::new());
                let bridge = Bridge {
                    rule: rule.id.clone(),
                    action: (**action).clone(),
                    rewrites: action.topics.iter().map(Rewrite::new).collect(),
                    marker: Marker::new(&rule.id, action.version),
                    outbox: Arc::clone(&outbox),
                    store: Arc::clone(&self.store),
                    writer: Arc::clone(&self.writer),
                    shared: Arc::clone(&self.shared),
                    wake: Arc::clone(&wake),
                    stats: Arc::clone(&stats),
                };
                let bridge_filters: Vec<String> = bridge
                    .rewrites
                    .iter()
                    .filter(|rewrite| rewrite.direction().outbound())
                    .map(|rewrite| rewrite.local_filter().to_owned())
                    .collect();
                // **A disabled rule connects to nothing**, as above.
                let pump = if rule.enabled {
                    match Supervisor::new(bridge, action) {
                        Ok(supervisor) => {
                            Some(tokio::spawn(supervisor.run(self.membership.ownership())))
                        }
                        Err(error) => {
                            tracing::error!(
                                rule = rule.id.as_str(),
                                %error,
                                "this bridge was accepted and its address was then refused, \
                                 which is a defect in this broker rather than in the rule; \
                                 it will not connect"
                            );
                            None
                        }
                    }
                } else {
                    tracing::info!(
                        rule = rule.id.as_str(),
                        "this bridge is switched off and connects to nothing"
                    );
                    None
                };
                Installed {
                    rule,
                    outbox: Some(outbox),
                    bridge_filters,
                    wake: Some(wake),
                    pump,
                    stats,
                }
            }
            Action::HttpIn(action) => {
                // **A disabled rule gets no route.** An ingest rule
                // left serving while it was switched off would be the
                // one connector that ignored the switch.
                if rule.enabled {
                    self.ingest
                        .register_with(&rule.id, action.clone(), Arc::clone(&stats));
                } else {
                    tracing::info!(
                        rule = rule.id.as_str(),
                        "this ingest rule is switched off and has no route"
                    );
                }
                Installed {
                    rule,
                    outbox: None,
                    bridge_filters: Vec::new(),
                    wake: None,
                    pump: None,
                    stats,
                }
            }
        }
    }
}

/// Close one interval of every rule's series, once per interval, for
/// as long as the registry exists.
///
/// **A tick rather than an event.** The ring has to advance whether or
/// not anything happened, because a quiet rule and a rule whose
/// connector died draw the same flat line only if the ring stands
/// still for the first of them; see [`crate::stats`].
///
/// A [`Weak`] reference, so that a registry nobody holds any more is
/// dropped rather than kept alive by its own sampler.
async fn sample_forever(registry: Weak<Registry>) {
    let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
    // The interval has already elapsed once by the time a tick is
    // missed, and a burst of catch-up samples would draw spikes that
    // never happened.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let Some(registry) = registry.upgrade() else {
            return;
        };
        registry.sample();
    }
}

/// The hook that turns a publication into envelopes.
///
/// Read [`broker_core::hooks`] before changing anything here. This runs
/// under the broker's lock: for as long as it is running, no other
/// connection in the process is served.
struct RuleHook {
    /// The rules to match against. Weak, for the reason
    /// [`Registry::hook`] gives.
    registry: Weak<Registry>,
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
    /// A read lock on the installed map, a clone of the payload —
    /// [`Bytes`], so a reference count — a clone of the topic, the
    /// encoded property block, which the message caches and the
    /// broker's own journal asks for anyway, and one wall-clock read.
    /// Nothing proportional to anything a client controls beyond the
    /// payload it already sent, and nothing that can block.
    fn message_received(&self, client: &ClientInfo<'_>, message: &mut Message) -> Disposition {
        let Some(registry) = self.registry.upgrade() else {
            // The registry has gone, so there are no rules to fire.
            return Disposition::Deliver;
        };
        let now = now_seconds();
        let mut records = Vec::new();
        let mut wake = Vec::new();
        {
            let installed = registry.installed.read();
            for held in installed.values() {
                if !held.fires(&message.topic, message.qos, client.client_id) {
                    continue;
                }
                // **The rule fired**, whether or not it has an outbox
                // to put anything in. Counted here rather than beside
                // the append, so that a connector which queues nothing
                // still reports that a message matched it.
                held.stats.fired();
                let Some(outbox) = &held.outbox else {
                    continue;
                };
                let body = BodyRecord {
                    topic: message.topic.to_string(),
                    payload: message.payload.clone(),
                    qos: stored_qos(message.qos),
                    retain: message.retain,
                    expires_at: message.expires_at,
                    properties: message.properties.encoded(),
                };
                // The body is filed under the broker's own identifier
                // for this publication, so a message that reaches a
                // durable subscriber *and* fires three rules is one
                // stored payload with four references, not four copies
                // of it.
                let (_, more) = outbox.append(
                    MessageId(*broker_core::session::MessageId::new().as_bytes()),
                    MessageId(*message.id.as_bytes()),
                    body,
                    client.client_id,
                    now,
                );
                records.extend(more);
                if let Some(notify) = &held.wake {
                    wake.push(Arc::clone(notify));
                }
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
