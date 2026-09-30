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
//! # What the pass costs, measured
//!
//! The hook matches a message against every installed rule, which is
//! work proportional to the rule count with the broker's one lock
//! held. That is inherent rather than an oversight — a rule is a
//! filter, and a publication has to be offered to each of them — but
//! it is a number worth having rather than a shrug. One publication
//! through `sdmqtt_server::publish`, release build, no socket:
//!
//! | rules | matching none | matching one |
//! |---:|---:|---:|
//! | 0 | 1.49 µs | 1.46 µs |
//! | 10 | 2.41 µs | 4.15 µs |
//! | 200 | 20.9 µs | 23.4 µs |
//! | 1,000 | 97.8 µs | 103 µs |
//!
//! Linear, at about a tenth of a microsecond per rule, on top of the
//! 1.5 µs a publication costs with no rules at all. Two hundred rules
//! is 20 µs under the lock, which is a fiftieth of what the read
//! resources cost before they were fixed and three orders of magnitude
//! under the measurements recorded in the design's open questions.
//!
//! **A thousand rules is 100 µs, and that is the number to watch.**
//! Nothing in section 9 imagines a node with a thousand rules, and if
//! one ever exists the answer is the one `broker-core` already uses
//! for subscriptions: put the rule filters in a matching trie instead
//! of a list. That is not done here because a trie for two hundred
//! filters is slower than a list of them, and because what is being
//! avoided is a structure nobody has needed yet.
//!
//! `what_rules_cost_the_broker`, in `sdmqtt_admin::rules`, is the
//! measurement. **Nothing else added here is under that lock**: the
//! statistics, the dead letters and every rule resource read their
//! own locks and never the broker's, and the outbox depth is a
//! maintained number rather than a store read per rule.
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
//! # An ingest rule's socket is bound with the rule, not before it
//!
//! Every other kind of rule runs on something this crate can start: a
//! task, a client, a timer. An `http_in` rule runs on a **listening
//! socket**, which this crate cannot bind — the listener lives in the
//! admin crate, which depends on this one rather than the other way
//! round — and which for a while was bound at startup and only if this
//! node held an ingest rule at that moment.
//!
//! That made the first ingest rule created over the API a rule that
//! was stored, installed, answered `200` and reachable by nobody until
//! the process was restarted. It is worse than not supporting it: a
//! route in a map with no socket in front of it is the one failure
//! that looks exactly like success.
//!
//! So the socket is the registry's to ask for, through
//! [`IngestListening`], and it is asked **before** the rule is
//! installed and again after anything that could have taken the last
//! route away. What comes out of that is the only property worth
//! stating: **an enabled ingest rule this node holds is one that can
//! be reached**, whether it arrived in a file or in a request.
//!
//! [`http_out`]: crate::http_out
//! [`Record::RulePut`]: storage::record::Record::RulePut

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};
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
use crate::dlq::{DeadLetter, DeadLetterLog};
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
    /// There is already a rule by that name on this node.
    ///
    /// Only [`Registry::create`] raises it, and only under
    /// [`Registry::changing`]: that is what makes it a fact rather
    /// than something that was true a moment ago. A create that
    /// replaced would make two operators writing one identifier a
    /// race whose loser never finds out.
    #[error(
        "this node already holds a rule called {id:?}; change it with a PUT to \
         /api/v1/rules/{id}"
    )]
    AlreadyExists {
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
    /// This rule and another this node holds loop through one remote.
    ///
    /// The refusal `crate::bridge::topics::check` makes of a single
    /// bridge, across a pair of them: see
    /// [`crate::bridge::topics::check_across`]. Raised **before**
    /// anything is stored, so the node is exactly as it was.
    #[error("{why}")]
    Loops {
        /// The rule being installed.
        id: String,
        /// The one it loops with.
        other: String,
        /// The whole of the refusal, which names both entries and a
        /// witness on each side.
        why: String,
    },
    /// The rule cannot work on **this** node, for a reason that is
    /// about the node rather than about the rule.
    ///
    /// A poller whose body limit is larger than this broker's maximum
    /// packet size is the one case: the same rule on a node with a
    /// larger limit is fine, so it is not something `crate::spec` can
    /// refuse when the rule is read.
    #[error("{why}")]
    Unusable {
        /// Which rule.
        id: String,
        /// Which of its fields, for a form to mark.
        field: &'static str,
        /// The whole of the refusal.
        why: String,
    },
    /// The socket its route would be served on is not there.
    ///
    /// An ingest rule with no listener in front of it is a rule that
    /// answers nothing, so this is a refusal rather than a warning.
    /// Raised **before** anything is stored or installed, so the
    /// ordinary reading of it is that this node is exactly as it was;
    /// see [`Registry::install`] for the one ordering that is not.
    #[error("the rule {id:?} accepts posts and this node has nowhere to serve it: {why}")]
    Listener {
        /// Which rule.
        id: String,
        /// What the listener said.
        why: String,
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

    /// A listener failure about `id`.
    fn listener(id: &RuleId, why: String) -> Self {
        Self::Listener {
            id: id.as_str().to_owned(),
            why,
        }
    }
}

/// What an [`IngestListening`] method gives back.
///
/// Hand written rather than a dependency: one boxed future in one
/// trait is not worth a crate, and this crate's budget is counted.
type Asking<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Where this node's ingest routes are actually served.
///
/// # Why the registry has a say in a socket at all
///
/// Read the module note first. The short version is that an `http_in`
/// rule runs on a listening socket the way an `http_out` rule runs on
/// a task, and a rule installed without the thing it runs on is a rule
/// that reports itself as working and is not.
///
/// This crate cannot bind one — the listener is `sdmqtt_admin`'s and
/// that crate depends on this one — so the binary implements this and
/// hands it over with [`Registry::serve_ingest_on`]. What stays here
/// is the part that has to be a fact rather than a habit: the call is
/// in [`Registry::install`], which is the one door every rule comes
/// through, and not in whichever caller happened to think of it.
///
/// # One method, because there is one question
///
/// [`IngestListening::reconcile`] is told what is wanted and leaves
/// the socket that way, whatever it was doing before. Separate `bind`
/// and `close` calls would be two states for a caller to keep in step
/// and would need it to know which one it was already in.
///
/// It is asynchronous because binding is, and the answer has to be
/// known **before** the request that caused it is answered: a `200`
/// sent while a socket was still being bound somewhere else is the
/// same lie in a shorter window.
pub trait IngestListening: Send + Sync {
    /// Serve the ingest routes if `wanted`, and stop serving them if
    /// not.
    ///
    /// Idempotent in both directions, and told what is wanted rather
    /// than asked to work it out: the registry knows a route is about
    /// to exist before the route does.
    ///
    /// # Errors
    ///
    /// The address could not be bound, or this node is stopping and
    /// will bind nothing more. A string, because the type of the
    /// failure belongs to whoever does the binding.
    fn reconcile(&self, wanted: bool) -> Asking<'_, Result<(), String>>;
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
    /// The dead letters this rule has produced.
    ///
    /// Kept across an update for the reason the statistics are: a rule
    /// being edited is the same rule, and a letter it produced ten
    /// minutes ago is still the thing an operator came to look at.
    dlq: Arc<DeadLetterLog>,
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

impl Drop for Installed {
    /// **Stop the pump, whether or not anybody asked.**
    ///
    /// Dropping a [`JoinHandle`] detaches the task rather than
    /// aborting it, and this crate had no [`Drop`] anywhere, so an
    /// installation that reached a drop with its pump still in it left
    /// a task running that nothing held and nothing could stop. For a
    /// bridge that is a second connection to the remote under one
    /// client identifier, each closing the other, for the life of the
    /// process.
    ///
    /// The drop that mattered is the map insert in
    /// [`Registry::install_locked`]: it hands back whatever it
    /// replaced, and two writers racing meant one of them replacing a
    /// handle the other had just spawned. The lock above makes that
    /// race impossible; this makes it harmless, which is the half that
    /// does not depend on every future caller remembering the lock.
    fn drop(&mut self) {
        self.stop();
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
    /// The socket those routes are served on, once something is
    /// serving them.
    ///
    /// Set once, by the binary, and **after the MQTT listener has
    /// bound**: a post is a publication, and section 9.2's ordering
    /// says the subscribers it reaches must be attachable first. That
    /// ordering is why this is handed over afterwards rather than
    /// given to [`Registry::new`].
    ///
    /// Never set on a node whose configuration serves the routes on
    /// the admin listener — that listener is bound whatever this node
    /// holds, so there is nothing to reconcile — nor in the tests of
    /// everything below the shell, where an unset socket means the
    /// registry asks nobody for one.
    ingest_socket: OnceLock<Arc<dyn IngestListening>>,
    /// What is installed, by identifier.
    installed: RwLock<BTreeMap<RuleId, Installed>>,
    /// Held for the whole of any change to what this node is running.
    ///
    /// **Not the same lock as [`Registry::installed`]**, and it cannot
    /// be: installing makes a durable write and opens an outbox, and
    /// the rule this module rests on is that nothing holds the map's
    /// write lock across an `await` — a hook reads that lock under the
    /// broker's own, so a disk flush inside it would stop every
    /// publish in the process. This one is an asynchronous lock
    /// precisely so that it *can* be held across those awaits, and
    /// nothing reading a rule ever takes it.
    ///
    /// What it buys is that a change is one change. Installing
    /// uninstalls, spawns and inserts, and two writers interleaving
    /// those meant both spawned and the second insert dropped the
    /// first handle — which detaches rather than aborts, leaving a
    /// supervisor running that nothing could reach. It is also what
    /// makes [`Registry::create`]'s refusal of a name something
    /// already holds mean anything: a check outside this lock is a
    /// check whose answer can be false by the time it is used, which
    /// is the race that endpoint's comment says it refuses replacement
    /// to avoid.
    changing: tokio::sync::Mutex<()>,
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
        let registry = Arc::new(Self {
            shared,
            store,
            writer,
            membership,
            client: client(),
            ingest: Arc::new(Ingest::new()),
            ingest_socket: OnceLock::new(),
            installed: RwLock::new(BTreeMap::new()),
            changing: tokio::sync::Mutex::new(()),
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

    /// Serve this node's ingest routes on `socket` from here on.
    ///
    /// Handed over once, by whoever bound the listeners, and after
    /// the one the ingest routes publish through; see
    /// [`Registry::ingest_socket`]. Until it is, a rule with a route
    /// is installed without one being asked for, which is what
    /// startup wants: the rules a configuration names are installed
    /// before anything binds at all, and the binary binds the socket
    /// for them the moment it is allowed to.
    ///
    /// Twice is a defect in the caller rather than a state to
    /// support, and is said rather than silently ignored.
    pub fn serve_ingest_on(&self, socket: Arc<dyn IngestListening>) {
        if self.ingest_socket.set(socket).is_err() {
            tracing::error!(
                "this node was given a second ingest socket; the first is still the one \
                 being served and the second is bound to nothing"
            );
        }
    }

    /// Leave the ingest socket bound if `wanted` and closed if not.
    ///
    /// `Ok(())` when nothing has handed one over, which is a node
    /// that serves its ingest routes somewhere that is bound
    /// regardless — the admin listener — or one that has not finished
    /// starting.
    async fn reconcile_ingest(&self, id: &RuleId, wanted: bool) -> Result<(), InstallError> {
        let Some(socket) = self.ingest_socket.get() else {
            return Ok(());
        };
        socket
            .reconcile(wanted)
            .await
            .map_err(|why| InstallError::listener(id, why))
    }

    /// Whether installing `rule` gives this node a route to serve.
    ///
    /// A **disabled** ingest rule needs no socket, for the same
    /// reason it gets no route: `enabled` is the switch an operator
    /// reaches for, and a listener that stayed up for a rule that was
    /// switched off would be the one part of a connector that ignored
    /// it.
    fn wants_a_route(rule: &Rule) -> bool {
        rule.enabled && matches!(rule.action, Action::HttpIn(_))
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
    ///
    /// [`InstallError::Listener`] when an ingest rule's socket could
    /// not be bound. Asked for **first**, before the store is even
    /// written to, so that the usual reading of this error is that
    /// nothing about this node changed. The exception is the second
    /// call below: a rule that installed perfectly and then lost a
    /// race with a delete that closed the socket is reported rather
    /// than answered `Ok`, because what the caller is owed is whether
    /// its rule can be reached and not whether a map was written.
    pub async fn install(&self, rule: Rule) -> Result<(), InstallError> {
        let _changing = self.changing.lock().await;
        self.install_locked(rule).await
    }

    /// Store `rule` and start it, refusing a name this node already
    /// holds.
    ///
    /// What the create endpoint needs, and the check has to be **here**
    /// rather than in the handler: a `get` followed by an `install` is
    /// two operations, and two operators writing the same identifier at
    /// the same time both pass the first and both run the second, so
    /// the loser is told it created a rule that another request
    /// replaced. Under [`Registry::changing`] the look and the install
    /// are one change and the refusal means what it says.
    ///
    /// # Errors
    ///
    /// [`InstallError::AlreadyExists`] when this node holds a rule of
    /// that name, and whatever [`Registry::install`] can say otherwise.
    pub async fn create(&self, rule: Rule) -> Result<(), InstallError> {
        let _changing = self.changing.lock().await;
        if self.installed.read().contains_key(&rule.id) {
            return Err(InstallError::AlreadyExists {
                id: rule.id.as_str().to_owned(),
            });
        }
        self.install_locked(rule).await
    }

    /// The whole of an install, with [`Registry::changing`] already
    /// held.
    async fn install_locked(&self, rule: Rule) -> Result<(), InstallError> {
        let id = rule.id.clone();
        // **Before anything changes**, like the route below and for
        // the same reason: a pair of bridges that would loop is a
        // refusal with this node exactly as it was.
        self.check_for_a_loop(&rule)?;
        self.check_it_fits(&rule)?;
        // **Before anything changes.** An address something else is
        // already on is then a refusal with the node exactly as it
        // was, rather than a stored rule nothing can reach.
        if Self::wants_a_route(&rule) {
            self.reconcile_ingest(&id, true).await?;
        }
        let installed = self.install_now(rule).await;
        // And again afterwards, because installing a rule can take a
        // route away as well as add one: the rule this replaced may
        // have been the last one with a route, and an install that
        // failed after the bind above leaves a socket nothing needs.
        // What is wanted is read from the routes rather than from the
        // rule, so that the socket follows what is actually served.
        let settled = self.reconcile_ingest(&id, !self.ingest.is_empty()).await;
        installed.and(settled)
    }

    /// Store `rule`, start it, and put it in the map.
    ///
    /// Split from [`Registry::install`] so that the socket is settled
    /// on every path out of this, including the ones that fail.
    async fn install_now(&self, rule: Rule) -> Result<(), InstallError> {
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
        let dlq = self.installed.read().get(&rule.id).map_or_else(
            || Arc::new(DeadLetterLog::new()),
            |held| Arc::clone(&held.dlq),
        );
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
        // resolves by closing one of them. This is also what withdraws
        // an ingest route the replacement may not want back.
        self.uninstall(&rule.id);

        // Whatever the insert hands back is dropped here, and a
        // dropped [`Installed`] aborts its pump. Under
        // [`Registry::changing`] there is never anything to hand back;
        // see the [`Drop`] implementation for why it is written down
        // anyway.
        let installed = self.start(rule, outbox, stats, dlq);
        drop(
            self.installed
                .write()
                .insert(installed.rule.id.clone(), installed),
        );
        Ok(())
    }

    /// Refuse a bridge that loops through a remote with one this node
    /// already holds.
    ///
    /// `crate::bridge::spec` refuses a bridge that loops through its
    /// own entries when the rule is read, which is where a single
    /// rule's loop belongs. A pair of rules cannot be seen from
    /// there — neither rule is wrong — so it is seen here, which is
    /// the one place that knows what else this node is running.
    ///
    /// **Whether or not either rule is enabled.** A disabled bridge
    /// connects to nothing, and an operator who stored the pair and
    /// switched one on would find out then; the moment to say so is
    /// when it is written.
    fn check_for_a_loop(&self, rule: &Rule) -> Result<(), InstallError> {
        let Action::MqttBridge(action) = &rule.action else {
            return Ok(());
        };
        let installed = self.installed.read();
        for (id, held) in installed.iter() {
            // The rule this one replaces is not another rule.
            if id == &rule.id {
                continue;
            }
            let Action::MqttBridge(other) = &held.rule.action else {
                continue;
            };
            crate::bridge::topics::check_across([
                (rule.id.as_str(), action.as_ref()),
                (id.as_str(), other.as_ref()),
            ])
            .map_err(|error| InstallError::Loops {
                id: rule.id.as_str().to_owned(),
                other: id.as_str().to_owned(),
                why: error.to_string(),
            })?;
        }
        Ok(())
    }

    /// Refuse a poller whose body limit is larger than this broker
    /// could publish.
    ///
    /// Checked here rather than in `crate::spec`, because the number
    /// it is checked against is the **node's**: the same rule on a
    /// node with a larger `broker.max_packet_size` is fine, and a rule
    /// is read in places that have no broker at all.
    fn check_it_fits(&self, rule: &Rule) -> Result<(), InstallError> {
        let Action::HttpPoll(action) = &rule.action else {
            return Ok(());
        };
        let max_packet_size = self
            .shared
            .with_broker(|broker| broker.config().limits.max_packet_size);
        crate::http_poll::check_body_limit(action, max_packet_size).map_err(|error| {
            InstallError::Unusable {
                id: rule.id.as_str().to_owned(),
                field: error.field(),
                why: error.to_string(),
            }
        })
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
        // The read and the install are one change: a switch that read
        // a rule, was overtaken by an edit, and then wrote back what it
        // had read would undo the edit and report success.
        let _changing = self.changing.lock().await;
        let Some(mut rule) = self.get(id) else {
            return Err(InstallError::NoSuchRule {
                id: id.as_str().to_owned(),
            });
        };
        if rule.enabled == enabled {
            return Ok(rule);
        }
        rule.enabled = enabled;
        self.install_locked(rule.clone()).await?;
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
    ///
    /// [`InstallError::Listener`] is possible and means the rule is
    /// gone and its socket did not close, which is a port serving
    /// nothing rather than anything a caller has lost.
    pub async fn remove(&self, id: &RuleId) -> Result<(), InstallError> {
        // For the reason the others take it, and one of its own: the
        // depth is read, the record is written and the rule is taken
        // out of the map, and an install landing between the first two
        // would delete a rule the node had just been given.
        let _changing = self.changing.lock().await;
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
        // Its route went with it, and it may have been the last one:
        // a node that holds no ingest rule holds no ingest socket
        // either, which is the same answer it gives before the first
        // one is created.
        self.reconcile_ingest(id, !self.ingest.is_empty()).await
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

    /// The dead letters one rule has kept, oldest first, and how many
    /// it has produced altogether.
    ///
    /// `None` when there is no such rule, which a caller has to be
    /// able to tell from a rule that has dead-lettered nothing.
    pub fn dead_letters_of(&self, id: &RuleId) -> Option<(u64, Vec<DeadLetter>)> {
        self.installed
            .read()
            .get(id)
            .map(|held| (held.dlq.produced(), held.dlq.recent()))
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
    fn start(
        &self,
        rule: Rule,
        outbox: Option<Arc<Outbox>>,
        stats: Arc<RuleStats>,
        dlq: Arc<DeadLetterLog>,
    ) -> Installed {
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
                            Arc::new(BrokerDeadLetters {
                                shared: Arc::clone(&self.shared),
                                kept: Arc::clone(&dlq),
                            }),
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
                    dlq,
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
                    dlq,
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
                    dlq,
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
                    dlq,
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

/// Publishes a dead letter back into the broker, and keeps it.
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
///
/// **And keeps it**, which is section 11.1's half of the same event:
/// the letter goes on the topic for anything subscribed, and a copy
/// goes in the rule's bounded ring for the page nobody had open when
/// it happened. See [`crate::dlq`] for why that is a ring here rather
/// than a subscription there.
///
/// One of these per rule rather than one for the node, so that the
/// ring it fills is the rule's own and nothing has to work out whose
/// letter this was from the topic it is going to.
struct BrokerDeadLetters {
    shared: Arc<Shared>,
    kept: Arc<DeadLetterLog>,
}

impl DeadLetters for BrokerDeadLetters {
    fn dead_letter(&self, topic: &str, payload: Vec<u8>) {
        let now = now_seconds();
        // Kept before it is published, so that a page refreshed the
        // instant a subscriber saw the letter finds it here too.
        self.kept.record(topic, now, &payload);
        let message = Message::new(topic, Bytes::from(payload), QoS::AtMostOnce, false);
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
    use std::time::Duration;

    use broker_core::broker::{Broker, BrokerConfig};
    use mqtt_codec::Version;
    use storage::{MemoryStorage, WriterConfig};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    use crate::rule::{BridgeTopic, Direction, HttpOut, MqttBridge};
    use crate::singleton::NodeId;

    use super::*;

    /// Sets a flag when it is dropped, which for a spawned task means
    /// when the task ends or is aborted.
    struct Ended(Arc<AtomicBool>);

    impl Drop for Ended {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// A rule with an action that starts nothing, for the one test
    /// that supplies its own pump.
    fn a_rule() -> Rule {
        Rule {
            id: RuleId::new("held").expect("a valid identifier"),
            name: "Held".to_owned(),
            enabled: true,
            trigger: Some(crate::rule::Trigger::new("sensor/#").expect("a valid filter")),
            action: Action::HttpOut(HttpOut {
                url: "http://127.0.0.1:1/hook".to_owned(),
                ..HttpOut::default()
            }),
        }
    }

    /// Dropping an installation stops what it was running.
    ///
    /// The whole of the reason this crate has a [`Drop`] at all.
    /// Dropping a task handle **detaches** the task, so without this
    /// an installation replaced in the map leaves a pump running that
    /// nothing holds and nothing can stop; for a bridge that is a
    /// second connection to the remote under one client identifier
    /// until the process restarts.
    ///
    /// Asserted through the task itself rather than through the
    /// handle: a handle says whether it was aborted, and what is
    /// wanted is whether the *task* stopped.
    #[tokio::test]
    async fn dropping_an_installation_stops_its_pump() {
        let stopped = Arc::new(AtomicBool::new(false));
        let flag = Ended(Arc::clone(&stopped));
        let pump = tokio::spawn(async move {
            let _flag = flag;
            std::future::pending::<()>().await;
        });
        // It is running before anything is dropped, so that the
        // assertion below is about the drop rather than about a task
        // that never started.
        tokio::task::yield_now().await;
        assert!(
            !stopped.load(Ordering::SeqCst),
            "the pump ended on its own, so this test would pass without a drop"
        );

        let installed = Installed {
            rule: a_rule(),
            outbox: None,
            bridge_filters: Vec::new(),
            wake: None,
            pump: Some(pump),
            dlq: Arc::new(DeadLetterLog::new()),
            stats: Arc::new(RuleStats::new()),
        };
        drop(installed);

        // An abort takes effect at the runtime's next pass over the
        // task, not at the call.
        for _ in 0..1_000 {
            if stopped.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            stopped.load(Ordering::SeqCst),
            "a dropped installation left its pump running, detached, with nothing \
             holding it"
        );
    }

    /// A registry over a memory store, with nothing installed.
    fn registry() -> (Arc<Registry>, Arc<MemoryStorage>) {
        let store = Arc::new(MemoryStorage::new());
        let writer = Arc::new(
            Writer::spawn(
                Arc::clone(&store) as Arc<dyn Storage>,
                WriterConfig::default(),
            )
            .expect("the writer starts"),
        );
        let shared = Arc::new(Shared::with_store(
            Broker::new(BrokerConfig::default()),
            Arc::clone(&writer),
        ));
        let registry = Registry::new(
            shared,
            Arc::clone(&store) as Arc<dyn Storage>,
            writer,
            Arc::new(Membership::alone(NodeId::mint())),
        );
        (registry, store)
    }

    /// A bridge to `address` that carries one filter outwards.
    fn a_bridge(address: String) -> Rule {
        Rule {
            id: RuleId::new("hub").expect("a valid identifier"),
            name: "To the hub".to_owned(),
            enabled: true,
            trigger: None,
            action: Action::MqttBridge(Box::new(MqttBridge {
                address,
                client_id: "hub".to_owned(),
                version: Version::V5,
                topics: vec![BridgeTopic {
                    filter: "sensor/#".to_owned(),
                    direction: Direction::Out,
                    remote_prefix: "from-here/".to_owned(),
                    ..BridgeTopic::default()
                }],
                ..MqttBridge::default()
            })),
        }
    }

    /// Writing the same bridge rule several times at once leaves one
    /// connection to the remote, not several.
    ///
    /// The symptom the lock and the drop exist for, and the only one
    /// an operator ever sees: a bridge is a client identifier, and two
    /// connections under one identifier is each closing the other for
    /// ever.
    ///
    /// **Open connections, not accepted ones.** Eight installs start
    /// eight supervisors and stop seven, so eight connections reaching
    /// this listener is correct; what is not correct is two of them
    /// still being there at the end. The remote answers nothing, so a
    /// supervisor that is still running is still connected — its
    /// connack timeout is far longer than this test — and one that was
    /// aborted has had its socket closed, which the reader below sees.
    ///
    /// Multi-threaded on purpose. The window is between the spawn and
    /// the insert and holds no await, so on one thread it cannot be
    /// split at all and this would be a test about nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn writing_one_rule_from_several_places_at_once_leaves_one_pump() {
        let remote = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        // Plain text, which is refused to anywhere but this
        // machine and is what a loopback listener is.
        let address = format!("mqtt://{}", remote.local_addr().expect("the port"));
        let (registry, _store) = registry();

        let open = Arc::new(AtomicI64::new(0));
        let accepting = {
            let open = Arc::clone(&open);
            tokio::spawn(async move {
                while let Ok((mut stream, _)) = remote.accept().await {
                    open.fetch_add(1, Ordering::SeqCst);
                    let open = Arc::clone(&open);
                    tokio::spawn(async move {
                        // Read and answer nothing. What this is for is
                        // noticing the close, which is what says a
                        // supervisor is gone.
                        let mut buffer = [0u8; 1024];
                        loop {
                            match stream.read(&mut buffer).await {
                                Ok(0) | Err(_) => break,
                                Ok(_) => {}
                            }
                        }
                        open.fetch_sub(1, Ordering::SeqCst);
                    });
                }
            })
        };

        let rule = a_bridge(address);
        let writing: Vec<JoinHandle<Result<(), InstallError>>> = (0..8)
            .map(|_| {
                let registry = Arc::clone(&registry);
                let rule = rule.clone();
                tokio::spawn(async move { registry.install(rule).await })
            })
            .collect();
        for written in writing {
            written
                .await
                .expect("the install did not panic")
                .expect("the install succeeds");
        }

        // The surviving supervisor has to reach the socket and the
        // aborted ones' closes have to arrive. Waited for rather than
        // slept through, so that a slow machine does not read as a
        // failure.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline && open.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // And then long enough that a second connection, if there were
        // one, would have arrived.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            open.load(Ordering::SeqCst),
            1,
            "this node holds one bridge rule and had more than one connection to its \
             remote open, which is two supervisors under one client identifier"
        );

        registry.shutdown().await;
        accepting.abort();
    }
}
