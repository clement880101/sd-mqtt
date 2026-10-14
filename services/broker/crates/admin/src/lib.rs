//! The admin listener: health, metrics, the read resources, publishing
//! and the events stream.
//!
//! An [`axum`] router over the same [`Shared`] the MQTT listener uses.
//! It reads the broker under the existing lock and writes through the
//! same path a connection does, so there is no second set of rules
//! about retention, expiry, matching or queueing.
//!
//! # What protects it
//!
//! Where it listens. Section 11.1: until milestone 7 there are no API
//! keys, so the listener binds loopback by default and a non-loopback
//! address with no token is refused at startup. See
//! [`AdminConfig::check`].
//!
//! # HTTP/1.1 only, deliberately
//!
//! Section 11.1: browsers negotiate HTTP/2 only over TLS, TLS is
//! milestone 7, and the crates bought now would buy nothing until
//! then. The consequence is that an events stream costs one of the six
//! connections a browser allows per origin, which is why the interface
//! opens exactly one stream for the whole application.

use std::net::SocketAddr;
use std::sync::Arc;

use serde::Serialize;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use sdmqtt_server::shared::Shared;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

pub mod acl;
pub mod assets;
pub mod config;
pub mod error;
pub mod events;
pub mod health;
pub mod host;
pub mod resources;
pub mod rules;
pub mod taps;
pub mod users;

#[cfg(test)]
mod testing;

pub use assets::{INTERFACE_BUILT, NOTICES, NOTICES_PATH};
pub use config::{AdminConfig, AdminStartError, Token};
pub use error::{ApiError, ErrorCode};
pub use events::{EventBody, EventId, Events};
pub use health::Readiness;
pub use resources::{ListenerProtocol, ListenerView};
pub use taps::{Reservation, Tap, TapLimits, Taps, IDLE_TIMEOUT, MAX_TAPS, TAP_CAPACITY};

// The seam's vocabulary, re-exported at the crate root beside the
// admin listener it was factored out of: a caller serving a route set
// of its own builds one of these, and should not have to name the
// same version of axum in its own manifest to do it.
pub use axum::Router;

/// How often the events stream's numbers are sampled.
///
/// A second, which is the resolution the Overview page's rate charts
/// draw at. Faster would be more numbers than a chart can show, and
/// slower would make a connect take longer to appear than a person
/// waits before reloading.
pub const DEFAULT_EVENTS_INTERVAL: Duration = Duration::from_secs(1);

/// How long shutdown waits for open connections before giving up.
///
/// Section 11.1 asks for the whole admin server to be bounded by a
/// timeout **as a backstop**. The mechanism that actually ends a
/// stream is the signal every one of them selects on; this is what
/// stops a bug in that mechanism from being an unkillable process,
/// and it is generous because reaching it at all is a defect rather
/// than a slow client.
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// The listeners this node has bound, as they are now.
///
/// Shared and mutable rather than a list built at startup, because
/// one of them is not a fact about startup: section 9.2's ingest
/// listener is bound while an ingest rule needs it and closed when
/// the last one is deleted or switched off, both of which can happen
/// over the API while this process runs. A `/api/v1/listeners` fixed
/// when the binary finished starting would answer with a port that
/// had since closed, or leave out one that had since opened, and an
/// inventory that is wrong about what is listening is worth less than
/// no inventory at all.
///
/// The lock is held to clone the rows and for nothing else; see
/// [`resources`].
pub type Listeners = Arc<RwLock<Vec<ListenerView>>>;

/// A [`Listeners`] holding `views`.
///
/// A constructor rather than a type everybody spells out, so that
/// nothing outside this crate has to name the lock or depend on the
/// crate it comes from.
pub fn listener_list(views: Vec<ListenerView>) -> Listeners {
    Arc::new(RwLock::new(views))
}

/// This node's view of the cluster, for whoever is holding one.
///
/// A trait rather than the mesh itself. What this API needs of section
/// 8 is four strings per peer; taking `cluster::mesh::Mesh` would put
/// the whole of a transport, a handshake and a failure detector behind
/// an HTTP handler, and would make this crate unbuildable without it.
pub trait Cluster: Send + Sync + 'static {
    /// The peers this node has met, in a stable order.
    ///
    /// Peers only: this node is not one of them, because whether a row
    /// is this node is what [`crate::resources`] reports separately and
    /// a view that included itself would have two answers to it.
    fn peers(&self) -> Vec<ClusterPeer>;

    /// How many nodes this cluster is meant to have, if an operator
    /// said. Section 8.5.
    ///
    /// `None` is an operator who did not say, and then
    /// [`crate::health`] falls back to the nodes this one has met —
    /// which is the count it has always served and is the one with the
    /// blind spot that made this method necessary: three nodes that
    /// never heard of a fourth cannot tell themselves apart from four
    /// with one down.
    ///
    /// It comes through this trait, rather than from a configuration
    /// the handler could read for itself, so that the number an
    /// operator reads off the probe is the same number that decides
    /// whether a connect is refused. Two readings of "whole" in one
    /// process is a probe that says `complete` while clients are being
    /// turned away.
    fn expected(&self) -> Option<usize>;

    /// Take a node out of membership at once, section 8.6's operator
    /// override of the grace.
    ///
    /// `id` is the canonical text of the identity, as `peers` reports
    /// it. The three answers are three different HTTP replies, and the
    /// trait says which rather than returning a bool and leaving the
    /// handler to guess why.
    fn forget(&self, id: &str) -> Forgotten;

    /// Section 8.6's copies, as this node sees them.
    ///
    /// `None` on a node that does not replicate — one without a
    /// store, or a cluster of one with nothing to copy to — and the
    /// page then shows no replication block rather than a row of
    /// zeros that would read as "healthy and idle".
    fn replication(&self) -> Option<Replication>;
}

/// What this node knows about its copies. Section 8.6.
///
/// **Every number is this node's own**: sessions it owns and streams,
/// copies it holds for others, and how far its followers are behind
/// *it*. There is no cluster-wide figure here for the reason the
/// membership table has none: nothing is replicated about
/// replication, and two nodes' pages disagreeing is each of them
/// telling the truth about its own queues.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Replication {
    /// `async` or `sync`: when a durable publish is acknowledged.
    pub mode: &'static str,
    /// Sessions this node owns and streams to at least one follower.
    pub streaming: u64,
    /// Sessions this node holds a copy of for another node.
    pub holding: u64,
    /// Changes the furthest-behind copy of any session this node
    /// owns has not applied. The loss window in asynchronous mode.
    pub lag: u64,
    /// Sessions this node owns whose replica set is short of a copy,
    /// as of the last repair sweep.
    pub repairs_pending: u64,
    /// Sessions whose queue this node has re-streamed to a follower
    /// that held none, ever.
    pub repairs_done: u64,
    /// One row per peer this node shares a copy with, by identity.
    pub peers: Vec<PeerReplication>,
}

/// One peer's share of this node's copies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PeerReplication {
    /// The peer's identity, in the canonical text.
    pub id: String,
    /// Sessions this node owns that the peer has confirmed a copy of.
    pub confirmed: u64,
    /// The furthest any of those copies is behind this node.
    pub behind: u64,
    /// Sessions the peer owns that this node holds a copy of.
    pub held: u64,
}

/// What forgetting a node came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Forgotten {
    /// It was in the table and is not now. Repair starts at once.
    Gone,
    /// This node has never met it, so there is nothing to forget.
    NotMet,
    /// The identifier is this node's own.
    ///
    /// Refused: a node cannot take itself out of its own cluster this
    /// way, and an operator who meant to decommission it stops the
    /// process. Not silently a no-op, because the request said
    /// something impossible and should be told so.
    Myself,
}

/// One peer, as the API reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterPeer {
    /// Its identity, in the canonical text.
    pub id: String,
    /// What an operator calls it. Never compared.
    pub name: String,
    /// The address it says peers should dial.
    pub address: String,
    /// `up`, `suspect` or `down`.
    pub state: &'static str,
}

/// Everything a handler can reach.
///
/// Cloned per request by axum, so everything in it is behind an
/// [`Arc`].
#[derive(Clone)]
pub struct AdminState {
    /// The broker, behind the one lock the whole process shares.
    pub shared: Arc<Shared>,
    /// What startup has managed so far. See [`Readiness`].
    pub readiness: Arc<Readiness>,
    /// What this node is.
    ///
    /// **Section 8.1's UUID**, minted on first start and persisted,
    /// which is the identity singleton ownership and every conflict
    /// tie-break order by. It used to be the word `local`, hardcoded,
    /// which was a second answer to "which node" in a binary that has
    /// a real one.
    ///
    /// Still a string rather than a `NodeId`: what this crate does
    /// with it is print it, and taking `cluster`'s type would put the
    /// whole of section 8 behind an HTTP handler that needs sixteen
    /// bytes of text.
    pub node_id: Arc<str>,
    /// What an operator calls this node.
    ///
    /// Never compared; see `cluster::identity`. It is here because a
    /// listing of nodes with only identifiers in it cannot be matched
    /// against the configuration an operator wrote.
    pub node_name: Arc<str>,
    /// The address this node tells peers to dial, when it clusters.
    ///
    /// Empty on a node that does not. **Not the bind address**:
    /// section 8.1, and `cluster::mesh` for why.
    pub node_address: Arc<str>,
    /// This node's view of the cluster, when it has one.
    ///
    /// `None` on a node that does not cluster, which is a different
    /// thing from a cluster of one that has lost every peer, and the
    /// two answer differently.
    pub cluster: Option<Arc<dyn Cluster>>,
    /// What this node has bound.
    ///
    /// Given rather than discovered: a bound socket does not know
    /// what it is for, and the broker does not know a listener
    /// exists. What was bound is what the API reports.
    ///
    /// Two things add a row to it themselves. This listener does,
    /// because a caller cannot supply a port that does not exist
    /// until the socket is bound and that with a configured port of
    /// zero is the operating system's to choose. And section 9.2's
    /// ingest listener does, because **it is not a fact about
    /// startup**: it is bound while this node holds an ingest rule
    /// and closed when the last one goes, so a list fixed at startup
    /// would report a port that is not there, or miss one that is.
    /// See [`Listeners`].
    pub listeners: Listeners,
    /// When this process started serving, for the uptime the
    /// Overview page shows.
    pub started: Instant,
    /// The recent events, and the live feed of new ones.
    ///
    /// See [`events`]: the ring is what makes a reconnect honest,
    /// and it is what the sampler publishes into.
    pub events: Arc<Events>,
    /// Set once, when this listener is stopping.
    ///
    /// Every open events stream selects on it, because graceful
    /// shutdown waits for every open connection and an events body
    /// never ends on its own. A [`watch`] rather than a broadcast so
    /// that a stream opened *after* the signal sees it immediately
    /// rather than waiting for one that will not come again.
    pub shutdown: watch::Sender<bool>,
    /// How often the sampler reads the broker.
    pub events_interval: Duration,
    /// The interim bearer token a mutating request must present,
    /// when one is configured.
    ///
    /// Set by [`AdminServer::bind`] from the configuration it is
    /// given, so that the token the guard checks and the token the
    /// startup refusal counted on cannot be two different things.
    /// Behind an [`Arc`] because this whole struct is cloned per
    /// request and a secret is not worth an allocation each time.
    pub token: Option<Arc<Token>>,
    /// The hostnames this listener answers to beyond an address
    /// literal and `localhost`.
    ///
    /// Set by [`AdminServer::bind`] from the configuration, for the
    /// same reason the token is: the value the guard reads and the
    /// value the operator wrote must be one thing.
    pub allowed_hosts: Arc<[String]>,
    /// The rules this node is running, when it has somewhere to
    /// keep one.
    ///
    /// `None` on a node with no store: section 9.1 makes a rule
    /// durable metadata, so a broker that writes nothing down holds
    /// no rules and says so rather than answering with an empty
    /// collection. See [`rules`].
    pub rules: Option<Arc<::rules::live::Registry>>,
    /// Section 12.1's directory, on a node with a store. See [`users`].
    pub directory: Option<Arc<::directory::Directory>>,
    /// The live subscribers the interface reads.
    ///
    /// See [`taps`]: a tap is a real session inside the broker, so
    /// this is where the cap, the rings and the leases live. Empty
    /// on a broker nobody has pointed a Topics page at.
    pub taps: Arc<Taps>,
}

impl AdminState {
    /// A state over one broker, nothing ready and nothing bound yet.
    ///
    /// The readiness gates start shut: a node that has not said it is
    /// ready is not. The caller opens them as startup passes each
    /// one, and fills in the listeners it bound.
    pub fn new(shared: Arc<Shared>) -> Self {
        let events = Arc::new(Events::new(Arc::clone(shared.metrics())));
        let (shutdown, _) = watch::channel(false);
        Self {
            events,
            shutdown,
            events_interval: DEFAULT_EVENTS_INTERVAL,
            shared,
            readiness: Arc::new(Readiness::starting()),
            node_id: Arc::from("local"),
            node_name: Arc::from("local"),
            node_address: Arc::from(""),
            cluster: None,
            listeners: listener_list(Vec::new()),
            started: Instant::now(),
            token: None,
            allowed_hosts: Arc::from(Vec::new()),
            rules: None,
            directory: None,
            taps: Arc::new(Taps::new()),
        }
    }

    /// Sample the broker this often for the events stream.
    #[must_use]
    pub fn with_events_interval(mut self, interval: Duration) -> Self {
        self.events_interval = interval;
        self
    }

    /// Guard mutating requests with `token`.
    #[must_use]
    pub fn with_token(mut self, token: Option<Token>) -> Self {
        self.token = token.map(Arc::new);
        self
    }

    /// Also answer to these hostnames. See [`host`].
    #[must_use]
    pub fn with_allowed_hosts(mut self, hosts: Vec<String>) -> Self {
        self.allowed_hosts = Arc::from(hosts);
        self
    }

    /// Share readiness gates somebody else is holding.
    #[must_use]
    pub fn with_readiness(mut self, readiness: Arc<Readiness>) -> Self {
        self.readiness = readiness;
        self
    }

    /// Name this node.
    #[must_use]
    pub fn with_node_id(mut self, node_id: impl Into<Arc<str>>) -> Self {
        self.node_id = node_id.into();
        self
    }

    /// Say what an operator calls this node.
    #[must_use]
    pub fn with_node_name(mut self, name: impl Into<Arc<str>>) -> Self {
        self.node_name = name.into();
        self
    }

    /// Say what this node tells peers to dial.
    #[must_use]
    pub fn with_node_address(mut self, address: impl Into<Arc<str>>) -> Self {
        self.node_address = address.into();
        self
    }

    /// Report the cluster this node is in.
    ///
    /// `None` is a node that does not cluster.
    #[must_use]
    pub fn with_cluster(mut self, cluster: Option<Arc<dyn Cluster>>) -> Self {
        self.cluster = cluster;
        self
    }

    /// Report the listeners in `listeners`, as they are when asked.
    ///
    /// The list itself is shared rather than copied, so that a
    /// listener bound or closed after this call is one this API knows
    /// about. See [`Listeners`].
    #[must_use]
    pub fn with_listeners(mut self, listeners: Listeners) -> Self {
        self.listeners = listeners;
        self
    }

    /// Serve the rules this registry is running. See [`rules`].
    #[must_use]
    pub fn with_rules(mut self, registry: Option<Arc<::rules::live::Registry>>) -> Self {
        self.rules = registry;
        self
    }

    /// Serve users from `directory`. Section 12.1.
    #[must_use]
    pub fn with_directory(mut self, directory: Option<Arc<::directory::Directory>>) -> Self {
        self.directory = directory;
        self
    }
}

/// A socket bound for a route set, before anything is served on it.
///
/// # The seam, and why it is two steps rather than one
///
/// Section 9.2 puts the ingest endpoint on a listener of its own by
/// default, so that accepting data does not require exposing the
/// administrative surface. That needs a second address serving a
/// *different* route set, and this crate is where the serving lives.
///
/// Binding and serving are separate because the admin listener has to
/// know what it bound before it can build its routes: the port may be
/// zero in the configuration and chosen by the operating system, and
/// `/api/v1/listeners` reports the port that was actually taken. A
/// single `serve(address, routes)` could not hand the address to the
/// thing building the routes.
///
/// # What is **not** in here
///
/// The host check, the token, the events sampler, the tap reaper and
/// the interface. All five belong to the admin route set and are
/// applied by [`router`], not by this. See [`host`] for the decision
/// about the first of them: the check is a blanket layer over the
/// admin router because a browser is pointed at that router, and an
/// ingest listener behind the same layer would mean an operator
/// listing every webhook sender's hostname, or every delivery refused
/// in a way that looks like a bug. Every ingest route is guarded by
/// its own rule's bearer token instead, which is why that listener
/// needs no rule about the name it is reached under.
pub struct Bound {
    /// The bound socket.
    listener: TcpListener,
    /// What it actually bound, with the port the operating system
    /// chose when the configuration asked for zero.
    address: SocketAddr,
    /// What this listener is called, for the log lines and for the
    /// refusal if it cannot be bound.
    what: &'static str,
}

impl Bound {
    /// Bind `address` for the listener called `what`.
    ///
    /// # Errors
    ///
    /// [`AdminStartError::Bind`] when the socket cannot be bound,
    /// which is usually something else already on the port. Naming
    /// `what` in it is the difference between an operator reading
    /// "cannot bind the ingest listener" and guessing which of the
    /// two failed.
    pub async fn at(what: &'static str, address: SocketAddr) -> Result<Self, AdminStartError> {
        let listener =
            TcpListener::bind(address)
                .await
                .map_err(|source| AdminStartError::Bind {
                    what,
                    address,
                    source,
                })?;
        let address = listener
            .local_addr()
            .map_err(|source| AdminStartError::Bind {
                what,
                address,
                source,
            })?;
        Ok(Self {
            listener,
            address,
            what,
        })
    }

    /// The address actually bound.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Serve `routes` here until the handle is shut down.
    ///
    /// `on_stopping` runs **before** hyper begins waiting for open
    /// connections, not after. Returning from the graceful-shutdown
    /// future is what starts that wait, and a body that never ends on
    /// its own — section 11.1's events stream — would make it a wait
    /// with no end. The admin listener passes the send that tells
    /// every open stream; a route set with no such body passes
    /// nothing, which is the honest answer rather than a channel
    /// nobody reads.
    pub fn serve(self, routes: Router, on_stopping: impl FnOnce() + Send + 'static) -> Serving {
        let (stop, stopped) = oneshot::channel();
        let what = self.what;
        let address = self.address;
        let serve = tokio::spawn(async move {
            let served = axum::serve(self.listener, routes)
                .with_graceful_shutdown(async move {
                    // An error is the sender dropped, which is the
                    // state we are waiting for.
                    let _ = stopped.await;
                    on_stopping();
                })
                .await;
            if let Err(error) = served {
                tracing::error!(%error, listener = what, "a listener stopped with an error");
            }
        });
        tracing::info!(%address, listener = what, "a listener is serving");
        Serving {
            address,
            what,
            stop: Some(stop),
            serve,
        }
    }
}

/// A bound listener and the task serving a route set on it.
pub struct Serving {
    /// What it bound.
    address: SocketAddr,
    /// What it is called.
    what: &'static str,
    /// Dropping this is the signal to stop.
    stop: Option<oneshot::Sender<()>>,
    /// The task running `axum::serve`.
    serve: JoinHandle<()>,
}

impl Serving {
    /// The address actually bound.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Stop serving, and wait for the server to finish.
    ///
    /// Bounded, as section 11.1 asks. What actually ends a body that
    /// does not end on its own is the signal it selects on, sent by
    /// the graceful shutdown future above; the timeout is the backstop
    /// for a body that does not, so that a broker stops rather than
    /// hanging on a browser tab. Reaching it is a defect and says so.
    pub async fn shutdown(mut self) {
        // Dropping the sender is the signal; sending is the same thing
        // said explicitly.
        drop(self.stop.take());
        if tokio::time::timeout(SHUTDOWN_GRACE, self.serve)
            .await
            .is_err()
        {
            tracing::error!(
                listener = self.what,
                grace = ?SHUTDOWN_GRACE,
                "a listener did not finish within its grace period; \
                 something held a connection open past the shutdown signal"
            );
        }
    }
}

/// Where the admin listener is started from.
pub struct AdminServer;

impl AdminServer {
    /// Check the configuration, bind the socket, then serve on it.
    ///
    /// The order matters: the refusal in [`AdminConfig::check`] runs
    /// **before** anything is bound, so an operator who asked for an
    /// unprotected listener is told that rather than being told the
    /// address was already in use, and no socket is ever opened on it.
    ///
    /// The socket is bound before this returns, so a caller learns
    /// immediately that the address was unavailable and a test can
    /// read the port it was given.
    pub async fn bind(
        config: &AdminConfig,
        state: AdminState,
    ) -> Result<AdminRunning, AdminStartError> {
        Self::bind_with(config, state, Router::new()).await
    }

    /// The same, serving `extra` beside the admin routes.
    ///
    /// For section 9.2's ingest endpoint, which an operator may choose
    /// to serve here rather than on a listener of its own. What is
    /// merged here **is** behind everything the admin route set is
    /// behind, the host check included; see [`host`]. That is the right
    /// way round: an operator who puts a route set on this listener
    /// takes this listener's rules with it, and the alternative would
    /// be a hole in a layer that is documented as having no exceptions.
    ///
    /// # Errors
    ///
    /// The same as [`AdminServer::bind`].
    pub async fn bind_with(
        config: &AdminConfig,
        state: AdminState,
        extra: Router<AdminState>,
    ) -> Result<AdminRunning, AdminStartError> {
        config.check()?;
        // The configuration is the authority on the token, not
        // whatever the caller happened to build into the state. The
        // refusal above decided whether this listener may be started
        // at all by reading `config.token`, so the guard that runs
        // per request must read the same field or the two could
        // disagree about whether this listener is protected.
        let state = state
            .with_token(config.token.clone())
            .with_allowed_hosts(config.allowed_hosts.clone());
        let bound = Bound::at("admin", config.address).await?;
        let address = bound.address();

        // This listener knows what it bound better than its caller
        // does; see `AdminState::listeners`.
        state.listeners.write().push(ListenerView {
            name: "admin".to_owned(),
            protocol: ListenerProtocol::Admin,
            address,
            // An admin listener has no topic namespace and no
            // connection limit of its own.
            mountpoint: String::new(),
            max_connections: None,
            max_qos: None,
            retain_available: None,
            durability: None,
        });

        let events = Arc::clone(&state.events);
        let taps = Arc::clone(&state.taps);
        let shared = Arc::clone(&state.shared);
        let sampler = events::spawn_sampler(state.clone(), state.events_interval);
        let reaper = taps::spawn_reaper(state.clone(), taps::REAP_INTERVAL);
        let signal = state.shutdown.clone();
        let serving = bound.serve(router(state, extra), move || {
            // **Before hyper starts waiting, not after.** An events
            // body does not finish on its own, and telling the streams
            // first is what makes that wait terminate. See
            // [`Bound::serve`].
            let _ = signal.send(true);
        });

        Ok(AdminRunning {
            serving,
            sampler,
            reaper,
            events,
            taps,
            shared,
        })
    }
}

/// The routes this listener serves.
///
/// The two fallbacks are registered rather than left to axum's
/// defaults, which are an empty `404` and an empty `405` with no
/// body at all. Section 13 and [`error`]: every failure this API
/// returns has the one shape, and "the path you asked for is not
/// served" is a failure a client has to be able to tell from a proxy
/// that swallowed the request.
///
/// The `404` half of that now lives in [`assets`], because the same
/// fallback has to decide whether the path is a document of the
/// interface first.
fn router(state: AdminState, extra: Router<AdminState>) -> Router {
    // **No compression layer, deliberately.** Section 11.1: a
    // compression layer applied across this listener would turn the
    // events stream into batch delivery. The events route is not
    // exempted from one; there is none to be exempted from.
    Router::new()
        .merge(health::routes())
        .merge(resources::routes())
        .merge(rules::routes())
        .merge(users::routes())
        .merge(acl::routes())
        .merge(events::routes())
        .merge(taps::routes())
        // Whatever the caller asked to be served here as well, inside
        // the layer below rather than outside it.
        .merge(extra)
        // The interface, and the one error shape for everything that
        // is neither a route above nor a document. See [`assets`]:
        // the set of documents is not known until the build script
        // has run, so they are matched here rather than registered.
        .fallback(assets::fallback)
        .method_not_allowed_fallback(error::method_not_allowed)
        // **Outside everything, including the fallbacks.** Section
        // 11.1 protects this listener by where it listens, which is
        // an answer to a program on another host and not to a
        // browser on this one: DNS rebinding makes a page's request
        // same-origin, so no preflight stops it. [`host`] checks the
        // name the request was made under, which is the one thing
        // the attack cannot forge. A layer rather than an extractor
        // per handler, because the rule has no exceptions and an
        // extractor a new route forgets is a rule with one.
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            host::guard,
        ))
        .with_state(state)
}

/// A bound admin listener and the task serving it.
///
/// The serving itself is a [`Serving`], which is the seam; what this
/// adds is everything that belongs to the *admin* route set and to no
/// other — the events sampler, the tap reaper, and the taps that have
/// to be closed before the process stops.
pub struct AdminRunning {
    serving: Serving,
    sampler: JoinHandle<()>,
    reaper: JoinHandle<()>,
    events: Arc<Events>,
    taps: Arc<Taps>,
    shared: Arc<Shared>,
}

impl AdminRunning {
    /// The address actually bound, with the port the operating system
    /// chose when the configuration asked for zero.
    pub fn address(&self) -> SocketAddr {
        self.serving.address()
    }

    /// The taps this listener is holding.
    ///
    /// A test reads it; nothing else needs to, because a tap is
    /// reached through the API that created it.
    pub fn taps(&self) -> &Arc<Taps> {
        &self.taps
    }

    /// The events ring this listener is serving.
    ///
    /// Milestone 4's hooks publish into it; today the sampler does,
    /// and a test does.
    pub fn events(&self) -> &Arc<Events> {
        &self.events
    }

    /// Stop serving, and wait for the server to finish.
    ///
    /// Bounded by [`Serving::shutdown`], as section 11.1 asks. What is
    /// here rather than there is what belongs to this route set: the
    /// taps, which are sessions inside the broker, and the two tasks
    /// that no other route set has.
    pub async fn shutdown(mut self) {
        // **Before the server stops, not after.** A tap is a session
        // the broker believes is attached, and a session that is
        // still attached when the process stops never had its
        // departure written down. The reaper is stopped first so
        // that it cannot be halfway through a sweep of the list this
        // is emptying.
        self.reaper.abort();
        let _ = (&mut self.reaper).await;
        self.taps.close_all(&self.shared);
        self.serving.shutdown().await;
        self.sampler.abort();
        let _ = self.sampler.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;
    use broker_core::broker::BrokerConfig;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    /// A state over a broker with no store, which is all any of these
    /// need.
    fn state() -> AdminState {
        AdminState::new(Arc::new(Shared::new(BrokerConfig::default())))
    }

    /// A route set is served where it is asked for, and nowhere else.
    ///
    /// # Every clause here is load bearing
    ///
    /// A seam that serves everything everywhere passes any test that
    /// only asks whether the second listener answers, so the assertions
    /// go in both directions: a route registered on the second listener
    /// is **not** on the admin one, and the admin routes are **not** on
    /// the second. Without the second half, [`router`] merged into
    /// every route set would pass.
    ///
    /// # And the host check is the admin route set's, not the seam's
    ///
    /// The last two assertions are the written decision in [`host`],
    /// said as something that fails. See the module documentation
    /// there: the check is a blanket layer over the admin router
    /// because that router is what a browser is pointed at, and an
    /// ingest listener behind it would mean an operator adding every
    /// webhook sender's hostname or seeing every delivery refused in a
    /// way that looks like a bug. Moving the layer into the seam, so
    /// that it applies to whatever is served, is one of the two
    /// mutations this test exists to kill.
    #[tokio::test]
    async fn a_route_set_is_served_where_it_is_asked_for_and_nowhere_else() {
        let admin = AdminServer::bind(&config(IpAddr::V4(Ipv4Addr::LOCALHOST), None), state())
            .await
            .expect("loopback needs no token");

        // A second listener, serving one route of its own and nothing
        // of the admin's. Nothing here is the admin's: no state, no
        // sampler, no interface, no layer.
        let bound = Bound::at("test", SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .expect("a port on the loopback");
        let other = bound.serve(
            Router::new().route(
                "/only-here",
                axum::routing::get(|| async { "served on the second listener" }),
            ),
            || {},
        );
        assert_ne!(
            other.address(),
            admin.address(),
            "two listeners, not one read twice"
        );

        // Where it was asked for.
        let answered = testing::get(other.address(), "/only-here").await;
        assert_eq!(answered.status, 200, "{}", answered.body);

        // And nowhere else.
        let elsewhere = testing::get(admin.address(), "/only-here").await;
        assert_eq!(
            elsewhere.status, 404,
            "a route registered on one listener answered on the other: {}",
            elsewhere.body
        );

        // The admin's own routes are not on the second listener
        // either. `/api/v1/retained` reads the broker, `/health/live`
        // is a probe and `/` is the interface: three different route
        // sources, because the seam merges five of them and a test of
        // one would pass against a seam that dropped only that one.
        for path in ["/api/v1/retained", "/health/live", "/metrics", "/"] {
            let leaked = testing::get(other.address(), path).await;
            assert_eq!(
                leaked.status, 404,
                "{path} is the admin listener's and was served on a listener \
                 that was never asked for it: {}",
                leaked.body
            );
        }

        // The host check belongs to the admin route set. It refuses a
        // name on the admin listener...
        let refused =
            testing::get_with_host(admin.address(), "/health/live", "attacker.example").await;
        assert_eq!(
            refused.status, 400,
            "the admin listener still checks the name it was reached under: {}",
            refused.body
        );
        // ...and does not reach a route set that never asked for it.
        let allowed =
            testing::get_with_host(other.address(), "/only-here", "attacker.example").await;
        assert_eq!(
            allowed.status, 200,
            "the host check was applied to a route set outside the admin router, \
             which would mean an ingest listener refusing every webhook sender \
             whose hostname an operator had not listed: {}",
            allowed.body
        );

        other.shutdown().await;
        admin.shutdown().await;
    }

    /// A configuration binding `address` with `token`, on a port the
    /// operating system chooses.
    fn config(ip: IpAddr, token: Option<&str>) -> AdminConfig {
        AdminConfig {
            address: SocketAddr::new(ip, 0),
            token: token.map(Token::new),
            ..AdminConfig::default()
        }
    }

    /// The three cases the rule has, one test each.
    ///
    /// Convention 5: a test of one of them passes against a guard that
    /// refuses everything and against a guard that refuses nothing, so
    /// each case is asserted on its own.
    ///
    /// Started through [`AdminServer::bind`] rather than by calling
    /// the guard, because what the rule promises is that the listener
    /// does not *start*. A unit test of the predicate would pass
    /// against a `bind` that never consults it.
    #[tokio::test]
    async fn a_non_loopback_address_with_no_token_refuses_to_start() {
        // TEST-NET-3, from RFC 5737. It cannot be bound on this host,
        // which is the point: the refusal must happen before anything
        // is bound, so the error must be the refusal and not a bind
        // failure.
        let config = config(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)), None);
        let error = AdminServer::bind(&config, state())
            .await
            .err()
            .expect("an unprotected listener does not start");
        assert!(
            matches!(error, AdminStartError::Unprotected { .. }),
            "the refusal is the rule's, not the socket's: {error:?}"
        );
        let said = error.to_string();
        assert!(
            said.contains("203.0.113.1"),
            "the refusal names the address: {said}"
        );
        assert!(
            said.contains("admin.token"),
            "and the key that is missing: {said}"
        );
    }

    #[tokio::test]
    async fn the_same_address_with_a_token_starts() {
        // The wildcard rather than TEST-NET-3, because this one has to
        // bind for real and no host owns 203.0.113.1. It is the same
        // rule: not loopback, and a token makes it acceptable.
        let config = config(IpAddr::V4(Ipv4Addr::UNSPECIFIED), Some("a-secret"));
        let running = AdminServer::bind(&config, state())
            .await
            .expect("a token makes a non-loopback address acceptable");
        assert_ne!(running.address().port(), 0, "a real port was chosen");
        running.shutdown().await;
    }

    #[tokio::test]
    async fn a_loopback_address_with_no_token_starts() {
        let config = config(IpAddr::V4(Ipv4Addr::LOCALHOST), None);
        let running = AdminServer::bind(&config, state())
            .await
            .expect("loopback needs no token");
        assert_eq!(running.address().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        running.shutdown().await;
    }

    /// The case the guard has to have a deliberate answer for.
    ///
    /// A wildcard address is neither loopback nor a named interface.
    /// It is *every* interface at once, so it is the most exposed
    /// thing an operator can write and is refused, not excused.
    #[tokio::test]
    async fn a_wildcard_address_with_no_token_refuses_to_start() {
        for ip in [
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        ] {
            let config = config(ip, None);
            let error = AdminServer::bind(&config, state())
                .await
                .err()
                .unwrap_or_else(|| panic!("{ip} reaches every interface and must be refused"));
            assert!(
                matches!(error, AdminStartError::Unprotected { .. }),
                "{ip} is refused by the rule: {error:?}"
            );
        }
    }

    /// `::1` is loopback, and so is `::ffff:127.0.0.1`.
    ///
    /// The second is the one a naive guard gets wrong:
    /// `Ipv6Addr::is_loopback` is false for an IPv4-mapped loopback
    /// address, so an operator who wrote a perfectly safe address
    /// would be told it is a hole.
    #[tokio::test]
    async fn every_spelling_of_loopback_starts_with_no_token() {
        for ip in [
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
        ] {
            let config = config(ip, None);
            let running = AdminServer::bind(&config, state())
                .await
                .unwrap_or_else(|error| panic!("{ip} is loopback and starts: {error}"));
            running.shutdown().await;
        }
    }

    /// A mapped address that is *not* loopback is still refused.
    ///
    /// The canonicalisation the previous test needs must not become a
    /// way to spell a routable address past the guard.
    #[tokio::test]
    async fn a_mapped_non_loopback_address_is_still_refused() {
        let ip = IpAddr::V6(Ipv4Addr::new(203, 0, 113, 1).to_ipv6_mapped());
        let error = AdminServer::bind(&config(ip, None), state())
            .await
            .err()
            .expect("a mapped routable address is not loopback");
        assert!(matches!(error, AdminStartError::Unprotected { .. }));
    }

    /// An empty token is not protection, and is refused wherever it
    /// is written.
    ///
    /// The hole this closes: an empty string satisfies "a token is
    /// configured", so a listener reachable off this host started
    /// believing itself guarded and then accepted a bearer header
    /// with nothing after it. The environment variable path already
    /// read an empty value as absent; the file path did not, and the
    /// two disagreeing is its own defect.
    #[test]
    fn an_empty_token_is_refused_rather_than_read_as_absent() {
        let routable = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1));
        let error = config(routable, Some(""))
            .check()
            .expect_err("an empty token protects nothing");
        assert!(
            matches!(error, AdminStartError::EmptyToken),
            "refused for being empty, not for being absent: {error:?}"
        );
    }

    /// And on loopback too, where nothing needed protecting.
    ///
    /// One answer everywhere is easier to predict than two, and an
    /// operator who wrote the key meant to set something.
    #[test]
    fn an_empty_token_is_refused_on_loopback_as_well() {
        let error = config(IpAddr::V4(Ipv4Addr::LOCALHOST), Some(""))
            .check()
            .expect_err("an empty token is a mistake wherever it is written");
        assert!(matches!(error, AdminStartError::EmptyToken));
    }

    /// The boundary: one character is a token and is accepted.
    ///
    /// Without this, a guard that refused every configured token
    /// would pass the two tests above.
    #[test]
    fn a_token_of_one_character_is_still_a_token() {
        let routable = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1));
        config(routable, Some("x"))
            .check()
            .expect("a short token is a poor token, not an absent one");
    }

    #[test]
    fn a_token_does_not_print_itself() {
        let config = AdminConfig {
            token: Some(Token::new("hunter2")),
            ..AdminConfig::default()
        };
        let shown = format!("{config:?}");
        assert!(
            !shown.contains("hunter2"),
            "section 12.3: a token must not reach a log line: {shown}"
        );
    }

    #[test]
    fn the_default_is_loopback() {
        let config = AdminConfig::default();
        assert!(config.enabled, "it is served unless an operator says not");
        assert!(
            config.address.ip().is_loopback(),
            "section 11.1: it binds the loopback address by default"
        );
        config
            .check()
            .expect("and therefore starts with no token configured");
    }
}
