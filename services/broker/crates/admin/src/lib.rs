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
use std::time::{Duration, Instant};

use axum::Router;
use sdmqtt_server::shared::Shared;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

pub mod assets;
pub mod config;
pub mod error;
pub mod events;
pub mod health;
pub mod resources;
pub mod taps;

#[cfg(test)]
mod testing;

pub use assets::INTERFACE_BUILT;
pub use config::{AdminConfig, AdminStartError, Token};
pub use error::{ApiError, ErrorCode};
pub use events::{EventBody, EventId, Events};
pub use health::Readiness;
pub use resources::{ListenerProtocol, ListenerView};
pub use taps::{Reservation, Tap, TapLimits, Taps, IDLE_TIMEOUT, MAX_TAPS, TAP_CAPACITY};

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
    /// What this node is called.
    ///
    /// Membership is milestone 5, so today this names a cluster of
    /// one. It is configurable rather than derived from a hostname
    /// because a hostname is not stable across a container restart
    /// and a node identifier has to be.
    pub node_id: Arc<str>,
    /// What this node has bound.
    ///
    /// Given rather than discovered: a bound socket does not know
    /// what it is for, and the broker does not know a listener
    /// exists. What startup bound is what the API reports.
    ///
    /// The one exception is this listener itself, which
    /// [`AdminServer::bind`] adds once it knows the port it was
    /// given. A caller cannot supply that: it does not exist until
    /// the socket is bound, and with a configured port of zero it is
    /// the operating system's to choose.
    pub listeners: Arc<Vec<ListenerView>>,
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
            listeners: Arc::new(Vec::new()),
            started: Instant::now(),
            token: None,
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

    /// Say what was bound.
    #[must_use]
    pub fn with_listeners(mut self, listeners: Vec<ListenerView>) -> Self {
        self.listeners = Arc::new(listeners);
        self
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
        config.check()?;
        // The configuration is the authority on the token, not
        // whatever the caller happened to build into the state. The
        // refusal above decided whether this listener may be started
        // at all by reading `config.token`, so the guard that runs
        // per request must read the same field or the two could
        // disagree about whether this listener is protected.
        let state = state.with_token(config.token.clone());
        let listener =
            TcpListener::bind(config.address)
                .await
                .map_err(|source| AdminStartError::Bind {
                    address: config.address,
                    source,
                })?;
        let address = listener
            .local_addr()
            .map_err(|source| AdminStartError::Bind {
                address: config.address,
                source,
            })?;

        // This listener knows what it bound better than its caller
        // does; see `AdminState::listeners`.
        let mut listeners = (*state.listeners).clone();
        listeners.push(ListenerView {
            name: "admin".to_owned(),
            protocol: ListenerProtocol::Admin,
            address,
            // An admin listener has no topic namespace and no
            // connection limit of its own.
            mountpoint: String::new(),
            max_connections: None,
        });
        let state = state.with_listeners(listeners);

        let (stop, stopped) = oneshot::channel();
        let events = Arc::clone(&state.events);
        let taps = Arc::clone(&state.taps);
        let shared = Arc::clone(&state.shared);
        let sampler = events::spawn_sampler(state.clone(), state.events_interval);
        let reaper = taps::spawn_reaper(state.clone(), taps::REAP_INTERVAL);
        let signal = state.shutdown.clone();
        let router = router(state);
        let serve = tokio::spawn(async move {
            let served = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    // An error is the sender dropped, which is the
                    // state we are waiting for.
                    let _ = stopped.await;
                    // **Before returning, not after.** Returning
                    // from here is what starts hyper waiting for
                    // every open connection to finish, and an events
                    // body does not finish on its own. Telling the
                    // streams first is what makes that wait
                    // terminate.
                    let _ = signal.send(true);
                })
                .await;
            if let Err(error) = served {
                tracing::error!(%error, "the admin listener stopped with an error");
            }
        });

        tracing::info!(%address, "the admin listener is serving");
        Ok(AdminRunning {
            address,
            stop: Some(stop),
            serve,
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
fn router(state: AdminState) -> Router {
    // **No compression layer, deliberately.** Section 11.1: a
    // compression layer applied across this listener would turn the
    // events stream into batch delivery. The events route is not
    // exempted from one; there is none to be exempted from.
    Router::new()
        .merge(health::routes())
        .merge(resources::routes())
        .merge(events::routes())
        .merge(taps::routes())
        // The interface, and the one error shape for everything that
        // is neither a route above nor a document. See [`assets`]:
        // the set of documents is not known until the build script
        // has run, so they are matched here rather than registered.
        .fallback(assets::fallback)
        .method_not_allowed_fallback(error::method_not_allowed)
        .with_state(state)
}

/// A bound admin listener and the task serving it.
pub struct AdminRunning {
    address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    serve: JoinHandle<()>,
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
        self.address
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
    /// Bounded, as section 11.1 asks. What actually ends an events
    /// stream is the signal it selects on, sent by the graceful
    /// shutdown future above; the timeout is the backstop for a
    /// stream that does not, so that a broker stops rather than
    /// hanging on a browser tab. Reaching it is a defect and says
    /// so.
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
        // Dropping the sender is the signal; sending is the same thing
        // said explicitly.
        drop(self.stop.take());
        if tokio::time::timeout(SHUTDOWN_GRACE, self.serve)
            .await
            .is_err()
        {
            tracing::error!(
                grace = ?SHUTDOWN_GRACE,
                "the admin listener did not finish within its grace period; \
                 something held a connection open past the shutdown signal"
            );
        }
        self.sampler.abort();
        let _ = self.sampler.await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_core::broker::BrokerConfig;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    /// A state over a broker with no store, which is all any of these
    /// need.
    fn state() -> AdminState {
        AdminState::new(Arc::new(Shared::new(BrokerConfig::default())))
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
