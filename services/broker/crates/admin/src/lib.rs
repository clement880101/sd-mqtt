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
use std::time::Instant;

use axum::Router;
use sdmqtt_server::shared::Shared;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub mod config;
pub mod error;
pub mod health;
pub mod resources;

#[cfg(test)]
mod testing;

pub use config::{AdminConfig, AdminStartError, Token};
pub use error::{ApiError, ErrorCode};
pub use health::Readiness;
pub use resources::{ListenerProtocol, ListenerView};

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
    pub listeners: Arc<Vec<ListenerView>>,
    /// When this process started serving, for the uptime the
    /// Overview page shows.
    pub started: Instant,
}

impl AdminState {
    /// A state over one broker, nothing ready and nothing bound yet.
    ///
    /// The readiness gates start shut: a node that has not said it is
    /// ready is not. The caller opens them as startup passes each
    /// one, and fills in the listeners it bound.
    pub fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            readiness: Arc::new(Readiness::starting()),
            node_id: Arc::from("local"),
            listeners: Arc::new(Vec::new()),
            started: Instant::now(),
        }
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

        let (stop, stopped) = oneshot::channel();
        let router = router(state);
        let serve = tokio::spawn(async move {
            let served = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    // An error is the sender dropped, which is the
                    // state we are waiting for.
                    let _ = stopped.await;
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
fn router(state: AdminState) -> Router {
    Router::new()
        .merge(health::routes())
        .merge(resources::routes())
        .fallback(error::not_found)
        .method_not_allowed_fallback(error::method_not_allowed)
        .with_state(state)
}

/// A bound admin listener and the task serving it.
pub struct AdminRunning {
    address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    serve: JoinHandle<()>,
}

impl AdminRunning {
    /// The address actually bound, with the port the operating system
    /// chose when the configuration asked for zero.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Stop serving, and wait for the server to finish.
    pub async fn shutdown(mut self) {
        // Dropping the sender is the signal; sending is the same thing
        // said explicitly.
        drop(self.stop.take());
        let _ = self.serve.await;
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
