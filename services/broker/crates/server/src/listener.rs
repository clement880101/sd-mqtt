//! Accepting connections, and stopping.
//!
//! The accept loop is the only thing in the process that creates
//! connections, and [`Running::shutdown`] is the only thing that stops
//! them. Everything between is [`crate::connection::serve`].
//!
//! # Stopping does not mean draining
//!
//! Shutdown signals every task and waits for the accept loop, and for
//! nothing else. A client that connects and never speaks would otherwise
//! hold the process open for as long as it liked, and a broker that
//! cannot be restarted promptly is a broker nobody upgrades.

use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use broker_core::broker::BrokerConfig;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Semaphore};
use tokio::task::JoinHandle;

use crate::connection::{serve, ConnectionConfig};
use crate::shared::Shared;
use crate::ticker::spawn_ticker;

/// The port MQTT over plain TCP is registered on.
pub const DEFAULT_PORT: u16 = 1883;

/// How long to wait before accepting again after an error that is
/// about the process rather than about one client.
///
/// Long enough that the loop yields and the log stays readable, short
/// enough that recovery is immediate once descriptors are freed. It is
/// a flat pause rather than a curve because no curve has been measured
/// here, and this project has twice been wrong about an optimisation it
/// guessed at.
const ACCEPT_PAUSE: Duration = Duration::from_millis(100);

/// How many connections one node serves at once by default.
///
/// The design targets a hundred thousand per node, so the cap is set
/// there rather than lower: a limit below the target would refuse
/// clients the machine can serve. It is a cap rather than a promise; the
/// process still needs the file descriptors to go with it.
pub const DEFAULT_MAX_CONNECTIONS: usize = 100_000;

/// How a listener is configured.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Where to bind.
    pub address: SocketAddr,
    /// How the broker behind the socket is configured.
    pub broker: BrokerConfig,
    /// How each accepted connection is configured.
    pub connection: ConnectionConfig,
    /// How many connections may be served at once. Beyond it a new
    /// connection is closed rather than queued.
    pub max_connections: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            // Every interface, because the first place this runs is a
            // container, where a broker on the loopback is a broker
            // nothing can reach.
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEFAULT_PORT),
            broker: BrokerConfig::default(),
            connection: ConnectionConfig::default(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

impl ServerConfig {
    /// A listener on the loopback, on a port the operating system picks.
    ///
    /// Port zero means a test never collides with another test over a
    /// fixed port and never needs one reserved. The port that was
    /// actually chosen is [`Running::address`].
    pub fn ephemeral() -> Self {
        Self {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            ..Self::default()
        }
    }
}

/// Where a server is started from.
pub struct Server;

impl Server {
    /// Bind the socket, then start serving on it.
    ///
    /// The socket is bound before this returns, so a caller learns
    /// immediately that the address was unavailable rather than from a
    /// log line some time later, and a test can read the port it was
    /// given.
    pub async fn bind(config: ServerConfig) -> io::Result<Running> {
        let shared = Arc::new(Shared::new(config.broker.clone()));
        Self::bind_shared(config, shared).await
    }

    /// The same, against a broker somebody else has already built.
    ///
    /// **This is what makes section 7.5 true rather than intended.**
    /// Restore is the first startup phase and must complete before any
    /// listener binds, so no client can observe a half-restored
    /// broker. Expressed as a signature: the socket is not bound until
    /// a whole [`Shared`] exists to hand it, so there is no window in
    /// which a listener could be serving one that is still being
    /// filled.
    ///
    /// `config.broker` is not read here — the broker arrived built —
    /// and the caller is expected to have built it from the same
    /// configuration. Everything else in `config` is used as usual.
    pub async fn bind_shared(config: ServerConfig, shared: Arc<Shared>) -> io::Result<Running> {
        let listener = TcpListener::bind(config.address).await?;
        let address = listener.local_addr()?;

        let (shutdown, _) = broadcast::channel(1);
        let ticker = spawn_ticker(Arc::clone(&shared), shutdown.subscribe());
        let permits = Arc::new(Semaphore::new(config.max_connections));
        // Subscribed here rather than inside the task, because a
        // broadcast receiver only hears what is sent after it exists. A
        // server shut down before its accept loop was first polled would
        // otherwise wait for a signal that had already been sent.
        let stop = shutdown.subscribe();
        let accept = tokio::spawn(accept_loop(
            listener,
            Arc::clone(&shared),
            config.connection.clone(),
            permits,
            shutdown.clone(),
            stop,
        ));

        tracing::info!(%address, "listening");
        Ok(Running {
            address,
            shared,
            shutdown,
            accept,
            ticker,
        })
    }
}

/// A bound listener, its accept loop and its ticker.
pub struct Running {
    address: SocketAddr,
    shared: Arc<Shared>,
    shutdown: broadcast::Sender<()>,
    accept: JoinHandle<()>,
    ticker: JoinHandle<()>,
}

impl Running {
    /// The address actually bound, with the port the operating system
    /// chose when the configuration asked for zero.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// The broker behind this listener.
    pub fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }

    /// Stop accepting, stop ticking, and stop the connections.
    ///
    /// Waits for the accept loop, so that the port is released by the
    /// time this returns, and deliberately not for the connections. Each
    /// is signalled and each discharges its disconnect obligation as it
    /// unwinds, but none of them is waited on: an idle client must not
    /// be able to hold the process open.
    pub async fn shutdown(self) {
        // An error here means every task has already stopped, which is
        // the state we are asking for.
        let _ = self.shutdown.send(());
        let _ = self.accept.await;
        let _ = self.ticker.await;
    }
}

/// Accept until told to stop.
///
/// An accept error never ends the loop: one connection the kernel could
/// not hand over is not a reason to stop serving every other client on
/// the node. What it does change is how soon we try again, which
/// [`affects_one_connection`] decides.
async fn accept_loop(
    listener: TcpListener,
    shared: Arc<Shared>,
    config: ConnectionConfig,
    permits: Arc<Semaphore>,
    shutdown: broadcast::Sender<()>,
    mut stop: broadcast::Receiver<()>,
) {
    loop {
        let accepted = tokio::select! {
            // A closed channel is a dropped `Running`, which means the
            // same thing as a signal.
            _ = stop.recv() => return,
            accepted = listener.accept() => accepted,
        };

        let (socket, peer) = match accepted {
            Ok(accepted) => accepted,
            Err(error) if affects_one_connection(&error) => {
                tracing::debug!(%error, "a connection was gone before it could be accepted");
                continue;
            }
            Err(error) => {
                // Warn rather than debug: descriptor exhaustion is a
                // configuration problem an operator can fix, and one
                // they will never learn about from a line they do not
                // see.
                tracing::warn!(
                    %error,
                    pause = ?ACCEPT_PAUSE,
                    "accepting failed for the whole listener; pausing before trying again"
                );
                tokio::select! {
                    // The pause must not delay a shutdown.
                    _ = stop.recv() => return,
                    _ = tokio::time::sleep(ACCEPT_PAUSE) => {}
                }
                continue;
            }
        };

        // Refused by closing rather than by queueing. A connection that
        // waits in a queue and eventually gets in is worse for the
        // client than a prompt refusal it can retry elsewhere.
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            tracing::warn!(%peer, "a connection was refused: at the connection limit");
            drop(socket);
            continue;
        };

        let shared = Arc::clone(&shared);
        let config = config.clone();
        let mut stop = shutdown.subscribe();
        tokio::spawn(async move {
            tokio::select! {
                served = serve(socket, shared, config) => {
                    if let Err(error) = served {
                        tracing::debug!(%peer, %error, "a connection ended");
                    }
                }
                // Dropping the `serve` future drops the value that owes
                // the broker a disconnect, which is how the obligation
                // is discharged on this path too.
                _ = stop.recv() => tracing::debug!(%peer, "a connection was stopped by shutdown"),
            }
            drop(permit);
        });
    }
}

/// Whether an accept error was about one connection or about the
/// process.
///
/// The listed kinds are all a peer that went away between the kernel's
/// handshake and our accept: ordinary, costing nothing, and worth
/// retrying immediately. Everything else pauses.
///
/// The classification is a whitelist rather than a list of the failures
/// worth pausing for, and deliberately so. Descriptor exhaustion, the
/// case that matters, arrives as `EMFILE` or `ENFILE`, and the standard
/// library categorises neither, so a blacklist would have to carry a
/// table of platform errno numbers and would silently miss whatever it
/// did not list. Erring towards the pause costs a hundred milliseconds
/// when it is wrong; erring the other way costs a core.
fn affects_one_connection(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
            | io::ErrorKind::TimedOut
            | io::ErrorKind::NotConnected
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Running out of file descriptors. The standard library gives
    /// neither a kind of its own on any platform this builds for, which
    /// is exactly why the classifier cannot key on them.
    const EMFILE: i32 = 24;
    const ENFILE: i32 = 23;

    #[test]
    fn an_error_about_one_connection_is_not_a_reason_to_pause() {
        // A peer that resets between the kernel's handshake and our
        // accept costs us nothing. Pausing for it would turn an
        // ordinary event into a hundred milliseconds of not serving
        // anybody.
        for kind in [
            std::io::ErrorKind::ConnectionAborted,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::Interrupted,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::NotConnected,
        ] {
            assert!(
                affects_one_connection(&io::Error::from(kind)),
                "{kind:?} concerns one connection"
            );
        }
    }

    #[test]
    fn running_out_of_file_descriptors_is_a_reason_to_pause() {
        // The failure this prevents: every accept fails instantly, the
        // loop spins on one core refusing every client, and the log
        // fills fast enough that nobody reads the reason.
        for errno in [EMFILE, ENFILE] {
            let error = io::Error::from_raw_os_error(errno);
            assert!(
                !affects_one_connection(&error),
                "errno {errno} is the process in trouble, not one client"
            );
        }
    }

    #[test]
    fn an_unrecognised_error_is_treated_as_the_process_being_in_trouble() {
        // The safe direction. Pausing for something ordinary costs a
        // hundred milliseconds; not pausing for something systemic
        // costs a core for as long as it lasts.
        assert!(!affects_one_connection(&io::Error::other("something new")));
    }

    #[tokio::test]
    async fn binds_to_an_ephemeral_port_and_reports_it() {
        let running = Server::bind(ServerConfig::ephemeral()).await.unwrap();
        assert_ne!(running.address().port(), 0, "the real port is reported");
        running.shutdown().await;
    }

    #[tokio::test]
    async fn accepts_a_connection() {
        let running = Server::bind(ServerConfig::ephemeral()).await.unwrap();
        let stream = tokio::net::TcpStream::connect(running.address()).await;
        assert!(stream.is_ok());
        running.shutdown().await;
    }

    #[tokio::test]
    async fn shutting_down_releases_the_port() {
        let config = ServerConfig::ephemeral();
        let running = Server::bind(config).await.unwrap();
        let address = running.address();
        running.shutdown().await;

        // The port is free again, so a second server can take it.
        let again = tokio::net::TcpListener::bind(address).await;
        assert!(again.is_ok(), "the listener did not release its port");
    }

    #[tokio::test]
    async fn shutting_down_does_not_wait_for_idle_connections() {
        // A client that connects and says nothing must not hold the
        // process open. A broker that cannot be restarted promptly is a
        // broker nobody upgrades.
        let running = Server::bind(ServerConfig::ephemeral()).await.unwrap();
        let _idle = tokio::net::TcpStream::connect(running.address())
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), running.shutdown())
            .await
            .expect("shutdown does not hang on an idle connection");
    }

    #[tokio::test]
    async fn the_ticker_runs_while_the_server_does() {
        let running = Server::bind(ServerConfig::ephemeral()).await.unwrap();
        let before = running.shared().with_broker(|broker| broker.clock().now());
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let after = running.shared().with_broker(|broker| broker.clock().now());
        assert!(
            after > before,
            "the clock did not advance: {before} then {after}"
        );
        running.shutdown().await;
    }
}
