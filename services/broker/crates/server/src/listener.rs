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

use broker_core::broker::BrokerConfig;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Semaphore};
use tokio::task::JoinHandle;

use crate::connection::{serve, ConnectionConfig};
use crate::shared::Shared;
use crate::ticker::spawn_ticker;

/// The port MQTT over plain TCP is registered on.
pub const DEFAULT_PORT: u16 = 1883;

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
        let listener = TcpListener::bind(config.address).await?;
        let address = listener.local_addr()?;

        let shared = Arc::new(Shared::new(config.broker.clone()));
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
/// An accept error is logged and the loop continues: one connection the
/// kernel could not hand over is not a reason to stop serving every
/// other client on the node.
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
            Err(error) => {
                tracing::warn!(%error, "a connection could not be accepted");
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
