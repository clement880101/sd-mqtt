//! Keeping one bridge connected, and saying whether it is.
//!
//! Section 9.2 asks for three things here and they are separable:
//! reconnection with exponential backoff and jitter, a state topic
//! reporting connectivity with a will supplying the offline value, and
//! a singleton that runs on exactly one node.
//!
//! # A clean start on the first connection of a process
//!
//! This is the one decision in the bridge that a reader will want
//! argued rather than stated.
//!
//! [`super::flow`] holds the in-flight window in memory: which of this
//! bridge's messages the remote has acknowledged, under which packet
//! identifiers. A process that restarts has forgotten all of it. If it
//! then reconnected asking the remote to **resume** a session, the
//! remote would still be holding identifiers this end can no longer
//! match, the outbox would re-send those same messages under fresh
//! identifiers, and the result is convention 3's failure exactly:
//! "delivers a message twice to a peer still waiting on the old
//! identifier, and later discards a genuinely new message when the
//! counter wraps onto the stranded one".
//!
//! So the **first** connection a process makes sets clean start,
//! whatever the configuration says, and every reconnection after it
//! within that process uses the configured value. A session the remote
//! discards holds nothing this end has forgotten; the outbox is
//! durable and re-sends from the beginning, and nothing is duplicated
//! or stranded.
//!
//! The cost is that a remote holding messages *for* us across our
//! restart discards them. That is a real cost and it is the smaller
//! one: those are messages the remote would deliver again on its own
//! terms if it kept them, and the alternative is a protocol violation.
//! Making the in-flight window durable is what removes the cost, it is
//! a record shape and a restore path rather than a line of code, and
//! it is named here so that it reads as a deferral rather than as an
//! oversight — convention 8.
//!
//! # The state topic is local, and the will is the remote's copy
//!
//! Section 9.2: "A state topic reports connectivity, with a will
//! supplying the offline value." The publication is made **here**, into
//! this broker, because that is what section 9.2's other sentence is
//! about — "a bridge whose own state topic matches its outbound filter
//! is refused […] because a bridge republishing its own liveness is a
//! loop with a heartbeat" — and only a local publication can match an
//! outbound filter.
//!
//! The will carries the same topic name to the remote, because the one
//! case a local publication cannot cover is the process dying: if this
//! broker is gone, nothing here publishes anything, and the far side
//! would otherwise believe a dead bridge is alive for ever.
//! [`super::topics::check`] refuses a state topic either side would
//! carry.
//!
//! # Plain text goes to this machine and nowhere else
//!
//! The same rule `http_out` holds an endpoint to, for the same reason
//! and with more at stake: a bridge sends a password in its CONNECT and
//! then everything it carries. A `mqtt://` address to anywhere but a
//! loopback host is refused when the configuration is read, and
//! `mqtts://` is what a real deployment uses.

use std::net::IpAddr;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::time::Duration;

use crate::rule::MqttBridge;
use crate::singleton::Ownership;

use super::backoff::{Backoff, Entropy};
use super::connack::{Negotiated, OFFLINE};
use super::flow::Flow;
use super::session::{self, Bridge, Ended};

/// How long a socket may take to open.
///
/// Bounded here rather than left to the operating system, which on a
/// host that drops packets silently is minutes. The bridge would be
/// stuck in `connect` for all of it, outside the backoff that exists
/// to make a dead remote cheap.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// What can be wrong with a bridge's address.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AddressError {
    /// The scheme is not one this bridge speaks.
    #[error(
        "mqtt_bridge.address {address:?} has no scheme this bridge speaks. Write \
         mqtts://host:port for a TLS link, or mqtt://host:port for a plain-text one to \
         this machine."
    )]
    Scheme {
        /// What was configured.
        address: String,
    },
    /// There is no host in it.
    #[error("mqtt_bridge.address {address:?} names no host")]
    NoHost {
        /// What was configured.
        address: String,
    },
    /// The port is not a port.
    #[error("mqtt_bridge.address {address:?} has no usable port: {why}")]
    Port {
        /// What was configured.
        address: String,
        /// Why it is not a port.
        why: String,
    },
    /// Plain text, to somewhere other than this machine.
    ///
    /// The same refusal `http_out` makes of an `http://` endpoint, and
    /// the stakes are higher: a bridge sends a password in its CONNECT
    /// and then everything it carries.
    #[error(
        "mqtt_bridge.address {address:?} is plain text to {host:?}, which is not this \
         machine. A bridge sends its password in the connect and then everything it \
         carries; use mqtts:// . Plain text is permitted to a loopback address, where \
         it does not leave the host."
    )]
    PlainTextOffHost {
        /// What was configured.
        address: String,
        /// The host it names.
        host: String,
    },
}

/// Where a bridge connects, and whether it does so in the clear.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Address {
    /// The host, as written, for the connection and for the TLS name.
    pub host: String,
    /// The port.
    pub port: u16,
    /// Whether the link is encrypted.
    pub tls: bool,
}

impl Address {
    /// `host:port`, for a socket.
    pub fn authority(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// The default port for a plain-text MQTT link.
const MQTT_PORT: u16 = 1883;
/// The default port for one over TLS.
const MQTTS_PORT: u16 = 8883;

/// Read a bridge's address, and refuse one it should not connect to.
///
/// # Errors
///
/// [`AddressError`], naming what is wrong with it.
pub fn address(configured: &str) -> Result<Address, AddressError> {
    let refuse = || AddressError::Scheme {
        address: configured.to_owned(),
    };
    let (tls, rest) = match configured.split_once("://") {
        Some(("mqtts" | "ssl" | "tls", rest)) => (true, rest),
        Some(("mqtt" | "tcp", rest)) => (false, rest),
        Some(_) => return Err(refuse()),
        // A bare `host:port` is refused rather than guessed at. Either
        // guess is wrong half the time and one of the two wrong
        // guesses sends a password in the clear.
        None => return Err(refuse()),
    };
    let rest = rest.trim_end_matches('/');

    let (host, port) = match rest.rsplit_once(':') {
        // An IPv6 literal is bracketed, and the colon inside it is not
        // the port separator.
        Some((host, port)) if !port.contains(']') => {
            let port = port.parse::<u16>().map_err(|error| AddressError::Port {
                address: configured.to_owned(),
                why: error.to_string(),
            })?;
            if port == 0 {
                return Err(AddressError::Port {
                    address: configured.to_owned(),
                    why: "port zero asks the operating system to choose, which is not \
                          something a remote broker can be listening on"
                        .to_owned(),
                });
            }
            (host, port)
        }
        _ => (rest, if tls { MQTTS_PORT } else { MQTT_PORT }),
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.is_empty() {
        return Err(AddressError::NoHost {
            address: configured.to_owned(),
        });
    }

    if !tls && !is_loopback(host) {
        return Err(AddressError::PlainTextOffHost {
            address: configured.to_owned(),
            host: host.to_owned(),
        });
    }

    Ok(Address {
        host: host.to_owned(),
        port,
        tls,
    })
}

/// Whether a host names this machine.
///
/// By literal address or by the one name every system resolves to one.
/// A name that *resolves* to a loopback address is deliberately not
/// accepted: that would make the refusal depend on a lookup which can
/// change between the check and the connection, and a check that can be
/// true when it is made and false when it matters is not a check.
fn is_loopback(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    host.parse::<IpAddr>()
        .is_ok_and(|address| address.is_loopback())
}

/// One bridge, kept connected.
pub struct Supervisor {
    /// Everything that does not change between connections.
    bridge: Bridge,
    /// Where to connect.
    address: Address,
    /// When to try again.
    backoff: Backoff,
    /// Where the jitter comes from.
    entropy: Entropy,
    /// Whether this process has connected yet. See the module
    /// documentation: the first connection of a process starts clean.
    connected_before: bool,
    /// The identifier to use, which is the one the remote assigned
    /// once it has assigned one.
    client_id: String,
    /// What this bridge has sent the remote and not had answered.
    ///
    /// **Here rather than in the session**, because it is what a
    /// resumed session resends: a window rebuilt per connection would
    /// have nothing to resume and the rule that a redelivery keeps its
    /// original identifier would have no path through the running
    /// system to be wrong on.
    flow: Flow,
}

impl Supervisor {
    /// A supervisor for one bridge.
    ///
    /// # Errors
    ///
    /// [`AddressError`] when the configured address is not one this
    /// bridge will connect to.
    pub fn new(bridge: Bridge, action: &MqttBridge) -> Result<Self, AddressError> {
        Ok(Self {
            address: address(&action.address)?,
            backoff: Backoff::new(action.reconnect_min_ms, action.reconnect_max_ms),
            entropy: Entropy::new(bridge.rule.as_str()),
            connected_before: false,
            client_id: action.client_id.clone(),
            // Sized from the configuration until a CONNACK says
            // otherwise, which it does on every connection.
            flow: Flow::new(action.receive_maximum),
            bridge,
        })
    }

    /// Where this connects.
    pub fn address(&self) -> &Address {
        &self.address
    }

    /// Connect, carry messages, reconnect, for as long as this node
    /// owns the rule.
    ///
    /// Section 9.2 makes a bridge a singleton. Two nodes running one
    /// bridge is two clients with the same identifier on the remote,
    /// which by [MQTT-3.1.4-2] means each one's connection takes the
    /// other's session away — a pair of bridges disconnecting each
    /// other in a loop as fast as they can reconnect. So `ownership`
    /// is not an optional refinement here; it is the difference
    /// between a bridge and an outage.
    pub async fn run(mut self, mut ownership: Ownership) {
        loop {
            if !ownership.owns() {
                tracing::info!(
                    rule = self.bridge.rule.as_str(),
                    owner = %ownership.owner(),
                    "this bridge is a singleton and another member owns it; nothing \
                     connects here until that changes"
                );
                while !ownership.owns() {
                    ownership.changed().await;
                }
                tracing::info!(
                    rule = self.bridge.rule.as_str(),
                    "this node has taken this bridge over"
                );
            }

            let ended = self.once().await;
            // The session says "offline" on its way out; this covers
            // the attempt that never got a session at all, where the
            // state topic would otherwise still say the bridge was up
            // from whenever it last was.
            session::state(&self.bridge, OFFLINE).await;

            if !ended.worth_retrying() {
                tracing::error!(
                    rule = self.bridge.rule.as_str(),
                    %ended,
                    "this bridge cannot connect to this remote, and reconnecting would \
                     be asking the same question for ever; it is stopping"
                );
                return;
            }

            let wait = self.backoff.wait(self.entropy.draw());
            tracing::warn!(
                rule = self.bridge.rule.as_str(),
                address = %self.address.authority(),
                %ended,
                failures = self.backoff.failures(),
                wait_ms = wait.as_millis() as u64,
                "the bridge is disconnected and will try again"
            );
            tokio::time::sleep(wait).await;
        }
    }

    /// One connection, from the socket to whatever ended it.
    async fn once(&mut self) -> Ended {
        let stream = match self.open().await {
            Ok(stream) => stream,
            Err(error) => return Ended::Io(error),
        };

        // **Clean on the first connection of this process.** The
        // module documentation has the argument; the short version is
        // that a resumed session would hold packet identifiers this
        // process has forgotten.
        let clean_start = !self.connected_before || self.bridge.action.clean_start;
        if !self.connected_before && !self.bridge.action.clean_start {
            tracing::info!(
                rule = self.bridge.rule.as_str(),
                "this is the first connection of this process, so it asks for a clean \
                 start even though the rule does not: a resumed session would hold \
                 packet identifiers this process no longer knows about"
            );
        }

        let (ready, carried, ended) = match stream {
            Stream::Plain(stream) => {
                session::run(
                    &self.bridge,
                    stream,
                    &self.client_id,
                    clean_start,
                    &mut self.flow,
                )
                .await
            }
            Stream::Tls(stream) => {
                session::run(
                    &self.bridge,
                    stream,
                    &self.client_id,
                    clean_start,
                    &mut self.flow,
                )
                .await
            }
        };

        if let Some(negotiated) = ready {
            self.settled(&negotiated);
        }
        tracing::info!(
            rule = self.bridge.rule.as_str(),
            sent = carried.sent,
            received = carried.received,
            refused = carried.refused,
            "this connection is over"
        );
        ended
    }

    /// A connection was established: remember what it settled.
    ///
    /// The line saying so, and the state topic, are the session's:
    /// "connected" is only true while the session has not returned, and
    /// this runs after it has.
    fn settled(&mut self, negotiated: &Negotiated) {
        // **On the session, not on the socket.** A remote that accepts
        // a connection and then refuses the CONNECT is a remote that is
        // up and saying no, and a backoff reset on the socket would ask
        // it again every `reconnect_min_ms` for as long as it stood.
        self.backoff.succeeded();
        self.connected_before = true;
        // The identifier the remote assigned, so that the next
        // connection resumes the session it kept rather than being
        // given a new one.
        self.client_id.clone_from(&negotiated.client_id);
    }

    /// Open a socket, and wrap it in TLS if the address asks for it.
    async fn open(&self) -> Result<Stream, String> {
        let authority = self.address.authority();
        let tcp = match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&authority)).await
        {
            Err(_) => {
                return Err(format!(
                    "the remote at {authority} did not accept a connection within {}s",
                    CONNECT_TIMEOUT.as_secs()
                ))
            }
            Ok(Err(error)) => return Err(format!("{authority}: {error}")),
            Ok(Ok(tcp)) => tcp,
        };
        // A bridge's traffic is small packets that matter as soon as
        // they exist, which is the case Nagle's algorithm is wrong
        // for: it would hold an acknowledgement waiting for a second
        // packet that is not coming.
        let _ = tcp.set_nodelay(true);

        if !self.address.tls {
            return Ok(Stream::Plain(tcp));
        }

        let name = tokio_rustls::rustls::pki_types::ServerName::try_from(self.address.host.clone())
            .map_err(|error| {
                format!(
                    "{} is not a name TLS can verify: {error}",
                    self.address.host
                )
            })?;
        let connector = tokio_rustls::TlsConnector::from(tls_config());
        let tls = tokio::time::timeout(CONNECT_TIMEOUT, connector.connect(name, tcp))
            .await
            .map_err(|_| {
                format!(
                    "the TLS handshake with {authority} did not finish within {}s",
                    CONNECT_TIMEOUT.as_secs()
                )
            })?
            .map_err(|error| format!("the TLS handshake with {authority} failed: {error}"))?;
        Ok(Stream::Tls(Box::new(tls)))
    }
}

/// Either kind of socket.
///
/// An enum rather than a boxed trait object, so that the session's
/// generic parameter is a concrete type on both arms and neither is
/// paying for dynamic dispatch on every packet.
enum Stream {
    /// Plain TCP, which [`address`] permits only to this machine.
    Plain(TcpStream),
    /// TLS over TCP.
    ///
    /// Boxed: a rustls connection is large, and this enum is held
    /// across an await for the life of a connection.
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

/// The client configuration every bridge shares.
///
/// Built once. The roots are compiled in rather than read from the
/// operating system, for the reason `http_out::client` gives: a
/// distroless image has no trust store to read, and this broker is
/// built to run in one. The same provider and the same roots as the
/// HTTP client, so the binary links one of each.
fn tls_config() -> Arc<tokio_rustls::rustls::ClientConfig> {
    use std::sync::OnceLock;
    static CONFIG: OnceLock<Arc<tokio_rustls::rustls::ClientConfig>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let roots = tokio_rustls::rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };
            Arc::new(
                tokio_rustls::rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            )
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_names_a_host_a_port_and_whether_it_is_encrypted() {
        assert_eq!(
            address("mqtts://broker.example.com:8883").expect("a tls address"),
            Address {
                host: "broker.example.com".to_owned(),
                port: 8883,
                tls: true
            }
        );
        assert_eq!(
            address("mqtt://127.0.0.1:1883").expect("plain text to this machine"),
            Address {
                host: "127.0.0.1".to_owned(),
                port: 1883,
                tls: false
            }
        );
    }

    #[test]
    fn a_missing_port_takes_the_default_for_its_scheme() {
        assert_eq!(address("mqtts://broker.example.com").unwrap().port, 8883);
        assert_eq!(address("mqtt://localhost").unwrap().port, 1883);
    }

    /// A bridge sends a password in its connect and then everything it
    /// carries. The same refusal `http_out` makes of a plain-text
    /// endpoint, with more at stake.
    #[test]
    fn plain_text_to_anywhere_but_this_machine_is_refused() {
        for off_host in [
            "mqtt://broker.example.com:1883",
            "mqtt://10.0.0.7:1883",
            "mqtt://[2001:db8::1]:1883",
        ] {
            assert!(
                matches!(
                    address(off_host).expect_err("plain text off this host"),
                    AddressError::PlainTextOffHost { .. }
                ),
                "{off_host} should be refused"
            );
        }

        for on_host in [
            "mqtt://localhost:1883",
            "mqtt://LOCALHOST:1883",
            "mqtt://127.0.0.1:1883",
            "mqtt://127.9.9.9:1883",
            "mqtt://[::1]:1883",
        ] {
            address(on_host).unwrap_or_else(|error| panic!("{on_host} should be allowed: {error}"));
        }
    }

    /// A name that resolves to a loopback address is deliberately not
    /// accepted. A check that can be true when it is made and false
    /// when it matters is not a check.
    #[test]
    fn a_name_that_merely_resolves_to_loopback_is_not_this_machine() {
        assert!(matches!(
            address("mqtt://localhost.localdomain:1883").expect_err("a name, not the name"),
            AddressError::PlainTextOffHost { .. }
        ));
    }

    /// Either guess about a bare address is wrong half the time, and
    /// one of the two wrong guesses sends a password in the clear.
    #[test]
    fn an_address_with_no_scheme_is_refused_rather_than_guessed_at() {
        for bad in ["broker.example.com:1883", "1883", "", "https://a:1"] {
            assert!(
                matches!(
                    address(bad).expect_err("not an address this bridge speaks"),
                    AddressError::Scheme { .. }
                ),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_port_that_is_not_one_is_refused() {
        for bad in [
            "mqtts://a.example.com:0",
            "mqtts://a.example.com:70000",
            "mqtts://a.example.com:http",
        ] {
            assert!(
                matches!(
                    address(bad).expect_err("not a port"),
                    AddressError::Port { .. }
                ),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn an_ipv6_literal_keeps_its_colons() {
        let parsed = address("mqtts://[2001:db8::1]:8883").expect("an ipv6 address");
        assert_eq!(parsed.host, "2001:db8::1");
        assert_eq!(parsed.port, 8883);

        let default_port = address("mqtts://[2001:db8::1]").expect("an ipv6 address");
        assert_eq!(default_port.host, "2001:db8::1");
        assert_eq!(default_port.port, 8883);
    }

    #[test]
    fn an_address_with_no_host_is_refused() {
        assert!(matches!(
            address("mqtts://:8883").expect_err("no host"),
            AddressError::NoHost { .. }
        ));
    }
}
