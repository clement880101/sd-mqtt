//! What the admin listener is configured with, and what it refuses.
//!
//! # The listener is protected by where it listens
//!
//! Section 11.1: until milestone 7 there are no API keys. What stands
//! in for them is the address. The listener binds loopback by default,
//! a static bearer token may be configured, and mutating requests need
//! it when one is set.
//!
//! The token is an interim and the configuration says so. It is one
//! secret with no scopes, no rotation and no per-user identity, which
//! is precisely what milestone 7 replaces.

use std::fmt;
use std::net::{IpAddr, SocketAddr};

use serde::Deserialize;

/// The port the admin listener binds by default.
///
/// Adjacent to the registered MQTT port so that an operator who knows
/// one knows the other, and not a port any common tool claims. It is
/// not registered for this and does not need to be: it is bound on
/// loopback by default, and an operator who exposes it is configuring
/// it anyway.
pub const DEFAULT_PORT: u16 = 1884;

/// A secret that will not print itself.
///
/// Section 12.3: passwords and tokens are held in a wrapper that
/// refuses to print itself, so they cannot reach logs or crash
/// output. The whole configuration derives [`Debug`] and is logged at
/// startup, so without this the token reaches the first log line the
/// broker writes.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct Token(String);

impl Token {
    /// A token from the text an operator configured.
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }

    /// The secret itself, for the one comparison that needs it.
    ///
    /// Deliberately not `Display` and deliberately not `AsRef<str>`:
    /// the only way to get the characters out is to name this method,
    /// so a token cannot reach a formatter by accident.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Whether an operator configured the key and left it blank.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Not the length either. A length is a real, if small, hint
        // about a secret, and nothing debugging a configuration needs
        // it.
        f.write_str("Token(<redacted>)")
    }
}

/// The admin listener's configuration.
///
/// Deserialised straight out of the binary's `[admin]` section rather
/// than copied into a second struct there, so the defaults and the
/// refusal below cannot disagree with what the file is read into.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct AdminConfig {
    /// Whether to start it at all.
    ///
    /// Starting the admin listener is not optional in the sense that
    /// it needs no feature and no flag to exist, but an operator may
    /// turn it off.
    pub enabled: bool,
    /// Where to bind.
    pub address: SocketAddr,
    /// The interim bearer token, if one is configured.
    ///
    /// **Temporary.** Milestone 7 replaces it with API keys that have
    /// scopes, rotation and an identity behind them. It exists so that
    /// milestone 3 can be used and demonstrated without shipping
    /// something unprotected.
    pub token: Option<Token>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            address: SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), DEFAULT_PORT),
            token: None,
        }
    }
}

/// Why an admin listener would not start.
#[derive(Debug, thiserror::Error)]
pub enum AdminStartError {
    /// The address reaches further than loopback and no token is set.
    ///
    /// Refused, not warned about. See [`AdminConfig::check`].
    #[error(
        "refusing to start the admin listener on {address}: that address is reachable from \
         off this host and no admin.token is configured. The admin API publishes, so an \
         unauthenticated listener on it is a hole rather than a degraded mode. Set \
         admin.token (or SDMQTT_ADMIN_TOKEN), or bind a loopback address such as \
         127.0.0.1:{port}."
    )]
    Unprotected {
        /// The address that was asked for.
        address: SocketAddr,
        /// The default port, so the suggested address is a whole one.
        port: u16,
    },
    /// A token was configured, and it is empty.
    ///
    /// Refused rather than read as absent. An empty token satisfies
    /// "a token is configured" while protecting nothing, so a
    /// listener reachable off this host would start believing itself
    /// guarded. Reading it as absent instead would be the same
    /// reinterpretation of a value that cannot mean what it says
    /// that `broker.max_inflight = 0` is already refused for.
    #[error(
        "refusing to start the admin listener: admin.token is set to an empty string, which \
         protects nothing while satisfying the check that a token exists. Set a real token, \
         or remove the key and bind a loopback address."
    )]
    EmptyToken,
    /// The socket could not be bound, which is usually something else
    /// already on the port.
    #[error("cannot bind the admin listener to {address}: {source}")]
    Bind {
        /// The address that was asked for.
        address: SocketAddr,
        /// Why not.
        #[source]
        source: std::io::Error,
    },
}

impl AdminConfig {
    /// Refuse a configuration that cannot mean what it says.
    ///
    /// Section 11.1: binding a non-loopback address with no token
    /// configured is refused **at startup**, and the refusal names
    /// both the address and the missing key.
    ///
    /// # What a wildcard address is
    ///
    /// `0.0.0.0` and `[::]` are neither loopback nor a named
    /// interface: they are *every* interface, loopback and the public
    /// ones together. So they are the most exposed thing an operator
    /// can write, not a special case between the two, and they are
    /// refused without a token exactly as a routable address is.
    ///
    /// This is what the obvious guard already does — [`IpAddr::
    /// is_loopback`] is false for an unspecified address — but it is
    /// the case a reader will wonder about, so it is named here and
    /// has a test of its own rather than being left to be inferred.
    ///
    /// # And what an IPv4-mapped loopback address is
    ///
    /// `::ffff:127.0.0.1` is loopback traffic wearing an IPv6
    /// spelling, and [`Ipv6Addr::is_loopback`] says `false` for it:
    /// only `::1` is loopback to that method. Left alone, an operator
    /// who wrote a perfectly safe address would be told it is a hole.
    /// So the address is canonicalised first, which maps an
    /// IPv4-mapped address back to the IPv4 address it carries, and
    /// the question is asked of that.
    ///
    /// The canonicalisation is safe in the other direction too:
    /// `::ffff:0.0.0.0` canonicalises to `0.0.0.0` and is refused,
    /// and `::ffff:203.0.113.1` to `203.0.113.1` and is refused.
    ///
    /// [`Ipv6Addr::is_loopback`]: std::net::Ipv6Addr::is_loopback
    pub fn check(&self) -> Result<(), AdminStartError> {
        // Asked before the address, because an empty token is a
        // mistake wherever it is written. On loopback it protects
        // nothing and nothing needed protecting, which is merely
        // pointless; off it, it is the difference between guarded and
        // open. One answer for both is easier to predict than two.
        if self.token.as_ref().is_some_and(|token| token.is_empty()) {
            return Err(AdminStartError::EmptyToken);
        }
        if self.token.is_some() {
            return Ok(());
        }
        if self.address.ip().to_canonical().is_loopback() {
            return Ok(());
        }
        Err(AdminStartError::Unprotected {
            address: self.address,
            port: DEFAULT_PORT,
        })
    }
}
