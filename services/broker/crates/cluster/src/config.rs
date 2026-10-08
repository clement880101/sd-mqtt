//! What a cluster will not start without.
//!
//! Section 8.1: **a node with no secret configured refuses to cluster
//! at all**, and an empty secret is refused rather than read as absent.
//!
//! # There is no loopback exemption, and that is the difference
//!
//! The admin listener has one: it binds loopback by default and a
//! token is only required once it binds further, because a broker with
//! no configuration at all has to work and nothing reaches loopback
//! that is not already on the machine. A cluster has no such case. A
//! cluster needs seeds written down before it exists, so there is no
//! zero-configuration cluster to preserve — and a link grants session
//! takeover and unauthenticated writes to every replicated register,
//! which is a larger grant than the admin API's.
//!
//! So the refusal here is not about an address and there is no address
//! in it. [`ClusterConfig`] cannot be constructed without a secret, in
//! any circumstance, from any source.
//!
//! # The cluster name is bound into the handshake
//!
//! It is not decoration either: binding it stops a staging node
//! configured with a production secret from joining production. See
//! [`crate::handshake`], where it goes into the transcript.

use storage::Secret;

/// What a cluster is called when nothing says otherwise.
pub const DEFAULT_CLUSTER_NAME: &str = "sd-mqtt";

/// What an operator wrote.
///
/// The unchecked form, which is what a configuration file parses into:
/// it can hold the mistakes [`ClusterSettings::check`] refuses, because
/// a value that cannot be represented cannot be refused by name.
///
/// **An absent secret is `None` and an empty one is `Some("")`.** They
/// are different mistakes with different messages, and collapsing them
/// is the defect the admin listener's token had: an operator whose
/// environment variable expanded to nothing was told the thing was
/// guarded by something that had quietly decided it was not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSettings {
    /// What this cluster is called.
    pub name: String,
    /// The secret every node in it shares.
    pub secret: Option<Secret>,
}

impl Default for ClusterSettings {
    /// A cluster name and no secret, which does not cluster.
    ///
    /// The default is deliberately not a working cluster. There is
    /// nothing to preserve: see the module documentation.
    fn default() -> Self {
        Self {
            name: DEFAULT_CLUSTER_NAME.to_owned(),
            secret: None,
        }
    }
}

impl ClusterSettings {
    /// Refuse a configuration a cluster cannot start from.
    ///
    /// # Errors
    ///
    /// [`ConfigError::NoSecret`] when nothing was configured,
    /// [`ConfigError::EmptySecret`] when the key is there and blank,
    /// and [`ConfigError::EmptyName`] when the cluster has no name to
    /// bind into a handshake. Three causes, three messages: an
    /// operator's next move differs for each.
    pub fn check(&self) -> Result<ClusterConfig, ConfigError> {
        let Some(secret) = &self.secret else {
            return Err(ConfigError::NoSecret);
        };
        if secret.is_empty() {
            return Err(ConfigError::EmptySecret);
        }
        if self.name.is_empty() {
            return Err(ConfigError::EmptyName);
        }
        Ok(ClusterConfig {
            name: self.name.clone(),
            secret: secret.clone(),
        })
    }
}

/// A configuration a cluster can start from.
///
/// **Constructible only through [`ClusterSettings::check`]**, so
/// holding one of these is the proof that a secret was configured and
/// is not empty. That is the whole design: "refuses to cluster without
/// a secret" is not a check somewhere in the startup path that a later
/// caller might route around, it is the absence of a way to say it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    /// What this cluster is called. Bound into every handshake.
    name: String,
    /// The secret every node in it shares. Never sent.
    secret: Secret,
}

impl ClusterConfig {
    /// What this cluster is called.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The secret every node in it shares.
    ///
    /// **Never sent.** Section 8.1: wrapping the link in TLS is
    /// optional, so a handshake that sent the secret would hand it to a
    /// passive observer for ever. It is used to key a hash and for
    /// nothing else; see [`crate::handshake`].
    #[must_use]
    pub fn secret(&self) -> &Secret {
        &self.secret
    }
}

/// Why a cluster will not start.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    /// No secret was configured at all.
    #[error(
        "clustering needs cluster.secret, and none is configured. There is no \
         unauthenticated mode and no loopback exemption: a link grants session \
         takeover and unauthenticated writes to every replicated register. Set \
         cluster.secret or SDMQTT_CLUSTER_SECRET to the same value on every node."
    )]
    NoSecret,
    /// The key is there, and it is the empty string.
    ///
    /// Its own cause rather than being read as absent. The message says
    /// *empty* in as many words, because the operator who reaches this
    /// one has written the key and expanded something to nothing, and
    /// telling them it is missing sends them to look in the wrong file.
    #[error(
        "cluster.secret is an empty string, which protects nothing. It is refused \
         for being empty rather than read as absent: something was set and \
         expanded to nothing. Give every node the same non-empty value."
    )]
    EmptySecret,
    /// The cluster has no name.
    ///
    /// The name is bound into every handshake, so an empty one is a
    /// cluster that cannot be told apart from another empty one — which
    /// is the staging node joining production that binding it exists to
    /// stop.
    #[error(
        "cluster.name is an empty string. The name is bound into every handshake, \
         so that a node configured for one cluster cannot join another; leave the \
         key out to take the default."
    )]
    EmptyName,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret(text: &str) -> Option<Secret> {
        Some(Secret::new(text))
    }

    /// No secret, no clustering. The first of the three.
    #[test]
    fn a_configuration_with_no_secret_is_refused_as_missing() {
        let error = ClusterSettings {
            name: "production".to_owned(),
            secret: None,
        }
        .check()
        .expect_err("a cluster with no secret must not start");

        assert_eq!(error, ConfigError::NoSecret);
        let said = error.to_string();
        assert!(said.contains("cluster.secret"), "{said}");
        assert!(
            said.contains("loopback"),
            "an operator who expected the admin listener's exemption has to be \
             told there is not one here: {said}"
        );
    }

    /// An empty secret is refused **for being empty**. The second.
    ///
    /// The distinct message is the point. Read as absent it would send
    /// an operator whose variable expanded to nothing looking for a key
    /// they have already written.
    #[test]
    fn an_empty_secret_is_refused_for_being_empty_and_not_as_absent() {
        let error = ClusterSettings {
            name: "production".to_owned(),
            secret: secret(""),
        }
        .check()
        .expect_err("an empty secret protects nothing");

        assert_eq!(error, ConfigError::EmptySecret);
        assert_ne!(
            error,
            ConfigError::NoSecret,
            "the two mistakes have different answers and must not collapse"
        );
        let said = error.to_string();
        assert!(said.contains("empty"), "{said}");
        assert_ne!(
            said,
            ConfigError::NoSecret.to_string(),
            "and they must not read the same either"
        );
    }

    /// A secret that is there and is not empty starts a cluster. The
    /// third.
    #[test]
    fn a_secret_that_is_there_is_taken() {
        let config = ClusterSettings {
            name: "production".to_owned(),
            secret: secret("a-shared-secret"),
        }
        .check()
        .expect("this is a usable configuration");

        assert_eq!(config.name(), "production");
        assert_eq!(config.secret().expose(), "a-shared-secret");
    }

    /// The default configuration does not cluster.
    ///
    /// Section 8.1 in one assertion: there is no zero-configuration
    /// cluster, so the configuration a node has when nothing was said
    /// must refuse rather than start something unauthenticated.
    #[test]
    fn the_default_configuration_does_not_cluster() {
        assert_eq!(
            ClusterSettings::default().check(),
            Err(ConfigError::NoSecret)
        );
        assert_eq!(ClusterSettings::default().name, DEFAULT_CLUSTER_NAME);
    }

    /// A cluster with no name is refused too.
    #[test]
    fn a_cluster_with_no_name_is_refused() {
        assert_eq!(
            ClusterSettings {
                name: String::new(),
                secret: secret("a-shared-secret"),
            }
            .check(),
            Err(ConfigError::EmptyName)
        );
    }

    /// The secret does not print itself, wherever it is printed from.
    ///
    /// Section 12.3. A configuration is exactly the thing a startup
    /// logs, and `#[derive(Debug)]` on the struct around it would print
    /// whatever the field prints.
    #[test]
    fn a_configuration_does_not_print_its_secret() {
        let config = ClusterSettings {
            name: "production".to_owned(),
            secret: secret("a-shared-secret"),
        }
        .check()
        .expect("usable");

        let shown = format!("{config:?}");
        assert!(
            !shown.contains("a-shared-secret"),
            "the secret reached a formatter: {shown}"
        );
        let settings = format!(
            "{:?}",
            ClusterSettings {
                name: "production".to_owned(),
                secret: secret("a-shared-secret"),
            }
        );
        assert!(!settings.contains("a-shared-secret"), "{settings}");
    }
}
