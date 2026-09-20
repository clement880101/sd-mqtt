//! The configuration file, the environment and the command line.
//!
//! Three sources, layered by hand in that order: the file is read, the
//! environment overrides what it said, and a flag overrides both. The
//! design notes that thirty lines of this beats a configuration
//! framework's thirteen dependencies, and this is the thirty lines.
//!
//! # An unknown key is an error
//!
//! [`serde`]'s `deny_unknown_fields` rather than the usual silent
//! ignore. A misspelled listener address that is quietly dropped leaves
//! the broker on its default port, and an operator reads the absence of
//! a complaint as confirmation. The same argument makes an unreadable
//! environment value an error rather than a fallback.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use broker_core::broker::BrokerConfig;
use mqtt_codec::Limits;
use sdmqtt_server::connection::{outbox_depth_for, ConnectionConfig};
use sdmqtt_server::listener::{ServerConfig, DEFAULT_MAX_CONNECTIONS, DEFAULT_PORT};
use serde::Deserialize;
use thiserror::Error;

/// Where the configuration file is looked for when none is named.
pub const DEFAULT_PATH: &str = "sdmqtt.toml";

/// The environment variable naming the configuration file.
pub const PATH_VARIABLE: &str = "SDMQTT_CONFIG";

/// Why a configuration could not be assembled.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The file was named but could not be read. A file the operator
    /// asked for and did not get is a failure, not a fallback.
    #[error("cannot read {path}: {source}")]
    Unreadable {
        /// The file that could not be read.
        path: PathBuf,
        /// Why not.
        #[source]
        source: std::io::Error,
    },
    /// The file is not valid, which includes naming a key we do not
    /// know.
    #[error("{path} is not a valid configuration: {source}")]
    Invalid {
        /// The file the error is in.
        path: PathBuf,
        /// What the parser said, which names the offending key.
        #[source]
        source: toml::de::Error,
    },
    /// An environment variable could not be read as what it configures.
    #[error("{variable}={value:?} is not a valid {expected}")]
    Environment {
        /// The variable at fault.
        variable: &'static str,
        /// What it was set to.
        value: String,
        /// What it should have been.
        expected: &'static str,
    },
    /// A key parsed as its own type but cannot mean what it says.
    ///
    /// Separate from `Invalid` because the parser is content: the
    /// number is a number and the file is well formed. What is wrong is
    /// the meaning, and only the broker knows that.
    #[error("{key} cannot be {value}: {why}")]
    Unusable {
        /// The key at fault, written as it appears in the file.
        key: &'static str,
        /// What it was set to.
        value: String,
        /// Why that cannot work.
        why: &'static str,
    },
}

/// How much to log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum LogLevel {
    /// Only what has gone wrong.
    Error,
    /// And what might be about to.
    Warn,
    /// And the handful of lines an operator wants: what bound, what
    /// stopped.
    #[default]
    Info,
    /// And one line per connection.
    Debug,
    /// Everything.
    Trace,
}

impl LogLevel {
    /// The level to hand a subscriber.
    pub fn to_level(self) -> tracing::Level {
        match self {
            LogLevel::Error => tracing::Level::ERROR,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Trace => tracing::Level::TRACE,
        }
    }
}

impl std::str::FromStr for LogLevel {
    type Err = &'static str;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "error" => Ok(LogLevel::Error),
            "warn" => Ok(LogLevel::Warn),
            "info" => Ok(LogLevel::Info),
            "debug" => Ok(LogLevel::Debug),
            "trace" => Ok(LogLevel::Trace),
            _ => Err("one of error, warn, info, debug or trace"),
        }
    }
}

/// The listener section.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Listener {
    /// Where to bind.
    pub address: SocketAddr,
    /// How many connections to serve at once.
    pub max_connections: usize,
    /// The namespace this listener's clients live in. Two clients with
    /// the same identifier on two mountpoints are two sessions.
    pub mountpoint: String,
}

impl Default for Listener {
    fn default() -> Self {
        Self {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEFAULT_PORT),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            mountpoint: String::new(),
        }
    }
}

/// The broker section.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BrokerSection {
    /// Largest whole packet, header included, in bytes.
    pub max_packet_size: u32,
    /// Largest number of levels in a topic name or filter.
    pub max_topic_depth: u16,
    /// Largest client identifier length in bytes.
    pub max_client_id_len: u16,
    /// How many shards the subscription trie uses.
    pub trie_shards: usize,
    /// The largest keep-alive interval a client may have. A version 5
    /// client proposing more is clamped to this and told so in the
    /// connack.
    ///
    /// Zero disables the ceiling, and does no more than that: clients
    /// still keep alive, still propose their own intervals and are
    /// still disconnected when they fall silent past one. In
    /// particular it does not shrink the ring the broker sweeps them
    /// with, which is sized for at least a minute's interval however
    /// small this is.
    pub max_keep_alive: u16,
    /// The largest inflight window a session may have. A session's
    /// window is the lesser of this and what the client advertises,
    /// and one gives strict ordering.
    pub max_inflight: u16,
    /// How many messages a session's queue may hold waiting for a
    /// window slot.
    pub max_queue_messages: usize,
    /// How many bytes those messages may occupy in total. The one knob
    /// that bounds per-session memory.
    pub max_queue_bytes: usize,
}

impl Default for BrokerSection {
    fn default() -> Self {
        let limits = Limits::default();
        let broker = BrokerConfig::default();
        Self {
            max_packet_size: limits.max_packet_size,
            max_topic_depth: limits.max_topic_depth,
            max_client_id_len: limits.max_client_id_len,
            trie_shards: broker.trie_shards,
            max_keep_alive: broker.max_keep_alive,
            max_inflight: broker.max_inflight,
            max_queue_messages: broker.max_queue_messages,
            max_queue_bytes: broker.max_queue_bytes,
        }
    }
}

/// The log section.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Log {
    /// How much to log.
    pub level: LogLevel,
}

/// What the three sources add up to.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// The listener section.
    pub listener: Listener,
    /// The broker section.
    pub broker: BrokerSection,
    /// The log section.
    pub log: Log,
}

/// What the command line says, where it says anything.
///
/// `None` is "not given" rather than a value, which is what makes a
/// flag able to override a file without every flag having to restate
/// every default.
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    /// Where to bind.
    pub address: Option<SocketAddr>,
    /// How many connections to serve at once.
    pub max_connections: Option<usize>,
    /// The listener's namespace.
    pub mountpoint: Option<String>,
    /// How much to log.
    pub log_level: Option<LogLevel>,
}

impl Config {
    /// Read a configuration from the text of a file.
    pub fn from_toml(text: &str) -> Result<Self, ConfigError> {
        Self::parse(text, Path::new(DEFAULT_PATH))
    }

    fn parse(text: &str, path: &Path) -> Result<Self, ConfigError> {
        toml::from_str(text).map_err(|source| ConfigError::Invalid {
            path: path.to_path_buf(),
            source,
        })
    }

    /// The three sources, in order.
    ///
    /// A file named explicitly must exist; the default one need not,
    /// because the product's first promise is that no configuration is
    /// a working configuration.
    pub fn load(path: Option<&Path>, overrides: &Overrides) -> Result<Self, ConfigError> {
        let named = path.map(Path::to_path_buf).or_else(|| {
            std::env::var(PATH_VARIABLE)
                .ok()
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        });

        let mut config = match named {
            Some(path) => {
                let text =
                    std::fs::read_to_string(&path).map_err(|source| ConfigError::Unreadable {
                        path: path.clone(),
                        source,
                    })?;
                Self::parse(&text, &path)?
            }
            None => match std::fs::read_to_string(DEFAULT_PATH) {
                Ok(text) => Self::from_toml(&text)?,
                Err(_) => Self::default(),
            },
        };

        config.apply_env(|variable| std::env::var(variable).ok())?;
        config.apply(overrides);
        config.validate()?;
        Ok(config)
    }

    /// Refuse a configuration that parses but cannot mean what it says.
    ///
    /// Run after the environment and the command line, so that a value
    /// from any of the three sources is held to the same rule.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Zero is the sentinel the inflight window uses internally for
        // "unbounded", so an operator who writes it gets the opposite of
        // what the word suggests: not a window of nothing, but no window
        // at all, overriding even a client that asked for a strict one.
        // The shell takes the lesser of this and the client's Receive
        // Maximum, and the lesser of zero and anything is zero.
        //
        // Refused rather than reinterpreted. The sibling `max_keep_alive`
        // documents zero as "no ceiling", and either meaning is arguable
        // here, which is the argument for making the operator say which
        // they meant instead of guessing.
        if self.broker.max_inflight == 0 {
            return Err(ConfigError::Unusable {
                key: "broker.max_inflight",
                value: "0".to_string(),
                why: "a window of zero would send nothing; write 1 for strict ordering",
            });
        }
        if self.broker.max_queue_messages == 0 {
            return Err(ConfigError::Unusable {
                key: "broker.max_queue_messages",
                value: "0".to_string(),
                why: "a queue that holds nothing drops every message an offline session is sent",
            });
        }
        // The same failure through the other bound. A message's size in
        // the queue is never zero, so a byte budget of zero refuses
        // every one of them, exactly as a message count of zero does.
        if self.broker.max_queue_bytes == 0 {
            return Err(ConfigError::Unusable {
                key: "broker.max_queue_bytes",
                value: "0".to_string(),
                why: "a queue that holds nothing drops every message an offline session is sent",
            });
        }

        // The relation between the queue and the channel that drains
        // it. A session's writer reads one outbox, and the broker can
        // hand the shell a whole queue at once: `Outbound::pump` sends
        // while the window has room and an at-most-once dispatch
        // consumes no slot, so one acknowledgement empties the queue
        // into a single batch. Whatever does not fit is discarded
        // against a counter, with no reason and no `Action::Dropped`.
        //
        // Stated here as well as derived in `server`, because the
        // derivation is what a later change can quietly undo: a
        // configuration whose bounds outgrow their outbox is refused at
        // startup rather than silently lossy in production.
        let Some(required) =
            outbox_depth_for(self.broker.max_queue_messages, self.broker.max_inflight)
        else {
            return Err(ConfigError::Unusable {
                key: "broker.max_queue_messages",
                value: self.broker.max_queue_messages.to_string(),
                why: "too large for any outbox to hold one drain of the queue",
            });
        };
        if self.server().connection.outbox_depth < required {
            return Err(ConfigError::Unusable {
                key: "broker.max_queue_messages",
                value: self.broker.max_queue_messages.to_string(),
                why: "the outbox the shell would build cannot hold one drain of a queue this size",
            });
        }
        Ok(())
    }

    /// Let the environment override what the file said.
    ///
    /// The lookup is a parameter so that a test can layer the sources
    /// without touching the process environment, which every other test
    /// in the binary shares.
    pub fn apply_env(
        &mut self,
        lookup: impl Fn(&'static str) -> Option<String>,
    ) -> Result<(), ConfigError> {
        if let Some(value) = lookup("SDMQTT_ADDRESS") {
            self.listener.address = parse("SDMQTT_ADDRESS", &value, "socket address")?;
        }
        if let Some(value) = lookup("SDMQTT_MAX_CONNECTIONS") {
            self.listener.max_connections = parse("SDMQTT_MAX_CONNECTIONS", &value, "count")?;
        }
        if let Some(value) = lookup("SDMQTT_MOUNTPOINT") {
            self.listener.mountpoint = value;
        }
        if let Some(value) = lookup("SDMQTT_LOG") {
            self.log.level = parse("SDMQTT_LOG", &value, "log level")?;
        }
        Ok(())
    }

    /// Let the command line override the other two.
    pub fn apply(&mut self, overrides: &Overrides) {
        if let Some(address) = overrides.address {
            self.listener.address = address;
        }
        if let Some(max_connections) = overrides.max_connections {
            self.listener.max_connections = max_connections;
        }
        if let Some(mountpoint) = &overrides.mountpoint {
            self.listener.mountpoint.clone_from(mountpoint);
        }
        if let Some(level) = overrides.log_level {
            self.log.level = level;
        }
    }

    /// What the listener should be started with.
    pub fn server(&self) -> ServerConfig {
        let limits = Limits {
            max_packet_size: self.broker.max_packet_size,
            max_topic_depth: self.broker.max_topic_depth,
            max_client_id_len: self.broker.max_client_id_len,
        };
        ServerConfig {
            address: self.listener.address,
            broker: BrokerConfig {
                limits,
                trie_shards: self.broker.trie_shards,
                max_keep_alive: self.broker.max_keep_alive,
                max_inflight: self.broker.max_inflight,
                max_queue_messages: self.broker.max_queue_messages,
                max_queue_bytes: self.broker.max_queue_bytes,
            },
            connection: ConnectionConfig {
                limits,
                mountpoint: Arc::from(self.listener.mountpoint.as_str()),
                // Derived from the bounds it drains rather than left a
                // constant, so raising the queue raises the channel the
                // queue empties into. `validate` refuses a configuration
                // this cannot be sized for, so the fallback is
                // unreachable on anything `load` returns.
                outbox_depth: outbox_depth_for(
                    self.broker.max_queue_messages,
                    self.broker.max_inflight,
                )
                .unwrap_or(usize::MAX),
                ..ConnectionConfig::default()
            },
            max_connections: self.listener.max_connections,
        }
    }
}

/// Read one environment value, naming it when it cannot be read.
fn parse<T: std::str::FromStr>(
    variable: &'static str,
    value: &str,
    expected: &'static str,
) -> Result<T, ConfigError> {
    value.parse().map_err(|_| ConfigError::Environment {
        variable,
        value: value.to_owned(),
        expected,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_file_produces_working_defaults() {
        let config = Config::from_toml("").expect("an empty file is a valid one");
        assert_eq!(config, Config::default());
        assert_eq!(
            config.listener.address.port(),
            1883,
            "the registered MQTT port"
        );
        assert_eq!(
            config.server().address,
            config.listener.address,
            "and the defaults reach the server"
        );
    }

    #[test]
    fn every_field_can_be_overridden() {
        let text = r#"
            [listener]
            address = "127.0.0.1:18830"
            max_connections = 7
            mountpoint = "tenant-a"

            [broker]
            max_packet_size = 4096
            max_topic_depth = 4
            max_client_id_len = 23
            trie_shards = 2
            max_keep_alive = 60
            max_inflight = 3
            max_queue_messages = 5
            max_queue_bytes = 8192

            [log]
            level = "debug"
        "#;
        let config = Config::from_toml(text).expect("every key is known");

        assert_eq!(config.listener.address.to_string(), "127.0.0.1:18830");
        assert_eq!(config.listener.max_connections, 7);
        assert_eq!(config.listener.mountpoint, "tenant-a");
        assert_eq!(config.broker.max_packet_size, 4096);
        assert_eq!(config.broker.max_topic_depth, 4);
        assert_eq!(config.broker.max_client_id_len, 23);
        assert_eq!(config.broker.trie_shards, 2);
        assert_eq!(config.broker.max_keep_alive, 60);
        assert_eq!(config.broker.max_inflight, 3);
        assert_eq!(config.broker.max_queue_messages, 5);
        assert_eq!(config.broker.max_queue_bytes, 8192);
        assert_eq!(config.log.level, LogLevel::Debug);

        // Parsed is not the same as applied. A field read into this
        // struct and then never handed to the server is a setting that
        // does nothing, which is worse than one that was never offered.
        let server = config.server();
        assert_eq!(server.address.to_string(), "127.0.0.1:18830");
        assert_eq!(server.max_connections, 7);
        assert_eq!(&*server.connection.mountpoint, "tenant-a");
        assert_eq!(server.broker.limits.max_packet_size, 4096);
        assert_eq!(server.broker.limits.max_topic_depth, 4);
        assert_eq!(server.broker.limits.max_client_id_len, 23);
        assert_eq!(server.broker.trie_shards, 2);
        assert_eq!(server.broker.max_keep_alive, 60);
        assert_eq!(server.broker.max_inflight, 3);
        assert_eq!(server.broker.max_queue_messages, 5);
        assert_eq!(server.broker.max_queue_bytes, 8192);
        assert_eq!(server.connection.limits, server.broker.limits);
    }

    #[test]
    fn the_documented_defaults_are_the_literals_they_replace() {
        // Written against the numbers rather than against the
        // constants they now live in, because the point is that moving
        // a literal into configuration cannot quietly change it. An
        // assertion spelled `DEFAULT_MAX_INFLIGHT` would hold whatever
        // that became.
        let config = Config::default();
        assert_eq!(
            config.broker.max_inflight, 20,
            "section 6: the inflight window defaults to 20"
        );

        assert_eq!(
            config.broker.max_queue_messages, 1000,
            "section 6: a session queues a thousand messages"
        );
        assert_eq!(
            config.broker.max_queue_bytes,
            64 * 1024 * 1024,
            "and 64 MiB of them"
        );

        let server = config.server();
        assert_eq!(
            server.broker.max_inflight, 20,
            "and the default reaches the broker"
        );
        assert_eq!(server.broker.max_queue_messages, 1000);
        assert_eq!(server.broker.max_queue_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn a_misspelled_key_is_an_error_rather_than_a_default() {
        // The failure this prevents: a typo in the listener address
        // leaves the broker on its default port, which looks like it
        // worked right up until someone tries to connect.
        let error = Config::from_toml("[listener]\nadress = \"127.0.0.1:1883\"\n")
            .expect_err("an unknown key is refused");
        let message = error.to_string();
        assert!(message.contains("adress"), "the key is named: {message}");
    }

    #[test]
    fn a_misspelled_section_is_an_error_too() {
        let error = Config::from_toml("[listner]\naddress = \"127.0.0.1:1883\"\n")
            .expect_err("an unknown section is refused");
        let message = error.to_string();
        assert!(
            message.contains("listner"),
            "the section is named: {message}"
        );
    }

    #[test]
    fn an_environment_variable_overrides_the_file() {
        let mut config = Config::from_toml("[listener]\naddress = \"127.0.0.1:1\"\n").unwrap();
        config
            .apply_env(|key| (key == "SDMQTT_ADDRESS").then(|| "127.0.0.1:2".to_owned()))
            .unwrap();
        assert_eq!(config.listener.address.port(), 2);
    }

    #[test]
    fn every_environment_variable_is_honoured() {
        // One test per name, effectively. A variable the loader reads
        // but never applies is a documented setting that does nothing.
        let mut config = Config::default();
        config
            .apply_env(|key| match key {
                "SDMQTT_ADDRESS" => Some("127.0.0.1:18831".to_owned()),
                "SDMQTT_MAX_CONNECTIONS" => Some("11".to_owned()),
                "SDMQTT_MOUNTPOINT" => Some("tenant-b".to_owned()),
                "SDMQTT_LOG" => Some("trace".to_owned()),
                _ => None,
            })
            .unwrap();

        assert_eq!(config.listener.address.to_string(), "127.0.0.1:18831");
        assert_eq!(config.listener.max_connections, 11);
        assert_eq!(config.listener.mountpoint, "tenant-b");
        assert_eq!(config.log.level, LogLevel::Trace);
    }

    #[test]
    fn an_unreadable_environment_value_is_an_error_not_a_silent_default() {
        // Same reasoning as the misspelled key: a value that cannot be
        // read must not leave the previous one standing unannounced.
        let mut config = Config::default();
        let error = config
            .apply_env(|key| (key == "SDMQTT_ADDRESS").then(|| "not an address".to_owned()))
            .expect_err("an unreadable value is refused");
        let message = error.to_string();
        assert!(
            message.contains("SDMQTT_ADDRESS"),
            "the variable is named: {message}"
        );
        assert_eq!(
            config.listener.address,
            Config::default().listener.address,
            "and nothing was left half applied"
        );
    }

    #[test]
    fn a_flag_overrides_both_the_file_and_the_environment() {
        let mut config = Config::from_toml("[listener]\naddress = \"127.0.0.1:1\"\n").unwrap();
        config
            .apply_env(|key| (key == "SDMQTT_ADDRESS").then(|| "127.0.0.1:2".to_owned()))
            .unwrap();
        config.apply(&Overrides {
            address: Some("127.0.0.1:3".parse().unwrap()),
            ..Overrides::default()
        });
        assert_eq!(config.listener.address.port(), 3);
    }

    #[test]
    fn every_flag_is_honoured() {
        let mut config = Config::default();
        config.apply(&Overrides {
            address: Some("127.0.0.1:18832".parse().unwrap()),
            max_connections: Some(13),
            mountpoint: Some("tenant-c".to_owned()),
            log_level: Some(LogLevel::Warn),
        });

        assert_eq!(config.listener.address.to_string(), "127.0.0.1:18832");
        assert_eq!(config.listener.max_connections, 13);
        assert_eq!(config.listener.mountpoint, "tenant-c");
        assert_eq!(config.log.level, LogLevel::Warn);
    }

    #[test]
    fn an_absent_flag_leaves_what_was_there() {
        let mut config = Config::from_toml("[listener]\nmax_connections = 5\n").unwrap();
        config.apply(&Overrides::default());
        assert_eq!(config.listener.max_connections, 5);
    }

    #[test]
    fn an_inflight_window_of_zero_is_refused_rather_than_read_as_unbounded() {
        // Zero is the sentinel the inflight window uses for unbounded,
        // so accepting it here would give an operator asking for the
        // narrowest window the widest one, overriding even a client
        // that advertised a Receive Maximum of its own.
        let config = Config::from_toml("[broker]\nmax_inflight = 0\n").unwrap();
        let error = config.validate().expect_err("zero cannot be a window");
        assert!(
            error.to_string().contains("broker.max_inflight"),
            "the message names the key at fault: {error}"
        );
    }

    #[test]
    fn an_inflight_window_of_one_is_accepted() {
        // The other side of the boundary. One is the narrowest usable
        // window and is how strict ordering is asked for, so a guard
        // that refused it would be worse than no guard.
        let config = Config::from_toml("[broker]\nmax_inflight = 1\n").unwrap();
        config
            .validate()
            .expect("one is strict ordering, not an error");
    }

    #[test]
    fn a_queue_that_holds_nothing_is_refused() {
        let config = Config::from_toml("[broker]\nmax_queue_messages = 0\n").unwrap();
        let error = config.validate().expect_err("zero cannot be a queue");
        assert!(
            error.to_string().contains("broker.max_queue_messages"),
            "the message names the key at fault: {error}"
        );
    }

    #[test]
    fn a_queue_with_no_byte_budget_is_refused() {
        // The sibling of `max_queue_messages = 0`, and the same
        // failure: a message occupies a non-zero number of bytes in the
        // queue, so a budget of zero refuses every message rather than
        // bounding anything.
        let config = Config::from_toml("[broker]\nmax_queue_bytes = 0\n").unwrap();
        let error = config.validate().expect_err("zero cannot be a budget");
        assert!(
            error.to_string().contains("broker.max_queue_bytes"),
            "the message names the key at fault: {error}"
        );
    }

    #[test]
    fn the_default_bounds_still_fit_the_outbox_they_had() {
        // The lower end of the relation, against the numbers rather
        // than against the formula: the shipped queue holds a thousand
        // and the window twenty, and the outbox that drains them was a
        // flat 1024 before it was derived. Derived, it must not have
        // shrunk below either.
        let config = Config::default();
        let depth = config.server().connection.outbox_depth;
        assert!(
            depth >= 1000 + 20,
            "one drain of the default queue and window is {}, the outbox is {depth}",
            1000 + 20
        );
        assert!(
            depth >= 1024,
            "the outbox the default bounds had was 1024, now {depth}"
        );
    }

    #[test]
    fn a_raised_queue_bound_raises_the_outbox_that_drains_it() {
        // The defect. `Outbound::pump` sends while the window has room
        // and a fire-and-forget dispatch takes no slot, so one
        // acknowledgement flushes a whole queue into one batch: the
        // reviewer measured three thousand publishes to one subscriber
        // from a single acknowledgement with a queue of five thousand.
        // Against a fixed 1024 the rest is discarded with no reason and
        // no `Action::Dropped`.
        let config = Config::from_toml("[broker]\nmax_queue_messages = 5000\n").unwrap();
        config.validate().expect("five thousand is a usable queue");
        assert!(
            config.server().connection.outbox_depth >= 5000 + 20,
            "a queue of five thousand needs an outbox that can hold one drain of it, got {}",
            config.server().connection.outbox_depth
        );
    }

    #[test]
    fn a_wider_window_raises_the_outbox_too() {
        // The other input. A resume puts the window's contents back at
        // the head of the queue, bypassing its bounds, so the largest
        // batch is the queue plus the window rather than the queue
        // alone.
        let narrow = Config::from_toml("[broker]\nmax_inflight = 1\n").unwrap();
        let wide = Config::from_toml("[broker]\nmax_inflight = 500\n").unwrap();
        assert!(
            wide.server().connection.outbox_depth > narrow.server().connection.outbox_depth,
            "a window of five hundred needs more room than one of one"
        );
        assert!(
            wide.server().connection.outbox_depth >= wide.broker.max_queue_messages + 500,
            "and enough for a whole queue behind a whole window"
        );
    }

    #[test]
    fn a_queue_bound_no_outbox_could_drain_is_refused() {
        // The relation is enforced rather than merely derived, so a
        // configuration that breaks it is refused at startup instead of
        // discarding messages in production with nothing to say why.
        let text = format!("[broker]\nmax_queue_messages = {}\n", usize::MAX);
        let config = Config::from_toml(&text).unwrap();
        let error = config
            .validate()
            .expect_err("a queue no outbox can hold is refused");
        assert!(
            error.to_string().contains("broker.max_queue_messages"),
            "the message names the key at fault: {error}"
        );
    }

    #[test]
    fn loading_a_file_runs_the_guard() {
        // The three tests above call `validate` directly, so all of
        // them pass against a `load` that never calls it. This one is
        // the wiring: without it, deleting that one line from `load`
        // costs nothing and the guard guards nothing.
        let path = std::env::temp_dir().join(format!(
            "sdmqtt-validate-{}-{}.toml",
            std::process::id(),
            line!()
        ));
        std::fs::write(&path, "[broker]\nmax_inflight = 0\n").expect("write the file");
        let result = Config::load(Some(&path), &Overrides::default());
        std::fs::remove_file(&path).ok();

        let error = result.expect_err("loading a refused file fails");
        assert!(
            error.to_string().contains("broker.max_inflight"),
            "the message names the key at fault: {error}"
        );
    }

    #[test]
    fn the_shipped_defaults_are_usable() {
        // The guard runs on every start, so a default it refuses would
        // stop the broker from booting with no configuration at all.
        Config::default()
            .validate()
            .expect("the defaults must pass the guard they ship with");
    }
}
