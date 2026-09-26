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
//!
//! # The admin token comes from two of the three sources
//!
//! `admin.token` can be set in the file or in `SDMQTT_ADMIN_TOKEN`,
//! and there is deliberately no flag for it. A command line is
//! visible in `ps` to every user on the host and is kept in a shell's
//! history, so it is the one source of the three that cannot hold a
//! secret. Section 12.3 keeps the value itself out of logs by holding
//! it in a wrapper that refuses to print; see
//! [`sdmqtt_admin::Token`].
//!
//! The token is an interim that milestone 7 replaces with API keys.
//! Until then the admin listener is protected by where it listens, and
//! [`Config::validate`] refuses a configuration that binds it further
//! than loopback with no token set.
//!
//! # No store directory means no store
//!
//! `storage.directory` has **no default**, and a configuration that
//! does not set it starts a broker that writes nothing down: sessions,
//! their subscriptions and their queued messages last as long as the
//! process and no longer. This is a deliberate choice between two
//! defensible ones, and the reasons are worth the paragraph.
//!
//! Refusing to start without one was the alternative. It was rejected
//! because this binary's first promise is that `sdmqtt serve` with no
//! arguments and no configuration file starts a working broker, and a
//! refusal makes that false. Running without a store is also a real
//! mode rather than a degenerate one: it is what milestone one did, it
//! is what the test suite runs, and a broker in front of a stream of
//! at-most-once telemetry has nothing to gain from a disk.
//!
//! Defaulting the directory to some path was the other alternative, and
//! it is the worst of the three: an operator who never asked for
//! persistence would get a database file appearing in whatever
//! directory the service happened to start in, and would discover it
//! when the disk filled.
//!
//! So the absence is honoured, and it is made loud instead: starting
//! without a store writes a warning naming exactly what will not
//! survive a restart. Silence is what would make this dangerous, not
//! the default.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use broker_core::broker::BrokerConfig;
use mqtt_codec::{Limits, QoS};
use rules::http_in::HttpInError;
use rules::rule::{
    Action, BridgeTopic, ClientPattern, Direction, HttpIn, HttpOut, HttpPoll, MqttBridge, Rule,
    RuleId, TopicFromRequest, TopicSource, Trigger,
};
use sdmqtt_admin::config::{AdminConfig, AdminStartError, Token};
use sdmqtt_server::connection::{outbox_depth_for, ConnectionConfig};
use sdmqtt_server::listener::{ServerConfig, DEFAULT_MAX_CONNECTIONS, DEFAULT_PORT};
use serde::Deserialize;
use storage::record::Secret;
use storage::{RedbStorage, Storage, StorageError, WriterConfig};
use thiserror::Error;

/// Where the configuration file is looked for when none is named.
pub const DEFAULT_PATH: &str = "sdmqtt.toml";

/// What the store is called inside its directory.
///
/// A directory rather than a file is configured because a store is not
/// necessarily one file — redb's is, another backend's need not be —
/// and because an operator pointing a broker at a directory can be told
/// to back the whole of it up.
pub const STORE_FILE: &str = "sdmqtt.redb";

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
    /// The store's directory does not exist and could not be made.
    ///
    /// Names the path, because the usual causes — a typo, a parent
    /// that is not there, a volume not mounted yet, no permission —
    /// are all things an operator fixes by looking at the path and
    /// cannot fix without seeing it.
    #[error("cannot create the store directory {path}: {source}")]
    StoreDirectory {
        /// The directory that could not be made.
        path: PathBuf,
        /// Why not.
        #[source]
        source: std::io::Error,
    },
    /// The admin listener refused the configuration it was given.
    ///
    /// Carried verbatim rather than reworded. Section 11.1 says the
    /// refusal names the address and the missing key, and
    /// [`AdminStartError::Unprotected`] already does; wrapping it in a
    /// sentence of our own would be a second copy of that promise,
    /// free to drift from the one the admin crate enforces when
    /// nothing goes through this file.
    #[error("{0}")]
    Admin(#[from] AdminStartError),
    /// A rule cannot mean what it says.
    ///
    /// Its own variant rather than [`ConfigError::Unusable`] because a
    /// rule's key is not a fixed string: what an operator needs to see
    /// is which rule, by the name they gave it.
    #[error("the rule {id:?} cannot be used: {why}")]
    Rule {
        /// Which rule.
        id: String,
        /// What is wrong with it.
        why: String,
    },
    /// The store itself could not be opened.
    ///
    /// Separate from the directory failing, because the answers are
    /// different: a store held by another process means find the other
    /// broker, a store from a later build means run the right build,
    /// and a corrupt one means reach for a backup. [`StorageError`]
    /// keeps those apart and this keeps its message.
    #[error("cannot open the store at {path}: {source}")]
    StoreUnopenable {
        /// The store file that could not be opened.
        path: PathBuf,
        /// Why not.
        #[source]
        source: StorageError,
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
    /// How many connectors a publication may have been through.
    ///
    /// Section 9.2's third loop: a rule feeding a system that
    /// publishes back through another rule is a cycle no single rule is
    /// in the whole of. Every publication a connector originates
    /// carries a hop count, and one over this is dropped and counted.
    ///
    /// Node-wide, and not the same number as `mqtt_bridge.max_hops`,
    /// which is one bridge's own check on its own traffic. This one is
    /// the door every publication without a session goes through, so it
    /// is the one a connector written later cannot be written without.
    pub max_hops: u32,
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
            max_hops: broker.max_hops,
        }
    }
}

/// How hard a flush tries, as a configuration file spells it.
///
/// A second spelling of [`storage::SyncMode`] rather than a
/// `Deserialize` on that one, because the storage crate has no serde
/// dependency and a configuration format is not its business. The
/// mapping is pinned by a test in both directions: a transposition
/// here would round trip perfectly and quietly turn every
/// acknowledgement into a promise this broker does not keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase", deny_unknown_fields)]
pub enum Sync {
    /// A flush does not return until the writes are on the device.
    ///
    /// The default, and the only mode in which section 7.3's promise
    /// is kept: an acknowledgement is sent after a durable commit, and
    /// anything weaker makes the promise without keeping it.
    #[default]
    Immediate,
    /// A flush returns without waiting for the device.
    ///
    /// **Worth nothing against the redb backend**, whose weaker
    /// durability level keeps the commit in this process's memory and
    /// writes it out only on a clean shutdown. A broker that is killed
    /// loses everything since its last immediate flush, and under this
    /// mode there has never been one. For measuring what the durable
    /// path costs, not for trading durability away.
    Relaxed,
}

impl Sync {
    /// The storage crate's spelling.
    pub fn to_storage(self) -> storage::SyncMode {
        match self {
            Sync::Immediate => storage::SyncMode::Immediate,
            Sync::Relaxed => storage::SyncMode::Relaxed,
        }
    }
}

impl std::str::FromStr for Sync {
    type Err = &'static str;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "immediate" => Ok(Sync::Immediate),
            "relaxed" => Ok(Sync::Relaxed),
            _ => Err("one of immediate or relaxed"),
        }
    }
}

/// The storage section.
///
/// Section 7.2 names what is configurable: where the store is, how
/// hard a flush tries, and the two triggers that decide when the
/// batching writer flushes. The third trigger — an acknowledgement
/// that must not be lost — is deliberately not here, because it is a
/// correctness requirement rather than a tuning choice.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StorageSection {
    /// Where the store lives.
    ///
    /// **No default, and no directory means no store.** A broker
    /// configured without one keeps its sessions, their subscriptions
    /// and their queued messages in memory alone, and a restart
    /// destroys all three. The module documentation has why this is
    /// the default rather than a refusal or an invented path; the
    /// short version is that `sdmqtt serve` with no arguments must
    /// keep starting a working broker, and a database file appearing
    /// in whatever directory the service started in is worse than
    /// either.
    ///
    /// The directory is created if it is not there. The store inside
    /// it is [`STORE_FILE`].
    pub directory: Option<PathBuf>,
    /// How hard a flush tries. Immediate by default.
    pub sync: Sync,
    /// How long after a flush the next one becomes due, in
    /// milliseconds, if anything is outstanding.
    ///
    /// Two by default: section 7.2 asks for an interval in the low
    /// milliseconds, and this only ever applies to writes nobody is
    /// waiting on — a waiter flushes at once. Zero flushes every batch
    /// on its own, which is the uncoalesced rate and is useful for
    /// measuring it; it cannot spin the writer, which blocks rather
    /// than waking to find nothing outstanding.
    pub flush_interval_ms: u64,
    /// How many records may be applied before a flush becomes due.
    ///
    /// A thousand by default, which is where the storage crate's
    /// measurement was taken: coalesced a thousand at a time, the
    /// durable rate is over 300,000 a second. It bounds how much work
    /// one flush has to do, which is what keeps the flush from
    /// becoming the stall.
    pub flush_records: u64,
}

impl Default for StorageSection {
    /// No store, immediate flushes, two milliseconds, a thousand
    /// records.
    fn default() -> Self {
        let writer = WriterConfig::default();
        Self {
            directory: None,
            sync: Sync::Immediate,
            flush_interval_ms: u64::try_from(writer.interval.as_millis()).unwrap_or(u64::MAX),
            flush_records: writer.records,
        }
    }
}

impl StorageSection {
    /// Open the store, or say there is not one.
    ///
    /// `Ok(None)` is the configured absence of a directory and is not
    /// a failure. Everything else that goes wrong is one, and names
    /// the path: a startup that says only "cannot open the store"
    /// leaves an operator guessing which of the three paths in their
    /// deployment it meant.
    ///
    /// # Errors
    ///
    /// [`ConfigError::StoreDirectory`] when the directory is not there
    /// and cannot be made, and [`ConfigError::StoreUnopenable`] when
    /// the store itself will not open — held by another process, from
    /// a build that writes a format this one does not read, or
    /// corrupt.
    pub fn open(&self) -> Result<Option<Arc<dyn Storage>>, ConfigError> {
        let Some(directory) = &self.directory else {
            return Ok(None);
        };
        std::fs::create_dir_all(directory).map_err(|source| ConfigError::StoreDirectory {
            path: directory.clone(),
            source,
        })?;
        let path = directory.join(STORE_FILE);
        let store = RedbStorage::open_with(&path, self.sync.to_storage())
            .map_err(|source| ConfigError::StoreUnopenable { path, source })?;
        self.announce_sync_mode();
        Ok(Some(Arc::new(store)))
    }

    /// Say what durability this store was opened with, when that is
    /// worth saying.
    ///
    /// **Only the relaxed mode gets a line**, and it is a warning.
    /// Immediate is the default and keeps section 7.3's promise, so
    /// naming it on every start would be noise an operator learns to
    /// scroll past — which is how the line that matters gets missed.
    ///
    /// The line that matters is this one. A missing store directory is
    /// already warned about loudly, and this is the more dangerous of
    /// the two: a broker with no store never tells a publisher its
    /// message is safe, while this one says it on every acknowledgement
    /// and nothing has reached the device. The failure is invisible
    /// until a power cut, and by then the configuration that caused it
    /// is not in front of anybody.
    ///
    /// Returns whether it said anything, because a method whose only
    /// effect is a log line cannot otherwise be told from one that does
    /// nothing; `Shared::report_rejections` in `sdmqtt-server` is the
    /// same shape for the same reason.
    fn announce_sync_mode(&self) -> bool {
        if self.sync == Sync::Immediate {
            return false;
        }
        tracing::warn!(
            path = %self.directory.as_ref().map_or_else(String::new, |at| at.display().to_string()),
            "storage.sync is relaxed, so a flush returns without waiting for the device: \
             against this backend nothing is written to the file until a clean shutdown, \
             and every acknowledgement this broker sends is a promise it cannot keep \
             across a power cut or a kill"
        );
        true
    }

    /// How the batching writer should be configured.
    ///
    /// The two flush triggers come from here and the two high-water
    /// marks do not: those bound the process's memory rather than trade
    /// durability for throughput, which is the same reason section 7.2's
    /// third flush trigger is not an operator's choice either. Their
    /// defaults are on [`WriterConfig`].
    pub fn writer(&self) -> WriterConfig {
        WriterConfig {
            interval: Duration::from_millis(self.flush_interval_ms),
            records: self.flush_records,
            ..WriterConfig::default()
        }
    }
}

/// One rule, as a configuration file spells it.
///
/// Section 9.1's rule, flattened by one level: the trigger's three parts
/// sit beside the name rather than in a table of their own, because a
/// `[rules.trigger]` holding one required key and two optional ones is
/// ceremony around a filter.
///
/// **Rules are node-local in milestone 4** and replication is milestone
/// 5's. What a rule declared here does is get written into the store at
/// startup, so it survives a restart and its outbox hangs off it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleSection {
    /// What names it, and what its dead-letter topic is built from.
    ///
    /// One topic level: no separator, no wildcard, nothing that would
    /// give one rule's dead-letter topic to another. The rules crate
    /// refuses anything else.
    pub id: String,
    /// What an operator calls it.
    pub name: String,
    /// Whether it fires at all.
    ///
    /// **Not the same as deleting it.** Turning a connector off while
    /// an endpoint is down and back on afterwards is the ordinary
    /// thing to want, and deleting the rule would take its outbox with
    /// it.
    #[serde(default = "enabled_unless_said_otherwise")]
    pub enabled: bool,
    /// The topic filter a message must match.
    ///
    /// Required for an action that fires on published messages, and
    /// **refused** for one that does not: section 9.2's `http_in` is
    /// fired by a request, and a filter on it is a filter that can
    /// never match. An operator who wrote one believes something about
    /// when the rule runs that is not true, so it is said rather than
    /// ignored.
    pub filter: Option<String>,
    /// The lowest level that fires it, if it is narrowed.
    pub min_qos: Option<u8>,
    /// The client identifier pattern that fires it, if it is narrowed.
    pub client_id: Option<String>,
    /// Post a JSON envelope out, if that is what this rule does.
    pub http_out: Option<HttpOutSection>,
    /// Accept a post and publish its body, if that is what it does.
    pub http_in: Option<HttpInSection>,
    /// Fetch a URL on an interval and publish the body, if that is
    /// what it does.
    pub http_poll: Option<HttpPollSection>,
    /// Connect to a remote broker as a client, if that is what it
    /// does.
    pub mqtt_bridge: Option<MqttBridgeSection>,
}

/// A rule left out of a file is on.
fn enabled_unless_said_otherwise() -> bool {
    true
}

/// How an `http_out` rule posts.
///
/// Every field section 9.2 lists as configurable. The defaults are the
/// rules crate's own, restated here only where serde needs a function;
/// `url` has no default because a default that posts somewhere is worse
/// than one that posts nowhere.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HttpOutSection {
    /// Where to post. `https://`, or `http://` to a loopback address.
    pub url: String,
    /// Which method to use.
    pub method: String,
    /// Headers to send.
    ///
    /// A table, so it is written the way a header is read:
    /// `headers = { "x-token" = "secret" }`. **Sorted by name**, which
    /// a table is anyway and which costs nothing: HTTP gives no meaning
    /// to the order of two differently named headers, and a table
    /// cannot express the one case where order does matter — the same
    /// name twice — at all.
    pub headers: BTreeMap<String, String>,
    /// How long one attempt may take, in milliseconds.
    pub timeout_ms: u32,
    /// How many attempts may be in flight at once.
    pub max_concurrency: u16,
    /// How many times to retry after the first attempt fails.
    pub retries: u8,
    /// How long to wait before the first retry, in milliseconds.
    pub backoff_ms: u32,
}

impl Default for HttpOutSection {
    /// Nowhere to post, and bounds on everything else.
    fn default() -> Self {
        let action = HttpOut::default();
        Self {
            url: action.url,
            method: action.method,
            headers: BTreeMap::new(),
            timeout_ms: action.timeout_ms,
            max_concurrency: action.max_concurrency,
            retries: action.retries,
            backoff_ms: action.backoff_ms,
        }
    }
}

impl RuleSection {
    /// This section as the rules crate's own type.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Rule`] naming the rule and what is wrong with it.
    /// Everything an operator can get wrong is checked here, once, at
    /// startup: the identifier, the filter, the level, and then
    /// whichever action was written — the URL, the scheme, the method
    /// and every header for `http_out`, and the token, the topic, the
    /// source and the prefix for `http_in`. A rule that cannot work is
    /// a broker that does not start, rather than a rule that quietly
    /// never fires or an endpoint that quietly accepts anything.
    pub fn to_rule(&self) -> Result<Rule, ConfigError> {
        let wrong = |why: String| ConfigError::Rule {
            id: self.id.clone(),
            why,
        };

        let id = RuleId::new(self.id.clone()).map_err(|error| wrong(error.to_string()))?;
        let action = self.to_action(&wrong)?;

        // A filter belongs to an action that fires on messages. Which
        // of the two mistakes an operator made is said here, where both
        // halves are visible, rather than as one sentence about a
        // mismatch.
        let trigger = match (&self.filter, action.fires_on_messages()) {
            (Some(filter), true) => {
                let mut trigger =
                    Trigger::new(filter.clone()).map_err(|error| wrong(error.to_string()))?;
                if let Some(level) = self.min_qos {
                    trigger.min_qos = Some(match level {
                        0 => QoS::AtMostOnce,
                        1 => QoS::AtLeastOnce,
                        2 => QoS::ExactlyOnce,
                        other => {
                            return Err(wrong(format!(
                                "min_qos = {other} is not a level; MQTT has 0, 1 and 2"
                            )))
                        }
                    });
                }
                if let Some(pattern) = &self.client_id {
                    trigger.client_id = Some(ClientPattern::new(pattern.clone()));
                }
                Some(trigger)
            }
            (None, true) => {
                return Err(wrong(
                    "this rule fires on published messages and has no filter; set filter"
                        .to_owned(),
                ))
            }
            (Some(_), false) => {
                // Which kind of rule it is decides what fired it, and
                // an operator who wrote a filter believes something
                // about when the rule runs that is not true. Saying
                // which is the difference between a message they can
                // act on and one they have to guess at.
                return Err(wrong(match &action {
                    Action::HttpPoll(_) => "an http_poll rule is fired by its own interval \
                                            rather than by a published message, so it has \
                                            no filter; remove it"
                        .to_owned(),
                    Action::MqttBridge(_) => "an mqtt_bridge rule decides what moves with \
                                              its own topic entries, each of which has a \
                                              direction and a rewrite, so it has no \
                                              rule-level filter; put it in a \
                                              [[rules.mqtt_bridge.topics]] entry instead"
                        .to_owned(),
                    _ => "an http_in rule is fired by a request rather than by a published \
                          message, so it has no filter; remove it"
                        .to_owned(),
                }));
            }
            (None, false) => {
                // The two narrowings belong to a filter, so writing one
                // without a filter is the same mistake said smaller.
                if self.min_qos.is_some() || self.client_id.is_some() {
                    return Err(wrong(format!(
                        "min_qos and client_id narrow a filter, and an {} rule has \
                         none; remove them",
                        match &action {
                            Action::HttpPoll(_) => "http_poll",
                            Action::MqttBridge(_) => "mqtt_bridge",
                            _ => "http_in",
                        }
                    )));
                }
                None
            }
        };

        let mut rule = Rule::new(id, self.name.clone(), trigger, action)
            .map_err(|error| wrong(error.to_string()))?;
        rule.enabled = self.enabled;
        Ok(rule)
    }

    /// The one action this section declares.
    ///
    /// Section 9.1: **one** action. Two tables is an operator who means
    /// two rules, and none is a rule that does nothing; both are
    /// refused rather than resolved by a precedence nobody wrote down.
    ///
    /// Written as a count of what was declared rather than as a match
    /// over the combinations, because the combinations square with
    /// every action added and the answer for all but one of them is
    /// the same sentence.
    fn to_action(&self, wrong: &impl Fn(String) -> ConfigError) -> Result<Action, ConfigError> {
        let declared: Vec<&str> = [
            self.http_out.as_ref().map(|_| "[rules.http_out]"),
            self.http_in.as_ref().map(|_| "[rules.http_in]"),
            self.http_poll.as_ref().map(|_| "[rules.http_poll]"),
            self.mqtt_bridge.as_ref().map(|_| "[rules.mqtt_bridge]"),
        ]
        .into_iter()
        .flatten()
        .collect();
        if declared.len() > 1 {
            // Spelled as a word, because the sentence is read by a
            // person and because three is as many as there can be.
            let count = match declared.len() {
                2 => "two",
                3 => "three",
                _ => "four",
            };
            return Err(wrong(format!(
                "a rule has one action, section 9.1, and this one declares {}; \
                 write {count} rules",
                declared.join(" and "),
            )));
        }

        if let Some(out) = &self.http_out {
            let action = HttpOut {
                url: out.url.clone(),
                method: out.method.clone(),
                headers: out
                    .headers
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
                timeout_ms: out.timeout_ms,
                max_concurrency: out.max_concurrency,
                retries: out.retries,
                backoff_ms: out.backoff_ms,
            };
            // Before anything is stored and long before anything is
            // posted.
            rules::http_out::check(&action).map_err(|error| wrong(error.to_string()))?;
            return Ok(Action::HttpOut(action));
        }
        if let Some(into) = &self.http_in {
            return Ok(Action::HttpIn(into.to_action(wrong)?));
        }
        if let Some(poll) = &self.http_poll {
            return Ok(Action::HttpPoll(poll.to_action(wrong)?));
        }
        if let Some(bridge) = &self.mqtt_bridge {
            return Ok(Action::MqttBridge(Box::new(bridge.to_action(wrong)?)));
        }
        Err(wrong(
            "a rule needs an action; write a [rules.http_out], a [rules.http_in], an \
             [rules.http_poll] or an [rules.mqtt_bridge] table"
                .to_owned(),
        ))
    }
}

/// How an `http_in` rule accepts a post.
///
/// Every field section 9.2 lists, and no field it does not. The token
/// has no default because a default token is a secret in a repository,
/// and the topic has none because a default that publishes somewhere is
/// worse than one that publishes nowhere. Both are refused when left
/// out.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HttpInSection {
    /// The bearer token a request must present.
    ///
    /// **Not optional.** Section 9.2: the endpoint publishes, so an
    /// unguarded one is a hole rather than a degraded mode.
    ///
    /// A [`Secret`], so that the configuration this binary logs at
    /// startup does not carry it.
    pub token: Secret,
    /// Where the body is published when the request does not choose.
    pub topic: String,
    /// The header a request may name a topic in.
    ///
    /// Requires `topic_prefix`, and excludes `topic_query`.
    pub topic_header: Option<String>,
    /// The query parameter a request may name a topic in.
    ///
    /// Requires `topic_prefix`, and excludes `topic_header`.
    pub topic_query: Option<String>,
    /// The prefix a topic the request chose must lie at or under.
    ///
    /// **This is the whole safety of a rule that lets a request choose
    /// a topic.** Without it whoever holds this rule's token can
    /// publish anywhere on the broker.
    pub topic_prefix: Option<String>,
    /// The level the body is published at, as MQTT numbers it.
    pub qos: u8,
    /// Whether it is retained.
    pub retain: bool,
}

impl Default for HttpInSection {
    /// No token, nowhere to publish, and the level that promises
    /// nothing.
    fn default() -> Self {
        Self {
            token: Secret::new(String::new()),
            topic: String::new(),
            topic_header: None,
            topic_query: None,
            topic_prefix: None,
            qos: 0,
            retain: false,
        }
    }
}

impl HttpInSection {
    /// This table as the rules crate's own type.
    fn to_action(&self, wrong: &impl Fn(String) -> ConfigError) -> Result<HttpIn, ConfigError> {
        let qos = rules::http_in::qos_of(self.qos).ok_or_else(|| {
            wrong(format!(
                "http_in.qos = {} is not a level; MQTT has 0, 1 and 2",
                self.qos
            ))
        })?;

        // Two sources, or a prefix with nothing to bound, are refused
        // here rather than in the rules crate, because that crate's
        // type cannot express either: a source carries its prefix, so
        // the invalid combinations are the two keys' and not the
        // model's.
        let source = match (&self.topic_header, &self.topic_query) {
            (Some(_), Some(_)) => return Err(wrong(HttpInError::TwoSources.to_string())),
            (Some(name), None) => Some(TopicSource::Header(name.clone())),
            (None, Some(name)) => Some(TopicSource::Query(name.clone())),
            (None, None) => None,
        };
        let topic_from = match (source, &self.topic_prefix) {
            (Some(source), Some(prefix)) => Some(TopicFromRequest {
                source,
                prefix: prefix.clone(),
            }),
            (Some(source), None) => {
                let key = match source {
                    TopicSource::Header(_) => "topic_header",
                    TopicSource::Query(_) => "topic_query",
                };
                return Err(wrong(HttpInError::NoPrefix { key }.to_string()));
            }
            (None, Some(_)) => return Err(wrong(HttpInError::NoSource.to_string())),
            (None, None) => None,
        };

        let action = HttpIn {
            token: self.token.clone(),
            topic: self.topic.clone(),
            topic_from,
            qos,
            retain: self.retain,
        };
        // Before anything is stored and long before anything is served.
        rules::http_in::check(&action).map_err(|error| wrong(error.to_string()))?;
        Ok(action)
    }
}

/// How an `http_poll` rule fetches.
///
/// Every field section 9.2 lists as configurable for it, and no field
/// it does not. **There is no retry count and no backoff**, unlike
/// `[rules.http_out]`: a poller has no outbox, a fetch that failed is
/// tried again on the next interval, and a body republished late is
/// worse than one not republished. `crates/rules/src/http_poll.rs`
/// carries the argument.
///
/// `url` and `topic` have no default, because a default that fetches
/// from somewhere or publishes to somewhere is worse than one that
/// does neither. Both are refused when left out.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HttpPollSection {
    /// Where to fetch from. `https://`, or `http://` to a loopback
    /// address, on exactly the rule `[rules.http_out]` is held to.
    pub url: String,
    /// Headers to send.
    ///
    /// A table, sorted by name, for the reason
    /// [`HttpOutSection::headers`] gives.
    pub headers: BTreeMap<String, String>,
    /// How long between the start of one fetch and the next, in
    /// milliseconds.
    ///
    /// **A fetch slower than this loses the intervals it covered.**
    /// One fetch is ever in flight, and the boundaries a slow fetch
    /// passed are dropped rather than fired in a burst afterwards, so
    /// an endpoint that has become slow is not answered with a run of
    /// back-to-back requests.
    pub interval_ms: u32,
    /// How long one fetch may take, in milliseconds.
    ///
    /// May be longer than the interval, and is not refused for it: a
    /// fetch that overruns is bounded by the paragraph above rather
    /// than by a rule about what an operator may write.
    pub timeout_ms: u32,
    /// The most of a response body that will be read, in bytes.
    ///
    /// A body past this is a **failed fetch**, not a truncated
    /// publication: half a document on a topic is worse than nothing
    /// on it, and a subscriber cannot tell the two apart.
    pub max_body_bytes: u32,
    /// Where the body is published.
    pub topic: String,
    /// The level it is published at, as MQTT numbers it.
    pub qos: u8,
    /// Whether it is retained.
    ///
    /// Usually what an operator wants from a poller — the current
    /// value, available to a subscriber that has just connected — and
    /// off by default all the same, because a retained message outlives
    /// the rule that wrote it.
    pub retain: bool,
}

impl Default for HttpPollSection {
    /// Nowhere to fetch from, nowhere to publish to, and bounds on
    /// everything else. The rules crate's own defaults.
    fn default() -> Self {
        let action = HttpPoll::default();
        Self {
            url: action.url,
            headers: BTreeMap::new(),
            interval_ms: action.interval_ms,
            timeout_ms: action.timeout_ms,
            max_body_bytes: action.max_body_bytes,
            topic: action.topic,
            qos: 0,
            retain: action.retain,
        }
    }
}

impl HttpPollSection {
    /// This table as the rules crate's own type.
    fn to_action(&self, wrong: &impl Fn(String) -> ConfigError) -> Result<HttpPoll, ConfigError> {
        let qos = rules::http_in::qos_of(self.qos).ok_or_else(|| {
            wrong(format!(
                "http_poll.qos = {} is not a level; MQTT has 0, 1 and 2",
                self.qos
            ))
        })?;

        let action = HttpPoll {
            url: self.url.clone(),
            headers: self
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            interval_ms: self.interval_ms,
            timeout_ms: self.timeout_ms,
            max_body_bytes: self.max_body_bytes,
            topic: self.topic.clone(),
            qos,
            retain: self.retain,
        };
        // Before anything is stored and long before anything is
        // fetched.
        rules::http_poll::check(&action).map_err(|error| wrong(error.to_string()))?;
        Ok(action)
    }
}

/// How an `mqtt_bridge` rule connects. Section 9.2.
///
/// The address has no default because a default that connects
/// somewhere is worse than one that connects nowhere, and the topics
/// have none because a bridge that carries nothing is a bridge nobody
/// meant.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MqttBridgeSection {
    /// Where the remote broker is: `mqtts://host:port`, or
    /// `mqtt://host:port` to a loopback address.
    ///
    /// Plain text to anywhere but this machine is refused, on the same
    /// reasoning `http_out` refuses a plain-text endpoint and with more
    /// at stake: a bridge sends its password in the connect and then
    /// everything it carries.
    pub address: String,
    /// What this bridge calls itself to the remote.
    ///
    /// May be left out on MQTT 5, where the remote assigns one and says
    /// so. On 3.1.1 there is no way for it to say so, so it is
    /// required.
    pub client_id: String,
    /// Which version to speak: 4 for MQTT 3.1.1, 5 for MQTT 5.
    ///
    /// The protocol level byte rather than a name, because that is what
    /// goes on the wire and what every other tool an operator uses
    /// calls it.
    pub protocol_level: u8,
    /// The user name to offer.
    pub username: Option<String>,
    /// The password to offer.
    ///
    /// A [`Secret`], so the configuration this binary logs at startup
    /// does not carry it.
    pub password: Option<Secret>,
    /// The keep-alive to propose, in seconds.
    ///
    /// A proposal: MQTT 5 lets the remote override it, and the
    /// override is what this bridge then uses.
    pub keep_alive_secs: u16,
    /// Whether to ask the remote to discard any session it holds.
    ///
    /// **The first connection of a process asks for a clean start
    /// whatever this says.** The in-flight window is held in memory, so
    /// a resumed session would hold packet identifiers the restarted
    /// process no longer knows about; `rules::bridge::supervisor` has
    /// the argument.
    pub clean_start: bool,
    /// How long the remote should keep the session after a disconnect.
    pub session_expiry_secs: u32,
    /// How many acknowledged publications this end will take at once.
    pub receive_maximum: u16,
    /// The largest packet this end will take, in bytes.
    pub maximum_packet_size: u32,
    /// The highest topic alias this end will accept from the remote.
    ///
    /// Above zero licenses a compliant remote to start using aliases at
    /// any moment; `rules::bridge::aliases` is what makes that safe.
    pub topic_alias_maximum: u16,
    /// Where this bridge's connectivity is reported, on this broker.
    ///
    /// The same name is the will topic on the remote. A state topic
    /// this bridge would itself carry is refused, because a bridge
    /// republishing its own liveness is a loop with a heartbeat.
    pub state_topic: String,
    /// The first reconnection wait, in milliseconds.
    pub reconnect_min_ms: u32,
    /// The longest reconnection wait, in milliseconds.
    pub reconnect_max_ms: u32,
    /// The hop count above which a publication is dropped.
    pub max_hops: u32,
    /// What moves, and which way.
    pub topics: Vec<BridgeTopicSection>,
}

/// One topic entry on a bridge. Section 9.2.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BridgeTopicSection {
    /// The filter, carrying **neither** prefix.
    ///
    /// The local topics an outbound entry fires on are `local_prefix`
    /// followed by something matching this; the remote filter an
    /// inbound entry subscribes with is `remote_prefix` followed by the
    /// same.
    pub filter: String,
    /// `"in"`, `"out"` or `"both"`.
    ///
    /// Note that `"both"` on one entry is a loop by construction and is
    /// refused; `rules::bridge::topics` says why, and says what to
    /// write instead.
    pub direction: String,
    /// The level to move messages at.
    pub qos: u8,
    /// The prefix this broker's half of the topic carries.
    pub local_prefix: String,
    /// The prefix the remote's half carries.
    pub remote_prefix: String,
    /// The outbound template, if the topic is rewritten by position.
    ///
    /// `sensors/+/temp` with `template = "site-a/temp/{1}"` publishes
    /// `sensors/kitchen/temp` remotely as `site-a/temp/kitchen`.
    pub template: Option<String>,
}

impl Default for BridgeTopicSection {
    fn default() -> Self {
        Self {
            filter: String::new(),
            direction: "out".to_owned(),
            qos: 0,
            local_prefix: String::new(),
            remote_prefix: String::new(),
            template: None,
        }
    }
}

impl Default for MqttBridgeSection {
    /// Nowhere to connect to, nothing to carry, and bounds on
    /// everything else.
    fn default() -> Self {
        let action = MqttBridge::default();
        Self {
            address: action.address,
            client_id: action.client_id,
            protocol_level: 5,
            username: action.username,
            password: action.password,
            keep_alive_secs: action.keep_alive_secs,
            clean_start: action.clean_start,
            session_expiry_secs: action.session_expiry_secs,
            receive_maximum: action.receive_maximum,
            maximum_packet_size: action.maximum_packet_size,
            topic_alias_maximum: action.topic_alias_maximum,
            state_topic: action.state_topic,
            reconnect_min_ms: action.reconnect_min_ms,
            reconnect_max_ms: action.reconnect_max_ms,
            max_hops: action.max_hops,
            topics: Vec::new(),
        }
    }
}

impl MqttBridgeSection {
    /// This table as the rules crate's own type.
    fn to_action(&self, wrong: &impl Fn(String) -> ConfigError) -> Result<MqttBridge, ConfigError> {
        let version = match self.protocol_level {
            4 => mqtt_codec::Version::V311,
            5 => mqtt_codec::Version::V5,
            other => {
                return Err(wrong(format!(
                    "mqtt_bridge.protocol_level = {other} is not a version this bridge \
                     speaks; write 4 for MQTT 3.1.1 or 5 for MQTT 5"
                )))
            }
        };
        // [MQTT-3.1.3-x]: 3.1.1 has no way for a server to tell a
        // client what identifier it was given, so an empty one there is
        // a session that can never be resumed and a bridge whose
        // reconnections each get a new one. Refused when the
        // configuration is read rather than discovered on the wire.
        if self.client_id.is_empty() && version == mqtt_codec::Version::V311 {
            return Err(wrong(
                "mqtt_bridge.client_id is empty and protocol_level is 4. MQTT 3.1.1 has \
                 no way for the remote to say what identifier it assigned, so the session \
                 could never be resumed; set client_id, or speak MQTT 5."
                    .to_owned(),
            ));
        }
        if self.receive_maximum == 0 {
            return Err(wrong(
                "mqtt_bridge.receive_maximum is zero, which MQTT 5 makes a protocol error \
                 and which would be a bridge that can receive nothing"
                    .to_owned(),
            ));
        }
        if self.max_hops == 0 {
            return Err(wrong(
                "mqtt_bridge.max_hops is zero, so every message would be over the limit \
                 and this bridge would carry nothing"
                    .to_owned(),
            ));
        }

        let mut topics = Vec::with_capacity(self.topics.len());
        for (index, topic) in self.topics.iter().enumerate() {
            let direction = match topic.direction.as_str() {
                "in" => Direction::In,
                "out" => Direction::Out,
                "both" => Direction::Both,
                other => {
                    return Err(wrong(format!(
                        "mqtt_bridge topic {index}: direction = {other:?} is not a \
                         direction; write \"in\", \"out\" or \"both\""
                    )))
                }
            };
            let qos = rules::http_in::qos_of(topic.qos).ok_or_else(|| {
                wrong(format!(
                    "mqtt_bridge topic {index}: qos = {} is not a level; MQTT has 0, 1 \
                     and 2",
                    topic.qos
                ))
            })?;
            topics.push(BridgeTopic {
                filter: topic.filter.clone(),
                direction,
                qos,
                local_prefix: topic.local_prefix.clone(),
                remote_prefix: topic.remote_prefix.clone(),
                template: topic.template.clone(),
            });
        }

        let action = MqttBridge {
            address: self.address.clone(),
            client_id: self.client_id.clone(),
            version,
            username: self.username.clone(),
            password: self.password.clone(),
            keep_alive_secs: self.keep_alive_secs,
            clean_start: self.clean_start,
            session_expiry_secs: self.session_expiry_secs,
            receive_maximum: self.receive_maximum,
            maximum_packet_size: self.maximum_packet_size,
            topic_alias_maximum: self.topic_alias_maximum,
            state_topic: self.state_topic.clone(),
            reconnect_min_ms: self.reconnect_min_ms,
            reconnect_max_ms: self.reconnect_max_ms,
            max_hops: self.max_hops,
            topics,
        };

        // **Before anything is stored and long before anything
        // connects.** Both of them: the address, which is where a
        // plain-text link to somewhere other than this machine is
        // refused, and the topics, which is where a configuration that
        // loops is refused. Section 9.2 puts the second first among the
        // loop defences, and this is the moment it means.
        rules::bridge::supervisor::address(&action.address)
            .map_err(|error| wrong(error.to_string()))?;
        rules::bridge::topics::check(&action).map_err(|error| wrong(error.to_string()))?;
        Ok(action)
    }
}

/// The port the ingest listener binds by default.
///
/// Adjacent to the admin listener's so that an operator who knows one
/// knows the other. It is not registered for this and does not need to
/// be: it is bound on loopback by default, and an operator who exposes
/// it is configuring it anyway.
pub const DEFAULT_INGEST_PORT: u16 = 1885;

/// Which listener serves section 9.2's ingest routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestListener {
    /// A listener of its own, which is the default.
    ///
    /// Section 9.2: "it defaults to a separate ingest listener rather
    /// than the admin listener, so that publishing data does not
    /// require exposing the administrative surface."
    #[default]
    Own,
    /// The admin listener.
    ///
    /// "Permitted but must be chosen explicitly", which is what writing
    /// this value is. A route set served there takes that listener's
    /// rules with it, including the host check the ingest listener
    /// deliberately has none of.
    Admin,
}

/// Where the ingest endpoint is served.
///
/// # There is no `enabled` key, and no address rule either
///
/// A listener of its own is bound when this node holds an ingest rule
/// with a route, and not otherwise, so turning it off is deleting the
/// rules rather than a second switch that can disagree with them.
///
/// And unlike `[admin]`, a non-loopback address here is **not** refused
/// without a token. Section 9.2: "the ingest listener needs no address
/// rule of its own, because every route on it is individually
/// guarded." A rule without a token is refused where rules are
/// accepted, so a route that exists is a route with a secret in front
/// of it; there is nothing left for an address rule to protect.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct IngestConfig {
    /// Where a listener of its own binds.
    ///
    /// Loopback by default, which is the right default for a broker
    /// behind an ingress and the wrong one for a webhook sender on
    /// another host; an operator who wants the second writes it.
    pub address: SocketAddr,
    /// Which listener serves the routes.
    pub listener: IngestListener,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_INGEST_PORT),
            listener: IngestListener::Own,
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
    /// The storage section.
    pub storage: StorageSection,
    /// The admin listener's section.
    ///
    /// The admin crate's own type rather than a copy of it here. A
    /// second struct would be a second set of defaults, and the one
    /// that matters is the default address: `[admin]` absent from a
    /// file and `[admin]` present but empty must mean the same
    /// loopback address, or the refusal in `AdminConfig::check` guards
    /// a different configuration from the one that is served.
    pub admin: AdminConfig,
    /// Where section 9.2's ingest endpoint is served.
    pub ingest: IngestConfig,
    /// The log section.
    pub log: Log,
    /// The rules this node holds, section 9.1.
    ///
    /// Written as `[[rules]]` tables, which is why this is a list and
    /// not a map: a map keyed by identifier would read better and would
    /// put the identifier somewhere `deny_unknown_fields` cannot check
    /// it, so a typo in a key would silently create a second rule.
    #[serde(default)]
    pub rules: Vec<RuleSection>,
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
    /// Where the store lives.
    pub store_directory: Option<PathBuf>,
    /// Where the admin listener binds.
    pub admin_address: Option<SocketAddr>,
    /// Do not serve the admin listener at all.
    ///
    /// A flag rather than a `bool`, because `false` from a command
    /// line that did not mention it would override a file that turned
    /// the listener off. There is deliberately no flag that turns it
    /// back *on*: the default is on, so the only thing a command line
    /// has to be able to say is "not this time".
    pub no_admin: bool,
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
        // The same refusal `mqtt_bridge.max_hops` makes, for the same
        // reason: a maximum of zero is over-the-limit for the first
        // hop, so every ingest post, every poll and every message a
        // bridge brought in would be dropped, and the symptom is a
        // broker that silently carries nothing a connector gives it.
        if self.broker.max_hops == 0 {
            return Err(ConfigError::Unusable {
                key: "broker.max_hops",
                value: "0".to_string(),
                why: "every publication a connector originates has been through one                       connector, so a maximum of zero drops all of them",
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

        // An empty directory is not the same as no directory, and it
        // is what a file saying `directory = ""` asks for. Refused
        // here rather than read as "no store", because the two are
        // opposite intentions and the difference between them is a
        // broker that loses everything on a restart. The environment
        // is the one place an empty value does mean "no store", and it
        // clears the key rather than setting it to this.
        if self
            .storage
            .directory
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
        {
            return Err(ConfigError::Unusable {
                key: "storage.directory",
                value: String::new(),
                why: "name a directory, or leave the key out entirely to run with no store",
            });
        }

        // Section 11.1: a non-loopback admin address with no token is
        // refused at startup, not warned about.
        //
        // **Here, and not only where the listener binds.** This runs
        // before the store is opened and before anything is bound, so
        // an operator who asked for an unprotected listener is told so
        // by a process that has touched nothing, rather than by one
        // that has already taken the store's file lock. `AdminServer::
        // bind` checks again for the caller that never came through
        // this file.
        //
        // Only when it is going to be served. A listener that is
        // turned off exposes nothing, so refusing to start the broker
        // over the address it would not have bound would be a refusal
        // with nothing behind it.
        if self.admin.enabled {
            self.admin.check()?;
        }

        // **A rule with no store is a rule that silently loses
        // messages.** An envelope goes into a durable outbox before the
        // publisher is answered, and without `storage.directory` there
        // is nothing durable to put it in: every message in flight when
        // the process ends is gone, which is the one thing this
        // connector exists not to do. Refused rather than warned about,
        // unlike running with no store at all — that is a supported
        // mode, and declaring a rule is an operator saying they want
        // messages delivered somewhere else.
        if !self.rules.is_empty() && self.storage.directory.is_none() {
            return Err(ConfigError::Unusable {
                key: "storage.directory",
                value: "unset".to_string(),
                why: "a rule needs a durable outbox, so rules require a store directory",
            });
        }

        // Two rules under one identifier is one rule's outbox, one
        // rule's dead-letter topic and two rules' messages. The store
        // would accept it — a put replaces — and the second would
        // silently take the first's place.
        let mut seen = BTreeMap::new();
        for section in &self.rules {
            // Everything an operator can get wrong about a rule, said
            // once here with the rule named, so that a broker never
            // starts with a connector that cannot post.
            section.to_rule()?;
            if seen.insert(section.id.clone(), ()).is_some() {
                return Err(ConfigError::Rule {
                    id: section.id.clone(),
                    why: "two rules share this identifier; each rule needs its own".to_string(),
                });
            }
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
        if let Some(value) = lookup("SDMQTT_STORAGE_DIRECTORY") {
            // An empty value turns the store off rather than naming a
            // directory called "". It is the only way an environment
            // can undo a directory a file set, and `create_dir_all("")`
            // would otherwise fail with an error naming nothing.
            self.storage.directory = (!value.is_empty()).then(|| PathBuf::from(value));
        }
        if let Some(value) = lookup("SDMQTT_STORAGE_SYNC") {
            self.storage.sync = parse("SDMQTT_STORAGE_SYNC", &value, "sync mode")?;
        }
        if let Some(value) = lookup("SDMQTT_STORAGE_FLUSH_INTERVAL_MS") {
            self.storage.flush_interval_ms = parse(
                "SDMQTT_STORAGE_FLUSH_INTERVAL_MS",
                &value,
                "millisecond count",
            )?;
        }
        if let Some(value) = lookup("SDMQTT_STORAGE_FLUSH_RECORDS") {
            self.storage.flush_records =
                parse("SDMQTT_STORAGE_FLUSH_RECORDS", &value, "record count")?;
        }
        if let Some(value) = lookup("SDMQTT_ADMIN_ADDRESS") {
            self.admin.address = parse("SDMQTT_ADMIN_ADDRESS", &value, "socket address")?;
        }
        if let Some(value) = lookup("SDMQTT_ADMIN_ENABLED") {
            self.admin.enabled = parse("SDMQTT_ADMIN_ENABLED", &value, "true or false")?;
        }
        if let Some(value) = lookup("SDMQTT_ADMIN_TOKEN") {
            // **Kept, empty and all, so that `validate` refuses it.**
            //
            // This read it as absent, by analogy with
            // `SDMQTT_STORAGE_DIRECTORY`. The analogy does not hold.
            // An empty directory means "no store", which is a state
            // the broker has and documents; an empty token means "a
            // token is configured" while protecting nothing, and the
            // file's reading of exactly the same characters already
            // refuses to start over it. One value read two ways in
            // one program is the thing the empty-store-path fix
            // corrected, and the safe reading is the refusal: an
            // operator whose `SDMQTT_ADMIN_TOKEN` expanded to nothing
            // was told the listener was guarded by a listener that
            // had quietly decided it was not.
            //
            // The cost is that an environment can no longer take away
            // a token a file set. That is not a loss worth having:
            // the only thing it could accomplish is making a
            // protected listener unprotected, which is what
            // `admin.enabled = false` and a loopback address are for.
            self.admin.token = Some(Token::new(value));
        }
        if let Some(value) = lookup("SDMQTT_ADMIN_ALLOWED_HOSTS") {
            // Comma separated, because an environment holds strings
            // and this is the one key of the set that is a list. An
            // empty value is an empty list, which is the default and
            // is a working configuration: an address literal and
            // `localhost` are answered whatever is in here.
            self.admin.allowed_hosts = value
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(ToOwned::to_owned)
                .collect();
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
        if let Some(directory) = &overrides.store_directory {
            self.storage.directory = Some(directory.clone());
        }
        if let Some(address) = overrides.admin_address {
            self.admin.address = address;
        }
        if overrides.no_admin {
            self.admin.enabled = false;
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
                max_hops: self.broker.max_hops,
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

    // ---- rules ---------------------------------------------------------

    /// A file with a store and one rule in it.
    fn with_rule(rule: &str) -> String {
        format!("[storage]\ndirectory = \"/tmp/sdmqtt-test\"\n\n{rule}")
    }

    /// A rule whose only stated keys are the required ones.
    const MINIMAL: &str = r#"
        [[rules]]
        id = "telemetry"
        name = "Telemetry out"
        filter = "sensor/+/temp"

        [rules.http_out]
        url = "https://example.invalid/hook"
    "#;

    #[track_caller]
    fn refuse(text: &str) -> ConfigError {
        let mut config = Config::from_toml(text).expect("the file parses");
        config
            .storage
            .directory
            .get_or_insert_with(|| PathBuf::from("/tmp/sdmqtt-test"));
        config
            .validate()
            .expect_err("this configuration must be refused")
    }

    /// An ingest rule whose only stated keys are the required ones.
    const MINIMAL_INGEST: &str = r#"
        [[rules]]
        id = "gateway"
        name = "Gateway in"

        [rules.http_in]
        token = "a-secret"
        topic = "ingest/gateway"
    "#;

    // ---- http_in -------------------------------------------------------

    #[test]
    fn an_ingest_rule_takes_the_defaults_for_everything_it_does_not_say() {
        let config = Config::from_toml(&with_rule(MINIMAL_INGEST)).expect("every key is known");
        config.validate().expect("a minimal ingest rule is usable");

        let rule = config.rules[0].to_rule().expect("it converts");
        assert_eq!(rule.id.as_str(), "gateway");
        assert!(
            rule.trigger.is_none(),
            "an ingest rule is fired by a request and has no trigger"
        );
        let Action::HttpIn(action) = &rule.action else {
            panic!("this rule accepts posts");
        };
        assert_eq!(action.topic, "ingest/gateway");
        assert_eq!(
            action.qos,
            QoS::AtMostOnce,
            "the level that promises nothing"
        );
        assert!(!action.retain);
        assert!(
            action.topic_from.is_none(),
            "and no caller may choose a topic unless the rule says so"
        );
    }

    #[test]
    fn every_part_of_an_ingest_rule_can_be_said() {
        let text = with_rule(
            r#"
            [[rules]]
            id = "gateway"
            name = "Gateway in"
            enabled = false

            [rules.http_in]
            token = "a-secret"
            topic = "ingest/gateway/default"
            topic_header = "x-topic"
            topic_prefix = "ingest/gateway"
            qos = 1
            retain = true
        "#,
        );
        let config = Config::from_toml(&text).expect("every key is known");
        config.validate().expect("it is usable");
        let rule = config.rules[0].to_rule().expect("it converts");

        assert!(!rule.enabled);
        let Action::HttpIn(action) = &rule.action else {
            panic!("this rule accepts posts");
        };
        assert_eq!(action.token.expose(), "a-secret");
        assert_eq!(action.topic, "ingest/gateway/default");
        assert_eq!(action.qos, QoS::AtLeastOnce);
        assert!(action.retain);
        let from = action.topic_from.as_ref().expect("a caller may choose");
        assert_eq!(from.source, TopicSource::Header("x-topic".to_owned()));
        assert_eq!(from.prefix, "ingest/gateway");
    }

    /// Everything an operator can get wrong about an ingest rule,
    /// refused at startup with the rule named.
    ///
    /// One test rather than eleven, because what is being asserted is
    /// the same thing in each case — the broker does not start and says
    /// which rule — and a table makes the list of cases readable as a
    /// list. Each entry carries the fragment of the refusal that says
    /// *which* mistake it was, so this is not convention 2's "assert
    /// the category" wearing a table.
    #[test]
    fn what_an_ingest_rule_may_not_say_is_refused_by_name() {
        let cases: &[(&str, &str, &str)] = &[
            (
                "no token at all",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                [rules.http_in]
                topic = "ingest/gateway"
                "#,
                "token",
            ),
            (
                "a token that is present and empty",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                [rules.http_in]
                token = ""
                topic = "ingest/gateway"
                "#,
                "token",
            ),
            (
                "a header the caller may choose a topic in, with nothing to bound it",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                [rules.http_in]
                token = "a-secret"
                topic = "ingest/gateway"
                topic_header = "x-topic"
                "#,
                "topic_prefix",
            ),
            (
                "a query parameter, with nothing to bound it",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                [rules.http_in]
                token = "a-secret"
                topic = "ingest/gateway"
                topic_query = "t"
                "#,
                "topic_prefix",
            ),
            (
                "a prefix with nothing to bound",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                [rules.http_in]
                token = "a-secret"
                topic = "ingest/gateway"
                topic_prefix = "ingest"
                "#,
                "topic_prefix",
            ),
            (
                "both a header and a query parameter",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                [rules.http_in]
                token = "a-secret"
                topic = "ingest/gateway"
                topic_header = "x-topic"
                topic_query = "t"
                topic_prefix = "ingest"
                "#,
                "one of them",
            ),
            (
                "a prefix in the reserved namespace",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                [rules.http_in]
                token = "a-secret"
                topic = "ingest/gateway"
                topic_query = "t"
                topic_prefix = "$sd"
                "#,
                "dollar-prefixed",
            ),
            (
                "a prefix with a wildcard in it",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                [rules.http_in]
                token = "a-secret"
                topic = "ingest/gateway"
                topic_query = "t"
                topic_prefix = "ingest/#"
                "#,
                "topic_prefix",
            ),
            (
                "a topic that is not one",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                [rules.http_in]
                token = "a-secret"
                topic = "ingest/+"
                "#,
                "http_in.topic",
            ),
            (
                "a level MQTT does not have",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                [rules.http_in]
                token = "a-secret"
                topic = "ingest/gateway"
                qos = 3
                "#,
                "http_in.qos",
            ),
            (
                "a filter, which an ingest rule is not fired by",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                filter = "sensor/+/temp"
                [rules.http_in]
                token = "a-secret"
                topic = "ingest/gateway"
                "#,
                "remove it",
            ),
            (
                "both actions, where section 9.1 allows one",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                filter = "sensor/+/temp"
                [rules.http_in]
                token = "a-secret"
                topic = "ingest/gateway"
                [rules.http_out]
                url = "https://example.invalid/hook"
                "#,
                "two rules",
            ),
            (
                "no action at all",
                r#"
                [[rules]]
                id = "gateway"
                name = "Gateway in"
                filter = "sensor/+/temp"
                "#,
                "needs an action",
            ),
            (
                "an http_out rule with no filter",
                r#"
                [[rules]]
                id = "telemetry"
                name = "Telemetry out"
                [rules.http_out]
                url = "https://example.invalid/hook"
                "#,
                "set filter",
            ),
        ];

        for (what, text, fragment) in cases {
            let error = refuse(&with_rule(text));
            let said = error.to_string();
            assert!(
                said.contains("gateway") || said.contains("telemetry"),
                "{what}: the refusal must name the rule: {said}"
            );
            assert!(
                said.contains(fragment),
                "{what}: the refusal must say which mistake it was, and {fragment:?} is \
                 not in {said:?}"
            );
        }
    }

    /// The minimal rules above are accepted, so the table is not
    /// passing against a check that refuses everything.
    #[test]
    fn a_complete_ingest_rule_is_accepted() {
        Config::from_toml(&with_rule(MINIMAL_INGEST))
            .expect("every key is known")
            .validate()
            .expect("a minimal ingest rule is usable");
        let bounded = with_rule(
            r#"
            [[rules]]
            id = "gateway"
            name = "Gateway in"
            [rules.http_in]
            token = "a-secret"
            topic = "ingest/gateway/default"
            topic_query = "t"
            topic_prefix = "ingest/gateway"
            "#,
        );
        Config::from_toml(&bounded)
            .expect("every key is known")
            .validate()
            .expect("a rule that lets a caller choose, bounded, is usable");
    }

    /// Section 12.3, on the way in: the configuration is logged at
    /// startup and this is in it.
    #[test]
    fn an_ingest_token_does_not_print_itself() {
        let config = Config::from_toml(&with_rule(MINIMAL_INGEST)).expect("every key is known");
        let shown = format!("{config:?}");
        assert!(
            !shown.contains("a-secret"),
            "a token must not reach the first log line this binary writes: {shown}"
        );
    }

    // ---- mqtt_bridge ---------------------------------------------------

    /// A minimal bridge, which is the shape everything below is a
    /// mistake in.
    const MINIMAL_BRIDGE: &str = r##"
        [[rules]]
        id = "to-b"
        name = "Bridge to B"

        [rules.mqtt_bridge]
        address = "mqtts://remote.invalid:8883"
        client_id = "bridge-a"

        [[rules.mqtt_bridge.topics]]
        filter = "sensors/#"
        direction = "out"
        qos = 1
        remote_prefix = "from-a/"
        "##;

    #[test]
    fn a_bridge_table_reads_into_the_action() {
        let config = Config::from_toml(&with_rule(MINIMAL_BRIDGE)).expect("every key is known");
        let rule = config.rules[0].to_rule().expect("a usable bridge");
        let Action::MqttBridge(bridge) = &rule.action else {
            panic!("this rule bridges");
        };
        assert_eq!(bridge.address, "mqtts://remote.invalid:8883");
        assert_eq!(bridge.client_id, "bridge-a");
        assert_eq!(bridge.version, mqtt_codec::Version::V5);
        assert_eq!(bridge.topics.len(), 1);
        assert_eq!(bridge.topics[0].direction, Direction::Out);
        assert_eq!(bridge.topics[0].qos, QoS::AtLeastOnce);
        assert_eq!(bridge.topics[0].remote_prefix, "from-a/");
        assert!(rule.trigger.is_none(), "a bridge has no rule-level filter");
    }

    /// Every refusal the section makes, each named so a failure says
    /// which mistake it was rather than only that there was one —
    /// convention 2.
    #[test]
    fn a_bridge_that_cannot_work_is_refused_with_its_own_reason() {
        let cases: &[(&str, &str, &str)] = &[
            (
                "a filter that loops through a wildcard",
                r##"
                [[rules.mqtt_bridge.topics]]
                filter = "sensors/+/temp"
                direction = "out"
                [[rules.mqtt_bridge.topics]]
                filter = "sensors/kitchen/#"
                direction = "in"
                "##,
                "mqtt_bridge loops",
            ),
            (
                "a direction that is not one",
                r##"
                [[rules.mqtt_bridge.topics]]
                filter = "a/#"
                direction = "sideways"
                "##,
                "is not a direction",
            ),
            (
                "a level that is not one",
                r##"
                [[rules.mqtt_bridge.topics]]
                filter = "a/#"
                direction = "out"
                qos = 3
                "##,
                "is not a level",
            ),
            ("a bridge that carries nothing", "", "no topic entries"),
        ];
        for (what, topics, fragment) in cases {
            let text = with_rule(&format!(
                r##"
                [[rules]]
                id = "to-b"
                name = "Bridge to B"
                [rules.mqtt_bridge]
                address = "mqtts://remote.invalid:8883"
                client_id = "bridge-a"
                {topics}
                "##
            ));
            let said = Config::from_toml(&text)
                .expect("every key is known")
                .validate()
                .expect_err(&format!("{what} must be refused"))
                .to_string();
            assert!(
                said.contains(fragment),
                "{what}: the refusal must say which mistake it was, and {fragment:?} is \
                 not in {said:?}"
            );
        }
    }

    /// Refused when the configuration is read rather than discovered
    /// on the wire: MQTT 3.1.1 has no way for the remote to say what
    /// identifier it assigned, so the session could never be resumed.
    #[test]
    fn a_3_1_1_bridge_with_no_client_identifier_is_refused() {
        let text = with_rule(
            r##"
            [[rules]]
            id = "to-b"
            name = "Bridge to B"
            [rules.mqtt_bridge]
            address = "mqtts://remote.invalid:8883"
            protocol_level = 4
            [[rules.mqtt_bridge.topics]]
            filter = "a/#"
            direction = "out"
            "##,
        );
        let said = Config::from_toml(&text)
            .expect("every key is known")
            .validate()
            .expect_err("3.1.1 cannot assign one")
            .to_string();
        assert!(said.contains("client_id"), "{said}");

        // And the same rule on MQTT 5 is accepted, because there the
        // remote does say.
        let five = text.replace("protocol_level = 4", "protocol_level = 5");
        Config::from_toml(&five)
            .expect("every key is known")
            .validate()
            .expect("MQTT 5 assigns one and says so");
    }

    /// Section 12.3, on the way in: the configuration is logged at
    /// startup and a bridge's password is in it.
    #[test]
    fn a_bridge_password_does_not_print_itself() {
        let text = with_rule(
            r##"
            [[rules]]
            id = "to-b"
            name = "Bridge to B"
            [rules.mqtt_bridge]
            address = "mqtts://remote.invalid:8883"
            client_id = "bridge-a"
            username = "user"
            password = "a-bridge-secret"
            [[rules.mqtt_bridge.topics]]
            filter = "a/#"
            direction = "out"
            "##,
        );
        let config = Config::from_toml(&text).expect("every key is known");
        let shown = format!("{config:?}");
        assert!(
            !shown.contains("a-bridge-secret"),
            "a password must not reach the first log line this binary writes: {shown}"
        );
    }

    /// Section 9.1: one action. A rule that declares a bridge and
    /// something else is an operator who means two rules.
    #[test]
    fn a_rule_that_bridges_and_posts_is_refused() {
        let text = with_rule(
            r##"
            [[rules]]
            id = "two"
            name = "Two things"
            filter = "a/#"
            [rules.http_out]
            url = "https://example.invalid/hook"
            [rules.mqtt_bridge]
            address = "mqtts://remote.invalid:8883"
            client_id = "bridge-a"
            [[rules.mqtt_bridge.topics]]
            filter = "a/#"
            direction = "out"
            "##,
        );
        let said = Config::from_toml(&text)
            .expect("every key is known")
            .validate()
            .expect_err("a rule has one action")
            .to_string();
        assert!(said.contains("write two rules"), "{said}");
    }

    // ---- the ingest listener -------------------------------------------

    #[test]
    fn the_ingest_listener_defaults_to_one_of_its_own_on_loopback() {
        let config = Config::default();
        assert_eq!(config.ingest.listener, IngestListener::Own);
        assert!(
            config.ingest.address.ip().is_loopback(),
            "section 9.2: a listener of its own, and loopback until an operator says \
             otherwise"
        );
        assert_eq!(config.ingest.address.port(), DEFAULT_INGEST_PORT);
    }

    #[test]
    fn the_ingest_listener_can_be_moved_or_put_on_the_admin_listener() {
        let config = Config::from_toml(
            r#"
            [ingest]
            address = "0.0.0.0:9000"
            listener = "admin"
            "#,
        )
        .expect("every key is known");
        assert_eq!(config.ingest.address.to_string(), "0.0.0.0:9000");
        assert_eq!(config.ingest.listener, IngestListener::Admin);
    }

    /// A non-loopback ingest address with nothing else set is accepted.
    ///
    /// Deliberately unlike `[admin]`, and the difference is the point.
    /// Section 9.2: the ingest listener needs no address rule of its
    /// own, because every route on it is guarded by its own rule's
    /// token and a rule without one is refused. A refusal here would be
    /// a rule protecting something that is already protected, and would
    /// stop the deployment the feature exists for — a webhook sender on
    /// another host.
    #[test]
    fn a_non_loopback_ingest_address_needs_no_token_of_its_own() {
        let text = format!(
            "[ingest]\naddress = \"0.0.0.0:9000\"\n{}",
            with_rule(MINIMAL_INGEST)
        );
        Config::from_toml(&text)
            .expect("every key is known")
            .validate()
            .expect("every ingest route is individually guarded");
    }

    // ---- http_poll -----------------------------------------------------

    /// Every part of a polling rule can be said, and reaches the rules
    /// crate's own type.
    #[test]
    fn every_part_of_a_polling_rule_can_be_said() {
        let text = with_rule(
            r#"
            [[rules]]
            id = "weather"
            name = "Weather poll"

            [rules.http_poll]
            url = "https://example.invalid/now"
            headers = { "accept" = "text/plain" }
            interval_ms = 30000
            timeout_ms = 2500
            max_body_bytes = 65536
            topic = "weather/now"
            qos = 1
            retain = true
            "#,
        );
        let config = Config::from_toml(&text).expect("every key is known");
        config
            .validate()
            .expect("a complete polling rule is usable");

        let rule = config.rules[0].to_rule().expect("it converts");
        assert_eq!(rule.id.as_str(), "weather");
        assert!(
            rule.trigger.is_none(),
            "a poller is fired by its own interval and has no trigger"
        );
        let Action::HttpPoll(action) = &rule.action else {
            panic!("this rule polls: {:?}", rule.action);
        };
        assert_eq!(action.url, "https://example.invalid/now");
        assert_eq!(
            action.headers,
            vec![("accept".to_owned(), "text/plain".to_owned())]
        );
        assert_eq!(action.interval_ms, 30_000);
        assert_eq!(action.timeout_ms, 2_500);
        assert_eq!(action.max_body_bytes, 65_536);
        assert_eq!(action.topic, "weather/now");
        assert_eq!(action.qos, QoS::AtLeastOnce);
        assert!(action.retain);

        // And it survives the store, which is where it actually lives.
        let stored = rule.to_record();
        assert_eq!(
            Rule::try_from(&stored).expect("it reads back"),
            rule,
            "a polling rule round trips through the record"
        );
    }

    /// A polling rule says the two things it must and takes the rest
    /// from the rules crate.
    #[test]
    fn a_polling_rule_takes_the_rules_crate_defaults() {
        let text = with_rule(
            r#"
            [[rules]]
            id = "weather"
            name = "Weather poll"

            [rules.http_poll]
            url = "https://example.invalid/now"
            topic = "weather/now"
            "#,
        );
        let config = Config::from_toml(&text).expect("every key is known");
        config.validate().expect("a minimal polling rule is usable");

        let defaults = HttpPoll::default();
        let poll = config.rules[0].http_poll.as_ref().expect("this rule polls");
        assert_eq!(poll.interval_ms, defaults.interval_ms);
        assert_eq!(poll.timeout_ms, defaults.timeout_ms);
        assert_eq!(poll.max_body_bytes, defaults.max_body_bytes);
        assert!(!poll.retain, "a retained default outlives the rule");
    }

    /// What a polling rule may not say is refused, with the rule named
    /// and the mistake said.
    ///
    /// The accepted rule above is what stops this passing against a
    /// check that refuses everything. Convention 5.
    #[test]
    fn what_a_polling_rule_may_not_say_is_refused_by_name() {
        let cases: &[(&str, &str, &str)] = &[
            (
                "a plain-text url to somewhere that is not this machine",
                r#"
                [[rules]]
                id = "weather"
                name = "Weather poll"
                [rules.http_poll]
                url = "http://example.invalid/now"
                topic = "weather/now"
                "#,
                "in the clear",
            ),
            (
                "an interval of zero",
                r#"
                [[rules]]
                id = "weather"
                name = "Weather poll"
                [rules.http_poll]
                url = "https://example.invalid/now"
                topic = "weather/now"
                interval_ms = 0
                "#,
                "interval_ms is zero",
            ),
            (
                "a body limit of zero",
                r#"
                [[rules]]
                id = "weather"
                name = "Weather poll"
                [rules.http_poll]
                url = "https://example.invalid/now"
                topic = "weather/now"
                max_body_bytes = 0
                "#,
                "max_body_bytes is zero",
            ),
            (
                "a topic with a wildcard in it",
                r#"
                [[rules]]
                id = "weather"
                name = "Weather poll"
                [rules.http_poll]
                url = "https://example.invalid/now"
                topic = "weather/+"
                "#,
                "http_poll.topic",
            ),
            (
                "a topic in the reserved namespace",
                r#"
                [[rules]]
                id = "weather"
                name = "Weather poll"
                [rules.http_poll]
                url = "https://example.invalid/now"
                topic = "$sd/weather"
                "#,
                "dollar-prefixed",
            ),
            (
                "a level MQTT does not have",
                r#"
                [[rules]]
                id = "weather"
                name = "Weather poll"
                [rules.http_poll]
                url = "https://example.invalid/now"
                topic = "weather/now"
                qos = 3
                "#,
                "http_poll.qos",
            ),
            (
                "a filter, on a rule nothing published fires",
                r#"
                [[rules]]
                id = "weather"
                name = "Weather poll"
                filter = "weather/#"
                [rules.http_poll]
                url = "https://example.invalid/now"
                topic = "weather/now"
                "#,
                "its own interval",
            ),
            (
                "a narrowing with no filter to narrow",
                r#"
                [[rules]]
                id = "weather"
                name = "Weather poll"
                min_qos = 1
                [rules.http_poll]
                url = "https://example.invalid/now"
                topic = "weather/now"
                "#,
                "an http_poll rule has",
            ),
            (
                "two actions, where section 9.1 allows one",
                r#"
                [[rules]]
                id = "weather"
                name = "Weather poll"
                filter = "weather/#"
                [rules.http_poll]
                url = "https://example.invalid/now"
                topic = "weather/now"
                [rules.http_out]
                url = "https://example.invalid/hook"
                "#,
                "two rules",
            ),
        ];

        for (what, text, fragment) in cases {
            let error = refuse(&with_rule(text));
            let said = error.to_string();
            assert!(
                said.contains("weather"),
                "{what}: the refusal must name the rule: {said}"
            );
            assert!(
                said.contains(fragment),
                "{what}: the refusal must say which mistake it was, and {fragment:?} is \
                 not in {said:?}"
            );
        }
    }

    /// A rule with no action at all names every table it could have
    /// been.
    ///
    /// An operator who wrote none needs the list, and a list that goes
    /// stale as actions are added is worse than no list; this is what
    /// notices.
    #[test]
    fn a_rule_with_no_action_names_every_action_there_is() {
        let said = refuse(&with_rule(
            r#"
            [[rules]]
            id = "weather"
            name = "Weather poll"
            "#,
        ))
        .to_string();
        for table in ["[rules.http_out]", "[rules.http_in]", "[rules.http_poll]"] {
            assert!(
                said.contains(table),
                "a rule with no action must name {table}: {said}"
            );
        }
    }

    #[test]
    fn a_rule_takes_the_rules_crate_defaults_for_everything_it_does_not_say() {
        let config = Config::from_toml(&with_rule(MINIMAL)).expect("every key is known");
        config.validate().expect("a minimal rule is usable");

        assert_eq!(config.rules.len(), 1);
        let section = &config.rules[0];
        assert!(section.enabled, "a rule left alone is on");
        assert_eq!(section.min_qos, None, "and is not narrowed by level");
        assert_eq!(section.client_id, None);

        let defaults = HttpOut::default();
        let out = section.http_out.as_ref().expect("this rule posts out");
        assert_eq!(out.method, defaults.method);
        assert_eq!(out.timeout_ms, defaults.timeout_ms);
        assert_eq!(out.retries, defaults.retries);
        assert_eq!(out.backoff_ms, defaults.backoff_ms);
        assert_eq!(out.max_concurrency, defaults.max_concurrency);

        let rule = section.to_rule().expect("it converts");
        assert_eq!(rule.id.as_str(), "telemetry");
        assert_eq!(
            rule.trigger.as_ref().map(Trigger::filter),
            Some("sensor/+/temp")
        );
        assert_eq!(rule.id.dead_letter_topic(), "$sd/rules/telemetry/dlq");
    }

    #[test]
    fn every_part_of_a_rule_can_be_said() {
        let text = with_rule(
            r#"
            [[rules]]
            id = "telemetry"
            name = "Telemetry out"
            enabled = false
            filter = "sensor/+/temp"
            min_qos = 2
            client_id = "sensor-*"

            [rules.http_out]
            url = "https://example.invalid/hook"
            method = "PUT"
            timeout_ms = 2500
            max_concurrency = 8
            retries = 5
            backoff_ms = 750
            headers = { "x-token" = "secret" }
        "#,
        );
        let config = Config::from_toml(&text).expect("every key is known");
        config.validate().expect("it is usable");
        let rule = config.rules[0].to_rule().expect("it converts");

        assert!(!rule.enabled);
        let trigger = rule.trigger.as_ref().expect("this rule fires on messages");
        assert_eq!(trigger.min_qos, Some(QoS::ExactlyOnce));
        assert_eq!(
            trigger.client_id.as_ref().map(ClientPattern::as_str),
            Some("sensor-*")
        );
        let Action::HttpOut(action) = &rule.action else {
            panic!("this rule posts out");
        };
        assert_eq!(action.method, "PUT");
        assert_eq!(action.timeout_ms, 2_500);
        assert_eq!(action.max_concurrency, 8);
        assert_eq!(action.retries, 5);
        assert_eq!(action.backoff_ms, 750);
        assert_eq!(
            action.headers,
            vec![("x-token".to_owned(), "secret".to_owned())]
        );
    }

    /// A rule with nowhere durable to put an envelope is refused.
    ///
    /// Running with no store at all stays a warning, because that is a
    /// supported mode. Declaring a rule is an operator saying they want
    /// messages delivered somewhere else, and an outbox that is not
    /// written down loses every one that was in flight.
    #[test]
    fn a_rule_without_a_store_is_refused() {
        let mut config = Config::from_toml(MINIMAL).expect("the file parses");
        assert!(config.storage.directory.is_none());
        let error = config
            .validate()
            .expect_err("a rule with no store must be refused");
        assert!(
            matches!(&error, ConfigError::Unusable { key, .. } if *key == "storage.directory"),
            "said: {error}"
        );

        // And the same file with a store is fine, so the refusal is
        // about the store and not about the rule.
        config.storage.directory = Some(PathBuf::from("/tmp/sdmqtt-test"));
        config.validate().expect("with a store it is usable");
    }

    #[test]
    fn two_rules_under_one_identifier_are_refused() {
        let error = refuse(&with_rule(&format!("{MINIMAL}\n{MINIMAL}")));
        assert!(
            matches!(&error, ConfigError::Rule { id, .. } if id == "telemetry"),
            "said: {error}"
        );
    }

    /// Everything a rule can be wrong about is refused at startup, by
    /// name, rather than becoming a rule that quietly never fires.
    #[test]
    fn a_rule_that_cannot_post_stops_the_broker_starting() {
        for (what, rule) in [
            (
                "a plain-text url to somewhere that is not this machine",
                MINIMAL.replace(
                    "https://example.invalid/hook",
                    "http://example.invalid/hook",
                ),
            ),
            (
                "a url that is not one",
                MINIMAL.replace("https://example.invalid/hook", "not a url"),
            ),
            (
                "a method that is not one",
                format!("{MINIMAL}\n        method = \"NOT A METHOD\"\n"),
            ),
            (
                "a header name that cannot be sent",
                format!("{MINIMAL}\n        headers = {{ \"not a name\" = \"v\" }}\n"),
            ),
            (
                "an identifier with a topic separator in it",
                MINIMAL.replace(r#"id = "telemetry""#, r#"id = "a/b""#),
            ),
            (
                "a filter that is not one",
                MINIMAL.replace(
                    r#"filter = "sensor/+/temp""#,
                    r#"filter = "sensor/+x/temp""#,
                ),
            ),
            (
                "a level MQTT does not have",
                MINIMAL.replace(
                    r#"filter = "sensor/+/temp""#,
                    "filter = \"sensor/+/temp\"\n        min_qos = 3",
                ),
            ),
            (
                "a concurrency of zero, which is a rule that never posts",
                format!("{MINIMAL}\n        max_concurrency = 0\n"),
            ),
        ] {
            let error = refuse(&with_rule(&rule));
            assert!(
                matches!(&error, ConfigError::Rule { id, .. } if id == "telemetry" || id == "a/b"),
                "{what}: said {error}"
            );
        }
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

            [admin]
            enabled = true
            address = "127.0.0.1:18840"
            token = "a-secret"

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
        assert!(config.admin.enabled);
        assert_eq!(config.admin.address.to_string(), "127.0.0.1:18840");
        assert_eq!(
            config.admin.token.as_ref().map(Token::expose),
            Some("a-secret")
        );

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
            store_directory: Some(PathBuf::from("/var/lib/sdmqtt")),
            admin_address: Some("127.0.0.1:12345".parse().unwrap()),
            no_admin: true,
            log_level: Some(LogLevel::Warn),
        });

        assert_eq!(config.listener.address.to_string(), "127.0.0.1:18832");
        assert_eq!(config.listener.max_connections, 13);
        assert_eq!(config.listener.mountpoint, "tenant-c");
        assert_eq!(
            config.storage.directory.as_deref(),
            Some(Path::new("/var/lib/sdmqtt"))
        );
        assert_eq!(config.log.level, LogLevel::Warn);
        assert_eq!(config.admin.address.to_string(), "127.0.0.1:12345");
        assert!(!config.admin.enabled);
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

    // ----------------------------------------------------------------
    // Section 7.2 and 7.5: where the store is, and what happens
    // without one.
    // ----------------------------------------------------------------

    #[test]
    fn no_configured_directory_means_no_store() {
        // The decision, written as an assertion rather than only as a
        // paragraph. `sdmqtt serve` with no arguments and no file
        // starts a working broker, and it starts one that writes
        // nothing down.
        let config = Config::from_toml("").expect("an empty file is a valid one");
        assert_eq!(config.storage.directory, None);
        assert!(
            config
                .storage
                .open()
                .expect("an absent store is not a failure")
                .is_none(),
            "no directory opens no store rather than inventing a path"
        );
    }

    #[test]
    fn the_storage_defaults_are_what_the_documentation_says() {
        // Immediate, two milliseconds, a thousand records: the three
        // numbers the field documentation states. Asserted against
        // the writer's own defaults as well as against the literals,
        // so that a change on either side has to be made on both.
        let storage = StorageSection::default();
        assert_eq!(storage.sync, Sync::Immediate);
        assert_eq!(storage.flush_interval_ms, 2);
        assert_eq!(storage.flush_records, 1_000);
        assert_eq!(storage.writer(), WriterConfig::default());
        assert_eq!(storage.sync.to_storage(), storage::SyncMode::Immediate);
    }

    #[test]
    fn every_storage_key_parses_and_reaches_the_writer() {
        // Convention: parsed is not the same as applied. A key read
        // into this struct and never handed to the writer is a
        // setting that does nothing, which is worse than one that was
        // never offered.
        let text = r#"
            [storage]
            directory = "/var/lib/sdmqtt"
            sync = "relaxed"
            flush_interval_ms = 25
            flush_records = 7
        "#;
        let config = Config::from_toml(text).expect("every key is known");
        assert_eq!(
            config.storage.directory.as_deref(),
            Some(Path::new("/var/lib/sdmqtt"))
        );
        assert_eq!(config.storage.sync, Sync::Relaxed);
        assert_eq!(config.storage.flush_interval_ms, 25);
        assert_eq!(config.storage.flush_records, 7);

        let writer = config.storage.writer();
        assert_eq!(writer.interval, Duration::from_millis(25));
        assert_eq!(writer.records, 7);
        assert_eq!(config.storage.sync.to_storage(), storage::SyncMode::Relaxed);
    }

    #[test]
    fn an_unknown_storage_key_is_an_error() {
        let error = Config::from_toml("[storage]\ndirectroy = \"/tmp/x\"\n")
            .expect_err("a misspelled key that is dropped leaves the broker with no store");
        assert!(
            error.to_string().contains("directroy"),
            "the message must name the key: {error}"
        );
    }

    #[test]
    fn the_configured_directory_is_where_the_store_is_opened() {
        // The directory is created if it is not there, and the store
        // is `STORE_FILE` inside it. Both halves: a test that only
        // checked `open` succeeded would pass against a store opened
        // anywhere at all.
        let parent = tempfile::tempdir().expect("a temporary directory");
        let directory = parent.path().join("not").join("there").join("yet");
        let config = StorageSection {
            directory: Some(directory.clone()),
            ..StorageSection::default()
        };

        let store = config
            .open()
            .expect("a directory that can be made is made")
            .expect("a configured directory opens a store");
        assert!(
            directory.join(STORE_FILE).is_file(),
            "the store is a file called {STORE_FILE} inside the configured directory"
        );
        assert!(
            store
                .restore()
                .expect("a fresh store reads")
                .sessions
                .is_empty(),
            "a store made just now holds nothing"
        );
    }

    #[test]
    fn a_directory_that_cannot_be_created_is_a_startup_failure_naming_it() {
        // Not a panic, and not a silent fall back to no store, which
        // would be the worst of the three: a broker that looks like
        // it is persisting and is not.
        let parent = tempfile::tempdir().expect("a temporary directory");
        let blocking = parent.path().join("a-file");
        std::fs::write(&blocking, b"not a directory").expect("the file is written");
        let wanted = blocking.join("store");

        let config = StorageSection {
            directory: Some(wanted.clone()),
            ..StorageSection::default()
        };
        let error = match config.open() {
            Err(error) => error,
            // `Arc<dyn Storage>` is not `Debug`, so the success side
            // is named here rather than through `expect_err`.
            Ok(_) => panic!("a file is not a directory, and opening one must not succeed"),
        };
        assert!(
            matches!(&error, ConfigError::StoreDirectory { path, .. } if path == &wanted),
            "expected the directory failure naming {wanted:?}, got {error:?}"
        );
        assert!(
            error.to_string().contains(&wanted.display().to_string()),
            "the message must name the path an operator has to fix: {error}"
        );
    }

    #[test]
    fn a_store_that_will_not_open_is_told_apart_from_a_directory_that_will_not_be_made() {
        // Convention 2: the two failures have different fixes — find
        // the other process, run the right build, reach for a backup
        // on one; look at the path on the other — so a caller
        // asserting a category would be guarding neither.
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join(STORE_FILE);
        std::fs::write(&path, b"this is not an sd-mqtt store").expect("the file is written");

        let config = StorageSection {
            directory: Some(directory.path().to_path_buf()),
            ..StorageSection::default()
        };
        let error = match config.open() {
            Err(error) => error,
            Ok(_) => panic!("some other file is not a store, and must not open as one"),
        };
        assert!(
            matches!(&error, ConfigError::StoreUnopenable { path: named, .. } if named == &path),
            "expected the open failure naming {path:?}, got {error:?}"
        );
        assert!(
            error.to_string().contains(&path.display().to_string()),
            "the message must name the store: {error}"
        );
    }

    /// Run `body` with a subscriber that keeps whatever was logged.
    ///
    /// The line is the whole feature, so the test has to read the line.
    /// Asserting on the helper that decides to write it would be
    /// convention 5's unit test of a helper: it would pass with nothing
    /// calling it.
    fn logged_by(body: impl FnOnce()) -> String {
        use std::io::Write;
        use std::sync::{Arc, Mutex};

        #[derive(Clone, Default)]
        struct Sink(Arc<Mutex<Vec<u8>>>);

        impl Write for Sink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .expect("no test panics holding it")
                    .extend(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Sink {
            type Writer = Self;

            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let sink = Sink::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            .with_max_level(tracing::Level::TRACE)
            .finish();
        // Thread-local, so tests running beside this one keep their own
        // output.
        tracing::subscriber::with_default(subscriber, body);
        let written = sink.0.lock().expect("no test panics holding it").clone();
        String::from_utf8(written).expect("tracing writes text")
    }

    /// A store in `directory` opened in `sync` mode, and what it said.
    fn opening_says(directory: &Path, sync: Sync) -> String {
        logged_by(|| {
            StorageSection {
                directory: Some(directory.to_path_buf()),
                sync,
                ..StorageSection::default()
            }
            .open()
            .expect("the store opens");
        })
    }

    #[test]
    fn opening_a_store_that_acknowledges_before_the_device_says_so() {
        // The dangerous configuration is the quiet one. A missing store
        // directory already gets a loud warning, and this is worse: the
        // broker answers every publication with a promise, and under
        // this mode nothing has reached the file.
        let directory = tempfile::tempdir().expect("a temporary directory");
        let said = opening_says(directory.path(), Sync::Relaxed);
        assert!(
            said.contains("WARN") && said.contains("relaxed"),
            "opening a relaxed store said: {said:?}"
        );
    }

    #[test]
    fn opening_a_durable_store_is_quiet_about_it() {
        // The control, and the reason the line above is worth anything:
        // a warning on every start is one an operator learns to scroll
        // past.
        let directory = tempfile::tempdir().expect("a temporary directory");
        let said = opening_says(directory.path(), Sync::Immediate);
        assert!(
            said.is_empty(),
            "the default mode keeps section 7.3's promise and needs no line, but said: {said:?}"
        );
    }

    #[test]
    fn the_environment_can_name_a_store_and_can_take_one_away() {
        // The only place an empty value means "no store": an
        // environment has no way to remove a key a file set, so it
        // needs one to say so.
        let mut config =
            Config::from_toml("[storage]\ndirectory = \"/from/the/file\"\n").expect("a valid file");
        config
            .apply_env(|variable| {
                (variable == "SDMQTT_STORAGE_DIRECTORY").then(|| "/from/the/environment".to_owned())
            })
            .expect("a readable value");
        assert_eq!(
            config.storage.directory.as_deref(),
            Some(Path::new("/from/the/environment"))
        );

        config
            .apply_env(|variable| (variable == "SDMQTT_STORAGE_DIRECTORY").then(String::new))
            .expect("an empty value is readable");
        assert_eq!(
            config.storage.directory, None,
            "an empty value turns the store off rather than naming a directory called \"\""
        );
    }

    #[test]
    fn the_environment_reads_the_rest_of_the_storage_section() {
        let mut config = Config::default();
        config
            .apply_env(|variable| match variable {
                "SDMQTT_STORAGE_SYNC" => Some("relaxed".to_owned()),
                "SDMQTT_STORAGE_FLUSH_INTERVAL_MS" => Some("11".to_owned()),
                "SDMQTT_STORAGE_FLUSH_RECORDS" => Some("13".to_owned()),
                _ => None,
            })
            .expect("all three are readable");
        assert_eq!(config.storage.sync, Sync::Relaxed);
        assert_eq!(config.storage.writer().interval, Duration::from_millis(11));
        assert_eq!(config.storage.writer().records, 13);
    }

    #[test]
    fn an_unreadable_storage_value_is_named_rather_than_ignored() {
        let mut config = Config::default();
        let error = config
            .apply_env(|variable| (variable == "SDMQTT_STORAGE_SYNC").then(|| "fsync".to_owned()))
            .expect_err("a sync mode we do not have is not a fallback");
        assert!(
            error.to_string().contains("SDMQTT_STORAGE_SYNC")
                && error.to_string().contains("fsync"),
            "said: {error}"
        );
    }

    #[test]
    fn the_command_line_overrides_the_file_and_the_environment_for_the_store() {
        let mut config =
            Config::from_toml("[storage]\ndirectory = \"/from/the/file\"\n").expect("a valid file");
        config.apply(&Overrides {
            store_directory: Some(PathBuf::from("/from/the/flag")),
            ..Overrides::default()
        });
        assert_eq!(
            config.storage.directory.as_deref(),
            Some(Path::new("/from/the/flag"))
        );
    }

    #[test]
    fn a_directory_named_as_the_empty_string_is_refused() {
        // Opposite intentions: leaving the key out asks for no store,
        // and writing an empty string is a mistake. Reading the
        // second as the first would give an operator who meant to
        // persist a broker that loses everything on a restart, with
        // no complaint.
        let config = Config::from_toml("[storage]\ndirectory = \"\"\n").expect("it parses");
        let error = config
            .validate()
            .expect_err("an empty directory is not a directory");
        assert!(
            matches!(&error, ConfigError::Unusable { key, .. } if *key == "storage.directory"),
            "expected the key to be named, got {error:?}"
        );
    }

    #[test]
    fn the_two_sync_spellings_map_both_ways() {
        // Convention 5: a round trip cannot see a consistent error, so
        // the mapping is pinned in one direction as well as covered in
        // both. Transposing these would compile, round trip, and turn
        // every acknowledgement this broker sends into a promise it
        // does not keep, against a backend whose relaxed mode reaches
        // no file at all.
        assert_eq!(Sync::Immediate.to_storage(), storage::SyncMode::Immediate);
        assert_eq!(Sync::Relaxed.to_storage(), storage::SyncMode::Relaxed);
        assert_eq!("immediate".parse::<Sync>(), Ok(Sync::Immediate));
        assert_eq!("relaxed".parse::<Sync>(), Ok(Sync::Relaxed));
        assert!(
            "Immediate".parse::<Sync>().is_err(),
            "the spelling is exact"
        );
    }

    /// Section 11.1, at the earliest place it can be enforced.
    ///
    /// The refusal belongs to the admin crate and is tested there
    /// against a real `bind`. What is tested here is that it reaches
    /// the binary: a configuration that assembles cleanly and is then
    /// served would refuse only once the store had been opened.
    #[test]
    fn a_non_loopback_admin_address_with_no_token_is_refused_by_validate() {
        let config = Config {
            admin: AdminConfig {
                address: "0.0.0.0:1884".parse().expect("a valid address"),
                token: None,
                ..AdminConfig::default()
            },
            ..Config::default()
        };
        let error = config
            .validate()
            .expect_err("an unprotected admin listener is refused");
        assert!(matches!(error, ConfigError::Admin(_)), "{error:?}");
        let said = error.to_string();
        assert!(
            said.contains("0.0.0.0:1884"),
            "it names the address: {said}"
        );
        assert!(said.contains("admin.token"), "and the key: {said}");
    }

    #[test]
    fn a_token_makes_the_same_address_acceptable() {
        let config = Config {
            admin: AdminConfig {
                address: "0.0.0.0:1884".parse().expect("a valid address"),
                token: Some(Token::new("a-secret")),
                ..AdminConfig::default()
            },
            ..Config::default()
        };
        config.validate().expect("a token protects it");
    }

    /// A listener that is not served exposes nothing.
    #[test]
    fn a_disabled_admin_listener_is_not_held_to_the_rule() {
        let config = Config {
            admin: AdminConfig {
                enabled: false,
                address: "0.0.0.0:1884".parse().expect("a valid address"),
                ..AdminConfig::default()
            },
            ..Config::default()
        };
        config
            .validate()
            .expect("nothing is bound, so there is nothing to protect");
    }

    #[test]
    fn the_environment_configures_the_admin_listener() {
        let mut config = Config::default();
        config
            .apply_env(|variable| match variable {
                "SDMQTT_ADMIN_ADDRESS" => Some("127.0.0.1:19999".to_owned()),
                "SDMQTT_ADMIN_TOKEN" => Some("from-the-environment".to_owned()),
                "SDMQTT_ADMIN_ENABLED" => Some("false".to_owned()),
                _ => None,
            })
            .expect("all three parse");
        assert_eq!(config.admin.address.to_string(), "127.0.0.1:19999");
        assert_eq!(
            config.admin.token.as_ref().map(Token::expose),
            Some("from-the-environment")
        );
        assert!(!config.admin.enabled);
    }

    /// One value, one reading, whichever source it came from.
    ///
    /// An empty token in the file is refused. It used to be read as
    /// absent when it came from the environment, which is the same
    /// value meaning two things in one program — and the dangerous
    /// one of the two, because an operator whose variable expanded to
    /// nothing was told the listener was guarded by a listener that
    /// had decided it was not.
    #[test]
    fn an_empty_token_is_refused_wherever_it_came_from() {
        let from_the_file = Config::from_toml("[admin]\ntoken = \"\"\n").expect(
            "it parses; it is validate that \
                                                                 refuses it",
        );
        let said = from_the_file
            .validate()
            .expect_err("an empty token protects nothing")
            .to_string();
        assert!(said.contains("empty string"), "{said}");

        let mut from_the_environment = Config {
            admin: AdminConfig {
                token: Some(Token::new("from-the-file")),
                ..AdminConfig::default()
            },
            ..Config::default()
        };
        from_the_environment
            .apply_env(|variable| (variable == "SDMQTT_ADMIN_TOKEN").then(String::new))
            .expect("reading it is not the error; what it says is");
        assert_eq!(
            from_the_environment.admin.token.as_ref().map(Token::expose),
            Some(""),
            "it is kept rather than dropped, so that validate can see it"
        );
        assert_eq!(
            from_the_environment
                .validate()
                .expect_err("the same value, refused the same way")
                .to_string(),
            said,
            "and refused with the same sentence, so an operator reading one \
             log line learns the same thing either way"
        );
    }

    /// `--no-admin` absent must not turn a disabled listener back on.
    #[test]
    fn an_absent_flag_does_not_re_enable_the_admin_listener() {
        let mut config = Config {
            admin: AdminConfig {
                enabled: false,
                ..AdminConfig::default()
            },
            ..Config::default()
        };
        config.apply(&Overrides::default());
        assert!(
            !config.admin.enabled,
            "the file said no and the command line said nothing"
        );
    }
}
