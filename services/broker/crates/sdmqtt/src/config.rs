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
use rules::rule::{Action, ClientPattern, HttpOut, Rule, RuleId, Trigger};
use sdmqtt_admin::config::{AdminConfig, AdminStartError, Token};
use sdmqtt_server::connection::{outbox_depth_for, ConnectionConfig};
use sdmqtt_server::listener::{ServerConfig, DEFAULT_MAX_CONNECTIONS, DEFAULT_PORT};
use serde::Deserialize;
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
    pub filter: String,
    /// The lowest level that fires it, if it is narrowed.
    pub min_qos: Option<u8>,
    /// The client identifier pattern that fires it, if it is narrowed.
    pub client_id: Option<String>,
    /// What it does. One action, section 9.1.
    pub http_out: HttpOutSection,
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
    /// startup: the identifier, the filter, the level, the URL, the
    /// scheme, the method and every header. A rule that cannot post is
    /// a broker that does not start, rather than a rule that quietly
    /// never fires.
    pub fn to_rule(&self) -> Result<Rule, ConfigError> {
        let wrong = |why: String| ConfigError::Rule {
            id: self.id.clone(),
            why,
        };

        let id = RuleId::new(self.id.clone()).map_err(|error| wrong(error.to_string()))?;
        let mut trigger =
            Trigger::new(self.filter.clone()).map_err(|error| wrong(error.to_string()))?;
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

        let action = HttpOut {
            url: self.http_out.url.clone(),
            method: self.http_out.method.clone(),
            headers: self
                .http_out
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            timeout_ms: self.http_out.timeout_ms,
            max_concurrency: self.http_out.max_concurrency,
            retries: self.http_out.retries,
            backoff_ms: self.http_out.backoff_ms,
        };
        // Before anything is stored and long before anything is posted.
        rules::http_out::check(&action).map_err(|error| wrong(error.to_string()))?;

        let mut rule = Rule::new(id, self.name.clone(), trigger, Action::HttpOut(action))
            .map_err(|error| wrong(error.to_string()))?;
        rule.enabled = self.enabled;
        Ok(rule)
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
        assert_eq!(section.http_out.method, defaults.method);
        assert_eq!(section.http_out.timeout_ms, defaults.timeout_ms);
        assert_eq!(section.http_out.retries, defaults.retries);
        assert_eq!(section.http_out.backoff_ms, defaults.backoff_ms);
        assert_eq!(section.http_out.max_concurrency, defaults.max_concurrency);

        let rule = section.to_rule().expect("it converts");
        assert_eq!(rule.id.as_str(), "telemetry");
        assert_eq!(rule.trigger.filter(), "sensor/+/temp");
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
        assert_eq!(rule.trigger.min_qos, Some(QoS::ExactlyOnce));
        assert_eq!(
            rule.trigger.client_id.as_ref().map(ClientPattern::as_str),
            Some("sensor-*")
        );
        let Action::HttpOut(action) = &rule.action;
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
