//! The sd-mqtt broker.
//!
//! `sdmqtt serve` with no arguments starts a working broker on the
//! registered MQTT port. That is the product's first promise, and this
//! file is where it becomes true: everything below it is configured
//! entirely by its defaults, so the only thing a command line has to do
//! is not get in the way.

mod config;

use std::collections::HashSet;
use std::path::PathBuf;
use std::process::ExitCode;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use broker_core::broker::Broker;
use clap::{Args, Parser, Subcommand};
use config::{Config, ConfigError, LogLevel, Overrides};
use sdmqtt_admin::{
    AdminRunning, AdminServer, AdminStartError, AdminState, ListenerProtocol, ListenerView,
    Readiness,
};
use sdmqtt_server::listener::{Running, Server};
use sdmqtt_server::shared::Shared;
use storage::record::Record;
use storage::{Storage, StorageError, Writer};

/// A distributed MQTT broker.
#[derive(Debug, Parser)]
#[command(name = "sdmqtt", version, about, long_about = None)]
struct Cli {
    /// What to do. Serving is the default, so that the shortest thing
    /// anyone can type is the thing they almost always want.
    #[command(subcommand)]
    command: Option<Command>,
}

/// The things this binary does.
#[derive(Debug, Subcommand)]
enum Command {
    /// Serve MQTT over TCP.
    Serve(Serve),
    /// Print the third-party notices for everything in this binary.
    ///
    /// Section 3 and the licence policy in
    /// `docs/research/component-selection.md`: the code compiled into
    /// this binary is not all MIT, and the licences it is under ask
    /// for their text to travel with it. Until this existed nothing
    /// in the repository carried a single one of them, and the readme
    /// pointed at a file in a release artifact that has never been
    /// built.
    ///
    /// A subcommand rather than only a URL, because the admin
    /// listener can be turned off and a user who has the binary must
    /// still be able to read what is in it.
    Licenses,
}

/// What `serve` accepts, every one of it optional.
#[derive(Debug, Default, Clone, Args)]
struct Serve {
    /// Read this configuration file instead of looking for sdmqtt.toml.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    /// Bind here, overriding the file and the environment.
    #[arg(long, value_name = "ADDRESS")]
    address: Option<std::net::SocketAddr>,
    /// Serve at most this many connections at once.
    #[arg(long, value_name = "COUNT")]
    max_connections: Option<usize>,
    /// Namespace this listener's clients.
    #[arg(long, value_name = "NAME")]
    mountpoint: Option<String>,
    /// Keep the store in this directory. Without one, nothing is
    /// written down and a restart loses every session.
    #[arg(long, value_name = "PATH")]
    store_directory: Option<PathBuf>,
    /// Bind the admin listener here, overriding the file and the
    /// environment.
    #[arg(long, value_name = "ADDRESS")]
    admin_address: Option<std::net::SocketAddr>,
    /// Do not serve the admin listener.
    #[arg(long)]
    no_admin: bool,
    /// One of error, warn, info, debug or trace.
    #[arg(long, value_name = "LEVEL")]
    log_level: Option<LogLevel>,
}

// There is deliberately no `--admin-token`. A command line is visible
// in `ps` to every user on the host and is kept in a shell's history,
// so a flag is the one source of the three that cannot hold a secret
// safely. The token comes from the configuration file or from
// `SDMQTT_ADMIN_TOKEN`.

impl Serve {
    /// What this asks the configuration to change.
    fn overrides(&self) -> Overrides {
        Overrides {
            address: self.address,
            max_connections: self.max_connections,
            mountpoint: self.mountpoint.clone(),
            store_directory: self.store_directory.clone(),
            admin_address: self.admin_address,
            no_admin: self.no_admin,
            log_level: self.log_level,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let serve = match cli.command {
        Some(Command::Serve(serve)) => serve,
        Some(Command::Licenses) => {
            // Straight to standard output, so that `sdmqtt licenses >
            // THIRD-PARTY.txt` and a pipe into a pager both do the
            // obvious thing. Nothing is logged around it: this is not
            // the broker starting.
            print!("{}", sdmqtt_admin::NOTICES);
            return ExitCode::SUCCESS;
        }
        None => Serve::default(),
    };

    match run(serve).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // Written to standard error rather than logged, because the
            // most likely failure is that the configuration was bad and
            // the subscriber it configures does not exist yet.
            //
            // Not walked as a chain of causes: every message here names
            // its own, so a walk would print each one twice.
            eprintln!("sdmqtt: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Everything that can go wrong before the broker is serving.
#[derive(Debug, thiserror::Error)]
enum StartupError {
    /// The configuration could not be assembled.
    #[error("{0}")]
    Config(#[from] ConfigError),
    /// The address could not be bound, which is usually another broker
    /// already on the port.
    #[error("cannot bind: {0}")]
    Bind(#[from] std::io::Error),
    /// The store would not say what it holds.
    ///
    /// Fatal rather than a fresh start. A store that cannot be read is
    /// a store whose sessions and queued messages are still in it, and
    /// serving clients as though they were not would destroy them the
    /// first time anything was written.
    #[error("cannot read the store: {0}")]
    Restore(#[source] StorageError),
    /// The operating system would not give us the writer's thread.
    ///
    /// There is no fallback. The writer owns a thread on purpose: a
    /// durable commit blocks, and blocking a runtime worker stalls
    /// every connection that worker was driving.
    #[error("cannot start the storage writer: {0}")]
    Writer(#[source] std::io::Error),
    /// The admin listener would not start.
    ///
    /// Its refusal to bind an unprotected address is checked by
    /// `Config::validate` before anything happens, so what reaches
    /// here is the socket: something else on the port. Fatal rather
    /// than degraded, because a broker with no metrics and no health
    /// endpoints is not a broker anything should be routing traffic
    /// to, and an operator who wanted one without them can say so.
    #[error("{0}")]
    Admin(#[from] AdminStartError),
}

/// Wait for whichever stop signal comes first, and name it.
///
/// Both are ordinary endings rather than faults. They are distinguished
/// in the log because they mean different things to whoever is reading
/// it: a termination is a deployment stopping the process, an interrupt
/// is a person at a keyboard.
///
/// Termination matters more than it looks. Docker, Kubernetes and
/// systemd all stop a process with `SIGTERM` and escalate to `SIGKILL`
/// after a grace period, so a broker that ignores it is killed on every
/// orderly stop: no session detached, no will published, and every
/// client left to discover the shutdown by timeout. The graceful
/// shutdown below would be unreachable in the one environment this is
/// built to run in.
#[cfg(unix)]
async fn stop_signal() -> Result<&'static str, std::io::Error> {
    use tokio::signal::unix::{signal, SignalKind};

    // Installed before either is awaited, so a signal arriving during
    // startup is caught rather than killing the process by default.
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        interrupted = tokio::signal::ctrl_c() => interrupted.map(|()| "SIGINT"),
        _ = terminate.recv() => Ok("SIGTERM"),
    }
}

/// Wait for an interrupt.
///
/// Tokio's named signals are unix only, so the interrupt is the
/// portable path and the only one everywhere else.
#[cfg(not(unix))]
async fn stop_signal() -> Result<&'static str, std::io::Error> {
    tokio::signal::ctrl_c().await.map(|()| "SIGINT")
}

/// The wall clock in whole seconds, as the broker counts time.
///
/// Section 7.5: the clock a restored broker starts with decides which
/// sessions outlived the outage, so it must be a scale that survives
/// the outage. A clock that started at zero each time — which is what
/// a broker with no store still does, because nothing it writes down
/// outlives it — would make every stored expiry meaningless.
///
/// A clock before the epoch reads as zero and one beyond 2106 reads as
/// the end of the range. Neither is reachable by a machine whose time
/// is set; both are better than a panic in the first line of startup.
fn now_seconds() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u32::try_from(since.as_secs()).unwrap_or(u32::MAX)
        })
}

/// Open the store, rebuild the broker from it, and start its writer.
///
/// Section 7.5 in the order it is written: read the clock, read the
/// store, rebuild, and only then hand back something a listener can be
/// given. Nothing here binds a socket, so there is no moment at which
/// a client could reach a broker that is half rebuilt.
///
/// `Ok(None)` means the configuration named no store directory, which
/// is a supported mode and not a failure; see the documentation on
/// [`config::StorageSection::directory`].
fn restore_broker(config: &Config) -> Result<Option<Arc<Shared>>, StartupError> {
    let Some(store) = config.storage.open()? else {
        return Ok(None);
    };
    restore_from(store, config).map(Some)
}

/// The same, against a store somebody else opened.
///
/// A seam, and it exists for one reason: the clock must be read
/// **before** the store is read, and the difference that makes is the
/// duration of the read. Against a store that answers in microseconds
/// the two orders are indistinguishable, so a test that could not make
/// the read slow could not tell them apart, and the rule section 7.5
/// states would be guarded by nothing.
fn restore_from(store: Arc<dyn Storage>, config: &Config) -> Result<Arc<Shared>, StartupError> {
    // **Before the restore, not after.** Section 7.5: this is what
    // decides which sessions outlived the outage, and a clock read
    // afterwards would count the read of the store itself against
    // every session's expiry — destroying, on a store large enough to
    // take seconds to read, exactly the sessions that were closest to
    // surviving.
    let now = now_seconds();
    let restored = store.restore().map_err(StartupError::Restore)?;

    let mut broker = Broker::new(config.server().broker);
    let report = broker_core::restore::restore(&mut broker, &restored, now);

    let writer = Arc::new(
        Writer::spawn(Arc::clone(&store), config.storage.writer()).map_err(StartupError::Writer)?,
    );

    // A session the restore skipped is still in the store, and nothing
    // in `broker-core` can tell it otherwise: the broker does no I/O.
    // Leaving it would hold the session and its queue on disk for the
    // life of the file, and the next restart would skip them again.
    //
    // **The removal is not enough on its own.** `Storage::apply` says a
    // `SessionRemoved` deliberately does not release the bodies of the
    // queue and inflight entries it discards, because a caller that
    // already emits those releases would then release twice and the
    // store cannot tell the two callers apart. This is that caller, so
    // it owes one release per entry: one reference is taken by the
    // `BodyPut` beside each `QueuePushed`, and it is not released when
    // the delivery moves from the queue into the window, so a queue
    // entry and an inflight entry hold one reference each. Without
    // them the count never reaches zero, the payload stays in the file
    // forever, and the restart after this one no longer sees the
    // session that referenced it.
    //
    // **One batch**, which is what matters: a batch is applied in full
    // or not at all, so the entries cannot be discarded without their
    // releases and the releases cannot be applied without the discard.
    // Split across two, a crash between them loses whichever half went
    // second, and the half that reaches the store on its own is either
    // the leak this fixes or a body deleted underneath a live entry.
    //
    // The releases go before the removal because that is the order the
    // facts happened in. The store does not require it — a release
    // names a body and not a session — but a reader of the batch
    // should not have to know that to see it is right.
    //
    // Not waited on: nothing was promised to anybody, and a removal
    // that does not survive a crash before the next flush is simply
    // done again on the restart that follows. Removing a session the
    // store no longer holds is a no-op rather than a refusal, so a
    // repeat costs nothing.
    if !report.expired.is_empty() {
        let expired: HashSet<&storage::record::SessionKey> = report.expired.iter().collect();
        let mut removals = Vec::new();
        let mut released = 0_usize;
        for held in restored
            .sessions
            .iter()
            .filter(|held| expired.contains(&held.session.key))
        {
            for body in held
                .queue
                .iter()
                .map(|entry| entry.delivery.body)
                .chain(held.inflight.iter().map(|entry| entry.delivery.body))
            {
                removals.push(Record::BodyReleased { id: body });
                released += 1;
            }
            removals.push(Record::SessionRemoved {
                session: held.session.key.clone(),
            });
        }
        tracing::info!(
            count = report.expired.len(),
            bodies_released = released,
            "sessions whose expiry passed while the broker was down are being removed"
        );
        if writer.apply(removals).is_err() {
            tracing::error!("the storage writer stopped before the expired sessions were removed");
        }
    }

    tracing::info!(
        sessions = report.sessions,
        subscriptions = report.subscriptions,
        expired = report.expired.len(),
        filters_refused = report.filters_refused,
        identifier_clashes = report.identifier_clashes,
        bodies_missing = report.bodies_missing,
        "restored from the store"
    );
    // Every number above that should be zero and is not is a
    // disagreement between what was stored and what came back, and is
    // worth more than a field in a line an operator may not be
    // reading.
    if report.filters_refused > 0 {
        tracing::warn!(
            count = report.filters_refused,
            "the store held filters this build will not accept; \
             those sessions have silently stopped receiving them"
        );
    }
    if report.identifier_clashes > 0 {
        tracing::warn!(
            count = report.identifier_clashes,
            "the stored state claimed the same packet identifier twice; \
             nothing was dropped, but the store disagrees with itself"
        );
    }
    // An error rather than a warning, unlike the two above: those lose a
    // subscription and an identifier, and this one loses a message that
    // a publisher may have been told was safe.
    if report.bodies_missing > 0 {
        tracing::error!(
            count = report.bodies_missing,
            "queued or in-flight messages named a body the store no longer holds; \
             those messages are gone and will not be delivered"
        );
    }

    Ok(Arc::new(Shared::with_store(broker, writer)))
}

/// Start the admin listener beside the MQTT one.
///
/// **After restore and beside the listener**, which is the order
/// section 11.1 asks for: nothing answers a readiness probe until
/// the store has been read, and liveness answers the moment this
/// binds. `Ok(None)` is an operator who turned it off.
///
/// It is given the readiness gates rather than its own, because what
/// readiness reports is what *startup* did and startup is here.
async fn start_admin(
    config: &Config,
    running: &Running,
    readiness: Arc<Readiness>,
) -> Result<Option<AdminRunning>, StartupError> {
    if !config.admin.enabled {
        tracing::warn!(
            "the admin listener is disabled, so this node serves no health endpoints, \
             no metrics and no API"
        );
        return Ok(None);
    }
    let state = AdminState::new(Arc::clone(running.shared()))
        .with_readiness(readiness)
        // What this process bound, which nothing can discover: a
        // socket does not know what it is for and the broker does
        // not know a listener exists. The admin listener adds its
        // own row once it knows its port.
        .with_listeners(vec![ListenerView {
            name: "mqtt".to_owned(),
            protocol: ListenerProtocol::Mqtt,
            address: running.address(),
            mountpoint: config.listener.mountpoint.clone(),
            max_connections: Some(config.listener.max_connections),
        }]);
    let admin = AdminServer::bind(&config.admin, state).await?;
    tracing::info!(
        address = %admin.address(),
        "the admin API, metrics and health endpoints are serving"
    );
    Ok(Some(admin))
}

/// Configure, restore, bind, and serve until told to stop.
async fn run(serve: Serve) -> Result<(), StartupError> {
    let config = Config::load(serve.config.as_deref(), &serve.overrides())?;

    // Logs go to standard error so that anything this binary is ever
    // asked to write to standard output stays parseable.
    tracing_subscriber::fmt()
        .with_max_level(config.log.level.to_level())
        .with_writer(std::io::stderr)
        .init();

    // The gates a readiness probe reads. They start shut and are
    // opened below as startup passes each one; liveness reads none of
    // them, deliberately, so that a probe cannot kill a broker in the
    // middle of a restore.
    let readiness = Arc::new(Readiness::starting());

    // Section 7.2 and 7.5: the store is opened and the broker rebuilt
    // from it before anything binds.
    let shared = match restore_broker(&config)? {
        Some(shared) => {
            readiness.storage_is_open();
            shared
        }
        None => {
            // Loud, because the alternative to a default is silence
            // and silence is what would make this dangerous. An
            // operator who meant to configure a store and mistyped the
            // key gets a broker that works perfectly until it is
            // restarted.
            tracing::warn!(
                "no storage.directory is configured, so nothing is written down: \
                 sessions, their subscriptions and their queued messages last as \
                 long as this process and no longer"
            );
            // A distinct call although it opens the same gate:
            // running with no store is a supported mode, not a node
            // that is forever unready.
            readiness.storage_is_not_configured();
            Arc::new(Shared::new(config.server().broker))
        }
    };

    let running = Server::bind_shared(config.server(), shared).await?;
    tracing::info!(
        address = %running.address(),
        mountpoint = %config.listener.mountpoint,
        max_connections = config.listener.max_connections,
        "sdmqtt is serving MQTT over TCP"
    );

    let admin = start_admin(&config, &running, Arc::clone(&readiness)).await?;
    // Both of them, and only once both are bound. A node that said
    // it was ready with one listener up would be sent traffic for
    // the other.
    readiness.listeners_are_bound();

    // A signal is the ordinary way this process ends, so it is an
    // ending rather than an error: the listener stops accepting, the
    // port is released, and the exit status says nothing went wrong.
    match stop_signal().await {
        Ok(signal) => tracing::info!(signal, "signalled, stopping"),
        // Nothing can be done about it, and waiting forever for a
        // signal that will never arrive is worse than stopping.
        Err(error) => tracing::error!(%error, "cannot listen for a stop signal; stopping"),
    }

    // **Both stop on the same signal**, and the admin listener stops
    // first. It is the one with a mutating endpoint, so nothing can
    // publish into a broker that is being torn down, and it is the
    // one holding connections that do not end on their own: section
    // 11.1's events stream. Its shutdown is bounded, so a stream
    // that ignored the signal delays this by its grace period rather
    // than forever.
    if let Some(admin) = admin {
        admin.shutdown().await;
        tracing::info!("the admin listener has stopped");
    }
    running.shutdown().await;
    tracing::info!("stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::StorageSection;
    use storage::record::{
        BodyRecord, DeliveryRecord, InflightRecord, InflightState, MessageId, ProtocolVersion,
        QueueEntry, SessionKey, SessionRecord, SubscriptionRecord,
    };
    use storage::{Qos, RetainHandling};

    /// A configuration whose store is `directory` and whose everything
    /// else is the default.
    fn with_store(directory: &std::path::Path) -> Config {
        Config {
            storage: StorageSection {
                directory: Some(directory.to_path_buf()),
                ..StorageSection::default()
            },
            ..Config::default()
        }
    }

    /// One durable session, expiring at `expires_at`, holding one
    /// filter.
    fn a_session(client_id: &str, expires_at: u32) -> Vec<Record> {
        let key = SessionKey::new("", client_id);
        vec![
            Record::SessionPut(SessionRecord {
                key: key.clone(),
                version: ProtocolVersion::V5,
                expiry_interval: 3_600,
                expires_at: Some(expires_at),
                will: None,
                will_at: None,
            }),
            Record::SubscriptionAdded {
                session: key,
                subscription: SubscriptionRecord {
                    filter: "a/b".to_owned(),
                    qos: Qos::AtLeastOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: RetainHandling::SendAlways,
                    subscription_id: None,
                },
            },
        ]
    }

    /// A body, distinct in every field that is stored.
    fn a_body(marker: u8) -> (MessageId, BodyRecord) {
        (
            MessageId([marker; 16]),
            BodyRecord {
                topic: format!("a/{marker}"),
                payload: bytes::Bytes::from(vec![marker; 8]),
                qos: Qos::AtLeastOnce,
                retain: false,
                expires_at: None,
                properties: bytes::Bytes::new(),
            },
        )
    }

    /// What a delivery of `body` looks like on the wire.
    fn a_delivery(body: MessageId) -> DeliveryRecord {
        DeliveryRecord {
            body,
            qos: Qos::AtLeastOnce,
            retain: false,
            subscription_ids: Vec::new(),
            dup: false,
        }
    }

    /// One queue entry and one inflight entry for `client_id`, each with
    /// a body of its own, as a real session holds them.
    ///
    /// One reference each, which is how the broker takes them: the
    /// `BodyPut` beside a `QueuePushed` is the reference the queued
    /// delivery holds, and it is *not* released when the delivery moves
    /// into the window, so the entry that is holding it when the broker
    /// stops is holding exactly one.
    ///
    /// `marker` names the queued body and `marker + 1` the one in the
    /// window, so that two sessions seeded here share nothing and a
    /// report can say which session's body was left behind.
    fn a_queue_and_a_window(client_id: &str, marker: u8) -> Vec<Record> {
        let key = SessionKey::new("", client_id);
        let (queued, queued_body) = a_body(marker);
        let (sent, sent_body) = a_body(marker + 1);
        vec![
            Record::BodyPut {
                id: queued,
                body: queued_body,
            },
            Record::QueuePushed {
                session: key.clone(),
                entry: QueueEntry {
                    sequence: 1,
                    packet_id: None,
                    delivery: a_delivery(queued),
                },
            },
            Record::BodyPut {
                id: sent,
                body: sent_body,
            },
            Record::QueuePushed {
                session: key.clone(),
                entry: QueueEntry {
                    sequence: 2,
                    packet_id: Some(7),
                    delivery: a_delivery(sent),
                },
            },
            // Popped into the window, which moves the reference rather
            // than releasing and retaking it.
            Record::QueuePopped {
                session: key.clone(),
                sequence: 2,
            },
            Record::InflightPut {
                session: key,
                entry: InflightRecord {
                    packet_id: 7,
                    sequence: 2,
                    state: InflightState::AwaitingAck,
                    sent_at: 0,
                    delivery: a_delivery(sent),
                },
            },
        ]
    }

    /// Write `records` into a store at `directory` and close it again,
    /// so that the next open is a restart rather than a second handle.
    fn seed(directory: &std::path::Path, records: Vec<Record>) {
        let section = StorageSection {
            directory: Some(directory.to_path_buf()),
            ..StorageSection::default()
        };
        let store = section.open().expect("the store opens").expect("it exists");
        store.apply(&records).expect("the store takes them");
        store.flush().expect("and writes them down");
    }

    #[test]
    fn no_configured_directory_restores_nothing_and_is_not_a_failure() {
        // The decision, at the place that acts on it. Running without
        // a store is a mode, not an error, and `sdmqtt serve` with no
        // arguments depends on it.
        let restored =
            restore_broker(&Config::default()).expect("an absent store is not a failure");
        assert!(restored.is_none());
    }

    #[test]
    fn a_session_that_outlived_the_outage_comes_back_with_its_subscription() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        // An hour from now, on the wall clock, because that is the
        // scale a stored expiry is on once it has survived a restart.
        seed(
            directory.path(),
            a_session("survivor", now_seconds().saturating_add(3_600)),
        );

        let shared = restore_broker(&with_store(directory.path()))
            .expect("the store opens")
            .expect("a directory was configured");
        let (sessions, subscriptions, clock) = shared.with_broker(|broker| {
            (
                broker.session_count(),
                broker.subscription_count(),
                broker.clock().now(),
            )
        });
        assert_eq!(sessions, 1, "the session came back");
        assert_eq!(
            subscriptions, 1,
            "and its filter re-entered the matching trie, which is the half \
             a session-only rebuild gets wrong"
        );

        // Section 7.5, and the reason the clock is read before restore
        // rather than after: a broker that started its clock at zero
        // would judge every stored expiry against a scale the store
        // was never written on. The exact instant is not assertable —
        // the difference a late read makes is the duration of the
        // restore — but the scale is, and getting the scale wrong is
        // the failure that matters.
        //
        // Against the epoch directly rather than against
        // `now_seconds`, which is the function under test here:
        // convention 5, a round trip cannot see a consistent error. A
        // clock stuck at zero agrees with itself perfectly, and every
        // other test in this file seeds its fixture through the same
        // function, so all of them would agree with it too.
        let epoch_now = u32::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("this machine's clock is after 1970")
                .as_secs(),
        )
        .expect("and before 2106");
        assert!(
            clock.abs_diff(epoch_now) < 60,
            "the broker's clock is wall time, not seconds since it started: {clock}"
        );
    }

    #[test]
    fn a_session_whose_expiry_passed_is_neither_restored_nor_left_on_disk() {
        // Three parts, and the last two are what nothing else guards.
        // `broker-core` cannot remove the session itself — it does no
        // I/O — so a shell that only skipped it would leave
        // the session and its queue on disk for the life of the file,
        // and skip them again on every restart after this one.
        //
        // Removing the session is not enough. `Storage::apply` says a
        // `SessionRemoved` does not release the bodies of the entries
        // it discards, so a shell that only removed the session would
        // leave every body it referenced behind at a reference count
        // that can never reach zero: a leak that accumulates on every
        // restart, and which the next restart cannot even see, because
        // the session that named the bodies is gone.
        //
        // So the session is seeded with a queue entry and an inflight
        // entry, each holding one reference to a body of its own. Both
        // shapes, because they are released by different halves of the
        // shell's loop and a fixture with only a queue would let the
        // inflight half be deleted in silence.
        let directory = tempfile::tempdir().expect("a temporary directory");
        let mut seeded = a_session("ghost", now_seconds().saturating_sub(3_600));
        seeded.extend(a_queue_and_a_window("ghost", 0x11));
        seed(directory.path(), seeded);

        let shared = restore_broker(&with_store(directory.path()))
            .expect("the store opens")
            .expect("a directory was configured");
        assert_eq!(
            shared.with_broker(|broker| broker.session_count()),
            0,
            "a session whose expiry passed while the broker was down is not restored"
        );

        // Dropping the shell drops the writer, which flushes and
        // joins before it goes, so the removal is on the device by
        // the time this returns and the store's lock is released.
        drop(shared);

        let store = with_store(directory.path())
            .storage
            .open()
            .expect("the store reopens")
            .expect("it exists");
        let held = store.restore().expect("it reads");
        assert!(
            held.sessions.is_empty(),
            "the expired session is gone from the store, not merely skipped"
        );
        // Named rather than counted: a leak is only actionable if the
        // report says which body is still there, and 0x11 is the
        // queued one while 0x12 is the one in the window.
        let leaked: Vec<u8> = held.bodies.keys().map(|id| id.0[0]).collect();
        assert!(
            held.bodies.is_empty(),
            "the bodies the expired session referenced are still in the file, \
             at a reference count nothing will ever bring to zero: {leaked:02x?}"
        );
    }

    #[test]
    fn reclaiming_an_expired_session_leaves_its_neighbour_alone() {
        // The other edge of the same loop. The releases are owed by
        // exactly the sessions the restore skipped, and a release
        // names a body rather than a session, so nothing in the store
        // would refuse one emitted for a session that is still alive:
        // it would take the count of a body a live queue entry
        // references down to zero and delete it, which is a message
        // lost and a restore that cannot complete. A fixture holding
        // only expired sessions cannot tell the two apart, because
        // every release it emits is one that was owed.
        let directory = tempfile::tempdir().expect("a temporary directory");
        let mut seeded = a_session("ghost", now_seconds().saturating_sub(3_600));
        seeded.extend(a_queue_and_a_window("ghost", 0x11));
        seeded.extend(a_session("survivor", now_seconds().saturating_add(3_600)));
        seeded.extend(a_queue_and_a_window("survivor", 0x33));
        seed(directory.path(), seeded);

        let shared = restore_broker(&with_store(directory.path()))
            .expect("the store opens")
            .expect("a directory was configured");
        assert_eq!(
            shared.with_broker(|broker| broker.session_count()),
            1,
            "one of the two outlived the outage"
        );
        drop(shared);

        let store = with_store(directory.path())
            .storage
            .open()
            .expect("the store reopens")
            .expect("it exists");
        let held = store.restore().expect("it reads");
        let keys: Vec<&str> = held
            .sessions
            .iter()
            .map(|session| session.session.key.client_id.as_str())
            .collect();
        assert_eq!(keys, vec!["survivor"], "only the ghost was removed");
        let mut bodies: Vec<u8> = held.bodies.keys().map(|id| id.0[0]).collect();
        bodies.sort_unstable();
        assert_eq!(
            bodies,
            vec![0x33, 0x34],
            "the survivor's queued body and the one in its window are both still \
             held, and the ghost's are both gone"
        );
    }

    /// A store that takes its time answering [`Storage::restore`].
    ///
    /// The whole point of the seam it is passed through. A store that
    /// answers instantly cannot tell a clock read before the store
    /// from one read after it, because the difference between them is
    /// exactly how long the read took — and a store large enough to
    /// take seconds is the case section 7.5's rule is about.
    struct SlowToRead {
        inner: storage::MemoryStorage,
        delay: std::time::Duration,
    }

    impl storage::Storage for SlowToRead {
        fn apply(&self, records: &[Record]) -> Result<(), storage::StorageError> {
            self.inner.apply(records)
        }

        fn flush(&self) -> Result<(), storage::StorageError> {
            self.inner.flush()
        }

        fn restore(&self) -> Result<storage::Restored, storage::StorageError> {
            std::thread::sleep(self.delay);
            self.inner.restore()
        }

        fn queue_head(
            &self,
            session: &SessionKey,
            cursor: u64,
        ) -> Result<Option<storage::QueueEntry>, storage::StorageError> {
            self.inner.queue_head(session, cursor)
        }

        fn queue_depth(&self, session: &SessionKey) -> Result<u64, storage::StorageError> {
            self.inner.queue_depth(session)
        }

        fn highest_sequence(&self, session: &SessionKey) -> Result<u64, storage::StorageError> {
            self.inner.highest_sequence(session)
        }

        fn outbox_head(
            &self,
            rule: &str,
            cursor: u64,
        ) -> Result<Option<storage::OutboxRecord>, storage::StorageError> {
            self.inner.outbox_head(rule, cursor)
        }

        fn outbox_depth(&self, rule: &str) -> Result<u64, storage::StorageError> {
            self.inner.outbox_depth(rule)
        }

        fn outbox_highest_sequence(&self, rule: &str) -> Result<u64, storage::StorageError> {
            self.inner.outbox_highest_sequence(rule)
        }
    }

    #[test]
    fn the_clock_is_read_before_the_store_and_not_after() {
        // Section 7.5, and the only shape that can see the
        // difference. The store takes three seconds to answer and
        // holds a session expiring one second from now:
        //
        // - read before, the clock is now and the session survives,
        //   which is right: it was alive when the broker started.
        // - read after, the clock is three seconds on and the session
        //   is destroyed, and removed from the store, for having
        //   expired during the read of the store that was going to
        //   restore it.
        //
        // Every other test here passes either way, because every
        // other store answers in microseconds.
        let delay = std::time::Duration::from_secs(3);
        let store = SlowToRead {
            inner: storage::MemoryStorage::new(),
            delay,
        };
        let expires_at = now_seconds().saturating_add(1);
        storage::Storage::apply(&store, &a_session("close-run", expires_at))
            .expect("the store takes them");

        let shared = restore_from(Arc::new(store), &Config::default())
            .expect("a store that is slow is not a store that failed");
        assert_eq!(
            shared.with_broker(|broker| broker.session_count()),
            1,
            "a session alive when the broker started must not be destroyed \
             by how long the broker took to read the store"
        );
    }
}
