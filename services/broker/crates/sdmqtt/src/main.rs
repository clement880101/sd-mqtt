//! The sd-mqtt broker.
//!
//! `sdmqtt serve` with no arguments starts a working broker on the
//! registered MQTT port. That is the product's first promise, and this
//! file is where it becomes true: everything below it is configured
//! entirely by its defaults, so the only thing a command line has to do
//! is not get in the way.

mod config;
mod connectors;
mod ingest;

use std::collections::HashSet;
use std::path::PathBuf;
use std::process::ExitCode;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use broker_core::broker::Broker;
use clap::{Args, Parser, Subcommand};
use cluster::membership::MemberState;
use cluster::mesh::Mesh;
use cluster::{
    HybridClock, MetadataError, NodeIdentity, Replicated, TcpTransport, DEFAULT_MAX_DRIFT_MILLIS,
};
use config::{Config, ConfigError, IngestListener, LogLevel, Overrides};
use ingest::IngestSocket;
use rules::live::IngestListening;
use rules::Ingest;
use sdmqtt_admin::{
    listener_list, AdminRunning, AdminServer, AdminStartError, AdminState, ClusterPeer,
    ListenerProtocol, ListenerView, Listeners, Readiness, Router,
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
pub enum StartupError {
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
    /// A rule could not be installed.
    ///
    /// Fatal rather than a rule that is quietly missing. A broker that
    /// started with a connector silently not running is a broker
    /// dropping messages an operator believes are being delivered, and
    /// the outbox those messages would have gone into is exactly what
    /// this milestone exists to make trustworthy.
    #[error("cannot start a rule: {0}")]
    Connector(String),
    /// This node's identity could not be established.
    ///
    /// Fatal. Section 8.1 makes the identity the thing singleton
    /// ownership and every conflict tie-break order by, so a node that
    /// cannot write down who it is would be a different node after the
    /// next restart — and would be, without saying so.
    #[error("{0}")]
    Identity(#[from] cluster::IdentityError),
    /// A stored replicated register could not be read back.
    ///
    /// Fatal, and for the reason [`Self::Restore`] is. Section 8.2's
    /// registers are what routes to a peer, what serves a replicated
    /// rule and what claims a session; coming up holding half of one
    /// this build does not understand is a node that looks perfectly
    /// healthy and routes nothing. A store written by this build cannot
    /// contain one, so this is a store written by something else.
    #[error("cannot read a replicated register from the store: {0}")]
    Metadata(#[from] MetadataError),
    /// The cluster's own configuration will not do.
    ///
    /// Carried verbatim: `cluster::ConfigError` names which of the
    /// three things is wrong and what to do about it.
    #[error("{0}")]
    Cluster(#[from] cluster::ConfigError),
    /// The cluster listener could not be bound.
    ///
    /// Fatal, and for the reason the other listeners' failures are: a
    /// node whose peers cannot reach it is a node that has joined
    /// nothing, and discovering that from a membership of zero an hour
    /// later is worse than not starting.
    #[error("cannot bind the cluster listener on {address}: {source}")]
    ClusterListener {
        /// Where it was asked to bind.
        address: std::net::SocketAddr,
        /// Why it could not.
        #[source]
        source: std::io::Error,
    },
}

/// The mesh, as the two things that read it want it.
///
/// One adapter for both, because the two traits are two views of one
/// set of numbers and a second adapter would be a second place for
/// them to disagree.
#[derive(Debug)]
struct MeshView(Arc<Mesh>);

impl sdmqtt_admin::Cluster for MeshView {
    fn peers(&self) -> Vec<ClusterPeer> {
        self.0
            .members()
            .into_iter()
            .map(|member| ClusterPeer {
                id: member.identity.id().to_string(),
                name: member.identity.name().to_owned(),
                address: member.advertised,
                state: member.state.label(),
            })
            .collect()
    }
}

impl sdmqtt_server::metrics::ClusterMetrics for MeshView {
    fn members(&self) -> u64 {
        // **Itself included.** A node that has lost every peer is a
        // cluster of one, and reporting none would be wrong rather
        // than empty.
        self.0
            .members()
            .iter()
            .filter(|member| member.state == MemberState::Alive)
            .count() as u64
            + 1
    }

    fn bytes_sent(&self) -> u64 {
        self.0.stats().bytes_sent
    }

    fn bytes_received(&self) -> u64 {
        self.0.stats().bytes_received
    }

    fn bytes_dropped(&self) -> u64 {
        self.0.stats().bytes_dropped
    }
}

/// Who this node is, as the admin API has to report it.
///
/// One parameter rather than two, because they are one fact: a node
/// with no store has neither, a node with a store has an identity, and
/// only a node that clusters has a mesh.
struct Node<'a> {
    /// Section 8.1's identity, on a node that has somewhere to keep
    /// one.
    identity: Option<&'a NodeIdentity>,
    /// Its mesh, on a node that clusters.
    mesh: Option<&'a Arc<Mesh>>,
}

/// Start section 8.1's mesh, when this node clusters.
///
/// `Ok(None)` is a node with no `cluster.address`, which is the
/// default and a supported way to run this broker.
///
/// **After the store has been read**, because the identity a peer is
/// told is the one the store holds; `Config::validate` has already
/// refused clustering without a store, so the identity is always
/// there by the time this is reached.
async fn start_cluster(
    config: &Config,
    rebuilt: Option<&Rebuilt>,
) -> Result<Option<Arc<Mesh>>, StartupError> {
    let Some(address) = config.cluster.address else {
        if config.cluster.secret.is_some() {
            // Said out loud. An operator who put the secret in the
            // environment of every node and forgot the address on one
            // of them has a node that looks configured and clusters
            // with nobody.
            tracing::warn!(
                "cluster.secret is configured and cluster.address is not, so this node \
                 does not cluster at all"
            );
        }
        return Ok(None);
    };
    let Some(rebuilt) = rebuilt else {
        // `Config::validate` refuses this, so reaching it means
        // something went round the file. Refusing again is cheaper
        // than a node whose identity lasts until its next restart.
        return Err(StartupError::Cluster(cluster::ConfigError::NoSecret));
    };

    let settings = config.cluster.settings().check()?;
    let transport = Arc::new(
        TcpTransport::bind(&address.to_string())
            .await
            .map_err(|source| StartupError::ClusterListener { address, source })?,
    );
    let mesh = Mesh::start(
        rebuilt.identity.clone(),
        settings,
        config.cluster.mesh(),
        transport,
    );
    tracing::info!(
        node = %rebuilt.identity,
        address = %mesh.address(),
        advertise = mesh.advertise(),
        seeds = config.cluster.seeds.len(),
        cluster = config.cluster.name,
        "this node is clustering"
    );
    Ok(Some(mesh))
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
fn restore_broker(config: &Config) -> Result<Option<Rebuilt>, StartupError> {
    let Some(store) = config.storage.open()? else {
        return Ok(None);
    };
    restore_from(store, config).map(Some)
}

/// A broker rebuilt from a store, and the two things that read and
/// write it.
///
/// The store and the writer come back out because the connectors need
/// them: a rule is written into the store at startup and its pump reads
/// the outbox the hook fills. [`Shared`] holds a writer of its own and
/// does not hand it out, deliberately — the only way to reach it
/// through [`Shared`] is to hold the broker's lock, which is what keeps
/// the store's order the order the broker produced.
struct Rebuilt {
    /// What a listener is given.
    shared: Arc<Shared>,
    /// What a rule's pump reads.
    store: Arc<dyn Storage>,
    /// What a rule's hook and pump write through.
    writer: Arc<Writer>,
    /// Who this node is, section 8.1.
    ///
    /// Read out of the same restore as everything else and minted here
    /// if the store has never held one, which is what makes a store
    /// directory and a node the same thing.
    identity: NodeIdentity,
    /// This node's replicated metadata, section 8.2.
    ///
    /// Opened out of the same restore as everything else, which is
    /// what it has to be: the clock inside it resumes from the highest
    /// reading this node has ever written and the registers are the
    /// ones it held, and a second restore would be a second answer to
    /// both.
    replicated: Arc<Replicated>,
}

/// The same, against a store somebody else opened.
///
/// A seam, and it exists for one reason: the clock must be read
/// **before** the store is read, and the difference that makes is the
/// duration of the read. Against a store that answers in microseconds
/// the two orders are indistinguishable, so a test that could not make
/// the read slow could not tell them apart, and the rule section 7.5
/// states would be guarded by nothing.
fn restore_from(store: Arc<dyn Storage>, config: &Config) -> Result<Rebuilt, StartupError> {
    // **Before the restore, not after.** Section 7.5: this is what
    // decides which sessions outlived the outage, and a clock read
    // afterwards would count the read of the store itself against
    // every session's expiry — destroying, on a store large enough to
    // take seconds to read, exactly the sessions that were closest to
    // surviving.
    let now = now_seconds();
    let restored = store.restore().map_err(StartupError::Restore)?;

    // Section 8.1, and before anything else is decided: who this node
    // is. Out of the restore that has just been read rather than out of
    // a second one, and written back only when there is something to
    // say — a founding, or an operator's rename.
    let reconciled =
        NodeIdentity::adopt(store.as_ref(), restored.node.as_ref(), &config.node.name)?;
    if reconciled.founded {
        tracing::info!(
            node = %reconciled.identity,
            "this store had no identity, so one was minted; it is this node's \
             for the life of the store directory"
        );
    } else {
        tracing::info!(node = %reconciled.identity, "this node");
    }

    let mut broker = Broker::new(config.server().broker);
    let report = broker_core::restore::restore(&mut broker, &restored, now);

    let writer = Arc::new(
        Writer::spawn(Arc::clone(&store), config.storage.writer()).map_err(StartupError::Writer)?,
    );

    // Section 8.2, out of the same restore. **The clock resumes from
    // the stored reading rather than from zero**: a clock that
    // restarted behind itself would re-issue a stamp it had already
    // used, and two values under one key carrying one stamp is the
    // single divergence no later exchange repairs. The registers come
    // back with it, tombstones included, because a node that dropped a
    // tombstone would be handed its own older copy by the next peer it
    // spoke to and would resurrect what it had deleted.
    let replicated = Arc::new(
        Replicated::open(
            reconciled.identity.id(),
            HybridClock::resume(restored.clock, DEFAULT_MAX_DRIFT_MILLIS),
            &restored,
        )
        .map_err(StartupError::Metadata)?,
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

    Ok(Rebuilt {
        shared: Arc::new(Shared::with_replication(
            broker,
            Arc::clone(&writer),
            Arc::clone(&replicated),
        )),
        store,
        writer,
        identity: reconciled.identity,
        replicated,
    })
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
    listeners: Listeners,
    ingest: &Arc<Ingest>,
    registry: Option<&Arc<rules::live::Registry>>,
    node: Node<'_>,
) -> Result<Option<AdminRunning>, StartupError> {
    if !config.admin.enabled {
        tracing::warn!(
            "the admin listener is disabled, so this node serves no health endpoints, \
             no metrics and no API"
        );
        if config.ingest.listener == IngestListener::Admin {
            // Not a warning attached to the rules: the operator asked
            // for both things and they contradict each other, so the
            // one that silently does nothing is said out loud.
            //
            // **Whether or not this node holds an ingest rule yet.**
            // It used to be said only when one existed, which made it
            // a line nobody saw on exactly the node where a rule
            // created later would be served nowhere.
            tracing::error!(
                "ingest.listener = \"admin\" and the admin listener is disabled, so this \
                 node's ingest rules are served nowhere"
            );
        }
        return Ok(None);
    }
    let mut state = AdminState::new(Arc::clone(running.shared()))
        .with_readiness(readiness)
        .with_listeners(listeners)
        // Section 11.1 serves rules, and section 9.1 makes one
        // durable metadata, so a node with no store has none to
        // serve. See `sdmqtt_admin::rules`.
        .with_rules(registry.map(Arc::clone));
    // **Section 8.1's identity, not the word `local`.** A node with no
    // store has none — its identity is a thing that is written down —
    // so it keeps the old answer, which is honest: it is the only node
    // there will ever be in its cluster, and it cannot be in one.
    if let Some(identity) = node.identity {
        state = state
            .with_node_id(identity.id().to_string())
            .with_node_name(identity.name());
    }
    if let Some(mesh) = node.mesh {
        state = state
            .with_node_address(mesh.advertise())
            .with_cluster(Some(
                Arc::new(MeshView(Arc::clone(mesh))) as Arc<dyn sdmqtt_admin::Cluster>
            ));
    }

    // Section 9.2: serving the ingest routes here is permitted and
    // must be chosen explicitly. `Router::new()` is the ordinary case.
    //
    // **Merged whether or not this node holds an ingest rule.** A
    // route set with nothing registered in it answers every
    // identifier the way it answers one nobody configured, so serving
    // it costs a node with no ingest rules nothing at all — and the
    // alternative is the defect this listener's own socket used to
    // have, because an axum router cannot grow a route once it is
    // being served and a rule created later would have nowhere to
    // go until the next restart. The one route here has the rule
    // identifier as a path segment for exactly that reason; see
    // `rules::http_in::INGEST_PATH`.
    let extra = if config.ingest.listener == IngestListener::Admin {
        tracing::info!(
            rules = ingest.len(),
            "the ingest routes are served on the admin listener, as this configuration \
             asked for; they are behind the admin listener's host check as well as their \
             own tokens"
        );
        rules::http_in::routes::<AdminState>(Arc::clone(running.shared()), Arc::clone(ingest))
    } else {
        Router::new()
    };

    let admin = AdminServer::bind_with(&config.admin, state, extra).await?;
    tracing::info!(
        address = %admin.address(),
        "the admin API, metrics and health endpoints are serving"
    );
    Ok(Some(admin))
}

/// Take charge of the ingest listener, when the routes are served on
/// a socket of this node's own.
///
/// `Ok(None)` is a configuration that puts the routes on the admin
/// listener, which is bound whatever this node holds and so has
/// nothing for anybody to take charge of.
///
/// What comes back is **the socket rather than a listener**, because
/// whether anything is bound is a question about what this node holds
/// now and the answer changes while it runs: read [`ingest`] for why.
/// This binds it if there is already an ingest rule to serve, which is
/// what keeps an unavailable address a startup failure, and hands it
/// to the registry so that the first rule created afterwards binds it
/// too.
///
/// **After the MQTT listener has bound**, which is the order section
/// 9.2 wants and the reason the registry is given this here rather
/// than when it is built: a post is a publication, and a publication
/// that reaches subscribers before the listener they are attached
/// through is bound reaches none of them.
async fn start_ingest(
    config: &Config,
    shared: &Arc<Shared>,
    connectors: &connectors::Connectors,
    listeners: &Listeners,
) -> Result<Option<Arc<IngestSocket>>, StartupError> {
    if config.ingest.listener != IngestListener::Own {
        return Ok(None);
    }
    let socket = Arc::new(IngestSocket::new(
        config.ingest.address,
        Arc::clone(shared),
        Arc::clone(connectors.ingest()),
        Arc::clone(listeners),
    ));
    socket.bind_if_wanted().await?;
    if let Some(registry) = connectors.registry() {
        registry.serve_ingest_on(Arc::clone(&socket) as Arc<dyn IngestListening>);
    }
    Ok(Some(socket))
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
    let rebuilt = match restore_broker(&config)? {
        Some(rebuilt) => {
            readiness.storage_is_open();
            Some(rebuilt)
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
            None
        }
    };
    let shared = match &rebuilt {
        Some(rebuilt) => Arc::clone(&rebuilt.shared),
        None => Arc::new(Shared::new(config.server().broker)),
    };

    // **Before anything binds.** A rule half installed is a message
    // that fires nothing, and the first thing a client does after a
    // listener comes up could be to publish one. `Config::validate` has
    // already refused a configuration with rules and no store, so a
    // rule here always has somewhere durable to put an envelope.
    let connectors = match &rebuilt {
        Some(rebuilt) => {
            connectors::start(
                &config,
                &shared,
                &rebuilt.store,
                &rebuilt.writer,
                rebuilt.identity.id(),
                &rebuilt.replicated,
            )
            .await?
        }
        None => connectors::start_without_rules(),
    };
    if connectors.len() > 0 {
        tracing::info!(rules = connectors.len(), "rules are running");
    }

    let running = Server::bind_shared(config.server(), shared).await?;
    tracing::info!(
        address = %running.address(),
        mountpoint = %config.listener.mountpoint,
        max_connections = config.listener.max_connections,
        "sdmqtt is serving MQTT over TCP"
    );

    // Section 8.1's mesh, after the store has been read and after the
    // listener clients use is up. A peer that connects before the MQTT
    // listener is bound is no trouble — nothing is forwarded over a
    // link in this milestone — and binding this one last means a
    // cluster port already in use is not discovered after the broker
    // has started answering clients.
    let mesh = start_cluster(&config, rebuilt.as_ref()).await?;
    if let Some(mesh) = &mesh {
        let metrics: Arc<dyn sdmqtt_server::metrics::ClusterMetrics> =
            Arc::new(MeshView(Arc::clone(mesh)));
        // Section 11.3's four cluster numbers, registered **only
        // here**: a node that does not cluster leaves them absent
        // rather than reporting zero, which is a different statement.
        running.shared().metrics().watch_cluster(metrics);
    }

    // What this process bound, which nothing can discover: a socket
    // does not know what it is for and the broker does not know a
    // listener exists. The other two listeners add their own rows,
    // the admin one once it knows its port and the ingest one
    // whenever it is bound — which is not only at startup; see
    // `ingest`.
    let mut bound = vec![ListenerView {
        name: "mqtt".to_owned(),
        protocol: ListenerProtocol::Mqtt,
        address: running.address(),
        mountpoint: config.listener.mountpoint.clone(),
        max_connections: Some(config.listener.max_connections),
    }];
    if let (Some(mesh), Some(address)) = (&mesh, config.cluster.address) {
        bound.push(ListenerView {
            name: "cluster".to_owned(),
            protocol: ListenerProtocol::Cluster,
            // What was actually bound, port and all, which is not what
            // was asked for when the port was zero.
            address: mesh.address().parse().unwrap_or(address),
            // The advertised address, which is what a peer dials and
            // is not the bind address. It has nowhere better to go in
            // this row, and a row that showed only a wildcard bind
            // would be the one thing an operator cannot use.
            mountpoint: mesh.advertise().to_owned(),
            max_connections: None,
        });
    }
    let listeners = listener_list(bound);

    // **After the MQTT listener, not before.** An ingest post is a
    // publication, and a publication that reaches subscribers before
    // the listener they are attached through is bound reaches none of
    // them. The broker itself is already restored, which is what
    // matters for durability.
    let ingest = start_ingest(&config, running.shared(), &connectors, &listeners).await?;
    let admin = start_admin(
        &config,
        &running,
        Arc::clone(&readiness),
        Arc::clone(&listeners),
        connectors.ingest(),
        connectors.registry(),
        Node {
            identity: rebuilt.as_ref().map(|rebuilt| &rebuilt.identity),
            mesh: mesh.as_ref(),
        },
    )
    .await?;
    // All of them, and only once all are bound. A node that said
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
    // And the cluster links, before the MQTT listener: a peer that is
    // told this node has gone can move on, and leaving the links up
    // while sessions are being torn down would have peers heartbeating
    // at a broker that is closing.
    if let Some(mesh) = &mesh {
        mesh.shutdown();
        tracing::info!("the cluster links have closed");
    }
    // And the ingest listener beside it, for the same reason: it
    // publishes, so nothing may enter a broker that is being torn
    // down. Before the MQTT listener stops, so that a post already in
    // flight is still delivered to the subscribers that are attached.
    //
    // **Here rather than with the pumps below**, although the socket
    // is now the registry's to open: what decides this ordering is
    // what the listener does to the broker, and that has not changed
    // because the moment it is bound has. A socket this node bound
    // ten seconds ago over the API stops exactly where one bound at
    // startup did.
    if let Some(ingest) = ingest {
        if ingest.close().await {
            tracing::info!("the ingest listener has stopped");
        }
    }
    running.shutdown().await;
    // After the listener, so that nothing can enter an outbox while its
    // pump is being stopped, and before the process ends, so that a
    // pump in the middle of a store write is joined rather than left.
    connectors.shutdown().await;
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
            .expect("a directory was configured")
            .shared;
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
            .expect("a directory was configured")
            .shared;
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
            .expect("a directory was configured")
            .shared;
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

        fn body(
            &self,
            id: &storage::MessageId,
        ) -> Result<Option<storage::BodyRecord>, storage::StorageError> {
            self.inner.body(id)
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
            .expect("a store that is slow is not a store that failed")
            .shared;
        assert_eq!(
            shared.with_broker(|broker| broker.session_count()),
            1,
            "a session alive when the broker started must not be destroyed \
             by how long the broker took to read the store"
        );
    }
}
