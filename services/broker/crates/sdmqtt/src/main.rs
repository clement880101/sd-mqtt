//! The sd-mqtt broker.
//!
//! `sdmqtt serve` with no arguments starts a working broker on the
//! registered MQTT port. That is the product's first promise, and this
//! file is where it becomes true: everything below it is configured
//! entirely by its defaults, so the only thing a command line has to do
//! is not get in the way.

mod config;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use config::{Config, ConfigError, LogLevel, Overrides};
use sdmqtt_server::listener::Server;

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
    /// One of error, warn, info, debug or trace.
    #[arg(long, value_name = "LEVEL")]
    log_level: Option<LogLevel>,
}

impl Serve {
    /// What this asks the configuration to change.
    fn overrides(&self) -> Overrides {
        Overrides {
            address: self.address,
            max_connections: self.max_connections,
            mountpoint: self.mountpoint.clone(),
            log_level: self.log_level,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let serve = match cli.command {
        Some(Command::Serve(serve)) => serve,
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
}

/// Configure, bind, and serve until interrupted.
async fn run(serve: Serve) -> Result<(), StartupError> {
    let config = Config::load(serve.config.as_deref(), &serve.overrides())?;

    // Logs go to standard error so that anything this binary is ever
    // asked to write to standard output stays parseable.
    tracing_subscriber::fmt()
        .with_max_level(config.log.level.to_level())
        .with_writer(std::io::stderr)
        .init();

    let running = Server::bind(config.server()).await?;
    tracing::info!(
        address = %running.address(),
        mountpoint = %config.listener.mountpoint,
        max_connections = config.listener.max_connections,
        "sdmqtt is serving MQTT over TCP"
    );

    // An interrupt is the ordinary way this process ends, so it is an
    // ending rather than an error: the listener stops accepting, the
    // port is released, and the exit status says nothing went wrong.
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "cannot listen for an interrupt; stopping");
    } else {
        tracing::info!("interrupted, stopping");
    }

    running.shutdown().await;
    tracing::info!("stopped");
    Ok(())
}
