//! `sdmqtt-load`: open and hold many MQTT connections, then say how many
//! held and why the rest did not. See the library for what it measures.
//!
//! Exits zero only when every connection was opened and was still
//! answering at the end, so a script can use it as a gate.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use sdmqtt_load::{run, Plan};

/// Open and hold many MQTT connections against one broker.
#[derive(Debug, Parser)]
#[command(name = "sdmqtt-load", version, about)]
struct Args {
    /// The broker's MQTT listener.
    #[arg(long, default_value = "127.0.0.1:1883")]
    target: SocketAddr,
    /// How many connections to open.
    #[arg(long, default_value_t = 1_000)]
    connections: usize,
    /// The first source address; the rest follow it, one per 25,000
    /// connections, which on Linux are all loopback within 127.0.0.0/8.
    #[arg(long, default_value = "127.0.0.2")]
    first_source: Ipv4Addr,
    /// Connections per source address, under the ephemeral range.
    #[arg(long, default_value_t = 25_000)]
    per_source: usize,
    /// Seconds to hold them once all have been attempted.
    #[arg(long, default_value_t = 60)]
    hold: u64,
    /// The keep-alive each CONNECT proposes, in seconds.
    #[arg(long, default_value_t = 60)]
    keep_alive: u16,
    /// Connects in flight at once.
    #[arg(long, default_value_t = 512)]
    concurrency: usize,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let count = args.connections.div_ceil(args.per_source.max(1)).max(1);
    let first = u32::from(args.first_source);
    let sources = (0..count)
        .map(|offset| IpAddr::V4(Ipv4Addr::from(first + u32::try_from(offset).unwrap_or(0))))
        .collect();
    let plan = Plan {
        target: args.target,
        connections: args.connections,
        sources,
        hold: Duration::from_secs(args.hold),
        keep_alive: args.keep_alive,
        concurrency: args.concurrency,
        connect_timeout: Duration::from_secs(30),
    };
    let report = run(&plan).await;
    print!("{report}");
    if report.still_connected == args.connections {
        ExitCode::SUCCESS
    } else {
        println!(
            "target missed: {} of {} held",
            report.still_connected, args.connections
        );
        ExitCode::FAILURE
    }
}
