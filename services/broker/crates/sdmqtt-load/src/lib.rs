//! Opens and holds many MQTT connections. Section 15's connection target.
//!
//! # What it measures
//!
//! How many MQTT 3.1.1 connections one broker accepts and keeps: each is
//! a CONNECT answered by a CONNACK of zero, held for the plan's duration
//! with pings at half the keep-alive, and **asked again at the end** — a
//! PINGREQ that must come back as a PINGRESP — so a connection the
//! broker dropped quietly in the meantime is not counted as held.
//!
//! # Why the sources
//!
//! One source address can open about 28,000 connections to one address
//! and port, the size of the ephemeral range, and then fails with
//! `EADDRNOTAVAIL`. A hundred thousand needs several sources; on Linux
//! every address in `127.0.0.0/8` is loopback without configuration, so
//! the connections are dealt round-robin across the plan's sources.
//!
//! # Failures are reported by cause
//!
//! A number with no reason attached is what this tool exists to avoid:
//! "92,000" means something different when the other 8,000 hit the
//! broker's descriptor limit than when they hit this process's. Every
//! connection that did not end up held is counted under what stopped it.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::Semaphore;

/// What to do.
#[derive(Debug, Clone)]
pub struct Plan {
    /// The broker's MQTT listener.
    pub target: SocketAddr,
    /// How many connections to open.
    pub connections: usize,
    /// Source addresses, dealt round-robin. One is enough below about
    /// 28,000 connections.
    pub sources: Vec<IpAddr>,
    /// How long to hold them once all have been attempted.
    pub hold: Duration,
    /// The keep-alive each CONNECT proposes, in seconds; pings go at half.
    pub keep_alive: u16,
    /// How many connects may be in flight at once, so the broker's
    /// accept backlog is not the thing measured.
    pub concurrency: usize,
    /// How long one connect, CONNACK included, may take.
    pub connect_timeout: Duration,
}

/// What happened.
#[derive(Debug, Default)]
pub struct Report {
    /// Answered with a CONNACK of zero.
    pub connected: usize,
    /// Of those, still answering a ping when the hold ended.
    pub still_connected: usize,
    /// Everything that did not connect, by cause.
    pub failed: BTreeMap<String, usize>,
    /// How long opening them all took.
    pub opening: Duration,
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let seconds = self.opening.as_secs_f64().max(0.001);
        writeln!(
            f,
            "connected {} in {:.1}s ({:.0}/s), still connected at the end {}",
            self.connected,
            seconds,
            self.connected as f64 / seconds,
            self.still_connected
        )?;
        for (cause, count) in &self.failed {
            writeln!(f, "  not connected, {cause}: {count}")?;
        }
        Ok(())
    }
}

/// An MQTT 3.1.1 CONNECT for `client_id`, clean session, no credentials.
fn connect_packet(client_id: &str, keep_alive: u16) -> Vec<u8> {
    let mut body = Vec::with_capacity(16 + client_id.len());
    body.extend_from_slice(&[0, 4, b'M', b'Q', b'T', b'T', 4, 0x02]);
    body.extend_from_slice(&keep_alive.to_be_bytes());
    let id = client_id.as_bytes();
    body.extend_from_slice(&u16::try_from(id.len()).unwrap_or(u16::MAX).to_be_bytes());
    body.extend_from_slice(id);
    let mut packet = vec![0x10];
    let mut length = body.len();
    loop {
        let mut byte = u8::try_from(length % 128).unwrap_or(0);
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if length == 0 {
            break;
        }
    }
    packet.extend_from_slice(&body);
    packet
}

const PINGREQ: [u8; 2] = [0xC0, 0];
const PINGRESP: [u8; 2] = [0xD0, 0];

/// The cause an I/O error is counted under.
fn cause_of(error: &io::Error) -> String {
    match error.raw_os_error() {
        // The two names differ by platform; what matters is which side
        // ran out of what.
        Some(24) => "EMFILE (this process is out of descriptors)".to_owned(),
        Some(49 | 99) => "EADDRNOTAVAIL (a source address is out of ports)".to_owned(),
        _ => match error.kind() {
            io::ErrorKind::ConnectionRefused => "connection refused".to_owned(),
            io::ErrorKind::ConnectionReset => "reset by the broker".to_owned(),
            io::ErrorKind::UnexpectedEof => "closed before CONNACK".to_owned(),
            io::ErrorKind::BrokenPipe => "reset by the broker".to_owned(),
            other => format!("{other:?}"),
        },
    }
}

/// Open one connection and read its CONNACK.
async fn open(plan: &Plan, index: usize) -> Result<TcpStream, String> {
    let source = plan.sources[index % plan.sources.len()];
    let attempt = async {
        let socket = match source {
            IpAddr::V4(_) => TcpSocket::new_v4(),
            IpAddr::V6(_) => TcpSocket::new_v6(),
        }
        .map_err(|e| cause_of(&e))?;
        socket
            .bind(SocketAddr::new(source, 0))
            .map_err(|e| cause_of(&e))?;
        let mut stream = socket
            .connect(plan.target)
            .await
            .map_err(|e| cause_of(&e))?;
        stream
            .write_all(&connect_packet(&format!("load-{index}"), plan.keep_alive))
            .await
            .map_err(|e| cause_of(&e))?;
        let mut ack = [0u8; 4];
        stream
            .read_exact(&mut ack)
            .await
            .map_err(|e| cause_of(&e))?;
        match ack {
            [0x20, 2, _, 0] => Ok(stream),
            [0x20, 2, _, code] => Err(format!("CONNACK return code {code}")),
            _ => Err("something other than a CONNACK".to_owned()),
        }
    };
    tokio::time::timeout(plan.connect_timeout, attempt)
        .await
        .unwrap_or_else(|_| Err("timed out".to_owned()))
}

/// Hold one connection until `until`, pinging at half the keep-alive,
/// then ask once more. True when it answered the last ping.
async fn hold(mut stream: TcpStream, keep_alive: u16, until: Instant) -> bool {
    let interval = Duration::from_secs(u64::from(keep_alive.max(2)) / 2);
    loop {
        let now = Instant::now();
        if now >= until {
            break;
        }
        tokio::time::sleep(interval.min(until - now)).await;
        if Instant::now() < until && !ping(&mut stream).await {
            return false;
        }
    }
    ping(&mut stream).await
}

async fn ping(stream: &mut TcpStream) -> bool {
    if stream.write_all(&PINGREQ).await.is_err() {
        return false;
    }
    let mut answer = [0u8; 2];
    matches!(
        tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut answer)).await,
        Ok(Ok(_))
    ) && answer == PINGRESP
}

/// Carry out `plan`.
pub async fn run(plan: &Plan) -> Report {
    let plan = Arc::new(plan.clone());
    let gate = Arc::new(Semaphore::new(plan.concurrency.max(1)));
    let started = Instant::now();
    let mut opening = Vec::with_capacity(plan.connections);
    for index in 0..plan.connections {
        let plan = Arc::clone(&plan);
        let gate = Arc::clone(&gate);
        opening.push(tokio::spawn(async move {
            let _permit = gate.acquire_owned().await;
            open(&plan, index).await
        }));
    }
    let mut report = Report::default();
    let mut held = Vec::new();
    for task in opening {
        match task.await {
            Ok(Ok(stream)) => {
                report.connected += 1;
                held.push(stream);
            }
            Ok(Err(cause)) => *report.failed.entry(cause).or_insert(0) += 1,
            Err(_) => {
                *report
                    .failed
                    .entry("the task panicked".to_owned())
                    .or_insert(0) += 1
            }
        }
    }
    report.opening = started.elapsed();
    let until = Instant::now() + plan.hold;
    let holding: Vec<_> = held
        .into_iter()
        .map(|stream| tokio::spawn(hold(stream, plan.keep_alive, until)))
        .collect();
    for task in holding {
        if matches!(task.await, Ok(true)) {
            report.still_connected += 1;
        }
    }
    report
}
