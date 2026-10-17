//! The admin listener comes up with the broker and goes down with it.
//!
//! Against the real binary over real sockets, because everything this
//! asserts is a property of the process: that a second listener was
//! bound at all, that it answers before and after the MQTT listener
//! does, and that a signal stops both.
//!
//! # The clause that is the whole test
//!
//! **A client is holding an events stream open when the signal
//! arrives.** Without that, this passes against the bug section 11.1
//! warns about: graceful shutdown waits for every open connection and
//! the body of an events stream never ends on its own, so one browser
//! tab left open on the Overview page would stop the broker exiting.
//! Since a clean stop is how sessions reach disk, that is a data bug
//! wearing a usability costume, and a test that merely stops an idle
//! broker cannot see it.
//!
//! # Why it is bounded
//!
//! A regression here is a process that never exits, and a test that
//! hangs stalls a build instead of reporting. Everything that waits
//! for the broker waits with a deadline and fails with what it was
//! waiting for.

#![cfg(unix)]

use std::fs::File;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// How long to wait for a freshly started broker to accept.
///
/// Generous, because this runs on shared hardware and a flaky test
/// of a shutdown rule is worse than no test.
const STARTUP: Duration = Duration::from_secs(20);

/// How long a request may take.
const PATIENCE: Duration = Duration::from_secs(10);

/// How long the broker has to exit after the signal.
///
/// Comfortably longer than anything a correct shutdown does, and
/// finite so that the bug this test exists for is reported rather
/// than left hanging a build. The admin listener's own backstop is
/// ten seconds, so this must be longer than that too: reaching the
/// backstop is a different failure from never exiting, and this test
/// should be able to tell them apart by the exit code rather than by
/// having timed out first.
const TO_STOP: Duration = Duration::from_secs(30);

/// A running `sdmqtt`, with both of its listeners.
struct Broker {
    child: Child,
    mqtt: SocketAddr,
    admin: SocketAddr,
    log: PathBuf,
}

impl Broker {
    /// Start one and wait until both listeners accept.
    fn start(log: PathBuf, store: PathBuf) -> Self {
        let mut last = String::new();
        for _ in 0..5 {
            let mqtt: SocketAddr = format!("127.0.0.1:{}", free_port())
                .parse()
                .expect("an address");
            let admin: SocketAddr = format!("127.0.0.1:{}", free_port())
                .parse()
                .expect("an address");
            let child = Command::new(env!("CARGO_BIN_EXE_sdmqtt"))
                .arg("serve")
                .arg("--address")
                .arg(mqtt.to_string())
                .arg("--admin-address")
                .arg(admin.to_string())
                .arg("--log-level")
                .arg("info")
                .arg("--store-directory")
                .arg(&store)
                // A configuration file in the environment of whoever
                // is running the tests must not reach this broker.
                .env("SDMQTT_CONFIG", "")
                .env("SDMQTT_STORAGE_DIRECTORY", "")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::from(
                    File::create(&log).expect("a log file can be created"),
                ))
                .spawn()
                .expect("the broker binary runs");
            let mut broker = Self {
                child,
                mqtt,
                admin,
                log: log.clone(),
            };
            match broker.wait_until_listening() {
                Ok(()) => return broker,
                Err(why) => {
                    let _ = broker.child.kill();
                    let _ = broker.child.wait();
                    last = why;
                }
            }
        }
        panic!("the broker never started: {last}");
    }

    /// Both listeners, not one. A broker whose admin listener never
    /// bound would otherwise start this test perfectly happily.
    fn wait_until_listening(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + STARTUP;
        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!("it exited with {status}: {}", self.logged()));
            }
            let reachable = |address: &SocketAddr| {
                TcpStream::connect_timeout(address, Duration::from_millis(200)).is_ok()
            };
            if reachable(&self.mqtt) && reachable(&self.admin) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(format!(
            "it did not accept on both {} and {} within {STARTUP:?}: {}",
            self.mqtt,
            self.admin,
            self.logged()
        ))
    }

    /// What it wrote to standard error.
    fn logged(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Ask it to stop the way a deployment does.
    ///
    /// `SIGTERM` rather than `Child::kill`, which is `SIGKILL` and
    /// runs no destructor: the whole question here is whether the
    /// *graceful* path terminates. Sent with `kill` rather than a
    /// signalling crate, because a dependency for one line of a test
    /// is a dependency to audit.
    fn terminate(&self) {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .expect("kill runs");
        assert!(status.success(), "the signal was delivered");
    }

    /// Wait for it to exit, and say how.
    fn wait_for_exit(&mut self) -> Result<std::process::ExitStatus, String> {
        let deadline = Instant::now() + TO_STOP;
        while Instant::now() < deadline {
            match self.child.try_wait() {
                Ok(Some(status)) => return Ok(status),
                Ok(None) => std::thread::sleep(Duration::from_millis(20)),
                Err(error) => return Err(format!("it could not be waited for: {error}")),
            }
        }
        Err(format!(
            "it was still running {TO_STOP:?} after the signal: {}",
            self.logged()
        ))
    }
}

impl Drop for Broker {
    /// Never leave one behind, however the test ended.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A port the operating system has just said is free.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("a port on the loopback")
        .local_addr()
        .expect("it has an address")
        .port()
}

/// `GET path`, read to the end.
fn get(address: SocketAddr, path: &str) -> String {
    let mut socket = TcpStream::connect_timeout(&address, PATIENCE).expect("the listener accepts");
    socket
        .set_read_timeout(Some(PATIENCE))
        .expect("a read deadline");
    socket
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .expect("the request is written");
    let mut raw = Vec::new();
    socket.read_to_end(&mut raw).expect("the response ends");
    String::from_utf8_lossy(&raw).into_owned()
}

/// Read whatever is left of `socket`, up to end of file.
fn read_to_end(socket: &mut TcpStream, patience: Duration) -> Vec<u8> {
    socket
        .set_read_timeout(Some(patience))
        .expect("a read deadline");
    let mut raw = Vec::new();
    let deadline = Instant::now() + patience;
    loop {
        assert!(
            Instant::now() < deadline,
            "the body never ended: {}",
            String::from_utf8_lossy(&raw)
        );
        let mut chunk = [0u8; 4096];
        match socket.read(&mut chunk) {
            Ok(0) => return raw,
            Ok(read) => raw.extend_from_slice(&chunk[..read]),
            Err(error) => panic!("the socket failed rather than ending: {error}"),
        }
    }
}

/// Open the events stream and read its head, leaving the body open.
///
/// The returned socket is what makes this test the test. While it
/// lives, the broker has a connection it is expected to wait for.
fn hold_the_events_stream(address: SocketAddr) -> TcpStream {
    let mut socket = TcpStream::connect_timeout(&address, PATIENCE).expect("the listener accepts");
    socket
        .set_read_timeout(Some(PATIENCE))
        .expect("a read deadline");
    socket
        .write_all(
            format!(
                "GET /api/v1/events HTTP/1.1\r\nHost: {address}\r\n\
                 Accept: text/event-stream\r\n\r\n"
            )
            .as_bytes(),
        )
        .expect("the request is written");

    // Read until the head is complete, so that the stream is
    // established rather than merely requested. A socket that had
    // only been connected would be closed by hyper's own idle
    // handling and would prove nothing.
    let mut raw = Vec::new();
    let deadline = Instant::now() + PATIENCE;
    while !raw.windows(4).any(|window| window == b"\r\n\r\n") {
        assert!(
            Instant::now() < deadline,
            "the events stream never answered: {}",
            String::from_utf8_lossy(&raw)
        );
        let mut chunk = [0u8; 1024];
        let read = socket.read(&mut chunk).expect("the socket is readable");
        assert!(read > 0, "the events stream closed instead of answering");
        raw.extend_from_slice(&chunk[..read]);
    }
    let head = String::from_utf8_lossy(&raw).into_owned();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    assert!(
        head.contains("text/event-stream"),
        "it is a stream, not a document: {head}"
    );
    socket
}

#[test]
fn the_admin_listener_serves_beside_the_broker_and_stops_with_it() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut broker = Broker::start(
        directory.path().join("broker.log"),
        directory.path().join("store"),
    );

    // It answers, over a real socket, on the second listener.
    let live = get(broker.admin, "/health/live");
    assert!(live.starts_with("HTTP/1.1 200"), "{live}");
    assert!(live.contains("\"status\":\"alive\""), "{live}");

    let ready = get(broker.admin, "/health/ready");
    assert!(
        ready.starts_with("HTTP/1.1 200"),
        "a broker that is serving is ready: {ready}"
    );
    assert!(ready.contains("\"listeners\":true"), "{ready}");
    assert!(ready.contains("\"storage\":true"), "{ready}");

    let metrics = get(broker.admin, "/metrics");
    assert!(metrics.starts_with("HTTP/1.1 200"), "{metrics}");
    assert!(
        metrics.contains("application/openmetrics-text"),
        "the exposition format's own content type: {metrics}"
    );
    assert!(
        metrics.contains("sdmqtt_sockets_opened_total"),
        "the registry the broker has been filling: {metrics}"
    );

    // And it reports the two listeners this process actually bound,
    // which is the thing a resource cannot discover for itself.
    let listeners = get(broker.admin, "/api/v1/listeners");
    assert!(
        listeners.contains(&format!("\"address\":\"{}\"", broker.mqtt)),
        "{listeners}"
    );
    assert!(
        listeners.contains(&format!("\"address\":\"{}\"", broker.admin)),
        "{listeners}"
    );

    // **The clause that is the test.** A browser is watching.
    let mut watching = hold_the_events_stream(broker.admin);

    let signalled = Instant::now();
    broker.terminate();

    // The body **ends**, and ends properly: a chunked body that has
    // finished is terminated by a zero-length chunk, and a socket
    // that was merely dropped when the process died is not. That is
    // the difference between a stream that saw the signal and a
    // stream that was cut off by the backstop, and it is a fact
    // about the bytes rather than about how long anything took.
    let tail = read_to_end(&mut watching, TO_STOP);
    assert!(
        tail.ends_with(b"0\r\n\r\n"),
        "the events body must end on the signal, not be cut off: {:?}",
        String::from_utf8_lossy(&tail[tail.len().saturating_sub(64)..])
    );

    let status = broker
        .wait_for_exit()
        .expect("a signalled broker exits, even with an events stream open");
    assert!(
        status.success(),
        "an orderly stop is not a failure: {status}, {}",
        broker.logged()
    );

    // And it stopped because the stream saw the signal, not because
    // the backstop gave up on it. Section 11.1 has the timeout as a
    // backstop for a defect; reaching it on every shutdown would be
    // that defect, and this is the only thing that can tell a broker
    // which waited from one which did not.
    let took = signalled.elapsed();
    assert!(
        took < sdmqtt_admin::SHUTDOWN_GRACE,
        "it took {took:?}, which is the {:?} backstop rather than the signal: {}",
        sdmqtt_admin::SHUTDOWN_GRACE,
        broker.logged()
    );
}

/// How long the second signal may take to end the process.
///
/// Well inside the admin listener's ten-second grace, which is what
/// the first stop alone would run to here, and long enough that a
/// slow machine cannot fail this for being slow.
const TO_STOP_NOW: Duration = Duration::from_secs(5);

/// Hold an admin request open by never finishing its body.
///
/// The route reads the body before it answers, so hyper counts the
/// request as in flight and the graceful shutdown waits for it — for
/// the whole of the listener's grace, since the body never comes.
/// This is the lever that makes the first stop slow enough to send a
/// second signal into.
fn stall_a_request(address: SocketAddr) -> TcpStream {
    let mut socket =
        TcpStream::connect_timeout(&address, PATIENCE).expect("the admin listener accepts");
    socket
        .write_all(
            format!(
                "PUT /api/v1/rules/stalled HTTP/1.1\r\nHost: {address}\r\n\
                 Content-Type: application/json\r\nContent-Length: 4096\r\n\r\n{{\"id\":"
            )
            .as_bytes(),
        )
        .expect("the request head is written");
    socket
}

/// Wait for a line to appear in the broker's log.
fn wait_for_log(broker: &Broker, fragment: &str) {
    let deadline = Instant::now() + PATIENCE;
    while Instant::now() < deadline {
        if broker.logged().contains(fragment) {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!(
        "{fragment:?} never appeared in the log: {}",
        broker.logged()
    );
}

/// A second signal during the stop ends the process now.
///
/// The first signal starts an orderly stop, and an orderly stop is
/// bounded but not instant: the admin listener alone may wait its
/// ten-second grace for a connection that does not finish. An operator
/// who presses Ctrl-C again, or a supervisor sending its second
/// `SIGTERM` before it escalates to `SIGKILL`, is saying the wait is
/// over. Before this test the second signal reached nothing: tokio's
/// handler stays installed once it is, so the signal neither stopped
/// the process by default nor interrupted the stop, and the operator
/// waited out the grace anyway.
///
/// The exit status is the one a shell reports for a process the signal
/// killed, because that is what was asked for; it is checked so that
/// a stop that merely finished on its own cannot pass this.
#[test]
fn a_second_signal_during_the_stop_ends_the_process_now() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let mut broker = Broker::start(
        directory.path().join("broker.log"),
        directory.path().join("store"),
    );

    // The connection the first stop will wait for. A moment for the
    // head to be parsed, so the request is in flight when the signal
    // arrives rather than still in a socket buffer.
    let stalled = stall_a_request(broker.admin);
    std::thread::sleep(Duration::from_millis(300));

    broker.terminate();
    wait_for_log(&broker, "signalled, stopping");

    let again = Instant::now();
    broker.terminate();
    let status = broker
        .wait_for_exit()
        .expect("a broker signalled twice exits");
    let took = again.elapsed();
    assert!(
        took < TO_STOP_NOW,
        "the second signal took {took:?} to end the process; the first stop alone would \
         have waited out the admin listener's grace: {}",
        broker.logged()
    );
    assert_eq!(
        status.code(),
        Some(128 + 15),
        "the exit status says the second SIGTERM ended it: {status}, {}",
        broker.logged()
    );
    drop(stalled);
}
