//! The interface reaches a browser from the binary.
//!
//! Everything between `npm run build` and a document on a socket, in
//! one test against the real binary: the bundler wrote it, the build
//! script compressed and embedded it, the listener negotiated it, and
//! what comes back is byte for byte what was built.
//!
//! # Why it skips rather than builds
//!
//! Section 11.2: a build of the binary must not require Node. A test
//! that ran `npm` would make that false for anyone who ran the suite,
//! which is everyone. So it skips when there is no built interface,
//! **loudly**, naming what to run.
//!
//! A test that quietly passes when it could not check anything is the
//! thing convention 5 exists about, so the skip is as visible as this
//! file can make it, and there is a test beside it that always runs
//! and asserts the two states are told apart correctly.

use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// How long to wait for a freshly started broker to accept.
const STARTUP: Duration = Duration::from_secs(20);

/// How long a request may take.
const PATIENCE: Duration = Duration::from_secs(10);

/// What to run when there is no interface to test.
const HOW_TO_BUILD: &str = "cd services/web && npm ci && npm run build, \
                            then rebuild: cargo build -p sdmqtt";

/// A running `sdmqtt`, with the address of its admin listener.
struct Broker {
    child: Child,
    admin: SocketAddr,
    log: PathBuf,
}

impl Broker {
    /// Start one and wait until the admin listener accepts.
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
                    fs::File::create(&log).expect("a log file can be created"),
                ))
                .spawn()
                .expect("the broker binary runs");
            let mut broker = Self {
                child,
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

    /// Wait until the admin listener accepts, or say why it did not.
    fn wait_until_listening(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + STARTUP;
        while Instant::now() < deadline {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Err(format!("it exited with {status}: {}", self.logged()));
            }
            if TcpStream::connect_timeout(&self.admin, Duration::from_millis(200)).is_ok() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(format!(
            "it did not accept on {} within {STARTUP:?}: {}",
            self.admin,
            self.logged()
        ))
    }

    /// What it wrote to standard error.
    fn logged(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
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

/// One response, with the body kept as bytes.
///
/// Bytes, because half of what this test is about is not text.
struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Response {
    /// One header's value, by lowercased name.
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(had, _)| had == name)
            .map(|(_, value)| value.as_str())
    }
}

/// `GET path`, with the headers given.
fn get(address: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Response {
    let mut socket = TcpStream::connect_timeout(&address, PATIENCE).expect("the listener accepts");
    socket
        .set_read_timeout(Some(PATIENCE))
        .expect("a read deadline");
    let mut head = format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    socket
        .write_all(head.as_bytes())
        .expect("the request is written");
    let mut raw = Vec::new();
    socket.read_to_end(&mut raw).expect("the response ends");

    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a response has a blank line after its headers");
    let text = String::from_utf8_lossy(&raw[..split]).into_owned();
    let mut lines = text.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("a numeric status code");
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    Response {
        status,
        headers,
        body: raw[split + 4..].to_vec(),
    }
}

/// Where `npm run build` writes, from this crate.
fn dist() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../web/dist")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("services/web/dist"))
}

/// The script the entry document loads, as an absolute path.
fn script_in(document: &str) -> String {
    let at = document
        .find("<script")
        .unwrap_or_else(|| panic!("the entry document loads a script: {document}"));
    let tail = &document[at..];
    let start = tail.find("src=\"").expect("the script has a source") + 5;
    let end = tail[start..].find('"').expect("the source is quoted") + start;
    tail[start..end].to_owned()
}

/// The whole of it, against the real binary.
#[test]
fn the_interface_reaches_a_browser_from_the_binary() {
    if !sdmqtt_admin::INTERFACE_BUILT {
        // Loud, and on both streams, because a skip that scrolls past
        // is a test that silently passed. Nothing below could check
        // anything, and saying so is the whole of what this can do
        // without making a Rust build depend on npm.
        let notice = format!(
            "\n\
             ==========================================================\n\
             SKIPPED: there is no built interface to test.\n\
             This binary embeds the page that says so, which is a\n\
             supported build: section 11.2 says a build of the binary\n\
             must not require Node. Nothing below was checked.\n\
             To check it:  {HOW_TO_BUILD}\n\
             ==========================================================\n"
        );
        println!("{notice}");
        eprintln!("{notice}");
        return;
    }

    let directory = tempfile::tempdir().expect("a temporary directory");
    let broker = Broker::start(
        directory.path().join("broker.log"),
        directory.path().join("store"),
    );

    // ---------------------------------------------------------
    // The root is the built application
    // ---------------------------------------------------------

    let root = get(broker.admin, "/", &[]);
    assert_eq!(root.status, 200, "{}", broker.logged());
    assert_eq!(
        root.header("content-type"),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(
        root.header("vary"),
        Some("accept-encoding"),
        "or a shared cache will hand these bytes to a client that asked for gzip"
    );
    let document = String::from_utf8(root.body.clone()).expect("the document is text");
    assert!(
        !document.contains("interface was not built"),
        "this binary was built with an interface, so the root must not be the \
         page that says it was not: {document}"
    );

    // Byte for byte what the bundler wrote, which is the whole of
    // what the build script and the embedding owe.
    let built = fs::read(dist().join("index.html")).expect("the built entry document is readable");
    assert_eq!(
        root.body, built,
        "what is served is not what was built; the embedding is stale or wrong"
    );

    // ---------------------------------------------------------
    // And the script it names is served too
    // ---------------------------------------------------------

    // A page whose script is a 404 is not an interface reaching a
    // browser, and the root alone cannot tell the difference.
    let script = script_in(&document);
    let served = get(broker.admin, &script, &[("Accept-Encoding", "gzip")]);
    assert_eq!(served.status, 200, "the entry document loads {script}");
    assert_eq!(
        served.header("content-type"),
        Some("text/javascript; charset=utf-8"),
        "a script served as anything else is refused by a browser"
    );
    assert_eq!(served.header("content-encoding"), Some("gzip"));

    // ---------------------------------------------------------
    // Brotli is brotli, and it is the same document
    // ---------------------------------------------------------

    let compressed = get(broker.admin, "/", &[("Accept-Encoding", "br")]);
    assert_eq!(compressed.status, 200);
    assert_eq!(compressed.header("content-encoding"), Some("br"));
    assert_ne!(
        compressed.body, root.body,
        "labelled brotli and sent uncompressed is the worst of both"
    );
    assert_ne!(
        compressed.header("etag"),
        root.header("etag"),
        "two encodings are two representations and need two tags"
    );

    let mut decompressed = Vec::new();
    brotli::Decompressor::new(compressed.body.as_slice(), 4096)
        .read_to_end(&mut decompressed)
        .expect("the bytes are brotli, and a decoder says so");
    assert_eq!(
        decompressed, root.body,
        "the compressed form decodes to a different document from the one \
         served uncompressed"
    );
}

/// The two states are told apart by something, and it is not a guess.
///
/// This runs in both trees. Without it the test above has no guard at
/// all in the tree where it skips, and a flag stuck at `false` would
/// turn the whole file into a no-op nobody would notice.
#[test]
fn whether_an_interface_was_built_agrees_with_whether_one_is_there() {
    let on_disk = dist().join("index.html").is_file();
    assert_eq!(
        sdmqtt_admin::INTERFACE_BUILT,
        on_disk,
        "the binary says the interface was {} and the tree says it was {}. \
         If a build has happened since this binary was compiled, rebuild it: \
         {HOW_TO_BUILD}",
        if sdmqtt_admin::INTERFACE_BUILT {
            "built"
        } else {
            "not built"
        },
        if on_disk { "built" } else { "not built" },
    );
}
