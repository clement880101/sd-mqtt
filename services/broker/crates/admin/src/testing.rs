//! An HTTP client for this crate's tests, small enough to read.
//!
//! The tests speak HTTP over a real socket rather than calling the
//! router as a `tower` service, for the same reason `server`'s tests
//! use a socket rather than a duplex stream: half of what this crate
//! owes is what a socket sees. A status line, a content type and a
//! body that ends are not properties of a handler's return value.
//!
//! It is thirty lines rather than a client crate because the project
//! already builds its own MQTT test client on its own codec, and a
//! second HTTP stack in the dev-dependencies would be a larger thing
//! to audit than the thing it tests.
//!
//! **`Connection: close` on every request**, which is what makes it
//! small: the response ends at end of file, so nothing here has to
//! keep a connection pool or decide when a body is complete. It also
//! means these tests never exercise keep-alive, which is hyper's
//! business and not this crate's.

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// What came back.
#[derive(Debug)]
pub(crate) struct Response {
    /// The status code from the status line.
    pub status: u16,
    /// The headers, lowercased names, in the order they arrived.
    pub headers: Vec<(String, String)>,
    /// The body, decoded if it arrived chunked.
    pub body: String,
}

impl Response {
    /// One header's value, by lowercased name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(had, _)| had == name)
            .map(|(_, value)| value.as_str())
    }
}

/// `GET path`, with no authorization.
pub(crate) async fn get(address: SocketAddr, path: &str) -> Response {
    request(address, "GET", path, None).await
}

/// What came back, before anything decided it was text.
///
/// The interface is served precompressed, so half of what this crate
/// answers with is not UTF-8 and reading it as a string would panic
/// on the bytes the test is about.
#[derive(Debug)]
pub(crate) struct Raw {
    /// The status code from the status line.
    pub status: u16,
    /// The headers, lowercased names, in the order they arrived.
    pub headers: Vec<(String, String)>,
    /// The body exactly as it arrived.
    pub body: Vec<u8>,
}

impl Raw {
    /// One header's value, by lowercased name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(had, _)| had == name)
            .map(|(_, value)| value.as_str())
    }
}

/// `GET path` carrying `headers`, keeping the body as bytes.
pub(crate) async fn get_raw(address: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Raw {
    let mut stream = TcpStream::connect(address)
        .await
        .expect("the admin listener accepts");
    let mut head = format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    stream
        .write_all(head.as_bytes())
        .await
        .expect("the request is written");
    stream.flush().await.expect("and flushed");

    let mut raw = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        stream.read_to_end(&mut raw),
    )
    .await
    .expect("the response ends")
    .expect("the socket is readable");
    split(&raw)
}

/// One request with no body, and the whole of the response.
pub(crate) async fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
) -> Response {
    with_body(address, method, path, token, None).await
}

/// `POST path` carrying `body` as JSON.
pub(crate) async fn post_json(
    address: SocketAddr,
    path: &str,
    token: Option<&str>,
    body: &str,
) -> Response {
    with_body(address, "POST", path, token, Some(body)).await
}

/// One request, and the whole of the response.
pub(crate) async fn with_body(
    address: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    body: Option<&str>,
) -> Response {
    let mut stream = TcpStream::connect(address)
        .await
        .expect("the admin listener accepts");
    let authorization = match token {
        Some(token) => format!("Authorization: Bearer {token}\r\n"),
        None => String::new(),
    };
    // A content length rather than a chunked body, because the point
    // of this client is to be small enough to read and hyper accepts
    // both.
    let content = match body {
        Some(body) => format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ),
        None => String::new(),
    };
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\
         {authorization}{content}\r\n{}",
        body.unwrap_or_default()
    );
    stream
        .write_all(head.as_bytes())
        .await
        .expect("the request is written");
    stream.flush().await.expect("and flushed");

    let mut raw = Vec::new();
    // Bounded, so a handler that never ends its body fails the test
    // rather than hanging the suite. Section 11.1 has exactly one
    // endpoint whose body is meant to stay open, and it is not one
    // this client is for.
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        stream.read_to_end(&mut raw),
    )
    .await
    .expect("the response ends")
    .expect("the socket is readable");

    parse(&raw)
}

/// Split a response into its status, its headers and its body,
/// insisting the body is text.
fn parse(raw: &[u8]) -> Response {
    let split = split(raw);
    Response {
        status: split.status,
        headers: split.headers,
        body: String::from_utf8(split.body).expect("the body is text"),
    }
}

/// Split a response into its status, its headers and its body.
fn split(raw: &[u8]) -> Raw {
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a response has a blank line after its headers");
    let head = std::str::from_utf8(&raw[..split]).expect("the head is text");
    let body = &raw[split + 4..];

    let mut lines = head.split("\r\n");
    let status_line = lines.next().expect("a status line");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .expect("a status code")
        .parse()
        .expect("a numeric status code");

    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();

    let chunked = headers
        .iter()
        .any(|(name, value)| name == "transfer-encoding" && value.contains("chunked"));
    let body = if chunked {
        dechunk(body)
    } else {
        body.to_vec()
    };

    Raw {
        status,
        headers,
        body,
    }
}

/// Undo chunked transfer encoding.
///
/// hyper uses a content length for everything this client asks for, so
/// this is a backstop rather than a path the tests walk. It is here
/// because the alternative when it is ever needed is a test failing
/// with hexadecimal lengths embedded in its assertion.
fn dechunk(mut body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(end) = body.windows(2).position(|window| window == b"\r\n") {
        let size = std::str::from_utf8(&body[..end])
            .ok()
            .and_then(|line| usize::from_str_radix(line.split(';').next().unwrap_or(line), 16).ok())
            .expect("a chunk size");
        if size == 0 {
            break;
        }
        let start = end + 2;
        out.extend_from_slice(&body[start..start + size]);
        body = &body[start + size + 2..];
    }
    out
}

// ---------------------------------------------------------------
// A client for a body that does not end
// ---------------------------------------------------------------

/// The head of a response whose body is still arriving.
#[derive(Debug)]
pub(crate) struct Head {
    /// The status code from the status line.
    pub status: u16,
    /// The headers, lowercased names.
    pub headers: Vec<(String, String)>,
}

impl Head {
    /// One header's value, by lowercased name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(had, _)| had == name)
            .map(|(_, value)| value.as_str())
    }
}

/// A response this client reads a piece at a time.
///
/// The other client here reads to end of file, which an events
/// stream never reaches. This one holds the socket open, reads when
/// it is asked to, and **stops reading when it is not** — which is
/// how a test makes a reader fall behind for real rather than by
/// calling something that says it has.
///
/// It undoes chunked transfer encoding incrementally, because hyper
/// has no content length to give a body it cannot see the end of.
pub(crate) struct Stream {
    socket: TcpStream,
    /// Bytes read and not yet decoded.
    raw: Vec<u8>,
    /// Everything the body has said so far, decoded.
    text: String,
    /// Whether the head has been consumed out of `raw`.
    started: bool,
}

impl Stream {
    /// Open `path`, optionally resuming after `last_event_id`.
    pub async fn open(address: SocketAddr, path: &str, last_event_id: Option<&str>) -> Self {
        let socket = TcpStream::connect(address)
            .await
            .expect("the admin listener accepts");
        let resume = match last_event_id {
            Some(id) => format!("Last-Event-ID: {id}\r\n"),
            None => String::new(),
        };
        let mut stream = Self {
            socket,
            raw: Vec::new(),
            text: String::new(),
            started: false,
        };
        let head = format!(
            "GET {path} HTTP/1.1\r\nHost: {address}\r\nAccept: text/event-stream\r\n{resume}\r\n"
        );
        stream
            .socket
            .write_all(head.as_bytes())
            .await
            .expect("the request is written");
        stream.socket.flush().await.expect("and flushed");
        stream
    }

    /// Read until the headers are complete, and return them.
    pub async fn head(&mut self) -> Head {
        loop {
            if let Some(split) = self.raw.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = std::str::from_utf8(&self.raw[..split]).expect("the head is text");
                let mut lines = head.split("\r\n");
                let status_line = lines.next().expect("a status line");
                let status = status_line
                    .split_whitespace()
                    .nth(1)
                    .expect("a status code")
                    .parse()
                    .expect("a numeric status code");
                let headers = lines
                    .filter_map(|line| line.split_once(':'))
                    .map(|(name, value)| {
                        (name.trim().to_ascii_lowercase(), value.trim().to_owned())
                    })
                    .collect();
                self.raw.drain(..split + 4);
                self.started = true;
                self.decode();
                return Head { status, headers };
            }
            assert!(self.read_more().await, "the response head never arrived");
        }
    }

    /// Read until `ready` is satisfied by everything seen so far.
    ///
    /// Fails rather than hangs, because a stream test that hangs
    /// stalls a build instead of reporting.
    pub async fn until(
        &mut self,
        patience: std::time::Duration,
        ready: impl Fn(&str) -> bool,
    ) -> String {
        let deadline = tokio::time::Instant::now() + patience;
        loop {
            if ready(&self.text) {
                return self.text.clone();
            }
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !left.is_zero(),
                "the stream never said it: {}",
                &self.text[..1024.min(self.text.len())]
            );
            let more = tokio::time::timeout(left, self.read_more())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "the stream never said it: {}",
                        &self.text[..1024.min(self.text.len())]
                    )
                });
            assert!(more, "the stream ended before it said it: {}", self.text);
        }
    }

    /// Read to the end of the body, and return the whole of it.
    pub async fn drain(&mut self) -> String {
        while self.read_more().await {}
        self.text.clone()
    }

    /// Stop reading, and let the socket close.
    pub async fn close(self) {
        drop(self.socket);
    }

    /// One read, decoded. `false` at end of file.
    async fn read_more(&mut self) -> bool {
        let mut chunk = [0u8; 16 * 1024];
        let read = self
            .socket
            .read(&mut chunk)
            .await
            .expect("the socket is readable");
        if read == 0 {
            return false;
        }
        self.raw.extend_from_slice(&chunk[..read]);
        if self.started {
            self.decode();
        }
        true
    }

    /// Move every complete chunk out of `raw` and into `text`.
    fn decode(&mut self) {
        let mut consumed = 0;
        loop {
            let rest = &self.raw[consumed..];
            let Some(end) = rest.windows(2).position(|w| w == b"\r\n") else {
                break;
            };
            let Some(size) = std::str::from_utf8(&rest[..end]).ok().and_then(|line| {
                usize::from_str_radix(line.split(';').next().unwrap_or(line), 16).ok()
            }) else {
                break;
            };
            // The whole chunk and the carriage return and line feed
            // after it, or wait for the rest of it.
            if rest.len() < end + 2 + size + 2 {
                break;
            }
            let body = &rest[end + 2..end + 2 + size];
            self.text.push_str(&String::from_utf8_lossy(body));
            consumed += end + 2 + size + 2;
            if size == 0 {
                break;
            }
        }
        self.raw.drain(..consumed);
    }
}

/// Connect an MQTT 5 client to `address` and wait for its
/// acknowledgement, returning the socket that keeps it connected.
///
/// The smallest thing that makes the broker report a connect, for
/// the one test that has to prove the events have a source rather
/// than only a ring.
pub(crate) async fn mqtt_connect(address: SocketAddr, client_id: &str) -> TcpStream {
    use bytes::BytesMut;
    use mqtt_codec::packet::Connect;
    use mqtt_codec::{decode, encode, Limits, Packet, Properties, ReasonCode, Version};

    let mut socket = TcpStream::connect(address)
        .await
        .expect("the MQTT listener accepts");
    let mut out = BytesMut::new();
    encode(
        &Packet::Connect(Box::new(Connect {
            version: Version::V5,
            client_id: client_id.into(),
            clean_start: true,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: Properties::default(),
        })),
        Version::V5,
        &mut out,
    )
    .expect("the packet encodes");
    socket.write_all(&out).await.expect("the write succeeds");
    socket.flush().await.expect("the flush succeeds");

    let mut buf = BytesMut::new();
    loop {
        if let Some(packet) =
            decode(&mut buf, Version::V5, &Limits::default()).expect("a valid packet")
        {
            match packet {
                Packet::ConnAck(ack) => {
                    assert_eq!(ack.reason, ReasonCode::Success);
                    return socket;
                }
                other => panic!("expected an acknowledgement, got {other:?}"),
            }
        }
        let mut chunk = [0u8; 1024];
        let read = tokio::time::timeout(std::time::Duration::from_secs(5), socket.read(&mut chunk))
            .await
            .expect("the broker answers")
            .expect("the socket is readable");
        assert!(read > 0, "the broker closed without acknowledging");
        buf.extend_from_slice(&chunk[..read]);
    }
}
