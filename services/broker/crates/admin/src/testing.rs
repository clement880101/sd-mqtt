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

/// Split a response into its status, its headers and its body.
fn parse(raw: &[u8]) -> Response {
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

    Response {
        status,
        headers,
        body: String::from_utf8(body).expect("the body is text"),
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
