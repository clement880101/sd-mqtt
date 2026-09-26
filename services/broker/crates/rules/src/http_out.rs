//! Posting an envelope, and knowing when not to.
//!
//! Section 9.2's `http_out`. The hook that fires a rule writes an
//! envelope to the durable outbox and returns; everything here runs in a
//! task of its own, outside the broker's lock, because
//! [`broker_core::hooks`] refuses to let a hook reach the network.
//!
//! # Delivery is at least once, and the envelope says so
//!
//! **A timeout cannot be distinguished from a failure.** A request that
//! does not answer inside the deadline may have been received, processed
//! and acted upon; all this end knows is that no response came back. The
//! only two choices are to retry, and risk a duplicate, or not to, and
//! risk a message that was never delivered. This posts again.
//!
//! So an endpoint on the other end of an `http_out` rule **will
//! eventually see the same message twice** and has to be able to cope. It
//! is given what it needs to: [`Envelope::id`] names the delivery, is
//! minted once when the rule fires, is stored with the envelope, and does
//! not change between attempts. A receiver that remembers recent
//! identifiers can drop the repeat. One that does not must be idempotent
//! by nature, which most things that accept a sensor reading are.
//!
//! This is written down again in the operator documentation, because an
//! operator reading about a connector is not reading this file.
//!
//! # Retrying is how an endpoint that is already struggling is finished
//!
//! An endpoint that fails is usually failing for everybody at once. A
//! backoff held **per envelope** would mean a rule with a thousand
//! messages waiting makes a thousand attempts as fast as the network
//! allows, because each envelope's own timer has expired. That is a
//! thundering herd with extra steps, and it arrives exactly when the
//! endpoint can least afford it.
//!
//! So the backoff is held **on the rule**. [`Delivery::run`] stops
//! approaching the endpoint at all until the wait has been served,
//! whatever is queued behind it, and a `Retry-After` the endpoint sends
//! is honoured over the rule's own figure. The queue does not move
//! faster than the endpoint says it can.
//!
//! # Three settings on the client that are bugs if omitted
//!
//! Each of these is in [`client`], with the reason beside it:
//!
//! - **A pool timer.** Without one the idle timeout never fires, and
//!   connections are held open forever.
//! - **A connect timeout.** Without one a black-holed host is bounded
//!   only by the request deadline, and holds the connection attempt open
//!   meanwhile.
//! - **Cancelled-request retry turned off.** The client retries a
//!   request it believes was cancelled, by default, which is an attempt
//!   outside the rule's retry count and outside its backoff.

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http_body_util::{BodyExt, Full, Limited};
use hyper::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE, RETRY_AFTER};
use hyper::{Method, Request, Uri};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use storage::record::{OutboxRecord, Record};
use storage::{Storage, Writer};

use crate::envelope::Envelope;
use crate::outbox::Outbox;
use crate::rule::HttpOut;

/// How long a connection may sit idle in the pool before it is closed.
///
/// Only meaningful because [`client`] gives the pool a timer; without
/// one this number is decoration and every connection is held forever.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the transport has to establish a connection.
///
/// Separate from the rule's own timeout, and shorter than any sensible
/// value of it. A host that accepts nothing and answers nothing — a
/// firewall dropping packets rather than refusing them — is otherwise
/// bounded only by the request deadline.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The most of a response this will read.
///
/// The body is not used for anything but a log line, and an endpoint
/// answering forever must not be able to grow this task's memory.
/// Everything past this is a failed attempt rather than a truncated
/// success, because a response this end could not finish reading is a
/// response this end cannot claim to have understood.
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// The longest a `Retry-After` may push the rule out.
///
/// An endpoint is allowed to ask for patience and is not allowed to
/// silence a rule for a week. Past this the header is clamped rather
/// than ignored, because the endpoint is still saying "not yet".
const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);

/// How long the pump waits before looking again at an outbox that was
/// empty.
///
/// A poll rather than a notification, and deliberately: the hook that
/// fills the outbox runs under the broker's lock and may not do anything
/// that could block, which a bounded channel send can. A second of
/// latency on an idle rule is the price, and the rule that matters — one
/// with a backlog — never waits, because the pump goes straight round
/// again while anything is waiting.
const IDLE_POLL: Duration = Duration::from_secs(1);

/// What can go wrong before a single request is made.
///
/// Every one of these is a rule an operator configured wrongly, found
/// when the rule is turned into a poster rather than on the first
/// message. A rule that cannot post is refused at that point and says
/// which part of it is wrong.
#[derive(Debug, thiserror::Error)]
pub enum HttpOutError {
    /// The URL did not parse.
    #[error("{url:?} is not a URL")]
    NotAUrl {
        /// What was configured.
        url: String,
    },
    /// The URL is plain text to somewhere that is not this machine.
    #[error(
        "{url:?} would post in the clear: an http:// URL is permitted only to a loopback address"
    )]
    NotEncrypted {
        /// What was configured.
        url: String,
    },
    /// The URL has no host to connect to.
    #[error("{url:?} names no host")]
    NoHost {
        /// What was configured.
        url: String,
    },
    /// The method is not one HTTP has.
    #[error("{method:?} is not an HTTP method")]
    NotAMethod {
        /// What was configured.
        method: String,
    },
    /// A header name or value is not one that can be sent.
    #[error("the header {name:?} cannot be sent: {why}")]
    BadHeader {
        /// Which header.
        name: String,
        /// What is wrong with it.
        why: String,
    },
    /// Concurrency was configured as zero, which is a rule that never
    /// posts.
    #[error("a maximum concurrency of zero is a rule that never delivers anything")]
    NoConcurrency,
}

impl HttpOutError {
    /// Which field of an `http_out` action this refusal is about.
    ///
    /// Named here for the reason [`crate::rule::RuleError::field`]
    /// gives: the HTTP API marks the input at fault, and a table of
    /// variants to field names anywhere but beside the variants is a
    /// table that drifts.
    pub fn field(&self) -> &'static str {
        match self {
            Self::NotAUrl { .. } | Self::NotEncrypted { .. } | Self::NoHost { .. } => {
                "http_out.url"
            }
            Self::NotAMethod { .. } => "http_out.method",
            Self::BadHeader { .. } => "http_out.headers",
            Self::NoConcurrency => "http_out.max_concurrency",
        }
    }
}

/// What one attempt did.
///
/// Four outcomes rather than a `Result`, because the three failures are
/// answered differently: a refusal may carry a `Retry-After`, a timeout
/// says nothing about whether the endpoint acted on the request, and a
/// transport failure is the only one where nothing can have happened at
/// the far end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attempt {
    /// The endpoint answered with a 2xx.
    Delivered {
        /// Which one.
        status: u16,
    },
    /// The endpoint answered, and not with success.
    Refused {
        /// What it said.
        status: u16,
        /// How long it asked to be left alone, if it said.
        retry_after: Option<Duration>,
    },
    /// Nothing came back inside the rule's timeout.
    ///
    /// **This is not the same as a failure**, although it is treated as
    /// one: the request may have arrived and been acted on. See the
    /// module documentation on at-least-once delivery.
    TimedOut,
    /// The request could not be made or the response could not be read.
    Failed {
        /// What went wrong, for a log line.
        why: String,
    },
}

impl Attempt {
    /// Whether the envelope is finished with.
    pub fn delivered(&self) -> bool {
        matches!(self, Self::Delivered { .. })
    }

    /// How long the endpoint asked to be left alone, if it asked.
    fn requested_wait(&self) -> Option<Duration> {
        match self {
            Self::Refused { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// The HTTP client every rule shares.
///
/// One client, not one per rule: the connection pool is the thing worth
/// sharing, and two rules posting to one endpoint should reuse a
/// connection rather than each holding their own.
///
/// # The three settings
///
/// Read the module documentation first. Each of the three below is a bug
/// by omission rather than a tuning choice, which is why they are here
/// in one function with the reasons attached rather than spread over the
/// call sites that would otherwise be entitled to leave them out.
pub fn client() -> Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<hyper::body::Bytes>> {
    let mut http = HttpConnector::new();
    // The connector is about to be wrapped in one that also speaks
    // https, and it refuses any scheme but http unless told not to.
    http.enforce_http(false);
    // **Bug by omission, one of three.** Without this a host that
    // accepts nothing and refuses nothing is bounded only by the
    // rule's own request deadline, and the connection attempt is held
    // open for all of it.
    http.set_connect_timeout(Some(CONNECT_TIMEOUT));

    let https = hyper_rustls::HttpsConnectorBuilder::new()
        // Roots compiled in rather than read from the operating system:
        // a distroless image has no trust store to read, and this
        // broker is built to run in one.
        .with_webpki_roots()
        // Plain http reaches the connector at all so that a loopback
        // endpoint can be posted to; what refuses everything else is
        // `Poster::new`, before a request is ever built. See
        // [`check_scheme`].
        .https_or_http()
        .enable_http1()
        .wrap_connector(http);

    Client::builder(TokioExecutor::new())
        // **Bug by omission, two of three.** Without a timer the idle
        // timeout below never fires at all, and every connection this
        // client opens is held until the process ends.
        .pool_timer(TokioTimer::new())
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        // **Bug by omission, three of three.** The default is to retry
        // a request the client believes was cancelled, which is an
        // attempt made outside the rule's retry count and outside its
        // backoff — the one thing this module exists to bound.
        .retry_canceled_requests(false)
        .build(https)
}

/// A rule's configured endpoint, ready to be posted to.
///
/// Everything an operator can get wrong is checked here, once, rather
/// than on every message: the URL, the scheme, the method and every
/// header.
#[derive(Debug, Clone)]
pub struct Poster {
    /// The shared client.
    client: Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<hyper::body::Bytes>>,
    /// Where to post.
    uri: Uri,
    /// Which method.
    method: Method,
    /// What to send with it, content type included.
    headers: HeaderMap,
    /// How long one attempt may take.
    timeout: Duration,
}

impl Poster {
    /// Check a rule's endpoint and prepare to post to it.
    ///
    /// # Errors
    ///
    /// [`HttpOutError`], naming the part of the configuration that is
    /// wrong.
    pub fn new(
        client: Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<hyper::body::Bytes>>,
        action: &HttpOut,
    ) -> Result<Self, HttpOutError> {
        let Endpoint {
            uri,
            method,
            headers,
            timeout,
        } = parts(action)?;
        Ok(Self {
            client,
            uri,
            method,
            headers,
            timeout,
        })
    }

    /// Where this posts.
    pub fn uri(&self) -> &Uri {
        &self.uri
    }

    /// Make one attempt.
    ///
    /// Bounded by the rule's timeout however the endpoint behaves: an
    /// endpoint that accepts the connection and then says nothing is the
    /// most common real failure, and it is the one a retry count alone
    /// does nothing about.
    pub async fn post(&self, envelope: &Envelope) -> Attempt {
        let mut request = Request::builder()
            .method(self.method.clone())
            .uri(self.uri.clone())
            .body(Full::new(hyper::body::Bytes::from(envelope.to_json())))
            .expect("a checked method, a checked uri and an owned body");
        *request.headers_mut() = self.headers.clone();

        match tokio::time::timeout(self.timeout, self.client.request(request)).await {
            Err(_) => Attempt::TimedOut,
            Ok(Err(error)) => Attempt::Failed {
                why: error.to_string(),
            },
            Ok(Ok(response)) => {
                let status = response.status();
                let retry_after = retry_after(response.headers());
                // Read and discard, bounded. The body is not used, but
                // a response left unread keeps the connection out of
                // the pool, and an unbounded read is an endpoint that
                // answers forever growing this task's memory.
                //
                // Still bounded by the rule's timeout: a slow body is
                // the same failure as a slow status line.
                let read = tokio::time::timeout(
                    self.timeout,
                    Limited::new(response.into_body(), MAX_RESPONSE_BYTES).collect(),
                )
                .await;
                match read {
                    Err(_) => Attempt::TimedOut,
                    Ok(Err(error)) => Attempt::Failed {
                        why: format!("the response body could not be read: {error}"),
                    },
                    Ok(Ok(_)) if status.is_success() => Attempt::Delivered {
                        status: status.as_u16(),
                    },
                    Ok(Ok(_)) => Attempt::Refused {
                        status: status.as_u16(),
                        retry_after,
                    },
                }
            }
        }
    }
}

/// Check a rule's endpoint without building anything to post with.
///
/// For whatever reads an operator's configuration: **a rule that cannot
/// post should stop the broker starting**, said once with the rule
/// named, rather than becoming a rule that quietly never fires. Exactly
/// the checks [`Poster::new`] makes, because it is the same function.
///
/// # Errors
///
/// [`HttpOutError`], naming the part of the configuration that is wrong.
pub fn check(action: &HttpOut) -> Result<(), HttpOutError> {
    parts(action).map(|_| ())
}

/// Everything a request needs, from what an operator configured.
///
/// **Not just this connector's.** Section 9.2 gives two connectors a
/// URL — `http_out` and `http_poll` — and everything that can be wrong
/// with one is wrong in the same way for both: the refusal of a plain
/// `http://` URL to anywhere but this machine is the same rule, and a
/// second copy of it is a second place for it to be weakened. So this
/// is the one that both go through; see [`Endpoint`].
#[derive(Debug, Clone)]
pub struct Endpoint {
    /// Where the request goes.
    pub uri: Uri,
    /// Which method it uses.
    pub method: Method,
    /// What it carries.
    pub headers: HeaderMap,
    /// How long one attempt may take.
    pub timeout: Duration,
}

/// Check a URL, a method, some headers and a timeout, whichever
/// connector configured them.
///
/// `content_type` is set before the configured headers are, so that a
/// connector configuring its own replaces this rather than being
/// replaced by it. A connector that sends no body passes `None`.
///
/// # Errors
///
/// [`HttpOutError`], naming the part of the configuration that is
/// wrong. Every variant but [`HttpOutError::NoConcurrency`], which
/// belongs to `http_out`'s own pump and is checked by [`parts`].
pub fn endpoint(
    url: &str,
    method: &str,
    headers: &[(String, String)],
    timeout_ms: u32,
    content_type: Option<&'static str>,
) -> Result<Endpoint, HttpOutError> {
    let uri: Uri = url.parse().map_err(|_| HttpOutError::NotAUrl {
        url: url.to_owned(),
    })?;
    check_scheme(&uri, url)?;

    let method = Method::from_bytes(method.as_bytes()).map_err(|_| HttpOutError::NotAMethod {
        method: method.to_owned(),
    })?;

    let mut built = HeaderMap::new();
    if let Some(content_type) = content_type {
        built.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    }
    for (name, value) in headers {
        let name =
            HeaderName::from_bytes(name.as_bytes()).map_err(|error| HttpOutError::BadHeader {
                name: name.clone(),
                why: error.to_string(),
            })?;
        let value = HeaderValue::from_str(value).map_err(|error| HttpOutError::BadHeader {
            name: name.as_str().to_owned(),
            why: error.to_string(),
        })?;
        built.insert(name, value);
    }

    Ok(Endpoint {
        uri,
        method,
        headers: built,
        timeout: Duration::from_millis(u64::from(timeout_ms)),
    })
}

/// Everything a request needs, from what an `http_out` rule configured.
fn parts(action: &HttpOut) -> Result<Endpoint, HttpOutError> {
    if action.max_concurrency == 0 {
        return Err(HttpOutError::NoConcurrency);
    }
    endpoint(
        &action.url,
        &action.method,
        &action.headers,
        action.timeout_ms,
        Some("application/json"),
    )
}

/// Refuse a URL that would put the payload on a network in the clear.
///
/// **The design says HTTPS and this permits one exception**: a plain
/// `http://` URL whose host is a loopback address. The rule the design
/// is stating is that a payload and the credentials in a rule's headers
/// must not cross a network unencrypted, and traffic to `127.0.0.1` does
/// not cross one. Posting to a sidecar on the loopback is a real and
/// common deployment, and the alternative — a test-only door in this
/// function — would mean the refusal every other URL meets is never the
/// code any test exercises.
///
/// A host name other than `localhost` is refused even if it resolves to
/// a loopback address, because what it resolves to is not known here and
/// may not be the same thing twice.
fn check_scheme(uri: &Uri, configured: &str) -> Result<(), HttpOutError> {
    let Some(host) = uri.host() else {
        return Err(HttpOutError::NoHost {
            url: configured.to_owned(),
        });
    };
    match uri.scheme_str() {
        Some("https") => Ok(()),
        Some("http") if is_loopback(host) => Ok(()),
        _ => Err(HttpOutError::NotEncrypted {
            url: configured.to_owned(),
        }),
    }
}

/// Whether this host is unambiguously this machine.
///
/// Literals and the one name every operating system is required to map
/// to a loopback address. Anything else is a name whose meaning depends
/// on a resolver this code cannot see.
fn is_loopback(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    // `Uri::host` leaves the brackets on an IPv6 literal.
    let host = host.strip_prefix('[').unwrap_or(host);
    let host = host.strip_suffix(']').unwrap_or(host);
    if let Ok(address) = host.parse::<Ipv4Addr>() {
        return address.is_loopback();
    }
    host.parse::<std::net::Ipv6Addr>()
        .is_ok_and(|address| address.is_loopback())
}

/// The `Retry-After` header as a duration, when it is one.
///
/// Only the delay-seconds form. The HTTP-date form is also legal and is
/// deliberately not parsed: reading it needs a date parser and a trusted
/// clock agreement with the endpoint, and an endpoint that sends one
/// falls back to the rule's own backoff, which is a worse answer rather
/// than a wrong one.
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(RETRY_AFTER)?.to_str().ok()?;
    let seconds: u64 = value.trim().parse().ok()?;
    Some(Duration::from_secs(seconds).min(MAX_RETRY_AFTER))
}

/// Where an envelope goes when it has run out of attempts.
///
/// A trait rather than a broker handle, because this crate must not have
/// one: publishing goes through the shell, which is the only thing that
/// holds the broker's lock. Section 9.2 sends it to
/// `$sd/rules/<id>/dlq`.
pub trait DeadLetters: Send + Sync {
    /// Publish a dead letter. Must not block.
    fn dead_letter(&self, topic: &str, payload: Vec<u8>);
}

/// What one turn of the pump did, which is what decides how long before
/// the next one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Turn {
    /// Nothing was waiting.
    Idle,
    /// An envelope was delivered and has left the outbox.
    Delivered,
    /// An attempt failed. **The whole rule waits this long**, whatever
    /// else is queued behind it; see the module documentation.
    BackedOff(Duration),
    /// The envelope ran out of attempts and went to the dead-letter
    /// topic. **The rule still waits**, for the length carried here: an
    /// endpoint that is failing must not be emptied into the
    /// dead-letter topic as fast as the network allows.
    DeadLettered(Duration),
    /// The head of the outbox names a payload the store no longer holds,
    /// so there is nothing to post. The envelope is dropped rather than
    /// retried forever.
    Orphaned,
    /// The store would not answer, so nothing could be decided.
    Stalled,
}

/// One rule's outbox, drained.
///
/// Holds no state about any envelope: everything about how far a
/// delivery has got is in the stored record, so this can be stopped,
/// dropped and rebuilt without losing a count.
pub struct Delivery {
    /// What this rule has been doing. See [`crate::stats`].
    stats: Arc<crate::stats::RuleStats>,
    /// Whose outbox.
    outbox: Arc<Outbox>,
    /// Where envelopes are read from.
    store: Arc<dyn Storage>,
    /// Where records are written.
    writer: Arc<Writer>,
    /// Where they are posted.
    poster: Poster,
    /// How many retries follow the first attempt.
    retries: u8,
    /// How long to wait after the first failure.
    backoff: Duration,
    /// Where an exhausted envelope goes.
    dead_letters: Arc<dyn DeadLetters>,
}

impl Delivery {
    /// A pump for one rule.
    pub fn new(
        outbox: Arc<Outbox>,
        store: Arc<dyn Storage>,
        writer: Arc<Writer>,
        poster: Poster,
        action: &HttpOut,
        dead_letters: Arc<dyn DeadLetters>,
    ) -> Self {
        Self {
            stats: Arc::new(crate::stats::RuleStats::new()),
            outbox,
            store,
            writer,
            poster,
            retries: action.retries,
            backoff: Duration::from_millis(u64::from(action.backoff_ms)),
            dead_letters,
        }
    }

    /// Count what this pump does against `stats`.
    ///
    /// A builder rather than a seventh argument to [`Delivery::new`],
    /// because every test of this pump is about what it posts and none
    /// of them is about the counters: a required argument would put a
    /// handle nobody reads in twenty call sites.
    #[must_use]
    pub fn with_stats(mut self, stats: Arc<crate::stats::RuleStats>) -> Self {
        self.stats = stats;
        self
    }

    /// Take one envelope as far as it goes.
    ///
    /// At most one attempt, at most one request. Returning after a
    /// failure rather than moving to the next envelope is what makes the
    /// backoff belong to the rule: the caller sleeps, and nothing else
    /// in this outbox is posted meanwhile.
    pub async fn turn(&self) -> Turn {
        let entry = match self.outbox.head(self.store.as_ref(), 0) {
            Ok(Some(entry)) => entry,
            Ok(None) => return Turn::Idle,
            Err(error) => {
                tracing::error!(
                    rule = self.outbox.rule().as_str(),
                    %error,
                    "the outbox could not be read"
                );
                return Turn::Stalled;
            }
        };

        // **The deadline is honoured, not only the count.** Both halves
        // of an attempt are in the stored record and a restart that
        // respected the count and forgot the deadline would still post
        // the moment the process came back — which is precisely the
        // node returning from an outage and hitting a still-dead
        // endpoint that the durable record exists to prevent. A pump
        // that has just been built has made no attempts of its own and
        // has no other way to know it is too early.
        //
        // Coarse seconds, because that is what the record holds: a
        // backoff under a second rounds to no wait at all, which is the
        // same answer the rule's own timer would have given by the time
        // the process restarted.
        let now = now_seconds();
        if entry.next_attempt_at > now {
            let wait = Duration::from_secs(u64::from(entry.next_attempt_at - now));
            tracing::debug!(
                rule = self.outbox.rule().as_str(),
                sequence = entry.sequence,
                attempts = entry.attempts,
                wait_s = wait.as_secs(),
                "the head of this outbox is not due yet"
            );
            return Turn::BackedOff(wait);
        }

        let body = match self.store.body(&entry.body) {
            Ok(Some(body)) => body,
            Ok(None) => {
                // The payload is gone, so there is nothing to post and
                // no attempt count that would ever make it come back.
                // Dropped and counted rather than retried, and the
                // release that goes with it is owed regardless: the
                // store refuses a release of a body it does not hold,
                // so this one removal goes on its own.
                tracing::error!(
                    rule = self.outbox.rule().as_str(),
                    sequence = entry.sequence,
                    "an envelope named a payload the store no longer holds; it cannot be sent"
                );
                self.record(self.outbox.orphaned(entry.sequence)).await;
                return Turn::Orphaned;
            }
            Err(error) => {
                tracing::error!(
                    rule = self.outbox.rule().as_str(),
                    %error,
                    "the payload could not be read"
                );
                return Turn::Stalled;
            }
        };

        let envelope = Envelope::build(&entry, &body);
        let attempt = self.poster.post(&envelope).await;
        if attempt.delivered() {
            tracing::debug!(
                rule = self.outbox.rule().as_str(),
                sequence = entry.sequence,
                id = %envelope.id,
                "delivered"
            );
            self.stats.delivered();
            self.finish(&entry).await;
            return Turn::Delivered;
        }

        // The attempt that has just happened is not yet in the stored
        // count, so this is what the count will become.
        let made = u32::from(self.retries).saturating_add(1);
        if entry.attempts.saturating_add(1) >= made {
            tracing::warn!(
                rule = self.outbox.rule().as_str(),
                sequence = entry.sequence,
                id = %envelope.id,
                attempts = entry.attempts + 1,
                ?attempt,
                "an envelope ran out of attempts and is being dead-lettered"
            );
            self.stats.failed();
            self.stats.dead_lettered();
            self.dead_letters.dead_letter(
                &self.outbox.rule().dead_letter_topic(),
                dead_letter(&envelope, &attempt, entry.attempts + 1),
            );
            self.finish(&entry).await;
            // The endpoint still failed, so the rule still waits. A
            // dead letter that reset the backoff would let a rule with
            // a backlog against a dead endpoint empty itself into the
            // dead-letter topic as fast as the network allowed.
            return Turn::DeadLettered(self.wait_after(&entry, &attempt));
        }

        let wait = self.wait_after(&entry, &attempt);
        let next_attempt_at =
            now_seconds().saturating_add(u32::try_from(wait.as_secs()).unwrap_or(u32::MAX));
        tracing::warn!(
            rule = self.outbox.rule().as_str(),
            sequence = entry.sequence,
            id = %envelope.id,
            attempts = entry.attempts + 1,
            ?attempt,
            wait_ms = wait.as_millis(),
            "an attempt failed; the rule is backing off"
        );
        self.stats.failed();
        self.record(vec![self.outbox.attempted(&entry, next_attempt_at)])
            .await;
        Turn::BackedOff(wait)
    }

    /// The envelope is finished with: it leaves, and the reference its
    /// append took is released, in one batch.
    async fn finish(&self, entry: &OutboxRecord) {
        self.record(self.outbox.completed(entry)).await;
    }

    /// Write what just happened, and **wait for it to be durable**.
    ///
    /// Waiting costs a flush per envelope, and both halves of the reason
    /// are worth having.
    ///
    /// The first is correctness inside one process. The next thing this
    /// pump does is read the head of the outbox again. A batch handed to
    /// the writer is applied on its own thread, some time later, so a
    /// pump that did not wait would read back the envelope it had just
    /// delivered and post it a second time — not as the at-least-once
    /// duplicate a timeout produces, but every time, against a perfectly
    /// healthy endpoint.
    ///
    /// The second is the one the milestone is about. An attempt that is
    /// not durable before the next attempt is made is an attempt count
    /// that a restart at the wrong moment resets, which is exactly the
    /// state this design put in the store rather than in memory.
    async fn record(&self, records: Vec<Record>) {
        let Ok(durable) = self.writer.apply_durable(records) else {
            tracing::error!(
                rule = self.outbox.rule().as_str(),
                "the storage writer stopped, so this rule can no longer record what it did"
            );
            return;
        };
        if let Err(error) = durable.await {
            tracing::error!(
                rule = self.outbox.rule().as_str(),
                %error,
                "what this rule did could not be made durable"
            );
        }
    }

    /// How long the rule waits after this attempt.
    ///
    /// The endpoint's own `Retry-After` wins where it sent one, because
    /// it knows more about its own recovery than a number in our
    /// configuration does. Otherwise the rule's backoff, doubled once
    /// per attempt already made and capped, which is the ordinary
    /// exponential shape.
    fn wait_after(&self, entry: &OutboxRecord, attempt: &Attempt) -> Duration {
        if let Some(asked) = attempt.requested_wait() {
            return asked;
        }
        let doubling = 1_u32 << entry.attempts.min(16);
        self.backoff.saturating_mul(doubling).min(MAX_RETRY_AFTER)
    }

    /// Drain this rule's outbox until the task is dropped.
    ///
    /// **The sleep after a failure is the whole design.** It is the rule
    /// waiting, not one envelope: nothing else in this outbox is posted
    /// during it, so an endpoint that is failing sees one attempt per
    /// window rather than one per message queued behind the first.
    pub async fn run(self) {
        loop {
            match self.turn().await {
                Turn::Delivered | Turn::Orphaned => {}
                Turn::Idle => tokio::time::sleep(IDLE_POLL).await,
                Turn::BackedOff(wait) | Turn::DeadLettered(wait) => {
                    tokio::time::sleep(wait).await;
                }
                // A store that will not answer is not something to spin
                // on. The same wait as an idle outbox, because the next
                // read is the only way to find out whether it recovered.
                Turn::Stalled => tokio::time::sleep(IDLE_POLL).await,
            }
        }
    }
}

/// What goes to `$sd/rules/<id>/dlq`.
///
/// The envelope that could not be delivered, wrapped with why. The
/// envelope is nested rather than flattened so that a consumer of the
/// dead-letter topic can hand the inner document to whatever would have
/// received it, unchanged, once the endpoint is back.
fn dead_letter(envelope: &Envelope, attempt: &Attempt, attempts: u32) -> Vec<u8> {
    let reason = match attempt {
        Attempt::Delivered { .. } => "delivered".to_owned(),
        Attempt::Refused { status, .. } => format!("the endpoint answered {status}"),
        Attempt::TimedOut => "the endpoint did not answer in time".to_owned(),
        Attempt::Failed { why } => why.clone(),
    };
    let document = serde_json::json!({
        "reason": reason,
        "attempts": attempts,
        "envelope": envelope,
    });
    serde_json::to_vec(&document).expect("a dead letter is strings, numbers and an envelope")
}

/// The wall clock in whole seconds, as the stored records count time.
fn now_seconds() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u32::try_from(since.as_secs()).unwrap_or(u32::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn action(url: &str) -> HttpOut {
        HttpOut {
            url: url.to_owned(),
            ..HttpOut::default()
        }
    }

    #[track_caller]
    fn refused(url: &str) -> HttpOutError {
        Poster::new(client(), &action(url))
            .err()
            .unwrap_or_else(|| panic!("{url} must be refused"))
    }

    /// A plain-text URL to anywhere but this machine is refused.
    ///
    /// Section 9.2 posts over HTTPS. The payload is somebody's data and
    /// the headers are usually somebody's bearer token, and neither
    /// crosses a network in the clear.
    #[test]
    fn a_plain_text_url_to_a_real_host_is_refused() {
        assert!(matches!(
            refused("http://example.invalid/hook"),
            HttpOutError::NotEncrypted { .. }
        ));
        // Including one that merely looks local.
        assert!(matches!(
            refused("http://localhost.example.invalid/hook"),
            HttpOutError::NotEncrypted { .. }
        ));
        // And a scheme that is neither.
        assert!(matches!(
            refused("ftp://example.invalid/hook"),
            HttpOutError::NotEncrypted { .. }
        ));
    }

    /// And to this machine it is permitted, which is the documented
    /// exception.
    ///
    /// A sidecar on the loopback is a real deployment and nothing
    /// crosses a network to reach it. This is also what lets every
    /// delivery test below exercise the same posting code an operator's
    /// rule uses, rather than a door opened for tests.
    #[test]
    fn a_plain_text_url_to_the_loopback_is_permitted() {
        for url in [
            "http://127.0.0.1:8080/hook",
            "http://127.0.0.2:8080/hook",
            "http://localhost:8080/hook",
            "http://LOCALHOST:8080/hook",
            "http://[::1]:8080/hook",
        ] {
            assert!(
                Poster::new(client(), &action(url)).is_ok(),
                "{url} must be permitted"
            );
        }
    }

    #[test]
    fn an_https_url_is_permitted_wherever_it_points() {
        assert!(Poster::new(client(), &action("https://example.invalid/hook")).is_ok());
    }

    #[test]
    fn a_url_that_is_not_one_is_refused_as_such() {
        assert!(matches!(
            refused("not a url at all"),
            HttpOutError::NotAUrl { .. }
        ));
        // A URI with no authority parses perfectly well and names
        // nothing to connect to, which is a different mistake from a
        // string that is not a URI at all and deserves a different
        // message. `https:///hook` is **not** an example of it: the
        // parser rejects an empty authority outright, so it arrives
        // here as `NotAUrl`. This assertion said otherwise and was
        // wrong about the parser rather than about the code.
        assert!(matches!(refused("/hook"), HttpOutError::NoHost { .. }));
        assert!(matches!(
            refused("https:///hook"),
            HttpOutError::NotAUrl { .. }
        ));
    }

    #[test]
    fn a_method_and_a_header_are_checked_before_anything_is_sent() {
        let mut bad_method = action("https://example.invalid/hook");
        bad_method.method = "NOT A METHOD".to_owned();
        assert!(matches!(
            Poster::new(client(), &bad_method),
            Err(HttpOutError::NotAMethod { .. })
        ));

        let mut bad_header = action("https://example.invalid/hook");
        bad_header.headers = vec![("not a name".to_owned(), "v".to_owned())];
        assert!(matches!(
            Poster::new(client(), &bad_header),
            Err(HttpOutError::BadHeader { .. })
        ));

        let mut bad_value = action("https://example.invalid/hook");
        bad_value.headers = vec![("x-token".to_owned(), "a\nb".to_owned())];
        assert!(matches!(
            Poster::new(client(), &bad_value),
            Err(HttpOutError::BadHeader { .. })
        ));
    }

    #[test]
    fn a_rule_that_may_never_post_is_refused() {
        let mut none = action("https://example.invalid/hook");
        none.max_concurrency = 0;
        assert!(matches!(
            Poster::new(client(), &none),
            Err(HttpOutError::NoConcurrency)
        ));
    }

    #[test]
    fn a_retry_after_in_seconds_is_read_and_clamped() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("12"));
        assert_eq!(retry_after(&headers), Some(Duration::from_secs(12)));

        headers.insert(RETRY_AFTER, HeaderValue::from_static("999999"));
        assert_eq!(
            retry_after(&headers),
            Some(MAX_RETRY_AFTER),
            "an endpoint may ask for patience and may not silence a rule for a week"
        );

        // The HTTP-date form is legal and deliberately not parsed; the
        // rule's own backoff applies instead.
        headers.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Wed, 21 Oct 2026 07:28:00 GMT"),
        );
        assert_eq!(retry_after(&headers), None);
        assert_eq!(retry_after(&HeaderMap::new()), None);
    }
}
