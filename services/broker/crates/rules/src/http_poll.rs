//! Fetching a URL on an interval, and publishing what came back.
//!
//! Section 9.2's `http_poll`: a GET on an interval, publishing the
//! response body to a topic, running on exactly one node — the one
//! [`crate::singleton`] selects.
//!
//! The client, the timeout discipline and the refusal of a plain-text
//! URL to anywhere but this machine are [`crate::http_out`]'s, reused
//! rather than written again. There is one description of what a
//! connector may be pointed at, and it is [`http_out::endpoint`].
//!
//! # A poller is not an outbox, and that is the design
//!
//! The connector next door has a durable outbox, so a reader arriving
//! here will look for this one's and should be told there is none
//! rather than left to wonder whether it was forgotten.
//!
//! An outbox exists for `http_out` because a message that fired a rule
//! is **somebody else's data**, accepted from a publisher, and losing
//! it in a restart loses something that was entrusted to this broker.
//! There is no equivalent here. A poller is not given anything: it goes
//! and asks. A fetch that failed has produced nothing to lose, and the
//! endpoint will be asked again one interval later.
//!
//! Worse, a queue would actively make this connector wrong. What a
//! poller publishes is the current value of something — a reading, a
//! rate, a status — and a value that could not be fetched at noon is
//! not owed to anybody at half past. **A stale body is worse than a
//! missing one**, because a subscriber cannot tell a late publication
//! from a fresh one, and a retained topic carrying a reading from
//! before an outage looks exactly like a working sensor.
//!
//! So there is no queue, no retry count and no backoff, and none of the
//! three is a field somebody forgot. What a failed fetch gets is the
//! next interval, which is the same thing a successful one gets.
//!
//! # A fetch slower than the interval loses the ticks it covered
//!
//! The question the outbound connector answers with a rule about
//! per-rule backoff turns up here in a different disguise: an endpoint
//! that has become slow must not be sent more requests because it has
//! become slow.
//!
//! Two things are decided, and they are separate:
//!
//! **One fetch at a time, ever.** [`Poller::run`] awaits each fetch
//! before it considers the next, so there is no arrangement of
//! intervals and response times that puts two requests to one endpoint
//! in flight at once. An endpoint that stops answering altogether is
//! approached once per timeout rather than once per interval — the
//! opposite of the amplification that finishes off an endpoint which is
//! already struggling.
//!
//! **The ticks a slow fetch covered are dropped, not queued.** When a
//! fetch overruns, the next one is due at the next interval boundary
//! *after it finished*, and the boundaries that passed meanwhile are
//! gone. The alternative — the default of every interval timer in every
//! runtime, including tokio's — is to fire them in a burst, which for a
//! poller means a slow endpoint is answered with back-to-back requests
//! carrying no gap at all, and means the broker publishes a run of
//! readings all taken at the same moment. Neither is what anybody wants
//! from a connector whose whole job is "the current value, every so
//! often".
//!
//! [`next_due`] is where that lives, and it is a pure function so that
//! the rule can be asserted without waiting for a clock.
//!
//! # What it publishes carries a hop count
//!
//! Section 9.2: every publication a connector originates carries one,
//! because the loop this broker cannot otherwise see is a cycle of
//! connectors with no bridge in it. See [`crate::hops`]; a poller
//! originates, so its count is one.
//!
//! # The body is bounded, and a body that did not fit is a failed fetch
//!
//! `http_out` reads a bounded response and treats anything past the
//! bound as a failure rather than a truncated success, because a
//! response this end could not finish reading is a response this end
//! cannot claim to have understood. The same rule, and here it has a
//! second edge: half a JSON document published to a topic is worse than
//! nothing published at all, and a subscriber has no way to tell.

use std::sync::Arc;

use http_body_util::{BodyExt, Full, Limited};
// `hyper`'s re-export of `bytes::Bytes`, which is the same type the
// broker's publish path takes. Named through hyper so that this crate
// does not have to carry `bytes` as a dependency of its own for one
// alias; the budget in section 15 counts those.
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use mqtt_codec::QoS;
use sdmqtt_server::publish::{publish, Publication, PublishRefused};
use sdmqtt_server::shared::Shared;
use tokio::time::{Duration, Instant};

use crate::hops;
use crate::http_out::{self, Endpoint, HttpOutError};
use crate::rule::{HttpPoll, RuleId};
use crate::singleton::Ownership;

/// The method a poller uses. Section 9.2 says a GET and says no more,
/// so there is nothing to configure and nothing for an operator to get
/// wrong.
const METHOD: &str = "GET";

/// What can be wrong with an `http_poll` rule.
///
/// Found when the rule is read rather than on the first fetch: a rule
/// that cannot fetch should stop the broker starting, said once with
/// the rule named, rather than becoming a rule that quietly publishes
/// nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HttpPollError {
    /// The endpoint is not one this broker will fetch from.
    ///
    /// **The same checks `http_out` makes**, because it is the same
    /// function: the URL, the scheme, and every header. Section 9.2
    /// points two connectors at a URL and a second description of what
    /// a URL may be is a second place for the refusal of a plain-text
    /// one to be weakened.
    #[error("http_poll: {0}")]
    Endpoint(String),
    /// The interval is zero.
    ///
    /// A poller with no interval is not a fast poller; it is a loop
    /// that asks an endpoint for the same document as quickly as that
    /// endpoint can answer, for as long as the broker runs.
    #[error(
        "http_poll.interval_ms is zero, which is not a fast poller but a loop that fetches \
         as fast as the endpoint answers; set an interval"
    )]
    NoInterval,
    /// The body limit is zero.
    ///
    /// Refused rather than taken to mean "no limit", which is the other
    /// thing zero could plausibly mean and is the dangerous one.
    #[error(
        "http_poll.max_body_bytes is zero, so no response could ever be read; it bounds a \
         body rather than removing the bound"
    )]
    NoBody,
    /// The configured topic is not one a message can be published to.
    #[error("http_poll.topic {topic:?} is not a topic a message can be published to: {why}")]
    NotATopic {
        /// What was configured.
        topic: String,
        /// Why it is not a topic.
        why: String,
    },
    /// The configured topic is in the reserved namespace.
    #[error(
        "http_poll.topic {topic:?} is in the dollar-prefixed namespace the broker reserves \
         for itself, which is not publishable"
    )]
    Reserved {
        /// What was configured.
        topic: String,
    },
}

impl HttpPollError {
    /// Which field of an `http_poll` action this refusal is about.
    ///
    /// [`HttpPollError::Endpoint`] carries a sentence rather than the
    /// [`HttpOutError`] it came from, so the field it names is the
    /// URL: that is what all but one of those refusals is about, and
    /// the one exception is a header whose own sentence says so.
    pub fn field(&self) -> &'static str {
        match self {
            Self::Endpoint(_) => "http_poll.url",
            Self::NoInterval => "http_poll.interval_ms",
            Self::NoBody => "http_poll.max_body_bytes",
            Self::NotATopic { .. } | Self::Reserved { .. } => "http_poll.topic",
        }
    }
}

/// Check a rule's configuration without building anything to fetch
/// with.
///
/// For whatever reads an operator's configuration. Exactly the checks
/// [`Poller::new`] makes, because it is the same function.
///
/// # Errors
///
/// [`HttpPollError`], naming the part of the configuration that is
/// wrong.
pub fn check(action: &HttpPoll) -> Result<(), HttpPollError> {
    parts(action).map(|_| ())
}

/// Everything a poller needs, from what an operator configured.
fn parts(action: &HttpPoll) -> Result<Endpoint, HttpPollError> {
    if action.interval_ms == 0 {
        return Err(HttpPollError::NoInterval);
    }
    if action.max_body_bytes == 0 {
        return Err(HttpPollError::NoBody);
    }

    // The broker's own limits are not reachable from here, and the
    // default ones are what every topic in a configuration is checked
    // against elsewhere in this crate.
    let limits = mqtt_codec::Limits::default();
    mqtt_codec::topic::validate_name(&action.topic, &limits).map_err(|error| {
        HttpPollError::NotATopic {
            topic: action.topic.clone(),
            why: error.to_string(),
        }
    })?;
    if action.topic.starts_with('$') {
        return Err(HttpPollError::Reserved {
            topic: action.topic.clone(),
        });
    }

    // A GET carries no body, so no content type is set: a request that
    // announces the type of a body it does not have is a request that
    // says something untrue about itself.
    http_out::endpoint(
        &action.url,
        METHOD,
        &action.headers,
        action.timeout_ms,
        None,
    )
    .map_err(|error: HttpOutError| HttpPollError::Endpoint(error.to_string()))
}

/// What one fetch produced.
///
/// Four outcomes rather than a `Result`, as [`crate::http_out::Attempt`]
/// has four, and for the same reason: they are not the same event. Only
/// the first publishes anything; the other three skip the interval, and
/// which of the three it was is the only thing an operator has to go on
/// when the topic stops moving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fetched {
    /// The endpoint answered with a 2xx and a body that fitted.
    Body(Bytes),
    /// The endpoint answered, and not with success.
    Refused {
        /// What it said.
        status: u16,
    },
    /// Nothing came back inside the rule's timeout.
    TimedOut,
    /// The request could not be made, or the response could not be
    /// read — a body past the limit included.
    Failed {
        /// What went wrong, for a log line.
        why: String,
    },
}

/// A rule's configured endpoint, ready to be fetched from.
///
/// Everything an operator can get wrong is checked once here rather
/// than on every interval.
#[derive(Debug, Clone)]
pub struct Fetcher {
    /// The shared client. The same one every `http_out` rule posts
    /// through: the connection pool is the thing worth sharing.
    client: Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>,
    /// Where to fetch, how, and how long it may take.
    endpoint: Endpoint,
    /// The most of a response body that will be read.
    max_body: usize,
}

impl Fetcher {
    /// Check a rule's endpoint and prepare to fetch from it.
    ///
    /// # Errors
    ///
    /// [`HttpPollError`], naming the part of the configuration that is
    /// wrong.
    pub fn new(
        client: Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>,
        action: &HttpPoll,
    ) -> Result<Self, HttpPollError> {
        Ok(Self {
            client,
            endpoint: parts(action)?,
            max_body: action.max_body_bytes as usize,
        })
    }

    /// Where this fetches from.
    pub fn uri(&self) -> &hyper::Uri {
        &self.endpoint.uri
    }

    /// Make one request.
    ///
    /// Bounded by the rule's timeout however the endpoint behaves,
    /// **including while the body is being read**: an endpoint that
    /// answers with a status line and then trickles is the same failure
    /// as one that never answers at all, and a poller that waited for
    /// it would have no interval left.
    pub async fn fetch(&self) -> Fetched {
        let mut request = Request::builder()
            .method(self.endpoint.method.clone())
            .uri(self.endpoint.uri.clone())
            .body(Full::new(Bytes::new()))
            .expect("a checked method, a checked uri and an empty body");
        *request.headers_mut() = self.endpoint.headers.clone();

        let timeout = self.endpoint.timeout;
        match tokio::time::timeout(timeout, self.client.request(request)).await {
            Err(_) => Fetched::TimedOut,
            Ok(Err(error)) => Fetched::Failed {
                why: error.to_string(),
            },
            Ok(Ok(response)) => {
                let status = response.status();
                if !status.is_success() {
                    // The body of a refusal is not read. It is not
                    // going to be published, and reading it would let
                    // an endpoint that is refusing hold this poller for
                    // the whole timeout as well.
                    return Fetched::Refused {
                        status: status.as_u16(),
                    };
                }
                match tokio::time::timeout(
                    timeout,
                    Limited::new(response.into_body(), self.max_body).collect(),
                )
                .await
                {
                    Err(_) => Fetched::TimedOut,
                    // A body past the limit arrives here, as the error
                    // `Limited` returns. A failed fetch and not a
                    // truncated success: half a document published to a
                    // topic is worse than nothing published, and a
                    // subscriber cannot tell the two apart.
                    Ok(Err(error)) => Fetched::Failed {
                        why: format!(
                            "the response body could not be read within {} bytes: {error}",
                            self.max_body
                        ),
                    },
                    Ok(Ok(body)) => Fetched::Body(body.to_bytes()),
                }
            }
        }
    }
}

/// What one turn of a poller did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Polled {
    /// The body was fetched and published.
    Published {
        /// How big it was.
        bytes: usize,
        /// How many subscribers it reached. Zero is an ordinary
        /// answer.
        delivered: usize,
    },
    /// The fetch produced no body, so nothing was published.
    ///
    /// **The interval is skipped and the poller carries on.** There is
    /// nothing else to do with a failed fetch: there is no queue to put
    /// it in, and the endpoint will be asked again one interval later.
    Skipped(Fetched),
    /// A body was fetched and the broker would not publish it.
    Refused(String),
}

/// One rule's poller.
///
/// Holds nothing about any fetch between intervals, because there is
/// nothing to hold: what was fetched was published or was not, and
/// either way the next interval starts again from the endpoint.
pub struct Poller {
    /// Whose poller.
    rule: RuleId,
    /// Where the body comes from.
    fetcher: Fetcher,
    /// Where it goes. The broker, through the one publish every caller
    /// without a session goes through.
    shared: Arc<Shared>,
    /// The topic it is published to.
    topic: String,
    /// The level it is published at.
    qos: QoS,
    /// Whether it is retained.
    retain: bool,
    /// How long between the start of one fetch and the next.
    interval: Duration,
    /// What this rule has been doing. See [`crate::stats`].
    stats: Arc<crate::stats::RuleStats>,
}

impl Poller {
    /// A poller for one rule.
    ///
    /// # Errors
    ///
    /// [`HttpPollError`], naming the part of the configuration that is
    /// wrong.
    pub fn new(
        rule: RuleId,
        client: Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>,
        shared: Arc<Shared>,
        action: &HttpPoll,
    ) -> Result<Self, HttpPollError> {
        Ok(Self {
            rule,
            fetcher: Fetcher::new(client, action)?,
            shared,
            topic: action.topic.clone(),
            qos: action.qos,
            retain: action.retain,
            interval: Duration::from_millis(u64::from(action.interval_ms)),
            stats: Arc::new(crate::stats::RuleStats::new()),
        })
    }

    /// Count what this poller does against `stats`.
    ///
    /// A builder rather than a fifth argument, for the reason
    /// [`crate::http_out::Delivery::with_stats`] gives.
    #[must_use]
    pub fn with_stats(mut self, stats: Arc<crate::stats::RuleStats>) -> Self {
        self.stats = stats;
        self
    }

    /// Where this fetches from.
    pub fn uri(&self) -> &hyper::Uri {
        self.fetcher.uri()
    }

    /// Fetch once, and publish what came back.
    ///
    /// One request, one publication at most. Separate from
    /// [`Self::run`] so that what a fetch does and when a fetch happens
    /// can be asserted apart from each other.
    pub async fn poll(&self) -> Polled {
        let fetched = self.fetcher.fetch().await;
        let Fetched::Body(body) = fetched else {
            return Polled::Skipped(fetched);
        };
        let bytes = body.len();

        match publish(
            &self.shared,
            Publication {
                topic: self.topic.clone(),
                payload: body,
                qos: self.qos,
                retain: self.retain,
                // Section 9.2 gives a polling rule a topic, a level and
                // a retain flag, and no expiry. A reading that is
                // replaced every interval does not need one, and an
                // expiry nobody configured is a message this broker
                // would drop for a reason an operator never asked for.
                message_expiry_interval: None,
                // Section 9.2: every publication a connector originates
                // carries a hop count. This one originates.
                user_properties: hops::originated(),
                origin: self.rule.as_str().to_owned(),
            },
        )
        .await
        {
            Ok(published) => Polled::Published {
                bytes,
                delivered: published.delivered,
            },
            Err(refused @ (PublishRefused::NotATopic(_) | PublishRefused::Reserved)) => {
                // Both are refused by `check` before the broker starts,
                // so reaching one here means a rule got past that —
                // worth saying loudly rather than counting quietly.
                Polled::Refused(refused.to_string())
            }
            Err(refused @ PublishRefused::NotDurable) => Polled::Refused(refused.to_string()),
            // Unreachable while a poller stamps the first hop, which
            // is under every maximum `validate` permits. Answered
            // rather than made a panic, because "unreachable" here
            // depends on a number in a configuration file and this
            // connector's whole discipline is that a fetch that failed
            // has lost nothing.
            Err(refused @ PublishRefused::TooManyHops(_)) => Polled::Refused(refused.to_string()),
        }
    }

    /// Fetch on the interval, for as long as this node owns the rule.
    ///
    /// **Section 9.2 makes a poller a singleton**, so `ownership` is
    /// not an optional refinement: two nodes running one poller is two
    /// requests per interval to somebody else's endpoint and two
    /// publications of the same reading. Today there is one member and
    /// it owns everything; the loop is written over the rule anyway,
    /// for the reason [`crate::singleton`] gives.
    ///
    /// A node that takes over starts the schedule where it took over,
    /// and fetches at once rather than one interval later. It has not
    /// seen the previous owner's phase and cannot inherit it, and the
    /// first thing an operator wants from a node that has just taken a
    /// poller over is the current value.
    pub async fn run(self, mut ownership: Ownership) {
        // Now, so that the node which owns this fetches as soon as it
        // starts rather than one interval later. An operator who has
        // just restarted the broker wants the current reading, not the
        // one after next.
        let mut due = Instant::now();
        loop {
            if !ownership.owns() {
                tracing::info!(
                    rule = self.rule.as_str(),
                    owner = %ownership.owner(),
                    "this poller is a singleton and another member owns it; \
                     nothing is fetched here until that changes"
                );
                while !ownership.owns() {
                    ownership.changed().await;
                }
                tracing::info!(
                    rule = self.rule.as_str(),
                    "this node has taken this poller over and is fetching now"
                );
                due = Instant::now();
                continue;
            }

            tokio::time::sleep_until(due).await;
            if !ownership.owns() {
                // Ownership moved while this was waiting. Nothing is
                // fetched: the member that now owns it is the one that
                // should be asking.
                continue;
            }

            let polled = self.poll().await;
            // **`fired` is the interval coming round**, and the
            // outcome below is a different fact about the same turn:
            // a poller that fetches every second and publishes
            // nothing is the shape an operator needs to see, and one
            // counter fed from both could not show it.
            self.stats.fired();
            match &polled {
                Polled::Published { .. } => self.stats.delivered(),
                Polled::Skipped(_) | Polled::Refused(_) => self.stats.failed(),
            }
            match &polled {
                Polled::Published { bytes, delivered } => tracing::debug!(
                    rule = self.rule.as_str(),
                    topic = %self.topic,
                    bytes,
                    delivered,
                    "a poller published a body"
                ),
                // A warning and not an error: one failed fetch is an
                // ordinary event for a poller, and the next interval
                // happens regardless. An operator watching a topic stop
                // moving needs the reason, which is why the fetch is
                // carried into the line.
                Polled::Skipped(why) => tracing::warn!(
                    rule = self.rule.as_str(),
                    // Scheme and host, and nothing else. This line is
                    // written on every failed poll, and for the
                    // commonest target the rest of the URL is the
                    // credential; see `http_out::redacted_url`.
                    url = http_out::redacted_url(&self.fetcher.uri().to_string()),
                    ?why,
                    "a fetch produced nothing, so this interval is skipped; \
                     the next one happens as usual"
                ),
                Polled::Refused(why) => tracing::error!(
                    rule = self.rule.as_str(),
                    topic = %self.topic,
                    why,
                    "a poller fetched a body the broker would not publish"
                ),
            }

            due = next_due(due, Instant::now(), self.interval);
        }
    }
}

/// When the next fetch is due.
///
/// `due` is when the fetch that has just finished was due, and `now` is
/// the moment it finished. The answer is the first interval boundary
/// **strictly after `now`**, counting from `due`.
///
/// # This is the decision about a fetch slower than the interval
///
/// For a fetch that finished inside its interval the answer is
/// `due + interval`, which is what any spelling of this would give.
///
/// For one that overran, every boundary it covered is **dropped**. A
/// fetch that took three and a half intervals is followed by a wait of
/// half an interval, not by three fetches made back to back. The
/// alternative is what a runtime's interval timer does by default:
/// missed ticks fire in a burst, which against an endpoint that has
/// become slow means a run of requests with no gap between them — the
/// amplification the connector next door has a rule about — and means
/// publishing several readings all taken at one moment.
///
/// Written as a function of three values rather than as a timer
/// setting, so that the rule can be asserted without waiting for a
/// clock. A timing test can say a poller kept going; only this can say
/// what it decided.
fn next_due(due: Instant, now: Instant, interval: Duration) -> Instant {
    // A zero interval is refused by `check` before a poller exists, so
    // this cannot be reached through one. Clamped rather than asserted
    // because the alternative is a division by zero in a broker.
    let interval = interval.max(Duration::from_nanos(1));
    let elapsed = now.saturating_duration_since(due);
    // Integer division, then one more: for `elapsed` inside the first
    // interval this is 1, and for `elapsed` landing exactly on a
    // boundary it is the boundary after it, which is what "strictly
    // after now" means.
    let periods = elapsed.as_nanos() / interval.as_nanos() + 1;
    due + interval.saturating_mul(u32::try_from(periods).unwrap_or(u32::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn action(url: &str) -> HttpPoll {
        HttpPoll {
            url: url.to_owned(),
            topic: "weather/now".to_owned(),
            ..HttpPoll::default()
        }
    }

    // -----------------------------------------------------------
    // A fetch slower than the interval
    // -----------------------------------------------------------

    /// The whole decision, asserted without a clock.
    ///
    /// Each case is a fetch that started when it was due and finished
    /// at `finished`, and the answer is when the next one is due.
    #[test]
    fn a_slow_fetch_loses_the_ticks_it_covered() {
        let interval = Duration::from_millis(100);
        let due = Instant::now();

        let cases: &[(u64, u64, &str)] = &[
            (
                0,
                100,
                "a fetch that took no time at all is followed by one interval",
            ),
            (
                40,
                100,
                "and so is one that finished comfortably inside its interval: the schedule \
                 is fixed to the interval, not to when the last one ended",
            ),
            (99, 100, "including one that only just finished in time"),
            (
                100,
                200,
                "a fetch that lands exactly on the boundary has used that boundary up, so \
                 the next is the one after it; the answer is strictly after now, never now",
            ),
            (
                150,
                200,
                "a fetch half an interval over waits the other half rather than going \
                 again at once",
            ),
            (
                350,
                400,
                "a fetch three and a half intervals long drops the three boundaries it \
                 covered; a burst would fire them back to back against an endpoint that \
                 has just proved it is slow",
            ),
            (
                1_000,
                1_100,
                "and ten intervals drops ten, however long the endpoint has been ill",
            ),
        ];

        for (finished, expected, why) in cases {
            assert_eq!(
                next_due(due, due + Duration::from_millis(*finished), interval),
                due + Duration::from_millis(*expected),
                "a fetch due at 0 and finished at {finished}ms: {why}"
            );
        }
    }

    /// The schedule does not drift, however the fetches fall.
    ///
    /// Asserted separately because the case above fixes `due` at one
    /// point: a rule that answered `now + interval` would pass every
    /// case there whose fetch was instant, and would walk the schedule
    /// forward by one fetch duration per interval for ever.
    #[test]
    fn the_schedule_does_not_drift() {
        let interval = Duration::from_millis(100);
        let start = Instant::now();
        let mut due = start;
        // Ten intervals, each with a fetch taking a different slice of
        // its interval.
        for round in 0..10u64 {
            let took = Duration::from_millis(round * 9 % 100);
            due = next_due(due, due + took, interval);
        }
        assert_eq!(
            due,
            start + interval * 10,
            "ten intervals later is ten intervals later, whatever each fetch cost"
        );
    }

    // -----------------------------------------------------------
    // What a rule may say
    // -----------------------------------------------------------

    /// The endpoint rule is `http_out`'s, and reaches a poller
    /// unchanged.
    ///
    /// Not a test of `check_scheme` — that has its own, beside it — but
    /// of the wiring: a poller that did not go through the shared
    /// function would be a second place for the refusal to be missing.
    #[test]
    fn a_plain_text_url_to_a_real_host_is_refused_here_too() {
        let refused = check(&action("http://example.invalid/now")).expect_err("must be refused");
        assert!(
            matches!(&refused, HttpPollError::Endpoint(why) if why.contains("in the clear")),
            "{refused}"
        );
        assert!(check(&action("https://example.invalid/now")).is_ok());
        assert!(
            check(&action("http://127.0.0.1:8080/now")).is_ok(),
            "and the loopback exception reaches a poller as well"
        );
        assert!(matches!(
            check(&action("not a url at all")),
            Err(HttpPollError::Endpoint(_))
        ));
    }

    #[test]
    fn a_header_is_checked_before_anything_is_fetched() {
        let mut bad = action("https://example.invalid/now");
        bad.headers = vec![("not a name".to_owned(), "v".to_owned())];
        assert!(matches!(check(&bad), Err(HttpPollError::Endpoint(_))));
    }

    /// A poller that would fetch as fast as the endpoint answers is
    /// refused.
    #[test]
    fn an_interval_of_zero_is_refused() {
        let mut none = action("https://example.invalid/now");
        none.interval_ms = 0;
        assert_eq!(check(&none), Err(HttpPollError::NoInterval));
    }

    /// And one that could never read a response.
    ///
    /// Zero is refused rather than read as "no limit", which is the
    /// other thing it could mean and is the one that would let an
    /// endpoint answering for ever grow this task's memory.
    #[test]
    fn a_body_limit_of_zero_is_refused() {
        let mut none = action("https://example.invalid/now");
        none.max_body_bytes = 0;
        assert_eq!(check(&none), Err(HttpPollError::NoBody));
    }

    #[test]
    fn the_topic_must_be_a_topic_and_not_a_reserved_one() {
        let mut wildcard = action("https://example.invalid/now");
        wildcard.topic = "weather/+".to_owned();
        assert!(matches!(
            check(&wildcard),
            Err(HttpPollError::NotATopic { .. })
        ));

        let mut empty = action("https://example.invalid/now");
        empty.topic = String::new();
        assert!(matches!(
            check(&empty),
            Err(HttpPollError::NotATopic { .. })
        ));

        let mut reserved = action("https://example.invalid/now");
        reserved.topic = "$sd/anything".to_owned();
        assert!(matches!(
            check(&reserved),
            Err(HttpPollError::Reserved { .. })
        ));
    }

    /// The topic is checked before the endpoint is.
    ///
    /// Not an ordering anybody depends on, but it is asserted because
    /// the two halves are in one function and a reader should not have
    /// to guess which refusal an operator sees when both are wrong.
    #[test]
    fn a_rule_wrong_in_two_ways_names_the_topic_first() {
        let mut both = action("http://example.invalid/now");
        both.topic = "weather/#".to_owned();
        assert!(matches!(check(&both), Err(HttpPollError::NotATopic { .. })));
    }
}
