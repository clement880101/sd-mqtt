//! Accepting a post and publishing its body.
//!
//! Section 9.2's `http_in`: `POST /ingest/<rule-id>`, guarded by that
//! rule's bearer token, publishing the request body to a configured
//! topic at a configured level and retain setting.
//!
//! # The prefix is the whole safety of the feature
//!
//! The topic may instead come from a named header or query parameter.
//! Section 9.2 allows that only "restricted by a prefix so a caller
//! cannot publish anywhere", and that clause is not a refinement — it is
//! the difference between an ingest endpoint and a way for whoever holds
//! one rule's token to publish to **any** topic on the broker, the
//! reserved `$` namespace included.
//!
//! Two things enforce it, and they are in different registers on
//! purpose.
//!
//! The first is the shape: [`TopicFromRequest`] holds the source and
//! the prefix together, so a rule that reads a topic from a request and
//! has no prefix is not a check somebody can forget — it is a value
//! nobody can construct.
//!
//! The second is [`is_under`], which compares **by topic level**. The
//! mutation that looks right and is not is `topic.starts_with(prefix)`:
//! it admits `a/bc` under a prefix of `a/b`, which is a different
//! subtree belonging to somebody else. Every spelling of that escape
//! anybody could think of is enumerated in
//! [`tests::a_topic_that_escapes_its_prefix_is_refused_however_it_is_spelled`],
//! which is the test this module is written around.
//!
//! This project refuses rather than reinterprets when a value cannot
//! mean what it says, and a topic that escapes its prefix is exactly
//! that: it is refused, not trimmed, not rewritten under the prefix, and
//! not published to the configured default instead.
//!
//! # There is no second publish here
//!
//! The body goes to [`sdmqtt_server::publish`], which is the same call a
//! connection's publish and the admin API's endpoint make. A second
//! publish would be a second set of rules about retention, expiry and
//! matching, and the two would drift the first time one was corrected.
//!
//! An at-least-once ingest therefore waits for durability before it
//! answers, exactly as the admin endpoint does. The response is the
//! promise.
//!
//! # The body limit is the broker's maximum packet size
//!
//! If the two disagree, ingest accepts a body the broker could never
//! carry in a PUBLISH and the caller is told the post succeeded. So the
//! limit is derived from `broker.limits.max_packet_size` and from
//! nothing else; see [`largest_body`].
//!
//! # What this listener does not check
//!
//! The name it was reached under. That is a written decision, not an
//! omission, and it is written where the check is: see
//! `sdmqtt_admin::host`. The short version is that every route here is
//! guarded by its own rule's token, so a rebound page reaching this
//! listener has exactly the access anybody on the network has.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Extension, Path, Query};
use axum::http::header::{HeaderMap, HeaderName, AUTHORIZATION};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use mqtt_codec::QoS;
use sdmqtt_server::publish::{publish, Publication, PublishRefused};
use sdmqtt_server::shared::Shared;
use serde::Serialize;
use storage::record::Secret;

use crate::rule::{Action, HttpIn, Rule, RuleId, TopicSource};

/// The path an ingest rule is reached at, with the rule identifier as
/// its last level.
///
/// One route for every rule rather than one route per rule, because a
/// router that grew a route per rule would have to be rebuilt whenever
/// a rule was added, and milestone 5 adds them at runtime.
pub const INGEST_PATH: &str = "/ingest/{rule}";

/// What a PUBLISH costs besides its payload, in bytes.
///
/// Every term is a field of the packet the broker would build, counted
/// at its largest:
///
/// - **1** for the fixed header's first byte.
/// - **4** for the remaining-length varint, which is at most four bytes.
/// - **2** for the topic's length prefix, plus the topic itself, which
///   is added by [`largest_body`] because it is not a constant.
/// - **2** for the packet identifier. Counted whatever the rule's level
///   is, so that re-levelling a rule does not change what it accepts.
/// - **1** for the property-length varint of the empty property block
///   an ingest publication carries.
///
/// **A subscriber's own subscription identifier is not counted**, and
/// cannot be: it is added per subscriber when the message is delivered
/// and is not known here. What a particular client will accept is
/// between the broker and that client, and the write path's own size
/// limit already enforces it. What this bounds is the broker's figure.
pub const PUBLISH_OVERHEAD_BYTES: usize = 1 + 4 + 2 + 2 + 1;

/// The largest body that can be published to `topic` within
/// `max_packet_size`.
///
/// Saturating, so a broker configured with a maximum packet size
/// smaller than the overhead of the topic it was asked about accepts an
/// empty body and nothing else, rather than wrapping to something
/// enormous.
pub fn largest_body(max_packet_size: u32, topic: &str) -> usize {
    (max_packet_size as usize).saturating_sub(PUBLISH_OVERHEAD_BYTES + topic.len())
}

/// Whether `topic` is at or under `prefix`, **by topic level**.
///
/// The one function the safety of this feature rests on.
///
/// A topic is under a prefix when the prefix's levels are the first of
/// the topic's levels: `a/b` is at `a/b`, `a/b/c` is under it, and
/// `a/bc` is **not**, because `bc` is a different level from `b` however
/// much of it starts the same way.
///
/// Written as a walk over levels rather than as a comparison of
/// strings, because the string forms that happen to be right are one
/// character away from the ones that are not. `topic.starts_with(prefix)`
/// admits `a/bc`. `topic.starts_with(&format!("{prefix}/"))` refuses
/// `a/b` itself, which is inside its own prefix. This asks the question
/// the design asks.
///
/// **Nothing is normalised on the way in.** There is no case folding, no
/// Unicode normalisation, no percent-decoding and no collapsing of
/// repeated separators, because MQTT does none of those: a topic is a
/// sequence of bytes split at `/` and compared exactly. Anything this
/// function did that the broker's matcher does not would be a topic
/// accepted here and delivered somewhere else.
pub fn is_under(prefix: &str, topic: &str) -> bool {
    let mut levels = topic.split('/');
    for level in prefix.split('/') {
        match levels.next() {
            Some(found) if found == level => {}
            _ => return false,
        }
    }
    true
}

/// What can be wrong with an `http_in` rule.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HttpInError {
    /// The configured topic is not one a message can be published to.
    #[error("http_in.topic {topic:?} is not a topic a message can be published to: {why}")]
    NotATopic {
        /// What was configured.
        topic: String,
        /// Why it is not a topic.
        why: String,
    },
    /// The configured topic, or a prefix, is in the reserved namespace.
    #[error(
        "{key} {value:?} is in the dollar-prefixed namespace the broker reserves for itself, \
         which is not publishable"
    )]
    Reserved {
        /// Which key.
        key: &'static str,
        /// What was configured.
        value: String,
    },
    /// A topic source was configured with no prefix to bound it.
    #[error(
        "http_in.{key} lets a caller choose the topic, so http_in.topic_prefix is required: \
         without it whoever holds this rule's token can publish anywhere on the broker"
    )]
    NoPrefix {
        /// Which source was configured.
        key: &'static str,
    },
    /// A prefix was configured with no source to bound.
    ///
    /// Refused rather than ignored, because an operator who wrote one
    /// believes callers may choose a topic under it, and they may not.
    #[error(
        "http_in.topic_prefix bounds a topic a caller chose, and this rule lets no caller \
         choose one; set http_in.topic_header or http_in.topic_query, or remove the prefix"
    )]
    NoSource,
    /// Both a header and a query parameter were configured.
    #[error(
        "http_in.topic_header and http_in.topic_query are two places to say one thing and \
         nothing says which wins; configure one of them"
    )]
    TwoSources,
    /// The prefix is not usable as one.
    #[error("http_in.topic_prefix {prefix:?} is not usable as a prefix: {why}")]
    BadPrefix {
        /// What was configured.
        prefix: String,
        /// Why it is not usable.
        why: String,
    },
    /// The header the topic is read from is not a header name.
    #[error("http_in.topic_header {name:?} is not a header name: {why}")]
    BadHeader {
        /// What was configured.
        name: String,
        /// Why it is not a header name.
        why: String,
    },
    /// The query parameter the topic is read from has no name.
    #[error("http_in.topic_query must name a parameter")]
    BadQuery,
}

/// Check an ingest rule without binding anything.
///
/// For whatever reads an operator's configuration: a rule that cannot
/// accept a post should stop the broker starting, said once with the
/// rule named, rather than becoming an endpoint that refuses everything
/// or — far worse — one that accepts anything.
///
/// The token is **not** checked here. It is checked by [`Rule::new`],
/// which is where every rule in this process is accepted, so that a
/// rule read back from the store is refused by the same rule as one
/// read out of a file.
///
/// # Errors
///
/// [`HttpInError`], naming the key that is wrong.
pub fn check(action: &HttpIn) -> Result<(), HttpInError> {
    // The broker's own limits are not reachable from here, and the
    // default ones are what every topic in a configuration is checked
    // against elsewhere in this crate.
    let limits = mqtt_codec::Limits::default();
    mqtt_codec::topic::validate_name(&action.topic, &limits).map_err(|error| {
        HttpInError::NotATopic {
            topic: action.topic.clone(),
            why: error.to_string(),
        }
    })?;
    if action.topic.starts_with('$') {
        return Err(HttpInError::Reserved {
            key: "http_in.topic",
            value: action.topic.clone(),
        });
    }

    let Some(from) = &action.topic_from else {
        return Ok(());
    };

    match &from.source {
        TopicSource::Header(name) => {
            HeaderName::from_bytes(name.as_bytes()).map_err(|error| HttpInError::BadHeader {
                name: name.clone(),
                why: error.to_string(),
            })?;
        }
        TopicSource::Query(name) => {
            if name.is_empty() {
                return Err(HttpInError::BadQuery);
            }
        }
    }

    // A prefix is a topic name with no wildcard in it, which is what
    // `validate_name` says, and three more things.
    if from.prefix.is_empty() {
        return Err(HttpInError::BadPrefix {
            prefix: from.prefix.clone(),
            why: "an empty prefix bounds nothing, so it would let a caller publish anywhere"
                .to_owned(),
        });
    }
    mqtt_codec::topic::validate_name(&from.prefix, &limits).map_err(|error| {
        HttpInError::BadPrefix {
            prefix: from.prefix.clone(),
            why: error.to_string(),
        }
    })?;
    if from.prefix.starts_with('$') {
        return Err(HttpInError::Reserved {
            key: "http_in.topic_prefix",
            value: from.prefix.clone(),
        });
    }
    // `a/b/` is three levels — `a`, `b` and the empty one — so a caller
    // asking for `a/b/c` would be refused by a prefix an operator wrote
    // meaning exactly that. The separator is what separates a prefix
    // from what follows it and does not belong in the prefix itself.
    if from.prefix.ends_with('/') {
        return Err(HttpInError::BadPrefix {
            prefix: from.prefix.clone(),
            why: "a prefix is written without a trailing separator: with one it is a prefix \
                  of three levels ending in an empty one, and no caller could satisfy it"
                .to_owned(),
        });
    }
    Ok(())
}

// ---------------------------------------------------------------
// The routes
// ---------------------------------------------------------------

/// The ingest rules this node serves, by identifier.
///
/// Built once at startup from the rules the node holds. Milestone 5
/// replicates rules and will want this to change while the process is
/// running; what it holds would then go behind the lock the rest of the
/// replicated metadata is behind, and nothing about the route changes.
#[derive(Debug, Default)]
pub struct Ingest {
    /// The rules with a route, by identifier.
    rules: BTreeMap<String, HttpIn>,
}

impl Ingest {
    /// Nothing served.
    pub fn new() -> Self {
        Self::default()
    }

    /// The ingest rules among `rules`, each with a route.
    ///
    /// A rule whose action is not `http_in` gets no route, which is why
    /// `POST /ingest/<some-http_out-rule>` answers as though the path
    /// does not exist.
    pub fn from_rules<'a>(rules: impl IntoIterator<Item = &'a Rule>) -> Self {
        let mut ingest = Self::new();
        for rule in rules {
            if let Action::HttpIn(action) = &rule.action {
                ingest.register(&rule.id, action.clone());
            }
        }
        ingest
    }

    /// Give `id` a route, unless it has no token.
    ///
    /// **The second half of the refusal.** [`Rule::new`] refuses a rule
    /// with no token, so a rule that reaches here should always have
    /// one; this is what makes that a fact rather than a habit. A
    /// tokenless rule gets **no route at all** rather than an open one,
    /// so the path answers as though it does not exist — which is the
    /// same answer an identifier nobody configured gets, and therefore
    /// tells a caller nothing.
    pub fn register(&mut self, id: &RuleId, action: HttpIn) {
        if action.token.is_empty() {
            tracing::error!(
                rule = id.as_str(),
                "an ingest rule reached route registration with no token and has been given \
                 no route; the endpoint publishes, so an unguarded one is a hole"
            );
            return;
        }
        self.rules.insert(id.as_str().to_owned(), action);
    }

    /// The rule served at `id`, if one is.
    pub fn rule(&self, id: &str) -> Option<&HttpIn> {
        self.rules.get(id)
    }

    /// How many rules have a route.
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// Whether none has.
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

/// Everything a request reaches.
#[derive(Clone)]
struct Endpoint {
    /// The rules with a route.
    ingest: Arc<Ingest>,
    /// The broker to publish into.
    shared: Arc<Shared>,
    /// The broker's maximum packet size, read once when the routes were
    /// built, which is the only time it can change.
    max_packet_size: u32,
}

/// The ingest route set.
///
/// Generic in the state so that the same routes can be merged into a
/// router that has one — section 9.2 permits serving them on the admin
/// listener, chosen explicitly — and served on one that does not, which
/// is the default. Nothing here reads axum state: what a handler needs
/// arrives as an [`Extension`], which no router's state type can
/// disagree about.
///
/// The body limit is applied here rather than by whoever serves this,
/// so that a route set carries its own bound wherever it is mounted.
pub fn routes<S>(shared: Arc<Shared>, ingest: Arc<Ingest>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let max_packet_size = shared.with_broker(|broker| broker.config().limits.max_packet_size);
    Router::new()
        .route(INGEST_PATH, post(ingest_one))
        // The coarse bound, which is what stops a body being buffered
        // before anything has looked at it. The exact one is in the
        // handler, where the topic is known and the overhead can be
        // counted; both numbers come from this one field, so they
        // cannot disagree about what the broker will carry.
        .layer(DefaultBodyLimit::max(max_packet_size as usize))
        .layer(Extension(Endpoint {
            ingest,
            shared,
            max_packet_size,
        }))
}

/// The error body, in the shape section 13 gives every failure of this
/// project's HTTP surfaces.
///
/// The shape is `sdmqtt_admin::error`'s and is written again here
/// because this crate cannot depend on that one — a connector that
/// pulled in the administrative interface would be the layering
/// backwards. That the two agree is asserted from `sdmqtt`, which can
/// see both.
#[derive(Debug, Serialize)]
struct ErrorBody {
    /// What went wrong, for a machine. From the same closed set.
    code: &'static str,
    /// What went wrong, for a person. Never matched on.
    message: String,
}

/// A failure, nested under `error` so that `"error" in body` is a
/// complete test of whether a response is one.
#[derive(Debug, Serialize)]
struct ApiError {
    /// The failure.
    error: ErrorBody,
}

impl ApiError {
    /// A failure with a status, a code and a sentence, as a response.
    ///
    /// Not named `new`: it does not return one of these, it returns
    /// what one becomes on the wire. Nothing else ever holds one.
    fn refusal(status: StatusCode, code: &'static str, message: impl Into<String>) -> Response {
        (
            status,
            Json(Self {
                error: ErrorBody {
                    code,
                    message: message.into(),
                },
            }),
        )
            .into_response()
    }

    /// The answer to a rule that has no route.
    fn no_such_rule() -> Response {
        not_found_body()
    }
}

/// The one shape, for a path this listener does not serve.
fn not_found_body() -> Response {
    ApiError::refusal(
        StatusCode::NOT_FOUND,
        "not_found",
        "no such resource on this listener",
    )
}

/// The fallback for a path an ingest listener does not serve.
///
/// Registered by whoever serves [`routes`] on a listener of its own,
/// rather than left to axum's default, which is an empty body with no
/// shape at all: "the path you asked for is not served" is a failure a
/// client has to be able to tell from a proxy that swallowed the
/// request.
pub async fn not_found() -> Response {
    not_found_body()
}

/// The fallback for a method a served path does not accept.
pub async fn method_not_allowed() -> Response {
    ApiError::refusal(
        StatusCode::METHOD_NOT_ALLOWED,
        "method_not_allowed",
        "that resource does not accept this method",
    )
}

/// What an accepted post answers.
#[derive(Debug, Serialize)]
struct Accepted {
    /// How many subscribers it reached. Zero is an ordinary answer.
    delivered: usize,
    /// Whether this answer waited for the store. See
    /// [`sdmqtt_server::publish`]: the response is the promise, so at
    /// an acknowledged level it is not sent until the message is
    /// durable.
    durable: bool,
}

/// Compare a presented secret with the configured one without stopping
/// at the first difference.
///
/// The same argument the admin listener's token comparison makes, and
/// it is stronger here: an ingest listener is reached from off this
/// host by design, so every comparison this function ever makes against
/// a real secret is against a caller on the network. Six lines rather
/// than a crate, and no `return` inside the loop for a reviewer to
/// miss.
///
/// **No test can tell this from an early-returning `==`.** The two
/// agree on every answer, and what separates them is how long a wrong
/// answer takes — nanoseconds, flaky on a loaded machine and green on a
/// fast one with the bug present. What guards it is this comment and
/// the shape of the function.
fn secret_eq(presented: &str, configured: &Secret) -> bool {
    let (presented, configured) = (presented.as_bytes(), configured.expose().as_bytes());
    // Folded in rather than returned on, so that a wrong length costs
    // the same as a wrong byte.
    let mut difference = presented.len() ^ configured.len();
    for (index, byte) in presented.iter().enumerate() {
        // The configured token may be shorter, and an index past its
        // end would panic. Wrapping keeps the work per presented byte
        // the same whichever is longer; the length term above is what
        // makes the answer right.
        let against = if configured.is_empty() {
            0
        } else {
            configured[index % configured.len()]
        };
        difference |= usize::from(byte ^ against);
    }
    difference == 0
}

/// Why a request's chosen topic was not usable.
#[derive(Debug, PartialEq, Eq)]
enum TopicRefused {
    /// The request named the source more than once.
    Repeated,
    /// The header's value is not text.
    NotText,
    /// The topic escapes the rule's prefix.
    OutsidePrefix,
}

/// The topic this request publishes to.
///
/// The configured one unless the rule lets the request choose and the
/// request did. A rule that lets a request choose and a request that
/// does not is the configured topic, not a refusal: the source is how a
/// caller *may* say where it is going, not how it must.
fn topic_for(
    action: &HttpIn,
    headers: &HeaderMap,
    query: &[(String, String)],
) -> Result<String, TopicRefused> {
    let Some(from) = &action.topic_from else {
        return Ok(action.topic.clone());
    };
    let chosen = match &from.source {
        TopicSource::Header(name) => {
            let mut values = headers.get_all(name).iter();
            let Some(value) = values.next() else {
                return Ok(action.topic.clone());
            };
            // Two of the same header is two answers to one question,
            // and nothing says which wins.
            if values.next().is_some() {
                return Err(TopicRefused::Repeated);
            }
            value
                .to_str()
                .map_err(|_| TopicRefused::NotText)?
                .to_owned()
        }
        TopicSource::Query(name) => {
            let mut found = query.iter().filter(|(key, _)| key == name).map(|(_, v)| v);
            let Some(value) = found.next() else {
                return Ok(action.topic.clone());
            };
            if found.next().is_some() {
                return Err(TopicRefused::Repeated);
            }
            value.clone()
        }
    };
    // **The check.** By topic level; see [`is_under`].
    if !is_under(&from.prefix, &chosen) {
        return Err(TopicRefused::OutsidePrefix);
    }
    Ok(chosen)
}

/// `POST /ingest/<rule-id>`.
///
/// The order of what it does is part of what it promises. An identifier
/// with no route is answered as a path that does not exist **before**
/// the token is looked at, so that a rule with no route and a rule that
/// was never configured are indistinguishable from outside. The cost is
/// that a caller can learn which identifiers have a route; the benefit
/// is that a rule an operator disabled by removing its token does not
/// advertise itself as a rule whose token is worth guessing. Neither
/// tells anybody anything they can publish with.
async fn ingest_one(
    Extension(endpoint): Extension<Endpoint>,
    Path(rule): Path<String>,
    headers: HeaderMap,
    Query(query): Query<Vec<(String, String)>>,
    body: Bytes,
) -> Response {
    let Some(action) = endpoint.ingest.rule(&rule) else {
        return ApiError::no_such_rule();
    };

    let presented = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or("");
    if !secret_eq(presented, &action.token) {
        // The same sentence whether the header was missing, the scheme
        // was wrong or the secret was: which of the three it was is a
        // fact about this rule's configuration, and the caller has not
        // earned it.
        return ApiError::refusal(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "this endpoint needs this rule's token, presented as \
             `Authorization: Bearer <token>`",
        );
    }

    let topic = match topic_for(action, &headers, &query) {
        Ok(topic) => topic,
        Err(TopicRefused::Repeated) => {
            return ApiError::refusal(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "the topic was given more than once, and nothing says which one wins",
            )
        }
        Err(TopicRefused::NotText) => {
            return ApiError::refusal(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "the header the topic is read from is not text",
            )
        }
        Err(TopicRefused::OutsidePrefix) => {
            let prefix = action
                .topic_from
                .as_ref()
                .map_or("", |from| from.prefix.as_str());
            return ApiError::refusal(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                format!(
                    "that topic is outside this rule's prefix {prefix:?}. A topic is inside \
                     it when the prefix's levels are the first of the topic's levels, so \
                     {prefix:?} and anything below it are accepted and nothing else is."
                ),
            );
        }
    };

    // The exact limit, now that the topic is known. The layer above has
    // already refused anything larger than the broker's whole packet;
    // this is the part of it the payload may have.
    let largest = largest_body(endpoint.max_packet_size, &topic);
    if body.len() > largest {
        return ApiError::refusal(
            StatusCode::PAYLOAD_TOO_LARGE,
            "invalid_request",
            format!(
                "the body is {} bytes and the largest this broker can publish to a topic of \
                 {} bytes is {largest}; the limit is broker.max_packet_size, which is {}",
                body.len(),
                topic.len(),
                endpoint.max_packet_size
            ),
        );
    }

    match publish(
        &endpoint.shared,
        Publication {
            topic,
            payload: body,
            qos: action.qos,
            retain: action.retain,
            // Section 9.2 gives an ingest rule a topic, a level and a
            // retain flag, and no expiry. A message with none is
            // deliverable until it is delivered, which is what every
            // other publication with no expiry does.
            message_expiry_interval: None,
        },
    )
    .await
    {
        Ok(published) => Json(Accepted {
            delivered: published.delivered,
            durable: published.durable,
        })
        .into_response(),
        Err(refused @ (PublishRefused::NotATopic(_) | PublishRefused::Reserved)) => {
            ApiError::refusal(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                refused.to_string(),
            )
        }
        Err(refused @ PublishRefused::NotDurable) => ApiError::refusal(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            refused.to_string(),
        ),
    }
}

/// The level, as an operator's configuration numbers it.
///
/// Here rather than in the binary's configuration module because it is
/// this crate's vocabulary, and because `http_in.qos` and the MQTT
/// packet field must be numbered by one rule.
pub fn qos_of(number: u8) -> Option<QoS> {
    match number {
        0 => Some(QoS::AtMostOnce),
        1 => Some(QoS::AtLeastOnce),
        2 => Some(QoS::ExactlyOnce),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rule::TopicFromRequest;

    /// An ingest rule that lets a request choose a topic under
    /// `prefix`.
    fn bounded_by(prefix: &str) -> HttpIn {
        HttpIn {
            token: Secret::new("a-secret"),
            topic: "ingest/default".to_owned(),
            topic_from: Some(TopicFromRequest {
                source: TopicSource::Header("x-topic".to_owned()),
                prefix: prefix.to_owned(),
            }),
            qos: QoS::AtMostOnce,
            retain: false,
        }
    }

    // -----------------------------------------------------------
    // The prefix, which is the whole safety of the feature
    // -----------------------------------------------------------

    /// Every spelling of an escape anybody could think of, refused.
    ///
    /// **This is the test this module exists for.** Without the prefix
    /// an ingest rule is a way for whoever holds one rule's token to
    /// publish to any topic on the broker, and the mutation that looks
    /// right and is not is `topic.starts_with(prefix)`, which admits
    /// `a/bc` under a prefix of `a/b`.
    ///
    /// The accepted half is as load bearing as the refused one: a check
    /// that refused everything would pass a table of refusals, and an
    /// ingest rule that refuses every topic is a rule that delivers
    /// nothing. Convention 5.
    ///
    /// # What each spelling is, and why it lands where it does
    ///
    /// Nothing here is normalised, because MQTT normalises nothing: a
    /// topic is bytes split at `/` and compared exactly. So the answer
    /// to every one of these is decided by the levels alone.
    #[test]
    fn a_topic_that_escapes_its_prefix_is_refused_however_it_is_spelled() {
        let inside: &[(&str, &str, &str)] = &[
            ("a/b", "a/b", "a topic is inside its own prefix"),
            ("a/b", "a/b/c", "and so is one level below it"),
            ("a/b", "a/b/c/d/e", "and any number of levels below it"),
            (
                "a/b",
                "a/b/",
                "an empty last level is a legal topic and is below the prefix",
            ),
            (
                "a/b",
                "a/b/../c",
                "MQTT has no path semantics, so `..` is an ordinary level and this is \
                 genuinely inside; refusing it would be inventing a rule the matcher \
                 does not have",
            ),
            (
                "a/b",
                "a/b/%2F",
                "and a percent sequence nothing decodes is an ordinary level too",
            ),
            ("a", "a/b", "a one-level prefix bounds the subtree below it"),
            ("a", "a", "and itself"),
            (
                "site/one",
                "site/one/sensor/1",
                "a prefix of two levels works the same way",
            ),
        ];
        for (prefix, topic, why) in inside {
            assert!(
                is_under(prefix, topic),
                "{topic:?} should be inside {prefix:?}: {why}"
            );
        }

        let outside: &[(&str, &str, &str)] = &[
            (
                "a/b",
                "a/bc",
                "**the mutation's signature.** `a/bc` starts with the string `a/b` and \
                 is a different level, so it is a different subtree belonging to \
                 somebody else",
            ),
            ("a/b", "a/bc/d", "and so is everything below it"),
            (
                "a/b",
                "a/bb",
                "one character longer is still a different level",
            ),
            (
                "a/b",
                "a/b c",
                "and so is one with a space in it, which a header value may carry",
            ),
            ("a", "ab", "the same escape against a one-level prefix"),
            ("site/one", "site/onex", "and against a two-level one"),
            (
                "a/b",
                "a",
                "a topic shorter than its prefix is not under it",
            ),
            ("a/b", "a/", "including one whose second level is empty"),
            (
                "a/b",
                "",
                "and the empty topic, which is not a topic at all",
            ),
            (
                "a/b",
                "/a/b",
                "a leading separator is an empty first level, so this is three levels \
                 and the first is not `a`",
            ),
            (
                "a/b",
                "a//b",
                "an empty level in the middle is a level, not whitespace to collapse",
            ),
            (
                "a/b",
                "x/a/b",
                "the prefix appearing later is not the prefix",
            ),
            ("a/b", "b/a", "nor are its levels in another order"),
            (
                "a/b",
                "A/B",
                "MQTT topics are case sensitive, so nothing is folded",
            ),
            (
                "a/b",
                "a/b\u{ff0f}c",
                "a fullwidth solidus is not a separator: `b\u{ff0f}c` is one level, and \
                 anything that normalised it would accept a topic the broker's matcher \
                 would deliver elsewhere",
            ),
            (
                "a/b",
                "a/b%2Fc",
                "a percent-encoded separator that nothing decoded is a literal level; \
                 what a query parameter decodes to is decided before this function \
                 sees it, and is then an ordinary topic",
            ),
            (
                "a/b",
                "a/../a/b",
                "there is no walking upward: the second level is `..`",
            ),
            (
                "a/b",
                " a/b",
                "nothing is trimmed, so a leading space is part of the first level",
            ),
            ("a/b", "a/b ", "and a trailing one is part of the last"),
            (
                "a/b",
                "a/b\u{0}",
                "a NUL is a character like any other here, and the topic validator is \
                 what refuses it",
            ),
            (
                "a/b",
                "$sd/rules/x/dlq",
                "the reserved namespace is outside every prefix an operator may write, \
                 because a prefix in it is refused",
            ),
        ];
        for (prefix, topic, why) in outside {
            assert!(
                !is_under(prefix, topic),
                "{topic:?} must not be inside {prefix:?}: {why}"
            );
        }
    }

    /// A wildcard below the prefix is inside it and is still not a
    /// topic.
    ///
    /// Said on its own because the two checks are separate and both are
    /// needed: `a/b/#` **is** under `a/b` by level, so the prefix does
    /// not refuse it, and what does is the publish path's own
    /// `validate_name`. A reader who assumed the prefix covered this
    /// would be wrong.
    #[test]
    fn a_wildcard_below_the_prefix_is_inside_it_and_is_refused_elsewhere() {
        assert!(is_under("a/b", "a/b/#"));
        assert!(is_under("a/b", "a/b/+"));
        let limits = mqtt_codec::Limits::default();
        assert!(mqtt_codec::topic::validate_name("a/b/#", &limits).is_err());
        assert!(mqtt_codec::topic::validate_name("a/b/+", &limits).is_err());
        assert!(mqtt_codec::topic::validate_name("a/b\u{0}", &limits).is_err());
    }

    // -----------------------------------------------------------
    // What a rule may say
    // -----------------------------------------------------------

    #[test]
    fn a_source_needs_a_prefix_that_is_usable_as_one() {
        for (prefix, why) in [
            ("", "an empty prefix bounds nothing"),
            (
                "a/#",
                "a prefix is a topic name, so no multi-level wildcard",
            ),
            ("a/+/b", "and no single-level one"),
            ("$sd/rules", "and not the reserved namespace"),
            (
                "a/b/",
                "and not a trailing separator, which makes a third, empty level no \
                 caller could satisfy",
            ),
        ] {
            assert!(
                check(&bounded_by(prefix)).is_err(),
                "{prefix:?} should not be usable as a prefix: {why}"
            );
        }
        check(&bounded_by("a/b")).expect("an ordinary prefix is usable");
        check(&bounded_by("a")).expect("and so is a one-level one");
    }

    #[test]
    fn a_rule_that_chooses_nothing_needs_no_prefix() {
        let action = HttpIn {
            token: Secret::new("a-secret"),
            topic: "ingest/one".to_owned(),
            topic_from: None,
            qos: QoS::AtLeastOnce,
            retain: true,
        };
        check(&action).expect("a rule with one fixed topic is complete");
    }

    #[test]
    fn the_configured_topic_must_be_a_topic_and_not_a_reserved_one() {
        let mut action = bounded_by("a/b");
        action.topic = "a/+".to_owned();
        assert!(matches!(check(&action), Err(HttpInError::NotATopic { .. })));

        let mut action = bounded_by("a/b");
        action.topic = "$sd/anything".to_owned();
        assert!(matches!(
            check(&action),
            Err(HttpInError::Reserved {
                key: "http_in.topic",
                ..
            })
        ));
    }

    #[test]
    fn a_header_the_topic_is_read_from_must_be_a_header_name() {
        let mut action = bounded_by("a/b");
        action.topic_from = Some(TopicFromRequest {
            source: TopicSource::Header("not a header name".to_owned()),
            prefix: "a/b".to_owned(),
        });
        assert!(matches!(check(&action), Err(HttpInError::BadHeader { .. })));

        action.topic_from = Some(TopicFromRequest {
            source: TopicSource::Query(String::new()),
            prefix: "a/b".to_owned(),
        });
        assert_eq!(check(&action), Err(HttpInError::BadQuery));
    }

    // -----------------------------------------------------------
    // The body limit
    // -----------------------------------------------------------

    /// The limit is the broker's number minus what else the packet
    /// carries, and it moves with the topic.
    #[test]
    fn the_largest_body_is_the_packet_size_less_what_else_a_publish_carries() {
        assert_eq!(
            largest_body(1_000, "a/b"),
            1_000 - PUBLISH_OVERHEAD_BYTES - 3
        );
        assert!(
            largest_body(1_000, "a/much/longer/topic") < largest_body(1_000, "a/b"),
            "a longer topic leaves less room for the body"
        );
        // A broker whose maximum packet size is smaller than the
        // overhead accepts an empty body and nothing else, rather than
        // wrapping to something enormous.
        assert_eq!(largest_body(2, "a/b"), 0);
    }

    // -----------------------------------------------------------
    // The registry
    // -----------------------------------------------------------

    /// A tokenless rule gets no route, not an open one.
    ///
    /// **Reached through [`Ingest::register`] rather than through a
    /// rule**, because `Rule::new` refuses a tokenless rule and there
    /// is no way to build one to hand to `Ingest::from_rules`. That is
    /// the point of having the check twice: this is the one that can
    /// still be exercised, and the one that holds if the other is ever
    /// weakened.
    #[test]
    fn a_rule_with_no_token_is_given_no_route() {
        let mut ingest = Ingest::new();
        let id = RuleId::new("gateway").expect("an identifier");
        let mut action = bounded_by("a/b");
        action.token = Secret::new(String::new());
        ingest.register(&id, action);
        assert!(
            ingest.rule("gateway").is_none(),
            "a tokenless rule must have no route at all rather than an open one"
        );
        assert!(ingest.is_empty());
    }

    #[test]
    fn a_rule_that_is_not_an_ingest_rule_gets_no_route() {
        let out = Rule::new(
            RuleId::new("telemetry").expect("an identifier"),
            "Telemetry out",
            Some(crate::rule::Trigger::new("a/#").expect("a filter")),
            Action::HttpOut(crate::rule::HttpOut {
                url: "https://example.invalid/hook".to_owned(),
                ..crate::rule::HttpOut::default()
            }),
        )
        .expect("a named rule");
        let into = Rule::new(
            RuleId::new("gateway").expect("an identifier"),
            "Gateway in",
            None,
            Action::HttpIn(bounded_by("a/b")),
        )
        .expect("a named rule");

        let ingest = Ingest::from_rules([&out, &into]);
        assert_eq!(ingest.len(), 1);
        assert!(ingest.rule("gateway").is_some());
        assert!(
            ingest.rule("telemetry").is_none(),
            "a rule that does not accept posts has no ingest route"
        );
    }
}
