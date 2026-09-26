//! A rule as something outside this process spells it.
//!
//! Section 9.1's rule, flattened by one level and written in the
//! vocabulary an operator uses rather than the one [`crate::rule`]
//! holds: a trigger's three parts sit beside the name, an action is a
//! table named after what it does, and a level is the number MQTT
//! writes.
//!
//! # Why this is here and not in the binary
//!
//! It was in the binary, as the configuration file's `[[rules]]`
//! table, for as long as a configuration file was the only way to
//! write a rule. Section 11.1 makes the admin API a second way, and
//! the admin crate cannot see the binary.
//!
//! Two shapes would have been the alternative, and the reason against
//! it is the one that already put publishing behind one shared call:
//! the refusals below have good arguments behind them — an ingest
//! action with no token is a hole, a bridge whose filters intersect is
//! a storm, a trigger on an action that does not fire on messages is
//! an operator believing something untrue — and a second description
//! of them is a second set that drifts the first time one is
//! corrected. So there is one [`RuleSpec`], one [`RuleSpec::to_rule`],
//! and a file and an API that both go through it.
//!
//! **serde is what makes that possible across two formats.** The same
//! derive reads TOML tables and JSON objects, so `filter = "a/+/b"`
//! and `"filter": "a/+/b"` are one field with one meaning, and the key
//! an error names is a key both callers recognise.
//!
//! # Every refusal names a field
//!
//! [`SpecError`] carries a `field` as well as a sentence, because the
//! two callers need different things from it: a configuration file's
//! reader prints the sentence, and an HTTP API marks the input that
//! was wrong so a form can point at it. The name comes from the error
//! that was raised — each of the rules crate's own error types answers
//! `field()` — rather than from a table at the call site, which is a
//! table that drifts the first time a variant is added.
//!
//! # Secrets go in and do not come back out
//!
//! `http_in.token` and `mqtt_bridge.password` are [`Secret`]s, which
//! do not serialise. Section 12.3 holds them in a wrapper that refuses
//! to print itself, and a rule listing that handed a bearer token to
//! every reader of an unauthenticated resource would be that wrapper
//! working exactly as far as the first `serde` derive.
//!
//! **`http_out.headers` and `http_poll.headers` are the third
//! secret**, and they are not a [`Secret`] because a header map is a
//! map: the configuration file's own documentation says a rule's
//! headers are usually somebody's bearer token, and a listing that
//! handed those out would be the same hole with none of the type
//! system in the way. So [`RuleSpec::of`] writes the header **names**
//! and empty values. A form can then say which headers a rule sends
//! without being told what they say.
//!
//! What an update does about a secret it was never given is the
//! updater's problem rather than this module's; see
//! `sdmqtt_admin::rules`, which puts the stored one back.

use std::collections::BTreeMap;

use mqtt_codec::QoS;
use serde::{Deserialize, Serialize};
use storage::record::Secret;

use crate::http_in::HttpInError;
use crate::rule::{
    Action, BridgeTopic, ClientPattern, Direction, HttpIn, HttpOut, HttpPoll, MqttBridge, Rule,
    RuleError, RuleId, TopicFromRequest, TopicSource, Trigger,
};

/// Why a written rule is not a rule.
///
/// A field and a sentence. The sentence is the one the check that
/// refused it wrote, so two callers refusing the same shape say the
/// same thing; the field is what a form marks, and is never read by a
/// person.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{why}")]
pub struct SpecError {
    /// Which key is at fault, in the spelling both a configuration
    /// file and the API use. Nested keys are written with a full
    /// stop: `http_in.token`.
    pub field: &'static str,
    /// One sentence, for a person.
    pub why: String,
}

impl SpecError {
    /// A refusal about `field`.
    pub fn new(field: &'static str, why: impl Into<String>) -> Self {
        Self {
            field,
            why: why.into(),
        }
    }

    /// The refusal [`Rule::new`] made, with the field it is about.
    pub fn from_rule(error: RuleError) -> Self {
        Self::new(error.field(), error.to_string())
    }

    /// The refusal [`crate::http_in::check`] made.
    pub fn from_http_in(error: HttpInError) -> Self {
        Self::new(error.field(), error.to_string())
    }
}

/// One rule, as a configuration file spells it.
///
/// Section 9.1's rule, flattened by one level: the trigger's three parts
/// sit beside the name rather than in a table of their own, because a
/// `[rules.trigger]` holding one required key and two optional ones is
/// ceremony around a filter.
///
/// **Rules are node-local in milestone 4** and replication is milestone
/// 5's. What a rule declared here does is get written into the store at
/// startup, so it survives a restart and its outbox hangs off it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleSpec {
    /// What names it, and what its dead-letter topic is built from.
    ///
    /// One topic level: no separator, no wildcard, nothing that would
    /// give one rule's dead-letter topic to another. The rules crate
    /// refuses anything else.
    pub id: String,
    /// What an operator calls it.
    pub name: String,
    /// Whether it fires at all.
    ///
    /// **Not the same as deleting it.** Turning a connector off while
    /// an endpoint is down and back on afterwards is the ordinary
    /// thing to want, and deleting the rule would take its outbox with
    /// it.
    #[serde(default = "enabled_unless_said_otherwise")]
    pub enabled: bool,
    /// The topic filter a message must match.
    ///
    /// Required for an action that fires on published messages, and
    /// **refused** for one that does not: section 9.2's `http_in` is
    /// fired by a request, and a filter on it is a filter that can
    /// never match. An operator who wrote one believes something about
    /// when the rule runs that is not true, so it is said rather than
    /// ignored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    /// The lowest level that fires it, if it is narrowed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_qos: Option<u8>,
    /// The client identifier pattern that fires it, if it is narrowed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// Post a JSON envelope out, if that is what this rule does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_out: Option<HttpOutSpec>,
    /// Accept a post and publish its body, if that is what it does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_in: Option<HttpInSpec>,
    /// Fetch a URL on an interval and publish the body, if that is
    /// what it does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_poll: Option<HttpPollSpec>,
    /// Connect to a remote broker as a client, if that is what it
    /// does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mqtt_bridge: Option<MqttBridgeSpec>,
}

/// A rule left out of a file is on.
fn enabled_unless_said_otherwise() -> bool {
    true
}

/// How an `http_out` rule posts.
///
/// Every field section 9.2 lists as configurable. The defaults are the
/// rules crate's own, restated here only where serde needs a function;
/// `url` has no default because a default that posts somewhere is worse
/// than one that posts nowhere.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HttpOutSpec {
    /// Where to post. `https://`, or `http://` to a loopback address.
    pub url: String,
    /// Which method to use.
    pub method: String,
    /// Headers to send.
    ///
    /// A table, so it is written the way a header is read:
    /// `headers = { "x-token" = "secret" }`. **Sorted by name**, which
    /// a table is anyway and which costs nothing: HTTP gives no meaning
    /// to the order of two differently named headers, and a table
    /// cannot express the one case where order does matter — the same
    /// name twice — at all.
    pub headers: BTreeMap<String, String>,
    /// How long one attempt may take, in milliseconds.
    pub timeout_ms: u32,
    /// How many attempts may be in flight at once.
    pub max_concurrency: u16,
    /// How many times to retry after the first attempt fails.
    pub retries: u8,
    /// How long to wait before the first retry, in milliseconds.
    pub backoff_ms: u32,
}

impl Default for HttpOutSpec {
    /// Nowhere to post, and bounds on everything else.
    fn default() -> Self {
        let action = HttpOut::default();
        Self {
            url: action.url,
            method: action.method,
            headers: BTreeMap::new(),
            timeout_ms: action.timeout_ms,
            max_concurrency: action.max_concurrency,
            retries: action.retries,
            backoff_ms: action.backoff_ms,
        }
    }
}

impl RuleSpec {
    /// A rule this process holds, written back out.
    ///
    /// **Convention 1, from the other side.** The reader of a written
    /// rule is [`RuleSpec::to_rule`]; this is the writer, so what it
    /// produces has to be something that reader accepts. A listing
    /// that handed back a shape the API would refuse on the way in
    /// would make the obvious thing — read a rule, change one field,
    /// write it back — fail for reasons nobody could see.
    ///
    /// **The secrets do not come back**, so the spec this returns has
    /// an empty `http_in.token` and no `mqtt_bridge.password`. They
    /// are skipped on the way to the wire anyway; they are cleared
    /// here as well so that a caller holding one of these in memory
    /// is not holding a secret either.
    pub fn of(rule: &Rule) -> Self {
        let trigger = rule.trigger.as_ref();
        Self {
            id: rule.id.as_str().to_owned(),
            name: rule.name.clone(),
            enabled: rule.enabled,
            filter: trigger.map(|trigger| trigger.filter().to_owned()),
            min_qos: trigger.and_then(|trigger| trigger.min_qos).map(qos_number),
            client_id: trigger
                .and_then(|trigger| trigger.client_id.as_ref())
                .map(|pattern| pattern.as_str().to_owned()),
            http_out: match &rule.action {
                Action::HttpOut(action) => Some(HttpOutSpec {
                    url: action.url.clone(),
                    method: action.method.clone(),
                    headers: redacted(&action.headers),
                    timeout_ms: action.timeout_ms,
                    max_concurrency: action.max_concurrency,
                    retries: action.retries,
                    backoff_ms: action.backoff_ms,
                }),
                _ => None,
            },
            http_in: match &rule.action {
                Action::HttpIn(action) => {
                    Some(HttpInSpec {
                        token: Secret::new(String::new()),
                        topic: action.topic.clone(),
                        topic_header: action.topic_from.as_ref().and_then(|from| {
                            match &from.source {
                                TopicSource::Header(name) => Some(name.clone()),
                                TopicSource::Query(_) => None,
                            }
                        }),
                        topic_query: action.topic_from.as_ref().and_then(|from| {
                            match &from.source {
                                TopicSource::Query(name) => Some(name.clone()),
                                TopicSource::Header(_) => None,
                            }
                        }),
                        topic_prefix: action.topic_from.as_ref().map(|from| from.prefix.clone()),
                        qos: qos_number(action.qos),
                        retain: action.retain,
                    })
                }
                _ => None,
            },
            http_poll: match &rule.action {
                Action::HttpPoll(action) => Some(HttpPollSpec {
                    url: action.url.clone(),
                    headers: redacted(&action.headers),
                    interval_ms: action.interval_ms,
                    timeout_ms: action.timeout_ms,
                    max_body_bytes: action.max_body_bytes,
                    topic: action.topic.clone(),
                    qos: qos_number(action.qos),
                    retain: action.retain,
                }),
                _ => None,
            },
            mqtt_bridge: match &rule.action {
                Action::MqttBridge(action) => Some(MqttBridgeSpec {
                    address: action.address.clone(),
                    client_id: action.client_id.clone(),
                    protocol_level: action.version.level(),
                    username: action.username.clone(),
                    password: None,
                    keep_alive_secs: action.keep_alive_secs,
                    clean_start: action.clean_start,
                    session_expiry_secs: action.session_expiry_secs,
                    receive_maximum: action.receive_maximum,
                    maximum_packet_size: action.maximum_packet_size,
                    topic_alias_maximum: action.topic_alias_maximum,
                    state_topic: action.state_topic.clone(),
                    reconnect_min_ms: action.reconnect_min_ms,
                    reconnect_max_ms: action.reconnect_max_ms,
                    max_hops: action.max_hops,
                    topics: action
                        .topics
                        .iter()
                        .map(|topic| BridgeTopicSpec {
                            filter: topic.filter.clone(),
                            direction: match topic.direction {
                                Direction::In => "in".to_owned(),
                                Direction::Out => "out".to_owned(),
                                Direction::Both => "both".to_owned(),
                            },
                            qos: qos_number(topic.qos),
                            local_prefix: topic.local_prefix.clone(),
                            remote_prefix: topic.remote_prefix.clone(),
                            template: topic.template.clone(),
                        })
                        .collect(),
                }),
                _ => None,
            },
        }
    }

    /// This section as the rules crate's own type.
    ///
    /// # Errors
    ///
    /// [`SpecError`], naming the field at fault and what is wrong
    /// with it.
    /// Everything an operator can get wrong is checked here, once, at
    /// startup: the identifier, the filter, the level, and then
    /// whichever action was written — the URL, the scheme, the method
    /// and every header for `http_out`, and the token, the topic, the
    /// source and the prefix for `http_in`. A rule that cannot work is
    /// a broker that does not start, rather than a rule that quietly
    /// never fires or an endpoint that quietly accepts anything.
    pub fn to_rule(&self) -> Result<Rule, SpecError> {
        let id = RuleId::new(self.id.clone()).map_err(SpecError::from_rule)?;
        let action = self.to_action()?;

        // A filter belongs to an action that fires on messages. Which
        // of the two mistakes an operator made is said here, where both
        // halves are visible, rather than as one sentence about a
        // mismatch.
        let trigger = match (&self.filter, action.fires_on_messages()) {
            (Some(filter), true) => {
                let mut trigger = Trigger::new(filter.clone()).map_err(SpecError::from_rule)?;
                if let Some(level) = self.min_qos {
                    trigger.min_qos = Some(match level {
                        0 => QoS::AtMostOnce,
                        1 => QoS::AtLeastOnce,
                        2 => QoS::ExactlyOnce,
                        other => {
                            return Err(SpecError::new(
                                "min_qos",
                                format!("min_qos = {other} is not a level; MQTT has 0, 1 and 2"),
                            ))
                        }
                    });
                }
                if let Some(pattern) = &self.client_id {
                    trigger.client_id = Some(ClientPattern::new(pattern.clone()));
                }
                Some(trigger)
            }
            (None, true) => {
                return Err(SpecError::new(
                    "filter",
                    "this rule fires on published messages and has no filter; set filter",
                ))
            }
            (Some(_), false) => {
                // Which kind of rule it is decides what fired it, and
                // an operator who wrote a filter believes something
                // about when the rule runs that is not true. Saying
                // which is the difference between a message they can
                // act on and one they have to guess at.
                return Err(SpecError::new(
                    "filter",
                    match &action {
                        Action::HttpPoll(_) => "an http_poll rule is fired by its own interval \
                                            rather than by a published message, so it has \
                                            no filter; remove it"
                            .to_owned(),
                        Action::MqttBridge(_) => "an mqtt_bridge rule decides what moves with \
                                              its own topic entries, each of which has a \
                                              direction and a rewrite, so it has no \
                                              rule-level filter; put it in a \
                                              [[rules.mqtt_bridge.topics]] entry instead"
                            .to_owned(),
                        _ => "an http_in rule is fired by a request rather than by a published \
                          message, so it has no filter; remove it"
                            .to_owned(),
                    },
                ));
            }
            (None, false) => {
                // The two narrowings belong to a filter, so writing one
                // without a filter is the same mistake said smaller.
                if self.min_qos.is_some() || self.client_id.is_some() {
                    return Err(SpecError::new(
                        "min_qos",
                        format!(
                            "min_qos and client_id narrow a filter, and an {} rule has \
                             none; remove them",
                            match &action {
                                Action::HttpPoll(_) => "http_poll",
                                Action::MqttBridge(_) => "mqtt_bridge",
                                _ => "http_in",
                            }
                        ),
                    ));
                }
                None
            }
        };

        let mut rule =
            Rule::new(id, self.name.clone(), trigger, action).map_err(SpecError::from_rule)?;
        rule.enabled = self.enabled;
        Ok(rule)
    }

    /// The one action this section declares.
    ///
    /// Section 9.1: **one** action. Two tables is an operator who means
    /// two rules, and none is a rule that does nothing; both are
    /// refused rather than resolved by a precedence nobody wrote down.
    ///
    /// Written as a count of what was declared rather than as a match
    /// over the combinations, because the combinations square with
    /// every action added and the answer for all but one of them is
    /// the same sentence.
    fn to_action(&self) -> Result<Action, SpecError> {
        let declared: Vec<&str> = [
            self.http_out.as_ref().map(|_| "[rules.http_out]"),
            self.http_in.as_ref().map(|_| "[rules.http_in]"),
            self.http_poll.as_ref().map(|_| "[rules.http_poll]"),
            self.mqtt_bridge.as_ref().map(|_| "[rules.mqtt_bridge]"),
        ]
        .into_iter()
        .flatten()
        .collect();
        if declared.len() > 1 {
            // Spelled as a word, because the sentence is read by a
            // person and because three is as many as there can be.
            let count = match declared.len() {
                2 => "two",
                3 => "three",
                _ => "four",
            };
            return Err(SpecError::new(
                "action",
                format!(
                    "a rule has one action, section 9.1, and this one declares {}; \
                     write {count} rules",
                    declared.join(" and "),
                ),
            ));
        }

        if let Some(out) = &self.http_out {
            let action = HttpOut {
                url: out.url.clone(),
                method: out.method.clone(),
                headers: out
                    .headers
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
                timeout_ms: out.timeout_ms,
                max_concurrency: out.max_concurrency,
                retries: out.retries,
                backoff_ms: out.backoff_ms,
            };
            // Before anything is stored and long before anything is
            // posted.
            crate::http_out::check(&action)
                .map_err(|error| SpecError::new(error.field(), error.to_string()))?;
            return Ok(Action::HttpOut(action));
        }
        if let Some(into) = &self.http_in {
            return Ok(Action::HttpIn(into.to_action()?));
        }
        if let Some(poll) = &self.http_poll {
            return Ok(Action::HttpPoll(poll.to_action()?));
        }
        if let Some(bridge) = &self.mqtt_bridge {
            return Ok(Action::MqttBridge(Box::new(bridge.to_action()?)));
        }
        Err(SpecError::new(
            "action",
            "a rule needs an action; write a [rules.http_out], a [rules.http_in], an \
             [rules.http_poll] or an [rules.mqtt_bridge] table",
        ))
    }
}

/// How an `http_in` rule accepts a post.
///
/// Every field section 9.2 lists, and no field it does not. The token
/// has no default because a default token is a secret in a repository,
/// and the topic has none because a default that publishes somewhere is
/// worse than one that publishes nowhere. Both are refused when left
/// out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HttpInSpec {
    /// The bearer token a request must present.
    ///
    /// **Not optional.** Section 9.2: the endpoint publishes, so an
    /// unguarded one is a hole rather than a degraded mode.
    ///
    /// A [`Secret`], so that the configuration this binary logs at
    /// startup does not carry it, and **skipped on the way out**: a
    /// rule read back over the API carries no token at all. See this
    /// module's note on secrets.
    #[serde(skip_serializing)]
    pub token: Secret,
    /// Where the body is published when the request does not choose.
    pub topic: String,
    /// The header a request may name a topic in.
    ///
    /// Requires `topic_prefix`, and excludes `topic_query`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_header: Option<String>,
    /// The query parameter a request may name a topic in.
    ///
    /// Requires `topic_prefix`, and excludes `topic_header`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_query: Option<String>,
    /// The prefix a topic the request chose must lie at or under.
    ///
    /// **This is the whole safety of a rule that lets a request choose
    /// a topic.** Without it whoever holds this rule's token can
    /// publish anywhere on the broker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_prefix: Option<String>,
    /// The level the body is published at, as MQTT numbers it.
    pub qos: u8,
    /// Whether it is retained.
    pub retain: bool,
}

impl Default for HttpInSpec {
    /// No token, nowhere to publish, and the level that promises
    /// nothing.
    fn default() -> Self {
        Self {
            token: Secret::new(String::new()),
            topic: String::new(),
            topic_header: None,
            topic_query: None,
            topic_prefix: None,
            qos: 0,
            retain: false,
        }
    }
}

impl HttpInSpec {
    /// This table as the rules crate's own type.
    fn to_action(&self) -> Result<HttpIn, SpecError> {
        let qos = crate::http_in::qos_of(self.qos).ok_or_else(|| {
            SpecError::new(
                "http_in.qos",
                format!(
                    "http_in.qos = {} is not a level; MQTT has 0, 1 and 2",
                    self.qos
                ),
            )
        })?;

        // Two sources, or a prefix with nothing to bound, are refused
        // here rather than in the rules crate, because that crate's
        // type cannot express either: a source carries its prefix, so
        // the invalid combinations are the two keys' and not the
        // model's.
        let source = match (&self.topic_header, &self.topic_query) {
            (Some(_), Some(_)) => return Err(SpecError::from_http_in(HttpInError::TwoSources)),
            (Some(name), None) => Some(TopicSource::Header(name.clone())),
            (None, Some(name)) => Some(TopicSource::Query(name.clone())),
            (None, None) => None,
        };
        let topic_from = match (source, &self.topic_prefix) {
            (Some(source), Some(prefix)) => Some(TopicFromRequest {
                source,
                prefix: prefix.clone(),
            }),
            (Some(source), None) => {
                let key = match source {
                    TopicSource::Header(_) => "topic_header",
                    TopicSource::Query(_) => "topic_query",
                };
                return Err(SpecError::from_http_in(HttpInError::NoPrefix { key }));
            }
            (None, Some(_)) => return Err(SpecError::from_http_in(HttpInError::NoSource)),
            (None, None) => None,
        };

        let action = HttpIn {
            token: self.token.clone(),
            topic: self.topic.clone(),
            topic_from,
            qos,
            retain: self.retain,
        };
        // Before anything is stored and long before anything is served.
        crate::http_in::check(&action).map_err(SpecError::from_http_in)?;
        Ok(action)
    }
}

/// How an `http_poll` rule fetches.
///
/// Every field section 9.2 lists as configurable for it, and no field
/// it does not. **There is no retry count and no backoff**, unlike
/// `[rules.http_out]`: a poller has no outbox, a fetch that failed is
/// tried again on the next interval, and a body republished late is
/// worse than one not republished. `crates/rules/src/http_poll.rs`
/// carries the argument.
///
/// `url` and `topic` have no default, because a default that fetches
/// from somewhere or publishes to somewhere is worse than one that
/// does neither. Both are refused when left out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct HttpPollSpec {
    /// Where to fetch from. `https://`, or `http://` to a loopback
    /// address, on exactly the rule `[rules.http_out]` is held to.
    pub url: String,
    /// Headers to send.
    ///
    /// A table, sorted by name, for the reason
    /// [`HttpOutSpec::headers`] gives.
    pub headers: BTreeMap<String, String>,
    /// How long between the start of one fetch and the next, in
    /// milliseconds.
    ///
    /// **A fetch slower than this loses the intervals it covered.**
    /// One fetch is ever in flight, and the boundaries a slow fetch
    /// passed are dropped rather than fired in a burst afterwards, so
    /// an endpoint that has become slow is not answered with a run of
    /// back-to-back requests.
    pub interval_ms: u32,
    /// How long one fetch may take, in milliseconds.
    ///
    /// May be longer than the interval, and is not refused for it: a
    /// fetch that overruns is bounded by the paragraph above rather
    /// than by a rule about what an operator may write.
    pub timeout_ms: u32,
    /// The most of a response body that will be read, in bytes.
    ///
    /// A body past this is a **failed fetch**, not a truncated
    /// publication: half a document on a topic is worse than nothing
    /// on it, and a subscriber cannot tell the two apart.
    pub max_body_bytes: u32,
    /// Where the body is published.
    pub topic: String,
    /// The level it is published at, as MQTT numbers it.
    pub qos: u8,
    /// Whether it is retained.
    ///
    /// Usually what an operator wants from a poller — the current
    /// value, available to a subscriber that has just connected — and
    /// off by default all the same, because a retained message outlives
    /// the rule that wrote it.
    pub retain: bool,
}

impl Default for HttpPollSpec {
    /// Nowhere to fetch from, nowhere to publish to, and bounds on
    /// everything else. The rules crate's own defaults.
    fn default() -> Self {
        let action = HttpPoll::default();
        Self {
            url: action.url,
            headers: BTreeMap::new(),
            interval_ms: action.interval_ms,
            timeout_ms: action.timeout_ms,
            max_body_bytes: action.max_body_bytes,
            topic: action.topic,
            qos: 0,
            retain: action.retain,
        }
    }
}

impl HttpPollSpec {
    /// This table as the rules crate's own type.
    fn to_action(&self) -> Result<HttpPoll, SpecError> {
        let qos = crate::http_in::qos_of(self.qos).ok_or_else(|| {
            SpecError::new(
                "http_poll.qos",
                format!(
                    "http_poll.qos = {} is not a level; MQTT has 0, 1 and 2",
                    self.qos
                ),
            )
        })?;

        let action = HttpPoll {
            url: self.url.clone(),
            headers: self
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            interval_ms: self.interval_ms,
            timeout_ms: self.timeout_ms,
            max_body_bytes: self.max_body_bytes,
            topic: self.topic.clone(),
            qos,
            retain: self.retain,
        };
        // Before anything is stored and long before anything is
        // fetched.
        crate::http_poll::check(&action)
            .map_err(|error| SpecError::new(error.field(), error.to_string()))?;
        Ok(action)
    }
}

/// How an `mqtt_bridge` rule connects. Section 9.2.
///
/// The address has no default because a default that connects
/// somewhere is worse than one that connects nowhere, and the topics
/// have none because a bridge that carries nothing is a bridge nobody
/// meant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MqttBridgeSpec {
    /// Where the remote broker is: `mqtts://host:port`, or
    /// `mqtt://host:port` to a loopback address.
    ///
    /// Plain text to anywhere but this machine is refused, on the same
    /// reasoning `http_out` refuses a plain-text endpoint and with more
    /// at stake: a bridge sends its password in the connect and then
    /// everything it carries.
    pub address: String,
    /// What this bridge calls itself to the remote.
    ///
    /// May be left out on MQTT 5, where the remote assigns one and says
    /// so. On 3.1.1 there is no way for it to say so, so it is
    /// required.
    pub client_id: String,
    /// Which version to speak: 4 for MQTT 3.1.1, 5 for MQTT 5.
    ///
    /// The protocol level byte rather than a name, because that is what
    /// goes on the wire and what every other tool an operator uses
    /// calls it.
    pub protocol_level: u8,
    /// The user name to offer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// The password to offer.
    ///
    /// A [`Secret`], so the configuration this binary logs at startup
    /// does not carry it, and **skipped on the way out**, like an
    /// ingest rule's token and for the same reason.
    #[serde(skip_serializing)]
    pub password: Option<Secret>,
    /// The keep-alive to propose, in seconds.
    ///
    /// A proposal: MQTT 5 lets the remote override it, and the
    /// override is what this bridge then uses.
    pub keep_alive_secs: u16,
    /// Whether to ask the remote to discard any session it holds.
    ///
    /// **The first connection of a process asks for a clean start
    /// whatever this says.** The in-flight window is held in memory, so
    /// a resumed session would hold packet identifiers the restarted
    /// process no longer knows about; [`crate::bridge::supervisor`] has
    /// the argument.
    pub clean_start: bool,
    /// How long the remote should keep the session after a disconnect.
    pub session_expiry_secs: u32,
    /// How many acknowledged publications this end will take at once.
    pub receive_maximum: u16,
    /// The largest packet this end will take, in bytes.
    pub maximum_packet_size: u32,
    /// The highest topic alias this end will accept from the remote.
    ///
    /// Above zero licenses a compliant remote to start using aliases at
    /// any moment; [`crate::bridge::aliases`] is what makes that safe.
    pub topic_alias_maximum: u16,
    /// Where this bridge's connectivity is reported, on this broker.
    ///
    /// The same name is the will topic on the remote. A state topic
    /// this bridge would itself carry is refused, because a bridge
    /// republishing its own liveness is a loop with a heartbeat.
    pub state_topic: String,
    /// The first reconnection wait, in milliseconds.
    pub reconnect_min_ms: u32,
    /// The longest reconnection wait, in milliseconds.
    pub reconnect_max_ms: u32,
    /// The hop count above which a publication is dropped.
    pub max_hops: u32,
    /// What moves, and which way.
    pub topics: Vec<BridgeTopicSpec>,
}

/// One topic entry on a bridge. Section 9.2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct BridgeTopicSpec {
    /// The filter, carrying **neither** prefix.
    ///
    /// The local topics an outbound entry fires on are `local_prefix`
    /// followed by something matching this; the remote filter an
    /// inbound entry subscribes with is `remote_prefix` followed by the
    /// same.
    pub filter: String,
    /// `"in"`, `"out"` or `"both"`.
    ///
    /// Note that `"both"` on one entry is a loop by construction and is
    /// refused; [`crate::bridge::topics`] says why, and says what to
    /// write instead.
    pub direction: String,
    /// The level to move messages at.
    pub qos: u8,
    /// The prefix this broker's half of the topic carries.
    pub local_prefix: String,
    /// The prefix the remote's half carries.
    pub remote_prefix: String,
    /// The outbound template, if the topic is rewritten by position.
    ///
    /// `sensors/+/temp` with `template = "site-a/temp/{1}"` publishes
    /// `sensors/kitchen/temp` remotely as `site-a/temp/kitchen`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
}

impl Default for BridgeTopicSpec {
    fn default() -> Self {
        Self {
            filter: String::new(),
            direction: "out".to_owned(),
            qos: 0,
            local_prefix: String::new(),
            remote_prefix: String::new(),
            template: None,
        }
    }
}

impl Default for MqttBridgeSpec {
    /// Nowhere to connect to, nothing to carry, and bounds on
    /// everything else.
    fn default() -> Self {
        let action = MqttBridge::default();
        Self {
            address: action.address,
            client_id: action.client_id,
            protocol_level: 5,
            username: action.username,
            password: action.password,
            keep_alive_secs: action.keep_alive_secs,
            clean_start: action.clean_start,
            session_expiry_secs: action.session_expiry_secs,
            receive_maximum: action.receive_maximum,
            maximum_packet_size: action.maximum_packet_size,
            topic_alias_maximum: action.topic_alias_maximum,
            state_topic: action.state_topic,
            reconnect_min_ms: action.reconnect_min_ms,
            reconnect_max_ms: action.reconnect_max_ms,
            max_hops: action.max_hops,
            topics: Vec::new(),
        }
    }
}

impl MqttBridgeSpec {
    /// This table as the rules crate's own type.
    fn to_action(&self) -> Result<MqttBridge, SpecError> {
        let version = match self.protocol_level {
            4 => mqtt_codec::Version::V311,
            5 => mqtt_codec::Version::V5,
            other => {
                return Err(SpecError::new(
                    "mqtt_bridge.protocol_level",
                    format!(
                        "mqtt_bridge.protocol_level = {other} is not a version this bridge \
                         speaks; write 4 for MQTT 3.1.1 or 5 for MQTT 5"
                    ),
                ))
            }
        };
        // [MQTT-3.1.3-x]: 3.1.1 has no way for a server to tell a
        // client what identifier it was given, so an empty one there is
        // a session that can never be resumed and a bridge whose
        // reconnections each get a new one. Refused when the
        // configuration is read rather than discovered on the wire.
        if self.client_id.is_empty() && version == mqtt_codec::Version::V311 {
            return Err(SpecError::new(
                "mqtt_bridge.client_id",
                "mqtt_bridge.client_id is empty and protocol_level is 4. MQTT 3.1.1 has \
                 no way for the remote to say what identifier it assigned, so the session \
                 could never be resumed; set client_id, or speak MQTT 5.",
            ));
        }
        if self.receive_maximum == 0 {
            return Err(SpecError::new(
                "mqtt_bridge.receive_maximum",
                "mqtt_bridge.receive_maximum is zero, which MQTT 5 makes a protocol error \
                 and which would be a bridge that can receive nothing",
            ));
        }
        if self.max_hops == 0 {
            return Err(SpecError::new(
                "mqtt_bridge.max_hops",
                "mqtt_bridge.max_hops is zero, so every message would be over the limit \
                 and this bridge would carry nothing",
            ));
        }

        let mut topics = Vec::with_capacity(self.topics.len());
        for (index, topic) in self.topics.iter().enumerate() {
            let direction = match topic.direction.as_str() {
                "in" => Direction::In,
                "out" => Direction::Out,
                "both" => Direction::Both,
                other => {
                    return Err(SpecError::new(
                        "mqtt_bridge.topics",
                        format!(
                            "mqtt_bridge topic {index}: direction = {other:?} is not a \
                             direction; write \"in\", \"out\" or \"both\""
                        ),
                    ))
                }
            };
            let qos = crate::http_in::qos_of(topic.qos).ok_or_else(|| {
                SpecError::new(
                    "mqtt_bridge.topics",
                    format!(
                        "mqtt_bridge topic {index}: qos = {} is not a level; MQTT has 0, 1 \
                         and 2",
                        topic.qos
                    ),
                )
            })?;
            topics.push(BridgeTopic {
                filter: topic.filter.clone(),
                direction,
                qos,
                local_prefix: topic.local_prefix.clone(),
                remote_prefix: topic.remote_prefix.clone(),
                template: topic.template.clone(),
            });
        }

        let action = MqttBridge {
            address: self.address.clone(),
            client_id: self.client_id.clone(),
            version,
            username: self.username.clone(),
            password: self.password.clone(),
            keep_alive_secs: self.keep_alive_secs,
            clean_start: self.clean_start,
            session_expiry_secs: self.session_expiry_secs,
            receive_maximum: self.receive_maximum,
            maximum_packet_size: self.maximum_packet_size,
            topic_alias_maximum: self.topic_alias_maximum,
            state_topic: self.state_topic.clone(),
            reconnect_min_ms: self.reconnect_min_ms,
            reconnect_max_ms: self.reconnect_max_ms,
            max_hops: self.max_hops,
            topics,
        };

        // **Before anything is stored and long before anything
        // connects.** Both of them: the address, which is where a
        // plain-text link to somewhere other than this machine is
        // refused, and the topics, which is where a configuration that
        // loops is refused. Section 9.2 puts the second first among the
        // loop defences, and this is the moment it means.
        crate::bridge::supervisor::address(&action.address)
            .map_err(|error| SpecError::new(error.field(), error.to_string()))?;
        crate::bridge::topics::check(&action)
            .map_err(|error| SpecError::new(error.field(), error.to_string()))?;
        Ok(action)
    }
}

/// The header names a rule sends, with nothing of what they say.
///
/// See the module's note on secrets: a header map is where a rule
/// keeps its bearer token, and a listing that gave the values out
/// would be a hole. The names are kept because a form showing "this
/// rule sends `x-token`" is useful and telling it what the token is is
/// not.
fn redacted(headers: &[(String, String)]) -> BTreeMap<String, String> {
    headers
        .iter()
        .map(|(name, _)| (name.clone(), String::new()))
        .collect()
}

/// MQTT's numbering, on the way out.
///
/// The inverse of [`crate::http_in::qos_of`], which is the way in.
fn qos_number(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One rule of each of the four kinds, written the way an
    /// operator writes one.
    fn written() -> Vec<RuleSpec> {
        let base = |id: &str, name: &str| RuleSpec {
            id: id.to_owned(),
            name: name.to_owned(),
            enabled: true,
            filter: None,
            min_qos: None,
            client_id: None,
            http_out: None,
            http_in: None,
            http_poll: None,
            mqtt_bridge: None,
        };
        vec![
            RuleSpec {
                filter: Some("sensor/+/temp".to_owned()),
                min_qos: Some(1),
                client_id: Some("gateway-*".to_owned()),
                http_out: Some(HttpOutSpec {
                    url: "https://example.invalid/hook".to_owned(),
                    method: "POST".to_owned(),
                    headers: [("x-token".to_owned(), "the-bearer-token".to_owned())]
                        .into_iter()
                        .collect(),
                    timeout_ms: 2_000,
                    max_concurrency: 4,
                    retries: 3,
                    backoff_ms: 500,
                }),
                ..base("telemetry", "Telemetry out")
            },
            RuleSpec {
                http_in: Some(HttpInSpec {
                    token: Secret::new("a-secret"),
                    topic: "ingest/across".to_owned(),
                    topic_header: Some("x-topic".to_owned()),
                    topic_query: None,
                    topic_prefix: Some("ingest".to_owned()),
                    qos: 1,
                    retain: false,
                }),
                ..base("gateway", "Gateway in")
            },
            RuleSpec {
                http_poll: Some(HttpPollSpec {
                    url: "https://example.invalid/now".to_owned(),
                    headers: BTreeMap::new(),
                    interval_ms: 30_000,
                    timeout_ms: 5_000,
                    max_body_bytes: 65_536,
                    topic: "weather/now".to_owned(),
                    qos: 0,
                    retain: true,
                }),
                ..base("weather", "The weather, on an interval")
            },
            RuleSpec {
                mqtt_bridge: Some(MqttBridgeSpec {
                    address: "mqtts://remote.invalid:8883".to_owned(),
                    client_id: "bridge-one".to_owned(),
                    password: Some(Secret::new("a-secret")),
                    state_topic: "bridge/one/state".to_owned(),
                    topics: vec![
                        BridgeTopicSpec {
                            filter: "site/+/data".to_owned(),
                            direction: "out".to_owned(),
                            qos: 1,
                            template: Some("elsewhere/data/{1}".to_owned()),
                            ..BridgeTopicSpec::default()
                        },
                        BridgeTopicSpec {
                            filter: "commands/#".to_owned(),
                            direction: "in".to_owned(),
                            ..BridgeTopicSpec::default()
                        },
                    ],
                    ..MqttBridgeSpec::default()
                }),
                ..base("across", "A bridge")
            },
        ]
    }

    /// Convention 1: what the writer produces, the reader accepts.
    ///
    /// The listing over the admin API is [`RuleSpec::of`] and the
    /// create is [`RuleSpec::to_rule`], so a shape one produces that
    /// the other refuses would make the obvious edit — read a rule,
    /// change one field, write it back — fail for reasons nobody
    /// could see.
    ///
    /// **Everything but the secrets**, which is the whole of the
    /// exception and is asserted separately below: a secret is left
    /// out on purpose, so a rule carrying one comes back needing it
    /// put back. What that costs is one function in the updater and
    /// two tests here.
    #[test]
    fn a_rule_written_back_out_is_one_that_can_be_read_back_in() {
        for spec in written() {
            let rule = spec.to_rule().expect("the written rule is a rule");
            let mut again = RuleSpec::of(&rule);
            put_the_secrets_back(&mut again, &spec);
            let rebuilt = again.clone().to_rule().unwrap_or_else(|error| {
                panic!("{} did not survive a round trip: {error}", spec.id)
            });
            assert_eq!(rule, rebuilt, "{} came back as a different rule", spec.id);
            // And a second turn changes nothing, which is what says
            // the conversion has reached a fixed point rather than
            // drifting one field at a time.
            let mut third = RuleSpec::of(&rebuilt);
            put_the_secrets_back(&mut third, &spec);
            assert_eq!(third, again, "{} drifts", spec.id);
        }
    }

    /// What the admin API's update does, written here so that the
    /// round trip above says which fields are the exception.
    fn put_the_secrets_back(spec: &mut RuleSpec, from: &RuleSpec) {
        if let (Some(into), Some(had)) = (&mut spec.http_in, &from.http_in) {
            into.token = had.token.clone();
        }
        if let (Some(into), Some(had)) = (&mut spec.mqtt_bridge, &from.mqtt_bridge) {
            into.password.clone_from(&had.password);
        }
        if let (Some(into), Some(had)) = (&mut spec.http_out, &from.http_out) {
            into.headers.clone_from(&had.headers);
        }
        if let (Some(into), Some(had)) = (&mut spec.http_poll, &from.http_poll) {
            into.headers.clone_from(&had.headers);
        }
    }

    /// The exception, asserted rather than described: an ingest rule
    /// read back out is **refused** until its token is put back.
    ///
    /// That refusal is the backstop for the one dangerous shape a
    /// redaction can produce — a caller writing back what it read and
    /// silently taking the guard off an endpoint that publishes.
    #[test]
    fn an_ingest_rule_read_back_out_is_refused_until_its_token_is_put_back() {
        let spec = written()
            .into_iter()
            .find(|spec| spec.http_in.is_some())
            .expect("an ingest rule");
        let rule = spec.to_rule().expect("the written rule is a rule");
        let error = RuleSpec::of(&rule)
            .to_rule()
            .expect_err("a rule read back with no token must be refused");
        assert_eq!(error.field, "http_in.token");
    }

    /// Section 12.3: the two secrets do not come back out.
    ///
    /// Asserted on the serialised form as well as on the value,
    /// because the value is what a caller holds and the bytes are
    /// what a reader of an unauthenticated resource sees.
    #[test]
    fn a_rule_written_back_out_carries_no_secret() {
        for spec in written() {
            let rule = spec.to_rule().expect("the written rule is a rule");
            let out = RuleSpec::of(&rule);
            if let Some(http_in) = &out.http_in {
                assert!(http_in.token.is_empty(), "{} gave out its token", spec.id);
            }
            if let Some(bridge) = &out.mqtt_bridge {
                assert!(
                    bridge.password.is_none(),
                    "{} gave out its password",
                    spec.id
                );
            }
            // The header names are kept, which is what makes the
            // redaction below about values rather than about the map.
            if let Some(action) = &out.http_out {
                assert!(
                    action.headers.contains_key("x-token"),
                    "{} lost the name of a header it sends",
                    spec.id
                );
            }
            // The header values, which are the third secret and are
            // not a `Secret`: see the module note.
            for headers in [
                out.http_out.as_ref().map(|action| &action.headers),
                out.http_poll.as_ref().map(|action| &action.headers),
            ]
            .into_iter()
            .flatten()
            {
                for (name, value) in headers {
                    assert!(value.is_empty(), "{} gave out the value of {name}", spec.id);
                }
            }
            // And the same three, serialised, because the value a
            // caller holds and the bytes a reader of an
            // unauthenticated resource sees are two different
            // questions.
            let json = serde_json::to_string(&out).expect("a spec serialises");
            for secret in ["a-secret", "the-bearer-token"] {
                assert!(
                    !json.contains(secret),
                    "{} serialised a secret: {json}",
                    spec.id
                );
            }
        }
    }

    /// Every refusal names a field an operator wrote, and the name is
    /// one of the keys the spec above is written with.
    ///
    /// Convention 5: a `field()` that answered the same string for
    /// everything would satisfy an assertion that it answered
    /// *something*, so the answers are checked against the keys.
    #[test]
    fn every_refusal_names_a_key_a_rule_is_written_with() {
        let cases: Vec<(&str, String)> = vec![
            (
                "http_in.token",
                r#"{"id":"gateway","name":"Gateway in","http_in":{"topic":"in/here"}}"#.to_owned(),
            ),
            (
                "filter",
                r#"{"id":"gateway","name":"Gateway in","filter":"a/+/b",
                    "http_in":{"token":"s","topic":"in/here"}}"#
                    .to_owned(),
            ),
            (
                "filter",
                r#"{"id":"telemetry","name":"Out","http_out":{"url":"https://e.invalid/h"}}"#
                    .to_owned(),
            ),
            (
                "mqtt_bridge.topics",
                r#"{"id":"across","name":"Loop","mqtt_bridge":{
                     "address":"mqtts://remote.invalid:8883","client_id":"b",
                     "topics":[{"filter":"site/+/d","direction":"out"},
                               {"filter":"site/+/d","direction":"in"}]}}"#
                    .to_owned(),
            ),
            (
                "mqtt_bridge.address",
                r#"{"id":"across","name":"Plain","mqtt_bridge":{
                     "address":"mqtt://elsewhere.invalid:1883","client_id":"b",
                     "topics":[{"filter":"site/+/d","direction":"out"}]}}"#
                    .to_owned(),
            ),
            (
                "http_out.url",
                r#"{"id":"telemetry","name":"Out","filter":"a/+/b",
                    "http_out":{"url":"http://elsewhere.invalid/hook"}}"#
                    .to_owned(),
            ),
            ("action", r#"{"id":"nothing","name":"Nothing"}"#.to_owned()),
            (
                "id",
                r#"{"id":"a/b","name":"Slashed","filter":"a/+/b",
                    "http_out":{"url":"https://e.invalid/h"}}"#
                    .to_owned(),
            ),
            (
                "name",
                r#"{"id":"unnamed","name":"","filter":"a/+/b",
                    "http_out":{"url":"https://e.invalid/h"}}"#
                    .to_owned(),
            ),
        ];
        for (field, written) in cases {
            let spec: RuleSpec = serde_json::from_str(&written)
                .unwrap_or_else(|error| panic!("{written} is not a spec: {error}"));
            let error = spec
                .to_rule()
                .expect_err(&format!("{written} should be refused"));
            assert_eq!(error.field, field, "{written} was refused as {error}");
        }
    }
}
