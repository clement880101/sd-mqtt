//! What a rule is.
//!
//! Section 9.1: an identifier, a name, an enabled flag, a trigger and one
//! action. The trigger is a topic filter, optionally narrowed by a minimum
//! quality of service and a client identifier pattern.
//!
//! **A rule is node-local in milestone 4.** Section 9.1 says rules are
//! replicated metadata, and replication is milestone 5; until then a rule
//! is stored durably on the node that holds it and applies there. Nothing
//! about the shape below changes when replication arrives — what changes
//! is how many nodes hold it — and saying so here stops a reader taking a
//! single-node broker's behaviour for the design.

use std::fmt;

use mqtt_codec::QoS;
use storage::record::{
    ActionRecord, HttpInRecord, HttpOutRecord, Qos, RuleRecord, Secret, TopicFromRecord,
    TopicFromRequestRecord, TriggerRecord,
};

/// What can be wrong with a rule.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuleError {
    /// The identifier is not one a topic level can carry.
    #[error("a rule identifier may not be empty or contain '/', '+', '#' or a NUL: {0:?}")]
    BadIdentifier(String),
    /// The trigger's filter is not a topic filter.
    #[error("a rule's trigger is not a topic filter: {0:?}")]
    BadFilter(String),
    /// A rule must be called something an operator can recognise.
    #[error("a rule must have a name")]
    NoName,
    /// An action that fires on messages was given no trigger.
    #[error("a rule whose action fires on published messages needs a trigger")]
    NoTrigger,
    /// An action that does not fire on messages was given one.
    ///
    /// Refused rather than ignored. A filter on an ingest rule is a
    /// filter that can never fire, and an operator who wrote one
    /// believes something about when the rule runs that is not true.
    #[error(
        "an http_in rule is fired by a request rather than by a published message, \
         so it has no trigger; remove the filter"
    )]
    TriggerOnIngest,
    /// An ingest rule was given no bearer token.
    ///
    /// Section 9.2: **the token is not optional and a rule without one
    /// is refused**, on the same reasoning that refuses an unprotected
    /// admin listener. The endpoint publishes, so an unguarded one is a
    /// hole rather than a degraded mode.
    #[error(
        "an http_in rule must have a token: the endpoint publishes, so an unguarded \
         one is a hole rather than a degraded mode. Set http_in.token."
    )]
    NoToken,
}

/// What names a rule, for as long as it exists.
///
/// **Constrained to what one topic level can carry**, because section 9.2
/// publishes an exhausted envelope to `$sd/rules/<id>/dlq`. An identifier
/// with a separator or a wildcard in it would make that topic something
/// other than the one rule's, which is convention 1 read from the other
/// side: the reader of that topic rejects it, so the writer of the
/// identifier must.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuleId(String);

impl RuleId {
    /// Build an identifier, or say why it is not one.
    ///
    /// # Errors
    ///
    /// [`RuleError::BadIdentifier`] when it is empty or holds a character
    /// a topic level may not.
    pub fn new(id: impl Into<String>) -> Result<Self, RuleError> {
        let id = id.into();
        if id.is_empty() || id.contains(['/', '+', '#', '\u{0}']) {
            return Err(RuleError::BadIdentifier(id));
        }
        Ok(Self(id))
    }

    /// The identifier as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Where this rule's exhausted envelopes go. Section 9.2.
    pub fn dead_letter_topic(&self) -> String {
        format!("$sd/rules/{}/dlq", self.0)
    }
}

impl fmt::Display for RuleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Which client identifiers a trigger accepts.
///
/// A glob, and `*` is the whole of the syntax: it stands for any run of
/// characters, including none, and every other character matches itself.
/// Written down here because the design says "a client identifier
/// pattern" and nothing more, and a pattern language nobody wrote down is
/// one every operator guesses differently.
///
/// No character class, no alternation and no escape. An operator who
/// wants a literal `*` in a client identifier cannot express it, which is
/// a limit worth having: the alternative is an escape character that then
/// needs its own escape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientPattern {
    /// The pattern as written, kept so it can be stored again unchanged.
    source: String,
    /// The literal runs between the wildcards.
    segments: Vec<String>,
    /// Whether the pattern begins with a wildcard.
    open_start: bool,
    /// Whether it ends with one.
    open_end: bool,
}

impl ClientPattern {
    /// Compile a pattern. Every string is one.
    pub fn new(source: impl Into<String>) -> Self {
        let source = source.into();
        let segments: Vec<String> = source
            .split('*')
            .filter(|part| !part.is_empty())
            .map(str::to_owned)
            .collect();
        Self {
            open_start: source.starts_with('*'),
            open_end: source.ends_with('*'),
            segments,
            source,
        }
    }

    /// The pattern as written.
    pub fn as_str(&self) -> &str {
        &self.source
    }

    /// Whether a client identifier matches.
    pub fn matches(&self, client_id: &str) -> bool {
        // A pattern of nothing but wildcards has no segments and accepts
        // everything; an empty pattern has none either and accepts only
        // the empty identifier.
        if self.segments.is_empty() {
            return self.open_start || self.open_end || client_id.is_empty();
        }

        let mut rest = client_id;
        for (index, segment) in self.segments.iter().enumerate() {
            let first = index == 0;
            let last = index + 1 == self.segments.len();
            let at = if first && !self.open_start {
                if !rest.starts_with(segment.as_str()) {
                    return false;
                }
                0
            } else {
                match rest.find(segment.as_str()) {
                    Some(at) => at,
                    None => return false,
                }
            };
            if last && !self.open_end {
                // The last literal must be the end of the identifier, so
                // search from the right rather than accepting the first
                // occurrence: "a*b" matches "axbyb".
                return rest[at..].ends_with(segment.as_str())
                    || rest.rfind(segment.as_str()).is_some_and(|last_at| {
                        last_at >= at && rest.len() == last_at + segment.len()
                    });
            }
            rest = &rest[at + segment.len()..];
        }
        true
    }
}

/// When a rule fires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trigger {
    /// The topic filter a message must match.
    filter: String,
    /// The lowest level that fires the rule, if it is narrowed.
    pub min_qos: Option<QoS>,
    /// Which publishing clients fire it, if it is narrowed.
    pub client_id: Option<ClientPattern>,
}

impl Trigger {
    /// A trigger on a filter, unnarrowed.
    ///
    /// # Errors
    ///
    /// [`RuleError::BadFilter`] when the filter is not one. Validated
    /// here rather than when a message arrives, because a rule that can
    /// never match is a rule an operator wrote and nothing told them
    /// about.
    pub fn new(filter: impl Into<String>) -> Result<Self, RuleError> {
        let filter = filter.into();
        if mqtt_codec::topic::validate_filter(&filter, &mqtt_codec::Limits::default()).is_err() {
            return Err(RuleError::BadFilter(filter));
        }
        Ok(Self {
            filter,
            min_qos: None,
            client_id: None,
        })
    }

    /// The filter.
    pub fn filter(&self) -> &str {
        &self.filter
    }

    /// Narrow it to messages at this level or above.
    #[must_use]
    pub fn at_least(mut self, qos: QoS) -> Self {
        self.min_qos = Some(qos);
        self
    }

    /// Narrow it to clients whose identifier matches.
    #[must_use]
    pub fn from_clients(mut self, pattern: ClientPattern) -> Self {
        self.client_id = Some(pattern);
        self
    }

    /// Whether this message from this client fires the rule.
    ///
    /// The filter is matched by `broker-core`'s reference matcher, which
    /// is the one matcher in this workspace tested against the
    /// specification's own examples. It is documented as written for
    /// obviousness rather than speed; with a handful of rules that is the
    /// right trade, and a node with enough rules for it to matter wants
    /// them in the subscription trie rather than a faster loop here.
    pub fn matches(&self, topic: &str, qos: QoS, client_id: &str) -> bool {
        if !broker_core::topic::reference::matches(&self.filter, topic) {
            return false;
        }
        if self.min_qos.is_some_and(|minimum| qos < minimum) {
            return false;
        }
        self.client_id
            .as_ref()
            .is_none_or(|pattern| pattern.matches(client_id))
    }
}

/// What a rule does when it fires.
///
/// One action, section 9.1. Two today: `http_out` posts an envelope out
/// and `http_in` accepts a post in. `http_poll` and `mqtt_bridge` are
/// later milestones' variants of this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Post a JSON envelope to a URL. Section 9.2.
    HttpOut(HttpOut),
    /// Accept a post and publish its body. Section 9.2.
    HttpIn(HttpIn),
}

impl Action {
    /// Whether this action is fired by a published message.
    ///
    /// The question a trigger answers. `http_out` is; `http_in` is
    /// fired by a request and has nothing to trigger on.
    pub fn fires_on_messages(&self) -> bool {
        match self {
            Self::HttpOut(_) => true,
            Self::HttpIn(_) => false,
        }
    }
}

/// Where a request may name the topic it is publishing to.
///
/// Section 9.2: a named header **or** a query parameter. An enum rather
/// than two optional fields, so that "both, and nothing says which
/// wins" is a state that cannot be written down.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicSource {
    /// The named request header.
    Header(String),
    /// The named query parameter.
    Query(String),
}

impl TopicSource {
    /// The header or parameter name, whichever this is.
    pub fn name(&self) -> &str {
        match self {
            Self::Header(name) | Self::Query(name) => name,
        }
    }
}

/// A topic the request chose, and the prefix that bounds it.
///
/// **The prefix is not optional, and that is deliberate.** Section 9.2
/// allows the topic to come from a header or a query parameter
/// "restricted by a prefix so a caller cannot publish anywhere", and
/// that restriction is the whole safety of the feature: without it an
/// ingest rule is a way for whoever holds one rule's token to publish
/// to any topic on the broker, including the reserved namespace. Held
/// together with the source rather than beside it, so that a source
/// with no prefix is not a check somebody can forget — it is a value
/// nobody can construct.
///
/// What "under" means is [`crate::http_in::is_under`], and it is by
/// topic level rather than by string prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicFromRequest {
    /// Where the request says it.
    pub source: TopicSource,
    /// The prefix the topic must be at or under.
    pub prefix: String,
}

/// How an `http_in` action accepts a post. Section 9.2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpIn {
    /// The bearer token a request must present.
    ///
    /// Not optional. [`Rule::new`] refuses an empty one, which is where
    /// every rule this process holds is accepted — from a
    /// configuration file and from the store alike.
    pub token: Secret,
    /// Where the body is published when the request does not choose.
    pub topic: String,
    /// Where the request may choose instead, and what bounds it.
    pub topic_from: Option<TopicFromRequest>,
    /// The level the body is published at.
    pub qos: QoS,
    /// Whether it is retained.
    pub retain: bool,
}

impl Default for HttpIn {
    /// No token, nowhere to publish, and the level that promises
    /// nothing.
    ///
    /// The token is deliberately empty rather than a generated
    /// placeholder: a default that is a secret is a secret in a
    /// repository. [`Rule::new`] refuses this value, which is the point
    /// — the default is a shape to fill in, not a rule that runs.
    fn default() -> Self {
        Self {
            token: Secret::new(String::new()),
            topic: String::new(),
            topic_from: None,
            qos: QoS::AtMostOnce,
            retain: false,
        }
    }
}

/// How an `http_out` action posts.
///
/// Every field section 9.2 lists as configurable, and no field it does
/// not: method, headers, timeout, maximum concurrency, retry count and
/// backoff.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpOut {
    /// Where to post.
    pub url: String,
    /// Which method to use.
    pub method: String,
    /// Headers to send, in the order they were configured.
    pub headers: Vec<(String, String)>,
    /// How long one attempt may take, in milliseconds.
    pub timeout_ms: u32,
    /// How many attempts may be in flight at once.
    pub max_concurrency: u16,
    /// How many times to retry after the first attempt fails.
    ///
    /// Section 9.2 gives a rule a retry count because retrying is how an
    /// endpoint that is already struggling is taken down; the count and
    /// the backoff beside it are the whole of what bounds that.
    pub retries: u8,
    /// How long to wait before the first retry, in milliseconds.
    pub backoff_ms: u32,
}

impl Default for HttpOut {
    /// Somewhere to post, and bounds on everything else.
    ///
    /// The url is deliberately empty rather than a placeholder: a default
    /// that posts somewhere is worse than one that posts nowhere.
    fn default() -> Self {
        Self {
            url: String::new(),
            method: "POST".to_owned(),
            headers: Vec::new(),
            timeout_ms: 10_000,
            max_concurrency: 4,
            retries: 3,
            backoff_ms: 1_000,
        }
    }
}

/// A rule: what fires, and what it does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    /// What names it.
    pub id: RuleId,
    /// What an operator calls it.
    pub name: String,
    /// Whether it fires at all.
    ///
    /// Separate from deleting it, because turning a connector off while
    /// an endpoint is down and turning it back on afterwards is the
    /// ordinary thing to want, and deleting the rule would take its
    /// outbox with it.
    pub enabled: bool,
    /// When it fires, for an action that fires on messages.
    ///
    /// `None` for one that does not: section 9.2's `http_in` is fired
    /// by a request, and a filter on it would be a filter that can
    /// never match. [`Rule::new`] refuses either half without the
    /// other, so this is `Some` exactly when
    /// [`Action::fires_on_messages`] is true.
    pub trigger: Option<Trigger>,
    /// What it does. One, section 9.1.
    pub action: Action,
}

impl Rule {
    /// A rule.
    ///
    /// **This is where a rule is accepted**, from a configuration file
    /// and from the store alike — [`Rule::try_from`] goes through here
    /// too — so it is where the refusals that must hold of every rule
    /// in this process live.
    ///
    /// # Errors
    ///
    /// - [`RuleError::NoName`] when the name is empty.
    /// - [`RuleError::NoTrigger`] when an action that fires on
    ///   published messages was given none.
    /// - [`RuleError::TriggerOnIngest`] when an action that does not
    ///   was given one.
    /// - [`RuleError::NoToken`] when an `http_in` action has no bearer
    ///   token. Section 9.2: the token is not optional. Refused here
    ///   rather than only where a route is registered, because a rule
    ///   stored without one is a rule that will be registered later.
    pub fn new(
        id: RuleId,
        name: impl Into<String>,
        trigger: Option<Trigger>,
        action: Action,
    ) -> Result<Self, RuleError> {
        let name = name.into();
        if name.is_empty() {
            return Err(RuleError::NoName);
        }
        match (action.fires_on_messages(), trigger.is_some()) {
            (true, false) => return Err(RuleError::NoTrigger),
            (false, true) => return Err(RuleError::TriggerOnIngest),
            _ => {}
        }
        if let Action::HttpIn(http) = &action {
            if http.token.is_empty() {
                return Err(RuleError::NoToken);
            }
        }
        Ok(Self {
            id,
            name,
            enabled: true,
            trigger,
            action,
        })
    }

    /// Whether this message fires this rule.
    ///
    /// A disabled rule fires on nothing, and so does a rule with no
    /// trigger. Asked here rather than by every caller, because a
    /// caller that forgot would run a rule an operator switched off.
    pub fn fires_on(&self, topic: &str, qos: QoS, client_id: &str) -> bool {
        self.enabled
            && self
                .trigger
                .as_ref()
                .is_some_and(|trigger| trigger.matches(topic, qos, client_id))
    }

    /// This rule as the store holds it.
    pub fn to_record(&self) -> RuleRecord {
        RuleRecord {
            id: self.id.as_str().to_owned(),
            name: self.name.clone(),
            enabled: self.enabled,
            // A rule with no trigger stores the empty one. See
            // [`RuleRecord::trigger`]: an empty filter is not a filter,
            // so it cannot be read back as one somebody meant.
            trigger: self.trigger.as_ref().map_or_else(
                || TriggerRecord {
                    filter: String::new(),
                    min_qos: None,
                    client_id: None,
                },
                |trigger| TriggerRecord {
                    filter: trigger.filter().to_owned(),
                    min_qos: trigger.min_qos.map(stored_qos),
                    client_id: trigger
                        .client_id
                        .as_ref()
                        .map(|pattern| pattern.as_str().to_owned()),
                },
            ),
            action: match &self.action {
                Action::HttpOut(http) => ActionRecord::HttpOut(HttpOutRecord {
                    url: http.url.clone(),
                    method: http.method.clone(),
                    headers: http.headers.clone(),
                    timeout_ms: http.timeout_ms,
                    max_concurrency: http.max_concurrency,
                    retries: http.retries,
                    backoff_ms: http.backoff_ms,
                }),
                Action::HttpIn(http) => ActionRecord::HttpIn(HttpInRecord {
                    token: http.token.clone(),
                    topic: http.topic.clone(),
                    topic_from: http.topic_from.as_ref().map(|from| TopicFromRequestRecord {
                        source: match &from.source {
                            TopicSource::Header(name) => TopicFromRecord::Header(name.clone()),
                            TopicSource::Query(name) => TopicFromRecord::Query(name.clone()),
                        },
                        prefix: from.prefix.clone(),
                    }),
                    qos: stored_qos(http.qos),
                    retain: http.retain,
                }),
            },
        }
    }
}

impl TryFrom<&RuleRecord> for Rule {
    type Error = RuleError;

    /// Rebuild a rule from what the store held.
    ///
    /// **Validated on the way in, not assumed.** The store is bytes on a
    /// disk and a rule read back from one has been outside this process;
    /// accepting an identifier with a separator in it would give one
    /// rule's dead-letter topic to another. This is the same rule the
    /// codec keeps: what a reader rejects, a writer must refuse, and the
    /// reader is here.
    fn try_from(record: &RuleRecord) -> Result<Self, Self::Error> {
        let action = match &record.action {
            ActionRecord::HttpOut(http) => Action::HttpOut(HttpOut {
                url: http.url.clone(),
                method: http.method.clone(),
                headers: http.headers.clone(),
                timeout_ms: http.timeout_ms,
                max_concurrency: http.max_concurrency,
                retries: http.retries,
                backoff_ms: http.backoff_ms,
            }),
            ActionRecord::HttpIn(http) => Action::HttpIn(HttpIn {
                token: http.token.clone(),
                topic: http.topic.clone(),
                topic_from: http.topic_from.as_ref().map(|from| TopicFromRequest {
                    source: match &from.source {
                        TopicFromRecord::Header(name) => TopicSource::Header(name.clone()),
                        TopicFromRecord::Query(name) => TopicSource::Query(name.clone()),
                    },
                    prefix: from.prefix.clone(),
                }),
                qos: live_qos(http.qos),
                retain: http.retain,
            }),
        };

        // An empty filter is the stored spelling of "no trigger"; see
        // [`RuleRecord::trigger`]. Anything else has to be a filter,
        // and `Rule::new` decides whether this action may have one.
        let trigger = if record.trigger.filter.is_empty() {
            None
        } else {
            let mut trigger = Trigger::new(record.trigger.filter.clone())?;
            trigger.min_qos = record.trigger.min_qos.map(live_qos);
            trigger.client_id = record
                .trigger
                .client_id
                .as_ref()
                .map(|pattern| ClientPattern::new(pattern.clone()));
            Some(trigger)
        };

        let mut rule = Self::new(
            RuleId::new(record.id.clone())?,
            record.name.clone(),
            trigger,
            action,
        )?;
        rule.enabled = record.enabled;
        Ok(rule)
    }
}

/// A live level as the store spells it.
fn stored_qos(qos: QoS) -> Qos {
    match qos {
        QoS::AtMostOnce => Qos::AtMostOnce,
        QoS::AtLeastOnce => Qos::AtLeastOnce,
        QoS::ExactlyOnce => Qos::ExactlyOnce,
    }
}

/// A stored level as the broker spells it.
fn live_qos(qos: Qos) -> QoS {
    match qos {
        Qos::AtMostOnce => QoS::AtMostOnce,
        Qos::AtLeastOnce => QoS::AtLeastOnce,
        Qos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http_out() -> Action {
        Action::HttpOut(HttpOut {
            url: "https://example.invalid/hook".to_owned(),
            headers: vec![("x-token".to_owned(), "secret".to_owned())],
            ..HttpOut::default()
        })
    }

    fn rule() -> Rule {
        Rule::new(
            RuleId::new("telemetry").expect("a plain word is an identifier"),
            "Telemetry out",
            Some(Trigger::new("sensor/+/temp").expect("a valid filter")),
            http_out(),
        )
        .expect("a named rule")
    }

    #[test]
    fn an_identifier_must_fit_in_one_topic_level() {
        // The dead-letter topic is `$sd/rules/<id>/dlq`, so anything that
        // is not one level makes that topic something other than this
        // rule's.
        for bad in ["", "a/b", "a+b", "a#", "a\u{0}b"] {
            assert!(
                RuleId::new(bad).is_err(),
                "{bad:?} should not be an identifier"
            );
        }
        assert!(RuleId::new("telemetry-out_1").is_ok());
    }

    #[test]
    fn the_dead_letter_topic_is_the_rule_s_own() {
        let id = RuleId::new("telemetry").expect("an identifier");
        assert_eq!(id.dead_letter_topic(), "$sd/rules/telemetry/dlq");
    }

    #[test]
    fn a_trigger_refuses_something_that_is_not_a_filter() {
        assert!(Trigger::new("a/#/b").is_err(), "a hash must be last");
        assert!(Trigger::new("a/+/b").is_ok());
    }

    #[test]
    fn a_trigger_matches_on_the_filter() {
        let trigger = Trigger::new("sensor/+/temp").expect("a filter");
        assert!(trigger.matches("sensor/1/temp", QoS::AtMostOnce, "anyone"));
        assert!(!trigger.matches("sensor/1/humidity", QoS::AtMostOnce, "anyone"));
    }

    #[test]
    fn a_minimum_level_narrows_it() {
        let trigger = Trigger::new("a")
            .expect("a filter")
            .at_least(QoS::AtLeastOnce);
        assert!(!trigger.matches("a", QoS::AtMostOnce, "c"));
        assert!(trigger.matches("a", QoS::AtLeastOnce, "c"));
        assert!(trigger.matches("a", QoS::ExactlyOnce, "c"));
    }

    #[test]
    fn a_client_pattern_narrows_it() {
        let trigger = Trigger::new("a")
            .expect("a filter")
            .from_clients(ClientPattern::new("sensor-*"));
        assert!(trigger.matches("a", QoS::AtMostOnce, "sensor-7"));
        assert!(!trigger.matches("a", QoS::AtMostOnce, "gateway-7"));
    }

    #[test]
    fn a_client_pattern_is_a_glob_and_nothing_else() {
        let cases: &[(&str, &str, bool)] = &[
            ("sensor", "sensor", true),
            ("sensor", "sensor-1", false),
            ("*", "", true),
            ("*", "anything", true),
            ("", "", true),
            ("", "anything", false),
            ("sensor-*", "sensor-1", true),
            ("sensor-*", "sensor-", true),
            ("sensor-*", "sensor", false),
            ("*-temp", "sensor-temp", true),
            ("*-temp", "sensor-temp-x", false),
            ("a*b", "ab", true),
            ("a*b", "axyb", true),
            ("a*b", "axbyb", true),
            ("a*b", "axby", false),
            ("a*b*c", "a1b2c", true),
            ("a*b*c", "abc", true),
            ("a*b*c", "acb", false),
            // A literal asterisk cannot be expressed, and the pattern
            // that looks as though it should is a wildcard.
            ("*", "*", true),
        ];
        for (pattern, client_id, expected) in cases {
            assert_eq!(
                ClientPattern::new(*pattern).matches(client_id),
                *expected,
                "{pattern:?} against {client_id:?}"
            );
        }
    }

    #[test]
    fn a_disabled_rule_fires_on_nothing() {
        let mut rule = rule();
        assert!(rule.fires_on("sensor/1/temp", QoS::AtMostOnce, "c"));
        rule.enabled = false;
        assert!(!rule.fires_on("sensor/1/temp", QoS::AtMostOnce, "c"));
    }

    #[test]
    fn a_rule_must_be_called_something() {
        let outcome = Rule::new(
            RuleId::new("r").expect("an identifier"),
            "",
            Some(Trigger::new("a").expect("a filter")),
            http_out(),
        );
        assert_eq!(outcome.unwrap_err(), RuleError::NoName);
    }

    #[test]
    fn a_rule_round_trips_through_its_record() {
        let mut original = rule();
        original.enabled = false;
        original.trigger = original.trigger.clone().map(|trigger| {
            trigger
                .at_least(QoS::ExactlyOnce)
                .from_clients(ClientPattern::new("sensor-*"))
        });

        let rebuilt = Rule::try_from(&original.to_record()).expect("our own record must rebuild");
        assert_eq!(rebuilt, original);
    }

    /// An ingest rule that can be built, for the tests below.
    fn http_in() -> HttpIn {
        HttpIn {
            token: Secret::new("a-secret"),
            topic: "ingest/one".to_owned(),
            topic_from: Some(TopicFromRequest {
                source: TopicSource::Query("t".to_owned()),
                prefix: "ingest".to_owned(),
            }),
            qos: QoS::AtLeastOnce,
            retain: true,
        }
    }

    fn ingest_rule() -> Rule {
        Rule::new(
            RuleId::new("gateway").expect("an identifier"),
            "Gateway in",
            None,
            Action::HttpIn(http_in()),
        )
        .expect("a named rule")
    }

    /// Section 9.2: the token is not optional.
    ///
    /// **Refused where a rule is accepted**, which is here, rather than
    /// only where a route is registered. A rule stored without a token
    /// is a rule that will be registered later, and by then the file
    /// that produced it is not what anybody is looking at.
    #[test]
    fn an_ingest_rule_without_a_token_is_refused() {
        let mut action = http_in();
        action.token = Secret::new(String::new());
        let outcome = Rule::new(
            RuleId::new("gateway").expect("an identifier"),
            "Gateway in",
            None,
            Action::HttpIn(action),
        );
        assert_eq!(outcome.unwrap_err(), RuleError::NoToken);
    }

    /// And the same rule, refused on the way back out of the store.
    ///
    /// The store is bytes on a disk and has been outside this process.
    /// A record with an empty token would otherwise come back as a rule
    /// the registry then has to refuse a second time, and this is the
    /// refusal that makes that second one unreachable.
    #[test]
    fn a_stored_ingest_rule_without_a_token_is_refused() {
        let mut record = ingest_rule().to_record();
        let ActionRecord::HttpIn(http) = &mut record.action else {
            panic!("this rule accepts posts");
        };
        http.token = Secret::new(String::new());
        assert_eq!(Rule::try_from(&record).unwrap_err(), RuleError::NoToken);
    }

    /// A trigger belongs to an action that fires on messages, and to no
    /// other.
    ///
    /// Both halves, because one alone passes against a rule that
    /// ignores the pairing entirely.
    #[test]
    fn a_trigger_belongs_to_the_action_that_fires_on_messages() {
        let missing = Rule::new(
            RuleId::new("telemetry").expect("an identifier"),
            "Telemetry out",
            None,
            http_out(),
        );
        assert_eq!(missing.unwrap_err(), RuleError::NoTrigger);

        let spurious = Rule::new(
            RuleId::new("gateway").expect("an identifier"),
            "Gateway in",
            Some(Trigger::new("a/#").expect("a filter")),
            Action::HttpIn(http_in()),
        );
        assert_eq!(spurious.unwrap_err(), RuleError::TriggerOnIngest);
    }

    /// An ingest rule fires on no message, whatever is published.
    #[test]
    fn an_ingest_rule_fires_on_nothing_a_client_publishes() {
        let rule = ingest_rule();
        assert!(!rule.fires_on("ingest/one", QoS::ExactlyOnce, "anyone"));
        assert!(!rule.fires_on("anything", QoS::AtMostOnce, ""));
    }

    #[test]
    fn an_ingest_rule_round_trips_through_its_record() {
        let mut original = ingest_rule();
        original.enabled = false;
        let record = original.to_record();
        assert_eq!(
            record.trigger.filter, "",
            "an ingest rule stores the empty trigger"
        );
        let rebuilt = Rule::try_from(&record).expect("our own record must rebuild");
        assert_eq!(rebuilt, original);
    }

    /// A stored ingest rule carrying a trigger is refused.
    ///
    /// The store's empty filter means "no trigger"; anything else
    /// paired with an action that does not fire on messages is a record
    /// that cannot mean what it says.
    #[test]
    fn a_stored_ingest_rule_with_a_trigger_is_refused() {
        let mut record = ingest_rule().to_record();
        record.trigger.filter = "a/#".to_owned();
        assert_eq!(
            Rule::try_from(&record).unwrap_err(),
            RuleError::TriggerOnIngest
        );
    }

    /// And a stored `http_out` rule with an empty one, from the other
    /// side.
    #[test]
    fn a_stored_rule_that_fires_on_messages_needs_a_filter() {
        let mut record = rule().to_record();
        record.trigger.filter = String::new();
        assert_eq!(Rule::try_from(&record).unwrap_err(), RuleError::NoTrigger);
    }

    /// A token does not print itself, wherever it is held.
    ///
    /// Section 12.3. The whole rule is logged at startup and printed by
    /// a failing storage test, and an ingest rule's token is a secret
    /// that lives in one.
    #[test]
    fn an_ingest_rule_does_not_print_its_token() {
        let rule = Rule::new(
            RuleId::new("gateway").expect("an identifier"),
            "Gateway in",
            None,
            Action::HttpIn(HttpIn {
                token: Secret::new("hunter2"),
                ..http_in()
            }),
        )
        .expect("a named rule");
        for shown in [format!("{rule:?}"), format!("{:?}", rule.to_record())] {
            assert!(
                !shown.contains("hunter2"),
                "section 12.3: a token must not reach a log line: {shown}"
            );
        }
    }

    #[test]
    fn a_stored_rule_is_validated_on_the_way_back_in() {
        // The store is bytes on a disk and has been outside this process.
        let mut record = rule().to_record();
        record.id = "a/b".to_owned();
        assert!(matches!(
            Rule::try_from(&record),
            Err(RuleError::BadIdentifier(_))
        ));

        let mut record = rule().to_record();
        record.trigger.filter = "a/#/b".to_owned();
        assert!(matches!(
            Rule::try_from(&record),
            Err(RuleError::BadFilter(_))
        ));
    }
}
