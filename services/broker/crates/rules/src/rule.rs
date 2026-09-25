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
use storage::record::{ActionRecord, HttpOutRecord, Qos, RuleRecord, TriggerRecord};

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
/// One action, section 9.1. An enum of one today: `http_out` is the
/// action milestone 4 builds, and the others section 9.2 names are later
/// milestones' variants of this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Post a JSON envelope to a URL. Section 9.2.
    HttpOut(HttpOut),
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
    /// When it fires.
    pub trigger: Trigger,
    /// What it does. One, section 9.1.
    pub action: Action,
}

impl Rule {
    /// A rule.
    ///
    /// # Errors
    ///
    /// [`RuleError::NoName`] when the name is empty.
    pub fn new(
        id: RuleId,
        name: impl Into<String>,
        trigger: Trigger,
        action: Action,
    ) -> Result<Self, RuleError> {
        let name = name.into();
        if name.is_empty() {
            return Err(RuleError::NoName);
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
    /// A disabled rule fires on nothing. Asked here rather than by every
    /// caller, because a caller that forgot would run a rule an operator
    /// switched off.
    pub fn fires_on(&self, topic: &str, qos: QoS, client_id: &str) -> bool {
        self.enabled && self.trigger.matches(topic, qos, client_id)
    }

    /// This rule as the store holds it.
    pub fn to_record(&self) -> RuleRecord {
        RuleRecord {
            id: self.id.as_str().to_owned(),
            name: self.name.clone(),
            enabled: self.enabled,
            trigger: TriggerRecord {
                filter: self.trigger.filter().to_owned(),
                min_qos: self.trigger.min_qos.map(stored_qos),
                client_id: self
                    .trigger
                    .client_id
                    .as_ref()
                    .map(|pattern| pattern.as_str().to_owned()),
            },
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
        let mut trigger = Trigger::new(record.trigger.filter.clone())?;
        trigger.min_qos = record.trigger.min_qos.map(live_qos);
        trigger.client_id = record
            .trigger
            .client_id
            .as_ref()
            .map(|pattern| ClientPattern::new(pattern.clone()));

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
            Trigger::new("sensor/+/temp").expect("a valid filter"),
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
            Trigger::new("a").expect("a filter"),
            http_out(),
        );
        assert_eq!(outcome.unwrap_err(), RuleError::NoName);
    }

    #[test]
    fn a_rule_round_trips_through_its_record() {
        let mut original = rule();
        original.enabled = false;
        original.trigger = original
            .trigger
            .clone()
            .at_least(QoS::ExactlyOnce)
            .from_clients(ClientPattern::new("sensor-*"));

        let rebuilt = Rule::try_from(&original.to_record()).expect("our own record must rebuild");
        assert_eq!(rebuilt, original);
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
