//! Access control rules: who may publish and subscribe where. Section
//! 12.1.
//!
//! # Deny by default once any rule exists
//!
//! With no rule at all, every publish and subscribe is allowed, which is
//! what the broker did through milestone 6 and what the conformance
//! suite's plain pass relies on. The first rule flips the default: from
//! then on an operation is allowed only by a rule that says so. The page
//! says this above the table, because an operator who writes one rule
//! and locks everyone else out should have been told.
//!
//! # First match decides
//!
//! Rules are consulted in `order`, then by identifier, so two nodes
//! holding the same set consult it the same way. The first rule whose
//! subject and filter match and that has an opinion on the operation
//! decides; a rule with `None` for that operation is skipped. This is
//! the shape every broker's operator already knows, and it lets a
//! narrow deny sit above a broad allow.
//!
//! # Matching is the topic crate's
//!
//! A rule's filter matches a published topic the way a subscription
//! does, wildcards and all. A requested subscription filter is matched
//! against the rule's filter as a topic name would be — `fleet/+/temp`
//! is covered by a rule for `fleet/#`, and a request for `#` is covered
//! by a rule for `#` and by nothing narrower.
//!
//! # Subjects are names or prefixes
//!
//! Section 12.1 maps "a user or client pattern" to filters. The pattern
//! is a name, or a prefix ending in `*`: `client:sensor-*` is every
//! client whose identifier starts `sensor-`, and `user:*` is every
//! authenticated connection — which is the one way to tell a logged-in
//! client from an anonymous one in a rule. A `*` anywhere else is
//! refused rather than read as something: two kinds of wildcard in one
//! word is a grammar an operator has to guess at.

use broker_core::hooks::{ClientInfo, Decision, Hook};
use broker_core::session::message::Message;
use mqtt_codec::QoS;
use std::sync::Arc;
use storage::record::AclRecord;

/// A name, or every name with a prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pattern {
    /// Exactly this.
    Exactly(String),
    /// Anything that starts with this, including this.
    Prefix(String),
}

impl Pattern {
    fn parse(text: &str, what: &str) -> Result<Self, String> {
        if text.is_empty() {
            return Err(format!("{what} needs a name after the colon"));
        }
        let (stem, open) = match text.strip_suffix('*') {
            Some(stem) => (stem, true),
            None => (text, false),
        };
        if stem.contains('*') {
            return Err(format!(
                "{what} takes a `*` only at the end, meaning any name that starts with what \
                 comes before it"
            ));
        }
        Ok(if open {
            Self::Prefix(stem.to_owned())
        } else {
            Self::Exactly(stem.to_owned())
        })
    }

    fn matches(&self, name: &str) -> bool {
        match self {
            Self::Exactly(exact) => name == exact,
            Self::Prefix(prefix) => name.starts_with(prefix.as_str()),
        }
    }
}

/// Whom a rule is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subject {
    /// Every connection.
    Anyone,
    /// Connections authenticated as a user the pattern names.
    User(Pattern),
    /// Connections whose client identifier the pattern names.
    Client(Pattern),
}

impl Subject {
    /// Parse the stored form: `anyone`, `user:<name>` or `client:<id>`,
    /// where a name may end in `*`.
    ///
    /// # Errors
    ///
    /// Anything else, as a sentence for the configuration or the API.
    pub fn parse(text: &str) -> Result<Self, String> {
        if text == "anyone" {
            return Ok(Self::Anyone);
        }
        if let Some(name) = text.strip_prefix("user:") {
            return Pattern::parse(name, "user:").map(Self::User);
        }
        if let Some(id) = text.strip_prefix("client:") {
            return Pattern::parse(id, "client:").map(Self::Client);
        }
        Err(
            "a subject is `anyone`, `user:<name>` or `client:<id>`, and a name may end in `*`"
                .to_owned(),
        )
    }

    fn covers(&self, client: &ClientInfo<'_>) -> bool {
        match self {
            Self::Anyone => true,
            Self::User(pattern) => client.username.is_some_and(|name| pattern.matches(name)),
            Self::Client(pattern) => pattern.matches(client.client_id),
        }
    }
}

/// Which operation a rule is being asked about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// Publishing to a topic.
    Publish,
    /// Subscribing to a filter.
    Subscribe,
}

/// The answer the rules give, as the hook chain hears it.
///
/// `None` when there are no rules at all: the chain goes on and the
/// configured default applies.
#[must_use]
pub fn decide(
    rules: &[AclRecord],
    client: &ClientInfo<'_>,
    operation: Operation,
    topic: &str,
) -> Option<bool> {
    if rules.is_empty() {
        return None;
    }
    for rule in rules {
        let opinion = match operation {
            Operation::Publish => rule.publish,
            Operation::Subscribe => rule.subscribe,
        };
        let Some(opinion) = opinion else {
            continue;
        };
        let Ok(subject) = Subject::parse(&rule.subject) else {
            continue;
        };
        if !subject.covers(client) {
            continue;
        }
        if broker_core::topic::reference::matches(&rule.filter, topic) {
            return Some(opinion);
        }
    }
    Some(false)
}

/// Sort rules the way they are consulted: by order, then identifier.
pub fn in_order(rules: &mut [AclRecord]) {
    rules.sort_by(|a, b| a.order.cmp(&b.order).then_with(|| a.id.cmp(&b.id)));
}

/// Where the hook finds the rules in force: the configured ones and,
/// on a node with a store, the directory's replicated ones.
pub trait RuleSource: Send + Sync + std::fmt::Debug {
    /// Every rule, in consultation order.
    fn rules(&self) -> Vec<AclRecord>;
}

/// The access control hook. Section 9.3's `authorize_publish` and
/// `authorize_subscribe`, answered from the rules.
#[derive(Debug)]
pub struct AclHook {
    source: Arc<dyn RuleSource>,
}

impl AclHook {
    /// A hook over `source`.
    #[must_use]
    pub fn over(source: Arc<dyn RuleSource>) -> Arc<dyn Hook> {
        Arc::new(Self { source })
    }

    fn judge(&self, client: &ClientInfo<'_>, operation: Operation, topic: &str) -> Decision {
        match decide(&self.source.rules(), client, operation, topic) {
            None => Decision::Next,
            Some(true) => Decision::Allow,
            Some(false) => Decision::Deny,
        }
    }
}

impl Hook for AclHook {
    fn name(&self) -> &str {
        "access-control"
    }

    fn authorize_publish(&self, client: &ClientInfo<'_>, message: &Message) -> Decision {
        self.judge(client, Operation::Publish, &message.topic)
    }

    fn authorize_subscribe(&self, client: &ClientInfo<'_>, filter: &str, _qos: QoS) -> Decision {
        // A shared subscription is judged by the filter it shares: a
        // rule about `fleet/#` is a rule about `$share/g/fleet/#` too,
        // and a rule's `#` never covers the dollar the prefix begins
        // with. A malformed prefix was refused by the codec before the
        // hook is asked; here it is judged as written.
        let filter = match mqtt_codec::topic::parse_shared(filter) {
            Ok(Some((_group, inner))) => inner,
            Ok(None) | Err(_) => filter,
        };
        self.judge(client, Operation::Subscribe, filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(
        id: &str,
        order: u32,
        subject: &str,
        filter: &str,
        publish: Option<bool>,
        subscribe: Option<bool>,
    ) -> AclRecord {
        AclRecord {
            id: id.to_owned(),
            order,
            subject: subject.to_owned(),
            filter: filter.to_owned(),
            publish,
            subscribe,
        }
    }

    fn client<'a>(id: &'a str, username: Option<&'a str>) -> ClientInfo<'a> {
        ClientInfo {
            subscriber: None,
            mountpoint: "",
            client_id: id,
            username,
        }
    }

    /// No rules: no opinion, so the default applies.
    #[test]
    fn with_no_rules_the_chain_is_not_answered() {
        assert_eq!(
            decide(&[], &client("c", None), Operation::Publish, "a/b"),
            None
        );
    }

    /// The first rule flips the default to deny.
    #[test]
    fn one_rule_denies_everything_it_does_not_allow() {
        let rules = [rule("r", 1, "anyone", "fleet/#", Some(true), None)];
        let c = client("c", None);
        assert_eq!(
            decide(&rules, &c, Operation::Publish, "fleet/7/temp"),
            Some(true)
        );
        assert_eq!(
            decide(&rules, &c, Operation::Publish, "office/light"),
            Some(false)
        );
        // The rule has no opinion on subscribing, so nothing allows it.
        assert_eq!(
            decide(&rules, &c, Operation::Subscribe, "fleet/#"),
            Some(false)
        );
    }

    /// A narrow deny above a broad allow, by order.
    #[test]
    fn the_first_matching_rule_with_an_opinion_decides() {
        let mut rules = vec![
            rule("all", 100, "anyone", "#", Some(true), Some(true)),
            rule(
                "no-sub",
                10,
                "anyone",
                "test/nosubscribe",
                None,
                Some(false),
            ),
        ];
        in_order(&mut rules);
        let c = client("c", None);
        assert_eq!(
            decide(&rules, &c, Operation::Subscribe, "test/nosubscribe"),
            Some(false)
        );
        assert_eq!(
            decide(&rules, &c, Operation::Subscribe, "test/other"),
            Some(true)
        );
        // The deny has no opinion on publishing, so the allow is asked.
        assert_eq!(
            decide(&rules, &c, Operation::Publish, "test/nosubscribe"),
            Some(true)
        );
    }

    /// Wildcards in the rule's filter match a published topic the way a
    /// subscription would; a string compare would get both wrong.
    #[test]
    fn a_wildcard_filter_covers_matching_topics_and_no_others() {
        let rules = [rule(
            "r",
            1,
            "anyone",
            "fleet/+/telemetry",
            Some(true),
            None,
        )];
        let c = client("c", None);
        assert_eq!(
            decide(&rules, &c, Operation::Publish, "fleet/7/telemetry"),
            Some(true)
        );
        assert_eq!(
            decide(&rules, &c, Operation::Publish, "fleet/7/status"),
            Some(false)
        );
        assert_eq!(
            decide(&rules, &c, Operation::Publish, "fleet/7/8/telemetry"),
            Some(false)
        );
    }

    /// Subjects: a user's rule does not cover an anonymous client, and
    /// a client's rule covers exactly that identifier.
    #[test]
    fn subjects_cover_whom_they_name() {
        let rules = [
            rule("alice", 1, "user:alice", "#", Some(true), Some(true)),
            rule("gw", 2, "client:gateway-1", "fleet/#", Some(true), None),
        ];
        assert_eq!(
            decide(&rules, &client("x", Some("alice")), Operation::Publish, "a"),
            Some(true)
        );
        assert_eq!(
            decide(&rules, &client("x", None), Operation::Publish, "a"),
            Some(false)
        );
        assert_eq!(
            decide(
                &rules,
                &client("gateway-1", None),
                Operation::Publish,
                "fleet/1"
            ),
            Some(true)
        );
        assert_eq!(
            decide(
                &rules,
                &client("gateway-2", None),
                Operation::Publish,
                "fleet/1"
            ),
            Some(false)
        );
        assert_eq!(
            decide(
                &rules,
                &client("gateway-1", None),
                Operation::Subscribe,
                "fleet/1"
            ),
            Some(false)
        );
    }

    /// Section 12.1's patterns: a trailing `*` matches every name with
    /// that prefix, `user:*` is every authenticated connection and not
    /// an anonymous one, and a `*` anywhere else is refused rather than
    /// guessed at.
    #[test]
    fn a_trailing_star_covers_every_name_with_that_prefix() {
        let rules = [
            rule("sensors", 1, "client:sensor-*", "fleet/#", Some(true), None),
            rule("members", 2, "user:*", "members/#", Some(true), None),
        ];
        let cases = [
            (client("sensor-7", None), "fleet/7", Some(true)),
            (client("sensor-", None), "fleet/7", Some(true)),
            (client("sensor", None), "fleet/7", Some(false)),
            (client("gateway-1", None), "fleet/7", Some(false)),
            (client("x", Some("alice")), "members/news", Some(true)),
            (client("x", None), "members/news", Some(false)),
        ];
        for (who, topic, expected) in cases {
            assert_eq!(
                decide(&rules, &who, Operation::Publish, topic),
                expected,
                "{} as {:?} on {topic}",
                who.client_id,
                who.username
            );
        }
        assert!(Subject::parse("client:sen*sor").is_err());
        assert!(Subject::parse("user:**").is_err());
        assert!(Subject::parse("client:*").is_ok());
    }

    #[test]
    fn subjects_parse_and_refuse_the_rest() {
        assert_eq!(Subject::parse("anyone"), Ok(Subject::Anyone));
        assert_eq!(
            Subject::parse("user:alice"),
            Ok(Subject::User(Pattern::Exactly("alice".to_owned())))
        );
        assert_eq!(
            Subject::parse("client:gw"),
            Ok(Subject::Client(Pattern::Exactly("gw".to_owned())))
        );
        assert_eq!(
            Subject::parse("client:gw-*"),
            Ok(Subject::Client(Pattern::Prefix("gw-".to_owned())))
        );
        assert!(Subject::parse("user:").is_err());
        assert!(Subject::parse("everyone").is_err());
    }
}
