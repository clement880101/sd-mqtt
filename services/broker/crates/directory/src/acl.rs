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

use broker_core::hooks::{ClientInfo, Decision, Hook};
use broker_core::session::message::Message;
use mqtt_codec::QoS;
use std::sync::Arc;
use storage::record::AclRecord;

/// Whom a rule is about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subject {
    /// Every connection.
    Anyone,
    /// Connections authenticated as this user.
    User(String),
    /// Connections with this client identifier.
    Client(String),
}

impl Subject {
    /// Parse the stored form: `anyone`, `user:<name>` or `client:<id>`.
    ///
    /// # Errors
    ///
    /// Anything else, as a sentence for the configuration or the API.
    pub fn parse(text: &str) -> Result<Self, String> {
        if text == "anyone" {
            return Ok(Self::Anyone);
        }
        if let Some(name) = text.strip_prefix("user:") {
            if name.is_empty() {
                return Err("user: needs a name after the colon".to_owned());
            }
            return Ok(Self::User(name.to_owned()));
        }
        if let Some(id) = text.strip_prefix("client:") {
            if id.is_empty() {
                return Err("client: needs an identifier after the colon".to_owned());
            }
            return Ok(Self::Client(id.to_owned()));
        }
        Err("a subject is `anyone`, `user:<name>` or `client:<id>`".to_owned())
    }

    fn covers(&self, client: &ClientInfo<'_>) -> bool {
        match self {
            Self::Anyone => true,
            Self::User(name) => client.username == Some(name.as_str()),
            Self::Client(id) => client.client_id == id,
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

    #[test]
    fn subjects_parse_and_refuse_the_rest() {
        assert_eq!(Subject::parse("anyone"), Ok(Subject::Anyone));
        assert_eq!(
            Subject::parse("user:alice"),
            Ok(Subject::User("alice".to_owned()))
        );
        assert_eq!(
            Subject::parse("client:gw"),
            Ok(Subject::Client("gw".to_owned()))
        );
        assert!(Subject::parse("user:").is_err());
        assert!(Subject::parse("everyone").is_err());
    }
}
