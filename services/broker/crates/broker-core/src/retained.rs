//! Retained message storage.
//!
//! # Precondition
//!
//! Every topic and filter handed to this module must already have been
//! validated by [`mqtt_codec::topic`]. Traversal recurses once per level,
//! so depth is stack depth, and nothing here bounds it: the bound is
//! [`mqtt_codec::topic::MAX_TOPIC_DEPTH`], enforced at validation. An
//! unvalidated topic of sufficient depth aborts the process rather than
//! returning an error.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use foldhash::fast::RandomState;
use mqtt_codec::QoS;
use parking_lot::RwLock;

use crate::properties::SharedProperties;

/// A message held for future subscribers to a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedMessage {
    /// The payload. An empty payload is a deletion, not a message.
    pub payload: Bytes,
    /// The quality of service it was published at.
    pub qos: QoS,
    /// A coarse deadline in seconds, past which it is discarded.
    pub expires_at: Option<u32>,
    /// The properties the message was published with, part of the stored
    /// Application Message: [MQTT-3.3.2-17] requires User Properties to be
    /// forwarded unaltered, and the Content Type and the rest travel with
    /// them. Shared rather than owned for the same reason [`Message`]
    /// shares its own, because fan-out clones one retained message per
    /// matching subscriber.
    ///
    /// Two of them are not this store's to hold. Subscription Identifiers
    /// are per subscriber and [`outbound::publish`] overwrites them on
    /// every delivery, so one stored here cannot reach a client. A Topic
    /// Alias is stripped on ingress, which is the single place that rule
    /// lives.
    ///
    /// The Message Expiry Interval stored here is the one the publisher
    /// sent, which is stale the moment it is stored. [`outbound::publish`]
    /// rewrites it from [`Self::expires_at`] on the way out.
    ///
    /// [`Message`]: crate::session::message::Message
    /// [`outbound::publish`]: crate::session::outbound
    pub properties: Arc<SharedProperties>,
}

/// Retained messages, stored by topic and queried by filter.
///
/// This is a separate structure from the subscription trie because the
/// traversal runs the other way: here a filter carrying wildcards is walked
/// against stored topics, rather than a topic being walked against stored
/// filters.
pub struct RetainedStore {
    root: RwLock<RetainedNode>,
}

#[derive(Default)]
struct RetainedNode {
    children: HashMap<Box<str>, RetainedNode, RandomState>,
    message: Option<RetainedMessage>,
}

impl RetainedNode {
    fn is_empty(&self) -> bool {
        self.children.is_empty() && self.message.is_none()
    }

    /// Whether this node or any descendant holds a message. Stops at the
    /// first one found, where `count` walks the whole tree.
    fn holds_any(&self) -> bool {
        self.message.is_some() || self.children.values().any(Self::holds_any)
    }

    fn count(&self) -> usize {
        usize::from(self.message.is_some()) + self.children.values().map(Self::count).sum::<usize>()
    }

    fn nodes(&self) -> usize {
        1 + self.children.values().map(Self::nodes).sum::<usize>()
    }

    /// Collect this node's message and every descendant's, which is what a
    /// multi-level wildcard means.
    fn collect_subtree(
        &self,
        now: u32,
        prefix: &mut Vec<String>,
        out: &mut Vec<(String, RetainedMessage)>,
    ) {
        if let Some(message) = &self.message {
            if !is_expired(message, now) {
                out.push((prefix.join("/"), message.clone()));
            }
        }
        for (level, child) in &self.children {
            prefix.push(level.to_string());
            child.collect_subtree(now, prefix, out);
            prefix.pop();
        }
    }

    fn matching(
        &self,
        filter: &[&str],
        now: u32,
        prefix: &mut Vec<String>,
        at_root: bool,
        out: &mut Vec<(String, RetainedMessage)>,
    ) {
        let Some((level, rest)) = filter.split_first() else {
            if let Some(message) = &self.message {
                if !is_expired(message, now) {
                    out.push((prefix.join("/"), message.clone()));
                }
            }
            return;
        };

        match *level {
            "#" => {
                for (child_level, child) in &self.children {
                    // A leading wildcard must not reach a dollar topic.
                    if at_root && child_level.starts_with('$') {
                        continue;
                    }
                    prefix.push(child_level.to_string());
                    child.collect_subtree(now, prefix, out);
                    prefix.pop();
                }
                // A multi-level wildcard also matches the parent level, but
                // never the root, where it would name the empty topic.
                if !at_root {
                    if let Some(message) = &self.message {
                        if !is_expired(message, now) {
                            out.push((prefix.join("/"), message.clone()));
                        }
                    }
                }
            }
            "+" => {
                for (child_level, child) in &self.children {
                    if at_root && child_level.starts_with('$') {
                        continue;
                    }
                    prefix.push(child_level.to_string());
                    child.matching(rest, now, prefix, false, out);
                    prefix.pop();
                }
            }
            literal => {
                if let Some(child) = self.children.get(literal) {
                    prefix.push(literal.to_string());
                    child.matching(rest, now, prefix, false, out);
                    prefix.pop();
                }
            }
        }
    }

    /// Collect every message held, expired or not, with its topic.
    fn collect_all(&self, prefix: &mut Vec<String>, out: &mut Vec<(String, RetainedMessage)>) {
        if let Some(message) = &self.message {
            out.push((prefix.join("/"), message.clone()));
        }
        for (level, child) in &self.children {
            prefix.push(level.to_string());
            child.collect_all(prefix, out);
            prefix.pop();
        }
    }

    fn sweep(&mut self, now: u32, prefix: &mut Vec<String>, removed: &mut Vec<String>) {
        if let Some(message) = &self.message {
            if is_expired(message, now) {
                self.message = None;
                removed.push(prefix.join("/"));
            }
        }
        // `retain` cannot borrow `prefix` mutably from inside the
        // closure while the loop needs it, so the levels are walked and
        // the empty branches pruned in two passes rather than one.
        for (level, child) in self.children.iter_mut() {
            prefix.push(level.to_string());
            child.sweep(now, prefix, removed);
            prefix.pop();
        }
        self.children.retain(|_, child| !child.is_empty());
    }
}

impl RetainedStore {
    /// An empty store.
    pub fn new() -> Self {
        Self {
            root: RwLock::new(RetainedNode::default()),
        }
    }

    /// Store a retained message, or delete the topic's message when the
    /// payload is empty.
    ///
    /// The empty-payload rule is the protocol's, not a convenience: an
    /// empty retained publish clears the topic and is not itself stored.
    pub fn set(&self, topic: &str, message: RetainedMessage) {
        if message.payload.is_empty() {
            self.remove(topic);
            return;
        }
        let mut guard = self.root.write();
        let mut node = &mut *guard;
        for level in topic.split('/') {
            node = node.children.entry(Box::from(level)).or_default();
        }
        node.message = Some(message);
    }

    /// Delete the message retained under `topic`, pruning any branch it
    /// leaves empty.
    pub fn remove(&self, topic: &str) {
        let levels: Vec<&str> = topic.split('/').collect();
        remove_at(&mut self.root.write(), &levels);
    }

    /// Append every retained message whose topic matches `filter` to `out`,
    /// each with its topic.
    ///
    /// A filter whose first level is a wildcard does not reach a topic
    /// whose first level begins with a dollar sign, the same rule
    /// [`SubscriptionTrie::matching`] documents. The traversal runs the
    /// other way here, so the rule is enforced differently: the root's
    /// wildcard arms skip dollar children, rather than a dollar topic
    /// skipping the wildcard root.
    ///
    /// [`SubscriptionTrie::matching`]: crate::subscriptions::SubscriptionTrie::matching
    ///
    /// `now` is the current coarse time in seconds. A message whose
    /// deadline has passed is skipped whether or not a sweep has run, so a
    /// reader never sees an expired message; [`Self::sweep_expired`]
    /// reclaims the memory but is not what enforces the deadline. Both use
    /// the same strictly-after comparison, so they never disagree about
    /// whether one particular message is still live.
    pub fn matching(&self, filter: &str, now: u32, out: &mut Vec<(String, RetainedMessage)>) {
        let levels: Vec<&str> = filter.split('/').collect();
        let mut prefix = Vec::new();
        self.root
            .read()
            .matching(&levels, now, &mut prefix, true, out);
    }

    /// Discard every message whose deadline has passed, appending each
    /// one's topic to `removed`. Returns how many went. A message
    /// expires strictly after its deadline.
    ///
    /// The topics are reported rather than counted because a retained
    /// message reclaimed here is a durable change: the store holds it
    /// until something says it has gone, and nothing else ever will.
    /// Counting alone would leave every expired message on disk for the
    /// life of the file, invisible because every reader skips it.
    pub fn sweep_expired(&self, now: u32, removed: &mut Vec<String>) -> usize {
        let before = removed.len();
        let mut prefix = Vec::new();
        self.root.write().sweep(now, &mut prefix, removed);
        removed.len() - before
    }

    /// Every message held, with its topic, expired or not.
    ///
    /// Deliberately including the expired: this reports what is
    /// *stored*, which is the question a comparison against a restored
    /// broker asks. [`Self::matching`] answers the other question, what
    /// a reader may see, and skips them.
    pub fn entries(&self, out: &mut Vec<(String, RetainedMessage)>) {
        let mut prefix = Vec::new();
        self.root.read().collect_all(&mut prefix, out);
    }

    /// How many messages are retained.
    pub fn len(&self) -> usize {
        self.root.read().count()
    }

    /// Whether nothing is retained.
    ///
    /// Stops at the first message found rather than walking the whole
    /// tree, because the name reads as cheap and callers will treat it
    /// that way. Expiry is not considered: a message past its deadline
    /// that no sweep has reclaimed still counts as retained here, while
    /// [`matching`] will not return it.
    ///
    /// [`matching`]: Self::matching
    pub fn is_empty(&self) -> bool {
        !self.root.read().holds_any()
    }

    /// How many nodes the tree holds, including the root. Exposed so that
    /// pruning can be tested rather than assumed.
    pub fn node_count(&self) -> usize {
        self.root.read().nodes()
    }
}

impl Default for RetainedStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Whether `message`'s deadline has passed at `now`.
///
/// Strictly after: a message whose deadline is exactly `now` is still
/// live. Both reading and sweeping go through here so that the two can
/// never disagree about one message.
fn is_expired(message: &RetainedMessage, now: u32) -> bool {
    message.expires_at.is_some_and(|deadline| now > deadline)
}

fn remove_at(node: &mut RetainedNode, levels: &[&str]) -> bool {
    let Some((level, rest)) = levels.split_first() else {
        let had = node.message.is_some();
        node.message = None;
        return had;
    };

    let Some(child) = node.children.get_mut(*level) else {
        return false;
    };
    let removed = remove_at(child, rest);
    if removed && child.is_empty() {
        node.children.remove(*level);
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mqtt_codec::{Properties, QoS};
    use std::sync::Arc;

    fn message(body: &'static str) -> RetainedMessage {
        RetainedMessage {
            payload: Bytes::from_static(body.as_bytes()),
            qos: QoS::AtMostOnce,
            expires_at: None,
            properties: Arc::new(Properties::default().into()),
        }
    }

    fn topics(store: &RetainedStore, filter: &str) -> Vec<String> {
        topics_at(store, filter, 0)
    }

    fn topics_at(store: &RetainedStore, filter: &str, now: u32) -> Vec<String> {
        let mut out = Vec::new();
        store.matching(filter, now, &mut out);
        let mut names: Vec<String> = out.into_iter().map(|(topic, _)| topic).collect();
        names.sort();
        names
    }

    #[test]
    fn stores_and_retrieves_by_exact_topic() {
        let store = RetainedStore::new();
        store.set("a/b", message("hello"));
        assert_eq!(topics(&store, "a/b"), vec!["a/b".to_string()]);
        assert!(topics(&store, "a/c").is_empty());
    }

    #[test]
    fn an_empty_payload_deletes_rather_than_storing() {
        let store = RetainedStore::new();
        store.set("a/b", message("hello"));
        assert_eq!(store.len(), 1);

        store.set(
            "a/b",
            RetainedMessage {
                payload: Bytes::new(),
                ..message("")
            },
        );
        assert_eq!(store.len(), 0, "an empty payload must delete");
        assert!(topics(&store, "a/b").is_empty());
    }

    #[test]
    fn an_empty_payload_for_an_absent_topic_is_harmless() {
        let store = RetainedStore::new();
        store.set(
            "a/b",
            RetainedMessage {
                payload: Bytes::new(),
                ..message("")
            },
        );
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn setting_again_replaces() {
        let store = RetainedStore::new();
        store.set("a/b", message("first"));
        store.set("a/b", message("second"));
        assert_eq!(store.len(), 1);

        let mut out = Vec::new();
        store.matching("a/b", 0, &mut out);
        assert_eq!(&out[0].1.payload[..], b"second");
    }

    #[test]
    fn matches_single_and_multi_level_wildcards() {
        let store = RetainedStore::new();
        for topic in ["a", "a/b", "a/b/c", "a/x", "b/b"] {
            store.set(topic, message("v"));
        }

        assert_eq!(topics(&store, "a/+"), vec!["a/b".to_string(), "a/x".into()]);
        assert_eq!(
            topics(&store, "a/#"),
            vec!["a".to_string(), "a/b".into(), "a/b/c".into(), "a/x".into()],
            "a multi-level wildcard includes the parent level"
        );
        assert_eq!(topics(&store, "#").len(), 5);
        assert_eq!(topics(&store, "+"), vec!["a".to_string()]);
    }

    #[test]
    fn a_leading_wildcard_never_matches_a_dollar_topic() {
        let store = RetainedStore::new();
        store.set("$SYS/uptime", message("v"));
        store.set("ordinary", message("v"));

        assert_eq!(topics(&store, "#"), vec!["ordinary".to_string()]);
        assert_eq!(topics(&store, "+"), vec!["ordinary".to_string()]);
        assert_eq!(topics(&store, "$SYS/#"), vec!["$SYS/uptime".to_string()]);
    }

    #[test]
    fn deleting_prunes_empty_branches() {
        let store = RetainedStore::new();
        store.set("a/b/c", message("v"));
        store.remove("a/b/c");
        assert_eq!(store.len(), 0);
        assert_eq!(store.node_count(), 1, "only the root should remain");
    }

    #[test]
    fn an_expired_message_is_neither_returned_nor_kept() {
        let store = RetainedStore::new();
        store.set(
            "a/b",
            RetainedMessage {
                expires_at: Some(100),
                ..message("v")
            },
        );
        store.set("a/c", message("v"));

        assert!(topics(&store, "a/+").contains(&"a/b".to_string()));

        assert_eq!(store.sweep_expired(101, &mut Vec::new()), 1);
        assert_eq!(topics(&store, "a/+"), vec!["a/c".to_string()]);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn a_sweep_before_the_deadline_keeps_the_message() {
        let store = RetainedStore::new();
        store.set(
            "a/b",
            RetainedMessage {
                expires_at: Some(100),
                ..message("v")
            },
        );
        assert_eq!(
            store.sweep_expired(100, &mut Vec::new()),
            0,
            "the deadline itself is not past"
        );
        assert_eq!(store.len(), 1);
    }

    /// The deadline is enforced on read, not by the sweep. Delivery on
    /// subscribe calls `matching` and may run long after the last sweep,
    /// so an expired message must be invisible with no sweep at all.
    #[test]
    fn an_expired_message_is_absent_from_matching_without_a_sweep() {
        let store = RetainedStore::new();
        store.set(
            "a/b",
            RetainedMessage {
                expires_at: Some(100),
                ..message("v")
            },
        );
        store.set("a/c", message("v"));

        assert_eq!(
            topics_at(&store, "a/+", 100),
            vec!["a/b".to_string(), "a/c".into()]
        );
        assert_eq!(
            topics_at(&store, "a/+", 101),
            vec!["a/c".to_string()],
            "past the deadline it must not be returned"
        );
        assert_eq!(store.len(), 2, "but nothing has been reclaimed yet");
    }

    /// Every path into the tree applies the deadline, not only the exact
    /// one: the subtree walk a multi-level wildcard takes, and the parent
    /// message a multi-level wildcard also matches.
    #[test]
    fn expiry_is_applied_on_every_traversal_path() {
        let store = RetainedStore::new();
        for topic in ["a", "a/b", "a/b/c"] {
            store.set(
                topic,
                RetainedMessage {
                    expires_at: Some(100),
                    ..message("v")
                },
            );
        }

        assert_eq!(topics_at(&store, "a/#", 100).len(), 3);
        assert!(
            topics_at(&store, "a/#", 101).is_empty(),
            "the subtree walk and the parent level both apply it"
        );
        assert!(topics_at(&store, "+", 101).is_empty());
        assert!(topics_at(&store, "a/b", 101).is_empty());
        assert!(topics_at(&store, "a/+", 101).is_empty());
    }

    /// A helper beside the test rather than in `message`, so that every
    /// other test keeps saying "no properties" by construction.
    fn retained_with(body: &'static str, properties: &Properties) -> RetainedMessage {
        RetainedMessage {
            properties: Arc::new(properties.clone().into()),
            ..message(body)
        }
    }

    #[test]
    fn a_retained_message_keeps_its_properties() {
        let store = RetainedStore::new();
        let properties = Properties {
            content_type: Some("text/plain".to_string()),
            user_properties: vec![("a".to_string(), "2".to_string())],
            ..Properties::default()
        };
        store.set("retain/probe", retained_with("body", &properties));

        let mut found = Vec::new();
        store.matching("retain/probe", 0, &mut found);

        let (_, message) = found.pop().expect("the message is there");
        assert_eq!(
            message.properties.user_properties,
            vec![("a".to_string(), "2".to_string())],
            "[MQTT-3.3.2-17]: user properties are forwarded unaltered"
        );
        assert_eq!(
            message.properties.content_type.as_deref(),
            Some("text/plain")
        );
    }
}
