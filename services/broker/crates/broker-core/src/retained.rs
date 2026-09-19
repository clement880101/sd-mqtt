//! Retained message storage.

use std::collections::HashMap;

use bytes::Bytes;
use foldhash::fast::RandomState;
use mqtt_codec::QoS;
use parking_lot::RwLock;

/// A message held for future subscribers to a topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedMessage {
    /// The payload. An empty payload is a deletion, not a message.
    pub payload: Bytes,
    /// The quality of service it was published at.
    pub qos: QoS,
    /// A coarse deadline in seconds, past which it is discarded.
    pub expires_at: Option<u32>,
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

    fn count(&self) -> usize {
        usize::from(self.message.is_some()) + self.children.values().map(Self::count).sum::<usize>()
    }

    fn nodes(&self) -> usize {
        1 + self.children.values().map(Self::nodes).sum::<usize>()
    }

    /// Collect this node's message and every descendant's, which is what a
    /// multi-level wildcard means.
    fn collect_subtree(&self, prefix: &mut Vec<String>, out: &mut Vec<(String, RetainedMessage)>) {
        if let Some(message) = &self.message {
            out.push((prefix.join("/"), message.clone()));
        }
        for (level, child) in &self.children {
            prefix.push(level.to_string());
            child.collect_subtree(prefix, out);
            prefix.pop();
        }
    }

    fn matching(
        &self,
        filter: &[&str],
        prefix: &mut Vec<String>,
        at_root: bool,
        out: &mut Vec<(String, RetainedMessage)>,
    ) {
        let Some((level, rest)) = filter.split_first() else {
            if let Some(message) = &self.message {
                out.push((prefix.join("/"), message.clone()));
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
                    child.collect_subtree(prefix, out);
                    prefix.pop();
                }
                // A multi-level wildcard also matches the parent level, but
                // never the root, where it would name the empty topic.
                if !at_root {
                    if let Some(message) = &self.message {
                        out.push((prefix.join("/"), message.clone()));
                    }
                }
            }
            "+" => {
                for (child_level, child) in &self.children {
                    if at_root && child_level.starts_with('$') {
                        continue;
                    }
                    prefix.push(child_level.to_string());
                    child.matching(rest, prefix, false, out);
                    prefix.pop();
                }
            }
            literal => {
                if let Some(child) = self.children.get(literal) {
                    prefix.push(literal.to_string());
                    child.matching(rest, prefix, false, out);
                    prefix.pop();
                }
            }
        }
    }

    fn sweep(&mut self, now: u32) -> usize {
        let mut removed = 0;
        if let Some(message) = &self.message {
            if message.expires_at.is_some_and(|deadline| now > deadline) {
                self.message = None;
                removed += 1;
            }
        }
        self.children.retain(|_, child| {
            removed += child.sweep(now);
            !child.is_empty()
        });
        removed
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
    pub fn matching(&self, filter: &str, out: &mut Vec<(String, RetainedMessage)>) {
        let levels: Vec<&str> = filter.split('/').collect();
        let mut prefix = Vec::new();
        self.root.read().matching(&levels, &mut prefix, true, out);
    }

    /// Discard every message whose deadline has passed. Returns how many
    /// went. A message expires strictly after its deadline.
    pub fn sweep_expired(&self, now: u32) -> usize {
        self.root.write().sweep(now)
    }

    /// How many messages are retained.
    pub fn len(&self) -> usize {
        self.root.read().count()
    }

    /// Whether nothing is retained.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
    use mqtt_codec::QoS;

    fn message(body: &'static str) -> RetainedMessage {
        RetainedMessage {
            payload: Bytes::from_static(body.as_bytes()),
            qos: QoS::AtMostOnce,
            expires_at: None,
        }
    }

    fn topics(store: &RetainedStore, filter: &str) -> Vec<String> {
        let mut out = Vec::new();
        store.matching(filter, &mut out);
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
        store.matching("a/b", &mut out);
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

        assert_eq!(store.sweep_expired(101), 1);
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
            store.sweep_expired(100),
            0,
            "the deadline itself is not past"
        );
        assert_eq!(store.len(), 1);
    }
}
