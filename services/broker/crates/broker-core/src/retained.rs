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

use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;
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
///
/// # Two views of the same messages
///
/// The tree answers "which messages match this filter", which is what
/// the protocol asks on every subscribe. It cannot answer "the next
/// hundred topics after this one", because its children are hashed and
/// a hash map has no order to resume from. That is the question the
/// Topics page asks, and answering it by collecting the whole store and
/// sorting it held the broker's one lock for 44.9 milliseconds at a
/// hundred thousand topics — on an endpoint section 11.1 deliberately
/// leaves unauthenticated.
///
/// So the topics are kept a second time, in order, in a `BTreeSet`
/// beside the tree. [`Self::page`] seeks into it and takes one page,
/// and [`Self::len`] is its length rather than a walk, which is the two
/// gauges' half of the same problem.
///
/// **Both live under one lock**, not two, so there is no window in
/// which a reader can see a topic in one and not the other. The
/// remaining way they could disagree is a mutation that touches the
/// tree and forgets the order, which is the failure mode a maintained
/// structure has; [`Self::count_by_walking`] is the same number the
/// slow way and `the_retained_index_agrees_with_a_walk` in
/// `tests/journal.rs` asserts the two after every generated history.
pub struct RetainedStore {
    inner: RwLock<Inner>,
}

/// The tree and the order it is enumerated in, behind one lock.
struct Inner {
    root: RetainedNode,
    /// Every topic holding a message, in topic order.
    ///
    /// Exactly the topics the tree holds a message on, expired or not:
    /// [`RetainedStore::len`] counts what is *stored* and so does this.
    /// Its length is therefore the count, which is why there is no
    /// separate counter beside it — a second number is a second thing
    /// to keep in step.
    topics: BTreeSet<Box<str>>,
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

    /// The message held under one exact topic, if any.
    ///
    /// Expiry is not applied: this reports what is stored, which is
    /// what the ordered index indexes.
    fn get(&self, topic: &str) -> Option<&RetainedMessage> {
        let mut node = self;
        for level in topic.split('/') {
            node = node.children.get(level)?;
        }
        node.message.as_ref()
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
            inner: RwLock::new(Inner {
                root: RetainedNode::default(),
                topics: BTreeSet::new(),
            }),
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
        let mut guard = self.inner.write();
        let inner = &mut *guard;
        let mut node = &mut inner.root;
        for level in topic.split('/') {
            node = node.children.entry(Box::from(level)).or_default();
        }
        node.message = Some(message);
        // Idempotent, so replacing a message costs a lookup and no
        // allocation. The order is written here because this is the
        // one place a topic starts holding a message.
        if !inner.topics.contains(topic) {
            inner.topics.insert(Box::from(topic));
        }
    }

    /// Delete the message retained under `topic`, pruning any branch it
    /// leaves empty.
    pub fn remove(&self, topic: &str) {
        let levels: Vec<&str> = topic.split('/').collect();
        let mut guard = self.inner.write();
        let inner = &mut *guard;
        if remove_at(&mut inner.root, &levels) {
            inner.topics.remove(topic);
        }
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
        self.inner
            .read()
            .root
            .matching(&levels, now, &mut prefix, true, out);
    }

    /// Append at most `limit` messages to `out`, in topic order,
    /// starting strictly after `after`. Returns whether any remain
    /// beyond them.
    ///
    /// # Why this is not [`Self::entries`] with a cursor
    ///
    /// It is the same rows in a different order and at a different
    /// cost. `entries` walks the tree, so it does work proportional to
    /// the store however few rows the caller wanted; this seeks the
    /// ordered index and does work proportional to the page. At a
    /// hundred thousand topics that is the difference between 44.9
    /// milliseconds and a few microseconds, on the process's one lock,
    /// on an endpoint that needs no token.
    ///
    /// Strictly after, because `after` is the last topic of the
    /// previous page and returning it again would repeat a row. A
    /// cursor naming a topic that has since been deleted still works:
    /// the range simply resumes at whatever now follows it, which is
    /// the point of a key cursor rather than an offset.
    ///
    /// **Expired messages are included**, as [`Self::entries`]
    /// includes them and [`Self::len`] counts them, so that a page and
    /// the total it is a page of are the same collection. A reader
    /// that must not see one — delivery on subscribe — asks
    /// [`Self::matching`], which applies the deadline.
    pub fn page(
        &self,
        after: Option<&str>,
        limit: usize,
        out: &mut Vec<(String, RetainedMessage)>,
    ) -> bool {
        let guard = self.inner.read();
        let lower = match after {
            Some(after) => Bound::Excluded(after),
            None => Bound::Unbounded,
        };
        let mut topics = guard
            .topics
            .range::<str, _>((lower, Bound::Unbounded))
            .map(|topic| &**topic);
        for topic in topics.by_ref().take(limit) {
            let message = guard
                .root
                .get(topic)
                .expect("the index names only topics the tree holds");
            out.push((topic.to_owned(), message.clone()));
        }
        topics.next().is_some()
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
        let mut guard = self.inner.write();
        let inner = &mut *guard;
        inner.root.sweep(now, &mut prefix, removed);
        // The sweep reports every topic it took, which is exactly what
        // the order has to give up. Driven off the same list the
        // journal is, so the two cannot disagree about which messages
        // went.
        for topic in &removed[before..] {
            inner.topics.remove(topic.as_str());
        }
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
        self.inner.read().root.collect_all(&mut prefix, out);
    }

    /// How many messages are retained.
    ///
    /// The ordered index's length, which is constant time. It was the
    /// tree's own count, measured at 2.66 milliseconds with a hundred
    /// thousand topics — under the broker's one lock, once a second
    /// from the events sampler and again on every metrics scrape.
    /// [`Self::count_by_walking`] is the same number the slow way, for
    /// the property that keeps this one honest.
    ///
    /// Expiry is not considered, and must not be: this counts what is
    /// *stored*, which is the one view that can tell a swept message
    /// from one that is merely invisible. See [`Broker::retained_count`]
    /// for why that distinction is what the number is for.
    ///
    /// [`Broker::retained_count`]: crate::broker::Broker::retained_count
    pub fn len(&self) -> usize {
        self.inner.read().topics.len()
    }

    /// The same, by walking the tree. For the test that keeps the
    /// maintained figure honest, and for nothing else.
    pub fn count_by_walking(&self) -> usize {
        self.inner.read().root.count()
    }

    /// Whether nothing is retained.
    ///
    /// Expiry is not considered: a message past its deadline that no
    /// sweep has reclaimed still counts as retained here, while
    /// [`matching`] will not return it.
    ///
    /// [`matching`]: Self::matching
    pub fn is_empty(&self) -> bool {
        self.inner.read().topics.is_empty()
    }

    /// The same, by walking the tree until it finds one. Beside
    /// [`Self::count_by_walking`] and for the same reason: an index
    /// that has gone empty while the tree has not is a page that
    /// reports nothing over a store that holds everything, and
    /// nothing else here would notice.
    pub fn is_empty_by_walking(&self) -> bool {
        !self.inner.read().root.holds_any()
    }

    /// How many nodes the tree holds, including the root. Exposed so that
    /// pruning can be tested rather than assumed.
    pub fn node_count(&self) -> usize {
        self.inner.read().root.nodes()
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

    // ---------------------------------------------------------------
    // The order beside the tree
    // ---------------------------------------------------------------

    /// One page, as topics.
    fn page(store: &RetainedStore, after: Option<&str>, limit: usize) -> (Vec<String>, bool) {
        let mut out = Vec::new();
        let more = store.page(after, limit, &mut out);
        (out.into_iter().map(|(topic, _)| topic).collect(), more)
    }

    /// Walk every page and prove the walk sees each topic once, in
    /// order, and then stops.
    ///
    /// The boundary is the point, the same one the admin API's own
    /// page walk tests: a store of exactly one page's worth must not
    /// say there is another page, and a client cannot tell an empty
    /// page from a store that shrank underneath it.
    #[test]
    fn a_walk_of_the_pages_visits_every_topic_once_in_order() {
        for count in [0usize, 1, 2, 3, 4, 7] {
            let store = RetainedStore::new();
            // Inserted in an order that is neither the topic order
            // nor its reverse, because a page that came out right
            // only because it was written right proves nothing.
            for index in (0..count).rev().chain(0..count) {
                store.set(&format!("site/{index:02}/state"), message("v"));
            }

            let mut seen = Vec::new();
            let mut cursor: Option<String> = None;
            let mut pages = 0;
            loop {
                let (here, more) = page(&store, cursor.as_deref(), 3);
                pages += 1;
                assert!(pages <= 10, "the walk does not end at {count} topics");
                assert!(here.len() <= 3, "a page holds at most what was asked");
                if cursor.is_some() {
                    assert!(!here.is_empty(), "a cursor for a page with nothing on it");
                }
                cursor = here.last().cloned();
                seen.extend(here);
                if !more {
                    break;
                }
            }

            let expected: Vec<String> = (0..count)
                .map(|index| format!("site/{index:02}/state"))
                .collect();
            assert_eq!(seen, expected, "with {count} topics and pages of three");
        }
    }

    /// A cursor naming a topic that has since been deleted resumes
    /// after it rather than failing or starting again.
    ///
    /// This is the whole reason the cursor is a key and not an
    /// offset, and it is the case a reader hits in practice: a page
    /// is read, the topic it ended on is cleared, and the next page
    /// is asked for.
    #[test]
    fn a_cursor_naming_a_topic_that_has_gone_resumes_after_it() {
        let store = RetainedStore::new();
        for topic in ["a", "b", "c", "d"] {
            store.set(topic, message("v"));
        }
        store.remove("b");
        assert_eq!(
            page(&store, Some("b"), 10).0,
            vec!["c".to_string(), "d".into()]
        );
    }

    /// Every mutation the store offers, and the order after each one.
    ///
    /// Written as one test over the four paths rather than four,
    /// because what is being asserted is that *none* of them forgets:
    /// a maintained structure fails by one path missing the update,
    /// and a test per path is a list somebody adds to when they
    /// remember to.
    #[test]
    fn every_mutation_keeps_the_order_and_the_tree_agreeing() {
        let store = RetainedStore::new();
        let agree = |what: &str| {
            assert_eq!(
                store.len(),
                store.count_by_walking(),
                "the index and the tree disagree after {what}"
            );
            assert_eq!(
                store.is_empty(),
                store.is_empty_by_walking(),
                "emptiness disagrees after {what}"
            );
            let mut held = Vec::new();
            store.entries(&mut held);
            let mut walked: Vec<String> = held.into_iter().map(|(topic, _)| topic).collect();
            walked.sort();
            assert_eq!(page(&store, None, 1_000).0, walked, "the page after {what}");
        };

        agree("nothing at all");
        store.set("a/b", message("v"));
        store.set("a/c", message("v"));
        store.set("z", message("v"));
        agree("three arrivals");

        store.set("a/b", message("again"));
        agree("a replacement");

        store.set(
            "a/c",
            RetainedMessage {
                payload: Bytes::new(),
                ..message("")
            },
        );
        agree("an empty payload, which deletes");

        store.remove("z");
        agree("a removal");

        store.remove("never/stored");
        agree("a removal of something absent");

        store.set(
            "e/f",
            RetainedMessage {
                expires_at: Some(100),
                ..message("v")
            },
        );
        agree("a message with a deadline");
        assert_eq!(store.sweep_expired(101, &mut Vec::new()), 1);
        agree("the sweep that reclaimed it");

        store.remove("a/b");
        agree("the last one");
        assert!(store.is_empty());
    }

    /// The page reports what is stored, deadline or not, because the
    /// count it is a page of does too.
    ///
    /// A page that skipped an expired message while `len` counted it
    /// would hand a reader a total it can never reach by paging.
    #[test]
    fn a_page_holds_an_expired_message_no_sweep_has_reclaimed() {
        let store = RetainedStore::new();
        store.set(
            "a",
            RetainedMessage {
                expires_at: Some(100),
                ..message("v")
            },
        );
        store.set("b", message("v"));

        assert_eq!(store.len(), 2);
        assert_eq!(page(&store, None, 10).0, vec!["a".to_string(), "b".into()]);
        assert!(
            topics_at(&store, "#", 101).contains(&"b".to_string()),
            "while a reader that must not see it still does not"
        );
        assert_eq!(topics_at(&store, "#", 101).len(), 1);
    }

    /// A limit of zero asks for nothing and says there is more.
    ///
    /// The admin API refuses a zero limit before it gets here, but
    /// this is a public method on `broker-core` and a caller that
    /// passes one must get an empty page rather than the whole store.
    #[test]
    fn a_page_of_nothing_is_empty_and_says_there_is_more() {
        let store = RetainedStore::new();
        store.set("a", message("v"));
        assert_eq!(page(&store, None, 0), (Vec::new(), true));

        let empty = RetainedStore::new();
        assert_eq!(page(&empty, None, 0), (Vec::new(), false));
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
