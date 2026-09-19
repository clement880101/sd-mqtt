# Routing Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `broker-core`'s matching layer: a subscription trie that answers "who wants this topic" fast, and a retained store that answers "what was retained under this filter".

**Architecture:** Two structures over one shared topic-level representation. Both are synchronous and hold no async machinery, so they can be tested exhaustively without a runtime. The trie is sharded by first topic level, with a separate root for filters that begin with a wildcard and therefore span every shard.

**Tech Stack:** Rust, `mqtt-codec` for `QoS` and topic validation, `foldhash` for hashing, `parking_lot` for shard locks, `proptest` for the differential tests, `criterion` for benchmarks.

**Spec:** `docs/superpowers/specs/2026-09-19-sd-mqtt-design.md`, sections 5.3, 6 and 10.1.

---

## The idea that makes this plan work

A topic trie is easy to get subtly wrong and hard to review by reading. So the first thing built is a **reference matcher**: a naive, obviously-correct function that splits a filter and a topic into levels and walks them recursively. It is slow and that is fine, because nothing calls it in production.

Everything after it is then tested by **differential property testing**: for generated filters and topics, the fast trie must agree with the reference matcher exactly. A trie bug becomes a failing property with a shrunk counterexample, rather than a subtle delivery fault discovered in production.

This is the same technique that caught real bugs in the codec, applied where it is worth even more.

## Conventions inherited from the codec

These were settled by review during the previous plan and apply here unchanged.

1. **The writer refuses what the reader rejects.** Symmetry between the two directions of any pair.
2. **Assert exact error strings, not error variants.** A test asserting only a variant passes for any other cause of the same variant.
3. **Prove a test has teeth by mutation.** Break the implementation deliberately, confirm that test and only that test fails, restore. Back the file up with `cp`, not `git checkout --`, because the change being proven is usually uncommitted.
4. **Derive protocol rules from the specification, not from this plan.** The plan's tables have been wrong before. Download the OASIS HTML with `curl` and parse the raw markup; the fetch tool has mis-transcribed specification tables more than once.

---

## File Structure

| Path | Responsibility |
|---|---|
| `services/broker/crates/broker-core/Cargo.toml` | Crate manifest |
| `services/broker/crates/broker-core/src/lib.rs` | Public surface, re-exports |
| `services/broker/crates/broker-core/src/topic/reference.rs` | The naive reference matcher |
| `services/broker/crates/broker-core/src/topic/levels.rs` | Level interning |
| `services/broker/crates/broker-core/src/topic/mod.rs` | Topic module root |
| `services/broker/crates/broker-core/src/subscriptions/node.rs` | One trie node, unsharded |
| `services/broker/crates/broker-core/src/subscriptions/trie.rs` | The sharded trie and its wildcard root |
| `services/broker/crates/broker-core/src/subscriptions/shared.rs` | Shared subscription groups |
| `services/broker/crates/broker-core/src/subscriptions/mod.rs` | Subscription module root |
| `services/broker/crates/broker-core/src/retained.rs` | Retained message store |
| `services/broker/crates/broker-core/tests/differential.rs` | Trie against the reference matcher |
| `services/broker/crates/broker-core/benches/matching.rs` | Matching benchmarks |

Split by responsibility: a change to shared-subscription policy touches one file, a change to node layout touches another.

---

## Task 1: The broker-core crate

**Files:**
- Create: `services/broker/crates/broker-core/Cargo.toml`, `services/broker/crates/broker-core/src/lib.rs`

- [ ] **Step 1: Write the manifest**

Create `services/broker/crates/broker-core/Cargo.toml`:

```toml
[package]
name = "broker-core"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
repository.workspace = true
description = "Subscription matching and retained message storage for sd-mqtt"

[dependencies]
# cargo-deny's wildcard ban treats a path dependency with no version
# requirement as a wildcard, so an internal dependency needs both.
mqtt-codec = { path = "../mqtt-codec", version = "0.1.0" }
bytes.workspace = true
thiserror.workspace = true
foldhash = "0.2"
parking_lot = "0.12"

[dev-dependencies]
proptest.workspace = true

[lints]
workspace = true
```

- [ ] **Step 2: Write the library root**

Create `services/broker/crates/broker-core/src/lib.rs`:

```rust
//! Subscription matching and retained message storage.
//!
//! This crate answers two questions, both synchronously and without any
//! async machinery: given a topic, which subscribers want it, and given a
//! filter, which retained messages match it.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
```

- [ ] **Step 3: Verify it builds and the license gate still passes**

Run:

```bash
cd services/broker && cargo build -p broker-core
```

Expected: `Finished`. Then, from the repository root:

```bash
cargo deny --manifest-path services/broker/Cargo.toml --config deny.toml check licenses bans sources
```

Expected: `licenses ok`, `bans ok`, `sources ok`. `foldhash` is Zlib and `parking_lot` is MIT OR Apache-2.0, both already on the allow list. If either fails, stop and report rather than widening the allow list.

- [ ] **Step 4: Commit**

```bash
git add services/broker/crates/broker-core services/broker/Cargo.lock
git commit -m "chore(core): add the broker-core crate"
```

---

## Task 2: The reference matcher

Naive, obviously correct, and never called in production. Everything else is tested against it.

**Files:**
- Create: `services/broker/crates/broker-core/src/topic/mod.rs`, `services/broker/crates/broker-core/src/topic/reference.rs`
- Modify: `services/broker/crates/broker-core/src/lib.rs`

- [ ] **Step 1: Declare the module**

Add to `lib.rs`:

```rust
pub mod topic;
```

Create `services/broker/crates/broker-core/src/topic/mod.rs`:

```rust
//! Topic representation and matching.

pub mod reference;
```

- [ ] **Step 2: Write the failing test**

Create `services/broker/crates/broker-core/src/topic/reference.rs` with tests only:

```rust
//! A deliberately naive matcher, used as the oracle for the fast paths.

#[cfg(test)]
mod tests {
    use super::*;

    /// Cases taken from the specification's own matching examples.
    #[test]
    fn matches_the_specification_examples() {
        assert!(matches("sport/tennis/player1/#", "sport/tennis/player1"));
        assert!(matches(
            "sport/tennis/player1/#",
            "sport/tennis/player1/ranking"
        ));
        assert!(matches(
            "sport/tennis/player1/#",
            "sport/tennis/player1/score/wimbledon"
        ));
        assert!(matches("sport/#", "sport"));
        assert!(matches("sport/tennis/+", "sport/tennis/player1"));
        assert!(matches("sport/tennis/+", "sport/tennis/player2"));
        assert!(!matches("sport/tennis/+", "sport/tennis/player1/ranking"));
        assert!(matches("sport/+", "sport/"));
        assert!(!matches("sport/+", "sport"));
        assert!(matches("+/+", "/finance"));
        assert!(matches("/+", "/finance"));
        assert!(!matches("+", "/finance"));
    }

    #[test]
    fn a_leading_wildcard_never_matches_a_dollar_topic() {
        assert!(!matches("#", "$SYS/broker/uptime"));
        assert!(!matches("+/broker/uptime", "$SYS/broker/uptime"));
        assert!(matches("$SYS/#", "$SYS/broker/uptime"));
        assert!(matches("$SYS/broker/+", "$SYS/broker/uptime"));
    }

    #[test]
    fn a_dollar_filter_does_not_match_an_ordinary_topic() {
        assert!(!matches("$SYS/#", "sport/tennis"));
    }

    #[test]
    fn empty_levels_are_ordinary_levels() {
        assert!(matches("a/+/b", "a//b"));
        assert!(matches("a//b", "a//b"));
        assert!(!matches("a/b", "a//b"));
    }

    #[test]
    fn a_multi_level_wildcard_matches_the_parent_level() {
        assert!(matches("a/#", "a"));
        assert!(matches("#", "a"));
        assert!(matches("#", "a/b/c"));
    }

    #[test]
    fn an_exact_filter_matches_only_itself() {
        assert!(matches("a/b/c", "a/b/c"));
        assert!(!matches("a/b/c", "a/b"));
        assert!(!matches("a/b", "a/b/c"));
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core reference`
Expected: FAIL to compile, with `matches` not found.

- [ ] **Step 4: Write the implementation**

Insert at the top of `reference.rs`, above the `#[cfg(test)]` module:

```rust
/// Whether `filter` matches `topic`.
///
/// This is the oracle. It is written for obviousness rather than speed, and
/// nothing on a hot path calls it. Both arguments are assumed already
/// validated by `mqtt_codec::topic`.
///
/// The one rule that is easy to miss: a filter whose first level is a
/// wildcard must not match a topic whose first level begins with a dollar
/// sign, so that a subscription to everything does not silently receive the
/// broker's own status topics.
pub fn matches(filter: &str, topic: &str) -> bool {
    let first_level_is_wildcard = matches!(filter.split('/').next(), Some("+") | Some("#"));
    if first_level_is_wildcard && topic.starts_with('$') {
        return false;
    }

    let filter_levels: Vec<&str> = filter.split('/').collect();
    let topic_levels: Vec<&str> = topic.split('/').collect();
    walk(&filter_levels, &topic_levels)
}

fn walk(filter: &[&str], topic: &[&str]) -> bool {
    match (filter.first(), topic.first()) {
        // A multi-level wildcard absorbs everything that remains, including
        // nothing at all, which is why `a/#` matches `a`.
        (Some(&"#"), _) => true,
        (Some(&level), Some(&actual)) => {
            (level == "+" || level == actual) && walk(&filter[1..], &topic[1..])
        }
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core reference`
Expected: PASS, six tests.

- [ ] **Step 6: Check the examples against the specification**

The cases in `matches_the_specification_examples` are quoted from MQTT's own matching section. Verify them against the specification text rather than against this plan. Download the OASIS HTML with `curl` and read section 4.7. Report any case where the plan and the specification disagree, and do not change the implementation to satisfy a plan case the specification contradicts.

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/broker-core/src
git commit -m "feat(core): a reference topic matcher to test the fast paths against"
```

---

## Task 3: Level interning

Topics repeat their levels constantly. Interning turns every comparison into an integer comparison and lets a node key be a fixed size.

**Files:**
- Create: `services/broker/crates/broker-core/src/topic/levels.rs`
- Modify: `services/broker/crates/broker-core/src/topic/mod.rs`

- [ ] **Step 1: Declare the module**

Add `pub mod levels;` to `src/topic/mod.rs`.

- [ ] **Step 2: Write the failing test**

Create `levels.rs` with tests only:

```rust
//! Interning of topic levels, so comparisons are integer comparisons.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_level_interns_to_the_same_identifier() {
        let interner = Interner::new();
        let a = interner.intern("sport");
        let b = interner.intern("sport");
        assert_eq!(a, b);
    }

    #[test]
    fn different_levels_intern_differently() {
        let interner = Interner::new();
        assert_ne!(interner.intern("sport"), interner.intern("finance"));
    }

    #[test]
    fn an_empty_level_is_a_level() {
        let interner = Interner::new();
        let empty = interner.intern("");
        assert_eq!(empty, interner.intern(""));
        assert_ne!(empty, interner.intern("a"));
    }

    #[test]
    fn resolves_an_identifier_back_to_its_text() {
        let interner = Interner::new();
        let id = interner.intern("sport");
        assert_eq!(interner.resolve(id).as_deref(), Some("sport"));
    }

    #[test]
    fn an_unknown_identifier_resolves_to_nothing() {
        let interner = Interner::new();
        assert_eq!(interner.resolve(LevelId(9999)), None);
    }

    #[test]
    fn lookup_does_not_intern() {
        let interner = Interner::new();
        assert_eq!(interner.lookup("sport"), None);
        let id = interner.intern("sport");
        assert_eq!(interner.lookup("sport"), Some(id));
    }

    #[test]
    fn splits_a_topic_into_interned_levels() {
        let interner = Interner::new();
        let levels = interner.intern_topic("a/b/a");
        assert_eq!(levels.len(), 3);
        assert_eq!(levels[0], levels[2], "the repeated level interns once");
        assert_ne!(levels[0], levels[1]);
    }

    #[test]
    fn interning_is_stable_across_many_levels() {
        let interner = Interner::new();
        let ids: Vec<_> = (0..1000).map(|n| interner.intern(&n.to_string())).collect();
        for (n, id) in ids.iter().enumerate() {
            assert_eq!(interner.lookup(&n.to_string()), Some(*id));
        }
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core levels`
Expected: FAIL to compile, with `Interner` and `LevelId` not found.

- [ ] **Step 4: Write the implementation**

Insert at the top of `levels.rs`:

```rust
use std::collections::HashMap;

use foldhash::fast::RandomState;
use parking_lot::RwLock;

/// An interned topic level.
///
/// Comparing two of these is an integer comparison, and storing one in a
/// trie node costs four bytes rather than a heap string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LevelId(pub u32);

/// Assigns a stable [`LevelId`] to each distinct topic level.
///
/// Interning is append-only: an identifier is never reused or invalidated,
/// so a trie node may hold one indefinitely. That costs memory proportional
/// to the number of distinct levels ever seen, which is bounded in practice
/// by the topic vocabulary of the deployment rather than by traffic.
pub struct Interner {
    inner: RwLock<Inner>,
}

struct Inner {
    to_id: HashMap<Box<str>, LevelId, RandomState>,
    to_text: Vec<Box<str>>,
}

impl Interner {
    /// An empty interner.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                to_id: HashMap::with_hasher(RandomState::default()),
                to_text: Vec::new(),
            }),
        }
    }

    /// The identifier for `level`, assigning one if it is new.
    pub fn intern(&self, level: &str) -> LevelId {
        if let Some(id) = self.lookup(level) {
            return id;
        }
        let mut inner = self.inner.write();
        // Re-check: another writer may have interned it between the read
        // lock being released and the write lock being taken.
        if let Some(id) = inner.to_id.get(level) {
            return *id;
        }
        let id = LevelId(inner.to_text.len() as u32);
        let boxed: Box<str> = level.into();
        inner.to_text.push(boxed.clone());
        inner.to_id.insert(boxed, id);
        id
    }

    /// The identifier for `level` if it has been interned, without
    /// interning it.
    ///
    /// Matching uses this: a level that has never been subscribed to cannot
    /// match anything, and interning it would grow the table with traffic
    /// rather than with vocabulary.
    pub fn lookup(&self, level: &str) -> Option<LevelId> {
        self.inner.read().to_id.get(level).copied()
    }

    /// The text an identifier stands for.
    pub fn resolve(&self, id: LevelId) -> Option<String> {
        self.inner
            .read()
            .to_text
            .get(id.0 as usize)
            .map(|text| text.to_string())
    }

    /// Split a topic or filter and intern every level.
    pub fn intern_topic(&self, topic: &str) -> Vec<LevelId> {
        topic.split('/').map(|level| self.intern(level)).collect()
    }

    /// How many distinct levels have been interned.
    pub fn len(&self) -> usize {
        self.inner.read().to_text.len()
    }

    /// Whether nothing has been interned.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for Interner {
    fn default() -> Self {
        Self::new()
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core levels`
Expected: PASS, eight tests.

- [ ] **Step 6: Prove the double-checked locking is needed**

The `intern` method takes a read lock, then a write lock, then checks again. Convince yourself the second check is load-bearing by writing a scratch test that spawns sixteen threads all interning the same hundred levels, and asserts that every thread sees the same identifier for each level and that `len()` is exactly one hundred at the end. Run it, then delete it and confirm a clean tree. Report what you saw.

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/broker-core/src
git commit -m "feat(core): intern topic levels"
```

---
## Task 4: A single trie node

The node is unsharded and knows nothing about locking. It is where the
matching rules actually live, so it is worth getting right in isolation
before any concurrency is added.

**Files:**
- Create: `services/broker/crates/broker-core/src/subscriptions/mod.rs`, `services/broker/crates/broker-core/src/subscriptions/node.rs`
- Modify: `services/broker/crates/broker-core/src/lib.rs`

- [ ] **Step 1: Declare the modules**

Add `pub mod subscriptions;` to `lib.rs`. Create `src/subscriptions/mod.rs`:

```rust
//! Subscription storage and matching.

pub mod node;
```

- [ ] **Step 2: Write the failing test**

Create `node.rs` with tests only:

```rust
//! One node of the subscription trie.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::levels::Interner;
    use crate::topic::reference;
    use mqtt_codec::QoS;

    fn sub(id: u64) -> Subscription {
        Subscription {
            subscriber: SubscriberId(id),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            id: None,
        }
    }

    /// Insert every filter with its index as the subscriber, then assert the
    /// node agrees with the reference matcher about which ones match.
    fn check(filters: &[&str], topic: &str) {
        let interner = Interner::new();
        let mut root = Node::default();
        for (index, filter) in filters.iter().enumerate() {
            root.insert(&interner, filter, sub(index as u64));
        }

        let mut got = Vec::new();
        root.matching(&interner, topic, &mut got);
        let mut got: Vec<u64> = got.into_iter().map(|s| s.subscriber.0).collect();
        got.sort_unstable();

        let mut expected: Vec<u64> = filters
            .iter()
            .enumerate()
            .filter(|(_, f)| reference::matches(f, topic))
            .map(|(index, _)| index as u64)
            .collect();
        expected.sort_unstable();

        assert_eq!(got, expected, "topic {topic:?} against {filters:?}");
    }

    const FILTERS: &[&str] = &[
        "a", "a/b", "a/b/c", "a/+", "a/+/c", "+/b", "+", "#", "a/#", "a/b/#", "+/+", "/a", "a/",
    ];

    #[test]
    fn agrees_with_the_reference_matcher() {
        for topic in [
            "a", "a/b", "a/b/c", "a/b/c/d", "b", "b/b", "/a", "a/", "a//c", "",
        ] {
            check(FILTERS, topic);
        }
    }

    #[test]
    fn a_multi_level_wildcard_matches_the_parent() {
        check(&["a/#"], "a");
        check(&["#"], "a/b/c");
    }

    #[test]
    fn removing_a_subscriber_removes_only_that_one() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a/b", sub(1));
        root.insert(&interner, "a/b", sub(2));

        assert!(root.remove(&interner, "a/b", SubscriberId(1)));

        let mut got = Vec::new();
        root.matching(&interner, "a/b", &mut got);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].subscriber, SubscriberId(2));
    }

    #[test]
    fn removing_the_last_subscriber_prunes_the_branch() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a/b/c", sub(1));
        assert!(!root.is_empty());
        assert!(root.remove(&interner, "a/b/c", SubscriberId(1)));
        assert!(root.is_empty(), "the whole branch should be gone");
    }

    #[test]
    fn removing_something_absent_reports_false() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a/b", sub(1));
        assert!(!root.remove(&interner, "a/b", SubscriberId(2)));
        assert!(!root.remove(&interner, "a/c", SubscriberId(1)));
        assert!(!root.remove(&interner, "x/y/z", SubscriberId(1)));
    }

    #[test]
    fn resubscribing_replaces_the_options_rather_than_duplicating() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a/b", sub(1));
        let replaced = root.insert(
            &interner,
            "a/b",
            Subscription {
                qos: QoS::ExactlyOnce,
                ..sub(1)
            },
        );
        assert!(replaced, "the second insert should report a replacement");

        let mut got = Vec::new();
        root.matching(&interner, "a/b", &mut got);
        assert_eq!(got.len(), 1, "a resubscribe must not duplicate");
        assert_eq!(got[0].qos, QoS::ExactlyOnce);
    }

    #[test]
    fn a_matching_call_appends_rather_than_clearing() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a", sub(1));

        let mut got = vec![sub(99)];
        root.matching(&interner, "a", &mut got);
        assert_eq!(got.len(), 2, "the caller's existing entries must survive");
    }

    #[test]
    fn matching_an_unseen_level_allocates_nothing_in_the_interner() {
        let interner = Interner::new();
        let mut root = Node::default();
        root.insert(&interner, "a", sub(1));
        let before = interner.len();

        let mut got = Vec::new();
        root.matching(&interner, "never/seen/before", &mut got);
        assert!(got.is_empty());
        assert_eq!(
            interner.len(),
            before,
            "matching must not grow the interner with traffic"
        );
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core node`
Expected: FAIL to compile, with `Node`, `Subscription`, `SubscriberId` not found.

- [ ] **Step 4: Write the implementation**

Insert at the top of `node.rs`:

```rust
use std::collections::HashMap;

use foldhash::fast::RandomState;
use mqtt_codec::QoS;

use crate::topic::levels::{Interner, LevelId};

/// Identifies the session a subscription belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriberId(pub u64);

/// One subscriber's interest in one filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    /// Whose subscription this is.
    pub subscriber: SubscriberId,
    /// The maximum quality of service to deliver at.
    pub qos: QoS,
    /// Suppress delivery of this subscriber's own publications.
    pub no_local: bool,
    /// Forward the retain flag as published rather than clearing it.
    pub retain_as_published: bool,
    /// The MQTT 5 subscription identifier, echoed back on delivery.
    pub id: Option<u32>,
}

/// One level of the subscription trie.
///
/// Three kinds of edge leave a node: literal children keyed by interned
/// level, a single-level wildcard child, and a multi-level wildcard, which
/// is a terminal rather than an edge because nothing can follow it.
#[derive(Debug, Default)]
pub struct Node {
    children: HashMap<LevelId, Node, RandomState>,
    plus: Option<Box<Node>>,
    /// Subscribers whose filter ended exactly at this node.
    exact: Vec<Subscription>,
    /// Subscribers whose filter ended in a multi-level wildcard here.
    multi: Vec<Subscription>,
}

impl Node {
    /// Whether this node holds nothing and can be pruned.
    pub fn is_empty(&self) -> bool {
        self.children.is_empty() && self.plus.is_none() && self.exact.is_empty() && self.multi.is_empty()
    }

    /// Add a subscription, replacing any the same subscriber already had on
    /// this filter. Returns whether it replaced one.
    ///
    /// Replacing rather than appending is the protocol's rule: subscribing
    /// again to a filter you already hold updates its options.
    pub fn insert(&mut self, interner: &Interner, filter: &str, sub: Subscription) -> bool {
        let levels: Vec<&str> = filter.split('/').collect();
        self.insert_at(interner, &levels, sub)
    }

    fn insert_at(&mut self, interner: &Interner, levels: &[&str], sub: Subscription) -> bool {
        let Some((level, rest)) = levels.split_first() else {
            return replace_or_push(&mut self.exact, sub);
        };

        match *level {
            "#" => replace_or_push(&mut self.multi, sub),
            "+" => {
                let child = self.plus.get_or_insert_with(|| Box::new(Node::default()));
                child.insert_at(interner, rest, sub)
            }
            literal => {
                // Interning here, on the write path, is what keeps the
                // table growing with vocabulary rather than with traffic.
                let id = interner.intern(literal);
                self.children
                    .entry(id)
                    .or_default()
                    .insert_at(interner, rest, sub)
            }
        }
    }

    /// Remove one subscriber's subscription to one filter, pruning any
    /// branch it leaves empty. Returns whether anything was removed.
    pub fn remove(&mut self, interner: &Interner, filter: &str, subscriber: SubscriberId) -> bool {
        let levels: Vec<&str> = filter.split('/').collect();
        self.remove_at(interner, &levels, subscriber)
    }

    fn remove_at(&mut self, interner: &Interner, levels: &[&str], subscriber: SubscriberId) -> bool {
        let Some((level, rest)) = levels.split_first() else {
            return drop_subscriber(&mut self.exact, subscriber);
        };

        match *level {
            "#" => drop_subscriber(&mut self.multi, subscriber),
            "+" => {
                let Some(child) = self.plus.as_mut() else {
                    return false;
                };
                let removed = if rest.is_empty() {
                    drop_subscriber(&mut child.exact, subscriber)
                } else {
                    child.remove_at(interner, rest, subscriber)
                };
                if removed && child.is_empty() {
                    self.plus = None;
                }
                removed
            }
            literal => {
                // A level never interned cannot be in the trie, and
                // interning it here would grow the table on a miss.
                let Some(id) = interner.lookup(literal) else {
                    return false;
                };
                let Some(child) = self.children.get_mut(&id) else {
                    return false;
                };
                let removed = if rest.is_empty() {
                    drop_subscriber(&mut child.exact, subscriber)
                } else {
                    child.remove_at(interner, rest, subscriber)
                };
                if removed && child.is_empty() {
                    self.children.remove(&id);
                }
                removed
            }
        }
    }

    /// Append every subscription matching `topic` to `out`.
    ///
    /// Appends rather than clears, so a caller can gather matches from
    /// several tries into one buffer and reuse it across publishes.
    pub fn matching(&self, interner: &Interner, topic: &str, out: &mut Vec<Subscription>) {
        let levels: Vec<&str> = topic.split('/').collect();
        self.match_at(interner, &levels, out);
    }

    fn match_at(&self, interner: &Interner, levels: &[&str], out: &mut Vec<Subscription>) {
        // A multi-level wildcard here matches this level and everything
        // below it, including the case where nothing is below.
        out.extend(self.multi.iter().cloned());

        let Some((level, rest)) = levels.split_first() else {
            out.extend(self.exact.iter().cloned());
            return;
        };

        if let Some(id) = interner.lookup(level) {
            if let Some(child) = self.children.get(&id) {
                child.match_at(interner, rest, out);
            }
        }
        if let Some(child) = self.plus.as_ref() {
            child.match_at(interner, rest, out);
        }
    }

    /// Remove every subscription belonging to one subscriber, wherever it
    /// is. Returns how many were removed.
    ///
    /// This walks the whole trie, so it is for session teardown rather than
    /// for an unsubscribe.
    pub fn remove_subscriber(&mut self, subscriber: SubscriberId) -> usize {
        let mut removed = 0;
        removed += usize::from(drop_subscriber(&mut self.exact, subscriber));
        removed += usize::from(drop_subscriber(&mut self.multi, subscriber));

        if let Some(child) = self.plus.as_mut() {
            removed += child.remove_subscriber(subscriber);
            if child.is_empty() {
                self.plus = None;
            }
        }
        self.children.retain(|_, child| {
            removed += child.remove_subscriber(subscriber);
            !child.is_empty()
        });

        removed
    }

    /// How many subscriptions this node and its descendants hold.
    pub fn len(&self) -> usize {
        self.exact.len()
            + self.multi.len()
            + self.plus.as_ref().map_or(0, |child| child.len())
            + self.children.values().map(Node::len).sum::<usize>()
    }
}

fn replace_or_push(list: &mut Vec<Subscription>, sub: Subscription) -> bool {
    if let Some(slot) = list.iter_mut().find(|s| s.subscriber == sub.subscriber) {
        *slot = sub;
        true
    } else {
        list.push(sub);
        false
    }
}

fn drop_subscriber(list: &mut Vec<Subscription>, subscriber: SubscriberId) -> bool {
    let before = list.len();
    list.retain(|s| s.subscriber != subscriber);
    list.len() != before
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core node`
Expected: PASS, eight tests.

- [ ] **Step 6: Prove the pruning is real**

`removing_the_last_subscriber_prunes_the_branch` passes if pruning works. Prove it can fail: comment out the two `if removed && child.is_empty()` blocks, confirm that test and only that test fails, then restore from a `cp` backup. Report what you saw.

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/broker-core/src
git commit -m "feat(core): a subscription trie node"
```

---

## Task 5: The sharded trie

A single lock over the whole trie would serialise every subscribe against
every publish. Sharding by first topic level fixes that for the common
case, but it has one wrinkle worth stating plainly, because it is the part
a reader will question.

**A filter whose first level is a wildcard belongs to no shard.** `+/b` and
`#` can match a topic in any shard, so they cannot live in one. They go in
a separate wildcard root that every publish also consults. Filters like
these are rare in practice and numerous only in pathological deployments,
so one extra lock acquisition per publish is the right trade.

The dollar-topic rule falls out of this neatly: a publish to a topic whose
first level begins with a dollar sign simply skips the wildcard root.

**Files:**
- Create: `services/broker/crates/broker-core/src/subscriptions/trie.rs`
- Modify: `services/broker/crates/broker-core/src/subscriptions/mod.rs`

- [ ] **Step 1: Declare the module**

Add `pub mod trie;` to `src/subscriptions/mod.rs`, and re-export the useful names:

```rust
pub use node::{SubscriberId, Subscription};
pub use trie::SubscriptionTrie;
```

- [ ] **Step 2: Write the failing test**

Create `trie.rs` with tests only:

```rust
//! The sharded subscription trie.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::reference;
    use mqtt_codec::QoS;

    fn sub(id: u64) -> Subscription {
        Subscription {
            subscriber: SubscriberId(id),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            id: None,
        }
    }

    fn ids(trie: &SubscriptionTrie, topic: &str) -> Vec<u64> {
        let mut out = Vec::new();
        trie.matching(topic, &mut out);
        let mut ids: Vec<u64> = out.into_iter().map(|s| s.subscriber.0).collect();
        ids.sort_unstable();
        ids
    }

    fn expected(filters: &[&str], topic: &str) -> Vec<u64> {
        let mut ids: Vec<u64> = filters
            .iter()
            .enumerate()
            .filter(|(_, f)| reference::matches(f, topic))
            .map(|(index, _)| index as u64)
            .collect();
        ids.sort_unstable();
        ids
    }

    const FILTERS: &[&str] = &[
        "a", "a/b", "a/+", "a/#", "+/b", "+", "#", "+/+/c", "$SYS/#", "$SYS/broker/+", "b/c",
    ];

    fn populated() -> SubscriptionTrie {
        let trie = SubscriptionTrie::new(8);
        for (index, filter) in FILTERS.iter().enumerate() {
            trie.insert(filter, sub(index as u64));
        }
        trie
    }

    #[test]
    fn agrees_with_the_reference_matcher_including_wildcard_roots() {
        let trie = populated();
        for topic in [
            "a",
            "a/b",
            "a/b/c",
            "b/b",
            "b/c",
            "x/y/c",
            "$SYS/broker/uptime",
            "$SYS/broker",
            "$other",
        ] {
            assert_eq!(ids(&trie, topic), expected(FILTERS, topic), "topic {topic:?}");
        }
    }

    #[test]
    fn a_leading_wildcard_never_reaches_a_dollar_topic() {
        let trie = SubscriptionTrie::new(4);
        trie.insert("#", sub(1));
        trie.insert("+/broker/uptime", sub(2));
        trie.insert("$SYS/#", sub(3));

        assert_eq!(ids(&trie, "$SYS/broker/uptime"), vec![3]);
        assert_eq!(ids(&trie, "ordinary/topic"), vec![1]);
    }

    #[test]
    fn counts_subscriptions_across_shards_and_the_wildcard_root() {
        let trie = populated();
        assert_eq!(trie.len(), FILTERS.len());
    }

    #[test]
    fn removing_reports_whether_anything_went() {
        let trie = SubscriptionTrie::new(4);
        trie.insert("a/b", sub(1));
        assert!(trie.remove("a/b", SubscriberId(1)));
        assert!(!trie.remove("a/b", SubscriberId(1)));
        assert_eq!(trie.len(), 0);
    }

    #[test]
    fn removing_a_subscriber_clears_every_shard_and_the_wildcard_root() {
        let trie = SubscriptionTrie::new(4);
        trie.insert("a/b", sub(1));
        trie.insert("q/r", sub(1));
        trie.insert("#", sub(1));
        trie.insert("a/b", sub(2));
        assert_eq!(trie.len(), 4);

        assert_eq!(trie.remove_subscriber(SubscriberId(1)), 3);
        assert_eq!(trie.len(), 1);
        assert_eq!(ids(&trie, "a/b"), vec![2]);
    }

    #[test]
    fn a_single_shard_behaves_the_same_as_many() {
        for shards in [1usize, 2, 8, 64] {
            let trie = SubscriptionTrie::new(shards);
            for (index, filter) in FILTERS.iter().enumerate() {
                trie.insert(filter, sub(index as u64));
            }
            assert_eq!(
                ids(&trie, "a/b"),
                expected(FILTERS, "a/b"),
                "with {shards} shards"
            );
        }
    }

    #[test]
    fn zero_shards_is_treated_as_one() {
        let trie = SubscriptionTrie::new(0);
        trie.insert("a", sub(1));
        assert_eq!(ids(&trie, "a"), vec![1]);
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core trie`
Expected: FAIL to compile, with `SubscriptionTrie` not found.

- [ ] **Step 4: Write the implementation**

Insert at the top of `trie.rs`:

```rust
use std::hash::BuildHasher;

use foldhash::fast::RandomState;
use parking_lot::RwLock;

use crate::subscriptions::node::{Node, SubscriberId, Subscription};
use crate::topic::levels::Interner;

/// A subscription trie sharded by first topic level.
///
/// Filters whose first level is literal live in the shard that level hashes
/// to. Filters whose first level is a wildcard can match a topic in any
/// shard, so they live in a separate root that every publish also consults.
pub struct SubscriptionTrie {
    interner: Interner,
    shards: Box<[RwLock<Node>]>,
    /// Filters beginning with `+` or `#`, which belong to no shard.
    wildcard: RwLock<Node>,
    hasher: RandomState,
}

impl SubscriptionTrie {
    /// A trie with `shards` shards. Zero is treated as one.
    pub fn new(shards: usize) -> Self {
        let shards = shards.max(1);
        Self {
            interner: Interner::new(),
            shards: (0..shards).map(|_| RwLock::new(Node::default())).collect(),
            wildcard: RwLock::new(Node::default()),
            hasher: RandomState::default(),
        }
    }

    fn shard_of(&self, first_level: &str) -> &RwLock<Node> {
        let index = (self.hasher.hash_one(first_level) as usize) % self.shards.len();
        &self.shards[index]
    }

    fn first_level_is_wildcard(filter: &str) -> bool {
        matches!(filter.split('/').next(), Some("+") | Some("#"))
    }

    /// Add a subscription, replacing any the same subscriber held on this
    /// filter. Returns whether it replaced one.
    pub fn insert(&self, filter: &str, sub: Subscription) -> bool {
        if Self::first_level_is_wildcard(filter) {
            return self.wildcard.write().insert(&self.interner, filter, sub);
        }
        let first = filter.split('/').next().unwrap_or("");
        self.shard_of(first)
            .write()
            .insert(&self.interner, filter, sub)
    }

    /// Remove one subscriber's subscription to one filter. Returns whether
    /// anything was removed.
    pub fn remove(&self, filter: &str, subscriber: SubscriberId) -> bool {
        if Self::first_level_is_wildcard(filter) {
            return self
                .wildcard
                .write()
                .remove(&self.interner, filter, subscriber);
        }
        let first = filter.split('/').next().unwrap_or("");
        self.shard_of(first)
            .write()
            .remove(&self.interner, filter, subscriber)
    }

    /// Append every subscription matching `topic` to `out`.
    ///
    /// A topic whose first level begins with a dollar sign skips the
    /// wildcard root entirely, which is how the protocol's rule that a
    /// leading wildcard must not match a dollar topic is enforced.
    pub fn matching(&self, topic: &str, out: &mut Vec<Subscription>) {
        let first = topic.split('/').next().unwrap_or("");
        self.shard_of(first)
            .read()
            .matching(&self.interner, topic, out);

        if !topic.starts_with('$') {
            self.wildcard.read().matching(&self.interner, topic, out);
        }
    }

    /// Remove every subscription belonging to one subscriber. Returns how
    /// many were removed. For session teardown, not for unsubscribe.
    pub fn remove_subscriber(&self, subscriber: SubscriberId) -> usize {
        let mut removed = self.wildcard.write().remove_subscriber(subscriber);
        for shard in self.shards.iter() {
            removed += shard.write().remove_subscriber(subscriber);
        }
        removed
    }

    /// How many subscriptions the trie holds.
    pub fn len(&self) -> usize {
        self.wildcard.read().len()
            + self
                .shards
                .iter()
                .map(|shard| shard.read().len())
                .sum::<usize>()
    }

    /// Whether the trie holds nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many distinct topic levels have been interned.
    pub fn interned_levels(&self) -> usize {
        self.interner.len()
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core trie`
Expected: PASS, seven tests.

- [ ] **Step 6: Prove the wildcard root is load-bearing**

Make `matching` skip the wildcard root unconditionally, confirm
`agrees_with_the_reference_matcher_including_wildcard_roots` and
`a_leading_wildcard_never_reaches_a_dollar_topic` both fail, then restore
from a `cp` backup. Then make it consult the wildcard root even for dollar
topics, confirm only the dollar test fails, restore. Report both.

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/broker-core/src
git commit -m "feat(core): shard the subscription trie by first topic level"
```

---
## Task 6: Shared subscription groups

A shared subscription is identified by the pair of its share name and its
filter, not by the share name alone: `$share/a/x` and `$share/a/y` are two
different groups. Delivery picks one member per matching group.

**What this task does not do.** The specification's selection policy
prefers a local online member, then the least loaded remote node. None of
that state exists yet. This task builds the structure and a round-robin
selection, and leaves a seam for policy. Say so in the doc comment rather
than implying the policy is finished.

The structure that makes this cheap: the trie stores **one synthetic entry
per group**, not one per member, so matching a topic yields matching groups
directly rather than requiring a scan of every group.

**Files:**
- Create: `services/broker/crates/broker-core/src/subscriptions/shared.rs`
- Modify: `services/broker/crates/broker-core/src/subscriptions/mod.rs`

- [ ] **Step 1: Declare the module**

Add `pub mod shared;` to `src/subscriptions/mod.rs` and re-export `SharedSubscriptions`.

- [ ] **Step 2: Write the failing test**

Create `shared.rs` with tests only:

```rust
//! Shared subscription groups.

#[cfg(test)]
mod tests {
    use super::*;
    use mqtt_codec::QoS;

    fn sub(id: u64) -> Subscription {
        Subscription {
            subscriber: SubscriberId(id),
            qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            id: None,
        }
    }

    fn picked(shared: &SharedSubscriptions, topic: &str) -> Vec<u64> {
        let mut out = Vec::new();
        shared.select(topic, &mut out);
        let mut ids: Vec<u64> = out.into_iter().map(|s| s.subscriber.0).collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn one_member_of_a_group_receives_each_message() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert("g", "a/b", sub(2));
        shared.insert("g", "a/b", sub(3));

        for _ in 0..10 {
            assert_eq!(picked(&shared, "a/b").len(), 1);
        }
    }

    #[test]
    fn selection_rotates_through_the_members() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert("g", "a/b", sub(2));
        shared.insert("g", "a/b", sub(3));

        let mut seen = std::collections::HashSet::new();
        for _ in 0..30 {
            seen.extend(picked(&shared, "a/b"));
        }
        assert_eq!(seen.len(), 3, "every member should be picked eventually");
    }

    #[test]
    fn the_share_name_and_the_filter_together_identify_a_group() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert("g", "a/+", sub(2));
        shared.insert("h", "a/b", sub(3));

        // Three distinct groups all match, so all three receive one each.
        assert_eq!(picked(&shared, "a/b").len(), 3);
    }

    #[test]
    fn a_wildcard_group_filter_still_matches() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/#", sub(1));
        assert_eq!(picked(&shared, "a/b/c"), vec![1]);
    }

    #[test]
    fn removing_the_last_member_removes_the_group() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        assert_eq!(shared.group_count(), 1);

        assert!(shared.remove("g", "a/b", SubscriberId(1)));
        assert_eq!(shared.group_count(), 0);
        assert!(picked(&shared, "a/b").is_empty());
    }

    #[test]
    fn removing_one_member_leaves_the_group() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert("g", "a/b", sub(2));

        assert!(shared.remove("g", "a/b", SubscriberId(1)));
        assert_eq!(shared.group_count(), 1);
        assert_eq!(picked(&shared, "a/b"), vec![2]);
    }

    #[test]
    fn removing_a_subscriber_clears_it_from_every_group() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert("h", "c/d", sub(1));
        shared.insert("h", "c/d", sub(2));

        assert_eq!(shared.remove_subscriber(SubscriberId(1)), 2);
        assert_eq!(shared.group_count(), 1);
        assert_eq!(picked(&shared, "c/d"), vec![2]);
    }

    #[test]
    fn resubscribing_updates_options_rather_than_duplicating() {
        let shared = SharedSubscriptions::new(4);
        shared.insert("g", "a/b", sub(1));
        shared.insert(
            "g",
            "a/b",
            Subscription {
                qos: QoS::ExactlyOnce,
                ..sub(1)
            },
        );
        assert_eq!(shared.member_count(), 1);

        let mut out = Vec::new();
        shared.select("a/b", &mut out);
        assert_eq!(out[0].qos, QoS::ExactlyOnce);
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core shared`
Expected: FAIL to compile, with `SharedSubscriptions` not found.

- [ ] **Step 4: Write the implementation**

Insert at the top of `shared.rs`:

```rust
use std::collections::HashMap;

use foldhash::fast::RandomState;
use parking_lot::RwLock;

use crate::subscriptions::node::{SubscriberId, Subscription};
use crate::subscriptions::trie::SubscriptionTrie;

/// Shared subscription groups, keyed by share name and filter together.
///
/// The trie holds one synthetic entry per group rather than one per
/// member, so matching a topic yields the matching groups directly instead
/// of requiring a scan over every group that exists.
///
/// Selection is round-robin within a group. The specification's fuller
/// policy, preferring a local online member and then the least loaded
/// remote node, needs session and cluster state that does not exist yet;
/// [`SharedSubscriptions::select`] is the seam where that policy will go.
pub struct SharedSubscriptions {
    /// Keyed by a synthetic subscriber identifier standing for a group.
    trie: SubscriptionTrie,
    groups: RwLock<Groups>,
}

#[derive(Default)]
struct Groups {
    by_key: HashMap<(Box<str>, Box<str>), u64, RandomState>,
    by_id: HashMap<u64, Group, RandomState>,
    next_id: u64,
}

struct Group {
    key: (Box<str>, Box<str>),
    members: Vec<Subscription>,
    cursor: usize,
}

impl SharedSubscriptions {
    /// An empty set of groups, over a trie with `shards` shards.
    pub fn new(shards: usize) -> Self {
        Self {
            trie: SubscriptionTrie::new(shards),
            groups: RwLock::new(Groups {
                by_key: HashMap::with_hasher(RandomState::default()),
                by_id: HashMap::with_hasher(RandomState::default()),
                next_id: 0,
            }),
        }
    }

    /// Add a member to the group named by `group` on `filter`, replacing
    /// any subscription that member already had there. Returns whether it
    /// replaced one.
    ///
    /// `filter` is the inner filter, with the `$share/<name>/` prefix
    /// already stripped by the caller.
    pub fn insert(&self, group: &str, filter: &str, sub: Subscription) -> bool {
        let key = (Box::from(group), Box::from(filter));
        let mut groups = self.groups.write();

        // Read the lookup out before the match, so its borrow does not
        // live across the arm that mutates the same map.
        let existing = groups.by_key.get(&key).copied();
        let id = match existing {
            Some(id) => id,
            None => {
                let id = groups.next_id;
                groups.next_id += 1;
                groups.by_key.insert(key.clone(), id);
                groups.by_id.insert(
                    id,
                    Group {
                        key: key.clone(),
                        members: Vec::new(),
                        cursor: 0,
                    },
                );
                // One synthetic entry stands for the whole group.
                self.trie.insert(
                    filter,
                    Subscription {
                        subscriber: SubscriberId(id),
                        ..sub.clone()
                    },
                );
                id
            }
        };

        let entry = groups.by_id.get_mut(&id).expect("group just created");
        if let Some(slot) = entry
            .members
            .iter_mut()
            .find(|m| m.subscriber == sub.subscriber)
        {
            *slot = sub;
            true
        } else {
            entry.members.push(sub);
            false
        }
    }

    /// Remove a member. Returns whether anything was removed. Removing the
    /// last member removes the group.
    pub fn remove(&self, group: &str, filter: &str, subscriber: SubscriberId) -> bool {
        let key = (Box::from(group), Box::from(filter));
        let mut groups = self.groups.write();

        let Some(id) = groups.by_key.get(&key).copied() else {
            return false;
        };
        let Some(entry) = groups.by_id.get_mut(&id) else {
            return false;
        };

        let before = entry.members.len();
        entry.members.retain(|m| m.subscriber != subscriber);
        if entry.members.len() == before {
            return false;
        }

        if entry.members.is_empty() {
            groups.by_id.remove(&id);
            groups.by_key.remove(&key);
            self.trie.remove(filter, SubscriberId(id));
        }
        true
    }

    /// Remove one subscriber from every group it belongs to. Returns how
    /// many memberships went.
    pub fn remove_subscriber(&self, subscriber: SubscriberId) -> usize {
        let mut groups = self.groups.write();
        let mut removed = 0;
        let mut emptied: Vec<u64> = Vec::new();

        for (id, entry) in groups.by_id.iter_mut() {
            let before = entry.members.len();
            entry.members.retain(|m| m.subscriber != subscriber);
            removed += before - entry.members.len();
            if entry.members.is_empty() {
                emptied.push(*id);
            }
        }

        for id in emptied {
            if let Some(entry) = groups.by_id.remove(&id) {
                self.trie.remove(&entry.key.1, SubscriberId(id));
                groups.by_key.remove(&entry.key);
            }
        }

        removed
    }

    /// Append one chosen member of each group matching `topic` to `out`.
    ///
    /// This is the seam for delivery policy. Today it is round-robin within
    /// each group; it will later prefer a local online member and then the
    /// least loaded remote node.
    pub fn select(&self, topic: &str, out: &mut Vec<Subscription>) {
        let mut matched = Vec::new();
        self.trie.matching(topic, &mut matched);
        if matched.is_empty() {
            return;
        }

        let mut groups = self.groups.write();
        for synthetic in matched {
            let Some(entry) = groups.by_id.get_mut(&synthetic.subscriber.0) else {
                continue;
            };
            if entry.members.is_empty() {
                continue;
            }
            let index = entry.cursor % entry.members.len();
            entry.cursor = entry.cursor.wrapping_add(1);
            out.push(entry.members[index].clone());
        }
    }

    /// How many groups exist.
    pub fn group_count(&self) -> usize {
        self.groups.read().by_id.len()
    }

    /// How many memberships exist across all groups.
    pub fn member_count(&self) -> usize {
        self.groups
            .read()
            .by_id
            .values()
            .map(|g| g.members.len())
            .sum()
    }

    /// Whether no group exists.
    pub fn is_empty(&self) -> bool {
        self.group_count() == 0
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core shared`
Expected: PASS, eight tests.

- [ ] **Step 6: Prove one member is chosen, not all**

Make `select` push every member of a matching group rather than one,
confirm `one_member_of_a_group_receives_each_message` fails while
`selection_rotates_through_the_members` still passes, then restore from a
`cp` backup. That contrast shows the rotation test alone would not catch
fan-out to the whole group. Report what you saw.

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/broker-core/src
git commit -m "feat(core): shared subscription groups with round-robin selection"
```

---

## Task 7: The retained store

Retained matching runs the opposite way to subscription matching: a filter
with wildcards is walked against stored topics, rather than a topic being
walked against stored filters. That makes it a different traversal, not a
reuse of the subscription trie.

One rule that is easy to lose, and which the design spec calls out: **a
retained publish with an empty payload deletes the retained message** for
that topic. It is not stored and not sent to future subscribers.

**Files:**
- Create: `services/broker/crates/broker-core/src/retained.rs`
- Modify: `services/broker/crates/broker-core/src/lib.rs`

- [ ] **Step 1: Declare the module**

Add `pub mod retained;` to `lib.rs`.

- [ ] **Step 2: Write the failing test**

Create `retained.rs` with tests only:

```rust
//! Retained message storage.

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
        assert_eq!(store.sweep_expired(100), 0, "the deadline itself is not past");
        assert_eq!(store.len(), 1);
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd services/broker && cargo test -p broker-core retained`
Expected: FAIL to compile, with `RetainedStore` and `RetainedMessage` not found.

- [ ] **Step 4: Write the implementation**

Insert at the top of `retained.rs`:

```rust
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
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd services/broker && cargo test -p broker-core retained`
Expected: PASS, nine tests.

- [ ] **Step 6: Check the empty-payload rule against the specification**

Read MQTT 3.1.1 section 3.3.1.3 and MQTT 5.0 section 3.3.1.3 rather than
trusting this plan. Confirm three things: an empty retained payload removes
the stored message, the empty message itself is not stored, and it is still
delivered to subscribers matching at the time. The third is delivery
behaviour and belongs to a later plan, not here. Report what you found and
whether the plan agrees.

- [ ] **Step 7: Commit**

```bash
git add services/broker/crates/broker-core/src
git commit -m "feat(core): retained message store"
```

---
## Task 8: Differential testing against the reference matcher

This is the task the whole plan is arranged around. Everything before it
built fast structures; this one proves they agree with the obvious one.

**Files:**
- Create: `services/broker/crates/broker-core/tests/differential.rs`

- [ ] **Step 1: Write the test**

Create `services/broker/crates/broker-core/tests/differential.rs`:

```rust
//! The fast structures must agree with the naive matcher, always.
//!
//! A trie is easy to get subtly wrong and hard to review by reading, so
//! correctness here is established by comparison rather than by inspection:
//! for generated filters and topics, the sharded trie and the retained
//! store must return exactly what `reference::matches` says they should. A
//! bug becomes a shrunk counterexample instead of a delivery fault found in
//! production.

use broker_core::retained::{RetainedMessage, RetainedStore};
use broker_core::subscriptions::{SubscriberId, Subscription, SubscriptionTrie};
use broker_core::topic::reference;
use bytes::Bytes;
use mqtt_codec::QoS;
use proptest::prelude::*;

fn sub(id: u64) -> Subscription {
    Subscription {
        subscriber: SubscriberId(id),
        qos: QoS::AtMostOnce,
        no_local: false,
        retain_as_published: false,
        id: None,
    }
}

/// A small alphabet, so that collisions and shared prefixes are common
/// rather than vanishingly rare. Empty levels are legal and included.
fn any_level() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => "[ab]",
        2 => "[ab]{2}",
        1 => Just(String::new()),
    ]
}

fn any_filter() -> impl Strategy<Value = String> {
    (
        prop::collection::vec(prop_oneof![3 => any_level(), 1 => Just("+".to_string())], 1..4),
        prop::bool::weighted(0.25),
        prop::bool::weighted(0.15),
    )
        .prop_map(|(mut levels, trailing_hash, dollar_root)| {
            if dollar_root {
                levels[0] = "$SYS".to_string();
            }
            if trailing_hash {
                levels.push("#".to_string());
            }
            levels.join("/")
        })
}

fn any_topic() -> impl Strategy<Value = String> {
    (
        prop::collection::vec(any_level(), 1..5),
        prop::bool::weighted(0.15),
    )
        .prop_map(|(mut levels, dollar_root)| {
            if dollar_root {
                levels[0] = "$SYS".to_string();
            }
            levels.join("/")
        })
}

fn oracle(filters: &[String], topic: &str) -> Vec<u64> {
    let mut ids: Vec<u64> = filters
        .iter()
        .enumerate()
        .filter(|(_, filter)| reference::matches(filter, topic))
        .map(|(index, _)| index as u64)
        .collect();
    ids.sort_unstable();
    ids
}

proptest! {
    /// The core claim: the sharded trie and the naive matcher never disagree.
    #[test]
    fn the_trie_agrees_with_the_reference_matcher(
        filters in prop::collection::vec(any_filter(), 1..12),
        topics in prop::collection::vec(any_topic(), 1..12),
        shards in 1usize..8,
    ) {
        let trie = SubscriptionTrie::new(shards);
        for (index, filter) in filters.iter().enumerate() {
            trie.insert(filter, sub(index as u64));
        }

        for topic in &topics {
            let mut matched = Vec::new();
            trie.matching(topic, &mut matched);
            let mut got: Vec<u64> = matched.into_iter().map(|s| s.subscriber.0).collect();
            got.sort_unstable();

            prop_assert_eq!(got, oracle(&filters, topic), "topic {:?}", topic);
        }
    }

    /// Whatever was inserted can be removed, leaving nothing behind. This
    /// catches pruning bugs that leave orphaned nodes holding subscriptions.
    #[test]
    fn inserting_then_removing_everything_empties_the_trie(
        filters in prop::collection::vec(any_filter(), 1..12),
    ) {
        let trie = SubscriptionTrie::new(4);
        for (index, filter) in filters.iter().enumerate() {
            trie.insert(filter, sub(index as u64));
        }
        for (index, filter) in filters.iter().enumerate() {
            trie.remove(filter, SubscriberId(index as u64));
        }
        prop_assert_eq!(trie.len(), 0);
    }

    /// Tearing down one session must not disturb another's subscriptions.
    #[test]
    fn removing_one_subscriber_leaves_the_others_intact(
        filters in prop::collection::vec(any_filter(), 1..8),
        topics in prop::collection::vec(any_topic(), 1..8),
    ) {
        let trie = SubscriptionTrie::new(4);
        // Two subscribers hold identical interests.
        for filter in &filters {
            trie.insert(filter, sub(1));
            trie.insert(filter, sub(2));
        }

        trie.remove_subscriber(SubscriberId(1));

        for topic in &topics {
            let mut matched = Vec::new();
            trie.matching(topic, &mut matched);
            let survivors: Vec<u64> = matched.into_iter().map(|s| s.subscriber.0).collect();
            prop_assert!(
                survivors.iter().all(|&id| id == 2),
                "subscriber 1 survived for {:?}",
                topic
            );

            let expected_any = filters.iter().any(|f| reference::matches(f, topic));
            prop_assert_eq!(!survivors.is_empty(), expected_any, "topic {:?}", topic);
        }
    }

    /// Insert order must not change what matches.
    #[test]
    fn matching_does_not_depend_on_insertion_order(
        filters in prop::collection::vec(any_filter(), 2..8),
        topic in any_topic(),
    ) {
        let forward = SubscriptionTrie::new(4);
        for (index, filter) in filters.iter().enumerate() {
            forward.insert(filter, sub(index as u64));
        }
        let backward = SubscriptionTrie::new(4);
        for (index, filter) in filters.iter().enumerate().rev() {
            backward.insert(filter, sub(index as u64));
        }

        let mut a = Vec::new();
        forward.matching(&topic, &mut a);
        let mut b = Vec::new();
        backward.matching(&topic, &mut b);

        let mut a: Vec<u64> = a.into_iter().map(|s| s.subscriber.0).collect();
        let mut b: Vec<u64> = b.into_iter().map(|s| s.subscriber.0).collect();
        a.sort_unstable();
        b.sort_unstable();
        prop_assert_eq!(a, b);
    }

    /// The retained store runs the traversal the other way round, so it
    /// gets its own comparison against the same oracle.
    #[test]
    fn the_retained_store_agrees_with_the_reference_matcher(
        stored in prop::collection::vec(any_topic(), 1..12),
        filters in prop::collection::vec(any_filter(), 1..12),
    ) {
        let store = RetainedStore::new();
        for topic in &stored {
            store.set(
                topic,
                RetainedMessage {
                    payload: Bytes::from_static(b"v"),
                    qos: QoS::AtMostOnce,
                    expires_at: None,
                },
            );
        }

        for filter in &filters {
            let mut matched = Vec::new();
            store.matching(filter, &mut matched);
            let mut got: Vec<String> = matched.into_iter().map(|(topic, _)| topic).collect();
            got.sort();
            got.dedup();

            let mut want: Vec<String> = stored
                .iter()
                .filter(|topic| reference::matches(filter, topic))
                .cloned()
                .collect();
            want.sort();
            want.dedup();

            prop_assert_eq!(got, want, "filter {:?}", filter);
        }
    }
}
```

- [ ] **Step 2: Run it**

Run: `cd services/broker && cargo test -p broker-core --test differential`
Expected: PASS, five properties, 256 generated cases each by default.

If a property fails, proptest prints a shrunk counterexample. **Treat that
as a real bug in the fast structure, not in the oracle**, unless you can
show from the specification that the oracle is wrong. The oracle was
checked against the specification in task 2.

- [ ] **Step 3: Prove the differential test has teeth**

Three mutations, each restored from a `cp` backup, each reported:

1. In `node.rs`, make `match_at` skip the single-level wildcard child.
   Confirm `the_trie_agrees_with_the_reference_matcher` fails.
2. In `trie.rs`, make `matching` consult the wildcard root even for dollar
   topics. Confirm the same property fails, with a `$SYS` counterexample.
3. In `retained.rs`, make the multi-level wildcard skip the parent level.
   Confirm `the_retained_store_agrees_with_the_reference_matcher` fails.

If any mutation does not produce a failure, the generators are not
reaching that code and need widening. Say so rather than moving on.

- [ ] **Step 4: Commit**

```bash
git add services/broker/crates/broker-core/tests
git commit -m "test(core): differential testing against the reference matcher"
```

---

## Task 9: Matching benchmarks

The design budget says a publish should not cost more than the depth of
its topic times wildcard branching. Nothing in this plan measures that, so
a regression would be invisible.

**Files:**
- Create: `services/broker/crates/broker-core/benches/matching.rs`
- Modify: `services/broker/crates/broker-core/Cargo.toml`

- [ ] **Step 1: Declare the benchmark**

Add to `services/broker/crates/broker-core/Cargo.toml`:

```toml
[dev-dependencies.criterion]
version = "0.8"
default-features = false
features = ["cargo_bench_support"]

[[bench]]
name = "matching"
harness = false
```

Disabling default features drops criterion's plotting stack, which pulls a
large tree we do not need for a number on a terminal.

- [ ] **Step 2: Write the benchmark**

Create `services/broker/crates/broker-core/benches/matching.rs`:

```rust
//! What a publish costs, against a trie of a realistic size.

use broker_core::subscriptions::{SubscriberId, Subscription, SubscriptionTrie};
use criterion::{criterion_group, criterion_main, Criterion};
use mqtt_codec::QoS;
use std::hint::black_box;

fn sub(id: u64) -> Subscription {
    Subscription {
        subscriber: SubscriberId(id),
        qos: QoS::AtMostOnce,
        no_local: false,
        retain_as_published: false,
        id: None,
    }
}

/// Ten thousand device-shaped subscriptions, the shape a fleet produces.
fn populated() -> SubscriptionTrie {
    let trie = SubscriptionTrie::new(64);
    let mut id = 0u64;
    for site in 0..100 {
        for device in 0..100 {
            trie.insert(&format!("site/{site}/device/{device}/telemetry"), sub(id));
            id += 1;
        }
        trie.insert(&format!("site/{site}/device/+/telemetry"), sub(id));
        id += 1;
    }
    trie.insert("site/#", sub(id));
    trie
}

fn benches(c: &mut Criterion) {
    let trie = populated();
    let mut out = Vec::with_capacity(16);

    c.bench_function("match one deep topic", |b| {
        b.iter(|| {
            out.clear();
            trie.matching(black_box("site/42/device/17/telemetry"), &mut out);
            black_box(out.len())
        })
    });

    c.bench_function("match a topic nothing wants", |b| {
        b.iter(|| {
            out.clear();
            trie.matching(black_box("other/thing/entirely"), &mut out);
            black_box(out.len())
        })
    });

    c.bench_function("subscribe then unsubscribe", |b| {
        b.iter(|| {
            trie.insert(black_box("bench/scratch/topic"), sub(u64::MAX));
            trie.remove(black_box("bench/scratch/topic"), SubscriberId(u64::MAX));
        })
    });
}

criterion_group!(matching, benches);
criterion_main!(matching);
```

- [ ] **Step 3: Run it**

Run: `cd services/broker && cargo bench -p broker-core`
Expected: three timings. Record them in your report. There is no threshold
to meet yet; the point is a baseline to notice a regression against.

Confirm the first benchmark returns a non-zero match count, so it is not
timing an empty walk. If it returns zero, the fixture is wrong.

- [ ] **Step 4: Confirm the license gate still passes**

Run, from the repository root:

```bash
cargo deny --manifest-path services/broker/Cargo.toml --config deny.toml check licenses bans sources
```

criterion pulls a moderate tree. If anything in it falls outside the allow
list, **stop and report rather than widening the list**. The policy is
MIT-compatible permissive, and a benchmarking tool is not a reason to
relax it.

- [ ] **Step 5: Commit**

```bash
git add services/broker/crates/broker-core services/broker/Cargo.lock
git commit -m "bench(core): baseline matching timings"
```

---

## Amendments after review

Changes made during execution, after review found problems with the plan as
written. Recorded so the plan matches the code.

**Retained expiry is checked on read** (commit `6a59062`). `matching`
returned expired messages until a sweep ran, while the field was documented
as a deadline past which a message is discarded. The next plan calls
`matching` to deliver retained messages on subscribe, so as written it
would have delivered expired ones. `matching` now takes the current time
and shares one expiry helper with the sweep so the two cannot drift.

**The trie node and the interner are crate-private** (same commit). Both
were public, and every node method takes a caller-supplied interner.
Passing the wrong one makes every literal level miss silently and matching
return wrong answers with no error. `topic::reference` stays public because
the differential test needs it. Three now-dead methods went with them.

**The generators could not produce a bare multi-level wildcard** (commit
`668ec79`). This was the real finding. A reviewer proved it by deleting the
dollar-topic guard in the retained store, which only a bare `#` reaches,
and watching all five properties stay green at 20,000 cases. That mutation
is exactly the failure of exposing broker status topics to a
subscribe-to-everything client. With the widened generators the same
deletion fails in the default 256 cases, on `stored = ["$SYS"], filters =
["#"]`. A bare single-level wildcard turned out to be reachable already.

**Pruning is asserted, not assumed** (same commit). The removal property
checked the subscription count, which an orphaned empty branch passes. The
retained store already exposed a node count for exactly this reason; the
trie now does too.

**Traversal depth has a ceiling** (commit `efcc8a3`). Every traversal
recurses per topic level, and the only bound was a `u16` configuration
value with no upper limit, so an operator could crash the broker by raising
it. Measured: a worker thread on a 2 MiB stack survives about 6,500 levels
and aborts by 7,000. The codec now clamps the depth check to a ceiling of
1024, roughly six times inside the measured limit.

**An optimisation that does not work, recorded so nobody repeats it**
(commit `26c1944`). The interner takes a read lock per level per node
visited. Hoisting every lookup under one guard into a vector is the obvious
fix and measures 30 percent slower on a match and 53 percent slower on a
miss, because it trades an uncontended read lock for an allocation per
publish and gives up the early exit on the first unknown level, which is
most of the work on a topic nobody wants.

## Done when

- `cargo test -p broker-core` passes, covering unit tests and the five
  differential properties.
- `cargo clippy --all-targets -- -D warnings` is clean.
- `cargo fmt --all --check` shows no diff.
- `cargo deny check` passes with the allow list unchanged.
- Every mutation listed in tasks 4, 5, 6 and 8 produced the failure it
  should have, and the tree is clean afterwards.
- `cargo bench -p broker-core` produces a baseline.

## What this plan deliberately leaves out

Named so the next plan can pick them up, and so their absence is not
mistaken for an oversight.

- **Sessions, queues and quality-of-service state machines.** Nothing here
  delivers a message; it only answers who wants one.
- **The selection policy for shared subscriptions.** Round-robin is a
  placeholder. Preferring a local online member and then the least loaded
  remote node needs session and cluster state.
- **Retained message delivery on subscribe**, including the three
  retain-handling options, which is a session concern.
- **The `$SYS` tree.** Publishing broker statistics is a later concern,
  though the matcher already keeps a leading wildcard away from it.
- **Persistence.** Everything here is in memory. The storage trait comes in
  the milestone after next.

## Next plan

Sessions and delivery: the session actor, the quality-of-service state
machines, the inflight window and queues with their overflow policy, wills
including the MQTT 5 delay, and retained delivery on subscribe. It consumes
this crate and changes nothing in it.
