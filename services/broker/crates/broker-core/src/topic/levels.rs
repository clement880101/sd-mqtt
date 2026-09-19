//! Interning of topic levels, so comparisons are integer comparisons.

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
