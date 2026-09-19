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
        // Truncating here would hand out an identifier already in use,
        // and two different levels comparing equal is a false match with
        // nothing to signal it. Unreachable short of four billion
        // distinct levels, but it is the one place in the matching path
        // where a silently wrong answer could originate, so it aborts
        // rather than wraps.
        let next = u32::try_from(inner.to_text.len()).expect("level identifier space exhausted");
        let id = LevelId(next);
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

    /// How many distinct levels have been interned.
    pub fn len(&self) -> usize {
        self.inner.read().to_text.len()
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
    fn lookup_does_not_intern() {
        let interner = Interner::new();
        assert_eq!(interner.lookup("sport"), None);
        let id = interner.intern("sport");
        assert_eq!(interner.lookup("sport"), Some(id));
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
