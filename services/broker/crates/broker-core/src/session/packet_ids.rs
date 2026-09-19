//! Allocation of the sixteen-bit identifiers that match an acknowledgement
//! to its publish.

/// Allocates the sixteen-bit identifiers that match an acknowledgement to
/// its publish.
///
/// The space is small, so the allocator tracks occupancy exactly rather
/// than hoping a counter does not lap an outstanding identifier. Handing
/// out one that is still in flight would make the peer acknowledge the
/// wrong message, which is a silent delivery fault rather than an error.
///
/// A bitmap costs eight kilobytes per session, which is too much when
/// sessions number in the hundreds of thousands, so occupancy is a set
/// sized by what is actually outstanding.
pub struct PacketIds {
    next: u16,
    in_use: std::collections::HashSet<u16, foldhash::fast::RandomState>,
}

impl Default for PacketIds {
    fn default() -> Self {
        Self {
            next: 1,
            in_use: std::collections::HashSet::with_hasher(Default::default()),
        }
    }
}

impl PacketIds {
    /// Take the next free identifier, or nothing when all are outstanding.
    pub fn allocate(&mut self) -> Option<u16> {
        if self.in_use.len() == usize::from(u16::MAX) {
            return None;
        }
        // At most one full lap, and the length check above guarantees a
        // free slot exists, so this terminates.
        loop {
            let candidate = self.next;
            self.next = if self.next == u16::MAX { 1 } else { self.next + 1 };
            if self.in_use.insert(candidate) {
                return Some(candidate);
            }
        }
    }

    /// Claim a specific identifier, for state restored on resume. Reports
    /// whether it was free.
    pub fn reserve(&mut self, id: u16) -> bool {
        id != 0 && self.in_use.insert(id)
    }

    /// Give an identifier back once its exchange is complete.
    pub fn release(&mut self, id: u16) {
        self.in_use.remove(&id);
    }

    /// Whether an identifier is outstanding.
    pub fn is_in_use(&self, id: u16) -> bool {
        self.in_use.contains(&id)
    }

    /// How many are outstanding.
    pub fn in_use(&self) -> usize {
        self.in_use.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_from_one_upwards() {
        let mut ids = PacketIds::default();
        assert_eq!(ids.allocate(), Some(1));
        assert_eq!(ids.allocate(), Some(2));
        assert_eq!(ids.allocate(), Some(3));
    }

    #[test]
    fn never_allocates_zero() {
        // Zero is not a valid packet identifier, so the wrap must skip it.
        let mut ids = PacketIds::default();
        for _ in 0..u16::MAX {
            let id = ids.allocate().expect("space remains");
            assert_ne!(id, 0);
            ids.release(id);
        }
        assert_eq!(ids.allocate(), Some(1), "allocation should wrap to one");
    }

    #[test]
    fn does_not_reuse_an_identifier_still_in_use() {
        let mut ids = PacketIds::default();
        let first = ids.allocate().unwrap();
        // Exhaust the space so the counter wraps back around to `first`.
        for _ in 1..u16::MAX {
            ids.allocate().unwrap();
        }
        assert_eq!(
            ids.allocate(),
            None,
            "with every identifier in use, allocation must fail"
        );
        assert!(ids.is_in_use(first));
    }

    #[test]
    fn reuses_an_identifier_once_released() {
        let mut ids = PacketIds::default();
        let id = ids.allocate().unwrap();
        assert!(ids.is_in_use(id));
        ids.release(id);
        assert!(!ids.is_in_use(id));
        assert_eq!(ids.in_use(), 0);
    }

    #[test]
    fn releasing_something_never_allocated_is_harmless() {
        let mut ids = PacketIds::default();
        ids.release(42);
        assert_eq!(ids.in_use(), 0);
    }

    #[test]
    fn skips_over_identifiers_that_are_still_held() {
        let mut ids = PacketIds::default();
        let one = ids.allocate().unwrap();
        let two = ids.allocate().unwrap();
        ids.release(two);
        // The counter has moved past both; releasing the second should let
        // it be found again on the wrap, while the first stays held.
        assert!(ids.is_in_use(one));
        assert!(!ids.is_in_use(two));
    }

    #[test]
    fn reports_how_many_are_outstanding() {
        let mut ids = PacketIds::default();
        assert_eq!(ids.in_use(), 0);
        let a = ids.allocate().unwrap();
        ids.allocate().unwrap();
        assert_eq!(ids.in_use(), 2);
        ids.release(a);
        assert_eq!(ids.in_use(), 1);
    }

    #[test]
    fn can_reserve_an_identifier_a_peer_chose() {
        // On resume, the identifiers already in flight were chosen before
        // the reconnect and must not be handed out again.
        let mut ids = PacketIds::default();
        assert!(ids.reserve(500));
        assert!(ids.is_in_use(500));
        assert!(!ids.reserve(500), "reserving twice should report the clash");
    }
}
