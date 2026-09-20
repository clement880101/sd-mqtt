//! A clock the broker is given rather than one it reads.

use std::sync::atomic::{AtomicU32, Ordering};

/// Whole seconds since an arbitrary origin, advanced explicitly.
///
/// Every deadline in the broker is in whole seconds, and reading the
/// operating system clock per packet would be waste. More importantly,
/// taking time as a parameter rather than reading it makes the whole
/// broker deterministic: a test advances thirty seconds in a microsecond
/// and a will delay is exercised exactly, with no sleeping and no flakes.
///
/// One thread advances it, once a second. Everyone else reads.
pub struct CoarseClock {
    now: AtomicU32,
}

impl CoarseClock {
    /// A clock reading `start`.
    pub fn new(start: u32) -> Self {
        Self {
            now: AtomicU32::new(start),
        }
    }

    /// The current reading.
    ///
    /// Relaxed ordering is deliberate. Readers need a recent value, not a
    /// synchronised one; a deadline that fires a few microseconds late is
    /// indistinguishable from one that fires on a coarse second boundary.
    pub fn now(&self) -> u32 {
        self.now.load(Ordering::Relaxed)
    }

    /// Move forward by `seconds`, returning the new reading.
    pub fn advance(&self, seconds: u32) -> u32 {
        self.now.fetch_add(seconds, Ordering::Relaxed) + seconds
    }

    /// The reading `interval` seconds from now, saturating.
    ///
    /// Saturating matters: a session expiry near the end of the range must
    /// not wrap into the past, which would expire the session the moment
    /// it was created.
    pub fn deadline(&self, interval: u32) -> u32 {
        self.now().saturating_add(interval)
    }
}

impl Default for CoarseClock {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn starts_where_it_is_told() {
        let clock = CoarseClock::new(100);
        assert_eq!(clock.now(), 100);
    }

    #[test]
    fn advances_by_whole_seconds() {
        let clock = CoarseClock::new(0);
        assert_eq!(clock.advance(1), 1);
        assert_eq!(clock.advance(29), 30);
        assert_eq!(clock.now(), 30);
    }

    #[test]
    fn a_deadline_is_a_sum_that_saturates() {
        // A session expiry of the maximum interval must not wrap into the
        // past, which would expire the session immediately.
        let clock = CoarseClock::new(u32::MAX - 5);
        assert_eq!(clock.deadline(100), u32::MAX);
        assert_eq!(CoarseClock::new(0).deadline(100), 100);
    }

    #[test]
    fn is_shared_without_locking() {
        let clock = Arc::new(CoarseClock::new(0));
        let reader = Arc::clone(&clock);
        clock.advance(7);
        assert_eq!(reader.now(), 7);
    }

    #[test]
    fn many_threads_see_a_monotonic_value() {
        // The ticker is one thread; everyone else reads. A reader must
        // never see time go backwards.
        let clock = Arc::new(CoarseClock::new(0));
        let ticker = Arc::clone(&clock);
        let handle = std::thread::spawn(move || {
            for _ in 0..1000 {
                ticker.advance(1);
            }
        });

        let mut last = 0;
        for _ in 0..10_000 {
            let now = clock.now();
            assert!(now >= last, "time went backwards: {last} then {now}");
            last = now;
        }
        handle.join().expect("the ticker finished");
        assert_eq!(clock.now(), 1000);
    }
}
