//! When to try the remote again.
//!
//! Section 9.2: "Reconnection uses exponential backoff with jitter."
//! Both halves matter and they answer different problems.
//!
//! **Exponential** is about the remote. A bridge that reconnects every
//! second to a broker which is down is a bridge holding that broker's
//! accept queue open while it tries to come back, and the failure mode
//! is a remote that never quite recovers because its clients will not
//! let it.
//!
//! **Jitter** is about the fleet. Backoff alone synchronises: every
//! bridge that lost the same remote at the same moment retries at the
//! same moment, forever, in a tighter and tighter convoy as the waits
//! double together. A wait picked uniformly from zero to the current
//! ceiling is the "full jitter" of the usual analysis, and it is the
//! spelling chosen here because it is the one that actually spreads
//! the herd rather than narrowing the band it arrives in. The cost is
//! that a single bridge sometimes retries sooner than the ceiling
//! suggests, which for a single bridge is not a cost.
//!
//! # No `rand`
//!
//! A dependency for one `u64` per reconnection, weighed against
//! section 15's budget, is not worth it. [`Entropy`] is SplitMix64 over
//! the clock, which is a well-studied finaliser and is being asked for
//! a jitter rather than for a key. [`Backoff::wait`] takes the number
//! as an argument so that the schedule can be asserted without one.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A source of jitter.
///
/// SplitMix64: one addition of the golden-gamma constant and three
/// xor-shift-multiply rounds. Seeded from the clock, because the one
/// thing a reconnection jitter must not be is the same on every node
/// that started from the same configuration file.
#[derive(Debug, Clone)]
pub struct Entropy {
    state: u64,
}

/// SplitMix64's increment, the odd 64-bit approximation of the golden
/// ratio that the algorithm is defined with.
const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

impl Entropy {
    /// A source seeded from the clock and from `salt`.
    ///
    /// The salt is the bridge's own name. Two bridges on one node that
    /// lost two remotes in the same nanosecond should still not wake
    /// together, and the clock alone cannot tell them apart.
    pub fn new(salt: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64);
        let mut state = now ^ GAMMA;
        for byte in salt.bytes() {
            state = state.rotate_left(5) ^ u64::from(byte);
        }
        Self { state }
    }

    /// A source with a known seed, for a test that wants a schedule it
    /// can write down.
    pub fn seeded(seed: u64) -> Self {
        Self { state: seed }
    }

    /// The next number.
    pub fn draw(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GAMMA);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// The reconnection schedule for one bridge.
#[derive(Debug, Clone)]
pub struct Backoff {
    /// The first ceiling, and the one a reset returns to.
    min: Duration,
    /// The ceiling the doubling stops at.
    max: Duration,
    /// How many failures in a row, which is what the ceiling doubles
    /// on.
    failures: u32,
}

impl Backoff {
    /// A schedule between `min_ms` and `max_ms`.
    ///
    /// A `min` of zero would make every ceiling zero and the schedule
    /// a spin, so it is lifted to a millisecond; a `max` below `min` is
    /// lifted to `min`, because the alternative is a range with no
    /// numbers in it. Neither is a configuration anybody means, and
    /// both are answered here rather than left to produce a bridge
    /// that hammers a dead remote.
    pub fn new(min_ms: u32, max_ms: u32) -> Self {
        let min = Duration::from_millis(u64::from(min_ms.max(1)));
        Self {
            min,
            max: Duration::from_millis(u64::from(max_ms)).max(min),
            failures: 0,
        }
    }

    /// Forget the failures, because the connection worked.
    ///
    /// **Called when the session is established, not when the socket
    /// opens.** A remote that accepts a connection and then refuses the
    /// CONNECT is a remote that is up and saying no, and a bridge that
    /// reset its backoff on the socket would retry that refusal every
    /// `min` for as long as it stood.
    pub fn succeeded(&mut self) {
        self.failures = 0;
    }

    /// How long to wait before the next attempt, given one random
    /// number.
    ///
    /// The ceiling is `min` doubled once per failure so far, capped at
    /// `max`; the wait is picked uniformly below the ceiling. Taking
    /// the number as an argument rather than drawing it is what lets a
    /// test assert the ceiling rather than assert that a wait happened.
    pub fn wait(&mut self, entropy: u64) -> Duration {
        let ceiling = self.ceiling();
        self.failures = self.failures.saturating_add(1);
        let span = ceiling.as_millis().max(1) as u64;
        Duration::from_millis(entropy % span)
    }

    /// The current ceiling, before this attempt is counted.
    ///
    /// Saturating rather than wrapping: a shift of sixty-four is
    /// undefined in the abstract machine and zero in the obvious
    /// implementation, and a ceiling that fell back to zero after an
    /// hour of failures would turn the longest outage into the
    /// tightest retry loop. The one place a wrong answer here is not
    /// visible until production.
    pub fn ceiling(&self) -> Duration {
        let doublings = self.failures.min(u32::BITS - 1);
        self.min
            .saturating_mul(1u32.checked_shl(doublings).unwrap_or(u32::MAX))
            .min(self.max)
    }

    /// How many failures in a row this has seen.
    pub fn failures(&self) -> u32 {
        self.failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ceiling_doubles_and_then_stops() {
        let mut backoff = Backoff::new(100, 1_000);
        let ceilings: Vec<u64> = (0..8)
            .map(|_| {
                let ceiling = backoff.ceiling().as_millis() as u64;
                backoff.wait(0);
                ceiling
            })
            .collect();
        assert_eq!(
            ceilings,
            vec![100, 200, 400, 800, 1_000, 1_000, 1_000, 1_000]
        );
    }

    /// The wait is below the ceiling and not equal to it, which is the
    /// whole of "full jitter": two bridges that failed together do not
    /// wake together.
    #[test]
    fn a_wait_is_somewhere_below_the_ceiling() {
        let mut backoff = Backoff::new(100, 10_000);
        backoff.wait(0);
        backoff.wait(0);
        let ceiling = backoff.ceiling();
        let mut seen = std::collections::BTreeSet::new();
        for entropy in [0u64, 1, 17, 199, 4_000_000_009, u64::MAX] {
            let mut same = backoff.clone();
            let wait = same.wait(entropy);
            assert!(wait < ceiling, "{wait:?} should be below {ceiling:?}");
            seen.insert(wait);
        }
        assert!(
            seen.len() > 1,
            "different entropy must give different waits, or there is no jitter"
        );
    }

    #[test]
    fn a_success_puts_the_ceiling_back_where_it_started() {
        let mut backoff = Backoff::new(100, 10_000);
        for _ in 0..5 {
            backoff.wait(0);
        }
        assert!(backoff.ceiling() > Duration::from_millis(100));
        backoff.succeeded();
        assert_eq!(backoff.ceiling(), Duration::from_millis(100));
        assert_eq!(backoff.failures(), 0);
    }

    /// The failure that is invisible until an outage lasts an hour: a
    /// shift that wrapped would send the ceiling back to nothing and
    /// turn the longest outage into the tightest loop.
    #[test]
    fn a_very_long_outage_stays_at_the_maximum() {
        let mut backoff = Backoff::new(1, 60_000);
        for _ in 0..10_000 {
            backoff.wait(0);
        }
        assert_eq!(backoff.ceiling(), Duration::from_millis(60_000));
    }

    #[test]
    fn a_configuration_that_would_spin_is_lifted() {
        let mut zero = Backoff::new(0, 0);
        assert_eq!(zero.ceiling(), Duration::from_millis(1));
        assert!(zero.wait(7) < Duration::from_millis(2));

        // A maximum below the minimum is a range with nothing in it.
        let backwards = Backoff::new(5_000, 100);
        assert_eq!(backwards.ceiling(), Duration::from_millis(5_000));
    }

    #[test]
    fn two_bridges_with_the_same_configuration_do_not_share_a_schedule() {
        let mut one = Entropy::new("bridge-a");
        let mut two = Entropy::new("bridge-b");
        let first: Vec<u64> = (0..4).map(|_| one.draw()).collect();
        let second: Vec<u64> = (0..4).map(|_| two.draw()).collect();
        assert_ne!(first, second, "the salt must separate them");
    }

    #[test]
    fn the_same_seed_gives_the_same_numbers() {
        let mut one = Entropy::seeded(12_345);
        let mut two = Entropy::seeded(12_345);
        for _ in 0..16 {
            assert_eq!(one.draw(), two.draw());
        }
        let mut other = Entropy::seeded(12_346);
        assert_ne!(Entropy::seeded(12_345).draw(), other.draw());
    }
}
