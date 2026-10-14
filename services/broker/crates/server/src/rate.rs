//! Section 10.3's two rate limiters, and the algorithm they share.
//!
//! # The generic cell rate algorithm, on the coarse clock
//!
//! A limiter is one number, the *theoretical arrival time*: when the
//! sender would have caught up if it had kept exactly to the rate. A
//! cell is admitted when admitting it — moving that time on by one
//! interval — leaves it no more than one burst ahead of now. That is
//! the whole of the state, which is why the publish cap fits in one
//! 64-bit field and costs no map lookup.
//!
//! Now is the broker's coarse clock, read and never asked of the
//! operating system: whole seconds, held here as nanoseconds so an
//! interval shorter than a second is exact. Because now moves a whole
//! second at a time, **a burst below one second's worth of the rate
//! would cap a sender below the rate itself** — inside one coarse
//! second only the burst is ever admitted — so a burst is never less
//! than the rate. [`Gcra::new`] makes it so.
//!
//! # Publishing
//!
//! [`PublishLimit`] is the per-session cap: an operator's
//! `max_publish_rate` with its burst, checked as each PUBLISH arrives.
//! Over it, a version 5 client is told DISCONNECT 0x96, "message rate
//! too high", and a 3.1.1 one is closed: neither version has a way to
//! refuse one publication for rate and keep the connection.
//!
//! # Authentication failures
//!
//! [`AuthFailures`] is the per-address limiter: a map from address to
//! arrival time behind one mutex — failures are rare, so one is
//! enough — swept by the one-second tick of every entry that has
//! drained. Only a failure charges it. While an address is over its
//! limit, a CONNECT from it is refused **before its credentials are
//! looked at**, so a guesser learns nothing from the attempts the
//! limiter turned away. An IPv6 address is counted by its /64, which is
//! what one host is usually given; counting each address would let one
//! host try as many times as it has addresses to spare.
//!
//! The map is bounded. A full map tracks no new address and says so
//! once a sweep, rather than growing without limit under a flood from
//! many addresses: the limiter exists to slow one guesser, and memory
//! is the broker's to protect first.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};

/// Nanoseconds in a coarse second.
const SECOND: u64 = 1_000_000_000;

/// How many addresses the authentication limiter tracks at most.
pub const MAX_TRACKED_ADDRESSES: usize = 65_536;

/// A rate and its burst, as the algorithm holds them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Gcra {
    /// Nanoseconds between cells at exactly the rate.
    interval: u64,
    /// How far ahead of now the arrival time may run: a burst's worth
    /// of intervals.
    tolerance: u64,
}

impl Gcra {
    /// `cells` per `period_seconds`, with `burst` admitted at once.
    ///
    /// `None` for a rate of zero, which is no limit. A burst below the
    /// rate per coarse second is raised to it; see the module notes.
    #[must_use]
    pub fn new(cells: u32, period_seconds: u32, burst: u32) -> Option<Self> {
        if cells == 0 || period_seconds == 0 {
            return None;
        }
        let interval = (u64::from(period_seconds) * SECOND / u64::from(cells)).max(1);
        let per_second = u64::from(cells).div_ceil(u64::from(period_seconds));
        let burst = u64::from(burst).max(per_second).max(1);
        Some(Self {
            interval,
            tolerance: interval.saturating_mul(burst),
        })
    }

    /// Admit one cell at `now` (coarse seconds) against `arrival`,
    /// moving it on when admitted.
    pub fn admit(&self, arrival: &mut u64, now: u32) -> bool {
        let now = u64::from(now) * SECOND;
        let next = (*arrival).max(now).saturating_add(self.interval);
        if next - now > self.tolerance {
            return false;
        }
        *arrival = next;
        true
    }

    /// Whether a cell at `now` would be refused, without taking one.
    #[must_use]
    pub fn exhausted(&self, arrival: u64, now: u32) -> bool {
        let now = u64::from(now) * SECOND;
        arrival.max(now).saturating_add(self.interval) - now > self.tolerance
    }

    /// Charge one cell whether or not it fits: a failure has happened
    /// and is counted, admitted or not.
    pub fn charge(&self, arrival: &mut u64, now: u32) {
        let now = u64::from(now) * SECOND;
        *arrival = (*arrival).max(now).saturating_add(self.interval);
    }

    /// Whether an arrival time has drained by `now`: the entry holding
    /// it says nothing a fresh one would not.
    #[must_use]
    pub fn drained(arrival: u64, now: u32) -> bool {
        arrival <= u64::from(now) * SECOND
    }
}

/// The per-session publish cap. One field of state.
#[derive(Debug)]
pub struct PublishLimit {
    rate: Gcra,
    arrival: AtomicU64,
}

impl PublishLimit {
    /// A cap of `per_second` publications with `burst` at once; `None`
    /// when `per_second` is zero.
    #[must_use]
    pub fn new(per_second: u32, burst: u32) -> Option<Self> {
        Gcra::new(per_second, 1, burst).map(|rate| Self {
            rate,
            arrival: AtomicU64::new(0),
        })
    }

    /// Admit one publication at `now`.
    ///
    /// One connection's reader is the only caller, so a load and a
    /// store cannot interleave with another admission; the atomic is
    /// there because the connection is shared between tasks, not for
    /// contention.
    pub fn admit(&self, now: u32) -> bool {
        let mut arrival = self.arrival.load(Ordering::Relaxed);
        let admitted = self.rate.admit(&mut arrival, now);
        if admitted {
            self.arrival.store(arrival, Ordering::Relaxed);
        }
        admitted
    }
}

/// What an address is counted under: itself for IPv4, its /64 for
/// IPv6, and an IPv4 address mapped into IPv6 as the IPv4 address it is.
#[must_use]
pub fn address_key(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V4(_) => address,
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => {
                let mut segments = v6.segments();
                segments[4..].fill(0);
                IpAddr::V6(segments.into())
            }
        },
    }
}

/// The per-address authentication failure limiter.
#[derive(Debug)]
pub struct AuthFailures {
    rate: Option<Gcra>,
    arrivals: HashMap<IpAddr, u64>,
    /// Whether a failure went untracked since the last sweep, so the
    /// warning is said once a second and not once a failure.
    overflowed: bool,
}

impl AuthFailures {
    /// `per_minute` failures per address, all of them at once if they
    /// come together; zero is no limit.
    #[must_use]
    pub fn new(per_minute: u32) -> Self {
        Self {
            rate: Gcra::new(per_minute, 60, per_minute),
            arrivals: HashMap::new(),
            overflowed: false,
        }
    }

    /// Whether `address` may not try now.
    #[must_use]
    pub fn limited(&self, address: IpAddr, now: u32) -> bool {
        let Some(rate) = &self.rate else {
            return false;
        };
        self.arrivals
            .get(&address_key(address))
            .is_some_and(|arrival| rate.exhausted(*arrival, now))
    }

    /// `address` failed to authenticate at `now`.
    pub fn failed(&mut self, address: IpAddr, now: u32) {
        let Some(rate) = self.rate else {
            return;
        };
        let key = address_key(address);
        if !self.arrivals.contains_key(&key) && self.arrivals.len() >= MAX_TRACKED_ADDRESSES {
            self.overflowed = true;
            return;
        }
        rate.charge(self.arrivals.entry(key).or_insert(0), now);
    }

    /// Drop every address that has drained by `now`. Answers how many
    /// are still held, and whether one went untracked since the last
    /// sweep.
    pub fn sweep(&mut self, now: u32) -> (usize, bool) {
        self.arrivals
            .retain(|_, arrival| !Gcra::drained(*arrival, now));
        let overflowed = std::mem::take(&mut self.overflowed);
        (self.arrivals.len(), overflowed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    /// A burst is admitted at once, the next cell waits, and a second
    /// of the coarse clock gives back exactly a second's worth.
    #[test]
    fn a_burst_then_the_rate_and_no_more() {
        let limit = PublishLimit::new(10, 20).expect("a limit");
        for at in 0..20 {
            assert!(limit.admit(100), "cell {at} of the burst");
        }
        assert!(!limit.admit(100), "one past the burst");
        // Still refused within the same coarse second.
        assert!(!limit.admit(100));
        // One second on: ten more, then refused.
        for at in 0..10 {
            assert!(limit.admit(101), "cell {at} of the next second");
        }
        assert!(!limit.admit(101));
        // A quiet minute refills to the burst and no further.
        for at in 0..20 {
            assert!(limit.admit(160), "cell {at} after a rest");
        }
        assert!(!limit.admit(160));
    }

    /// A burst below one coarse second's worth would cap below the
    /// rate, so it is raised to the rate.
    #[test]
    fn a_burst_below_the_rate_is_raised_to_it() {
        let limit = PublishLimit::new(50, 1).expect("a limit");
        for second in 0..5 {
            for at in 0..50 {
                assert!(limit.admit(second), "second {second}, cell {at}");
            }
            assert!(!limit.admit(second), "second {second}, one over");
        }
    }

    /// Zero is no limit, not a limit of zero.
    #[test]
    fn a_rate_of_zero_is_no_limit() {
        assert!(PublishLimit::new(0, 0).is_none());
        let mut failures = AuthFailures::new(0);
        let here = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        for _ in 0..1000 {
            failures.failed(here, 5);
        }
        assert!(!failures.limited(here, 5));
        assert_eq!(failures.sweep(5).0, 0, "nothing was tracked");
    }

    /// Failures are counted per address; the limit is per minute and
    /// drains; a sweep forgets a drained address.
    #[test]
    fn failures_limit_one_address_and_drain_over_the_minute() {
        let mut failures = AuthFailures::new(5);
        let guesser = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7));
        let bystander = IpAddr::V4(Ipv4Addr::new(198, 51, 100, 8));
        for at in 0..5 {
            assert!(!failures.limited(guesser, 10), "before failure {at}");
            failures.failed(guesser, 10);
        }
        assert!(failures.limited(guesser, 10), "five in a minute");
        assert!(!failures.limited(bystander, 10), "and only that address");
        // Twelve seconds is one failure's worth of a five-a-minute rate.
        assert!(failures.limited(guesser, 21));
        assert!(!failures.limited(guesser, 22));
        assert_eq!(failures.sweep(22).0, 1, "not drained yet");
        assert_eq!(failures.sweep(69).0, 1);
        assert_eq!(failures.sweep(70).0, 0, "a minute after the burst");
        assert!(!failures.limited(guesser, 70));
    }

    /// One host's /64 is one address; a mapped IPv4 address is the
    /// IPv4 address.
    #[test]
    fn an_ipv6_host_is_counted_by_its_prefix() {
        let mut failures = AuthFailures::new(2);
        let one: Ipv6Addr = "2001:db8:1:2::1".parse().expect("an address");
        let same_prefix: Ipv6Addr = "2001:db8:1:2:ffff::9".parse().expect("an address");
        let next_prefix: Ipv6Addr = "2001:db8:1:3::1".parse().expect("an address");
        failures.failed(IpAddr::V6(one), 0);
        failures.failed(IpAddr::V6(same_prefix), 0);
        assert!(failures.limited(IpAddr::V6(one), 0));
        assert!(failures.limited(IpAddr::V6(same_prefix), 0));
        assert!(!failures.limited(IpAddr::V6(next_prefix), 0));

        let v4 = Ipv4Addr::new(203, 0, 113, 5);
        failures.failed(IpAddr::V4(v4), 0);
        failures.failed(IpAddr::V6(v4.to_ipv6_mapped()), 0);
        assert!(failures.limited(IpAddr::V4(v4), 0));
    }

    /// A full map tracks no new address, says so once, and keeps the
    /// ones it has.
    #[test]
    fn a_full_map_tracks_nobody_new_and_says_so() {
        let mut failures = AuthFailures::new(1);
        for index in 0..MAX_TRACKED_ADDRESSES {
            let index = u32::try_from(index).expect("small");
            failures.failed(IpAddr::V4(Ipv4Addr::from(0x0a00_0000 | index)), 0);
        }
        let late = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 200));
        failures.failed(late, 0);
        assert!(!failures.limited(late, 0), "untracked");
        assert!(failures.limited(IpAddr::V4(Ipv4Addr::from(0x0a00_0000)), 0));
        assert_eq!(failures.sweep(0), (MAX_TRACKED_ADDRESSES, true));
        assert_eq!(
            failures.sweep(0),
            (MAX_TRACKED_ADDRESSES, false),
            "said once"
        );
    }
}
