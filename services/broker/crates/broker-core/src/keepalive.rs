//! Disconnecting clients that have gone quiet, at scale.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::subscriptions::SubscriberId;

/// When a connection was last heard from.
///
/// Shared with the connection so that an arriving packet performs one
/// relaxed store and touches no timer at all. That is the whole reason
/// this is a ring rather than a timer per connection: at a hundred
/// thousand connections, rescheduling a timer per packet means a
/// contended lock per packet.
#[derive(Clone)]
pub struct Activity(Arc<AtomicU32>);

impl Activity {
    /// An activity cell reading `now`.
    pub fn new(now: u32) -> Self {
        Self(Arc::new(AtomicU32::new(now)))
    }

    /// Record that something arrived. One relaxed store, nothing else.
    pub fn touch(&self, now: u32) {
        self.0.store(now, Ordering::Relaxed);
    }

    /// When something last arrived.
    pub fn last(&self) -> u32 {
        self.0.load(Ordering::Relaxed)
    }
}

struct Armed {
    id: SubscriberId,
    /// The negotiated interval in seconds. Zero disables the deadline.
    keep_alive: u16,
    activity: Activity,
}

/// Connections arranged by when they next need checking.
///
/// One timer drives the whole process. Each tick inspects one bucket, so
/// the work per second is the population divided by the number of
/// buckets rather than the whole population.
pub struct KeepAliveRing {
    buckets: Vec<Vec<Armed>>,
    cursor: usize,
    count: usize,
    last_examined: usize,
}

/// A client is disconnected after this multiple of its interval.
const LIMIT_NUMERATOR: u32 = 3;
const LIMIT_DENOMINATOR: u32 = 2;

impl KeepAliveRing {
    /// A ring sized for keep-alive intervals up to `max_keep_alive`
    /// seconds. A larger interval still works, by re-arming.
    pub fn new(max_keep_alive: u16) -> Self {
        let slots = (u32::from(max_keep_alive) * LIMIT_NUMERATOR / LIMIT_DENOMINATOR) as usize + 2;
        Self {
            buckets: (0..slots.max(2)).map(|_| Vec::new()).collect(),
            cursor: 0,
            count: 0,
            last_examined: 0,
        }
    }

    fn limit(keep_alive: u16) -> u32 {
        u32::from(keep_alive) * LIMIT_NUMERATOR / LIMIT_DENOMINATOR
    }

    /// Arm a connection.
    pub fn insert(&mut self, id: SubscriberId, keep_alive: u16, activity: Activity, now: u32) {
        let armed = Armed {
            id,
            keep_alive,
            activity,
        };
        self.count += 1;
        self.arm(armed, now);
    }

    /// Place a connection in the bucket where it next needs checking.
    fn arm(&mut self, armed: Armed, now: u32) {
        let slots = self.buckets.len();
        if armed.keep_alive == 0 {
            // No deadline. Park it a full revolution away so it is
            // revisited rarely and costs almost nothing.
            let slot = (self.cursor + slots - 1) % slots;
            self.buckets[slot].push(armed);
            return;
        }

        let due = armed
            .activity
            .last()
            .saturating_add(Self::limit(armed.keep_alive));
        let ahead = due.saturating_sub(now).max(1) as usize;
        let slot = (self.cursor + ahead.min(slots - 1)) % slots;
        self.buckets[slot].push(armed);
    }

    /// Disarm a connection.
    pub fn remove(&mut self, id: SubscriberId) {
        for bucket in &mut self.buckets {
            let before = bucket.len();
            bucket.retain(|a| a.id != id);
            self.count -= before - bucket.len();
        }
    }

    /// Advance to `now`, appending every connection that has gone quiet
    /// past its limit. Anything still alive is re-armed.
    pub fn tick(&mut self, now: u32, expired: &mut Vec<SubscriberId>) {
        self.cursor = (self.cursor + 1) % self.buckets.len();
        let due = std::mem::take(&mut self.buckets[self.cursor]);
        self.last_examined = due.len();

        for armed in due {
            if armed.keep_alive == 0 {
                self.arm(armed, now);
                continue;
            }
            let silent_for = now.saturating_sub(armed.activity.last());
            if silent_for > Self::limit(armed.keep_alive) {
                self.count -= 1;
                expired.push(armed.id);
            } else {
                self.arm(armed, now);
            }
        }
    }

    /// How many connections are armed.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Whether none are.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// How many entries the most recent tick examined.
    ///
    /// Exposed so a test can assert the ring does bounded work per tick
    /// rather than assume it. That is the whole reason this is a ring
    /// rather than a list, and nothing else observes it: a mutation that
    /// degrades the sweep to the full population changes no outcome, only
    /// the cost.
    pub fn last_examined(&self) -> usize {
        self.last_examined
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscriptions::SubscriberId;

    fn ring() -> KeepAliveRing {
        KeepAliveRing::new(120)
    }

    fn expired_after(ring: &mut KeepAliveRing, from: u32, to: u32) -> Vec<SubscriberId> {
        let mut out = Vec::new();
        for now in (from + 1)..=to {
            ring.tick(now, &mut out);
        }
        out
    }

    #[test]
    fn a_silent_client_is_expired_after_one_and_a_half_intervals() {
        let mut ring = ring();
        let activity = Activity::new(0);
        ring.insert(SubscriberId(1), 10, activity, 0);

        // Nothing at fifteen seconds exactly; the limit is not yet past.
        assert!(expired_after(&mut ring, 0, 15).is_empty());
        assert_eq!(expired_after(&mut ring, 15, 16), vec![SubscriberId(1)]);
    }

    #[test]
    fn a_client_that_keeps_talking_is_never_expired() {
        let mut ring = ring();
        let activity = Activity::new(0);
        ring.insert(SubscriberId(1), 10, activity.clone(), 0);

        let mut out = Vec::new();
        for now in 1..=100 {
            activity.touch(now);
            ring.tick(now, &mut out);
        }
        assert!(out.is_empty());
    }

    #[test]
    fn a_client_that_falls_silent_partway_is_expired_from_its_last_word() {
        let mut ring = ring();
        let activity = Activity::new(0);
        ring.insert(SubscriberId(1), 10, activity.clone(), 0);

        let mut out = Vec::new();
        for now in 1..=50 {
            activity.touch(now);
            ring.tick(now, &mut out);
        }
        assert!(out.is_empty(), "still talking at fifty");

        // Silent from fifty. The limit is fifteen seconds later.
        assert!(expired_after(&mut ring, 50, 65).is_empty());
        assert_eq!(expired_after(&mut ring, 65, 66), vec![SubscriberId(1)]);
    }

    #[test]
    fn a_keep_alive_of_zero_means_no_deadline() {
        // The protocol uses zero to disable the mechanism entirely.
        let mut ring = ring();
        ring.insert(SubscriberId(1), 0, Activity::new(0), 0);
        assert!(expired_after(&mut ring, 0, 10_000).is_empty());
    }

    #[test]
    fn removing_a_connection_stops_it_being_swept() {
        let mut ring = ring();
        ring.insert(SubscriberId(1), 10, Activity::new(0), 0);
        ring.remove(SubscriberId(1));
        assert!(expired_after(&mut ring, 0, 100).is_empty());
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn connections_arriving_over_time_are_swept_across_buckets() {
        // Clients connect over time, and the ring places each one by when
        // it is next due, so the work spreads without any scheduling.
        //
        // "Over time" is ticked here, not merely labelled: arm() places a
        // connection relative to the ring's own cursor, which only tracks
        // the passage of time because tick() is called once per second. A
        // batch of inserts with no ticks between them leaves the cursor
        // stuck at second zero regardless of the `now` each one claims to
        // arrive at, which collapses every one of them onto the same
        // bucket instead of spreading them. Advancing the clock between
        // arrivals is what makes this a simulation of the real driver
        // (one ticker, once a second, independent of connection traffic)
        // rather than of a false instant where everyone arrives at once.
        let mut ring = ring();
        let mut activities = Vec::new();
        let mut out = Vec::new();
        let mut worst = 0;
        let mut now = 0u32;
        for arrived in 0u32..100 {
            if arrived > 0 {
                now += 1;
                ring.tick(now, &mut out);
                worst = worst.max(ring.last_examined());
            }
            for _ in 0..10u64 {
                let activity = Activity::new(arrived);
                ring.insert(
                    SubscriberId(activities.len() as u64),
                    60,
                    activity.clone(),
                    arrived,
                );
                activities.push(activity);
            }
        }

        // The earliest arrivals' sixty-second keep-alive can lapse before
        // the hundredth second of arrivals has even finished ticking by,
        // so some are already expired here, folded into `out` above
        // rather than dropped: that is real work the ring did while
        // still accepting new connections, not a separate phase.
        for _ in 0..200 {
            now += 1;
            ring.tick(now, &mut out);
            worst = worst.max(ring.last_examined());
        }
        assert_eq!(out.len(), 1000, "all of them eventually");
        assert!(ring.is_empty());
        // The bound is about the shape of the sweep, not a precise budget:
        // a thousand connections spread across a hundred arrival seconds
        // and a hundred and eighty-two buckets should never need a tick
        // to examine more than a small fraction of the population.
        assert!(
            worst <= 100,
            "no tick should examine the whole population, worst was {worst}"
        );
    }

    #[test]
    fn a_simultaneous_herd_is_checked_together_and_that_is_known() {
        // Connections armed in the same second with the same interval fall
        // due in the same second, so the ring does not spread them. This is
        // a real limit, not an accident: spreading them would mean firing
        // some early, which the protocol forbids, or late, which needs a
        // measurement nobody has taken. The test records the behaviour so a
        // later change to it is deliberate.
        let mut ring = ring();
        for n in 0..500u64 {
            ring.insert(SubscriberId(n), 10, Activity::new(0), 0);
        }

        let mut out = Vec::new();
        let mut worst = 0;
        for now in 1..=30 {
            ring.tick(now, &mut out);
            worst = worst.max(ring.last_examined());
        }
        assert_eq!(out.len(), 500);
        assert_eq!(worst, 500, "the herd is examined in one tick");
    }

    #[test]
    fn a_keep_alive_longer_than_the_ring_still_expires() {
        // The ring is sized for a maximum; a larger interval must be
        // handled by re-arming rather than by never firing.
        let mut ring = KeepAliveRing::new(10);
        ring.insert(SubscriberId(1), 600, Activity::new(0), 0);
        assert!(expired_after(&mut ring, 0, 900).is_empty());
        assert_eq!(expired_after(&mut ring, 900, 901), vec![SubscriberId(1)]);
    }

    #[test]
    fn touching_is_cheap_and_shared() {
        let activity = Activity::new(0);
        let copy = activity.clone();
        copy.touch(42);
        assert_eq!(activity.last(), 42);
    }
}
