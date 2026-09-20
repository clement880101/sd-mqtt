//! Disconnecting clients that have gone quiet, at scale.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use foldhash::fast::RandomState;

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
///
/// Departure is the other half of that promise, and it is the half that
/// was quietly missing. `remove` used to scan every bucket, which at
/// fifty thousand armed measured forty microseconds a call and grew
/// linearly; a herd of a hundred thousand timing out together would
/// have spent minutes inside one tick, which is precisely the stall the
/// ring exists to prevent. Every entry's address is now recorded, so a
/// removal touches one bucket slot and nothing else.
pub struct KeepAliveRing {
    buckets: Vec<Vec<Armed>>,
    /// Where each armed connection sits, as (bucket, index within it).
    ///
    /// Kept in step by every path that moves an entry. The invariant is
    /// simply that `buckets[b][i].id == id` for each `id -> (b, i)`
    /// here, and that the two hold the same population.
    placed: HashMap<SubscriberId, (usize, usize), RandomState>,
    cursor: usize,
    last_examined: usize,
    last_removal_examined: usize,
}

/// A client is disconnected after this multiple of its interval.
const LIMIT_NUMERATOR: u32 = 3;
const LIMIT_DENOMINATOR: u32 = 2;

/// The smallest interval the ring is ever sized for, in seconds.
///
/// `max_keep_alive` is a clamp ceiling, and zero there means "no
/// ceiling" rather than "no interval": a client may still negotiate any
/// interval it likes, and every one of them still has to be swept.
/// Sizing the ring from the ceiling alone gave an operator who wrote
/// zero a ring of two buckets, so every armed connection landed in one
/// of two and each tick examined half the population. No wrong answer
/// and no panic, just the linear sweep the ring exists to avoid.
///
/// A minute, because the ring costs one empty vector per bucket and
/// ninety-odd of them is nothing, while the interval is long enough
/// that the connections spread across them are spread for a useful
/// distance.
const MIN_RING_KEEP_ALIVE: u16 = 60;

impl KeepAliveRing {
    /// A ring sized for keep-alive intervals up to `max_keep_alive`
    /// seconds, and never for fewer than `MIN_RING_KEEP_ALIVE`. A
    /// larger interval still works, by re-arming.
    ///
    /// The floor is what keeps the ring a ring. `max_keep_alive` is the
    /// version 5 clamp ceiling and zero disables it, which says nothing
    /// about how many buckets the sweep should be spread over; reading
    /// it as a size collapsed the whole population onto two.
    pub fn new(max_keep_alive: u16) -> Self {
        let sized_for = max_keep_alive.max(MIN_RING_KEEP_ALIVE);
        let slots = Self::limit(sized_for) as usize + 2;
        Self {
            buckets: (0..slots).map(|_| Vec::new()).collect(),
            placed: HashMap::with_hasher(RandomState::default()),
            cursor: 0,
            last_examined: 0,
            last_removal_examined: 0,
        }
    }

    fn limit(keep_alive: u16) -> u32 {
        u32::from(keep_alive) * LIMIT_NUMERATOR / LIMIT_DENOMINATOR
    }

    /// Arm a connection.
    ///
    /// Arming one that is already armed replaces it rather than adding
    /// a second entry. Two entries for one connection would expire it
    /// twice, and the second would fire on a timestamp belonging to a
    /// connection that no longer exists.
    pub fn insert(&mut self, id: SubscriberId, keep_alive: u16, activity: Activity, now: u32) {
        self.remove(id);
        let armed = Armed {
            id,
            keep_alive,
            activity,
        };
        self.arm(armed, now);
    }

    /// Place a connection in the bucket where it next needs checking.
    fn arm(&mut self, armed: Armed, now: u32) {
        let slots = self.buckets.len();
        let slot = if armed.keep_alive == 0 {
            // No deadline. Park it a full revolution away so it is
            // revisited rarely and costs almost nothing.
            (self.cursor + slots - 1) % slots
        } else {
            let due = armed
                .activity
                .last()
                .saturating_add(Self::limit(armed.keep_alive));
            let ahead = due.saturating_sub(now).max(1) as usize;
            (self.cursor + ahead.min(slots - 1)) % slots
        };

        let id = armed.id;
        self.buckets[slot].push(armed);
        self.placed.insert(id, (slot, self.buckets[slot].len() - 1));
    }

    /// Disarm a connection.
    ///
    /// One hash lookup and one swap, whatever the population. The entry
    /// swapped into the hole learns its new index, which is the only
    /// other thing that moved.
    pub fn remove(&mut self, id: SubscriberId) {
        let Some((slot, index)) = self.placed.remove(&id) else {
            self.last_removal_examined = 0;
            return;
        };
        let bucket = &mut self.buckets[slot];
        bucket.swap_remove(index);
        // `swap_remove` moved the last entry into the hole, unless the
        // hole *was* the last entry.
        if let Some(moved) = bucket.get(index) {
            self.placed.insert(moved.id, (slot, index));
            self.last_removal_examined = 2;
        } else {
            self.last_removal_examined = 1;
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
                // Already out of its bucket, so only the index needs
                // forgetting. The caller must not ask the ring to
                // remove it again.
                self.placed.remove(&armed.id);
                expired.push(armed.id);
            } else {
                self.arm(armed, now);
            }
        }
    }

    /// How many buckets the sweep is spread over.
    ///
    /// Exposed so a test can assert the ring has them. Nothing else
    /// observes the count: a change that collapses it alters no
    /// outcome the protocol can see, only the cost of every tick, and
    /// that is exactly the mutation that survives a test measuring
    /// which connections expired.
    pub fn buckets(&self) -> usize {
        self.buckets.len()
    }

    /// How many connections are armed.
    pub fn len(&self) -> usize {
        self.placed.len()
    }

    /// Whether none are.
    pub fn is_empty(&self) -> bool {
        self.placed.is_empty()
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

    /// How many entries the most recent [`KeepAliveRing::remove`]
    /// touched.
    ///
    /// The other half of the same promise, and the half that was
    /// missing. [`KeepAliveRing::last_examined`] measured the sweep
    /// while a linear scan sat beside it on every departure, invisible
    /// to the one accessor that existed: a test measuring only the
    /// cheap half of an operation proves nothing about the expensive
    /// half. This is bounded by a small constant however many
    /// connections are armed, and a test says so.
    pub fn last_removal_examined(&self) -> usize {
        self.last_removal_examined
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
    fn a_maximum_of_zero_still_gives_the_ring_its_buckets() {
        // Convention 5, in its newest disguise. `max_keep_alive` is
        // two things at once: the version 5 clamp ceiling, where zero
        // is documented to an operator as "no ceiling", and the size
        // of this ring, where zero used to mean two buckets. Every
        // armed connection then landed in one of two and each tick
        // examined half the population — the right answer at the cost
        // the ring exists to avoid.
        //
        // Asserted on the bucket count and on the work one tick does,
        // never on which connections expire. That depends only on each
        // connection's own timestamp, not on where it sits in the
        // ring, so a ring of two would pass it.
        assert_eq!(
            KeepAliveRing::new(0).buckets(),
            KeepAliveRing::new(MIN_RING_KEEP_ALIVE).buckets(),
            "a ceiling of none is not a ring of none"
        );
        assert!(
            KeepAliveRing::new(0).buckets() >= 90,
            "a minute's interval at one and a half times it is ninety seconds of buckets, got {}",
            KeepAliveRing::new(0).buckets()
        );

        // And the cost that count is there to buy. Arrivals are ticked
        // apart rather than batched, because `arm` places a connection
        // relative to the cursor and a batch with no ticks between
        // collapses onto one bucket however many there are.
        let mut ring = KeepAliveRing::new(0);
        let mut out = Vec::new();
        let mut worst = 0;
        let mut now = 0u32;
        for arrived in 0u32..50 {
            if arrived > 0 {
                now += 1;
                ring.tick(now, &mut out);
                worst = worst.max(ring.last_examined());
            }
            for _ in 0..20u64 {
                ring.insert(
                    SubscriberId(ring.len() as u64),
                    60,
                    Activity::new(arrived),
                    arrived,
                );
            }
        }
        for _ in 0..150 {
            now += 1;
            ring.tick(now, &mut out);
            worst = worst.max(ring.last_examined());
        }
        assert_eq!(out.len(), 1000, "all of them eventually");
        assert!(
            worst <= 100,
            "a thousand armed over fifty seconds must not pile into one tick, worst was {worst}"
        );
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
    fn removing_costs_the_same_however_many_are_armed() {
        // The assertion the ring was missing. `last_examined` proved the
        // sweep was bounded while `remove` scanned every bucket beside
        // it: forty microseconds a call at fifty thousand armed, growing
        // linearly, on a path the broker walks at every departure.
        //
        // Measured in entries touched rather than in time, so it is a
        // statement about the algorithm and does not flake on a loaded
        // machine. A scan would touch the whole population and this
        // would read in the thousands.
        for population in [1u64, 100, 10_000] {
            let mut ring = KeepAliveRing::new(120);
            for n in 0..population {
                ring.insert(SubscriberId(n), 60, Activity::new(0), 0);
            }

            let mut worst = 0;
            for n in 0..population {
                ring.remove(SubscriberId(n));
                worst = worst.max(ring.last_removal_examined());
            }
            assert!(ring.is_empty(), "everything came out");
            assert!(
                worst <= 2,
                "removal touched {worst} entries with {population} armed"
            );
        }
    }

    #[test]
    fn removing_one_leaves_the_rest_findable() {
        // The risk a swap-remove introduces: the entry moved into the
        // hole must learn its new index, or the next removal takes the
        // wrong one out and the ring and its index disagree forever.
        let mut ring = ring();
        for n in 0..8u64 {
            ring.insert(SubscriberId(n), 60, Activity::new(0), 0);
        }
        // Front, middle and back of the same bucket, in an order that
        // makes the swap move an entry that is itself removed later.
        for n in [0u64, 3, 7, 1] {
            ring.remove(SubscriberId(n));
        }
        assert_eq!(ring.len(), 4);

        let mut out = Vec::new();
        for now in 1..=120 {
            ring.tick(now, &mut out);
        }
        out.sort_unstable_by_key(|id| id.0);
        assert_eq!(
            out,
            vec![
                SubscriberId(2),
                SubscriberId(4),
                SubscriberId(5),
                SubscriberId(6)
            ],
            "exactly the ones not removed, each exactly once"
        );
    }

    #[test]
    fn arming_an_armed_connection_replaces_it() {
        // A takeover arms a handle that is already armed. Two entries
        // for one connection means the older one expires it on a
        // timestamp belonging to a connection that has already gone.
        let mut ring = ring();
        let first = Activity::new(0);
        ring.insert(SubscriberId(1), 2, first, 0);
        ring.insert(SubscriberId(1), 60, Activity::new(0), 0);
        assert_eq!(ring.len(), 1, "one connection, one entry");

        let mut out = Vec::new();
        for now in 1..=10 {
            ring.tick(now, &mut out);
        }
        assert!(
            out.is_empty(),
            "the replaced entry's two-second deadline is gone, not lurking"
        );
    }

    #[test]
    fn touching_is_cheap_and_shared() {
        let activity = Activity::new(0);
        let copy = activity.clone();
        copy.touch(42);
        assert_eq!(activity.last(), 42);
    }
}
