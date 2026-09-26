//! What each rule has been doing, as a series rather than a number.
//!
//! Five numbers per rule: how often it fired, how much it delivered,
//! how many attempts failed, how many envelopes ran out of attempts,
//! and how much is waiting in its outbox. Per rule, because one total
//! across every rule tells an operator nothing about which one is
//! broken.
//!
//! # A series, not a number
//!
//! A counter answers "how many since the process started", which is
//! the question nobody asks: what an operator wants to see is whether
//! a rule stopped an hour ago, or is failing now, or has a backlog
//! that is growing. That is a shape, and a shape needs history.
//!
//! So each rule keeps a ring of [`HISTORY`] samples, one per
//! [`SAMPLE_INTERVAL`], and a sample is the **difference** since the
//! one before it. The four counters are differences; the fifth,
//! `waiting`, is a depth and is the value itself, because the
//! difference of a queue depth is not a thing anybody wants to look
//! at.
//!
//! **The ring advances whether or not anything happened.** A quiet
//! rule draws a flat line at zero and a busy rule whose connector died
//! draws a line that falls to zero and stays there; a ring that only
//! moved when something happened would draw those two the same way,
//! which is exactly the distinction the chart exists to make.
//!
//! # How much history, where it is, and what it costs
//!
//! One minute, at one second. In memory, beside the installed rule,
//! and **lost when the process stops** — which is correct for what it
//! is: this is a live view of what a rule is doing now, and a rule's
//! durable history of what it delivered is the endpoint's own logs and
//! its dead-letter topic.
//!
//! A [`Sample`] is five `u32`s, so twenty bytes; [`HISTORY`] of them
//! is 1,200 bytes in a [`VecDeque`] that never reallocates, plus the
//! five counters and a lock. Call it 1.3 KB per rule:
//!
//! | rules | history |
//! |---:|---:|
//! | 10 | 13 KB |
//! | 200 | 260 KB |
//! | 1,000 | 1.3 MB |
//!
//! Two hundred is the table size the interface work sizes itself
//! against, so a quarter of a megabyte is the number that matters, and
//! it is not one worth a smaller ring for. A thousand rules on one
//! node is more rules than section 9 imagines and still costs less
//! than one large retained message.
//!
//! `u32` rather than `u64` for a sample: a rule would have to fire
//! four billion times in one second to overflow one, and the running
//! totals beside the ring are `u64` and are what an operator reads for
//! a lifetime count.
//!
//! # The same numbers reach Prometheus
//!
//! A series a browser can draw is half of what an operator needs; the
//! other half is an alert, and an alert is a scrape. So each rule also
//! holds the owned metric handles `sdmqtt_server::metrics` resolves
//! for it, and the four counters move both at once. One increment,
//! two sinks — which is not the "two counters fed from one event" the
//! tests below exist to catch, because it is the same fact exported
//! twice rather than two facts sharing an increment.
//!
//! The handles are optional, so a [`RuleStats`] can be built with no
//! broker behind it: that is what the tests below do, and what a
//! connector built by a test of its own gets.
//!
//! # Nothing is walked to answer a request
//!
//! The sampler is a task that ticks once a second and reads each
//! rule's atomics. Answering a request copies the ring out under its
//! own lock — never the broker's, which this module never takes — and
//! serialises outside it.
//!
//! The one number that could have been a walk is `waiting`, and it is
//! not: [`crate::Outbox`] maintains its own depth, seeded from the
//! store when it is opened and moved by the same two methods that put
//! an envelope in and take one out. Reading every rule's outbox from
//! the store once a second would be the mistake the read resources
//! were fixed for, with a new name on it.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use sdmqtt_server::metrics::{RuleCounters, RuleEvent};
use serde::Serialize;

/// How many samples a rule keeps.
///
/// Sixty, at one a second, which is one minute. See the module note
/// for what that costs; the short answer is a quarter of a megabyte at
/// two hundred rules.
pub const HISTORY: usize = 60;

/// How long one sample covers.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// One interval of a rule's life.
///
/// The four counters are differences over the interval. `waiting` is a
/// depth at the moment the sample was taken, because the difference of
/// a queue depth is not a thing anybody wants to look at.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Sample {
    /// How many times the rule fired.
    pub fired: u32,
    /// How much reached where it was going.
    pub delivered: u32,
    /// How many attempts failed.
    pub failed: u32,
    /// How many envelopes ran out of attempts.
    pub dead_lettered: u32,
    /// How much was waiting in the outbox.
    pub waiting: u32,
}

/// What a rule has done since this process started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Totals {
    /// How many times it has fired.
    pub fired: u64,
    /// How much has reached where it was going.
    pub delivered: u64,
    /// How many attempts have failed.
    pub failed: u64,
    /// How many envelopes have run out of attempts.
    pub dead_lettered: u64,
    /// How much is waiting in the outbox now.
    pub waiting: u64,
}

/// The ring, and the totals the last sample was taken at.
#[derive(Debug, Default)]
struct History {
    /// What the counters read when the last sample was taken, so that
    /// a sample is a difference and not a running total.
    at_last_sample: Totals,
    /// The samples, oldest first.
    samples: VecDeque<Sample>,
}

/// What one rule has been doing.
///
/// Counters are atomics rather than a lock, because two of them are
/// incremented from a hook that runs under the broker's one lock and a
/// hook may not block. The ring is behind a lock, and it is touched
/// once a second by the sampler and once per request by a reader.
#[derive(Debug)]
pub struct RuleStats {
    fired: AtomicU64,
    delivered: AtomicU64,
    failed: AtomicU64,
    dead_lettered: AtomicU64,
    history: Mutex<History>,
    /// The same four numbers, and the depth, as Prometheus sees them.
    ///
    /// `None` for a [`RuleStats`] built with no broker behind it.
    exported: Option<RuleCounters>,
}

impl Default for RuleStats {
    fn default() -> Self {
        Self::new()
    }
}

impl RuleStats {
    /// A rule that has done nothing yet, reporting to nothing but
    /// itself.
    pub fn new() -> Self {
        Self::exporting(None)
    }

    /// The same, also moving `exported` whenever anything moves.
    pub fn exporting(exported: Option<RuleCounters>) -> Self {
        Self {
            exported,
            fired: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            dead_lettered: AtomicU64::new(0),
            history: Mutex::new(History {
                at_last_sample: Totals::default(),
                samples: VecDeque::with_capacity(HISTORY),
            }),
        }
    }

    /// The rule fired: a message matched it, a request arrived for it,
    /// or its interval came round.
    ///
    /// **Not the same as delivering anything.** A rule that fires a
    /// thousand times and delivers nothing is the shape an operator
    /// most needs to see, and one counter fed from both events could
    /// not show it.
    pub fn fired(&self) {
        self.fired.fetch_add(1, Ordering::Relaxed);
        self.export(RuleEvent::Fired);
    }

    /// Something reached where the rule was sending it.
    pub fn delivered(&self) {
        self.delivered.fetch_add(1, Ordering::Relaxed);
        self.export(RuleEvent::Delivered);
    }

    /// An attempt failed.
    ///
    /// One attempt, not one envelope: an envelope that is tried four
    /// times before it succeeds counts three failures and one
    /// delivery, which is what says the endpoint is unreliable rather
    /// than that a message was lost.
    pub fn failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        self.export(RuleEvent::Failed);
    }

    /// An envelope ran out of attempts and went to the dead-letter
    /// topic.
    ///
    /// **A different fact from the attempt that exhausted it**, which
    /// is counted as a failure of its own where it happened. `failed`
    /// counts attempts and this counts envelopes, so a rule with four
    /// retries against a dead endpoint reports five failures and one
    /// dead letter — and an operator reading either gets the number
    /// its name says rather than a mixture.
    pub fn dead_lettered(&self) {
        self.dead_lettered.fetch_add(1, Ordering::Relaxed);
        self.export(RuleEvent::DeadLettered);
    }

    /// What this rule has done since the process started.
    ///
    /// `waiting` is the caller's, because the depth belongs to the
    /// outbox and a rule without one has none.
    pub fn totals(&self, waiting: u64) -> Totals {
        Totals {
            fired: self.fired.load(Ordering::Relaxed),
            delivered: self.delivered.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            dead_lettered: self.dead_lettered.load(Ordering::Relaxed),
            waiting,
        }
    }

    /// Close one interval and start the next.
    ///
    /// Called once per [`SAMPLE_INTERVAL`] whether or not anything
    /// happened, because a ring that stood still while a rule was
    /// quiet would draw a stopped rule and an idle one the same way.
    pub fn sample(&self, waiting: u64) {
        if let Some(exported) = &self.exported {
            // Set here rather than where the outbox moves, because it
            // is a depth: a scrape wants the number as it is, and the
            // once-a-second tick this is on is finer than any scrape
            // interval an operator configures.
            exported
                .waiting()
                .set(i64::try_from(waiting).unwrap_or(i64::MAX));
        }
        let now = self.totals(waiting);
        let mut history = self.history.lock();
        let before = history.at_last_sample;
        let sample = Sample {
            fired: difference(now.fired, before.fired),
            delivered: difference(now.delivered, before.delivered),
            failed: difference(now.failed, before.failed),
            dead_lettered: difference(now.dead_lettered, before.dead_lettered),
            waiting: u32::try_from(waiting).unwrap_or(u32::MAX),
        };
        history.at_last_sample = now;
        if history.samples.len() == HISTORY {
            history.samples.pop_front();
        }
        history.samples.push_back(sample);
    }

    /// The samples this rule has, oldest first.
    ///
    /// Copied out under the ring's lock and read outside it. Shorter
    /// than [`HISTORY`] for a rule that has not been installed for a
    /// minute yet, which a drawing has to cope with anyway: a rule
    /// created a second ago has one point and no line.
    pub fn series(&self) -> Vec<Sample> {
        self.history.lock().samples.iter().copied().collect()
    }
}

impl RuleStats {
    /// Move the exported counter beside the one just moved.
    fn export(&self, event: RuleEvent) {
        if let Some(exported) = &self.exported {
            exported.event(event).inc();
        }
    }
}

/// How much a counter moved, as a number a sample can hold.
///
/// Saturating rather than wrapping in both directions. A counter
/// cannot go backwards, so the subtraction is only defensive; and a
/// rule that fired more than four billion times in one second has a
/// problem this number is not going to help with.
fn difference(now: u64, before: u64) -> u32 {
    u32::try_from(now.saturating_sub(before)).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sample_is_the_difference_and_not_the_total() {
        let stats = RuleStats::new();
        stats.fired();
        stats.fired();
        stats.sample(0);
        stats.fired();
        stats.sample(0);

        let series = stats.series();
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].fired, 2);
        assert_eq!(
            series[1].fired, 1,
            "the second sample repeated the first one's total"
        );
        assert_eq!(stats.totals(0).fired, 3);
    }

    /// The mutation the plan names: a series that only advances when
    /// something happened makes a quiet rule look like a busy one
    /// whose chart stopped.
    #[test]
    fn the_series_advances_when_nothing_happens() {
        let stats = RuleStats::new();
        for _ in 0..5 {
            stats.sample(0);
        }
        assert_eq!(stats.series().len(), 5);
        assert!(stats
            .series()
            .iter()
            .all(|sample| *sample == Sample::default()));
    }

    /// Each counter moves when its own thing happens and when nothing
    /// else does. A counter that rose on everything is worse than no
    /// counter.
    #[test]
    fn each_counter_moves_only_for_its_own_event() {
        /// One counter's name and the call that moves it.
        type Event = (&'static str, fn(&RuleStats));
        let events: [Event; 4] = [
            ("fired", RuleStats::fired),
            ("delivered", RuleStats::delivered),
            ("failed", RuleStats::failed),
            ("dead_lettered", RuleStats::dead_lettered),
        ];
        for (name, event) in events {
            let stats = RuleStats::new();
            event(&stats);
            let totals = stats.totals(0);
            let moved = [
                ("fired", totals.fired),
                ("delivered", totals.delivered),
                ("failed", totals.failed),
                ("dead_lettered", totals.dead_lettered),
            ];
            for (counter, value) in moved {
                let expected = u64::from(counter == name);
                assert_eq!(
                    value, expected,
                    "{name} moved {counter} to {value}, and should have moved it to {expected}"
                );
            }
        }
    }

    #[test]
    fn the_ring_keeps_the_last_minute_and_no_more() {
        let stats = RuleStats::new();
        for turn in 0..HISTORY + 10 {
            for _ in 0..turn {
                stats.fired();
            }
            stats.sample(0);
        }
        let series = stats.series();
        assert_eq!(series.len(), HISTORY);
        // The oldest kept sample is the one for turn 10, which fired
        // ten times: everything before it has fallen out.
        assert_eq!(series[0].fired, 10);
        assert_eq!(
            series[HISTORY - 1].fired,
            u32::try_from(HISTORY + 9).unwrap()
        );
    }

    #[test]
    fn waiting_is_the_depth_and_not_a_difference() {
        let stats = RuleStats::new();
        stats.sample(4);
        stats.sample(4);
        let series = stats.series();
        assert_eq!(series[0].waiting, 4);
        assert_eq!(
            series[1].waiting, 4,
            "a depth that did not change was reported as zero"
        );
    }
}
