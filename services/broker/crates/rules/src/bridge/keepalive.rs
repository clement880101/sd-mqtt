//! Pinging the remote, and noticing when it stops answering.
//!
//! Item 4, and the research notes are precise about why it is on the
//! list: "The broker's own ring answers the opposite question and does
//! not do this." `broker_core::keepalive` is a structure for finding,
//! cheaply and among a hundred thousand connections, the ones that have
//! gone quiet. A bridge has one connection and two questions about it,
//! and neither is the one that ring answers:
//!
//! **When to send a ping.** [MQTT-3.1.2-20]: a client must send a
//! PINGREQ if it has sent nothing else within the keep-alive. The
//! clock runs from the last packet **sent**, not from the last one
//! received, because it is this end's silence the remote is measuring.
//!
//! **When to give up.** The specification says what a *server* does
//! about a silent client — one and a half keep-alives, [MQTT-3.1.2-22]
//! — and says nothing normative about how long a client waits for its
//! PINGRESP. So this is a decision rather than a quotation, and it is
//! made here rather than scattered: **one keep-alive interval**, from
//! the moment the ping went out.
//!
//! The reasoning is that the deadline exists to distinguish a link
//! which is slow from one which is gone, and a keep-alive is already
//! the operator's statement of how long silence may last on a working
//! link. A longer deadline leaves a bridge attached to a black hole for
//! as long as it takes; a shorter one declares a busy remote dead. It
//! is also bounded below, because a keep-alive of one second would
//! otherwise make a one-second round trip a dead link.
//!
//! # A deadline that is never checked is the failure mode here
//!
//! Convention 5 records a keep-alive guarded by a test asserting that
//! no timeout occurred, with every property fixing the interval at
//! zero — five mutations survived, one of which deleted the mechanism.
//! So the tests below are written the other way round: every one of
//! them makes the deadline pass and asserts what happens, and the one
//! that turns the mechanism off says so in its name.

use std::time::Duration;

use tokio::time::Instant;

/// What the keep-alive wants done next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Due {
    /// Nothing, until this moment.
    Idle(Instant),
    /// Send a ping.
    Ping,
    /// The remote has not answered inside the deadline. The link is
    /// gone whatever the socket believes.
    Dead,
}

/// The shortest deadline a ping is given, whatever the keep-alive is.
///
/// A keep-alive of one second would otherwise make any round trip over
/// a second a dead link, which on a congested or distant link is most
/// of them.
const FLOOR: Duration = Duration::from_secs(5);

/// One connection's keep-alive.
pub struct KeepAlive {
    /// The negotiated interval. `None` disables the mechanism
    /// entirely, which is what a keep-alive of zero means on either
    /// side.
    interval: Option<Duration>,
    /// When something was last sent to the remote.
    last_sent: Instant,
    /// When a ping went out and is still unanswered.
    awaiting: Option<Instant>,
}

impl KeepAlive {
    /// A keep-alive on the interval the CONNACK settled.
    ///
    /// The interval is the negotiated one, not the configured one:
    /// [MQTT-3.1.2-21] lets the remote replace what was proposed, and
    /// [`super::connack`] has already applied that.
    pub fn new(interval: Option<Duration>, now: Instant) -> Self {
        Self {
            interval,
            last_sent: now,
            awaiting: None,
        }
    }

    /// Whether the mechanism is on at all.
    pub fn enabled(&self) -> bool {
        self.interval.is_some()
    }

    /// Something went out to the remote.
    ///
    /// **Any packet, not only a ping.** [MQTT-3.1.2-20] is about the
    /// client having sent nothing, so a bridge carrying traffic never
    /// needs to ping, and one that pinged anyway would be spending a
    /// round trip per interval on a busy link for no reason.
    pub fn sent(&mut self, now: Instant) {
        self.last_sent = now;
    }

    /// The remote answered a ping.
    pub fn pong(&mut self) {
        self.awaiting = None;
    }

    /// Forget a ping in flight, because the connection ended.
    pub fn reset(&mut self, now: Instant) {
        self.last_sent = now;
        self.awaiting = None;
    }

    /// Whether a ping is outstanding.
    pub fn waiting(&self) -> bool {
        self.awaiting.is_some()
    }

    /// The deadline a ping is given.
    pub fn deadline(&self) -> Option<Duration> {
        self.interval.map(|interval| interval.max(FLOOR))
    }

    /// What to do now.
    ///
    /// A pure function of the two timestamps and the interval, so the
    /// decision can be asserted without waiting for a clock — which is
    /// the only way to write a test that makes a deadline *pass*
    /// rather than one that asserts a deadline did not.
    pub fn due(&mut self, now: Instant) -> Due {
        let Some(interval) = self.interval else {
            // No keep-alive means no ping and no deadline, so the next
            // thing to wake for is whatever else the session has. A
            // long idle rather than a busy loop.
            return Due::Idle(now + Duration::from_secs(3_600));
        };

        if let Some(sent_at) = self.awaiting {
            let deadline = sent_at + interval.max(FLOOR);
            if now >= deadline {
                return Due::Dead;
            }
            return Due::Idle(deadline);
        }

        let next = self.last_sent + interval;
        if now >= next {
            self.awaiting = Some(now);
            self.last_sent = now;
            return Due::Ping;
        }
        Due::Idle(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(base: Instant, secs: u64) -> Instant {
        base + Duration::from_secs(secs)
    }

    #[tokio::test]
    async fn a_ping_goes_out_when_nothing_else_has() {
        let base = Instant::now();
        let mut keep = KeepAlive::new(Some(Duration::from_secs(30)), base);

        assert_eq!(keep.due(at(base, 29)), Due::Idle(at(base, 30)));
        assert_eq!(keep.due(at(base, 30)), Due::Ping);
        assert!(keep.waiting());
    }

    /// [MQTT-3.1.2-20] is about the client having sent nothing, so a
    /// bridge carrying traffic never needs to ping. One that pinged
    /// anyway would spend a round trip per interval on a busy link for
    /// no reason at all.
    #[tokio::test]
    async fn traffic_postpones_the_ping() {
        let base = Instant::now();
        let mut keep = KeepAlive::new(Some(Duration::from_secs(30)), base);

        keep.sent(at(base, 20));
        assert_eq!(keep.due(at(base, 30)), Due::Idle(at(base, 50)));
        keep.sent(at(base, 45));
        assert_eq!(keep.due(at(base, 50)), Due::Idle(at(base, 75)));
        assert!(!keep.waiting());
    }

    /// The half of the mechanism convention 5 says gets left untested:
    /// the deadline is made to **pass**, and the answer is that the
    /// link is dead.
    #[tokio::test]
    async fn a_ping_that_is_not_answered_inside_the_deadline_is_a_dead_link() {
        let base = Instant::now();
        let mut keep = KeepAlive::new(Some(Duration::from_secs(30)), base);

        assert_eq!(keep.due(at(base, 30)), Due::Ping);
        assert_eq!(
            keep.due(at(base, 59)),
            Due::Idle(at(base, 60)),
            "still inside the deadline"
        );
        assert_eq!(keep.due(at(base, 60)), Due::Dead);
    }

    #[tokio::test]
    async fn an_answered_ping_clears_the_deadline() {
        let base = Instant::now();
        let mut keep = KeepAlive::new(Some(Duration::from_secs(30)), base);

        assert_eq!(keep.due(at(base, 30)), Due::Ping);
        keep.pong();
        assert!(!keep.waiting());
        assert_eq!(
            keep.due(at(base, 59)),
            Due::Idle(at(base, 60)),
            "back to waiting for the next interval, not for a deadline"
        );
        assert_eq!(keep.due(at(base, 3_600)), Due::Ping, "and it pings again");
    }

    /// A short keep-alive would otherwise make any slow round trip a
    /// dead link.
    #[tokio::test]
    async fn a_very_short_keep_alive_still_gives_the_remote_a_floor() {
        let base = Instant::now();
        let mut keep = KeepAlive::new(Some(Duration::from_secs(1)), base);
        assert_eq!(keep.deadline(), Some(FLOOR));

        assert_eq!(keep.due(at(base, 1)), Due::Ping);
        assert_eq!(keep.due(at(base, 4)), Due::Idle(at(base, 6)));
        assert_eq!(keep.due(at(base, 6)), Due::Dead);
    }

    /// The configuration that disables the mechanism, named so that a
    /// reader cannot mistake a test with the interval at zero for a
    /// test of the mechanism. Convention 5: every property in the
    /// keep-alive tests that this project got wrong fixed the interval
    /// at zero, which turns the feature off.
    #[tokio::test]
    async fn a_keep_alive_of_zero_turns_the_mechanism_off() {
        let base = Instant::now();
        let mut keep = KeepAlive::new(None, base);
        assert!(!keep.enabled());
        assert_eq!(keep.deadline(), None);

        // However long passes, there is no ping and no death.
        for hours in [1u64, 24, 24 * 365] {
            assert!(matches!(keep.due(at(base, hours * 3_600)), Due::Idle(_)));
        }
        assert!(!keep.waiting());
    }

    #[tokio::test]
    async fn a_new_connection_has_no_ping_in_flight() {
        let base = Instant::now();
        let mut keep = KeepAlive::new(Some(Duration::from_secs(30)), base);
        assert_eq!(keep.due(at(base, 30)), Due::Ping);

        let later = at(base, 100);
        keep.reset(later);
        assert!(!keep.waiting());
        assert_eq!(keep.due(later), Due::Idle(at(later, 30)));
    }
}
