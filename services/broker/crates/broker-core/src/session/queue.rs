//! The outbound queue: what waits when the inflight window is full.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use mqtt_codec::QoS;

use crate::session::message::Message;

/// A message addressed to one subscriber, with everything delivery needs.
///
/// The quality of service here is the *delivery* level, the lesser of what
/// the publisher asked for and what the subscription granted, which is why
/// it is separate from the message's own.
#[derive(Debug, Clone)]
pub struct Delivery {
    /// The message itself, shared with every other subscriber's copy.
    pub message: Message,
    /// The level to deliver at.
    pub qos: QoS,
    /// The retain flag as it should appear on the wire, which depends on
    /// the subscription's retain-as-published option.
    pub retain: bool,
    /// MQTT 5 subscription identifiers to echo back, one per matching
    /// subscription that carried one.
    pub subscription_ids: Vec<u32>,
    /// Whether this is a redelivery, set when a message is requeued on
    /// resume. It becomes the duplicate flag on the wire.
    pub dup: bool,
    /// The identifier this was already sent with, when it is being
    /// redelivered. Section 4.4 requires a resumed publish to keep its
    /// original identifier: a peer that received the first copy is
    /// waiting on that identifier, and a new one makes the message look
    /// unrelated.
    pub packet_id: Option<u16>,
    /// The per-session sequence number this was enqueued under.
    ///
    /// Section 7.4: assigned on enqueue, monotonic within the session,
    /// never reused, and the key the stored queue is read back in. It is
    /// **[`Outbound::offer`]'s to assign**, not a caller's: whatever is
    /// here when a delivery is offered is overwritten, because the
    /// counter that must never go backwards lives there and nowhere
    /// else. A caller putting a message back — [`Queue::requeue`], or
    /// restore — keeps the number the delivery already carries.
    ///
    /// [`Outbound::offer`]: crate::session::Outbound::offer
    pub sequence: u64,
}

impl Delivery {
    /// Roughly how much of the queue's byte budget this occupies.
    pub fn size_in_queue(&self) -> usize {
        self.message.size_in_queue()
    }
}

/// Why a message was discarded.
///
/// Every discard carries one of these, because a message that disappears
/// without being counted is a support case nobody can answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DropReason {
    /// The queue was at its message limit.
    QueueFull,
    /// The queue was at its byte limit.
    ByteBudget,
    /// Fire-and-forget delivery to a session with nobody attached.
    QosZeroWhileOffline,
    /// The message's expiry deadline had passed.
    Expired,
    /// The session was destroyed.
    SessionEnded,
    /// The session moved to another node, section 8.4.
    ///
    /// **Not a loss, and it is here anyway.** A message reported with
    /// this reason left this node's queue in the same breath as it was
    /// handed to the node that now owns the session, so the broker that
    /// reports it is reporting a move. It is a [`DropReason`] because
    /// the alternative is a message leaving a queue with no record of
    /// why, which is the one thing this enum exists to prevent: the
    /// conservation harness would see a message vanish, and an operator
    /// looking at a node that shed a thousand queued messages in a
    /// second could not tell a migration from a bug. What the count
    /// cannot say is whether the far node received them; that is
    /// `cluster::migrate`'s shortfall, counted separately and on the
    /// node that would know.
    Migrated,
    /// The peer answered with a failure reason, so it will never take
    /// the message. The exchange is over by [MQTT-4.4.0-2] and the
    /// message must not be retransmitted.
    RejectedByPeer,
    /// The encoded packet was larger than the Maximum Packet Size the
    /// peer advertised. [MQTT-3.1.2-24] forbids sending it and
    /// [MQTT-3.1.2-25] says to behave as though it had been delivered,
    /// so this is the only record that it was not.
    TooLargeForPeer,
    /// The authorizing chain, or section 12.3's reserved namespace,
    /// refused the publish and there was no acknowledgement to say so
    /// on: an at-most-once publish, which is otherwise the one refusal
    /// nothing would count.
    Refused,
}

impl Drop for Queue {
    /// Take whatever is still waiting off the shared total.
    ///
    /// Not every path out of a session clears its queue first: the
    /// registry hands a removed session back and the caller drops it,
    /// and a clean start replaces one outright. Without this, every
    /// such departure would leave its messages on the total forever
    /// and the gauge would only ever rise.
    fn drop(&mut self) {
        self.total.take(self.entries.len());
    }
}

/// What to discard when the queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Overflow {
    /// Refuse the arrival. The default, because it preserves the older
    /// messages a subscriber has been waiting longest for.
    #[default]
    DropNewest,
    /// Evict the head to make room, which suits a consumer that only
    /// cares about recent state.
    DropOldest,
}

/// Bounds on one queue.
#[derive(Debug, Clone, Copy)]
pub struct QueueLimits {
    /// How many messages may wait.
    pub max_messages: usize,
    /// How many bytes they may occupy in total.
    ///
    /// Separate from the count because either bound alone is escapable: a
    /// count permits a thousand enormous payloads, a byte total permits a
    /// million empty ones.
    pub max_bytes: usize,
    /// What to discard when either bound is reached.
    pub overflow: Overflow,
    /// Whether to hold fire-and-forget messages for a session with nobody
    /// attached. Off by default, matching the protocol's intent that such
    /// a message is worth no effort.
    pub queue_qos0_while_offline: bool,
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self {
            max_messages: 1000,
            max_bytes: 64 * 1024 * 1024,
            overflow: Overflow::DropNewest,
            queue_qos0_while_offline: false,
        }
    }
}

/// How many messages are waiting in every queue that shares this.
///
/// Maintained beside each queue's own length, and for one reason:
/// summing the lengths means walking every session under the
/// broker's one lock, measured at 405 microseconds with a hundred
/// thousand sessions against 29 with ten thousand. A metrics endpoint
/// a scraper hits every fifteen seconds must not stall every packet
/// in the process for that, and this project has already called a
/// forty microsecond sweep too long to hold.
///
/// Kept exactly where [`Queue::bytes`] is kept, which is the same
/// discipline applied to a number one layer up, and a queue dropped
/// with messages still in it takes them off this too — see the
/// [`Drop`] below. That the two agree is asserted by walking, in
/// `the_maintained_queue_total_agrees_with_a_walk`.
///
/// [`Queue::bytes`]: Queue::bytes
#[derive(Clone, Debug, Default)]
pub struct QueueTotal(Arc<AtomicUsize>);

impl QueueTotal {
    /// How many messages are waiting, across every queue sharing this.
    pub fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }

    fn add(&self, messages: usize) {
        self.0.fetch_add(messages, Ordering::Relaxed);
    }

    fn take(&self, messages: usize) {
        self.0.fetch_sub(messages, Ordering::Relaxed);
    }
}

/// Messages waiting for room in the inflight window.
pub struct Queue {
    entries: VecDeque<Delivery>,
    bytes: usize,
    limits: QueueLimits,
    total: QueueTotal,
}

impl Queue {
    /// An empty queue with the given bounds, counted only by itself.
    ///
    /// For a queue that is not part of a broker: a unit test, or a
    /// property. A queue the registry makes is given the process's
    /// total instead, through [`Queue::sharing`].
    pub fn new(limits: QueueLimits) -> Self {
        Self::sharing(limits, QueueTotal::default())
    }

    /// The same, counted into `total` as well as by itself.
    pub fn sharing(limits: QueueLimits, total: QueueTotal) -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            limits,
            total,
        }
    }

    /// Offer a message to the queue.
    ///
    /// Returns whether it was accepted. Anything discarded, including the
    /// arrival itself when it is refused, is appended to `dropped` with
    /// the reason, so the caller can count it.
    pub fn push(
        &mut self,
        item: Delivery,
        now: u32,
        online: bool,
        dropped: &mut Vec<(Delivery, DropReason)>,
    ) -> bool {
        let size = item.size_in_queue();
        if let Some(reason) = self.refusal(&item.message, item.qos, now, online) {
            // Only a bound can be made room for, and only by a queue
            // that evicts to make it. An expired message, a
            // fire-and-forget delivery to an offline session and a
            // message larger than the whole budget are refusals however
            // much room is cleared.
            let at_a_bound = matches!(reason, DropReason::QueueFull | DropReason::ByteBudget)
                && size <= self.limits.max_bytes;
            if !at_a_bound || self.limits.overflow != Overflow::DropOldest {
                dropped.push((item, reason));
                return false;
            }

            while self.is_over(size) {
                let reason = self.bound_reached();
                match self.entries.pop_front() {
                    Some(evicted) => {
                        self.bytes -= evicted.size_in_queue();
                        self.total.take(1);
                        dropped.push((evicted, reason));
                    }
                    // Nothing left to evict, which the size check in
                    // `refusal` should already have prevented.
                    None => {
                        dropped.push((item, reason));
                        return false;
                    }
                }
            }
        }

        self.bytes += size;
        self.total.add(1);
        self.entries.push_back(item);
        true
    }

    /// Why this message would not be taken, or `None` if it would.
    ///
    /// Every reason a message is refused lives here and nowhere else.
    /// [`Queue::push`] asks and then acts on the answer;
    /// [`Queue::would_refuse`] asks and only reports it. Neither
    /// restates a condition, so a rule added to one is a rule added to
    /// both.
    ///
    /// The three, in the order they are asked: the message has expired,
    /// it is fire-and-forget for a session with nobody attached, or it
    /// is over a bound. The first two are the ones a caller asking in
    /// advance most needs, because they do not depend on how full the
    /// queue is and so cannot be guessed from its occupancy.
    fn refusal(&self, message: &Message, qos: QoS, now: u32, online: bool) -> Option<DropReason> {
        if message.is_expired(now) {
            return Some(DropReason::Expired);
        }
        if qos == QoS::AtMostOnce && !online && !self.limits.queue_qos0_while_offline {
            return Some(DropReason::QosZeroWhileOffline);
        }
        let size = message.size_in_queue();
        // A message larger than the whole budget can never fit, whatever
        // is evicted, so refuse it rather than emptying the queue trying.
        if size > self.limits.max_bytes {
            return Some(DropReason::ByteBudget);
        }
        if self.is_over(size) {
            return Some(self.bound_reached());
        }
        None
    }

    /// Which bound the queue has reached, for the report.
    fn bound_reached(&self) -> DropReason {
        if self.entries.len() >= self.limits.max_messages {
            DropReason::QueueFull
        } else {
            DropReason::ByteBudget
        }
    }

    /// Whether a message of `size` bytes is over either bound.
    ///
    /// Not a refusal on its own: a queue that evicts to make room is over
    /// its bound until it has evicted enough.
    fn is_over(&self, size: usize) -> bool {
        self.entries.len() >= self.limits.max_messages || self.bytes + size > self.limits.max_bytes
    }

    /// Whether offering this message would be refused.
    ///
    /// The question section 6 asks of a shared subscription's members
    /// before it hands one the message: "a member whose queue is full is
    /// skipped and the next is tried". It costs nothing to be refused
    /// here and the message is already lost by the time an offer is,
    /// which is why the question is asked at all.
    ///
    /// It takes the whole message, and the level it would be delivered
    /// at, because being full is only one of the three reasons
    /// [`Queue::push`] refuses one. Asking about a size alone answered
    /// a third of the question and read as the whole of it, which lost
    /// a fire-and-forget publish to an offline member of a shared group
    /// while an idle member with an empty queue received nothing.
    ///
    /// Both this and `push` get their answer from the same
    /// private `refusal`, so they cannot disagree about *whether* a
    /// message is refused. They differ in one deliberate way, which is
    /// what they do with the answer: an [`Overflow::DropOldest`] queue
    /// at its bound accepts the message by evicting an older one, and
    /// is still reported refused here. A member that need evict nothing
    /// is the better place to send it, and the group has one or it has
    /// not.
    pub fn would_refuse(&self, message: &Message, qos: QoS, now: u32, online: bool) -> bool {
        self.refusal(message, qos, now, online).is_some()
    }

    /// Put a message back at the head, ahead of anything newer.
    ///
    /// Used on resume, where a message that was already sent but not
    /// acknowledged must go out before anything that arrived since. It
    /// bypasses the bounds deliberately: the message was already accepted,
    /// and refusing it now would lose something the peer may be expecting.
    ///
    /// The duplicate flag is the caller's to set, not this method's. The
    /// other caller requeues a message because no identifier was free,
    /// and that message was never sent, so claiming it is a redelivery
    /// would be a lie to the subscriber.
    pub fn requeue(&mut self, item: Delivery) {
        self.bytes += item.size_in_queue();
        self.total.add(1);
        self.entries.push_front(item);
    }

    /// Take the next message, discarding any that expired while waiting.
    pub fn pop(&mut self, now: u32, dropped: &mut Vec<(Delivery, DropReason)>) -> Option<Delivery> {
        while let Some(item) = self.entries.pop_front() {
            self.bytes -= item.size_in_queue();
            self.total.take(1);
            if item.message.is_expired(now) {
                dropped.push((item, DropReason::Expired));
                continue;
            }
            return Some(item);
        }
        None
    }

    /// Discard everything, reporting each message with the given reason.
    pub fn clear(&mut self, reason: DropReason, dropped: &mut Vec<(Delivery, DropReason)>) {
        self.total.take(self.entries.len());
        for item in self.entries.drain(..) {
            dropped.push((item, reason));
        }
        self.bytes = 0;
    }

    /// Every message waiting, in order.
    ///
    /// Exposed so the conservation property can assert that nothing is
    /// lost rather than assume it.
    pub fn iter(&self) -> impl Iterator<Item = &Delivery> {
        self.entries.iter()
    }

    /// Every message waiting, in order, mutably.
    ///
    /// For the one caller that changes a resident entry rather than
    /// moving it: [`Outbound::renumber`] hands a queued delivery a fresh
    /// sequence number when a resume has put an older message in front
    /// of it. Nothing here may change the *order*, which is the queue's
    /// own to decide.
    ///
    /// [`Outbound::renumber`]: crate::session::Outbound
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Delivery> {
        self.entries.iter_mut()
    }

    /// The message most recently accepted, which is the one a successful
    /// [`Queue::push`] just took.
    ///
    /// There for the journal: a push consumes the delivery, and the
    /// record that says what was queued is worth building only once the
    /// queue has said it took it.
    pub fn back(&self) -> Option<&Delivery> {
        self.entries.back()
    }

    /// How many messages are waiting.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The shared total this queue counts into.
    pub fn total(&self) -> &QueueTotal {
        &self.total
    }

    /// How many bytes they occupy.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Whether nothing is waiting.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The bounds in force.
    pub fn limits(&self) -> QueueLimits {
        self.limits
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::message::Message;
    use bytes::Bytes;
    use mqtt_codec::QoS;

    fn delivery(qos: QoS, payload: &'static [u8]) -> Delivery {
        Delivery {
            message: Message::new("a/b", Bytes::from_static(payload), qos, false),
            qos,
            retain: false,
            subscription_ids: Vec::new(),
            dup: false,
            packet_id: None,
            sequence: 0,
        }
    }

    fn limits(max_messages: usize, overflow: Overflow) -> QueueLimits {
        QueueLimits {
            max_messages,
            max_bytes: usize::MAX,
            overflow,
            queue_qos0_while_offline: false,
        }
    }

    #[test]
    fn holds_messages_in_order() {
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();
        for n in 0..3u8 {
            let mut item = delivery(QoS::AtLeastOnce, b"x");
            item.message.payload = Bytes::from(vec![n]);
            assert!(queue.push(item, 0, true, &mut dropped));
        }
        assert!(dropped.is_empty());
        assert_eq!(queue.len(), 3);

        for n in 0..3u8 {
            let item = queue.pop(0, &mut dropped).expect("in order");
            assert_eq!(&item.message.payload[..], &[n]);
        }
        assert!(queue.is_empty());
    }

    #[test]
    fn a_full_queue_says_so_before_it_is_offered_anything() {
        // What section 6's skip asks of a shared group's members, and
        // it has to be answerable in advance: an offer that is refused
        // has already lost the message. On the boundary in both
        // directions, and against `push` rather than against a
        // restatement of its rule, because the two disagreeing is the
        // whole failure this can have.
        let mut queue = Queue::new(limits(2, Overflow::DropNewest));
        let mut dropped = Vec::new();
        let probe = delivery(QoS::AtLeastOnce, b"1");

        for payload in [b"1", b"2"] {
            assert!(
                !queue.would_refuse(&probe.message, probe.qos, 0, true),
                "room for {payload:?}"
            );
            assert!(queue.push(delivery(QoS::AtLeastOnce, payload), 0, true, &mut dropped));
        }

        assert!(
            queue.would_refuse(&probe.message, probe.qos, 0, true),
            "two is the bound, so it is full"
        );
        assert!(
            !queue.push(delivery(QoS::AtLeastOnce, b"3"), 0, true, &mut dropped),
            "and push agrees"
        );
    }

    #[test]
    fn a_queue_out_of_bytes_says_so_before_it_is_offered_anything() {
        // The other bound, on its own boundary: room for two of this
        // size and not a third, though the message count is nowhere
        // near its own limit.
        let probe = delivery(QoS::AtLeastOnce, b"payload");
        let size = probe.size_in_queue();
        let mut queue = Queue::new(QueueLimits {
            max_messages: usize::MAX,
            max_bytes: size * 2,
            overflow: Overflow::DropNewest,
            queue_qos0_while_offline: false,
        });
        let mut dropped = Vec::new();

        for _ in 0..2 {
            assert!(!queue.would_refuse(&probe.message, probe.qos, 0, true));
            assert!(queue.push(
                delivery(QoS::AtLeastOnce, b"payload"),
                0,
                true,
                &mut dropped
            ));
        }

        assert!(
            queue.would_refuse(&probe.message, probe.qos, 0, true),
            "a third would go over the budget"
        );
        assert!(!queue.push(
            delivery(QoS::AtLeastOnce, b"payload"),
            0,
            true,
            &mut dropped
        ));
    }

    #[test]
    fn drop_newest_refuses_the_arrival_and_says_why() {
        let mut queue = Queue::new(limits(2, Overflow::DropNewest));
        let mut dropped = Vec::new();
        assert!(queue.push(delivery(QoS::AtLeastOnce, b"1"), 0, true, &mut dropped));
        assert!(queue.push(delivery(QoS::AtLeastOnce, b"2"), 0, true, &mut dropped));

        assert!(!queue.push(delivery(QoS::AtLeastOnce, b"3"), 0, true, &mut dropped));
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].1, DropReason::QueueFull);
        assert_eq!(&dropped[0].0.message.payload[..], b"3", "the arrival goes");
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn drop_oldest_evicts_the_head_and_accepts_the_arrival() {
        let mut queue = Queue::new(limits(2, Overflow::DropOldest));
        let mut dropped = Vec::new();
        queue.push(delivery(QoS::AtLeastOnce, b"1"), 0, true, &mut dropped);
        queue.push(delivery(QoS::AtLeastOnce, b"2"), 0, true, &mut dropped);

        assert!(queue.push(delivery(QoS::AtLeastOnce, b"3"), 0, true, &mut dropped));
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].1, DropReason::QueueFull);
        assert_eq!(&dropped[0].0.message.payload[..], b"1", "the head goes");
        assert_eq!(queue.len(), 2);

        let first = queue.pop(0, &mut dropped).unwrap();
        assert_eq!(&first.message.payload[..], b"2");
    }

    #[test]
    fn the_byte_budget_bounds_the_queue_independently_of_the_count() {
        let mut queue = Queue::new(QueueLimits {
            max_messages: 1000,
            max_bytes: delivery(QoS::AtLeastOnce, b"0123456789").size_in_queue() * 2,
            overflow: Overflow::DropNewest,
            queue_qos0_while_offline: false,
        });
        let mut dropped = Vec::new();

        assert!(queue.push(
            delivery(QoS::AtLeastOnce, b"0123456789"),
            0,
            true,
            &mut dropped
        ));
        assert!(queue.push(
            delivery(QoS::AtLeastOnce, b"0123456789"),
            0,
            true,
            &mut dropped
        ));
        assert!(!queue.push(
            delivery(QoS::AtLeastOnce, b"0123456789"),
            0,
            true,
            &mut dropped
        ));

        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].1, DropReason::ByteBudget);
        assert_eq!(queue.len(), 2, "the count limit was nowhere near");
    }

    #[test]
    fn popping_frees_the_byte_budget() {
        let mut queue = Queue::new(QueueLimits {
            max_messages: 1000,
            max_bytes: delivery(QoS::AtLeastOnce, b"0123456789").size_in_queue(),
            overflow: Overflow::DropNewest,
            queue_qos0_while_offline: false,
        });
        let mut dropped = Vec::new();
        assert!(queue.push(
            delivery(QoS::AtLeastOnce, b"0123456789"),
            0,
            true,
            &mut dropped
        ));
        assert!(!queue.push(
            delivery(QoS::AtLeastOnce, b"0123456789"),
            0,
            true,
            &mut dropped
        ));

        queue.pop(0, &mut dropped).unwrap();
        assert_eq!(queue.bytes(), 0);
        assert!(queue.push(
            delivery(QoS::AtLeastOnce, b"0123456789"),
            0,
            true,
            &mut dropped
        ));
    }

    #[test]
    fn a_fire_and_forget_message_is_discarded_while_offline_by_default() {
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();

        assert!(!queue.push(delivery(QoS::AtMostOnce, b"x"), 0, false, &mut dropped));
        assert_eq!(dropped[0].1, DropReason::QosZeroWhileOffline);
        assert!(queue.is_empty());

        // Online, it queues like anything else.
        dropped.clear();
        assert!(queue.push(delivery(QoS::AtMostOnce, b"x"), 0, true, &mut dropped));
        assert!(dropped.is_empty());
    }

    #[test]
    fn queueing_fire_and_forget_while_offline_can_be_turned_on() {
        let mut queue = Queue::new(QueueLimits {
            queue_qos0_while_offline: true,
            ..limits(10, Overflow::DropNewest)
        });
        let mut dropped = Vec::new();
        assert!(queue.push(delivery(QoS::AtMostOnce, b"x"), 0, false, &mut dropped));
        assert!(dropped.is_empty());
    }

    #[test]
    fn an_expired_message_is_discarded_on_the_way_out() {
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();

        let mut stale = delivery(QoS::AtLeastOnce, b"stale");
        stale.message.expires_at = Some(100);
        queue.push(stale, 0, true, &mut dropped);
        queue.push(delivery(QoS::AtLeastOnce, b"fresh"), 0, true, &mut dropped);
        assert!(dropped.is_empty(), "expiry is checked on the way out");

        let got = queue.pop(101, &mut dropped).expect("the fresh one");
        assert_eq!(&got.message.payload[..], b"fresh");
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].1, DropReason::Expired);
        assert!(queue.is_empty());
    }

    #[test]
    fn an_expired_message_is_not_admitted() {
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();
        let mut stale = delivery(QoS::AtLeastOnce, b"x");
        stale.message.expires_at = Some(100);

        assert!(!queue.push(stale, 101, true, &mut dropped));
        assert_eq!(dropped[0].1, DropReason::Expired);
    }

    #[test]
    fn clearing_reports_everything_it_discards() {
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();
        queue.push(delivery(QoS::AtLeastOnce, b"1"), 0, true, &mut dropped);
        queue.push(delivery(QoS::AtLeastOnce, b"2"), 0, true, &mut dropped);

        queue.clear(DropReason::SessionEnded, &mut dropped);
        assert_eq!(dropped.len(), 2);
        assert!(dropped.iter().all(|(_, r)| *r == DropReason::SessionEnded));
        assert!(queue.is_empty());
        assert_eq!(queue.bytes(), 0);
    }

    #[test]
    fn requeueing_puts_a_message_back_at_the_head() {
        // Resume puts unacknowledged messages back before anything newer.
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();
        queue.push(delivery(QoS::AtLeastOnce, b"new"), 0, true, &mut dropped);
        let mut old = delivery(QoS::AtLeastOnce, b"old");
        old.dup = true;
        queue.requeue(old);

        let first = queue.pop(0, &mut dropped).unwrap();
        assert_eq!(&first.message.payload[..], b"old");
        assert!(first.dup, "the flag the caller set survives the requeue");
    }

    #[test]
    fn requeueing_does_not_invent_a_duplicate_flag() {
        // The other caller requeues because no identifier was free. That
        // message was never sent, so it must not go out claiming to be a
        // second attempt at something the subscriber already saw.
        let mut queue = Queue::new(limits(10, Overflow::DropNewest));
        let mut dropped = Vec::new();
        queue.requeue(delivery(QoS::AtLeastOnce, b"never sent"));

        let first = queue.pop(0, &mut dropped).unwrap();
        assert!(
            !first.dup,
            "a message that was never sent is not a duplicate"
        );
    }

    #[test]
    fn a_queue_counts_its_messages_into_the_total_it_shares() {
        let total = QueueTotal::default();
        let mut first = Queue::sharing(QueueLimits::default(), total.clone());
        let mut second = Queue::sharing(QueueLimits::default(), total.clone());
        let mut dropped = Vec::new();

        assert!(first.push(delivery(QoS::AtLeastOnce, b"x"), 0, true, &mut dropped));
        assert!(first.push(delivery(QoS::AtLeastOnce, b"x"), 0, true, &mut dropped));
        assert!(second.push(delivery(QoS::AtLeastOnce, b"x"), 0, true, &mut dropped));
        assert_eq!(total.get(), 3, "three messages across two queues");

        first.pop(0, &mut dropped);
        assert_eq!(total.get(), 2, "one taken");

        first.requeue(delivery(QoS::AtLeastOnce, b"x"));
        assert_eq!(total.get(), 3, "one put back");

        first.clear(DropReason::SessionEnded, &mut dropped);
        assert_eq!(total.get(), 1, "a cleared queue holds nothing");
    }

    #[test]
    fn a_queue_dropped_with_messages_in_it_takes_them_off_the_total() {
        // The broker clears a session's queue before destroying it,
        // so nothing in the broker reaches this today and the
        // differential property cannot see it deleted. It is kept
        // because the alternative is a total that only ever rises the
        // first time a path removes a session without clearing it,
        // and a gauge that only rises is the failure this whole task
        // is about. Reached directly, since nothing else reaches it.
        let total = QueueTotal::default();
        let mut dropped = Vec::new();
        {
            let mut queue = Queue::sharing(QueueLimits::default(), total.clone());
            assert!(queue.push(delivery(QoS::AtLeastOnce, b"x"), 0, true, &mut dropped));
            assert!(queue.push(delivery(QoS::AtLeastOnce, b"x"), 0, true, &mut dropped));
            assert_eq!(total.get(), 2);
        }
        assert_eq!(total.get(), 0, "the queue went and took its messages");
    }
}
