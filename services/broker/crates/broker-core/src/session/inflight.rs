//! Messages sent and not yet acknowledged.

use crate::session::queue::Delivery;

/// Where a message has reached in its acknowledgement exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InflightState {
    /// Sent at least-once, waiting for the acknowledgement.
    AwaitingAck,
    /// Sent exactly-once, waiting for the receipt.
    AwaitingReceived,
    /// Receipt seen and release sent, waiting for completion.
    ///
    /// The distinction matters on resume: a message in this state must be
    /// resumed by resending the release, not the original publish, because
    /// the peer has already taken ownership of it.
    AwaitingComplete,
}

/// One message sent and not yet acknowledged.
#[derive(Debug, Clone)]
pub struct InflightEntry {
    /// The identifier the peer will acknowledge.
    pub packet_id: u16,
    /// What was sent.
    pub delivery: Delivery,
    /// How far the exchange has got.
    pub state: InflightState,
    /// When it was sent, in coarse seconds.
    pub sent_at: u32,
}

/// The window of messages sent and not yet acknowledged.
///
/// Backed by a vector in send order rather than a map. The window is small
/// by design: the default is twenty, and one is the documented way to get
/// strict ordering, so a linear scan is faster than hashing and keeps
/// resume order correct for free.
pub struct Inflight {
    entries: Vec<InflightEntry>,
    limit: usize,
}

impl Inflight {
    /// A window holding at most `limit` messages. Zero means unbounded.
    pub fn new(limit: usize) -> Self {
        Self {
            entries: Vec::new(),
            limit,
        }
    }

    /// Whether another message may be sent.
    pub fn has_room(&self) -> bool {
        self.limit == 0 || self.entries.len() < self.limit
    }

    /// Record a message as sent. The caller checks [`Inflight::has_room`]
    /// first; this does not enforce the limit, because resume legitimately
    /// restores a window that a lowered limit would now refuse.
    pub fn push(&mut self, entry: InflightEntry) {
        self.entries.push(entry);
    }

    /// Find an outstanding message by identifier.
    pub fn get_mut(&mut self, packet_id: u16) -> Option<&mut InflightEntry> {
        self.entries.iter_mut().find(|e| e.packet_id == packet_id)
    }

    /// Take an outstanding message out, its exchange complete.
    pub fn remove(&mut self, packet_id: u16) -> Option<InflightEntry> {
        let index = self.entries.iter().position(|e| e.packet_id == packet_id)?;
        Some(self.entries.remove(index))
    }

    /// Every outstanding message, in the order sent.
    pub fn iter(&self) -> impl Iterator<Item = &InflightEntry> {
        self.entries.iter()
    }

    /// Empty the window, yielding everything in the order sent.
    pub fn drain(&mut self) -> impl Iterator<Item = InflightEntry> + '_ {
        self.entries.drain(..)
    }

    /// Change the limit.
    ///
    /// Lowering it below what is already outstanding stops new sends but
    /// never discards, because a peer advertising a smaller receive
    /// maximum on resume has not forgotten what it already holds.
    pub fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
    }

    /// How many are outstanding.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing is outstanding.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::message::Message;
    use crate::session::queue::Delivery;
    use bytes::Bytes;
    use mqtt_codec::QoS;

    fn entry(packet_id: u16, qos: QoS) -> InflightEntry {
        InflightEntry {
            packet_id,
            delivery: Delivery {
                message: Message::new("a/b", Bytes::from_static(b"x"), qos, false),
                qos,
                retain: false,
                subscription_ids: Vec::new(),
                dup: false,
            },
            state: InflightState::AwaitingAck,
            sent_at: 0,
        }
    }

    #[test]
    fn accepts_up_to_its_limit() {
        let mut inflight = Inflight::new(2);
        assert!(inflight.has_room());
        inflight.push(entry(1, QoS::AtLeastOnce));
        assert!(inflight.has_room());
        inflight.push(entry(2, QoS::AtLeastOnce));
        assert!(!inflight.has_room());
        assert_eq!(inflight.len(), 2);
    }

    #[test]
    fn a_limit_of_zero_means_unbounded() {
        // The protocol has no way to say "no messages at all", and zero is
        // the conventional spelling of "no limit".
        let mut inflight = Inflight::new(0);
        for id in 1..=100 {
            assert!(inflight.has_room());
            inflight.push(entry(id, QoS::AtLeastOnce));
        }
        assert!(inflight.has_room());
    }

    #[test]
    fn finds_and_removes_by_packet_identifier() {
        let mut inflight = Inflight::new(10);
        inflight.push(entry(7, QoS::AtLeastOnce));
        inflight.push(entry(9, QoS::ExactlyOnce));

        assert!(inflight.get_mut(7).is_some());
        assert!(inflight.get_mut(8).is_none());

        let removed = inflight.remove(7).expect("present");
        assert_eq!(removed.packet_id, 7);
        assert!(inflight.get_mut(7).is_none());
        assert_eq!(inflight.len(), 1);
    }

    #[test]
    fn removing_something_absent_reports_nothing() {
        let mut inflight = Inflight::new(10);
        assert!(inflight.remove(1).is_none());
    }

    #[test]
    fn preserves_send_order() {
        // Resume resends in the order originally sent, not in identifier
        // order, which differ once the identifier space wraps.
        let mut inflight = Inflight::new(10);
        for id in [9u16, 3, 7] {
            inflight.push(entry(id, QoS::AtLeastOnce));
        }
        let order: Vec<u16> = inflight.iter().map(|e| e.packet_id).collect();
        assert_eq!(order, vec![9, 3, 7]);
    }

    #[test]
    fn draining_empties_in_send_order() {
        let mut inflight = Inflight::new(10);
        for id in [5u16, 1, 4] {
            inflight.push(entry(id, QoS::AtLeastOnce));
        }
        let drained: Vec<u16> = inflight.drain().map(|e| e.packet_id).collect();
        assert_eq!(drained, vec![5, 1, 4]);
        assert_eq!(inflight.len(), 0);
    }

    #[test]
    fn the_limit_can_be_lowered_below_what_is_already_outstanding() {
        // A peer may advertise a smaller receive maximum on resume. The
        // window must stop accepting, not discard what is already sent.
        let mut inflight = Inflight::new(10);
        for id in 1..=5 {
            inflight.push(entry(id, QoS::AtLeastOnce));
        }
        inflight.set_limit(2);
        assert_eq!(inflight.len(), 5, "nothing outstanding may be discarded");
        assert!(!inflight.has_room());
    }

    #[test]
    fn a_state_transition_is_visible_through_the_lookup() {
        let mut inflight = Inflight::new(10);
        inflight.push(entry(1, QoS::ExactlyOnce));
        inflight.get_mut(1).unwrap().state = InflightState::AwaitingComplete;
        assert_eq!(
            inflight.get_mut(1).unwrap().state,
            InflightState::AwaitingComplete
        );
    }
}
