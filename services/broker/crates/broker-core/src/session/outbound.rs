//! Deciding what goes on the wire, and when.

use std::sync::Arc;

use mqtt_codec::packet::{Ack, Publish};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode};
use storage::record::{Record, SessionKey};

use crate::journal::{self, Journal};
use crate::session::inflight::{Inflight, InflightEntry, InflightState};
use crate::session::message::Message;
use crate::session::packet_ids::PacketIds;
use crate::session::queue::{Delivery, DropReason, Queue, QueueLimits};
use crate::session::Action;

/// Decides what goes on the wire to one client, and when.
///
/// There is deliberately no retransmission timer. MQTT 5 forbids resending
/// on a live connection, and a connection either delivers a packet or
/// dies, so a timer would only manufacture duplicates. Redelivery happens
/// on resume and nowhere else.
pub struct Outbound {
    /// Whose this is, so that a record can say so. Section 7.3: the
    /// broker says what happened, and what happened is always to
    /// somebody.
    session: SessionKey,
    queue: Queue,
    inflight: Inflight,
    packet_ids: PacketIds,
    online: bool,
    /// Reused across calls so the hot path allocates nothing.
    scratch: Vec<(Delivery, DropReason)>,
    /// What changed durably and has not been handed over.
    journal: Journal,
    /// The highest sequence number ever assigned to this session's
    /// queue, whether or not that entry is still held.
    ///
    /// Section 7.4: two entries sharing a number, or a number going
    /// backwards, is what must never happen, so this is a high-water
    /// mark and not the largest key present. Zero for a session nothing
    /// has been queued for, which makes the first assigned number one.
    highest_sequence: u64,
}

impl Outbound {
    /// A new outbound path with the given queue bounds and window size.
    pub fn new(session: SessionKey, limits: QueueLimits, inflight_limit: usize) -> Self {
        Self {
            session,
            queue: Queue::new(limits),
            inflight: Inflight::new(inflight_limit),
            packet_ids: PacketIds::default(),
            online: false,
            scratch: Vec::new(),
            journal: Journal::new(),
            highest_sequence: 0,
        }
    }

    /// Move everything this path has said onto the end of `out`.
    ///
    /// The caller does this under the same lock that produced the
    /// records; see the module documentation on why leaving them here is
    /// not a tidiness question.
    pub fn take_journal(&mut self, out: &mut Vec<Record>) {
        self.journal.drain_into(out);
    }

    /// Whether anything said has not been handed over.
    pub fn has_unsaid(&self) -> bool {
        !self.journal.is_empty()
    }

    /// The highest sequence number ever assigned to this queue.
    pub fn highest_sequence(&self) -> u64 {
        self.highest_sequence
    }

    /// Set the high-water mark, for a path rebuilding this from storage.
    ///
    /// Nothing else may lower it: section 7.4's rule is about the
    /// assignment, and a mark that went backwards would hand a second
    /// entry a number the store has already spent.
    pub fn set_highest_sequence(&mut self, highest: u64) {
        self.highest_sequence = highest;
    }

    /// Whether a connection is attached.
    pub fn set_online(&mut self, online: bool) {
        self.online = online;
    }

    /// Narrow the window to what the peer said it can receive.
    pub fn set_inflight_limit(&mut self, limit: usize) {
        self.inflight.set_limit(limit);
    }

    /// Whether this message, delivered at `qos`, would be refused.
    ///
    /// Asked of a shared subscription's members before one of them is
    /// handed the message, which is the only way section 6's "a member
    /// whose queue is full is skipped" can be honoured: an offer that is
    /// refused has already lost the message.
    ///
    /// The whole message rather than its size, and this session's own
    /// attachment, because [`Queue::would_refuse`] weighs all three of
    /// the reasons an offer is refused and two of them — expiry, and a
    /// fire-and-forget delivery to a session with nobody attached — are
    /// invisible in a byte count.
    ///
    /// Only the queue is consulted. A full inflight window is not a
    /// refusal, because the queue exists precisely to hold what the
    /// window has no room for.
    pub fn would_refuse(&self, message: &Message, qos: QoS, now: u32) -> bool {
        self.queue.would_refuse(message, qos, now, self.online)
    }

    /// Offer a message for delivery.
    ///
    /// This is where a sequence number is assigned, and the only place:
    /// whatever [`Delivery::sequence`] holds on the way in is replaced.
    ///
    /// # What is not written down
    ///
    /// Section 7.3: "At-most-once publications are journalled where they
    /// touch durable state", and one delivered straight to an attached
    /// client touches none. So a fire-and-forget message that would go
    /// out of the front door the moment it came in the back skips the
    /// queue entirely rather than being pushed, popped and journalled
    /// four records deep for nothing. The condition is exactly the one
    /// under which the pump below would have taken it again at once, so
    /// nothing else about it changes.
    pub fn offer(&mut self, mut item: Delivery, now: u32, out: &mut Vec<Action>) {
        if item.qos == QoS::AtMostOnce
            && self.online
            && self.queue.is_empty()
            && self.inflight.has_room()
            && !self
                .queue
                .would_refuse(&item.message, item.qos, now, self.online)
        {
            self.dispatch(item, now, Resting::No, out);
            return;
        }

        let assigned = self.highest_sequence + 1;
        item.sequence = assigned;
        self.scratch.clear();
        let accepted = self.queue.push(item, now, self.online, &mut self.scratch);
        // A push reports two different things in one list: entries it
        // evicted, which were resident and are leaving a queue the store
        // holds them in, and the arrival itself when it was refused,
        // which never entered one. The refusal is last and happens only
        // when the push says no, which is what tells them apart.
        let resident = self.scratch.len() - usize::from(!accepted);
        self.report(resident, out);
        if accepted {
            self.highest_sequence = assigned;
            let (entry, body, id) = {
                let queued = self.queue.back().expect("the push said it took it");
                (
                    journal::queue_entry(queued),
                    journal::body_record(&queued.message),
                    journal::stored_message_id(queued.message.id),
                )
            };
            // One put is one reference, and this is the one reference a
            // queued delivery holds: it is released when the delivery
            // leaves for good, and not when it moves into the window.
            self.journal.push(Record::BodyPut { id, body });
            self.journal.push(Record::QueuePushed {
                session: self.session.clone(),
                entry,
            });
        }
        self.pump(now, out);
    }

    /// Send whatever the window has room for.
    fn pump(&mut self, now: u32, out: &mut Vec<Action>) {
        while self.online && self.inflight.has_room() {
            self.scratch.clear();
            let next = self.queue.pop(now, &mut self.scratch);
            // Everything a pop discards was resident: it was in the
            // queue and expired while waiting there.
            let resident = self.scratch.len();
            self.report(resident, out);
            let Some(item) = next else { break };
            if !self.dispatch(item, now, Resting::Yes, out) {
                break;
            }
        }
    }

    /// Put one message on the wire. Returns whether to keep going.
    ///
    /// `resting` says whether the store believes this delivery is in the
    /// queue, which decides whether leaving it is a thing to write down.
    /// Only a fire-and-forget message taken by [`Outbound::offer`]'s
    /// direct path was never there.
    fn dispatch(
        &mut self,
        item: Delivery,
        now: u32,
        resting: Resting,
        out: &mut Vec<Action>,
    ) -> bool {
        if item.qos == QoS::AtMostOnce {
            out.push(Action::Send(publish(&item, None, now)));
            if resting == Resting::Yes {
                // Nothing follows it: there is no acknowledgement to
                // wait for, so the reference the queue held goes now.
                self.journal.push(Record::QueuePopped {
                    session: self.session.clone(),
                    sequence: item.sequence,
                });
                self.journal.push(Record::BodyReleased {
                    id: journal::stored_message_id(item.message.id),
                });
            }
            return true;
        }

        let packet_id = match item.packet_id {
            // A redelivery keeps the identifier it was first sent with,
            // which is still reserved because the resume did not release
            // it. Section 4.4 requires this: the peer is waiting on that
            // identifier, and a fresh one makes the redelivery look like
            // an unrelated message.
            Some(reserved) => reserved,
            None => {
                let Some(fresh) = self.packet_ids.allocate() else {
                    // Every identifier is outstanding. Put it back and
                    // wait for an acknowledgement to free one; this is a
                    // stall, not a loss.
                    //
                    // Nothing is written down, deliberately. The entry
                    // goes back to the head under the number it already
                    // has, which is the number the store still has it
                    // under, so as far as anything durable is concerned
                    // it never left.
                    self.queue.requeue(item);
                    return false;
                };
                fresh
            }
        };

        out.push(Action::Send(publish(&item, Some(packet_id), now)));
        let state = if item.qos == QoS::AtLeastOnce {
            InflightState::AwaitingAck
        } else {
            InflightState::AwaitingReceived
        };
        let entry = InflightEntry {
            packet_id,
            delivery: item,
            state,
            sent_at: now,
        };
        if resting == Resting::Yes {
            self.journal.push(Record::QueuePopped {
                session: self.session.clone(),
                sequence: entry.delivery.sequence,
            });
        }
        // The body is not released here. The reference the queue took
        // covers the window too, because the entry that leaves the queue
        // is the entry that enters the window, and a store that released
        // on the pop would delete the body between the two.
        self.journal.push(Record::InflightPut {
            session: self.session.clone(),
            entry: journal::inflight_record(&entry),
        });
        self.inflight.push(entry);
        true
    }

    /// Handle an at-least-once acknowledgement.
    ///
    /// It must name an entry that is waiting for one. An acknowledgement
    /// settling an exactly-once entry would end a four-packet exchange
    /// after one packet.
    ///
    /// A failure reason means the peer refused the message rather than
    /// took it, so it is reported as discarded. [MQTT-4.4.0-2] treats
    /// the publish as acknowledged either way and forbids retransmitting
    /// it, which is exactly what makes a refusal a discard: nothing will
    /// ever carry this message again.
    pub fn on_puback(
        &mut self,
        packet_id: u16,
        reason: ReasonCode,
        now: u32,
        out: &mut Vec<Action>,
    ) -> Result<(), ReasonCode> {
        let entry = self.complete(packet_id, InflightState::AwaitingAck)?;
        if reason.is_failure() {
            out.push(Action::Dropped {
                id: entry.delivery.message.id,
                reason: DropReason::RejectedByPeer,
                qos: entry.delivery.qos,
                topic: Arc::clone(&entry.delivery.message.topic),
            });
        }
        self.pump(now, out);
        Ok(())
    }

    /// A publish was discarded on the way out, and no acknowledgement
    /// for it can ever arrive.
    ///
    /// [MQTT-3.1.2-24] forbids sending a packet larger than the peer's
    /// Maximum Packet Size, and [MQTT-3.1.2-25] says what to do
    /// instead: "discard the packet and behave as if it had completed
    /// delivery". Behaving that way is the whole of this method, and
    /// it is the half that is easy to omit. Dropping only the bytes
    /// leaves the entry in the window and its identifier reserved,
    /// waiting on a peer that was never told to answer, so the window
    /// fills one message at a time and delivery stops for good.
    ///
    /// Silent when the identifier names nothing outstanding, or names
    /// an entry the peer already owns. The caller is a writer draining
    /// a queue, so the session it addresses may have been resumed or
    /// taken over since the packet was rendered; an unconditional
    /// release would then settle an unrelated delivery. An entry in
    /// [`InflightState::AwaitingComplete`] is likewise not this one:
    /// what goes out for it is a release, which carries no payload and
    /// cannot be too large.
    pub fn on_discarded(&mut self, packet_id: u16, now: u32, out: &mut Vec<Action>) {
        let state = match self.inflight.get_mut(packet_id) {
            Some(entry) if entry.state != InflightState::AwaitingComplete => entry.state,
            _ => return,
        };
        let entry = self
            .complete(packet_id, state)
            .expect("the entry was just found in that state");
        // Nothing leaves this module unaccounted for. A message that
        // vanishes without an action saying so is a support case
        // nobody can answer, and this one vanishes by design.
        out.push(Action::Dropped {
            id: entry.delivery.message.id,
            reason: DropReason::TooLargeForPeer,
            qos: entry.delivery.qos,
            topic: Arc::clone(&entry.delivery.message.topic),
        });
        self.pump(now, out);
    }

    /// Handle an exactly-once receipt.
    ///
    /// A failure reason ends the exchange: the peer has refused the
    /// message, so there is nothing to release. The message is reported
    /// as discarded, because it was.
    ///
    /// A receipt for an identifier that is not outstanding is answered
    /// with a release carrying `PacketIdentifierNotFound`, not refused.
    /// [MQTT-4.3.3-4] requires a release in answer to any receipt whose
    /// reason is below 0x80, and the release reason table holds exactly
    /// two values, so 0x92 exists for this case and nothing else. Sending
    /// nothing would leave the peer waiting for a release that never
    /// comes, which is worse than a protocol error because nothing
    /// reports it.
    pub fn on_pubrec(
        &mut self,
        packet_id: u16,
        reason: ReasonCode,
        now: u32,
        out: &mut Vec<Action>,
    ) -> Result<(), ReasonCode> {
        if reason.is_failure() {
            // The peer will never take it, and [MQTT-4.4.0-2] forbids
            // retransmitting it. That is a discard, and this module's
            // rule is that nothing is discarded without an action saying
            // so: otherwise the message leaves the window and no counter
            // anywhere moves.
            let entry = self.complete(packet_id, InflightState::AwaitingReceived)?;
            out.push(Action::Dropped {
                id: entry.delivery.message.id,
                reason: DropReason::RejectedByPeer,
                qos: entry.delivery.qos,
                topic: Arc::clone(&entry.delivery.message.topic),
            });
            self.pump(now, out);
            return Ok(());
        }

        if self
            .inflight
            .get_mut(packet_id)
            .is_some_and(|e| e.state != InflightState::AwaitingReceived)
        {
            // Present, but not waiting for a receipt. Answering with a
            // release would put an at-least-once exchange into a state
            // it has no fourth packet for, and a second receipt for an
            // entry already released is the peer contradicting itself.
            return Err(ReasonCode::ProtocolError);
        }

        let Some(entry) = self.inflight.get_mut(packet_id) else {
            // Nothing to track: the exchange is over from our side the
            // moment this goes out. The completion the peer answers with
            // will name an identifier we do not hold either, and is
            // reported to the caller in the same way as any other.
            out.push(Action::Send(Packet::PubRel(Ack {
                packet_id,
                reason: ReasonCode::PacketIdentifierNotFound,
                properties: Properties::default(),
            })));
            return Ok(());
        };
        entry.state = InflightState::AwaitingComplete;
        // How far the exchange has got is durable: section 7.5 resumes
        // an entry in this state by resending the release rather than
        // the publish, and a restart that forgot the state would send
        // the peer a second copy of a message it already owns.
        let updated = journal::inflight_record(entry);
        self.journal.push(Record::InflightPut {
            session: self.session.clone(),
            entry: updated,
        });
        out.push(Action::Send(Packet::PubRel(Ack {
            packet_id,
            reason: ReasonCode::Success,
            properties: Properties::default(),
        })));
        Ok(())
    }

    /// Handle an exactly-once completion.
    ///
    /// A completion is the last packet of the exchange, so one naming an
    /// identifier that is not outstanding is answered with silence: the
    /// specification defines no response to it, and 0x92 may travel only
    /// on a release or a completion, neither of which follows. It is
    /// reported to the caller to be counted, and nothing more.
    pub fn on_pubcomp(
        &mut self,
        packet_id: u16,
        now: u32,
        out: &mut Vec<Action>,
    ) -> Result<(), ReasonCode> {
        self.complete(packet_id, InflightState::AwaitingComplete)?;
        self.pump(now, out);
        Ok(())
    }

    /// Settle an exchange, but only from the state that ends it.
    ///
    /// Matching on the identifier alone is not enough. A completion for
    /// an entry that was never receipted would free the slot and the
    /// identifier and mark the message delivered, with no drop action
    /// and no error: the message is simply gone. The state is what tells
    /// a legitimate final packet from one that arrives out of sequence.
    fn complete(
        &mut self,
        packet_id: u16,
        expected: InflightState,
    ) -> Result<InflightEntry, ReasonCode> {
        let Some(entry) = self.inflight.get_mut(packet_id) else {
            return Err(ReasonCode::PacketIdentifierNotFound);
        };
        if entry.state != expected {
            return Err(ReasonCode::ProtocolError);
        }
        let entry = self
            .inflight
            .remove(packet_id)
            .expect("the entry was found a moment ago");
        self.packet_ids.release(packet_id);
        // Every way an exchange ends comes through here, which is why
        // the records do too: an acknowledgement, a receipt refused, a
        // completion, and a packet discarded for being too large all
        // settle the same entry and release the same reference.
        self.journal.push(Record::InflightRemoved {
            session: self.session.clone(),
            packet_id,
        });
        self.journal.push(Record::BodyReleased {
            id: journal::stored_message_id(entry.delivery.message.id),
        });
        Ok(entry)
    }

    /// The connection went away. Nothing is discarded; the window is held
    /// until the session resumes or expires.
    pub fn on_disconnect(&mut self) {
        self.online = false;
    }

    /// A connection attached again.
    ///
    /// Anything whose receipt already arrived stays outstanding and has
    /// its release resent, because the peer owns it. Everything else goes
    /// back to the head of the queue, marked as a redelivery and still
    /// holding the identifier it was first sent with.
    pub fn on_resume(&mut self, now: u32, out: &mut Vec<Action>) {
        self.online = true;

        let drained: Vec<InflightEntry> = self.inflight.drain().collect();
        let (owned_by_peer, mut to_resend): (Vec<_>, Vec<_>) = drained
            .into_iter()
            .partition(|e| e.state == InflightState::AwaitingComplete);

        for entry in owned_by_peer {
            out.push(Action::Send(Packet::PubRel(Ack {
                packet_id: entry.packet_id,
                reason: ReasonCode::Success,
                properties: Properties::default(),
            })));
            self.inflight.push(entry);
        }

        // Said in arrival order and done in reverse, which are two
        // different orders and both are forced. A record may only ever
        // raise the high-water mark, so the numbers must be written down
        // ascending; a requeue goes to the head, so the last one put
        // back ends up first.
        //
        // The number is a fresh one rather than the one the entry had.
        // Section 7.4 spends a sequence for good — the pop that moved
        // this entry into the window spent its old one — and the store
        // refuses a number it has already seen, so an entry coming back
        // out of the window rejoins the queue carrying the highest
        // number in it while sitting at the head of it. Those two facts
        // disagree, and [`Outbound::renumber`] below is what settles
        // them.
        for entry in &mut to_resend {
            self.highest_sequence += 1;
            entry.delivery.sequence = self.highest_sequence;
            // The identifier stays reserved and travels with the message.
            // Releasing it here and allocating a fresh one on the way out
            // would break exactly-once for any peer that received the
            // first copy and lost its receipt.
            entry.delivery.packet_id = Some(entry.packet_id);
            entry.delivery.dup = true;
            self.journal.push(Record::InflightRemoved {
                session: self.session.clone(),
                packet_id: entry.packet_id,
            });
            // No body reference is taken. The one the delivery took when
            // it was first queued was never released: it covered the
            // window, and it covers the queue again now.
            self.journal.push(Record::QueuePushed {
                session: self.session.clone(),
                entry: journal::queue_entry(&entry.delivery),
            });
        }

        // Reversed, because each requeue goes to the head, so the last one
        // pushed ends up first.
        let requeued = !to_resend.is_empty();
        for entry in to_resend.into_iter().rev() {
            self.queue.requeue(entry.delivery);
        }

        self.pump(now, out);
        if requeued {
            self.renumber();
        }
    }

    /// Put the queue's numbering back in the order the queue is in.
    ///
    /// Section 7.4 makes the sequence number the stored queue's key, so
    /// reading a restored queue in key order is how the order a session
    /// will deliver in survives a restart. A resume breaks that: the
    /// entries coming back out of the window belong at the head and can
    /// only be written down under fresh numbers, which are the largest
    /// in the queue. Everything already waiting behind them is then
    /// numbered *below* messages it must be delivered *after*.
    ///
    /// Usually nothing comes of it, because the pump takes every
    /// requeued entry straight back out again — the window it drained
    /// had room for exactly them. The exception is a window that has
    /// been narrowed since: section 4.9 has the peer's receive maximum
    /// re-read on each connection, the shell narrows the window after
    /// the resume rather than before, so a client that reconnects
    /// advertising less than it did leaves entries stranded in the
    /// queue under numbers that put a newer message in front of them.
    /// A restart then delivers that newer message ahead of redeliveries
    /// the peer is still holding identifiers for, which [MQTT-4.6.0-6]
    /// forbids for any two messages from the same publisher.
    ///
    /// So the queue is walked once and everything from the first
    /// backwards step onwards is given a fresh number, ascending, which
    /// restores "key order is delivery order" without moving anything.
    /// The reference each delivery holds on its body is untouched: the
    /// entry never leaves the queue, so the pop and the push below are
    /// the same entry under a new key and not a release and a take.
    ///
    /// A queue already in order costs a walk and no records, which is
    /// every resume but the narrowed one.
    fn renumber(&mut self) {
        let Self {
            queue,
            journal,
            session,
            highest_sequence,
            ..
        } = self;
        let mut previous = 0;
        let mut renumbering = false;
        for item in queue.iter_mut() {
            if !renumbering && item.sequence > previous {
                previous = item.sequence;
                continue;
            }
            // Once one entry has moved, every entry behind it must move
            // too, or it would overtake them.
            renumbering = true;
            let spent = item.sequence;
            *highest_sequence += 1;
            item.sequence = *highest_sequence;
            journal.push(Record::QueuePopped {
                session: session.clone(),
                sequence: spent,
            });
            journal.push(Record::QueuePushed {
                session: session.clone(),
                entry: journal::queue_entry(item),
            });
        }
    }

    /// Discard everything, reporting each message.
    pub fn clear(&mut self, reason: DropReason, out: &mut Vec<Action>) {
        self.scratch.clear();
        self.queue.clear(reason, &mut self.scratch);
        // Everything a clear reports was resident in the queue.
        let resident = self.scratch.len();
        self.report(resident, out);
        let drained: Vec<InflightEntry> = self.inflight.drain().collect();
        for entry in drained {
            // The window held these identifiers; nothing will
            // acknowledge them now, so they come back here or never.
            self.packet_ids.release(entry.packet_id);
            self.journal.push(Record::InflightRemoved {
                session: self.session.clone(),
                packet_id: entry.packet_id,
            });
            self.journal.push(Record::BodyReleased {
                id: journal::stored_message_id(entry.delivery.message.id),
            });
            out.push(Action::Dropped {
                id: entry.delivery.message.id,
                reason,
                qos: entry.delivery.qos,
                topic: Arc::clone(&entry.delivery.message.topic),
            });
        }
    }

    /// Put back a message that was outstanding when this session was
    /// last written to storage, reserving the identifier it was sent
    /// under.
    ///
    /// The mirror of [`Inbound::reserve`](crate::session::Inbound::reserve).
    /// Without it a session rebuilt from storage, or taken over by
    /// another node, starts with an empty window and hands identifiers
    /// the peer is still holding to unrelated messages.
    ///
    /// Reports whether the identifier was free. A clash means the stored
    /// state disagrees with itself; the entry is restored either way,
    /// because dropping a message is worse than holding a suspect
    /// identifier, and the caller is better placed to say what to do.
    pub fn restore(&mut self, entry: InflightEntry) -> bool {
        let was_free = self.packet_ids.reserve(entry.packet_id);
        self.inflight.push(entry);
        was_free
    }

    /// Put back a message that was waiting in the queue when this
    /// session was last written to storage.
    ///
    /// The queue's own seam, beside [`Outbound::restore`] for the
    /// window. Nothing is journalled: a restore is reading what was
    /// already said, not saying it again, and the delivery keeps the
    /// sequence number it was stored under because that number is spent.
    ///
    /// It goes to the head, so a caller replaying a stored queue replays
    /// it newest first.
    ///
    /// The identifier is reserved, when the entry carries one, exactly
    /// as [`Outbound::restore`] reserves the window's. A queued entry
    /// carries one when a resume put it back before it could be resent,
    /// and the peer is holding that identifier just as surely as it
    /// holds an outstanding one: an allocator rebuilt without it hands
    /// the same value to an unrelated message and the peer discards one
    /// of the two as a repeat. Reports whether it was free, so that a
    /// stored state disagreeing with itself is visible rather than
    /// silent.
    pub fn requeue(&mut self, item: Delivery) -> bool {
        let was_free = match item.packet_id {
            Some(packet_id) => self.packet_ids.reserve(packet_id),
            None => true,
        };
        self.queue.requeue(item);
        was_free
    }

    /// Every message outstanding, in the order sent, for persistence.
    ///
    /// The mirror of [`Inbound::held`](crate::session::Inbound::held).
    pub fn outstanding(&self) -> impl Iterator<Item = &InflightEntry> + '_ {
        self.inflight.iter()
    }

    /// Every message waiting, in the order the queue holds it.
    pub fn waiting(&self) -> impl Iterator<Item = &Delivery> + '_ {
        self.queue.iter()
    }

    /// How many are outstanding.
    pub fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    /// How many are waiting.
    pub fn queued_len(&self) -> usize {
        self.queue.len()
    }

    /// Every message still held, queued or outstanding.
    ///
    /// Exposed so the conservation property can assert that nothing is
    /// lost rather than assume it.
    pub fn pending_ids(&self) -> impl Iterator<Item = crate::session::MessageId> + '_ {
        self.queue
            .iter()
            .map(|d| d.message.id)
            .chain(self.inflight.iter().map(|e| e.delivery.message.id))
    }

    /// Turn everything the queue discarded into drop actions.
    ///
    /// A discarded message that was holding an identifier gives it back.
    /// A resumed message carries its identifier in the queue, so without
    /// this an expiry or an eviction would strand that identifier for the
    /// life of the session.
    /// `resident` is how many of the reported messages were in the
    /// queue, which is all of them except a refused arrival: one that
    /// was never taken has nothing to pop and no reference to release,
    /// and saying otherwise would release a body nobody holds.
    fn report(&mut self, resident: usize, out: &mut Vec<Action>) {
        // Taken out so the identifiers can be released while draining;
        // the buffer goes back afterwards, so the hot path still does not
        // allocate.
        let mut scratch = std::mem::take(&mut self.scratch);
        for (index, (item, reason)) in scratch.drain(..).enumerate() {
            if let Some(packet_id) = item.packet_id {
                self.packet_ids.release(packet_id);
            }
            if index < resident {
                self.journal.push(Record::QueuePopped {
                    session: self.session.clone(),
                    sequence: item.sequence,
                });
                self.journal.push(Record::BodyReleased {
                    id: journal::stored_message_id(item.message.id),
                });
            }
            out.push(Action::Dropped {
                id: item.message.id,
                reason,
                qos: item.qos,
                topic: Arc::clone(&item.message.topic),
            });
        }
        self.scratch = scratch;
    }
}

/// Whether the store believes a delivery is sitting in the queue.
///
/// The one thing [`Outbound::dispatch`] cannot work out for itself, and
/// the difference between a message whose departure must be written down
/// and one that was never written down in the first place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resting {
    /// It is in the queue, and leaving it is a durable change.
    Yes,
    /// It never entered one.
    No,
}

fn publish(item: &Delivery, packet_id: Option<u16>, now: u32) -> Packet {
    // The property block is cloned per delivery because the subscription
    // identifiers differ per subscriber. That clone is the packet-sizing
    // question recorded in the design spec's open questions.
    let mut properties = item.message.properties.live().clone();
    properties.subscription_identifiers = item.subscription_ids.clone();
    // [MQTT-3.3.2-6]: what the client is told is what is left, not what
    // was published. `expires_at` is the subtraction already done, so
    // the remainder is the distance from now to the deadline. Written
    // unconditionally rather than only when a deadline exists, so that a
    // cloned property block cannot carry a stale interval past a message
    // that has none: the retained store keeps the publisher's interval
    // whole, and a message with no deadline must carry none.
    properties.message_expiry_interval = item.message.expires_at.map(|at| at.saturating_sub(now));

    Packet::Publish(Publish {
        dup: item.dup,
        qos: item.qos,
        retain: item.retain,
        topic: item.message.topic.to_string(),
        packet_id,
        payload: item.message.payload.clone(),
        properties,
    })
}

#[cfg(test)]
mod tests {
    /// Whose this path belongs to; see the note in `inbound`.
    fn a_key() -> SessionKey {
        SessionKey::new("", "c")
    }

    /// A property block as a message carries one.
    fn shared(properties: Properties) -> Arc<SharedProperties> {
        Arc::new(SharedProperties::new(properties))
    }

    use super::*;
    use crate::properties::SharedProperties;
    use crate::session::message::Message;
    use crate::session::queue::{Delivery, DropReason, QueueLimits};
    use crate::session::Action;
    use bytes::Bytes;
    use mqtt_codec::packet::Publish;
    use mqtt_codec::{Packet, QoS, ReasonCode};

    #[test]
    fn a_delivery_reports_the_expiry_that_is_left_rather_than_the_one_it_arrived_with() {
        let mut out = online(10);
        let mut actions = Vec::new();
        let mut item = delivery(QoS::AtMostOnce, b"body");
        item.message.expires_at = Some(60);
        item.message.properties = shared(Properties {
            message_expiry_interval: Some(60),
            ..Properties::default()
        });

        // Sent at tick 20, so forty seconds of the sixty remain.
        out.offer(item, 20, &mut actions);

        assert_eq!(
            sent(&actions)[0].properties.message_expiry_interval,
            Some(40),
            "[MQTT-3.3.2-6]: the interval sent is the one received less the wait"
        );
    }

    /// The boundary itself. A message expires strictly after its
    /// deadline, so one sent in the very second of that deadline is
    /// still live and its remainder is zero. Zero and absent are not
    /// the same answer: 3.3.2.3.3 reads an absent Message Expiry
    /// Interval as no expiry at all, so reporting nothing here would
    /// tell the client the opposite of the truth.
    ///
    /// The two tests around this one both pick a remainder well clear
    /// of zero, so dropping a zero remainder passed them both.
    #[test]
    fn a_delivery_in_the_second_of_its_deadline_reports_zero_rather_than_nothing() {
        let mut out = online(10);
        let mut actions = Vec::new();
        let mut item = delivery(QoS::AtMostOnce, b"body");
        item.message.expires_at = Some(60);
        item.message.properties = shared(Properties {
            message_expiry_interval: Some(60),
            ..Properties::default()
        });

        out.offer(item, 60, &mut actions);

        assert_eq!(sent(&actions).len(), 1, "the deadline itself is not past");
        assert_eq!(
            sent(&actions)[0].properties.message_expiry_interval,
            Some(0),
            "3.3.2.3.3: an absent interval means no expiry, which is not what is left"
        );
    }

    /// The queue is where the waiting actually happens, and a queued
    /// message goes out through the acknowledged arm of `dispatch`,
    /// not the fire-and-forget one. `dispatch` calls `publish` twice,
    /// once per arm, and the test above walks only the first: passing
    /// a constant instead of the clock on the second arm survived the
    /// whole suite until this test existed.
    #[test]
    fn a_message_that_waited_offline_reports_what_is_left_when_it_finally_goes() {
        let mut out = Outbound::new(a_key(), QueueLimits::default(), 10);
        let mut actions = Vec::new();
        let mut item = delivery(QoS::AtLeastOnce, b"body");
        item.message.expires_at = Some(60);
        item.message.properties = shared(Properties {
            message_expiry_interval: Some(60),
            ..Properties::default()
        });

        out.offer(item, 5, &mut actions);
        assert!(sent(&actions).is_empty(), "nobody is attached to send to");

        // The client comes back thirty seconds later, with twenty-five
        // of the sixty already spent waiting.
        out.on_resume(30, &mut actions);

        assert_eq!(
            sent(&actions)[0].properties.message_expiry_interval,
            Some(30),
            "[MQTT-3.3.2-6]: the wait comes off the interval on this path too"
        );
    }

    #[test]
    fn a_delivery_with_no_deadline_carries_no_expiry() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtMostOnce, b"body"), 20, &mut actions);
        assert_eq!(sent(&actions)[0].properties.message_expiry_interval, None);
    }

    fn delivery(qos: QoS, payload: &'static [u8]) -> Delivery {
        Delivery {
            message: Message::new("a/b", Bytes::from_static(payload), qos, false),
            qos,
            retain: false,
            dup: false,
            subscription_ids: Vec::new(),
            packet_id: None,
            sequence: 0,
        }
    }

    fn online(inflight_limit: usize) -> Outbound {
        let mut out = Outbound::new(a_key(), QueueLimits::default(), inflight_limit);
        out.set_online(true);
        out
    }

    fn sent(actions: &[Action]) -> Vec<&Publish> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send(Packet::Publish(p)) => Some(p),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn fire_and_forget_goes_out_immediately_and_is_not_tracked() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtMostOnce, b"x"), 0, &mut actions);

        assert_eq!(sent(&actions).len(), 1);
        assert_eq!(sent(&actions)[0].packet_id, None);
        assert_eq!(out.inflight_len(), 0, "nothing to acknowledge");
    }

    #[test]
    fn at_least_once_is_tracked_until_acknowledged() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);

        let packet_id = sent(&actions)[0].packet_id.expect("an identifier");
        assert_eq!(out.inflight_len(), 1);

        actions.clear();
        out.on_puback(packet_id, ReasonCode::Success, 0, &mut actions)
            .expect("known");
        assert_eq!(out.inflight_len(), 0);
    }

    #[test]
    fn exactly_once_runs_the_four_packet_exchange() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.expect("an identifier");

        actions.clear();
        out.on_pubrec(packet_id, ReasonCode::Success, 0, &mut actions)
            .expect("known");
        assert!(
            matches!(actions.first(), Some(Action::Send(Packet::PubRel(_)))),
            "a receipt is answered with a release"
        );
        assert_eq!(out.inflight_len(), 1, "still outstanding until completion");

        actions.clear();
        out.on_pubcomp(packet_id, 0, &mut actions).expect("known");
        assert_eq!(out.inflight_len(), 0);
    }

    #[test]
    fn a_failing_receipt_ends_the_exchange_without_a_release() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();

        actions.clear();
        out.on_pubrec(packet_id, ReasonCode::NotAuthorized, 0, &mut actions)
            .expect("known");
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::Send(Packet::PubRel(_)))),
            "a refused message is not released"
        );
        assert_eq!(out.inflight_len(), 0);
    }

    #[test]
    fn a_receipt_for_an_unknown_identifier_is_answered_with_a_release() {
        // The release reason table holds exactly two values, so 0x92
        // exists for this case and nothing else. Answering with an error
        // and no packet would leave the peer waiting for a release that
        // never comes, which is worse than a protocol error because
        // nothing reports it.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.on_pubrec(99, ReasonCode::Success, 0, &mut actions)
            .expect("an unknown identifier is answered, not refused");

        let Some(Action::Send(Packet::PubRel(ack))) = actions.first() else {
            panic!("a release should have been sent, got {actions:?}");
        };
        assert_eq!(ack.packet_id, 99);
        assert_eq!(ack.reason, ReasonCode::PacketIdentifierNotFound);
        assert_eq!(out.inflight_len(), 0, "there is nothing to track");
    }

    #[test]
    fn an_acknowledgement_does_not_settle_an_exactly_once_entry() {
        // One packet would end a four-packet exchange.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();

        assert_eq!(
            out.on_puback(packet_id, ReasonCode::Success, 0, &mut actions),
            Err(ReasonCode::ProtocolError)
        );
        assert_eq!(out.inflight_len(), 1, "the exchange is still running");
    }

    #[test]
    fn a_completion_does_not_settle_an_entry_that_was_never_receipted() {
        // The dangerous one. Without the state check the broker frees the
        // slot and the identifier and counts the message delivered, with
        // no drop action and no error: it is simply gone.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();

        assert_eq!(
            out.on_pubcomp(packet_id, 0, &mut actions),
            Err(ReasonCode::ProtocolError)
        );
        assert_eq!(out.inflight_len(), 1, "the message is still held");
        assert!(
            out.packet_ids.is_in_use(packet_id),
            "and its identifier is still reserved"
        );
    }

    #[test]
    fn a_receipt_does_not_answer_an_at_least_once_entry_with_a_release() {
        // At-least-once has no third packet, so a release would put the
        // exchange into a state with no way out.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();

        actions.clear();
        assert_eq!(
            out.on_pubrec(packet_id, ReasonCode::Success, 0, &mut actions),
            Err(ReasonCode::ProtocolError)
        );
        assert!(actions.is_empty(), "and no release goes out");
    }

    #[test]
    fn a_failing_receipt_does_not_remove_an_entry_the_peer_still_owns() {
        // Once the peer has receipted successfully it owns the message
        // and we are holding the release. A refusal now contradicts that.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();
        out.on_pubrec(packet_id, ReasonCode::Success, 0, &mut actions)
            .expect("the first receipt is in order");

        assert_eq!(
            out.on_pubrec(packet_id, ReasonCode::NotAuthorized, 0, &mut actions),
            Err(ReasonCode::ProtocolError)
        );
        assert_eq!(out.inflight_len(), 1, "the peer still owns it");
    }

    #[test]
    fn the_window_bounds_how_many_are_in_flight() {
        let mut out = online(2);
        let mut actions = Vec::new();
        for _ in 0..5 {
            out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        }
        assert_eq!(sent(&actions).len(), 2, "only the window goes out");
        assert_eq!(out.queued_len(), 3);
    }

    #[test]
    fn an_acknowledgement_lets_the_next_one_out() {
        let mut out = online(1);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"1"), 0, &mut actions);
        out.offer(delivery(QoS::AtLeastOnce, b"2"), 0, &mut actions);
        let first = sent(&actions)[0].packet_id.unwrap();

        actions.clear();
        out.on_puback(first, ReasonCode::Success, 0, &mut actions)
            .expect("known");
        assert_eq!(sent(&actions).len(), 1, "the queue drains as room appears");
        assert_eq!(out.queued_len(), 0);
    }

    #[test]
    fn nothing_is_sent_while_offline() {
        let mut out = Outbound::new(a_key(), QueueLimits::default(), 10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);

        assert!(sent(&actions).is_empty());
        assert_eq!(out.queued_len(), 1, "it waits instead");
    }

    #[test]
    fn resume_resends_unacknowledged_messages_marked_as_duplicates() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        assert!(!sent(&actions)[0].dup, "the first send is not a duplicate");

        out.set_online(false);
        actions.clear();
        out.on_resume(0, &mut actions);

        let resent = sent(&actions);
        assert_eq!(resent.len(), 1);
        assert!(resent[0].dup, "a redelivery carries the duplicate flag");
    }

    #[test]
    fn resume_keeps_the_identifier_an_at_least_once_message_was_sent_with() {
        // Section 4.4 requires the resumed publish to carry its original
        // identifier. A fresh one leaves the peer holding the first one
        // forever, and the broker will eventually hand that same value to
        // an unrelated message which the peer then discards as a repeat.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        let first = sent(&actions)[0].packet_id.expect("an identifier");

        out.set_online(false);
        actions.clear();
        out.on_resume(0, &mut actions);

        assert_eq!(
            sent(&actions)[0].packet_id,
            Some(first),
            "the redelivery must name the identifier the peer is waiting on"
        );
    }

    #[test]
    fn resume_keeps_the_identifier_an_exactly_once_message_was_sent_with() {
        // The same, and worse: a peer that received the publish and lost
        // the receipt is holding this identifier awaiting a release. A
        // redelivery under a new one has no record on the peer, so the
        // payload reaches the application twice and exactly-once is
        // broken by an ordinary dropped connection.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let first = sent(&actions)[0].packet_id.expect("an identifier");

        out.set_online(false);
        actions.clear();
        out.on_resume(0, &mut actions);

        let resent = sent(&actions);
        assert_eq!(resent.len(), 1);
        assert_eq!(resent[0].packet_id, Some(first));
        assert!(resent[0].dup, "and it is still a redelivery");
    }

    #[test]
    fn an_identifier_held_for_a_redelivery_is_freed_if_the_message_is_dropped() {
        // The resumed message keeps its identifier while it waits in the
        // queue. If it never goes out, the identifier must come back:
        // otherwise every interrupted delivery leaks one, and a long
        // lived session runs out.
        let mut out = online(10);
        let mut actions = Vec::new();

        let mut item = delivery(QoS::AtLeastOnce, b"x");
        item.message.expires_at = Some(5);
        out.offer(item, 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.expect("an identifier");

        out.set_online(false);
        actions.clear();
        // Resume after the deadline: the message is requeued holding its
        // identifier, then discarded on the way out.
        out.on_resume(9, &mut actions);

        assert!(sent(&actions).is_empty(), "it expired instead of going out");
        assert!(matches!(
            actions.first(),
            Some(Action::Dropped {
                reason: DropReason::Expired,
                ..
            })
        ));

        assert!(
            !out.packet_ids.is_in_use(packet_id),
            "the identifier it was holding must have come back"
        );
    }

    #[test]
    fn resume_resends_a_release_rather_than_the_publish() {
        // Once the peer has sent a receipt it owns the message. Sending
        // the publish again would deliver it twice.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();
        out.on_pubrec(packet_id, ReasonCode::Success, 0, &mut actions)
            .expect("known");

        out.set_online(false);
        actions.clear();
        out.on_resume(0, &mut actions);

        assert!(sent(&actions).is_empty(), "the publish must not be resent");
        assert!(
            matches!(actions.first(), Some(Action::Send(Packet::PubRel(_)))),
            "the release is resent instead"
        );
    }

    #[test]
    fn resume_preserves_the_order_messages_were_sent_in() {
        let mut out = online(3);
        let mut actions = Vec::new();
        for n in 0..3u8 {
            let mut item = delivery(QoS::AtLeastOnce, b"x");
            item.message.payload = Bytes::from(vec![n]);
            out.offer(item, 0, &mut actions);
        }

        out.set_online(false);
        actions.clear();
        out.on_resume(0, &mut actions);

        let order: Vec<u8> = sent(&actions).iter().map(|p| p.payload[0]).collect();
        assert_eq!(order, vec![0, 1, 2]);
    }

    #[test]
    fn an_identifier_is_free_again_once_the_exchange_completes() {
        // Nothing else asserts this, and the failure it guards against is
        // invisible: the session keeps working until it has sent 65,535
        // messages, then stalls forever because every identifier is
        // recorded as outstanding. It looks like a healthy broker right
        // up to the point a client stops receiving.
        let mut out = online(10);
        let mut actions = Vec::new();

        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        let first = sent(&actions)[0].packet_id.unwrap();
        out.on_puback(first, ReasonCode::Success, 0, &mut actions)
            .expect("known");
        assert!(
            !out.packet_ids.is_in_use(first),
            "an acknowledged message gives its identifier back"
        );

        actions.clear();
        out.offer(delivery(QoS::ExactlyOnce, b"y"), 0, &mut actions);
        let second = sent(&actions)[0].packet_id.unwrap();
        out.on_pubrec(second, ReasonCode::Success, 0, &mut actions)
            .expect("known");
        assert!(
            out.packet_ids.is_in_use(second),
            "but not before the exchange is over"
        );
        out.on_pubcomp(second, 0, &mut actions).expect("known");
        assert!(
            !out.packet_ids.is_in_use(second),
            "a completed exchange gives its identifier back"
        );

        assert_eq!(out.packet_ids.in_use(), 0);
    }

    #[test]
    fn resume_republishes_a_message_whose_receipt_never_arrived() {
        // The mirror of resume_resends_a_release_rather_than_the_publish.
        // Only an entry whose receipt arrived may be resumed with a
        // release. Doing it for one still awaiting a receipt sends a
        // release for a message the client never received; the client
        // answers packet-identifier-not-found, the broker counts the
        // message delivered, and it is silently lost.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();

        out.set_online(false);
        actions.clear();
        out.on_resume(0, &mut actions);

        let resent = sent(&actions);
        assert_eq!(resent.len(), 1, "the publish itself must go out again");
        assert_eq!(resent[0].packet_id, Some(packet_id));
        assert!(resent[0].dup);
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::Send(Packet::PubRel(_)))),
            "and no release, because the client never took ownership"
        );
    }

    #[test]
    fn a_restored_window_keeps_its_identifiers_out_of_circulation() {
        // Takeover and cluster resume both rebuild a session from
        // storage. If the window comes back empty the peer is still
        // holding identifiers this allocator thinks are free.
        let mut out = online(10);

        assert!(out.restore(InflightEntry {
            packet_id: 1,
            delivery: delivery(QoS::AtLeastOnce, b"stored"),
            state: InflightState::AwaitingAck,
            sent_at: 0,
        }));

        assert_eq!(out.inflight_len(), 1);
        assert_eq!(out.outstanding().count(), 1);
        assert!(out.packet_ids.is_in_use(1));

        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"new"), 0, &mut actions);
        assert_ne!(
            sent(&actions)[0].packet_id,
            Some(1),
            "a new message must not take an identifier the peer holds"
        );
    }

    #[test]
    fn a_restored_window_is_redelivered_on_resume() {
        // The whole point of restoring it: the takeover node resends
        // what the old one had outstanding, under the same identifiers.
        let mut out = Outbound::new(a_key(), QueueLimits::default(), 10);
        out.restore(InflightEntry {
            packet_id: 7,
            delivery: delivery(QoS::ExactlyOnce, b"stored"),
            state: InflightState::AwaitingReceived,
            sent_at: 0,
        });

        let mut actions = Vec::new();
        out.on_resume(0, &mut actions);

        let resent = sent(&actions);
        assert_eq!(resent.len(), 1);
        assert_eq!(resent[0].packet_id, Some(7));
        assert!(resent[0].dup);
    }

    #[test]
    fn a_requeued_message_keeps_its_identifier_out_of_circulation() {
        // The queue's half of the same rule as
        // `a_restored_window_keeps_its_identifiers_out_of_circulation`,
        // and the easier one to miss: a queued entry carries an
        // identifier only when a resume put it back before it could be
        // resent, which is a state a live broker passes through and a
        // stored one can be read back in the middle of. The peer holds
        // that identifier either way. Without the reservation the
        // allocator hands the same value to the next message and the
        // peer discards one of the two as a repeat.
        let mut out = online(10);
        let mut waiting = delivery(QoS::AtLeastOnce, b"waiting");
        waiting.packet_id = Some(1);
        waiting.dup = true;
        assert!(out.requeue(waiting));

        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"new"), 0, &mut actions);
        let issued: Vec<Option<u16>> = sent(&actions).iter().map(|p| p.packet_id).collect();
        assert!(
            !issued.contains(&Some(1)) || issued.iter().filter(|id| **id == Some(1)).count() == 1,
            "the requeued message may use its own identifier and nothing else may"
        );
        assert!(
            out.packet_ids.is_in_use(1),
            "the identifier the peer is holding must be reserved"
        );
        assert_eq!(
            sent(&actions)
                .iter()
                .find(|p| &p.payload[..] == b"new")
                .expect("the new message went out")
                .packet_id,
            Some(2),
            "so a new message takes the next one instead"
        );
    }

    #[test]
    fn requeueing_an_identifier_the_window_holds_reports_the_clash() {
        let mut out = online(10);
        assert!(out.restore(InflightEntry {
            packet_id: 4,
            delivery: delivery(QoS::AtLeastOnce, b"outstanding"),
            state: InflightState::AwaitingAck,
            sent_at: 0,
        }));
        let mut waiting = delivery(QoS::AtLeastOnce, b"waiting");
        waiting.packet_id = Some(4);
        assert!(
            !out.requeue(waiting),
            "the stored state claims one identifier twice"
        );
        assert_eq!(
            out.queued_len(),
            1,
            "but the message is not dropped over it"
        );
    }

    #[test]
    fn requeueing_a_message_that_never_had_an_identifier_reserves_nothing() {
        let mut out = Outbound::new(a_key(), QueueLimits::default(), 10);
        assert!(out.requeue(delivery(QoS::AtLeastOnce, b"never sent")));
        assert_eq!(out.packet_ids.in_use(), 0);
    }

    #[test]
    fn restoring_an_identifier_twice_reports_the_clash() {
        let mut out = online(10);
        assert!(out.restore(InflightEntry {
            packet_id: 3,
            delivery: delivery(QoS::AtLeastOnce, b"a"),
            state: InflightState::AwaitingAck,
            sent_at: 0,
        }));
        assert!(
            !out.restore(InflightEntry {
                packet_id: 3,
                delivery: delivery(QoS::AtLeastOnce, b"b"),
                state: InflightState::AwaitingAck,
                sent_at: 0,
            }),
            "the second says the stored state disagrees with itself"
        );
        assert_eq!(out.inflight_len(), 2, "but neither message is dropped");
    }

    #[test]
    fn a_drop_is_reported_with_its_reason_and_topic() {
        let mut out = Outbound::new(
            a_key(),
            QueueLimits {
                max_messages: 1,
                ..QueueLimits::default()
            },
            10,
        );
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"1"), 0, &mut actions);
        out.offer(delivery(QoS::AtLeastOnce, b"2"), 0, &mut actions);

        let dropped: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Dropped { reason, topic, .. } => Some((*reason, topic.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].0, DropReason::QueueFull);
        assert_eq!(&*dropped[0].1, "a/b");
    }

    #[test]
    fn clearing_reports_every_message_it_discards() {
        // Both halves must be accounted for: what is waiting and what is
        // already on the wire. A message that disappears without a drop
        // action is a support case nobody can answer.
        let mut out = online(1);
        let mut actions = Vec::new();
        let sent_one = delivery(QoS::AtLeastOnce, b"1");
        let queued_one = delivery(QoS::AtLeastOnce, b"2");
        let expected = vec![sent_one.message.id, queued_one.message.id];
        out.offer(sent_one, 0, &mut actions);
        out.offer(queued_one, 0, &mut actions);
        assert_eq!(out.inflight_len(), 1);
        assert_eq!(out.queued_len(), 1);

        actions.clear();
        out.clear(DropReason::SessionEnded, &mut actions);

        let mut reported: Vec<_> = actions
            .iter()
            .filter_map(|a| match a {
                Action::Dropped { id, reason, .. } => {
                    assert_eq!(*reason, DropReason::SessionEnded);
                    Some(*id)
                }
                _ => None,
            })
            .collect();
        reported.sort();
        let mut expected = expected;
        expected.sort();
        assert_eq!(reported, expected, "every message must be reported");
        assert_eq!(out.queued_len(), 0);
        assert_eq!(out.inflight_len(), 0);
    }

    #[test]
    fn an_exactly_once_message_discarded_for_size_frees_its_slot_and_its_identifier() {
        // [MQTT-3.1.2-25]: behave as if delivery had completed. The
        // exactly-once case is the one with no existing completion
        // path: `on_puback` ends an at-least-once exchange, but a
        // message still awaiting its receipt has nothing that ends it
        // short of a refusal. Leaving it outstanding holds a window
        // slot and an identifier for an acknowledgement that cannot
        // arrive, because the packet was never sent.
        let mut out = online(1);
        let mut actions = Vec::new();
        let item = delivery(QoS::ExactlyOnce, b"x");
        let message_id = item.message.id;
        out.offer(item, 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();

        actions.clear();
        out.on_discarded(packet_id, 0, &mut actions);

        assert!(
            actions.iter().any(|a| matches!(
                a,
                Action::Dropped { id, reason: DropReason::TooLargeForPeer, .. }
                    if *id == message_id
            )),
            "a message that vanishes without a counter moving is unanswerable"
        );
        assert_eq!(out.inflight_len(), 0, "the slot is free");
        assert!(
            !out.packet_ids.is_in_use(packet_id),
            "and so is the identifier"
        );
    }

    #[test]
    fn a_message_behind_one_discarded_for_size_is_sent() {
        // The consequence of the test above, and the one a test that
        // only watches the discarded message cannot see. With a window
        // of one, an uncompleted discard stops this session delivering
        // anything, ever again.
        let mut out = online(1);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"too big"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();
        out.offer(delivery(QoS::ExactlyOnce, b"next"), 0, &mut actions);
        assert_eq!(sent(&actions).len(), 1, "the window holds one");

        actions.clear();
        out.on_discarded(packet_id, 0, &mut actions);

        let sent = sent(&actions);
        assert_eq!(sent.len(), 1, "the one behind it goes out");
        assert_eq!(&sent[0].payload[..], b"next");
    }

    #[test]
    fn a_message_the_peer_already_owns_is_not_discarded_for_size() {
        // A message past its receipt is resumed by resending the
        // release, which carries no payload and cannot be too large.
        // So a discard naming that identifier is a writer settling an
        // exchange that is not its own: the identifier has been reused
        // by a session that resumed or was taken over since. Freeing
        // it would strand the exactly-once delivery that now holds it.
        let mut out = online(4);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();
        out.on_pubrec(packet_id, ReasonCode::Success, 0, &mut actions)
            .expect("known");

        actions.clear();
        out.on_discarded(packet_id, 0, &mut actions);

        assert!(actions.is_empty(), "nothing to settle here");
        assert_eq!(out.inflight_len(), 1, "the exchange is still the peer's");
        assert!(out.packet_ids.is_in_use(packet_id));
    }

    #[test]
    fn a_discard_naming_nothing_outstanding_does_nothing() {
        // The writer renders from a queue, so by the time it discards,
        // the session may have been cleared or resumed out from under
        // the identifier. That is ordinary, not a fault.
        let mut out = online(4);
        let mut actions = Vec::new();

        out.on_discarded(7, 0, &mut actions);

        assert!(actions.is_empty());
        assert_eq!(out.inflight_len(), 0);
    }

    #[test]
    fn a_refused_message_is_reported_as_discarded() {
        // It leaves the window and will never be retransmitted, so it is
        // as discarded as one the queue evicted. This module's rule is
        // that nothing goes without an action naming why.
        let mut out = online(10);
        let mut actions = Vec::new();
        let item = delivery(QoS::ExactlyOnce, b"x");
        let message_id = item.message.id;
        out.offer(item, 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();

        actions.clear();
        out.on_pubrec(packet_id, ReasonCode::NotAuthorized, 0, &mut actions)
            .expect("known");

        assert!(
            actions.iter().any(|a| matches!(
                a,
                Action::Dropped { id, reason: DropReason::RejectedByPeer, .. } if *id == message_id
            )),
            "the refusal must be counted, not swallowed"
        );
    }

    #[test]
    fn an_at_least_once_refusal_is_reported_as_discarded() {
        // The mirror of a_refused_message_is_reported_as_discarded, on
        // the other packet. [MQTT-4.4.0-2] forbids retransmitting it, so
        // nothing will ever carry this message again: that is a discard,
        // and a peer refusing must not look like a peer accepting.
        let mut out = online(10);
        let mut actions = Vec::new();
        let item = delivery(QoS::AtLeastOnce, b"x");
        let message_id = item.message.id;
        out.offer(item, 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();

        actions.clear();
        out.on_puback(packet_id, ReasonCode::NotAuthorized, 0, &mut actions)
            .expect("known");

        assert!(
            actions.iter().any(|a| matches!(
                a,
                Action::Dropped { id, reason: DropReason::RejectedByPeer, .. } if *id == message_id
            )),
            "the refusal must be counted, not swallowed"
        );
        assert_eq!(out.inflight_len(), 0, "and the exchange is over either way");
        assert!(!out.packet_ids.is_in_use(packet_id));
    }

    #[test]
    fn an_at_least_once_success_is_not_reported_as_discarded() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();

        actions.clear();
        out.on_puback(packet_id, ReasonCode::Success, 0, &mut actions)
            .expect("known");
        assert!(
            !actions.iter().any(|a| matches!(a, Action::Dropped { .. })),
            "a delivered message is not a discarded one"
        );
    }

    #[test]
    fn a_refused_message_gives_its_identifier_back() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::ExactlyOnce, b"x"), 0, &mut actions);
        let packet_id = sent(&actions)[0].packet_id.unwrap();

        out.on_pubrec(packet_id, ReasonCode::NotAuthorized, 0, &mut actions)
            .expect("known");
        assert!(!out.packet_ids.is_in_use(packet_id));
    }

    #[test]
    fn clearing_gives_back_the_identifiers_the_window_held() {
        // A session that is torn down and rebuilt keeps its allocator.
        // Without this, every takeover leaks the whole window.
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        out.offer(delivery(QoS::ExactlyOnce, b"y"), 0, &mut actions);
        assert_eq!(out.packet_ids.in_use(), 2);

        actions.clear();
        out.clear(DropReason::SessionEnded, &mut actions);

        assert_eq!(out.inflight_len(), 0);
        assert_eq!(
            out.packet_ids.in_use(),
            0,
            "nothing will acknowledge them now, so they come back here or never"
        );
    }

    #[test]
    fn disconnecting_stops_anything_further_going_out() {
        let mut out = online(10);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"1"), 0, &mut actions);
        assert_eq!(sent(&actions).len(), 1);

        out.on_disconnect();
        actions.clear();
        out.offer(delivery(QoS::AtLeastOnce, b"2"), 0, &mut actions);

        assert!(sent(&actions).is_empty(), "nobody is attached");
        assert_eq!(out.queued_len(), 1, "it waits instead");
        assert_eq!(out.inflight_len(), 1, "and the window is held, not lost");
    }

    #[test]
    fn the_subscription_identifiers_are_echoed_on_the_packet() {
        // This is how an MQTT 5 client learns which of its subscriptions
        // matched, and the identifiers differ per subscriber, which is
        // why the property block is cloned per delivery.
        let mut out = online(10);
        let mut actions = Vec::new();
        let mut item = delivery(QoS::AtLeastOnce, b"x");
        item.subscription_ids = vec![7, 9];
        out.offer(item, 0, &mut actions);

        let published = sent(&actions);
        assert_eq!(published.len(), 1);
        assert_eq!(
            published[0].properties.subscription_identifiers,
            vec![7, 9],
            "both identifiers, in order"
        );
    }

    #[test]
    fn a_subscription_identifier_on_the_message_never_reaches_a_subscriber_that_has_none() {
        // A Subscription Identifier belongs to the subscriber, not to
        // the message: [MQTT-3.3.4-6] forbids a Client from putting one
        // on a PUBLISH, and nothing upstream strips it, so a message's
        // property block may carry one. It also reaches here through
        // the retained store, which keeps the publisher's properties
        // whole. The overwrite below is what keeps either from leaking,
        // and it is unconditional for exactly that reason.
        //
        // The existing echo test cannot see this: it gives the delivery
        // identifiers of its own, so an overwrite guarded by
        // `if !subscription_ids.is_empty()` passes it and the whole
        // suite besides.
        let mut out = online(10);
        let mut actions = Vec::new();
        let mut item = delivery(QoS::AtLeastOnce, b"x");
        item.message.properties = shared(Properties {
            subscription_identifiers: vec![3],
            ..Properties::default()
        });
        assert!(item.subscription_ids.is_empty(), "this subscriber has none");
        out.offer(item, 0, &mut actions);

        let published = sent(&actions);
        assert_eq!(published.len(), 1);
        assert!(
            published[0].properties.subscription_identifiers.is_empty(),
            "an identifier the subscriber did not earn must not go out"
        );
    }

    #[test]
    fn a_message_that_expired_while_waiting_is_dropped_rather_than_sent() {
        let mut out = Outbound::new(a_key(), QueueLimits::default(), 10);
        let mut actions = Vec::new();
        let mut item = delivery(QoS::AtLeastOnce, b"stale");
        item.message.expires_at = Some(100);
        let id = item.message.id;
        out.offer(item, 0, &mut actions);
        assert_eq!(out.queued_len(), 1, "it waits while nobody is attached");

        actions.clear();
        out.on_resume(101, &mut actions);

        assert!(sent(&actions).is_empty(), "the deadline has passed");
        assert!(
            actions.iter().any(|a| matches!(
                a,
                Action::Dropped {
                    id: dropped,
                    reason: DropReason::Expired,
                    ..
                } if *dropped == id
            )),
            "the drop must be reported with its reason, got {actions:?}"
        );
        assert_eq!(out.queued_len(), 0);
    }

    #[test]
    fn narrowing_the_window_lets_fewer_out() {
        // A peer may advertise a smaller receive maximum than the default.
        let mut out = online(10);
        out.set_inflight_limit(1);
        let mut actions = Vec::new();
        for _ in 0..3 {
            out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        }

        assert_eq!(sent(&actions).len(), 1, "only the narrowed window");
        assert_eq!(out.inflight_len(), 1);
        assert_eq!(out.queued_len(), 2);
    }

    // ---- sequence numbers, section 7.4 ---------------------------------

    /// The numbers the queue is holding, in the order it holds them.
    fn queued_sequences(out: &Outbound) -> Vec<u64> {
        out.waiting().map(|item| item.sequence).collect()
    }

    #[test]
    fn a_sequence_number_is_assigned_on_enqueue_and_counts_up() {
        // Section 7.4: per-session, assigned on enqueue, monotonic. The
        // first is one, because the high-water mark of a session nothing
        // has been queued for is zero.
        let mut out = Outbound::new(a_key(), QueueLimits::default(), 10);
        let mut actions = Vec::new();
        for _ in 0..3 {
            out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        }

        assert_eq!(queued_sequences(&out), vec![1, 2, 3]);
        assert_eq!(out.highest_sequence(), 3);
    }

    #[test]
    fn a_refused_message_does_not_spend_a_number() {
        // The high-water mark is what was assigned, and nothing is
        // assigned to a message the queue would not take. A mark raised
        // by a refusal is not wrong in the way a reused number is, but
        // it is a number the store will never see and a gap nothing
        // explains.
        let mut out = Outbound::new(
            a_key(),
            QueueLimits {
                max_messages: 1,
                ..QueueLimits::default()
            },
            10,
        );
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"1"), 0, &mut actions);
        out.offer(delivery(QoS::AtLeastOnce, b"2"), 0, &mut actions);

        assert_eq!(out.queued_len(), 1, "the second was refused");
        assert_eq!(out.highest_sequence(), 1);
    }

    #[test]
    fn a_sequence_number_is_not_reused_after_the_entry_leaves_the_queue() {
        // The one a naive "next is the length plus one" gets wrong, and
        // it gets it wrong silently: the store refuses the reused number
        // and the batch carrying it is a message lost.
        let mut out = Outbound::new(a_key(), QueueLimits::default(), 10);
        let mut actions = Vec::new();
        for _ in 0..3 {
            out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        }
        out.on_resume(0, &mut actions);
        assert_eq!(out.queued_len(), 0, "the window took all three");

        out.offer(delivery(QoS::AtLeastOnce, b"fourth"), 0, &mut actions);
        assert_eq!(
            queued_sequences(&out),
            Vec::<u64>::new(),
            "it went straight out"
        );
        assert_eq!(
            out.outstanding()
                .last()
                .expect("it is outstanding")
                .delivery
                .sequence,
            4,
            "an empty queue must not hand out one again"
        );
    }

    #[test]
    fn two_sessions_number_their_queues_independently() {
        // Per-session, so the second session's first message is its
        // first, whatever the first session has spent. A counter shared
        // between them would number this three.
        let mut first = Outbound::new(SessionKey::new("", "one"), QueueLimits::default(), 10);
        let mut second = Outbound::new(SessionKey::new("", "two"), QueueLimits::default(), 10);
        let mut actions = Vec::new();
        for _ in 0..2 {
            first.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);
        }
        second.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);

        assert_eq!(queued_sequences(&first), vec![1, 2]);
        assert_eq!(queued_sequences(&second), vec![1]);
    }

    #[test]
    fn a_restored_session_carries_on_from_the_mark_it_left() {
        // Section 7.4: the mark is the highest ever assigned, not the
        // largest key still present, so a restored session that started
        // again from one would hand out numbers the store has already
        // spent on entries it has since delivered.
        let mut out = Outbound::new(a_key(), QueueLimits::default(), 10);
        out.set_highest_sequence(41);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"x"), 0, &mut actions);

        assert_eq!(queued_sequences(&out), vec![42]);
    }

    #[test]
    fn a_resume_numbers_a_redelivery_above_what_is_already_waiting() {
        // The requeued entry goes to the head, so it must also carry the
        // larger number, or the stored order and the delivered order
        // disagree.
        let mut out = online(1);
        let mut actions = Vec::new();
        out.offer(delivery(QoS::AtLeastOnce, b"sent"), 0, &mut actions);
        out.offer(delivery(QoS::AtLeastOnce, b"waiting"), 0, &mut actions);
        assert_eq!(queued_sequences(&out), vec![2], "the second waits");

        out.on_disconnect();
        actions.clear();
        out.on_resume(0, &mut actions);

        // The window of one took the redelivery straight back, so only
        // the message that was already waiting is left.
        assert_eq!(queued_sequences(&out), vec![2]);
        assert_eq!(
            out.outstanding()
                .next()
                .expect("the redelivery")
                .delivery
                .sequence,
            3,
            "a spent number cannot come back, so the redelivery takes a new one"
        );
    }

    #[test]
    fn a_narrowed_window_leaves_the_queue_numbered_in_the_order_it_will_deliver() {
        // The case the pump does not cover. Section 4.9 re-reads the
        // peer's receive maximum on every connection and the shell
        // narrows the window after the resume, so a client that comes
        // back advertising less than it did strands redeliveries in the
        // queue. They carry the largest numbers and sit at the head, and
        // a restart reading the stored queue in key order would deliver
        // the message behind them first: a message the peer has never
        // seen, ahead of duplicates it is still holding identifiers for.
        //
        // Nothing else sees this. The differential test compares a live
        // broker against one rebuilt from its own records, and both read
        // the same queue; what it cannot see is the two of them reading
        // it in different orders.
        let mut out = online(3);
        let mut actions = Vec::new();
        for payload in [b"a", b"b", b"c"] {
            out.offer(delivery(QoS::AtLeastOnce, payload), 0, &mut actions);
        }
        out.offer(delivery(QoS::AtLeastOnce, b"waiting"), 0, &mut actions);
        assert_eq!(out.inflight_len(), 3);
        assert_eq!(queued_sequences(&out), vec![4]);

        out.on_disconnect();
        out.set_inflight_limit(1);
        actions.clear();
        out.on_resume(0, &mut actions);

        let order = queued_sequences(&out);
        let mut ascending = order.clone();
        ascending.sort_unstable();
        assert_eq!(
            order, ascending,
            "the stored queue is read in key order, so key order is delivery order"
        );
        // Concretely: a, b and c were requeued as 5, 6 and 7, the
        // narrowed window took only a back, and the message that was
        // waiting behind them as 4 has to move above the two left.
        assert_eq!(order, vec![6, 7, 8]);
        let payloads: Vec<&[u8]> = out
            .waiting()
            .map(|item| &item.message.payload[..])
            .collect();
        assert_eq!(
            payloads,
            vec![&b"b"[..], &b"c"[..], &b"waiting"[..]],
            "and nothing has changed places"
        );
    }

    #[test]
    fn a_resume_that_the_window_absorbs_renumbers_nothing() {
        // The common case, and the reason the walk above is a walk
        // rather than a rewrite: the window the resume drained has room
        // for exactly what came out of it, so everything still waiting
        // keeps the number it was given.
        let mut out = online(2);
        let mut actions = Vec::new();
        for payload in [b"a", b"b"] {
            out.offer(delivery(QoS::AtLeastOnce, payload), 0, &mut actions);
        }
        out.offer(delivery(QoS::AtLeastOnce, b"waiting"), 0, &mut actions);

        out.on_disconnect();
        actions.clear();
        out.on_resume(0, &mut actions);

        assert_eq!(
            queued_sequences(&out),
            vec![3],
            "the entry that never moved keeps its number"
        );
        assert_eq!(
            out.highest_sequence(),
            5,
            "only the two redeliveries spent one"
        );
    }

    #[test]
    fn an_acknowledgement_for_an_unknown_identifier_is_reported() {
        let mut out = online(10);
        let mut actions = Vec::new();
        assert!(out
            .on_puback(99, ReasonCode::Success, 0, &mut actions)
            .is_err());
    }
}
