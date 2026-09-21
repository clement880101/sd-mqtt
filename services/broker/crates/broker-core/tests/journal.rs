//! Everything the broker must not lose, it says out loud.
//!
//! The property the milestone turns on, and its difficulty is not the
//! code. It is **exhaustiveness**: a record type that is never emitted is
//! a piece of state that silently does not survive a restart, nothing
//! fails, and no ordinary test looks for the absence of a record. A list
//! of hand-written assertions tests only the cases whoever wrote the list
//! thought of, which is the same list the implementer was working from.
//!
//! So this is differential, in the shape `tests/differential.rs` uses
//! against a naive matcher and the storage crate uses against its
//! in-memory model:
//!
//! 1. Drive a live [`Broker`] through a generated sequence of client
//!    operations, taking its journal after every one.
//! 2. Apply every record to a [`MemoryStorage`], in order, as the shell
//!    would.
//! 3. Restore, and rebuild a second broker from what comes back.
//! 4. Assert the two brokers' durable state is equal.
//!
//! Anything the broker changes and does not say makes step 4 fail, and it
//! fails whether or not anybody thought of it. That is the whole point.
//!
//! # What "the same" means, and what it deliberately does not
//!
//! [`Broker::durable_state`] reports durable state only. It says nothing
//! about connections, handles, keep-alive or receive quota, and the
//! reasoning is on the type: the rebuilt broker has none of a running
//! one's connection state and is right not to, so a comparison including
//! it would fail for correct reasons.
//!
//! One durable field follows from that and is therefore expected to
//! differ. A connected session has no expiry deadline, because a
//! connected session cannot expire; a restored one is not connected,
//! because whatever stopped the broker disconnected it, so restore
//! gives it one counted from the restore. [`check`] states the value it
//! must be given rather than skipping the field — a session left
//! without a deadline is a session that can never expire, which is the
//! bug this accounts for and must not hide.
//!
//! # What this test can produce
//!
//! Counted by name in [`the_generator_produces_every_case_the_property_needs`],
//! which fails on a zero. Convention 5 records a differential suite that
//! could not generate a bare multi-level wildcard and so let a deleted
//! guard survive twenty thousand cases, and the storage crate's first
//! generator produced none of the four cases it needed.
//!
//! - A takeover: a second connection for an identity that has a live one.
//! - A clean start over a session that exists, which destroys it.
//! - A session that expires while nothing is attached.
//! - A queue that fills while a session is away and drains when it comes
//!   back, which is the restart-shaped gap.
//! - Two sessions holding one message, so a body carries two references.
//! - Every step of both acknowledged handshakes, including a receipt that
//!   moves an entry to awaiting-complete and a resume that resends a
//!   release rather than a publish.
//! - A queue at its bound dropping a message, and a message that expires
//!   while waiting in one.
//! - A retained message set, replaced, cleared by an empty payload, and
//!   reclaimed by the expiry sweep.
//! - A will published by a takeover, by a keep-alive timeout and by an
//!   expiry, and a will cancelled by a graceful disconnect.
//! - Shared subscriptions, subscription identifiers and every
//!   retain-handling option.
//! - A peer that comes back advertising a narrower receive maximum than
//!   the one its last connection ran under, which strands a redelivery
//!   in the queue in front of a message numbered below it.
//!
//! # What it cannot
//!
//! - **The subscription trie and the shared-subscription table.**
//!   [`Broker::durable_state`] reads a session's subscriptions from the
//!   session, which is where the records come from, so a rebuild that
//!   put nothing back into the matching structures would still compare
//!   equal. What the records *carry* is what this test guards; that
//!   restore re-enters them into both is proved in `restore`'s own
//!   tests, by publishing to a restored filter, and
//!   `tests/differential.rs` already guards the trie itself.
//! - **Packet identifiers.** They are not durable state — the store
//!   holds the identifier each inflight entry was sent under, and that
//!   is compared — but the allocator's reserved set is not compared.
//!   That restore takes both the window's identifiers and the queue's
//!   out of circulation is `outbound`'s own tests.
//! - **A case it produces twice in three hundred histories.** The
//!   stranded redelivery above is generated, and rarely: with the fix
//!   removed the property found it at case 290 and the budget is 256.
//!   It is written out as
//!   [`a_redelivery_stranded_by_a_narrowed_window_is_stored_in_delivery_order`]
//!   so that it is reached on purpose rather than on a seed, because
//!   this project does not check proptest's regression files in.
//! - **A mapping that is wrong the same way in both directions.**
//!   Convention 5's first example: a quality of service level, a
//!   retain-handling option or an exchange state translated to storage
//!   and back through transposed arms round trips perfectly here and is
//!   wrong against every file a released build wrote. Measured, not
//!   assumed: transposing awaiting-received and awaiting-complete in
//!   `journal` leaves this property passing on five hundred cases. What
//!   catches it is the one-directional assertions in `journal`'s own
//!   tests, which pin each arm to a named variant.
//! - **Durability.** Every store here is in memory. When a write reached
//!   a device is `storage`'s `tests/redb.rs`, in a child process, which
//!   is the only shape of test that can tell the difference.
//! - **A wide universe of names.** Clients, topics and filters come from
//!   small fixed sets, chosen so that sequences collide on the same
//!   session often rather than by luck. Arbitrary Unicode and deep
//!   hierarchies are the codec's problem and are tested there.

use std::collections::{BTreeMap, BTreeSet};

use broker_core::broker::{Broker, BrokerConfig};
use broker_core::journal;
use broker_core::registry::SessionId;
use broker_core::restore::restore;
use broker_core::subscriptions::SubscriberId;
use bytes::Bytes;
use mqtt_codec::packet::{
    Ack, Connect, Disconnect, Publish, RetainHandling, Subscribe, SubscriptionOptions, Unsubscribe,
    Will,
};
use mqtt_codec::{Packet, Properties, QoS, ReasonCode, Version};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{TestCaseResult, TestRunner};
use storage::record::{InflightState, Record, SessionKey};
use storage::{MemoryStorage, Restored, Storage};

// ---- the universe the generator draws from ------------------------------

/// Three, which is the smallest number that gives a shared subscription
/// two members and still leaves somebody publishing.
const CLIENTS: [&str; 3] = ["one", "two", "three"];

/// Topics chosen so that the filters below overlap on them: `a/b` is
/// matched by four of the five, which is what makes two sessions hold one
/// message a common event rather than a rare one.
const TOPICS: [&str; 4] = ["a/b", "a/c", "a/b/c", "x/y"];

/// Filters chosen for overlap, for a shared group, and for the
/// multi-level wildcard that convention 5 records a suite being unable to
/// produce.
const FILTERS: [&str; 6] = ["a/b", "a/+", "a/#", "#", "$share/g/a/+", "x/y"];

/// A small window and a small queue, so that the window fills, the queue
/// fills behind it and both bounds are reached by ordinary traffic rather
/// than by a generator aiming at them.
fn config() -> BrokerConfig {
    BrokerConfig {
        max_inflight: 2,
        max_queue_messages: 3,
        max_queue_bytes: 64 * 1024,
        ..BrokerConfig::default()
    }
}

// ---- what a client may do -----------------------------------------------

/// One thing a client does.
///
/// **Every index is resolved against the state that exists at the moment
/// the operation runs**, not against a name chosen when it was
/// generated. `client` on everything but a connect picks from the
/// clients that currently have a connection, `which` on a settle picks
/// from the exchanges that are actually outstanding, and so on.
///
/// That is not a convenience. The storage crate's first generator chose
/// uniformly, had almost every operation rejected, accumulated no state
/// and tested nothing while passing. The first draft of this one aimed
/// at named clients and reached a queue forty times in four thousand
/// operations, which is the same failure one degree milder.
#[derive(Debug, Clone)]
enum Op {
    Connect {
        client: usize,
        clean_start: bool,
        v5: bool,
        expiry: u32,
        keep_alive: u16,
        will: Option<GeneratedWill>,
        /// What the peer says it can receive, which the shell turns
        /// into the session's send window **after** the connect, the
        /// same way `connection.rs` does. Section 4.9: it is not
        /// session state, so a client may come back advertising less
        /// than it did, and a resume that has already run under the
        /// wider window then strands redeliveries in the queue.
        receive_maximum: u16,
    },
    Subscribe {
        client: usize,
        filter: usize,
        qos: u8,
        no_local: bool,
        retain_as_published: bool,
        handling: u8,
        subscription_id: Option<u32>,
    },
    /// Unsubscribe from a filter the session actually holds, picked by
    /// index, or from a generated one when it holds none.
    Unsubscribe {
        client: usize,
        which: usize,
        held: bool,
    },
    Publish {
        client: usize,
        topic: usize,
        qos: u8,
        retain: bool,
        empty: bool,
        expiry: Option<u32>,
    },
    /// Take the next step of whichever outbound exchange is outstanding,
    /// picked by index among the ones that are.
    Settle {
        client: usize,
        which: usize,
        fail: bool,
    },
    /// Release an exactly-once identifier the broker is holding, picked
    /// by index among the ones it holds.
    Release {
        client: usize,
        which: usize,
    },
    Disconnect {
        client: usize,
        graceful: bool,
    },
    /// The connection went away without saying anything.
    Vanish {
        client: usize,
    },
    Tick {
        seconds: u32,
    },
}

#[derive(Debug, Clone)]
struct GeneratedWill {
    topic: usize,
    qos: u8,
    retain: bool,
    delay: u32,
}

fn any_will() -> impl Strategy<Value = GeneratedWill> {
    (
        0..TOPICS.len(),
        0u8..3,
        any::<bool>(),
        prop_oneof![3 => Just(0u32), 2 => 1u32..3],
    )
        .prop_map(|(topic, qos, retain, delay)| GeneratedWill {
            topic,
            qos,
            retain,
            delay,
        })
}

fn any_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Weighted towards connecting and publishing, because the states
        // worth reaching are downstream of traffic: a queue that never
        // fills exercises no bound, and a session that never returns
        // exercises no resume.
        4 => (
            0..CLIENTS.len(),
            prop::bool::weighted(0.25),
            prop::bool::weighted(0.7),
            prop_oneof![2 => Just(0u32), 3 => 4u32..12, 2 => Just(u32::MAX)],
            prop_oneof![4 => Just(0u16), 1 => 2u16..4],
            prop_oneof![2 => Just(None), 1 => any_will().prop_map(Some)],
            // One is as narrow as a window goes and is the value that
            // strands a redelivery, so it is worth a third of the
            // connects rather than a third of a wide range.
            prop_oneof![1 => Just(1u16), 1 => Just(2u16), 1 => Just(u16::MAX)],
        )
            .prop_map(
                |(client, clean_start, v5, expiry, keep_alive, will, receive_maximum)| {
                    Op::Connect {
                        client,
                        clean_start,
                        v5,
                        expiry,
                        keep_alive,
                        will,
                        receive_maximum,
                    }
                }
            ),
        6 => (
            0..CLIENTS.len(),
            0..FILTERS.len(),
            any_qos(),
            prop::bool::weighted(0.15),
            any::<bool>(),
            0u8..3,
            prop_oneof![2 => Just(None), 1 => (1u32..4).prop_map(Some)],
        )
            .prop_map(
                |(client, filter, qos, no_local, retain_as_published, handling, subscription_id)| {
                    Op::Subscribe {
                        client,
                        filter,
                        qos,
                        no_local,
                        retain_as_published,
                        handling,
                        subscription_id,
                    }
                }
            ),
        1 => (0..CLIENTS.len(), 0usize..6, prop::bool::weighted(0.8))
            .prop_map(|(client, which, held)| Op::Unsubscribe { client, which, held }),
        10 => (
            0..CLIENTS.len(),
            0..TOPICS.len(),
            any_qos(),
            prop::bool::weighted(0.25),
            prop::bool::weighted(0.15),
            prop_oneof![2 => Just(None), 2 => (0u32..2).prop_map(Some)],
        )
            .prop_map(|(client, topic, qos, retain, empty, expiry)| Op::Publish {
                client,
                topic,
                qos,
                retain,
                empty,
                expiry,
            }),
        4 => (0..CLIENTS.len(), 0usize..4, prop::bool::weighted(0.15))
            .prop_map(|(client, which, fail)| Op::Settle { client, which, fail }),
        3 => (0..CLIENTS.len(), 0usize..4).prop_map(|(client, which)| Op::Release { client, which }),
        1 => (0..CLIENTS.len(), prop::bool::weighted(0.6))
            .prop_map(|(client, graceful)| Op::Disconnect { client, graceful }),
        2 => (0..CLIENTS.len()).prop_map(|client| Op::Vanish { client }),
        3 => (1u32..4).prop_map(|seconds| Op::Tick { seconds }),
    ]
}

/// Skewed above at-most-once, because a fire-and-forget delivery to an
/// attached client is journalled nowhere by design, so a uniform choice
/// would spend a third of the traffic on nothing this test can see.
fn any_qos() -> impl Strategy<Value = u8> {
    prop_oneof![1 => Just(0u8), 2 => Just(1u8), 2 => Just(2u8)]
}

fn any_history() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(any_op(), 1..48)
}

// ---- the cases the property would be worthless without ------------------

#[derive(Debug, Default, Clone, Copy)]
struct Coverage {
    /// A connect that displaced a live connection for the same identity.
    takeover: u64,
    /// A clean start over a session that already existed.
    clean_start_over_an_existing_session: u64,
    /// A session destroyed by its expiry passing.
    session_expired: u64,
    /// A queue that held messages while nothing was attached and then
    /// drained on the reconnection: the restart-shaped gap.
    queue_drained_after_a_gap: u64,
    /// One publication queued or outstanding for two sessions at once,
    /// so one body carries two references.
    two_sessions_holding_one_message: u64,
    /// An exactly-once entry that reached awaiting-complete, which is
    /// resumed by resending the release rather than the publish.
    an_entry_awaiting_completion: u64,
    /// A resume that put an outstanding entry back in the queue.
    a_resume_that_requeued: u64,
    /// A redelivery still waiting in the queue once the connection was
    /// fully established, with a message behind it. It happens when the
    /// peer comes back advertising a narrower window than the one the
    /// resume ran under, and it is the case that makes the stored
    /// queue's key order disagree with the order the session will
    /// deliver in: the redelivery carries the larger number and is
    /// delivered first.
    a_redelivery_left_waiting_in_the_queue: u64,
    /// An exactly-once identifier the broker held and the peer released.
    an_inbound_identifier_released: u64,
    /// A message discarded because the queue was at a bound.
    a_queue_bound_reached: u64,
    /// A message that expired while it was waiting.
    a_message_expired_in_a_queue: u64,
    /// A retained message stored.
    a_retained_message_set: u64,
    /// A retained message cleared by an empty payload.
    a_retained_message_cleared: u64,
    /// A retained message reclaimed by the expiry sweep.
    a_retained_message_swept: u64,
    /// A will armed and published.
    a_will_published: u64,
    /// A will that was owed and then cancelled, by a graceful departure
    /// or by the client coming back before the delay elapsed.
    a_will_cancelled: u64,
    /// A subscription carrying an identifier to echo.
    a_subscription_with_an_identifier: u64,
    /// A shared subscription.
    a_shared_subscription: u64,
    /// A session that survived the whole history holding a queue.
    a_session_holding_a_queue_at_the_end: u64,
}

impl Coverage {
    fn add(&mut self, other: Self) {
        for (slot, value) in self.each_mut().into_iter().zip(other.each()) {
            *slot += value;
        }
    }

    fn each(&self) -> [u64; 19] {
        [
            self.takeover,
            self.clean_start_over_an_existing_session,
            self.session_expired,
            self.queue_drained_after_a_gap,
            self.two_sessions_holding_one_message,
            self.an_entry_awaiting_completion,
            self.a_resume_that_requeued,
            self.a_redelivery_left_waiting_in_the_queue,
            self.an_inbound_identifier_released,
            self.a_queue_bound_reached,
            self.a_message_expired_in_a_queue,
            self.a_retained_message_set,
            self.a_retained_message_cleared,
            self.a_retained_message_swept,
            self.a_will_published,
            self.a_will_cancelled,
            self.a_subscription_with_an_identifier,
            self.a_shared_subscription,
            self.a_session_holding_a_queue_at_the_end,
        ]
    }

    fn each_mut(&mut self) -> [&mut u64; 19] {
        [
            &mut self.takeover,
            &mut self.clean_start_over_an_existing_session,
            &mut self.session_expired,
            &mut self.queue_drained_after_a_gap,
            &mut self.two_sessions_holding_one_message,
            &mut self.an_entry_awaiting_completion,
            &mut self.a_resume_that_requeued,
            &mut self.a_redelivery_left_waiting_in_the_queue,
            &mut self.an_inbound_identifier_released,
            &mut self.a_queue_bound_reached,
            &mut self.a_message_expired_in_a_queue,
            &mut self.a_retained_message_set,
            &mut self.a_retained_message_cleared,
            &mut self.a_retained_message_swept,
            &mut self.a_will_published,
            &mut self.a_will_cancelled,
            &mut self.a_subscription_with_an_identifier,
            &mut self.a_shared_subscription,
            &mut self.a_session_holding_a_queue_at_the_end,
        ]
    }

    const NAMES: [&'static str; 19] = [
        "a takeover",
        "a clean start over an existing session",
        "a session that expires",
        "a queue that drains after a restart-shaped gap",
        "two sessions holding one message",
        "an exactly-once entry awaiting completion",
        "a resume that requeued an outstanding entry",
        "a redelivery left waiting ahead of a queued message",
        "an inbound exactly-once identifier released",
        "a queue bound reached",
        "a message that expired while queued",
        "a retained message set",
        "a retained message cleared",
        "a retained message reclaimed by the sweep",
        "a will published",
        "a will cancelled",
        "a subscription carrying an identifier",
        "a shared subscription",
        "a session still holding a queue at the end",
    ];

    fn named(&self) -> Vec<(&'static str, u64)> {
        Self::NAMES.into_iter().zip(self.each()).collect()
    }
}

// ---- driving one history ------------------------------------------------

/// What the driver remembers about one client between operations.
struct ClientState {
    /// The next packet identifier it will publish under. Wraps over a
    /// small range so that identifiers are reused once the exchanges
    /// they belonged to have finished, which is what a real client does
    /// and what reaches the redelivery path.
    next_packet_id: u16,
    /// Whether its queue was non-empty while nothing was attached, so
    /// that a later drain can be recognised as the restart-shaped gap.
    queued_while_away: bool,
}

#[derive(Debug, Default, Clone, Copy)]
struct Stats {
    subscribes: u64,
    publishes: u64,
    queue_pushed: u64,
    records: u64,
    delivered: u64,
}

struct Run {
    stats: Stats,
    broker: Broker,
    store: MemoryStorage,
    clients: Vec<ClientState>,
    coverage: Coverage,
    /// Reused so that driving a history allocates little.
    actions: Vec<(SubscriberId, broker_core::session::Action)>,
    records: Vec<Record>,
}

impl Run {
    fn new() -> Self {
        Self {
            stats: Stats::default(),
            broker: Broker::new(config()),
            store: MemoryStorage::new(),
            clients: (0..CLIENTS.len())
                .map(|_| ClientState {
                    next_packet_id: 1,
                    queued_while_away: false,
                })
                .collect(),
            coverage: Coverage::default(),
            actions: Vec::new(),
            records: Vec::new(),
        }
    }

    fn handle_of(&self, client: usize) -> Option<SubscriberId> {
        self.broker
            .subscriber_of(&SessionId::new("", CLIENTS[client]))
    }

    /// Whether this client has a session with a connection attached.
    fn attached(&self, client: usize) -> Option<SubscriberId> {
        let handle = self.handle_of(client)?;
        let session = self.broker.session(handle)?;
        session.is_connected().then_some(handle)
    }

    /// Every client that currently has a connection.
    fn attached_clients(&self) -> Vec<usize> {
        (0..CLIENTS.len())
            .filter(|client| self.attached(*client).is_some())
            .collect()
    }

    /// One of them, chosen by an index that means "whichever is the
    /// `n`th" rather than "the one called `n`".
    fn pick_attached(&self, index: usize) -> Option<(usize, SubscriberId)> {
        let attached = self.attached_clients();
        if attached.is_empty() {
            return None;
        }
        let client = attached[index % attached.len()];
        Some((client, self.attached(client)?))
    }

    /// Hand the journal to the store, exactly as the shell would, and
    /// refuse to go on if the store would not take it.
    fn commit(&mut self) -> Result<(), String> {
        self.records.clear();
        self.broker.take_journal(&mut self.records);
        if self.broker.sessions_holding_records() != 0 {
            return Err(
                "a session is still holding records that nothing drained, so the shell would \
                 acknowledge before committing them"
                    .to_string(),
            );
        }
        self.stats.records += self.records.len() as u64;
        for record in &self.records {
            match record {
                Record::QueuePushed { .. } => self.stats.queue_pushed += 1,
                Record::InflightPut { .. } => self.stats.delivered += 1,
                _ => {}
            }
        }
        self.store
            .apply(&self.records)
            .map_err(|error| format!("the store refused the journal: {error}\n{:?}", self.records))
    }

    fn note_queue_state(&mut self) {
        for client in 0..CLIENTS.len() {
            let Some(handle) = self.handle_of(client) else {
                continue;
            };
            let Some(session) = self.broker.session(handle) else {
                continue;
            };
            if !session.is_connected() && session.outbound.queued_len() > 0 {
                self.clients[client].queued_while_away = true;
            }
        }
    }

    fn apply(&mut self, op: &Op) -> Result<(), String> {
        self.actions.clear();
        match op {
            Op::Connect {
                client,
                clean_start,
                v5,
                expiry,
                keep_alive,
                will,
                receive_maximum,
            } => {
                let existed = self.handle_of(*client).is_some();
                let was_attached = self.attached(*client).is_some();
                let owed_a_will = self
                    .handle_of(*client)
                    .and_then(|handle| self.broker.session(handle))
                    .is_some_and(|session| session.will_at().is_some());
                let outstanding = self
                    .handle_of(*client)
                    .and_then(|handle| self.broker.session(handle))
                    .map_or(0, |session| session.outbound.inflight_len());
                let queued_away = self.clients[*client].queued_while_away;
                let queued_count = self
                    .handle_of(*client)
                    .and_then(|handle| self.broker.session(handle))
                    .map_or(0, |session| session.outbound.queued_len());

                if was_attached {
                    self.coverage.takeover += 1;
                }
                if existed && *clean_start {
                    self.coverage.clean_start_over_an_existing_session += 1;
                }
                if !clean_start && outstanding > 0 {
                    self.coverage.a_resume_that_requeued += 1;
                }

                let connect = Connect {
                    version: if *v5 { Version::V5 } else { Version::V311 },
                    client_id: CLIENTS[*client].to_string(),
                    clean_start: *clean_start,
                    keep_alive: *keep_alive,
                    username: None,
                    password: None,
                    will: will.as_ref().map(|will| Will {
                        topic: TOPICS[will.topic].to_string(),
                        payload: Bytes::from_static(b"gone"),
                        qos: qos_of(will.qos),
                        retain: will.retain,
                        properties: Properties {
                            will_delay_interval: (will.delay > 0).then_some(will.delay),
                            ..Properties::default()
                        },
                    }),
                    properties: Properties {
                        session_expiry_interval: v5.then_some(*expiry),
                        ..Properties::default()
                    },
                };
                let mut actions = std::mem::take(&mut self.actions);
                let registration = self.broker.connect("", &connect, &mut actions);
                self.actions = actions;

                // Exactly what `connection.rs` does, and in the same
                // place: after the connect, so the resume the connect
                // just ran used the window the *last* connection left
                // behind. That ordering is the whole reason a
                // redelivery can be stranded.
                let configured = usize::from(config().max_inflight);
                if let Some(session) = self.broker.session_mut(registration.subscriber) {
                    session
                        .outbound
                        .set_inflight_limit(usize::from(*receive_maximum).min(configured));
                    // A redelivery stranded in the queue **with
                    // something behind it**, which is the whole of the
                    // condition: a stranded redelivery alone is in
                    // order with itself, and only a message queued
                    // earlier sitting behind it can be overtaken.
                    let waiting: Vec<bool> =
                        session.outbound.waiting().map(|item| item.dup).collect();
                    if waiting.iter().rev().skip(1).any(|dup| *dup) {
                        self.coverage.a_redelivery_left_waiting_in_the_queue += 1;
                    }
                }

                if owed_a_will && !clean_start {
                    let still_owed = self
                        .handle_of(*client)
                        .and_then(|handle| self.broker.session(handle))
                        .is_some_and(|session| session.will_at().is_some());
                    if !still_owed {
                        self.coverage.a_will_cancelled += 1;
                    }
                }
                if queued_away && !clean_start {
                    let left = self
                        .handle_of(*client)
                        .and_then(|handle| self.broker.session(handle))
                        .map_or(0, |session| session.outbound.queued_len());
                    // Fewer waiting than were waiting, which is the
                    // resume having taken some of them out to the wire.
                    // Not "none left": the window is two and the queue
                    // three, so a full queue drains partly and a test
                    // demanding empty would be counting a coincidence.
                    if left < queued_count {
                        self.coverage.queue_drained_after_a_gap += 1;
                        self.clients[*client].queued_while_away = false;
                    }
                }
            }

            Op::Subscribe {
                client,
                filter,
                qos,
                no_local,
                retain_as_published,
                handling,
                subscription_id,
            } => {
                let Some((_, handle)) = self.pick_attached(*client) else {
                    return Ok(());
                };
                if subscription_id.is_some() {
                    self.coverage.a_subscription_with_an_identifier += 1;
                }
                self.stats.subscribes += 1;
                if FILTERS[*filter].starts_with("$share/") {
                    self.coverage.a_shared_subscription += 1;
                }
                let packet = Packet::Subscribe(Subscribe {
                    packet_id: 1,
                    filters: vec![SubscriptionOptions {
                        filter: FILTERS[*filter].to_string(),
                        qos: qos_of(*qos),
                        no_local: *no_local,
                        retain_as_published: *retain_as_published,
                        retain_handling: handling_of(*handling),
                    }],
                    properties: Properties {
                        subscription_identifiers: subscription_id.iter().copied().collect(),
                        ..Properties::default()
                    },
                });
                let mut actions = std::mem::take(&mut self.actions);
                let _ = self.broker.handle(handle, &packet, &mut actions);
                self.actions = actions;
            }

            Op::Unsubscribe {
                client,
                which,
                held,
            } => {
                let Some((_, handle)) = self.pick_attached(*client) else {
                    return Ok(());
                };
                let filters: Vec<String> = self
                    .broker
                    .session(handle)
                    .map(|session| session.filters().map(str::to_string).collect())
                    .unwrap_or_default();
                let filter = match (held, filters.is_empty()) {
                    (true, false) => filters[which % filters.len()].clone(),
                    _ => FILTERS[which % FILTERS.len()].to_string(),
                };
                let packet = Packet::Unsubscribe(Unsubscribe {
                    packet_id: 1,
                    filters: vec![filter],
                    properties: Properties::default(),
                });
                let mut actions = std::mem::take(&mut self.actions);
                let _ = self.broker.handle(handle, &packet, &mut actions);
                self.actions = actions;
            }

            Op::Publish {
                client,
                topic,
                qos,
                retain,
                empty,
                expiry,
            } => {
                let Some((client, handle)) = self.pick_attached(*client) else {
                    return Ok(());
                };
                let client = &client;
                let qos = qos_of(*qos);
                let packet_id = (qos != QoS::AtMostOnce).then(|| {
                    let id = self.clients[*client].next_packet_id;
                    // A small range, so identifiers come round again and
                    // a redelivery of one still held is reachable.
                    self.clients[*client].next_packet_id = if id >= 6 { 1 } else { id + 1 };
                    id
                });
                let payload = if *empty {
                    Bytes::new()
                } else {
                    Bytes::from_static(b"payload")
                };
                if *retain {
                    if payload.is_empty() {
                        self.coverage.a_retained_message_cleared += 1;
                    } else {
                        self.coverage.a_retained_message_set += 1;
                    }
                }
                let packet = Packet::Publish(Publish {
                    dup: false,
                    qos,
                    retain: *retain,
                    topic: TOPICS[*topic].to_string(),
                    packet_id,
                    payload,
                    properties: Properties {
                        message_expiry_interval: *expiry,
                        ..Properties::default()
                    },
                });
                self.stats.publishes += 1;
                let before = self.holders();
                let mut actions = std::mem::take(&mut self.actions);
                let _ = self.broker.handle(handle, &packet, &mut actions);
                self.actions = actions;
                let after = self.holders();
                for (id, count) in &after {
                    if *count >= 2 && before.get(id).copied().unwrap_or(0) < 2 {
                        self.coverage.two_sessions_holding_one_message += 1;
                    }
                }
                self.count_drops();
            }

            Op::Settle {
                client,
                which,
                fail,
            } => {
                let busy: Vec<usize> = self
                    .attached_clients()
                    .into_iter()
                    .filter(|candidate| {
                        self.attached(*candidate)
                            .and_then(|handle| self.broker.session(handle))
                            .is_some_and(|session| session.outbound.inflight_len() > 0)
                    })
                    .collect();
                if busy.is_empty() {
                    return Ok(());
                }
                let handle = self
                    .attached(busy[client % busy.len()])
                    .expect("it was attached a moment ago");
                let Some(session) = self.broker.session(handle) else {
                    return Ok(());
                };
                let outstanding: Vec<(u16, InflightState)> = session
                    .outbound
                    .outstanding()
                    .map(|entry| (entry.packet_id, journal::stored_inflight_state(entry.state)))
                    .collect();
                if outstanding.is_empty() {
                    return Ok(());
                }
                let (packet_id, state) = outstanding[which % outstanding.len()];
                let reason = if *fail {
                    ReasonCode::UnspecifiedError
                } else {
                    ReasonCode::Success
                };
                let packet = match state {
                    InflightState::AwaitingAck => Packet::PubAck(ack(packet_id, reason)),
                    InflightState::AwaitingReceived => Packet::PubRec(ack(packet_id, reason)),
                    InflightState::AwaitingComplete => {
                        Packet::PubComp(ack(packet_id, ReasonCode::Success))
                    }
                };
                let mut actions = std::mem::take(&mut self.actions);
                let _ = self.broker.handle(handle, &packet, &mut actions);
                self.actions = actions;
                if state == InflightState::AwaitingReceived && !fail {
                    self.coverage.an_entry_awaiting_completion += 1;
                }
                self.count_drops();
            }

            Op::Release { client, which } => {
                let holding: Vec<usize> = self
                    .attached_clients()
                    .into_iter()
                    .filter(|candidate| {
                        self.attached(*candidate)
                            .and_then(|handle| self.broker.session(handle))
                            .is_some_and(|session| session.inbound.outstanding() > 0)
                    })
                    .collect();
                if holding.is_empty() {
                    return Ok(());
                }
                let handle = self
                    .attached(holding[client % holding.len()])
                    .expect("it was attached a moment ago");
                let Some(session) = self.broker.session(handle) else {
                    return Ok(());
                };
                let held: Vec<u16> = session.inbound.held().collect();
                if held.is_empty() {
                    return Ok(());
                }
                let packet_id = held[which % held.len()];
                self.coverage.an_inbound_identifier_released += 1;
                let packet = Packet::PubRel(ack(packet_id, ReasonCode::Success));
                let mut actions = std::mem::take(&mut self.actions);
                let _ = self.broker.handle(handle, &packet, &mut actions);
                self.actions = actions;
            }

            Op::Disconnect { client, graceful } => {
                let Some((_, handle)) = self.pick_attached(*client) else {
                    return Ok(());
                };
                let had_a_will = self
                    .broker
                    .session(handle)
                    .is_some_and(|session| session.will().is_some());
                let packet = Packet::Disconnect(Disconnect {
                    reason: if *graceful {
                        ReasonCode::Success
                    } else {
                        ReasonCode::DisconnectWithWillMessage
                    },
                    properties: Properties::default(),
                });
                let mut actions = std::mem::take(&mut self.actions);
                let _ = self.broker.handle(handle, &packet, &mut actions);
                self.actions = actions;
                if had_a_will
                    && self
                        .broker
                        .session(handle)
                        .is_some_and(|session| session.will().is_none())
                {
                    self.coverage.a_will_cancelled += 1;
                }
            }

            Op::Vanish { client } => {
                let Some((_, handle)) = self.pick_attached(*client) else {
                    return Ok(());
                };
                let mut actions = std::mem::take(&mut self.actions);
                self.broker.disconnect(handle, &mut actions);
                self.actions = actions;
            }

            Op::Tick { seconds } => {
                for _ in 0..*seconds {
                    let alive: BTreeSet<String> = self.live_sessions();
                    let retained_before = self.broker.retained_count();
                    let owed: usize = self.wills_owed();

                    let mut actions = std::mem::take(&mut self.actions);
                    self.broker.tick(&mut actions);
                    self.actions = actions;

                    let after = self.live_sessions();
                    self.coverage.session_expired += (alive.difference(&after).count()) as u64;
                    if self.broker.retained_count() < retained_before {
                        self.coverage.a_retained_message_swept += 1;
                    }
                    let still_owed = self.wills_owed();
                    if still_owed < owed {
                        self.coverage.a_will_published += (owed - still_owed) as u64;
                    }
                    self.count_drops();
                }
            }
        }

        self.count_drops();
        self.note_queue_state();
        self.commit()
    }

    /// How many sessions hold each message, queued or outstanding.
    fn holders(&self) -> BTreeMap<broker_core::session::MessageId, usize> {
        let mut counts = BTreeMap::new();
        for client in 0..CLIENTS.len() {
            let Some(handle) = self.handle_of(client) else {
                continue;
            };
            let Some(session) = self.broker.session(handle) else {
                continue;
            };
            for id in session.outbound.pending_ids() {
                *counts.entry(id).or_insert(0) += 1;
            }
        }
        counts
    }

    fn live_sessions(&self) -> BTreeSet<String> {
        (0..CLIENTS.len())
            .filter(|client| self.handle_of(*client).is_some())
            .map(|client| CLIENTS[client].to_string())
            .collect()
    }

    fn wills_owed(&self) -> usize {
        (0..CLIENTS.len())
            .filter_map(|client| self.handle_of(client))
            .filter_map(|handle| self.broker.session(handle))
            .filter(|session| session.will_at().is_some())
            .count()
    }

    fn count_drops(&mut self) {
        for (_, action) in &self.actions {
            if let broker_core::session::Action::Dropped { reason, .. } = action {
                match reason {
                    broker_core::session::DropReason::QueueFull
                    | broker_core::session::DropReason::ByteBudget => {
                        self.coverage.a_queue_bound_reached += 1;
                    }
                    broker_core::session::DropReason::Expired => {
                        self.coverage.a_message_expired_in_a_queue += 1;
                    }
                    _ => {}
                }
            }
        }
        self.actions.clear();
    }
}

fn qos_of(value: u8) -> QoS {
    match value % 3 {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

fn handling_of(value: u8) -> RetainHandling {
    match value % 3 {
        0 => RetainHandling::SendAlways,
        1 => RetainHandling::SendIfNew,
        _ => RetainHandling::DoNotSend,
    }
}

fn ack(packet_id: u16, reason: ReasonCode) -> Ack {
    Ack {
        packet_id,
        reason,
        properties: Properties::default(),
    }
}

// ---- rebuilding a broker from what the store gives back -----------------

/// Build a second broker holding exactly what the store says the first
/// one had.
///
/// [`broker_core::restore`] is the real thing, and using it is the
/// point: a copy of the restore path living in this file would be a
/// second implementation of section 7.5 that this property compared
/// against the first, and the two would agree with each other while
/// both disagreed with what the shell actually runs.
fn rebuild(restored: &Restored, now: u32) -> Broker {
    let mut broker = Broker::new(config());
    let report = restore(&mut broker, restored, now);
    assert_eq!(
        report.filters_refused, 0,
        "every filter the broker accepted once it accepts again"
    );
    assert_eq!(
        report.identifier_clashes, 0,
        "the stored state must not claim one identifier twice"
    );
    broker
}

/// Every body the store holds must be referenced, and every reference
/// must find its body.
///
/// The equality below cannot see this on its own: both brokers derive
/// their body map from the entries they hold, so a body leaked by a
/// missing release is invisible there and is visible here.
fn bodies_are_exactly_what_is_referenced(restored: &Restored) -> Result<(), String> {
    let mut referenced = BTreeSet::new();
    for session in &restored.sessions {
        for entry in &session.queue {
            referenced.insert(entry.delivery.body);
        }
        for entry in &session.inflight {
            referenced.insert(entry.delivery.body);
        }
    }
    let held: BTreeSet<_> = restored.bodies.keys().copied().collect();
    if held != referenced {
        return Err(format!(
            "the store holds bodies nothing references, or references bodies it does not hold: \
             held {held:?}, referenced {referenced:?}"
        ));
    }
    Ok(())
}

fn drive(history: &[Op]) -> Result<(Run, Restored), String> {
    let mut run = Run::new();
    for (index, op) in history.iter().enumerate() {
        run.apply(op)
            .map_err(|why| format!("operation {index} ({op:?}): {why}"))?;
    }
    for client in 0..CLIENTS.len() {
        if let Some(handle) = run.handle_of(client) {
            if run
                .broker
                .session(handle)
                .is_some_and(|session| session.outbound.queued_len() > 0)
            {
                run.coverage.a_session_holding_a_queue_at_the_end += 1;
            }
        }
    }
    let restored = run.store.restore().map_err(|error| error.to_string())?;
    Ok((run, restored))
}

fn check(history: &[Op]) -> TestCaseResult {
    let (run, restored) =
        drive(history).map_err(|why| TestCaseError::fail(format!("driving failed: {why}")))?;

    bodies_are_exactly_what_is_referenced(&restored).map_err(TestCaseError::fail)?;

    // The maintained queue total, against the same number the slow
    // way. It exists because summing every session's queue under the
    // broker's one lock is too much to hold while a scraper asks, and
    // a figure maintained at each of a queue's four mutation points
    // is a figure that drifts the first time a path forgets one. This
    // is where the drift would show: hundreds of generated histories,
    // each pushing, popping, evicting, requeueing, expiring, taking
    // over and clean-starting, and none of them written by whoever
    // wrote the four increments.
    if run.broker.queue_depth() != run.broker.queue_depth_by_walking() {
        return Err(TestCaseError::fail(format!(
            "the maintained queue total says {} and the sessions hold {}",
            run.broker.queue_depth(),
            run.broker.queue_depth_by_walking()
        )));
    }

    let now = run.broker.clock().now();
    let rebuilt = rebuild(&restored, now);
    let mut live = run.broker.durable_state();
    // The one durable difference the two brokers are meant to have; see
    // the note at the top of this file. The expected deadline is
    // restated from the rule rather than read back from the broker,
    // because a comparison that asked the implementation what it does
    // could not see it do the wrong thing.
    for (client, name) in CLIENTS.iter().enumerate() {
        if run.attached(client).is_none() {
            continue;
        }
        let Some(session) = live.sessions.get_mut(&SessionKey::new("", name)) else {
            continue;
        };
        let interval = session.session.expiry_interval;
        session.session.expires_at = Some(if interval == 0 {
            // At once. A deadline of zero seconds is not rounded.
            now
        } else {
            // Rounded up, because the clock is whole seconds and only
            // erring early is wrong.
            now.saturating_add(interval).saturating_add(1)
        });
    }
    let restored_state = rebuilt.durable_state();

    if live != restored_state {
        // Named rather than dumped whole: a whole-state diff of two
        // brokers is unreadable, and the first difference is the one
        // that matters.
        for (key, session) in &live.sessions {
            match restored_state.sessions.get(key) {
                None => {
                    return Err(TestCaseError::fail(format!(
                        "session {key:?} did not survive: {session:?}"
                    )))
                }
                Some(other) if other != session => {
                    if other.session != session.session {
                        return Err(TestCaseError::fail(format!(
                            "session {key:?}'s own fields changed: live {:?}, restored {:?}",
                            session.session, other.session
                        )));
                    }
                    if other.subscriptions != session.subscriptions {
                        return Err(TestCaseError::fail(format!(
                            "session {key:?}'s subscriptions changed: live {:?}, restored {:?}",
                            session.subscriptions, other.subscriptions
                        )));
                    }
                    if other.inbound != session.inbound {
                        return Err(TestCaseError::fail(format!(
                            "session {key:?}'s inbound identifiers changed: live {:?}, restored \
                             {:?}",
                            session.inbound, other.inbound
                        )));
                    }
                    if other.inflight != session.inflight {
                        return Err(TestCaseError::fail(format!(
                            "session {key:?}'s window changed: live {:?}, restored {:?}",
                            session.inflight, other.inflight
                        )));
                    }
                    if other.queue != session.queue {
                        return Err(TestCaseError::fail(format!(
                            "session {key:?}'s queue changed: live {:?}, restored {:?}",
                            session.queue, other.queue
                        )));
                    }
                    return Err(TestCaseError::fail(format!(
                        "session {key:?}'s high-water mark changed: live {}, restored {}",
                        session.highest_sequence, other.highest_sequence
                    )));
                }
                Some(_) => {}
            }
        }
        for key in restored_state.sessions.keys() {
            if !live.sessions.contains_key(key) {
                return Err(TestCaseError::fail(format!(
                    "session {key:?} came back from a store that should not hold it"
                )));
            }
        }
        if live.retained != restored_state.retained {
            return Err(TestCaseError::fail(format!(
                "the retained messages changed: live {:?}, restored {:?}",
                live.retained, restored_state.retained
            )));
        }
        if live.bodies != restored_state.bodies {
            return Err(TestCaseError::fail(format!(
                "the bodies changed: live {:?}, restored {:?}",
                live.bodies.keys().collect::<Vec<_>>(),
                restored_state.bodies.keys().collect::<Vec<_>>()
            )));
        }
        return Err(TestCaseError::fail("the two brokers differ"));
    }

    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// A broker rebuilt from what the broker said is the same broker.
    #[test]
    fn what_the_broker_says_is_enough_to_rebuild_it(history in any_history()) {
        check(&history)?;
    }
}

/// A redelivery stranded ahead of a queued message is still stored in
/// the order it will be delivered in.
///
/// Written out rather than left to the generator, which produces this
/// twice in three hundred and eighty-four histories: the property found
/// it at case 290 of 8,000 when the fix was removed, and 256 is the
/// configured budget. A case a property reaches by luck is one it also
/// misses by luck, and this project does not check proptest's
/// regression files in, so there is nowhere for the seed to live.
///
/// The shape, under a window of two and a queue of three:
///
/// 1. A client subscribes and publishes to itself until two are
///    outstanding and a third is waiting.
/// 2. It reconnects advertising a receive maximum of one. The resume
///    runs first and under the *old* window, because section 4.9 makes
///    the receive maximum a property of the connection and the shell
///    applies it after the connect. So two redeliveries go back to the
///    head of the queue under fresh numbers, and the narrowed pump
///    takes only one of them out again.
/// 3. The one left behind is a redelivery, carrying the largest number
///    in the queue while sitting in front of a message numbered lower.
///    Read back in key order it is delivered second, which is a message
///    the peer has never seen going out ahead of a duplicate it is
///    still holding an identifier for.
#[test]
fn a_redelivery_stranded_by_a_narrowed_window_is_stored_in_delivery_order() {
    fn connect(receive_maximum: u16) -> Op {
        Op::Connect {
            client: 0,
            clean_start: false,
            v5: false,
            expiry: 0,
            keep_alive: 0,
            will: None,
            receive_maximum,
        }
    }
    fn publish() -> Op {
        Op::Publish {
            client: 0,
            topic: 0,
            qos: 1,
            retain: false,
            empty: false,
            expiry: None,
        }
    }

    let history = vec![
        connect(2),
        Op::Subscribe {
            client: 0,
            filter: 1,
            qos: 1,
            no_local: false,
            retain_as_published: false,
            handling: 0,
            subscription_id: None,
        },
        // Two fill the window and a third waits behind it. Nothing
        // acknowledges them, so they stay outstanding.
        publish(),
        publish(),
        publish(),
        // The first reconnection still has room for both redeliveries.
        connect(1),
        // The second does not, and that is the one that strands.
        connect(1),
    ];

    let (run, restored) = drive(&history).expect("the history drives cleanly");
    let handle = run.handle_of(0).expect("the client has a session");
    let session = run.broker.session(handle).expect("it is registered");
    let waiting: Vec<bool> = session.outbound.waiting().map(|item| item.dup).collect();
    assert!(
        waiting.iter().rev().skip(1).any(|dup| *dup),
        "the history must actually strand a redelivery ahead of something, got {waiting:?}"
    );
    assert_eq!(restored.sessions.len(), 1);

    check(&history).expect("the stored order is the delivery order");
}

// ---- sequence numbers, walked through the broker ------------------------

/// Every sequence number a session holds, queued or outstanding.
fn sequences_of(broker: &Broker, client: &str) -> Vec<u64> {
    let state = broker.durable_state();
    let held = state
        .sessions
        .get(&storage::record::SessionKey::new("", client))
        .unwrap_or_else(|| panic!("{client} has a session"));
    let mut numbers: Vec<u64> = held
        .inflight
        .iter()
        .map(|entry| entry.sequence)
        .chain(held.queue.iter().map(|entry| entry.sequence))
        .collect();
    numbers.sort_unstable();
    numbers
}

/// Numbering is per session, proved through the broker rather than
/// against two queues built by hand.
///
/// One publication reaches two subscribers, so the two sessions take the
/// same message at the same instant. Each numbers it one. A counter
/// shared between them — a static, or a field that moved up to the
/// registry — numbers the second two, and nothing else in the suite
/// would say so: the store would take both numbers happily, and the
/// only symptom is a replica in milestone 6 applying two sessions'
/// operations in an order neither of them was in.
#[test]
fn two_sessions_number_their_queues_independently() {
    let history = vec![
        Op::Connect {
            client: 0,
            clean_start: false,
            v5: true,
            expiry: u32::MAX,
            keep_alive: 0,
            will: None,
            receive_maximum: u16::MAX,
        },
        Op::Connect {
            client: 1,
            clean_start: false,
            v5: true,
            expiry: u32::MAX,
            keep_alive: 0,
            will: None,
            receive_maximum: u16::MAX,
        },
        Op::Subscribe {
            client: 0,
            filter: 1,
            qos: 1,
            no_local: false,
            retain_as_published: false,
            handling: 0,
            subscription_id: None,
        },
        Op::Subscribe {
            client: 1,
            filter: 1,
            qos: 1,
            no_local: false,
            retain_as_published: false,
            handling: 0,
            subscription_id: None,
        },
        Op::Publish {
            client: 0,
            topic: 0,
            qos: 1,
            retain: false,
            empty: false,
            expiry: None,
        },
    ];

    let (run, _) = drive(&history).expect("the history drives cleanly");
    assert_eq!(sequences_of(&run.broker, CLIENTS[0]), vec![1]);
    assert_eq!(
        sequences_of(&run.broker, CLIENTS[1]),
        vec![1],
        "the second session's first message is its first"
    );
}

/// A restored session carries on from the number it left, not from one.
///
/// Section 7.4's fourth property, and the one the differential test
/// above cannot reach on its own: it compares two brokers, so a rebuild
/// that dropped the mark and a `durable_state` that read the mark from
/// the same wrong place would agree. This asks what the restored
/// session does *next*, which is the only question a spent number can
/// be wrong about.
///
/// A number handed out twice is not a corruption here — it is a batch
/// the store refuses, and section 7.3 has the shell waiting on that
/// batch before it acknowledges, so it is a publisher told a message
/// was safe and a message that was not.
#[test]
fn a_restored_session_carries_on_from_the_number_it_left() {
    let history = vec![
        Op::Connect {
            client: 0,
            clean_start: false,
            v5: true,
            expiry: u32::MAX,
            keep_alive: 0,
            will: None,
            receive_maximum: u16::MAX,
        },
        Op::Subscribe {
            client: 0,
            filter: 1,
            qos: 1,
            no_local: false,
            retain_as_published: false,
            handling: 0,
            subscription_id: None,
        },
        Op::Publish {
            client: 0,
            topic: 0,
            qos: 1,
            retain: false,
            empty: false,
            expiry: None,
        },
        Op::Publish {
            client: 0,
            topic: 0,
            qos: 1,
            retain: false,
            empty: false,
            expiry: None,
        },
        Op::Publish {
            client: 0,
            topic: 0,
            qos: 1,
            retain: false,
            empty: false,
            expiry: None,
        },
    ];

    let (run, restored) = drive(&history).expect("the history drives cleanly");
    assert_eq!(
        sequences_of(&run.broker, CLIENTS[0]),
        vec![1, 2, 3],
        "two in the window and one waiting behind it"
    );

    let mut rebuilt = rebuild(&restored, run.broker.clock().now());
    let handle = rebuilt
        .subscriber_of(&SessionId::new("", CLIENTS[0]))
        .expect("the session came back");
    let mut actions = Vec::new();
    rebuilt
        .handle(
            handle,
            &Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic: TOPICS[0].to_string(),
                packet_id: Some(1),
                payload: Bytes::from_static(b"after the restart"),
                properties: Properties::default(),
            }),
            &mut actions,
        )
        .expect("a publish the broker accepted before it accepts again");

    assert_eq!(
        sequences_of(&rebuilt, CLIENTS[0]),
        vec![1, 2, 3, 4],
        "the next number after a restart is the next number, not the first"
    );
}

/// The generator can produce every case this test would be worthless
/// without.
///
/// Convention 5: a generator that cannot produce a case is how a property
/// test quietly becomes decorative. The storage crate's first generator
/// produced none of the four cases it needed and passed anyway. This runs
/// the generator with a deterministic runner — the same histories every
/// time, so a failure here is a defect and never a bad seed — and fails
/// naming whichever case came out zero.
#[test]
fn the_generator_produces_every_case_the_property_needs() {
    const HISTORIES: usize = 384;

    let mut runner = TestRunner::deterministic();
    let strategy = any_history();
    let mut total = Coverage::default();
    let mut stats = Stats::default();
    let mut operations = 0usize;

    for _ in 0..HISTORIES {
        let history = strategy
            .new_tree(&mut runner)
            .expect("the generator produces a history")
            .current();
        operations += history.len();
        let (run, _) = drive(&history).expect("a generated history drives cleanly");
        stats.subscribes += run.stats.subscribes;
        stats.publishes += run.stats.publishes;
        stats.queue_pushed += run.stats.queue_pushed;
        stats.records += run.stats.records;
        stats.delivered += run.stats.delivered;
        total.add(run.coverage);
    }

    println!("{HISTORIES} histories, {operations} operations, {stats:?}");
    for (case, count) in total.named() {
        println!("  {count:>6}  {case}");
    }
    for (case, count) in total.named() {
        assert!(
            count > 0,
            "the generator cannot produce {case}, so the property does not test it"
        );
    }
}
