//! Taps: a subscriber inside the broker whose deliveries a browser
//! can read.
//!
//! Section 11.2 says the Topics page has "a retained browser and a
//! **live** publish and subscribe tool". The publish half went
//! through the broker's own publish path from the start. The
//! subscribe half did not exist: the events stream carries counter
//! deltas and connection events and no publications, so milestone
//! three's watcher paged the retained resource and matched a filter
//! in the browser. That sees retained messages only, and samples
//! rather than streams. This is the mechanism that replaces it.
//!
//! A tap is **a real session**. It connects through
//! [`Broker::connect`], subscribes through [`Broker::handle`] with a
//! genuine SUBSCRIBE packet, and reads its deliveries out of the
//! outbox [`Shared::register_outbox`] hands it — the same three calls
//! a TCP connection makes. There is no second matcher here, for the
//! same reason [`crate::resources::publish`] has no second publish:
//! a second implementation of matching is a second set of rules that
//! will drift from the first the day either is corrected. Shared
//! subscriptions, the rule that a leading wildcard does not match a
//! `$` topic, no-local, retain-as-published, message expiry and the
//! per-session queue bounds are therefore all the broker's, already
//! tested where they live.
//!
//! # The four limits, and why each is the number it is
//!
//! A tap is a way for somebody holding the interface to make the
//! broker do work, and a filter of `#` on a busy broker is the
//! obvious abuse. The mechanism is the easy half; these are the
//! hard half.
//!
//! **[`MAX_TAPS`], how many may exist at once: 8.** One Topics page
//! opens at most one tap, and section 11.1's one-stream-per-document
//! rule plus a browser's six-connections-per-origin ceiling means a
//! handful of tabs is the realistic maximum an operator reaches. Eight
//! leaves room for those tabs and for somebody with `curl` beside
//! them, while bounding the worst case — eight taps all on `#` — to
//! eight extra copies of the broker's traffic, which is less fan-out
//! than eight ordinary subscribers already cost and which no
//! configuration can grow. The ninth is **refused**, naming the cap,
//! rather than evicting the oldest: evicting would silently break a
//! tap somebody is watching, and a watcher that goes quiet for a
//! reason it is not told is the failure this whole feature exists to
//! stop. A tap the broker has closed does not count, because it holds
//! no broker resources any more; see [`Taps::live`].
//!
//! **[`TAP_CAPACITY`], how many messages one tap holds: 256, and the
//! oldest goes.** The page renders the ring as a plain list and is
//! not windowed — the same limitation the retained browser has and
//! for the same reason — so the number has to be one a document can
//! hold without the scroll cost the Clients table was built to avoid.
//! 256 rows is already more than a person reads in one screen, and at
//! the payload cap below it is at most 256 KiB per tap and 2 MiB
//! across all eight, which is not memory worth thinking about beside
//! a broker sized for hundreds of thousands of sessions. **The oldest
//! is dropped, not the newest**, because somebody watching a filter
//! wants what is happening now; and every drop is counted and
//! reported, both in total and as the gap a particular reader missed,
//! so a tap showing nothing can always be told from a tap whose ring
//! is being overrun.
//!
//! **[`MAX_PAYLOAD_BYTES`], how much of a payload it keeps: 1024.**
//! Deliberately [`crate::resources::MAX_INLINE_PAYLOAD`] itself and
//! not a second constant equal to it, because the Topics page shows
//! retained payloads and tapped payloads in two lists on one screen
//! and two different truncation points would be a thing to explain.
//! What it buys is the same thing there: a tap that forwarded
//! arbitrary payloads would let one 10 MB publication turn a poll
//! into a 10 MB response. Over it, the row carries the real byte
//! length and `text_omitted: "too_large"`; a payload that is not
//! UTF-8 carries `"binary"`. The row is never silently shortened,
//! because a truncated payload that does not say so is worse than no
//! payload.
//!
//! **[`IDLE_TIMEOUT`], when a tap expires unread: 120 seconds.** A
//! browser tab closes without telling anyone, so the only evidence a
//! tap is still wanted is that somebody is reading it. The page polls
//! once a second while it is visible; every current browser throttles
//! a hidden tab's timers to roughly one per minute. So the timeout
//! has to be a multiple of that minute or an operator who switches
//! tabs to read something else comes back to a dead tap. Two minutes
//! is two of those throttled polls, which survives one being missed
//! entirely, and it bounds what a forgotten tap costs to two minutes
//! of one subscription. Longer would mean a closed tab keeps matching
//! and copying messages for longer than anybody would wait before
//! wondering why. The sweep runs every [`REAP_INTERVAL`] and does
//! bounded work, because there are at most [`MAX_TAPS`] of them.
//!
//! # What stops a tap affecting delivery to a real client
//!
//! A tap is an observer. Seven things enforce that, and they are
//! listed because "it is only a subscriber" is not an argument.
//!
//! 1. **A tap is its own session.** Its queue, its inflight window
//!    and its outbox are the broker's per-session structures, bounded
//!    by the same configured limits every other session has. Nothing
//!    it fills belongs to anybody else.
//! 2. **[`Shared::deliver`] never blocks.** It is a `try_send` per
//!    addressee and it moves on. A tap whose reader stalled loses
//!    actions against the existing slow-reader counter; the loop
//!    continues to the next subscriber in the same batch.
//! 3. **The drain task never touches the broker lock.** It moves an
//!    action out of a channel and into this module's own mutex, which
//!    is held for a push and never across an await.
//! 4. **A tap subscribes at at-most-once and nothing else.** So it
//!    consumes no inflight slot, owes no acknowledgement and is
//!    waited for by nothing; a QoS 1 or 2 publication reaches it
//!    downgraded by the protocol's own `min(published, granted)`
//!    rule, exactly as it would reach any QoS 0 subscriber.
//! 5. **A shared filter is refused.** `$share/group/filter` is the
//!    one filter through which a new subscriber takes messages *away*
//!    from existing ones: the broker picks one member of the group
//!    per message, and a tap that joined would win some of those
//!    picks. That is the single way an ordinary subscription could
//!    change what a real client receives, so it is the single filter
//!    a tap may not have. See [`open`].
//! 6. **A tap cannot take over a real client's session.** Its client
//!    identifier is checked against the registry under the broker
//!    lock before it connects, and another is chosen if it is taken,
//!    so a tap never displaces anybody. The reverse — a real client
//!    connecting with a tap's identifier — displaces the tap, which
//!    notices its [`Action::Close`] and reports itself closed; the
//!    epoch check in [`Tap::teardown`] is what stops it then
//!    detaching the newcomer.
//! 7. **A full ring drops; it never pushes back.** There is no path
//!    from this module to anything the broker waits on.
//!
//! # What a tap does cost
//!
//! Said plainly rather than left to be found. A tap writes to the
//! store: its session and its subscription are durable facts like any
//! other, so opening one journals a session record and a subscription
//! record, and closing one journals their removal. That is a handful
//! of records per tap and **not** one per message — a fire-and-forget
//! delivery to an attached session writes nothing, which is
//! `Outbound::offer`'s direct path. A tap also appears on the Clients
//! page, because it is a client; it is named `$admin-tap-<n>` so that
//! the row explains itself, which is the thing milestone three's
//! demonstration said a tap must not fail to do.
//!
//! [`Broker::connect`]: broker_core::broker::Broker::connect
//! [`Broker::handle`]: broker_core::broker::Broker::handle
//! [`Shared::register_outbox`]: sdmqtt_server::shared::Shared::register_outbox
//! [`Shared::deliver`]: sdmqtt_server::shared::Shared::deliver

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use broker_core::broker::Broker;
use broker_core::registry::SessionId;
use broker_core::session::Action;
use broker_core::subscriptions::SubscriberId;
use mqtt_codec::packet::{Connect, RetainHandling, Subscribe, SubscriptionOptions, Unsubscribe};
use mqtt_codec::{Packet, Properties, QoS, Version};
use parking_lot::Mutex;
use sdmqtt_server::connection::outbox_depth_for;
use sdmqtt_server::shared::Shared;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::{ApiError, ErrorCode};
use crate::resources::{Authorized, MAX_INLINE_PAYLOAD};
use crate::AdminState;

/// How many taps may exist at once. See the module documentation.
pub const MAX_TAPS: usize = 8;

/// How many messages one tap holds before the oldest is dropped.
pub const TAP_CAPACITY: usize = 256;

/// The largest payload a tap carries to a reader.
///
/// The retained resource's own cap, named rather than repeated, so
/// that the two lists the Topics page draws cannot truncate at two
/// different points.
pub const MAX_PAYLOAD_BYTES: usize = MAX_INLINE_PAYLOAD;

/// How long a tap survives with nobody reading it.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// How often the sweep looks for one that has.
///
/// Well under the timeout, so that a tap is reaped within a small
/// fraction of it rather than up to one whole extra period late, and
/// far above anything that would make the sweep itself a cost: it
/// walks at most [`MAX_TAPS`] entries and takes no broker lock unless
/// it finds one to close.
pub const REAP_INTERVAL: Duration = Duration::from_secs(5);

/// How many candidate identifiers a tap tries before giving up.
///
/// Reached only if somebody has deliberately connected real clients
/// under this many consecutive `$admin-tap-<n>` names, which is not
/// an accident. Bounded rather than a loop, because the alternative
/// to a bound is holding the broker lock for as long as an adversary
/// likes.
const IDENTIFIER_ATTEMPTS: usize = 64;

// ---------------------------------------------------------------
// What a reader sees
// ---------------------------------------------------------------

/// One publication a tap caught.
#[derive(Debug, Clone, Serialize)]
pub struct Captured {
    /// Where this one sits in the tap's own sequence. Monotonic from
    /// one, and what a reader passes back as `after`.
    pub seq: u64,
    /// The topic it was published to.
    pub topic: String,
    /// The level it was **delivered** at, which is the lesser of the
    /// level it was published at and the tap's granted zero. It is
    /// therefore always zero, and is carried anyway so that a reader
    /// is not relying on that staying true.
    pub qos: u8,
    /// Whether the publisher set the retain flag.
    ///
    /// Real because the tap subscribes with retain-as-published; a
    /// subscription without it would see this false on every message
    /// and the page would be showing a constant.
    pub retain: bool,
    /// The payload's size in bytes, always, whether or not the
    /// payload itself is here.
    pub size: usize,
    /// The payload as text, when it is text and within
    /// [`MAX_PAYLOAD_BYTES`].
    pub text: Option<String>,
    /// Why `text` is null, when it is: `binary` or `too_large`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_omitted: Option<&'static str>,
    /// When the tap caught it, in milliseconds since the epoch.
    ///
    /// Stamped here rather than left to the browser, because a poll
    /// returns a batch and a browser stamping on arrival would show
    /// a second's worth of traffic as having happened at one instant.
    pub at: u64,
}

/// The limits every answer repeats.
///
/// Sent on every response rather than documented and assumed. The
/// page has to tell an operator what the bounds are — a watcher
/// showing nothing has to be distinguishable from a watcher whose
/// ring is full — and a page that hard-codes the numbers is a second
/// place they are written down.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct TapLimits {
    /// [`MAX_TAPS`].
    pub max_taps: usize,
    /// [`TAP_CAPACITY`].
    pub capacity: usize,
    /// [`MAX_PAYLOAD_BYTES`].
    pub max_payload_bytes: usize,
    /// [`IDLE_TIMEOUT`], in seconds.
    pub idle_timeout_seconds: u64,
}

impl TapLimits {
    /// The limits this build enforces.
    pub const fn current() -> Self {
        Self {
            max_taps: MAX_TAPS,
            capacity: TAP_CAPACITY,
            max_payload_bytes: MAX_PAYLOAD_BYTES,
            idle_timeout_seconds: IDLE_TIMEOUT.as_secs(),
        }
    }
}

/// What a tap is, without its contents.
#[derive(Debug, Clone, Serialize)]
pub struct TapView {
    /// The identifier this API knows it by.
    pub id: u64,
    /// The filter it subscribed with.
    pub filter: String,
    /// The session it is, as the Clients page shows it.
    pub client_id: String,
    /// How many publications it has caught since it opened.
    pub delivered: u64,
    /// How many it dropped from its ring to make room. The oldest,
    /// always.
    pub dropped: u64,
    /// How many the broker discarded before they reached the ring at
    /// all, which is the tap's own session queue overflowing.
    pub lost: u64,
    /// Why the broker closed this tap's session, when it has.
    ///
    /// Null for a live tap. A closed tap still answers, so that a
    /// reader learns why rather than finding a `404` it cannot
    /// explain, and it no longer counts against [`MAX_TAPS`].
    pub closed: Option<String>,
}

/// What opening a tap answers.
#[derive(Debug, Serialize)]
pub struct TapOpened {
    /// The tap.
    #[serde(flatten)]
    pub tap: TapView,
    /// What it is bounded by.
    pub limits: TapLimits,
}

/// What reading a tap answers.
#[derive(Debug, Serialize)]
pub struct TapRead {
    /// The tap.
    #[serde(flatten)]
    pub tap: TapView,
    /// What it is bounded by.
    pub limits: TapLimits,
    /// Everything after the `after` the reader passed, oldest first.
    pub messages: Vec<Captured>,
    /// What to pass as `after` next time.
    ///
    /// The highest sequence this tap has issued, not the highest
    /// returned: a reader that is handed nothing must not be sent
    /// back to a cursor the ring has already dropped past.
    pub next: u64,
    /// How many publications this **particular** reader missed
    /// between its cursor and the oldest message still held.
    ///
    /// Distinct from `dropped`, which is the tap's total since it
    /// opened. A reader polling promptly sees `dropped` climbing and
    /// `missed` at zero, which means somebody else's poll was late,
    /// not this one's.
    pub missed: u64,
}

/// The list of taps.
#[derive(Debug, Serialize)]
pub struct TapList {
    /// Every tap this process holds, closed ones included.
    pub items: Vec<TapView>,
    /// How many of them are live and so count against the cap.
    pub live: usize,
    /// What they are bounded by.
    pub limits: TapLimits,
}

// ---------------------------------------------------------------
// The ring
// ---------------------------------------------------------------

/// Everything about a tap that changes after it opens.
///
/// One mutex rather than one per field: it is taken by a drain task
/// pushing a message and by a handler copying a page out, both of
/// which are short, and two locks would be two orders to get wrong.
#[derive(Debug)]
struct Inner {
    held: VecDeque<Captured>,
    /// The sequence the next message will be given.
    next_seq: u64,
    delivered: u64,
    dropped: u64,
    lost: u64,
    /// When somebody last read this tap. The lease.
    last_read: Instant,
    /// Why the broker closed the session, once it has.
    closed: Option<String>,
}

impl Inner {
    fn new(now: Instant) -> Self {
        Self {
            held: VecDeque::new(),
            next_seq: 1,
            delivered: 0,
            dropped: 0,
            lost: 0,
            last_read: now,
            closed: None,
        }
    }

    /// Take one publication, dropping the oldest if the ring is full.
    fn push(&mut self, publish: mqtt_codec::packet::Publish, at: u64) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.delivered += 1;
        let (text, text_omitted) = describe(&publish.payload);
        self.held.push_back(Captured {
            seq,
            topic: publish.topic,
            qos: qos_number(publish.qos),
            retain: publish.retain,
            size: publish.payload.len(),
            text,
            text_omitted,
            at,
        });
        // `while` rather than `if`: the capacity is a constant today
        // and a loop costs one comparison, and an `if` against a ring
        // that somehow held two too many would leave it permanently
        // one over.
        while self.held.len() > TAP_CAPACITY {
            self.held.pop_front();
            self.dropped += 1;
        }
    }
}

/// The part of a tap the drain task shares with the handlers.
#[derive(Debug)]
struct TapState {
    inner: Mutex<Inner>,
}

/// The protocol's numbering.
fn qos_number(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

/// Describe a payload without always carrying it.
///
/// The same two answers, in the same order, that the retained
/// resource gives: too large before not-text, so that a huge payload
/// is reported as huge whether or not it also happens to be binary.
fn describe(payload: &[u8]) -> (Option<String>, Option<&'static str>) {
    if payload.len() > MAX_PAYLOAD_BYTES {
        return (None, Some("too_large"));
    }
    match std::str::from_utf8(payload) {
        Ok(text) => (Some(text.to_owned()), None),
        Err(_) => (None, Some("binary")),
    }
}

/// Now, in milliseconds since the epoch.
///
/// A clock that has been set backwards before the epoch answers zero
/// rather than panicking: a timestamp is for a person reading a list,
/// and no timestamp is worth taking a broker down for.
fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

// ---------------------------------------------------------------
// One tap
// ---------------------------------------------------------------

/// A subscriber inside the broker that an interface reads.
#[derive(Debug)]
pub struct Tap {
    id: u64,
    filter: String,
    client_id: String,
    subscriber: SubscriberId,
    /// Which registration of that handle is ours. A newcomer taking
    /// the identifier over gets a new one, and that is how teardown
    /// knows not to detach somebody else's session.
    epoch: u64,
    state: Arc<TapState>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Tap {
    /// What this tap looks like from outside.
    fn view(&self) -> TapView {
        let inner = self.state.inner.lock();
        TapView {
            id: self.id,
            filter: self.filter.clone(),
            client_id: self.client_id.clone(),
            delivered: inner.delivered,
            dropped: inner.dropped,
            lost: inner.lost,
            closed: inner.closed.clone(),
        }
    }

    /// Whether the broker still holds this tap's session.
    fn is_live(&self) -> bool {
        self.state.inner.lock().closed.is_none()
    }

    /// Read everything after `after`, and renew the lease.
    ///
    /// The lease is renewed here and nowhere else, because reading is
    /// the only evidence there is that somebody still wants this. A
    /// tap that is being filled but never read is precisely the one
    /// the timeout exists for.
    fn read(&self, after: Option<u64>, now: Instant) -> TapRead {
        let mut inner = self.state.inner.lock();
        inner.last_read = now;
        let oldest = inner.held.front().map_or(inner.next_seq, |first| first.seq);
        let missed = match after {
            // The reader's cursor is behind the oldest thing still
            // held, so everything between them is gone. Saturating
            // because a reader that is ahead of the ring — which a
            // restarted tap would look like — has missed nothing.
            Some(after) => oldest.saturating_sub(after.saturating_add(1)),
            None => 0,
        };
        let messages: Vec<Captured> = match after {
            Some(after) => inner
                .held
                .iter()
                .filter(|message| message.seq > after)
                .cloned()
                .collect(),
            None => inner.held.iter().cloned().collect(),
        };
        let next = inner.next_seq - 1;
        TapRead {
            tap: TapView {
                id: self.id,
                filter: self.filter.clone(),
                client_id: self.client_id.clone(),
                delivered: inner.delivered,
                dropped: inner.dropped,
                lost: inner.lost,
                closed: inner.closed.clone(),
            },
            limits: TapLimits::current(),
            messages,
            next,
            missed,
        }
    }

    /// Pretend this tap was last read `by` earlier than it was.
    ///
    /// Test-only, and only for the one thing a test cannot otherwise
    /// reach: that the **spawned** sweep runs at all. Everything
    /// about *when* a tap is idle is judged against an instant the
    /// caller passes to [`Taps::reap`], which needs no such help.
    #[cfg(test)]
    fn age(&self, by: Duration) {
        let mut inner = self.state.inner.lock();
        inner.last_read -= by;
    }

    /// Whether nobody has read this tap for longer than the timeout.
    fn is_idle(&self, now: Instant) -> bool {
        now.duration_since(self.state.inner.lock().last_read) > IDLE_TIMEOUT
    }

    /// Give the session back to the broker.
    ///
    /// **Unsubscribe, then disconnect.** The unsubscribe is not
    /// decoration: without it the filter stays in the trie until the
    /// session's expiry is swept, and every matching publication in
    /// that window is queued for a session nobody is reading and then
    /// counted as a discard against a reader that is not slow but
    /// gone.
    ///
    /// **Silent when a newcomer has taken the handle over.** The
    /// epoch check is [`Attached::detach`]'s, for the same reason:
    /// disconnecting here would detach a live client, and removing
    /// the outbox would remove theirs.
    ///
    /// [`Attached::detach`]: sdmqtt_server::connection
    fn teardown(&self, shared: &Shared) {
        if let Some(task) = self.task.lock().take() {
            task.abort();
        }
        let (actions, ours) = shared.with_broker(|broker| {
            let superseded = broker
                .session(self.subscriber)
                .is_some_and(|session| session.epoch() != self.epoch);
            let mut out = Vec::new();
            if !superseded {
                let unsubscribe = Packet::Unsubscribe(Unsubscribe {
                    packet_id: 1,
                    filters: vec![self.filter.clone()],
                    properties: Properties::default(),
                });
                // An error here is a session that is already gone,
                // which is the state this is trying to reach.
                let _ = broker.handle(self.subscriber, &unsubscribe, &mut out);
                broker.disconnect(self.subscriber, &mut out);
            }
            (out, !superseded)
        });
        shared.deliver(actions);
        if ours {
            shared.remove_outbox(self.subscriber);
        }
    }
}

// ---------------------------------------------------------------
// The registry
// ---------------------------------------------------------------

/// Every tap this process holds.
#[derive(Debug, Default)]
pub struct Taps {
    held: Mutex<Vec<Arc<Tap>>>,
    next_id: AtomicU64,
}

impl Taps {
    /// An empty registry.
    pub fn new() -> Self {
        Self {
            held: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// How many taps hold a session, which is what the cap counts.
    pub fn live(&self) -> usize {
        self.held.lock().iter().filter(|tap| tap.is_live()).count()
    }

    /// Every tap, closed ones included.
    fn list(&self) -> Vec<TapView> {
        self.held.lock().iter().map(|tap| tap.view()).collect()
    }

    /// One tap by identifier.
    fn get(&self, id: u64) -> Option<Arc<Tap>> {
        self.held
            .lock()
            .iter()
            .find(|tap| tap.id == id)
            .map(Arc::clone)
    }

    /// Forget a tap, returning it so the caller can tear it down
    /// **outside** this lock.
    ///
    /// Taking the broker lock under this one would be a second lock
    /// order in a process that has one, and the teardown takes the
    /// broker lock.
    fn take(&self, id: u64) -> Option<Arc<Tap>> {
        let mut held = self.held.lock();
        let at = held.iter().position(|tap| tap.id == id)?;
        Some(held.remove(at))
    }

    /// Close and forget everything that nobody has read in time.
    ///
    /// Returns how many were closed, because a sweep whose only
    /// effect is that something is gone cannot otherwise be told from
    /// one that did nothing. Convention 5: the assertion that a tap
    /// expired must be able to fail for the right reason.
    pub fn reap(&self, shared: &Shared, now: Instant) -> usize {
        let expired: Vec<Arc<Tap>> = {
            let mut held = self.held.lock();
            let mut expired = Vec::new();
            held.retain(|tap| {
                if tap.is_idle(now) {
                    expired.push(Arc::clone(tap));
                    false
                } else {
                    true
                }
            });
            expired
        };
        for tap in &expired {
            tracing::debug!(
                tap = tap.id,
                filter = %tap.filter,
                "a tap expired with nobody reading it"
            );
            tap.teardown(shared);
        }
        expired.len()
    }

    /// Close and forget every tap, whatever its lease says.
    ///
    /// Shutdown. A tap left behind is a session the broker never
    /// detaches, and a clean stop is how sessions reach disk.
    pub fn close_all(&self, shared: &Shared) {
        let all: Vec<Arc<Tap>> = std::mem::take(&mut *self.held.lock());
        for tap in &all {
            tap.teardown(shared);
        }
    }
}

// ---------------------------------------------------------------
// Opening one
// ---------------------------------------------------------------

/// What a request to open a tap may say.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TapRequest {
    /// The filter to subscribe with. An MQTT topic filter, so
    /// wildcards are the point; a shared filter is refused.
    pub filter: String,
}

/// Open a tap on `filter`, or say why not.
///
/// # Order, which matters
///
/// The outbox is registered **before** anything the connect produced
/// is delivered, exactly as a connection does it, or the first thing
/// addressed to this session is counted as having no reader. The
/// subscribe is a second broker section rather than part of the
/// first, because [`Broker::handle`] is the only entry point that
/// runs the subscription rules and it takes a handle that does not
/// exist until the connect returns.
///
/// [`Broker::handle`]: broker_core::broker::Broker::handle
pub fn open(state: &AdminState, filter: &str) -> Result<Arc<Tap>, ApiError> {
    // Refused before anything is built, and refused by name. A
    // shared subscription is the one filter through which a
    // subscriber takes messages away from other subscribers, and a
    // tap that could do that would not be an observer.
    //
    // An unparseable `$share/...` falls through to the validation
    // below, which names what is wrong with it; it is refused either
    // way.
    if let Ok(Some(_)) = mqtt_codec::topic::parse_shared(filter) {
        return Err(ApiError::in_field(
            ErrorCode::InvalidRequest,
            "filter",
            "a tap cannot hold a shared subscription: the broker gives each message to \
             one member of a share group, so a tap in the group would take messages away \
             from the clients that are meant to have them. Tap the group's own filter \
             instead.",
        ));
    }

    // Counted, and the count is of live taps: a tap the broker has
    // closed holds nothing and standing in the way of a new one would
    // be a cap on history rather than on work.
    if state.taps.live() >= MAX_TAPS {
        return Err(ApiError::new(
            ErrorCode::ResourceExhausted,
            format!(
                "this broker already has the {MAX_TAPS} taps it allows. A tap is a real \
                 subscriber inside the broker, so they are capped rather than queued. Close \
                 one, or wait for an unread one to expire after {} seconds.",
                IDLE_TIMEOUT.as_secs()
            ),
        ));
    }

    let id = state.taps.next_id.fetch_add(1, Ordering::Relaxed);
    let filter = filter.to_owned();

    // The connect, and the identifier it takes. Both under one
    // acquisition of the lock: a name checked in one section and used
    // in another is a name a real client could claim in between.
    let connected = state.shared.with_broker(|broker| {
        mqtt_codec::topic::validate_subscription(&filter, &broker.config().limits).map_err(
            |error| {
                ApiError::in_field(
                    ErrorCode::InvalidRequest,
                    "filter",
                    format!("that is not a topic filter a tap can subscribe with: {error}"),
                )
            },
        )?;
        let Some(client_id) = free_identifier(broker, id) else {
            return Err(ApiError::new(
                ErrorCode::ResourceExhausted,
                "no unused tap identifier could be found; something is holding sessions \
                 named like this broker's taps",
            ));
        };
        let connect = tap_connect(&client_id);
        let mut out = Vec::new();
        let registration = broker.connect(MOUNTPOINT, &connect, &mut out);
        Ok((client_id, registration, out))
    })?;
    let (client_id, registration, connect_actions) = connected;

    // Sized the way a connection's is, from the bounds this broker is
    // configured with. A smaller channel would make
    // `Shared::deliver`'s "this should be impossible" branch
    // reachable, and an unreachable branch that a tap can reach is
    // a warning an operator cannot act on.
    let depth = state.shared.with_broker(|broker| {
        outbox_depth_for(
            broker.config().max_queue_messages,
            broker.config().max_inflight,
        )
    });
    let Some(depth) = depth else {
        return Err(ApiError::new(
            ErrorCode::Internal,
            "this broker's queue bounds do not size an outbox, so a tap cannot be attached",
        ));
    };
    let outbox = state.shared.register_outbox(registration.subscriber, depth);
    state.shared.deliver(connect_actions);

    // The subscribe, through the packet path and not around it.
    let subscribed = state.shared.with_broker(|broker| {
        let mut out = Vec::new();
        let packet = Packet::Subscribe(tap_subscribe(&filter));
        let outcome = broker.handle(registration.subscriber, &packet, &mut out);
        (outcome, out)
    });
    let (outcome, actions) = subscribed;

    // The SUBACK is read out of the batch rather than delivered and
    // forgotten: whether the broker granted this filter is the
    // answer this request owes its caller, and the broker is the only
    // thing that knows it.
    let granted = outcome.is_ok()
        && actions.iter().any(|(_, action)| match action {
            Action::Send(Packet::SubAck(ack)) => {
                ack.reasons.iter().all(|reason| !reason.is_failure())
            }
            _ => false,
        });
    state.shared.deliver(actions);

    let state_inner = Arc::new(TapState {
        inner: Mutex::new(Inner::new(Instant::now())),
    });
    let tap = Arc::new(Tap {
        id,
        filter: filter.clone(),
        client_id,
        subscriber: registration.subscriber,
        epoch: registration.epoch,
        state: Arc::clone(&state_inner),
        task: Mutex::new(None),
    });

    if !granted {
        tap.teardown(&state.shared);
        return Err(ApiError::in_field(
            ErrorCode::InvalidRequest,
            "filter",
            format!("the broker refused a subscription to {filter}"),
        ));
    }

    let task = tokio::spawn(drain(Arc::clone(&state_inner), outbox));
    *tap.task.lock() = Some(task);
    state.taps.held.lock().push(Arc::clone(&tap));
    tracing::info!(tap = id, %filter, "a tap was opened");
    Ok(tap)
}

/// The namespace a tap's session lives in.
///
/// The default one, empty, because a tap has to see what the default
/// listener's clients see and a mountpoint is a namespace a
/// subscription cannot cross.
const MOUNTPOINT: &str = "";

/// What a tap's session is called.
fn identifier_for(id: u64, attempt: usize) -> String {
    if attempt == 0 {
        format!("$admin-tap-{id}")
    } else {
        format!("$admin-tap-{id}-{attempt}")
    }
}

/// An identifier no session is using.
///
/// **Checked before the connect and under the same lock.** A tap that
/// connected with a name a real client already holds would displace
/// that client, which is the one thing this module is not allowed to
/// do.
fn free_identifier(broker: &Broker, id: u64) -> Option<String> {
    (0..IDENTIFIER_ATTEMPTS).find_map(|attempt| {
        let candidate = identifier_for(id, attempt);
        broker
            .subscriber_of(&SessionId::new(MOUNTPOINT, &candidate))
            .is_none()
            .then_some(candidate)
    })
}

/// The connect a tap makes.
///
/// Version 5 with no session expiry property, which is an expiry of
/// zero: the session ends when the tap detaches, so a tap that is
/// closed, reaped, or lost to a restart leaves nothing behind. Clean
/// start for the same reason. Keep-alive zero, because a tap sends
/// nothing and a keep-alive would have the broker close it for being
/// exactly what it is.
fn tap_connect(client_id: &str) -> Connect {
    Connect {
        version: Version::V5,
        client_id: client_id.to_owned(),
        clean_start: true,
        keep_alive: 0,
        username: None,
        password: None,
        will: None,
        properties: Properties::default(),
    }
}

/// The subscribe a tap makes.
fn tap_subscribe(filter: &str) -> Subscribe {
    Subscribe {
        packet_id: 1,
        filters: vec![SubscriptionOptions {
            filter: filter.to_owned(),
            // At most once and nothing else; see the module
            // documentation.
            qos: QoS::AtMostOnce,
            // A tap never publishes, so this can never apply. False
            // is what an ordinary subscriber sends.
            no_local: false,
            // So that the `retain` a reader sees is the publisher's
            // and not a constant false.
            retain_as_published: true,
            // **Not** the retained store. The Topics page has a
            // retained browser beside this that pages it with its own
            // bounds; sending the whole store into a 256-message ring
            // would fill the ring with history that is already on
            // screen, and it would do that fan-out under the broker's
            // one lock at the moment somebody typed `#`.
            retain_handling: RetainHandling::DoNotSend,
        }],
        properties: Properties::default(),
    }
}

/// Move what the broker addresses to this tap into its ring.
///
/// Ends when the outbox closes, which is a teardown or a takeover.
async fn drain(state: Arc<TapState>, mut outbox: mpsc::Receiver<Action>) {
    while let Some(action) = outbox.recv().await {
        match action {
            Action::Send(Packet::Publish(publish)) => {
                let at = epoch_millis();
                state.inner.lock().push(publish, at);
            }
            // The broker discarded something meant for this tap: its
            // own queue overflowed, or the message expired waiting.
            // Counted, because a message that vanishes with no number
            // moving is the thing section 11.3 forbids.
            Action::Dropped { .. } => {
                state.inner.lock().lost += 1;
            }
            Action::Close(reason) => {
                state.inner.lock().closed =
                    Some(format!("the broker closed this tap's session: {reason:?}"));
                return;
            }
            // A SUBACK, an UNSUBACK, a PINGRESP. Nothing a tap shows.
            Action::Send(_) => {}
        }
    }
    let mut inner = state.inner.lock();
    if inner.closed.is_none() {
        inner.closed = Some("this tap's session went away".to_owned());
    }
}

/// Sweep expired taps until this listener stops.
pub fn spawn_reaper(state: AdminState, every: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(every);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            state.taps.reap(&state.shared, Instant::now());
        }
    })
}

// ---------------------------------------------------------------
// The HTTP
// ---------------------------------------------------------------

/// What a read may ask for.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadQuery {
    after: Option<u64>,
}

/// Open a tap.
async fn create(
    State(state): State<AdminState>,
    _authorized: Authorized,
    body: Result<Json<TapRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<TapOpened>, ApiError> {
    let Json(request) = body.map_err(|rejection| {
        ApiError::invalid_request(format!(
            "the request body is not usable: {}",
            rejection.body_text()
        ))
    })?;
    let tap = open(&state, &request.filter)?;
    Ok(Json(TapOpened {
        tap: tap.view(),
        limits: TapLimits::current(),
    }))
}

/// List the taps.
async fn list(State(state): State<AdminState>, _authorized: Authorized) -> Json<TapList> {
    let items = state.taps.list();
    Json(TapList {
        live: state.taps.live(),
        items,
        limits: TapLimits::current(),
    })
}

/// Read one, and renew its lease.
async fn read(
    State(state): State<AdminState>,
    _authorized: Authorized,
    Path(id): Path<u64>,
    query: Result<Query<ReadQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Json<TapRead>, ApiError> {
    let Query(query) = query.map_err(|rejection| {
        ApiError::invalid_request(format!("the query string is not usable: {rejection}"))
    })?;
    let Some(tap) = state.taps.get(id) else {
        return Err(gone(id));
    };
    Ok(Json(tap.read(query.after, Instant::now())))
}

/// Close one.
async fn close(
    State(state): State<AdminState>,
    _authorized: Authorized,
    Path(id): Path<u64>,
) -> Result<Json<TapView>, ApiError> {
    let Some(tap) = state.taps.take(id) else {
        return Err(gone(id));
    };
    tap.teardown(&state.shared);
    tracing::info!(tap = id, "a tap was closed");
    Ok(Json(tap.view()))
}

/// The one sentence for a tap that is not there.
///
/// It says both things it could mean, because this API cannot tell
/// them apart and a reader that is told only "no such tap" will go
/// looking for a bug in its own identifier.
fn gone(id: u64) -> ApiError {
    ApiError::not_found(format!(
        "there is no tap {id}: either it was never opened, or nobody read it for {} seconds \
         and it expired. Open another.",
        IDLE_TIMEOUT.as_secs()
    ))
}

/// The routes this module owns.
///
/// **All of them need the token**, including the reads, which is not
/// the rule the read resources follow. A tap is a session inside the
/// broker and every call to one extends its life, so reading a tap
/// changes broker state and is a mutation in the only sense that
/// matters here.
pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/api/v1/taps", post(create).get(list))
        .route("/api/v1/taps/{id}", get(read).delete(close))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;
    use crate::{AdminConfig, AdminRunning, AdminServer};
    use bytes::BytesMut;
    use mqtt_codec::packet::Publish;
    use mqtt_codec::{decode, encode, Limits, ReasonCode};
    use sdmqtt_server::listener::{Running, Server, ServerConfig};
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// How long a socket round trip, or a drain task, may take before
    /// the test calls it a failure.
    const REPLY: Duration = Duration::from_secs(5);

    // -----------------------------------------------------------
    // A broker with both listeners over it, and a client for each.
    // -----------------------------------------------------------

    /// An MQTT listener and an admin listener over one broker.
    struct Both {
        mqtt: Running,
        admin: AdminRunning,
        token: Option<String>,
    }

    impl Both {
        async fn start() -> Self {
            Self::start_with(None).await
        }

        async fn start_with(token: Option<&str>) -> Self {
            let mqtt = Server::bind(ServerConfig::ephemeral())
                .await
                .expect("the MQTT listener binds");
            let config = AdminConfig {
                address: "127.0.0.1:0".parse().expect("a valid address"),
                token: token.map(crate::Token::new),
                ..AdminConfig::default()
            };
            let admin = AdminServer::bind(&config, AdminState::new(Arc::clone(mqtt.shared())))
                .await
                .expect("loopback needs no token");
            Self {
                mqtt,
                admin,
                token: token.map(str::to_owned),
            }
        }

        fn shared(&self) -> &Arc<Shared> {
            self.mqtt.shared()
        }

        /// Open a tap on `filter`, expecting it to be opened.
        async fn tap(&self, filter: &str) -> Value {
            let answer = self.open(filter).await;
            assert_eq!(answer.status, 200, "{}", answer.body);
            json(&answer.body)
        }

        /// Ask for a tap on `filter`, whatever the answer.
        async fn open(&self, filter: &str) -> testing::Response {
            let body = serde_json::to_string(&serde_json::json!({ "filter": filter }))
                .expect("the request encodes");
            testing::post_json(
                self.admin.address(),
                "/api/v1/taps",
                self.token.as_deref(),
                &body,
            )
            .await
        }

        /// Read a tap.
        async fn read(&self, id: u64, after: Option<u64>) -> Value {
            let answer = self.read_raw(id, after).await;
            assert_eq!(answer.status, 200, "{}", answer.body);
            json(&answer.body)
        }

        async fn read_raw(&self, id: u64, after: Option<u64>) -> testing::Response {
            let path = match after {
                Some(after) => format!("/api/v1/taps/{id}?after={after}"),
                None => format!("/api/v1/taps/{id}"),
            };
            testing::request(self.admin.address(), "GET", &path, self.token.as_deref()).await
        }

        async fn close(&self, id: u64) -> testing::Response {
            testing::request(
                self.admin.address(),
                "DELETE",
                &format!("/api/v1/taps/{id}"),
                self.token.as_deref(),
            )
            .await
        }

        /// Read tap `id` until `ready` is happy, or give up loudly.
        ///
        /// The drain task is a separate task, so a message that has
        /// been published has not necessarily been captured yet.
        async fn until(&self, id: u64, ready: impl Fn(&Value) -> bool) -> Value {
            let deadline = Instant::now() + REPLY;
            loop {
                let body = self.read(id, None).await;
                if ready(&body) {
                    return body;
                }
                assert!(
                    Instant::now() < deadline,
                    "tap {id} never settled; last answer was {body}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        /// Ask a read resource until `ready` is happy.
        async fn until_resource(&self, path: &str, ready: impl Fn(&str) -> bool) -> String {
            let deadline = Instant::now() + REPLY;
            loop {
                let body = testing::get(self.admin.address(), path).await.body;
                if ready(&body) {
                    return body;
                }
                assert!(
                    Instant::now() < deadline,
                    "{path} never settled; last answer was {body}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }

        async fn stop(self) {
            self.admin.shutdown().await;
            self.mqtt.shutdown().await;
        }
    }

    fn json(body: &str) -> Value {
        serde_json::from_str(body).unwrap_or_else(|error| panic!("{body} is not JSON: {error}"))
    }

    /// The topics a tap read holds, oldest first.
    fn topics(read: &Value) -> Vec<String> {
        read["messages"]
            .as_array()
            .expect("messages is a list")
            .iter()
            .map(|message| message["topic"].as_str().expect("a topic").to_owned())
            .collect()
    }

    /// The payloads a tap read holds, oldest first.
    fn texts(read: &Value) -> Vec<String> {
        read["messages"]
            .as_array()
            .expect("messages is a list")
            .iter()
            .map(|message| message["text"].as_str().unwrap_or("<omitted>").to_owned())
            .collect()
    }

    fn count(read: &Value) -> usize {
        read["messages"].as_array().expect("a list").len()
    }

    fn number(read: &Value, key: &str) -> u64 {
        read[key]
            .as_u64()
            .unwrap_or_else(|| panic!("{key} is a number in {read}"))
    }

    // -----------------------------------------------------------
    // An MQTT client, over a real socket.
    // -----------------------------------------------------------

    async fn send(stream: &mut TcpStream, packet: &Packet) {
        let mut buf = BytesMut::new();
        encode(packet, Version::V5, &mut buf).expect("the packet encodes");
        stream.write_all(&buf).await.expect("the write succeeds");
        stream.flush().await.expect("the flush succeeds");
    }

    async fn next(stream: &mut TcpStream, buf: &mut BytesMut) -> Option<Packet> {
        loop {
            if let Some(packet) =
                decode(buf, Version::V5, &Limits::default()).expect("a valid packet")
            {
                return Some(packet);
            }
            let mut chunk = [0u8; 4096];
            let read = tokio::time::timeout(REPLY, stream.read(&mut chunk))
                .await
                .expect("the broker answers or closes")
                .expect("the socket is readable");
            if read == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..read]);
        }
    }

    async fn handshake(running: &Running, client_id: &str) -> (TcpStream, BytesMut) {
        let mut stream = TcpStream::connect(running.address())
            .await
            .expect("the listener accepts");
        let mut buf = BytesMut::new();
        send(
            &mut stream,
            &Packet::Connect(Box::new(Connect {
                version: Version::V5,
                client_id: client_id.into(),
                clean_start: true,
                keep_alive: 0,
                username: None,
                password: None,
                will: None,
                properties: Properties {
                    session_expiry_interval: Some(3_600),
                    ..Properties::default()
                },
            })),
        )
        .await;
        match next(&mut stream, &mut buf).await {
            Some(Packet::ConnAck(ack)) => assert_eq!(ack.reason, ReasonCode::Success),
            other => panic!("expected an acknowledgement, got {other:?}"),
        }
        (stream, buf)
    }

    /// A publication over the wire, with the payload and flags given.
    fn publication(topic: &str, payload: &[u8], retain: bool) -> Packet {
        Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain,
            topic: topic.into(),
            packet_id: None,
            payload: bytes::Bytes::copy_from_slice(payload),
            properties: Properties::default(),
        })
    }

    // -----------------------------------------------------------
    // The gap this exists to close.
    // -----------------------------------------------------------

    /// The headline, and the exact thing milestone three could not
    /// do.
    ///
    /// Convention 5: the publication is made by a real client over a
    /// real socket **without the retain flag**, and the retained
    /// resource is asserted to be empty in the same test. A tap that
    /// secretly read the retained store would fail on the second
    /// half, and a watcher built the old way would fail on the
    /// first.
    #[tokio::test]
    async fn a_tap_sees_a_publication_that_is_never_retained() {
        let both = Both::start().await;
        let opened = both.tap("sensors/#").await;
        assert_eq!(opened["filter"], "sensors/#");
        assert_eq!(opened["limits"]["capacity"], TAP_CAPACITY);

        let (mut client, _buf) = handshake(&both.mqtt, "publisher").await;
        send(
            &mut client,
            &publication("sensors/attic/temp", b"3.0", false),
        )
        .await;

        let read = both.until(1, |read| count(read) == 1).await;
        assert_eq!(topics(&read), vec!["sensors/attic/temp"]);
        assert_eq!(texts(&read), vec!["3.0"]);
        assert_eq!(read["messages"][0]["retain"], false);
        assert_eq!(number(&read, "next"), 1);

        // And the store really is empty, so nothing here could have
        // come from it.
        let retained = testing::get(both.admin.address(), "/api/v1/retained?limit=1")
            .await
            .body;
        assert!(
            retained.contains("\"total\":0"),
            "nothing was retained: {retained}"
        );
        both.stop().await;
    }

    /// A filter is a filter, and the broker's is the one that runs.
    #[tokio::test]
    async fn a_tap_ignores_a_topic_its_filter_does_not_match() {
        let both = Both::start().await;
        both.tap("sensors/+/temp").await;
        let (mut client, _buf) = handshake(&both.mqtt, "publisher").await;
        send(&mut client, &publication("sensors/a/temp", b"one", false)).await;
        send(
            &mut client,
            &publication("sensors/a/humidity", b"two", false),
        )
        .await;
        send(
            &mut client,
            &publication("sensors/a/b/temp", b"three", false),
        )
        .await;
        send(&mut client, &publication("sensors/z/temp", b"four", false)).await;

        let read = both.until(1, |read| count(read) == 2).await;
        assert_eq!(topics(&read), vec!["sensors/a/temp", "sensors/z/temp"]);
        // Nothing arrived late: a fifth message would have to appear
        // for this to be the wrong two.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(count(&both.read(1, None).await), 2);
        both.stop().await;
    }

    /// The `$` rule the tap gets for free, and the reason it goes
    /// through the broker's matcher rather than a second one.
    ///
    /// [MQTT-4.7.2-1]: a filter beginning with a wildcard does not
    /// match a topic beginning with `$`. Nothing in this module
    /// implements that; deleting the rule from `broker-core` is what
    /// would break this test, which is the point.
    #[tokio::test]
    async fn a_tap_on_everything_does_not_see_a_dollar_topic() {
        let both = Both::start().await;
        both.tap("#").await;
        let (mut client, _buf) = handshake(&both.mqtt, "publisher").await;
        send(&mut client, &publication("$SYS/broker/uptime", b"1", false)).await;
        send(&mut client, &publication("ordinary/topic", b"2", false)).await;

        let read = both.until(1, |read| count(read) == 1).await;
        assert_eq!(topics(&read), vec!["ordinary/topic"]);

        // Named explicitly, it is visible: the rule is about leading
        // wildcards and not about `$` being unreachable.
        both.tap("$SYS/#").await;
        send(&mut client, &publication("$SYS/broker/uptime", b"3", false)).await;
        let named = both.until(2, |read| count(read) == 1).await;
        assert_eq!(topics(&named), vec!["$SYS/broker/uptime"]);
        both.stop().await;
    }

    // -----------------------------------------------------------
    // The limits, which are the hard half.
    // -----------------------------------------------------------

    /// The cap binds, says so, and a closed tap gives its slot back.
    #[tokio::test]
    async fn the_tap_after_the_cap_is_refused_and_closing_one_makes_room() {
        let both = Both::start().await;
        for index in 0..MAX_TAPS {
            let answer = both.open(&format!("a/{index}")).await;
            assert_eq!(answer.status, 200, "tap {index}: {}", answer.body);
        }
        let refused = both.open("one/too/many").await;
        assert_eq!(refused.status, 429, "{}", refused.body);
        let body = json(&refused.body);
        assert_eq!(body["error"]["code"], "resource_exhausted");
        assert!(
            refused.body.contains(&MAX_TAPS.to_string()),
            "the refusal names the cap: {}",
            refused.body
        );

        // And the broker really is holding that many and no more.
        let list = testing::get(both.admin.address(), "/api/v1/taps").await;
        assert_eq!(json(&list.body)["live"], MAX_TAPS);

        assert_eq!(both.close(1).await.status, 200);
        let allowed = both.open("one/too/many").await;
        assert_eq!(allowed.status, 200, "{}", allowed.body);
        both.stop().await;
    }

    /// A full ring loses its oldest, keeps its newest, and counts
    /// what it lost.
    ///
    /// Convention 5: the assertion is on *which* messages survived,
    /// not only on how many. A ring that dropped the newest instead
    /// would hold exactly the same number of rows.
    #[tokio::test]
    async fn a_full_ring_drops_its_oldest_and_counts_them() {
        let both = Both::start().await;
        both.tap("ring/#").await;
        let (mut client, _buf) = handshake(&both.mqtt, "publisher").await;
        let over = 20;
        let sent = TAP_CAPACITY + over;
        for index in 0..sent {
            send(
                &mut client,
                &publication("ring/one", index.to_string().as_bytes(), false),
            )
            .await;
        }

        let read = both
            .until(1, |read| number(read, "delivered") == sent as u64)
            .await;
        assert_eq!(count(&read), TAP_CAPACITY, "the ring holds its capacity");
        assert_eq!(number(&read, "dropped"), over as u64);
        assert_eq!(number(&read, "lost"), 0, "the broker dropped nothing");
        // The oldest went, which is what the rows say.
        assert_eq!(texts(&read).first().map(String::as_str), Some("20"));
        assert_eq!(
            texts(&read).last().map(String::as_str),
            Some((sent - 1).to_string().as_str())
        );
        assert_eq!(number(&read, "next"), sent as u64);
        both.stop().await;
    }

    /// A reader that fell behind is told how much it personally
    /// missed, which is not the same number as the tap's total.
    #[tokio::test]
    async fn a_reader_that_fell_behind_is_told_what_it_missed() {
        let both = Both::start().await;
        both.tap("ring/#").await;
        let (mut client, _buf) = handshake(&both.mqtt, "publisher").await;
        // One, read, then enough to push it out twice over.
        send(&mut client, &publication("ring/one", b"first", false)).await;
        let first = both.until(1, |read| count(read) == 1).await;
        assert_eq!(number(&first, "missed"), 0);
        assert_eq!(number(&first, "next"), 1);

        for index in 0..TAP_CAPACITY + 9 {
            send(
                &mut client,
                &publication("ring/one", index.to_string().as_bytes(), false),
            )
            .await;
        }
        both.until(1, |read| {
            number(read, "delivered") == TAP_CAPACITY as u64 + 10
        })
        .await;

        // The reader comes back with the cursor it had.
        let late = both.read(1, Some(1)).await;
        assert_eq!(count(&late), TAP_CAPACITY);
        // Sequences 2..=10 are gone: nine of them, plus the one the
        // reader already had.
        assert_eq!(number(&late, "missed"), 9);
        assert_eq!(number(&late, "dropped"), 10);

        // A reader that is up to date is told it missed nothing,
        // however much the tap has dropped in total.
        let caught_up = both.read(1, Some(number(&late, "next"))).await;
        assert_eq!(count(&caught_up), 0);
        assert_eq!(number(&caught_up, "missed"), 0);
        assert_eq!(number(&caught_up, "dropped"), 10);
        both.stop().await;
    }

    /// The payload cap, at the boundary and on both sides of it.
    #[tokio::test]
    async fn a_payload_over_the_cap_is_described_rather_than_carried() {
        let both = Both::start().await;
        both.tap("big/#").await;
        let (mut client, _buf) = handshake(&both.mqtt, "publisher").await;

        let at_the_cap = vec![b'x'; MAX_PAYLOAD_BYTES];
        let over_the_cap = vec![b'y'; MAX_PAYLOAD_BYTES + 1];
        // 0x80 is a continuation byte with nothing to continue, so
        // this is bytes and not text at any length.
        let not_text = vec![0x80u8, 0x80, 0x80];
        send(&mut client, &publication("big/exact", &at_the_cap, false)).await;
        send(&mut client, &publication("big/over", &over_the_cap, false)).await;
        send(&mut client, &publication("big/binary", &not_text, false)).await;

        let read = both.until(1, |read| count(read) == 3).await;
        let messages = read["messages"].as_array().expect("a list");

        assert_eq!(messages[0]["size"], MAX_PAYLOAD_BYTES);
        assert_eq!(
            messages[0]["text"].as_str().map(str::len),
            Some(MAX_PAYLOAD_BYTES),
            "exactly at the cap is carried whole"
        );
        assert!(messages[0].get("text_omitted").is_none());

        assert_eq!(messages[1]["size"], MAX_PAYLOAD_BYTES + 1);
        assert!(
            messages[1]["text"].is_null(),
            "one byte over is not carried"
        );
        assert_eq!(messages[1]["text_omitted"], "too_large");

        assert_eq!(messages[2]["size"], 3);
        assert!(messages[2]["text"].is_null());
        assert_eq!(messages[2]["text_omitted"], "binary");
        both.stop().await;
    }

    /// A tap nobody reads expires, and takes its session with it.
    ///
    /// The clock is moved rather than waited on: a test that slept
    /// for the timeout would be two minutes long, and one that
    /// shortened the timeout would not be testing the timeout this
    /// broker has. [`Taps::reap`] takes the instant to judge against
    /// for exactly this reason.
    #[tokio::test]
    async fn a_tap_nobody_reads_expires_and_gives_its_session_back() {
        let both = Both::start().await;
        let opened = both.tap("idle/#").await;
        let client_id = opened["client_id"].as_str().expect("a name").to_owned();
        both.until_resource("/api/v1/subscriptions", |body| body.contains("idle/#"))
            .await;

        // One second short of the timeout, it survives.
        let nearly = Instant::now() + IDLE_TIMEOUT - Duration::from_secs(1);
        assert_eq!(both.shared_reap(nearly), 0, "it is not idle yet");
        assert_eq!(both.read_raw(1, None).await.status, 200);

        // A read renews the lease, so the same instant that would
        // have reaped it a moment ago no longer does.
        let past = Instant::now() + IDLE_TIMEOUT + Duration::from_secs(1);
        assert_eq!(both.shared_reap(past), 1, "it is idle now");

        let gone = both.read_raw(1, None).await;
        assert_eq!(gone.status, 404, "{}", gone.body);
        assert_eq!(json(&gone.body)["error"]["code"], "not_found");

        // The broker has the session back: the subscription goes at
        // once, and the session itself on the expiry the tap was
        // connected with.
        let subscriptions = both
            .until_resource("/api/v1/subscriptions", |body| !body.contains("idle/#"))
            .await;
        assert!(
            !subscriptions.contains("idle/#"),
            "the filter left the trie: {subscriptions}"
        );
        both.until_resource("/api/v1/sessions", |body| !body.contains(&client_id))
            .await;
        both.stop().await;
    }

    /// Reading is what keeps a tap alive, and nothing else does.
    ///
    /// The two instants are half a second apart and judged from the
    /// midpoint, so the assertion is on **which** of them the lease
    /// runs from rather than on a margin of microseconds. A tap
    /// whose lease was never renewed, and so was reaped on a fixed
    /// age from when it opened, fails the first half; a reap that
    /// never fires fails the second.
    #[tokio::test]
    async fn reading_a_tap_renews_its_lease() {
        let both = Both::start().await;
        let opened = Instant::now();
        both.tap("lease/#").await;

        tokio::time::sleep(Duration::from_millis(500)).await;
        both.read(1, None).await;

        // Past the timeout measured from when it opened, and short
        // of it measured from when it was read.
        let between = opened + IDLE_TIMEOUT + Duration::from_millis(250);
        assert_eq!(
            both.shared_reap(between),
            0,
            "the lease runs from the read, not from the open"
        );
        assert_eq!(both.read_raw(1, None).await.status, 200);

        // A whole timeout on from that read, it goes.
        let later = Instant::now() + IDLE_TIMEOUT + Duration::from_secs(1);
        assert_eq!(both.shared_reap(later), 1);
        both.stop().await;
    }

    /// A shared filter is refused, by name and with the reason.
    ///
    /// This is the one filter through which a subscriber changes what
    /// other subscribers receive, which is the one thing a tap is
    /// not allowed to do.
    #[tokio::test]
    async fn a_shared_filter_is_refused() {
        let both = Both::start().await;
        let refused = both.open("$share/group/sensors/#").await;
        assert_eq!(refused.status, 400, "{}", refused.body);
        let body = json(&refused.body);
        assert_eq!(body["error"]["code"], "invalid_request");
        assert_eq!(body["error"]["field"], "filter");

        // And nothing was built on the way to refusing it.
        assert_eq!(
            json(
                &testing::get(both.admin.address(), "/api/v1/taps")
                    .await
                    .body
            )["live"],
            0
        );
        let sessions = testing::get(both.admin.address(), "/api/v1/sessions?limit=1").await;
        assert!(
            sessions.body.contains("\"total\":0"),
            "no session was made: {}",
            sessions.body
        );

        // The inner filter on its own is perfectly acceptable, so
        // the refusal is about sharing and not about the topic.
        assert_eq!(both.open("sensors/#").await.status, 200);
        both.stop().await;
    }

    /// A filter that is not a filter takes the one error shape.
    #[tokio::test]
    async fn a_filter_that_will_not_do_is_refused_in_the_one_shape() {
        let both = Both::start().await;
        for filter in ["", "a/#/b", "a/b+"] {
            let refused = both.open(filter).await;
            assert_eq!(refused.status, 400, "{filter}: {}", refused.body);
            let body = json(&refused.body);
            assert_eq!(body["error"]["code"], "invalid_request", "{filter}");
            assert_eq!(body["error"]["field"], "filter", "{filter}");
        }
        both.stop().await;
    }

    // -----------------------------------------------------------
    // The rule that a tap is only an observer.
    // -----------------------------------------------------------

    /// A tap does not take a message away from a real subscriber.
    ///
    /// Both are on the same filter and both get the message. The
    /// subscriber is read over its own socket rather than through a
    /// resource, because what is being asserted is a delivery.
    #[tokio::test]
    async fn a_tap_does_not_take_a_message_from_a_real_subscriber() {
        let both = Both::start().await;
        let (mut subscriber, mut inbox) = handshake(&both.mqtt, "a-real-client").await;
        send(
            &mut subscriber,
            &Packet::Subscribe(Subscribe {
                packet_id: 1,
                filters: vec![SubscriptionOptions {
                    filter: "shared/topic".into(),
                    qos: QoS::AtMostOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: RetainHandling::SendAlways,
                }],
                properties: Properties::default(),
            }),
        )
        .await;
        match next(&mut subscriber, &mut inbox).await {
            Some(Packet::SubAck(ack)) => assert_eq!(ack.reasons, vec![ReasonCode::Success]),
            other => panic!("expected a subscribe acknowledgement, got {other:?}"),
        }

        both.tap("shared/topic").await;

        let (mut publisher, _buf) = handshake(&both.mqtt, "a-publisher").await;
        send(&mut publisher, &publication("shared/topic", b"both", false)).await;

        match next(&mut subscriber, &mut inbox).await {
            Some(Packet::Publish(publish)) => {
                assert_eq!(publish.topic, "shared/topic");
                assert_eq!(&publish.payload[..], b"both");
            }
            other => panic!("the real subscriber must still receive it, got {other:?}"),
        }
        let read = both.until(1, |read| count(read) == 1).await;
        assert_eq!(texts(&read), vec!["both"]);
        both.stop().await;
    }

    /// A tap never displaces a client holding the name it wanted.
    ///
    /// The identifier is checked against the registry before the
    /// connect and under the same lock. Without that, opening a tap
    /// while a client happened to be called `$admin-tap-1` would
    /// disconnect that client, which is the one thing this module is
    /// forbidden to do.
    #[tokio::test]
    async fn a_tap_does_not_displace_a_client_holding_its_identifier() {
        let both = Both::start().await;
        let squatter = identifier_for(1, 0);
        let (mut client, mut buf) = handshake(&both.mqtt, &squatter).await;

        let opened = both.tap("anything/#").await;
        assert_ne!(
            opened["client_id"], squatter,
            "the tap took another name: {opened}"
        );
        assert_eq!(opened["client_id"], identifier_for(1, 1));

        // The squatter is untouched: still connected, and its socket
        // still answers.
        let clients = testing::get(both.admin.address(), "/api/v1/clients").await;
        assert!(
            clients
                .body
                .contains(&format!("\"client_id\":\"{squatter}\"")),
            "{}",
            clients.body
        );
        send(&mut client, &Packet::PingReq).await;
        assert!(
            matches!(next(&mut client, &mut buf).await, Some(Packet::PingResp)),
            "the displaced-nobody client is still there"
        );
        both.stop().await;
    }

    /// Closing a tap gives the broker its subscription and its
    /// session back.
    #[tokio::test]
    async fn closing_a_tap_takes_its_subscription_and_its_session_with_it() {
        let both = Both::start().await;
        let opened = both.tap("going/#").await;
        let client_id = opened["client_id"].as_str().expect("a name").to_owned();
        both.until_resource("/api/v1/subscriptions", |body| body.contains("going/#"))
            .await;

        let closed = both.close(1).await;
        assert_eq!(closed.status, 200, "{}", closed.body);

        let subscriptions = testing::get(both.admin.address(), "/api/v1/subscriptions").await;
        assert!(
            !subscriptions.body.contains("going/#"),
            "the filter left the trie at once: {}",
            subscriptions.body
        );
        both.until_resource("/api/v1/sessions", |body| !body.contains(&client_id))
            .await;

        // And it is gone from this API too.
        assert_eq!(both.read_raw(1, None).await.status, 404);
        both.stop().await;
    }

    /// A tap is granted at-most-once, and reports the publisher's
    /// retain flag rather than a constant.
    #[tokio::test]
    async fn a_tap_is_granted_at_most_once_and_keeps_the_retain_flag() {
        let both = Both::start().await;
        both.tap("levels/#").await;
        let (mut client, mut buf) = handshake(&both.mqtt, "publisher").await;

        // Exactly-once on the wire, which the tap's granted zero
        // must downgrade.
        send(
            &mut client,
            &Packet::Publish(Publish {
                dup: false,
                qos: QoS::ExactlyOnce,
                retain: true,
                topic: "levels/two".into(),
                packet_id: Some(7),
                payload: bytes::Bytes::from_static(b"kept"),
                properties: Properties::default(),
            }),
        )
        .await;
        match next(&mut client, &mut buf).await {
            Some(Packet::PubRec(ack)) => assert_eq!(ack.packet_id, 7),
            other => panic!("expected a receipt, got {other:?}"),
        }

        let read = both.until(1, |read| count(read) == 1).await;
        assert_eq!(read["messages"][0]["qos"], 0, "downgraded to the grant");
        assert_eq!(
            read["messages"][0]["retain"], true,
            "retain as published, so the flag is the publisher's"
        );
        both.stop().await;
    }

    /// A client that takes a tap's identifier over displaces the
    /// tap, and the tap's teardown must not then detach that client.
    ///
    /// The other half of hazard six. The epoch check is the same one
    /// a connection's own detach makes, and without it closing the
    /// tap afterwards would disconnect a live client and remove the
    /// outbox its writer is reading — which presents as a client
    /// that was dropped for no reason anybody can explain.
    #[tokio::test]
    async fn a_tap_displaced_by_a_real_client_does_not_take_it_down() {
        let both = Both::start().await;
        let opened = both.tap("taken/#").await;
        let name = opened["client_id"].as_str().expect("a name").to_owned();

        // The takeover, over a real socket and with the tap's own
        // identifier.
        let (mut client, mut buf) = handshake(&both.mqtt, &name).await;
        let displaced = both.until(1, |read| !read["closed"].is_null()).await;
        assert!(
            displaced["closed"]
                .as_str()
                .expect("a reason")
                .contains("closed this tap's session"),
            "{displaced}"
        );
        // A closed tap holds nothing, so it stops counting against
        // the cap.
        assert_eq!(
            json(
                &testing::get(both.admin.address(), "/api/v1/taps")
                    .await
                    .body
            )["live"],
            0
        );

        // Now close the tap. The session it names is somebody
        // else's.
        assert_eq!(both.close(1).await.status, 200);

        // The client is untouched: its socket still answers, and
        // the broker still calls it connected.
        send(&mut client, &Packet::PingReq).await;
        assert!(
            matches!(next(&mut client, &mut buf).await, Some(Packet::PingResp)),
            "the client that took the identifier over is still served"
        );
        let clients = testing::get(both.admin.address(), "/api/v1/clients").await;
        assert!(
            clients.body.contains(&format!("\"client_id\":\"{name}\"")),
            "{}",
            clients.body
        );
        both.stop().await;
    }

    /// A discard the broker reports against a tap is counted.
    ///
    /// **What this does not prove**, said rather than left to be
    /// discovered: that the broker ever produces one for a tap. A
    /// tap is a connected at-most-once subscriber, so
    /// `Outbound::offer` dispatches to it directly and its queue,
    /// which is where a discard comes from, stays empty. The action
    /// is delivered here through [`Shared::deliver`] — the real
    /// path, the real channel and the real drain task — because the
    /// branch that handles it must not be the one branch of the four
    /// that silently drops a message. Section 11.3: a message that
    /// vanishes with no number moving is a support case nobody can
    /// answer.
    ///
    /// [`Shared::deliver`]: sdmqtt_server::shared::Shared::deliver
    #[tokio::test]
    async fn a_discard_reported_against_a_tap_is_counted() {
        use broker_core::session::message::MessageId;
        use broker_core::session::DropReason;

        let both = Both::start().await;
        both.tap("counted/#").await;
        let subscriber = both
            .shared()
            .with_broker(|broker: &mut Broker| {
                broker.subscriber_of(&SessionId::new("", "$admin-tap-1"))
            })
            .expect("the tap has a session");

        both.shared().deliver(vec![(
            subscriber,
            Action::Dropped {
                id: MessageId::new(),
                reason: DropReason::QueueFull,
                qos: QoS::AtMostOnce,
                topic: Arc::from("counted/one"),
            },
        )]);

        let read = both.until(1, |read| number(read, "lost") == 1).await;
        assert_eq!(count(&read), 0, "a discard is not a message to show");
        assert_eq!(number(&read, "delivered"), 0);
        both.stop().await;
    }

    /// The sweep runs on its own, without anybody calling it.
    ///
    /// Every other expiry test calls [`Taps::reap`] directly, which
    /// is what lets them judge the boundary without waiting two
    /// minutes — and which means all of them pass against a broker
    /// that never spawns the sweep at all. That is convention 5's
    /// "a path no test walks", and a tap nobody ever reaped is a
    /// session that never leaves. So this one walks it: a real tap,
    /// a real [`spawn_reaper`], and nothing else touching it.
    #[tokio::test]
    async fn the_spawned_sweep_runs_without_anybody_calling_it() {
        let mqtt = Server::bind(ServerConfig::ephemeral())
            .await
            .expect("the MQTT listener binds");
        let state = AdminState::new(Arc::clone(mqtt.shared()));
        let tap = open(&state, "swept/#").expect("the tap opens");
        assert_eq!(state.taps.live(), 1);
        tap.age(IDLE_TIMEOUT + Duration::from_secs(1));

        let reaper = spawn_reaper(state.clone(), Duration::from_millis(20));
        let deadline = Instant::now() + REPLY;
        while state.taps.live() > 0 {
            assert!(Instant::now() < deadline, "the sweep never ran");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(state.taps.get(tap.id).is_none(), "and it was forgotten");
        reaper.abort();
        mqtt.shutdown().await;
    }

    /// A tap is live traffic and not the retained store.
    ///
    /// It subscribes with retain handling "do not send", which is a
    /// deliberate choice and not an accident of the default: the
    /// Topics page has a retained browser beside the tap that pages
    /// the store with its own bounds, and sending the whole store
    /// into a 256-message ring would fill the ring with history that
    /// is already on screen — under the broker's one lock, at the
    /// moment somebody typed `#`.
    ///
    /// Convention 5: the retained message is asserted to exist, so
    /// this cannot pass because nothing was retained, and a live
    /// publication to the same topic is asserted to arrive, so it
    /// cannot pass because the tap sees nothing at all.
    #[tokio::test]
    async fn a_tap_does_not_replay_the_retained_store() {
        let both = Both::start().await;
        let (mut client, _buf) = handshake(&both.mqtt, "publisher").await;
        send(&mut client, &publication("kept/one", b"before", true)).await;
        both.until_resource("/api/v1/retained", |body| body.contains("kept/one"))
            .await;

        both.tap("kept/#").await;
        // Long enough for a retained replay to have arrived, which
        // is the only thing this can wait for: an assertion that
        // something has not happened has to say how long it waited.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let quiet = both.read(1, None).await;
        assert_eq!(count(&quiet), 0, "the store is not replayed: {quiet}");

        send(&mut client, &publication("kept/one", b"after", true)).await;
        let live = both.until(1, |read| count(read) == 1).await;
        assert_eq!(texts(&live), vec!["after"]);
        assert_eq!(
            live["messages"][0]["retain"], true,
            "and a retained publication is still seen when it is made"
        );
        both.stop().await;
    }

    /// A cursor returns what is new and nothing else.
    #[tokio::test]
    async fn the_cursor_returns_only_what_is_new() {
        let both = Both::start().await;
        both.tap("walk/#").await;
        let (mut client, _buf) = handshake(&both.mqtt, "publisher").await;
        send(&mut client, &publication("walk/a", b"one", false)).await;
        let first = both.until(1, |read| count(read) == 1).await;
        let cursor = number(&first, "next");

        send(&mut client, &publication("walk/b", b"two", false)).await;
        let second = both.until(1, |read| number(read, "delivered") == 2).await;
        assert_eq!(count(&second), 2, "with no cursor, everything held");

        let only_new = both.read(1, Some(cursor)).await;
        assert_eq!(texts(&only_new), vec!["two"]);
        assert_eq!(number(&only_new, "missed"), 0);
        both.stop().await;
    }

    // -----------------------------------------------------------
    // The HTTP around it.
    // -----------------------------------------------------------

    /// Every tap route needs the token, reads included.
    ///
    /// Not the rule the read resources follow, and deliberately so:
    /// reading a tap renews its lease, which is a change to how long
    /// a session lives inside the broker.
    #[tokio::test]
    async fn every_tap_route_needs_the_token() {
        let both = Both::start_with(Some("the-right-one")).await;
        both.tap("guarded/#").await;

        for (method, path) in [
            ("POST", "/api/v1/taps"),
            ("GET", "/api/v1/taps"),
            ("GET", "/api/v1/taps/1"),
            ("DELETE", "/api/v1/taps/1"),
        ] {
            let refused =
                testing::request(both.admin.address(), method, path, Some("the-wrong-one")).await;
            assert_eq!(refused.status, 401, "{method} {path}: {}", refused.body);
            assert_eq!(json(&refused.body)["error"]["code"], "unauthorized");

            let none = testing::request(both.admin.address(), method, path, None).await;
            assert_eq!(none.status, 401, "{method} {path}: {}", none.body);
        }
        both.stop().await;
    }

    /// A tap that is not there says both things that can mean.
    #[tokio::test]
    async fn a_tap_that_is_not_there_says_which_two_things_that_means() {
        let both = Both::start().await;
        let missing = both.read_raw(404, None).await;
        assert_eq!(missing.status, 404, "{}", missing.body);
        assert_eq!(json(&missing.body)["error"]["code"], "not_found");
        assert!(
            missing.body.contains("expired"),
            "an operator is told why it might be gone: {}",
            missing.body
        );
        assert_eq!(both.close(404).await.status, 404);
        both.stop().await;
    }

    /// Stopping the listener gives every tap's session back.
    ///
    /// A session still attached when the process stops never had its
    /// departure written down.
    #[tokio::test]
    async fn shutting_down_closes_every_tap() {
        let both = Both::start().await;
        both.tap("stopping/#").await;
        both.tap("stopping/too").await;
        let shared = Arc::clone(both.shared());
        let mqtt = both.mqtt;
        both.admin.shutdown().await;

        let subscriptions = shared.with_broker(|broker: &mut Broker| broker.subscription_count());
        assert_eq!(subscriptions, 0, "both taps unsubscribed");
        mqtt.shutdown().await;
    }

    impl Both {
        /// Sweep, judging idleness against `now`.
        fn shared_reap(&self, now: Instant) -> usize {
            self.admin.taps().reap(self.shared(), now)
        }
    }
}
