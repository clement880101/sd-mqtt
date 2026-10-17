//! The events stream, and the ring that makes a reconnect honest.
//!
//! Section 11.1: the stream pushes counter deltas, connect and
//! disconnect events, and events carry identifiers so a reconnecting
//! browser resumes with the `Last-Event-ID` header. Rule outcomes are
//! milestone 4.
//!
//! # The header is not the mechanism; the ring is
//!
//! A [`broadcast`] channel drops messages for a receiver that has
//! fallen behind and reports the drop as a [`RecvError::Lagged`],
//! which is exactly the error the idiomatic `while let Ok(..)` throws
//! away. Section 11.3 forbids a message disappearing uncounted, so
//! nothing here discards that error:
//!
//! - Recent events are kept in a bounded ring keyed by identifier.
//! - A reconnecting browser is replayed from the ring.
//! - A browser asking for an identifier the ring no longer holds is
//!   sent a **resynchronise** event telling it to refetch, rather
//!   than a gap that looks like a complete history.
//! - A live reader that falls behind gets the same resynchronise, and
//!   every event it lost is counted — here and in the registry, as
//!   [`Metrics::admin_events_dropped`].
//!
//! The two are not the same failure. The ring dropping its oldest
//! event is normal and costs nobody anything, because a reader that
//! wanted it is told to refetch. The broadcast dropping an event for
//! a slow reader is the one that would otherwise be silent.
//!
//! # The identifier carries the run
//!
//! Identifiers count from one in each process, so a browser that
//! watched the broker before a restart reconnects with a number the
//! new process may since have passed, and a ring that answered the
//! number alone would replay this run's events as the previous run's
//! gap. The `id:` on the wire is therefore `<epoch>-<id>`, where the
//! epoch names the run, and [`Events::resume`] resynchronises any
//! identifier from another epoch whatever its number. A bare number
//! is still read as this run's, so an interface that kept one keeps
//! working.
//!
//! # Why the subscribe and the backlog happen under one lock
//!
//! The race this exists to not have: read the ring, then subscribe,
//! and an event published in between is in neither, which is a gap
//! nothing reports. Subscribe first and read the ring second, without
//! a lock, and the same event is in both, which is a duplicate.
//!
//! So [`Events::publish`] holds the ring's lock across **both** the
//! push and the broadcast send, and [`Events::subscribe`] holds it
//! across **both** the `subscribe()` and the read. Every event is
//! therefore either wholly before a subscription — in the ring
//! snapshot and not in the receiver — or wholly after it. Exactly
//! once, in order, with no gap. The send is a non-blocking channel
//! send and there is no `await` under the lock; this is the same
//! reasoning [`Shared::with_broker_committed`] uses to keep the
//! store's order the order the broker produced.
//!
//! # A stream must end when the broker stops
//!
//! Section 11.1: graceful shutdown waits for every open connection
//! and the body of an events stream never ends on its own, so one
//! browser tab left open on the Overview page would stop the broker
//! exiting — and a clean stop is how sessions reach disk. Every
//! stream therefore selects on the same signal the listener does; see
//! [`AdminState::shutdown`] and the test named for it.
//!
//! # Never compressed, never buffered
//!
//! There is no compression layer on this router at all, which is how
//! "the events stream is never compressed" is kept: not by exempting
//! a route from a layer, but by there being no layer to exempt it
//! from. A compression layer across the listener would turn a live
//! stream into batch delivery, which presents as an interface that
//! updates in clumps.
//!
//! The header that tells an intermediary not to buffer is sent
//! explicitly; see [`X_ACCEL_BUFFERING`].
//!
//! [`RecvError::Lagged`]: broadcast::error::RecvError::Lagged
//! [`Metrics::admin_events_dropped`]: sdmqtt_server::metrics::Metrics::admin_events_dropped
//! [`Shared::with_broker_committed`]: sdmqtt_server::shared::Shared::with_broker_committed

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use broker_core::registry::SessionId;
use broker_core::subscriptions::SubscriberId;
use futures_core::Stream;
use parking_lot::Mutex;
use sdmqtt_server::metrics::Metrics;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;

use crate::AdminState;

/// An event's identifier. Monotonic within one process, starting at
/// [`FIRST`].
pub type EventId = u64;

/// The first identifier this process issues.
///
/// One rather than zero, so that a `Last-Event-ID: 0` from a client
/// that has seen nothing means "everything you still hold" rather
/// than naming an event.
pub const FIRST: EventId = 1;

/// How many recent events the ring holds.
///
/// Enough that a browser reloading a page, or losing its connection
/// for a few seconds on a busy broker, is replayed rather than told
/// to refetch. Small enough that the memory is not worth thinking
/// about: an event is a small enumeration behind an [`Arc`].
pub const RING_CAPACITY: usize = 1_024;

/// How far behind the live channel a reader may fall before it is
/// told to resynchronise.
///
/// Smaller than the ring on purpose. A reader this far behind is not
/// keeping up with the present, and replaying the ring at it would
/// make it further behind; the ring's job is to cover a *gap between
/// two connections*, not to prop up a reader that cannot drink from
/// the live feed.
pub const LIVE_CAPACITY: usize = 256;

/// How many finished frames may wait for a slow socket.
///
/// The back pressure this creates is what makes the lag detection
/// work: when a socket stops draining, this fills, the pump stops
/// taking from the broadcast channel, the broadcast channel starts
/// dropping for it, and the reader is told to resynchronise. A larger
/// buffer here would only delay that.
const PENDING_FRAMES: usize = 64;

/// How long a disconnected browser should wait before reconnecting.
///
/// **Set rather than left to the browser's default**, which is three
/// seconds. A broker that has just restarted does not want every open
/// tab retrying every three seconds; the herd arrives while it is
/// reading its store, which is the slowest and most important thing
/// it ever does. Fifteen seconds is long enough that a restart is not
/// a stampede and short enough that a person watching a dashboard
/// does not think it has died.
pub const RETRY: Duration = Duration::from_secs(15);

/// How often a comment is sent on an idle stream.
///
/// A dead intermediary between the browser and the broker is
/// otherwise invisible until the next real event, which on a quiet
/// broker may be minutes.
const KEEP_ALIVE: Duration = Duration::from_secs(20);

/// The header that tells an intermediary not to buffer this response.
///
/// Section 11.1 names it as the one thing the broker owes a
/// development proxy, and it owes it to a production one anyway:
/// nginx and several load balancers buffer a response body by
/// default, which turns a live stream into an interface that updates
/// in clumps and is miserable to diagnose from either end.
pub const X_ACCEL_BUFFERING: HeaderName = HeaderName::from_static("x-accel-buffering");

// ---------------------------------------------------------------
// What an event is
// ---------------------------------------------------------------

/// The counters a rate chart is drawn from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CounterTotals {
    /// Bytes read from client sockets.
    pub bytes_received: u64,
    /// Bytes written to client sockets.
    pub bytes_sent: u64,
    /// Client sockets this broker has begun serving.
    pub sockets_opened: u64,
}

/// The levels the Overview page shows beside the rates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Gauges {
    /// Sessions with a connection attached.
    pub connections: u64,
    /// Sessions, connected or not.
    pub sessions: u64,
    /// Subscriptions held across every session.
    pub subscriptions: u64,
    /// Messages the retained store holds.
    pub retained: u64,
    /// Messages waiting in every session's queue.
    pub queue_depth: u64,
}

/// What an event says.
///
/// Tagged, so that a client reads one field to know what it has. The
/// tag is repeated as the SSE `event:` name, because a browser adds
/// listeners by that name and should not have to parse a body to
/// route one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventBody {
    /// A session has a connection attached.
    Connected {
        /// The listener namespace it belongs to.
        mountpoint: String,
        /// Which client.
        client_id: String,
    },
    /// A session no longer has one. The session itself may well
    /// survive; see the Sessions page.
    Disconnected {
        /// The listener namespace it belonged to.
        mountpoint: String,
        /// Which client.
        client_id: String,
    },
    /// Where the numbers are now, and how far they moved.
    Counters {
        /// Since the process started.
        totals: CounterTotals,
        /// Since the previous sample, so a rate is a division.
        deltas: CounterTotals,
        /// Levels, which have no delta.
        gauges: Gauges,
        /// How long the deltas cover.
        over_ms: u64,
    },
    /// Everything you believe may be stale: refetch the resources.
    ///
    /// **Not stored in the ring and carries no identifier.** It is
    /// not a fact about the broker, it is a fact about this one
    /// reader, so replaying it to another would be a lie. Leaving its
    /// identifier off means the browser keeps the last real one it
    /// saw, which is the honest answer: we cannot say which event it
    /// is now up to date with, only that it must refetch.
    Resynchronise {
        /// Why, for a person reading a console. Never matched on.
        reason: &'static str,
    },
}

impl EventBody {
    /// The SSE `event:` name for this body.
    ///
    /// The same strings the `kind` tag uses, from one match, so the
    /// two cannot drift.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Connected { .. } => "connected",
            Self::Disconnected { .. } => "disconnected",
            Self::Counters { .. } => "counters",
            Self::Resynchronise { .. } => "resynchronise",
        }
    }
}

/// One event with the identifier it was issued.
#[derive(Debug, Clone)]
pub struct Stored {
    /// Monotonic within this process.
    pub id: EventId,
    /// Behind an [`Arc`] because every live reader is handed a clone.
    pub body: Arc<EventBody>,
}

// ---------------------------------------------------------------
// The ring
// ---------------------------------------------------------------

/// The ring's contents, behind one lock.
#[derive(Debug)]
struct Ring {
    /// The identifier the next event will be issued.
    next: EventId,
    /// The most recent [`RING_CAPACITY`] events, oldest first.
    held: VecDeque<Stored>,
}

impl Ring {
    /// What a reader resuming after `after` is owed.
    ///
    /// Returns the events it missed, and whether it must
    /// resynchronise instead.
    fn since(&self, after: Option<EventId>) -> (Vec<Stored>, bool) {
        let Some(after) = after else {
            // A browser with no header has just loaded the pages and
            // fetched the resources, so it is up to date by
            // definition and wants the live feed and nothing else.
            // Replaying the ring at it would show it a connect it has
            // already drawn.
            return (Vec::new(), false);
        };
        // An identifier this process has never issued. Identifiers
        // start again at one on a restart, so this is a browser that
        // was watching the broker before it restarted: everything it
        // believes is from another run. [`Events::resume`] catches a
        // tagged identifier from another run before it gets here,
        // whatever its number; this is the net under a bare number.
        if after >= self.next {
            return (Vec::new(), true);
        }
        // `next` when the ring is empty, which makes the comparison
        // below say "nothing is missing" rather than "everything is".
        let oldest = self.held.front().map_or(self.next, |stored| stored.id);
        // `after + 1` is the first event it is owed. Older than the
        // ring's oldest means the gap cannot be filled, and a short
        // answer would be a gap pretending to be complete.
        if after + 1 < oldest {
            return (Vec::new(), true);
        }
        (
            self.held
                .iter()
                .filter(|stored| stored.id > after)
                .cloned()
                .collect(),
            false,
        )
    }
}

/// What a `Last-Event-ID` said, read against this run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resume {
    /// No usable identifier: the live feed from now on.
    Fresh,
    /// An identifier this run issued, or a bare number read as one.
    After(EventId),
    /// An identifier another run issued.
    AnotherRun,
}

/// The run this process is: the clock, in milliseconds.
///
/// Two runs of one process cannot share a millisecond, and nothing
/// compares two epochs except for equality, so the clock is enough
/// and costs no dependency. A clock before 1970 is run zero, which is
/// still a run.
fn epoch_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|since| u64::try_from(since.as_millis()).ok())
        .unwrap_or(0)
}

/// What a new subscriber is handed.
#[derive(Debug)]
pub struct Subscription {
    /// The events it missed, oldest first. Empty when it missed none
    /// and when it missed too many.
    pub backlog: Vec<Stored>,
    /// Whether it must refetch instead of trusting the backlog.
    pub resynchronise: bool,
    /// Why, when it must. For a person reading a console; never
    /// matched on.
    pub why: &'static str,
    /// Everything published after this subscription was taken.
    pub live: broadcast::Receiver<Stored>,
}

/// The recent events, and the live feed of new ones.
#[derive(Debug)]
pub struct Events {
    ring: Mutex<Ring>,
    live: broadcast::Sender<Stored>,
    dropped: AtomicU64,
    /// Where a drop is counted for a scraper as well as for a test.
    metrics: Arc<Metrics>,
    /// Which run of the process these identifiers belong to.
    ///
    /// Identifiers start again at [`FIRST`] on a restart, and a
    /// browser that was watching the old process reconnects with the
    /// last one it saw. Once the new process has issued more events
    /// than that number, the ring would hold a "gap" between the two
    /// that was in fact another run's history and replay it as though
    /// it followed. So the identifier on the wire is `<epoch>-<id>`,
    /// and a `Last-Event-ID` from another epoch is told to
    /// resynchronise whatever its number; see [`Events::resume`].
    epoch: u64,
}

impl Events {
    /// An empty ring over `metrics`, for this run of the process.
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self::with_epoch(metrics, epoch_now())
    }

    /// An empty ring whose identifiers belong to run `epoch`.
    ///
    /// What [`Events::new`] calls with the clock, and what a test
    /// calls with two different numbers to stand for two runs.
    pub fn with_epoch(metrics: Arc<Metrics>, epoch: u64) -> Self {
        let (live, _) = broadcast::channel(LIVE_CAPACITY);
        Self {
            ring: Mutex::new(Ring {
                next: FIRST,
                held: VecDeque::new(),
            }),
            live,
            dropped: AtomicU64::new(0),
            metrics,
            epoch,
        }
    }

    /// The run these identifiers belong to.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// What the stream writes in `id:` for event `id`: the run, a
    /// dash, the number.
    pub fn tagged(&self, id: EventId) -> String {
        format!("{}-{id}", self.epoch)
    }

    /// Subscribe, resuming from whatever `Last-Event-ID` said.
    ///
    /// `None` is a browser with no header, which is up to date by
    /// definition. A tagged identifier from this run resumes after
    /// it, exactly as [`Events::subscribe`] does. One from **another
    /// run** is told to resynchronise whatever its number: the
    /// browser's whole picture is of a process that no longer exists,
    /// and nothing in this ring is what it missed.
    ///
    /// A bare number is read as this run's, which is what every
    /// identifier was before the run was part of it, so an interface
    /// that kept one keeps working. Unreadable is read as absent
    /// rather than refused, for the reason [`last_event_id`] gives.
    pub fn resume(&self, last_event_id: Option<&str>) -> Subscription {
        match self.resume_of(last_event_id) {
            Resume::Fresh => self.subscribe(None),
            Resume::After(after) => self.subscribe(Some(after)),
            Resume::AnotherRun => {
                // Under the ring's lock like every other subscribe, so
                // that the receiver and the ring agree about where
                // "live" starts.
                let _ring = self.ring.lock();
                Subscription {
                    backlog: Vec::new(),
                    resynchronise: true,
                    why: "the events you missed are from before this broker restarted",
                    live: self.live.subscribe(),
                }
            }
        }
    }

    /// Read a `Last-Event-ID` against this run.
    fn resume_of(&self, last_event_id: Option<&str>) -> Resume {
        let Some(value) = last_event_id.map(str::trim) else {
            return Resume::Fresh;
        };
        match value.split_once('-') {
            Some((run, id)) => match (run.parse::<u64>(), id.parse::<EventId>()) {
                (Ok(run), Ok(id)) if run == self.epoch => Resume::After(id),
                (Ok(_), Ok(_)) => Resume::AnotherRun,
                _ => Resume::Fresh,
            },
            None => value.parse().map_or(Resume::Fresh, Resume::After),
        }
    }

    /// Issue an event, and return the identifier it was given.
    ///
    /// The broadcast send happens **under the ring's lock**, and that
    /// is the whole ordering guarantee: two publishers that take the
    /// lock in one order cannot reach the channel in the other, so
    /// the order in the ring and the order on every live feed are the
    /// same order. Nothing here blocks — a broadcast send never waits
    /// for a receiver — and there is no `await` to hold the lock
    /// across.
    pub fn publish(&self, body: EventBody) -> EventId {
        let mut ring = self.ring.lock();
        let id = ring.next;
        ring.next += 1;
        let stored = Stored {
            id,
            body: Arc::new(body),
        };
        ring.held.push_back(stored.clone());
        while ring.held.len() > RING_CAPACITY {
            ring.held.pop_front();
        }
        // An error is "nobody is listening", which is the ordinary
        // state of a broker with no browser open.
        let _ = self.live.send(stored);
        id
    }

    /// Subscribe, resuming after `after`.
    ///
    /// The receiver is taken **under the same lock** the backlog is
    /// read under, which is what makes the two exactly meet. See the
    /// module documentation.
    pub fn subscribe(&self, after: Option<EventId>) -> Subscription {
        let ring = self.ring.lock();
        let live = self.live.subscribe();
        let (backlog, resynchronise) = ring.since(after);
        Subscription {
            backlog,
            resynchronise,
            why: "the events you missed are no longer held",
            live,
        }
    }

    /// Record that a reader lost `count` events.
    ///
    /// Section 11.3: counted, in the registry a scraper reads and in
    /// a number this crate's tests can assert on. Both from here, so
    /// that a later caller cannot do half of it.
    pub fn dropped(&self, count: u64) {
        self.dropped.fetch_add(count, Ordering::Relaxed);
        self.metrics.admin_events_dropped().inc_by(count);
    }

    /// How many events readers have lost.
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// The identifier the next event will be issued, for a test.
    pub fn next_id(&self) -> EventId {
        self.ring.lock().next
    }

    /// How many live feeds are open.
    ///
    /// One per open stream: the handler takes a receiver and hands
    /// it to the task that feeds the socket, so this counts the
    /// tasks that have not finished. It is what lets a test see the
    /// per-stream cleanup path happen rather than assume it.
    pub fn live_readers(&self) -> usize {
        self.live.receiver_count()
    }
}

// ---------------------------------------------------------------
// The stream
// ---------------------------------------------------------------

/// Wait until the listener is stopping.
///
/// Returns when the flag is set, and also when the sender has gone,
/// which is the same thing said by a shorter route. Cancel safe:
/// [`watch::Receiver::changed`] is, and re-entering re-reads the
/// value.
pub(crate) async fn stopping(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

/// The finished frames on their way to one socket.
///
/// A [`Stream`] over a channel rather than a generator, because the
/// interesting logic — selecting on shutdown, turning a lag into a
/// resynchronise — is a loop that wants `await`, and a hand-written
/// `poll_next` holding a boxed future for it is where a stream grows
/// a race. This polls one channel and does nothing else.
struct Frames {
    frames: mpsc::Receiver<SseEvent>,
}

impl Stream for Frames {
    type Item = Result<SseEvent, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.frames.poll_recv(cx).map(|frame| frame.map(Ok))
    }
}

/// The frame carrying one stored event.
///
/// The identifier on the wire is `<epoch>-<id>`, so that a browser
/// resuming after a restart can be told its identifier is from another
/// run; see [`Events::resume`]. The browser never reads it, only
/// echoes it.
fn frame(epoch: u64, stored: &Stored) -> SseEvent {
    SseEvent::default()
        .id(format!("{epoch}-{}", stored.id))
        .event(stored.body.name())
        .json_data(&*stored.body)
        .expect("an event this crate defined serialises")
}

/// The frame telling a reader to refetch. No identifier; see
/// [`EventBody::Resynchronise`].
fn resynchronise(reason: &'static str) -> SseEvent {
    let body = EventBody::Resynchronise { reason };
    SseEvent::default()
        .event(body.name())
        .json_data(&body)
        .expect("an event this crate defined serialises")
}

/// The first frame of every stream: the retry interval, and nothing
/// a browser will deliver to a listener.
fn opening() -> SseEvent {
    SseEvent::default()
        .retry(RETRY)
        .comment("sd-mqtt events; see /api/v1/events")
}

/// Hand one frame to the socket, giving up if the listener stops.
///
/// The send can wait, because the channel in front of the socket is
/// bounded and that back pressure is what turns a slow reader into a
/// counted lag. Waiting on it without also watching the shutdown
/// would be the bug this whole arrangement exists to avoid, one
/// level down.
async fn hand_over(
    frames: &mpsc::Sender<SseEvent>,
    frame: SseEvent,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        biased;
        () = stopping(shutdown) => false,
        sent = frames.send(frame) => sent.is_ok(),
    }
}

/// Feed one stream until it ends, the client goes, or the broker
/// stops.
async fn pump(state: AdminState, subscription: Subscription, frames: mpsc::Sender<SseEvent>) {
    let mut shutdown = state.shutdown.subscribe();
    let Subscription {
        backlog,
        resynchronise: stale,
        why,
        mut live,
    } = subscription;
    let epoch = state.events.epoch();

    if !hand_over(&frames, opening(), &mut shutdown).await {
        return;
    }
    if stale && !hand_over(&frames, resynchronise(why), &mut shutdown).await {
        return;
    }
    for stored in backlog {
        if !hand_over(&frames, frame(epoch, &stored), &mut shutdown).await {
            return;
        }
    }

    loop {
        let received = tokio::select! {
            biased;
            // First, so that a shutdown that arrives at the same
            // moment as an event ends the stream rather than racing
            // it. The body must end; one more event need not arrive.
            () = stopping(&mut shutdown) => return,
            received = live.recv() => received,
        };
        match received {
            Ok(stored) => {
                if !hand_over(&frames, frame(epoch, &stored), &mut shutdown).await {
                    return;
                }
            }
            // **The error the idiomatic combinator discards.** This
            // reader was too slow and the channel dropped `lost`
            // events for it. Counted, and answered with a
            // resynchronise rather than with the next event as
            // though nothing had happened.
            Err(broadcast::error::RecvError::Lagged(lost)) => {
                state.events.dropped(lost);
                tracing::warn!(
                    lost,
                    "an events reader fell behind; it is being told to resynchronise"
                );
                if !hand_over(
                    &frames,
                    resynchronise("this stream fell behind and lost events"),
                    &mut shutdown,
                )
                .await
                {
                    return;
                }
            }
            // The sender is gone, which only happens when the whole
            // listener has.
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// The identifier a reconnecting browser says it is up to.
///
/// An unparseable header is read as absent rather than refused: the
/// worst it costs is a browser that sees the live feed from now on,
/// and refusing the request would leave an interface with no stream
/// at all over a header it did not write by hand.
///
/// Returned as text rather than parsed here, because what it means
/// depends on which run issued it and [`Events::resume`] is what knows.
fn last_event_id(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
}

/// The events stream.
async fn events(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    let subscription = state.events.resume(last_event_id(&headers));
    let (sender, frames) = mpsc::channel(PENDING_FRAMES);
    // One task per open stream. It ends when the client goes, when
    // the broadcast sender goes, or when the listener stops, and
    // dropping its sender is what ends the body.
    tokio::spawn(pump(state, subscription, sender));

    let mut response = Sse::new(Frames { frames })
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
        .into_response();
    // `Content-Type: text/event-stream` and `Cache-Control: no-cache`
    // come from `Sse`. This one does not, and section 11.1 asks for
    // it by name.
    response
        .headers_mut()
        .insert(X_ACCEL_BUFFERING, HeaderValue::from_static("no"));
    response
}

/// The route this module owns.
pub(crate) fn routes() -> Router<AdminState> {
    Router::new().route("/api/v1/events", get(events))
}

// ---------------------------------------------------------------
// Where the events come from
// ---------------------------------------------------------------

/// What the previous sample saw.
#[derive(Debug)]
struct Sample {
    at: Instant,
    totals: CounterTotals,
    gauges: Gauges,
    attached: usize,
    /// Who was connected, by handle, with the name to report when
    /// they go.
    ///
    /// Keyed by [`SubscriberId`] rather than by identity because the
    /// broker's own record of a change is keyed that way, and
    /// because a handle is eight bytes the broker never reuses while
    /// an identity is two reference-counted strings to hash. The
    /// name is carried so that a departure can be reported after the
    /// session holding it has gone.
    connected: HashMap<SubscriberId, SessionId>,
    /// Whether the broker is recording changes for this sampler yet.
    ///
    /// False for exactly one tick, which arms the recording and takes
    /// the first picture under one acquisition of the lock.
    watching: bool,
}

/// Read the broker and turn what changed into events.
///
/// # What this is, and what it should become
///
/// Nothing in `broker-core` or `sdmqtt-server` reports a connect to
/// anybody: the broker does no I/O and the shell has no observer to
/// tell. So today the events are *derived* by sampling, which is
/// honest about connects and disconnects — a client that connected
/// and left inside one interval is never reported — and exact about
/// the counters, which are totals and cannot lose a step.
///
/// Milestone 4 adds hooks (section 11.3: "every discard path has both
/// a metric and a hook"), and when it does, the connect and
/// disconnect events belong on one of them and this sampler shrinks
/// to the counters. Recorded here so that it stays a decision rather
/// than becoming an omission.
///
/// # What it costs the one lock in the process
///
/// Working out *who* connected used to be a walk of every session,
/// built into a fresh set under the lock and diffed against the last
/// one. It was behind a tripwire that skipped it when no counter had
/// moved, which protects an idle broker and nothing else: one
/// connect or one disconnect a second arms it, and that is the
/// normal state of a broker with a hundred thousand sessions on it.
/// Measured on a release build, the set build alone held the lock
/// for 11.9 milliseconds at a hundred thousand sessions and 570
/// microseconds at ten thousand — against the 405 microseconds that
/// made summing every queue unacceptable for a scraper asking every
/// fifteen seconds, and this ran every second whether or not anybody
/// was watching.
///
/// So the broker records what changed where it changes, and this
/// reads the record: [`Presence`], which is the same answer
/// [`QueueTotal`] is. The work under the lock is now the number of
/// sessions that *moved*, and [`sample`] returns that number so a
/// test can say so — `one_sample_reads_only_what_changed`, which is
/// convention 5's "measure the algorithm" and not a stopwatch.
///
/// Two things still walk: the first sample of all, which arms the
/// recording and takes the first picture under one acquisition of
/// the lock, and a sample after the broker's log overflowed, which
/// means nothing read it for sixty-five thousand changes. Both
/// rebuild the whole picture, which is what makes them correct
/// rather than an optimisation with a hole in it.
///
/// **All five gauges are maintained numbers now, and that sentence
/// has been wrong before.** It was wrong when it was first written,
/// and milestone three corrected the claim rather than the code
/// because there was nothing else to read: `subscription_count`
/// walked the subscription trie and `retained_count` walked the
/// retained tree, both under this lock, both on every tick and both
/// again on every metrics scrape, measured at 2.64 and 2.66
/// milliseconds with a hundred thousand of each. Both are read from
/// the structure that keeps them now — a counter on the trie and the
/// length of the retained store's ordered index — and both are
/// guarded against drifting from what they count by a property that
/// walks. A whole sample over a hundred thousand sessions, a hundred
/// thousand subscriptions and ten thousand retained topics went from
/// 3.77 milliseconds to 1.24 microseconds;
/// `what_a_whole_sample_costs` is the measurement, and it is the
/// same broker on the same machine either side.
///
/// [`Presence`]: broker_core::registry::Presence
/// [`QueueTotal`]: broker_core::session::QueueTotal
fn sample(state: &AdminState, previous: &mut Sample) -> usize {
    let metrics = state.shared.metrics();
    let totals = CounterTotals {
        bytes_received: metrics.bytes_received().get(),
        bytes_sent: metrics.bytes_sent().get(),
        sockets_opened: metrics.socket_opened().get(),
    };
    let attached = state.shared.outbox_count();

    let mut changes = Vec::new();
    let (gauges, walked) = state.shared.with_broker(|broker| {
        let gauges = Gauges {
            connections: attached as u64,
            sessions: broker.session_count() as u64,
            subscriptions: broker.subscription_count() as u64,
            retained: broker.retained_count() as u64,
            queue_depth: broker.queue_depth() as u64,
        };
        // Arming and the first picture, in that order and under one
        // acquisition of the lock: a connect between the two would
        // otherwise be in neither.
        let whole = if previous.watching {
            broker.presence().take(&mut changes)
        } else {
            broker.presence().watch();
            true
        };
        let walked = whole.then(|| {
            broker
                .sessions()
                .filter(|(_, session)| session.is_connected())
                .map(|(subscriber, session)| (subscriber, session.id().clone()))
                .collect::<HashMap<_, _>>()
        });
        (gauges, walked)
    });
    previous.watching = true;

    let examined = match walked {
        // The whole picture, because there was no usable record of
        // what changed. Everything the sampler holds and the broker
        // does not is a departure, and the other way round is an
        // arrival.
        Some(now) => {
            let examined = now.len();
            for (subscriber, id) in &now {
                if !previous.connected.contains_key(subscriber) {
                    state.events.publish(EventBody::Connected {
                        mountpoint: id.mountpoint.to_string(),
                        client_id: id.client_id.to_string(),
                    });
                }
            }
            for (subscriber, id) in &previous.connected {
                if !now.contains_key(subscriber) {
                    state.events.publish(EventBody::Disconnected {
                        mountpoint: id.mountpoint.to_string(),
                        client_id: id.client_id.to_string(),
                    });
                }
            }
            previous.connected = now;
            examined
        }
        // The ordinary tick: only what moved, and nothing was
        // touched under the lock but that.
        None => {
            let examined = changes.len();
            for change in changes {
                // A change is only an event if it is news. The record
                // carries the state a session ended up in, so a
                // client that dropped and came back between two
                // samples says "connected" to a sampler that already
                // had it connected, and that is not a connect.
                if change.connected {
                    if previous
                        .connected
                        .insert(change.subscriber, change.id.clone())
                        .is_none()
                    {
                        state.events.publish(EventBody::Connected {
                            mountpoint: change.id.mountpoint.to_string(),
                            client_id: change.id.client_id.to_string(),
                        });
                    }
                } else if previous.connected.remove(&change.subscriber).is_some() {
                    state.events.publish(EventBody::Disconnected {
                        mountpoint: change.id.mountpoint.to_string(),
                        client_id: change.id.client_id.to_string(),
                    });
                }
            }
            examined
        }
    };

    // Emitted only when something moved. A counters event every
    // interval forever would churn the ring, and a browser that
    // reconnected after a quiet minute would be told to
    // resynchronise because a thousand identical samples had pushed
    // its place out of it.
    if totals != previous.totals || gauges != previous.gauges {
        let now = Instant::now();
        state.events.publish(EventBody::Counters {
            totals,
            deltas: CounterTotals {
                bytes_received: totals.bytes_received - previous.totals.bytes_received,
                bytes_sent: totals.bytes_sent - previous.totals.bytes_sent,
                sockets_opened: totals.sockets_opened - previous.totals.sockets_opened,
            },
            gauges,
            over_ms: now.duration_since(previous.at).as_millis() as u64,
        });
        previous.at = now;
    }
    previous.totals = totals;
    previous.gauges = gauges;
    previous.attached = attached;
    examined
}

/// Sample every `interval` until the listener stops.
pub(crate) fn spawn_sampler(state: AdminState, interval: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut shutdown = state.shutdown.subscribe();
        let mut previous = Sample {
            at: Instant::now(),
            totals: CounterTotals::default(),
            gauges: Gauges::default(),
            attached: 0,
            connected: HashMap::new(),
            watching: false,
        };
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick completes immediately, which would sample a
        // broker nothing has happened to yet.
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                () = stopping(&mut shutdown) => return,
                // What it examined is for the test that says the
                // work is bounded; the loop has no use for it.
                _ = ticker.tick() => { sample(&state, &mut previous); },
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Stream as TestStream;
    use crate::{AdminConfig, AdminRunning, AdminServer};
    use broker_core::broker::BrokerConfig;
    use broker_core::registry::MAX_PENDING_PRESENCE;
    use sdmqtt_server::shared::Shared;
    use std::net::SocketAddr;

    /// How long a stream may take to say something before the test
    /// calls it a failure.
    const PATIENCE: Duration = Duration::from_secs(10);

    /// A ring over a registry nobody else is reading.
    fn ring() -> Arc<Events> {
        Arc::new(Events::new(Arc::new(Metrics::new())))
    }

    /// A connect event naming `client_id`.
    fn connected(client_id: &str) -> EventBody {
        EventBody::Connected {
            mountpoint: String::new(),
            client_id: client_id.to_owned(),
        }
    }

    /// An admin listener over a broker with no store, sampling every
    /// `interval`.
    async fn serve(interval: Duration) -> AdminRunning {
        let config = AdminConfig {
            address: "127.0.0.1:0"
                .parse::<SocketAddr>()
                .expect("a valid address"),
            ..AdminConfig::default()
        };
        let state = AdminState::new(Arc::new(Shared::new(BrokerConfig::default())))
            .with_events_interval(interval);
        AdminServer::bind(&config, state)
            .await
            .expect("loopback needs no token")
    }

    // -----------------------------------------------------------
    // The ring, without a socket in the way.
    // -----------------------------------------------------------

    #[test]
    fn a_reconnect_receives_exactly_what_it_missed_once_each_in_order() {
        let events = ring();
        let first = events.publish(connected("a"));
        let second = events.publish(connected("b"));
        let third = events.publish(connected("c"));

        let resumed = events.subscribe(Some(first));
        assert!(!resumed.resynchronise, "the ring still holds the gap");
        let ids: Vec<EventId> = resumed.backlog.iter().map(|stored| stored.id).collect();
        assert_eq!(
            ids,
            vec![second, third],
            "exactly the ones after the header, once each, in order"
        );

        // And a reader that has seen everything is owed nothing,
        // which is not the same as being owed the whole ring.
        let current = events.subscribe(Some(third));
        assert!(current.backlog.is_empty(), "{:?}", current.backlog);
        assert!(!current.resynchronise);
    }

    #[test]
    fn an_identifier_older_than_the_ring_is_answered_with_a_resynchronise() {
        let events = ring();
        // Two more than the ring holds, so the first two are gone
        // and a reader that has seen only the first has missed one
        // that cannot be replayed.
        for index in 0..RING_CAPACITY + 2 {
            events.publish(connected(&format!("client-{index}")));
        }
        let oldest_held = FIRST + 2;
        assert_eq!(events.next_id(), FIRST + RING_CAPACITY as EventId + 2);

        let resumed = events.subscribe(Some(FIRST));
        assert!(
            resumed.resynchronise,
            "the event it missed is no longer held"
        );
        assert!(
            resumed.backlog.is_empty(),
            "and it is given no partial history to read as complete: {} events",
            resumed.backlog.len()
        );

        // The boundary, because "older than the ring" off by one
        // would either resynchronise a reader that did not need it
        // or hand a gap to one that did. A reader whose very next
        // event is the oldest the ring still holds is replayed.
        let just_enough = events.subscribe(Some(oldest_held - 1));
        assert!(
            !just_enough.resynchronise,
            "a reader whose next event is still held is replayed, not reset"
        );
        assert_eq!(just_enough.backlog.len(), RING_CAPACITY);
        assert_eq!(
            just_enough.backlog.first().map(|stored| stored.id),
            Some(oldest_held)
        );
    }

    #[test]
    fn an_identifier_this_process_never_issued_is_a_restart() {
        let events = ring();
        events.publish(connected("a"));
        // A browser that was watching the broker before it restarted.
        let resumed = events.subscribe(Some(9_999));
        assert!(resumed.resynchronise);
        assert!(resumed.backlog.is_empty());
    }

    #[test]
    fn a_subscriber_with_no_header_is_given_the_live_feed_and_no_history() {
        let events = ring();
        events.publish(connected("a"));
        let fresh = events.subscribe(None);
        assert!(fresh.backlog.is_empty());
        assert!(!fresh.resynchronise);
    }

    #[test]
    fn the_backlog_and_the_live_feed_meet_exactly() {
        // The race the lock exists for: an event published after a
        // subscription must be on its receiver and not in its
        // backlog, and one published before must be the other way
        // round. Neither in both, neither in neither.
        let events = ring();
        let first = events.publish(connected("before"));
        let resumed = events.subscribe(Some(first));
        let after = events.publish(connected("after"));

        assert!(resumed.backlog.is_empty(), "nothing had happened yet");
        let mut live = resumed.live;
        let received = live.try_recv().expect("the event after the subscription");
        assert_eq!(received.id, after);
        assert!(
            live.try_recv().is_err(),
            "and nothing else; the one before was not replayed onto the feed"
        );
    }

    #[test]
    fn a_subscriber_that_falls_behind_is_counted_and_told_to_resynchronise() {
        // The test the whole ring exists for, at the level the
        // counting happens. The socket-level half is
        // `a_stream_that_falls_behind_is_counted_and_resynchronised`.
        let events = ring();
        let mut live = events.subscribe(None).live;

        // Further behind than the live channel holds.
        for index in 0..LIVE_CAPACITY + 10 {
            events.publish(connected(&format!("client-{index}")));
        }

        match live.try_recv() {
            Err(broadcast::error::TryRecvError::Lagged(lost)) => {
                assert!(lost > 0, "the channel says how many it dropped");
                events.dropped(lost);
            }
            other => panic!("expected the lag the ring exists for, got {other:?}"),
        }
        assert!(
            events.dropped_count() >= 10,
            "every dropped event is counted: {}",
            events.dropped_count()
        );
    }

    /// An identifier from another run of this process is told to
    /// resynchronise, never replayed.
    ///
    /// Identifiers start again at [`FIRST`] on a restart. A browser
    /// that was watching the old process reconnects with its last
    /// identifier, and once the new process has issued more events
    /// than that number the ring held a "gap" that was in fact another
    /// run's history, and replayed it as though it followed. So the
    /// identifier on the wire carries the run as well as the number,
    /// and a number from another run is resynchronised whatever it is.
    /// A bare number is read as this run's, which is what every
    /// identifier was before the run was part of it.
    #[test]
    fn an_identifier_from_another_run_is_told_to_resynchronise() {
        let before = Arc::new(Events::with_epoch(Arc::new(Metrics::new()), 1));
        let after = Arc::new(Events::with_epoch(Arc::new(Metrics::new()), 2));
        for index in 0..5 {
            before.publish(connected(&format!("old-{index}")));
        }
        for index in 0..10 {
            after.publish(connected(&format!("new-{index}")));
        }

        // The id the old run put on its third event, handed to the new
        // run, which has passed that number.
        let stale = after.resume(Some(&before.tagged(3)));
        assert!(stale.resynchronise, "another run's identifier is stale");
        assert!(
            stale.backlog.is_empty(),
            "and nothing of this run is dressed up as what it missed: {:?}",
            stale.backlog
        );

        // This run's own identifier resumes exactly as before.
        let resumed = after.resume(Some(&after.tagged(3)));
        assert!(!resumed.resynchronise);
        assert_eq!(resumed.backlog.len(), 7, "{:?}", resumed.backlog);
        assert_eq!(resumed.backlog[0].id, 4);

        // A bare number is this run's: the identifiers the interface
        // sent before the run was part of them.
        let plain = after.resume(Some("3"));
        assert!(!plain.resynchronise);
        assert_eq!(plain.backlog.len(), 7);

        // Unreadable is absent, as it always was.
        let fresh = after.resume(Some("not-an-id"));
        assert!(!fresh.resynchronise);
        assert!(fresh.backlog.is_empty());
        let fresh = after.resume(None);
        assert!(!fresh.resynchronise);
        assert!(fresh.backlog.is_empty());
    }

    #[test]
    fn identifiers_are_monotonic_across_a_reconnect() {
        let events = ring();
        let first = events.publish(connected("a"));
        drop(events.subscribe(Some(first)));
        let second = events.publish(connected("b"));
        drop(events.subscribe(Some(second)));
        let third = events.publish(connected("c"));

        assert_eq!(first, FIRST);
        assert!(
            second > first && third > second,
            "a reconnect does not restart the numbering: {first}, {second}, {third}"
        );
    }

    // -----------------------------------------------------------
    // Over a socket.
    // -----------------------------------------------------------

    #[tokio::test]
    async fn the_stream_says_what_a_browser_needs_and_is_never_compressed() {
        let admin = serve(Duration::from_millis(10)).await;
        let mut stream = TestStream::open(admin.address(), "/api/v1/events", None).await;
        let head = stream.head().await;

        assert_eq!(head.status, 200);
        assert_eq!(head.header("content-type"), Some("text/event-stream"));
        assert_eq!(head.header("cache-control"), Some("no-cache"));
        assert_eq!(
            head.header("x-accel-buffering"),
            Some("no"),
            "section 11.1: intermediaries are told not to buffer it"
        );
        assert_eq!(
            head.header("content-encoding"),
            None,
            "a live stream is never compressed"
        );

        // The retry interval is set rather than left to the
        // browser's three seconds.
        let opening = stream.until(PATIENCE, |seen| seen.contains("retry:")).await;
        assert!(
            opening.contains(&format!("retry: {}", RETRY.as_millis())),
            "{opening}"
        );

        stream.close().await;
        admin.shutdown().await;
    }

    #[tokio::test]
    async fn a_connect_over_mqtt_reaches_the_stream() {
        // Convention 5: an event published by the test into the ring
        // proves the ring. This proves there is a source.
        use sdmqtt_server::listener::{Server, ServerConfig};

        let mqtt = Server::bind(ServerConfig::ephemeral())
            .await
            .expect("the MQTT listener binds");
        let config = AdminConfig {
            address: "127.0.0.1:0"
                .parse::<SocketAddr>()
                .expect("a valid address"),
            ..AdminConfig::default()
        };
        let admin = AdminServer::bind(
            &config,
            AdminState::new(Arc::clone(mqtt.shared()))
                .with_events_interval(Duration::from_millis(10)),
        )
        .await
        .expect("loopback needs no token");

        let mut stream = TestStream::open(admin.address(), "/api/v1/events", None).await;
        stream.head().await;

        let client = crate::testing::mqtt_connect(mqtt.address(), "watched").await;
        let seen = stream
            .until(PATIENCE, |seen| seen.contains("event: connected"))
            .await;
        assert!(seen.contains("\"client_id\":\"watched\""), "{seen}");

        drop(client);
        let seen = stream
            .until(PATIENCE, |seen| seen.contains("event: disconnected"))
            .await;
        assert!(seen.contains("\"client_id\":\"watched\""), "{seen}");

        stream.close().await;
        admin.shutdown().await;
        mqtt.shutdown().await;
    }

    #[tokio::test]
    async fn a_reconnect_over_a_socket_receives_what_it_missed() {
        let admin = serve(Duration::from_secs(3_600)).await;
        let events = Arc::clone(admin.events());
        let first = events.publish(connected("a"));
        let second = events.publish(connected("b"));

        let mut stream =
            TestStream::open(admin.address(), "/api/v1/events", Some(&first.to_string())).await;
        stream.head().await;
        let seen = stream
            .until(PATIENCE, |seen| {
                seen.contains(&format!("id: {}", events.tagged(second)))
            })
            .await;
        assert!(
            !seen.contains("event: resynchronise"),
            "the ring held it: {seen}"
        );
        assert!(!seen.contains("\"client_id\":\"a\""), "once each: {seen}");

        // And the live feed continues from there.
        let third = events.publish(connected("c"));
        let seen = stream
            .until(PATIENCE, |seen| {
                seen.contains(&format!("id: {}", events.tagged(third)))
            })
            .await;
        assert!(seen.contains("\"client_id\":\"c\""), "{seen}");

        stream.close().await;
        admin.shutdown().await;
    }

    #[tokio::test]
    async fn a_reconnect_past_the_ring_is_told_to_resynchronise_over_a_socket() {
        let admin = serve(Duration::from_secs(3_600)).await;
        let events = Arc::clone(admin.events());
        for index in 0..RING_CAPACITY + 2 {
            events.publish(connected(&format!("client-{index}")));
        }

        let mut stream =
            TestStream::open(admin.address(), "/api/v1/events", Some(&FIRST.to_string())).await;
        stream.head().await;
        let seen = stream
            .until(PATIENCE, |seen| seen.contains("event: resynchronise"))
            .await;
        assert!(
            !seen.contains("event: connected"),
            "no gap dressed up as a history: {seen}"
        );

        stream.close().await;
        admin.shutdown().await;
    }

    /// Over a socket: every `id:` carries the run, this run's resumes,
    /// and a previous run's is resynchronised even though this run has
    /// passed its number.
    #[tokio::test]
    async fn an_identifier_from_before_a_restart_is_resynchronised_over_a_socket() {
        let admin = serve(Duration::from_secs(3_600)).await;
        let events = Arc::clone(admin.events());
        for index in 0..3 {
            events.publish(connected(&format!("client-{index}")));
        }

        let mut stream = TestStream::open(
            admin.address(),
            "/api/v1/events",
            Some(&events.tagged(FIRST)),
        )
        .await;
        stream.head().await;
        let seen = stream
            .until(PATIENCE, |seen| seen.contains("client-2"))
            .await;
        assert!(
            seen.contains(&format!("id: {}", events.tagged(FIRST + 1))),
            "the identifier on the wire carries the run: {seen}"
        );
        assert!(
            !seen.contains("client-0"),
            "resumed after the first: {seen}"
        );
        stream.close().await;

        // What a browser that watched the previous process sends: the
        // first identifier, tagged with a run this is not.
        let previous = format!("{}-{FIRST}", events.epoch().wrapping_add(1));
        let mut stream = TestStream::open(admin.address(), "/api/v1/events", Some(&previous)).await;
        stream.head().await;
        let seen = stream
            .until(PATIENCE, |seen| seen.contains("event: resynchronise"))
            .await;
        assert!(
            !seen.contains("event: connected"),
            "another run's history was replayed as this run's: {seen}"
        );
        stream.close().await;
        admin.shutdown().await;
    }

    #[tokio::test]
    async fn a_stream_that_falls_behind_is_counted_and_resynchronised() {
        // The one the ring exists for, over a socket that is not
        // read. Nothing here drains the response, so the frames back
        // up, the pump stops taking from the broadcast channel, and
        // the reader falls behind for real.
        let admin = serve(Duration::from_secs(3_600)).await;
        let events = Arc::clone(admin.events());

        let mut stream = TestStream::open(admin.address(), "/api/v1/events", None).await;
        // The subscription is taken in the handler before the head
        // is written, so by the time this returns the reader exists
        // and everything below is published behind it.
        stream.head().await;

        // Far more than the frame channel, the socket's own buffers
        // and the live channel hold together, with payloads big
        // enough that they cannot all be swallowed by the kernel.
        // Nothing reads the socket while this runs, which is what
        // makes the reader fall behind for real rather than by being
        // told it has.
        for index in 0..(LIVE_CAPACITY + PENDING_FRAMES) * 8 {
            events.publish(connected(&format!("{index:0>512}")));
        }

        // Now drain it. The frames already handed over come first,
        // then the lag, and the lag is what this is looking for.
        let seen = stream
            .until(PATIENCE, |seen| seen.contains("event: resynchronise"))
            .await;
        assert!(
            seen.contains("\"this stream fell behind"),
            "it is told why: {}",
            &seen[seen.len().saturating_sub(400)..]
        );
        assert!(
            events.dropped_count() > 0,
            "every dropped event is counted: {}",
            events.dropped_count()
        );
        assert!(
            admin.events().dropped_count()
                <= u64::try_from((LIVE_CAPACITY + PENDING_FRAMES) * 8).expect("a small number"),
            "and not more than were published"
        );

        stream.close().await;
        admin.shutdown().await;
    }

    /// A client that goes away takes its stream's task with it.
    ///
    /// **The path development cannot exercise.** `services/web`'s
    /// readme records what was measured about the development
    /// server; whatever a proxy does, this is the guard that says
    /// the broker end is right, and it runs against the broker
    /// directly rather than through anything.
    ///
    /// **The cleanup is discovered by writing, not by waiting.** A
    /// closed socket is a half close: the server may still write to
    /// it, and only the peer's reset on that write ends the
    /// connection. So the task does not notice a departed browser
    /// until it has frames to send, and it takes more than one — the
    /// first is written to a socket that has not complained yet. The
    /// loop below publishes until it notices, which is what the
    /// sampler does for real every second. A broker with a browser
    /// that left and nothing happening since keeps the task, which
    /// is a wait bounded by the next event rather than a leak.
    #[tokio::test]
    async fn a_client_that_goes_away_ends_its_stream() {
        // An interval long enough that the sampler cannot be what
        // ends this, so the only thing that can is the client
        // leaving.
        let admin = serve(Duration::from_secs(3600)).await;
        let events = Arc::clone(admin.events());
        assert_eq!(events.live_readers(), 0, "nothing is watching yet");

        let mut stream = TestStream::open(admin.address(), "/api/v1/events", None).await;
        assert_eq!(stream.head().await.status, 200);
        let deadline = tokio::time::Instant::now() + PATIENCE;
        while events.live_readers() == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the stream never registered a reader"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(events.live_readers(), 1, "exactly one, for the one socket");

        drop(stream);

        let deadline = tokio::time::Instant::now() + PATIENCE;
        let mut published = 0;
        while events.live_readers() > 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the stream's task outlived the client that asked for it by \
                 {published} events, so every tab a person closes leaves one \
                 behind"
            );
            events.publish(connected("after-the-browser-left"));
            published += 1;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        admin.shutdown().await;
    }

    #[tokio::test]
    async fn the_stream_ends_when_the_listener_stops() {
        // Asserted as the server task finishing, not as the stream
        // having produced something: a stream that never ends still
        // produces plenty.
        let admin = serve(Duration::from_secs(3_600)).await;
        let mut stream = TestStream::open(admin.address(), "/api/v1/events", None).await;
        stream.head().await;

        tokio::time::timeout(PATIENCE, admin.shutdown())
            .await
            .expect(
                "graceful shutdown waits for every open connection, so a stream \
                 that does not end on the signal never lets the broker exit",
            );

        // And the body ends rather than being cut mid-frame with the
        // socket left open.
        let ended = tokio::time::timeout(PATIENCE, stream.drain())
            .await
            .expect("the body ends");
        assert!(ended.contains("retry:"), "it was a real stream: {ended}");
    }

    // -----------------------------------------------------------
    // What a sample costs
    // -----------------------------------------------------------

    /// A broker holding `population` connected sessions.
    fn populated(population: usize) -> AdminState {
        let state = AdminState::new(Arc::new(Shared::new(BrokerConfig::default())));
        for n in 0..population {
            attach(&state, n);
        }
        state
    }

    /// Connect one more client the way a connection does.
    fn attach(state: &AdminState, n: usize) {
        let connect = mqtt_codec::packet::Connect {
            version: mqtt_codec::Version::V5,
            client_id: format!("client-{n}"),
            clean_start: true,
            keep_alive: 0,
            username: None,
            password: None,
            will: None,
            properties: mqtt_codec::Properties::default(),
        };
        state.shared.with_broker(|broker| {
            let mut out = Vec::new();
            broker
                .connect("", &connect, &mut out)
                .expect("the default decision authenticates");
        });
        // What a real connection moves as it arrives, and what the
        // counters event is drawn from.
        state.shared.metrics().socket_opened().inc();
    }

    /// The sample a fresh sampler starts from.
    fn first_sample() -> Sample {
        Sample {
            at: Instant::now(),
            totals: CounterTotals::default(),
            gauges: Gauges::default(),
            attached: 0,
            connected: HashMap::new(),
            watching: false,
        }
    }

    /// Take the connection away from client `n`, the way a socket
    /// closing does.
    fn detach(state: &AdminState, n: usize) {
        let id = SessionId::new("", &format!("client-{n}"));
        let actions = state.shared.with_broker(|broker| {
            let mut out = Vec::new();
            if let Some(subscriber) = broker.subscriber_of(&id) {
                broker.disconnect(subscriber, &mut out);
            }
            out
        });
        drop(actions);
    }

    /// Every event the ring still holds, as `kind:client_id`.
    fn published(state: &AdminState) -> Vec<String> {
        state
            .events
            .subscribe(Some(FIRST - 1))
            .backlog
            .iter()
            .map(|stored| match stored.body.as_ref() {
                EventBody::Connected { client_id, .. } => format!("connected:{client_id}"),
                EventBody::Disconnected { client_id, .. } => format!("disconnected:{client_id}"),
                EventBody::Counters { .. } | EventBody::Resynchronise { .. } => {
                    "counters".to_owned()
                }
            })
            .filter(|line| line != "counters")
            .collect()
    }

    /// One sample reads what moved and not the population.
    ///
    /// Convention 5, and the shape
    /// `removing_costs_the_same_however_many_are_armed` uses: the
    /// assertion is in sessions examined rather than in
    /// microseconds, so it is a statement about the algorithm and
    /// does not flake on a loaded machine. The walk this replaced
    /// would read the whole population here, so the numbers below
    /// would be 10,000 rather than 1.
    #[test]
    fn one_sample_reads_only_what_changed() {
        for population in [1usize, 100, 10_000] {
            let state = populated(population);
            let mut previous = first_sample();

            // The first sample of all has no record to read, so it
            // takes the whole picture — and arms the record.
            assert_eq!(
                sample(&state, &mut previous),
                population,
                "the first sample learns the population"
            );

            // Everything after it reads only what moved.
            attach(&state, population);
            assert_eq!(
                sample(&state, &mut previous),
                1,
                "one connect with {population} sessions on the broker"
            );
            detach(&state, population);
            assert_eq!(
                sample(&state, &mut previous),
                1,
                "one disconnect with {population} sessions on the broker"
            );
            assert_eq!(
                sample(&state, &mut previous),
                0,
                "and nothing at all when nothing happened"
            );
        }
    }

    /// The events a sample publishes are the ones that happened.
    ///
    /// The bounded-work test above would pass just as well if the
    /// sampler read the record and threw it away, and the whole
    /// point of the record is the events it produces.
    #[test]
    fn a_connect_and_a_disconnect_are_each_reported_once() {
        let state = populated(3);
        let mut previous = first_sample();
        sample(&state, &mut previous);
        assert_eq!(
            published(&state).len(),
            3,
            "the three that were already connected"
        );

        attach(&state, 3);
        sample(&state, &mut previous);
        detach(&state, 3);
        sample(&state, &mut previous);
        // And a sample over which nothing happened says nothing.
        sample(&state, &mut previous);

        let seen = published(&state);
        assert_eq!(
            &seen[3..],
            &["connected:client-3", "disconnected:client-3"],
            "once each, in order: {seen:?}"
        );
    }

    /// A client that leaves and comes back between two samples is
    /// not news.
    ///
    /// The record keeps the state a session ended up in, not a
    /// history, and the walk this replaced could not see the middle
    /// of an interval either. What must not happen is a second
    /// `connected` for a client the page already shows as connected,
    /// which is a row the Clients table would then hold twice.
    #[test]
    fn a_flap_inside_one_interval_is_not_reported_twice() {
        let state = populated(1);
        let mut previous = first_sample();
        sample(&state, &mut previous);
        let before = published(&state).len();

        detach(&state, 0);
        attach(&state, 0);
        sample(&state, &mut previous);
        assert_eq!(
            published(&state).len(),
            before,
            "it ended the interval exactly as it started it: {:?}",
            published(&state)
        );

        // And the sampler still holds it, so its departure is still
        // reported when it really goes.
        detach(&state, 0);
        sample(&state, &mut previous);
        assert_eq!(
            published(&state).last().map(String::as_str),
            Some("disconnected:client-0"),
            "{:?}",
            published(&state)
        );
    }

    /// A sampler that stopped reading is given the whole picture
    /// rather than a record with holes in it.
    ///
    /// Convention 5's "a path no test walks": the overflow branch is
    /// reached only after [`MAX_PENDING_PRESENCE`] changes nobody
    /// read, which no ordinary test produces and a real broker
    /// reaches only if this task has stopped. Deleting the branch
    /// leaves a sampler whose set has quietly lost entries and whose
    /// pages therefore show clients that are not there.
    #[test]
    fn a_record_that_overflowed_makes_the_next_sample_walk() {
        let state = populated(1);
        let mut previous = first_sample();
        sample(&state, &mut previous);

        // Past the bound, with nobody reading in between.
        for n in 0..MAX_PENDING_PRESENCE + 1 {
            attach(&state, 1_000_000 + n);
        }
        // The walk sees every one of them, rather than the handful
        // the record had room for.
        let before = state.events.next_id();
        let examined = sample(&state, &mut previous);
        assert_eq!(
            examined,
            MAX_PENDING_PRESENCE + 2,
            "the whole population, because the record could not be trusted"
        );
        // And every one of them is reported. Counted as identifiers
        // issued rather than read out of the ring, which holds only
        // its last thousand: one connect each, and one counters
        // event.
        assert_eq!(
            state.events.next_id() - before,
            MAX_PENDING_PRESENCE as EventId + 2,
            "one event for every arrival the record lost, and the counters"
        );
    }

    /// What one sample costs under the broker's one lock, at the two
    /// populations the Clients page was built for.
    ///
    /// **A measurement, not an assertion**, which is why it is
    /// ignored: a wall clock on a shared machine is not a thing to
    /// gate a build on. The assertion beside it is
    /// `one_sample_reads_only_what_changed`, which counts sessions
    /// examined rather than microseconds, exactly as
    /// `removing_costs_the_same_however_many_are_armed` counts
    /// entries touched.
    ///
    ///     cargo test -p sdmqtt-admin --release -- --ignored --nocapture what_one_sample_costs
    #[test]
    #[ignore = "a measurement, not an assertion"]
    fn what_one_sample_costs() {
        for population in [10_000usize, 100_000] {
            let state = populated(population);
            let mut previous = first_sample();
            // The first sample of all is the one that learns the
            // population; every one after it is the steady state,
            // which is what this measures.
            sample(&state, &mut previous);

            let runs = 20u32;
            let mut worst = Duration::ZERO;
            let mut total = Duration::ZERO;
            for run in 0..runs as usize {
                // One connect a second is the normal state of a
                // broker this size, and it is what arms the walk.
                attach(&state, population + run);
                let started = Instant::now();
                sample(&state, &mut previous);
                let took = started.elapsed();
                worst = worst.max(took);
                total += took;
            }
            println!(
                "{population} sessions: {:?} mean, {:?} worst, over {runs} samples",
                total / runs,
                worst
            );
        }
    }

    /// What one sample costs on a broker that also holds
    /// subscriptions and retained messages.
    ///
    /// The measurement above isolates the thing this module changed,
    /// so its broker holds nothing else. This one is the whole tick,
    /// and it is here because two of the five gauges once were not
    /// the maintained numbers the sampler's documentation claimed:
    /// `subscription_count` walked the subscription trie and
    /// `retained_count` walked the retained tree, both on this lock,
    /// both on every tick, and the metrics endpoint asked for the
    /// same two. Both are maintained now. The measurement stays,
    /// because a gauge that goes back to walking is a change nothing
    /// else here would notice — every answer would still be right.
    ///
    ///     cargo test -p sdmqtt-admin --release -- --ignored --nocapture what_a_whole_sample_costs
    #[test]
    #[ignore = "a measurement, not an assertion"]
    fn what_a_whole_sample_costs() {
        for population in [10_000usize, 100_000] {
            let state = populated(population);
            // One subscription each, and a retained message per ten
            // sessions.
            for n in 0..population {
                subscribe(&state, n, &format!("site/{}/device/{n}/telemetry", n % 100));
            }
            for n in 0..population / 10 {
                retain(&state, &format!("site/{}/device/{n}/state", n % 100));
            }

            let mut previous = first_sample();
            sample(&state, &mut previous);

            let runs = 20u32;
            let mut worst = Duration::ZERO;
            let mut total = Duration::ZERO;
            for run in 0..runs as usize {
                attach(&state, population + run);
                let started = Instant::now();
                sample(&state, &mut previous);
                let took = started.elapsed();
                worst = worst.max(took);
                total += took;
            }
            println!(
                "{population} sessions, {population} subscriptions, {} retained: {:?} mean, {:?} worst",
                population / 10,
                total / runs,
                worst
            );
        }
    }

    /// Subscribe client `n` to `filter`, through the packet path.
    fn subscribe(state: &AdminState, n: usize, filter: &str) {
        let id = SessionId::new("", &format!("client-{n}"));
        state.shared.with_broker(|broker| {
            let Some(subscriber) = broker.subscriber_of(&id) else {
                return;
            };
            let packet = mqtt_codec::Packet::Subscribe(mqtt_codec::packet::Subscribe {
                packet_id: 1,
                filters: vec![mqtt_codec::packet::SubscriptionOptions {
                    filter: filter.to_owned(),
                    qos: mqtt_codec::QoS::AtMostOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: mqtt_codec::packet::RetainHandling::SendAlways,
                }],
                properties: mqtt_codec::Properties::default(),
            });
            let mut out = Vec::new();
            let _ = broker.handle(subscriber, &packet, &mut out);
        });
    }

    /// Retain a small message on `topic`.
    fn retain(state: &AdminState, topic: &str) {
        state.shared.with_broker(|broker| {
            broker.retained().set(
                topic,
                broker_core::retained::RetainedMessage {
                    payload: bytes::Bytes::from_static(b"21.5"),
                    qos: mqtt_codec::QoS::AtMostOnce,
                    expires_at: None,
                    properties: Arc::new(mqtt_codec::Properties::default().into()),
                },
            );
        });
    }
}
