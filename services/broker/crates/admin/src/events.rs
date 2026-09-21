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

use std::collections::{HashSet, VecDeque};
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
        // believes is from another run.
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

/// What a new subscriber is handed.
#[derive(Debug)]
pub struct Subscription {
    /// The events it missed, oldest first. Empty when it missed none
    /// and when it missed too many.
    pub backlog: Vec<Stored>,
    /// Whether it must refetch instead of trusting the backlog.
    pub resynchronise: bool,
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
}

impl Events {
    /// An empty ring over `metrics`.
    pub fn new(metrics: Arc<Metrics>) -> Self {
        let (live, _) = broadcast::channel(LIVE_CAPACITY);
        Self {
            ring: Mutex::new(Ring {
                next: FIRST,
                held: VecDeque::new(),
            }),
            live,
            dropped: AtomicU64::new(0),
            metrics,
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
fn frame(stored: &Stored) -> SseEvent {
    SseEvent::default()
        .id(stored.id.to_string())
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
        mut live,
    } = subscription;

    if !hand_over(&frames, opening(), &mut shutdown).await {
        return;
    }
    if stale
        && !hand_over(
            &frames,
            resynchronise("the events you missed are no longer held"),
            &mut shutdown,
        )
        .await
    {
        return;
    }
    for stored in backlog {
        if !hand_over(&frames, frame(&stored), &mut shutdown).await {
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
                if !hand_over(&frames, frame(&stored), &mut shutdown).await {
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
fn last_event_id(headers: &HeaderMap) -> Option<EventId> {
    headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse().ok())
}

/// The events stream.
async fn events(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    let subscription = state.events.subscribe(last_event_id(&headers));
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
    connected: HashSet<SessionId>,
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
/// The gauges are five numbers the broker already maintains, so the
/// ordinary tick is O(1) under the lock. Working out *who* connected
/// is not: it is a walk of every session, and the Clients page exists
/// because there may be a hundred thousand of them.
///
/// So the walk is behind a tripwire that is itself O(1). A session
/// cannot attach without `sockets_opened` moving, and cannot detach
/// without the outbox count moving, so when neither has moved since
/// the last sample there is nothing to diff and the walk is skipped
/// entirely. An idle broker pays five atomic loads a second.
fn sample(state: &AdminState, previous: &mut Sample) {
    let metrics = state.shared.metrics();
    let totals = CounterTotals {
        bytes_received: metrics.bytes_received().get(),
        bytes_sent: metrics.bytes_sent().get(),
        sockets_opened: metrics.socket_opened().get(),
    };
    let attached = state.shared.outbox_count();
    // The tripwire. `sockets_opened` only rises, so a connect always
    // moves it; a disconnect with no connect beside it always moves
    // the outbox count. Neither moving means nobody arrived and
    // nobody left.
    let walk = totals.sockets_opened != previous.totals.sockets_opened
        || attached != previous.attached
        // The first sample of all, so that clients already connected
        // when this listener started are not invisible until the
        // next one moves.
        || previous.connected.is_empty() && attached > 0;

    let (gauges, now) = state.shared.with_broker(|broker| {
        let gauges = Gauges {
            connections: attached as u64,
            sessions: broker.session_count() as u64,
            subscriptions: broker.subscription_count() as u64,
            retained: broker.retained_count() as u64,
            queue_depth: broker.queue_depth() as u64,
        };
        let now = walk.then(|| {
            broker
                .sessions()
                .filter(|(_, session)| session.is_connected())
                .map(|(_, session)| session.id().clone())
                .collect::<HashSet<_>>()
        });
        (gauges, now)
    });

    if let Some(now) = now {
        for arrived in now.difference(&previous.connected) {
            state.events.publish(EventBody::Connected {
                mountpoint: arrived.mountpoint.to_string(),
                client_id: arrived.client_id.to_string(),
            });
        }
        for left in previous.connected.difference(&now) {
            state.events.publish(EventBody::Disconnected {
                mountpoint: left.mountpoint.to_string(),
                client_id: left.client_id.to_string(),
            });
        }
        previous.connected = now;
    }

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
            connected: HashSet::new(),
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
                _ = ticker.tick() => sample(&state, &mut previous),
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
            .until(PATIENCE, |seen| seen.contains(&format!("id: {second}")))
            .await;
        assert!(
            !seen.contains("event: resynchronise"),
            "the ring held it: {seen}"
        );
        assert!(!seen.contains("\"client_id\":\"a\""), "once each: {seen}");

        // And the live feed continues from there.
        let third = events.publish(connected("c"));
        let seen = stream
            .until(PATIENCE, |seen| seen.contains(&format!("id: {third}")))
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
}
