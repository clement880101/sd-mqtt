//! The read resources the first three interface pages need.
//!
//! Nodes, listeners, clients, sessions, subscriptions and retained
//! messages. Rules, rule runs, dead letters, users, access control
//! rules and API keys are later milestones and are **not served at
//! all** — not as empty collections, which an interface would read as
//! "you have no rules" rather than "this build has no rules".
//!
//! # Nothing is serialised under the broker lock
//!
//! This is the shape to read the module for, because the obvious code
//! does the opposite.
//!
//! The broker's methods take an exclusive reference, so every packet
//! in this process passes through one lock; `Shared` documents that
//! it is the bottleneck at the connection counts this design targets.
//! A request listing a hundred thousand sessions that formatted JSON
//! while holding it would stall every connection in the process for
//! as long as the formatting took, and the Clients page exists
//! precisely to list a great many clients.
//!
//! So each handler does three things in this order, and the order is
//! the point:
//!
//! 1. **Under the lock**, choose the page and copy out the fields it
//!    needs, into owned values that borrow nothing from the broker.
//! 2. Release the lock, which happens when the closure returns
//!    because [`Shared::with_broker`] is a synchronous function whose
//!    guard is one of its locals.
//! 3. **Outside it**, serialise. The handler never calls a serialiser
//!    at all: it returns [`axum::Json`] of an owned value, and axum
//!    formats it in `IntoResponse` long after the closure returned.
//!    There is nowhere in a handler for a serialiser to be called
//!    from, which is a stronger guarantee than remembering not to.
//!
//! Step one is bounded too, which is more than the rule asks for. The
//! selection keeps a heap of at most one page, so the work done under
//! the lock is one cheap comparison per session plus one row built
//! per row returned — not one row built per session in the broker.
//! Without that, a page of ten out of a hundred thousand sessions
//! would allocate a hundred thousand rows under the lock and throw
//! away all but ten.
//!
//! **How much of this a test can see, honestly.** The bounded
//! selection is testable and is tested: a broker with far more
//! sessions than a page still answers a page, and removing the cap
//! fails two tests. Where the serialising happens is **not**
//! testable, and this was checked rather than assumed: the sessions
//! handler was rewritten to format its whole body inside the closure
//! and the entire suite still passed.
//!
//! That is a real gap and convention 7 says to report it rather than
//! to pretend otherwise. It is also not closable by a test. Any
//! correct implementation takes the lock at least once during the
//! request, so a test asking "was the lock held during this request"
//! says yes either way; the only thing separating the two is how
//! *long* it was held, and a test asserting a duration is
//! convention 5's "test measuring the wrong dimension" — flaky on a
//! loaded machine, and green on a fast one with the bug present.
//!
//! What guards it instead is the shape. A handler here returns
//! `Json(owned)` and calls no serialiser at all, so the mutation is
//! not an edit to a condition but a rewrite of the handler's return
//! type and body, which a reviewer sees. Where that is not enough —
//! a hundred thousand sessions and a real device — the thing to
//! measure is the contention the benchmark in `server` already
//! measures, not a unit test here.
//!
//! # Pagination
//!
//! Everything that can be large is paginated, which here is
//! everything, because one envelope for six resources is one thing
//! for the interface to learn.
//!
//! The cursor is **opaque**: a client passes back exactly what `next`
//! gave it and reads nothing into it. It happens to be the ordering
//! key of the last row of the previous page, which is what makes a
//! page stable while rows are being inserted and removed underneath
//! it — an offset would skip a row whenever one before it was
//! removed, and repeat one whenever one was added.
//!
//! [`Shared::with_broker`]: sdmqtt_server::shared::Shared::with_broker

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::net::SocketAddr;

use axum::extract::{FromRequestParts, Query, State};
use axum::http::request::Parts;
use axum::routing::get;
use axum::{Json, Router};
use broker_core::broker::Broker;
use broker_core::retained::RetainedMessage;
use mqtt_codec::{QoS, Version};
use serde::{Deserialize, Serialize};
use storage::record::{Qos, RetainHandling};

use crate::error::{ApiError, ErrorCode};
use crate::AdminState;

/// How many rows a page holds when the request does not say.
pub const DEFAULT_LIMIT: usize = 100;

/// The most a page may hold.
///
/// Refused rather than clamped. A client that asks for ten thousand
/// and silently gets a thousand has no way to tell that from a broker
/// holding a thousand, and will read the short page as the end of the
/// collection.
pub const MAX_LIMIT: usize = 1_000;

/// How many parts an ordering key has.
///
/// Three, because the widest resource here is a subscription, which
/// is identified by its mountpoint, its client and its filter. A
/// resource with fewer leaves the rest empty, and the empty string
/// sorts before everything, so a shorter key orders the way a reader
/// would expect.
const PARTS: usize = 3;

/// What separates the parts of a cursor.
///
/// A unit separator, which is a control character: legal in a URL
/// once percent-encoded, and not something a topic filter or a client
/// identifier contains in practice. "In practice" is not good enough
/// on its own — MQTT allows very nearly any UTF-8 in a client
/// identifier — so the parts are escaped as well, and one that does
/// contain it round trips.
const SEPARATOR: char = '\u{1f}';

/// The key a resource is ordered and paginated by.
///
/// Borrowed, deliberately. It is built once per row *under the lock*,
/// and an owned key would be up to three string allocations per
/// session in the broker rather than per row returned.
type Key<'a> = [&'a str; PARTS];

/// One page of anything.
///
/// The same envelope for all six resources.
#[derive(Debug, Serialize)]
pub struct Page<T> {
    /// This page's rows, in the resource's order.
    pub items: Vec<T>,
    /// How many rows the whole resource holds, not how many are on
    /// this page. The Clients page shows a count beside a virtualized
    /// table and would otherwise have to walk every page to find it.
    pub total: usize,
    /// The cursor for the next page, or `null` when this is the last
    /// one.
    ///
    /// `null` here and a full page are not a contradiction: a
    /// collection of exactly one page's worth ends on a full page,
    /// and saying otherwise would send a client after an empty one.
    pub next: Option<String>,
}

/// What a query string may say.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageQuery {
    limit: Option<usize>,
    after: Option<String>,
}

/// A validated `limit` and `after`.
///
/// An extractor of its own rather than `Query<PageQuery>`, so that a
/// query string that will not parse produces the one error shape
/// instead of axum's plain-text rejection. Section 13 and
/// [`crate::error`]: one shape for every error the API returns.
#[derive(Debug)]
struct Paging {
    limit: usize,
    after: Option<[String; PARTS]>,
}

impl<S: Send + Sync> FromRequestParts<S> for Paging {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(query) = Query::<PageQuery>::from_request_parts(parts, state)
            .await
            .map_err(|rejection| {
                ApiError::invalid_request(format!("the query string is not usable: {rejection}"))
            })?;

        let limit = match query.limit {
            None => DEFAULT_LIMIT,
            // Zero is refused rather than read as "the default" or as
            // "everything". Both readings are arguable, which is the
            // argument for making the caller say which it meant.
            Some(limit) if limit == 0 || limit > MAX_LIMIT => {
                return Err(ApiError::in_field(
                    ErrorCode::InvalidRequest,
                    "limit",
                    format!("limit must be between 1 and {MAX_LIMIT}, not {limit}"),
                ))
            }
            Some(limit) => limit,
        };

        let after = query.after.as_deref().map(decode_cursor).transpose()?;
        Ok(Self { limit, after })
    }
}

/// Render an ordering key as a cursor.
fn encode_cursor(key: Key<'_>) -> String {
    let mut out = String::new();
    for (index, part) in key.iter().enumerate() {
        if index > 0 {
            out.push(SEPARATOR);
        }
        for character in part.chars() {
            match character {
                // The escape character first, or unescaping is
                // ambiguous.
                '%' => out.push_str("%25"),
                SEPARATOR => out.push_str("%1F"),
                other => out.push(other),
            }
        }
    }
    out
}

/// Read a cursor back, refusing one this API did not write.
fn decode_cursor(cursor: &str) -> Result<[String; PARTS], ApiError> {
    let mut parts = cursor.split(SEPARATOR);
    let mut out: [String; PARTS] = Default::default();
    for slot in &mut out {
        let Some(part) = parts.next() else {
            return Err(bad_cursor());
        };
        *slot = unescape(part)?;
    }
    if parts.next().is_some() {
        return Err(bad_cursor());
    }
    Ok(out)
}

/// Undo [`encode_cursor`]'s escaping.
fn unescape(part: &str) -> Result<String, ApiError> {
    let mut out = String::with_capacity(part.len());
    let mut characters = part.chars();
    while let Some(character) = characters.next() {
        if character != '%' {
            out.push(character);
            continue;
        }
        let escape: String = characters.by_ref().take(2).collect();
        match escape.to_ascii_uppercase().as_str() {
            "25" => out.push('%'),
            "1F" => out.push(SEPARATOR),
            _ => return Err(bad_cursor()),
        }
    }
    Ok(out)
}

/// One sentence for every way a cursor can be wrong.
///
/// The parts of it a client could distinguish — too few separators,
/// an escape we did not write — are all the same mistake, which is
/// having made a cursor up rather than passing back the one it was
/// given.
fn bad_cursor() -> ApiError {
    ApiError::in_field(
        ErrorCode::InvalidRequest,
        "after",
        "after must be a cursor this API returned in a page's `next`",
    )
}

/// One row, with the key it is ordered by.
struct Chosen<'a, T> {
    key: Key<'a>,
    value: T,
}

// Ordered by the key alone. The value is whatever the caller needs to
// build a row and has no order of its own.
impl<T> Ord for Chosen<'_, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}
impl<T> PartialOrd for Chosen<'_, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> PartialEq for Chosen<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl<T> Eq for Chosen<'_, T> {}

/// What one pass over a resource found.
struct Selection<'a, T> {
    /// The page, in ascending key order.
    chosen: Vec<Chosen<'a, T>>,
    /// Everything the resource holds.
    total: usize,
    /// Everything after the cursor, which is how `next` is decided
    /// without a second pass.
    remaining: usize,
}

/// Take at most `limit` rows after `after`, in key order.
///
/// **This runs under the broker lock**, so what it does per item
/// matters. It is one key comparison and, for at most `limit` of
/// them, a push into a heap that never grows past `limit`. Sorting
/// the whole resource, or collecting it and slicing, would do work
/// proportional to the broker's size on every request for ten rows.
fn select<'a, T>(
    items: impl Iterator<Item = (Key<'a>, T)>,
    after: Option<&[String; PARTS]>,
    limit: usize,
) -> Selection<'a, T> {
    let mut heap: BinaryHeap<Chosen<'a, T>> = BinaryHeap::with_capacity(limit + 1);
    let mut total = 0;
    let mut remaining = 0;
    for (key, value) in items {
        total += 1;
        if after.is_some_and(|after| !is_after(&key, after)) {
            continue;
        }
        remaining += 1;
        heap.push(Chosen { key, value });
        // A max-heap capped at the page size holds the `limit`
        // smallest keys seen so far.
        if heap.len() > limit {
            heap.pop();
        }
    }
    Selection {
        chosen: heap.into_sorted_vec(),
        total,
        remaining,
    }
}

/// Whether `key` sorts strictly after `cursor`.
///
/// Strictly, so the row the cursor names is not returned twice.
fn is_after(key: &Key<'_>, cursor: &[String; PARTS]) -> bool {
    key.iter()
        .copied()
        .cmp(cursor.iter().map(String::as_str))
        .is_gt()
}

/// Turn a selection into a page, building each row.
///
/// `row` runs once per returned row and not once per row in the
/// broker, which is the whole reason [`select`] exists.
fn page<'a, T, R>(selection: Selection<'a, T>, row: impl Fn(Key<'a>, T) -> R) -> Page<R> {
    let Selection {
        chosen,
        total,
        remaining,
    } = selection;
    // A cursor only when something is actually left. A page that is
    // full but final must say `next: null`, or a client walks into an
    // empty page and cannot tell it from a collection that shrank.
    let next = (remaining > chosen.len())
        .then(|| chosen.last().map(|last| encode_cursor(last.key)))
        .flatten();
    Page {
        items: chosen
            .into_iter()
            .map(|Chosen { key, value }| row(key, value))
            .collect(),
        total,
        next,
    }
}

// ---------------------------------------------------------------
// Nodes
// ---------------------------------------------------------------

/// A node in this cluster.
#[derive(Debug, Serialize)]
pub struct NodeView {
    /// What this node is called.
    pub id: String,
    /// Whether it is the one answering.
    pub this_node: bool,
    /// `up`. Membership is milestone 5; until then the only node is
    /// the one answering, and it is up by virtue of answering.
    pub state: &'static str,
    /// The build serving this API.
    pub version: &'static str,
    /// How long it has been serving.
    pub uptime_seconds: u64,
}

async fn nodes(State(state): State<AdminState>, paging: Paging) -> Json<Page<NodeView>> {
    // Not read under the broker lock, because none of it is the
    // broker's. Paginated anyway so that all six resources have one
    // envelope; a cluster of one is one row.
    let uptime = state.started.elapsed().as_secs();
    let id = state.node_id.to_string();
    let selection = select(
        std::iter::once(([id.as_str(), "", ""], ())),
        paging.after.as_ref(),
        paging.limit,
    );
    Json(page(selection, |key, ()| NodeView {
        id: key[0].to_owned(),
        this_node: true,
        state: "up",
        version: env!("CARGO_PKG_VERSION"),
        uptime_seconds: uptime,
    }))
}

// ---------------------------------------------------------------
// Listeners
// ---------------------------------------------------------------

/// What a listener serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListenerProtocol {
    /// MQTT over TCP.
    Mqtt,
    /// This API.
    Admin,
}

/// A listener this node has bound.
///
/// Held by [`AdminState`] rather than discovered, because a bound
/// socket does not know what it is for and the broker does not know a
/// listener exists. What startup bound is what startup says.
#[derive(Debug, Clone, Serialize)]
pub struct ListenerView {
    /// A stable name, so an interface can key a row on something
    /// other than an address that may be `0.0.0.0`.
    pub name: String,
    /// What it serves.
    pub protocol: ListenerProtocol,
    /// The address actually bound, with the port the operating system
    /// chose when the configuration asked for zero.
    pub address: SocketAddr,
    /// The namespace this listener's clients live in. Empty for the
    /// default, and meaningless for the admin listener.
    pub mountpoint: String,
    /// How many connections it will serve at once, where it has a
    /// limit.
    pub max_connections: Option<usize>,
}

async fn listeners(State(state): State<AdminState>, paging: Paging) -> Json<Page<ListenerView>> {
    let all = state.listeners.clone();
    let selection = select(
        all.iter()
            .map(|listener| ([listener.name.as_str(), "", ""], listener)),
        paging.after.as_ref(),
        paging.limit,
    );
    Json(page(selection, |_, listener| listener.clone()))
}

// ---------------------------------------------------------------
// Sessions and clients
// ---------------------------------------------------------------

/// A session, connected or not.
///
/// A *client* is this same row filtered to the connected ones. They
/// are one shape on purpose: the Clients page and the Sessions page
/// show the same facts about the same thing, and two shapes would be
/// two things for the interface to keep in step.
#[derive(Debug, Serialize)]
pub struct SessionView {
    /// The listener namespace this session belongs to.
    pub mountpoint: String,
    /// The identifier the client chose, or one assigned to it.
    pub client_id: String,
    /// Whether a connection is attached right now.
    pub connected: bool,
    /// `3.1.1` or `5.0`, as an operator writes them.
    pub protocol_version: &'static str,
    /// How many filters it holds.
    pub subscriptions: usize,
    /// Messages waiting for it to come back or for its window to
    /// open.
    pub queued: usize,
    /// Messages sent and not yet acknowledged.
    pub inflight: usize,
    /// How long after a disconnect the session survives.
    pub expiry_interval: u32,
    /// When it will be discarded, in the broker's coarse seconds, and
    /// `null` while it is connected.
    pub expires_at: Option<u32>,
}

/// The one place a protocol version becomes text.
fn version_text(version: Version) -> &'static str {
    match version {
        Version::V311 => "3.1.1",
        Version::V5 => "5.0",
    }
}

/// Build the page of sessions that `connected_only` asks for.
///
/// The closure it is called in holds the broker lock, and everything
/// it returns is owned. See the module documentation.
fn session_page(broker: &Broker, paging: &Paging, connected_only: bool) -> Page<SessionView> {
    let selection = select(
        broker
            .sessions()
            .filter(|(_, session)| !connected_only || session.is_connected())
            .map(|(_, session)| {
                let id = session.id();
                ([&*id.mountpoint, &*id.client_id, ""], session)
            }),
        paging.after.as_ref(),
        paging.limit,
    );
    page(selection, |key, session| SessionView {
        mountpoint: key[0].to_owned(),
        client_id: key[1].to_owned(),
        connected: session.is_connected(),
        protocol_version: version_text(session.version()),
        subscriptions: session.subscriptions().len(),
        queued: session.outbound.queued_len(),
        inflight: session.outbound.inflight_len(),
        expiry_interval: session.expiry_interval(),
        expires_at: session.expires_at(),
    })
}

async fn sessions(State(state): State<AdminState>, paging: Paging) -> Json<Page<SessionView>> {
    // Under the lock: choose and copy. Outside it: nothing at all,
    // because axum serialises what this returns after the handler has
    // finished.
    Json(
        state
            .shared
            .with_broker(|broker| session_page(broker, &paging, false)),
    )
}

async fn clients(State(state): State<AdminState>, paging: Paging) -> Json<Page<SessionView>> {
    Json(
        state
            .shared
            .with_broker(|broker| session_page(broker, &paging, true)),
    )
}

// ---------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------

/// One filter one session holds.
#[derive(Debug, Serialize)]
pub struct SubscriptionView {
    /// The listener namespace.
    pub mountpoint: String,
    /// Whose it is.
    pub client_id: String,
    /// The filter, which is also what identifies it within its
    /// session.
    pub filter: String,
    /// The maximum level to deliver at, as MQTT numbers it.
    pub qos: u8,
    /// Suppress delivery of the subscriber's own publications.
    pub no_local: bool,
    /// Forward the retain flag as published rather than clearing it.
    pub retain_as_published: bool,
    /// When retained messages are sent, as MQTT numbers it.
    pub retain_handling: u8,
    /// The MQTT 5 subscription identifier echoed on delivery, if any.
    pub subscription_id: Option<u32>,
}

/// MQTT's numbering, not the stored discriminant.
///
/// [`storage::record::Qos`] is documented as **not** carrying the
/// protocol's numbers, and this API speaks the protocol's.
fn qos_number(qos: Qos) -> u8 {
    match qos {
        Qos::AtMostOnce => 0,
        Qos::AtLeastOnce => 1,
        Qos::ExactlyOnce => 2,
    }
}

/// The same for the wire's retain-handling option.
fn retain_handling_number(handling: RetainHandling) -> u8 {
    match handling {
        RetainHandling::SendAlways => 0,
        RetainHandling::SendIfNew => 1,
        RetainHandling::DoNotSend => 2,
    }
}

async fn subscriptions(
    State(state): State<AdminState>,
    paging: Paging,
) -> Json<Page<SubscriptionView>> {
    Json(state.shared.with_broker(|broker| {
        let selection = select(
            broker.sessions().flat_map(|(_, session)| {
                let id = session.id();
                session.subscriptions().iter().map(move |subscription| {
                    (
                        [
                            &*id.mountpoint,
                            &*id.client_id,
                            subscription.filter.as_str(),
                        ],
                        subscription,
                    )
                })
            }),
            paging.after.as_ref(),
            paging.limit,
        );
        page(selection, |key, subscription| SubscriptionView {
            mountpoint: key[0].to_owned(),
            client_id: key[1].to_owned(),
            filter: subscription.filter.clone(),
            qos: qos_number(subscription.qos),
            no_local: subscription.no_local,
            retain_as_published: subscription.retain_as_published,
            retain_handling: retain_handling_number(subscription.retain_handling),
            subscription_id: subscription.subscription_id,
        })
    }))
}

// ---------------------------------------------------------------
// Retained messages
// ---------------------------------------------------------------

/// The largest payload this API will put in a list.
///
/// The Topics page browses retained messages, so it wants to show
/// them; a list endpoint that can return a hundred rows of arbitrary
/// payload can return a response of arbitrary size. Anything larger
/// is described rather than sent, and says which reason.
const MAX_INLINE_PAYLOAD: usize = 1024;

/// A message held for future subscribers of a topic.
#[derive(Debug, Serialize)]
pub struct RetainedView {
    /// The topic it is held under.
    pub topic: String,
    /// The payload's size in bytes, always, whether or not the
    /// payload itself is here.
    pub size: usize,
    /// The level it was published at, as MQTT numbers it.
    pub qos: u8,
    /// When it will be discarded, in the broker's coarse seconds.
    pub expires_at: Option<u32>,
    /// The payload as text, when it is text and small enough.
    pub text: Option<String>,
    /// Why `text` is `null`, when it is: `binary` or `too_large`.
    /// Absent when the payload is there.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_omitted: Option<&'static str>,
}

/// The protocol's numbering for the codec's level.
fn codec_qos_number(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

/// Describe one retained payload without always carrying it.
fn payload_of(message: &RetainedMessage) -> (Option<String>, Option<&'static str>) {
    if message.payload.len() > MAX_INLINE_PAYLOAD {
        return (None, Some("too_large"));
    }
    match std::str::from_utf8(&message.payload) {
        Ok(text) => (Some(text.to_owned()), None),
        Err(_) => (None, Some("binary")),
    }
}

async fn retained(State(state): State<AdminState>, paging: Paging) -> Json<Page<RetainedView>> {
    // The retained store enumerates by walking its trie and building
    // a topic string per entry, which is the only enumeration it
    // offers. That walk is O(the store) under the lock whatever this
    // module does; what this module controls is that no payload is
    // copied and nothing is formatted until the lock is gone. The
    // selection still bounds the rows built to one page.
    let held = state.shared.with_broker(|broker| {
        let mut entries = Vec::new();
        broker.retained().entries(&mut entries);
        entries
    });
    let selection = select(
        held.iter()
            .map(|(topic, message)| ([topic.as_str(), "", ""], message)),
        paging.after.as_ref(),
        paging.limit,
    );
    Json(page(selection, |key, message| {
        let (text, text_omitted) = payload_of(message);
        RetainedView {
            topic: key[0].to_owned(),
            size: message.payload.len(),
            qos: codec_qos_number(message.qos),
            expires_at: message.expires_at,
            text,
            text_omitted,
        }
    }))
}

/// The routes this module owns.
pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/api/v1/nodes", get(nodes))
        .route("/api/v1/listeners", get(listeners))
        .route("/api/v1/clients", get(clients))
        .route("/api/v1/sessions", get(sessions))
        .route("/api/v1/subscriptions", get(subscriptions))
        .route("/api/v1/retained", get(retained))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;
    use crate::{AdminConfig, AdminRunning, AdminServer, AdminState};
    use broker_core::broker::{Broker, BrokerConfig};
    use broker_core::registry::SessionId;
    use bytes::BytesMut;
    use mqtt_codec::packet::{Connect, Publish, Subscribe, SubscriptionOptions};
    use mqtt_codec::{decode, encode, Limits, Packet, Properties, ReasonCode};
    use sdmqtt_server::listener::{Running, Server, ServerConfig};
    use sdmqtt_server::shared::Shared;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    /// How long a socket round trip may take before the test calls it
    /// a failure.
    const REPLY: Duration = Duration::from_secs(5);

    // -----------------------------------------------------------
    // A broker, an admin listener, and an MQTT client for them.
    // -----------------------------------------------------------

    /// An MQTT listener and an admin listener over the same broker.
    struct Both {
        mqtt: Running,
        admin: AdminRunning,
    }

    impl Both {
        async fn start() -> Self {
            let mqtt = Server::bind(ServerConfig::ephemeral())
                .await
                .expect("the MQTT listener binds");
            let admin = admin_over(Arc::clone(mqtt.shared()), vec![]).await;
            Self { mqtt, admin }
        }

        /// One `GET`, answered.
        async fn get(&self, path: &str) -> testing::Response {
            testing::get(self.admin.address(), path).await
        }

        async fn stop(self) {
            self.admin.shutdown().await;
            self.mqtt.shutdown().await;
        }
    }

    /// An admin listener over `shared`, on a loopback port the
    /// operating system chooses.
    async fn admin_over(shared: Arc<Shared>, listeners: Vec<ListenerView>) -> AdminRunning {
        let config = AdminConfig {
            address: "127.0.0.1:0".parse().expect("a valid address"),
            ..AdminConfig::default()
        };
        AdminServer::bind(
            &config,
            AdminState::new(shared)
                .with_node_id("node-under-test")
                .with_listeners(listeners),
        )
        .await
        .expect("loopback needs no token")
    }

    fn bytes_of(packet: &Packet) -> BytesMut {
        let mut buf = BytesMut::new();
        encode(packet, mqtt_codec::Version::V5, &mut buf).expect("the packet encodes");
        buf
    }

    async fn send(stream: &mut TcpStream, packet: &Packet) {
        let buf = bytes_of(packet);
        stream.write_all(&buf).await.expect("the write succeeds");
        stream.flush().await.expect("the flush succeeds");
    }

    async fn next(stream: &mut TcpStream, buf: &mut BytesMut) -> Option<Packet> {
        loop {
            if let Some(packet) =
                decode(buf, mqtt_codec::Version::V5, &Limits::default()).expect("a valid packet")
            {
                return Some(packet);
            }
            let mut chunk = [0u8; 1024];
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

    /// Connect a client that outlives its connection, and wait for
    /// its acknowledgement.
    ///
    /// The expiry interval matters: a version 5 client that does not
    /// send one gets zero, its session ends the moment it
    /// disconnects, and the half of this test about what a session
    /// list still holds would be testing nothing.
    async fn handshake(running: &Running, client_id: &str) -> (TcpStream, BytesMut) {
        let mut stream = TcpStream::connect(running.address())
            .await
            .expect("the listener accepts");
        let mut buf = BytesMut::new();
        send(
            &mut stream,
            &Packet::Connect(Box::new(Connect {
                version: mqtt_codec::Version::V5,
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

    /// Ask for `path` until `ready` is happy, or give up loudly.
    ///
    /// A disconnect is noticed by the connection task rather than by
    /// the request, so there is nothing to await on this side.
    async fn until(both: &Both, path: &str, ready: impl Fn(&str) -> bool) -> String {
        let deadline = std::time::Instant::now() + REPLY;
        loop {
            let body = both.get(path).await.body;
            if ready(&body) {
                return body;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{path} never settled; last answer was {body}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // -----------------------------------------------------------
    // What the broker holds, and what it stops holding.
    // -----------------------------------------------------------

    /// Every read resource, against a broker driven over a real
    /// socket.
    ///
    /// Convention 5: the client connects, subscribes and retains
    /// through the listener rather than by calling into the broker,
    /// because a resource that reports what a hand-built struct holds
    /// is not a resource that reports what a client did.
    ///
    /// The second half is the half that matters. A list that only
    /// ever grows looks right until somebody leaves.
    #[tokio::test]
    async fn every_resource_reports_what_the_broker_holds() {
        let both = Both::start().await;
        let (mut client, mut buf) = handshake(&both.mqtt, "reporter").await;

        send(
            &mut client,
            &Packet::Subscribe(Subscribe {
                packet_id: 1,
                filters: vec![SubscriptionOptions {
                    filter: "sensors/+/temp".into(),
                    qos: QoS::AtLeastOnce,
                    no_local: false,
                    retain_as_published: false,
                    retain_handling: mqtt_codec::packet::RetainHandling::SendAlways,
                }],
                properties: Properties::default(),
            }),
        )
        .await;
        match next(&mut client, &mut buf).await {
            Some(Packet::SubAck(ack)) => assert_eq!(ack.reasons, vec![ReasonCode::GrantedQos1]),
            other => panic!("expected a subscribe acknowledgement, got {other:?}"),
        }

        send(
            &mut client,
            &Packet::Publish(Publish {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: true,
                topic: "sensors/a/temp".into(),
                packet_id: None,
                payload: bytes::Bytes::from_static(b"21.5"),
                properties: Properties::default(),
            }),
        )
        .await;
        // The client is its own subscriber, so the delivery coming
        // back is the proof the publication was handled.
        match next(&mut client, &mut buf).await {
            Some(Packet::Publish(publish)) => assert_eq!(publish.topic, "sensors/a/temp"),
            other => panic!("expected the delivery back, got {other:?}"),
        }

        // Every resource now says so.
        let clients = both.get("/api/v1/clients").await;
        assert_eq!(clients.status, 200, "{}", clients.body);
        assert!(
            clients.body.contains("\"client_id\":\"reporter\""),
            "{}",
            clients.body
        );
        assert!(
            clients.body.contains("\"connected\":true"),
            "{}",
            clients.body
        );
        assert!(
            clients.body.contains("\"subscriptions\":1"),
            "{}",
            clients.body
        );
        assert!(
            clients.body.contains("\"protocol_version\":\"5.0\""),
            "{}",
            clients.body
        );

        let sessions = both.get("/api/v1/sessions").await;
        assert!(
            sessions.body.contains("\"client_id\":\"reporter\""),
            "{}",
            sessions.body
        );

        let subscriptions = both.get("/api/v1/subscriptions").await;
        assert!(
            subscriptions.body.contains("\"filter\":\"sensors/+/temp\""),
            "{}",
            subscriptions.body
        );
        assert!(
            subscriptions.body.contains("\"qos\":1"),
            "{}",
            subscriptions.body
        );

        let retained = both.get("/api/v1/retained").await;
        assert!(
            retained.body.contains("\"topic\":\"sensors/a/temp\""),
            "{}",
            retained.body
        );
        assert!(
            retained.body.contains("\"text\":\"21.5\""),
            "{}",
            retained.body
        );
        assert!(retained.body.contains("\"size\":4"), "{}", retained.body);

        // And then it leaves.
        drop(client);

        let clients = until(&both, "/api/v1/clients", |body| {
            body.contains("\"total\":0")
        })
        .await;
        assert!(
            !clients.contains("reporter"),
            "a client that has gone is not a client: {clients}"
        );

        let sessions = both.get("/api/v1/sessions").await;
        assert!(
            sessions.body.contains("\"client_id\":\"reporter\""),
            "its session survives the connection, and says it is not connected: {}",
            sessions.body
        );
        assert!(
            sessions.body.contains("\"connected\":false"),
            "{}",
            sessions.body
        );

        // The retained message is not the client's, and outlives it.
        let retained = both.get("/api/v1/retained").await;
        assert!(
            retained.body.contains("sensors/a/temp"),
            "{}",
            retained.body
        );

        both.stop().await;
    }

    // -----------------------------------------------------------
    // Nodes and listeners.
    // -----------------------------------------------------------

    #[tokio::test]
    async fn nodes_reports_this_node() {
        let both = Both::start().await;
        let response = both.get("/api/v1/nodes").await;
        assert_eq!(response.status, 200);
        assert!(
            response.body.contains("\"id\":\"node-under-test\""),
            "{}",
            response.body
        );
        assert!(
            response.body.contains("\"this_node\":true"),
            "{}",
            response.body
        );
        assert!(response.body.contains("\"total\":1"), "{}", response.body);
        both.stop().await;
    }

    #[tokio::test]
    async fn listeners_reports_what_was_bound() {
        let mqtt = Server::bind(ServerConfig::ephemeral())
            .await
            .expect("the MQTT listener binds");
        let described = vec![ListenerView {
            name: "mqtt".to_owned(),
            protocol: ListenerProtocol::Mqtt,
            address: mqtt.address(),
            mountpoint: String::new(),
            max_connections: Some(1_024),
        }];
        let admin = admin_over(Arc::clone(mqtt.shared()), described).await;

        let response = testing::get(admin.address(), "/api/v1/listeners").await;
        assert_eq!(response.status, 200);
        assert!(
            response.body.contains("\"protocol\":\"mqtt\""),
            "{}",
            response.body
        );
        assert!(
            response.body.contains(&mqtt.address().to_string()),
            "the address actually bound, port and all: {}",
            response.body
        );
        assert!(
            response.body.contains("\"max_connections\":1024"),
            "{}",
            response.body
        );

        admin.shutdown().await;
        mqtt.shutdown().await;
    }

    // -----------------------------------------------------------
    // Pagination.
    // -----------------------------------------------------------

    /// A broker holding `count` sessions, none of them connected.
    ///
    /// Built through `Broker::connect` — the call a connection makes —
    /// rather than over sockets, because what is being tested here is
    /// paging over a population, and five hundred sockets would be
    /// measuring the operating system.
    fn a_broker_holding(count: usize) -> Arc<Shared> {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        shared.with_broker(|broker: &mut Broker| {
            for index in 0..count {
                let mut out = Vec::new();
                let mut connect = Connect {
                    version: mqtt_codec::Version::V5,
                    // Zero padded, so that the order the API returns
                    // is the order a reader expects and the test can
                    // name a row by its position.
                    client_id: format!("client-{index:04}"),
                    clean_start: true,
                    keep_alive: 0,
                    username: None,
                    password: None,
                    will: None,
                    properties: Properties {
                        session_expiry_interval: Some(3_600),
                        ..Properties::default()
                    },
                };
                connect.properties.session_expiry_interval = Some(3_600);
                broker.connect("", &connect, &mut out);
            }
        });
        shared
    }

    /// Walk every page, and prove the walk ends where the collection
    /// does.
    ///
    /// The boundary is the point. An off-by-one that hands out a
    /// cursor after the last full page sends a client into an empty
    /// page, and a client cannot tell that from a collection that
    /// shrank underneath it.
    #[tokio::test]
    async fn a_walk_of_the_pages_visits_every_row_once_and_then_stops() {
        for count in [0_usize, 1, 4, 5, 6] {
            let admin = admin_over(a_broker_holding(count), vec![]).await;

            let mut seen = Vec::new();
            let mut pages = 0;
            let mut cursor: Option<String> = None;
            loop {
                let path = match &cursor {
                    None => "/api/v1/sessions?limit=5".to_owned(),
                    Some(after) => format!("/api/v1/sessions?limit=5&after={}", escape(after)),
                };
                let response = testing::get(admin.address(), &path).await;
                assert_eq!(response.status, 200, "{}", response.body);
                pages += 1;
                assert!(pages <= 10, "the walk does not end: {}", response.body);

                let body = &response.body;
                assert!(
                    body.contains(&format!("\"total\":{count}")),
                    "every page carries the whole count: {body}"
                );
                let here = client_ids(body);
                assert!(
                    here.len() <= 5,
                    "a page holds at most what was asked for: {body}"
                );
                if cursor.is_some() {
                    assert!(
                        !here.is_empty(),
                        "a cursor was handed out for a page with nothing on it: {body}"
                    );
                }
                seen.extend(here);
                match next_cursor(body) {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }

            let expected: Vec<String> = (0..count)
                .map(|index| format!("client-{index:04}"))
                .collect();
            assert_eq!(seen, expected, "with {count} sessions and pages of five");
            admin.shutdown().await;
        }
    }

    /// The selection under the lock is bounded by the page, not by
    /// the population.
    #[tokio::test]
    async fn a_page_of_a_large_broker_is_still_a_page() {
        let admin = admin_over(a_broker_holding(500), vec![]).await;
        let response = testing::get(admin.address(), "/api/v1/sessions?limit=3").await;
        assert_eq!(response.status, 200);
        let here = client_ids(&response.body);
        assert_eq!(here, ["client-0000", "client-0001", "client-0002"]);
        assert!(response.body.contains("\"total\":500"), "{}", response.body);
        admin.shutdown().await;
    }

    /// A cursor is opaque, and a client identifier holding the
    /// separator still round trips.
    #[test]
    fn a_cursor_round_trips_whatever_is_in_it() {
        for parts in [
            ["", "plain", ""],
            ["tenant", "has\u{1f}separator", "a/b"],
            ["%", "%1F", "%25"],
        ] {
            let cursor = encode_cursor(parts);
            let back = decode_cursor(&cursor).expect("what we wrote, we read");
            assert_eq!(back, parts.map(str::to_owned), "through {cursor:?}");
        }
    }

    // -----------------------------------------------------------
    // The error shape.
    // -----------------------------------------------------------

    /// An unknown resource and a malformed request produce the same
    /// shape, with a code a client can branch on.
    #[tokio::test]
    async fn every_failure_has_the_one_shape() {
        let both = Both::start().await;

        let cases: [(&str, u16, &str); 6] = [
            ("/api/v1/nowhere", 404, "not_found"),
            ("/api/v1/rules", 404, "not_found"),
            ("/api/v1/sessions?limit=abc", 400, "invalid_request"),
            ("/api/v1/sessions?limit=0", 400, "invalid_request"),
            ("/api/v1/sessions?limit=100000", 400, "invalid_request"),
            (
                "/api/v1/sessions?after=not-a-cursor",
                400,
                "invalid_request",
            ),
        ];
        for (path, status, code) in cases {
            let response = both.get(path).await;
            assert_eq!(response.status, status, "{path}: {}", response.body);
            assert_eq!(
                response.header("content-type"),
                Some("application/json"),
                "{path}"
            );
            assert!(
                response.body.contains(&format!("\"code\":\"{code}\"")),
                "{path} answers with a code, not a sentence: {}",
                response.body
            );
            assert!(
                response.body.starts_with("{\"error\":"),
                "{path} nests under `error`: {}",
                response.body
            );
        }

        // A parameter at fault is named, so a form can mark it.
        let response = both.get("/api/v1/sessions?limit=0").await;
        assert!(
            response.body.contains("\"field\":\"limit\""),
            "{}",
            response.body
        );

        // And a method a path does not accept is the same shape too.
        let response =
            testing::request(both.admin.address(), "DELETE", "/api/v1/sessions", None).await;
        assert_eq!(response.status, 405, "{}", response.body);
        assert!(
            response.body.contains("\"code\":\"method_not_allowed\""),
            "{}",
            response.body
        );

        both.stop().await;
    }

    /// Later milestones' resources are absent, not empty.
    #[tokio::test]
    async fn the_resources_of_later_milestones_are_not_served() {
        let both = Both::start().await;
        for path in [
            "/api/v1/rules",
            "/api/v1/dead-letters",
            "/api/v1/users",
            "/api/v1/api-keys",
        ] {
            let response = both.get(path).await;
            assert_eq!(
                response.status, 404,
                "{path} must not answer an empty collection: {}",
                response.body
            );
        }
        both.stop().await;
    }

    /// An unknown query parameter is refused rather than ignored.
    ///
    /// The same rule the configuration file works by: a misspelled
    /// key that is quietly dropped is read by its author as having
    /// been honoured.
    #[tokio::test]
    async fn an_unknown_query_parameter_is_refused() {
        let both = Both::start().await;
        let response = both.get("/api/v1/sessions?limitt=5").await;
        assert_eq!(response.status, 400, "{}", response.body);
        assert!(
            response.body.contains("\"code\":\"invalid_request\""),
            "{}",
            response.body
        );
        both.stop().await;
    }

    // -----------------------------------------------------------
    // Reading a body without a JSON parser.
    // -----------------------------------------------------------

    /// Every `client_id` in a page, in the order they appear.
    fn client_ids(body: &str) -> Vec<String> {
        field_values(body, "\"client_id\":\"")
    }

    /// The `next` cursor, or `None` when the page says `null`.
    fn next_cursor(body: &str) -> Option<String> {
        field_values(body, "\"next\":\"").into_iter().next()
    }

    fn field_values(body: &str, prefix: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut rest = body;
        while let Some(at) = rest.find(prefix) {
            rest = &rest[at + prefix.len()..];
            let (value, remainder) = json_string(rest);
            out.push(value);
            rest = remainder;
        }
        out
    }

    /// Read one JSON string body, stopping at the unescaped quote
    /// that ends it and undoing the escapes inside it.
    ///
    /// Needed because a cursor holds a control character, which
    /// `serde_json` writes as `\u001f`. A test that read the six
    /// characters of that escape and handed them back would be
    /// testing its own naivety rather than the API.
    fn json_string(rest: &str) -> (String, &str) {
        let mut out = String::new();
        let mut characters = rest.char_indices();
        while let Some((index, character)) = characters.next() {
            match character {
                '"' => return (out, &rest[index..]),
                '\\' => {
                    let (_, escape) = characters.next().expect("an escape is not the last byte");
                    match escape {
                        'u' => {
                            let digits: String = (0..4)
                                .map(|_| characters.next().expect("four hexadecimal digits").1)
                                .collect();
                            let code =
                                u32::from_str_radix(&digits, 16).expect("hexadecimal digits");
                            out.push(char::from_u32(code).expect("a scalar value"));
                        }
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        other => out.push(other),
                    }
                }
                other => out.push(other),
            }
        }
        panic!("a JSON string that does not end: {rest}")
    }

    /// Percent-encode a cursor for a query string.
    ///
    /// The separator is a control character and a filter may hold a
    /// `+` or a `/`, none of which survive a query string unescaped.
    fn escape(value: &str) -> String {
        let mut out = String::new();
        for byte in value.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(char::from(byte));
                }
                other => out.push_str(&format!("%{other:02X}")),
            }
        }
        out
    }

    /// The session identity the key is built from, kept honest.
    ///
    /// Not a test of the API: a test that the key this module orders
    /// by is the identity the broker uses, so that two clients with
    /// the same name on two mountpoints are two rows.
    #[test]
    fn the_ordering_key_is_the_session_identity() {
        let one = SessionId::new("tenant-a", "shared-name");
        let two = SessionId::new("tenant-b", "shared-name");
        assert_ne!(
            encode_cursor([&one.mountpoint, &one.client_id, ""]),
            encode_cursor([&two.mountpoint, &two.client_id, ""])
        );
    }
}
