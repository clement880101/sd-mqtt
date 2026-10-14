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
//! # Step one is bounded by the page, not by the broker
//!
//! This was half true for a milestone and the half that was missing
//! was the expensive one. The selection kept a heap of at most one
//! page, so the *rows built* were bounded — but the pass was not. It
//! looked at every session, every subscription and every retained
//! message to decide which hundred to keep, and at a hundred
//! thousand of each that was 13.5, 25.5 and 44.9 milliseconds with
//! the process's one lock held and every MQTT client in it waiting.
//! **Four of these six resources need no token**, by section 11.1's
//! design, so on any listener a browser can reach that was an
//! unauthenticated caller stalling the broker as fast as it could
//! ask.
//!
//! What was missing was somewhere to seek to. A hash map has no
//! order to resume from, so the only way to find the hundred rows
//! after a cursor was to look at all of them. The two structures
//! underneath keep an order now — the registry indexes its sessions
//! by identity in a `BTreeMap`, and the retained store keeps its
//! topics in a `BTreeSet` beside its tree — and each handler seeks
//! into it and takes one page. Every total is a maintained number,
//! so saying how large the collection is costs nothing either. One
//! page of a hundred, at a hundred thousand of everything:
//!
//! | resource | before | after |
//! |---|---:|---:|
//! | sessions | 13.55 ms | 4.9 µs |
//! | clients | 7.89 ms | 4.8 µs |
//! | subscriptions | 25.49 ms | 8.8 µs |
//! | retained | 44.93 ms | 8.5 µs |
//!
//! Flat across a thousand, ten thousand and a hundred thousand,
//! which is the point: it is the page that is paid for and not the
//! broker. `what_a_page_costs_under_the_lock` is the measurement,
//! and it asks for a page from the middle of each collection as well
//! as the first one, because only a structure that can seek answers
//! those two the same way.
//!
//! **The one remaining pass is the Clients page**, which wants the
//! connected sessions and skips the rest as it walks the order. The
//! 4.8 µs in the table is that page over a population with every
//! other session connected, which is the case the filter never has
//! to skip far in; with ten of a hundred thousand connected the same
//! page is 1.25 ms, and 3.7 ms at its worst. See [`session_page`]
//! for what that buys and why a second ordered index is still
//! deliberately not here.
//!
//! **How much of this a test can see, honestly.** The bounded
//! selection is testable and is tested: a broker with far more
//! sessions than a page still answers a page, and each resource's
//! page walk visits every row once and stops where the collection
//! does. Where the serialising happens is **not**
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

use axum::extract::rejection::JsonRejection;
use axum::extract::{FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use broker_core::broker::Broker;
use broker_core::registry::SessionId;
use broker_core::retained::RetainedMessage;
use broker_core::session::Session;
use bytes::Bytes;
use mqtt_codec::{QoS, Version};
use sdmqtt_server::publish::{Publication, PublishRefused};
use serde::{Deserialize, Serialize};
use storage::record::{Qos, RetainHandling, SubscriptionRecord};

use crate::error::{ApiError, ErrorCode};
use crate::{AdminState, Forgotten};

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
pub(crate) const PARTS: usize = 3;

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
/// Borrowed, deliberately: an owned key would be up to three string
/// allocations per row considered rather than per row returned.
///
/// Four of the six resources no longer build one per row at all.
/// They read an order the broker already keeps and write a cursor
/// only for the last row of a page; see [`encode_cursor`]. What is
/// left on this path is nodes and listeners, which are a handful of
/// rows held by the admin listener itself and not by the broker.
pub(crate) type Key<'a> = [&'a str; PARTS];

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
    ///
    /// **Exact, and never an estimate or a cap.** Counting was the
    /// one part of answering a page that still had to see everything
    /// once the pages themselves could seek, and the answer is that
    /// none of the four counts anything: every one is read from a
    /// number the broker maintains where it changes — the session
    /// count, the connected count, the subscriptions gauge and the
    /// retained store's ordered index, which is its own length. So
    /// this is the whole collection, at the instant the lock was
    /// held, and a client can page to exactly this many rows.
    ///
    /// Two of them include something a reader might not expect, and
    /// both are what they always were. A retained message past its
    /// deadline that no sweep has reclaimed is still stored and is
    /// still counted, and it is still on the page. A session is
    /// counted whether or not anything is attached to it; the
    /// connected ones are the Clients resource.
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
pub(crate) struct Paging {
    pub(crate) limit: usize,
    pub(crate) after: Option<[String; PARTS]>,
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
pub(crate) struct Selection<'a, T> {
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
/// One key comparison per item and, for at most `limit` of them, a
/// push into a heap that never grows past `limit`. So the rows built
/// are bounded by the page — but the *pass* is not, because a
/// selection has to see an item to reject it.
///
/// **That is why nothing the broker holds comes through here any
/// more.** The four resources that can be large seek into an order
/// the broker keeps and never look at a row they will not return;
/// see the module documentation for what the pass cost before they
/// did. What is left is nodes and listeners, which are a handful of
/// rows the admin listener holds itself, are not behind the broker's
/// lock, and want the same envelope as the other four.
pub(crate) fn select<'a, T>(
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
pub(crate) fn page<'a, T, R>(
    selection: Selection<'a, T>,
    row: impl Fn(Key<'a>, T) -> R,
) -> Page<R> {
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
#[derive(Debug, Clone, Serialize)]
pub struct NodeView {
    /// Section 8.1's identity: the UUID, in its canonical text.
    pub id: String,
    /// What an operator calls it. Never compared.
    pub name: String,
    /// The address it says peers should dial.
    ///
    /// Empty on a node that does not cluster. **Not a bind address**,
    /// and not the address a link arrived from: see `cluster::mesh`.
    pub address: String,
    /// Whether it is the one answering.
    pub this_node: bool,
    /// `up`, `suspect` or `down`.
    ///
    /// For this node it is `up` by virtue of answering. For a peer it
    /// is what section 8.1's failure detection says: suspect after
    /// three missed beats, dead after the window.
    pub state: &'static str,
    /// The build serving this API.
    ///
    /// **`null` for a peer.** Membership carries an identity, a name
    /// and an address, and nothing about the build at the other end;
    /// reporting this node's version beside a peer's row would be a
    /// number that is wrong exactly when it matters, during an
    /// upgrade.
    pub version: Option<&'static str>,
    /// How long it has been serving, `null` for a peer and for the
    /// same reason.
    pub uptime_seconds: Option<u64>,
}

/// What a forgotten node is answered with.
#[derive(Debug, Serialize)]
struct Forgot {
    id: String,
}

/// Take a node out of this node's membership at once.
///
/// Section 8.6's operator override: a node that is never coming back is
/// forgotten now rather than after `cluster.gone_after_ms`, and the
/// replica sets that included it are repaired now. **This node's own
/// table only.** Membership is not replicated — section 8.2 — so an
/// operator decommissioning a node tells each surviving node, or waits
/// for the grace on the ones not told. The response says so.
///
/// Token-guarded like every write here. A node that does not cluster
/// has nothing to forget and answers not found for any identifier.
async fn forget_node(
    State(state): State<AdminState>,
    _authorized: Authorized,
    Path(id): Path<String>,
) -> Result<Json<Forgot>, ApiError> {
    let Some(cluster) = &state.cluster else {
        return Err(ApiError::not_found(
            "this node does not cluster, so there is no membership to forget a node from",
        ));
    };
    match cluster.forget(&id) {
        Forgotten::Gone => Ok(Json(Forgot { id })),
        Forgotten::NotMet => Err(ApiError::not_found(format!(
            "this node has not met a node {id}; nothing to forget. Each node keeps its own \
             membership, so a node already forgotten here may still be known elsewhere"
        ))),
        Forgotten::Myself => Err(ApiError::invalid_request(
            "a node cannot forget itself; to decommission this node, stop it",
        )),
    }
}

async fn nodes(State(state): State<AdminState>, paging: Paging) -> Json<Page<NodeView>> {
    // Not read under the broker lock, because none of it is the
    // broker's. Paginated anyway so that all six resources have one
    // envelope; a cluster of one is still one row.
    let mut rows = vec![NodeView {
        id: state.node_id.to_string(),
        name: state.node_name.to_string(),
        address: state.node_address.to_string(),
        this_node: true,
        state: "up",
        version: Some(env!("CARGO_PKG_VERSION")),
        uptime_seconds: Some(state.started.elapsed().as_secs()),
    }];
    if let Some(cluster) = &state.cluster {
        rows.extend(cluster.peers().into_iter().map(|peer| NodeView {
            id: peer.id,
            name: peer.name,
            address: peer.address,
            this_node: false,
            state: peer.state,
            version: None,
            uptime_seconds: None,
        }));
    }
    // By identity, which is the order section 8.1 says everything
    // orders by, so two nodes asked the same question list the cluster
    // the same way round.
    rows.sort_by(|one, other| one.id.cmp(&other.id));

    let selection = select(
        rows.iter().map(|row| ([row.id.as_str(), "", ""], row)),
        paging.after.as_ref(),
        paging.limit,
    );
    Json(page(selection, |_key, row| row.clone()))
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
    /// Section 9.2's ingest endpoint, on a listener of its own.
    ///
    /// Absent when the ingest routes are served on the admin listener,
    /// because then there is no second socket to report.
    Ingest,
    /// Section 8.1's cluster link, on a node that clusters.
    ///
    /// Its row carries the **advertised** address in the mountpoint
    /// field as well as the bind address, because a node bound to
    /// every interface has a bind address no peer can use and that is
    /// exactly the row an operator is looking at when they ask why a
    /// peer cannot reach them.
    Cluster,
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
    /// Section 5.4's cap, on an MQTT listener; `null` on every other row.
    pub max_qos: Option<u8>,
    /// Whether retained messages are accepted, on an MQTT listener.
    pub retain_available: Option<bool>,
    /// Section 8.6's mode for publishes on an MQTT listener: `async`,
    /// `sync`, or `node` for the node's own. `null` on every other row.
    pub durability: Option<&'static str>,
}

async fn listeners(State(state): State<AdminState>, paging: Paging) -> Json<Page<ListenerView>> {
    // Copied out under the lock and paged outside it, which is the
    // shape every read resource here has. A handful of rows, so the
    // copy costs nothing worth arranging around.
    let all = state.listeners.read().clone();
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

/// The session identity a cursor names.
///
/// The first two parts of the key, which is the whole of what the
/// broker orders sessions by. A cursor naming a session that has
/// since gone is not an error: the range resumes at whatever now
/// follows it, which is why the cursor is a key and not an offset.
fn identity_of(after: &[String; PARTS]) -> SessionId {
    SessionId::new(&after[0], &after[1])
}

/// One row of the sessions or clients page.
fn session_view(session: &Session) -> SessionView {
    let id = session.id();
    SessionView {
        mountpoint: id.mountpoint.to_string(),
        client_id: id.client_id.to_string(),
        connected: session.is_connected(),
        protocol_version: version_text(session.version()),
        subscriptions: session.subscriptions().len(),
        queued: session.outbound.queued_len(),
        inflight: session.outbound.inflight_len(),
        expiry_interval: session.expiry_interval(),
        expires_at: session.expires_at(),
    }
}

/// Build the page of sessions that `connected_only` asks for.
///
/// The closure it is called in holds the broker lock, and everything
/// it returns is owned. See the module documentation.
///
/// # What this looks at, and it is no longer everything
///
/// It used to keep a heap of one page over a pass across every
/// session in the broker. The rows built were bounded and the pass
/// was not, so a page of ten out of a hundred thousand cost 13.5
/// milliseconds on the process's one lock — on an endpoint section
/// 11.1 leaves unauthenticated. The registry keeps its sessions in
/// identity order now, so this seeks to the cursor and takes what it
/// needs; see [`Broker::sessions_after`].
///
/// **The Clients page is the one exception, and it is bounded by the
/// population rather than by the page.** It wants the connected
/// sessions and the order does not know which those are, so it skips
/// the rest as it walks. A page costs the page plus whatever is
/// interleaved with it, and in the worst case — a hundred thousand
/// sessions of which only the last ten are connected — that is still
/// a pass over the identity order.
///
/// # What that costs, measured rather than argued
///
/// It used to say the pass was "much cheaper" and leave it there,
/// which is an argument and not a number, and the measurement beside
/// it could not have produced one: its population had every other
/// session connected, so the filter rejected one row for each row it
/// took and the page cost a page. That is convention 5's "test
/// measuring the wrong dimension" — the number that appeared to
/// bound this page was taken on the population this page is not slow
/// on. [`what_the_clients_page_costs_when_few_are_connected`] is the
/// case that was missing, ten connected of the whole population and
/// those ten last in the order:
///
/// | sessions | 10 connected | all connected |
/// |---:|---:|---:|
/// | 1 000 | 6.0 µs | 4.1 µs |
/// | 10 000 | 54.2 µs | 5.0 µs |
/// | 100 000 | 1.25 ms | 6.1 µs |
///
/// So it is linear in the population, as the paragraph above says,
/// and the constant is small: 1.25 ms where the unbounded selection
/// this replaced was 7.89 ms, and 3.7 ms at its worst of twenty
/// runs. Still milliseconds of the process's one lock, on an
/// endpoint section 11.1 leaves unauthenticated, and this is a
/// realistic population rather than a contrived one — a fleet of
/// persistent sessions that are mostly offline is the ordinary shape
/// of an MQTT deployment, not an exotic one.
///
/// # And it is still not fixed here
///
/// A second ordered index holding only the connected sessions would
/// bound it properly. It is deliberately not here, and the reason is
/// no longer "no measurement has shown it" — one has — but what the
/// index would cost to be right.
///
/// Connectedness changes on the session and not on the registry, so
/// the index cannot live in `Registry` beside `by_identity`; it has
/// to be a shared structure every session mutates, the shape
/// `AttachedSessions` already has, and it has to be correct in five
/// places: a clean start replacing a live session, a claim, a
/// disconnect, a destroy, and the restore that rebuilds the registry
/// from the store. It adds an owned `SessionId` and a tree operation
/// to every connect and every disconnect, which is the path the
/// design's hundred-thousand-connection target runs through. That is
/// a structure that can drift and a cost on a hot path, against a
/// millisecond on a page nobody has yet been measured asking for
/// twice a second.
///
/// The bound that would not add a structure is to stop this walk
/// after a fixed number of skipped rows and hand back a short page
/// with a cursor. That bounds the lock properly and costs the
/// caller round trips instead — but it changes what a page means for
/// every resource that shares this envelope, which is a milestone's
/// decision and not a follow-up's.
///
/// Deferred, then, and not forgotten: convention 8 says those two
/// look identical in a codebase and opposite in a review, so the
/// number is written down where the next person meets the code.
fn session_page(broker: &Broker, paging: &Paging, connected_only: bool) -> Page<SessionView> {
    let after = paging.after.as_ref().map(identity_of);
    let mut rows = broker
        .sessions_after(after.as_ref())
        .filter(|(_, session)| !connected_only || session.is_connected());

    let mut items: Vec<SessionView> = Vec::with_capacity(paging.limit);
    for (_, session) in rows.by_ref().take(paging.limit) {
        items.push(session_view(session));
    }
    // One row past the page, and no further. A page that is full but
    // final must say `next: null`, or a client walks into an empty
    // page and cannot tell it from a collection that shrank.
    let more = rows.next().is_some();
    let next = more
        .then(|| {
            items
                .last()
                .map(|row| encode_cursor([&row.mountpoint, &row.client_id, ""]))
        })
        .flatten();

    Page {
        items,
        // Both are maintained counts, so the whole collection's size
        // costs nothing to report beside a page of it.
        total: if connected_only {
            broker.connected_count()
        } else {
            broker.session_count()
        },
        next,
    }
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

/// Build the page of subscriptions.
///
/// Named rather than written inside the handler's closure so that
/// the measurement can time exactly what the lock is held for: see
/// `what_a_page_costs_under_the_lock`.
fn subscription_page(broker: &Broker, paging: &Paging) -> Page<SubscriptionView> {
    // A subscription's key is its session's identity and then its
    // filter, so a cursor resumes *inside* a session: at the filter
    // after the one it names, in the session it names. That session
    // has to be handed back, not skipped past, or every filter it
    // holds after the cursor disappears.
    let resume = paging
        .after
        .as_ref()
        .map(|after| (identity_of(after), after[2].as_str()));
    let mut items: Vec<SubscriptionView> = Vec::with_capacity(paging.limit);
    let mut more = false;

    'sessions: for (_, session) in broker.sessions_from(resume.as_ref().map(|(id, _)| id)) {
        if session.subscriptions().is_empty() {
            continue;
        }
        // Only for the session the cursor names. Any session after
        // it starts at its first filter.
        let after_filter = resume
            .as_ref()
            .filter(|(id, _)| id == session.id())
            .map(|(_, filter)| *filter);

        // A session's filters are held in the order they arrived,
        // which is not the order this page is in. Sorted per session
        // rather than across the broker: this is the subscriptions of
        // the sessions a page touches, not of every session there is.
        let mut held: Vec<&SubscriptionRecord> = session.subscriptions().iter().collect();
        held.sort_unstable_by(|one, two| one.filter.cmp(&two.filter));

        for record in held {
            if after_filter.is_some_and(|filter| record.filter.as_str() <= filter) {
                continue;
            }
            if items.len() == paging.limit {
                more = true;
                break 'sessions;
            }
            let id = session.id();
            items.push(SubscriptionView {
                mountpoint: id.mountpoint.to_string(),
                client_id: id.client_id.to_string(),
                filter: record.filter.clone(),
                qos: qos_number(record.qos),
                no_local: record.no_local,
                retain_as_published: record.retain_as_published,
                retain_handling: retain_handling_number(record.retain_handling),
                subscription_id: record.subscription_id,
            });
        }
    }

    let next = more
        .then(|| {
            items.last().map(|row| {
                encode_cursor([
                    row.mountpoint.as_str(),
                    row.client_id.as_str(),
                    row.filter.as_str(),
                ])
            })
        })
        .flatten();

    Page {
        items,
        // A maintained number, and the same one section 11.3's
        // subscriptions gauge reports: every filter every session
        // holds is in exactly one of the trie and the shared table.
        // `the_subscriptions_total_is_what_paging_them_gives` says so
        // from the outside, and `tests/journal.rs` says so from
        // inside after every generated history.
        total: broker.subscription_count(),
        next,
    }
}

async fn subscriptions(
    State(state): State<AdminState>,
    paging: Paging,
) -> Json<Page<SubscriptionView>> {
    Json(
        state
            .shared
            .with_broker(|broker| subscription_page(broker, &paging)),
    )
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
pub const MAX_INLINE_PAYLOAD: usize = 1024;

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

/// One page of retained messages, as copied out under the lock.
struct RetainedHeld {
    /// The page, in topic order, with nothing formatted yet.
    entries: Vec<(String, RetainedMessage)>,
    /// How many the store holds, not how many are on this page.
    total: usize,
    /// Whether the store holds anything after this page.
    more: bool,
}

/// What the retained handler copies out while it holds the lock.
///
/// # Why this one does not go through [`select`]
///
/// [`select`] bounds the *rows built* to a page and it always did,
/// but it bounds nothing about the pass: it sees every item, so it
/// needs every item, and the only enumeration the retained store
/// offered was a walk that built a topic string and cloned a message
/// per entry. That is 44.9 milliseconds at a hundred thousand topics
/// on the process's one lock, on an endpoint that needs no token.
///
/// The store now keeps its topics in order beside the tree, so this
/// seeks to the cursor and takes one page. See
/// [`RetainedStore::page`]. The rows are still built outside the
/// lock, as the module documentation says they must be: a page of a
/// hundred inline payloads is a hundred kilobytes of copying and
/// UTF-8 validation, which is not a thing to do while the broker
/// waits.
///
/// The cursor's other two parts are ignored because a retained key
/// only ever has one: the key is `[topic, "", ""]`, so the whole of
/// the ordering is the topic, and the comparison this replaces
/// reached the same answer through the array.
///
/// [`RetainedStore::page`]: broker_core::retained::RetainedStore::page
fn retained_held(broker: &Broker, paging: &Paging) -> RetainedHeld {
    let after = paging.after.as_ref().map(|after| after[0].as_str());
    let mut entries = Vec::with_capacity(paging.limit);
    let more = broker.retained().page(after, paging.limit, &mut entries);
    RetainedHeld {
        entries,
        // Constant time: the store's ordered index is its own count.
        total: broker.retained_count(),
        more,
    }
}

async fn retained(State(state): State<AdminState>, paging: Paging) -> Json<Page<RetainedView>> {
    let RetainedHeld {
        entries,
        total,
        more,
    } = state
        .shared
        .with_broker(|broker| retained_held(broker, &paging));
    // A cursor only when something is actually left, exactly as
    // [`page`] decides it: a page that is full but final must say
    // `next: null`.
    let next = more
        .then(|| {
            entries
                .last()
                .map(|(topic, _)| encode_cursor([topic.as_str(), "", ""]))
        })
        .flatten();
    Json(Page {
        items: entries
            .iter()
            .map(|(topic, message)| {
                let (text, text_omitted) = payload_of(message);
                RetainedView {
                    topic: topic.clone(),
                    size: message.payload.len(),
                    qos: codec_qos_number(message.qos),
                    expires_at: message.expires_at,
                    text,
                    text_omitted,
                }
            })
            .collect(),
        total,
        next,
    })
}

// ---------------------------------------------------------------
// Publishing
// ---------------------------------------------------------------

/// Whether a request carried the token, when one is configured.
///
/// An extractor rather than a line at the top of the handler,
/// because a second mutating endpoint added later gets the guard by
/// naming it in its signature and cannot get it by forgetting to.
///
/// Section 11.1: only *mutating* requests need it. The read
/// resources are not guarded, and the listener's real protection
/// until milestone 7 is where it listens.
#[derive(Debug)]
pub struct Authorized;

impl FromRequestParts<AdminState> for Authorized {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AdminState,
    ) -> Result<Self, Self::Rejection> {
        let Some(configured) = state.token.as_deref() else {
            // Nothing to present. Section 11.1: a listener with no
            // token is a loopback listener, which the startup
            // refusal guarantees.
            return Ok(Self);
        };
        let presented = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .unwrap_or("");
        if secret_eq(presented, configured.expose()) {
            Ok(Self)
        } else {
            // The same sentence whether the header was missing, the
            // scheme was wrong or the secret was: which of the three
            // it was is a fact about our configuration, and the
            // caller has not earned it.
            Err(ApiError::new(
                ErrorCode::Unauthorized,
                "this endpoint needs the configured admin token, presented as \
                 `Authorization: Bearer <token>`",
            ))
        }
    }
}

/// Compare a presented secret with the configured one without
/// stopping at the first difference.
///
/// # The ruling, and why
///
/// An ordinary `==` on two `str`s compares lengths and then bytes,
/// and returns at the first pair that differ. The time it takes
/// therefore depends on how much of the secret the caller already
/// has right, which over enough samples recovers the secret one byte
/// at a time.
///
/// Three things could argue for `==` here anyway: the listener binds
/// loopback by default, the timing difference is nanoseconds against
/// milliseconds of network and scheduler noise, and there is no TLS
/// yet for an attacker to be hiding behind. None of them survives
/// being looked at.
///
/// The case for constant time is that **a token is configured
/// precisely when the listener is not on loopback.** That is the
/// whole of what [`AdminConfig::check`] enforces: no token means
/// loopback, so every request this comparison ever runs against a
/// real secret arrives from somewhere else on the network. The
/// threat model for the ordinary comparison is exactly the
/// configuration this comparison exists for. It also costs a
/// handful of nanoseconds on a path that runs once per mutating
/// request rather than once per packet, and an `==` on a secret is
/// something a reviewer cannot tell from an oversight.
///
/// So: constant time, hand-written rather than a dependency, because
/// it is six lines and a crate for six lines is a licence to audit.
///
/// # No test can tell the two apart, and that was checked
///
/// Convention 7: the plan's second mutation for this task is to put
/// an ordinary early-returning `==` here, and it **survives the whole
/// suite**. That is not a gap that can be closed. The two comparisons
/// agree on every answer — the test below asserts exactly that, and
/// would be wrong to assert anything else — and the only thing
/// separating them is how long a wrong answer takes, which is
/// convention 5's "test measuring the wrong dimension": nanoseconds,
/// flaky on a loaded machine and green on a fast one with the bug
/// present. What guards it is this comment and the shape of the
/// function, which has no `return` in its loop for a reviewer to
/// miss.
///
/// **What it does not hide is the length.** `presented.len()` is the
/// caller's own, so the loop's length tells the caller nothing it
/// did not supply; the length of the configured token is folded in
/// as a value rather than branched on. Hiding the length entirely
/// would mean comparing fixed-width digests, which would mean a hash
/// function, and milestone 7 replaces this whole mechanism with API
/// keys that have one.
///
/// [`AdminConfig::check`]: crate::config::AdminConfig::check
fn secret_eq(presented: &str, configured: &str) -> bool {
    let (presented, configured) = (presented.as_bytes(), configured.as_bytes());
    // Folded in rather than returned on, so that a wrong length
    // costs the same as a wrong byte.
    let mut difference = presented.len() ^ configured.len();
    for (index, byte) in presented.iter().enumerate() {
        // The configured token may be shorter, and an index past its
        // end would panic. Wrapping keeps the work per presented byte
        // the same whichever is longer; the length term above is what
        // makes the answer right.
        let against = if configured.is_empty() {
            0
        } else {
            configured[index % configured.len()]
        };
        difference |= usize::from(byte ^ against);
    }
    difference == 0
}

/// What a publish request may say.
///
/// The payload is text. This endpoint is what the Topics page's
/// publish tool posts and what an operator reaches for with `curl`,
/// and both of those type a string; a binary payload is what an MQTT
/// client is for. Said here rather than left to be discovered,
/// because "the API cannot express it" and "the API silently mangled
/// it" look the same from outside.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishRequest {
    /// Where to publish it. A topic name, so no wildcards.
    topic: String,
    /// What to publish, as text. Empty by default, which on a
    /// retained publish is how a topic is cleared.
    #[serde(default)]
    payload: String,
    /// The quality of service to publish at, as MQTT numbers it.
    #[serde(default)]
    qos: u8,
    /// Whether to retain it.
    #[serde(default)]
    retain: bool,
    /// How long the message stays deliverable, in seconds.
    #[serde(default)]
    message_expiry_interval: Option<u32>,
}

/// What a publish that was accepted answers.
#[derive(Debug, Serialize)]
struct PublishAccepted {
    /// How many subscribers it reached. Zero is an ordinary answer:
    /// a retained publish to a topic nobody holds reaches nobody and
    /// is still kept.
    delivered: usize,
    /// Whether this answer waited for the store.
    ///
    /// Reported rather than implied, because the promise the caller
    /// is entitled to read into a `200` differs between the levels
    /// and an interface should not have to know the rule to say so.
    durable: bool,
}

/// MQTT's numbering, on the way in.
fn qos_of(number: u8) -> Result<QoS, ApiError> {
    match number {
        0 => Ok(QoS::AtMostOnce),
        1 => Ok(QoS::AtLeastOnce),
        2 => Ok(QoS::ExactlyOnce),
        other => Err(ApiError::in_field(
            ErrorCode::InvalidRequest,
            "qos",
            format!("qos must be 0, 1 or 2, not {other}"),
        )),
    }
}

/// A refusal from the one publish, in this API's one shape.
///
/// The sentences are the publish path's own, so that two endpoints
/// refusing the same topic say the same thing. What is added here is
/// the code and the field, which are HTTP and belong to this crate.
pub(crate) fn publish_refusal(refused: &PublishRefused) -> ApiError {
    match refused {
        // A form can mark the input that was wrong without reading the
        // sentence.
        PublishRefused::NotATopic(_) | PublishRefused::Reserved => {
            ApiError::in_field(ErrorCode::InvalidRequest, "topic", refused.to_string())
        }
        PublishRefused::NotDurable => ApiError::new(ErrorCode::Internal, refused.to_string()),
        // Kept here, confirmed nowhere, and the operator asked for
        // that to be a refusal: the one refusal in this list that is
        // not a fault in the request or in this node.
        PublishRefused::NotConfirmed => ApiError::new(ErrorCode::Unavailable, refused.to_string()),
        // The cycle this bounds has no bridge in it and no admin API
        // in it either: an operator publishing by hand carries no hop
        // count, so there is no chain here for a limit to end. Should
        // one ever arrive it is this broker's defect and not the
        // caller's, which is what `internal` says. `loop_detected`
        // belongs to section 9.2's ingest endpoint, which is the one
        // place in such a cycle a caller can see.
        PublishRefused::TooManyHops(_) => ApiError::new(ErrorCode::Internal, refused.to_string()),
    }
}

/// Publish a message, through the path a connection publishes by.
///
/// # There is no second publish here
///
/// This is the HTTP and nothing else: a body to validate, and an
/// answer. Everything that decides what a publication *means* —
/// retention, expiry, matching, shared-subscription selection, the
/// per-session queue and its bounds, and the wait that makes an
/// acknowledged level's answer a promise — is
/// [`sdmqtt_server::publish`], which is the one publish and is shared
/// with section 9.2's ingest endpoint. It was extracted from here when
/// that second caller arrived, which is the moment a copy would have
/// been made instead.
async fn publish(
    State(state): State<AdminState>,
    _authorized: Authorized,
    body: Result<Json<PublishRequest>, JsonRejection>,
) -> Result<Json<PublishAccepted>, ApiError> {
    let Json(request) = body.map_err(|rejection| {
        // Section 13: axum's own rejection is plain text, and this
        // API answers one shape and no other.
        ApiError::invalid_request(format!(
            "the request body is not usable: {}",
            rejection.body_text()
        ))
    })?;

    let published = sdmqtt_server::publish::publish(
        &state.shared,
        Publication {
            topic: request.topic,
            payload: Bytes::from(request.payload),
            qos: qos_of(request.qos)?,
            retain: request.retain,
            message_expiry_interval: request.message_expiry_interval,
            // An operator publishing by hand is not a connector, so
            // there is no hop count to carry; see `rules::hops`.
            user_properties: Vec::new(),
            // Named so that a rule's trigger can tell a message an
            // operator published by hand from one a client sent.
            origin: "admin-api".to_owned(),
        },
    )
    .await
    .map_err(|refused| publish_refusal(&refused))?;

    Ok(Json(PublishAccepted {
        delivered: published.delivered,
        durable: published.durable,
    }))
}

/// The routes this module owns.
pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/api/v1/nodes", get(nodes))
        .route("/api/v1/nodes/{id}", delete(forget_node))
        .route("/api/v1/listeners", get(listeners))
        .route("/api/v1/clients", get(clients))
        .route("/api/v1/sessions", get(sessions))
        .route("/api/v1/subscriptions", get(subscriptions))
        .route("/api/v1/retained", get(retained))
        .route("/api/v1/publish", post(publish))
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
    // The real writer over a store whose flush this crate's tests can
    // hold open, exposed by `sdmqtt-server` under its `testing`
    // feature rather than written again here. Section 7.3's ordering
    // is only visible while a flush is in progress, and a second
    // gated store would be a second set of assumptions about what a
    // device does.
    use sdmqtt_server::shared::gated::GatedStore;
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
                .with_listeners(crate::listener_list(listeners)),
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

    /// A cluster this node has met, for the listing to report.
    #[derive(Debug)]
    struct Peers(Vec<crate::ClusterPeer>);

    impl crate::Cluster for Peers {
        fn peers(&self) -> Vec<crate::ClusterPeer> {
            self.0.clone()
        }

        /// Nothing configured, which is what this listing is about: it
        /// reports the nodes met and never a count of them.
        fn expected(&self) -> Option<usize> {
            None
        }

        fn forget(&self, _: &str) -> crate::Forgotten {
            crate::Forgotten::NotMet
        }

        fn replication(&self) -> Option<crate::Replication> {
            None
        }
    }

    fn peer(id: &str, name: &str, state: &'static str) -> crate::ClusterPeer {
        crate::ClusterPeer {
            id: id.to_owned(),
            name: name.to_owned(),
            address: format!("10.0.0.1:7700/{name}"),
            state,
        }
    }

    async fn admin_over_a_cluster(peers: Vec<crate::ClusterPeer>) -> AdminRunning {
        let config = AdminConfig {
            address: "127.0.0.1:0".parse().expect("a valid address"),
            ..AdminConfig::default()
        };
        AdminServer::bind(
            &config,
            AdminState::new(Arc::new(Shared::new(BrokerConfig::default())))
                .with_node_id("bbbbbbbb-0000-7000-8000-000000000000")
                .with_node_name("edge-b")
                .with_node_address("10.0.0.1:7700/edge-b")
                .with_cluster(Some(Arc::new(Peers(peers)) as Arc<dyn crate::Cluster>)),
        )
        .await
        .expect("loopback needs no token")
    }

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

    /// The listing reports the peers as well, in identity order, and
    /// says which row is this node.
    ///
    /// The identifier used to be the word `local`, hardcoded, on a
    /// node that had a real one. This is what says it is not any more.
    #[tokio::test]
    async fn nodes_reports_the_cluster_in_identity_order() {
        let admin = admin_over_a_cluster(vec![
            peer("cccccccc-0000-7000-8000-000000000000", "edge-c", "up"),
            peer("aaaaaaaa-0000-7000-8000-000000000000", "edge-a", "down"),
        ])
        .await;
        let response = testing::get(admin.address(), "/api/v1/nodes").await;
        assert_eq!(response.status, 200);
        assert!(response.body.contains("\"total\":3"), "{}", response.body);

        let order: Vec<&str> = ["edge-a", "edge-b", "edge-c"]
            .into_iter()
            .filter(|name| response.body.contains(&format!("\"name\":\"{name}\"")))
            .collect();
        assert_eq!(order, ["edge-a", "edge-b", "edge-c"], "{}", response.body);
        let positions: Vec<usize> = ["edge-a", "edge-b", "edge-c"]
            .into_iter()
            .map(|name| {
                response
                    .body
                    .find(&format!("\"name\":\"{name}\""))
                    .expect("every node is listed")
            })
            .collect();
        assert!(
            positions[0] < positions[1] && positions[1] < positions[2],
            "the rows must be in identity order: {}",
            response.body
        );

        // The peer that is down says so, and this node says which it
        // is. A version is reported for this node and for no peer.
        assert!(
            response.body.contains("\"state\":\"down\""),
            "{}",
            response.body
        );
        assert_eq!(
            response.body.matches("\"this_node\":true").count(),
            1,
            "{}",
            response.body
        );
        assert_eq!(
            response.body.matches("\"version\":null").count(),
            2,
            "a peer's build is not something membership carries: {}",
            response.body
        );
        assert!(
            response.body.contains("10.0.0.1:7700/edge-a"),
            "the address a peer advertised is what an operator dials: {}",
            response.body
        );
        admin.shutdown().await;
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
            max_qos: Some(1),
            retain_available: Some(false),
            durability: Some("sync"),
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
        // Section 5.4 and 8.6's policy, which is what an operator reads
        // this row for once there is more than one listener.
        assert!(
            response.body.contains("\"max_qos\":1")
                && response.body.contains("\"retain_available\":false")
                && response.body.contains("\"durability\":\"sync\""),
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
                broker
                    .connect("", &connect, &mut out)
                    .expect("the default decision authenticates");
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

    /// A broker holding `count` sessions, each with `each` filters,
    /// with every other session disconnected and one shared
    /// subscription on top.
    ///
    /// The filters are named so that their order is not the order
    /// they were taken in, because a page that comes out sorted only
    /// because it was written sorted proves nothing. The shared one
    /// is there because a shared subscription is the only filter that
    /// lives in the shared table rather than the trie, and the
    /// subscriptions total adds the two: a page that counted one of
    /// them twice, or neither, shows up nowhere else.
    fn a_broker_subscribing(count: usize, each: usize) -> Arc<Shared> {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        shared.with_broker(|broker: &mut Broker| {
            for index in 0..count {
                let connect = Connect {
                    version: mqtt_codec::Version::V5,
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
                let mut out = Vec::new();
                let subscriber = broker
                    .connect("", &connect, &mut out)
                    .expect("the default decision authenticates")
                    .subscriber;

                let mut filters: Vec<String> = (0..each)
                    .rev()
                    .map(|filter| format!("site/{filter:02}/telemetry"))
                    .collect();
                if index == 0 {
                    filters.push("$share/g/shared/topic".to_owned());
                }
                for filter in filters {
                    let packet = Packet::Subscribe(Subscribe {
                        packet_id: 1,
                        filters: vec![SubscriptionOptions {
                            filter,
                            qos: mqtt_codec::QoS::AtMostOnce,
                            no_local: false,
                            retain_as_published: false,
                            retain_handling: mqtt_codec::packet::RetainHandling::SendAlways,
                        }],
                        properties: Properties::default(),
                    });
                    let mut out = Vec::new();
                    let _ = broker.handle(subscriber, &packet, &mut out);
                }

                if index % 2 == 0 {
                    if let Some(session) = broker.session_mut(subscriber) {
                        session.on_disconnected(false, 0);
                    }
                }
            }
        });
        shared
    }

    /// Walk every page of `resource`, asserting the total and that
    /// no page after the first is empty. Returns the values of
    /// `field`, in the order they came.
    async fn walk(admin: &AdminRunning, resource: &str, field: &str, total: usize) -> Vec<String> {
        let mut seen = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0;
        loop {
            let path = match &cursor {
                None => format!("/api/v1/{resource}?limit=3"),
                Some(after) => format!("/api/v1/{resource}?limit=3&after={}", escape(after)),
            };
            let response = testing::get(admin.address(), &path).await;
            assert_eq!(response.status, 200, "{}", response.body);
            pages += 1;
            assert!(
                pages <= total + 2,
                "the walk does not end: {}",
                response.body
            );

            let body = &response.body;
            assert!(
                body.contains(&format!("\"total\":{total}")),
                "every page carries the whole count: {body}"
            );
            let here = field_values(body, &format!("\"{field}\":\""));
            assert!(here.len() <= 3, "a page holds at most what was asked");
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
        seen
    }

    /// Walking the subscriptions visits every filter of every
    /// session once, in order, and the total is what the walk gives.
    ///
    /// This is the page whose cursor resumes *inside* a row's parent:
    /// it names a session and a filter, and the session it names has
    /// to be handed back rather than skipped past. Several filters
    /// per session and pages of three make the boundary fall inside a
    /// session rather than between two, which is the case that
    /// distinguishes the two readings.
    #[tokio::test]
    async fn a_walk_of_the_subscriptions_pages_visits_every_row_once_and_then_stops() {
        let sessions = 4;
        let each = 3;
        let total = sessions * each + 1;
        let admin = admin_over(a_broker_subscribing(sessions, each), vec![]).await;

        let filters = walk(&admin, "subscriptions", "filter", total).await;
        let clients = walk(&admin, "subscriptions", "client_id", total).await;

        let mut expected: Vec<(String, String)> = Vec::new();
        for index in 0..sessions {
            let client = format!("client-{index:04}");
            if index == 0 {
                expected.push((client.clone(), "$share/g/shared/topic".to_owned()));
            }
            for filter in 0..each {
                expected.push((client.clone(), format!("site/{filter:02}/telemetry")));
            }
        }
        let got: Vec<(String, String)> = clients.into_iter().zip(filters).collect();
        assert_eq!(got, expected);
        admin.shutdown().await;
    }

    /// The subscriptions total is what paging them gives.
    ///
    /// The total is read from the broker's maintained gauge, which
    /// adds the trie and the shared table; the rows come from what
    /// each session holds. Those are two different structures, and
    /// nothing else here would notice them disagreeing — the page
    /// would simply stop early against a total it never reaches, or
    /// run past one.
    #[tokio::test]
    async fn the_subscriptions_total_is_what_paging_them_gives() {
        let admin = admin_over(a_broker_subscribing(5, 2), vec![]).await;
        let response = testing::get(admin.address(), "/api/v1/subscriptions?limit=1").await;
        assert!(
            response.body.contains("\"total\":11"),
            "five sessions of two filters and one shared: {}",
            response.body
        );
        let rows = walk(&admin, "subscriptions", "filter", 11).await;
        assert_eq!(rows.len(), 11, "and the walk gives exactly that many");
        admin.shutdown().await;
    }

    /// The Clients page shows the connected sessions and counts
    /// them, with the disconnected ones interleaved.
    ///
    /// Every other session here is away, so the page has to skip as
    /// it walks and the total has to be the connected ones rather
    /// than all of them. A page that forgot the filter would show
    /// the absent; a total that counted every session would tell an
    /// operator there are twice as many clients as there are.
    #[tokio::test]
    async fn the_clients_page_is_the_connected_sessions_and_counts_them() {
        let admin = admin_over(a_broker_subscribing(6, 1), vec![]).await;

        let all = walk(&admin, "sessions", "client_id", 6).await;
        assert_eq!(all.len(), 6);

        let connected = walk(&admin, "clients", "client_id", 3).await;
        assert_eq!(
            connected,
            ["client-0001", "client-0003", "client-0005"],
            "the odd-numbered ones are the ones still here"
        );
        admin.shutdown().await;
    }

    /// A broker holding `count` retained messages, on topics that do
    /// not sort in the order they are written.
    ///
    /// Reversed rather than shuffled, because a page that comes out
    /// in order only because it was written in order proves nothing,
    /// and a deterministic wrong order is a better test than a
    /// random one.
    fn a_broker_retaining(count: usize) -> Arc<Shared> {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        shared.with_broker(|broker: &mut Broker| {
            for index in (0..count).rev() {
                broker.retained().set(
                    &format!("site/{index:04}/state"),
                    RetainedMessage {
                        payload: Bytes::from_static(b"21.5"),
                        qos: mqtt_codec::QoS::AtMostOnce,
                        expires_at: None,
                        properties: Arc::new(Properties::default().into()),
                    },
                );
            }
        });
        shared
    }

    /// The same walk over the retained resource, which reaches the
    /// store's ordered index rather than this module's selection.
    ///
    /// The sessions walk above cannot see this one. The two resources
    /// answer a page by different means now — sessions through a
    /// bounded pass, retained through a seek into the store's own
    /// order — so a cursor that is wrong in one is not wrong in the
    /// other, and the boundary between a full final page and a page
    /// with something after it is the case that distinguishes them.
    #[tokio::test]
    async fn a_walk_of_the_retained_pages_visits_every_topic_once_and_then_stops() {
        for count in [0_usize, 1, 4, 5, 6] {
            let admin = admin_over(a_broker_retaining(count), vec![]).await;

            let mut seen = Vec::new();
            let mut pages = 0;
            let mut cursor: Option<String> = None;
            loop {
                let path = match &cursor {
                    None => "/api/v1/retained?limit=5".to_owned(),
                    Some(after) => format!("/api/v1/retained?limit=5&after={}", escape(after)),
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
                let here = field_values(body, "\"topic\":\"");
                assert!(here.len() <= 5, "a page holds at most what was asked");
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
                .map(|index| format!("site/{index:04}/state"))
                .collect();
            assert_eq!(seen, expected, "with {count} topics and pages of five");
            admin.shutdown().await;
        }
    }

    /// A cursor naming a topic that has been cleared since it was
    /// handed out resumes after it.
    ///
    /// The case a reader hits in practice, and the one an offset
    /// cursor gets wrong: read a page, the last topic on it is
    /// cleared, ask for the next page.
    #[tokio::test]
    async fn a_retained_cursor_survives_the_topic_it_names_going_away() {
        let shared = a_broker_retaining(4);
        let admin = admin_over(Arc::clone(&shared), vec![]).await;

        let first = testing::get(admin.address(), "/api/v1/retained?limit=2").await;
        assert_eq!(
            field_values(&first.body, "\"topic\":\""),
            ["site/0000/state", "site/0001/state"]
        );
        let cursor = next_cursor(&first.body).expect("there are two more");

        shared.with_broker(|broker: &mut Broker| broker.retained().remove("site/0001/state"));

        let second = testing::get(
            admin.address(),
            &format!("/api/v1/retained?limit=2&after={}", escape(&cursor)),
        )
        .await;
        assert_eq!(
            field_values(&second.body, "\"topic\":\""),
            ["site/0002/state", "site/0003/state"],
            "{}",
            second.body
        );
        assert!(
            second.body.contains("\"total\":3"),
            "and the total is what the store holds now: {}",
            second.body
        );
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

    // -----------------------------------------------------------
    // Publishing
    // -----------------------------------------------------------

    /// An admin listener over `shared`, with `token` configured.
    async fn admin_with_token(shared: Arc<Shared>, token: Option<&str>) -> AdminRunning {
        let config = AdminConfig {
            address: "127.0.0.1:0".parse().expect("a valid address"),
            token: token.map(crate::Token::new),
            ..AdminConfig::default()
        };
        AdminServer::bind(&config, AdminState::new(shared))
            .await
            .expect("loopback needs no token")
    }

    /// The body of a publish request.
    fn publication(topic: &str, payload: &str, qos: u8) -> String {
        format!("{{\"topic\":\"{topic}\",\"payload\":\"{payload}\",\"qos\":{qos}}}")
    }

    /// The test the whole task turns on.
    ///
    /// Convention 5: a handler that formatted a delivery itself and
    /// pushed it at an outbox would satisfy any assertion about what
    /// the API returned. The only thing that cannot be satisfied by a
    /// parallel implementation is a subscriber on a real socket
    /// receiving the message, because reaching it means going through
    /// the matching, the retention and the queueing a connection's
    /// publish goes through.
    #[tokio::test]
    async fn a_publish_through_the_api_reaches_a_subscriber_over_mqtt() {
        let both = Both::start().await;
        let (mut client, mut buf) = handshake(&both.mqtt, "listener").await;

        send(
            &mut client,
            &Packet::Subscribe(Subscribe {
                packet_id: 1,
                filters: vec![SubscriptionOptions {
                    filter: "from/+/api".into(),
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

        let response = testing::post_json(
            both.admin.address(),
            "/api/v1/publish",
            None,
            &publication("from/the/api", "hello", 1),
        )
        .await;
        assert_eq!(response.status, 200, "{}", response.body);
        assert!(
            response.body.contains("\"delivered\":1"),
            "the answer says how many subscribers it reached: {}",
            response.body
        );

        match next(&mut client, &mut buf).await {
            Some(Packet::Publish(publish)) => {
                assert_eq!(publish.topic, "from/the/api");
                assert_eq!(&publish.payload[..], b"hello");
                // Delivered at the level the subscription granted,
                // which is the matching layer's rule and not this
                // module's.
                assert_eq!(publish.qos, QoS::AtLeastOnce);
            }
            other => panic!("expected the publication, got {other:?}"),
        }

        both.stop().await;
    }

    /// Section 12.3's reserved namespace, through the new door to it.
    ///
    /// **Retained, and the store checked afterwards.** A refusal that
    /// arrives after the publication has been made is a different
    /// answer to the same request and not a refusal at all, and the
    /// version of it that hurts is the retained one, which stays in
    /// the broker until somebody finds it. `a/$b` is asserted beside
    /// it because the rule is about the first level: a check written
    /// as "contains a dollar" would pass every other assertion here.
    #[tokio::test]
    async fn a_publish_into_the_dollar_namespace_is_refused_and_writes_nothing() {
        let both = Both::start().await;
        let address = both.admin.address();

        for topic in ["$SYS/broker/uptime", "$share/group/x", "$"] {
            let refused = testing::post_json(
                address,
                "/api/v1/publish",
                None,
                &format!("{{\"topic\":\"{topic}\",\"payload\":\"x\",\"retain\":true}}"),
            )
            .await;
            assert_eq!(refused.status, 400, "{topic}: {}", refused.body);
            assert!(
                refused.body.contains("\"code\":\"invalid_request\""),
                "{topic}: {}",
                refused.body
            );
            assert!(
                refused.body.contains("\"field\":\"topic\""),
                "a form can mark the input without reading the sentence: {}",
                refused.body
            );
        }

        let retained = both.get("/api/v1/retained").await;
        assert!(
            !retained.body.contains('$'),
            "nothing was written: {}",
            retained.body
        );

        // And a dollar below the first level is an ordinary topic,
        // which is what the matching layer already believes.
        let allowed = testing::post_json(
            address,
            "/api/v1/publish",
            None,
            "{\"topic\":\"ordinary/$below\",\"payload\":\"x\",\"retain\":true}",
        )
        .await;
        assert_eq!(allowed.status, 200, "{}", allowed.body);

        both.stop().await;
    }

    /// Retention through the API is the store's rule, not a second one.
    ///
    /// A retained publish and then an empty one to the same topic,
    /// because "an empty payload clears the topic" is exactly the
    /// kind of rule a parallel implementation would not have.
    #[tokio::test]
    async fn a_retained_publish_through_the_api_obeys_the_retention_rules() {
        let both = Both::start().await;
        let address = both.admin.address();

        let response = testing::post_json(
            address,
            "/api/v1/publish",
            None,
            "{\"topic\":\"kept/here\",\"payload\":\"first\",\"retain\":true}",
        )
        .await;
        assert_eq!(response.status, 200, "{}", response.body);

        let retained = both.get("/api/v1/retained").await;
        assert!(
            retained.body.contains("\"topic\":\"kept/here\""),
            "{}",
            retained.body
        );
        assert!(
            retained.body.contains("\"text\":\"first\""),
            "{}",
            retained.body
        );

        let response = testing::post_json(
            address,
            "/api/v1/publish",
            None,
            "{\"topic\":\"kept/here\",\"payload\":\"\",\"retain\":true}",
        )
        .await;
        assert_eq!(response.status, 200, "{}", response.body);

        let retained = both.get("/api/v1/retained").await;
        assert!(
            !retained.body.contains("kept/here"),
            "an empty retained payload clears the topic: {}",
            retained.body
        );

        both.stop().await;
    }

    /// The three cases the token rule has, against one listener.
    ///
    /// Convention 5 again: each on its own passes against a guard
    /// that refuses everything or nothing, so all three are asserted
    /// together.
    #[tokio::test]
    async fn the_token_is_required_and_must_be_the_right_one() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let admin = admin_with_token(shared, Some("the-right-one")).await;
        let body = publication("guarded/topic", "x", 0);

        let refused = testing::post_json(admin.address(), "/api/v1/publish", None, &body).await;
        assert_eq!(refused.status, 401, "{}", refused.body);
        assert!(
            refused.body.contains("\"code\":\"unauthorized\""),
            "the one error shape, with a code: {}",
            refused.body
        );

        let wrong = testing::post_json(
            admin.address(),
            "/api/v1/publish",
            Some("the-wrong-one"),
            &body,
        )
        .await;
        assert_eq!(wrong.status, 401, "{}", wrong.body);

        // A prefix of the right token, which is what a comparison
        // that stopped at the first difference would be generous to
        // in the one way that matters.
        let prefix =
            testing::post_json(admin.address(), "/api/v1/publish", Some("the-right"), &body).await;
        assert_eq!(prefix.status, 401, "{}", prefix.body);

        let accepted = testing::post_json(
            admin.address(),
            "/api/v1/publish",
            Some("the-right-one"),
            &body,
        )
        .await;
        assert_eq!(accepted.status, 200, "{}", accepted.body);

        admin.shutdown().await;
    }

    /// With no token configured there is nothing to present.
    ///
    /// The other half of the rule. Without it, a handler that
    /// refused every request would pass the test above.
    #[tokio::test]
    async fn no_token_configured_means_none_is_asked_for() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let admin = admin_with_token(shared, None).await;
        let response = testing::post_json(
            admin.address(),
            "/api/v1/publish",
            None,
            &publication("open/topic", "x", 0),
        )
        .await;
        assert_eq!(response.status, 200, "{}", response.body);
        admin.shutdown().await;
    }

    /// A reader is not a mutation and is not guarded by the token.
    ///
    /// Section 11.1: *mutating* requests need the key. Guarding the
    /// readers too would be a different rule, and one the interface
    /// is not written against.
    #[tokio::test]
    async fn a_read_needs_no_token_even_when_one_is_configured() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let admin = admin_with_token(shared, Some("a-secret")).await;
        let response = testing::get(admin.address(), "/api/v1/sessions").await;
        assert_eq!(response.status, 200, "{}", response.body);
        admin.shutdown().await;
    }

    /// A broker whose store's flush this test controls, and an admin
    /// listener over it.
    async fn admin_over_a_gated_store() -> (AdminRunning, Arc<GatedStore>) {
        let store = Arc::new(GatedStore::new());
        let writer = storage::Writer::spawn(
            Arc::clone(&store) as Arc<dyn storage::Storage>,
            storage::WriterConfig::default(),
        )
        .expect("a writer thread");
        let shared = Arc::new(Shared::with_store(
            Broker::new(BrokerConfig::default()),
            Arc::new(writer),
        ));
        (admin_with_token(shared, None).await, store)
    }

    /// Section 7.3, through the API: the response is the promise.
    ///
    /// The same shape as the connection's test and for the same
    /// reason. An assertion that the response eventually arrives
    /// passes against a handler that answers first and writes later,
    /// so the only thing that can tell the two apart is a store that
    /// has not answered yet.
    #[tokio::test]
    async fn an_at_least_once_publish_does_not_answer_until_it_is_durable() {
        let (admin, store) = admin_over_a_gated_store().await;
        store.hold();
        let address = admin.address();

        let mut asking = tokio::spawn(async move {
            testing::post_json(
                address,
                "/api/v1/publish",
                None,
                &publication("durable/topic", "x", 1),
            )
            .await
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(400), &mut asking)
                .await
                .is_err(),
            "the answer overtook the flush it is a promise about"
        );

        store.release();
        let response = tokio::time::timeout(REPLY, asking)
            .await
            .expect("the answer arrives once the device does")
            .expect("the request task did not panic");
        assert_eq!(response.status, 200, "{}", response.body);
        assert!(
            response.body.contains("\"durable\":true"),
            "and says it waited: {}",
            response.body
        );
        assert!(store.flushes() > 0, "the store was asked for a flush");

        admin.shutdown().await;
    }

    /// The other side of the decision.
    ///
    /// At most once promises nobody anything, and section 7.3 names
    /// that level as the one never waited on. A handler that waited
    /// on everything would put every publish behind the device.
    #[tokio::test]
    async fn an_at_most_once_publish_is_not_held_up_by_the_store() {
        let (admin, store) = admin_over_a_gated_store().await;
        store.hold();

        let response = tokio::time::timeout(
            Duration::from_millis(2_000),
            testing::post_json(
                admin.address(),
                "/api/v1/publish",
                None,
                &publication("undurable/topic", "x", 0),
            ),
        )
        .await
        .expect("at most once waits for nothing");
        assert_eq!(response.status, 200, "{}", response.body);
        assert!(
            response.body.contains("\"durable\":false"),
            "and says it did not wait: {}",
            response.body
        );

        store.release();
        admin.shutdown().await;
    }

    /// A publish that cannot mean what it says takes the one shape.
    #[tokio::test]
    async fn a_publish_that_will_not_do_answers_in_the_one_shape() {
        let both = Both::start().await;
        let address = both.admin.address();

        let cases: [(&str, u16, &str, Option<&str>); 6] = [
            // A wildcard is a filter, and a publish names a topic.
            (
                "{\"topic\":\"a/+/b\",\"payload\":\"x\"}",
                400,
                "invalid_request",
                Some("topic"),
            ),
            (
                "{\"topic\":\"a/#\",\"payload\":\"x\"}",
                400,
                "invalid_request",
                Some("topic"),
            ),
            (
                "{\"topic\":\"\",\"payload\":\"x\"}",
                400,
                "invalid_request",
                Some("topic"),
            ),
            (
                "{\"topic\":\"a/b\",\"payload\":\"x\",\"qos\":3}",
                400,
                "invalid_request",
                Some("qos"),
            ),
            // Not JSON at all.
            ("{", 400, "invalid_request", None),
            // A key this endpoint does not know, which a client that
            // misspelled one would otherwise read as honoured.
            (
                "{\"topic\":\"a/b\",\"paylod\":\"x\"}",
                400,
                "invalid_request",
                None,
            ),
        ];
        for (body, status, code, field) in cases {
            let response = testing::post_json(address, "/api/v1/publish", None, body).await;
            assert_eq!(response.status, status, "{body}: {}", response.body);
            assert_eq!(
                response.header("content-type"),
                Some("application/json"),
                "{body}"
            );
            assert!(
                response.body.starts_with("{\"error\":"),
                "{body} nests under `error`: {}",
                response.body
            );
            assert!(
                response.body.contains(&format!("\"code\":\"{code}\"")),
                "{body}: {}",
                response.body
            );
            if let Some(field) = field {
                assert!(
                    response.body.contains(&format!("\"field\":\"{field}\"")),
                    "{body} names the parameter at fault: {}",
                    response.body
                );
            }
        }

        // And a method the path does not accept.
        let response = testing::request(address, "GET", "/api/v1/publish", None).await;
        assert_eq!(response.status, 405, "{}", response.body);

        both.stop().await;
    }

    /// The comparison the token is checked with.
    ///
    /// A unit test of the predicate, which convention 5 says is not
    /// a test of the caller — the caller is covered by
    /// [`the_token_is_required_and_must_be_the_right_one`] over a
    /// socket. This is here for the cases a socket test would need a
    /// server each for.
    #[test]
    fn the_secret_comparison_answers_what_equality_would() {
        assert!(secret_eq("abc", "abc"));
        assert!(!secret_eq("abc", "abd"));
        assert!(!secret_eq("abc", "abcd"), "a prefix is not a match");
        assert!(!secret_eq("abcd", "abc"), "nor is an extension");
        assert!(!secret_eq("", "abc"));
        assert!(!secret_eq("abc", ""));
        assert!(secret_eq("", ""));
        // Not a character comparison: a token is bytes, and two
        // different strings that happen to share a prefix must not
        // match on it.
        assert!(!secret_eq("\u{00e9}", "e"));
    }

    // -----------------------------------------------------------
    // What a page costs the one lock in the process
    // -----------------------------------------------------------

    /// How wide the zero padding on a generated name is.
    ///
    /// Six digits, so that a hundred thousand of them sort in the
    /// order they were made and a cursor into the middle of the
    /// collection can be written down by hand.
    const WIDTH: usize = 6;

    /// A broker holding `count` sessions, `count` subscriptions and
    /// `count` retained messages, with `connected` saying which
    /// sessions kept their connection.
    ///
    /// A parameter rather than a rule, because which sessions are
    /// connected is the only thing the Clients page's cost depends
    /// on, and a builder that decides it for every measurement
    /// decides what they can show. See
    /// [`what_the_clients_page_costs_when_few_are_connected`].
    fn a_broker_holding_everything_where(
        count: usize,
        connected: impl Fn(usize) -> bool,
    ) -> Arc<Shared> {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        shared.with_broker(|broker: &mut Broker| {
            for index in 0..count {
                let connect = Connect {
                    version: mqtt_codec::Version::V5,
                    client_id: format!("client-{index:0WIDTH$}"),
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
                let mut out = Vec::new();
                let accepted = broker
                    .connect("", &connect, &mut out)
                    .expect("the default decision authenticates");
                let subscriber = accepted.subscriber;

                let subscribe = Packet::Subscribe(Subscribe {
                    packet_id: 1,
                    filters: vec![SubscriptionOptions {
                        filter: format!("site/{index:0WIDTH$}/telemetry"),
                        qos: mqtt_codec::QoS::AtMostOnce,
                        no_local: false,
                        retain_as_published: false,
                        retain_handling: mqtt_codec::packet::RetainHandling::SendAlways,
                    }],
                    properties: Properties::default(),
                });
                let mut out = Vec::new();
                let _ = broker.handle(subscriber, &subscribe, &mut out);

                broker.retained().set(
                    &format!("site/{index:0WIDTH$}/state"),
                    RetainedMessage {
                        payload: Bytes::from_static(b"21.5"),
                        qos: mqtt_codec::QoS::AtMostOnce,
                        expires_at: None,
                        properties: Arc::new(Properties::default().into()),
                    },
                );

                if !connected(index) {
                    if let Some(session) = broker.session_mut(subscriber) {
                        session.on_disconnected(false, 0);
                    }
                }
            }
        });
        shared
    }

    /// The same broker with every other session connected.
    ///
    /// Every session is connected except the even-numbered ones,
    /// because the Clients page filters and a population that is
    /// entirely connected would never make it skip a row.
    ///
    /// It is **not** the Clients page's worst case either: a filter
    /// that rejects every other row skips one for each row it takes,
    /// so a page still costs a page. What that costs when it does
    /// not is measured separately, by
    /// [`what_the_clients_page_costs_when_few_are_connected`].
    fn a_broker_holding_everything(count: usize) -> Arc<Shared> {
        a_broker_holding_everything_where(count, |index| index % 2 != 0)
    }

    /// Run `body` twenty times and print the mean and the worst.
    fn timed(what: &str, population: usize, mut body: impl FnMut()) {
        let runs = 20u32;
        let mut worst = Duration::ZERO;
        let mut total = Duration::ZERO;
        for _ in 0..runs {
            let started = std::time::Instant::now();
            body();
            let took = started.elapsed();
            worst = worst.max(took);
            total += took;
        }
        println!(
            "  {population:>7}  {what:<26} {:>12?} mean, {:>12?} worst",
            total / runs,
            worst
        );
    }

    /// What one page of each resource holds the broker's one lock
    /// for, at the three populations the design targets.
    ///
    /// **A measurement, not an assertion**, which is why it is
    /// ignored: a wall clock on a shared machine is not a thing to
    /// gate a build on. What is asserted instead is the algorithm —
    /// `a_page_of_a_large_broker_is_still_a_page` for the page, and
    /// the drift properties in `broker-core` for the maintained
    /// numbers a bounded page is built on.
    ///
    /// Two cursors per resource, because they are different
    /// questions. The first page can be answered by whatever the
    /// iteration order happens to be; a page from the middle can
    /// only be answered cheaply by a structure that can seek.
    ///
    ///     cargo test -p sdmqtt-admin --release -- --ignored --nocapture what_a_page_costs
    #[test]
    #[ignore = "a measurement, not an assertion"]
    fn what_a_page_costs_under_the_lock() {
        println!("one page of {DEFAULT_LIMIT}, under the broker lock");
        for population in [1_000usize, 10_000, 100_000] {
            let shared = a_broker_holding_everything(population);
            let middle = population / 2;
            let client = format!("client-{middle:0WIDTH$}");
            let topic = format!("site/{middle:0WIDTH$}/state");
            let filter = format!("site/{middle:0WIDTH$}/telemetry");

            for (where_from, sessions_after, retained_after, subscriptions_after) in [
                ("from the start", None, None, None),
                (
                    "from the middle",
                    Some([String::new(), client.clone(), String::new()]),
                    Some([topic.clone(), String::new(), String::new()]),
                    Some([String::new(), client.clone(), filter.clone()]),
                ),
            ] {
                let paging = |after: Option<[String; PARTS]>| Paging {
                    limit: DEFAULT_LIMIT,
                    after,
                };

                let sessions = paging(sessions_after);
                timed(&format!("sessions, {where_from}"), population, || {
                    let page = shared.with_broker(|broker| session_page(broker, &sessions, false));
                    std::hint::black_box(page);
                });

                let clients = paging(
                    where_from
                        .ends_with("middle")
                        .then(|| [String::new(), client.clone(), String::new()]),
                );
                timed(&format!("clients, {where_from}"), population, || {
                    let page = shared.with_broker(|broker| session_page(broker, &clients, true));
                    std::hint::black_box(page);
                });

                let subs = paging(subscriptions_after);
                timed(&format!("subscriptions, {where_from}"), population, || {
                    let page = shared.with_broker(|broker| subscription_page(broker, &subs));
                    std::hint::black_box(page);
                });

                let held = paging(retained_after);
                timed(&format!("retained, {where_from}"), population, || {
                    let entries = shared.with_broker(|broker| retained_held(broker, &held));
                    std::hint::black_box(entries);
                });
            }
        }
    }

    /// What the Clients page costs when few of many are connected.
    ///
    /// **The case [`what_a_page_costs_under_the_lock`] cannot
    /// show.** Its population has every other session connected, so
    /// the filter in [`session_page`] rejects one row for each row
    /// it takes and a page still costs a page. The number it prints
    /// beside "clients" is therefore a bound on the Clients page
    /// only for the population on which the Clients page is not the
    /// one that walks — convention 5's "test measuring the wrong
    /// dimension", wearing a measurement's clothes rather than an
    /// assertion's.
    ///
    /// Here ten sessions of the whole population are connected, and
    /// they are the last ten in identity order. That is the worst
    /// case [`session_page`]'s own documentation names: a page
    /// cannot fill until the walk reaches the end, so it pays for
    /// the population and not for the page. The same page over the
    /// same population with everything connected is measured beside
    /// it, because a number with nothing to compare it against says
    /// how fast this machine is and not what the filter costs.
    ///
    ///     cargo test -p sdmqtt-admin --release -- --ignored --nocapture what_the_clients_page_costs
    #[test]
    #[ignore = "a measurement, not an assertion"]
    fn what_the_clients_page_costs_when_few_are_connected() {
        /// How many of the population kept their connection.
        const CONNECTED: usize = 10;

        println!("one Clients page of {DEFAULT_LIMIT}, under the broker lock");
        for population in [1_000usize, 10_000, 100_000] {
            let paging = Paging {
                limit: DEFAULT_LIMIT,
                after: None,
            };

            let few = a_broker_holding_everything_where(population, |index| {
                index >= population - CONNECTED
            });
            timed(
                &format!("{CONNECTED} of them connected"),
                population,
                || {
                    let page = few.with_broker(|broker| session_page(broker, &paging, true));
                    std::hint::black_box(page);
                },
            );
            drop(few);

            let all = a_broker_holding_everything_where(population, |_| true);
            timed("all of them connected", population, || {
                let page = all.with_broker(|broker| session_page(broker, &paging, true));
                std::hint::black_box(page);
            });
            drop(all);
        }
    }

    /// What the two gauges that walk cost, beside the three that do
    /// not.
    ///
    ///     cargo test -p sdmqtt-admin --release -- --ignored --nocapture what_the_gauges_cost
    #[test]
    #[ignore = "a measurement, not an assertion"]
    fn what_the_gauges_cost() {
        println!("one read of each gauge, under the broker lock");
        for population in [1_000usize, 10_000, 100_000] {
            let shared = a_broker_holding_everything(population);
            timed("session_count", population, || {
                std::hint::black_box(shared.with_broker(|broker| broker.session_count()));
            });
            timed("queue_depth", population, || {
                std::hint::black_box(shared.with_broker(|broker| broker.queue_depth()));
            });
            timed("subscription_count", population, || {
                std::hint::black_box(shared.with_broker(|broker| broker.subscription_count()));
            });
            timed("retained_count", population, || {
                std::hint::black_box(shared.with_broker(|broker| broker.retained_count()));
            });
            timed("all five gauges", population, || {
                std::hint::black_box(shared.with_broker(|broker| {
                    (
                        broker.session_count(),
                        broker.subscription_count(),
                        broker.retained_count(),
                        broker.queue_depth(),
                    )
                }));
            });
        }
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
