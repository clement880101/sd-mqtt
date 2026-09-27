//! Rules, read and written over the API. Sections 9.1 and 11.1.
//!
//! List, read, create, update, enable, disable and delete. Mutating
//! requests need the token, as every mutating request on this listener
//! does.
//!
//! # There is no second validator here
//!
//! This module does the HTTP and nothing else about what a rule is. A
//! body is deserialised into a [`RuleSpec`] — the same type a
//! `[[rules]]` table in the configuration file is read into — and
//! handed to [`RuleSpec::to_rule`], which is the one place a rule is
//! accepted in this process.
//!
//! That matters because the refusals have arguments behind them and
//! the arguments are not obvious. An `http_in` action with no token is
//! a hole rather than a degraded mode, because the endpoint publishes.
//! A bridge whose inbound and outbound filters intersect after
//! rewriting is a storm, caught at configuration time because the
//! marker that would otherwise catch it does not exist in MQTT 3.1.1.
//! A trigger on an action that is not fired by a message is an
//! operator believing something about when their rule runs that is not
//! true. A second set of those checks written here is a second set
//! that drifts the first time one of them is corrected, which is the
//! argument that already put publishing behind one shared call.
//!
//! What this module adds is the HTTP: a code, a status, and the name
//! of the field at fault — which comes from the refusal itself, since
//! every error type in the rules crate answers `field()`.
//!
//! # Nothing is walked under the broker lock
//!
//! Nothing here takes the broker lock at all. The installed rules are
//! behind the registry's own lock, which is read to clone out a page
//! and released before anything is serialised, exactly as
//! [`crate::resources`] does with the broker's. The registry's write
//! lock is taken by an install and never held across an `await`; see
//! [`::rules::live`].
//!
//! **A rule's outbox depth is not read here at all.** It is a stored
//! counter, and walking every rule's outbox to answer a listing is the
//! mistake this crate's other resources were fixed for, with a new
//! name. The one place a depth is read is a delete, which reads one
//! rule's maintained number to decide whether it is about to throw
//! away messages this broker accepted.
//!
//! # Secrets go in and do not come back out
//!
//! Section 12.3: a token or a password is held in a wrapper that
//! refuses to print itself. A rule read back over this API therefore
//! carries **no** `http_in.token` and **no** `mqtt_bridge.password`,
//! and the read resources are not behind the token, so that is not a
//! nicety.
//!
//! The consequence is that the obvious edit — read a rule, change one
//! field, write it back — would otherwise erase the secret. So an
//! update that leaves a secret out **keeps the stored one**. Clearing
//! one means deleting the rule and creating it again, which is the
//! right amount of ceremony for taking the guard off an endpoint that
//! publishes.
//!
//! # A node with no store holds no rules
//!
//! Section 9.1 makes a rule durable metadata, and a broker started
//! with no `storage.directory` writes nothing down. Such a node
//! answers [`ErrorCode::NotFound`] here rather than an empty
//! collection, for the reason [`crate::resources`] gives about the
//! resources of later milestones: an interface reads an empty
//! collection as "you have no rules" rather than "this node cannot
//! have any".

use std::collections::BTreeMap;
use std::sync::Arc;

use ::rules::http_out::redacted_url;
use ::rules::live::{InstallError, Registry};
use ::rules::rule::{Rule, RuleId};
use ::rules::spec::{RuleSpec, SpecError};
use ::rules::stats::{Sample, Totals, SAMPLE_INTERVAL};
use ::rules::Action;
use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::error::{ApiError, ErrorCode};
use crate::resources::{page, select, Authorized, Page, Paging};
use serde::Serialize;

use crate::AdminState;

/// The registry this node serves, or the refusal for a node with none.
fn registry(state: &AdminState) -> Result<&Arc<Registry>, ApiError> {
    state.rules.as_ref().ok_or_else(|| {
        ApiError::not_found(
            "this node has no store, so it holds no rules: section 9.1 makes a rule \
             durable metadata, and a broker started without storage.directory writes \
             nothing down",
        )
    })
}

/// A rule this node holds, as the API spells one.
///
/// The written shape and nothing beside it, so that what a listing
/// gives back is what a create would take. What each rule has *been
/// doing* is a resource of its own; a rule and its statistics change
/// at completely different rates and joining them would make a page
/// that only wants the second refetch the first.
type RuleView = RuleSpec;

/// What refusing a rule looks like over HTTP.
///
/// The sentence is the refusal's own, so that a configuration file and
/// this API say the same thing about the same mistake; the field is
/// the one the refusal named, so a form can mark the input rather than
/// parse the sentence.
fn refusal(error: SpecError) -> ApiError {
    ApiError::in_field(ErrorCode::InvalidRequest, error.field, error.why)
}

/// What failing to install a rule looks like over HTTP.
fn install_refusal(error: &InstallError) -> ApiError {
    match error {
        // The caller did nothing wrong and cannot fix it.
        InstallError::Storage { .. } => ApiError::new(ErrorCode::Internal, error.to_string()),
        InstallError::NoSuchRule { .. } => ApiError::not_found(error.to_string()),
        // Well formed, and would succeed once the outbox has drained.
        InstallError::OutboxNotEmpty { .. } => {
            ApiError::in_field(ErrorCode::Conflict, "id", error.to_string())
        }
        // Well formed, and disagrees with what is there. Raised by the
        // registry rather than here, because a check this handler made
        // itself would be a check-then-act across an await.
        InstallError::AlreadyExists { .. } => {
            ApiError::in_field(ErrorCode::Conflict, "id", error.to_string())
        }
        // The caller did nothing wrong either, and this is the
        // failure this endpoint used to hide: an ingest rule with
        // nowhere to be reached is refused here rather than created
        // and reported as working. Not `invalid_request`, because
        // the same body on a node whose ingest address is free is
        // accepted.
        InstallError::Listener { .. } => ApiError::new(ErrorCode::Internal, error.to_string()),
        // The request would extend a cycle of connectors, which is
        // what that code is for. Not `invalid_request`: this body is
        // well formed and would be accepted on a node that did not
        // hold the other rule.
        // The body is fine and names a field; a node with a larger
        // maximum packet size would accept it unchanged.
        InstallError::Unusable { field, .. } => {
            ApiError::in_field(ErrorCode::InvalidRequest, field, error.to_string())
        }
        InstallError::Loops { .. } => ApiError::in_field(
            ErrorCode::LoopDetected,
            "mqtt_bridge.topics",
            error.to_string(),
        ),
    }
}

/// Read a body in the one error shape.
///
/// Axum's own rejection is plain text, and section 13 says this API
/// answers one shape and no other.
fn body(body: Result<Json<RuleSpec>, JsonRejection>) -> Result<RuleSpec, ApiError> {
    body.map(|Json(spec)| spec).map_err(|rejection| {
        ApiError::invalid_request(format!(
            "the request body is not a rule: {}",
            rejection.body_text()
        ))
    })
}

/// The identifier in the path, or the refusal for one no rule can have.
///
/// Checked here as well as by [`RuleSpec::to_rule`], because a path is
/// not a body: `GET /api/v1/rules/a/b` is a path that no rule could
/// ever be at, and answering "no such rule" is the honest answer to it.
fn identifier(id: &str) -> Result<RuleId, ApiError> {
    RuleId::new(id)
        .map_err(|error| ApiError::in_field(ErrorCode::NotFound, "id", error.to_string()))
}

/// Carry a secret the caller left out over from the stored rule.
///
/// See the module note. A spec arrives from a caller who read the rule
/// back and changed one field, which means the secrets are missing
/// because this API does not give them out, not because the caller
/// means to remove them.
///
/// **Four of them**, not two. A header value is the third and is one
/// with no type saying so: a rule's headers are where its bearer token
/// goes, they are written back out with their names and empty values,
/// and an empty value here therefore means "the one you already have"
/// rather than "send nothing".
///
/// The fourth is the **URL**, which for a chat platform's webhook is
/// the whole of the credential. It is written back out as its scheme
/// and host with the rest elided, so a caller sending back exactly
/// what it was given means "the one you already have" too; anything
/// else is a caller repointing the rule, which is a change they are
/// entitled to make. Compared against the redaction of the stored URL
/// rather than against the marker alone, so that pasting one rule's
/// redacted URL into another rule cannot pick up that other rule's
/// secret.
fn keep_secrets(spec: &mut RuleSpec, stored: &Rule) {
    if let (Some(written), Action::HttpOut(held)) = (&mut spec.http_out, &stored.action) {
        if written.url == redacted_url(&held.url) {
            written.url.clone_from(&held.url);
        }
    }
    if let (Some(written), Action::HttpPoll(held)) = (&mut spec.http_poll, &stored.action) {
        if written.url == redacted_url(&held.url) {
            written.url.clone_from(&held.url);
        }
    }
    match (&mut spec.http_in, &stored.action) {
        (Some(written), Action::HttpIn(held)) if written.token.is_empty() => {
            written.token = held.token.clone();
        }
        _ => {}
    }
    match (&mut spec.mqtt_bridge, &stored.action) {
        (Some(written), Action::MqttBridge(held)) if written.password.is_none() => {
            written.password.clone_from(&held.password);
        }
        _ => {}
    }
    if let (Some(written), Action::HttpOut(held)) = (&mut spec.http_out, &stored.action) {
        keep_headers(&mut written.headers, &held.headers);
    }
    if let (Some(written), Action::HttpPoll(held)) = (&mut spec.http_poll, &stored.action) {
        keep_headers(&mut written.headers, &held.headers);
    }
}

/// Fill in the value of every header the caller sent empty, from the
/// header of that name the rule already had.
///
/// A header the stored rule does not have keeps its empty value, which
/// is a legal header and is what an operator who wrote one means.
fn keep_headers(written: &mut BTreeMap<String, String>, held: &[(String, String)]) {
    for (name, value) in written {
        if !value.is_empty() {
            continue;
        }
        if let Some((_, had)) = held.iter().find(|(had, _)| had == name) {
            value.clone_from(had);
        }
    }
}

/// Every rule this node holds, a page at a time.
///
/// Cloned out of the registry under its read lock and serialised long
/// after it was released: this handler returns `Json` of an owned
/// value and calls no serialiser itself, which is the shape
/// [`crate::resources`] explains at length.
async fn list(
    State(state): State<AdminState>,
    paging: Paging,
) -> Result<Json<Page<RuleView>>, ApiError> {
    let held = registry(&state)?.list();
    let keys: Vec<_> = held
        .iter()
        .map(|rule| ([rule.id.as_str(), "", ""], rule))
        .collect();
    let selection = select(keys.into_iter(), paging.after.as_ref(), paging.limit);
    Ok(Json(page(selection, |_, rule| RuleSpec::of(rule))))
}

/// One rule.
async fn read(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<RuleView>, ApiError> {
    let id = identifier(&id)?;
    let rule = registry(&state)?
        .get(&id)
        .ok_or_else(|| ApiError::not_found(format!("there is no rule called {:?}", id.as_str())))?;
    Ok(Json(RuleSpec::of(&rule)))
}

/// Create a rule.
///
/// Refused when one of that name already exists, rather than quietly
/// replacing it: a create that overwrote something would make two
/// operators writing the same identifier a race whose loser never
/// finds out. Changing one is [`update`].
async fn create(
    State(state): State<AdminState>,
    _authorized: Authorized,
    request: Result<Json<RuleSpec>, JsonRejection>,
) -> Result<Json<RuleView>, ApiError> {
    let registry = registry(&state)?;
    let spec = body(request)?;
    // Through the one conversion, before anything is looked up: a
    // body that is not a rule is refused whatever is already stored.
    let rule = spec.to_rule().map_err(refusal)?;
    // **The look and the install are one call**, because the registry
    // is where they can be one change. A `get` here followed by an
    // `install` is two, and two operators writing this identifier at
    // the same moment both pass the first and both run the second —
    // which is exactly the race this endpoint refuses replacement to
    // avoid, with the refusal not doing it.
    registry
        .create(rule.clone())
        .await
        .map_err(|error| install_refusal(&error))?;
    Ok(Json(RuleSpec::of(&rule)))
}

/// Replace a rule, or create one at a name nothing holds.
///
/// The whole rule, not a patch. A partial update of a rule would mean
/// deciding what an absent action table means — leave the action
/// alone, or remove it — and neither answer is one an operator could
/// guess. The one exception is a secret, which this API never gives
/// out and therefore cannot ask a caller to send back; see the module
/// note.
async fn update(
    State(state): State<AdminState>,
    _authorized: Authorized,
    Path(id): Path<String>,
    request: Result<Json<RuleSpec>, JsonRejection>,
) -> Result<Json<RuleView>, ApiError> {
    let registry = registry(&state)?;
    let id = identifier(&id)?;
    let mut spec = body(request)?;
    if spec.id != id.as_str() {
        return Err(ApiError::in_field(
            ErrorCode::InvalidRequest,
            "id",
            format!(
                "this is /api/v1/rules/{} and the body calls the rule {:?}; a rule's \
                 identifier is what names its outbox and its dead-letter topic, so it is \
                 not something an update changes",
                id.as_str(),
                spec.id
            ),
        ));
    }
    if let Some(stored) = registry.get(&id) {
        keep_secrets(&mut spec, &stored);
    }
    let rule = spec.to_rule().map_err(refusal)?;
    registry
        .install(rule.clone())
        .await
        .map_err(|error| install_refusal(&error))?;
    Ok(Json(RuleSpec::of(&rule)))
}

/// Turn a rule on.
async fn enable(
    State(state): State<AdminState>,
    _authorized: Authorized,
    Path(id): Path<String>,
) -> Result<Json<RuleView>, ApiError> {
    switch(&state, &id, true).await
}

/// Turn a rule off, without deleting it.
async fn disable(
    State(state): State<AdminState>,
    _authorized: Authorized,
    Path(id): Path<String>,
) -> Result<Json<RuleView>, ApiError> {
    switch(&state, &id, false).await
}

/// What both of the two above do.
async fn switch(state: &AdminState, id: &str, enabled: bool) -> Result<Json<RuleView>, ApiError> {
    let id = identifier(id)?;
    let rule = registry(state)?
        .set_enabled(&id, enabled)
        .await
        .map_err(|error| install_refusal(&error))?;
    Ok(Json(RuleSpec::of(&rule)))
}

/// Delete a rule and everything running for it.
///
/// Refused while its outbox holds anything, because deleting a rule
/// deletes its outbox and what is in one is messages this broker told
/// somebody it had accepted. See [`::rules::live`].
async fn delete(
    State(state): State<AdminState>,
    _authorized: Authorized,
    Path(id): Path<String>,
) -> Result<Json<Deleted>, ApiError> {
    let id = identifier(&id)?;
    registry(&state)?
        .remove(&id)
        .await
        .map_err(|error| install_refusal(&error))?;
    Ok(Json(Deleted {
        id: id.as_str().to_owned(),
    }))
}

/// What a delete that happened answers.
///
/// A body rather than an empty `204`, so that every answer this API
/// gives is JSON and a client has one thing to parse.
#[derive(Debug, serde::Serialize)]
struct Deleted {
    /// What is gone.
    id: String,
}

// ---------------------------------------------------------------
// What each rule has been doing
// ---------------------------------------------------------------

/// One rule's numbers, and the series behind them.
///
/// A resource of its own rather than fields on [`RuleView`], because
/// a rule and its statistics change at completely different rates: the
/// first changes when an operator edits it and the second changes
/// every second, and a page that polls the second would be refetching
/// the first for nothing.
#[derive(Debug, Serialize)]
struct RuleStatsView {
    /// Whose numbers.
    id: String,
    /// What the rule has done since this process started.
    ///
    /// **Since the process started, not since the rule was created.**
    /// The series below is in memory and so are these; a rule's
    /// durable history of what it delivered is its endpoint's own logs
    /// and its dead-letter topic.
    totals: Totals,
    /// How long one point of the series covers.
    ///
    /// Sent rather than assumed, so that a drawing does not have to
    /// know this module's constant to label an axis.
    interval_seconds: u64,
    /// The series, oldest first, at most one point per
    /// `interval_seconds` for the last minute.
    ///
    /// **Five arrays rather than an array of five-field objects.** A
    /// sparkline wants one of them at a time, and the columnar shape
    /// is both what the drawing reads and about a third of the bytes.
    ///
    /// Shorter than the full minute for a rule installed less than a
    /// minute ago, and empty for one installed less than a second ago.
    /// Both happen constantly and a drawing has to cope with them.
    series: Series,
}

/// The five arrays a sparkline is drawn from.
#[derive(Debug, Default, Serialize)]
struct Series {
    /// How many times the rule fired in each interval.
    fired: Vec<u32>,
    /// How much reached where it was going.
    delivered: Vec<u32>,
    /// How many attempts failed.
    failed: Vec<u32>,
    /// How many envelopes ran out of attempts.
    dead_lettered: Vec<u32>,
    /// How much was waiting in the outbox at each point.
    waiting: Vec<u32>,
}

impl From<Vec<Sample>> for Series {
    fn from(samples: Vec<Sample>) -> Self {
        let mut series = Self::default();
        for sample in samples {
            series.fired.push(sample.fired);
            series.delivered.push(sample.delivered);
            series.failed.push(sample.failed);
            series.dead_lettered.push(sample.dead_lettered);
            series.waiting.push(sample.waiting);
        }
        series
    }
}

/// One row of the statistics resource.
fn stats_view(id: &RuleId, totals: Totals, samples: Vec<Sample>) -> RuleStatsView {
    RuleStatsView {
        id: id.as_str().to_owned(),
        totals,
        interval_seconds: SAMPLE_INTERVAL.as_secs(),
        series: samples.into(),
    }
}

/// What every rule has been doing, a page at a time.
///
/// **Not under `/api/v1/rules/`**, and the reason is the router rather
/// than taste: a static `stats` segment there would take precedence
/// over `{id}`, so a rule an operator called `stats` would be a rule
/// nobody could read back. A resource of its own has no such trap.
async fn statistics(
    State(state): State<AdminState>,
    paging: Paging,
) -> Result<Json<Page<RuleStatsView>>, ApiError> {
    let held = registry(&state)?.statistics();
    let keys: Vec<_> = held
        .iter()
        .map(|(id, totals, samples)| ([id.as_str(), "", ""], (id, totals, samples)))
        .collect();
    let selection = select(keys.into_iter(), paging.after.as_ref(), paging.limit);
    Ok(Json(page(selection, |_, (id, totals, samples)| {
        stats_view(id, *totals, samples.clone())
    })))
}

/// What one rule has been doing.
async fn statistics_of(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<RuleStatsView>, ApiError> {
    let id = identifier(&id)?;
    let (totals, samples) = registry(&state)?
        .statistics_of(&id)
        .ok_or_else(|| ApiError::not_found(format!("there is no rule called {:?}", id.as_str())))?;
    Ok(Json(stats_view(&id, totals, samples)))
}

// ---------------------------------------------------------------
// Dead letters
// ---------------------------------------------------------------

/// What a rule has given up on.
///
/// **A resource rather than a subscription**, and the argument is
/// [`::rules::dlq`]'s: a subscription only shows what is published
/// while somebody is watching, and nobody watches a rules page waiting
/// for a dead letter. An operator finds out from an alert and opens
/// the page afterwards, by which time a tap would have nothing to
/// show — which is indistinguishable from a rule that has never
/// dead-lettered anything.
///
/// Section 9.2 is unchanged: the letter is still published to
/// `$sd/rules/<id>/dlq`, so anything that wants every letter for ever
/// subscribes durably. This is the recent history, bounded and in
/// memory, for the page.
#[derive(Debug, Serialize)]
struct DeadLetterView {
    /// Which letter this is for this rule, counting from one since
    /// the process started.
    sequence: u64,
    /// When it was published, on the wall clock, in whole seconds.
    at: u32,
    /// The topic it went to.
    topic: String,
    /// What was published, as text, cut to what this broker keeps.
    document: String,
    /// Whether `document` is the whole of it. The whole of it is on
    /// the topic.
    truncated: bool,
}

/// One rule's dead letters, most recent first.
///
/// Most recent first because that is the order they are read in, and
/// the ordering key is the sequence counted downwards so that the
/// cursor, the page and the `total` behave exactly as every other
/// resource's do. The collection is bounded by
/// [`::rules::dlq::KEPT`], so a page is very nearly always the whole
/// of it; it is paginated anyway, because one envelope for every
/// resource is one thing for the interface to learn.
async fn dead_letters(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    paging: Paging,
) -> Result<Json<Page<DeadLetterView>>, ApiError> {
    let id = identifier(&id)?;
    let (_produced, kept) = registry(&state)?
        .dead_letters_of(&id)
        .ok_or_else(|| ApiError::not_found(format!("there is no rule called {:?}", id.as_str())))?;
    let keys: Vec<_> = kept
        .iter()
        .map(|letter| (newest_first(letter.sequence), letter))
        .collect();
    let selection = select(
        keys.iter()
            .map(|(key, letter)| ([key.as_str(), "", ""], *letter)),
        paging.after.as_ref(),
        paging.limit,
    );
    Ok(Json(page(selection, |_, letter| DeadLetterView {
        sequence: letter.sequence,
        at: letter.at,
        topic: letter.topic.clone(),
        document: letter.document.clone(),
        truncated: letter.truncated,
    })))
}

/// A sort key that puts the newest letter first.
///
/// The ordering helpers sort ascending, and what an operator wants to
/// see first is the letter that has just arrived. Counted downwards
/// and zero-padded so that it sorts as a number rather than as text.
fn newest_first(sequence: u64) -> String {
    format!("{:020}", u64::MAX - sequence)
}

/// The routes this module owns.
pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/api/v1/rules", get(list).post(create))
        .route("/api/v1/rules/{id}", get(read).put(update).delete(delete))
        .route("/api/v1/rules/{id}/enable", post(enable))
        .route("/api/v1/rules/{id}/disable", post(disable))
        .route("/api/v1/rules/{id}/stats", get(statistics_of))
        .route("/api/v1/rules/{id}/dead-letters", get(dead_letters))
        .route("/api/v1/rule-stats", get(statistics))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use broker_core::broker::{Broker, BrokerConfig};
    use rules::singleton::{Membership, NodeId};
    use sdmqtt_server::publish::Publication;
    use sdmqtt_server::shared::Shared;
    use storage::{MemoryStorage, Storage, Writer, WriterConfig};

    use super::*;
    use crate::testing;
    use crate::{AdminConfig, AdminRunning, AdminServer, AdminState};

    /// How long a thing that should happen is waited for.
    const PATIENCE: Duration = Duration::from_secs(5);

    /// An admin listener, the registry behind it, and the store under
    /// both.
    struct Node {
        admin: AdminRunning,
        store: Arc<dyn Storage>,
        registry: Arc<Registry>,
        shared: Arc<Shared>,
    }

    impl Node {
        /// A broker with a store, a registry over it with its hook
        /// installed, and an admin listener serving both.
        async fn start(token: Option<&str>) -> Self {
            let store: Arc<dyn Storage> = Arc::new(MemoryStorage::new());
            let writer = Arc::new(
                Writer::spawn(Arc::clone(&store), WriterConfig::default())
                    .expect("a writer thread"),
            );
            let shared = Arc::new(Shared::with_store(
                Broker::new(BrokerConfig::default()),
                Arc::clone(&writer),
            ));
            let registry = Registry::new(
                Arc::clone(&shared),
                Arc::clone(&store),
                writer,
                Arc::new(Membership::alone(NodeId::new("local"))),
            );
            // The same wiring the binary does, and the reason a rule
            // created below actually fires: without it there is no
            // hook and the test would be about a list in memory.
            shared.with_broker(|broker| {
                broker.hooks_mut().install(registry.hook());
            });
            let config = AdminConfig {
                address: "127.0.0.1:0".parse().expect("a valid address"),
                token: token.map(crate::Token::new),
                ..AdminConfig::default()
            };
            let admin = AdminServer::bind(
                &config,
                AdminState::new(Arc::clone(&shared)).with_rules(Some(Arc::clone(&registry))),
            )
            .await
            .expect("loopback needs no token");
            Self {
                admin,
                store,
                registry,
                shared,
            }
        }

        /// A node with no store, which is a node that holds no rules.
        async fn without_a_store() -> AdminRunning {
            let shared = Arc::new(Shared::new(BrokerConfig::default()));
            let config = AdminConfig {
                address: "127.0.0.1:0".parse().expect("a valid address"),
                ..AdminConfig::default()
            };
            AdminServer::bind(&config, AdminState::new(shared))
                .await
                .expect("loopback needs no token")
        }

        async fn post(&self, path: &str, body: &str) -> testing::Response {
            testing::post_json(self.admin.address(), path, None, body).await
        }

        async fn put(&self, path: &str, body: &str) -> testing::Response {
            testing::with_body(self.admin.address(), "PUT", path, None, Some(body)).await
        }

        async fn get(&self, path: &str) -> testing::Response {
            testing::get(self.admin.address(), path).await
        }

        async fn delete(&self, path: &str) -> testing::Response {
            testing::request(self.admin.address(), "DELETE", path, None).await
        }

        /// One rule's dead letters, as JSON.
        async fn dead_letters(&self, rule: &str) -> serde_json::Value {
            let answered = self
                .get(&format!("/api/v1/rules/{rule}/dead-letters"))
                .await;
            assert_eq!(answered.status, 200, "{}", answered.body);
            serde_json::from_str(&answered.body).expect("dead letters are JSON")
        }

        /// One rule's statistics, as JSON.
        async fn stats(&self, rule: &str) -> serde_json::Value {
            let answered = self.get(&format!("/api/v1/rules/{rule}/stats")).await;
            assert_eq!(answered.status, 200, "{}", answered.body);
            serde_json::from_str(&answered.body).expect("statistics are JSON")
        }

        /// Wait until `rule`'s outbox holds `expected` envelopes.
        ///
        /// A poll rather than a sleep, because what is being waited
        /// for is the writer's next batch and not a duration.
        async fn until_outbox(&self, rule: &str, expected: u64) -> bool {
            let deadline = Instant::now() + PATIENCE;
            while Instant::now() < deadline {
                if self.store.outbox_depth(rule).expect("the store answers") == expected {
                    return true;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            false
        }

        async fn stop(self) {
            self.registry.shutdown().await;
            self.admin.shutdown().await;
        }
    }

    /// A rule that posts what it sees on `sensor/+/temp` somewhere
    /// nothing is listening.
    ///
    /// **The endpoint is deliberately dead and the backoff is an
    /// hour**, so an envelope that reaches the outbox stays there: the
    /// first attempt is refused, the pump waits, and nothing removes
    /// the entry the assertion is about. A rule whose endpoint
    /// answered would be a race between the assertion and the pump.
    fn posting_rule(id: &str) -> String {
        format!(
            r#"{{"id":"{id}","name":"Telemetry out","filter":"sensor/+/temp",
                 "http_out":{{"url":"http://127.0.0.1:1/","timeout_ms":100,
                              "retries":5,"backoff_ms":3600000}}}}"#
        )
    }

    /// One publication through the API, which goes through the broker
    /// and therefore through the hook.
    fn publication(topic: &str) -> String {
        format!(r#"{{"topic":"{topic}","payload":"25.5","qos":0}}"#)
    }

    /// Percent-encode a cursor for a query string.
    ///
    /// The separator a cursor is built with is a control character,
    /// which does not survive a request line unescaped.
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

    /// What the `error` object of a failure said.
    fn error_of(body: &str) -> (String, String) {
        let value: serde_json::Value = serde_json::from_str(body).expect("a JSON body");
        let error = value.get("error").expect("a failure is nested under error");
        (
            error["code"].as_str().unwrap_or_default().to_owned(),
            error["field"].as_str().unwrap_or_default().to_owned(),
        )
    }

    // -----------------------------------------------------------
    // The one that proves it went all the way
    // -----------------------------------------------------------

    /// Convention 5: a rule added to a list in memory satisfies any
    /// assertion about what the API returned. The only thing that
    /// cannot be satisfied that way is a message published afterwards
    /// putting an envelope in that rule's outbox, because reaching the
    /// outbox means going through the broker's publish path and the
    /// hook installed on it.
    #[tokio::test]
    async fn a_rule_created_over_the_api_fires() {
        let node = Node::start(None).await;
        let created = node.post("/api/v1/rules", &posting_rule("telemetry")).await;
        assert_eq!(created.status, 200, "{}", created.body);

        let published = node
            .post("/api/v1/publish", &publication("sensor/1/temp"))
            .await;
        assert_eq!(published.status, 200, "{}", published.body);

        assert!(
            node.until_outbox("telemetry", 1).await,
            "the rule created over the API did not fire: nothing reached its outbox"
        );
        node.stop().await;
    }

    /// The other half of the same question: a rule fires on what its
    /// filter says and on nothing else. A rule that fired on
    /// everything would pass the test above.
    #[tokio::test]
    async fn a_rule_created_over_the_api_does_not_fire_on_everything() {
        let node = Node::start(None).await;
        node.post("/api/v1/rules", &posting_rule("telemetry")).await;

        let published = node
            .post("/api/v1/publish", &publication("something/else"))
            .await;
        assert_eq!(published.status, 200, "{}", published.body);
        // The one the rule does want, published second, so that the
        // assertion is about which of the two arrived rather than
        // about whether the writer has caught up.
        node.post("/api/v1/publish", &publication("sensor/1/temp"))
            .await;

        assert!(node.until_outbox("telemetry", 1).await);
        assert_eq!(
            node.store.outbox_depth("telemetry").expect("the store"),
            1,
            "the rule fired on a topic its filter does not match"
        );
        node.stop().await;
    }

    /// `enabled` is what an operator reaches for to turn a connector
    /// off, and it has to reach all the way to the hook.
    #[tokio::test]
    async fn a_rule_disabled_over_the_api_stops_firing() {
        let node = Node::start(None).await;
        node.post("/api/v1/rules", &posting_rule("telemetry")).await;
        node.post("/api/v1/publish", &publication("sensor/1/temp"))
            .await;
        assert!(node.until_outbox("telemetry", 1).await);

        let off = node.post("/api/v1/rules/telemetry/disable", "").await;
        assert_eq!(off.status, 200, "{}", off.body);
        assert!(
            off.body.contains("\"enabled\":false"),
            "a disabled rule should say so: {}",
            off.body
        );

        node.post("/api/v1/publish", &publication("sensor/2/temp"))
            .await;
        // Long enough that a rule which was still firing would have
        // reached the outbox: the writer's interval is two
        // milliseconds.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            node.store.outbox_depth("telemetry").expect("the store"),
            1,
            "a disabled rule fired"
        );

        let on = node.post("/api/v1/rules/telemetry/enable", "").await;
        assert_eq!(on.status, 200, "{}", on.body);
        node.post("/api/v1/publish", &publication("sensor/3/temp"))
            .await;
        assert!(
            node.until_outbox("telemetry", 2).await,
            "a rule switched back on did not fire"
        );
        node.stop().await;
    }

    // -----------------------------------------------------------
    // The refusals, each one actually tried
    // -----------------------------------------------------------

    /// Section 9.2: the token is not optional, because the endpoint
    /// publishes.
    #[tokio::test]
    async fn an_ingest_rule_with_no_token_is_refused() {
        let node = Node::start(None).await;
        let answered = node
            .post(
                "/api/v1/rules",
                r#"{"id":"gateway","name":"Gateway in",
                    "http_in":{"topic":"ingest/across","qos":1}}"#,
            )
            .await;
        assert_eq!(answered.status, 400, "{}", answered.body);
        assert_eq!(
            error_of(&answered.body),
            ("invalid_request".to_owned(), "http_in.token".to_owned()),
            "{}",
            answered.body
        );
        assert!(
            node.registry.is_empty(),
            "a refused rule was installed anyway"
        );
        node.stop().await;
    }

    /// Section 9.2's first line of defence against a bridge loop, and
    /// the one that works on MQTT 3.1.1.
    #[tokio::test]
    async fn a_bridge_whose_filters_intersect_is_refused() {
        let node = Node::start(None).await;
        let answered = node
            .post(
                "/api/v1/rules",
                r#"{"id":"across","name":"A bridge that loops",
                    "mqtt_bridge":{"address":"mqtts://remote.example:8883",
                                   "client_id":"bridge-one",
                                   "topics":[{"filter":"site/+/data","direction":"out"},
                                             {"filter":"site/+/data","direction":"in"}]}}"#,
            )
            .await;
        assert_eq!(answered.status, 400, "{}", answered.body);
        assert_eq!(
            error_of(&answered.body),
            (
                "invalid_request".to_owned(),
                "mqtt_bridge.topics".to_owned()
            ),
            "{}",
            answered.body
        );
        assert!(
            answered.body.contains("site/"),
            "the refusal should name a topic both sides match: {}",
            answered.body
        );
        assert!(node.registry.is_empty());
        node.stop().await;
    }

    /// The loop no single rule is wrong in: two bridges to one remote.
    ///
    /// The check above takes one bridge and is called once per rule,
    /// so neither of these is refused on its own — asserted, because
    /// otherwise this could pass for that reason. Rule A carries
    /// `site/#` out to the remote under `up/`; rule B reads `up/site/#`
    /// back off the same remote and publishes it here as `site/#`,
    /// which is what A carries out. On MQTT 5 the marker closes that
    /// one turn later; on 3.1.1 nothing closes it at all.
    #[tokio::test]
    async fn a_second_bridge_that_loops_through_the_first_is_refused() {
        let node = Node::start(None).await;
        let out = node
            .post(
                "/api/v1/rules",
                r#"{"id":"upward","name":"Up to the hub",
                    "mqtt_bridge":{"address":"mqtts://remote.example:8883",
                                   "client_id":"bridge-out",
                                   "topics":[{"filter":"site/#","direction":"out",
                                              "remote_prefix":"up/"}]}}"#,
            )
            .await;
        assert_eq!(out.status, 200, "{}", out.body);

        let back = node
            .post(
                "/api/v1/rules",
                r#"{"id":"downward","name":"Back from the hub",
                    "mqtt_bridge":{"address":"mqtts://remote.example:8883",
                                   "client_id":"bridge-in",
                                   "topics":[{"filter":"site/#","direction":"in",
                                              "remote_prefix":"up/"}]}}"#,
            )
            .await;
        assert_eq!(back.status, 508, "{}", back.body);
        assert_eq!(
            error_of(&back.body),
            ("loop_detected".to_owned(), "mqtt_bridge.topics".to_owned()),
            "{}",
            back.body
        );
        assert!(
            back.body.contains("upward") && back.body.contains("downward"),
            "the refusal should name both rules: {}",
            back.body
        );
        assert_eq!(
            node.registry.len(),
            1,
            "the refused bridge was installed anyway"
        );

        // The same second rule, delivering where the first does not
        // carry from, is a mirror rather than a cycle and is accepted.
        // Without this the assertion above would be satisfied by
        // refusing every second bridge to one remote.
        let mirrored = node
            .post(
                "/api/v1/rules",
                r#"{"id":"mirrored","name":"A mirror",
                    "mqtt_bridge":{"address":"mqtts://remote.example:8883",
                                   "client_id":"bridge-mirror",
                                   "topics":[{"filter":"site/#","direction":"in",
                                              "remote_prefix":"up/",
                                              "local_prefix":"mirror/"}]}}"#,
            )
            .await;
        assert_eq!(mirrored.status, 200, "{}", mirrored.body);
        node.stop().await;
    }

    /// An operator who put a filter on a rule a message does not fire
    /// believes something about when it runs that is not true.
    #[tokio::test]
    async fn a_trigger_on_an_action_that_does_not_fire_on_messages_is_refused() {
        let node = Node::start(None).await;
        let answered = node
            .post(
                "/api/v1/rules",
                r#"{"id":"gateway","name":"Gateway in","filter":"sensor/+/temp",
                    "http_in":{"token":"a-secret","topic":"ingest/across","qos":1}}"#,
            )
            .await;
        assert_eq!(answered.status, 400, "{}", answered.body);
        assert_eq!(
            error_of(&answered.body),
            ("invalid_request".to_owned(), "filter".to_owned()),
            "{}",
            answered.body
        );
        assert!(node.registry.is_empty());
        node.stop().await;
    }

    /// The same rule without the filter is accepted, which is what
    /// makes the refusal above about the filter rather than about
    /// ingest rules.
    #[tokio::test]
    async fn the_same_ingest_rule_without_a_filter_is_accepted_and_served() {
        let node = Node::start(None).await;
        let answered = node
            .post(
                "/api/v1/rules",
                r#"{"id":"gateway","name":"Gateway in",
                    "http_in":{"token":"a-secret","topic":"ingest/across","qos":1}}"#,
            )
            .await;
        assert_eq!(answered.status, 200, "{}", answered.body);
        assert_eq!(
            node.registry.ingest().len(),
            1,
            "an ingest rule created over the API got no route"
        );
        node.stop().await;
    }

    // -----------------------------------------------------------
    // Reading, writing and the shapes in between
    // -----------------------------------------------------------

    /// Convention 1, from the other side: what a listing gives back is
    /// what a create takes.
    #[tokio::test]
    async fn a_rule_read_back_is_one_that_can_be_written_back() {
        let node = Node::start(None).await;
        node.post("/api/v1/rules", &posting_rule("telemetry")).await;

        let read = node.get("/api/v1/rules/telemetry").await;
        assert_eq!(read.status, 200, "{}", read.body);

        let written = node.put("/api/v1/rules/telemetry", &read.body).await;
        assert_eq!(
            written.status, 200,
            "a rule read back was refused on the way in: {}",
            written.body
        );
        assert_eq!(
            node.get("/api/v1/rules/telemetry").await.body,
            read.body,
            "writing a rule back changed it"
        );
        node.stop().await;
    }

    /// Section 12.3: a secret goes in and does not come back out.
    #[tokio::test]
    async fn a_rule_read_back_carries_no_secret() {
        let node = Node::start(None).await;
        node.post(
            "/api/v1/rules",
            r#"{"id":"gateway","name":"Gateway in",
                "http_in":{"token":"a-secret-nobody-may-read","topic":"ingest/across"}}"#,
        )
        .await;

        let read = node.get("/api/v1/rules/gateway").await;
        assert!(
            !read.body.contains("a-secret-nobody-may-read"),
            "the API gave out a bearer token: {}",
            read.body
        );
        let listed = node.get("/api/v1/rules").await;
        assert!(
            !listed.body.contains("a-secret-nobody-may-read"),
            "the listing gave out a bearer token: {}",
            listed.body
        );
        node.stop().await;
    }

    /// A rule's headers are where its bearer token goes, and the read
    /// resources are not behind the admin token, so the values are
    /// redacted and the names are not.
    #[tokio::test]
    async fn a_rules_header_values_do_not_come_back_but_their_names_do() {
        let node = Node::start(None).await;
        let created = node
            .post(
                "/api/v1/rules",
                r#"{"id":"telemetry","name":"Telemetry out","filter":"sensor/+/temp",
                    "http_out":{"url":"http://127.0.0.1:1/",
                                "headers":{"x-token":"the-bearer-token"}}}"#,
            )
            .await;
        assert_eq!(created.status, 200, "{}", created.body);

        let read = node.get("/api/v1/rules/telemetry").await;
        assert!(
            !read.body.contains("the-bearer-token"),
            "the API gave out a header value: {}",
            read.body
        );
        assert!(
            read.body.contains("x-token"),
            "the API hid the header name as well as the value: {}",
            read.body
        );

        // And writing it back keeps the value, which is what stops a
        // read, change and write from quietly taking the guard off
        // the endpoint this rule posts to.
        let written = node.put("/api/v1/rules/telemetry", &read.body).await;
        assert_eq!(written.status, 200, "{}", written.body);
        let held = node
            .registry
            .get(&RuleId::new("telemetry").expect("an identifier"))
            .expect("the rule is still there");
        match &held.action {
            Action::HttpOut(action) => assert_eq!(
                action.headers,
                vec![("x-token".to_owned(), "the-bearer-token".to_owned())],
                "writing a rule back emptied the header it sends"
            ),
            other => panic!("the action changed: {other:?}"),
        }
        node.stop().await;
    }

    /// The consequence of the rule above: an update that leaves the
    /// secret out keeps the stored one, because the caller was never
    /// given it to send back.
    #[tokio::test]
    async fn an_update_that_leaves_a_secret_out_keeps_it() {
        let node = Node::start(None).await;
        node.post(
            "/api/v1/rules",
            r#"{"id":"gateway","name":"Gateway in",
                "http_in":{"token":"a-secret-nobody-may-read","topic":"ingest/across"}}"#,
        )
        .await;
        let renamed = node
            .put(
                "/api/v1/rules/gateway",
                r#"{"id":"gateway","name":"Gateway in, renamed",
                    "http_in":{"topic":"ingest/across"}}"#,
            )
            .await;
        assert_eq!(
            renamed.status, 200,
            "an update with no token was refused, so the secret was lost: {}",
            renamed.body
        );
        let held = node
            .registry
            .get(&RuleId::new("gateway").expect("an identifier"))
            .expect("the rule is still there");
        match &held.action {
            Action::HttpIn(action) => assert_eq!(
                action.token.expose(),
                "a-secret-nobody-may-read",
                "the update replaced the token with nothing"
            ),
            other => panic!("the action changed: {other:?}"),
        }
        node.stop().await;
    }

    /// The URL is a secret too, and the read resources need no token.
    ///
    /// For the commonest outbound target the URL *is* the credential:
    /// a chat platform's webhook is a long unguessable path and
    /// nothing else. What comes back is the scheme and the host, which
    /// is what a form needs to show an operator where their rule
    /// posts, and the rest is elided.
    ///
    /// The second half is what stops the first being a redaction
    /// nobody could live with: a caller that reads a rule, renames it
    /// and writes it back keeps the URL it was never given. Without
    /// that, the obvious edit silently repoints the rule at a path
    /// that is three dots.
    #[tokio::test]
    async fn a_rules_url_does_not_come_back_but_its_host_does() {
        let node = Node::start(None).await;
        node.post(
            "/api/v1/rules",
            r#"{"id":"telemetry","name":"Telemetry out","filter":"sensor/+/temp",
                "http_out":{"url":"https://hooks.example.invalid/services/T0/an-unguessable-webhook"}}"#,
        )
        .await;

        for path in ["/api/v1/rules/telemetry", "/api/v1/rules"] {
            let read = node.get(path).await;
            assert!(
                !read.body.contains("an-unguessable-webhook"),
                "{path} gave out the path of a webhook URL: {}",
                read.body
            );
            assert!(
                read.body.contains("hooks.example.invalid"),
                "{path} did not say where the rule posts at all: {}",
                read.body
            );
        }

        // Read, renamed, written back: the URL the caller was never
        // given is the URL the rule keeps.
        let read = node.get("/api/v1/rules/telemetry").await;
        let written = read.body.replace("Telemetry out", "Telemetry, renamed");
        let updated = node.put("/api/v1/rules/telemetry", &written).await;
        assert_eq!(
            updated.status, 200,
            "writing back what was read was refused: {}",
            updated.body
        );
        let held = node
            .registry
            .get(&RuleId::new("telemetry").expect("an identifier"))
            .expect("the rule is still there");
        assert_eq!(held.name, "Telemetry, renamed");
        match &held.action {
            Action::HttpOut(action) => assert_eq!(
                action.url, "https://hooks.example.invalid/services/T0/an-unguessable-webhook",
                "an update that sent back the redacted URL repointed the rule"
            ),
            other => panic!("the action changed: {other:?}"),
        }
        node.stop().await;
    }

    /// A create that overwrote something would make two operators
    /// writing the same identifier a race whose loser never finds out.
    #[tokio::test]
    async fn creating_a_rule_at_a_name_something_holds_is_refused() {
        let node = Node::start(None).await;
        node.post("/api/v1/rules", &posting_rule("telemetry")).await;
        let again = node.post("/api/v1/rules", &posting_rule("telemetry")).await;
        assert_eq!(again.status, 409, "{}", again.body);
        assert_eq!(
            error_of(&again.body),
            ("conflict".to_owned(), "id".to_owned()),
            "{}",
            again.body
        );
        node.stop().await;
    }

    /// A rule's identifier names its outbox and its dead-letter topic,
    /// so an update cannot change it.
    #[tokio::test]
    async fn an_update_that_renames_the_rule_is_refused() {
        let node = Node::start(None).await;
        node.post("/api/v1/rules", &posting_rule("telemetry")).await;
        let answered = node
            .put("/api/v1/rules/telemetry", &posting_rule("something-else"))
            .await;
        assert_eq!(answered.status, 400, "{}", answered.body);
        assert_eq!(
            error_of(&answered.body),
            ("invalid_request".to_owned(), "id".to_owned()),
            "{}",
            answered.body
        );
        node.stop().await;
    }

    /// Deleting a rule deletes its outbox, and what is in one is
    /// messages this broker told somebody it had accepted.
    #[tokio::test]
    async fn a_rule_with_something_waiting_is_not_deleted() {
        let node = Node::start(None).await;
        node.post("/api/v1/rules", &posting_rule("telemetry")).await;
        node.post("/api/v1/publish", &publication("sensor/1/temp"))
            .await;
        assert!(node.until_outbox("telemetry", 1).await);

        let refused = node.delete("/api/v1/rules/telemetry").await;
        assert_eq!(refused.status, 409, "{}", refused.body);
        assert_eq!(
            error_of(&refused.body),
            ("conflict".to_owned(), "id".to_owned()),
            "{}",
            refused.body
        );
        assert!(
            node.registry
                .get(&RuleId::new("telemetry").expect("an identifier"))
                .is_some(),
            "the refused delete took the rule anyway"
        );
        node.stop().await;
    }

    /// And one with nothing waiting is deleted, which is what makes
    /// the refusal above about the outbox.
    #[tokio::test]
    async fn a_rule_with_an_empty_outbox_is_deleted_and_stops_firing() {
        let node = Node::start(None).await;
        node.post("/api/v1/rules", &posting_rule("telemetry")).await;

        let gone = node.delete("/api/v1/rules/telemetry").await;
        assert_eq!(gone.status, 200, "{}", gone.body);
        assert!(node.registry.is_empty());
        assert_eq!(node.get("/api/v1/rules/telemetry").await.status, 404);

        node.post("/api/v1/publish", &publication("sensor/1/temp"))
            .await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            node.store.outbox_depth("telemetry").expect("the store"),
            0,
            "a deleted rule fired"
        );
        node.stop().await;
    }

    /// Section 11.1: only mutating requests need the token.
    ///
    /// Convention 5: each on its own passes against a guard that is
    /// not there, so every mutating route is tried and the reads are
    /// tried beside them.
    #[tokio::test]
    async fn every_mutating_rule_request_needs_the_token() {
        let node = Node::start(Some("a-secret-for-the-test")).await;
        let address = node.admin.address();
        let token = Some("a-secret-for-the-test");

        // With it, so that the rule the rest of the test needs exists.
        let created =
            testing::post_json(address, "/api/v1/rules", token, &posting_rule("telemetry")).await;
        assert_eq!(created.status, 200, "{}", created.body);

        for (method, path, body) in [
            ("POST", "/api/v1/rules", Some(posting_rule("another"))),
            (
                "PUT",
                "/api/v1/rules/telemetry",
                Some(posting_rule("telemetry")),
            ),
            ("POST", "/api/v1/rules/telemetry/enable", None),
            ("POST", "/api/v1/rules/telemetry/disable", None),
            ("DELETE", "/api/v1/rules/telemetry", None),
        ] {
            let without = testing::with_body(address, method, path, None, body.as_deref()).await;
            assert_eq!(
                without.status, 401,
                "{method} {path} was served without the token: {}",
                without.body
            );
            let wrong =
                testing::with_body(address, method, path, Some("not-it"), body.as_deref()).await;
            assert_eq!(
                wrong.status, 401,
                "{method} {path} was served with the wrong token: {}",
                wrong.body
            );
        }

        // And the reads are not guarded, which is what makes the
        // assertions above about mutation rather than about the
        // listener.
        assert_eq!(testing::get(address, "/api/v1/rules").await.status, 200);
        assert_eq!(
            testing::get(address, "/api/v1/rules/telemetry")
                .await
                .status,
            200
        );
        node.stop().await;
    }

    /// Section 9.1 makes a rule durable metadata, so a node that
    /// writes nothing down holds none — and says so rather than
    /// answering with an empty collection.
    #[tokio::test]
    async fn a_node_with_no_store_says_it_holds_no_rules() {
        let admin = Node::without_a_store().await;
        let answered = testing::get(admin.address(), "/api/v1/rules").await;
        assert_eq!(answered.status, 404, "{}", answered.body);
        assert_eq!(error_of(&answered.body).0, "not_found", "{}", answered.body);
        admin.shutdown().await;
    }

    // -----------------------------------------------------------
    // What each rule has been doing
    // -----------------------------------------------------------

    /// The two numbers a publication moves, and the three it does not.
    ///
    /// Convention 5: a counter that rose on everything would satisfy
    /// an assertion that the one being tested rose, so all five are
    /// read after each step.
    #[tokio::test]
    async fn a_publication_moves_fired_and_waiting_and_nothing_else() {
        let node = Node::start(None).await;
        node.post("/api/v1/rules", &posting_rule("telemetry")).await;

        let before = node.stats("telemetry").await;
        assert_eq!(before["totals"]["fired"], 0);
        assert_eq!(before["totals"]["waiting"], 0);

        node.post("/api/v1/publish", &publication("sensor/1/temp"))
            .await;
        assert!(node.until_outbox("telemetry", 1).await);

        let after = node.stats("telemetry").await;
        assert_eq!(after["totals"]["fired"], 1, "{after}");
        assert_eq!(
            after["totals"]["waiting"], 1,
            "the outbox depth is not reported: {after}"
        );
        // The endpoint is dead and the backoff is an hour, so nothing
        // has been delivered and nothing has been abandoned. What has
        // happened is at most one failed attempt, which is the pump's
        // and not this publication's.
        assert_eq!(after["totals"]["delivered"], 0, "{after}");
        assert_eq!(after["totals"]["dead_lettered"], 0, "{after}");

        // A message the rule does not match moves nothing at all.
        let quiet = node.stats("telemetry").await;
        node.post("/api/v1/publish", &publication("something/else"))
            .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let still = node.stats("telemetry").await;
        assert_eq!(
            still["totals"]["fired"], quiet["totals"]["fired"],
            "a message the rule does not match moved its counters"
        );
        node.stop().await;
    }

    /// The mutation the plan names: a series that only advances when
    /// something happens makes a quiet rule look like a busy one whose
    /// chart stopped.
    ///
    /// Asserted through the resource rather than on the ring, because
    /// the ring's own test cannot see whether anything ever calls it.
    #[tokio::test]
    async fn the_series_advances_while_a_rule_is_quiet() {
        let node = Node::start(None).await;
        node.post("/api/v1/rules", &posting_rule("telemetry")).await;

        let deadline = Instant::now() + PATIENCE;
        let mut points = 0;
        while Instant::now() < deadline {
            let stats = node.stats("telemetry").await;
            points = stats["series"]["fired"].as_array().expect("a series").len();
            if points >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            points >= 2,
            "the series stood still while the rule was quiet, so a stopped rule and an \
             idle one draw the same line"
        );
        let stats = node.stats("telemetry").await;
        assert_eq!(stats["interval_seconds"], 1, "{stats}");
        for name in ["fired", "delivered", "failed", "dead_lettered", "waiting"] {
            assert_eq!(
                stats["series"][name].as_array().expect("a series").len(),
                points,
                "the five arrays of the series are different lengths: {stats}"
            );
        }
        node.stop().await;
    }

    /// Section 11.3, for a rule: the same four numbers reach a
    /// scraper, labelled by rule and by outcome.
    ///
    /// A chart is what an operator looks at and an alert is what tells
    /// them to; a rule whose numbers only existed in a browser would
    /// have the first and not the second.
    #[tokio::test]
    async fn what_a_rule_has_done_reaches_the_metrics_endpoint() {
        let node = Node::start(None).await;
        node.post("/api/v1/rules", &posting_rule("telemetry")).await;
        node.post("/api/v1/publish", &publication("sensor/1/temp"))
            .await;
        assert!(node.until_outbox("telemetry", 1).await);
        // The depth is set by the sampler, so it is there a second
        // after the envelope is.
        tokio::time::sleep(Duration::from_millis(1_200)).await;

        let exposition = node.get("/metrics").await;
        assert_eq!(exposition.status, 200, "{}", exposition.body);
        for wanted in [
            "sdmqtt_rule_events_total{rule=\"telemetry\",event=\"fired\"} 1",
            "sdmqtt_rule_outbox_waiting{rule=\"telemetry\"} 1",
        ] {
            assert!(
                exposition.body.contains(wanted),
                "the exposition has no {wanted}:\n{}",
                exposition.body
            );
        }
        // And the outcomes that did not happen are not claimed to
        // have. A family that reported every rule as delivering would
        // pass the two assertions above.
        assert!(
            exposition
                .body
                .contains("sdmqtt_rule_events_total{rule=\"telemetry\",event=\"delivered\"} 0"),
            "the exposition claims a delivery that did not happen:\n{}",
            exposition.body
        );
        node.stop().await;
    }

    /// The statistics of every rule, as a page.
    #[tokio::test]
    async fn every_rule_reports_what_it_has_been_doing() {
        let node = Node::start(None).await;
        for id in ["alpha", "bravo"] {
            node.post("/api/v1/rules", &posting_rule(id)).await;
        }
        let listed = node.get("/api/v1/rule-stats").await;
        assert_eq!(listed.status, 200, "{}", listed.body);
        let page: serde_json::Value = serde_json::from_str(&listed.body).expect("a page");
        assert_eq!(page["total"], 2);
        assert_eq!(page["items"][0]["id"], "alpha");
        assert_eq!(page["items"][1]["id"], "bravo");
        assert_eq!(page["items"][0]["totals"]["fired"], 0);
        assert_eq!(
            node.get("/api/v1/rule-stats/").await.status,
            404,
            "a resource that does not exist was served"
        );
        node.stop().await;
    }

    /// A rule's numbers survive it being disabled and enabled again.
    ///
    /// An operator watching a chart has not asked for it to start over
    /// because they turned a connector off for a minute.
    #[tokio::test]
    async fn a_rules_numbers_survive_being_switched_off_and_on() {
        let node = Node::start(None).await;
        node.post("/api/v1/rules", &posting_rule("telemetry")).await;
        node.post("/api/v1/publish", &publication("sensor/1/temp"))
            .await;
        assert!(node.until_outbox("telemetry", 1).await);
        assert_eq!(node.stats("telemetry").await["totals"]["fired"], 1);

        node.post("/api/v1/rules/telemetry/disable", "").await;
        node.post("/api/v1/rules/telemetry/enable", "").await;
        assert_eq!(
            node.stats("telemetry").await["totals"]["fired"],
            1,
            "switching a rule off and on again started its numbers over"
        );
        node.stop().await;
    }

    // -----------------------------------------------------------
    // Dead letters
    // -----------------------------------------------------------

    /// A rule that gives up on an envelope keeps the letter where
    /// somebody who was not watching can find it.
    ///
    /// The whole argument for this being a resource is that the page
    /// is opened **after** the letter was published, so the test
    /// opens it after: nothing is subscribed to anything while the
    /// rule is failing.
    #[tokio::test]
    async fn a_rule_that_gives_up_keeps_the_letter_for_a_page_nobody_had_open() {
        let node = Node::start(None).await;
        // No retries and an endpoint that is not there, so the first
        // attempt is the last and the envelope is abandoned at once.
        let created = node
            .post(
                "/api/v1/rules",
                r#"{"id":"telemetry","name":"Telemetry out","filter":"sensor/+/temp",
                    "http_out":{"url":"http://127.0.0.1:1/","timeout_ms":100,
                                "retries":0,"backoff_ms":10}}"#,
            )
            .await;
        assert_eq!(created.status, 200, "{}", created.body);

        assert_eq!(
            node.dead_letters("telemetry").await["total"],
            0,
            "a rule that has dead-lettered nothing reported a letter"
        );

        node.post("/api/v1/publish", &publication("sensor/1/temp"))
            .await;

        let deadline = Instant::now() + PATIENCE;
        let mut kept = node.dead_letters("telemetry").await;
        while kept["total"] == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
            kept = node.dead_letters("telemetry").await;
        }
        assert_eq!(kept["total"], 1, "the letter never reached the resource");

        let letter = &kept["items"][0];
        assert_eq!(letter["sequence"], 1);
        assert_eq!(letter["topic"], "$sd/rules/telemetry/dlq");
        assert_eq!(letter["truncated"], false);
        let document = letter["document"].as_str().expect("a document");
        assert!(
            document.contains("\"attempts\":1"),
            "the kept document is not the letter that was published: {document}"
        );
        assert!(
            document.contains("sensor/1/temp"),
            "the kept document does not carry the message that was given up on: {document}"
        );

        // And it is the same event the counter counted, rather than a
        // second one this ring invented.
        assert_eq!(
            node.stats("telemetry").await["totals"]["dead_lettered"],
            1,
            "the ring and the counter disagree about how many letters there were"
        );
        node.stop().await;
    }

    /// A rule's letters survive it being switched off and on again.
    ///
    /// Switching a connector off is what an operator does *because* of
    /// a dead letter, so a switch that discarded the evidence would
    /// take it away at the moment it was being read.
    #[tokio::test]
    async fn a_rules_letters_survive_being_switched_off_and_on() {
        let node = Node::start(None).await;
        node.post(
            "/api/v1/rules",
            r#"{"id":"telemetry","name":"Telemetry out","filter":"sensor/+/temp",
                "http_out":{"url":"http://127.0.0.1:1/","timeout_ms":100,
                            "retries":0,"backoff_ms":10}}"#,
        )
        .await;
        node.post("/api/v1/publish", &publication("sensor/1/temp"))
            .await;

        let deadline = Instant::now() + PATIENCE;
        while node.dead_letters("telemetry").await["total"] == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(node.dead_letters("telemetry").await["total"], 1);

        node.post("/api/v1/rules/telemetry/disable", "").await;
        node.post("/api/v1/rules/telemetry/enable", "").await;
        assert_eq!(
            node.dead_letters("telemetry").await["total"],
            1,
            "switching the rule off and on again discarded the letter that made \
             somebody switch it off"
        );
        node.stop().await;
    }

    /// A rule that does not exist is not a rule with no letters.
    #[tokio::test]
    async fn the_dead_letters_of_a_rule_that_does_not_exist_are_not_an_empty_list() {
        let node = Node::start(None).await;
        let answered = node.get("/api/v1/rules/nothing/dead-letters").await;
        assert_eq!(answered.status, 404, "{}", answered.body);
        assert_eq!(error_of(&answered.body).0, "not_found", "{}", answered.body);
        node.stop().await;
    }

    // -----------------------------------------------------------
    // What any of this costs the broker
    // -----------------------------------------------------------

    /// What a publication costs with a great many rules installed,
    /// and what a page of their statistics costs beside it.
    ///
    /// **A measurement, not an assertion**, which is why it is
    /// ignored: a wall clock on a shared machine is not a thing to
    /// gate a build on. It is here for the same reason
    /// `what_a_page_costs_under_the_lock` is in [`crate::resources`],
    /// and it answers the same question about a different lock.
    ///
    /// The first number is the one that matters. A publication goes
    /// through the broker's one lock, and the hook this milestone
    /// installs reads the registry's map while it is held, so the
    /// work under that lock is proportional to the rules on the node.
    /// The second number is not under the broker lock at all — the
    /// statistics resource never takes it — and is here to say what
    /// the series costs to copy out.
    ///
    ///     cargo test -p sdmqtt-admin --release -- --ignored --nocapture what_rules_cost
    #[tokio::test]
    #[ignore = "a measurement, not an assertion"]
    async fn what_rules_cost_the_broker() {
        for rules in [0_usize, 10, 200, 1_000] {
            let node = Node::start(None).await;
            for n in 0..rules {
                let written = format!(
                    r#"{{"id":"rule-{n}","name":"Rule {n}","filter":"bench/{n}/+",
                         "http_out":{{"url":"http://127.0.0.1:1/","timeout_ms":50,
                                      "retries":250,"backoff_ms":3600000}}}}"#
                );
                let created = node.post("/api/v1/rules", &written).await;
                assert_eq!(created.status, 200, "{}", created.body);
            }

            // **The publish itself and not a socket round trip.**
            // The handler does nothing but read a body and call this,
            // and a connection per request is tens of microseconds of
            // noise that would bury the number being measured.
            let rounds = 2_000;
            let mut missed = Duration::ZERO;
            let mut matched = Duration::ZERO;
            for (topic, into) in [("bench/none/x", &mut missed), ("bench/7/x", &mut matched)] {
                let started = Instant::now();
                for _ in 0..rounds {
                    sdmqtt_server::publish::publish(
                        &node.shared,
                        Publication {
                            topic: topic.to_owned(),
                            payload: bytes::Bytes::from_static(b"25.5"),
                            qos: mqtt_codec::QoS::AtMostOnce,
                            retain: false,
                            message_expiry_interval: None,
                            user_properties: Vec::new(),
                            origin: "measurement".to_owned(),
                        },
                    )
                    .await
                    .expect("the publication is accepted");
                }
                *into = started.elapsed() / rounds;
            }

            let started = Instant::now();
            let page = node.get("/api/v1/rule-stats?limit=100").await;
            let stats = started.elapsed();
            assert_eq!(page.status, 200);

            println!(
                "{rules:>5} rules: {missed:>9.2?} per publication matching none, \
                 {matched:>9.2?} matching one, \
                 {stats:>9.2?} for a page of 100 statistics over a socket"
            );
            node.stop().await;
        }
    }

    /// The listing is a page with the same envelope as every other
    /// resource, in identifier order.
    #[tokio::test]
    async fn the_listing_is_a_page_in_identifier_order() {
        let node = Node::start(None).await;
        for id in ["charlie", "alpha", "bravo"] {
            let created = node.post("/api/v1/rules", &posting_rule(id)).await;
            assert_eq!(created.status, 200, "{}", created.body);
        }
        let listed = node.get("/api/v1/rules?limit=2").await;
        let page: serde_json::Value = serde_json::from_str(&listed.body).expect("a page");
        assert_eq!(page["total"], 3);
        assert_eq!(page["items"][0]["id"], "alpha");
        assert_eq!(page["items"][1]["id"], "bravo");
        let next = page["next"].as_str().expect("a cursor").to_owned();
        let rest = node
            .get(&format!("/api/v1/rules?after={}", escape(&next)))
            .await;
        let page: serde_json::Value = serde_json::from_str(&rest.body).expect("a page");
        assert_eq!(page["items"][0]["id"], "charlie");
        assert!(page["next"].is_null());
        node.stop().await;
    }
}
