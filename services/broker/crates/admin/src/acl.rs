//! Section 11.1's access control resource, over the directory.
//!
//! A rule is written whole and read back whole; the directory's
//! `check_rule` is the one judge of what a rule is, so the API and the
//! configuration file refuse the same things with the same words. The
//! listing is in consultation order, which is what an operator reading
//! it to find out why a client was refused needs.

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use directory::{AclRecord, Directory, DirectoryError};
use serde::{Deserialize, Serialize};

use crate::error::{ApiError, ErrorCode};
use crate::resources::{page, select, Authorized, Page, Paging};
use crate::AdminState;

/// A rule as the API spells one, in and out.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AclSpec {
    /// What an operator calls it.
    pub id: String,
    /// Lower is consulted first. 100 by default.
    #[serde(default = "default_order")]
    pub order: u32,
    /// `anyone`, `user:<name>` or `client:<id>`.
    pub subject: String,
    /// The topic filter, with MQTT wildcards.
    pub filter: String,
    /// Whether publishing is allowed, when the rule has an opinion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish: Option<bool>,
    /// Whether subscribing is allowed, when the rule has an opinion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribe: Option<bool>,
}

fn default_order() -> u32 {
    100
}

impl AclSpec {
    fn of(rule: &AclRecord) -> Self {
        Self {
            id: rule.id.clone(),
            order: rule.order,
            subject: rule.subject.clone(),
            filter: rule.filter.clone(),
            publish: rule.publish,
            subscribe: rule.subscribe,
        }
    }

    fn to_record(&self) -> AclRecord {
        AclRecord {
            id: self.id.clone(),
            order: self.order,
            subject: self.subject.clone(),
            filter: self.filter.clone(),
            publish: self.publish,
            subscribe: self.subscribe,
        }
    }
}

fn directory(state: &AdminState) -> Result<&std::sync::Arc<Directory>, ApiError> {
    state.directory.as_ref().ok_or_else(|| {
        ApiError::not_found(
            "this node has no store, so it holds no access control rules through the API: \
             section 12.1 makes a rule durable metadata, and a broker started without \
             storage.directory writes nothing down. Rules can still be written in the \
             configuration file as [[acl]] tables",
        )
    })
}

fn refusal(error: DirectoryError) -> ApiError {
    match error {
        DirectoryError::BadRule { field, why } => {
            ApiError::in_field(ErrorCode::InvalidRequest, field, why)
        }
        DirectoryError::NoSuchRule { .. } => ApiError::not_found(error.to_string()),
        DirectoryError::Storage { .. } => ApiError::new(ErrorCode::Internal, error.to_string()),
        other => ApiError::new(ErrorCode::Internal, other.to_string()),
    }
}

async fn list(
    State(state): State<AdminState>,
    paging: Paging,
) -> Result<Json<Page<AclSpec>>, ApiError> {
    let held = directory(&state)?.acl();
    // In consultation order, which is not identifier order: the
    // pagination key carries the order first so a page boundary falls
    // where the listing does.
    let keyed: Vec<(String, &AclRecord)> = held
        .iter()
        .map(|rule| (format!("{:010}/{}", rule.order, rule.id), rule))
        .collect();
    let keys: Vec<_> = keyed
        .iter()
        .map(|(key, rule)| ([key.as_str(), "", ""], *rule))
        .collect();
    let selection = select(keys.into_iter(), paging.after.as_ref(), paging.limit);
    Ok(Json(page(selection, |_, rule| AclSpec::of(rule))))
}

async fn read(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<AclSpec>, ApiError> {
    directory(&state)?
        .acl()
        .iter()
        .find(|rule| rule.id == id)
        .map(|rule| Json(AclSpec::of(rule)))
        .ok_or_else(|| ApiError::not_found(format!("there is no access control rule named {id}")))
}

async fn create(
    State(state): State<AdminState>,
    _authorized: Authorized,
    body: Result<Json<AclSpec>, JsonRejection>,
) -> Result<Json<AclSpec>, ApiError> {
    let Json(spec) = body.map_err(|rejection| {
        ApiError::invalid_request(format!(
            "the request body is not an access control rule: {}",
            rejection.body_text()
        ))
    })?;
    let rule = directory(&state)?
        .put_acl(spec.to_record())
        .await
        .map_err(refusal)?;
    Ok(Json(AclSpec::of(&rule)))
}

async fn delete(
    State(state): State<AdminState>,
    _authorized: Authorized,
    Path(id): Path<String>,
) -> Result<Json<Deleted>, ApiError> {
    directory(&state)?.delete_acl(&id).await.map_err(refusal)?;
    Ok(Json(Deleted { id }))
}

/// What a delete that happened answers.
#[derive(Debug, Serialize)]
struct Deleted {
    /// What is gone.
    id: String,
}

/// The routes this module owns.
pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/api/v1/acl", get(list).post(create))
        .route("/api/v1/acl/{id}", get(read).delete(delete))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use broker_core::broker::{Broker, BrokerConfig};
    use cluster::{HybridClock, NodeId, Replicated, DEFAULT_MAX_DRIFT_MILLIS};
    use sdmqtt_server::shared::Shared;
    use storage::{MemoryStorage, Storage, Writer};

    use crate::config::AdminConfig;
    use crate::{testing, AdminServer, AdminState};

    async fn admin_over_a_directory() -> (crate::AdminRunning, Arc<directory::Directory>) {
        let store = Arc::new(MemoryStorage::new());
        let writer = Arc::new(
            Writer::spawn(
                Arc::clone(&store) as Arc<dyn Storage>,
                storage::WriterConfig::default(),
            )
            .expect("a writer thread"),
        );
        let replicated = Arc::new(
            Replicated::open(
                NodeId::mint(),
                HybridClock::resume(None, DEFAULT_MAX_DRIFT_MILLIS),
                &storage::Restored::default(),
            )
            .expect("an empty store holds nothing to refuse"),
        );
        let shared = Arc::new(Shared::with_replication(
            Broker::new(BrokerConfig::default()),
            Arc::clone(&writer),
            Arc::clone(&replicated),
        ));
        let directory = directory::Directory::new(
            writer,
            Arc::clone(&shared),
            replicated,
            directory::Settings::default(),
            Vec::new(),
            Vec::new(),
        );
        let config = AdminConfig {
            address: "127.0.0.1:0".parse().expect("a valid address"),
            ..AdminConfig::default()
        };
        let state = AdminState::new(shared).with_directory(Some(Arc::clone(&directory)));
        let admin = AdminServer::bind(&config, state)
            .await
            .expect("loopback needs no token");
        (admin, directory)
    }

    /// Rules are written whole, listed in consultation order, and
    /// refused by field when they are not rules.
    #[tokio::test]
    async fn rules_are_created_listed_in_order_and_deleted() {
        let (admin, directory) = admin_over_a_directory().await;
        let all = testing::post_json(
            admin.address(),
            "/api/v1/acl",
            None,
            r##"{"id":"all","subject":"anyone","filter":"#","publish":true,"subscribe":true}"##,
        )
        .await;
        assert_eq!(all.status, 200, "{}", all.body);
        assert!(
            all.body.contains("\"order\":100"),
            "the default order: {}",
            all.body
        );
        let narrow = testing::post_json(
            admin.address(),
            "/api/v1/acl",
            None,
            r#"{"id":"no-secret","order":10,"subject":"anyone","filter":"secret/#","subscribe":false}"#,
        )
        .await;
        assert_eq!(narrow.status, 200, "{}", narrow.body);
        assert!(
            !narrow.body.contains("publish"),
            "an absent opinion is absent: {}",
            narrow.body
        );

        let listed = testing::get(admin.address(), "/api/v1/acl").await;
        let first = listed.body.find("no-secret").expect("listed");
        let second = listed.body.find("\"id\":\"all\"").expect("listed");
        assert!(
            first < second,
            "consultation order, not identifier order: {}",
            listed.body
        );
        assert_eq!(directory.acl().len(), 2);

        let bad = testing::post_json(
            admin.address(),
            "/api/v1/acl",
            None,
            r##"{"id":"x","subject":"everyone","filter":"#","publish":true}"##,
        )
        .await;
        assert_eq!(bad.status, 400, "{}", bad.body);
        assert!(bad.body.contains("\"field\":\"subject\""), "{}", bad.body);
        let empty = testing::post_json(
            admin.address(),
            "/api/v1/acl",
            None,
            r##"{"id":"y","subject":"anyone","filter":"#"}"##,
        )
        .await;
        assert_eq!(empty.status, 400, "{}", empty.body);

        let one = testing::get(admin.address(), "/api/v1/acl/no-secret").await;
        assert_eq!(one.status, 200);
        let gone = testing::request(admin.address(), "DELETE", "/api/v1/acl/no-secret", None).await;
        assert_eq!(gone.status, 200, "{}", gone.body);
        assert_eq!(directory.acl().len(), 1);
        let again =
            testing::request(admin.address(), "DELETE", "/api/v1/acl/no-secret", None).await;
        assert_eq!(again.status, 404);
        admin.shutdown().await;
    }
}
