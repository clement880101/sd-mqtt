//! Section 11.1's API keys resource, over the directory.
//!
//! # Shown once
//!
//! `POST /api/v1/keys` takes a name and a scope and answers with the
//! view and, this once, the key's text. The directory keeps a digest of
//! the text and nothing that would give it back; a key whose text is
//! lost is revoked and minted again. The listing and the read answer
//! the view alone.
//!
//! # Admin, and nothing less
//!
//! Minting a key is granting what it may do, and revoking one is
//! taking it away; both are `admin`'s. A `write` key that could mint an
//! `admin` one would be an `admin` key with an extra step.

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use directory::{ApiKeyView, Directory, DirectoryError, Scope};
use serde::{Deserialize, Serialize};
use storage::record::Secret;

use crate::error::{ApiError, ErrorCode};
use crate::resources::{page, select, Authorized, Page, Paging, ToAdminister};
use crate::AdminState;

/// A key as a caller asks for one.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeySpec {
    /// What to call it.
    pub name: String,
    /// `read`, `write` or `admin`.
    pub scope: Scope,
}

/// A key as it is answered at creation: the view, and the text once.
#[derive(Debug, Serialize)]
pub struct Issued {
    /// Everything a listing would show.
    #[serde(flatten)]
    pub view: ApiKeyView,
    /// The key itself. Not stored, not shown again.
    pub key: Secret,
}

fn directory(state: &AdminState) -> Result<&std::sync::Arc<Directory>, ApiError> {
    state.directory.as_ref().ok_or_else(|| {
        ApiError::not_found(
            "this node has no store, so it holds no API keys: section 11.1 makes a key \
             durable metadata, and a broker started without storage.directory writes \
             nothing down. The configured admin.token still works",
        )
    })
}

fn refusal(error: DirectoryError) -> ApiError {
    match error {
        DirectoryError::BadName => {
            ApiError::in_field(ErrorCode::InvalidRequest, "name", error.to_string())
        }
        DirectoryError::NoSuchKey { .. } => ApiError::not_found(error.to_string()),
        DirectoryError::Storage { .. } => ApiError::new(ErrorCode::Internal, error.to_string()),
        other => ApiError::new(ErrorCode::Internal, other.to_string()),
    }
}

async fn list(
    State(state): State<AdminState>,
    paging: Paging,
) -> Result<Json<Page<ApiKeyView>>, ApiError> {
    let held = directory(&state)?.api_keys();
    let keys: Vec<_> = held
        .iter()
        .map(|key| ([key.id.as_str(), "", ""], key))
        .collect();
    let selection = select(keys.into_iter(), paging.after.as_ref(), paging.limit);
    Ok(Json(page(selection, |_, key| key.clone())))
}

async fn read(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Json<ApiKeyView>, ApiError> {
    directory(&state)?
        .api_key(&id)
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("there is no API key with the identifier {id}")))
}

async fn create(
    State(state): State<AdminState>,
    _authorized: Authorized<ToAdminister>,
    body: Result<Json<KeySpec>, JsonRejection>,
) -> Result<Json<Issued>, ApiError> {
    let Json(spec) = body.map_err(|rejection| {
        ApiError::invalid_request(format!(
            "the request body is not an API key: {}",
            rejection.body_text()
        ))
    })?;
    let minted = directory(&state)?
        .create_api_key(&spec.name, spec.scope)
        .await
        .map_err(refusal)?;
    Ok(Json(Issued {
        view: ApiKeyView::from(&minted.record),
        key: minted.text,
    }))
}

async fn revoke(
    State(state): State<AdminState>,
    _authorized: Authorized<ToAdminister>,
    Path(id): Path<String>,
) -> Result<Json<Revoked>, ApiError> {
    directory(&state)?
        .revoke_api_key(&id)
        .await
        .map_err(refusal)?;
    Ok(Json(Revoked { id }))
}

/// What a revocation that happened answers.
#[derive(Debug, Serialize)]
struct Revoked {
    /// What is gone.
    id: String,
}

/// The routes this module owns.
pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/api/v1/keys", get(list).post(create))
        .route("/api/v1/keys/{id}", get(read).delete(revoke))
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

    /// An admin listener over a directory on an in-memory store, with
    /// `token` configured when one is given.
    async fn admin_over_a_directory(
        token: Option<&str>,
    ) -> (crate::AdminRunning, Arc<directory::Directory>) {
        let store = Arc::new(MemoryStorage::new());
        let writer = Arc::new(
            Writer::spawn(
                Arc::clone(&store) as Arc<dyn Storage>,
                storage::WriterConfig::default(),
            )
            .expect("a writer thread"),
        );
        let me = NodeId::mint();
        let replicated = Arc::new(
            Replicated::open(
                me,
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
            Vec::new(),
        );
        let config = AdminConfig {
            address: "127.0.0.1:0".parse().expect("a valid address"),
            token: token.map(crate::Token::new),
            ..AdminConfig::default()
        };
        let state = AdminState::new(shared).with_directory(Some(Arc::clone(&directory)));
        let admin = AdminServer::bind(&config, state)
            .await
            .expect("loopback needs no token");
        (admin, directory)
    }

    /// Mint a key as `bearer` and answer its text and identifier.
    async fn mint(
        admin: &crate::AdminRunning,
        bearer: Option<&str>,
        name: &str,
        scope: &str,
    ) -> (String, String) {
        let body = format!("{{\"name\":\"{name}\",\"scope\":\"{scope}\"}}");
        let answered = testing::post_json(admin.address(), "/api/v1/keys", bearer, &body).await;
        assert_eq!(answered.status, 200, "{}", answered.body);
        let json: serde_json::Value = serde_json::from_str(&answered.body).expect("JSON");
        let key = json["key"].as_str().expect("the key, once").to_owned();
        let id = json["id"].as_str().expect("its identifier").to_owned();
        assert!(key.starts_with("sdmqtt_"), "{key}");
        assert!(key.contains(&id), "the text carries the identifier: {key}");
        assert_eq!(json["scope"], scope);
        assert_eq!(json["name"], name);
        (key, id)
    }

    /// The body of a publish request.
    fn publication(topic: &str) -> String {
        format!("{{\"topic\":\"{topic}\",\"payload\":\"x\",\"qos\":0}}")
    }

    /// Mint, list, read, revoke; and the key's text appears once.
    #[tokio::test]
    async fn keys_are_minted_once_listed_without_secrets_and_revoked() {
        let (admin, directory) = admin_over_a_directory(None).await;
        let (key, id) = mint(&admin, None, "ci", "write").await;
        assert_eq!(directory.scope_of_key(&key), Some(directory::Scope::Write));

        let listed = testing::get(admin.address(), "/api/v1/keys").await;
        assert_eq!(listed.status, 200);
        assert!(listed.body.contains("\"total\":1"), "{}", listed.body);
        assert!(
            listed.body.contains(&format!("\"id\":\"{id}\"")),
            "{}",
            listed.body
        );
        let secret = key.rsplit('_').next().expect("the secret half");
        assert!(
            !listed.body.contains(secret) && !listed.body.contains("\"key\""),
            "a listing carries no secret: {}",
            listed.body
        );
        let one = testing::get(admin.address(), &format!("/api/v1/keys/{id}")).await;
        assert_eq!(one.status, 200, "{}", one.body);
        assert!(!one.body.contains(secret), "{}", one.body);
        let missing = testing::get(admin.address(), "/api/v1/keys/nobody").await;
        assert_eq!(missing.status, 404);

        let unknown_scope = testing::post_json(
            admin.address(),
            "/api/v1/keys",
            None,
            r#"{"name":"ci","scope":"root"}"#,
        )
        .await;
        assert_eq!(unknown_scope.status, 400, "{}", unknown_scope.body);
        let unnamed = testing::post_json(
            admin.address(),
            "/api/v1/keys",
            None,
            r#"{"name":"","scope":"read"}"#,
        )
        .await;
        assert_eq!(unnamed.status, 400, "{}", unnamed.body);
        assert!(
            unnamed.body.contains("\"field\":\"name\""),
            "{}",
            unnamed.body
        );

        let revoked = testing::request(
            admin.address(),
            "DELETE",
            &format!("/api/v1/keys/{id}"),
            None,
        )
        .await;
        assert_eq!(revoked.status, 200, "{}", revoked.body);
        assert_eq!(directory.scope_of_key(&key), None);
        let again = testing::request(
            admin.address(),
            "DELETE",
            &format!("/api/v1/keys/{id}"),
            None,
        )
        .await;
        assert_eq!(again.status, 404);
        let listed = testing::get(admin.address(), "/api/v1/keys").await;
        assert!(listed.body.contains("\"total\":0"), "{}", listed.body);

        admin.shutdown().await;
    }

    /// Each scope covers its own endpoints and the ones below, and is
    /// `forbidden` above; the configured token is `admin`; a revoked
    /// or invented key is `unauthorized`.
    #[tokio::test]
    async fn a_key_grants_its_scope_and_nothing_above_it() {
        let (admin, _directory) = admin_over_a_directory(Some("bootstrap")).await;
        let at = admin.address();

        // Minting needs admin: the token has it, and nothing presented
        // or a wrong bearer does not.
        let nothing =
            testing::post_json(at, "/api/v1/keys", None, r#"{"name":"a","scope":"read"}"#).await;
        assert_eq!(nothing.status, 401, "{}", nothing.body);
        let invented = testing::post_json(
            at,
            "/api/v1/keys",
            Some("sdmqtt_nobody_atall"),
            r#"{"name":"a","scope":"read"}"#,
        )
        .await;
        assert_eq!(invented.status, 401, "{}", invented.body);
        assert!(
            invented.body.contains("\"code\":\"unauthorized\""),
            "{}",
            invented.body
        );

        let (reader, _) = mint(&admin, Some("bootstrap"), "dashboard", "read").await;
        let (writer, _) = mint(&admin, Some("bootstrap"), "ci", "write").await;
        let (operator, operator_id) = mint(&admin, Some("bootstrap"), "ops", "admin").await;

        // read: a tap opens; a publish is forbidden, and the refusal
        // names both scopes.
        let tap =
            testing::post_json(at, "/api/v1/taps", Some(&reader), r#"{"filter":"a/#"}"#).await;
        assert_eq!(tap.status, 200, "{}", tap.body);
        let refused =
            testing::post_json(at, "/api/v1/publish", Some(&reader), &publication("a/b")).await;
        assert_eq!(refused.status, 403, "{}", refused.body);
        assert!(
            refused.body.contains("\"code\":\"forbidden\"")
                && refused.body.contains("read")
                && refused.body.contains("write"),
            "{}",
            refused.body
        );

        // write: a publish goes; a user does not; a tap still opens.
        let published =
            testing::post_json(at, "/api/v1/publish", Some(&writer), &publication("a/b")).await;
        assert_eq!(published.status, 200, "{}", published.body);
        let no_user = testing::post_json(
            at,
            "/api/v1/users",
            Some(&writer),
            r#"{"name":"alice","password":"hunter2"}"#,
        )
        .await;
        assert_eq!(no_user.status, 403, "{}", no_user.body);
        let no_key = testing::post_json(
            at,
            "/api/v1/keys",
            Some(&writer),
            r#"{"name":"more","scope":"read"}"#,
        )
        .await;
        assert_eq!(no_key.status, 403, "{}", no_key.body);
        let tap =
            testing::post_json(at, "/api/v1/taps", Some(&writer), r#"{"filter":"b/#"}"#).await;
        assert_eq!(tap.status, 200, "{}", tap.body);

        // admin: everything, including minting another admin key.
        let user = testing::post_json(
            at,
            "/api/v1/users",
            Some(&operator),
            r#"{"name":"alice","password":"hunter2"}"#,
        )
        .await;
        assert_eq!(user.status, 200, "{}", user.body);
        let (_, _) = mint(&admin, Some(&operator), "another", "admin").await;

        // A prefix of a real key is not a key.
        let prefix = &writer[..writer.len() - 1];
        let short =
            testing::post_json(at, "/api/v1/publish", Some(prefix), &publication("a/b")).await;
        assert_eq!(short.status, 401, "{}", short.body);

        // Revoked, by the token, and the key is nothing at once.
        let revoked = testing::request(
            at,
            "DELETE",
            &format!("/api/v1/keys/{operator_id}"),
            Some("bootstrap"),
        )
        .await;
        assert_eq!(revoked.status, 200, "{}", revoked.body);
        let gone = testing::post_json(
            at,
            "/api/v1/users",
            Some(&operator),
            r#"{"name":"bob","password":"hunter2"}"#,
        )
        .await;
        assert_eq!(gone.status, 401, "{}", gone.body);

        admin.shutdown().await;
    }

    /// Without a store there are no keys; the token still works, and a
    /// bearer that is not it is refused rather than looked up nowhere.
    #[tokio::test]
    async fn a_node_without_a_store_holds_no_keys() {
        let shared = Arc::new(Shared::new(BrokerConfig::default()));
        let config = AdminConfig {
            address: "127.0.0.1:0".parse().expect("a valid address"),
            token: Some(crate::Token::new("bootstrap")),
            ..AdminConfig::default()
        };
        let admin = AdminServer::bind(&config, AdminState::new(shared))
            .await
            .expect("loopback needs no token");
        let at = admin.address();
        let none = testing::post_json(
            at,
            "/api/v1/keys",
            Some("bootstrap"),
            r#"{"name":"a","scope":"read"}"#,
        )
        .await;
        assert_eq!(none.status, 404, "{}", none.body);
        let listed = testing::get(at, "/api/v1/keys").await;
        assert_eq!(listed.status, 404);
        let wrong = testing::post_json(
            at,
            "/api/v1/publish",
            Some("sdmqtt_a_b"),
            &publication("a/b"),
        )
        .await;
        assert_eq!(wrong.status, 401, "{}", wrong.body);
        let token = testing::post_json(
            at,
            "/api/v1/publish",
            Some("bootstrap"),
            &publication("a/b"),
        )
        .await;
        assert_eq!(token.status, 200, "{}", token.body);
        admin.shutdown().await;
    }
}
