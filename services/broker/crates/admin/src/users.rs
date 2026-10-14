//! Section 11.1's users resource, over the directory.
//!
//! # A password goes in and nothing comes out
//!
//! `POST /api/v1/users` takes a name and a password and answers with the
//! view: the name, which credential forms exist, when it was made. The
//! argon2 string and the SCRAM verifier are never returned — section
//! 12.2 says a verifier is a secret equivalent to a private key — and
//! there is no endpoint that reads them. Setting a password again is
//! the same request to the same path: a put over a held name replaces
//! both forms, because both are derived from the one password.
//!
//! # Guarded like every mutation
//!
//! Creating and deleting need the configured token today, and an
//! `admin` key once API keys exist. [`Authorized`] is the guard.

use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use directory::{Directory, DirectoryError, UserView};
use serde::Deserialize;
use storage::record::Secret;

use crate::error::{ApiError, ErrorCode};
use crate::resources::{page, select, Authorized, Page, Paging};
use crate::AdminState;

/// A user as a caller writes one.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserSpec {
    /// The name a client presents.
    pub name: String,
    /// The password. Hashed before it is stored and never echoed; the
    /// wrapper keeps it out of any log line this handler could write.
    pub password: Secret,
}

fn directory(state: &AdminState) -> Result<&std::sync::Arc<Directory>, ApiError> {
    state.directory.as_ref().ok_or_else(|| {
        ApiError::not_found(
            "this node has no store, so it holds no users: section 12.1 makes a user \
             durable metadata, and a broker started without storage.directory writes \
             nothing down",
        )
    })
}

fn refusal(error: DirectoryError) -> ApiError {
    match error {
        DirectoryError::BadName => {
            ApiError::in_field(ErrorCode::InvalidRequest, "name", error.to_string())
        }
        DirectoryError::EmptyPassword => {
            ApiError::in_field(ErrorCode::InvalidRequest, "password", error.to_string())
        }
        DirectoryError::NoSuchUser { .. } => ApiError::not_found(error.to_string()),
        DirectoryError::Storage { .. } => ApiError::new(ErrorCode::Internal, error.to_string()),
    }
}

async fn list(
    State(state): State<AdminState>,
    paging: Paging,
) -> Result<Json<Page<UserView>>, ApiError> {
    let held = directory(&state)?.users();
    let keys: Vec<_> = held
        .iter()
        .map(|user| ([user.name.as_str(), "", ""], user))
        .collect();
    let selection = select(keys.into_iter(), paging.after.as_ref(), paging.limit);
    Ok(Json(page(selection, |_, user| user.clone())))
}

async fn read(
    State(state): State<AdminState>,
    Path(name): Path<String>,
) -> Result<Json<UserView>, ApiError> {
    directory(&state)?
        .user(&name)
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("there is no user named {name}")))
}

async fn create(
    State(state): State<AdminState>,
    _authorized: Authorized,
    body: Result<Json<UserSpec>, JsonRejection>,
) -> Result<Json<UserView>, ApiError> {
    let Json(spec) = body.map_err(|rejection| {
        ApiError::invalid_request(format!(
            "the request body is not a user: {}",
            rejection.body_text()
        ))
    })?;
    let view = directory(&state)?
        .put_user(&spec.name, &spec.password)
        .await
        .map_err(refusal)?;
    Ok(Json(view))
}

async fn delete(
    State(state): State<AdminState>,
    _authorized: Authorized,
    Path(name): Path<String>,
) -> Result<Json<Deleted>, ApiError> {
    directory(&state)?
        .delete_user(&name)
        .await
        .map_err(refusal)?;
    Ok(Json(Deleted { name }))
}

/// What a delete that happened answers: JSON, like every answer here.
#[derive(Debug, serde::Serialize)]
struct Deleted {
    /// Who is gone.
    name: String,
}

/// The routes this module owns.
pub(crate) fn routes() -> Router<AdminState> {
    Router::new()
        .route("/api/v1/users", get(list).post(create))
        .route("/api/v1/users/{name}", get(read).delete(delete))
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

    /// An admin listener over a directory on an in-memory store.
    async fn admin_over_a_directory() -> (crate::AdminRunning, Arc<directory::Directory>) {
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

    /// Create, list, read, delete; and the password never comes back.
    #[tokio::test]
    async fn users_are_created_listed_and_deleted_and_the_password_never_returns() {
        let (admin, directory) = admin_over_a_directory().await;
        let created = testing::post_json(
            admin.address(),
            "/api/v1/users",
            None,
            r#"{"name":"alice","password":"hunter2"}"#,
        )
        .await;
        assert_eq!(created.status, 200, "{}", created.body);
        assert!(
            created.body.contains("\"name\":\"alice\""),
            "{}",
            created.body
        );
        assert!(
            created.body.contains("\"has_password\":true"),
            "{}",
            created.body
        );
        assert!(created.body.contains("\"scram\":true"), "{}", created.body);
        assert!(
            !created.body.contains("hunter2") && !created.body.contains("argon2"),
            "the answer carries no secret: {}",
            created.body
        );
        assert!(directory.check_password("alice", b"hunter2"));

        let listed = testing::get(admin.address(), "/api/v1/users").await;
        assert!(listed.body.contains("\"total\":1"), "{}", listed.body);
        assert!(!listed.body.contains("argon2"), "{}", listed.body);
        let one = testing::get(admin.address(), "/api/v1/users/alice").await;
        assert_eq!(one.status, 200);
        let missing = testing::get(admin.address(), "/api/v1/users/nobody").await;
        assert_eq!(missing.status, 404);

        // A second put is a password change, not a second user.
        let changed = testing::post_json(
            admin.address(),
            "/api/v1/users",
            None,
            r#"{"name":"alice","password":"hunter3"}"#,
        )
        .await;
        assert_eq!(changed.status, 200, "{}", changed.body);
        assert!(directory.check_password("alice", b"hunter3"));
        assert!(!directory.check_password("alice", b"hunter2"));
        let listed = testing::get(admin.address(), "/api/v1/users").await;
        assert!(listed.body.contains("\"total\":1"), "{}", listed.body);

        let empty = testing::post_json(
            admin.address(),
            "/api/v1/users",
            None,
            r#"{"name":"bob","password":""}"#,
        )
        .await;
        assert_eq!(empty.status, 400, "{}", empty.body);
        assert!(
            empty.body.contains("\"field\":\"password\""),
            "{}",
            empty.body
        );

        let deleted =
            testing::request(admin.address(), "DELETE", "/api/v1/users/alice", None).await;
        assert_eq!(deleted.status, 200, "{}", deleted.body);
        assert!(!directory.check_password("alice", b"hunter3"));
        let again = testing::request(admin.address(), "DELETE", "/api/v1/users/alice", None).await;
        assert_eq!(again.status, 404);

        admin.shutdown().await;
    }
}
