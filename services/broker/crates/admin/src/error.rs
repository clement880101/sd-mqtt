//! The one shape every failure of this API takes.
//!
//! Section 13 says library crates expose typed errors and the binary
//! converts at the boundary. This is that boundary for HTTP, and the
//! shape below is what the interface is written against.
//!
//! # The shape
//!
//! Every failure — a bad query string, an unknown path, a wrong
//! method, a missing token, an internal fault — is answered with this
//! body and nothing else:
//!
//! ```json
//! {
//!   "error": {
//!     "code": "invalid_request",
//!     "message": "limit must be between 1 and 1000",
//!     "field": "limit"
//!   }
//! }
//! ```
//!
//! - `code` is a fixed, machine-readable string from a closed set. It
//!   is the only part a client may branch on.
//! - `message` is one English sentence for a human reading a log or a
//!   toast. It may be reworded in any release, and nothing may match
//!   on it.
//! - `field` names the request parameter at fault, and is absent when
//!   no single one is. It lets a form mark the input that was wrong
//!   without parsing the sentence.
//!
//! The `Content-Type` is `application/json`, and the HTTP status is
//! whatever [`ErrorCode::status`] says for the code. The two always
//! agree: the status is derived from the code rather than passed
//! beside it, so an endpoint cannot answer `404` with a code meaning
//! something else.
//!
//! # Why a code and not a sentence
//!
//! A client that has to distinguish "this session is gone" from "your
//! token is wrong" can either read a fixed token or match on prose.
//! Matching on prose makes every message a compatibility surface:
//! the first time somebody improves the wording, an interface that
//! was branching on it silently takes the wrong branch. A closed set
//! of codes makes the wording free to change and the branch safe.
//!
//! # Why the body is nested under `error`
//!
//! So that a success and a failure can never be confused by a client
//! that forgot to look at the status code. No successful response
//! this API returns has a top-level `error` key, and every failure
//! has one, so `"error" in body` is a complete test.
//!
//! # What is deliberately outside it
//!
//! The health endpoints. An unready readiness probe is that endpoint
//! working correctly rather than failing, and a probe wants the same
//! document either way; see [`crate::health`]. The metrics endpoint
//! answers in the exposition format on success and in this shape on
//! failure, because a scraper reading a JSON error is better off than
//! one reading a truncated exposition.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

/// The closed set of things a client may branch on.
///
/// **Adding a variant is a compatible change and renaming one is
/// not.** A client is expected to treat a code it does not know as
/// the class its HTTP status implies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The request itself is wrong: a query parameter that will not
    /// parse, a value out of range, a body that is not what the
    /// endpoint accepts.
    InvalidRequest,
    /// No such path, or no such thing at a path that exists.
    NotFound,
    /// That path does not accept that method.
    MethodNotAllowed,
    /// A token is configured and the request did not present the
    /// right one.
    ///
    /// Used from the first mutating endpoint onwards. Defined here
    /// rather than where it is first thrown, because the set is meant
    /// to be readable in one place.
    Unauthorized,
    /// Something went wrong that is not the caller's fault.
    Internal,
}

impl ErrorCode {
    /// The status that goes with this code.
    ///
    /// One function, so that the code and the status cannot disagree.
    pub fn status(self) -> StatusCode {
        match self {
            ErrorCode::InvalidRequest => StatusCode::BAD_REQUEST,
            ErrorCode::NotFound => StatusCode::NOT_FOUND,
            ErrorCode::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            ErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
            ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// The contents of the `error` key.
#[derive(Debug, Serialize)]
pub struct ApiErrorBody {
    /// What went wrong, for a machine.
    pub code: ErrorCode,
    /// What went wrong, for a person. Never matched on.
    pub message: String,
    /// The request parameter at fault, when one is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<&'static str>,
}

/// A failure, in the one shape.
///
/// Nested under `error` on the wire, which is what the wrapper below
/// is for.
#[derive(Debug, Serialize)]
pub struct ApiError {
    /// The failure.
    pub error: ApiErrorBody,
}

impl ApiError {
    /// A failure with a code and a sentence.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            error: ApiErrorBody {
                code,
                message: message.into(),
                field: None,
            },
        }
    }

    /// The same, naming the parameter at fault.
    pub fn in_field(code: ErrorCode, field: &'static str, message: impl Into<String>) -> Self {
        let mut error = Self::new(code, message);
        error.error.field = Some(field);
        error
    }

    /// The request will not parse or cannot mean what it says.
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidRequest, message)
    }

    /// No such path, or no such thing.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    /// The code, for a test that should not read the sentence either.
    pub fn code(&self) -> ErrorCode {
        self.error.code
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // The status comes from the code and from nowhere else.
        (self.error.code.status(), Json(self)).into_response()
    }
}

/// What answers a path this API does not serve.
///
/// Registered as the router's fallback rather than left to axum's
/// default, which is an empty `404` with no body at all. A client
/// that gets one of those cannot tell a path it spelled wrong from a
/// proxy that swallowed the request.
pub(crate) async fn not_found() -> ApiError {
    ApiError::not_found("no such resource on this listener")
}

/// What answers a method a served path does not accept.
pub(crate) async fn method_not_allowed() -> ApiError {
    ApiError::new(
        ErrorCode::MethodNotAllowed,
        "that resource does not accept this method",
    )
}
