//! Error envelopes returned by routes and consumed by `agent::run_turn`.
//!
//! `TurnError` is the agent-loop error type; `RouteError` describes provider
//! mismatches surfaced to the client. The free helpers (`turn_error`,
//! `busy_error`, `unsupported_provider_error`, `internal_error*`) build Axum
//! `Response`s in the Bun-compatible `{ name, data: { message } }` shape so
//! the SDK's generated client can `response.json()` non-2xx bodies without
//! parse errors.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Stable error-name set the SDK can rely on. Every literal passed to
/// [`internal_error_named`] (and the limits-module helpers) must appear
/// here. This is enforced by a `debug_assert!` in
/// [`internal_error_named`] so a forgotten entry surfaces in tests and
/// CI rather than in production.
///
/// Two case conventions coexist deliberately:
///
/// - `PascalCaseError` — legacy / library-error wraps. Names predating
///   the operational-invariant work and matching Bun's `NamedError`
///   class names.
/// - `snake_case` — boundary/policy errors specified by the migration
///   plan's "Operational invariants" and "Storage and process
///   self-healing invariants" sections. Locked in by the doc.
///
/// Keep this list in sync with
/// [`packages/kilo-vscode-sidecar-rs/CONTRACT.md`](../../../CONTRACT.md).
pub(crate) const ALLOWED_INTERNAL_ERROR_NAMES: &[&str] = &[
    // Legacy / library wraps.
    "InternalError",
    "BusyError",
    "UnsupportedProviderError",
    "OauthCallbackError",
    "OauthCallbackListenerError",
    // Operational-invariant boundary errors.
    "request_too_large",
    "sse_capacity_exceeded",
    "session_quota_exceeded",
    "message_part_too_large",
    "shell_unavailable",
    "schema_migration_failed",
    "store_unavailable",
    // OAuth flow errors (4xx; emitted by routes/config.rs OAuth handlers).
    "OauthUnsupportedProvider",
    "OauthUnsupportedMethod",
    "OauthCodeMissing",
    "OauthPendingMissing",
    "OauthStateMismatch",
    "OauthCallbackTimeout",
    // PTY route errors (Rust-only surface; routes/pty.rs).
    "RustPtyOpenError",
    "RustPtySpawnError",
    "RustPtyWriterError",
    "RustPtyReaderError",
    "RustPtyNotFoundError",
    "RustPtyWriteError",
    "RustPtyResizeError",
    // Worktree route errors (routes/worktree.rs).
    "WorktreeInvalidInputError",
    "WorktreeCreateFailedError",
    "WorktreeRemoveFailedError",
    "WorktreeResetFailedError",
    "WorktreeResetUnsafeError",
    "WorktreeNotGitError",
    "WorktreeListFailedError",
    "WorktreePathSafetyError",
    // MCP route errors (Rust-only surface; routes/mcp.rs and kilo-mcp).
    "RustMcpAuthInvalidError",
    "RustMcpNotFoundError",
    "RustMcpAuthPersistError",
    "RustMcpOAuthConfigError",
    "RustMcpOAuthPersistError",
    "RustMcpOAuthCallbackError",
    "RustMcpOAuthStateError",
    "RustMcpDisabledError",
    "RustMcpDisconnectedError",
    "RustMcpOAuthDiscoveryError",
    "RustMcpOAuthRegistrationError",
    "RustMcpAuthRefreshError",
    "RustMcpRemoteConfigError",
    "RustMcpHttpError",
    "RustMcpTimeoutError",
    "RustMcpMalformedResponseError",
    "RustMcpToolError",
    "RustMcpWriteError",
    "RustMcpClosedError",
    "RustMcpNotImplementedError",
    // Assistant-side error-envelope names. These are emitted via direct
    // `json!` into `assistant.info.error` (NOT via `internal_error_named`)
    // and are documented in CONTRACT.md's "Assistant message error
    // envelopes" table. Listed here so the registry stays the single
    // source of truth for every named error the sidecar emits.
    "CompactionError",
    "APIError",
];

#[derive(Debug)]
pub(crate) enum TurnError {
    Busy,
    NotFound,
    Unsupported(RouteError),
    Db(rusqlite::Error),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RouteError {
    pub(crate) provider: String,
    pub(crate) model: Option<String>,
    pub(crate) reason: &'static str,
}

impl From<rusqlite::Error> for TurnError {
    fn from(err: rusqlite::Error) -> Self {
        Self::Db(err)
    }
}

pub(crate) fn turn_error(err: TurnError) -> Response {
    match err {
        TurnError::Busy => busy_error(),
        TurnError::NotFound => StatusCode::NOT_FOUND.into_response(),
        TurnError::Unsupported(err) => unsupported_provider_error(err),
        TurnError::Db(err) => internal_error(err.to_string()),
    }
}

pub(crate) fn busy_error() -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "name": "BusyError",
            "data": { "message": "Session is busy" },
        })),
    )
        .into_response()
}

pub(crate) fn unsupported_provider_error(err: RouteError) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "name": "UnsupportedProviderError",
            "data": {
                "message": err.reason,
                "providerID": err.provider,
                "modelID": err.model,
            },
        })),
    )
        .into_response()
}

/// Produce a Bun-compatible 500 response body. Bun's `ErrorMiddleware`
/// returns the `NamedError.toObject()` shape `{ name, data: { message } }`
/// for unhandled errors; the default name is `"InternalError"`. Use
/// [`internal_error_named`] when the call site has a meaningful tag (e.g.
/// `OauthCallbackError`). Returning plain text would make the SDK throw a
/// JSON parse error and mask the underlying cause.
pub(crate) fn internal_error(message: impl Into<String>) -> Response {
    internal_error_named("InternalError", message)
}

pub(crate) fn internal_error_named(name: &str, message: impl Into<String>) -> Response {
    debug_assert!(
        ALLOWED_INTERNAL_ERROR_NAMES.contains(&name),
        "internal_error_named({name:?}) uses a name absent from ALLOWED_INTERNAL_ERROR_NAMES — \
         add it to error.rs and CONTRACT.md so the SDK type union stays in sync",
    );
    named_response(StatusCode::INTERNAL_SERVER_ERROR, name, message)
}

/// Bun-compatible 400 NamedError response with the registry assertion
/// that `internal_error_named` enforces. Canonical replacement for the
/// free `bad_request_named` helper in `routes/config.rs`, which bypasses
/// `ALLOWED_INTERNAL_ERROR_NAMES` and is therefore deprecated. Call
/// sites are migrated in a separate pass; new code should call this
/// helper.
#[allow(dead_code)]
pub(crate) fn bad_request_named(name: &'static str, message: impl Into<String>) -> Response {
    debug_assert!(
        ALLOWED_INTERNAL_ERROR_NAMES.contains(&name),
        "bad_request_named({name:?}) uses a name absent from ALLOWED_INTERNAL_ERROR_NAMES — \
         add it to error.rs and CONTRACT.md so the SDK type union stays in sync",
    );
    named_response(StatusCode::BAD_REQUEST, name, message)
}

/// Build a non-2xx Bun-compatible NamedError response with a non-default
/// status code. Used by limits.rs for 413/503/507 responses, which must
/// also pass through the `ALLOWED_INTERNAL_ERROR_NAMES` debug-time check
/// so name drift surfaces in tests instead of production.
pub(crate) fn named_response(
    status: StatusCode,
    name: &str,
    message: impl Into<String>,
) -> Response {
    debug_assert!(
        ALLOWED_INTERNAL_ERROR_NAMES.contains(&name),
        "named_response({name:?}) uses a name absent from ALLOWED_INTERNAL_ERROR_NAMES — \
         add it to error.rs and CONTRACT.md so the SDK type union stays in sync",
    );
    (
        status,
        Json(json!({
            "name": name,
            "data": { "message": message.into() },
        })),
    )
        .into_response()
}
