//! Route handlers for global/instance lifecycle, paths, agents, project,
//! status, PTY stubs, and the remote-feature stub. MCP runtime moved to
//! `routes::mcp`; the OAuth-related provider routes live in `routes::config`.

use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use kilo_protocol::Health;
use serde_json::json;

use crate::AppState;

pub(crate) async fn health() -> Json<Health> {
    Json(Health::ok())
}

pub(crate) async fn paths(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.store.paths())
}

/// `POST /global/dispose` — Bun returns `true`. Bun also fires a
/// `global.disposed` SSE event and clears its in-memory config cache.
/// In Rust, the config cache is per-request (`Store::config()` re-reads
/// from disk every call), so a no-op is contract-equivalent for now. The
/// event is not emitted because no Rust subscriber listens for it; if a
/// future webview gains a `global.disposed` handler, this should fire
/// through `state.bus`.
pub(crate) async fn global_dispose() -> impl IntoResponse {
    Json(true)
}

/// `POST /instance/dispose` — Bun returns `true` after disposing the
/// per-directory `Instance`. Rust does not yet have a per-directory
/// instance container (single-store-per-process model in M5), so this is
/// a no-op. M9 will revisit when worktree concurrency lands.
pub(crate) async fn instance_dispose() -> impl IntoResponse {
    Json(true)
}

pub(crate) async fn warnings() -> impl IntoResponse {
    Json(Vec::<serde_json::Value>::new())
}

pub(crate) async fn agents() -> impl IntoResponse {
    Json(json!([
        {
            "name": "code",
            "description": "Default coding agent.",
            "mode": "primary",
            "native": true,
            "permission": [],
            "options": {}
        },
        {
            "name": "plan",
            "description": "Plan mode.",
            "mode": "primary",
            "native": true,
            "permission": [],
            "options": {}
        }
    ]))
}

// PTY routes moved to `crate::routes::pty`. This module no longer
// declares the not-implemented stubs (see `routes/pty.rs` for the real
// `portable-pty`-backed handlers).

pub(crate) async fn project(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.store.project())
}

pub(crate) async fn status() -> impl IntoResponse {
    Json(json!({}))
}

/// `GET /remote/status` — the extension's RemoteStatusService polls this.
/// Bun shape: `{ enabled: boolean, ... }`. Returning `enabled: false`
/// disables the remote feature path while keeping the route 200 OK so
/// the service doesn't go into permanent error.
pub(crate) async fn remote_status() -> impl IntoResponse {
    Json(json!({ "enabled": false }))
}
