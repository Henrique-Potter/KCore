//! Route handlers for global/instance lifecycle, paths, agents, project,
//! status, PTY stubs, and the remote-feature stub. MCP runtime moved to
//! `routes::mcp`; the OAuth-related provider routes live in `routes::config`.

use std::{collections::BTreeMap, sync::Arc};

use axum::{extract::State, response::IntoResponse, Json};
use kilo_protocol::{GlobalEvent, Health};
use serde_json::json;

use crate::http::sse;
use crate::AppState;

pub(crate) async fn health() -> Json<Health> {
    Json(Health::ok())
}

pub(crate) async fn paths(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.store.paths())
}

/// `POST /global/dispose` — Bun returns `true` and publishes
/// `global.disposed` so webview subscribers (`event-reducer.ts:27`,
/// `KiloProvider.ts:2712`, `AutocompleteServiceManager.ts:106`)
/// re-bootstrap after login/logout/org-switch. The Rust config cache is
/// per-request, so the response stays a no-op `true`; only the event is
/// added.
pub(crate) async fn global_dispose(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    sse::publish(&state, GlobalEvent::bus("global.disposed", json!({})));
    Json(true)
}

/// `POST /instance/dispose` — Bun returns `true` and publishes
/// `server.instance.disposed`, which the `/event` SSE loop watches to
/// terminate the stream so the SDK reconnect callback chain refires
/// (`event-reducer.ts:105`, `KiloProvider.ts:2717-2723`). Rust still has
/// no per-directory instance container; the response remains `true`.
pub(crate) async fn instance_dispose(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    sse::publish(
        &state,
        GlobalEvent::bus("server.instance.disposed", json!({})),
    );
    Json(true)
}

pub(crate) async fn warnings() -> impl IntoResponse {
    Json(Vec::<serde_json::Value>::new())
}

pub(crate) async fn agents(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(crate::agent::catalog::list(&state))
}

// PTY routes moved to `crate::routes::pty`. This module no longer
// declares the not-implemented stubs (see `routes/pty.rs` for the real
// `portable-pty`-backed handlers).

pub(crate) async fn project(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.store.project())
}

pub(crate) async fn status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let runners = state.runners.lock().unwrap();
    let status = runners
        .keys()
        .map(|id| (id.clone(), json!({ "type": "busy" })))
        .collect::<BTreeMap<_, _>>();
    Json(status)
}

/// `GET /remote/status` — the extension's RemoteStatusService polls this.
/// Bun shape: `{ enabled: boolean, ... }`. Returning `enabled: false`
/// disables the remote feature path while keeping the route 200 OK so
/// the service doesn't go into permanent error.
pub(crate) async fn remote_status() -> impl IntoResponse {
    Json(json!({ "enabled": false }))
}
