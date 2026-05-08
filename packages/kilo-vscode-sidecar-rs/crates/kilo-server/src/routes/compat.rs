//! Compatibility handlers for extension-invoked routes that are outside
//! the Rust sidecar's core agent loop.
//!
//! These routes intentionally return conservative disabled/empty shapes
//! instead of 404. The VS Code extension already calls them through the
//! generated SDK; a 404 can trip the runtime fallback/mutation logic and
//! degrade unrelated chat flows. Provider-backed implementations can
//! replace these stubs slice-by-slice without changing the route contract.

use std::convert::Infallible;
use std::sync::Arc;

use async_stream::stream;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    Json,
};
use futures_core::Stream;
use serde_json::{json, Value};

use crate::AppState;

pub(crate) async fn remote_enable() -> impl IntoResponse {
    Json(remote_disabled())
}

pub(crate) async fn remote_disable() -> impl IntoResponse {
    Json(remote_disabled())
}

pub(crate) async fn commit_message(Json(input): Json<Value>) -> impl IntoResponse {
    let selected = input
        .get("selectedFiles")
        .and_then(Value::as_array)
        .map(|items| items.len())
        .unwrap_or(0);
    let message = if selected > 0 {
        format!("Update {selected} selected file(s)")
    } else {
        "Update files".to_string()
    };
    Json(json!({ "message": message }))
}

pub(crate) async fn kilo_profile() -> impl IntoResponse {
    Json(json!({
        "profile": {
            "email": "",
            "name": "Kilo",
            "organizations": []
        },
        "balance": null,
        "currentOrgId": null
    }))
}

pub(crate) async fn kilo_organization(Json(_input): Json<Value>) -> impl IntoResponse {
    Json(true)
}

pub(crate) async fn kilo_fim(
    Json(_input): Json<Value>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let out = stream! {
        yield Ok(Event::default().data(
            json!({
                "choices": [{ "delta": { "content": "" } }],
                "usage": { "prompt_tokens": 0, "completion_tokens": 0 },
                "cost": 0
            })
            .to_string(),
        ));
    };
    Sse::new(out)
}

pub(crate) async fn kilo_cloud_sessions() -> impl IntoResponse {
    Json(json!({ "cliSessions": [], "nextCursor": null }))
}

pub(crate) async fn kilo_cloud_session(Path(id): Path<String>) -> impl IntoResponse {
    Json(json!({ "id": id, "messages": [] }))
}

pub(crate) async fn kilo_cloud_import(Json(input): Json<Value>) -> impl IntoResponse {
    Json(json!({
        "id": input.get("sessionId").and_then(Value::as_str).unwrap_or_default(),
        "imported": false
    }))
}

pub(crate) async fn kilocode_import_project(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    import_response(state.store.import_project(input).map(|id| (id, false)))
}

pub(crate) async fn kilocode_import_session(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    import_response(state.store.import_session(input))
}

pub(crate) async fn kilocode_import_message(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    import_response(state.store.import_message(input).map(|id| (id, false)))
}

pub(crate) async fn kilocode_import_part(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    import_response(state.store.import_part(input).map(|id| (id, false)))
}

pub(crate) async fn kilocode_remove_skill(Json(_input): Json<Value>) -> impl IntoResponse {
    Json(true)
}

pub(crate) async fn kilocode_remove_agent(Json(_input): Json<Value>) -> impl IntoResponse {
    Json(true)
}

fn remote_disabled() -> Value {
    json!({ "enabled": false, "connected": false })
}

fn import_response(result: rusqlite::Result<(String, bool)>) -> Response {
    match result {
        Ok((id, skipped)) => {
            let mut body = json!({ "ok": true, "id": id });
            if skipped {
                body["skipped"] = Value::Bool(true);
            }
            Json(body).into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "ok": false,
                "id": "",
                "error": err.to_string()
            })),
        )
            .into_response(),
    }
}
