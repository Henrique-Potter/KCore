//! Route handlers for /session* CRUD. The /experimental/worktree* handlers
//! and the git-worktree helper graph live in `routes::worktree`.

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use kilo_protocol::{
    GlobalEvent, MessageAppendInput, SessionCreateInput, SessionForkInput, SessionRevertInput,
    SessionShareInput, SessionUpdateInput, SessionViewedInput,
};
use kilo_store::{SessionMutation, SessionQuery};

use crate::limits::{session_quota_exceeded_error, MAX_SESSIONS_PER_WORKSPACE};
use crate::{internal_error, publish_events, AppState, ViewedState};

pub(crate) async fn sessions(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> impl IntoResponse {
    let input = SessionQuery {
        directory: query.get("directory").cloned(),
        roots: query
            .get("roots")
            .is_some_and(|value| value == "true" || value == "1"),
        start: query.get("start").and_then(|value| value.parse().ok()),
        search: query.get("search").cloned(),
        limit: query.get("limit").and_then(|value| value.parse().ok()),
    };

    Json(state.store.sessions(&input))
}

pub(crate) async fn create_session(
    State(state): State<Arc<AppState>>,
    Json(input): Json<SessionCreateInput>,
) -> Response {
    // Per Operational invariants → 5: cap at MAX_SESSIONS_PER_WORKSPACE.
    // Scope is per-workspace (= per-directory). Older sessions should be
    // archived rather than deleted when the cap trips; this slice rejects
    // the new write with a stable error name so the UI can surface a
    // remediation. Archive automation is a M14 follow-up.
    let directory = state.store.paths().directory;
    if state.store.session_count(Some(&directory)) >= MAX_SESSIONS_PER_WORKSPACE {
        return session_quota_exceeded_error();
    }

    match state.store.create_session_record(input) {
        Ok(record) => {
            let dir = state.store.paths().directory;
            publish_events(
                &state,
                dir,
                record.session.project_id.clone(),
                [record.event],
            );
            Json(record.session).into_response()
        }
        Err(err) => internal_error(err.to_string()),
    }
}

pub(crate) async fn viewed(
    State(state): State<Arc<AppState>>,
    Json(input): Json<SessionViewedInput>,
) -> impl IntoResponse {
    set_viewed(&state, input).await;
    Json(true)
}

pub(crate) async fn set_viewed(state: &Arc<AppState>, input: SessionViewedInput) {
    let next = ViewedState {
        focused: input.focused.into_iter().collect(),
        open: input.open.into_iter().collect(),
    };
    *state.viewed.write().await = next;
}

#[cfg(test)]
pub(crate) async fn viewed_snapshot(state: &Arc<AppState>) -> ViewedState {
    state.viewed.read().await.clone()
}

pub(crate) async fn session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.session(&id) {
        Some(item) => Json(item).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub(crate) async fn children(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.children(&id) {
        Some(items) => Json(items).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub(crate) async fn todos(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.store.todos(&id) {
        Some(items) => Json(items).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub(crate) async fn fork_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<SessionForkInput>,
) -> Response {
    match state.store.fork_session_record(&id, input) {
        Ok(Some(record)) => {
            publish_events(
                &state,
                state.store.paths().directory,
                record.session.project_id.clone(),
                record.events,
            );
            Json(record.session).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

pub(crate) async fn diff_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.diff(&id) {
        Some(diff) => Json(diff).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

pub(crate) async fn share_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    body: Option<Json<SessionShareInput>>,
) -> Response {
    match state
        .store
        .set_share_record(&id, body.and_then(|Json(input)| input.url))
    {
        Ok(record) => session_mutation_response(&state, record),
        Err(err) => internal_error(err.to_string()),
    }
}

pub(crate) async fn unshare_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.clear_share_record(&id) {
        Ok(record) => session_mutation_response(&state, record),
        Err(err) => internal_error(err.to_string()),
    }
}

/// Manually trigger session compaction. Calls
/// [`crate::agent::compaction::compact_session`] which summarizes the
/// transcript via a non-streaming provider call and writes the summary
/// back as the new context anchor. Bun parity:
/// `packages/opencode/src/session/compaction.ts::create` invoked via
/// `Service.summarize` at `prompt.ts:1576`.
///
/// Returns the summary text on success, `404` if the session is unknown,
/// `500` with the named error envelope if compaction itself failed.
/// `auths` and `model` are resolved from the running sidecar context —
/// the route does NOT take these as user input, matching the SDK shape.
pub(crate) async fn summarize_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    if state.store.session(&id).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let auths_map = state.store.provider_auths();
    if auths_map.is_empty() {
        // No provider configured — the route stays safe and returns
        // `{ ok: false, reason }` so manual UI triggers don't get a
        // hard 500 just because no auth is set up yet. The agent
        // loop's reactive compaction is unaffected; that path only
        // fires when a real provider call already succeeded enough to
        // hit a context-window error.
        return Json(serde_json::json!({
            "ok": false,
            "reason": "no provider configured for summarization",
        }))
        .into_response();
    }
    let auths = serde_json::json!(auths_map);
    let cancel = std::sync::atomic::AtomicBool::new(false);
    // Model resolution: leave None for now and let the provider crate
    // pick the default from config. Bun's manual summarize allows
    // overriding via body; we follow up later if a UI flow requires it.
    match crate::agent::compaction::compact_session(&state, &id, None, &auths, &cancel).await {
        Ok(text) => Json(serde_json::json!({ "summary": text, "ok": true })).into_response(),
        Err(crate::agent::compaction::CompactionError::EmptyHistory) => {
            Json(serde_json::json!({ "ok": true, "summary": "" })).into_response()
        }
        Err(err) => {
            let envelope = crate::agent::compaction::compaction_error_envelope(&err);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(envelope)).into_response()
        }
    }
}

pub(crate) async fn revert_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<SessionRevertInput>,
) -> Response {
    match state.store.set_revert_record(&id, input) {
        Ok(record) => session_mutation_response(&state, record),
        Err(err) => internal_error(err.to_string()),
    }
}

pub(crate) async fn unrevert_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.clear_revert_record(&id) {
        Ok(record) => session_mutation_response(&state, record),
        Err(err) => internal_error(err.to_string()),
    }
}

/// `PATCH /session/{id}` — Bun shape: body is `{ title?, permission?, time? }`,
/// response is the updated `Session`. Persists `title`, `permission`, and
/// `time.archived`, treating absent fields as a no-op. Emits `session.updated`
/// SSE so the sidebar's listing reflects the change without a full reload.
pub(crate) async fn update_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<SessionUpdateInput>,
) -> Response {
    match state.store.update_session(&id, input) {
        Ok(Some(session)) => {
            crate::http::sse::publish(
                &state,
                GlobalEvent::session(
                    "session.updated",
                    Arc::from(state.store.paths().directory),
                    session.clone(),
                ),
            );
            Json(session).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

pub(crate) async fn delete_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.delete_session_record(&id) {
        Ok(record) if record.session.is_some() => {
            if let (Some(session), Some(event)) = (&record.session, record.event) {
                publish_events(
                    &state,
                    state.store.paths().directory,
                    session.project_id.clone(),
                    [event],
                );
            }
            Json(true).into_response()
        }
        Ok(_) => Json(true).into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

pub(crate) async fn append_message(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<MessageAppendInput>,
) -> Response {
    match state.store.append_message_record(&id, input) {
        Ok(record) => {
            let Some(session) = state.store.session(&id) else {
                return internal_error("missing session after append");
            };
            let dir = state.store.paths().directory;
            publish_events(&state, dir, session.project_id, record.events);
            Json(record.result).into_response()
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

pub(crate) fn session_mutation_response(state: &AppState, record: SessionMutation) -> Response {
    let Some(session) = record.session else {
        return StatusCode::NOT_FOUND.into_response();
    };
    publish_events(
        state,
        state.store.paths().directory,
        session.project_id.clone(),
        record.events,
    );
    Json(session).into_response()
}
