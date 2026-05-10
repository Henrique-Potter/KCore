//! Route handlers for /session* CRUD. The /experimental/worktree* handlers
//! and the git-worktree helper graph live in `routes::worktree`.

use std::{
    collections::BTreeMap,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use kilo_protocol::{
    GlobalEvent, MessageAppendInput, PromptInput, SessionCreateInput, SessionForkInput,
    SessionRevertInput, SessionShareInput, SessionUpdateInput, SessionViewedInput,
};
use kilo_store::{SessionMutation, SessionQuery};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::limits::{session_quota_exceeded_error, MAX_SESSIONS_PER_WORKSPACE};
use crate::{
    agent, busy_error, internal_error, publish_events, unsupported_provider_error, AppState,
    TurnError, ViewedState,
};

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

/// Bun's INIT command template. Source of truth:
/// `packages/opencode/src/command/template/initialize.txt`. Embedded
/// rather than read from disk because the template is part of the
/// observable contract — the Rust sidecar runs without an OpenCode
/// install on the path. `${path}` is substituted with the worktree.
const INIT_PROMPT_TEMPLATE: &str =
    include_str!("../../../../../opencode/src/command/template/initialize.txt");

#[derive(Debug, Default, Deserialize)]
pub(crate) struct SessionInitInput {
    #[serde(rename = "providerID", default)]
    pub(crate) provider_id: Option<String>,
    #[serde(rename = "modelID", default)]
    pub(crate) model_id: Option<String>,
    #[serde(rename = "messageID", default)]
    pub(crate) message_id: Option<String>,
}

/// `POST /session/{id}/init` — Bun parity:
/// `packages/opencode/src/server/routes/instance/session.ts:320`.
///
/// Bun dispatches the synthetic `/init` command, which loads the
/// `initialize.txt` template and runs it through the prompt pipeline.
/// We mirror that by building a `PromptInput` whose first text part is
/// the rendered template (`${path}` -> worktree directory) and then
/// dispatching through the same `agent::run_turn` entrypoint that
/// `routes::prompt::prompt` uses, so the response shape is byte-for-byte
/// identical to `POST /session/{id}/message`.
pub(crate) async fn init_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    body: Option<Json<SessionInitInput>>,
) -> Response {
    let input = body.map(|Json(value)| value).unwrap_or_default();
    let directory = state.store.paths().directory;
    let rendered = INIT_PROMPT_TEMPLATE.replace("${path}", &directory);
    let model = match (input.provider_id.as_deref(), input.model_id.as_deref()) {
        (Some(provider), Some(model)) => Some(json!({
            "providerID": provider,
            "modelID": model,
        })),
        _ => None,
    };
    let prompt = PromptInput {
        parts: vec![json!({
            "type": "text",
            "text": rendered,
        })],
        model,
        message_id: input.message_id,
        ..PromptInput::default()
    };
    match agent::run_turn(state, id, prompt).await {
        Ok(result) => Json(result).into_response(),
        Err(TurnError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(TurnError::Busy) => busy_error(),
        Err(TurnError::Unsupported(err)) => unsupported_provider_error(err),
        Err(TurnError::Db(err)) => internal_error(err.to_string()),
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

/// Body for `POST /session/{id}/summarize`. Bun parity:
/// `packages/opencode/src/server/routes/instance/session.ts:556-562`
/// validates `{ providerID, modelID, auto? }`. We accept all three as
/// optional so an empty body still falls back to the default-resolved
/// model (Bun rejects empty body via zod; the Rust port stays lenient
/// to keep CLI ergonomics — a missing provider then fails loudly via
/// `compact_session` -> `MissingProvider`).
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SummarizeBody {
    #[serde(rename = "providerID", default)]
    pub(crate) provider_id: Option<String>,
    #[serde(rename = "modelID", default)]
    pub(crate) model_id: Option<String>,
    /// Bun's `auto` flag distinguishes server-driven summarization from a
    /// user click. When true, we mirror Bun's `compaction.create` +
    /// `prompt.loop({sessionID})` chain — after the summary anchor
    /// lands, fire a fresh background turn so the agent picks up where
    /// the compaction summarized from. The route still returns
    /// promptly; the follow-up runs through the standard SSE pipeline.
    #[serde(default)]
    pub(crate) auto: bool,
}

/// Synthetic prompt used when `summarize` is invoked with `auto: true`
/// and the session has no recoverable user message to replay.
/// Mirrors Bun's `prompt.loop` continuation prompt (the agent picks up
/// from the summary anchor regardless of the literal text).
const AUTO_FOLLOWUP_PROMPT: &str = "Continue with the work.";

/// Pull the most-recent user message text and any persisted
/// `info.provider` hint. Used by the `auto: true` summarize path so the
/// follow-up turn re-uses whatever provider the session was already
/// driving (Bun parity: `prompt.loop` reads the running session state
/// rather than re-resolving). Returns `(text, provider)` — `text`
/// defaults to `AUTO_FOLLOWUP_PROMPT` when no user message exists,
/// `provider` is `None` unless the stored user message carried one.
fn last_user_prompt_hints(state: &AppState, id: &str) -> (String, Option<Value>) {
    let Some(page) = state.store.messages(id, None, None) else {
        return (AUTO_FOLLOWUP_PROMPT.to_string(), None);
    };
    for msg in page.items.iter().rev() {
        if msg.info.get("role").and_then(Value::as_str) != Some("user") {
            continue;
        }
        let text = msg
            .parts
            .iter()
            .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        let text = if text.trim().is_empty() {
            AUTO_FOLLOWUP_PROMPT.to_string()
        } else {
            text
        };
        let provider = msg.info.get("provider").cloned();
        return (text, provider);
    }
    (AUTO_FOLLOWUP_PROMPT.to_string(), None)
}

/// Manually trigger session compaction. Calls
/// [`crate::agent::compaction::compact_session`] which summarizes the
/// transcript via a non-streaming provider call and writes the summary
/// back as the new context anchor. Bun parity:
/// `packages/opencode/src/session/compaction.ts::create` invoked via
/// `Service.summarize` at `prompt.ts:1576`.
///
/// Body: `{ providerID, modelID, auto? }` — all optional; matches the
/// Bun shape (`session.ts:556-562`). When `providerID`+`modelID` are
/// supplied they override the session's default for this summarize call.
/// `auto` distinguishes server-driven summarization from user-clicked.
///
/// Returns `Json(true)` on success, `404` if the session is unknown,
/// `500` with the named error envelope if compaction itself failed.
pub(crate) async fn summarize_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    body: Option<Json<SummarizeBody>>,
) -> Response {
    let body = body.map(|Json(value)| value).unwrap_or_default();
    if state.store.session(&id).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let auths_map = state.store.provider_auths();
    if auths_map.is_empty() {
        // No provider configured — the route stays safe and returns
        // `false` so manual UI triggers don't get a hard 500 just
        // because no auth is set up yet. The agent loop's reactive
        // compaction is unaffected; that path only fires when a real
        // provider call already succeeded enough to hit a
        // context-window error.
        return Json(false).into_response();
    }
    let auths = serde_json::json!(auths_map);
    let cancel = std::sync::atomic::AtomicBool::new(false);
    // Model override: when the body supplies both `providerID` and
    // `modelID` we shape them into the same JSON envelope the provider
    // crate accepts (`{providerID, modelID}`). Otherwise pass `None` so
    // the provider crate falls back to the configured default.
    let model_value = match (body.provider_id.as_deref(), body.model_id.as_deref()) {
        (Some(provider), Some(model)) => Some(json!({
            "providerID": provider,
            "modelID": model,
        })),
        _ => None,
    };
    let outcome = crate::agent::compaction::compact_session(
        &state,
        &id,
        model_value.as_ref(),
        &auths,
        &cancel,
    )
    .await;
    match outcome {
        Ok(_) | Err(crate::agent::compaction::CompactionError::EmptyHistory) => {
            // `EmptyHistory`: nothing to summarize. Bun returns `true`
            // from `prompt.loop({sessionID})` regardless of whether
            // `compact.create` had work to do, so we follow suit.
            if body.auto {
                spawn_auto_followup(&state, &id);
            }
            Json(true).into_response()
        }
        Err(err) => {
            let envelope = crate::agent::compaction::compaction_error_envelope(&err);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(envelope)).into_response()
        }
    }
}

/// Bun parity: when `summarize` is called with `auto: true`, after
/// `compaction.create` lands the summary anchor Bun chains
/// `prompt.loop({sessionID})` to keep the agent driving. The Rust port
/// reproduces that by replaying the last user message text (or a
/// synthesized "Continue" prompt) through `agent::run_turn_async`,
/// which returns immediately and runs the turn loop in the background.
/// The session-id existence check inside `run_turn_async` is a defense
/// against the session being deleted between `summarize_session`'s
/// initial check and this follow-up — we just discard its `Response`
/// because the route already committed to `Json(true)`.
fn spawn_auto_followup(state: &Arc<AppState>, id: &str) {
    let (text, provider) = last_user_prompt_hints(state, id);
    let prompt = PromptInput {
        parts: vec![json!({ "type": "text", "text": text })],
        provider,
        ..PromptInput::default()
    };
    let st = state.clone();
    let sid = id.to_string();
    tokio::spawn(async move {
        let _ = agent::run_turn_async(State(st), Path(sid), Json(prompt)).await;
    });
}

pub(crate) async fn revert_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(mut input): Json<SessionRevertInput>,
) -> Response {
    if let Some(session) = state.store.session(&id) {
        if let Some((mid, snapshot)) = snapshot_target(&state, &id, &input) {
            let store = state.store.clone();
            let project = session.project_id.clone();
            let snapshot_for_revert = snapshot.clone();
            let work = tokio::task::spawn_blocking(move || {
                crate::snapshot::revert(&store, &project, &snapshot_for_revert)
            })
            .await;
            let reverted = match work {
                Ok(Ok(value)) => value,
                Ok(Err(err)) => return internal_error(err.to_string()),
                Err(err) => return internal_error(err.to_string()),
            };
            // Bun-parity structured summary: diff `<base=snapshot>..<head=redo>`
            // and project to `{additions, deletions, files, diffs}` (no `patch`).
            let store = state.store.clone();
            let project = session.project_id.clone();
            let redo = reverted.redo.clone();
            let summary_work = tokio::task::spawn_blocking(move || {
                crate::snapshot::diff_full(&store, &project, &snapshot, &redo)
                    .map(|diff| crate::snapshot::summary_from_diff_full(&diff))
            })
            .await;
            let summary = match summary_work {
                Ok(Ok(value)) => value,
                // Fall back to the lightweight numstat summary captured during
                // the revert call if diff_full fails for any reason — keeps the
                // route from breaking on transient git errors.
                Ok(Err(_)) => reverted.summary,
                Err(err) => return internal_error(err.to_string()),
            };
            let mut revert = input
                .revert
                .take()
                .filter(Value::is_object)
                .unwrap_or_else(|| json!({}));
            revert["messageID"] = json!(mid);
            if let Some(pid) = input.part_id.as_deref() {
                revert["partID"] = json!(pid);
            }
            revert["snapshot"] = json!(reverted.redo);
            revert["diff"] = json!(reverted.diff);
            input.revert = Some(revert);
            input.summary = Some(summary);
        }
    }
    match state.store.set_revert_record(&id, input) {
        Ok(record) => session_mutation_response(&state, record),
        Err(err) => internal_error(err.to_string()),
    }
}

pub(crate) async fn unrevert_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    if let Some(session) = state.store.session(&id) {
        if let Some(snapshot) = session
            .revert
            .as_ref()
            .and_then(|value| value.get("snapshot"))
            .and_then(Value::as_str)
            .map(str::to_string)
        {
            let store = state.store.clone();
            let project = session.project_id.clone();
            let work = tokio::task::spawn_blocking(move || {
                crate::snapshot::restore(&store, &project, &snapshot)
            })
            .await;
            match work {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return internal_error(err.to_string()),
                Err(err) => return internal_error(err.to_string()),
            }
        }
    }
    match state.store.clear_revert_record(&id) {
        Ok(record) => session_mutation_response(&state, record),
        Err(err) => internal_error(err.to_string()),
    }
}

fn snapshot_target(
    state: &AppState,
    id: &str,
    input: &SessionRevertInput,
) -> Option<(String, String)> {
    let target = input.message_id.as_deref().or_else(|| {
        input
            .revert
            .as_ref()
            .and_then(|value| value.get("messageID"))
            .and_then(Value::as_str)
    })?;
    if let Some(snapshot) = state.store.message(id, target).and_then(|message| {
        message
            .info
            .get("snapshot")
            .and_then(Value::as_str)
            .map(str::to_string)
    }) {
        return Some((target.to_string(), snapshot));
    }
    let page = state.store.messages(id, None, None)?;
    let mut prior = None;
    for message in page.items {
        let Some(mid) = message.info.get("id").and_then(Value::as_str) else {
            continue;
        };
        if let Some(snapshot) = message.info.get("snapshot").and_then(Value::as_str) {
            prior = Some((mid.to_string(), snapshot.to_string()));
        }
        if mid == target {
            return prior;
        }
    }
    None
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
    state.cancel_prompt_queue(&id);
    state.remove_prompt_queue(&id);
    state.set_session_agent(&id, None);
    state.broken_turn_anchors.lock().unwrap().remove(&id);
    state.clear_network_waits_for_session(&id);
    match state.store.delete_session_record(&id) {
        Ok(record) if record.session.is_some() => {
            if let (Some(session), Some(event)) = (&record.session, record.event) {
                publish_events(
                    &state,
                    state.store.paths().directory,
                    session.project_id.clone(),
                    [event],
                );
                // Best-effort `git gc --prune=now` on the project's snapshot
                // dir(s). Fire-and-forget — the route returns immediately and
                // the cleanup runs on the blocking pool. See
                // `snapshot::on_session_deleted` for the rationale (snapshot
                // dirs are shared per worktree, so we GC instead of rm).
                let store = state.store.clone();
                let project_for_cleanup = session.project_id.clone();
                let sid_for_cleanup = id.clone();
                tokio::task::spawn_blocking(move || {
                    let _ = crate::snapshot::on_session_deleted(
                        &store,
                        &project_for_cleanup,
                        &sid_for_cleanup,
                    );
                });
                // Best-effort plan-markdown cleanup. The `plan_exit` tool path
                // writes plan files to `<worktree>/.kilo/plans/<created>-<slug>.md`
                // (see `agent::parts::plan_path`). When the session is deleted
                // we remove the matching plan file. The `Session` shape has no
                // `metadata.plan` field today so we always derive the path
                // from `time.created` + `slug`. Path-safety: `resolve_under`
                // rejects `..` traversal and we additionally verify the
                // resolved path lives under `<worktree>/.kilo/plans/`.
                let worktree = PathBuf::from(state.store.paths().directory);
                let plan_rel = format!(".kilo/plans/{}-{}.md", session.time.created, session.slug);
                tokio::task::spawn_blocking(move || {
                    cleanup_plan_file(&worktree, &plan_rel);
                });
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

/// Best-effort `<worktree>/.kilo/plans/*.md` removal. Used by
/// `delete_session` to remove the plan markdown produced by the `plan_exit`
/// tool path when its owning session is deleted. Errors are swallowed by
/// design — this is opportunistic cleanup, not a correctness requirement.
fn cleanup_plan_file(worktree: &FsPath, rel: &str) {
    let Ok(path) = crate::util::paths::resolve_under(worktree, rel) else {
        return;
    };
    let plans_root = worktree.join(".kilo").join("plans");
    if !path.starts_with(&plans_root) {
        return;
    }
    if path.is_file() {
        let _ = std::fs::remove_file(&path);
    }
}
