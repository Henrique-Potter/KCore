//! Route handlers for the prompt entrypoints. The body is intentionally
//! thin — agent orchestration (start_runner, ensure_prompt_supported,
//! prompt_turn) lives in lib.rs and is invoked through the agent::run_turn
//! / agent::run_turn_async shims so the seam is explicit.

use std::{
    collections::BTreeSet,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use kilo_protocol::{CommandInput, PromptInput};
use serde_json::{json, Value};

use crate::{agent, busy_error, internal_error, unsupported_provider_error, AppState, TurnError};

use super::permissions::{dismiss_question_suggestion_waits, reject_pending_for_sessions};

pub(crate) async fn prompt(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<PromptInput>,
) -> Response {
    match agent::run_turn(state, id, input).await {
        Ok(result) => Json(result).into_response(),
        Err(TurnError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(TurnError::Busy) => busy_error(),
        Err(TurnError::Unsupported(err)) => unsupported_provider_error(err),
        Err(TurnError::Db(err)) => internal_error(err.to_string()),
    }
}

pub(crate) async fn command(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<CommandInput>,
) -> Response {
    if input.command.trim().is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let mut parts = vec![json!({
        "type": "text",
        "text": command_text(&input.command, &input.arguments),
    })];
    parts.extend(input.parts);
    let prompt = PromptInput {
        parts,
        agent: input.agent,
        model: input.model.as_deref().map(command_model),
        tools: None,
        message_id: input.message_id,
        system: None,
        format: None,
        variant: input.variant.map(Value::String),
        provider: None,
        editor_context: None,
    };
    match agent::run_turn(state, id, prompt).await {
        Ok(result) => Json(result).into_response(),
        Err(TurnError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(TurnError::Busy) => busy_error(),
        Err(TurnError::Unsupported(err)) => unsupported_provider_error(err),
        Err(TurnError::Db(err)) => internal_error(err.to_string()),
    }
}

pub(crate) async fn prompt_async(
    state: State<Arc<AppState>>,
    id: Path<String>,
    input: Json<PromptInput>,
) -> Response {
    if state.0.store.session(&id.0).is_some() {
        let ids = active_session_family(&state.0, session_family(&state.0, &id.0));
        dismiss_question_suggestion_waits(&state.0, &ids);
        // Mid-loop follow-up break: if a runner is already active for
        // this session, signal it to break at the next safe boundary so
        // the queued follow-up turn picks up the partial assistant
        // message in history. Distinct from `abort_session` — we do NOT
        // bump the prompt-queue version (queue is preserved) and do NOT
        // run the reject-pending cascade on permissions. Bun parity:
        // `kilocode/session/prompt-queue.ts:hasFollowup` short-circuits
        // the active turn when a same-session prompt arrives.
        let runners = state.0.runners.lock().unwrap();
        for sid in &ids {
            if let Some(runner) = runners.get(sid) {
                runner.follow_up_break.store(true, Ordering::SeqCst);
                runner.cancel.store(true, Ordering::SeqCst);
            }
        }
    }
    agent::run_turn_async(state, id, input).await
}

fn command_text(command: &str, args: &str) -> String {
    let args = args.trim();
    if args.is_empty() {
        return format!("/{command}");
    }
    format!("/{command} {args}")
}

fn command_model(model: &str) -> Value {
    let Some((provider, id)) = model.split_once('/') else {
        return Value::String(model.to_string());
    };
    json!({
        "providerID": provider,
        "modelID": id,
    })
}

pub(crate) async fn abort_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    if state.store.session(&id).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let ids = active_session_family(&state, session_family(&state, &id));
    for id in &ids {
        state.cancel_prompt_queue(id);
    }
    {
        let runners = state.runners.lock().unwrap();
        for id in &ids {
            if let Some(runner) = runners.get(id) {
                runner.cancel.store(true, Ordering::SeqCst);
            }
        }
    }
    schedule_hard_abort(state.clone(), ids.clone());
    reject_pending_for_sessions(&state, &ids);
    Json(true).into_response()
}

fn schedule_hard_abort(state: Arc<AppState>, ids: Vec<String>) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let runners = state.runners.lock().unwrap();
        for id in ids {
            let Some(runner) = runners.get(&id) else {
                continue;
            };
            if let Some(abort) = runner.abort.lock().unwrap().as_ref() {
                abort.abort();
            }
        }
    });
}

fn session_family(state: &AppState, root: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut stack = vec![root.to_string()];
    let mut out = Vec::new();
    while let Some(id) = stack.pop() {
        if !seen.insert(id.clone()) {
            continue;
        }
        out.push(id.clone());
        if let Some(children) = state.store.children(&id) {
            stack.extend(children.into_iter().map(|child| child.id));
        }
    }
    out
}

fn active_session_family(state: &AppState, roots: Vec<String>) -> Vec<String> {
    let mut seen = roots.into_iter().collect::<BTreeSet<_>>();
    loop {
        let mut changed = false;
        let runners = state.runners.lock().unwrap();
        for (id, runner) in runners.iter() {
            let id_seen = seen.contains(id);
            let parent_seen = runner
                .parent
                .as_ref()
                .map(|parent| seen.contains(parent))
                .unwrap_or(false);
            if id_seen || parent_seen {
                changed |= seen.insert(id.clone());
                if let Some(parent) = &runner.parent {
                    changed |= seen.insert(parent.clone());
                }
            }
        }
        drop(runners);
        if !changed {
            break;
        }
    }
    seen.into_iter().collect()
}
