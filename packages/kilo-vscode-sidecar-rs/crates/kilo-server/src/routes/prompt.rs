//! Route handlers for the prompt entrypoints. The body is intentionally
//! thin — agent orchestration (start_runner, ensure_prompt_supported,
//! prompt_turn) lives in lib.rs and is invoked through the agent::run_turn
//! / agent::run_turn_async shims so the seam is explicit.

use std::{
    collections::BTreeSet,
    sync::{atomic::Ordering, Arc},
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use kilo_protocol::PromptInput;

use crate::{agent, busy_error, internal_error, unsupported_provider_error, AppState, TurnError};

use super::permissions::reject_pending_for_sessions;

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

pub(crate) async fn prompt_async(
    state: State<Arc<AppState>>,
    id: Path<String>,
    input: Json<PromptInput>,
) -> Response {
    agent::run_turn_async(state, id, input).await
}

pub(crate) async fn abort_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    if state.store.session(&id).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let ids = session_family(&state, &id);
    {
        let runners = state.runners.lock().unwrap();
        for id in &ids {
            if let Some(runner) = runners.get(id) {
                runner.cancel.store(true, Ordering::SeqCst);
            }
        }
    }
    reject_pending_for_sessions(&state, &ids);
    Json(true).into_response()
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
