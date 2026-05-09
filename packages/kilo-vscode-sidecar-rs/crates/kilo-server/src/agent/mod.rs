//! Agent turn entrypoints. Step 6 lifted the turn loop, prompt-guard,
//! and tool-part assembly out of `lib.rs` into the `turn` and `parts`
//! submodules. Step 7 lifted the fake-provider runtime into `fake` and
//! the per-tool dispatch into `tools`. Step 8 lifted the OpenAI
//! streaming pipeline (`prompt_openai_stream`) and the permission
//! machinery (`ask_permission*`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use kilo_protocol::{MessageAppendResult, PromptInput};

use crate::{busy_error, turn_error, unsupported_provider_error, AppState, TurnError};

pub(crate) mod catalog;
pub(crate) mod compaction;
pub(crate) mod diagnostics;
pub(crate) mod fake;
pub(crate) mod mcp_dispatch;
pub(crate) mod openai_stream;
pub(crate) mod parts;
pub(crate) mod permission;
pub(crate) mod plugin;
pub(crate) mod shape;
pub(crate) mod tools;
pub(crate) mod turn;

/// Single source of truth for "did the runner cancel?". Lives at the
/// agent layer so `fake::prompt_fake`, `turn::prompt_turn`, and
/// `openai_stream::prompt_openai_stream` all observe the same atomic
/// load through one symbol.
pub(crate) fn is_canceled(cancel: &AtomicBool) -> bool {
    cancel.load(Ordering::SeqCst)
}

/// Run the prompt turn synchronously (matching the legacy `prompt_guarded`
/// signature). Wired up so `routes::prompt::prompt` does not need to know
/// about `prompt_guarded` / `prompt_turn` / `prompt_fake` /
/// `prompt_openai_stream`.
pub(crate) async fn run_turn(
    state: Arc<AppState>,
    id: String,
    input: PromptInput,
) -> Result<MessageAppendResult, TurnError> {
    turn::prompt_guarded(state, id, input).await
}

/// Async variant: validate, claim a runner slot, and spawn the turn loop in
/// the background. Mirrors the body `routes::prompt::prompt_async` used to
/// inline; the seam is explicit so the spawned call routes through here.
pub(crate) async fn run_turn_async(
    state: State<Arc<AppState>>,
    id: Path<String>,
    input: Json<PromptInput>,
) -> Response {
    let State(state) = state;
    let Path(id) = id;
    let Json(input) = input;
    if state.store.session(&id).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let text = parts::prompt_text(&input.parts);
    if let Err(err) = turn::ensure_prompt_supported(&state, &input, &text) {
        return unsupported_provider_error(err);
    }
    let guard = match turn::start_runner(state.clone(), &id) {
        Ok(guard) => guard,
        Err(TurnError::Busy) => return busy_error(),
        Err(err) => return turn_error(err),
    };
    let runner_id = guard.id.clone();
    let task = tokio::spawn(async move {
        let id = guard.id.clone();
        let res = turn::prompt_turn(&guard.state, &id, input, guard.cancel.clone()).await;
        if let Err(err) = res {
            eprintln!("[kilo-server] prompt_async {id}: {err:?}");
        }
        drop(guard);
    });
    // Install the abort handle so `abort_session` can preempt a task
    // suspended on a non-cooperative `.await`. The runner may already be
    // gone by the time the task finishes — guard against that race.
    if let Some(runner) = state.runners.lock().unwrap().get(&runner_id) {
        *runner.abort.lock().unwrap() = Some(task.abort_handle());
    }
    StatusCode::NO_CONTENT.into_response()
}
