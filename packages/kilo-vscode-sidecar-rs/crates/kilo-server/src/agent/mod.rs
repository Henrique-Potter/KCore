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

use crate::{unsupported_provider_error, AppState, TurnError};

pub(crate) mod catalog;
pub(crate) mod compaction;
pub(crate) mod diagnostics;
pub(crate) mod fake;
pub(crate) mod mcp_dispatch;
pub(crate) mod openai_stream;
pub(crate) mod parts;
pub(crate) mod permission;
pub(crate) mod plugin;
pub(crate) mod retry;
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

/// Async variant: validate, enqueue by session, and spawn the turn loop in
/// the background. Same-session follow-ups wait for the current runner instead
/// of surfacing `BusyError`; different sessions still run independently.
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
    let queue = state.prompt_queue(&id);
    let version = state.prompt_queue_version(&id);
    let (abort_tx, abort_rx) = tokio::sync::oneshot::channel::<tokio::task::AbortHandle>();
    let task = tokio::spawn(async move {
        let _slot = queue.lock().await;
        if !state.prompt_queue_current(&id, version) {
            return;
        }
        let guard = loop {
            match turn::start_runner(state.clone(), &id) {
                Ok(guard) => break guard,
                Err(TurnError::Busy) => {
                    state.runner_notify.notified().await;
                    if !state.prompt_queue_current(&id, version) {
                        return;
                    }
                }
                Err(err) => {
                    eprintln!("[kilo-server] prompt_async {id}: {err:?}");
                    return;
                }
            }
        };
        let Ok(handle) = abort_rx.await else {
            return;
        };
        if let Some(runner) = guard.state.runners.lock().unwrap().get(&id) {
            *runner.abort.lock().unwrap() = Some(handle);
        }
        let res = turn::prompt_turn(&guard.state, &id, input, guard.cancel.clone()).await;
        if let Err(err) = res {
            eprintln!("[kilo-server] prompt_async {id}: {err:?}");
        }
        drop(guard);
    });
    // Install the abort handle so `abort_session` can preempt a task
    // suspended on a non-cooperative `.await`. The runner may already be
    // gone by the time the task finishes — guard against that race.
    let _ = abort_tx.send(task.abort_handle());
    StatusCode::NO_CONTENT.into_response()
}
