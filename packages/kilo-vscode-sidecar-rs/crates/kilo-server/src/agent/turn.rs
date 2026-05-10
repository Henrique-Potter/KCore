//! Agent turn loop: claim a runner slot, dispatch the prompt to the right
//! provider, persist the assistant message, publish SSE.
//!
//! Step 6 of the kilo-server module split: the turn-loop entrypoints
//! (`prompt_guarded`, `start_runner`, `prompt_turn`, `ensure_prompt_supported`)
//! moved here from `lib.rs`. The fake-provider runtime (`prompt_fake`) and
//! the OpenAI streaming pipeline (`prompt_openai_stream`) still live in
//! `lib.rs` for now (Steps 7 + 8); this module reaches them via
//! `crate::*` as a transitional seam.
//!
//! No behavior changes from the verbatim cut — only visibility (`pub(crate)`),
//! import paths, and the `agent::run_turn` shim now calls into this module.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
};

use kilo_protocol::{KiloPath, MessageAppendInput, MessageAppendResult, PromptInput};
use kilo_provider::{ChatMessage, ChatToolCall};
use serde_json::{json, Value};

use crate::agent::fake::{fake_abort, fake_error, fake_provider, prompt_fake};
use crate::agent::is_canceled;
use crate::agent::openai_stream::zero_tokens;
use crate::agent::openai_stream::{is_openai_oauth, prompt_instructions, prompt_openai_stream};
use crate::agent::parts::{
    aborted_error, api_error, append_assistant, assistant_error_info, assistant_path, prompt_text,
    provider_error, real_tools, task_tool_part, user_info,
};
use crate::agent::permission::ask_question;
use crate::{
    publish_error, publish_events, publish_idle, publish_status, publish_turn_close,
    publish_turn_open, registry, AppState, RouteError, Runner, RunnerGuard, TurnError,
};

pub(crate) async fn prompt_guarded(
    state: Arc<AppState>,
    id: String,
    input: PromptInput,
) -> Result<MessageAppendResult, TurnError> {
    if state.store.session(&id).is_none() {
        return Err(TurnError::NotFound);
    }
    let text = prompt_text(&input.parts);
    ensure_prompt_supported(&state, &input, &text).map_err(TurnError::Unsupported)?;
    let (input, _) = expand_command(state.as_ref(), input, text).map_err(TurnError::Unsupported)?;
    let guard = start_runner(state, &id)?;
    let result = prompt_turn(&guard.state, &id, input, guard.cancel.clone()).await;
    drop(guard);
    result
}

pub(crate) fn start_runner(state: Arc<AppState>, id: &str) -> Result<RunnerGuard, TurnError> {
    start_runner_with_parent(state, id, None)
}

pub(crate) fn start_runner_with_parent(
    state: Arc<AppState>,
    id: &str,
    parent: Option<String>,
) -> Result<RunnerGuard, TurnError> {
    let cancel = {
        let mut runners = state.runners.lock().unwrap();
        if runners.contains_key(id) {
            return Err(TurnError::Busy);
        }
        let cancel = Arc::new(AtomicBool::new(false));
        runners.insert(
            id.to_string(),
            Runner {
                cancel: cancel.clone(),
                follow_up_break: Arc::new(AtomicBool::new(false)),
                parent,
                abort: std::sync::Mutex::new(None),
                awaiting_plan_followup: Arc::new(AtomicBool::new(false)),
                mid_stream_retries: Arc::new(AtomicU8::new(0)),
            },
        );
        cancel
    };
    Ok(RunnerGuard {
        state,
        id: id.to_string(),
        cancel,
    })
}

pub(crate) async fn prompt_turn(
    state: &Arc<AppState>,
    id: &str,
    input: PromptInput,
    cancel: Arc<AtomicBool>,
) -> Result<MessageAppendResult, TurnError> {
    let mut current = input;
    // Plan-mode follow-up loop. Each pass runs the inner turn once,
    // then — if the assistant just emitted `plan_exit` from the `plan`
    // agent — asks the user via `ask_question` whether to hand off to
    // the implementation agent. On "yes" we synthesize a new user
    // message and re-enter the loop with `agent = "code"`. On "no" or
    // a rejected/dropped question we return the original assistant
    // result. Bun parity: `kilocode/plan-followup.ts::ask`.
    loop {
        let result = run_one_turn(state, id, current.clone(), cancel.clone()).await?;
        match plan_followup_decision(state, id, &result, cancel.clone()).await? {
            PlanFollowup::Stay => return Ok(result),
            PlanFollowup::Continue { next, .. } => {
                current = next;
            }
        }
    }
}

async fn run_one_turn(
    state: &Arc<AppState>,
    id: &str,
    input: PromptInput,
    cancel: Arc<AtomicBool>,
) -> Result<MessageAppendResult, TurnError> {
    let Some(session) = state.store.session(id) else {
        return Err(TurnError::Db(rusqlite::Error::QueryReturnedNoRows));
    };

    let text = prompt_text(&input.parts);
    ensure_prompt_supported(&state, &input, &text).map_err(TurnError::Unsupported)?;
    let (input, text) =
        expand_command(state.as_ref(), input, text).map_err(TurnError::Unsupported)?;
    let paths = state.store.paths();
    let dir = paths.directory.clone();
    let project = session.project_id;
    // Record the agent name so the permission layer can derive its
    // hard-rule veto (Bun parity: `kilocode/session/prompt.ts:60-72`).
    state.set_session_agent(id, input.agent.as_deref());
    publish_turn_open(&state, id);
    publish_status(&state, id, "busy");

    let is_child = state
        .runners
        .lock()
        .unwrap()
        .get(id)
        .and_then(|runner| runner.parent.as_ref())
        .is_some();
    let snapshot =
        if is_child || fake_provider(&input) || fake_abort(&input) || fake_error(&input, &text) {
            None
        } else {
            pre_turn_snapshot(state.store.clone(), project.clone()).await
        };
    let mut info = user_info(&paths, &input);
    // Bun parity: `KiloSessionPromptQueue.scope()` retargeting. If the
    // previous turn broke via `follow_up_break`, the session has a
    // pending anchor — the parent of the broken user message — that
    // this follow-up should adopt so the new user message lands as a
    // sibling of the broken one, not a child of the partial assistant.
    // Empty anchor means "session root" (no parent).
    if let Some(anchor) = state.take_broken_turn_anchor(id) {
        if anchor.is_empty() {
            info.as_object_mut().map(|map| map.remove("parentID"));
        } else {
            info["parentID"] = json!(anchor);
        }
    }
    if let Some(snapshot) = snapshot {
        info["snapshot"] = json!(snapshot);
    }
    let user = state.store.append_message_record(
        id,
        MessageAppendInput {
            info,
            parts: input.parts.clone(),
        },
    )?;
    publish_events(&state, dir.clone(), project.clone(), user.events);
    let mut user = user.result;
    let mut active = input.clone();
    let mut active_text = text.clone();

    if is_canceled(&cancel) || fake_abort(&input) {
        commit_broken_turn_anchor(state, id, &user);
        let assistant = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: assistant_error_info(&paths, &user, &active, aborted_error()),
                parts: Vec::new(),
            },
        )?;
        publish_events(&state, dir, project, assistant.events);
        publish_error(&state, id, assistant.result.info["error"].clone());
        publish_idle(&state, id);
        publish_turn_close(&state, id, "interrupted");

        return Ok(assistant.result);
    }

    if fake_error(&input, &text) {
        let assistant = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: assistant_error_info(&paths, &user, &active, api_error()),
                parts: Vec::new(),
            },
        )?;
        publish_events(&state, dir, project, assistant.events);
        publish_error(&state, id, assistant.result.info["error"].clone());
        publish_idle(&state, id);
        publish_turn_close(&state, id, "error");

        return Ok(assistant.result);
    }

    if let Some(next) = handle_inline_subtasks(
        state,
        id,
        &active,
        &user,
        dir.clone(),
        project.clone(),
        cancel.clone(),
    )
    .await?
    {
        user = next.user;
        active = next.input;
        active_text = next.text;
    }
    if is_canceled(&cancel) {
        commit_broken_turn_anchor(state, id, &user);
        let assistant = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: assistant_error_info(&paths, &user, &active, aborted_error()),
                parts: Vec::new(),
            },
        )?;
        publish_events(&state, dir, project, assistant.events);
        publish_error(&state, id, assistant.result.info["error"].clone());
        publish_idle(&state, id);
        publish_turn_close(&state, id, "interrupted");

        return Ok(assistant.result);
    }

    if fake_provider(&active) {
        // Capture the user's parentage before the value is consumed by
        // `prompt_fake`. If the runtime breaks via `follow_up_break`
        // mid-stream, the queued follow-up turn must re-anchor under
        // this parent (Bun parity: queue scope retargeting).
        let user_parent = user_parent_anchor(&user);
        let result =
            prompt_fake(&state, id, active, user, active_text, dir, project, cancel).await?;
        commit_broken_turn_anchor_for_parent(state, id, &user_parent);
        return Ok(result);
    }

    if is_openai_oauth(&state, active.model.as_ref()) {
        let user_parent = user_parent_anchor(&user);
        let result = prompt_openai_stream(
            state.clone(),
            id,
            active,
            user,
            active_text,
            dir,
            project,
            cancel,
        )
        .await?;
        commit_broken_turn_anchor_for_parent(state, id, &user_parent);
        return Ok(result);
    }

    let out = match kilo_provider::chat_tools_with_auth(
        &state.store.config(),
        &json!(state.store.provider_auths()),
        active.model.as_ref(),
        prompt_instructions(&active, Some(state.store.paths().directory.as_str())),
        vec![ChatMessage {
            role: "user".to_string(),
            content: active_text,
            responses: Vec::new(),
            attachments: Vec::new(),
        }],
        real_tools(&state, &active),
    )
    .await
    {
        Ok(out) => out,
        Err(err) => {
            let assistant = state.store.append_message_record(
                id,
                MessageAppendInput {
                    info: assistant_error_info(&paths, &user, &active, provider_error(err)),
                    parts: Vec::new(),
                },
            )?;
            publish_events(&state, dir, project, assistant.events);
            publish_error(&state, id, assistant.result.info["error"].clone());
            publish_idle(&state, id);
            publish_turn_close(&state, id, "error");

            return Ok(assistant.result);
        }
    };

    Ok(append_assistant(
        &state, id, &active, &user, out, dir, project,
    )?)
}

async fn pre_turn_snapshot(store: kilo_store::Store, project: String) -> Option<String> {
    #[cfg(test)]
    {
        let root = std::path::PathBuf::from(store.paths().worktree);
        if !root.starts_with(std::env::temp_dir()) {
            return None;
        }
    }
    tokio::task::spawn_blocking(move || crate::snapshot::track(&store, &project))
        .await
        .ok()
        .and_then(Result::ok)
}

struct InlineSubtask {
    user: MessageAppendResult,
    input: PromptInput,
    text: String,
}

async fn handle_inline_subtasks(
    state: &Arc<AppState>,
    id: &str,
    input: &PromptInput,
    user: &MessageAppendResult,
    dir: String,
    project: String,
    cancel: Arc<AtomicBool>,
) -> rusqlite::Result<Option<InlineSubtask>> {
    let tasks = input
        .parts
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("subtask"))
        .collect::<Vec<_>>();
    if tasks.is_empty() {
        return Ok(None);
    }

    let mut command = false;
    for (idx, task) in tasks.iter().enumerate() {
        if is_canceled(&cancel) {
            break;
        }
        let agent = task
            .get("agent")
            .and_then(Value::as_str)
            .unwrap_or("general");
        let description = task
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("Subtask");
        let prompt = task
            .get("prompt")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let cmd = task.get("command").and_then(Value::as_str);
        command = command || cmd.is_some();
        let model = task.get("model").cloned().or_else(|| input.model.clone());
        let start = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: inline_assistant_info(
                    user,
                    input,
                    agent,
                    model.as_ref(),
                    &state.store.paths(),
                ),
                parts: Vec::new(),
            },
        )?;
        publish_events(state, dir.clone(), project.clone(), start.events);
        let mid = start.result.info["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let pid = format!("prt_{mid}_task");
        let call = ChatToolCall {
            id: format!("call_{mid}_task_{idx}"),
            name: "task".to_string(),
            input: json!({
                "description": description,
                "prompt": prompt,
                "subagent_type": agent,
                "command": cmd,
            }),
        };
        let part = task_tool_part(
            state,
            id,
            &mid,
            &pid,
            0,
            &call,
            start.result.time,
            cancel.clone(),
            model,
            None,
        )
        .await;
        let cancelled = is_canceled(&cancel);
        let mut info = crate::agent::parts::assistant_completed_info(&start.result);
        info["finish"] = json!("tool-calls");
        let result = state.store.append_message_record(
            id,
            MessageAppendInput {
                info,
                parts: vec![part],
            },
        )?;
        publish_events(state, dir.clone(), project.clone(), result.events);
        if cancelled {
            break;
        }
    }

    if !command {
        return Ok(None);
    }

    let text = "Summarize the task tool output above and continue with your task.".to_string();
    let mut next = input.clone();
    next.parts = vec![json!({
        "type": "text",
        "text": text,
        "synthetic": true,
    })];
    let paths = state.store.paths();
    let record = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: user_info(&paths, &next),
            parts: next.parts.clone(),
        },
    )?;
    publish_events(state, dir, project, record.events);
    Ok(Some(InlineSubtask {
        user: record.result,
        input: next,
        text,
    }))
}

fn inline_assistant_info(
    user: &MessageAppendResult,
    input: &PromptInput,
    agent: &str,
    model: Option<&Value>,
    paths: &KiloPath,
) -> Value {
    let provider = model
        .and_then(|value| value.get("providerID"))
        .or_else(|| {
            input
                .model
                .as_ref()
                .and_then(|value| value.get("providerID"))
        })
        .and_then(Value::as_str)
        .unwrap_or("openai");
    let model = model
        .and_then(|value| value.get("modelID"))
        .or_else(|| input.model.as_ref().and_then(|value| value.get("modelID")))
        .and_then(Value::as_str)
        .unwrap_or("gpt-5.1-codex");
    json!({
        "role": "assistant",
        "parentID": user.info["id"].clone(),
        "providerID": provider,
        "modelID": model,
        "agent": agent,
        "path": assistant_path(paths),
        "cost": 0,
        "tokens": zero_tokens(),
    })
}

pub(crate) fn ensure_prompt_supported(
    state: &AppState,
    input: &PromptInput,
    text: &str,
) -> Result<(), RouteError> {
    if fake_provider(input) || fake_abort(input) || fake_error(input, text) {
        return Ok(());
    }
    if is_openai_oauth(state, input.model.as_ref()) {
        return Ok(());
    }

    Err(RouteError {
        provider: crate::agent::openai_stream::model_provider(input.model.as_ref())
            .unwrap_or("missing")
            .to_string(),
        model: crate::agent::openai_stream::model_name(input.model.as_ref()).map(str::to_string),
        reason: "The Rust sidecar currently supports only fake providers and OpenAI OAuth/Codex prompts. Configure OpenAI OAuth or use the Bun runtime for this provider.",
    })
}

fn expand_command(
    state: &AppState,
    mut input: PromptInput,
    text: String,
) -> Result<(PromptInput, String), RouteError> {
    if !is_openai_oauth(state, input.model.as_ref()) {
        return Ok((input, text));
    }
    let Some((name, rest)) = registry::slash(&text) else {
        return Ok((input, text));
    };
    let paths = state.store.paths();
    let Some(cmd) = registry::command(
        Path::new(&paths.directory),
        Path::new(&paths.config),
        Path::new(&paths.home),
        name,
    ) else {
        return Ok((input, text));
    };
    let text = registry::expand(&cmd.template, rest);
    if cmd.subtask == Some(true) {
        input.parts = vec![json!({
            "type": "subtask",
            "agent": cmd.agent.as_deref().unwrap_or("general"),
            "description": cmd.description.as_deref().unwrap_or(""),
            "command": name,
            "model": input.model.clone().unwrap_or_else(|| json!({})),
            "prompt": text,
            "metadata": {
                "source": "rust-openai-oauth-slash-command",
                "mode": "inline"
            }
        })];
        return Ok((input, text));
    }
    let mut parts = vec![json!({ "type": "text", "text": text })];
    parts.extend(
        input
            .parts
            .iter()
            .filter(|part| part.get("type").and_then(Value::as_str) != Some("text"))
            .cloned(),
    );
    input.parts = parts;
    Ok((input, text))
}

/// Outcome of the plan-followup decision after a single inner turn.
#[allow(clippy::large_enum_variant)]
pub(crate) enum PlanFollowup {
    /// No follow-up needed (or the user declined). The caller returns
    /// the assistant message produced by the just-completed turn.
    Stay,
    /// User accepted the implementation handoff. The caller re-enters
    /// the turn loop with `next` as the synthesized user prompt;
    /// `agent_switch` records which agent the loop is now running for
    /// any downstream telemetry.
    Continue {
        next: PromptInput,
        /// Read by tests only — production callers in `turn.rs:110` ignore
        /// it. Kept on the enum so the migration assertion in
        /// `agent_basics.rs` can verify the routed agent name without
        /// reaching into private state.
        #[allow(dead_code)]
        agent_switch: String,
    },
}

/// Detect a `plan_exit` tool-call on the just-persisted assistant
/// message and, if the message belonged to the `plan` agent, ask the
/// user whether to continue with implementation. Mirrors Bun's
/// `kilocode/plan-followup.ts::ask` flow (`prompt({sessionID})` then
/// branches on the answer).
///
/// Returns `Stay` when:
/// - the assistant agent isn't `plan`,
/// - no completed `plan_exit` part was emitted,
/// - the question gets rejected/dropped, or
/// - the user picks "no".
///
/// Returns `Continue` with a synthesized user prompt when the user
/// picks "yes". The caller is responsible for re-entering the turn
/// loop; this helper does not call back into `prompt_turn` itself
/// to keep the recursion shape obvious.
pub(crate) async fn plan_followup_decision(
    state: &Arc<AppState>,
    sid: &str,
    result: &MessageAppendResult,
    cancel: Arc<AtomicBool>,
) -> Result<PlanFollowup, TurnError> {
    if is_canceled(&cancel) {
        return Ok(PlanFollowup::Stay);
    }
    if !is_plan_agent(result) {
        return Ok(PlanFollowup::Stay);
    }
    let plan_path = match find_completed_plan_exit(result) {
        Some(path) => path,
        None => return Ok(PlanFollowup::Stay),
    };
    // Mark the runner as suspended on a follow-up question so abort/
    // cancel routes can distinguish a paused turn from a producing one.
    set_awaiting_plan_followup(state, sid, true);
    let target_agent = preferred_followup_agent(state);
    let info = plan_followup_question_info(sid, result);
    let answer = ask_question(state, info).await;
    set_awaiting_plan_followup(state, sid, false);
    let yes = match answer {
        Ok(value) => answer_is_yes(&value),
        Err(_) => false,
    };
    if !yes {
        return Ok(PlanFollowup::Stay);
    }
    let prompt_text = followup_prompt_text(plan_path.as_deref());
    let next = synth_followup_input(result, &target_agent, prompt_text);
    Ok(PlanFollowup::Continue {
        next,
        agent_switch: target_agent,
    })
}

fn is_plan_agent(result: &MessageAppendResult) -> bool {
    result.info.get("agent").and_then(Value::as_str) == Some("plan")
}

/// Walk `result.parts` for the latest completed `plan_exit` tool. The
/// tool's metadata carries the planned file path under `state.metadata.plan`
/// (see `agent::parts::real_mutating_tool_part::"plan_exit"`).
fn find_completed_plan_exit(result: &MessageAppendResult) -> Option<Option<String>> {
    for part in result.parts.iter().rev() {
        if part.get("type").and_then(Value::as_str) != Some("tool") {
            continue;
        }
        if part.get("tool").and_then(Value::as_str) != Some("plan_exit") {
            continue;
        }
        let status = part
            .get("state")
            .and_then(|state| state.get("status"))
            .and_then(Value::as_str);
        if status != Some("completed") {
            continue;
        }
        let plan = part
            .get("state")
            .and_then(|state| state.get("metadata"))
            .and_then(|metadata| metadata.get("plan"))
            .and_then(Value::as_str)
            .map(str::to_string);
        return Some(plan);
    }
    None
}

/// Pick the agent the implementation phase should run under. Bun's
/// `resolveCodeModel` defaults to the `code` agent; if that builtin has
/// been disabled by config, fall back to the `general` orchestrator
/// agent which is always present in the catalog.
fn preferred_followup_agent(state: &AppState) -> String {
    if state.agent_info("code").is_some() {
        return "code".to_string();
    }
    if state.agent_info("general").is_some() {
        return "general".to_string();
    }
    "code".to_string()
}

fn plan_followup_question_info(sid: &str, result: &MessageAppendResult) -> Value {
    let mid = result
        .info
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let qid = format!("question_plan_followup_{mid}");
    json!({
        "id": qid,
        "sessionID": sid,
        "status": "pending",
        "questions": [{
            "question": "Continue with implementation?",
            "header": "Implement",
            "options": [
                { "label": "yes", "description": "Implement the plan in this session" },
                { "label": "no", "description": "End this turn without implementing" }
            ],
            "multiple": false,
            "custom": false,
        }],
        "blocking": true,
        "metadata": { "kind": "plan_followup" },
        "text": "Continue with implementation?",
    })
}

fn answer_is_yes(value: &Value) -> bool {
    let pick = first_answer(value);
    matches!(pick.as_deref(), Some(s) if matches!(s.trim().to_ascii_lowercase().as_str(), "yes" | "y" | "continue"))
}

/// Pull the first reply string out of the answers payload that
/// `reply_question` forwards. Accepts both the nested array shape
/// (`[["yes"]]`) and the flat string fallback (`"yes"`).
fn first_answer(value: &Value) -> Option<String> {
    if let Some(s) = value.as_str() {
        return Some(s.to_string());
    }
    if let Some(outer) = value.as_array() {
        for item in outer {
            if let Some(s) = item.as_str() {
                return Some(s.to_string());
            }
            if let Some(inner) = item.as_array() {
                for entry in inner {
                    if let Some(s) = entry.as_str() {
                        return Some(s.to_string());
                    }
                }
            }
        }
    }
    None
}

fn followup_prompt_text(plan_path: Option<&str>) -> String {
    let suffix = match plan_path {
        Some(path) if !path.is_empty() => {
            format!(" The plan is also saved at {path}.")
        }
        _ => String::new(),
    };
    format!(
        "The plan above describes the work. Implement it now.{suffix} \
Use the `code` agent (or default agent) and execute the plan step-by-step, \
asking for permission only when required by the agent's own permission rules."
    )
}

/// Build the synthetic user `PromptInput` for the implementation
/// re-entry. Reuses the original prompt's model so the next turn keeps
/// the same provider routing, but switches the agent and replaces the
/// parts with a single text part marked synthetic.
fn synth_followup_input(
    result: &MessageAppendResult,
    target_agent: &str,
    prompt_text: String,
) -> PromptInput {
    let model = result.info.get("model").cloned();
    PromptInput {
        parts: vec![json!({
            "type": "text",
            "text": prompt_text,
            "synthetic": true,
        })],
        agent: Some(target_agent.to_string()),
        model,
        ..Default::default()
    }
}

fn set_awaiting_plan_followup(state: &AppState, sid: &str, value: bool) {
    if let Some(runner) = state.runners.lock().unwrap().get(sid) {
        runner
            .awaiting_plan_followup
            .store(value, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Read the runner's `follow_up_break` flag without holding the lock
/// across the caller's work.
fn follow_up_break_set(state: &AppState, sid: &str) -> bool {
    state
        .runners
        .lock()
        .unwrap()
        .get(sid)
        .map(|runner| runner.follow_up_break.load(Ordering::SeqCst))
        .unwrap_or(false)
}

/// Read the parentID off a just-persisted user message. Empty string
/// means the message was a session-root child (no parent).
fn user_parent_anchor(user: &MessageAppendResult) -> String {
    user.info
        .get("parentID")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// If the active runner is breaking via `follow_up_break`, persist the
/// broken user message's parentage as the session's pending anchor for
/// the next queued follow-up. Bun parity:
/// `KiloSessionPromptQueue.scope()` retargets the next prompt under
/// `target.base`'s parent rather than the broken assistant.
fn commit_broken_turn_anchor(state: &AppState, sid: &str, user: &MessageAppendResult) {
    if !follow_up_break_set(state, sid) {
        return;
    }
    state.set_broken_turn_anchor(sid, &user_parent_anchor(user));
}

/// Variant of [`commit_broken_turn_anchor`] for call sites that already
/// extracted the parent before passing the user message into a consumer
/// (the fake/oauth pipelines move it by value).
fn commit_broken_turn_anchor_for_parent(state: &AppState, sid: &str, parent: &str) {
    if !follow_up_break_set(state, sid) {
        return;
    }
    state.set_broken_turn_anchor(sid, parent);
}
