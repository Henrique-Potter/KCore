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
    sync::{atomic::AtomicBool, Arc},
};

use kilo_protocol::{MessageAppendInput, MessageAppendResult, PromptInput};
use kilo_provider::{ChatMessage, ChatToolCall};
use serde_json::{json, Value};

use crate::agent::fake::{fake_abort, fake_error, fake_provider, prompt_fake};
use crate::agent::is_canceled;
use crate::agent::openai_stream::zero_tokens;
use crate::agent::openai_stream::{is_openai_oauth, prompt_instructions, prompt_openai_stream};
use crate::agent::parts::{
    aborted_error, api_error, append_assistant, assistant_error_info, prompt_text, provider_error,
    real_tools, task_tool_part, user_info,
};
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
    let Some(session) = state.store.session(id) else {
        return Err(TurnError::Db(rusqlite::Error::QueryReturnedNoRows));
    };

    let text = prompt_text(&input.parts);
    ensure_prompt_supported(&state, &input, &text).map_err(TurnError::Unsupported)?;
    let (input, text) =
        expand_command(state.as_ref(), input, text).map_err(TurnError::Unsupported)?;
    let dir = state.store.paths().directory;
    let project = session.project_id;
    // Record the agent name so the permission layer can derive its
    // hard-rule veto (Bun parity: `kilocode/session/prompt.ts:60-72`).
    state.set_session_agent(id, input.agent.as_deref());
    publish_turn_open(&state, id);
    publish_status(&state, id, "busy");

    let user = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: user_info(&input),
            parts: input.parts.clone(),
        },
    )?;
    publish_events(&state, dir.clone(), project.clone(), user.events);
    let mut user = user.result;
    let mut active = input.clone();
    let mut active_text = text.clone();

    if is_canceled(&cancel) || fake_abort(&input) {
        let assistant = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: assistant_error_info(&user, &active, aborted_error()),
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
                info: assistant_error_info(&user, &active, api_error()),
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

    if fake_provider(&active) {
        return Ok(prompt_fake(&state, id, active, user, active_text, dir, project, cancel).await?);
    }

    if is_openai_oauth(&state, active.model.as_ref()) {
        return Ok(prompt_openai_stream(
            state.clone(),
            id,
            active,
            user,
            active_text,
            dir,
            project,
            cancel,
        )
        .await?);
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
                    info: assistant_error_info(&user, &active, provider_error(err)),
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
                info: inline_assistant_info(user, input, agent, model.as_ref()),
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
        )
        .await;
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
    let record = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: user_info(&next),
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
        "path": {},
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
    input.parts = vec![json!({ "type": "text", "text": text })];
    Ok((input, text))
}
