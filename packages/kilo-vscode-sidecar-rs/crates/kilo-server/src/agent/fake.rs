//! Fake-provider runtime: deterministic echo+tool path used by the M7
//! oracle harness and the sidebar smoke fixtures.
//!
//! Step 7 of the kilo-server module split: `prompt_fake`, the FakeCall
//! plumbing (`fake_*` predicates / `fake_tool_calls` / `fake_tool_parts`),
//! and the per-call dispatch wrapper (`fake_tool_part` /
//! `fake_tool_completed` / `fake_tool_error`) all moved here.
//!
//! Tool execution is funnelled through [`crate::agent::tools::dispatch`],
//! which is the only surface this module reaches into the `tools` subtree.

use std::{
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use kilo_protocol::{MessageAppendInput, MessageAppendResult, PromptInput};
use kilo_provider::ChatToolCall;
use serde_json::{json, Value};
use tokio::task::JoinSet;

use crate::agent::is_canceled;
use crate::agent::parts::{
    aborted_error, assistant_completed_info, assistant_error_info, assistant_info, task_tool_part,
    tool_completed, tool_error,
};
use crate::agent::permission::ask_permission;
use crate::agent::shape::{repair_tool_name, step_finish_part};
use crate::agent::tools::dispatch_with_cancel;
use crate::{
    publish_error, publish_events, publish_idle, publish_part_delta, publish_turn_close, AppState,
    FakeCall, Repair, KNOWN_TOOLS,
};

pub(crate) async fn prompt_fake(
    state: &Arc<AppState>,
    id: &str,
    input: PromptInput,
    user: MessageAppendResult,
    text: String,
    dir: String,
    project: String,
    cancel: Arc<AtomicBool>,
) -> rusqlite::Result<MessageAppendResult> {
    let paths = state.store.paths();
    let calls = fake_tool_calls(&input);
    let body = if calls.is_empty() {
        format!("Echo: {text}")
    } else {
        let names = calls
            .iter()
            .map(|call| call.tool.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        format!("Completed {} fake tool call(s): {names}", calls.len())
    };
    if let Some(delay) = fake_delay(&input) {
        wait_fake(delay, &cancel).await;
    }
    if is_canceled(&cancel) {
        let assistant = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: assistant_error_info(&paths, &user, &input, aborted_error()),
                parts: Vec::new(),
            },
        )?;
        publish_events(state, dir, project, assistant.events);
        publish_error(state, id, assistant.result.info["error"].clone());
        publish_idle(state, id);
        publish_turn_close(state, id, "interrupted");

        return Ok(assistant.result);
    }
    let start = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: assistant_info(&paths, &user, &input),
            parts: vec![json!({
                "type": "text",
                "text": "",
                "synthetic": true,
            })],
        },
    )?;
    publish_events(&state, dir.clone(), project.clone(), start.events);

    let mid = start.result.info["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let pid = start.result.parts[0]["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();

    publish_part_delta(state, id, &mid, &pid, &body);

    let tools = fake_tool_parts(
        state.clone(),
        id.to_string(),
        mid.clone(),
        pid.clone(),
        calls,
        start.result.time,
        cancel.clone(),
        input.model.clone(),
    )
    .await;
    if is_canceled(&cancel) {
        let assistant = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: assistant_error_info(&paths, &user, &input, aborted_error()),
                parts: Vec::new(),
            },
        )?;
        publish_events(state, dir, project, assistant.events);
        publish_error(state, id, assistant.result.info["error"].clone());
        publish_idle(state, id);
        publish_turn_close(state, id, "interrupted");

        return Ok(assistant.result);
    }
    let mut parts = vec![json!({
        "id": pid,
        "type": "text",
        "text": body,
        "synthetic": true,
    })];
    parts.extend(tools);
    parts.push(step_finish_part(id, &mid, &start.result.parts[0]["id"]));

    let assistant = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: assistant_completed_info(&start.result),
            parts,
        },
    )?;
    publish_events(state, dir, project, assistant.events);
    publish_idle(state, id);
    publish_turn_close(state, id, "completed");

    Ok(assistant.result)
}

pub(crate) fn fake_provider(input: &PromptInput) -> bool {
    fake_flag(input, "fake")
        || fake_flag(input, "fakeProvider")
        || fake_delay(input).is_some()
        || !fake_tool_calls(input).is_empty()
}

pub(crate) fn fake_abort(input: &PromptInput) -> bool {
    fake_flag(input, "fakeAbort")
}

pub(crate) fn fake_error(input: &PromptInput, text: &str) -> bool {
    fake_flag(input, "fakeError") || text.trim() == "__KILO_FAKE_PROVIDER_ERROR__"
}

fn fake_flag(input: &PromptInput, key: &str) -> bool {
    input
        .provider
        .as_ref()
        .and_then(|value| value.get(key))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn fake_delay(input: &PromptInput) -> Option<u64> {
    input
        .provider
        .as_ref()
        .and_then(|value| value.get("fakeDelayMs"))
        .and_then(Value::as_u64)
}

fn fake_tool_calls(input: &PromptInput) -> Vec<FakeCall> {
    input
        .provider
        .as_ref()
        .and_then(|value| value.get("fakeToolCalls"))
        .and_then(Value::as_array)
        .map(|calls| {
            calls
                .iter()
                .filter_map(|call| {
                    let raw = call.get("tool").and_then(Value::as_str)?;
                    let input = call.get("input").cloned().unwrap_or_else(|| json!({}));
                    let delay = call.get("delayMs").and_then(Value::as_u64).unwrap_or(0);
                    Some(match repair_tool_name(raw, KNOWN_TOOLS) {
                        Repair::Valid(tool) => FakeCall {
                            tool,
                            input,
                            delay,
                            invalid: None,
                        },
                        Repair::Invalid(name) => FakeCall {
                            tool: "invalid".to_string(),
                            input,
                            delay,
                            invalid: Some(name),
                        },
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) async fn fake_tool_parts(
    state: Arc<AppState>,
    sid: String,
    mid: String,
    pid: String,
    calls: Vec<FakeCall>,
    time: i64,
    cancel: Arc<AtomicBool>,
    model: Option<Value>,
) -> Vec<Value> {
    let mut set = JoinSet::new();
    for (idx, call) in calls.into_iter().enumerate() {
        let state = state.clone();
        let sid = sid.clone();
        let mid = mid.clone();
        let pid = pid.clone();
        let cancel = cancel.clone();
        let model = model.clone();
        set.spawn(async move {
            wait_fake(call.delay, &cancel).await;
            let part = if is_canceled(&cancel) {
                tool_error(
                    &mid,
                    &pid,
                    idx,
                    &call.tool,
                    &fake_call_id(&pid, idx),
                    &call.input,
                    "Tool call aborted".to_string(),
                    time,
                )
            } else {
                fake_tool_part(&state, &sid, &mid, &pid, idx, &call, time, cancel, model).await
            };
            (idx, part)
        });
    }
    let mut parts = Vec::new();
    while let Some(item) = set.join_next().await {
        if let Ok(item) = item {
            parts.push(item);
        }
    }
    parts.sort_by_key(|(idx, _)| *idx);
    parts.into_iter().map(|(_, part)| part).collect()
}

async fn fake_tool_part(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &FakeCall,
    time: i64,
    cancel: Arc<AtomicBool>,
    model: Option<Value>,
) -> Value {
    if call.tool == "invalid" {
        return tool_error(
            mid,
            pid,
            idx,
            "invalid",
            &fake_call_id(pid, idx),
            &call.input,
            format!(
                "Unknown tool: {}",
                call.invalid.as_deref().unwrap_or("invalid")
            ),
            time,
        );
    }
    if call.tool == "task" {
        let got = ChatToolCall {
            id: fake_call_id(pid, idx),
            name: "task".to_string(),
            input: call.input.clone(),
        };
        return match ask_permission(state, sid, mid, pid, idx, "task", &got.id, &got.input).await {
            Ok(()) => {
                task_tool_part(
                    state,
                    sid,
                    mid,
                    pid,
                    idx,
                    &got,
                    time,
                    cancel,
                    model,
                    Some(json!({ "fake": true })),
                )
                .await
            }
            Err(err) => tool_error(mid, pid, idx, "task", &got.id, &got.input, err, time),
        };
    }
    let root = PathBuf::from(state.store.paths().directory);
    match dispatch_with_cancel(&call.tool, &call.input, &root, Some(&cancel)) {
        Some(Ok((title, output, metadata))) => fake_tool_completed(
            mid,
            pid,
            idx,
            &call.tool,
            &call.input,
            title,
            output,
            metadata,
            time,
        ),
        Some(Err(err)) => fake_tool_error(mid, pid, idx, call, err, time),
        None => tool_error(
            mid,
            pid,
            idx,
            &call.tool,
            &fake_call_id(pid, idx),
            &call.input,
            format!("Unsupported fake tool: {}", call.tool),
            time,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn fake_tool_completed(
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    input: &Value,
    title: String,
    output: String,
    metadata: Value,
    time: i64,
) -> Value {
    tool_completed(
        mid,
        pid,
        idx,
        tool,
        &fake_call_id(pid, idx),
        input,
        title,
        output,
        metadata,
        time,
    )
}

fn fake_tool_error(
    mid: &str,
    pid: &str,
    idx: usize,
    call: &FakeCall,
    err: String,
    time: i64,
) -> Value {
    tool_error(
        mid,
        pid,
        idx,
        &call.tool,
        &fake_call_id(pid, idx),
        &call.input,
        err,
        time,
    )
}

fn fake_call_id(pid: &str, idx: usize) -> String {
    format!("call_{pid}_{idx}")
}

pub(crate) async fn wait_fake(ms: u64, cancel: &AtomicBool) {
    let mut left = ms;
    while left > 0 && !is_canceled(cancel) {
        let next = left.min(10);
        tokio::time::sleep(Duration::from_millis(next)).await;
        left -= next;
    }
}
