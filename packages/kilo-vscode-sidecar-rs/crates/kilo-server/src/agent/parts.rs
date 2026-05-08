//! Assistant message + tool part assembly helpers.
//!
//! Step 6 of the kilo-server module split: every helper that builds the
//! Bun-shape `assistant` message info / parts list / tool result envelope
//! lives here. The fake-provider runtime (`prompt_fake`, `fake_*`) and the
//! OpenAI streaming pipeline (`prompt_openai_stream`) still live in
//! `lib.rs` for now (Steps 7 + 8), and reach into this module via
//! `crate::agent::parts::*` for the leaf shape helpers below.
//!
//! No behavior changes from the verbatim cut — only visibility (`pub(crate)`)
//! and import paths.

use std::{
    cell::RefCell,
    future::Future,
    path::Path as FsPath,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, LazyLock,
    },
};

use kilo_protocol::{
    MessageAppendInput, MessageAppendResult, PromptInput, Session, SessionCreateInput,
};
use kilo_provider::{
    ChatMessage, ChatOutput, ChatResponseItem, ChatTool, ChatToolCall, ProviderError,
};
use serde_json::{json, Map, Value};
use tokio::sync::Semaphore;

use crate::agent::permission::ask_permission;
use crate::agent::shape::{repair_tool_name, step_finish_part_usage};
use crate::agent::tools::bash::fake_bash;
use crate::agent::tools::common::{model_toolcall, tool_enabled};
use crate::agent::tools::defs::{
    apply_patch_def, bash_def, edit_def, grep_def, question_def, read_def, task_def, write_def,
};
use crate::agent::tools::fs::{fake_edit, fake_grep, fake_read, fake_write};
use crate::agent::tools::patch::fake_apply_patch;
use crate::{AppState, PermissionRule, Repair, KNOWN_TOOLS};

const TASK_TOOL_CONCURRENCY: usize = 16;
static TASK_TOOL_SLOTS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(TASK_TOOL_CONCURRENCY)));
thread_local! {
    static TASK_TOOL_RUNTIME: RefCell<Option<tokio::runtime::Runtime>> = const { RefCell::new(None) };
}

fn block_on_task_runtime<F, T>(future: F) -> Result<T, String>
where
    F: Future<Output = T>,
{
    TASK_TOOL_RUNTIME.with(|slot| {
        if slot.borrow().is_none() {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| format!("Failed to start task runtime: {err}"))?;
            *slot.borrow_mut() = Some(rt);
        }
        let guard = slot.borrow();
        let rt = guard
            .as_ref()
            .ok_or_else(|| "Task runtime is unavailable".to_string())?;
        Ok(rt.block_on(future))
    })
}

pub(crate) fn max_iterations_error(cap: usize) -> Value {
    json!({
        "name": "MaxIterationsError",
        "data": {
            "message": format!(
                "OpenAI OAuth tool loop exceeded {cap} iterations without a terminal stop"
            )
        }
    })
}

pub(crate) fn append_assistant(
    state: &AppState,
    id: &str,
    input: &PromptInput,
    user: &MessageAppendResult,
    out: ChatOutput,
    dir: String,
    project: String,
) -> rusqlite::Result<MessageAppendResult> {
    let start = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: assistant_provider_info(user, input, &out),
            parts: vec![json!({
                "type": "text",
                "text": "",
            })],
        },
    )?;
    crate::publish_events(state, dir.clone(), project.clone(), start.events);

    let mid = start.result.info["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let pid = start.result.parts[0]["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    crate::publish_part_delta(state, id, &mid, &pid, &out.text);

    let parts = assistant_parts(
        &PathBuf::from(state.store.paths().directory),
        id,
        &mid,
        &pid,
        out,
        start.result.time,
    );

    let assistant = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: assistant_completed_info(&start.result),
            parts,
        },
    )?;
    crate::publish_events(state, dir, project, assistant.events);
    crate::publish_idle(state, id);
    crate::publish_turn_close(state, id, "completed");

    Ok(assistant.result)
}

pub(crate) fn assistant_parts(
    root: &FsPath,
    sid: &str,
    mid: &str,
    pid: &str,
    out: ChatOutput,
    time: i64,
) -> Vec<Value> {
    let mut parts = vec![json!({
        "id": pid,
        "type": "text",
        "text": out.text,
    })];
    parts.extend(real_tool_parts(root, mid, pid, &out.tool_calls, time));
    parts.push(step_finish_part_usage(
        sid,
        mid,
        &parts[0],
        out.usage.as_ref(),
        out.finish.as_deref(),
    ));
    parts
}

pub(crate) fn real_tools(state: &AppState, input: &PromptInput) -> Vec<ChatTool> {
    let enabled = tools_on(input);
    let mut tools = if enabled {
        let mut t = vec![read_def(), grep_def(), task_def(), question_def()];
        if state.mutating_tools_enabled() {
            t.extend([write_def(), edit_def(), apply_patch_def(), bash_def()]);
        }
        t
    } else {
        Vec::new()
    };
    if let Some(t) = structured_output_tool(input) {
        tools.push(t);
    }
    // Connected MCP servers expose namespaced tools (Bun: `mcp/index.ts:685`).
    // Disabled / failed / pending servers contribute nothing.
    if enabled {
        tools.extend(crate::agent::mcp_dispatch::mcp_chat_tools(state));
        // Registered plugin tools (Bun parity: `plugin.tool()` decoration).
        tools.extend(crate::agent::plugin::plugin_chat_tools(state));
    }
    tools
}

/// Synthesize a per-turn `StructuredOutput` tool when the user's
/// `PromptInput.format` is `{ "type": "json_schema", "schema": {...} }`.
/// Mirrors Bun's `prompt.ts:1969-1995`. The model is instructed (via
/// `STRUCTURED_OUTPUT_SYSTEM_PROMPT`) to call this tool with a payload
/// matching the supplied schema instead of replying with raw text.
pub(crate) fn structured_output_tool(input: &PromptInput) -> Option<ChatTool> {
    let format = input.format.as_ref()?;
    if format.get("type").and_then(Value::as_str) != Some("json_schema") {
        return None;
    }
    let schema = format.get("schema").cloned().unwrap_or_else(|| {
        // Tolerate `{ "type": "json_schema", "json_schema": { "schema": ... } }`
        // (the Bun SDK generates this richer shape via `zod-to-json-schema`).
        format
            .pointer("/json_schema/schema")
            .cloned()
            .unwrap_or(serde_json::json!({ "type": "object", "additionalProperties": true }))
    });
    Some(ChatTool {
        name: STRUCTURED_OUTPUT_TOOL_NAME.to_string(),
        description: "Provide the final response in the user-requested structured JSON format. \
                      Call this with a single argument matching the supplied schema."
            .to_string(),
        parameters: schema,
    })
}

/// Name of the synthesized structured-output tool. Public so the OpenAI
/// streaming pipeline can intercept tool calls for this name without
/// dispatching them to the normal tool runners.
pub(crate) const STRUCTURED_OUTPUT_TOOL_NAME: &str = "StructuredOutput";

/// Dispatch an MCP tool call. Permission-gates via the standard
/// `ask_permission` flow (permission name `"mcp"`, pattern = the
/// namespaced tool name). On approval, invokes the tool and converts the
/// MCP `result` payload into a `tool` part. Errors flow through
/// [`tool_error`] like any other tool failure so the loop's
/// `is_permission_denial` / iteration policy keeps working.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn mcp_tool_part(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    descriptor: &crate::agent::mcp_dispatch::McpToolDescriptor,
    call: &ChatToolCall,
    time: i64,
) -> Value {
    use crate::agent::mcp_dispatch::{mcp_invoke, McpInvokeResult};
    use crate::agent::permission::ask_mcp_permission;
    let tool_name = &descriptor.namespaced;
    if let Err(err) =
        ask_mcp_permission(state, sid, mid, pid, idx, tool_name, &call.id, &call.input).await
    {
        return tool_error(mid, pid, idx, tool_name, &call.id, &call.input, err, time);
    }
    match mcp_invoke(state, descriptor, call.input.clone()).await {
        McpInvokeResult::Ok(result) => {
            let output = mcp_result_text(&result);
            let title = format!("{} ({})", descriptor.tool, descriptor.client);
            let metadata = json!({ "result": result, "client": descriptor.client });
            tool_completed(
                mid,
                pid,
                idx,
                tool_name,
                &call.id,
                &call.input,
                title,
                output,
                metadata,
                time,
            )
        }
        McpInvokeResult::Err(err) => {
            tool_error(mid, pid, idx, tool_name, &call.id, &call.input, err, time)
        }
    }
}

/// Run a registered plugin tool. Permission-gates via
/// `ask_plugin_permission`, then delegates to the synchronous handler.
/// Bun parity: `plugin.tool()`-registered tools go through the same
/// `permission.ask` flow as built-ins.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn plugin_tool_part(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &crate::agent::plugin::PluginTool,
    call: &ChatToolCall,
    time: i64,
) -> Value {
    use crate::agent::permission::ask_plugin_permission;
    use crate::agent::plugin::invoke_plugin_tool;
    if let Err(err) =
        ask_plugin_permission(state, sid, mid, pid, idx, &tool.name, &call.id, &call.input).await
    {
        return tool_error(mid, pid, idx, &tool.name, &call.id, &call.input, err, time);
    }
    match invoke_plugin_tool(tool, call) {
        Ok((title, output, metadata)) => tool_completed(
            mid,
            pid,
            idx,
            &tool.name,
            &call.id,
            &call.input,
            title,
            output,
            metadata,
            time,
        ),
        Err(err) => tool_error(mid, pid, idx, &tool.name, &call.id, &call.input, err, time),
    }
}

/// Best-effort text extraction from an MCP `tools/call` result. Per
/// MCP spec, the result has a `content: [{type: "text", text: "..."}]`
/// array; we concatenate the text segments. Falls back to the raw JSON
/// string if no text content is present.
fn mcp_result_text(result: &Value) -> String {
    if let Some(items) = result.get("content").and_then(Value::as_array) {
        let mut text = String::new();
        for item in items {
            if item.get("type").and_then(Value::as_str) != Some("text") {
                continue;
            }
            let Some(value) = item.get("text").and_then(Value::as_str) else {
                continue;
            };
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(value);
        }
        if !text.is_empty() {
            return text;
        }
    }
    serde_json::to_string(result).unwrap_or_default()
}

fn tools_on(input: &PromptInput) -> bool {
    input
        .tools
        .as_ref()
        .map(|value| match value {
            Value::Bool(value) => *value,
            Value::Array(items) => items.iter().any(tool_enabled),
            Value::Object(map) => {
                map.get("read").is_some_and(tool_enabled)
                    || map.get("grep").is_some_and(tool_enabled)
                    || map.get("write").is_some_and(tool_enabled)
                    || map.get("edit").is_some_and(tool_enabled)
                    || map.get("apply_patch").is_some_and(tool_enabled)
                    || map.get("bash").is_some_and(tool_enabled)
                    || map.get("task").is_some_and(tool_enabled)
                    || map.get("mcp").is_some_and(tool_enabled)
                    || map.get("*").is_some_and(tool_enabled)
            }
            _ => false,
        })
        .unwrap_or_else(|| {
            // The extension's `client.session.promptAsync` sends model as
            // `{ providerID, modelID }` only — no `capabilities` blob. The
            // inline check returns false for that shape, which previously
            // produced an empty tools array and the model would respond
            // "I don't have tools to read files."
            //
            // Fix: when capabilities is absent, infer toolcall from the
            // provider id. Every model in our static OpenAI Codex catalog
            // ([`kilo_provider::openai_models`]) sets `toolcall: true`,
            // and the Kilo gateway models do too. This default keeps the
            // explicit `tools: false` opt-out path intact (the outer
            // match arm handles that) while letting the common case work.
            model_toolcall(input.model.as_ref()) || provider_implies_toolcall(input.model.as_ref())
        })
}

/// True when the model's `providerID` belongs to a provider whose
/// catalog uniformly supports tool calling. Used as a fallback when the
/// caller didn't include `capabilities` inline.
fn provider_implies_toolcall(model: Option<&Value>) -> bool {
    let Some(model) = model else {
        return false;
    };
    let provider = model
        .get("providerID")
        .or_else(|| model.get("provider"))
        .or_else(|| model.get("providerId"))
        .and_then(Value::as_str);
    matches!(provider, Some("openai" | "kilo"))
}

pub(crate) fn real_messages(state: &AppState, id: &str, text: &str) -> Vec<ChatMessage> {
    // Summary-anchor compaction (Bun parity, see
    // `agent::compaction::compact_session`): if any persisted message
    // has `info.summary == true`, drop everything BEFORE the latest
    // such anchor. The anchor itself is the new context floor and gets
    // sent to the model as a single user-role primer message — we
    // rewrite the role from `assistant` to `user` so the next turn
    // doesn't think the model already responded.
    let raw = state
        .store
        .messages(id, None, None)
        .map(|page| page.items)
        .unwrap_or_default();
    let summary_anchor = raw
        .iter()
        .rposition(|msg| msg.info.get("summary").and_then(Value::as_bool) == Some(true));
    let mut msgs: Vec<ChatMessage> = Vec::new();
    let start = summary_anchor.unwrap_or(0);
    for (idx, msg) in raw.iter().enumerate().skip(start) {
        let Some(role) = msg.info.get("role").and_then(Value::as_str) else {
            continue;
        };
        if role == "system" {
            continue;
        }
        let content = prompt_text(&msg.parts);
        let calls = tool_part_calls(&msg.parts);
        let outs = msg
            .parts
            .iter()
            .filter_map(tool_part_response)
            .collect::<Vec<_>>();
        // The summary anchor itself becomes a `user` primer — rewrite
        // its role and skip its synthetic tool-call/output history
        // (compaction strips tool I/O before summarizing anyway).
        let is_anchor = Some(idx) == summary_anchor;
        if is_anchor {
            if !content.is_empty() {
                msgs.push(ChatMessage {
                    role: "user".to_string(),
                    content: format!(
                        "[Compacted context summary — earlier history was elided to fit the model's context window.]\n{content}"
                    ),
                    responses: Vec::new(),
                });
            }
            continue;
        }
        if !content.is_empty() || !calls.is_empty() {
            msgs.push(ChatMessage {
                role: role.to_string(),
                content,
                responses: calls,
            });
        }
        if !outs.is_empty() {
            msgs.push(ChatMessage {
                role: "tool".to_string(),
                content: String::new(),
                responses: outs,
            });
        }
    }
    let has_user = msgs
        .iter()
        .any(|msg| msg.role == "user" && msg.content == text);
    if !has_user {
        msgs.push(ChatMessage {
            role: "user".to_string(),
            content: text.to_string(),
            responses: Vec::new(),
        });
    }
    msgs
}

fn tool_part_calls(parts: &[Value]) -> Vec<ChatResponseItem> {
    parts
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("tool"))
        .filter_map(|part| {
            Some(ChatResponseItem::FunctionCall(ChatToolCall {
                id: part.get("callID")?.as_str()?.to_string(),
                name: part.get("tool")?.as_str()?.to_string(),
                input: part.get("state")?.get("input")?.clone(),
            }))
        })
        .collect()
}

pub(crate) fn tool_part_response(part: &Value) -> Option<ChatResponseItem> {
    let id = part.get("callID")?.as_str()?.to_string();
    let state = part.get("state")?;
    let raw = state
        .get("output")
        .and_then(Value::as_str)
        .or_else(|| state.get("error").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    let output = if part.get("tool").and_then(Value::as_str) == Some("invalid")
        && state.get("error").is_some()
    {
        format!("The arguments provided to the tool are invalid: {raw}")
    } else {
        raw
    };
    Some(ChatResponseItem::FunctionOutput { id, output })
}

pub(crate) fn real_tool_parts(
    root: &FsPath,
    mid: &str,
    pid: &str,
    calls: &[ChatToolCall],
    time: i64,
) -> Vec<Value> {
    calls
        .iter()
        .enumerate()
        .map(|(idx, call)| real_safe_tool_part(root, mid, pid, idx, call, time))
        .collect()
}

pub(crate) fn real_safe_tool_part(
    root: &FsPath,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &ChatToolCall,
    time: i64,
) -> Value {
    if call.name == "invalid" {
        return invalid_tool_part(mid, pid, idx, call, time);
    }
    let canonical = match repair_tool_name(&call.name, KNOWN_TOOLS) {
        Repair::Valid(name) => name,
        Repair::Invalid(raw) => {
            return tool_error(
                mid,
                pid,
                idx,
                &call.name,
                &call.id,
                &call.input,
                format!("Unknown tool: {raw}"),
                time,
            );
        }
    };
    match canonical.as_str() {
        "read" => match fake_read(root, &call.input) {
            Ok((title, output, metadata)) => tool_completed(
                mid,
                pid,
                idx,
                &canonical,
                &call.id,
                &call.input,
                title,
                output,
                metadata,
                time,
            ),
            Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
        },
        "grep" => match fake_grep(root, &call.input) {
            Ok((title, output, metadata)) => tool_completed(
                mid,
                pid,
                idx,
                &canonical,
                &call.id,
                &call.input,
                title,
                output,
                metadata,
                time,
            ),
            Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
        },
        _ => tool_error(
            mid,
            pid,
            idx,
            &canonical,
            &call.id,
            &call.input,
            format!("Unsupported tool: {canonical}"),
            time,
        ),
    }
}

/// M7 Fix 5: live tool-name repair. The model frequently emits `Read`,
/// `READ`, `Grep`, etc. instead of the canonical lowercase names. Bun
/// normalizes these via `experimental_repairToolCall`
/// ([`session/llm.ts:363-383`](../../../../../opencode/src/session/llm.ts:363));
/// we mirror the contract by funnelling every live `ChatToolCall` through
/// [`repair_tool_name`] before dispatching. On `Repair::Invalid`, we
/// produce the same Unknown-tool error shape that the fake path emits via
/// `fake_tool_part`'s `"invalid"` arm (see [`tool_error`]).
pub(crate) async fn real_tool_part(
    state: &std::sync::Arc<AppState>,
    root: &FsPath,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &ChatToolCall,
    time: i64,
    cancel: Arc<AtomicBool>,
    model: Option<Value>,
) -> Value {
    if call.name == "invalid" {
        return invalid_tool_part(mid, pid, idx, call, time);
    }
    let canonical = match repair_tool_name(&call.name, KNOWN_TOOLS) {
        Repair::Valid(name) => name,
        Repair::Invalid(raw) => {
            // Before declaring the tool unknown, see if it matches a
            // connected MCP server's namespaced tool catalog. The MCP
            // namespace is `{client}_{tool}` (Bun parity:
            // `mcp/index.ts:685`), reverse-resolved by string match
            // against the live client list — never by string-split.
            if let Some(descriptor) = crate::agent::mcp_dispatch::mcp_lookup(state, &call.name) {
                return mcp_tool_part(state, sid, mid, pid, idx, &descriptor, call, time).await;
            }
            // Then check the plugin registry (Bun parity: `plugin.tool()`).
            if let Some(plugin_tool) = state.plugin_tool_lookup(&call.name) {
                return plugin_tool_part(state, sid, mid, pid, idx, &plugin_tool, call, time).await;
            }
            return tool_error(
                mid,
                pid,
                idx,
                &call.name,
                &call.id,
                &call.input,
                format!("Unknown tool: {raw}"),
                time,
            );
        }
    };
    match canonical.as_str() {
        "read" => match fake_read(root, &call.input) {
            Ok((title, output, metadata)) => tool_completed(
                mid,
                pid,
                idx,
                &canonical,
                &call.id,
                &call.input,
                title,
                output,
                metadata,
                time,
            ),
            Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
        },
        "grep" => match fake_grep(root, &call.input) {
            Ok((title, output, metadata)) => tool_completed(
                mid,
                pid,
                idx,
                &canonical,
                &call.id,
                &call.input,
                title,
                output,
                metadata,
                time,
            ),
            Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
        },
        "write" | "edit" | "apply_patch" | "bash" => {
            match ask_permission(state, sid, mid, pid, idx, &canonical, &call.id, &call.input).await
            {
                Ok(()) => real_mutating_tool_part(root, mid, pid, idx, &canonical, call, time),
                Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
            }
        }
        "task" => {
            match ask_permission(state, sid, mid, pid, idx, &canonical, &call.id, &call.input).await
            {
                Ok(()) => {
                    task_tool_part(state, sid, mid, pid, idx, call, time, cancel, model).await
                }
                Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
            }
        }
        "question" => question_tool_part(state, sid, mid, pid, idx, call, time).await,
        _ => tool_error(
            mid,
            pid,
            idx,
            &canonical,
            &call.id,
            &call.input,
            format!("Unsupported tool: {canonical}"),
            time,
        ),
    }
}

/// Built-in `question` tool: surfaces a UI prompt via [`ask_question`]
/// and waits for the user's reply. The tool result is the raw answer
/// payload; rejection produces a `tool_error` so the loop's standard
/// terminal-on-denial logic kicks in.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn question_tool_part(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &ChatToolCall,
    time: i64,
) -> Value {
    use crate::agent::permission::ask_question;
    let text = call
        .input
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let options = call
        .input
        .get("options")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|label| json!({ "label": label, "description": "" }))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if text.is_empty() {
        return tool_error(
            mid,
            pid,
            idx,
            "question",
            &call.id,
            &call.input,
            "question.text is required".to_string(),
            time,
        );
    }
    let qid = format!("question_{mid}_{pid}_{idx}");
    let info = json!({
        "id": qid,
        "sessionID": sid,
        "status": "pending",
        "questions": [{
            "question": text.clone(),
            "header": "Question",
            "options": options,
            "multiple": false,
            "custom": true,
        }],
        "blocking": true,
        "tool": { "messageID": mid, "callID": call.id.clone() },
        "text": text,
    });
    match ask_question(state, info).await {
        Ok(answers) => {
            let title = "User reply".to_string();
            let output = if answers.is_string() {
                answers.as_str().unwrap_or("").to_string()
            } else {
                serde_json::to_string(&answers).unwrap_or_default()
            };
            let metadata = json!({ "answers": answers });
            tool_completed(
                mid,
                pid,
                idx,
                "question",
                &call.id,
                &call.input,
                title,
                output,
                metadata,
                time,
            )
        }
        Err(err) => tool_error(mid, pid, idx, "question", &call.id, &call.input, err, time),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn task_tool_part(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &ChatToolCall,
    time: i64,
    cancel: Arc<AtomicBool>,
    model: Option<Value>,
) -> Value {
    match execute_task_tool(
        state,
        sid,
        mid,
        pid,
        idx,
        call,
        time,
        &call.input,
        cancel,
        model,
    )
    .await
    {
        Ok((title, output, metadata)) => tool_completed(
            mid,
            pid,
            idx,
            "task",
            &call.id,
            &call.input,
            title,
            output,
            metadata,
            time,
        ),
        Err(err) => tool_error(mid, pid, idx, "task", &call.id, &call.input, err, time),
    }
}

fn validate_task_agent(state: &AppState, agent: &str) -> Result<(), String> {
    if agent == "general" {
        return Ok(());
    }
    let Some(_) = state.agent_config(agent) else {
        return Err(format!(
            "Unknown agent type: {agent} is not a valid agent type"
        ));
    };
    if state.agent_mode(agent).as_deref() == Some("primary") {
        return Err(format!(
            "Agent \"{agent}\" is a primary agent and cannot be used as a subagent"
        ));
    }
    Ok(())
}

fn task_child_permission(state: &AppState, parent: &Session) -> Value {
    let mut rules = vec![PermissionRule {
        permission: "task".to_string(),
        pattern: "*".to_string(),
        action: "deny".to_string(),
    }];
    if let Some(agent) = state.session_agent(&parent.id) {
        rules.extend(
            state
                .agent_permission_rules(&agent)
                .into_iter()
                .filter(task_inherits),
        );
    }
    rules.extend(
        session_permission_rules(parent.permission.as_ref())
            .into_iter()
            .filter(task_inherits),
    );
    permission_rules_value(rules)
}

fn task_inherits(rule: &PermissionRule) -> bool {
    matches!(rule.permission.as_str(), "edit" | "bash" | "mcp")
}

fn session_permission_rules(value: Option<&Value>) -> Vec<PermissionRule> {
    let Some(Value::Object(map)) = value else {
        return Vec::new();
    };
    let mut rules = Vec::new();
    for (permission, value) in map {
        if let Some(action) = value.as_str() {
            rules.push(PermissionRule {
                permission: permission.clone(),
                pattern: "*".to_string(),
                action: action.to_string(),
            });
            continue;
        }
        if let Some(items) = value.as_object() {
            for (pattern, action) in items {
                if let Some(action) = action.as_str() {
                    rules.push(PermissionRule {
                        permission: permission.clone(),
                        pattern: pattern.clone(),
                        action: action.to_string(),
                    });
                }
            }
        }
    }
    rules
}

fn permission_rules_value(rules: Vec<PermissionRule>) -> Value {
    let mut root = Map::new();
    for rule in rules {
        let entry = root
            .entry(rule.permission)
            .or_insert_with(|| Value::Object(Map::new()));
        let Some(map) = entry.as_object_mut() else {
            continue;
        };
        map.insert(rule.pattern, Value::String(rule.action));
    }
    Value::Object(root)
}

async fn execute_task_tool(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &ChatToolCall,
    time: i64,
    input: &Value,
    cancel: Arc<AtomicBool>,
    model: Option<Value>,
) -> Result<(String, String, Value), String> {
    let description = required_str(input, "description")?;
    let prompt = required_str(input, "prompt")?;
    let agent = required_str(input, "subagent_type")?;
    let command = input
        .get("command")
        .and_then(Value::as_str)
        .map(str::to_string);
    let existing = input.get("task_id").and_then(Value::as_str);
    validate_task_agent(state, &agent)?;
    let parent = state
        .store
        .session(sid)
        .ok_or_else(|| format!("Parent session not found: {sid}"))?;
    let permission = task_child_permission(state, &parent);
    let child = match existing.and_then(|id| state.store.session(id)) {
        Some(session) => {
            if session.parent_id.as_deref() != Some(sid) {
                return Err(format!(
                    "Task session {} does not belong to parent session {sid}",
                    session.id
                ));
            }
            session
        }
        None => state
            .store
            .create_session(SessionCreateInput {
                parent_id: Some(sid.to_string()),
                title: Some(format!("{description} (@{agent} subagent)")),
                permission: Some(permission),
                ..Default::default()
            })
            .map_err(|err| format!("Failed to create task session: {err}"))?,
    };
    publish_task_child(state, sid, mid, pid, idx, call, time, &child.id);
    let model = model.unwrap_or_else(|| {
        json!({
            "providerID": "openai",
            "modelID": "gpt-5.1-codex"
        })
    });
    let _slot = TASK_TOOL_SLOTS
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| "Task runner is unavailable".to_string())?;
    // The child prompt future is intentionally non-Send because the
    // agent loop can recursively reach the task tool again. Keep the
    // blocking-thread boundary, but reuse a per-thread current_thread
    // runtime instead of constructing one for every task call.
    let got = state.clone();
    let child_id = child.id.clone();
    let child_prompt = prompt.clone();
    let child_agent = agent.clone();
    let child_model = model.clone();
    let result = tokio::task::spawn_blocking(move || {
        block_on_task_runtime(async move {
            let guard = crate::agent::turn::start_runner(got.clone(), &child_id)
                .map_err(|err| format!("Task session is busy: {err:?}"))?;
            let child_cancel = guard.cancel.clone();
            let bridge = tokio::spawn(async move {
                while !cancel.load(Ordering::SeqCst) && !child_cancel.load(Ordering::SeqCst) {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                if cancel.load(Ordering::SeqCst) {
                    child_cancel.store(true, Ordering::SeqCst);
                }
            });
            let result = crate::agent::turn::prompt_turn(
                &guard.state,
                &child_id,
                PromptInput {
                    parts: vec![json!({ "type": "text", "text": child_prompt })],
                    agent: Some(child_agent),
                    model: Some(child_model),
                    ..Default::default()
                },
                guard.cancel.clone(),
            )
            .await
            .map_err(|err| format!("Task prompt failed: {err:?}"));
            bridge.abort();
            drop(guard);
            result
        })
    })
    .await
    .map_err(|err| format!("Task runtime failed: {err}"))???;
    if let Some(err) = result.info.get("error") {
        return Err(format!("Task failed: {err}"));
    }
    let text = result
        .parts
        .iter()
        .rev()
        .find(|part| part.get("type").and_then(Value::as_str) == Some("text"))
        .and_then(|part| part.get("text"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let output = [
        format!(
            "task_id: {} (for resuming to continue this task if needed)",
            child.id
        ),
        String::new(),
        "<task_result>".to_string(),
        text.to_string(),
        "</task_result>".to_string(),
    ]
    .join("\n");
    let mut metadata = json!({
        "sessionId": child.id,
        "model": model
    });
    if let Some(command) = command {
        metadata["command"] = json!(command);
    }
    Ok((description, output, metadata))
}

#[allow(clippy::too_many_arguments)]
fn publish_task_child(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &ChatToolCall,
    time: i64,
    child: &str,
) {
    let Some(msg) = state.store.message(sid, mid) else {
        return;
    };
    let Some(session) = state.store.session(sid) else {
        return;
    };
    let mut part = tool_running(mid, pid, idx, "task", &call.id, &call.input, time);
    part["state"]["metadata"] = json!({ "sessionId": child });
    if let Ok(record) = state.store.append_message_record(
        sid,
        MessageAppendInput {
            info: msg.info,
            parts: vec![part],
        },
    ) {
        let dir = state.store.paths().directory;
        crate::publish_events(state, dir, session.project_id, record.events);
    }
}

fn required_str(input: &Value, key: &str) -> Result<String, String> {
    input
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("{key} is required"))
}

fn invalid_tool_part(mid: &str, pid: &str, idx: usize, call: &ChatToolCall, time: i64) -> Value {
    let raw = call
        .input
        .get("tool")
        .and_then(Value::as_str)
        .unwrap_or("invalid");
    let err = call
        .input
        .get("error")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| format!("Invalid tool call: {raw}"));
    tool_error(mid, pid, idx, "invalid", &call.id, &call.input, err, time)
}

pub(crate) fn real_mutating_tool_part(
    root: &FsPath,
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &ChatToolCall,
    time: i64,
) -> Value {
    let res = match tool {
        "write" => fake_write(root, &call.input),
        "edit" => fake_edit(root, &call.input),
        "apply_patch" => fake_apply_patch(root, &call.input),
        "bash" => fake_bash(root, &call.input),
        _ => Err(format!("Unsupported tool: {tool}")),
    };
    match res {
        Ok((title, output, metadata)) => tool_completed(
            mid,
            pid,
            idx,
            tool,
            &call.id,
            &call.input,
            title,
            output,
            metadata,
            time,
        ),
        Err(err) => tool_error(mid, pid, idx, tool, &call.id, &call.input, err, time),
    }
}

pub(crate) fn assistant_info(user: &MessageAppendResult, input: &PromptInput) -> Value {
    let agent = input.agent.as_deref().unwrap_or("code");
    json!({
        "role": "assistant",
        "parentID": user.info["id"].clone(),
        "providerID": "local",
        "modelID": "fake-echo",
        "agent": agent,
        "path": {},
        "cost": 0,
        "tokens": {
            "input": 0,
            "output": 0,
            "reasoning": 0,
            "cache": { "read": 0, "write": 0 }
        },
    })
}

pub(crate) fn assistant_provider_info(
    user: &MessageAppendResult,
    input: &PromptInput,
    out: &ChatOutput,
) -> Value {
    let agent = input.agent.as_deref().unwrap_or("code");
    json!({
        "role": "assistant",
        "parentID": user.info["id"].clone(),
        "providerID": out.provider,
        "modelID": out.model,
        "agent": agent,
        "path": {},
        "cost": 0,
        "tokens": {
            "input": 0,
            "output": 0,
            "reasoning": 0,
            "cache": { "read": 0, "write": 0 }
        },
    })
}

pub(crate) fn assistant_completed_info(start: &MessageAppendResult) -> Value {
    let mut info = start.info.clone();
    info["finish"] = json!("stop");
    if let Some(time) = info.get_mut("time").and_then(Value::as_object_mut) {
        time.insert("updated".to_string(), json!(start.time));
        time.insert("completed".to_string(), json!(start.time));
        return info;
    }

    info["time"] = json!({
        "created": start.time,
        "updated": start.time,
        "completed": start.time,
    });
    info
}

pub(crate) fn assistant_error_info(
    user: &MessageAppendResult,
    input: &PromptInput,
    error: Value,
) -> Value {
    let mut info = assistant_info(user, input);
    info["error"] = error;
    info["finish"] = json!("error");
    info
}

pub(crate) fn aborted_error() -> Value {
    json!({
        "name": "MessageAbortedError",
        "data": { "message": "The operation was aborted." }
    })
}

pub(crate) fn api_error() -> Value {
    json!({
        "name": "APIError",
        "data": {
            "message": "Deterministic fake provider error",
            "isRetryable": false,
            "metadata": { "source": "rust-fake-provider" }
        }
    })
}

pub(crate) fn provider_error(err: ProviderError) -> Value {
    json!({
        "name": "APIError",
        "data": {
            "message": err.to_string(),
            "isRetryable": false,
            "metadata": { "source": "rust-provider" }
        }
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn tool_completed(
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &str,
    input: &Value,
    title: String,
    output: String,
    metadata: Value,
    time: i64,
) -> Value {
    let metadata = tool_metadata(tool, input, metadata);
    json!({
        "id": format!("{pid}_{idx}_{tool}"),
        "type": "tool",
        "messageID": mid,
        "callID": call,
        "tool": tool,
        "state": {
            "status": "completed",
            "input": tool_input(input),
            "output": output,
            "metadata": metadata,
            "title": title,
            "time": { "start": time, "end": time }
        },
    })
}

pub(crate) fn tool_running(
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &str,
    input: &Value,
    time: i64,
) -> Value {
    json!({
        "id": format!("{pid}_{idx}_{tool}"),
        "type": "tool",
        "messageID": mid,
        "callID": call,
        "tool": tool,
        "state": {
            "status": "running",
            "input": tool_input(input),
            "time": { "start": time }
        },
    })
}

fn tool_metadata(tool: &str, input: &Value, mut metadata: Value) -> Value {
    let Some(map) = metadata.as_object_mut() else {
        return metadata;
    };
    if let Some(path) = input.get("filePath").cloned() {
        map.entry("file".to_string()).or_insert(path.clone());
        map.entry("path".to_string()).or_insert(path);
    }
    if matches!(tool, "write" | "edit" | "apply_patch" | "bash") {
        map.entry("diff".to_string())
            .or_insert(Value::String(String::new()));
    }
    metadata
}

pub(crate) fn tool_error(
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &str,
    input: &Value,
    err: String,
    time: i64,
) -> Value {
    let error = tool_error_value(&err);
    json!({
        "id": format!("{pid}_{idx}_{tool}"),
        "type": "tool",
        "messageID": mid,
        "callID": call,
        "tool": tool,
        "state": {
            "status": "error",
            "input": tool_input(input),
            "error": err,
            "metadata": { "error": error },
            "time": { "start": time, "end": time }
        },
    })
}

fn tool_error_value(err: &str) -> Value {
    // The bash tool's `shell_unavailable` constant is matched first
    // because exact-match is more reliable than the substring heuristics
    // below — see `agent::tools::bash::SHELL_UNAVAILABLE_ERROR`. The
    // user-visible message expands the constant into a remediation hint.
    let (name, message) = if err == crate::agent::tools::bash::SHELL_UNAVAILABLE_ERROR {
        (
            "shell_unavailable",
            "No usable shell found. Install WSL bash or Git for Windows on this host, or set KILO_BASH_PATH to a bash.exe.",
        )
    } else if err.contains("Unsafe path") || err.contains("Unsafe workdir") {
        ("PathError", err)
    } else if err.contains(" is required") || err.contains("must be") {
        ("ValidationError", err)
    } else if err.contains("matched") || err.contains("was not found") {
        ("EditError", err)
    } else if err.contains("Patch") || err.contains("patch") || err.contains("Malformed") {
        ("PatchError", err)
    } else if err.contains("Permission") {
        ("PermissionRejectedError", err)
    } else {
        ("ToolError", err)
    };
    json!({ "name": name, "data": { "message": message } })
}

pub(crate) fn tool_input(input: &Value) -> Value {
    input
        .as_object()
        .map_or_else(|| json!({}), |map| json!(map))
}

pub(crate) fn prompt_text(parts: &[Value]) -> String {
    let mut text = String::new();
    for part in parts {
        let Some(value) = (match part.get("type").and_then(Value::as_str) {
            Some("text") => part.get("text").and_then(Value::as_str),
            Some("subtask") => part.get("prompt").and_then(Value::as_str),
            _ => None,
        }) else {
            continue;
        };
        if !text.is_empty() {
            text.push('\n');
        }
        text.push_str(value);
    }
    text
}

pub(crate) fn user_info(input: &PromptInput) -> Value {
    let mut info = json!({
        "role": "user",
        "path": {},
    });
    if let Some(id) = input.message_id.as_deref() {
        info["id"] = json!(id);
    }
    if let Some(agent) = input.agent.as_deref() {
        info["agent"] = json!(agent);
    }
    if let Some(value) = input.model.as_ref() {
        info["model"] = value.clone();
    }
    if let Some(value) = input.tools.as_ref() {
        info["tools"] = value.clone();
    }
    if let Some(value) = input.system.as_ref() {
        info["system"] = value.clone();
    }
    if let Some(value) = input.format.as_ref() {
        info["format"] = value.clone();
    }
    if let Some(value) = input.variant.as_ref() {
        info["variant"] = value.clone();
    }
    if let Some(value) = input.editor_context.as_ref() {
        info["editorContext"] = value.clone();
    }
    info
}
