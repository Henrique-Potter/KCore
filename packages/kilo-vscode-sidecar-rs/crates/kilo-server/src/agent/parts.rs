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
    time::{SystemTime, UNIX_EPOCH},
};

use kilo_protocol::{
    GlobalEvent, KiloPath, MessageAppendInput, MessageAppendResult, PromptInput, Session,
    SessionCreateInput,
};
use kilo_provider::{
    ChatAttachment, ChatMessage, ChatOutput, ChatResponseItem, ChatTool, ChatToolCall,
    ProviderError, ReasoningItem,
};
use serde_json::{json, Map, Value};
use tokio::sync::Semaphore;

use crate::agent::permission::ask_permission;
use crate::agent::shape::{
    repair_tool_name, step_finish_part_usage, tokens_value, usage_cost_value,
};
use crate::agent::tools::common::{model_toolcall, tool_enabled};
use crate::agent::tools::defs::{
    apply_patch_def, bash_def, edit_def, glob_def, grep_def, lsp_def, plan_exit_def, question_def,
    read_def, skill_def, suggest_def, task_def, todowrite_def, webfetch_def, write_def,
};
#[cfg(test)]
use crate::agent::tools::fs::{fake_edit, fake_write};
use crate::agent::tools::fs::{
    fake_edit_gated, fake_glob_gated, fake_grep, fake_grep_gated, fake_read, fake_read_gated,
    fake_write_gated,
};
#[cfg(test)]
use crate::agent::tools::patch::fake_apply_patch;
use crate::agent::tools::patch::fake_apply_patch_gated;
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
        let local = tokio::task::LocalSet::new();
        Ok(rt.block_on(local.run_until(future)))
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
    let has = !out.tool_calls.is_empty();
    let paths = state.store.paths();
    let start = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: assistant_provider_info(&paths, user, input, &out),
            parts: if has {
                Vec::new()
            } else {
                vec![json!({
                    "type": "text",
                    "text": "",
                })]
            },
        },
    )?;
    crate::publish_events(state, dir.clone(), project.clone(), start.events);

    let mid = start.result.info["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let pid = start
        .result
        .parts
        .first()
        .and_then(|part| part["id"].as_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("prt_{mid}"));
    if !has {
        crate::publish_part_delta(state, id, &mid, &pid, &out.text);
    }

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
    let mut parts = real_tool_parts(root, mid, pid, &out.tool_calls, time);
    if parts.is_empty() || !out.text.is_empty() {
        parts.push(assistant_text_part(pid, out.text));
    }
    let anchor = parts
        .first()
        .cloned()
        .unwrap_or_else(|| assistant_text_part(pid, ""));
    parts.push(step_finish_part_usage(
        sid,
        mid,
        &anchor,
        out.usage.as_ref(),
        out.finish.as_deref(),
    ));
    parts
}

fn assistant_text_part(pid: &str, text: impl Into<String>) -> Value {
    json!({
        "id": pid,
        "type": "text",
        "text": text.into(),
    })
}

pub(crate) fn real_tools(state: &AppState, input: &PromptInput) -> Vec<ChatTool> {
    let enabled = tools_on(input);
    let mut tools = if enabled {
        let mut out = vec![
            read_def(),
            glob_def(),
            grep_def(),
            webfetch_def(),
            todowrite_def(),
            skill_def(),
            suggest_def(),
            lsp_def(),
            task_def(),
            question_def(),
        ];
        if input.agent.as_deref() == Some("plan") {
            out.push(plan_exit_def());
        }
        if state.mutating_tools_enabled() {
            out.extend([write_def(), edit_def(), apply_patch_def(), bash_def()]);
        }
        out.retain(|tool| tool_available(input, &tool.name));
        out
    } else {
        Vec::new()
    };
    if let Some(t) = structured_output_tool(input) {
        tools.push(t);
    }
    // Connected MCP servers expose namespaced tools (Bun: `mcp/index.ts:685`).
    // Disabled / failed / pending servers contribute nothing.
    if enabled && tool_available(input, "mcp") {
        tools.extend(crate::agent::mcp_dispatch::mcp_chat_tools(state));
    }
    if enabled && tool_available(input, "plugin") {
        // Registered plugin tools (Bun parity: `plugin.tool()` decoration).
        tools.extend(crate::agent::plugin::plugin_chat_tools(state));
    }
    tools
}

fn tool_available(input: &PromptInput, name: &str) -> bool {
    match input.tools.as_ref() {
        Some(Value::Bool(value)) => *value,
        Some(Value::String(value)) => value == name || value == "*",
        Some(Value::Array(items)) => items.iter().any(|item| match item {
            Value::String(value) => value == name || value == "*",
            value => tool_enabled(value),
        }),
        Some(Value::Object(map)) => map
            .get(name)
            .or_else(|| map.get("*"))
            .map(tool_enabled)
            .unwrap_or(false),
        Some(_) => false,
        None => true,
    }
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
    cancel: &AtomicBool,
) -> Value {
    use crate::agent::mcp_dispatch::{mcp_invoke, McpInvokeResult};
    use crate::agent::permission::ask_mcp_permission;
    let tool_name = &descriptor.namespaced;
    if let Err(err) =
        ask_mcp_permission(state, sid, mid, pid, idx, tool_name, &call.id, &call.input).await
    {
        return tool_error(mid, pid, idx, tool_name, &call.id, &call.input, err, time);
    }
    if is_tool_canceled(cancel) {
        return aborted_tool_part(mid, pid, idx, tool_name, call, time);
    }
    match mcp_invoke(state, descriptor, call.input.clone(), cancel).await {
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
    cancel: &AtomicBool,
) -> Value {
    use crate::agent::permission::ask_plugin_permission;
    use crate::agent::plugin::invoke_plugin_tool;
    if let Err(err) =
        ask_plugin_permission(state, sid, mid, pid, idx, &tool.name, &call.id, &call.input).await
    {
        return tool_error(mid, pid, idx, &tool.name, &call.id, &call.input, err, time);
    }
    if is_tool_canceled(cancel) {
        return aborted_tool_part(mid, pid, idx, &tool.name, call, time);
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
                    || map.get("webfetch").is_some_and(tool_enabled)
                    || map.get("todowrite").is_some_and(tool_enabled)
                    || map.get("skill").is_some_and(tool_enabled)
                    || map.get("suggest").is_some_and(tool_enabled)
                    || map.get("lsp").is_some_and(tool_enabled)
                    || map.get("write").is_some_and(tool_enabled)
                    || map.get("edit").is_some_and(tool_enabled)
                    || map.get("apply_patch").is_some_and(tool_enabled)
                    || map.get("bash").is_some_and(tool_enabled)
                    || map.get("task").is_some_and(tool_enabled)
                    || map.get("plan_exit").is_some_and(tool_enabled)
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
    let last_user = raw
        .iter()
        .enumerate()
        .skip(summary_anchor.unwrap_or(0))
        .filter(|(_, msg)| msg.info.get("role").and_then(Value::as_str) == Some("user"))
        .map(|(idx, _)| idx)
        .last();
    let mut msgs: Vec<ChatMessage> = Vec::new();
    let start = summary_anchor.unwrap_or(0);
    for (idx, msg) in raw.iter().enumerate().skip(start) {
        let Some(role) = msg.info.get("role").and_then(Value::as_str) else {
            continue;
        };
        if role == "system" {
            continue;
        }
        let mut content = prompt_text(&msg.parts);
        if Some(idx) == last_user {
            if let Some(block) = environment_details(msg.info.get("editorContext")) {
                if !content.is_empty() {
                    content.push('\n');
                }
                content.push_str(&block);
            }
            if msg.info.get("agent").and_then(Value::as_str) == Some("plan") {
                if let Some(session) = state.store.session(id) {
                    if !content.is_empty() {
                        content.push_str("\n\n");
                    }
                    content.push_str(&plan_mode_prompt(&state.store.paths().directory, &session));
                }
            }
        }
        // Cross-turn encrypted reasoning replay (Wave 5 Group V): for
        // assistant turns, prepend persisted `reasoning` parts that
        // carry an `itemID` + `encryptedContent` so the next request's
        // `input[]` gets a `{type:"reasoning", id, encrypted_content,
        // summary}` item before any function_call / output items.
        // `responses_input` already emits reasoning entries first when
        // it walks `msg.responses`, so ordering is preserved.
        let mut calls = if role == "assistant" {
            reasoning_part_responses(&msg.parts)
        } else {
            Vec::new()
        };
        calls.extend(tool_part_calls(&msg.parts));
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
                    attachments: Vec::new(),
                });
            }
            continue;
        }
        // For user-role messages, run multimodal resolvers on the parts:
        // turn `file://` URLs and directory references into inline text
        // blocks + image/PDF attachments, and inline `@<path>` mentions
        // from the user's text. Other roles (assistant/tool) only carry
        // already-stored data URL attachments.
        let (prefix, attachments) = if role == "user" {
            let resolved = resolve_user_multimodal(
                FsPath::new(&state.store.paths().directory),
                &msg.parts,
                &content,
            );
            (resolved.prefix, resolved.attachments)
        } else {
            (String::new(), file_attachments(&msg.parts))
        };
        let content = if prefix.is_empty() {
            content
        } else if content.is_empty() {
            prefix
        } else {
            format!("{prefix}\n\n{content}")
        };
        if !content.is_empty() || !calls.is_empty() || !attachments.is_empty() {
            msgs.push(ChatMessage {
                role: role.to_string(),
                content,
                responses: calls,
                attachments,
            });
        }
        if !outs.is_empty() {
            msgs.push(ChatMessage {
                role: "tool".to_string(),
                content: String::new(),
                responses: outs,
                attachments: Vec::new(),
            });
        }
    }
    let has_user = raw.iter().skip(start).any(|msg| {
        msg.info.get("role").and_then(Value::as_str) == Some("user")
            && prompt_text(&msg.parts) == text
    });
    if !has_user {
        msgs.push(ChatMessage {
            role: "user".to_string(),
            content: text.to_string(),
            responses: Vec::new(),
            attachments: Vec::new(),
        });
    }
    msgs
}

fn tool_part_calls(parts: &[Value]) -> Vec<ChatResponseItem> {
    parts
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("tool"))
        .filter(|part| is_settled_tool_part(part))
        .filter_map(|part| {
            Some(ChatResponseItem::FunctionCall(ChatToolCall {
                id: part.get("callID")?.as_str()?.to_string(),
                name: part.get("tool")?.as_str()?.to_string(),
                input: part.get("state")?.get("input")?.clone(),
            }))
        })
        .collect()
}

/// Rebuild encrypted reasoning items from persisted assistant parts so
/// follow-up turns get cache hits on the prior reasoning trace. Each
/// streamed reasoning item lands on disk as one or more parts keyed by
/// `<itemID>:<summary_index>` (see
/// [`agent::openai_stream::reasoning_part_with`]); we group those parts
/// by their bare `itemID`, take the first non-empty `encryptedContent`
/// blob, and concat the per-summary `text` fields in arrival order
/// into the `summary` list. Parts without an `encryptedContent` blob
/// are skipped — those are stale/legacy reasoning records that can't
/// be replayed. Bun parity: `convert-to-openai-responses-input.ts:185-244`.
fn reasoning_part_responses(parts: &[Value]) -> Vec<ChatResponseItem> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: std::collections::HashMap<String, ReasoningItem> =
        std::collections::HashMap::new();
    for part in parts {
        if part.get("type").and_then(Value::as_str) != Some("reasoning") {
            continue;
        }
        let item_id = match part.get("itemID").and_then(Value::as_str) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => continue,
        };
        let encrypted = match part.get("encryptedContent").and_then(Value::as_str) {
            Some(blob) if !blob.is_empty() => blob.to_string(),
            _ => continue,
        };
        let text = part
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string);
        let entry = groups.entry(item_id.clone()).or_insert_with(|| {
            order.push(item_id.clone());
            ReasoningItem {
                id: item_id.clone(),
                encrypted_content: encrypted,
                summary: Vec::new(),
            }
        });
        if let Some(text) = text {
            entry.summary.push(text);
        }
    }
    order
        .into_iter()
        .filter_map(|id| groups.remove(&id).map(ChatResponseItem::Reasoning))
        .collect()
}

pub(crate) fn tool_part_response(part: &Value) -> Option<ChatResponseItem> {
    if !is_settled_tool_part(part) {
        return None;
    }
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

fn is_settled_tool_part(part: &Value) -> bool {
    let Some(state) = part.get("state") else {
        return false;
    };
    match state.get("status").and_then(Value::as_str) {
        Some("completed" | "error") => true,
        Some("pending" | "running") => false,
        Some(_) => false,
        None => state.get("output").is_some() || state.get("error").is_some(),
    }
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
                return mcp_tool_part(state, sid, mid, pid, idx, &descriptor, call, time, &cancel)
                    .await;
            }
            // Then check the plugin registry (Bun parity: `plugin.tool()`).
            if let Some(plugin_tool) = state.plugin_tool_lookup(&call.name) {
                return plugin_tool_part(
                    state,
                    sid,
                    mid,
                    pid,
                    idx,
                    &plugin_tool,
                    call,
                    time,
                    &cancel,
                )
                .await;
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
    if is_tool_canceled(&cancel) {
        return aborted_tool_part(mid, pid, idx, &canonical, call, time);
    }
    match canonical.as_str() {
        "read" => {
            match fake_read_gated(state, sid, mid, pid, idx, root, &call.input, Some(&cancel)).await
            {
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
            }
        }
        "glob" => {
            match fake_glob_gated(state, sid, mid, pid, idx, root, &call.input, Some(&cancel)).await
            {
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
            }
        }
        "grep" => {
            match fake_grep_gated(state, sid, mid, pid, idx, root, &call.input, Some(&cancel)).await
            {
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
            }
        }
        "webfetch" => {
            match ask_permission(state, sid, mid, pid, idx, &canonical, &call.id, &call.input).await
            {
                Ok(()) => {
                    if is_tool_canceled(&cancel) {
                        aborted_tool_part(mid, pid, idx, &canonical, call, time)
                    } else {
                        match crate::agent::tools::webfetch::fake_webfetch_cancel(
                            &call.input,
                            Some(&cancel),
                        )
                        .await
                        {
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
                            Err(err) => tool_error(
                                mid,
                                pid,
                                idx,
                                &canonical,
                                &call.id,
                                &call.input,
                                err,
                                time,
                            ),
                        }
                    }
                }
                Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
            }
        }
        "todowrite" => {
            match ask_permission(state, sid, mid, pid, idx, &canonical, &call.id, &call.input).await
            {
                Ok(()) => {
                    if is_tool_canceled(&cancel) {
                        aborted_tool_part(mid, pid, idx, &canonical, call, time)
                    } else {
                        match todowrite_tool_part(state, sid, mid, pid, idx, call, time) {
                            Ok(part) => part,
                            Err(err) => tool_error(
                                mid,
                                pid,
                                idx,
                                &canonical,
                                &call.id,
                                &call.input,
                                err,
                                time,
                            ),
                        }
                    }
                }
                Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
            }
        }
        "skill" => {
            match ask_permission(state, sid, mid, pid, idx, &canonical, &call.id, &call.input).await
            {
                Ok(()) => {
                    if is_tool_canceled(&cancel) {
                        aborted_tool_part(mid, pid, idx, &canonical, call, time)
                    } else {
                        match skill_tool_part(state, mid, pid, idx, call, time) {
                            Ok(part) => part,
                            Err(err) => tool_error(
                                mid,
                                pid,
                                idx,
                                &canonical,
                                &call.id,
                                &call.input,
                                err,
                                time,
                            ),
                        }
                    }
                }
                Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
            }
        }
        "suggest" => suggest_tool_part(state, sid, mid, pid, idx, call, time).await,
        "lsp" => {
            match ask_permission(state, sid, mid, pid, idx, &canonical, &call.id, &call.input).await
            {
                Ok(()) => {
                    if is_tool_canceled(&cancel) {
                        aborted_tool_part(mid, pid, idx, &canonical, call, time)
                    } else {
                        match lsp_tool_part(root, mid, pid, idx, call, time) {
                            Ok(part) => part,
                            Err(err) => tool_error(
                                mid,
                                pid,
                                idx,
                                &canonical,
                                &call.id,
                                &call.input,
                                err,
                                time,
                            ),
                        }
                    }
                }
                Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
            }
        }
        "write" | "edit" | "apply_patch" | "bash" => {
            match ask_permission(state, sid, mid, pid, idx, &canonical, &call.id, &call.input).await
            {
                Ok(()) => {
                    if is_tool_canceled(&cancel) {
                        aborted_tool_part(mid, pid, idx, &canonical, call, time)
                    } else {
                        real_mutating_tool_part_gated(
                            state, sid, root, mid, pid, idx, &canonical, call, time, &cancel,
                        )
                        .await
                    }
                }
                Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
            }
        }
        "task" => {
            match ask_permission(state, sid, mid, pid, idx, &canonical, &call.id, &call.input).await
            {
                Ok(()) => {
                    if is_tool_canceled(&cancel) {
                        aborted_tool_part(mid, pid, idx, &canonical, call, time)
                    } else {
                        task_tool_part(state, sid, mid, pid, idx, call, time, cancel, model, None)
                            .await
                    }
                }
                Err(err) => tool_error(mid, pid, idx, &canonical, &call.id, &call.input, err, time),
            }
        }
        "question" => question_tool_part(state, sid, mid, pid, idx, call, time).await,
        "plan_exit" => {
            let plan = state
                .store
                .session(sid)
                .map(|session| plan_path(root, &session).to_string_lossy().to_string());
            let output = plan
                .as_ref()
                .map(|plan| format!("Plan is ready at {plan}. Ending planning turn."))
                .unwrap_or_else(|| "Plan is ready. Ending planning turn.".to_string());
            tool_completed(
                mid,
                pid,
                idx,
                "plan_exit",
                &call.id,
                &call.input,
                "Planning complete".to_string(),
                output,
                json!({ "plan": plan }),
                time,
            )
        }
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

fn is_tool_canceled(cancel: &AtomicBool) -> bool {
    crate::agent::is_canceled(cancel)
}

fn aborted_tool_part(
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &ChatToolCall,
    time: i64,
) -> Value {
    tool_error(
        mid,
        pid,
        idx,
        tool,
        &call.id,
        &call.input,
        "Tool call aborted".to_string(),
        time,
    )
}

fn lsp_tool_part(
    root: &FsPath,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &ChatToolCall,
    time: i64,
) -> Result<Value, String> {
    let operation = call
        .input
        .get("operation")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "operation is required".to_string())?;
    let path = call
        .input
        .get("filePath")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "filePath is required".to_string())?;
    let line = tool_position(&call.input, "line")?;
    let character = tool_position(&call.input, "character")?;
    let target = crate::util::paths::resolve_under(root, path)
        .map_err(|_| format!("Unsafe path: {path}"))?;
    if !target.exists() {
        return Err(format!("File not found: {}", target.to_string_lossy()));
    }
    let rel = crate::util::paths::slash(target.strip_prefix(root).unwrap_or(&target));
    let title = format!("{operation} {rel}:{line}:{character}");
    let result = match operation {
        "documentSymbol" => {
            let mut out = Vec::new();
            crate::routes::files::collect_symbols(root, &target, "", 200, &mut out);
            out
        }
        "workspaceSymbol" => crate::routes::files::search_symbols(root, "", 200),
        "goToDefinition"
        | "findReferences"
        | "hover"
        | "goToImplementation"
        | "prepareCallHierarchy"
        | "incomingCalls"
        | "outgoingCalls" => return Err("No LSP server available for this file type.".to_string()),
        _ => return Err(format!("Unsupported LSP operation: {operation}")),
    };
    let output = if result.is_empty() {
        format!("No results found for {operation}")
    } else {
        serde_json::to_string_pretty(&result).unwrap_or_else(|_| "[]".to_string())
    };
    Ok(tool_completed(
        mid,
        pid,
        idx,
        "lsp",
        &call.id,
        &call.input,
        title,
        output,
        json!({ "result": result }),
        time,
    ))
}

fn tool_position(input: &Value, key: &str) -> Result<usize, String> {
    let Some(value) = input.get(key).and_then(Value::as_u64) else {
        return Err(format!("{key} must be greater than or equal to 1"));
    };
    if value == 0 {
        return Err(format!("{key} must be greater than or equal to 1"));
    }
    Ok(value as usize)
}

fn plan_mode_prompt(root: &str, session: &Session) -> String {
    let plan = plan_path(FsPath::new(root), session);
    format!(
        "Plan mode is active. Do not execute the implementation yet. You may read files, inspect the codebase, ask clarifying questions, and write or edit only this plan file: {}.\nAt the end of planning, call plan_exit.",
        plan.to_string_lossy()
    )
}

fn plan_path(root: &FsPath, session: &Session) -> PathBuf {
    root.join(".kilo")
        .join("plans")
        .join(format!("{}-{}.md", session.time.created, session.slug))
}

async fn suggest_tool_part(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &ChatToolCall,
    time: i64,
) -> Value {
    let text = match call.input.get("suggest").and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => value.to_string(),
        _ => {
            return tool_error(
                mid,
                pid,
                idx,
                "suggest",
                &call.id,
                &call.input,
                "suggest is required".to_string(),
                time,
            );
        }
    };
    let actions = match validate_suggestion_actions(&call.input) {
        Ok(value) => value,
        Err(err) => {
            return tool_error(mid, pid, idx, "suggest", &call.id, &call.input, err, time);
        }
    };
    let id = format!("suggestion_{mid}_{pid}_{idx}");
    let (tx, rx) = tokio::sync::oneshot::channel();
    let info = json!({
        "id": id,
        "sessionID": sid,
        "text": text,
        "actions": actions,
        "blocking": false,
        "tool": {
            "messageID": mid,
            "callID": call.id,
        },
    });
    state.suggestions.lock().unwrap().insert(
        id.clone(),
        crate::PendingSuggestion {
            info: info.clone(),
            reply: tx,
        },
    );
    crate::http::sse::publish(state, GlobalEvent::bus("suggestion.shown", info.clone()));
    crate::publish_idle(state, sid);

    match rx.await {
        Ok(crate::SuggestionDecision::Dismiss) => tool_completed(
            mid,
            pid,
            idx,
            "suggest",
            &call.id,
            &call.input,
            "Suggestion dismissed".to_string(),
            "User dismissed the suggestion.".to_string(),
            json!({ "dismissed": true, "truncated": false }),
            time,
        ),
        Ok(crate::SuggestionDecision::Accept(index)) => {
            crate::publish_status(state, sid, "busy");
            let Some(action) = info
                .get("actions")
                .and_then(Value::as_array)
                .and_then(|items| items.get(index))
                .cloned()
            else {
                return tool_error(
                    mid,
                    pid,
                    idx,
                    "suggest",
                    &call.id,
                    &call.input,
                    format!("Invalid action index: {index}"),
                    time,
                );
            };
            let label = action
                .get("label")
                .and_then(Value::as_str)
                .unwrap_or("Suggestion");
            let prompt = action.get("prompt").and_then(Value::as_str).unwrap_or("");
            let resolved = resolve_suggestion_prompt(state, prompt);
            tool_completed(
                mid,
                pid,
                idx,
                "suggest",
                &call.id,
                &call.input,
                format!("User accepted: {label}"),
                format!(
                    "User accepted the suggestion \"{label}\". Carry out the following request now:\n\n{resolved}"
                ),
                json!({ "accepted": action, "dismissed": false, "truncated": false }),
                time,
            )
        }
        Err(_) => tool_error(
            mid,
            pid,
            idx,
            "suggest",
            &call.id,
            &call.input,
            "Suggestion was cancelled".to_string(),
            time,
        ),
    }
}

fn validate_suggestion_actions(input: &Value) -> Result<Vec<Value>, String> {
    let Some(items) = input.get("actions").and_then(Value::as_array) else {
        return Err("actions is required".to_string());
    };
    if items.is_empty() || items.len() > 2 {
        return Err("actions must contain 1 or 2 items".to_string());
    }
    items
        .iter()
        .map(|item| {
            let label = item
                .get("label")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "action.label is required".to_string())?;
            let prompt = item
                .get("prompt")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "action.prompt is required".to_string())?;
            let mut action = json!({ "label": label, "prompt": prompt });
            if let Some(description) = item.get("description").and_then(Value::as_str) {
                action["description"] = json!(description);
            }
            Ok(action)
        })
        .collect()
}

fn resolve_suggestion_prompt(state: &AppState, prompt: &str) -> String {
    let Some((name, args)) = crate::registry::slash(prompt) else {
        return prompt.to_string();
    };
    let paths = state.store.paths();
    let root = FsPath::new(&paths.directory);
    let config = FsPath::new(&paths.config);
    let home = FsPath::new(&paths.home);
    let Some(cmd) = crate::registry::command(root, config, home, name) else {
        return prompt.to_string();
    };
    let out = crate::registry::expand(&cmd.template, args);
    if out.is_empty() {
        prompt.to_string()
    } else {
        out
    }
}

fn skill_tool_part(
    state: &Arc<AppState>,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &ChatToolCall,
    time: i64,
) -> Result<Value, String> {
    let name = call
        .input
        .get("name")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "name is required".to_string())?;
    let paths = state.store.paths();
    let root = FsPath::new(&paths.directory);
    let config = FsPath::new(&paths.config);
    let home = FsPath::new(&paths.home);
    let list = crate::registry::skills(root, config, home);
    let info = list.iter().find(|item| item.name == name).ok_or_else(|| {
        let available = list
            .iter()
            .map(|item| item.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "Skill \"{name}\" not found. Available skills: {}",
            if available.is_empty() {
                "none".to_string()
            } else {
                available
            }
        )
    })?;
    let dir = FsPath::new(&info.location)
        .parent()
        .ok_or_else(|| "skill has no parent directory".to_string())?;
    let files = sample_skill_files(dir);
    let base = file_url(dir);
    let output = [
        format!("<skill_content name=\"{}\">", info.name),
        format!("# Skill: {}", info.name),
        String::new(),
        info.content.trim().to_string(),
        String::new(),
        format!("Base directory for this skill: {base}"),
        "Relative paths in this skill (e.g., scripts/, reference/) are relative to this base directory.".to_string(),
        "Note: file list is sampled.".to_string(),
        String::new(),
        "<skill_files>".to_string(),
        files,
        "</skill_files>".to_string(),
        "</skill_content>".to_string(),
    ]
    .join("\n");
    Ok(tool_completed(
        mid,
        pid,
        idx,
        "skill",
        &call.id,
        &call.input,
        format!("Loaded skill: {}", info.name),
        output,
        json!({ "name": info.name, "dir": dir.to_string_lossy() }),
        time,
    ))
}

fn sample_skill_files(dir: &FsPath) -> String {
    let mut todo = vec![dir.to_path_buf()];
    let mut files = Vec::new();
    while let Some(path) = todo.pop() {
        let Ok(items) = std::fs::read_dir(path) else {
            continue;
        };
        let mut items = items.flatten().map(|item| item.path()).collect::<Vec<_>>();
        items.sort();
        for item in items.into_iter().rev() {
            if files.len() >= 10 {
                break;
            }
            let Ok(meta) = std::fs::symlink_metadata(&item) else {
                continue;
            };
            if meta.is_dir() {
                todo.push(item);
                continue;
            }
            if item.file_name().and_then(|value| value.to_str()) == Some("SKILL.md") {
                continue;
            }
            files.push(format!("<file>{}</file>", item.to_string_lossy()));
        }
        if files.len() >= 10 {
            break;
        }
    }
    files.join("\n")
}

fn file_url(path: &FsPath) -> String {
    let value = path.to_string_lossy().replace('\\', "/");
    if value.starts_with('/') {
        format!("file://{value}")
    } else {
        format!("file:///{value}")
    }
}

fn todowrite_tool_part(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &ChatToolCall,
    time: i64,
) -> Result<Value, String> {
    let todos = validate_todos(&call.input)?;
    let saved = state
        .store
        .update_todos(sid, &todos)
        .map_err(|err| format!("Failed to update todos: {err}"))?
        .ok_or_else(|| format!("Session not found: {sid}"))?;
    crate::http::sse::publish(
        state,
        GlobalEvent::bus("todo.updated", json!({ "sessionID": sid, "todos": saved })),
    );
    let active = saved
        .iter()
        .filter(|todo| todo.get("status").and_then(Value::as_str) != Some("completed"))
        .count();
    Ok(tool_completed(
        mid,
        pid,
        idx,
        "todowrite",
        &call.id,
        &call.input,
        format!("{active} todos"),
        serde_json::to_string_pretty(&saved).unwrap_or_else(|_| "[]".to_string()),
        json!({ "todos": saved }),
        time,
    ))
}

fn validate_todos(input: &Value) -> Result<Vec<Value>, String> {
    let Some(items) = input.get("todos").and_then(Value::as_array) else {
        return Err("todos is required".to_string());
    };
    items
        .iter()
        .map(|item| {
            let content = required_todo_str(item, "content")?;
            let status = required_todo_str(item, "status")?;
            let priority = required_todo_str(item, "priority")?;
            Ok(json!({
                "content": content,
                "status": status,
                "priority": priority,
            }))
        })
        .collect()
}

fn required_todo_str(input: &Value, key: &str) -> Result<String, String> {
    input
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| format!("todo.{key} is required"))
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
    provider: Option<Value>,
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
        provider,
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
    let Some(info) = state.agent_info(agent) else {
        return Err(format!(
            "Unknown agent type: {agent} is not a valid agent type"
        ));
    };
    if info.get("mode").and_then(Value::as_str) == Some("primary") {
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
        || is_mcp_tool_permission(&rule.permission)
}

fn is_mcp_tool_permission(permission: &str) -> bool {
    permission.ends_with("_*") || permission.contains('_')
}

pub(crate) fn task_child_tools(state: &AppState, parent: &Session) -> Value {
    let mut out = json!({
        "read": true,
        "glob": true,
        "grep": true,
        "webfetch": true,
        "skill": true,
        "suggest": true,
        "lsp": true,
        "todowrite": false,
        "write": true,
        "edit": true,
        "apply_patch": true,
        "bash": true,
        "question": true,
        "task": false,
        "plan_exit": false,
        "mcp": true,
        "plugin": true,
    });
    apply_task_child_disable_map(
        state,
        parent,
        out.as_object_mut().expect("static literal is an object"),
    );
    out
}

/// Compute the tools-disable map for a task subagent and merge it into
/// `out`. Mirrors Bun's `tool/task.ts:163-167` policy:
/// - Disable `task` when the parent agent has no
///   `{permission: "task", action: "allow"}` rule (recursion guard).
/// - Disable `todowrite` when the parent agent has no
///   `{permission: "todowrite", action: "allow"}` rule.
/// - Disable every entry in `cfg.experimental.primary_tools`.
///
/// `task` and `todowrite` are already statically disabled in
/// `task_child_tools()`; the parent-allow checks above can re-enable
/// them for parents that explicitly granted the permission. The
/// `primary_tools` list always takes effect as a deny.
fn apply_task_child_disable_map(state: &AppState, parent: &Session, out: &mut Map<String, Value>) {
    let parent_rules = state
        .session_agent(&parent.id)
        .map(|name| state.agent_permission_rules(&name))
        .unwrap_or_default();
    if parent_allows(&parent_rules, "task") {
        out.insert("task".to_string(), Value::Bool(true));
    } else {
        out.insert("task".to_string(), Value::Bool(false));
    }
    if parent_allows(&parent_rules, "todowrite") {
        out.insert("todowrite".to_string(), Value::Bool(true));
    } else {
        out.insert("todowrite".to_string(), Value::Bool(false));
    }
    for tool in primary_tools_list(state) {
        out.insert(tool, Value::Bool(false));
    }
}

fn parent_allows(rules: &[PermissionRule], permission: &str) -> bool {
    rules
        .iter()
        .any(|rule| rule.permission == permission && rule.action == "allow")
}

fn primary_tools_list(state: &AppState) -> Vec<String> {
    state
        .store
        .config()
        .data
        .get("experimental")
        .and_then(Value::as_object)
        .and_then(|exp| exp.get("primary_tools"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn task_child_model(state: &AppState, agent: &str, parent: Option<Value>) -> Value {
    state
        .agent_info(agent)
        .and_then(|info| info.get("model").cloned())
        .or(parent)
        .unwrap_or_else(|| {
            json!({
                "providerID": "openai",
                "modelID": "gpt-5.1-codex"
            })
        })
}

fn task_child_variant(input: &Value, model: &Value) -> Option<Value> {
    input
        .get("variant")
        .cloned()
        .or_else(|| model.get("variant").cloned())
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
    provider: Option<Value>,
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
    let model = task_child_model(state, &agent, model);
    let variant = task_child_variant(input, &model);
    // Compute the tools-disable map up front so the parent-context lookups
    // (`session_agent`, config) happen on the parent thread; the spawned
    // blocking task only carries the resolved JSON value.
    let child_tools = task_child_tools(state, &parent);
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
    let parent_id = sid.to_string();
    let child_prompt = prompt.clone();
    let child_agent = agent.clone();
    let child_model = model.clone();
    let child_variant = variant.clone();
    let child_tools_value = child_tools;
    let result = tokio::task::spawn_blocking(move || {
        block_on_task_runtime(async move {
            let guard = crate::agent::turn::start_runner_with_parent(
                got.clone(),
                &child_id,
                Some(parent_id),
            )
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
            let run_state = guard.state.clone();
            let run_id = child_id.clone();
            let run_cancel = guard.cancel.clone();
            let task = tokio::task::spawn_local(async move {
                crate::agent::turn::prompt_turn(
                    &run_state,
                    &run_id,
                    PromptInput {
                        parts: vec![json!({ "type": "text", "text": child_prompt })],
                        agent: Some(child_agent),
                        model: Some(child_model),
                        tools: Some(child_tools_value),
                        variant: child_variant,
                        provider,
                        ..Default::default()
                    },
                    run_cancel,
                )
                .await
            });
            if let Some(runner) = guard.state.runners.lock().unwrap().get(&child_id) {
                *runner.abort.lock().unwrap() = Some(task.abort_handle());
            }
            let result = match task.await {
                Ok(result) => result.map_err(|err| format!("Task prompt failed: {err:?}")),
                Err(err) if err.is_cancelled() => Err("Task prompt aborted".to_string()),
                Err(err) => Err(format!("Task prompt failed: {err}")),
            };
            bridge.abort();
            drop(guard);
            result
        })
    })
    .await
    .map_err(|err| format!("Task runtime failed: {err}"))???;
    let child_cost = state.store.assistant_cost_total(&child.id);
    if child_cost > 0.0 {
        match state
            .store
            .add_task_message_cost_record(sid, mid, &child.id, child_cost)
        {
            Ok(events) => {
                if !events.is_empty() {
                    crate::publish_events(
                        state,
                        state.store.paths().directory,
                        parent.project_id,
                        events,
                    );
                }
            }
            Err(err) => eprintln!("[kilo-server] task cost propagation failed: {err}"),
        }
    }
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

/// Sync, path-strict mutating-tool dispatch.
///
/// Retained for direct test access (`tests::permissions::*`) — the
/// production live-stream path goes through `real_mutating_tool_part_gated`
/// so that out-of-worktree paths surface a single
/// `external_directory` permission ask instead of being rejected
/// outright. Tests that need to assert the strict path-rejection shape
/// keep calling this helper.
#[cfg(test)]
pub(crate) fn real_mutating_tool_part(
    root: &FsPath,
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &ChatToolCall,
    time: i64,
    cancel: &AtomicBool,
) -> Value {
    let res = match tool {
        "write" => fake_write(root, &call.input),
        "edit" => fake_edit(root, &call.input),
        "apply_patch" => fake_apply_patch(root, &call.input),
        "bash" => crate::agent::tools::bash::fake_bash_with_cancel(root, &call.input, Some(cancel)),
        _ => Err(format!("Unsupported tool: {tool}")),
    };
    finalize_mutating_tool_part(mid, pid, idx, tool, call, time, res)
}

/// Async sibling of `real_mutating_tool_part` used by the live OpenAI
/// streaming dispatch. Routes each mutating tool through its
/// `*_gated` wrapper so an out-of-worktree path raises a single
/// `external_directory` permission ask before the write side-effect
/// runs. Path-strict behavior for the legacy direct callers (tests)
/// stays in `real_mutating_tool_part`.
#[allow(clippy::too_many_arguments)]
async fn real_mutating_tool_part_gated(
    state: &Arc<AppState>,
    sid: &str,
    root: &FsPath,
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &ChatToolCall,
    time: i64,
    cancel: &AtomicBool,
) -> Value {
    let res = match tool {
        "write" => fake_write_gated(state, sid, mid, pid, idx, root, &call.input).await,
        "edit" => fake_edit_gated(state, sid, mid, pid, idx, root, &call.input).await,
        "apply_patch" => fake_apply_patch_gated(state, sid, mid, pid, idx, root, &call.input).await,
        "bash" => {
            crate::agent::tools::bash::fake_bash_gated(
                state,
                sid,
                mid,
                pid,
                idx,
                root,
                &call.input,
                Some(cancel),
            )
            .await
        }
        _ => Err(format!("Unsupported tool: {tool}")),
    };
    finalize_mutating_tool_part(mid, pid, idx, tool, call, time, res)
}

fn finalize_mutating_tool_part(
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &ChatToolCall,
    time: i64,
    res: Result<(String, String, Value), String>,
) -> Value {
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

/// Build the `path: { cwd, root }` object Bun emits at
/// [`session/prompt.ts:1537`](../../../../../opencode/src/session/prompt.ts:1537).
/// `cwd` is the request directory; `root` is the project worktree.
pub(crate) fn assistant_path(paths: &KiloPath) -> Value {
    json!({
        "cwd": paths.directory,
        "root": paths.worktree,
    })
}

pub(crate) fn assistant_info(
    paths: &KiloPath,
    user: &MessageAppendResult,
    input: &PromptInput,
) -> Value {
    let agent = input.agent.as_deref().unwrap_or("code");
    json!({
        "role": "assistant",
        "parentID": user.info["id"].clone(),
        "providerID": "local",
        "modelID": "fake-echo",
        "agent": agent,
        "path": assistant_path(paths),
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
    paths: &KiloPath,
    user: &MessageAppendResult,
    input: &PromptInput,
    out: &ChatOutput,
) -> Value {
    let agent = input.agent.as_deref().unwrap_or("code");
    let cost = out
        .usage
        .as_ref()
        .map(|usage| usage_cost_value(input.model.as_ref(), usage))
        .unwrap_or_else(|| json!(0));
    let tokens = out
        .usage
        .as_ref()
        .map(tokens_value)
        .unwrap_or_else(crate::agent::openai_stream::zero_tokens);
    json!({
        "role": "assistant",
        "parentID": user.info["id"].clone(),
        "providerID": out.provider,
        "modelID": out.model,
        "agent": agent,
        "path": assistant_path(paths),
        "cost": cost,
        "tokens": tokens,
    })
}

pub(crate) fn assistant_completed_info(start: &MessageAppendResult) -> Value {
    let mut info = start.info.clone();
    let now = now_millis();
    info["finish"] = json!("stop");
    if let Some(time) = info.get_mut("time").and_then(Value::as_object_mut) {
        time.insert("updated".to_string(), json!(now));
        time.insert("completed".to_string(), json!(now));
        return info;
    }

    info["time"] = json!({
        "created": start.time,
        "updated": now,
        "completed": now,
    });
    info
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn assistant_error_info(
    paths: &KiloPath,
    user: &MessageAppendResult,
    input: &PromptInput,
    error: Value,
) -> Value {
    let mut info = assistant_info(paths, user, input);
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
    let mut data = json!({
        "message": err.to_string(),
        "isRetryable": err.is_retryable(),
        "metadata": { "source": "rust-provider" }
    });
    if let Some(status) = err.status_code() {
        data["statusCode"] = json!(status);
    }
    if let Some(body) = err.response_body() {
        data["responseBody"] = json!(body);
    }
    if let Some(ms) = err.retry_after_ms() {
        data["responseHeaders"] = json!({ "retry-after-ms": ms.to_string() });
    }
    json!({ "name": "APIError", "data": data })
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
    // Bun parity: post-process the output through the truncation service
    // before persisting (`tool/tool.ts:91-130`). Tools that already set
    // `metadata.truncated` themselves (e.g. `bash`, which has its own
    // 64 KiB local cap) are skipped. The 1 MiB ceiling below stays as a
    // defense-in-depth safety net for the rare case where truncation
    // can't write to disk.
    let (output, metadata) = apply_truncate(output, metadata);
    let output = crate::limits::truncate_tool_output(output);
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

/// Run the tool-output truncate post-processor and merge its findings
/// into the metadata payload. Returns `(possibly_previewed_output,
/// updated_metadata)`. Skipped if the caller already populated
/// `metadata.truncated`.
fn apply_truncate(output: String, mut metadata: Value) -> (String, Value) {
    let already_handled = metadata
        .as_object()
        .and_then(|map| map.get("truncated"))
        .is_some();
    if already_handled {
        return (output, metadata);
    }
    let state_dir = kilo_store::Store::resolve_state_dir();
    let result = crate::agent::tools::truncate::truncate_for_tool(&state_dir, &output);
    if !result.truncated {
        return (output, metadata);
    }
    if let Some(map) = metadata.as_object_mut() {
        map.insert("truncated".to_string(), Value::Bool(true));
        map.insert(
            "originalBytes".to_string(),
            Value::Number(result.original_bytes.into()),
        );
        map.insert(
            "originalLines".to_string(),
            Value::Number(result.original_lines.into()),
        );
        if let Some(path) = result.output_path.as_ref() {
            map.insert(
                "outputPath".to_string(),
                Value::String(path.display().to_string()),
            );
        }
    }
    (result.preview, metadata)
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

fn file_attachments(parts: &[Value]) -> Vec<ChatAttachment> {
    parts
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("file"))
        .filter_map(|part| {
            let mime = part.get("mime").and_then(Value::as_str)?;
            if mime == "text/plain" {
                return None;
            }
            let url = part.get("url").and_then(Value::as_str)?;
            if !url.starts_with("data:") {
                return None;
            }
            Some(ChatAttachment {
                mime: mime.to_string(),
                url: url.to_string(),
                filename: part
                    .get("filename")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
        })
        .collect()
}

// ---------- multimodal user-input resolvers ----------
//
// Bun's `session/prompt.ts` resolves three multimodal seams on the user
// message before forwarding to the provider:
//   1. `file://` URLs in `file` parts → read disk bytes, emit either
//      inline text (text MIMEs) or base64 data URL attachments
//      (image/PDF/octet-stream).
//   2. Directory paths in `file` parts → recursive walk, emit one
//      synthetic text block per source file (wrapped in `<file path=…>`),
//      one attachment per image, capped to keep prompts bounded.
//   3. `@<path>` mentions in user text → if the token resolves to an
//      existing file under the worktree, inline the file content as a
//      synthetic text block before the user's text.
// MCP `resource` parts are not handled here — `agent::mcp_dispatch` does
// not yet expose a `read_resource` helper, so these are deferred (TODO).
//
// All path resolution flows through `crate::util::paths::resolve_under`
// so we cannot read outside the worktree. Text reads use the BOM-aware
// `agent::tools::encoding::read_to_string` helper.

const MULTIMODAL_FILE_BYTES_CAP: u64 = 1024 * 1024; // 1 MiB
const MULTIMODAL_DIR_DEPTH_CAP: usize = 3;
const MULTIMODAL_DIR_FILE_CAP: usize = 50;
const MULTIMODAL_DIR_BYTES_CAP: u64 = 5 * 1024 * 1024; // 5 MiB

struct ResolvedMultimodal {
    /// Synthetic text blocks (each one a `<file path="...">…</file>`
    /// envelope, joined by blank lines) that should be prepended BEFORE
    /// the user's own text in the final ChatMessage content.
    prefix: String,
    /// Image/PDF/octet-stream attachments to forward alongside the
    /// message — pre-existing data URL attachments from the stored
    /// part list are preserved untouched.
    attachments: Vec<ChatAttachment>,
}

fn resolve_user_multimodal(root: &FsPath, parts: &[Value], user_text: &str) -> ResolvedMultimodal {
    let mut prefix_blocks: Vec<String> = Vec::new();
    let mut attachments: Vec<ChatAttachment> = Vec::new();

    // (1) preserve already-resolved data URL attachments from the part list.
    attachments.extend(file_attachments(parts));

    // (2) resolve `file://` URLs and directory references.
    for part in parts {
        if part.get("type").and_then(Value::as_str) != Some("file") {
            continue;
        }
        let Some(url) = part.get("url").and_then(Value::as_str) else {
            continue;
        };
        if !url.starts_with("file://") {
            // data: URLs are already covered by `file_attachments`;
            // anything else (https://, etc.) is intentionally ignored.
            continue;
        }
        let Some(rel) = parse_file_url(url) else {
            continue;
        };
        let Ok(abs) = crate::util::paths::resolve_under(root, &rel) else {
            // Path escaped the worktree — silently drop, never include.
            continue;
        };
        ingest_path(&abs, &mut prefix_blocks, &mut attachments, 0);
    }

    // (3) resolve `@<path>` mentions in the user text.
    for token in mention_tokens(user_text) {
        let Ok(abs) = crate::util::paths::resolve_under(root, token) else {
            continue;
        };
        if !abs.is_file() {
            continue;
        }
        // @-mentions only inline files (never recursively expand a dir
        // referenced by `@`), and we keep the same per-file caps.
        ingest_file_at(&abs, token, &mut prefix_blocks, &mut attachments);
    }

    ResolvedMultimodal {
        prefix: prefix_blocks.join("\n\n"),
        attachments,
    }
}

/// Strip the `file://` scheme from a URL and return a plain path string.
/// Accepts both `file:///abs/path` and `file://relative/path` forms.
fn parse_file_url(url: &str) -> Option<String> {
    let rest = url.strip_prefix("file://")?;
    let rest = rest.strip_prefix('/').unwrap_or(rest);
    let decoded = percent_decode_simple(rest);
    Some(decoded)
}

/// Minimal percent-decoder for path segments — only `%XX` triplets where
/// XX is two hex digits. Anything else passes through verbatim.
fn percent_decode_simple(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_digit(bytes[i + 1]);
            let lo = hex_digit(bytes[i + 2]);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(10 + (b - b'a')),
        b'A'..=b'F' => Some(10 + (b - b'A')),
        _ => None,
    }
}

/// Ingest one path — file or directory — into the prefix/attachments
/// accumulators. Directories recurse with bounded depth and file/byte
/// caps; once a cap is hit we emit a single truncation marker.
fn ingest_path(
    abs: &FsPath,
    prefix: &mut Vec<String>,
    attachments: &mut Vec<ChatAttachment>,
    depth: usize,
) {
    if abs.is_dir() {
        ingest_dir(abs, abs, prefix, attachments, depth);
        return;
    }
    if abs.is_file() {
        // Use the path-relative-to-itself for the `<file path=…>` label
        // when ingesting a top-level file; mention/dir paths re-key
        // appropriately via their callers.
        let label = abs.to_string_lossy().into_owned();
        ingest_file_at(abs, &label, prefix, attachments);
    }
}

/// Walk a directory recursively (depth-capped, blocklist-filtered), and
/// emit one prefix block per text file or one attachment per image.
/// After hitting either the file-count or total-byte cap, emits a
/// single `[directory contents truncated]` text block and returns.
fn ingest_dir(
    base: &FsPath,
    current: &FsPath,
    prefix: &mut Vec<String>,
    attachments: &mut Vec<ChatAttachment>,
    depth: usize,
) {
    if depth > MULTIMODAL_DIR_DEPTH_CAP {
        return;
    }
    let Ok(entries) = std::fs::read_dir(current) else {
        return;
    };
    let mut sorted: Vec<_> = entries.flatten().collect();
    sorted.sort_by_key(|e| e.file_name());
    for entry in sorted {
        let path = entry.path();
        let rel = path
            .strip_prefix(base)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        if crate::util::git::generated_like(&rel) {
            continue;
        }
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if meta.is_dir() {
            ingest_dir(base, &path, prefix, attachments, depth + 1);
            continue;
        }
        if !meta.is_file() {
            continue;
        }
        // Cap check: emit truncation marker once and stop.
        let used_bytes: u64 = prefix.iter().map(|b| b.len() as u64).sum::<u64>()
            + attachments.iter().map(|a| a.url.len() as u64).sum::<u64>();
        if prefix.len() + attachments.len() >= MULTIMODAL_DIR_FILE_CAP
            || used_bytes >= MULTIMODAL_DIR_BYTES_CAP
        {
            prefix.push("[directory contents truncated]".to_string());
            return;
        }
        ingest_file_at(&path, &rel, prefix, attachments);
    }
}

/// Read a single file under the worktree, classify by extension, and
/// dispatch into either the prefix accumulator (text MIMEs) or the
/// attachment list (image/PDF/octet-stream). Reads beyond the 1 MiB
/// file cap are skipped entirely.
fn ingest_file_at(
    abs: &FsPath,
    label: &str,
    prefix: &mut Vec<String>,
    attachments: &mut Vec<ChatAttachment>,
) {
    let Ok(meta) = std::fs::metadata(abs) else {
        return;
    };
    if !meta.is_file() {
        return;
    }
    if meta.len() > MULTIMODAL_FILE_BYTES_CAP {
        return;
    }
    let Ok(bytes) = std::fs::read(abs) else {
        return;
    };
    let mime = mime_for_extension(abs);
    if mime.starts_with("text/") || mime == "application/json" {
        let (text, _) = crate::agent::tools::encoding::read_to_string(&bytes);
        prefix.push(format!("<file path=\"{label}\">\n{text}\n</file>"));
        return;
    }
    // Binary-ish: emit as a base64 data URL attachment.
    use base64::{engine::general_purpose::STANDARD, Engine};
    let url = format!("data:{};base64,{}", mime, STANDARD.encode(&bytes));
    let filename = abs
        .file_name()
        .map(|name| name.to_string_lossy().into_owned());
    attachments.push(ChatAttachment {
        mime: mime.to_string(),
        url,
        filename,
    });
}

/// Lean ext→MIME table for the multimodal resolver. Anything not listed
/// falls back to `application/octet-stream` (treated as a binary file
/// attachment downstream).
fn mime_for_extension(path: &FsPath) -> &'static str {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("pdf") => "application/pdf",
        Some("txt") => "text/plain",
        Some("md" | "markdown") => "text/markdown",
        Some("json") => "application/json",
        Some("html" | "htm") => "text/html",
        Some("css") => "text/css",
        _ => "application/octet-stream",
    }
}

/// Iterate `@\S+` tokens in user text, returning each token's path
/// portion (without the leading `@`). No regex dependency — we scan
/// byte-by-byte and harvest runs of non-whitespace after each `@`.
fn mention_tokens(text: &str) -> Vec<&str> {
    let mut out: Vec<&str> = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'@' {
            i += 1;
            continue;
        }
        // `@` must follow whitespace or be the very first character so
        // we don't pick up email-ish substrings (`foo@bar` etc).
        if i > 0 {
            let prev = bytes[i - 1];
            if !prev.is_ascii_whitespace() {
                i += 1;
                continue;
            }
        }
        let start = i + 1;
        let mut end = start;
        while end < bytes.len() && !bytes[end].is_ascii_whitespace() {
            end += 1;
        }
        if end > start {
            out.push(&text[start..end]);
        }
        i = end;
    }
    out
}

fn environment_details(ctx: Option<&Value>) -> Option<String> {
    let ctx = ctx?;
    let mut lines = vec![
        "<environment_details>".to_string(),
        format!(
            "Current time: {}",
            chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%:z")
        ),
    ];
    if let Some(value) = ctx.get("activeFile").and_then(Value::as_str) {
        lines.push(format!("Active file: {value}"));
    }
    if let Some(files) = ctx.get("visibleFiles").and_then(Value::as_array) {
        if !files.is_empty() {
            lines.push("Visible files:".to_string());
            for file in files.iter().filter_map(Value::as_str) {
                lines.push(format!("  {file}"));
            }
        }
    }
    if let Some(tabs) = ctx.get("openTabs").and_then(Value::as_array) {
        if !tabs.is_empty() {
            lines.push("Open tabs:".to_string());
            for tab in tabs.iter().filter_map(Value::as_str) {
                lines.push(format!("  {tab}"));
            }
        }
    }
    lines.push("</environment_details>".to_string());
    Some(lines.join("\n"))
}

pub(crate) fn user_info(paths: &KiloPath, input: &PromptInput) -> Value {
    let mut info = json!({
        "role": "user",
        "path": assistant_path(paths),
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

#[cfg(test)]
mod multimodal_tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn unique_root() -> PathBuf {
        static IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = IDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let name = format!(
            "kilo-parts-multimodal-{}-{}-{seq}",
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            std::process::id()
        );
        std::env::temp_dir().join(name)
    }

    fn file_url_for(abs: &FsPath) -> String {
        // Use a `file:///` form regardless of platform; resolve_under
        // strips the leading slash and then re-anchors to the worktree.
        let s = abs.to_string_lossy().replace('\\', "/");
        if s.starts_with('/') {
            format!("file://{s}")
        } else {
            format!("file:///{s}")
        }
    }

    #[test]
    fn file_url_resolves_to_input_file_with_data_url() {
        let root = unique_root();
        fs::create_dir_all(&root).unwrap();
        let path = root.join("doc.pdf");
        fs::write(&path, b"%PDF-1.4 fake pdf bytes").unwrap();

        let parts = vec![json!({
            "type": "file",
            "url": file_url_for(&path),
        })];
        let resolved = resolve_user_multimodal(&root, &parts, "");
        assert!(
            resolved.prefix.is_empty(),
            "binary files must not produce inline text, got: {:?}",
            resolved.prefix
        );
        assert_eq!(resolved.attachments.len(), 1);
        assert_eq!(resolved.attachments[0].mime, "application/pdf");
        assert!(
            resolved.attachments[0]
                .url
                .starts_with("data:application/pdf;base64,"),
            "expected data URL, got {}",
            resolved.attachments[0].url
        );
        assert_eq!(resolved.attachments[0].filename.as_deref(), Some("doc.pdf"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn file_url_text_resolves_to_input_text() {
        let root = unique_root();
        fs::create_dir_all(&root).unwrap();
        let path = root.join("note.md");
        fs::write(&path, "hello *world*").unwrap();

        let parts = vec![json!({
            "type": "file",
            "url": file_url_for(&path),
        })];
        let resolved = resolve_user_multimodal(&root, &parts, "");
        assert!(resolved.attachments.is_empty());
        assert!(
            resolved.prefix.contains("hello *world*"),
            "missing inlined text body: {:?}",
            resolved.prefix
        );
        assert!(resolved.prefix.starts_with("<file path=\""));
        assert!(resolved.prefix.contains("</file>"));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn file_url_outside_worktree_is_rejected() {
        let root = unique_root();
        fs::create_dir_all(&root).unwrap();
        // Path with `..` in it must be refused by resolve_under.
        let parts = vec![json!({
            "type": "file",
            "url": "file:///../etc/passwd",
        })];
        let resolved = resolve_user_multimodal(&root, &parts, "");
        assert!(resolved.prefix.is_empty());
        assert!(resolved.attachments.is_empty());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn directory_expansion_walks_recursively_with_blocklist() {
        let root = unique_root();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::create_dir_all(root.join("node_modules").join("dep")).unwrap();
        fs::write(root.join("src").join("a.txt"), "alpha").unwrap();
        fs::write(root.join("src").join("b.md"), "beta").unwrap();
        fs::write(
            root.join("node_modules").join("dep").join("ignored.txt"),
            "should not appear",
        )
        .unwrap();

        let parts = vec![json!({
            "type": "file",
            "url": file_url_for(&root.join("src")),
        })];
        let resolved = resolve_user_multimodal(&root, &parts, "");
        assert!(
            resolved.prefix.contains("alpha"),
            "expected alpha in prefix: {:?}",
            resolved.prefix
        );
        assert!(resolved.prefix.contains("beta"));
        assert!(
            !resolved.prefix.contains("should not appear"),
            "node_modules content leaked into prefix: {:?}",
            resolved.prefix
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn directory_expansion_caps_at_size_limit() {
        let root = unique_root();
        fs::create_dir_all(root.join("many")).unwrap();
        for i in 0..(MULTIMODAL_DIR_FILE_CAP + 5) {
            fs::write(root.join("many").join(format!("f{i:02}.txt")), "x").unwrap();
        }
        let parts = vec![json!({
            "type": "file",
            "url": file_url_for(&root.join("many")),
        })];
        let resolved = resolve_user_multimodal(&root, &parts, "");
        let truncated = resolved
            .prefix
            .lines()
            .any(|line| line.contains("[directory contents truncated]"));
        assert!(
            truncated,
            "expected truncation marker after {} files; prefix len = {}",
            MULTIMODAL_DIR_FILE_CAP,
            resolved.prefix.len()
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn at_mention_in_user_text_inlines_existing_file() {
        let root = unique_root();
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("README.md"), "# header").unwrap();

        let parts: Vec<Value> = vec![];
        let resolved = resolve_user_multimodal(&root, &parts, "look at @README.md please");
        assert!(
            resolved.prefix.contains("# header"),
            "missing inlined README contents: {:?}",
            resolved.prefix
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn at_mention_with_nonexistent_path_stays_as_text() {
        let root = unique_root();
        fs::create_dir_all(&root).unwrap();

        let parts: Vec<Value> = vec![];
        let resolved = resolve_user_multimodal(&root, &parts, "see @does-not-exist for context");
        assert!(
            resolved.prefix.is_empty(),
            "non-resolving @-mention must not inject prefix content: {:?}",
            resolved.prefix
        );
        assert!(resolved.attachments.is_empty());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn mention_tokens_only_after_whitespace_or_start() {
        // Email-ish substrings must not be picked up.
        let tokens = mention_tokens("contact me at foo@bar.com or @real");
        assert_eq!(tokens, vec!["real"]);
    }
}

#[cfg(test)]
mod reasoning_replay_tests {
    //! Cross-turn encrypted reasoning round-trip: persisted assistant
    //! `reasoning` parts must rebuild as `ChatResponseItem::Reasoning`
    //! entries on the assistant turn so the next request's `input[]`
    //! gets a `{type: "reasoning", id, encrypted_content, summary}`
    //! cache-hit anchor. Tests build the persisted-part shape directly
    //! and exercise the `reasoning_part_responses` projection that
    //! `real_messages` uses for the assistant role.

    use super::*;

    fn reasoning_part(item_id: &str, encrypted: &str, text: &str) -> Value {
        let mut part = json!({
            "type": "reasoning",
            "text": text,
            "time": { "start": 0 },
        });
        if !item_id.is_empty() {
            part["itemID"] = json!(item_id);
        }
        if !encrypted.is_empty() {
            part["encryptedContent"] = json!(encrypted);
        }
        part
    }

    fn function_call_part(call_id: &str, tool: &str, input: Value) -> Value {
        json!({
            "type": "tool",
            "tool": tool,
            "callID": call_id,
            "state": {
                "status": "completed",
                "input": input,
                "output": "ok",
            },
        })
    }

    #[test]
    fn real_messages_emits_reasoning_response_items_for_persisted_parts() {
        let parts = vec![
            reasoning_part("rs_1", "enc-blob-1", "thinking step one"),
            reasoning_part("rs_1", "enc-blob-1", "thinking step two"),
            reasoning_part("rs_2", "enc-blob-2", "second item summary"),
            json!({ "type": "text", "text": "final answer" }),
        ];
        let items = reasoning_part_responses(&parts);
        assert_eq!(items.len(), 2, "one ChatResponseItem per distinct itemID");
        match &items[0] {
            ChatResponseItem::Reasoning(item) => {
                assert_eq!(item.id, "rs_1");
                assert_eq!(item.encrypted_content, "enc-blob-1");
                assert_eq!(
                    item.summary,
                    vec![
                        "thinking step one".to_string(),
                        "thinking step two".to_string()
                    ],
                    "summary deltas under the same itemID concat in arrival order"
                );
            }
            other => panic!("expected Reasoning, got {other:?}"),
        }
        match &items[1] {
            ChatResponseItem::Reasoning(item) => {
                assert_eq!(item.id, "rs_2");
                assert_eq!(item.encrypted_content, "enc-blob-2");
                assert_eq!(item.summary, vec!["second item summary".to_string()]);
            }
            other => panic!("expected Reasoning, got {other:?}"),
        }
    }

    #[test]
    fn real_messages_skips_reasoning_parts_without_encrypted_content() {
        let parts = vec![
            // Missing encryptedContent — legacy/stale, must be dropped.
            json!({
                "type": "reasoning",
                "text": "stale summary",
                "itemID": "rs_legacy",
                "time": { "start": 0 },
            }),
            // Empty string encryptedContent — also drop.
            reasoning_part("rs_empty", "", "empty blob"),
            // Missing itemID — can't anchor cache, drop.
            json!({
                "type": "reasoning",
                "text": "no anchor",
                "encryptedContent": "blob",
                "time": { "start": 0 },
            }),
            // Valid one to confirm filter is correct.
            reasoning_part("rs_ok", "enc-ok", "ok summary"),
        ];
        let items = reasoning_part_responses(&parts);
        assert_eq!(items.len(), 1);
        match &items[0] {
            ChatResponseItem::Reasoning(item) => {
                assert_eq!(item.id, "rs_ok");
                assert_eq!(item.encrypted_content, "enc-ok");
            }
            other => panic!("expected Reasoning, got {other:?}"),
        }
    }

    #[test]
    fn real_messages_orders_reasoning_before_assistant_text() {
        // Mixed assistant-turn parts: reasoning interleaved with tool
        // parts. The projection must yield reasoning entries first
        // (their position in the `responses` vec is what `responses_input`
        // walks for the head-of-turn replay).
        let parts = vec![
            reasoning_part("rs_a", "enc-a", "before tool"),
            function_call_part("call_1", "read", json!({"path": "x"})),
            reasoning_part("rs_b", "enc-b", "after tool"),
            json!({ "type": "text", "text": "answer" }),
        ];
        let mut combined = reasoning_part_responses(&parts);
        combined.extend(tool_part_calls(&parts));
        // Reasoning items first, in persisted order.
        assert!(matches!(combined[0], ChatResponseItem::Reasoning(_)));
        assert!(matches!(combined[1], ChatResponseItem::Reasoning(_)));
        // Tool call follows.
        match &combined[2] {
            ChatResponseItem::FunctionCall(call) => {
                assert_eq!(call.id, "call_1");
                assert_eq!(call.name, "read");
            }
            other => panic!("expected FunctionCall after reasoning, got {other:?}"),
        }
        // Sanity: reasoning ids in order.
        match (&combined[0], &combined[1]) {
            (ChatResponseItem::Reasoning(a), ChatResponseItem::Reasoning(b)) => {
                assert_eq!(a.id, "rs_a");
                assert_eq!(b.id, "rs_b");
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn real_messages_handles_assistant_with_no_reasoning_parts() {
        // Regression: non-reasoning models / pre-Wave-5-U sessions emit
        // turns with zero reasoning parts. The projection must produce
        // an empty Vec so the assembled `ChatMessage.responses` only
        // carries the existing tool_call / function_output entries.
        let parts = vec![
            json!({ "type": "text", "text": "plain answer" }),
            function_call_part("call_x", "grep", json!({"pattern": "foo"})),
        ];
        let items = reasoning_part_responses(&parts);
        assert!(
            items.is_empty(),
            "no reasoning parts should yield no Reasoning items, got {items:?}"
        );
    }
}
