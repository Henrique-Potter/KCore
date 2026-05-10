//! OpenAI OAuth Codex Responses streaming pipeline.
//!
//! Step 8 of the kilo-server module split: `prompt_openai_stream`,
//! `finalize_openai_aborted`, the `assistant_*_info` / `step_finish_part_*`
//! shape helpers, the live-tool-name repair (`repair_tool_name`),
//! noop-injection rules (`should_inject_noop`), tool-call detection
//! (`has_tool_call(s)`), token shape conversions (`tokens_value`,
//! `zero_tokens`), and the prompt composition (`prompt_instructions`,
//! `format_type`, `is_openai_oauth`, `env_openai_oauth`,
//! `OPENAI_OAUTH_SOUL_RAW`, `STRUCTURED_OUTPUT_SYSTEM_PROMPT`) all moved
//! here verbatim from `lib.rs`.
//!
//! Visibility is `pub(crate)` for everything that crosses the module
//! boundary; the inner `step_finish_part`, `tokens_value`, and helpers
//! consumed by `agent::parts` / `agent::fake` are reachable through
//! `crate::agent::openai_stream::*`.

use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use kilo_protocol::{
    GlobalEvent, KiloPath, MessageAppendInput, MessageAppendResult, PromptInput, SessionUpdateInput,
};
#[cfg(test)]
use kilo_provider::ChatTool;
use kilo_provider::{
    ChatMessage, ChatResponseItem, ChatUsage, ProviderError, ReasoningItem, StreamEvent,
};
use serde_json::{json, Value};
use tokio::task::JoinSet;

use crate::agent::is_canceled;
use crate::agent::parts::{
    aborted_error, assistant_completed_info, assistant_error_info, max_iterations_error,
    provider_error, real_messages, real_tool_part, real_tools, tool_error, tool_part_response,
    tool_running,
};
use crate::agent::retry::{retry_status, sleep_or_cancel, RetryPolicy, RetryState};
use crate::agent::shape::{
    repair_tool_name, step_finish_part_iter, step_start_part, tokens_value, usage_accumulate,
    usage_cost_value,
};
use crate::{oauth, registry};
use crate::{
    publish_error, publish_events, publish_idle, publish_part_delta, publish_status,
    publish_status_value, publish_turn_close, AppState,
};
use crate::{Repair, KNOWN_TOOLS};

/// Hard cap on iterations of the live OAuth tool loop. Mirrors Bun's
/// `streamText` `maxSteps` ceiling — protects against a model that
/// indefinitely calls tools and never produces a terminal stop.
pub(crate) const OPENAI_OAUTH_MAX_ITERATIONS: usize = 16;

/// Doom-loop window. When the last `DOOM_LOOP_THRESHOLD` *completed*
/// tool parts have the same tool name AND the same input JSON, the loop
/// pauses and asks the user via `ask_doom_loop`. Matches Bun's
/// `processor.ts:27` constant.
pub(crate) const DOOM_LOOP_THRESHOLD: usize = 3;

/// Maximum mid-stream retries per turn. When the OpenAI Responses
/// stream errors AFTER content (text deltas, reasoning, or tool calls)
/// has already streamed within an iteration, the agent loop replays
/// the iteration with the partial assistant content appended to the
/// request so the model can continue coherently. Beyond this cap the
/// partial is finalized with a terminal error envelope.
pub(crate) const MID_STREAM_RETRY_CAP: u8 = 3;

/// Default delay between mid-stream retry rounds when the upstream
/// error doesn't carry a `Retry-After`. Matches the lower bound of the
/// pre-response retry backoff so users see the same minimum wait shape.
const MID_STREAM_RETRY_DEFAULT_DELAY: Duration = Duration::from_millis(1_000);

/// Returns the per-retry delay to use when the upstream error has no
/// explicit `Retry-After`. Tests override via `KILO_MID_STREAM_RETRY_MS`
/// to keep three back-to-back retries from blowing a multi-second
/// timeout; production callers always see [`MID_STREAM_RETRY_DEFAULT_DELAY`].
fn mid_stream_default_delay() -> Duration {
    std::env::var("KILO_MID_STREAM_RETRY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(MID_STREAM_RETRY_DEFAULT_DELAY)
}

const TITLE_PROMPT: &str = include_str!("../../../../../opencode/src/agent/prompt/title.txt");

#[derive(Clone)]
struct PendingTool {
    idx: usize,
    tool: String,
    call: String,
    input: Value,
}

pub(crate) async fn prompt_openai_stream(
    state: Arc<AppState>,
    id: &str,
    input: PromptInput,
    user: MessageAppendResult,
    text: String,
    dir: String,
    project: String,
    cancel: Arc<AtomicBool>,
) -> rusqlite::Result<MessageAppendResult> {
    // M7 Fix 1+2: this is the outer turn loop. One assistant message per
    // turn carries text + tool_call + tool_result parts across iterations
    // (Bun parity — see `processor.ts:301-302, 334-335`). We start with a
    // initial empty text part for the first stream deltas; each iteration
    // appends its own chronological parts via `append_message_record`,
    // which upserts on (`info.id`, `part.id`).
    let paths = state.store.paths();
    let start = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: assistant_openai_info(&paths, &user, &input),
            parts: vec![json!({ "type": "text", "text": "" })],
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
    let root = PathBuf::from(paths.directory.clone());
    let mut auths = match oauth::tokens::fresh_auths(&state, &cancel).await {
        Ok(auths) => auths,
        Err(err) => {
            if is_canceled(&cancel) || err.contains("aborted") {
                return finalize_openai_aborted(
                    &state,
                    id,
                    &dir,
                    &project,
                    &start.result,
                    &mid,
                    &pid,
                    "",
                    &[],
                    None,
                );
            }
            let mut info = start.result.info.clone();
            info["error"] = provider_error(ProviderError::Api(err));
            info["finish"] = json!("error");
            let assistant = state.store.append_message_record(
                id,
                MessageAppendInput {
                    info,
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
    let cfg = state.store.config();
    let instructions = prompt_instructions_with_root(
        &input,
        Some(paths.directory.as_str()),
        Some(paths.config.as_str()),
        Some(paths.home.as_str()),
        agent_prompt(&state, &input).as_deref(),
    );
    let tools = real_tools(&state, &input);
    let mut base_messages = real_messages(&state, id, &text);
    let stream_model = input.model.clone();
    spawn_title_generation(
        state.clone(),
        id.to_string(),
        cfg.clone(),
        auths.clone(),
        input.model.clone(),
        text.clone(),
        cancel.clone(),
    );

    // Accumulated state across iterations.
    let mut deltas = String::new();
    let mut tool_parts: Vec<Value> = Vec::new();
    // `total_usage` sums every iteration's usage so the assistant's final
    // `info.tokens` reflects the entire turn (Bun's
    // `processor.ts:443-447` accumulates the same way). `last_iter_usage`
    // is the most recent iteration only — used by `finalize_openai_aborted`
    // when an abort interrupts mid-iteration.
    let mut total_usage = ChatUsage::default();
    let mut last_iter_usage: Option<ChatUsage> = None;
    let mut last_finish: Option<String> = None;
    let mut history_extension: Vec<ChatMessage> = Vec::new();
    let mut last_text: String = String::new();
    let mut seen_malformed_tool_arguments: HashSet<String> = HashSet::new();
    let mut compaction_attempts: usize = 0;
    let mut retry = RetryState::new(RetryPolicy::from_env());

    // Captured `StructuredOutput` payload, set when the model calls the
    // synthetic structured-output tool (Bun: `prompt.ts:1969-1995`). The
    // synchronous closure below cannot mutate this directly, so we use an
    // `Arc<Mutex<...>>` and snapshot it after each iteration.
    let structured_capture: Arc<std::sync::Mutex<Option<Value>>> =
        Arc::new(std::sync::Mutex::new(None));
    let wants_structured = input
        .format
        .as_ref()
        .and_then(|f| f.get("type"))
        .and_then(Value::as_str)
        == Some("json_schema");

    for iteration in 0..OPENAI_OAUTH_MAX_ITERATIONS {
        // Pre-flight cancel check — short-circuit before opening another
        // upstream connection.
        if is_canceled(&cancel) {
            return finalize_openai_aborted(
                &state,
                id,
                &dir,
                &project,
                &start.result,
                &mid,
                &pid,
                &deltas,
                &tool_parts,
                Some(&total_usage),
            );
        }
        match oauth::tokens::fresh_auths(&state, &cancel).await {
            Ok(next) => auths = next,
            Err(err) => {
                if is_canceled(&cancel) || err.contains("aborted") {
                    return finalize_openai_aborted(
                        &state,
                        id,
                        &dir,
                        &project,
                        &start.result,
                        &mid,
                        &pid,
                        &deltas,
                        &tool_parts,
                        Some(&total_usage),
                    );
                }
                let assistant = state.store.append_message_record(
                    id,
                    MessageAppendInput {
                        info: assistant_error_info(
                            &paths,
                            &user,
                            &input,
                            provider_error(ProviderError::Api(err)),
                        ),
                        parts: Vec::new(),
                    },
                )?;
                publish_events(&state, dir, project, assistant.events);
                publish_error(&state, id, assistant.result.info["error"].clone());
                publish_idle(&state, id);
                publish_turn_close(&state, id, "error");
                return Ok(assistant.result);
            }
        }

        // Per-iteration `step-start` part. Bun emits this from the AI SDK
        // `start-step` event (`processor.ts:402-412`); we generate it
        // directly off the loop index. Append to the assistant message and
        // remember it so the final `assistant_parts_with` carries it.
        let step_start = step_start_part(id, &mid, &pid, iteration);
        if let Ok(record) = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: start.result.info.clone(),
                parts: vec![step_start.clone()],
            },
        ) {
            publish_events(&state, dir.clone(), project.clone(), record.events);
        }
        tool_parts.push(step_start);

        // Per-iteration in-stream tool dispatch. The closure spawns each
        // `ChatToolCall` into the JoinSet IMMEDIATELY as `ToolCall`
        // arrives from the SSE parser, matching the AI SDK's
        // `ToolCallComplete` semantics. Tools execute in parallel with
        // any subsequent text deltas. The lock is held only across the
        // synchronous `set.spawn(...)` call — never across an `.await`.
        let join_set: Arc<std::sync::Mutex<JoinSet<(usize, Value)>>> =
            Arc::new(std::sync::Mutex::new(JoinSet::new()));
        let pending_tools: Arc<std::sync::Mutex<Vec<PendingTool>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let iter_text: Arc<std::sync::Mutex<String>> =
            Arc::new(std::sync::Mutex::new(String::new()));
        let iter_reasoning: Arc<std::sync::Mutex<BTreeMap<String, String>>> =
            Arc::new(std::sync::Mutex::new(BTreeMap::new()));
        // Encrypted reasoning items captured from `response.output_item.done`
        // — replayed verbatim into the next iteration's request `input[]`
        // so cache hits survive a multi-step turn. Keyed by the same `rid`
        // (raw item id from `rs_*`) used for `iter_reasoning` summary text.
        // Insertion order is preserved via `Vec` rather than a map so the
        // request emits items in the order the model produced them.
        let iter_reasoning_encrypted: Arc<std::sync::Mutex<Vec<ReasoningItem>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let plan_exit_seen = Arc::new(AtomicBool::new(false));
        let stream_root = root.clone();
        let stream_mid = mid.clone();
        let stream_pid = pid.clone();
        let stream_id = id.to_string();
        let stream_cancel = cancel.clone();
        let stream_state = state.clone();
        let stream_join = join_set.clone();
        let stream_pending = pending_tools.clone();
        let stream_model = stream_model.clone();
        let stream_dir = dir.clone();
        let stream_project = project.clone();
        let stream_text_pid = iteration_text_part_id(&pid, iteration);
        let stream_reasoning = iter_reasoning.clone();
        let stream_reasoning_encrypted = iter_reasoning_encrypted.clone();
        let stream_plan_exit = plan_exit_seen.clone();
        let base_idx = tool_parts.len();
        let start_time = start.result.time;

        // Compose this iteration's outbound message list. The first
        // iteration uses the persisted history; subsequent iterations
        // append tool-call/tool-result synthetic messages so the model
        // sees the outcome of its previous step.
        let mut iter_messages = base_messages.clone();
        iter_messages.extend(history_extension.iter().cloned());

        let out = loop {
            let before_delta_len = deltas.len();
            let before_pending_len = pending_tools.lock().map(|items| items.len()).unwrap_or(0);
            let before_reasoning_len = iter_reasoning.lock().map(|items| items.len()).unwrap_or(0);
            let result = kilo_provider::stream_openai_oauth(
                &cfg,
                &auths,
                input.model.as_ref(),
                Some(id),
                instructions.clone(),
                iter_messages.clone(),
                tools.clone(),
                &cancel,
                |event| match event {
                    StreamEvent::TextDelta(delta) => {
                        publish_part_delta(&state, id, &mid, &stream_text_pid, &delta);
                        if let Ok(mut buf) = iter_text.lock() {
                            buf.push_str(&delta);
                        }
                        deltas.push_str(&delta);
                    }
                    StreamEvent::ReasoningStart { id: rid } => {
                        let pid = reasoning_part_id(&pid, iteration, &rid);
                        if let Ok(mut map) = stream_reasoning.lock() {
                            map.entry(rid).or_default();
                        }
                        let part = reasoning_part(&pid, "", start_time, None);
                        if let Ok(record) = state.store.append_message_record(
                            id,
                            MessageAppendInput {
                                info: start.result.info.clone(),
                                parts: vec![part],
                            },
                        ) {
                            publish_events(&state, dir.clone(), project.clone(), record.events);
                        }
                    }
                    StreamEvent::ReasoningDelta { id: rid, delta } => {
                        let pid = reasoning_part_id(&pid, iteration, &rid);
                        publish_part_delta(&state, id, &mid, &pid, &delta);
                        if let Ok(mut map) = stream_reasoning.lock() {
                            map.entry(rid).or_default().push_str(&delta);
                        }
                    }
                    StreamEvent::ReasoningEnd { .. } => {}
                    StreamEvent::ReasoningItem {
                        id: rid,
                        encrypted_content,
                    } => {
                        // Capture the encrypted reasoning blob (verbatim
                        // shape: `id` + `encrypted_content`) for replay
                        // into the next iteration's request `input[]`.
                        // Bun's converter emits these as first-class
                        // `{type:"reasoning"}` items between prior tool
                        // I/O and the current user input — see
                        // `provider/sdk/copilot/responses/convert-to-openai-responses-input.ts:185-244`.
                        // The streamed summaries live under
                        // `<item_id>:<summary_index>` keys (per
                        // `reasoning_id` in the provider parser); collect
                        // every matching prefix so multi-summary traces
                        // round-trip in order.
                        let summary = stream_reasoning
                            .lock()
                            .ok()
                            .map(|map| {
                                let prefix = format!("{rid}:");
                                map.iter()
                                    .filter(|(key, _)| key.starts_with(&prefix))
                                    .filter(|(_, text)| !text.is_empty())
                                    .map(|(_, text)| text.clone())
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        if let Ok(mut items) = stream_reasoning_encrypted.lock() {
                            // Dedupe on id — the parser may emit a
                            // duplicate `output_item.done` if the upstream
                            // resends the trailing frame.
                            if !items.iter().any(|item| item.id == rid) {
                                items.push(ReasoningItem {
                                    id: rid,
                                    encrypted_content,
                                    summary,
                                });
                            }
                        }
                    }
                    StreamEvent::ToolCall(call) => {
                        if call.name == crate::agent::parts::STRUCTURED_OUTPUT_TOOL_NAME {
                            if let Ok(mut slot) = structured_capture.lock() {
                                *slot = Some(call.input.clone());
                            }
                            return;
                        }
                        let exit = call.name == "plan_exit";
                        if stream_plan_exit.load(Ordering::SeqCst) && !exit {
                            return;
                        }
                        let join = stream_join.clone();
                        let cancel = stream_cancel.clone();
                        let root = stream_root.clone();
                        let mid = stream_mid.clone();
                        let pid = stream_pid.clone();
                        let id = stream_id.clone();
                        let state = stream_state.clone();
                        let model = stream_model.clone();
                        let dir = stream_dir.clone();
                        let project = stream_project.clone();
                        let pending_tools = stream_pending.clone();
                        let plan_exit = stream_plan_exit.clone();
                        let idx = {
                            let guard = join.lock().unwrap();
                            base_idx + guard.len()
                        };
                        let tool = live_tool_name(&call.name);
                        let pending =
                            tool_running(&mid, &pid, idx, &tool, &call.id, &call.input, start_time);
                        if let Ok(record) = state.store.append_message_record(
                            &id,
                            MessageAppendInput {
                                info: start.result.info.clone(),
                                parts: vec![pending],
                            },
                        ) {
                            publish_events(&state, dir, project, record.events);
                        }
                        if let Ok(mut pending) = pending_tools.lock() {
                            pending.push(PendingTool {
                                idx,
                                tool: tool.clone(),
                                call: call.id.clone(),
                                input: call.input.clone(),
                            });
                        }
                        if exit {
                            plan_exit.store(true, Ordering::SeqCst);
                            if let Ok(mut guard) = join.lock() {
                                guard.abort_all();
                            }
                        }
                        let mut guard = join.lock().unwrap();
                        guard.spawn(async move {
                            if is_canceled(&cancel) {
                                return (
                                    idx,
                                    tool_error(
                                        &mid,
                                        &pid,
                                        idx,
                                        &tool,
                                        &call.id,
                                        &call.input,
                                        "Tool call aborted".to_string(),
                                        start_time,
                                    ),
                                );
                            }
                            let part = real_tool_part(
                                &state, &root, &id, &mid, &pid, idx, &call, start_time, cancel,
                                model,
                            )
                            .await;
                            (idx, part)
                        });
                    }
                    StreamEvent::ToolDelta { .. }
                    | StreamEvent::Usage(_)
                    | StreamEvent::Finish(_) => {}
                    StreamEvent::Error(err) => {
                        publish_error(&state, id, provider_error(ProviderError::Api(err)))
                    }
                },
            )
            .await;
            if let Err(err) = &result {
                let pending_len = pending_tools.lock().map(|items| items.len()).unwrap_or(0);
                let reasoning_len = iter_reasoning.lock().map(|items| items.len()).unwrap_or(0);
                let clean = deltas.len() == before_delta_len
                    && pending_len == before_pending_len
                    && reasoning_len == before_reasoning_len;
                // Mid-stream branch: content streamed before the failure.
                // The pre-response retry path only fires for `clean`
                // failures; here we replay the iteration with the
                // partial assistant content appended so the model
                // continues coherently. Capped at
                // `MID_STREAM_RETRY_CAP` per turn — beyond that, the
                // partial finalizes as a terminal error.
                if !clean && !is_canceled(&cancel) && !matches!(err, ProviderError::Aborted) {
                    let retries = mid_stream_retry_counter(&state, id);
                    let attempt = retries
                        .as_ref()
                        .map(|counter| counter.load(Ordering::SeqCst))
                        .unwrap_or(MID_STREAM_RETRY_CAP);
                    if attempt < MID_STREAM_RETRY_CAP {
                        if let Some(counter) = retries.as_ref() {
                            counter.store(attempt + 1, Ordering::SeqCst);
                        }
                        let next_attempt = attempt + 1;
                        let delay = err
                            .retry_after_ms()
                            .map(Duration::from_millis)
                            .unwrap_or_else(mid_stream_default_delay);
                        let next_at = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|value| value.as_millis() as u64)
                            .unwrap_or(0)
                            .saturating_add(delay.as_millis() as u64);
                        publish_status_value(
                            &state,
                            id,
                            json!({
                                "type": "retry",
                                "attempt": next_attempt,
                                "message": err.to_string(),
                                "next": next_at,
                            }),
                        );
                        // Abort any in-flight tool tasks; the retry's
                        // model output will re-issue them. The
                        // `pending_tools` slate gets cleared so the
                        // doom-loop / missing-output bookkeeping
                        // doesn't double-count.
                        {
                            let mut guard = join_set.lock().unwrap();
                            guard.abort_all();
                        }
                        if let Ok(mut pending) = pending_tools.lock() {
                            pending.clear();
                        }
                        // Snapshot the partial assistant content the
                        // model produced this iteration. We splice it
                        // into the request `input[]` as a synthetic
                        // assistant ChatMessage so the retry round
                        // continues from where the failure cut the
                        // stream — no `previous_response_id` needed,
                        // works regardless of the upstream `store`
                        // flag.
                        let partial_text = iter_text
                            .lock()
                            .map(|guard| guard.clone())
                            .unwrap_or_default();
                        let partial_reasoning: Vec<ReasoningItem> = iter_reasoning_encrypted
                            .lock()
                            .map(|items| items.clone())
                            .unwrap_or_default();
                        if !partial_text.is_empty() || !partial_reasoning.is_empty() {
                            let responses: Vec<ChatResponseItem> = partial_reasoning
                                .into_iter()
                                .map(ChatResponseItem::Reasoning)
                                .collect();
                            iter_messages.push(ChatMessage {
                                role: "assistant".to_string(),
                                content: partial_text,
                                responses,
                                attachments: Vec::new(),
                            });
                        }
                        if sleep_or_cancel(&cancel, delay).await {
                            break Err(ProviderError::Aborted);
                        }
                        publish_status(&state, id, "busy");
                        continue;
                    }
                }
                if clean {
                    if crate::routes::network::disconnected(err) {
                        // Audit Fix: skip the SSE fan-out if a wait is
                        // already pending for this session. Bun's
                        // `SessionNetwork.ask` only ever holds one wait
                        // per session in practice (the resolver clears
                        // the slot before the loop reaches another
                        // failed connect); rapid back-to-back transport
                        // failures here would otherwise queue duplicate
                        // `session.network.asked` envelopes for the
                        // webview before the user has a chance to reply
                        // to the first.
                        let already_asked = state.has_network_wait_for_session(id);
                        if !already_asked {
                            publish_status_value(
                                &state,
                                id,
                                json!({
                                    "type": "retry",
                                    "attempt": 0,
                                    "message": crate::routes::network::message(err),
                                    "next": 0,
                                }),
                            );
                        }
                        match crate::routes::network::ask_network_wait(
                            &state,
                            id,
                            crate::routes::network::message(err),
                            &cancel,
                        )
                        .await
                        {
                            Ok(()) => {
                                // Wait resolved cleanly (user replied,
                                // or — once `routes/network.rs` is wired
                                // to the auto-probe path — connectivity
                                // returned). Publish the parity
                                // `session.network.restored` envelope
                                // (Bun: `session/network.ts:228-244`)
                                // so the webview can clear any
                                // "offline" UI before the retry round
                                // streams any deltas.
                                crate::http::sse::publish(
                                    &state,
                                    GlobalEvent::bus(
                                        "session.network.restored",
                                        json!({ "sessionID": id }),
                                    ),
                                );
                                publish_status(&state, id, "busy");
                                continue;
                            }
                            Err(reason) if reason == "aborted" => {
                                break Err(ProviderError::Aborted)
                            }
                            Err(reason) => break Err(ProviderError::Api(reason)),
                        }
                    }
                    if let Some(wait) = retry.next(err) {
                        publish_status_value(&state, id, retry_status(&wait));
                        if sleep_or_cancel(&cancel, wait.delay).await {
                            break Err(ProviderError::Aborted);
                        }
                        publish_status(&state, id, "busy");
                        continue;
                    }
                }
            }
            break result;
        };

        let aborting = is_canceled(&cancel) || matches!(&out, Err(ProviderError::Aborted));
        if aborting {
            let mut owned = {
                let mut guard = join_set.lock().unwrap();
                std::mem::take(&mut *guard)
            };
            owned.abort_all();
            drop(owned);
            tool_parts.extend(pending_abort_parts(&mid, &pid, start_time, &pending_tools));
            return finalize_openai_aborted(
                &state,
                id,
                &dir,
                &project,
                &start.result,
                &mid,
                &pid,
                &deltas,
                &tool_parts,
                Some(&total_usage),
            );
        }

        // Drain whichever tool tasks managed to spawn before stream end.
        // Cancelled turns finalize above without waiting for long-running
        // tools, so Stop can always win promptly.
        let mut iter_tools: Vec<(usize, Value)> = Vec::new();
        let mut owned = {
            let mut guard = join_set.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        while let Some(item) = owned.join_next().await {
            if let Ok(item) = item {
                if let Ok(record) = state.store.append_message_record(
                    id,
                    MessageAppendInput {
                        info: start.result.info.clone(),
                        parts: vec![item.1.clone()],
                    },
                ) {
                    publish_events(&state, dir.clone(), project.clone(), record.events);
                }
                iter_tools.push(item);
            }
        }
        iter_tools.sort_by_key(|(idx, _)| *idx);
        let mut iter_tools_only: Vec<Value> =
            iter_tools.into_iter().map(|(_, part)| part).collect();
        iter_tools_only.extend(missing_tool_output_parts(
            &mid,
            &pid,
            start_time,
            &pending_tools,
            &iter_tools_only,
        ));
        let had_tools_this_iter = !iter_tools_only.is_empty();
        let denied_this_iter = iter_tools_only.iter().any(is_permission_denial);
        let plan_exit_this_iter = iter_tools_only.iter().any(is_plan_exit);
        let malformed_this_iter = iter_tools_only.iter().find_map(malformed_tool_arguments);
        tool_parts.extend(iter_tools_only.iter().cloned());

        // Cancel branch: update the IN-PLACE assistant message — never
        // append a second one. Bun's `run-state.ts:48-68 onInterrupt`
        // expects exactly one assistant record per cancelled turn.
        if is_canceled(&cancel) {
            return finalize_openai_aborted(
                &state,
                id,
                &dir,
                &project,
                &start.result,
                &mid,
                &pid,
                &deltas,
                &tool_parts,
                Some(&total_usage),
            );
        }

        // The stream completed without a terminal error path. Reset
        // the per-turn mid-stream retry counter so a later iteration
        // that hits its own mid-stream failure can retry up to the
        // cap again — a clean success budget always replenishes.
        if matches!(&out, Ok(_)) {
            if let Some(counter) = mid_stream_retry_counter(&state, id) {
                counter.store(0, Ordering::SeqCst);
            }
        }

        let provider_out = match out {
            Ok(out) => out,
            Err(ProviderError::Aborted) => {
                return finalize_openai_aborted(
                    &state,
                    id,
                    &dir,
                    &project,
                    &start.result,
                    &mid,
                    &pid,
                    &deltas,
                    &tool_parts,
                    Some(&total_usage),
                );
            }
            Err(ProviderError::ContextWindow(detail)) => {
                // Bun parity: `prompt.ts:1495-1515` triggers compaction
                // when usage overflows the model's context window. We
                // detect reactively (HTTP 400 with context-length error)
                // rather than proactively, then summarize + retry.
                if compaction_attempts >= crate::agent::compaction::MAX_COMPACTION_ATTEMPTS {
                    let assistant = state.store.append_message_record(
                        id,
                        MessageAppendInput {
                            info: assistant_error_info(
                                &paths,
                                &user,
                                &input,
                                crate::agent::compaction::compaction_error_envelope(
                                    &crate::agent::compaction::CompactionError::EmptyResult,
                                ),
                            ),
                            parts: Vec::new(),
                        },
                    )?;
                    publish_events(&state, dir.clone(), project.clone(), assistant.events);
                    publish_error(&state, id, assistant.result.info["error"].clone());
                    publish_idle(&state, id);
                    publish_turn_close(&state, id, "error");
                    return Ok(assistant.result);
                }
                compaction_attempts += 1;
                match crate::agent::compaction::compact_session(
                    &state,
                    id,
                    input.model.as_ref(),
                    &auths,
                    &cancel,
                )
                .await
                {
                    Ok(_) => {
                        // History is now anchored on the new summary
                        // record. Re-enter the loop from the same
                        // iteration index — the next pass calls
                        // `real_messages` again and gets the compacted
                        // view. We DON'T re-push step-start/finish for
                        // this retried iter; the overhead is one wasted
                        // step-start emission per retry.
                        let _ = detail;
                        continue;
                    }
                    Err(err) => {
                        eprintln!("[kilo-server] compaction failed: {err}");
                        let assistant = state.store.append_message_record(
                            id,
                            MessageAppendInput {
                                info: assistant_error_info(
                                    &paths,
                                    &user,
                                    &input,
                                    crate::agent::compaction::compaction_error_envelope(&err),
                                ),
                                parts: Vec::new(),
                            },
                        )?;
                        publish_events(&state, dir, project, assistant.events);
                        publish_error(&state, id, assistant.result.info["error"].clone());
                        publish_idle(&state, id);
                        publish_turn_close(&state, id, "error");
                        return Ok(assistant.result);
                    }
                }
            }
            Err(err) => {
                let assistant = state.store.append_message_record(
                    id,
                    MessageAppendInput {
                        info: assistant_error_info(&paths, &user, &input, provider_error(err)),
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

        if let Some(usage) = provider_out.usage.clone() {
            usage_accumulate(&mut total_usage, &usage);
            last_iter_usage = Some(usage);
        }
        last_finish = provider_out.finish.clone();

        // Track this iteration's text before writing step-finish so persisted
        // part order matches what the user saw: tools first, then the later
        // conclusion text in the continuation step. Reusing the initial empty
        // text part for every iteration made final answers appear above long
        // tool transcripts in the UI.
        //
        // After a mid-stream retry the iteration's accumulated `iter_text`
        // spans both halves of the stream while `provider_out.text` carries
        // only the retry's continuation. Prefer the longer of the two so
        // the persisted text part includes everything the user saw.
        let iter_text_snapshot = iter_text
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let this_iter_text = if iter_text_snapshot.len() >= provider_out.text.len() {
            iter_text_snapshot
        } else {
            provider_out.text.clone()
        };
        last_text = this_iter_text.clone();
        // Snapshot encrypted reasoning items so the persisted part can
        // carry `encryptedContent` for any future round-trip. Read once
        // here so we don't fight the per-event closure for the lock.
        let encrypted_snapshot: Vec<ReasoningItem> = iter_reasoning_encrypted
            .lock()
            .map(|items| items.clone())
            .unwrap_or_default();
        let reasoning_parts = iter_reasoning
            .lock()
            .map(|map| {
                map.iter()
                    .filter(|(_, text)| !text.is_empty())
                    .map(|(rid, text)| {
                        // `iter_reasoning`'s key is the per-summary
                        // `<item_id>:<summary_index>` shape (`reasoning_id`
                        // in the provider parser). Strip the suffix so we
                        // can match the bare `<item_id>` carried on the
                        // `ReasoningItem` event.
                        let item_id = rid.split(':').next().unwrap_or(rid.as_str());
                        let encrypted = encrypted_snapshot
                            .iter()
                            .find(|item| item.id == item_id)
                            .map(|item| item.encrypted_content.clone());
                        reasoning_part_with(
                            &reasoning_part_id(&pid, iteration, rid),
                            text,
                            start_time,
                            Some(start_time),
                            item_id,
                            encrypted.as_deref(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for part in reasoning_parts {
            if let Ok(record) = state.store.append_message_record(
                id,
                MessageAppendInput {
                    info: start.result.info.clone(),
                    parts: vec![part.clone()],
                },
            ) {
                publish_events(&state, dir.clone(), project.clone(), record.events);
            }
            tool_parts.push(part);
        }
        if !this_iter_text.is_empty() {
            let text_part = text_part(&iteration_text_part_id(&pid, iteration), &this_iter_text);
            if let Ok(record) = state.store.append_message_record(
                id,
                MessageAppendInput {
                    info: start.result.info.clone(),
                    parts: vec![text_part.clone()],
                },
            ) {
                publish_events(&state, dir.clone(), project.clone(), record.events);
            }
            tool_parts.push(text_part);
        }

        // Per-iteration `step-finish` part. Bun emits this from the AI SDK
        // `finish-step` event (`processor.ts:414-473`). Carries the
        // *iteration*'s usage (not the cumulative total) so consumers can
        // attribute cost to individual steps. The final `info.tokens` on
        // the assistant message reflects the cumulative `total_usage`.
        let step_finish = step_finish_part_iter(
            id,
            &mid,
            &pid,
            iteration,
            input.model.as_ref(),
            last_iter_usage.as_ref(),
            last_finish.as_deref(),
        );
        if let Ok(record) = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: start.result.info.clone(),
                parts: vec![step_finish.clone()],
            },
        ) {
            publish_events(&state, dir.clone(), project.clone(), record.events);
        }
        tool_parts.push(step_finish);

        // Bun-parity proactive compaction (overflow.ts:19-26 +
        // prompt.ts:1493-1497). After every iteration's step-finish is
        // persisted, check whether the cumulative turn usage has crossed
        // the model's context-window threshold; if so, run compaction
        // BEFORE the next request. This is additive to the reactive
        // ContextWindow path above — proactive just lowers how often the
        // upstream 400 has to fire. `compaction.auto = false` opts out.
        // Bounded by the same `MAX_COMPACTION_ATTEMPTS` cap so a turn
        // that keeps re-overflowing terminates instead of looping.
        if compaction_attempts < crate::agent::compaction::MAX_COMPACTION_ATTEMPTS
            && should_compact_proactively(&cfg, input.model.as_ref(), &total_usage)
        {
            compaction_attempts += 1;
            match crate::agent::compaction::compact_session(
                &state,
                id,
                input.model.as_ref(),
                &auths,
                &cancel,
            )
            .await
            {
                Ok(_) => {
                    // Drop the per-turn `history_extension` so the next
                    // iteration's `iter_messages` is rebuilt from
                    // `real_messages`, which now anchors on the new
                    // summary record. Reset `total_usage` to reflect
                    // the post-compaction reality: the model's input
                    // context has been replaced with the summary, so
                    // accumulated tokens from before the compaction
                    // shouldn't count toward the next threshold check.
                    // Bun's loop re-enters with fresh `tokens` after
                    // `compaction.create` (`prompt.ts:1493-1516`); we
                    // mirror that by zeroing here. Note: assistant
                    // `info.tokens` for the user-visible turn is built
                    // from `total_usage` at finalize time — this means
                    // the user sees only post-compaction tokens, which
                    // matches Bun's behavior since the pre-compaction
                    // streamed parts have been replaced by the
                    // summary anchor.
                    history_extension.clear();
                    base_messages = real_messages(&state, id, &text);
                    total_usage = ChatUsage::default();
                    last_iter_usage = None;
                }
                Err(err) => {
                    eprintln!(
                        "[kilo-server] proactive compaction failed: {err}"
                    );
                    let assistant = state.store.append_message_record(
                        id,
                        MessageAppendInput {
                            info: assistant_error_info(
                                &paths,
                                &user,
                                &input,
                                crate::agent::compaction::compaction_error_envelope(&err),
                            ),
                            parts: Vec::new(),
                        },
                    )?;
                    publish_events(&state, dir, project, assistant.events);
                    publish_error(&state, id, assistant.result.info["error"].clone());
                    publish_idle(&state, id);
                    publish_turn_close(&state, id, "error");
                    return Ok(assistant.result);
                }
            }
        }

        // Structured output capture (Bun: `prompt.ts:1629-1634`). Once
        // the model has called the synthetic `StructuredOutput` tool, the
        // turn is logically complete — terminate the loop and let the
        // post-loop block write the structured payload onto the assistant
        // info. We force `last_finish = "stop"` so the terminal record
        // doesn't claim the model wanted more.
        if structured_capture
            .lock()
            .map(|slot| slot.is_some())
            .unwrap_or(false)
        {
            last_finish = Some("stop".to_string());
            break;
        }

        if plan_exit_this_iter {
            last_finish = Some("stop".to_string());
            break;
        }

        // Iteration termination policy. Bun's `streamText` continues the
        // loop while the model emits tool calls and stops when it emits
        // only text (the `tool_calls` finish-reason is the chat-completions
        // signal; the Responses API uses `response.completed` regardless,
        // so we key on "did the model produce any tool calls this turn?").
        if !had_tools_this_iter {
            break;
        }

        // Permission denial is a terminal tool outcome for this Rust parity
        // slice. Bun surfaces the denied tool result to the user instead of
        // blindly re-entering the model loop with a synthetic success path.
        if denied_this_iter {
            break;
        }

        // Doom-loop guard runs after provider output and the iteration's
        // step-finish have been persisted, so denying continuation does
        // not lose usage, finish reason, or provider errors from this step.
        if had_tools_this_iter {
            if let Some((dtool, dinput)) = detect_doom_loop(&tool_parts) {
                let synth_idx = tool_parts.len();
                let synth_call = format!("doom_loop_{iteration}");
                let metadata = json!({ "tool": dtool, "input": dinput });
                if crate::agent::permission::ask_doom_loop(
                    &state,
                    id,
                    &mid,
                    &pid,
                    synth_idx,
                    &dtool,
                    &synth_call,
                    &metadata,
                )
                .await
                .is_err()
                {
                    break;
                }
            }
        }

        if let Some(err) = malformed_this_iter {
            if !seen_malformed_tool_arguments.insert(err.signature) {
                let parts = assistant_parts_with(
                    &mid,
                    &pid,
                    &last_text,
                    &tool_parts,
                    Some(&total_usage),
                    last_finish.as_deref(),
                    id,
                );
                let assistant = state.store.append_message_record(
                    id,
                    MessageAppendInput {
                        info: assistant_error_info(
                            &paths,
                            &user,
                            &input,
                            malformed_tool_arguments_error(&err.err),
                        ),
                        parts,
                    },
                )?;
                publish_events(&state, dir, project, assistant.events);
                publish_error(&state, id, assistant.result.info["error"].clone());
                publish_idle(&state, id);
                publish_turn_close(&state, id, "error");
                return Ok(assistant.result);
            }
        }

        // Build the follow-up history slice the next iteration will send
        // to the Responses API. Bun's converter emits native
        // `function_call` and `function_call_output` input items; mirror
        // that shape instead of summarizing tools as prompt text.
        // Encrypted reasoning items captured this iteration are echoed
        // first so the Responses API can attach cache continuity to the
        // prior trace (`provider/sdk/copilot/responses/convert-to-openai-responses-input.ts:185-244`).
        if !this_iter_text.is_empty()
            || !provider_out.tool_calls.is_empty()
            || !encrypted_snapshot.is_empty()
        {
            let mut responses: Vec<ChatResponseItem> = encrypted_snapshot
                .iter()
                .cloned()
                .map(ChatResponseItem::Reasoning)
                .collect();
            responses.extend(
                provider_out
                    .tool_calls
                    .iter()
                    .cloned()
                    .map(ChatResponseItem::FunctionCall),
            );
            history_extension.push(ChatMessage {
                role: "assistant".to_string(),
                content: this_iter_text.clone(),
                responses,
                attachments: Vec::new(),
            });
        }
        let outputs = iter_tools_only
            .iter()
            .filter_map(tool_part_response)
            .collect::<Vec<_>>();
        if !outputs.is_empty() {
            history_extension.push(ChatMessage {
                role: "tool".to_string(),
                content: String::new(),
                responses: outputs,
                attachments: Vec::new(),
            });
        }

        // Last-iteration overflow guard — if we hit the cap with tools
        // still wanting more, return a MaxIterationsError envelope and
        // persist the partial assistant message.
        if iteration + 1 == OPENAI_OAUTH_MAX_ITERATIONS {
            let parts = assistant_parts_with(
                &mid,
                &pid,
                &last_text,
                &tool_parts,
                Some(&total_usage),
                last_finish.as_deref(),
                id,
            );
            let assistant = state.store.append_message_record(
                id,
                MessageAppendInput {
                    info: assistant_error_info(
                        &paths,
                        &user,
                        &input,
                        max_iterations_error(OPENAI_OAUTH_MAX_ITERATIONS),
                    ),
                    parts,
                },
            )?;
            publish_events(&state, dir, project, assistant.events);
            publish_error(&state, id, assistant.result.info["error"].clone());
            publish_idle(&state, id);
            publish_turn_close(&state, id, "error");
            return Ok(assistant.result);
        }
    }

    // Normal terminal path: write final parts + completed info on the
    // existing assistant message id (upsert).
    let final_text = if last_text.is_empty() {
        deltas
    } else {
        last_text
    };
    let mut parts = assistant_parts_with(
        &mid,
        &pid,
        &final_text,
        &tool_parts,
        Some(&total_usage),
        last_finish.as_deref(),
        id,
    );
    if let Some(part) = patch_part_for_snapshot(
        state.store.clone(),
        project.clone(),
        &user.info,
        id,
        &mid,
        &pid,
    )
    .await
    {
        if let Ok(record) = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: start.result.info.clone(),
                parts: vec![part.clone()],
            },
        ) {
            publish_events(&state, dir.clone(), project.clone(), record.events);
        }
        parts.push(part);
    }
    let captured_structured = structured_capture
        .lock()
        .ok()
        .and_then(|guard| guard.clone());

    // Structured-output failure mode (Bun: `prompt.ts:1637-1646`). Asked
    // for json_schema, the model finished without ever calling
    // StructuredOutput → the assistant message is failed with a stable
    // `StructuredOutputError` envelope so the SDK can surface it.
    if wants_structured && captured_structured.is_none() {
        let assistant = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: assistant_error_info(&paths, &user, &input, structured_output_error()),
                parts,
            },
        )?;
        publish_events(&state, dir, project, assistant.events);
        publish_error(&state, id, assistant.result.info["error"].clone());
        publish_idle(&state, id);
        publish_turn_close(&state, id, "error");
        return Ok(assistant.result);
    }

    let mut info = assistant_stream_completed_info(
        &start.result,
        input.model.as_ref(),
        Some(&total_usage),
        last_finish.as_deref(),
    );
    if let Some(existing) = state
        .store
        .message(id, &mid)
        .and_then(|message| message.info.get("cost").and_then(Value::as_f64))
        .filter(|cost| cost.is_finite() && *cost > 0.0)
    {
        let own = info.get("cost").and_then(Value::as_f64).unwrap_or(0.0);
        info["cost"] = json!(existing + own);
    }
    if let Some(structured) = captured_structured {
        info["structured"] = structured;
    }
    let assistant = state
        .store
        .append_message_record(id, MessageAppendInput { info, parts })?;
    publish_events(&state, dir, project, assistant.events);
    publish_idle(&state, id);
    publish_turn_close(&state, id, "completed");

    Ok(assistant.result)
}

async fn patch_part_for_snapshot(
    store: kilo_store::Store,
    project: String,
    info: &Value,
    sid: &str,
    mid: &str,
    pid: &str,
) -> Option<Value> {
    let base = info.get("snapshot").and_then(Value::as_str)?.to_string();
    let patch =
        tokio::task::spawn_blocking(move || crate::snapshot::patch(&store, &project, &base))
            .await
            .ok()
            .and_then(Result::ok)?;
    if patch.files.is_empty() {
        return None;
    }
    Some(json!({
        "id": format!("{pid}_patch"),
        "messageID": mid,
        "sessionID": sid,
        "type": "patch",
        "hash": patch.hash,
        "files": patch.files,
    }))
}

fn pending_abort_parts(
    mid: &str,
    pid: &str,
    time: i64,
    pending: &Arc<std::sync::Mutex<Vec<PendingTool>>>,
) -> Vec<Value> {
    pending
        .lock()
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    tool_error(
                        mid,
                        pid,
                        item.idx,
                        &item.tool,
                        &item.call,
                        &item.input,
                        "Tool call aborted".to_string(),
                        time,
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn missing_tool_output_parts(
    mid: &str,
    pid: &str,
    time: i64,
    pending: &Arc<std::sync::Mutex<Vec<PendingTool>>>,
    settled: &[Value],
) -> Vec<Value> {
    let done = settled
        .iter()
        .filter_map(|part| part.get("callID").and_then(Value::as_str))
        .collect::<HashSet<_>>();
    pending
        .lock()
        .map(|items| {
            items
                .iter()
                .filter(|item| !done.contains(item.call.as_str()))
                .map(|item| {
                    tool_error(
                        mid,
                        pid,
                        item.idx,
                        &item.tool,
                        &item.call,
                        &item.input,
                        "Tool call did not return an output".to_string(),
                        time,
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Extract `(tool, input_json)` from a completed tool part, or `None` if
/// the part isn't a settled tool result. Pending/running tool parts and
/// non-tool parts (text, step-start, step-finish) are excluded so a still
/// in-flight call doesn't trip the doom-loop guard.
fn doom_loop_signature(part: &Value) -> Option<(String, String)> {
    if part.get("type").and_then(Value::as_str) != Some("tool") {
        return None;
    }
    let status = part
        .pointer("/state/status")
        .and_then(Value::as_str)
        .unwrap_or("");
    if status == "pending" || status == "running" {
        return None;
    }
    let tool = part.get("tool").and_then(Value::as_str)?.to_string();
    let input = part
        .pointer("/state/input")
        .map(|v| v.to_string())
        .unwrap_or_default();
    Some((tool, input))
}

/// Returns `Some((tool_name, input_json_value))` when the tail of `parts`
/// contains [`DOOM_LOOP_THRESHOLD`] consecutive identical tool calls,
/// matching Bun's `processor.ts:357-380` heuristic. `None` otherwise.
pub(crate) fn detect_doom_loop(parts: &[Value]) -> Option<(String, Value)> {
    let mut tail: Vec<(String, String)> = Vec::with_capacity(DOOM_LOOP_THRESHOLD);
    for part in parts.iter().rev() {
        if let Some(sig) = doom_loop_signature(part) {
            tail.push(sig);
            if tail.len() == DOOM_LOOP_THRESHOLD {
                break;
            }
        } else if part.get("type").and_then(Value::as_str) == Some("tool") {
            // a still-pending tool breaks the run of identical completions
            return None;
        }
    }
    if tail.len() < DOOM_LOOP_THRESHOLD {
        return None;
    }
    if !tail.windows(2).all(|w| w[0] == w[1]) {
        return None;
    }
    let last_part = parts.iter().rev().find(|p| {
        p.get("type").and_then(Value::as_str) == Some("tool") && doom_loop_signature(p).is_some()
    })?;
    let tool = last_part
        .get("tool")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let input = last_part
        .pointer("/state/input")
        .cloned()
        .unwrap_or(Value::Null);
    Some((tool, input))
}

fn spawn_title_generation(
    state: Arc<AppState>,
    id: String,
    cfg: kilo_protocol::Config,
    auths: Value,
    model: Option<Value>,
    text: String,
    cancel: Arc<AtomicBool>,
) {
    if has_openai_base_override(&cfg) || !should_generate_title(&state, &id) {
        return;
    }
    tokio::spawn(async move {
        let msg = format!("Generate a title for this conversation:\n\n{}", text.trim());
        let out = kilo_provider::chat_tools_with_auth_cancel(
            &cfg,
            &auths,
            model.as_ref(),
            Some(TITLE_PROMPT.to_string()),
            vec![ChatMessage {
                role: "user".to_string(),
                content: msg,
                responses: Vec::new(),
                attachments: Vec::new(),
            }],
            Vec::new(),
            &cancel,
        )
        .await;
        let Ok(out) = out else {
            return;
        };
        let Some(title) = clean_title(&out.text) else {
            return;
        };
        if !should_generate_title(&state, &id) {
            return;
        }
        let updated = state.store.update_session(
            &id,
            SessionUpdateInput {
                title: Some(title),
                permission: None,
                time: None,
            },
        );
        if let Ok(Some(session)) = updated {
            crate::http::sse::publish(
                &state,
                GlobalEvent::session(
                    "session.updated",
                    Arc::from(state.store.paths().directory.as_str()),
                    session,
                ),
            );
        }
    });
}

fn has_openai_base_override(cfg: &kilo_protocol::Config) -> bool {
    cfg.data
        .get("provider")
        .and_then(Value::as_object)
        .and_then(|providers| providers.get("openai"))
        .and_then(Value::as_object)
        .and_then(|openai| openai.get("options"))
        .and_then(Value::as_object)
        .and_then(|options| options.get("baseURL"))
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
}

pub(crate) fn should_generate_title(state: &AppState, id: &str) -> bool {
    let Some(session) = state.store.session(id) else {
        return false;
    };
    if session.parent_id.is_some() || !is_default_session_title(&session.title) {
        return false;
    }
    let Some(page) = state.store.messages(id, None, None) else {
        return false;
    };
    page.items
        .iter()
        .filter(|msg| is_real_user_message(msg))
        .count()
        == 1
}

fn is_real_user_message(msg: &kilo_protocol::Message) -> bool {
    if msg.info.get("role").and_then(Value::as_str) != Some("user") {
        return false;
    }
    !msg.parts.iter().all(|part| part.get("synthetic").is_some())
}

pub(crate) fn is_default_session_title(title: &str) -> bool {
    ["New session - ", "Child session - "]
        .iter()
        .filter_map(|prefix| title.strip_prefix(prefix))
        .any(|rest| rest.ends_with('Z') && chrono::DateTime::parse_from_rfc3339(rest).is_ok())
}

pub(crate) fn clean_title(text: &str) -> Option<String> {
    let mut text = text.to_string();
    while let Some(start) = text.find("<think>") {
        let after = start + "<think>".len();
        let Some(end) = text[after..].find("</think>") else {
            text.truncate(start);
            break;
        };
        let end = after + end + "</think>".len();
        text.replace_range(start..end, "");
    }
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(|line| {
            let chars = line.chars().collect::<Vec<_>>();
            if chars.len() > 100 {
                format!("{}...", chars.iter().take(97).collect::<String>())
            } else {
                line.to_string()
            }
        })
}

fn live_tool_name(raw: &str) -> String {
    match repair_tool_name(raw, KNOWN_TOOLS) {
        Repair::Valid(name) => name,
        Repair::Invalid(_) => raw.to_string(),
    }
}

fn is_permission_denial(part: &Value) -> bool {
    part.get("state")
        .and_then(|state| state.get("metadata"))
        .and_then(|metadata| metadata.get("error"))
        .and_then(|error| error.get("name"))
        .and_then(Value::as_str)
        == Some("PermissionRejectedError")
}

fn is_plan_exit(part: &Value) -> bool {
    part.get("type").and_then(Value::as_str) == Some("tool")
        && part.get("tool").and_then(Value::as_str) == Some("plan_exit")
        && part
            .get("state")
            .and_then(|state| state.get("status"))
            .and_then(Value::as_str)
            == Some("completed")
}

struct MalformedToolArguments {
    err: String,
    signature: String,
}

fn malformed_tool_arguments(part: &Value) -> Option<MalformedToolArguments> {
    let err = part
        .get("state")
        .and_then(|state| state.get("error"))
        .and_then(Value::as_str)?
        .to_string();
    if part.get("tool").and_then(Value::as_str) == Some("invalid")
        && err.contains("Invalid tool arguments JSON")
    {
        let tool = part
            .get("state")
            .and_then(|state| state.get("input"))
            .and_then(|input| input.get("tool"))
            .and_then(Value::as_str)
            .unwrap_or("invalid");
        let raw = part
            .get("state")
            .and_then(|state| state.get("input"))
            .and_then(|input| input.get("arguments"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        return Some(MalformedToolArguments {
            signature: format!("{tool}\0{err}\0{raw}"),
            err,
        });
    }
    None
}

fn malformed_tool_arguments_error(err: &str) -> Value {
    json!({
        "name": "MalformedToolArgumentsError",
        "data": {
            "message": format!(
                "OpenAI OAuth stream returned malformed tool arguments twice in the same turn: {err}"
            ),
            "isRetryable": false,
            "metadata": { "source": "rust-openai-oauth" }
        }
    })
}

/// Stable envelope returned when a `format: json_schema` turn ended
/// without the model ever invoking the synthesized `StructuredOutput`
/// tool. Mirrors Bun's `MessageV2.StructuredOutputError`.
fn structured_output_error() -> Value {
    json!({
        "name": "StructuredOutputError",
        "data": {
            "message": "The model did not call the StructuredOutput tool. Structured output is required when format.type is json_schema.",
            "isRetryable": true,
            "metadata": { "source": "rust-openai-oauth" }
        }
    })
}

/// Snapshot the runner's mid-stream retry counter, or `None` when no
/// runner is registered for the session (test paths that drive the
/// agent loop without a `RunnerGuard` see `None` and skip the cap).
fn mid_stream_retry_counter(state: &AppState, id: &str) -> Option<Arc<AtomicU8>> {
    state
        .runners
        .lock()
        .ok()?
        .get(id)
        .map(|runner| runner.mid_stream_retries.clone())
}

/// Persist an aborted OAuth turn IN-PLACE on the existing assistant
/// message. Mirrors Bun's `run-state.ts:48-68 onInterrupt`: one assistant
/// record per cancelled turn, carrying whatever partial deltas and
/// completed tool-result parts had streamed before the abort. Re-issuing
/// `append_message_record` with the same `info.id` and explicit `id`
/// fields on each part triggers the store's upsert path.
///
/// Branches on the active runner's `follow_up_break` flag. When
/// `prompt_async` arrives mid-turn for the same session, the route trips
/// both `cancel` and `follow_up_break`; we then stamp `finish: "follow_up"`,
/// omit the error envelope, and skip `publish_error` so the webview does
/// not surface an error toast for an intentional break. A regular
/// user-driven abort (`abort_session`) leaves `follow_up_break` clear —
/// the turn finalizes with the canonical `MessageAbortedError` envelope
/// and `publish_turn_close(_, "interrupted")`.
#[allow(clippy::too_many_arguments)]
fn finalize_openai_aborted(
    state: &AppState,
    id: &str,
    dir: &str,
    project: &str,
    start: &MessageAppendResult,
    mid: &str,
    pid: &str,
    deltas: &str,
    tool_parts: &[Value],
    usage: Option<&ChatUsage>,
) -> rusqlite::Result<MessageAppendResult> {
    let follow_up = state
        .runners
        .lock()
        .unwrap()
        .get(id)
        .map(|runner| runner.follow_up_break.load(Ordering::SeqCst))
        .unwrap_or(false);

    let mut info = start.info.clone();
    if follow_up {
        info["finish"] = json!("follow_up");
        info.as_object_mut().map(|map| map.remove("error"));
    } else {
        info["error"] = aborted_error();
        info["finish"] = json!("aborted");
    }
    if let Some(usage) = usage {
        info["tokens"] = tokens_value(usage);
    }
    if let Some(time) = info.get_mut("time").and_then(Value::as_object_mut) {
        time.insert("updated".to_string(), json!(start.time));
        time.insert("completed".to_string(), json!(start.time));
    }
    let mut parts = Vec::new();
    if !deltas.is_empty() {
        parts.push(json!({
            "id": pid,
            "type": "text",
            "text": deltas,
        }));
    }
    parts.extend(tool_parts.iter().cloned());

    let assistant = state
        .store
        .append_message_record(id, MessageAppendInput { info, parts })?;
    publish_events(
        &state,
        dir.to_string(),
        project.to_string(),
        assistant.events,
    );
    if follow_up {
        publish_idle(state, id);
        publish_turn_close(state, id, "follow_up");
    } else {
        publish_error(state, id, assistant.result.info["error"].clone());
        publish_idle(state, id);
        publish_turn_close(state, id, "interrupted");
    }

    // Drop the unused pre-existing message id binding silently — the
    // `mid` and `start.info["id"]` are the same value, but this fn uses
    // `start.info` directly for the upsert. Suppress unused warning.
    let _ = mid;

    Ok(assistant.result)
}

/// Compose the final assistant parts list from accumulated text + tool
/// parts. The per-iteration `step-start` / `step-finish` parts are already
/// in `tool_parts` (appended inline by the loop body), so we no longer
/// add a trailing terminal `step-finish` here — Bun emits one `finish-step`
/// per iteration (`processor.ts:414-473`), not one per turn.
#[allow(unused_variables)] // mid/usage/finish/sid kept for API stability with error paths
fn assistant_parts_with(
    mid: &str,
    pid: &str,
    text: &str,
    tool_parts: &[Value],
    usage: Option<&ChatUsage>,
    finish: Option<&str>,
    sid: &str,
) -> Vec<Value> {
    let mut parts = Vec::new();
    parts.extend(tool_parts.iter().cloned());
    if !text.is_empty() && !parts.iter().any(is_nonempty_text_part) {
        parts.push(text_part(pid, text));
    }
    if parts.is_empty() {
        parts.push(text_part(pid, ""));
    }
    parts
}

fn iteration_text_part_id(pid: &str, iteration: usize) -> String {
    if iteration == 0 {
        return pid.to_string();
    }
    format!("{pid}_text_{iteration}")
}

fn text_part(pid: &str, text: &str) -> Value {
    json!({
        "id": pid,
        "type": "text",
        "text": text,
    })
}

fn reasoning_part_id(pid: &str, iteration: usize, id: &str) -> String {
    let clean = id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("{pid}_reasoning_{iteration}_{clean}")
}

fn reasoning_part(pid: &str, text: &str, start: i64, end: Option<i64>) -> Value {
    reasoning_part_with(pid, text, start, end, "", None)
}

/// Like [`reasoning_part`] but also stamps the upstream item id
/// (`itemID`) and verbatim `encryptedContent` blob so a future
/// persistence-driven reload can rebuild the encrypted reasoning items
/// for cache-hit replay. Empty `item_id` / `encrypted` are skipped so
/// non-reasoning models don't get a payload bloat.
fn reasoning_part_with(
    pid: &str,
    text: &str,
    start: i64,
    end: Option<i64>,
    item_id: &str,
    encrypted: Option<&str>,
) -> Value {
    let mut part = json!({
        "id": pid,
        "type": "reasoning",
        "text": text,
        "time": { "start": start },
    });
    if let Some(end) = end {
        part["time"]["end"] = json!(end);
    }
    if !item_id.is_empty() {
        part["itemID"] = json!(item_id);
    }
    if let Some(blob) = encrypted.filter(|text| !text.is_empty()) {
        part["encryptedContent"] = json!(blob);
    }
    part
}

fn is_nonempty_text_part(part: &Value) -> bool {
    part.get("type").and_then(Value::as_str) == Some("text")
        && part
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.is_empty())
}

pub(crate) fn model_provider(model: Option<&Value>) -> Option<&str> {
    let model = model?;
    model
        .get("providerID")
        .or_else(|| model.get("provider"))
        .or_else(|| model.get("providerId"))
        .and_then(Value::as_str)
}

pub(crate) fn model_name(model: Option<&Value>) -> Option<&str> {
    let model = model?;
    model
        .get("modelID")
        .or_else(|| model.get("model"))
        .or_else(|| model.get("modelId"))
        .or_else(|| model.get("id"))
        .and_then(Value::as_str)
}

/// Bun-parity proactive compaction threshold. Mirrors
/// [`packages/opencode/src/session/overflow.ts:19-26`](../../../../../opencode/src/session/overflow.ts):
/// `count >= context * (1 - reserve_fraction)` where the reserve roughly
/// matches Bun's `min(20_000, model.limit.output)`. We keep this as a
/// single fraction (0.85) per the task spec instead of recomputing the
/// reserve per request — it's a conservative, threshold-only check.
pub(crate) const PROACTIVE_COMPACTION_THRESHOLD: f64 = 0.85;

/// Resolve the model's context-window limit. Walks (in order):
///
/// 1. `model.limit.context` if the caller passed an enriched model record;
/// 2. the OpenAI registry shipped in `kilo_provider::openai_models` —
///    looked up by `modelID`; OR
/// 3. a conservative fallback derived from the id pattern. Bun does the
///    same fallback shape inside the Provider model loader.
///
/// Returns `0` when nothing is known — callers treat `0` as "skip the
/// proactive check" (matches Bun's `overflow.ts:21`).
pub(crate) fn model_context_limit(model: Option<&Value>) -> u64 {
    if let Some(model) = model {
        if let Some(ctx) = model
            .pointer("/limit/context")
            .and_then(Value::as_u64)
            .filter(|value| *value > 0)
        {
            return ctx;
        }
    }
    let id = model_name(model).unwrap_or_default();
    if id.is_empty() {
        return 0;
    }
    if let Some(ctx) = kilo_provider::openai_models::registry("")
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(id))
        .and_then(|(_, value)| value.pointer("/limit/context").and_then(Value::as_u64))
        .filter(|value| *value > 0)
    {
        return ctx;
    }
    let lower = id.to_ascii_lowercase();
    if lower.starts_with("gpt-5") || lower.starts_with("gpt-4") {
        return 200_000;
    }
    if lower.starts_with("o1") {
        return 128_000;
    }
    32_000
}

/// Read `cfg.compaction.auto` — `false` opts out of proactive compaction.
/// Default (missing key, non-bool) is `true`, mirroring Bun's
/// `overflow.ts:20` short-circuit.
pub(crate) fn compaction_auto_enabled(cfg: &kilo_protocol::Config) -> bool {
    cfg.data
        .get("compaction")
        .and_then(|value| value.get("auto"))
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

/// Returns `true` when the cumulative iteration tokens have crossed the
/// proactive compaction threshold. Bun keys on `tokens.input + output +
/// cache.read + cache.write` (`overflow.ts:24`); we use the same
/// components from [`ChatUsage`].
pub(crate) fn should_compact_proactively(
    cfg: &kilo_protocol::Config,
    model: Option<&Value>,
    usage: &ChatUsage,
) -> bool {
    if !compaction_auto_enabled(cfg) {
        return false;
    }
    let context = model_context_limit(model);
    if context == 0 {
        return false;
    }
    let count = if usage.total > 0 {
        usage.total
    } else {
        usage
            .input
            .saturating_add(usage.output)
            .saturating_add(usage.cache_read)
            .saturating_add(usage.cache_write)
    };
    (count as f64) >= (context as f64) * PROACTIVE_COMPACTION_THRESHOLD
}

pub(crate) fn is_openai_oauth(state: &AppState, model: Option<&Value>) -> bool {
    let provider = model_provider(model);
    let auth_oauth = state
        .store
        .provider_auth("openai")
        .and_then(|value| {
            value
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .as_deref()
        == Some("oauth");
    provider == Some("openai") && (auth_oauth || env_openai_oauth())
}

fn env_openai_oauth() -> bool {
    std::env::var("KILO_AUTH_CONTENT")
        .ok()
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .and_then(|value| {
            value
                .get("openai")
                .and_then(|auth| auth.get("type"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .as_deref()
        == Some("oauth")
}

/// Composes the `instructions` field for the OAuth Responses API call per
/// [`llm.ts:155-159`](../../../../../opencode/src/session/llm.ts:155): the
/// soul prompt is prepended to the joined system strings.
///
/// Source of truth: `packages/opencode/src/kilocode/soul.txt`. The Bun path
/// reads it via `SOUL.trim()` at
/// [`session/system.ts:32-34`](../../../../../opencode/src/session/system.ts:32);
/// we embed the file at compile time so the Rust sidecar carries the same
/// bytes without a filesystem dependency.
pub(crate) const OPENAI_OAUTH_SOUL_RAW: &str =
    include_str!("../../../../../opencode/src/kilocode/soul.txt");

/// Codex provider system prompt. For OAuth Codex models this is the
/// `agent.prompt | provider(model)` slot in
/// [`session/llm.ts:115`](../../../../../opencode/src/session/llm.ts:115)
/// (`SystemPrompt.provider(model)` returns `[PROMPT_CODEX]` when the model
/// id contains `codex` — see
/// [`session/system.ts:60-61`](../../../../../opencode/src/session/system.ts:60))
/// AND it appears again in `instruction.system()` per
/// [`session/prompt.ts:1605-1611`](../../../../../opencode/src/session/prompt.ts:1605).
/// We embed once and place it in the provider slot only; the second
/// occurrence Bun produces is incidental (`provider()` and
/// `instructions()` both surface the same bytes).
pub(crate) const OPENAI_OAUTH_CODEX_RAW: &str =
    include_str!("../../../../../opencode/src/session/prompt/codex.txt");
const STRUCTURED_OUTPUT_SYSTEM_PROMPT: &str = "IMPORTANT: The user has requested structured output. You MUST use the StructuredOutput tool to provide your final response. Do NOT respond with plain text - you MUST call the StructuredOutput tool with your answer formatted according to the schema.";

/// Compose the OAuth `instructions` body Bun assembles at
/// [`session/llm.ts:108-159`](../../../../../opencode/src/session/llm.ts:108)
/// + [`session/prompt.ts:1605-1611`](../../../../../opencode/src/session/prompt.ts:1605):
///
/// 1. Soul prompt (Kilo identity), prepended for OAuth at `llm.ts:157`.
/// 2. Provider system prompt — for Codex models this is `codex.txt`,
///    chosen by `SystemPrompt.provider()` at `system.ts:60-61`.
/// 3. Environment block: model id, working dir, platform, date — built
///    by `SystemPrompt.environment` at `system.ts:86-105`. Bun injects
///    this into `input.system` server-side at `prompt.ts:1611`.
/// 4. Caller-supplied `input.system` string(s) (the SDK's `system`
///    parameter at `sdk.gen.ts:2541`).
/// 5. `STRUCTURED_OUTPUT_SYSTEM_PROMPT` when the format is json_schema
///    (kilocode-specific addition at `llm.ts:1613`).
///
/// `working_dir` is the request directory the agent is operating in — when
/// `Some`, the env block is included; when `None`, it's omitted (test-only
/// path).
pub(crate) fn prompt_instructions(
    input: &PromptInput,
    working_dir: Option<&str>,
) -> Option<String> {
    prompt_instructions_with_root(input, working_dir, None, None, None)
}

pub(crate) fn prompt_instructions_with_root(
    input: &PromptInput,
    working_dir: Option<&str>,
    config_dir: Option<&str>,
    home_dir: Option<&str>,
    agent_prompt: Option<&str>,
) -> Option<String> {
    let mut pieces: Vec<String> = vec![OPENAI_OAUTH_SOUL_RAW.trim().to_string()];
    match agent_prompt
        .map(str::trim)
        .filter(|prompt| !prompt.is_empty())
    {
        Some(prompt) => pieces.push(prompt.to_string()),
        None => pieces.push(OPENAI_OAUTH_CODEX_RAW.trim().to_string()),
    }
    if let Some(dir) = working_dir {
        pieces.push(env_block(input, dir));
        let fallback = Path::new(dir).join(".kilo");
        let config = config_dir.map(PathBuf::from).unwrap_or(fallback);
        let home = home_dir
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(dir));
        let skills = registry::skills(Path::new(dir), &config, &home);
        if let Some(text) = registry::skills_prompt(&skills) {
            pieces.push(text);
        }
    }
    if let Some(value) = input.system.as_ref() {
        match value {
            Value::String(text) => {
                if !text.is_empty() {
                    pieces.push(text.clone());
                }
            }
            Value::Array(items) => {
                for item in items {
                    if let Some(text) = item.as_str() {
                        if !text.is_empty() {
                            pieces.push(text.to_string());
                        }
                    }
                }
            }
            Value::Object(_) => pieces.push(value.to_string()),
            _ => {}
        }
    }
    if format_type(input.format.as_ref()) == Some("json_schema") {
        pieces.push(STRUCTURED_OUTPUT_SYSTEM_PROMPT.to_string());
    }
    Some(pieces.join("\n"))
}

fn agent_prompt(state: &AppState, input: &PromptInput) -> Option<String> {
    input
        .agent
        .as_deref()
        .and_then(|agent| state.agent_info(agent))
        .and_then(|agent| {
            agent
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

/// Build the `<env>` block Bun emits at
/// [`session/system.ts:86-105`](../../../../../opencode/src/session/system.ts:86):
/// model attribution line, then a `<env>...</env>` section with working
/// directory, platform, date, and config-path lines. Editor context lines
/// (`staticEnvLines(editorContext)`) are deferred — Rust's `EditorContext`
/// shape isn't migrated yet.
fn env_block(input: &PromptInput, working_dir: &str) -> String {
    let provider = model_provider(input.model.as_ref()).unwrap_or("openai");
    let model = model_name(input.model.as_ref()).unwrap_or("gpt-5.1-codex");
    let platform = std::env::consts::OS;
    // Mirrors `new Date().toDateString()` — Sun May 02 2026 — the format
    // Bun uses verbatim. chrono's `format("%a %b %d %Y")` produces the same
    // shape ("Sun May 02 2026").
    let today = chrono::Local::now().format("%a %b %d %Y").to_string();
    let shell = input
        .editor_context
        .as_ref()
        .and_then(|ctx| ctx.get("shell"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!("\n  Default shell: {value}"))
        .unwrap_or_default();
    format!(
        "You are powered by the model named {model}. The exact model ID is {provider}/{model}\n\
         Here is some useful information about the environment you are running in:\n\
         <env>\n  \
           Working directory: {working_dir}\n  \
           Platform: {platform}\n  \
           Today's date: {today}\n  \
           Optional project config: AGENTS.md, kilo.json[c], .kilo/kilo.json[c], .kilo/command/*.md, .kilo/agent/*.md. Do not assume optional config files exist; list/glob before reading them. Put new commands and agents in .kilo/. Do not use .kilocode/ or .opencode/.{shell}\n\
         </env>",
    )
}

fn format_type(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str)
}

fn assistant_openai_info(
    paths: &KiloPath,
    user: &MessageAppendResult,
    input: &PromptInput,
) -> Value {
    let agent = input.agent.as_deref().unwrap_or("code");
    let model = input
        .model
        .as_ref()
        .and_then(|value| {
            value
                .get("modelID")
                .or_else(|| value.get("model"))
                .or_else(|| value.get("id"))
        })
        .and_then(Value::as_str)
        .unwrap_or("gpt-5.1-codex");
    json!({
        "role": "assistant",
        "parentID": user.info["id"].clone(),
        "providerID": "openai",
        "modelID": model,
        "agent": agent,
        "path": crate::agent::parts::assistant_path(paths),
        "cost": 0,
        "tokens": zero_tokens(),
    })
}

fn assistant_stream_completed_info(
    start: &MessageAppendResult,
    model: Option<&Value>,
    usage: Option<&ChatUsage>,
    finish: Option<&str>,
) -> Value {
    let mut info = assistant_completed_info(start);
    info["finish"] = json!(finish.unwrap_or("stop"));
    if let Some(usage) = usage {
        info["tokens"] = tokens_value(usage);
        info["cost"] = usage_cost_value(model, usage);
    }
    info
}

pub(crate) fn zero_tokens() -> Value {
    json!({
        "input": 0,
        "output": 0,
        "reasoning": 0,
        "cache": { "read": 0, "write": 0 }
    })
}

#[cfg(test)]
pub(crate) fn has_tool_calls(messages: &[Value]) -> bool {
    messages.iter().any(|message| has_tool_call(message))
}

#[cfg(test)]
pub(crate) fn has_tool_call(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            map.get("toolCalls")
                .and_then(Value::as_array)
                .is_some_and(|items| !items.is_empty())
                || map
                    .get("tool_calls")
                    .and_then(Value::as_array)
                    .is_some_and(|items| !items.is_empty())
                || map
                    .get("parts")
                    .and_then(Value::as_array)
                    .is_some_and(|parts| parts.iter().any(has_tool_call))
                || map
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind == "tool" || kind == "tool-call")
        }
        Value::Array(items) => items.iter().any(has_tool_call),
        _ => false,
    }
}

#[cfg(test)]
pub(crate) fn should_inject_noop(
    provider: &str,
    lite: bool,
    tools: &[ChatTool],
    messages: &[Value],
) -> bool {
    (lite || provider.contains("github-copilot")) && tools.is_empty() && has_tool_calls(messages)
}

#[cfg(test)]
mod doom_loop_tests {
    use super::*;

    fn tool_part(tool: &str, input: Value) -> Value {
        json!({
            "type": "tool",
            "tool": tool,
            "state": { "status": "completed", "input": input },
        })
    }

    #[test]
    fn detect_doom_loop_fires_on_three_identical_completed_tool_calls() {
        let parts = vec![
            tool_part("bash", json!({ "cmd": "ls" })),
            tool_part("bash", json!({ "cmd": "ls" })),
            tool_part("bash", json!({ "cmd": "ls" })),
        ];
        let hit = detect_doom_loop(&parts).expect("should detect");
        assert_eq!(hit.0, "bash");
        assert_eq!(hit.1, json!({ "cmd": "ls" }));
    }

    #[test]
    fn detect_doom_loop_ignores_text_and_step_parts_in_between() {
        let parts = vec![
            json!({ "type": "text", "text": "considering" }),
            tool_part("bash", json!({ "cmd": "ls" })),
            json!({ "type": "step-finish", "tokens": { "total": 10 } }),
            json!({ "type": "step-start" }),
            tool_part("bash", json!({ "cmd": "ls" })),
            json!({ "type": "step-finish", "tokens": { "total": 10 } }),
            json!({ "type": "step-start" }),
            tool_part("bash", json!({ "cmd": "ls" })),
        ];
        let hit = detect_doom_loop(&parts).expect("text/step parts should not break the streak");
        assert_eq!(hit.0, "bash");
    }

    #[test]
    fn detect_doom_loop_skips_when_inputs_differ() {
        let parts = vec![
            tool_part("bash", json!({ "cmd": "ls" })),
            tool_part("bash", json!({ "cmd": "ls -la" })),
            tool_part("bash", json!({ "cmd": "ls" })),
        ];
        assert!(detect_doom_loop(&parts).is_none());
    }

    #[test]
    fn detect_doom_loop_skips_when_tools_differ() {
        let parts = vec![
            tool_part("bash", json!({ "cmd": "ls" })),
            tool_part("read", json!({ "filePath": "x" })),
            tool_part("bash", json!({ "cmd": "ls" })),
        ];
        assert!(detect_doom_loop(&parts).is_none());
    }

    #[test]
    fn detect_doom_loop_skips_when_pending_tool_in_window() {
        let parts = vec![
            tool_part("bash", json!({ "cmd": "ls" })),
            tool_part("bash", json!({ "cmd": "ls" })),
            json!({
                "type": "tool",
                "tool": "bash",
                "state": { "status": "running", "input": { "cmd": "ls" } }
            }),
        ];
        assert!(detect_doom_loop(&parts).is_none());
    }

    #[test]
    fn detect_doom_loop_no_match_under_threshold() {
        let parts = vec![
            tool_part("bash", json!({ "cmd": "ls" })),
            tool_part("bash", json!({ "cmd": "ls" })),
        ];
        assert!(detect_doom_loop(&parts).is_none());
    }

    #[test]
    fn structured_output_error_envelope_has_stable_name() {
        let err = structured_output_error();
        assert_eq!(err["name"], "StructuredOutputError");
        assert_eq!(err["data"]["isRetryable"], true);
        assert!(err["data"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("StructuredOutput"));
    }
}
