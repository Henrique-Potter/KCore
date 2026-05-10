//! Context-window compaction (Bun parity:
//! [`packages/opencode/src/session/compaction.ts`](../../../../../opencode/src/session/compaction.ts)
//! plus the loop integration in `prompt.ts:1495-1515` and `1654-1675`).
//!
//! When the model rejects a turn with a context-length error
//! ([`kilo_provider::ProviderError::ContextWindow`]), the agent loop
//! invokes [`compact_session`] which:
//!
//! 1. Reads the entire transcript from the store.
//! 2. Asks a non-streaming provider call to summarize it.
//! 3. Writes the summary back as a single assistant-role message with
//!    `info.summary == true`.
//! 4. Subsequent calls to [`crate::agent::parts::real_messages`] include
//!    only messages from the latest summary anchor onward, so the next
//!    iteration sends the model a much smaller context window.
//!
//! The Rust port intentionally diverges from Bun's full
//! `compaction.create` flow (which has prune-by-token, summary diffs,
//! and per-message `summary: true` flags) — we surface the same outer
//! contract (overflow → summarize → retry) without the prune layer.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use kilo_protocol::{GlobalEvent, MessageAppendInput};
use kilo_provider::{chat_tools_with_auth_cancel, ChatMessage, ChatTool, ProviderError};
use serde_json::{json, Value};

use crate::http::sse;
use crate::AppState;

const SUMMARY_PROMPT: &str = "You are a session summarizer. The conversation below exceeded the model's context window and must be condensed to fit. Produce a concise, factual summary that preserves: \
(1) the user's original goal and any constraints the user imposed; \
(2) every file path / identifier / configuration value the agent needed to remember; \
(3) the latest in-progress task or open question; \
(4) any decisions the agent made that should not be revisited. \
Drop verbatim tool I/O, intermediate reasoning, and stylistic chatter. Output the summary as plain prose under 1000 words. Do NOT call any tools.";

/// Hard cap on consecutive compaction attempts within a single turn.
/// Mirrors Bun's `KiloSessionPrompt.guardCompactionAttempt` behavior
/// (`packages/opencode/src/kilocode/session/prompt.ts`) — stops the
/// loop from spinning if the post-summary context still overflows.
pub(crate) const MAX_COMPACTION_ATTEMPTS: usize = 3;

/// Build the compact-version of the session history, ask the provider
/// for a summary, and write it back to the store as the new anchor.
/// Returns the summary text on success.
///
/// The provider call uses the same auth + model as the live turn but
/// goes through the **non-streaming** `chat_tools_with_auth` path with
/// an empty tools array — summarization must not produce tool calls
/// even if the underlying model is tool-capable.
pub(crate) async fn compact_session(
    state: &Arc<AppState>,
    sid: &str,
    model: Option<&Value>,
    auths: &Value,
    cancel: &AtomicBool,
) -> Result<String, CompactionError> {
    if cancel.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(CompactionError::Cancelled);
    }
    // Move the (potentially large) sqlite transcript read off the runtime
    // executor thread. The store/sqlite call itself can't be cancelled,
    // but if Stop fires while it's in flight we observe it on the way out
    // and bail before the (much more expensive) provider call.
    let st = state.clone();
    let sid_owned = sid.to_string();
    let history = tokio::task::spawn_blocking(move || collect_history(&st, &sid_owned))
        .await
        .map_err(|err| CompactionError::Provider(ProviderError::Http(err.to_string())))?;
    if cancel.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(CompactionError::Cancelled);
    }
    if history.is_empty() {
        return Err(CompactionError::EmptyHistory);
    }
    let cfg = state.store.config();
    let messages = vec![ChatMessage {
        role: "user".to_string(),
        content: format_history_for_summary(&history),
        responses: Vec::new(),
        attachments: Vec::new(),
    }];
    let out = chat_tools_with_auth_cancel(
        &cfg,
        auths,
        model,
        Some(SUMMARY_PROMPT.to_string()),
        messages,
        Vec::<ChatTool>::new(),
        cancel,
    )
    .await
    .map_err(|err| match err {
        ProviderError::Aborted => CompactionError::Cancelled,
        other => CompactionError::Provider(other),
    })?;

    if out.text.is_empty() {
        return Err(CompactionError::EmptyResult);
    }
    record_summary(state, sid, &out.text).map_err(CompactionError::Store)?;
    sse::publish(
        state,
        GlobalEvent::bus("session.compaction.compacted", json!({ "sessionID": sid })),
    );
    Ok(out.text)
}

/// Append a synthetic assistant-role message tagged with `summary: true`
/// to the session. [`crate::agent::parts::real_messages`] uses this as
/// the new anchor — every message before it is dropped on the next read.
pub(crate) fn record_summary(state: &AppState, sid: &str, text: &str) -> rusqlite::Result<()> {
    let now = chrono::Utc::now().timestamp_millis();
    let id = format!("msg_summary_{now}");
    let info = json!({
        "id": id,
        "role": "assistant",
        "sessionID": sid,
        "summary": true,
        "time": { "created": now, "updated": now, "completed": now },
        "tokens": { "input": 0, "output": 0, "reasoning": 0, "total": 0,
                    "cache": { "read": 0, "write": 0 } },
        "cost": 0,
        "finish": "stop",
    });
    let part_id = format!("{id}_part");
    let parts = vec![json!({
        "id": part_id,
        "type": "text",
        "text": text,
    })];
    state
        .store
        .append_message_record(sid, MessageAppendInput { info, parts })?;
    Ok(())
}

/// Pull all messages, drop tool internals (calls + outputs), and produce
/// a `Vec<(role, text)>` suitable for prompt-summarization. Tool I/O is
/// elided per the SUMMARY_PROMPT contract — they're rarely worth
/// preserving across a context-window boundary and they're the largest
/// source of noise.
fn collect_history(state: &AppState, sid: &str) -> Vec<(String, String)> {
    state
        .store
        .messages(sid, None, None)
        .map(|page| {
            page.items
                .into_iter()
                .filter_map(|msg| {
                    let role = msg.info.get("role").and_then(Value::as_str)?;
                    if role == "system" {
                        return None;
                    }
                    if msg.info.get("summary").and_then(Value::as_bool) == Some(true) {
                        return None;
                    }
                    let text = msg
                        .parts
                        .iter()
                        .filter(|p| p.get("type").and_then(Value::as_str) == Some("text"))
                        .filter_map(|p| p.get("text").and_then(Value::as_str))
                        .collect::<Vec<_>>()
                        .join("\n");
                    if text.trim().is_empty() {
                        return None;
                    }
                    Some((role.to_string(), text))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Render the (role, text) pairs as a single user message body for the
/// summarizer. Keeps it readable so the model can produce a reasonable
/// summary; doesn't try to reconstruct the original conversation
/// structure (the prompt instructs the model to focus on intent).
fn format_history_for_summary(history: &[(String, String)]) -> String {
    history
        .iter()
        .map(|(role, text)| format!("[{role}]\n{text}"))
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[derive(Debug)]
pub(crate) enum CompactionError {
    Provider(ProviderError),
    Store(rusqlite::Error),
    EmptyHistory,
    EmptyResult,
    Cancelled,
}

impl std::fmt::Display for CompactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Provider(err) => write!(f, "compaction provider error: {err}"),
            Self::Store(err) => write!(f, "compaction store error: {err}"),
            Self::EmptyHistory => write!(f, "no messages to summarize"),
            Self::EmptyResult => write!(f, "summarizer returned empty text"),
            Self::Cancelled => write!(f, "compaction cancelled"),
        }
    }
}

/// Stable error envelope for the assistant message when compaction
/// itself fails. The agent loop writes this to `info.error` and emits a
/// `session.idle` so the UI can re-prompt.
pub(crate) fn compaction_error_envelope(err: &CompactionError) -> Value {
    json!({
        "name": "CompactionError",
        "data": {
            "message": err.to_string(),
            "isRetryable": false,
            "metadata": { "source": "rust-compaction" }
        }
    })
}
