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
use kilo_store::Store;
use serde_json::{json, Value};

use crate::http::sse;
use crate::AppState;

/// Bun parity: the structured Markdown skeleton from
/// `packages/opencode/src/session/compaction.ts:40-75`. The model fills
/// each section from the conversation context. Keep section headers and
/// order in lock-step with Bun — `validate_sections` and the parts
/// renderer both rely on the `## Goal`/`## Constraints`/`## Progress`/
/// `## Open Issues`/`## Next Steps` shape.
const SUMMARY_TEMPLATE: &str =
    "Output exactly this Markdown structure and keep the section order unchanged:\n\
---\n\
## Goal\n\
- [single-sentence task summary]\n\
\n\
## Constraints\n\
- [hard rules: file paths to avoid, libraries required, coding conventions, or \"(none)\"]\n\
\n\
## Progress\n\
- [what's been done so far in this session, or \"(none)\"]\n\
\n\
## Open Issues\n\
- [known problems, incomplete work, errors encountered, or \"(none)\"]\n\
\n\
## Next Steps\n\
- [what should happen next when work resumes, or \"(none)\"]\n\
---\n\
\n\
Rules:\n\
- Keep every section, even when empty.\n\
- Cap each section at roughly 200 words; use terse bullets, not prose paragraphs.\n\
- Preserve exact file paths, commands, error strings, and identifiers when known.\n\
- Drop verbatim tool I/O, intermediate reasoning, and stylistic chatter.\n\
- Do not mention the summary process or that context was compacted.\n\
- Do NOT call any tools.";

/// Section headers we expect the model to emit. `validate_sections`
/// warns (non-fatal) when any are missing — Bun is similarly lenient.
const REQUIRED_SECTIONS: &[&str] = &[
    "## Goal",
    "## Constraints",
    "## Progress",
    "## Open Issues",
    "## Next Steps",
];

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
    let snapshot =
        tokio::task::spawn_blocking(move || collect_session_snapshot(&st.store, &sid_owned))
            .await
            .map_err(|err| CompactionError::Provider(ProviderError::Http(err.to_string())))?;
    if cancel.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(CompactionError::Cancelled);
    }
    if snapshot.history.is_empty() {
        return Err(CompactionError::EmptyHistory);
    }
    let cfg = state.store.config();
    let prompt = build_summary_prompt(snapshot.prior_summary.as_deref());
    let messages = vec![ChatMessage {
        role: "user".to_string(),
        content: format_history_for_summary(&snapshot.history),
        responses: Vec::new(),
        attachments: Vec::new(),
    }];
    let out = chat_tools_with_auth_cancel(
        &cfg,
        auths,
        model,
        Some(prompt),
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
    validate_sections(sid, &out.text);
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

/// History snapshot used by the summarizer: the conversation excerpt
/// (sans summary anchors / system msgs / empty parts) plus the most
/// recent prior summary if one exists. The latter is the anchor Bun's
/// `buildPrompt` weaves into the system prompt so successive
/// compactions chain coherently
/// (`packages/opencode/src/session/compaction.ts:121-131`).
struct SessionSnapshot {
    history: Vec<(String, String)>,
    prior_summary: Option<String>,
}

/// Pull all messages once, partition into `(history, prior_summary)`.
/// Tool I/O is elided per the `SUMMARY_TEMPLATE` contract — it's rarely
/// worth preserving across a context-window boundary and it's the
/// largest source of noise.
fn collect_session_snapshot(store: &Store, sid: &str) -> SessionSnapshot {
    let raw = store
        .messages(sid, None, None)
        .map(|page| page.items)
        .unwrap_or_default();
    let mut prior_summary: Option<String> = None;
    let mut history: Vec<(String, String)> = Vec::with_capacity(raw.len());
    for msg in raw {
        let Some(role) = msg.info.get("role").and_then(Value::as_str) else {
            continue;
        };
        if role == "system" {
            continue;
        }
        let text = msg
            .parts
            .iter()
            .filter(|p| p.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|p| p.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        if msg.info.get("summary").and_then(Value::as_bool) == Some(true) {
            // Latest summary wins — `parts.rs::real_messages` uses
            // `rposition` on the same flag, keep the semantics aligned.
            if !text.trim().is_empty() {
                prior_summary = Some(text);
            }
            continue;
        }
        if text.trim().is_empty() {
            continue;
        }
        history.push((role.to_string(), text));
    }
    SessionSnapshot {
        history,
        prior_summary,
    }
}

/// Build the system prompt for the summarizer call. When `prior` is
/// `Some`, prepend an anchored-update preamble so the model refines the
/// existing summary instead of starting from scratch — Bun parity with
/// `compaction.ts:121-131` (`buildPrompt`).
pub(crate) fn build_summary_prompt(prior: Option<&str>) -> String {
    match prior {
        Some(text) if !text.trim().is_empty() => format!(
            "## Previous summary\n\n{text}\n\n## Update instructions\n\
             Incorporate the previous summary above. Add new progress, \
             mark items as resolved, update next steps. Preserve still-true \
             details, drop stale ones, and merge in new facts from the \
             conversation history.\n\n{SUMMARY_TEMPLATE}"
        ),
        _ => SUMMARY_TEMPLATE.to_string(),
    }
}

/// Non-fatal section check. Bun's compaction is also lenient: a missing
/// header is logged but the summary still persists. The agent-loop test
/// suite exercises the happy-path content; this guard catches drift in
/// the fake provider output without blocking real turns.
fn validate_sections(sid: &str, text: &str) {
    let missing: Vec<&str> = REQUIRED_SECTIONS
        .iter()
        .copied()
        .filter(|hdr| !text.contains(hdr))
        .collect();
    if !missing.is_empty() {
        eprintln!("[kilo-server] compaction summary for {sid} missing sections: {missing:?}");
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use kilo_protocol::SessionCreateInput;

    fn unique_root() -> std::path::PathBuf {
        static IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = IDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("kilo-compaction-test-{}-{seq}", std::process::id()))
    }

    fn store_at(root: &std::path::Path) -> Store {
        Store::for_test(root)
    }

    /// Bun parity: the system prompt must carry the `## Goal` and
    /// `## Constraints` headers (and the rest of the structured skeleton).
    /// Locks the prompt against accidental drift back to freeform prose.
    #[test]
    fn compaction_uses_structured_template() {
        let prompt = build_summary_prompt(None);
        assert!(
            prompt.contains("## Goal"),
            "missing `## Goal` header in:\n{prompt}"
        );
        assert!(
            prompt.contains("## Constraints"),
            "missing `## Constraints` header in:\n{prompt}"
        );
        assert!(
            prompt.contains("## Progress"),
            "missing `## Progress` header in:\n{prompt}"
        );
        assert!(
            prompt.contains("## Open Issues"),
            "missing `## Open Issues` header in:\n{prompt}"
        );
        assert!(
            prompt.contains("## Next Steps"),
            "missing `## Next Steps` header in:\n{prompt}"
        );
        assert!(
            !prompt.contains("Previous summary"),
            "fresh prompt must not reference a prior anchor"
        );
    }

    /// When a session already carries an `info.summary == true` message
    /// the new compaction's system prompt prepends a "Previous summary"
    /// block so the model updates the anchor instead of starting from
    /// scratch (Bun parity: `compaction.ts:121-131`).
    #[test]
    fn compaction_anchors_prior_summary_when_present() {
        let root = unique_root();
        let store = store_at(&root);
        let sid = store
            .create_session(SessionCreateInput::default())
            .expect("create session")
            .id;
        // Seed a real user turn so history isn't empty.
        store
            .append_message_record(
                &sid,
                MessageAppendInput {
                    info: json!({
                        "id": "msg_user_1",
                        "role": "user",
                        "sessionID": sid,
                        "time": { "created": 1, "updated": 1, "completed": 1 },
                    }),
                    parts: vec![json!({
                        "id": "p_user_1", "type": "text", "text": "do the thing"
                    })],
                },
            )
            .unwrap();
        // Seed a prior summary anchor (assistant role + summary: true).
        let anchor_text = "## Goal\n- Old goal text\n\n## Next Steps\n- Old next steps";
        store
            .append_message_record(
                &sid,
                MessageAppendInput {
                    info: json!({
                        "id": "msg_summary_old",
                        "role": "assistant",
                        "sessionID": sid,
                        "summary": true,
                        "time": { "created": 2, "updated": 2, "completed": 2 },
                    }),
                    parts: vec![json!({
                        "id": "p_summary_old", "type": "text", "text": anchor_text
                    })],
                },
            )
            .unwrap();
        // Seed a follow-up user turn after the anchor.
        store
            .append_message_record(
                &sid,
                MessageAppendInput {
                    info: json!({
                        "id": "msg_user_2",
                        "role": "user",
                        "sessionID": sid,
                        "time": { "created": 3, "updated": 3, "completed": 3 },
                    }),
                    parts: vec![json!({
                        "id": "p_user_2", "type": "text", "text": "follow up work"
                    })],
                },
            )
            .unwrap();

        let snap = collect_session_snapshot(&store, &sid);
        assert_eq!(
            snap.prior_summary.as_deref(),
            Some(anchor_text),
            "snapshot must surface the prior summary text"
        );
        // History excludes the anchor itself but keeps both user turns.
        assert_eq!(
            snap.history.len(),
            2,
            "history should hold the two user turns, got: {:?}",
            snap.history
        );

        let prompt = build_summary_prompt(snap.prior_summary.as_deref());
        assert!(
            prompt.contains("## Previous summary"),
            "anchored prompt missing `## Previous summary`:\n{prompt}"
        );
        assert!(
            prompt.contains(anchor_text),
            "anchored prompt missing prior summary body:\n{prompt}"
        );
        assert!(
            prompt.contains("## Update instructions"),
            "anchored prompt missing update instructions:\n{prompt}"
        );
        // Template still appended after the anchor block.
        assert!(prompt.contains("## Goal"));
        assert!(prompt.contains("## Constraints"));

        let _ = std::fs::remove_dir_all(root);
    }

    /// Fresh session — no prior summary — must produce the bare
    /// `SUMMARY_TEMPLATE` with no anchor preamble.
    #[test]
    fn compaction_skips_anchor_when_no_prior_summary() {
        let root = unique_root();
        let store = store_at(&root);
        let sid = store
            .create_session(SessionCreateInput::default())
            .expect("create session")
            .id;
        store
            .append_message_record(
                &sid,
                MessageAppendInput {
                    info: json!({
                        "id": "msg_user_1",
                        "role": "user",
                        "sessionID": sid,
                        "time": { "created": 1, "updated": 1, "completed": 1 },
                    }),
                    parts: vec![json!({
                        "id": "p_user_1", "type": "text", "text": "fresh task"
                    })],
                },
            )
            .unwrap();

        let snap = collect_session_snapshot(&store, &sid);
        assert!(
            snap.prior_summary.is_none(),
            "fresh session must have no prior summary, got: {:?}",
            snap.prior_summary
        );

        let prompt = build_summary_prompt(snap.prior_summary.as_deref());
        assert!(
            !prompt.contains("Previous summary"),
            "prompt must not include `Previous summary` block:\n{prompt}"
        );
        assert!(
            !prompt.contains("Update instructions"),
            "prompt must not include update instructions:\n{prompt}"
        );
        assert_eq!(prompt, SUMMARY_TEMPLATE, "prompt must be the bare template");

        let _ = std::fs::remove_dir_all(root);
    }
}
