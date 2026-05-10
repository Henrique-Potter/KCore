//! `POST /enhance-prompt` — small-model rewrite of the user's prompt.
//!
//! Bun parity: [`packages/opencode/src/kilocode/enhance-prompt.ts`](../../../../../opencode/src/kilocode/enhance-prompt.ts).
//! Bun runs `generateText` with a single instruction system prompt and
//! one user message, then strips fences/quotes. We do the same through
//! `kilo_provider::chat_tools_with_auth_cancel` against the user's
//! OpenAI Codex OAuth credentials. On any failure (no auth, timeout,
//! provider error) we fall back to echoing the input so the webview
//! "Improve my prompt" button never returns nothing — the user can still
//! send the original text.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use kilo_provider::{chat_tools_with_auth_cancel, ChatMessage, ChatTool};
use serde::Deserialize;
use serde_json::json;

use crate::{oauth, AppState};

#[derive(Deserialize)]
pub(crate) struct EnhanceInput {
    text: String,
}

const INSTRUCTION: &str =
    "Rewrite the following user prompt so it is clearer, more specific, and unambiguous. \
     Reply with only the rewritten prompt — no preamble, explanations, bullet points, \
     placeholders, or surrounding quotes.";

const TIMEOUT: Duration = Duration::from_secs(30);
const MAX_LEN: usize = 4_000;

pub(crate) async fn enhance_prompt(
    State(state): State<Arc<AppState>>,
    Json(input): Json<EnhanceInput>,
) -> Response {
    let text = input.text.trim();
    if text.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }

    match enhance_via_llm(&state, text).await {
        Ok(out) => Json(json!({ "text": out })).into_response(),
        Err(reason) => {
            eprintln!(
                "[kilo-server] enhance-prompt LLM call failed ({reason}); falling back to echo"
            );
            Json(json!({ "text": text })).into_response()
        }
    }
}

async fn enhance_via_llm(state: &Arc<AppState>, text: &str) -> Result<String, String> {
    let cfg = state.store.config();
    let model = json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" });
    let messages = vec![ChatMessage {
        role: "user".to_string(),
        content: text.to_string(),
        responses: Vec::new(),
        attachments: Vec::new(),
    }];
    let cancel = AtomicBool::new(false);
    let auths = oauth::tokens::fresh_auths(state, &cancel).await?;
    let call = chat_tools_with_auth_cancel(
        &cfg,
        &auths,
        Some(&model),
        Some(INSTRUCTION.to_string()),
        messages,
        Vec::<ChatTool>::new(),
        &cancel,
    );

    let out = tokio::time::timeout(TIMEOUT, call)
        .await
        .map_err(|_| "timeout".to_string())?
        .map_err(|err| err.to_string())?;
    let cleaned = clean_output(&out.text);
    if cleaned.is_empty() {
        return Err("empty response".to_string());
    }
    Ok(cleaned)
}

fn clean_output(raw: &str) -> String {
    let mut out = raw.trim();
    if let Some(rest) = out.strip_prefix("```") {
        // Drop the first newline (and any leading language tag).
        if let Some(idx) = rest.find('\n') {
            out = &rest[idx + 1..];
        } else {
            out = rest;
        }
    }
    if let Some(rest) = out.strip_suffix("```") {
        out = rest;
    }
    let out = out.trim();
    let out = if (out.starts_with('"') && out.ends_with('"') && out.len() >= 2)
        || (out.starts_with('\'') && out.ends_with('\'') && out.len() >= 2)
    {
        &out[1..out.len() - 1]
    } else {
        out
    };
    let out = out.trim();
    if out.len() > MAX_LEN {
        out.chars().take(MAX_LEN).collect()
    } else {
        out.to_string()
    }
}
