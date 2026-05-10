//! `POST /log` — log forwarding from the VS Code extension webview.
//! Bun parity: `packages/opencode/src/server/routes/control/index.ts:111`.
//!
//! Two payload shapes are accepted:
//!
//! * Bun control route shape: `{service, level, message, extra}`.
//! * Extension webview shape: `{level, scope, message, data}`.
//!
//! Either way the route emits a single `eprintln!` line prefixed with
//! `[kilo-server]` and returns `Json(true)` so callers see the same
//! 200 success the Bun route returned.

use axum::{response::IntoResponse, Json};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub(crate) struct LogInput {
    #[serde(default)]
    level: Option<String>,
    #[serde(default)]
    message: Option<String>,
    /// Bun shape carries `service`; the extension webview carries `scope`.
    /// Either fills the bracketed tag in the emitted line.
    #[serde(default)]
    service: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    /// Bun shape uses `extra`; the extension uses `data`. Whichever is
    /// present (preferring `extra` on conflict) is appended as compact
    /// JSON so the line stays grep-friendly.
    #[serde(default)]
    extra: Option<Value>,
    #[serde(default)]
    data: Option<Value>,
}

pub(crate) async fn log_handler(body: Option<Json<LogInput>>) -> impl IntoResponse {
    let input = body.map(|Json(value)| value).unwrap_or(LogInput {
        level: None,
        message: None,
        service: None,
        scope: None,
        extra: None,
        data: None,
    });
    let level = input.level.as_deref().unwrap_or("info");
    let tag = input
        .scope
        .as_deref()
        .or(input.service.as_deref())
        .unwrap_or("client");
    let message = input.message.as_deref().unwrap_or("");
    let extras = input.extra.as_ref().or(input.data.as_ref());
    match extras.filter(|value| !value.is_null()) {
        Some(value) => {
            let rendered = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string());
            eprintln!("[kilo-server] [log:{level}] [{tag}] {message} {rendered}");
        }
        None => {
            eprintln!("[kilo-server] [log:{level}] [{tag}] {message}");
        }
    }
    Json(true)
}
