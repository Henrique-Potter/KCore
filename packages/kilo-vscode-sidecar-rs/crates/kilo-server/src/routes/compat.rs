//! Compatibility handlers for extension-invoked routes that are outside
//! the Rust sidecar's core agent loop.
//!
//! These routes intentionally return conservative disabled/empty shapes
//! instead of 404. The VS Code extension already calls them through the
//! generated SDK; a 404 can trip the runtime fallback/mutation logic and
//! degrade unrelated chat flows. Provider-backed implementations can
//! replace these stubs slice-by-slice without changing the route contract.

use std::convert::Infallible;
use std::fs;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    Json,
};
use futures_core::Stream;
use kilo_provider::{chat_tools_with_auth_cancel, ChatMessage, ChatTool};
use serde_json::{json, Value};

use crate::util::git::git_text;
use crate::{oauth, AppState};

pub(crate) async fn remote_enable() -> impl IntoResponse {
    Json(remote_disabled())
}

pub(crate) async fn remote_disable() -> impl IntoResponse {
    Json(remote_disabled())
}

const COMMIT_TIMEOUT: Duration = Duration::from_secs(30);
const COMMIT_MAX_LEN: usize = 200;
const COMMIT_DIFF_BUDGET: usize = 12_000;

const COMMIT_INSTRUCTION: &str = "You write Conventional Commits-style git commit messages. \
     Reply with only the commit message — no preamble, no quotes, no markdown fences. \
     The subject line must be \u{2264} 72 characters and use imperative mood.";

/// `POST /commit-message`. Extension consumer:
/// `kilo-vscode/src/services/commit-message/index.ts`. Bun parity:
/// `packages/opencode/src/kilocode/commit-message/generate.ts`.
///
/// Tries a small-model LLM call (30s timeout) backed by the user's
/// OpenAI Codex OAuth token. On any failure (no auth, timeout, provider
/// error, empty response) we fall back to the literal stub so the
/// extension never crashes — the user can still manually type a message.
pub(crate) async fn commit_message(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    let path = input
        .get("path")
        .and_then(Value::as_str)
        .map(str::to_string);
    let selected: Vec<String> = input
        .get("selectedFiles")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let previous = input
        .get("previousMessage")
        .and_then(Value::as_str)
        .map(str::to_string);

    let llm = match path.as_deref() {
        Some(repo) if !repo.is_empty() => {
            generate_commit_message(&state, repo, &selected, previous.as_deref()).await
        }
        _ => Err("missing repo path".to_string()),
    };
    match llm {
        Ok(message) => Json(json!({ "message": message })).into_response(),
        Err(reason) => {
            eprintln!(
                "[kilo-server] commit-message LLM call failed ({reason}); falling back to stub"
            );
            Json(json!({ "message": fallback_commit_message(&selected) })).into_response()
        }
    }
}

async fn generate_commit_message(
    state: &Arc<AppState>,
    repo: &str,
    selected: &[String],
    previous: Option<&str>,
) -> Result<String, String> {
    let diff = collect_diff(repo, selected);
    if diff.trim().is_empty() {
        return Err("no diff content".to_string());
    }
    let cfg = state.store.config();
    let model = json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" });

    let mut user = format!(
        "Generate a short Conventional Commits style message (\u{2264} 72 chars subject) from this diff:\n\n{diff}"
    );
    if let Some(prev) = previous.filter(|s| !s.trim().is_empty()) {
        user = format!(
            "Generate a DIFFERENT commit message from the previous one (\"{prev}\"). Use a different type, scope, or wording.\n\n{user}"
        );
    }

    let messages = vec![ChatMessage {
        role: "user".to_string(),
        content: user,
        responses: Vec::new(),
        attachments: Vec::new(),
    }];
    let cancel = AtomicBool::new(false);
    let auths = oauth::tokens::fresh_auths(state, &cancel).await?;
    let call = chat_tools_with_auth_cancel(
        &cfg,
        &auths,
        Some(&model),
        Some(COMMIT_INSTRUCTION.to_string()),
        messages,
        Vec::<ChatTool>::new(),
        &cancel,
    );
    let out = tokio::time::timeout(COMMIT_TIMEOUT, call)
        .await
        .map_err(|_| "timeout".to_string())?
        .map_err(|err| err.to_string())?;
    let cleaned = clean_commit(&out.text);
    if cleaned.is_empty() {
        return Err("empty response".to_string());
    }
    Ok(cleaned)
}

fn collect_diff(repo: &str, selected: &[String]) -> String {
    let root = FsPath::new(repo);
    let staged = git_text(root, &["diff", "--cached", "--no-color"]).unwrap_or_default();
    let mut diff = if staged.trim().is_empty() {
        let mut args = vec!["diff", "--no-color"];
        if !selected.is_empty() {
            args.push("--");
            for path in selected {
                args.push(path.as_str());
            }
        }
        git_text(root, &args).unwrap_or_default()
    } else if !selected.is_empty() {
        let mut args = vec!["diff", "--cached", "--no-color", "--"];
        for path in selected {
            args.push(path.as_str());
        }
        git_text(root, &args).unwrap_or_default()
    } else {
        staged
    };
    if diff.len() > COMMIT_DIFF_BUDGET {
        diff.truncate(COMMIT_DIFF_BUDGET);
        diff.push_str("\n... [truncated]");
    }
    diff
}

fn clean_commit(raw: &str) -> String {
    let mut out = raw.trim();
    if let Some(rest) = out.strip_prefix("```") {
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
    if out.len() > COMMIT_MAX_LEN {
        out.chars().take(COMMIT_MAX_LEN).collect()
    } else {
        out.to_string()
    }
}

fn fallback_commit_message(selected: &[String]) -> String {
    if !selected.is_empty() {
        format!("Update {} selected file(s)", selected.len())
    } else {
        "Update files".to_string()
    }
}

pub(crate) async fn kilo_profile() -> impl IntoResponse {
    Json(json!({
        "profile": {
            "email": "",
            "name": "Kilo",
            "organizations": []
        },
        "balance": null,
        "currentOrgId": null
    }))
}

pub(crate) async fn kilo_organization(Json(_input): Json<Value>) -> impl IntoResponse {
    Json(true)
}

pub(crate) async fn kilo_fim(
    Json(_input): Json<Value>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let out = stream! {
        yield Ok(Event::default().data(
            json!({
                "choices": [{ "delta": { "content": "" } }],
                "usage": { "prompt_tokens": 0, "completion_tokens": 0 },
                "cost": 0
            })
            .to_string(),
        ));
    };
    Sse::new(out)
}

pub(crate) async fn kilo_cloud_sessions() -> impl IntoResponse {
    Json(json!({ "cliSessions": [], "nextCursor": null }))
}

/// `GET /kilo/cloud/session/{id}`. Bun proxies to the cloud server which
/// returns a `{info, messages}` envelope (`kilo-gateway/src/server/routes.ts:420`).
/// Without cloud connectivity we emit a stub envelope that has the same
/// top-level shape so the extension consumer (`kilo-vscode/.../cloud-session.ts`,
/// reads `data.info.title`) does not throw — the cloud-sessions sidebar
/// renders an offline placeholder instead of crashing.
pub(crate) async fn kilo_cloud_session(Path(id): Path<String>) -> impl IntoResponse {
    Json(json!({
        "info": {
            "id": id,
            "title": "Cloud session (offline)",
            "time": { "created": 0, "updated": 0 }
        },
        "messages": []
    }))
}

pub(crate) async fn kilo_cloud_import(Json(input): Json<Value>) -> impl IntoResponse {
    Json(json!({
        "id": input.get("sessionId").and_then(Value::as_str).unwrap_or_default(),
        "imported": false
    }))
}

pub(crate) async fn kilocode_import_project(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    import_response(state.store.import_project(input).map(|id| (id, false)))
}

pub(crate) async fn kilocode_import_session(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    import_response(state.store.import_session(input))
}

pub(crate) async fn kilocode_import_message(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    import_response(state.store.import_message(input).map(|id| (id, false)))
}

pub(crate) async fn kilocode_import_part(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    import_response(state.store.import_part(input).map(|id| (id, false)))
}

/// `POST /kilocode/skill/remove`. Bun parity:
/// `packages/opencode/src/skill/index.ts:313-320`. The body's
/// `location` is the absolute path to the SKILL.md file; we delete its
/// parent directory (the convention from the Bun side). Returns
/// `Json(true)` on success, 400 with `{error}` on failure, mirroring
/// Bun's `c.json(true)` + 400 envelope.
pub(crate) async fn kilocode_remove_skill(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    let location = match input.get("location").and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => value.to_string(),
        _ => return remove_error(StatusCode::BAD_REQUEST, "missing location"),
    };
    let resolved = match canonical(&location) {
        Some(path) => path,
        None => return remove_error(StatusCode::BAD_REQUEST, "invalid location"),
    };
    if !resolved.exists() {
        return remove_error(StatusCode::NOT_FOUND, "skill not found");
    }
    let safe_roots = safe_roots(&state);
    if !is_under_any(&resolved, &safe_roots) {
        return remove_error(
            StatusCode::FORBIDDEN,
            "skill location is outside known config/worktree/home directories",
        );
    }
    let dir = match resolved.parent() {
        Some(parent) => parent.to_path_buf(),
        None => return remove_error(StatusCode::BAD_REQUEST, "skill has no parent directory"),
    };
    if !is_under_any(&dir, &safe_roots) {
        return remove_error(
            StatusCode::FORBIDDEN,
            "skill parent is outside known config/worktree/home directories",
        );
    }
    if let Err(err) = fs::remove_dir_all(&dir) {
        return remove_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("failed to remove skill directory: {err}"),
        );
    }
    Json(true).into_response()
}

/// `POST /kilocode/agent/remove`. Bun parity:
/// `packages/opencode/src/kilocode/agent/index.ts:431-492`. We delete
/// any `agent/<name>.md`, `agents/<name>.md`, `mode/<name>.md`, or
/// `modes/<name>.md` files found under the same directories the
/// registry scans (config + per-worktree `.kilo*` + home `.kilo*`).
/// Inline (kilo.json `agent.<name>`) entries are rejected with a clear
/// error — the extension's `removeConfigEntryFromAllScopes` fallback
/// handles those.
pub(crate) async fn kilocode_remove_agent(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    let name = match input.get("name").and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => value.to_string(),
        _ => return remove_error(StatusCode::BAD_REQUEST, "missing agent name"),
    };
    if !valid_agent_name(&name) {
        return remove_error(StatusCode::BAD_REQUEST, "invalid agent name");
    }
    let mut removed = 0usize;
    let mut errors: Vec<String> = Vec::new();
    for dir in agent_search_dirs(&state) {
        for sub in ["agent", "agents", "mode", "modes"] {
            let candidate = dir.join(sub).join(format!("{name}.md"));
            if !candidate.is_file() {
                continue;
            }
            match fs::remove_file(&candidate) {
                Ok(()) => removed += 1,
                Err(err) => errors.push(format!("{}: {err}", candidate.display())),
            }
        }
    }
    if removed == 0 {
        let detail = if errors.is_empty() {
            format!("no agent file found on disk for {name}")
        } else {
            format!(
                "no agent file removed for {name}; errors: {}",
                errors.join("; ")
            )
        };
        return remove_error(StatusCode::NOT_FOUND, &detail);
    }
    Json(true).into_response()
}

fn safe_roots(state: &AppState) -> Vec<PathBuf> {
    let paths = state.store.paths();
    [paths.config, paths.worktree, paths.directory, paths.home]
        .into_iter()
        .filter_map(|p| canonical(&p))
        .collect()
}

fn agent_search_dirs(state: &AppState) -> Vec<PathBuf> {
    let paths = state.store.paths();
    let mut out: Vec<PathBuf> = Vec::new();
    out.push(PathBuf::from(&paths.config));
    let root = PathBuf::from(&paths.directory);
    let home = PathBuf::from(&paths.home);
    for name in [".kilo", ".kilocode", ".opencode"] {
        out.push(root.join(name));
        out.push(home.join(name));
    }
    out
}

fn canonical(path: &str) -> Option<PathBuf> {
    let raw = PathBuf::from(path);
    raw.canonicalize().ok().or(Some(raw))
}

fn is_under_any(path: &FsPath, roots: &[PathBuf]) -> bool {
    let canon = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    roots.iter().any(|root| canon.starts_with(root))
}

fn valid_agent_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        && !name.starts_with('.')
}

fn remove_error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

fn remote_disabled() -> Value {
    json!({ "enabled": false, "connected": false })
}

fn import_response(result: rusqlite::Result<(String, bool)>) -> Response {
    match result {
        Ok((id, skipped)) => {
            let mut body = json!({ "ok": true, "id": id });
            if skipped {
                body["skipped"] = Value::Bool(true);
            }
            Json(body).into_response()
        }
        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "ok": false,
                "id": "",
                "error": err.to_string()
            })),
        )
            .into_response(),
    }
}
