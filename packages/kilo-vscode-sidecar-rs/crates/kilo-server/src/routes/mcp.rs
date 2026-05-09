//! MCP runtime: route handlers, local stdio JSON-RPC framing, remote SSE
//! parsing, OAuth dynamic client registration, and auth-header redaction.
//! M9 extraction target — this file's contents are intended to move into
//! the `kilo-mcp` crate. Do not grow further; new MCP work belongs there.

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    path::PathBuf,
    process::{ChildStdin, Command, Stdio},
    sync::atomic::AtomicBool,
    sync::{mpsc, Arc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use kilo_protocol::Config;
use rand::{rngs::OsRng, RngCore};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::util::paths::resolve_under;
use crate::{AppState, McpChild};

const MCP_PROTOCOL_VERSION: &str = "2025-03-26";
pub(crate) const MCP_DEFAULT_TIMEOUT_MS: u64 = 30_000;
const MCP_CALL_ID: i64 = 3;
const MCP_REFRESH_ID: i64 = 4;
const MCP_AUTH_REDACTED: &str = "[redacted]";
const MCP_OAUTH_PENDING_TTL_SECONDS: i64 = 600;

/// `GET /mcp` — M11-safe status surface. Parse the existing Rust config shape
/// and merge in-memory lifecycle state for local stdio children. Disabled
/// servers stay `disabled`; remote transports remain explicit failures until
/// a later M11 slice implements HTTP/SSE/OAuth.
pub(crate) async fn mcp_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(mcp_status_map(&state))
}

/// `POST /mcp` — register an in-memory MCP endpoint. Persistence is still a
/// later M11 slice, but `connect` can now launch local stdio configs added
/// through this route.
pub(crate) async fn mcp_add(
    State(state): State<Arc<AppState>>,
    Json(input): Json<kilo_mcp::AddInput>,
) -> Response {
    let status = kilo_mcp::baseline(&input.config);
    state
        .mcp_configs
        .lock()
        .unwrap()
        .insert(input.name.clone(), input.config);
    state.mcp.lock().unwrap().insert(input.name, status);
    Json(mcp_status_map(&state)).into_response()
}

/// `PUT /mcp/{name}/auth` — narrow M11 remote auth persistence baseline. This
/// intentionally avoids interactive OAuth and only updates static remote MCP
/// headers in the existing config shape. `{ "token": "..." }` stores a bearer
/// `Authorization` header; `{ "headers": { ... } }` stores the supplied static
/// headers. Status remains redacted because MCP status never echoes config.
pub(crate) async fn mcp_auth(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(input): Json<kilo_mcp::AuthInput>,
) -> Response {
    let token = mcp_auth_token(&input);
    let headers = match mcp_auth_headers(&input) {
        Ok(value) => value,
        Err(msg) => {
            return mcp_error(
                StatusCode::BAD_REQUEST,
                "RustMcpAuthInvalidError",
                &name,
                msg,
            )
        }
    };
    if let Err(err) = mcp_remote_headers(&headers) {
        return mcp_error(err.status(), err.name(), &name, err.message());
    }

    let mut cfg = state.store.config();
    let Some(root) = cfg.data.get_mut("mcp").and_then(Value::as_object_mut) else {
        return mcp_error(
            StatusCode::NOT_FOUND,
            "RustMcpNotFoundError",
            &name,
            "MCP server is not configured.",
        );
    };
    let Some(item) = root.get_mut(&name).and_then(Value::as_object_mut) else {
        return mcp_error(
            StatusCode::NOT_FOUND,
            "RustMcpNotFoundError",
            &name,
            "MCP server is not configured.",
        );
    };
    if item.get("type").and_then(Value::as_str) != Some("remote") {
        return mcp_error(
            StatusCode::BAD_REQUEST,
            "RustMcpAuthInvalidError",
            &name,
            "MCP auth updates are only supported for remote servers.",
        );
    }
    if !headers.is_empty() {
        item.insert("headers".to_string(), json!(headers));
    }
    let persist = token
        .as_ref()
        .map(|value| state.store.set_mcp_auth(&name, value.clone()));
    match (state.store.set_config(Config { data: cfg.data }), persist) {
        (Ok(_), None) => {
            if let Some(cfg) = mcp_config(&state, &name) {
                state.mcp_configs.lock().unwrap().insert(name, cfg);
            }
            Json(json!({ "headers": mcp_redact_headers(&headers) })).into_response()
        }
        (Ok(_), Some(Ok(auth))) => {
            if let Some(cfg) = mcp_config(&state, &name) {
                state.mcp_configs.lock().unwrap().insert(name, cfg);
            }
            Json(json!({ "auth": mcp_redact_auth(&auth) })).into_response()
        }
        (Err(err), _) | (_, Some(Err(err))) => mcp_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RustMcpAuthPersistError",
            &name,
            err.to_string(),
        ),
    }
}

/// `POST /mcp/{name}/oauth/authorize` — explicit-config OAuth scaffold for
/// remote MCP servers. No discovery and no dynamic client registration: all
/// endpoints and client fields must be present in the configured remote item.
pub(crate) async fn mcp_oauth_authorize(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    let cfg = match mcp_oauth_config(&state, &name).await {
        Ok(value) => value,
        Err(err) => return mcp_error(err.status, err.name, &name, err.message),
    };
    let state_value = mcp_oauth_random();
    let verifier = mcp_oauth_random();
    let challenge = mcp_oauth_challenge(&verifier);
    let url = match mcp_oauth_authorize_url(&cfg, &state_value, &challenge) {
        Ok(value) => value,
        Err(err) => {
            return mcp_error(
                StatusCode::BAD_REQUEST,
                "RustMcpOAuthConfigError",
                &name,
                err,
            )
        }
    };
    let mut auth = state.store.mcp_auth(&name).unwrap_or_else(|| json!({}));
    mcp_oauth_expire_pending(&mut auth);
    auth["pending"] = json!({
        "state": state_value,
        "codeVerifier": verifier,
        "serverName": name,
        "createdAt": mcp_now_seconds(),
        "redirectUri": cfg.redirect,
    });
    if let Err(err) = state.store.set_mcp_auth(&name, auth) {
        return mcp_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "RustMcpOAuthPersistError",
            &name,
            err.to_string(),
        );
    }
    Json(json!({ "url": url, "method": "manual", "state": state_value })).into_response()
}

pub(crate) async fn mcp_oauth_callback(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let html = |status, title: &str, msg: &str| {
        (
            status,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            format!("<html><body><h1>{title}</h1><p>{msg}</p></body></html>"),
        )
            .into_response()
    };
    let Some(code) = query.get("code").filter(|value| !value.is_empty()) else {
        return html(
            StatusCode::BAD_REQUEST,
            "RustMcpOAuthCallbackError",
            "Missing OAuth code.",
        );
    };
    let Some(got) = query.get("state").filter(|value| !value.is_empty()) else {
        return html(
            StatusCode::BAD_REQUEST,
            "RustMcpOAuthStateError",
            "Missing OAuth state.",
        );
    };
    let cfg = match mcp_oauth_config(&state, &name).await {
        Ok(value) => value,
        Err(err) => return html(err.status, err.name, &err.message),
    };
    let mut auth = state.store.mcp_auth(&name).unwrap_or_else(|| json!({}));
    mcp_oauth_expire_pending(&mut auth);
    let Some(pending) = auth.get("pending") else {
        return html(
            StatusCode::BAD_REQUEST,
            "RustMcpOAuthStateError",
            "No pending OAuth authorization.",
        );
    };
    if pending.get("state").and_then(Value::as_str) != Some(got) {
        return html(
            StatusCode::BAD_REQUEST,
            "RustMcpOAuthStateError",
            "OAuth state does not match.",
        );
    }
    let Some(verifier) = pending.get("codeVerifier").and_then(Value::as_str) else {
        return html(
            StatusCode::BAD_REQUEST,
            "RustMcpOAuthStateError",
            "Pending OAuth verifier is missing.",
        );
    };
    match mcp_oauth_exchange(&cfg, code, verifier).await {
        Ok(data) => {
            let mut next = mcp_refreshed_auth(
                json!({
                    "tokenUrl": cfg.token,
                    "clientId": cfg.client,
                    "clientSecret": cfg.secret,
                    "scope": cfg.scope,
                }),
                data,
                "",
            );
            next.as_object_mut().map(|map| map.remove("pending"));
            if let Err(err) = state.store.set_mcp_auth(&name, next) {
                return html(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "RustMcpOAuthPersistError",
                    &err.to_string(),
                );
            }
            html(
                StatusCode::OK,
                "Kilo MCP OAuth complete",
                "Authorization succeeded. You can close this tab.",
            )
        }
        Err(err) => html(err.status(), err.name(), err.message()),
    }
}

/// `POST /mcp/{name}/connect` — local stdio lifecycle baseline plus the first
/// MCP handshake slice. This launches the configured process, sends a bounded
/// JSON-RPC `initialize`, then sends `tools/list` and stores discovered tool
/// definitions in the in-memory status surface.
pub(crate) async fn mcp_connect(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    let cfg = mcp_config(&state, &name);
    let Some(cfg) = cfg else {
        return mcp_not_implemented(&name);
    };
    let status = mcp_connect_config(&state, &name, &cfg).await;
    state.mcp.lock().unwrap().insert(name, status);
    Json(true).into_response()
}

/// `POST /mcp/{name}/disconnect` — terminate a tracked local stdio child if one
/// exists and mark the server disabled in memory. Unknown dynamic servers get a
/// named 501 envelope rather than a route-level 404.
pub(crate) async fn mcp_disconnect(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    if mcp_config(&state, &name).is_none() && !state.mcp.lock().unwrap().contains_key(&name) {
        return mcp_not_implemented(&name);
    }
    if let Some(mut child) = state.mcp_children.lock().unwrap().remove(&name) {
        if let Err(err) = mcp_stop_child(&mut child) {
            state.mcp.lock().unwrap().insert(
                name,
                kilo_mcp::Status::Failed {
                    error: format!("Unable to stop MCP server: {err}"),
                },
            );
            return Json(true).into_response();
        }
    }
    state
        .mcp
        .lock()
        .unwrap()
        .insert(name, kilo_mcp::Status::Disabled);
    Json(true).into_response()
}

/// `POST /mcp/{name}/tool` — M11 stdio-only tool invocation. Body accepts the
/// MCP call shape `{ name, arguments }` (with `input` accepted as a small caller
/// convenience) and returns the `tools/call` result payload unchanged.
pub(crate) async fn mcp_call_tool(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(input): Json<kilo_mcp::CallInput>,
) -> Response {
    let cfg = mcp_config(&state, &name);
    let Some(cfg) = cfg else {
        return mcp_not_implemented(&name);
    };
    if !kilo_mcp::enabled(&cfg) {
        return mcp_error(
            StatusCode::BAD_REQUEST,
            "RustMcpDisabledError",
            &name,
            "MCP server is disabled.",
        );
    }
    match cfg {
        kilo_mcp::Config::Local { timeout, .. } => {
            let timeout = Duration::from_millis(timeout.unwrap_or(MCP_DEFAULT_TIMEOUT_MS).max(1));
            let res = {
                let mut children = state.mcp_children.lock().unwrap();
                let Some(child) = children.get_mut(&name) else {
                    return mcp_error(
                        StatusCode::NOT_FOUND,
                        "RustMcpDisconnectedError",
                        &name,
                        "MCP server is not connected.",
                    );
                };
                mcp_call_child(child, input, timeout)
            };
            match res {
                Ok(value) => {
                    mcp_refresh_changed(&state, &name, timeout);
                    Json(value).into_response()
                }
                Err(err) => mcp_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    err.name(),
                    &name,
                    err.message(),
                ),
            }
        }
        kilo_mcp::Config::Remote {
            url,
            headers,
            timeout,
            ..
        } => {
            if !matches!(
                state.mcp.lock().unwrap().get(&name),
                Some(kilo_mcp::Status::Connected { .. })
            ) {
                return mcp_error(
                    StatusCode::NOT_FOUND,
                    "RustMcpDisconnectedError",
                    &name,
                    "MCP server is not connected.",
                );
            }
            let headers =
                match mcp_remote_request_headers(state.as_ref(), &name, headers.as_ref()).await {
                    Ok(value) => value,
                    Err(err) => {
                        state.mcp.lock().unwrap().insert(
                            name.clone(),
                            kilo_mcp::Status::Failed {
                                error: err.message().to_string(),
                            },
                        );
                        return mcp_error(err.status(), err.name(), &name, err.message());
                    }
                };
            match mcp_call_remote(&url, Some(&headers), input, timeout).await {
                Ok(value) => Json(value).into_response(),
                Err(err) => {
                    let status = err.status();
                    let error = err.message().to_string();
                    let kind = err.name();
                    state.mcp.lock().unwrap().insert(
                        name.clone(),
                        kilo_mcp::Status::Failed {
                            error: error.clone(),
                        },
                    );
                    mcp_error(status, kind, &name, error)
                }
            }
        }
    }
}

pub(crate) fn mcp_status_map(state: &AppState) -> kilo_mcp::StatusMap {
    mcp_reap_exited(state);
    mcp_drain_notifications(state);
    mcp_refresh_all_changed(state);
    let memory = state.mcp.lock().unwrap().clone();
    let mut result = kilo_mcp::status(&json!(state.store.config()), &memory);
    result.extend(memory);
    result
}

pub(crate) fn mcp_refresh_all_changed(state: &AppState) {
    let timeout = Duration::from_millis(MCP_DEFAULT_TIMEOUT_MS);
    let names = {
        let children = state.mcp_children.lock().unwrap();
        children
            .iter()
            .filter(|(_, child)| child.tools_changed)
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
    };
    for name in names {
        mcp_refresh_changed(state, &name, timeout);
    }
}

pub(crate) fn mcp_drain_notifications(state: &AppState) {
    let mut failed = Vec::new();
    {
        let mut children = state.mcp_children.lock().unwrap();
        for (name, child) in children.iter_mut() {
            loop {
                match child.rx.try_recv() {
                    Ok(value) => mcp_handle_notification(child, &value),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        failed.push(name.clone());
                        break;
                    }
                }
            }
        }
    }
    for name in failed {
        if let Some(mut child) = state.mcp_children.lock().unwrap().remove(&name) {
            let _ = mcp_stop_child(&mut child);
        }
        state.mcp.lock().unwrap().insert(
            name,
            kilo_mcp::Status::Failed {
                error: "MCP server stdout closed.".to_string(),
            },
        );
    }
}

pub(crate) fn mcp_refresh_changed(state: &AppState, name: &str, timeout: Duration) {
    let res = {
        let mut children = state.mcp_children.lock().unwrap();
        let Some(child) = children.get_mut(name) else {
            return;
        };
        if !child.tools_changed {
            return;
        }
        child.tools_changed = false;
        match mcp_list_tools(child, MCP_REFRESH_ID, timeout) {
            Ok(tools) => Ok(tools),
            Err(err) => {
                if matches!(
                    McpCallError::from_wait(err.clone()),
                    McpCallError::Closed(_)
                ) {
                    let _ = mcp_stop_child(child);
                }
                Err(McpCallError::from_wait(err))
            }
        }
    };
    match res {
        Ok(tools) => {
            state
                .mcp
                .lock()
                .unwrap()
                .insert(name.to_string(), kilo_mcp::Status::Connected { tools });
            crate::http::sse::publish(
                state,
                kilo_protocol::GlobalEvent::bus("mcp.tools.changed", json!({ "server": name })),
            );
        }
        Err(err) => {
            state.mcp_children.lock().unwrap().remove(name);
            state.mcp.lock().unwrap().insert(
                name.to_string(),
                kilo_mcp::Status::Failed {
                    error: err.message().to_string(),
                },
            );
        }
    }
}

pub(crate) fn mcp_config(state: &AppState, name: &str) -> Option<kilo_mcp::Config> {
    if let Some(cfg) = state.mcp_configs.lock().unwrap().get(name).cloned() {
        return Some(cfg);
    }
    let cfg = json!(state.store.config());
    cfg.get("mcp")
        .and_then(Value::as_object)
        .and_then(|data| data.get(name))
        .and_then(kilo_mcp::config)
}

pub(crate) async fn mcp_connect_config(
    state: &AppState,
    name: &str,
    cfg: &kilo_mcp::Config,
) -> kilo_mcp::Status {
    if !kilo_mcp::enabled(cfg) {
        return kilo_mcp::Status::Disabled;
    }
    let kilo_mcp::Config::Local {
        command,
        environment,
        cwd,
        timeout,
        ..
    } = cfg
    else {
        let kilo_mcp::Config::Remote {
            url,
            headers,
            timeout,
            ..
        } = cfg
        else {
            return kilo_mcp::baseline(cfg);
        };
        let headers = match mcp_remote_request_headers(state, name, headers.as_ref()).await {
            Ok(value) => value,
            Err(err) => {
                return kilo_mcp::Status::Failed {
                    error: err.message().to_string(),
                }
            }
        };
        match mcp_handshake_remote(url, Some(&headers), *timeout).await {
            Ok(tools) => return kilo_mcp::Status::Connected { tools },
            Err(err) => {
                return kilo_mcp::Status::Failed {
                    error: err.message().to_string(),
                }
            }
        }
    };
    if command.is_empty() || command[0].is_empty() {
        return kilo_mcp::Status::Failed {
            error: "MCP local command is empty.".to_string(),
        };
    }

    if let Some(mut child) = state.mcp_children.lock().unwrap().remove(name) {
        let _ = mcp_stop_child(&mut child);
    }

    match mcp_spawn_local(
        state,
        command,
        environment.as_ref(),
        cwd.as_deref(),
        *timeout,
    ) {
        Ok((child, tools)) => {
            state
                .mcp_children
                .lock()
                .unwrap()
                .insert(name.to_string(), child);
            kilo_mcp::Status::Connected { tools }
        }
        Err(err) => kilo_mcp::Status::Failed { error: err },
    }
}

pub(crate) fn mcp_spawn_local(
    state: &AppState,
    command: &[String],
    environment: Option<&BTreeMap<String, String>>,
    cwd: Option<&str>,
    timeout: Option<u64>,
) -> Result<(McpChild, Vec<kilo_mcp::Tool>), String> {
    let root = PathBuf::from(state.store.paths().directory);
    let dir = match cwd {
        Some(value) if !value.is_empty() => {
            resolve_under(&root, value).map_err(|_| format!("Unsafe MCP cwd: {value}"))?
        }
        _ => root,
    };
    let mut cmd = Command::new(&command[0]);
    cmd.args(&command[1..])
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if command[0] == "opencode" {
        cmd.env("BUN_BE_BUN", "1");
    }
    if let Some(env) = environment {
        cmd.envs(env);
    }
    let mut child = cmd
        .spawn()
        .map_err(|err| format!("Unable to start MCP server: {err}"))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| "Unable to open MCP server stdin.".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Unable to open MCP server stdout.".to_string())?;
    let rx = mcp_reader(stdout);
    let mut item = McpChild {
        child,
        stdin,
        rx,
        tools_changed: false,
    };
    let res = mcp_handshake(&mut item, timeout.unwrap_or(MCP_DEFAULT_TIMEOUT_MS));
    match res {
        Ok(tools) => Ok((item, tools)),
        Err(err) => {
            let _ = mcp_stop_child(&mut item);
            Err(err)
        }
    }
}

pub(crate) fn mcp_stop_child(child: &mut McpChild) -> std::io::Result<()> {
    if child.child.try_wait()?.is_some() {
        return Ok(());
    }
    child.child.kill()?;
    let _ = child.child.wait()?;
    Ok(())
}

pub(crate) fn mcp_reader(stdout: std::process::ChildStdout) -> mpsc::Receiver<Value> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        while let Ok(value) = mcp_read_message(&mut reader) {
            if tx.send(value).is_err() {
                return;
            }
        }
    });
    rx
}

pub(crate) fn mcp_handshake(
    child: &mut McpChild,
    timeout: u64,
) -> Result<Vec<kilo_mcp::Tool>, String> {
    let timeout = Duration::from_millis(timeout.max(1));
    let init = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": { "name": "kilo-rust-sidecar", "version": env!("CARGO_PKG_VERSION") }
        }
    });
    mcp_write_message(&mut child.stdin, &init)
        .map_err(|err| format!("MCP initialize write failed: {err}"))?;
    let init = mcp_wait_response(child, 1, timeout)
        .map_err(|err| format!("MCP initialize failed: {err}"))?;
    mcp_response_error(&init).map_err(|err| format!("MCP initialize failed: {err}"))?;

    mcp_list_tools(child, 2, timeout)
}

pub(crate) fn mcp_list_tools(
    child: &mut McpChild,
    id: i64,
    timeout: Duration,
) -> Result<Vec<kilo_mcp::Tool>, String> {
    let list = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/list",
        "params": {}
    });
    mcp_write_message(&mut child.stdin, &list)
        .map_err(|err| format!("MCP tools/list write failed: {err}"))?;
    let list = mcp_wait_response(child, id, timeout)
        .map_err(|err| format!("MCP tools/list failed: {err}"))?;
    mcp_response_error(&list).map_err(|err| format!("MCP tools/list failed: {err}"))?;
    let tools = list
        .get("result")
        .and_then(|value| value.get("tools"))
        .cloned()
        .ok_or_else(|| "MCP tools/list failed: missing result.tools".to_string())?;
    serde_json::from_value(tools)
        .map_err(|err| format!("MCP tools/list returned invalid tools: {err}"))
}

pub(crate) fn mcp_call_child(
    child: &mut McpChild,
    input: kilo_mcp::CallInput,
    timeout: Duration,
) -> Result<Value, McpCallError> {
    mcp_call_child_cancel(child, input, timeout, None)
}

pub(crate) fn mcp_call_child_cancel(
    child: &mut McpChild,
    input: kilo_mcp::CallInput,
    timeout: Duration,
    cancel: Option<&AtomicBool>,
) -> Result<Value, McpCallError> {
    let req = json!({
        "jsonrpc": "2.0",
        "id": MCP_CALL_ID,
        "method": "tools/call",
        "params": {
            "name": input.name,
            "arguments": input.arguments,
        }
    });
    mcp_write_message(&mut child.stdin, &req)
        .map_err(|err| McpCallError::Write(format!("MCP tools/call write failed: {err}")))?;
    let res = mcp_wait_response_cancel(child, MCP_CALL_ID, timeout, cancel)
        .map_err(McpCallError::from_wait)?;
    mcp_response_error(&res).map_err(McpCallError::Rpc)?;
    res.get("result")
        .cloned()
        .ok_or_else(|| McpCallError::Malformed("missing result".to_string()))
}

pub(crate) fn mcp_wait_response(
    child: &mut McpChild,
    id: i64,
    timeout: Duration,
) -> Result<Value, String> {
    mcp_wait_response_cancel(child, id, timeout, None)
}

pub(crate) fn mcp_wait_response_cancel(
    child: &mut McpChild,
    id: i64,
    timeout: Duration,
    cancel: Option<&AtomicBool>,
) -> Result<Value, String> {
    let start = Instant::now();
    loop {
        if cancel.is_some_and(crate::agent::is_canceled) {
            return Err("tool call aborted".to_string());
        }
        if let Some(status) = child
            .child
            .try_wait()
            .map_err(|err| format!("process status failed: {err}"))?
        {
            return Err(format!("server exited with {status}"));
        }
        let Some(left) = timeout.checked_sub(start.elapsed()) else {
            return Err("timed out waiting for response".to_string());
        };
        let wait = left.min(Duration::from_millis(50));
        match child.rx.recv_timeout(wait) {
            Ok(value) if value.get("id").and_then(Value::as_i64) == Some(id) => return Ok(value),
            Ok(value) => {
                mcp_handle_notification(child, &value);
                continue;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("server stdout closed".to_string())
            }
        }
    }
}

pub(crate) fn mcp_handle_notification(child: &mut McpChild, value: &Value) {
    if value.get("id").is_some() {
        return;
    }
    if value.get("method").and_then(Value::as_str) == Some("notifications/tools/list_changed") {
        child.tools_changed = true;
    }
}

pub(crate) fn mcp_write_message(out: &mut ChildStdin, value: &Value) -> std::io::Result<()> {
    let data = serde_json::to_vec(value)?;
    write!(out, "Content-Length: {}\r\n\r\n", data.len())?;
    out.write_all(&data)?;
    out.flush()
}

pub(crate) fn mcp_read_message(
    reader: &mut BufReader<std::process::ChildStdout>,
) -> Result<Value, String> {
    let mut len: Option<usize> = None;
    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line).map_err(|err| err.to_string())?;
        if read == 0 {
            return Err("server stdout closed".to_string());
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.eq_ignore_ascii_case("content-length") {
            len = value.trim().parse::<usize>().ok();
        }
    }
    let len = len.ok_or_else(|| "missing Content-Length".to_string())?;
    let mut buf = vec![0; len];
    reader.read_exact(&mut buf).map_err(|err| err.to_string())?;
    serde_json::from_slice(&buf).map_err(|err| err.to_string())
}

pub(crate) fn mcp_response_error(value: &Value) -> Result<(), String> {
    let Some(err) = value.get("error") else {
        return Ok(());
    };
    let msg = err
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| err.as_str())
        .unwrap_or("JSON-RPC error");
    Err(msg.to_string())
}

pub(crate) async fn mcp_handshake_remote(
    url: &str,
    headers: Option<&BTreeMap<String, String>>,
    timeout: Option<u64>,
) -> Result<Vec<kilo_mcp::Tool>, McpRemoteError> {
    let req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": { "name": "kilo-rust-sidecar", "version": env!("CARGO_PKG_VERSION") }
        }
    });
    let init = mcp_post_remote(url, headers, req, 1, timeout).await?;
    mcp_response_error(&init).map_err(McpRemoteError::Rpc)?;
    let list = json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {} });
    let res = mcp_post_remote(url, headers, list, 2, timeout).await?;
    mcp_response_error(&res).map_err(McpRemoteError::Rpc)?;
    let tools = res
        .get("result")
        .and_then(|value| value.get("tools"))
        .cloned()
        .ok_or_else(|| {
            McpRemoteError::Malformed("MCP tools/list failed: missing result.tools".to_string())
        })?;
    serde_json::from_value(tools).map_err(|err| {
        McpRemoteError::Malformed(format!("MCP tools/list returned invalid tools: {err}"))
    })
}

pub(crate) async fn mcp_call_remote(
    url: &str,
    headers: Option<&BTreeMap<String, String>>,
    input: kilo_mcp::CallInput,
    timeout: Option<u64>,
) -> Result<Value, McpRemoteError> {
    let req = json!({
        "jsonrpc": "2.0",
        "id": MCP_CALL_ID,
        "method": "tools/call",
        "params": { "name": input.name, "arguments": input.arguments }
    });
    let res = mcp_post_remote(url, headers, req, MCP_CALL_ID, timeout).await?;
    mcp_response_error(&res).map_err(McpRemoteError::Rpc)?;
    res.get("result")
        .cloned()
        .ok_or_else(|| McpRemoteError::Malformed("missing result".to_string()))
}

pub(crate) async fn mcp_post_remote(
    url: &str,
    headers: Option<&BTreeMap<String, String>>,
    value: Value,
    id: i64,
    timeout: Option<u64>,
) -> Result<Value, McpRemoteError> {
    if url.is_empty() {
        return Err(McpRemoteError::Config(
            "MCP remote url is empty.".to_string(),
        ));
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(
            timeout.unwrap_or(MCP_DEFAULT_TIMEOUT_MS).max(1),
        ))
        .build()
        .map_err(|err| McpRemoteError::Http(format!("MCP remote client failed: {err}")))?;
    let mut req = client
        .post(url)
        .header(
            header::ACCEPT.as_str(),
            "application/json, text/event-stream",
        )
        .json(&value);
    if let Some(headers) = headers {
        req = req.headers(mcp_remote_headers(headers)?);
    }
    let res = req.send().await.map_err(|err| {
        if err.is_timeout() {
            McpRemoteError::Timeout("MCP remote request timed out.".to_string())
        } else {
            McpRemoteError::Http(format!("MCP remote request failed: {err}"))
        }
    })?;
    let status = res.status();
    if !status.is_success() {
        return Err(McpRemoteError::Http(format!(
            "MCP remote returned HTTP {status}"
        )));
    }
    let ctype = res
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let text = res.text().await.map_err(|err| {
        McpRemoteError::Malformed(format!("MCP remote response read failed: {err}"))
    })?;
    if ctype.contains("text/event-stream") {
        return mcp_parse_sse_response(&text, id);
    }
    serde_json::from_str(&text).map_err(|err| {
        McpRemoteError::Malformed(format!("MCP remote returned invalid JSON: {err}"))
    })
}

pub(crate) fn mcp_remote_headers(
    headers: &BTreeMap<String, String>,
) -> Result<HeaderMap, McpRemoteError> {
    let mut map = HeaderMap::new();
    for (key, value) in headers {
        let name = HeaderName::from_bytes(key.as_bytes()).map_err(|err| {
            McpRemoteError::Config(format!("Invalid MCP remote header name {key}: {err}"))
        })?;
        let val = HeaderValue::from_str(value).map_err(|err| {
            McpRemoteError::Config(format!("Invalid MCP remote header value for {key}: {err}"))
        })?;
        map.insert(name, val);
    }
    Ok(map)
}

pub(crate) fn mcp_auth_headers(
    input: &kilo_mcp::AuthInput,
) -> Result<BTreeMap<String, String>, String> {
    let has_token = input.token.as_ref().is_some_and(|value| !value.is_empty());
    let has_headers = input
        .headers
        .as_ref()
        .is_some_and(|value| !value.is_empty());
    let has_auth = input
        .access_token
        .as_ref()
        .is_some_and(|value| !value.is_empty())
        || input
            .refresh_token
            .as_ref()
            .is_some_and(|value| !value.is_empty());
    if (has_token as u8 + has_headers as u8 + has_auth as u8) != 1 {
        return Err(
            "Provide exactly one of token, non-empty headers, or token refresh fields.".to_string(),
        );
    }
    if let Some(token) = input.token.as_ref() {
        return Ok(BTreeMap::from([(
            "Authorization".to_string(),
            format!("Bearer {token}"),
        )]));
    }
    Ok(input.headers.clone().unwrap_or_default())
}

pub(crate) fn mcp_auth_token(input: &kilo_mcp::AuthInput) -> Option<Value> {
    let has = input
        .access_token
        .as_ref()
        .is_some_and(|value| !value.is_empty())
        || input
            .refresh_token
            .as_ref()
            .is_some_and(|value| !value.is_empty());
    if !has {
        return None;
    }
    Some(json!({
        "accessToken": input.access_token,
        "refreshToken": input.refresh_token,
        "expiresAt": input.expires_at,
        "tokenUrl": input.token_url,
        "clientId": input.client_id,
        "clientSecret": input.client_secret,
        "scope": input.scope,
    }))
}

pub(crate) fn mcp_redact_headers(headers: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .iter()
        .map(|(key, value)| {
            let hidden = key.eq_ignore_ascii_case("authorization")
                || key.eq_ignore_ascii_case("proxy-authorization")
                || key.to_ascii_lowercase().contains("token")
                || key.to_ascii_lowercase().contains("secret");
            (
                key.clone(),
                if hidden {
                    MCP_AUTH_REDACTED.to_string()
                } else {
                    value.clone()
                },
            )
        })
        .collect()
}

pub(crate) fn mcp_redact_auth(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let data = map
                .iter()
                .map(|(key, value)| {
                    let low = key.to_ascii_lowercase();
                    let hidden = low.contains("token") || low.contains("secret");
                    (
                        key.clone(),
                        if hidden && !value.is_null() {
                            Value::String(MCP_AUTH_REDACTED.to_string())
                        } else {
                            mcp_redact_auth(value)
                        },
                    )
                })
                .collect();
            Value::Object(data)
        }
        Value::Array(data) => Value::Array(data.iter().map(mcp_redact_auth).collect()),
        value => value.clone(),
    }
}

pub(crate) async fn mcp_remote_request_headers(
    state: &AppState,
    name: &str,
    headers: Option<&BTreeMap<String, String>>,
) -> Result<BTreeMap<String, String>, McpRemoteError> {
    let mut map = headers.cloned().unwrap_or_default();
    let Some(auth) = state.store.mcp_auth(name) else {
        return Ok(map);
    };
    let token = mcp_token_for_request(state, name, auth).await?;
    if let Some(token) = token {
        map.insert("Authorization".to_string(), format!("Bearer {token}"));
    }
    Ok(map)
}

async fn mcp_token_for_request(
    state: &AppState,
    name: &str,
    auth: Value,
) -> Result<Option<String>, McpRemoteError> {
    let access = auth
        .get("accessToken")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    if access.is_some() && !mcp_auth_expired(&auth) {
        return Ok(access.map(str::to_string));
    }
    let refresh = auth
        .get("refreshToken")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            McpRemoteError::Auth(
                "MCP auth token is expired and no refresh token is stored.".to_string(),
            )
        })?;
    let url = auth
        .get("tokenUrl")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            McpRemoteError::Auth(
                "MCP auth token is expired and no token endpoint is stored.".to_string(),
            )
        })?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(MCP_DEFAULT_TIMEOUT_MS))
        .build()
        .map_err(|err| McpRemoteError::Auth(format!("MCP token refresh client failed: {err}")))?;
    let mut form = vec![
        ("grant_type".to_string(), "refresh_token".to_string()),
        ("refresh_token".to_string(), refresh.to_string()),
    ];
    if let Some(value) = auth.get("clientId").and_then(Value::as_str) {
        form.push(("client_id".to_string(), value.to_string()));
    }
    if let Some(value) = auth.get("clientSecret").and_then(Value::as_str) {
        form.push(("client_secret".to_string(), value.to_string()));
    }
    if let Some(value) = auth.get("scope").and_then(Value::as_str) {
        form.push(("scope".to_string(), value.to_string()));
    }
    let res =
        client.post(url).form(&form).send().await.map_err(|err| {
            McpRemoteError::Auth(format!("MCP token refresh request failed: {err}"))
        })?;
    if !res.status().is_success() {
        return Err(McpRemoteError::Auth(format!(
            "MCP token refresh returned HTTP {}",
            res.status()
        )));
    }
    let data: Value = res.json().await.map_err(|err| {
        McpRemoteError::Auth(format!("MCP token refresh returned invalid JSON: {err}"))
    })?;
    let access = data
        .get("access_token")
        .or_else(|| data.get("accessToken"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            McpRemoteError::Auth("MCP token refresh did not return an access token.".to_string())
        })?
        .to_string();
    let next = mcp_refreshed_auth(auth, data, &access);
    state.store.set_mcp_auth(name, next).map_err(|err| {
        McpRemoteError::Auth(format!("MCP token refresh persistence failed: {err}"))
    })?;
    Ok(Some(access.to_string()))
}

fn mcp_refreshed_auth(mut auth: Value, data: Value, access: &str) -> Value {
    if access.is_empty() {
        if let Some(value) = data
            .get("access_token")
            .or_else(|| data.get("accessToken"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        {
            auth["accessToken"] = json!(value);
        }
    } else {
        auth["accessToken"] = json!(access);
    }
    if let Some(value) = data
        .get("refresh_token")
        .or_else(|| data.get("refreshToken"))
    {
        auth["refreshToken"] = value.clone();
    }
    if let Some(value) = data.get("scope").and_then(Value::as_str) {
        auth["scope"] = json!(value);
    }
    if let Some(value) = data.get("expires_at").or_else(|| data.get("expiresAt")) {
        auth["expiresAt"] = value.clone();
    } else if let Some(value) = data
        .get("expires_in")
        .or_else(|| data.get("expiresIn"))
        .and_then(Value::as_i64)
    {
        auth["expiresAt"] = json!(mcp_now_seconds() + value);
    }
    auth
}

struct McpOAuthConfig {
    authorize: String,
    token: String,
    client: String,
    secret: Option<String>,
    scope: Option<String>,
    redirect: String,
}

struct McpOAuthConfigError {
    status: StatusCode,
    name: &'static str,
    message: String,
}

async fn mcp_oauth_config(
    state: &AppState,
    name: &str,
) -> Result<McpOAuthConfig, McpOAuthConfigError> {
    let Some(kilo_mcp::Config::Remote { url, oauth, .. }) = mcp_config(state, name) else {
        return Err(McpOAuthConfigError {
            status: StatusCode::NOT_FOUND,
            name: "RustMcpNotFoundError",
            message: "MCP remote server is not configured.".to_string(),
        });
    };
    if oauth
        .as_ref()
        .is_some_and(|value| value == &Value::Bool(false))
    {
        return Err(McpOAuthConfigError {
            status: StatusCode::BAD_REQUEST,
            name: "RustMcpOAuthConfigError",
            message: "MCP OAuth is disabled for this remote server.".to_string(),
        });
    }
    let data = oauth.filter(Value::is_object).unwrap_or_else(|| json!({}));
    let field = |names: &[&str]| {
        names
            .iter()
            .find_map(|key| data.get(*key).and_then(Value::as_str))
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let missing = |field: &str| McpOAuthConfigError {
        status: StatusCode::BAD_REQUEST,
        name: "RustMcpOAuthConfigError",
        message: format!("MCP OAuth config is missing {field}."),
    };
    let meta = if field(&[
        "authorizeUrl",
        "authorizationEndpoint",
        "authorization_endpoint",
    ])
    .is_some()
        && field(&["tokenUrl", "tokenEndpoint", "token_endpoint"]).is_some()
    {
        None
    } else {
        Some(mcp_oauth_discover(&url).await?)
    };
    let persisted = state.store.mcp_auth(name).unwrap_or_else(|| json!({}));
    let client = field(&["clientId", "client_id"])
        .or_else(|| {
            persisted
                .get("client")
                .and_then(|value| value.get("clientId"))
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .or_else(|| {
            persisted
                .get("clientId")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        });
    let mut secret = field(&["clientSecret", "client_secret"]).or_else(|| {
        persisted
            .get("client")
            .and_then(|value| value.get("clientSecret"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    });
    let redirect = field(&["redirectUri", "redirect_uri"])
        .unwrap_or_else(|| format!("http://127.0.0.1:4099/mcp/{name}/oauth/callback"));
    let client = match client {
        Some(value) => value,
        None => {
            let Some(endpoint) = meta.as_ref().and_then(|value| value.registration.clone()) else {
                return Err(missing("clientId"));
            };
            let item = mcp_oauth_register(&endpoint, &redirect).await?;
            let mut auth = persisted;
            auth["client"] = json!({
                "clientId": item.0,
                "clientSecret": item.1,
            });
            state
                .store
                .set_mcp_auth(name, auth)
                .map_err(|err| McpOAuthConfigError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    name: "RustMcpOAuthPersistError",
                    message: err.to_string(),
                })?;
            secret = item.1;
            item.0
        }
    };
    Ok(McpOAuthConfig {
        authorize: field(&[
            "authorizeUrl",
            "authorizationEndpoint",
            "authorization_endpoint",
        ])
        .or_else(|| meta.as_ref().map(|value| value.authorize.clone()))
        .ok_or_else(|| missing("authorizeUrl"))?,
        token: field(&["tokenUrl", "tokenEndpoint", "token_endpoint"])
            .or_else(|| meta.as_ref().map(|value| value.token.clone()))
            .ok_or_else(|| missing("tokenUrl"))?,
        client,
        secret,
        scope: field(&["scope"]).or_else(|| meta.as_ref().and_then(|value| value.scope.clone())),
        redirect,
    })
}

struct McpOAuthMetadata {
    authorize: String,
    token: String,
    registration: Option<String>,
    scope: Option<String>,
}

async fn mcp_oauth_discover(url: &str) -> Result<McpOAuthMetadata, McpOAuthConfigError> {
    let base = reqwest::Url::parse(url).map_err(|err| McpOAuthConfigError {
        status: StatusCode::BAD_REQUEST,
        name: "RustMcpOAuthDiscoveryError",
        message: format!("Invalid MCP remote url for OAuth discovery: {err}"),
    })?;
    let host = base.host_str().unwrap_or_default();
    let root = match base.port() {
        Some(port) => format!("{}://{}:{}", base.scheme(), host, port),
        None => format!("{}://{}", base.scheme(), host),
    };
    let endpoint = format!("{root}/.well-known/oauth-authorization-server");
    let res = reqwest::get(&endpoint)
        .await
        .map_err(|err| McpOAuthConfigError {
            status: StatusCode::BAD_GATEWAY,
            name: "RustMcpOAuthDiscoveryError",
            message: format!("MCP OAuth metadata discovery failed: {err}"),
        })?;
    if !res.status().is_success() {
        return Err(McpOAuthConfigError {
            status: StatusCode::BAD_GATEWAY,
            name: "RustMcpOAuthDiscoveryError",
            message: format!(
                "MCP OAuth metadata discovery returned HTTP {}",
                res.status()
            ),
        });
    }
    let data: Value = res.json().await.map_err(|err| McpOAuthConfigError {
        status: StatusCode::BAD_GATEWAY,
        name: "RustMcpOAuthDiscoveryError",
        message: format!("MCP OAuth metadata is invalid JSON: {err}"),
    })?;
    let get = |key: &str| {
        data.get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
    };
    let authorize = get("authorization_endpoint").ok_or_else(|| McpOAuthConfigError {
        status: StatusCode::BAD_GATEWAY,
        name: "RustMcpOAuthDiscoveryError",
        message: "MCP OAuth metadata is missing authorization_endpoint.".to_string(),
    })?;
    let token = get("token_endpoint").ok_or_else(|| McpOAuthConfigError {
        status: StatusCode::BAD_GATEWAY,
        name: "RustMcpOAuthDiscoveryError",
        message: "MCP OAuth metadata is missing token_endpoint.".to_string(),
    })?;
    Ok(McpOAuthMetadata {
        authorize: authorize.to_string(),
        token: token.to_string(),
        registration: get("registration_endpoint").map(str::to_string),
        scope: data
            .get("scopes_supported")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .filter(|value| !value.is_empty()),
    })
}

async fn mcp_oauth_register(
    endpoint: &str,
    redirect: &str,
) -> Result<(String, Option<String>), McpOAuthConfigError> {
    let body = json!({
        "client_name": "Kilo Rust Sidecar MCP",
        "redirect_uris": [redirect],
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none"
    });
    let client = reqwest::Client::new();
    let res = client
        .post(endpoint)
        .json(&body)
        .send()
        .await
        .map_err(|err| McpOAuthConfigError {
            status: StatusCode::BAD_GATEWAY,
            name: "RustMcpOAuthRegistrationError",
            message: format!("MCP OAuth dynamic client registration failed: {err}"),
        })?;
    if !res.status().is_success() {
        return Err(McpOAuthConfigError {
            status: StatusCode::BAD_GATEWAY,
            name: "RustMcpOAuthRegistrationError",
            message: format!(
                "MCP OAuth dynamic client registration returned HTTP {}",
                res.status()
            ),
        });
    }
    let data: Value = res.json().await.map_err(|err| McpOAuthConfigError {
        status: StatusCode::BAD_GATEWAY,
        name: "RustMcpOAuthRegistrationError",
        message: format!("MCP OAuth dynamic client registration returned invalid JSON: {err}"),
    })?;
    let client = data
        .get("client_id")
        .or_else(|| data.get("clientId"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| McpOAuthConfigError {
            status: StatusCode::BAD_GATEWAY,
            name: "RustMcpOAuthRegistrationError",
            message: "MCP OAuth dynamic client registration did not return client_id.".to_string(),
        })?;
    let secret = data
        .get("client_secret")
        .or_else(|| data.get("clientSecret"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    Ok((client.to_string(), secret))
}

fn mcp_oauth_authorize_url(
    cfg: &McpOAuthConfig,
    state: &str,
    challenge: &str,
) -> Result<String, String> {
    let mut url = reqwest::Url::parse(&cfg.authorize)
        .map_err(|err| format!("Invalid MCP OAuth authorizeUrl: {err}"))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("response_type", "code");
        pairs.append_pair("client_id", &cfg.client);
        pairs.append_pair("redirect_uri", &cfg.redirect);
        pairs.append_pair("state", state);
        pairs.append_pair("code_challenge", challenge);
        pairs.append_pair("code_challenge_method", "S256");
        if let Some(scope) = cfg.scope.as_ref() {
            pairs.append_pair("scope", scope);
        }
    }
    Ok(url.to_string())
}

async fn mcp_oauth_exchange(
    cfg: &McpOAuthConfig,
    code: &str,
    verifier: &str,
) -> Result<Value, McpRemoteError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(MCP_DEFAULT_TIMEOUT_MS))
        .build()
        .map_err(|err| McpRemoteError::Auth(format!("MCP OAuth token client failed: {err}")))?;
    let mut form = vec![
        ("grant_type".to_string(), "authorization_code".to_string()),
        ("code".to_string(), code.to_string()),
        ("redirect_uri".to_string(), cfg.redirect.clone()),
        ("client_id".to_string(), cfg.client.clone()),
        ("code_verifier".to_string(), verifier.to_string()),
    ];
    if let Some(secret) = cfg.secret.as_ref() {
        form.push(("client_secret".to_string(), secret.clone()));
    }
    let res = client
        .post(&cfg.token)
        .form(&form)
        .send()
        .await
        .map_err(|err| McpRemoteError::Auth(format!("MCP OAuth token request failed: {err}")))?;
    if !res.status().is_success() {
        return Err(McpRemoteError::Auth(format!(
            "MCP OAuth token exchange returned HTTP {}",
            res.status()
        )));
    }
    let data: Value = res.json().await.map_err(|err| {
        McpRemoteError::Auth(format!(
            "MCP OAuth token exchange returned invalid JSON: {err}"
        ))
    })?;
    if data
        .get("access_token")
        .or_else(|| data.get("accessToken"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        return Err(McpRemoteError::Auth(
            "MCP OAuth token exchange did not return an access token.".to_string(),
        ));
    }
    Ok(data)
}

fn mcp_oauth_random() -> String {
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn mcp_oauth_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

fn mcp_oauth_expire_pending(auth: &mut Value) {
    let expired = auth
        .get("pending")
        .and_then(|value| value.get("createdAt"))
        .and_then(Value::as_i64)
        .is_some_and(|at| mcp_now_seconds() - at > MCP_OAUTH_PENDING_TTL_SECONDS);
    if expired {
        auth.as_object_mut().map(|map| map.remove("pending"));
    }
}

fn mcp_auth_expired(auth: &Value) -> bool {
    let Some(value) = auth.get("expiresAt") else {
        return false;
    };
    let at = value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
        .unwrap_or(0);
    at <= mcp_now_seconds()
}

fn mcp_now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(crate) fn mcp_parse_sse_response(text: &str, id: i64) -> Result<Value, McpRemoteError> {
    let mut data = Vec::new();
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(value) = line.strip_prefix("data:") {
            data.push(value.trim_start().to_string());
            continue;
        }
        if !line.is_empty() {
            continue;
        }
        if let Some(value) = mcp_sse_data(&data, id)? {
            return Ok(value);
        }
        data.clear();
    }
    if let Some(value) = mcp_sse_data(&data, id)? {
        return Ok(value);
    }
    Err(McpRemoteError::Malformed(format!(
        "MCP remote SSE response missing JSON-RPC id {id}"
    )))
}

fn mcp_sse_data(data: &[String], id: i64) -> Result<Option<Value>, McpRemoteError> {
    if data.is_empty() {
        return Ok(None);
    }
    let raw = data.join("\n");
    if raw.trim() == "[DONE]" {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(&raw).map_err(|err| {
        McpRemoteError::Malformed(format!("MCP remote SSE data is invalid JSON: {err}"))
    })?;
    if value.get("id").and_then(Value::as_i64) == Some(id) {
        return Ok(Some(value));
    }
    Ok(None)
}

pub(crate) enum McpRemoteError {
    Auth(String),
    Config(String),
    Http(String),
    Timeout(String),
    Malformed(String),
    Rpc(String),
}

impl McpRemoteError {
    fn name(&self) -> &'static str {
        match self {
            Self::Auth(_) => "RustMcpAuthRefreshError",
            Self::Config(_) => "RustMcpRemoteConfigError",
            Self::Http(_) => "RustMcpHttpError",
            Self::Timeout(_) => "RustMcpTimeoutError",
            Self::Malformed(_) => "RustMcpMalformedResponseError",
            Self::Rpc(_) => "RustMcpToolError",
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::Auth(_) => StatusCode::UNAUTHORIZED,
            Self::Config(_) => StatusCode::BAD_REQUEST,
            Self::Http(_) | Self::Malformed(_) | Self::Rpc(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
        }
    }

    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Auth(err)
            | Self::Config(err)
            | Self::Http(err)
            | Self::Timeout(err)
            | Self::Malformed(err)
            | Self::Rpc(err) => err,
        }
    }
}

pub(crate) enum McpCallError {
    Write(String),
    Timeout(String),
    Closed(String),
    Malformed(String),
    Rpc(String),
}

impl McpCallError {
    fn from_wait(err: String) -> Self {
        if err.contains("timed out") {
            return Self::Timeout(err);
        }
        if err.contains("stdout closed") || err.contains("server exited") {
            return Self::Closed(err);
        }
        Self::Malformed(err)
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Write(_) => "RustMcpWriteError",
            Self::Timeout(_) => "RustMcpTimeoutError",
            Self::Closed(_) => "RustMcpClosedError",
            Self::Malformed(_) => "RustMcpMalformedResponseError",
            Self::Rpc(_) => "RustMcpToolError",
        }
    }

    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Write(err)
            | Self::Timeout(err)
            | Self::Closed(err)
            | Self::Malformed(err)
            | Self::Rpc(err) => err,
        }
    }
}

pub(crate) fn mcp_error(
    status: StatusCode,
    name: &str,
    server: &str,
    message: impl Into<String>,
) -> Response {
    (status, Json(kilo_mcp::error(name, server, message))).into_response()
}

pub(crate) fn mcp_reap_exited(state: &AppState) {
    let mut failed = Vec::new();
    {
        let mut children = state.mcp_children.lock().unwrap();
        children.retain(|name, child| match child.child.try_wait() {
            Ok(Some(status)) => {
                failed.push((name.clone(), format!("MCP server exited with {status}")));
                false
            }
            Ok(None) => true,
            Err(err) => {
                failed.push((
                    name.clone(),
                    format!("Unable to check MCP server status: {err}"),
                ));
                false
            }
        });
    }
    if failed.is_empty() {
        return;
    }
    let mut status = state.mcp.lock().unwrap();
    for (name, error) in failed {
        status.insert(name, kilo_mcp::Status::Failed { error });
    }
}

pub(crate) fn mcp_not_implemented(name: &str) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(kilo_mcp::not_implemented(name)),
    )
        .into_response()
}
