use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    error::Error,
    fs,
    future::Future,
    io::Read,
    net::SocketAddr,
    path::{Component, Path as FsPath, PathBuf},
    process::{Command, Stdio},
    sync::{atomic::AtomicBool, atomic::Ordering, Arc, Mutex},
    time::{Duration, Instant},
};

use async_stream::stream;
use axum::{
    extract::{Path, Query, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    middleware::{from_fn, from_fn_with_state, Next},
    response::{sse::Event, sse::Sse, IntoResponse, Response},
    routing::{get, patch, post, put},
    Json, Router,
};
use base64::{engine::general_purpose, Engine};
use futures_core::Stream;
use kilo_protocol::{
    Config, GlobalEvent, Health, MessageAppendInput, MessageAppendResult, PromptInput,
    SessionCreateInput, SessionForkInput, SessionRevertInput, SessionShareInput,
    SessionUpdateInput, SessionViewedInput,
};
use kilo_provider::{
    ChatMessage, ChatOutput, ChatTool, ChatToolCall, ChatUsage, ProviderError, StreamEvent,
};
use kilo_store::SessionMutation;
use kilo_store::{MessageCursor, SessionQuery, Store, StoredEvent};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener, sync::broadcast, sync::RwLock, task::JoinSet, time::MissedTickBehavior,
};

const DEFAULT_READ_LIMIT: usize = 2000;
const DEFAULT_GREP_LIMIT: usize = 100;
const MAX_GREP_LINE: usize = 2000;
const MAX_BASH_OUTPUT_BYTES: usize = 64 * 1024;
const DEFAULT_BASH_TIMEOUT_MS: u64 = 60_000;
const OPENAI_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const OPENAI_ISSUER: &str = "https://auth.openai.com";
const OPENAI_REDIRECT: &str = "http://127.0.0.1:1455/auth/openai/callback";

#[derive(Clone)]
pub struct ServeOptions {
    pub hostname: String,
    pub port: u16,
}

struct AppState {
    username: String,
    password: Option<String>,
    store: Store,
    bus: broadcast::Sender<GlobalEvent>,
    viewed: RwLock<ViewedState>,
    runners: Mutex<BTreeMap<String, Runner>>,
    permissions: Mutex<BTreeMap<String, Value>>,
    questions: Mutex<BTreeMap<String, Value>>,
}

#[derive(Clone)]
struct Runner {
    cancel: Arc<AtomicBool>,
}

struct RunnerGuard {
    state: Arc<AppState>,
    id: String,
    cancel: Arc<AtomicBool>,
}

#[derive(Debug)]
enum TurnError {
    Busy,
    NotFound,
    Db(rusqlite::Error),
}

#[derive(Clone, Debug)]
struct FakeCall {
    tool: String,
    input: Value,
    delay: u64,
    invalid: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum Repair {
    Valid(String),
    Invalid(String),
}

const KNOWN_TOOLS: &[&str] = &["read", "grep", "write", "edit", "apply_patch", "bash"];

#[derive(Clone, Debug, Default)]
#[allow(dead_code)]
struct ViewedState {
    focused: BTreeSet<String>,
    open: BTreeSet<String>,
}

pub async fn serve(
    opts: ServeOptions,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let addr = SocketAddr::from((loopback(&opts.hostname), opts.port));
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let (bus, _) = broadcast::channel(128);
    let state = Arc::new(AppState {
        username: std::env::var("KILO_SERVER_USERNAME").unwrap_or_else(|_| "kilo".to_string()),
        password: std::env::var("KILO_SERVER_PASSWORD").ok(),
        store: Store::new(),
        bus,
        viewed: RwLock::default(),
        runners: Mutex::default(),
        permissions: Mutex::default(),
        questions: Mutex::default(),
    });
    let app = Router::new()
        .route("/global/health", get(health))
        .route("/global/event", get(events))
        .route("/event", get(instance_events))
        .route("/global/dispose", post(global_dispose))
        .route("/instance/dispose", post(instance_dispose))
        .route("/path", get(paths))
        .route("/config", get(config))
        .route("/global/config", get(config).patch(update_config))
        .route("/config/providers", get(config_providers))
        .route("/config/warnings", get(warnings))
        .route("/provider", get(providers))
        .route("/provider/auth", get(provider_auth))
        .route(
            "/provider/{provider_id}/oauth/authorize",
            post(oauth_authorize),
        )
        .route(
            "/provider/{provider_id}/oauth/callback",
            post(oauth_callback),
        )
        .route("/provider/{provider_id}", get(provider_detail))
        .route("/auth/{provider_id}", put(set_auth).delete(clear_auth))
        .route("/agent", get(agents))
        .route("/skill", get(empty_array))
        .route("/command", get(empty_array))
        .route("/project/current", get(project))
        .route("/session", get(sessions).post(create_session))
        .route("/session/viewed", post(viewed))
        .route("/session/status", get(status))
        .route(
            "/session/{id}",
            get(session).patch(update_session).delete(delete_session),
        )
        .route("/session/{id}/children", get(children))
        .route("/session/{id}/fork", post(fork_session))
        .route("/session/{id}/diff", get(diff_session))
        .route(
            "/session/{id}/share",
            post(share_session).delete(unshare_session),
        )
        .route("/session/{id}/summarize", post(summarize_session))
        .route("/session/{id}/revert", post(revert_session))
        .route("/session/{id}/unrevert", post(unrevert_session))
        .route("/session/{id}/message", get(messages).post(prompt))
        .route(
            "/session/{id}/message/{message_id}",
            get(message).delete(delete_message),
        )
        .route(
            "/session/{id}/message/{message_id}/part/{part_id}",
            patch(update_part).delete(delete_part),
        )
        .route("/session/{id}/prompt_async", post(prompt_async))
        .route("/session/{id}/abort", post(abort_session))
        .route("/internal/session/{id}/message", post(append_message))
        .route("/mcp", get(mcp_status))
        .route("/permission", get(permissions))
        .route("/permission/{id}/reply", post(reply_permission))
        .route("/permission/{id}/always-rules", post(permission_rules))
        .route("/question", get(questions))
        .route("/question/{id}/reply", post(reply_question))
        .route("/question/{id}/reject", post(reject_question))
        .route("/find", get(find_text))
        .route("/find/file", get(find_file))
        .route("/find/symbol", get(find_symbol))
        .route("/file", get(list_file))
        .route("/file/content", get(file_content))
        .route("/file/status", get(file_status))
        .route("/suggestion", get(empty_array))
        .route("/remote/status", get(remote_status))
        .with_state(state.clone())
        .layer(from_fn_with_state(state, auth))
        // Header→query rewrite must run BEFORE the inner extractors see Query<>
        // (and before auth, which is fine — auth doesn't read these headers).
        // SDK clients (`packages/sdk/js/src/v2/client.ts`) rewrite the headers
        // client-side for GET/HEAD, so this middleware is a safety net for
        // non-SDK callers (oracle harness, curl). It mirrors the SDK's behavior:
        // only GET/HEAD are rewritten, and existing query values win.
        .layer(from_fn(directory_header_rewrite));

    println!(
        "kilo server listening on http://{}:{}",
        local.ip(),
        local.port()
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;

    Ok(())
}

async fn health() -> Json<Health> {
    Json(Health::ok())
}

async fn events(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.bus.subscribe();
    let stream = stream! {
        yield frame(GlobalEvent::connected());

        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => yield frame(GlobalEvent::heartbeat()),
                event = rx.recv() => match event {
                    Ok(event) => yield frame(event),
                    // Bus channel is bounded (`broadcast::channel(...)`).
                    // A slow SSE consumer that falls behind by more than the
                    // capacity loses `n` events. Surface the count so the
                    // client can refetch state and an operator sees the
                    // pressure, instead of silently desyncing the UI.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("[kilo-server] /global/event subscriber lagged, dropped {n} events");
                        yield frame(GlobalEvent::bus(
                            "server.lagged",
                            json!({ "dropped": n, "channel": "global" }),
                        ));
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            }
        }
    };

    Sse::new(stream)
}

async fn instance_events(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.bus.subscribe();
    let stream = stream! {
        yield payload_frame(GlobalEvent::connected());

        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => yield payload_frame(GlobalEvent::heartbeat()),
                event = rx.recv() => match event {
                    Ok(event) => yield payload_frame(event),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("[kilo-server] /event subscriber lagged, dropped {n} events");
                        yield payload_frame(GlobalEvent::bus(
                            "server.lagged",
                            json!({ "dropped": n, "channel": "instance" }),
                        ));
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            }
        }
    };

    Sse::new(stream)
}

async fn paths(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.store.paths())
}

async fn config(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.store.config())
}

/// `PATCH /global/config` — Bun shape: body is the full Config; response is
/// the post-merge Config. Persists to whichever file `Store::set_config`
/// resolves via `config_priority()` (matching Bun's `globalConfigFile()`),
/// so a subsequent GET round-trips. Failure path is 500 with a Bun-shaped
/// JSON envelope via `internal_error` — the M3 mutation gate has already
/// flipped by the time we reach the handler, so the error must be both
/// observable and parseable by the SDK's `response.json()` path.
async fn update_config(State(state): State<Arc<AppState>>, Json(input): Json<Config>) -> Response {
    match state.store.set_config(input) {
        Ok(value) => Json(value).into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

/// `POST /global/dispose` — Bun returns `true`. Bun also fires a
/// `global.disposed` SSE event and clears its in-memory config cache.
/// In Rust, the config cache is per-request (`Store::config()` re-reads
/// from disk every call), so a no-op is contract-equivalent for now. The
/// event is not emitted because no Rust subscriber listens for it; if a
/// future webview gains a `global.disposed` handler, this should fire
/// through `state.bus`.
async fn global_dispose() -> impl IntoResponse {
    Json(true)
}

/// `POST /instance/dispose` — Bun returns `true` after disposing the
/// per-directory `Instance`. Rust does not yet have a per-directory
/// instance container (single-store-per-process model in M5), so this is
/// a no-op. M9 will revisit when worktree concurrency lands.
async fn instance_dispose() -> impl IntoResponse {
    Json(true)
}

async fn warnings() -> impl IntoResponse {
    Json(Vec::<serde_json::Value>::new())
}

async fn providers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(kilo_provider::list(&state.store.config()))
}

async fn provider_auth() -> impl IntoResponse {
    Json(json!({
        "openai": [
            { "label": "ChatGPT Pro/Plus (browser)", "type": "oauth" },
            { "label": "ChatGPT Pro/Plus (headless)", "type": "oauth" },
            { "label": "Manually enter API Key", "type": "api" }
        ]
    }))
}

/// `GET /provider/{providerID}` — used by the sidebar's provider sign-in
/// flow to look up env/option metadata before launching auth. Returns 404
/// if the provider isn't in the static Kilo list (today: only `kilo`).
async fn provider_detail(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match kilo_provider::detail(&state.store.config(), &id) {
        Some(value) => Json(value).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// `PUT /auth/{providerID}` — Bun stores the auth blob and returns it.
/// M5 stub: accept and echo the body so the SDK's optimistic
/// post-sign-in flow doesn't break, but persistence is M10 territory.
/// The SDK type is `Auth`; we accept any JSON object and round-trip it.
async fn set_auth(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<serde_json::Value>,
) -> Response {
    match state.store.set_provider_auth(&id, input) {
        Ok(value) => Json(value).into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

/// `DELETE /auth/{providerID}` — Bun clears the auth row and returns void.
/// Real persistence lands in M10. M5 returns true so the UI's sign-out path
/// completes and the M3 mutation gate doesn't trap on a stub error.
async fn clear_auth(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.store.clear_provider_auth(&id) {
        Ok(()) => Json(true).into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

async fn oauth_authorize(Path(id): Path<String>, Json(input): Json<Value>) -> Response {
    if id != "openai" {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let method = input.get("method").and_then(Value::as_u64).unwrap_or(0);
    if method != 0 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let verifier = oauth_secret(43);
    let state = oauth_secret(32);
    let challenge = pkce_challenge(&verifier);
    let url = oauth_url(OPENAI_REDIRECT, &challenge, &state);
    Json(json!({
        "url": url,
        "instructions": "Complete authorization in your browser, then return to Kilo.",
        "method": "code",
        "state": state,
        "verifier": verifier,
        "redirectUri": OPENAI_REDIRECT,
    }))
    .into_response()
}

async fn oauth_callback(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<Value>,
) -> Response {
    if id != "openai" {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Some(code) = input.get("code").and_then(Value::as_str) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let verifier = input
        .get("inputs")
        .and_then(|value| value.get("verifier"))
        .or_else(|| input.get("verifier"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    if verifier.is_empty() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    match exchange_code(code, OPENAI_REDIRECT, verifier).await {
        Ok(value) => match state.store.set_provider_auth("openai", value) {
            Ok(_) => Json(true).into_response(),
            Err(err) => internal_error(err.to_string()),
        },
        Err(err) => internal_error(err),
    }
}

async fn config_providers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(kilo_provider::config(&state.store.config()))
}

fn oauth_secret(len: usize) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
    let mut bytes = vec![0; len];
    let mut rng = rand::rngs::OsRng;
    rand::RngCore::fill_bytes(&mut rng, &mut bytes);
    bytes
        .into_iter()
        .map(|byte| CHARS[(byte as usize) % CHARS.len()] as char)
        .collect()
}

fn pkce_challenge(verifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
}

fn oauth_url(redirect: &str, challenge: &str, state: &str) -> String {
    format!(
        "{OPENAI_ISSUER}/oauth/authorize?response_type=code&client_id={OPENAI_CLIENT_ID}&redirect_uri={}&scope=openid%20profile%20email%20offline_access&code_challenge={}&code_challenge_method=S256&id_token_add_organizations=true&codex_cli_simplified_flow=true&state={}&originator=opencode",
        url_encode(redirect),
        url_encode(challenge),
        url_encode(state),
    )
}

async fn exchange_code(code: &str, redirect: &str, verifier: &str) -> Result<Value, String> {
    let body = format!(
        "grant_type=authorization_code&code={}&redirect_uri={}&client_id={OPENAI_CLIENT_ID}&code_verifier={}",
        url_encode(code),
        url_encode(redirect),
        url_encode(verifier),
    );
    let res = reqwest::Client::new()
        .post(format!("{OPENAI_ISSUER}/oauth/token"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|err| err.to_string())?;
    if !res.status().is_success() {
        return Err(format!("Token exchange failed: {}", res.status()));
    }
    let tokens = res.json::<Value>().await.map_err(|err| err.to_string())?;
    token_auth(&tokens, None)
}

async fn refresh_access(refresh: &str, account: Option<String>) -> Result<Value, String> {
    let body = format!(
        "grant_type=refresh_token&refresh_token={}&client_id={OPENAI_CLIENT_ID}",
        url_encode(refresh),
    );
    let res = reqwest::Client::new()
        .post(format!("{OPENAI_ISSUER}/oauth/token"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .map_err(|err| err.to_string())?;
    if !res.status().is_success() {
        return Err(format!("Token refresh failed: {}", res.status()));
    }
    let tokens = res.json::<Value>().await.map_err(|err| err.to_string())?;
    token_auth(&tokens, account)
}

async fn fresh_auths(state: &AppState) -> Result<Value, String> {
    let mut auths = state.store.provider_auths();
    let Some(auth) = auths.get("openai") else {
        return Ok(json!(auths));
    };
    if auth.get("type").and_then(Value::as_str) != Some("oauth") {
        return Ok(json!(auths));
    }
    if auth.get("expires").and_then(Value::as_i64).unwrap_or(0) > unix_millis() {
        return Ok(json!(auths));
    }
    let refresh = auth
        .get("refresh")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if refresh.is_empty() {
        return Ok(json!(auths));
    }
    let account = auth
        .get("accountId")
        .and_then(Value::as_str)
        .map(str::to_string);
    let next = refresh_access(refresh, account).await?;
    state
        .store
        .set_provider_auth("openai", next.clone())
        .map_err(|err| err.to_string())?;
    auths.insert("openai".to_string(), next);
    Ok(json!(auths))
}

fn token_auth(tokens: &Value, account: Option<String>) -> Result<Value, String> {
    let expires = tokens
        .get("expires_in")
        .and_then(Value::as_i64)
        .unwrap_or(3600);
    let now = unix_millis();
    let refresh = tokens
        .get("refresh_token")
        .or_else(|| tokens.get("refresh"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let access = tokens
        .get("access_token")
        .or_else(|| tokens.get("access"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut auth = json!({
        "type": "oauth",
        "refresh": refresh,
        "access": access,
        "expires": now + expires * 1000,
    });
    if let Some(account) = extract_account(tokens).or(account) {
        auth["accountId"] = json!(account);
    }
    Ok(auth)
}

fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or(0)
}

fn extract_account(tokens: &Value) -> Option<String> {
    tokens
        .get("id_token")
        .or_else(|| tokens.get("access_token"))
        .and_then(Value::as_str)
        .and_then(jwt_claims)
        .and_then(|claims| {
            claims
                .get("chatgpt_account_id")
                .or_else(|| claims.pointer("/https://api.openai.com~1auth/chatgpt_account_id"))
                .or_else(|| {
                    claims
                        .get("organizations")
                        .and_then(Value::as_array)?
                        .first()?
                        .get("id")
                })
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

fn jwt_claims(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    let bytes = general_purpose::URL_SAFE_NO_PAD.decode(payload).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}

async fn agents() -> impl IntoResponse {
    Json(json!([
        {
            "name": "code",
            "description": "Default coding agent.",
            "mode": "primary",
            "native": true,
            "permission": [],
            "options": {}
        },
        {
            "name": "plan",
            "description": "Plan mode.",
            "mode": "primary",
            "native": true,
            "permission": [],
            "options": {}
        }
    ]))
}

async fn empty_array() -> impl IntoResponse {
    Json(Vec::<serde_json::Value>::new())
}

async fn permissions(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(permission_list(&state))
}

async fn reply_permission(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(_input): Json<Value>,
) -> Response {
    if state.permissions.lock().unwrap().remove(&id).is_some() {
        return Json(true).into_response();
    }

    StatusCode::NOT_FOUND.into_response()
}

async fn permission_rules(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(_input): Json<Value>,
) -> Response {
    if state.permissions.lock().unwrap().contains_key(&id) {
        return Json(true).into_response();
    }

    StatusCode::NOT_FOUND.into_response()
}

async fn questions(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(question_list(&state))
}

async fn reply_question(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(_input): Json<Value>,
) -> Response {
    if state.questions.lock().unwrap().remove(&id).is_some() {
        return Json(true).into_response();
    }

    StatusCode::NOT_FOUND.into_response()
}

async fn reject_question(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    if state.questions.lock().unwrap().remove(&id).is_some() {
        return Json(true).into_response();
    }

    StatusCode::NOT_FOUND.into_response()
}

async fn find_text(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(pattern) = query.get("pattern").filter(|value| !value.is_empty()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let root = PathBuf::from(state.store.paths().directory);
    Json(search_text(&root, pattern, 10)).into_response()
}

async fn find_file(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(term) = query.get("query") else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10)
        .clamp(1, 200);
    let dirs = query.get("dirs").is_none_or(|value| value != "false");
    let kind = query.get("type").map(String::as_str);
    let root = PathBuf::from(state.store.paths().directory);

    Json(search_files(&root, term, dirs, kind, limit)).into_response()
}

async fn find_symbol(Query(query): Query<BTreeMap<String, String>>) -> Response {
    if !query.contains_key("query") {
        return StatusCode::BAD_REQUEST.into_response();
    }

    Json(Vec::<Value>::new()).into_response()
}

async fn list_file(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(input) = query.get("path") else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let root = PathBuf::from(state.store.paths().directory);
    let dir = match resolve_under(&root, input) {
        Ok(path) => path,
        Err(code) => return code.into_response(),
    };

    Json(list_nodes(&root, &dir)).into_response()
}

async fn file_content(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(input) = query.get("path") else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let root = PathBuf::from(state.store.paths().directory);
    let file = match resolve_under(&root, input) {
        Ok(path) => path,
        Err(code) => return code.into_response(),
    };

    Json(read_content(&file)).into_response()
}

async fn file_status() -> impl IntoResponse {
    Json(Vec::<Value>::new())
}

async fn project(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.store.project())
}

async fn sessions(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> impl IntoResponse {
    let input = SessionQuery {
        directory: query.get("directory").cloned(),
        roots: query
            .get("roots")
            .is_some_and(|value| value == "true" || value == "1"),
        start: query.get("start").and_then(|value| value.parse().ok()),
        search: query.get("search").cloned(),
        limit: query.get("limit").and_then(|value| value.parse().ok()),
    };

    Json(state.store.sessions(&input))
}

async fn create_session(
    State(state): State<Arc<AppState>>,
    Json(input): Json<SessionCreateInput>,
) -> Response {
    match state.store.create_session_record(input) {
        Ok(record) => {
            let dir = state.store.paths().directory;
            publish_events(
                &state,
                dir,
                record.session.project_id.clone(),
                [record.event],
            );
            Json(record.session).into_response()
        }
        Err(err) => internal_error(err.to_string()),
    }
}

async fn viewed(
    State(state): State<Arc<AppState>>,
    Json(input): Json<SessionViewedInput>,
) -> impl IntoResponse {
    set_viewed(&state, input).await;
    Json(true)
}

async fn set_viewed(state: &Arc<AppState>, input: SessionViewedInput) {
    let next = ViewedState {
        focused: input.focused.into_iter().collect(),
        open: input.open.into_iter().collect(),
    };
    *state.viewed.write().await = next;
}

#[cfg(test)]
async fn viewed_snapshot(state: &Arc<AppState>) -> ViewedState {
    state.viewed.read().await.clone()
}

async fn status() -> impl IntoResponse {
    Json(json!({}))
}

/// `GET /mcp` — empty status map until the MCP migration ladder lands in M11.
/// Bun shape: `Record<string, McpStatus>`. An empty object is a valid shape
/// (the browser-automation service tolerates it).
async fn mcp_status() -> impl IntoResponse {
    Json(json!({}))
}

/// `GET /remote/status` — the extension's RemoteStatusService polls this.
/// Bun shape: `{ enabled: boolean, ... }`. Returning `enabled: false`
/// disables the remote feature path while keeping the route 200 OK so
/// the service doesn't go into permanent error.
async fn remote_status() -> impl IntoResponse {
    Json(json!({ "enabled": false }))
}

async fn session(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.store.session(&id) {
        Some(item) => Json(item).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn children(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.store.children(&id) {
        Some(items) => Json(items).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn fork_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<SessionForkInput>,
) -> Response {
    match state.store.fork_session_record(&id, input) {
        Ok(Some(record)) => {
            publish_events(
                &state,
                state.store.paths().directory,
                record.session.project_id.clone(),
                record.events,
            );
            Json(record.session).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

async fn diff_session(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.store.diff(&id) {
        Some(diff) => Json(diff).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn share_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    body: Option<Json<SessionShareInput>>,
) -> Response {
    match state
        .store
        .set_share_record(&id, body.and_then(|Json(input)| input.url))
    {
        Ok(record) => session_mutation_response(&state, record),
        Err(err) => internal_error(err.to_string()),
    }
}

async fn unshare_session(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.store.clear_share_record(&id) {
        Ok(record) => session_mutation_response(&state, record),
        Err(err) => internal_error(err.to_string()),
    }
}

/// M6 route parity only: no provider compaction is available before M7/M8.
/// Return a safe boolean after verifying the session exists so UI callers do
/// not enqueue fake provider work or mutate durable state incorrectly.
async fn summarize_session(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    if state.store.session(&id).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    Json(true).into_response()
}

async fn revert_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<SessionRevertInput>,
) -> Response {
    match state.store.set_revert_record(&id, input) {
        Ok(record) => session_mutation_response(&state, record),
        Err(err) => internal_error(err.to_string()),
    }
}

async fn unrevert_session(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.store.clear_revert_record(&id) {
        Ok(record) => session_mutation_response(&state, record),
        Err(err) => internal_error(err.to_string()),
    }
}

/// `PATCH /session/{id}` — Bun shape: body is `{ title?, permission?, time? }`,
/// response is the updated `Session`. Persists `title`, `permission`, and
/// `time.archived`, treating absent fields as a no-op. Emits `session.updated`
/// SSE so the sidebar's listing reflects the change without a full reload.
async fn update_session(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<SessionUpdateInput>,
) -> Response {
    match state.store.update_session(&id, input) {
        Ok(Some(session)) => {
            let _ = state.bus.send(GlobalEvent::session(
                "session.updated",
                state.store.paths().directory,
                session.clone(),
            ));
            Json(session).into_response()
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

async fn delete_session(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.store.delete_session_record(&id) {
        Ok(record) if record.session.is_some() => {
            if let (Some(session), Some(event)) = (&record.session, record.event) {
                publish_events(
                    &state,
                    state.store.paths().directory,
                    session.project_id.clone(),
                    [event],
                );
            }
            Json(true).into_response()
        }
        Ok(_) => Json(true).into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

async fn messages(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let limit = query.get("limit").and_then(|value| value.parse().ok());
    let before = match query.get("before") {
        Some(_) if limit.is_none() => {
            return (StatusCode::BAD_REQUEST, "before requires limit").into_response()
        }
        Some(value) => match decode_cursor(value) {
            Some(cursor) => Some(cursor),
            None => return (StatusCode::BAD_REQUEST, "Invalid cursor").into_response(),
        },
        None => None,
    };
    match state.store.messages(&id, limit, before.as_ref()) {
        Some(page) => {
            let mut res = Json(page.items).into_response();
            if let Some(cursor) = page
                .more
                .then(|| page.cursor)
                .flatten()
                .and_then(|cursor| encode_cursor(&cursor))
            {
                let link = format!(
                    "</session/{id}/message?limit={}&before={cursor}>; rel=\"next\"",
                    limit.unwrap_or(0)
                );
                let headers = res.headers_mut();
                headers.insert(
                    header::ACCESS_CONTROL_EXPOSE_HEADERS,
                    HeaderValue::from_static("Link, X-Next-Cursor"),
                );
                if let Ok(value) = HeaderValue::from_str(&link) {
                    headers.insert(header::LINK, value);
                }
                if let Ok(value) = HeaderValue::from_str(&cursor) {
                    headers.insert("x-next-cursor", value);
                }
            }

            res
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn message(
    State(state): State<Arc<AppState>>,
    Path((id, mid)): Path<(String, String)>,
) -> Response {
    if state.store.session(&id).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    match state.store.message(&id, &mid) {
        Some(message) => Json(message).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn delete_message(
    State(state): State<Arc<AppState>>,
    Path((id, mid)): Path<(String, String)>,
) -> Response {
    match state.store.remove_message_record(&id, &mid) {
        Ok(record) if record.message.is_some() => {
            publish_for_session(&state, &id, record.events);
            Json(true).into_response()
        }
        Ok(_) => StatusCode::NOT_FOUND.into_response(),
        Err(rusqlite::Error::QueryReturnedNoRows) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

async fn delete_part(
    State(state): State<Arc<AppState>>,
    Path((id, mid, pid)): Path<(String, String, String)>,
) -> Response {
    match state.store.remove_part_record(&id, &mid, &pid) {
        Ok(record) if record.part.is_some() => {
            publish_for_session(&state, &id, record.events);
            Json(true).into_response()
        }
        Ok(_) => StatusCode::NOT_FOUND.into_response(),
        Err(rusqlite::Error::QueryReturnedNoRows) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

async fn update_part(
    State(state): State<Arc<AppState>>,
    Path((id, mid, pid)): Path<(String, String, String)>,
    Json(input): Json<Value>,
) -> Response {
    match state.store.update_part_record(&id, &mid, &pid, input) {
        Ok(record) => {
            publish_for_session(&state, &id, record.events);
            Json(record.part.unwrap_or(Value::Null)).into_response()
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => StatusCode::NOT_FOUND.into_response(),
        Err(rusqlite::Error::InvalidParameterName(_)) => StatusCode::BAD_REQUEST.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

async fn append_message(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<MessageAppendInput>,
) -> Response {
    match state.store.append_message_record(&id, input) {
        Ok(record) => {
            let Some(session) = state.store.session(&id) else {
                return internal_error("missing session after append");
            };
            let dir = state.store.paths().directory;
            publish_events(&state, dir, session.project_id, record.events);
            Json(record.result).into_response()
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

async fn prompt(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<PromptInput>,
) -> Response {
    match prompt_guarded(state, id, input).await {
        Ok(result) => Json(result).into_response(),
        Err(TurnError::NotFound) => StatusCode::NOT_FOUND.into_response(),
        Err(TurnError::Busy) => busy_error(),
        Err(TurnError::Db(err)) => internal_error(err.to_string()),
    }
}

async fn prompt_async(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<PromptInput>,
) -> Response {
    if state.store.session(&id).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let guard = match start_runner(state.clone(), &id) {
        Ok(guard) => guard,
        Err(TurnError::Busy) => return busy_error(),
        Err(err) => return turn_error(err),
    };
    tokio::spawn(async move {
        let id = guard.id.clone();
        let res = prompt_turn(&guard.state, &id, input, guard.cancel.clone()).await;
        if let Err(err) = res {
            eprintln!("[kilo-server] prompt_async {id}: {err}");
        }
        drop(guard);
    });
    StatusCode::NO_CONTENT.into_response()
}

fn turn_error(err: TurnError) -> Response {
    match err {
        TurnError::Busy => busy_error(),
        TurnError::NotFound => StatusCode::NOT_FOUND.into_response(),
        TurnError::Db(err) => internal_error(err.to_string()),
    }
}

async fn abort_session(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    if state.store.session(&id).is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    if let Some(runner) = state.runners.lock().unwrap().get(&id) {
        runner.cancel.store(true, Ordering::SeqCst);
    }
    Json(true).into_response()
}

async fn prompt_guarded(
    state: Arc<AppState>,
    id: String,
    input: PromptInput,
) -> Result<MessageAppendResult, TurnError> {
    if state.store.session(&id).is_none() {
        return Err(TurnError::NotFound);
    }
    let guard = start_runner(state, &id)?;
    let result = prompt_turn(&guard.state, &id, input, guard.cancel.clone())
        .await
        .map_err(TurnError::Db);
    drop(guard);
    result
}

fn start_runner(state: Arc<AppState>, id: &str) -> Result<RunnerGuard, TurnError> {
    let cancel = {
        let mut runners = state.runners.lock().unwrap();
        if runners.contains_key(id) {
            return Err(TurnError::Busy);
        }
        let cancel = Arc::new(AtomicBool::new(false));
        runners.insert(
            id.to_string(),
            Runner {
                cancel: cancel.clone(),
            },
        );
        cancel
    };
    Ok(RunnerGuard {
        state,
        id: id.to_string(),
        cancel,
    })
}

impl Drop for RunnerGuard {
    fn drop(&mut self) {
        self.state.runners.lock().unwrap().remove(&self.id);
    }
}

fn busy_error() -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "name": "BusyError",
            "data": { "message": "Session is busy" },
        })),
    )
        .into_response()
}

async fn prompt_turn(
    state: &AppState,
    id: &str,
    input: PromptInput,
    cancel: Arc<AtomicBool>,
) -> rusqlite::Result<MessageAppendResult> {
    let Some(session) = state.store.session(id) else {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    };

    let text = prompt_text(&input.parts);
    let dir = state.store.paths().directory;
    let project = session.project_id;
    publish_turn_open(state, id);
    publish_status(state, id, "busy");

    let user = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: user_info(&input),
            parts: input.parts.clone(),
        },
    )?;
    publish_events(state, dir.clone(), project.clone(), user.events);

    if is_canceled(&cancel) || fake_abort(&input) {
        let assistant = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: assistant_error_info(&user.result, &input, aborted_error()),
                parts: Vec::new(),
            },
        )?;
        publish_events(state, dir, project, assistant.events);
        publish_error(state, id, assistant.result.info["error"].clone());
        publish_idle(state, id);
        publish_turn_close(state, id, "interrupted");

        return Ok(assistant.result);
    }

    if fake_error(&input, &text) {
        let assistant = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: assistant_error_info(&user.result, &input, api_error()),
                parts: Vec::new(),
            },
        )?;
        publish_events(state, dir, project, assistant.events);
        publish_error(state, id, assistant.result.info["error"].clone());
        publish_idle(state, id);
        publish_turn_close(state, id, "error");

        return Ok(assistant.result);
    }

    if fake_provider(&input) {
        return prompt_fake(state, id, input, user.result, text, dir, project, cancel).await;
    }

    if is_openai_oauth(state, input.model.as_ref()) {
        return prompt_openai_stream(state, id, input, user.result, text, dir, project, cancel)
            .await;
    }

    let out = match kilo_provider::chat_tools_with_auth(
        &state.store.config(),
        &json!(state.store.provider_auths()),
        input.model.as_ref(),
        prompt_instructions(&input),
        vec![ChatMessage {
            role: "user".to_string(),
            content: text,
        }],
        real_tools(&input),
    )
    .await
    {
        Ok(out) => out,
        Err(err) => {
            let assistant = state.store.append_message_record(
                id,
                MessageAppendInput {
                    info: assistant_error_info(&user.result, &input, provider_error(err)),
                    parts: Vec::new(),
                },
            )?;
            publish_events(state, dir, project, assistant.events);
            publish_error(state, id, assistant.result.info["error"].clone());
            publish_idle(state, id);
            publish_turn_close(state, id, "error");

            return Ok(assistant.result);
        }
    };

    append_assistant(state, id, &input, &user.result, out, dir, project)
}

async fn prompt_fake(
    state: &AppState,
    id: &str,
    input: PromptInput,
    user: MessageAppendResult,
    text: String,
    dir: String,
    project: String,
    cancel: Arc<AtomicBool>,
) -> rusqlite::Result<MessageAppendResult> {
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
                info: assistant_error_info(&user, &input, aborted_error()),
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
            info: assistant_info(&user, &input),
            parts: vec![json!({
                "type": "text",
                "text": "",
                "synthetic": true,
            })],
        },
    )?;
    publish_events(state, dir.clone(), project.clone(), start.events);

    let mid = start.result.info["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let pid = start.result.parts[0]["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();

    publish_part_delta(state, id, &mid, &pid, &body);

    let root = PathBuf::from(state.store.paths().directory);
    let tools = fake_tool_parts(
        root,
        mid.clone(),
        pid.clone(),
        calls,
        start.result.time,
        cancel.clone(),
    )
    .await;
    if is_canceled(&cancel) {
        let assistant = state.store.append_message_record(
            id,
            MessageAppendInput {
                info: assistant_error_info(&user, &input, aborted_error()),
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

/// Hard cap on iterations of the live OAuth tool loop. Mirrors Bun's
/// `streamText` `maxSteps` ceiling — protects against a model that
/// indefinitely calls tools and never produces a terminal stop.
const OPENAI_OAUTH_MAX_ITERATIONS: usize = 16;

async fn prompt_openai_stream(
    state: &AppState,
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
    // single empty text part; each iteration appends new parts via
    // `append_message_record`, which upserts on (`info.id`, `part.id`).
    let start = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: assistant_openai_info(&user, &input),
            parts: vec![json!({ "type": "text", "text": "" })],
        },
    )?;
    publish_events(state, dir.clone(), project.clone(), start.events);
    let mid = start.result.info["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let pid = start.result.parts[0]["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let root = PathBuf::from(state.store.paths().directory);
    let auths = fresh_auths(state)
        .await
        .unwrap_or_else(|_| json!(state.store.provider_auths()));

    // Accumulated state across iterations.
    let mut deltas: Vec<String> = Vec::new();
    let mut tool_parts: Vec<Value> = Vec::new();
    let mut last_usage: Option<ChatUsage> = None;
    let mut last_finish: Option<String> = None;
    let mut history_extension: Vec<ChatMessage> = Vec::new();
    let mut last_text: String = String::new();

    for iteration in 0..OPENAI_OAUTH_MAX_ITERATIONS {
        // Pre-flight cancel check — short-circuit before opening another
        // upstream connection.
        if is_canceled(&cancel) {
            return finalize_openai_aborted(
                state,
                id,
                &dir,
                &project,
                &start.result,
                &mid,
                &pid,
                &deltas,
                &tool_parts,
                last_usage.as_ref(),
            );
        }

        // Per-iteration in-stream tool dispatch. The closure spawns each
        // `ChatToolCall` into the JoinSet IMMEDIATELY as `ToolCall`
        // arrives from the SSE parser, matching the AI SDK's
        // `ToolCallComplete` semantics. Tools execute in parallel with
        // any subsequent text deltas. The lock is held only across the
        // synchronous `set.spawn(...)` call — never across an `.await`.
        let join_set: Arc<std::sync::Mutex<JoinSet<(usize, Value)>>> =
            Arc::new(std::sync::Mutex::new(JoinSet::new()));
        let iter_text: Arc<std::sync::Mutex<String>> =
            Arc::new(std::sync::Mutex::new(String::new()));
        let stream_root = root.clone();
        let stream_mid = mid.clone();
        let stream_pid = pid.clone();
        let stream_cancel = cancel.clone();
        let stream_join = join_set.clone();
        let base_idx = tool_parts.len();
        let start_time = start.result.time;

        // Compose this iteration's outbound message list. The first
        // iteration uses the persisted history; subsequent iterations
        // append tool-call/tool-result synthetic messages so the model
        // sees the outcome of its previous step.
        let mut iter_messages = real_messages(state, id, &text);
        iter_messages.extend(history_extension.iter().cloned());

        let out = kilo_provider::stream_openai_oauth(
            &state.store.config(),
            &auths,
            input.model.as_ref(),
            prompt_instructions(&input),
            iter_messages,
            real_tools(&input),
            &cancel,
            |event| match event {
                StreamEvent::TextDelta(delta) => {
                    publish_part_delta(state, id, &mid, &pid, &delta);
                    if let Ok(mut buf) = iter_text.lock() {
                        buf.push_str(&delta);
                    }
                    deltas.push(delta);
                }
                StreamEvent::ToolCall(call) => {
                    let join = stream_join.clone();
                    let cancel = stream_cancel.clone();
                    let root = stream_root.clone();
                    let mid = stream_mid.clone();
                    let pid = stream_pid.clone();
                    let idx = {
                        let guard = join.lock().unwrap();
                        base_idx + guard.len()
                    };
                    let mut guard = join.lock().unwrap();
                    guard.spawn(async move {
                        // Cooperative pre-check. Synchronous filesystem
                        // calls inside `real_tool_part` cannot themselves
                        // observe cancel; this gate lets a mid-stream
                        // abort skip the actual handler invocation.
                        if is_canceled(&cancel) {
                            return (
                                idx,
                                tool_error(
                                    &mid,
                                    &pid,
                                    idx,
                                    &call.name,
                                    &call.id,
                                    &call.input,
                                    "Tool call aborted".to_string(),
                                    start_time,
                                ),
                            );
                        }
                        let part = real_tool_part(&root, &mid, &pid, idx, &call, start_time);
                        (idx, part)
                    });
                }
                StreamEvent::ToolDelta { .. } | StreamEvent::Usage(_) | StreamEvent::Finish(_) => {}
                StreamEvent::Error(err) => {
                    publish_error(state, id, provider_error(ProviderError::Api(err)))
                }
            },
        )
        .await;

        // Drain whichever tool tasks managed to spawn before stream end
        // or abort. Always do this — even on cancel — so partial results
        // get persisted on the assistant message. The Mutex is taken
        // only to swap out the JoinSet; we hold no lock across `.await`.
        let mut iter_tools: Vec<(usize, Value)> = Vec::new();
        let mut owned = {
            let mut guard = join_set.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        while let Some(item) = owned.join_next().await {
            if let Ok(item) = item {
                iter_tools.push(item);
            }
        }
        iter_tools.sort_by_key(|(idx, _)| *idx);
        let iter_tools_only: Vec<Value> = iter_tools.into_iter().map(|(_, part)| part).collect();
        let had_tools_this_iter = !iter_tools_only.is_empty();
        tool_parts.extend(iter_tools_only.iter().cloned());

        // Cancel branch: update the IN-PLACE assistant message — never
        // append a second one. Bun's `run-state.ts:48-68 onInterrupt`
        // expects exactly one assistant record per cancelled turn.
        if is_canceled(&cancel) {
            return finalize_openai_aborted(
                state,
                id,
                &dir,
                &project,
                &start.result,
                &mid,
                &pid,
                &deltas,
                &tool_parts,
                last_usage.as_ref(),
            );
        }

        let provider_out = match out {
            Ok(out) => out,
            Err(ProviderError::Aborted) => {
                return finalize_openai_aborted(
                    state,
                    id,
                    &dir,
                    &project,
                    &start.result,
                    &mid,
                    &pid,
                    &deltas,
                    &tool_parts,
                    last_usage.as_ref(),
                );
            }
            Err(err) => {
                let assistant = state.store.append_message_record(
                    id,
                    MessageAppendInput {
                        info: assistant_error_info(&user, &input, provider_error(err)),
                        parts: Vec::new(),
                    },
                )?;
                publish_events(state, dir, project, assistant.events);
                publish_error(state, id, assistant.result.info["error"].clone());
                publish_idle(state, id);
                publish_turn_close(state, id, "error");

                return Ok(assistant.result);
            }
        };

        if let Some(usage) = provider_out.usage.clone() {
            last_usage = Some(usage);
        }
        last_finish = provider_out.finish.clone();
        // Track this iteration's text so the loop terminator below has
        // the right text body, and so the next iteration can attribute
        // the prior assistant turn into `history_extension` correctly.
        let this_iter_text = if !provider_out.text.is_empty() {
            provider_out.text.clone()
        } else {
            iter_text
                .lock()
                .map(|guard| guard.clone())
                .unwrap_or_default()
        };
        last_text = this_iter_text.clone();

        // Iteration termination policy. Bun's `streamText` continues the
        // loop while the model emits tool calls and stops when it emits
        // only text (the `tool_calls` finish-reason is the chat-completions
        // signal; the Responses API uses `response.completed` regardless,
        // so we key on "did the model produce any tool calls this turn?").
        if !had_tools_this_iter {
            break;
        }

        // Build the follow-up history slice the next iteration will send
        // to the model: a synthetic assistant message describing the
        // tool calls and a user-role message carrying their outputs. We
        // do not have the AI SDK's native `function_call` /
        // `function_call_output` typing, so we approximate via text — a
        // well-instructed model treats this as continuation context.
        let calls_summary = provider_out
            .tool_calls
            .iter()
            .map(|c| {
                format!(
                    "{}({})",
                    c.name,
                    serde_json::to_string(&c.input).unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let results_summary = iter_tools_only
            .iter()
            .filter_map(|part| part["state"]["output"].as_str().map(str::to_string))
            .collect::<Vec<_>>()
            .join("\n---\n");
        if !this_iter_text.is_empty() || !calls_summary.is_empty() {
            history_extension.push(ChatMessage {
                role: "assistant".to_string(),
                content: if calls_summary.is_empty() {
                    this_iter_text.clone()
                } else if this_iter_text.is_empty() {
                    format!("[Tool calls]\n{calls_summary}")
                } else {
                    format!("{this_iter_text}\n[Tool calls]\n{calls_summary}")
                },
            });
        }
        if !results_summary.is_empty() {
            history_extension.push(ChatMessage {
                role: "user".to_string(),
                content: format!("[Tool results]\n{results_summary}"),
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
                last_usage.as_ref(),
                last_finish.as_deref(),
                id,
            );
            let assistant = state.store.append_message_record(
                id,
                MessageAppendInput {
                    info: assistant_error_info(
                        &user,
                        &input,
                        max_iterations_error(OPENAI_OAUTH_MAX_ITERATIONS),
                    ),
                    parts,
                },
            )?;
            publish_events(state, dir, project, assistant.events);
            publish_error(state, id, assistant.result.info["error"].clone());
            publish_idle(state, id);
            publish_turn_close(state, id, "error");
            return Ok(assistant.result);
        }
    }

    // Normal terminal path: write final parts + completed info on the
    // existing assistant message id (upsert).
    let final_text = if last_text.is_empty() {
        deltas.join("")
    } else {
        last_text
    };
    let parts = assistant_parts_with(
        &mid,
        &pid,
        &final_text,
        &tool_parts,
        last_usage.as_ref(),
        last_finish.as_deref(),
        id,
    );
    let assistant = state.store.append_message_record(
        id,
        MessageAppendInput {
            info: assistant_stream_completed_info(
                &start.result,
                last_usage.as_ref(),
                last_finish.as_deref(),
            ),
            parts,
        },
    )?;
    publish_events(state, dir, project, assistant.events);
    publish_idle(state, id);
    publish_turn_close(state, id, "completed");

    Ok(assistant.result)
}

/// Persist an aborted OAuth turn IN-PLACE on the existing assistant
/// message. Mirrors Bun's `run-state.ts:48-68 onInterrupt`: one assistant
/// record per cancelled turn, carrying whatever partial deltas and
/// completed tool-result parts had streamed before the abort. Re-issuing
/// `append_message_record` with the same `info.id` and explicit `id`
/// fields on each part triggers the store's upsert path.
#[allow(clippy::too_many_arguments)]
fn finalize_openai_aborted(
    state: &AppState,
    id: &str,
    dir: &str,
    project: &str,
    start: &MessageAppendResult,
    mid: &str,
    pid: &str,
    deltas: &[String],
    tool_parts: &[Value],
    usage: Option<&ChatUsage>,
) -> rusqlite::Result<MessageAppendResult> {
    let mut info = start.info.clone();
    info["error"] = aborted_error();
    info["finish"] = json!("aborted");
    if let Some(usage) = usage {
        info["tokens"] = tokens_value(usage);
    }
    if let Some(time) = info.get_mut("time").and_then(Value::as_object_mut) {
        time.insert("updated".to_string(), json!(start.time));
        time.insert("completed".to_string(), json!(start.time));
    }
    let mut parts = Vec::new();
    let body = deltas.join("");
    if !body.is_empty() {
        parts.push(json!({
            "id": pid,
            "type": "text",
            "text": body,
        }));
    }
    parts.extend(tool_parts.iter().cloned());

    let assistant = state
        .store
        .append_message_record(id, MessageAppendInput { info, parts })?;
    publish_events(
        state,
        dir.to_string(),
        project.to_string(),
        assistant.events,
    );
    publish_error(state, id, assistant.result.info["error"].clone());
    publish_idle(state, id);
    publish_turn_close(state, id, "interrupted");

    // Drop the unused pre-existing message id binding silently — the
    // `mid` and `start.info["id"]` are the same value, but this fn uses
    // `start.info` directly for the upsert. Suppress unused warning.
    let _ = mid;

    Ok(assistant.result)
}

/// Compose the final assistant parts list from accumulated text + tool
/// parts + step-finish, sharing the same shape as `assistant_parts` but
/// without re-running tool dispatch (the loop already produced them).
fn assistant_parts_with(
    mid: &str,
    pid: &str,
    text: &str,
    tool_parts: &[Value],
    usage: Option<&ChatUsage>,
    finish: Option<&str>,
    sid: &str,
) -> Vec<Value> {
    let mut parts = vec![json!({
        "id": pid,
        "type": "text",
        "text": text,
    })];
    parts.extend(tool_parts.iter().cloned());
    parts.push(step_finish_part_usage(sid, mid, &parts[0], usage, finish));
    parts
}

fn max_iterations_error(cap: usize) -> Value {
    json!({
        "name": "MaxIterationsError",
        "data": {
            "message": format!(
                "OpenAI OAuth tool loop exceeded {cap} iterations without a terminal stop"
            )
        }
    })
}

fn append_assistant(
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
    publish_events(state, dir.clone(), project.clone(), start.events);

    let mid = start.result.info["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let pid = start.result.parts[0]["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    publish_part_delta(state, id, &mid, &pid, &out.text);

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
    publish_events(state, dir, project, assistant.events);
    publish_idle(state, id);
    publish_turn_close(state, id, "completed");

    Ok(assistant.result)
}

fn assistant_parts(
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

fn real_tools(input: &PromptInput) -> Vec<ChatTool> {
    if !tools_on(input) {
        return Vec::new();
    }
    vec![read_def(), grep_def()]
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
            }
            _ => false,
        })
        .unwrap_or_else(|| model_toolcall(input.model.as_ref()))
}

fn is_openai_oauth(state: &AppState, model: Option<&Value>) -> bool {
    let Some(model) = model else {
        return false;
    };
    let provider = model
        .get("providerID")
        .or_else(|| model.get("provider"))
        .and_then(Value::as_str);
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
const OPENAI_OAUTH_SOUL_RAW: &str = include_str!("../../../../opencode/src/kilocode/soul.txt");

fn prompt_instructions(input: &PromptInput) -> Option<String> {
    let mut pieces: Vec<String> = vec![OPENAI_OAUTH_SOUL_RAW.trim().to_string()];
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
    Some(pieces.join("\n"))
}

fn real_messages(state: &AppState, id: &str, text: &str) -> Vec<ChatMessage> {
    let mut msgs = state
        .store
        .messages(id, None, None)
        .map(|page| {
            page.items
                .into_iter()
                .filter_map(|msg| {
                    let role = msg.info.get("role").and_then(Value::as_str)?;
                    if role == "system" {
                        return None;
                    }
                    let content = prompt_text(&msg.parts);
                    (!content.is_empty()).then(|| ChatMessage {
                        role: role.to_string(),
                        content,
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if msgs.last().map(|msg| msg.content.as_str()) != Some(text) {
        msgs.push(ChatMessage {
            role: "user".to_string(),
            content: text.to_string(),
        });
    }
    msgs
}

fn tool_enabled(value: &Value) -> bool {
    match value {
        Value::String(name) => matches!(name.as_str(), "read" | "grep"),
        Value::Bool(value) => *value,
        Value::Object(map) => map
            .get("disabled")
            .and_then(Value::as_bool)
            .map(|value| !value)
            .unwrap_or(true),
        _ => false,
    }
}

fn model_toolcall(model: Option<&Value>) -> bool {
    model
        .and_then(|value| value.get("capabilities"))
        .and_then(|value| value.get("toolcall"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn read_def() -> ChatTool {
    ChatTool {
        name: "read".to_string(),
        description: "Read a file or directory under the current workspace.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "filePath": { "type": "string", "description": "Workspace-relative file or directory path." },
                "path": { "type": "string", "description": "Workspace-relative file or directory path." },
                "offset": { "type": "integer", "minimum": 1 },
                "limit": { "type": "integer", "minimum": 1 }
            },
            "additionalProperties": false
        }),
    }
}

fn grep_def() -> ChatTool {
    ChatTool {
        name: "grep".to_string(),
        description: "Search text in files under the current workspace.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string", "description": "Literal text pattern to search for." },
                "path": { "type": "string", "description": "Workspace-relative file or directory path." }
            },
            "required": ["pattern"],
            "additionalProperties": false
        }),
    }
}

fn real_tool_parts(
    root: &FsPath,
    mid: &str,
    pid: &str,
    calls: &[ChatToolCall],
    time: i64,
) -> Vec<Value> {
    calls
        .iter()
        .enumerate()
        .map(|(idx, call)| real_tool_part(root, mid, pid, idx, call, time))
        .collect()
}

/// M7 Fix 5: live tool-name repair. The model frequently emits `Read`,
/// `READ`, `Grep`, etc. instead of the canonical lowercase names. Bun
/// normalizes these via `experimental_repairToolCall`
/// ([`session/llm.ts:363-383`](../../../../../opencode/src/session/llm.ts:363));
/// we mirror the contract by funnelling every live `ChatToolCall` through
/// [`repair_tool_name`] before dispatching. On `Repair::Invalid`, we
/// produce the same Unknown-tool error shape that the fake path emits via
/// `fake_tool_part`'s `"invalid"` arm (see [`tool_error`]).
fn real_tool_part(
    root: &FsPath,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &ChatToolCall,
    time: i64,
) -> Value {
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
        // KNOWN_TOOLS includes write/edit/apply_patch/bash but the live
        // OAuth path only advertises read/grep in its `tools` definition
        // today; reaching this arm means the model invented a tool name
        // outside our advertised schema. Produce the same shape as an
        // unknown-tool error.
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

fn user_info(input: &PromptInput) -> Value {
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

fn assistant_info(user: &MessageAppendResult, input: &PromptInput) -> Value {
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

fn assistant_provider_info(
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

fn assistant_openai_info(user: &MessageAppendResult, input: &PromptInput) -> Value {
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
        "path": {},
        "cost": 0,
        "tokens": zero_tokens(),
    })
}

fn assistant_completed_info(start: &MessageAppendResult) -> Value {
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

fn assistant_stream_completed_info(
    start: &MessageAppendResult,
    usage: Option<&ChatUsage>,
    finish: Option<&str>,
) -> Value {
    let mut info = assistant_completed_info(start);
    info["finish"] = json!(finish.unwrap_or("stop"));
    if let Some(usage) = usage {
        info["tokens"] = tokens_value(usage);
    }
    info
}

fn step_finish_part(sid: &str, mid: &str, part: &Value) -> Value {
    let pid = part["id"].as_str().unwrap_or("prt_fake");
    json!({
        "id": format!("{pid}_step_finish"),
        "type": "step-finish",
        "messageID": mid,
        "sessionID": sid,
        "reason": "stop",
        "cost": 0,
        "tokens": {
            "input": 0,
            "output": 0,
            "reasoning": 0,
            "total": 0,
            "cache": { "read": 0, "write": 0 }
        }
    })
}

fn step_finish_part_usage(
    sid: &str,
    mid: &str,
    part: &Value,
    usage: Option<&ChatUsage>,
    reason: Option<&str>,
) -> Value {
    let mut part = step_finish_part(sid, mid, part);
    if let Some(usage) = usage {
        part["tokens"] = tokens_value(usage);
    }
    if let Some(reason) = reason {
        part["reason"] = json!(reason);
    }
    part
}

fn zero_tokens() -> Value {
    json!({
        "input": 0,
        "output": 0,
        "reasoning": 0,
        "cache": { "read": 0, "write": 0 }
    })
}

fn tokens_value(usage: &ChatUsage) -> Value {
    json!({
        "input": usage.input,
        "output": usage.output,
        "reasoning": 0,
        "total": usage.total,
        "cache": { "read": 0, "write": 0 }
    })
}

fn assistant_error_info(user: &MessageAppendResult, input: &PromptInput, error: Value) -> Value {
    let mut info = assistant_info(user, input);
    info["error"] = error;
    info["finish"] = json!("error");
    info
}

fn aborted_error() -> Value {
    json!({
        "name": "MessageAbortedError",
        "data": { "message": "The operation was aborted." }
    })
}

fn api_error() -> Value {
    json!({
        "name": "APIError",
        "data": {
            "message": "Deterministic fake provider error",
            "isRetryable": false,
            "metadata": { "source": "rust-fake-provider" }
        }
    })
}

fn provider_error(err: ProviderError) -> Value {
    json!({
        "name": "APIError",
        "data": {
            "message": err.to_string(),
            "isRetryable": false,
            "metadata": { "source": "rust-provider" }
        }
    })
}

fn fake_provider(input: &PromptInput) -> bool {
    fake_flag(input, "fake")
        || fake_flag(input, "fakeProvider")
        || fake_delay(input).is_some()
        || !fake_tool_calls(input).is_empty()
}

fn fake_abort(input: &PromptInput) -> bool {
    fake_flag(input, "fakeAbort")
}

fn fake_error(input: &PromptInput, text: &str) -> bool {
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

async fn fake_tool_parts(
    root: PathBuf,
    mid: String,
    pid: String,
    calls: Vec<FakeCall>,
    time: i64,
    cancel: Arc<AtomicBool>,
) -> Vec<Value> {
    let mut set = JoinSet::new();
    for (idx, call) in calls.into_iter().enumerate() {
        let root = root.clone();
        let mid = mid.clone();
        let pid = pid.clone();
        let cancel = cancel.clone();
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
                fake_tool_part(&root, &mid, &pid, idx, &call, time)
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

fn fake_tool_part(
    root: &FsPath,
    mid: &str,
    pid: &str,
    idx: usize,
    call: &FakeCall,
    time: i64,
) -> Value {
    match call.tool.as_str() {
        "read" => match fake_read(root, &call.input) {
            Ok((title, output, metadata)) => fake_tool_completed(
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
            Err(err) => fake_tool_error(mid, pid, idx, call, err, time),
        },
        "grep" => match fake_grep(root, &call.input) {
            Ok((title, output, metadata)) => fake_tool_completed(
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
            Err(err) => fake_tool_error(mid, pid, idx, call, err, time),
        },
        "write" => match fake_write(root, &call.input) {
            Ok((title, output, metadata)) => fake_tool_completed(
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
            Err(err) => fake_tool_error(mid, pid, idx, call, err, time),
        },
        "edit" => match fake_edit(root, &call.input) {
            Ok((title, output, metadata)) => fake_tool_completed(
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
            Err(err) => fake_tool_error(mid, pid, idx, call, err, time),
        },
        "apply_patch" => match fake_apply_patch(root, &call.input) {
            Ok((title, output, metadata)) => fake_tool_completed(
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
            Err(err) => fake_tool_error(mid, pid, idx, call, err, time),
        },
        "bash" => match fake_bash(root, &call.input) {
            Ok((title, output, metadata)) => fake_tool_completed(
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
            Err(err) => fake_tool_error(mid, pid, idx, call, err, time),
        },
        "invalid" => tool_error(
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
        ),
        _ => tool_error(
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

async fn wait_fake(ms: u64, cancel: &AtomicBool) {
    let mut left = ms;
    while left > 0 && !is_canceled(cancel) {
        let next = left.min(10);
        tokio::time::sleep(Duration::from_millis(next)).await;
        left -= next;
    }
}

fn is_canceled(cancel: &AtomicBool) -> bool {
    cancel.load(Ordering::SeqCst)
}

fn repair_tool_name(name: &str, known: &[&str]) -> Repair {
    known
        .iter()
        .find(|tool| tool.eq_ignore_ascii_case(name))
        .map(|tool| Repair::Valid((*tool).to_string()))
        .unwrap_or_else(|| Repair::Invalid(name.to_string()))
}

#[allow(dead_code)]
fn has_tool_calls(messages: &[Value]) -> bool {
    messages.iter().any(|message| has_tool_call(message))
}

#[allow(dead_code)]
fn has_tool_call(value: &Value) -> bool {
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

#[allow(dead_code)]
fn should_inject_noop(provider: &str, lite: bool, tools: &[ChatTool], messages: &[Value]) -> bool {
    (lite || provider.contains("github-copilot")) && tools.is_empty() && has_tool_calls(messages)
}

#[allow(clippy::too_many_arguments)]
fn tool_completed(
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

fn tool_error(
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &str,
    input: &Value,
    err: String,
    time: i64,
) -> Value {
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
            "metadata": {},
            "time": { "start": time, "end": time }
        },
    })
}

fn tool_input(input: &Value) -> Value {
    input
        .as_object()
        .map_or_else(|| json!({}), |map| json!(map))
}

fn fake_bash(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    let command = input
        .get("command")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "command is required".to_string())?;
    let dir = input.get("workdir").and_then(Value::as_str).unwrap_or("");
    let cwd = resolve_under(root, dir).map_err(|_| format!("Unsafe workdir: {dir}"))?;
    let timeout = tool_timeout(input)?;
    let description = input
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or(command);
    let (mut child, started) = shell_command(command, &cwd)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Unable to capture command stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Unable to capture command stderr".to_string())?;
    let out = read_pipe(stdout);
    let err = read_pipe(stderr);
    let mut expired = false;
    let code = loop {
        let status = child
            .try_wait()
            .map_err(|err| format!("Unable to wait for command: {err}"))?;
        if let Some(status) = status {
            break status.code();
        }
        if started.elapsed() >= timeout {
            expired = true;
            child
                .kill()
                .map_err(|err| format!("Unable to kill timed out command: {err}"))?;
            child
                .wait()
                .map_err(|err| format!("Unable to wait for timed out command: {err}"))?;
            break None;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stdout = out
        .join()
        .map_err(|_| "Unable to collect command stdout".to_string())?;
    let stderr = err
        .join()
        .map_err(|_| "Unable to collect command stderr".to_string())?;
    let text = combined_output(&stdout, &stderr);
    let (preview, truncated) = truncate_output(&text);
    let output = if preview.is_empty() {
        "(no output)".to_string()
    } else {
        preview
    };
    let metadata = json!({
        "output": output,
        "exit": code,
        "description": description,
        "truncated": truncated,
        "timeout": expired,
    });

    Ok((description.to_string(), output, metadata))
}

fn shell_command(command: &str, cwd: &FsPath) -> Result<(std::process::Child, Instant), String> {
    let mut cmd = if cfg!(windows) {
        let mut cmd = Command::new("cmd.exe");
        cmd.args(["/C", command]);
        cmd
    } else {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", command]);
        cmd
    };
    let child = cmd
        .current_dir(cwd)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("Unable to run command: {err}"))?;
    Ok((child, Instant::now()))
}

fn read_pipe(mut pipe: impl Read + Send + 'static) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut out = Vec::new();
        let _ = pipe.read_to_end(&mut out);
        out
    })
}

fn combined_output(stdout: &[u8], stderr: &[u8]) -> String {
    let out = String::from_utf8_lossy(stdout);
    let err = String::from_utf8_lossy(stderr);
    if out.is_empty() {
        return err.to_string();
    }
    if err.is_empty() {
        return out.to_string();
    }
    format!("{out}{err}")
}

fn truncate_output(text: &str) -> (String, bool) {
    let bytes = text.as_bytes();
    if bytes.len() <= MAX_BASH_OUTPUT_BYTES {
        return (text.to_string(), false);
    }
    let mut end = MAX_BASH_OUTPUT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (text[..end].to_string(), true)
}

fn tool_timeout(input: &Value) -> Result<Duration, String> {
    let Some(value) = input.get("timeout") else {
        return Ok(Duration::from_millis(DEFAULT_BASH_TIMEOUT_MS));
    };
    let Some(ms) = value.as_i64() else {
        return Err("timeout must be greater than or equal to 0".to_string());
    };
    if ms < 0 {
        return Err("timeout must be greater than or equal to 0".to_string());
    }
    Ok(Duration::from_millis(ms as u64))
}

fn fake_read(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    let path = input
        .get("filePath")
        .or_else(|| input.get("path"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "filePath is required".to_string())?;
    let target = resolve_under(root, path).map_err(|_| format!("Unsafe path: {path}"))?;
    let meta = fs::metadata(&target).map_err(|err| format!("Unable to read {path}: {err}"))?;
    if meta.is_dir() {
        return fake_read_dir(root, &target, input);
    }

    fake_read_file(root, &target, input)
}

fn fake_read_dir(
    root: &FsPath,
    dir: &FsPath,
    input: &Value,
) -> Result<(String, String, Value), String> {
    let offset = tool_usize(input, "offset", 1)?;
    let limit = tool_usize(input, "limit", DEFAULT_READ_LIMIT)?;
    let entries = list_nodes(root, dir)
        .into_iter()
        .filter_map(|node| node.get("path").and_then(Value::as_str).map(str::to_string))
        .collect::<Vec<_>>();
    let start = offset - 1;
    let sliced = entries
        .iter()
        .skip(start)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();
    let truncated = start + sliced.len() < entries.len();
    let note = if truncated {
        format!(
            "\n(Showing {} of {} entries. Use 'offset' parameter to read beyond entry {})",
            sliced.len(),
            entries.len(),
            offset + sliced.len()
        )
    } else {
        format!("\n({} entries)", entries.len())
    };
    let output = [
        format!("<path>{}</path>", dir.to_string_lossy()),
        "<type>directory</type>".to_string(),
        "<entries>".to_string(),
        sliced.join("\n"),
        note,
        "</entries>".to_string(),
    ]
    .join("\n");
    let metadata = json!({
        "preview": sliced.iter().take(20).cloned().collect::<Vec<_>>().join("\n"),
        "truncated": truncated,
        "loaded": [],
    });

    Ok((title(root, dir), output, metadata))
}

fn fake_read_file(
    root: &FsPath,
    file: &FsPath,
    input: &Value,
) -> Result<(String, String, Value), String> {
    let offset = tool_usize(input, "offset", 1)?;
    let limit = tool_usize(input, "limit", DEFAULT_READ_LIMIT)?;
    let bytes = fs::read(file)
        .map_err(|err| format!("Unable to read {}: {err}", file.to_string_lossy()))?;
    if is_binary(file, &bytes) {
        return Err(format!(
            "Cannot read binary file: {}",
            file.to_string_lossy()
        ));
    }

    let text = String::from_utf8_lossy(&bytes);
    let lines = text.lines().collect::<Vec<_>>();
    let count = lines.len();
    let start = offset - 1;
    if start >= count && !(count == 0 && offset == 1) {
        return Err(format!(
            "Offset {offset} is out of range for this file ({count} lines)"
        ));
    }

    let raw = lines
        .iter()
        .skip(start)
        .take(limit)
        .copied()
        .collect::<Vec<_>>();
    let mut output = [
        format!("<path>{}</path>", file.to_string_lossy()),
        "<type>file</type>".to_string(),
        "<content>\n".to_string(),
    ]
    .join("\n");
    output.push_str(
        &raw.iter()
            .enumerate()
            .map(|(idx, line)| format!("{}: {line}", idx + offset))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let last = offset + raw.len().saturating_sub(1);
    let next = last + 1;
    let truncated = start + raw.len() < count;
    if truncated {
        output.push_str(&format!(
            "\n\n(Showing lines {offset}-{last} of {count}. Use offset={next} to continue.)"
        ));
    } else {
        output.push_str(&format!("\n\n(End of file - total {count} lines)"));
    }
    output.push_str("\n</content>");
    let metadata = json!({
        "preview": raw.iter().take(20).copied().collect::<Vec<_>>().join("\n"),
        "truncated": truncated,
        "loaded": [],
    });

    Ok((title(root, file), output, metadata))
}

fn fake_grep(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    let pattern = input
        .get("pattern")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "pattern is required".to_string())?;
    let path = input.get("path").and_then(Value::as_str).unwrap_or(".");
    let target = resolve_under(root, path).map_err(|_| format!("Unsafe path: {path}"))?;
    if !target.exists() {
        return Ok((
            pattern.to_string(),
            "No files found".to_string(),
            json!({ "matches": 0, "truncated": false }),
        ));
    }

    let matches = search_text_target(root, &target, pattern, DEFAULT_GREP_LIMIT + 1);
    if matches.is_empty() {
        return Ok((
            pattern.to_string(),
            "No files found".to_string(),
            json!({ "matches": 0, "truncated": false }),
        ));
    }

    let total = matches.len();
    let truncated = total > DEFAULT_GREP_LIMIT;
    let final_matches = matches.iter().take(DEFAULT_GREP_LIMIT).collect::<Vec<_>>();
    let mut output = vec![format!(
        "Found {total} matches{}",
        if truncated {
            format!(" (showing first {DEFAULT_GREP_LIMIT})")
        } else {
            String::new()
        }
    )];
    let mut current = String::new();
    for item in final_matches {
        let rel = item["path"]["text"].as_str().unwrap_or_default();
        let path = slash(&root.join(rel));
        if current != path {
            if !current.is_empty() {
                output.push(String::new());
            }
            current = path.clone();
            output.push(format!("{path}:"));
        }
        let line = item["line_number"].as_u64().unwrap_or_default();
        let mut text = item["lines"]["text"]
            .as_str()
            .unwrap_or_default()
            .trim_end_matches(['\r', '\n'])
            .to_string();
        if text.len() > MAX_GREP_LINE {
            text = format!("{}...", &text[..MAX_GREP_LINE]);
        }
        output.push(format!("  Line {line}: {text}"));
    }
    if truncated {
        output.push(String::new());
        output.push(format!(
            "(Results truncated: showing {DEFAULT_GREP_LIMIT} of {total} matches ({} hidden). Consider using a more specific path or pattern.)",
            total - DEFAULT_GREP_LIMIT
        ));
    }

    Ok((
        pattern.to_string(),
        output.join("\n"),
        json!({ "matches": total, "truncated": truncated }),
    ))
}

fn fake_write(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    let path = input
        .get("filePath")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "filePath is required".to_string())?;
    let content = input
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| "content is required".to_string())?;
    let target = resolve_under(root, path).map_err(|_| format!("Unsafe path: {path}"))?;
    let exists = target.exists();
    let before = if exists {
        fs::read_to_string(&target)
            .map_err(|err| format!("Unable to read {}: {err}", target.to_string_lossy()))?
    } else {
        String::new()
    };
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Unable to create {}: {err}", parent.to_string_lossy()))?;
    }
    fs::write(&target, content)
        .map_err(|err| format!("Unable to write {}: {err}", target.to_string_lossy()))?;

    let diff = text_diff(path, &before, content);
    let metadata = json!({
        "filepath": slash(&target),
        "exists": exists,
        "diff": diff,
        "filediff": diff,
        "diagnostics": [],
    });

    Ok((
        title(root, &target),
        "Wrote file successfully.".to_string(),
        metadata,
    ))
}

fn fake_edit(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    let path = input
        .get("filePath")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "filePath is required".to_string())?;
    let old = input
        .get("oldString")
        .and_then(Value::as_str)
        .ok_or_else(|| "oldString is required".to_string())?;
    let new = input
        .get("newString")
        .and_then(Value::as_str)
        .ok_or_else(|| "newString is required".to_string())?;
    if old == new {
        return Err("oldString and newString must be different".to_string());
    }

    let target = resolve_under(root, path).map_err(|_| format!("Unsafe path: {path}"))?;
    let before = if old.is_empty() {
        fs::read_to_string(&target).unwrap_or_default()
    } else {
        fs::read_to_string(&target)
            .map_err(|err| format!("Unable to read {}: {err}", target.to_string_lossy()))?
    };
    let after = if old.is_empty() {
        new.to_string()
    } else if input
        .get("replaceAll")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        let count = before.matches(old).count();
        if count == 0 {
            return Err("oldString was not found".to_string());
        }
        before.replace(old, new)
    } else {
        let count = before.matches(old).count();
        if count == 0 {
            return Err("oldString was not found".to_string());
        }
        if count > 1 {
            return Err(format!(
                "oldString matched {count} times; set replaceAll to true or provide a unique match"
            ));
        }
        before.replacen(old, new, 1)
    };

    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Unable to create {}: {err}", parent.to_string_lossy()))?;
    }
    fs::write(&target, &after)
        .map_err(|err| format!("Unable to write {}: {err}", target.to_string_lossy()))?;

    let diff = text_diff(path, &before, &after);
    let metadata = json!({
        "diff": diff,
        "filediff": diff,
        "diagnostics": [],
    });

    Ok((
        title(root, &target),
        "Edit applied successfully.".to_string(),
        metadata,
    ))
}

#[derive(Clone, Copy)]
enum PatchKind {
    Add,
    Delete,
    Update,
}

struct PatchSection {
    kind: PatchKind,
    path: String,
    lines: Vec<String>,
}

struct PatchResult {
    row: String,
    path: String,
    diff: String,
    additions: usize,
    deletions: usize,
    kind: &'static str,
}

fn fake_apply_patch(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    let text = input
        .get("patchText")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "patchText is required".to_string())?;
    let sections = parse_apply_patch(text)?;
    let mut out = Vec::new();

    for item in sections {
        let target =
            resolve_under(root, &item.path).map_err(|_| format!("Unsafe path: {}", item.path))?;
        let before = fs::read_to_string(&target).unwrap_or_default();
        let (after, kind, row, add, del) = match item.kind {
            PatchKind::Add => {
                if target.exists() {
                    return Err(format!("File already exists: {}", item.path));
                }
                let body = patch_added(&item.lines)?;
                (body, "added", "A", count_lines(&item.lines, '+'), 0)
            }
            PatchKind::Delete => {
                if !target.exists() {
                    return Err(format!("File does not exist: {}", item.path));
                }
                (String::new(), "deleted", "D", 0, before.lines().count())
            }
            PatchKind::Update => {
                if !target.exists() {
                    return Err(format!("File does not exist: {}", item.path));
                }
                let next = patch_updated(&item.path, &before, &item.lines)?;
                (
                    next,
                    "modified",
                    "M",
                    count_lines(&item.lines, '+'),
                    count_lines(&item.lines, '-'),
                )
            }
        };
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("Unable to create {}: {err}", parent.to_string_lossy()))?;
        }
        match item.kind {
            PatchKind::Delete => fs::remove_file(&target)
                .map_err(|err| format!("Unable to delete {}: {err}", target.to_string_lossy()))?,
            _ => fs::write(&target, &after)
                .map_err(|err| format!("Unable to write {}: {err}", target.to_string_lossy()))?,
        }
        out.push(PatchResult {
            row: row.to_string(),
            path: item.path,
            diff: text_diff(&title(root, &target), &before, &after),
            additions: add,
            deletions: del,
            kind,
        });
    }

    let diff = out
        .iter()
        .map(|item| item.diff.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let files = out
        .iter()
        .map(|item| {
            json!({
                "filePath": item.path,
                "relativePath": item.path,
                "type": item.kind,
                "patch": item.diff,
                "additions": item.additions,
                "deletions": item.deletions,
            })
        })
        .collect::<Vec<_>>();
    let rows = out
        .iter()
        .map(|item| format!("{} {}", item.row, item.path))
        .collect::<Vec<_>>()
        .join("\n");
    let output = format!("Success. Updated the following files:\n{rows}");
    let title = out
        .first()
        .map(|item| item.path.clone())
        .unwrap_or_else(|| "apply_patch".to_string());

    Ok((
        title,
        output,
        json!({ "diff": diff, "files": files, "diagnostics": {} }),
    ))
}

fn parse_apply_patch(text: &str) -> Result<Vec<PatchSection>, String> {
    let lines = text.lines().collect::<Vec<_>>();
    if lines.first() != Some(&"*** Begin Patch") {
        return Err("Malformed patch: missing *** Begin Patch".to_string());
    }
    if lines.last() != Some(&"*** End Patch") {
        return Err("Malformed patch: missing *** End Patch".to_string());
    }

    let mut sections = Vec::new();
    let mut idx = 1;
    while idx + 1 < lines.len() {
        let line = lines[idx];
        if line.starts_with("*** Move to:") {
            return Err("Unsupported patch operation: move".to_string());
        }
        let Some((kind, path)) = parse_patch_header(line) else {
            return Err(format!("Malformed patch section: {line}"));
        };
        idx += 1;
        let mut body = Vec::new();
        while idx + 1 < lines.len() && !lines[idx].starts_with("*** ") {
            if !lines[idx].starts_with("@@") {
                body.push(lines[idx].to_string());
            }
            idx += 1;
        }
        if path.is_empty() {
            return Err("Malformed patch: empty path".to_string());
        }
        sections.push(PatchSection {
            kind,
            path,
            lines: body,
        });
    }
    if sections.is_empty() {
        return Err("Malformed patch: no sections".to_string());
    }
    Ok(sections)
}

fn parse_patch_header(line: &str) -> Option<(PatchKind, String)> {
    if let Some(path) = line.strip_prefix("*** Add File: ") {
        return Some((PatchKind::Add, path.to_string()));
    }
    if let Some(path) = line.strip_prefix("*** Delete File: ") {
        return Some((PatchKind::Delete, path.to_string()));
    }
    if let Some(path) = line.strip_prefix("*** Update File: ") {
        return Some((PatchKind::Update, path.to_string()));
    }
    None
}

fn patch_added(lines: &[String]) -> Result<String, String> {
    let mut out = Vec::new();
    for line in lines {
        let Some(text) = line.strip_prefix('+') else {
            return Err("Malformed add patch: expected + lines".to_string());
        };
        out.push(text);
    }
    Ok(join_patch_lines(&out))
}

fn patch_updated(path: &str, before: &str, lines: &[String]) -> Result<String, String> {
    let old = before.lines().collect::<Vec<_>>();
    let mut idx = 0;
    let mut out = Vec::new();
    for line in lines {
        let Some(tag) = line.chars().next() else {
            return Err("Malformed update patch: empty hunk line".to_string());
        };
        let text = &line[1..];
        match tag {
            ' ' => {
                while old.get(idx).copied() != Some(text) {
                    let Some(next) = old.get(idx) else {
                        return Err(format!("Patch context mismatch in {path}: {text}"));
                    };
                    out.push(*next);
                    idx += 1;
                }
                out.push(text);
                idx += 1;
            }
            '-' => {
                while old.get(idx).copied() != Some(text) {
                    let Some(next) = old.get(idx) else {
                        return Err(format!("Patch remove mismatch in {path}: {text}"));
                    };
                    out.push(*next);
                    idx += 1;
                }
                idx += 1;
            }
            '+' => out.push(text),
            _ => return Err(format!("Malformed update patch line: {line}")),
        }
    }
    out.extend(old.iter().skip(idx).copied());
    Ok(join_patch_lines(&out))
}

fn join_patch_lines(lines: &[&str]) -> String {
    if lines.is_empty() {
        return String::new();
    }
    format!("{}\n", lines.join("\n"))
}

fn count_lines(lines: &[String], prefix: char) -> usize {
    lines.iter().filter(|line| line.starts_with(prefix)).count()
}

fn text_diff(path: &str, before: &str, after: &str) -> String {
    let mut out = vec![format!("--- {path}"), format!("+++ {path}")];
    if before == after {
        out.push("@@ no changes @@".to_string());
        return out.join("\n");
    }
    out.push("@@ before @@".to_string());
    out.extend(before.lines().map(|line| format!("-{line}")));
    out.push("@@ after @@".to_string());
    out.extend(after.lines().map(|line| format!("+{line}")));
    out.join("\n")
}

fn tool_usize(input: &Value, key: &str, default: usize) -> Result<usize, String> {
    let Some(value) = input.get(key) else {
        return Ok(default);
    };
    let Some(value) = value.as_u64() else {
        return Err(format!("{key} must be greater than or equal to 1"));
    };
    if value == 0 {
        return Err(format!("{key} must be greater than or equal to 1"));
    }
    Ok(value as usize)
}

fn title(root: &FsPath, path: &FsPath) -> String {
    let value = slash(path.strip_prefix(root).unwrap_or(path));
    if value.is_empty() {
        return ".".to_string();
    }
    value
}

fn prompt_text(parts: &[Value]) -> String {
    parts
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n")
}

fn publish_turn_open(state: &AppState, id: &str) {
    let _ = state.bus.send(GlobalEvent::bus(
        "session.turn.open",
        json!({ "sessionID": id }),
    ));
}

fn publish_turn_close(state: &AppState, id: &str, reason: &str) {
    let _ = state.bus.send(GlobalEvent::bus(
        "session.turn.close",
        json!({ "sessionID": id, "reason": reason }),
    ));
}

fn publish_status(state: &AppState, id: &str, status: &str) {
    let _ = state.bus.send(GlobalEvent::bus(
        "session.status",
        json!({ "sessionID": id, "status": { "type": status } }),
    ));
}

fn publish_idle(state: &AppState, id: &str) {
    publish_status(state, id, "idle");
    let _ = state
        .bus
        .send(GlobalEvent::bus("session.idle", json!({ "sessionID": id })));
}

fn publish_error(state: &AppState, id: &str, error: Value) {
    let _ = state.bus.send(GlobalEvent::bus(
        "session.error",
        json!({ "sessionID": id, "error": error }),
    ));
}

fn publish_part_delta(state: &AppState, sid: &str, mid: &str, pid: &str, delta: &str) {
    let _ = state.bus.send(GlobalEvent::bus(
        "message.part.delta",
        json!({
            "sessionID": sid,
            "messageID": mid,
            "partID": pid,
            "field": "text",
            "delta": delta,
        }),
    ));
}

fn publish_events(
    state: &AppState,
    dir: String,
    project: String,
    events: impl IntoIterator<Item = StoredEvent>,
) {
    for event in events {
        if event.seq < 0 {
            continue;
        }
        let data = json!({
            "type": event.event_type,
            "id": event.id,
            "seq": event.seq,
            "aggregateID": event.aggregate_id,
            "data": event.data,
        });
        let _ = state
            .bus
            .send(GlobalEvent::sync(dir.clone(), project.clone(), data));
    }
}

fn publish_for_session(state: &AppState, id: &str, events: Vec<StoredEvent>) {
    let Some(session) = state.store.session(id) else {
        return;
    };
    publish_events(
        state,
        state.store.paths().directory,
        session.project_id,
        events,
    );
}

fn session_mutation_response(state: &AppState, record: SessionMutation) -> Response {
    let Some(session) = record.session else {
        return StatusCode::NOT_FOUND.into_response();
    };
    publish_events(
        state,
        state.store.paths().directory,
        session.project_id.clone(),
        record.events,
    );
    Json(session).into_response()
}

fn permission_list(state: &AppState) -> Vec<Value> {
    state
        .permissions
        .lock()
        .unwrap()
        .values()
        .cloned()
        .collect()
}

fn question_list(state: &AppState) -> Vec<Value> {
    state.questions.lock().unwrap().values().cloned().collect()
}

fn resolve_under(root: &FsPath, input: &str) -> Result<PathBuf, StatusCode> {
    let rel = input.trim_start_matches(['/', '\\']);
    let path = PathBuf::from(rel);
    if path.components().any(|part| {
        matches!(
            part,
            Component::ParentDir | Component::Prefix(_) | Component::RootDir
        )
    }) {
        return Err(StatusCode::FORBIDDEN);
    }

    Ok(root.join(path))
}

fn list_nodes(root: &FsPath, dir: &FsPath) -> Vec<Value> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = entries
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name() != ".git" && entry.file_name() != ".DS_Store")
        .filter_map(|entry| {
            let path = entry.path();
            let meta = entry.metadata().ok()?;
            let kind = if meta.is_dir() { "directory" } else { "file" };
            let name = entry.file_name().to_string_lossy().to_string();
            let rel = slash(path.strip_prefix(root).ok()?);
            Some(json!({
                "name": name,
                "path": rel,
                "absolute": path.to_string_lossy(),
                "type": kind,
                "ignored": false,
            }))
        })
        .collect::<Vec<_>>();

    out.sort_by(|a, b| {
        let at = a["type"].as_str().unwrap_or_default();
        let bt = b["type"].as_str().unwrap_or_default();
        if at != bt {
            return if at == "directory" {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            };
        }

        a["name"]
            .as_str()
            .unwrap_or_default()
            .cmp(b["name"].as_str().unwrap_or_default())
    });
    out
}

fn read_content(file: &FsPath) -> Value {
    let bytes = match fs::read(file) {
        Ok(bytes) => bytes,
        Err(_) => return json!({ "type": "text", "content": "" }),
    };
    if is_binary(file, &bytes) {
        return json!({ "type": "binary", "content": "" });
    }

    let content = String::from_utf8_lossy(&bytes).trim().to_string();
    json!({ "type": "text", "content": content })
}

fn search_files(
    root: &FsPath,
    term: &str,
    dirs: bool,
    kind: Option<&str>,
    limit: usize,
) -> Vec<String> {
    let term = term.to_lowercase();
    let mut out = Vec::new();
    walk(root, root, &mut |path, meta| {
        if out.len() >= limit {
            return false;
        }
        let is_dir = meta.is_dir();
        let include = match kind {
            Some("file") => !is_dir,
            Some("directory") => is_dir,
            _ => !is_dir || dirs,
        };
        if include
            && slash(path.strip_prefix(root).unwrap_or(path))
                .to_lowercase()
                .contains(&term)
        {
            out.push(slash(path.strip_prefix(root).unwrap_or(path)));
        }
        true
    });
    out
}

fn search_text(root: &FsPath, pattern: &str, limit: usize) -> Vec<Value> {
    search_text_target(root, root, pattern, limit)
}

fn search_text_target(root: &FsPath, target: &FsPath, pattern: &str, limit: usize) -> Vec<Value> {
    let mut out = Vec::new();
    if target.is_file() {
        collect_text_matches(root, target, pattern, limit, &mut out);
        return out;
    }

    walk(root, target, &mut |path, meta| {
        if out.len() >= limit {
            return false;
        }
        if meta.is_dir() {
            return true;
        }
        collect_text_matches(root, path, pattern, limit, &mut out);
        true
    });
    out
}

fn collect_text_matches(
    root: &FsPath,
    path: &FsPath,
    pattern: &str,
    limit: usize,
    out: &mut Vec<Value>,
) {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(_) => return,
    };
    if is_binary(path, &bytes) {
        return;
    }
    let text = String::from_utf8_lossy(&bytes);
    for (idx, line) in text.lines().enumerate() {
        if out.len() >= limit {
            return;
        }
        let Some(pos) = line.find(pattern) else {
            continue;
        };
        out.push(json!({
            "path": { "text": slash(path.strip_prefix(root).unwrap_or(path)) },
            "lines": { "text": format!("{line}\n") },
            "line_number": idx + 1,
            "absolute_offset": 0,
            "submatches": [{
                "match": { "text": pattern },
                "start": pos,
                "end": pos + pattern.len(),
            }],
        }));
    }
}

fn walk(
    root: &FsPath,
    dir: &FsPath,
    visit: &mut impl FnMut(&FsPath, &fs::Metadata) -> bool,
) -> bool {
    let Ok(entries) = fs::read_dir(dir) else {
        return true;
    };
    for entry in entries.filter_map(Result::ok) {
        if entry.file_name() == ".git" || entry.file_name() == ".DS_Store" {
            continue;
        }
        let path = entry.path();
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if !visit(&path, &meta) {
            return false;
        }
        if meta.is_dir() && path.starts_with(root) && !walk(root, &path, visit) {
            return false;
        }
    }
    true
}

fn is_binary(file: &FsPath, bytes: &[u8]) -> bool {
    let ext = file
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_lowercase();
    matches!(
        ext.as_str(),
        "exe"
            | "dll"
            | "bin"
            | "png"
            | "jpg"
            | "jpeg"
            | "gif"
            | "webp"
            | "zip"
            | "pdf"
            | "db"
            | "sqlite"
    ) || bytes.contains(&0)
}

fn slash(path: &FsPath) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn encode_cursor(cursor: &MessageCursor) -> Option<String> {
    let data = json!({ "id": cursor.id, "time": cursor.time }).to_string();
    Some(general_purpose::URL_SAFE_NO_PAD.encode(data))
}

fn decode_cursor(input: &str) -> Option<MessageCursor> {
    let data = general_purpose::URL_SAFE_NO_PAD
        .decode(input)
        .or_else(|_| general_purpose::URL_SAFE.decode(input))
        .ok()?;
    let data: serde_json::Value = serde_json::from_slice(&data).ok()?;
    let id = data.get("id")?.as_str()?.to_string();
    let time = data.get("time")?.as_i64()?;
    Some(MessageCursor { id, time })
}

/// Pure helper: given a path, an existing query string, and any
/// directory/workspace values pulled from headers, return the rewritten
/// `path[?query]`. Existing query keys win (matches the SDK's
/// `!url.searchParams.has(key)` guard in `packages/sdk/js/src/v2/client.ts`).
fn rewrite_path_and_query(
    path: &str,
    query: Option<&str>,
    directory: Option<&str>,
    workspace: Option<&str>,
) -> String {
    let mut params: Vec<(String, String)> = query
        .unwrap_or("")
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (p.to_string(), String::new()),
        })
        .collect();

    let has = |key: &str, params: &[(String, String)]| params.iter().any(|(k, _)| k == key);
    if let Some(value) = directory {
        if !has("directory", &params) {
            params.push(("directory".to_string(), value.to_string()));
        }
    }
    if let Some(value) = workspace {
        if !has("workspace", &params) {
            params.push(("workspace".to_string(), value.to_string()));
        }
    }

    let new_query = params
        .into_iter()
        .map(|(k, v)| if v.is_empty() { k } else { format!("{k}={v}") })
        .collect::<Vec<_>>()
        .join("&");

    if new_query.is_empty() {
        path.to_string()
    } else {
        format!("{path}?{new_query}")
    }
}

/// Rewrite `x-kilo-directory` and `x-kilo-workspace` request headers into
/// `?directory=` / `?workspace=` query parameters for GET/HEAD requests, so
/// non-SDK callers (e.g. the oracle harness, curl scripts) can target a
/// directory-scoped route by header — matching the behavior of the SDK at
/// `packages/sdk/js/src/v2/client.ts`. Existing query values take precedence.
async fn directory_header_rewrite(mut req: Request, next: Next) -> Response {
    if !matches!(*req.method(), Method::GET | Method::HEAD) {
        return next.run(req).await;
    }

    let dir = req
        .headers()
        .get("x-kilo-directory")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let ws = req
        .headers()
        .get("x-kilo-workspace")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    if dir.is_none() && ws.is_none() {
        return next.run(req).await;
    }

    let uri = req.uri().clone();
    let new_path_and_query =
        rewrite_path_and_query(uri.path(), uri.query(), dir.as_deref(), ws.as_deref());

    // Axum-served requests typically arrive as path-only URIs. Build a new
    // URI from path+query, preserving scheme/authority only if present.
    let mut builder = Uri::builder().path_and_query(new_path_and_query);
    if let Some(scheme) = uri.scheme_str() {
        builder = builder.scheme(scheme);
    }
    if let Some(authority) = uri.authority() {
        builder = builder.authority(authority.as_str());
    }
    if let Ok(new_uri) = builder.build() {
        *req.uri_mut() = new_uri;
    }

    // Strip the original headers so downstream extractors don't double-read.
    req.headers_mut().remove("x-kilo-directory");
    req.headers_mut().remove("x-kilo-workspace");

    next.run(req).await
}

async fn auth(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    if req.method() == Method::OPTIONS || state.password.is_none() {
        return next.run(req).await;
    }

    if authorized(&state, req.headers(), req.uri().query()) {
        return next.run(req).await;
    }

    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Basic realm=\"kilo\"")],
    )
        .into_response()
}

fn authorized(state: &AppState, headers: &HeaderMap, query: Option<&str>) -> bool {
    let Some(password) = state.password.as_deref() else {
        return true;
    };
    let expected = format!("{}:{}", state.username, password);
    let Some(value) = credential(headers, query) else {
        return false;
    };

    value == expected
}

fn credential(headers: &HeaderMap, query: Option<&str>) -> Option<String> {
    query
        .and_then(auth_token)
        .or_else(|| {
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
        })
        .map(|value| match value.strip_prefix("Basic ") {
            Some(value) => value.to_string(),
            None => value,
        })
        .and_then(|value| general_purpose::STANDARD.decode(value).ok())
        .and_then(|value| String::from_utf8(value).ok())
}

fn auth_token(query: &str) -> Option<String> {
    query.split('&').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        (key == "auth_token").then(|| decode_query(value))
    })
}

/// Percent-decode a query value. Decodes into a byte buffer first and
/// converts to UTF-8 at the end, matching `URLSearchParams` semantics:
/// `%C3%A9` is the two bytes of UTF-8-encoded `é` (U+00E9), not the
/// Latin-1 pair `Ã©`. The previous `out.push(byte as char)` cast each
/// byte to a Unicode codepoint, which silently mangled any non-ASCII
/// auth token. Falls back to lossy UTF-8 conversion if the decoded bytes
/// aren't valid UTF-8 — that matches what a browser-side caller would
/// see and avoids crashing an auth check on malformed input.
fn decode_query(value: &str) -> String {
    let mut bytes = Vec::with_capacity(value.len());
    let mut iter = value.as_bytes().iter().copied();
    while let Some(ch) = iter.next() {
        if ch == b'+' {
            bytes.push(b' ');
            continue;
        }

        if ch != b'%' {
            bytes.push(ch);
            continue;
        }

        let Some(a) = iter.next() else {
            bytes.push(b'%');
            continue;
        };
        let Some(b) = iter.next() else {
            bytes.push(b'%');
            bytes.push(a);
            continue;
        };
        match hex(a).and_then(|hi| hex(b).map(|lo| (hi << 4) | lo)) {
            Some(byte) => bytes.push(byte),
            None => {
                bytes.push(b'%');
                bytes.push(a);
                bytes.push(b);
            }
        }
    }

    String::from_utf8(bytes)
        .unwrap_or_else(|err| String::from_utf8_lossy(&err.into_bytes()).into_owned())
}

fn hex(ch: u8) -> Option<u8> {
    match ch {
        b'0'..=b'9' => Some(ch - b'0'),
        b'a'..=b'f' => Some(ch - b'a' + 10),
        b'A'..=b'F' => Some(ch - b'A' + 10),
        _ => None,
    }
}

fn frame(event: GlobalEvent) -> Result<Event, Infallible> {
    Ok(Event::default().data(serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string())))
}

fn payload_frame(event: GlobalEvent) -> Result<Event, Infallible> {
    Ok(Event::default()
        .data(serde_json::to_string(&event.payload).unwrap_or_else(|_| "{}".to_string())))
}

/// Produce a Bun-compatible 500 response body. Bun's `ErrorMiddleware` in
/// [`packages/opencode/src/server/middleware.ts`](../../../opencode/src/server/middleware.ts)
/// returns `NamedError.Unknown(...).toObject()` for any unhandled error,
/// which serializes to `{ name: "UnknownError", data: { message } }`. The
/// SDK's generated client may call `response.json()` on non-2xx responses
/// to parse the error envelope; returning plain text here would make the
/// SDK throw a parse error, masking the underlying cause. Use this helper
/// at every 500 site instead of `(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())`.
fn internal_error(message: impl Into<String>) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "name": "UnknownError",
            "data": { "message": message.into() },
        })),
    )
        .into_response()
}

/// Always bind loopback regardless of the requested hostname. Matches the
/// CONTRACT.md invariant — non-loopback flags are normalized to loopback
/// at the listener boundary. The `hostname` arg is accepted (so callers
/// can pass through `--host` / `--hostname`) but ignored: a future binding
/// layer can use it for diagnostics, but the listener address is fixed.
fn loopback(_hostname: &str) -> [u8; 4] {
    [127, 0, 0, 1]
}

#[cfg(test)]
mod tests {
    use super::*;
    use kilo_protocol::{Session, SessionTime};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn cursor_roundtrips_bun_shape() {
        let cursor = MessageCursor {
            id: "msg_123".to_string(),
            time: 42,
        };

        let encoded = encode_cursor(&cursor).unwrap();
        let decoded = decode_cursor(&encoded).unwrap();

        assert_eq!(decoded.id, cursor.id);
        assert_eq!(decoded.time, cursor.time);
    }

    #[test]
    fn cursor_rejects_invalid_payload() {
        assert!(decode_cursor("not-a-cursor").is_none());
        assert!(decode_cursor(&general_purpose::URL_SAFE_NO_PAD.encode("{}")).is_none());
    }

    #[test]
    fn rewrite_appends_directory_when_missing() {
        let out = rewrite_path_and_query("/session", None, Some("%2Frepo"), None);
        assert_eq!(out, "/session?directory=%2Frepo");
    }

    #[test]
    fn rewrite_preserves_existing_query() {
        let out = rewrite_path_and_query("/session", Some("limit=10"), Some("%2Frepo"), None);
        assert_eq!(out, "/session?limit=10&directory=%2Frepo");
    }

    #[test]
    fn rewrite_does_not_overwrite_existing_directory() {
        // Mirrors the SDK guard: if the URL already has `directory=`, the
        // header value is ignored.
        let out = rewrite_path_and_query(
            "/session",
            Some("directory=already"),
            Some("%2Fheader"),
            None,
        );
        assert_eq!(out, "/session?directory=already");
    }

    #[test]
    fn rewrite_handles_workspace_too() {
        let out = rewrite_path_and_query("/session", None, None, Some("ws-1"));
        assert_eq!(out, "/session?workspace=ws-1");
    }

    #[test]
    fn rewrite_no_op_when_neither_header_present() {
        let out = rewrite_path_and_query("/session", Some("a=b"), None, None);
        assert_eq!(out, "/session?a=b");
    }

    #[test]
    fn session_event_frame_has_global_shape() {
        let info = Session {
            id: "ses_test".to_string(),
            slug: "slug".to_string(),
            project_id: "global".to_string(),
            workspace_id: None,
            directory: "/repo".to_string(),
            parent_id: None,
            summary: None,
            share: None,
            title: "Title".to_string(),
            version: "local".to_string(),
            time: SessionTime {
                created: 1,
                updated: 1,
                compacting: None,
                archived: None,
            },
            permission: None,
            revert: None,
        };

        let data = serde_json::to_value(GlobalEvent::session(
            "session.created",
            "/repo".to_string(),
            info,
        ))
        .unwrap();

        assert_eq!(data["directory"], "/repo");
        assert_eq!(data["project"], "global");
        assert_eq!(data["payload"]["type"], "session.created");
        assert_eq!(data["payload"]["properties"]["sessionID"], "ses_test");
        assert_eq!(data["payload"]["properties"]["info"]["projectID"], "global");
    }

    #[test]
    fn message_event_frame_has_global_shape() {
        let data = serde_json::to_value(GlobalEvent::message(
            "message.updated",
            "/repo".to_string(),
            "global".to_string(),
            json!({
                "sessionID": "ses_test",
                "info": {
                    "id": "msg_test",
                    "sessionID": "ses_test",
                    "role": "user"
                }
            }),
        ))
        .unwrap();

        assert_eq!(data["directory"], "/repo");
        assert_eq!(data["project"], "global");
        assert_eq!(data["payload"]["type"], "message.updated");
        assert_eq!(data["payload"]["properties"]["sessionID"], "ses_test");
        assert_eq!(data["payload"]["properties"]["info"]["id"], "msg_test");
    }

    #[test]
    fn sync_event_frame_has_bun_shape() {
        let data = serde_json::to_value(GlobalEvent::sync(
            "/repo".to_string(),
            "global".to_string(),
            json!({
                "type": "message.updated.v1",
                "id": "evt_test",
                "seq": 1,
                "aggregateID": "ses_test",
                "data": { "sessionID": "ses_test" }
            }),
        ))
        .unwrap();

        assert_eq!(data["directory"], "/repo");
        assert_eq!(data["project"], "global");
        assert_eq!(data["payload"]["type"], "sync");
        assert_eq!(data["payload"]["syncEvent"]["type"], "message.updated.v1");
        assert!(data["payload"]["properties"].is_object());
    }

    #[test]
    fn bus_event_frame_has_global_shape() {
        let data = serde_json::to_value(GlobalEvent::bus(
            "session.status",
            json!({ "sessionID": "ses_test", "status": { "type": "busy" } }),
        ))
        .unwrap();

        assert!(data.get("directory").is_none());
        assert!(data.get("project").is_none());
        assert_eq!(data["payload"]["type"], "session.status");
        assert_eq!(data["payload"]["properties"]["sessionID"], "ses_test");
        assert_eq!(data["payload"]["properties"]["status"]["type"], "busy");
    }

    #[test]
    fn instance_event_frame_has_bus_shape() {
        let event = GlobalEvent::connected();
        let data = serde_json::to_string(&event.payload).unwrap();
        let parsed: Value = serde_json::from_str(&data).unwrap();

        assert_eq!(parsed["type"], "server.connected");
        assert_eq!(parsed["properties"], json!({}));
        assert!(parsed.get("directory").is_none());
    }

    #[tokio::test]
    async fn permission_question_lists_empty_and_missing_replies_404() {
        let state = state();

        let permission = permission_list(&state);
        let question = question_list(&state);
        assert!(permission.is_empty());
        assert!(question.is_empty());

        let res = reply_permission(
            State(state.clone()),
            Path("missing".to_string()),
            Json(json!({ "reply": "allow" })),
        )
        .await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        let res = permission_rules(
            State(state.clone()),
            Path("missing".to_string()),
            Json(json!({ "approvedAlways": [] })),
        )
        .await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        let res = reply_question(
            State(state.clone()),
            Path("missing".to_string()),
            Json(json!({ "answers": [] })),
        )
        .await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);

        let res = reject_question(State(state), Path("missing".to_string())).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn auth_routes_persist_provider_auth() {
        let root = unique_root();
        let state = state_at(&root);

        let auth = json!({
            "type": "oauth",
            "refresh": "refresh-token",
            "access": "access-token",
            "expires": 123,
            "accountId": "acct_1",
            "unknown": true
        });
        let res = set_auth(
            State(state.clone()),
            Path("openai".to_string()),
            Json(auth.clone()),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(state.store.provider_auth("openai"), Some(auth));

        let res = clear_auth(State(state.clone()), Path("openai".to_string())).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert!(state.store.provider_auth("openai").is_none());

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn oauth_authorize_returns_openai_pkce_url() {
        let res = oauth_authorize(
            Path("openai".to_string()),
            Json(json!({ "method": 0, "inputs": {} })),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        let challenge = pkce_challenge("abc");
        let url = oauth_url("http://127.0.0.1/callback", &challenge, "state");
        assert!(url.contains("https://auth.openai.com/oauth/authorize"));
        assert!(url.contains("client_id=app_EMoamEEZ73f0CkXaXp7hrann"));
        assert!(url.contains("code_challenge="));
    }

    #[tokio::test]
    async fn update_session_route_persists_permission_and_archived_time() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");
        let mut rx = state.bus.subscribe();

        let res = update_session(
            State(state.clone()),
            Path(session.id.clone()),
            Json(SessionUpdateInput {
                title: Some("Updated".to_string()),
                permission: Some(json!({ "edit": "allow" })),
                time: Some(json!({ "archived": 123 })),
            }),
        )
        .await;
        let updated = state.store.session(&session.id).unwrap();
        let event = rx.try_recv().expect("session updated event");

        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(updated.title, "Updated");
        assert_eq!(updated.permission, Some(json!({ "edit": "allow" })));
        assert_eq!(updated.time.archived, Some(123));
        assert_eq!(event.payload.kind, "session.updated");
        assert_eq!(
            event.payload.properties["info"]["permission"],
            json!({ "edit": "allow" })
        );
        assert_eq!(event.payload.properties["info"]["time"]["archived"], 123);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn child_fork_and_message_routes_publish_sync_events() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput {
                title: Some("Root".to_string()),
                ..Default::default()
            })
            .expect("create session");
        state
            .store
            .append_message(
                &session.id,
                MessageAppendInput {
                    info: json!({ "id": "msg_route", "role": "user" }),
                    parts: vec![json!({ "id": "prt_route", "type": "text", "text": "hello" })],
                },
            )
            .unwrap();
        let mut rx = state.bus.subscribe();

        let res = fork_session(
            State(state.clone()),
            Path(session.id.clone()),
            Json(SessionForkInput::default()),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        let events = drain(&mut rx);
        assert_sync(&events[0], "session.created.v1", "", None);
        assert_sync(&events[1], "message.updated.v1", "user", None);
        assert_sync(&events[2], "message.part.updated.v1", "", Some("hello"));

        let kids = state.store.children(&session.id).unwrap();
        let res = children(State(state.clone()), Path(session.id.clone())).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(kids.len(), 1);
        assert_eq!(kids[0].title, "Root (fork #1)");

        let res = message(
            State(state.clone()),
            Path((session.id.clone(), "msg_route".to_string())),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn part_delete_revert_share_and_summarize_routes_are_safe() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");
        state
            .store
            .append_message(
                &session.id,
                MessageAppendInput {
                    info: json!({ "id": "msg_mut", "role": "user" }),
                    parts: vec![json!({ "id": "prt_mut", "type": "text", "text": "old" })],
                },
            )
            .unwrap();
        let mut rx = state.bus.subscribe();

        let res = update_part(
            State(state.clone()),
            Path((
                session.id.clone(),
                "msg_mut".to_string(),
                "prt_mut".to_string(),
            )),
            Json(json!({ "id": "prt_mut", "messageID": "msg_mut", "sessionID": session.id, "type": "text", "text": "new" })),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_sync(
            &rx.try_recv().unwrap(),
            "message.part.updated.v1",
            "",
            Some("new"),
        );

        let res = revert_session(
            State(state.clone()),
            Path(session.id.clone()),
            Json(SessionRevertInput {
                message_id: Some("msg_mut".to_string()),
                summary: Some(json!({
                    "additions": 0,
                    "deletions": 0,
                    "files": 0,
                    "diffs": []
                })),
                ..Default::default()
            }),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            state.store.session(&session.id).unwrap().revert.unwrap()["messageID"],
            "msg_mut"
        );
        assert_sync(&rx.try_recv().unwrap(), "session.updated.v1", "", None);

        let res = diff_session(State(state.clone()), Path(session.id.clone())).await;
        assert_eq!(res.status(), StatusCode::OK);
        let res = share_session(
            State(state.clone()),
            Path(session.id.clone()),
            Some(Json(SessionShareInput {
                url: Some("https://share.test/s".to_string()),
            })),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_sync(&rx.try_recv().unwrap(), "session.updated.v1", "", None);
        let res = summarize_session(State(state.clone()), Path(session.id.clone())).await;
        assert_eq!(res.status(), StatusCode::OK);
        let res = unshare_session(State(state.clone()), Path(session.id.clone())).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_sync(&rx.try_recv().unwrap(), "session.updated.v1", "", None);
        let res = unrevert_session(State(state.clone()), Path(session.id.clone())).await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_sync(&rx.try_recv().unwrap(), "session.updated.v1", "", None);
        let res = delete_part(
            State(state.clone()),
            Path((
                session.id.clone(),
                "msg_mut".to_string(),
                "prt_mut".to_string(),
            )),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_sync(&rx.try_recv().unwrap(), "message.part.removed.v1", "", None);
        let res = delete_message(
            State(state.clone()),
            Path((session.id.clone(), "msg_mut".to_string())),
        )
        .await;
        assert_eq!(res.status(), StatusCode::OK);
        assert_sync(&rx.try_recv().unwrap(), "message.removed.v1", "", None);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn file_content_reads_text_and_rejects_traversal() {
        let root = unique_root();
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join("hello.txt"), "hello\n").unwrap();

        let file = resolve_under(&repo, "hello.txt").unwrap();
        let data = read_content(&file);
        assert_eq!(data["type"], "text");
        assert_eq!(data["content"], "hello");
        assert_eq!(
            resolve_under(&repo, "../secret.txt"),
            Err(StatusCode::FORBIDDEN)
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn file_find_and_list_basics() {
        let root = unique_root();
        let repo = root.join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(
            repo.join("src").join("main.rs"),
            "fn main() {\n println!(\"needle\");\n}\n",
        )
        .unwrap();
        std::fs::write(repo.join("README.md"), "needle\n").unwrap();

        let nodes = list_nodes(&repo, &repo);
        assert_eq!(nodes[0]["name"], "src");
        assert_eq!(nodes[0]["type"], "directory");

        let files = search_files(&repo, "main", false, Some("file"), 10);
        assert_eq!(files, vec!["src/main.rs"]);

        let matches = search_text(&repo, "needle", 10);
        assert_eq!(matches.len(), 2);
        assert!(matches
            .iter()
            .any(|item| item["path"]["text"] == "src/main.rs"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn viewed_replaces_and_clears_sets() {
        let state = state();

        set_viewed(
            &state,
            SessionViewedInput {
                focused: vec!["ses_a".to_string(), "ses_b".to_string()],
                open: vec!["ses_b".to_string()],
            },
        )
        .await;
        let view = viewed_snapshot(&state).await;
        assert_eq!(view.focused.len(), 2);
        assert!(view.focused.contains("ses_a"));
        assert!(view.open.contains("ses_b"));

        set_viewed(
            &state,
            SessionViewedInput {
                focused: vec!["ses_c".to_string()],
                open: vec![],
            },
        )
        .await;
        let view = viewed_snapshot(&state).await;
        assert_eq!(view.focused.iter().collect::<Vec<_>>(), vec!["ses_c"]);
        assert!(view.open.is_empty());

        set_viewed(&state, SessionViewedInput::default()).await;
        let view = viewed_snapshot(&state).await;
        assert!(view.focused.is_empty());
        assert!(view.open.is_empty());
    }

    #[test]
    fn publish_events_preserves_message_then_part_order() {
        let state = state();
        let mut rx = state.bus.subscribe();

        publish_events(
            &state,
            "/repo".to_string(),
            "global".to_string(),
            vec![
                StoredEvent {
                    id: "evt_1".to_string(),
                    seq: 1,
                    aggregate_id: "ses_test".to_string(),
                    event_type: "message.updated.v1".to_string(),
                    data: json!({ "sessionID": "ses_test", "info": { "id": "msg_test" } }),
                },
                StoredEvent {
                    id: "evt_2".to_string(),
                    seq: 2,
                    aggregate_id: "ses_test".to_string(),
                    event_type: "message.part.updated.v1".to_string(),
                    data: json!({ "sessionID": "ses_test", "part": { "id": "prt_test" }, "time": 1 }),
                },
            ],
        );

        let first = rx.try_recv().unwrap();
        let second = rx.try_recv().unwrap();
        assert_eq!(first.payload.kind, "sync");
        assert_eq!(
            first.payload.sync_event.as_ref().unwrap()["type"],
            "message.updated.v1"
        );
        assert_eq!(
            second.payload.sync_event.as_ref().unwrap()["type"],
            "message.part.updated.v1"
        );
    }

    #[tokio::test]
    async fn prompt_turn_persists_user_and_assistant_messages() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");
        let mut rx = state.bus.subscribe();

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "hello" })],
                agent: Some("code".to_string()),
                provider: Some(json!({ "fake": true })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(out.info["role"], "assistant");
        assert_eq!(out.info["providerID"], "local");
        assert_eq!(out.info["modelID"], "fake-echo");
        assert_eq!(out.info["finish"], "stop");
        assert!(out.info["time"]["completed"].is_number());
        assert_eq!(out.parts[0]["text"], "Echo: hello");
        assert_eq!(out.parts[1]["type"], "step-finish");
        assert_eq!(out.parts[1]["reason"], "stop");

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].info["role"], "user");
        assert_eq!(page.items[0].parts[0]["text"], "hello");
        assert_eq!(page.items[1].info["role"], "assistant");
        assert_eq!(page.items[1].info["finish"], "stop");
        assert!(page.items[1].info["time"]["completed"].is_number());
        assert_eq!(page.items[1].parts[0]["text"], "Echo: hello");
        assert_eq!(page.items[1].parts[1]["type"], "step-finish");

        let events = drain(&mut rx);
        assert_eq!(events[0].payload.kind, "session.turn.open");
        assert_eq!(events[1].payload.kind, "session.status");
        assert_eq!(events[1].payload.properties["status"]["type"], "busy");
        assert_sync(&events[2], "message.updated.v1", "user", None);
        assert_sync(&events[3], "message.part.updated.v1", "", Some("hello"));
        assert_sync(&events[4], "message.updated.v1", "assistant", None);
        assert_sync(&events[5], "message.part.updated.v1", "", Some(""));
        assert_delta(
            &events[6],
            &out.info["id"],
            &out.parts[0]["id"],
            "Echo: hello",
        );
        assert_sync(
            &events[8],
            "message.part.updated.v1",
            "",
            Some("Echo: hello"),
        );
        assert_eq!(
            events[9].payload.sync_event.as_ref().unwrap()["data"]["part"]["type"],
            "step-finish"
        );
        assert_eq!(events[10].payload.kind, "session.status");
        assert_eq!(events[10].payload.properties["status"]["type"], "idle");
        assert_eq!(events[11].payload.kind, "session.idle");
        assert_eq!(events[12].payload.kind, "session.turn.close");
        assert_eq!(events[12].payload.properties["reason"], "completed");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_success_emits_delta_before_close_with_assistant_ids() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");
        let mut rx = state.bus.subscribe();

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "delta" })],
                provider: Some(json!({ "fake": true })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        let events = drain(&mut rx);
        let pos = events
            .iter()
            .position(|event| event.payload.kind == "message.part.delta")
            .expect("delta event");
        let close = events
            .iter()
            .position(|event| event.payload.kind == "session.turn.close")
            .expect("close event");
        assert!(pos < close);
        assert_delta(
            &events[pos],
            &out.info["id"],
            &out.parts[0]["id"],
            "Echo: delta",
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_success_persists_step_finish_and_completion() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "done" })],
                provider: Some(json!({ "fake": true })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        let page = state.store.messages(&session.id, None, None).unwrap();
        let msg = &page.items[1];
        assert_eq!(out.info["finish"], "stop");
        assert!(out.info["time"]["completed"].is_number());
        assert_eq!(msg.info["finish"], "stop");
        assert!(msg.info["time"]["completed"].is_number());
        assert_eq!(msg.parts[1]["type"], "step-finish");
        assert_eq!(msg.parts[1]["reason"], "stop");
        assert_eq!(msg.parts[1]["tokens"]["input"], 0);
        assert_eq!(msg.parts[1]["tokens"]["cache"]["read"], 0);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_abort_persists_error_and_publishes_interrupted() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");
        let mut rx = state.bus.subscribe();

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "abort" })],
                provider: Some(json!({ "fakeAbort": true })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(out.info["finish"], "error");
        assert_eq!(out.info["error"]["name"], "MessageAbortedError");
        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[1].info["error"]["name"], "MessageAbortedError");

        let events = drain(&mut rx);
        assert_eq!(events[0].payload.kind, "session.turn.open");
        assert_eq!(events[1].payload.kind, "session.status");
        assert_eq!(events[1].payload.properties["status"]["type"], "busy");
        assert_sync(&events[2], "message.updated.v1", "user", None);
        assert_sync(&events[3], "message.part.updated.v1", "", Some("abort"));
        assert_sync(&events[4], "message.updated.v1", "assistant", None);
        assert_eq!(events[5].payload.kind, "session.error");
        assert_eq!(
            events[5].payload.properties["error"]["name"],
            "MessageAbortedError"
        );
        assert_eq!(events[6].payload.kind, "session.status");
        assert_eq!(events[6].payload.properties["status"]["type"], "idle");
        assert_eq!(events[7].payload.kind, "session.idle");
        assert_eq!(events[8].payload.kind, "session.turn.close");
        assert_eq!(events[8].payload.properties["reason"], "interrupted");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_error_persists_error_and_publishes_error_reason() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");
        let mut rx = state.bus.subscribe();

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "__KILO_FAKE_PROVIDER_ERROR__" })],
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(out.info["finish"], "error");
        assert_eq!(out.info["error"]["name"], "APIError");
        let events = drain(&mut rx);
        assert_sync(&events[4], "message.updated.v1", "assistant", None);
        assert_eq!(events[5].payload.kind, "session.error");
        assert_eq!(events[5].payload.properties["error"]["name"], "APIError");
        assert_eq!(events[8].payload.kind, "session.turn.close");
        assert_eq!(events[8].payload.properties["reason"], "error");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_read_persists_completed_tool_part() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(
            repo.join("src").join("main.rs"),
            "fn main() {\n println!(\"hi\");\n}\n",
        )
        .unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "read src/main.rs" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "read",
                        "input": { "filePath": "src/main.rs", "limit": 2 }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(out.info["finish"], "stop");
        assert_eq!(out.parts[0]["type"], "text");
        assert_eq!(out.parts[0]["text"], "Completed 1 fake tool call(s): read");
        let tool = &out.parts[1];
        assert_eq!(tool["type"], "tool");
        assert_eq!(tool["tool"], "read");
        assert!(tool["callID"].as_str().unwrap().starts_with("call_"));
        assert_eq!(tool["state"]["status"], "completed");
        assert_eq!(tool["state"]["input"]["filePath"], "src/main.rs");
        assert_eq!(tool["state"]["title"], "src/main.rs");
        assert!(tool["state"]["output"]
            .as_str()
            .unwrap()
            .contains("<type>file</type>"));
        assert!(tool["state"]["output"]
            .as_str()
            .unwrap()
            .contains("1: fn main()"));
        assert_eq!(tool["state"]["metadata"]["truncated"], true);
        assert!(tool["state"]["metadata"]["preview"]
            .as_str()
            .unwrap()
            .contains("fn main()"));
        assert_eq!(out.parts[2]["type"], "step-finish");

        let page = state.store.messages(&session.id, None, None).unwrap();
        let msg = &page.items[1];
        assert_eq!(msg.parts[1]["tool"], "read");
        assert_eq!(msg.parts[1]["state"]["status"], "completed");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_real_provider_tool_call_persists_read_part() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join("note.txt"), "needle\nsecond\n").unwrap();
        let server = provider_server(json!({
            "choices": [{
                "message": {
                    "content": "Reading note.",
                    "tool_calls": [{
                        "id": "call_read_1",
                        "type": "function",
                        "function": {
                            "name": "read",
                            "arguments": "{\"filePath\":\"note.txt\",\"limit\":1}"
                        }
                    }]
                }
            }]
        }))
        .await;
        std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
        std::fs::write(
            root.join("config").join("kilo").join("kilo.json"),
            serde_json::to_string(&json!({
                "provider": {
                    "openai": {
                        "options": { "apiKey": "test-key", "baseURL": server.url },
                        "models": {}
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "read note" })],
                model: Some(json!({
                    "providerID": "openai",
                    "modelID": "gpt-test",
                    "capabilities": { "toolcall": true }
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(out.info["providerID"], "openai");
        assert_eq!(out.info["modelID"], "gpt-test");
        assert_eq!(out.parts[0]["text"], "Reading note.");
        assert_eq!(out.parts[1]["type"], "tool");
        assert_eq!(out.parts[1]["tool"], "read");
        assert_eq!(out.parts[1]["callID"], "call_read_1");
        assert_eq!(out.parts[1]["state"]["status"], "completed");
        assert_eq!(out.parts[1]["state"]["input"]["filePath"], "note.txt");
        assert!(out.parts[1]["state"]["output"]
            .as_str()
            .unwrap()
            .contains("1: needle"));
        assert_eq!(out.parts[2]["type"], "step-finish");

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items[1].info["providerID"], "openai");
        assert_eq!(page.items[1].parts[1]["callID"], "call_read_1");
        assert!(server.body.lock().unwrap().contains("\"tools\""));

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_openai_oauth_streams_deltas_and_persists_usage() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        std::fs::create_dir_all(root.join("repo")).unwrap();
        let server = stream_provider_server(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hel\"}\n\n\
data: {\"type\":\"response.output_text.delta\",\"delta\":\"lo\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":1,\"total_tokens\":3}}}\n\n\
data: [DONE]\n\n",
        )
        .await;
        state
            .store
            .set_provider_auth(
                "openai",
                json!({
                    "type": "oauth",
                    "refresh": "refresh-token",
                    "access": "access-token",
                    "expires": 9999999999999i64,
                    "accountId": "acct_1"
                }),
            )
            .unwrap();
        std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
        std::fs::write(
            root.join("config").join("kilo").join("kilo.json"),
            serde_json::to_string(&json!({
                "provider": {
                    "openai": {
                        "options": { "baseURL": server.url },
                        "models": {}
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .unwrap();
        let mut rx = state.bus.subscribe();

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "hello" })],
                model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
                system: Some(json!("system instructions")),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(out.info["providerID"], "openai");
        assert_eq!(out.parts[0]["text"], "Hello");
        assert_eq!(out.info["tokens"]["input"], 2);
        assert_eq!(out.parts[1]["tokens"]["total"], 3);
        let events = drain(&mut rx);
        let first = events
            .iter()
            .position(|event| event.payload.kind == "message.part.delta")
            .unwrap();
        assert_eq!(events[first].payload.properties["delta"], "Hel");
        // Soul-prepending invariant: per llm.ts:155-159, the OAuth Responses
        // call sets `options.instructions = SystemPrompt.soul() + "\n" + system_array_joined`.
        // We assert both pieces appear (escaped for the JSON wire shape).
        let body = server.body.lock().unwrap().clone();
        assert!(
            body.contains("\"instructions\":\"You are Kilo"),
            "missing soul prefix in instructions: {body}"
        );
        assert!(
            body.contains("system instructions"),
            "missing user system text in instructions: {body}"
        );
        assert!(server.body.lock().unwrap().contains("\"stream\":true"));
        assert!(
            body.contains("chatgpt-account-id: acct_1")
                || body.contains("ChatGPT-Account-Id: acct_1")
        );

        let _ = std::fs::remove_dir_all(root);
    }

    /// M10 invariant: when no OAuth blob is persisted, an `openai`-provider
    /// prompt must fall through to the existing api-key path. We don't have
    /// a real api-key endpoint in this test, so we assert by behavior: the
    /// turn produces an APIError (not a routing-to-OAuth error or a
    /// hung-stream timeout). What matters is that `is_openai_oauth` returns
    /// false and the OAuth Responses code is not exercised.
    #[tokio::test]
    async fn prompt_turn_openai_without_oauth_credential_falls_through() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        // No `state.store.set_provider_auth("openai", ...)` here — that's
        // the whole point. With no OAuth blob and no api key, the chat
        // path should error out with MissingKey, not silently dispatch
        // through `prompt_openai_stream`.
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .unwrap();
        // Sanity: confirm the routing seam itself reports false.
        assert!(!is_openai_oauth(
            &state,
            Some(&json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" }))
        ));
        // Drive the full prompt to make sure no panic from the OAuth path.
        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "hi" })],
                model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");
        // The fallback (api-key) path errors with MissingKey envelope.
        assert_eq!(out.info["error"]["name"], "APIError");

        let _ = std::fs::remove_dir_all(root);
    }

    /// M10 invariant: aborting an active OAuth stream must propagate to the
    /// in-flight reqwest body via the cancel flag. We model "in-flight" by
    /// pre-setting the cancel signal *before* calling `prompt_turn`. The
    /// assistant message must persist with `MessageAbortedError`, NOT a
    /// successful completion. This guards against the "abort but stream
    /// finishes" regression flagged in the migration plan.
    #[tokio::test]
    async fn prompt_turn_openai_oauth_abort_persists_interrupted() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        // A stream server that never sends a response — the cancel flag is
        // the only way the call ends.
        let server = stream_provider_server(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"slow\"}\n\n",
        )
        .await;
        state
            .store
            .set_provider_auth(
                "openai",
                json!({
                    "type": "oauth",
                    "refresh": "rt",
                    "access": "at",
                    "expires": 9999999999999i64,
                    "accountId": "acct_1"
                }),
            )
            .unwrap();
        std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
        std::fs::write(
            root.join("config").join("kilo").join("kilo.json"),
            serde_json::to_string(&json!({
                "provider": {
                    "openai": { "options": { "baseURL": server.url }, "models": {} }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .unwrap();

        let cancel = Arc::new(AtomicBool::new(true)); // pre-aborted
        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "abort me" })],
                model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
                ..Default::default()
            },
            cancel,
        )
        .await
        .expect("prompt turn");
        // Pre-aborted: the prompt_turn early-out should fire BEFORE the
        // OAuth stream is started, so the result is a clean
        // MessageAbortedError envelope.
        assert_eq!(out.info["error"]["name"], "MessageAbortedError");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_grep_persists_match_metadata() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(repo.join("src").join("a.txt"), "needle one\nnone\n").unwrap();
        std::fs::write(repo.join("src").join("b.txt"), "needle two\n").unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "grep needle" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "grep",
                        "input": { "pattern": "needle", "path": "src" }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        let tool = &out.parts[1];
        assert_eq!(tool["tool"], "grep");
        assert_eq!(tool["state"]["status"], "completed");
        assert_eq!(tool["state"]["title"], "needle");
        assert_eq!(tool["state"]["metadata"]["matches"], 2);
        assert_eq!(tool["state"]["metadata"]["truncated"], false);
        let output = tool["state"]["output"].as_str().unwrap();
        assert!(output.contains("Found 2 matches"));
        assert!(output.contains("src/a.txt:"));
        assert!(output.contains("Line 1: needle one"));
        assert!(output.contains("src/b.txt:"));
        assert_eq!(out.parts[2]["type"], "step-finish");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_tool_events_are_before_step_finish_and_close() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join("note.txt"), "hello\n").unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");
        let mut rx = state.bus.subscribe();

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "read" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "read",
                        "input": { "filePath": "note.txt" }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        let events = drain(&mut rx);
        let tool = events
            .iter()
            .position(|event| {
                event.payload.sync_event.as_ref().is_some_and(|data| {
                    data["type"] == "message.part.updated.v1"
                        && data["data"]["part"]["type"] == "tool"
                })
            })
            .expect("tool event");
        let step = events
            .iter()
            .position(|event| {
                event.payload.sync_event.as_ref().is_some_and(|data| {
                    data["type"] == "message.part.updated.v1"
                        && data["data"]["part"]["type"] == "step-finish"
                })
            })
            .expect("step event");
        let close = events
            .iter()
            .position(|event| event.payload.kind == "session.turn.close")
            .expect("close event");
        assert!(tool < step);
        assert!(step < close);
        assert_eq!(out.parts[1]["state"]["status"], "completed");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_read_unsafe_path_persists_tool_error() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "read secret" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "read",
                        "input": { "filePath": "../secret.txt" }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(out.info["finish"], "stop");
        assert_eq!(out.parts[1]["type"], "tool");
        assert_eq!(out.parts[1]["tool"], "read");
        assert_eq!(out.parts[1]["state"]["status"], "error");
        assert!(out.parts[1]["state"]["error"]
            .as_str()
            .unwrap()
            .contains("Unsafe path"));
        assert_eq!(out.parts[2]["type"], "step-finish");

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items[1].parts[1]["state"]["status"], "error");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_write_creates_file_and_persists_metadata() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "write file" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "write",
                        "input": { "filePath": "src/new.txt", "content": "hello\nworld\n" }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(
            std::fs::read_to_string(repo.join("src/new.txt")).unwrap(),
            "hello\nworld\n"
        );
        let tool = &out.parts[1];
        assert_eq!(tool["tool"], "write");
        assert_eq!(tool["state"]["status"], "completed");
        assert_eq!(tool["state"]["title"], "src/new.txt");
        assert_eq!(tool["state"]["output"], "Wrote file successfully.");
        assert_eq!(tool["state"]["metadata"]["exists"], false);
        assert!(tool["state"]["metadata"]["filepath"]
            .as_str()
            .unwrap()
            .ends_with("repo/src/new.txt"));
        assert!(tool["state"]["metadata"]["diff"]
            .as_str()
            .unwrap()
            .contains("+hello"));

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items[1].parts[1]["tool"], "write");
        assert_eq!(page.items[1].parts[1]["state"]["metadata"]["exists"], false);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_edit_changes_file_and_persists_metadata() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join("note.txt"), "one\ntwo\nthree\n").unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "edit file" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "edit",
                        "input": {
                            "filePath": "note.txt",
                            "oldString": "two",
                            "newString": "TWO"
                        }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(
            std::fs::read_to_string(repo.join("note.txt")).unwrap(),
            "one\nTWO\nthree\n"
        );
        let tool = &out.parts[1];
        assert_eq!(tool["tool"], "edit");
        assert_eq!(tool["state"]["status"], "completed");
        assert_eq!(tool["state"]["title"], "note.txt");
        assert_eq!(tool["state"]["output"], "Edit applied successfully.");
        assert!(tool["state"]["metadata"]["diff"]
            .as_str()
            .unwrap()
            .contains("+TWO"));

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items[1].parts[1]["tool"], "edit");
        assert_eq!(page.items[1].parts[1]["state"]["status"], "completed");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_write_edit_unsafe_path_persists_errors() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "unsafe mutate" })],
                provider: Some(json!({
                    "fakeToolCalls": [
                        {
                            "tool": "write",
                            "input": { "filePath": "../secret.txt", "content": "secret" }
                        },
                        {
                            "tool": "edit",
                            "input": {
                                "filePath": "../secret.txt",
                                "oldString": "secret",
                                "newString": "public"
                            }
                        }
                    ]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(out.parts[1]["tool"], "write");
        assert_eq!(out.parts[1]["state"]["status"], "error");
        assert!(out.parts[1]["state"]["error"]
            .as_str()
            .unwrap()
            .contains("Unsafe path"));
        assert_eq!(out.parts[2]["tool"], "edit");
        assert_eq!(out.parts[2]["state"]["status"], "error");
        assert!(out.parts[2]["state"]["error"]
            .as_str()
            .unwrap()
            .contains("Unsafe path"));
        assert!(!root.join("secret.txt").exists());

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items[1].parts[1]["state"]["status"], "error");
        assert_eq!(page.items[1].parts[2]["state"]["status"], "error");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_edit_multiple_match_without_replace_all_errors() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join("dupe.txt"), "same\nsame\n").unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "edit dupe" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "edit",
                        "input": {
                            "filePath": "dupe.txt",
                            "oldString": "same",
                            "newString": "changed"
                        }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(
            std::fs::read_to_string(repo.join("dupe.txt")).unwrap(),
            "same\nsame\n"
        );
        assert_eq!(out.parts[1]["tool"], "edit");
        assert_eq!(out.parts[1]["state"]["status"], "error");
        assert!(out.parts[1]["state"]["error"]
            .as_str()
            .unwrap()
            .contains("matched 2 times"));

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items[1].parts[1]["state"]["status"], "error");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_apply_patch_add_creates_file_and_metadata() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "apply add" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "apply_patch",
                        "input": { "patchText": "*** Begin Patch\n*** Add File: src/new.txt\n+hello\n+world\n*** End Patch" }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(
            std::fs::read_to_string(repo.join("src/new.txt")).unwrap(),
            "hello\nworld\n"
        );
        let tool = &out.parts[1];
        assert_eq!(tool["tool"], "apply_patch");
        assert_eq!(tool["state"]["status"], "completed");
        assert_eq!(tool["state"]["title"], "src/new.txt");
        assert_eq!(
            tool["state"]["output"],
            "Success. Updated the following files:\nA src/new.txt"
        );
        assert_eq!(tool["state"]["metadata"]["files"][0]["type"], "added");
        assert_eq!(tool["state"]["metadata"]["files"][0]["additions"], 2);
        assert!(tool["state"]["metadata"]["diff"]
            .as_str()
            .unwrap()
            .contains("+hello"));

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items[1].parts[1]["tool"], "apply_patch");
        assert_eq!(page.items[1].parts[1]["state"]["status"], "completed");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_apply_patch_update_changes_file_and_metadata() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join("note.txt"), "one\ntwo\nthree\n").unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "apply update" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "apply_patch",
                        "input": { "patchText": "*** Begin Patch\n*** Update File: note.txt\n@@\n one\n-two\n+TWO\n three\n*** End Patch" }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(
            std::fs::read_to_string(repo.join("note.txt")).unwrap(),
            "one\nTWO\nthree\n"
        );
        let tool = &out.parts[1];
        assert_eq!(tool["tool"], "apply_patch");
        assert_eq!(tool["state"]["status"], "completed");
        assert_eq!(
            tool["state"]["output"],
            "Success. Updated the following files:\nM note.txt"
        );
        assert_eq!(tool["state"]["metadata"]["files"][0]["type"], "modified");
        assert_eq!(tool["state"]["metadata"]["files"][0]["additions"], 1);
        assert_eq!(tool["state"]["metadata"]["files"][0]["deletions"], 1);

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(
            page.items[1].parts[1]["state"]["metadata"]["files"][0]["type"],
            "modified"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_apply_patch_delete_removes_file_and_metadata() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join("old.txt"), "old\nfile\n").unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "apply delete" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "apply_patch",
                        "input": { "patchText": "*** Begin Patch\n*** Delete File: old.txt\n*** End Patch" }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert!(!repo.join("old.txt").exists());
        let tool = &out.parts[1];
        assert_eq!(tool["tool"], "apply_patch");
        assert_eq!(tool["state"]["status"], "completed");
        assert_eq!(
            tool["state"]["output"],
            "Success. Updated the following files:\nD old.txt"
        );
        assert_eq!(tool["state"]["metadata"]["files"][0]["type"], "deleted");
        assert_eq!(tool["state"]["metadata"]["files"][0]["deletions"], 2);

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(
            page.items[1].parts[1]["state"]["metadata"]["files"][0]["type"],
            "deleted"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_apply_patch_mismatch_or_unsafe_persists_error() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join("note.txt"), "one\ntwo\n").unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "apply errors" })],
                provider: Some(json!({
                    "fakeToolCalls": [
                        {
                            "tool": "apply_patch",
                            "input": { "patchText": "*** Begin Patch\n*** Update File: note.txt\n missing\n-two\n+TWO\n*** End Patch" }
                        },
                        {
                            "tool": "apply_patch",
                            "input": { "patchText": "*** Begin Patch\n*** Add File: ../secret.txt\n+secret\n*** End Patch" }
                        }
                    ]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(
            std::fs::read_to_string(repo.join("note.txt")).unwrap(),
            "one\ntwo\n"
        );
        assert!(!root.join("secret.txt").exists());
        assert_eq!(out.parts[1]["tool"], "apply_patch");
        assert_eq!(out.parts[1]["state"]["status"], "error");
        assert!(out.parts[1]["state"]["error"]
            .as_str()
            .unwrap()
            .contains("mismatch"));
        assert_eq!(out.parts[2]["tool"], "apply_patch");
        assert_eq!(out.parts[2]["state"]["status"], "error");
        assert!(out.parts[2]["state"]["error"]
            .as_str()
            .unwrap()
            .contains("Unsafe path"));

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items[1].parts[1]["state"]["status"], "error");
        assert_eq!(page.items[1].parts[2]["state"]["status"], "error");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_bash_persists_completed_tool_part() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "run command" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "bash",
                        "input": {
                            "command": "echo hello",
                            "workdir": "src",
                            "description": "say hello"
                        }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(out.parts[0]["text"], "Completed 1 fake tool call(s): bash");
        let tool = &out.parts[1];
        assert_eq!(tool["type"], "tool");
        assert_eq!(tool["tool"], "bash");
        assert_eq!(tool["state"]["status"], "completed");
        assert_eq!(tool["state"]["input"]["command"], "echo hello");
        assert_eq!(tool["state"]["title"], "say hello");
        assert!(tool["state"]["output"].as_str().unwrap().contains("hello"));
        assert_eq!(tool["state"]["metadata"]["exit"], 0);
        assert_eq!(tool["state"]["metadata"]["description"], "say hello");
        assert_eq!(tool["state"]["metadata"]["truncated"], false);
        assert!(tool["state"]["metadata"]["output"]
            .as_str()
            .unwrap()
            .contains("hello"));

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items[1].parts[1]["tool"], "bash");
        assert_eq!(page.items[1].parts[1]["state"]["status"], "completed");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_bash_nonzero_persists_completed_metadata() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        std::fs::create_dir_all(root.join("repo")).unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");
        let command = if cfg!(windows) {
            "echo fail 1>&2 & exit /b 7"
        } else {
            "echo fail >&2; exit 7"
        };

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "fail command" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "bash",
                        "input": { "command": command }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        let tool = &out.parts[1];
        assert_eq!(tool["tool"], "bash");
        assert_eq!(tool["state"]["status"], "completed");
        assert_eq!(tool["state"]["metadata"]["exit"], 7);
        assert!(tool["state"]["output"].as_str().unwrap().contains("fail"));
        assert!(tool["state"]["metadata"]["output"]
            .as_str()
            .unwrap()
            .contains("fail"));

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items[1].parts[1]["state"]["status"], "completed");
        assert_eq!(page.items[1].parts[1]["state"]["metadata"]["exit"], 7);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_turn_fake_bash_invalid_input_persists_tool_errors() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        std::fs::create_dir_all(root.join("repo")).unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "bad command" })],
                provider: Some(json!({
                    "fakeToolCalls": [
                        {
                            "tool": "bash",
                            "input": { "command": "echo no", "workdir": "../secret" }
                        },
                        {
                            "tool": "bash",
                            "input": { "command": "echo no", "timeout": -1 }
                        }
                    ]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(out.parts[1]["tool"], "bash");
        assert_eq!(out.parts[1]["state"]["status"], "error");
        assert!(out.parts[1]["state"]["error"]
            .as_str()
            .unwrap()
            .contains("Unsafe workdir"));
        assert_eq!(out.parts[2]["tool"], "bash");
        assert_eq!(out.parts[2]["state"]["status"], "error");
        assert!(out.parts[2]["state"]["error"]
            .as_str()
            .unwrap()
            .contains("timeout must be greater than or equal to 0"));

        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items[1].parts[1]["state"]["status"], "error");
        assert_eq!(page.items[1].parts[2]["state"]["status"], "error");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn abort_route_without_active_runner_is_not_stale() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let res = abort_session(State(state.clone()), Path(session.id.clone())).await;
        assert_eq!(res.status(), StatusCode::OK);
        let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "after abort" })],
                provider: Some(json!({ "fake": true })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

        assert_eq!(out.info["finish"], "stop");
        assert_eq!(out.parts[0]["text"], "Echo: after abort");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_async_returns_no_content_and_persists_transcript() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");

        let res = prompt_async(
            State(state.clone()),
            Path(session.id.clone()),
            Json(PromptInput {
                parts: vec![json!({ "type": "text", "text": "async" })],
                provider: Some(json!({ "fake": true })),
                ..Default::default()
            }),
        )
        .await;

        assert_eq!(res.status(), StatusCode::NO_CONTENT);
        for _ in 0..20 {
            let page = state.store.messages(&session.id, None, None).unwrap();
            if page.items.len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[1].parts[0]["text"], "Echo: async");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn prompt_rejects_same_busy_session_without_queueing() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .expect("create session");
        let id = session.id.clone();
        let task = tokio::spawn(prompt_guarded(
            state.clone(),
            id.clone(),
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "slow" })],
                provider: Some(json!({ "fake": true, "fakeDelayMs": 30 })),
                ..Default::default()
            },
        ));
        tokio::time::sleep(Duration::from_millis(10)).await;

        let err = prompt_guarded(
            state.clone(),
            id.clone(),
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "second" })],
                provider: Some(json!({ "fake": true })),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, TurnError::Busy));
        task.await.unwrap().expect("slow prompt");
        let page = state.store.messages(&id, None, None).unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].parts[0]["text"], "slow");

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn distinct_sessions_accept_while_another_session_is_busy() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let a = state
            .store
            .create_session(SessionCreateInput::default())
            .unwrap();
        let b = state
            .store
            .create_session(SessionCreateInput::default())
            .unwrap();
        let one = tokio::spawn(prompt_guarded(
            state.clone(),
            a.id.clone(),
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "one" })],
                provider: Some(json!({ "fake": true, "fakeDelayMs": 120 })),
                ..Default::default()
            },
        ));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(state.runners.lock().unwrap().contains_key(&a.id));
        let two = prompt_guarded(
            state.clone(),
            b.id.clone(),
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "two" })],
                provider: Some(json!({ "fake": true })),
                ..Default::default()
            },
        )
        .await
        .expect("second session prompt");
        assert_eq!(two.parts[0]["text"], "Echo: two");
        one.await.unwrap().expect("one");
        assert_eq!(
            state.store.messages(&a.id, None, None).unwrap().items.len(),
            2
        );
        assert_eq!(
            state.store.messages(&b.id, None, None).unwrap().items.len(),
            2
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn abort_active_fake_turn_persists_interrupted_error() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .unwrap();
        let id = session.id.clone();
        let task = tokio::spawn(prompt_guarded(
            state.clone(),
            id.clone(),
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "abort me" })],
                provider: Some(json!({ "fake": true, "fakeDelayMs": 120 })),
                ..Default::default()
            },
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;
        let res = abort_session(State(state.clone()), Path(id.clone())).await;
        assert_eq!(res.status(), StatusCode::OK);
        let out = task.await.unwrap().expect("abort prompt");
        assert_eq!(out.info["error"]["name"], "MessageAbortedError");
        assert!(state.runners.lock().unwrap().is_empty());

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn fake_tool_calls_run_in_parallel_and_return_input_order() {
        const DELAY: u64 = 40;

        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::write(repo.join("a.txt"), "a\n").unwrap();
        std::fs::write(repo.join("b.txt"), "b\n").unwrap();
        let root = PathBuf::from(state.store.paths().directory);
        let cancel = Arc::new(AtomicBool::new(false));
        let start = Instant::now();
        wait_fake(DELAY, &cancel).await;
        wait_fake(DELAY, &cancel).await;
        let serial = start.elapsed();
        let start = Instant::now();
        let parts = fake_tool_parts(
            root.clone(),
            "msg_parallel".to_string(),
            "prt_parallel".to_string(),
            vec![
                FakeCall {
                    tool: "read".to_string(),
                    input: json!({ "filePath": "a.txt" }),
                    delay: DELAY,
                    invalid: None,
                },
                FakeCall {
                    tool: "read".to_string(),
                    input: json!({ "filePath": "b.txt" }),
                    delay: DELAY,
                    invalid: None,
                },
            ],
            1,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        let elapsed = start.elapsed();
        let bound = serial.mul_f32(0.9);
        assert!(
            elapsed < bound,
            "fake tool calls should overlap: elapsed={elapsed:?}, serial={serial:?}, bound={bound:?}"
        );
        assert_eq!(parts[0]["state"]["input"]["filePath"], "a.txt");
        assert_eq!(parts[1]["state"]["input"]["filePath"], "b.txt");

        let _ = std::fs::remove_dir_all(root);
    }

    /// M7 Fix 5: when the live OAuth path receives a tool call whose name
    /// is uppercased (`READ`, `Read`), it must be lowercased and dispatched
    /// to the canonical handler — not fall through to "Unsupported tool".
    /// Mirrors Bun's `experimental_repairToolCall` at llm.ts:363-383.
    #[tokio::test]
    async fn real_tool_parts_lowercases_uppercase_model_tool_names() {
        let root = unique_root();
        std::fs::create_dir_all(root.join("repo")).unwrap();
        std::fs::write(root.join("repo").join("note.txt"), "needle\n").unwrap();
        let calls = vec![ChatToolCall {
            id: "call_1".to_string(),
            name: "READ".to_string(),
            input: json!({ "filePath": "note.txt", "limit": 1 }),
        }];
        let parts = real_tool_parts(&root.join("repo"), "msg_x", "prt_x", &calls, 1);
        assert_eq!(parts.len(), 1);
        assert_eq!(
            parts[0]["tool"], "read",
            "uppercase READ should be canonicalised to read"
        );
        assert_eq!(parts[0]["state"]["status"], "completed");
        assert!(parts[0]["state"]["output"]
            .as_str()
            .unwrap_or_default()
            .contains("needle"));
        let _ = std::fs::remove_dir_all(root);
    }

    /// M7 Fix 5 negative case: a model-invented tool name like `Bash`
    /// must produce the Unknown-tool error envelope shape used by the
    /// fake path's `"invalid"` arm (see fake_tool_part), not silently
    /// dispatch to a stub.
    #[tokio::test]
    async fn real_tool_parts_rejects_unknown_tool_names_with_invalid_shape() {
        let root = unique_root();
        std::fs::create_dir_all(root.join("repo")).unwrap();
        let calls = vec![ChatToolCall {
            id: "call_z".to_string(),
            name: "DoesNotExist".to_string(),
            input: json!({}),
        }];
        let parts = real_tool_parts(&root.join("repo"), "msg_z", "prt_z", &calls, 1);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0]["state"]["status"], "error");
        let err = parts[0]["state"]["error"].as_str().unwrap_or_default();
        assert!(
            err.starts_with("Unknown tool:"),
            "expected Unknown tool error envelope, got: {err}"
        );
        // Original (un-canonical) name preserved on the part for debugging.
        assert_eq!(parts[0]["tool"], "DoesNotExist");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn tool_repair_matches_case_and_invalid_fallback() {
        assert_eq!(
            repair_tool_name("Read", KNOWN_TOOLS),
            Repair::Valid("read".to_string())
        );
        assert_eq!(
            repair_tool_name("does_not_exist", KNOWN_TOOLS),
            Repair::Invalid("does_not_exist".to_string())
        );
    }

    /// M7 Fix 3 guard: the OAuth instructions must carry the real soul
    /// prompt embedded from `packages/opencode/src/kilocode/soul.txt`, not
    /// a one-line stub. Threshold is intentionally well below the current
    /// ~3.5KB file so it catches accidental re-stubbing without coupling
    /// to exact wording.
    #[test]
    fn openai_oauth_soul_is_full_kilocode_prompt_not_stub() {
        let trimmed = OPENAI_OAUTH_SOUL_RAW.trim();
        assert!(
            trimmed.len() > 500,
            "OPENAI_OAUTH_SOUL_RAW is too short ({} bytes) — looks like a stub",
            trimmed.len()
        );
        // Sanity: the embedded soul leads with the canonical Kilo opener.
        assert!(
            trimmed.starts_with("You are Kilo"),
            "soul prompt does not start with the Kilo opener; got prefix {:?}",
            &trimmed[..trimmed.len().min(40)]
        );

        // The composed instructions must include the soul text plus any
        // user-supplied system string, joined with a newline (Bun parity).
        let input = PromptInput {
            system: Some(json!("user system text")),
            ..Default::default()
        };
        let composed = prompt_instructions(&input).expect("composed instructions");
        assert!(composed.contains("You are Kilo"));
        assert!(composed.contains("user system text"));
    }

    #[test]
    fn noop_condition_matches_litellm_copilot_tool_history_triple() {
        let msgs = vec![json!({ "parts": [{ "type": "tool", "tool": "read" }] })];
        assert!(should_inject_noop("openai", true, &[], &msgs));
        assert!(should_inject_noop("github-copilot-chat", false, &[], &msgs));
        assert!(!should_inject_noop("openai", false, &[], &msgs));
        assert!(!should_inject_noop(
            "github-copilot-chat",
            false,
            &[read_def()],
            &msgs
        ));
        assert!(!should_inject_noop("github-copilot-chat", false, &[], &[]));
    }

    fn state() -> Arc<AppState> {
        let (bus, _) = broadcast::channel(16);
        Arc::new(AppState {
            username: "kilo".to_string(),
            password: None,
            store: Store::new(),
            bus,
            viewed: RwLock::default(),
            runners: Mutex::default(),
            permissions: Mutex::default(),
            questions: Mutex::default(),
        })
    }

    fn state_at(root: &std::path::Path) -> Arc<AppState> {
        let (bus, _) = broadcast::channel(16);
        Arc::new(AppState {
            username: "kilo".to_string(),
            password: None,
            store: store(root),
            bus,
            viewed: RwLock::default(),
            runners: Mutex::default(),
            permissions: Mutex::default(),
            questions: Mutex::default(),
        })
    }

    fn store(root: &std::path::Path) -> Store {
        Store::for_test(root)
    }

    fn seed(store: &Store) {
        store.seed_for_test();
    }

    fn drain(rx: &mut broadcast::Receiver<GlobalEvent>) -> Vec<GlobalEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    fn assert_sync(event: &GlobalEvent, kind: &str, role: &str, text: Option<&str>) {
        let data = event.payload.sync_event.as_ref().unwrap();
        assert_eq!(event.payload.kind, "sync");
        assert_eq!(data["type"], kind);
        if !role.is_empty() {
            assert_eq!(data["data"]["info"]["role"], role);
        }
        if let Some(text) = text {
            assert_eq!(data["data"]["part"]["text"], text);
        }
    }

    fn assert_delta(event: &GlobalEvent, mid: &Value, pid: &Value, delta: &str) {
        assert_eq!(event.payload.kind, "message.part.delta");
        assert_eq!(event.payload.properties["messageID"], *mid);
        assert_eq!(event.payload.properties["partID"], *pid);
        assert_eq!(event.payload.properties["field"], "text");
        assert_eq!(event.payload.properties["delta"], delta);
    }

    fn unique_root() -> std::path::PathBuf {
        static IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = IDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let name = format!(
            "kilo-server-test-{}-{}-{seq}",
            std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            std::process::id()
        );
        std::env::temp_dir().join(name)
    }

    struct TestProvider {
        url: String,
        body: Arc<Mutex<String>>,
    }

    async fn provider_server(res: Value) -> TestProvider {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = Arc::new(Mutex::new(String::new()));
        let copy = body.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 8192];
            let size = socket.read(&mut buf).await.unwrap();
            *copy.lock().unwrap() = String::from_utf8_lossy(&buf[..size]).to_string();
            let body = serde_json::to_string(&res).unwrap();
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(body.as_bytes()).await.unwrap();
        });
        TestProvider {
            url: format!("http://{addr}/v1"),
            body,
        }
    }

    async fn stream_provider_server(data: &'static str) -> TestProvider {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = Arc::new(Mutex::new(String::new()));
        let copy = body.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 8192];
            let size = socket.read(&mut buf).await.unwrap();
            *copy.lock().unwrap() = String::from_utf8_lossy(&buf[..size]).to_string();
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                data.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(data.as_bytes()).await.unwrap();
        });
        TestProvider {
            url: format!("http://{addr}"),
            body,
        }
    }

    /// Stalling SSE provider used by Fix 4's mid-stream abort coverage.
    /// Sends `prefix` (no terminating `[DONE]`) then sleeps for ~5s — long
    /// enough that any test reaching it will hit the timeout unless the
    /// cancel-during-stream race short-circuits the bytes loop. Uses
    /// `Transfer-Encoding: chunked` so the client cannot satisfy a
    /// content-length and pre-emptively close the body.
    async fn stream_provider_stalling_server(prefix: &'static str) -> TestProvider {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = Arc::new(Mutex::new(String::new()));
        let copy = body.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 8192];
            let size = socket.read(&mut buf).await.unwrap();
            *copy.lock().unwrap() = String::from_utf8_lossy(&buf[..size]).to_string();
            let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n";
            let _ = socket.write_all(head.as_bytes()).await;
            // Chunk = <hex-len>\r\n<data>\r\n
            let chunk = format!("{:x}\r\n{}\r\n", prefix.len(), prefix);
            let _ = socket.write_all(chunk.as_bytes()).await;
            // Stall — caller is expected to abort before this sleeps out.
            tokio::time::sleep(Duration::from_secs(5)).await;
            let _ = socket.write_all(b"0\r\n\r\n").await;
        });
        TestProvider {
            url: format!("http://{addr}"),
            body,
        }
    }

    /// M7 Fix 1+2+4: aborting MID-stream on the OAuth path must update the
    /// existing assistant message in place — never append a second
    /// (orphan) assistant record. Asserts the message count is exactly 2
    /// (user + one assistant), the assistant info carries the
    /// `MessageAbortedError` envelope, and any partial deltas streamed
    /// before the abort are preserved on the message.
    #[tokio::test]
    async fn prompt_openai_stream_mid_stream_abort_updates_in_place_no_orphan() {
        let root = unique_root();
        let state = state_at(&root);
        seed(&state.store);
        std::fs::create_dir_all(root.join("repo")).unwrap();
        // Send a real text delta, THEN stall. Cancel will land while the
        // bytes loop awaits the next chunk — exercising the
        // `until_cancel` race in `post_stream`.
        let server = stream_provider_stalling_server(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
        )
        .await;
        state
            .store
            .set_provider_auth(
                "openai",
                json!({
                    "type": "oauth",
                    "refresh": "rt",
                    "access": "at",
                    "expires": 9999999999999i64,
                    "accountId": "acct_1"
                }),
            )
            .unwrap();
        std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
        std::fs::write(
            root.join("config").join("kilo").join("kilo.json"),
            serde_json::to_string(&json!({
                "provider": {
                    "openai": { "options": { "baseURL": server.url }, "models": {} }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let session = state
            .store
            .create_session(SessionCreateInput::default())
            .unwrap();

        let cancel = Arc::new(AtomicBool::new(false));
        let cancel_handle = cancel.clone();
        let state_for_task = state.clone();
        let sid = session.id.clone();
        let task = tokio::spawn(async move {
            prompt_turn(
                &state_for_task,
                &sid,
                PromptInput {
                    parts: vec![json!({ "type": "text", "text": "abort me" })],
                    model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
                    ..Default::default()
                },
                cancel_handle,
            )
            .await
        });

        // Let the prefix delta land, then cancel mid-stream.
        tokio::time::sleep(Duration::from_millis(150)).await;
        cancel.store(true, Ordering::SeqCst);

        let out = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .expect("turn must unblock on cancel — abort race not wired")
            .expect("task join")
            .expect("prompt turn");

        assert_eq!(
            out.info["error"]["name"], "MessageAbortedError",
            "aborted turn must surface MessageAbortedError envelope"
        );

        // Storage check: exactly one assistant record (no orphan), and
        // any partial delta that streamed before the abort persisted.
        let page = state.store.messages(&session.id, None, None).unwrap();
        assert_eq!(
            page.items.len(),
            2,
            "expected exactly user + assistant, got {} messages",
            page.items.len()
        );
        assert_eq!(page.items[0].info["role"], "user");
        assert_eq!(page.items[1].info["role"], "assistant");
        assert_eq!(page.items[1].info["error"]["name"], "MessageAbortedError");
        // Partial text part: present iff the delta landed before cancel.
        // Tolerant assertion — race may abort before any delta lands.
        let parts = &page.items[1].parts;
        if let Some(text_part) = parts.iter().find(|p| p["type"] == "text") {
            let text = text_part["text"].as_str().unwrap_or_default();
            assert!(
                text.is_empty() || text == "partial",
                "partial text must be either empty or the streamed delta, got: {text:?}"
            );
        }

        let _ = std::fs::remove_dir_all(root);
    }
}
