use std::{
    collections::{BTreeMap, BTreeSet},
    env, fmt,
    sync::{atomic::AtomicBool, LazyLock},
    time::Duration,
};

use futures_util::StreamExt;
use kilo_protocol::{Config, ConfigProvidersResult, ProviderResult};
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub mod openai_models;
pub mod openai_oauth;
pub mod openai_responses;

pub use openai_oauth::{
    begin_oauth, compute_challenge, exchange_code, generate_state, generate_verifier,
    refresh_token, OAuthBegin, OAuthError, OAuthTokens,
};
pub use openai_responses::{classify_stream, error_envelope, ResponsesStreamPart, CODEX_ENDPOINT};

const OPENAI_BASE: &str = "https://api.openai.com/v1";
const CODEX_BASE: &str = "https://chatgpt.com/backend-api/codex";
static HTTP: LazyLock<Result<reqwest::Client, String>> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .map_err(|err| err.to_string())
});
static AGENT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "opencode/{} ({}/{})",
        env!("CARGO_PKG_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH
    )
});

#[derive(Clone, Debug, PartialEq)]
pub struct ChatRequest {
    pub provider: String,
    pub model: String,
    pub base: String,
    pub auth: ChatAuth,
    pub session_id: Option<String>,
    pub instructions: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub tools: Vec<ChatTool>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChatAuth {
    Api {
        key: String,
    },
    Oauth {
        access: String,
        account: Option<String>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
    #[serde(default, skip_serializing)]
    pub responses: Vec<ChatResponseItem>,
    #[serde(default, skip_serializing)]
    pub attachments: Vec<ChatAttachment>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ReasoningItem {
    pub id: String,
    pub encrypted_content: String,
    /// Summary lines captured from `response.reasoning_summary_text.delta`
    /// events. Optional — Responses input still gets a cache hit on the
    /// `id` + `encrypted_content` pair.
    #[serde(default)]
    pub summary: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ChatAttachment {
    pub mime: String,
    pub url: String,
    pub filename: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum ChatResponseItem {
    FunctionCall(ChatToolCall),
    FunctionOutput {
        id: String,
        output: String,
    },
    /// OpenAI Responses encrypted reasoning item captured from
    /// `response.output_item.done`. Replayed into the next request's
    /// `input[]` as `{type: "reasoning", id, encrypted_content, summary}`
    /// so multi-iteration turns get cache hits on reasoning models.
    Reasoning(ReasoningItem),
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ChatTool {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ChatToolCall {
    pub id: String,
    pub name: String,
    pub input: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChatOutput {
    pub provider: String,
    pub model: String,
    pub text: String,
    pub tool_calls: Vec<ChatToolCall>,
    pub usage: Option<ChatUsage>,
    pub finish: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChatUsage {
    pub input: u64,
    pub output: u64,
    pub total: u64,
    pub reasoning: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChatParsed {
    pub text: String,
    pub tool_calls: Vec<ChatToolCall>,
    pub usage: Option<ChatUsage>,
    pub finish: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiStatusError {
    pub status: u16,
    pub message: String,
    pub body: String,
    pub retry_after_ms: Option<u64>,
    pub retryable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StreamEvent {
    TextDelta(String),
    ReasoningStart {
        id: String,
    },
    ReasoningDelta {
        id: String,
        delta: String,
    },
    ReasoningEnd {
        id: String,
    },
    /// `response.output_item.done` for a `type: "reasoning"` item — carries
    /// the verbatim `encrypted_content` blob the agent loop must echo back
    /// into the next request's `input[]` for cache continuity. Emitted
    /// once per reasoning item, after its trailing `ReasoningEnd` events.
    ReasoningItem {
        id: String,
        encrypted_content: String,
    },
    ToolDelta {
        id: String,
        name: Option<String>,
        arguments: String,
    },
    ToolCall(ChatToolCall),
    Usage(ChatUsage),
    Finish(String),
    Error(String),
}

impl ChatRequest {
    pub fn is_openai_oauth(&self) -> bool {
        self.provider == "openai" && matches!(self.auth, ChatAuth::Oauth { .. })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProviderError {
    MissingProvider,
    MissingModel {
        provider: String,
    },
    MissingBase {
        provider: String,
    },
    MissingKey {
        provider: String,
    },
    Http(String),
    Api(String),
    ApiStatus(ApiStatusError),
    Response(String),
    /// The cancel flag was tripped while a request was in-flight (during
    /// HTTP connect/headers or while awaiting a stream chunk). Surfaced by
    /// [`post_stream`] when the `until_cancel` race wins. kilo-server maps
    /// this to the `MessageAbortedError` envelope via `aborted_error()`.
    Aborted,
    /// The model rejected the input as too long (Bun parity: triggers
    /// compaction in `prompt.ts:1654-1675`). Detected at HTTP-status time
    /// by [`api_is_context_window`] from upstream error bodies. The agent
    /// loop catches this, summarizes the session, and retries.
    ContextWindow(String),
}

impl fmt::Display for ProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingProvider => write!(f, "missing providerID"),
            Self::MissingModel { provider } => write!(f, "missing modelID for provider {provider}"),
            Self::MissingBase { provider } => write!(f, "missing baseURL for provider {provider}"),
            Self::MissingKey { provider } => write!(f, "missing apiKey for provider {provider}"),
            Self::Http(err) => write!(f, "provider HTTP error: {err}"),
            Self::Api(err) => write!(f, "provider API error: {err}"),
            Self::ApiStatus(err) => write!(f, "provider API error: {}", err.message),
            Self::ContextWindow(err) => write!(f, "provider context window exceeded: {err}"),
            Self::Response(err) => write!(f, "provider response error: {err}"),
            Self::Aborted => write!(f, "provider request aborted"),
        }
    }
}

impl std::error::Error for ProviderError {}

impl ProviderError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::ApiStatus(err) => err.retryable,
            Self::Http(_) => true,
            _ => false,
        }
    }

    pub fn status_code(&self) -> Option<u16> {
        match self {
            Self::ApiStatus(err) => Some(err.status),
            _ => None,
        }
    }

    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            Self::ApiStatus(err) => err.retry_after_ms,
            _ => None,
        }
    }

    pub fn response_body(&self) -> Option<&str> {
        match self {
            Self::ApiStatus(err) => Some(err.body.as_str()),
            _ => None,
        }
    }
}

pub async fn chat(
    cfg: &Config,
    model: Option<&Value>,
    messages: Vec<ChatMessage>,
) -> Result<ChatOutput, ProviderError> {
    chat_tools(cfg, model, messages, Vec::new()).await
}

pub async fn chat_tools(
    cfg: &Config,
    model: Option<&Value>,
    messages: Vec<ChatMessage>,
    tools: Vec<ChatTool>,
) -> Result<ChatOutput, ProviderError> {
    chat_tools_with_auth(cfg, &Value::Null, model, None, messages, tools).await
}

pub async fn chat_tools_with_auth(
    cfg: &Config,
    auths: &Value,
    model: Option<&Value>,
    instructions: Option<String>,
    messages: Vec<ChatMessage>,
    tools: Vec<ChatTool>,
) -> Result<ChatOutput, ProviderError> {
    let req = resolve_tools_with_auth(cfg, auths, model, instructions, messages, tools)?;
    let out = post(&req).await?;
    Ok(ChatOutput {
        provider: req.provider,
        model: req.model,
        text: out.text,
        tool_calls: out.tool_calls,
        usage: out.usage,
        finish: out.finish,
    })
}

/// Cancellable variant of [`chat_tools_with_auth`]. Used by the agent
/// compaction path so a Stop press during a context-window summarize
/// returns `ProviderError::Aborted` immediately instead of waiting for
/// the upstream call to settle. Mirrors the `until_cancel` race already
/// in `post_stream`.
pub async fn chat_tools_with_auth_cancel(
    cfg: &Config,
    auths: &Value,
    model: Option<&Value>,
    instructions: Option<String>,
    messages: Vec<ChatMessage>,
    tools: Vec<ChatTool>,
    cancel: &AtomicBool,
) -> Result<ChatOutput, ProviderError> {
    let req = resolve_tools_with_auth(cfg, auths, model, instructions, messages, tools)?;
    let out = post_cancel(&req, cancel).await?;
    Ok(ChatOutput {
        provider: req.provider,
        model: req.model,
        text: out.text,
        tool_calls: out.tool_calls,
        usage: out.usage,
        finish: out.finish,
    })
}

/// Streaming entry point for the OpenAI OAuth (Codex Responses API) path.
/// Public alias of [`stream_openai_oauth`] — the plan calls this name so
/// new callers should prefer it; the longer name is kept for the
/// existing kilo-server call site.
pub async fn responses_stream(
    cfg: &Config,
    auths: &Value,
    model: Option<&Value>,
    instructions: Option<String>,
    messages: Vec<ChatMessage>,
    tools: Vec<ChatTool>,
    cancel: &AtomicBool,
    emit: impl FnMut(StreamEvent),
) -> Result<ChatOutput, ProviderError> {
    stream_openai_oauth(
        cfg,
        auths,
        model,
        None,
        instructions,
        messages,
        tools,
        cancel,
        emit,
    )
    .await
}

pub async fn stream_openai_oauth(
    cfg: &Config,
    auths: &Value,
    model: Option<&Value>,
    session_id: Option<&str>,
    instructions: Option<String>,
    messages: Vec<ChatMessage>,
    tools: Vec<ChatTool>,
    cancel: &AtomicBool,
    mut emit: impl FnMut(StreamEvent),
) -> Result<ChatOutput, ProviderError> {
    let mut req = resolve_tools_with_auth(cfg, auths, model, instructions, messages, tools)?;
    req.session_id = session_id.map(str::to_string);
    if !req.is_openai_oauth() {
        return Err(ProviderError::MissingKey {
            provider: req.provider,
        });
    }
    let out = post_stream(&req, cancel, &mut emit).await?;
    Ok(ChatOutput {
        provider: req.provider,
        model: req.model,
        text: out.text,
        tool_calls: out.tool_calls,
        usage: out.usage,
        finish: out.finish,
    })
}

pub fn resolve(
    cfg: &Config,
    model: Option<&Value>,
    messages: Vec<ChatMessage>,
) -> Result<ChatRequest, ProviderError> {
    resolve_tools(cfg, model, None, messages, Vec::new())
}

pub fn resolve_tools(
    cfg: &Config,
    model: Option<&Value>,
    instructions: Option<String>,
    messages: Vec<ChatMessage>,
    tools: Vec<ChatTool>,
) -> Result<ChatRequest, ProviderError> {
    resolve_tools_with_auth(cfg, &Value::Null, model, instructions, messages, tools)
}

pub fn resolve_tools_with_auth(
    cfg: &Config,
    auths: &Value,
    model: Option<&Value>,
    instructions: Option<String>,
    messages: Vec<ChatMessage>,
    tools: Vec<ChatTool>,
) -> Result<ChatRequest, ProviderError> {
    let provider = text(model, &["providerID", "provider", "providerId"])
        .map(str::to_string)
        .ok_or(ProviderError::MissingProvider)?;
    let model = text(model, &["modelID", "model", "modelId", "id"])
        .map(str::to_string)
        .ok_or_else(|| ProviderError::MissingModel {
            provider: provider.clone(),
        })?;
    // Audit Fix 6: auth.json wins over `cfg.options.apiKey`. Bun's
    // precedence is "persisted auth blob first, then config-only api key,
    // then env". The previous chain checked `provider_option(...,
    // "apiKey")` first, which let a stale or low-priority config-supplied
    // api key shadow an OAuth token in `auths` and silently demote the
    // OpenAI request from Codex back to the api.openai.com chat path.
    let auth = auth_value(auths, &provider)
        .or_else(|| provider_option(cfg, &provider, "apiKey").map(|key| ChatAuth::Api { key }))
        .or_else(|| env_auth(&provider))
        .or_else(|| {
            first([
                env_value(&format!("{}_API_KEY", env_name(&provider))),
                env_value("OPENAI_API_KEY"),
            ])
            .map(|key| ChatAuth::Api { key })
        });
    let auth = auth.ok_or_else(|| ProviderError::MissingKey {
        provider: provider.clone(),
    })?;
    let base = first([
        provider_option(cfg, &provider, "baseURL"),
        match (&auth, provider.as_str()) {
            (ChatAuth::Oauth { .. }, "openai") => Some(CODEX_BASE.to_string()),
            _ => None,
        },
        model_url(cfg, &provider, &model),
        env_value(&format!("{}_BASE_URL", env_name(&provider))),
        env_value("OPENAI_BASE_URL"),
        match (&auth, provider.as_str()) {
            (_, "openai") => Some(OPENAI_BASE.to_string()),
            _ => None,
        },
    ])
    .ok_or_else(|| ProviderError::MissingBase {
        provider: provider.clone(),
    })?;

    Ok(ChatRequest {
        provider,
        model,
        base,
        auth,
        session_id: None,
        instructions,
        messages,
        tools,
    })
}

pub fn parse(body: &Value) -> Result<ChatParsed, ProviderError> {
    let calls = parse_response_tool_calls(body);
    if let Some(text) = parse_response_text(body) {
        return Ok(ChatParsed {
            text,
            tool_calls: calls,
            usage: parse_usage(body),
            finish: parse_finish(body),
        });
    }
    if !calls.is_empty() {
        return Ok(ChatParsed {
            text: String::new(),
            tool_calls: calls,
            usage: parse_usage(body),
            finish: parse_finish(body),
        });
    }

    let msg = body
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("message"))
        .ok_or_else(|| ProviderError::Response("missing response text".to_string()))?;
    let text = message_text(msg);
    let calls = parse_tool_calls(msg);
    if text.is_empty() && calls.is_empty() {
        return Err(ProviderError::Response("missing response text".to_string()));
    }

    Ok(ChatParsed {
        text,
        tool_calls: calls,
        usage: parse_usage(body),
        finish: parse_finish(body),
    })
}

pub fn list(cfg: &Config) -> ProviderResult {
    list_with_auth(cfg, &Value::Null)
}

/// `GET /provider` enriched with the persisted auth blob. Audit Fix 5:
/// when openai's auth indicates `oauth`, run the OpenAI model list through
/// [`openai_models::filter_codex_models`] so models outside the Codex
/// allow-list are dropped and per-token cost is zeroed (the ChatGPT
/// subscription covers it). Bun's source of truth is
/// [`packages/opencode/src/plugin/codex.ts:373-400`](../../../../../opencode/src/plugin/codex.ts:373).
/// API-key auth bypasses the filter entirely.
pub fn list_with_auth(cfg: &Config, auths: &Value) -> ProviderResult {
    let mut all = vec![openai_provider("openai")];

    if openai_auth_kind(auths) == Some(AuthKind::Oauth) {
        for item in all.iter_mut() {
            if item.get("id").and_then(Value::as_str) != Some("openai") {
                continue;
            }
            if let Some(models) = item.get_mut("models").and_then(Value::as_object_mut) {
                openai_models::filter_codex_models(models);
            }
        }
    }

    let defaults = all
        .iter()
        .filter_map(|item| {
            let id = item.get("id")?.as_str()?;
            let models = item.get("models")?.as_object()?;
            let first = models.keys().next()?.to_string();
            Some((id.to_string(), first))
        })
        .collect();
    let connected = all
        .iter()
        .filter_map(|item| {
            let id = item.get("id")?.as_str()?;
            connected(cfg, auths, id).then(|| id.to_string())
        })
        .collect();

    ProviderResult {
        all,
        defaults,
        connected,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum AuthKind {
    Oauth,
    Api,
}

fn openai_auth_kind(auths: &Value) -> Option<AuthKind> {
    match auths.get("openai")?.get("type")?.as_str()? {
        "oauth" => Some(AuthKind::Oauth),
        "api" => Some(AuthKind::Api),
        _ => None,
    }
}

fn connected(cfg: &Config, auths: &Value, id: &str) -> bool {
    if auths.get(id).and_then(|value| value.get("type")).is_some() {
        return true;
    }
    provider_option(cfg, id, "apiKey").is_some() || env_auth(id).is_some()
}

/// `GET /provider/{providerID}`. Returns the per-provider record from the
/// list — same shape as one entry of `list().all`. Used by the sign-in
/// flow in [`provider-actions.ts`](../../kilo-vscode/src/provider-actions.ts)
/// to fetch env/option metadata before launching auth. Returns `None` if
/// the provider is unknown so the route can map it to 404.
pub fn detail(cfg: &Config, id: &str) -> Option<Value> {
    list(cfg)
        .all
        .into_iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(id))
}

pub fn config(cfg: &Config) -> ConfigProvidersResult {
    config_with_auth(cfg, &Value::Null)
}

/// Audit Fix 5: same OAuth-aware filtering as [`list_with_auth`], for the
/// `GET /config/providers` shape. Used by the sidebar to render the model
/// picker after sign-in.
pub fn config_with_auth(cfg: &Config, auths: &Value) -> ConfigProvidersResult {
    let data = list_with_auth(cfg, auths);
    ConfigProvidersResult {
        providers: data.all,
        defaults: data.defaults,
    }
}

fn openai_provider(npm: &str) -> Value {
    json!({
        "id": "openai",
        "name": "OpenAI",
        "source": "custom",
        "env": ["OPENAI_API_KEY"],
        "options": {},
        "models": openai_models::registry(npm)
    })
}

async fn post(req: &ChatRequest) -> Result<ChatParsed, ProviderError> {
    let client = http()?;
    let res = match &req.auth {
        ChatAuth::Oauth { access, account } if req.provider == "openai" => {
            let url = format!("{}/responses", req.base.trim_end_matches('/'));
            let mut call = client.post(url).bearer_auth(access).json(&json!({
                "model": req.model,
                "instructions": req.instructions,
                "input": responses_input(&req.messages),
                "stream": false,
                "store": false,
            }));
            call = openai_oauth_headers(call, req.session_id.as_deref());
            if let Some(account) = account {
                call = call.header("ChatGPT-Account-Id", account);
            }
            call.send().await
        }
        ChatAuth::Oauth { .. } => {
            return Err(ProviderError::MissingKey {
                provider: req.provider.clone(),
            })
        }
        ChatAuth::Api { key } => {
            let url = format!("{}/chat/completions", req.base.trim_end_matches('/'));
            let mut body = json!({
                "model": req.model,
                "messages": req.messages,
                "stream": false,
            });
            if !req.tools.is_empty() {
                body["tools"] = json!(openai_tools(&req.tools));
                body["tool_choice"] = json!(tool_choice(&req.tools));
            }
            client.post(url).bearer_auth(key).json(&body).send().await
        }
    }
    .map_err(|err| ProviderError::Http(err.to_string()))?;
    let status = res.status();
    let headers = res.headers().clone();
    let raw = res
        .text()
        .await
        .map_err(|err| ProviderError::Response(err.to_string()))?;
    let body = serde_json::from_str::<Value>(&raw)
        .map_err(|err| ProviderError::Response(err.to_string()))?;

    if !status.is_success() {
        return Err(api_status_error(status.as_u16(), &headers, &raw));
    }

    parse(&body)
}

/// Detect "model context window exceeded" responses across the provider
/// shapes we currently call. OpenAI Responses API surfaces this as
/// `error.code = "context_length_exceeded"` (or
/// `error.type = "invalid_request_error"` with a message containing
/// `"context length"` / `"too long"` / `"maximum context"`). Bun does
/// the same matching in `provider/error.ts`. Status is usually 400 but
/// some gateways return 413; we accept both.
fn api_is_context_window(status: u16, raw: &str) -> bool {
    if status != 400 && status != 413 && status != 422 {
        return false;
    }
    if let Ok(body) = serde_json::from_str::<Value>(raw) {
        let code = body.pointer("/error/code").and_then(Value::as_str);
        if matches!(
            code,
            Some("context_length_exceeded")
                | Some("string_above_max_length")
                | Some("max_tokens_exceeded"),
        ) {
            return true;
        }
        let message = body
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or("");
        let lower = message.to_ascii_lowercase();
        return lower.contains("context length")
            || lower.contains("context window")
            || lower.contains("maximum context")
            || lower.contains("input is too long")
            || lower.contains("token limit");
    }
    let lower = raw.to_ascii_lowercase();
    lower.contains("context_length_exceeded")
        || lower.contains("context length")
        || lower.contains("context window")
        || lower.contains("token limit")
}

/// Polling helper used by [`post_stream`] / [`post_cancel`] to race
/// against cancellation. Resolves only when `cancel` is set. We poll on
/// a short interval rather than swap to
/// `tokio_util::sync::CancellationToken` because the rest of
/// kilo-server runs on `Arc<AtomicBool>` (~50 sites); converting now
/// would be a coordinated change that's out of scope for the M7 OAuth
/// fix. The 10ms cadence matches the existing `wait_fake` helper in
/// kilo-server.
async fn until_cancel(cancel: &AtomicBool) {
    while !cancel.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Cancellable mirror of [`post`]. Race the upstream send + body read
/// against `cancel` so an abort during compaction or any other
/// non-streaming call returns `ProviderError::Aborted` promptly. Body
/// shape is identical to `post` once the futures settle — only the
/// outer `tokio::select!` is added.
async fn post_cancel(req: &ChatRequest, cancel: &AtomicBool) -> Result<ChatParsed, ProviderError> {
    let client = http()?;
    let call = match &req.auth {
        ChatAuth::Oauth { access, account } if req.provider == "openai" => {
            let url = format!("{}/responses", req.base.trim_end_matches('/'));
            let mut call = client.post(url).bearer_auth(access).json(&json!({
                "model": req.model,
                "instructions": req.instructions,
                "input": responses_input(&req.messages),
                "stream": false,
                "store": false,
            }));
            call = openai_oauth_headers(call, req.session_id.as_deref());
            if let Some(account) = account {
                call = call.header("ChatGPT-Account-Id", account);
            }
            call
        }
        ChatAuth::Oauth { .. } => {
            return Err(ProviderError::MissingKey {
                provider: req.provider.clone(),
            })
        }
        ChatAuth::Api { key } => {
            let url = format!("{}/chat/completions", req.base.trim_end_matches('/'));
            let mut body = json!({
                "model": req.model,
                "messages": req.messages,
                "stream": false,
            });
            if !req.tools.is_empty() {
                body["tools"] = json!(openai_tools(&req.tools));
                body["tool_choice"] = json!(tool_choice(&req.tools));
            }
            client.post(url).bearer_auth(key).json(&body)
        }
    };
    let res = tokio::select! {
        _ = until_cancel(cancel) => return Err(ProviderError::Aborted),
        res = call.send() => res.map_err(|err| ProviderError::Http(err.to_string()))?,
    };
    let status = res.status();
    let headers = res.headers().clone();
    let body = res.text();
    let raw = tokio::select! {
        _ = until_cancel(cancel) => return Err(ProviderError::Aborted),
        body = body => body.map_err(|err| ProviderError::Response(err.to_string()))?,
    };
    let body = serde_json::from_str::<Value>(&raw)
        .map_err(|err| ProviderError::Response(err.to_string()))?;

    if !status.is_success() {
        return Err(api_status_error(status.as_u16(), &headers, &raw));
    }

    parse(&body)
}

async fn post_stream(
    req: &ChatRequest,
    cancel: &AtomicBool,
    emit: &mut impl FnMut(StreamEvent),
) -> Result<ChatParsed, ProviderError> {
    let client = http()?;
    let ChatAuth::Oauth { access, account } = &req.auth else {
        return Err(ProviderError::MissingKey {
            provider: req.provider.clone(),
        });
    };
    let url = format!("{}/responses", req.base.trim_end_matches('/'));
    let mut body = json!({
        "model": req.model,
        "instructions": req.instructions,
        "input": responses_input(&req.messages),
        "stream": true,
        "store": false,
    });
    if !req.tools.is_empty() {
        body["tools"] = json!(responses_tools(&req.tools));
        body["tool_choice"] = json!(tool_choice(&req.tools));
    }
    let mut call = client.post(url).bearer_auth(access).json(&body);
    call = openai_oauth_headers(call, req.session_id.as_deref());
    if let Some(account) = account {
        call = call.header("ChatGPT-Account-Id", account);
    }
    // M7 Fix 4 (a): cancel must interrupt the HTTP connect/headers wait,
    // not just the post-headers byte loop. Race the send against
    // `until_cancel` so an abort during DNS/TLS/headers returns Aborted
    // immediately instead of blocking until the upstream replies.
    let res = tokio::select! {
        _ = until_cancel(cancel) => return Err(ProviderError::Aborted),
        res = call.send() => res.map_err(|err| ProviderError::Http(err.to_string()))?,
    };
    let status = res.status();
    if !status.is_success() {
        let headers = res.headers().clone();
        let raw = res.text().await.unwrap_or_default();
        return Err(api_status_error(status.as_u16(), &headers, &raw));
    }

    let mut state = StreamState::default();
    let mut buf = String::new();
    // Audit Fix 1: Codex chunks may split a multi-byte UTF-8 codepoint
    // across two `bytes_stream()` items. Eagerly calling
    // `std::str::from_utf8(&chunk)` panicked the turn whenever a 3- or 4-byte
    // sequence (em-dash, emoji, etc.) landed across a chunk boundary. We
    // accumulate raw bytes in `pending` and peel off only the longest
    // valid UTF-8 prefix per iteration via [`decode_utf8_lossy_streaming`],
    // retaining trailing partial bytes for the next chunk.
    let mut pending: Vec<u8> = Vec::new();
    let mut bytes = res.bytes_stream();
    loop {
        // M7 Fix 4 (b): cancel can land any time, including while awaiting
        // the next stream chunk. Race the per-chunk await so an abort
        // unblocks immediately rather than waiting for the next byte from
        // upstream. `bytes.next()` returns `None` to signal end-of-stream;
        // we map cancellation to the same shape so the function returns
        // the partial state to the caller (matching the historical
        // "break the loop on cancel" semantics).
        let item = tokio::select! {
            _ = until_cancel(cancel) => None,
            item = bytes.next() => item,
        };
        let Some(item) = item else { break };
        if cancel.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        let chunk = item.map_err(|err| ProviderError::Http(err.to_string()))?;
        pending.extend_from_slice(&chunk);
        let decoded = decode_utf8_streaming(&mut pending)
            .map_err(|err| ProviderError::Response(err.to_string()))?;
        buf.push_str(&decoded);
        while let Some(event) = next_sse(&mut buf) {
            for out in parse_stream_event(&event, &mut state)? {
                let err = match &out {
                    StreamEvent::Error(err) => Some(err.clone()),
                    _ => None,
                };
                emit(out);
                if let Some(err) = err {
                    return Err(ProviderError::Api(err));
                }
            }
        }
    }
    // Audit Fix 1: at end-of-stream, any bytes still in `pending` must be a
    // truncated trailing codepoint (the connection died mid-codepoint). Drop
    // them rather than panicking, but only after we've drained `buf`.
    if !buf.trim().is_empty() {
        for out in parse_stream_event(&buf, &mut state)? {
            let err = match &out {
                StreamEvent::Error(err) => Some(err.clone()),
                _ => None,
            };
            emit(out);
            if let Some(err) = err {
                return Err(ProviderError::Api(err));
            }
        }
    }

    Ok(ChatParsed {
        text: state.text,
        tool_calls: state.calls,
        usage: state.usage,
        finish: state.finish,
    })
}

fn http() -> Result<reqwest::Client, ProviderError> {
    match &*HTTP {
        Ok(client) => Ok(client.clone()),
        Err(err) => Err(ProviderError::Http(err.clone())),
    }
}

fn openai_user_agent() -> &'static str {
    &AGENT
}

fn openai_oauth_headers(
    call: reqwest::RequestBuilder,
    session: Option<&str>,
) -> reqwest::RequestBuilder {
    let call = call
        .header("originator", "opencode")
        .header("User-Agent", openai_user_agent());
    if let Some(session) = session {
        return call.header("session_id", session);
    }
    call
}

fn openai_tools(tools: &[ChatTool]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.parameters,
                }
            })
        })
        .collect()
}

fn responses_tools(tools: &[ChatTool]) -> Vec<Value> {
    tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters,
            })
        })
        .collect()
}

fn tool_choice(tools: &[ChatTool]) -> &'static str {
    if tools.iter().any(|tool| tool.name == "StructuredOutput") {
        return "required";
    }
    "auto"
}

fn responses_input(messages: &[ChatMessage]) -> Vec<Value> {
    let outputs = response_outputs(messages);
    let cap = messages.iter().fold(0, |sum, msg| {
        sum + msg.responses.len()
            + if !msg.content.is_empty() || !msg.attachments.is_empty() || msg.responses.is_empty()
            {
                1
            } else {
                0
            }
    });
    let mut out = Vec::with_capacity(cap);
    for msg in messages {
        // Replay encrypted reasoning items at the head of the assistant
        // turn (before any text/tool content) so the Responses API can
        // attach cache continuity to the prior trace. Bun emits these as
        // first-class `{type: "reasoning", id, encrypted_content}` input
        // items in the same order the model produced them. Carried via
        // `ChatResponseItem::Reasoning` entries on the assistant message.
        for item in &msg.responses {
            if let ChatResponseItem::Reasoning(reasoning) = item {
                out.push(reasoning_input_item(reasoning));
            }
        }
        if !msg.content.is_empty() || !msg.attachments.is_empty() || msg.responses.is_empty() {
            out.push(json!({
                "role": msg.role,
                "content": responses_content(msg),
            }));
        }
        for item in &msg.responses {
            match item {
                ChatResponseItem::FunctionCall(call) => {
                    out.push(responses_item(item));
                    match outputs.get(call.id.as_str()) {
                        Some(output) => out.push(output_item(&call.id, output)),
                        None => out.push(missing_output_item(&call.id)),
                    }
                }
                ChatResponseItem::FunctionOutput { .. } => {}
                ChatResponseItem::Reasoning(_) => {}
            }
        }
    }
    out
}

fn reasoning_input_item(item: &ReasoningItem) -> Value {
    let summary = item
        .summary
        .iter()
        .filter(|text| !text.is_empty())
        .map(|text| {
            json!({
                "type": "summary_text",
                "text": text,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "type": "reasoning",
        "id": item.id,
        "encrypted_content": item.encrypted_content,
        "summary": summary,
    })
}

fn responses_content(msg: &ChatMessage) -> Vec<Value> {
    let mut content = Vec::with_capacity(1 + msg.attachments.len());
    if !msg.content.is_empty() || msg.attachments.is_empty() {
        content.push(json!({
            "type": if msg.role == "assistant" { "output_text" } else { "input_text" },
            "text": msg.content,
        }));
    }
    if msg.role != "user" {
        return content;
    }
    for item in &msg.attachments {
        if item.mime.starts_with("image/") {
            content.push(json!({
                "type": "input_image",
                "image_url": item.url,
            }));
        } else if item.url.starts_with("data:") {
            let filename = item
                .filename
                .clone()
                .unwrap_or_else(|| "attachment".to_string());
            content.push(json!({
                "type": "input_file",
                "filename": filename,
                "file_data": item.url,
            }));
        }
    }
    content
}

fn response_outputs<'a>(messages: &'a [ChatMessage]) -> BTreeMap<&'a str, &'a str> {
    let mut ids = BTreeMap::new();
    for msg in messages {
        for item in &msg.responses {
            if let ChatResponseItem::FunctionOutput { id, output } = item {
                ids.insert(id.as_str(), output.as_str());
            }
        }
    }
    ids
}

fn responses_item(item: &ChatResponseItem) -> Value {
    match item {
        ChatResponseItem::FunctionCall(call) => json!({
            "type": "function_call",
            "call_id": call.id,
            "name": call.name,
            "arguments": serde_json::to_string(&call.input).unwrap_or_else(|_| "{}".to_string()),
        }),
        ChatResponseItem::FunctionOutput { id, output } => output_item(id, output),
        ChatResponseItem::Reasoning(reasoning) => reasoning_input_item(reasoning),
    }
}

fn output_item(id: &str, output: &str) -> Value {
    json!({
        "type": "function_call_output",
        "call_id": id,
        "output": output,
    })
}

fn missing_output_item(id: &str) -> Value {
    output_item(id, "Tool call did not return an output")
}

fn parse_response_text(body: &Value) -> Option<String> {
    let mut text = String::new();
    for item in body.get("output")?.as_array()? {
        let Some(items) = item.get("content").and_then(Value::as_array) else {
            continue;
        };
        for item in items {
            if let Some(value) = item.get("text").and_then(Value::as_str) {
                text.push_str(value);
            }
        }
    }

    (!text.is_empty()).then_some(text)
}

#[derive(Default)]
struct StreamState {
    text: String,
    tools: Vec<PartialTool>,
    reasoning: BTreeMap<usize, PartialReasoning>,
    current_reasoning: Option<usize>,
    calls: Vec<ChatToolCall>,
    ids: BTreeSet<String>,
    usage: Option<ChatUsage>,
    finish: Option<String>,
}

#[derive(Default)]
struct PartialTool {
    id: String,
    name: Option<String>,
    args: String,
    done: bool,
}

#[derive(Default)]
struct PartialReasoning {
    id: String,
    summaries: BTreeSet<u64>,
}

pub fn parse_stream(input: &str) -> Result<Vec<StreamEvent>, ProviderError> {
    let mut state = StreamState::default();
    let mut out = Vec::new();
    for event in input.split("\n\n") {
        out.extend(parse_stream_event(event, &mut state)?);
    }
    Ok(out)
}

/// Audit Fix 1: streaming UTF-8 decoder. Drains the longest valid UTF-8
/// prefix from `pending`, leaving trailing partial-codepoint bytes (1-3
/// bytes) in `pending` for the next chunk to complete. Returns
/// `Err(string)` on a hard decode error (an invalid byte sequence that
/// isn't merely a truncated trailing codepoint).
///
/// Called per-chunk in [`post_stream`]. Codex chunks routinely split a
/// 3-byte codepoint (e.g. em-dash `e2 80 94`, U+2014) across two
/// `bytes_stream()` items; the prior eager `from_utf8(&chunk)` call site
/// panicked the turn the moment that happened.
fn decode_utf8_streaming(pending: &mut Vec<u8>) -> Result<String, String> {
    match std::str::from_utf8(pending) {
        Ok(s) => {
            let out = s.to_string();
            pending.clear();
            Ok(out)
        }
        Err(err) => {
            let valid_up_to = err.valid_up_to();
            // If there's an explicit error_len, the bytes from
            // valid_up_to..valid_up_to+error_len are genuinely invalid —
            // not just a truncated trailing codepoint. That's a hard error.
            if err.error_len().is_some() {
                return Err(format!(
                    "invalid UTF-8 in stream at byte {valid_up_to}: {err}"
                ));
            }
            // Trailing partial codepoint: peel off the valid prefix and
            // retain the trailing 1-3 bytes for the next chunk.
            let valid_bytes = pending[..valid_up_to].to_vec();
            let trailing = pending.split_off(valid_up_to);
            *pending = trailing;
            // valid_bytes is, by construction, a valid UTF-8 sequence.
            Ok(String::from_utf8(valid_bytes).expect("valid_up_to is utf-8 boundary"))
        }
    }
}

#[cfg(test)]
fn drain_sse(buf: &mut String) -> Vec<String> {
    let mut out = Vec::new();
    while let Some(event) = next_sse(buf) {
        out.push(event);
    }
    out
}

fn next_sse(buf: &mut String) -> Option<String> {
    while let Some(idx) = sse_break(buf) {
        let tail = if buf[idx..].starts_with("\r\n\r\n") {
            4
        } else {
            2
        };
        let event = buf[..idx].to_string();
        buf.drain(..idx + tail);
        return Some(event);
    }
    None
}

fn sse_break(text: &str) -> Option<usize> {
    text.find("\n\n").or_else(|| text.find("\r\n\r\n"))
}

fn parse_stream_event(
    raw: &str,
    state: &mut StreamState,
) -> Result<Vec<StreamEvent>, ProviderError> {
    let mut data = String::new();
    for line in raw.lines().filter_map(|line| line.strip_prefix("data:")) {
        if !data.is_empty() {
            data.push('\n');
        }
        data.push_str(line.trim_start());
    }
    if data.is_empty() || data == "[DONE]" {
        return Ok(Vec::new());
    }
    let value: Value =
        serde_json::from_str(&data).map_err(|err| ProviderError::Response(err.to_string()))?;
    let mut out = Vec::new();
    if let Some(err) = stream_error(&value) {
        out.push(StreamEvent::Error(err));
        return Ok(out);
    }
    if let Some(delta) = stream_text(&value) {
        state.text.push_str(&delta);
        out.push(StreamEvent::TextDelta(delta));
    }
    out.extend(stream_reasoning(&value, state));
    out.extend(stream_tool(&value, state));
    if let Some(usage) = parse_usage(&value) {
        state.usage = Some(usage.clone());
        out.push(StreamEvent::Usage(usage));
    }
    if let Some(done) = parse_finish(&value).or_else(|| stream_finish(&value)) {
        state.finish = Some(done.clone());
        out.push(StreamEvent::Finish(done));
    }
    Ok(out)
}

fn stream_reasoning(value: &Value, state: &mut StreamState) -> Vec<StreamEvent> {
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if kind == "response.output_item.added" {
        let item = value.get("item").or_else(|| value.get("output"));
        let Some(item) =
            item.filter(|item| item.get("type").and_then(Value::as_str) == Some("reasoning"))
        else {
            return Vec::new();
        };
        let idx = response_output_index(value, state);
        let id = item
            .get("id")
            .or_else(|| value.get("item_id"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("reasoning_{idx}"));
        let part = state.reasoning.entry(idx).or_default();
        part.id = id;
        state.current_reasoning = Some(idx);
        if part.summaries.insert(0) {
            return vec![StreamEvent::ReasoningStart {
                id: reasoning_id(&part.id, 0),
            }];
        }
        return Vec::new();
    }

    if kind == "response.reasoning_summary_part.added"
        || kind == "response.reasoning_summary_text.delta"
    {
        let idx = state
            .current_reasoning
            .unwrap_or_else(|| response_output_index(value, state));
        let item = value
            .get("item_id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| {
                state
                    .reasoning
                    .get(&idx)
                    .map(|item| item.id.clone())
                    .unwrap_or_else(|| format!("reasoning_{idx}"))
            });
        let summary = value
            .get("summary_index")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let part = state.reasoning.entry(idx).or_default();
        if part.id.is_empty() {
            part.id = item;
        }
        let id = reasoning_id(&part.id, summary);
        let mut out = Vec::new();
        if part.summaries.insert(summary) {
            out.push(StreamEvent::ReasoningStart { id: id.clone() });
        }
        if kind.ends_with(".delta") {
            if let Some(delta) = value.get("delta").and_then(Value::as_str) {
                out.push(StreamEvent::ReasoningDelta {
                    id,
                    delta: delta.to_string(),
                });
            }
        }
        return out;
    }

    if kind == "response.output_item.done" {
        let item = value.get("item").or_else(|| value.get("output"));
        let Some(item) =
            item.filter(|item| item.get("type").and_then(Value::as_str) == Some("reasoning"))
        else {
            return Vec::new();
        };
        let idx = response_output_index(value, state);
        let Some(part) = state.reasoning.remove(&idx) else {
            return Vec::new();
        };
        if state.current_reasoning == Some(idx) {
            state.current_reasoning = None;
        }
        let mut out: Vec<StreamEvent> = part
            .summaries
            .into_iter()
            .map(|summary| StreamEvent::ReasoningEnd {
                id: reasoning_id(&part.id, summary),
            })
            .collect();
        // Encrypted reasoning round-trip: echo `{id, encrypted_content}`
        // back into the next Responses request's `input[]` for cache
        // hits. Only emit when both fields are present — non-reasoning
        // models and partial frames must stay no-op.
        let encrypted = item
            .get("encrypted_content")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty());
        if let Some(encrypted) = encrypted {
            if !part.id.is_empty() {
                out.push(StreamEvent::ReasoningItem {
                    id: part.id.clone(),
                    encrypted_content: encrypted.to_string(),
                });
            }
        }
        return out;
    }

    Vec::new()
}

fn response_output_index(value: &Value, state: &StreamState) -> usize {
    value
        .get("output_index")
        .or_else(|| value.get("item_index"))
        .and_then(Value::as_u64)
        .or(state.current_reasoning.map(|idx| idx as u64))
        .unwrap_or(state.reasoning.len() as u64) as usize
}

fn reasoning_id(item: &str, summary: u64) -> String {
    format!("{item}:{summary}")
}

fn stream_error(value: &Value) -> Option<String> {
    value.get("error").map(api_message).or_else(|| {
        (value.get("type").and_then(Value::as_str) == Some("error")).then(|| api_message(value))
    })
}

fn stream_text(value: &Value) -> Option<String> {
    // Only `response.output_text.delta` events carry assistant text.
    // The Responses API also uses `delta` for `function_call_arguments.delta`
    // (tool args, not text); routing those through here corrupts the
    // assistant transcript with raw JSON args. Anchor on the event `type`.
    let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
    let is_text_event = kind == "response.output_text.delta"
        || kind == "response.content_part.added"
        || kind == "response.message.delta";
    let nested = value
        .pointer("/response/output_text/delta")
        .and_then(Value::as_str);
    if !is_text_event && nested.is_none() {
        return None;
    }
    first_str(value, &["delta", "text"])
        .or_else(|| {
            value
                .pointer("/response/output_text/delta")
                .and_then(Value::as_str)
        })
        .map(str::to_string)
}

fn stream_tool(value: &Value, state: &mut StreamState) -> Vec<StreamEvent> {
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let item = value.get("item").or_else(|| value.get("output"));
    let idx = value
        .get("output_index")
        .or_else(|| value.get("item_index"))
        .and_then(Value::as_u64)
        .unwrap_or(state.tools.len() as u64) as usize;
    if kind == "response.output_item.added" || kind == "response.output_item.done" {
        if let Some(item) =
            item.filter(|item| item.get("type").and_then(Value::as_str) == Some("function_call"))
        {
            ensure_tool(state, idx);
            let part = &mut state.tools[idx];
            if let Some(id) = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(Value::as_str)
            {
                part.id = id.to_string();
            }
            if let Some(name) = item.get("name").and_then(Value::as_str) {
                part.name = Some(name.to_string());
            }
            if let Some(args) = item.get("arguments").and_then(Value::as_str) {
                if kind.ends_with("done") {
                    if !args.is_empty() {
                        part.args = args.to_string();
                    }
                } else {
                    part.args.push_str(args);
                }
            }
            if kind.ends_with("done") {
                if part.done {
                    return Vec::new();
                }
                part.done = true;
                let call = finish_tool(part, idx);
                let call = unique_tool_call(call, &mut state.ids);
                state.calls.push(call.clone());
                return vec![StreamEvent::ToolCall(call)];
            }
        }
    }
    if !kind.contains("function_call") {
        return Vec::new();
    }
    ensure_tool(state, idx);
    let part = &mut state.tools[idx];
    let id = value
        .get("call_id")
        .or_else(|| value.get("id"))
        .and_then(Value::as_str)
        .unwrap_or(&part.id)
        .to_string();
    if !id.is_empty() {
        part.id = id.clone();
    }
    if let Some(name) = value.get("name").and_then(Value::as_str) {
        part.name = Some(name.to_string());
    }
    if kind.ends_with("done") {
        if let Some(args) = first_str(value, &["arguments", "arguments_delta"]) {
            if !args.is_empty() {
                part.args = args.to_string();
            }
        }
        if part.done {
            return Vec::new();
        }
        part.done = true;
        let call = finish_tool(part, idx);
        let call = unique_tool_call(call, &mut state.ids);
        state.calls.push(call.clone());
        return vec![StreamEvent::ToolCall(call)];
    }
    let delta = first_str(value, &["delta", "arguments_delta", "arguments"])
        .unwrap_or_default()
        .to_string();
    part.args.push_str(&delta);
    vec![StreamEvent::ToolDelta {
        id: part.id.clone(),
        name: part.name.clone(),
        arguments: delta,
    }]
}

fn ensure_tool(state: &mut StreamState, idx: usize) {
    while state.tools.len() <= idx {
        state.tools.push(PartialTool::default());
    }
}

fn finish_tool(part: &PartialTool, idx: usize) -> ChatToolCall {
    let name = part.name.clone().unwrap_or_else(|| "unknown".to_string());
    let id = if part.id.is_empty() {
        format!("call_{name}_{idx}")
    } else {
        part.id.clone()
    };
    let (name, input) = parse_tool_input(&name, Some(&part.args));
    ChatToolCall { id, name, input }
}

fn parse_tool_input(name: &str, raw: Option<&str>) -> (String, Value) {
    let Some(args) = raw else {
        return invalid_tool_input(name, "Missing tool arguments".to_string(), "");
    };
    if args.trim().is_empty() {
        return invalid_tool_input(name, "Missing tool arguments".to_string(), args);
    }
    match serde_json::from_str::<Value>(args) {
        Ok(value @ Value::Object(_)) => (name.to_string(), value),
        Ok(value) => invalid_tool_input(
            name,
            format!(
                "Tool arguments must be a JSON object, got {}",
                json_type(&value)
            ),
            args,
        ),
        Err(err) => invalid_tool_input(name, format!("Invalid tool arguments JSON: {err}"), args),
    }
}

fn invalid_tool_input(name: &str, err: String, raw: &str) -> (String, Value) {
    (
        "invalid".to_string(),
        json!({
            "tool": name,
            "error": err,
            "arguments": raw,
        }),
    )
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn unique_tool_call(mut call: ChatToolCall, ids: &mut BTreeSet<String>) -> ChatToolCall {
    if ids.insert(call.id.clone()) {
        return call;
    }
    let raw = call.id.clone();
    let mut idx = 1;
    loop {
        let next = format!("{raw}_{idx}");
        if ids.insert(next.clone()) {
            call.id = next;
            return call;
        }
        idx += 1;
    }
}

fn stream_finish(value: &Value) -> Option<String> {
    match value.get("type").and_then(Value::as_str) {
        Some("response.completed") => Some("stop".to_string()),
        Some("response.failed") => Some("error".to_string()),
        Some("response.incomplete") => Some("length".to_string()),
        _ => None,
    }
}

fn first_str<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter().find_map(|key| value.get(*key)?.as_str())
}

fn parse_response_tool_calls(body: &Value) -> Vec<ChatToolCall> {
    body.get("output")
        .and_then(Value::as_array)
        .map(|items| {
            let mut ids = BTreeSet::new();
            items
                .iter()
                .enumerate()
                .filter_map(|(idx, item)| parse_response_tool_call(item, idx))
                .map(|call| unique_tool_call(call, &mut ids))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_response_tool_call(item: &Value, idx: usize) -> Option<ChatToolCall> {
    if item.get("type").and_then(Value::as_str)? != "function_call" {
        return None;
    }
    let name = item.get("name")?.as_str()?.to_string();
    let id = item
        .get("call_id")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| format!("call_{name}_{idx}"));
    let (name, input) = parse_tool_input(&name, item.get("arguments").and_then(Value::as_str));
    Some(ChatToolCall { id, name, input })
}

fn parse_usage(body: &Value) -> Option<ChatUsage> {
    let usage = body
        .get("usage")
        .or_else(|| body.pointer("/response/usage"))?;
    let input = usage
        .get("input_tokens")
        .or_else(|| usage.get("prompt_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output = usage
        .get("output_tokens")
        .or_else(|| usage.get("completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total = usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(input + output);
    // OpenAI Responses API exposes cached input under `input_tokens_details.cached_tokens`
    // and reasoning output under `output_tokens_details.reasoning_tokens`. Older shapes
    // surface `cached_tokens` flat on the usage object.
    let reasoning = usage
        .pointer("/output_tokens_details/reasoning_tokens")
        .or_else(|| usage.get("reasoning_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_read = usage
        .pointer("/input_tokens_details/cached_tokens")
        .or_else(|| usage.get("cached_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let cache_write = usage
        .pointer("/input_tokens_details/cache_creation_input_tokens")
        .or_else(|| usage.get("cache_creation_input_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    Some(ChatUsage {
        input,
        output,
        total,
        reasoning,
        cache_read,
        cache_write,
    })
}

fn parse_finish(body: &Value) -> Option<String> {
    body.get("choices")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .and_then(|item| item.get("finish_reason"))
        .and_then(Value::as_str)
        .or_else(|| body.get("finish_reason").and_then(Value::as_str))
        .or_else(|| body.pointer("/response/status").and_then(Value::as_str))
        .map(|value| match value {
            "completed" => "stop".to_string(),
            "incomplete" => "length".to_string(),
            other => other.to_string(),
        })
}

fn message_text(msg: &Value) -> String {
    let Some(content) = msg.get("content") else {
        return String::new();
    };
    if let Some(text) = content.as_str() {
        return text.to_string();
    }
    let Some(items) = content.as_array() else {
        return String::new();
    };
    let mut out = String::new();
    for item in items {
        if let Some(text) = item.get("text").and_then(Value::as_str) {
            out.push_str(text);
        }
    }
    out
}

fn parse_tool_calls(msg: &Value) -> Vec<ChatToolCall> {
    msg.get("tool_calls")
        .and_then(Value::as_array)
        .map(|items| {
            let mut ids = BTreeSet::new();
            items
                .iter()
                .enumerate()
                .filter_map(|(idx, item)| parse_tool_call(item, idx))
                .map(|call| unique_tool_call(call, &mut ids))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_tool_call(item: &Value, idx: usize) -> Option<ChatToolCall> {
    let fun = item.get("function")?;
    let name = fun.get("name")?.as_str()?.to_string();
    let id = item
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| format!("call_{name}_{idx}"));
    let (name, input) = parse_tool_input(&name, fun.get("arguments").and_then(Value::as_str));

    Some(ChatToolCall { id, name, input })
}

fn api_message(body: &Value) -> String {
    body.get("error")
        .and_then(|value| value.get("message"))
        .and_then(Value::as_str)
        .or_else(|| body.get("message").and_then(Value::as_str))
        .unwrap_or("provider request failed")
        .to_string()
}

fn api_status_message(status: u16, raw: &str) -> String {
    let snippet = raw.trim().chars().take(500).collect::<String>();
    let body = serde_json::from_str::<Value>(&snippet).unwrap_or(Value::Null);
    let msg = if body.is_null() {
        snippet.clone()
    } else {
        api_message(&body)
    };
    if msg.trim().is_empty() {
        return format!("provider request failed (status {status})");
    }
    format!("{msg} (status {status}; body: {snippet})")
}

fn api_status_error(status: u16, headers: &HeaderMap, raw: &str) -> ProviderError {
    let msg = api_status_message(status, raw);
    if api_is_context_window(status, raw) {
        return ProviderError::ContextWindow(msg);
    }
    ProviderError::ApiStatus(ApiStatusError {
        status,
        message: msg,
        body: raw.to_string(),
        retry_after_ms: retry_after_ms(headers),
        retryable: api_retryable(status, raw),
    })
}

fn api_retryable(status: u16, raw: &str) -> bool {
    if raw.contains("FreeUsageLimitError") {
        return false;
    }
    if status >= 500 || matches!(status, 408 | 409 | 425 | 429) {
        return true;
    }
    let lower = raw.to_ascii_lowercase();
    lower.contains("rate increased too quickly")
        || lower.contains("rate limit")
        || lower.contains("too many requests")
        || lower.contains("temporarily unavailable")
        || lower.contains("overloaded")
}

fn retry_after_ms(headers: &HeaderMap) -> Option<u64> {
    let direct = headers
        .get("retry-after-ms")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_ms);
    if direct.is_some() {
        return direct;
    }
    let value = headers.get("retry-after")?.to_str().ok()?.trim();
    parse_seconds_ms(value).or_else(|| parse_http_date_ms(value))
}

fn parse_ms(value: &str) -> Option<u64> {
    let ms = value.trim().parse::<f64>().ok()?;
    if !ms.is_finite() || ms < 0.0 {
        return None;
    }
    Some(ms.ceil() as u64)
}

fn parse_seconds_ms(value: &str) -> Option<u64> {
    let seconds = value.parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    Some((seconds * 1000.0).ceil() as u64)
}

fn parse_http_date_ms(value: &str) -> Option<u64> {
    let date = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    let now = chrono::Utc::now();
    let ms = date.with_timezone(&chrono::Utc) - now;
    (ms.num_milliseconds() > 0).then(|| ms.num_milliseconds() as u64)
}

fn first<const N: usize>(items: [Option<String>; N]) -> Option<String> {
    items
        .into_iter()
        .flatten()
        .find(|item| !item.trim().is_empty())
}

fn provider_option(cfg: &Config, provider: &str, key: &str) -> Option<String> {
    cfg.data
        .get("provider")?
        .get(provider)?
        .get("options")?
        .get(key)?
        .as_str()
        .map(str::to_string)
}

fn model_url(cfg: &Config, provider: &str, model: &str) -> Option<String> {
    cfg.data
        .get("provider")?
        .get(provider)?
        .get("models")?
        .get(model)?
        .get("api")?
        .get("url")?
        .as_str()
        .map(str::to_string)
}

fn env_value(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.trim().is_empty())
}

fn env_auth(provider: &str) -> Option<ChatAuth> {
    let all = env_value("KILO_AUTH_CONTENT")?;
    let data: Value = serde_json::from_str(&all).ok()?;
    auth_value(&data, provider)
}

fn auth_value(data: &Value, provider: &str) -> Option<ChatAuth> {
    let item = data.get(provider)?;
    match item.get("type").and_then(Value::as_str)? {
        "oauth" => Some(ChatAuth::Oauth {
            access: item.get("access")?.as_str()?.to_string(),
            account: item
                .get("accountId")
                .and_then(Value::as_str)
                .map(str::to_string),
        }),
        "api" => Some(ChatAuth::Api {
            key: item.get("key")?.as_str()?.to_string(),
        }),
        _ => None,
    }
}

fn env_name(provider: &str) -> String {
    provider
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

fn text<'a>(value: Option<&'a Value>, keys: &[&str]) -> Option<&'a str> {
    let value = value?;
    keys.iter().find_map(|key| value.get(*key)?.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::thread;

    const CODEX_TOOL_CALL_STREAM: &str =
        include_str!("../../../fixtures/openai-responses/codex-tool-call.sse");
    const CODEX_FINAL_TEXT_STREAM: &str =
        include_str!("../../../fixtures/openai-responses/codex-final-text.sse");

    fn fixture_stream(raw: &str) -> String {
        raw.replace("\r\n", "\n")
    }

    #[test]
    fn parse_usage_reads_cached_and_reasoning_from_responses_api() {
        let body = json!({
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "total_tokens": 150,
                "input_tokens_details": {
                    "cached_tokens": 30,
                    "cache_creation_input_tokens": 12,
                },
                "output_tokens_details": { "reasoning_tokens": 20 }
            }
        });
        let u = parse_usage(&body).unwrap();
        assert_eq!(u.input, 100);
        assert_eq!(u.output, 50);
        assert_eq!(u.total, 150);
        assert_eq!(u.cache_read, 30);
        assert_eq!(u.cache_write, 12);
        assert_eq!(u.reasoning, 20);
    }

    #[test]
    fn api_is_context_window_detects_openai_context_length_exceeded() {
        let body = r#"{"error":{"code":"context_length_exceeded","message":"This model's maximum context length is 128000 tokens."}}"#;
        assert!(api_is_context_window(400, body));
        assert!(api_is_context_window(422, body));
        assert!(!api_is_context_window(500, body)); // wrong status
        assert!(!api_is_context_window(
            400,
            r#"{"error":{"message":"unrelated"}}"#
        ));
    }

    #[test]
    fn api_is_context_window_falls_back_to_substring_match() {
        // Plain text body, no JSON.
        assert!(api_is_context_window(
            400,
            "request rejected: token limit exceeded"
        ));
        // JSON shape but only message mentions the limit.
        assert!(api_is_context_window(
            400,
            r#"{"error":{"message":"Input is too long for context window."}}"#
        ));
    }

    #[test]
    fn parse_usage_falls_back_to_flat_keys() {
        let body = json!({
            "usage": {
                "prompt_tokens": 42,
                "completion_tokens": 13,
                "cached_tokens": 7,
                "reasoning_tokens": 4
            }
        });
        let u = parse_usage(&body).unwrap();
        assert_eq!(u.input, 42);
        assert_eq!(u.output, 13);
        assert_eq!(u.total, 55);
        assert_eq!(u.cache_read, 7);
        assert_eq!(u.reasoning, 4);
        assert_eq!(u.cache_write, 0);
    }

    fn header_server() -> (String, thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0; 8192];
            let got = stream.read(&mut buf).unwrap();
            let req = String::from_utf8_lossy(&buf[..got]).to_string();
            let body =
                r#"{"output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}]}"#;
            let res = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(res.as_bytes()).unwrap();
            req
        });
        (url, handle)
    }

    #[test]
    fn resolves_model_config_and_key() {
        let _guard = env_guard();
        env::remove_var("KILO_AUTH_CONTENT");
        env::remove_var("OPENAI_API_KEY");
        env::remove_var("OPENAI_BASE_URL");
        let cfg = Config {
            data: BTreeMap::from([(
                "provider".to_string(),
                json!({
                    "openai": {
                        "options": { "apiKey": "cfg-key", "baseURL": "http://local/v1" },
                        "models": { "gpt-test": { "api": { "url": "http://model/v1" } } }
                    }
                }),
            )]),
        };

        let req = resolve(
            &cfg,
            Some(&json!({ "providerID": "openai", "modelID": "gpt-test" })),
            vec![ChatMessage {
                role: "user".to_string(),
                content: "hello".to_string(),
                responses: Vec::new(),
                attachments: Vec::new(),
            }],
        )
        .unwrap();

        assert_eq!(req.provider, "openai");
        assert_eq!(req.model, "gpt-test");
        assert_eq!(req.base, "http://local/v1");
        assert_eq!(
            req.auth,
            ChatAuth::Api {
                key: "cfg-key".to_string()
            }
        );
        assert_eq!(req.messages[0].content, "hello");
    }

    #[tokio::test]
    async fn openai_oauth_post_sends_session_and_bun_shaped_user_agent() {
        let (url, handle) = header_server();
        let req = ChatRequest {
            provider: "openai".to_string(),
            model: "gpt-5.1-codex".to_string(),
            base: url,
            auth: ChatAuth::Oauth {
                access: "access".to_string(),
                account: None,
            },
            session_id: Some("ses_123".to_string()),
            instructions: None,
            messages: Vec::new(),
            tools: Vec::new(),
        };

        let out = post(&req).await.unwrap();
        let raw = handle.join().unwrap();

        assert_eq!(out.text, "ok");
        assert!(raw.contains("session_id: ses_123"));
        assert!(!raw.contains("User-Agent: opencode/rust-sidecar"));
        assert!(raw.contains(&format!(
            "user-agent: opencode/{}",
            env!("CARGO_PKG_VERSION")
        )));
        assert!(raw.contains(&format!(
            "({}/{})",
            std::env::consts::OS,
            std::env::consts::ARCH
        )));
    }

    #[test]
    fn resolves_provider_env_before_openai_env() {
        let _guard = env_guard();
        env::remove_var("KILO_AUTH_CONTENT");
        env::set_var("OPENAI_API_KEY", "openai-key");
        env::set_var("ACME_API_KEY", "acme-key");
        env::set_var("OPENAI_BASE_URL", "http://openai/v1");
        env::set_var("ACME_BASE_URL", "http://acme/v1");
        let cfg = Config {
            data: BTreeMap::new(),
        };

        let req = resolve(
            &cfg,
            Some(&json!({ "providerID": "acme", "modelID": "chat" })),
            vec![],
        )
        .unwrap();

        assert_eq!(req.base, "http://acme/v1");
        assert_eq!(
            req.auth,
            ChatAuth::Api {
                key: "acme-key".to_string()
            }
        );

        env::remove_var("OPENAI_API_KEY");
        env::remove_var("ACME_API_KEY");
        env::remove_var("OPENAI_BASE_URL");
        env::remove_var("ACME_BASE_URL");
    }

    #[test]
    fn rejects_missing_key() {
        let _guard = env_guard();
        env::remove_var("KILO_AUTH_CONTENT");
        env::remove_var("OPENAI_API_KEY");
        env::remove_var("OPENAI_BASE_URL");
        let cfg = Config {
            data: BTreeMap::new(),
        };

        let err = resolve(
            &cfg,
            Some(&json!({ "providerID": "openai", "modelID": "gpt-test" })),
            vec![],
        )
        .unwrap_err();

        assert_eq!(
            err,
            ProviderError::MissingKey {
                provider: "openai".to_string()
            }
        );
    }

    #[test]
    fn parses_openai_chat_response() {
        let out = parse(&json!({
            "choices": [{ "message": { "content": "real response" } }]
        }))
        .unwrap();

        assert_eq!(out.text, "real response");
        assert!(out.tool_calls.is_empty());
    }

    #[test]
    fn api_status_message_keeps_status_and_body() {
        let err = api_status_message(400, r#"{"error":{"message":"bad transcript"}}"#);

        assert!(err.contains("bad transcript"));
        assert!(err.contains("status 400"));
        assert!(err.contains("body:"));
    }

    #[test]
    fn api_status_message_keeps_non_json_body() {
        let err = api_status_message(502, "upstream unavailable");

        assert!(err.contains("upstream unavailable"));
        assert!(err.contains("status 502"));
    }

    #[test]
    fn api_status_error_marks_5xx_retryable_with_retry_after_ms() {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after-ms", HeaderValue::from_static("1250"));
        let err = api_status_error(503, &headers, r#"{"error":{"message":"Overloaded"}}"#);

        let ProviderError::ApiStatus(err) = err else {
            panic!("expected api status error");
        };
        assert_eq!(err.status, 503);
        assert_eq!(err.retry_after_ms, Some(1250));
        assert!(err.retryable);
        assert!(err.message.contains("Overloaded"));
    }

    #[test]
    fn api_status_error_does_not_retry_free_usage_limit() {
        let err = api_status_error(429, &HeaderMap::new(), "FreeUsageLimitError");

        let ProviderError::ApiStatus(err) = err else {
            panic!("expected api status error");
        };
        assert!(!err.retryable);
    }

    #[test]
    fn parses_openai_responses_stream_events() {
        let events = parse_stream(
            r#"data: {"type":"response.output_text.delta","delta":"Hel"}

data: {"type":"response.output_text.delta","delta":"lo"}

data: {"type":"response.function_call_arguments.delta","output_index":1,"call_id":"call_read","name":"read","delta":"{\"filePath\":"}

data: {"type":"response.function_call_arguments.delta","output_index":1,"delta":"\"note.txt\"}"}

data: {"type":"response.function_call_arguments.done","output_index":1,"call_id":"call_read","name":"read"}

data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}}

data: [DONE]

"#,
        )
        .unwrap();

        assert_eq!(events[0], StreamEvent::TextDelta("Hel".to_string()));
        assert_eq!(events[1], StreamEvent::TextDelta("lo".to_string()));
        assert!(events
            .iter()
            .any(|event| matches!(event, StreamEvent::ToolDelta { .. })));
        let call = events
            .iter()
            .find_map(|event| match event {
                StreamEvent::ToolCall(call) => Some(call),
                _ => None,
            })
            .expect("tool call");
        assert_eq!(
            call,
            &ChatToolCall {
                id: "call_read".to_string(),
                name: "read".to_string(),
                input: json!({ "filePath": "note.txt" }),
            }
        );
        assert!(events.iter().any(|event| event
            == &StreamEvent::Usage(ChatUsage {
                input: 7,
                output: 3,
                total: 10,
                ..Default::default()
            })));
        assert!(events
            .iter()
            .any(|event| event == &StreamEvent::Finish("stop".to_string())));
    }

    #[test]
    fn parses_openai_responses_reasoning_summary_events() {
        let events = parse_stream(
            r#"data: {"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning"}}

data: {"type":"response.reasoning_summary_text.delta","item_id":"rs_1","summary_index":0,"delta":"thinking "}

data: {"type":"response.reasoning_summary_text.delta","item_id":"rs_1","summary_index":0,"delta":"hard"}

data: {"type":"response.output_item.done","output_index":0,"item":{"id":"rs_1","type":"reasoning"}}

data: [DONE]

"#,
        )
        .unwrap();

        assert_eq!(
            events,
            vec![
                StreamEvent::ReasoningStart {
                    id: "rs_1:0".to_string()
                },
                StreamEvent::ReasoningDelta {
                    id: "rs_1:0".to_string(),
                    delta: "thinking ".to_string()
                },
                StreamEvent::ReasoningDelta {
                    id: "rs_1:0".to_string(),
                    delta: "hard".to_string()
                },
                StreamEvent::ReasoningEnd {
                    id: "rs_1:0".to_string()
                },
            ]
        );
    }

    #[test]
    fn reasoning_item_with_encrypted_content_is_captured_in_stream_events() {
        // `response.output_item.done` for a `type:"reasoning"` item that
        // carries `encrypted_content` should emit a `ReasoningItem` event
        // alongside the trailing `ReasoningEnd`s. Required for cache-hit
        // round-trip in the next Responses request.
        let events = parse_stream(
            r#"data: {"type":"response.output_item.added","output_index":0,"item":{"id":"rs_2","type":"reasoning"}}

data: {"type":"response.reasoning_summary_text.delta","item_id":"rs_2","summary_index":0,"delta":"thinking"}

data: {"type":"response.output_item.done","output_index":0,"item":{"id":"rs_2","type":"reasoning","encrypted_content":"BLOB"}}

data: [DONE]

"#,
        )
        .unwrap();

        let captured = events
            .iter()
            .find_map(|event| match event {
                StreamEvent::ReasoningItem {
                    id,
                    encrypted_content,
                } => Some((id.as_str(), encrypted_content.as_str())),
                _ => None,
            })
            .expect("reasoning item event");
        assert_eq!(captured, ("rs_2", "BLOB"));
        // ReasoningEnd must still fire for the streamed summary so the
        // UI knows the trace closed.
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ReasoningEnd { id } if id == "rs_2:0"
        )));
    }

    #[test]
    fn reasoning_item_without_encrypted_content_emits_no_round_trip_event() {
        // Non-reasoning models still flush an `output_item.done` for any
        // streamed summary but do not carry `encrypted_content`. Make
        // sure we don't synthesize a bogus replay item.
        let events = parse_stream(
            r#"data: {"type":"response.output_item.added","output_index":0,"item":{"id":"rs_3","type":"reasoning"}}

data: {"type":"response.output_item.done","output_index":0,"item":{"id":"rs_3","type":"reasoning"}}

data: [DONE]

"#,
        )
        .unwrap();
        assert!(!events
            .iter()
            .any(|event| matches!(event, StreamEvent::ReasoningItem { .. })));
    }

    #[test]
    fn responses_input_emits_reasoning_items_before_assistant_content() {
        // Encrypted reasoning items on an assistant message should
        // surface as standalone `{type:"reasoning"}` input items
        // BEFORE the assistant content, matching Bun's converter at
        // `provider/sdk/copilot/responses/convert-to-openai-responses-input.ts:185-244`.
        let input = responses_input(&[
            ChatMessage {
                role: "assistant".to_string(),
                content: "I'll inspect.".to_string(),
                responses: vec![
                    ChatResponseItem::Reasoning(ReasoningItem {
                        id: "rs_42".to_string(),
                        encrypted_content: "ENC".to_string(),
                        summary: vec!["thinking hard".to_string()],
                    }),
                    ChatResponseItem::FunctionCall(ChatToolCall {
                        id: "call_read".to_string(),
                        name: "read".to_string(),
                        input: json!({ "filePath": "x" }),
                    }),
                ],
                attachments: Vec::new(),
            },
            ChatMessage {
                role: "tool".to_string(),
                content: String::new(),
                responses: vec![ChatResponseItem::FunctionOutput {
                    id: "call_read".to_string(),
                    output: "ok".to_string(),
                }],
                attachments: Vec::new(),
            },
        ]);

        // Order: reasoning -> assistant message -> function_call -> function_call_output.
        assert_eq!(input[0]["type"], "reasoning");
        assert_eq!(input[0]["id"], "rs_42");
        assert_eq!(input[0]["encrypted_content"], "ENC");
        assert_eq!(input[0]["summary"][0]["type"], "summary_text");
        assert_eq!(input[0]["summary"][0]["text"], "thinking hard");
        assert_eq!(input[1]["role"], "assistant");
        assert_eq!(input[2]["type"], "function_call");
        assert_eq!(input[3]["type"], "function_call_output");
    }

    #[test]
    fn responses_input_does_not_emit_reasoning_items_for_messages_without_them() {
        // Non-reasoning models — pure user/assistant turn must not get
        // a synthetic empty reasoning item.
        let input = responses_input(&[ChatMessage {
            role: "assistant".to_string(),
            content: "hi".to_string(),
            responses: Vec::new(),
            attachments: Vec::new(),
        }]);
        assert!(input
            .iter()
            .all(|item| item.get("type").and_then(Value::as_str) != Some("reasoning")));
    }

    #[test]
    fn replays_recorded_codex_tool_call_stream_fixture() {
        let raw = fixture_stream(CODEX_TOOL_CALL_STREAM);
        let events = parse_stream(&raw).unwrap();
        let text = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::TextDelta(delta) => Some(delta.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(text, "I'll inspect the file.");

        let calls = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::ToolCall(call) => Some(call),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "call_read_note");
        assert_eq!(calls[0].name, "read");
        assert_eq!(
            calls[0].input,
            json!({ "filePath": "repo/note.txt", "limit": 20 })
        );
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::Usage(ChatUsage {
                input: 111,
                output: 22,
                total: 133,
                ..
            })
        )));
        assert!(events
            .iter()
            .any(|event| event == &StreamEvent::Finish("stop".to_string())));

        let parts = classify_stream(&raw).unwrap();
        assert!(parts
            .iter()
            .any(|part| matches!(part, ResponsesStreamPart::ToolCallArgsDelta { id, .. } if id == "call_read_note")));
        assert!(parts.iter().any(|part| matches!(
            part,
            ResponsesStreamPart::ToolCallComplete { id, name, args }
                if id == "call_read_note"
                    && name == "read"
                    && args == &json!({ "filePath": "repo/note.txt", "limit": 20 })
        )));
        assert!(parts.iter().any(|part| matches!(
            part,
            ResponsesStreamPart::Finish { reason, usage: Some(usage) }
                if reason == "stop" && usage.input == 111 && usage.output == 22 && usage.total == 133
        )));
    }

    #[test]
    fn replays_recorded_codex_final_text_stream_fixture() {
        let raw = fixture_stream(CODEX_FINAL_TEXT_STREAM);
        let events = parse_stream(&raw).unwrap();
        let text = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::TextDelta(delta) => Some(delta.as_str()),
                _ => None,
            })
            .collect::<String>();
        assert_eq!(text, "The file says: `fixture ok`.");
        assert!(events.iter().any(|event| event
            == &StreamEvent::Usage(ChatUsage {
                input: 55,
                output: 11,
                total: 66,
                ..Default::default()
            })));
        assert!(events
            .iter()
            .any(|event| event == &StreamEvent::Finish("stop".to_string())));
    }

    #[tokio::test]
    #[ignore = "live OpenAI Pro OAuth smoke; set KILO_OPENAI_PRO_LIVE_TEST=1 and KILO_OPENAI_PRO_ACCESS_TOKEN"]
    async fn live_openai_pro_oauth_responses_smoke_skips_without_explicit_env() {
        if env::var("KILO_OPENAI_PRO_LIVE_TEST").as_deref() != Ok("1") {
            eprintln!("live smoke skipped: KILO_OPENAI_PRO_LIVE_TEST is not 1");
            return;
        }
        let access = match env::var("KILO_OPENAI_PRO_ACCESS_TOKEN") {
            Ok(value) if !value.trim().is_empty() => value,
            _ => {
                eprintln!("live smoke skipped: KILO_OPENAI_PRO_ACCESS_TOKEN is not set");
                return;
            }
        };
        let model = env::var("KILO_OPENAI_PRO_MODEL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "gpt-5.1-codex".to_string());
        let mut auth = json!({
            "type": "oauth",
            "access": access,
            "refresh": "redacted-live-smoke-refresh-not-used",
            "expires": 9999999999999i64,
        });
        if let Ok(account) = env::var("KILO_OPENAI_PRO_ACCOUNT_ID") {
            if !account.trim().is_empty() {
                auth["accountId"] = json!(account);
            }
        }
        let cfg = Config {
            data: BTreeMap::new(),
        };
        let auths = json!({ "openai": auth });
        let cancel = AtomicBool::new(false);
        let mut events = Vec::new();
        let out = responses_stream(
            &cfg,
            &auths,
            Some(&json!({ "providerID": "openai", "modelID": model })),
            Some("Respond with a short smoke-test acknowledgement.".to_string()),
            vec![ChatMessage {
                role: "user".to_string(),
                content: "Say KILO_SMOKE_OK and nothing sensitive.".to_string(),
                responses: Vec::new(),
                attachments: Vec::new(),
            }],
            Vec::new(),
            &cancel,
            |event| events.push(event),
        )
        .await
        .expect("live OpenAI Pro OAuth Responses smoke");

        assert_eq!(out.provider, "openai");
        assert!(
            !out.text.trim().is_empty()
                || events
                    .iter()
                    .any(|event| matches!(event, StreamEvent::TextDelta(_)))
        );
        assert!(
            out.finish.is_some()
                || events
                    .iter()
                    .any(|event| matches!(event, StreamEvent::Finish(_)))
        );
    }

    #[test]
    fn parses_openai_tool_calls() {
        let out = parse(&json!({
            "choices": [{
                "message": {
                    "content": "I'll read it.",
                    "tool_calls": [{
                        "id": "call_read",
                        "type": "function",
                        "function": {
                            "name": "read",
                            "arguments": "{\"filePath\":\"src/main.rs\",\"limit\":2}"
                        }
                    }]
                }
            }]
        }))
        .unwrap();

        assert_eq!(out.text, "I'll read it.");
        assert_eq!(out.tool_calls.len(), 1);
        assert_eq!(out.tool_calls[0].id, "call_read");
        assert_eq!(out.tool_calls[0].name, "read");
        assert_eq!(out.tool_calls[0].input["filePath"], "src/main.rs");
        assert_eq!(out.tool_calls[0].input["limit"], 2);
    }

    #[test]
    fn parses_openai_tool_calls_without_text() {
        let out = parse(&json!({
            "choices": [{
                "message": {
                    "content": null,
                    "tool_calls": [{
                        "id": "call_grep",
                        "type": "function",
                        "function": {
                            "name": "grep",
                            "arguments": "{\"pattern\":\"needle\"}"
                        }
                    }]
                }
            }]
        }))
        .unwrap();

        assert_eq!(out.text, "");
        assert_eq!(out.tool_calls[0].name, "grep");
        assert_eq!(out.tool_calls[0].input["pattern"], "needle");
    }

    #[test]
    fn malformed_tool_arguments_become_invalid_tool_call() {
        let out = parse(&json!({
            "choices": [{
                "message": {
                    "content": null,
                    "tool_calls": [{
                        "id": "call_bad",
                        "type": "function",
                        "function": {
                            "name": "read",
                            "arguments": "{\"filePath\":"
                        }
                    }]
                }
            }]
        }))
        .unwrap();

        assert_eq!(out.tool_calls.len(), 1);
        assert_eq!(out.tool_calls[0].id, "call_bad");
        assert_eq!(out.tool_calls[0].name, "invalid");
        assert_eq!(out.tool_calls[0].input["tool"], "read");
        assert!(out.tool_calls[0].input["error"]
            .as_str()
            .unwrap()
            .contains("Invalid tool arguments JSON"));
    }

    #[test]
    fn response_tool_calls_have_deterministic_ids() {
        let out = parse(&json!({
            "output": [
                {
                    "type": "function_call",
                    "name": "read",
                    "arguments": "{\"filePath\":\"a.txt\"}"
                },
                {
                    "type": "function_call",
                    "call_id": "call_read_0",
                    "name": "read",
                    "arguments": "{\"filePath\":\"b.txt\"}"
                }
            ]
        }))
        .unwrap();

        assert_eq!(out.tool_calls[0].id, "call_read_0");
        assert_eq!(out.tool_calls[1].id, "call_read_0_1");
    }

    #[test]
    fn stream_malformed_tool_arguments_become_invalid_tool_call() {
        let events = parse_stream(
            r#"data: {"type":"response.function_call_arguments.delta","output_index":0,"call_id":"call_bad","name":"read","delta":"{\"filePath\":"}

data: {"type":"response.function_call_arguments.done","output_index":0,"call_id":"call_bad","name":"read"}

"#,
        )
        .unwrap();

        let call = events
            .iter()
            .find_map(|event| match event {
                StreamEvent::ToolCall(call) => Some(call),
                _ => None,
            })
            .expect("tool call");
        assert_eq!(call.id, "call_bad");
        assert_eq!(call.name, "invalid");
        assert_eq!(call.input["tool"], "read");
        assert!(call.input["error"]
            .as_str()
            .unwrap()
            .contains("Invalid tool arguments JSON"));
    }

    #[test]
    fn stream_done_arguments_replace_deltas_instead_of_appending() {
        let events = parse_stream(
            r#"data: {"type":"response.output_item.added","output_index":0,"item":{"id":"fc_read","type":"function_call","call_id":"call_read","name":"read","arguments":""}}

data: {"type":"response.function_call_arguments.delta","output_index":0,"call_id":"call_read","name":"read","delta":"{\"filePath\":"}

data: {"type":"response.function_call_arguments.delta","output_index":0,"call_id":"call_read","name":"read","delta":"\"src/main.rs\"}"}

data: {"type":"response.function_call_arguments.done","output_index":0,"call_id":"call_read","name":"read","arguments":"{\"filePath\":\"src/main.rs\"}"}

"#,
        )
        .unwrap();

        let calls = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::ToolCall(call) => Some(call),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "call_read");
        assert_eq!(calls[0].name, "read");
        assert_eq!(calls[0].input["filePath"], "src/main.rs");
    }

    #[test]
    fn stream_output_item_done_does_not_duplicate_completed_tool_call() {
        let events = parse_stream(
            r#"data: {"type":"response.output_item.added","output_index":0,"item":{"id":"fc_read","type":"function_call","call_id":"call_read","name":"read","arguments":""}}

data: {"type":"response.function_call_arguments.delta","output_index":0,"call_id":"call_read","name":"read","delta":"{\"filePath\":\"src/main.rs\"}"}

data: {"type":"response.function_call_arguments.done","output_index":0,"call_id":"call_read","name":"read"}

data: {"type":"response.output_item.done","output_index":0,"item":{"id":"fc_read","type":"function_call","call_id":"call_read","name":"read","arguments":"{\"filePath\":\"src/main.rs\"}"}}

"#,
        )
        .unwrap();

        let calls = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::ToolCall(call) => Some(call),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "call_read");
        assert_eq!(calls[0].input["filePath"], "src/main.rs");
    }

    #[test]
    fn responses_input_uses_structured_tool_continuation_items() {
        let input = responses_input(&[
            ChatMessage {
                role: "user".to_string(),
                content: "inspect file".to_string(),
                responses: Vec::new(),
                attachments: Vec::new(),
            },
            ChatMessage {
                role: "assistant".to_string(),
                content: "I'll read it.".to_string(),
                responses: vec![ChatResponseItem::FunctionCall(ChatToolCall {
                    id: "call_read".to_string(),
                    name: "read".to_string(),
                    input: json!({ "filePath": "src/main.rs" }),
                })],
                attachments: Vec::new(),
            },
            ChatMessage {
                role: "tool".to_string(),
                content: String::new(),
                responses: vec![ChatResponseItem::FunctionOutput {
                    id: "call_read".to_string(),
                    output: "file contents".to_string(),
                }],
                attachments: Vec::new(),
            },
        ]);

        assert_eq!(input.len(), 4);
        assert_eq!(input[1]["content"][0]["type"], "output_text");
        assert_eq!(input[2]["type"], "function_call");
        assert_eq!(input[2]["call_id"], "call_read");
        assert_eq!(input[2]["name"], "read");
        assert_eq!(input[2]["arguments"], r#"{"filePath":"src/main.rs"}"#);
        assert_eq!(input[3]["type"], "function_call_output");
        assert_eq!(input[3]["call_id"], "call_read");
        assert_eq!(input[3]["output"], "file contents");
        assert!(!serde_json::to_string(&input)
            .unwrap()
            .contains("[Tool results]"));
    }

    #[test]
    fn responses_input_preserves_multimodal_attachments() {
        let input = responses_input(&[ChatMessage {
            role: "user".to_string(),
            content: "inspect these".to_string(),
            responses: Vec::new(),
            attachments: vec![
                ChatAttachment {
                    mime: "image/png".to_string(),
                    url: "data:image/png;base64,aW1n".to_string(),
                    filename: Some("screen.png".to_string()),
                },
                ChatAttachment {
                    mime: "application/pdf".to_string(),
                    url: "data:application/pdf;base64,cGRm".to_string(),
                    filename: Some("spec.pdf".to_string()),
                },
            ],
        }]);

        assert_eq!(input.len(), 1);
        assert_eq!(input[0]["content"][0]["type"], "input_text");
        assert_eq!(input[0]["content"][1]["type"], "input_image");
        assert_eq!(
            input[0]["content"][1]["image_url"],
            "data:image/png;base64,aW1n"
        );
        assert_eq!(input[0]["content"][2]["type"], "input_file");
        assert_eq!(input[0]["content"][2]["filename"], "spec.pdf");
        assert_eq!(
            input[0]["content"][2]["file_data"],
            "data:application/pdf;base64,cGRm"
        );
    }

    #[test]
    fn responses_input_synthesizes_missing_tool_output() {
        let input = responses_input(&[ChatMessage {
            role: "assistant".to_string(),
            content: String::new(),
            responses: vec![ChatResponseItem::FunctionCall(ChatToolCall {
                id: "call_orphan".to_string(),
                name: "task".to_string(),
                input: json!({ "description": "Map repo" }),
            })],
            attachments: Vec::new(),
        }]);

        assert_eq!(input.len(), 2);
        assert_eq!(input[0]["type"], "function_call");
        assert_eq!(input[0]["call_id"], "call_orphan");
        assert_eq!(input[1]["type"], "function_call_output");
        assert_eq!(input[1]["call_id"], "call_orphan");
        assert_eq!(input[1]["output"], "Tool call did not return an output");
    }

    #[test]
    fn responses_input_drops_orphan_tool_output() {
        let input = responses_input(&[ChatMessage {
            role: "tool".to_string(),
            content: String::new(),
            responses: vec![ChatResponseItem::FunctionOutput {
                id: "call_orphan".to_string(),
                output: "late output".to_string(),
            }],
            attachments: Vec::new(),
        }]);

        assert!(input.is_empty(), "orphan tool output must not be replayed");
    }

    #[test]
    fn tool_choice_requires_structured_output_tool() {
        let tools = vec![ChatTool {
            name: "StructuredOutput".to_string(),
            description: "Final JSON".to_string(),
            parameters: json!({ "type": "object" }),
        }];

        assert_eq!(tool_choice(&tools), "required");
    }

    #[test]
    fn tool_choice_defaults_to_auto_for_regular_tools() {
        let tools = vec![ChatTool {
            name: "read".to_string(),
            description: "Read file".to_string(),
            parameters: json!({ "type": "object" }),
        }];

        assert_eq!(tool_choice(&tools), "auto");
    }

    #[test]
    fn resolves_openai_oauth_to_codex_endpoint() {
        let _guard = env_guard();
        env::remove_var("OPENAI_BASE_URL");
        env::set_var(
            "KILO_AUTH_CONTENT",
            r#"{"openai":{"type":"oauth","access":"access-token","refresh":"refresh-token","expires":1,"accountId":"acct_1"}}"#,
        );
        let cfg = Config {
            data: BTreeMap::new(),
        };

        let req = resolve(
            &cfg,
            Some(&json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            vec![],
        )
        .unwrap();

        assert_eq!(req.base, CODEX_BASE);
        assert_eq!(
            req.auth,
            ChatAuth::Oauth {
                access: "access-token".to_string(),
                account: Some("acct_1".to_string())
            }
        );

        env::remove_var("KILO_AUTH_CONTENT");
    }

    #[test]
    fn resolves_persisted_auth_before_env_fallback() {
        let _guard = env_guard();
        env::set_var(
            "KILO_AUTH_CONTENT",
            r#"{"openai":{"type":"oauth","access":"env-token","refresh":"refresh-token","expires":1}}"#,
        );
        let cfg = Config {
            data: BTreeMap::new(),
        };

        let req = resolve_tools_with_auth(
            &cfg,
            &json!({
                "openai": {
                    "type": "oauth",
                    "access": "stored-token",
                    "refresh": "refresh-token",
                    "expires": 1,
                    "accountId": "acct_stored"
                }
            }),
            Some(&json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            Some("system text".to_string()),
            vec![],
            vec![],
        )
        .unwrap();

        assert_eq!(req.instructions, Some("system text".to_string()));
        assert_eq!(req.base, CODEX_BASE);
        assert_eq!(
            req.auth,
            ChatAuth::Oauth {
                access: "stored-token".to_string(),
                account: Some("acct_stored".to_string())
            }
        );

        env::remove_var("KILO_AUTH_CONTENT");
    }

    #[test]
    fn parses_openai_responses_response() {
        let out = parse(&json!({
            "output": [{
                "content": [
                    { "type": "output_text", "text": "real " },
                    { "type": "output_text", "text": "response" }
                ]
            }]
        }))
        .unwrap();

        assert_eq!(out.text, "real response");
        assert!(out.tool_calls.is_empty());
    }

    fn env_guard() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn decode_utf8_streaming_handles_split_em_dash() {
        // Audit Fix 1: em-dash U+2014 is `e2 80 94`. Split across chunks
        // such that one ends at `e2` and the next starts at `80 94`.
        let payload = "hello \u{2014} world\n";
        let bytes = payload.as_bytes();
        let split = bytes.iter().position(|b| *b == 0xe2).unwrap();
        let (a, b) = bytes.split_at(split + 1);
        let mut pending = Vec::new();
        let mut assembled = String::new();

        // First chunk: contains everything up to and including `e2`. The
        // decoder must defer the trailing `e2` and emit "hello ".
        pending.extend_from_slice(a);
        assembled.push_str(&decode_utf8_streaming(&mut pending).unwrap());
        assert!(!pending.is_empty(), "trailing e2 must be retained");

        // Second chunk: `80 94 ' world\n'`. Now the decoder can emit the
        // em-dash and the rest of the line.
        pending.extend_from_slice(b);
        assembled.push_str(&decode_utf8_streaming(&mut pending).unwrap());
        assert!(pending.is_empty(), "no leftover bytes at end");
        assert_eq!(assembled, payload);

        // And the SSE drain on the assembled buffer should produce a clean
        // text-delta event matching the unsplit payload.
        let mut buf = String::new();
        let frame = format!("data: {{\"type\":\"response.output_text.delta\",\"delta\":\"hello \u{2014} world\\n\"}}\n\n");
        buf.push_str(&frame);
        let events = drain_sse(&mut buf);
        let mut state = StreamState::default();
        let parsed = parse_stream_event(&events[0], &mut state).unwrap();
        assert_eq!(
            parsed[0],
            StreamEvent::TextDelta("hello \u{2014} world\n".to_string())
        );
    }

    #[test]
    fn decode_utf8_streaming_rejects_invalid_sequence() {
        // Hard error path: a continuation byte (`80`) with no leading byte
        // is unambiguously invalid, not just a truncated trailing codepoint.
        let mut pending = vec![b'a', 0x80, b'b'];
        assert!(decode_utf8_streaming(&mut pending).is_err());
    }

    #[test]
    fn decode_utf8_streaming_handles_clean_chunks() {
        let mut pending = b"plain ascii".to_vec();
        let out = decode_utf8_streaming(&mut pending).unwrap();
        assert_eq!(out, "plain ascii");
        assert!(pending.is_empty());
    }

    /// Audit Fix 5: when openai's auth row indicates `oauth`, the model
    /// list is filtered through `filter_codex_models` (admit-list +
    /// zero-cost stamp). API-key auth keeps the unfiltered list.
    #[test]
    fn list_with_oauth_auth_filters_codex_and_zeroes_cost() {
        let cfg = Config {
            data: BTreeMap::new(),
        };
        let auths = json!({
            "openai": {
                "type": "oauth",
                "access": "AT",
                "refresh": "RT",
                "expires": 1
            }
        });
        let result = list_with_auth(&cfg, &auths);
        let openai = result
            .all
            .iter()
            .find(|item| item.get("id").and_then(Value::as_str) == Some("openai"))
            .expect("openai entry");
        let models = openai["models"].as_object().unwrap();
        assert!(
            models.len() > 1,
            "OAuth registry should expose real choices"
        );
        assert_eq!(result.connected, vec!["openai"]);
        assert!(models.contains_key("gpt-5.1-codex"));
        assert!(models.contains_key("gpt-5.1-codex-max"));
        assert!(models.contains_key("gpt-5.1-codex-mini"));
        assert!(models.contains_key("gpt-5.2"));
        assert!(models.contains_key("gpt-5.4-mini"));
        assert!(models.contains_key("gpt-5.5"));
        assert!(!models.contains_key("gpt-5.1"));
        assert!(!models.contains_key("gpt-4.1"));
        assert!(!models.contains_key("o1-preview"));
        for model in models.values() {
            assert_eq!(model["cost"]["input"], 0);
            assert_eq!(model["cost"]["output"], 0);
            assert_eq!(model["cost"]["cache"]["read"], 0);
            assert_eq!(model["cost"]["cache"]["write"], 0);
        }
    }

    #[test]
    fn list_without_oauth_auth_does_not_zero_cost_or_filter() {
        let _guard = env_guard();
        env::remove_var("KILO_AUTH_CONTENT");
        env::remove_var("OPENAI_API_KEY");
        env::remove_var("OPENAI_BASE_URL");
        let cfg = Config {
            data: BTreeMap::new(),
        };
        // No `auths` blob — API-key path.
        let result = list_with_auth(&cfg, &Value::Null);
        let openai = result
            .all
            .iter()
            .find(|item| item.get("id").and_then(Value::as_str) == Some("openai"))
            .expect("openai entry");
        let models = openai["models"].as_object().unwrap();
        assert!(result.connected.is_empty());
        assert!(models.contains_key("gpt-5.1-codex"));
        assert!(models.contains_key("gpt-5.1"));
        assert!(models.contains_key("gpt-4.1"));
        assert!(models.contains_key("o1-preview"));
        assert_ne!(models["gpt-5.1-codex"]["cost"]["input"], 0);
        assert_ne!(models["gpt-4.1"]["cost"]["input"], 0);
    }

    #[test]
    fn list_keeps_openai_even_if_config_hides_it() {
        let cfg = Config {
            data: BTreeMap::from([
                ("disabled_providers".to_string(), json!(["openai"])),
                ("enabled_providers".to_string(), json!(["kilo"])),
            ]),
        };
        let result = list_with_auth(&cfg, &Value::Null);
        assert_eq!(result.all.len(), 1);
        assert_eq!(result.all[0]["id"], "openai");
        assert!(result.defaults.contains_key("openai"));
    }

    #[test]
    fn list_with_api_auth_does_not_filter_openai_registry() {
        let cfg = Config {
            data: BTreeMap::new(),
        };
        let auths = json!({
            "openai": {
                "type": "api",
                "key": "sk-test"
            }
        });
        let result = list_with_auth(&cfg, &auths);
        let openai = result
            .all
            .iter()
            .find(|item| item.get("id").and_then(Value::as_str) == Some("openai"))
            .expect("openai entry");
        let models = openai["models"].as_object().unwrap();
        assert_eq!(result.connected, vec!["openai"]);
        assert!(models.contains_key("gpt-5.1-codex"));
        assert!(models.contains_key("gpt-4.1"));
        assert!(models.contains_key("o1-preview"));
        assert_ne!(models["gpt-5.1-codex"]["cost"]["input"], 0);
    }

    /// Audit Fix 6: auth.json wins over `cfg.options.apiKey`. Both
    /// sources present → resolver returns OAuth and CODEX_BASE.
    #[test]
    fn resolver_prefers_oauth_in_auths_over_config_api_key() {
        let _g = env_guard();
        env::remove_var("KILO_AUTH_CONTENT");
        env::remove_var("OPENAI_API_KEY");
        env::remove_var("OPENAI_BASE_URL");
        let cfg = Config {
            data: BTreeMap::from([(
                "provider".to_string(),
                json!({
                    "openai": {
                        "options": { "apiKey": "config-key" }
                    }
                }),
            )]),
        };
        let auths = json!({
            "openai": {
                "type": "oauth",
                "access": "stored-access",
                "refresh": "rt",
                "expires": 1,
                "accountId": "acct_oauth"
            }
        });
        let req = resolve_tools_with_auth(
            &cfg,
            &auths,
            Some(&json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            None,
            vec![],
            vec![],
        )
        .unwrap();
        assert_eq!(
            req.auth,
            ChatAuth::Oauth {
                access: "stored-access".to_string(),
                account: Some("acct_oauth".to_string())
            }
        );
        assert_eq!(req.base, CODEX_BASE);
    }
}
