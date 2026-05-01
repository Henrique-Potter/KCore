use std::{collections::BTreeSet, env, fmt, sync::atomic::AtomicBool, time::Duration};

use futures_util::StreamExt;
use kilo_protocol::{Config, ConfigProvidersResult, ProviderResult};
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

#[derive(Clone, Debug, PartialEq)]
pub struct ChatRequest {
    pub provider: String,
    pub model: String,
    pub base: String,
    pub auth: ChatAuth,
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

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ChatTool {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatUsage {
    pub input: u64,
    pub output: u64,
    pub total: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChatParsed {
    pub text: String,
    pub tool_calls: Vec<ChatToolCall>,
    pub usage: Option<ChatUsage>,
    pub finish: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum StreamEvent {
    TextDelta(String),
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
    Response(String),
    /// The cancel flag was tripped while a request was in-flight (during
    /// HTTP connect/headers or while awaiting a stream chunk). Surfaced by
    /// [`post_stream`] when the `until_cancel` race wins. kilo-server maps
    /// this to the `MessageAbortedError` envelope via `aborted_error()`.
    Aborted,
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
            Self::Response(err) => write!(f, "provider response error: {err}"),
            Self::Aborted => write!(f, "provider request aborted"),
        }
    }
}

impl std::error::Error for ProviderError {}

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
    instructions: Option<String>,
    messages: Vec<ChatMessage>,
    tools: Vec<ChatTool>,
    cancel: &AtomicBool,
    mut emit: impl FnMut(StreamEvent),
) -> Result<ChatOutput, ProviderError> {
    let req = resolve_tools_with_auth(cfg, auths, model, instructions, messages, tools)?;
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
    let auth = provider_option(cfg, &provider, "apiKey")
        .map(|key| ChatAuth::Api { key })
        .or_else(|| auth_value(auths, &provider))
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
        instructions,
        messages,
        tools,
    })
}

pub fn parse(body: &Value) -> Result<ChatParsed, ProviderError> {
    if let Some(text) = parse_response_text(body) {
        return Ok(ChatParsed {
            text,
            tool_calls: parse_response_tool_calls(body),
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
    let enabled = strings(cfg.data.get("enabled_providers"));
    let disabled = strings(cfg.data.get("disabled_providers"));
    let mut all = vec![
        provider(
            "kilo",
            "Kilo",
            "big-pickle",
            "Big Pickle",
            "@kilocode/kilo-gateway",
        ),
        provider(
            "openai",
            "OpenAI",
            "gpt-5.1-codex",
            "GPT-5.1 Codex",
            "openai",
        ),
    ];

    all.retain(|item| {
        let id = item.get("id").and_then(Value::as_str).unwrap_or_default();
        (enabled.is_empty() || enabled.contains(id)) && !disabled.contains(id)
    });

    let defaults = all
        .iter()
        .filter_map(|item| {
            let id = item.get("id")?.as_str()?;
            let models = item.get("models")?.as_object()?;
            let first = models.keys().next()?.to_string();
            Some((id.to_string(), first))
        })
        .collect();

    ProviderResult {
        all,
        defaults,
        connected: vec![],
    }
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
    let data = list(cfg);
    ConfigProvidersResult {
        providers: data.all,
        defaults: data.defaults,
    }
}

fn provider(id: &str, name: &str, model: &str, label: &str, npm: &str) -> Value {
    json!({
        "id": id,
        "name": name,
        "source": "custom",
        "env": [],
        "options": {},
        "models": {
            model: {
                "id": model,
                "providerID": id,
                "api": { "id": model, "url": "", "npm": npm },
                "name": label,
                "capabilities": {
                    "temperature": true,
                    "reasoning": false,
                    "attachment": true,
                    "toolcall": true,
                    "input": { "text": true, "audio": false, "image": true, "video": false, "pdf": true },
                    "output": { "text": true, "audio": false, "image": false, "video": false, "pdf": false },
                    "interleaved": false
                },
                "cost": { "input": 0, "output": 0, "cache": { "read": 0, "write": 0 } },
                "limit": { "context": 200000, "input": 200000, "output": 8192 },
                "status": "active",
                "options": {},
                "headers": {},
                "release_date": "",
                "variants": {}
            }
        }
    })
}

fn strings(value: Option<&Value>) -> BTreeSet<&str> {
    value
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

async fn post(req: &ChatRequest) -> Result<ChatParsed, ProviderError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .map_err(|err| ProviderError::Http(err.to_string()))?;
    let res = match &req.auth {
        ChatAuth::Oauth { access, account } if req.provider == "openai" => {
            let url = format!("{}/responses", req.base.trim_end_matches('/'));
            let mut call = client
                .post(url)
                .bearer_auth(access)
                .header("originator", "opencode")
                .header("User-Agent", "opencode/rust-sidecar")
                .json(&json!({
                    "model": req.model,
                    "instructions": req.instructions,
                    "input": responses_input(&req.messages),
                    "stream": false,
                    "store": false,
                }));
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
                body["tool_choice"] = json!("auto");
            }
            client.post(url).bearer_auth(key).json(&body).send().await
        }
    }
    .map_err(|err| ProviderError::Http(err.to_string()))?;
    let status = res.status();
    let body = res
        .json::<Value>()
        .await
        .map_err(|err| ProviderError::Response(err.to_string()))?;

    if !status.is_success() {
        return Err(ProviderError::Api(api_message(&body)));
    }

    parse(&body)
}

/// Polling helper used by [`post_stream`] to race against cancellation.
/// Resolves only when `cancel` is set. We poll on a short interval rather
/// than swap to `tokio_util::sync::CancellationToken` because the rest of
/// kilo-server runs on `Arc<AtomicBool>` (~50 sites); converting now would
/// be a coordinated change that's out of scope for the M7 OAuth fix. The
/// 10ms cadence matches the existing `wait_fake` helper in kilo-server.
async fn until_cancel(cancel: &AtomicBool) {
    while !cancel.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn post_stream(
    req: &ChatRequest,
    cancel: &AtomicBool,
    emit: &mut impl FnMut(StreamEvent),
) -> Result<ChatParsed, ProviderError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .map_err(|err| ProviderError::Http(err.to_string()))?;
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
        body["tool_choice"] = json!("auto");
    }
    let mut call = client
        .post(url)
        .bearer_auth(access)
        .header("originator", "opencode")
        .header("User-Agent", "opencode/rust-sidecar")
        .json(&body);
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
        let body = res.json::<Value>().await.unwrap_or_else(|_| json!({}));
        return Err(ProviderError::Api(api_message(&body)));
    }

    let mut state = StreamState::default();
    let mut buf = String::new();
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
        let text =
            std::str::from_utf8(&chunk).map_err(|err| ProviderError::Response(err.to_string()))?;
        buf.push_str(text);
        for event in drain_sse(&mut buf) {
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

fn responses_input(messages: &[ChatMessage]) -> Vec<Value> {
    messages
        .iter()
        .map(|msg| {
            json!({
                "role": msg.role,
                "content": [{
                    "type": if msg.role == "assistant" { "output_text" } else { "input_text" },
                    "text": msg.content,
                }]
            })
        })
        .collect()
}

fn parse_response_text(body: &Value) -> Option<String> {
    let text = body
        .get("output")?
        .as_array()?
        .iter()
        .flat_map(|item| {
            item.get("content")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("");

    (!text.is_empty()).then_some(text)
}

#[derive(Default)]
struct StreamState {
    text: String,
    tools: Vec<PartialTool>,
    calls: Vec<ChatToolCall>,
    usage: Option<ChatUsage>,
    finish: Option<String>,
}

#[derive(Default)]
struct PartialTool {
    id: String,
    name: Option<String>,
    args: String,
}

pub fn parse_stream(input: &str) -> Result<Vec<StreamEvent>, ProviderError> {
    let mut state = StreamState::default();
    let mut out = Vec::new();
    for event in input.split("\n\n") {
        out.extend(parse_stream_event(event, &mut state)?);
    }
    Ok(out)
}

fn drain_sse(buf: &mut String) -> Vec<String> {
    let mut out = Vec::new();
    while let Some(idx) = sse_break(buf) {
        let tail = if buf[idx..].starts_with("\r\n\r\n") {
            4
        } else {
            2
        };
        let event = buf[..idx].to_string();
        buf.drain(..idx + tail);
        out.push(event);
    }
    out
}

fn sse_break(text: &str) -> Option<usize> {
    text.find("\n\n").or_else(|| text.find("\r\n\r\n"))
}

fn parse_stream_event(
    raw: &str,
    state: &mut StreamState,
) -> Result<Vec<StreamEvent>, ProviderError> {
    let data = raw
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim_start)
        .collect::<Vec<_>>()
        .join("\n");
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
                part.args.push_str(args);
            }
            if kind.ends_with("done") {
                let call = finish_tool(part);
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
    let delta = first_str(value, &["delta", "arguments_delta", "arguments"])
        .unwrap_or_default()
        .to_string();
    part.args.push_str(&delta);
    if kind.ends_with("done") {
        let call = finish_tool(part);
        state.calls.push(call.clone());
        return vec![StreamEvent::ToolCall(call)];
    }
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

fn finish_tool(part: &PartialTool) -> ChatToolCall {
    let name = part.name.clone().unwrap_or_else(|| "unknown".to_string());
    let id = if part.id.is_empty() {
        name.clone()
    } else {
        part.id.clone()
    };
    let input = serde_json::from_str(&part.args).unwrap_or_else(|_| json!({}));
    ChatToolCall { id, name, input }
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
        .map(|items| items.iter().filter_map(parse_response_tool_call).collect())
        .unwrap_or_default()
}

fn parse_response_tool_call(item: &Value) -> Option<ChatToolCall> {
    if item.get("type").and_then(Value::as_str)? != "function_call" {
        return None;
    }
    let name = item.get("name")?.as_str()?.to_string();
    let id = item
        .get("call_id")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .unwrap_or(&name)
        .to_string();
    let input = item
        .get("arguments")
        .and_then(Value::as_str)
        .and_then(|args| serde_json::from_str(args).ok())
        .unwrap_or_else(|| json!({}));
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
    Some(ChatUsage {
        input,
        output,
        total,
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
    content
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

fn parse_tool_calls(msg: &Value) -> Vec<ChatToolCall> {
    msg.get("tool_calls")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(parse_tool_call).collect())
        .unwrap_or_default()
}

fn parse_tool_call(item: &Value) -> Option<ChatToolCall> {
    let fun = item.get("function")?;
    let name = fun.get("name")?.as_str()?.to_string();
    let id = item
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or(&name)
        .to_string();
    let input = fun
        .get("arguments")
        .and_then(Value::as_str)
        .and_then(|args| serde_json::from_str(args).ok())
        .unwrap_or_else(|| json!({}));

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
    use std::collections::BTreeMap;
    use std::sync::{Mutex, MutexGuard, OnceLock};

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
            })));
        assert!(events
            .iter()
            .any(|event| event == &StreamEvent::Finish("stop".to_string())));
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
}
