//! Route handlers for /config*, /provider*, /auth/{providerID}, and the two
//! OAuth handlers (`oauth_authorize`, `oauth_callback`). The OAuth handlers
//! are thin: they pull state, dispatch into `crate::oauth::*`, and shape the
//! response. The substantive listener / token-exchange / PKCE / JWT logic
//! lives in `crate::oauth::{listener,tokens,crypto,url}`.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use kilo_protocol::{Config, GlobalEvent};
use serde_json::{json, Map, Value};

use crate::oauth::crypto::{oauth_secret, pkce_challenge};
use crate::oauth::listener::ensure_oauth_listener;
use crate::oauth::tokens::exchange_code;
use crate::oauth::url::oauth_url;
use crate::oauth::{OAUTH_PENDING_TTL, OPENAI_REDIRECT};
use crate::{http::sse, internal_error, internal_error_named, AppState, PendingAuth};

#[cfg(not(test))]
const OAUTH_CALLBACK_WAIT: Duration = OAUTH_PENDING_TTL;
#[cfg(test)]
const OAUTH_CALLBACK_WAIT: Duration = Duration::from_millis(25);

fn bad_request_named(name: &str, message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "name": name,
            "data": { "message": message },
        })),
    )
        .into_response()
}

fn oauth_method(input: &Value) -> Option<&str> {
    match input.get("method") {
        Some(Value::String(value)) => Some(value.as_str()),
        Some(Value::Number(value)) if value.as_u64() == Some(0) => Some("auto"),
        None => Some("auto"),
        _ => None,
    }
}

pub(crate) async fn config(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.store.config())
}

/// `PATCH /global/config` — Bun shape: body is the full Config; response is
/// the post-merge Config. Persists to whichever file `Store::set_config`
/// resolves via `config_priority()` (matching Bun's `globalConfigFile()`),
/// so a subsequent GET round-trips. Failure path is 500 with a Bun-shaped
/// JSON envelope via `internal_error` — the M3 mutation gate has already
/// flipped by the time we reach the handler, so the error must be both
/// observable and parseable by the SDK's `response.json()` path.
pub(crate) async fn update_config(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Config>,
) -> Response {
    let mut current = state.store.config();
    merge_config(&mut current, input);
    let out = match state.store.set_config(current) {
        Ok(value) => value,
        Err(err) => return internal_error(err.to_string()),
    };
    sse::publish(&state, GlobalEvent::bus("global.config.updated", json!({})));
    Json(out).into_response()
}

fn merge_config(target: &mut Config, patch: Config) {
    for (key, value) in patch.data {
        if value.is_null() {
            target.data.remove(&key);
            continue;
        }
        match target.data.get_mut(&key) {
            Some(existing) => merge_value(existing, value),
            None => {
                target.data.insert(key, value);
            }
        }
    }
}

fn merge_value(target: &mut Value, patch: Value) {
    match (target, patch) {
        (Value::Object(target), Value::Object(patch)) => merge_object(target, patch),
        (target, patch) => *target = patch,
    }
}

fn merge_object(target: &mut Map<String, Value>, patch: Map<String, Value>) {
    for (key, value) in patch {
        if value.is_null() {
            target.remove(&key);
            continue;
        }
        match target.get_mut(&key) {
            Some(existing) => merge_value(existing, value),
            None => {
                target.insert(key, value);
            }
        }
    }
}

pub(crate) async fn providers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // Audit Fix 5: pass the persisted auth blob so the OAuth-only Codex
    // model filter and zero-cost stamp run when the openai entry has an
    // `oauth` auth row in `auth.json`. API-key auth keeps the unfiltered
    // upstream list unchanged.
    let auths = json!(state.store.provider_auths());
    Json(kilo_provider::list_with_auth(&state.store.config(), &auths))
}

pub(crate) async fn provider_auth() -> impl IntoResponse {
    Json(json!({
        "openai": [
            { "label": "ChatGPT Pro/Plus (browser)", "type": "oauth" }
        ]
    }))
}

/// `GET /provider/{providerID}` — used by the sidebar's provider sign-in
/// flow to look up env/option metadata before launching auth. Returns 404
/// if the provider isn't in the static Kilo list (today: only `kilo`).
pub(crate) async fn provider_detail(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match kilo_provider::detail(&state.store.config(), &id) {
        Some(value) => Json(value).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// `PUT /auth/{providerID}` — Bun stores the auth blob and returns it.
/// M5 stub: accept and echo the body so the SDK's optimistic
/// post-sign-in flow doesn't break, but persistence is M10 territory.
/// The SDK type is `Auth`; we accept any JSON object and round-trip it.
pub(crate) async fn set_auth(
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
pub(crate) async fn clear_auth(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.clear_provider_auth(&id) {
        Ok(()) => Json(true).into_response(),
        Err(err) => internal_error(err.to_string()),
    }
}

pub(crate) async fn config_providers(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // Audit Fix 5: same OAuth-aware filter as `providers()`.
    let auths = json!(state.store.provider_auths());
    Json(kilo_provider::config_with_auth(
        &state.store.config(),
        &auths,
    ))
}

pub(crate) async fn oauth_authorize(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<Value>,
) -> Response {
    if id != "openai" {
        return bad_request_named(
            "OauthUnsupportedProvider",
            "OAuth is only supported for openai.",
        );
    }
    if oauth_method(&input) != Some("auto") {
        return bad_request_named(
            "OauthUnsupportedMethod",
            "OpenAI OAuth only supports the auto method.",
        );
    }
    let verifier = oauth_secret(43);
    let csrf = oauth_secret(32);
    let challenge = pkce_challenge(&verifier);
    let url = oauth_url(OPENAI_REDIRECT, &challenge, &csrf);
    if let Err(err) = ensure_oauth_listener(state.clone()).await {
        return internal_error_named("OauthCallbackListenerError", err.to_string());
    }

    // Audit Fix 3: stash the verifier and state server-side, keyed by
    // provider id, so the SDK's `oauth_callback` posting only `{ method,
    // code, state }` can complete the flow without echoing the verifier
    // back. Reap stale entries on insert to bound memory of unfinished
    // flows.
    {
        let mut pending = state.oauth_pending.lock().unwrap();
        let now = Instant::now();
        pending.retain(|_, entry| entry.expires_at > now);
        let (complete, _) = tokio::sync::watch::channel(None);
        if let Some(old) = pending.insert(
            id.clone(),
            PendingAuth {
                verifier: verifier.clone(),
                state: csrf.clone(),
                expires_at: now + OAUTH_PENDING_TTL,
                complete,
            },
        ) {
            let _ = old.complete.send(Some(Err(
                "OAuth authorization was replaced; restart authorization.".to_string(),
            )));
        }
    }

    // We also continue to surface `verifier` in the response so older
    // clients that posted `{ verifier }` on callback keep working
    // (backwards-compat). New clients ignore it.
    Json(json!({
        "url": url,
        "instructions": "Complete authorization in your browser, then return to Kilo.",
        "method": "auto",
        "state": csrf,
        "verifier": verifier,
        "redirectUri": OPENAI_REDIRECT,
    }))
    .into_response()
}

pub(crate) async fn oauth_callback(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(input): Json<Value>,
) -> Response {
    if id != "openai" {
        return bad_request_named(
            "OauthUnsupportedProvider",
            "OAuth is only supported for openai.",
        );
    }
    let method = oauth_method(&input);
    let code = input.get("code").and_then(Value::as_str);
    if code.is_none() && method == Some("auto") {
        return wait_for_oauth_callback(state, &id, &input).await;
    }
    let Some(code) = code else {
        return bad_request_named(
            "OauthCodeMissing",
            "OAuth callback is missing an authorization code.",
        );
    };
    if method.is_none() {
        return bad_request_named(
            "OauthUnsupportedMethod",
            "OpenAI OAuth only supports the auto method.",
        );
    }

    // Backwards-compat path: a client may still post `{verifier}` (or
    // `{inputs:{verifier}}`) on callback. Accept that and skip the pending
    // lookup. The canonical Bun-compat path posts only `{code, state}` and
    // we resolve the verifier from `state.oauth_pending`.
    let body_verifier = input
        .get("inputs")
        .and_then(|value| value.get("verifier"))
        .or_else(|| input.get("verifier"))
        .and_then(Value::as_str)
        .map(str::to_string);

    let body_state = input
        .get("state")
        .and_then(Value::as_str)
        .map(str::to_string);

    let (verifier, complete) = if let Some(verifier) = body_verifier {
        // Caller supplied verifier directly. Best-effort: also pop the
        // pending entry by id so it doesn't leak.
        let entry = remove_pending_oauth_if_state_matches(&state, &id, body_state.as_deref());
        (verifier, entry.map(|entry| entry.complete))
    } else {
        // Server-side lookup. Reap stale, then look up by provider id.
        let entry = match take_pending_oauth(&state, &id, body_state.as_deref()) {
            PendingTake::Taken(entry) => entry,
            PendingTake::Missing => {
                return bad_request_named(
                    "OauthPendingMissing",
                    "No pending OAuth flow; call /provider/{id}/oauth/authorize first.",
                );
            }
            PendingTake::StateMismatch => {
                return bad_request_named(
                    "OauthStateMismatch",
                    "OAuth callback state did not match the pending flow.",
                );
            }
        };
        (entry.verifier, Some(entry.complete))
    };

    complete_oauth_callback(state, code, &verifier, complete).await
}

pub(crate) async fn complete_browser_oauth_callback(
    state: Arc<AppState>,
    code: &str,
    csrf: &str,
) -> Response {
    let entry = match take_pending_oauth(&state, "openai", Some(csrf)) {
        PendingTake::Taken(entry) => entry,
        PendingTake::Missing => {
            return bad_request_named(
                "OauthPendingMissing",
                "No pending OAuth flow; call /provider/{id}/oauth/authorize first.",
            );
        }
        PendingTake::StateMismatch => {
            return bad_request_named(
                "OauthStateMismatch",
                "OAuth callback state did not match the pending flow.",
            );
        }
    };
    complete_oauth_callback(state, code, &entry.verifier, Some(entry.complete)).await
}

enum PendingTake {
    Taken(PendingAuth),
    Missing,
    StateMismatch,
}

fn take_pending_oauth(state: &Arc<AppState>, id: &str, csrf: Option<&str>) -> PendingTake {
    let mut pending = state.oauth_pending.lock().unwrap();
    let now = Instant::now();
    pending.retain(|_, entry| entry.expires_at > now);
    let Some(entry) = pending.get(id) else {
        return PendingTake::Missing;
    };
    if csrf != Some(entry.state.as_str()) {
        return PendingTake::StateMismatch;
    }
    pending
        .remove(id)
        .map(PendingTake::Taken)
        .unwrap_or(PendingTake::Missing)
}

fn remove_pending_oauth_if_state_matches(
    state: &Arc<AppState>,
    id: &str,
    csrf: Option<&str>,
) -> Option<PendingAuth> {
    let mut pending = state.oauth_pending.lock().unwrap();
    let now = Instant::now();
    pending.retain(|_, entry| entry.expires_at > now);
    if let Some(csrf) = csrf {
        let Some(entry) = pending.get(id) else {
            return None;
        };
        if csrf != entry.state {
            return None;
        }
    }
    pending.remove(id)
}

async fn wait_for_oauth_callback(state: Arc<AppState>, id: &str, input: &Value) -> Response {
    let body_state = input
        .get("state")
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut rx = {
        let mut pending = state.oauth_pending.lock().unwrap();
        let now = Instant::now();
        pending.retain(|_, entry| entry.expires_at > now);
        let Some(entry) = pending.get(id) else {
            return bad_request_named(
                "OauthPendingMissing",
                "No pending OAuth flow; call /provider/{id}/oauth/authorize first.",
            );
        };
        if body_state
            .as_deref()
            .is_some_and(|value| value != entry.state)
        {
            return bad_request_named(
                "OauthStateMismatch",
                "OAuth callback state did not match the pending flow.",
            );
        }
        entry.complete.subscribe()
    };
    let wait = async {
        loop {
            if let Some(done) = rx.borrow().clone() {
                return match done {
                    Ok(()) => Json(true).into_response(),
                    Err(err) => internal_error_named("OauthCallbackError", err),
                };
            }
            if rx.changed().await.is_err() {
                return internal_error_named(
                    "OauthCallbackError",
                    "OAuth callback waiter closed before completion.",
                );
            }
        }
    };
    tokio::time::timeout(OAUTH_CALLBACK_WAIT, wait)
        .await
        .unwrap_or_else(|_| {
            bad_request_named(
                "OauthCallbackTimeout",
                "Timed out waiting for browser OAuth callback; restart authorization and try again.",
            )
        })
}

async fn complete_oauth_callback(
    state: Arc<AppState>,
    code: &str,
    verifier: &str,
    complete: Option<tokio::sync::watch::Sender<Option<Result<(), String>>>>,
) -> Response {
    // The OAuth callback handler has no Stop button to bind to — the user
    // is in their browser. The 30s reqwest timeout in `token_client` is
    // the safety net here.
    let cancel = AtomicBool::new(false);
    let res = match exchange_code(
        &state.oauth_token_endpoint,
        code,
        OPENAI_REDIRECT,
        verifier,
        &cancel,
    )
    .await
    {
        Ok(value) => state
            .store
            .set_provider_auth("openai", value)
            .map(|_| ())
            .map_err(|err| err.to_string()),
        Err(err) => Err(err),
    };
    if let Some(tx) = complete {
        let _ = tx.send(Some(res.clone()));
    }
    match res {
        Ok(()) => Json(true).into_response(),
        Err(err) => internal_error_named("OauthCallbackError", err),
    }
}
