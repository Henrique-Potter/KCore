//! OAuth token exchange + refresh + auth-blob shaping.
//!
//! `exchange_code` runs once after the browser redirect; `refresh_access`
//! runs whenever the persisted access token has expired (driven by
//! `fresh_auths`, called from the agent turn loop). `merge_token_auth`
//! preserves any non-token fields (`enterpriseUrl`, future fields) across a
//! refresh — Audit Fix 7.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde_json::{json, Value};

use crate::util::encoding::unix_millis;
use crate::AppState;

use super::crypto::{claim_account, jwt_claims};
use super::url::url_encode;
use super::OPENAI_CLIENT_ID;

/// 30s cap on the token endpoint. Independent of the cancel race so a
/// stuck endpoint with no Stop press still surfaces a timeout instead of
/// hanging indefinitely.
const TOKEN_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Polling helper mirroring `kilo_provider::until_cancel`. Used to race
/// the synchronous `reqwest` send/json futures against an `AtomicBool`
/// cancel signal at 10ms cadence.
async fn until_cancel(cancel: &AtomicBool) {
    while !cancel.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn token_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .timeout(TOKEN_HTTP_TIMEOUT)
        .build()
        .map_err(|err| err.to_string())
}

pub(crate) async fn exchange_code(
    endpoint: &str,
    code: &str,
    redirect: &str,
    verifier: &str,
    cancel: &AtomicBool,
) -> Result<Value, String> {
    let body = format!(
        "grant_type=authorization_code&code={}&redirect_uri={}&client_id={OPENAI_CLIENT_ID}&code_verifier={}",
        url_encode(code),
        url_encode(redirect),
        url_encode(verifier),
    );
    let client = token_client()?;
    let send = client
        .post(endpoint)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send();
    let res = tokio::select! {
        _ = until_cancel(cancel) => return Err("aborted".to_string()),
        res = send => res.map_err(|err| err.to_string())?,
    };
    if !res.status().is_success() {
        return Err(format!("Token exchange failed: {}", res.status()));
    }
    let body = res.json::<Value>();
    let tokens = tokio::select! {
        _ = until_cancel(cancel) => return Err("aborted".to_string()),
        tokens = body => tokens.map_err(|err| err.to_string())?,
    };
    token_auth(&tokens, None)
}

pub(crate) async fn refresh_access(
    endpoint: &str,
    existing: &Value,
    refresh: &str,
    account: Option<String>,
    cancel: &AtomicBool,
) -> Result<Value, String> {
    let body = format!(
        "grant_type=refresh_token&refresh_token={}&client_id={OPENAI_CLIENT_ID}",
        url_encode(refresh),
    );
    let client = token_client()?;
    let send = client
        .post(endpoint)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send();
    let res = tokio::select! {
        _ = until_cancel(cancel) => return Err("aborted".to_string()),
        res = send => res.map_err(|err| err.to_string())?,
    };
    if !res.status().is_success() {
        return Err(format!("Token refresh failed: {}", res.status()));
    }
    let body = res.json::<Value>();
    let tokens = tokio::select! {
        _ = until_cancel(cancel) => return Err("aborted".to_string()),
        tokens = body => tokens.map_err(|err| err.to_string())?,
    };
    // Audit Fix 7: merge fresh tokens into the existing auth blob so non-token
    // fields (`enterpriseUrl`, future fields) survive the refresh.
    merge_token_auth(existing, &tokens, account)
}

pub(crate) async fn fresh_auths(state: &AppState, cancel: &AtomicBool) -> Result<Value, String> {
    let mut auths = state.store.provider_auths();
    let Some(auth) = auths.get("openai") else {
        return Ok(json!(auths));
    };
    if auth.get("type").and_then(Value::as_str) != Some("oauth") {
        return Ok(json!(auths));
    }
    if !oauth_access_stale(auth) {
        return Ok(json!(auths));
    }

    let _guard = state.oauth_refresh.lock().await;
    auths = state.store.provider_auths();
    let Some(auth) = auths.get("openai") else {
        return Ok(json!(auths));
    };
    if auth.get("type").and_then(Value::as_str) != Some("oauth") {
        return Ok(json!(auths));
    }
    if !oauth_access_stale(auth) {
        return Ok(json!(auths));
    }

    let refresh = auth
        .get("refresh")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if refresh.is_empty() {
        return Err(
            "OpenAI OAuth access token is expired or invalid and no refresh token is available. Please sign in again."
                .to_string(),
        );
    }
    let account = auth
        .get("accountId")
        .and_then(Value::as_str)
        .map(str::to_string);
    let next = refresh_access(&state.oauth_token_endpoint, auth, refresh, account, cancel).await?;
    state
        .store
        .set_provider_auth("openai", next.clone())
        .map_err(|err| err.to_string())?;
    auths.insert("openai".to_string(), next);
    Ok(json!(auths))
}

fn oauth_access_stale(auth: &Value) -> bool {
    let now = unix_millis();
    if auth.get("expires").and_then(Value::as_i64).unwrap_or(0) <= now {
        return true;
    }
    let Some(access) = auth.get("access").and_then(Value::as_str) else {
        return true;
    };
    let Some(claims) = jwt_claims(access) else {
        return true;
    };
    let Some(exp) = claims.get("exp").and_then(Value::as_i64) else {
        return false;
    };
    exp * 1000 <= now + 60_000
}

pub(crate) fn token_auth(tokens: &Value, account: Option<String>) -> Result<Value, String> {
    merge_token_auth(&Value::Null, tokens, account)
}

/// Audit Fix 7: merge a fresh token-endpoint response into an existing
/// auth blob, preserving any non-token fields (`enterpriseUrl`, future
/// fields). When `existing` is `Null`, behaves like the previous
/// "rebuild from scratch" path used by the initial code-exchange.
pub(crate) fn merge_token_auth(
    existing: &Value,
    tokens: &Value,
    account: Option<String>,
) -> Result<Value, String> {
    let expires = tokens
        .get("expires_in")
        .and_then(Value::as_i64)
        .unwrap_or(3600);
    let now = unix_millis();
    let refresh = tokens
        .get("refresh_token")
        .or_else(|| tokens.get("refresh"))
        .and_then(Value::as_str)
        .or_else(|| existing.get("refresh").and_then(Value::as_str))
        .unwrap_or_default();
    let access = tokens
        .get("access_token")
        .or_else(|| tokens.get("access"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "Token response missing access token".to_string())?;
    if refresh.trim().is_empty() {
        return Err("Token response missing refresh token".to_string());
    }

    // Start from the existing auth blob (preserving non-token fields), or
    // an empty object if there isn't one. Then stamp the fresh token
    // fields on top.
    let mut auth = match existing {
        Value::Object(_) => existing.clone(),
        _ => json!({}),
    };
    if let Some(obj) = auth.as_object_mut() {
        obj.insert("type".to_string(), json!("oauth"));
        obj.insert("refresh".to_string(), json!(refresh));
        obj.insert("access".to_string(), json!(access));
        obj.insert("expires".to_string(), json!(now + expires * 1000));
        if let Some(account) = extract_account(tokens).or(account) {
            obj.insert("accountId".to_string(), json!(account));
        }
    }
    Ok(auth)
}

pub(crate) fn extract_account(tokens: &Value) -> Option<String> {
    // Audit Fix 2: try `id_token` first, but if its JWT decodes but lacks
    // the claim (or fails to decode at all), fall back to `access_token`.
    // Mirrors the same chain in [`openai_oauth::parse_token_response`].
    let claim_account = |token: &str| jwt_claims(token).and_then(claim_account);
    tokens
        .get("id_token")
        .and_then(Value::as_str)
        .and_then(claim_account)
        .or_else(|| {
            tokens
                .get("access_token")
                .and_then(Value::as_str)
                .and_then(claim_account)
        })
}
