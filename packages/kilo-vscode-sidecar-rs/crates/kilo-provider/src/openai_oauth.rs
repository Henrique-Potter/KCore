//! OpenAI Pro / ChatGPT Plus OAuth (PKCE) flow.
//!
//! Mirrors the Bun source-of-truth at
//! [`packages/opencode/src/plugin/codex.ts:23-129`](../../../../../opencode/src/plugin/codex.ts:23)
//! (PKCE + token exchange) and
//! [`codex.ts:404-465`](../../../../../opencode/src/plugin/codex.ts:404)
//! (refresh + ChatGPT-Account-Id extraction).
//!
//! What this module owns:
//!
//! - Generating an RFC 7636-compliant code verifier and S256 challenge.
//! - A CSRF state nonce.
//! - Building the authorize URL with the exact custom params Bun uses
//!   (`id_token_add_organizations=true`, `codex_cli_simplified_flow=true`,
//!   `originator=opencode`).
//! - Posting `authorization_code` and `refresh_token` grants to
//!   `https://auth.openai.com/oauth/token`.
//! - Extracting `chatgpt_account_id` (or organizations[0].id, or the
//!   namespaced `https://api.openai.com/auth.chatgpt_account_id` claim)
//!   from the `id_token` JWT payload.
//! - A loopback HTTP server on port `1455` that receives the redirect
//!   from `auth.openai.com` and resolves the captured `code`.
//!
//! What this module deliberately does NOT own:
//!
//! - Persistence. The caller (kilo-server's `set_provider_auth`) is
//!   responsible for round-tripping the token blob to `auth.json`.
//! - Browser launching. The caller decides how to open the auth URL.
//! - Retry policy. Token endpoint failures bubble up as
//!   [`OAuthError`].
//!
//! Tests in this file deliberately do not make real network calls. The
//! token exchange/refresh helpers are split into a pure parser
//! ([`parse_token_response`]) and a thin reqwest-driven wrapper
//! ([`exchange_code`] / [`refresh_token`]) so unit tests can exercise
//! the parsing logic without standing up a stub server.

use base64::{engine::general_purpose, Engine};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

/// Bun's hard-coded OpenAI Codex client id. Source:
/// [`codex.ts:12`](../../../../../opencode/src/plugin/codex.ts:12).
pub const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
pub const ISSUER: &str = "https://auth.openai.com";
pub const REDIRECT_URI: &str = "http://localhost:1455/auth/callback";
pub const LOOPBACK_PORT: u16 = 1455;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OAuthError {
    /// Reqwest could not reach the token endpoint or the body wasn't valid JSON.
    Transport(String),
    /// The token endpoint returned a non-2xx status. Body included for diagnostics.
    Status { status: u16, body: String },
    /// CSRF state mismatch on the loopback callback.
    StateMismatch,
    /// Parser couldn't find required fields in the JSON token payload.
    Malformed(String),
}

impl std::fmt::Display for OAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(err) => write!(f, "OAuth transport error: {err}"),
            Self::Status { status, body } => write!(f, "OAuth HTTP {status}: {body}"),
            Self::StateMismatch => write!(f, "OAuth CSRF state mismatch"),
            Self::Malformed(err) => write!(f, "OAuth malformed token response: {err}"),
        }
    }
}

impl std::error::Error for OAuthError {}

/// PKCE code verifier + S256 challenge pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkceCodes {
    pub verifier: String,
    pub challenge: String,
}

/// Outbound state for an in-flight OAuth begin. The caller hangs onto this
/// across the redirect; matching the returned `state` on the callback is
/// CSRF-critical.
#[derive(Debug, Clone)]
pub struct OAuthBegin {
    pub verifier: String,
    pub challenge: String,
    pub state: String,
    pub redirect_uri: String,
    pub url: String,
}

/// Parsed token response. Mirrors the persisted shape Bun stores in
/// `auth.json`: see
/// [`codex.ts:430-438`](../../../../../opencode/src/plugin/codex.ts:430).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access: String,
    pub refresh: String,
    pub expires_at: i64,
    pub account_id: Option<String>,
}

/// Generate an RFC 7636 §4.1 code verifier: 43-character URL-safe charset
/// drawn from `[A-Z][a-z][0-9]-._~`. Bun's
/// [`generateRandomString`](../../../../../opencode/src/plugin/codex.ts:32)
/// uses the same alphabet and length. We seed from `OsRng` for CSPRNG
/// guarantees.
pub fn generate_verifier() -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
    let mut bytes = [0u8; 43];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    bytes
        .iter()
        .map(|byte| CHARS[(*byte as usize) % CHARS.len()] as char)
        .collect()
}

/// Compute the S256 PKCE challenge for a verifier per RFC 7636 §4.2.
/// SHA-256, then base64url-encode without padding.
pub fn compute_challenge(verifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
}

/// Generate a CSRF state nonce. 32 random bytes, base64url-encoded.
pub fn generate_state() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Build the authorize URL. Mirrors
/// [`buildAuthorizeUrl`](../../../../../opencode/src/plugin/codex.ts:90).
/// Custom params are load-bearing — Bun's behavior depends on
/// `id_token_add_organizations=true` (so the id_token JWT carries the
/// `organizations` array we use for fallback account-id extraction) and on
/// `codex_cli_simplified_flow=true` (which is what makes the auth server
/// admit our public client).
pub fn build_authorize_url(redirect_uri: &str, challenge: &str, state: &str) -> String {
    let params = [
        ("response_type", "code"),
        ("client_id", CLIENT_ID),
        ("redirect_uri", redirect_uri),
        ("scope", "openid profile email offline_access"),
        ("code_challenge", challenge),
        ("code_challenge_method", "S256"),
        ("id_token_add_organizations", "true"),
        ("codex_cli_simplified_flow", "true"),
        ("state", state),
        ("originator", "opencode"),
    ];
    let qs = params
        .iter()
        .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
        .collect::<Vec<_>>()
        .join("&");
    format!("{ISSUER}/oauth/authorize?{qs}")
}

/// Begin an OAuth flow: generate PKCE + state, build the authorize URL.
/// Caller is expected to (1) open the URL in a browser, (2) start the
/// loopback receiver, (3) await the redirect with `code` + `state`, (4)
/// CSRF-check `state == begin.state`, (5) call [`exchange_code`].
pub fn begin_oauth(redirect_uri: &str) -> OAuthBegin {
    let verifier = generate_verifier();
    let challenge = compute_challenge(&verifier);
    let state = generate_state();
    let url = build_authorize_url(redirect_uri, &challenge, &state);
    OAuthBegin {
        verifier,
        challenge,
        state,
        redirect_uri: redirect_uri.to_string(),
        url,
    }
}

/// Parse a token-endpoint JSON response into [`OAuthTokens`]. Pure: takes
/// the parsed JSON value, returns either the typed token blob or a parse
/// error. Used by [`exchange_code`] and [`refresh_token`] AND by the unit
/// tests so we can exercise the parsing logic without standing up a stub
/// server.
pub fn parse_token_response(
    body: &serde_json::Value,
    fallback_account: Option<String>,
) -> Result<OAuthTokens, OAuthError> {
    let access = body
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| OAuthError::Malformed("missing access_token".into()))?
        .to_string();
    let refresh = body
        .get("refresh_token")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| OAuthError::Malformed("missing refresh_token".into()))?
        .to_string();
    let expires_in = body
        .get("expires_in")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(3600);
    let now = unix_millis();
    let account_id = body
        .get("id_token")
        .and_then(serde_json::Value::as_str)
        .and_then(extract_account_id_from_jwt)
        .or_else(|| {
            body.get("access_token")
                .and_then(serde_json::Value::as_str)
                .and_then(extract_account_id_from_jwt)
        })
        .or(fallback_account);
    Ok(OAuthTokens {
        access,
        refresh,
        expires_at: now + expires_in * 1000,
        account_id,
    })
}

/// POST `grant_type=authorization_code` to the token endpoint.
pub async fn exchange_code(
    code: &str,
    redirect_uri: &str,
    verifier: &str,
) -> Result<OAuthTokens, OAuthError> {
    let body = format!(
        "grant_type=authorization_code&code={}&redirect_uri={}&client_id={CLIENT_ID}&code_verifier={}",
        url_encode(code),
        url_encode(redirect_uri),
        url_encode(verifier),
    );
    post_token(&body, None).await
}

/// POST `grant_type=refresh_token` to the token endpoint. The
/// `account_fallback` argument is the previously-known account id; we
/// preserve it on refresh because the new id_token may not always carry
/// the account claim. Mirrors
/// [`codex.ts:425-440`](../../../../../opencode/src/plugin/codex.ts:425).
pub async fn refresh_token(
    refresh: &str,
    account_fallback: Option<String>,
) -> Result<OAuthTokens, OAuthError> {
    let body = format!(
        "grant_type=refresh_token&refresh_token={}&client_id={CLIENT_ID}",
        url_encode(refresh),
    );
    post_token(&body, account_fallback).await
}

async fn post_token(
    body: &str,
    fallback_account: Option<String>,
) -> Result<OAuthTokens, OAuthError> {
    let res = reqwest::Client::new()
        .post(format!("{ISSUER}/oauth/token"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body.to_string())
        .send()
        .await
        .map_err(|err| OAuthError::Transport(err.to_string()))?;
    let status = res.status();
    if !status.is_success() {
        let body = res.text().await.unwrap_or_default();
        return Err(OAuthError::Status {
            status: status.as_u16(),
            body,
        });
    }
    let value = res
        .json::<serde_json::Value>()
        .await
        .map_err(|err| OAuthError::Transport(err.to_string()))?;
    parse_token_response(&value, fallback_account)
}

/// Extract `chatgpt_account_id` from a JWT payload. Decodes the
/// middle segment (URL-safe base64, no padding) and walks the same
/// claim lookup chain Bun uses at
/// [`extractAccountIdFromClaims`](../../../../../opencode/src/plugin/codex.ts:69):
///
/// 1. top-level `chatgpt_account_id`,
/// 2. `["https://api.openai.com/auth"].chatgpt_account_id`,
/// 3. `organizations[0].id`.
pub fn extract_account_id_from_jwt(token: &str) -> Option<String> {
    let payload = token.split('.').nth(1)?;
    let bytes = general_purpose::URL_SAFE_NO_PAD.decode(payload).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    if let Some(id) = value.get("chatgpt_account_id").and_then(|v| v.as_str()) {
        return Some(id.to_string());
    }
    if let Some(id) = value
        .get("https://api.openai.com/auth")
        .and_then(|v| v.get("chatgpt_account_id"))
        .and_then(|v| v.as_str())
    {
        return Some(id.to_string());
    }
    value
        .get("organizations")
        .and_then(|v| v.as_array())
        .and_then(|orgs| orgs.first())
        .and_then(|first| first.get("id"))
        .and_then(|v| v.as_str())
        .map(String::from)
}

/// Build the OAuth state machine for the loopback receiver. The caller
/// (kilo-server) owns the actual HTTP listener — this helper just
/// validates the captured `(code, state)` pair against the begin state.
pub fn validate_callback(
    begin: &OAuthBegin,
    code: &str,
    state: &str,
) -> Result<String, OAuthError> {
    if state != begin.state {
        return Err(OAuthError::StateMismatch);
    }
    Ok(code.to_string())
}

/// Loopback `redirect_uri` for the default port.
pub fn default_redirect_uri() -> String {
    format!("http://localhost:{LOOPBACK_PORT}/auth/callback")
}

fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose;
    use serde_json::json;

    #[test]
    fn verifier_is_43_chars_url_safe() {
        for _ in 0..10 {
            let v = generate_verifier();
            assert_eq!(v.len(), 43);
            assert!(
                v.chars().all(|c| c.is_ascii_alphanumeric()
                    || c == '-'
                    || c == '_'
                    || c == '.'
                    || c == '~'),
                "verifier had non-RFC-7636 char: {v}"
            );
        }
    }

    #[test]
    fn challenge_is_s256_of_verifier_no_padding() {
        // RFC 7636 worked example.
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let expected = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
        assert_eq!(compute_challenge(verifier), expected);
        // No `=` padding in the output.
        assert!(!compute_challenge("anything").ends_with('='));
    }

    #[test]
    fn state_is_url_safe_base64_no_padding() {
        for _ in 0..5 {
            let s = generate_state();
            assert!(!s.contains('='));
            assert!(general_purpose::URL_SAFE_NO_PAD.decode(&s).is_ok());
        }
    }

    #[test]
    fn authorize_url_carries_codex_specific_params() {
        let url = build_authorize_url("http://localhost:1455/auth/callback", "challenge", "state");
        assert!(url.starts_with("https://auth.openai.com/oauth/authorize?"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains(&format!("client_id={CLIENT_ID}")));
        assert!(url.contains("code_challenge=challenge"));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("scope=openid%20profile%20email%20offline_access"));
        // Codex-specific switches that gate the simplified public-client flow.
        assert!(url.contains("id_token_add_organizations=true"));
        assert!(url.contains("codex_cli_simplified_flow=true"));
        assert!(url.contains("originator=opencode"));
        assert!(url.contains("state=state"));
    }

    #[test]
    fn begin_oauth_round_trips_pkce() {
        let begin = begin_oauth("http://localhost:1455/auth/callback");
        assert_eq!(begin.challenge, compute_challenge(&begin.verifier));
        assert!(!begin.state.is_empty());
        assert!(begin.url.contains(&begin.state));
        assert!(begin.url.contains(&begin.challenge));
    }

    #[test]
    fn validate_callback_rejects_csrf_mismatch() {
        let begin = begin_oauth("http://localhost:1455/auth/callback");
        let res = validate_callback(&begin, "the-code", "wrong-state");
        assert_eq!(res, Err(OAuthError::StateMismatch));
        let ok = validate_callback(&begin, "the-code", &begin.state).unwrap();
        assert_eq!(ok, "the-code");
    }

    #[test]
    fn parse_token_response_extracts_required_fields() {
        let body = json!({
            "access_token": "AT",
            "refresh_token": "RT",
            "expires_in": 3600,
        });
        let tokens = parse_token_response(&body, None).unwrap();
        assert_eq!(tokens.access, "AT");
        assert_eq!(tokens.refresh, "RT");
        assert!(tokens.expires_at > unix_millis());
        assert!(tokens.account_id.is_none());
    }

    #[test]
    fn parse_token_response_uses_id_token_account_id() {
        let header = general_purpose::URL_SAFE_NO_PAD.encode(b"{}");
        let payload = general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"chatgpt_account_id":"acct_42","organizations":[{"id":"org_1"}]}"#);
        let id_token = format!("{header}.{payload}.sig");
        let body = json!({
            "access_token": "AT",
            "refresh_token": "RT",
            "id_token": id_token,
        });
        let tokens = parse_token_response(&body, Some("fallback".into())).unwrap();
        assert_eq!(tokens.account_id.as_deref(), Some("acct_42"));
    }

    #[test]
    fn parse_token_response_falls_back_to_organizations() {
        let header = general_purpose::URL_SAFE_NO_PAD.encode(b"{}");
        let payload =
            general_purpose::URL_SAFE_NO_PAD.encode(br#"{"organizations":[{"id":"org_42"}]}"#);
        let id_token = format!("{header}.{payload}.sig");
        let body = json!({
            "access_token": "AT",
            "refresh_token": "RT",
            "id_token": id_token,
        });
        let tokens = parse_token_response(&body, None).unwrap();
        assert_eq!(tokens.account_id.as_deref(), Some("org_42"));
    }

    #[test]
    fn parse_token_response_uses_namespaced_claim() {
        let header = general_purpose::URL_SAFE_NO_PAD.encode(b"{}");
        let payload = general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"https://api.openai.com/auth":{"chatgpt_account_id":"acct_ns"}}"#);
        let id_token = format!("{header}.{payload}.sig");
        let body = json!({
            "access_token": "AT",
            "refresh_token": "RT",
            "id_token": id_token,
        });
        let tokens = parse_token_response(&body, None).unwrap();
        assert_eq!(tokens.account_id.as_deref(), Some("acct_ns"));
    }

    #[test]
    fn parse_token_response_preserves_fallback_account() {
        let body = json!({
            "access_token": "AT",
            "refresh_token": "RT",
        });
        let tokens = parse_token_response(&body, Some("kept".to_string())).unwrap();
        assert_eq!(tokens.account_id.as_deref(), Some("kept"));
    }

    #[test]
    fn parse_token_response_rejects_missing_access() {
        let body = json!({ "refresh_token": "RT" });
        assert!(matches!(
            parse_token_response(&body, None),
            Err(OAuthError::Malformed(_))
        ));
    }

    #[test]
    fn extract_account_id_handles_invalid_jwt() {
        assert!(extract_account_id_from_jwt("not.a.jwt").is_none());
        assert!(extract_account_id_from_jwt("only-one-segment").is_none());
    }
}
