//! PKCE secret/challenge generation and JWT claim parsing for the OpenAI
//! OAuth flow. None of this depends on `AppState`; the helpers are pure
//! transforms on bytes/strings. Kept together because they share the same
//! crypto/encoding primitives (`Sha256`, base64 URL-safe).

use base64::{engine::general_purpose, Engine};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub(crate) fn oauth_secret(len: usize) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
    let mut bytes = vec![0; len];
    let mut rng = rand::rngs::OsRng;
    rand::RngCore::fill_bytes(&mut rng, &mut bytes);
    bytes
        .into_iter()
        .map(|byte| CHARS[(byte as usize) % CHARS.len()] as char)
        .collect()
}

pub(crate) fn pkce_challenge(verifier: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
}

pub(crate) fn claim_account(claims: Value) -> Option<String> {
    claims
        .get("chatgpt_account_id")
        .or_else(|| claims.pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id"))
        .or_else(|| {
            claims
                .get("organizations")
                .and_then(Value::as_array)?
                .first()?
                .get("id")
        })
        .and_then(Value::as_str)
        .map(str::to_string)
}

pub(crate) fn jwt_claims(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    let bytes = general_purpose::URL_SAFE_NO_PAD.decode(payload).ok()?;
    serde_json::from_slice(&bytes).ok()
}
