//! URL helpers used by the OpenAI OAuth flow.
//!
//! The browser-callback listener parses query strings off the redirect URL,
//! and the authorize/exchange/refresh paths build URL-encoded request bodies.
//! These helpers stay narrow on purpose — `decode_query` in `util::encoding`
//! (which middleware also uses) is the actual byte-level percent decoder;
//! this module wraps it for OAuth's `Option<String>` shape.

use std::collections::BTreeMap;

use crate::util::encoding::decode_query;

/// Build the upstream OpenAI authorize URL with PKCE + CSRF parameters.
/// Constants (`OPENAI_ISSUER`, `OPENAI_CLIENT_ID`) live in `oauth::mod`.
pub(crate) fn oauth_url(redirect: &str, challenge: &str, state: &str) -> String {
    use super::{OPENAI_CLIENT_ID, OPENAI_ISSUER};
    format!(
        "{OPENAI_ISSUER}/oauth/authorize?response_type=code&client_id={OPENAI_CLIENT_ID}&redirect_uri={}&scope=openid%20profile%20email%20offline_access&code_challenge={}&code_challenge_method=S256&id_token_add_organizations=true&codex_cli_simplified_flow=true&state={}&originator=opencode",
        url_encode(redirect),
        url_encode(challenge),
        url_encode(state),
    )
}

pub(crate) fn query_params(query: &str) -> BTreeMap<String, String> {
    query
        .split('&')
        .filter_map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            Some((url_decode(key)?, url_decode(value)?))
        })
        .collect()
}

pub(crate) fn url_decode(value: &str) -> Option<String> {
    Some(decode_query(value))
}

pub(crate) fn url_encode(value: &str) -> String {
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
