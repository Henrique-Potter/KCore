//! Auth-routes, provider-routes, OAuth authorize/callback, OAuth token
//! merge, internal-error envelope, and config-resolver tests.

use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use base64::{engine::general_purpose, Engine};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

use crate::agent::tools::defs::read_def;
use crate::oauth::crypto::pkce_challenge;
use crate::oauth::tokens::{extract_account, fresh_auths, merge_token_auth};
use crate::oauth::url::{oauth_url, query_params};
use crate::oauth::OPENAI_REDIRECT;
use crate::routes::config::{
    clear_auth, config_providers, oauth_authorize, provider_auth, providers, set_auth,
};
use crate::util::encoding::unix_millis;
use crate::{internal_error, internal_error_named, PendingAuth};

use super::common::{
    oauth_callback_dispatch, provider_server, response_to_value, state, state_at, state_at_with,
    state_with_listener_addr, state_with_token_endpoint, store, unique_root, ENV_RESOLVE_LOCK,
};

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
async fn provider_routes_filter_codex_models_only_for_oauth() {
    let root = unique_root();
    let state = state_at(&root);
    state
        .store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "access": "access-token",
                "refresh": "refresh-token",
                "expires": 1
            }),
        )
        .unwrap();

    let out = providers(State(state.clone())).await.into_response();
    let body = response_to_value(out).await;
    assert_eq!(body["all"].as_array().unwrap().len(), 1);
    assert_eq!(body["all"][0]["id"], "openai");
    assert_eq!(body["connected"], json!(["openai"]));
    let openai = body["all"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some("openai"))
        .unwrap();
    let models = openai["models"].as_object().unwrap();
    assert!(models.contains_key("gpt-5.1-codex"));
    assert!(models.contains_key("gpt-5.5"));
    assert!(!models.contains_key("gpt-4.1"));
    assert_eq!(models["gpt-5.1-codex"]["cost"]["input"], 0);

    state.store.clear_provider_auth("openai").unwrap();
    state
        .store
        .set_provider_auth("openai", json!({ "type": "api", "key": "sk-test" }))
        .unwrap();
    let out = config_providers(State(state.clone())).await.into_response();
    let body = response_to_value(out).await;
    let openai = body["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some("openai"))
        .unwrap();
    let models = openai["models"].as_object().unwrap();
    assert!(models.contains_key("gpt-5.1-codex"));
    assert!(models.contains_key("gpt-4.1"));
    assert!(models.contains_key("o1-preview"));
    assert_ne!(models["gpt-5.1-codex"]["cost"]["input"], 0);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn provider_auth_exposes_only_openai_browser_oauth() {
    let out = provider_auth().await.into_response();
    let body = response_to_value(out).await;
    assert_eq!(
        body,
        json!({
            "openai": [
                { "label": "ChatGPT Pro/Plus (browser)", "type": "oauth" }
            ]
        })
    );
}

#[tokio::test]
async fn oauth_authorize_returns_openai_pkce_url() {
    // Audit Fix 3: signature now includes `State<Arc<AppState>>` so
    // the handler can stash the verifier in the pending map.
    let st = state();
    let res = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": 0, "inputs": {} })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let body = response_to_value(res).await;
    assert_eq!(body["method"], "auto");

    let challenge = pkce_challenge("abc");
    let url = oauth_url(OPENAI_REDIRECT, &challenge, "state");
    assert!(url.contains("https://auth.openai.com/oauth/authorize"));
    assert!(url.contains("client_id=app_EMoamEEZ73f0CkXaXp7hrann"));
    assert!(url.contains("redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback"));
    assert!(url.contains("code_challenge="));

    // Pending entry stored.
    assert_eq!(st.oauth_pending.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn oauth_authorize_surfaces_listener_bind_failure() {
    let held = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = held.local_addr().unwrap();
    let st = state_with_listener_addr(addr);
    let res = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": 0 })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response_to_value(res).await;
    assert_eq!(body["name"], "OauthCallbackListenerError");
    assert_eq!(st.oauth_pending.lock().unwrap().len(), 0);
}

#[test]
fn oauth_callback_query_decodes_browser_redirect_params() {
    let params = query_params("code=abc%2B123&state=csrf+value");
    assert_eq!(params.get("code").map(String::as_str), Some("abc+123"));
    assert_eq!(params.get("state").map(String::as_str), Some("csrf value"));
}

/// Audit Fix 2: when `id_token` decodes but lacks the
/// `chatgpt_account_id` claim, fall back to `access_token`. Mirrors
/// `parse_token_response` in `kilo_provider::openai_oauth`.
#[test]
fn extract_account_falls_back_to_access_token_when_id_token_lacks_claim() {
    let header = general_purpose::URL_SAFE_NO_PAD.encode(b"{}");
    // id_token JWT decodes but carries no recognised claim.
    let payload_no_claim = general_purpose::URL_SAFE_NO_PAD.encode(br#"{"sub":"user_x"}"#);
    let id_token = format!("{header}.{payload_no_claim}.sig");
    // access_token JWT carries the claim.
    let payload_claim =
        general_purpose::URL_SAFE_NO_PAD.encode(br#"{"chatgpt_account_id":"acct_from_access"}"#);
    let access_token = format!("{header}.{payload_claim}.sig");

    let tokens = json!({
        "id_token": id_token,
        "access_token": access_token,
    });
    assert_eq!(
        extract_account(&tokens).as_deref(),
        Some("acct_from_access")
    );
}

#[test]
fn extract_account_prefers_id_token_when_both_carry_claim() {
    let header = general_purpose::URL_SAFE_NO_PAD.encode(b"{}");
    let payload_id =
        general_purpose::URL_SAFE_NO_PAD.encode(br#"{"chatgpt_account_id":"acct_from_id"}"#);
    let id_token = format!("{header}.{payload_id}.sig");
    let payload_acc =
        general_purpose::URL_SAFE_NO_PAD.encode(br#"{"chatgpt_account_id":"acct_from_access"}"#);
    let access_token = format!("{header}.{payload_acc}.sig");

    let tokens = json!({
        "id_token": id_token,
        "access_token": access_token,
    });
    assert_eq!(extract_account(&tokens).as_deref(), Some("acct_from_id"));
}

#[test]
fn extract_account_reads_openai_auth_namespaced_claim() {
    let header = general_purpose::URL_SAFE_NO_PAD.encode(b"{}");
    let payload = general_purpose::URL_SAFE_NO_PAD
        .encode(br#"{"https://api.openai.com/auth":{"chatgpt_account_id":"acct_ns"}}"#);
    let tokens = json!({
        "id_token": format!("{header}.{payload}.sig"),
        "access_token": "access",
    });

    assert_eq!(extract_account(&tokens).as_deref(), Some("acct_ns"));
}

/// Audit Fix 7: token refresh must preserve non-token fields
/// (`enterpriseUrl`, future fields) on the existing auth blob instead
/// of rebuilding from scratch.
#[test]
fn token_auth_preserves_existing_enterprise_url() {
    let existing = json!({
        "type": "oauth",
        "refresh": "old-refresh",
        "access": "old-access",
        "expires": 1i64,
        "enterpriseUrl": "https://acme.example/v1",
    });
    let tokens = json!({
        "access_token": "new-access",
        "refresh_token": "new-refresh",
        "expires_in": 3600,
    });
    let merged = merge_token_auth(&existing, &tokens, None).expect("token_auth merge");
    assert_eq!(merged["access"], "new-access");
    assert_eq!(merged["refresh"], "new-refresh");
    assert_eq!(merged["enterpriseUrl"], "https://acme.example/v1");
    assert_eq!(merged["type"], "oauth");
}

#[test]
fn token_auth_rejects_missing_access_and_preserves_refresh_fallback() {
    let existing = json!({
        "type": "oauth",
        "refresh": "old-refresh",
        "access": "old-access",
        "expires": 1i64,
    });
    let bad = merge_token_auth(&existing, &json!({ "refresh_token": "new-refresh" }), None);
    assert!(bad.unwrap_err().contains("access token"));

    let merged = merge_token_auth(
        &existing,
        &json!({ "access_token": "new-access", "expires_in": 3600 }),
        None,
    )
    .expect("refresh fallback");
    assert_eq!(merged["access"], "new-access");
    assert_eq!(merged["refresh"], "old-refresh");
}

#[tokio::test]
async fn fresh_auths_refreshes_malformed_unexpired_access_token() {
    let root = unique_root();
    let server = provider_server(json!({
        "access_token": jwt(r#"{"exp":4102444800,"chatgpt_account_id":"acct_new"}"#),
        "refresh_token": "new-refresh",
        "expires_in": 3600
    }))
    .await;
    let st = state_at_with(
        Some(store(&root)),
        "127.0.0.1:0".parse().unwrap(),
        server.url.clone(),
    );
    st.store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "access": "not-a-jwt",
                "refresh": "old-refresh",
                "expires": unix_millis() + 3_600_000,
                "accountId": "acct_old"
            }),
        )
        .unwrap();

    let cancel = std::sync::atomic::AtomicBool::new(false);
    let auths = fresh_auths(&st, &cancel).await.expect("fresh auths");
    assert_eq!(auths["openai"]["refresh"], "new-refresh");
    assert_eq!(auths["openai"]["accountId"], "acct_new");
    assert_ne!(auths["openai"]["access"], "not-a-jwt");
    assert!(server
        .body
        .lock()
        .unwrap()
        .contains("refresh_token=old-refresh"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn fresh_auths_serializes_concurrent_openai_refreshes() {
    let root = unique_root();
    let server = provider_server(json!({
        "access_token": jwt(r#"{"exp":4102444800,"chatgpt_account_id":"acct_new"}"#),
        "refresh_token": "new-refresh",
        "expires_in": 3600
    }))
    .await;
    let st = state_at_with(
        Some(store(&root)),
        "127.0.0.1:0".parse().unwrap(),
        server.url.clone(),
    );
    st.store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "access": "not-a-jwt",
                "refresh": "old-refresh",
                "expires": unix_millis() + 3_600_000,
                "accountId": "acct_old"
            }),
        )
        .unwrap();

    let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let left_state = st.clone();
    let right_state = st.clone();
    let left_cancel = cancel.clone();
    let right_cancel = cancel.clone();
    let (left, right) = tokio::join!(
        async move { fresh_auths(&left_state, &left_cancel).await },
        async move { fresh_auths(&right_state, &right_cancel).await },
    );

    let left = left.expect("left refresh");
    let right = right.expect("right refresh");
    assert_eq!(left["openai"]["refresh"], "new-refresh");
    assert_eq!(right["openai"]["refresh"], "new-refresh");
    assert_eq!(
        st.store.provider_auth("openai").unwrap()["refresh"],
        "new-refresh"
    );
    assert!(server
        .body
        .lock()
        .unwrap()
        .contains("refresh_token=old-refresh"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn fresh_auths_aborts_promptly_when_cancel_fires_during_token_request() {
    // Fix C2: the token endpoint POST in `refresh_access` previously had
    // no cancel race and no timeout. This test stands up a TCP listener
    // that accepts connections and never replies, points the token
    // endpoint at it, trips cancel, and asserts `fresh_auths` returns
    // an `aborted` error within ~250ms.
    let root = unique_root();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
    // Hold the listener so the OS-level connect succeeds; we just never
    // write a response. The accepted socket stays in the listener's
    // backlog (no `accept()` call) so reqwest hangs on read.
    tokio::spawn(async move {
        let _l = listener;
        // Park forever — the test will drop us when the runtime shuts down.
        std::future::pending::<()>().await;
    });
    let st = state_at_with(Some(store(&root)), "127.0.0.1:0".parse().unwrap(), endpoint);
    st.store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "access": "not-a-jwt",
                "refresh": "old-refresh",
                "expires": unix_millis() + 3_600_000,
                "accountId": "acct_old"
            }),
        )
        .unwrap();
    let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel_handle = cancel.clone();
    let st_clone = st.clone();
    let task = tokio::spawn(async move { fresh_auths(&st_clone, &cancel_handle).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.store(true, std::sync::atomic::Ordering::SeqCst);
    let started = Instant::now();
    let out = tokio::time::timeout(Duration::from_millis(250), task)
        .await
        .expect("fresh_auths must return within 250ms of cancel")
        .expect("task join");
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "elapsed {:?} exceeded budget",
        started.elapsed()
    );
    let err = out.expect_err("fresh_auths must surface an error on cancel");
    assert!(
        err.contains("aborted"),
        "error must include `aborted`, got: {err}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn fresh_auths_rejects_stale_oauth_without_refresh_token() {
    let root = unique_root();
    let st = state_at(&root);
    st.store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "access": "not-a-jwt",
                "expires": unix_millis() + 3_600_000,
            }),
        )
        .unwrap();

    let cancel = std::sync::atomic::AtomicBool::new(false);
    let err = fresh_auths(&st, &cancel).await.unwrap_err();
    assert!(err.contains("Please sign in again"));

    let _ = std::fs::remove_dir_all(root);
}

fn jwt(payload: &str) -> String {
    let header = general_purpose::URL_SAFE_NO_PAD.encode(b"{}");
    let payload = general_purpose::URL_SAFE_NO_PAD.encode(payload.as_bytes());
    format!("{header}.{payload}.sig")
}

/// Audit Fix 11: the default 500 envelope name is `InternalError`,
/// matching what Bun returns for un-tagged errors. The body shape is
/// `{ name: "InternalError", data: { message: ... } }`.
#[tokio::test]
async fn internal_error_envelope_uses_internal_error_name() {
    let res = internal_error("boom");
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["name"], "InternalError");
    assert_eq!(value["data"]["message"], "boom");
}

#[tokio::test]
async fn internal_error_named_uses_provided_name() {
    let res = internal_error_named("OauthCallbackError", "exchange failed");
    let bytes = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["name"], "OauthCallbackError");
    assert_eq!(value["data"]["message"], "exchange failed");
}

/// Wave 1 Group E follow-up: every OAuth NamedError that the
/// `routes/config.rs` handlers can emit must be listed in
/// `ALLOWED_INTERNAL_ERROR_NAMES`. The canonical
/// `error::bad_request_named` enforces this via `debug_assert!`, so the
/// loop below would panic in test builds if any name leaked through
/// without registration.
#[tokio::test]
async fn bad_request_named_emits_only_registered_names() {
    use crate::error::bad_request_named;

    for name in [
        "OauthUnsupportedProvider",
        "OauthUnsupportedMethod",
        "OauthCodeMissing",
        "OauthPendingMissing",
        "OauthStateMismatch",
        "OauthCallbackTimeout",
    ] {
        let res = bad_request_named(name, "msg");
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(res.into_body(), 4096).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["name"], name);
        assert_eq!(value["data"]["message"], "msg");
    }
}

/// Audit Fix 8: the `read` tool schema must require `filePath` and not
/// surface a `path` synonym in `properties`.
#[test]
fn read_tool_schema_requires_file_path_only() {
    let tool = read_def();
    let props = tool.parameters["properties"].as_object().unwrap();
    assert!(props.contains_key("filePath"));
    assert!(
        !props.contains_key("path"),
        "read tool schema must not advertise a `path` synonym"
    );
    let required = tool.parameters["required"].as_array().unwrap();
    assert!(
        required.iter().any(|v| v == "filePath"),
        "read tool schema must declare `filePath` as required"
    );
}

/// Audit Fix 3 & 4: oauth_callback wired to a server-side pending map.
/// Authorize stores the verifier+state, callback consumes it. The
/// canonical Bun-compat shape posts only `{ code, state }`.
#[tokio::test]
async fn oauth_authorize_then_callback_without_body_verifier_uses_stored_verifier() {
    let st = state();
    let auth_res = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": 0 })),
    )
    .await;
    let body = response_to_value(auth_res).await;
    let csrf = body["state"].as_str().unwrap().to_string();
    // Pending map populated.
    let pending_count = st.oauth_pending.lock().unwrap().len();
    assert_eq!(pending_count, 1);

    // Callback with `{code, state}` only. Token exchange against
    // auth.openai.com will fail (no network in tests, fake code), but
    // the handler must NOT short-circuit with a "missing verifier" 400
    // — i.e. it must look up the stored verifier and proceed to the
    // exchange step. We expect a 500 envelope (`OauthCallbackError`),
    // not a 400.
    let cb = oauth_callback_dispatch(
        st.clone(),
        "openai",
        json!({ "code": "the-code", "state": csrf }),
    )
    .await;
    assert_ne!(
        cb.status(),
        StatusCode::BAD_REQUEST,
        "callback with stored verifier must not 400"
    );
    // And the pending entry must be consumed (single-use).
    assert_eq!(
        st.oauth_pending.lock().unwrap().len(),
        0,
        "pending entry must be consumed on callback"
    );
}

#[tokio::test]
async fn oauth_authorize_then_callback_exchanges_and_stores_tokens() {
    let server = provider_server(json!({
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "expires_in": 3600,
    }))
    .await;
    let st = state_with_token_endpoint(server.url.clone());
    let auth_res = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": 0 })),
    )
    .await;
    assert_eq!(auth_res.status(), StatusCode::OK);
    let body = response_to_value(auth_res).await;
    let csrf = body["state"].as_str().unwrap().to_string();

    let cb = oauth_callback_dispatch(
        st.clone(),
        "openai",
        json!({ "code": "the-code", "state": csrf }),
    )
    .await;
    assert_eq!(cb.status(), StatusCode::OK);
    assert_eq!(response_to_value(cb).await, json!(true));

    let auth = st.store.provider_auth("openai").expect("stored auth");
    assert_eq!(auth["type"], "oauth");
    assert_eq!(auth["access"], "access-token");
    assert_eq!(auth["refresh"], "refresh-token");
    assert!(auth["expires"].as_i64().unwrap() > unix_millis());
    assert_eq!(st.oauth_pending.lock().unwrap().len(), 0);

    let req = server.body.lock().unwrap().clone();
    assert!(req.contains("POST /v1 HTTP/1.1"));
    assert!(req.contains("grant_type=authorization_code"));
    assert!(req.contains("code=the-code"));
    assert!(req.contains("redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback"));
    assert!(req.contains("code_verifier="));
}

#[tokio::test]
async fn oauth_callback_without_authorize_fails_clean() {
    let st = state();
    let cb = oauth_callback_dispatch(
        st.clone(),
        "openai",
        json!({ "code": "the-code", "state": "anything" }),
    )
    .await;
    assert_eq!(cb.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn oauth_callback_state_mismatch_fails() {
    let st = state();
    let auth_res = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": 0 })),
    )
    .await;
    let _body = response_to_value(auth_res).await;
    let cb = oauth_callback_dispatch(
        st.clone(),
        "openai",
        json!({ "code": "the-code", "state": "wrong-state" }),
    )
    .await;
    assert_eq!(cb.status(), StatusCode::BAD_REQUEST);
    let body = response_to_value(cb).await;
    assert_eq!(body["name"], "OauthStateMismatch");
    assert_eq!(st.oauth_pending.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn oauth_stale_callback_does_not_consume_current_flow() {
    let server = provider_server(json!({
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "expires_in": 3600,
    }))
    .await;
    let st = state_with_token_endpoint(server.url.clone());
    let auth = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": 0 })),
    )
    .await;
    let first = response_to_value(auth).await;
    let stale = first["state"].as_str().unwrap().to_string();
    let auth = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": 0 })),
    )
    .await;
    let second = response_to_value(auth).await;
    let csrf = second["state"].as_str().unwrap().to_string();

    let cb = oauth_callback_dispatch(
        st.clone(),
        "openai",
        json!({ "code": "stale-code", "state": stale }),
    )
    .await;
    assert_eq!(cb.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response_to_value(cb).await["name"], "OauthStateMismatch");
    assert_eq!(st.oauth_pending.lock().unwrap().len(), 1);

    let cb = oauth_callback_dispatch(
        st.clone(),
        "openai",
        json!({ "code": "fresh-code", "state": csrf }),
    )
    .await;
    assert_eq!(cb.status(), StatusCode::OK);
    assert_eq!(response_to_value(cb).await, json!(true));
    assert_eq!(st.oauth_pending.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn oauth_browser_stale_callback_does_not_consume_current_flow() {
    let server = provider_server(json!({
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "expires_in": 3600,
    }))
    .await;
    let st = state_with_token_endpoint(server.url.clone());
    let auth = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": "auto" })),
    )
    .await;
    let first = response_to_value(auth).await;
    let stale = first["state"].as_str().unwrap().to_string();
    let auth = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": "auto" })),
    )
    .await;
    let second = response_to_value(auth).await;
    let csrf = second["state"].as_str().unwrap().to_string();

    let browser =
        crate::routes::config::complete_browser_oauth_callback(st.clone(), "stale-code", &stale)
            .await;
    assert_eq!(browser.status(), StatusCode::BAD_REQUEST);
    assert_eq!(st.oauth_pending.lock().unwrap().len(), 1);

    let browser =
        crate::routes::config::complete_browser_oauth_callback(st.clone(), "fresh-code", &csrf)
            .await;
    assert_eq!(browser.status(), StatusCode::OK);
    assert_eq!(st.oauth_pending.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn oauth_callback_expired_pending_entry_fails() {
    let st = state();
    // Prime the map with an entry that's already expired.
    {
        let mut pending = st.oauth_pending.lock().unwrap();
        pending.insert(
            "openai".to_string(),
            PendingAuth {
                verifier: "v".to_string(),
                state: "s".to_string(),
                expires_at: Instant::now() - Duration::from_secs(1),
                complete: tokio::sync::watch::channel(None).0,
            },
        );
    }
    let cb = oauth_callback_dispatch(
        st.clone(),
        "openai",
        json!({ "code": "the-code", "state": "s" }),
    )
    .await;
    assert_eq!(cb.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn oauth_auto_callback_waits_for_browser_completion_and_stores_tokens() {
    let server = provider_server(json!({
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "expires_in": 3600,
    }))
    .await;
    let st = state_with_token_endpoint(server.url.clone());
    let auth_res = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": "auto" })),
    )
    .await;
    let body = response_to_value(auth_res).await;
    let csrf = body["state"].as_str().unwrap().to_string();

    let task = tokio::spawn({
        let state = st.clone();
        let csrf = csrf.clone();
        async move {
            oauth_callback_dispatch(state, "openai", json!({ "method": "auto", "state": csrf }))
                .await
        }
    });
    tokio::task::yield_now().await;

    let browser =
        crate::routes::config::complete_browser_oauth_callback(st.clone(), "the-code", &csrf).await;
    assert_eq!(browser.status(), StatusCode::OK);

    let cb = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("auto callback waiter should complete")
        .unwrap();
    assert_eq!(cb.status(), StatusCode::OK);
    assert_eq!(response_to_value(cb).await, json!(true));

    let auth = st.store.provider_auth("openai").expect("stored auth");
    assert_eq!(auth["type"], "oauth");
    assert_eq!(auth["access"], "access-token");
    assert_eq!(auth["refresh"], "refresh-token");
    assert_eq!(st.oauth_pending.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn oauth_auto_callback_times_out_with_named_error() {
    let st = state();
    let auth_res = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": 0, "inputs": {} })),
    )
    .await;
    let body = response_to_value(auth_res).await;
    let state = body["state"].as_str().unwrap().to_string();

    let cb =
        oauth_callback_dispatch(st, "openai", json!({ "method": "auto", "state": state })).await;
    assert_eq!(cb.status(), StatusCode::BAD_REQUEST);
    let body = response_to_value(cb).await;
    assert_eq!(body["name"], "OauthCallbackTimeout");
    assert!(body["data"]["message"]
        .as_str()
        .unwrap()
        .contains("Timed out"));
}

#[tokio::test]
async fn oauth_replaced_auto_callback_waiter_gets_named_error() {
    let st = state();
    let auth = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": "auto" })),
    )
    .await;
    let body = response_to_value(auth).await;
    let csrf = body["state"].as_str().unwrap().to_string();
    let task = tokio::spawn({
        let st = st.clone();
        async move {
            oauth_callback_dispatch(st, "openai", json!({ "method": "auto", "state": csrf })).await
        }
    });
    tokio::task::yield_now().await;

    let auth = oauth_authorize(
        State(st.clone()),
        Path("openai".to_string()),
        Json(json!({ "method": "auto" })),
    )
    .await;
    assert_eq!(auth.status(), StatusCode::OK);

    let cb = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("replaced waiter should complete")
        .unwrap();
    assert_eq!(cb.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = response_to_value(cb).await;
    assert_eq!(body["name"], "OauthCallbackError");
    assert!(body["data"]["message"]
        .as_str()
        .unwrap()
        .contains("was replaced"));
}

#[tokio::test]
async fn oauth_callback_bad_requests_return_named_json_errors() {
    let st = state();
    let missing =
        oauth_callback_dispatch(st.clone(), "openai", json!({ "method": "manual" })).await;
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
    let body = response_to_value(missing).await;
    assert_eq!(body["name"], "OauthCodeMissing");
    assert!(body["data"]["message"]
        .as_str()
        .unwrap()
        .contains("authorization code"));

    let pending = oauth_callback_dispatch(
        st.clone(),
        "openai",
        json!({ "method": "auto", "code": "the-code", "state": "missing" }),
    )
    .await;
    assert_eq!(pending.status(), StatusCode::BAD_REQUEST);
    let body = response_to_value(pending).await;
    assert_eq!(body["name"], "OauthPendingMissing");
    assert!(body["data"]["message"]
        .as_str()
        .unwrap()
        .contains("pending OAuth"));

    let unsupported = oauth_callback_dispatch(st, "anthropic", json!({ "method": "auto" })).await;
    assert_eq!(unsupported.status(), StatusCode::BAD_REQUEST);
    let body = response_to_value(unsupported).await;
    assert_eq!(body["name"], "OauthUnsupportedProvider");
    assert!(body["data"]["message"].as_str().unwrap().contains("openai"));
}

/// Audit Fix 6: when both an OAuth blob in `auths` and an `apiKey` in
/// `cfg.options` are present for openai, OAuth wins. Auth.json is the
/// source of truth.
#[test]
fn resolver_prefers_oauth_over_config_api_key_for_openai() {
    let _g = ENV_RESOLVE_LOCK
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    std::env::remove_var("KILO_AUTH_CONTENT");
    std::env::remove_var("OPENAI_API_KEY");
    std::env::remove_var("OPENAI_BASE_URL");
    let cfg = kilo_protocol::Config {
        data: std::collections::BTreeMap::from([(
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
            "access": "stored-token",
            "refresh": "rt",
            "expires": 1,
            "accountId": "acct_oauth"
        }
    });
    let req = kilo_provider::resolve_tools_with_auth(
        &cfg,
        &auths,
        Some(&json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
        None,
        vec![],
        vec![],
    )
    .unwrap();
    assert!(matches!(req.auth, kilo_provider::ChatAuth::Oauth { .. }));
    assert_eq!(req.base, "https://chatgpt.com/backend-api/codex");
}
