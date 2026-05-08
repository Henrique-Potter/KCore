//! Remote (HTTP) MCP tests: tool discovery, header forwarding, OAuth
//! authorize/callback, dynamic-client registration, and refresh
//! flows. NOTE: this file is allowed to exceed the 1.2k LoC limit
//! because the Remote-MCP and OAuth-MCP concerns cluster together —
//! splitting them further would force a third file with shared
//! fixtures. Future M9 work should consider a dedicated `mcp_oauth`
//! module.

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use serde_json::json;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc as StdArc;
use tower::ServiceExt;

use crate::http::build_router as app;

use super::common::{
    mcp_add_remote_auth_server, mcp_add_remote_oauth_server, mcp_add_remote_server,
    mcp_connect_test_server, mcp_json_response, mcp_oauth_bad_metadata_server,
    mcp_oauth_metadata_server, mcp_remote_auth_server, mcp_remote_response, mcp_remote_server,
    mcp_remote_token_server, mcp_sse_response, mcp_status_value, mcp_token_error_server,
    mcp_token_server, response_to_string, response_to_value, state_at, unique_root,
};

#[tokio::test]
async fn mcp_remote_connect_discovers_tools_from_json() {
    let root = unique_root();
    let st = state_at(&root);
    let server = mcp_remote_server(vec![
        mcp_json_response(json!({ "jsonrpc": "2.0", "id": 1, "result": {} })),
        mcp_json_response(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": { "tools": [{ "name": "echo", "description": "Echo input", "inputSchema": { "type": "object" } }] }
        })),
    ])
    .await;

    mcp_add_remote_server(&st, "remote", &server.url, 1000).await;
    mcp_connect_test_server(&st, "remote").await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    let data = response_to_value(res).await;
    assert_eq!(data["remote"]["status"], "connected");
    assert_eq!(data["remote"]["tools"][0]["name"], "echo");
}

#[tokio::test]
async fn mcp_remote_tool_call_returns_json_result() {
    let root = unique_root();
    let st = state_at(&root);
    let server = mcp_remote_server(vec![
        mcp_json_response(json!({ "jsonrpc": "2.0", "id": 1, "result": {} })),
        mcp_json_response(json!({ "jsonrpc": "2.0", "id": 2, "result": { "tools": [] } })),
        mcp_json_response(json!({
            "jsonrpc": "2.0",
            "id": 3,
            "result": { "content": [{ "type": "text", "text": "hello" }] }
        })),
    ])
    .await;

    mcp_add_remote_server(&st, "remote", &server.url, 1000).await;
    mcp_connect_test_server(&st, "remote").await;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "name": "echo", "arguments": {} }).to_string(),
        ))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data["content"][0]["text"], "hello");
}

#[tokio::test]
async fn mcp_remote_configured_headers_forward_on_connect_and_tool_call() {
    let root = unique_root();
    let st = state_at(&root);
    let seen = StdArc::new(AtomicUsize::new(0));
    let server = mcp_remote_auth_server(
        vec![
            mcp_json_response(json!({ "jsonrpc": "2.0", "id": 1, "result": {} })),
            mcp_json_response(json!({ "jsonrpc": "2.0", "id": 2, "result": { "tools": [] } })),
            mcp_json_response(json!({
                "jsonrpc": "2.0",
                "id": 3,
                "result": { "content": [{ "type": "text", "text": "authed" }] }
            })),
        ],
        seen.clone(),
    )
    .await;

    mcp_add_remote_auth_server(&st, "remote", &server.url, "Bearer static", 1000).await;
    mcp_connect_test_server(&st, "remote").await;
    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "name": "echo" }).to_string()))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(seen.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn mcp_remote_auth_update_persists_header_and_redacts_response() {
    let root = unique_root();
    fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        json!({
            "mcp": {
                "remote": { "type": "remote", "url": "http://127.0.0.1:1/mcp", "enabled": true }
            }
        })
        .to_string(),
    )
    .unwrap();
    let st = state_at(&root);

    let req = Request::builder()
        .method(Method::PUT)
        .uri("/mcp/remote/auth")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "token": "secret-token" }).to_string()))
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data["headers"]["Authorization"], "[redacted]");
    assert!(!data.to_string().contains("secret-token"));

    let req = Request::builder()
        .method(Method::GET)
        .uri("/config")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    let data = response_to_value(res).await;
    assert_eq!(
        data["mcp"]["remote"]["headers"]["Authorization"],
        "Bearer secret-token"
    );

    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    let data = response_to_value(res).await;
    assert!(!data.to_string().contains("secret-token"));
    assert_eq!(data["remote"]["status"], "failed");
}

#[tokio::test]
async fn mcp_remote_persisted_access_token_forwards_on_connect_and_tool_call() {
    let root = unique_root();
    let st = state_at(&root);
    let seen = StdArc::new(AtomicUsize::new(0));
    let server = mcp_remote_token_server(
        vec![
            mcp_json_response(json!({ "jsonrpc": "2.0", "id": 1, "result": {} })),
            mcp_json_response(json!({ "jsonrpc": "2.0", "id": 2, "result": { "tools": [] } })),
            mcp_json_response(json!({
                "jsonrpc": "2.0",
                "id": 3,
                "result": { "content": [{ "type": "text", "text": "token" }] }
            })),
        ],
        "Bearer fresh",
        seen.clone(),
    )
    .await;

    mcp_add_remote_server(&st, "remote", &server.url, 1000).await;
    st.store
        .set_mcp_auth(
            "remote",
            json!({ "accessToken": "fresh", "refreshToken": "refresh", "expiresAt": 4102444800i64 }),
        )
        .unwrap();
    mcp_connect_test_server(&st, "remote").await;
    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "name": "echo" }).to_string()))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(seen.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn mcp_remote_expired_token_refreshes_persists_and_uses_new_bearer() {
    let root = unique_root();
    let st = state_at(&root);
    let seen = StdArc::new(AtomicUsize::new(0));
    let token = mcp_token_server(json!({
        "access_token": "new-access",
        "refresh_token": "new-refresh",
        "expires_in": 3600,
        "scope": "tools"
    }))
    .await;
    let server = mcp_remote_token_server(
        vec![
            mcp_json_response(json!({ "jsonrpc": "2.0", "id": 1, "result": {} })),
            mcp_json_response(json!({ "jsonrpc": "2.0", "id": 2, "result": { "tools": [] } })),
        ],
        "Bearer new-access",
        seen.clone(),
    )
    .await;

    mcp_add_remote_server(&st, "remote", &server.url, 1000).await;
    st.store
        .set_mcp_auth(
            "remote",
            json!({
                "accessToken": "old-access",
                "refreshToken": "old-refresh",
                "expiresAt": 1,
                "tokenUrl": token.url,
                "clientId": "client",
                "clientSecret": "secret"
            }),
        )
        .unwrap();
    mcp_connect_test_server(&st, "remote").await;

    let auth = st.store.mcp_auth("remote").unwrap();
    assert_eq!(auth["accessToken"], "new-access");
    assert_eq!(auth["refreshToken"], "new-refresh");
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn mcp_remote_refresh_failure_returns_named_error_and_redacts_status() {
    let root = unique_root();
    let st = state_at(&root);
    let token = mcp_token_error_server().await;
    let server = mcp_remote_server(vec![]).await;

    mcp_add_remote_server(&st, "remote", &server.url, 1000).await;
    st.store
        .set_mcp_auth(
            "remote",
            json!({
                "accessToken": "old-access",
                "refreshToken": "secret-refresh",
                "expiresAt": 1,
                "tokenUrl": token.url,
                "clientSecret": "client-secret"
            }),
        )
        .unwrap();
    let connect = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/connect")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(connect).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let data = mcp_status_value(&st).await;
    assert_eq!(data["remote"]["status"], "failed");
    assert!(!data.to_string().contains("secret-refresh"));
    assert!(!data.to_string().contains("client-secret"));
    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "name": "echo" }).to_string()))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let data = response_to_value(res).await;
    assert!(!data.to_string().contains("secret-refresh"));
    assert!(!data.to_string().contains("client-secret"));
}

#[tokio::test]
async fn mcp_remote_auth_update_invalid_body_returns_named_error() {
    let root = unique_root();
    fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        json!({
            "mcp": {
                "remote": { "type": "remote", "url": "http://127.0.0.1:1/mcp", "enabled": true }
            }
        })
        .to_string(),
    )
    .unwrap();
    let st = state_at(&root);

    let req = Request::builder()
        .method(Method::PUT)
        .uri("/mcp/remote/auth")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "token": "", "headers": {} }).to_string(),
        ))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let data = response_to_value(res).await;
    assert_eq!(data["name"], "RustMcpAuthInvalidError");
    assert_eq!(data["data"]["server"], "remote");
}

#[tokio::test]
async fn mcp_oauth_authorize_validates_missing_config_with_named_error() {
    let root = unique_root();
    let st = state_at(&root);
    mcp_add_remote_server(&st, "remote", "http://127.0.0.1:1/mcp", 1000).await;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/oauth/authorize")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
    let data = response_to_value(res).await;
    assert_eq!(data["name"], "RustMcpOAuthDiscoveryError");
    assert!(data["data"]["message"]
        .as_str()
        .unwrap()
        .contains("metadata discovery"));
}

#[tokio::test]
async fn mcp_oauth_authorize_returns_pkce_url_and_persists_pending() {
    let root = unique_root();
    let st = state_at(&root);
    mcp_add_remote_oauth_server(
        &st,
        "remote",
        "http://127.0.0.1:1/mcp",
        "http://auth.test/authorize",
        "http://auth.test/token",
        "http://127.0.0.1:4099/mcp/remote/oauth/callback",
    )
    .await;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/oauth/authorize")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    let url = data["url"].as_str().unwrap();
    assert!(url.contains("client_id=client"));
    assert!(url
        .contains("redirect_uri=http%3A%2F%2F127.0.0.1%3A4099%2Fmcp%2Fremote%2Foauth%2Fcallback"));
    assert!(url.contains("state="));
    assert!(url.contains("code_challenge="));
    assert!(url.contains("code_challenge_method=S256"));
    assert!(url.contains("scope=tools+read"));
    assert_eq!(data["method"], "manual");
    assert!(data["state"].as_str().is_some());
    let auth = st.store.mcp_auth("remote").unwrap();
    assert_eq!(auth["pending"]["state"], data["state"]);
    assert!(auth["pending"]["codeVerifier"].as_str().unwrap().len() > 20);
    assert!(!data
        .to_string()
        .contains(auth["pending"]["codeVerifier"].as_str().unwrap()));
}

#[tokio::test]
async fn mcp_oauth_callback_rejects_wrong_state_with_html_error() {
    let root = unique_root();
    let st = state_at(&root);
    mcp_add_remote_oauth_server(
        &st,
        "remote",
        "http://127.0.0.1:1/mcp",
        "http://auth.test/authorize",
        "http://auth.test/token",
        "http://127.0.0.1:4099/mcp/remote/oauth/callback",
    )
    .await;
    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/oauth/authorize")
        .body(Body::empty())
        .unwrap();
    let _ = app(st.clone()).oneshot(req).await.unwrap();

    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp/remote/oauth/callback?code=abc&state=wrong")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let text = response_to_string(res).await;
    assert!(text.contains("RustMcpOAuthStateError"));
}

#[tokio::test]
async fn mcp_oauth_callback_exchanges_persists_and_uses_bearer() {
    let root = unique_root();
    let st = state_at(&root);
    let token = mcp_token_server(json!({
        "access_token": "oauth-access",
        "refresh_token": "oauth-refresh",
        "expires_in": 3600,
        "scope": "tools read"
    }))
    .await;
    let seen = StdArc::new(AtomicUsize::new(0));
    let server = mcp_remote_token_server(
        vec![
            mcp_json_response(json!({ "jsonrpc": "2.0", "id": 1, "result": {} })),
            mcp_json_response(json!({ "jsonrpc": "2.0", "id": 2, "result": { "tools": [] } })),
            mcp_json_response(json!({ "jsonrpc": "2.0", "id": 3, "result": { "content": [{ "type": "text", "text": "oauth" }] } })),
        ],
        "Bearer oauth-access",
        seen.clone(),
    )
    .await;
    mcp_add_remote_oauth_server(
        &st,
        "remote",
        &server.url,
        "http://auth.test/authorize",
        &token.url,
        "http://127.0.0.1:4099/mcp/remote/oauth/callback",
    )
    .await;
    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/oauth/authorize")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    let data = response_to_value(res).await;
    let state = data["state"].as_str().unwrap();

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/mcp/remote/oauth/callback?code=abc&state={state}"))
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let text = response_to_string(res).await;
    assert!(text.contains("Authorization succeeded"));
    assert!(!text.contains("oauth-access"));
    let auth = st.store.mcp_auth("remote").unwrap();
    assert_eq!(auth["accessToken"], "oauth-access");
    assert_eq!(auth["refreshToken"], "oauth-refresh");
    assert!(auth.get("pending").is_none());

    mcp_connect_test_server(&st, "remote").await;
    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "name": "echo" }).to_string()))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(seen.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn mcp_oauth_callback_exchange_error_redacts_secrets() {
    let root = unique_root();
    let st = state_at(&root);
    let token = mcp_token_error_server().await;
    mcp_add_remote_oauth_server(
        &st,
        "remote",
        "http://127.0.0.1:1/mcp",
        "http://auth.test/authorize",
        &token.url,
        "http://127.0.0.1:4099/mcp/remote/oauth/callback",
    )
    .await;
    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/oauth/authorize")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    let data = response_to_value(res).await;
    let state = data["state"].as_str().unwrap();

    let req = Request::builder()
        .method(Method::GET)
        .uri(format!(
            "/mcp/remote/oauth/callback?code=secret-code&state={state}"
        ))
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    let text = response_to_string(res).await;
    assert!(text.contains("RustMcpAuthRefreshError"));
    assert!(!text.contains("secret-code"));
    assert!(!text.contains("client-secret"));
}

#[tokio::test]
async fn mcp_oauth_authorize_discovers_metadata_without_explicit_endpoints() {
    let root = unique_root();
    let st = state_at(&root);
    let oauth = mcp_oauth_metadata_server(None).await;
    mcp_add_remote_server(&st, "remote", &format!("{}/mcp", oauth.root), 1000).await;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/oauth/authorize")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    let url = data["url"].as_str().unwrap();
    assert!(url.starts_with(&format!("{}/authorize?", oauth.root)));
    assert!(url.contains("client_id=registered-client"));
    assert!(url.contains("scope=tools+read"));
}

#[tokio::test]
async fn mcp_oauth_authorize_registers_and_reuses_dynamic_client() {
    let root = unique_root();
    let st = state_at(&root);
    let count = StdArc::new(AtomicUsize::new(0));
    let oauth = mcp_oauth_metadata_server(Some(count.clone())).await;
    mcp_add_remote_server(&st, "remote", &format!("{}/mcp", oauth.root), 1000).await;

    for _ in 0..2 {
        let req = Request::builder()
            .method(Method::POST)
            .uri("/mcp/remote/oauth/authorize")
            .body(Body::empty())
            .unwrap();
        let res = app(st.clone()).oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let data = response_to_value(res).await;
        assert!(data["url"]
            .as_str()
            .unwrap()
            .contains("client_id=registered-client"));
        assert!(!data.to_string().contains("registered-secret"));
    }

    assert_eq!(count.load(Ordering::SeqCst), 1);
    let auth = st.store.mcp_auth("remote").unwrap();
    assert_eq!(auth["client"]["clientId"], "registered-client");
    assert_eq!(auth["client"]["clientSecret"], "registered-secret");
}

#[tokio::test]
async fn mcp_oauth_explicit_config_overrides_discovered_metadata() {
    let root = unique_root();
    let st = state_at(&root);
    let oauth = mcp_oauth_metadata_server(None).await;
    mcp_add_remote_oauth_server(
        &st,
        "remote",
        &format!("{}/mcp", oauth.root),
        "http://auth.test/authorize",
        "http://auth.test/token",
        "http://127.0.0.1:4099/mcp/remote/oauth/callback",
    )
    .await;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/oauth/authorize")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    let url = data["url"].as_str().unwrap();
    assert!(url.starts_with("http://auth.test/authorize?"));
    assert!(url.contains("client_id=client"));
}

#[tokio::test]
async fn mcp_oauth_invalid_metadata_returns_named_error_without_secret() {
    let root = unique_root();
    let st = state_at(&root);
    let oauth = mcp_oauth_bad_metadata_server().await;
    mcp_add_remote_server(&st, "remote", &format!("{}/mcp", oauth.root), 1000).await;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/oauth/authorize")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_GATEWAY);
    let data = response_to_value(res).await;
    assert_eq!(data["name"], "RustMcpOAuthDiscoveryError");
    assert!(!data.to_string().contains("registered-secret"));
}

#[tokio::test]
async fn mcp_remote_sse_response_parsing_discovers_tools() {
    let root = unique_root();
    let st = state_at(&root);
    let server = mcp_remote_server(vec![
        mcp_json_response(json!({ "jsonrpc": "2.0", "id": 1, "result": {} })),
        mcp_sse_response("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":[{\"name\":\"sse\",\"inputSchema\":{}}]}}\n\n"),
    ])
    .await;

    mcp_add_remote_server(&st, "remote", &server.url, 1000).await;
    mcp_connect_test_server(&st, "remote").await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    let data = response_to_value(res).await;
    assert_eq!(data["remote"]["status"], "connected");
    assert_eq!(data["remote"]["tools"][0]["name"], "sse");
}

#[tokio::test]
async fn mcp_remote_rpc_error_returns_named_error() {
    let root = unique_root();
    let st = state_at(&root);
    let server = mcp_remote_server(vec![
        mcp_json_response(json!({ "jsonrpc": "2.0", "id": 1, "result": {} })),
        mcp_json_response(json!({ "jsonrpc": "2.0", "id": 2, "result": { "tools": [] } })),
        mcp_json_response(
            json!({ "jsonrpc": "2.0", "id": 3, "error": { "message": "remote failed" } }),
        ),
    ])
    .await;

    mcp_add_remote_server(&st, "remote", &server.url, 1000).await;
    mcp_connect_test_server(&st, "remote").await;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/remote/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "name": "fail", "arguments": {} }).to_string(),
        ))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let data = response_to_value(res).await;
    assert_eq!(data["name"], "RustMcpToolError");
    assert_eq!(data["data"]["message"], "remote failed");
}

#[tokio::test]
async fn mcp_remote_tool_failure_marks_only_that_server_failed() {
    let root = unique_root();
    let st = state_at(&root);
    let bad = mcp_remote_server(vec![
        mcp_json_response(json!({ "jsonrpc": "2.0", "id": 1, "result": {} })),
        mcp_json_response(json!({ "jsonrpc": "2.0", "id": 2, "result": { "tools": [] } })),
        mcp_remote_response("500 Internal Server Error", "application/json", "{}"),
    ])
    .await;
    let ok = mcp_remote_server(vec![
        mcp_json_response(json!({ "jsonrpc": "2.0", "id": 1, "result": {} })),
        mcp_json_response(json!({ "jsonrpc": "2.0", "id": 2, "result": { "tools": [] } })),
        mcp_json_response(json!({
            "jsonrpc": "2.0",
            "id": 3,
            "result": { "content": [{ "type": "text", "text": "ok" }] }
        })),
    ])
    .await;

    mcp_add_remote_server(&st, "bad", &bad.url, 1000).await;
    mcp_add_remote_server(&st, "ok", &ok.url, 1000).await;
    mcp_connect_test_server(&st, "bad").await;
    mcp_connect_test_server(&st, "ok").await;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/bad/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "name": "echo" }).to_string()))
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);

    let data = mcp_status_value(&st).await;
    assert_eq!(data["bad"]["status"], "failed");
    assert_eq!(data["ok"]["status"], "connected");

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/ok/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "name": "echo" }).to_string()))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data["content"][0]["text"], "ok");
}
