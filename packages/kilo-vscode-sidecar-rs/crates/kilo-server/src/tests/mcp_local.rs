//! Local (stdio) MCP tests: status discovery, lifecycle (connect /
//! disconnect / re-connect), tool-call dispatch, child-exit handling,
//! and stdio RPC error paths.

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use serde_json::json;
use std::fs;
use std::time::Duration;
use tower::ServiceExt;

use crate::http::build_router as app;

use super::common::{
    mcp_add_test_server, mcp_connect_test_server, mcp_script_runtime, mcp_status_value,
    response_to_value, state_at, unique_root, MCP_EXIT_SERVER_JS, MCP_STALE_RESPONSE_SERVER_JS,
    MCP_TEST_SERVER_JS,
};

#[tokio::test]
async fn mcp_status_empty_config_returns_empty_map() {
    let root = unique_root();
    let st = state_at(&root);
    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();

    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data, json!({}));
}

#[tokio::test]
async fn mcp_status_lists_configured_servers_without_launching() {
    let root = unique_root();
    let st = state_at(&root);
    let dir = root.join("config").join("kilo");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
            dir.join("kilo.json"),
            json!({
                "mcp": {
                    "playwright": { "type": "local", "command": ["npx", "@playwright/mcp"], "enabled": true },
                    "off": { "type": "remote", "url": "https://example.test/mcp", "enabled": false },
                    "legacy": { "command": ["node", "server.js"] }
                }
            })
            .to_string(),
        )
        .unwrap();
    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();

    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;

    assert_eq!(data["playwright"]["status"], "failed");
    assert_eq!(
        data["playwright"]["error"],
        "Rust sidecar MCP local server is not connected."
    );
    assert_eq!(data["off"]["status"], "disabled");
    assert!(data.get("legacy").is_none());
}

#[tokio::test]
async fn mcp_connect_missing_command_records_failed_status() {
    let root = unique_root();
    let st = state_at(&root);
    let add = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "name": "missing",
                "config": { "type": "local", "command": ["kilo-mcp-definitely-missing-command"] }
            })
            .to_string(),
        ))
        .unwrap();

    let res = app(st.clone()).oneshot(add).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let connect = Request::builder()
        .method(Method::POST)
        .uri("/mcp/missing/connect")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(connect).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_to_value(res).await, json!(true));

    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    let data = response_to_value(res).await;
    assert_eq!(data["missing"]["status"], "failed");
    assert!(data["missing"]["error"]
        .as_str()
        .unwrap_or_default()
        .contains("Unable to start MCP server"));
}

#[tokio::test]
async fn mcp_stdio_connect_discovers_tools_and_disconnects() {
    let root = unique_root();
    fs::create_dir_all(root.join("repo")).unwrap();
    let st = state_at(&root);
    let Some(runtime) = mcp_script_runtime() else {
        eprintln!("skipping mcp stdio discovery test: no node or bun command found");
        return;
    };
    let script = root.join("mcp-test-server.js");
    fs::write(&script, MCP_TEST_SERVER_JS).unwrap();
    let add = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({
                "name": "browser",
                "config": {
                    "type": "local",
                    "command": [runtime, script],
                    "enabled": true,
                    "timeout": 1000,
                },
            })
            .to_string(),
        ))
        .unwrap();

    let res = app(st.clone()).oneshot(add).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data["browser"]["status"], "failed");

    let connect = Request::builder()
        .method(Method::POST)
        .uri("/mcp/browser/connect")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(connect).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data, json!(true));

    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    let data = response_to_value(res).await;
    assert_eq!(data["browser"]["status"], "connected");
    assert_eq!(data["browser"]["tools"][0]["name"], "echo");
    assert_eq!(data["browser"]["tools"][0]["description"], "Echo input");
    assert_eq!(data["browser"]["tools"][0]["inputSchema"]["type"], "object");

    let disconnect = Request::builder()
        .method(Method::POST)
        .uri("/mcp/browser/disconnect")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(disconnect).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data, json!(true));

    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    let data = response_to_value(res).await;
    assert_eq!(data["browser"]["status"], "disabled");

    let missing = Request::builder()
        .method(Method::POST)
        .uri("/mcp/missing/connect")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(missing).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    let data = response_to_value(res).await;
    assert_eq!(data["name"], "RustMcpNotImplementedError");
    assert_eq!(data["data"]["server"], "missing");
}

#[tokio::test]
async fn mcp_stdio_tool_call_returns_result() {
    let root = unique_root();
    fs::create_dir_all(root.join("repo")).unwrap();
    let st = state_at(&root);
    let Some(runtime) = mcp_script_runtime() else {
        eprintln!("skipping mcp stdio tool call test: no node or bun command found");
        return;
    };
    let script = root.join("mcp-test-server.js");
    fs::write(&script, MCP_TEST_SERVER_JS).unwrap();

    mcp_add_test_server(&st, "browser", &runtime, &script, 1000).await;
    mcp_connect_test_server(&st, "browser").await;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/browser/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "name": "echo", "arguments": { "text": "hello" } }).to_string(),
        ))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(
        data,
        json!({ "content": [{ "type": "text", "text": "hello" }] })
    );
}

#[tokio::test]
async fn mcp_stdio_tools_list_changed_refreshes_tools() {
    let root = unique_root();
    fs::create_dir_all(root.join("repo")).unwrap();
    let st = state_at(&root);
    let Some(runtime) = mcp_script_runtime() else {
        eprintln!("skipping mcp stdio notification test: no node or bun command found");
        return;
    };
    let script = root.join("mcp-test-server.js");
    fs::write(&script, MCP_TEST_SERVER_JS).unwrap();

    mcp_add_test_server(&st, "browser", &runtime, &script, 1000).await;
    mcp_connect_test_server(&st, "browser").await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data["browser"]["tools"][0]["name"], "echo");

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data["browser"]["status"], "connected");
    assert_eq!(data["browser"]["tools"][0]["name"], "echo2");
    assert_eq!(
        data["browser"]["tools"][0]["description"],
        "Echo input changed"
    );

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/browser/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "name": "echo2", "arguments": { "text": "after" } }).to_string(),
        ))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(
        data,
        json!({ "content": [{ "type": "text", "text": "after" }] })
    );
}

#[tokio::test]
async fn mcp_stdio_child_exit_is_failed_and_reconnects() {
    let root = unique_root();
    fs::create_dir_all(root.join("repo")).unwrap();
    let st = state_at(&root);
    let Some(runtime) = mcp_script_runtime() else {
        eprintln!("skipping mcp stdio reconnect test: no node or bun command found");
        return;
    };
    let script = root.join("mcp-exit-server.js");
    fs::write(&script, MCP_EXIT_SERVER_JS).unwrap();

    mcp_add_test_server(&st, "flaky", &runtime, &script, 1000).await;
    mcp_connect_test_server(&st, "flaky").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let data = mcp_status_value(&st).await;
    assert_eq!(data["flaky"]["status"], "failed");
    assert!(data["flaky"]["error"]
        .as_str()
        .unwrap_or_default()
        .contains("MCP server exited"));

    mcp_connect_test_server(&st, "flaky").await;
    let data = mcp_status_value(&st).await;
    assert_eq!(data["flaky"]["status"], "connected");
    assert_eq!(data["flaky"]["tools"][0]["name"], "echo");
}

#[tokio::test]
async fn mcp_stdio_one_failed_server_does_not_break_another() {
    let root = unique_root();
    fs::create_dir_all(root.join("repo")).unwrap();
    let st = state_at(&root);
    let Some(runtime) = mcp_script_runtime() else {
        eprintln!("skipping mcp stdio multi-server test: no node or bun command found");
        return;
    };
    let ok = root.join("mcp-test-server.js");
    let bad = root.join("mcp-exit-server.js");
    fs::write(&ok, MCP_TEST_SERVER_JS).unwrap();
    fs::write(&bad, MCP_EXIT_SERVER_JS).unwrap();

    mcp_add_test_server(&st, "ok", &runtime, &ok, 1000).await;
    mcp_add_test_server(&st, "bad", &runtime, &bad, 1000).await;
    mcp_connect_test_server(&st, "ok").await;
    mcp_connect_test_server(&st, "bad").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let data = mcp_status_value(&st).await;
    assert_eq!(data["bad"]["status"], "failed");
    assert_eq!(data["ok"]["status"], "connected");

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/ok/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "name": "echo", "arguments": { "text": "alive" } }).to_string(),
        ))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data["content"][0]["text"], "alive");
}

#[tokio::test]
async fn mcp_stdio_tool_call_missing_server_returns_named_error() {
    let root = unique_root();
    let st = state_at(&root);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/missing/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "name": "echo" }).to_string()))
        .unwrap();

    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    let data = response_to_value(res).await;
    assert_eq!(data["name"], "RustMcpNotImplementedError");
    assert_eq!(data["data"]["server"], "missing");
}

/// Fix 1: regression coverage for the JSON-RPC id-channel misroute. The
/// fixture server emits a stale `id=1` frame BEFORE the real response.
/// With the monotonic id allocator the client allocates a fresh id
/// (>=100), so the stale `id=1` body must be discarded and the call must
/// return the real result. Under the old fixed-`MCP_CALL_ID = 3` plus
/// `MCP_RESOURCE_READ_ID = 7` constants any late prior response with the
/// same id would short-circuit the wait loop and a tools/call could
/// accidentally bind to a resources/read body (or vice versa).
#[tokio::test]
async fn mcp_id_channel_does_not_misroute_late_response() {
    let root = unique_root();
    fs::create_dir_all(root.join("repo")).unwrap();
    let st = state_at(&root);
    let Some(runtime) = mcp_script_runtime() else {
        eprintln!("skipping mcp id misroute test: no node or bun command found");
        return;
    };
    let script = root.join("mcp-stale-server.js");
    fs::write(&script, MCP_STALE_RESPONSE_SERVER_JS).unwrap();

    crate::tests::common::mcp_add_test_server(&st, "stale", &runtime, &script, 1000).await;
    crate::tests::common::mcp_connect_test_server(&st, "stale").await;

    // Two back-to-back calls. Each gets a stale id=1 frame followed by
    // the real response. Both must return the real body.
    for text in ["alpha", "beta"] {
        let req = Request::builder()
            .method(Method::POST)
            .uri("/mcp/stale/tool")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                json!({ "name": "echo", "arguments": { "text": text } }).to_string(),
            ))
            .unwrap();
        let res = app(st.clone()).oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let data = response_to_value(res).await;
        assert_eq!(
            data,
            json!({ "content": [{ "type": "text", "text": text }] }),
            "stale id=1 frame leaked into call result"
        );
    }
}

/// Fix 1 unit slice: every call to `mcp_alloc_id` must hand back a
/// distinct value. A re-used id is the precondition for late-response
/// misrouting.
#[tokio::test]
async fn mcp_alloc_id_returns_unique_values() {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    for _ in 0..1000 {
        let id = crate::routes::mcp::mcp_alloc_id();
        assert!(seen.insert(id), "duplicate id from mcp_alloc_id: {id}");
    }
}

/// Fix 3: CSRF state compare uses a constant-time helper. Smoke check
/// the helper's contract — equal slices match, prefix mismatches don't,
/// length mismatches don't.
#[tokio::test]
async fn mcp_csrf_state_compare_is_constant_time() {
    use crate::routes::mcp::constant_time_eq;
    assert!(constant_time_eq(b"abc", b"abc"));
    assert!(!constant_time_eq(b"abc", b"abd"));
    assert!(!constant_time_eq(b"abc", b"abcd"));
    assert!(!constant_time_eq(b"", b"a"));
    assert!(constant_time_eq(b"", b""));
    // Sanity: also rejects an empty `got` even when the expected state
    // happens to be empty-prefix — the comparator can't short-circuit
    // and accidentally accept the empty input as a match.
    assert!(!constant_time_eq(b"longer-state", b"longer-stat"));
}

#[tokio::test]
async fn mcp_stdio_tool_call_rpc_error_returns_named_error() {
    let root = unique_root();
    fs::create_dir_all(root.join("repo")).unwrap();
    let st = state_at(&root);
    let Some(runtime) = mcp_script_runtime() else {
        eprintln!("skipping mcp stdio tool error test: no node or bun command found");
        return;
    };
    let script = root.join("mcp-test-server.js");
    fs::write(&script, MCP_TEST_SERVER_JS).unwrap();

    mcp_add_test_server(&st, "browser", &runtime, &script, 1000).await;
    mcp_connect_test_server(&st, "browser").await;

    let req = Request::builder()
        .method(Method::POST)
        .uri("/mcp/browser/tool")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "name": "fail" }).to_string()))
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let data = response_to_value(res).await;
    assert_eq!(data["name"], "RustMcpToolError");
    assert_eq!(data["data"]["message"], "tool failed");
    assert_eq!(data["data"]["server"], "browser");
}
