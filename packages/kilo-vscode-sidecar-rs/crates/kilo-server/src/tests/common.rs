//! Shared test fixtures and helpers used across multiple `tests/*.rs`
//! files. Each item here is `pub(super)` so siblings can reach it via
//! `use super::common::*;` while remaining invisible outside the
//! `tests` module tree.

use axum::{
    extract::{Json, Path, State},
    http::{header, Method, Request, StatusCode},
    response::Response,
};
use http_body_util::BodyExt;
use kilo_protocol::GlobalEvent;
use kilo_store::Store;
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::path::Path as FsPath;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc as StdArc;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, RwLock};
use tower::ServiceExt;

use crate::http::build_router as app;
use crate::oauth::OPENAI_ISSUER;
use crate::routes::config::oauth_callback;
use crate::util::git::GIT;
use crate::AppState;

pub(super) const CODEX_TOOL_CALL_STREAM: &str =
    include_str!("../../../../fixtures/openai-responses/codex-tool-call.sse");
pub(super) const CODEX_FINAL_TEXT_STREAM: &str =
    include_str!("../../../../fixtures/openai-responses/codex-final-text.sse");

pub(super) fn fixture_stream(raw: &'static str) -> String {
    raw.replace("\r\n", "\n")
}

pub(super) fn state() -> Arc<AppState> {
    state_at_with(
        None,
        SocketAddr::from(([127, 0, 0, 1], 0)),
        format!("{OPENAI_ISSUER}/oauth/token"),
    )
}

pub(super) fn state_with_listener_addr(addr: SocketAddr) -> Arc<AppState> {
    state_at_with(None, addr, format!("{OPENAI_ISSUER}/oauth/token"))
}

pub(super) fn state_with_token_endpoint(endpoint: String) -> Arc<AppState> {
    state_at_with(None, SocketAddr::from(([127, 0, 0, 1], 0)), endpoint)
}

pub(super) fn state_at(root: &std::path::Path) -> Arc<AppState> {
    state_at_with(
        Some(store(root)),
        SocketAddr::from(([127, 0, 0, 1], 0)),
        format!("{OPENAI_ISSUER}/oauth/token"),
    )
}

pub(super) fn oauth_auth() -> Value {
    json!({
        "type": "oauth",
        "refresh": "refresh-token",
        "access": "e30.eyJleHAiOjQxMDI0NDQ4MDAsImNoYXRncHRfYWNjb3VudF9pZCI6ImFjY3RfMSJ9.sig",
        "expires": 9999999999999i64,
        "accountId": "acct_1"
    })
}

pub(super) fn write_openai_config(root: &std::path::Path, url: &str) {
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": { "openai": { "options": { "baseURL": url }, "models": {} } }
        }))
        .unwrap(),
    )
    .unwrap();
}

pub(super) fn write_command(root: &std::path::Path, name: &str, text: &str) {
    let dir = root.join("config").join("kilo").join("command");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{name}.md")), text).unwrap();
}

pub(super) fn ok_stream() -> &'static str {
    "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n"
}

pub(super) fn state_at_with(
    store: Option<Store>,
    addr: SocketAddr,
    endpoint: String,
) -> Arc<AppState> {
    // Capacity 256: each store event now emits two frames (bus-shape mirror +
    // sync envelope) per the Bun-parity change in `http/sse.rs::publish_events`,
    // and several tests run a full prompt turn that fires ≥10 store mutations
    // back-to-back. The previous 16-slot channel overflowed and `drain` saw
    // a `Lagged` error before any frames were collected.
    let (bus, _) = broadcast::channel(256);
    Arc::new(AppState {
        username: "kilo".to_string(),
        password: None,
        store: store.unwrap_or_else(Store::new),
        bus,
        viewed: RwLock::default(),
        runners: Mutex::default(),
        runner_notify: tokio::sync::Notify::new(),
        prompt_queues: Mutex::default(),
        prompt_queue_versions: Mutex::default(),
        permissions: Mutex::default(),
        approvals: Mutex::default(),
        questions: Mutex::default(),
        suggestions: Mutex::default(),
        network: Mutex::default(),
        mcp: Mutex::default(),
        mcp_configs: Mutex::default(),
        mcp_children: Mutex::default(),
        pty: Mutex::default(),
        plugin_tools: Mutex::default(),
        session_agents: Mutex::default(),
        session_hard_rules: Mutex::default(),
        broken_turn_anchors: Mutex::default(),
        oauth_pending: Mutex::default(),
        oauth_listener: Mutex::default(),
        oauth_listener_addr: addr,
        oauth_token_endpoint: endpoint,
        sse_capacity: AppState::new_sse_capacity(),
    })
}

pub(super) fn store(root: &std::path::Path) -> Store {
    Store::for_test(root)
}

pub(super) fn init_git_repo(repo: &std::path::Path) -> bool {
    git(repo, &["init", "-b", "main"])
        || (git(repo, &["init"]) && git(repo, &["checkout", "-B", "main"]))
}

pub(super) fn git(repo: &std::path::Path, args: &[&str]) -> bool {
    let out = Command::new(GIT)
        .args(args)
        .env("GIT_AUTHOR_NAME", "Kilo Test")
        .env("GIT_AUTHOR_EMAIL", "kilo@example.test")
        .env("GIT_COMMITTER_NAME", "Kilo Test")
        .env("GIT_COMMITTER_EMAIL", "kilo@example.test")
        .current_dir(repo)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output();
    out.is_ok_and(|out| out.status.success())
}

/// Audit Fix 3 helpers: read a `Response`'s JSON body in tests.
pub(super) async fn response_to_value(res: Response) -> Value {
    let bytes = axum::body::to_bytes(res.into_body(), 64 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(json!(null))
}

pub(super) async fn response_to_string(res: Response) -> String {
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

pub(super) async fn oauth_callback_dispatch(st: Arc<AppState>, id: &str, input: Value) -> Response {
    oauth_callback(State(st), Path(id.to_string()), Json(input)).await
}

/// Process-wide guard for tests that mutate `KILO_AUTH_CONTENT` /
/// `OPENAI_*_KEY` env vars from the server crate. Required because
/// `kilo-provider` already holds its own env-mutex; without this guard
/// a concurrent provider test could see our env state.
pub(super) static ENV_RESOLVE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub(super) fn seed(store: &Store) {
    store.seed_for_test();
}

pub(super) fn drain(rx: &mut broadcast::Receiver<crate::http::sse::BusEvent>) -> Vec<GlobalEvent> {
    // The bus carries pre-serialized `BusEvent`s; tests want typed
    // `GlobalEvent`s for assertions. Round-tripping back through
    // `serde_json::from_str` is the cheapest way to keep the existing
    // assertion shape without rewriting hundreds of test bodies.
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event.as_global());
    }
    events
}

pub(super) fn assert_sync(event: &GlobalEvent, kind: &str, role: &str, text: Option<&str>) {
    let data = event.payload.sync_event.as_ref().unwrap();
    assert_eq!(event.payload.kind, "sync");
    // After the Bun-parity bus-mirror change in `http/sse.rs::publish_events`,
    // the sync envelope's `syncEvent.type` is the canonical `.1` form
    // (`packages/sdk/js/src/v2/gen/types.gen.ts:1175`). Accept either the new
    // `.1` suffix or the legacy `.v1` so callers can assert against the form
    // they're most familiar with.
    let actual = data["type"].as_str().unwrap_or("");
    let actual_base = actual
        .strip_suffix(".1")
        .or_else(|| actual.strip_suffix(".v1"))
        .unwrap_or(actual);
    let expected_base = kind
        .strip_suffix(".1")
        .or_else(|| kind.strip_suffix(".v1"))
        .unwrap_or(kind);
    assert_eq!(
        actual_base, expected_base,
        "sync envelope type mismatch: expected `{kind}`, got `{actual}`",
    );
    if !role.is_empty() {
        assert_eq!(data["data"]["info"]["role"], role);
    }
    if let Some(text) = text {
        assert_eq!(data["data"]["part"]["text"], text);
    }
}

/// True when the event is a bus-shape mirror of a store mutation (added by
/// `http/sse.rs::publish_events` to mirror Bun's `ProjectBus.publish` shape).
/// Tests that assert on indexed event ordering use this to filter out the
/// new mirrors while still seeing every non-store bus event (turn.open,
/// status, idle, message.part.delta, etc.) and every sync envelope.
pub(super) fn is_store_bus_mirror(event: &GlobalEvent) -> bool {
    matches!(
        event.payload.kind.as_str(),
        "session.created"
            | "session.updated"
            | "session.deleted"
            | "message.updated"
            | "message.removed"
            | "message.part.updated"
            | "message.part.removed"
    )
}

/// Drain that drops the new bus-shape store-event mirrors. Use this in tests
/// that assert on `events[N]` ordering and want to keep the pre-mirror
/// indexing semantics. Tests that need the bus-shape mirror itself should
/// call `drain` directly and inspect both halves.
pub(super) fn drain_no_store_mirror(
    rx: &mut broadcast::Receiver<crate::http::sse::BusEvent>,
) -> Vec<GlobalEvent> {
    drain(rx)
        .into_iter()
        .filter(|event| !is_store_bus_mirror(event))
        .collect()
}

/// Pull events one-by-one until a sync envelope appears, returning it. The
/// bus-shape mirrors that precede the sync envelope are dropped. Use this to
/// replace direct `rx.try_recv()` calls in tests that expected a sync event.
pub(super) fn recv_sync(rx: &mut broadcast::Receiver<crate::http::sse::BusEvent>) -> GlobalEvent {
    loop {
        let event = rx
            .try_recv()
            .expect("expected a sync event but the bus was empty")
            .as_global();
        if event.payload.kind == "sync" {
            return event;
        }
        // Skip non-sync bus frames (mirrors, turn.open, status, idle, etc.).
    }
}

pub(super) fn assert_delta(event: &GlobalEvent, mid: &Value, pid: &Value, delta: &str) {
    assert_eq!(event.payload.kind, "message.part.delta");
    assert_eq!(event.payload.properties["messageID"], *mid);
    assert_eq!(event.payload.properties["partID"], *pid);
    assert_eq!(event.payload.properties["field"], "text");
    assert_eq!(event.payload.properties["delta"], delta);
}

pub(super) fn unique_root() -> std::path::PathBuf {
    static IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = IDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let name = format!(
        "kilo-server-test-{}-{}-{seq}",
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
        std::process::id()
    );
    std::env::temp_dir().join(name)
}

pub(super) struct TestProvider {
    pub url: String,
    pub body: Arc<Mutex<String>>,
}

pub(super) struct TestStreamProvider {
    pub url: String,
    pub bodies: Arc<Mutex<Vec<String>>>,
}

pub(super) async fn provider_server(res: Value) -> TestProvider {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = Arc::new(Mutex::new(String::new()));
    let copy = body.clone();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        // 64 KiB buffer: after the codex.txt + env-block addition to OAuth
        // instructions (`agent/openai_stream.rs::prompt_instructions`), real
        // prompt bodies serialize to ~14 KiB. The previous 8 KiB read got a
        // partial request, the mock then wrote its response, and reqwest
        // surfaced "error sending request" when its remaining write found
        // the half-closed socket.
        let mut buf = vec![0; 65536];
        let size = socket.read(&mut buf).await.unwrap();
        *copy.lock().unwrap() = String::from_utf8_lossy(&buf[..size]).to_string();
        let body = serde_json::to_string(&res).unwrap();
        let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(body.as_bytes()).await.unwrap();
    });
    TestProvider {
        url: format!("http://{addr}/v1"),
        body,
    }
}

pub(super) async fn stream_provider_server(data: &'static str) -> TestProvider {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = Arc::new(Mutex::new(String::new()));
    let copy = body.clone();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        // 64 KiB buffer: after the codex.txt + env-block addition to OAuth
        // instructions (`agent/openai_stream.rs::prompt_instructions`), real
        // prompt bodies serialize to ~14 KiB. The previous 8 KiB read got a
        // partial request, the mock then wrote its response, and reqwest
        // surfaced "error sending request" when its remaining write found
        // the half-closed socket.
        let mut buf = vec![0; 65536];
        let size = socket.read(&mut buf).await.unwrap();
        *copy.lock().unwrap() = String::from_utf8_lossy(&buf[..size]).to_string();
        let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                data.len()
            );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(data.as_bytes()).await.unwrap();
    });
    TestProvider {
        url: format!("http://{addr}"),
        body,
    }
}

pub(super) async fn stream_provider_sequence(data: Vec<String>) -> TestStreamProvider {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let copy = bodies.clone();
    tokio::spawn(async move {
        for item in data {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 65536];
            let size = socket.read(&mut buf).await.unwrap();
            copy.lock()
                .unwrap()
                .push(String::from_utf8_lossy(&buf[..size]).to_string());
            let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    item.len()
                );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(item.as_bytes()).await.unwrap();
        }
    });
    TestStreamProvider {
        url: format!("http://{addr}"),
        bodies,
    }
}

/// Stalling SSE provider used by Fix 4's mid-stream abort coverage.
/// Sends `prefix` (no terminating `[DONE]`) then sleeps for ~5s — long
/// enough that any test reaching it will hit the timeout unless the
/// cancel-during-stream race short-circuits the bytes loop. Uses
/// `Transfer-Encoding: chunked` so the client cannot satisfy a
/// content-length and pre-emptively close the body.
pub(super) async fn stream_provider_stalling_server(prefix: &'static str) -> TestProvider {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = Arc::new(Mutex::new(String::new()));
    let copy = body.clone();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        // 64 KiB buffer: after the codex.txt + env-block addition to OAuth
        // instructions (`agent/openai_stream.rs::prompt_instructions`), real
        // prompt bodies serialize to ~14 KiB. The previous 8 KiB read got a
        // partial request, the mock then wrote its response, and reqwest
        // surfaced "error sending request" when its remaining write found
        // the half-closed socket.
        let mut buf = vec![0; 65536];
        let size = socket.read(&mut buf).await.unwrap();
        *copy.lock().unwrap() = String::from_utf8_lossy(&buf[..size]).to_string();
        let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n";
        let _ = socket.write_all(head.as_bytes()).await;
        // Chunk = <hex-len>\r\n<data>\r\n
        let chunk = format!("{:x}\r\n{}\r\n", prefix.len(), prefix);
        let _ = socket.write_all(chunk.as_bytes()).await;
        // Stall — caller is expected to abort before this sleeps out.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let _ = socket.write_all(b"0\r\n\r\n").await;
    });
    TestProvider {
        url: format!("http://{addr}"),
        body,
    }
}

// --------------------------------------------------------------------
// MCP shared helpers (used by both `mcp_local` and `mcp_remote`).
// --------------------------------------------------------------------

pub(super) async fn mcp_add_test_server(
    st: &Arc<AppState>,
    name: &str,
    runtime: &str,
    script: &FsPath,
    timeout: u64,
) {
    let add = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(
            json!({
                "name": name,
                "config": {
                    "type": "local",
                    "command": [runtime, script],
                    "enabled": true,
                    "timeout": timeout,
                },
            })
            .to_string(),
        ))
        .unwrap();

    let res = app(st.clone()).oneshot(add).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

pub(super) async fn mcp_add_remote_server(st: &Arc<AppState>, name: &str, url: &str, timeout: u64) {
    let add = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(
            json!({
                "name": name,
                "config": { "type": "remote", "url": url, "enabled": true, "timeout": timeout },
            })
            .to_string(),
        ))
        .unwrap();
    let res = app(st.clone()).oneshot(add).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

pub(super) async fn mcp_add_remote_auth_server(
    st: &Arc<AppState>,
    name: &str,
    url: &str,
    auth: &str,
    timeout: u64,
) {
    let add = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(
            json!({
                "name": name,
                "config": {
                    "type": "remote",
                    "url": url,
                    "enabled": true,
                    "timeout": timeout,
                    "headers": { "Authorization": auth }
                },
            })
            .to_string(),
        ))
        .unwrap();
    let res = app(st.clone()).oneshot(add).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

pub(super) async fn mcp_add_remote_oauth_server(
    st: &Arc<AppState>,
    name: &str,
    url: &str,
    authorize: &str,
    token: &str,
    redirect: &str,
) {
    let add = Request::builder()
        .method(Method::POST)
        .uri("/mcp")
        .header(header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(
            json!({
                "name": name,
                "config": {
                    "type": "remote",
                    "url": url,
                    "enabled": true,
                    "timeout": 1000,
                    "oauth": {
                        "authorizeUrl": authorize,
                        "tokenUrl": token,
                        "clientId": "client",
                        "clientSecret": "client-secret",
                        "redirectUri": redirect,
                        "scope": "tools read"
                    }
                },
            })
            .to_string(),
        ))
        .unwrap();
    let res = app(st.clone()).oneshot(add).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

pub(super) struct McpRemoteReply {
    pub ctype: &'static str,
    pub body: String,
    pub status: &'static str,
}

pub(super) struct McpRemoteServer {
    pub url: String,
}

pub(super) struct McpOAuthServer {
    pub root: String,
}

pub(super) fn mcp_json_response(value: Value) -> McpRemoteReply {
    McpRemoteReply {
        ctype: "application/json",
        body: value.to_string(),
        status: "200 OK",
    }
}

pub(super) fn mcp_remote_response(
    status: &'static str,
    ctype: &'static str,
    body: &str,
) -> McpRemoteReply {
    McpRemoteReply {
        ctype,
        body: body.to_string(),
        status,
    }
}

pub(super) fn mcp_sse_response(value: &str) -> McpRemoteReply {
    McpRemoteReply {
        ctype: "text/event-stream",
        body: value.to_string(),
        status: "200 OK",
    }
}

pub(super) async fn mcp_remote_server(res: Vec<McpRemoteReply>) -> McpRemoteServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        for item in res {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 65536];
            let _ = socket.read(&mut buf).await.unwrap();
            let head = format!(
                "HTTP/1.1 {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                item.status,
                item.ctype,
                item.body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(item.body.as_bytes()).await.unwrap();
        }
    });
    McpRemoteServer {
        url: format!("http://{addr}/mcp"),
    }
}

pub(super) async fn mcp_remote_auth_server(
    res: Vec<McpRemoteReply>,
    seen: StdArc<AtomicUsize>,
) -> McpRemoteServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        for item in res {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 65536];
            let got = socket.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..got]);
            if req.contains("authorization: Bearer static") {
                seen.fetch_add(1, Ordering::SeqCst);
            }
            let head = format!(
                "HTTP/1.1 {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                item.status,
                item.ctype,
                item.body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(item.body.as_bytes()).await.unwrap();
        }
    });
    McpRemoteServer {
        url: format!("http://{addr}/mcp"),
    }
}

pub(super) async fn mcp_remote_token_server(
    res: Vec<McpRemoteReply>,
    auth: &'static str,
    seen: StdArc<AtomicUsize>,
) -> McpRemoteServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        for item in res {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 65536];
            let got = socket.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..got]);
            if req.contains(&format!("authorization: {auth}")) {
                seen.fetch_add(1, Ordering::SeqCst);
            }
            let head = format!(
                "HTTP/1.1 {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                item.status,
                item.ctype,
                item.body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(item.body.as_bytes()).await.unwrap();
        }
    });
    McpRemoteServer {
        url: format!("http://{addr}/mcp"),
    }
}

pub(super) async fn mcp_token_server(body: Value) -> McpRemoteServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 65536];
        let _ = socket.read(&mut buf).await.unwrap();
        let body = body.to_string();
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(body.as_bytes()).await.unwrap();
    });
    McpRemoteServer {
        url: format!("http://{addr}/token"),
    }
}

pub(super) async fn mcp_token_error_server() -> McpRemoteServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 65536];
        let _ = socket.read(&mut buf).await.unwrap();
        let body = "{}";
        let head = format!(
            "HTTP/1.1 401 Unauthorized\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(body.as_bytes()).await.unwrap();
    });
    McpRemoteServer {
        url: format!("http://{addr}/token"),
    }
}

pub(super) async fn mcp_oauth_metadata_server(
    count: Option<StdArc<AtomicUsize>>,
) -> McpOAuthServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let root = format!("http://{addr}");
    let root_task = root.clone();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0; 65536];
            let got = socket.read(&mut buf).await.unwrap();
            let req = String::from_utf8_lossy(&buf[..got]);
            let body = if req.starts_with("GET /.well-known/oauth-authorization-server") {
                json!({
                    "authorization_endpoint": format!("{root_task}/authorize"),
                    "token_endpoint": format!("{root_task}/token"),
                    "registration_endpoint": format!("{root_task}/register"),
                    "scopes_supported": ["tools", "read"]
                })
                .to_string()
            } else if req.starts_with("POST /register") {
                if let Some(count) = count.as_ref() {
                    count.fetch_add(1, Ordering::SeqCst);
                }
                json!({ "client_id": "registered-client", "client_secret": "registered-secret" })
                    .to_string()
            } else {
                "{}".to_string()
            };
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(body.as_bytes()).await.unwrap();
        }
    });
    McpOAuthServer { root }
}

pub(super) async fn mcp_oauth_bad_metadata_server() -> McpOAuthServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 65536];
        let _ = socket.read(&mut buf).await.unwrap();
        let body =
            json!({ "registration_endpoint": "https://secret.example/register" }).to_string();
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(body.as_bytes()).await.unwrap();
    });
    McpOAuthServer {
        root: format!("http://{addr}"),
    }
}

pub(super) async fn mcp_connect_test_server(st: &Arc<AppState>, name: &str) {
    let connect = Request::builder()
        .method(Method::POST)
        .uri(format!("/mcp/{name}/connect"))
        .body(axum::body::Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(connect).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_to_value(res).await, json!(true));
}

pub(super) async fn mcp_status_value(st: &Arc<AppState>) -> Value {
    let req = Request::builder()
        .method(Method::GET)
        .uri("/mcp")
        .body(axum::body::Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    response_to_value(res).await
}

pub(super) fn mcp_script_runtime() -> Option<&'static str> {
    if command_exists("node") {
        return Some("node");
    }
    if command_exists("bun") {
        return Some("bun");
    }
    None
}

pub(super) fn command_exists(name: &str) -> bool {
    Command::new(name)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

pub(super) const MCP_TEST_SERVER_JS: &str = r#"
const input = process.stdin
let buffer = Buffer.alloc(0)
let listed = 0
function send(value) {
  const body = Buffer.from(JSON.stringify(value))
  process.stdout.write(`Content-Length: ${body.length}\r\n\r\n`)
  process.stdout.write(body)
}
const tools = [
  [{ name: "echo", description: "Echo input", inputSchema: { type: "object", properties: {} } }],
  [{ name: "echo2", description: "Echo input changed", inputSchema: { type: "object", properties: {} } }],
]
function receive(value) {
  if (value.method === "initialize") {
    send({ jsonrpc: "2.0", id: value.id, result: { protocolVersion: "2025-03-26", capabilities: { tools: {} }, serverInfo: { name: "test", version: "1" } } })
    return
  }
  if (value.method === "tools/list") {
    send({ jsonrpc: "2.0", id: value.id, result: { tools: tools[Math.min(listed, tools.length - 1)] } })
    listed += 1
    return
  }
  if (value.method === "tools/call") {
    if (value.params.name === "fail") {
      send({ jsonrpc: "2.0", id: value.id, error: { code: -32000, message: "tool failed" } })
      return
    }
    send({ jsonrpc: "2.0", id: value.id, result: { content: [{ type: "text", text: value.params.arguments?.text ?? "" }] } })
    return
  }
  send({ jsonrpc: "2.0", id: value.id, error: { code: -32601, message: "not found" } })
}
input.on("data", (chunk) => {
  buffer = Buffer.concat([buffer, chunk])
  while (true) {
    const header = buffer.indexOf("\r\n\r\n")
    if (header < 0) return
    const text = buffer.slice(0, header).toString("utf8")
    const match = /content-length:\s*(\d+)/i.exec(text)
    if (!match) process.exit(2)
    const length = Number(match[1])
    const start = header + 4
    if (buffer.length < start + length) return
    const body = buffer.slice(start, start + length).toString("utf8")
    buffer = buffer.slice(start + length)
    receive(JSON.parse(body))
  }
})
setTimeout(() => send({ jsonrpc: "2.0", method: "notifications/tools/list_changed", params: {} }), 50)
setInterval(() => {}, 1000)
"#;

pub(super) const MCP_EXIT_SERVER_JS: &str = r#"
const input = process.stdin
let buffer = Buffer.alloc(0)
function send(value) {
  const body = Buffer.from(JSON.stringify(value))
  process.stdout.write(`Content-Length: ${body.length}\r\n\r\n`)
  process.stdout.write(body)
}
function receive(value) {
  if (value.method === "initialize") {
    send({ jsonrpc: "2.0", id: value.id, result: { protocolVersion: "2025-03-26", capabilities: { tools: {} }, serverInfo: { name: "test", version: "1" } } })
    return
  }
  if (value.method === "tools/list") {
    send({ jsonrpc: "2.0", id: value.id, result: { tools: [{ name: "echo", description: "Echo input", inputSchema: { type: "object", properties: {} } }] } })
    return
  }
}
input.on("data", (chunk) => {
  buffer = Buffer.concat([buffer, chunk])
  while (true) {
    const header = buffer.indexOf("\r\n\r\n")
    if (header < 0) return
    const text = buffer.slice(0, header).toString("utf8")
    const match = /content-length:\s*(\d+)/i.exec(text)
    if (!match) process.exit(2)
    const length = Number(match[1])
    const start = header + 4
    if (buffer.length < start + length) return
    const body = buffer.slice(start, start + length).toString("utf8")
    buffer = buffer.slice(start + length)
    receive(JSON.parse(body))
  }
})
setTimeout(() => process.exit(42), 50)
"#;
