//! Basic prompt-turn tests against the fake provider: persistence,
//! delta ordering, abort/error reasons, busy-session rejection,
//! provider gating, and parallel fake-tool execution.

use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use kilo_protocol::{PromptInput, SessionCreateInput};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::agent::fake::{fake_tool_parts, wait_fake};
use crate::agent::parts::prompt_text;
use crate::agent::turn::{
    ensure_prompt_supported, plan_followup_decision, prompt_guarded, prompt_turn, start_runner,
    PlanFollowup,
};
use crate::routes::health::agents;
use crate::routes::prompt::{abort_session, prompt, prompt_async};
use crate::{FakeCall, PendingPermission, PendingQuestion, PendingSuggestion, TurnError};

use super::common::{
    assert_delta, assert_sync, drain, drain_no_store_mirror, response_to_value, seed, state,
    state_at, unique_root, ENV_RESOLVE_LOCK,
};

#[tokio::test]
async fn agents_route_lists_native_kilo_agent_types() {
    let root = unique_root();
    let state = state_at(&root);

    let body = response_to_value(agents(State(state.clone())).await.into_response()).await;
    let items = body.as_array().expect("agent list");
    let names = items
        .iter()
        .filter_map(|item| item.get("name").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>();

    assert!(names.contains(&"code"));
    assert!(names.contains(&"plan"));
    assert!(names.contains(&"general"));
    assert!(names.contains(&"explore"));
    assert!(names.contains(&"ask"));
    let explore = items
        .iter()
        .find(|item| item.get("name").and_then(serde_json::Value::as_str) == Some("explore"))
        .expect("explore agent");
    assert!(explore
        .get("prompt")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .contains("codebases"));
    assert_eq!(state.agent_info("build").unwrap()["mode"], "primary");
    assert_eq!(state.agent_info("explore").unwrap()["mode"], "subagent");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn agents_route_merges_custom_agent_config() {
    let root = unique_root();
    let dir = root.join("config").join("kilo");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("kilo.json"),
        serde_json::to_string(&json!({
            "default_agent": "research",
            "agent": {
                "research": {
                    "description": "Custom research mode.",
                    "mode": "subagent",
                    "permission": { "read": "allow", "bash": "deny" },
                    "options": { "source": "test" }
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let state = state_at(&root);

    let body = response_to_value(agents(State(state.clone())).await.into_response()).await;
    let items = body.as_array().expect("agent list");
    let first = items.first().expect("default first");
    let custom = items
        .iter()
        .find(|item| item.get("name").and_then(serde_json::Value::as_str) == Some("research"))
        .expect("custom agent");

    assert_eq!(first["name"], "research");
    assert_eq!(custom["mode"], "subagent");
    assert_eq!(custom["options"]["source"], "test");
    assert_eq!(state.agent_info("research").unwrap()["mode"], "subagent");
    assert_eq!(state.agent_permission_rules("research").len(), 2);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_persists_user_and_assistant_messages() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let mut rx = state.bus.subscribe();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "hello" })],
            agent: Some("code".to_string()),
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.info["role"], "assistant");
    assert_eq!(out.info["providerID"], "local");
    assert_eq!(out.info["modelID"], "fake-echo");
    assert_eq!(out.info["finish"], "stop");
    assert!(out.info["time"]["completed"].is_number());
    assert_eq!(out.parts[0]["text"], "Echo: hello");
    assert_eq!(out.parts[1]["type"], "step-finish");
    assert_eq!(out.parts[1]["reason"], "stop");

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].info["role"], "user");
    assert_eq!(page.items[0].parts[0]["text"], "hello");
    assert_eq!(page.items[1].info["role"], "assistant");
    assert_eq!(page.items[1].info["finish"], "stop");
    assert!(page.items[1].info["time"]["completed"].is_number());
    assert_eq!(page.items[1].parts[0]["text"], "Echo: hello");
    assert_eq!(page.items[1].parts[1]["type"], "step-finish");

    // `drain_no_store_mirror` drops the new bus-shape mirrors (added by
    // `http/sse.rs::publish_events` for Bun parity) so this test keeps its
    // pre-mirror indexing semantics — sync envelopes and non-store bus
    // events stay in the same slots.
    let events = drain_no_store_mirror(&mut rx);
    assert_eq!(events[0].payload.kind, "session.turn.open");
    assert_eq!(events[1].payload.kind, "session.status");
    assert_eq!(events[1].payload.properties["status"]["type"], "busy");
    assert_sync(&events[2], "message.updated.v1", "user", None);
    assert_sync(&events[3], "message.part.updated.v1", "", Some("hello"));
    assert_sync(&events[4], "message.updated.v1", "assistant", None);
    assert_sync(&events[5], "message.part.updated.v1", "", Some(""));
    assert_delta(
        &events[6],
        &out.info["id"],
        &out.parts[0]["id"],
        "Echo: hello",
    );
    assert_sync(
        &events[8],
        "message.part.updated.v1",
        "",
        Some("Echo: hello"),
    );
    assert_eq!(
        events[9].payload.sync_event.as_ref().unwrap()["data"]["part"]["type"],
        "step-finish"
    );
    assert_eq!(events[10].payload.kind, "session.status");
    assert_eq!(events[10].payload.properties["status"]["type"], "idle");
    assert_eq!(events[11].payload.kind, "session.idle");
    assert_eq!(events[12].payload.kind, "session.turn.close");
    assert_eq!(events[12].payload.properties["reason"], "completed");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_success_emits_delta_before_close_with_assistant_ids() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let mut rx = state.bus.subscribe();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "delta" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    let events = drain(&mut rx);
    let pos = events
        .iter()
        .position(|event| event.payload.kind == "message.part.delta")
        .expect("delta event");
    let close = events
        .iter()
        .position(|event| event.payload.kind == "session.turn.close")
        .expect("close event");
    assert!(pos < close);
    assert_delta(
        &events[pos],
        &out.info["id"],
        &out.parts[0]["id"],
        "Echo: delta",
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_success_persists_step_finish_and_completion() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "done" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    let page = state.store.messages(&session.id, None, None).unwrap();
    let msg = &page.items[1];
    assert_eq!(out.info["finish"], "stop");
    assert!(out.info["time"]["completed"].is_number());
    assert_eq!(msg.info["finish"], "stop");
    assert!(msg.info["time"]["completed"].is_number());
    assert_eq!(msg.parts[1]["type"], "step-finish");
    assert_eq!(msg.parts[1]["reason"], "stop");
    assert_eq!(msg.parts[1]["tokens"]["input"], 0);
    assert_eq!(msg.parts[1]["tokens"]["cache"]["read"], 0);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_abort_persists_error_and_publishes_interrupted() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let mut rx = state.bus.subscribe();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "abort" })],
            provider: Some(json!({ "fakeAbort": true })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.info["finish"], "error");
    assert_eq!(out.info["error"]["name"], "MessageAbortedError");
    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[1].info["error"]["name"], "MessageAbortedError");

    let events = drain_no_store_mirror(&mut rx);
    assert_eq!(events[0].payload.kind, "session.turn.open");
    assert_eq!(events[1].payload.kind, "session.status");
    assert_eq!(events[1].payload.properties["status"]["type"], "busy");
    assert_sync(&events[2], "message.updated.v1", "user", None);
    assert_sync(&events[3], "message.part.updated.v1", "", Some("abort"));
    assert_sync(&events[4], "message.updated.v1", "assistant", None);
    assert_eq!(events[5].payload.kind, "session.error");
    assert_eq!(
        events[5].payload.properties["error"]["name"],
        "MessageAbortedError"
    );
    assert_eq!(events[6].payload.kind, "session.status");
    assert_eq!(events[6].payload.properties["status"]["type"], "idle");
    assert_eq!(events[7].payload.kind, "session.idle");
    assert_eq!(events[8].payload.kind, "session.turn.close");
    assert_eq!(events[8].payload.properties["reason"], "interrupted");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_error_persists_error_and_publishes_error_reason() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let mut rx = state.bus.subscribe();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "__KILO_FAKE_PROVIDER_ERROR__" })],
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.info["finish"], "error");
    assert_eq!(out.info["error"]["name"], "APIError");
    let events = drain_no_store_mirror(&mut rx);
    assert_sync(&events[4], "message.updated.v1", "assistant", None);
    assert_eq!(events[5].payload.kind, "session.error");
    assert_eq!(events[5].payload.properties["error"]["name"], "APIError");
    assert_eq!(events[8].payload.kind, "session.turn.close");
    assert_eq!(events[8].payload.properties["reason"], "error");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_read_persists_completed_tool_part() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(
        repo.join("src").join("main.rs"),
        "fn main() {\n println!(\"hi\");\n}\n",
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "read src/main.rs" })],
            provider: Some(json!({
                "fakeToolCalls": [{
                    "tool": "read",
                    "input": { "filePath": "src/main.rs", "limit": 2 }
                }]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.info["finish"], "stop");
    assert_eq!(out.parts[0]["type"], "text");
    assert_eq!(out.parts[0]["text"], "Completed 1 fake tool call(s): read");
    let tool = &out.parts[1];
    assert_eq!(tool["type"], "tool");
    assert_eq!(tool["tool"], "read");
    assert!(tool["callID"].as_str().unwrap().starts_with("call_"));
    assert_eq!(tool["state"]["status"], "completed");
    assert_eq!(tool["state"]["input"]["filePath"], "src/main.rs");
    assert_eq!(tool["state"]["title"], "src/main.rs");
    assert!(tool["state"]["output"]
        .as_str()
        .unwrap()
        .contains("<type>file</type>"));
    assert!(tool["state"]["output"]
        .as_str()
        .unwrap()
        .contains("1: fn main()"));
    assert_eq!(tool["state"]["metadata"]["truncated"], true);
    assert!(tool["state"]["metadata"]["preview"]
        .as_str()
        .unwrap()
        .contains("fn main()"));
    assert_eq!(out.parts[2]["type"], "step-finish");

    let page = state.store.messages(&session.id, None, None).unwrap();
    let msg = &page.items[1];
    assert_eq!(msg.parts[1]["tool"], "read");
    assert_eq!(msg.parts[1]["state"]["status"], "completed");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_rejects_api_key_openai_before_mutation() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    state
        .store
        .set_provider_auth("openai", json!({ "type": "api", "key": "sk-test" }))
        .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let mut rx = state.bus.subscribe();

    let res = prompt(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "read note" })],
            model: Some(json!({
                "providerID": "openai",
                "modelID": "gpt-test",
                "capabilities": { "toolcall": true }
            })),
            ..Default::default()
        }),
    )
    .await
    .into_response();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = response_to_value(res).await;
    assert_eq!(body["name"], "UnsupportedProviderError");
    assert_eq!(body["data"]["providerID"], "openai");
    assert_eq!(body["data"]["modelID"], "gpt-test");
    let page = state.store.messages(&session.id, None, None).unwrap();
    assert!(page.items.is_empty());
    assert!(state.runners.lock().unwrap().is_empty());
    assert!(rx.try_recv().is_err());

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_async_rejects_deferred_provider_before_runner() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let res = prompt_async(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "hi" })],
            model: Some(json!({ "providerID": "anthropic", "modelID": "claude-test" })),
            ..Default::default()
        }),
    )
    .await
    .into_response();

    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body = response_to_value(res).await;
    assert_eq!(body["name"], "UnsupportedProviderError");
    assert_eq!(body["data"]["providerID"], "anthropic");
    assert_eq!(body["data"]["modelID"], "claude-test");
    assert!(state.runners.lock().unwrap().is_empty());
    assert!(state
        .store
        .messages(&session.id, None, None)
        .unwrap()
        .items
        .is_empty());

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn provider_route_gate_accepts_fake_and_env_oauth_only() {
    let _g = ENV_RESOLVE_LOCK
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    std::env::remove_var("KILO_AUTH_CONTENT");
    let state = state();

    let fake = PromptInput {
        provider: Some(json!({ "fake": true })),
        ..Default::default()
    };
    assert!(ensure_prompt_supported(&state, &fake, "hi").is_ok());

    let bad = PromptInput {
        model: Some(json!({ "providerID": "gemini", "modelID": "gemini-test" })),
        ..Default::default()
    };
    assert_eq!(
        ensure_prompt_supported(&state, &bad, "hi")
            .unwrap_err()
            .provider,
        "gemini"
    );

    std::env::set_var(
        "KILO_AUTH_CONTENT",
        r#"{"openai":{"type":"oauth","access":"env-token"}}"#,
    );
    let oauth = PromptInput {
        model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
        ..Default::default()
    };
    assert!(ensure_prompt_supported(&state, &oauth, "hi").is_ok());
    std::env::remove_var("KILO_AUTH_CONTENT");
}

#[tokio::test]
async fn abort_route_without_active_runner_is_not_stale() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let res = abort_session(State(state.clone()), Path(session.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);
    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "after abort" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.info["finish"], "stop");
    assert_eq!(out.parts[0]["text"], "Echo: after abort");

    let _ = std::fs::remove_dir_all(root);
}

/// Fix H2 (revised): `fake_read` no longer hard-caps file size. The
/// streaming implementation reads line-by-line via `BufReader`, so
/// large files are still readable when the agent uses `offset`/`limit`
/// to page through them — memory is bounded by the captured window,
/// not by the file size. Generates a ~50 MB file with sequentially
/// numbered lines and asserts that windowed reads at the head and
/// deep into the file both succeed and return the expected line text.
#[tokio::test]
async fn fake_read_streams_window_from_large_file() {
    use crate::agent::tools::fs::fake_read_cancel;
    use std::io::{BufWriter, Write};

    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let dir = PathBuf::from(state.store.paths().directory);
    std::fs::create_dir_all(&dir).unwrap();
    let target = dir.join("big.log");
    // ~45 MB on disk: 5,500,000 lines of `<n>\n` averaging ~8 bytes
    // each. The deep-window assertion needs at least `offset + limit
    // = 5_000_010` lines.
    {
        let f = std::fs::File::create(&target).unwrap();
        let mut w = BufWriter::with_capacity(1 << 20, f);
        for i in 1..=5_500_000usize {
            writeln!(w, "{i}").unwrap();
        }
        w.flush().unwrap();
    }

    // Head window. Streaming means we only allocate ~10 line strings
    // plus the line buffer — orders of magnitude less than the legacy
    // whole-file `String::from_utf8_lossy(&bytes).lines().collect()`.
    let started = Instant::now();
    let head = fake_read_cancel(&dir, &json!({ "filePath": "big.log", "limit": 10 }), None)
        .expect("head window must succeed");
    let head_elapsed = started.elapsed();
    let head_output = head.1.as_str();
    assert!(
        head_output.contains("\n1: 1\n"),
        "head window must include line 1, got:\n{head_output}"
    );
    assert!(
        head_output.contains("\n10: 10\n"),
        "head window must include line 10, got tail:\n{}",
        &head_output[head_output.len().saturating_sub(200)..]
    );
    assert_eq!(head.2["truncated"], true);

    // Deep window. The legacy implementation would have allocated the
    // entire ~45 MB file into a `String` before slicing. With
    // streaming, memory stays bounded by the captured window plus
    // one reused line buffer; runtime is `offset + limit` line reads.
    let started_deep = Instant::now();
    let deep = fake_read_cancel(
        &dir,
        &json!({ "filePath": "big.log", "offset": 5_000_000, "limit": 10 }),
        None,
    )
    .expect("deep window must succeed");
    let deep_elapsed = started_deep.elapsed();
    let deep_output = deep.1.as_str();
    assert!(
        deep_output.contains("\n5000000: 5000000\n"),
        "deep window must include line 5,000,000"
    );
    assert!(
        deep_output.contains("\n5000009: 5000009\n"),
        "deep window must include line 5,000,009"
    );
    assert_eq!(deep.2["truncated"], true);

    // Both reads should be bounded-time. The head is essentially
    // free; the deep read is `offset` line decodes. 30 seconds is
    // generous for cold-cache disk on Windows but tight enough to
    // flag a regression that re-introduces whole-file loading.
    assert!(
        head_elapsed < Duration::from_secs(2),
        "head streaming window took {head_elapsed:?}"
    );
    assert!(
        deep_elapsed < Duration::from_secs(30),
        "deep streaming window took {deep_elapsed:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// Fix H2: walk + collect_text_matches now observe a passed cancel
/// atomic between files / between matched lines. A grep over many
/// files with a tripped cancel must terminate promptly with the
/// cancellation error.
#[tokio::test]
async fn fake_grep_observes_cancel_between_walked_files() {
    use crate::agent::tools::fs::fake_grep_cancel;

    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let dir = PathBuf::from(state.store.paths().directory);
    std::fs::create_dir_all(&dir).unwrap();
    // Generate enough files that the walk has work to do. The cancel
    // check fires before each file's read; with cancel pre-set, the
    // walk drains immediately and grep returns aborted.
    for i in 0..200 {
        std::fs::write(
            dir.join(format!("file_{i}.txt")),
            format!("needle on line {i}\n"),
        )
        .unwrap();
    }
    let cancel = AtomicBool::new(true); // pre-tripped
    let started = Instant::now();
    let res = fake_grep_cancel(&dir, &json!({ "pattern": "needle" }), Some(&cancel));
    let err = res.expect_err("pre-tripped cancel must short-circuit grep");
    assert_eq!(err, "Tool call aborted");
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "grep with pre-tripped cancel took {:?}",
        started.elapsed()
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_async_installs_abort_handle_and_abort_session_preempts_spawned_task() {
    // Fix C1: `abort_session` flips `runner.cancel`, but a spawned task
    // suspended on a non-cooperative `.await` won't see it. The fix
    // installs `task.abort_handle()` on the runner so abort can preempt
    // the future. This test exercises the install + abort path.
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    // 60s fake delay — long enough that a missing abort would let the
    // test sit on `wait_fake`. The cancel race in `wait_fake` would
    // also bail, but the assertion below targets the abort handle
    // itself, not the cooperative path.
    let res = prompt_async(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "stuck" })],
            provider: Some(json!({ "fake": true, "fakeDelayMs": 60000 })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // Wait until the runner exists and has its abort handle installed.
    let mut installed = false;
    for _ in 0..50 {
        if let Some(runner) = state.runners.lock().unwrap().get(&session.id) {
            if runner.abort.lock().unwrap().is_some() {
                installed = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(installed, "abort handle must be installed on the runner");

    let res = abort_session(State(state.clone()), Path(session.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);

    // Runner must drop quickly. Tolerance: ~1s.
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(1) {
        if state.runners.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        state.runners.lock().unwrap().is_empty(),
        "abort_session must clear the runner within 1s"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn abort_session_from_task_child_cancels_parent_runner_too() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let parent = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("parent session");
    let child = state
        .store
        .create_session(SessionCreateInput {
            parent_id: Some(parent.id.clone()),
            ..Default::default()
        })
        .expect("child session");
    let run = crate::agent::turn::start_runner(state.clone(), &parent.id).unwrap();
    let sub = crate::agent::turn::start_runner_with_parent(
        state.clone(),
        &child.id,
        Some(parent.id.clone()),
    )
    .unwrap();

    let res = abort_session(State(state.clone()), Path(child.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert!(run.cancel.load(Ordering::SeqCst));
    assert!(sub.cancel.load(Ordering::SeqCst));

    drop(sub);
    drop(run);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_async_returns_no_content_and_persists_transcript() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let res = prompt_async(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "async" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        }),
    )
    .await;

    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    for _ in 0..20 {
        let page = state.store.messages(&session.id, None, None).unwrap();
        if page.items.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[1].parts[0]["text"], "Echo: async");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_async_queues_same_session_followup() {
    // After the mid-loop follow-up break landed, a same-session
    // `prompt_async` arriving while the first turn is still running
    // signals the active runner to break (not just queue and wait).
    // The first turn finalizes as `interrupted` with whatever partial
    // state it had; the queued follow-up then runs normally.
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let first = prompt_async(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "first" })],
            provider: Some(json!({ "fake": true, "fakeDelayMs": 80 })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(first.status(), StatusCode::NO_CONTENT);
    for _ in 0..20 {
        if state.runners.lock().unwrap().contains_key(&session.id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let second = prompt_async(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "second" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(second.status(), StatusCode::NO_CONTENT);

    for _ in 0..80 {
        let page = state.store.messages(&session.id, None, None).unwrap();
        if page.items.len() == 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items.len(), 4);
    assert_eq!(page.items[0].parts[0]["text"], "first");
    // First turn was broken mid-flight by the follow-up — assistant
    // record carries an aborted error envelope rather than the full
    // echo. `finish` is the generic `"error"` because that's what
    // `assistant_error_info` stamps; the discriminator is `error.name`.
    assert_eq!(
        page.items[1].info["error"]["name"].as_str(),
        Some("MessageAbortedError")
    );
    assert_eq!(page.items[2].parts[0]["text"], "second");
    assert_eq!(prompt_text(&page.items[3].parts), "Echo: second");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_async_follow_up_breaks_active_turn_and_continues_with_new_user_message() {
    // Bun parity: `kilocode/session/prompt-queue.ts:hasFollowup` —
    // when a follow-up `prompt_async` arrives mid-turn, the active
    // turn breaks at the next safe boundary (cancel observed inside
    // `wait_fake`) and the queued follow-up turn fires with the new
    // user message appended. The first turn's partial state must
    // persist (user message + interrupted assistant record), and
    // the abort-style `reject_pending_for_sessions` cascade must NOT
    // fire (this is a follow-up break, not a user abort).
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    // Stage a pending permission so we can assert the follow-up does
    // NOT reject it (distinguishing follow-up break from abort_session).
    let (perm_tx, mut perm_rx) = tokio::sync::oneshot::channel();
    state.permissions.lock().unwrap().insert(
        "perm_followup".to_string(),
        PendingPermission {
            info: json!({
                "id": "perm_followup",
                "sessionID": session.id.clone(),
                "permission": "edit",
            }),
            reply: perm_tx,
        },
    );

    let first = prompt_async(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "long" })],
            // 400ms gives the cancel-observing wait_fake loop plenty of
            // chances to see the follow-up signal mid-flight.
            provider: Some(json!({ "fake": true, "fakeDelayMs": 400 })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(first.status(), StatusCode::NO_CONTENT);
    let mut active = false;
    for _ in 0..30 {
        if state.runners.lock().unwrap().contains_key(&session.id) {
            active = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(active, "first runner never started");

    // Snapshot the active runner's flag pre-followup (must be false).
    {
        let runners = state.runners.lock().unwrap();
        let runner = runners.get(&session.id).expect("runner present");
        assert!(!runner.follow_up_break.load(Ordering::SeqCst));
        assert!(!runner.cancel.load(Ordering::SeqCst));
    }

    let second = prompt_async(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "appended" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(second.status(), StatusCode::NO_CONTENT);

    // Wait for both turns to settle: 4 messages total
    // (user "long", aborted assistant, user "appended", echo assistant).
    for _ in 0..200 {
        let page = state.store.messages(&session.id, None, None).unwrap();
        if page.items.len() >= 4 && state.runners.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(
        page.items.len(),
        4,
        "expected 4 messages, got {}",
        page.items.len()
    );
    assert_eq!(page.items[0].info["role"].as_str(), Some("user"));
    assert_eq!(page.items[0].parts[0]["text"], "long");
    // First assistant: interrupted (mid-turn break before echo could land).
    // `finish` is the generic `"error"` per `assistant_error_info`; the
    // distinguishing field is `error.name == "MessageAbortedError"`.
    assert_eq!(page.items[1].info["role"].as_str(), Some("assistant"));
    assert_eq!(
        page.items[1].info["error"]["name"].as_str(),
        Some("MessageAbortedError"),
        "first turn should finalize as aborted on follow-up break"
    );
    // Follow-up turn carries the new user message and a clean echo.
    assert_eq!(page.items[2].info["role"].as_str(), Some("user"));
    assert_eq!(page.items[2].parts[0]["text"], "appended");
    assert_eq!(page.items[3].info["role"].as_str(), Some("assistant"));
    assert_eq!(prompt_text(&page.items[3].parts), "Echo: appended");

    // Bun parity (`KiloSessionPromptQueue.scope()`): the queued
    // follow-up's user message is a SIBLING of the broken turn's user
    // message (sharing the same parent), not a child of the partial
    // assistant. Both U1 and U2 here have no parent (session root), so
    // their `parentID` fields match (absent).
    let u1_parent = page.items[0].info.get("parentID").cloned();
    let u2_parent = page.items[2].info.get("parentID").cloned();
    assert_eq!(
        u1_parent, u2_parent,
        "follow-up user message must be a sibling of the broken user message"
    );
    // The session-scoped anchor must have been consumed by the
    // follow-up; subsequent prompts get natural parentage.
    assert!(
        state.take_broken_turn_anchor(&session.id).is_none(),
        "broken_turn_anchor must be consumed exactly once"
    );

    // Permission survived the break — follow-up did NOT call
    // reject_pending_for_sessions.
    assert!(
        state
            .permissions
            .lock()
            .unwrap()
            .contains_key("perm_followup"),
        "follow-up break must not reject pending permissions"
    );
    assert!(
        perm_rx.try_recv().is_err(),
        "permission reply channel must remain unresolved"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_async_followup_inherits_broken_turn_parent_anchor() {
    // Bun parity: `KiloSessionPromptQueue.scope()` retargets the queued
    // follow-up's `parentID` onto the broken turn's parent. Here we
    // pre-seed a non-empty anchor (simulating what `commit_broken_turn_anchor`
    // would write) and assert the next regular `prompt` turn picks it
    // up as the new user message's `parentID`, then clears the anchor.
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    // Simulate a broken-turn anchor pointing at a fake parent message id.
    state.set_broken_turn_anchor(&session.id, "msg_root_parent");

    let res = prompt(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "after-break" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let page = state.store.messages(&session.id, None, None).unwrap();
    let user = page
        .items
        .iter()
        .find(|m| m.info["role"].as_str() == Some("user"))
        .expect("user message persisted");
    assert_eq!(
        user.info.get("parentID").and_then(|v| v.as_str()),
        Some("msg_root_parent"),
        "follow-up user message must adopt the pending broken_turn_anchor"
    );

    // Anchor must be consumed exactly once.
    assert!(
        state.take_broken_turn_anchor(&session.id).is_none(),
        "broken_turn_anchor must be cleared after the first follow-up reads it"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn regular_prompt_without_break_uses_natural_parentage() {
    // Regression: a normal prompt turn (no prior follow-up break) must
    // not invent a stale anchor. The new user message's `parentID` is
    // absent — the session-root case — exactly as the pre-anchor flow.
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    // Sanity: no anchor preset.
    assert!(state.take_broken_turn_anchor(&session.id).is_none());

    let res = prompt(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "fresh" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let page = state.store.messages(&session.id, None, None).unwrap();
    let user = page
        .items
        .iter()
        .find(|m| m.info["role"].as_str() == Some("user"))
        .expect("user message persisted");
    assert!(
        user.info.get("parentID").is_none(),
        "natural turn must not stamp a parentID on the user message; got {:?}",
        user.info.get("parentID")
    );

    // Anchor remains unset post-turn.
    assert!(
        state.take_broken_turn_anchor(&session.id).is_none(),
        "successful turn must not leak a broken_turn_anchor"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn abort_session_cancels_queued_async_followup() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let first = prompt_async(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "slow" })],
            provider: Some(json!({ "fake": true, "fakeDelayMs": 120 })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(first.status(), StatusCode::NO_CONTENT);
    for _ in 0..20 {
        if state.runners.lock().unwrap().contains_key(&session.id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let second = prompt_async(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "queued" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(second.status(), StatusCode::NO_CONTENT);
    let res = abort_session(State(state.clone()), Path(session.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);

    for _ in 0..40 {
        if state.runners.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let page = state.store.messages(&session.id, None, None).unwrap();
    assert!(page.items.len() <= 2);
    assert!(page
        .items
        .iter()
        .all(|msg| prompt_text(&msg.parts) != "queued"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_async_dismisses_question_suggestion_waits_without_rejecting_permissions() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let first = prompt_async(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "slow" })],
            provider: Some(json!({ "fake": true, "fakeDelayMs": 120 })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(first.status(), StatusCode::NO_CONTENT);
    for _ in 0..20 {
        if state.runners.lock().unwrap().contains_key(&session.id) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let (perm_tx, _perm_rx) = tokio::sync::oneshot::channel();
    state.permissions.lock().unwrap().insert(
        "perm_wait".to_string(),
        PendingPermission {
            info: json!({
                "id": "perm_wait",
                "sessionID": session.id.clone(),
                "permission": "edit",
            }),
            reply: perm_tx,
        },
    );
    let (que_tx, que_rx) = tokio::sync::oneshot::channel();
    state.questions.lock().unwrap().insert(
        "que_wait".to_string(),
        PendingQuestion {
            info: json!({
                "id": "que_wait",
                "sessionID": session.id.clone(),
            }),
            reply: que_tx,
        },
    );
    let (sgt_tx, sgt_rx) = tokio::sync::oneshot::channel();
    state.suggestions.lock().unwrap().insert(
        "sgt_wait".to_string(),
        PendingSuggestion {
            info: json!({
                "id": "sgt_wait",
                "sessionID": session.id.clone(),
            }),
            reply: sgt_tx,
        },
    );

    let second = prompt_async(
        State(state.clone()),
        Path(session.id.clone()),
        Json(PromptInput {
            parts: vec![json!({ "type": "text", "text": "queued" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        }),
    )
    .await;
    assert_eq!(second.status(), StatusCode::NO_CONTENT);
    assert!(matches!(
        que_rx.await.unwrap(),
        crate::QuestionReply::Rejected
    ));
    assert_eq!(sgt_rx.await.unwrap(), crate::SuggestionDecision::Dismiss);
    assert!(!state.questions.lock().unwrap().contains_key("que_wait"));
    assert!(!state.suggestions.lock().unwrap().contains_key("sgt_wait"));
    assert!(state.permissions.lock().unwrap().contains_key("perm_wait"));

    let res = abort_session(State(state.clone()), Path(session.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);
    for _ in 0..40 {
        if state.runners.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_rejects_same_busy_session_without_queueing() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let id = session.id.clone();
    let task = tokio::spawn(prompt_guarded(
        state.clone(),
        id.clone(),
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "slow" })],
            provider: Some(json!({ "fake": true, "fakeDelayMs": 30 })),
            ..Default::default()
        },
    ));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let err = prompt_guarded(
        state.clone(),
        id.clone(),
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "second" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(matches!(err, TurnError::Busy));
    task.await.unwrap().expect("slow prompt");
    let page = state.store.messages(&id, None, None).unwrap();
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].parts[0]["text"], "slow");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn distinct_sessions_accept_while_another_session_is_busy() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let a = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let b = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let one = tokio::spawn(prompt_guarded(
        state.clone(),
        a.id.clone(),
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "one" })],
            provider: Some(json!({ "fake": true, "fakeDelayMs": 120 })),
            ..Default::default()
        },
    ));
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(state.runners.lock().unwrap().contains_key(&a.id));
    let two = prompt_guarded(
        state.clone(),
        b.id.clone(),
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "two" })],
            provider: Some(json!({ "fake": true })),
            ..Default::default()
        },
    )
    .await
    .expect("second session prompt");
    assert_eq!(two.parts[0]["text"], "Echo: two");
    one.await.unwrap().expect("one");
    assert_eq!(
        state.store.messages(&a.id, None, None).unwrap().items.len(),
        2
    );
    assert_eq!(
        state.store.messages(&b.id, None, None).unwrap().items.len(),
        2
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn abort_active_fake_turn_persists_interrupted_error() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let id = session.id.clone();
    let task = tokio::spawn(prompt_guarded(
        state.clone(),
        id.clone(),
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "abort me" })],
            provider: Some(json!({ "fake": true, "fakeDelayMs": 120 })),
            ..Default::default()
        },
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let res = abort_session(State(state.clone()), Path(id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);
    let out = task.await.unwrap().expect("abort prompt");
    assert_eq!(out.info["error"]["name"], "MessageAbortedError");
    assert!(state.runners.lock().unwrap().is_empty());

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn fake_tool_calls_run_in_parallel_and_return_input_order() {
    const DELAY: u64 = 40;

    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("a.txt"), "a\n").unwrap();
    std::fs::write(repo.join("b.txt"), "b\n").unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    wait_fake(DELAY, &cancel).await;
    wait_fake(DELAY, &cancel).await;
    let serial = start.elapsed();
    let start = Instant::now();
    let parts = fake_tool_parts(
        state.clone(),
        "sid_parallel".to_string(),
        "msg_parallel".to_string(),
        "prt_parallel".to_string(),
        vec![
            FakeCall {
                tool: "read".to_string(),
                input: json!({ "filePath": "a.txt" }),
                delay: DELAY,
                invalid: None,
            },
            FakeCall {
                tool: "read".to_string(),
                input: json!({ "filePath": "b.txt" }),
                delay: DELAY,
                invalid: None,
            },
        ],
        1,
        Arc::new(AtomicBool::new(false)),
        None,
    )
    .await;
    let elapsed = start.elapsed();
    let bound = serial.mul_f32(0.9);
    assert!(
        elapsed < bound,
        "fake tool calls should overlap: elapsed={elapsed:?}, serial={serial:?}, bound={bound:?}"
    );
    assert_eq!(parts[0]["state"]["input"]["filePath"], "a.txt");
    assert_eq!(parts[1]["state"]["input"]["filePath"], "b.txt");

    let _ = std::fs::remove_dir_all(root);
}

// ---------------------------------------------------------------------
// Plan-mode follow-up handoff
// ---------------------------------------------------------------------

/// Build a synthetic assistant `MessageAppendResult` carrying a
/// completed `plan_exit` tool part. Used by the plan-followup tests to
/// drive `plan_followup_decision` without needing a fake provider that
/// can dispatch `plan_exit` end-to-end.
fn synth_plan_exit_result(
    mid: &str,
    agent: &str,
    plan_path: Option<&str>,
) -> kilo_protocol::MessageAppendResult {
    let metadata = match plan_path {
        Some(path) => json!({ "plan": path }),
        None => json!({ "plan": null }),
    };
    let part = json!({
        "id": format!("{mid}_part_plan_exit"),
        "type": "tool",
        "messageID": mid,
        "callID": format!("call_{mid}_plan"),
        "tool": "plan_exit",
        "state": {
            "status": "completed",
            "input": {},
            "output": "Plan is ready",
            "metadata": metadata,
            "title": "Planning complete",
            "time": { "start": 1, "end": 1 },
        },
    });
    kilo_protocol::MessageAppendResult {
        info: json!({
            "id": mid,
            "role": "assistant",
            "agent": agent,
            "finish": "stop",
        }),
        parts: vec![part],
        time: 1,
    }
}

#[tokio::test]
async fn plan_exit_raises_continue_question() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("session");
    let _runner = start_runner(state.clone(), &session.id).expect("runner");
    let mut bus = state.bus.subscribe();

    let result = synth_plan_exit_result("msg_plan_exit", "plan", Some(".kilo/plans/draft.md"));
    let cancel = Arc::new(AtomicBool::new(false));
    let state_for_task = state.clone();
    let sid = session.id.clone();
    let task = tokio::spawn(async move {
        plan_followup_decision(&state_for_task, &sid, &result, cancel).await
    });

    // Wait for the question to land in state.questions.
    let qid = "question_plan_followup_msg_plan_exit".to_string();
    for _ in 0..50 {
        if state.questions.lock().unwrap().contains_key(&qid) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let info = state
        .questions
        .lock()
        .unwrap()
        .get(&qid)
        .expect("plan-followup question")
        .info
        .clone();
    assert_eq!(info["sessionID"], session.id);
    assert_eq!(
        info["questions"][0]["question"],
        "Continue with implementation?"
    );
    assert_eq!(info["questions"][0]["options"][0]["label"], "yes");
    assert_eq!(info["questions"][0]["options"][1]["label"], "no");
    assert_eq!(info["metadata"]["kind"], "plan_followup");

    // The bus should have published the question.
    let mut saw_asked = false;
    while let Ok(event) = bus.try_recv() {
        if event.as_global().payload.kind == "question.asked" {
            saw_asked = true;
            break;
        }
    }
    assert!(saw_asked, "expected question.asked SSE event");

    // Reject the question to let the spawned task complete.
    let entry = state.questions.lock().unwrap().remove(&qid).unwrap();
    let _ = entry.reply.send(crate::QuestionReply::Rejected);
    let outcome = task.await.unwrap().unwrap();
    assert!(matches!(outcome, PlanFollowup::Stay));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn plan_followup_yes_dispatches_implementation_prompt() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("session");
    let _runner = start_runner(state.clone(), &session.id).expect("runner");

    let result = synth_plan_exit_result("msg_yes", "plan", Some(".kilo/plans/handoff.md"));
    let cancel = Arc::new(AtomicBool::new(false));
    let state_for_task = state.clone();
    let sid = session.id.clone();
    let task = tokio::spawn(async move {
        plan_followup_decision(&state_for_task, &sid, &result, cancel).await
    });

    let qid = "question_plan_followup_msg_yes".to_string();
    for _ in 0..50 {
        if state.questions.lock().unwrap().contains_key(&qid) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let entry = state
        .questions
        .lock()
        .unwrap()
        .remove(&qid)
        .expect("question");
    let _ = entry
        .reply
        .send(crate::QuestionReply::Answers(json!([["yes"]])));

    let outcome = task.await.unwrap().unwrap();
    let PlanFollowup::Continue { next, agent_switch } = outcome else {
        panic!("expected PlanFollowup::Continue");
    };
    assert_eq!(agent_switch, "code");
    assert_eq!(next.agent.as_deref(), Some("code"));
    assert_eq!(next.parts.len(), 1);
    assert_eq!(next.parts[0]["type"], "text");
    assert_eq!(next.parts[0]["synthetic"], true);
    let text = next.parts[0]["text"].as_str().unwrap();
    assert!(
        text.contains(".kilo/plans/handoff.md"),
        "expected plan path in synth prompt, got {text}"
    );
    assert!(text.to_lowercase().contains("implement"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn plan_followup_no_ends_turn_cleanly() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("session");
    let _runner = start_runner(state.clone(), &session.id).expect("runner");

    let result = synth_plan_exit_result("msg_no", "plan", None);
    let cancel = Arc::new(AtomicBool::new(false));
    let state_for_task = state.clone();
    let sid = session.id.clone();
    let task = tokio::spawn(async move {
        plan_followup_decision(&state_for_task, &sid, &result, cancel).await
    });

    let qid = "question_plan_followup_msg_no".to_string();
    for _ in 0..50 {
        if state.questions.lock().unwrap().contains_key(&qid) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let entry = state
        .questions
        .lock()
        .unwrap()
        .remove(&qid)
        .expect("question");
    let _ = entry
        .reply
        .send(crate::QuestionReply::Answers(json!([["no"]])));

    let outcome = task.await.unwrap().unwrap();
    assert!(
        matches!(outcome, PlanFollowup::Stay),
        "expected Stay outcome on `no` reply"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn plan_followup_skips_when_agent_is_not_plan() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("session");
    let _runner = start_runner(state.clone(), &session.id).expect("runner");

    // Same plan_exit part shape but assistant.agent = "code" — must
    // not raise the follow-up question (Bun parity: only the `plan`
    // agent triggers the handoff prompt).
    let result = synth_plan_exit_result("msg_code", "code", Some(".kilo/plans/x.md"));
    let cancel = Arc::new(AtomicBool::new(false));
    let outcome = plan_followup_decision(&state, &session.id, &result, cancel)
        .await
        .unwrap();
    assert!(matches!(outcome, PlanFollowup::Stay));
    assert!(state.questions.lock().unwrap().is_empty());

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn plan_followup_skips_when_no_plan_exit_part() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("session");
    let _runner = start_runner(state.clone(), &session.id).expect("runner");

    // Plan agent but the assistant message has no plan_exit tool part —
    // user is still planning, no follow-up.
    let result = kilo_protocol::MessageAppendResult {
        info: json!({
            "id": "msg_planning",
            "role": "assistant",
            "agent": "plan",
            "finish": "stop",
        }),
        parts: vec![json!({
            "id": "msg_planning_text",
            "type": "text",
            "text": "Working on the plan.",
        })],
        time: 1,
    };
    let cancel = Arc::new(AtomicBool::new(false));
    let outcome = plan_followup_decision(&state, &session.id, &result, cancel)
        .await
        .unwrap();
    assert!(matches!(outcome, PlanFollowup::Stay));
    assert!(state.questions.lock().unwrap().is_empty());

    let _ = std::fs::remove_dir_all(root);
}
