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
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::agent::fake::{fake_tool_parts, wait_fake};
use crate::agent::turn::{ensure_prompt_supported, prompt_guarded, prompt_turn};
use crate::routes::prompt::{abort_session, prompt, prompt_async};
use crate::{FakeCall, TurnError};

use super::common::{
    assert_delta, assert_sync, drain, drain_no_store_mirror, response_to_value, seed, state,
    state_at, unique_root, ENV_RESOLVE_LOCK,
};

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
    let root = PathBuf::from(state.store.paths().directory);
    let cancel = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    wait_fake(DELAY, &cancel).await;
    wait_fake(DELAY, &cancel).await;
    let serial = start.elapsed();
    let start = Instant::now();
    let parts = fake_tool_parts(
        root.clone(),
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
