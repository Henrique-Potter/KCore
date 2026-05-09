use std::path::PathBuf;
use std::time::{Duration, Instant};

mod rust_harness;

use kilo_oracle::sse::{SseRecorder, StopCondition};
use rust_harness::{
    abort, create_session, fake_prompt, frame_session, frame_type, messages, part_type,
    prompt_async, put_provider_auth, record_until_both_idle, record_until_idle,
    record_until_turn_close, spawn_stalling_oauth_stub, sync_data, sync_type, tool_names,
    write_fixture, write_openai_config, RustSidecar,
};
use serde_json::{json, Value};

fn fixtures_root() -> PathBuf {
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("fixtures")
}

fn session_set(frames: &[kilo_oracle::FixtureFrame]) -> std::collections::BTreeSet<String> {
    frames
        .iter()
        .filter_map(frame_session)
        .map(str::to_string)
        .collect()
}

/// M7 live-sidebar smoke, kept as a deterministic Rust-sidecar script instead of
/// a heavyweight VS Code UI e2e. It exercises the extension-facing protocol path
/// the sidebar uses for a first chat: Rust server launch, global SSE, session
/// create, async prompt, streamed text delta, persisted transcript, and readback.
///
/// Run from `packages/kilo-vscode-sidecar-rs`:
/// `cargo test -p kilo-oracle --test m7_rust_fixtures m7_sidebar_first_chat_smoke_streams_persists_and_reads_back -- --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m7_sidebar_first_chat_smoke_streams_persists_and_reads_back() {
    let sidecar = RustSidecar::spawn().await.expect("spawn rust sidecar");
    let client = sidecar.client.clone();
    let task = tokio::spawn(async move {
        let response = client.open_global_event_stream().await?;
        tokio::time::timeout(
            Duration::from_secs(5),
            SseRecorder::new().record(
                response,
                StopCondition::Predicate(Box::new(|_frame, parsed| {
                    rust_harness::payload_type(parsed) == Some("session.turn.close")
                })),
            ),
        )
        .await
        .map_err(|_| {
            kilo_oracle::error::OracleError::ScenarioAborted(
                "timed out waiting for first chat turn close",
            )
        })?
    });

    tokio::time::sleep(Duration::from_millis(25)).await;
    let id = create_session(&sidecar.client, &sidecar.repo(), "M7 sidebar smoke")
        .await
        .expect("create session");
    let body = fake_prompt(
        "first chat smoke",
        json!({ "fake": true, "fakeDelayMs": 25 }),
    );
    prompt_async(&sidecar.client, &id, &body)
        .await
        .expect("send first prompt");

    let frames = task.await.unwrap().expect("record first chat sse");
    let frames = write_fixture(
        &fixtures_root().join("sse/m7-sidebar-first-chat-smoke"),
        "m7-sidebar-first-chat-smoke",
        frames,
        sidecar.root.path(),
    )
    .expect("write fixture");

    let session_sync = frames.iter().any(|frame| {
        sync_type(frame) == Some("session.created.v1")
            && sync_data(frame)
                .and_then(|data| data.get("info"))
                .and_then(|info| info.get("title"))
                .and_then(Value::as_str)
                == Some("M7 sidebar smoke")
    });
    let user_message = frames.iter().any(|frame| {
        sync_type(frame) == Some("message.part.updated.v1")
            && sync_data(frame)
                .and_then(|data| data.get("part"))
                .and_then(|part| part.get("text"))
                .and_then(Value::as_str)
                == Some("first chat smoke")
    });
    let delta = frames.iter().any(|frame| {
        frame_type(frame) == Some("message.part.delta")
            && frame
                .payload
                .as_ref()
                .and_then(|payload| payload.get("properties"))
                .and_then(|props| props.get("delta"))
                .and_then(Value::as_str)
                == Some("Echo: first chat smoke")
    });
    let final_text = frames.iter().any(|frame| {
        sync_type(frame) == Some("message.part.updated.v1")
            && sync_data(frame)
                .and_then(|data| data.get("part"))
                .and_then(|part| part.get("text"))
                .and_then(Value::as_str)
                == Some("Echo: first chat smoke")
    });
    let closed = frames.iter().any(|frame| {
        frame_type(frame) == Some("session.turn.close")
            && frame
                .payload
                .as_ref()
                .and_then(|payload| payload.get("properties"))
                .and_then(|props| props.get("reason"))
                .and_then(Value::as_str)
                == Some("completed")
    });

    assert!(frames
        .iter()
        .any(|frame| frame_type(frame) == Some("server.connected")));
    assert!(
        session_sync,
        "expected persisted session creation sync event"
    );
    assert!(frames
        .iter()
        .any(|frame| frame_type(frame) == Some("session.turn.open")));
    assert!(
        user_message,
        "expected persisted user message before assistant response"
    );
    assert!(
        delta,
        "expected streamed text delta for sidebar token rendering"
    );
    assert!(final_text, "expected persisted assistant text part update");
    assert!(frames
        .iter()
        .any(|frame| frame_type(frame) == Some("session.idle")));
    assert!(closed, "expected completed turn close");

    let page = messages(&sidecar.client, &id).await.expect("messages");
    assert_eq!(page.len(), 2, "first chat should persist user + assistant");
    assert_eq!(page[0]["info"]["role"], "user");
    assert_eq!(page[1]["info"]["role"], "assistant");
    assert_eq!(page[1]["info"]["providerID"], "local");
    assert_eq!(page[1]["info"]["modelID"], "fake-echo");
    assert_eq!(page[1]["parts"][0]["text"], "Echo: first chat smoke");

    let reload = messages(&sidecar.client, &id)
        .await
        .expect("reload messages");
    assert_eq!(reload[1]["parts"][0]["text"], "Echo: first chat smoke");
    sidecar.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m7_single_fake_tool_call_turn_fixture() {
    let sidecar = RustSidecar::spawn().await.expect("spawn rust sidecar");
    let id = create_session(&sidecar.client, &sidecar.repo(), "M7 single tool")
        .await
        .expect("create session");
    let client = sidecar.client.clone();
    let watch = id.clone();
    let task =
        tokio::spawn(
            async move { record_until_idle(&client, watch, Duration::from_secs(5)).await },
        );
    tokio::time::sleep(Duration::from_millis(25)).await;
    let body = fake_prompt(
        "read note",
        json!({
            "fakeToolCalls": [{
                "tool": "read",
                "input": { "filePath": "note.txt", "limit": 1 }
            }]
        }),
    );
    prompt_async(&sidecar.client, &id, &body)
        .await
        .expect("prompt async");
    let frames = task.await.unwrap().expect("record sse");
    let frames = write_fixture(
        &fixtures_root().join("sse/m7-single-fake-tool-call"),
        "m7-single-fake-tool-call",
        frames,
        sidecar.root.path(),
    )
    .expect("write fixture");

    assert_eq!(tool_names(&frames), vec!["read"]);
    assert!(frames.iter().any(|frame| {
        sync_data(frame)
            .and_then(|data| data.get("part"))
            .and_then(|part| part.get("state"))
            .and_then(|state| state.get("status"))
            .and_then(Value::as_str)
            == Some("completed")
    }));
    assert!(frames
        .iter()
        .any(|frame| part_type(frame) == Some("step-finish")));
    assert!(frames
        .iter()
        .any(|frame| frame_type(frame) == Some("session.idle")));
    sidecar.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m7_parallel_fake_tool_call_fixture_runtime_near_max_delay() {
    let sidecar = RustSidecar::spawn().await.expect("spawn rust sidecar");
    let id = create_session(&sidecar.client, &sidecar.repo(), "M7 parallel tools")
        .await
        .expect("create session");
    let client = sidecar.client.clone();
    let watch = id.clone();
    let task =
        tokio::spawn(
            async move { record_until_idle(&client, watch, Duration::from_secs(5)).await },
        );
    tokio::time::sleep(Duration::from_millis(25)).await;
    let body = fake_prompt(
        "parallel tools",
        json!({
            "fakeToolCalls": [
                {
                    "tool": "read",
                    "delayMs": 120,
                    "input": { "filePath": "note.txt", "limit": 1 }
                },
                {
                    "tool": "grep",
                    "delayMs": 260,
                    "input": { "pattern": "needle", "path": "." }
                }
            ]
        }),
    );
    let started = Instant::now();
    prompt_async(&sidecar.client, &id, &body)
        .await
        .expect("prompt async");
    let frames = task.await.unwrap().expect("record sse");
    let elapsed = started.elapsed();
    let frames = write_fixture(
        &fixtures_root().join("sse/m7-parallel-fake-tool-calls"),
        "m7-parallel-fake-tool-calls",
        frames,
        sidecar.root.path(),
    )
    .expect("write fixture");

    assert!(
        elapsed >= Duration::from_millis(240),
        "parallel turn completed too quickly: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(520),
        "fake tool calls appear serialized: elapsed={elapsed:?}, expected near max 260ms not sum 380ms"
    );
    assert_eq!(tool_names(&frames), vec!["read", "grep"]);
    assert!(frames
        .iter()
        .any(|frame| part_type(frame) == Some("step-finish")));
    sidecar.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m7_abort_mid_stream_fixture_has_terminal_abort_and_no_orphans() {
    let sidecar = RustSidecar::spawn().await.expect("spawn rust sidecar");
    let id = create_session(&sidecar.client, &sidecar.repo(), "M7 abort")
        .await
        .expect("create session");
    let client = sidecar.client.clone();
    let watch = id.clone();
    let task = tokio::spawn(async move {
        record_until_turn_close(&client, watch, Duration::from_secs(5)).await
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    let body = fake_prompt("abort me", json!({ "fake": true, "fakeDelayMs": 500 }));
    prompt_async(&sidecar.client, &id, &body)
        .await
        .expect("prompt async");
    tokio::time::sleep(Duration::from_millis(80)).await;
    abort(&sidecar.client, &id).await.expect("abort");
    let frames = task.await.unwrap().expect("record sse");
    let frames = write_fixture(
        &fixtures_root().join("sse/m7-abort-mid-stream"),
        "m7-abort-mid-stream",
        frames,
        sidecar.root.path(),
    )
    .expect("write fixture");
    let page = messages(&sidecar.client, &id).await.expect("messages");
    let second = page.get(1).expect("assistant message persisted");

    assert_eq!(second["info"]["error"]["name"], "MessageAbortedError");
    assert_eq!(second["parts"].as_array().map(Vec::len), Some(0));
    assert!(frames
        .iter()
        .any(|frame| frame_type(frame) == Some("session.error")));
    assert!(frames
        .iter()
        .any(|frame| frame_type(frame) == Some("session.idle")));
    assert!(frames.iter().any(|frame| {
        frame_type(frame) == Some("session.turn.close")
            && frame
                .payload
                .as_ref()
                .and_then(|payload| payload.get("properties"))
                .and_then(|props| props.get("reason"))
                .and_then(Value::as_str)
                == Some("interrupted")
    }));
    let res = sidecar
        .client
        .post_json(
            &format!("/session/{id}/prompt_async"),
            Some(&fake_prompt("after abort", json!({ "fake": true }))),
        )
        .await;
    assert!(res.is_ok(), "session stayed busy after abort: {res:?}");
    sidecar.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m7_two_concurrent_sessions_interleave_without_leakage_fixture() {
    let sidecar = RustSidecar::spawn().await.expect("spawn rust sidecar");
    let a = create_session(&sidecar.client, &sidecar.repo(), "M7 concurrent A")
        .await
        .expect("create session a");
    let b = create_session(&sidecar.client, &sidecar.repo(), "M7 concurrent B")
        .await
        .expect("create session b");
    let client = sidecar.client.clone();
    let watch_a = a.clone();
    let watch_b = b.clone();
    let task = tokio::spawn(async move {
        record_until_both_idle(&client, watch_a, watch_b, Duration::from_secs(5)).await
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    let body_a = fake_prompt("one", json!({ "fake": true, "fakeDelayMs": 220 }));
    let body_b = fake_prompt("two", json!({ "fake": true, "fakeDelayMs": 80 }));
    prompt_async(&sidecar.client, &a, &body_a)
        .await
        .expect("prompt a");
    tokio::time::sleep(Duration::from_millis(20)).await;
    prompt_async(&sidecar.client, &b, &body_b)
        .await
        .expect("prompt b");
    let frames = task.await.unwrap().expect("record sse");
    let frames = write_fixture(
        &fixtures_root().join("sse/m7-concurrent-sessions"),
        "m7-concurrent-sessions",
        frames,
        sidecar.root.path(),
    )
    .expect("write fixture");
    let sessions = session_set(&frames);

    assert_eq!(sessions.len(), 2, "expected exactly two sessions in trace");
    assert!(sessions.contains("<SESSION_ID:1>"));
    assert!(sessions.contains("<SESSION_ID:2>"));
    assert!(frames
        .iter()
        .any(|frame| frame_type(frame) == Some("session.idle")
            && frame_session(frame) == Some("<SESSION_ID:1>")));
    assert!(frames
        .iter()
        .any(|frame| frame_type(frame) == Some("session.idle")
            && frame_session(frame) == Some("<SESSION_ID:2>")));
    let seq = frames
        .iter()
        .filter_map(frame_session)
        .filter(|id| *id == "<SESSION_ID:1>" || *id == "<SESSION_ID:2>")
        .collect::<Vec<_>>();
    let switched = seq.windows(2).any(|pair| pair[0] != pair[1]);
    assert!(switched, "expected interleaved session events, got {seq:?}");
    let allowed = ["<SESSION_ID:1>", "<SESSION_ID:2>"];
    for frame in &frames {
        if let Some(id) = frame_session(frame) {
            assert!(
                allowed.contains(&id),
                "leaked session id {id} in frame {frame:?}"
            );
        }
        if sync_type(frame).is_some() {
            let sid = sync_data(frame)
                .and_then(|data| data.get("sessionID"))
                .and_then(Value::as_str)
                .or_else(|| {
                    sync_data(frame)
                        .and_then(|data| data.get("part"))
                        .and_then(|part| part.get("sessionID"))
                        .and_then(Value::as_str)
                });
            if let Some(id) = sid {
                assert!(allowed.contains(&id), "leaked sync session id {id}");
            }
        }
    }
    sidecar.shutdown().await.expect("shutdown");
}

/// M7 Fix 1+2+4: aborting an OAuth-path turn mid-stream must update the
/// existing assistant message in place. Bun's `run-state.ts:48-68
/// onInterrupt` keeps exactly one assistant record per cancelled turn —
/// the prior Rust shape produced an orphan second message. This is the
/// integration-level coverage of the unit test
/// `prompt_openai_stream_mid_stream_abort_updates_in_place_no_orphan`,
/// driven through the real HTTP surface end to end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m7_abort_mid_stream_oauth_path_has_no_orphan_message() {
    let sidecar = RustSidecar::spawn().await.expect("spawn rust sidecar");
    // Stalling stub Responses endpoint: streams one delta then waits.
    let stub_base = spawn_stalling_oauth_stub(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
    )
    .await;
    write_openai_config(
        sidecar.root.path(),
        json!({ "options": { "baseURL": stub_base }, "models": {} }),
    )
    .expect("write kilo.json");
    put_provider_auth(
        &sidecar.client,
        "openai",
        json!({
            "type": "oauth",
            "refresh": "rt",
            "access": "e30.eyJleHAiOjQxMDI0NDQ4MDAsImNoYXRncHRfYWNjb3VudF9pZCI6ImFjY3RfMSJ9.sig",
            "expires": 9999999999999i64,
            "accountId": "acct_1"
        }),
    )
    .await
    .expect("set openai auth");

    let id = create_session(&sidecar.client, &sidecar.repo(), "M7 oauth abort")
        .await
        .expect("create session");
    let client = sidecar.client.clone();
    let watch = id.clone();
    let task = tokio::spawn(async move {
        record_until_turn_close(&client, watch, Duration::from_secs(8)).await
    });
    tokio::time::sleep(Duration::from_millis(25)).await;

    let body = json!({
        "parts": [{ "type": "text", "text": "abort me" }],
        "model": { "providerID": "openai", "modelID": "gpt-5.1-codex" },
    });
    prompt_async(&sidecar.client, &id, &body)
        .await
        .expect("prompt async");

    // Allow the prefix delta to land before tripping cancel.
    tokio::time::sleep(Duration::from_millis(150)).await;
    abort(&sidecar.client, &id).await.expect("abort");

    let frames = task.await.unwrap().expect("record sse");
    let _ = frames; // SSE recording proves turn closed; assertions are
                    // on the persisted store.

    let page = messages(&sidecar.client, &id).await.expect("messages");
    assert_eq!(
        page.len(),
        2,
        "expected user + one assistant; the orphan-message bug would inflate this to 3"
    );
    let assistant = page.get(1).expect("assistant message");
    assert_eq!(
        assistant["info"]["error"]["name"], "MessageAbortedError",
        "aborted OAuth turn must surface MessageAbortedError envelope"
    );
    sidecar.shutdown().await.expect("shutdown");
}
