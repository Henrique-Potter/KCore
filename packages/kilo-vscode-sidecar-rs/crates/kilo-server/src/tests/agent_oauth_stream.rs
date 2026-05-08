//! OpenAI OAuth Responses-API tests: streaming deltas, slash-command
//! expansion, structured tool items, follow-up replays, permission
//! gating in the OAuth path, malformed-args diagnostic, fixture
//! replay, and abort coverage. NOTE: this file is allowed to exceed
//! the 1.2k LoC limit because the OAuth-stream integration test cases
//! cluster on shared mock fixtures; future M9 work should consider a
//! dedicated `agent_oauth_permission` module.

use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use kilo_protocol::{PromptInput, SessionCreateInput};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::agent::openai_stream::is_openai_oauth;
use crate::agent::turn::prompt_turn;
use crate::routes::permissions::{permission_list, reply_permission};
use crate::TurnError;

use super::common::{
    drain, fixture_stream, oauth_auth, ok_stream, seed, state_at, stream_provider_sequence,
    stream_provider_server, stream_provider_stalling_server, unique_root, write_command,
    write_openai_config, CODEX_FINAL_TEXT_STREAM, CODEX_TOOL_CALL_STREAM,
};

#[tokio::test]
async fn prompt_turn_openai_oauth_streams_deltas_and_persists_usage() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let server = stream_provider_server(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hel\"}\n\n\
data: {\"type\":\"response.output_text.delta\",\"delta\":\"lo\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":1,\"total_tokens\":3}}}\n\n\
data: [DONE]\n\n",
        )
        .await;
    state
        .store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "refresh": "refresh-token",
                "access": "e30.eyJleHAiOjQxMDI0NDQ4MDAsImNoYXRncHRfYWNjb3VudF9pZCI6ImFjY3RfMSJ9.sig",
                "expires": 9999999999999i64,
                "accountId": "acct_1"
            }),
        )
        .unwrap();
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": {
                "openai": {
                    "options": { "baseURL": server.url },
                    "models": {}
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let mut rx = state.bus.subscribe();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "hello" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            system: Some(json!("system instructions")),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.info["providerID"], "openai");
    assert_eq!(out.parts[0]["text"], "Hello");
    assert_eq!(out.info["tokens"]["input"], 2);
    // Per-iteration step parts (Bun parity, processor.ts:402-473): each
    // iteration emits a step-start at the top and a step-finish at the
    // bottom. Single-iteration turns therefore produce: [text, step-start, step-finish].
    assert_eq!(out.parts[1]["type"], "step-start");
    assert_eq!(out.parts[2]["type"], "step-finish");
    assert_eq!(out.parts[2]["tokens"]["total"], 3);
    let events = drain(&mut rx);
    let first = events
        .iter()
        .position(|event| event.payload.kind == "message.part.delta")
        .unwrap();
    assert_eq!(events[first].payload.properties["delta"], "Hel");
    // Soul-prepending invariant: per llm.ts:155-159, the OAuth Responses
    // call sets `options.instructions = SystemPrompt.soul() + "\n" + system_array_joined`.
    // We assert both pieces appear (escaped for the JSON wire shape).
    let body = server.body.lock().unwrap().clone();
    assert!(
        body.contains("\"instructions\":\"You are Kilo"),
        "missing soul prefix in instructions: {body}"
    );
    assert!(
        body.contains("system instructions"),
        "missing user system text in instructions: {body}"
    );
    assert_eq!(
        body.matches("\"text\":\"hello\"").count(),
        1,
        "current turn user message should be sent once and not duplicated by the placeholder assistant: {body}"
    );
    assert!(
        !body.contains("\"text\":\"\",\"type\":\"output_text\""),
        "empty in-progress assistant placeholder leaked to provider input: {body}"
    );
    assert!(server.body.lock().unwrap().contains("\"stream\":true"));
    assert!(
        body.contains("chatgpt-account-id: acct_1") || body.contains("ChatGPT-Account-Id: acct_1")
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_expands_non_subtask_slash_command() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    write_command(
        &root,
        "draft",
        "---\ndescription: Draft\nsubtask: false\n---\nTitle: $1\nBody: $ARGUMENTS\nTail: $2",
    );
    let server = stream_provider_server(ok_stream()).await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "/draft \"hello world\" tail words" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    let body = server.body.lock().unwrap().clone();
    assert!(
        body.contains("Title: hello world"),
        "missing positional expansion: {body}"
    );
    assert!(
        body.contains("Body: \\\"hello world\\\" tail words"),
        "missing arguments expansion: {body}"
    );
    assert!(
        body.contains("Tail: tail words"),
        "missing trailing positional expansion: {body}"
    );
    assert!(
        !body.contains("/draft"),
        "slash invocation leaked to provider: {body}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_regular_prompt_unchanged_by_command_registry() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    write_command(
        &root,
        "draft",
        "---\ndescription: Draft\n---\nchanged $ARGUMENTS",
    );
    let server = stream_provider_server(ok_stream()).await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "draft plain text" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    let body = server.body.lock().unwrap().clone();
    assert!(
        body.contains("draft plain text"),
        "normal prompt missing: {body}"
    );
    assert!(
        !body.contains("changed"),
        "normal prompt was command-expanded: {body}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_handles_subtask_slash_command_inline() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    write_command(
        &root,
        "child",
        "---\ndescription: Child\nsubtask: true\n---\nrun $ARGUMENTS",
    );
    let server =
        stream_provider_sequence(vec![ok_stream().to_string(), ok_stream().to_string()]).await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "/child now" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("subtask command handled inline");

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items[0].parts[0]["type"], "subtask");
    assert_eq!(page.items[0].parts[0]["agent"], "general");
    assert_eq!(page.items[0].parts[0]["description"], "Child");
    assert_eq!(page.items[0].parts[0]["command"], "child");
    assert_eq!(page.items[0].parts[0]["prompt"], "run now");
    assert_eq!(
        page.items[0].parts[0]["metadata"]["source"],
        "rust-openai-oauth-slash-command"
    );
    assert_eq!(page.items[0].parts[0]["metadata"]["mode"], "inline");

    let task = page
        .items
        .iter()
        .flat_map(|msg| msg.parts.iter())
        .find(|part| {
            part.get("type").and_then(|value| value.as_str()) == Some("tool")
                && part.get("tool").and_then(|value| value.as_str()) == Some("task")
        })
        .expect("task wrapper part");
    assert_eq!(task["state"]["status"], "completed");
    assert!(task["state"]["output"]
        .as_str()
        .unwrap()
        .contains("<task_result>"));
    let child = task["state"]["metadata"]["sessionId"].as_str().unwrap();
    assert_eq!(
        state.store.session(child).unwrap().parent_id,
        Some(session.id.clone())
    );
    assert!(page.items.iter().any(|msg| {
        msg.parts.iter().any(|part| {
            part.get("text").and_then(|value| value.as_str())
                == Some("Summarize the task tool output above and continue with your task.")
        })
    }));

    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 2);
    assert!(
        bodies[0].contains("run now"),
        "subtask prompt missing: {}",
        bodies[0]
    );
    assert!(
        bodies[1].contains("Summarize the task tool output above and continue with your task."),
        "parent continuation missing: {}",
        bodies[1]
    );
    assert!(
        !bodies.join("\n").contains("/child"),
        "slash invocation leaked to provider: {:?}",
        bodies
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_continues_with_structured_tool_items() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo").join("repo")).unwrap();
    std::fs::write(
        root.join("repo").join("repo").join("note.txt"),
        "structured result",
    )
    .unwrap();
    let server = stream_provider_sequence(vec![
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_read\",\"name\":\"read\",\"delta\":\"{\\\"filePath\\\":\"}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"\\\"repo/note.txt\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_read\",\"name\":\"read\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n",
        ]
        .into_iter()
        .map(str::to_string)
        .collect())
        .await;
    state
        .store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "refresh": "refresh-token",
                "access": "e30.eyJleHAiOjQxMDI0NDQ4MDAsImNoYXRncHRfYWNjb3VudF9pZCI6ImFjY3RfMSJ9.sig",
                "expires": 9999999999999i64,
                "accountId": "acct_1"
            }),
        )
        .unwrap();
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": {
                "openai": {
                    "options": { "baseURL": server.url },
                    "models": {}
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "read note" })],
            model: Some(json!({
                "providerID": "openai",
                "modelID": "gpt-5.1-codex",
                "capabilities": { "toolcall": true }
            })),
            tools: Some(json!(true)),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.parts[0]["text"], "done");
    // Layout: [text, step-start_0, tool, step-finish_0, step-start_1, step-finish_1]
    assert_eq!(out.parts[1]["type"], "step-start");
    assert_eq!(out.parts[2]["type"], "tool");
    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 2);
    let body = &bodies[1];
    assert!(body.contains("\"type\":\"function_call\""), "{body}");
    assert!(body.contains("\"call_id\":\"call_read\""), "{body}");
    assert!(body.contains("\"name\":\"read\""), "{body}");
    assert!(
        body.contains("\"arguments\":\"{\\\"filePath\\\":\\\"repo/note.txt\\\"}\""),
        "{body}"
    );
    assert!(body.contains("\"type\":\"function_call_output\""), "{body}");
    assert!(body.contains("structured result"), "{body}");
    assert!(!body.contains("[Tool calls]"), "{body}");
    assert!(!body.contains("[Tool results]"), "{body}");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_executes_parallel_task_tool_calls() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let server = stream_provider_sequence(vec![
        parallel_task_stream(),
        ok_stream().to_string(),
        ok_stream().to_string(),
        ok_stream().to_string(),
    ])
    .await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "task": "allow" })),
            ..Default::default()
        })
        .unwrap();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "run two subtasks" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn with task tools");

    let tasks = out
        .parts
        .iter()
        .filter(|part| {
            part.get("type").and_then(|value| value.as_str()) == Some("tool")
                && part.get("tool").and_then(|value| value.as_str()) == Some("task")
        })
        .collect::<Vec<_>>();
    assert_eq!(tasks.len(), 2);
    assert!(tasks
        .iter()
        .all(|part| part["state"]["status"] == "completed"));
    let children = state.store.children(&session.id).unwrap();
    assert_eq!(children.len(), 2);
    assert!(children
        .iter()
        .all(|child| child.permission == Some(json!({ "task": { "*": "deny" } }))));
    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 4);
    let child_bodies = &bodies[1..3];
    assert!(
        child_bodies.iter().any(|body| body.contains("first child")),
        "{child_bodies:?}"
    );
    assert!(
        child_bodies
            .iter()
            .any(|body| body.contains("second child")),
        "{child_bodies:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_exposes_task_child_before_child_permission() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let server = stream_provider_sequence(vec![
        single_task_stream(),
        edit_tool_stream(),
        ok_stream().to_string(),
    ])
    .await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "task": "allow" })),
            ..Default::default()
        })
        .unwrap();
    let mut rx = state.bus.subscribe();
    let turn = tokio::spawn({
        let state = state.clone();
        let sid = session.id.clone();
        async move {
            prompt_turn(
                &state,
                &sid,
                PromptInput {
                    parts: vec![json!({ "type": "text", "text": "run one subtask" })],
                    model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
                    ..Default::default()
                },
                Arc::new(AtomicBool::new(false)),
            )
            .await
        }
    });

    let mut saw_child = false;
    let mut perm = None;
    for _ in 0..80 {
        let event = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for task child metadata before child permission")
            .expect("bus event")
            .as_global();
        if event.payload.kind == "message.part.updated" {
            let part = &event.payload.properties["part"];
            if part["tool"] == "task"
                && part["state"]["status"] == "running"
                && part["state"]["metadata"]["sessionId"].is_string()
            {
                saw_child = true;
            }
        }
        if event.payload.kind == "permission.asked" {
            assert!(
                saw_child,
                "child permission was published before parent task part exposed sessionId"
            );
            perm = event.payload.properties["id"].as_str().map(str::to_string);
            break;
        }
    }
    let id = perm.expect("child edit permission request");
    let res = reply_permission(
        State(state.clone()),
        Path(id),
        Json(json!({ "reply": "reject" })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    tokio::time::timeout(Duration::from_secs(2), turn)
        .await
        .expect("turn should finish after rejecting child permission")
        .expect("join")
        .expect("turn result");

    let _ = std::fs::remove_dir_all(root);
}

fn parallel_task_stream() -> String {
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_task\",\"status\":\"in_progress\",\"model\":\"gpt-5.1-codex\"}}\n\n\
data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_task_one\",\"type\":\"function_call\",\"call_id\":\"call_task_one\",\"name\":\"task\",\"arguments\":\"\"}}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_task_one\",\"name\":\"task\",\"delta\":\"{\\\"description\\\":\\\"First child\\\",\\\"prompt\\\":\\\"first child\\\",\\\"subagent_type\\\":\\\"general\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_task_one\",\"name\":\"task\"}\n\n\
data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"id\":\"fc_task_two\",\"type\":\"function_call\",\"call_id\":\"call_task_two\",\"name\":\"task\",\"arguments\":\"\"}}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"call_id\":\"call_task_two\",\"name\":\"task\",\"delta\":\"{\\\"description\\\":\\\"Second child\\\",\\\"prompt\\\":\\\"second child\\\",\\\"subagent_type\\\":\\\"general\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":1,\"call_id\":\"call_task_two\",\"name\":\"task\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_task\",\"status\":\"completed\",\"usage\":{\"input_tokens\":20,\"output_tokens\":5,\"total_tokens\":25}}}\n\n\
data: [DONE]\n\n"
        .to_string()
}

fn single_task_stream() -> String {
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_task\",\"status\":\"in_progress\",\"model\":\"gpt-5.1-codex\"}}\n\n\
data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_task\",\"type\":\"function_call\",\"call_id\":\"call_task\",\"name\":\"task\",\"arguments\":\"\"}}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_task\",\"name\":\"task\",\"delta\":\"{\\\"description\\\":\\\"Child edit\\\",\\\"prompt\\\":\\\"edit a file\\\",\\\"subagent_type\\\":\\\"general\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_task\",\"name\":\"task\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_task\",\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n"
        .to_string()
}

fn edit_tool_stream() -> String {
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_edit\",\"status\":\"in_progress\",\"model\":\"gpt-5.1-codex\"}}\n\n\
data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_edit\",\"type\":\"function_call\",\"call_id\":\"call_edit\",\"name\":\"edit\",\"arguments\":\"\"}}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_edit\",\"name\":\"edit\",\"delta\":\"{\\\"filePath\\\":\\\"repo/file.txt\\\",\\\"oldString\\\":\\\"old\\\",\\\"newString\\\":\\\"new\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_edit\",\"name\":\"edit\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_edit\",\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n"
        .to_string()
}

fn slow_plugin_stream() -> &'static str {
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_plugin\",\"status\":\"in_progress\",\"model\":\"gpt-5.1-codex\"}}\n\n\
data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_plugin\",\"type\":\"function_call\",\"call_id\":\"call_slow\",\"name\":\"slow_diff\",\"arguments\":\"\"}}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_slow\",\"name\":\"slow_diff\",\"delta\":\"{}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_slow\",\"name\":\"slow_diff\"}\n\n"
}

#[tokio::test]
async fn prompt_turn_openai_oauth_followup_replays_prior_transcript() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo").join("repo")).unwrap();
    std::fs::write(
        root.join("repo").join("repo").join("note.txt"),
        "first note",
    )
    .unwrap();
    let server = stream_provider_sequence(vec![
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_read\",\"name\":\"read\",\"delta\":\"{\\\"filePath\\\":\"}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"\\\"repo/note.txt\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_read\",\"name\":\"read\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n".to_string(),
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"first answer\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n".to_string(),
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"second answer\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n".to_string(),
        ])
        .await;
    state
        .store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "refresh": "refresh-token",
                "access": "e30.eyJleHAiOjQxMDI0NDQ4MDAsImNoYXRncHRfYWNjb3VudF9pZCI6ImFjY3RfMSJ9.sig",
                "expires": 9999999999999i64
            }),
        )
        .unwrap();
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": { "openai": { "options": { "baseURL": server.url }, "models": {} } }
        }))
        .unwrap(),
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let model = json!({
        "providerID": "openai",
        "modelID": "gpt-5.1-codex",
        "capabilities": { "toolcall": true }
    });

    prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "read note" })],
            model: Some(model.clone()),
            tools: Some(json!(true)),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("first prompt");
    prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "what did it say?" })],
            model: Some(model),
            tools: Some(json!(true)),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("second prompt");

    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 3);
    let body = &bodies[2];
    assert!(body.contains("read note"), "{body}");
    assert!(body.contains("first answer"), "{body}");
    assert!(body.contains("what did it say?"), "{body}");
    assert!(body.contains("\"type\":\"function_call\""), "{body}");
    assert!(body.contains("\"call_id\":\"call_read\""), "{body}");
    assert!(body.contains("\"type\":\"function_call_output\""), "{body}");
    assert!(body.contains("first note"), "{body}");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_waits_for_permission_before_mutating_tool() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    let server = stream_provider_sequence(vec![
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_write\",\"name\":\"write\",\"delta\":\"{\\\"filePath\\\":\\\"approved.txt\\\",\\\"content\\\":\\\"yes\\\\n\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_write\",\"name\":\"write\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"wrote it\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n",
        ]
        .into_iter()
        .map(str::to_string)
        .collect())
        .await;
    state
        .store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "refresh": "refresh-token",
                "access": "e30.eyJleHAiOjQxMDI0NDQ4MDAsImNoYXRncHRfYWNjb3VudF9pZCI6ImFjY3RfMSJ9.sig",
                "expires": 9999999999999i64,
                "accountId": "acct_1"
            }),
        )
        .unwrap();
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": { "openai": { "options": { "baseURL": server.url }, "models": {} } }
        }))
        .unwrap(),
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let task = tokio::spawn({
        let state = state.clone();
        let sid = session.id.clone();
        async move {
            prompt_turn(
                &state,
                &sid,
                PromptInput {
                    parts: vec![json!({ "type": "text", "text": "write approved" })],
                    model: Some(json!({
                        "providerID": "openai",
                        "modelID": "gpt-5.1-codex",
                        "capabilities": { "toolcall": true }
                    })),
                    tools: Some(json!(true)),
                    ..Default::default()
                },
                Arc::new(AtomicBool::new(false)),
            )
            .await
        }
    });

    let mut items = Vec::new();
    for _ in 0..50 {
        items = permission_list(&state);
        if !items.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["permission"], "edit");
    assert_eq!(items[0]["patterns"], json!(["approved.txt"]));
    assert!(!task.is_finished());
    let id = items[0]["id"].as_str().unwrap().to_string();
    let res = reply_permission(
        State(state.clone()),
        Path(id),
        Json(json!({ "reply": "once" })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let out = task.await.unwrap().expect("prompt turn");
    assert_eq!(out.parts[0]["text"], "wrote it");
    // Layout: [text, step-start_0, tool(write), step-finish_0, step-start_1, step-finish_1]
    assert_eq!(out.parts[1]["type"], "step-start");
    assert_eq!(out.parts[2]["state"]["status"], "completed");
    assert_eq!(
        std::fs::read_to_string(root.join("repo").join("approved.txt")).unwrap(),
        "yes\n"
    );
    assert_eq!(server.bodies.lock().unwrap().len(), 2);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_permission_reject_stops_before_continuation() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    let server = stream_provider_sequence(vec![
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_write\",\"name\":\"write\",\"delta\":\"{\\\"filePath\\\":\\\"denied.txt\\\",\\\"content\\\":\\\"no\\\\n\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_write\",\"name\":\"write\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"should not happen\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n",
        ]
        .into_iter()
        .map(str::to_string)
        .collect())
        .await;
    state
        .store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "refresh": "refresh-token",
                "access": "e30.eyJleHAiOjQxMDI0NDQ4MDAsImNoYXRncHRfYWNjb3VudF9pZCI6ImFjY3RfMSJ9.sig",
                "expires": 9999999999999i64,
                "accountId": "acct_1"
            }),
        )
        .unwrap();
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": { "openai": { "options": { "baseURL": server.url }, "models": {} } }
        }))
        .unwrap(),
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let task = tokio::spawn({
        let state = state.clone();
        let sid = session.id.clone();
        async move {
            prompt_turn(
                &state,
                &sid,
                PromptInput {
                    parts: vec![json!({ "type": "text", "text": "write denied" })],
                    model: Some(json!({
                        "providerID": "openai",
                        "modelID": "gpt-5.1-codex",
                        "capabilities": { "toolcall": true }
                    })),
                    tools: Some(json!(true)),
                    ..Default::default()
                },
                Arc::new(AtomicBool::new(false)),
            )
            .await
        }
    });

    let mut id = None;
    for _ in 0..50 {
        id = permission_list(&state)
            .first()
            .and_then(|item| item["id"].as_str())
            .map(str::to_string);
        if id.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let id = id.expect("pending permission");
    let res = reply_permission(
        State(state.clone()),
        Path(id),
        Json(json!({ "reply": "reject" })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let out = task.await.unwrap().expect("prompt turn");
    // Layout: [text, step-start_0, tool(write), step-finish_0]. parts[2] is the rejected tool.
    assert_eq!(out.parts[1]["type"], "step-start");
    assert_eq!(out.parts[2]["state"]["status"], "error");
    assert_eq!(
        out.parts[2]["state"]["metadata"]["error"]["name"],
        "PermissionRejectedError"
    );
    assert!(out.parts[2]["state"]["error"]
        .as_str()
        .unwrap()
        .contains("Permission rejected"));
    assert!(!root.join("repo").join("denied.txt").exists());
    assert_eq!(server.bodies.lock().unwrap().len(), 1);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_repeated_malformed_tool_args_stops_with_diagnostic() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    let bad = "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_bad\",\"name\":\"read\",\"delta\":\"{\\\"filePath\\\":} trailing\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_bad\",\"name\":\"read\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n";
    let server = stream_provider_sequence(vec![
        bad.to_string(),
        bad.to_string(),
        ok_stream().to_string(),
    ])
    .await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": { "openai": { "options": { "baseURL": server.url }, "models": {} } }
        }))
        .unwrap(),
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    let out = tokio::time::timeout(
        Duration::from_secs(3),
        prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "read malformed" })],
                model: Some(json!({
                    "providerID": "openai",
                    "modelID": "gpt-5.1-codex",
                    "capabilities": { "toolcall": true }
                })),
                tools: Some(json!(true)),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        ),
    )
    .await
    .expect("malformed loop should stop promptly")
    .expect("prompt turn");

    assert_eq!(out.info["finish"], "error");
    assert_eq!(out.info["error"]["name"], "MalformedToolArgumentsError");
    assert!(out.info["error"]["data"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("Invalid tool arguments JSON"));
    // Layout (2 malformed iterations):
    //   [text, step-start_0, tool(invalid_0), step-finish_0,
    //          step-start_1, tool(invalid_1), step-finish_1]
    assert_eq!(out.parts[1]["type"], "step-start");
    assert_eq!(out.parts[2]["type"], "tool");
    assert_eq!(out.parts[2]["tool"], "invalid");
    assert_eq!(out.parts[2]["callID"], "call_bad");
    assert_eq!(
        out.parts[2]["state"]["input"]["arguments"],
        "{\"filePath\":} trailing"
    );
    assert!(out.parts[2]["state"]["error"]
        .as_str()
        .unwrap_or_default()
        .contains("Invalid tool arguments JSON"));
    assert_eq!(out.parts[3]["type"], "step-finish");
    assert_eq!(out.parts[4]["type"], "step-start");
    assert_eq!(out.parts[5]["type"], "tool");
    assert_eq!(out.parts[5]["tool"], "invalid");
    assert_eq!(out.parts[5]["callID"], "call_bad");
    assert_eq!(
        out.parts[5]["state"]["input"]["arguments"],
        "{\"filePath\":} trailing"
    );
    assert!(out.parts[6]["type"]
        .as_str()
        .unwrap_or_default()
        .contains("finish"));

    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 2);
    assert!(
        bodies[1].contains("The arguments provided to the tool are invalid:"),
        "{}",
        bodies[1]
    );
    assert!(
        bodies[1].contains("Invalid tool arguments JSON"),
        "{}",
        bodies[1]
    );
    assert!(bodies[1].contains("call_bad"), "{}", bodies[1]);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_malformed_repeat_tracking_is_signature_scoped() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    let bad_a = "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_a\",\"name\":\"read\",\"delta\":\"{\\\"filePath\\\":} trailing\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_a\",\"name\":\"read\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n";
    let bad_b = "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_b\",\"name\":\"read\",\"delta\":\"{\\\"filePath\\\": true,}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_b\",\"name\":\"read\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n";
    let server = stream_provider_sequence(vec![
        bad_a.to_string(),
        bad_b.to_string(),
        bad_a.to_string(),
    ])
    .await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": { "openai": { "options": { "baseURL": server.url }, "models": {} } }
        }))
        .unwrap(),
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "read malformed" })],
            model: Some(json!({
                "providerID": "openai",
                "modelID": "gpt-5.1-codex",
                "capabilities": { "toolcall": true }
            })),
            tools: Some(json!(true)),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.info["finish"], "error");
    assert_eq!(out.info["error"]["name"], "MalformedToolArgumentsError");
    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 3);
    assert!(bodies[1].contains("call_a"), "{}", bodies[1]);
    assert!(
        bodies[1].contains("The arguments provided to the tool are invalid:"),
        "{}",
        bodies[1]
    );
    assert!(bodies[2].contains("call_b"), "{}", bodies[2]);
    assert!(out.parts.iter().any(|part| part["callID"] == "call_a"));
    assert!(out.parts.iter().any(|part| part["callID"] == "call_b"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_replays_recorded_codex_fixture_and_persists_transcript() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo").join("repo")).unwrap();
    std::fs::write(
        root.join("repo").join("repo").join("note.txt"),
        "fixture ok",
    )
    .unwrap();
    let server = stream_provider_sequence(vec![
        fixture_stream(CODEX_TOOL_CALL_STREAM),
        fixture_stream(CODEX_FINAL_TEXT_STREAM),
    ])
    .await;
    state
        .store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "refresh": "refresh-token",
                "access": "e30.eyJleHAiOjQxMDI0NDQ4MDAsImNoYXRncHRfYWNjb3VudF9pZCI6ImFjY3RfMSJ9.sig",
                "expires": 9999999999999i64,
                "accountId": "acct_1"
            }),
        )
        .unwrap();
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": {
                "openai": {
                    "options": { "baseURL": server.url },
                    "models": {}
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "read the smoke fixture" })],
            model: Some(json!({
                "providerID": "openai",
                "modelID": "gpt-5.1-codex",
                "capabilities": { "toolcall": true }
            })),
            tools: Some(json!(true)),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.info["providerID"], "openai");
    assert_eq!(out.info["modelID"], "gpt-5.1-codex");
    assert_eq!(out.info["finish"], "stop");
    assert_eq!(out.info["cost"], 0);
    // Bun parity (processor.ts:443-447): assistant.info.tokens accumulates
    // across iterations. Iter 0 (CODEX_TOOL_CALL_STREAM) had 111/22/133;
    // iter 1 (CODEX_FINAL_TEXT_STREAM) had 55/11/66.
    assert_eq!(out.info["tokens"]["input"], 166);
    assert_eq!(out.info["tokens"]["output"], 33);
    assert_eq!(out.info["tokens"]["total"], 199);
    // Layout: [text, step-start_0, tool(read), step-finish_0(133),
    //          step-start_1, step-finish_1(66)]
    assert_eq!(out.parts[0]["type"], "text");
    assert_eq!(out.parts[0]["text"], "The file says: `fixture ok`.");
    assert_eq!(out.parts[1]["type"], "step-start");
    assert_eq!(out.parts[2]["type"], "tool");
    assert_eq!(out.parts[2]["tool"], "read");
    assert_eq!(out.parts[2]["callID"], "call_read_note");
    assert_eq!(out.parts[2]["state"]["status"], "completed");
    assert_eq!(out.parts[2]["state"]["input"]["filePath"], "repo/note.txt");
    assert!(out.parts[2]["state"]["output"]
        .as_str()
        .unwrap()
        .contains("fixture ok"));
    assert_eq!(out.parts[3]["type"], "step-finish");
    assert_eq!(out.parts[3]["reason"], "stop");
    assert_eq!(out.parts[3]["cost"], 0);
    assert_eq!(out.parts[3]["tokens"]["total"], 133);
    assert_eq!(out.parts[4]["type"], "step-start");
    assert_eq!(out.parts[5]["type"], "step-finish");
    assert_eq!(out.parts[5]["tokens"]["total"], 66);

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items.len(), 2);
    let msg = &page.items[1];
    assert_eq!(msg.info["role"], "assistant");
    assert_eq!(msg.info["tokens"]["total"], 199);
    assert_eq!(msg.parts[0]["text"], "The file says: `fixture ok`.");
    assert_eq!(msg.parts[1]["type"], "step-start");
    assert_eq!(msg.parts[2]["type"], "tool");
    assert_eq!(msg.parts[3]["type"], "step-finish");

    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 2);
    assert!(
        bodies[1].contains("\"type\":\"function_call\""),
        "{}",
        bodies[1]
    );
    assert!(
        bodies[1].contains("\"call_id\":\"call_read_note\""),
        "{}",
        bodies[1]
    );
    assert!(
        bodies[1].contains("\"type\":\"function_call_output\""),
        "{}",
        bodies[1]
    );
    assert!(bodies[1].contains("fixture ok"), "{}", bodies[1]);
    assert!(!bodies[1].contains("[Tool calls]"), "{}", bodies[1]);
    assert!(!bodies[1].contains("[Tool results]"), "{}", bodies[1]);

    let _ = std::fs::remove_dir_all(root);
}

/// Gap 5 policy: OpenAI without OAuth is not part of the Rust-only real
/// provider target yet. It must fail before turn/session mutation rather
/// than falling through to the legacy api-key chat path.
#[tokio::test]
async fn prompt_turn_openai_without_oauth_credential_rejects_before_mutation() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    assert!(!is_openai_oauth(
        &state,
        Some(&json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" }))
    ));

    let err = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "hi" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap_err();

    let TurnError::Unsupported(err) = err else {
        panic!("expected unsupported provider error");
    };
    assert_eq!(err.provider, "openai");
    assert_eq!(err.model.as_deref(), Some("gpt-5.1-codex"));
    let page = state.store.messages(&session.id, None, None).unwrap();
    assert!(page.items.is_empty());

    let _ = std::fs::remove_dir_all(root);
}

/// M10 invariant: aborting an active OAuth stream must propagate to the
/// in-flight reqwest body via the cancel flag. We model "in-flight" by
/// pre-setting the cancel signal *before* calling `prompt_turn`. The
/// assistant message must persist with `MessageAbortedError`, NOT a
/// successful completion. This guards against the "abort but stream
/// finishes" regression flagged in the migration plan.
#[tokio::test]
async fn prompt_turn_openai_oauth_abort_persists_interrupted() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    // A stream server that never sends a response — the cancel flag is
    // the only way the call ends.
    let server = stream_provider_server(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"slow\"}\n\n",
    )
    .await;
    state
        .store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "refresh": "rt",
                "access": "e30.eyJleHAiOjQxMDI0NDQ4MDAsImNoYXRncHRfYWNjb3VudF9pZCI6ImFjY3RfMSJ9.sig",
                "expires": 9999999999999i64,
                "accountId": "acct_1"
            }),
        )
        .unwrap();
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": {
                "openai": { "options": { "baseURL": server.url }, "models": {} }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    let cancel = Arc::new(AtomicBool::new(true)); // pre-aborted
    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "abort me" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        cancel,
    )
    .await
    .expect("prompt turn");
    // Pre-aborted: the prompt_turn early-out should fire BEFORE the
    // OAuth stream is started, so the result is a clean
    // MessageAbortedError envelope.
    assert_eq!(out.info["error"]["name"], "MessageAbortedError");

    let _ = std::fs::remove_dir_all(root);
}

/// M7 Fix 1+2+4: aborting MID-stream on the OAuth path must update the
/// existing assistant message in place — never append a second
/// (orphan) assistant record. Asserts the message count is exactly 2
/// (user + one assistant), the assistant info carries the
/// `MessageAbortedError` envelope, and any partial deltas streamed
/// before the abort are preserved on the message.
#[tokio::test]
async fn prompt_openai_stream_mid_stream_abort_updates_in_place_no_orphan() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    // Send a real text delta, THEN stall. Cancel will land while the
    // bytes loop awaits the next chunk — exercising the
    // `until_cancel` race in `post_stream`.
    let server = stream_provider_stalling_server(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
    )
    .await;
    state
        .store
        .set_provider_auth(
            "openai",
            json!({
                "type": "oauth",
                "refresh": "rt",
                "access": "e30.eyJleHAiOjQxMDI0NDQ4MDAsImNoYXRncHRfYWNjb3VudF9pZCI6ImFjY3RfMSJ9.sig",
                "expires": 9999999999999i64,
                "accountId": "acct_1"
            }),
        )
        .unwrap();
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": {
                "openai": { "options": { "baseURL": server.url }, "models": {} }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_handle = cancel.clone();
    let state_for_task = state.clone();
    let sid = session.id.clone();
    let task = tokio::spawn(async move {
        prompt_turn(
            &state_for_task,
            &sid,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "abort me" })],
                model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
                ..Default::default()
            },
            cancel_handle,
        )
        .await
    });

    // Let the prefix delta land, then cancel mid-stream.
    tokio::time::sleep(Duration::from_millis(150)).await;
    cancel.store(true, Ordering::SeqCst);

    let out = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("turn must unblock on cancel — abort race not wired")
        .expect("task join")
        .expect("prompt turn");

    assert_eq!(
        out.info["error"]["name"], "MessageAbortedError",
        "aborted turn must surface MessageAbortedError envelope"
    );

    // Storage check: exactly one assistant record (no orphan), and
    // any partial delta that streamed before the abort persisted.
    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(
        page.items.len(),
        2,
        "expected exactly user + assistant, got {} messages",
        page.items.len()
    );
    assert_eq!(page.items[0].info["role"], "user");
    assert_eq!(page.items[1].info["role"], "assistant");
    assert_eq!(page.items[1].info["error"]["name"], "MessageAbortedError");
    // Partial text part: present iff the delta landed before cancel.
    // Tolerant assertion — race may abort before any delta lands.
    let parts = &page.items[1].parts;
    if let Some(text_part) = parts.iter().find(|p| p["type"] == "text") {
        let text = text_part["text"].as_str().unwrap_or_default();
        assert!(
            text.is_empty() || text == "partial",
            "partial text must be either empty or the streamed delta, got: {text:?}"
        );
    }

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_openai_stream_abort_does_not_wait_for_running_tool() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    state.register_plugin_tool(
        "slow_diff",
        "Slow diff",
        json!({ "type": "object", "properties": {} }),
        |_| {
            std::thread::sleep(Duration::from_millis(1500));
            Ok(json!({ "ok": true }))
        },
    );
    let server = stream_provider_stalling_server(slow_plugin_stream()).await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "plugin": { "slow_diff": "allow" } })),
            ..Default::default()
        })
        .unwrap();

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_handle = cancel.clone();
    let state_for_task = state.clone();
    let sid = session.id.clone();
    let task = tokio::spawn(async move {
        prompt_turn(
            &state_for_task,
            &sid,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "review diff" })],
                model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
                ..Default::default()
            },
            cancel_handle,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    cancel.store(true, Ordering::SeqCst);

    let out = tokio::time::timeout(Duration::from_millis(750), task)
        .await
        .expect("abort must not wait for the running tool to finish")
        .expect("task join")
        .expect("prompt turn");

    assert_eq!(out.info["error"]["name"], "MessageAbortedError");
    let tool = out
        .parts
        .iter()
        .find(|part| part["tool"] == "slow_diff")
        .expect("aborted tool part");
    assert_eq!(tool["state"]["status"], "error");
    assert_eq!(tool["state"]["error"], "Tool call aborted");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_structured_output_captures_tool_payload_into_info() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    // Single iteration: model emits a function_call to `StructuredOutput`
    // with the typed payload, then `response.completed`.
    let server = stream_provider_server(
        "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_struct\",\"name\":\"StructuredOutput\",\"delta\":\"{\\\"city\\\":\\\"Paris\\\",\\\"temp\\\":21}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_struct\",\"name\":\"StructuredOutput\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":4,\"output_tokens\":2,\"total_tokens\":6}}}\n\n\
data: [DONE]\n\n",
    )
    .await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "weather please" })],
            model: Some(json!({
                "providerID": "openai",
                "modelID": "gpt-5.1-codex",
                "capabilities": { "toolcall": true }
            })),
            tools: Some(json!(true)),
            format: Some(json!({
                "type": "json_schema",
                "schema": {
                    "type": "object",
                    "properties": {
                        "city": { "type": "string" },
                        "temp": { "type": "number" }
                    },
                    "required": ["city", "temp"]
                }
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.info["finish"], "stop");
    assert_eq!(out.info["structured"]["city"], "Paris");
    assert_eq!(out.info["structured"]["temp"], 21);
    // The synthetic StructuredOutput call must NOT produce a real tool
    // part — the closure intercepts it and returns early.
    assert!(
        !out.parts.iter().any(|p| p["tool"] == "StructuredOutput"),
        "structured-output call leaked into a tool part: {out:?}"
    );
    let body = server.body.lock().unwrap().clone();
    assert!(
        body.contains("\"name\":\"StructuredOutput\""),
        "outbound tool catalog missing StructuredOutput: {body}"
    );
    assert!(
        body.contains("MUST use the StructuredOutput tool"),
        "outbound instructions missing structured-output system prompt: {body}"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// Provider mock that returns a 400 with a context-length error body
/// on the first request (the live turn), then a 200 JSON body for the
/// non-streaming summarization call, then a 200 SSE stream for the
/// retry. Drives the compaction overflow→retry test below.
async fn stream_provider_overflow_then_ok(
    success_data: &'static str,
) -> super::common::TestProvider {
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = Arc::new(Mutex::new(String::new()));
    let copy = body.clone();
    tokio::spawn(async move {
        // 1) Live turn → 400 context_length_exceeded.
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 65536];
        let _ = socket.read(&mut buf).await;
        let err_body = r#"{"error":{"code":"context_length_exceeded","message":"This model's maximum context length is 8192 tokens."}}"#;
        let head = format!(
            "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            err_body.len()
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(err_body.as_bytes()).await.unwrap();
        // 2) Compaction's non-streaming summarize call → 200 JSON.
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 65536];
        let _ = socket.read(&mut buf).await;
        let summary_body = r#"{"output":[{"type":"message","content":[{"type":"output_text","text":"User asked about X. Goal: Y. Latest: Z."}]}],"usage":{"input_tokens":50,"output_tokens":15,"total_tokens":65}}"#;
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            summary_body.len()
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(summary_body.as_bytes()).await.unwrap();
        // 3) Retry of live turn with compacted history → 200 SSE.
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 65536];
        let size = socket.read(&mut buf).await.unwrap();
        *copy.lock().unwrap() = String::from_utf8_lossy(&buf[..size]).to_string();
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            success_data.len()
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(success_data.as_bytes()).await.unwrap();
    });
    super::common::TestProvider {
        url: format!("http://{addr}"),
        body,
    }
}

#[tokio::test]
async fn prompt_turn_openai_oauth_compaction_recovers_from_context_overflow() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    let server = stream_provider_overflow_then_ok(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello after compact\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":3,\"total_tokens\":8}}}\n\n\
data: [DONE]\n\n",
    )
    .await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "long convo" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(
        out.info["finish"], "stop",
        "expected post-compaction success: {out:?}"
    );
    let text_part = out.parts.iter().find(|p| p["type"] == "text").unwrap();
    assert_eq!(text_part["text"], "hello after compact");

    let messages = state.store.messages(&session.id, None, None).unwrap();
    let summary_msg = messages
        .items
        .iter()
        .find(|m| m.info.get("summary").and_then(|v| v.as_bool()) == Some(true))
        .expect("summary anchor message must exist after compaction");
    assert_eq!(summary_msg.info["role"], "assistant");
    assert!(summary_msg.parts[0]["text"]
        .as_str()
        .unwrap_or_default()
        .contains("User asked about X"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_openai_oauth_structured_output_required_but_unused_yields_error() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    // Model ignores the requirement and just produces text.
    let server = stream_provider_server(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"sorry, no tool\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":3,\"total_tokens\":6}}}\n\n\
data: [DONE]\n\n",
    )
    .await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "weather please" })],
            model: Some(json!({
                "providerID": "openai",
                "modelID": "gpt-5.1-codex",
                "capabilities": { "toolcall": true }
            })),
            tools: Some(json!(true)),
            format: Some(json!({
                "type": "json_schema",
                "schema": { "type": "object" }
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(
        out.info["error"]["name"], "StructuredOutputError",
        "expected StructuredOutputError envelope, got: {:?}",
        out.info["error"]
    );

    let _ = std::fs::remove_dir_all(root);
}
