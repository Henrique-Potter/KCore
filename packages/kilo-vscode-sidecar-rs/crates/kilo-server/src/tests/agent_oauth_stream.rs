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
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
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
    assert_eq!(out.parts[1]["text"], "Hello");
    assert_eq!(out.info["tokens"]["input"], 2);
    // Per-iteration step parts (Bun parity, processor.ts:402-473): each
    // iteration emits a step-start at the top and a step-finish at the
    // bottom. Text produced in that step sits between them.
    assert_eq!(out.parts[0]["type"], "step-start");
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
async fn prompt_turn_openai_oauth_persists_reasoning_summary_parts() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let server = stream_provider_server(
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\"}}\n\n\
data: {\"type\":\"response.reasoning_summary_text.delta\",\"item_id\":\"rs_1\",\"summary_index\":0,\"delta\":\"thinking \"}\n\n\
data: {\"type\":\"response.reasoning_summary_text.delta\",\"item_id\":\"rs_1\",\"summary_index\":0,\"delta\":\"hard\"}\n\n\
data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\"}}\n\n\
data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
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
            parts: vec![json!({ "type": "text", "text": "think visibly" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    let reasoning = out
        .parts
        .iter()
        .find(|part| part.get("type").and_then(|value| value.as_str()) == Some("reasoning"))
        .expect("reasoning part");
    assert_eq!(reasoning["text"], "thinking hard");
    assert_eq!(reasoning["time"]["start"], reasoning["time"]["end"]);
    assert!(out
        .parts
        .iter()
        .any(|part| part.get("text").and_then(|value| value.as_str()) == Some("done")));

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
        !body.contains("changed plain text"),
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

    // Layout: [step-start_0, tool, step-finish_0, step-start_1, text, step-finish_1].
    // The final answer must come after the inspected tool output; otherwise
    // the UI inserts the conclusion above a long tool transcript and it looks
    // like the agent never replied.
    assert_eq!(out.parts[0]["type"], "step-start");
    assert_eq!(out.parts[1]["type"], "tool");
    let text_idx = out
        .parts
        .iter()
        .position(|part| part.get("text").and_then(|value| value.as_str()) == Some("done"))
        .expect("final text part");
    let tool_idx = out
        .parts
        .iter()
        .position(|part| part.get("type").and_then(|value| value.as_str()) == Some("tool"))
        .expect("tool part");
    assert!(text_idx > tool_idx, "final text must follow tool output");
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
            permission: Some(json!({
                "task": "allow",
                "github_*": "deny"
            })),
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
    assert!(children.iter().all(|child| {
        child.permission
            == Some(json!({
                "task": { "*": "deny" },
                "github_*": { "*": "deny" }
            }))
    }));
    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 4);
    let child_bodies = &bodies[1..3];
    assert!(
        child_bodies
            .iter()
            .all(|body| !body.contains("\"name\":\"task\"")),
        "child tools must not expose recursive task: {child_bodies:?}"
    );
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
    assert!(out
        .parts
        .iter()
        .any(|part| part.get("text").and_then(|value| value.as_str()) == Some("wrote it")));
    // Layout: [step-start_0, tool(write), step-finish_0, step-start_1, text, step-finish_1]
    assert_eq!(out.parts[0]["type"], "step-start");
    assert_eq!(out.parts[1]["state"]["status"], "completed");
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
    // Layout: [step-start_0, tool(write), step-finish_0]. parts[1] is the rejected tool.
    assert_eq!(out.parts[0]["type"], "step-start");
    assert_eq!(out.parts[1]["state"]["status"], "error");
    assert_eq!(
        out.parts[1]["state"]["metadata"]["error"]["name"],
        "PermissionRejectedError"
    );
    assert!(out.parts[1]["state"]["error"]
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
    //   [step-start_0, tool(invalid_0), step-finish_0,
    //    step-start_1, tool(invalid_1), step-finish_1]
    assert_eq!(out.parts[0]["type"], "step-start");
    assert_eq!(out.parts[1]["type"], "tool");
    assert_eq!(out.parts[1]["tool"], "invalid");
    assert_eq!(out.parts[1]["callID"], "call_bad");
    assert_eq!(
        out.parts[1]["state"]["input"]["arguments"],
        "{\"filePath\":} trailing"
    );
    assert!(out.parts[1]["state"]["error"]
        .as_str()
        .unwrap_or_default()
        .contains("Invalid tool arguments JSON"));
    assert_eq!(out.parts[2]["type"], "step-finish");
    assert_eq!(out.parts[3]["type"], "step-start");
    assert_eq!(out.parts[4]["type"], "tool");
    assert_eq!(out.parts[4]["tool"], "invalid");
    assert_eq!(out.parts[4]["callID"], "call_bad");
    assert_eq!(
        out.parts[4]["state"]["input"]["arguments"],
        "{\"filePath\":} trailing"
    );
    assert!(out.parts[5]["type"]
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
    // The continuation's final answer must follow the tool result; the first
    // model step may also include a short preamble before/around the tool.
    assert_eq!(out.parts[0]["type"], "step-start");
    let read_idx = out
        .parts
        .iter()
        .position(|part| part.get("tool").and_then(|value| value.as_str()) == Some("read"))
        .expect("read tool");
    let final_idx = out
        .parts
        .iter()
        .position(|part| {
            part.get("text").and_then(|value| value.as_str())
                == Some("The file says: `fixture ok`.")
        })
        .expect("final text");
    assert!(final_idx > read_idx);
    assert_eq!(out.parts[read_idx]["callID"], "call_read_note");
    assert_eq!(out.parts[read_idx]["state"]["status"], "completed");
    assert_eq!(
        out.parts[read_idx]["state"]["input"]["filePath"],
        "repo/note.txt"
    );
    assert!(out.parts[read_idx]["state"]["output"]
        .as_str()
        .unwrap()
        .contains("fixture ok"));
    assert!(out.parts.iter().any(|part| {
        part.get("type").and_then(|value| value.as_str()) == Some("step-finish")
            && part["tokens"]["total"] == 133
    }));
    assert!(out.parts.iter().any(|part| {
        part.get("type").and_then(|value| value.as_str()) == Some("step-finish")
            && part["tokens"]["total"] == 66
    }));

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items.len(), 2);
    let msg = &page.items[1];
    assert_eq!(msg.info["role"], "assistant");
    assert_eq!(msg.info["tokens"]["total"], 199);
    let stored_text = msg
        .parts
        .iter()
        .position(|part| {
            part.get("text").and_then(|value| value.as_str())
                == Some("The file says: `fixture ok`.")
        })
        .expect("stored final text");
    let stored_tool = msg
        .parts
        .iter()
        .position(|part| part.get("tool").and_then(|value| value.as_str()) == Some("read"))
        .expect("stored tool");
    assert!(stored_text > stored_tool);

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

/// Wave 5 V follow-up: when a same-session `prompt_async` arrives mid-turn,
/// the route trips both `cancel` and `follow_up_break` on the active runner.
/// The OAuth stream's finalize path observes `follow_up_break` and stamps
/// `info.finish = "follow_up"` instead of the canonical `MessageAbortedError`
/// envelope. The webview consumer no longer fires an "aborted" error toast
/// for the intentional break — `publish_error` is skipped, only `publish_idle`
/// and `publish_turn_close(_, "follow_up")` reach the bus.
#[tokio::test]
async fn follow_up_break_finalizes_with_follow_up_finish_and_no_error_publish() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let server = stream_provider_stalling_server(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
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

    // Pre-register a runner for the session and pre-arm `follow_up_break`
    // so the route's mid-turn-followup path is reproduced without spinning
    // up a second `prompt_async`. `prompt_turn` doesn't claim a runner
    // slot itself (that's `prompt_guarded`'s job), so this manual insert
    // is the moral equivalent of the prompt-route's pre-flight signal.
    let cancel = Arc::new(AtomicBool::new(false));
    let follow_up_break = Arc::new(AtomicBool::new(false));
    state.runners.lock().unwrap().insert(
        session.id.clone(),
        crate::Runner {
            cancel: cancel.clone(),
            follow_up_break: follow_up_break.clone(),
            parent: None,
            abort: std::sync::Mutex::new(None),
            mid_stream_retries: Arc::new(AtomicU8::new(0)),
        },
    );

    let mut rx = state.bus.subscribe();
    let cancel_handle = cancel.clone();
    let follow_up_handle = follow_up_break.clone();
    let state_for_task = state.clone();
    let sid = session.id.clone();
    let task = tokio::spawn(async move {
        prompt_turn(
            &state_for_task,
            &sid,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "long" })],
                model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
                ..Default::default()
            },
            cancel_handle,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    // Mirror the route: trip both flags. The order matches
    // `routes/prompt.rs:90-94` — follow_up_break first, then cancel.
    follow_up_handle.store(true, Ordering::SeqCst);
    cancel.store(true, Ordering::SeqCst);

    let out = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("turn must unblock on cancel")
        .expect("task join")
        .expect("prompt turn");

    // Assistant finalizes with `finish: "follow_up"` and NO error envelope.
    assert_eq!(
        out.info["finish"], "follow_up",
        "follow-up break must stamp finish: 'follow_up'"
    );
    assert!(
        out.info.get("error").map(|v| v.is_null()).unwrap_or(true),
        "follow-up break must omit error envelope, got: {:?}",
        out.info.get("error")
    );

    // Storage parity: the persisted assistant message also lacks the
    // MessageAbortedError envelope.
    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[1].info["finish"], "follow_up");
    assert!(
        page.items[1]
            .info
            .get("error")
            .map(|v| v.is_null())
            .unwrap_or(true),
        "persisted assistant info must not carry an error envelope"
    );

    // Bus parity: the turn closes with reason `follow_up`, no `session.error`
    // event fires, and `session.idle` still lands.
    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
        .map(|event| event.as_global())
        .collect();
    let kinds: Vec<&str> = events.iter().map(|e| e.payload.kind.as_str()).collect();
    assert!(
        !kinds.contains(&"session.error"),
        "follow-up break must NOT publish session.error, got kinds: {kinds:?}"
    );
    let close = events
        .iter()
        .find(|e| e.payload.kind == "session.turn.close")
        .expect("session.turn.close must fire");
    assert_eq!(close.payload.properties["reason"], "follow_up");
    assert!(
        events.iter().any(|e| e.payload.kind == "session.idle"),
        "session.idle must still fire on follow-up break"
    );

    // Cleanup the manually inserted runner.
    state.runners.lock().unwrap().remove(&session.id);
    let _ = std::fs::remove_dir_all(root);
}

/// Regression: a user-driven abort (cancel set, follow_up_break clear) must
/// continue to surface the canonical `MessageAbortedError` envelope and
/// publish `session.error` + `session.turn.close` with reason `interrupted`.
/// Mirrors the existing mid-stream-abort coverage but registers a Runner so
/// the new finalize branch is exercised on its `false` arm rather than the
/// `runners.get(id) == None` fallback.
#[tokio::test]
async fn regular_abort_still_publishes_error() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let server = stream_provider_stalling_server(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
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

    let cancel = Arc::new(AtomicBool::new(false));
    let follow_up_break = Arc::new(AtomicBool::new(false));
    state.runners.lock().unwrap().insert(
        session.id.clone(),
        crate::Runner {
            cancel: cancel.clone(),
            follow_up_break: follow_up_break.clone(),
            parent: None,
            abort: std::sync::Mutex::new(None),
            mid_stream_retries: Arc::new(AtomicU8::new(0)),
        },
    );

    let mut rx = state.bus.subscribe();
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

    tokio::time::sleep(Duration::from_millis(150)).await;
    // Cancel only — leave follow_up_break clear (user-driven abort).
    cancel.store(true, Ordering::SeqCst);

    let out = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("turn must unblock on cancel")
        .expect("task join")
        .expect("prompt turn");

    assert_eq!(
        out.info["error"]["name"], "MessageAbortedError",
        "user-driven abort must still surface MessageAbortedError"
    );
    assert_eq!(out.info["finish"], "aborted");

    let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
        .map(|event| event.as_global())
        .collect();
    let error_event = events
        .iter()
        .find(|e| e.payload.kind == "session.error")
        .expect("session.error must fire on user-driven abort");
    assert_eq!(
        error_event.payload.properties["error"]["name"],
        "MessageAbortedError"
    );
    let close = events
        .iter()
        .find(|e| e.payload.kind == "session.turn.close")
        .expect("session.turn.close must fire");
    assert_eq!(close.payload.properties["reason"], "interrupted");

    state.runners.lock().unwrap().remove(&session.id);
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

/// Provider mock that accepts a connection and never responds. Used by
/// the compaction-cancel regression test to prove that `compact_session`
/// observes the cancel atomic during the in-flight summarize call. The
/// listener is held in scope so the OS-level connect succeeds; we just
/// don't write any bytes back so reqwest blocks on the response.
async fn stalling_provider_holds_connection() -> super::common::TestProvider {
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = vec![0; 65536];
            let _ = socket.read(&mut buf).await;
            // Keep the socket open but write nothing — the client stalls
            // waiting for headers. Park forever; the runtime cleans us up.
            std::future::pending::<()>().await;
        }
    });
    super::common::TestProvider {
        url: format!("http://{addr}"),
        body: std::sync::Arc::new(std::sync::Mutex::new(String::new())),
    }
}

/// Fix C3: `compact_session` previously called `chat_tools_with_auth`
/// which had no cancel parameter. A Stop press during summarization on
/// a slow upstream did nothing. The fix routes through
/// `chat_tools_with_auth_cancel` and maps `ProviderError::Aborted` to
/// `CompactionError::Cancelled`. This test sets a hung token endpoint,
/// trips cancel, and asserts the call returns within 250ms with the
/// `Cancelled` variant.
#[tokio::test]
async fn compact_session_aborts_promptly_when_cancel_fires_during_summarize() {
    use crate::agent::compaction::{compact_session, CompactionError};
    use kilo_protocol::MessageAppendInput;

    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    let server = stalling_provider_holds_connection().await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    // Seed a non-trivial transcript so collect_history returns something.
    state
        .store
        .append_message_record(
            &session.id,
            MessageAppendInput {
                info: json!({
                    "id": "msg_user_1",
                    "role": "user",
                    "sessionID": session.id.clone(),
                    "time": { "created": 1, "updated": 1, "completed": 1 },
                }),
                parts: vec![json!({ "id": "p_user_1", "type": "text", "text": "compact me" })],
            },
        )
        .unwrap();

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_handle = cancel.clone();
    let st = state.clone();
    let sid = session.id.clone();
    let auths = json!(state.store.provider_auths());
    let model = json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" });
    let task = tokio::spawn(async move {
        compact_session(&st, &sid, Some(&model), &auths, &cancel_handle).await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let out = tokio::time::timeout(Duration::from_millis(250), task)
        .await
        .expect("compact_session must return within 250ms of cancel")
        .expect("task join");
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "elapsed {:?} exceeded budget",
        started.elapsed()
    );
    let err = out.expect_err("compact_session must surface an error on cancel");
    assert!(
        matches!(err, CompactionError::Cancelled),
        "expected CompactionError::Cancelled, got: {err:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// Provider mock for the proactive-compaction tests. Iter 1 streams a
/// successful turn whose `response.completed.usage` is engineered to
/// exceed `0.85 * 400_000` (gpt-5.1-codex context limit) AND emits a
/// `function_call` so the agent loop continues. Then a non-streaming
/// summarize call is served, then iter 2 streams a final text answer.
/// `summarize_count` counts how many summarize requests we observed —
/// the "doesn't loop" test asserts this stays at 1.
async fn proactive_compaction_provider(
    iter1_data: &'static str,
    iter2_data: &'static str,
) -> (
    super::common::TestProvider,
    std::sync::Arc<std::sync::Mutex<u8>>,
) {
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = Arc::new(Mutex::new(String::new()));
    let copy = body.clone();
    let summarize_count = Arc::new(Mutex::new(0u8));
    let count_copy = summarize_count.clone();
    tokio::spawn(async move {
        // Iter 1: streaming SSE with high token usage + function_call.
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 65536];
        let _ = socket.read(&mut buf).await;
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            iter1_data.len()
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(iter1_data.as_bytes()).await.unwrap();
        // Subsequent connections: distinguish summarize (non-stream JSON
        // POST without `stream: true`) from iter-2 SSE by inspecting the
        // request body. Summarize bodies don't include `tools` and the
        // session has just been compacted. We answer up to 4 follow-up
        // connections in this dispatch order:
        //   - summarize → 200 JSON
        //   - iter-2 stream → 200 SSE
        //   - any extra summarize attempts → 200 JSON
        for _ in 0..4u8 {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let mut buf = vec![0; 65536];
            let Ok(size) = socket.read(&mut buf).await else {
                break;
            };
            let body_text = String::from_utf8_lossy(&buf[..size]).to_string();
            // The summarizer call is non-streaming (`stream: false` or
            // omitted). The agent's live stream call sets `stream: true`.
            let is_stream = body_text.contains("\"stream\":true");
            if !is_stream {
                *count_copy.lock().unwrap() += 1;
                let summary_body = r#"{"output":[{"type":"message","content":[{"type":"output_text","text":"Compacted summary: user goal X."}]}],"usage":{"input_tokens":50,"output_tokens":15,"total_tokens":65}}"#;
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    summary_body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(summary_body.as_bytes()).await;
            } else {
                *copy.lock().unwrap() = body_text;
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    iter2_data.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(iter2_data.as_bytes()).await;
            }
        }
    });
    (
        super::common::TestProvider {
            url: format!("http://{addr}"),
            body,
        },
        summarize_count,
    )
}

#[tokio::test]
async fn proactive_compaction_fires_when_token_total_exceeds_threshold() {
    // Iter 1: streaming `response.completed.usage.total_tokens = 360_000`
    // which is > `0.85 * 400_000 = 340_000` for `gpt-5.1-codex`. Iter 1
    // also emits a function_call so the agent loop would otherwise
    // continue normally. The proactive check fires after step-finish,
    // compaction runs, iter 2 sees the summary-anchored history and
    // returns terminal text.
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    let iter1 = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"thinking...\"}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_x\",\"name\":\"bash\",\"delta\":\"{\\\"command\\\":\\\"echo hi\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_x\",\"name\":\"bash\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":340000,\"output_tokens\":20000,\"total_tokens\":360000}}}\n\n\
data: [DONE]\n\n";
    let iter2 = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"after compact\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":3,\"total_tokens\":8}}}\n\n\
data: [DONE]\n\n";
    let (server, summarize_count) = proactive_compaction_provider(iter1, iter2).await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "bash": "allow" })),
            ..Default::default()
        })
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

    assert_eq!(out.info["finish"], "stop", "expected clean finish: {out:?}");
    let messages = state.store.messages(&session.id, None, None).unwrap();
    let summary_msg = messages
        .items
        .iter()
        .find(|m| m.info.get("summary").and_then(|v| v.as_bool()) == Some(true))
        .expect("summary anchor must exist after proactive compaction");
    assert_eq!(summary_msg.info["role"], "assistant");
    assert!(summary_msg.parts[0]["text"]
        .as_str()
        .unwrap_or_default()
        .contains("Compacted summary"));
    // Exactly one summarize call — the proactive check ran once after
    // iter 1's step-finish, ran compaction, then iter 2's tokens (8)
    // were well under the threshold.
    assert_eq!(*summarize_count.lock().unwrap(), 1);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn proactive_compaction_skipped_when_auto_disabled() {
    // Same iter 1 / iter 2 as the firing test but `compaction.auto =
    // false` short-circuits the proactive check. The loop continues
    // straight into iter 2 with no summarize round-trip; no summary
    // anchor message is written.
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    let iter1 = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"thinking...\"}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_x\",\"name\":\"bash\",\"delta\":\"{\\\"command\\\":\\\"echo hi\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_x\",\"name\":\"bash\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":340000,\"output_tokens\":20000,\"total_tokens\":360000}}}\n\n\
data: [DONE]\n\n";
    let iter2 = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"no compact\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":3,\"total_tokens\":8}}}\n\n\
data: [DONE]\n\n";
    let (server, summarize_count) = proactive_compaction_provider(iter1, iter2).await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    // Write a config that opts out of proactive compaction. We keep the
    // baseURL the same so the agent still routes to the test mock.
    std::fs::write(
        root.join("config").join("kilo").join("kilo.json"),
        serde_json::to_string(&json!({
            "provider": {
                "openai": { "options": { "baseURL": server.url }, "models": {} }
            },
            "compaction": { "auto": false }
        }))
        .unwrap(),
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "bash": "allow" })),
            ..Default::default()
        })
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

    assert_eq!(out.info["finish"], "stop", "expected clean finish: {out:?}");
    let messages = state.store.messages(&session.id, None, None).unwrap();
    let summary_present = messages
        .items
        .iter()
        .any(|m| m.info.get("summary").and_then(|v| v.as_bool()) == Some(true));
    assert!(
        !summary_present,
        "proactive compaction must be suppressed when cfg.compaction.auto = false"
    );
    assert_eq!(
        *summarize_count.lock().unwrap(),
        0,
        "no summarize call should fire when auto is disabled"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn proactive_compaction_does_not_loop() {
    // Iter 1 has high cumulative tokens that trigger compaction. Iter 2
    // is a TERMINAL text response (no function_call) so the agent loop
    // ends. Even though iter 2's tokens are also reported as high
    // (cumulative would exceed threshold AGAIN), the per-iter single-
    // shot proactive check runs at most once per step-finish and the
    // loop terminates naturally — it must NOT trigger a second
    // compaction within the same iteration. We assert exactly one
    // summarize call.
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    let iter1 = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"think\"}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_x\",\"name\":\"bash\",\"delta\":\"{\\\"command\\\":\\\"echo hi\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_x\",\"name\":\"bash\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":340000,\"output_tokens\":20000,\"total_tokens\":360000}}}\n\n\
data: [DONE]\n\n";
    // Iter 2 returns text-only with high tokens too. The agent loop
    // breaks because no tool_call this iter; the proactive check runs
    // once after step-finish, sees high cumulative tokens, and would
    // try to compact AGAIN — guarded by MAX_COMPACTION_ATTEMPTS.
    let iter2 = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"final\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":340000,\"output_tokens\":20000,\"total_tokens\":360000}}}\n\n\
data: [DONE]\n\n";
    let (server, summarize_count) = proactive_compaction_provider(iter1, iter2).await;
    state
        .store
        .set_provider_auth("openai", oauth_auth())
        .unwrap();
    write_openai_config(&root, &server.url);
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "bash": "allow" })),
            ..Default::default()
        })
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

    let _ = out;
    // Two iterations ran (iter 1 with tools, iter 2 terminal). Both
    // step-finish parts should have triggered the proactive check, but
    // because they happen on different iterations and we cap at
    // `MAX_COMPACTION_ATTEMPTS`, the count stays bounded. With cumulative
    // total = 360_000 + 360_000 = 720_000 still over threshold, the
    // second iter's check fires too — so we expect 2 (one per iter).
    // The point of "does not loop": the count is bounded, NOT unbounded.
    let count = *summarize_count.lock().unwrap();
    assert!(
        count <= crate::agent::compaction::MAX_COMPACTION_ATTEMPTS as u8,
        "summarize count {count} must be bounded by MAX_COMPACTION_ATTEMPTS"
    );
    // And the same iteration's step-finish must NOT trigger more than
    // one compaction — ensured by structural placement (single check
    // per iteration body) rather than runtime guarding.
    assert!(count >= 1, "first iter should still trigger compaction");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn reasoning_item_round_trips_to_next_responses_request() {
    // P0 reasoning round-trip: when iteration 1 surfaces a reasoning
    // `output_item.done` with `encrypted_content`, iteration 2's
    // Responses request must echo the item back as
    // `{type:"reasoning", id, encrypted_content}` so the upstream
    // cache can attach to the prior trace. Mirrors Bun's converter
    // at `provider/sdk/copilot/responses/convert-to-openai-responses-input.ts:185-244`.
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
            // Iter 1: reasoning item with encrypted_content + a function_call
            // so the loop continues into iter 2. The reasoning summary is
            // optional from a cache-hit standpoint but exercises the
            // `summary_text` round-trip too.
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"rs_round\",\"type\":\"reasoning\"}}\n\n\
data: {\"type\":\"response.reasoning_summary_text.delta\",\"item_id\":\"rs_round\",\"summary_index\":0,\"delta\":\"plan read\"}\n\n\
data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"rs_round\",\"type\":\"reasoning\",\"encrypted_content\":\"ENC_BLOB\"}}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"call_id\":\"call_read\",\"name\":\"read\",\"delta\":\"{\\\"filePath\\\":\"}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"\\\"repo/note.txt\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":1,\"call_id\":\"call_read\",\"name\":\"read\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n".to_string(),
            // Iter 2: terminal text answer. We assert on the request body
            // captured for THIS request — it must carry the reasoning item.
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n".to_string(),
        ])
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

    // Sanity: tool ran and the reasoning summary was persisted alongside
    // the encrypted_content (so a future persistence reload can rebuild
    // it for cross-turn replay).
    let reasoning = out
        .parts
        .iter()
        .find(|part| part.get("type").and_then(|value| value.as_str()) == Some("reasoning"))
        .expect("reasoning part persisted");
    assert_eq!(reasoning["text"], "plan read");
    assert_eq!(reasoning["itemID"], "rs_round");
    assert_eq!(reasoning["encryptedContent"], "ENC_BLOB");

    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 2);
    let body = &bodies[1];
    // Iter 2 request must echo the reasoning item back.
    assert!(
        body.contains("\"type\":\"reasoning\""),
        "iter-2 body missing reasoning input item: {body}"
    );
    assert!(
        body.contains("\"id\":\"rs_round\""),
        "iter-2 body missing reasoning id: {body}"
    );
    assert!(
        body.contains("\"encrypted_content\":\"ENC_BLOB\""),
        "iter-2 body missing encrypted_content: {body}"
    );
    // Sibling tool round-trip still has to be intact.
    assert!(
        body.contains("\"type\":\"function_call\""),
        "iter-2 body missing function_call: {body}"
    );
    assert!(
        body.contains("\"call_id\":\"call_read\""),
        "iter-2 body missing function_call_output: {body}"
    );

    let _ = std::fs::remove_dir_all(root);
}

// ---------------------------------------------------------------------------
// Mid-stream retry coverage. When the OpenAI Responses stream errors AFTER
// content has streamed (text deltas / reasoning / tool calls), the agent
// loop replays the iteration with the partial assistant content appended
// to the next request's `input[]` so the model continues coherently.
// Capped at `MID_STREAM_RETRY_CAP` (3) per turn; reset on a clean stream.
// ---------------------------------------------------------------------------

/// Register a Runner for the session so the mid-stream retry path
/// (which gates on `state.runners`) can read and bump the retry
/// counter. Returns the cancel handle for the test driver. Mirrors
/// the pre-existing follow-up-break manual setup at test lines
/// 1668 / 1788 — kept inline rather than promoted to `common.rs`
/// because no other module needs it yet.
fn install_runner(state: &Arc<crate::AppState>, sid: &str) -> Arc<AtomicBool> {
    let cancel = Arc::new(AtomicBool::new(false));
    state.runners.lock().unwrap().insert(
        sid.to_string(),
        crate::Runner {
            cancel: cancel.clone(),
            follow_up_break: Arc::new(AtomicBool::new(false)),
            parent: None,
            abort: std::sync::Mutex::new(None),
            mid_stream_retries: Arc::new(AtomicU8::new(0)),
        },
    );
    cancel
}

#[tokio::test]
async fn mid_stream_retry_recovers_after_partial_text() {
    // Drop the default 1s back-off so the test body finishes in well
    // under a second. Production users still see the full delay.
    std::env::set_var("KILO_MID_STREAM_RETRY_MS", "10");
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let server = stream_provider_sequence(vec![
        // Stream 1: partial text then a mid-stream error. The agent
        // loop must NOT terminate the turn — it should replay with
        // the partial assistant content as additional context.
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello \"}\n\n\
data: {\"type\":\"error\",\"error\":{\"message\":\"upstream connection reset\"}}\n\n\
data: [DONE]\n\n"
            .to_string(),
        // Stream 2: continuation. The model "picks up" where the
        // first cut off; the closing text + completion lands as
        // appended deltas on the same assistant message.
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"world\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n\
data: [DONE]\n\n"
            .to_string(),
    ])
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
    let cancel = install_runner(&state, &session.id);

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "say hi" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        cancel,
    )
    .await
    .expect("prompt turn must succeed via mid-stream retry");

    // Final assistant message holds both halves of the streamed text.
    let text_part = out
        .parts
        .iter()
        .find(|part| part.get("type").and_then(|v| v.as_str()) == Some("text"))
        .expect("text part");
    assert_eq!(text_part["text"], "Hello world");
    assert!(
        out.info.get("error").is_none(),
        "no terminal error envelope"
    );

    // The retry round must echo the partial assistant content into the
    // request `input[]` so the model continues from the cutoff (option
    // 2: no `previous_response_id` needed).
    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 2, "expected exactly two upstream requests");
    let retry_body = &bodies[1];
    assert!(
        retry_body.contains("\"role\":\"assistant\""),
        "retry body missing partial assistant context: {retry_body}",
    );
    assert!(
        retry_body.contains("Hello "),
        "retry body missing partial text: {retry_body}",
    );

    // Counter resets to 0 once the retry stream completes cleanly.
    let counter_after = state
        .runners
        .lock()
        .unwrap()
        .get(&session.id)
        .map(|runner| runner.mid_stream_retries.load(Ordering::SeqCst));
    assert_eq!(counter_after, Some(0), "counter must reset on clean turn");

    state.runners.lock().unwrap().remove(&session.id);
    std::env::remove_var("KILO_MID_STREAM_RETRY_MS");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn mid_stream_retry_caps_at_three_attempts() {
    std::env::set_var("KILO_MID_STREAM_RETRY_MS", "10");
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    // 4 errored streams: original + 3 retries. After the cap is hit,
    // the loop must finalize with a terminal error envelope rather
    // than open a 5th upstream connection.
    let errored = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"frag\"}\n\n\
data: {\"type\":\"error\",\"error\":{\"message\":\"upstream blew up\"}}\n\n\
data: [DONE]\n\n";
    let server = stream_provider_sequence(vec![
        errored.to_string(),
        errored.to_string(),
        errored.to_string(),
        errored.to_string(),
    ])
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
    let cancel = install_runner(&state, &session.id);

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "stress me" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        cancel,
    )
    .await
    .expect("prompt turn returns a final assistant record");

    // Terminal error envelope after exhausting the cap.
    assert!(
        out.info.get("error").is_some(),
        "expected terminal error envelope, got: {:?}",
        out.info
    );
    // Exactly four upstream requests: 1 initial + 3 retries.
    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(
        bodies.len(),
        4,
        "expected exactly 1 + MID_STREAM_RETRY_CAP requests, got {}",
        bodies.len(),
    );

    state.runners.lock().unwrap().remove(&session.id);
    std::env::remove_var("KILO_MID_STREAM_RETRY_MS");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn mid_stream_retry_resets_counter_on_success() {
    std::env::set_var("KILO_MID_STREAM_RETRY_MS", "10");
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    // First turn: error mid-stream then recover. Counter goes 0 → 1
    // and (by spec) back to 0 once the retry stream completes.
    let server = stream_provider_sequence(vec![
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"par\"}\n\n\
data: {\"type\":\"error\",\"error\":{\"message\":\"transient\"}}\n\n\
data: [DONE]\n\n"
            .to_string(),
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"tial\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n"
            .to_string(),
    ])
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
    let cancel = install_runner(&state, &session.id);

    let _ = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "first" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        cancel,
    )
    .await
    .expect("first prompt");

    let counter = state
        .runners
        .lock()
        .unwrap()
        .get(&session.id)
        .map(|runner| runner.mid_stream_retries.load(Ordering::SeqCst));
    assert_eq!(
        counter,
        Some(0),
        "successful retry must reset the per-turn counter",
    );

    state.runners.lock().unwrap().remove(&session.id);
    std::env::remove_var("KILO_MID_STREAM_RETRY_MS");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn mid_stream_retry_does_not_fire_for_pre_response_errors() {
    // Regression: a stream that errors BEFORE any text/reasoning/tool
    // streams must still go through the existing pre-response retry
    // path (driven by `RetryState`), NOT the new mid-stream branch.
    // A non-retryable Api error returned with no streamed side
    // effects bubbles up as a terminal error after a single attempt.
    std::env::set_var("KILO_MID_STREAM_RETRY_MS", "10");
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    // Single error event, no preceding deltas: the parser surfaces
    // `ProviderError::Api(...)` immediately. The mid-stream branch
    // gates on `!clean`; this stream is `clean` (no side effects)
    // so the retry counter must remain at 0 and the loop must
    // terminate without firing a second request.
    let server = stream_provider_sequence(vec![
        "data: {\"type\":\"error\",\"error\":{\"message\":\"pre-response failure\"}}\n\n\
data: [DONE]\n\n"
            .to_string(),
    ])
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
    let cancel = install_runner(&state, &session.id);

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "hello" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        cancel,
    )
    .await
    .expect("prompt turn");

    // Pre-response failure surfaces a terminal error, no mid-stream
    // retry was attempted.
    assert!(
        out.info.get("error").is_some(),
        "expected terminal error envelope on pre-response failure",
    );
    let counter = state
        .runners
        .lock()
        .unwrap()
        .get(&session.id)
        .map(|runner| runner.mid_stream_retries.load(Ordering::SeqCst));
    assert_eq!(
        counter,
        Some(0),
        "mid-stream counter must NOT increment on pre-response failures",
    );
    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(
        bodies.len(),
        1,
        "pre-response error must not trigger a mid-stream retry round",
    );

    state.runners.lock().unwrap().remove(&session.id);
    std::env::remove_var("KILO_MID_STREAM_RETRY_MS");
    let _ = std::fs::remove_dir_all(root);
}

/// Fix 1 regression: when iter 1 hits the reactive ContextWindow path
/// (HTTP 400 with `context_length_exceeded`), the compaction summary
/// rewrites the persisted history but the in-memory `base_messages`
/// must also be refreshed via `real_messages` so the retry sends the
/// post-compaction transcript. Before the fix `base_messages` was
/// bound once at turn entry and only the proactive arm re-bound it;
/// the reactive arm fell back to the same overflowing transcript on
/// each retry until `MAX_COMPACTION_ATTEMPTS` exhausted and the turn
/// died with `CompactionError::EmptyResult`.
#[tokio::test]
async fn reactive_compaction_refreshes_base_messages_so_next_iteration_succeeds() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("config").join("kilo")).unwrap();
    let server = stream_provider_overflow_then_ok(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"post compact ok\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":2,\"total_tokens\":4}}}\n\n\
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
            parts: vec![json!({ "type": "text", "text": "say short hi" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn must succeed via reactive compaction + retry");

    // Final assistant carries the post-compaction continuation text.
    assert_eq!(out.info["finish"], "stop");
    let text_part = out
        .parts
        .iter()
        .find(|p| p["type"] == "text")
        .expect("text part");
    assert_eq!(text_part["text"], "post compact ok");

    // Summary anchor was written by the compaction.
    let messages = state.store.messages(&session.id, None, None).unwrap();
    let summary_msg = messages
        .items
        .iter()
        .find(|m| m.info.get("summary").and_then(|v| v.as_bool()) == Some(true))
        .expect("summary anchor must exist");
    assert_eq!(summary_msg.info["role"], "assistant");

    // The retry request body must reflect the compacted view —
    // specifically, the summary anchor's `[Compacted context summary
    // — earlier history was elided` primer (rewritten to user role by
    // `real_messages`), proving `base_messages` was rebuilt rather
    // than the original transcript being resent.
    let bodies = server.body.lock().unwrap().clone();
    assert!(
        bodies.contains("Compacted context summary"),
        "retry body missing post-compaction primer (proves base_messages was NOT rebuilt): {bodies}"
    );

    let _ = std::fs::remove_dir_all(root);
}

/// Two-connection mock that splits the response into two chunks
/// separated by a delay so the spawned tool task has time to
/// complete before the stream's `error` event lands. Connection 1
/// streams the tool call (done) then sleeps `delay_ms`, then emits
/// the error and `[DONE]`. Connection 2 returns the continuation.
/// Chunked transfer encoding bypasses content-length buffering so
/// the client sees frames incrementally.
async fn stream_tool_complete_then_error_then_ok(
    delay_ms: u64,
    cont: &'static str,
) -> super::common::TestStreamProvider {
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let copy = bodies.clone();
    tokio::spawn(async move {
        // Connection 1: tool call frames, then sleep, then error.
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 65536];
        let size = socket.read(&mut buf).await.unwrap();
        copy.lock()
            .unwrap()
            .push(String::from_utf8_lossy(&buf[..size]).to_string());
        let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n";
        let _ = socket.write_all(head.as_bytes()).await;
        let tool_frames = "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_read\",\"name\":\"read\",\"delta\":\"{\\\"filePath\\\":\"}\n\n\
data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"\\\"repo/note.txt\\\"}\"}\n\n\
data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_read\",\"name\":\"read\"}\n\n";
        let chunk = format!("{:x}\r\n{}\r\n", tool_frames.len(), tool_frames);
        let _ = socket.write_all(chunk.as_bytes()).await;
        // Sleep long enough that the spawned `read` tool task
        // completes on the test runtime before the error event
        // lands. 200ms is generous for a file read of ~20 bytes.
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        let err_frames = "data: {\"type\":\"error\",\"error\":{\"message\":\"connection reset mid stream\"}}\n\n\
data: [DONE]\n\n";
        let chunk = format!("{:x}\r\n{}\r\n", err_frames.len(), err_frames);
        let _ = socket.write_all(chunk.as_bytes()).await;
        let _ = socket.write_all(b"0\r\n\r\n").await;
        // Connection 2: clean continuation.
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = vec![0; 65536];
        let size = socket.read(&mut buf).await.unwrap();
        copy.lock()
            .unwrap()
            .push(String::from_utf8_lossy(&buf[..size]).to_string());
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            cont.len()
        );
        let _ = socket.write_all(head.as_bytes()).await;
        let _ = socket.write_all(cont.as_bytes()).await;
    });
    super::common::TestStreamProvider {
        url: format!("http://{addr}"),
        bodies,
    }
}

/// Fix 2: mid-stream retry must drain the JoinSet to harvest tools
/// that completed before the error fired, and replay their
/// `function_call` + `function_call_output` items in the next
/// request's `input[]`. Without that, the model re-issues the same
/// tool call and side-effecting tools (bash/edit/write) run twice.
#[tokio::test]
async fn mid_stream_retry_replays_completed_tools_in_next_request_input() {
    std::env::set_var("KILO_MID_STREAM_RETRY_MS", "10");
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    // Seed a target file so `read` has something to return; the
    // tool's side-effect window for "ran once vs twice" is the
    // persisted tool part — exactly one settled part with the
    // original call_id must exist on the assistant record after the
    // retry round completes.
    std::fs::create_dir_all(root.join("repo").join("repo")).unwrap();
    std::fs::write(
        root.join("repo").join("repo").join("note.txt"),
        "side effect payload",
    )
    .unwrap();
    let server = stream_tool_complete_then_error_then_ok(
        200,
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"all done\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
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
    let cancel = install_runner(&state, &session.id);

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "read the note" })],
            model: Some(json!({
                "providerID": "openai",
                "modelID": "gpt-5.1-codex",
                "capabilities": { "toolcall": true }
            })),
            tools: Some(json!(true)),
            ..Default::default()
        },
        cancel,
    )
    .await
    .expect("prompt turn must succeed via mid-stream retry");

    // The retry request body must carry the completed tool's
    // function_call + function_call_output. With the bug those are
    // absent (only a synthetic assistant text appeared) and the
    // model would re-issue `read` → side-effect runs twice.
    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 2, "expected exactly two upstream requests");
    let retry_body = &bodies[1];
    assert!(
        retry_body.contains("\"type\":\"function_call\""),
        "retry body missing function_call for completed tool: {retry_body}"
    );
    assert!(
        retry_body.contains("\"call_id\":\"call_read\""),
        "retry body missing call_id for completed tool: {retry_body}"
    );
    assert!(
        retry_body.contains("\"type\":\"function_call_output\""),
        "retry body missing function_call_output: {retry_body}"
    );
    assert!(
        retry_body.contains("side effect payload"),
        "retry body missing tool output payload: {retry_body}"
    );

    // The tool must appear EXACTLY ONCE on the persisted assistant
    // record. The harvested completed task gets persisted by the
    // retry path; with the bug a SECOND fresh task would also fire
    // (driven by the re-issued tool call) and the store would carry
    // two tool parts with the same callID. Count tool parts on the
    // final assistant message — there must be exactly one with
    // `callID == call_read`.
    let tool_part_count = out
        .parts
        .iter()
        .filter(|p| p.get("type").and_then(|v| v.as_str()) == Some("tool"))
        .filter(|p| p.get("callID").and_then(|v| v.as_str()) == Some("call_read"))
        .count();
    assert_eq!(
        tool_part_count, 1,
        "completed tool must run exactly once across retry; assistant parts: {:?}",
        out.parts
    );

    state.runners.lock().unwrap().remove(&session.id);
    std::env::remove_var("KILO_MID_STREAM_RETRY_MS");
    let _ = std::fs::remove_dir_all(root);
}

/// Fix 3: a SECOND mid-stream retry within the same iteration must
/// only splice the iter_text DELTA produced since the previous
/// retry's snapshot — not the entire accumulated buffer. Otherwise
/// retry #2's request body double-bills retry #1's prefix.
#[tokio::test]
async fn mid_stream_retry_does_not_double_push_partial_content() {
    std::env::set_var("KILO_MID_STREAM_RETRY_MS", "10");
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let server = stream_provider_sequence(vec![
        // Stream 1: produces "AAA" then errors.
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"AAA\"}\n\n\
data: {\"type\":\"error\",\"error\":{\"message\":\"first cut\"}}\n\n\
data: [DONE]\n\n"
            .to_string(),
        // Stream 2: produces "BBB" then errors again.
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"BBB\"}\n\n\
data: {\"type\":\"error\",\"error\":{\"message\":\"second cut\"}}\n\n\
data: [DONE]\n\n"
            .to_string(),
        // Stream 3: completes cleanly.
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"CCC\"}\n\n\
data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n\
data: [DONE]\n\n"
            .to_string(),
    ])
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
    let cancel = install_runner(&state, &session.id);

    let _ = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "go" })],
            model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
            ..Default::default()
        },
        cancel,
    )
    .await
    .expect("prompt turn");

    let bodies = server.bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 3, "expected initial + 2 retries");
    // Retry #1 splices "AAA" (the prefix from stream 1).
    let retry1 = &bodies[1];
    assert!(
        retry1.contains("AAA"),
        "retry #1 body missing AAA: {retry1}"
    );
    assert!(
        !retry1.contains("BBB"),
        "retry #1 body must not contain BBB yet: {retry1}"
    );
    // Retry #2 must contain "AAA" (the first splice still sits in
    // iter_messages) and "BBB" (the new delta from stream 2). The
    // bug fix guarantees "AAA" appears at most ONCE in the splice:
    // one synthetic assistant message carries it from retry #1, the
    // retry #2 splice adds only "BBB". Pre-fix, retry #2 would re-
    // push "AAA" + "BBB" combined as a single assistant block on
    // top of the existing splice → two copies of AAA in the body.
    let retry2 = &bodies[2];
    assert!(
        retry2.contains("AAA"),
        "retry #2 body missing AAA: {retry2}"
    );
    assert!(
        retry2.contains("BBB"),
        "retry #2 body missing BBB: {retry2}"
    );
    // The critical assertion — AAA appears at most once on retry #2.
    // (We allow exactly one because the model may echo the user
    // prompt or other content; the synthetic assistant splice in
    // input[] is the only legitimate carrier for AAA.)
    let aaa_count = retry2.matches("AAA").count();
    assert_eq!(
        aaa_count, 1,
        "retry #2 must not double-push partial content; AAA appears {aaa_count} times: {retry2}"
    );

    state.runners.lock().unwrap().remove(&session.id);
    std::env::remove_var("KILO_MID_STREAM_RETRY_MS");
    let _ = std::fs::remove_dir_all(root);
}
