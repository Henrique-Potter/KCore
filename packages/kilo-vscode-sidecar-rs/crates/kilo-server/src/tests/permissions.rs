//! Permission, question, and suggestion routes plus mutating-tool
//! permission gating tests.

use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
};
use kilo_protocol::{PromptInput, SessionCreateInput};
use kilo_provider::ChatToolCall;
use serde_json::json;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use crate::agent::parts::{
    question_tool_part, real_mutating_tool_part, real_tool_part, real_tools,
};
use crate::agent::turn::start_runner;
use crate::routes::permissions::{
    permission_list, permission_rules, question_list, reject_question, reply_permission,
    reply_question,
};
use crate::routes::prompt::abort_session;

use super::common::{seed, state, state_at, unique_root};

#[tokio::test]
async fn permission_question_lists_empty_and_missing_replies_404() {
    let state = state();

    let permission = permission_list(&state);
    let question = question_list(&state);
    assert!(permission.is_empty());
    assert!(question.is_empty());

    let res = reply_permission(
        State(state.clone()),
        Path("missing".to_string()),
        Json(json!({ "reply": "allow" })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    let res = permission_rules(
        State(state.clone()),
        Path("missing".to_string()),
        Json(json!({ "approvedAlways": [] })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    let res = reply_question(
        State(state.clone()),
        Path("missing".to_string()),
        Json(json!({ "answers": [] })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    let res = reject_question(State(state), Path("missing".to_string())).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn permission_reply_allows_waiting_tool_call_once() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let repo = PathBuf::from(state.store.paths().directory);
    let call = ChatToolCall {
        id: "call_write_once".to_string(),
        name: "write".to_string(),
        input: json!({ "filePath": "allowed.txt", "content": "ok\n" }),
    };
    let mut bus = state.bus.subscribe();
    let got = state.clone();
    let dir = repo.clone();
    let sid = session.id.clone();
    let req = call.clone();
    let task = tokio::spawn(async move {
        real_tool_part(
            &got,
            &dir,
            &sid,
            "msg_perm_once",
            "prt_perm_once",
            0,
            &req,
            1,
            Arc::new(AtomicBool::new(false)),
            None,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let items = permission_list(&state);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["permission"], "edit");
    assert_eq!(items[0]["patterns"], json!(["allowed.txt"]));
    assert!(!repo.join("allowed.txt").exists());
    let asked = bus.try_recv().unwrap().as_global();
    assert_eq!(asked.payload.kind, "permission.asked");
    assert_eq!(asked.payload.properties["sessionID"], session.id);
    assert_eq!(
        asked.payload.properties["metadata"]["filePath"],
        "allowed.txt"
    );
    assert_eq!(
        asked.payload.properties["tool"]["callID"],
        "call_write_once"
    );
    assert_eq!(
        asked.payload.properties["tool"]["messageID"],
        "msg_perm_once"
    );

    let id = items[0]["id"].as_str().unwrap().to_string();
    let res = reply_permission(
        State(state.clone()),
        Path(id),
        Json(json!({ "reply": "once" })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let replied = bus.try_recv().unwrap().as_global();
    assert_eq!(replied.payload.kind, "permission.replied");
    assert_eq!(
        replied.payload.properties["requestID"],
        "permission_msg_perm_once_prt_perm_once_0"
    );
    assert_eq!(replied.payload.properties["reply"], "once");
    let part = task.await.unwrap();
    assert_eq!(part["state"]["status"], "completed");
    assert_eq!(
        std::fs::read_to_string(repo.join("allowed.txt")).unwrap(),
        "ok\n"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn question_reply_and_reject_publish_bun_shapes() {
    let state = state();
    let mut bus = state.bus.subscribe();
    let (tx_reply, _rx_reply) = tokio::sync::oneshot::channel();
    state.questions.lock().unwrap().insert(
        "que_reply".to_string(),
        crate::PendingQuestion {
            info: json!({
                "id": "que_reply",
                "sessionID": "ses_question",
                "questions": [{
                    "question": "Pick?",
                    "header": "Pick",
                    "options": [{ "label": "A", "description": "A" }]
                }],
                "blocking": true,
            }),
            reply: tx_reply,
        },
    );

    let res = reply_question(
        State(state.clone()),
        Path("que_reply".to_string()),
        Json(json!({ "answers": [["A"]] })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let event = bus.try_recv().unwrap().as_global();
    assert_eq!(event.payload.kind, "question.replied");
    assert_eq!(event.payload.properties["sessionID"], "ses_question");
    assert_eq!(event.payload.properties["requestID"], "que_reply");
    assert_eq!(event.payload.properties["answers"], json!([["A"]]));

    let (tx_reject, _rx_reject) = tokio::sync::oneshot::channel();
    state.questions.lock().unwrap().insert(
        "que_reject".to_string(),
        crate::PendingQuestion {
            info: json!({
                "id": "que_reject",
                "sessionID": "ses_question",
                "questions": [],
            }),
            reply: tx_reject,
        },
    );
    let res = reject_question(State(state), Path("que_reject".to_string())).await;
    assert_eq!(res.status(), StatusCode::OK);
    let event = bus.try_recv().unwrap().as_global();
    assert_eq!(event.payload.kind, "question.rejected");
    assert_eq!(event.payload.properties["sessionID"], "ses_question");
    assert_eq!(event.payload.properties["requestID"], "que_reject");
}

#[tokio::test]
async fn question_tool_publishes_webview_recoverable_shape() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let call = ChatToolCall {
        id: "call_question_shape".to_string(),
        name: "question".to_string(),
        input: json!({
            "text": "Continue?",
            "options": ["Yes", "No"],
        }),
    };
    let mut bus = state.bus.subscribe();
    let got = state.clone();
    let sid = session.id.clone();
    let req = call.clone();
    let task = tokio::spawn(async move {
        question_tool_part(&got, &sid, "msg_question", "prt_question", 0, &req, 1).await
    });

    for _ in 0..50 {
        if !question_list(&state).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let pending = question_list(&state);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["id"], "question_msg_question_prt_question_0");
    assert_eq!(pending[0]["sessionID"], session.id);
    assert_eq!(pending[0]["blocking"], true);
    assert_eq!(pending[0]["tool"]["messageID"], "msg_question");
    assert_eq!(pending[0]["tool"]["callID"], "call_question_shape");
    assert_eq!(pending[0]["questions"][0]["question"], "Continue?");
    assert_eq!(pending[0]["questions"][0]["header"], "Question");
    assert_eq!(pending[0]["questions"][0]["options"][0]["label"], "Yes");
    assert_eq!(pending[0]["questions"][0]["options"][0]["description"], "");
    assert_eq!(pending[0]["text"], "Continue?");
    let asked = bus.try_recv().unwrap().as_global();
    assert_eq!(asked.payload.kind, "question.asked");
    assert_eq!(
        asked.payload.properties["questions"][0]["options"][1]["label"],
        "No"
    );

    let res = reply_question(
        State(state.clone()),
        Path("question_msg_question_prt_question_0".to_string()),
        Json(json!({ "answers": [["Yes"]] })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let part = task.await.unwrap();
    assert_eq!(part["state"]["status"], "completed");
    assert_eq!(part["state"]["metadata"]["answers"], json!([["Yes"]]));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn abort_session_rejects_pending_prompts_for_child_sessions() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let parent = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let child = state
        .store
        .create_session(SessionCreateInput {
            parent_id: Some(parent.id.clone()),
            ..Default::default()
        })
        .unwrap();
    let run = start_runner(state.clone(), &parent.id).unwrap();
    let sub = start_runner(state.clone(), &child.id).unwrap();
    let mut bus = state.bus.subscribe();
    let (perm_tx, perm_rx) = tokio::sync::oneshot::channel();
    state.permissions.lock().unwrap().insert(
        "perm_child".to_string(),
        crate::PendingPermission {
            info: json!({
                "id": "perm_child",
                "sessionID": child.id.clone(),
                "permission": "edit",
            }),
            reply: perm_tx,
        },
    );
    let (question_tx, question_rx) = tokio::sync::oneshot::channel();
    state.questions.lock().unwrap().insert(
        "que_child".to_string(),
        crate::PendingQuestion {
            info: json!({
                "id": "que_child",
                "sessionID": child.id.clone(),
                "text": "continue?",
            }),
            reply: question_tx,
        },
    );
    let (suggestion_tx, suggestion_rx) = tokio::sync::oneshot::channel();
    state.suggestions.lock().unwrap().insert(
        "sgt_child".to_string(),
        crate::PendingSuggestion {
            info: json!({
                "id": "sgt_child",
                "sessionID": child.id.clone(),
            }),
            reply: suggestion_tx,
        },
    );

    let res = abort_session(State(state.clone()), Path(parent.id.clone())).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert!(run.cancel.load(Ordering::SeqCst));
    assert!(sub.cancel.load(Ordering::SeqCst));
    assert!(permission_list(&state).is_empty());
    assert!(question_list(&state).is_empty());
    assert_eq!(perm_rx.await.unwrap(), crate::PermissionDecision::Reject);
    assert!(matches!(
        question_rx.await.unwrap(),
        crate::QuestionReply::Rejected
    ));
    assert_eq!(
        suggestion_rx.await.unwrap(),
        crate::SuggestionDecision::Dismiss
    );
    let event = bus.try_recv().unwrap().as_global();
    assert_eq!(event.payload.kind, "permission.replied");
    assert_eq!(event.payload.properties["requestID"], "perm_child");
    assert_eq!(event.payload.properties["reply"], "reject");
    let event = bus.try_recv().unwrap().as_global();
    assert_eq!(event.payload.kind, "question.rejected");
    assert_eq!(event.payload.properties["requestID"], "que_child");
    let event = bus.try_recv().unwrap().as_global();
    assert_eq!(event.payload.kind, "suggestion.dismissed");
    assert_eq!(event.payload.properties["requestID"], "sgt_child");
    drop(run);
    drop(sub);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn mutating_tool_accept_reject_parts_keep_structured_shapes() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "edit": "allow" })),
            ..Default::default()
        })
        .unwrap();
    let repo = PathBuf::from(state.store.paths().directory);
    let call = ChatToolCall {
        id: "call_write_shape".to_string(),
        name: "write".to_string(),
        input: json!({ "filePath": "shape.txt", "content": "ok\n" }),
    };

    let part = real_tool_part(
        &state,
        &repo,
        &session.id,
        "msg_tool_shape",
        "prt_tool_shape",
        0,
        &call,
        1,
        Arc::new(AtomicBool::new(false)),
        None,
    )
    .await;
    assert_eq!(part["type"], "tool");
    assert_eq!(part["tool"], "write");
    assert_eq!(part["state"]["status"], "completed");
    assert_eq!(part["state"]["input"]["filePath"], "shape.txt");
    assert_eq!(part["state"]["metadata"]["file"], "shape.txt");
    assert!(part["state"]["metadata"]["diff"]
        .as_str()
        .unwrap()
        .contains("+ok"));

    let blocked = ChatToolCall {
        id: "call_write_blocked".to_string(),
        name: "write".to_string(),
        input: json!({ "filePath": "../blocked.txt", "content": "no\n" }),
    };
    let err = real_mutating_tool_part(
        &repo,
        "msg_tool_shape",
        "prt_tool_shape",
        1,
        "write",
        &blocked,
        1,
    );
    assert_eq!(err["state"]["status"], "error");
    assert_eq!(err["state"]["metadata"]["error"]["name"], "PathError");
    assert_eq!(err["state"]["input"]["filePath"], "../blocked.txt");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn permission_reject_prevents_mutating_tool_call() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let repo = PathBuf::from(state.store.paths().directory);
    let call = ChatToolCall {
        id: "call_write_reject".to_string(),
        name: "write".to_string(),
        input: json!({ "filePath": "denied.txt", "content": "no\n" }),
    };
    let got = state.clone();
    let dir = repo.clone();
    let sid = session.id.clone();
    let req = call.clone();
    let task = tokio::spawn(async move {
        real_tool_part(
            &got,
            &dir,
            &sid,
            "msg_perm_reject",
            "prt_perm_reject",
            0,
            &req,
            1,
            Arc::new(AtomicBool::new(false)),
            None,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let id = permission_list(&state)[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let res = reply_permission(
        State(state.clone()),
        Path(id),
        Json(json!({ "reply": "reject" })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let part = task.await.unwrap();
    assert_eq!(part["state"]["status"], "error");
    assert!(part["state"]["error"]
        .as_str()
        .unwrap_or_default()
        .contains("Permission rejected"));
    assert!(!repo.join("denied.txt").exists());

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn session_permission_allow_executes_write_without_prompt() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "edit": "allow" })),
            ..Default::default()
        })
        .unwrap();
    let repo = PathBuf::from(state.store.paths().directory);
    let call = ChatToolCall {
        id: "call_write_allowed".to_string(),
        name: "write".to_string(),
        input: json!({ "filePath": "preapproved.txt", "content": "yes\n" }),
    };
    let part = real_tool_part(
        &state,
        &repo,
        &session.id,
        "msg_perm_allowed",
        "prt_perm_allowed",
        0,
        &call,
        1,
        Arc::new(AtomicBool::new(false)),
        None,
    )
    .await;
    assert!(permission_list(&state).is_empty());
    assert_eq!(part["state"]["status"], "completed");
    assert_eq!(
        std::fs::read_to_string(repo.join("preapproved.txt")).unwrap(),
        "yes\n"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn real_mutating_write_is_pending_until_permission_reply() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let repo = PathBuf::from(state.store.paths().directory);
    let call = ChatToolCall {
        id: "call_write_pending".to_string(),
        name: "write".to_string(),
        input: json!({ "filePath": "pending.txt", "content": "wait\n" }),
    };
    let got = state.clone();
    let dir = repo.clone();
    let sid = session.id.clone();
    let req = call.clone();
    let task = tokio::spawn(async move {
        real_tool_part(
            &got,
            &dir,
            &sid,
            "msg_perm_pending",
            "prt_perm_pending",
            0,
            &req,
            1,
            Arc::new(AtomicBool::new(false)),
            None,
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(permission_list(&state).len(), 1);
    assert!(!task.is_finished());
    assert!(!repo.join("pending.txt").exists());

    let id = permission_list(&state)[0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let res = reply_permission(
        State(state.clone()),
        Path(id),
        Json(json!({ "reply": "allow" })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let part = task.await.unwrap();
    assert_eq!(part["state"]["status"], "completed");
    assert_eq!(
        std::fs::read_to_string(repo.join("pending.txt")).unwrap(),
        "wait\n"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn real_tools_only_advertises_mutating_tools_when_gate_active() {
    let state = state();
    let input = PromptInput {
        tools: Some(json!(true)),
        ..Default::default()
    };
    let names = real_tools(&state, &input)
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    assert!(state.permission_gate_active());
    assert_eq!(
        names,
        vec![
            "read",
            "grep",
            "task",
            "question",
            "write",
            "edit",
            "apply_patch",
            "bash"
        ]
    );
}

/// Regression: the VS Code extension's `client.session.promptAsync` flow
/// sends `model: { providerID, modelID }` only — no `capabilities` field.
/// Pre-fix `tools_on()` fell back to `model_toolcall(input.model)`, which
/// returned `false` for that shape, producing an empty tools array. The
/// model would then respond "I don't have tools to read files".
///
/// Post-fix: when `input.tools` is unset and `input.model.capabilities`
/// is absent, infer toolcall capability from the provider id. Every
/// model in our static OpenAI Codex catalog and the Kilo gateway
/// supports tool calling.
#[tokio::test]
async fn real_tools_infers_toolcall_from_provider_id_when_capabilities_absent() {
    let state = state();
    let input = PromptInput {
        tools: None,
        model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
        ..Default::default()
    };
    let names = real_tools(&state, &input)
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();

    // Sanity: the legacy capability-style model still works.
    let inline_caps = PromptInput {
        tools: None,
        model: Some(json!({
            "providerID": "openai",
            "modelID": "gpt-5.1-codex",
            "capabilities": { "toolcall": true },
        })),
        ..Default::default()
    };
    let names_inline = real_tools(&state, &inline_caps)
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert_eq!(names, names_inline);
    assert_eq!(
        names,
        vec![
            "read",
            "grep",
            "task",
            "question",
            "write",
            "edit",
            "apply_patch",
            "bash"
        ],
        "extension's `{{providerID, modelID}}` shape must produce a non-empty tools list",
    );

    // Explicit `tools: false` still wins — the inference is only the
    // fallback when the field is absent.
    let off = PromptInput {
        tools: Some(json!(false)),
        model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
        ..Default::default()
    };
    assert!(real_tools(&state, &off).is_empty());

    let task_only = PromptInput {
        tools: Some(json!({ "task": true })),
        model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
        ..Default::default()
    };
    let names = real_tools(&state, &task_only)
        .into_iter()
        .map(|tool| tool.name)
        .collect::<Vec<_>>();
    assert!(
        names.contains(&"task".to_string()),
        "tools map with task enabled must advertise task: {names:?}"
    );

    // Unknown provider doesn't trigger the inference.
    let unknown = PromptInput {
        tools: None,
        model: Some(json!({ "providerID": "anthropic", "modelID": "claude-3-7" })),
        ..Default::default()
    };
    assert!(real_tools(&state, &unknown).is_empty());
}
