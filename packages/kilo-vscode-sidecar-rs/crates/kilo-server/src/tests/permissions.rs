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
    accept_suggestion, allow_everything, permission_list, permission_rules, question_list,
    reject_question, reply_permission, reply_question, suggestion_list,
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

/// Fix H1: `task` subagent dispatch previously didn't re-check cancel
/// after the user approved permission. With the fix, a Stop press
/// during the "Allow subagent?" prompt produces an aborted tool part
/// instead of spawning a child session. We trigger the permission ask
/// for `task`, set cancel, then reply allow, and assert: tool part is
/// `error` with `Tool call aborted` and no child session was created.
#[tokio::test]
async fn task_subagent_cancel_after_permission_approve_skips_child_spawn() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let repo = PathBuf::from(state.store.paths().directory);
    let call = ChatToolCall {
        id: "call_task_cancel".to_string(),
        name: "task".to_string(),
        input: json!({
            "description": "summarize",
            "prompt": "summarize the code",
            "subagent_type": "general"
        }),
    };
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_handle = cancel.clone();
    let got = state.clone();
    let dir = repo.clone();
    let sid = session.id.clone();
    let req = call.clone();
    let task = tokio::spawn(async move {
        real_tool_part(
            &got,
            &dir,
            &sid,
            "msg_task_cancel",
            "prt_task_cancel",
            0,
            &req,
            1,
            cancel_handle,
            None,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let pending = permission_list(&state);
    assert_eq!(pending.len(), 1, "permission ask must be pending");
    let id = pending[0]["id"].as_str().unwrap().to_string();

    // Trip cancel BEFORE replying. The reply unblocks the await, but the
    // post-permission re-check must see the flag and short-circuit.
    cancel.store(true, Ordering::SeqCst);
    let res = reply_permission(
        State(state.clone()),
        Path(id),
        Json(json!({ "reply": "once" })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let part = task.await.unwrap();
    assert_eq!(part["type"], "tool");
    assert_eq!(part["tool"], "task");
    assert_eq!(part["state"]["status"], "error");
    assert_eq!(part["state"]["error"], "Tool call aborted");

    // No child session must have been created — task tool short-circuited
    // before `execute_task_tool` ran.
    let children = state.store.children(&session.id).unwrap_or_default();
    assert!(
        children.is_empty(),
        "no child session must be created when cancel races permission approve"
    );

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
        &AtomicBool::new(false),
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
            "glob",
            "grep",
            "webfetch",
            "todowrite",
            "skill",
            "suggest",
            "lsp",
            "task",
            "question",
            "write",
            "edit",
            "apply_patch",
            "bash"
        ]
    );
}

#[tokio::test]
async fn todowrite_tool_persists_todos_and_publishes_update() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "todowrite": "allow" })),
            ..Default::default()
        })
        .unwrap();
    let call = ChatToolCall {
        id: "call_todo".to_string(),
        name: "todowrite".to_string(),
        input: json!({
            "todos": [
                { "content": "map", "status": "completed", "priority": "high" },
                { "content": "ship", "status": "in_progress", "priority": "medium" }
            ]
        }),
    };
    let mut bus = state.bus.subscribe();

    let part = real_tool_part(
        &state,
        std::path::Path::new(&state.store.paths().directory),
        &session.id,
        "msg_todo",
        "prt_todo",
        0,
        &call,
        1,
        Arc::new(AtomicBool::new(false)),
        None,
    )
    .await;

    assert_eq!(part["tool"], "todowrite");
    assert_eq!(part["state"]["status"], "completed");
    assert_eq!(part["state"]["metadata"]["todos"][1]["content"], "ship");
    assert_eq!(
        state.store.todos(&session.id).unwrap()[1]["status"],
        "in_progress"
    );
    let event = bus.try_recv().unwrap().as_global();
    assert_eq!(event.payload.kind, "todo.updated");
    assert_eq!(event.payload.properties["sessionID"], session.id);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn lsp_tool_returns_document_symbols_without_lsp_server() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = PathBuf::from(state.store.paths().directory);
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(
        repo.join("src").join("main.rs"),
        "pub fn helper() {}\nstruct HelperState;\n",
    )
    .unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "lsp": "allow" })),
            ..Default::default()
        })
        .unwrap();
    let call = ChatToolCall {
        id: "call_lsp".to_string(),
        name: "lsp".to_string(),
        input: json!({
            "operation": "documentSymbol",
            "filePath": "src/main.rs",
            "line": 1,
            "character": 1,
        }),
    };

    let part = real_tool_part(
        &state,
        &repo,
        &session.id,
        "msg_lsp",
        "prt_lsp",
        0,
        &call,
        1,
        Arc::new(AtomicBool::new(false)),
        None,
    )
    .await;

    assert_eq!(part["tool"], "lsp");
    assert_eq!(part["state"]["status"], "completed");
    assert_eq!(part["state"]["metadata"]["result"][0]["name"], "helper");
    assert_eq!(
        part["state"]["metadata"]["result"][0]["path"],
        "src/main.rs"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn lsp_tool_reports_unavailable_server_for_position_queries() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = PathBuf::from(state.store.paths().directory);
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("main.rs"), "fn main() {}\n").unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "lsp": "allow" })),
            ..Default::default()
        })
        .unwrap();
    let call = ChatToolCall {
        id: "call_lsp_hover".to_string(),
        name: "lsp".to_string(),
        input: json!({
            "operation": "hover",
            "filePath": "main.rs",
            "line": 1,
            "character": 4,
        }),
    };

    let part = real_tool_part(
        &state,
        &repo,
        &session.id,
        "msg_lsp_hover",
        "prt_lsp_hover",
        0,
        &call,
        1,
        Arc::new(AtomicBool::new(false)),
        None,
    )
    .await;

    assert_eq!(part["tool"], "lsp");
    assert_eq!(part["state"]["status"], "error");
    assert!(part["state"]["error"]
        .as_str()
        .unwrap_or_default()
        .contains("No LSP server available"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn suggest_tool_waits_for_accept_and_returns_prompt() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let repo = PathBuf::from(state.store.paths().directory);
    let call = ChatToolCall {
        id: "call_suggest".to_string(),
        name: "suggest".to_string(),
        input: json!({
            "suggest": "Review these changes?",
            "actions": [{
                "label": "Review",
                "description": "Run local review",
                "prompt": "/local-review-uncommitted"
            }]
        }),
    };
    let mut bus = state.bus.subscribe();
    let got = state.clone();
    let sid = session.id.clone();
    let req = call.clone();
    let task = tokio::spawn(async move {
        real_tool_part(
            &got,
            &repo,
            &sid,
            "msg_suggest",
            "prt_suggest",
            0,
            &req,
            1,
            Arc::new(AtomicBool::new(false)),
            None,
        )
        .await
    });

    for _ in 0..50 {
        if !suggestion_list(&state).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let pending = suggestion_list(&state);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["id"], "suggestion_msg_suggest_prt_suggest_0");
    assert_eq!(pending[0]["blocking"], false);
    assert_eq!(pending[0]["tool"]["callID"], "call_suggest");
    let shown = bus.try_recv().unwrap().as_global();
    assert_eq!(shown.payload.kind, "suggestion.shown");
    assert_eq!(shown.payload.properties["text"], "Review these changes?");
    let idle = bus.try_recv().unwrap().as_global();
    assert_eq!(idle.payload.kind, "session.status");

    let res = accept_suggestion(
        State(state.clone()),
        Path("suggestion_msg_suggest_prt_suggest_0".to_string()),
        Json(json!({ "index": 0 })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let accepted = bus.try_recv().unwrap().as_global();
    assert_eq!(accepted.payload.kind, "session.idle");
    let accepted = bus.try_recv().unwrap().as_global();
    assert_eq!(accepted.payload.kind, "suggestion.accepted");
    let part = task.await.unwrap();
    assert_eq!(part["tool"], "suggest");
    assert_eq!(part["state"]["status"], "completed");
    assert_eq!(part["state"]["metadata"]["accepted"]["label"], "Review");
    assert!(part["state"]["output"]
        .as_str()
        .unwrap_or_default()
        .contains("/local-review-uncommitted"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn skill_tool_loads_discovered_skill_content() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = PathBuf::from(state.store.paths().directory);
    let dir = repo.join(".kilo").join("skills").join("focus");
    std::fs::create_dir_all(dir.join("scripts")).unwrap();
    std::fs::write(
        dir.join("SKILL.md"),
        "---\nname: focus\ndescription: Focus workflow\n---\nUse focused steps.",
    )
    .unwrap();
    std::fs::write(dir.join("scripts").join("run.ps1"), "Write-Output ok").unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "skill": { "focus": "allow" } })),
            ..Default::default()
        })
        .unwrap();
    let call = ChatToolCall {
        id: "call_skill".to_string(),
        name: "skill".to_string(),
        input: json!({ "name": "focus" }),
    };

    let part = real_tool_part(
        &state,
        &repo,
        &session.id,
        "msg_skill",
        "prt_skill",
        0,
        &call,
        1,
        Arc::new(AtomicBool::new(false)),
        None,
    )
    .await;

    assert_eq!(part["tool"], "skill");
    assert_eq!(part["state"]["status"], "completed");
    let output = part["state"]["output"].as_str().unwrap_or_default();
    assert!(output.contains("<skill_content name=\"focus\">"));
    assert!(output.contains("Use focused steps."));
    assert!(output.contains("<skill_files>"));
    assert!(output.contains("run.ps1"));
    assert_eq!(part["state"]["metadata"]["name"], "focus");

    let _ = std::fs::remove_dir_all(root);
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
            "glob",
            "grep",
            "webfetch",
            "todowrite",
            "skill",
            "suggest",
            "lsp",
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
    assert_eq!(names, vec!["task"]);

    // Unknown provider doesn't trigger the inference.
    let unknown = PromptInput {
        tools: None,
        model: Some(json!({ "providerID": "anthropic", "modelID": "claude-3-7" })),
        ..Default::default()
    };
    assert!(real_tools(&state, &unknown).is_empty());
}

/// Helper: park a pending permission entry directly into state without
/// going through the full `real_tool_part` flow. Returns the receiver so
/// tests can assert on the resolution, and the request id used.
fn park_pending(
    state: &Arc<crate::AppState>,
    id: &str,
    sid: &str,
    permission: &str,
    patterns: Vec<&str>,
    metadata: serde_json::Value,
) -> tokio::sync::oneshot::Receiver<crate::PermissionDecision> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let info = json!({
        "id": id,
        "sessionID": sid,
        "status": "pending",
        "permission": permission,
        "patterns": patterns,
        "always": patterns,
        "metadata": metadata,
        "tool": { "messageID": "mid", "callID": "call", "name": permission },
    });
    state
        .permissions
        .lock()
        .unwrap()
        .insert(id.to_string(), crate::PendingPermission { info, reply: tx });
    rx
}

/// Fix 1: `saveAlwaysRules` must drain pending sibling entries the new
/// rule covers. Park two pending `edit` requests for the same session;
/// reply to the first via `/always-rules` with `approvedAlways: ["*"]`.
/// The sibling's pattern (`other.txt`) is covered by the wildcard, so it
/// must resolve `once` and disappear from the pending map.
#[tokio::test]
async fn save_always_rules_drains_covered_sibling() {
    let state = state();
    let sid = "ses_drain";
    let mut bus = state.bus.subscribe();
    park_pending(
        &state,
        "perm_origin",
        sid,
        "edit",
        vec!["origin.txt"],
        json!({ "filePath": "origin.txt" }),
    );
    let sibling_rx = park_pending(
        &state,
        "perm_sibling",
        sid,
        "edit",
        vec!["other.txt"],
        json!({ "filePath": "other.txt" }),
    );
    // Drop pre-existing bus events.
    while bus.try_recv().is_ok() {}

    let res = permission_rules(
        State(state.clone()),
        Path("perm_origin".to_string()),
        Json(json!({ "approvedAlways": ["*"] })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    // The sibling must have been drained as `once` (allow). Origin is
    // intentionally left in the pending map for the UI's follow-up reply.
    let pending = permission_list(&state);
    let ids: Vec<&str> = pending
        .iter()
        .map(|item| item["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["perm_origin"]);
    assert_eq!(
        sibling_rx.await.unwrap(),
        crate::PermissionDecision::Allow,
        "sibling must resolve as `once` once the wildcard rule covers it",
    );

    // A `permission.replied` for the sibling must have been published.
    let mut saw = false;
    while let Ok(event) = bus.try_recv() {
        let event = event.as_global();
        if event.payload.kind == "permission.replied"
            && event.payload.properties["requestID"] == "perm_sibling"
        {
            assert_eq!(event.payload.properties["reply"], "once");
            saw = true;
            break;
        }
    }
    assert!(saw, "drainCovered must publish permission.replied");
}

/// Fix 2: a `reject` reply must cascade to every sibling pending entry
/// in the same session. Park three entries — two on session A, one on
/// session B — then reject one of session A's. Both A entries must
/// resolve to `Reject`; B must remain untouched.
#[tokio::test]
async fn reject_reply_cascades_across_session_siblings() {
    let state = state();
    let mut bus = state.bus.subscribe();
    let rx_a1 = park_pending(
        &state,
        "perm_a1",
        "ses_a",
        "edit",
        vec!["a1.txt"],
        json!({ "filePath": "a1.txt" }),
    );
    let rx_a2 = park_pending(
        &state,
        "perm_a2",
        "ses_a",
        "edit",
        vec!["a2.txt"],
        json!({ "filePath": "a2.txt" }),
    );
    let _rx_b = park_pending(
        &state,
        "perm_b",
        "ses_b",
        "edit",
        vec!["b.txt"],
        json!({ "filePath": "b.txt" }),
    );
    while bus.try_recv().is_ok() {}

    let res = reply_permission(
        State(state.clone()),
        Path("perm_a1".to_string()),
        Json(json!({ "reply": "reject" })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    assert_eq!(rx_a1.await.unwrap(), crate::PermissionDecision::Reject);
    assert_eq!(rx_a2.await.unwrap(), crate::PermissionDecision::Reject);
    let pending = permission_list(&state);
    assert_eq!(pending.len(), 1, "session B's entry must remain pending");
    assert_eq!(pending[0]["id"], "perm_b");

    // Two `permission.replied` events with reply=reject expected.
    let mut rejected = Vec::new();
    while let Ok(event) = bus.try_recv() {
        let event = event.as_global();
        if event.payload.kind == "permission.replied"
            && event.payload.properties["reply"] == "reject"
        {
            rejected.push(
                event.payload.properties["requestID"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }
    }
    assert!(rejected.contains(&"perm_a1".to_string()));
    assert!(rejected.contains(&"perm_a2".to_string()));
    assert!(!rejected.contains(&"perm_b".to_string()));
}

/// Fix 3: `POST /permission/allow-everything` (no sessionID) appends the
/// global wildcard rule and drains every pending entry. We park two
/// entries on different sessions; both must resolve `once` after the
/// route runs.
#[tokio::test]
async fn allow_everything_global_clears_all_pending() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let rx1 = park_pending(
        &state,
        "perm_a",
        "ses_x",
        "edit",
        vec!["x.txt"],
        json!({ "filePath": "x.txt" }),
    );
    let rx2 = park_pending(
        &state,
        "perm_b",
        "ses_y",
        "bash",
        vec!["ls"],
        json!({ "command": "ls" }),
    );

    let res = allow_everything(State(state.clone()), Json(json!({ "enable": true }))).await;
    assert_eq!(res.status(), StatusCode::OK);

    assert!(permission_list(&state).is_empty());
    assert_eq!(rx1.await.unwrap(), crate::PermissionDecision::Allow);
    assert_eq!(rx2.await.unwrap(), crate::PermissionDecision::Allow);

    // The wildcard rule must now be visible in approvals.
    let approvals = state.approvals.lock().unwrap().clone();
    assert!(approvals
        .iter()
        .any(|r| r.permission == "*" && r.pattern == "*" && r.action == "allow"));

    let _ = std::fs::remove_dir_all(root);
}

/// Fix 4: editing a path inside `.kilo/` must downgrade the user's
/// "always" choice to a one-shot allow — no rule may be persisted, and
/// the metadata must carry the `disableAlways: true` UI hint.
#[tokio::test]
async fn config_path_downgrades_always_to_once() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let repo = PathBuf::from(state.store.paths().directory);
    std::fs::create_dir_all(repo.join(".kilo/agents")).unwrap();
    let call = ChatToolCall {
        id: "call_kilo_edit".to_string(),
        name: "write".to_string(),
        input: json!({ "filePath": ".kilo/agents/x.json", "content": "{}\n" }),
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
            "msg_kilo_edit",
            "prt_kilo_edit",
            0,
            &req,
            1,
            Arc::new(AtomicBool::new(false)),
            None,
        )
        .await
    });

    // Wait for the permission ask.
    for _ in 0..50 {
        if !permission_list(&state).is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let pending = permission_list(&state);
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0]["metadata"]["disableAlways"], true,
        "protected requests must mark `disableAlways` so the UI hides Allow always",
    );
    let id = pending[0]["id"].as_str().unwrap().to_string();

    // User clicks "Allow always" — must be downgraded to once: tool
    // succeeds, but no permission rule is persisted.
    let res = reply_permission(
        State(state.clone()),
        Path(id),
        Json(json!({ "reply": "always" })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let part = task.await.unwrap();
    assert_eq!(part["state"]["status"], "completed");

    let approvals = state.approvals.lock().unwrap().clone();
    assert!(
        approvals.is_empty(),
        "always→once on protected paths must NOT persist a rule, got {approvals:?}",
    );
    let persisted = state.store.permission_rules();
    assert!(persisted.is_empty(), "disk persistence must also be empty");

    let _ = std::fs::remove_dir_all(root);
}
