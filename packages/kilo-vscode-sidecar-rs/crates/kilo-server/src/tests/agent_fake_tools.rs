//! Fake-provider tool-call tests covering `grep`, `read`, `write`,
//! `edit`, `apply_patch`, and `bash` tool execution paths.

use kilo_protocol::{PromptInput, SessionCreateInput};
use serde_json::json;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::agent::turn::prompt_turn;

use super::common::{drain, seed, state_at, unique_root};

#[tokio::test]
async fn prompt_turn_fake_grep_persists_match_metadata() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(repo.join("src").join("a.txt"), "needle one\nnone\n").unwrap();
    std::fs::write(repo.join("src").join("b.txt"), "needle two\n").unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "grep needle" })],
            provider: Some(json!({
                "fakeToolCalls": [{
                    "tool": "grep",
                    "input": { "pattern": "needle", "path": "src" }
                }]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    let tool = &out.parts[1];
    assert_eq!(tool["tool"], "grep");
    assert_eq!(tool["state"]["status"], "completed");
    assert_eq!(tool["state"]["title"], "needle");
    assert_eq!(tool["state"]["metadata"]["matches"], 2);
    assert_eq!(tool["state"]["metadata"]["truncated"], false);
    let output = tool["state"]["output"].as_str().unwrap();
    assert!(output.contains("Found 2 matches"));
    assert!(output.contains("src/a.txt:"));
    assert!(output.contains("Line 1: needle one"));
    assert!(output.contains("src/b.txt:"));
    assert_eq!(out.parts[2]["type"], "step-finish");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_tool_events_are_before_step_finish_and_close() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("note.txt"), "hello\n").unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    let mut rx = state.bus.subscribe();

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "read" })],
            provider: Some(json!({
                "fakeToolCalls": [{
                    "tool": "read",
                    "input": { "filePath": "note.txt" }
                }]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    let events = drain(&mut rx);
    let tool = events
        .iter()
        .position(|event| {
            event.payload.sync_event.as_ref().is_some_and(|data| {
                data["type"] == "message.part.updated.1" && data["data"]["part"]["type"] == "tool"
            })
        })
        .expect("tool event");
    let step = events
        .iter()
        .position(|event| {
            event.payload.sync_event.as_ref().is_some_and(|data| {
                data["type"] == "message.part.updated.1"
                    && data["data"]["part"]["type"] == "step-finish"
            })
        })
        .expect("step event");
    let close = events
        .iter()
        .position(|event| event.payload.kind == "session.turn.close")
        .expect("close event");
    assert!(tool < step);
    assert!(step < close);
    assert_eq!(out.parts[1]["state"]["status"], "completed");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_read_unsafe_path_persists_tool_error() {
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
            parts: vec![json!({ "type": "text", "text": "read secret" })],
            provider: Some(json!({
                "fakeToolCalls": [{
                    "tool": "read",
                    "input": { "filePath": "../secret.txt" }
                }]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.info["finish"], "stop");
    assert_eq!(out.parts[1]["type"], "tool");
    assert_eq!(out.parts[1]["tool"], "read");
    assert_eq!(out.parts[1]["state"]["status"], "error");
    assert!(out.parts[1]["state"]["error"]
        .as_str()
        .unwrap()
        .contains("Unsafe path"));
    assert_eq!(out.parts[2]["type"], "step-finish");

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items[1].parts[1]["state"]["status"], "error");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_write_creates_file_and_persists_metadata() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "write file" })],
            provider: Some(json!({
                "fakeToolCalls": [{
                    "tool": "write",
                    "input": { "filePath": "src/new.txt", "content": "hello\nworld\n" }
                }]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(
        std::fs::read_to_string(repo.join("src/new.txt")).unwrap(),
        "hello\nworld\n"
    );
    let tool = &out.parts[1];
    assert_eq!(tool["tool"], "write");
    assert_eq!(tool["state"]["status"], "completed");
    assert_eq!(tool["state"]["title"], "src/new.txt");
    assert_eq!(tool["state"]["output"], "Wrote file successfully.");
    assert_eq!(tool["state"]["metadata"]["exists"], false);
    assert!(tool["state"]["metadata"]["filepath"]
        .as_str()
        .unwrap()
        .ends_with("repo/src/new.txt"));
    assert!(tool["state"]["metadata"]["diff"]
        .as_str()
        .unwrap()
        .contains("+hello"));

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items[1].parts[1]["tool"], "write");
    assert_eq!(page.items[1].parts[1]["state"]["metadata"]["exists"], false);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_edit_changes_file_and_persists_metadata() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("note.txt"), "one\ntwo\nthree\n").unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "edit file" })],
            provider: Some(json!({
                "fakeToolCalls": [{
                    "tool": "edit",
                    "input": {
                        "filePath": "note.txt",
                        "oldString": "two",
                        "newString": "TWO"
                    }
                }]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(
        std::fs::read_to_string(repo.join("note.txt")).unwrap(),
        "one\nTWO\nthree\n"
    );
    let tool = &out.parts[1];
    assert_eq!(tool["tool"], "edit");
    assert_eq!(tool["state"]["status"], "completed");
    assert_eq!(tool["state"]["title"], "note.txt");
    assert_eq!(tool["state"]["output"], "Edit applied successfully.");
    assert!(tool["state"]["metadata"]["diff"]
        .as_str()
        .unwrap()
        .contains("+TWO"));

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items[1].parts[1]["tool"], "edit");
    assert_eq!(page.items[1].parts[1]["state"]["status"], "completed");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_write_edit_unsafe_path_persists_errors() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "unsafe mutate" })],
            provider: Some(json!({
                "fakeToolCalls": [
                    {
                        "tool": "write",
                        "input": { "filePath": "../secret.txt", "content": "secret" }
                    },
                    {
                        "tool": "edit",
                        "input": {
                            "filePath": "../secret.txt",
                            "oldString": "secret",
                            "newString": "public"
                        }
                    }
                ]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.parts[1]["tool"], "write");
    assert_eq!(out.parts[1]["state"]["status"], "error");
    assert!(out.parts[1]["state"]["error"]
        .as_str()
        .unwrap()
        .contains("Unsafe path"));
    assert_eq!(out.parts[2]["tool"], "edit");
    assert_eq!(out.parts[2]["state"]["status"], "error");
    assert!(out.parts[2]["state"]["error"]
        .as_str()
        .unwrap()
        .contains("Unsafe path"));
    assert!(!root.join("secret.txt").exists());

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items[1].parts[1]["state"]["status"], "error");
    assert_eq!(page.items[1].parts[2]["state"]["status"], "error");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_edit_multiple_match_without_replace_all_errors() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("dupe.txt"), "same\nsame\n").unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "edit dupe" })],
            provider: Some(json!({
                "fakeToolCalls": [{
                    "tool": "edit",
                    "input": {
                        "filePath": "dupe.txt",
                        "oldString": "same",
                        "newString": "changed"
                    }
                }]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(
        std::fs::read_to_string(repo.join("dupe.txt")).unwrap(),
        "same\nsame\n"
    );
    assert_eq!(out.parts[1]["tool"], "edit");
    assert_eq!(out.parts[1]["state"]["status"], "error");
    assert!(out.parts[1]["state"]["error"]
        .as_str()
        .unwrap()
        .contains("matched 2 times"));

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items[1].parts[1]["state"]["status"], "error");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_apply_patch_add_creates_file_and_metadata() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "apply add" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "apply_patch",
                        "input": { "patchText": "*** Begin Patch\n*** Add File: src/new.txt\n+hello\n+world\n*** End Patch" }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

    assert_eq!(
        std::fs::read_to_string(repo.join("src/new.txt")).unwrap(),
        "hello\nworld\n"
    );
    let tool = &out.parts[1];
    assert_eq!(tool["tool"], "apply_patch");
    assert_eq!(tool["state"]["status"], "completed");
    assert_eq!(tool["state"]["title"], "src/new.txt");
    assert_eq!(
        tool["state"]["output"],
        "Success. Updated the following files:\nA src/new.txt"
    );
    assert_eq!(tool["state"]["metadata"]["files"][0]["type"], "added");
    assert_eq!(tool["state"]["metadata"]["files"][0]["additions"], 2);
    assert!(tool["state"]["metadata"]["diff"]
        .as_str()
        .unwrap()
        .contains("+hello"));

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items[1].parts[1]["tool"], "apply_patch");
    assert_eq!(page.items[1].parts[1]["state"]["status"], "completed");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_apply_patch_update_changes_file_and_metadata() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("note.txt"), "one\ntwo\nthree\n").unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "apply update" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "apply_patch",
                        "input": { "patchText": "*** Begin Patch\n*** Update File: note.txt\n@@\n one\n-two\n+TWO\n three\n*** End Patch" }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

    assert_eq!(
        std::fs::read_to_string(repo.join("note.txt")).unwrap(),
        "one\nTWO\nthree\n"
    );
    let tool = &out.parts[1];
    assert_eq!(tool["tool"], "apply_patch");
    assert_eq!(tool["state"]["status"], "completed");
    assert_eq!(
        tool["state"]["output"],
        "Success. Updated the following files:\nM note.txt"
    );
    assert_eq!(tool["state"]["metadata"]["files"][0]["type"], "modified");
    assert_eq!(tool["state"]["metadata"]["files"][0]["additions"], 1);
    assert_eq!(tool["state"]["metadata"]["files"][0]["deletions"], 1);

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(
        page.items[1].parts[1]["state"]["metadata"]["files"][0]["type"],
        "modified"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_apply_patch_delete_removes_file_and_metadata() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("old.txt"), "old\nfile\n").unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "apply delete" })],
                provider: Some(json!({
                    "fakeToolCalls": [{
                        "tool": "apply_patch",
                        "input": { "patchText": "*** Begin Patch\n*** Delete File: old.txt\n*** End Patch" }
                    }]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

    assert!(!repo.join("old.txt").exists());
    let tool = &out.parts[1];
    assert_eq!(tool["tool"], "apply_patch");
    assert_eq!(tool["state"]["status"], "completed");
    assert_eq!(
        tool["state"]["output"],
        "Success. Updated the following files:\nD old.txt"
    );
    assert_eq!(tool["state"]["metadata"]["files"][0]["type"], "deleted");
    assert_eq!(tool["state"]["metadata"]["files"][0]["deletions"], 2);

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(
        page.items[1].parts[1]["state"]["metadata"]["files"][0]["type"],
        "deleted"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_apply_patch_mismatch_or_unsafe_persists_error() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("note.txt"), "one\ntwo\n").unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
            &state,
            &session.id,
            PromptInput {
                parts: vec![json!({ "type": "text", "text": "apply errors" })],
                provider: Some(json!({
                    "fakeToolCalls": [
                        {
                            "tool": "apply_patch",
                            "input": { "patchText": "*** Begin Patch\n*** Update File: note.txt\n missing\n-two\n+TWO\n*** End Patch" }
                        },
                        {
                            "tool": "apply_patch",
                            "input": { "patchText": "*** Begin Patch\n*** Add File: ../secret.txt\n+secret\n*** End Patch" }
                        }
                    ]
                })),
                ..Default::default()
            },
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .expect("prompt turn");

    assert_eq!(
        std::fs::read_to_string(repo.join("note.txt")).unwrap(),
        "one\ntwo\n"
    );
    assert!(!root.join("secret.txt").exists());
    assert_eq!(out.parts[1]["tool"], "apply_patch");
    assert_eq!(out.parts[1]["state"]["status"], "error");
    assert!(out.parts[1]["state"]["error"]
        .as_str()
        .unwrap()
        .contains("mismatch"));
    assert_eq!(out.parts[2]["tool"], "apply_patch");
    assert_eq!(out.parts[2]["state"]["status"], "error");
    assert!(out.parts[2]["state"]["error"]
        .as_str()
        .unwrap()
        .contains("Unsafe path"));

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items[1].parts[1]["state"]["status"], "error");
    assert_eq!(page.items[1].parts[2]["state"]["status"], "error");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_bash_persists_completed_tool_part() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "run command" })],
            provider: Some(json!({
                "fakeToolCalls": [{
                    "tool": "bash",
                    "input": {
                        "command": "echo hello",
                        "workdir": "src",
                        "description": "say hello"
                    }
                }]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.parts[0]["text"], "Completed 1 fake tool call(s): bash");
    let tool = &out.parts[1];
    assert_eq!(tool["type"], "tool");
    assert_eq!(tool["tool"], "bash");
    assert_eq!(tool["state"]["status"], "completed");
    assert_eq!(tool["state"]["input"]["command"], "echo hello");
    assert_eq!(tool["state"]["title"], "say hello");
    assert!(tool["state"]["output"].as_str().unwrap().contains("hello"));
    assert_eq!(tool["state"]["metadata"]["exit"], 0);
    assert_eq!(tool["state"]["metadata"]["description"], "say hello");
    assert_eq!(tool["state"]["metadata"]["truncated"], false);
    assert!(tool["state"]["metadata"]["output"]
        .as_str()
        .unwrap()
        .contains("hello"));

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items[1].parts[1]["tool"], "bash");
    assert_eq!(page.items[1].parts[1]["state"]["status"], "completed");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_bash_nonzero_persists_completed_metadata() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");
    // Bash semantics on every host. Per migration plan **Cross-shell tool
    // execution**: the bash tool resolves to WSL bash / Git Bash on Windows,
    // so the same bash-shaped command runs everywhere. The legacy
    // `cfg!(windows)` cmd.exe branch is gone now that Windows routes
    // through real bash.
    let command = "echo fail >&2; exit 7";

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "fail command" })],
            provider: Some(json!({
                "fakeToolCalls": [{
                    "tool": "bash",
                    "input": { "command": command }
                }]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    let tool = &out.parts[1];
    assert_eq!(tool["tool"], "bash");
    assert_eq!(tool["state"]["status"], "completed");
    assert_eq!(tool["state"]["metadata"]["exit"], 7);
    assert!(tool["state"]["output"].as_str().unwrap().contains("fail"));
    assert!(tool["state"]["metadata"]["output"]
        .as_str()
        .unwrap()
        .contains("fail"));

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items[1].parts[1]["state"]["status"], "completed");
    assert_eq!(page.items[1].parts[1]["state"]["metadata"]["exit"], 7);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_bash_invalid_input_persists_tool_errors() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "bad command" })],
            provider: Some(json!({
                "fakeToolCalls": [
                    {
                        "tool": "bash",
                        "input": { "command": "echo no", "workdir": "../secret" }
                    },
                    {
                        "tool": "bash",
                        "input": { "command": "echo no", "timeout": -1 }
                    }
                ]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    assert_eq!(out.parts[1]["tool"], "bash");
    assert_eq!(out.parts[1]["state"]["status"], "error");
    assert!(out.parts[1]["state"]["error"]
        .as_str()
        .unwrap()
        .contains("Unsafe workdir"));
    assert_eq!(out.parts[2]["tool"], "bash");
    assert_eq!(out.parts[2]["state"]["status"], "error");
    assert!(out.parts[2]["state"]["error"]
        .as_str()
        .unwrap()
        .contains("timeout must be greater than or equal to 0"));

    let page = state.store.messages(&session.id, None, None).unwrap();
    assert_eq!(page.items[1].parts[1]["state"]["status"], "error");
    assert_eq!(page.items[1].parts[2]["state"]["status"], "error");

    let _ = std::fs::remove_dir_all(root);
}
