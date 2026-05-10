//! Fake-provider tool-call tests covering `grep`, `read`, `write`,
//! `edit`, `apply_patch`, and `bash` tool execution paths.

use kilo_protocol::{PromptInput, SessionCreateInput};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::agent::tools::bash::fake_bash_with_cancel;
use crate::agent::tools::fs::{fake_edit, fake_glob, fake_grep, fake_read, fake_write};
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
async fn prompt_turn_fake_glob_persists_match_metadata() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(repo.join("src").join("nested")).unwrap();
    std::fs::write(repo.join("src").join("a.rs"), "fn a() {}\n").unwrap();
    std::fs::write(repo.join("src").join("nested").join("b.rs"), "fn b() {}\n").unwrap();
    std::fs::write(repo.join("src").join("nested").join("c.ts"), "export {}\n").unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "glob rust files" })],
            provider: Some(json!({
                "fakeToolCalls": [{
                    "tool": "glob",
                    "input": { "pattern": "src/**/*.rs" }
                }]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    let tool = &out.parts[1];
    assert_eq!(tool["tool"], "glob");
    assert_eq!(tool["state"]["status"], "completed");
    assert_eq!(tool["state"]["metadata"]["count"], 2);
    assert_eq!(tool["state"]["metadata"]["truncated"], false);
    let output = tool["state"]["output"].as_str().unwrap();
    assert!(output.contains("src/a.rs"), "{output}");
    assert!(output.contains("src/nested/b.rs"), "{output}");
    assert!(!output.contains("src/nested/c.ts"), "{output}");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fake_glob_supports_basename_patterns_and_braces() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(repo.join("README.md"), "# hi\n").unwrap();
    std::fs::write(repo.join("src").join("main.ts"), "main\n").unwrap();
    std::fs::write(repo.join("src").join("main.js"), "main\n").unwrap();
    std::fs::write(repo.join("src").join("main.rs"), "main\n").unwrap();

    let (_, output, meta) =
        fake_glob(&repo, &json!({ "pattern": "*.{ts,js}", "path": "src" })).expect("glob");

    assert_eq!(meta["count"], 2);
    assert!(output.contains("src/main.ts"), "{output}");
    assert!(output.contains("src/main.js"), "{output}");
    assert!(!output.contains("src/main.rs"), "{output}");

    let (_, output, meta) = fake_glob(&repo, &json!({ "pattern": "README.md" })).expect("glob");
    assert_eq!(meta["count"], 1);
    assert!(output.contains("README.md"), "{output}");

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

#[test]
fn fake_bash_stops_promptly_when_cancelled() {
    let root = unique_root();
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let flag = cancel.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        flag.store(true, Ordering::SeqCst);
    });

    let start = Instant::now();
    let out = fake_bash_with_cancel(
        &root.join("repo"),
        &json!({
            "command": "sleep 5",
            "timeout": 5000,
            "description": "sleep"
        }),
        Some(cancel.as_ref()),
    )
    .expect("bash should cancel cleanly");

    assert!(
        start.elapsed() < Duration::from_secs(2),
        "cancelled bash waited too long: {:?}",
        start.elapsed()
    );
    assert_eq!(out.2["cancelled"], true);
    assert_eq!(out.2["timeout"], false);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fake_bash_accepts_absolute_workdir_inside_repo() {
    let root = unique_root();
    let repo = root.join("repo");
    let src = repo.join("src");
    std::fs::create_dir_all(&src).unwrap();

    let out = fake_bash_with_cancel(
        &repo,
        &json!({
            "command": "echo ok",
            "workdir": src.to_string_lossy(),
            "description": "absolute workdir"
        }),
        None,
    )
    .expect("absolute workdir inside repo should be accepted");

    assert_eq!(out.2["exit"], 0);
    assert!(out.1.contains("ok"));

    #[cfg(windows)]
    {
        let slash = src
            .to_string_lossy()
            .replace('\\', "/")
            .to_ascii_lowercase();
        let out = fake_bash_with_cancel(
            &repo,
            &json!({
                "command": "echo ok",
                "workdir": slash,
                "description": "absolute slash workdir"
            }),
            None,
        )
        .expect("lowercase slash absolute workdir inside repo should be accepted");
        assert_eq!(out.2["exit"], 0);
    }

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fake_file_tools_accept_absolute_paths_inside_repo() {
    let root = unique_root();
    let repo = root.join("repo");
    let src = repo.join("src");
    let note = src.join("note.txt");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(&note, "hello needle\n").unwrap();

    let read = fake_read(&repo, &json!({ "filePath": note.to_string_lossy() }))
        .expect("read should accept an absolute path inside repo");
    assert!(read.1.contains("hello needle"));

    let grep = fake_grep(
        &repo,
        &json!({ "pattern": "needle", "path": src.to_string_lossy() }),
    )
    .expect("grep should accept an absolute path inside repo");
    assert_eq!(grep.2["matches"], 1);

    let write = repo.join("created.txt");
    fake_write(
        &repo,
        &json!({ "filePath": write.to_string_lossy(), "content": "new\n" }),
    )
    .expect("write should accept an absolute path inside repo");
    assert_eq!(std::fs::read_to_string(&write).unwrap(), "new\n");

    fake_edit(
        &repo,
        &json!({
            "filePath": write.to_string_lossy(),
            "oldString": "new",
            "newString": "changed"
        }),
    )
    .expect("edit should accept an absolute path inside repo");
    assert_eq!(std::fs::read_to_string(&write).unwrap(), "changed\n");

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

#[tokio::test]
async fn prompt_turn_fake_task_creates_child_session_and_completed_part() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "task": "allow" })),
            ..Default::default()
        })
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "run task" })],
            provider: Some(json!({
                "fakeToolCalls": [{
                    "tool": "task",
                    "input": {
                        "description": "bench child",
                        "prompt": "child done",
                        "subagent_type": "general"
                    }
                }]
            })),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .expect("prompt turn");

    let tool = &out.parts[1];
    assert_eq!(tool["type"], "tool");
    assert_eq!(tool["tool"], "task");
    assert_eq!(tool["state"]["status"], "completed");
    assert_eq!(tool["state"]["title"], "bench child");
    let output = tool["state"]["output"].as_str().unwrap();
    assert!(output.contains("<task_result>"), "{output}");
    assert!(output.contains("child done"), "{output}");
    let child = tool["state"]["metadata"]["sessionId"].as_str().unwrap();
    let children = state.store.children(&session.id).unwrap();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].id, child);

    let page = state.store.messages(child, None, None).unwrap();
    let body = serde_json::to_string(&page.items).unwrap();
    assert!(body.contains("Echo: child done"), "{body}");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn prompt_turn_fake_parallel_task_calls_create_children_in_input_order() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let session = state
        .store
        .create_session(SessionCreateInput {
            permission: Some(json!({ "task": "allow" })),
            ..Default::default()
        })
        .expect("create session");

    let out = prompt_turn(
        &state,
        &session.id,
        PromptInput {
            parts: vec![json!({ "type": "text", "text": "run tasks" })],
            provider: Some(json!({
                "fakeToolCalls": [
                    {
                        "tool": "task",
                        "delayMs": 25,
                        "input": {
                            "description": "bench child one",
                            "prompt": "child one done",
                            "subagent_type": "general"
                        }
                    },
                    {
                        "tool": "task",
                        "input": {
                            "description": "bench child two",
                            "prompt": "child two done",
                            "subagent_type": "general"
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

    assert_eq!(out.parts[1]["tool"], "task");
    assert_eq!(out.parts[1]["state"]["title"], "bench child one");
    assert_eq!(out.parts[2]["tool"], "task");
    assert_eq!(out.parts[2]["state"]["title"], "bench child two");
    assert!(out.parts[1]["state"]["output"]
        .as_str()
        .unwrap()
        .contains("child one done"));
    assert!(out.parts[2]["state"]["output"]
        .as_str()
        .unwrap()
        .contains("child two done"));
    let children = state.store.children(&session.id).unwrap();
    assert_eq!(children.len(), 2);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fake_edit_preserves_crlf_when_old_and_new_use_lf() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let path = repo.join("crlf.txt");
    std::fs::write(&path, b"alpha\r\nbeta\r\ngamma\r\n").unwrap();

    fake_edit(
        &repo,
        &json!({
            "filePath": path.to_string_lossy(),
            "oldString": "beta",
            "newString": "BETA"
        }),
    )
    .expect("edit");

    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(bytes, b"alpha\r\nBETA\r\ngamma\r\n");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fake_write_then_edit_preserves_utf16_le_bom() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let path = repo.join("u16.txt");

    let mut bytes = vec![0xff, 0xfe];
    for unit in "hello".encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    std::fs::write(&path, &bytes).unwrap();

    let read = fake_read(&repo, &json!({ "filePath": path.to_string_lossy() }))
        .expect("encoding-aware read");
    assert!(read.1.contains("hello"));

    fake_write(
        &repo,
        &json!({
            "filePath": path.to_string_lossy(),
            "content": "world"
        }),
    )
    .expect("encoding-preserving write");

    let after = std::fs::read(&path).unwrap();
    assert!(after.starts_with(&[0xff, 0xfe]), "BOM dropped on write");
    let mut expected = vec![0xff, 0xfe];
    for unit in "world".encode_utf16() {
        expected.extend_from_slice(&unit.to_le_bytes());
    }
    assert_eq!(after, expected);

    fake_edit(
        &repo,
        &json!({
            "filePath": path.to_string_lossy(),
            "oldString": "world",
            "newString": "earth"
        }),
    )
    .expect("encoding-preserving edit");
    let edited = std::fs::read(&path).unwrap();
    let mut expected_edit = vec![0xff, 0xfe];
    for unit in "earth".encode_utf16() {
        expected_edit.extend_from_slice(&unit.to_le_bytes());
    }
    assert_eq!(edited, expected_edit);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn fake_edit_indentation_skew_succeeds_via_replacer_chain() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let path = repo.join("ind.rs");
    std::fs::write(
        &path,
        "fn main() {\n        if x {\n            do_thing();\n        }\n}\n",
    )
    .unwrap();

    fake_edit(
        &repo,
        &json!({
            "filePath": path.to_string_lossy(),
            "oldString": "if x {\n    do_thing();\n}",
            "newString": "if x { done(); }"
        }),
    )
    .expect("edit through replacer chain");

    let after = std::fs::read_to_string(&path).unwrap();
    assert!(after.contains("if x { done(); }"));

    let _ = std::fs::remove_dir_all(root);
}

// ----------------------------------------------------------------
// External-directory permission gate (Bun parity:
// `packages/opencode/src/tool/external-directory.ts:25-56`).
// Exercises the full `fake_read_gated` flow including the
// `permission.asked` event and the structured deny error.
// ----------------------------------------------------------------

#[tokio::test]
async fn read_outside_worktree_asks_external_directory_permission() {
    use crate::agent::tools::fs::fake_read_gated;
    use crate::PermissionDecision;

    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let outside = root.join("secrets.txt");
    std::fs::write(&outside, "top secret\n").unwrap();

    let task_state = state.clone();
    let task_repo = repo.clone();
    let task_outside = outside.clone();
    let join = tokio::spawn(async move {
        fake_read_gated(
            &task_state,
            "sid-ext-1",
            "mid-ext-1",
            "pid-ext-1",
            0,
            &task_repo,
            &json!({ "filePath": task_outside.to_string_lossy() }),
            None,
        )
        .await
    });

    // Wait for the gate to publish the ask.
    let entry = loop {
        tokio::task::yield_now().await;
        let mut perms = state.permissions.lock().unwrap();
        if let Some(key) = perms.keys().next().cloned() {
            break perms.remove(&key).unwrap();
        }
    };
    assert_eq!(entry.info["permission"], "external_directory");
    assert_eq!(entry.info["metadata"]["kind"], "read");
    let patterns = entry.info["patterns"].as_array().unwrap();
    assert_eq!(patterns.len(), 1);
    let pattern = patterns[0].as_str().unwrap();
    assert!(
        pattern.contains("secrets.txt"),
        "ask pattern must point at the external file: {pattern}",
    );

    // Deny → tool returns a structured error mentioning the path.
    let _ = entry.reply.send(PermissionDecision::Reject);
    let err = join.await.unwrap().unwrap_err();
    assert!(
        err.contains("External directory access denied"),
        "expected denial message; got: {err}",
    );
    assert!(err.contains("secrets.txt"), "got: {err}");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn read_outside_worktree_with_approval_succeeds() {
    use crate::agent::tools::fs::fake_read_gated;
    use crate::PermissionDecision;

    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let outside = root.join("notes.txt");
    std::fs::write(&outside, "line one\nline two\n").unwrap();

    let task_state = state.clone();
    let task_repo = repo.clone();
    let task_outside = outside.clone();
    let join = tokio::spawn(async move {
        fake_read_gated(
            &task_state,
            "sid-ext-2",
            "mid-ext-2",
            "pid-ext-2",
            0,
            &task_repo,
            &json!({ "filePath": task_outside.to_string_lossy() }),
            None,
        )
        .await
    });

    let entry = loop {
        tokio::task::yield_now().await;
        let mut perms = state.permissions.lock().unwrap();
        if let Some(key) = perms.keys().next().cloned() {
            break perms.remove(&key).unwrap();
        }
    };
    let _ = entry.reply.send(PermissionDecision::Allow);
    let (_title, output, _meta) = join.await.unwrap().expect("approved external read");
    assert!(output.contains("line one"), "{output}");
    assert!(output.contains("line two"), "{output}");

    let _ = std::fs::remove_dir_all(root);
}
