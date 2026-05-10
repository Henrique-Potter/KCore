//! Misc agent surface tests: real-tool-parts canonicalisation, tool
//! repair, OAuth soul/instructions composition, no-op condition, plus
//! PTY and skill/command route tests.

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use http_body_util::BodyExt;
use kilo_protocol::{
    MessageAppendInput, MessageAppendResult, PromptInput, SessionCreateInput, SessionUpdateInput,
};
use kilo_provider::{ChatOutput, ChatToolCall};
use serde_json::{json, Value};
use std::fs;
use tower::ServiceExt;

use crate::agent::openai_stream::{
    clean_title, is_default_session_title, prompt_instructions, prompt_instructions_with_root,
    should_generate_title, should_inject_noop, OPENAI_OAUTH_CODEX_RAW, OPENAI_OAUTH_SOUL_RAW,
};
use crate::agent::parts::{
    assistant_completed_info, assistant_parts, real_messages, real_tool_parts, task_child_tools,
    tool_completed, tool_running,
};
use crate::agent::shape::repair_tool_name;
use crate::agent::tools::defs::read_def;
use crate::http::build_router as app;
use crate::{Repair, KNOWN_TOOLS};

use super::common::{response_to_value, seed, state, state_at, unique_root};

#[tokio::test]
async fn pty_routes_404_unknown_id_and_succeed_for_real_lifecycle() {
    let st = state();

    // PUT /pty/<unknown> → 404 with named envelope.
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/pty/pty_unknown")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "input": "x" }).to_string()))
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let data: Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(data["name"], "RustPtyNotFoundError");

    // DELETE /pty/<unknown> → 404 with named envelope.
    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/pty/pty_unknown")
        .body(Body::empty())
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    // POST /pty → spawns a real shell, returns an id we can DELETE.
    // We pick a portable command that exits immediately so the test
    // doesn't hang waiting on a child: `cmd /c exit` on Windows,
    // `/bin/sh -c "exit"` elsewhere.
    let argv: Vec<&str> = if cfg!(windows) {
        vec!["cmd.exe", "/c", "exit"]
    } else {
        vec!["/bin/sh", "-c", "exit"]
    };
    let req = Request::builder()
        .method(Method::POST)
        .uri("/pty")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            json!({ "cwd": ".", "command": argv, "size": { "cols": 80, "rows": 24 } }).to_string(),
        ))
        .unwrap();
    let res = app(st.clone()).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data: Value =
        serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let id = data["id"].as_str().unwrap_or("").to_string();
    assert!(id.starts_with("pty_"), "unexpected pty id: {data:?}");

    // Allow the spawned shell a beat to exit before we DELETE.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let req = Request::builder()
        .method(Method::DELETE)
        .uri(format!("/pty/{id}"))
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn skill_and_command_routes_return_discovered_registry_data() {
    let root = unique_root();
    let repo = root.join("repo");
    fs::create_dir_all(repo.join(".kilo").join("skills").join("plan")).unwrap();
    fs::create_dir_all(repo.join(".kilo").join("skills").join("off")).unwrap();
    fs::create_dir_all(repo.join(".kilo").join("command")).unwrap();
    fs::write(
        repo.join(".kilo")
            .join("skills")
            .join("plan")
            .join("SKILL.md"),
        "---\nname: plan\ndescription: Plan carefully\n---\nUse first principles.\n",
    )
    .unwrap();
    fs::write(
        repo.join(".kilo")
            .join("skills")
            .join("off")
            .join("SKILL.md"),
        "---\nname: off\ndescription: Hidden\ndisabled: true\n---\nDo not load.\n",
    )
    .unwrap();
    fs::write(
        repo.join(".kilo").join("command").join("ship.md"),
        "---\ndescription: Ship it\nagent: build\nmodel: openai/gpt-5\nsubtask: true\n---\nShip $ARGUMENTS with $1.\n",
    )
    .unwrap();
    fs::write(
        repo.join(".kilo").join("command").join("review.md"),
        "---\nname: deep-review\ndescription: >-\n  Review with: context\ntemplate: |-\n  Check $1\n  Then $ARGUMENTS\n---\nignored body\n",
    )
    .unwrap();
    fs::write(
        repo.join(".kilo").join("command").join("off.md"),
        "---\ndescription: Hidden\ndisabled: true\n---\nDo not load.\n",
    )
    .unwrap();
    let state = state_at(&root);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/skill")
        .body(Body::empty())
        .unwrap();
    let res = app(state.clone()).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data[0]["name"], "plan");
    assert_eq!(data[0]["description"], "Plan carefully");
    assert!(data[0]["content"]
        .as_str()
        .unwrap()
        .contains("first principles"));

    let req = Request::builder()
        .method(Method::GET)
        .uri("/command")
        .body(Body::empty())
        .unwrap();
    let res = app(state).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert!(data
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["name"] == "ship" && item["source"] == "command"));
    assert!(data
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["name"] == "plan" && item["source"] == "skill"));
    let ship = data
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["name"] == "ship")
        .unwrap();
    assert_eq!(ship["agent"], "build");
    assert_eq!(ship["model"], "openai/gpt-5");
    assert_eq!(ship["subtask"], true);
    let review = data
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["name"] == "deep-review")
        .unwrap();
    assert_eq!(review["description"], "Review with: context");
    assert_eq!(review["template"], "Check $1\nThen $ARGUMENTS");
    assert!(review["hints"].as_array().unwrap().contains(&json!("$1")));
    assert!(review["hints"]
        .as_array()
        .unwrap()
        .contains(&json!("$ARGUMENTS")));
    assert!(!data
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["name"] == "off"));
}

#[test]
fn assistant_parts_places_followup_text_after_tools() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("note.txt"), "hello\n").unwrap();
    let out = ChatOutput {
        provider: "test".to_string(),
        model: "test-model".to_string(),
        text: "done".to_string(),
        tool_calls: vec![ChatToolCall {
            id: "call_read".to_string(),
            name: "read".to_string(),
            input: json!({ "filePath": "note.txt" }),
        }],
        usage: None,
        finish: Some("stop".to_string()),
    };

    let parts = assistant_parts(&repo, "ses_x", "msg_x", "prt_x", out, 1);

    assert_eq!(parts[0]["type"], "tool");
    assert_eq!(parts[0]["tool"], "read");
    assert_eq!(parts[1]["type"], "text");
    assert_eq!(parts[1]["text"], "done");
    assert_eq!(parts[2]["type"], "step-finish");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn tool_completed_truncates_large_outputs_at_common_boundary() {
    let part = tool_completed(
        "msg",
        "prt",
        0,
        "plugin",
        "call",
        &json!({}),
        "plugin".to_string(),
        "x".repeat(crate::limits::MAX_TOOL_OUTPUT_BYTES + 128),
        json!({}),
        1,
    );
    let output = part["state"]["output"].as_str().unwrap();

    assert!(output.ends_with(crate::limits::TRUNCATION_SENTINEL));
    assert!(
        output.len()
            <= crate::limits::MAX_TOOL_OUTPUT_BYTES + crate::limits::TRUNCATION_SENTINEL.len()
    );
}

#[test]
fn real_messages_injects_plan_mode_file_contract_for_plan_agent() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput {
            title: Some("Plan me".to_string()),
            ..Default::default()
        })
        .unwrap();
    state
        .store
        .append_message(
            &session.id,
            MessageAppendInput {
                info: json!({ "id": "msg_plan_user", "role": "user", "agent": "plan" }),
                parts: vec![json!({ "type": "text", "text": "draft the plan" })],
            },
        )
        .unwrap();

    let messages = real_messages(&state, &session.id, "draft the plan");
    let content = &messages.last().unwrap().content;

    assert!(content.contains("Plan mode is active"));
    assert!(content.contains(".kilo"));
    assert!(content.contains("plans"));
    assert!(content.contains("plan_exit"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn title_generation_helpers_match_bun_gates() {
    assert!(is_default_session_title(
        "New session - 2026-05-09T16:03:04.123Z"
    ));
    assert!(is_default_session_title(
        "Child session - 2026-05-09T16:03:04.123Z"
    ));
    assert!(!is_default_session_title("New session - not a date"));

    assert_eq!(
        clean_title("<think>draft</think>\n  Rust sidecar retry layer\nmore").as_deref(),
        Some("Rust sidecar retry layer")
    );
    assert_eq!(clean_title("<think>unterminated\nfallback"), None);
}

#[test]
fn should_generate_title_only_for_first_real_root_user_message() {
    let st = state();
    let session = st
        .store
        .create_session_record(SessionCreateInput {
            title: None,
            ..Default::default()
        })
        .unwrap()
        .session;

    assert!(!should_generate_title(&st, &session.id));

    st.store
        .append_message_record(
            &session.id,
            MessageAppendInput {
                info: json!({ "role": "user" }),
                parts: vec![json!({ "type": "text", "text": "debug auth refresh" })],
            },
        )
        .unwrap();
    assert!(should_generate_title(&st, &session.id));

    st.store
        .update_session(
            &session.id,
            SessionUpdateInput {
                title: Some("Manual title".to_string()),
                permission: None,
                time: None,
            },
        )
        .unwrap();
    assert!(!should_generate_title(&st, &session.id));

    let child = st
        .store
        .create_session_record(SessionCreateInput {
            parent_id: Some(session.id.clone()),
            title: None,
            ..Default::default()
        })
        .unwrap()
        .session;
    st.store
        .append_message_record(
            &child.id,
            MessageAppendInput {
                info: json!({ "role": "user" }),
                parts: vec![json!({ "type": "text", "text": "child task" })],
            },
        )
        .unwrap();
    assert!(!should_generate_title(&st, &child.id));
}

#[test]
fn assistant_completed_info_uses_completion_time_not_start_time() {
    let start = MessageAppendResult {
        info: json!({
            "role": "assistant",
            "time": {
                "created": 1,
                "updated": 1,
            }
        }),
        parts: Vec::new(),
        time: 1,
    };

    let info = assistant_completed_info(&start);

    assert_eq!(info["time"]["created"], 1);
    assert!(info["time"]["updated"].as_i64().unwrap_or_default() > 1);
    assert!(info["time"]["completed"].as_i64().unwrap_or_default() > 1);
}

/// M7 Fix 5: when the live OAuth path receives a tool call whose name
/// is uppercased (`READ`, `Read`), it must be lowercased and dispatched
/// to the canonical handler — not fall through to "Unsupported tool".
/// Mirrors Bun's `experimental_repairToolCall` at llm.ts:363-383.
#[tokio::test]
async fn real_tool_parts_lowercases_uppercase_model_tool_names() {
    let root = unique_root();
    std::fs::create_dir_all(root.join("repo")).unwrap();
    std::fs::write(root.join("repo").join("note.txt"), "needle\n").unwrap();
    let calls = vec![ChatToolCall {
        id: "call_1".to_string(),
        name: "READ".to_string(),
        input: json!({ "filePath": "note.txt", "limit": 1 }),
    }];
    let parts = real_tool_parts(&root.join("repo"), "msg_x", "prt_x", &calls, 1);
    assert_eq!(parts.len(), 1);
    assert_eq!(
        parts[0]["tool"], "read",
        "uppercase READ should be canonicalised to read"
    );
    assert_eq!(parts[0]["state"]["status"], "completed");
    assert!(parts[0]["state"]["output"]
        .as_str()
        .unwrap_or_default()
        .contains("needle"));
    let _ = std::fs::remove_dir_all(root);
}

/// M7 Fix 5 negative case: a model-invented tool name like `Bash`
/// must produce the Unknown-tool error envelope shape used by the
/// fake path's `"invalid"` arm (see fake_tool_part), not silently
/// dispatch to a stub.
#[tokio::test]
async fn real_tool_parts_rejects_unknown_tool_names_with_invalid_shape() {
    let root = unique_root();
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let calls = vec![ChatToolCall {
        id: "call_z".to_string(),
        name: "DoesNotExist".to_string(),
        input: json!({}),
    }];
    let parts = real_tool_parts(&root.join("repo"), "msg_z", "prt_z", &calls, 1);
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0]["state"]["status"], "error");
    let err = parts[0]["state"]["error"].as_str().unwrap_or_default();
    assert!(
        err.starts_with("Unknown tool:"),
        "expected Unknown tool error envelope, got: {err}"
    );
    // Original (un-canonical) name preserved on the part for debugging.
    assert_eq!(parts[0]["tool"], "DoesNotExist");
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn real_tool_parts_rejects_invalid_argument_repairs() {
    let root = unique_root();
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let calls = vec![ChatToolCall {
        id: "call_bad".to_string(),
        name: "invalid".to_string(),
        input: json!({
            "tool": "read",
            "error": "Invalid tool arguments JSON: EOF while parsing a value",
            "arguments": "{\"filePath\":"
        }),
    }];
    let parts = real_tool_parts(&root.join("repo"), "msg_bad", "prt_bad", &calls, 1);
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0]["tool"], "invalid");
    assert_eq!(parts[0]["callID"], "call_bad");
    assert_eq!(parts[0]["state"]["status"], "error");
    assert_eq!(parts[0]["state"]["input"]["tool"], "read");
    assert!(parts[0]["state"]["error"]
        .as_str()
        .unwrap()
        .contains("Invalid tool arguments JSON"));
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn real_messages_skips_unsettled_tool_parts() {
    let root = unique_root();
    let state = state_at(&root);
    seed(&state.store);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    let user = state
        .store
        .append_message_record(
            &session.id,
            MessageAppendInput {
                info: json!({ "role": "user" }),
                parts: vec![json!({ "type": "text", "text": "spawn agents" })],
            },
        )
        .unwrap()
        .result;
    state
        .store
        .append_message_record(
            &session.id,
            MessageAppendInput {
                info: json!({
                    "id": "msg_assistant",
                    "role": "assistant",
                    "parentID": user.info["id"].clone()
                }),
                parts: vec![tool_running(
                    "msg_assistant",
                    "prt_assistant",
                    0,
                    "task",
                    "call_missing_output",
                    &json!({
                        "description": "Map repo",
                        "prompt": "map repo",
                        "subagent_type": "explore"
                    }),
                    1,
                )],
            },
        )
        .unwrap();

    let messages = real_messages(&state, &session.id, "continue");
    assert!(
        messages.iter().all(|message| message.responses.is_empty()),
        "unsettled running tool calls must not replay as bare function_call items: {messages:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn real_messages_injects_editor_context_into_latest_user_message() {
    let root = unique_root();
    let state = state_at(&root);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    state
        .store
        .append_message_record(
            &session.id,
            MessageAppendInput {
                info: json!({
                    "role": "user",
                    "editorContext": {
                        "activeFile": "src/lib.rs",
                        "visibleFiles": ["src/lib.rs", "Cargo.toml"],
                        "openTabs": ["README.md"]
                    }
                }),
                parts: vec![json!({ "type": "text", "text": "continue" })],
            },
        )
        .unwrap();

    let messages = real_messages(&state, &session.id, "continue");
    let content = &messages.last().expect("last message").content;
    assert!(content.contains("<environment_details>"));
    assert!(content.contains("Active file: src/lib.rs"));
    assert!(content.contains("Visible files:"));
    assert!(content.contains("  Cargo.toml"));
    assert!(content.contains("Open tabs:"));
    assert!(content.contains("  README.md"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn real_messages_preserves_data_url_file_attachments() {
    let root = unique_root();
    let state = state_at(&root);
    let session = state
        .store
        .create_session(SessionCreateInput::default())
        .unwrap();
    state
        .store
        .append_message_record(
            &session.id,
            MessageAppendInput {
                info: json!({ "role": "user" }),
                parts: vec![
                    json!({ "type": "text", "text": "inspect image" }),
                    json!({
                        "type": "file",
                        "mime": "image/png",
                        "filename": "screen.png",
                        "url": "data:image/png;base64,aW1n"
                    }),
                    json!({
                        "type": "file",
                        "mime": "text/plain",
                        "filename": "note.txt",
                        "url": "data:text/plain;base64,aGVsbG8="
                    }),
                ],
            },
        )
        .unwrap();

    let messages = real_messages(&state, &session.id, "inspect image");
    let user = messages.last().expect("user message");
    assert_eq!(user.attachments.len(), 1);
    assert_eq!(user.attachments[0].mime, "image/png");
    assert_eq!(user.attachments[0].filename.as_deref(), Some("screen.png"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn tool_repair_matches_case_and_invalid_fallback() {
    assert_eq!(
        repair_tool_name("Read", KNOWN_TOOLS),
        Repair::Valid("read".to_string())
    );
    assert_eq!(
        repair_tool_name("does_not_exist", KNOWN_TOOLS),
        Repair::Invalid("does_not_exist".to_string())
    );
}

/// M7 Fix 3 guard: the OAuth instructions must carry the real soul
/// prompt embedded from `packages/opencode/src/kilocode/soul.txt`, not
/// a one-line stub. Threshold is intentionally well below the current
/// ~3.5KB file so it catches accidental re-stubbing without coupling
/// to exact wording.
#[test]
fn openai_oauth_soul_is_full_kilocode_prompt_not_stub() {
    let trimmed = OPENAI_OAUTH_SOUL_RAW.trim();
    assert!(
        trimmed.len() > 500,
        "OPENAI_OAUTH_SOUL_RAW is too short ({} bytes) — looks like a stub",
        trimmed.len()
    );
    // Sanity: the embedded soul leads with the canonical Kilo opener.
    assert!(
        trimmed.starts_with("You are Kilo"),
        "soul prompt does not start with the Kilo opener; got prefix {:?}",
        &trimmed[..trimmed.len().min(40)]
    );

    // The composed instructions must include the soul text plus any
    // user-supplied system string, joined with a newline (Bun parity).
    let input = PromptInput {
        system: Some(json!("user system text")),
        ..Default::default()
    };
    let composed = prompt_instructions(&input, None).expect("composed instructions");
    assert!(composed.contains("You are Kilo"));
    assert!(composed.contains("user system text"));
}

#[test]
fn openai_oauth_instructions_include_codex_prompt_and_env_block_when_dir_set() {
    // Bun composes `[soul, provider(model), env, ...input.system, ...]` for
    // OAuth chats (see `session/llm.ts:108-159` + `session/prompt.ts:1605-1611`).
    // The Rust port must surface (a) `codex.txt` in the provider slot and
    // (b) the `<env>` block with the working dir / model id / platform —
    // without those the Codex Responses API gets near-empty instructions
    // and tool-use degrades.
    let input = PromptInput {
        model: Some(json!({ "providerID": "openai", "modelID": "gpt-5.1-codex" })),
        ..Default::default()
    };
    let composed = prompt_instructions(&input, Some("/repo")).expect("composed");
    // Soul stays first.
    assert!(composed.starts_with("You are Kilo"));
    // Codex provider prompt is embedded — `## Editing constraints` is a
    // section header unique to `codex.txt`, not the soul.
    assert!(
        composed.contains("## Editing constraints"),
        "expected codex.txt body (looking for `## Editing constraints` heading)",
    );
    // Env block carries the per-request context the model expects.
    assert!(composed.contains("<env>"));
    assert!(composed.contains("</env>"));
    assert!(composed.contains("Working directory: /repo"));
    assert!(composed.contains("openai/gpt-5.1-codex"));
    assert!(composed.contains("Optional project config:"));
    assert!(composed.contains("Do not assume optional config files exist"));
    assert!(composed.contains("list/glob before reading them"));
    assert!(composed.contains("Today's date:"));
}

#[test]
fn openai_oauth_instructions_include_discovered_skill_guidance() {
    let root = unique_root();
    let repo = root.join("repo");
    fs::create_dir_all(repo.join(".kilo").join("skills").join("focus")).unwrap();
    fs::write(
        repo.join(".kilo")
            .join("skills")
            .join("focus")
            .join("SKILL.md"),
        "---\nname: focus\ndescription: Stay narrowly scoped\n---\nKeep changes small.\n",
    )
    .unwrap();
    let input = PromptInput::default();
    let composed = prompt_instructions_with_root(
        &input,
        Some(repo.to_str().unwrap()),
        Some(root.join("config").join("kilo").to_str().unwrap()),
        Some(root.to_str().unwrap()),
        None,
    )
    .expect("composed");
    assert!(composed.contains("Skills provide specialized instructions"));
    assert!(composed.contains("<available_skills>"));
    assert!(composed.contains("<name>focus</name>"));
    assert!(composed.contains("<description>Stay narrowly scoped</description>"));
}

#[test]
fn openai_oauth_instructions_use_agent_prompt_over_codex_prompt() {
    let input = PromptInput::default();
    let composed = prompt_instructions_with_root(
        &input,
        Some("/repo"),
        None,
        None,
        Some("Agent-specific instructions."),
    )
    .expect("composed instructions");

    assert!(composed.contains(OPENAI_OAUTH_SOUL_RAW.trim()));
    assert!(composed.contains("Agent-specific instructions."));
    assert!(!composed.contains(OPENAI_OAUTH_CODEX_RAW.trim()));
}

#[test]
fn openai_oauth_instructions_skip_env_block_when_dir_not_set() {
    // Pure-mode call — used by tests / introspection paths that don't have
    // a working directory available. Should still emit soul + codex + any
    // user system, just without the `<env>` block.
    let input = PromptInput::default();
    let composed = prompt_instructions(&input, None).expect("composed");
    assert!(composed.starts_with("You are Kilo"));
    assert!(!composed.contains("<env>"));
}

#[test]
fn openai_oauth_instructions_include_format_contract() {
    let input = PromptInput {
        system: Some(json!(["first system", "second system"])),
        format: Some(json!({
            "type": "json_schema",
            "name": "Answer",
            "schema": { "type": "object" }
        })),
        ..Default::default()
    };
    let composed = prompt_instructions(&input, None).expect("composed instructions");
    assert!(composed.starts_with("You are Kilo"));
    assert!(composed.contains("first system\nsecond system"));
    assert!(composed.contains("structured output"));
    assert!(composed.contains("StructuredOutput"));
}

#[test]
fn noop_condition_matches_litellm_copilot_tool_history_triple() {
    let msgs = vec![json!({ "parts": [{ "type": "tool", "tool": "read" }] })];
    assert!(should_inject_noop("openai", true, &[], &msgs));
    assert!(should_inject_noop("github-copilot-chat", false, &[], &msgs));
    assert!(!should_inject_noop("openai", false, &[], &msgs));
    assert!(!should_inject_noop(
        "github-copilot-chat",
        false,
        &[read_def()],
        &msgs
    ));
    assert!(!should_inject_noop("github-copilot-chat", false, &[], &[]));
}

/// Helper: write `kilo.json` with the supplied agent block at the
/// given test root. Mirrors `tests/agent_basics.rs:custom_agent_config`.
fn write_agent_config(root: &std::path::Path, body: Value) {
    let dir = root.join("config").join("kilo");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("kilo.json"), serde_json::to_string(&body).unwrap()).unwrap();
}

#[tokio::test]
async fn task_child_disables_task_tool_when_parent_agent_lacks_permission() {
    // Parent agent has no `task` allow rule → child tool map sets
    // `task: false` (Bun parity: `tool/task.ts:165`).
    let root = unique_root();
    write_agent_config(
        &root,
        json!({
            "agent": {
                "code": {
                    "permission": [
                        { "permission": "edit", "pattern": "*", "action": "allow" }
                    ]
                }
            }
        }),
    );
    let st = state_at(&root);
    seed(&st.store);
    let parent = st
        .store
        .create_session(SessionCreateInput::default())
        .expect("create parent");
    st.set_session_agent(&parent.id, Some("code"));

    let tools = task_child_tools(&st, &parent);
    assert_eq!(tools["task"], json!(false), "tools={tools}");
    // Static default for `todowrite` stays disabled too in this case.
    assert_eq!(tools["todowrite"], json!(false));

    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn task_child_disables_todowrite_when_parent_agent_lacks_permission() {
    // Parent agent allows `task` but not `todowrite` → child gets
    // `task: true`, `todowrite: false`.
    let root = unique_root();
    write_agent_config(
        &root,
        json!({
            "agent": {
                "code": {
                    "permission": [
                        { "permission": "task", "pattern": "*", "action": "allow" }
                    ]
                }
            }
        }),
    );
    let st = state_at(&root);
    seed(&st.store);
    let parent = st
        .store
        .create_session(SessionCreateInput::default())
        .expect("create parent");
    st.set_session_agent(&parent.id, Some("code"));

    let tools = task_child_tools(&st, &parent);
    assert_eq!(tools["task"], json!(true), "tools={tools}");
    assert_eq!(tools["todowrite"], json!(false));

    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn task_child_disables_primary_tools_from_experimental_config() {
    // Every tool in `experimental.primary_tools` is forced false in the
    // child map (Bun parity: `tool/task.ts:166`).
    let root = unique_root();
    write_agent_config(
        &root,
        json!({
            "agent": {
                "code": {
                    "permission": [
                        { "permission": "task", "pattern": "*", "action": "allow" },
                        { "permission": "todowrite", "pattern": "*", "action": "allow" }
                    ]
                }
            },
            "experimental": {
                "primary_tools": ["bash", "edit"]
            }
        }),
    );
    let st = state_at(&root);
    seed(&st.store);
    let parent = st
        .store
        .create_session(SessionCreateInput::default())
        .expect("create parent");
    st.set_session_agent(&parent.id, Some("code"));

    let tools = task_child_tools(&st, &parent);
    assert_eq!(tools["bash"], json!(false), "tools={tools}");
    assert_eq!(tools["edit"], json!(false));
    // `task` and `todowrite` still re-enabled because parent has
    // explicit allow rules and they are not in the primary_tools list.
    assert_eq!(tools["task"], json!(true));
    assert_eq!(tools["todowrite"], json!(true));
    // Unrelated tools are unaffected.
    assert_eq!(tools["read"], json!(true));

    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn task_child_keeps_task_when_parent_agent_explicitly_allows() {
    // Parent agent explicitly grants `task` allow → child map flips the
    // static default `task: false` back to `task: true`.
    let root = unique_root();
    write_agent_config(
        &root,
        json!({
            "agent": {
                "code": {
                    "permission": [
                        { "permission": "task", "pattern": "*", "action": "allow" },
                        { "permission": "todowrite", "pattern": "*", "action": "allow" }
                    ]
                }
            }
        }),
    );
    let st = state_at(&root);
    seed(&st.store);
    let parent = st
        .store
        .create_session(SessionCreateInput::default())
        .expect("create parent");
    st.set_session_agent(&parent.id, Some("code"));

    let tools = task_child_tools(&st, &parent);
    assert_eq!(tools["task"], json!(true), "tools={tools}");
    assert_eq!(tools["todowrite"], json!(true));

    let _ = fs::remove_dir_all(&root);
}
