//! Agent-loop MCP integration tests. Coverage:
//!
//! * `mcp_chat_tools` namespacing matches Bun's `{client}_{tool}` per
//!   `packages/opencode/src/mcp/index.ts:685`.
//! * `mcp_lookup` reverses the namespace by client-list lookup.
//! * Disabled / failed MCP servers contribute no tools to the catalog.
//! * `real_tools` (the per-turn tool catalog feeder) merges MCP entries
//!   alongside the built-ins and the structured-output tool.

use kilo_mcp::{Status, Tool};
use kilo_protocol::PromptInput;
use serde_json::json;

use crate::agent::mcp_dispatch::{mcp_chat_tools, mcp_lookup};
use crate::agent::parts::real_tools;
use crate::AppState;

use super::common::{state_at, unique_root};

fn install_fake_connected(state: &AppState, name: &str, tool_name: &str) {
    let tools = vec![Tool {
        name: tool_name.to_string(),
        description: Some("fake tool".to_string()),
        input_schema: json!({ "type": "object" }),
    }];
    state
        .mcp
        .lock()
        .unwrap()
        .insert(name.to_string(), Status::Connected { tools });
}

#[test]
fn mcp_chat_tools_returns_namespaced_entries_for_connected_servers() {
    let root = unique_root();
    let state = state_at(&root);
    install_fake_connected(&state, "context7", "resolve_library_id");
    install_fake_connected(&state, "playwright", "navigate");

    let tools = mcp_chat_tools(&state);
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
    assert!(
        names.contains(&"context7_resolve_library_id"),
        "missing namespaced context7 tool: {names:?}"
    );
    assert!(
        names.contains(&"playwright_navigate"),
        "missing namespaced playwright tool: {names:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn mcp_lookup_resolves_namespaced_name_to_descriptor() {
    let root = unique_root();
    let state = state_at(&root);
    install_fake_connected(&state, "context7", "resolve_library_id");

    let descriptor = mcp_lookup(&state, "context7_resolve_library_id")
        .expect("known namespaced tool should resolve");
    assert_eq!(descriptor.client, "context7");
    assert_eq!(descriptor.tool, "resolve_library_id");
    assert!(mcp_lookup(&state, "nonexistent_thing").is_none());

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn mcp_chat_tools_skips_disabled_and_failed_servers() {
    let root = unique_root();
    let state = state_at(&root);
    state.mcp.lock().unwrap().insert(
        "broken".to_string(),
        Status::Failed {
            error: "boom".to_string(),
        },
    );
    state
        .mcp
        .lock()
        .unwrap()
        .insert("off".to_string(), Status::Disabled);

    let tools = mcp_chat_tools(&state);
    assert!(
        tools.is_empty(),
        "got tools from disabled/failed servers: {tools:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn agent_hard_rules_returns_empty_for_unconstrained_modes() {
    let root = unique_root();
    let state = state_at(&root);
    // Non-constrained agents never get a hard veto layer, even when the
    // catalog ships default rules for them.
    assert!(state.agent_hard_rules("build").is_empty());
    assert!(state.agent_hard_rules("code").is_empty());
    assert!(state.agent_hard_rules("orchestrator").is_empty());

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn is_protected_request_gates_bash_redirection_and_mutators() {
    use crate::agent::permission::is_protected_request;
    let root = unique_root();
    let state = state_at(&root);

    // Shell redirection into a config dir downgrades always->once.
    let meta = json!({ "command": "echo hi > .kilo/x.json" });
    assert!(is_protected_request(&state, "bash", &[], &meta));
    // Mutating utility targeting a root-level config file.
    let meta = json!({ "command": "rm -rf kilo.json" });
    assert!(is_protected_request(&state, "bash", &[], &meta));
    // Unrelated bash command is not protected.
    let meta = json!({ "command": "echo hello world" });
    assert!(!is_protected_request(&state, "bash", &[], &meta));
    // Non-edit, non-bash permission keys remain unaffected.
    let meta = json!({ "command": "echo hi > .kilo/x.json" });
    assert!(!is_protected_request(&state, "read", &[], &meta));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn agent_hard_rules_emits_builtin_denies_for_ask_and_plan() {
    // Bun parity: with no user config, the constraint modes ship hard
    // deny rules for write/edit/apply_patch/bash (and plan_exit allow
    // for plan). Mirrors `kilocode/agent/index.ts:{askGuard,planGuard}`.
    let root = unique_root();
    let state = state_at(&root);

    let ask = state.agent_hard_rules("ask");
    for key in ["edit", "bash", "apply_patch", "write"] {
        assert!(
            ask.iter()
                .any(|r| r.permission == key && r.pattern == "*" && r.action == "deny"),
            "ask missing default {key}:* deny: {ask:?}"
        );
    }

    let plan = state.agent_hard_rules("plan");
    for key in ["edit", "bash", "apply_patch", "write"] {
        assert!(
            plan.iter()
                .any(|r| r.permission == key && r.pattern == "*" && r.action == "deny"),
            "plan missing default {key}:* deny: {plan:?}"
        );
    }
    assert!(
        plan.iter()
            .any(|r| r.permission == "plan_exit" && r.pattern == "*" && r.action == "allow"),
        "plan missing plan_exit:* allow: {plan:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn agent_hard_rules_returns_configured_rules_for_ask_agent() {
    use kilo_protocol::Config;
    use std::collections::BTreeMap;
    let root = unique_root();
    let state = state_at(&root);

    // Seed config with an `ask` agent that hard-denies edits.
    let mut data = BTreeMap::new();
    data.insert(
        "agent".to_string(),
        json!({
            "ask": {
                "permission": {
                    "edit": "deny",
                    "bash": { "rm *": "deny" }
                }
            }
        }),
    );
    state
        .store
        .set_config(Config { data })
        .expect("set_config failed");

    let rules = state.agent_hard_rules("ask");
    assert!(
        rules
            .iter()
            .any(|r| r.permission == "edit" && r.pattern == "*" && r.action == "deny"),
        "missing edit:* deny rule: {rules:?}"
    );
    assert!(
        rules
            .iter()
            .any(|r| r.permission == "bash" && r.pattern == "rm *" && r.action == "deny"),
        "missing bash:rm * deny rule: {rules:?}"
    );

    // Non-constrained agents return empty even when configured.
    assert!(state.agent_hard_rules("build").is_empty());

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn persisted_permission_rules_round_trip_through_disk() {
    let root = unique_root();
    let state = state_at(&root);

    // Persist via the store and confirm the file exists + parses back.
    let rules = vec![
        json!({ "permission": "edit", "pattern": "docs/*.md", "action": "allow" }),
        json!({ "permission": "bash", "pattern": "*", "action": "deny" }),
    ];
    state
        .store
        .append_permission_rules(&rules)
        .expect("append should succeed");

    let read = state.store.permission_rules();
    assert_eq!(read.len(), 2);
    assert_eq!(read[0]["permission"], "edit");
    assert_eq!(read[1]["action"], "deny");

    // Append more — order is preserved (later rule wins per `findLast`).
    state
        .store
        .append_permission_rules(&[json!({
            "permission": "bash", "pattern": "git *", "action": "allow"
        })])
        .expect("second append should succeed");
    let read = state.store.permission_rules();
    assert_eq!(read.len(), 3);
    assert_eq!(read[2]["pattern"], "git *");

    // Decode through the typed adapter.
    use crate::PermissionRule;
    let typed = PermissionRule::from_persisted(read);
    assert_eq!(typed.len(), 3);
    assert_eq!(typed[0].permission, "edit");
    assert_eq!(typed[2].action, "allow");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn real_tools_merges_built_ins_and_mcp_entries() {
    let root = unique_root();
    let state = state_at(&root);
    install_fake_connected(&state, "weather", "lookup");

    let input = PromptInput {
        tools: Some(json!(true)),
        model: Some(json!({
            "providerID": "openai",
            "modelID": "gpt-5.1-codex",
            "capabilities": { "toolcall": true }
        })),
        ..Default::default()
    };
    let names: Vec<String> = real_tools(&state, &input)
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert!(names.contains(&"read".to_string()));
    assert!(names.contains(&"grep".to_string()));
    assert!(names.contains(&"weather_lookup".to_string()));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn plugin_tool_registration_appears_in_real_tools_and_dispatches() {
    use crate::agent::parts::real_tool_part;
    use kilo_provider::ChatToolCall;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    let root = unique_root();
    let state = state_at(&root);

    // Register a counting plugin tool. The handler runs synchronously
    // and returns the squared input for verification.
    state.register_plugin_tool(
        "math_square",
        "Square a number.",
        json!({ "type": "object", "properties": { "n": { "type": "number" } }, "required": ["n"] }),
        |input: &serde_json::Value| {
            let n = input["n"]
                .as_f64()
                .ok_or_else(|| "n must be a number".to_string())?;
            Ok(json!({ "result": n * n }))
        },
    );

    // It surfaces in `real_tools` alongside the built-ins.
    let input = PromptInput {
        tools: Some(json!(true)),
        model: Some(json!({
            "providerID": "openai",
            "modelID": "gpt-5.1-codex",
            "capabilities": { "toolcall": true }
        })),
        // Pre-allow plugin permission so the test doesn't have to drive
        // a permission round-trip.
        agent: None,
        ..Default::default()
    };
    let names: Vec<String> = real_tools(&state, &input)
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert!(
        names.contains(&"math_square".to_string()),
        "plugin tool missing from catalog: {names:?}"
    );

    // Pre-allow plugin permission via the in-memory approvals so the
    // dispatch call doesn't block on an interactive prompt.
    state.approvals.lock().unwrap().push(crate::PermissionRule {
        permission: "plugin".to_string(),
        pattern: "math_square".to_string(),
        action: "allow".to_string(),
    });

    // Dispatch. The model's "tool name" is the plugin tool name.
    let call = ChatToolCall {
        id: "call_plugin_1".to_string(),
        name: "math_square".to_string(),
        input: json!({ "n": 7 }),
    };
    let part = real_tool_part(
        &state,
        std::path::Path::new(&state.store.paths().directory),
        "ses_plugin",
        "msg_plugin",
        "prt_plugin",
        0,
        &call,
        1,
        Arc::new(AtomicBool::new(false)),
        None,
    )
    .await;
    assert_eq!(part["type"], "tool");
    assert_eq!(part["tool"], "math_square");
    assert_eq!(part["state"]["status"], "completed");
    assert_eq!(part["state"]["metadata"]["result"]["result"], 49.0);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn question_round_trip_through_routes() {
    use crate::agent::permission::ask_question;
    use crate::routes::permissions::{question_list, reply_question};
    use axum::{
        extract::{Json as AJson, Path as APath, State as AState},
        http::StatusCode,
    };

    let root = unique_root();
    let state = state_at(&root);

    // Spawn an asker that awaits a reply.
    let asker_state = state.clone();
    let asker = tokio::spawn(async move {
        ask_question(
            &asker_state,
            json!({
                "id": "q_test_1",
                "sessionID": "ses_x",
                "text": "pick one",
                "options": ["a", "b"]
            }),
        )
        .await
    });

    // Wait until the question lands in state.questions.
    for _ in 0..50 {
        if !question_list(&state).is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let pending = question_list(&state);
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["id"], "q_test_1");

    // Reply via the HTTP route.
    let res = reply_question(
        AState(state.clone()),
        APath("q_test_1".to_string()),
        AJson(json!({ "answers": ["a"] })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    let answers = asker.await.unwrap().expect("asker should succeed");
    assert_eq!(answers, json!(["a"]));

    let _ = std::fs::remove_dir_all(root);
}

// ---------- read_resource (Wave 5 follow-up) ----------
//
// Mirrors Bun's `MCP.readResource(server, uri)` at
// `packages/opencode/src/mcp/index.ts:747-751`. The transport happy path
// is covered by the routes-level `mcp_remote` / `mcp_local` tests
// (`mcp_post_remote` / `mcp_call_child_cancel` are the same plumbing);
// here we lock down the configuration / status guards.

#[tokio::test]
async fn read_resource_errors_when_server_not_configured() {
    use crate::agent::mcp_dispatch::{read_resource, McpResourceError};
    let root = unique_root();
    let state = state_at(&root);
    let err = read_resource(&state, "ghost", "mcp://ghost/x").await.err();
    assert!(
        matches!(err, Some(McpResourceError::NotConfigured(_))),
        "missing server should surface NotConfigured"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn read_resource_errors_when_local_server_not_connected() {
    use crate::agent::mcp_dispatch::{read_resource, McpResourceError};
    let root = unique_root();
    let state = state_at(&root);
    state.mcp_configs.lock().unwrap().insert(
        "local-fake".to_string(),
        kilo_mcp::Config::Local {
            command: vec!["nonexistent-binary".to_string()],
            environment: None,
            cwd: None,
            enabled: Some(true),
            timeout: Some(500),
        },
    );
    let err = read_resource(&state, "local-fake", "mcp://local-fake/x")
        .await
        .err();
    assert!(
        matches!(err, Some(McpResourceError::NotConnected(_))),
        "configured-but-no-child should surface NotConnected"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn read_resource_errors_when_server_disabled() {
    use crate::agent::mcp_dispatch::{read_resource, McpResourceError};
    let root = unique_root();
    let state = state_at(&root);
    state.mcp_configs.lock().unwrap().insert(
        "off".to_string(),
        kilo_mcp::Config::Local {
            command: vec!["echo".to_string()],
            environment: None,
            cwd: None,
            enabled: Some(false),
            timeout: Some(500),
        },
    );
    let err = read_resource(&state, "off", "mcp://off/x").await.err();
    assert!(
        matches!(err, Some(McpResourceError::Disabled(_))),
        "disabled config must short-circuit before dialing"
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn read_resource_calls_resources_read_method_on_server() {
    // Drive the remote path against an in-process JSON-RPC fixture and
    // assert (a) the request advertises `method: "resources/read"` with
    // the URI in `params.uri`, and (b) the helper returns the
    // `result.contents[]` array verbatim.
    use crate::agent::mcp_dispatch::read_resource;
    use crate::tests::common::{
        mcp_add_remote_server, mcp_connect_test_server, mcp_json_response, mcp_remote_server,
    };
    use serde_json::json;

    let root = unique_root();
    let state = state_at(&root);

    // Connect handshake (initialize → tools/list) followed by the
    // resources/read response. The fixture echoes back a typed contents
    // payload so we can assert on it in the caller.
    let server = mcp_remote_server(vec![
        mcp_json_response(json!({ "jsonrpc": "2.0", "id": 1, "result": {} })),
        mcp_json_response(json!({
            "jsonrpc": "2.0", "id": 2,
            "result": { "tools": [] }
        })),
        mcp_json_response(json!({
            "jsonrpc": "2.0", "id": 7,
            "result": {
                "contents": [
                    { "uri": "mcp://docs/readme", "mimeType": "text/markdown", "text": "# hi" }
                ]
            }
        })),
    ])
    .await;

    mcp_add_remote_server(&state, "docs", &server.url, 1000).await;
    mcp_connect_test_server(&state, "docs").await;

    let contents = read_resource(&state, "docs", "mcp://docs/readme")
        .await
        .expect("read_resource should succeed");
    assert_eq!(contents.len(), 1, "contents passthrough: {contents:?}");
    assert_eq!(contents[0]["uri"], "mcp://docs/readme");
    assert_eq!(contents[0]["mimeType"], "text/markdown");
    assert_eq!(contents[0]["text"], "# hi");

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn real_tools_does_not_advertise_mcp_when_tools_are_disabled() {
    let root = unique_root();
    let state = state_at(&root);
    install_fake_connected(&state, "weather", "lookup");

    let input = PromptInput {
        tools: Some(json!(false)),
        model: Some(json!({
            "providerID": "openai",
            "modelID": "gpt-5.1-codex",
            "capabilities": { "toolcall": true }
        })),
        ..Default::default()
    };
    let names: Vec<String> = real_tools(&state, &input)
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert!(
        names.is_empty(),
        "explicit tools=false should suppress MCP tools too: {names:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}
