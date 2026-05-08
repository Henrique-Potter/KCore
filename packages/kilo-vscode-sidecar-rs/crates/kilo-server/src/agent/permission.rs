//! Permission machinery: ruleset evaluation, the per-call ask/await
//! handshake, and the wildcard matcher.
//!
//! Step 8 of the kilo-server module split: `ask_permission`,
//! `ask_permission_once`, `permission_decision`, `permission_ruleset`,
//! `parse_permission_rules`, `evaluate_permission`, and `wildcard_match`
//! moved here verbatim from `lib.rs`. The fake-tool runtime
//! (`agent::tools::common::tool_permission`/`tool_patterns`) and the
//! routes layer (`routes::permissions::reply_permission`) import the
//! resulting helpers via their typed paths.

use std::sync::Arc;

use kilo_protocol::GlobalEvent;
use serde_json::{json, Value};

use crate::agent::tools::common::{tool_patterns, tool_permission};
use crate::{AppState, PendingPermission, PermissionDecision, PermissionRule};

pub(crate) async fn ask_permission(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &str,
    input: &Value,
) -> Result<(), String> {
    let permission = tool_permission(tool);
    let patterns = tool_patterns(tool, input);
    let soft = permission_ruleset(state, sid);
    let hard = hard_ruleset(state, sid);
    for pattern in &patterns {
        let rule = evaluate_permission_layered(permission, pattern, &soft, &hard);
        if rule.action == "deny" {
            return Err(format!("Permission denied for {permission}: {pattern}"));
        }
        if rule.action != "allow" {
            return ask_permission_once(
                state, sid, mid, pid, idx, permission, tool, call, input, patterns,
            )
            .await;
        }
    }
    Ok(())
}

/// Ask the user a question and await their reply. Mirrors Bun's
/// `Question` flow: the consumer gets back the typed answer payload (or
/// an error if rejected/dropped). Used by tools that need user input
/// before continuing.
///
/// `info` is the question shape as published to SSE consumers. Required
/// fields per Bun's contract:
/// - `id` — unique question id; reused as the route key
/// - `sessionID` — session this question belongs to
/// - `text` — prompt body
/// - `options` — optional array of choice strings
///
/// The HTTP routes `reply_question` / `reject_question`
/// (`routes::permissions`) drive the resolution; this helper just
/// awaits whatever the route sends back.
pub(crate) async fn ask_question(state: &Arc<AppState>, info: Value) -> Result<Value, String> {
    let id = info
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "question info is missing `id`".to_string())?
        .to_string();
    let (tx, rx) = tokio::sync::oneshot::channel();
    state.questions.lock().unwrap().insert(
        id.clone(),
        crate::PendingQuestion {
            info: info.clone(),
            reply: tx,
        },
    );
    crate::http::sse::publish(state, GlobalEvent::bus("question.asked", info));
    match rx.await {
        Ok(crate::QuestionReply::Answers(answers)) => Ok(answers),
        Ok(crate::QuestionReply::Rejected) => Err("question rejected by user".to_string()),
        Err(_) => Err(format!("question {id} was dropped without a reply")),
    }
}

/// Hard permission rules for the session — agent-derived veto layer.
/// Empty when no `ask`/`plan` agent is active. Matches Bun's
/// `kilocode/session/prompt.ts:60-72` policy.
pub(crate) fn hard_ruleset(state: &AppState, sid: &str) -> Vec<PermissionRule> {
    state.session_hard_rules(sid)
}

/// MCP tool permission gate. Bun gates every MCP tool dispatch with
/// `permission.ask({ permission: key, patterns: ["*"], always: ["*"] })`
/// at `prompt.ts:477`. We use `permission = "mcp"` and pattern = the
/// namespaced tool name (`{client}_{tool}`), so a user rule like
/// `"mcp": { "context7_resolve_library_id": "allow" }` works as expected
/// and a blanket `"mcp": "deny"` blocks every MCP server.
pub(crate) async fn ask_mcp_permission(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    namespaced: &str,
    call: &str,
    input: &Value,
) -> Result<(), String> {
    let soft = permission_ruleset(state, sid);
    let hard = hard_ruleset(state, sid);
    let rule = evaluate_permission_layered("mcp", namespaced, &soft, &hard);
    if rule.action == "deny" {
        return Err(format!("MCP permission denied for {namespaced}"));
    }
    if rule.action == "allow" {
        return Ok(());
    }
    ask_permission_once(
        state,
        sid,
        mid,
        pid,
        idx,
        "mcp",
        namespaced,
        call,
        input,
        vec![namespaced.to_string()],
    )
    .await
}

/// Plugin tool permission gate. Bun parity: `plugin.tool()`-registered
/// tools go through `permission.ask({ permission: "plugin", patterns:
/// [<name>] })`. Users can allow specific tools with rules like
/// `"plugin": { "weather_lookup": "allow" }` or block everything via
/// `"plugin": "deny"`.
pub(crate) async fn ask_plugin_permission(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &str,
    input: &Value,
) -> Result<(), String> {
    let soft = permission_ruleset(state, sid);
    let hard = hard_ruleset(state, sid);
    let rule = evaluate_permission_layered("plugin", tool, &soft, &hard);
    if rule.action == "deny" {
        return Err(format!("Plugin permission denied for {tool}"));
    }
    if rule.action == "allow" {
        return Ok(());
    }
    ask_permission_once(
        state,
        sid,
        mid,
        pid,
        idx,
        "plugin",
        tool,
        call,
        input,
        vec![tool.to_string()],
    )
    .await
}

/// Doom-loop guard. Mirrors Bun's `processor.ts:357-380`: when the model
/// has emitted `DOOM_LOOP_THRESHOLD` consecutive identical tool calls
/// (same tool name, same input JSON), pause the loop and surface a
/// `permission.asked` event with `permission: "doom_loop"`. The user can
/// allow continuation, deny, or set a sticky rule. Unlike `ask_permission`
/// this does not collapse the tool name onto a permission key — the
/// permission *is* `"doom_loop"` and the pattern is the tool name itself,
/// so a user rule `"doom_loop": { "bash": "allow" }` lets the loop keep
/// running for that specific tool only.
pub(crate) async fn ask_doom_loop(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    tool: &str,
    call: &str,
    input: &Value,
) -> Result<(), String> {
    let soft = permission_ruleset(state, sid);
    let hard = hard_ruleset(state, sid);
    let rule = evaluate_permission_layered("doom_loop", tool, &soft, &hard);
    if rule.action == "deny" {
        return Err(format!("Doom-loop denied for {tool}"));
    }
    if rule.action == "allow" {
        return Ok(());
    }
    ask_permission_once(
        state,
        sid,
        mid,
        pid,
        idx,
        "doom_loop",
        tool,
        call,
        input,
        vec![tool.to_string()],
    )
    .await
}

async fn ask_permission_once(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    permission: &str,
    tool: &str,
    call: &str,
    input: &Value,
    patterns: Vec<String>,
) -> Result<(), String> {
    let id = format!("permission_{mid}_{pid}_{idx}");
    let (tx, rx) = tokio::sync::oneshot::channel();
    let info = json!({
        "id": id,
        "sessionID": sid,
        "status": "pending",
        "permission": permission,
        "patterns": patterns,
        "always": patterns,
        "metadata": input,
        "tool": { "messageID": mid, "callID": call, "name": tool },
    });
    state.permissions.lock().unwrap().insert(
        id.clone(),
        PendingPermission {
            info: info.clone(),
            reply: tx,
        },
    );
    crate::http::sse::publish(state, GlobalEvent::bus("permission.asked", info));
    match rx.await {
        Ok(PermissionDecision::Allow) => Ok(()),
        Ok(PermissionDecision::Always) => {
            let mut new_rules = Vec::with_capacity(patterns.len());
            for pattern in patterns {
                new_rules.push(PermissionRule {
                    permission: permission.to_string(),
                    pattern,
                    action: "allow".to_string(),
                });
            }
            // Persist BEFORE in-memory update so a crash mid-Always
            // can't leave the live ruleset richer than disk.
            let serialized: Vec<Value> = new_rules
                .iter()
                .filter_map(|r| serde_json::to_value(r).ok())
                .collect();
            if let Err(err) = state.store.append_permission_rules(&serialized) {
                eprintln!(
                    "[kilo-server] failed to persist permission rules: {err}; in-memory only"
                );
            }
            state.approvals.lock().unwrap().extend(new_rules);
            Ok(())
        }
        Ok(PermissionDecision::Reject) => Err(format!("Permission rejected for {permission}")),
        Err(_) => Err(format!("Permission request was dropped for {permission}")),
    }
}

pub(crate) fn permission_decision(input: &Value) -> Option<PermissionDecision> {
    match input.get("reply").and_then(Value::as_str)? {
        "once" | "allow" => Some(PermissionDecision::Allow),
        "always" => Some(PermissionDecision::Always),
        "reject" | "deny" => Some(PermissionDecision::Reject),
        _ => None,
    }
}

fn permission_ruleset(state: &AppState, sid: &str) -> Vec<PermissionRule> {
    let mut rules = state.approvals.lock().unwrap().clone();
    if let Some(session) = state.store.session(sid) {
        rules.extend(parse_permission_rules(session.permission.as_ref()));
    }
    rules
}

fn parse_permission_rules(value: Option<&Value>) -> Vec<PermissionRule> {
    let Some(Value::Object(map)) = value else {
        return Vec::new();
    };
    let mut rules = Vec::new();
    for (permission, value) in map {
        if let Some(action) = value.as_str() {
            rules.push(PermissionRule {
                permission: permission.clone(),
                pattern: "*".to_string(),
                action: action.to_string(),
            });
            continue;
        }
        if let Some(items) = value.as_object() {
            for (pattern, action) in items {
                if let Some(action) = action.as_str() {
                    rules.push(PermissionRule {
                        permission: permission.clone(),
                        pattern: pattern.clone(),
                        action: action.to_string(),
                    });
                }
            }
        }
    }
    rules
}

pub(crate) fn evaluate_permission(
    permission: &str,
    pattern: &str,
    rules: &[PermissionRule],
) -> PermissionRule {
    rules
        .iter()
        .rev()
        .find(|rule| {
            wildcard_match(permission, &rule.permission) && wildcard_match(pattern, &rule.pattern)
        })
        .cloned()
        .unwrap_or_else(|| PermissionRule {
            permission: permission.to_string(),
            pattern: "*".to_string(),
            action: "ask".to_string(),
        })
}

/// Layered evaluation. Bun (`permission/index.ts:217-271`) consults a
/// `hardRuleset` BEFORE the soft layers and lets it `deny` regardless of
/// any saved/session/local "always" approval. The hard layer can also
/// `allow` to short-circuit prompts. Anything else (`ask`/no-match) falls
/// through to the soft `evaluate_permission` against the merged rules.
pub(crate) fn evaluate_permission_layered(
    permission: &str,
    pattern: &str,
    soft: &[PermissionRule],
    hard: &[PermissionRule],
) -> PermissionRule {
    // Hard veto/grant: take the LAST matching hard rule (Bun's `findLast`
    // semantics — later rules win). A hard `deny` is unbeatable; a hard
    // `allow` skips the prompt; a hard `ask` is treated as no-match so
    // the soft layer still gets a chance to say allow/deny.
    if let Some(hit) = hard.iter().rev().find(|rule| {
        wildcard_match(permission, &rule.permission) && wildcard_match(pattern, &rule.pattern)
    }) {
        if hit.action == "deny" || hit.action == "allow" {
            return hit.clone();
        }
    }
    evaluate_permission(permission, pattern, soft)
}

/// Glob matcher. Supports `*` (any sequence) and `?` (single char) anywhere
/// in the pattern. Two-pointer linear-time backtrack. Replaces the prior
/// prefix/suffix-only matcher to fit Bun's `Wildcard.match`
/// semantics — needed for rules like `bash:rm *` and `mcp:server_*`.
fn wildcard_match(value: &str, pattern: &str) -> bool {
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let mut vi = 0usize;
    let mut pi = 0usize;
    let mut star_p: Option<usize> = None;
    let mut star_v: Option<usize> = None;
    while vi < v.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == v[vi]) {
            vi += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star_p = Some(pi);
            star_v = Some(vi);
            pi += 1;
        } else if let (Some(sp), Some(sv)) = (star_p, star_v) {
            // Backtrack: extend the previous '*' to absorb one more char.
            pi = sp + 1;
            star_v = Some(sv + 1);
            vi = sv + 1;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(permission: &str, pattern: &str, action: &str) -> PermissionRule {
        PermissionRule {
            permission: permission.to_string(),
            pattern: pattern.to_string(),
            action: action.to_string(),
        }
    }

    #[test]
    fn wildcard_match_supports_full_glob() {
        assert!(wildcard_match("anything", "*"));
        assert!(wildcard_match("rm -rf /", "rm *"));
        assert!(wildcard_match("git status", "git *"));
        assert!(wildcard_match("foo_bar_baz", "*_bar_*"));
        assert!(wildcard_match("abc", "a?c"));
        assert!(wildcard_match("README.md", "*.md"));
        assert!(!wildcard_match("rm -rf /", "ls *"));
        assert!(!wildcard_match("ab", "a?c"));
        assert!(wildcard_match("", "*"));
        assert!(wildcard_match("", ""));
        assert!(!wildcard_match("x", ""));
    }

    #[test]
    fn evaluate_permission_layered_hard_deny_beats_soft_allow() {
        let soft = vec![rule("bash", "*", "allow")];
        let hard = vec![rule("bash", "rm *", "deny")];
        assert_eq!(
            evaluate_permission_layered("bash", "rm -rf /", &soft, &hard).action,
            "deny"
        );
        // Non-matching hard rule lets soft win.
        assert_eq!(
            evaluate_permission_layered("bash", "ls -la", &soft, &hard).action,
            "allow"
        );
    }

    #[test]
    fn evaluate_permission_layered_hard_allow_short_circuits() {
        let soft = vec![rule("edit", "*", "deny")];
        let hard = vec![rule("edit", "docs/*", "allow")];
        assert_eq!(
            evaluate_permission_layered("edit", "docs/readme.md", &soft, &hard).action,
            "allow"
        );
        assert_eq!(
            evaluate_permission_layered("edit", "src/main.rs", &soft, &hard).action,
            "deny"
        );
    }

    #[test]
    fn evaluate_permission_layered_hard_ask_falls_through() {
        // A hard "ask" is not a final answer; it lets the soft layer
        // decide. Bun's hardPermissions only adds deny/allow rules, but
        // we still defend the path.
        let soft = vec![rule("mcp", "*", "allow")];
        let hard = vec![rule("mcp", "*", "ask")];
        assert_eq!(
            evaluate_permission_layered("mcp", "x_y", &soft, &hard).action,
            "allow"
        );
    }

    #[test]
    fn evaluate_permission_default_is_ask() {
        let rules: Vec<PermissionRule> = vec![];
        assert_eq!(evaluate_permission("bash", "ls", &rules).action, "ask");
    }
}
