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

use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use kilo_protocol::GlobalEvent;
use serde_json::{json, Value};

use crate::agent::tools::common::{tool_patterns, tool_permission};
use crate::util::paths::{resolve_with_external, ResolveOutcome};
use crate::{AppState, PendingPermission, PermissionDecision, PermissionRule};

/// Bun-parity errors the per-tool `external_directory` gate can raise.
/// Tools convert these into a flat string for the existing tool-error
/// pipeline via [`PermissionError::to_display`]. Carrying the structured
/// shape lets callers (and future tests) inspect the rejected paths
/// without re-parsing a free-form message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PermissionError {
    /// User denied the `external_directory` permission ask. `paths` is
    /// the list of out-of-worktree absolute paths that were presented.
    ExternalDirectoryDenied { paths: Vec<String> },
}

impl PermissionError {
    /// Render as a flat string for the existing tool-error surface.
    /// Mentions the specific denied paths so the user understands why
    /// the tool refused to run.
    pub(crate) fn to_display(&self) -> String {
        match self {
            PermissionError::ExternalDirectoryDenied { paths } => {
                if paths.is_empty() {
                    "External directory access denied".to_string()
                } else {
                    format!("External directory access denied for: {}", paths.join(", "))
                }
            }
        }
    }
}

/// Bun-parity `external_directory` gate. Filter `candidates` to those
/// that resolve OUTSIDE `root`, then raise a single `permission.asked`
/// event covering the lot. Mirrors
/// [`packages/opencode/src/tool/external-directory.ts:25-56`](../../../../../opencode/src/tool/external-directory.ts).
///
/// `kind` is one of `"read" | "write" | "execute"` and rides through to
/// the metadata so the UI can show an appropriate prompt.
///
/// Skips the ask entirely when every candidate already lives under
/// `root` — that's the hot path and must not deadlock waiting for a UI
/// reply that no one would ever send.
pub(crate) async fn ask_external_directory(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    root: &FsPath,
    candidates: &[String],
    kind: &str,
) -> Result<(), PermissionError> {
    let mut externals: Vec<String> = Vec::new();
    for value in candidates {
        if value.is_empty() {
            continue;
        }
        if let ResolveOutcome::External(abs) = resolve_with_external(root, value) {
            externals.push(abs.to_string_lossy().into_owned());
        }
    }
    if externals.is_empty() {
        return Ok(());
    }

    // De-dupe identical paths; the model can repeat a path across
    // arguments (e.g. apply_patch source + move-to dest pointing at the
    // same external file) and we don't want to wallpaper the UI with
    // duplicate ask entries inside the same metadata blob.
    externals.sort();
    externals.dedup();

    let metadata = json!({
        "paths": externals.clone(),
        "kind": kind,
    });
    let info_input = metadata.clone();
    let id = format!("permission_{mid}_{pid}_{idx}_external");
    let (tx, rx) = tokio::sync::oneshot::channel();
    let info = json!({
        "id": id,
        "sessionID": sid,
        "status": "pending",
        "permission": "external_directory",
        "patterns": externals.clone(),
        "always": ["*"],
        "metadata": info_input,
        "tool": { "messageID": mid, "callID": "", "name": "external_directory" },
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
        Ok(PermissionDecision::Allow) | Ok(PermissionDecision::Always) => Ok(()),
        Ok(PermissionDecision::Reject) => {
            Err(PermissionError::ExternalDirectoryDenied { paths: externals })
        }
        Err(_) => Err(PermissionError::ExternalDirectoryDenied { paths: externals }),
    }
}

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
    let legacy = evaluate_permission_layered(namespaced, "*", &soft, &hard);
    if rule.action == "deny" || legacy.action == "deny" {
        return Err(format!("MCP permission denied for {namespaced}"));
    }
    if rule.action == "allow" || legacy.action == "allow" {
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
    let protected = is_protected_request(state, permission, &patterns, input);
    let mut metadata = input.clone();
    if protected {
        // UI hint: the webview reads this to hide the "Allow always" toggle.
        // Bun parity: `permission/index.ts:251-258` injects the same key.
        if let Some(map) = metadata.as_object_mut() {
            map.insert("disableAlways".to_string(), Value::Bool(true));
        } else {
            metadata = json!({ "disableAlways": true });
        }
    }
    let info = json!({
        "id": id,
        "sessionID": sid,
        "status": "pending",
        "permission": permission,
        "patterns": patterns,
        "always": patterns,
        "metadata": metadata,
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
            // Bun parity (`permission/index.ts:308`): a request flagged by
            // `ConfigProtection.isRequest` cannot persist an "always" rule.
            // Treat as `once` — the action is granted but no rule is saved.
            if protected {
                return Ok(());
            }
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

/// Bun parity: `kilocode/permission/config-paths.ts::ConfigProtection.isRequest`.
/// Decide whether a permission request targets protected config files /
/// dirs. Currently only gates `edit` (write/edit/apply_patch all collapse
/// onto this permission key). Returns `true` if any pattern OR any
/// metadata-derived path resolves to a protected location, in which case
/// the caller forces `always` → `once` and disables the "Allow always"
/// toggle in the UI.
pub(crate) fn is_protected_request(
    state: &AppState,
    permission: &str,
    patterns: &[String],
    metadata: &Value,
) -> bool {
    if permission != "edit" {
        return false;
    }
    let paths = state.store.paths();
    for pattern in patterns {
        if config_paths::is_protected(pattern, &paths) {
            return true;
        }
    }
    if let Some(fp) = metadata.get("filePath").and_then(Value::as_str) {
        if config_paths::is_protected(fp, &paths) {
            return true;
        }
    }
    if let Some(files) = metadata.get("files").and_then(Value::as_array) {
        for file in files {
            for key in ["filePath", "movePath"] {
                if let Some(value) = file.get(key).and_then(Value::as_str) {
                    if config_paths::is_protected(value, &paths) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// `is_protected_request` overload that consumes a pending entry's `info`
/// shape (as published in `permission.asked`). Used by the routes layer
/// (`saveAlwaysRules`, `allowEverything`, drain) to decide whether to
/// downgrade or skip an entry without re-deriving the permission/patterns.
pub(crate) fn is_protected_info(state: &AppState, info: &Value) -> bool {
    let permission = info
        .get("permission")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let patterns: Vec<String> = info
        .get("patterns")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let empty = Value::Object(Default::default());
    let metadata = info.get("metadata").unwrap_or(&empty);
    is_protected_request(state, permission, &patterns, metadata)
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
    permission_rules_for_session(state, sid)
}

/// Public alias of `permission_ruleset`. The drain pass in
/// `routes::permissions::permission_rules`/`allow_everything` consults the
/// merged soft ruleset (`state.approvals` + per-session rules) when
/// re-evaluating sibling pending entries.
pub(crate) fn permission_rules_for_session(state: &AppState, sid: &str) -> Vec<PermissionRule> {
    let mut rules = state.approvals.lock().unwrap().clone();
    if let Some(session) = state.store.session(sid) {
        rules.extend(parse_permission_rules(session.permission.as_ref()));
    }
    rules
}

fn parse_permission_rules(value: Option<&Value>) -> Vec<PermissionRule> {
    let Some(value) = value else {
        return Vec::new();
    };
    // Array form: `[{ permission, pattern, action }]` (Bun's normalized
    // shape, also produced by `allow_everything`). Object form:
    // `{ "edit": "allow", "bash": { "git push": "deny" } }` (legacy
    // global config shape). Both must round-trip cleanly.
    if let Some(items) = value.as_array() {
        let mut rules = Vec::new();
        for item in items {
            let Some(permission) = item.get("permission").and_then(Value::as_str) else {
                continue;
            };
            let Some(action) = item.get("action").and_then(Value::as_str) else {
                continue;
            };
            rules.push(PermissionRule {
                permission: permission.to_string(),
                pattern: item
                    .get("pattern")
                    .and_then(Value::as_str)
                    .unwrap_or("*")
                    .to_string(),
                action: action.to_string(),
            });
        }
        return rules;
    }
    let Some(map) = value.as_object() else {
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

/// Bun parity: `kilocode/permission/config-paths.ts`. Match a path
/// (absolute or relative) against the protected config locations. Used
/// by `is_protected_request` to gate the "Allow always" downgrade.
pub(crate) mod config_paths {
    use super::{FsPath, PathBuf};
    use kilo_protocol::KiloPath;
    use std::env;

    /// Workspace-relative directory prefixes that always count as config.
    const CONFIG_DIRS: &[&str] = &[".kilo/", ".kilocode/", ".opencode/"];
    /// Subdirectories of the config dirs that are NOT config files (Bun's
    /// `EXCLUDED_SUBDIRS`). Plan markdown sidecars live here.
    const EXCLUDED_SUBDIRS: &[&str] = &["plans/"];
    /// Root-level filenames that always count as config.
    const CONFIG_ROOT_FILES: &[&str] = &[
        "kilo.json",
        "kilo.jsonc",
        "kilocode.json",
        "opencode.json",
        "opencode.jsonc",
        "AGENTS.md",
        ".kilocoderules",
        ".kilocodeignore",
        ".kilocodemodes",
    ];

    fn normalize(value: &str) -> String {
        value.replace('\\', "/")
    }

    fn excluded(remainder: &str) -> bool {
        EXCLUDED_SUBDIRS
            .iter()
            .any(|sub| remainder.starts_with(sub))
    }

    /// Project-relative protection check. Mirrors Bun's
    /// `ConfigProtection.isRelative`.
    pub(crate) fn is_relative(pattern: &str) -> bool {
        let normalized = normalize(pattern);
        for dir in CONFIG_DIRS {
            let bare = &dir[..dir.len() - 1];
            if normalized == bare || normalized.ends_with(&format!("/{bare}")) {
                return true;
            }
            if normalized.starts_with(dir) {
                if excluded(&normalized[dir.len()..]) {
                    continue;
                }
                return true;
            }
            if let Some(idx) = normalized.find(&format!("/{dir}")) {
                let after = &normalized[idx + 1 + dir.len()..];
                if excluded(after) {
                    continue;
                }
                return true;
            }
        }
        let basename = normalized.rsplit('/').next().unwrap_or(&normalized);
        if normalized == basename {
            return CONFIG_ROOT_FILES.iter().any(|name| *name == normalized);
        }
        false
    }

    fn within(child: &FsPath, parent: &FsPath) -> bool {
        let child = normalize(&child.to_string_lossy());
        let parent = normalize(&parent.to_string_lossy());
        if parent.is_empty() {
            return false;
        }
        child == parent || child.starts_with(&format!("{parent}/"))
    }

    fn config_dirs(paths: &KiloPath) -> Vec<PathBuf> {
        let mut out = Vec::new();
        if !paths.config.is_empty() {
            out.push(PathBuf::from(&paths.config));
        }
        if let Ok(xdg) = env::var("XDG_CONFIG_HOME") {
            let xdg = xdg.trim();
            if !xdg.is_empty() {
                out.push(PathBuf::from(xdg).join("kilo"));
            }
        }
        // Legacy global dirs Bun calls out (`KilocodePaths.globalDirs`).
        let home = if paths.home.is_empty() {
            env::var("HOME")
                .or_else(|_| env::var("USERPROFILE"))
                .unwrap_or_default()
        } else {
            paths.home.clone()
        };
        if !home.is_empty() {
            out.push(PathBuf::from(&home).join(".kilo"));
            out.push(PathBuf::from(&home).join(".kilocode"));
        }
        out
    }

    /// Absolute-path protection check. Mirrors Bun's
    /// `ConfigProtection.isAbsolute`.
    pub(crate) fn is_absolute(filepath: &str, paths: &KiloPath) -> bool {
        let target = PathBuf::from(filepath);
        for dir in config_dirs(paths) {
            if within(&target, &dir) {
                return true;
            }
        }
        false
    }

    /// Combined entry point. Picks `is_absolute` for absolute paths,
    /// `is_relative` otherwise. Returns `true` if the path is protected.
    pub(crate) fn is_protected(value: &str, paths: &KiloPath) -> bool {
        let p = FsPath::new(value);
        if p.is_absolute() {
            return is_absolute(value, paths);
        }
        is_relative(value)
    }
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

    #[test]
    fn config_paths_relative_recognises_kilo_dirs_and_root_files() {
        use super::config_paths::is_relative;
        assert!(is_relative(".kilo/agents/x.json"));
        assert!(is_relative(".kilocode/config.json"));
        assert!(is_relative(".opencode/foo"));
        assert!(is_relative("packages/sub/.kilo/foo"));
        assert!(is_relative("AGENTS.md"));
        assert!(is_relative("kilo.json"));
        assert!(is_relative(".kilocoderules"));
        assert!(!is_relative(".kilo/plans/draft.md"));
        assert!(!is_relative("src/main.rs"));
        assert!(!is_relative("docs/README.md"));
    }

    // ----------------------------------------------------------------
    // External-directory gate tests. Built on a minimal in-memory
    // AppState — we only need `permissions` + `bus` + the fields the
    // SSE publisher reaches for. Reusing `tests/common::state_at` would
    // require either widening its visibility or pulling in a full Store,
    // both of which are noisier than just constructing the bag here.
    // ----------------------------------------------------------------

    fn ext_test_state() -> Arc<AppState> {
        let (bus, _) = tokio::sync::broadcast::channel(64);
        Arc::new(AppState {
            username: "kilo".to_string(),
            password: None,
            store: kilo_store::Store::new(),
            bus,
            viewed: tokio::sync::RwLock::default(),
            runners: std::sync::Mutex::default(),
            runner_notify: tokio::sync::Notify::new(),
            prompt_queues: std::sync::Mutex::default(),
            prompt_queue_versions: std::sync::Mutex::default(),
            permissions: std::sync::Mutex::default(),
            approvals: std::sync::Mutex::default(),
            questions: std::sync::Mutex::default(),
            suggestions: std::sync::Mutex::default(),
            network: std::sync::Mutex::default(),
            mcp: std::sync::Mutex::default(),
            mcp_configs: std::sync::Mutex::default(),
            mcp_children: std::sync::Mutex::default(),
            pty: std::sync::Mutex::default(),
            plugin_tools: std::sync::Mutex::default(),
            session_agents: std::sync::Mutex::default(),
            session_hard_rules: std::sync::Mutex::default(),
            broken_turn_anchors: std::sync::Mutex::default(),
            oauth_pending: std::sync::Mutex::default(),
            oauth_listener: std::sync::Mutex::default(),
            oauth_listener_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            oauth_token_endpoint: format!("{}/oauth/token", crate::oauth::OPENAI_ISSUER),
            sse_capacity: AppState::new_sse_capacity(),
        })
    }

    fn unique_root(label: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let p = std::env::temp_dir().join(format!("kilo-extdir-{label}-{stamp}-{n}"));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[tokio::test]
    async fn external_directory_paths_inside_worktree_pass_without_ask() {
        let root = unique_root("inside");
        let worktree = root.join("repo");
        std::fs::create_dir_all(&worktree).unwrap();
        let state = ext_test_state();
        // Inside-only candidates must short-circuit before any UI ask is
        // queued. Empty-string candidates are also ignored.
        let candidates = vec![
            "src/main.rs".to_string(),
            "".to_string(),
            worktree.join("nested.rs").to_string_lossy().into_owned(),
        ];
        let res = ask_external_directory(
            &state,
            "sid",
            "mid",
            "pid",
            0,
            &worktree,
            &candidates,
            "read",
        )
        .await;
        assert!(
            res.is_ok(),
            "inside-only candidates must not block: {res:?}"
        );
        assert!(
            state.permissions.lock().unwrap().is_empty(),
            "no PendingPermission should be queued for inside-only candidates",
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn external_directory_paths_outside_raise_permission_ask() {
        let root = unique_root("outside");
        let worktree = root.join("repo");
        std::fs::create_dir_all(&worktree).unwrap();
        let outside = root.join("outside.txt").to_string_lossy().into_owned();
        let state = ext_test_state();
        let task_state = state.clone();
        let task_outside = outside.clone();
        let task_worktree = worktree.clone();
        let join = tokio::spawn(async move {
            ask_external_directory(
                &task_state,
                "sid",
                "mid",
                "pid",
                7,
                &task_worktree,
                &[task_outside],
                "read",
            )
            .await
        });

        // Spin briefly until the gate registers its PendingPermission.
        let key = loop {
            tokio::task::yield_now().await;
            let key = state.permissions.lock().unwrap().keys().next().cloned();
            if let Some(key) = key {
                break key;
            }
        };
        assert_eq!(key, "permission_mid_pid_7_external");
        let entry = state.permissions.lock().unwrap().remove(&key).unwrap();
        assert_eq!(entry.info["permission"], "external_directory");
        assert_eq!(entry.info["sessionID"], "sid");
        assert_eq!(entry.info["metadata"]["kind"], "read");
        let patterns = entry.info["patterns"].as_array().unwrap();
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].as_str().unwrap(), &outside);
        // Resolve the gate so the spawned task can finish.
        let _ = entry.reply.send(PermissionDecision::Allow);
        let res = join.await.unwrap();
        assert!(res.is_ok(), "approval must let the gate return Ok: {res:?}");
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn external_directory_denied_returns_structured_error() {
        let root = unique_root("denied");
        let worktree = root.join("repo");
        std::fs::create_dir_all(&worktree).unwrap();
        let outside_a = root.join("a.txt").to_string_lossy().into_owned();
        let outside_b = root.join("b.txt").to_string_lossy().into_owned();
        let state = ext_test_state();
        let task_state = state.clone();
        let task_worktree = worktree.clone();
        let candidates = vec![outside_a.clone(), outside_b.clone()];
        let join = tokio::spawn(async move {
            ask_external_directory(
                &task_state,
                "sid",
                "mid",
                "pid",
                3,
                &task_worktree,
                &candidates,
                "write",
            )
            .await
        });
        let entry = loop {
            tokio::task::yield_now().await;
            let mut perms = state.permissions.lock().unwrap();
            if let Some((k, _)) = perms.iter().next() {
                let k = k.clone();
                let v = perms.remove(&k).unwrap();
                break v;
            }
        };
        let _ = entry.reply.send(PermissionDecision::Reject);
        let err = join.await.unwrap().unwrap_err();
        let PermissionError::ExternalDirectoryDenied { paths } = err;
        let mut sorted = paths.clone();
        sorted.sort();
        let mut expected = vec![outside_a.clone(), outside_b.clone()];
        expected.sort();
        assert_eq!(sorted, expected);
        let display = PermissionError::ExternalDirectoryDenied { paths }.to_display();
        assert!(display.contains(&outside_a), "{display}");
        assert!(display.contains(&outside_b), "{display}");
        let _ = std::fs::remove_dir_all(root);
    }
}
