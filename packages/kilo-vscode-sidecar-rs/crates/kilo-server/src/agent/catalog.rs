use serde_json::{json, Map, Value};

use crate::AppState;

const PROMPT_ASK: &str = include_str!("../../../../../opencode/src/agent/prompt/ask.txt");
const PROMPT_COMPACTION: &str =
    include_str!("../../../../../opencode/src/agent/prompt/compaction.txt");
const PROMPT_DEBUG: &str = include_str!("../../../../../opencode/src/agent/prompt/debug.txt");
const PROMPT_EXPLORE: &str = include_str!("../../../../../opencode/src/agent/prompt/explore.txt");
const PROMPT_ORCHESTRATOR: &str =
    include_str!("../../../../../opencode/src/agent/prompt/orchestrator.txt");
const PROMPT_SUMMARY: &str = include_str!("../../../../../opencode/src/agent/prompt/summary.txt");
const PROMPT_TITLE: &str = include_str!("../../../../../opencode/src/agent/prompt/title.txt");

pub(crate) fn list(state: &AppState) -> Vec<Value> {
    let cfg = state.store.config();
    let default = cfg
        .data
        .get("default_agent")
        .and_then(Value::as_str)
        .map(resolve_key)
        .unwrap_or_else(|| "code".to_string());
    let mut agents = builtins();
    let configs = cfg
        .data
        .get("agent")
        .or_else(|| cfg.data.get("agents"))
        .and_then(Value::as_object);

    if let Some(configs) = configs {
        for (key, config) in configs {
            let name = resolve_key(key);
            if config.get("disable").and_then(Value::as_bool) == Some(true) {
                agents.remove(&name);
                continue;
            }
            let mut item = agents.remove(&name).unwrap_or_else(|| {
                json!({
                    "name": name,
                    "mode": "all",
                    "native": false,
                    "permission": [],
                    "options": {}
                })
            });
            merge_config(&mut item, config);
            agents.insert(
                item.get("name")
                    .and_then(Value::as_str)
                    .unwrap_or(&name)
                    .to_string(),
                item,
            );
        }
    }

    let mut out = agents
        .into_values()
        .map(project_for_sdk)
        .collect::<Vec<_>>();
    out.sort_by(|a, b| {
        let an = a.get("name").and_then(Value::as_str).unwrap_or_default();
        let bn = b.get("name").and_then(Value::as_str).unwrap_or_default();
        let ad = an == default;
        let bd = bn == default;
        bd.cmp(&ad).then_with(|| an.cmp(bn))
    });
    out
}

/// Translate the internal agent shape (with `native` + `permission` as
/// a rule list) into the SDK's wire shape:
/// - `builtIn` (boolean) — mirrors `native`. Internal `native` is kept
///   as a Rust extra so existing readers don't break, but the SDK type
///   reads `builtIn` (`packages/sdk/js/src/gen/types.gen.ts:1589`).
/// - `permission` (object map keyed by permission id) — translated from
///   the rule-list shape via Bun's `Permission.toConfig`
///   (`packages/opencode/src/permission/index.ts:524-545`).
/// - `tools` (object map keyed by tool id, value=boolean). Defaulted to
///   the built-in tool catalog all-true; if not determinable, emit `{}`
///   so the SDK type still narrows.
fn project_for_sdk(mut item: Value) -> Value {
    let Some(map) = item.as_object_mut() else {
        return item;
    };
    let native = map.get("native").and_then(Value::as_bool).unwrap_or(false);
    map.insert("builtIn".to_string(), Value::Bool(native));
    let perm = map.remove("permission").unwrap_or(Value::Null);
    map.insert("permission".to_string(), permission_map(&perm));
    if !map.contains_key("tools") {
        map.insert("tools".to_string(), default_tools_map());
    }
    item
}

/// Inverse of the rule-list shape used internally — produce the
/// `{permission_key: action | {pattern: action}}` object the SDK
/// `Agent.permission` field expects. Mirrors Bun's `toConfig` in
/// `permission/index.ts:524-545`. Scalar-only permissions (single
/// `*` rule) collapse to a string action; everything else nests by
/// pattern.
fn permission_map(value: &Value) -> Value {
    const SCALAR_ONLY: &[&str] = &[
        "todowrite",
        "todoread",
        "question",
        "webfetch",
        "websearch",
        "codesearch",
        "doom_loop",
    ];
    let mut out = Map::new();
    let rules = match value {
        Value::Array(items) => items.clone(),
        Value::Object(_) => return value.clone(),
        _ => return Value::Object(out),
    };
    for rule in &rules {
        let Some(perm) = rule.get("permission").and_then(Value::as_str) else {
            continue;
        };
        let Some(action) = rule.get("action").and_then(Value::as_str) else {
            continue;
        };
        let pattern = rule.get("pattern").and_then(Value::as_str).unwrap_or("*");
        if SCALAR_ONLY.contains(&perm) {
            if pattern == "*" {
                out.insert(perm.to_string(), Value::String(action.to_string()));
            }
            continue;
        }
        match out.get(perm).cloned() {
            None => {
                let mut nested = Map::new();
                nested.insert(pattern.to_string(), Value::String(action.to_string()));
                out.insert(perm.to_string(), Value::Object(nested));
            }
            Some(Value::String(existing)) => {
                let mut nested = Map::new();
                nested.insert("*".to_string(), Value::String(existing));
                nested.insert(pattern.to_string(), Value::String(action.to_string()));
                out.insert(perm.to_string(), Value::Object(nested));
            }
            Some(Value::Object(mut nested)) => {
                nested.insert(pattern.to_string(), Value::String(action.to_string()));
                out.insert(perm.to_string(), Value::Object(nested));
            }
            Some(_) => {}
        }
    }
    Value::Object(out)
}

/// Default tools-availability map: every built-in tool is enabled.
/// Mirrors Bun's `resolveTools` defaulting to all-on for unconfigured
/// agents. Listed explicitly (not derived from `KNOWN_TOOLS`) so the
/// SDK shape doesn't shift when the internal tool registry grows.
fn default_tools_map() -> Value {
    json!({
        "read": true,
        "glob": true,
        "grep": true,
        "webfetch": true,
        "todowrite": true,
        "skill": true,
        "suggest": true,
        "lsp": true,
        "write": true,
        "edit": true,
        "apply_patch": true,
        "bash": true,
        "task": true,
        "question": true,
        "plan_exit": false,
    })
}

pub(crate) fn get(state: &AppState, agent: &str) -> Option<Value> {
    let name = resolve_key(agent);
    list(state).into_iter().find(|item| {
        item.get("name")
            .and_then(Value::as_str)
            .map(|item| item == name)
            .unwrap_or(false)
    })
}

fn resolve_key(agent: &str) -> String {
    if agent == "build" {
        "code".to_string()
    } else {
        agent.to_string()
    }
}

fn builtins() -> Map<String, Value> {
    [
        (
            "code",
            json!({
                "name": "code",
                "description": "The default agent. Executes tools based on configured permissions.",
                "mode": "primary",
                "native": true,
                "permission": [],
                "options": {}
            }),
        ),
        (
            "plan",
            json!({
                "name": "plan",
                "description": "Plan mode. Can only edit plan files; all other filesystem mutations are denied.",
                "mode": "primary",
                "native": true,
                "permission": [],
                "options": {}
            }),
        ),
        (
            "general",
            json!({
                "name": "general",
                "description": "General-purpose agent for researching complex questions and executing multi-step tasks. Use this agent to execute multiple units of work in parallel.",
                "mode": "subagent",
                "native": true,
                "permission": [],
                "options": {}
            }),
        ),
        (
            "explore",
            json!({
                "name": "explore",
                "description": "Fast agent specialized for exploring codebases. Use this when you need to quickly find files by patterns, search code for keywords, or answer questions about the codebase.",
                "prompt": PROMPT_EXPLORE,
                "mode": "subagent",
                "native": true,
                "permission": [],
                "options": {}
            }),
        ),
        (
            "debug",
            json!({
                "name": "debug",
                "description": "Diagnose and fix software issues with systematic debugging methodology.",
                "prompt": PROMPT_DEBUG,
                "mode": "primary",
                "native": true,
                "permission": [],
                "options": {}
            }),
        ),
        (
            "orchestrator",
            json!({
                "name": "orchestrator",
                "description": "Coordinate complex tasks by delegating to specialized agents in parallel.",
                "prompt": PROMPT_ORCHESTRATOR,
                "mode": "primary",
                "native": true,
                "deprecated": true,
                "permission": [],
                "options": {}
            }),
        ),
        (
            "ask",
            json!({
                "name": "ask",
                "description": "Get answers and explanations without making changes to the codebase.",
                "prompt": PROMPT_ASK,
                "mode": "primary",
                "native": true,
                "permission": [],
                "options": {}
            }),
        ),
        (
            "compaction",
            json!({
                "name": "compaction",
                "mode": "primary",
                "native": true,
                "hidden": true,
                "prompt": PROMPT_COMPACTION,
                "permission": [],
                "options": {}
            }),
        ),
        (
            "summary",
            json!({
                "name": "summary",
                "mode": "primary",
                "native": true,
                "hidden": true,
                "prompt": PROMPT_SUMMARY,
                "permission": [],
                "options": {}
            }),
        ),
        (
            "title",
            json!({
                "name": "title",
                "mode": "primary",
                "native": true,
                "hidden": true,
                "temperature": 0.5,
                "prompt": PROMPT_TITLE,
                "permission": [],
                "options": {}
            }),
        ),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_string(), value))
    .collect()
}

fn merge_config(item: &mut Value, config: &Value) {
    let Some(map) = item.as_object_mut() else {
        return;
    };
    for key in [
        "description",
        "variant",
        "prompt",
        "temperature",
        "color",
        "hidden",
        "mode",
        "steps",
    ] {
        if let Some(value) = config.get(key) {
            map.insert(key.to_string(), value.clone());
        }
    }
    if let Some(value) = config.get("top_p").or_else(|| config.get("topP")) {
        map.insert("topP".to_string(), value.clone());
    }
    if let Some(value) = config.get("name") {
        map.insert("name".to_string(), value.clone());
    }
    if let Some(value) = config.get("model") {
        map.insert("model".to_string(), parse_model(value));
    }
    if let Some(value) = config.get("permission") {
        map.insert("permission".to_string(), permission_value(value));
    }
    if let Some(value) = config.get("options") {
        merge_options(map, value);
    }
}

fn parse_model(value: &Value) -> Value {
    if value.is_object() {
        return value.clone();
    }
    let Some(raw) = value.as_str() else {
        return value.clone();
    };
    let Some((provider, model)) = raw.split_once('/') else {
        return json!({ "modelID": raw, "providerID": "openai" });
    };
    json!({ "modelID": model, "providerID": provider })
}

fn merge_options(map: &mut Map<String, Value>, value: &Value) {
    let entry = map
        .entry("options".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let (Some(dst), Some(src)) = (entry.as_object_mut(), value.as_object()) else {
        return;
    };
    for (key, value) in src {
        dst.insert(key.clone(), value.clone());
    }
}

pub(crate) fn permission_value(value: &Value) -> Value {
    if value.is_array() {
        return value.clone();
    }
    Value::Array(permission_rules(value))
}

pub(crate) fn permission_rules(value: &Value) -> Vec<Value> {
    let Some(map) = value.as_object() else {
        return value.as_array().cloned().unwrap_or_default();
    };
    let mut out = Vec::new();
    for (permission, value) in map {
        if let Some(action) = value.as_str() {
            out.push(json!({
                "permission": permission,
                "pattern": "*",
                "action": action
            }));
            continue;
        }
        if let Some(items) = value.as_object() {
            for (pattern, action) in items {
                if let Some(action) = action.as_str() {
                    out.push(json!({
                        "permission": permission,
                        "pattern": pattern,
                        "action": action
                    }));
                }
            }
        }
    }
    out
}
