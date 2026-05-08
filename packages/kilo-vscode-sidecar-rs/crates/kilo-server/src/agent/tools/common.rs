//! Shared helpers used across the fake-tool runtime.
//!
//! Step 7 of the kilo-server module split: these helpers were verbatim-cut
//! out of `lib.rs`. `tool_input` continues to live in `agent::parts` (Step 6
//! placed it there); use `crate::agent::parts::tool_input` to reach it.
//!
//! Visibility is `pub(crate)` for everything that crosses the module
//! boundary (`agent::fake`, `agent::tools::*`, and the still-in-`lib.rs`
//! permission machinery that consumes `tool_permission`/`tool_patterns`).

use std::path::Path as FsPath;

use serde_json::Value;

use crate::agent::tools::patch::parse_apply_patch;
use crate::util::paths::slash;
use crate::KNOWN_TOOLS;

pub(crate) fn tool_enabled(value: &Value) -> bool {
    match value {
        Value::String(name) => KNOWN_TOOLS.contains(&name.as_str()),
        Value::Bool(value) => *value,
        Value::Object(map) => map
            .get("disabled")
            .and_then(Value::as_bool)
            .map(|value| !value)
            .unwrap_or(true),
        _ => false,
    }
}

pub(crate) fn model_toolcall(model: Option<&Value>) -> bool {
    model
        .and_then(|value| value.get("capabilities"))
        .and_then(|value| value.get("toolcall"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

pub(crate) fn tool_usize(input: &Value, key: &str, default: usize) -> Result<usize, String> {
    let Some(value) = input.get(key) else {
        return Ok(default);
    };
    let Some(value) = value.as_u64() else {
        return Err(format!("{key} must be greater than or equal to 1"));
    };
    if value == 0 {
        return Err(format!("{key} must be greater than or equal to 1"));
    }
    Ok(value as usize)
}

pub(crate) fn title(root: &FsPath, path: &FsPath) -> String {
    let value = slash(path.strip_prefix(root).unwrap_or(path));
    if value.is_empty() {
        return ".".to_string();
    }
    value
}

pub(crate) fn tool_permission(tool: &str) -> &str {
    match tool {
        "write" | "edit" | "apply_patch" => "edit",
        value => value,
    }
}

pub(crate) fn tool_patterns(tool: &str, input: &Value) -> Vec<String> {
    match tool {
        "write" | "edit" => input
            .get("filePath")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| vec![value.to_string()])
            .unwrap_or_else(|| vec!["*".to_string()]),
        "apply_patch" => patch_patterns(input).unwrap_or_else(|| vec!["*".to_string()]),
        "task" => input
            .get("subagent_type")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| vec![value.to_string()])
            .unwrap_or_else(|| vec!["*".to_string()]),
        "bash" => vec!["*".to_string()],
        _ => vec!["*".to_string()],
    }
}

pub(crate) fn patch_patterns(input: &Value) -> Option<Vec<String>> {
    let text = input.get("patchText")?.as_str()?;
    let sections = parse_apply_patch(text).ok()?;
    Some(sections.into_iter().map(|item| item.path).collect())
}
