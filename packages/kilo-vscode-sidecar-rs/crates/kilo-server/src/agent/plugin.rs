//! Plugin tool registry. The minimum surface needed to mirror Bun's
//! `plugin.tool()` decoration
//! ([`packages/opencode/src/plugin/index.ts`](../../../../../opencode/src/plugin/index.ts))
//! — extra tools that callers can register at startup time without
//! patching the built-in catalog in `agent::tools::defs`.
//!
//! The Rust port is in-process only (no dynamic loading): consumers
//! register their tools by calling [`AppState::register_plugin_tool`]
//! before `serve()` opens the listener. Each registered tool surfaces
//! to the model alongside the built-ins (`real_tools` merges them) and
//! is dispatched by [`real_tool_part`] when the model calls it.
//!
//! Permission gating: every plugin tool goes through `ask_permission`
//! with `permission = "plugin"` and pattern = the tool name. Users can
//! allow specific tools via `"plugin": { "<toolname>": "allow" }` or
//! deny everything with `"plugin": "deny"`.

use std::sync::Arc;

use kilo_provider::{ChatTool, ChatToolCall};
use serde_json::{json, Value};

use crate::AppState;

/// Synchronous handler signature. The handler receives the parsed tool
/// input and returns either a result payload or an error string. Async
/// work can be done via `tokio::task::block_in_place` + `Handle::block_on`
/// if the caller needs it; this signature stays simple.
pub type PluginToolFn = Arc<dyn Fn(&Value) -> Result<Value, String> + Send + Sync + 'static>;

/// One registered plugin tool. Consumers construct via
/// [`AppState::register_plugin_tool`].
#[derive(Clone)]
pub(crate) struct PluginTool {
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) parameters: Value,
    pub(crate) handler: PluginToolFn,
}

impl AppState {
    /// Register a plugin tool. Idempotent on `name`: a second call with
    /// the same name replaces the prior registration. Mirrors Bun's
    /// `plugin.tool()` decorator semantics — last writer wins.
    #[allow(dead_code)]
    pub fn register_plugin_tool<F>(
        &self,
        name: impl Into<String>,
        description: impl Into<String>,
        parameters: Value,
        handler: F,
    ) where
        F: Fn(&Value) -> Result<Value, String> + Send + Sync + 'static,
    {
        let tool = PluginTool {
            name: name.into(),
            description: description.into(),
            parameters,
            handler: Arc::new(handler),
        };
        let mut guard = self.plugin_tools.lock().unwrap();
        guard.retain(|existing| existing.name != tool.name);
        guard.push(tool);
    }

    /// Snapshot the registered plugin tools — used by `real_tools` to
    /// merge into the per-turn tool catalog. Returns an owned `Vec` so
    /// the caller doesn't have to hold the registry lock across an
    /// await.
    pub(crate) fn plugin_tool_snapshot(&self) -> Vec<PluginTool> {
        self.plugin_tools.lock().unwrap().clone()
    }

    /// Look up a registered plugin tool by name. Returns `None` if no
    /// plugin claims that name (caller should fall through to the
    /// next dispatch path).
    pub(crate) fn plugin_tool_lookup(&self, name: &str) -> Option<PluginTool> {
        self.plugin_tools
            .lock()
            .unwrap()
            .iter()
            .find(|tool| tool.name == name)
            .cloned()
    }
}

/// Convert plugin tools into [`ChatTool`] descriptors the model sees.
pub(crate) fn plugin_chat_tools(state: &AppState) -> Vec<ChatTool> {
    state
        .plugin_tool_snapshot()
        .into_iter()
        .map(|tool| ChatTool {
            name: tool.name,
            description: tool.description,
            parameters: tool.parameters,
        })
        .collect()
}

/// Run a plugin tool's handler. The dispatch is permission-gated at
/// the call site (`real_tool_part`); this function just runs the user
/// code and converts its return into a tool-result payload. Errors
/// from the handler become tool errors via the caller's normal
/// `tool_error` path.
pub(crate) fn invoke_plugin_tool(
    tool: &PluginTool,
    call: &ChatToolCall,
) -> Result<(String, String, Value), String> {
    let result = (tool.handler)(&call.input)?;
    let title = format!("plugin:{}", tool.name);
    let output = result_text(&result);
    let metadata = json!({ "result": result });
    Ok((title, output, metadata))
}

/// Plain-text view of the plugin handler's return value. Strings pass
/// through; everything else is JSON-serialized so the model sees
/// readable output.
fn result_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => serde_json::to_string_pretty(value).unwrap_or_default(),
    }
}
