//! MCP tool dispatch from inside the agent loop.
//!
//! Bridges the in-process MCP registry (connected clients + their tool
//! catalogs, exposed over HTTP via [`crate::routes::mcp`]) into the agent
//! turn loop:
//!
//! * [`mcp_chat_tools`] flattens every connected client's tool list into
//!   namespaced [`ChatTool`] descriptors using Bun's `{client}_{tool}`
//!   convention (`packages/opencode/src/mcp/index.ts:685`). The model
//!   sees one flat tool catalog; the namespace is reversible because we
//!   look up clients explicitly, never by string-split.
//! * [`mcp_lookup`] reverse-resolves a namespaced tool name back to the
//!   `(client, original_tool_name)` pair, matching against the live
//!   connected catalog. Returns `None` for unknown tools so callers can
//!   fall through to other dispatch paths.
//! * [`mcp_invoke`] runs the tool by routing to the appropriate
//!   `mcp_call_child` (local stdio) or `mcp_call_remote` (HTTP/SSE)
//!   helper. Permission gating is the caller's responsibility — see
//!   `agent::parts::mcp_tool_part`.

use std::{sync::atomic::AtomicBool, time::Duration};

use kilo_provider::ChatTool;
use serde_json::{json, Value};

use crate::routes::mcp::{mcp_call_remote, mcp_config, MCP_DEFAULT_TIMEOUT_MS};
use crate::AppState;

/// Namespaced view of an MCP tool: the public name the model sees, the
/// raw client name, and the original tool name as the upstream server
/// reported it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct McpToolDescriptor {
    pub(crate) namespaced: String,
    pub(crate) client: String,
    pub(crate) tool: String,
    pub(crate) input_schema: Value,
    pub(crate) description: Option<String>,
}

/// Enumerate every `Status::Connected` MCP server's tool list and return
/// flat [`McpToolDescriptor`]s. Disabled / failed / pending servers
/// contribute nothing.
pub(crate) fn mcp_descriptors(state: &AppState) -> Vec<McpToolDescriptor> {
    let snapshot = state.mcp.lock().unwrap().clone();
    let mut out = Vec::new();
    for (client, status) in snapshot {
        if let kilo_mcp::Status::Connected { tools } = status {
            for tool in tools {
                out.push(McpToolDescriptor {
                    namespaced: format!("{client}_{tool_name}", tool_name = tool.name),
                    client: client.clone(),
                    tool: tool.name,
                    input_schema: tool.input_schema,
                    description: tool.description,
                });
            }
        }
    }
    out
}

/// Convert connected MCP tools into [`ChatTool`] entries the model can
/// see. Empty `Vec` if no MCP servers are connected.
pub(crate) fn mcp_chat_tools(state: &AppState) -> Vec<ChatTool> {
    mcp_descriptors(state)
        .into_iter()
        .map(|d| ChatTool {
            name: d.namespaced,
            description: d.description.unwrap_or_default(),
            parameters: if d.input_schema.is_null() {
                json!({ "type": "object", "additionalProperties": true })
            } else {
                d.input_schema
            },
        })
        .collect()
}

/// Reverse-resolve a namespaced MCP tool name. Returns `None` when the
/// name doesn't belong to any connected MCP client.
pub(crate) fn mcp_lookup(state: &AppState, name: &str) -> Option<McpToolDescriptor> {
    mcp_descriptors(state)
        .into_iter()
        .find(|d| d.namespaced == name)
}

/// Outcome of an MCP invocation. The successful payload is the JSON-RPC
/// `result` field as returned by the server; the failure variant is a
/// human-readable string.
pub(crate) enum McpInvokeResult {
    Ok(Value),
    Err(String),
}

/// Invoke an MCP tool synchronously by descriptor. Routes to the local
/// stdio or remote HTTP transport depending on the server's config.
pub(crate) async fn mcp_invoke(
    state: &AppState,
    descriptor: &McpToolDescriptor,
    arguments: Value,
    cancel: &AtomicBool,
) -> McpInvokeResult {
    let cfg = match mcp_config(state, &descriptor.client) {
        Some(cfg) => cfg,
        None => {
            return McpInvokeResult::Err(format!(
                "MCP server {} is not configured",
                descriptor.client
            ))
        }
    };
    if !kilo_mcp::enabled(&cfg) {
        return McpInvokeResult::Err(format!("MCP server {} is disabled", descriptor.client));
    }
    let input = kilo_mcp::CallInput {
        name: descriptor.tool.clone(),
        arguments,
    };
    match cfg {
        kilo_mcp::Config::Local { timeout, .. } => {
            // M1 hardening gap (deferred): the global `mcp_children`
            // Mutex is held across the synchronous `mcp_call_child_cancel`
            // poll loop, blocking concurrent MCP status / refresh / second
            // tool calls until this one settles. The cancel race inside
            // `mcp_wait_response_cancel` still wins promptly, so Stop is
            // honored — but unrelated MCP traffic is gated. Fixing this
            // safely means lifting per-server children behind their own
            // `Arc<Mutex<McpChild>>` so the BTreeMap lock releases before
            // the poll. ~13 access sites in routes/mcp.rs need the same
            // change; punted to a follow-up to avoid a half-done refactor.
            let timeout = Duration::from_millis(timeout.unwrap_or(MCP_DEFAULT_TIMEOUT_MS).max(1));
            let mut children = state.mcp_children.lock().unwrap();
            let Some(child) = children.get_mut(&descriptor.client) else {
                return McpInvokeResult::Err(format!(
                    "MCP server {} is not connected",
                    descriptor.client
                ));
            };
            match crate::routes::mcp::mcp_call_child_cancel(child, input, timeout, Some(cancel)) {
                Ok(value) => McpInvokeResult::Ok(value),
                Err(err) => McpInvokeResult::Err(err.message().to_string()),
            }
        }
        kilo_mcp::Config::Remote {
            url,
            headers,
            timeout,
            ..
        } => {
            // Remote dispatch needs request headers (potentially OAuth-resolved). For
            // the agent-loop slice we use the static configured headers only —
            // OAuth refresh on demand is route-only for now.
            tokio::select! {
                res = mcp_call_remote(&url, headers.as_ref(), input, timeout) => {
                    match res {
                        Ok(value) => McpInvokeResult::Ok(value),
                        Err(err) => McpInvokeResult::Err(err.message().to_string()),
                    }
                }
                _ = wait_cancel(cancel) => McpInvokeResult::Err("MCP tool call aborted".to_string()),
            }
        }
    }
}

async fn wait_cancel(cancel: &AtomicBool) {
    while !crate::agent::is_canceled(cancel) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
