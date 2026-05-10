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
//! * [`read_resource`] resolves an MCP resource URI by issuing a
//!   `resources/read` JSON-RPC against the named server (Bun parity:
//!   `packages/opencode/src/mcp/index.ts:747` `MCP.readResource`). Used
//!   by `agent::parts::resolve_user_multimodal` to inline `mcp://`
//!   resource parts on user messages.

use std::{collections::BTreeMap, sync::atomic::AtomicBool, time::Duration};

use kilo_provider::ChatTool;
use serde_json::{json, Value};

use crate::routes::mcp::{
    mcp_alloc_id, mcp_call_remote, mcp_config, mcp_post_remote, mcp_remote_request_headers,
    mcp_response_error, mcp_wait_response_cancel, mcp_write_message, MCP_DEFAULT_TIMEOUT_MS,
};
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

/// Per-MCP-spec `resources/read` returns a `result.contents` array whose
/// entries each carry `{uri, mimeType, text?, blob?}`. We keep the raw
/// JSON values so the consumer (`agent::parts::resolve_user_multimodal`)
/// can pattern-match on `text` (inline string) vs `blob` (base64) without
/// committing to a specific Rust shape — matching how `mcp_invoke`
/// returns the raw JSON-RPC `result` payload.
pub(crate) type ResourceContents = Vec<Value>;

/// Failure modes for [`read_resource`]. Mirrors the local/remote split
/// in [`mcp_invoke`] so callers can tell config / connection / RPC
/// errors apart and emit a useful placeholder. The `String` payload
/// carries the upstream message for diagnostic surfaces (logs / future
/// route handlers); the in-tree caller in `agent::parts` only branches
/// on the variant.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum McpResourceError {
    /// Server isn't configured at all in `kilo.json` / state.
    NotConfigured(String),
    /// Server is configured but disabled — refuse before dialing.
    Disabled(String),
    /// Local stdio server is not in `state.mcp_children` (never
    /// connected, or already disconnected).
    NotConnected(String),
    /// Underlying transport / RPC / parsing error. Carries a
    /// human-readable message.
    Rpc(String),
}

impl McpResourceError {
    #[allow(dead_code)]
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::NotConfigured(err)
            | Self::Disabled(err)
            | Self::NotConnected(err)
            | Self::Rpc(err) => err,
        }
    }
}

/// Read a resource by URI from a named MCP server.
///
/// Bun parity: mirrors `MCP.readResource(clientName, resourceUri)` at
/// `packages/opencode/src/mcp/index.ts:747-751`, which sends the
/// `resources/read` JSON-RPC request and returns the result. We expose
/// only the `contents` array since that's the only field
/// `resolve_user_multimodal` consumes — wrapping in additional shape
/// would force callers to drill back through `Value`.
pub(crate) async fn read_resource(
    state: &AppState,
    server: &str,
    uri: &str,
) -> Result<ResourceContents, McpResourceError> {
    let cfg = mcp_config(state, server).ok_or_else(|| {
        McpResourceError::NotConfigured(format!("MCP server {server} is not configured"))
    })?;
    if !kilo_mcp::enabled(&cfg) {
        return Err(McpResourceError::Disabled(format!(
            "MCP server {server} is disabled"
        )));
    }
    let result = match cfg {
        kilo_mcp::Config::Local { timeout, .. } => {
            let timeout = Duration::from_millis(timeout.unwrap_or(MCP_DEFAULT_TIMEOUT_MS).max(1));
            // Same lock-and-poll shape as `mcp_invoke` — the
            // `mcp_children` Mutex is held across the synchronous
            // poll loop. See the M1 hardening note in `mcp_invoke`.
            let mut children = state.mcp_children.lock().unwrap();
            let child = children.get_mut(server).ok_or_else(|| {
                McpResourceError::NotConnected(format!("MCP server {server} is not connected"))
            })?;
            // Fix 1: allocate a fresh JSON-RPC id per request so a late
            // response from a previously timed-out call can't bind here.
            let id = mcp_alloc_id();
            let req = json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "resources/read",
                "params": { "uri": uri }
            });
            mcp_write_message(&mut child.stdin, &req).map_err(|err| {
                McpResourceError::Rpc(format!("MCP resources/read write failed: {err}"))
            })?;
            let res = mcp_wait_response_cancel(child, id, timeout, None)
                .map_err(McpResourceError::Rpc)?;
            mcp_response_error(&res).map_err(McpResourceError::Rpc)?;
            res.get("result").cloned().ok_or_else(|| {
                McpResourceError::Rpc("MCP resources/read missing result".to_string())
            })?
        }
        kilo_mcp::Config::Remote {
            url,
            headers,
            timeout,
            ..
        } => {
            // Connectedness mirrors the route handler in
            // `routes::mcp::mcp_call_tool` — refuse if the registry
            // hasn't observed a successful connect.
            let connected = matches!(
                state.mcp.lock().unwrap().get(server),
                Some(kilo_mcp::Status::Connected { .. })
            );
            if !connected {
                return Err(McpResourceError::NotConnected(format!(
                    "MCP server {server} is not connected"
                )));
            }
            let resolved: BTreeMap<String, String> =
                mcp_remote_request_headers(state, server, headers.as_ref())
                    .await
                    .map_err(|err| McpResourceError::Rpc(err.message().to_string()))?;
            let id = mcp_alloc_id();
            let req = json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "resources/read",
                "params": { "uri": uri }
            });
            let res = mcp_post_remote(&url, Some(&resolved), req, id, timeout)
                .await
                .map_err(|err| McpResourceError::Rpc(err.message().to_string()))?;
            mcp_response_error(&res).map_err(McpResourceError::Rpc)?;
            res.get("result").cloned().ok_or_else(|| {
                McpResourceError::Rpc("MCP resources/read missing result".to_string())
            })?
        }
    };
    let contents = result
        .get("contents")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(contents)
}
