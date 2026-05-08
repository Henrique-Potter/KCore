use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub const CRATE: &str = "kilo-mcp";

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum Config {
    #[serde(rename = "local")]
    Local {
        command: Vec<String>,
        #[serde(default)]
        environment: Option<BTreeMap<String, String>>,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        enabled: Option<bool>,
        #[serde(default)]
        timeout: Option<u64>,
    },
    #[serde(rename = "remote")]
    Remote {
        url: String,
        #[serde(default)]
        enabled: Option<bool>,
        #[serde(default)]
        headers: Option<BTreeMap<String, String>>,
        #[serde(default)]
        oauth: Option<Value>,
        #[serde(default)]
        timeout: Option<u64>,
    },
}

#[derive(Clone, Debug, Deserialize)]
pub struct AddInput {
    pub name: String,
    pub config: Config,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CallInput {
    pub name: String,
    #[serde(default = "default_arguments", alias = "input")]
    pub arguments: Value,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AuthInput {
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub headers: Option<BTreeMap<String, String>>,
    #[serde(default, rename = "accessToken", alias = "access_token")]
    pub access_token: Option<String>,
    #[serde(default, rename = "refreshToken", alias = "refresh_token")]
    pub refresh_token: Option<String>,
    #[serde(default, rename = "expiresAt", alias = "expires_at")]
    pub expires_at: Option<Value>,
    #[serde(
        default,
        rename = "tokenUrl",
        alias = "tokenEndpoint",
        alias = "token_endpoint"
    )]
    pub token_url: Option<String>,
    #[serde(default, rename = "clientId", alias = "client_id")]
    pub client_id: Option<String>,
    #[serde(default, rename = "clientSecret", alias = "client_secret")]
    pub client_secret: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Tool {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, rename = "inputSchema")]
    pub input_schema: Value,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "status")]
pub enum Status {
    #[serde(rename = "connected")]
    Connected {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tools: Vec<Tool>,
    },
    #[serde(rename = "disabled")]
    Disabled,
    #[serde(rename = "failed")]
    Failed { error: String },
    #[serde(rename = "needs_auth")]
    NeedsAuth,
    #[serde(rename = "needs_client_registration")]
    NeedsClientRegistration { error: String },
}

pub type StatusMap = BTreeMap<String, Status>;

fn default_arguments() -> Value {
    json!({})
}

pub fn configs(value: &Value) -> BTreeMap<String, Config> {
    let Some(data) = value.get("mcp").and_then(Value::as_object) else {
        return BTreeMap::new();
    };
    data.iter()
        .filter_map(|(key, value)| config(value).map(|cfg| (key.clone(), cfg)))
        .collect()
}

pub fn config(value: &Value) -> Option<Config> {
    serde_json::from_value(value.clone()).ok()
}

pub fn enabled(cfg: &Config) -> bool {
    match cfg {
        Config::Local { enabled, .. } | Config::Remote { enabled, .. } => enabled.unwrap_or(true),
    }
}

pub fn baseline(cfg: &Config) -> Status {
    if !enabled(cfg) {
        return Status::Disabled;
    }
    if matches!(cfg, Config::Remote { .. }) {
        return Status::Failed {
            error: "Rust sidecar MCP remote server is not connected.".to_string(),
        };
    }
    Status::Failed {
        error: "Rust sidecar MCP local server is not connected.".to_string(),
    }
}

pub fn status(config: &Value, memory: &StatusMap) -> StatusMap {
    configs(config)
        .into_iter()
        .map(|(name, cfg)| {
            let value = memory.get(&name).cloned().unwrap_or_else(|| baseline(&cfg));
            (name, value)
        })
        .collect()
}

pub fn not_implemented(name: &str) -> Value {
    json!({
        "name": "RustMcpNotImplementedError",
        "data": {
            "message": "Rust sidecar MCP lifecycle routes are registered, but server launch and transport support are not implemented yet.",
            "server": name,
        },
    })
}

pub fn error(name: &str, server: &str, message: impl Into<String>) -> Value {
    json!({
        "name": name,
        "data": {
            "message": message.into(),
            "server": server,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_lists_configured_servers() {
        let cfg = json!({
            "mcp": {
                "off": { "type": "local", "command": ["node", "server.js"], "enabled": false },
                "on": { "type": "remote", "url": "https://example.test/mcp" },
                "bad": { "command": ["node"] }
            }
        });

        let got = status(&cfg, &StatusMap::new());

        assert_eq!(got.get("off"), Some(&Status::Disabled));
        assert_eq!(
            got.get("on"),
            Some(&Status::Failed {
                error: "Rust sidecar MCP remote server is not connected.".to_string(),
            })
        );
        assert!(!got.contains_key("bad"));
    }

    #[test]
    fn in_memory_status_overrides_baseline_for_configured_server() {
        let cfg = json!({ "mcp": { "playwright": { "type": "local", "command": ["npx", "@playwright/mcp"] } } });
        let memory = StatusMap::from([("playwright".to_string(), Status::Disabled)]);

        assert_eq!(
            status(&cfg, &memory).get("playwright"),
            Some(&Status::Disabled)
        );
    }
}
