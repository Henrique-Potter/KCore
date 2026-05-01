use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub const CONTRACT: &str = "kilo-vscode-sidecar.preview.0";
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Serialize)]
pub struct Health {
    pub healthy: bool,
    pub version: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct KiloPath {
    pub home: String,
    pub state: String,
    pub config: String,
    pub worktree: String,
    pub directory: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(flatten)]
    pub data: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Project {
    pub id: String,
    pub worktree: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vcs: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<ProjectIcon>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commands: Option<ProjectCommands>,
    pub time: Time,
    pub sandboxes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProjectIcon {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(rename = "override", skip_serializing_if = "Option::is_none")]
    pub override_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProjectCommands {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Time {
    pub created: i64,
    pub updated: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initialized: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Session {
    pub id: String,
    pub slug: String,
    #[serde(rename = "projectID")]
    pub project_id: String,
    #[serde(rename = "workspaceID", skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    pub directory: String,
    #[serde(rename = "parentID", skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share: Option<Value>,
    pub title: String,
    pub version: String,
    pub time: SessionTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revert: Option<Value>,
}

/// PATCH `/session/{sessionID}` body. Accepts Bun's update shape for `title`,
/// `permission`, and `time.archived`; `time` stays generic so `null` and
/// future nested keys remain harmless to deserialize during the migration.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct SessionUpdateInput {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub permission: Option<Value>,
    #[serde(default)]
    pub time: Option<Value>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct SessionCreateInput {
    #[serde(rename = "parentID")]
    pub parent_id: Option<String>,
    pub title: Option<String>,
    pub permission: Option<Value>,
    #[serde(rename = "workspaceID")]
    pub workspace_id: Option<String>,
    pub platform: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct SessionForkInput {
    #[serde(rename = "messageID", default)]
    pub message_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct SessionRevertInput {
    #[serde(rename = "messageID", default)]
    pub message_id: Option<String>,
    #[serde(rename = "partID", default)]
    pub part_id: Option<String>,
    #[serde(default)]
    pub revert: Option<Value>,
    #[serde(default)]
    pub summary: Option<Value>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct SessionShareInput {
    #[serde(default)]
    pub url: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct SessionViewedInput {
    #[serde(default)]
    pub focused: Vec<String>,
    #[serde(default)]
    pub open: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SessionTime {
    pub created: i64,
    pub updated: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compacting: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archived: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Message {
    pub info: Value,
    pub parts: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MessageAppendInput {
    pub info: Value,
    #[serde(default)]
    pub parts: Vec<Value>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct PromptInput {
    #[serde(default)]
    pub parts: Vec<Value>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub model: Option<Value>,
    #[serde(default)]
    pub tools: Option<Value>,
    #[serde(rename = "messageID", default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub system: Option<Value>,
    #[serde(default)]
    pub format: Option<Value>,
    #[serde(default)]
    pub variant: Option<Value>,
    /// Generic provider payload passthrough. The Rust fake-provider harness
    /// currently recognizes only internal `fake*` keys for deterministic M7
    /// scaffolding; real provider routing is intentionally out of scope.
    #[serde(default)]
    pub provider: Option<Value>,
    #[serde(rename = "editorContext", default)]
    pub editor_context: Option<Value>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MessageAppendResult {
    pub info: Value,
    pub parts: Vec<Value>,
    pub time: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProviderResult {
    pub all: Vec<Value>,
    #[serde(rename = "default")]
    pub defaults: BTreeMap<String, String>,
    pub connected: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConfigProvidersResult {
    pub providers: Vec<Value>,
    #[serde(rename = "default")]
    pub defaults: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct GlobalEvent {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub directory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub payload: Payload,
}

#[derive(Clone, Debug, Serialize)]
pub struct Payload {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "syncEvent")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_event: Option<Value>,
    pub properties: Value,
}

impl Health {
    pub fn ok() -> Self {
        Self {
            healthy: true,
            version: VERSION,
        }
    }
}

impl GlobalEvent {
    pub fn connected() -> Self {
        Self::event("server.connected")
    }

    pub fn heartbeat() -> Self {
        Self::event("server.heartbeat")
    }

    pub fn session(kind: &'static str, directory: String, info: Session) -> Self {
        Self {
            directory: Some(directory),
            project: Some(info.project_id.clone()),
            payload: Payload {
                kind: kind.to_string(),
                sync_event: None,
                properties: json!({
                    "sessionID": info.id,
                    "info": info,
                }),
            },
        }
    }

    pub fn message(
        kind: &'static str,
        directory: String,
        project: String,
        properties: Value,
    ) -> Self {
        Self {
            directory: Some(directory),
            project: Some(project),
            payload: Payload {
                kind: kind.to_string(),
                sync_event: None,
                properties,
            },
        }
    }

    pub fn sync(directory: String, project: String, event: Value) -> Self {
        Self {
            directory: Some(directory),
            project: Some(project),
            payload: Payload {
                kind: "sync".to_string(),
                sync_event: Some(event),
                properties: json!({}),
            },
        }
    }

    pub fn bus(kind: &'static str, properties: Value) -> Self {
        Self {
            directory: None,
            project: None,
            payload: Payload {
                kind: kind.to_string(),
                sync_event: None,
                properties,
            },
        }
    }

    fn event(kind: &'static str) -> Self {
        Self {
            directory: None,
            project: None,
            payload: Payload {
                kind: kind.to_string(),
                sync_event: None,
                properties: json!({}),
            },
        }
    }
}
