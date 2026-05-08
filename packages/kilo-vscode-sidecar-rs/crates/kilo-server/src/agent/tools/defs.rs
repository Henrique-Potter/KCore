//! `ChatTool` schema definitions for the six fake/real tools.
//!
//! Step 7 of the kilo-server module split: verbatim cut from `lib.rs`. The
//! schemas drive the OpenAI Responses API `tools` array as well as the
//! oracle harness's parity checks; they are the single source of truth
//! for tool name + parameter shape.

use kilo_provider::ChatTool;
use serde_json::json;

pub(crate) fn read_def() -> ChatTool {
    // Audit Fix 8: drop the `path` synonym — Bun's tool schema only
    // surfaces `filePath`, and advertising both confused some models into
    // emitting tool calls keyed on `path`. Mark `filePath` as required so
    // OpenAI's Responses API rejects malformed tool calls upstream.
    ChatTool {
        name: "read".to_string(),
        description: "Read a file or directory under the current workspace.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "filePath": { "type": "string", "description": "Workspace-relative file or directory path." },
                "offset": { "type": "integer", "minimum": 1 },
                "limit": { "type": "integer", "minimum": 1 }
            },
            "required": ["filePath"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn grep_def() -> ChatTool {
    ChatTool {
        name: "grep".to_string(),
        description: "Search text in files under the current workspace.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string", "description": "Literal text pattern to search for." },
                "path": { "type": "string", "description": "Workspace-relative file or directory path." }
            },
            "required": ["pattern"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn write_def() -> ChatTool {
    ChatTool {
        name: "write".to_string(),
        description: "Write a file under the current workspace after permission approval."
            .to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "filePath": { "type": "string", "description": "Workspace-relative file path." },
                "content": { "type": "string", "description": "Complete file content to write." }
            },
            "required": ["filePath", "content"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn edit_def() -> ChatTool {
    ChatTool {
        name: "edit".to_string(),
        description:
            "Replace text in a file under the current workspace after permission approval."
                .to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "filePath": { "type": "string", "description": "Workspace-relative file path." },
                "oldString": { "type": "string", "description": "Text to replace. Empty creates/replaces the file content." },
                "newString": { "type": "string", "description": "Replacement text." },
                "replaceAll": { "type": "boolean", "description": "Replace every match instead of requiring a unique match." }
            },
            "required": ["filePath", "oldString", "newString"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn apply_patch_def() -> ChatTool {
    ChatTool {
        name: "apply_patch".to_string(),
        description: "Apply a file patch under the current workspace after permission approval."
            .to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "patchText": { "type": "string", "description": "Patch text beginning with *** Begin Patch and ending with *** End Patch." }
            },
            "required": ["patchText"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn bash_def() -> ChatTool {
    ChatTool {
        name: "bash".to_string(),
        description: "Run a shell command in the current workspace after permission approval."
            .to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "command": { "type": "string", "description": "Command to run." },
                "description": { "type": "string", "description": "Short human-readable description of the command." },
                "workdir": { "type": "string", "description": "Workspace-relative working directory." },
                "timeout": { "type": "integer", "minimum": 0, "maximum": 600000, "description": "Timeout in milliseconds." }
            },
            "required": ["command"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn question_def() -> ChatTool {
    ChatTool {
        name: "question".to_string(),
        description: "Ask the user a clarifying question and wait for their reply. \
                      Use only when essential information is missing and cannot be \
                      inferred from the workspace; the user sees a UI prompt and \
                      responds with one of the offered options (or freeform text)."
            .to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "text": { "type": "string", "description": "The question to put to the user." },
                "options": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Optional list of suggested answer choices."
                }
            },
            "required": ["text"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn task_def() -> ChatTool {
    ChatTool {
        name: "task".to_string(),
        description: "Launch a subagent in a child session and return its result.".to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "description": { "type": "string", "description": "A short 3-5 word description of the task." },
                "prompt": { "type": "string", "description": "The task for the subagent to perform." },
                "subagent_type": { "type": "string", "description": "The type of specialized agent to use for this task." },
                "task_id": { "type": "string", "description": "Existing child session id to resume." },
                "command": { "type": "string", "description": "Slash command that triggered this task." }
            },
            "required": ["description", "prompt", "subagent_type"],
            "additionalProperties": false
        }),
    }
}
