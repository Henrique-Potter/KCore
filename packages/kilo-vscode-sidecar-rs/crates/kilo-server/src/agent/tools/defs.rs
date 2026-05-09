//! `ChatTool` schema definitions for the six fake/real tools.
//!
//! Step 7 of the kilo-server module split: verbatim cut from `lib.rs`. The
//! schemas drive the OpenAI Responses API `tools` array as well as the
//! oracle harness's parity checks; they are the single source of truth
//! for tool name + parameter shape.
//!
//! Per-tool descriptions are loaded verbatim from Bun's `.txt` siblings
//! under `prompts/` via `include_str!`. Bun is the executable oracle;
//! every multi-paragraph guidance block (read-before-edit, "AVOID
//! `cd ... && cmd`", offset semantics, image/PDF support, etc.) must
//! reach the model unchanged. See migration-gaps.md "bash tool prompt
//! loading" for the gap this closes.

use kilo_provider::ChatTool;
use serde_json::json;

const READ_DESCRIPTION: &str = include_str!("prompts/read.txt");
const GLOB_DESCRIPTION: &str = include_str!("prompts/glob.txt");
const GREP_DESCRIPTION: &str = include_str!("prompts/grep.txt");
const WEBFETCH_DESCRIPTION: &str = include_str!("../../../../../../opencode/src/tool/webfetch.txt");
const TODOWRITE_DESCRIPTION: &str =
    include_str!("../../../../../../opencode/src/tool/todowrite.txt");
const SKILL_DESCRIPTION: &str = include_str!("../../../../../../opencode/src/tool/skill.txt");
const SUGGEST_DESCRIPTION: &str =
    include_str!("../../../../../../opencode/src/kilocode/suggestion/tool.txt");
const LSP_DESCRIPTION: &str = include_str!("../../../../../../opencode/src/tool/lsp.txt");
const WRITE_DESCRIPTION: &str = include_str!("prompts/write.txt");
const EDIT_DESCRIPTION: &str = include_str!("prompts/edit.txt");
const APPLY_PATCH_DESCRIPTION: &str = include_str!("prompts/apply_patch.txt");
const BASH_DESCRIPTION: &str = include_str!("prompts/bash.txt");
const QUESTION_DESCRIPTION: &str = include_str!("prompts/question.txt");
const TASK_DESCRIPTION: &str = include_str!("prompts/task.txt");
const PLAN_EXIT_DESCRIPTION: &str =
    include_str!("../../../../../../opencode/src/tool/plan-exit.txt");

/// Build the bash tool description with the small subset of placeholders
/// we have context for at description-build time substituted.
///
/// Bun substitutes six placeholders (`${directory}`, `${os}`, `${shell}`,
/// `${chaining}`, `${maxLines}`, `${maxBytes}`) at registry-build time
/// (`packages/opencode/src/tool/bash.ts:611-616`). For the first pass we
/// only have static info: OS family and the platform-specific chaining
/// hint. The other placeholders pass through unchanged; later milestones
/// can replace them at the agent-loop level when worktree path, resolved
/// shell binary, and truncation limits are threaded through.
fn build_bash_description() -> String {
    // Match Bun's text for the POSIX `&&` branch (`bash.ts:604-605`); on
    // Windows the powershell branch is used. The Rust shell selection
    // currently picks `cmd.exe`/`pwsh` but we don't have that resolved
    // here, so use the `if ($?) { ... }` form on Windows where `&&` is
    // unsupported by Windows PowerShell 5.1 — same wording Bun ships.
    let chaining = if cfg!(windows) {
        "If the commands depend on each other and must run sequentially, avoid '&&' in this shell because Windows PowerShell 5.1 does not support it. Use PowerShell conditionals such as `cmd1; if ($?) { cmd2 }` when later commands must depend on earlier success."
    } else {
        "If the commands depend on each other and must run sequentially, use a single Bash call with '&&' to chain them together (e.g., `git add . && git commit -m \"message\" && git push`). For instance, if one operation must complete before another starts (like mkdir before cp, Write before Bash for git operations, or git add before git commit), run these operations sequentially instead."
    };
    BASH_DESCRIPTION
        .replace("${os}", std::env::consts::OS)
        .replace("${chaining}", chaining)
}

pub(crate) fn read_def() -> ChatTool {
    // Audit Fix 8: drop the `path` synonym — Bun's tool schema only
    // surfaces `filePath`, and advertising both confused some models into
    // emitting tool calls keyed on `path`. Mark `filePath` as required so
    // OpenAI's Responses API rejects malformed tool calls upstream.
    ChatTool {
        name: "read".to_string(),
        description: READ_DESCRIPTION.to_string(),
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
        description: GREP_DESCRIPTION.to_string(),
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

pub(crate) fn glob_def() -> ChatTool {
    ChatTool {
        name: "glob".to_string(),
        description: GLOB_DESCRIPTION.to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "pattern": { "type": "string", "description": "The glob pattern to match files against." },
                "path": { "type": "string", "description": "Directory to search in. If omitted, the current working directory is used." }
            },
            "required": ["pattern"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn webfetch_def() -> ChatTool {
    ChatTool {
        name: "webfetch".to_string(),
        description: WEBFETCH_DESCRIPTION.to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "url": { "type": "string", "description": "The URL to fetch content from." },
                "format": {
                    "type": "string",
                    "enum": ["text", "markdown", "html"],
                    "description": "The format to return the content in. Defaults to markdown."
                },
                "timeout": {
                    "type": "number",
                    "minimum": 1,
                    "maximum": 120,
                    "description": "Optional timeout in seconds, capped at 120."
                }
            },
            "required": ["url"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn todowrite_def() -> ChatTool {
    ChatTool {
        name: "todowrite".to_string(),
        description: TODOWRITE_DESCRIPTION.to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "todos": {
                    "type": "array",
                    "description": "The updated todo list.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "content": { "type": "string", "description": "Brief description of the task." },
                            "status": {
                                "type": "string",
                                "description": "Current status of the task: pending, in_progress, completed, cancelled."
                            },
                            "priority": { "type": "string", "description": "Priority level of the task: high, medium, low." }
                        },
                        "required": ["content", "status", "priority"],
                        "additionalProperties": false
                    }
                }
            },
            "required": ["todos"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn skill_def() -> ChatTool {
    ChatTool {
        name: "skill".to_string(),
        description: SKILL_DESCRIPTION.to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "The name of the skill from available_skills."
                }
            },
            "required": ["name"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn suggest_def() -> ChatTool {
    ChatTool {
        name: "suggest".to_string(),
        description: SUGGEST_DESCRIPTION.to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "suggest": {
                    "type": "string",
                    "description": "Short suggestion text shown to the user."
                },
                "actions": {
                    "type": "array",
                    "description": "Available actions the user can take.",
                    "minItems": 1,
                    "maxItems": 2,
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": { "type": "string", "description": "Button or option label (1-5 words)." },
                            "description": { "type": "string", "description": "Brief explanation of what this action does." },
                            "prompt": { "type": "string", "description": "Synthetic user prompt to inject when this action is accepted." }
                        },
                        "required": ["label", "prompt"],
                        "additionalProperties": false
                    }
                }
            },
            "required": ["suggest", "actions"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn lsp_def() -> ChatTool {
    ChatTool {
        name: "lsp".to_string(),
        description: LSP_DESCRIPTION.to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "operation": {
                    "type": "string",
                    "enum": [
                        "goToDefinition",
                        "findReferences",
                        "hover",
                        "documentSymbol",
                        "workspaceSymbol",
                        "goToImplementation",
                        "prepareCallHierarchy",
                        "incomingCalls",
                        "outgoingCalls"
                    ],
                    "description": "The LSP operation to perform."
                },
                "filePath": { "type": "string", "description": "The absolute or relative path to the file." },
                "line": { "type": "integer", "minimum": 1, "description": "The line number (1-based, as shown in editors)." },
                "character": { "type": "integer", "minimum": 1, "description": "The character offset (1-based, as shown in editors)." }
            },
            "required": ["operation", "filePath", "line", "character"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn write_def() -> ChatTool {
    ChatTool {
        name: "write".to_string(),
        description: WRITE_DESCRIPTION.to_string(),
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
        description: EDIT_DESCRIPTION.to_string(),
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
        description: APPLY_PATCH_DESCRIPTION.to_string(),
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
        description: build_bash_description(),
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
        description: QUESTION_DESCRIPTION.to_string(),
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
        description: TASK_DESCRIPTION.to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "description": { "type": "string", "description": "A short 3-5 word description of the task." },
                "prompt": { "type": "string", "description": "The task for the subagent to perform." },
                "subagent_type": { "type": "string", "description": "The type of specialized agent to use for this task." },
                "task_id": { "type": "string", "description": "Existing child session id to resume." },
                "command": { "type": "string", "description": "Slash command that triggered this task." },
                "variant": { "description": "Optional model variant/reasoning-effort override for the subagent." }
            },
            "required": ["description", "prompt", "subagent_type"],
            "additionalProperties": false
        }),
    }
}

pub(crate) fn plan_exit_def() -> ChatTool {
    ChatTool {
        name: "plan_exit".to_string(),
        description: PLAN_EXIT_DESCRIPTION.to_string(),
        parameters: json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }),
    }
}
