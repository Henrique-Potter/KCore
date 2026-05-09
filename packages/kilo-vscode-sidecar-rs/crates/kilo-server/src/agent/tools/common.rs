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
        "webfetch" => input
            .get("url")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| vec![value.to_string()])
            .unwrap_or_else(|| vec!["*".to_string()]),
        "todowrite" => vec!["*".to_string()],
        "skill" => input
            .get("name")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| vec![value.to_string()])
            .unwrap_or_else(|| vec!["*".to_string()]),
        "lsp" => vec!["*".to_string()],
        "bash" => {
            let cmd = input.get("command").and_then(Value::as_str).unwrap_or("");
            bash_command_patterns(cmd)
        }
        _ => vec!["*".to_string()],
    }
}

pub(crate) fn patch_patterns(input: &Value) -> Option<Vec<String>> {
    let text = input.get("patchText")?.as_str()?;
    let sections = parse_apply_patch(text).ok()?;
    Some(sections.into_iter().map(|item| item.path).collect())
}

/// First-word-extraction heuristic mirroring Bun's `BashArity.prefix`
/// for the common cases. Returns most-specific to least-specific
/// patterns terminating in `"*"`. Stays ASCII / no regex.
///
/// We intentionally don't reproduce the full ARITY table from
/// `packages/opencode/src/permission/arity.ts` — just the verbs that
/// users typically write deny rules against. Verbs not in the table
/// fall back to `["<v1> *", "*"]`.
pub(crate) fn bash_command_patterns(command: &str) -> Vec<String> {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return vec!["*".to_string()];
    }

    // Stop at the first shell pipe / chain separator so `a | b` matches
    // on `a` only. Heredoc bodies likewise cap at the first `<<`.
    let head = first_segment(trimmed);
    let tokens: Vec<&str> = head.split_whitespace().collect();
    if tokens.is_empty() {
        return vec!["*".to_string()];
    }

    // `bash -c "rm -rf /"` / `sh -c "..."`: try to extract the inner
    // command's first word. On any parse failure fall back to a
    // shell-flavored pattern.
    if matches!(tokens[0], "bash" | "sh") && tokens.get(1).copied() == Some("-c") {
        if let Some(inner) = tokens.get(2).copied() {
            let inner = inner.trim_matches(|c: char| c == '\'' || c == '"');
            if let Some(first) = inner.split_whitespace().next() {
                let first = strip_dot_slash(first);
                if !first.is_empty() {
                    return one_word_patterns(first);
                }
            }
        }
        return vec![format!("{} *", tokens[0]), "*".to_string()];
    }

    let v1 = strip_dot_slash(tokens[0]);
    if v1.is_empty() {
        return vec!["*".to_string()];
    }

    if is_multi_word_verb(v1) {
        if let Some(v2) = tokens.get(1).copied() {
            // Skip flags as the second token — they don't define a
            // subcommand. Use a single-word pattern instead.
            if !v2.starts_with('-') && !v2.is_empty() {
                return vec![format!("{v1} {v2} *"), format!("{v1} *"), "*".to_string()];
            }
        }
    }

    one_word_patterns(v1)
}

fn one_word_patterns(v1: &str) -> Vec<String> {
    vec![format!("{v1} *"), "*".to_string()]
}

fn first_segment(command: &str) -> &str {
    // Cap at any of: `|`, `&`, `;`, `<<`. We don't try to be a real
    // shell parser — the goal is "match what the user typed at the
    // start", not full lexing.
    let bytes = command.as_bytes();
    let mut end = bytes.len();
    for (i, &b) in bytes.iter().enumerate() {
        if matches!(b, b'|' | b'&' | b';') {
            end = i;
            break;
        }
        if b == b'<' && bytes.get(i + 1) == Some(&b'<') {
            end = i;
            break;
        }
    }
    command[..end].trim()
}

fn strip_dot_slash(token: &str) -> &str {
    token.strip_prefix("./").unwrap_or(token)
}

/// Common multi-word verbs whose first subcommand carries the meaning
/// (e.g. `git push`, `npm install`, `docker compose`). Mirrors the
/// arity-2/3 entries in Bun's `ARITY` table that practitioners write
/// rules against. Not exhaustive; unknown verbs degrade to one-word.
fn is_multi_word_verb(v1: &str) -> bool {
    matches!(
        v1,
        "git"
            | "npm"
            | "yarn"
            | "pnpm"
            | "bun"
            | "cargo"
            | "docker"
            | "kubectl"
            | "aws"
            | "gcloud"
            | "az"
            | "gh"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_push_origin_main_expands_two_words() {
        let p = bash_command_patterns("git push origin main");
        assert!(p.contains(&"git push *".to_string()), "{p:?}");
        assert!(p.contains(&"git *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn cat_is_single_word() {
        let p = bash_command_patterns("cat /etc/passwd");
        assert!(p.contains(&"cat *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
        assert!(!p.iter().any(|v| v.starts_with("cat /")), "{p:?}");
    }

    #[test]
    fn npm_install_expands_two_words() {
        let p = bash_command_patterns("npm install lodash");
        assert!(p.contains(&"npm install *".to_string()), "{p:?}");
        assert!(p.contains(&"npm *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn rm_is_single_word() {
        let p = bash_command_patterns("rm -rf /");
        assert!(p.contains(&"rm *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn empty_command_is_wildcard() {
        assert_eq!(bash_command_patterns(""), vec!["*".to_string()]);
    }

    #[test]
    fn whitespace_only_command_is_wildcard() {
        assert_eq!(bash_command_patterns("  "), vec!["*".to_string()]);
    }

    #[test]
    fn dot_slash_script_is_stripped() {
        let p = bash_command_patterns("./script.sh arg1");
        assert!(p.contains(&"script.sh *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn bash_dash_c_extracts_inner_first_word() {
        let p = bash_command_patterns("bash -c 'rm -rf /'");
        assert!(p.contains(&"rm *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn pipe_caps_at_first_command() {
        let p = bash_command_patterns("cat /etc/passwd | grep root");
        assert!(p.contains(&"cat *".to_string()), "{p:?}");
        assert!(!p.iter().any(|v| v.contains("grep")), "{p:?}");
    }

    #[test]
    fn cargo_build_expands_two_words() {
        let p = bash_command_patterns("cargo build --release");
        assert!(p.contains(&"cargo build *".to_string()), "{p:?}");
        assert!(p.contains(&"cargo *".to_string()), "{p:?}");
    }

    #[test]
    fn git_with_only_flag_falls_back_to_one_word() {
        // `git --version` — second token is a flag, not a subcommand.
        let p = bash_command_patterns("git --version");
        assert!(p.contains(&"git *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn ordering_is_specific_to_least_specific() {
        let p = bash_command_patterns("git push origin main");
        let i_specific = p.iter().position(|v| v == "git push *").unwrap();
        let i_mid = p.iter().position(|v| v == "git *").unwrap();
        let i_wild = p.iter().position(|v| v == "*").unwrap();
        assert!(i_specific < i_mid && i_mid < i_wild);
    }
}
