//! Shared helpers used across the fake-tool runtime.
//!
//! Step 7 of the kilo-server module split: these helpers were verbatim-cut
//! out of `lib.rs`. `tool_input` continues to live in `agent::parts` (Step 6
//! placed it there); use `crate::agent::parts::tool_input` to reach it.
//!
//! Visibility is `pub(crate)` for everything that crosses the module
//! boundary (`agent::fake`, `agent::tools::*`, and the still-in-`lib.rs`
//! permission machinery that consumes `tool_permission`/`tool_patterns`).

use std::cell::RefCell;
use std::path::Path as FsPath;

use serde_json::Value;
use tree_sitter::{Node, Parser, Tree};

use crate::agent::tools::patch::parse_apply_patch;
use crate::util::paths::slash;
use crate::KNOWN_TOOLS;

thread_local! {
    /// One tree-sitter-bash `Parser` per thread. `Parser::new()` plus
    /// `set_language` is ~200-500µs each call; bash permission checks
    /// happen on every tool invocation so caching the parser saves
    /// roughly 80% of the per-call parse cost. `set_language` is
    /// idempotent — once initialised we just reuse the parser.
    static BASH_PARSER: RefCell<Option<Parser>> = const { RefCell::new(None) };
}

/// Parse `command` with the cached thread-local bash parser, hand the
/// produced tree to `f`, and return its result. Returns `None` if the
/// parser couldn't be initialised or the parse itself failed.
///
/// The parser is moved out of the `RefCell` for the duration of the
/// parse + callback so recursive calls (`arity_prefixes` → `parse_bash`
/// for `bash -c` inner commands) don't double-borrow. The parser is
/// returned to the slot on exit; if a recursive call already populated
/// the slot, this outer call's parser is dropped.
fn with_bash_parser<R>(command: &str, f: impl FnOnce(&Tree) -> R) -> Option<R> {
    let mut parser = BASH_PARSER.with(|slot| slot.borrow_mut().take());
    if parser.is_none() {
        let mut fresh = Parser::new();
        fresh.set_language(&tree_sitter_bash::language()).ok()?;
        parser = Some(fresh);
    }
    let mut parser = parser?;
    let tree = parser.parse(command, None);
    let result = tree.map(|tree| f(&tree));
    BASH_PARSER.with(|slot| {
        let mut borrow = slot.borrow_mut();
        if borrow.is_none() {
            *borrow = Some(parser);
        }
    });
    result
}

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

/// Tree-sitter-backed permission pattern extractor for bash commands.
///
/// Mirrors the Bun implementation in `packages/opencode/src/tool/bash.ts`
/// (`collect` + `BashArity.prefix`). For each `command` node in the parse
/// tree (recursing through pipelines, subshells `(...)`, command
/// substitutions `$(...)`, and skipping heredoc bodies), we extract the
/// command's leading verb tokens and apply the arity table from
/// `packages/opencode/src/permission/arity.ts`.
///
/// Output: most-specific to least-specific patterns terminating in `"*"`,
/// deduplicated, with `*` always last. For example:
///   `cat foo | grep bar`     -> ["cat *", "grep *", "*"]
///   `aws s3 ls bucket`       -> ["aws s3 ls *", "aws s3 *", "aws *", "*"]
///   `(cd /tmp && rm foo)`    -> ["rm *", "*"]            (cd is filtered)
///   `echo $(rm -rf /)`       -> ["echo *", "rm *", "*"]
///
/// Falls back to the wave 2 K first-word heuristic on parser failure.
pub(crate) fn bash_command_patterns(command: &str) -> Vec<String> {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return vec!["*".to_string()];
    }

    match parse_bash(command) {
        Some(patterns) if !patterns.is_empty() => patterns,
        _ => fallback_patterns(trimmed),
    }
}

/// Run the tree-sitter-bash parser and walk the AST. Returns `None` if
/// the parser cannot be initialised; returns `Some(empty)` when the parse
/// succeeded but yielded no usable command nodes (caller falls back).
fn parse_bash(command: &str) -> Option<Vec<String>> {
    let bytes = command.as_bytes();
    with_bash_parser(command, |tree| {
        let root = tree.root_node();

        // A parse error at the top level (e.g. `if then else`) means
        // the grammar bailed; let the caller fall back to the
        // heuristic. We intentionally don't reject *any* error in the
        // tree — most real commands tolerate minor parse warnings
        // while still extracting sensible command nodes.
        if root.has_error() && root.child_count() == 0 {
            return None;
        }
        if root.kind() == "ERROR" {
            return None;
        }

        let mut commands: Vec<Vec<String>> = Vec::new();
        walk(root, bytes, &mut commands);

        if commands.is_empty() && root.has_error() {
            // Parse produced no commands and the tree has errors -> the
            // input is malformed enough that fallback is safer.
            return None;
        }

        let mut patterns: Vec<String> = Vec::new();
        for tokens in commands {
            for prefix in arity_prefixes(&tokens) {
                let value = format!("{prefix} *");
                if !patterns.contains(&value) {
                    patterns.push(value);
                }
            }
        }
        if !patterns.contains(&"*".to_string()) {
            patterns.push("*".to_string());
        }
        Some(patterns)
    })
    .flatten()
}

/// Recursively visit nodes, collecting tokens for each `command` node.
/// Skips heredoc bodies (their contents are data, not commands) and
/// recurses into subshells / command substitutions naturally because
/// tree-sitter exposes their inner programs as descendants.
fn walk(node: Node, bytes: &[u8], out: &mut Vec<Vec<String>>) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        // `heredoc_body` is the literal text between `<<EOF` and `EOF`.
        // tree-sitter-bash sometimes re-parses it as commands; we never
        // want to treat that as executable.
        if child.kind() == "heredoc_body" {
            continue;
        }
        if child.kind() == "command" {
            if let Some(tokens) = command_tokens(child, bytes) {
                out.push(tokens);
            }
            // A command can still contain a command_substitution argument
            // that holds nested commands -> keep descending.
        }
        walk(child, bytes, out);
    }
}

/// Extract the meaningful tokens (`command_name` + non-flag word args)
/// from a `command` node. Returns `None` if no command name is present.
fn command_tokens(node: Node, bytes: &[u8]) -> Option<Vec<String>> {
    let mut tokens: Vec<String> = Vec::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "command_name" => {
                let text = node_text(child, bytes);
                let stripped = strip_dot_slash(text.trim());
                if !stripped.is_empty() {
                    tokens.push(stripped.to_string());
                }
            }
            // Skip command substitutions, expansions, redirections, etc.
            // BashArity only consumes literal subcommand verbs.
            "word" | "string" | "raw_string" | "concatenation" | "number" => {
                let text = node_text(child, bytes).trim().to_string();
                if !text.is_empty() {
                    tokens.push(text);
                }
            }
            _ => {}
        }
    }
    if tokens.is_empty() {
        return None;
    }
    Some(tokens)
}

fn node_text<'a>(node: Node<'a>, bytes: &'a [u8]) -> &'a str {
    std::str::from_utf8(&bytes[node.start_byte()..node.end_byte()]).unwrap_or("")
}

/// Apply the arity table to a token list and emit every nested prefix
/// from longest-meaningful-down to single-word, e.g.
///   ["aws", "s3", "ls", "bucket"] -> ["aws s3 ls", "aws s3", "aws"].
/// The wildcard `*` is appended by the caller.
fn arity_prefixes(tokens: &[String]) -> Vec<String> {
    if tokens.is_empty() {
        return Vec::new();
    }
    // `bash -c "cd /tmp && rm foo"`: re-parse the inner string with
    // the same tree-sitter bash grammar so chained / piped / quoted
    // verbs all surface (`split_whitespace` would emit `&&` as a
    // pseudo-token and lose every verb after the first).
    if matches!(tokens[0].as_str(), "bash" | "sh")
        && tokens.get(1).map(String::as_str) == Some("-c")
    {
        if let Some(inner) = tokens.get(2) {
            let inner = inner.trim_matches(|c: char| c == '\'' || c == '"');
            // Recursive parse via the cached parser. `parse_bash`
            // already appends a `*` wildcard; strip it so the caller
            // can decide where the wildcard lands in the merged list.
            if let Some(mut inner_patterns) = parse_bash(inner) {
                inner_patterns.retain(|p| p != "*");
                if !inner_patterns.is_empty() {
                    // Strip the trailing ` *` we'll re-add upstream.
                    let prefixes: Vec<String> = inner_patterns
                        .into_iter()
                        .filter_map(|p| p.strip_suffix(" *").map(str::to_string))
                        .collect();
                    if !prefixes.is_empty() {
                        return prefixes;
                    }
                }
            }
            // Inner parse failed or yielded nothing useful — fall back
            // to the naive whitespace split so we still surface
            // *something* (the audited regression).
            let inner_tokens: Vec<String> = inner.split_whitespace().map(str::to_string).collect();
            if !inner_tokens.is_empty() {
                return arity_prefixes(&inner_tokens);
            }
        }
        return vec![tokens[0].clone()];
    }

    // Drop flags before computing meaningful prefix length: `git --version`
    // is arity 1 because `--version` isn't a subcommand. Mirrors Bun's
    // `BashArity.prefix(tokens)` -> ARITY[prefix] lookup which iterates
    // joined non-flag prefixes.
    let meaningful: Vec<&str> = tokens
        .iter()
        .map(String::as_str)
        .filter(|t| !t.starts_with('-'))
        .collect();

    let arity_len = compute_arity(&meaningful);

    let mut out: Vec<String> = Vec::new();
    // Walk from `arity_len` down to 1 emitting nested prefixes.
    for len in (1..=arity_len).rev() {
        let slice = &meaningful[..len];
        let value = slice.join(" ");
        if !value.is_empty() && !out.contains(&value) {
            out.push(value);
        }
    }
    out
}

/// Bun arity table from `packages/opencode/src/permission/arity.ts`.
/// Entries map a literal command prefix to the number of meaningful
/// (non-flag) tokens that constitute the "human-understandable command".
/// Longest matching prefix wins. Verbs not in the table degrade to
/// arity 1, matching Bun's `prefix()` fallback at `arity.ts:8`.
fn compute_arity(tokens: &[&str]) -> usize {
    if tokens.is_empty() {
        return 0;
    }
    for len in (1..=tokens.len()).rev() {
        let key = tokens[..len].join(" ");
        if let Some(arity) = arity_lookup(&key) {
            // Clamp to the available token count -- a 3-arity command
            // like `aws s3 ls` with only 2 tokens collapses to 2.
            return arity.min(tokens.len());
        }
    }
    1
}

fn arity_lookup(key: &str) -> Option<usize> {
    Some(match key {
        // arity 1 - explicit so longer prefixes don't accidentally win
        "cat" | "cd" | "chmod" | "chown" | "cp" | "echo" | "env" | "export" | "grep" | "kill"
        | "killall" | "ln" | "ls" | "mkdir" | "mv" | "ps" | "pwd" | "rm" | "rmdir" | "sleep"
        | "source" | "tail" | "touch" | "unset" | "which" => 1,

        // arity 2 - common verb + subcommand
        "bazel" | "brew" | "bun" | "cargo" | "cdk" | "cf" | "cmake" | "composer" | "consul"
        | "crictl" | "deno" | "docker" | "eksctl" | "firebase" | "flyctl" | "git" | "go"
        | "gradle" | "helm" | "heroku" | "hugo" | "ip" | "kind" | "kubectl" | "kustomize"
        | "make" | "mc" | "minikube" | "mongosh" | "mvn" | "mysql" | "ng" | "npm" | "nvm"
        | "nx" | "openssl" | "pip" | "pipenv" | "pnpm" | "podman" | "poetry" | "psql"
        | "pulumi" | "pyenv" | "python" | "rake" | "rbenv" | "redis-cli" | "rustup"
        | "serverless" | "skaffold" | "sls" | "sst" | "swift" | "systemctl" | "terraform"
        | "tmux" | "turbo" | "ufw" | "vault" | "vercel" | "volta" | "wp" | "yarn" => 2,

        // arity 3 - service / subgroup-scoped CLIs
        "aws" | "az" | "doctl" | "gcloud" | "gh" | "sfdx" => 3,

        // arity 3 - explicit two-word verbs whose third token carries meaning
        "bun run"
        | "bun x"
        | "cargo add"
        | "cargo run"
        | "consul kv"
        | "deno task"
        | "docker builder"
        | "docker compose"
        | "docker container"
        | "docker image"
        | "docker network"
        | "docker volume"
        | "eksctl create"
        | "git config"
        | "git remote"
        | "git stash"
        | "ip addr"
        | "ip link"
        | "ip netns"
        | "ip route"
        | "kind create"
        | "kubectl kustomize"
        | "kubectl rollout"
        | "mc admin"
        | "npm exec"
        | "npm init"
        | "npm run"
        | "npm view"
        | "openssl req"
        | "openssl x509"
        | "pnpm dlx"
        | "pnpm exec"
        | "pnpm run"
        | "podman container"
        | "podman image"
        | "pulumi stack"
        | "terraform workspace"
        | "vault auth"
        | "vault kv"
        | "yarn dlx"
        | "yarn run" => 3,

        _ => return None,
    })
}

fn strip_dot_slash(token: &str) -> &str {
    token.strip_prefix("./").unwrap_or(token)
}

/// First-word heuristic from wave 2 K, retained as the parser-failure
/// fallback. Stays ASCII / no regex.
fn fallback_patterns(trimmed: &str) -> Vec<String> {
    let head = first_segment(trimmed);
    let tokens: Vec<&str> = head.split_whitespace().collect();
    if tokens.is_empty() {
        return vec!["*".to_string()];
    }

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

    if arity_lookup(v1).map(|a| a >= 2).unwrap_or(false) {
        if let Some(v2) = tokens.get(1).copied() {
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

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Wave 2 K tests (must continue to pass) ----

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
        // Wave 2 K capped at the first segment; tree-sitter now emits
        // patterns for every segment. Ensure the original `cat *`
        // assertion still holds (the additional `grep *` is verified by
        // the dedicated pipeline test below).
        let p = bash_command_patterns("cat /etc/passwd | grep root");
        assert!(p.contains(&"cat *".to_string()), "{p:?}");
    }

    #[test]
    fn cargo_build_expands_two_words() {
        let p = bash_command_patterns("cargo build --release");
        assert!(p.contains(&"cargo build *".to_string()), "{p:?}");
        assert!(p.contains(&"cargo *".to_string()), "{p:?}");
    }

    #[test]
    fn git_with_only_flag_falls_back_to_one_word() {
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

    // ---- Tree-sitter-only tests (cases the heuristic could not handle) ----

    #[test]
    fn pipeline_emits_patterns_for_each_segment() {
        let p = bash_command_patterns("cat foo | grep bar | tail");
        assert!(p.contains(&"cat *".to_string()), "{p:?}");
        assert!(p.contains(&"grep *".to_string()), "{p:?}");
        assert!(p.contains(&"tail *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn command_substitution_extracts_inner_command() {
        let p = bash_command_patterns("echo $(rm -rf /)");
        assert!(p.contains(&"echo *".to_string()), "{p:?}");
        assert!(p.contains(&"rm *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn subshell_extracts_inner() {
        let p = bash_command_patterns("(cd /tmp && rm foo)");
        assert!(p.contains(&"rm *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn aws_three_word_arity() {
        let p = bash_command_patterns("aws s3 ls bucket");
        assert!(p.contains(&"aws s3 ls *".to_string()), "{p:?}");
        assert!(p.contains(&"aws s3 *".to_string()), "{p:?}");
        assert!(p.contains(&"aws *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn heredoc_body_ignored() {
        // The `rm /` line is the heredoc body of `cat <<EOF` and must
        // not produce a `rm *` permission pattern.
        let p = bash_command_patterns("cat <<EOF\nrm /\nEOF\n");
        assert!(p.contains(&"cat *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
        assert!(!p.contains(&"rm *".to_string()), "{p:?}");
    }

    #[test]
    fn bash_arity_recursive_parse_extracts_inner_verbs() {
        // Audit bug: `bash -c "cd /tmp && rm foo"` used to split on
        // whitespace, producing `["cd", "/tmp", "&&", "rm", "foo"]`
        // and surfacing only `cd *`. Tree-sitter re-parse must catch
        // both `cd` and `rm` (and the `*` wildcard).
        let p = bash_command_patterns("bash -c \"cd /tmp && rm foo\"");
        assert!(p.contains(&"cd *".to_string()), "{p:?}");
        assert!(p.contains(&"rm *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn bash_arity_recursive_parse_handles_pipe() {
        let p = bash_command_patterns("bash -c 'cat foo | grep bar'");
        assert!(p.contains(&"cat *".to_string()), "{p:?}");
        assert!(p.contains(&"grep *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn bash_arity_thread_local_parser_reused() {
        // Smoke test: call repeatedly. Confirms the thread-local
        // parser is functional under repeated reuse and produces
        // stable output. The first call lazily initialises the
        // thread_local Parser; later calls must hit the cached one
        // without rebuilding `set_language` (idempotent reuse).
        let mut last: Option<Vec<String>> = None;
        for _ in 0..100 {
            let p = bash_command_patterns("git push origin main");
            if let Some(prev) = &last {
                assert_eq!(prev, &p, "results diverged across calls: {p:?}");
            }
            last = Some(p);
        }
        let p = last.expect("at least one iteration");
        assert!(p.contains(&"git push *".to_string()), "{p:?}");
        assert!(p.contains(&"git *".to_string()), "{p:?}");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }

    #[test]
    fn malformed_bash_falls_back_to_heuristic() {
        // Incomplete `if` / `then` / `else` -- tree-sitter still
        // produces a partial tree, but should not panic and should at
        // minimum return something containing `*`.
        let p = bash_command_patterns("if then else");
        assert!(p.contains(&"*".to_string()), "{p:?}");
    }
}
