//! Route handlers for `/vcs` and `/vcs/diff`. Bun parity:
//! `packages/opencode/src/server/routes/instance/index.ts:130` (the `Vcs`
//! service backing both routes lives at
//! `packages/opencode/src/project/vcs.ts:117-145`).
//!
//! The Bun route returns snake_case `default_branch` (`Vcs.Info` zod schema
//! at `project/vcs.ts:120`); preserve that wire shape.
//!
//! Both handlers shell out via [`crate::util::git::git_text`]. On a
//! non-git directory or detached HEAD, `/vcs` returns `{branch: null,
//! default_branch: null}` and `/vcs/diff` returns `[]`.

use std::{
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};

use axum::{extract::State, response::IntoResponse, Json};
use serde_json::{json, Value};

use crate::util::git::git_text;
use crate::AppState;

pub(crate) async fn vcs_branch(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let root = PathBuf::from(state.store.paths().directory);
    Json(json!({
        "branch": current_branch(&root),
        "default_branch": default_branch(&root),
    }))
}

pub(crate) async fn vcs_diff(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let root = PathBuf::from(state.store.paths().directory);
    Json(diff_items(&root))
}

/// `git symbolic-ref --short HEAD` — Bun parity: `Git.branch` at
/// `packages/opencode/src/git/index.ts:145`. Returns `None` for detached
/// HEAD or non-git roots (the porcelain command exits non-zero in both
/// cases, and `git_text` returns `None`).
fn current_branch(root: &FsPath) -> Option<String> {
    let raw = git_text(root, &["symbolic-ref", "--quiet", "--short", "HEAD"])?;
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Default-branch lookup. Mirrors `Git.defaultBranch` at
/// `packages/opencode/src/git/index.ts:158`: prefer
/// `refs/remotes/origin/HEAD`, then fall back to `main`/`master` if
/// they exist locally.
fn default_branch(root: &FsPath) -> Option<String> {
    if let Some(raw) = git_text(root, &["symbolic-ref", "refs/remotes/origin/HEAD"]) {
        let ref_path = raw.trim();
        if let Some(stripped) = ref_path.strip_prefix("refs/remotes/origin/") {
            if !stripped.is_empty() {
                return Some(stripped.to_string());
            }
        }
    }
    let heads = git_text(
        root,
        &["for-each-ref", "--format=%(refname:short)", "refs/heads"],
    )?;
    let names: Vec<&str> = heads
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if names.iter().any(|name| *name == "main") {
        return Some("main".to_string());
    }
    if names.iter().any(|name| *name == "master") {
        return Some("master".to_string());
    }
    None
}

/// Returns `[{file, status, additions, deletions}]` per file — the
/// minimum useful shape for the extension's diff badges. Combines
/// `git status --porcelain` (status classification, includes untracked)
/// with `git diff --numstat HEAD` (numeric add/del). On a non-git root
/// the `git_text` calls fail and we return `[]`.
fn diff_items(root: &FsPath) -> Vec<Value> {
    let status_raw = match git_text(root, &["status", "--porcelain=v1", "--untracked-files=all"]) {
        Some(text) => text,
        None => return Vec::new(),
    };
    let numstat = git_text(root, &["diff", "--numstat", "HEAD"]).unwrap_or_default();
    let mut stats: std::collections::BTreeMap<String, (u64, u64)> =
        std::collections::BTreeMap::new();
    for line in numstat.lines() {
        let mut cols = line.splitn(3, '\t');
        let adds = cols.next().unwrap_or("0");
        let dels = cols.next().unwrap_or("0");
        let file = match cols.next() {
            Some(value) if !value.is_empty() => value.to_string(),
            _ => continue,
        };
        let adds = adds.parse().unwrap_or(0);
        let dels = dels.parse().unwrap_or(0);
        stats.insert(file, (adds, dels));
    }
    let mut out = Vec::new();
    for line in status_raw.lines() {
        if line.len() < 4 {
            continue;
        }
        let code = &line[..2];
        let file = line[3..].trim().trim_matches('"');
        if file.is_empty() {
            continue;
        }
        let status = classify(code);
        let (additions, deletions) = stats.get(file).copied().unwrap_or((0, 0));
        out.push(json!({
            "file": file,
            "status": status,
            "additions": additions,
            "deletions": deletions,
        }));
    }
    out
}

/// Mirrors `Git.kind` (`packages/opencode/src/git/index.ts`): the porcelain
/// status code's first non-space char wins, with `??` -> "added" and
/// rename codes mapped to "renamed".
fn classify(code: &str) -> &'static str {
    let bytes = code.as_bytes();
    let primary = bytes.first().copied().unwrap_or(b' ');
    let secondary = bytes.get(1).copied().unwrap_or(b' ');
    if primary == b'?' && secondary == b'?' {
        return "added";
    }
    if primary == b'R' || secondary == b'R' {
        return "renamed";
    }
    let pick = if primary != b' ' { primary } else { secondary };
    match pick {
        b'A' => "added",
        b'D' => "deleted",
        b'M' | b'T' | b'U' => "modified",
        b'C' => "added",
        _ => "modified",
    }
}
