//! `apply_patch` fake-tool runtime + the apply-patch parser.
//!
//! Step 7 of the kilo-server module split: verbatim cut from `lib.rs`. The
//! `slash` / `resolve_under` path helpers now live in `util::paths`.

use std::{fs, path::Path as FsPath};

use serde_json::{json, Value};

use crate::agent::tools::common::title;
use crate::agent::tools::diff::text_diff;
use crate::util::paths::resolve_under;

#[derive(Clone, Copy)]
pub(crate) enum PatchKind {
    Add,
    Delete,
    Update,
}

pub(crate) struct PatchSection {
    pub(crate) kind: PatchKind,
    pub(crate) path: String,
    pub(crate) lines: Vec<String>,
}

pub(crate) struct PatchResult {
    pub(crate) row: String,
    pub(crate) path: String,
    pub(crate) diff: String,
    pub(crate) additions: usize,
    pub(crate) deletions: usize,
    pub(crate) kind: &'static str,
}

pub(crate) fn fake_apply_patch(
    root: &FsPath,
    input: &Value,
) -> Result<(String, String, Value), String> {
    let text = input
        .get("patchText")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "patchText is required".to_string())?;
    let sections = parse_apply_patch(text)?;
    let mut out = Vec::new();

    for item in sections {
        let target =
            resolve_under(root, &item.path).map_err(|_| format!("Unsafe path: {}", item.path))?;
        let before = fs::read_to_string(&target).unwrap_or_default();
        let (after, kind, row, add, del) = match item.kind {
            PatchKind::Add => {
                if target.exists() {
                    return Err(format!("File already exists: {}", item.path));
                }
                let body = patch_added(&item.lines)?;
                (body, "added", "A", count_lines(&item.lines, '+'), 0)
            }
            PatchKind::Delete => {
                if !target.exists() {
                    return Err(format!("File does not exist: {}", item.path));
                }
                (String::new(), "deleted", "D", 0, before.lines().count())
            }
            PatchKind::Update => {
                if !target.exists() {
                    return Err(format!("File does not exist: {}", item.path));
                }
                let next = patch_updated(&item.path, &before, &item.lines)?;
                (
                    next,
                    "modified",
                    "M",
                    count_lines(&item.lines, '+'),
                    count_lines(&item.lines, '-'),
                )
            }
        };
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("Unable to create {}: {err}", parent.to_string_lossy()))?;
        }
        match item.kind {
            PatchKind::Delete => fs::remove_file(&target)
                .map_err(|err| format!("Unable to delete {}: {err}", target.to_string_lossy()))?,
            _ => fs::write(&target, &after)
                .map_err(|err| format!("Unable to write {}: {err}", target.to_string_lossy()))?,
        }
        out.push(PatchResult {
            row: row.to_string(),
            path: item.path,
            diff: text_diff(&title(root, &target), &before, &after),
            additions: add,
            deletions: del,
            kind,
        });
    }

    let diff = out
        .iter()
        .map(|item| item.diff.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let files = out
        .iter()
        .map(|item| {
            json!({
                "filePath": item.path,
                "relativePath": item.path,
                "type": item.kind,
                "patch": item.diff,
                "additions": item.additions,
                "deletions": item.deletions,
            })
        })
        .collect::<Vec<_>>();
    let rows = out
        .iter()
        .map(|item| format!("{} {}", item.row, item.path))
        .collect::<Vec<_>>()
        .join("\n");
    let output = format!("Success. Updated the following files:\n{rows}");
    let title = out
        .first()
        .map(|item| item.path.clone())
        .unwrap_or_else(|| "apply_patch".to_string());

    // Per-file diagnostics keyed by path. Re-reads each modified file
    // off disk and runs the same heuristic surface used by `write` /
    // `edit` (`agent::diagnostics`). Deleted files contribute nothing.
    let mut diagnostics_map = serde_json::Map::new();
    for item in &out {
        if item.kind == "deleted" {
            continue;
        }
        let target = match resolve_under(root, &item.path) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let content = fs::read_to_string(&target).unwrap_or_default();
        let diags = crate::agent::diagnostics::diagnostics_for(&item.path, &content);
        if !diags.as_array().map(|a| a.is_empty()).unwrap_or(true) {
            diagnostics_map.insert(item.path.clone(), diags);
        }
    }

    Ok((
        title,
        output,
        json!({ "diff": diff, "files": files, "diagnostics": Value::Object(diagnostics_map) }),
    ))
}

pub(crate) fn parse_apply_patch(text: &str) -> Result<Vec<PatchSection>, String> {
    let lines = text.lines().collect::<Vec<_>>();
    if lines.first() != Some(&"*** Begin Patch") {
        return Err("Malformed patch: missing *** Begin Patch".to_string());
    }
    if lines.last() != Some(&"*** End Patch") {
        return Err("Malformed patch: missing *** End Patch".to_string());
    }

    let mut sections = Vec::new();
    let mut idx = 1;
    while idx + 1 < lines.len() {
        let line = lines[idx];
        if line.starts_with("*** Move to:") {
            return Err("Unsupported patch operation: move".to_string());
        }
        let Some((kind, path)) = parse_patch_header(line) else {
            return Err(format!("Malformed patch section: {line}"));
        };
        idx += 1;
        let mut body = Vec::new();
        while idx + 1 < lines.len() && !lines[idx].starts_with("*** ") {
            if !lines[idx].starts_with("@@") {
                body.push(lines[idx].to_string());
            }
            idx += 1;
        }
        if path.is_empty() {
            return Err("Malformed patch: empty path".to_string());
        }
        sections.push(PatchSection {
            kind,
            path,
            lines: body,
        });
    }
    if sections.is_empty() {
        return Err("Malformed patch: no sections".to_string());
    }
    Ok(sections)
}

fn parse_patch_header(line: &str) -> Option<(PatchKind, String)> {
    if let Some(path) = line.strip_prefix("*** Add File: ") {
        return Some((PatchKind::Add, path.to_string()));
    }
    if let Some(path) = line.strip_prefix("*** Delete File: ") {
        return Some((PatchKind::Delete, path.to_string()));
    }
    if let Some(path) = line.strip_prefix("*** Update File: ") {
        return Some((PatchKind::Update, path.to_string()));
    }
    None
}

pub(crate) fn patch_added(lines: &[String]) -> Result<String, String> {
    let mut out = Vec::new();
    for line in lines {
        let Some(text) = line.strip_prefix('+') else {
            return Err("Malformed add patch: expected + lines".to_string());
        };
        out.push(text);
    }
    Ok(join_patch_lines(&out))
}

pub(crate) fn patch_updated(path: &str, before: &str, lines: &[String]) -> Result<String, String> {
    let old = before.lines().collect::<Vec<_>>();
    let mut idx = 0;
    let mut out = Vec::new();
    for line in lines {
        let Some(tag) = line.chars().next() else {
            return Err("Malformed update patch: empty hunk line".to_string());
        };
        let text = &line[1..];
        match tag {
            ' ' => {
                while old.get(idx).copied() != Some(text) {
                    let Some(next) = old.get(idx) else {
                        return Err(format!("Patch context mismatch in {path}: {text}"));
                    };
                    out.push(*next);
                    idx += 1;
                }
                out.push(text);
                idx += 1;
            }
            '-' => {
                while old.get(idx).copied() != Some(text) {
                    let Some(next) = old.get(idx) else {
                        return Err(format!("Patch remove mismatch in {path}: {text}"));
                    };
                    out.push(*next);
                    idx += 1;
                }
                idx += 1;
            }
            '+' => out.push(text),
            _ => return Err(format!("Malformed update patch line: {line}")),
        }
    }
    out.extend(old.iter().skip(idx).copied());
    Ok(join_patch_lines(&out))
}

pub(crate) fn join_patch_lines(lines: &[&str]) -> String {
    if lines.is_empty() {
        return String::new();
    }
    format!("{}\n", lines.join("\n"))
}

pub(crate) fn count_lines(lines: &[String], prefix: char) -> usize {
    lines.iter().filter(|line| line.starts_with(prefix)).count()
}
