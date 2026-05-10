//! `apply_patch` fake-tool runtime + the apply-patch parser.
//!
//! Bun-parity parser for the `*** Begin Patch ... *** End Patch` format
//! (`packages/opencode/src/patch/index.ts`). Supports `*** Add File:`,
//! `*** Update File:`, `*** Delete File:`, `*** Move to:` rename headers,
//! multiple `@@ <ctx>` chunks per Update section, the `*** End of File`
//! anchor, and UTF-8 BOM round-tripping.

use std::{fs, path::Path as FsPath, sync::Arc};

use serde_json::{json, Value};

use crate::agent::permission::{ask_external_directory, PermissionError};
use crate::agent::tools::common::title;
use crate::agent::tools::diff::text_diff;
use crate::util::paths::{resolve_relaxed, resolve_under};
use crate::AppState;

const UTF8_BOM: [u8; 3] = [0xef, 0xbb, 0xbf];

#[derive(Clone, Copy)]
pub(crate) enum PatchKind {
    Add,
    Delete,
    Update,
}

/// Legacy flat row used by `common::patch_patterns` to harvest the file
/// paths an apply_patch call will touch (for permission scoping).
/// Carries no body — the rich body lives on `FileOp`.
pub(crate) struct PatchSection {
    #[allow(dead_code)]
    pub(crate) kind: PatchKind,
    pub(crate) path: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum FileOp {
    Add {
        path: String,
        content: String,
    },
    Delete {
        path: String,
    },
    Update {
        path: String,
        move_to: Option<String>,
        hunks: Vec<Hunk>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Hunk {
    pub(crate) context: Option<String>,
    pub(crate) anchor: HunkAnchor,
    pub(crate) lines: Vec<HunkLine>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum HunkAnchor {
    Context,
    EndOfFile,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HunkLine {
    Add(String),
    Remove(String),
    Keep(String),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PatchError {
    pub(crate) kind: PatchErrorKind,
    pub(crate) file: Option<String>,
    pub(crate) message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PatchErrorKind {
    MissingBeginMarker,
    MissingEndMarker,
    EmptyPatch,
    EmptyPath,
    InvalidHunkBody,
    UnsafePath,
    FileExists,
    FileMissing,
    MoveDestExists,
    ChunkContextNotFound,
    Io,
}

impl PatchError {
    fn new(kind: PatchErrorKind, message: impl Into<String>) -> Self {
        PatchError {
            kind,
            file: None,
            message: message.into(),
        }
    }

    fn with_file(mut self, file: impl Into<String>) -> Self {
        self.file = Some(file.into());
        self
    }

    fn kind_str(&self) -> &'static str {
        match self.kind {
            PatchErrorKind::MissingBeginMarker => "MissingBeginMarker",
            PatchErrorKind::MissingEndMarker => "MissingEndMarker",
            PatchErrorKind::EmptyPatch => "EmptyPatch",
            PatchErrorKind::EmptyPath => "EmptyPath",
            PatchErrorKind::InvalidHunkBody => "InvalidHunkBody",
            PatchErrorKind::UnsafePath => "UnsafePath",
            PatchErrorKind::FileExists => "FileExists",
            PatchErrorKind::FileMissing => "FileMissing",
            PatchErrorKind::MoveDestExists => "MoveDestExists",
            PatchErrorKind::ChunkContextNotFound => "ChunkContextNotFound",
            PatchErrorKind::Io => "IoError",
        }
    }

    /// Render as a flat string for the existing tool-error pipeline.
    /// Format: `<Kind>: <message>` (with the file inlined into message
    /// already where useful) so existing assertions like
    /// `error.contains("mismatch")` continue to fire.
    pub(crate) fn to_display(&self) -> String {
        match (&self.file, self.kind) {
            (Some(file), PatchErrorKind::UnsafePath) => format!("Unsafe path: {file}"),
            (Some(file), _) => format!("{}: {} ({})", self.kind_str(), self.message, file),
            (None, _) => format!("{}: {}", self.kind_str(), self.message),
        }
    }

    #[cfg(test)]
    pub(crate) fn to_value(&self) -> Value {
        json!({
            "kind": self.kind_str(),
            "file": self.file,
            "message": self.message,
        })
    }
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
    fake_apply_patch_inner(root, input, false)
}

fn fake_apply_patch_inner(
    root: &FsPath,
    input: &Value,
    allow_external: bool,
) -> Result<(String, String, Value), String> {
    let text = input
        .get("patchText")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "patchText is required".to_string())?;

    let ops = parse_patch(text).map_err(|err| err.to_display())?;

    let mut out = Vec::new();
    for op in ops {
        let r = apply_op(root, &op, allow_external).map_err(|err| err.to_display())?;
        out.push(r);
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

    let mut diagnostics_map = serde_json::Map::new();
    for item in &out {
        if item.kind == "deleted" {
            continue;
        }
        let target = if allow_external {
            match resolve_relaxed(root, &item.path) {
                Ok(p) => p,
                Err(_) => continue,
            }
        } else {
            match resolve_under(root, &item.path) {
                Ok(p) => p,
                Err(_) => continue,
            }
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

/// Gated apply_patch: kind = "write". Aggregates source paths AND
/// `*** Move to:` destinations, then raises a single ask covering all
/// externals. On approval, the inner applier runs with `allow_external`
/// so individual hunks can write to approved out-of-tree paths.
pub(crate) async fn fake_apply_patch_gated(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    root: &FsPath,
    input: &Value,
) -> Result<(String, String, Value), String> {
    let text = input.get("patchText").and_then(Value::as_str).unwrap_or("");
    if !text.is_empty() {
        // Only collect candidate paths — parser failure here is not
        // fatal; the inner applier will surface the structured parse
        // error through its existing path.
        let candidates: Vec<String> = match parse_patch(text) {
            Ok(ops) => {
                let mut out = Vec::new();
                for op in ops {
                    match op {
                        FileOp::Add { path, .. } | FileOp::Delete { path } => out.push(path),
                        FileOp::Update { path, move_to, .. } => {
                            out.push(path);
                            if let Some(dest) = move_to {
                                out.push(dest);
                            }
                        }
                    }
                }
                out
            }
            Err(_) => Vec::new(),
        };
        if !candidates.is_empty() {
            ask_external_directory(state, sid, mid, pid, idx, root, &candidates, "write")
                .await
                .map_err(|err: PermissionError| err.to_display())?;
        }
    }
    fake_apply_patch_inner(root, input, true)
}

fn apply_op(root: &FsPath, op: &FileOp, allow_external: bool) -> Result<PatchResult, PatchError> {
    match op {
        FileOp::Add { path, content } => apply_add(root, path, content, allow_external),
        FileOp::Delete { path } => apply_delete(root, path, allow_external),
        FileOp::Update {
            path,
            move_to,
            hunks,
        } => apply_update(root, path, move_to.as_deref(), hunks, allow_external),
    }
}

fn resolve_patch_path(
    root: &FsPath,
    path: &str,
    allow_external: bool,
) -> Result<std::path::PathBuf, PatchError> {
    if allow_external {
        resolve_relaxed(root, path).map_err(|_| unsafe_path_error(path))
    } else {
        resolve_under(root, path).map_err(|_| unsafe_path_error(path))
    }
}

fn apply_add(
    root: &FsPath,
    path: &str,
    content: &str,
    allow_external: bool,
) -> Result<PatchResult, PatchError> {
    let target = resolve_patch_path(root, path, allow_external)?;
    if target.exists() {
        return Err(PatchError::new(
            PatchErrorKind::FileExists,
            format!("File already exists: {path}"),
        )
        .with_file(path));
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|err| io_error(path, err))?;
    }
    fs::write(&target, content).map_err(|err| io_error(path, err))?;
    Ok(PatchResult {
        row: "A".to_string(),
        path: path.to_string(),
        diff: text_diff(&title(root, &target), "", content),
        additions: count_newline_lines(content),
        deletions: 0,
        kind: "added",
    })
}

fn apply_delete(
    root: &FsPath,
    path: &str,
    allow_external: bool,
) -> Result<PatchResult, PatchError> {
    let target = resolve_patch_path(root, path, allow_external)?;
    if !target.exists() {
        return Err(PatchError::new(
            PatchErrorKind::FileMissing,
            format!("File does not exist: {path}"),
        )
        .with_file(path));
    }
    let before_bytes = fs::read(&target).map_err(|err| io_error(path, err))?;
    let (before, _) = strip_utf8_bom(&before_bytes);
    let deletions = before.lines().count();
    fs::remove_file(&target).map_err(|err| io_error(path, err))?;
    Ok(PatchResult {
        row: "D".to_string(),
        path: path.to_string(),
        diff: text_diff(&title(root, &target), &before, ""),
        additions: 0,
        deletions,
        kind: "deleted",
    })
}

fn apply_update(
    root: &FsPath,
    path: &str,
    move_to: Option<&str>,
    hunks: &[Hunk],
    allow_external: bool,
) -> Result<PatchResult, PatchError> {
    let source = resolve_patch_path(root, path, allow_external)?;
    if !source.exists() {
        return Err(PatchError::new(
            PatchErrorKind::FileMissing,
            format!("File does not exist: {path}"),
        )
        .with_file(path));
    }

    let raw = fs::read(&source).map_err(|err| io_error(path, err))?;
    let (before, had_bom) = strip_utf8_bom(&raw);
    let after = apply_hunks(path, &before, hunks)?;

    let (additions, deletions) = hunk_stats(hunks);

    let final_path = move_to.unwrap_or(path);
    let target = resolve_patch_path(root, final_path, allow_external)?;

    if let Some(dest_str) = move_to {
        if target.exists() && target != source {
            return Err(PatchError::new(
                PatchErrorKind::MoveDestExists,
                format!("Move destination already exists: {dest_str}"),
            )
            .with_file(dest_str));
        }
    }

    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(|err| io_error(final_path, err))?;
    }

    let mtime = fs::metadata(&source).and_then(|m| m.modified()).ok();

    let mut bytes = Vec::with_capacity(after.len() + 3);
    if had_bom {
        bytes.extend_from_slice(&UTF8_BOM);
    }
    bytes.extend_from_slice(after.as_bytes());
    fs::write(&target, &bytes).map_err(|err| io_error(final_path, err))?;

    if let (Some(time), true) = (mtime, move_to.is_some() || had_bom) {
        // Best-effort mtime preservation; ignore failures.
        let _ = filetime_set(&target, time);
    }

    if move_to.is_some() && target != source {
        fs::remove_file(&source).map_err(|err| io_error(path, err))?;
    }

    Ok(PatchResult {
        row: "M".to_string(),
        path: final_path.to_string(),
        diff: text_diff(&title(root, &target), &before, &after),
        additions,
        deletions,
        kind: "modified",
    })
}

fn filetime_set(path: &FsPath, time: std::time::SystemTime) -> std::io::Result<()> {
    let file = fs::OpenOptions::new().write(true).open(path)?;
    file.set_modified(time)
}

fn unsafe_path_error(path: &str) -> PatchError {
    PatchError::new(PatchErrorKind::UnsafePath, format!("Unsafe path: {path}")).with_file(path)
}

fn io_error(path: &str, err: std::io::Error) -> PatchError {
    PatchError::new(PatchErrorKind::Io, err.to_string()).with_file(path)
}

fn strip_utf8_bom(bytes: &[u8]) -> (String, bool) {
    if bytes.starts_with(&UTF8_BOM) {
        let text = String::from_utf8_lossy(&bytes[UTF8_BOM.len()..]).into_owned();
        (text, true)
    } else {
        (String::from_utf8_lossy(bytes).into_owned(), false)
    }
}

fn count_newline_lines(content: &str) -> usize {
    if content.is_empty() {
        return 0;
    }
    content.lines().count()
}

fn hunk_stats(hunks: &[Hunk]) -> (usize, usize) {
    let mut adds = 0usize;
    let mut dels = 0usize;
    for hunk in hunks {
        for line in &hunk.lines {
            match line {
                HunkLine::Add(_) => adds += 1,
                HunkLine::Remove(_) => dels += 1,
                HunkLine::Keep(_) => {}
            }
        }
    }
    (adds, dels)
}

// ---------- Parser ----------

pub(crate) fn parse_patch(text: &str) -> Result<Vec<FileOp>, PatchError> {
    let lines: Vec<&str> = text.lines().collect();
    if lines.first().map(|s| s.trim()) != Some("*** Begin Patch") {
        return Err(PatchError::new(
            PatchErrorKind::MissingBeginMarker,
            "Malformed patch: missing *** Begin Patch",
        ));
    }
    let end_idx = lines
        .iter()
        .rposition(|line| line.trim() == "*** End Patch")
        .ok_or_else(|| {
            PatchError::new(
                PatchErrorKind::MissingEndMarker,
                "Malformed patch: missing *** End Patch",
            )
        })?;

    let mut ops = Vec::new();
    let mut i = 1;
    while i < end_idx {
        let line = lines[i];
        if let Some(rest) = line.strip_prefix("*** Add File:") {
            let path = rest.trim().to_string();
            if path.is_empty() {
                return Err(PatchError::new(
                    PatchErrorKind::EmptyPath,
                    "Malformed patch: empty path",
                ));
            }
            i += 1;
            let (content, next) = parse_add_body(&lines, i, end_idx);
            ops.push(FileOp::Add { path, content });
            i = next;
            continue;
        }
        if let Some(rest) = line.strip_prefix("*** Delete File:") {
            let path = rest.trim().to_string();
            if path.is_empty() {
                return Err(PatchError::new(
                    PatchErrorKind::EmptyPath,
                    "Malformed patch: empty path",
                ));
            }
            ops.push(FileOp::Delete { path });
            i += 1;
            continue;
        }
        if let Some(rest) = line.strip_prefix("*** Update File:") {
            let path = rest.trim().to_string();
            if path.is_empty() {
                return Err(PatchError::new(
                    PatchErrorKind::EmptyPath,
                    "Malformed patch: empty path",
                ));
            }
            i += 1;
            let mut move_to: Option<String> = None;
            if i < end_idx {
                if let Some(rest) = lines[i].strip_prefix("*** Move to:") {
                    let dest = rest.trim();
                    if dest.is_empty() {
                        return Err(PatchError::new(
                            PatchErrorKind::EmptyPath,
                            "Malformed patch: empty *** Move to: path",
                        )
                        .with_file(&path));
                    }
                    move_to = Some(dest.to_string());
                    i += 1;
                }
            }
            let (hunks, next) = parse_update_hunks(&lines, i, end_idx, &path)?;
            ops.push(FileOp::Update {
                path,
                move_to,
                hunks,
            });
            i = next;
            continue;
        }
        // Lines outside section headers are ignored (matches Bun's
        // tolerant scan between Begin/End markers).
        i += 1;
    }

    if ops.is_empty() {
        return Err(PatchError::new(
            PatchErrorKind::EmptyPatch,
            "Malformed patch: no sections",
        ));
    }
    Ok(ops)
}

fn parse_add_body(lines: &[&str], start: usize, end: usize) -> (String, usize) {
    let mut out = String::new();
    let mut i = start;
    while i < end {
        let line = lines[i];
        if line.starts_with("*** ") {
            break;
        }
        if let Some(rest) = line.strip_prefix('+') {
            out.push_str(rest);
            out.push('\n');
        }
        i += 1;
    }
    (out, i)
}

fn parse_update_hunks(
    lines: &[&str],
    start: usize,
    end: usize,
    path: &str,
) -> Result<(Vec<Hunk>, usize), PatchError> {
    let mut hunks: Vec<Hunk> = Vec::new();
    let mut i = start;
    let mut current: Option<Hunk> = None;

    // Implicit hunk: Bun tolerates an Update body that omits the leading
    // `@@` and starts directly with ` `/`+`/`-`. Keep parity by lazily
    // opening a default-context hunk on the first body line.
    let open_default = |hunks: &mut Vec<Hunk>, current: &mut Option<Hunk>| {
        if current.is_none() {
            *current = Some(Hunk {
                context: None,
                anchor: HunkAnchor::Context,
                lines: Vec::new(),
            });
        }
        let _ = hunks;
    };

    while i < end {
        let line = lines[i];
        if line.starts_with("*** ") && line != "*** End of File" {
            break;
        }
        if let Some(rest) = line.strip_prefix("@@") {
            if let Some(h) = current.take() {
                hunks.push(h);
            }
            let ctx = rest.trim();
            current = Some(Hunk {
                context: if ctx.is_empty() {
                    None
                } else {
                    Some(ctx.to_string())
                },
                anchor: HunkAnchor::Context,
                lines: Vec::new(),
            });
            i += 1;
            continue;
        }
        if line == "*** End of File" {
            open_default(&mut hunks, &mut current);
            if let Some(h) = current.as_mut() {
                h.anchor = HunkAnchor::EndOfFile;
            }
            i += 1;
            continue;
        }
        if line.is_empty() {
            // Blank line inside a hunk body is treated as a Keep "" line
            // for parity with patches generated by tools that do not
            // prefix blank context lines with a space.
            open_default(&mut hunks, &mut current);
            if let Some(h) = current.as_mut() {
                h.lines.push(HunkLine::Keep(String::new()));
            }
            i += 1;
            continue;
        }

        let first = line.chars().next().unwrap();
        let body = &line[1..];
        let entry = match first {
            ' ' => HunkLine::Keep(body.to_string()),
            '+' => HunkLine::Add(body.to_string()),
            '-' => HunkLine::Remove(body.to_string()),
            _ => {
                return Err(PatchError::new(
                    PatchErrorKind::InvalidHunkBody,
                    format!("Malformed update patch line: {line}"),
                )
                .with_file(path));
            }
        };
        open_default(&mut hunks, &mut current);
        if let Some(h) = current.as_mut() {
            h.lines.push(entry);
        }
        i += 1;
    }

    if let Some(h) = current.take() {
        hunks.push(h);
    }
    Ok((hunks, i))
}

// ---------- Applier ----------

fn apply_hunks(path: &str, before: &str, hunks: &[Hunk]) -> Result<String, PatchError> {
    if hunks.is_empty() {
        return Ok(before.to_string());
    }

    let mut lines: Vec<String> = before.lines().map(|s| s.to_string()).collect();
    let trailing_nl = before.ends_with('\n') || before.is_empty();

    let mut cursor: usize = 0;
    for hunk in hunks {
        cursor = apply_one_hunk(path, &mut lines, hunk, cursor)?;
    }

    let mut out = lines.join("\n");
    if trailing_nl && !out.is_empty() {
        out.push('\n');
    } else if !trailing_nl && before.is_empty() && !out.is_empty() {
        out.push('\n');
    }
    Ok(out)
}

fn apply_one_hunk(
    path: &str,
    lines: &mut Vec<String>,
    hunk: &Hunk,
    start: usize,
) -> Result<usize, PatchError> {
    // Build the old-pattern (Keep + Remove) and new-pattern (Keep + Add).
    let pattern: Vec<&String> = hunk
        .lines
        .iter()
        .filter_map(|l| match l {
            HunkLine::Keep(s) | HunkLine::Remove(s) => Some(s),
            HunkLine::Add(_) => None,
        })
        .collect();
    let new_lines: Vec<String> = hunk
        .lines
        .iter()
        .filter_map(|l| match l {
            HunkLine::Keep(s) | HunkLine::Add(s) => Some(s.clone()),
            HunkLine::Remove(_) => None,
        })
        .collect();

    // Pure addition: no Keep / Remove lines — splice in at cursor (or
    // EOF if anchored).
    if pattern.is_empty() {
        let insert_at = if matches!(hunk.anchor, HunkAnchor::EndOfFile) {
            lines.len()
        } else {
            start.min(lines.len())
        };
        for (offset, value) in new_lines.iter().enumerate() {
            lines.insert(insert_at + offset, value.clone());
        }
        return Ok(insert_at + new_lines.len());
    }

    let pattern_owned: Vec<String> = pattern.into_iter().cloned().collect();

    // Optional context anchor: scan for a unique prefix line ahead of
    // the pattern to disambiguate non-unique hunks.
    let scan_from = if let Some(ctx) = &hunk.context {
        match find_line(lines, ctx, start) {
            Some(idx) => idx + 1,
            None => {
                return Err(PatchError::new(
                    PatchErrorKind::ChunkContextNotFound,
                    format!("Failed to find context '{ctx}' in {path}"),
                )
                .with_file(path));
            }
        }
    } else {
        start
    };

    let found = match hunk.anchor {
        HunkAnchor::EndOfFile => {
            if pattern_owned.len() > lines.len() {
                None
            } else {
                let tail = lines.len() - pattern_owned.len();
                if tail >= scan_from && slice_eq(lines, tail, &pattern_owned) {
                    Some(tail)
                } else {
                    None
                }
            }
        }
        HunkAnchor::Context => find_subsequence(lines, &pattern_owned, scan_from),
    };

    let Some(idx) = found else {
        return Err(PatchError::new(
            PatchErrorKind::ChunkContextNotFound,
            format!(
                "Patch context mismatch in {path}: {}",
                pattern_owned.join("\n")
            ),
        )
        .with_file(path));
    };

    lines.splice(idx..idx + pattern_owned.len(), new_lines.iter().cloned());
    Ok(idx + new_lines.len())
}

fn slice_eq(lines: &[String], start: usize, pattern: &[String]) -> bool {
    if start + pattern.len() > lines.len() {
        return false;
    }
    for (offset, want) in pattern.iter().enumerate() {
        if &lines[start + offset] != want {
            return false;
        }
    }
    true
}

fn find_subsequence(lines: &[String], pattern: &[String], start: usize) -> Option<usize> {
    if pattern.is_empty() || pattern.len() > lines.len() {
        return None;
    }
    let limit = lines.len() - pattern.len();
    for i in start..=limit {
        if slice_eq(lines, i, pattern) {
            return Some(i);
        }
    }
    // Fall back: ignore start cursor and rescan from 0 (matches Bun's
    // forgiving seek when the context appears earlier in the file).
    if start > 0 {
        for i in 0..=limit {
            if slice_eq(lines, i, pattern) {
                return Some(i);
            }
        }
    }
    None
}

fn find_line(lines: &[String], needle: &str, start: usize) -> Option<usize> {
    for (i, line) in lines.iter().enumerate().skip(start) {
        if line == needle {
            return Some(i);
        }
    }
    if start > 0 {
        for (i, line) in lines.iter().enumerate() {
            if line == needle {
                return Some(i);
            }
        }
    }
    None
}

// ---------- Legacy shim for `common::patch_patterns` ----------

/// Returns one [`PatchSection`] per write target (source for Update,
/// destination for Move). Used solely to harvest paths for permission
/// scoping; the full body is dropped.
pub(crate) fn parse_apply_patch(text: &str) -> Result<Vec<PatchSection>, String> {
    let ops = parse_patch(text).map_err(|err| err.to_display())?;
    let mut out = Vec::new();
    for op in ops {
        match op {
            FileOp::Add { path, .. } => out.push(PatchSection {
                kind: PatchKind::Add,
                path,
            }),
            FileOp::Delete { path } => out.push(PatchSection {
                kind: PatchKind::Delete,
                path,
            }),
            FileOp::Update { path, move_to, .. } => {
                let final_path = move_to.unwrap_or(path);
                out.push(PatchSection {
                    kind: PatchKind::Update,
                    path: final_path,
                });
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_root(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path = std::env::temp_dir().join(format!("kilo-patch-{label}-{nanos}-{n}"));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn parses_move_to_directive() {
        let text = "*** Begin Patch\n\
                    *** Update File: src/old.txt\n\
                    *** Move to: src/new.txt\n\
                    @@\n\
                    -hello\n\
                    +world\n\
                    *** End Patch";
        let ops = parse_patch(text).expect("parse");
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            FileOp::Update {
                path,
                move_to,
                hunks,
            } => {
                assert_eq!(path, "src/old.txt");
                assert_eq!(move_to.as_deref(), Some("src/new.txt"));
                assert_eq!(hunks.len(), 1);
                assert_eq!(hunks[0].lines.len(), 2);
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn applies_move_to_renames_file_and_preserves_content() {
        let root = temp_root("rename-pure");
        let src = root.join("a.txt");
        let dst_rel = "b.txt";
        fs::write(&src, "x\ny\n").unwrap();
        let text = format!(
            "*** Begin Patch\n*** Update File: a.txt\n*** Move to: {dst_rel}\n*** End Patch"
        );
        let res = fake_apply_patch(&root, &json!({ "patchText": text })).expect("apply");
        let (_title, output, _meta) = res;
        assert!(output.contains("M b.txt"));
        assert!(!src.exists());
        assert_eq!(fs::read_to_string(root.join(dst_rel)).unwrap(), "x\ny\n");
    }

    #[test]
    fn applies_move_with_body_change() {
        let root = temp_root("rename-edit");
        fs::write(root.join("a.txt"), "alpha\nbeta\n").unwrap();
        let text = concat!(
            "*** Begin Patch\n",
            "*** Update File: a.txt\n",
            "*** Move to: b.txt\n",
            "@@\n",
            " alpha\n",
            "-beta\n",
            "+gamma\n",
            "*** End Patch",
        );
        fake_apply_patch(&root, &json!({ "patchText": text })).expect("apply");
        assert!(!root.join("a.txt").exists());
        assert_eq!(
            fs::read_to_string(root.join("b.txt")).unwrap(),
            "alpha\ngamma\n"
        );
    }

    #[test]
    fn move_to_existing_destination_errors() {
        let root = temp_root("rename-conflict");
        fs::write(root.join("a.txt"), "a\n").unwrap();
        fs::write(root.join("b.txt"), "b\n").unwrap();
        let text = "*** Begin Patch\n\
                    *** Update File: a.txt\n\
                    *** Move to: b.txt\n\
                    @@\n\
                    -a\n\
                    +A\n\
                    *** End Patch";
        let err = fake_apply_patch(&root, &json!({ "patchText": text })).unwrap_err();
        assert!(err.contains("MoveDestExists"), "got: {err}");
        assert_eq!(fs::read_to_string(root.join("a.txt")).unwrap(), "a\n");
        assert_eq!(fs::read_to_string(root.join("b.txt")).unwrap(), "b\n");
    }

    #[test]
    fn parses_multi_chunk_update_section() {
        let text = concat!(
            "*** Begin Patch\n",
            "*** Update File: f.txt\n",
            "@@\n",
            " line 1\n",
            "-line 2\n",
            "+LINE 2\n",
            "@@\n",
            " line 3\n",
            "-line 4\n",
            "+LINE 4\n",
            "*** End Patch",
        );
        let ops = parse_patch(text).expect("parse");
        match &ops[0] {
            FileOp::Update { hunks, .. } => {
                assert_eq!(hunks.len(), 2);
                assert_eq!(hunks[0].lines.len(), 3);
                assert_eq!(hunks[1].lines.len(), 3);
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn applies_multi_chunk_to_distinct_regions() {
        let root = temp_root("multi-chunk");
        fs::write(root.join("f.txt"), "line 1\nline 2\nline 3\nline 4\n").unwrap();
        let text = concat!(
            "*** Begin Patch\n",
            "*** Update File: f.txt\n",
            "@@\n",
            " line 1\n",
            "-line 2\n",
            "+LINE 2\n",
            "@@\n",
            " line 3\n",
            "-line 4\n",
            "+LINE 4\n",
            "*** End Patch",
        );
        fake_apply_patch(&root, &json!({ "patchText": text })).expect("apply");
        assert_eq!(
            fs::read_to_string(root.join("f.txt")).unwrap(),
            "line 1\nLINE 2\nline 3\nLINE 4\n"
        );
    }

    #[test]
    fn eof_anchor_matches_at_file_tail() {
        let root = temp_root("eof-match");
        fs::write(root.join("f.txt"), "a\nb\nc\n").unwrap();
        let text = "*** Begin Patch\n\
                    *** Update File: f.txt\n\
                    @@\n\
                    -c\n\
                    +C\n\
                    *** End of File\n\
                    *** End Patch";
        fake_apply_patch(&root, &json!({ "patchText": text })).expect("apply");
        assert_eq!(fs::read_to_string(root.join("f.txt")).unwrap(), "a\nb\nC\n");
    }

    #[test]
    fn eof_anchor_rejects_when_not_at_tail() {
        let root = temp_root("eof-fail");
        fs::write(root.join("f.txt"), "a\nb\nc\nd\n").unwrap();
        let text = "*** Begin Patch\n\
                    *** Update File: f.txt\n\
                    @@\n\
                    -b\n\
                    +B\n\
                    *** End of File\n\
                    *** End Patch";
        let err = fake_apply_patch(&root, &json!({ "patchText": text })).unwrap_err();
        assert!(err.contains("ChunkContextNotFound"), "got: {err}");
        assert_eq!(
            fs::read_to_string(root.join("f.txt")).unwrap(),
            "a\nb\nc\nd\n"
        );
    }

    #[test]
    fn preserves_utf8_bom_on_apply() {
        let root = temp_root("bom");
        let path = root.join("f.txt");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&UTF8_BOM);
        bytes.extend_from_slice(b"alpha\nbeta\n");
        fs::write(&path, &bytes).unwrap();

        let text = concat!(
            "*** Begin Patch\n",
            "*** Update File: f.txt\n",
            "@@\n",
            " alpha\n",
            "-beta\n",
            "+BETA\n",
            "*** End Patch",
        );
        fake_apply_patch(&root, &json!({ "patchText": text })).expect("apply");
        let raw = fs::read(&path).unwrap();
        assert!(raw.starts_with(&UTF8_BOM), "BOM was dropped");
        assert_eq!(&raw[3..], b"alpha\nBETA\n");
    }

    #[test]
    fn chunk_context_not_found_returns_structured_error() {
        let root = temp_root("ctx-miss");
        fs::write(root.join("f.txt"), "one\ntwo\n").unwrap();
        let text = "*** Begin Patch\n\
                    *** Update File: f.txt\n\
                    @@\n\
                    -nonexistent\n\
                    +replacement\n\
                    *** End Patch";
        let err = fake_apply_patch(&root, &json!({ "patchText": text })).unwrap_err();
        assert!(err.contains("ChunkContextNotFound"), "got: {err}");
        assert!(err.contains("f.txt"));
    }

    #[test]
    fn structured_error_payload_round_trips() {
        let err = PatchError::new(PatchErrorKind::MoveDestExists, "exists").with_file("b.txt");
        let v = err.to_value();
        assert_eq!(v["kind"], "MoveDestExists");
        assert_eq!(v["file"], "b.txt");
        assert_eq!(v["message"], "exists");
    }
}
