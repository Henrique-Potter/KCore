//! Route handlers for /find* and /file*. The file-search helpers
//! (search_files, search_text, search_symbols, walk, is_binary, etc.) live
//! alongside their handlers; some (list_nodes, is_binary, search_text_target)
//! are also reachable from the agent fake-tool helpers via a re-export at
//! lib.rs scope.

use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader, Read},
    path::{Path as FsPath, PathBuf},
    sync::{atomic::AtomicBool, Arc},
};

/// Cancel cadence for the per-file streaming grep loop. Mirrors the
/// constant in `agent::tools::fs` — checking the atomic on every line
/// is wasteful; every 256 lines is a reasonable bound on stalled aborts.
const GREP_CANCEL_CADENCE: usize = 256;
/// Bytes peeked off the front of a file to decide whether it's binary
/// without loading the whole thing into memory.
const GREP_BINARY_SNIFF_BYTES: usize = 8192;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};

use crate::util::git::{git_count, git_text, line_count};
use crate::util::paths::{resolve_under, slash};
use crate::AppState;

pub(crate) async fn find_text(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(pattern) = query.get("pattern").filter(|value| !value.is_empty()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let root = PathBuf::from(state.store.paths().directory);
    Json(search_text(&root, pattern, 10)).into_response()
}

pub(crate) async fn find_file(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(term) = query.get("query") else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10)
        .clamp(1, 200);
    let dirs = query.get("dirs").is_none_or(|value| value != "false");
    let kind = query.get("type").map(String::as_str);
    let root = PathBuf::from(state.store.paths().directory);

    Json(search_files(&root, term, dirs, kind, limit)).into_response()
}

pub(crate) async fn find_symbol(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(term) = query.get("query") else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let root = PathBuf::from(state.store.paths().directory);

    Json(search_symbols(&root, term, 10)).into_response()
}

pub(crate) async fn list_file(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(input) = query.get("path") else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let root = PathBuf::from(state.store.paths().directory);
    let dir = match resolve_under(&root, input) {
        Ok(path) => path,
        Err(code) => return code.into_response(),
    };

    Json(list_nodes(&root, &dir)).into_response()
}

pub(crate) async fn file_content(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(input) = query.get("path") else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let root = PathBuf::from(state.store.paths().directory);
    let file = match resolve_under(&root, input) {
        Ok(path) => path,
        Err(code) => return code.into_response(),
    };

    Json(read_content(&file)).into_response()
}

pub(crate) async fn file_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let root = PathBuf::from(state.store.paths().directory);
    Json(git_status(&root))
}

pub(crate) fn list_nodes(root: &FsPath, dir: &FsPath) -> Vec<Value> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out = entries
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name() != ".git" && entry.file_name() != ".DS_Store")
        .filter_map(|entry| {
            let path = entry.path();
            let meta = entry.metadata().ok()?;
            let kind = if meta.is_dir() { "directory" } else { "file" };
            let name = entry.file_name().to_string_lossy().to_string();
            let rel = slash(path.strip_prefix(root).ok()?);
            Some(json!({
                "name": name,
                "path": rel,
                "absolute": path.to_string_lossy(),
                "type": kind,
                "ignored": false,
            }))
        })
        .collect::<Vec<_>>();

    out.sort_by(|a, b| {
        let at = a["type"].as_str().unwrap_or_default();
        let bt = b["type"].as_str().unwrap_or_default();
        if at != bt {
            return if at == "directory" {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            };
        }

        a["name"]
            .as_str()
            .unwrap_or_default()
            .cmp(b["name"].as_str().unwrap_or_default())
    });
    out
}

pub(crate) fn read_content(file: &FsPath) -> Value {
    let bytes = match fs::read(file) {
        Ok(bytes) => bytes,
        Err(_) => return json!({ "type": "text", "content": "" }),
    };
    if is_binary(file, &bytes) {
        return json!({ "type": "binary", "content": "" });
    }

    let content = String::from_utf8_lossy(&bytes).trim().to_string();
    json!({ "type": "text", "content": content })
}

pub(crate) fn search_files(
    root: &FsPath,
    term: &str,
    dirs: bool,
    kind: Option<&str>,
    limit: usize,
) -> Vec<String> {
    let term = term.to_lowercase();
    let mut out = Vec::new();
    walk(root, root, &mut |path, meta| {
        if out.len() >= limit {
            return false;
        }
        let is_dir = meta.is_dir();
        let include = match kind {
            Some("file") => !is_dir,
            Some("directory") => is_dir,
            _ => !is_dir || dirs,
        };
        if include
            && slash(path.strip_prefix(root).unwrap_or(path))
                .to_lowercase()
                .contains(&term)
        {
            out.push(slash(path.strip_prefix(root).unwrap_or(path)));
        }
        true
    });
    out
}

pub(crate) fn search_text(root: &FsPath, pattern: &str, limit: usize) -> Vec<Value> {
    search_text_target(root, root, pattern, limit)
}

pub(crate) fn search_symbols(root: &FsPath, term: &str, limit: usize) -> Vec<Value> {
    let term = term.to_lowercase();
    let mut out = Vec::new();
    walk(root, root, &mut |path, meta| {
        if out.len() >= limit {
            return false;
        }
        if meta.is_dir() || !is_text_source(path) {
            return true;
        }
        collect_symbols(root, path, &term, limit, &mut out);
        true
    });
    out
}

pub(crate) fn collect_symbols(
    root: &FsPath,
    path: &FsPath,
    term: &str,
    limit: usize,
    out: &mut Vec<Value>,
) {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(_) => return,
    };
    if is_binary(path, &bytes) {
        return;
    }
    let text = String::from_utf8_lossy(&bytes);
    let uri = format!("file://{}", slash(path));
    for (idx, line) in text.lines().enumerate() {
        if out.len() >= limit {
            return;
        }
        let Some((name, kind, col)) = symbol_line(line) else {
            continue;
        };
        if !term.is_empty() && !name.to_lowercase().contains(term) {
            continue;
        }
        out.push(json!({
            "name": name,
            "kind": kind,
            "location": {
                "uri": uri,
                "range": range(idx, col, col + name.len()),
            },
            "path": slash(path.strip_prefix(root).unwrap_or(path)),
        }));
    }
}

pub(crate) fn symbol_line(line: &str) -> Option<(String, u8, usize)> {
    let rules: &[(&str, u8)] = &[
        ("fn ", 12),
        ("function ", 12),
        ("class ", 5),
        ("interface ", 11),
        ("struct ", 23),
        ("enum ", 10),
        ("const ", 14),
        ("let ", 13),
    ];
    let trimmed = line.trim_start();
    let mut base = line.len() - trimmed.len();
    let trimmed = trimmed
        .strip_prefix("pub ")
        .inspect(|_| base += 4)
        .unwrap_or(trimmed);
    for (prefix, kind) in rules {
        let Some(rest) = trimmed.strip_prefix(prefix) else {
            continue;
        };
        let name = symbol_name(rest)?;
        return Some((name.to_string(), *kind, base + prefix.len()));
    }
    None
}

pub(crate) fn symbol_name(input: &str) -> Option<&str> {
    let end = input
        .char_indices()
        .find_map(|(idx, ch)| (!is_symbol_char(ch)).then_some(idx))
        .unwrap_or(input.len());
    (end > 0).then_some(&input[..end])
}

pub(crate) fn is_symbol_char(ch: char) -> bool {
    ch == '_' || ch == '$' || ch.is_ascii_alphanumeric()
}

pub(crate) fn range(line: usize, start: usize, end: usize) -> Value {
    json!({
        "start": { "line": line, "character": start },
        "end": { "line": line, "character": end },
    })
}

pub(crate) fn search_text_target(
    root: &FsPath,
    target: &FsPath,
    pattern: &str,
    limit: usize,
) -> Vec<Value> {
    search_text_target_cancel(root, target, pattern, limit, None)
}

/// Cancel-aware variant of [`search_text_target`]. The agent dispatch
/// path passes `Some(cancel)`; HTTP route handlers without a runner
/// context pass `None` and observe the same legacy behavior. Cancel is
/// checked between scanned files and at every line during a single
/// file's scan.
pub(crate) fn search_text_target_cancel(
    root: &FsPath,
    target: &FsPath,
    pattern: &str,
    limit: usize,
    cancel: Option<&AtomicBool>,
) -> Vec<Value> {
    let mut out = Vec::new();
    if target.is_file() {
        collect_text_matches_cancel(root, target, pattern, limit, &mut out, cancel);
        return out;
    }

    walk_cancel(
        root,
        target,
        &mut |path, meta| {
            if out.len() >= limit {
                return false;
            }
            if meta.is_dir() {
                return true;
            }
            collect_text_matches_cancel(root, path, pattern, limit, &mut out, cancel);
            true
        },
        cancel,
    );
    out
}

/// Stream-scan `path` for `pattern` line-by-line. Memory is bounded by
/// the per-line buffer (one `Vec<u8>` reused across lines) plus
/// whatever the caller's `out` accumulator already holds. The legacy
/// implementation read the whole file into a `Vec<u8>` before scanning
/// — pathological for multi-GB log files. Cancel is observed every
/// `GREP_CANCEL_CADENCE` lines and after each match push.
///
/// Early exit: returns as soon as `out.len() >= limit` so a single
/// huge file can't keep scanning past the caller's cap.
pub(crate) fn collect_text_matches_cancel(
    root: &FsPath,
    path: &FsPath,
    pattern: &str,
    limit: usize,
    out: &mut Vec<Value>,
    cancel: Option<&AtomicBool>,
) {
    if cancel.is_some_and(crate::agent::is_canceled) {
        return;
    }
    if out.len() >= limit {
        return;
    }

    // Sniff the first 8 KB to decide binary without loading the file.
    let mut sniff_handle = match std::fs::File::open(path) {
        Ok(handle) => handle,
        Err(_) => return,
    };
    let mut sniff = vec![0u8; GREP_BINARY_SNIFF_BYTES];
    let read_n = sniff_handle.read(&mut sniff).unwrap_or(0);
    sniff.truncate(read_n);
    if is_binary(path, &sniff) {
        return;
    }
    drop(sniff_handle);

    let handle = match std::fs::File::open(path) {
        Ok(handle) => handle,
        Err(_) => return,
    };
    let mut reader = BufReader::new(handle);
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    let mut idx = 0usize;
    loop {
        if out.len() >= limit {
            return;
        }
        if idx % GREP_CANCEL_CADENCE == 0 && cancel.is_some_and(crate::agent::is_canceled) {
            return;
        }
        buf.clear();
        let Ok(n) = reader.read_until(b'\n', &mut buf) else {
            return;
        };
        if n == 0 {
            return;
        }
        // Strip trailing \r?\n to match prior `text.lines()` semantics
        // (the persisted match text intentionally re-adds the trailing
        // newline below).
        if buf.last() == Some(&b'\n') {
            buf.pop();
        }
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
        let line = String::from_utf8_lossy(&buf);
        if let Some(pos) = line.find(pattern) {
            out.push(json!({
                "path": { "text": slash(path.strip_prefix(root).unwrap_or(path)) },
                "lines": { "text": format!("{line}\n") },
                "line_number": idx + 1,
                "absolute_offset": 0,
                "submatches": [{
                    "match": { "text": pattern },
                    "start": pos,
                    "end": pos + pattern.len(),
                }],
            }));
        }
        idx += 1;
    }
}

pub(crate) fn walk(
    root: &FsPath,
    dir: &FsPath,
    visit: &mut impl FnMut(&FsPath, &fs::Metadata) -> bool,
) -> bool {
    walk_cancel(root, dir, visit, None)
}

pub(crate) fn walk_cancel(
    root: &FsPath,
    dir: &FsPath,
    visit: &mut impl FnMut(&FsPath, &fs::Metadata) -> bool,
    cancel: Option<&AtomicBool>,
) -> bool {
    let Ok(entries) = fs::read_dir(dir) else {
        return true;
    };
    for entry in entries.filter_map(Result::ok) {
        if cancel.is_some_and(crate::agent::is_canceled) {
            return false;
        }
        if entry.file_name() == ".git" || entry.file_name() == ".DS_Store" {
            continue;
        }
        let path = entry.path();
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if !visit(&path, &meta) {
            return false;
        }
        if meta.is_dir() && path.starts_with(root) && !walk_cancel(root, &path, visit, cancel) {
            return false;
        }
    }
    true
}

pub(crate) fn is_binary(file: &FsPath, bytes: &[u8]) -> bool {
    let ext = file
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_lowercase();
    matches!(
        ext.as_str(),
        "exe"
            | "dll"
            | "bin"
            | "png"
            | "jpg"
            | "jpeg"
            | "gif"
            | "webp"
            | "zip"
            | "pdf"
            | "db"
            | "sqlite"
    ) || bytes.contains(&0)
}

pub(crate) fn is_text_source(file: &FsPath) -> bool {
    let ext = file
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_lowercase();
    matches!(
        ext.as_str(),
        "rs" | "ts"
            | "tsx"
            | "js"
            | "jsx"
            | "mjs"
            | "cjs"
            | "go"
            | "py"
            | "java"
            | "kt"
            | "kts"
            | "cs"
            | "cpp"
            | "cc"
            | "cxx"
            | "c"
            | "h"
            | "hpp"
            | "swift"
            | "php"
            | "rb"
    )
}

pub(crate) fn git_status(root: &FsPath) -> Vec<Value> {
    if !root.join(".git").exists()
        && git_text(root, &["rev-parse", "--is-inside-work-tree"]).is_none()
    {
        return Vec::new();
    }
    let mut out = Vec::new();
    if let Some(text) = git_text(
        root,
        &[
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.quotepath=false",
            "diff",
            "--numstat",
            "HEAD",
        ],
    ) {
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let mut parts = line.split('\t');
            let added = parts.next().unwrap_or("0");
            let removed = parts.next().unwrap_or("0");
            let path = parts.next().unwrap_or_default();
            if path.is_empty() {
                continue;
            }
            out.push(json!({
                "path": path,
                "added": git_count(added),
                "removed": git_count(removed),
                "status": "modified",
            }));
        }
    }
    if let Some(text) = git_text(
        root,
        &[
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.quotepath=false",
            "ls-files",
            "--others",
            "--exclude-standard",
        ],
    ) {
        for path in text.lines().filter(|line| !line.trim().is_empty()) {
            out.push(json!({
                "path": path,
                "added": line_count(&root.join(path)),
                "removed": 0,
                "status": "added",
            }));
        }
    }
    if let Some(text) = git_text(
        root,
        &[
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.quotepath=false",
            "diff",
            "--name-only",
            "--diff-filter=D",
            "HEAD",
        ],
    ) {
        for path in text.lines().filter(|line| !line.trim().is_empty()) {
            out.push(json!({
                "path": path,
                "added": 0,
                "removed": 0,
                "status": "deleted",
            }));
        }
    }
    out
}
