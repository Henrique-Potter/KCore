//! `read`, `grep`, `write`, `edit` fake-tool runtime.
//!
//! Step 7 of the kilo-server module split: verbatim cut from `lib.rs`. The
//! `slash` / `resolve_under` path helpers now live in `util::paths`; the
//! search/walk helpers (`is_binary`, `list_nodes`, `search_text_target`)
//! live in `routes::files` (re-exported at lib.rs scope).

use std::{
    fs,
    io::{BufRead, BufReader, Read},
    path::{Path as FsPath, PathBuf},
    sync::atomic::AtomicBool,
    time::SystemTime,
};

use serde_json::{json, Value};

use crate::agent::permission::{ask_external_directory, PermissionError};
use crate::agent::tools::common::{title, tool_usize};
use crate::agent::tools::diff::{diff_stats, text_diff};
use crate::agent::tools::encoding::{self, Encoding};
use crate::agent::tools::replacers::{replace as replace_chain, ReplaceError};
use crate::routes::files::{is_binary, list_nodes, search_text_target_cancel};
use crate::util::paths::{resolve_relaxed, resolve_under, slash};
use crate::AppState;
use std::sync::Arc;

pub(crate) const DEFAULT_READ_LIMIT: usize = 2000;
pub(crate) const DEFAULT_GLOB_LIMIT: usize = 100;
pub(crate) const DEFAULT_GREP_LIMIT: usize = 100;
pub(crate) const MAX_GREP_LINE: usize = 2000;

/// Internal resolver gate. When `allow_external` is true (the gated
/// async path has already secured the user's approval), accept paths
/// that resolve outside the worktree; otherwise fall through to the
/// strict `resolve_under` rejection. Tools call this in place of a bare
/// `resolve_under(root, path)`.
fn resolve_path(root: &FsPath, raw: &str, allow_external: bool) -> Result<PathBuf, String> {
    let outcome = if allow_external {
        resolve_relaxed(root, raw).map_err(|_| format!("Unsafe path: {raw}"))?
    } else {
        resolve_under(root, raw).map_err(|_| format!("Unsafe path: {raw}"))?
    };
    Ok(outcome)
}
/// Cancel cadence for line-by-line read/grep loops. Checking the
/// atomic on every line is wasteful; every 256 lines is plenty fast
/// to abort a runaway scan without hot-loading the cancel atomic.
const CANCEL_CADENCE: usize = 256;
/// Bytes peeked off the front of a file to decide whether it's binary
/// without loading the whole thing into memory.
const BINARY_SNIFF_BYTES: usize = 8192;

pub(crate) fn fake_read(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    fake_read_cancel(root, input, None)
}

/// Cancel-aware variant. Live OAuth tool dispatch passes `Some(cancel)`;
/// the static `chat_tools_with_auth` shape (`real_safe_tool_part`) and
/// the test seam pass `None`. Cancel observation is cooperative — the
/// per-line streaming loop checks every `CANCEL_CADENCE` lines so an
/// in-flight read aborts within a bounded number of lines.
pub(crate) fn fake_read_cancel(
    root: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
) -> Result<(String, String, Value), String> {
    fake_read_inner(root, input, cancel, false)
}

fn fake_read_inner(
    root: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
    allow_external: bool,
) -> Result<(String, String, Value), String> {
    if cancel.is_some_and(crate::agent::is_canceled) {
        return Err("Tool call aborted".to_string());
    }
    // Audit Fix 8: schema only advertises `filePath`. Don't accept a `path`
    // synonym at the dispatch site either.
    let path = input
        .get("filePath")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "filePath is required".to_string())?;
    let target = resolve_path(root, path, allow_external)?;
    let meta = fs::metadata(&target).map_err(|err| format!("Unable to read {path}: {err}"))?;
    if meta.is_dir() {
        return fake_read_dir(root, &target, input);
    }

    fake_read_file(root, &target, input, cancel)
}

/// Bun-parity gated entry. Pre-flight extracts `filePath`, classifies
/// it via `resolve_with_external`, and (only when the path resolves
/// outside the worktree) raises an `external_directory` permission ask.
/// On approval — or when the path is already inside — falls through to
/// the streaming sync impl with externals allowed.
pub(crate) async fn fake_read_gated(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    root: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
) -> Result<(String, String, Value), String> {
    let candidate = input
        .get("filePath")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_default();
    if !candidate.is_empty() {
        ask_external_directory(state, sid, mid, pid, idx, root, &[candidate], "read")
            .await
            .map_err(|err: PermissionError| err.to_display())?;
    }
    fake_read_inner(root, input, cancel, true)
}

fn fake_read_dir(
    root: &FsPath,
    dir: &FsPath,
    input: &Value,
) -> Result<(String, String, Value), String> {
    let offset = tool_usize(input, "offset", 1)?;
    let limit = tool_usize(input, "limit", DEFAULT_READ_LIMIT)?;
    let entries = list_nodes(root, dir)
        .into_iter()
        .filter_map(|node| node.get("path").and_then(Value::as_str).map(str::to_string))
        .collect::<Vec<_>>();
    let start = offset - 1;
    let sliced = entries
        .iter()
        .skip(start)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();
    let truncated = start + sliced.len() < entries.len();
    let note = if truncated {
        format!(
            "\n(Showing {} of {} entries. Use 'offset' parameter to read beyond entry {})",
            sliced.len(),
            entries.len(),
            offset + sliced.len()
        )
    } else {
        format!("\n({} entries)", entries.len())
    };
    let output = [
        format!("<path>{}</path>", dir.to_string_lossy()),
        "<type>directory</type>".to_string(),
        "<entries>".to_string(),
        sliced.join("\n"),
        note,
        "</entries>".to_string(),
    ]
    .join("\n");
    let metadata = json!({
        "preview": sliced.iter().take(20).cloned().collect::<Vec<_>>().join("\n"),
        "truncated": truncated,
        "loaded": [],
    });

    Ok((title(root, dir), output, metadata))
}

/// Stream-read a window of `[offset, offset+limit)` lines without
/// loading the whole file into memory. Memory is bounded by
/// `limit * per-line-bytes` (the captured window) plus one reused
/// `Vec<u8>` line buffer.
///
/// Three phases against a single `BufReader`:
/// 1. Skip the first `offset-1` lines (no allocation per line).
/// 2. Capture up to `limit` lines into the window.
/// 3. Read one more line to flip `truncated` if anything follows.
///    The whole-file line count is deliberately *not* computed —
///    that's the streaming win for multi-GB files.
///
/// Cancel is observed every `CANCEL_CADENCE` lines in each phase.
fn fake_read_file(
    root: &FsPath,
    file: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
) -> Result<(String, String, Value), String> {
    let offset = tool_usize(input, "offset", 1)?;
    let limit = tool_usize(input, "limit", DEFAULT_READ_LIMIT)?;

    // Sniff the first 8 KB to decide binary without loading the file.
    let mut sniff_handle = std::fs::File::open(file)
        .map_err(|err| format!("Unable to read {}: {err}", file.to_string_lossy()))?;
    let mut sniff = vec![0u8; BINARY_SNIFF_BYTES];
    let read_n = sniff_handle.read(&mut sniff).unwrap_or(0);
    sniff.truncate(read_n);
    let sniff_encoding = encoding::detect(&sniff);
    // UTF-16 is intentionally not flagged as binary by `is_binary`
    // when the BOM is recognized — we'll route to the encoding-aware
    // full-file decode path below.
    if !matches!(sniff_encoding, Encoding::Utf16Le | Encoding::Utf16Be) && is_binary(file, &sniff) {
        return Err(format!(
            "Cannot read binary file: {}",
            file.to_string_lossy()
        ));
    }
    drop(sniff_handle);

    // Non-UTF-8 / UTF-8-BOM files: read fully, decode, then window.
    // Streaming line-by-line read is only safe for plain UTF-8 because
    // UTF-16 has 0x0A bytes inside other code units.
    if !matches!(sniff_encoding, Encoding::Utf8) {
        return fake_read_file_encoded(root, file, input, cancel, sniff_encoding);
    }

    // Reopen for the streaming read; simpler than rewinding.
    let handle = std::fs::File::open(file)
        .map_err(|err| format!("Unable to read {}: {err}", file.to_string_lossy()))?;
    let mut reader = BufReader::new(handle);
    let start = offset - 1;
    let mut idx = 0usize;
    let mut buf: Vec<u8> = Vec::with_capacity(256);

    // Phase 1: skip [0, start) without allocating per line.
    while idx < start {
        if idx % CANCEL_CADENCE == 0 && cancel.is_some_and(crate::agent::is_canceled) {
            return Err("Tool call aborted".to_string());
        }
        buf.clear();
        let n = reader
            .read_until(b'\n', &mut buf)
            .map_err(|err| format!("Unable to read {}: {err}", file.to_string_lossy()))?;
        if n == 0 {
            break; // hit EOF before reaching offset
        }
        idx += 1;
    }

    // Edge: offset out of range. Match prior wording. Allow `offset == 1`
    // on an empty file (no lines to read, but the call is still valid).
    if idx < start && !(idx == 0 && offset == 1) {
        return Err(format!(
            "Offset {offset} is out of range for this file ({idx} lines)"
        ));
    }

    // Phase 2: capture [start, start+limit). One String per kept line —
    // total bounded by `limit`, not file size. We read one line past
    // the window (if it exists) to set `truncated` without scanning
    // the whole tail.
    let mut window: Vec<String> = Vec::with_capacity(limit.min(DEFAULT_READ_LIMIT));
    while window.len() < limit {
        if window.len() % CANCEL_CADENCE == 0 && cancel.is_some_and(crate::agent::is_canceled) {
            return Err("Tool call aborted".to_string());
        }
        buf.clear();
        let n = reader
            .read_until(b'\n', &mut buf)
            .map_err(|err| format!("Unable to read {}: {err}", file.to_string_lossy()))?;
        if n == 0 {
            break;
        }
        // Strip trailing \r?\n to match prior `text.lines()` behavior.
        if buf.last() == Some(&b'\n') {
            buf.pop();
        }
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
        window.push(String::from_utf8_lossy(&buf).into_owned());
    }

    // Probe one byte past the window to set `truncated` without
    // counting the entire tail. Streaming means we deliberately don't
    // know the file's total line count — the prior `of N` summary is
    // dropped in favor of a window-relative hint.
    let truncated = {
        if cancel.is_some_and(crate::agent::is_canceled) {
            return Err("Tool call aborted".to_string());
        }
        buf.clear();
        let n = reader
            .read_until(b'\n', &mut buf)
            .map_err(|err| format!("Unable to read {}: {err}", file.to_string_lossy()))?;
        n != 0
    };

    let mut output = [
        format!("<path>{}</path>", file.to_string_lossy()),
        "<type>file</type>".to_string(),
        "<content>\n".to_string(),
    ]
    .join("\n");
    output.push_str(
        &window
            .iter()
            .enumerate()
            .map(|(i, line)| format!("{}: {line}", i + offset))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let last = offset + window.len().saturating_sub(1);
    let next = last + 1;
    if truncated {
        output.push_str(&format!(
            "\n\n(Showing lines {offset}-{last}. Use offset={next} to continue.)"
        ));
    } else {
        output.push_str(&format!("\n\n(End of file - {} lines read)", window.len()));
    }
    output.push_str("\n</content>");
    let metadata = json!({
        "preview": window.iter().take(20).cloned().collect::<Vec<_>>().join("\n"),
        "truncated": truncated,
        "loaded": [],
    });

    Ok((title(root, file), output, metadata))
}

/// Encoding-aware read for files whose BOM identifies a non-plain-UTF-8
/// encoding. Reads the whole file, decodes once, then applies the same
/// `[offset, offset+limit)` windowing the streaming UTF-8 path uses.
/// Memory cost is proportional to file size — acceptable for the rare
/// UTF-16 / UTF-8-BOM case.
fn fake_read_file_encoded(
    root: &FsPath,
    file: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
    _enc: Encoding,
) -> Result<(String, String, Value), String> {
    let offset = tool_usize(input, "offset", 1)?;
    let limit = tool_usize(input, "limit", DEFAULT_READ_LIMIT)?;
    if cancel.is_some_and(crate::agent::is_canceled) {
        return Err("Tool call aborted".to_string());
    }
    let bytes = fs::read(file)
        .map_err(|err| format!("Unable to read {}: {err}", file.to_string_lossy()))?;
    let (text, _) = encoding::read_to_string(&bytes);
    let lines: Vec<&str> = text.split('\n').collect();
    let total = lines.len();
    let start = offset - 1;
    if start >= total && !(start == 0 && offset == 1) {
        return Err(format!(
            "Offset {offset} is out of range for this file ({total} lines)"
        ));
    }
    let window: Vec<String> = lines
        .iter()
        .skip(start)
        .take(limit)
        .map(|s| s.trim_end_matches('\r').to_string())
        .collect();
    let truncated = start + window.len() < total;

    let mut output = [
        format!("<path>{}</path>", file.to_string_lossy()),
        "<type>file</type>".to_string(),
        "<content>\n".to_string(),
    ]
    .join("\n");
    output.push_str(
        &window
            .iter()
            .enumerate()
            .map(|(i, line)| format!("{}: {line}", i + offset))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    let last = offset + window.len().saturating_sub(1);
    let next = last + 1;
    if truncated {
        output.push_str(&format!(
            "\n\n(Showing lines {offset}-{last}. Use offset={next} to continue.)"
        ));
    } else {
        output.push_str(&format!("\n\n(End of file - {} lines read)", window.len()));
    }
    output.push_str("\n</content>");
    let metadata = json!({
        "preview": window.iter().take(20).cloned().collect::<Vec<_>>().join("\n"),
        "truncated": truncated,
        "loaded": [],
    });
    Ok((title(root, file), output, metadata))
}

pub(crate) fn fake_grep(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    fake_grep_cancel(root, input, None)
}

pub(crate) fn fake_glob(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    fake_glob_cancel(root, input, None)
}

pub(crate) fn fake_glob_cancel(
    root: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
) -> Result<(String, String, Value), String> {
    fake_glob_inner(root, input, cancel, false)
}

fn fake_glob_inner(
    root: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
    allow_external: bool,
) -> Result<(String, String, Value), String> {
    if cancel.is_some_and(crate::agent::is_canceled) {
        return Err("Tool call aborted".to_string());
    }
    let pattern = input
        .get("pattern")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "pattern is required".to_string())?;
    let absolute = split_absolute_glob(pattern);
    let base = absolute
        .as_ref()
        .map(|item| item.dir.as_str())
        .or_else(|| input.get("path").and_then(Value::as_str))
        .unwrap_or(".");
    let search = resolve_path(root, base, allow_external)?;
    if search.exists() && !search.is_dir() {
        return Err(format!(
            "glob path must be a directory: {}",
            search.to_string_lossy()
        ));
    }
    let pattern = absolute
        .as_ref()
        .map(|item| item.pattern.as_str())
        .unwrap_or(pattern);

    let mut files = Vec::new();
    if search.exists() {
        collect_glob(root, &search, &search, pattern, &mut files, cancel)?;
    }
    files.sort_by(|a, b| {
        b.mtime
            .cmp(&a.mtime)
            .then_with(|| slash(&a.path).cmp(&slash(&b.path)))
    });
    let truncated = files.len() > DEFAULT_GLOB_LIMIT;
    files.truncate(DEFAULT_GLOB_LIMIT);

    let mut output = Vec::new();
    if files.is_empty() {
        output.push("No files found".to_string());
    } else {
        output.extend(files.iter().map(|file| slash(&file.path)));
        if truncated {
            output.push(String::new());
            output.push(format!(
                "(Results are truncated: showing first {DEFAULT_GLOB_LIMIT} results. Consider using a more specific path or pattern.)"
            ));
        }
    }

    Ok((
        title(root, &search),
        output.join("\n"),
        json!({ "count": files.len(), "truncated": truncated }),
    ))
}

pub(crate) fn fake_grep_cancel(
    root: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
) -> Result<(String, String, Value), String> {
    fake_grep_inner(root, input, cancel, false)
}

fn fake_grep_inner(
    root: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
    allow_external: bool,
) -> Result<(String, String, Value), String> {
    if cancel.is_some_and(crate::agent::is_canceled) {
        return Err("Tool call aborted".to_string());
    }
    let pattern = input
        .get("pattern")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "pattern is required".to_string())?;
    let path = input.get("path").and_then(Value::as_str).unwrap_or(".");
    let target = resolve_path(root, path, allow_external)?;
    if !target.exists() {
        return Ok((
            pattern.to_string(),
            "No files found".to_string(),
            json!({ "matches": 0, "truncated": false }),
        ));
    }

    let matches = search_text_target_cancel(root, &target, pattern, DEFAULT_GREP_LIMIT + 1, cancel);
    if cancel.is_some_and(crate::agent::is_canceled) {
        return Err("Tool call aborted".to_string());
    }
    if matches.is_empty() {
        return Ok((
            pattern.to_string(),
            "No files found".to_string(),
            json!({ "matches": 0, "truncated": false }),
        ));
    }

    let total = matches.len();
    let truncated = total > DEFAULT_GREP_LIMIT;
    let final_matches = matches.iter().take(DEFAULT_GREP_LIMIT).collect::<Vec<_>>();
    let mut output = vec![format!(
        "Found {total} matches{}",
        if truncated {
            format!(" (showing first {DEFAULT_GREP_LIMIT})")
        } else {
            String::new()
        }
    )];
    let mut current = String::new();
    for item in final_matches {
        let rel = item["path"]["text"].as_str().unwrap_or_default();
        let path = slash(&root.join(rel));
        if current != path {
            if !current.is_empty() {
                output.push(String::new());
            }
            current = path.clone();
            output.push(format!("{path}:"));
        }
        let line = item["line_number"].as_u64().unwrap_or_default();
        let mut text = item["lines"]["text"]
            .as_str()
            .unwrap_or_default()
            .trim_end_matches(['\r', '\n'])
            .to_string();
        if text.len() > MAX_GREP_LINE {
            text = format!("{}...", &text[..MAX_GREP_LINE]);
        }
        output.push(format!("  Line {line}: {text}"));
    }
    if truncated {
        output.push(String::new());
        output.push(format!(
            "(Results truncated: showing {DEFAULT_GREP_LIMIT} of {total} matches ({} hidden). Consider using a more specific path or pattern.)",
            total - DEFAULT_GREP_LIMIT
        ));
    }

    Ok((
        pattern.to_string(),
        output.join("\n"),
        json!({ "matches": total, "truncated": truncated }),
    ))
}

struct AbsoluteGlob {
    dir: String,
    pattern: String,
}

struct GlobFile {
    path: PathBuf,
    mtime: u128,
}

fn split_absolute_glob(pattern: &str) -> Option<AbsoluteGlob> {
    let normalized = pattern.replace('\\', "/");
    if !PathBuf::from(&normalized).is_absolute() {
        return None;
    }
    let len = normalized.len();
    let idx = normalized
        .find(|ch| matches!(ch, '*' | '?' | '{' | '['))
        .unwrap_or(len);
    let cut = normalized[..idx].rfind('/').unwrap_or(0);
    let pattern = if idx == len {
        "*".to_string()
    } else {
        normalized[cut + 1..].to_string()
    };
    let dir = if idx == len {
        normalized
    } else if cut == 0 {
        "/".to_string()
    } else {
        normalized[..cut].to_string()
    };
    Some(AbsoluteGlob { dir, pattern })
}

fn collect_glob(
    root: &FsPath,
    base: &FsPath,
    dir: &FsPath,
    pattern: &str,
    files: &mut Vec<GlobFile>,
    cancel: Option<&AtomicBool>,
) -> Result<(), String> {
    if files.len() > DEFAULT_GLOB_LIMIT {
        return Ok(());
    }
    if cancel.is_some_and(crate::agent::is_canceled) {
        return Err("Tool call aborted".to_string());
    }
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return Ok(()),
    };
    for entry in entries.flatten() {
        if cancel.is_some_and(crate::agent::is_canceled) {
            return Err("Tool call aborted".to_string());
        }
        let path = entry.path();
        let name = entry.file_name();
        if name.to_string_lossy() == ".git" {
            continue;
        }
        let meta = match entry.metadata() {
            Ok(meta) => meta,
            Err(_) => continue,
        };
        if meta.is_dir() {
            collect_glob(root, base, &path, pattern, files, cancel)?;
            if files.len() > DEFAULT_GLOB_LIMIT {
                return Ok(());
            }
            continue;
        }
        if !meta.is_file() {
            continue;
        }
        let rel = path.strip_prefix(base).unwrap_or(&path);
        if !glob_match(pattern, &slash(rel)) {
            continue;
        }
        let mtime = meta
            .modified()
            .ok()
            .and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|time| time.as_millis())
            .unwrap_or(0);
        files.push(GlobFile {
            path: if path.is_absolute() {
                path
            } else {
                root.join(path)
            },
            mtime,
        });
        if files.len() > DEFAULT_GLOB_LIMIT {
            return Ok(());
        }
    }
    Ok(())
}

fn glob_match(pattern: &str, rel: &str) -> bool {
    let rel = rel.replace('\\', "/").trim_start_matches("./").to_string();
    expand_braces(pattern)
        .iter()
        .any(|pattern| glob_match_one(pattern, &rel))
}

fn glob_match_one(pattern: &str, rel: &str) -> bool {
    let pattern = pattern
        .replace('\\', "/")
        .trim_start_matches("./")
        .to_string();
    if !pattern.contains('/') {
        return rel
            .rsplit('/')
            .next()
            .is_some_and(|name| segment_match(&pattern, name));
    }
    let pats = pattern.split('/').collect::<Vec<_>>();
    let parts = rel.split('/').collect::<Vec<_>>();
    path_match(&pats, &parts)
}

fn path_match(pattern: &[&str], parts: &[&str]) -> bool {
    if pattern.is_empty() {
        return parts.is_empty();
    }
    if pattern[0] == "**" {
        return (0..=parts.len()).any(|idx| path_match(&pattern[1..], &parts[idx..]));
    }
    if parts.is_empty() {
        return false;
    }
    segment_match(pattern[0], parts[0]) && path_match(&pattern[1..], &parts[1..])
}

fn segment_match(pattern: &str, text: &str) -> bool {
    let p = pattern.as_bytes();
    let t = text.as_bytes();
    let mut pi = 0usize;
    let mut ti = 0usize;
    let mut star = None;
    let mut mark = 0usize;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == b'?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
            continue;
        }
        if pi < p.len() && p[pi] == b'*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
            continue;
        }
        if let Some(idx) = star {
            pi = idx + 1;
            mark += 1;
            ti = mark;
            continue;
        }
        return false;
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
}

fn expand_braces(pattern: &str) -> Vec<String> {
    let Some(open) = pattern.find('{') else {
        return vec![pattern.to_string()];
    };
    let Some(close) = pattern[open + 1..].find('}').map(|idx| idx + open + 1) else {
        return vec![pattern.to_string()];
    };
    let mut out = Vec::new();
    let before = &pattern[..open];
    let after = &pattern[close + 1..];
    for item in pattern[open + 1..close].split(',') {
        for expanded in expand_braces(&format!("{before}{item}{after}")) {
            out.push(expanded);
        }
    }
    out
}

pub(crate) fn fake_write(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    fake_write_inner(root, input, false)
}

fn fake_write_inner(
    root: &FsPath,
    input: &Value,
    allow_external: bool,
) -> Result<(String, String, Value), String> {
    let path = input
        .get("filePath")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "filePath is required".to_string())?;
    let content = input
        .get("content")
        .and_then(Value::as_str)
        .ok_or_else(|| "content is required".to_string())?;
    let target = resolve_path(root, path, allow_external)?;
    let exists = target.exists();
    let (before, prior_enc) = if exists {
        let bytes = fs::read(&target)
            .map_err(|err| format!("Unable to read {}: {err}", target.to_string_lossy()))?;
        let (text, enc) = encoding::read_to_string(&bytes);
        (text, Some(enc))
    } else {
        (String::new(), None)
    };
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Unable to create {}: {err}", parent.to_string_lossy()))?;
    }
    let bytes = match prior_enc {
        Some(enc) => encoding::write_bytes(content, enc),
        None => content.as_bytes().to_vec(),
    };
    fs::write(&target, &bytes)
        .map_err(|err| format!("Unable to write {}: {err}", target.to_string_lossy()))?;

    let diff = text_diff(path, &before, content);
    let (add, del) = diff_stats(&before, content);
    let diagnostics = crate::agent::diagnostics::diagnostics_for(path, content);
    let metadata = json!({
        "filepath": slash(&target),
        "file": path,
        "path": path,
        "exists": exists,
        "diff": diff,
        "filediff": diff,
        "additions": add,
        "deletions": del,
        "diagnostics": diagnostics,
    });

    Ok((
        title(root, &target),
        "Wrote file successfully.".to_string(),
        metadata,
    ))
}

pub(crate) fn fake_edit(root: &FsPath, input: &Value) -> Result<(String, String, Value), String> {
    fake_edit_inner(root, input, false)
}

fn fake_edit_inner(
    root: &FsPath,
    input: &Value,
    allow_external: bool,
) -> Result<(String, String, Value), String> {
    let path = input
        .get("filePath")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "filePath is required".to_string())?;
    let old = input
        .get("oldString")
        .and_then(Value::as_str)
        .ok_or_else(|| "oldString is required".to_string())?;
    let new = input
        .get("newString")
        .and_then(Value::as_str)
        .ok_or_else(|| "newString is required".to_string())?;
    if old == new {
        return Err("oldString and newString must be different".to_string());
    }

    let target = resolve_path(root, path, allow_external)?;
    let (before, prior_enc) = if old.is_empty() {
        match fs::read(&target) {
            Ok(bytes) => {
                let (text, enc) = encoding::read_to_string(&bytes);
                (text, Some(enc))
            }
            Err(_) => (String::new(), None),
        }
    } else {
        let bytes = fs::read(&target)
            .map_err(|err| format!("Unable to read {}: {err}", target.to_string_lossy()))?;
        let (text, enc) = encoding::read_to_string(&bytes);
        (text, Some(enc))
    };
    let replace_all = input
        .get("replaceAll")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let after = if old.is_empty() {
        new.to_string()
    } else {
        match replace_chain(&before, old, new, replace_all) {
            Ok(out) => out,
            Err(ReplaceError::NotFound) => return Err("oldString was not found".to_string()),
            Err(ReplaceError::EmptyOld) => return Err("oldString is required".to_string()),
            Err(ReplaceError::MultipleMatches { count }) => {
                return Err(format!(
                    "oldString matched {count} times; set replaceAll to true or provide a unique match"
                ));
            }
        }
    };

    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Unable to create {}: {err}", parent.to_string_lossy()))?;
    }
    let bytes = match prior_enc {
        Some(enc) => encoding::write_bytes(&after, enc),
        None => after.as_bytes().to_vec(),
    };
    fs::write(&target, &bytes)
        .map_err(|err| format!("Unable to write {}: {err}", target.to_string_lossy()))?;

    let diff = text_diff(path, &before, &after);
    let (add, del) = diff_stats(&before, &after);
    let diagnostics = crate::agent::diagnostics::diagnostics_for(path, &after);
    let metadata = json!({
        "file": path,
        "path": path,
        "diff": diff,
        "filediff": diff,
        "additions": add,
        "deletions": del,
        "diagnostics": diagnostics,
    });

    Ok((
        title(root, &target),
        "Edit applied successfully.".to_string(),
        metadata,
    ))
}

/// Gated grep: kind = "read". Pre-flights `path` (defaults to `.`,
/// always inside) and only raises an ask when an explicit absolute path
/// outside the worktree is supplied.
pub(crate) async fn fake_grep_gated(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    root: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
) -> Result<(String, String, Value), String> {
    let candidate = input
        .get("path")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_default();
    if !candidate.is_empty() {
        ask_external_directory(state, sid, mid, pid, idx, root, &[candidate], "read")
            .await
            .map_err(|err: PermissionError| err.to_display())?;
    }
    fake_grep_inner(root, input, cancel, true)
}

/// Gated glob: kind = "read". Pre-flights both an absolute pattern's
/// derived dir and the explicit `path` argument, raising a single ask
/// for any externals.
pub(crate) async fn fake_glob_gated(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    root: &FsPath,
    input: &Value,
    cancel: Option<&AtomicBool>,
) -> Result<(String, String, Value), String> {
    let pattern = input
        .get("pattern")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut candidates: Vec<String> = Vec::new();
    if let Some(item) = split_absolute_glob(pattern) {
        candidates.push(item.dir);
    }
    if let Some(path) = input.get("path").and_then(Value::as_str) {
        if !path.is_empty() {
            candidates.push(path.to_string());
        }
    }
    if !candidates.is_empty() {
        ask_external_directory(state, sid, mid, pid, idx, root, &candidates, "read")
            .await
            .map_err(|err: PermissionError| err.to_display())?;
    }
    fake_glob_inner(root, input, cancel, true)
}

/// Gated write: kind = "write". Bun parity for the per-call
/// `external_directory` ask raised inside the write tool.
pub(crate) async fn fake_write_gated(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    root: &FsPath,
    input: &Value,
) -> Result<(String, String, Value), String> {
    let candidate = input
        .get("filePath")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_default();
    if !candidate.is_empty() {
        ask_external_directory(state, sid, mid, pid, idx, root, &[candidate], "write")
            .await
            .map_err(|err: PermissionError| err.to_display())?;
    }
    fake_write_inner(root, input, true)
}

/// Gated edit: kind = "write".
pub(crate) async fn fake_edit_gated(
    state: &Arc<AppState>,
    sid: &str,
    mid: &str,
    pid: &str,
    idx: usize,
    root: &FsPath,
    input: &Value,
) -> Result<(String, String, Value), String> {
    let candidate = input
        .get("filePath")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_default();
    if !candidate.is_empty() {
        ask_external_directory(state, sid, mid, pid, idx, root, &[candidate], "write")
            .await
            .map_err(|err: PermissionError| err.to_display())?;
    }
    fake_edit_inner(root, input, true)
}
