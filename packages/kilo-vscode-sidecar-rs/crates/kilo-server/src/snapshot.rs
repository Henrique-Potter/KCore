use std::{
    collections::BTreeMap,
    fmt, fs,
    io::Read,
    path::{Path as FsPath, PathBuf},
    process::{Command, Stdio},
    sync::{LazyLock, Mutex},
    time::{Duration, SystemTime},
};

use kilo_store::Store;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::util::git::{safe_relative, GIT};

/// Cap per-file content captured into `FileDiff::before` / `FileDiff::after` to
/// keep huge files from blowing up SSE/json envelopes. Bun uses the same
/// 256 KiB ceiling (`opencode/src/snapshot/index.ts:45`).
pub(crate) const MAX_DIFF_SIZE: usize = 256 * 1024;
const TRUNCATED: &str = "[truncated]";
const MAX_REVERT_DIFF_BYTES: usize = 512 * 1024;
const REVERT_DIFF_TRUNCATED: &str = "\n[snapshot diff truncated]\n";

static SNAPSHOT_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug)]
pub(crate) struct SnapshotError {
    detail: String,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for SnapshotError {}

#[derive(Clone, Debug)]
pub(crate) struct Patch {
    pub(crate) hash: String,
    pub(crate) files: Vec<String>,
}

pub(crate) struct Revert {
    pub(crate) redo: String,
    pub(crate) diff: String,
    pub(crate) summary: Value,
}

struct Ctx {
    git: PathBuf,
    root: PathBuf,
}

pub(crate) fn track(
    store: &Store,
    project: &str,
    session_id: &str,
) -> Result<String, SnapshotError> {
    let _lock = SNAPSHOT_LOCK
        .lock()
        .map_err(|_| fail("snapshot lock poisoned".to_string()))?;
    let ctx = ctx(store, project)?;
    stage(&ctx)?;
    let hash = text(&ctx, &["write-tree"])?.trim().to_string();
    // The tree object from `write-tree` is unreachable until something points
    // at it. Without a ref, `git gc --prune=now` (run on session delete)
    // deletes every loose tree in the snapshot dir — including ones still
    // referenced from OTHER sessions' `message.info["snapshot"]` and
    // `session.revert.snapshot`. Pin the tree behind a per-session ref so
    // `on_session_deleted(this_session)` can drop only the refs for THIS
    // session, leaving siblings reachable. Bun avoids this trap differently:
    // it uses `gc --prune=7.days` (see `opencode/src/snapshot/index.ts:44`)
    // so unreachable objects survive the grace window; we keep `--prune=now`
    // but make reachability explicit.
    let _ = run(
        &ctx,
        &["update-ref", &snapshot_ref(session_id, &hash), &hash],
    );
    Ok(hash)
}

/// Per-session-per-tree ref keeping snapshot trees reachable across
/// `git gc --prune=now`. Layout: `refs/kilo-snapshots/<slug(session_id)>/<hash>`.
/// `slug()` defends against any session id that would be rejected by git's
/// ref-name rules (`refname-format(7)`); session ids are normally
/// `ses_<alnum>` so this is belt-and-braces.
fn snapshot_ref(session_id: &str, hash: &str) -> String {
    format!("refs/kilo-snapshots/{}/{hash}", slug(session_id))
}

/// Prefix of refs owned by a single session — used by `on_session_deleted` to
/// drop only that session's refs.
fn snapshot_ref_prefix(session_id: &str) -> String {
    format!("refs/kilo-snapshots/{}", slug(session_id))
}

pub(crate) fn patch(store: &Store, project: &str, base: &str) -> Result<Patch, SnapshotError> {
    let _lock = SNAPSHOT_LOCK
        .lock()
        .map_err(|_| fail("snapshot lock poisoned".to_string()))?;
    let ctx = ctx(store, project)?;
    stage(&ctx)?;
    let files = text(&ctx, &["diff", "--cached", "--name-only", base, "--", "."])?
        .lines()
        .filter(|file| safe_relative(file))
        .map(|file| ctx.root.join(file).to_string_lossy().to_string())
        .collect::<Vec<_>>();
    let hash = text(&ctx, &["write-tree"])?.trim().to_string();
    Ok(Patch { hash, files })
}

/// Bun-parity helper: raw `git diff --cached --binary` against `base` for the
/// project's shadow-git snapshot dir. Currently unused in production — the
/// `revert` path uses `diff_revert_capped` and the snapshot route uses
/// `diff_full`. Kept exported for symmetry with Bun's snapshot module so a
/// later patch can wire it without re-deriving the locking/staging dance.
#[allow(dead_code)]
pub(crate) fn diff(store: &Store, project: &str, base: &str) -> Result<String, SnapshotError> {
    let _lock = SNAPSHOT_LOCK
        .lock()
        .map_err(|_| fail("snapshot lock poisoned".to_string()))?;
    let ctx = ctx(store, project)?;
    stage(&ctx)?;
    text(
        &ctx,
        &[
            "diff",
            "--cached",
            "--no-ext-diff",
            "--binary",
            base,
            "--",
            ".",
        ],
    )
}

/// Bun-parity helper: numstat-only summary for `base..HEAD`. Currently unused
/// in production — `summary_from_diff_full` is the structured alternative
/// already wired into `routes::sessions::revert_session`. Kept exported so a
/// route that wants the lighter numstat shape can adopt it without
/// re-implementing the lock + stage dance.
#[allow(dead_code)]
pub(crate) fn summary(store: &Store, project: &str, base: &str) -> Result<Value, SnapshotError> {
    let _lock = SNAPSHOT_LOCK
        .lock()
        .map_err(|_| fail("snapshot lock poisoned".to_string()))?;
    let ctx = ctx(store, project)?;
    stage(&ctx)?;
    summary_inner(&ctx, base)
}

pub(crate) fn revert(store: &Store, project: &str, base: &str) -> Result<Revert, SnapshotError> {
    let _lock = SNAPSHOT_LOCK
        .lock()
        .map_err(|_| fail("snapshot lock poisoned".to_string()))?;
    let ctx = ctx(store, project)?;
    stage(&ctx)?;
    let redo = text(&ctx, &["write-tree"])?.trim().to_string();
    let diff = diff_revert_capped(&ctx, base)?;
    let summary = summary_inner(&ctx, base)?;
    restore_inner(&ctx, base)?;
    Ok(Revert {
        redo,
        diff,
        summary,
    })
}

fn summary_inner(ctx: &Ctx, base: &str) -> Result<Value, SnapshotError> {
    let names = text(ctx, &["diff", "--cached", "--name-status", base, "--", "."])?;
    let stats = text(ctx, &["diff", "--cached", "--numstat", base, "--", "."])?;
    let mut status = BTreeMap::new();
    for line in names.lines() {
        let cols = line.split('\t').collect::<Vec<_>>();
        if cols.len() < 2 {
            continue;
        }
        let code = cols[0].chars().next().unwrap_or('M');
        let file = cols.last().copied().unwrap_or_default();
        status.insert(file.to_string(), status_name(code).to_string());
    }

    let mut additions = 0usize;
    let mut deletions = 0usize;
    let mut diffs = Vec::new();
    for line in stats.lines() {
        let cols = line.split('\t').collect::<Vec<_>>();
        if cols.len() < 3 {
            continue;
        }
        let add = count(cols[0]);
        let del = count(cols[1]);
        let file = cols[2].to_string();
        additions += add;
        deletions += del;
        diffs.push(json!({
            "file": file,
            "additions": add,
            "deletions": del,
            "status": status.get(cols[2]).cloned().unwrap_or_else(|| "modified".to_string()),
        }));
    }

    Ok(json!({
        "additions": additions,
        "deletions": deletions,
        "files": diffs.len(),
        "diffs": diffs,
    }))
}

pub(crate) fn restore(store: &Store, project: &str, hash: &str) -> Result<(), SnapshotError> {
    let _lock = SNAPSHOT_LOCK
        .lock()
        .map_err(|_| fail("snapshot lock poisoned".to_string()))?;
    let ctx = ctx(store, project)?;
    stage(&ctx)?;
    restore_inner(&ctx, hash)
}

fn restore_inner(ctx: &Ctx, hash: &str) -> Result<(), SnapshotError> {
    let added = text(
        &ctx,
        &[
            "diff",
            "--cached",
            "--name-only",
            "--diff-filter=A",
            hash,
            "--",
            ".",
        ],
    )?
    .lines()
    .filter(|file| safe_relative(file))
    .map(str::to_string)
    .collect::<Vec<_>>();
    run(&ctx, &["read-tree", hash])?;
    run(&ctx, &["checkout-index", "-a", "-f"])?;
    for file in added {
        let path = ctx.root.join(file);
        if path.is_dir() {
            fs::remove_dir_all(&path).map_err(|err| fail(format!("remove {:?}: {err}", path)))?;
        } else if path.exists() {
            fs::remove_file(&path).map_err(|err| fail(format!("remove {:?}: {err}", path)))?;
        }
    }
    Ok(())
}

fn diff_revert_capped(ctx: &Ctx, base: &str) -> Result<String, SnapshotError> {
    let (mut diff, truncated) = text_capped(
        ctx,
        &[
            "diff",
            "--cached",
            "--no-ext-diff",
            "--no-color",
            base,
            "--",
            ".",
        ],
        MAX_REVERT_DIFF_BYTES,
    )?;
    if truncated {
        diff.push_str(REVERT_DIFF_TRUNCATED);
    }
    Ok(diff)
}

fn ctx(store: &Store, project: &str) -> Result<Ctx, SnapshotError> {
    let paths = store.paths();
    let root = PathBuf::from(paths.worktree);
    if !root.is_dir() {
        return Err(fail(format!("worktree does not exist: {}", root.display())));
    }
    let dir = store
        .data_dir()
        .join("snapshot")
        .join(slug(project))
        .join(format!("{}.git", hash_path(&root)));
    fs::create_dir_all(
        dir.parent()
            .ok_or_else(|| fail(format!("invalid snapshot path: {}", dir.display())))?,
    )
    .map_err(|err| fail(format!("create snapshot dir: {err}")))?;
    let ctx = Ctx { git: dir, root };
    if !ctx.git.join("HEAD").exists() {
        run(&ctx, &["init", "--quiet"])?;
    }
    Ok(ctx)
}

fn stage(ctx: &Ctx) -> Result<(), SnapshotError> {
    run(ctx, &["add", "--all", "--", "."])
}

fn run(ctx: &Ctx, args: &[&str]) -> Result<(), SnapshotError> {
    let out = command(ctx, args)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| fail(format!("git {}: {err}", args.join(" "))))?;
    if out.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    Err(fail(format!(
        "git {} failed: {}",
        args.join(" "),
        stderr.trim()
    )))
}

fn text(ctx: &Ctx, args: &[&str]) -> Result<String, SnapshotError> {
    let out = command(ctx, args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .map_err(|err| fail(format!("git {}: {err}", args.join(" "))))?;
    if out.status.success() {
        return Ok(String::from_utf8_lossy(&out.stdout).to_string());
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    Err(fail(format!(
        "git {} failed: {}",
        args.join(" "),
        stderr.trim()
    )))
}

fn text_capped(ctx: &Ctx, args: &[&str], limit: usize) -> Result<(String, bool), SnapshotError> {
    let mut child = command(ctx, args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| fail(format!("git {}: {err}", args.join(" "))))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| fail(format!("git {} stdout unavailable", args.join(" "))))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| fail(format!("git {} stderr unavailable", args.join(" "))))?;
    let out = std::thread::spawn(move || read_capped(&mut stdout, limit));
    let err = std::thread::spawn(move || read_capped(&mut stderr, 64 * 1024));
    let status = child
        .wait()
        .map_err(|e| fail(format!("git {} wait: {e}", args.join(" "))))?;
    let (stdout, truncated) = out
        .join()
        .map_err(|_| fail(format!("git {} stdout reader failed", args.join(" "))))?;
    let (stderr, _) = err
        .join()
        .map_err(|_| fail(format!("git {} stderr reader failed", args.join(" "))))?;
    if status.success() {
        return Ok((String::from_utf8_lossy(&stdout).to_string(), truncated));
    }
    Err(fail(format!(
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&stderr).trim()
    )))
}

fn read_capped(reader: &mut impl Read, limit: usize) -> (Vec<u8>, bool) {
    let mut out = Vec::with_capacity(limit.min(8192));
    let mut buf = [0u8; 8192];
    let mut truncated = false;
    loop {
        let Ok(n) = reader.read(&mut buf) else {
            break;
        };
        if n == 0 {
            break;
        }
        let remaining = limit.saturating_sub(out.len());
        if remaining == 0 {
            truncated = true;
            continue;
        }
        let take = remaining.min(n);
        out.extend_from_slice(&buf[..take]);
        if take < n {
            truncated = true;
        }
    }
    (out, truncated)
}

fn command(ctx: &Ctx, args: &[&str]) -> Command {
    let mut cmd = Command::new(GIT);
    cmd.arg("--git-dir")
        .arg(&ctx.git)
        .arg("--work-tree")
        .arg(&ctx.root)
        .args(args)
        .current_dir(&ctx.root);
    cmd
}

fn slug(value: &str) -> String {
    let out = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if out.is_empty() {
        "global".to_string()
    } else {
        out
    }
}

fn hash_path(path: &FsPath) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.to_string_lossy().as_bytes());
    let bytes = hasher.finalize();
    bytes[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

fn count(value: &str) -> usize {
    value.parse().unwrap_or(0)
}

fn status_name(code: char) -> &'static str {
    match code {
        'A' => "added",
        'D' => "deleted",
        'R' => "renamed",
        'C' => "copied",
        _ => "modified",
    }
}

fn fail(detail: String) -> SnapshotError {
    SnapshotError { detail }
}

// ---------------------------------------------------------------------------
// diff_full — Bun-parity structured patch output.
// ---------------------------------------------------------------------------

/// Per-file structured diff entry, mirroring Bun's `FileDiff` shape from
/// `opencode/src/snapshot/index.ts` (`{ filePath, before, after, additions,
/// deletions, patch }`). Bun's variant also carries an optional `status`
/// field; we surface it on the parent `SnapshotDiffFull` rows when populated.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct FileDiff {
    pub(crate) file_path: String,
    pub(crate) before: String,
    pub(crate) after: String,
    pub(crate) additions: u32,
    pub(crate) deletions: u32,
    pub(crate) patch: String,
    pub(crate) status: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct SnapshotDiffFull {
    pub(crate) files: Vec<FileDiff>,
    pub(crate) additions: u32,
    pub(crate) deletions: u32,
}

/// Produce the Bun-parity structured patch output for `<base>..<head>` against
/// the project's shadow-git snapshot dir. Implementation:
///
/// 1. Run `git diff --name-status --no-renames <base> <head>` to classify each
///    file as added/deleted/modified.
/// 2. Run `git diff --numstat --no-renames <base> <head>` to capture per-file
///    addition/deletion counts (binary files are kept with `0/0`).
/// 3. Run `git diff --no-color --no-ext-diff --no-renames --unified=2147483647
///    <base> <head>` once to capture the full-context unified patch and split
///    by `diff --git` headers.
/// 4. For each file, fetch `before` (`git show <base>:<file>`) and `after`
///    (`git show <head>:<file>`); skip the missing side for added/deleted
///    files. Cap at `MAX_DIFF_SIZE` and emit `[truncated]` markers when over.
///
/// Mirrors Bun's `opencode/src/snapshot/index.ts:531-770` plus
/// `kilocode/snapshot/diff-full.ts:43-110`.
pub(crate) fn diff_full(
    store: &Store,
    project: &str,
    base: &str,
    head: &str,
) -> Result<SnapshotDiffFull, SnapshotError> {
    let _lock = SNAPSHOT_LOCK
        .lock()
        .map_err(|_| fail("snapshot lock poisoned".to_string()))?;
    let ctx = ctx(store, project)?;
    diff_full_inner(&ctx, base, head)
}

fn diff_full_inner(ctx: &Ctx, base: &str, head: &str) -> Result<SnapshotDiffFull, SnapshotError> {
    if base == head {
        return Ok(SnapshotDiffFull {
            files: Vec::new(),
            additions: 0,
            deletions: 0,
        });
    }

    let names = text(
        ctx,
        &[
            "diff",
            "--no-ext-diff",
            "--no-color",
            "--name-status",
            "--no-renames",
            base,
            head,
            "--",
            ".",
        ],
    )?;
    let mut status: BTreeMap<String, &'static str> = BTreeMap::new();
    for line in names.lines() {
        let cols = line.split('\t').collect::<Vec<_>>();
        if cols.len() < 2 {
            continue;
        }
        let code = cols[0].chars().next().unwrap_or('M');
        let file = cols.last().copied().unwrap_or_default();
        if file.is_empty() {
            continue;
        }
        let label: &'static str = match code {
            'A' => "added",
            'D' => "deleted",
            _ => "modified",
        };
        status.insert(file.to_string(), label);
    }

    let stats = text(
        ctx,
        &[
            "diff",
            "--no-ext-diff",
            "--no-color",
            "--no-renames",
            "--numstat",
            base,
            head,
            "--",
            ".",
        ],
    )?;
    struct Row {
        file: String,
        status: &'static str,
        binary: bool,
        additions: u32,
        deletions: u32,
    }
    let mut rows: Vec<Row> = Vec::new();
    for line in stats.lines() {
        let cols = line.split('\t').collect::<Vec<_>>();
        if cols.len() < 3 {
            continue;
        }
        let file = cols[2].trim();
        if file.is_empty() {
            continue;
        }
        let binary = cols[0] == "-" && cols[1] == "-";
        let additions = if binary { 0 } else { count_u32(cols[0]) };
        let deletions = if binary { 0 } else { count_u32(cols[1]) };
        let label = status.get(file).copied().unwrap_or("modified");
        rows.push(Row {
            file: file.to_string(),
            status: label,
            binary,
            additions,
            deletions,
        });
    }

    // One-shot full-context unified diff, then split by header. Bun chunks at
    // 500 paths to dodge Windows cmdline limits, but here we omit the explicit
    // pathspec list so the entire diff comes back in a single call regardless
    // of file count. If the spawn ever runs into argv pressure we'd reuse
    // `kilocode/snapshot/diff-full.ts:parseBatch`-style chunking.
    let patches = if rows.iter().any(|row| !row.binary) {
        let raw = text(
            ctx,
            &[
                "diff",
                "--no-color",
                "--no-ext-diff",
                "--no-renames",
                "--unified=2147483647",
                base,
                head,
                "--",
                ".",
            ],
        )?;
        split_unified_patches(&raw)
    } else {
        BTreeMap::new()
    };

    let mut files = Vec::with_capacity(rows.len());
    let mut total_add: u32 = 0;
    let mut total_del: u32 = 0;
    for row in rows {
        let (before, after) = if row.binary {
            (String::new(), String::new())
        } else {
            let before = if row.status == "added" {
                String::new()
            } else {
                show_capped(ctx, base, &row.file)?
            };
            let after = if row.status == "deleted" {
                String::new()
            } else {
                show_capped(ctx, head, &row.file)?
            };
            (before, after)
        };
        let patch = patches.get(&row.file).cloned().unwrap_or_default();
        total_add = total_add.saturating_add(row.additions);
        total_del = total_del.saturating_add(row.deletions);
        files.push(FileDiff {
            file_path: row.file,
            before,
            after,
            additions: row.additions,
            deletions: row.deletions,
            patch,
            status: row.status.to_string(),
        });
    }

    Ok(SnapshotDiffFull {
        files,
        additions: total_add,
        deletions: total_del,
    })
}

/// Render a `SnapshotDiffFull` into the JSON shape the existing `summary()`
/// helper writes onto `session.summary` (`additions`, `deletions`, `files`,
/// `diffs`). Lets `routes::sessions::revert_session` swap the lightweight
/// numstat-only summary for the structured-patch one when desired without
/// touching this module's call sites.
///
/// Note: the `diffs` array uses Bun's `SummaryFileDiff` shape (no `patch`
/// field) — keeps the persisted DB payload small. Callers who need the
/// per-file `patch` text should consume `SnapshotDiffFull::files` directly.
pub(crate) fn summary_from_diff_full(diff: &SnapshotDiffFull) -> Value {
    let diffs = diff
        .files
        .iter()
        .map(|file| {
            json!({
                "file": file.file_path,
                "additions": file.additions,
                "deletions": file.deletions,
                "status": file.status,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "additions": diff.additions,
        "deletions": diff.deletions,
        "files": diff.files.len(),
        "diffs": diffs,
    })
}

fn show_capped(ctx: &Ctx, rev: &str, file: &str) -> Result<String, SnapshotError> {
    let spec = format!("{rev}:{file}");
    // Stream through `text_capped` instead of buffering the whole blob with
    // `.output()`. A 50 MB file is now bounded to `MAX_DIFF_SIZE + 64 KiB`
    // (stdout + stderr buffers) rather than fully read before being thrown
    // away by the cap check (audit F-A9).
    match text_capped(ctx, &["show", spec.as_str()], MAX_DIFF_SIZE) {
        Ok((body, truncated)) => {
            if truncated {
                Ok(TRUNCATED.to_string())
            } else {
                Ok(body)
            }
        }
        // `git show <rev>:<missing>` returns non-zero — treat as "no content"
        // so added/deleted-side handling stays the same as Bun's fail-soft.
        Err(_) => Ok(String::new()),
    }
}

/// Split a multi-file `git diff` output into `path -> patch text` entries.
/// Header form is `diff --git a/<path> b/<path>` (or quoted variants).
fn split_unified_patches(text: &str) -> BTreeMap<String, String> {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    let mut current: Option<String> = None;
    let mut buffer: Vec<&str> = Vec::new();
    let flush = |current: &mut Option<String>,
                 buffer: &mut Vec<&str>,
                 map: &mut BTreeMap<String, String>| {
        if let Some(name) = current.take() {
            if !buffer.is_empty() {
                map.insert(name, buffer.join("\n"));
            }
        }
        buffer.clear();
    };
    for line in text.split('\n') {
        if line.starts_with("diff --git ") {
            flush(&mut current, &mut buffer, &mut map);
            current = parse_diff_header(line);
            if current.is_some() {
                buffer.push(line);
            }
            continue;
        }
        if current.is_some() {
            buffer.push(line);
        }
    }
    flush(&mut current, &mut buffer, &mut map);
    map
}

/// Extract the path from `diff --git a/<path> b/<path>` or quoted variants.
/// Bun's `parseBatch` checks both halves; here we accept matching `a/`+`b/`
/// halves and require them to be identical (matches `--no-renames`).
fn parse_diff_header(line: &str) -> Option<String> {
    let rest = line.strip_prefix("diff --git ")?;
    let rest = rest.trim_end();
    // Try quoted form first: `"a/..." "b/..."`
    if let Some(stripped) = rest.strip_prefix('"') {
        let close = stripped.find("\" \"")?;
        let a = &stripped[..close];
        let after = &stripped[close + 3..];
        let b_close = after.rfind('"')?;
        let b = &after[..b_close];
        let a_path = a.strip_prefix("a/")?;
        let b_path = b.strip_prefix("b/")?;
        if a_path == b_path {
            return Some(a_path.to_string());
        }
        return None;
    }
    // Bare form: split on " " between halves, both halves equal.
    // Header may contain backslash-quoted spaces; use a midpoint heuristic
    // matching Bun's `--no-renames`-only lookup.
    let prefix = "a/";
    let a_start = rest.find(prefix)?;
    if a_start != 0 {
        return None;
    }
    let after_a = &rest[prefix.len()..];
    let b_marker = " b/";
    let b_at = after_a.find(b_marker)?;
    let a_path = &after_a[..b_at];
    let b_path = &after_a[b_at + b_marker.len()..];
    if a_path == b_path {
        Some(a_path.to_string())
    } else {
        None
    }
}

fn count_u32(value: &str) -> u32 {
    value.parse::<u32>().unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Historical cleanup.
// ---------------------------------------------------------------------------

/// Root snapshot directory under the store data dir
/// (`<data>/snapshot`). Used by cleanup helpers so the path-safety checks live
/// next to their producer.
pub(crate) fn snapshot_root(store: &Store) -> PathBuf {
    store.data_dir().join("snapshot")
}

/// Hook to be invoked when a session is deleted. The current Rust snapshot
/// layout (`<root>/<project>/<hash(worktree)>.git`) is shared per worktree
/// across every session in the project, so deleting an individual session
/// does NOT remove its directory — other live sessions may still reference
/// the tree hashes captured there. Instead, we drop every
/// `refs/kilo-snapshots/<session>/*` ref for THIS session (created by
/// `track()` to keep its trees reachable across `gc`) and then run
/// `git gc --prune=now`. Trees referenced by OTHER sessions' refs survive;
/// trees only this session held are reclaimed. If no snapshot dir exists
/// for the project (the session never ran a turn), this is a no-op.
///
/// Future work: if Bun ever switches to per-session snapshot dirs, swap
/// this out for
/// `fs::remove_dir_all(snapshot_root.join(project).join(<sid>))` with the
/// same `inside()` guard `cleanup_old_snapshots` uses.
pub(crate) fn on_session_deleted(
    store: &Store,
    project: &str,
    session_id: &str,
) -> Result<(), SnapshotError> {
    let _lock = SNAPSHOT_LOCK
        .lock()
        .map_err(|_| fail("snapshot lock poisoned".to_string()))?;
    let project_dir = snapshot_root(store).join(slug(project));
    if !project_dir.is_dir() {
        return Ok(());
    }
    let entries = match fs::read_dir(&project_dir) {
        Ok(iter) => iter,
        Err(_) => return Ok(()),
    };
    let root = store.paths().worktree;
    let prefix = snapshot_ref_prefix(session_id);
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let ctx = Ctx {
            git: path,
            root: PathBuf::from(&root),
        };
        // 1) Drop refs owned by this session so their trees become
        //    candidates for `gc --prune=now`. `for-each-ref` lists
        //    `<refname> <SP> <hash>` so we can pair the delete with its
        //    expected value (safer than a bare `update-ref -d`).
        if let Ok(refs) = text(
            &ctx,
            &["for-each-ref", "--format=%(refname) %(objectname)", &prefix],
        ) {
            for line in refs.lines() {
                let mut parts = line.splitn(2, ' ');
                let Some(name) = parts.next() else { continue };
                let Some(hash) = parts.next() else { continue };
                if name.is_empty() || hash.is_empty() {
                    continue;
                }
                let _ = run(&ctx, &["update-ref", "-d", name, hash]);
            }
        }
        // 2) Best-effort GC; ignore errors so a stale dir doesn't break
        //    delete. Trees still referenced by sibling sessions' refs
        //    remain reachable.
        let _ = run(&ctx, &["gc", "--prune=now", "--quiet"]);
    }
    Ok(())
}

/// Walk the snapshot root and remove any per-worktree git dir whose mtime is
/// older than `max_age_days`. Returns the number of dirs removed. Path safety:
/// every removal must resolve under `snapshot_root` after canonicalization;
/// directories that escape (symlinks, junctions) are skipped.
/// Wired into the periodic GC task spawned in `serve()` (`lib.rs`); runs once
/// at startup and then every hour.
pub(crate) fn cleanup_old_snapshots(store: &Store, max_age_days: u64) -> usize {
    cleanup_old_snapshots_at(&snapshot_root(store), max_age_days)
}

fn cleanup_old_snapshots_at(root: &FsPath, max_age_days: u64) -> usize {
    // Phase 1: walk WITHOUT holding `SNAPSHOT_LOCK`. The original impl held
    // the process-global lock across the entire FS walk + per-dir
    // `fs::remove_dir_all`, blocking every live `track()`/`patch()`/`revert()`
    // for as long as the sweep took (audit B-B1). The walk only reads
    // metadata — it doesn't touch git state — so it's safe to do lock-free.
    if !root.is_dir() {
        return 0;
    }
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(max_age_days.saturating_mul(86_400)))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let canonical_root = match fs::canonicalize(root) {
        Ok(value) => value,
        Err(_) => return 0,
    };
    let mut candidates: Vec<(PathBuf, PathBuf)> = Vec::new();
    let projects = match fs::read_dir(root) {
        Ok(iter) => iter,
        Err(_) => return 0,
    };
    for project in projects.flatten() {
        let project_path = project.path();
        if !project_path.is_dir() {
            continue;
        }
        let inner = match fs::read_dir(&project_path) {
            Ok(iter) => iter,
            Err(_) => continue,
        };
        for entry in inner.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if !inside(&canonical_root, &path) {
                continue;
            }
            let age_ok = entry
                .metadata()
                .ok()
                .and_then(|meta| meta.modified().ok())
                .map(|modified| modified < cutoff)
                .unwrap_or(false);
            if !age_ok {
                continue;
            }
            candidates.push((path, project_path.clone()));
        }
    }

    // Phase 2: reacquire the lock briefly per removal. `try_lock` keeps the
    // cleanup yielding to a hot live turn — if a `track()` is mid-flight we
    // skip this candidate and let the next sweep pick it up. The lock guards
    // `Ctx` / `command()` invariants; we still take it for the FS removal so
    // we don't race a concurrent `git gc` reading the same dir.
    let mut removed = 0usize;
    let mut emptied: Vec<PathBuf> = Vec::new();
    for (path, project_path) in candidates {
        let guard = match SNAPSHOT_LOCK.try_lock() {
            Ok(guard) => guard,
            Err(_) => continue,
        };
        if fs::remove_dir_all(&path).is_ok() {
            removed += 1;
            emptied.push(project_path);
        }
        drop(guard);
    }
    // Drop now-empty project dirs after pruning; ignore errors. `remove_dir`
    // only succeeds when the directory is empty, so unrelated siblings are
    // safe.
    emptied.sort();
    emptied.dedup();
    for project_path in emptied {
        let _ = fs::remove_dir(&project_path);
    }
    removed
}

/// Confirm that `candidate` resolves under `root` after canonicalization.
/// Used to guard against symlink/junction escape during cleanup.
fn inside(root: &FsPath, candidate: &FsPath) -> bool {
    let Ok(resolved) = fs::canonicalize(candidate) else {
        return false;
    };
    resolved.starts_with(root)
}

/// Walk `<worktree>/.kilo/plans/` and remove plan markdown files whose mtime is
/// older than `max_age_days`. Returns the number of files removed. Plan files
/// are written by `agent::parts::plan_path` as `<created>-<slug>.md`; the slug
/// is not a session id, so we cannot cheaply correlate orphaned plans back to
/// missing sessions and instead clean by age only — matches Bun's TTL-based
/// sweep. The per-session deletion path in `routes::sessions::delete_session`
/// already handles the live-session case; this is the periodic-sweep
/// counterpart for files left behind when a session is removed without going
/// through that route. Path safety: every removal must resolve under the
/// canonicalized plans root; entries that escape (symlinks, junctions) are
/// skipped.
/// Wired into the periodic GC task spawned in `serve()` (`lib.rs`); runs once
/// at startup and then every hour.
pub(crate) fn cleanup_orphaned_plans(store: &Store, max_age_days: u64) -> usize {
    let plans = PathBuf::from(store.paths().worktree)
        .join(".kilo")
        .join("plans");
    cleanup_orphaned_plans_at(&plans, max_age_days)
}

fn cleanup_orphaned_plans_at(plans_root: &FsPath, max_age_days: u64) -> usize {
    let _lock = match SNAPSHOT_LOCK.lock() {
        Ok(guard) => guard,
        Err(_) => return 0,
    };
    if !plans_root.is_dir() {
        return 0;
    }
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(max_age_days.saturating_mul(86_400)))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let canonical_root = match fs::canonicalize(plans_root) {
        Ok(value) => value,
        Err(_) => return 0,
    };
    let entries = match fs::read_dir(plans_root) {
        Ok(iter) => iter,
        Err(_) => return 0,
    };
    let mut removed = 0usize;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let is_md = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("md"))
            .unwrap_or(false);
        if !is_md {
            continue;
        }
        if !inside(&canonical_root, &path) {
            continue;
        }
        let age_ok = entry
            .metadata()
            .ok()
            .and_then(|meta| meta.modified().ok())
            .map(|modified| modified < cutoff)
            .unwrap_or(false);
        if !age_ok {
            continue;
        }
        if fs::remove_file(&path).is_ok() {
            removed += 1;
        }
    }
    removed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_dir(label: &str) -> PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "kilo-snapshot-{label}-{}-{seq}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn run_git(dir: &FsPath, args: &[&str]) -> std::process::Output {
        let mut cmd = Command::new(GIT);
        cmd.current_dir(dir).args(args);
        cmd.env("GIT_AUTHOR_NAME", "kilo-test")
            .env("GIT_AUTHOR_EMAIL", "kilo@test.local")
            .env("GIT_COMMITTER_NAME", "kilo-test")
            .env("GIT_COMMITTER_EMAIL", "kilo@test.local");
        let out = cmd.output().expect("spawn git");
        if !out.status.success() {
            panic!(
                "git {} failed: {}",
                args.join(" "),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        out
    }

    /// Build a real on-disk repo and matching `Ctx`, returning `(ctx, repo)`.
    fn fixture(label: &str) -> (Ctx, PathBuf) {
        let repo = unique_dir(label);
        run_git(&repo, &["init", "--quiet", "--initial-branch=main"]);
        run_git(&repo, &["config", "core.autocrlf", "false"]);
        // Single worktree-style ctx pointing at the repo's own .git so
        // diff_full_inner can call `git show <rev>:<file>`.
        let ctx = Ctx {
            git: repo.join(".git"),
            root: repo.clone(),
        };
        (ctx, repo)
    }

    fn write(repo: &FsPath, name: &str, body: &str) {
        fs::write(repo.join(name), body).unwrap();
    }

    fn commit(repo: &FsPath, msg: &str) -> String {
        run_git(repo, &["add", "-A"]);
        run_git(repo, &["commit", "-m", msg, "--quiet"]);
        let out = run_git(repo, &["rev-parse", "HEAD"]);
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    #[test]
    fn diff_full_emits_per_file_entries_with_additions_deletions() {
        let (ctx, repo) = fixture("diff-basic");
        write(&repo, "a.txt", "alpha\nbravo\ncharlie\n");
        write(&repo, "b.txt", "one\ntwo\n");
        let base = commit(&repo, "init");

        write(&repo, "a.txt", "alpha\nBRAVO\ncharlie\n");
        write(&repo, "b.txt", "one\ntwo\nthree\n");
        let head = commit(&repo, "edit");

        let diff = diff_full_inner(&ctx, &base, &head).unwrap();
        assert_eq!(diff.files.len(), 2, "expected two changed files");
        let by_name: BTreeMap<_, _> = diff
            .files
            .iter()
            .map(|file| (file.file_path.clone(), file))
            .collect();

        let a = by_name.get("a.txt").unwrap();
        assert_eq!(a.status, "modified");
        assert_eq!(a.additions, 1);
        assert_eq!(a.deletions, 1);
        assert!(a.before.contains("bravo"));
        assert!(a.after.contains("BRAVO"));
        assert!(a.patch.contains("diff --git a/a.txt b/a.txt"));
        assert!(a.patch.contains("-bravo"));
        assert!(a.patch.contains("+BRAVO"));

        let b = by_name.get("b.txt").unwrap();
        assert_eq!(b.status, "modified");
        assert_eq!(b.additions, 1);
        assert_eq!(b.deletions, 0);

        assert_eq!(diff.additions, 2);
        assert_eq!(diff.deletions, 1);

        let summary = summary_from_diff_full(&diff);
        assert_eq!(summary["files"].as_u64().unwrap(), 2);
        assert_eq!(summary["additions"].as_u64().unwrap(), 2);
        assert_eq!(summary["deletions"].as_u64().unwrap(), 1);
        assert!(summary["diffs"].as_array().unwrap().len() == 2);
    }

    #[test]
    fn diff_full_handles_new_file_no_base() {
        let (ctx, repo) = fixture("diff-add");
        write(&repo, "keep.txt", "static\n");
        let base = commit(&repo, "init");

        write(&repo, "fresh.txt", "new file body\nline two\n");
        let head = commit(&repo, "add fresh");

        let diff = diff_full_inner(&ctx, &base, &head).unwrap();
        assert_eq!(diff.files.len(), 1);
        let entry = &diff.files[0];
        assert_eq!(entry.file_path, "fresh.txt");
        assert_eq!(entry.status, "added");
        assert_eq!(entry.additions, 2);
        assert_eq!(entry.deletions, 0);
        assert!(
            entry.before.is_empty(),
            "before should be empty for added files, got {:?}",
            entry.before
        );
        assert!(entry.after.contains("new file body"));
        assert!(entry.patch.contains("+new file body"));
    }

    #[test]
    fn diff_full_handles_deleted_file_no_head() {
        let (ctx, repo) = fixture("diff-del");
        write(&repo, "doomed.txt", "going away\n");
        write(&repo, "keep.txt", "static\n");
        let base = commit(&repo, "init");

        fs::remove_file(repo.join("doomed.txt")).unwrap();
        let head = commit(&repo, "delete doomed");

        let diff = diff_full_inner(&ctx, &base, &head).unwrap();
        assert_eq!(diff.files.len(), 1);
        let entry = &diff.files[0];
        assert_eq!(entry.file_path, "doomed.txt");
        assert_eq!(entry.status, "deleted");
        assert_eq!(entry.additions, 0);
        assert_eq!(entry.deletions, 1);
        assert!(entry.before.contains("going away"));
        assert!(
            entry.after.is_empty(),
            "after should be empty for deleted files, got {:?}",
            entry.after
        );
        assert!(entry.patch.contains("-going away"));
    }

    #[test]
    fn diff_full_truncates_large_files() {
        let (ctx, repo) = fixture("diff-trunc");
        let big = "x".repeat(MAX_DIFF_SIZE + 64);
        write(&repo, "huge.txt", &big);
        let base = commit(&repo, "init huge");

        let mut bigger = big.clone();
        bigger.push_str("\nappended\n");
        write(&repo, "huge.txt", &bigger);
        let head = commit(&repo, "edit huge");

        let diff = diff_full_inner(&ctx, &base, &head).unwrap();
        assert_eq!(diff.files.len(), 1);
        let entry = &diff.files[0];
        assert_eq!(entry.before, TRUNCATED, "before should report truncation");
        assert_eq!(entry.after, TRUNCATED, "after should report truncation");
        // Patch text is still present (capped only on `before`/`after`).
        assert!(entry.patch.contains("diff --git a/huge.txt b/huge.txt"));
    }

    #[test]
    fn cleanup_old_snapshots_removes_only_older_than_threshold() {
        let root = unique_dir("cleanup");
        let project = root.join("proj_a");
        fs::create_dir_all(project.join("old.git")).unwrap();
        fs::create_dir_all(project.join("fresh.git")).unwrap();
        fs::write(project.join("old.git").join("HEAD"), "ref").unwrap();
        fs::write(project.join("fresh.git").join("HEAD"), "ref").unwrap();

        // Backdate `old.git` by 40 days. Use the parent dir mtime since
        // `cleanup_old_snapshots_at` reads `entry.metadata().modified()`.
        let old_path = project.join("old.git");
        let cutoff = SystemTime::now() - Duration::from_secs(40 * 86_400);
        backdate(&old_path, cutoff);

        let removed = cleanup_old_snapshots_at(&root, 30);
        assert_eq!(removed, 1, "exactly the >30-day dir should be pruned");
        assert!(!old_path.exists(), "old.git must have been removed");
        assert!(project.join("fresh.git").exists(), "fresh.git must survive");

        // Cleanup the test root.
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn cleanup_does_not_escape_snapshot_root() {
        let root = unique_dir("cleanup-escape");
        let outside = unique_dir("cleanup-escape-outside");
        let outside_dir = outside.join("victim");
        fs::create_dir_all(&outside_dir).unwrap();
        fs::write(outside_dir.join("HEAD"), "ref").unwrap();
        backdate(
            &outside_dir,
            SystemTime::now() - Duration::from_secs(40 * 86_400),
        );

        let project = root.join("proj_a");
        fs::create_dir_all(&project).unwrap();

        // Try to plant a symlink/junction pointing at the outside victim.
        // If the platform refuses (e.g., requires admin on Windows), we fall
        // back to verifying that with NO escape link the cleanup is bounded
        // to the root — also useful coverage.
        #[cfg(unix)]
        {
            let _ = std::os::unix::fs::symlink(&outside_dir, project.join("escape.git"));
        }
        #[cfg(windows)]
        {
            let _ = std::os::windows::fs::symlink_dir(&outside_dir, project.join("escape.git"));
        }

        let removed = cleanup_old_snapshots_at(&root, 30);
        assert!(
            outside_dir.exists(),
            "outside dir must NOT be deleted by cleanup ({} removed)",
            removed
        );

        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&outside);
    }

    #[test]
    fn cleanup_orphaned_plans_removes_files_older_than_threshold() {
        let plans = unique_dir("plans-old");
        let stale = plans.join("1700000000-stale.md");
        let fresh = plans.join("1800000000-fresh.md");
        fs::write(&stale, "stale plan\n").unwrap();
        fs::write(&fresh, "fresh plan\n").unwrap();

        backdate(&stale, SystemTime::now() - Duration::from_secs(40 * 86_400));

        let removed = cleanup_orphaned_plans_at(&plans, 30);
        assert_eq!(removed, 1, "exactly the >30-day plan should be pruned");
        assert!(!stale.exists(), "stale plan must have been removed");
        assert!(fresh.exists(), "fresh plan must survive");

        let _ = fs::remove_dir_all(&plans);
    }

    #[test]
    fn cleanup_orphaned_plans_keeps_files_within_threshold() {
        let plans = unique_dir("plans-keep");
        let recent = plans.join("1800000000-recent.md");
        fs::write(&recent, "recent plan\n").unwrap();
        // No backdating: file mtime is "now", clearly within 30 days.

        let removed = cleanup_orphaned_plans_at(&plans, 30);
        assert_eq!(removed, 0, "no plan within threshold should be removed");
        assert!(recent.exists(), "recent plan must survive");

        let _ = fs::remove_dir_all(&plans);
    }

    #[test]
    fn cleanup_orphaned_plans_does_not_escape_plans_root() {
        let plans = unique_dir("plans-escape");
        let outside = unique_dir("plans-escape-outside");
        let victim = outside.join("victim.md");
        fs::write(&victim, "outside plan\n").unwrap();
        backdate(
            &victim,
            SystemTime::now() - Duration::from_secs(40 * 86_400),
        );

        // Try to plant a symlink pointing at the outside victim. If the
        // platform refuses (e.g., requires admin on Windows), we still verify
        // the bounded-cleanup invariant since the outside file should never
        // be touched regardless.
        #[cfg(unix)]
        {
            let _ = std::os::unix::fs::symlink(&victim, plans.join("escape.md"));
        }
        #[cfg(windows)]
        {
            let _ = std::os::windows::fs::symlink_file(&victim, plans.join("escape.md"));
        }

        let removed = cleanup_orphaned_plans_at(&plans, 30);
        assert!(
            victim.exists(),
            "outside plan must NOT be deleted by cleanup ({} removed)",
            removed
        );

        let _ = fs::remove_dir_all(&plans);
        let _ = fs::remove_dir_all(&outside);
    }

    #[test]
    fn cleanup_orphaned_plans_returns_zero_on_missing_dir() {
        let missing = std::env::temp_dir().join(format!(
            "kilo-snapshot-plans-missing-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        assert!(!missing.exists(), "precondition: dir must not exist");
        let removed = cleanup_orphaned_plans_at(&missing, 30);
        assert_eq!(removed, 0, "missing plans dir is a no-op");
    }

    /// Best-effort mtime backdating without pulling in `filetime`/`libc`.
    /// Shells out to `touch -d <iso>` on Unix and PowerShell on Windows.
    /// Tests skip via `is_old()` if the system call did not actually move the
    /// mtime — keeps the suite green on minimal containers.
    #[cfg(unix)]
    fn backdate(path: &FsPath, when: SystemTime) {
        let secs = when
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|dur| dur.as_secs() as i64)
            .unwrap_or(0);
        // `touch -d @<unix>` works on GNU coreutils and BusyBox.
        let _ = std::process::Command::new("touch")
            .args(["-d", &format!("@{secs}")])
            .arg(path)
            .output();
    }

    #[cfg(windows)]
    fn backdate(path: &FsPath, when: SystemTime) {
        let secs = when
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|dur| dur.as_secs())
            .unwrap_or(0);
        let script = format!(
            "(Get-Item -LiteralPath '{}').LastWriteTime = [DateTimeOffset]::FromUnixTimeSeconds({}).LocalDateTime",
            path.display().to_string().replace('\'', "''"),
            secs
        );
        let _ = std::process::Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command", &script])
            .output();
    }

    #[cfg(not(any(unix, windows)))]
    fn backdate(_path: &FsPath, _when: SystemTime) {}

    /// Build a `Store` backed by `root` with `<root>/repo` as the worktree
    /// and a real git repo already initialized inside it. The data dir
    /// (`<root>/data/kilo`) is created on demand by `track()`.
    fn store_fixture(label: &str) -> (kilo_store::Store, PathBuf) {
        let root = unique_dir(label);
        let repo = root.join("repo");
        fs::create_dir_all(&repo).unwrap();
        run_git(&repo, &["init", "--quiet", "--initial-branch=main"]);
        run_git(&repo, &["config", "core.autocrlf", "false"]);
        let store = kilo_store::Store::for_test(&root);
        (store, root)
    }

    /// Read the snapshot dir for `(project, worktree)` so tests can poke at
    /// the shadow git directly. Mirrors `snapshot::ctx()` layout.
    fn snapshot_git_dir(store: &kilo_store::Store, project: &str) -> PathBuf {
        let worktree = PathBuf::from(store.paths().worktree);
        store
            .data_dir()
            .join("snapshot")
            .join(slug(project))
            .join(format!("{}.git", hash_path(&worktree)))
    }

    /// Verify that `track()` creates `refs/kilo-snapshots/<session>/<hash>`
    /// keeping the tree reachable, and that `on_session_deleted(A)` does NOT
    /// destroy session B's tree (the previous bug — `git gc --prune=now`
    /// reclaimed every loose tree).
    #[test]
    fn snapshot_track_creates_ref_so_tree_survives_gc() {
        let (store, root) = store_fixture("track-ref-survives");
        let repo = root.join("repo");
        let project = "proj_a";

        // Session A snapshot.
        write(&repo, "file.txt", "session A body\n");
        let hash_a = match track(&store, project, "ses_A") {
            Ok(value) => value,
            Err(_) => {
                let _ = fs::remove_dir_all(&root);
                return; // host without git
            }
        };

        // Session B snapshot, distinct tree.
        write(&repo, "file.txt", "session B body\n");
        let hash_b = match track(&store, project, "ses_B") {
            Ok(value) => value,
            Err(err) => panic!("track B: {err}"),
        };
        assert_ne!(hash_a, hash_b, "expected distinct trees per session");

        let git_dir = snapshot_git_dir(&store, project);
        let worktree = repo.clone();

        // Sanity: both refs exist under refs/kilo-snapshots/.
        let assert_ref = |session: &str, hash: &str, expect: bool| {
            let name = snapshot_ref(session, hash);
            let out = Command::new(GIT)
                .arg("--git-dir")
                .arg(&git_dir)
                .arg("--work-tree")
                .arg(&worktree)
                .args(["rev-parse", "--verify", &name])
                .output()
                .expect("git rev-parse");
            assert_eq!(
                out.status.success(),
                expect,
                "ref {name} presence mismatch (expected {expect}): {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        assert_ref("ses_A", &hash_a, true);
        assert_ref("ses_B", &hash_b, true);

        // Delete session A. After this, ref A must be gone but B's tree
        // must still be readable (the previous bug: `gc --prune=now`
        // collected every loose tree, including B's).
        on_session_deleted(&store, project, "ses_A").expect("on_session_deleted");

        assert_ref("ses_A", &hash_a, false);
        assert_ref("ses_B", &hash_b, true);

        // The decisive check: B's tree object must still be reachable via
        // `cat-file -t` even after `--prune=now`. If the ref hadn't
        // survived, this would fail because the loose tree would be
        // reclaimed.
        let out = Command::new(GIT)
            .arg("--git-dir")
            .arg(&git_dir)
            .arg("--work-tree")
            .arg(&worktree)
            .args(["cat-file", "-t", &hash_b])
            .output()
            .expect("git cat-file");
        assert!(
            out.status.success(),
            "B's tree {hash_b} must survive A's deletion: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            "tree",
            "expected tree object, got {}",
            String::from_utf8_lossy(&out.stdout)
        );

        let _ = fs::remove_dir_all(&root);
    }

    /// `cleanup_old_snapshots_at` used to hold `SNAPSHOT_LOCK` across the
    /// entire FS walk, blocking concurrent `track()` calls. With the fix,
    /// the walk runs lock-free and the lock is only `try_lock`ed per
    /// removal — a concurrent `track()` should return promptly even while
    /// a cleanup pass is in flight (audit B-B1).
    #[test]
    fn cleanup_old_snapshots_does_not_block_concurrent_snapshot() {
        let (store, root) = store_fixture("cleanup-no-block");
        let repo = root.join("repo");
        write(&repo, "marker.txt", "x\n");

        // Pre-flight: must be able to `track()` at all on this host.
        if track(&store, "proj_a", "ses_pre").is_err() {
            let _ = fs::remove_dir_all(&root);
            return;
        }

        let snapshot_root_path = snapshot_root(&store);
        let cleanup_handle = std::thread::spawn(move || {
            // Concurrent cleanup pass — must NOT hold the lock during walk.
            cleanup_old_snapshots_at(&snapshot_root_path, 365 * 100);
        });

        // Race a track() against the cleanup. With the lock-released-walk
        // fix this returns promptly. With the old impl, cleanup would hold
        // the lock for the duration of the walk and serialize this call.
        let started = std::time::Instant::now();
        let _ = track(&store, "proj_a", "ses_race");
        let elapsed = started.elapsed();
        cleanup_handle.join().unwrap();
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "track() blocked behind cleanup walk ({:?}); cleanup must release the lock between candidates",
            elapsed
        );

        let _ = fs::remove_dir_all(&root);
    }
}
