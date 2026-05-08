//! Route handlers for /experimental/worktree* plus the git-worktree helper
//! graph (process wrappers, path safety, diff metadata).

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path as FsPath, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};

use crate::util::encoding::unix_millis;
use crate::util::git::{
    bounded_line_count, generated_like, git_count, git_text, safe_relative, text_line_count, GIT,
};
use crate::AppState;

const MAX_UNTRACKED_BYTES: u64 = 1_000_000;
const MAX_DIFF_DETAIL_BYTES: u64 = 20_000_000;

pub(crate) async fn worktrees(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let root = worktree_root(&state, &query);
    match git_worktrees(&root) {
        Ok(items) => Json(items).into_response(),
        Err(err) => worktree_error(err),
    }
}

pub(crate) async fn create_worktree(
    State(state): State<Arc<AppState>>,
    body: Option<Json<Value>>,
) -> Response {
    let input = body.map(|Json(value)| value).unwrap_or_else(|| json!({}));
    let root = PathBuf::from(state.store.paths().worktree);
    let name = input
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_string);
    match worktree_create(&state, &root, name.as_deref()) {
        Ok(info) => Json(info).into_response(),
        Err(err) => worktree_error(err),
    }
}

pub(crate) async fn delete_worktree(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    let Some(dir) = input.get("directory").and_then(Value::as_str) else {
        return worktree_error(WorktreeError::bad(
            "WorktreeInvalidInputError",
            "Missing worktree directory",
        ));
    };
    let root = PathBuf::from(state.store.paths().worktree);
    match worktree_delete(&root, dir) {
        Ok(()) => Json(true).into_response(),
        Err(err) => worktree_error(err),
    }
}

pub(crate) async fn reset_worktree(
    State(state): State<Arc<AppState>>,
    Json(input): Json<Value>,
) -> Response {
    let Some(dir) = input.get("directory").and_then(Value::as_str) else {
        return worktree_error(WorktreeError::bad(
            "WorktreeInvalidInputError",
            "Missing worktree directory",
        ));
    };
    let root = PathBuf::from(state.store.paths().worktree);
    match worktree_reset(&root, dir) {
        Ok(()) => Json(true).into_response(),
        Err(err) => worktree_error(err),
    }
}

pub(crate) async fn worktree_diff(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let root = worktree_root(&state, &query);
    let base = query.get("base").map(String::as_str).unwrap_or("HEAD");
    Json(worktree_diff_items(&root, base, false)).into_response()
}

pub(crate) async fn worktree_diff_summary(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let root = worktree_root(&state, &query);
    let base = query.get("base").map(String::as_str).unwrap_or("HEAD");
    Json(worktree_diff_items(&root, base, true)).into_response()
}

pub(crate) async fn worktree_diff_file(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let Some(file) = query.get("file").filter(|value| !value.is_empty()) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let root = worktree_root(&state, &query);
    let base = query.get("base").map(String::as_str).unwrap_or("HEAD");
    if !safe_relative(file) {
        return StatusCode::FORBIDDEN.into_response();
    }
    Json(worktree_file_diff(&root, base, file)).into_response()
}

pub(crate) fn worktree_root(state: &AppState, query: &BTreeMap<String, String>) -> PathBuf {
    query
        .get("directory")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(state.store.paths().directory))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GitOutput {
    code: Option<i32>,
    text: String,
    err: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorktreeEntry {
    path: PathBuf,
    branch: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorktreeError {
    status: StatusCode,
    name: &'static str,
    message: String,
}

impl WorktreeError {
    fn bad(name: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            name,
            message: message.into(),
        }
    }

    fn fail(name: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            name,
            message: message.into(),
        }
    }
}

pub(crate) fn worktree_error(err: WorktreeError) -> Response {
    (
        err.status,
        Json(json!({
            "name": err.name,
            "data": { "message": err.message },
        })),
    )
        .into_response()
}

pub(crate) fn git_worktrees(root: &FsPath) -> Result<Vec<String>, WorktreeError> {
    ensure_git_worktree(root)?;
    Ok(read_worktrees(root)?
        .into_iter()
        .map(|entry| path_arg(&entry.path))
        .collect())
}

pub(crate) fn worktree_create(
    state: &AppState,
    root: &FsPath,
    name: Option<&str>,
) -> Result<Value, WorktreeError> {
    let root = ensure_git_worktree(root)?;
    let parent = worktree_parent(state, &root)?;
    let name = worktree_name(name.unwrap_or_default())?;
    let dir = parent.join(&name);
    let branch = format!("opencode/{name}");
    ensure_near(&parent, &dir)?;
    if dir.exists() {
        return Err(WorktreeError::bad(
            "WorktreeCreateFailedError",
            "Worktree directory already exists",
        ));
    }
    if git_run(
        &root,
        &[
            "show-ref",
            "--verify",
            "--quiet",
            &format!("refs/heads/{branch}"),
        ],
    )
    .code
        == Some(0)
    {
        return Err(WorktreeError::bad(
            "WorktreeCreateFailedError",
            "Worktree branch already exists",
        ));
    }
    let dir_str = path_arg(&dir);
    let out = git_run(
        &root,
        &["worktree", "add", "--no-checkout", "-b", &branch, &dir_str],
    );
    if out.code != Some(0) {
        return Err(WorktreeError::fail(
            "WorktreeCreateFailedError",
            git_message(out, "Failed to create git worktree"),
        ));
    }
    let out = git_run(&dir, &["reset", "--hard"]);
    if out.code != Some(0) {
        return Err(WorktreeError::fail(
            "WorktreeCreateFailedError",
            git_message(out, "Failed to populate git worktree"),
        ));
    }
    Ok(json!({
        "name": name,
        "branch": branch,
        "directory": path_arg(&dir),
    }))
}

pub(crate) fn worktree_delete(root: &FsPath, dir: &str) -> Result<(), WorktreeError> {
    let root = ensure_git_worktree(root)?;
    let target = canonical_existing(FsPath::new(dir), "WorktreePathSafetyError")?;
    let primary = canonical_existing(&root, "WorktreePathSafetyError")?;
    if same_path(&primary, &target) {
        return Err(WorktreeError::bad(
            "WorktreeRemoveFailedError",
            "Cannot remove the primary workspace",
        ));
    }
    let entries = read_worktrees(&root)?;
    let Some(entry) = entries
        .into_iter()
        .find(|entry| same_path(&entry.path, &target))
    else {
        return Err(WorktreeError::bad(
            "WorktreeRemoveFailedError",
            "Worktree not found",
        ));
    };
    let target_str = path_arg(&entry.path);
    let out = git_run(&root, &["worktree", "remove", "--force", &target_str]);
    if out.code != Some(0) {
        return Err(WorktreeError::fail(
            "WorktreeRemoveFailedError",
            git_message(out, "Failed to remove git worktree"),
        ));
    }
    if let Some(branch) = entry
        .branch
        .and_then(|value| value.strip_prefix("refs/heads/").map(str::to_string))
    {
        let out = git_run(&root, &["branch", "-D", &branch]);
        if out.code != Some(0) {
            return Err(WorktreeError::fail(
                "WorktreeRemoveFailedError",
                git_message(out, "Failed to delete worktree branch"),
            ));
        }
    }
    Ok(())
}

pub(crate) fn worktree_reset(root: &FsPath, dir: &str) -> Result<(), WorktreeError> {
    let root = ensure_git_worktree(root)?;
    let target = canonical_existing(FsPath::new(dir), "WorktreePathSafetyError")?;
    let primary = canonical_existing(&root, "WorktreePathSafetyError")?;
    if same_path(&primary, &target) {
        return Err(WorktreeError::bad(
            "WorktreeResetFailedError",
            "Cannot reset the primary workspace",
        ));
    }
    let entries = read_worktrees(&root)?;
    let Some(entry) = entries
        .into_iter()
        .find(|entry| same_path(&entry.path, &target))
    else {
        return Err(WorktreeError::bad(
            "WorktreeResetFailedError",
            "Worktree not found",
        ));
    };
    ensure_clean(&entry.path)?;
    let base = default_branch(&root)?;
    let out = git_run(&entry.path, &["reset", "--hard", &base]);
    if out.code != Some(0) {
        return Err(WorktreeError::fail(
            "WorktreeResetFailedError",
            git_message(out, "Failed to reset worktree to target"),
        ));
    }
    let out = git_run(
        &entry.path,
        &["-c", "core.fsmonitor=false", "status", "--porcelain=v1"],
    );
    if out.code != Some(0) {
        return Err(WorktreeError::fail(
            "WorktreeResetFailedError",
            git_message(out, "Failed to read git status"),
        ));
    }
    if !out.text.trim().is_empty() {
        return Err(WorktreeError::bad(
            "WorktreeResetFailedError",
            format!("Worktree reset left local changes:\n{}", out.text.trim()),
        ));
    }
    Ok(())
}

pub(crate) fn ensure_clean(root: &FsPath) -> Result<(), WorktreeError> {
    let out = git_run(
        root,
        &["-c", "core.fsmonitor=false", "status", "--porcelain=v1"],
    );
    if out.code != Some(0) {
        return Err(WorktreeError::fail(
            "WorktreeResetFailedError",
            git_message(out, "Failed to read git status"),
        ));
    }
    if !out.text.trim().is_empty() {
        return Err(WorktreeError::bad(
            "WorktreeResetUnsafeError",
            "Refusing to reset a worktree with local changes",
        ));
    }
    Ok(())
}

pub(crate) fn ensure_git_worktree(root: &FsPath) -> Result<PathBuf, WorktreeError> {
    let out = git_run(root, &["rev-parse", "--is-inside-work-tree"]);
    if out.code != Some(0) || out.text.trim() != "true" {
        return Err(WorktreeError::bad(
            "WorktreeNotGitError",
            "Worktrees are only supported for git projects",
        ));
    }
    canonical_existing(root, "WorktreeNotGitError")
}

pub(crate) fn worktree_parent(state: &AppState, root: &FsPath) -> Result<PathBuf, WorktreeError> {
    let parent = root
        .parent()
        .unwrap_or(root)
        .join(".kilo-worktrees")
        .join("worktree")
        .join(state.store.project().id);
    fs::create_dir_all(&parent).map_err(|err| {
        WorktreeError::fail(
            "WorktreeCreateFailedError",
            format!("Failed to create worktree root: {err}"),
        )
    })?;
    let parent = fs::canonicalize(&parent).map_err(|err| {
        WorktreeError::fail(
            "WorktreeCreateFailedError",
            format!("Failed to resolve worktree root: {err}"),
        )
    })?;
    if same_path(&parent, root) {
        return Err(WorktreeError::bad(
            "WorktreeCreateFailedError",
            "Worktree root cannot be the primary workspace",
        ));
    }
    Ok(parent)
}

pub(crate) fn read_worktrees(root: &FsPath) -> Result<Vec<WorktreeEntry>, WorktreeError> {
    let out = git_run(root, &["worktree", "list", "--porcelain"]);
    if out.code != Some(0) {
        return Err(WorktreeError::fail(
            "WorktreeListFailedError",
            git_message(out, "Failed to read git worktrees"),
        ));
    }
    let mut items = Vec::new();
    for line in out.text.lines() {
        if let Some(path) = line.strip_prefix("worktree ") {
            items.push(WorktreeEntry {
                path: canonical_existing(FsPath::new(path), "WorktreeListFailedError")?,
                branch: None,
            });
            continue;
        }
        if let Some(branch) = line.strip_prefix("branch ") {
            if let Some(entry) = items.last_mut() {
                entry.branch = Some(branch.to_string());
            }
        }
    }
    Ok(items)
}

pub(crate) fn default_branch(root: &FsPath) -> Result<String, WorktreeError> {
    for item in ["origin/HEAD", "main", "master", "dev", "develop", "HEAD"] {
        let out = git_run(root, &["rev-parse", "--verify", "--quiet", item]);
        if out.code == Some(0) {
            return Ok(item.to_string());
        }
    }
    Err(WorktreeError::fail(
        "WorktreeResetFailedError",
        "Default branch not found",
    ))
}

pub(crate) fn worktree_name(input: &str) -> Result<String, WorktreeError> {
    let slug = input
        .trim()
        .to_lowercase()
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_string();
    if slug.is_empty() {
        return Ok(format!("worktree-{}", unix_millis()));
    }
    if slug.len() > 80 || slug == "." || slug == ".." {
        return Err(WorktreeError::bad(
            "WorktreeInvalidInputError",
            "Invalid worktree name",
        ));
    }
    Ok(slug)
}

pub(crate) fn ensure_near(root: &FsPath, target: &FsPath) -> Result<(), WorktreeError> {
    if safe_child(root, target) {
        return Ok(());
    }
    Err(WorktreeError::bad(
        "WorktreePathSafetyError",
        "Worktree path must stay inside the managed worktree root",
    ))
}

pub(crate) fn safe_child(root: &FsPath, target: &FsPath) -> bool {
    let root = canonical_key(root);
    let target = canonical_key(target);
    target.starts_with(&format!("{root}{}", std::path::MAIN_SEPARATOR))
}

pub(crate) fn same_path(a: &FsPath, b: &FsPath) -> bool {
    canonical_key(a) == canonical_key(b)
}

pub(crate) fn canonical_existing(
    path: &FsPath,
    name: &'static str,
) -> Result<PathBuf, WorktreeError> {
    fs::canonicalize(path)
        .map_err(|err| WorktreeError::bad(name, format!("Failed to resolve worktree path: {err}")))
}

pub(crate) fn canonical_key(path: &FsPath) -> String {
    let value = path_arg(path);
    if cfg!(windows) {
        return value.to_lowercase();
    }
    value
}

pub(crate) fn path_arg(path: &FsPath) -> String {
    let value = path.to_string_lossy().to_string();
    if cfg!(windows) {
        return value
            .strip_prefix(r"\\?\")
            .or_else(|| value.strip_prefix(r"\?\"))
            .unwrap_or(&value)
            .to_string();
    }
    value
}

pub(crate) fn git_run(root: &FsPath, args: &[&str]) -> GitOutput {
    match Command::new(GIT)
        .args(args)
        .current_dir(root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
    {
        Ok(out) => GitOutput {
            code: out.status.code(),
            text: String::from_utf8_lossy(&out.stdout).to_string(),
            err: String::from_utf8_lossy(&out.stderr).to_string(),
        },
        Err(err) => GitOutput {
            code: None,
            text: String::new(),
            err: err.to_string(),
        },
    }
}

pub(crate) fn git_message(out: GitOutput, default: &str) -> String {
    let msg = if out.err.trim().is_empty() {
        out.text.trim()
    } else {
        out.err.trim()
    };
    if msg.is_empty() {
        return default.to_string();
    }
    msg.to_string()
}

pub(crate) fn worktree_diff_items(root: &FsPath, base: &str, summary: bool) -> Vec<Value> {
    let Some(anc) = git_ancestor(root, base) else {
        return Vec::new();
    };
    worktree_diff_meta(root, &anc)
        .into_iter()
        .map(|item| {
            if summary {
                return worktree_summary(&item);
            }
            worktree_detail(root, &anc, &item).unwrap_or_else(|| worktree_summary(&item))
        })
        .collect()
}

pub(crate) fn worktree_file_diff(root: &FsPath, base: &str, file: &str) -> Option<Value> {
    let anc = git_ancestor(root, base)?;
    let item = worktree_file_meta(root, &anc, file)?;
    Some(worktree_detail(root, &anc, &item).unwrap_or_else(|| worktree_summary(&item)))
}

pub(crate) fn git_ancestor(root: &FsPath, base: &str) -> Option<String> {
    let base = resolve_git_base(root, base);
    git_text(root, &["merge-base", "HEAD", &base]).map(|value| value.trim().to_string())
}

pub(crate) fn resolve_git_base(root: &FsPath, base: &str) -> String {
    if !base.is_empty() && base != "HEAD" {
        return base.to_string();
    }
    for name in ["main", "master", "dev", "develop"] {
        if git_text(
            root,
            &[
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("refs/heads/{name}"),
            ],
        )
        .is_some()
        {
            return name.to_string();
        }
    }
    "HEAD".to_string()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WorktreeMeta {
    file: String,
    add: usize,
    del: usize,
    status: String,
    tracked: bool,
    stamp: String,
}

pub(crate) fn worktree_diff_meta(root: &FsPath, anc: &str) -> Vec<WorktreeMeta> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    let stat = worktree_numstat(root, anc, None);
    if let Some(text) = git_text(
        root,
        &[
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.quotepath=false",
            "diff",
            "--name-status",
            "--no-renames",
            anc,
        ],
    ) {
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let mut parts = line.split('\t');
            let code = parts.next().unwrap_or_default();
            let file = parts.collect::<Vec<_>>().join("\t");
            if file.is_empty() || code.is_empty() {
                continue;
            }
            seen.insert(file.clone());
            let count = stat.get(&file).copied().unwrap_or_default();
            let status = worktree_status(code);
            out.push(WorktreeMeta {
                stamp: if status == "deleted" {
                    format!("deleted:{anc}")
                } else {
                    stat_stamp(root, &file)
                },
                file,
                add: count.0,
                del: count.1,
                status: status.to_string(),
                tracked: true,
            });
        }
    }
    if let Some(text) = git_text(root, &["ls-files", "--others", "--exclude-standard"]) {
        for file in text.lines().filter(|line| !line.trim().is_empty()) {
            if seen.contains(file) || !safe_relative(file) || !root.join(file).exists() {
                continue;
            }
            out.push(WorktreeMeta {
                file: file.to_string(),
                add: bounded_line_count(&root.join(file), MAX_UNTRACKED_BYTES),
                del: 0,
                status: "added".to_string(),
                tracked: false,
                stamp: stat_stamp(root, file),
            });
        }
    }
    out
}

pub(crate) fn worktree_file_meta(root: &FsPath, anc: &str, file: &str) -> Option<WorktreeMeta> {
    if git_text(root, &["ls-files", "--error-unmatch", "--", file]).is_none() {
        let full = root.join(file);
        if !full.exists() {
            return None;
        }
        return Some(WorktreeMeta {
            file: file.to_string(),
            add: bounded_line_count(&full, MAX_UNTRACKED_BYTES),
            del: 0,
            status: "added".to_string(),
            tracked: false,
            stamp: stat_stamp(root, file),
        });
    }
    let text = git_text(
        root,
        &[
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.quotepath=false",
            "diff",
            "--name-status",
            "--no-renames",
            anc,
            "--",
            file,
        ],
    )?;
    let line = text.lines().find(|line| !line.trim().is_empty())?;
    let mut parts = line.split('\t');
    let code = parts.next().unwrap_or_default();
    let path = parts.collect::<Vec<_>>().join("\t");
    if code.is_empty() || path.is_empty() {
        return None;
    }
    let stat = worktree_numstat(root, anc, Some(file));
    let count = stat
        .get(file)
        .or_else(|| stat.get(&path))
        .copied()
        .unwrap_or_default();
    let status = worktree_status(code);
    Some(WorktreeMeta {
        stamp: if status == "deleted" {
            format!("deleted:{anc}")
        } else {
            stat_stamp(root, &path)
        },
        file: path,
        add: count.0,
        del: count.1,
        status: status.to_string(),
        tracked: true,
    })
}

pub(crate) fn worktree_numstat(
    root: &FsPath,
    anc: &str,
    file: Option<&str>,
) -> BTreeMap<String, (usize, usize)> {
    let args = if let Some(file) = file {
        vec![
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.quotepath=false",
            "diff",
            "--numstat",
            "--no-renames",
            anc,
            "--",
            file,
        ]
    } else {
        vec![
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.quotepath=false",
            "diff",
            "--numstat",
            "--no-renames",
            anc,
        ]
    };
    let mut out = BTreeMap::new();
    let Some(text) = git_text(root, &args) else {
        return out;
    };
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let parts = line.split('\t').collect::<Vec<_>>();
        if parts.len() < 3 {
            continue;
        }
        let file = parts[2..].join("\t");
        out.insert(file, (git_count(parts[0]), git_count(parts[1])));
    }
    out
}

pub(crate) fn worktree_status(code: &str) -> &'static str {
    if code == "A" {
        return "added";
    }
    if code == "D" {
        return "deleted";
    }
    "modified"
}

pub(crate) fn worktree_summary(item: &WorktreeMeta) -> Value {
    json!({
        "file": item.file,
        "patch": "",
        "before": "",
        "after": "",
        "additions": item.add,
        "deletions": item.del,
        "status": item.status,
        "tracked": item.tracked,
        "generatedLike": generated_like(&item.file),
        "summarized": true,
        "stamp": item.stamp,
    })
}

pub(crate) fn worktree_detail(root: &FsPath, anc: &str, item: &WorktreeMeta) -> Option<Value> {
    let before_bytes = if item.status == "added" {
        0
    } else {
        git_text(root, &["cat-file", "-s", &format!("{anc}:{}", item.file)])
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(0)
    };
    let after_bytes = if item.status == "deleted" {
        0
    } else {
        fs::metadata(root.join(&item.file))
            .map(|meta| meta.len())
            .unwrap_or(0)
    };
    if before_bytes > MAX_DIFF_DETAIL_BYTES || after_bytes > MAX_DIFF_DETAIL_BYTES {
        return None;
    }
    let before = if item.status == "added" {
        String::new()
    } else {
        git_text(root, &["show", &format!("{anc}:{}", item.file)]).unwrap_or_default()
    };
    let after = if item.status == "deleted" {
        String::new()
    } else {
        fs::read_to_string(root.join(&item.file)).unwrap_or_default()
    };
    let patch = if item.tracked {
        git_text(
            root,
            &[
                "-c",
                "core.fsmonitor=false",
                "-c",
                "core.quotepath=false",
                "diff",
                "--no-ext-diff",
                "--no-renames",
                anc,
                "--",
                &item.file,
            ],
        )
        .unwrap_or_default()
    } else {
        untracked_patch(&item.file, &after)
    };
    let add = if item.status == "added" && item.add == 0 && !item.tracked {
        text_line_count(&after)
    } else {
        item.add
    };
    Some(json!({
        "file": item.file,
        "patch": patch,
        "before": before,
        "after": after,
        "additions": add,
        "deletions": item.del,
        "status": item.status,
        "tracked": item.tracked,
        "generatedLike": generated_like(&item.file),
        "summarized": false,
        "stamp": item.stamp,
    }))
}

pub(crate) fn untracked_patch(file: &str, content: &str) -> String {
    if content.is_empty() {
        return format!(
            "diff --git a/{file} b/{file}\nnew file mode 100644\n--- /dev/null\n+++ b/{file}\n"
        );
    }
    let lines = content.split('\n').collect::<Vec<_>>();
    let body = if content.ends_with('\n') {
        &lines[..lines.len().saturating_sub(1)]
    } else {
        &lines[..]
    };
    let mut out = format!(
        "diff --git a/{file} b/{file}\nnew file mode 100644\n--- /dev/null\n+++ b/{file}\n@@ -0,0 +1,{} @@\n",
        body.len()
    );
    out.push_str(
        &body
            .iter()
            .map(|line| format!("+{line}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
    if content.ends_with('\n') {
        out.push('\n');
    } else {
        out.push_str("\n\\ No newline at end of file\n");
    }
    out
}

pub(crate) fn stat_stamp(root: &FsPath, file: &str) -> String {
    let Ok(meta) = fs::metadata(root.join(file)) else {
        return format!("missing:{file}");
    };
    let time = meta
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|time| time.as_millis())
        .unwrap_or(0);
    format!("{}:{time}", meta.len())
}
