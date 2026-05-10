//! Path helpers shared by route handlers, the fake-tool runtime, and the
//! agent turn loop.
//!
//! Step 8 of the kilo-server module split: `slash` and `resolve_under` were
//! free functions in `lib.rs`. They moved here so `routes::*` and
//! `agent::tools::*` import from a typed seam instead of the crate root.
//!
//! The search/walk family (`list_nodes`, `read_content`, `is_binary`,
//! `walk`) lives in `routes/files.rs` because it is invoked through route
//! handlers and via a re-export at lib.rs scope; nothing changes here.

use std::path::{Component, Path as FsPath, PathBuf};

use axum::http::StatusCode;

/// Convert a `&Path` to its canonical comparison form. Delegates to
/// [`kilo_store::paths::to_canonical_string`] so kilo-server, kilo-store, and
/// the oracle harness all share one normalization implementation. See the
/// **Cross-platform path handling** section of the migration plan for why.
pub(crate) fn slash(path: &FsPath) -> String {
    kilo_store::paths::to_canonical_string(path)
}

pub(crate) fn resolve_under(root: &FsPath, input: &str) -> Result<PathBuf, StatusCode> {
    let raw = input.trim();
    let path = PathBuf::from(raw);
    if path
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(StatusCode::FORBIDDEN);
    }

    if path.is_absolute() && is_within_path(root, &path) {
        return Ok(path);
    }

    let rel = raw.trim_start_matches(['/', '\\']);
    let path = PathBuf::from(rel);
    if path
        .components()
        .any(|part| matches!(part, Component::Prefix(_) | Component::RootDir))
    {
        return Err(StatusCode::FORBIDDEN);
    }

    Ok(root.join(path))
}

/// Like [`resolve_under`], but additionally canonicalizes the resolved
/// path (resolving symlinks) and re-verifies the canonical form still
/// lives under `root`. Use this anywhere a tool is about to read/write
/// a path that may have been laundered through a symlink:
/// `worktree/foo -> /etc/passwd` would pass the lexical check in
/// `resolve_under` but escape on canonicalize.
///
/// Paths that don't resolve (write-to-create / new-file case) keep the
/// lexical answer — there's nothing to canonicalize yet. The deepest
/// existing parent is still resolved so a symlink on an intermediate
/// directory is caught.
pub(crate) fn resolve_under_strict(root: &FsPath, input: &str) -> Result<PathBuf, StatusCode> {
    let lexical = resolve_under(root, input)?;
    let canonical = canonicalize_with_parent(&lexical);
    let canonical_root = std::fs::canonicalize(root).unwrap_or_else(|_| root.to_path_buf());
    let canonical_root = strip_unc(canonical_root);
    if is_within_path(&canonical_root, &canonical) {
        return Ok(lexical);
    }
    Err(StatusCode::FORBIDDEN)
}

/// Best-effort canonicalization that survives non-existent leaves. If the
/// whole path resolves, return its canonical form (UNC prefix stripped on
/// Windows). Otherwise canonicalize the deepest ancestor that exists and
/// re-attach the missing tail — so a symlink anywhere along the existing
/// prefix still gets resolved.
fn canonicalize_with_parent(target: &FsPath) -> PathBuf {
    if let Ok(p) = std::fs::canonicalize(target) {
        return strip_unc(p);
    }
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    let mut cursor = target;
    loop {
        match cursor.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => {
                if let Some(name) = cursor.file_name() {
                    tail.push(name.to_os_string());
                }
                if let Ok(canon) = std::fs::canonicalize(parent) {
                    let mut out = strip_unc(canon);
                    for piece in tail.iter().rev() {
                        out.push(piece);
                    }
                    return out;
                }
                cursor = parent;
            }
            _ => return target.to_path_buf(),
        }
    }
}

fn strip_unc(p: PathBuf) -> PathBuf {
    let s = p.to_string_lossy();
    if let Some(rest) = s.strip_prefix(r"\\?\") {
        return PathBuf::from(rest.to_string());
    }
    p
}

/// Outcome of `resolve_with_external`. Bun parity: paths outside the
/// worktree are not rejected outright — they can be approved per-call by
/// the user via the `external_directory` permission ask. The strict
/// `resolve_under` rejects them; this variant classifies them so the
/// caller can decide whether to gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResolveOutcome {
    /// Path resolves to a location under `root`. Safe to use without an
    /// extra permission ask.
    Inside(PathBuf),
    /// Path resolves to an absolute location outside `root`. Caller must
    /// gate via [`crate::agent::permission::ask_external_directory`]
    /// before touching the filesystem.
    External(PathBuf),
    /// Path cannot be resolved safely (traversal escape, root-relative
    /// non-absolute components, etc.). Reject outright.
    Invalid,
}

/// Classify `input` against `root` without rejecting external paths.
/// `..`-traversal and root-prefixed-but-non-absolute inputs still map to
/// `Invalid` for parity with `resolve_under`'s defense-in-depth — only
/// the "absolute path that escapes the worktree" case becomes `External`.
pub(crate) fn resolve_with_external(root: &FsPath, input: &str) -> ResolveOutcome {
    let raw = input.trim();
    let path = PathBuf::from(raw);
    if path
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return ResolveOutcome::Invalid;
    }

    if path.is_absolute() {
        if is_within_path(root, &path) {
            return ResolveOutcome::Inside(path);
        }
        return ResolveOutcome::External(path);
    }

    let rel = raw.trim_start_matches(['/', '\\']);
    let stripped = PathBuf::from(rel);
    if stripped
        .components()
        .any(|part| matches!(part, Component::Prefix(_) | Component::RootDir))
    {
        return ResolveOutcome::Invalid;
    }

    ResolveOutcome::Inside(root.join(stripped))
}

/// Tool-facing resolver: accept both inside and approved-external paths,
/// reject only `Invalid`. Use this inside a tool *after* the caller has
/// gated externals via `ask_external_directory`. Sync seams that cannot
/// gate should keep using [`resolve_under`].
pub(crate) fn resolve_relaxed(root: &FsPath, input: &str) -> Result<PathBuf, StatusCode> {
    match resolve_with_external(root, input) {
        ResolveOutcome::Inside(p) | ResolveOutcome::External(p) => Ok(p),
        ResolveOutcome::Invalid => Err(StatusCode::FORBIDDEN),
    }
}

fn is_within_path(root: &FsPath, path: &FsPath) -> bool {
    let root = cmp_path(root);
    let path = cmp_path(path);
    kilo_store::paths::is_within(&root, &path)
}

fn cmp_path(path: &FsPath) -> String {
    let out = kilo_store::paths::clean(path.to_string_lossy().as_ref());
    #[cfg(windows)]
    {
        return out.to_ascii_lowercase();
    }
    #[cfg(not(windows))]
    {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp_root(label: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let p = std::env::temp_dir().join(format!("kilo-paths-{label}-{stamp}-{n}"));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn resolve_under_strict_accepts_inside_existing_file() {
        let root = tmp_root("strict-inside");
        let worktree = root.join("repo");
        std::fs::create_dir_all(&worktree).unwrap();
        std::fs::write(worktree.join("hello.txt"), "hi").unwrap();
        let resolved = resolve_under_strict(&worktree, "hello.txt").expect("inside");
        assert_eq!(resolved, worktree.join("hello.txt"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_under_strict_accepts_path_that_does_not_exist() {
        // Write-to-create: the file isn't there yet but the parent is
        // the worktree itself, so canonicalize_with_parent resolves to
        // worktree + filename and the check passes.
        let root = tmp_root("strict-create");
        let worktree = root.join("repo");
        std::fs::create_dir_all(&worktree).unwrap();
        let resolved = resolve_under_strict(&worktree, "new-file.txt").expect("create");
        assert_eq!(resolved, worktree.join("new-file.txt"));
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn resolve_under_strict_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;
        let root = tmp_root("strict-symlink");
        let worktree = root.join("repo");
        let outside = root.join("outside");
        std::fs::create_dir_all(&worktree).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("secret.txt"), "leak").unwrap();
        // worktree/escape -> outside (a symlink). Lexically `escape/x`
        // is inside the worktree; canonically it resolves outside.
        symlink(&outside, worktree.join("escape")).unwrap();
        assert!(resolve_under(&worktree, "escape/secret.txt").is_ok());
        assert_eq!(
            resolve_under_strict(&worktree, "escape/secret.txt"),
            Err(StatusCode::FORBIDDEN),
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn resolve_under_strict_still_rejects_dotdot() {
        let root = tmp_root("strict-dotdot");
        let worktree = root.join("repo");
        std::fs::create_dir_all(&worktree).unwrap();
        assert_eq!(
            resolve_under_strict(&worktree, "../secret.txt"),
            Err(StatusCode::FORBIDDEN),
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
