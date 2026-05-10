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
