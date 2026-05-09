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
