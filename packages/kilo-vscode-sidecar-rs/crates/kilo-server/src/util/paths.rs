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
    let rel = input.trim_start_matches(['/', '\\']);
    let path = PathBuf::from(rel);
    if path.components().any(|part| {
        matches!(
            part,
            Component::ParentDir | Component::Prefix(_) | Component::RootDir
        )
    }) {
        return Err(StatusCode::FORBIDDEN);
    }

    Ok(root.join(path))
}
