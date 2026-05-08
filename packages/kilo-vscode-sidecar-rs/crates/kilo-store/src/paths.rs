//! Path normalization helpers used by the storage layer and route
//! comparators. Forward-slash form is the canonical persistence and
//! comparison shape — Windows `\\` separators are mapped to `/` so a
//! workspace path persisted on Windows compares equal to the same path
//! produced by `git --porcelain` (which always emits `/`).
//!
//! Per **Cross-platform path handling** in the migration plan:
//!
//! - Every path comparison must go through [`normalize_separators`] before
//!   comparing — Windows `\\` vs `/` drift is the symptom that produced
//!   `worktree_list_reads_git_porcelain_paths` failures.
//! - UNC paths, drive letters, paths with non-ASCII characters, and paths
//!   over 260 characters round-trip through workspace and session storage
//!   without truncation.
//! - The helper is in this crate so kilo-store comparators (project /
//!   session directory matches) and kilo-server comparators (worktree
//!   listings, file routes) share one source of truth.

use std::path::{Component, Path, PathBuf};

/// Replace every backslash with a forward slash. Idempotent and
/// allocation-free when the input has no backslashes (`String::replace`
/// short-circuits in that case in practice — but call sites should still
/// avoid this in hot loops).
pub fn normalize_separators<S: AsRef<str>>(path: S) -> String {
    path.as_ref().replace('\\', "/")
}

/// Convert a [`Path`] to its canonical comparison form: a `String` with
/// forward-slash separators. Never truncates non-ASCII bytes — the
/// underlying `Path::to_string_lossy()` only substitutes for invalid UTF-8
/// (which storage already disallows).
pub fn to_canonical_string(path: &Path) -> String {
    normalize_separators(path.to_string_lossy().as_ref())
}

/// Two paths compare equal when their canonical-string forms are equal.
/// Use this in any code that previously did `a == b` on `&Path` or
/// `&String` values that may have come from different operating systems.
pub fn paths_equal(a: &str, b: &str) -> bool {
    normalize_separators(a) == normalize_separators(b)
}

/// True when `child` is the same path as `parent` or is nested under it,
/// after separator normalization. Comparison is byte-exact (so case still
/// matters on case-sensitive filesystems); callers that need case-insensitive
/// matching on Windows must lowercase before calling.
pub fn is_within(parent: &str, child: &str) -> bool {
    let parent = strip_trailing_slash(&normalize_separators(parent));
    let child = strip_trailing_slash(&normalize_separators(child));
    if parent == child {
        return true;
    }
    if parent.is_empty() {
        return false;
    }
    let needle = format!("{parent}/");
    child.starts_with(&needle)
}

fn strip_trailing_slash(s: &str) -> String {
    let mut out = s.to_string();
    while out.len() > 1 && out.ends_with('/') {
        out.pop();
    }
    out
}

/// Return the path without any UNC prefix (`\\?\` / `\\.\` on Windows).
/// Persisted paths should round-trip through the verbatim form so the
/// comparison helpers above don't false-negative on a UNC vs non-UNC pair
/// pointing at the same location.
pub fn strip_unc_prefix(path: &str) -> &str {
    let s = path;
    if let Some(rest) = s.strip_prefix(r"\\?\") {
        return rest;
    }
    if let Some(rest) = s.strip_prefix(r"\\.\") {
        return rest;
    }
    s
}

/// Best-effort cleanup: strip UNC prefixes, normalize to forward slashes,
/// and collapse any `.` components. Does NOT resolve symlinks — that is
/// the kernel's job and would change behavior semantics. Useful for
/// canonicalizing a user-entered workspace string before persisting.
pub fn clean(path: &str) -> String {
    let stripped = strip_unc_prefix(path);
    let normalized = normalize_separators(stripped);
    let pb = PathBuf::from(&normalized);
    let mut out = PathBuf::new();
    let mut first_root = true;
    for component in pb.components() {
        match component {
            Component::CurDir => continue,
            Component::Prefix(prefix) => {
                out.push(prefix.as_os_str());
                first_root = false;
            }
            Component::RootDir => {
                if first_root {
                    out.push("/");
                    first_root = false;
                } else {
                    out.push(component.as_os_str());
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    if out.as_os_str().is_empty() {
        return ".".to_string();
    }
    to_canonical_string(&out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_separators_handles_mixed_input() {
        assert_eq!(normalize_separators(r"a\b/c"), "a/b/c");
        assert_eq!(normalize_separators("a/b/c"), "a/b/c");
        assert_eq!(normalize_separators(r"a\b\c"), "a/b/c");
    }

    #[test]
    fn paths_equal_normalizes_separators() {
        assert!(paths_equal(r"C:\Users\Alice", "C:/Users/Alice"));
        assert!(paths_equal("/tmp/work", "/tmp/work"));
        assert!(!paths_equal(r"C:\Users\Alice", r"C:\Users\Bob"));
    }

    #[test]
    fn is_within_allows_self_and_nested() {
        assert!(is_within("/a/b", "/a/b"));
        assert!(is_within("/a/b", "/a/b/c"));
        assert!(is_within(r"C:\Users", "C:/Users/Alice"));
        assert!(!is_within("/a/b", "/a/bc")); // prefix-but-not-nested
        assert!(!is_within("/a/b", "/x/y"));
    }

    #[test]
    fn is_within_handles_trailing_slash() {
        assert!(is_within("/a/b/", "/a/b/c"));
        assert!(is_within("/a/b", "/a/b/"));
    }

    #[test]
    fn strip_unc_prefix_handles_verbatim_paths() {
        assert_eq!(strip_unc_prefix(r"\\?\C:\Users"), r"C:\Users");
        assert_eq!(strip_unc_prefix(r"\\.\COM3"), "COM3");
        assert_eq!(strip_unc_prefix("C:/Users"), "C:/Users");
    }

    #[test]
    fn clean_normalizes_unc_and_dot_components() {
        // CurDir component handling: we keep the original separators inside
        // components and only remove `.` segments.
        let out = clean(r"\\?\C:\Users\.\Alice");
        assert!(out.contains("Users") && !out.contains("/./"));
    }

    #[test]
    fn long_paths_are_preserved() {
        let long = format!("/long/{}", "x".repeat(300));
        assert_eq!(normalize_separators(&long), long);
        assert!(is_within("/long", &long));
    }

    #[test]
    fn non_ascii_round_trips() {
        let p = "/proj/héllo/世界";
        assert_eq!(normalize_separators(p), p);
        assert!(is_within("/proj", p));
    }
}
