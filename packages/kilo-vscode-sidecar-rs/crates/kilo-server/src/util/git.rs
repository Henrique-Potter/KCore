//! Git porcelain helpers and file-metadata heuristics shared by routes that
//! shape diff / file-listing payloads. `git_text` shells out to `git`;
//! `bounded_line_count` / `text_line_count` / `line_count` build line-count
//! deltas; `generated_like` and `safe_relative` filter paths the Bun client
//! treats as noise (build artifacts, absolute paths).

use std::{
    fs,
    path::{Component, Path as FsPath},
    process::{Command, Stdio},
};

#[cfg(windows)]
pub(crate) const GIT: &str = "git.exe";
#[cfg(not(windows))]
pub(crate) const GIT: &str = "git";

pub(crate) fn bounded_line_count(path: &FsPath, limit: u64) -> usize {
    let Ok(meta) = fs::metadata(path) else {
        return 0;
    };
    if meta.len() > limit {
        return 0;
    }
    fs::read_to_string(path)
        .map(|text| text_line_count(&text))
        .unwrap_or(0)
}

pub(crate) fn text_line_count(text: &str) -> usize {
    if text.is_empty() {
        return 0;
    }
    if text.ends_with('\n') {
        return text.split('\n').count().saturating_sub(1);
    }
    text.split('\n').count()
}

pub(crate) fn generated_like(file: &str) -> bool {
    let folders = [
        "node_modules",
        "bower_components",
        ".pnpm-store",
        "vendor",
        ".npm",
        "dist",
        "build",
        "out",
        ".next",
        "target",
        "bin",
        "obj",
        ".git",
        ".svn",
        ".hg",
        ".vscode",
        ".idea",
        ".turbo",
        ".output",
        "desktop",
        ".sst",
        ".cache",
        ".webkit-cache",
        "__pycache__",
        ".pytest_cache",
        "mypy_cache",
        ".history",
        ".gradle",
    ];
    let parts = file.split(['/', '\\']).collect::<Vec<_>>();
    if parts.iter().any(|part| folders.contains(part)) {
        return true;
    }
    if parts
        .iter()
        .any(|part| ["logs", "tmp", "temp", "coverage", ".nyc_output"].contains(part))
    {
        return true;
    }
    [".swp", ".swo", ".pyc", ".log"]
        .iter()
        .any(|suffix| file.ends_with(suffix))
        || parts
            .last()
            .is_some_and(|base| [".DS_Store", "Thumbs.db"].contains(base))
}

pub(crate) fn safe_relative(file: &str) -> bool {
    let path = FsPath::new(file);
    !path.is_absolute()
        && path
            .components()
            .all(|part| matches!(part, Component::Normal(_) | Component::CurDir))
}

pub(crate) fn git_text(root: &FsPath, args: &[&str]) -> Option<String> {
    let output = Command::new(GIT)
        .args(args)
        .current_dir(root)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).to_string())
}

pub(crate) fn git_count(input: &str) -> usize {
    input.parse::<usize>().unwrap_or(0)
}

pub(crate) fn line_count(path: &FsPath) -> usize {
    let Ok(text) = fs::read_to_string(path) else {
        return 0;
    };
    text.split('\n').count()
}
