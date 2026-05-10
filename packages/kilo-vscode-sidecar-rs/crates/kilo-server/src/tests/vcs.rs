//! Tests for `GET /vcs` and `GET /vcs/diff`. The Rust sidecar shells
//! out to `git` via [`crate::util::git::git_text`]; the Bun route at
//! `packages/opencode/src/server/routes/instance/index.ts:130` returns
//! the same shape but builds it through the Effect-based `Vcs.Service`.

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use serde_json::Value;
use std::path::Path as FsPath;
use tower::ServiceExt;

use crate::http::build_router as app;

use super::common::{git, init_git_repo, response_to_string, state_at, unique_root};

/// Helper: write a tiny initial commit so the repo has a HEAD. Returns
/// `false` when `git` isn't on the PATH so the caller can short-circuit.
fn seed_initial_commit(repo: &FsPath) -> bool {
    if !init_git_repo(repo) {
        return false;
    }
    if !git(repo, &["config", "user.email", "kilo@example.test"])
        || !git(repo, &["config", "user.name", "Kilo Test"])
    {
        return false;
    }
    std::fs::write(repo.join("README.md"), "init\n").unwrap();
    git(repo, &["add", "."]) && git(repo, &["commit", "-m", "init"])
}

#[tokio::test]
async fn vcs_branch_returns_branch_in_real_git_repo() {
    let root = unique_root();
    // `Store::for_test` uses `<root>/repo` as the worktree directory
    // (`crates/kilo-store/src/lib.rs::for_test`), so the git repo has to
    // live there for the route to see it.
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    if !seed_initial_commit(&repo) {
        // No git on PATH, or init failed for environment reasons.
        let _ = std::fs::remove_dir_all(&root);
        return;
    }
    let st = state_at(&root);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/vcs")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&response_to_string(res).await).unwrap();
    // `init -b main` (or `checkout -B main`) sets HEAD on `main`.
    assert_eq!(body["branch"], "main");
    // No remote was set up, so origin/HEAD is unset; default_branch
    // falls back through `for-each-ref` and finds `main` locally.
    assert_eq!(body["default_branch"], "main");

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn vcs_branch_returns_null_in_non_git_dir() {
    let root = unique_root();
    // No git init under `<root>/repo`, so the route's `git_text` calls
    // exit non-zero and the handler returns null branch fields.
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let st = state_at(&root);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/vcs")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&response_to_string(res).await).unwrap();
    // Both branch fields null -> JSON `null`, not missing.
    assert!(body["branch"].is_null());
    assert!(body["default_branch"].is_null());

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn vcs_diff_reports_modified_files() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    if !seed_initial_commit(&repo) {
        let _ = std::fs::remove_dir_all(&root);
        return;
    }
    // Modify the seeded README so `git status --porcelain` reports it
    // as `M` and `git diff --numstat HEAD` reports +1/-0.
    std::fs::write(repo.join("README.md"), "init\nupdated line\n").unwrap();
    let st = state_at(&root);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/vcs/diff")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&response_to_string(res).await).unwrap();
    let items = body.as_array().expect("array body");
    let entry = items
        .iter()
        .find(|item| item["file"] == "README.md")
        .expect("README.md entry present");
    assert_eq!(entry["status"], "modified");
    assert_eq!(entry["additions"], 1);
    // `init\n` (1 line, ends with \n) -> `init\nupdated line\n` (2 lines).
    // numstat reports +1 / -0.
    assert_eq!(entry["deletions"], 0);

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn vcs_diff_returns_empty_in_non_git_dir() {
    let root = unique_root();
    std::fs::create_dir_all(root.join("repo")).unwrap();
    let st = state_at(&root);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/vcs/diff")
        .body(Body::empty())
        .unwrap();
    let res = app(st).oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: Value = serde_json::from_str(&response_to_string(res).await).unwrap();
    assert_eq!(body, serde_json::json!([]));

    let _ = std::fs::remove_dir_all(&root);
}
