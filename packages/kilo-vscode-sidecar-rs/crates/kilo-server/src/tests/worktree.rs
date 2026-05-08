//! Worktree create / reset / delete / diff / list tests.

use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path as FsPath;

use crate::routes::worktree::{
    create_worktree, delete_worktree, reset_worktree, same_path, worktree_diff, worktree_diff_file,
    worktree_diff_summary, worktrees,
};
use crate::util::paths::slash;

use super::common::{git, init_git_repo, response_to_value, state_at, unique_root};

#[tokio::test]
async fn worktree_diff_non_git_returns_empty_shapes() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("note.txt"), "hello\n").unwrap();
    let state = state_at(&root);

    let res = worktree_diff_summary(State(state.clone()), Query(BTreeMap::new())).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_to_value(res).await, json!([]));

    let res = worktree_diff_file(
        State(state),
        Query(BTreeMap::from([(
            "file".to_string(),
            "note.txt".to_string(),
        )])),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_to_value(res).await, Value::Null);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn worktree_lifecycle_non_git_rejects_mutators() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let state = state_at(&root);

    let res = create_worktree(
        State(state.clone()),
        Some(Json(json!({ "name": "feature" }))),
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response_to_value(res).await["name"], "WorktreeNotGitError");

    let res = delete_worktree(
        State(state.clone()),
        Json(json!({ "directory": repo.to_string_lossy() })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response_to_value(res).await["name"], "WorktreeNotGitError");

    let res = reset_worktree(
        State(state),
        Json(json!({ "directory": repo.to_string_lossy() })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response_to_value(res).await["name"], "WorktreeNotGitError");

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn worktree_lifecycle_create_reset_delete_temp_repo() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    if !init_git_repo(&repo) {
        let _ = std::fs::remove_dir_all(root);
        return;
    }
    std::fs::write(repo.join("note.txt"), "base\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-m", "base"]);
    let state = state_at(&root);

    let res = create_worktree(
        State(state.clone()),
        Some(Json(json!({ "name": "Feature Thing" }))),
    )
    .await;
    let status = res.status();
    let info = response_to_value(res).await;
    if status != StatusCode::OK {
        panic!("create failed: {info:?}");
    }
    assert_eq!(info["name"], "feature-thing");
    assert_eq!(info["branch"], "opencode/feature-thing");
    let dir = info["directory"].as_str().unwrap().to_string();
    assert!(std::path::Path::new(&dir).join("note.txt").exists());

    let res = worktrees(State(state.clone()), Query(BTreeMap::new())).await;
    assert_eq!(res.status(), StatusCode::OK);
    let items = response_to_value(res).await;
    assert!(items
        .as_array()
        .unwrap()
        .iter()
        .any(|item| same_path(FsPath::new(item.as_str().unwrap()), FsPath::new(&dir))));

    let res = reset_worktree(State(state.clone()), Json(json!({ "directory": dir }))).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_to_value(res).await, json!(true));

    let res = delete_worktree(State(state), Json(json!({ "directory": dir }))).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(response_to_value(res).await, json!(true));
    assert!(!std::path::Path::new(&dir).exists());
    assert!(!git(
        &repo,
        &["rev-parse", "--verify", "opencode/feature-thing"]
    ));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn worktree_lifecycle_path_safety_rejects_primary_and_missing_paths() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    if !init_git_repo(&repo) {
        let _ = std::fs::remove_dir_all(root);
        return;
    }
    std::fs::write(repo.join("note.txt"), "base\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-m", "base"]);
    let state = state_at(&root);

    let res = delete_worktree(
        State(state.clone()),
        Json(json!({ "directory": repo.to_string_lossy() })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_to_value(res).await["name"],
        "WorktreeRemoveFailedError"
    );

    let res = reset_worktree(
        State(state.clone()),
        Json(json!({ "directory": repo.to_string_lossy() })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_to_value(res).await["name"],
        "WorktreeResetFailedError"
    );

    let res = delete_worktree(
        State(state),
        Json(json!({ "directory": root.join("missing").to_string_lossy() })),
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_to_value(res).await["name"],
        "WorktreePathSafetyError"
    );

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn worktree_reset_refuses_local_changes() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    if !init_git_repo(&repo) {
        let _ = std::fs::remove_dir_all(root);
        return;
    }
    std::fs::write(repo.join("note.txt"), "base\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-m", "base"]);
    let state = state_at(&root);
    let res = create_worktree(State(state.clone()), Some(Json(json!({ "name": "dirty" })))).await;
    assert_eq!(res.status(), StatusCode::OK);
    let info = response_to_value(res).await;
    let dir = info["directory"].as_str().unwrap().to_string();
    std::fs::write(std::path::Path::new(&dir).join("note.txt"), "dirty\n").unwrap();

    let res = reset_worktree(State(state.clone()), Json(json!({ "directory": dir }))).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response_to_value(res).await["name"],
        "WorktreeResetUnsafeError"
    );

    let _ = delete_worktree(State(state), Json(json!({ "directory": dir }))).await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn worktree_diff_parses_tracked_and_untracked_files() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    if !init_git_repo(&repo) {
        let _ = std::fs::remove_dir_all(root);
        return;
    }
    std::fs::write(repo.join("src").join("note.txt"), "one\ntwo\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-m", "base"]);
    std::fs::write(repo.join("src").join("note.txt"), "one\ntwo\nthree\n").unwrap();
    std::fs::write(repo.join("new.txt"), "alpha\nbeta\n").unwrap();
    let state = state_at(&root);

    let res = worktree_diff_summary(State(state.clone()), Query(BTreeMap::new())).await;
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    let items = data.as_array().unwrap();
    let note = items
        .iter()
        .find(|item| item["file"] == "src/note.txt")
        .unwrap();
    assert_eq!(note["status"], "modified");
    assert_eq!(note["additions"], 1);
    assert_eq!(note["deletions"], 0);
    assert_eq!(note["tracked"], true);
    assert_eq!(note["summarized"], true);
    let new = items.iter().find(|item| item["file"] == "new.txt").unwrap();
    assert_eq!(new["status"], "added");
    assert_eq!(new["additions"], 2);
    assert_eq!(new["tracked"], false);

    let res = worktree_diff(State(state), Query(BTreeMap::new())).await;
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    let items = data.as_array().unwrap();
    let note = items
        .iter()
        .find(|item| item["file"] == "src/note.txt")
        .unwrap();
    assert!(note["patch"].as_str().unwrap().contains("+three"));
    assert_eq!(note["before"], "one\ntwo\n");
    assert_eq!(note["after"], "one\ntwo\nthree\n");
    assert_eq!(note["summarized"], false);

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn worktree_diff_file_uses_directory_query_scope() {
    let root = unique_root();
    let repo = root.join("repo");
    let alt = root.join("alt");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::create_dir_all(&alt).unwrap();
    if !init_git_repo(&alt) {
        let _ = std::fs::remove_dir_all(root);
        return;
    }
    std::fs::write(alt.join("only-alt.txt"), "base\n").unwrap();
    git(&alt, &["add", "."]);
    git(&alt, &["commit", "-m", "base"]);
    std::fs::write(alt.join("only-alt.txt"), "base\nscoped\n").unwrap();
    let state = state_at(&root);
    let res = worktree_diff_file(
        State(state),
        Query(BTreeMap::from([
            ("directory".to_string(), alt.to_string_lossy().to_string()),
            ("file".to_string(), "only-alt.txt".to_string()),
        ])),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    assert_eq!(data["file"], "only-alt.txt");
    assert!(data["patch"].as_str().unwrap().contains("+scoped"));

    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn worktree_list_reads_git_porcelain_paths() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    if !init_git_repo(&repo) {
        let _ = std::fs::remove_dir_all(root);
        return;
    }
    std::fs::write(repo.join("note.txt"), "base\n").unwrap();
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "-m", "base"]);
    let state = state_at(&root);

    let res = worktrees(State(state), Query(BTreeMap::new())).await;
    assert_eq!(res.status(), StatusCode::OK);
    let data = response_to_value(res).await;
    let items = data.as_array().unwrap();
    let repo = slash(&repo);
    assert!(
        items
            .iter()
            .any(|item| item.as_str().unwrap_or_default().replace('\\', "/") == repo),
        "expected repo {repo}, got {items:?}"
    );

    let _ = std::fs::remove_dir_all(root);
}
