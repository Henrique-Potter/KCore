//! File-content / file-find / symbol-search / git-status route tests.

use axum::http::StatusCode;

use crate::routes::files::{
    git_status, list_nodes, read_content, search_files, search_symbols, search_text,
};
use crate::util::paths::resolve_under;

use super::common::unique_root;

#[test]
fn file_content_reads_text_and_rejects_traversal() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("hello.txt"), "hello\n").unwrap();

    let file = resolve_under(&repo, "hello.txt").unwrap();
    let data = read_content(&file);
    assert_eq!(data["type"], "text");
    assert_eq!(data["content"], "hello");
    assert_eq!(
        resolve_under(&repo, "../secret.txt"),
        Err(StatusCode::FORBIDDEN)
    );

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn file_find_and_list_basics() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(
        repo.join("src").join("main.rs"),
        "fn main() {\n println!(\"needle\");\n}\n",
    )
    .unwrap();
    std::fs::write(repo.join("README.md"), "needle\n").unwrap();

    let nodes = list_nodes(&repo, &repo);
    assert_eq!(nodes[0]["name"], "src");
    assert_eq!(nodes[0]["type"], "directory");

    let files = search_files(&repo, "main", false, Some("file"), 10);
    assert_eq!(files, vec!["src/main.rs"]);

    let matches = search_text(&repo, "needle", 10);
    assert_eq!(matches.len(), 2);
    assert!(matches
        .iter()
        .any(|item| item["path"]["text"] == "src/main.rs"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn find_symbol_returns_lsp_shaped_text_symbols() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::write(
        repo.join("src").join("main.rs"),
        "pub fn helper() {\n}\nstruct HelperState;\n",
    )
    .unwrap();

    let symbols = search_symbols(&repo, "helper", 10);
    let fun = symbols
        .iter()
        .find(|item| item["kind"] == 12)
        .expect("function symbol");

    assert_eq!(symbols.len(), 2);
    assert_eq!(fun["name"], "helper");
    assert_eq!(fun["path"], "src/main.rs");
    assert!(fun["location"]["uri"]
        .as_str()
        .unwrap()
        .contains("src/main.rs"));
    assert_eq!(fun["location"]["range"]["start"]["line"], 0);

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn file_status_non_git_returns_empty_shape() {
    let root = unique_root();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();

    let status = git_status(&repo);

    assert!(status.is_empty());
    let _ = std::fs::remove_dir_all(root);
}
