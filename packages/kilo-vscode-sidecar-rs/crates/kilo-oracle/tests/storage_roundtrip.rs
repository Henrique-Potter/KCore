use std::path::Path;
use std::time::Duration;

mod rust_harness;

use kilo_oracle::normalize::Normalizer;
use kilo_oracle::{OracleClient, Redactions, SidecarHandle, SpawnConfig};
use pretty_assertions::assert_eq;
use rusqlite::Connection;
use serde_json::{json, Value};

use rust_harness::{
    abort, create_session, fake_prompt, messages, prompt_async, record_until_idle,
    spawn_stalling_oauth_stub, RustSidecar,
};

const TABLES: &[(&str, &[&str])] = &[
    // Bun source: packages/opencode/src/project/project.sql.ts ProjectTable.
    (
        "project",
        &[
            "id",
            "worktree",
            "vcs",
            "name",
            "icon_url",
            "icon_url_override",
            "icon_color",
            "time_created",
            "time_updated",
            "time_initialized",
            "sandboxes",
            "commands",
        ],
    ),
    // Bun source: packages/opencode/src/session/session.sql.ts SessionTable.
    (
        "session",
        &[
            "id",
            "project_id",
            "workspace_id",
            "parent_id",
            "slug",
            "directory",
            "title",
            "version",
            "share_url",
            "summary_additions",
            "summary_deletions",
            "summary_files",
            "summary_diffs",
            "revert",
            "permission",
            "time_created",
            "time_updated",
            "time_compacting",
            "time_archived",
        ],
    ),
    // Bun source: packages/opencode/src/session/session.sql.ts MessageTable.
    (
        "message",
        &["id", "session_id", "time_created", "time_updated", "data"],
    ),
    // Bun source: packages/opencode/src/session/session.sql.ts PartTable.
    (
        "part",
        &[
            "id",
            "message_id",
            "session_id",
            "time_created",
            "time_updated",
            "data",
        ],
    ),
    // Bun source: packages/opencode/src/sync/event.sql.ts EventSequenceTable.
    ("event_sequence", &["aggregate_id", "seq"]),
    // Bun source: packages/opencode/src/sync/event.sql.ts EventTable.
    ("event", &["id", "aggregate_id", "seq", "type", "data"]),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_schema_column_sets_match_bun_storage_source() {
    let sidecar = RustSidecar::spawn().await.expect("spawn rust sidecar");
    let db = sidecar.root.path().join("data/kilo/kilo.db");
    let conn = Connection::open(&db).expect("open rust initialized kilo.db");

    for (table, expected) in TABLES {
        let actual = columns(&conn, table);
        // Column order parity keeps Bun and Rust row mappers from silently swapping fields.
        assert_eq!(actual, expected.iter().map(|s| s.to_string()).collect::<Vec<_>>(), "{table} column inventory drift; update this oracle only after Bun schema source changes");
    }

    sidecar.shutdown().await.expect("shutdown rust sidecar");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_writes_then_bun_reads_storage_roundtrip() {
    if !bun_roundtrip_enabled() {
        eprintln!("not_measured rust_writes_then_bun_reads_storage_roundtrip: set KILO_ORACLE_BUN_ROUNDTRIP=1 to launch Bun");
        return;
    }

    let sidecar = RustSidecar::spawn().await.expect("spawn rust sidecar");
    let root = sidecar.root.path().to_path_buf();
    let repo = sidecar.repo();
    let ids = write_rust_cases(&sidecar, &repo).await;
    let rust = snapshot(&sidecar.client, &ids).await;
    sidecar
        .shutdown()
        .await
        .expect("shutdown rust before Bun opens sqlite");

    let mut bun = spawn_bun(&root, &repo).await;
    let client = OracleClient::unscoped("127.0.0.1", bun.ready.port, Some(&bun.password))
        .expect("bun client");
    let got = snapshot(&client, &ids).await;

    // Normalized snapshots prove Bun can decode Rust's rows without ID or timestamp volatility.
    assert_eq!(normalize(got, &root), normalize(rust, &root));
    bun.shutdown().await.expect("stop Bun sidecar");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bun_writes_then_rust_reads_storage_roundtrip() {
    if !bun_roundtrip_enabled() {
        eprintln!("not_measured bun_writes_then_rust_reads_storage_roundtrip: set KILO_ORACLE_BUN_ROUNDTRIP=1 to launch Bun");
        return;
    }

    let tmp = tempfile::tempdir().expect("temp root");
    let root = tmp.path().to_path_buf();
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).expect("repo dir");
    std::fs::write(repo.join("note.txt"), "needle\n").expect("repo note");
    let mut bun = spawn_bun(&root, &repo).await;
    let client = OracleClient::unscoped("127.0.0.1", bun.ready.port, Some(&bun.password))
        .expect("bun client");
    let ids = write_bun_cases(&client, &repo).await;
    let bun_snapshot = snapshot(&client, &ids).await;
    bun.shutdown()
        .await
        .expect("stop Bun before direct Rust sqlite read");

    let rust_snapshot = sqlite_snapshot(&root.join("data/kilo/kilo.db"), &ids);
    // The Rust-side sqlite decoder must see Bun-authored session/message payloads in the same shape.
    assert_eq!(
        normalize(rust_snapshot, &root),
        normalize(bun_snapshot, &root)
    );
}

fn columns(conn: &Connection, table: &str) -> Vec<String> {
    let mut stmt = conn
        .prepare(&format!("pragma table_info({table})"))
        .expect("pragma table_info");
    stmt.query_map([], |row| row.get::<_, String>(1))
        .expect("query columns")
        .map(|r| r.expect("column row"))
        .collect()
}

async fn write_rust_cases(sidecar: &RustSidecar, repo: &Path) -> Vec<String> {
    let mut ids = Vec::new();
    let single = create_session(&sidecar.client, repo, "roundtrip single")
        .await
        .expect("single session");
    prompt_async(
        &sidecar.client,
        &single,
        &fake_prompt("single message", json!({ "fake": true })),
    )
    .await
    .expect("single prompt");
    ids.push(single.clone());

    let tools = create_session(&sidecar.client, repo, "roundtrip tools")
        .await
        .expect("tool session");
    let watch = tools.clone();
    let client = sidecar.client.clone();
    let task =
        tokio::spawn(
            async move { record_until_idle(&client, watch, Duration::from_secs(5)).await },
        );
    prompt_async(&sidecar.client, &tools, &fake_prompt("multi tool", json!({ "fakeToolCalls": [{ "tool": "read", "input": { "filePath": "note.txt" } }, { "tool": "bash", "input": { "command": "echo ok" } }] }))).await.expect("tool prompt");
    task.await.expect("join idle").expect("idle");
    ids.push(tools.clone());

    let aborted = create_session(&sidecar.client, repo, "roundtrip aborted")
        .await
        .expect("aborted session");
    let base = spawn_stalling_oauth_stub(
        "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking\"}}]}\n\n",
    )
    .await;
    prompt_async(
        &sidecar.client,
        &aborted,
        &fake_prompt(
            "abort",
            json!({ "baseURL": base, "apiKey": "x", "model": "fake" }),
        ),
    )
    .await
    .expect("abort prompt");
    tokio::time::sleep(Duration::from_millis(100)).await;
    abort(&sidecar.client, &aborted).await.expect("abort turn");
    ids.push(aborted.clone());

    let archived = create_session(&sidecar.client, repo, "roundtrip archived")
        .await
        .expect("archived session");
    sidecar
        .client
        .patch_json(
            &format!("/session/{archived}"),
            &json!({ "time": { "archived": 1234567890123_i64 } }),
        )
        .await
        .expect("archive patch");
    ids.push(archived.clone());

    let forked: Value = sidecar
        .client
        .post_json(&format!("/session/{single}/fork"), Some(&json!({})))
        .await
        .expect("fork session");
    ids.push(forked["id"].as_str().expect("fork id").to_string());
    sidecar.client.post_json(&format!("/session/{single}/revert"), Some(&json!({ "messageID": "msg_revert", "summary": { "additions": 1, "deletions": 2, "files": 1, "diffs": [{ "file": "note.txt", "additions": 1, "deletions": 2 }] } }))).await.expect("summary revert");
    ids
}

async fn write_bun_cases(client: &OracleClient, repo: &Path) -> Vec<String> {
    let body = json!({ "title": "bun roundtrip single", "directory": repo.to_string_lossy() });
    let res = client
        .post_json("/session", Some(&body))
        .await
        .expect("bun create");
    let id = res["id"].as_str().expect("bun id").to_string();
    prompt_async(
        client,
        &id,
        &fake_prompt("bun message", json!({ "fake": true })),
    )
    .await
    .expect("bun prompt");
    vec![id]
}

async fn snapshot(client: &OracleClient, ids: &[String]) -> Value {
    let sessions = client.get_json("/session").await.expect("GET /session");
    let mut out = Vec::new();
    for id in ids {
        out.push((
            id,
            messages(client, id)
                .await
                .expect("GET /session/{id}/message"),
        ));
    }
    let messages = out;
    json!({ "sessions": sessions, "messages": messages })
}

fn sqlite_snapshot(db: &Path, ids: &[String]) -> Value {
    let conn = Connection::open(db).expect("open sqlite snapshot");
    let sessions: Vec<Value> = ids.iter().map(|id| {
        conn.query_row("select id, title, parent_id, summary_diffs, revert, time_archived from session where id = ?1", [id], |row| Ok(json!({ "id": row.get::<_, String>(0)?, "title": row.get::<_, String>(1)?, "parentID": row.get::<_, Option<String>>(2)?, "summaryDiffs": row.get::<_, Option<String>>(3)?, "revert": row.get::<_, Option<String>>(4)?, "archived": row.get::<_, Option<i64>>(5)? }))).expect("session row")
    }).collect();
    let messages: Vec<Value> = ids
        .iter()
        .map(|id| json!((id, messages_sql(&conn, id))))
        .collect();
    json!({ "sessions": sessions, "messages": messages })
}

fn messages_sql(conn: &Connection, id: &str) -> Vec<Value> {
    let mut stmt = conn
        .prepare("select id, data from message where session_id = ?1 order by time_created, id")
        .expect("message sql");
    stmt.query_map([id], |row| Ok(json!({ "id": row.get::<_, String>(0)?, "info": serde_json::from_str::<Value>(&row.get::<_, String>(1)?).unwrap() }))).expect("message rows").map(|r| r.expect("message row")).collect()
}

async fn spawn_bun(root: &Path, repo: &Path) -> SidecarHandle {
    let cfg = SpawnConfig::default()
        .with_cwd(repo.to_path_buf())
        .with_extra_env("HOME", root.join("home").to_string_lossy())
        .with_extra_env("USERPROFILE", root.join("home").to_string_lossy())
        .with_extra_env("XDG_DATA_HOME", root.join("data").to_string_lossy())
        .with_extra_env("XDG_CONFIG_HOME", root.join("config").to_string_lossy())
        .with_extra_env("XDG_STATE_HOME", root.join("state").to_string_lossy());
    SidecarHandle::spawn(cfg).await.expect("spawn Bun sidecar")
}

fn normalize(value: Value, root: &Path) -> Value {
    let mut norm = Normalizer::new(Redactions::default().with_workspace(root));
    norm.normalize_value(&value)
}

fn bun_roundtrip_enabled() -> bool {
    std::env::var("KILO_ORACLE_BUN_ROUNDTRIP").ok().as_deref() == Some("1")
}
