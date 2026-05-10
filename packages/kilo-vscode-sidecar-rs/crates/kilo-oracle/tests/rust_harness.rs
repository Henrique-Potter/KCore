//! Deterministic Rust sidecar harness for oracle replay tests.
//!
//! These helpers intentionally live under integration tests so the main
//! `kilo-oracle` library keeps its Bun-oracle boundary and does not depend on
//! `kilo-server` in normal builds.
//!
//! This module is `#[path]`-included from multiple oracle integration test
//! binaries; each binary uses a different subset of the helpers, so rustc's
//! per-binary unused-fn detection always reports a long false-positive list.
//! Suppress the noise here once instead of decorating every helper.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use kilo_oracle::error::{OracleError, OracleResult};
use kilo_oracle::fixture::{Fixture, FixtureFrame, FixtureMeta};
use kilo_oracle::http::OracleClient;
use kilo_oracle::normalize::{Normalizer, Redactions};
use kilo_oracle::sse::{SseFrame, SseRecorder, StopCondition};
use kilo_server::{serve, ServeOptions};
use serde_json::{json, Value};
use tokio::sync::oneshot;
use tokio::time::timeout;

static ENV_LOCK: Mutex<()> = Mutex::new(());

pub struct RustSidecar {
    pub root: tempfile::TempDir,
    pub client: OracleClient,
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    /// Audit Fix 12: panic-safe env + cwd restore. The previous shape did
    /// the restore manually inside `shutdown()` between the timeout block
    /// and the env restore — if any code in that window panicked, the
    /// guard dropped via stack unwind but env stayed dirty for the next
    /// test under the same poisoned ENV_LOCK. `EnvScope`'s Drop runs the
    /// restore unconditionally.
    scope: Option<EnvScope>,
    guard: Option<MutexGuard<'static, ()>>,
}

/// Audit Fix 12: RAII env+cwd restore. Captures the parent's
/// `EnvRestore` + cwd; on `Drop`, set the cwd and restore the env
/// variables — unless `mark_restored()` ran first (the happy path).
struct EnvScope {
    prev_env: Option<EnvRestore>,
    prev_cwd: Option<PathBuf>,
}

impl EnvScope {
    fn new(prev_env: EnvRestore, prev_cwd: PathBuf) -> Self {
        Self {
            prev_env: Some(prev_env),
            prev_cwd: Some(prev_cwd),
        }
    }

    fn mark_restored(&mut self) {
        if let Some(cwd) = self.prev_cwd.take() {
            let _ = std::env::set_current_dir(cwd);
        }
        if let Some(env) = self.prev_env.take() {
            env.restore();
        }
    }
}

impl Drop for EnvScope {
    fn drop(&mut self) {
        // If mark_restored() didn't run (panic between timeout and
        // restore in shutdown()), the next test that picks up the
        // poisoned ENV_LOCK still sees clean state.
        self.mark_restored();
    }
}

impl RustSidecar {
    pub async fn spawn() -> OracleResult<Self> {
        // ENV_LOCK serializes process-global env + cwd mutation across tests.
        // Guard must outlive the sidecar's last DB access AND env restore, so
        // we hold it until shutdown(). If a prior test panicked, the lock is
        // poisoned but the inner data is intact — recover it.
        let guard = ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner());
        let root = tempfile::tempdir()
            .map_err(|err| OracleError::Other(format!("create tempdir: {err}")))?;
        let repo = root.path().join("repo");
        std::fs::create_dir_all(&repo).map_err(|err| OracleError::Io {
            path: repo.clone(),
            source: err,
        })?;
        std::fs::write(repo.join("note.txt"), "needle\nsecond\n").map_err(|err| {
            OracleError::Io {
                path: repo.join("note.txt"),
                source: err,
            }
        })?;
        std::fs::create_dir_all(repo.join("src")).map_err(|err| OracleError::Io {
            path: repo.join("src"),
            source: err,
        })?;
        std::fs::write(repo.join("src").join("main.rs"), "fn main() {}\n").map_err(|err| {
            OracleError::Io {
                path: repo.join("src").join("main.rs"),
                source: err,
            }
        })?;

        seed_store(root.path())?;

        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .map_err(|err| OracleError::Other(format!("bind test listener: {err}")))?;
        let addr: SocketAddr = listener
            .local_addr()
            .map_err(|err| OracleError::Other(format!("test listener addr: {err}")))?;
        drop(listener);

        let (tx, rx) = oneshot::channel::<()>();
        let home = root.path().join("home");
        let data = root.path().join("data");
        let cfg = root.path().join("config");
        let state = root.path().join("state");

        // Mutate env + cwd on the outer thread under the held ENV_LOCK guard.
        // Doing this inside `tokio::spawn` would let other tests interleave
        // their env mutation against ours, since `Store::new()` resolves paths
        // lazily by reading env at request-time.
        let prev_cwd = std::env::current_dir()
            .map_err(|err| OracleError::Other(format!("current_dir: {err}")))?;
        let prev_env = EnvRestore::capture([
            "KILO_TEST_HOME",
            "HOME",
            "USERPROFILE",
            "XDG_DATA_HOME",
            "XDG_CONFIG_HOME",
            "XDG_STATE_HOME",
            "KILO_SERVER_PASSWORD",
        ]);
        std::env::set_var("KILO_TEST_HOME", &home);
        std::env::set_var("HOME", &home);
        std::env::set_var("USERPROFILE", &home);
        std::env::set_var("XDG_DATA_HOME", &data);
        std::env::set_var("XDG_CONFIG_HOME", &cfg);
        std::env::set_var("XDG_STATE_HOME", &state);
        std::env::remove_var("KILO_SERVER_PASSWORD");
        std::env::set_current_dir(&repo)
            .map_err(|err| OracleError::Other(format!("set_current_dir: {err}")))?;

        let task = tokio::spawn(async move {
            serve(
                ServeOptions {
                    hostname: "127.0.0.1".to_string(),
                    port: addr.port(),
                },
                async move {
                    let _ = rx.await;
                },
            )
            .await
        });

        let client = OracleClient::unscoped("127.0.0.1", addr.port(), None)?;
        wait_health(&client).await?;
        Ok(Self {
            root,
            client,
            stop: Some(tx),
            task,
            scope: Some(EnvScope::new(prev_env, prev_cwd)),
            guard: Some(guard),
        })
    }

    pub fn repo(&self) -> PathBuf {
        self.root.path().join("repo")
    }

    pub async fn shutdown(mut self) -> OracleResult<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let res = timeout(Duration::from_secs(5), &mut self.task)
            .await
            .map_err(|_| OracleError::ScenarioAborted("rust sidecar shutdown timed out"))?
            .map_err(|err| OracleError::Other(format!("rust sidecar task join: {err}")))?
            .map_err(|err| OracleError::Other(format!("rust sidecar serve: {err}")));
        // Audit Fix 12: explicit happy-path restore. `EnvScope::Drop`
        // backs us up if anything between here and the function return
        // panics.
        if let Some(mut scope) = self.scope.take() {
            scope.mark_restored();
        }
        self.guard.take();
        res
    }
}

fn seed_store(root: &Path) -> OracleResult<()> {
    let data = root.join("data").join("kilo");
    std::fs::create_dir_all(&data).map_err(|err| OracleError::Io {
        path: data.clone(),
        source: err,
    })?;
    let db = rusqlite::Connection::open(data.join("kilo.db"))
        .map_err(|err| OracleError::Other(format!("open seed db: {err}")))?;
    db.execute_batch(
        "create table project (
            id text primary key,
            worktree text not null,
            vcs text,
            name text,
            icon_url text,
            icon_url_override text,
            icon_color text,
            time_created integer not null,
            time_updated integer not null,
            time_initialized integer,
            sandboxes text not null,
            commands text
        );
        create table session (
            id text primary key,
            project_id text not null references project(id) on delete cascade,
            workspace_id text,
            parent_id text,
            slug text not null,
            directory text not null,
            title text not null,
            version text not null,
            share_url text,
            summary_additions integer,
            summary_deletions integer,
            summary_files integer,
            summary_diffs text,
            revert text,
            permission text,
            time_created integer not null,
            time_updated integer not null,
            time_compacting integer,
            time_archived integer
        );
        create table message (
            id text primary key,
            session_id text not null references session(id) on delete cascade,
            time_created integer not null,
            time_updated integer not null,
            data text not null
        );
        create table part (
            id text primary key,
            message_id text not null references message(id) on delete cascade,
            session_id text not null,
            time_created integer not null,
            time_updated integer not null,
            data text not null
        );
        create table event_sequence (
            aggregate_id text not null primary key,
            seq integer not null
        );
        create table event (
            id text primary key,
            aggregate_id text not null references event_sequence(aggregate_id) on delete cascade,
            seq integer not null,
            type text not null,
            data text not null
        );",
    )
    .map_err(|err| OracleError::Other(format!("seed db schema: {err}")))
}

impl Drop for RustSidecar {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        // Audit Fix 12: env + cwd restore now lives in `EnvScope::Drop`
        // — drop the scope here (or rely on field drop order) so a panic
        // anywhere in the path between `shutdown()`'s timeout and the
        // env restore can't leave env state dirty for the next test.
        drop(self.scope.take());
    }
}

struct EnvRestore {
    values: Vec<(&'static str, Option<String>)>,
}

impl EnvRestore {
    fn capture<const N: usize>(keys: [&'static str; N]) -> Self {
        Self {
            values: keys
                .into_iter()
                .map(|key| (key, std::env::var(key).ok()))
                .collect(),
        }
    }

    fn restore(self) {
        for (key, value) in self.values {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

async fn wait_health(client: &OracleClient) -> OracleResult<()> {
    let started = std::time::Instant::now();
    loop {
        if let Ok(value) = client.global_health().await {
            if value.get("healthy").and_then(Value::as_bool) == Some(true) {
                return Ok(());
            }
        }
        if started.elapsed() > Duration::from_secs(5) {
            return Err(OracleError::ReadyTimeout { timeout_secs: 5 });
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub async fn create_session(
    client: &OracleClient,
    dir: &Path,
    title: &str,
) -> OracleResult<String> {
    let body = json!({ "title": title, "directory": dir.to_string_lossy() });
    let res: Value = client.post_json("/session", Some(&body)).await?;
    res.get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| OracleError::other("session create did not return id"))
}

pub fn fake_prompt(text: &str, provider: Value) -> Value {
    json!({
        "parts": [{ "type": "text", "text": text }],
        "provider": provider,
    })
}

/// Spawn a TCP listener that accepts one connection, sends the given SSE
/// `prefix` bytes as a chunked-encoded body, then stalls for ~5s before
/// closing. Used by the M7 mid-stream-abort OAuth integration test:
/// streams a real text delta so the client commits a partial part, then
/// withholds further bytes so the cancel-during-`bytes.next()` race is
/// what unblocks the request. Returns the `http://addr` base URL.
pub async fn spawn_stalling_oauth_stub(prefix: &'static str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            // Eat the request bytes so the client side is unblocked.
            let mut buf = vec![0u8; 8192];
            let _ = tokio::io::AsyncReadExt::read(&mut socket, &mut buf).await;
            let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n";
            let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, head.as_bytes()).await;
            let chunk = format!("{:x}\r\n{}\r\n", prefix.len(), prefix);
            let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, chunk.as_bytes()).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
            let _ = tokio::io::AsyncWriteExt::write_all(&mut socket, b"0\r\n\r\n").await;
        }
    });
    format!("http://{addr}")
}

/// Persist a provider auth blob via the sidecar HTTP surface
/// (`PUT /auth/{provider_id}`). Used by the OAuth-path orphan check to
/// install an OAuth token without reaching into `kilo_store::Store`
/// privately. We bypass the `OracleClient` here because that wrapper
/// does not expose a generic PUT helper.
pub async fn put_provider_auth(
    client: &OracleClient,
    provider_id: &str,
    auth: Value,
) -> OracleResult<()> {
    let url = format!(
        "{}auth/{provider_id}",
        client.base().as_str().trim_end_matches('/').to_string() + "/"
    );
    let res = reqwest::Client::new()
        .put(url)
        .json(&auth)
        .send()
        .await
        .map_err(|err| OracleError::Other(format!("put auth: {err}")))?;
    if !res.status().is_success() {
        return Err(OracleError::Other(format!(
            "put auth status: {}",
            res.status()
        )));
    }
    Ok(())
}

/// Write the kilo.json provider config under the sidecar's
/// `XDG_CONFIG_HOME` (set by `RustSidecar::spawn` to `{root}/config`).
/// `provider_block` is the inner value of `provider.openai` (i.e. it will
/// be inserted under `provider.openai`).
pub fn write_openai_config(root: &Path, provider_block: Value) -> OracleResult<()> {
    let dir = root.join("config").join("kilo");
    std::fs::create_dir_all(&dir).map_err(|err| OracleError::Io {
        path: dir.clone(),
        source: err,
    })?;
    let path = dir.join("kilo.json");
    let body = serde_json::json!({
        "provider": { "openai": provider_block }
    });
    std::fs::write(&path, serde_json::to_string(&body).unwrap()).map_err(|err| OracleError::Io {
        path: path.clone(),
        source: err,
    })
}

pub async fn prompt_async(client: &OracleClient, id: &str, body: &Value) -> OracleResult<()> {
    let _: Value = client
        .post_json(&format!("/session/{id}/prompt_async"), Some(body))
        .await?;
    Ok(())
}

pub async fn abort(client: &OracleClient, id: &str) -> OracleResult<()> {
    let _: Value = client
        .post_json::<Value>(&format!("/session/{id}/abort"), None)
        .await?;
    Ok(())
}

pub async fn messages(client: &OracleClient, id: &str) -> OracleResult<Vec<Value>> {
    let value = client.get_json(&format!("/session/{id}/message")).await?;
    Ok(value.as_array().cloned().unwrap_or_default())
}

pub async fn record_until_idle(
    client: &OracleClient,
    id: String,
    limit: Duration,
) -> OracleResult<Vec<SseFrame>> {
    let response = client.open_global_event_stream().await?;
    timeout(
        limit,
        SseRecorder::new().record(
            response,
            StopCondition::Predicate(Box::new(move |_frame, parsed| {
                payload_type(parsed) == Some("session.idle")
                    && payload_session(parsed) == Some(id.as_str())
            })),
        ),
    )
    .await
    .map_err(|_| OracleError::ScenarioAborted("timed out waiting for session idle"))?
}

pub async fn record_until_turn_close(
    client: &OracleClient,
    id: String,
    limit: Duration,
) -> OracleResult<Vec<SseFrame>> {
    let response = client.open_global_event_stream().await?;
    timeout(
        limit,
        SseRecorder::new().record(
            response,
            StopCondition::Predicate(Box::new(move |_frame, parsed| {
                payload_type(parsed) == Some("session.turn.close")
                    && payload_session(parsed) == Some(id.as_str())
            })),
        ),
    )
    .await
    .map_err(|_| OracleError::ScenarioAborted("timed out waiting for session turn close"))?
}

pub async fn record_until_both_idle(
    client: &OracleClient,
    a: String,
    b: String,
    limit: Duration,
) -> OracleResult<Vec<SseFrame>> {
    let response = client.open_global_event_stream().await?;
    let seen = std::sync::Arc::new(std::sync::Mutex::new((false, false)));
    let flag = seen.clone();
    timeout(
        limit,
        SseRecorder::new().record(
            response,
            StopCondition::Predicate(Box::new(move |_frame, parsed| {
                if payload_type(parsed) != Some("session.idle") {
                    return false;
                }
                let Some(id) = payload_session(parsed) else {
                    return false;
                };
                let mut guard = flag.lock().unwrap();
                if id == a {
                    guard.0 = true;
                }
                if id == b {
                    guard.1 = true;
                }
                guard.0 && guard.1
            })),
        ),
    )
    .await
    .map_err(|_| OracleError::ScenarioAborted("timed out waiting for both sessions idle"))?
}

pub fn write_fixture(
    base: &Path,
    scenario: &str,
    frames: Vec<SseFrame>,
    root: &Path,
) -> OracleResult<Vec<FixtureFrame>> {
    let mut normalizer = Normalizer::new(Redactions::default().with_workspace(root));
    let mut meta = FixtureMeta::new(scenario, normalizer.map().clone());
    meta.captured = Some(false);
    meta.captured_at_iso = Some("deterministic-rust-fixture".to_string());
    meta.kilo_server_version = Some("rust-sidecar-test-harness".to_string());
    let frames = SseRecorder::to_fixture_frames(frames);
    let fixture = Fixture::at(base);
    fixture.write(&meta, &frames, &mut normalizer)?;
    fixture.read_frames()
}

pub fn payload_type(parsed: &Option<Value>) -> Option<&str> {
    parsed.as_ref()?.get("payload")?.get("type")?.as_str()
}

pub fn payload_session(parsed: &Option<Value>) -> Option<&str> {
    parsed
        .as_ref()?
        .get("payload")?
        .get("properties")?
        .get("sessionID")?
        .as_str()
}

pub fn frame_type(frame: &FixtureFrame) -> Option<&str> {
    frame.payload.as_ref()?.get("type")?.as_str()
}

pub fn frame_session(frame: &FixtureFrame) -> Option<&str> {
    frame
        .payload
        .as_ref()?
        .get("properties")?
        .get("sessionID")?
        .as_str()
}

pub fn sync_data(frame: &FixtureFrame) -> Option<&Value> {
    frame.payload.as_ref()?.get("syncEvent")?.get("data")
}

pub fn sync_type(frame: &FixtureFrame) -> Option<&str> {
    let raw = frame
        .payload
        .as_ref()?
        .get("syncEvent")?
        .get("type")?
        .as_str()?;
    // Bun emits `<type>.1`; the older Rust harness emitted `<type>.v1`.
    // Normalize to the legacy `.v1` form so the existing test helpers
    // ([`tool_names`], the M7 fixture filters) keep matching across the
    // two formats.
    if raw.ends_with(".1") && !raw.ends_with(".v1") {
        if let Some(stripped) = raw.strip_suffix(".1") {
            return Some(translate_to_v1(stripped));
        }
    }
    Some(raw)
}

fn translate_to_v1(base: &str) -> &'static str {
    match base {
        "message.updated" => "message.updated.v1",
        "message.removed" => "message.removed.v1",
        "message.part.updated" => "message.part.updated.v1",
        "message.part.removed" => "message.part.removed.v1",
        "session.created" => "session.created.v1",
        "session.updated" => "session.updated.v1",
        "session.deleted" => "session.deleted.v1",
        _ => "unknown.v1",
    }
}

pub fn part_type(frame: &FixtureFrame) -> Option<&str> {
    sync_data(frame)?.get("part")?.get("type")?.as_str()
}

pub fn tool_names(frames: &[FixtureFrame]) -> Vec<String> {
    frames
        .iter()
        .filter(|frame| sync_type(frame) == Some("message.part.updated.v1"))
        .filter(|frame| part_type(frame) == Some("tool"))
        .filter_map(|frame| {
            sync_data(frame)?
                .get("part")?
                .get("tool")?
                .as_str()
                .map(str::to_string)
        })
        .collect()
}
