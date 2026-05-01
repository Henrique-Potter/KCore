//! Live fixture recorder for M0.
//!
//! Run with:
//!
//! ```text
//! KILO_ORACLE_RECORD=1 cargo test -p kilo-oracle --tests --ignored record_fixtures
//! ```
//!
//! This test is `#[ignore]`d by default so the regular `cargo test -p kilo-oracle`
//! run never spawns the bundled `kilo.exe` and never writes to the fixtures
//! directory. Only invocations that explicitly set `KILO_ORACLE_RECORD=1`
//! perform the live recording.
//!
//! What it does:
//!
//! 1. Hashes the bundled `kilo.exe` so each fixture carries a provenance
//!    SHA-256 next to its `captured_at_iso` timestamp.
//! 2. Spawns `kilo.exe serve --port 0` in a fresh tempdir, with `HOME` and
//!    `USERPROFILE` overridden to point inside the tempdir as well — the Bun
//!    sidecar respects XDG paths derived from `HOME` over `KILO_HOME` for
//!    most of its on-disk state, so without this override the recorder would
//!    write fixtures derived from the developer's real `~/.local/share/kilo`.
//! 3. Captures three SSE frames from `GET /global/event` (the bootstrap
//!    `server.connected` followed by two `server.heartbeat`s).
//! 4. Calls `GET /global/health` to capture the live `version` string for the
//!    SSE meta + the store fixture.
//! 5. Shuts the sidecar down, then walks the tempdir and produces the store
//!    fixture inventory.
//! 6. Writes all three fixtures (`fixtures/sse/global-event-bootstrap.{meta.json,jsonl}`,
//!    `fixtures/startup/ready-line.json`, `fixtures/store/empty.json`) using
//!    the same `Normalizer` pipeline the synthetic scenarios use, with the
//!    additional capture-provenance fields populated.
//!
//! The recorder is deliberately a single sequential test rather than three
//! parallel ones: each fixture wants the *same* spawn instance for a coherent
//! `bun_binary_sha256` and `captured_at_iso`, and parallel test runs would
//! both bind a port and race for the fixtures directory.

#![allow(clippy::needless_pass_by_value)]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use chrono::Utc;
use kilo_oracle::{
    default_binary_path, Fixture, FixtureFile, FixtureMeta, Normalizer, OracleClient, Redactions,
    SidecarHandle, SpawnConfig, SseRecorder, StartupScenario, StopCondition, CONTRACT_VERSION,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn fixtures_root() -> PathBuf {
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("fixtures")
}

/// Compute the lowercase hex SHA-256 of a file. Reads in 1 MiB chunks so the
/// 178 MiB Bun binary doesn't sit in memory all at once.
fn hash_file(path: &Path) -> std::io::Result<String> {
    use std::fs::File;
    use std::io::Read;
    let mut f = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Walk `root` recursively and produce the store-fixture inventory shape
/// documented in `fixtures/store/empty.json::expected_shape`. Files smaller
/// than 16 KiB carry an SHA-256; larger files carry size only.
fn walk_store_inventory(root: &Path) -> std::io::Result<Vec<Value>> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<Value>) -> std::io::Result<()> {
        if !dir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let ft = entry.file_type()?;
            if ft.is_dir() {
                walk(root, &path, out)?;
            } else if ft.is_file() {
                let meta = entry.metadata()?;
                let size_bytes = meta.len();
                let rel = path
                    .strip_prefix(root)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/");
                let mut record = json!({
                    "path": rel,
                    "size_bytes": size_bytes,
                });
                if size_bytes < 16 * 1024 {
                    if let Ok(h) = hash_file(&path) {
                        record["sha256"] = Value::String(h);
                    }
                }
                out.push(record);
            }
        }
        Ok(())
    }

    let mut out = Vec::new();
    walk(root, root, &mut out)?;
    // Stable sort by path for byte-deterministic fixtures across runs.
    out.sort_by(|a, b| {
        a.get("path")
            .and_then(|p| p.as_str())
            .unwrap_or("")
            .cmp(b.get("path").and_then(|p| p.as_str()).unwrap_or(""))
    });
    Ok(out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "live recorder; requires KILO_ORACLE_RECORD=1 and a real kilo.exe"]
async fn record_fixtures() {
    if std::env::var("KILO_ORACLE_RECORD").as_deref() != Ok("1") {
        eprintln!(
            "record_fixtures: KILO_ORACLE_RECORD is not 1; skipping. \
             Set KILO_ORACLE_RECORD=1 to run."
        );
        return;
    }

    let bin = default_binary_path();
    assert!(
        bin.exists(),
        "kilo.exe not found at {} — build and bundle it first",
        bin.display()
    );

    let bun_binary_sha256 =
        hash_file(&bin).unwrap_or_else(|e| panic!("hash {}: {e}", bin.display()));

    // Pick the cwd up-front so we can point HOME/USERPROFILE at it too. This
    // matters because Bun reads / writes its store under XDG paths
    // (`$HOME/.local/share/kilo/`) and only honors `KILO_HOME` for a subset of
    // state. Without overriding HOME, the recorder would mix the developer's
    // real store into the captured fixture.
    let tmp = tempfile::tempdir().expect("create recording tempdir");
    let cwd = tmp.path().to_path_buf();

    let cfg = SpawnConfig::default()
        .with_binary(bin.clone())
        .with_cwd(cwd.clone())
        .with_extra_env("HOME", cwd.to_string_lossy())
        .with_extra_env("USERPROFILE", cwd.to_string_lossy())
        // Belt-and-suspenders: pin XDG paths inside cwd too. Bun follows
        // these on platforms that respect XDG.
        .with_extra_env("XDG_CACHE_HOME", cwd.join(".cache").to_string_lossy())
        .with_extra_env("XDG_DATA_HOME", cwd.join(".local/share").to_string_lossy())
        .with_extra_env("XDG_CONFIG_HOME", cwd.join(".config").to_string_lossy());

    let started_spawn = Instant::now();
    let mut sidecar = SidecarHandle::spawn(cfg)
        .await
        .expect("spawn kilo.exe failed");
    let startup_to_ready = started_spawn.elapsed();
    eprintln!(
        "record_fixtures: ready in {:?}, port={}",
        startup_to_ready, sidecar.ready.port
    );

    let client = OracleClient::unscoped("127.0.0.1", sidecar.ready.port, Some(&sidecar.password))
        .expect("build oracle client");

    // 1. Hit /global/health for the version string we'll record alongside the
    //    SSE meta and the store fixture. This also serves as a smoke test for
    //    the auth path before we open the SSE stream.
    let health = client
        .global_health()
        .await
        .expect("GET /global/health failed");
    let kilo_server_version = health
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    eprintln!(
        "record_fixtures: /global/health -> healthy={:?} version={:?}",
        health.get("healthy"),
        kilo_server_version
    );

    // 2. Open /global/event and capture three frames. Bound the entire SSE
    //    capture in a tokio timeout so the test cannot wedge the suite.
    let response = client
        .open_global_event_stream()
        .await
        .expect("open SSE stream failed");
    let frames = tokio::time::timeout(
        Duration::from_secs(45),
        SseRecorder::new().record(response, StopCondition::Frames(3)),
    )
    .await
    .expect("SSE recording timed out at 45s")
    .expect("SSE recording failed");

    assert_eq!(
        frames.len(),
        3,
        "expected exactly 3 SSE frames (connected + 2 heartbeats), got {}",
        frames.len()
    );

    // Compute observed heartbeat interval from raw (pre-normalization) ms.
    let hb1 = frames[1].wall_offset_ms;
    let hb2 = frames[2].wall_offset_ms;
    let observed_heartbeat_interval_ms = hb2.saturating_sub(hb1);
    eprintln!(
        "record_fixtures: SSE wall offsets ms = [{}, {}, {}], heartbeat_interval≈{}ms",
        frames[0].wall_offset_ms, hb1, hb2, observed_heartbeat_interval_ms
    );

    // 3. Run the canonical StartupScenario writer so the on-disk shape
    //    continues to match the byte-for-byte test in
    //    tests/startup_recorder.rs. We reproduce its steps inline rather than
    //    going through ScenarioRunner so we can also write the SSE + store
    //    fixtures from the same handle.
    let captured_at_iso = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let startup_value = StartupScenario::build_value(&sidecar.ready, &health);

    let mut startup_normalizer = Normalizer::new(
        Redactions::default()
            .with_password(sidecar.password.clone())
            .with_port(sidecar.ready.port)
            .with_workspace(sidecar.cwd.clone()),
    );
    let startup_path = fixtures_root().join("startup/ready-line.json");
    FixtureFile::new(&startup_path)
        .write_value(&startup_value, &mut startup_normalizer)
        .expect("write startup fixture");
    eprintln!(
        "record_fixtures: wrote startup fixture at {}",
        startup_path.display()
    );

    // 4. Convert the captured SSE frames into FixtureFrames and write the SSE
    //    meta + JSONL fixture, with the new capture-provenance fields
    //    populated.
    let fixture_frames = SseRecorder::to_fixture_frames(frames);
    let mut sse_normalizer = Normalizer::new(
        Redactions::default()
            .with_password(sidecar.password.clone())
            .with_port(sidecar.ready.port)
            .with_workspace(sidecar.cwd.clone()),
    );
    let mut meta = FixtureMeta::new("global-event", sse_normalizer.map().clone());
    meta.captured = Some(true);
    meta.captured_at_iso = Some(captured_at_iso.clone());
    meta.bun_binary_sha256 = Some(bun_binary_sha256.clone());
    meta.kilo_server_version = Some(kilo_server_version.clone());
    meta.observed_heartbeat_interval_ms = Some(observed_heartbeat_interval_ms);

    let sse_base = fixtures_root().join("sse/global-event-bootstrap");
    let sse_fixture = Fixture::at(&sse_base);
    sse_fixture
        .write(&meta, &fixture_frames, &mut sse_normalizer)
        .expect("write sse fixture");
    eprintln!(
        "record_fixtures: wrote SSE fixture pair at {} + {}",
        sse_fixture.meta_path.display(),
        sse_fixture.frames_path.display()
    );

    // 5. Stop the sidecar before we walk its store. Otherwise the SQLite WAL
    //    can race with our directory traversal.
    sidecar
        .shutdown()
        .await
        .expect("kill sidecar before store walk");
    drop(sidecar);
    // Brief settle so any final fsync lands on disk before we hash.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // 6. Walk the entire tempdir (HOME + KILO_HOME + XDG dirs all live under
    //    `cwd`). We deliberately walk the whole tempdir rather than just
    //    `<cwd>/.kilo` because Bun puts most of its state under XDG paths,
    //    not `.kilo` — a fact this M0 recording session exposes for the
    //    first time in fixture form.
    let store_inventory = walk_store_inventory(&cwd).expect("walk store inventory");
    eprintln!(
        "record_fixtures: captured {} files under {}",
        store_inventory.len(),
        cwd.display()
    );
    for f in &store_inventory {
        eprintln!("  - {}", f);
    }

    let store_value = json!({
        "contract_version": CONTRACT_VERSION,
        "scenario": "store-empty",
        "description": "First-launch on-disk layout produced by `kilo serve --port 0` against a fresh HOME tempdir, after a single `/global/health` GET and graceful shutdown. Captures the real Bun layout: SQLite database (`kilo.db` + WAL/SHM), version stamp, telemetry id, and a startup log under XDG paths derived from HOME — not the hand-coded `config/global.json` shape the previous placeholder advertised.",
        "captured": true,
        "captured_at_iso": captured_at_iso,
        "bun_binary_sha256": bun_binary_sha256,
        "kilo_server_version": kilo_server_version,
        "expected_shape": {
            "format": "List of relative paths under the recording tempdir (HOME + KILO_HOME + XDG dirs all rooted there). `size_bytes` is recorded for every file; `sha256` only for files smaller than 16 KiB. SQLite WAL/SHM files have unstable contents and are deliberately captured as size-only when they exceed 16 KiB, but for the empty-store baseline most are well under that threshold.",
            "files": store_inventory,
        },
        "notes": [
            "When Rust eventually owns the store, the diff between this fixture and Rust's freshly-created store is the read-side compatibility gate for milestone 6.",
            "The `kilo.db*` files are SQLite write-ahead log artifacts. Bun creates them eagerly even if no rows have been inserted, so they show up in this fixture even though no session work has happened.",
            "`telemetry-id` is a single-line random ID Bun writes once per host. Treated as opaque content here; the Rust port must produce-or-preserve this file but the value is not part of the contract.",
            "Re-run via `KILO_ORACLE_RECORD=1 cargo test -p kilo-oracle --tests --ignored record_fixtures`."
        ]
    });

    let store_path = fixtures_root().join("store/empty.json");
    let mut store_normalizer = Normalizer::new(Redactions::default());
    FixtureFile::new(&store_path)
        .write_value(&store_value, &mut store_normalizer)
        .expect("write store fixture");
    eprintln!(
        "record_fixtures: wrote store fixture at {}",
        store_path.display()
    );

    eprintln!(
        "record_fixtures: success — captured_at_iso={}, sha256={}, version={}",
        captured_at_iso, bun_binary_sha256, kilo_server_version
    );
}
