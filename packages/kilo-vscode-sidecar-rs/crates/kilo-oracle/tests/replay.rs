//! Fixture replay determinism — the M0 exit gate.
//!
//! The plan ([`plans/rust-vscode-sidecar-migration-plan.md`](../../../../plans/rust-vscode-sidecar-migration-plan.md))
//! requires "Bun oracle tests pass consistently". Concretely that means: each
//! fixture shipped under `packages/kilo-vscode-sidecar-rs/fixtures/` must
//!
//! - parse as valid JSON / JSONL,
//! - declare the contract version we currently freeze (`kilo-vscode-sidecar.preview.0`),
//! - round-trip through serialization without losing fields,
//! - re-normalize idempotently (running the normalizer on a normalized fixture
//!   must produce byte-identical output).
//!
//! Tests in this file run **without spawning a sidecar** and **without
//! network**. They consume only the on-disk fixture set produced by recording
//! sessions, which keeps `cargo test -p kilo-oracle` reproducible in CI.

use std::path::{Path, PathBuf};

use kilo_oracle::{
    Fixture, FixtureFile, FixtureFrame, FixtureMeta, IdentityMap, Normalizer, Redactions,
    CONTRACT_VERSION,
};
use serde_json::Value;

fn fixtures_root() -> PathBuf {
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("fixtures")
}

fn load_json(path: &Path) -> Value {
    let raw =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {} as JSON: {e}", path.display()))
}

fn load_fixture(name: &str) -> (FixtureMeta, Vec<FixtureFrame>) {
    let base = fixtures_root().join("sse").join(name);
    let fixture = Fixture::at(&base);
    (
        fixture.read_meta().expect("read fixture meta"),
        fixture.read_frames().expect("read fixture frames"),
    )
}

fn frame_type(frame: &FixtureFrame) -> Option<&str> {
    frame.payload.as_ref()?.get("type")?.as_str()
}

fn frame_session(frame: &FixtureFrame) -> Option<&str> {
    frame
        .payload
        .as_ref()?
        .get("properties")?
        .get("sessionID")?
        .as_str()
}

fn sync_type(frame: &FixtureFrame) -> Option<&str> {
    let raw = frame
        .payload
        .as_ref()?
        .get("syncEvent")?
        .get("type")?
        .as_str()?;
    // Bun emits `<type>.1`; older Rust harness emitted `<type>.v1`.
    // Translate `.1` → `.v1` so existing string-literal matchers keep
    // working across both wire formats.
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

fn sync_data(frame: &FixtureFrame) -> Option<&Value> {
    frame.payload.as_ref()?.get("syncEvent")?.get("data")
}

fn part_type(frame: &FixtureFrame) -> Option<&str> {
    sync_data(frame)?.get("part")?.get("type")?.as_str()
}

#[test]
fn startup_fixture_is_well_formed() {
    let path = fixtures_root().join("startup/ready-line.json");
    let v = load_json(&path);
    assert_eq!(
        v.get("contract_version").and_then(|c| c.as_str()),
        Some(CONTRACT_VERSION),
        "fixture {} must declare current contract version",
        path.display()
    );
    let stdout_line = v
        .get("stdout_line")
        .and_then(|c| c.as_str())
        .expect("stdout_line is required");
    assert!(
        stdout_line.starts_with("kilo server listening on http://"),
        "stdout_line must match readiness regex: {stdout_line}"
    );
    assert!(
        stdout_line.contains("<PORT>"),
        "stdout_line must be normalized (contains <PORT>): {stdout_line}"
    );

    let body = v
        .get("health_response_body")
        .expect("health_response_body required");
    assert_eq!(body.get("healthy").and_then(|h| h.as_bool()), Some(true));
}

#[test]
fn startup_fixture_idempotent_through_normalizer() {
    let path = fixtures_root().join("startup/ready-line.json");
    let file = FixtureFile::new(path.clone());
    let v = file.read_value().expect("read startup fixture");

    // Re-normalizing a fully-normalized fixture must be a no-op.
    let mut n = Normalizer::new(Redactions::default());
    let again = n.normalize_value(&v);
    pretty_assertions::assert_eq!(v, again, "startup fixture must be idempotent");
}

#[test]
fn global_event_bootstrap_meta_is_well_formed() {
    let base = fixtures_root().join("sse/global-event-bootstrap");
    let fixture = Fixture::at(&base);
    let meta = fixture.read_meta().expect("read global-event meta");
    assert_eq!(meta.contract_version, CONTRACT_VERSION);
    assert_eq!(meta.scenario, "global-event");

    // Provenance is required when `captured == true`. The recording session
    // populates all four fields together in tests/record_fixtures.rs; if any
    // of them is missing post-recording, that's a recorder bug.
    if meta.captured == Some(true) {
        let sha = meta
            .bun_binary_sha256
            .as_deref()
            .expect("captured meta must carry bun_binary_sha256");
        assert_eq!(
            sha.len(),
            64,
            "bun_binary_sha256 must be a 64-char lowercase hex string; got {sha}"
        );
        assert!(
            sha.chars().all(|c| c.is_ascii_hexdigit()),
            "bun_binary_sha256 must be hex; got {sha}"
        );
        assert!(
            meta.captured_at_iso
                .as_deref()
                .map(|s| s.ends_with('Z') && s.len() >= 20)
                .unwrap_or(false),
            "captured_at_iso must be an ISO-8601 UTC string; got {:?}",
            meta.captured_at_iso
        );
        assert!(
            meta.kilo_server_version
                .as_deref()
                .map(|v| !v.is_empty())
                .unwrap_or(false),
            "kilo_server_version must be non-empty; got {:?}",
            meta.kilo_server_version
        );
        // Heartbeat cadence sanity: the documented value is ~10s. Same band
        // as the per-frame check in tests further down.
        if let Some(hb) = meta.observed_heartbeat_interval_ms {
            assert!(
                (9_000..=11_500).contains(&hb),
                "observed_heartbeat_interval_ms = {hb} outside [9000, 11500]"
            );
        }
    }
}

#[test]
fn global_event_bootstrap_frames_are_well_formed() {
    let base = fixtures_root().join("sse/global-event-bootstrap");
    let fixture = Fixture::at(&base);
    let frames = fixture.read_frames().expect("read global-event frames");
    assert!(
        !frames.is_empty(),
        "global-event-bootstrap.jsonl should not be empty"
    );

    // First frame must be server.connected, captured close to t=0 — but
    // because this fixture is now produced by a real recorder rather than a
    // synthetic generator, `wall_offset_ms` after bucketing can land at 0 or
    // 1000 depending on connection latency. The schema invariant we enforce
    // is "very early in the trace", not "literally zero".
    let first = &frames[0];
    let payload = first
        .payload
        .as_ref()
        .expect("first frame should have a parsed payload");
    assert_eq!(
        payload.get("type").and_then(|t| t.as_str()),
        Some("server.connected"),
        "first frame must be server.connected"
    );
    assert!(
        first.wall_offset_ms <= 1000,
        "first frame should be near t=0 after bucketing; got {}",
        first.wall_offset_ms
    );

    // All subsequent frames are heartbeats. We additionally check that their
    // bucketed offsets fall within [9_000, 11_500] for heartbeat 1 and
    // [19_000, 21_500] for heartbeat 2 — Bun's documented cadence is 10 s,
    // and a 1.5 s late tolerance covers a slow CI host without hiding real
    // drift.
    let expected_bands: [(u64, u64); 2] = [(9_000, 11_500), (19_000, 21_500)];
    for (i, f) in frames[1..].iter().enumerate() {
        let payload = f.payload.as_ref().expect("heartbeat frame must parse");
        assert_eq!(
            payload.get("type").and_then(|t| t.as_str()),
            Some("server.heartbeat"),
            "non-first frame should be server.heartbeat"
        );
        assert_eq!(
            f.wall_offset_ms % 1000,
            0,
            "wall_offset_ms must be bucketed to whole seconds; got {}",
            f.wall_offset_ms
        );
        if let Some((lo, hi)) = expected_bands.get(i) {
            assert!(
                f.wall_offset_ms >= *lo && f.wall_offset_ms <= *hi,
                "heartbeat #{} wall_offset_ms = {} outside expected band [{lo}, {hi}]",
                i + 1,
                f.wall_offset_ms
            );
        }
    }
}

#[test]
fn global_event_bootstrap_idempotent_round_trip() {
    let base = fixtures_root().join("sse/global-event-bootstrap");
    let fixture = Fixture::at(&base);
    let frames = fixture.read_frames().expect("read");
    let meta = fixture.read_meta().expect("read meta");

    // Write into a tempdir under a fresh normalizer; result must match the on-disk
    // fixture byte-for-byte (after the same normalizer pass).
    let tmp = tempfile::tempdir().expect("tempdir");
    let mirror_base = tmp.path().join("global-event-bootstrap");
    let mirror = Fixture::at(&mirror_base);
    let mut n = Normalizer::new(Redactions::default());
    mirror.write(&meta, &frames, &mut n).expect("mirror write");

    let original_frames = fixture.read_frames().expect("re-read original frames");
    let mirror_frames = mirror.read_frames().expect("read mirror");
    pretty_assertions::assert_eq!(
        original_frames,
        mirror_frames,
        "fixture round-trip must be lossless"
    );
}

#[test]
fn m7_single_fake_tool_call_fixture_covers_tool_terminal_shape() {
    let (meta, frames) = load_fixture("m7-single-fake-tool-call");
    assert_eq!(meta.contract_version, CONTRACT_VERSION);
    assert_eq!(meta.scenario, "m7-single-fake-tool-call");
    assert_eq!(meta.captured, Some(false));
    assert!(frames.iter().any(|frame| part_type(frame) == Some("tool")));
    assert!(frames
        .iter()
        .any(|frame| part_type(frame) == Some("step-finish")));
    assert!(frames
        .iter()
        .any(|frame| frame_type(frame) == Some("session.idle")));
}

#[test]
fn m7_parallel_fake_tool_call_fixture_covers_two_tool_parts_in_order() {
    let (meta, frames) = load_fixture("m7-parallel-fake-tool-calls");
    assert_eq!(meta.scenario, "m7-parallel-fake-tool-calls");
    let tools = frames
        .iter()
        .filter(|frame| sync_type(frame) == Some("message.part.updated.v1"))
        .filter(|frame| part_type(frame) == Some("tool"))
        .filter_map(|frame| sync_data(frame)?.get("part")?.get("tool")?.as_str())
        .collect::<Vec<_>>();
    assert_eq!(tools, vec!["read", "grep"]);
    assert!(frames
        .iter()
        .any(|frame| part_type(frame) == Some("step-finish")));
}

#[test]
fn m7_abort_mid_stream_fixture_covers_terminal_abort_without_orphan_parts() {
    let (meta, frames) = load_fixture("m7-abort-mid-stream");
    assert_eq!(meta.scenario, "m7-abort-mid-stream");
    assert!(frames
        .iter()
        .any(|frame| frame_type(frame) == Some("session.error")));
    assert!(frames
        .iter()
        .any(|frame| frame_type(frame) == Some("session.idle")));
    assert!(frames.iter().any(|frame| {
        frame_type(frame) == Some("session.turn.close")
            && frame
                .payload
                .as_ref()
                .and_then(|payload| payload.get("properties"))
                .and_then(|props| props.get("reason"))
                .and_then(Value::as_str)
                == Some("interrupted")
    }));
    assert!(!frames.iter().any(|frame| part_type(frame) == Some("tool")));
}

#[test]
fn m7_concurrent_sessions_fixture_covers_interleaving_and_no_leakage() {
    let (meta, frames) = load_fixture("m7-concurrent-sessions");
    assert_eq!(meta.scenario, "m7-concurrent-sessions");
    let allowed = ["<SESSION_ID:1>", "<SESSION_ID:2>"];
    let mut seen = std::collections::BTreeSet::new();
    let seq = frames
        .iter()
        .filter_map(frame_session)
        .inspect(|id| {
            assert!(allowed.contains(id), "leaked session id {id}");
        })
        .inspect(|id| {
            seen.insert((*id).to_string());
        })
        .collect::<Vec<_>>();
    assert_eq!(seen.len(), 2);
    assert!(seq.windows(2).any(|pair| pair[0] != pair[1]));
}

#[test]
fn fixture_frame_round_trip() {
    // Independent shape test: build a frame, serialize, re-parse, compare.
    let frame = FixtureFrame {
        frame_index: 0,
        wall_offset_ms: 10_000,
        raw: r#"{"directory":null,"payload":{"type":"server.heartbeat","properties":{}}}"#
            .to_string(),
        payload: Some(serde_json::json!({"type":"server.heartbeat","properties":{}})),
        directory: None,
        parse_error: None,
    };
    let s = serde_json::to_string(&frame).expect("serialize");
    let back: FixtureFrame = serde_json::from_str(&s).expect("deserialize");
    assert_eq!(frame, back);
}

#[test]
fn fixture_meta_construction_uses_contract_version() {
    let meta = FixtureMeta::new("test", IdentityMap::default());
    assert_eq!(meta.contract_version, CONTRACT_VERSION);
    assert_eq!(meta.scenario, "test");
}

#[test]
fn store_fixture_declares_contract_and_capture_state() {
    let path = fixtures_root().join("store/empty.json");
    let v = load_json(&path);
    assert_eq!(
        v.get("contract_version").and_then(|c| c.as_str()),
        Some(CONTRACT_VERSION),
        "store fixture must declare contract version"
    );

    // The fixture is captured (`true`) once `KILO_ORACLE_RECORD=1` has been
    // run; before that it is a documented placeholder (`false`). Both states
    // are valid on disk — what matters is that *if* it is captured, the
    // `expected_shape.files` array is populated and provenance fields exist.
    let captured = v
        .get("captured")
        .and_then(|c| c.as_bool())
        .expect("store fixture must declare a `captured` boolean");

    if captured {
        let files = v
            .get("expected_shape")
            .and_then(|s| s.get("files"))
            .and_then(|f| f.as_array())
            .expect("captured store fixture must carry `expected_shape.files` array");
        assert!(
            !files.is_empty(),
            "captured store fixture must have at least one inventory entry"
        );
        for f in files {
            assert!(
                f.get("path").and_then(|p| p.as_str()).is_some(),
                "every store inventory entry must carry a `path` string: {f}"
            );
            assert!(
                f.get("size_bytes").and_then(|s| s.as_u64()).is_some(),
                "every store inventory entry must carry a `size_bytes` u64: {f}"
            );
        }
        // Provenance is required for captured fixtures.
        assert!(
            v.get("captured_at_iso").and_then(|s| s.as_str()).is_some(),
            "captured store fixture must carry `captured_at_iso`"
        );
        assert!(
            v.get("bun_binary_sha256")
                .and_then(|s| s.as_str())
                .is_some(),
            "captured store fixture must carry `bun_binary_sha256`"
        );
    } else {
        // Stub form: must call out how to capture it.
        let reason = v.get("reason").and_then(|s| s.as_str()).unwrap_or("");
        assert!(
            reason.contains("KILO_ORACLE_RECORD"),
            "store fixture stub should document the recording env var; reason = {reason:?}"
        );
    }
}

/// Discovery test: every `.json` / `.jsonl` file under `fixtures/` parses.
/// Catches accidental corruption / hand-edits that desync the format.
#[test]
fn every_shipped_fixture_parses() {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.extension().and_then(|s| s.to_str()) == Some("json")
                || p.extension().and_then(|s| s.to_str()) == Some("jsonl")
            {
                out.push(p);
            }
        }
    }
    let mut paths = Vec::new();
    walk(&fixtures_root(), &mut paths);
    assert!(
        !paths.is_empty(),
        "expected at least one shipped fixture under {}",
        fixtures_root().display()
    );

    for path in paths {
        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        if ext == "jsonl" {
            for (i, line) in raw.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                let _: Value = serde_json::from_str(line)
                    .unwrap_or_else(|e| panic!("{}: line {}: {e}", path.display(), i + 1));
            }
        } else {
            let _: Value =
                serde_json::from_str(&raw).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        }
    }
}
