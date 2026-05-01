//! End-to-end fixture pipeline test.
//!
//! Builds synthetic fixture frames that mimic what the SSE recorder produces
//! for the global-event bootstrap, runs them through `Fixture::write`, reads
//! back, and asserts byte-stable round-tripping plus normalizer idempotency.
//!
//! This is the M0 test that simulates the recording flow without spawning a
//! Bun process or hitting the network. It is the practical proof that "Bun
//! oracle tests pass consistently": the same input always produces the same
//! fixture file.

use kilo_oracle::{
    Fixture, FixtureFrame, FixtureMeta, IdentityMap, Normalizer, Redactions, CONTRACT_VERSION,
};
use serde_json::json;

fn synthetic_frames() -> Vec<FixtureFrame> {
    vec![
        FixtureFrame {
            frame_index: 0,
            wall_offset_ms: 0,
            raw: r#"{"directory":null,"payload":{"type":"server.connected","properties":{}}}"#
                .to_string(),
            payload: Some(json!({"type":"server.connected","properties":{}})),
            directory: None,
            parse_error: None,
        },
        FixtureFrame {
            frame_index: 1,
            wall_offset_ms: 10_000,
            raw: r#"{"directory":null,"payload":{"type":"server.heartbeat","properties":{}}}"#
                .to_string(),
            payload: Some(json!({"type":"server.heartbeat","properties":{}})),
            directory: None,
            parse_error: None,
        },
    ]
}

#[test]
fn fixture_pipeline_is_deterministic() {
    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let frames = synthetic_frames();
    let meta = FixtureMeta::new("synthetic-global-event", IdentityMap::default());

    let f1 = Fixture::at(dir1.path().join("trace"));
    let f2 = Fixture::at(dir2.path().join("trace"));

    let mut n1 = Normalizer::new(Redactions::default());
    let mut n2 = Normalizer::new(Redactions::default());

    f1.write(&meta, &frames, &mut n1).expect("write 1");
    f2.write(&meta, &frames, &mut n2).expect("write 2");

    // Two independent writes with identical inputs must produce byte-identical
    // files.
    let frames1 = std::fs::read(&f1.frames_path).unwrap();
    let frames2 = std::fs::read(&f2.frames_path).unwrap();
    assert_eq!(
        frames1, frames2,
        "fixture writer must be deterministic across two independent runs"
    );

    let meta1 = std::fs::read(&f1.meta_path).unwrap();
    let meta2 = std::fs::read(&f2.meta_path).unwrap();
    assert_eq!(meta1, meta2, "meta writer must be deterministic");

    // Round-trip: read back, write again, compare bytes.
    let read_frames = f1.read_frames().expect("read");
    let dir3 = tempfile::tempdir().unwrap();
    let f3 = Fixture::at(dir3.path().join("trace"));
    let mut n3 = Normalizer::new(Redactions::default());
    f3.write(&meta, &read_frames, &mut n3).expect("write 3");

    let frames3 = std::fs::read(&f3.frames_path).unwrap();
    assert_eq!(
        frames1, frames3,
        "round-tripped fixture must be byte-identical to the original"
    );
}

#[test]
fn fixture_meta_carries_contract_version() {
    let dir = tempfile::tempdir().unwrap();
    let f = Fixture::at(dir.path().join("trace"));
    let meta = FixtureMeta::new("test", IdentityMap::default());
    let mut n = Normalizer::new(Redactions::default());
    f.write(&meta, &[], &mut n).expect("write");

    let read = f.read_meta().expect("read meta");
    assert_eq!(read.contract_version, CONTRACT_VERSION);
    assert_eq!(read.scenario, "test");
}

#[test]
fn redactions_apply_to_payload_strings() {
    // Capture-style scenario: a frame whose `raw` contains a session id.
    // After normalization the same id must appear as a stable placeholder in
    // both `raw` and `payload`.
    let dir = tempfile::tempdir().unwrap();
    let f = Fixture::at(dir.path().join("trace"));
    let meta = FixtureMeta::new("test-redact", IdentityMap::default());

    let frame = FixtureFrame {
        frame_index: 0,
        wall_offset_ms: 0,
        raw: r#"{"payload":{"type":"session.created","properties":{"sessionID":"ses_abcdef0123456"}}}"#
            .to_string(),
        payload: Some(json!({
            "type": "session.created",
            "properties": { "sessionID": "ses_abcdef0123456" }
        })),
        directory: None,
        parse_error: None,
    };

    let mut n = Normalizer::new(Redactions::default());
    f.write(&meta, &[frame], &mut n).expect("write");

    let read_back = f.read_frames().expect("read");
    let frame_back = &read_back[0];

    // Both fields collapsed onto the same placeholder.
    assert!(
        frame_back.raw.contains("<SESSION_ID:1>"),
        "raw should be normalized: {}",
        frame_back.raw
    );
    let payload = frame_back.payload.as_ref().unwrap();
    let session_id = payload
        .get("properties")
        .and_then(|p| p.get("sessionID"))
        .and_then(|s| s.as_str())
        .expect("sessionID present");
    assert_eq!(session_id, "<SESSION_ID:1>");
}
