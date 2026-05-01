//! Regression test for the startup fixture pipeline.
//!
//! The shipped golden `fixtures/startup/ready-line.json` was previously
//! hand-edited to use `"<PORT>"` (string) and `"<VERSION>"` (string), shapes
//! the recorder did not produce: the recorder emitted a numeric `port` and a
//! version string sourced from the live `/global/health` response. This test
//! locks the recorder output and the on-disk golden to the same bytes.
//!
//! It exercises [`StartupScenario::build_value`] directly with realistic
//! recorder inputs and runs them through `FixtureFile::write_value` and the
//! full [`Normalizer`] pipeline. The result must equal the shipped fixture
//! byte-for-byte. If the normalization rules drift, this test fails — the
//! intent is to make the on-disk golden the source of truth and force the
//! recorder to keep matching it.
//!
//! Note: we do **not** spawn a real Bun process here. The scenario's only
//! external dependency is `OracleClient::global_health()`, which we satisfy
//! by calling `build_value` directly with a synthetic health body. A
//! parallel test under `tests/startup_recorder.rs` could front a one-shot
//! HTTP listener instead, but that adds a Send-bound to the future and a
//! socket lifecycle for a single GET; the data-flow we want to lock is
//! identical either way.

use std::path::PathBuf;

use kilo_oracle::{FixtureFile, Normalizer, ReadyLine, Redactions, StartupScenario};
use serde_json::json;

fn fixtures_root() -> PathBuf {
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("fixtures")
}

#[test]
fn startup_fixture_matches_recorder_output() {
    // Realistic recorder inputs: a parsed readiness line with a real port
    // and a `/global/health` body matching the shape returned by Bun.
    let ready = ReadyLine {
        host: "127.0.0.1".to_string(),
        port: 54321,
        raw: "kilo server listening on http://127.0.0.1:54321".to_string(),
    };
    let health = json!({
        "healthy": true,
        // Any non-empty version string; the normalizer must replace it
        // with `<VERSION>` regardless of content.
        "version": "0.1.2-bun"
    });

    let value = StartupScenario::build_value(&ready, &health);

    let mut normalizer = Normalizer::new(
        Redactions::default()
            // Use a synthetic password the way the live recorder does. It
            // must not appear in the recorder output even when the value
            // happens to look like noise.
            .with_password("synthetic-password-supersecret")
            .with_port(ready.port)
            .with_workspace("/tmp/kilo-oracle-fake-cwd"),
    );

    let tmp = tempfile::tempdir().expect("tempdir");
    let recorded_path = tmp.path().join("ready-line.json");
    FixtureFile::new(&recorded_path)
        .write_value(&value, &mut normalizer)
        .expect("write recorder output");

    let recorded = std::fs::read(&recorded_path).expect("read recorded fixture");
    let golden_path = fixtures_root().join("startup/ready-line.json");
    let golden = std::fs::read(&golden_path)
        .unwrap_or_else(|e| panic!("read golden {}: {e}", golden_path.display()));

    assert_eq!(
        std::str::from_utf8(&recorded).unwrap(),
        std::str::from_utf8(&golden).unwrap(),
        "recorder output must match shipped golden byte-for-byte. \n\
         Recorded at {}\n\
         Golden at {}\n\
         If you intended to change the fixture, regenerate it via the \n\
         recorder and commit both the new fixture and any normalizer \n\
         updates that drove the change.",
        recorded_path.display(),
        golden_path.display(),
    );
}

#[test]
fn startup_fixture_is_idempotent_when_re_normalized() {
    // Reading the on-disk fixture and renormalizing produces the same
    // bytes. This is the crate-wide idempotency invariant; we duplicate it
    // here so any regression that only affects `build_value` ordering is
    // caught alongside the byte-for-byte test above.
    let golden_path = fixtures_root().join("startup/ready-line.json");
    let v = FixtureFile::new(golden_path.clone())
        .read_value()
        .expect("read golden");
    let mut normalizer = Normalizer::new(Redactions::default());
    let again = normalizer.normalize_value(&v);
    pretty_assertions::assert_eq!(v, again);
}
