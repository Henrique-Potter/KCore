//! Startup scenario: readiness line + `/global/health`.
//!
//! This is the smallest fixture in the harness and the only one that runs
//! without any tool / provider key. It is the M0 "exit gate" baseline:
//! if this scenario can't be replayed deterministically, nothing else can.

use std::path::PathBuf;
use std::pin::Pin;

use serde_json::{json, Value};

use crate::error::OracleResult;
use crate::fixture::FixtureFile;
use crate::normalize::{Normalizer, Redactions};
use crate::readiness::ReadyLine;
use crate::CONTRACT_VERSION;

use super::{ScenarioContext, ScenarioRunner};

pub struct StartupScenario;

/// Notes embedded in the startup fixture for reviewer context. These are
/// constant (not recorder-derived) but live in the recorder output so the
/// shipped golden equals the recorder's own output byte-for-byte. See
/// `tests/replay.rs::startup_fixture_matches_recorder_output`.
const STARTUP_NOTES: &[&str] = &[
    "stdout_line is the exact line emitted by `kilo serve --port 0` once the server has bound a loopback port. The VS Code extension parses this with the regex in packages/kilo-vscode/src/services/cli-backend/server-utils.ts (`listening on http:\\/\\/[\\w.]+:(\\d+)`).",
    "The Rust sidecar must reproduce this line verbatim except for the port number; see CONTRACT.md.",
    "health_response_body version is normalized to `<VERSION>` because Rust and Bun report different version strings; the only invariant Rust must preserve is `healthy: true` plus a non-empty `version` string.",
    "This fixture is the canonical post-normalization form. Tests that capture against a live Bun process will produce the same shape after running through `Normalizer`.",
];

impl StartupScenario {
    /// Build the canonical startup fixture body from raw recorder inputs.
    /// Extracted so the same shape is produced by `record_to` and by the
    /// regression test in `tests/replay.rs` without duplicating the schema.
    pub fn build_value(ready: &ReadyLine, health: &Value) -> Value {
        json!({
            "contract_version": CONTRACT_VERSION,
            "scenario": "startup",
            "stdout_line": ready.raw,
            "parsed": {
                "host": ready.host,
                "port": ready.port,
            },
            "health_response_status": 200,
            "health_response_body": health,
            "notes": STARTUP_NOTES,
        })
    }
}

impl ScenarioRunner for StartupScenario {
    fn name(&self) -> &'static str {
        "startup"
    }

    fn record_to<'a>(
        &'a self,
        cx: &'a ScenarioContext<'a>,
    ) -> Pin<Box<dyn std::future::Future<Output = OracleResult<Vec<PathBuf>>> + 'a + Send>> {
        Box::pin(async move {
            let health = cx.client.global_health().await?;

            let mut normalizer = Normalizer::new(
                Redactions::default()
                    .with_password(cx.sidecar.password.clone())
                    .with_port(cx.sidecar.ready.port)
                    .with_workspace(cx.sidecar.cwd.clone()),
            );

            let value = Self::build_value(&cx.sidecar.ready, &health);

            let path = cx.fixture_path("startup/ready-line.json");
            let file = FixtureFile::new(&path);
            file.write_value(&value, &mut normalizer)?;
            Ok(vec![path])
        })
    }
}
