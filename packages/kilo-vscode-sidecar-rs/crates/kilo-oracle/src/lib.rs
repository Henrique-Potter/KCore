//! `kilo-oracle` — Bun sidecar oracle harness for the VS Code Rust sidecar
//! migration.
//!
//! This crate is a **black-box recorder** for the Bun-built sidecar that the
//! VS Code extension currently launches. It speaks the same wire protocol the
//! extension does (HTTP + SSE on a localhost loopback) and writes its
//! observations into "golden" fixtures under
//! `packages/kilo-vscode-sidecar-rs/fixtures/`. Later milestones diff the Rust
//! sidecar's own behavior against those fixtures.
//!
//! Design and rationale: see [`docs/oracle-design.md`](../docs/oracle-design.md).
//! Route inventory: see [`docs/route-inventory.md`](../docs/route-inventory.md).
//! Frozen wire contract: see [`CONTRACT.md`](../CONTRACT.md).
//!
//! # Strict crate boundary
//!
//! The oracle deliberately does **not** depend on `kilo-server`,
//! `kilo-protocol`, `kilo-store`, or `kilo-session`. The whole point of the
//! crate is to be the source of truth for those crates' contracts, so they
//! cannot also be its dependencies. All wire-level work uses `serde_json::Value`
//! plus narrow typed wrappers in [`readiness`].

pub const CRATE: &str = "kilo-oracle";

/// Contract version recorded in every fixture's metadata. Mirrors the value in
/// `CONTRACT.md`. Bump in lockstep with the doc.
pub const CONTRACT_VERSION: &str = "kilo-vscode-sidecar.preview.0";

pub mod benchmark;
pub mod error;
pub mod fixture;
pub mod http;
pub mod normalize;
pub mod readiness;
pub mod scenarios;
pub mod spawn;
pub mod sse;

pub use benchmark::{BenchmarkGate, BenchmarkReport, BenchmarkThresholds, GateMetric, GateStatus};
pub use error::{OracleError, OracleResult};
pub use fixture::{Fixture, FixtureFile, FixtureFrame, FixtureMeta};
pub use http::OracleClient;
pub use normalize::{IdentityMap, Normalizer, Redactions};
pub use readiness::{
    parse_ready_line, parse_ready_line_strict, parse_ready_line_strict_loopback, ReadyLine,
};
pub use spawn::{default_binary_path, SidecarHandle, SpawnConfig, DEFAULT_READY_TIMEOUT_SECS};
pub use sse::{SseFrame, SseRecorder, StopCondition};

pub use scenarios::permission::PermissionDecision;
pub use scenarios::{
    AbortScenario, ConcurrentScenario, GlobalEventScenario, PermissionScenario, PromptScenario,
    ScenarioContext, ScenarioRunner, SessionScenario, StartupScenario,
};
