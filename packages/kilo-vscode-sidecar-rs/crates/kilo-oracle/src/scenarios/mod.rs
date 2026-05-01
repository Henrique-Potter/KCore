//! High-level recording flows.
//!
//! Each scenario knows how to drive Bun through one VS-Code-relevant
//! interaction and produce a fixture from it. Scenarios are intentionally
//! small and side-effect-free outside of `record_to`.
//!
//! Today's scenarios (M0):
//!
//! - [`startup`] — spawn Bun, capture readiness + `/global/health`.
//! - [`global_event`] — capture the first N seconds of `/global/event`
//!   (`server.connected` + heartbeats).
//! - [`session`] — `POST /session` + `GET /session/{id}`. Captures the create
//!   response shape.
//! - [`prompt`] — `POST /session/{id}/prompt_async` paired with the SSE trace
//!   that follows. Requires a real provider; M0 ships a stub that records the
//!   request shape only.
//! - [`permission`] — capture an approval/deny round trip. Stub for the same
//!   reason as `prompt`.
//! - [`abort`] — `POST /session/{id}/abort` mid-stream. Stub.
//! - [`concurrent`] — Agent-Manager-style two-session flow. Stub.
//!
//! "Stub" means the scenario implements the *recording driver* (HTTP calls
//! sequenced correctly) but skips capture by default because it would need a
//! live provider key in CI. The relevant code is gated on
//! `KILO_ORACLE_RECORD=1` and prints a documented blocker when run without
//! it.

pub mod abort;
pub mod concurrent;
pub mod global_event;
pub mod permission;
pub mod prompt;
pub mod session;
pub mod startup;

pub use abort::AbortScenario;
pub use concurrent::ConcurrentScenario;
pub use global_event::GlobalEventScenario;
pub use permission::PermissionScenario;
pub use prompt::PromptScenario;
pub use session::SessionScenario;
pub use startup::StartupScenario;

use std::path::{Path, PathBuf};

use crate::error::OracleResult;
use crate::http::OracleClient;
use crate::spawn::SidecarHandle;

/// Common context all scenarios share.
pub struct ScenarioContext<'a> {
    pub sidecar: &'a SidecarHandle,
    pub client: &'a OracleClient,
    pub fixtures_root: PathBuf,
}

impl<'a> ScenarioContext<'a> {
    pub fn new(
        sidecar: &'a SidecarHandle,
        client: &'a OracleClient,
        fixtures_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            sidecar,
            client,
            fixtures_root: fixtures_root.into(),
        }
    }

    pub fn fixture_path(&self, sub: impl AsRef<Path>) -> PathBuf {
        self.fixtures_root.join(sub)
    }
}

/// Trait every scenario implements. The `record_to` method is the only thing
/// the harness driver calls.
pub trait ScenarioRunner {
    fn name(&self) -> &'static str;

    /// Drive the scenario and return the absolute path(s) it wrote.
    /// `cx.fixtures_root` is treated as the destination directory.
    ///
    /// The returned future is `Send` so harness drivers can hand it to
    /// `tokio::spawn`. Scenario-internal state must therefore be `Send` —
    /// the existing scenarios use `Arc<Mutex<…>>` (concurrent.rs) and plain
    /// `String`s, both of which qualify.
    fn record_to<'a>(
        &'a self,
        cx: &'a ScenarioContext<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = OracleResult<Vec<PathBuf>>> + 'a + Send>>;
}
