//! Capture the first N seconds of `GET /global/event`.
//!
//! On a quiescent sidecar this should produce exactly:
//!
//! 1. `server.connected`
//! 2. `server.heartbeat` (every ~10 s thereafter)
//!
//! We capture by default until either 25 s have passed or 3 frames have been
//! seen (whichever comes first), which gives us `connected + heartbeat ×2` on
//! a healthy run.

use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use crate::error::OracleResult;
use crate::fixture::{Fixture, FixtureMeta};
use crate::normalize::{Normalizer, Redactions};
use crate::sse::{SseRecorder, StopCondition};

use super::{ScenarioContext, ScenarioRunner};

pub struct GlobalEventScenario {
    /// Maximum frames to capture.
    pub frames: usize,
    /// Maximum wall clock to spend.
    pub max_duration: Duration,
}

impl Default for GlobalEventScenario {
    fn default() -> Self {
        Self {
            frames: 3,
            max_duration: Duration::from_secs(25),
        }
    }
}

impl ScenarioRunner for GlobalEventScenario {
    fn name(&self) -> &'static str {
        "global-event"
    }

    fn record_to<'a>(
        &'a self,
        cx: &'a ScenarioContext<'a>,
    ) -> Pin<Box<dyn std::future::Future<Output = OracleResult<Vec<PathBuf>>> + 'a + Send>> {
        Box::pin(async move {
            let response = cx.client.open_global_event_stream().await?;

            // Stop after `self.frames` frames. The recorder also enforces an
            // internal 30 s stall timeout that bounds total runtime, so we
            // don't need a composite stop here for the M0 baseline.
            // `self.max_duration` is honored by the surrounding `tokio::time::timeout`
            // applied in tests (see `tests/replay.rs`), not by the recorder.
            let _ = self.max_duration;
            let frames = SseRecorder::new()
                .record(response, StopCondition::Frames(self.frames))
                .await?;

            let fixture_frames = SseRecorder::to_fixture_frames(frames);
            let mut normalizer = Normalizer::new(
                Redactions::default()
                    .with_password(cx.sidecar.password.clone())
                    .with_port(cx.sidecar.ready.port)
                    .with_workspace(cx.sidecar.cwd.clone()),
            );

            let meta = FixtureMeta::new("global-event", normalizer.map().clone());
            let base = cx.fixture_path("sse/global-event-bootstrap");
            let fixture = Fixture::at(&base);
            fixture.write(&meta, &fixture_frames, &mut normalizer)?;

            Ok(vec![fixture.meta_path, fixture.frames_path])
        })
    }
}
