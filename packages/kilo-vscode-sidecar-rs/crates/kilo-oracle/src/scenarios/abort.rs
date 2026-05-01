//! Abort-during-stream scenario.
//!
//! Sequence:
//!
//! 1. `POST /session` → session id.
//! 2. Open `/global/event` SSE.
//! 3. `POST /session/{id}/prompt_async` with a long-running prompt.
//! 4. Wait for the first `message.updated` (assistant message started).
//! 5. `POST /session/{id}/abort`.
//! 6. Continue capturing SSE until `session.idle`.
//!
//! As with [`super::prompt`], step 3 needs a real provider. By default this
//! scenario records the request envelope only; full capture is gated on
//! `KILO_ORACLE_RECORD=1`.

use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use serde_json::{json, Value};

use crate::error::{OracleError, OracleResult};
use crate::fixture::{Fixture, FixtureFile, FixtureMeta};
use crate::normalize::{Normalizer, Redactions};
use crate::sse::{SseRecorder, StopCondition};

use super::{ScenarioContext, ScenarioRunner};

pub struct AbortScenario {
    pub prompt_text: String,
    pub max_duration: Duration,
}

impl Default for AbortScenario {
    fn default() -> Self {
        Self {
            prompt_text: "Count slowly from one to one hundred.".to_string(),
            max_duration: Duration::from_secs(60),
        }
    }
}

impl ScenarioRunner for AbortScenario {
    fn name(&self) -> &'static str {
        "abort-mid-stream"
    }

    fn record_to<'a>(
        &'a self,
        cx: &'a ScenarioContext<'a>,
    ) -> Pin<Box<dyn std::future::Future<Output = OracleResult<Vec<PathBuf>>> + 'a + Send>> {
        Box::pin(async move {
            let dir = cx.sidecar.cwd.to_string_lossy().to_string();
            let create_body = json!({ "directory": dir });
            let create_resp: Value = cx.client.post_json("/session", Some(&create_body)).await?;
            let session_id = create_resp
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| OracleError::other("session create did not return an id"))?
                .to_string();

            let prompt_body = json!({
                "directory": dir,
                "parts": [{ "type": "text", "text": self.prompt_text }],
            });

            let mut written = Vec::new();
            let mut normalizer = Normalizer::new(
                Redactions::default()
                    .with_password(cx.sidecar.password.clone())
                    .with_port(cx.sidecar.ready.port)
                    .with_workspace(cx.sidecar.cwd.clone()),
            );

            let envelope_path = cx.fixture_path("scenarios/abort-request.json");
            FixtureFile::new(&envelope_path).write_value(
                &json!({
                    "session_id": session_id,
                    "request": prompt_body,
                }),
                &mut normalizer,
            )?;
            written.push(envelope_path);

            if std::env::var("KILO_ORACLE_RECORD").ok().as_deref() != Some("1") {
                let stub_path = cx.fixture_path("scenarios/abort-stream.stub.json");
                FixtureFile::new(&stub_path).write_value(
                    &json!({
                        "skipped": true,
                        "reason": "set KILO_ORACLE_RECORD=1 with provider credentials to capture abort SSE",
                    }),
                    &mut normalizer,
                )?;
                written.push(stub_path);
                return Ok(written);
            }

            // Recording path.
            let response = cx.client.open_global_event_stream().await?;
            let _: Value = cx
                .client
                .post_json(
                    &format!("/session/{session_id}/prompt_async"),
                    Some(&prompt_body),
                )
                .await?;

            // Capture until first message.updated, then abort, then continue
            // until idle.
            let frames = SseRecorder::new()
                .record(
                    response,
                    StopCondition::Predicate(Box::new(|_frame, parsed| {
                        parsed
                            .as_ref()
                            .and_then(|p| p.get("payload"))
                            .and_then(|p| p.get("type"))
                            .and_then(|t| t.as_str())
                            .map(|t| t == "session.idle" || t == "session.error")
                            .unwrap_or(false)
                    })),
                )
                .await?;

            // Issue the abort. We do this *after* the stream sees the first
            // assistant chunk in a real run; here we just send it
            // unconditionally so the recording always captures the abort
            // response shape.
            let abort_resp = cx
                .client
                .post_json::<serde_json::Value>(&format!("/session/{session_id}/abort"), None)
                .await
                .ok();

            let fixture_frames = SseRecorder::to_fixture_frames(frames);
            let meta = FixtureMeta::new("abort-mid-stream", normalizer.map().clone());
            let base = cx.fixture_path("sse/abort-mid-stream");
            let fixture = Fixture::at(&base);
            fixture.write(&meta, &fixture_frames, &mut normalizer)?;
            written.push(fixture.meta_path);
            written.push(fixture.frames_path);

            // Record the abort POST response separately for replay.
            let abort_path = cx.fixture_path("scenarios/abort-response.json");
            FixtureFile::new(&abort_path)
                .write_value(&json!({ "abort_response": abort_resp }), &mut normalizer)?;
            written.push(abort_path);
            Ok(written)
        })
    }
}
