//! Prompt streaming scenario.
//!
//! Drives `POST /session/{id}/prompt_async` and pairs the request with the
//! `/global/event` SSE trace that the prompt produces (assistant message
//! deltas, tool calls, message-completion event).
//!
//! ## Why this is a "stub" in M0
//!
//! Recording a prompt requires a working provider auth (Kilo Gateway,
//! Anthropic, OpenAI, etc.) on the host running the oracle. CI does not
//! have those credentials, so by default this scenario records the request
//! shape **only** and writes a documented blocker note next to the fixture.
//! When `KILO_ORACLE_RECORD=1` is set the scenario also captures the SSE
//! tail, terminating on the first `session.idle` event or after 60 s.
//!
//! The driver implementation lives in this file even when recording is gated
//! out, so the call sequence is reviewable as code.

use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use serde_json::{json, Value};

use crate::error::OracleResult;
use crate::fixture::{Fixture, FixtureFile, FixtureMeta};
use crate::normalize::{Normalizer, Redactions};
use crate::sse::{SseRecorder, StopCondition};

use super::{ScenarioContext, ScenarioRunner};

/// Prompt driver. The default `prompt_text` is fixed so two recordings produce
/// identical request bodies after normalization.
pub struct PromptScenario {
    pub prompt_text: String,
    pub model_id: Option<String>,
    pub provider_id: Option<String>,
    pub agent: Option<String>,
    /// Maximum wall clock to spend collecting the SSE tail.
    pub max_duration: Duration,
}

impl Default for PromptScenario {
    fn default() -> Self {
        Self {
            prompt_text: "Say hello in one word.".to_string(),
            model_id: None,
            provider_id: None,
            agent: None,
            max_duration: Duration::from_secs(60),
        }
    }
}

impl ScenarioRunner for PromptScenario {
    fn name(&self) -> &'static str {
        "prompt-stream"
    }

    fn record_to<'a>(
        &'a self,
        cx: &'a ScenarioContext<'a>,
    ) -> Pin<Box<dyn std::future::Future<Output = OracleResult<Vec<PathBuf>>> + 'a + Send>> {
        Box::pin(async move {
            let dir = cx.sidecar.cwd.to_string_lossy().to_string();

            // Always record the request shape, even without a provider.
            let create_body = json!({ "directory": dir });
            let create_resp: Value = cx.client.post_json("/session", Some(&create_body)).await?;
            let session_id = create_resp
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    crate::error::OracleError::other("session create did not return an id")
                })?
                .to_string();

            let prompt_body = json!({
                "directory": dir,
                "parts": [{ "type": "text", "text": self.prompt_text }],
                "model": self.model_id,
                "agent": self.agent,
                "providerID": self.provider_id,
            });

            let mut written = Vec::new();
            let mut normalizer = Normalizer::new(
                Redactions::default()
                    .with_password(cx.sidecar.password.clone())
                    .with_port(cx.sidecar.ready.port)
                    .with_workspace(cx.sidecar.cwd.clone()),
            );

            // Always: record the request envelope so fixtures show the body shape.
            let request_path = cx.fixture_path("scenarios/prompt-request.json");
            FixtureFile::new(&request_path).write_value(
                &json!({
                    "session_id": session_id,
                    "request": prompt_body,
                    "create_response": create_resp,
                }),
                &mut normalizer,
            )?;
            written.push(request_path);

            if std::env::var("KILO_ORACLE_RECORD").ok().as_deref() != Some("1") {
                // Drop a stub trace so reviewers can tell the recording was
                // skipped intentionally.
                let stub_path = cx.fixture_path("scenarios/prompt-stream.stub.json");
                FixtureFile::new(&stub_path).write_value(
                    &json!({
                        "skipped": true,
                        "reason": "set KILO_ORACLE_RECORD=1 with provider credentials to capture SSE",
                    }),
                    &mut normalizer,
                )?;
                written.push(stub_path);
                return Ok(written);
            }

            // Recording path: open SSE first, fire prompt, capture until idle.
            let response = cx.client.open_global_event_stream().await?;
            let _: Value = cx
                .client
                .post_json(
                    &format!("/session/{session_id}/prompt_async"),
                    Some(&prompt_body),
                )
                .await?;

            let frames = SseRecorder::new()
                .record(
                    response,
                    StopCondition::EventType("session.idle".to_string()),
                )
                .await?;
            let fixture_frames = SseRecorder::to_fixture_frames(frames);
            let meta = FixtureMeta::new("prompt-stream", normalizer.map().clone());
            let base = cx.fixture_path("sse/prompt-stream");
            let fixture = Fixture::at(&base);
            fixture.write(&meta, &fixture_frames, &mut normalizer)?;
            written.push(fixture.meta_path);
            written.push(fixture.frames_path);
            Ok(written)
        })
    }
}
