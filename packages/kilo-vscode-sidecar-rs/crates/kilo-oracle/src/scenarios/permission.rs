//! Permission allow/deny scenario.
//!
//! Walks an entire permission round trip:
//!
//! 1. `POST /session` (no provider needed for the allocation itself)
//! 2. `POST /session/{id}/prompt_async` with a prompt that triggers a
//!    permission-gated tool (e.g. write/edit). This step needs a provider.
//! 3. `GET /permission` — capture the request envelope.
//! 4. `POST /permission/{requestID}/reply` with `{ "reply": "always" }` (or
//!    `"deny"`).
//! 5. Continue capturing SSE through `permission.replied` and the eventual
//!    `session.idle`.
//!
//! Like [`super::prompt`], the recording portion is gated behind
//! `KILO_ORACLE_RECORD=1` because step 2 requires a real provider key. By
//! default we exercise the request-shape path only and write a stub trace
//! alongside.

use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use serde_json::{json, Value};

use crate::error::{OracleError, OracleResult};
use crate::fixture::{Fixture, FixtureFile, FixtureMeta};
use crate::normalize::{Normalizer, Redactions};
use crate::sse::{SseRecorder, StopCondition};

use super::{ScenarioContext, ScenarioRunner};

/// Whether the permission scenario approves or denies the request.
#[derive(Debug, Clone, Copy)]
pub enum PermissionDecision {
    Allow,
    Deny,
}

impl PermissionDecision {
    fn reply_value(self) -> &'static str {
        match self {
            // Bun's permission reply enum: "once" | "always" | "reject".
            // The extension's tool-permission handler uses "always" for allow
            // and "reject" for deny, so we mirror that.
            PermissionDecision::Allow => "always",
            PermissionDecision::Deny => "reject",
        }
    }
}

pub struct PermissionScenario {
    pub decision: PermissionDecision,
    pub prompt_text: String,
    pub max_duration: Duration,
}

impl Default for PermissionScenario {
    fn default() -> Self {
        Self {
            decision: PermissionDecision::Allow,
            prompt_text: "Create a file called hello.txt with the content 'hi'.".to_string(),
            max_duration: Duration::from_secs(60),
        }
    }
}

impl ScenarioRunner for PermissionScenario {
    fn name(&self) -> &'static str {
        match self.decision {
            PermissionDecision::Allow => "permission-allow",
            PermissionDecision::Deny => "permission-deny",
        }
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

            // Always record the request envelope.
            let envelope_path = cx.fixture_path(format!("scenarios/{}-request.json", self.name()));
            FixtureFile::new(&envelope_path).write_value(
                &json!({
                    "session_id": session_id,
                    "request": prompt_body,
                    "decision": self.decision.reply_value(),
                }),
                &mut normalizer,
            )?;
            written.push(envelope_path);

            if std::env::var("KILO_ORACLE_RECORD").ok().as_deref() != Some("1") {
                let stub_path =
                    cx.fixture_path(format!("scenarios/{}-stream.stub.json", self.name()));
                FixtureFile::new(&stub_path).write_value(
                    &json!({
                        "skipped": true,
                        "reason": "set KILO_ORACLE_RECORD=1 with provider credentials and a permission-gated tool",
                    }),
                    &mut normalizer,
                )?;
                written.push(stub_path);
                return Ok(written);
            }

            // Live recording path:
            let response = cx.client.open_global_event_stream().await?;
            let _: Value = cx
                .client
                .post_json(
                    &format!("/session/{session_id}/prompt_async"),
                    Some(&prompt_body),
                )
                .await?;

            // Capture until we see a permission request show up; then issue
            // the decision; then keep capturing until idle.
            let frames = SseRecorder::new()
                .record(
                    response,
                    StopCondition::EventType("permission.updated".to_string()),
                )
                .await?;

            // Pull the latest pending permission and reply.
            let permissions: Value = cx.client.get_json("/permission").await?;
            if let Some(arr) = permissions.as_array() {
                if let Some(pending) = arr.iter().find(|p| {
                    p.get("status")
                        .and_then(|s| s.as_str())
                        .map(|s| s == "pending")
                        .unwrap_or(false)
                }) {
                    if let Some(req_id) = pending.get("id").and_then(|v| v.as_str()) {
                        let _ = cx
                            .client
                            .post_json(
                                &format!("/permission/{req_id}/reply"),
                                Some(&json!({ "reply": self.decision.reply_value() })),
                            )
                            .await;
                    }
                }
            }

            let fixture_frames = SseRecorder::to_fixture_frames(frames);
            let meta = FixtureMeta::new(self.name(), normalizer.map().clone());
            let base = cx.fixture_path(format!("sse/{}", self.name()));
            let fixture = Fixture::at(&base);
            fixture.write(&meta, &fixture_frames, &mut normalizer)?;
            written.push(fixture.meta_path);
            written.push(fixture.frames_path);
            Ok(written)
        })
    }
}
