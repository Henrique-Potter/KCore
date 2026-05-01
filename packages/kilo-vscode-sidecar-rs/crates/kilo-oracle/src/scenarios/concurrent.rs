//! Agent Manager concurrent-session scenario.
//!
//! Models the Agent Manager UX: two sessions, one Bun sidecar, simultaneous
//! prompts. The strict invariant we want to record is that **events are never
//! routed to the wrong session** — every `session.*`, `message.*`, and
//! `part.*` event in the fixture must carry a `directory`/`sessionID` that
//! matches one of the two sessions we created.
//!
//! Sequence:
//!
//! 1. `POST /session` ×2 → `session_a`, `session_b`.
//! 2. Open `/global/event` SSE.
//! 3. Fire `prompt_async` for both sessions back-to-back (no await in between).
//! 4. Record until **both** sessions have emitted `session.idle`.
//!
//! By default — same as the other prompt-driven scenarios — recording is gated
//! on `KILO_ORACLE_RECORD=1`. The default run records the two-session create
//! response shapes only.

use std::collections::HashSet;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

use serde_json::{json, Value};

use crate::error::{OracleError, OracleResult};
use crate::fixture::{Fixture, FixtureFile, FixtureMeta};
use crate::normalize::{Normalizer, Redactions};
use crate::sse::{SseRecorder, StopCondition};

use super::{ScenarioContext, ScenarioRunner};

pub struct ConcurrentScenario {
    pub prompt_text_a: String,
    pub prompt_text_b: String,
    pub max_duration: Duration,
}

impl Default for ConcurrentScenario {
    fn default() -> Self {
        Self {
            prompt_text_a: "What is two plus two?".to_string(),
            prompt_text_b: "What is the capital of France?".to_string(),
            max_duration: Duration::from_secs(120),
        }
    }
}

impl ScenarioRunner for ConcurrentScenario {
    fn name(&self) -> &'static str {
        "agent-manager-concurrent"
    }

    fn record_to<'a>(
        &'a self,
        cx: &'a ScenarioContext<'a>,
    ) -> Pin<Box<dyn std::future::Future<Output = OracleResult<Vec<PathBuf>>> + 'a + Send>> {
        Box::pin(async move {
            let dir = cx.sidecar.cwd.to_string_lossy().to_string();
            let create_body = json!({ "directory": dir });

            // Two session creates.
            let session_a: Value = cx.client.post_json("/session", Some(&create_body)).await?;
            let session_b: Value = cx.client.post_json("/session", Some(&create_body)).await?;

            let id_a = session_a
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| OracleError::other("session A: missing id"))?
                .to_string();
            let id_b = session_b
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| OracleError::other("session B: missing id"))?
                .to_string();

            let prompt_a = json!({
                "directory": dir,
                "parts": [{ "type": "text", "text": self.prompt_text_a }],
            });
            let prompt_b = json!({
                "directory": dir,
                "parts": [{ "type": "text", "text": self.prompt_text_b }],
            });

            let mut written = Vec::new();
            let mut normalizer = Normalizer::new(
                Redactions::default()
                    .with_password(cx.sidecar.password.clone())
                    .with_port(cx.sidecar.ready.port)
                    .with_workspace(cx.sidecar.cwd.clone()),
            );

            let envelope_path = cx.fixture_path("scenarios/agent-manager-concurrent-request.json");
            FixtureFile::new(&envelope_path).write_value(
                &json!({
                    "session_a": session_a,
                    "session_b": session_b,
                    "prompt_a": prompt_a,
                    "prompt_b": prompt_b,
                }),
                &mut normalizer,
            )?;
            written.push(envelope_path);

            if std::env::var("KILO_ORACLE_RECORD").ok().as_deref() != Some("1") {
                let stub_path =
                    cx.fixture_path("scenarios/agent-manager-concurrent-stream.stub.json");
                FixtureFile::new(&stub_path).write_value(
                    &json!({
                        "skipped": true,
                        "reason": "set KILO_ORACLE_RECORD=1 with provider credentials to capture both streams",
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
                .post_json(&format!("/session/{id_a}/prompt_async"), Some(&prompt_a))
                .await?;
            let _: Value = cx
                .client
                .post_json(&format!("/session/{id_b}/prompt_async"), Some(&prompt_b))
                .await?;

            // Track both idles before stopping.
            let id_a_seen = std::sync::Arc::new(std::sync::Mutex::new(false));
            let id_b_seen = std::sync::Arc::new(std::sync::Mutex::new(false));
            let watch_a = id_a.clone();
            let watch_b = id_b.clone();
            let id_a_handle = std::sync::Arc::clone(&id_a_seen);
            let id_b_handle = std::sync::Arc::clone(&id_b_seen);

            let frames = SseRecorder::new()
                .record(
                    response,
                    StopCondition::Predicate(Box::new(move |_frame, parsed| {
                        let Some(parsed) = parsed.as_ref() else {
                            return false;
                        };
                        let payload = parsed.get("payload");
                        let kind = payload.and_then(|p| p.get("type")).and_then(|t| t.as_str());
                        if kind != Some("session.idle") && kind != Some("session.error") {
                            return false;
                        }
                        let session_id = payload
                            .and_then(|p| p.get("properties"))
                            .and_then(|p| p.get("sessionID"))
                            .and_then(|s| s.as_str());
                        match session_id {
                            Some(s) if s == watch_a => {
                                *id_a_handle.lock().unwrap() = true;
                            }
                            Some(s) if s == watch_b => {
                                *id_b_handle.lock().unwrap() = true;
                            }
                            _ => {}
                        }
                        *id_a_handle.lock().unwrap() && *id_b_handle.lock().unwrap()
                    })),
                )
                .await?;

            // Sanity: every session.* event in the trace belongs to A or B.
            let leaked: HashSet<String> = frames
                .iter()
                .filter_map(|f| serde_json::from_str::<Value>(&f.data).ok())
                .filter_map(|v| {
                    v.get("payload")
                        .and_then(|p| p.get("properties"))
                        .and_then(|p| p.get("sessionID"))
                        .and_then(|s| s.as_str())
                        .map(str::to_string)
                })
                .filter(|s| s != &id_a && s != &id_b)
                .collect();
            if !leaked.is_empty() {
                tracing::warn!(?leaked, "concurrent scenario captured cross-session events");
            }

            let fixture_frames = SseRecorder::to_fixture_frames(frames);
            let meta = FixtureMeta::new("agent-manager-concurrent", normalizer.map().clone());
            let base = cx.fixture_path("sse/agent-manager-concurrent");
            let fixture = Fixture::at(&base);
            fixture.write(&meta, &fixture_frames, &mut normalizer)?;
            written.push(fixture.meta_path);
            written.push(fixture.frames_path);
            Ok(written)
        })
    }
}
