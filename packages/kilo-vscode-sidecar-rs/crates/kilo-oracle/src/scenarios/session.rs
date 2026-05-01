//! Capture session create/list/delete shapes.
//!
//! No provider is needed: `POST /session` only allocates a session record.
//! This scenario records the create response (which contains the session ID
//! shape, timestamps, defaults), the list response, and the delete response.

use std::path::PathBuf;
use std::pin::Pin;

use serde_json::{json, Value};

use crate::error::OracleResult;
use crate::fixture::FixtureFile;
use crate::normalize::{Normalizer, Redactions};

use super::{ScenarioContext, ScenarioRunner};

pub struct SessionScenario;

impl ScenarioRunner for SessionScenario {
    fn name(&self) -> &'static str {
        "session-basic"
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
                .map(str::to_string);

            let list_resp: Value = cx.client.get_json("/session").await?;

            let delete_resp = if let Some(id) = session_id.as_deref() {
                cx.client.delete_json(&format!("/session/{id}")).await.ok()
            } else {
                None
            };

            let mut normalizer = Normalizer::new(
                Redactions::default()
                    .with_password(cx.sidecar.password.clone())
                    .with_port(cx.sidecar.ready.port)
                    .with_workspace(cx.sidecar.cwd.clone()),
            );

            let bundle = json!({
                "create": create_resp,
                "list": list_resp,
                "delete": delete_resp,
            });

            let path = cx.fixture_path("scenarios/session-basic.json");
            FixtureFile::new(&path).write_value(&bundle, &mut normalizer)?;
            Ok(vec![path])
        })
    }
}
