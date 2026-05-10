//! External Bun-vs-Rust sidecar benchmark comparison harness.
//!
//! This module is intentionally a benchmark runner, not an oracle fixture
//! recorder. It launches each runtime as an external process, drives a small
//! deterministic fake-provider scenario matrix, samples the sidecar process by
//! pid, writes raw JSONL trial rows, and computes a compact Markdown summary.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{task::JoinHandle, time::timeout};

use crate::{
    default_binary_path, OracleClient, OracleError, OracleResult, SidecarHandle, SpawnConfig,
    SseFrame, SseRecorder, StopCondition,
};

const FAKE_PREFIX: &str = "__KILO_BENCH_FAKE__";
const DEFAULT_TRIALS: usize = 30;
const DEFAULT_WARMUPS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BenchRuntime {
    Bun,
    Rust,
}

impl BenchRuntime {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bun => "bun",
            Self::Rust => "rust",
        }
    }

    pub fn parse(value: &str) -> OracleResult<Vec<Self>> {
        match value {
            "bun" => Ok(vec![Self::Bun]),
            "rust" => Ok(vec![Self::Rust]),
            "both" => Ok(vec![Self::Bun, Self::Rust]),
            other => Err(OracleError::other(format!(
                "unknown runtime {other:?}; expected bun, rust, or both"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchScenario {
    ColdStartReady,
    TurnTextSmall,
    Manager2Sessions,
    TaskSingleChild,
    TaskFanout4,
    History100Messages,
    AbortInflight,
}

impl BenchScenario {
    pub fn name(self) -> &'static str {
        match self {
            Self::ColdStartReady => "cold_start_ready",
            Self::TurnTextSmall => "turn_text_small",
            Self::Manager2Sessions => "manager_2_sessions",
            Self::TaskSingleChild => "task_single_child",
            Self::TaskFanout4 => "task_fanout_4",
            Self::History100Messages => "history_100_messages",
            Self::AbortInflight => "abort_inflight",
        }
    }

    pub fn parse(value: &str) -> OracleResult<Vec<Self>> {
        match value {
            "all" | "smoke" | "pr_smoke" => Ok(Self::smoke()),
            "cold_start_ready" => Ok(vec![Self::ColdStartReady]),
            "turn_text_small" => Ok(vec![Self::TurnTextSmall]),
            "manager_2_sessions" => Ok(vec![Self::Manager2Sessions]),
            "task_single_child" => Ok(vec![Self::TaskSingleChild]),
            "task_fanout_4" => Ok(vec![Self::TaskFanout4]),
            "history_100_messages" => Ok(vec![Self::History100Messages]),
            "abort_inflight" | "tool_abort_inflight" => Ok(vec![Self::AbortInflight]),
            other => Err(OracleError::other(format!(
                "unknown scenario {other:?}; use all, smoke, or a known scenario name"
            ))),
        }
    }

    pub fn smoke() -> Vec<Self> {
        vec![
            Self::ColdStartReady,
            Self::TurnTextSmall,
            Self::Manager2Sessions,
            Self::TaskSingleChild,
            Self::TaskFanout4,
            Self::History100Messages,
            Self::AbortInflight,
        ]
    }
}

#[derive(Clone, Debug)]
pub struct BenchCompareConfig {
    pub runtimes: Vec<BenchRuntime>,
    pub scenarios: Vec<BenchScenario>,
    pub trials: usize,
    pub warmups: usize,
    pub output: PathBuf,
    pub bun_binary: PathBuf,
    pub rust_binary: PathBuf,
    pub workspace_seed: String,
}

impl Default for BenchCompareConfig {
    fn default() -> Self {
        Self {
            runtimes: vec![BenchRuntime::Bun, BenchRuntime::Rust],
            scenarios: BenchScenario::smoke(),
            trials: DEFAULT_TRIALS,
            warmups: DEFAULT_WARMUPS,
            output: PathBuf::from("target/bench/bun-vs-rust.jsonl"),
            bun_binary: default_binary_path(),
            rust_binary: default_rust_binary_path(),
            workspace_seed: "small".to_string(),
        }
    }
}

pub struct BenchSuite {
    cfg: BenchCompareConfig,
}

impl BenchSuite {
    pub fn new(cfg: BenchCompareConfig) -> Self {
        Self { cfg }
    }

    pub async fn run(&self) -> OracleResult<BenchCompareReport> {
        if let Some(parent) = self.cfg.output.parent() {
            fs::create_dir_all(parent).map_err(|source| OracleError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.cfg.output)
            .map_err(|source| OracleError::Io {
                path: self.cfg.output.clone(),
                source,
            })?;

        let mut rows = Vec::new();
        for scenario in &self.cfg.scenarios {
            let mut order = self.cfg.runtimes.clone();
            for trial in 0..(self.cfg.warmups + self.cfg.trials) {
                if trial % 2 == 1 {
                    order.reverse();
                }
                for runtime in &order {
                    let warmup = trial < self.cfg.warmups;
                    let index = if warmup {
                        trial
                    } else {
                        trial - self.cfg.warmups
                    };
                    let row = self.run_trial(*runtime, *scenario, index, warmup).await;
                    let row = match row {
                        Ok(row) => row,
                        Err(err) => BenchTrial {
                            runtime: *runtime,
                            scenario: scenario.name().to_string(),
                            trial: index,
                            warmup,
                            ok: false,
                            error: Some(err.to_string()),
                            ..BenchTrial::empty(*runtime, *scenario, index, warmup)
                        },
                    };
                    writeln!(file, "{}", serde_json::to_string(&row)?).map_err(|source| {
                        OracleError::Io {
                            path: self.cfg.output.clone(),
                            source,
                        }
                    })?;
                    rows.push(row);
                }
            }
        }
        Ok(BenchCompareReport::from_trials(self.cfg.clone(), rows))
    }

    async fn run_trial(
        &self,
        runtime: BenchRuntime,
        scenario: BenchScenario,
        trial: usize,
        warmup: bool,
    ) -> OracleResult<BenchTrial> {
        let root = tempfile::tempdir()
            .map_err(|err| OracleError::Other(format!("create benchmark tempdir: {err}")))?;
        let repo = root.path().join("repo");
        seed_workspace(&repo, &self.cfg.workspace_seed)?;
        let bin = match runtime {
            BenchRuntime::Bun => self.cfg.bun_binary.clone(),
            BenchRuntime::Rust => self.cfg.rust_binary.clone(),
        };
        if !bin.exists() {
            return Err(OracleError::other(format!(
                "{} binary is missing: {}",
                runtime.as_str(),
                bin.display()
            )));
        }

        let spawn_start = Instant::now();
        let mut sidecar =
            SidecarHandle::spawn(spawn_cfg(runtime, &bin, root.path(), &repo)).await?;
        let spawn_to_ready = spawn_start.elapsed().as_millis();
        let client =
            OracleClient::unscoped("127.0.0.1", sidecar.ready.port, Some(&sidecar.password))?;
        let before = ResourceSample::capture(sidecar.pid);
        let scenario_start = Instant::now();
        let run = ScenarioCx {
            runtime,
            client: client.clone(),
        };
        let outcome = scenario.run(&run).await?;
        let after = ResourceSample::capture(sidecar.pid);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let settled = ResourceSample::capture(sidecar.pid);
        let stop = Instant::now();
        sidecar.shutdown().await?;
        let shutdown = stop.elapsed().as_millis();

        let mut timings = outcome.timings;
        timings.spawn_to_ready_ms = Some(spawn_to_ready);
        timings.shutdown_ms = Some(shutdown);
        timings.request_to_idle_ms = timings
            .request_to_idle_ms
            .or(Some(scenario_start.elapsed().as_millis()));

        Ok(BenchTrial {
            runtime,
            scenario: scenario.name().to_string(),
            trial,
            warmup,
            ok: true,
            timings_ms: timings,
            resources: ResourceDelta::from_samples(before, after, settled),
            counts: outcome.counts,
            error: None,
        })
    }
}

struct ScenarioCx {
    runtime: BenchRuntime,
    client: OracleClient,
}

struct ScenarioOutcome {
    timings: TimingMetrics,
    counts: CountMetrics,
}

impl BenchScenario {
    async fn run(self, cx: &ScenarioCx) -> OracleResult<ScenarioOutcome> {
        match self {
            Self::ColdStartReady => {
                cx.client.global_health().await?;
                Ok(ScenarioOutcome {
                    timings: TimingMetrics::default(),
                    counts: CountMetrics {
                        http_requests: 1,
                        ..Default::default()
                    },
                })
            }
            Self::TurnTextSmall => {
                let id = create_session(cx, "bench turn text", false).await?;
                let prompt = fake_prompt(
                    cx.runtime,
                    "bench text",
                    json!({ "fakeText": "bench text" }),
                );
                let out = prompt_and_wait(cx, &id, &prompt).await?;
                verify_messages(cx, &id, &["bench text"]).await?;
                Ok(out)
            }
            Self::Manager2Sessions => {
                let a = create_session(cx, "bench manager a", false).await?;
                let b = create_session(cx, "bench manager b", false).await?;
                let pa = fake_prompt(
                    cx.runtime,
                    "manager one",
                    json!({ "fakeDelayMs": 80, "fakeText": "manager one" }),
                );
                let pb = fake_prompt(
                    cx.runtime,
                    "manager two",
                    json!({ "fakeDelayMs": 40, "fakeText": "manager two" }),
                );
                let start = Instant::now();
                let (ra, rb) =
                    tokio::join!(prompt_and_wait(cx, &a, &pa), prompt_and_wait(cx, &b, &pb));
                let mut out = merge_outcomes(ra?, rb?);
                out.timings.request_to_idle_ms = Some(start.elapsed().as_millis());
                verify_messages(cx, &a, &["manager one"]).await?;
                verify_messages(cx, &b, &["manager two"]).await?;
                Ok(out)
            }
            Self::TaskSingleChild => task_fanout(cx, 1).await,
            Self::TaskFanout4 => task_fanout(cx, 4).await,
            Self::History100Messages => history_100(cx).await,
            Self::AbortInflight => abort_inflight(cx).await,
        }
    }
}

async fn task_fanout(cx: &ScenarioCx, n: usize) -> OracleResult<ScenarioOutcome> {
    let id = create_session(cx, &format!("bench task fanout {n}"), true).await?;
    let calls = (0..n)
        .map(|idx| {
            let child = format!("child {idx} done");
            json!({
                "tool": "task",
                "input": {
                    "description": format!("bench child {idx}"),
                    "prompt": task_child_prompt(cx.runtime, &child),
                    "subagent_type": "general"
                }
            })
        })
        .collect::<Vec<_>>();
    let prompt = fake_prompt(
        cx.runtime,
        "parent",
        json!({
            "fakeText": "parent",
            "fakeToolCalls": calls
        }),
    );
    let start = Instant::now();
    let _: Value = cx
        .client
        .post_json(&format!("/session/{id}/prompt_async"), Some(&prompt))
        .await?;
    let accept = start.elapsed().as_millis();
    wait_for_children(cx, &id, n, Duration::from_secs(30)).await?;
    wait_for_message(
        cx,
        &id,
        &["<task_result>", "child 0 done"],
        Duration::from_secs(10),
    )
    .await?;
    let out = ScenarioOutcome {
        timings: TimingMetrics {
            request_to_accept_ms: Some(accept),
            request_to_idle_ms: Some(start.elapsed().as_millis()),
            ..Default::default()
        },
        counts: CountMetrics {
            http_requests: 4,
            sessions_created: 1 + n as u64,
            messages_written: 2,
            parts_written: count_parts(cx, &id).await.unwrap_or(0),
            tool_calls: n as u64,
            ..Default::default()
        },
    };
    Ok(out)
}

async fn history_100(cx: &ScenarioCx) -> OracleResult<ScenarioOutcome> {
    let id = create_session(cx, "bench history 100", false).await?;
    let mut http = 1;
    for idx in 0..100 {
        let body = fake_prompt(
            cx.runtime,
            &format!("history seed {idx}"),
            json!({ "fakeText": format!("history seed {idx}") }),
        );
        let _: Value = cx
            .client
            .post_json(&format!("/session/{id}/message"), Some(&body))
            .await?;
        http += 1;
    }
    let body = fake_prompt(
        cx.runtime,
        "history final",
        json!({ "fakeText": "history final" }),
    );
    let mut out = prompt_and_wait(cx, &id, &body).await?;
    verify_messages(cx, &id, &["history seed 99", "history final"]).await?;
    out.counts.http_requests += http;
    Ok(out)
}

async fn abort_inflight(cx: &ScenarioCx) -> OracleResult<ScenarioOutcome> {
    let id = create_session(cx, "bench abort", false).await?;
    let body = fake_prompt(
        cx.runtime,
        "abort me",
        json!({ "fakeDelayMs": 5000, "fakeText": "too late" }),
    );
    let start = Instant::now();
    let _: Value = cx
        .client
        .post_json(&format!("/session/{id}/prompt_async"), Some(&body))
        .await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let abort_start = Instant::now();
    let _: Value = cx
        .client
        .post_json::<Value>(&format!("/session/{id}/abort"), None)
        .await?;
    wait_for_message(cx, &id, &["MessageAbortedError"], Duration::from_secs(10)).await?;
    Ok(ScenarioOutcome {
        timings: TimingMetrics {
            request_to_accept_ms: Some(start.elapsed().as_millis()),
            abort_to_terminal_ms: Some(abort_start.elapsed().as_millis()),
            request_to_idle_ms: Some(start.elapsed().as_millis()),
            ..Default::default()
        },
        counts: CountMetrics {
            http_requests: 4,
            sessions_created: 1,
            messages_written: 2,
            ..Default::default()
        },
    })
}

async fn create_session(cx: &ScenarioCx, title: &str, task: bool) -> OracleResult<String> {
    let permission = if task {
        json!([{ "permission": "task", "pattern": "*", "action": "allow" }])
    } else {
        json!([])
    };
    let body = json!({
        "title": title,
        "permission": permission
    });
    let value = cx.client.post_json("/session", Some(&body)).await?;
    value
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| OracleError::other(format!("session create did not return id: {value}")))
}

fn fake_prompt(runtime: BenchRuntime, text: &str, mut control: Value) -> Value {
    match runtime {
        BenchRuntime::Bun => json!({
            "parts": [{ "type": "text", "text": format!("{FAKE_PREFIX}{}", serde_json::to_string(&control).unwrap()) }],
            "model": { "providerID": "fake", "modelID": "fake-echo" }
        }),
        BenchRuntime::Rust => {
            if let Some(map) = control.as_object_mut() {
                map.entry("fake".to_string()).or_insert(Value::Bool(true));
            }
            json!({
                "parts": [{ "type": "text", "text": text }],
                "provider": control
            })
        }
    }
}

fn task_child_prompt(runtime: BenchRuntime, text: &str) -> String {
    match runtime {
        BenchRuntime::Bun => format!(
            "{FAKE_PREFIX}{}",
            serde_json::to_string(&json!({ "fakeText": text })).unwrap()
        ),
        BenchRuntime::Rust => text.to_string(),
    }
}

async fn prompt_and_wait(cx: &ScenarioCx, id: &str, body: &Value) -> OracleResult<ScenarioOutcome> {
    let response = cx.client.open_global_event_stream().await?;
    let record_start = Instant::now();
    let watch = id.to_string();
    let task: JoinHandle<OracleResult<Vec<SseFrame>>> = tokio::spawn(async move {
        SseRecorder::new()
            .record(
                response,
                StopCondition::Predicate(Box::new(move |_frame, parsed| {
                    matches!(
                        payload_type(parsed),
                        Some("session.idle" | "session.turn.close" | "session.error")
                    ) && payload_session(parsed) == Some(watch.as_str())
                })),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let request_start = Instant::now();
    let _: Value = cx
        .client
        .post_json(&format!("/session/{id}/prompt_async"), Some(body))
        .await?;
    let accept = request_start.elapsed().as_millis();
    let frames = timeout(Duration::from_secs(30), task)
        .await
        .map_err(|_| OracleError::ScenarioAborted("timed out waiting for prompt idle"))?
        .map_err(|err| OracleError::other(format!("sse recorder join failed: {err}")))??;
    let base = request_start
        .duration_since(record_start)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);
    let first_delta = first_event_after(&frames, base, |frame, parsed| {
        is_payload_type(parsed, "message.part.delta") || frame.data.contains("message.part.delta")
    });
    let first_tool = first_event_after(&frames, base, |_frame, parsed| {
        parsed
            .as_ref()
            .and_then(|value| value.get("payload"))
            .and_then(|payload| payload.get("syncEvent"))
            .and_then(|sync| sync.get("data"))
            .and_then(|data| data.get("part"))
            .is_some_and(|part| {
                part.get("type").and_then(Value::as_str) == Some("tool")
                    && part
                        .get("state")
                        .and_then(|state| state.get("status"))
                        .and_then(Value::as_str)
                        .is_some_and(|status| status == "running" || status == "completed")
            })
    });
    let idle = first_event_after(&frames, base, |_frame, parsed| {
        matches!(
            payload_type(parsed),
            Some("session.idle" | "session.turn.close" | "session.error")
        ) && payload_session(parsed) == Some(id)
    });
    Ok(ScenarioOutcome {
        timings: TimingMetrics {
            request_to_accept_ms: Some(accept),
            request_to_first_delta_ms: first_delta.map(u128::from),
            request_to_first_tool_start_ms: first_tool.map(u128::from),
            request_to_idle_ms: idle.map(u128::from),
            ..Default::default()
        },
        counts: CountMetrics {
            http_requests: 3,
            sse_events: frames.len() as u64,
            sessions_created: 1,
            messages_written: 2,
            parts_written: count_parts(cx, id).await.unwrap_or(0),
            ..Default::default()
        },
    })
}

fn first_event_after<F>(frames: &[SseFrame], base: u64, f: F) -> Option<u64>
where
    F: Fn(&SseFrame, &Option<Value>) -> bool,
{
    frames.iter().find_map(|frame| {
        let parsed = serde_json::from_str::<Value>(&frame.data).ok();
        if f(frame, &parsed) {
            return Some(frame.wall_offset_ms.saturating_sub(base));
        }
        None
    })
}

async fn verify_messages(cx: &ScenarioCx, id: &str, needles: &[&str]) -> OracleResult<()> {
    wait_for_message(cx, id, needles, Duration::from_secs(10)).await
}

async fn wait_for_message(
    cx: &ScenarioCx,
    id: &str,
    needles: &[&str],
    limit: Duration,
) -> OracleResult<()> {
    let until = Instant::now() + limit;
    let mut last = String::new();
    while Instant::now() < until {
        let messages = cx
            .client
            .get_json(&format!("/session/{id}/message"))
            .await?;
        let text = serde_json::to_string(&messages)?;
        if needles.iter().all(|needle| text.contains(needle)) {
            return Ok(());
        }
        last = text;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(OracleError::other(format!(
        "messages for {id} did not contain {needles:?}; last={last}"
    )))
}

async fn wait_for_children(
    cx: &ScenarioCx,
    id: &str,
    expected: usize,
    limit: Duration,
) -> OracleResult<()> {
    let until = Instant::now() + limit;
    let mut last = Value::Null;
    while Instant::now() < until {
        let children = cx
            .client
            .get_json(&format!("/session/{id}/children"))
            .await?;
        let count = children.as_array().map(Vec::len).unwrap_or_default();
        if count == expected {
            return Ok(());
        }
        last = children;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(OracleError::other(format!(
        "expected {expected} child session(s) for {id}; last={last}"
    )))
}

async fn count_parts(cx: &ScenarioCx, id: &str) -> OracleResult<u64> {
    let messages = cx
        .client
        .get_json(&format!("/session/{id}/message"))
        .await?;
    Ok(messages
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|msg| msg.get("parts").and_then(Value::as_array))
        .map(|parts| parts.len() as u64)
        .sum())
}

fn merge_outcomes(a: ScenarioOutcome, b: ScenarioOutcome) -> ScenarioOutcome {
    ScenarioOutcome {
        timings: a.timings.max(b.timings),
        counts: a.counts + b.counts,
    }
}

fn payload_type(parsed: &Option<Value>) -> Option<&str> {
    parsed.as_ref()?.get("payload")?.get("type")?.as_str()
}

fn payload_session(parsed: &Option<Value>) -> Option<&str> {
    parsed
        .as_ref()?
        .get("payload")?
        .get("properties")?
        .get("sessionID")?
        .as_str()
}

fn is_payload_type(parsed: &Option<Value>, kind: &str) -> bool {
    payload_type(parsed) == Some(kind)
}

fn spawn_cfg(runtime: BenchRuntime, bin: &Path, root: &Path, repo: &Path) -> SpawnConfig {
    let home = root.join("home");
    let data = root.join("data");
    let config = root.join("config");
    let state = root.join("state");
    let cache = root.join("cache");
    let mut cfg = SpawnConfig::default()
        .with_binary(bin)
        .with_cwd(repo)
        .with_password("bench")
        .with_extra_env("KILO_TEST_HOME", home.to_string_lossy())
        .with_extra_env("HOME", home.to_string_lossy())
        .with_extra_env("USERPROFILE", home.to_string_lossy())
        .with_extra_env("XDG_DATA_HOME", data.to_string_lossy())
        .with_extra_env("XDG_CONFIG_HOME", config.to_string_lossy())
        .with_extra_env("XDG_STATE_HOME", state.to_string_lossy())
        .with_extra_env("XDG_CACHE_HOME", cache.to_string_lossy())
        .with_extra_env("KILO_DISABLE_MODELS_FETCH", "1")
        .with_extra_env("KILO_DISABLE_DEFAULT_PLUGINS", "1")
        .with_extra_env("KILO_TELEMETRY_LEVEL", "off")
        .with_extra_env("KILO_SERVER_PASSWORD", "bench");
    if runtime == BenchRuntime::Bun {
        cfg = cfg.with_extra_env("KILO_BENCH_FAKE_PROVIDER", "1");
    }
    cfg
}

fn seed_workspace(repo: &Path, seed: &str) -> OracleResult<()> {
    fs::create_dir_all(repo.join("src")).map_err(|source| OracleError::Io {
        path: repo.join("src"),
        source,
    })?;
    fs::write(repo.join("note.txt"), "needle\nsecond\n").map_err(|source| OracleError::Io {
        path: repo.join("note.txt"),
        source,
    })?;
    fs::write(repo.join("src").join("main.rs"), "fn main() {}\n").map_err(|source| {
        OracleError::Io {
            path: repo.join("src").join("main.rs"),
            source,
        }
    })?;
    if matches!(seed, "medium" | "large" | "huge") {
        for idx in 0..256 {
            fs::write(repo.join("src").join(format!("file-{idx}.txt")), "hello\n").map_err(
                |source| OracleError::Io {
                    path: repo.join("src").join(format!("file-{idx}.txt")),
                    source,
                },
            )?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TimingMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spawn_to_ready_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_to_accept_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_to_first_delta_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_to_first_tool_start_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_to_idle_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abort_to_terminal_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shutdown_ms: Option<u128>,
}

impl TimingMetrics {
    fn max(self, other: Self) -> Self {
        Self {
            spawn_to_ready_ms: max_opt(self.spawn_to_ready_ms, other.spawn_to_ready_ms),
            request_to_accept_ms: max_opt(self.request_to_accept_ms, other.request_to_accept_ms),
            request_to_first_delta_ms: max_opt(
                self.request_to_first_delta_ms,
                other.request_to_first_delta_ms,
            ),
            request_to_first_tool_start_ms: max_opt(
                self.request_to_first_tool_start_ms,
                other.request_to_first_tool_start_ms,
            ),
            request_to_idle_ms: max_opt(self.request_to_idle_ms, other.request_to_idle_ms),
            abort_to_terminal_ms: max_opt(self.abort_to_terminal_ms, other.abort_to_terminal_ms),
            shutdown_ms: max_opt(self.shutdown_ms, other.shutdown_ms),
        }
    }
}

fn max_opt<T: Ord>(a: Option<T>, b: Option<T>) -> Option<T> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResourceDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_peak_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rss_after_settle_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_user_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_kernel_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threads_peak: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handles_peak: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_processes_peak: Option<u32>,
}

impl ResourceDelta {
    fn from_samples(
        before: Option<ResourceSample>,
        after: Option<ResourceSample>,
        settled: Option<ResourceSample>,
    ) -> Self {
        let peak = [before.as_ref(), after.as_ref(), settled.as_ref()];
        Self {
            rss_peak_bytes: peak
                .iter()
                .filter_map(|sample| sample.and_then(|sample| sample.rss_bytes))
                .max(),
            rss_after_settle_bytes: settled.as_ref().and_then(|sample| sample.rss_bytes),
            cpu_user_ms: delta_opt(
                before.as_ref().and_then(|sample| sample.cpu_user_ms),
                after.as_ref().and_then(|sample| sample.cpu_user_ms),
            ),
            cpu_kernel_ms: delta_opt(
                before.as_ref().and_then(|sample| sample.cpu_kernel_ms),
                after.as_ref().and_then(|sample| sample.cpu_kernel_ms),
            ),
            threads_peak: peak
                .iter()
                .filter_map(|sample| sample.and_then(|sample| sample.threads))
                .max(),
            handles_peak: peak
                .iter()
                .filter_map(|sample| sample.and_then(|sample| sample.handles))
                .max(),
            child_processes_peak: peak
                .iter()
                .filter_map(|sample| sample.and_then(|sample| sample.child_processes))
                .max(),
        }
    }
}

fn delta_opt(before: Option<u64>, after: Option<u64>) -> Option<u64> {
    Some(after?.saturating_sub(before?))
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CountMetrics {
    pub http_requests: u64,
    pub sse_events: u64,
    pub sessions_created: u64,
    pub messages_written: u64,
    pub parts_written: u64,
    pub tool_calls: u64,
}

impl std::ops::Add for CountMetrics {
    type Output = CountMetrics;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            http_requests: self.http_requests + rhs.http_requests,
            sse_events: self.sse_events + rhs.sse_events,
            sessions_created: self.sessions_created + rhs.sessions_created,
            messages_written: self.messages_written + rhs.messages_written,
            parts_written: self.parts_written + rhs.parts_written,
            tool_calls: self.tool_calls + rhs.tool_calls,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchTrial {
    pub runtime: BenchRuntime,
    pub scenario: String,
    pub trial: usize,
    pub warmup: bool,
    pub ok: bool,
    pub timings_ms: TimingMetrics,
    pub resources: ResourceDelta,
    pub counts: CountMetrics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl BenchTrial {
    fn empty(runtime: BenchRuntime, scenario: BenchScenario, trial: usize, warmup: bool) -> Self {
        Self {
            runtime,
            scenario: scenario.name().to_string(),
            trial,
            warmup,
            ok: false,
            timings_ms: TimingMetrics::default(),
            resources: ResourceDelta::default(),
            counts: CountMetrics::default(),
            error: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchCompareReport {
    pub output: PathBuf,
    pub trials: Vec<BenchTrial>,
    pub summary: Vec<BenchSummaryRow>,
}

impl BenchCompareReport {
    fn from_trials(cfg: BenchCompareConfig, trials: Vec<BenchTrial>) -> Self {
        let summary = summarize(&trials);
        Self {
            output: cfg.output,
            trials,
            summary,
        }
    }

    pub fn markdown(&self) -> String {
        let mut out = String::new();
        out.push_str("| Scenario | Metric | Bun p50 | Rust p50 | Ratio | Winner | Notes |\n");
        out.push_str("|---|---:|---:|---:|---:|---|---|\n");
        for row in &self.summary {
            let bun = row
                .bun_p50
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string());
            let rust = row
                .rust_p50
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string());
            let ratio = row
                .rust_vs_bun_ratio
                .map(|v| format!("{v:.2}"))
                .unwrap_or_else(|| "-".to_string());
            let _ = writeln!(
                out,
                "| `{}` | `{}` | {} | {} | {} | {} | {} |",
                row.scenario, row.metric, bun, rust, ratio, row.winner, row.notes
            );
        }
        out
    }

    pub fn write_markdown(&self, path: impl AsRef<Path>) -> OracleResult<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| OracleError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        fs::write(path, self.markdown()).map_err(|source| OracleError::Io {
            path: path.to_path_buf(),
            source,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchSummaryRow {
    pub scenario: String,
    pub metric: String,
    pub bun_p50: Option<u128>,
    pub rust_p50: Option<u128>,
    pub rust_vs_bun_ratio: Option<f64>,
    pub winner: String,
    pub notes: String,
}

fn summarize(trials: &[BenchTrial]) -> Vec<BenchSummaryRow> {
    let mut grouped: BTreeMap<(String, String, BenchRuntime), Vec<u128>> = BTreeMap::new();
    for trial in trials.iter().filter(|trial| trial.ok && !trial.warmup) {
        for (name, value) in trial_metric_values(trial) {
            grouped
                .entry((trial.scenario.clone(), name.to_string(), trial.runtime))
                .or_default()
                .push(value);
        }
    }
    let mut keys = grouped
        .keys()
        .map(|(scenario, metric, _)| (scenario.clone(), metric.clone()))
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .map(|(scenario, metric)| {
            let bun_p50 = grouped
                .get(&(scenario.clone(), metric.clone(), BenchRuntime::Bun))
                .map(|values| percentile(values.clone(), 50));
            let rust_p50 = grouped
                .get(&(scenario.clone(), metric.clone(), BenchRuntime::Rust))
                .map(|values| percentile(values.clone(), 50));
            let ratio = match (bun_p50, rust_p50) {
                (Some(bun), Some(rust)) if bun > 0 => Some(rust as f64 / bun as f64),
                _ => None,
            };
            let winner = match (bun_p50, rust_p50) {
                (Some(bun), Some(rust)) if rust < bun => "rust",
                (Some(bun), Some(rust)) if bun < rust => "bun",
                (Some(_), Some(_)) => "tie",
                (None, Some(_)) => "rust-only",
                (Some(_), None) => "bun-only",
                (None, None) => "n/a",
            }
            .to_string();
            BenchSummaryRow {
                scenario,
                metric,
                bun_p50,
                rust_p50,
                rust_vs_bun_ratio: ratio,
                winner,
                notes: String::new(),
            }
        })
        .collect()
}

fn trial_metric_values(trial: &BenchTrial) -> Vec<(&'static str, u128)> {
    let mut out = Vec::new();
    push_metric(
        &mut out,
        "spawn_to_ready_ms",
        trial.timings_ms.spawn_to_ready_ms,
    );
    push_metric(
        &mut out,
        "request_to_accept_ms",
        trial.timings_ms.request_to_accept_ms,
    );
    push_metric(
        &mut out,
        "request_to_first_delta_ms",
        trial.timings_ms.request_to_first_delta_ms,
    );
    push_metric(
        &mut out,
        "request_to_first_tool_start_ms",
        trial.timings_ms.request_to_first_tool_start_ms,
    );
    push_metric(
        &mut out,
        "request_to_idle_ms",
        trial.timings_ms.request_to_idle_ms,
    );
    push_metric(
        &mut out,
        "abort_to_terminal_ms",
        trial.timings_ms.abort_to_terminal_ms,
    );
    push_metric(&mut out, "shutdown_ms", trial.timings_ms.shutdown_ms);
    if let Some(value) = trial.resources.rss_peak_bytes {
        out.push(("rss_peak_bytes", u128::from(value)));
    }
    if let Some(value) = trial.resources.rss_after_settle_bytes {
        out.push(("rss_after_settle_bytes", u128::from(value)));
    }
    out
}

fn push_metric(out: &mut Vec<(&'static str, u128)>, name: &'static str, value: Option<u128>) {
    if let Some(value) = value {
        out.push((name, value));
    }
}

fn percentile(mut values: Vec<u128>, pct: u32) -> u128 {
    values.sort_unstable();
    if values.is_empty() {
        return 0;
    }
    let idx = ((values.len() - 1) as u128 * u128::from(pct) / 100) as usize;
    values[idx]
}

#[derive(Clone, Debug)]
struct ResourceSample {
    rss_bytes: Option<u64>,
    cpu_user_ms: Option<u64>,
    cpu_kernel_ms: Option<u64>,
    threads: Option<u32>,
    handles: Option<u32>,
    child_processes: Option<u32>,
}

impl ResourceSample {
    fn capture(pid: u32) -> Option<Self> {
        platform_sample(pid)
    }
}

#[cfg(target_os = "windows")]
fn platform_sample(pid: u32) -> Option<ResourceSample> {
    let script = format!(
        "$p=Get-Process -Id {pid}; \
         $c=(Get-CimInstance Win32_Process -Filter \"ParentProcessId={pid}\" | Measure-Object).Count; \
         [Console]::WriteLine((@($p.WorkingSet64,$p.UserProcessorTime.TotalMilliseconds,$p.PrivilegedProcessorTime.TotalMilliseconds,$p.Threads.Count,$p.HandleCount,$c) -join ','))"
    );
    let output = Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    let cells = raw.trim().split(',').collect::<Vec<_>>();
    Some(ResourceSample {
        rss_bytes: cells.first().and_then(|v| v.parse().ok()),
        cpu_user_ms: cells.get(1).and_then(|v| parse_float_u64(v)),
        cpu_kernel_ms: cells.get(2).and_then(|v| parse_float_u64(v)),
        threads: cells.get(3).and_then(|v| v.parse().ok()),
        handles: cells.get(4).and_then(|v| v.parse().ok()),
        child_processes: cells.get(5).and_then(|v| v.parse().ok()),
    })
}

#[cfg(target_os = "linux")]
fn platform_sample(pid: u32) -> Option<ResourceSample> {
    let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rss_bytes = status.lines().find_map(|line| {
        let rest = line.strip_prefix("VmRSS:")?;
        let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
        Some(kb.saturating_mul(1024))
    });
    let threads = status
        .lines()
        .find_map(|line| line.strip_prefix("Threads:")?.trim().parse::<u32>().ok());
    let after = stat.rsplit_once(") ")?.1;
    let fields = after.split_whitespace().collect::<Vec<_>>();
    let user = fields.get(11).and_then(|v| v.parse::<u64>().ok());
    let kernel = fields.get(12).and_then(|v| v.parse::<u64>().ok());
    let cpu_user_ms = user.map(|ticks| ticks.saturating_mul(10));
    let cpu_kernel_ms = kernel.map(|ticks| ticks.saturating_mul(10));
    let child_processes = fs::read_dir(format!("/proc/{pid}/task"))
        .ok()
        .map(|entries| {
            entries
                .flatten()
                .filter_map(|entry| fs::read_to_string(entry.path().join("children")).ok())
                .map(|text| text.split_ascii_whitespace().count() as u32)
                .sum()
        });
    Some(ResourceSample {
        rss_bytes,
        cpu_user_ms,
        cpu_kernel_ms,
        threads,
        handles: None,
        child_processes,
    })
}

#[cfg(target_os = "macos")]
fn platform_sample(pid: u32) -> Option<ResourceSample> {
    let output = Command::new("ps")
        .args(["-o", "rss=,utime=,time=,thcount=", "-p", &pid.to_string()])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    let cells = raw.split_whitespace().collect::<Vec<_>>();
    Some(ResourceSample {
        rss_bytes: cells
            .first()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|kb| kb * 1024),
        cpu_user_ms: None,
        cpu_kernel_ms: None,
        threads: cells.get(3).and_then(|v| v.parse().ok()),
        handles: None,
        child_processes: child_processes_pgrep(pid),
    })
}

#[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
fn platform_sample(_pid: u32) -> Option<ResourceSample> {
    None
}

#[cfg(target_os = "macos")]
fn child_processes_pgrep(pid: u32) -> Option<u32> {
    let output = Command::new("pgrep")
        .args(["-P", &pid.to_string()])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    let raw = String::from_utf8_lossy(&output.stdout);
    Some(raw.lines().filter(|line| !line.trim().is_empty()).count() as u32)
}

#[cfg(target_os = "windows")]
fn parse_float_u64(value: &str) -> Option<u64> {
    value.parse::<f64>().ok().map(|value| value.max(0.0) as u64)
}

pub fn default_rust_binary_path() -> PathBuf {
    if let Ok(env) = std::env::var("KILO_BENCH_RUST_BINARY") {
        return PathBuf::from(env);
    }
    let bin = if cfg!(windows) {
        "kilo-vscode-sidecar.exe"
    } else {
        "kilo-vscode-sidecar"
    };
    if let Some(repo) = find_repo_root() {
        return repo
            .join("packages")
            .join("kilo-vscode")
            .join("bin")
            .join(bin);
    }
    PathBuf::from(bin)
}

fn find_repo_root() -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    let mut cursor: &Path = cwd.as_path();
    loop {
        if cursor.join("packages").join("kilo-vscode").exists()
            && cursor
                .join("packages")
                .join("kilo-vscode-sidecar-rs")
                .exists()
        {
            return Some(cursor.to_path_buf());
        }
        cursor = cursor.parent()?;
    }
}
