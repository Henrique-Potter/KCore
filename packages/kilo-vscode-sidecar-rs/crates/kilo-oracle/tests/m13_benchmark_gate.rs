use std::path::PathBuf;
use std::time::{Duration, Instant};

mod rust_harness;

use kilo_oracle::benchmark::{BenchmarkReport, BenchmarkThresholds, GateMetric};
use kilo_oracle::sse::{SseRecorder, StopCondition};
use rust_harness::{create_session, fake_prompt, prompt_async, RustSidecar};
use serde_json::json;

/// Read the test process's resident-set size in bytes. The M13 harness runs
/// the sidecar in-process, so this measurement is the sidecar's RSS plus
/// the small test-runner overhead. Returns `None` on unsupported platforms
/// or sampling failures so the metric falls back to `not_measured` rather
/// than panicking the gate.
fn current_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let buf = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in buf.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kib: u64 = rest
                    .trim()
                    .strip_suffix(" kB")
                    .unwrap_or_else(|| rest.trim())
                    .trim()
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()?;
                return Some(kib.saturating_mul(1024));
            }
        }
        None
    }
    #[cfg(target_os = "windows")]
    {
        // Pure-stdlib path: shell out to tasklist. The CSV format quotes
        // every field, so a row looks like:
        //   "kilo.exe","10640","Console","1","71,708 K"
        // The "Mem Usage" cell is the last quoted segment and uses a
        // thousands separator inside the quotes.
        use std::process::{Command, Stdio};
        let pid = std::process::id().to_string();
        let output = Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let line = String::from_utf8_lossy(&output.stdout).into_owned();
        // Split on "," to separate cells; tolerate a trailing newline.
        let cells: Vec<&str> = line.trim().trim_matches('"').split("\",\"").collect();
        // Mem Usage is the last cell; format is "12,345 K".
        let mem = cells.last()?.trim().trim_end_matches('"').trim();
        let kb = mem.strip_suffix(" K").or_else(|| mem.strip_suffix("K"))?;
        let digits: String = kb.chars().filter(|c| c.is_ascii_digit()).collect();
        digits.parse::<u64>().ok().map(|n| n.saturating_mul(1024))
    }
    #[cfg(target_os = "macos")]
    {
        // macOS: shell out to `ps -o rss= -p <pid>`. RSS is in KB.
        use std::process::{Command, Stdio};
        let pid = std::process::id().to_string();
        let output = Command::new("ps")
            .args(["-o", "rss=", "-p", &pid])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
        s.parse::<u64>().ok().map(|kb| kb.saturating_mul(1024))
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        None
    }
}

/// Sum of user + kernel CPU time consumed by the test process so far,
/// in microseconds. Returns `None` on platforms where the sample isn't
/// implementable in stdlib alone. Both samples must be drawn from the
/// same source for the delta to be meaningful, so the helper is
/// platform-uniform.
fn current_cpu_time_micros() -> Option<u128> {
    #[cfg(target_os = "linux")]
    {
        // /proc/self/stat: clock-tick units. Field 14 (utime) + 15 (stime).
        // Convert to microseconds via sysconf(_SC_CLK_TCK) — we approximate
        // as 100 (the canonical Linux value) since libc is not a dep.
        // Drift on non-default tick rates is acceptable for benchmarking.
        let buf = std::fs::read_to_string("/proc/self/stat").ok()?;
        let after_comm = buf.rsplit_once(") ")?;
        let fields: Vec<&str> = after_comm.1.split_whitespace().collect();
        // Field 14/15 in the original stat are at offsets 11/12 of the
        // post-`)` slice (because fields 1 + 2 are pid + comm).
        let utime: u64 = fields.get(11)?.parse().ok()?;
        let stime: u64 = fields.get(12)?.parse().ok()?;
        let ticks = u128::from(utime + stime);
        Some(ticks * 1_000_000 / 100)
    }
    #[cfg(target_os = "windows")]
    {
        // wmic was removed in Windows 11. Use PowerShell's `Get-Process`,
        // which exposes `TotalProcessorTime` as a `TimeSpan` we can
        // serialize to a known format. `Ticks` is in 100-ns units.
        use std::process::{Command, Stdio};
        let pid = std::process::id();
        let cmd = format!("(Get-Process -Id {pid}).TotalProcessorTime.Ticks",);
        let output = Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command", &cmd])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let raw = String::from_utf8_lossy(&output.stdout);
        let ticks: u128 = raw.trim().parse().ok()?;
        // 100-ns ticks → microseconds.
        Some(ticks / 10)
    }
    #[cfg(target_os = "macos")]
    {
        // macOS: `ps -o time= -p <pid>` returns the cumulative CPU time
        // formatted as `MM:SS.SS` or `H:MM:SS`. Parsing it cheaply isn't
        // free, but it's stdlib-only.
        use std::process::{Command, Stdio};
        let pid = std::process::id().to_string();
        let output = Command::new("ps")
            .args(["-o", "time=", "-p", &pid])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let s = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let mut total_seconds: u128 = 0;
        for part in s.rsplitn(3, ':') {
            // For HH:MM:SS, rsplitn yields seconds, minutes, hours in that order.
            let value = part.parse::<f64>().unwrap_or(0.0);
            total_seconds = total_seconds * 60 + (value as u128);
        }
        Some(total_seconds * 1_000_000)
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        None
    }
}

/// Compute idle CPU% over the supplied wall-clock window. Returns
/// `None` if either sample fails. The result is `100 * cpu_us / wall_us`,
/// capped at 100 (a process can briefly report >100% on multi-core
/// utilization, but for an "idle CPU with SSE connected" gate we cap to
/// keep the metric comparable across systems).
fn cpu_pct_over(start: u128, wall: Duration, end: u128) -> Option<u32> {
    let used = end.checked_sub(start)?;
    let wall_us = wall.as_micros();
    if wall_us == 0 {
        return None;
    }
    let pct = (used * 100 / wall_us).min(100);
    Some(pct as u32)
}

/// CPU% gate metric. The plan defines "idle CPU with SSE connected"
/// as "under 5% (preview), near zero (stable)". `KILO_M13_IDLE_CPU_PCT`
/// configures the threshold; no env var ⇒ sample-and-report only.
fn cpu_metric(label: &'static str, sample: Option<u32>, limit: Option<u32>) -> GateMetric {
    match (sample, limit) {
        (Some(pct), Some(limit)) => GateMetric::bytes(label, u64::from(pct), u64::from(limit)),
        (Some(pct), None) => GateMetric::not_measured(
            label,
            format!("sampled cpu={pct}%; gate threshold not configured"),
        ),
        (None, _) => {
            GateMetric::unsupported(label, "CPU sampling failed or unsupported on this platform")
        }
    }
}

/// Count children of the test process. Used to assert the in-process
/// harness doesn't leak any spawned bash/git children. Returns `None`
/// on platforms where the count isn't readable in stdlib.
fn child_process_count() -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        // /proc/self/task/<tid>/children — comma-separated PIDs of
        // direct children. We sum across all task directories so a
        // multi-threaded process still gets a meaningful count.
        let mut count: u32 = 0;
        let entries = std::fs::read_dir("/proc/self/task").ok()?;
        for entry in entries.flatten() {
            let children_path = entry.path().join("children");
            let Ok(buf) = std::fs::read_to_string(&children_path) else {
                continue;
            };
            count = count.saturating_add(buf.split_ascii_whitespace().count() as u32);
        }
        Some(count)
    }
    #[cfg(target_os = "windows")]
    {
        // PowerShell: `Get-CimInstance Win32_Process -Filter
        // "ParentProcessId=<pid>"`. Print one line per row and count.
        use std::process::{Command, Stdio};
        let pid = std::process::id();
        let cmd = format!(
            "(Get-CimInstance Win32_Process -Filter \"ParentProcessId={pid}\" | Measure-Object).Count",
        );
        let output = Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command", &cmd])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let raw = String::from_utf8_lossy(&output.stdout);
        raw.trim().parse::<u32>().ok()
    }
    #[cfg(target_os = "macos")]
    {
        use std::process::{Command, Stdio};
        let pid = std::process::id().to_string();
        let output = Command::new("pgrep")
            .args(["-P", &pid])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        // pgrep returns nonzero when no children — treat as zero.
        let raw = String::from_utf8_lossy(&output.stdout);
        Some(raw.lines().filter(|l| !l.trim().is_empty()).count() as u32)
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        None
    }
}

/// Leaked-children gate. Asserts no growth in direct children between
/// the start and end of the test run. Threshold is implicit: any growth
/// fails the gate.
fn leak_metric(label: &'static str, before: Option<u32>, after: Option<u32>) -> GateMetric {
    match (before, after) {
        (Some(before), Some(after)) => {
            let growth = after.saturating_sub(before);
            // Reuse `bytes` as a generic numeric carrier; threshold is 0
            // (any growth fails). For platforms where samples are
            // available, this is the strict gate.
            GateMetric::bytes(label, u64::from(growth), 0)
        }
        _ => GateMetric::unsupported(
            label,
            "child-process count failed or unsupported on this platform",
        ),
    }
}

/// Build the RSS gate metric from a sample and an optional threshold.
///
/// - Sample succeeds + threshold set → byte-gate `GateMetric::bytes(...)`,
///   the gate fails if the sample exceeds the threshold.
/// - Sample succeeds + no threshold → `not_measured` carrying the sampled
///   value in the note so CI can still observe drift.
/// - Sample fails (unsupported platform) → `unsupported`.
fn rss_metric(label: &'static str, sample: Option<u64>, limit: Option<u64>) -> GateMetric {
    match (sample, limit) {
        (Some(bytes), Some(limit)) => GateMetric::bytes(label, bytes, limit),
        (Some(bytes), None) => GateMetric::not_measured(
            label,
            format!("sampled rss={bytes} bytes; gate threshold not configured"),
        ),
        (None, _) => {
            GateMetric::unsupported(label, "RSS sampling failed or unsupported on this platform")
        }
    }
}

fn package_metric() -> GateMetric {
    let Some(path) = std::env::var_os("KILO_M13_PACKAGE_PATH").map(PathBuf::from) else {
        return GateMetric::not_measured(
            "package_size",
            "set KILO_M13_PACKAGE_PATH and KILO_M13_PACKAGE_SIZE_BYTES to gate package size",
        );
    };
    let Some(limit) = BenchmarkThresholds::from_env().package_size_bytes else {
        return GateMetric::not_measured(
            "package_size",
            "set KILO_M13_PACKAGE_SIZE_BYTES to gate package size",
        );
    };
    match std::fs::metadata(&path) {
        Ok(meta) => GateMetric::bytes("package_size", meta.len(), limit),
        Err(err) => GateMetric::not_measured("package_size", format!("metadata failed: {err}")),
    }
}

fn is_sse_type(frame: &kilo_oracle::sse::SseFrame, kind: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(&frame.data)
        .ok()
        .and_then(|value| {
            value
                .get("payload")?
                .get("type")?
                .as_str()
                .map(str::to_string)
        })
        .as_deref()
        == Some(kind)
}

/// M13 deterministic benchmark gate. This is intentionally a conservative CI-safe
/// smoke gate, not a performance lab: it launches the in-process Rust sidecar,
/// drives the fake-provider first-delta path, measures shutdown, and emits JSON.
///
/// Run from `packages/kilo-vscode-sidecar-rs`:
/// `cargo test -p kilo-oracle --test m13_benchmark_gate m13_rust_sidecar_benchmark_gate -- --nocapture`
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m13_rust_sidecar_benchmark_gate() {
    let thresholds = BenchmarkThresholds::from_env();
    let cpu_baseline_pid_count = child_process_count();
    let start = Instant::now();
    let sidecar = RustSidecar::spawn().await.expect("spawn rust sidecar");
    let cold = start.elapsed().as_millis();

    // Sample idle RSS right after readiness, before any session work.
    let idle_rss = current_rss_bytes();

    // Sample idle CPU over a short window with the SSE bus connected
    // but no work in flight. The plan's "Idle CPU with SSE connected"
    // gate target is "<5% (preview), near zero (stable)". We open the
    // global event stream, sleep through one heartbeat cycle, and
    // measure CPU% over that window.
    let idle_cpu_client = sidecar.client.clone();
    let cpu_idle_task = tokio::spawn(async move {
        // Open the SSE stream so the heartbeat path is exercised.
        let _stream = idle_cpu_client.open_global_event_stream().await.ok();
        tokio::time::sleep(Duration::from_millis(500)).await;
    });
    let cpu_start_us = current_cpu_time_micros();
    let cpu_window_start = Instant::now();
    let _ = cpu_idle_task.await;
    let idle_cpu_pct = match (cpu_start_us, current_cpu_time_micros()) {
        (Some(start), Some(end)) => cpu_pct_over(start, cpu_window_start.elapsed(), end),
        _ => None,
    };

    let id = create_session(&sidecar.client, &sidecar.repo(), "M13 benchmark gate")
        .await
        .expect("create session");
    let client = sidecar.client.clone();
    let watch = id.clone();
    let task = tokio::spawn(async move {
        let response = client.open_global_event_stream().await?;
        SseRecorder::new()
            .record(
                response,
                StopCondition::Predicate(Box::new(move |frame, parsed| {
                    is_sse_type(frame, "message.part.delta")
                        || rust_harness::payload_type(parsed) == Some("session.turn.close")
                })),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(25)).await;
    let token = Instant::now();
    prompt_async(
        &sidecar.client,
        &watch,
        &fake_prompt("m13 first token", json!({ "fakeDelayMs": 25 })),
    )
    .await
    .expect("prompt async");
    let frames = task.await.unwrap().expect("record first delta");
    assert!(frames
        .iter()
        .any(|frame| is_sse_type(frame, "message.part.delta")));
    let first = token.elapsed().as_millis();

    // Sample active RSS after the prompt stream completes.
    let active_rss = current_rss_bytes();

    let stop = Instant::now();
    sidecar.shutdown().await.expect("shutdown");
    let shutdown = stop.elapsed().as_millis();

    // Sample post-shutdown child count and compare against the
    // pre-spawn baseline. Any growth fails the gate.
    let after_pid_count = child_process_count();

    let metrics = vec![
        GateMetric::duration("cold_start_to_readiness", cold, thresholds.cold_start_ms),
        GateMetric::duration(
            "time_to_first_visible_token",
            first,
            thresholds.first_token_ms,
        ),
        GateMetric::duration("shutdown", shutdown, thresholds.shutdown_ms),
        GateMetric::duration("shutdown_hard_cap", shutdown, thresholds.shutdown_hard_ms),
        rss_metric("idle_rss", idle_rss, thresholds.idle_rss_bytes),
        rss_metric("active_rss", active_rss, thresholds.active_rss_bytes),
        cpu_metric("idle_cpu", idle_cpu_pct, thresholds.idle_cpu_pct),
        leak_metric("leaked_children", cpu_baseline_pid_count, after_pid_count),
        package_metric(),
    ];
    let report = BenchmarkReport::new(thresholds, metrics);
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    assert!(report.passed, "M13 benchmark gate failed");
}

/// **Active RSS growth gate.** Per migration plan: *"Active RSS — no
/// unbounded growth across repeated prompts; returns near idle baseline
/// after work completes."* Runs N fake-prompt turns back-to-back and
/// asserts RSS doesn't grow beyond a configurable bound.
///
/// `KILO_M13_GROWTH_TURNS` (default 10) controls how many turns to run.
/// `KILO_M13_GROWTH_MAX_BYTES` (default 32 MiB) is the maximum allowed
/// growth from the post-warmup baseline. The test always reports the
/// measured value so CI can observe trends even when the gate passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn m13_active_rss_no_unbounded_growth() {
    let turns: usize = std::env::var("KILO_M13_GROWTH_TURNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let max_growth_bytes: u64 = std::env::var("KILO_M13_GROWTH_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32 * 1024 * 1024);

    let sidecar = RustSidecar::spawn().await.expect("spawn rust sidecar");
    let id = create_session(&sidecar.client, &sidecar.repo(), "M13 growth gate")
        .await
        .expect("create session");

    // Warm up with one turn so JIT/lazy-init memory lands before the
    // baseline. Without this the first turn looks like a leak even though
    // it's just first-use allocation.
    let warmup_body = fake_prompt("warmup", json!({ "fakeDelayMs": 1 }));
    prompt_async(&sidecar.client, &id, &warmup_body)
        .await
        .expect("warmup prompt");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let baseline_rss = current_rss_bytes();

    for i in 0..turns {
        let body = fake_prompt(
            &format!("turn-{i}"),
            json!({ "fakeDelayMs": 1, "fakeText": "echo" }),
        );
        prompt_async(&sidecar.client, &id, &body)
            .await
            .expect("turn prompt");
        // Small yield so the bus drains and per-turn state can be GC'd
        // before the next iteration. Without this the test conflates
        // "in-flight per-turn buffers" with "true growth".
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Settle: give the bus a moment to drain, runners to drop, etc.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after_rss = current_rss_bytes();

    sidecar.shutdown().await.expect("shutdown");

    let growth = match (baseline_rss, after_rss) {
        (Some(before), Some(after)) => after.saturating_sub(before),
        _ => {
            eprintln!(
                "[m13_active_rss_no_unbounded_growth] RSS sampling unsupported; skipping gate"
            );
            return;
        }
    };
    println!(
        "[m13_active_rss_no_unbounded_growth] turns={turns} baseline={baseline_rss:?} after={after_rss:?} growth={growth} max_allowed={max_growth_bytes}",
    );
    assert!(
        growth <= max_growth_bytes,
        "RSS grew by {growth} bytes across {turns} turns (max allowed {max_growth_bytes}). Inspect for unbounded per-turn state.",
    );
}
