# Bun vs Rust Sidecar Benchmark Comparison

## Goal

Build a repeatable benchmark suite that answers one question cleanly:

> For Kilo's real VS Code workloads, where is Rust faster than Bun, where is Bun still faster, and where is either runtime too slow or too memory hungry to ship?

The suite must isolate sidecar/backend cost from provider latency. Use deterministic fake providers for primary comparisons, then run a small live-provider smoke only to catch integration cost that fake streams cannot model.

## Comparison Rules

- Run both runtimes as external processes through the same process contract: `serve --port 0`, readiness stdout, Basic Auth, `/global/event`, `/session/*`.
- Use a fresh temp workspace and isolated `HOME`/`USERPROFILE`/XDG dirs per trial.
- Seed identical repo files, config, agents, permissions, sessions, messages, and model/auth state before each trial.
- Disable telemetry, model discovery, default plugins, and network-dependent plugin work unless a scenario explicitly tests them.
- Use one external sampler for both runtimes. Do not compare Rust in-process RSS to Bun process RSS.
- Record raw trial JSONL first. Compute summary tables after the run.
- Report medians and tails, not one-off timings: p50, p90, p95, p99, min, max, stddev.
- Treat the first trial as warmup unless the scenario is explicitly cold-start.

Recommended shared env:

```powershell
KILO_SERVER_PASSWORD=bench
KILO_CLIENT=vscode
KILO_ENABLE_QUESTION_TOOL=true
KILOCODE_FEATURE=vscode-extension
KILO_TELEMETRY_LEVEL=off
KILO_APP_NAME=kilo-code
KILO_PLATFORM=vscode
KILO_DISABLE_CLAUDE_CODE=true
KILO_DISABLE_MODELS_FETCH=1
KILO_DISABLE_DEFAULT_PLUGINS=1
MIMALLOC_PURGE_DELAY=0
```

For Bun fake-provider scenarios also set:

```powershell
KILO_BENCH_FAKE_PROVIDER=1
```

## Harness Shape

Add a new benchmark runner rather than stretching `m13_benchmark_gate.rs` into a performance lab:

- `packages/kilo-vscode-sidecar-rs/crates/kilo-oracle/src/bench_compare.rs`
- `packages/kilo-vscode-sidecar-rs/crates/kilo-oracle/tests/bench_compare.rs`
- Optional CLI wrapper: `cargo run -p kilo-oracle --bin bench-compare -- --runtime both --scenario all --trials 30`

Runner inputs:

```json
{
  "runtime": "bun|rust",
  "binary": "path",
  "scenario": "name",
  "trials": 30,
  "warmups": 3,
  "workspace_seed": "small|medium|large|huge",
  "output": "target/bench/bun-vs-rust.jsonl"
}
```

Per-trial output:

```json
{
  "runtime": "bun",
  "scenario": "agent_task_fanout_8",
  "trial": 12,
  "ok": true,
  "timings_ms": {
    "spawn_to_ready": 481,
    "request_to_first_delta": 92,
    "request_to_idle": 812
  },
  "resources": {
    "rss_peak_bytes": 312000000,
    "rss_after_settle_bytes": 240000000,
    "cpu_user_ms": 430,
    "cpu_kernel_ms": 85,
    "threads_peak": 29,
    "handles_peak": 410,
    "child_processes_peak": 0
  },
  "counts": {
    "http_requests": 18,
    "sse_events": 154,
    "sessions_created": 9,
    "messages_written": 18,
    "parts_written": 74,
    "tool_calls": 8
  }
}
```

## Metric Set

Primary latency metrics:

| Metric | Why it matters |
|---|---|
| `spawn_to_ready_ms` | Sidebar open and Agent Manager activation feel. |
| `request_to_accept_ms` | HTTP route and busy-state overhead before async work starts. |
| `request_to_first_delta_ms` | Perceived chat responsiveness. |
| `request_to_first_tool_start_ms` | Tool orchestration latency. |
| `request_to_idle_ms` | Total turn cost after provider latency is removed. |
| `abort_to_terminal_ms` | User stop button responsiveness. |
| `shutdown_ms` | Extension dispose/reload behavior. |

Primary resource metrics:

| Metric | Why it matters |
|---|---|
| `idle_rss_bytes` | Cost of having the sidecar alive. |
| `active_rss_peak_bytes` | Burst memory during real work. |
| `rss_after_settle_bytes` | Whether per-turn state drops after completion. |
| `cpu_user_ms` / `cpu_kernel_ms` | Backend work independent of wall time. |
| `threads_peak` / `handles_peak` | Windows supportability and runaway task clues. |
| `child_processes_peak` | Tool and ripgrep/bash process leakage. |
| `sqlite_write_count` / `sqlite_busy_ms` | Session/message persistence bottlenecks. |
| `sse_events_per_sec` | Event bus throughput and client fanout pressure. |

Derived comparison fields:

| Field | Formula |
|---|---|
| `rust_vs_bun_ratio` | `rust_p50 / bun_p50` for latency or resource metric. |
| `winner` | Fastest runtime by p50 when p95 is within stability bounds. |
| `tail_regression` | Runtime whose p95 is more than 1.5x its own p50. |
| `memory_regression` | Runtime whose peak or settle RSS exceeds the other by 25% or more. |

## Scenario Matrix

### 1. Startup and Idle

| Scenario | Workload | Critical metric |
|---|---|---|
| `cold_start_ready` | Spawn sidecar to readiness, no requests. | `spawn_to_ready_ms`, `idle_rss_bytes` |
| `cold_start_first_chat` | Spawn, connect SSE, create session, fake one-token prompt. | `spawn_to_first_delta_ms` |
| `idle_sse_1` | One `/global/event` client for 30 s. | idle CPU, heartbeat stability |
| `idle_sse_16` | Sixteen SSE clients, no prompts. | idle CPU, RSS growth, event fanout |
| `shutdown_after_idle` | Spawn, connect SSE, shutdown. | `shutdown_ms`, leaked children |

### 2. Basic Session Management

| Scenario | Workload | Critical metric |
|---|---|---|
| `session_create_100` | Create 100 root sessions. | create throughput, SQLite writes |
| `session_list_1000` | Seed 1000 sessions, list roots and all. | list latency, RSS peak |
| `session_patch_100` | Rename/update 100 sessions. | update latency, SSE cost |
| `session_children_tree` | Seed 1 parent, 64 children, list family. | child lookup latency |
| `session_delete_cascade` | Delete a session with messages and children. | delete latency, DB cleanup |

### 3. Agentic Loop Baseline

| Scenario | Workload | Critical metric |
|---|---|---|
| `turn_text_small` | Fake provider emits one text delta. | first delta, idle |
| `turn_text_stream_100` | Fake provider emits 100 small deltas. | SSE throughput, write cost |
| `turn_text_stream_10k` | Fake provider emits 10k tokens as chunks. | CPU, RSS, transcript writes |
| `turn_2_iterations` | Fake provider emits tool call then continuation. | step overhead |
| `turn_doom_loop_4` | Fake provider repeats tool-call loop until permission gate. | loop guard cost |

### 4. Tool Execution and Parallelism

| Scenario | Workload | Critical metric |
|---|---|---|
| `tool_single_read` | One fake turn calls `read`. | first tool start, idle |
| `tool_parallel_2` | Two independent fake tool calls, each delayed 250 ms. | proves overlap |
| `tool_parallel_16` | Sixteen fake tool calls, each delayed 250 ms. | fanout overhead |
| `tool_large_output_1mb` | Tool returns near max captured output. | truncation cost, RSS |
| `tool_error_repair` | Bad/case-mismatched tool name then repaired. | repair overhead |
| `tool_abort_inflight` | Abort while tools are running. | cancel latency, no stale parts |

### 5. Task/Subagent Workloads

| Scenario | Workload | Critical metric |
|---|---|---|
| `task_single_child` | Parent fake call invokes `task`; child fake responds. | child create + result latency |
| `task_chain_depth_4` | Child invokes child recursively to depth 4. | recursion overhead, stack/runtime safety |
| `task_fanout_4` | Parent emits 4 task calls in one step. | parallel child orchestration |
| `task_fanout_16` | Parent emits 16 task calls, matching Rust bound. | bound behavior, tail latency |
| `task_resume_existing` | Reuse `task_id` for an existing child session. | lookup and resume cost |
| `task_permission_block` | Child asks a permission/question visible through parent family. | UI-adoption metadata latency |

### 6. Agent Manager Multi-Session

| Scenario | Workload | Critical metric |
|---|---|---|
| `manager_2_sessions` | Two concurrent prompts. | cross-session overlap |
| `manager_8_sessions` | Eight concurrent prompts with staggered fake delays. | scheduler fairness |
| `manager_32_sessions` | Thirty-two sessions, light fake prompts. | throughput and RSS |
| `manager_busy_same_session` | Two prompts to one session. | BusyError latency and correctness |
| `manager_abort_parent_child` | Abort parent with active child tasks. | family cancellation |
| `manager_status_poll_10hz` | Poll `/session/status`, questions, permissions while prompts run. | route overhead under load |

### 7. Big Context and Long History

| Scenario | Workload | Critical metric |
|---|---|---|
| `context_1mb` | Seed history with 1 MiB prompt context, run fake turn. | assembly CPU, RSS |
| `context_8mb` | Seed 8 MiB of messages and tool output. | large-request boundary |
| `context_16mb_limit` | Hit configured request/body limit. | stable 413/error shape |
| `history_100_messages` | Assemble prompt from 100 prior messages. | per-turn assembly |
| `history_1000_messages` | Assemble prompt from 1000 prior messages. | algorithmic scaling |
| `history_tools_noop_case` | Long history with prior tool calls and no active tools. | `_noop` compatibility cost |
| `compaction_trigger` | History exceeds threshold and triggers compaction path. | compaction route cost |

### 8. Storage and SSE Durability

| Scenario | Workload | Critical metric |
|---|---|---|
| `write_before_publish` | Verify every SSE event references already-readable rows. | correctness plus route latency |
| `sse_reconnect_mid_turn` | Disconnect/reconnect while fake turn streams. | replay/readback cost |
| `event_log_10k` | Seed many events, then prompt and list. | event-table scaling |
| `db_cold_large` | Start with large `kilo.db`. | startup and first list latency |
| `db_wal_contention` | Many sessions write while routes read. | SQLite busy time |

### 9. Files, Search, and Process Tools

| Scenario | Workload | Critical metric |
|---|---|---|
| `path_list_10k` | Workspace tree with 10k files. | file listing CPU and RSS |
| `grep_small` | Search small repo. | child process + parse overhead |
| `grep_large` | Search large repo with many matches. | streaming parse, truncation |
| `bash_quick_100` | 100 short shell commands. | process spawn overhead |
| `bash_long_abort` | Long command then abort. | process cleanup |

### 10. Live Provider Smoke

Run this sparingly and never use it as the main speed comparison.

| Scenario | Workload | Critical metric |
|---|---|---|
| `openai_oauth_first_delta` | One real OpenAI OAuth prompt, tiny context. | integration latency |
| `openai_oauth_tool_turn` | Real model calls one safe local tool. | provider stream compatibility |
| `openai_oauth_abort` | Abort live stream after first delta. | cancellation behavior |

## Fairness Details

Use two classes of benchmarks:

1. **Deterministic fake-provider benchmarks**: primary Bun vs Rust comparison. They remove network/model variance and expose backend overhead.
2. **Live-provider smokes**: integration confidence only. They should not decide fastest runtime.

For every scenario:

- Run `warmups = 3`, `trials = 30` locally.
- In CI, run `warmups = 1`, `trials = 5` and gate only gross regressions.
- Randomize runtime order per scenario: Bun/Rust/Rust/Bun, not all Bun then all Rust.
- Kill the process after each trial for cold scenarios; reuse the process for warm scenarios.
- Capture stdout/stderr/log path on failure.
- Store seed metadata and git SHA with every result.

## Required Fake Provider Extensions

The Bun fake provider now covers text, delay, and tool calls. Extend both fake runtimes to cover:

```json
{
  "fakeDeltas": ["a", "b", "c"],
  "fakeIterations": [
    { "text": "step 1", "toolCalls": [] },
    { "text": "step 2", "toolCalls": [] }
  ],
  "fakeUsage": { "input": 100000, "output": 1000 },
  "fakeReasoning": "reasoning text",
  "fakeError": { "afterMs": 50, "name": "APIError", "message": "synthetic" },
  "fakeToolCalls": [
    { "tool": "task", "input": {}, "delayMs": 250 }
  ]
}
```

Do not overfit the benchmark to fake-only shapes. The fake stream should emit the same message-part and SSE events that a real provider stream emits.

## Gates

Use three levels:

### PR Smoke Gate

Must be stable on developer machines and CI:

- `cold_start_ready`
- `turn_text_small`
- `tool_parallel_2`
- `task_single_child`
- `manager_2_sessions`
- `history_100_messages`
- `abort_inflight`

Gate rule: Rust must pass correctness, and no metric may regress more than 2x from the current checked-in baseline unless explicitly blessed.

### Nightly Perf Gate

Runs broader matrix with 30 trials:

- all startup/idle
- all basic session management
- all agentic loop baseline
- `task_fanout_4`, `task_fanout_16`
- `manager_8_sessions`, `manager_32_sessions`
- `context_1mb`, `context_8mb`, `history_1000_messages`
- `grep_large`, `bash_quick_100`

Gate rule: fail if p95 latency or peak RSS regresses by 25% against the last blessed Rust baseline, or if Rust loses to Bun by more than 25% on a scenario marked rollout-critical.

### Release Candidate Gate

Adds long-run and leak checks:

- 1 hour idle SSE
- 500 sequential fake turns in one session
- 1000 sessions create/list/delete
- 32 concurrent sessions for 10 rounds
- task fanout 16 for 50 rounds
- repeated abort during tool calls

Gate rule: no unbounded RSS growth, no leaked child processes, no stuck busy sessions, no orphan child task sessions, no SSE events pointing to missing rows.

## Rollout-Critical Winners

These are the scenarios that should matter most for choosing Bun vs Rust:

| Area | Scenario | Expected winner | Why |
|---|---|---|---|
| Startup | `cold_start_ready` | Rust should win | Smaller purpose-built process should start faster than Bun runtime. |
| Idle | `idle_sse_1`, `idle_sse_16` | Rust should win | Async Rust server should idle near zero CPU with lower RSS. |
| Session CRUD | `session_list_1000`, `session_children_tree` | Rust should win | Direct SQL and typed route handlers should avoid JS object churn. |
| Agentic loop | `turn_text_stream_100`, `tool_parallel_16` | Mixed | Bun benefits from AI SDK stream machinery; Rust can win if parser and storage writes are lean. |
| Subagents | `task_fanout_16` | Rust must be competitive | Prior Rust fixes targeted runtime construction and fanout bounds here. |
| Big context | `history_1000_messages`, `context_8mb` | Rust should win | Fewer temporary strings and direct assembly should reduce CPU/RSS. |
| Abort | `tool_abort_inflight`, `manager_abort_parent_child` | Rust must be correct first | Speed is irrelevant if stale child tasks survive. |
| File/process tools | `grep_large`, `bash_quick_100` | Mixed | OS process spawn dominates; route/tool plumbing overhead decides. |

## Implementation Steps

1. Add an external process sampler usable from Windows first:
   - RSS/current working set
   - peak working set if available
   - CPU user/kernel deltas
   - thread count
   - handle count
   - child process count
2. Refactor `SidecarHandle::spawn` to accept explicit Bun and Rust binaries and to expose the child PID.
3. Add `BenchScenario` trait with `seed`, `run`, and `verify` phases.
4. Port current Bun fake-provider smoke tests into scenario implementations.
5. Add Rust fake-provider parity controls for the extended fake stream controls.
6. Add JSONL result writer plus summary command.
7. Add checked-in baseline files:
   - `packages/kilo-vscode-sidecar-rs/benchmarks/baselines/windows-bun.json`
   - `packages/kilo-vscode-sidecar-rs/benchmarks/baselines/windows-rust.json`
8. Add PR smoke command:
   - `cargo test -p kilo-oracle --test bench_compare bench_compare_smoke -- --nocapture`
9. Add nightly command:
   - `cargo run -p kilo-oracle --bin bench-compare -- --runtime both --scenario nightly --trials 30`
10. Publish a Markdown summary table from the JSONL so humans can see winners quickly.

## Output Summary Format

Example summary:

| Scenario | Metric | Bun p50 | Rust p50 | Ratio | Winner | Notes |
|---|---:|---:|---:|---:|---|---|
| `task_fanout_16` | `request_to_idle_ms` | 1850 | 1210 | 0.65 | Rust | Rust stays within semaphore bound. |
| `history_1000_messages` | `rss_peak_mb` | 720 | 410 | 0.57 | Rust | Direct assembly avoids repeated temporary buffers. |
| `turn_text_stream_100` | `first_delta_ms` | 75 | 94 | 1.25 | Bun | Bun AI SDK stream path still lower overhead. |

## Non-Goals

- Do not benchmark model intelligence or provider speed here.
- Do not compare live OpenAI calls as primary performance data.
- Do not gate on absolute timings from a single laptop.
- Do not use this suite to justify behavior drift. Correctness parity still comes from oracle traces.

## First Milestone

The first useful slice is:

1. External sampler.
2. Shared runner for Bun and Rust external sidecars.
3. Scenarios:
   - `cold_start_ready`
   - `turn_text_small`
   - `manager_2_sessions`
   - `task_single_child`
   - `task_fanout_4`
   - `history_100_messages`
4. JSONL plus summary table.

That gives immediate signal on startup, agentic loop overhead, subagent overhead, session isolation, and prompt assembly without waiting for the full matrix.
