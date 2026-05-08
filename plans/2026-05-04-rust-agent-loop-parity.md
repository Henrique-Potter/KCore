# Rust Agent Loop — Bun Parity Plan (3 phases)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use `- [ ]` checkboxes.

**Goal:** Close the 6 remaining parity gaps on the Rust sidecar's OpenAI OAuth agent loop so it matches Bun's observable behavior: dynamic registry / MCP tools, structured-output-as-tool, per-iteration step-start/step-finish parts, richer token usage, doom-loop gating, hard-rule permission layering.

**Architecture:** All changes land in `packages/kilo-vscode-sidecar-rs/`. Phase 1 (telemetry) extends `ChatUsage` and emits per-iteration step parts. Phase 2 adds the doom-loop guard (reuses existing `ask_permission`) and structured-output tool plumbing. Phase 3 wires MCP tools into the per-turn catalog and adds the hard-ruleset veto layer plus full glob matching. Per the M10 scope memo, only the OpenAI OAuth path (`agent::openai_stream`) is targeted; the fake provider remains the test oracle.

**Tech Stack:** Rust 2021, tokio, serde_json, axum, rusqlite. Tests: `cargo test -p kilo-server` and `cargo test -p kilo-oracle`.

**Bun reference citations** (frozen for plan-time use):
- Agent loop: `packages/opencode/src/session/prompt.ts:1373-1697`
- Step parts: `packages/opencode/src/session/processor.ts:402-473`
- Step shapes: `packages/opencode/src/session/message-v2.ts:252-280`
- Token usage: `packages/opencode/src/session/session.ts:308-385`
- Structured output tool: `packages/opencode/src/session/prompt.ts:1566-1573, 1969-1995`
- Doom-loop: `packages/opencode/src/session/processor.ts:27, 357-380`
- MCP tools: `packages/opencode/src/mcp/index.ts:658-691`
- Hard rules: `packages/opencode/src/permission/index.ts:217-271`; `packages/opencode/src/kilocode/session/prompt.ts:60-72`

---

## Phase 1 — Telemetry foundation

### Task 1.1: Extend `ChatUsage` with cache + reasoning fields

**Files:**
- Modify: `crates/kilo-provider/src/lib.rs:82-87` (struct), `crates/kilo-provider/src/lib.rs:1171-1194` (`parse_usage`)
- Modify: `crates/kilo-server/src/agent/shape.rs:48-56` (`tokens_value`)

- [ ] Step 1: Extend `ChatUsage` struct

```rust
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChatUsage {
    pub input: u64,
    pub output: u64,
    pub total: u64,
    pub reasoning: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}
```

- [ ] Step 2: Extend `parse_usage` to read the new fields. OpenAI Responses API exposes them under `usage.input_tokens_details.cached_tokens` and `usage.output_tokens_details.reasoning_tokens` (Bun parses cache write across multiple provider keys; we only need OpenAI Responses for now).

```rust
fn parse_usage(body: &Value) -> Option<ChatUsage> {
    let usage = body.get("usage").or_else(|| body.pointer("/response/usage"))?;
    let input = usage.get("input_tokens").or_else(|| usage.get("prompt_tokens"))
        .and_then(Value::as_u64).unwrap_or(0);
    let output = usage.get("output_tokens").or_else(|| usage.get("completion_tokens"))
        .and_then(Value::as_u64).unwrap_or(0);
    let total = usage.get("total_tokens").and_then(Value::as_u64).unwrap_or(input + output);
    let reasoning = usage.pointer("/output_tokens_details/reasoning_tokens")
        .and_then(Value::as_u64).unwrap_or(0);
    let cache_read = usage.pointer("/input_tokens_details/cached_tokens")
        .or_else(|| usage.get("cached_tokens"))
        .and_then(Value::as_u64).unwrap_or(0);
    let cache_write = usage.pointer("/input_tokens_details/cache_creation_input_tokens")
        .and_then(Value::as_u64).unwrap_or(0);
    Some(ChatUsage { input, output, total, reasoning, cache_read, cache_write })
}
```

- [ ] Step 3: Update `tokens_value` in `agent/shape.rs:48-56`:

```rust
pub(crate) fn tokens_value(usage: &ChatUsage) -> Value {
    json!({
        "input": usage.input,
        "output": usage.output,
        "reasoning": usage.reasoning,
        "total": usage.total,
        "cache": { "read": usage.cache_read, "write": usage.cache_write }
    })
}
```

- [ ] Step 4: Add `usage_accumulate` helper in `agent/shape.rs` (used in 1.2 to sum per-iteration usage into a turn total):

```rust
pub(crate) fn usage_accumulate(acc: &mut ChatUsage, next: &ChatUsage) {
    acc.input += next.input;
    acc.output += next.output;
    acc.total += next.total;
    acc.reasoning += next.reasoning;
    acc.cache_read += next.cache_read;
    acc.cache_write += next.cache_write;
}
```

- [ ] Step 5: Add a unit test in `crates/kilo-provider/src/lib.rs` (in the existing `#[cfg(test)] mod tests` block, or create one) asserting `parse_usage` reads cached/reasoning correctly:

```rust
#[test]
fn parse_usage_reads_cached_and_reasoning() {
    let body = serde_json::json!({
        "usage": {
            "input_tokens": 100, "output_tokens": 50, "total_tokens": 150,
            "input_tokens_details": { "cached_tokens": 30 },
            "output_tokens_details": { "reasoning_tokens": 20 }
        }
    });
    let u = parse_usage(&body).unwrap();
    assert_eq!(u.cache_read, 30);
    assert_eq!(u.reasoning, 20);
}
```

- [ ] Step 6: `cargo test -p kilo-provider parse_usage_reads_cached_and_reasoning` — expect PASS.

### Task 1.2: Per-iteration step parts + accumulated usage

**Files:**
- Modify: `crates/kilo-server/src/agent/openai_stream.rs:48-477` (loop body)
- Modify: `crates/kilo-server/src/agent/shape.rs` (add `step_start_part`)

- [ ] Step 1: Add `step_start_part` factory in `agent/shape.rs`:

```rust
pub(crate) fn step_start_part(sid: &str, mid: &str, pid: &str, iteration: usize) -> Value {
    json!({
        "id": format!("{pid}_step_start_{iteration}"),
        "type": "step-start",
        "messageID": mid,
        "sessionID": sid
    })
}
```

- [ ] Step 2: In `openai_stream.rs` change `last_usage` from `Option<ChatUsage>` to a tuple `(ChatUsage /* total */, Option<ChatUsage> /* last_iter */)` or split into two bindings. Initialize `let mut total_usage = ChatUsage::default(); let mut last_iter_usage: Option<ChatUsage> = None;` near `openai_stream.rs:87`.

- [ ] Step 3: At the top of each loop iteration (after the cancel check at `openai_stream.rs:96`), append a `step-start` part to the assistant message and persist it:

```rust
let step_start = crate::agent::shape::step_start_part(id, &mid, &pid, iteration);
if let Ok(record) = state.store.append_message_record(
    id,
    MessageAppendInput { info: start.result.info.clone(), parts: vec![step_start.clone()] },
) {
    publish_events(&state, dir.clone(), project.clone(), record.events);
    tool_parts.push(step_start);
}
```

- [ ] Step 4: After `provider_out` is unwrapped (around `openai_stream.rs:322-325`), append the per-iteration `step-finish`:

```rust
if let Some(usage) = provider_out.usage.clone() {
    crate::agent::shape::usage_accumulate(&mut total_usage, &usage);
    last_iter_usage = Some(usage);
}
let step_finish = crate::agent::shape::step_finish_part_usage(
    id, &mid, &json!({ "id": format!("{pid}_iter_{iteration}") }),
    last_iter_usage.as_ref(), provider_out.finish.as_deref(),
);
if let Ok(record) = state.store.append_message_record(
    id,
    MessageAppendInput { info: start.result.info.clone(), parts: vec![step_finish.clone()] },
) {
    publish_events(&state, dir.clone(), project.clone(), record.events);
    tool_parts.push(step_finish);
}
```

- [ ] Step 5: Replace the final `assistant_parts_with` call (`openai_stream.rs:452-460, 614`) so it no longer appends a single `step-finish` (the per-iteration ones already cover it). The function becomes:

```rust
fn assistant_parts_with(
    _mid: &str, pid: &str, text: &str, tool_parts: &[Value],
    _usage: Option<&ChatUsage>, _finish: Option<&str>, _sid: &str,
) -> Vec<Value> {
    let mut parts = vec![json!({ "id": pid, "type": "text", "text": text })];
    parts.extend(tool_parts.iter().cloned());
    parts
}
```

- [ ] Step 6: Replace every existing reference to `last_usage` in this file (cancel finalize, error finalize, the final completed-info call) with `Some(&total_usage)` so the assistant `info.tokens` reflects the turn total, not just the last iteration.

- [ ] Step 7: Add an oracle test in `crates/kilo-oracle/tests/rust_harness.rs` (or a new fixture under `fixtures/sse/`) verifying that a 2-iteration fake-provider run produces `step-start` + `step-finish` for both iterations and the final `info.tokens.total` equals the sum.

- [ ] Step 8: `cargo test -p kilo-server --lib agent::` and `cargo test -p kilo-oracle` — expect PASS.

---

## Phase 2 — Agentic features

### Task 2.1: Doom-loop gating

**Files:**
- Modify: `crates/kilo-server/src/agent/openai_stream.rs` (after iter_tools sort, around line 264-269)
- Modify: `crates/kilo-server/src/agent/permission.rs` (no signature change; just used)
- Modify: `crates/kilo-server/src/agent/tools/common.rs` (add `tool_permission` exception so `"doom_loop"` is its own permission name)

- [ ] Step 1: Add a `DOOM_LOOP_THRESHOLD` constant in `agent/openai_stream.rs` near `OPENAI_OAUTH_MAX_ITERATIONS`:

```rust
pub(crate) const DOOM_LOOP_THRESHOLD: usize = 3;
```

- [ ] Step 2: Add a helper `last_n_tool_signatures` in `agent/openai_stream.rs`:

```rust
fn doom_loop_signature(part: &Value) -> Option<(String, String)> {
    if part.get("type").and_then(Value::as_str) != Some("tool") { return None; }
    let tool = part.get("tool").and_then(Value::as_str)?.to_string();
    let input = part.pointer("/state/input").map(|v| v.to_string()).unwrap_or_default();
    let status = part.pointer("/state/status").and_then(Value::as_str).unwrap_or("");
    if status == "pending" || status == "running" { return None; }
    Some((tool, input))
}

fn detect_doom_loop(parts: &[Value]) -> Option<(String, Value)> {
    if parts.len() < DOOM_LOOP_THRESHOLD { return None; }
    let tail = &parts[parts.len() - DOOM_LOOP_THRESHOLD..];
    let sigs: Vec<_> = tail.iter().filter_map(doom_loop_signature).collect();
    if sigs.len() < DOOM_LOOP_THRESHOLD { return None; }
    if sigs.windows(2).all(|w| w[0] == w[1]) {
        let last = tail.last().unwrap();
        let tool = last.get("tool").and_then(Value::as_str).unwrap_or("").to_string();
        let input = last.pointer("/state/input").cloned().unwrap_or(Value::Null);
        return Some((tool, input));
    }
    None
}
```

- [ ] Step 3: After `iter_tools_only` is sorted (around `openai_stream.rs:265`), check for doom-loop and call `ask_permission` with the synthetic permission name `"doom_loop"`:

```rust
if !denied_this_iter {
    if let Some((tool, input)) = detect_doom_loop(&tool_parts) {
        // Use a fresh fake call_id and idx that won't collide with real tool parts.
        let synth_idx = tool_parts.len();
        let synth_call = format!("doom_loop_{iteration}");
        let permission_input = json!({ "tool": tool, "input": input });
        if let Err(_) = crate::agent::permission::ask_doom_loop(
            &state, id, &mid, &pid, synth_idx, &tool, &synth_call, &permission_input,
        ).await {
            break; // user said no; terminate the turn
        }
    }
}
```

- [ ] Step 4: Add `ask_doom_loop` in `agent/permission.rs` (mirrors `ask_permission` but with fixed permission name and no tool→permission collapse):

```rust
pub(crate) async fn ask_doom_loop(
    state: &Arc<AppState>, sid: &str, mid: &str, pid: &str, idx: usize,
    tool: &str, call: &str, input: &Value,
) -> Result<(), String> {
    let rules = permission_ruleset(state, sid);
    let rule = evaluate_permission("doom_loop", tool, &rules);
    if rule.action == "deny" { return Err(format!("Doom-loop denied for {tool}")); }
    if rule.action == "allow" { return Ok(()); }
    ask_permission_once(state, sid, mid, pid, idx, "doom_loop", tool, call, input, vec![tool.to_string()]).await
}
```

- [ ] Step 5: Make `permission_ruleset`, `ask_permission_once`, `evaluate_permission` `pub(crate)` so `ask_doom_loop` can use them (they're currently file-private).

- [ ] Step 6: Add a unit test in `crates/kilo-server/src/tests/` exercising `detect_doom_loop` with three identical tool parts.

- [ ] Step 7: `cargo test -p kilo-server doom_loop` — expect PASS.

### Task 2.2: Structured output as a tool

**Files:**
- Modify: `crates/kilo-protocol/src/lib.rs` (add `format` to `PromptInput` if missing)
- Modify: `crates/kilo-server/src/agent/openai_stream.rs` (synthesize tool, force tool_choice, handle outcome)
- Modify: `crates/kilo-server/src/agent/parts.rs` (export `real_tools` modification or add helper)

- [ ] Step 1: Verify/add `format: Option<Value>` on `PromptInput` in `crates/kilo-protocol/src/lib.rs`. The shape is `{"type": "json_schema", "schema": {...}}`. If already present (Bun parity slice may have added it), skip.

- [ ] Step 2: Add `STRUCTURED_OUTPUT_TOOL_NAME` constant in `agent/openai_stream.rs`:

```rust
pub(crate) const STRUCTURED_OUTPUT_TOOL_NAME: &str = "StructuredOutput";
```

- [ ] Step 3: Add `structured_output_tool` factory in `agent/parts.rs` (alongside `real_tools`):

```rust
pub(crate) fn structured_output_tool(format: &Value) -> Option<ChatTool> {
    let schema = format.get("schema")?.clone();
    Some(ChatTool {
        name: "StructuredOutput".to_string(),
        description: "Provide the final response in the requested structured format.".to_string(),
        parameters: schema,
    })
}
```

- [ ] Step 4: Modify `real_tools` in `agent/parts.rs:125-134` to merge in the structured-output tool when `input.format.type == "json_schema"`:

```rust
pub(crate) fn real_tools(state: &Arc<AppState>, input: &PromptInput) -> Vec<ChatTool> {
    let mut tools = base_real_tools(state);
    if input.format.as_ref().and_then(|f| f.get("type")).and_then(Value::as_str) == Some("json_schema") {
        if let Some(t) = structured_output_tool(input.format.as_ref().unwrap()) {
            tools.push(t);
        }
    }
    tools
}
```

(Rename the current `real_tools` body to `base_real_tools` to keep the diff small.)

- [ ] Step 5: In `openai_stream.rs` add a `structured_capture: Option<Value>` accumulator and intercept `StructuredOutput` tool calls inside the existing `ToolCall` branch (`openai_stream.rs:163-232`) — instead of dispatching, store the input and break on the next iteration boundary. Pseudocode:

```rust
StreamEvent::ToolCall(call) if call.name == STRUCTURED_OUTPUT_TOOL_NAME => {
    structured_capture = Some(call.input.clone());
    // Don't spawn — let the loop see this and terminate.
}
```

- [ ] Step 6: After the iteration's drain (around `openai_stream.rs:264-269`), if `structured_capture.is_some()`, mark `last_finish = Some("stop".to_string())`, write the structured value into `info.structured` on the final assistant message, and break the loop.

- [ ] Step 7: When the loop ends without `structured_capture` set BUT `format.type == json_schema`, append a `StructuredOutputError` envelope (mirrors Bun `MessageV2.StructuredOutputError`).

- [ ] Step 8: Tweak `prompt_instructions_with_root` (`openai_stream.rs:720-...`) — it already conditionally appends `STRUCTURED_OUTPUT_SYSTEM_PROMPT`; verify the gate keys on `input.format.type == "json_schema"` and add it if missing.

- [ ] Step 9: Pass `tool_choice: "required"` to `stream_openai_oauth` when format is set. Add a `tool_choice: Option<&str>` parameter to that function (or thread via an existing struct) — minimum invasive change is a new parameter on `stream_openai_oauth` or a wrapper.

- [ ] Step 10: Add a fixture under `fixtures/sse/` with a fake-provider stream that emits a `StructuredOutput` tool call. Add an oracle test asserting `assistant.info.structured` matches the input.

- [ ] Step 11: `cargo test -p kilo-server --lib structured` and `cargo test -p kilo-oracle structured_output` — expect PASS.

---

## Phase 3 — MCP wiring + hard rules

### Task 3.1: MCP tools in agent loop

**Files:**
- Read: `crates/kilo-server/src/routes/mcp.rs` (lifecycle/tool listing reference)
- Modify: `crates/kilo-server/src/agent/parts.rs` (`real_tools`, `real_tool_part`)
- Modify: `crates/kilo-server/src/state.rs` (already has `mcp` map; add accessor)
- Modify: `crates/kilo-server/src/agent/tools/common.rs` (`tool_permission` for MCP namespace)

- [ ] Step 1: Add `state.mcp_tools_for_session(sid)` accessor in `state.rs` returning `Vec<(String /* namespaced name */, Value /* schema */, String /* client */, String /* original name */)>`. Pull from the existing `state.mcp` map. Use `format!("{client}_{tool}")` namespace per Bun's `mcp/index.ts:685`.

- [ ] Step 2: In `agent/parts.rs:125-134`, extend `real_tools` to merge MCP tool descriptors:

```rust
pub(crate) fn real_tools(state: &Arc<AppState>, input: &PromptInput) -> Vec<ChatTool> {
    let mut tools = base_real_tools(state);
    // Structured output (from 2.2)
    if let Some(t) = structured_output_tool_for(input) { tools.push(t); }
    // MCP tools
    for (name, schema, _client, _orig) in state.mcp_tools_for_session(/* sid */) {
        tools.push(ChatTool { name, description: String::new(), parameters: schema });
    }
    tools
}
```

(`real_tools` already gets `state` and `input`; the session id is reachable through `input` or via a new param. Plumb the sid down if needed.)

- [ ] Step 3: In `agent/parts.rs:393-450` `real_tool_part`, after the `repair_tool_name` step, check whether the (possibly invalid) name corresponds to a known MCP tool (`client_tool` namespace match). If yes, dispatch through a new `mcp_tool_part` helper instead of returning `unknown tool`.

```rust
if let Some(repair) = mcp_lookup(state, &call.name) {
    return mcp_tool_part(state, &repair, &call, mid, pid, idx, start_time).await;
}
```

- [ ] Step 4: Implement `mcp_tool_part` in a new file `crates/kilo-server/src/agent/mcp_dispatch.rs`:
  - Permission-gate via `ask_permission(state, sid, mid, pid, idx, "mcp", &call.id, &call.input)` (reuse existing flow; "mcp" becomes a new permission name).
  - Look up the `kilo_mcp::Client` from `state.mcp`.
  - Call `client.call_tool(orig_name, call.input).await` (mirror `routes/mcp.rs:mcp_call_tool` invocation).
  - Convert the result to a `tool` part using the existing `tool_part_response` helpers.

- [ ] Step 5: Add `tool_permission` mapping in `agent/tools/common.rs:61-66` so MCP-namespaced tools resolve to the `"mcp"` permission key:

```rust
pub(crate) fn tool_permission(tool: &str) -> &'static str {
    match tool {
        "write" | "edit" | "apply_patch" => "edit",
        "bash" => "bash",
        "task" => "task",
        name if name.contains('_') && /* matches a known mcp client prefix */ => "mcp",
        _ => tool,
    }
}
```

(Use an actual lookup against state, not just the underscore heuristic — the tool name pattern is `clientID_toolName` after sanitize. The cleanest implementation is a `is_mcp_tool(state, tool)` helper.)

- [ ] Step 6: Add a unit test in `crates/kilo-server/src/tests/` that registers a fake MCP client, runs `real_tools`, and asserts the namespaced tool appears.

- [ ] Step 7: Add an oracle test that:
  - Spawns a stdio MCP fake (or stubs the registry directly).
  - Runs a turn whose fake-provider trace calls the namespaced MCP tool.
  - Asserts the call reaches the MCP dispatch path and the tool result is persisted.

- [ ] Step 8: `cargo test -p kilo-server mcp` and `cargo test -p kilo-oracle mcp` — expect PASS.

### Task 3.2: Hard-rule permission layer

**Files:**
- Modify: `crates/kilo-server/src/agent/permission.rs:144-171` (evaluate_permission, wildcard_match)
- Modify: `crates/kilo-server/src/agent/permission.rs:20-46` (ask_permission)
- Modify: `crates/kilo-server/src/state.rs` (expose `agent_hard_rules(agent_name)`)
- Modify: `crates/kilo-store/src/lib.rs` (persist "always" decisions)

- [ ] Step 1: Replace the current `wildcard_match` (`permission.rs:163-171`) with a full glob matcher supporting `*` anywhere and `?`. Lift the algorithm from any standard rust glob impl or implement via two-pointer scanning. Add unit tests for `a*b`, `*foo`, `foo*`, `a?b`, `**`.

```rust
fn wildcard_match(value: &str, pattern: &str) -> bool {
    let mut vi = 0; let mut pi = 0;
    let mut star_v = None; let mut star_p = None;
    let v: Vec<char> = value.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    while vi < v.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == v[vi]) { vi += 1; pi += 1; }
        else if pi < p.len() && p[pi] == '*' { star_p = Some(pi); star_v = Some(vi); pi += 1; }
        else if let (Some(sp), Some(sv)) = (star_p, star_v) { pi = sp + 1; star_v = Some(sv + 1); vi = sv + 1; }
        else { return false; }
    }
    while pi < p.len() && p[pi] == '*' { pi += 1; }
    pi == p.len()
}
```

- [ ] Step 2: Extend `evaluate_permission` (`permission.rs:144-161`) to take a `hard_rules: &[PermissionRule]` slice and short-circuit if any hard rule matches with `deny`:

```rust
pub(crate) fn evaluate_permission_layered(
    permission: &str, pattern: &str,
    soft: &[PermissionRule], hard: &[PermissionRule],
) -> PermissionRule {
    if let Some(hard_hit) = hard.iter().rev().find(|r|
        wildcard_match(permission, &r.permission) && wildcard_match(pattern, &r.pattern) && r.action == "deny"
    ) {
        return hard_hit.clone();
    }
    evaluate_permission(permission, pattern, soft)
}
```

- [ ] Step 3: Modify `ask_permission` (`permission.rs:20-46`) to use `evaluate_permission_layered` and to NOT promote a hard-deny match into the user-prompt path. The `Always` reply must NOT add a rule that would override a hard-deny — guard `ask_permission_once`'s `Always` branch (`permission.rs:82-90`) to skip persisting if any hard rule denies.

- [ ] Step 4: Add `state.agent_hard_rules(agent_name)` returning `Vec<PermissionRule>`. Source: read agent config from `state.store.agents()` (or wherever agents come from); for `"ask"` and `"plan"` agents, return the configured `permission` rules as the hard layer (mirrors `kilocode/session/prompt.ts:60-72`).

- [ ] Step 5: Plumb the agent name through to `ask_permission` (currently it's not threaded). The session has an `agent` field (from `PromptInput`); reach it through `state.store.session(sid)` and call `state.agent_hard_rules(...)`.

- [ ] Step 6: Persist `Always` decisions to disk in `kilo-store` so they survive process restart (Bun's `PermissionTable`). Add `state.store.upsert_permission(rule)` and load these into `permission_ruleset` alongside in-memory `state.approvals`.

- [ ] Step 7: Add unit tests covering: (a) hard-deny beats user-always; (b) wildcard `bash:rm *` blocks `bash:rm -rf /`; (c) persisted always survives a fresh `state.approvals`.

- [ ] Step 8: `cargo test -p kilo-server permission` — expect PASS.

---

## Final verification

- [ ] `cargo fmt --all` from `packages/kilo-vscode-sidecar-rs/`
- [ ] `cargo check --workspace` — clean
- [ ] `cargo test --workspace` — all pass
- [ ] Update `agent/memory bank/Project Fact.md` to note parity gaps closed (don't claim full parity if any test was skipped — be honest about what landed).

## Out of scope (tracked, not implemented here)

- Full Bun `Permission.fromConfig` JSON-config layering (mid-migration)
- ConfigProtection ask-downgrade (specialized branch)
- `compactionAttempts` / context compaction (separate workstream)
- Provider-specific cache_write parsing for `anthropic`/`vertex`/`bedrock`/`venice` (M10 is OpenAI-only)
- Plugin `tool.definition` decoration (Kilo doesn't ship plugins on Rust side yet)
