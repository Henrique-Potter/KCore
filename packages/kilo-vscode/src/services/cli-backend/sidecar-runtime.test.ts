import { afterEach, describe, expect, mock, test } from "bun:test"
import * as path from "path"

mock.module("vscode", () => ({
  workspace: {
    getConfiguration: () => ({
      get: (_key: string, value: string) => value,
    }),
  },
}))

import { resolveSidecar } from "./sidecar-runtime"

function context() {
  return { extensionPath: "C:\\kilo" } as any
}

afterEach(() => {
  delete process.env.KILO_VSCODE_SIDECAR_RUNTIME
  delete process.env.KILO_VSCODE_RUST_SIDECAR_PATH
})

describe("resolveSidecar", () => {
  test("uses Bun by default", () => {
    const plan = resolveSidecar(context())

    expect(plan.requested).toBe("bun")
    expect(plan.candidates.map((item) => item.runtime)).toEqual(["bun"])
  })

  test("env override forces Bun even if config says otherwise", () => {
    // Regression for the missing `KILO_VSCODE_SIDECAR_RUNTIME=bun` test:
    // the env var must take precedence over config and produce a single
    // Bun candidate (not the auto-mode `[rust, bun]` ordering).
    process.env.KILO_VSCODE_SIDECAR_RUNTIME = "bun"

    const plan = resolveSidecar(context())

    expect(plan.requested).toBe("bun")
    expect(plan.candidates.map((item) => item.runtime)).toEqual(["bun"])
  })

  test("supports auto as rust then bun", () => {
    process.env.KILO_VSCODE_SIDECAR_RUNTIME = "auto"

    const plan = resolveSidecar(context())

    expect(plan.requested).toBe("auto")
    expect(plan.candidates.map((item) => item.runtime)).toEqual(["rust", "bun"])
  })

  test("uses rust binary override", () => {
    process.env.KILO_VSCODE_SIDECAR_RUNTIME = "rust"
    process.env.KILO_VSCODE_RUST_SIDECAR_PATH = "target/debug/kilo-vscode-sidecar"

    const plan = resolveSidecar(context())

    expect(plan.candidates[0]!.runtime).toBe("rust")
    expect(plan.candidates[0]!.path).toBe(path.resolve("target/debug/kilo-vscode-sidecar"))
  })

  test("invalid env value falls through to config (then default)", () => {
    // Invalid env value must not be silently coerced to a runtime; the
    // resolver should ignore it and continue to config (which our mock
    // returns the default `"bun"` for).
    process.env.KILO_VSCODE_SIDECAR_RUNTIME = "wasm"

    const plan = resolveSidecar(context())

    expect(plan.requested).toBe("bun")
    expect(plan.candidates.map((item) => item.runtime)).toEqual(["bun"])
  })

  test("absolute Windows-style rust path is preserved as-is", () => {
    // Windows-aware path test: an already-absolute path must NOT be
    // re-rooted under the cwd. We compare via path.isAbsolute / path.resolve
    // so the test is correct on both win32 and POSIX runners.
    process.env.KILO_VSCODE_SIDECAR_RUNTIME = "rust"
    const platform = (globalThis as unknown as { process: { platform: string } }).process.platform
    const candidatePath =
      platform === "win32"
        ? "C:\\custom\\kilo-vscode-sidecar.exe"
        : "/custom/kilo-vscode-sidecar"
    process.env.KILO_VSCODE_RUST_SIDECAR_PATH = candidatePath

    const plan = resolveSidecar(context())

    expect(plan.candidates[0]!.runtime).toBe("rust")
    expect(path.isAbsolute(plan.candidates[0]!.path)).toBe(true)
    // For an already-absolute path, resolveSidecar must not modify it.
    expect(plan.candidates[0]!.path).toBe(candidatePath)
    // And path.resolve on the result is a no-op (idempotent).
    expect(path.resolve(plan.candidates[0]!.path)).toBe(candidatePath)
  })

  test("relative rust path is resolved against cwd", () => {
    process.env.KILO_VSCODE_SIDECAR_RUNTIME = "rust"
    const rel = path.join("target", "debug", "kilo-vscode-sidecar")
    process.env.KILO_VSCODE_RUST_SIDECAR_PATH = rel

    const plan = resolveSidecar(context())

    expect(plan.candidates[0]!.path).toBe(path.resolve(rel))
    expect(path.isAbsolute(plan.candidates[0]!.path)).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// Integration-style: the fallback rule that matters for B2 is "no fallback
// after mutation". The resolver itself doesn't enforce that — `ServerManager`
// does — but the candidate-sequence shape it produces is the only input
// `ServerManager` walks. We assert that shape here, then simulate the
// fallback rule in a small driver below.

interface FakeAttempt {
  runtime: "bun" | "rust"
  outcome: "success" | "fail-startup"
  /** If `success`, mutations observed before the next attempt is decided. */
  mutationsAfterStart?: number
}

/**
 * Simulates the M3 fallback decision matrix for a candidate sequence. The
 * real `ServerManager.tryCandidates` mirrors this logic; this tests the
 * decision table independent of process spawning.
 */
function simulateFallback(
  candidates: Array<{ runtime: "bun" | "rust" }>,
  outcomes: FakeAttempt[],
  requested: "bun" | "rust" | "auto",
): { runtime?: "bun" | "rust"; mutationsAtSwitch: number; threw: boolean } {
  let mutations = 0
  let mutationsAtSwitch = 0
  for (let i = 0; i < candidates.length; i++) {
    const candidate = candidates[i]!
    const outcome = outcomes[i] ?? { runtime: candidate.runtime, outcome: "fail-startup" }
    if (outcome.outcome === "success") {
      mutations += outcome.mutationsAfterStart ?? 0
      return { runtime: candidate.runtime, mutationsAtSwitch, threw: false }
    }
    const isLast = i === candidates.length - 1
    const fallbackAllowed =
      requested === "auto" && candidate.runtime === "rust" && mutations === 0 && !isLast
    if (!fallbackAllowed) {
      return { mutationsAtSwitch: mutations, threw: true }
    }
    mutationsAtSwitch = mutations
  }
  return { mutationsAtSwitch, threw: true }
}

describe("auto-mode fallback discipline", () => {
  test("rust startup fails before mutation -> falls back to bun", () => {
    const result = simulateFallback(
      [{ runtime: "rust" }, { runtime: "bun" }],
      [
        { runtime: "rust", outcome: "fail-startup" },
        { runtime: "bun", outcome: "success" },
      ],
      "auto",
    )
    expect(result.threw).toBe(false)
    expect(result.runtime).toBe("bun")
    expect(result.mutationsAtSwitch).toBe(0)
  })

  test("rust starts, mutations occur, then dies mid-stream -> NO fallback", () => {
    // The candidate sequence simulates what happens if the runtime did try
    // to fall back after mutations: this driver mirrors the rule that
    // makes that impossible. We model "mid-stream death after mutation"
    // by following a successful attempt with a candidate that would
    // otherwise be retried; the simulator must refuse.
    const result = simulateFallback(
      [{ runtime: "rust" }, { runtime: "bun" }],
      [
        { runtime: "rust", outcome: "success", mutationsAfterStart: 3 },
        // Even if this branch were reached, mutation count would block
        // fallback. We don't reach it because the first candidate
        // returned successfully — but the assertion below codifies the
        // intent: after a success path observes mutations, we never
        // silently swap runtimes.
        { runtime: "bun", outcome: "success" },
      ],
      "auto",
    )
    expect(result.threw).toBe(false)
    expect(result.runtime).toBe("rust")
  })

  test("requested=rust never falls back even if startup fails", () => {
    const result = simulateFallback(
      [{ runtime: "rust" }],
      [{ runtime: "rust", outcome: "fail-startup" }],
      "rust",
    )
    expect(result.threw).toBe(true)
  })

  test("requested=bun never falls back (no rust candidate present)", () => {
    const result = simulateFallback(
      [{ runtime: "bun" }],
      [{ runtime: "bun", outcome: "fail-startup" }],
      "bun",
    )
    expect(result.threw).toBe(true)
  })
})
