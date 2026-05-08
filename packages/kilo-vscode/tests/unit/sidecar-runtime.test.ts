import { describe, it, expect, beforeEach, afterEach } from "bun:test"
import * as path from "node:path"
import {
  BUN_SIDECAR_PATH_ENV,
  DEFAULT_SIDECAR_ROLLOUT,
  DEFAULT_SIDECAR_RUNTIME,
  DEFAULT_STARTUP_TIMEOUT_MS,
  SIDECAR_KILL_SWITCH_ENV,
  SIDECAR_ROLLOUT_ENV,
  RUST_SIDECAR_PATH_ENV,
  SIDECAR_RUNTIME_ENV,
  normalizeRollout,
  normalizeRuntime,
  resolveSidecar,
} from "../../src/services/cli-backend/sidecar-runtime"

const ENV_KEYS = [SIDECAR_RUNTIME_ENV, SIDECAR_ROLLOUT_ENV, SIDECAR_KILL_SWITCH_ENV, BUN_SIDECAR_PATH_ENV, RUST_SIDECAR_PATH_ENV]

interface RestorableEnv {
  [k: string]: string | undefined
}

function snapshotEnv(): RestorableEnv {
  const out: RestorableEnv = {}
  for (const k of ENV_KEYS) out[k] = process.env[k]
  return out
}

function restoreEnv(snap: RestorableEnv): void {
  for (const k of ENV_KEYS) {
    if (snap[k] === undefined) delete process.env[k]
    else process.env[k] = snap[k]
  }
}

const fakeContext = { extensionPath: "/ext" } as unknown as Parameters<typeof resolveSidecar>[0]

function name(): string {
  return process.platform === "win32" ? "kilo-vscode-sidecar.exe" : "kilo-vscode-sidecar"
}

function bunName(): string {
  return process.platform === "win32" ? "kilo.exe" : "kilo"
}

function runtimes(plan: ReturnType<typeof resolveSidecar>): string[] {
  return plan.candidates.map((candidate) => candidate.runtime)
}

describe("resolveSidecar", () => {
  let env: RestorableEnv
  beforeEach(() => {
    env = snapshotEnv()
    for (const k of ENV_KEYS) delete process.env[k]
  })
  afterEach(() => {
    restoreEnv(env)
  })

  it("defaults to auto runtime", () => {
    const plan = resolveSidecar(fakeContext)

    expect(DEFAULT_SIDECAR_RUNTIME).toBe("auto")
    expect(DEFAULT_SIDECAR_ROLLOUT).toBe("preview")
    expect(plan.requested).toBe("auto")
    expect(plan.rollout).toBe("preview")
    expect(runtimes(plan)).toEqual(["rust", "bun"])
    expect(plan.candidates[0]!.runtime).toBe("rust")
    expect(plan.candidates[1]!.runtime).toBe("bun")
    expect(plan.candidates[0]!.startupTimeoutMs).toBe(DEFAULT_STARTUP_TIMEOUT_MS)
  })

  it("uses bundled paths when no override is set", () => {
    const plan = resolveSidecar(fakeContext)

    expect(plan.candidates[0]!.path).toBe(path.join("/ext", "bin", name()))
    expect(plan.candidates[1]!.path).toBe(path.join("/ext", "bin", bunName()))
  })

  it("returns only Bun when requested by config", () => {
    const plan = resolveSidecar(fakeContext, { runtime: "bun" })

    expect(plan.requested).toBe("bun")
    expect(runtimes(plan)).toEqual(["bun"])
  })

  it("returns only Rust when requested by config", () => {
    const plan = resolveSidecar(fakeContext, { runtime: "rust" })

    expect(plan.requested).toBe("rust")
    expect(runtimes(plan)).toEqual(["rust"])
  })

  it("prefers env runtime override over config", () => {
    process.env[SIDECAR_RUNTIME_ENV] = "bun"

    const plan = resolveSidecar(fakeContext, { runtime: "rust" })

    expect(plan.requested).toBe("bun")
    expect(runtimes(plan)).toEqual(["bun"])
  })

  it("ignores invalid runtime values", () => {
    process.env[SIDECAR_RUNTIME_ENV] = "unknown"

    const plan = resolveSidecar(fakeContext, { runtime: "rust" })

    expect(plan.requested).toBe("rust")
    expect(runtimes(plan)).toEqual(["rust"])
  })

  it("honors KILO_VSCODE_RUST_SIDECAR_PATH", () => {
    process.env[RUST_SIDECAR_PATH_ENV] = path.resolve("/custom/rust")

    const plan = resolveSidecar(fakeContext)

    expect(plan.candidates[0]!.path).toBe(path.resolve("/custom/rust"))
  })

  it("honors KILO_VSCODE_BUN_SIDECAR_PATH", () => {
    process.env[BUN_SIDECAR_PATH_ENV] = path.resolve("/custom/bun")

    const plan = resolveSidecar(fakeContext)

    expect(plan.candidates[1]!.path).toBe(path.resolve("/custom/bun"))
  })

  it("prevents Rust in auto when the env kill switch is active", () => {
    process.env[SIDECAR_KILL_SWITCH_ENV] = "1"

    const plan = resolveSidecar(fakeContext)

    expect(runtimes(plan)).toEqual(["bun"])
    expect(plan.blocked).toEqual([{ runtime: "rust", reason: `${SIDECAR_KILL_SWITCH_ENV} is active; Rust sidecar is disabled.` }])
  })

  it("falls back to Bun with a diagnostic when forced Rust is killed", () => {
    process.env[SIDECAR_RUNTIME_ENV] = "rust"
    process.env[SIDECAR_KILL_SWITCH_ENV] = "true"

    const plan = resolveSidecar(fakeContext)

    expect(plan.requested).toBe("rust")
    expect(runtimes(plan)).toEqual(["bun"])
    expect(plan.blocked[0]?.reason).toContain(SIDECAR_KILL_SWITCH_ENV)
  })

  it("prevents Rust when rollout is disabled by setting", () => {
    const plan = resolveSidecar(fakeContext, { rollout: "disabled" })

    expect(plan.rollout).toBe("disabled")
    expect(runtimes(plan)).toEqual(["bun"])
    expect(plan.blocked[0]?.reason).toContain("rollout is disabled")
  })

  it("uses env rollout override over setting", () => {
    process.env[SIDECAR_ROLLOUT_ENV] = "stable"

    const plan = resolveSidecar(fakeContext, { rollout: "disabled" })

    expect(plan.rollout).toBe("stable")
    expect(runtimes(plan)).toEqual(["rust", "bun"])
  })

  it("resolves a relative KILO_VSCODE_RUST_SIDECAR_PATH to an absolute path", () => {
    process.env[RUST_SIDECAR_PATH_ENV] = "relative/rust"

    const plan = resolveSidecar(fakeContext)

    expect(plan.candidates[0]!.path).toBe(path.resolve("relative/rust"))
    expect(path.isAbsolute(plan.candidates[0]!.path)).toBe(true)
  })

  it("normalizes runtime values", () => {
    expect(normalizeRuntime(" AUTO ")).toBe("auto")
    expect(normalizeRuntime("Rust")).toBe("rust")
    expect(normalizeRuntime("bun")).toBe("bun")
    expect(normalizeRuntime("nope")).toBeNull()
    expect(normalizeRollout(" disabled ")).toBe("disabled")
    expect(normalizeRollout("Stable")).toBe("stable")
    expect(normalizeRollout("nope")).toBeNull()
  })
})
