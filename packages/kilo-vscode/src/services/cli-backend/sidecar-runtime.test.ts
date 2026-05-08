import { afterEach, describe, expect, test } from "bun:test"
import * as path from "path"

import {
  BUN_SIDECAR_PATH_ENV,
  DEFAULT_STARTUP_TIMEOUT_MS,
  RUST_SIDECAR_PATH_ENV,
  SIDECAR_RUNTIME_ENV,
  resolveSidecar,
} from "./sidecar-runtime"

function context() {
  return { extensionPath: "C:\\kilo" } as any
}

afterEach(() => {
  delete process.env[SIDECAR_RUNTIME_ENV]
  delete process.env[BUN_SIDECAR_PATH_ENV]
  delete process.env[RUST_SIDECAR_PATH_ENV]
})

describe("resolveSidecar", () => {
  test("auto returns Rust then Bun sidecars", () => {
    const plan = resolveSidecar(context())

    expect(plan.requested).toBe("auto")
    expect(plan.candidates).toHaveLength(2)
    expect(plan.candidates[0]!.runtime).toBe("rust")
    expect(plan.candidates[1]!.runtime).toBe("bun")
    expect(plan.candidates[0]!.args).toEqual(["serve", "--port", "0"])
    expect(plan.candidates[0]!.startupTimeoutMs).toBe(DEFAULT_STARTUP_TIMEOUT_MS)
  })

  test("can force Bun", () => {
    process.env[SIDECAR_RUNTIME_ENV] = "bun"

    const plan = resolveSidecar(context())

    expect(plan.requested).toBe("bun")
    expect(plan.candidates.map((candidate) => candidate.runtime)).toEqual(["bun"])
  })

  test("uses rust binary override", () => {
    process.env[RUST_SIDECAR_PATH_ENV] = "target/debug/kilo-vscode-sidecar"

    const plan = resolveSidecar(context())

    expect(plan.candidates[0]!.path).toBe(path.resolve("target/debug/kilo-vscode-sidecar"))
  })

  test("absolute Windows-style rust path is preserved as-is", () => {
    const platform = (globalThis as unknown as { process: { platform: string } }).process.platform
    const candidate =
      platform === "win32"
        ? "C:\\custom\\kilo-vscode-sidecar.exe"
        : "/custom/kilo-vscode-sidecar"
    process.env[RUST_SIDECAR_PATH_ENV] = candidate

    const plan = resolveSidecar(context())

    expect(path.isAbsolute(plan.candidates[0]!.path)).toBe(true)
    expect(plan.candidates[0]!.path).toBe(candidate)
    expect(path.resolve(plan.candidates[0]!.path)).toBe(candidate)
  })
})
