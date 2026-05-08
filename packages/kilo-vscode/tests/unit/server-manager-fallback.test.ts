import { describe, it, expect, beforeEach, afterEach } from "bun:test"
import * as path from "node:path"
import { ServerManager, ServerStartupError } from "../../src/services/cli-backend/server-manager"
import {
  BUN_SIDECAR_PATH_ENV,
  SIDECAR_KILL_SWITCH_ENV,
  RUST_SIDECAR_PATH_ENV,
  SIDECAR_RUNTIME_ENV,
} from "../../src/services/cli-backend/sidecar-runtime"

const ENV_KEYS = [SIDECAR_RUNTIME_ENV, SIDECAR_KILL_SWITCH_ENV, BUN_SIDECAR_PATH_ENV, RUST_SIDECAR_PATH_ENV]

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

const fakeContext = {
  extensionPath: "/no-such-ext-path-for-tests",
  extension: { packageJSON: { version: "0.0.0-test" } },
} as unknown as ConstructorParameters<typeof ServerManager>[0]

describe("ServerManager Rust sidecar startup", () => {
  let env: RestorableEnv
  beforeEach(() => {
    env = snapshotEnv()
    process.env[RUST_SIDECAR_PATH_ENV] = path.resolve("/this/path/does/not/exist/rust")
    process.env[BUN_SIDECAR_PATH_ENV] = path.resolve("/this/path/does/not/exist/bun")
  })
  afterEach(() => {
    restoreEnv(env)
  })

  it("auto aggregates Rust then Bun startup failures before mutation", async () => {
    process.env[SIDECAR_RUNTIME_ENV] = "auto"

    const mgr = new ServerManager(fakeContext)
    let caught: unknown
    try {
      await mgr.getServer()
    } catch (err) {
      caught = err
    }

    expect(caught).toBeInstanceOf(Error)
    expect(String(caught instanceof Error ? caught.message : caught)).toContain("bun")
    if (caught instanceof ServerStartupError) {
      expect(caught.candidates.map((candidate) => candidate.runtime)).toEqual(["rust", "bun"])
      expect(caught.userDetails).toContain("[rust]")
      expect(caught.userDetails).toContain("[bun]")
    }
    mgr.dispose()
  })

  it("does not auto-fallback after a Rust mutation was observed", async () => {
    process.env[SIDECAR_RUNTIME_ENV] = "auto"

    const mgr = new ServerManager(fakeContext)
    mgr.markMutationAttempted()
    let caught: unknown
    try {
      await mgr.getServer()
    } catch (err) {
      caught = err
    }

    expect(caught).toBeInstanceOf(Error)
    if (caught instanceof ServerStartupError) {
      expect(caught.candidates.map((candidate) => candidate.runtime)).toEqual(["rust"])
      expect(caught.userDetails).not.toContain("[bun]")
    }
    mgr.dispose()
  })

  it("honors forced Bun runtime without trying Rust", async () => {
    process.env[SIDECAR_RUNTIME_ENV] = "bun"

    const mgr = new ServerManager(fakeContext)
    let caught: unknown
    try {
      await mgr.getServer()
    } catch (err) {
      caught = err
    }

    expect(caught).toBeInstanceOf(Error)
    if (caught instanceof ServerStartupError) {
      expect(caught.candidates.map((candidate) => candidate.runtime)).toEqual(["bun"])
      expect(caught.userDetails).not.toContain("[rust]")
    }
    mgr.dispose()
  })

  it("kill switch suppresses Rust and keeps Bun rollback before mutation", async () => {
    process.env[SIDECAR_RUNTIME_ENV] = "auto"
    process.env[SIDECAR_KILL_SWITCH_ENV] = "1"

    const mgr = new ServerManager(fakeContext)
    let caught: unknown
    try {
      await mgr.getServer()
    } catch (err) {
      caught = err
    }

    expect(caught).toBeInstanceOf(Error)
    if (caught instanceof ServerStartupError) {
      expect(caught.candidates.map((candidate) => candidate.runtime)).toEqual(["bun"])
      expect(caught.userDetails).not.toContain("[rust]")
    }
    mgr.dispose()
  })
})
