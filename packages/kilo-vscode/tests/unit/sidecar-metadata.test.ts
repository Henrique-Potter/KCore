import { afterEach, describe, expect, test } from "bun:test"
import { mkdirSync, rmSync } from "node:fs"
import { join } from "node:path"
import { tmpdir } from "node:os"
import { COMPATIBLE_ORACLE_VERSION, SIDECAR_CONTRACT_VERSION, artifact, manifest } from "../../script/sidecar-metadata"

const dir = join(tmpdir(), "kilo-sidecar-metadata-test")

afterEach(() => {
  rmSync(dir, { recursive: true, force: true })
})

describe("sidecar metadata", () => {
  test("describes a staged sidecar artifact", async () => {
    mkdirSync(dir, { recursive: true })
    const bin = join(dir, "kilo")
    await Bun.write(bin, "hello")

    expect(artifact({ kind: "bun-cli", file: bin, target: "linux-x64", version: "abc" })).toEqual({
      kind: "bun-cli",
      file: "kilo",
      target: "linux-x64",
      sha256: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
      size: 5,
      version: "abc",
      contractVersion: SIDECAR_CONTRACT_VERSION,
      compatibleOracleVersion: COMPATIBLE_ORACLE_VERSION,
    })
  })

  test("writes a deterministic sorted manifest", async () => {
    mkdirSync(dir, { recursive: true })
    const file = join(dir, "sidecars.json")

    await manifest(file, [
      { kind: "rust-sidecar", file: "kilo-vscode-sidecar", target: "linux-x64", sha256: "b", size: 2, version: "rust", contractVersion: 1, compatibleOracleVersion: "bun-cli-v1" },
      { kind: "bun-cli", file: "kilo", target: "linux-x64", sha256: "a", size: 1, version: "bun", contractVersion: 1, compatibleOracleVersion: "bun-cli-v1" },
    ])

    expect(await Bun.file(file).json()).toEqual({
      schema: 1,
      artifacts: [
        { kind: "bun-cli", file: "kilo", target: "linux-x64", sha256: "a", size: 1, version: "bun", contractVersion: 1, compatibleOracleVersion: "bun-cli-v1" },
        { kind: "rust-sidecar", file: "kilo-vscode-sidecar", target: "linux-x64", sha256: "b", size: 2, version: "rust", contractVersion: 1, compatibleOracleVersion: "bun-cli-v1" },
      ],
    })
  })
})
