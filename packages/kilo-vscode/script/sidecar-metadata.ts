import { readFileSync, statSync } from "node:fs"
import { basename } from "node:path"
import { createHash } from "node:crypto"

export type Kind = "bun-cli" | "rust-sidecar"
export const SIDECAR_CONTRACT_VERSION = 1
export const COMPATIBLE_ORACLE_VERSION = "bun-cli-v1"

export interface SidecarInput {
  kind: Kind
  file: string
  target: string | null
  version: string | null
}

export interface SidecarArtifact {
  kind: Kind
  file: string
  target: string | null
  sha256: string
  size: number
  version: string | null
  contractVersion: number
  compatibleOracleVersion: string
}

export interface SidecarManifest {
  schema: 1
  artifacts: SidecarArtifact[]
}

export function digest(file: string): string {
  return createHash("sha256").update(readFileSync(file)).digest("hex")
}

export function artifact(input: SidecarInput): SidecarArtifact {
  return {
    kind: input.kind,
    file: basename(input.file),
    target: input.target,
    sha256: digest(input.file),
    size: statSync(input.file).size,
    version: input.version,
    contractVersion: SIDECAR_CONTRACT_VERSION,
    compatibleOracleVersion: COMPATIBLE_ORACLE_VERSION,
  }
}

export async function manifest(file: string, artifacts: SidecarArtifact[]): Promise<void> {
  const data: SidecarManifest = {
    schema: 1,
    artifacts: artifacts.sort((a, b) => `${a.kind}:${a.file}`.localeCompare(`${b.kind}:${b.file}`)),
  }
  await Bun.write(file, JSON.stringify(data, null, 2) + "\n")
}
