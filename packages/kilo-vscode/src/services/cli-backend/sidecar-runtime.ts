import * as path from "path"
import * as vscode from "vscode"

export type SidecarKind = "bun" | "rust"
export type SidecarRuntime = SidecarKind | "auto"
export type SidecarRollout = "disabled" | "internal" | "preview" | "stable"

export interface SidecarBlock {
  runtime: SidecarKind
  reason: string
}

export interface SidecarCandidate {
  runtime: SidecarKind
  path: string
  args: string[]
  startupTimeoutMs: number
}

export interface SidecarPlan {
  requested: SidecarRuntime
  rollout: SidecarRollout
  candidates: SidecarCandidate[]
  blocked: SidecarBlock[]
}

export const DEFAULT_STARTUP_TIMEOUT_MS = 30_000
export const DEFAULT_SIDECAR_RUNTIME: SidecarRuntime = "auto"
export const DEFAULT_SIDECAR_ROLLOUT: SidecarRollout = "preview"
export const SIDECAR_RUNTIME_ENV = "KILO_VSCODE_SIDECAR_RUNTIME"
export const SIDECAR_ROLLOUT_ENV = "KILO_VSCODE_SIDECAR_ROLLOUT"
export const SIDECAR_KILL_SWITCH_ENV = "KILO_VSCODE_SIDECAR_KILL_SWITCH"
export const BUN_SIDECAR_PATH_ENV = "KILO_VSCODE_BUN_SIDECAR_PATH"
export const RUST_SIDECAR_PATH_ENV = "KILO_VSCODE_RUST_SIDECAR_PATH"

export function resolveSidecar(
  context: vscode.ExtensionContext,
  opts?: { runtime?: string; rollout?: string },
): SidecarPlan {
  const requested = resolveRequestedRuntime(opts?.runtime)
  const rollout = resolveRollout(opts?.rollout)
  const block = rustBlock(rollout)
  const bun = bunCandidate(context)
  if (requested === "bun") return { requested, rollout, candidates: [bun], blocked: [] }
  if (block) return { requested, rollout, candidates: [bun], blocked: [{ runtime: "rust", reason: block }] }
  const rust = rustCandidate(context)
  if (requested === "rust") return { requested, rollout, candidates: [rust], blocked: [] }
  return { requested, rollout, candidates: [rust, bun], blocked: [] }
}

export function resolveRequestedRuntime(configured?: string): SidecarRuntime {
  return normalizeRuntime(process.env[SIDECAR_RUNTIME_ENV]) ?? normalizeRuntime(configured) ?? configuredRuntime()
}

export function normalizeRuntime(value: unknown): SidecarRuntime | null {
  if (typeof value !== "string") return null
  const lower = value.trim().toLowerCase()
  if (lower === "bun" || lower === "rust" || lower === "auto") return lower
  return null
}

export function resolveRollout(configured?: string): SidecarRollout {
  return normalizeRollout(process.env[SIDECAR_ROLLOUT_ENV]) ?? normalizeRollout(configured) ?? configuredRollout()
}

export function normalizeRollout(value: unknown): SidecarRollout | null {
  if (typeof value !== "string") return null
  const lower = value.trim().toLowerCase()
  if (lower === "disabled" || lower === "internal" || lower === "preview" || lower === "stable") return lower
  return null
}

function rustBlock(rollout: SidecarRollout): string | null {
  if (truthy(process.env[SIDECAR_KILL_SWITCH_ENV])) return `${SIDECAR_KILL_SWITCH_ENV} is active; Rust sidecar is disabled.`
  if (rollout === "disabled") return "sidecar rollout is disabled; Rust sidecar is disabled."
  return null
}

function configuredRuntime(): SidecarRuntime {
  const cfg = vscode.workspace.getConfiguration("kilo-code.new").get<string>("sidecarRuntime", DEFAULT_SIDECAR_RUNTIME)
  return normalizeRuntime(cfg) ?? DEFAULT_SIDECAR_RUNTIME
}

function configuredRollout(): SidecarRollout {
  const cfg = vscode.workspace.getConfiguration("kilo-code.new").get<string>("sidecarRollout", DEFAULT_SIDECAR_ROLLOUT)
  return normalizeRollout(cfg) ?? DEFAULT_SIDECAR_ROLLOUT
}

function truthy(value: unknown): boolean {
  if (typeof value !== "string") return false
  const lower = value.trim().toLowerCase()
  return lower === "1" || lower === "true" || lower === "yes" || lower === "on"
}

function bunCandidate(context: vscode.ExtensionContext): SidecarCandidate {
  const name = process.platform === "win32" ? "kilo.exe" : "kilo"
  const override = process.env[BUN_SIDECAR_PATH_ENV]
  const bin = override ? resolvePath(override) : path.join(context.extensionPath, "bin", name)
  return {
    runtime: "bun",
    path: bin,
    args: ["serve", "--port", "0"],
    startupTimeoutMs: DEFAULT_STARTUP_TIMEOUT_MS,
  }
}

function rustCandidate(context: vscode.ExtensionContext): SidecarCandidate {
  const name = process.platform === "win32" ? "kilo-vscode-sidecar.exe" : "kilo-vscode-sidecar"
  const override = process.env[RUST_SIDECAR_PATH_ENV]
  const bin = override ? resolvePath(override) : path.join(context.extensionPath, "bin", name)
  return {
    runtime: "rust",
    path: bin,
    args: ["serve", "--port", "0"],
    startupTimeoutMs: DEFAULT_STARTUP_TIMEOUT_MS,
  }
}

function resolvePath(value: string): string {
  if (path.isAbsolute(value)) return value
  return path.resolve(value)
}
