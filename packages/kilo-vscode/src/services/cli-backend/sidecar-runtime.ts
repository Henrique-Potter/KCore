import * as path from "path"
import * as vscode from "vscode"

export type SidecarRuntime = "bun" | "rust" | "auto"
export type SidecarKind = "bun" | "rust"

export interface SidecarCandidate {
  runtime: SidecarKind
  path: string
  args: string[]
}

export interface SidecarPlan {
  requested: SidecarRuntime
  candidates: SidecarCandidate[]
}

const runtimes = new Set<SidecarRuntime>(["bun", "rust", "auto"])

export function resolveSidecar(context: vscode.ExtensionContext): SidecarPlan {
  const requested = resolveRuntime()
  const bun = bunCandidate(context)
  const rust = rustCandidate(context)
  const candidates = (() => {
    if (requested === "bun") return [bun]
    if (requested === "rust") return [rust]
    return [rust, bun]
  })()

  return { requested, candidates }
}

function resolveRuntime(): SidecarRuntime {
  const env = process.env.KILO_VSCODE_SIDECAR_RUNTIME?.toLowerCase()
  if (isRuntime(env)) return env

  const cfg = vscode.workspace.getConfiguration("kilo-code.new").get<string>("sidecarRuntime", "bun").toLowerCase()
  if (isRuntime(cfg)) return cfg

  return "bun"
}

function isRuntime(value: string | undefined): value is SidecarRuntime {
  return value !== undefined && runtimes.has(value as SidecarRuntime)
}

function bunCandidate(context: vscode.ExtensionContext): SidecarCandidate {
  const name = process.platform === "win32" ? "kilo.exe" : "kilo"
  return {
    runtime: "bun",
    path: path.join(context.extensionPath, "bin", name),
    args: ["serve", "--port", "0"],
  }
}

function rustCandidate(context: vscode.ExtensionContext): SidecarCandidate {
  const name = process.platform === "win32" ? "kilo-vscode-sidecar.exe" : "kilo-vscode-sidecar"
  const override = process.env.KILO_VSCODE_RUST_SIDECAR_PATH
  const bin = override ? resolvePath(override) : path.join(context.extensionPath, "bin", name)
  return {
    runtime: "rust",
    path: bin,
    args: ["serve", "--port", "0"],
  }
}

function resolvePath(value: string): string {
  if (path.isAbsolute(value)) return value
  return path.resolve(value)
}
