import { type ChildProcess } from "child_process"
import { spawn } from "../../util/process"
import * as crypto from "crypto"
import * as fs from "fs"
import * as vscode from "vscode"
import { t } from "./i18n"
import { isContractVersionCompatible, MAX_SUPPORTED_CONTRACT_VERSION, parseContractVersion, parseServerPort } from "./server-utils"
import { resolveSidecar, type SidecarCandidate, type SidecarKind, type SidecarRuntime } from "./sidecar-runtime"

export interface ServerInstance {
  port: number
  password: string
  process: ChildProcess
  /** Which sidecar runtime is actually running. */
  runtime: SidecarKind
}

export class ServerManager {
  private instance: ServerInstance | null = null
  private startupPromise: Promise<ServerInstance> | null = null

  /** Set to `true` after the Rust sidecar accepts a mutating SDK request. */
  private hasAttemptedMutation = false

  constructor(private readonly context: vscode.ExtensionContext) {}

  /**
   * Mark that the Rust sidecar accepted a mutating SDK request. Auto runtime
   * fallback to Bun is allowed only before this gate flips.
   */
  markMutationAttempted(): void {
    if (this.hasAttemptedMutation) {
      return
    }
    this.hasAttemptedMutation = true
    console.log("[Kilo New] ServerManager: Mutation observed")
  }

  /**
   * Whether mutation has been observed since the current sidecar was
   * started. Exposed for diagnostics and tests.
   */
  hasObservedMutation(): boolean {
    return this.hasAttemptedMutation
  }

  /**
   * Get or start the server instance
   */
  async getServer(): Promise<ServerInstance> {
    console.log("[Kilo New] ServerManager: 🔍 getServer called")
    if (this.instance) {
      console.log("[Kilo New] ServerManager: ♻️ Returning existing instance:", { port: this.instance.port })
      return this.instance
    }

    if (this.startupPromise) {
      console.log("[Kilo New] ServerManager: ⏳ Startup already in progress, waiting...")
      return this.startupPromise
    }

    console.log("[Kilo New] ServerManager: 🚀 Starting new server instance...")
    this.startupPromise = this.startServer()
    try {
      this.instance = await this.startupPromise
      console.log("[Kilo New] ServerManager: ✅ Server started successfully:", {
        port: this.instance.port,
        runtime: this.instance.runtime,
      })
      return this.instance
    } finally {
      this.startupPromise = null
    }
  }

  private async startServer(): Promise<ServerInstance> {
    const plan = resolveSidecar(this.context)
    const candidates = this.startupCandidates(plan.requested, plan.candidates)
      console.log("[Kilo New] ServerManager: 🧭 Sidecar plan:", {
        requested: plan.requested,
        rollout: plan.rollout,
        candidates: plan.candidates.map((c) => ({ runtime: c.runtime, path: c.path })),
        blocked: plan.blocked,
        startupCandidates: candidates.map((c) => c.runtime),
        mutationObserved: this.hasAttemptedMutation,
      })

    const failures: { runtime: SidecarKind; error: unknown }[] = []
    for (const candidate of candidates) {
      try {
        return await this.spawnCandidate(candidate)
      } catch (err) {
        failures.push({ runtime: candidate.runtime, error: err })
        console.warn("[Kilo New] ServerManager: Sidecar candidate failed:", {
          runtime: candidate.runtime,
          error: err instanceof Error ? err.message : String(err),
        })
      }
    }
    throw aggregateStartupError(failures)
  }

  private startupCandidates(requested: SidecarRuntime, candidates: SidecarCandidate[]): SidecarCandidate[] {
    if (requested !== "auto") return candidates
    if (this.hasAttemptedMutation) return candidates.slice(0, 1)
    return candidates
  }

  private async spawnCandidate(candidate: SidecarCandidate): Promise<ServerInstance> {
    const password = crypto.randomBytes(32).toString("hex")
    const cliPath = candidate.path
    console.log("[Kilo New] ServerManager: Sidecar path:", cliPath, "(runtime:", candidate.runtime + ")")
    console.log("[Kilo New] ServerManager: 🔐 Generated password (length):", password.length)

    // Verify the sidecar binary exists before spawning.
    if (!fs.existsSync(cliPath)) {
      throw new ServerStartupError(
        `${candidate.runtime} sidecar binary not found`,
        `Sidecar path: ${cliPath}\n${candidate.runtime} sidecar binary not found. Please ensure the sidecar is built and bundled with the extension.`,
      )
    }

    const stat = fs.statSync(cliPath)
    console.log("[Kilo New] ServerManager: Sidecar isFile:", stat.isFile())
    console.log("[Kilo New] ServerManager: Sidecar mode (octal):", (stat.mode & 0o777).toString(8))

    return new Promise((resolve, reject) => {
      console.log("[Kilo New] ServerManager: Spawning sidecar process:", cliPath, candidate.args)
      const claudeCompat = vscode.workspace.getConfiguration("kilo-code.new").get<boolean>("claudeCodeCompat", false)
      // Pin cwd so the sidecar doesn't inherit the extension host's cwd ("/" under F5 debug)
      const spawnCwd = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? process.env.HOME ?? require("os").homedir()
      const serverProcess = spawn(cliPath, candidate.args, {
        cwd: spawnCwd,
        env: {
          ...process.env,
          KILO_SERVER_PASSWORD: password,
          KILO_CLIENT: "vscode",
          KILO_ENABLE_QUESTION_TOOL: "true",
          KILOCODE_FEATURE: "vscode-extension",
          KILO_TELEMETRY_LEVEL: vscode.env.isTelemetryEnabled ? "all" : "off",
          KILO_APP_NAME: "kilo-code",
          KILO_EDITOR_NAME: vscode.env.appName,
          KILO_PLATFORM: "vscode",
          KILO_MACHINE_ID: vscode.env.machineId,
          KILO_APP_VERSION: this.context.extension.packageJSON.version,
          KILO_VSCODE_VERSION: vscode.version,
          KILOCODE_EDITOR_NAME: `${vscode.env.appName} ${vscode.version}`,
          ...(!claudeCompat && { KILO_DISABLE_CLAUDE_CODE: "true" }),
        },
        stdio: ["ignore", "pipe", "pipe"],
        detached: true,
      })
      console.log("[Kilo New] ServerManager: 📦 Process spawned with PID:", serverProcess.pid)

      let resolved = false
      const stderrLines: string[] = []

      serverProcess.stdout?.on("data", (data: Buffer) => {
        const output = data.toString()
        console.log("[Kilo New] ServerManager: Sidecar stdout:", output)

        const port = parseServerPort(output)
        if (port !== null && !resolved) {
          // Migration plan invariant (Wire-protocol versioning beyond v1):
          // refuse a sidecar whose contractVersion is newer than this
          // extension build's compiled-in maximum. Older sidecars (Bun and
          // pre-versioned Rust) report null and are accepted by default —
          // Bun is the oracle.
          const contractVersion = parseContractVersion(output)
          if (!isContractVersionCompatible(contractVersion)) {
            resolved = true
            console.error(
              "[Kilo New] ServerManager: ❌ Sidecar contract version too new:",
              contractVersion,
              "(extension max:",
              MAX_SUPPORTED_CONTRACT_VERSION,
              ")",
            )
            try {
              serverProcess.kill()
            } catch {
              // best-effort
            }
            reject(
              new Error(
                `Sidecar reports contract version ${contractVersion}, but this extension supports up to ${MAX_SUPPORTED_CONTRACT_VERSION}. Update the Kilo Code extension.`,
              ),
            )
            return
          }
          resolved = true
          console.log("[Kilo New] ServerManager: 🎯 Port detected:", port, "contract:", contractVersion ?? "unversioned")
          resolve({ port, password, process: serverProcess, runtime: candidate.runtime })
        }
      })

      serverProcess.stderr?.on("data", (data: Buffer) => {
        const errorOutput = data.toString()
        console.error("[Kilo New] ServerManager: Sidecar stderr:", errorOutput)
        stderrLines.push(errorOutput)
      })

      serverProcess.on("error", (error) => {
        console.error("[Kilo New] ServerManager: ❌ Process error:", error)
        if (!resolved) {
          reject(error)
        }
      })

      serverProcess.on("exit", (code) => {
        console.log("[Kilo New] ServerManager: 🛑 Process exited with code:", code)
        if (this.instance?.process === serverProcess) {
          this.instance = null
        }
        if (!resolved) {
          const { userMessage, userDetails } = toErrorMessage(
            t("server.processExited", { code: code ?? "null" }),
            stderrLines,
            cliPath,
          )
          reject(new ServerStartupError(userMessage, userDetails))
        }
      })

      const timeoutMs = candidate.startupTimeoutMs
      const timeoutSeconds = Math.ceil(timeoutMs / 1000)
      setTimeout(() => {
        if (!resolved) {
          console.error(
            `[Kilo New] ServerManager: ⏰ Server startup timeout (${timeoutSeconds}s, runtime=${candidate.runtime})`,
          )
          ServerManager.killProcess(serverProcess)
          const { userMessage, userDetails } = toErrorMessage(
            t("server.startupTimeout", { seconds: timeoutSeconds }),
            stderrLines,
            cliPath,
          )
          reject(new ServerStartupError(userMessage, userDetails))
        }
      }, timeoutMs)
    })
  }

  /**
   * Kill a process and its entire process group.
   * On Unix, we send the signal to -pid (negative) to reach the whole group,
   * mirroring the desktop app's ProcessGroup::leader() + start_kill() pattern.
   * On Windows, process.kill() on the child handle is sufficient.
   */
  private static killProcess(proc: ChildProcess, signal: NodeJS.Signals = "SIGTERM"): void {
    if (proc.pid === undefined) {
      return
    }
    try {
      if (process.platform !== "win32") {
        // Negative PID targets the entire process group
        process.kill(-proc.pid, signal)
      } else {
        proc.kill(signal)
      }
    } catch {
      // Process already gone — ignore
    }
  }

  dispose(): void {
    if (!this.instance) {
      return
    }
    const proc = this.instance.process
    this.instance = null

    console.log("[Kilo New] ServerManager: 🔴 Disposing — sending SIGTERM to process group, PID:", proc.pid)
    ServerManager.killProcess(proc, "SIGTERM")

    // SIGKILL fallback after 5s: mirrors the desktop app going straight to
    // start_kill(). Ensures the process tree dies even if SIGTERM is ignored
    // or Instance.disposeAll() hangs past the serve.ts shutdown timeout.
    const timer = setTimeout(() => {
      if (proc.exitCode === null) {
        console.warn("[Kilo New] ServerManager: ⚠️ Process did not exit after SIGTERM, sending SIGKILL")
        ServerManager.killProcess(proc, "SIGKILL")
      }
    }, 5000)
    // unref so this timer doesn't prevent the extension host from exiting
    timer.unref()
    proc.on("exit", () => clearTimeout(timer))
  }
}

export class ServerStartupError extends Error {
  readonly userMessage: string
  readonly userDetails: string
  /** Retained for compatibility with tests that inspect startup errors. */
  readonly candidates: ReadonlyArray<{ runtime: SidecarKind; error: unknown }>
  constructor(
    userMessage: string,
    userDetails: string,
    candidates: ReadonlyArray<{ runtime: SidecarKind; error: unknown }> = [],
  ) {
    super(userDetails)
    this.name = "ServerStartupError"
    this.userMessage = userMessage
    this.userDetails = userDetails
    this.candidates = candidates
  }
}

function aggregateStartupError(failures: ReadonlyArray<{ runtime: SidecarKind; error: unknown }>): ServerStartupError {
  const last = failures.at(-1)
  const message = last ? errorMessage(last.error) : "Sidecar startup failed"
  const lines = ["Sidecar startup failed."]
  for (const failure of failures) {
    lines.push(`\n[${failure.runtime}]`)
    lines.push(errorDetails(failure.error))
  }
  return new ServerStartupError(message, lines.join("\n").trim(), failures)
}

function errorMessage(err: unknown): string {
  if (err instanceof ServerStartupError) return err.userMessage
  if (err instanceof Error) return err.message
  return String(err)
}

function errorDetails(err: unknown): string {
  if (err instanceof ServerStartupError) return err.userDetails
  if (err instanceof Error) return err.stack ?? err.message
  return String(err)
}

function stripAnsi(str: string): string {
  return str.replace(/\x1b\[[0-9;]*m/g, "")
}

export function toErrorMessage(
  error: string,
  stderrLines: string[],
  cliPath?: string,
): {
  userMessage: string
  userDetails: string
  error: string
} {
  let lines = stderrLines.flatMap((line) => line.split("\n"))

  const errorLine = lines.map(stripAnsi).find((line) => /Error:\s+/.test(line))
  const userMessage = errorLine
    ? errorLine.match(/Error:\s+(.+)/)![1].trim()
    : stripAnsi([...lines].reverse().find((line) => line.trim() !== "") ?? error).trim()

  lines = [error, ...lines]
  if (cliPath && cliPath.trim() !== "") {
    lines = [`Sidecar path: ${cliPath}`, ...lines]
  }

  const detailsText = lines.map(stripAnsi).join("\n").trim()

  return {
    userMessage,
    userDetails: detailsText,
    error,
  }
}
