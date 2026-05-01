import { type ChildProcess } from "child_process"
import { spawn } from "../../util/process"
import * as crypto from "crypto"
import * as fs from "fs"
import * as vscode from "vscode"
import { t } from "./i18n"
import { parseServerPort } from "./server-utils"
import { resolveSidecar, type SidecarCandidate, type SidecarKind, type SidecarPlan } from "./sidecar-runtime"

function describeError(error: unknown): string {
  if (error instanceof Error) {
    return error.message
  }
  return String(error)
}

export interface ServerInstance {
  port: number
  password: string
  process: ChildProcess
  /** Which sidecar runtime is actually running (after any auto-fallback). */
  runtime: SidecarKind
}

const STARTUP_TIMEOUT_SECONDS = 30

export class ServerManager {
  private instance: ServerInstance | null = null
  private startupPromise: Promise<ServerInstance> | null = null

  /**
   * Set to `true` the first time the host has issued a mutating SDK request
   * against the running sidecar. While this flag is `false`, an `auto`-mode
   * Rust startup failure is allowed to fall back to Bun. Once `true`,
   * fallback is forbidden — falling back after Rust may have observed
   * mutations would silently lose state.
   *
   * Wired by `KiloConnectionService`: the SDK fetch wrapper calls
   * {@link markMutationAttempted} on every POST/PUT/PATCH/DELETE.
   */
  private hasAttemptedMutation = false

  constructor(private readonly context: vscode.ExtensionContext) {}

  /**
   * Mark that a mutating SDK request has been observed against this
   * sidecar. After this is called, automatic fallback from Rust to Bun is
   * disabled and a Rust startup or runtime failure is surfaced.
   *
   * Called from the SDK fetch wrapper in `KiloConnectionService` on every
   * POST/PUT/PATCH/DELETE — that is the contract path the SDK takes for
   * mutating routes. Idempotent and cheap.
   */
  markMutationAttempted(): void {
    if (this.hasAttemptedMutation) {
      return
    }
    this.hasAttemptedMutation = true
    console.log("[Kilo New] ServerManager: ✏️ Mutation observed — auto-fallback disabled for the rest of this session")
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
    console.log("[Kilo New] ServerManager: 🧭 Sidecar plan:", {
      requested: plan.requested,
      candidates: plan.candidates.map((c) => ({ runtime: c.runtime, path: c.path })),
    })

    return await this.tryCandidates(plan)
  }

  /**
   * Walk the resolver's candidate list, honoring fallback rules:
   *
   * - `requested === "bun"` or `"rust"`: only that single candidate is
   *   tried; failure is surfaced.
   * - `requested === "auto"`: candidates are tried in order. Fallback to
   *   the next candidate is only allowed when (a) the current candidate is
   *   the Rust sidecar and (b) `hasAttemptedMutation` is `false`. Bun
   *   failure is always surfaced.
   *
   * The fallback gate is conservative: once `hasAttemptedMutation` flips,
   * we never silently swap to a different process again.
   */
  private async tryCandidates(plan: SidecarPlan): Promise<ServerInstance> {
    const errors: Array<{ candidate: SidecarCandidate; error: unknown }> = []
    for (let i = 0; i < plan.candidates.length; i++) {
      const candidate = plan.candidates[i]!
      const isLast = i === plan.candidates.length - 1
      try {
        return await this.spawnCandidate(candidate)
      } catch (error) {
        errors.push({ candidate, error })
        const fallbackAllowed =
          plan.requested === "auto" &&
          candidate.runtime === "rust" &&
          !this.hasAttemptedMutation &&
          !isLast
        if (!fallbackAllowed) {
          throw error
        }
        console.warn(
          `[Kilo New] ServerManager: ⤵️ Rust sidecar failed to start (${describeError(error)}); falling back to next candidate`,
        )
      }
    }
    // Should be unreachable: the loop always either returns or throws.
    throw new ServerStartupError(
      "No sidecar candidate succeeded",
      errors.map((e) => `${e.candidate.runtime}@${e.candidate.path}: ${describeError(e.error)}`).join("\n"),
    )
  }

  private async spawnCandidate(candidate: SidecarCandidate): Promise<ServerInstance> {
    const password = crypto.randomBytes(32).toString("hex")
    const cliPath = candidate.path
    console.log("[Kilo New] ServerManager: 📍 CLI path:", cliPath, "(runtime:", candidate.runtime + ")")
    console.log("[Kilo New] ServerManager: 🔐 Generated password (length):", password.length)

    // Verify the CLI binary exists
    if (!fs.existsSync(cliPath)) {
      throw new Error(
        `CLI binary not found at expected path: ${cliPath}. Please ensure the CLI is built and bundled with the extension.`,
      )
    }

    const stat = fs.statSync(cliPath)
    console.log("[Kilo New] ServerManager: 📄 CLI isFile:", stat.isFile())
    console.log("[Kilo New] ServerManager: 📄 CLI mode (octal):", (stat.mode & 0o777).toString(8))

    return new Promise((resolve, reject) => {
      console.log("[Kilo New] ServerManager: 🎬 Spawning CLI process:", cliPath, candidate.args)
      const claudeCompat = vscode.workspace.getConfiguration("kilo-code.new").get<boolean>("claudeCodeCompat", false)
      // Pin cwd so the CLI doesn't inherit the extension host's cwd ("/" under F5 debug)
      const spawnCwd = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath ?? process.env.HOME ?? require("os").homedir()
      const serverProcess = spawn(cliPath, candidate.args, {
        cwd: spawnCwd,
        env: {
          ...process.env,
          // Force mimalloc (the allocator Bun ships with) to return freed pages
          // to the OS immediately instead of retaining them in its arenas.
          // Without this, Bun.spawn's piped stdio accumulates ~2 MB of native
          // RSS per call on Windows, causing the Agent Manager (which polls git
          // once per second per worktree) to reach multi-GB RSS in minutes.
          // See oven-sh/bun#18265 and Jarred's workaround note in #21560.
          MIMALLOC_PURGE_DELAY: "0",
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
        console.log("[Kilo New] ServerManager: 📥 CLI Server stdout:", output)

        const port = parseServerPort(output)
        if (port !== null && !resolved) {
          resolved = true
          console.log("[Kilo New] ServerManager: 🎯 Port detected:", port)
          resolve({ port, password, process: serverProcess, runtime: candidate.runtime })
        }
      })

      serverProcess.stderr?.on("data", (data: Buffer) => {
        const errorOutput = data.toString()
        console.error("[Kilo New] ServerManager: ⚠️ CLI Server stderr:", errorOutput)
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

      setTimeout(() => {
        if (!resolved) {
          console.error(`[Kilo New] ServerManager: ⏰ Server startup timeout (${STARTUP_TIMEOUT_SECONDS}s)`)
          ServerManager.killProcess(serverProcess)
          const { userMessage, userDetails } = toErrorMessage(
            t("server.startupTimeout", { seconds: STARTUP_TIMEOUT_SECONDS }),
            stderrLines,
            cliPath,
          )
          reject(new ServerStartupError(userMessage, userDetails))
        }
      }, STARTUP_TIMEOUT_SECONDS * 1000)
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
  constructor(userMessage: string, userDetails: string) {
    super(userDetails)
    this.name = "ServerStartupError"
    this.userMessage = userMessage
    this.userDetails = userDetails
  }
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
    lines = [`CLI path: ${cliPath}`, ...lines]
  }

  const detailsText = lines.map(stripAnsi).join("\n").trim()

  return {
    userMessage,
    userDetails: detailsText,
    error,
  }
}
