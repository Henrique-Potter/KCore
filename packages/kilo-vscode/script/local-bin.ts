#!/usr/bin/env bun
import { $ } from "bun"
import { join, relative } from "node:path"
import { chmodSync, copyFileSync, existsSync, mkdirSync, rmSync, statSync } from "node:fs"
import { artifact, manifest, type Kind, type SidecarArtifact } from "./sidecar-metadata"

const force = process.argv.includes("--force")

/**
 * Ensures the VS Code extension has both preview sidecar binaries under
 * `packages/kilo-vscode/bin`.
 */

const root = join(import.meta.dir, "..")
const packages = join(root, "..")
const cli = join(packages, "opencode")
const rust = join(packages, "kilo-vscode-sidecar-rs")
const out = join(root, "bin")
const cliName = process.platform === "win32" ? "kilo.exe" : "kilo"
const rustName = process.platform === "win32" ? "kilo-vscode-sidecar.exe" : "kilo-vscode-sidecar"
const cliTarget = join(out, cliName)
const rustTarget = join(out, rustName)
const cliVersion = join(out, ".cli-version")
const rustVersion = join(out, ".rust-sidecar-version")
const legacyVersion = join(out, ".rust-version")
const sidecars = join(out, "sidecars.json")

function log(msg: string) {
  console.log(`[local-bin] ${msg}`)
}

async function hash(): Promise<string | null> {
  try {
    const result = await $`git log -1 --format=%H -- .`.cwd(rust).quiet()
    return result.text().trim() || null
  } catch {
    return null
  }
}

async function cliHash(): Promise<string | null> {
  try {
    const result = await $`git log -1 --format=%H -- .`.cwd(cli).quiet()
    return result.text().trim() || null
  } catch {
    return null
  }
}

async function cliDirty(): Promise<boolean> {
  try {
    const result = await $`git status --porcelain -- .`.cwd(cli).quiet()
    return result.text().trim().length > 0
  } catch {
    return false
  }
}

async function cliStale(): Promise<boolean> {
  if (await cliDirty()) return true
  const rev = await cliHash()
  if (!rev) return false
  try {
    const stored = (await Bun.file(cliVersion).text()).trim()
    return stored !== rev
  } catch {
    return true
  }
}

async function dirty(): Promise<boolean> {
  try {
    const result = await $`git status --porcelain -- .`.cwd(rust).quiet()
    return result.text().trim().length > 0
  } catch {
    return false
  }
}

async function stale(): Promise<boolean> {
  if (await dirty()) return true
  const rev = await hash()
  if (!rev) return false
  try {
    const stored = (await Bun.file(rustVersion).text()).trim()
    return stored !== rev
  } catch {
    return true
  }
}

function built(): string {
  return join(rust, "target", "release", rustName)
}

function builtCli(): string {
  const dir = join(cli, "dist", "@kilocode", `cli-${bunTarget()}`, "bin")
  const bin = join(dir, cliName)
  if (existsSync(bin)) return bin
  return join(dir, "kilo")
}

function bunTarget(): string {
  const os = process.platform === "win32" ? "windows" : process.platform
  return `${os}-${process.arch}`
}

function target(): string {
  if (process.platform === "win32") return `win32-${process.arch}`
  return `${process.platform}-${process.arch}`
}

async function ensure(): Promise<string> {
  const bin = built()
  if (existsSync(bin) && !force && !(await stale())) return bin

  const pkg = Bun.file(join(rust, "Cargo.toml"))
  if (!(await pkg.exists())) {
    throw new Error(`Expected Rust sidecar package at ${rust}, but it does not exist.`)
  }

  log("Building Rust VS Code sidecar...")
  await $`cargo build --release -p kilo-vscode-sidecar`.cwd(rust)

  if (!existsSync(bin)) {
    throw new Error(`Rust sidecar build completed but no binary was found at ${bin}.`)
  }
  return bin
}

async function ensureCli(): Promise<string> {
  const bin = builtCli()
  if (existsSync(bin) && !force && !(await cliStale())) return bin

  const pkg = Bun.file(join(cli, "package.json"))
  if (!(await pkg.exists())) {
    throw new Error(`Expected Bun CLI package at ${cli}, but it does not exist.`)
  }

  log("Building Bun CLI sidecar...")
  await $`bun run script/build.ts --single --skip-install --skip-embed-web-ui`.cwd(cli)

  if (!existsSync(bin)) {
    throw new Error(`Bun CLI build completed but no binary was found at ${bin}.`)
  }
  return bin
}

function cleanup(): void {
  if (existsSync(legacyVersion)) {
    rmSync(legacyVersion)
  }
}

async function stage(source: string, target: string, version: string, hash: () => Promise<string | null>, label: string): Promise<void> {
  copyFileSync(source, target)
  chmodSync(target, 0o755)

  const rev = await hash()
  if (rev) await Bun.write(version, rev + "\n")

  log(`Copied ${label} from ${relative(packages, source)} -> ${relative(root, target)}`)
}

async function describe(kind: Kind, file: string, version: string): Promise<SidecarArtifact> {
  const rev = existsSync(version) ? (await Bun.file(version).text()).trim() || null : null
  return artifact({ kind, file, target: target(), version: rev })
}

async function writeMetadata(): Promise<void> {
  const items = []
  if (existsSync(cliTarget)) items.push(await describe("bun-cli", cliTarget, cliVersion))
  if (existsSync(rustTarget)) items.push(await describe("rust-sidecar", rustTarget, rustVersion))
  await manifest(sidecars, items)
  log(`Wrote sidecar metadata to ${relative(root, sidecars)}`)
}

async function main() {
  mkdirSync(out, { recursive: true })
  cleanup()

  const cliExists = await Bun.file(cliTarget).exists()
  const cliRebuild = force || (cliExists && (await cliStale()))

  if (cliExists && !cliRebuild) {
    const st = statSync(cliTarget)
    log(`Bun CLI sidecar already present at ${relative(root, cliTarget)} (${Math.round(st.size / 1024 / 1024)}MB). Use --force to rebuild.`)
  } else {
    if (cliExists) {
      log(force ? "Removing existing Bun CLI binary (--force)." : "Bun CLI source has changed; rebuilding.")
      rmSync(cliTarget)
    }
    await stage(await ensureCli(), cliTarget, cliVersion, cliHash, "Bun CLI sidecar")
  }

  const exists = await Bun.file(rustTarget).exists()
  const rebuild = force || (exists && (await stale()))

  if (exists && !rebuild) {
    const st = statSync(rustTarget)
    log(`Rust sidecar already present at ${relative(root, rustTarget)} (${Math.round(st.size / 1024 / 1024)}MB). Use --force to rebuild.`)
    await writeMetadata()
    return
  }

  if (exists && rebuild) {
    log(force ? "Removing existing Rust sidecar binary (--force)." : "Rust sidecar source has changed; rebuilding.")
    rmSync(rustTarget)
  }

  await stage(await ensure(), rustTarget, rustVersion, hash, "Rust sidecar")
  await writeMetadata()
}

try {
  await main()
} catch (err) {
  console.error(`[local-bin] ERROR: ${err instanceof Error ? err.message : String(err)}`)
  process.exit(1)
}
