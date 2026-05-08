#!/usr/bin/env bun
import { $ } from "bun"
import { join } from "node:path"
import { chmodSync, copyFileSync, existsSync, mkdirSync, rmSync } from "node:fs"
import { artifact, manifest } from "./sidecar-metadata"

const packageJsonPath = join(import.meta.dir, "..", "package.json")
const packageJson = await Bun.file(packageJsonPath).json()
const version = process.env.KILO_VERSION ? process.env.KILO_VERSION : packageJson.version
const prerelease = process.env.KILO_PRE_RELEASE === "true"
const cliDir = join(import.meta.dir, "..", "..", "opencode")
const cliDistDir = process.env.BUN_SIDECAR_DIST_DIR || join(cliDir, "dist")
const rustDir = join(import.meta.dir, "..", "..", "kilo-vscode-sidecar-rs")
const sidecarDir = process.env.RUST_SIDECAR_DIST_DIR || join(rustDir, "dist")

console.log(`Building VSCode extension version: ${version}${prerelease ? " (pre-release)" : ""}`)

if (packageJson.version !== version) {
  console.log(`Updating package.json version from ${packageJson.version} to ${version}`)
  packageJson.version = version
  await Bun.write(packageJsonPath, JSON.stringify(packageJson, null, 2) + "\n")
}

console.log(`Using Bun sidecar dist directory: ${cliDistDir}`)
console.log(`Using Rust sidecar dist directory: ${sidecarDir}`)

const targets = [
  { target: "linux-x64", cli: "cli-linux-x64", bun: "kilo", rust: "kilo-vscode-sidecar" },
  { target: "linux-arm64", cli: "cli-linux-arm64", bun: "kilo", rust: "kilo-vscode-sidecar" },
  { target: "alpine-x64", cli: "cli-linux-x64-musl", bun: "kilo", rust: "kilo-vscode-sidecar" },
  { target: "alpine-arm64", cli: "cli-linux-arm64-musl", bun: "kilo", rust: "kilo-vscode-sidecar" },
  { target: "darwin-x64", cli: "cli-darwin-x64", bun: "kilo", rust: "kilo-vscode-sidecar" },
  { target: "darwin-arm64", cli: "cli-darwin-arm64", bun: "kilo", rust: "kilo-vscode-sidecar" },
  { target: "win32-x64", cli: "cli-windows-x64", bun: "kilo.exe", rust: "kilo-vscode-sidecar.exe" },
  { target: "win32-arm64", cli: "cli-windows-arm64", bun: "kilo.exe", rust: "kilo-vscode-sidecar.exe" },
]

const binDir = join(import.meta.dir, "..", "bin")
const distDir = join(import.meta.dir, "..", "dist")
const outDir = join(import.meta.dir, "..", "out")
const meta = join(binDir, "sidecars.json")

function hostTarget(): string | null {
  const arch = process.arch === "x64" || process.arch === "arm64" ? process.arch : null
  if (!arch) return null
  if (process.platform === "win32") return `win32-${arch}`
  if (process.platform === "darwin") return `darwin-${arch}`
  if (process.platform === "linux") return `linux-${arch}`
  return null
}

function localCliBinary(binary: string): string {
  const dir = join(cliDir, "dist", "@kilocode", `cli-${hostCliTarget()}`, "bin")
  const bin = join(dir, binary)
  if (existsSync(bin)) return bin
  return join(dir, "kilo")
}

function localRustBinary(binary: string): string {
  return join(rustDir, "target", "release", binary)
}

function hostCliTarget(): string {
  const os = process.platform === "win32" ? "windows" : process.platform
  return `${os}-${process.arch}`
}

function sourceCliBinary(target: string, binary: string): string {
  const dir = join(cliDistDir, "@kilocode", target, "bin")
  const dist = join(dir, binary)
  if (existsSync(dist)) return dist
  const plain = join(dir, "kilo")
  if (existsSync(plain)) return plain
  if (`cli-${hostCliTarget()}` === target && existsSync(localCliBinary(binary))) return localCliBinary(binary)
  throw new Error(
    `Bun CLI sidecar binary not found for ${target}. Expected ${dist}. ` +
      `Set BUN_SIDECAR_DIST_DIR to a directory containing @kilocode/${target}/bin/${binary}.`,
  )
}

function sourceRustBinary(target: string, binary: string): string {
  const dist = join(sidecarDir, target, binary)
  if (existsSync(dist)) return dist
  if (hostTarget() === target && existsSync(localRustBinary(binary))) return localRustBinary(binary)
  throw new Error(
    `Rust sidecar binary not found for ${target}. Expected ${dist}. ` +
      `Set RUST_SIDECAR_DIST_DIR to a directory containing <target>/${binary}.`,
  )
}

async function version(dir: string): Promise<string | null> {
  try {
    const result = await $`git log -1 --format=%H -- .`.cwd(dir).quiet()
    return result.text().trim() || null
  } catch {
    return null
  }
}

const cliVersion = await version(cliDir)
const rustVersion = await version(rustDir)

console.log("\nCleaning up directories...")
for (const dir of [binDir, distDir, outDir]) {
  if (existsSync(dir)) {
    rmSync(dir, { recursive: true, force: true })
    console.log(`  Cleaned ${dir}`)
  }
}

mkdirSync(outDir, { recursive: true })
mkdirSync(distDir, { recursive: true })

console.log("\nRebuilding SDK types (ensures dist/ is in sync with server API)...")
await $`bun run --cwd ${join(import.meta.dir, "..", "..", "sdk", "js")} build`

console.log("\nCompiling extension...")
await $`bun run check-types`
await $`bun run lint`
await $`node ${join(import.meta.dir, "..", "esbuild.js")} --production`

for (const config of targets) {
  console.log(`\nProcessing target: ${config.target}`)

  if (existsSync(binDir)) {
    rmSync(binDir, { recursive: true, force: true })
  }
  mkdirSync(binDir, { recursive: true })

  const bunSource = sourceCliBinary(config.cli, config.bun)
  const bunTarget = join(binDir, config.bun)

  console.log(`  Copying Bun CLI sidecar from ${bunSource}...`)
  copyFileSync(bunSource, bunTarget)

  const rustSource = sourceRustBinary(config.target, config.rust)
  const rustTarget = join(binDir, config.rust)

  console.log(`  Copying Rust sidecar from ${rustSource}...`)
  copyFileSync(rustSource, rustTarget)

  if (!config.target.startsWith("win32")) {
    chmodSync(bunTarget, 0o755)
    chmodSync(rustTarget, 0o755)
  }

  await manifest(meta, [
    artifact({ kind: "bun-cli", file: bunTarget, target: config.target, version: cliVersion }),
    artifact({ kind: "rust-sidecar", file: rustTarget, target: config.target, version: rustVersion }),
  ])

  console.log(`  Bun CLI sidecar ready at ${bunTarget}`)
  console.log(`  Rust sidecar ready at ${rustTarget}`)
  console.log(`  Sidecar metadata ready at ${meta}`)

  console.log(`  Packaging .vsix for ${config.target}${prerelease ? " (pre-release)" : ""}...`)
  const vsixPath = join(outDir, `kilo-vscode-${config.target}.vsix`)
  const args = ["--no-dependencies", "--skip-license", "--target", config.target, "-o", vsixPath]
  if (prerelease) args.push("--pre-release")
  await $`vsce package ${args}`.env({
    ...process.env,
    npm_config_ignore_scripts: "true",
  })
  console.log(`  Created ${vsixPath}`)
}

console.log("\nAll VSIX packages built successfully!")
