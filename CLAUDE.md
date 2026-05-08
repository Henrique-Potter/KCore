# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

`AGENTS.md` (root) and `packages/<pkg>/AGENTS.md` are the long-form references — read the relevant one when touching a package. This file calls out the things that bite quickly.

## Repo shape in one paragraph

Turborepo + Bun workspaces. The core engine is the **CLI** at `packages/opencode/` (published as `@kilocode/cli`) — a fork of upstream [OpenCode](https://github.com/anomalyco/opencode) with Kilo additions. Every other product is a thin client that spawns `kilo serve --port 0` and talks to it over HTTP + SSE through the auto-generated `@kilocode/sdk`: the VS Code extension (`packages/kilo-vscode/`), the Tauri desktop shell (`packages/desktop/`), the shared SolidJS web UI (`packages/app/`), and the JetBrains plugin (`packages/kilo-jetbrains/`). A separate Rust workspace at `packages/kilo-vscode-sidecar-rs/` is an in-progress reimplementation of the VS Code sidecar — see "Rust sidecar" below.

## Build, lint, test

| Area | Command | Run from |
|---|---|---|
| Dev (CLI) | `bun run dev` (or `bun dev <args>` to forward args) | repo root |
| Lint (oxlint, fast) | `bun run lint` | repo root |
| Typecheck (uses `tsgo`, not `tsc`) | `bun turbo typecheck` | repo root |
| CLI tests | `bun test` (or `bun test ./test/tool/tool-define.test.ts`) | `packages/opencode/` |
| Extension tests | `bun run test:unit` / `bun run test` | `packages/kilo-vscode/` |
| Extension build + launch | `bun run extension` (add `--no-build` to skip) | repo root or `packages/kilo-vscode/` |
| SDK regen (after server route changes) | `./script/generate.ts` | repo root |
| Format | `bun run format` (run before committing) | the touched package |

**Never run `bun test` from the repo root** — the script prints `do not run tests from root` and exits 1. Tests live per-package.

CI guards to run locally before pushing (these block merges):

| Guard | Command | What it enforces |
|---|---|---|
| Knip | `bun run knip` (in `packages/kilo-vscode/`) | No unimported exports |
| `kilocode_change` markers in opencode | `bun run script/check-opencode-annotations.ts` (root) | Edits to shared opencode files must carry the marker (see "Fork merge") |
| `kilocode_change` markers absent in Kilo packages | `bun run check-kilocode-change` (in `packages/kilo-vscode/`) | Marker must NOT appear in entirely-Kilo packages |
| Source links | `bun run script/extract-source-links.ts` (root) | After URL changes in `kilo-vscode` or `opencode/src/`, regenerate `kilo-docs/source-links.md` |
| Markdown table padding | `bun run script/check-md-table-padding.ts [--fix]` (root) | Compact tables only — no padding for column alignment |

## `bun dev serve` vs `kilo serve` — important

`kilo serve` runs the **npm-installed production CLI** on `$PATH`, NOT the code in this worktree. To exercise local edits, use `bun dev serve …` — it imports `packages/opencode/src/index.ts` directly, no rebuild needed between edits. Same for `createKiloServer()` from `@kilocode/sdk/v2`: it spawns the PATH `kilo` binary, so don't use it to test local code.

`TESTING.md` has the full out-of-process curl recipe for spinning up the local backend with auth headers and `x-kilo-directory`.

## Fork merge: `kilocode_change` markers

Kilo CLI is a fork of [OpenCode](https://github.com/anomalyco/opencode), and we regularly merge upstream. **Minimize edits to shared (non-Kilo) code.** A path is Kilo-specific if any directory in it contains `kilo` (e.g. `packages/opencode/src/kilocode/`, `packages/opencode/test/kilocode/`, `packages/kilo-*`). Everything else is shared.

When you must edit shared code, mark it with `kilocode_change`:

```ts
const value = 42 // kilocode_change

// kilocode_change start
const foo = 1
const bar = 2
// kilocode_change end

// kilocode_change - new file
```

CI rejects unannotated edits to shared opencode files. Markers are NOT needed inside `kilocode`-named directories or files (and CI rejects them there too — they belong only at the upstream/fork boundary).

## Style guide highlights

Full guide in `AGENTS.md`, but the rules that matter for agent-written code:

- **Single-word names by default** for new locals/params/helpers (`pid`, `cfg`, `err`, `opts`, `dir`, `root`, `child`). Multi-word only when a single word would be unclear. Don't introduce camelCase compounds when a short single word reads cleanly.
- Prefer `const` + ternary over `let` + if/else.
- Prefer early returns over `else`.
- Avoid `try`/`catch` where possible; **never write empty catch blocks** — log at minimum.
- Avoid `any`; rely on type inference, only annotate exports.
- Prefer `obj.a` / `obj.b` over destructuring (preserves call-site context).
- Use Bun APIs (`Bun.file()`, etc.) where applicable.

Tests must exercise the real implementation — **avoid mocks**; do not duplicate logic into a test.

## Markdown tables

Compact form only — single-space-padded cells, minimal separator row. Padding for column alignment makes every content edit rewrite untouched rows. Markdown is excluded from prettier (see `.prettierignore`); `script/check-md-table-padding.ts --fix` re-collapses any padded tables.

## VS Code extension specifics (`packages/kilo-vscode/`)

- The extension bundles its own CLI binary at `bin/kilo` — it does NOT use a system `kilo`. Build/refresh it with `bun script/local-bin.ts` (add `--force` to rebuild).
- All VS Code commands use the `kilo-code.new.` prefix; view IDs use `kilo-code.new.` **except** the sidebar view (`kilo-code.SidebarProvider`, preserved for legacy upgrade compat).
- **Windows process spawning**: never import `spawn`/`execFile`/`exec` directly from `child_process` — they flash a cmd.exe window without `windowsHide: true`. Use the wrappers in `src/util/process.ts` instead.
- Webview is **Solid.js**, not React. Solid JSX compiles via `esbuild-plugin-solid`. New webview UI must use `@kilocode/kilo-ui` components — check `packages/app/src/` first for the reference composition.
- Large files in `src/agent-manager/` have `maxLines` caps enforced by `tests/unit/agent-manager-arch.test.ts`. **Do not raise the caps** — extract a vscode-free helper module instead. See `fork-session.ts` and `format-keybinding.ts` as the pattern.
- Debug output must be prepended with `[Kilo New]` for filterability.
- This package is entirely Kilo-specific — `kilocode_change` markers are NOT needed (and will fail CI) in any file under `packages/kilo-vscode/`.

## Rust sidecar (`packages/kilo-vscode-sidecar-rs/`)

A separate Cargo workspace reimplementing the Bun VS Code sidecar in Rust. Lives outside `packages/opencode/` to keep upstream merge churn down. Crate layout:

| Crate | Purpose |
|---|---|
| `kilo-vscode-sidecar` | Thin binary — command parsing, signal handling, process entrypoint |
| `kilo-server` | HTTP/SSE router, middleware, route handlers, OAuth, agent turn loop, fake-provider tools, permissions, file search. Internal module tree in `docs/architecture.md`. |
| `kilo-protocol` | Frozen preview wire types and version constants |
| `kilo-store` | Bun-compatible session/config/auth SQLite + JSON store |
| `kilo-provider` | Provider registry, OpenAI Responses API client, OAuth helpers |
| `kilo-oracle` | Bun/Rust oracle fixture harness for behavioral parity |
| `kilo-session`, `kilo-tools`, `kilo-mcp` | Reserved (empty) for later milestone extractions |

Build/run:

```sh
cargo fmt --all
cargo check --workspace
cargo run -p kilo-vscode-sidecar -- serve --port 0
```

Sidebar first-chat smoke (oracle integration test, no UI/provider creds needed):

```sh
cargo test -p kilo-oracle --test m7_rust_fixtures m7_sidebar_first_chat_smoke_streams_persists_and_reads_back -- --nocapture
```

To launch the VS Code extension against a local Rust sidecar build:

```bat
set KILO_VSCODE_RUST_SIDECAR_PATH=packages\kilo-vscode-sidecar-rs\target\debug\kilo-vscode-sidecar.exe
bun run extension
```

The frozen preview seam (process flags, HTTP routes implemented so far, auth, SSE shape, allowed `NamedError` names, ordering invariants) is in `packages/kilo-vscode-sidecar-rs/CONTRACT.md`. The Bun sidecar is the executable oracle — Rust must reproduce its observable behavior within the bounds documented there.

## Commits, changesets, PRs

- [Conventional Commits](https://www.conventionalcommits.org/) with package-named scopes: `vscode`, `cli`, `agent-manager`, `sdk`, `ui`, `i18n`, `kilo-docs`, `gateway`, `telemetry`, `desktop`. Omit scope when spanning multiple.
- User-facing changes need a changeset (`bunx changeset add`). Descriptions go straight into release notes — keep them user-feature-oriented and imperative ("Support exporting conversations as markdown"), not implementation-summary.
- PR descriptions: 2–3 lines covering **what** changed and **why**. Skip file inventories, test summaries, and anything obvious from the diff.
- Run `bun run format` in the touched package before committing — keeps commits free of styling-only diffs.
