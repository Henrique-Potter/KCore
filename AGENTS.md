# AGENTS.md

Kilo CLI is an open source AI coding agent that generates code from natural language, automates tasks, and supports 500+ AI models.

- **Always use parallel tools when possible.**
- Default branch: `main`.
- Execute requested actions without confirmation unless blocked by missing info or safety/irreversibility.
- If running in a git worktree, all changes go in your current working directory — never modify files in the main checkout.

## Build and Dev

- **Dev**: `bun run dev` from root, or `bun run --cwd packages/opencode --conditions=browser src/index.ts`. Pass args via `bun dev -- help`.
- **Extension**: `bun run extension` (build + launch VS Code in dev mode). Add `--no-build` to skip the build.
- **Typecheck**: `bun turbo typecheck` (uses `tsgo`, not `tsc`).
- **Test**: `bun test` from `packages/opencode/` (root blocks tests). Targeted: `bun test ./test/tool/tool-define.test.ts`.
- **CLI build artifact size**: after `bun run script/build.ts --single --skip-install` in `packages/opencode/`, check `du -h dist/@kilocode/*/bin/kilo`.
- **SDK regen**: after changing endpoints in `packages/opencode/src/server/`, run `./script/generate.ts` from root to regenerate `packages/sdk/js/`.
- **Backend smoke**: see [TESTING.md](./TESTING.md) for spawning the local backend (`bun dev serve`) and driving it via `curl` — preferred over `kilo serve` (prod binary) when testing fixes.

### CI guards (run locally before pushing)

| Guard | Command | What it enforces |
|---|---|---|
| Knip | `bun run knip` from `packages/kilo-vscode/` | All exported types/functions are imported. Remove or unexport orphans. |
| `kilocode_change` markers | `bun run check-kilocode-change` from `packages/kilo-vscode/` | The marker must not appear in `packages/kilo-vscode/` or `packages/kilo-ui/` (entirely Kilo additions). |
| OpenCode annotations | `bun run script/check-opencode-annotations.ts` from root | Kilo-specific edits inside shared `packages/opencode/` files must carry `kilocode_change` markers. Exempt: paths containing `kilocode`. |
| Source links | `bun run script/extract-source-links.ts` from root | After URL changes in `packages/kilo-vscode/{,webview-ui/}` or `packages/opencode/src/`, regenerate `packages/kilo-docs/source-links.md`. |
| Markdown table padding | `bun run script/check-md-table-padding.ts [--fix]` from root | Compact table cells (see Markdown Tables below). |

## Quality Checks

Before claiming an implementation is ready, run the smallest checks that catch lint/typecheck/test failures for the touched package. Don't rely on a manual extension launch to find build problems. Fix what you broke; if a check is still failing or couldn't be run, say so.

| Area | Checks |
|---|---|
| Root / cross-package | `bun run lint`, `bun run typecheck` |
| CLI | From `packages/opencode/`: `bun run typecheck`, `bun test` (or targeted) |
| VS Code extension | From `packages/kilo-vscode/`: `bun run typecheck`, `bun run lint`, `bun run test:unit` |
| Extension build/package | From `packages/kilo-vscode/`: `bun run compile` or `bun run package` when touching build, packaging, SDK, or webview paths |

Never run root `bun test` — the script prints `do not run tests from root` and exits 1.

## Products

All products are clients of the **CLI** (`packages/opencode/`), which contains the agent runtime, HTTP server, and session management. Each client spawns or connects to a `kilo serve` process and talks to it via HTTP + SSE through `@kilocode/sdk`.

| Product | Package | Description |
|---|---|---|
| Kilo CLI | `packages/opencode/` | Core engine. TUI, `kilo run`, `kilo serve`, `kilo web`. Fork of upstream OpenCode. |
| Kilo VS Code Extension | `packages/kilo-vscode/` | VS Code extension. Bundles the CLI binary, spawns `kilo serve` as a child. Includes the **Agent Manager** — a multi-session panel with git worktree isolation. |
| OpenCode Desktop | `packages/desktop/` | Standalone Tauri app. Bundles CLI as sidecar. Single-session UI. |
| OpenCode Web | `packages/app/` | Shared SolidJS frontend used by the desktop app and `kilo web`. |

**Agent Manager** is a feature inside `packages/kilo-vscode/` (extension code in `src/agent-manager/`, webview in `webview-ui/agent-manager/`), not a standalone product. See [`packages/kilo-vscode/AGENTS.md`](packages/kilo-vscode/AGENTS.md).

Extension-specific settings live in the Kilo extension settings, not default VS Code settings, unless intentionally VS Code-wide.

## Monorepo Structure

Turborepo + Bun workspaces.

| Package | Name | Purpose |
|---|---|---|
| `packages/opencode/` | `@kilocode/cli` | Core CLI — agents, tools, sessions, server, TUI. Most work happens here. |
| `packages/sdk/js/` | `@kilocode/sdk` | Auto-generated TypeScript SDK. Don't edit `src/gen/` by hand. |
| `packages/kilo-vscode/` | `kilo-code` | VS Code extension with sidebar chat + Agent Manager. |
| `packages/kilo-gateway/` | `@kilocode/kilo-gateway` | Kilo auth, provider routing, API integration. |
| `packages/kilo-telemetry/` | `@kilocode/kilo-telemetry` | PostHog + OpenTelemetry. |
| `packages/kilo-i18n/` | `@kilocode/kilo-i18n` | Translations. |
| `packages/kilo-ui/` | `@kilocode/kilo-ui` | SolidJS components shared by extension webview and `packages/app/`. |
| `packages/app/` | `@opencode-ai/app` | SolidJS web UI for desktop and `kilo web`. |
| `packages/desktop/` | `@opencode-ai/desktop` | Tauri desktop app shell. |
| `packages/util/` | `@opencode-ai/util` | Shared utilities (error, path, retry, slug). |
| `packages/plugin/` | `@kilocode/plugin` | Plugin/tool interface. |

## Style Guide

- Keep things in one function unless composable or reusable.
- Prefer `obj.a` / `obj.b` to destructuring (`const { a, b } = obj`) — preserves context.
- Avoid `try`/`catch` where possible.
- Avoid `any`.
- Use Bun APIs when applicable (`Bun.file()` etc.).
- Rely on type inference; only add explicit annotations for exports or clarity.

### Naming — single words by default (mandatory for agent-written code)

- New locals, params, and helpers get single-word names by default. Use multi-word names only when a single word would be unclear.
- Do not introduce new camelCase compounds when a short single-word alternative is clear.
- Before finishing edits, review touched lines and shorten new identifiers where possible.
- Prefer: `pid`, `cfg`, `err`, `opts`, `dir`, `root`, `child`, `state`, `timeout`.
- Avoid unless truly required: `inputPID`, `existingClient`, `connectTimeout`, `workerPath`.

```ts
// Good
const foo = 1
const bar = 2

// Bad
const fooBar = 1
const barBaz = 2
```

### Avoid `let`

Prefer `const` with a ternary over `let` + `if/else`.

```ts
// Good
const foo = condition ? 1 : 2

// Bad
let foo
if (condition) foo = 1
else foo = 2
```

### Avoid `else`

Prefer early returns or an IIFE.

```ts
// Good
function foo() {
  if (condition) return 1
  return 2
}

// Bad
function foo() {
  if (condition) return 1
  else return 2
}
```

### No empty catch blocks

An empty `catch` silently swallows errors. If you're tempted to write one:

1. Is the `try`/`catch` even needed? (prefer removing it)
2. Should the error be handled explicitly? (recover, retry, rethrow)
3. At minimum, log it.

```ts
// Good
try {
  await save(data)
} catch (err) {
  log.error("save failed", { err })
}

// Bad
try {
  await save(data)
} catch {}
```

## Testing

Avoid `mocks` as much as possible. Tests must exercise the real implementation; do not duplicate logic into a test.

## Markdown Tables

Do not pad markdown table cells for column alignment. Compact form, single-space-padded content cells, minimal separator row:

```
| Command | What it runs |
|---|---|
| `kilo serve` | The prod CLI on `$PATH`. |
```

Padding makes every content change rewrite the entire table, which blows up diffs on untouched rows. Markdown is excluded from prettier (see `.prettierignore`), so the formatter won't re-pad. CI runs `script/check-md-table-padding.ts`. Use `--fix` to auto-rewrite padded tables.

## Commit Conventions

[Conventional Commits](https://www.conventionalcommits.org/) with package-named scopes: `vscode`, `cli`, `agent-manager`, `sdk`, `ui`, `i18n`, `kilo-docs`, `gateway`, `telemetry`, `desktop`. Omit the scope when spanning multiple packages.

## Changesets

User-facing changes (features, fixes, breaking changes) need a changeset for release notes. `bunx changeset add` or create `.changeset/<slug>.md` manually. `patch` for fixes, `minor` for features, `major` for breaks.

Changeset descriptions go directly into release notes and are read by end users — keep them concise and feature-oriented (what changed from the user's perspective, not implementation). Imperative mood: "Support exporting conversations as markdown" — not "Add a new export handler that serializes session messages".

## Pull Requests

PR descriptions: 2–3 lines covering **what** changed and **why**. Focus on intent and context the diff can't show. Skip file inventories, test summaries, and anything obvious from the code.

## GitHub Issues

- Use the issue templates in `.github/ISSUE_TEMPLATE/` (`Bug report`, `Feature Request`, `Question`) — not blank issues.
- No platform prefixes in titles (`[JetBrains]`, `[VS Code]`, `[JB]`, etc.). Use plain descriptive titles.
- VS Code extension issues → project [`VS Code Extension`](https://github.com/orgs/Kilo-Org/projects/25).
- JetBrains plugin issues → project [`Jetbrains Plugin`](https://github.com/orgs/Kilo-Org/projects/39).
- With `gh`, prefer `gh issue create --template "..." --project "..."`. If project assignment fails, run `gh auth refresh -s project` and retry.

## Fork Merge Process

Kilo CLI is a fork of [opencode](https://github.com/anomalyco/opencode). We regularly merge upstream changes, so **minimize edits to shared (non-Kilo) code**.

A path is **Kilo-specific** if any directory in it contains `kilo`:

- `packages/opencode/src/kilocode/` — Kilo-specific source.
- `packages/opencode/test/kilocode/` — Kilo-specific tests.
- `packages/kilo-*` — entirely Kilo packages (e.g. `kilo-gateway`, `kilo-vscode`, `kilo-ui`, `kilo-docs`).

Everything else is shared. When you must edit shared code:

1. Keep the change minimal and isolated.
2. Mark it with `kilocode_change` (CI rejects unannotated edits in shared opencode files).
3. Don't refactor or reorganize upstream code unless absolutely necessary.

When adding a `kilocode_change` key to `Config.Info` in `packages/opencode/src/config/config.ts`, also add the matching JSON Schema entry in `apps/web/src/app/config.json/extras.ts` in the [cloud repo](https://github.com/Kilo-Org/cloud). See [CLI Config Schema](packages/kilo-docs/pages/contributing/architecture/config-schema.md).

### `kilocode_change` markers

```typescript
const value = 42 // kilocode_change

// kilocode_change start
const foo = 1
const bar = 2
// kilocode_change end

// kilocode_change - new file
```

JSX/TSX:

<!-- prettier-ignore -->
```tsx
{/* kilocode_change */}
{/* kilocode_change start */}
<MyComponent />
{/* kilocode_change end */}
```

Markers are **not** needed inside any `kilocode`-named directory or file — those paths are Kilo-only and won't conflict upstream.
