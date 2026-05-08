# Tool Behavior

<!-- memory-bank:managed:start section="lessons" version="1" -->
No approved lessons yet.
<!-- memory-bank:managed:end section="lessons" -->

## Local install notes

- 2026-05-06: Installed OpenAI Codex CLI globally with `npm.cmd install -g @openai/codex`.
- PowerShell execution policy blocked npm's generated `codex.ps1` shim, so `C:\Users\HPotter\AppData\Roaming\npm\codex.ps1` was moved to `codex.ps1.disabled`. Plain `codex` now resolves to `codex.cmd` and verifies as `codex-cli 0.128.0`.
