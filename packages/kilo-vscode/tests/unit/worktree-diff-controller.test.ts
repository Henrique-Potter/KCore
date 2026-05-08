import { describe, expect, it } from "bun:test"
import type { KiloClient } from "@kilocode/sdk/v2/client"
import { WorktreeDiffController } from "../../src/agent-manager/worktree-diff-controller"
import type { GitOps } from "../../src/agent-manager/GitOps"
import type { ManagedSession, Worktree, WorktreeStateManager } from "../../src/agent-manager/WorktreeStateManager"
import type { AgentManagerOutMessage, WorktreeDiffEntry } from "../../src/agent-manager/types"

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitFor(check: () => boolean, timeout = 500): Promise<void> {
  const start = Date.now()
  while (!check()) {
    if (Date.now() - start > timeout) throw new Error("timed out waiting for condition")
    await sleep(5)
  }
}

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

function diff(file: string): WorktreeDiffEntry {
  return {
    file,
    patch: "",
    before: "",
    after: "",
    additions: 1,
    deletions: 0,
    status: "modified",
    tracked: true,
    generatedLike: false,
    summarized: true,
    stamp: file,
  }
}

function state(): WorktreeStateManager {
  const sessions = new Map<string, ManagedSession>([
    ["a", { id: "a", worktreeId: "wa", createdAt: "2026-01-01T00:00:00.000Z" }],
    ["b", { id: "b", worktreeId: "wb", createdAt: "2026-01-01T00:00:00.000Z" }],
  ])
  const worktrees = new Map<string, Worktree>([
    [
      "wa",
      { id: "wa", branch: "a", path: "/tmp/a", parentBranch: "main", remote: "origin", createdAt: "2026-01-01" },
    ],
    [
      "wb",
      { id: "wb", branch: "b", path: "/tmp/b", parentBranch: "main", remote: "origin", createdAt: "2026-01-01" },
    ],
  ])

  return {
    getSession: (id: string) => sessions.get(id),
    getSessions: () => [...sessions.values()],
    getWorktree: (id: string) => worktrees.get(id),
  } as unknown as WorktreeStateManager
}

describe("WorktreeDiffController", () => {
  it("does not publish an old diff request after switching sessions", async () => {
    const posts: AgentManagerOutMessage[] = []
    const calls: string[] = []
    const a = deferred<WorktreeDiffEntry[]>()
    const b = deferred<WorktreeDiffEntry[]>()
    const ctl = new WorktreeDiffController({
      getState: () => state(),
      getRoot: () => undefined,
      getStateReady: () => undefined,
      getClient: () => ({}) as KiloClient,
      git: {} as GitOps,
      localDiff: (dir) => {
        calls.push(dir)
        return dir.endsWith("/a") ? a.promise : b.promise
      },
      localDiffFile: async () => null,
      post: (msg) => posts.push(msg),
      log: () => undefined,
    })

    ctl.start("a")
    await waitFor(() => calls.includes("/tmp/a"))
    ctl.start("b")
    await waitFor(() => calls.includes("/tmp/b"))

    b.resolve([diff("b.ts")])
    await waitFor(() => posts.some((msg) => msg.type === "agentManager.worktreeDiff" && msg.sessionId === "b"))
    a.resolve([diff("a.ts")])
    await sleep(20)
    ctl.stop()

    const diffs = posts.filter((msg) => msg.type === "agentManager.worktreeDiff")
    expect(diffs).toHaveLength(1)
    expect(diffs[0]?.sessionId).toBe("b")
  })

  it("does not publish a diff request after stop", async () => {
    const posts: AgentManagerOutMessage[] = []
    const calls: string[] = []
    const a = deferred<WorktreeDiffEntry[]>()
    const ctl = new WorktreeDiffController({
      getState: () => state(),
      getRoot: () => undefined,
      getStateReady: () => undefined,
      getClient: () => ({}) as KiloClient,
      git: {} as GitOps,
      localDiff: (dir) => {
        calls.push(dir)
        return a.promise
      },
      localDiffFile: async () => null,
      post: (msg) => posts.push(msg),
      log: () => undefined,
    })

    ctl.start("a")
    await waitFor(() => calls.includes("/tmp/a"))
    ctl.stop()
    a.resolve([diff("a.ts")])
    await sleep(20)

    expect(posts.filter((msg) => msg.type === "agentManager.worktreeDiff")).toHaveLength(0)
  })
})
