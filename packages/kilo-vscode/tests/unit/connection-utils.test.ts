import { describe, it, expect } from "bun:test"
import {
  resolveEventSessionId,
  createMutationTrackingFetch,
  isMutatingMethod,
  methodFromFetchArgs,
} from "../../src/services/cli-backend/connection-utils"
import type { Event } from "@kilocode/sdk/v2/client"

const noLookup = (_: string) => undefined

/** Helper to create a partial Event for testing — only the fields accessed by resolveEventSessionId matter. */
function event(partial: Record<string, unknown>): Event {
  return partial as unknown as Event
}

describe("resolveEventSessionId", () => {
  it("returns session id from session.created", () => {
    const e = event({
      type: "session.created",
      properties: {
        info: { id: "s1", title: "", directory: "", time: { created: 0, updated: 0 } },
      },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s1")
  })

  it("returns session id from session.updated", () => {
    const e = event({
      type: "session.updated",
      properties: {
        info: { id: "s2", title: "", directory: "", time: { created: 0, updated: 0 } },
      },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s2")
  })

  it("returns sessionID from session.status", () => {
    const e = event({
      type: "session.status",
      properties: { sessionID: "s3", status: { type: "idle" } },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s3")
  })

  it("returns sessionID from todo.updated", () => {
    const e = event({
      type: "todo.updated",
      properties: { sessionID: "s4", todos: [] },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s4")
  })

  it("returns sessionID from message.updated and calls onMessageUpdated", () => {
    const e = event({
      type: "message.updated",
      properties: {
        info: { id: "m1", sessionID: "s5", role: "assistant", time: { created: 0 } },
      },
    })
    const recorded: [string, string][] = []
    const result = resolveEventSessionId(e, noLookup, (mid, sid) => recorded.push([mid, sid]))
    expect(result).toBe("s5")
    expect(recorded).toEqual([["m1", "s5"]])
  })

  it("message.updated does not require onMessageUpdated callback", () => {
    const e = event({
      type: "message.updated",
      properties: {
        info: { id: "m1", sessionID: "s5", role: "assistant", time: { created: 0 } },
      },
    })
    expect(() => resolveEventSessionId(e, noLookup)).not.toThrow()
  })

  it("returns sessionID directly from message.part.updated when part has sessionID", () => {
    const e = event({
      type: "message.part.updated",
      properties: {
        part: { type: "text", id: "p1", text: "", sessionID: "s6", messageID: "m1" },
      },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s6")
  })

  it("falls back to lookup when message.part.updated has no sessionID but has messageID", () => {
    const e = event({
      type: "message.part.updated",
      properties: {
        part: { type: "text", id: "p1", text: "", messageID: "m2" },
      },
    })
    const lookup = (id: string) => (id === "m2" ? "s7" : undefined)
    expect(resolveEventSessionId(e, lookup)).toBe("s7")
  })

  it("returns undefined for message.part.updated with no sessionID and messageID not in map", () => {
    const e = event({
      type: "message.part.updated",
      properties: {
        part: { type: "text", id: "p1", text: "", messageID: "unknown" },
      },
    })
    expect(resolveEventSessionId(e, noLookup)).toBeUndefined()
  })

  it("returns undefined for message.part.updated with no messageID and no sessionID", () => {
    const e = event({
      type: "message.part.updated",
      properties: {
        part: { type: "text", id: "p1", text: "" },
      },
    })
    expect(resolveEventSessionId(e, noLookup)).toBeUndefined()
  })

  it("returns sessionID from permission.asked", () => {
    const e = event({
      type: "permission.asked",
      properties: {
        id: "p1",
        sessionID: "s8",
        permission: "read_file",
        patterns: [],
        metadata: {},
        always: [],
      },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s8")
  })

  it("returns sessionID from question.asked", () => {
    const e = event({
      type: "question.asked",
      properties: { id: "q1", sessionID: "s9", questions: [] },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s9")
  })

  it("returns sessionID from question.replied", () => {
    const e = event({
      type: "question.replied",
      properties: { sessionID: "s10", requestID: "r1", answers: [] },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s10")
  })

  it("returns sessionID from question.rejected", () => {
    const e = event({
      type: "question.rejected",
      properties: { sessionID: "s11", requestID: "r2" },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s11")
  })

  it("returns sessionID from suggestion.shown", () => {
    const e = event({
      type: "suggestion.shown",
      properties: { id: "sug_1", sessionID: "s12", text: "Review?", actions: [] },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s12")
  })

  it("returns sessionID from suggestion.accepted", () => {
    const e = event({
      type: "suggestion.accepted",
      properties: { sessionID: "s13", requestID: "sug_1", index: 0, action: { label: "Start", prompt: "x" } },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s13")
  })

  it("returns sessionID from suggestion.dismissed", () => {
    const e = event({
      type: "suggestion.dismissed",
      properties: { sessionID: "s14", requestID: "sug_2" },
    })
    expect(resolveEventSessionId(e, noLookup)).toBe("s14")
  })

  it("returns undefined for unknown event types (global events)", () => {
    const e = event({ type: "server.connected", properties: {} })
    expect(resolveEventSessionId(e, noLookup)).toBeUndefined()
  })

  it("returns undefined for another unknown event type", () => {
    const e = event({ type: "server.heartbeat", properties: {} })
    expect(resolveEventSessionId(e, noLookup)).toBeUndefined()
  })
})

// ---------------------------------------------------------------------------
// Mutation tracking tests
// ---------------------------------------------------------------------------

describe("isMutatingMethod", () => {
  it("treats POST/PUT/PATCH/DELETE as mutating", () => {
    expect(isMutatingMethod("POST")).toBe(true)
    expect(isMutatingMethod("PUT")).toBe(true)
    expect(isMutatingMethod("PATCH")).toBe(true)
    expect(isMutatingMethod("DELETE")).toBe(true)
  })

  it("treats GET/HEAD/OPTIONS as non-mutating", () => {
    expect(isMutatingMethod("GET")).toBe(false)
    expect(isMutatingMethod("HEAD")).toBe(false)
    expect(isMutatingMethod("OPTIONS")).toBe(false)
  })

  it("is case-insensitive", () => {
    expect(isMutatingMethod("post")).toBe(true)
    expect(isMutatingMethod("Patch")).toBe(true)
    expect(isMutatingMethod("get")).toBe(false)
  })
})

describe("methodFromFetchArgs", () => {
  it("reads method from a Request input", () => {
    const req = new Request("http://x/", { method: "PATCH" })
    expect(methodFromFetchArgs(req)).toBe("PATCH")
  })

  it("reads method from init when input is a string", () => {
    expect(methodFromFetchArgs("http://x/", { method: "POST" })).toBe("POST")
  })

  it("reads method from init when input is a URL", () => {
    expect(methodFromFetchArgs(new URL("http://x/"), { method: "DELETE" })).toBe("DELETE")
  })

  it("defaults to GET when neither carries a method", () => {
    expect(methodFromFetchArgs("http://x/")).toBe("GET")
    expect(methodFromFetchArgs("http://x/", {})).toBe("GET")
  })
})

describe("createMutationTrackingFetch", () => {
  function fakeResponse(status: number): Response {
    return new Response("body", { status })
  }

  it("records a 2xx mutation response", async () => {
    let count = 0
    const baseFetch = (async () => fakeResponse(200)) as typeof fetch
    const wrapped = createMutationTrackingFetch(baseFetch, () => count++)

    await wrapped("http://x/", { method: "POST" })

    expect(count).toBe(1)
  })

  it("does NOT record a 404 response because Rust never accepted the mutation", async () => {
    let count = 0
    const baseFetch = (async () => fakeResponse(404)) as typeof fetch
    const wrapped = createMutationTrackingFetch(baseFetch, () => count++)

    await wrapped("http://x/", { method: "PATCH" })

    expect(count).toBe(0)
  })

  it("does NOT record a 500 response because Rust crashed before persisting", async () => {
    let count = 0
    const baseFetch = (async () => fakeResponse(500)) as typeof fetch
    const wrapped = createMutationTrackingFetch(baseFetch, () => count++)

    await wrapped("http://x/", { method: "DELETE" })

    expect(count).toBe(0)
  })

  it("does NOT record a network error (baseFetch throws)", async () => {
    // A thrown fetch means we never even got a response, so Rust cannot
    // have observed the mutation. The error must propagate untouched.
    let count = 0
    const networkError = new Error("ECONNREFUSED")
    const baseFetch = (async () => {
      throw networkError
    }) as typeof fetch
    const wrapped = createMutationTrackingFetch(baseFetch, () => count++)

    await expect(wrapped("http://x/", { method: "POST" })).rejects.toBe(networkError)
    expect(count).toBe(0)
  })

  it("does NOT record a 2xx GET because only mutating methods count", async () => {
    let count = 0
    const baseFetch = (async () => fakeResponse(200)) as typeof fetch
    const wrapped = createMutationTrackingFetch(baseFetch, () => count++)

    await wrapped("http://x/", { method: "GET" })

    expect(count).toBe(0)
  })

  it("records a Request object with method PATCH", async () => {
    // The SDK normally constructs a `Request` rather than passing init —
    // the helper has to read method off the Request, not init.
    let count = 0
    const baseFetch = (async () => fakeResponse(200)) as typeof fetch
    const wrapped = createMutationTrackingFetch(baseFetch, () => count++)

    await wrapped(new Request("http://x/", { method: "PATCH" }))

    expect(count).toBe(1)
  })

  it("records every successful mutation; caller handles idempotence", async () => {
    let count = 0
    const baseFetch = (async () => fakeResponse(201)) as typeof fetch
    const wrapped = createMutationTrackingFetch(baseFetch, () => count++)

    await wrapped("http://x/", { method: "POST" })
    await wrapped("http://x/", { method: "POST" })

    // The wrapper itself does not memoize; ServerManager.markMutationAttempted
    // short-circuits after the first call. The contract is "called at least
    // once per successful mutation," not "called exactly once."
    expect(count).toBe(2)
  })

  it("returns the response untouched (does not consume the body)", async () => {
    // Reading response.ok doesn't consume the body, so callers can still
    // parse JSON from it — verify by reading the body downstream.
    const baseFetch = (async () =>
      new Response(JSON.stringify({ ok: 1 }), {
        status: 200,
        headers: { "content-type": "application/json" },
      })) as typeof fetch
    const wrapped = createMutationTrackingFetch(baseFetch, () => {})

    const res = await wrapped("http://x/", { method: "POST" })
    const body = await res.json()

    expect(body).toEqual({ ok: 1 })
  })

  it("records a streaming 2xx mutation BEFORE the body finishes", async () => {
    // `POST /session/{id}/prompt_async` returns a streaming 200 OK whose body
    // completes long after headers arrive. Record once the sidecar accepts the
    // prompt, not when the stream eventually drains.
    let count = 0

    // ReadableStream that never closes — simulates a long-running SSE body.
    let _streamController: ReadableStreamDefaultController<Uint8Array> | null = null
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        _streamController = controller
        controller.enqueue(new TextEncoder().encode("event: open\n\n"))
      },
    })
    const baseFetch = (async () =>
      new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } })) as typeof fetch
    const wrapped = createMutationTrackingFetch(baseFetch, () => count++)

    // Use a Request, mirroring how the SDK calls the wrapped fetch.
    const res = await wrapped(new Request("http://x/session/abc/prompt_async", { method: "POST" }))

    // Mutation must already be recorded; we have the response head, body still open.
    expect(count).toBe(1)
    expect(res.ok).toBe(true)
    expect(res.body).toBeDefined()

    // Cleanup: cancel the stream so the test process doesn't leak it.
    await res.body?.cancel()
  })

  it("does NOT record a streaming 4xx response because sidecar rejected the prompt", async () => {
    let count = 0
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(new TextEncoder().encode("error\n"))
        c.close()
      },
    })
    const baseFetch = (async () =>
      new Response(body, { status: 404, headers: { "content-type": "text/event-stream" } })) as typeof fetch
    const wrapped = createMutationTrackingFetch(baseFetch, () => count++)

    const res = await wrapped(new Request("http://x/session/abc/prompt_async", { method: "POST" }))

    expect(count).toBe(0)
    expect(res.ok).toBe(false)
    await res.body?.cancel()
  })
})
