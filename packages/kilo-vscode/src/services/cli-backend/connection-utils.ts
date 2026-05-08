import type { Event } from "@kilocode/sdk/v2/client"

/**
 * The HTTP methods that count as accepted sidecar mutations for diagnostics.
 *
 * GET/HEAD/OPTIONS are the only safe-by-default methods. Everything else
 * (POST/PUT/PATCH/DELETE) is treated as a mutation candidate.
 */
const MUTATING_METHODS = new Set(["POST", "PUT", "PATCH", "DELETE"])

export function isMutatingMethod(method: string): boolean {
  return MUTATING_METHODS.has(method.toUpperCase())
}

/**
 * Resolve the HTTP method from a `fetch(input, init)` call. The SDK passes
 * a `Request` instance for SDK-driven calls (method on the Request); the
 * `(string|URL)` form carries the verb in `init.method`. Defaults to GET
 * when neither is present, matching the WHATWG fetch default.
 */
export function methodFromFetchArgs(input: RequestInfo | URL, init?: RequestInit): string {
  if (typeof input === "object" && input !== null && "method" in (input as any)) {
    return (input as Request).method
  }
  return init?.method ?? "GET"
}

/**
 * Wrap a `fetch` so `onMutationObserved` runs after the sidecar has accepted a
 * mutation by responding with a 2xx status, never on the request side.
 *
 * Behavior:
 *
 * - Mutating method (POST/PUT/PATCH/DELETE) **with** a 2xx response →
 *   `onMutationObserved()` is called once. Streaming routes (e.g.
 *   `POST /session/{id}/prompt_async`) flip on the initial 200 OK headers,
 *   which is correct: by then Rust has accepted the prompt.
 * - Mutating method with a 3xx/4xx/5xx response → callback is **not** called.
 *   Rust did not accept the mutation.
 * - Mutating method with a network error (baseFetch throws) → callback is
 *   **not** called, error is re-thrown. Same reasoning: no acceptance.
 * - GET/HEAD/OPTIONS regardless of status → callback is **not** called.
 *
 * The returned function preserves the standard `fetch` signature so it can
 * be passed directly to `createKiloClient({ fetch })`.
 *
 * @param baseFetch The underlying fetch (typically the SDK's
 *   duplex/timeout-aware wrapper).
 * @param onMutationObserved Called once per successful mutation. Idempotent
 *   responsibility lives with the caller (`ServerManager.markMutationAttempted`
 *   already short-circuits on repeats), but the wrapper itself does not
 *   memoize, so each successful mutation triggers a callback.
 */
export function createMutationTrackingFetch(
  baseFetch: typeof fetch,
  onMutationObserved: () => void,
): typeof fetch {
  return async (input, init) => {
    const method = methodFromFetchArgs(input, init)
    const response = await baseFetch(input, init)
    if (isMutatingMethod(method) && response.ok) {
      onMutationObserved()
    }
    return response
  }
}

/**
 * Pure session ID resolution for SSE events.
 * The lookupMessageSessionId callback is used for message.part.updated fallback lookup,
 * and onMessageUpdated is called when message.updated is encountered so the caller can
 * record the messageID -> sessionID mapping.
 */
export function resolveEventSessionId(
  event: Event,
  lookupMessageSessionId: (messageId: string) => string | undefined,
  onMessageUpdated?: (messageId: string, sessionId: string) => void,
): string | undefined {
  switch (event.type) {
    case "session.created":
    case "session.updated":
      return event.properties.info.id
    case "session.status":
    case "session.idle":
    case "session.error":
    case "todo.updated":
      return event.properties.sessionID
    case "message.updated":
      onMessageUpdated?.(event.properties.info.id, event.properties.info.sessionID)
      return event.properties.info.sessionID
    case "message.part.updated": {
      const part = event.properties.part as { messageID?: string; sessionID?: string }
      if (part.sessionID) {
        return part.sessionID
      }
      if (!part.messageID) {
        return undefined
      }
      return lookupMessageSessionId(part.messageID)
    }
    case "message.part.delta":
      return event.properties.sessionID
    case "permission.asked":
    case "permission.replied":
    case "question.asked":
    case "question.replied":
    case "question.rejected":
      return event.properties.sessionID
    default:
      return resolveSuggestionSessionId(event)
  }
}

function resolveSuggestionSessionId(event: Event): string | undefined {
  switch (event.type) {
    case "suggestion.shown":
    case "suggestion.accepted":
    case "suggestion.dismissed":
      return event.properties.sessionID
    default:
      // session.network.* events are not yet in the SDK Event type union
      // (pending SDK regeneration). Handle them via string comparison.
      if ((event.type as string).startsWith("session.network.")) {
        return (event.properties as { sessionID: string }).sessionID
      }
      return undefined
  }
}
