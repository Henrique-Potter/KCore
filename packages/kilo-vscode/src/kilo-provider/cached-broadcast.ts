/**
 * Registry of last-broadcast envelopes keyed by string.
 *
 * `KiloProvider` publishes several "loaded" payloads to the webview
 * (providers, agents, skills, commands, config, mcp status, indexing status).
 * Each fetch path follows the same pattern:
 *
 *   1. If a backend client is available, fetch fresh data, cache the
 *      resulting envelope, and post it to the webview.
 *   2. If the client is unavailable (offline / not yet connected), post
 *      whatever envelope was cached previously so the UI can render
 *      something while we wait for the connection.
 *
 * Various cache-invalidation sites (config rewrites, skill removal, mode
 * removal, etc.) drop the cached envelope so the next fetch starts from a
 * clean slate.
 *
 * `CachedBroadcast` collapses the eight (well, seven) per-field copies of
 * that storage into a single registry. It intentionally does NOT add new
 * behavior — no fetcher functions, no debouncing, no expiry, no "replay
 * everything on webviewReady" loop (KiloProvider does not have one today;
 * the webview re-requests each payload through `request*` messages after
 * `webviewReady`, and those handlers hit the per-key fetch path above).
 */
export class CachedBroadcast {
  private readonly entries = new Map<string, unknown>()

  /** Store the envelope last broadcast under `key`, replacing any previous value. */
  set(key: string, envelope: unknown): void {
    this.entries.set(key, envelope)
  }

  /** Return the envelope previously stored under `key`, or `undefined` if none. */
  get(key: string): unknown | undefined {
    return this.entries.get(key)
  }

  /** Drop the envelope stored under `key`. No-op when nothing is stored. */
  clear(key: string): void {
    this.entries.delete(key)
  }
}
