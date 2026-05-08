/**
 * Parse the port number from sidecar startup output.
 * Matches lines like: "kilo server listening on http://127.0.0.1:12345"
 * (Bun and pre-contract-version Rust) and the extended Rust form:
 * "kilo server listening on http://127.0.0.1:12345 (contract=1)".
 * Returns the port number or null if not found.
 */
export function parseServerPort(output: string): number | null {
  const match = output.match(/listening on http:\/\/[\w.]+:(\d+)/)
  if (!match) return null
  return parseInt(match[1]!, 10)
}

/**
 * Parse the numeric contract version from the readiness line, if present.
 * Returns null for sidecars that don't announce a contract version (Bun
 * and the early Rust preview), which the caller must treat as
 * "compatible by default" — Bun is the oracle.
 *
 * Highest contract version this extension build supports. Sidecars
 * announcing a version above this are refused with a typed error so
 * users see an upgrade prompt instead of a confusing run-time failure.
 */
export const MAX_SUPPORTED_CONTRACT_VERSION = 1

export function parseContractVersion(output: string): number | null {
  const match = output.match(/listening on http:\/\/[\w.]+:\d+\s+\(contract=(\d+)\)/)
  if (!match) return null
  const version = parseInt(match[1]!, 10)
  return Number.isFinite(version) ? version : null
}

/** Returns true if the extension can talk to a sidecar with this contract version. */
export function isContractVersionCompatible(version: number | null): boolean {
  if (version === null) {
    // Older sidecar (Bun / pre-versioned Rust) — assume compatible. Bun is the oracle.
    return true
  }
  return version <= MAX_SUPPORTED_CONTRACT_VERSION
}
