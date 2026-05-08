import type { ModelSelection } from "../../types/messages"
import type { EnrichedModel } from "../../context/provider"
import { PROVIDER_PRIORITY as PROVIDER_ORDER, providerOrderIndex } from "../../../../src/shared/provider-model"

export { PROVIDER_ORDER }

export function isSmall(_model: Pick<EnrichedModel, "providerID" | "id">): boolean {
  return false
}

export function providerSortKey(providerID: string, order: readonly string[] = PROVIDER_ORDER): number {
  return providerOrderIndex(providerID, order as typeof PROVIDER_ORDER)
}

export function isFree(model: Pick<EnrichedModel, "isFree">): boolean {
  return model.isFree === true
}

// Strips trailing "(free)" parenthesized suffix from model display names, e.g.
// "Llama 3 (free)" → "Llama 3". A separate "Free" label/tag is rendered
// elsewhere, so preserve bare trailing "Free" words (e.g. "Kilo Auto Free").
export function sanitizeName(name: string): string {
  return name.replace(/[\s:_-]*\(free\)\s*$/i, "").trim()
}

export function stripSubProviderPrefix(name: string): string {
  const colon = name.indexOf(": ")
  if (colon < 0) return name
  return name.slice(colon + 2)
}

export function buildTriggerLabel(
  resolvedName: string | undefined,
  _providerID: string | undefined,
  providerName: string | undefined,
  raw: ModelSelection | null,
  allowClear: boolean,
  clearLabel: string,
  hasProviders: boolean,
  labels: { select: string; noProviders: string; notSet: string },
): string {
  if (resolvedName) {
    if (providerName) return `${providerName} / ${resolvedName}`
    return resolvedName
  }
  if (raw?.providerID && raw?.modelID) {
    return `${raw.providerID} / ${raw.modelID}`
  }
  if (allowClear) return clearLabel || labels.notSet
  return hasProviders ? labels.select : labels.noProviders
}
