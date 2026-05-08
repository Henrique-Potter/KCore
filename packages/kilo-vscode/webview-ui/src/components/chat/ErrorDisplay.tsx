import { Component, createMemo } from "solid-js"
import { Card } from "@kilocode/kilo-ui/card"
import { Collapsible } from "@kilocode/kilo-ui/collapsible"
import { ErrorDetails } from "@kilocode/kilo-ui/error-details"
import type { AssistantMessage } from "@kilocode/sdk/v2"
import { useLanguage } from "../../context/language"
import { unwrapError } from "../../utils/errorUtils"

export interface ErrorDisplayProps {
  error: NonNullable<AssistantMessage["error"]>
}

export const ErrorDisplay: Component<ErrorDisplayProps> = (props) => {
  const { t } = useLanguage()

  const errorText = createMemo(() => {
    const msg = props.error.data?.message
    if (typeof msg === "string") return unwrapError(msg)
    if (msg === undefined || msg === null) return ""
    return unwrapError(String(msg))
  })

  return (
    <Card variant="error" class="error-card">
      {errorText()}
      <Collapsible variant="ghost">
        <Collapsible.Trigger class="error-details-trigger">
          <span>{t("error.details.show")}</span>
          <Collapsible.Arrow />
        </Collapsible.Trigger>
        <Collapsible.Content>
          <ErrorDetails error={props.error} />
        </Collapsible.Content>
      </Collapsible>
    </Card>
  )
}
