/**
 * HistoryView component
 * Panel for local session history.
 */

import { Component } from "solid-js"
import { Button } from "@kilocode/kilo-ui/button"
import { useLanguage } from "../../context/language"
import SessionList from "./SessionList"

interface HistoryViewProps {
  onSelectSession: (id: string) => void
  onBack?: () => void
}

const HistoryView: Component<HistoryViewProps> = (props) => {
  const language = useLanguage()

  return (
    <div class="history-view">
      <div class="history-view-header">
        <Button variant="ghost" size="small" icon="arrow-left" onClick={() => props.onBack?.()}>
          {language.t("common.goBack")}
        </Button>
      </div>

      <div class="history-view-content">
        <SessionList onSelectSession={props.onSelectSession} />
      </div>
    </div>
  )
}

export default HistoryView
