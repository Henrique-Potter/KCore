import { useDialog } from "@kilocode/kilo-ui/context/dialog"
import { Dialog } from "@kilocode/kilo-ui/dialog"
import { List } from "@kilocode/kilo-ui/list"
import { ProviderIcon } from "@kilocode/kilo-ui/provider-icon"
import { Tag } from "@kilocode/kilo-ui/tag"
import { Show, createMemo } from "solid-js"
import { useLanguage } from "../../context/language"
import { useProvider } from "../../context/provider"
import ProviderConnectDialog from "./ProviderConnectDialog"
import { providerIcon } from "./provider-catalog"

type ProviderItem = {
  id: string
  name: string
}

const ProviderSelectDialog = () => {
  const dialog = useDialog()
  const provider = useProvider()
  const language = useLanguage()

  const items = createMemo<ProviderItem[]>(() => {
    language.locale()

    const all = Object.values(provider.providers())
    const available = all.filter((item) => item.id === "openai")

    return available.map((item) => ({
      id: item.id,
      name: item.name,
    }))
  })

  function open(item: ProviderItem) {
    dialog.show(() => <ProviderConnectDialog providerID={item.id} oauthOnly />)
  }

  return (
    <Dialog title={language.t("command.provider.connect")} size="large" transition>
      <List<ProviderItem>
        search={{ placeholder: language.t("dialog.provider.search.placeholder"), autofocus: true }}
        emptyMessage={language.t("dialog.provider.empty")}
        activeIcon="plus-small"
        key={(item) => item.id}
        items={items()}
        filterKeys={["id", "name"]}
        groupBy={() => language.t("dialog.provider.group.popular")}
        sortBy={(a, b) => a.name.localeCompare(b.name)}
        onSelect={(item) => {
          if (!item) return
          open(item)
        }}
      >
        {(item) => (
          <div style={{ display: "flex", gap: "10px", "align-items": "center", width: "100%", "min-width": 0 }}>
            <ProviderIcon id={providerIcon(item.id)} width={18} height={18} data-slot="list-item-extra-icon" />
            <div
              style={{
                display: "flex",
                gap: "8px",
                "align-items": "center",
                "min-width": 0,
                flex: 1,
                "flex-wrap": "wrap",
              }}
            >
              <span style={{ "font-size": "14px", "line-height": "20px", color: "var(--vscode-foreground)" }}>
                {item.name}
              </span>
              <Show when={item.id === "openai"}>
                <Tag>{language.t("settings.providers.tag.chatgpt")}</Tag>
              </Show>
            </div>
          </div>
        )}
      </List>
    </Dialog>
  )
}

export default ProviderSelectDialog
