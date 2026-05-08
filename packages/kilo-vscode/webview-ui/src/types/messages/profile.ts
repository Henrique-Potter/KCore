// Kilo notification types (mirrored from kilo-gateway)
export interface KilocodeNotificationAction {
  actionText: string
  actionURL: string
}

export interface KilocodeNotification {
  id: string
  title: string
  message: string
  action?: KilocodeNotificationAction
  showIn?: string[]
  suggestModelId?: string
}

// Profile types from kilo-gateway
export interface KilocodeBalance {
  balance: number
}

export interface ProfileData {
  profile: {
    email: string
    name?: string
    organizations?: Array<{ id: string; name: string; role: string }>
  }
  balance: KilocodeBalance | null
  currentOrgId: string | null
}

export type DeviceAuthStatus = "idle" | "initiating" | "pending" | "success" | "error" | "cancelled"

export interface DeviceAuthState {
  status: DeviceAuthStatus
  code?: string
  verificationUrl?: string
  expiresIn?: number
  error?: string
}

export interface CloudSessionInfo {
  session_id: string
  title?: string
  created_at: string
  updated_at: string
}
