export interface Alert3D {
  alert_id: string
  fraud_type: string
  severity: "critical" | "high" | "medium" | "low" | string
  description: string
  amount: number
  currency?: string
  detected_at: string
  is_dismissed?: boolean
  case_id?: string | null
  title?: string
  confidence?: number
  sender_iban?: string
  receiver_iban?: string
  transaction_id?: string
  sender_city?: string
  receiver_city?: string
  sender_name?: string
  receiver_name?: string
}

export interface BarDatum {
  label: string
  value: number
}

export interface RingDatum {
  label: string
  value: number
  color: string
}

export const SEVERITY_COLOR: Record<string, string> = {
  critical: "#ff3333",
  high: "#ff6600",
  medium: "#ffaa00",
  low: "#00ff88",
  info: "#00f0ff",
}

export function fraudLabel(type: string): string {
  const labels: Record<string, string> = {
    circular_ring: "Circular Ring",
    impossible_travel: "Impossible Travel",
    blacklist_keyword: "Blacklist Keyword",
    ml_ensemble: "ML Ensemble",
    mule_account: "Mule Account",
    high_value_anomaly: "High Value",
    none: "Normal",
  }
  return labels[type] ?? type.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
}
