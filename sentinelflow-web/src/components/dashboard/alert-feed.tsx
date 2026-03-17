"use client"

import { useEffect, useRef } from "react"
import { AlertTriangle, Zap, Globe, Link2, Clock } from "lucide-react"

interface Alert {
  alert_id: string
  fraud_type: string
  severity: "low" | "medium" | "high" | "critical"
  confidence: number
  description?: string
  detected_at: string
  amount: number
  sender_iban?: string
  receiver_iban?: string
}

interface AlertFeedProps {
  alerts: Alert[]
}

const fraudTypeConfig: Record<string, { icon: typeof AlertTriangle; label: string }> = {
  circular_ring: { icon: Link2, label: "Circular Ring" },
  velocity_anomaly: { icon: Zap, label: "Velocity" },
  geo_anomaly: { icon: Globe, label: "Geo Anomaly" },
  amount_anomaly: { icon: AlertTriangle, label: "Amount" },
  default: { icon: AlertTriangle, label: "Anomaly" },
}

const severityConfig = {
  critical: { dot: "bg-red-500", bg: "bg-red-500/5", border: "border-l-red-500" },
  high: { dot: "bg-red-400", bg: "bg-red-400/5", border: "border-l-red-400" },
  medium: { dot: "bg-amber-400", bg: "bg-amber-400/5", border: "border-l-amber-400" },
  low: { dot: "bg-zinc-400", bg: "bg-transparent", border: "border-l-zinc-600" },
}

function formatTime(dateString: string) {
  const date = new Date(dateString)
  const now = new Date()
  const diff = Math.floor((now.getTime() - date.getTime()) / 1000)

  if (diff < 60) return "Just now"
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`
  return date.toLocaleDateString()
}

function formatAmount(amount: number) {
  return new Intl.NumberFormat("tr-TR", {
    style: "currency",
    currency: "TRY",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(amount)
}

export function AlertFeed({ alerts }: AlertFeedProps) {
  const containerRef = useRef<HTMLDivElement>(null)

  return (
    <div className="h-full flex flex-col bg-zinc-900 border border-zinc-800 rounded-lg overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800">
        <div className="flex items-center gap-2">
          <h3 className="text-sm font-medium text-zinc-100">Recent Alerts</h3>
          {alerts.length > 0 && (
            <span className="px-1.5 py-0.5 text-[10px] font-medium bg-red-500/10 text-red-400 rounded">
              {alerts.length}
            </span>
          )}
        </div>
        <div className="flex items-center gap-1.5 text-zinc-500">
          <div className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-subtle-pulse" />
          <span className="text-[10px] font-medium uppercase tracking-wider">Live</span>
        </div>
      </div>

      {/* Alert List */}
      <div 
        ref={containerRef}
        className="flex-1 overflow-y-auto"
      >
        {alerts.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-zinc-500 py-12">
            <Clock className="w-8 h-8 mb-3 opacity-50" />
            <p className="text-sm">No alerts yet</p>
            <p className="text-xs text-zinc-600 mt-1">Monitoring transactions...</p>
          </div>
        ) : (
          <div className="divide-y divide-zinc-800/50">
            {alerts.map((alert, index) => {
              const config = fraudTypeConfig[alert.fraud_type] || fraudTypeConfig.default
              const severity = severityConfig[alert.severity] || severityConfig.low
              const Icon = config.icon

              return (
                <div
                  key={alert.alert_id}
                  className={`
                    group px-4 py-3 border-l-2 ${severity.border}
                    hover:bg-zinc-800/30 transition-colors duration-150
                    ${index === 0 ? "animate-slide-in" : ""}
                  `}
                >
                  {/* Top Row */}
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex items-center gap-2 min-w-0">
                      <div className={`w-1.5 h-1.5 rounded-full ${severity.dot} flex-shrink-0`} />
                      <div className="flex items-center gap-1.5 min-w-0">
                        <Icon className="w-3.5 h-3.5 text-zinc-400 flex-shrink-0" />
                        <span className="text-sm font-medium text-zinc-200 truncate">
                          {config.label}
                        </span>
                      </div>
                    </div>
                    <span className="text-sm font-mono font-medium text-zinc-100 flex-shrink-0">
                      {formatAmount(alert.amount)}
                    </span>
                  </div>

                  {/* Bottom Row */}
                  <div className="flex items-center justify-between mt-2 pl-3.5">
                    <span className="text-xs text-zinc-500">
                      {formatTime(alert.detected_at)}
                    </span>
                    <span className="text-[10px] font-medium text-zinc-500 bg-zinc-800 px-1.5 py-0.5 rounded">
                      {Math.round(alert.confidence * 100)}%
                    </span>
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}
