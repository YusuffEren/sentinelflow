"use client"

import { useEffect, useState, useCallback } from "react"
import { Header } from "@/components/layout/header"
import { useWebSocket } from "@/hooks/use-websocket"
import { 
  ShieldAlert, 
  Filter, 
  RefreshCw, 
  ChevronDown,
  ChevronRight,
  AlertTriangle,
  Clock,
  MapPin,
  TrendingUp,
  Eye,
  XCircle,
  Link as LinkIcon
} from "lucide-react"
import { cn } from "@/lib/utils"

interface Alert {
  alert_id: string
  fraud_type: string
  severity: string
  confidence: number
  transaction_id: string
  sender_iban: string
  sender_name: string
  sender_city: string
  receiver_iban: string
  receiver_name: string
  receiver_city: string
  amount: number
  currency: string
  title: string
  description: string
  detected_at: string
  is_dismissed: boolean
  case_id: string | null
}

interface AlertsResponse {
  total: number
  page: number
  page_size: number
  alerts: Alert[]
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000"

const severityColors: Record<string, string> = {
  low: "bg-emerald-500/10 text-emerald-400 border-emerald-500/20",
  medium: "bg-yellow-500/10 text-yellow-400 border-yellow-500/20",
  high: "bg-orange-500/10 text-orange-400 border-orange-500/20",
  critical: "bg-red-500/10 text-red-400 border-red-500/20",
}

const fraudTypeLabels: Record<string, string> = {
  circular_ring: "Döngüsel Transfer",
  impossible_travel: "İmkansız Seyahat",
  blacklist_keyword: "Kara Liste",
  mule_account: "Mule Hesap",
  structuring: "Yapılandırma",
  velocity_anomaly: "Hız Anomalisi",
  ml_ensemble: "ML Tespit",
  compliance_violation: "Uyum İhlali",
}

export default function AlertsPage() {
  const { isConnected, alerts: wsAlerts } = useWebSocket()
  
  const [alerts, setAlerts] = useState<Alert[]>([])
  const [loading, setLoading] = useState(true)
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [pageSize] = useState(20)
  
  // Filters
  const [severityFilter, setSeverityFilter] = useState<string | null>(null)
  const [fraudTypeFilter, setFraudTypeFilter] = useState<string | null>(null)
  
  // Selected alert for detail view
  const [selectedAlert, setSelectedAlert] = useState<Alert | null>(null)
  
  const fetchAlerts = useCallback(async () => {
    setLoading(true)
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        page_size: pageSize.toString(),
      })
      
      if (severityFilter) params.append("severity", severityFilter)
      if (fraudTypeFilter) params.append("fraud_type", fraudTypeFilter)
      
      const res = await fetch(`${API_BASE}/api/v1/alerts?${params}`)
      if (res.ok) {
        const data: AlertsResponse = await res.json()
        setAlerts(data.alerts)
        setTotal(data.total)
      }
    } catch (e) {
      console.error("Failed to fetch alerts", e)
    } finally {
      setLoading(false)
    }
  }, [page, pageSize, severityFilter, fraudTypeFilter])
  
  useEffect(() => {
    fetchAlerts()
  }, [fetchAlerts])
  
  // Merge WebSocket alerts
  useEffect(() => {
    if (wsAlerts.length > 0 && page === 1) {
      const newAlerts = wsAlerts.filter(
        (wa) => !alerts.find((a) => a.alert_id === wa.alert_id)
      )
      if (newAlerts.length > 0) {
        setAlerts((prev) => [...newAlerts, ...prev].slice(0, pageSize))
        setTotal((prev) => prev + newAlerts.length)
      }
    }
  }, [wsAlerts, page, alerts, pageSize])
  
  const handleDismiss = async (alertId: string) => {
    try {
      const res = await fetch(
        `${API_BASE}/api/v1/alerts/${alertId}/dismiss`,
        { method: "POST" }
      )
      if (res.ok) {
        setAlerts((prev) =>
          prev.map((a) =>
            a.alert_id === alertId ? { ...a, is_dismissed: true } : a
          )
        )
        if (selectedAlert?.alert_id === alertId) {
          setSelectedAlert({ ...selectedAlert, is_dismissed: true })
        }
      }
    } catch (e) {
      console.error("Failed to dismiss alert", e)
    }
  }
  
  const totalPages = Math.ceil(total / pageSize)
  
  return (
    <div className="h-screen flex flex-col bg-[#09090B]">
      <Header isConnected={isConnected} />
      
      <main className="flex-1 p-6 overflow-hidden">
        <div className="h-full flex flex-col gap-4">
          {/* Title and actions */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <ShieldAlert className="h-6 w-6 text-red-400" />
              <h1 className="text-2xl font-bold text-white">Alarmlar</h1>
              <span className="px-2 py-0.5 text-sm bg-zinc-800 text-zinc-400 rounded-full">
                {total} toplam
              </span>
            </div>
            
            <div className="flex items-center gap-3">
              {/* Severity filter */}
              <select
                value={severityFilter || ""}
                onChange={(e) => setSeverityFilter(e.target.value || null)}
                className="bg-zinc-900 border border-zinc-800 text-zinc-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500"
              >
                <option value="">Tüm Seviyeler</option>
                <option value="critical">Critical</option>
                <option value="high">High</option>
                <option value="medium">Medium</option>
                <option value="low">Low</option>
              </select>
              
              {/* Fraud type filter */}
              <select
                value={fraudTypeFilter || ""}
                onChange={(e) => setFraudTypeFilter(e.target.value || null)}
                className="bg-zinc-900 border border-zinc-800 text-zinc-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500"
              >
                <option value="">Tüm Tipler</option>
                {Object.entries(fraudTypeLabels).map(([key, label]) => (
                  <option key={key} value={key}>{label}</option>
                ))}
              </select>
              
              <button
                onClick={fetchAlerts}
                className="p-2 bg-zinc-900 border border-zinc-800 rounded-lg text-zinc-400 hover:text-white transition-colors"
              >
                <RefreshCw className={cn("h-5 w-5", loading && "animate-spin")} />
              </button>
            </div>
          </div>
          
          {/* Content */}
          <div className="flex-1 flex gap-4 min-h-0">
            {/* Alert list */}
            <div className="flex-1 bg-zinc-900/50 border border-zinc-800 rounded-xl overflow-hidden flex flex-col">
              <div className="flex-1 overflow-auto">
                {loading && alerts.length === 0 ? (
                  <div className="flex items-center justify-center h-full">
                    <RefreshCw className="h-8 w-8 text-zinc-600 animate-spin" />
                  </div>
                ) : alerts.length === 0 ? (
                  <div className="flex flex-col items-center justify-center h-full text-zinc-500">
                    <ShieldAlert className="h-12 w-12 mb-3 opacity-50" />
                    <p>Alarm bulunamadı</p>
                  </div>
                ) : (
                  <div className="divide-y divide-zinc-800">
                    {alerts.map((alert) => (
                      <div
                        key={alert.alert_id}
                        onClick={() => setSelectedAlert(alert)}
                        className={cn(
                          "p-4 cursor-pointer transition-colors hover:bg-zinc-800/50",
                          selectedAlert?.alert_id === alert.alert_id && "bg-zinc-800/80",
                          alert.is_dismissed && "opacity-50"
                        )}
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-1">
                              <span className={cn(
                                "px-2 py-0.5 text-xs font-medium rounded border",
                                severityColors[alert.severity]
                              )}>
                                {alert.severity.toUpperCase()}
                              </span>
                              <span className="text-xs text-zinc-500">
                                {fraudTypeLabels[alert.fraud_type] || alert.fraud_type}
                              </span>
                              {alert.is_dismissed && (
                                <span className="text-xs text-zinc-600">
                                  (Dismissed)
                                </span>
                              )}
                            </div>
                            
                            <p className="text-sm text-white font-medium truncate">
                              {alert.sender_name} → {alert.receiver_name}
                            </p>
                            
                            <div className="flex items-center gap-4 mt-1 text-xs text-zinc-500">
                              <span className="flex items-center gap-1">
                                <TrendingUp className="h-3 w-3" />
                                {(alert.confidence * 100).toFixed(0)}%
                              </span>
                              <span className="font-mono">
                                {alert.amount.toLocaleString("tr-TR")} {alert.currency}
                              </span>
                              <span className="flex items-center gap-1">
                                <Clock className="h-3 w-3" />
                                {new Date(alert.detected_at).toLocaleTimeString("tr-TR")}
                              </span>
                            </div>
                          </div>
                          
                          <ChevronRight className="h-5 w-5 text-zinc-600 flex-shrink-0" />
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
              
              {/* Pagination */}
              {totalPages > 1 && (
                <div className="flex items-center justify-between px-4 py-3 border-t border-zinc-800 bg-zinc-900/80">
                  <span className="text-sm text-zinc-500">
                    Sayfa {page} / {totalPages}
                  </span>
                  <div className="flex gap-2">
                    <button
                      onClick={() => setPage((p) => Math.max(1, p - 1))}
                      disabled={page === 1}
                      className="px-3 py-1.5 text-sm bg-zinc-800 text-zinc-300 rounded-lg disabled:opacity-50"
                    >
                      Önceki
                    </button>
                    <button
                      onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                      disabled={page === totalPages}
                      className="px-3 py-1.5 text-sm bg-zinc-800 text-zinc-300 rounded-lg disabled:opacity-50"
                    >
                      Sonraki
                    </button>
                  </div>
                </div>
              )}
            </div>
            
            {/* Alert detail panel */}
            <div className="w-96 bg-zinc-900/50 border border-zinc-800 rounded-xl p-4 overflow-auto">
              {selectedAlert ? (
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <h2 className="text-lg font-bold text-white">Alarm Detayı</h2>
                    <span className={cn(
                      "px-2 py-0.5 text-xs font-medium rounded border",
                      severityColors[selectedAlert.severity]
                    )}>
                      {selectedAlert.severity.toUpperCase()}
                    </span>
                  </div>
                  
                  <div className="space-y-3">
                    <div>
                      <label className="text-xs text-zinc-500 block">Alarm ID</label>
                      <p className="text-sm text-zinc-300 font-mono">{selectedAlert.alert_id}</p>
                    </div>
                    
                    <div>
                      <label className="text-xs text-zinc-500 block">Tip</label>
                      <p className="text-sm text-white">
                        {fraudTypeLabels[selectedAlert.fraud_type] || selectedAlert.fraud_type}
                      </p>
                    </div>
                    
                    <div>
                      <label className="text-xs text-zinc-500 block">Güven Skoru</label>
                      <div className="flex items-center gap-2">
                        <div className="flex-1 h-2 bg-zinc-800 rounded-full overflow-hidden">
                          <div 
                            className="h-full bg-gradient-to-r from-yellow-500 to-red-500"
                            style={{ width: `${selectedAlert.confidence * 100}%` }}
                          />
                        </div>
                        <span className="text-sm text-white font-mono">
                          {(selectedAlert.confidence * 100).toFixed(1)}%
                        </span>
                      </div>
                    </div>
                    
                    <hr className="border-zinc-800" />
                    
                    <div>
                      <label className="text-xs text-zinc-500 block">Gönderen</label>
                      <p className="text-sm text-white">{selectedAlert.sender_name}</p>
                      <p className="text-xs text-zinc-500 font-mono">{selectedAlert.sender_iban}</p>
                      {selectedAlert.sender_city && (
                        <p className="text-xs text-zinc-500 flex items-center gap-1 mt-1">
                          <MapPin className="h-3 w-3" />
                          {selectedAlert.sender_city}
                        </p>
                      )}
                    </div>
                    
                    <div>
                      <label className="text-xs text-zinc-500 block">Alıcı</label>
                      <p className="text-sm text-white">{selectedAlert.receiver_name}</p>
                      <p className="text-xs text-zinc-500 font-mono">{selectedAlert.receiver_iban}</p>
                      {selectedAlert.receiver_city && (
                        <p className="text-xs text-zinc-500 flex items-center gap-1 mt-1">
                          <MapPin className="h-3 w-3" />
                          {selectedAlert.receiver_city}
                        </p>
                      )}
                    </div>
                    
                    <div>
                      <label className="text-xs text-zinc-500 block">Tutar</label>
                      <p className="text-xl font-bold text-white">
                        {selectedAlert.amount.toLocaleString("tr-TR")} {selectedAlert.currency}
                      </p>
                    </div>
                    
                    <div>
                      <label className="text-xs text-zinc-500 block">Tespit Zamanı</label>
                      <p className="text-sm text-zinc-300">
                        {new Date(selectedAlert.detected_at).toLocaleString("tr-TR")}
                      </p>
                    </div>
                    
                    {selectedAlert.description && (
                      <div>
                        <label className="text-xs text-zinc-500 block">Açıklama</label>
                        <p className="text-sm text-zinc-400">{selectedAlert.description}</p>
                      </div>
                    )}
                    
                    <hr className="border-zinc-800" />
                    
                    {/* Actions */}
                    <div className="flex flex-col gap-2">
                      {!selectedAlert.is_dismissed && (
                        <button
                          onClick={() => handleDismiss(selectedAlert.alert_id)}
                          className="flex items-center justify-center gap-2 w-full py-2 bg-zinc-800 text-zinc-300 rounded-lg hover:bg-zinc-700 transition-colors"
                        >
                          <XCircle className="h-4 w-4" />
                          Reddet (False Positive)
                        </button>
                      )}
                      
                      {!selectedAlert.case_id && (
                        <button className="flex items-center justify-center gap-2 w-full py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-500 transition-colors">
                          <LinkIcon className="h-4 w-4" />
                          Vaka Oluştur
                        </button>
                      )}
                      
                      {selectedAlert.case_id && (
                        <p className="text-center text-sm text-zinc-500">
                          Bağlı Vaka: {selectedAlert.case_id}
                        </p>
                      )}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="flex flex-col items-center justify-center h-full text-zinc-500">
                  <Eye className="h-12 w-12 mb-3 opacity-50" />
                  <p>Detay görmek için alarm seçin</p>
                </div>
              )}
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}
