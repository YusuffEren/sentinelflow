"use client"

import { useEffect, useState, useCallback } from "react"
import { Header } from "@/components/layout/header"
import { useWebSocket } from "@/hooks/use-websocket"
import { useAuth } from "@/contexts/auth-context"
import {
  FolderKanban,
  RefreshCw,
  ChevronRight,
  Clock,
  User,
  AlertTriangle,
  CheckCircle,
  XCircle,
  Plus,
} from "lucide-react"
import { cn } from "@/lib/utils"

interface Case {
  case_id: string
  title: string
  description: string
  status: string
  priority: string
  primary_fraud_type: string | null
  alert_count: number
  total_amount: number
  max_severity: string
  assigned_to: string | null
  created_at: string
  updated_at: string
  sla_breached: boolean
}

interface CasesResponse {
  total: number
  page: number
  page_size: number
  cases: Case[]
}

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000"

const statusConfig: Record<string, { label: string; color: string; icon: any }> = {
  new: { label: "Yeni", color: "bg-blue-500/10 text-blue-400 border-blue-500/20", icon: AlertTriangle },
  triage: { label: "Triage", color: "bg-yellow-500/10 text-yellow-400 border-yellow-500/20", icon: AlertTriangle },
  investigating: { label: "İnceleniyor", color: "bg-orange-500/10 text-orange-400 border-orange-500/20", icon: Clock },
  escalated: { label: "Üst Seviye", color: "bg-red-500/10 text-red-400 border-red-500/20", icon: AlertTriangle },
  resolved_true_positive: { label: "Doğru Tespit", color: "bg-emerald-500/10 text-emerald-400 border-emerald-500/20", icon: CheckCircle },
  resolved_false_positive: { label: "Yanlış Alarm", color: "bg-zinc-500/10 text-zinc-400 border-zinc-500/20", icon: XCircle },
  closed: { label: "Kapatıldı", color: "bg-zinc-500/10 text-zinc-400 border-zinc-500/20", icon: CheckCircle },
}

const priorityColors: Record<string, string> = {
  P1: "bg-red-500/20 text-red-400",
  P2: "bg-orange-500/20 text-orange-400",
  P3: "bg-yellow-500/20 text-yellow-400",
  P4: "bg-zinc-500/20 text-zinc-400",
}

export default function CasesPage() {
  const { isConnected } = useWebSocket()
  const { user } = useAuth()
  
  const [cases, setCases] = useState<Case[]>([])
  const [loading, setLoading] = useState(true)
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [pageSize] = useState(20)
  
  // Filters
  const [statusFilter, setStatusFilter] = useState<string | null>(null)
  const [priorityFilter, setPriorityFilter] = useState<string | null>(null)
  
  // Stats
  const [stats, setStats] = useState<any>(null)
  
  const fetchCases = useCallback(async () => {
    setLoading(true)
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        page_size: pageSize.toString(),
      })
      
      if (statusFilter) params.append("status", statusFilter)
      if (priorityFilter) params.append("priority", priorityFilter)
      
      const res = await fetch(`${API_BASE}/api/v1/cases?${params}`)
      if (res.ok) {
        const data: CasesResponse = await res.json()
        setCases(data.cases)
        setTotal(data.total)
      }
    } catch (e) {
      console.error("Failed to fetch cases", e)
    } finally {
      setLoading(false)
    }
  }, [page, pageSize, statusFilter, priorityFilter])
  
  const fetchStats = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/v1/cases/stats`)
      if (res.ok) {
        setStats(await res.json())
      }
    } catch (e) {
      console.error("Failed to fetch stats", e)
    }
  }, [])
  
  useEffect(() => {
    fetchCases()
    fetchStats()
  }, [fetchCases, fetchStats])
  
  const totalPages = Math.ceil(total / pageSize)
  
  return (
    <div className="h-screen flex flex-col bg-[#09090B]">
      <Header isConnected={isConnected} />
      
      <main className="flex-1 p-6 overflow-hidden">
        <div className="h-full flex flex-col gap-4">
          {/* Title and actions */}
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <FolderKanban className="h-6 w-6 text-blue-400" />
              <h1 className="text-2xl font-bold text-white">Vakalar</h1>
              <span className="px-2 py-0.5 text-sm bg-zinc-800 text-zinc-400 rounded-full">
                {total} toplam
              </span>
            </div>
            
            <div className="flex items-center gap-3">
              {/* Status filter */}
              <select
                value={statusFilter || ""}
                onChange={(e) => setStatusFilter(e.target.value || null)}
                className="bg-zinc-900 border border-zinc-800 text-zinc-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500"
              >
                <option value="">Tüm Durumlar</option>
                <option value="new">Yeni</option>
                <option value="triage">Triage</option>
                <option value="investigating">İnceleniyor</option>
                <option value="escalated">Üst Seviye</option>
                <option value="resolved_true_positive">Doğru Tespit</option>
                <option value="resolved_false_positive">Yanlış Alarm</option>
                <option value="closed">Kapatıldı</option>
              </select>
              
              {/* Priority filter */}
              <select
                value={priorityFilter || ""}
                onChange={(e) => setPriorityFilter(e.target.value || null)}
                className="bg-zinc-900 border border-zinc-800 text-zinc-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500"
              >
                <option value="">Tüm Öncelikler</option>
                <option value="P1">P1 - Critical</option>
                <option value="P2">P2 - High</option>
                <option value="P3">P3 - Medium</option>
                <option value="P4">P4 - Low</option>
              </select>
              
              <button
                onClick={fetchCases}
                className="p-2 bg-zinc-900 border border-zinc-800 rounded-lg text-zinc-400 hover:text-white transition-colors"
              >
                <RefreshCw className={cn("h-5 w-5", loading && "animate-spin")} />
              </button>
            </div>
          </div>
          
          {/* Stats row */}
          {stats && (
            <div className="grid grid-cols-4 gap-4">
              <div className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-4">
                <p className="text-sm text-zinc-500">Toplam</p>
                <p className="text-2xl font-bold text-white">{stats.total}</p>
              </div>
              <div className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-4">
                <p className="text-sm text-zinc-500">Açık</p>
                <p className="text-2xl font-bold text-yellow-400">{stats.open}</p>
              </div>
              <div className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-4">
                <p className="text-sm text-zinc-500">Çözümlenen</p>
                <p className="text-2xl font-bold text-emerald-400">{stats.closed}</p>
              </div>
              <div className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-4">
                <p className="text-sm text-zinc-500">Çözüm Oranı</p>
                <p className="text-2xl font-bold text-blue-400">
                  {stats.total > 0 ? ((stats.closed / stats.total) * 100).toFixed(0) : 0}%
                </p>
              </div>
            </div>
          )}
          
          {/* Cases list */}
          <div className="flex-1 bg-zinc-900/50 border border-zinc-800 rounded-xl overflow-hidden flex flex-col">
            <div className="flex-1 overflow-auto">
              {loading && cases.length === 0 ? (
                <div className="flex items-center justify-center h-full">
                  <RefreshCw className="h-8 w-8 text-zinc-600 animate-spin" />
                </div>
              ) : cases.length === 0 ? (
                <div className="flex flex-col items-center justify-center h-full text-zinc-500">
                  <FolderKanban className="h-12 w-12 mb-3 opacity-50" />
                  <p>Vaka bulunamadı</p>
                </div>
              ) : (
                <div className="divide-y divide-zinc-800">
                  {cases.map((caseItem) => {
                    const statusInfo = statusConfig[caseItem.status] || statusConfig.new
                    const StatusIcon = statusInfo.icon
                    
                    return (
                      <div
                        key={caseItem.case_id}
                        className="p-4 hover:bg-zinc-800/50 transition-colors cursor-pointer"
                      >
                        <div className="flex items-start justify-between gap-4">
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2 mb-1">
                              <span className={cn(
                                "px-2 py-0.5 text-xs font-medium rounded",
                                priorityColors[caseItem.priority]
                              )}>
                                {caseItem.priority}
                              </span>
                              <span className={cn(
                                "px-2 py-0.5 text-xs font-medium rounded border flex items-center gap-1",
                                statusInfo.color
                              )}>
                                <StatusIcon className="w-3 h-3" />
                                {statusInfo.label}
                              </span>
                              {caseItem.sla_breached && (
                                <span className="px-2 py-0.5 text-xs font-medium rounded bg-red-500/20 text-red-400">
                                  SLA İhlali
                                </span>
                              )}
                            </div>
                            
                            <h3 className="text-sm font-medium text-white truncate">
                              {caseItem.title}
                            </h3>
                            
                            <div className="flex items-center gap-4 mt-2 text-xs text-zinc-500">
                              <span className="font-mono">{caseItem.case_id}</span>
                              <span>{caseItem.alert_count} alarm</span>
                              <span className="font-mono">
                                {caseItem.total_amount.toLocaleString("tr-TR")} TRY
                              </span>
                              {caseItem.assigned_to && (
                                <span className="flex items-center gap-1">
                                  <User className="w-3 h-3" />
                                  {caseItem.assigned_to}
                                </span>
                              )}
                              <span className="flex items-center gap-1">
                                <Clock className="w-3 h-3" />
                                {new Date(caseItem.created_at).toLocaleDateString("tr-TR")}
                              </span>
                            </div>
                          </div>
                          
                          <ChevronRight className="w-5 h-5 text-zinc-600 flex-shrink-0" />
                        </div>
                      </div>
                    )
                  })}
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
        </div>
      </main>
    </div>
  )
}
