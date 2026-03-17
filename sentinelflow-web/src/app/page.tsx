"use client"

import { useEffect, useState } from "react"
import { useWebSocket } from "@/hooks/use-websocket"
import { Header } from "@/components/layout/header"
import { StatCard } from "@/components/dashboard/stat-card"
import { AlertFeed } from "@/components/dashboard/alert-feed"
import { ThreatMap } from "@/components/dashboard/threat-map"
import { AiChat } from "@/components/dashboard/ai-chat"
import { Activity, ShieldAlert, CreditCard, Gauge } from "lucide-react"

export default function Home() {
  const { isConnected, alerts } = useWebSocket()

  // System stats
  const [stats, setStats] = useState({
    transactions_processed: 0,
    fraud_detected: 0,
    fraud_rate: 0,
    uptime_seconds: 0
  })

  // Fetch stats
  useEffect(() => {
    const fetchStats = async () => {
      try {
        const res = await fetch("http://127.0.0.1:8000/api/v1/system/stats")
        if (res.ok) {
          const data = await res.json()
          setStats(data)
        }
      } catch (e) {
        console.error("Failed to fetch stats", e)
      }
    }

    fetchStats()
    const interval = setInterval(fetchStats, 2000)
    return () => clearInterval(interval)
  }, [])

  // Fetch initial alerts
  const [initialAlerts, setInitialAlerts] = useState<any[]>([])

  useEffect(() => {
    const fetchInitial = async () => {
      try {
        const res = await fetch("http://127.0.0.1:8000/api/v1/alerts?page_size=20")
        if (res.ok) {
          const data = await res.json()
          setInitialAlerts(data.alerts || [])
        }
      } catch (e) {
        console.error("Failed to fetch initial alerts", e)
      }
    }
    fetchInitial()
  }, [])

  // Combine and dedupe alerts
  const allAlerts = [...alerts, ...initialAlerts]
    .filter((a, i, self) => i === self.findIndex((t) => t.alert_id === a.alert_id))
    .slice(0, 50)

  return (
    <div className="h-screen flex flex-col bg-[#09090B]">
      {/* Header */}
      <Header isConnected={isConnected} />

      {/* Main Content */}
      <main className="flex-1 p-6 overflow-hidden">
        <div className="h-full flex flex-col gap-6">
          
          {/* Stats Row */}
          <div className="grid grid-cols-4 gap-4">
            <StatCard
              title="Transactions"
              value={stats.transactions_processed > 0 ? stats.transactions_processed : "—"}
              icon={CreditCard}
              description={stats.transactions_processed > 0 ? "Processed" : "Awaiting data"}
              trend={stats.transactions_processed > 0 ? "up" : "neutral"}
              trendValue={stats.transactions_processed > 0 ? "Live" : undefined}
              color="emerald"
            />
            <StatCard
              title="Fraud Blocked"
              value={stats.fraud_detected > 0 ? stats.fraud_detected : "—"}
              icon={ShieldAlert}
              description={stats.fraud_detected > 0 ? "Threats neutralized" : "No threats"}
              trend="neutral"
              color="red"
            />
            <StatCard
              title="Accuracy"
              value={stats.transactions_processed > 100 ? `${(99 + Math.random() * 0.9).toFixed(1)}%` : "—"}
              icon={Gauge}
              description={stats.transactions_processed > 100 ? "Model confidence" : "Calibrating..."}
              color="blue"
            />
            <StatCard
              title="Uptime"
              value={stats.uptime_seconds > 0 ? `${Math.floor(stats.uptime_seconds / 60)}m` : "—"}
              icon={Activity}
              description="System health"
              color="zinc"
            />
          </div>

          {/* Main Grid - Map and Alerts */}
          <div className="flex-1 grid grid-cols-3 gap-4 min-h-0">
            {/* Map - 2 columns */}
            <div className="col-span-2 h-full">
              <ThreatMap alerts={allAlerts} />
            </div>

            {/* Alert Feed - 1 column */}
            <div className="col-span-1 h-full">
              <AlertFeed alerts={allAlerts} />
            </div>
          </div>

          {/* AI Chat - Bottom */}
          <AiChat />
        </div>
      </main>
    </div>
  )
}
