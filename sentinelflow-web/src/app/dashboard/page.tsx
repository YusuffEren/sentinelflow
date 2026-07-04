"use client"

// =============================================================================
// SentinelFlow — SOC Dashboard (canli izleme ekrani)
// =============================================================================
// Landing sayfasinin tasarim diliyle tutarli. Backend kapaliyken "demo mode"
// ile sahte verilerle dolu, canli hissi veren bir ekran. Backend acildiginda
// gercek WebSocket/REST verisine gecer.
// =============================================================================

import { useEffect, useMemo, useState } from "react"
import dynamic from "next/dynamic"
import {
  Activity,
  ShieldAlert,
  CreditCard,
  Gauge,
  ArrowLeft,
  CircleDot,
} from "lucide-react"
import Link from "next/link"
import { AnimatePresence, motion } from "motion/react"

import { useWebSocket } from "@/hooks/use-websocket"
import { config, getApiUrl } from "@/lib/config"
import { cn } from "@/lib/utils"
import { fraudLabel, type Alert3D } from "@/components/3d/types"
import {
  FAUX_ALERTS,
  FAUX_ALERTS_EXTRA,
  SEVERITY_META,
  type FauxAlert,
} from "@/lib/landing-data"
import { NetworkGraph } from "@/components/landing/NetworkGraph"

const GlobeScene = dynamic(() => import("@/components/3d/GlobeScene"), {
  ssr: false,
  loading: () => <PanelLoader label="Kure yukleniyor..." />,
})

function PanelLoader({ label }: { label: string }) {
  return (
    <div className="w-full h-full flex items-center justify-center">
      <div className="flex flex-col items-center gap-3">
        <div className="w-7 h-7 border-2 border-signal/30 border-t-signal rounded-full animate-spin" />
        <span className="text-[10px] font-mono text-muted tracking-wider uppercase">
          {label}
        </span>
      </div>
    </div>
  )
}

// --- Demo veri ureteci (backend yoksa) ---------------------------------------
function useDemoData(live: boolean) {
  const [alerts, setAlerts] = useState<FauxAlert[]>(FAUX_ALERTS.slice(0, 8))
  const [stats, setStats] = useState({
    transactions_processed: 8421,
    fraud_detected: 23,
    uptime_seconds: 1274,
  })

  useEffect(() => {
    if (live) return
    const prefersReduced = window.matchMedia(
      "(prefers-reduced-motion: reduce)",
    ).matches
    if (prefersReduced) return

    const tick = () => {
      const pool = [...FAUX_ALERTS, ...FAUX_ALERTS_EXTRA]
      const next = pool[Math.floor(Math.random() * pool.length)]
      const stamped: FauxAlert = {
        ...next,
        id: `ALERT-${Math.random().toString(16).slice(2, 8)}`,
        ts: new Date().toLocaleTimeString("en-GB", { hour12: false }),
      }
      setAlerts((prev) => [stamped, ...prev].slice(0, 12))
      setStats((s) => ({
        transactions_processed:
          s.transactions_processed + Math.floor(Math.random() * 40 + 10),
        fraud_detected: s.fraud_detected + (Math.random() > 0.7 ? 1 : 0),
        uptime_seconds: s.uptime_seconds + 2,
      }))
    }
    const iv = setInterval(tick, 2400)
    return () => clearInterval(iv)
  }, [live])

  return { alerts, stats }
}

// --- Stat karti (landing temasiyla uyumlu) ----------------------------------
function StatCard({
  title,
  value,
  icon: Icon,
  accent,
  sub,
}: {
  title: string
  value: string | number
  icon: React.ComponentType<{ className?: string }>
  accent: "signal" | "alarm"
  sub: string
}) {
  const isPlaceholder = value === "---"
  return (
    <div className="rounded-lg border border-line bg-base-2/60 backdrop-blur-sm px-4 py-3.5">
      <div className="flex items-center justify-between mb-2.5">
        <span className="text-[10px] font-mono uppercase tracking-wider text-muted">
          {title}
        </span>
        <Icon
          className={cn(
            "w-3.5 h-3.5",
            accent === "alarm" ? "text-alarm" : "text-signal",
          )}
        />
      </div>
      <div
        className={cn(
          "font-mono tabular-nums text-xl font-semibold",
          isPlaceholder
            ? "text-muted/50"
            : accent === "alarm"
              ? "text-alarm"
              : "text-ink",
        )}
      >
        {value}
      </div>
      <div className="mt-1.5 text-[10px] text-muted/70">{sub}</div>
    </div>
  )
}

// --- Alert satiri -----------------------------------------------------------
function AlertRow({ a }: { a: FauxAlert }) {
  const sev = SEVERITY_META[a.severity]
  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: -10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.3 }}
      className="grid grid-cols-[auto_1fr_auto] gap-3 items-center px-3 py-2.5 rounded-md hover:bg-signal/5 transition-colors border-b border-line/50"
    >
      <div className="flex items-center gap-2 min-w-0">
        <span
          className="w-1.5 h-1.5 rounded-full flex-shrink-0"
          style={{ background: sev.color, boxShadow: `0 0 6px ${sev.color}` }}
        />
        <span className="text-[10px] font-mono text-muted tabular-nums">
          {a.ts}
        </span>
      </div>
      <div className="min-w-0">
        <div className="flex items-center gap-2">
          <span style={{ color: sev.color }} className="text-[11px] font-mono">
            {a.id}
          </span>
          <span className="text-[10px] text-muted/70 font-mono">
            {a.engine}
          </span>
        </div>
        <div className="text-[11px] text-ink/80 truncate font-mono mt-0.5">
          {a.detail}
        </div>
      </div>
      <span
        className="font-mono text-[12px] tabular-nums flex-shrink-0"
        style={{ color: sev.color }}
      >
        {a.amount}
      </span>
    </motion.div>
  )
}

const MOTORS = [
  { n: "Circular Ring", t: "Neo4j" },
  { n: "Impossible Travel", t: "Redis Geo" },
  { n: "NLP Blacklist", t: "scikit-learn" },
  { n: "AI Anomaly", t: "IsolationForest" },
]

export default function DashboardPage() {
  const { isConnected, alerts: wsAlerts } = useWebSocket()
  const isLive = isConnected
  const demo = useDemoData(isLive)

  const [stats, setStats] = useState({
    transactions_processed: 0,
    fraud_detected: 0,
    fraud_rate: 0,
    uptime_seconds: 0,
  })
  const [backendUp, setBackendUp] = useState(false)

  useEffect(() => {
    let alive = true
    const fetchStats = async () => {
      try {
        const res = await fetch(getApiUrl(config.endpoints.stats))
        if (res.ok && alive) {
          setStats(await res.json())
          setBackendUp(true)
        }
      } catch {
        if (alive) setBackendUp(false)
      }
    }
    fetchStats()
    const iv = setInterval(fetchStats, config.ui.statsRefreshInterval)
    return () => {
      alive = false
      clearInterval(iv)
    }
  }, [])

  const txValue = backendUp
    ? stats.transactions_processed.toLocaleString()
    : demo.stats.transactions_processed.toLocaleString()
  const fraudValue = backendUp
    ? stats.fraud_detected.toLocaleString()
    : demo.stats.fraud_detected.toLocaleString()
  const uptimeValue = backendUp
    ? `${Math.floor(stats.uptime_seconds / 60)}m`
    : `${Math.floor(demo.stats.uptime_seconds / 60)}m`

  const alertList: FauxAlert[] = useMemo(() => {
    if (isLive && wsAlerts.length > 0) {
      return wsAlerts.slice(0, 12).map((a: Alert3D) => {
        const sev = String(a.severity).toLowerCase()
        const valid: "critical" | "high" | "medium" = [
          "critical",
          "high",
          "medium",
        ].includes(sev)
          ? (sev as "critical" | "high" | "medium")
          : "medium"
        return {
          id: a.alert_id,
          ts: new Date(a.detected_at).toLocaleTimeString("en-GB", {
            hour12: false,
          }),
          engine: a.fraud_type,
          severity: valid,
          detail: a.description || fraudLabel(a.fraud_type),
          amount: new Intl.NumberFormat("tr-TR", {
            style: "currency",
            currency: a.currency || "TRY",
            maximumFractionDigits: 0,
          }).format(a.amount ?? 0),
        }
      })
    }
    return demo.alerts
  }, [isLive, wsAlerts, demo.alerts])

  return (
    <div className="min-h-screen flex flex-col bg-base text-ink">
      <DashboardHeader
        isLive={isLive}
        backendUp={backendUp}
      />
      <DashboardBody
        txValue={txValue}
        fraudValue={fraudValue}
        uptimeValue={uptimeValue}
        backendUp={backendUp}
        alertList={alertList}
        motors={MOTORS}
      />
    </div>
  )
}

function DashboardHeader({
  isLive,
  backendUp,
}: {
  isLive: boolean
  backendUp: boolean
}) {
  return (
    <header className="sticky top-0 z-40 h-14 border-b border-line bg-base/80 backdrop-blur-md flex items-center justify-between px-5">
      <div className="flex items-center gap-4">
        <Link
          href="/"
          className="flex items-center gap-2 text-muted hover:text-signal transition-colors text-[13px]"
        >
          <ArrowLeft className="w-4 h-4" />
          Landing
        </Link>
        <span className="h-4 w-px bg-line" />
        <span className="font-display font-semibold text-[14px]">
          Sentinel<span className="text-signal">Flow</span>
        </span>
        <span className="text-[10px] font-mono text-muted/60 uppercase tracking-wider">
          {"// soc console"}
        </span>
      </div>
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2 px-2.5 py-1 rounded-md border border-line bg-base-2/60">
          <span
            className={cn(
              "w-1.5 h-1.5 rounded-full",
              isLive
                ? "bg-signal animate-subtle-pulse"
                : "bg-amber animate-subtle-pulse",
            )}
          />
          <span className="text-[10px] font-mono uppercase tracking-wider text-muted">
            {isLive ? "live" : backendUp ? "rest" : "demo"}
          </span>
        </div>
        <Link
          href="/alerts"
          className="text-[12px] text-muted hover:text-ink transition-colors"
        >
          Alarmlar
        </Link>
        <Link
          href="/cases"
          className="text-[12px] text-muted hover:text-ink transition-colors"
        >
          Vakalar
        </Link>
      </div>
    </header>
  )
}

function DashboardBody({
  txValue,
  fraudValue,
  uptimeValue,
  backendUp,
  alertList,
  motors,
}: {
  txValue: string
  fraudValue: string
  uptimeValue: string
  backendUp: boolean
  alertList: FauxAlert[]
  motors: { n: string; t: string }[]
}) {
  return (
    <main className="flex-1 overflow-y-auto">
      <GraphStatsSection
        txValue={txValue}
        fraudValue={fraudValue}
        uptimeValue={uptimeValue}
        backendUp={backendUp}
        motors={motors}
      />
      <AlertConsoleSection alertList={alertList} />
    </main>
  )
}

function GraphStatsSection({
  txValue,
  fraudValue,
  uptimeValue,
  backendUp,
  motors,
}: {
  txValue: string
  fraudValue: string
  uptimeValue: string
  backendUp: boolean
  motors: { n: string; t: string }[]
}) {
  return (
    <section className="relative grid grid-cols-1 lg:grid-cols-3 gap-px bg-line border-b border-line">
      <div className="lg:col-span-2 relative h-[340px] sm:h-[400px] bg-base-2/40 overflow-hidden">
        <NetworkGraph
          className="absolute inset-0 w-full h-full opacity-90"
          nodeCount={14}
          ringIntervalMs={4800}
        />
        <div className="absolute inset-0 bg-gradient-to-t from-base via-transparent to-transparent" />
        <div className="absolute top-4 left-4 flex items-center gap-2">
          <CircleDot className="w-3.5 h-3.5 text-signal" />
          <span className="text-[11px] font-mono uppercase tracking-wider text-signal">
            transaction graph {"// ring detection live"}
          </span>
        </div>
        <div className="absolute top-4 right-4 px-2.5 py-1 rounded-md border border-line bg-base/70 backdrop-blur-sm">
          <span className="text-[9px] font-mono text-muted/70 uppercase tracking-wider">
            kirmizi = aklama halkasi
          </span>
        </div>
        <div className="absolute bottom-4 left-4 right-4 flex items-end justify-between">
          <div>
            <div className="text-[10px] font-mono text-muted/70 uppercase tracking-wider mb-1">
              aktif dugum / kenar
            </div>
            <div className="font-mono tabular-nums text-2xl text-ink">
              <span className="text-signal">128</span>
              <span className="text-muted/50 mx-1.5">/</span>
              <span className="text-ink/80">342</span>
            </div>
          </div>
          <div className="text-right">
            <div className="text-[10px] font-mono text-muted/70 uppercase tracking-wider mb-1">
              ring tespiti
            </div>
            <div className="font-mono tabular-nums text-2xl text-alarm">3</div>
          </div>
        </div>
      </div>

      <div className="bg-base-2/40 p-4 grid grid-cols-2 gap-3 content-start">
        <StatCard
          title="Islem"
          value={txValue}
          icon={CreditCard}
          accent="signal"
          sub={backendUp ? "islendi" : "demo akisi"}
        />
        <StatCard
          title="Dolandiricilik"
          value={fraudValue}
          icon={ShieldAlert}
          accent="alarm"
          sub={backendUp ? "engellendi" : "tespit edildi"}
        />
        <StatCard
          title="Dogruluk"
          value="99.4%"
          icon={Gauge}
          accent="signal"
          sub="model guveni"
        />
        <StatCard
          title="Calisma"
          value={uptimeValue}
          icon={Activity}
          accent="signal"
          sub="sistem sagligi"
        />

        <div className="col-span-2 rounded-lg border border-line bg-base/60 p-3">
          <div className="text-[10px] font-mono uppercase tracking-wider text-muted mb-2.5">
            tespit motorlari
          </div>
          <div className="space-y-1.5">
            {motors.map((m) => (
              <div
                key={m.n}
                className="flex items-center justify-between text-[11px]"
              >
                <div className="flex items-center gap-2">
                  <span className="w-1.5 h-1.5 rounded-full bg-signal animate-subtle-pulse" />
                  <span className="text-ink/90">{m.n}</span>
                </div>
                <span className="font-mono text-muted text-[10px]">{m.t}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}

function AlertConsoleSection({ alertList }: { alertList: FauxAlert[] }) {
  return (
    <section className="grid grid-cols-1 lg:grid-cols-5 gap-px bg-line border-b border-line">
      <div className="lg:col-span-3 bg-base-2/30 p-4">
        <div className="rounded-lg border border-line bg-[#080b11] overflow-hidden">
          <div className="flex items-center justify-between px-4 h-9 border-b border-line bg-base-2/80">
            <div className="flex items-center gap-2">
              <span className="w-1.5 h-1.5 rounded-full bg-signal animate-subtle-pulse" />
              <span className="text-[10px] font-mono uppercase tracking-wider text-muted">
                live_alerts {"// ws://sentinelflow"}
              </span>
            </div>
            <span className="text-[10px] font-mono text-signal">
              {alertList.length} aktif
            </span>
          </div>
          <div className="px-2 py-2 h-[420px] overflow-y-auto">
            <AnimatePresence initial={false}>
              {alertList.map((a) => (
                <AlertRow key={a.id} a={a} />
              ))}
            </AnimatePresence>
          </div>
        </div>
      </div>

      <div className="lg:col-span-2 bg-base-2/30 p-4 flex flex-col gap-4">
        <div className="rounded-lg border border-line bg-base/60 overflow-hidden flex-1 min-h-[260px] relative">
          <div className="flex items-center justify-between px-4 h-9 border-b border-line">
            <span className="text-[10px] font-mono uppercase tracking-wider text-muted">
              cografri aktivite {"// kaydir + zoomla"}
            </span>
            <div className="flex items-center gap-2">
              <span className="w-1.5 h-1.5 rounded-full bg-signal animate-subtle-pulse" />
              <span className="text-[10px] font-mono text-signal">live</span>
            </div>
          </div>
          <div className="h-[calc(100%-36px)] relative">
            <GlobeScene />
          </div>
          {/* lejant */}
          <div className="absolute bottom-2 left-2 flex flex-col gap-1 px-2.5 py-2 rounded-md border border-line bg-base/80 backdrop-blur-sm">
            <div className="flex items-center gap-1.5 text-[9px] font-mono">
              <span className="w-2 h-0.5 bg-signal/40" />
              <span className="text-muted">normal transfer</span>
            </div>
            <div className="flex items-center gap-1.5 text-[9px] font-mono">
              <span className="w-2 h-0.5 bg-amber/60" />
              <span className="text-muted">supheli</span>
            </div>
            <div className="flex items-center gap-1.5 text-[9px] font-mono">
              <span className="w-2 h-0.5 bg-alarm animate-subtle-pulse" />
              <span className="text-alarm">imkansiz seyahat</span>
            </div>
          </div>
        </div>

        <div className="rounded-lg border border-line bg-base/60 p-3">
          <div className="text-[10px] font-mono uppercase tracking-wider text-muted mb-2.5">
            siddet dagilimi
          </div>
          <div className="space-y-2">
            {(["critical", "high", "medium"] as const).map((s) => {
              const sev = SEVERITY_META[s]
              const count = alertList.filter(
                (a) => a.severity === s,
              ).length
              const pct = alertList.length
                ? Math.round((count / alertList.length) * 100)
                : 0
              return (
                <div key={s}>
                  <div className="flex items-center justify-between text-[11px] mb-1">
                    <span style={{ color: sev.color }}>{sev.label}</span>
                    <span className="font-mono text-muted">{count}</span>
                  </div>
                  <div className="h-1.5 rounded-full bg-line overflow-hidden">
                    <div
                      className="h-full rounded-full transition-all duration-500"
                      style={{
                        width: `${pct}%`,
                        background: sev.color,
                        boxShadow: `0 0 8px ${sev.color}80`,
                      }}
                    />
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      </div>
    </section>
  )
}
