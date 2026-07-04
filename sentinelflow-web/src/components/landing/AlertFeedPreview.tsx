"use client"

// =============================================================================
// AlertFeedPreview — sahte ama akan bir "alert feed". Zaman damgalı uyarı
// satırları periyodik olarak tepeden düşer (slide-in), eski satırlar kayar.
// Gerçek SOC dashboard hissini önizler.
// =============================================================================

import { useEffect, useState } from "react"
import { AnimatePresence, motion } from "motion/react"
import {
  FAUX_ALERTS,
  FAUX_ALERTS_EXTRA,
  SEVERITY_META,
  type FauxAlert,
} from "@/lib/landing-data"

const POOL: FauxAlert[] = [...FAUX_ALERTS_EXTRA, ...FAUX_ALERTS]
const MAX_VISIBLE = 8

function fmtTs(base: Date, tick: number): string {
  const d = new Date(base.getTime() + tick * 2200)
  return d.toLocaleTimeString("en-GB", { hour12: false }) + "." + String(d.getMilliseconds()).padStart(3, "0").slice(0, 3)
}

export function AlertFeedPreview() {
  const [feed, setFeed] = useState<FauxAlert[]>(() => FAUX_ALERTS.slice(0, 6))
  const [tick, setTick] = useState(0)

  useEffect(() => {
    const prefersReduced =
      typeof window !== "undefined" &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches
    if (prefersReduced) return
    const iv = setInterval(() => {
      setTick((t) => t + 1)
      setFeed((prev) => {
        const next = POOL[Math.floor(Math.random() * POOL.length)]
        const stamped: FauxAlert = {
          ...next,
          id: `ALERT-${Math.random().toString(16).slice(2, 8)}`,
          ts: fmtTs(new Date(0), tick + 1),
        }
        return [stamped, ...prev].slice(0, MAX_VISIBLE)
      })
    }, 2200)
    return () => clearInterval(iv)
  }, [tick])

  return (
    <section className="relative py-24 sm:py-32 border-y border-line bg-base-2/30">
      <div className="mx-auto max-w-7xl px-5 sm:px-8">
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-8 lg:gap-10 items-start">
          {/* Sol: açıklama */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true, margin: "-80px" }}
            transition={{ duration: 0.5 }}
            className="lg:col-span-5"
          >
            <span className="font-mono text-[11px] uppercase tracking-[0.25em] text-signal">
              {"// Canlı Gösterim"}
            </span>
            <h2 className="mt-3 font-display text-3xl sm:text-4xl font-semibold tracking-tight text-ink">
              Bir operasyon merkezi
              <span className="text-signal"> camından.</span>
            </h2>
            <p className="mt-4 text-muted leading-relaxed">
              Her uyarı; motor adı, güven skoru, IBAN zinciri ve tutarıyla birlikte
              milisaniye zaman damgalı olarak düşer. Aşağıdaki akış canlı sistemin
              birebir önizlemesidir — satırlar gerçek hızda gelir.
            </p>

            {/* lejant */}
            <div className="mt-6 flex flex-wrap gap-4">
              {(["critical", "high", "medium"] as const).map((s) => (
                <div key={s} className="flex items-center gap-2">
                  <span
                    className="w-2 h-2 rounded-full"
                    style={{ background: SEVERITY_META[s].color }}
                  />
                  <span className="text-[11px] font-mono uppercase tracking-wider text-muted">
                    {SEVERITY_META[s].label}
                  </span>
                </div>
              ))}
            </div>
          </motion.div>

          {/* Sağ: alert konsolu */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true, margin: "-80px" }}
            transition={{ duration: 0.5, delay: 0.1 }}
            className="lg:col-span-7"
          >
            <div className="rounded-xl border border-line bg-[#080b11] overflow-hidden shadow-2xl shadow-black/40">
              {/* konsol başlığı */}
              <div className="flex items-center justify-between px-4 h-9 border-b border-line bg-base-2/80">
                <div className="flex items-center gap-2">
                  <span className="w-1.5 h-1.5 rounded-full bg-signal animate-subtle-pulse" />
                  <span className="text-[11px] font-mono text-muted uppercase tracking-wider">
                    live_alerts // ws://sentinelflow
                  </span>
                </div>
                <span className="text-[10px] font-mono text-signal">
                  streaming
                </span>
              </div>

              {/* başlık satırı */}
              <div className="grid grid-cols-[1fr_auto] gap-2 px-4 py-2 border-b border-line text-[10px] font-mono uppercase tracking-wider text-muted/70">
                <span>alert_id · engine · detail</span>
                <span>amount</span>
              </div>

              {/* akış */}
              <div className="px-2 py-2 h-[340px] overflow-hidden">
                <AnimatePresence initial={false}>
                  {feed.map((a) => {
                    const sev = SEVERITY_META[a.severity]
                    return (
                      <motion.div
                        key={a.id}
                        layout
                        initial={{ opacity: 0, y: -16, height: 0 }}
                        animate={{ opacity: 1, y: 0, height: "auto" }}
                        exit={{ opacity: 0 }}
                        transition={{ duration: 0.4, ease: "easeOut" }}
                        className="grid grid-cols-[1fr_auto] gap-2 items-center px-2 py-2 rounded-md hover:bg-signal/5 transition-colors"
                      >
                        <div className="min-w-0">
                          <div className="flex items-center gap-2 text-[11px] font-mono">
                            <span
                              className="w-1.5 h-1.5 rounded-full flex-shrink-0"
                              style={{
                                background: sev.color,
                                boxShadow: `0 0 6px ${sev.color}`,
                              }}
                            />
                            <span className="text-muted">{a.ts}</span>
                            <span style={{ color: sev.color }}>{a.id}</span>
                            <span className="text-muted/60 hidden sm:inline">
                              {a.engine}
                            </span>
                          </div>
                          <div className="mt-0.5 text-[12px] font-mono text-ink/80 truncate">
                            {a.detail}
                          </div>
                        </div>
                        <div
                          className="font-mono text-[12px] tabular-nums flex-shrink-0"
                          style={{ color: sev.color }}
                        >
                          {a.amount}
                        </div>
                      </motion.div>
                    )
                  })}
                </AnimatePresence>
              </div>
            </div>
          </motion.div>
        </div>
      </div>
    </section>
  )
}
