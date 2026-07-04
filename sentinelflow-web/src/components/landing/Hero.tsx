"use client"

// =============================================================================
// Hero — açılışta canlı çalışan minyatür graf/akış animasyonu (arka planda
// işlem noktaları akar, ara sıra biri "flagged" olup kırmızıya döner).
// Başlık sistemin ne yaptığını tek cümlede söyler. Altında canlı sayaçlar
// (işlem/sn, tespit gecikmesi, aktif motor sayısı) count-up ile dolar.
// =============================================================================

import { motion } from "motion/react"
import { ArrowRight, Activity, Timer, Cpu } from "lucide-react"
import Link from "next/link"
import { NetworkGraph } from "./NetworkGraph"
import { CountUp } from "./CountUp"
import { REPO_URL } from "@/lib/landing-data"

const HERO_STATS = [
  {
    icon: Activity,
    value: 10000,
    suffix: "",
    prefix: "",
    label: "İşlem / sn",
    accent: "signal" as const,
  },
  {
    icon: Timer,
    value: 100,
    suffix: "ms",
    prefix: "<",
    label: "Tespit gecikmesi",
    accent: "signal" as const,
  },
  {
    icon: Cpu,
    value: 4,
    suffix: "",
    prefix: "",
    label: "Aktif tespit motoru",
    accent: "alarm" as const,
  },
]

export function Hero() {
  return (
    <section className="relative min-h-[100svh] flex items-center overflow-hidden pt-14">
      {/* Canlı graf arka planı */}
      <div className="absolute inset-0">
        <NetworkGraph
          className="absolute inset-0 w-full h-full opacity-70"
          nodeCount={18}
          ringIntervalMs={5200}
        />
        {/* okunabilirlik için üst gradyan */}
        <div className="absolute inset-0 bg-gradient-to-b from-base/70 via-base/40 to-base" />
        <div className="absolute inset-0 bg-gradient-to-r from-base/80 via-transparent to-base/60" />
      </div>

      <div className="relative mx-auto max-w-7xl px-5 sm:px-8 w-full">
        <div className="max-w-3xl">
          {/* durum rozeti */}
          <motion.div
            initial={{ opacity: 0, y: 12 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.15 }}
            className="inline-flex items-center gap-2 px-3 py-1 rounded-full border border-signal/25 bg-signal/5 backdrop-blur-sm"
          >
            <span className="w-1.5 h-1.5 rounded-full bg-signal animate-subtle-pulse" />
            <span className="text-[11px] font-mono tracking-wider text-signal uppercase">
              SOC · canlı izleme aktif
            </span>
          </motion.div>

          {/* başlık */}
          <motion.h1
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.22 }}
            className="mt-6 font-display font-semibold tracking-tight text-[clamp(2.5rem,7vw,4.75rem)] leading-[1.02] text-ink"
          >
            Para hareket ederken
            <br />
            <span className="text-signal">dolandırıcılığı</span> yakalar.
          </motion.h1>

          {/* alt başlık */}
          <motion.p
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.34 }}
            className="mt-5 max-w-xl text-[15px] sm:text-base text-muted leading-relaxed"
          >
            Kafka üzerinden akan işlemleri; Neo4j graf veritabanıyla dairesel
            aklama zincirleri, Redis geo-spatial ile imkansız seyahat, NLP ile
            şüpheli kelimeler ve Isolation Forest ile istatistiksel anormallikler
            için sub-100ms gecikmeyle tarar.
          </motion.p>

          {/* CTA */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.46 }}
            className="mt-8 flex flex-wrap items-center gap-3"
          >
            <a
              href="#kurulum"
              className="group inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-signal text-base font-medium text-[#04141a] hover:bg-signal-soft transition-colors"
            >
              Hızlı başla
              <ArrowRight className="w-4 h-4 group-hover:translate-x-0.5 transition-transform" />
            </a>
            <a
              href="#motorlar"
              className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg border border-line bg-base-2/50 text-ink hover:border-signal/40 hover:text-signal transition-colors"
            >
              Tespit motorları
            </a>
            <Link
              href="/dashboard"
              className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg text-muted hover:text-ink transition-colors text-sm"
            >
              Canlı dashboard →
            </Link>
          </motion.div>

          {/* canlı sayaçlar */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.58 }}
            className="mt-12 grid grid-cols-3 gap-px overflow-hidden rounded-xl border border-line bg-line"
          >
            {HERO_STATS.map((s) => (
              <div
                key={s.label}
                className="bg-base-2/80 backdrop-blur-sm px-4 py-4 sm:px-5 sm:py-5"
              >
                <div className="flex items-center gap-1.5 text-[11px] uppercase tracking-wider text-muted">
                  <s.icon
                    className={`w-3 h-3 ${s.accent === "alarm" ? "text-alarm" : "text-signal"}`}
                  />
                  {s.label}
                </div>
                <div
                  className={`mt-2 font-mono tabular-nums text-2xl sm:text-3xl font-semibold ${s.accent === "alarm" ? "text-alarm" : "text-ink"}`}
                >
                  <CountUp
                    value={s.value}
                    prefix={s.prefix}
                    suffix={s.suffix}
                    duration={1.8}
                  />
                </div>
              </div>
            ))}
          </motion.div>
        </div>
      </div>

      {/* GitHub köşe rozeti */}
      <div className="absolute bottom-6 right-6 hidden sm:block">
        <a
          href={REPO_URL}
          target="_blank"
          rel="noreferrer"
          className="font-mono text-[11px] text-muted/70 hover:text-signal transition-colors"
        >
          github.com/YusuffEren/sentinelflow
        </a>
      </div>
    </section>
  )
}
