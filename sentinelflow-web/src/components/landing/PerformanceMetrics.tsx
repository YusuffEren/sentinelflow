"use client"

// =============================================================================
// PerformanceMetrics — 10.000 tx/sn, <100ms tespit, <50ms graf sorgu gibi
// rakamları büyük tipografiyle, scroll'da count-up ile gösterir.
// =============================================================================

import { motion } from "motion/react"
import { METRICS } from "@/lib/landing-data"
import { CountUp } from "./CountUp"

export function PerformanceMetrics() {
  return (
    <section
      id="metrikler"
      className="relative py-24 sm:py-32 border-y border-line bg-base-2/30"
    >
      {/* arka planda ince sinyal izi */}
      <div className="absolute inset-0 pointer-events-none opacity-[0.04]">
        <div
          className="absolute inset-0"
          style={{
            backgroundImage:
              "linear-gradient(to right, #00e5c7 1px, transparent 1px)",
            backgroundSize: "48px 100%",
          }}
        />
      </div>

      <div className="relative mx-auto max-w-7xl px-5 sm:px-8">
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true, margin: "-80px" }}
          transition={{ duration: 0.5 }}
          className="max-w-2xl"
        >
          <span className="font-mono text-[11px] uppercase tracking-[0.25em] text-signal">
            {"// Performans"}
          </span>
          <h2 className="mt-3 font-display text-3xl sm:text-4xl font-semibold tracking-tight text-ink">
            İnsan refleksinden hızlı.
          </h2>
          <p className="mt-4 text-muted leading-relaxed">
            İşlemler Kafka topic&apos;ine düştükten sonra bir uyarının dashboard&apos;a
            düşmesine kadar geçen süre genellikle bir göz kırpmadan kısadır.
          </p>
        </motion.div>

        <div className="mt-14 grid grid-cols-2 lg:grid-cols-4 gap-px overflow-hidden rounded-xl border border-line bg-line">
          {METRICS.map((m, i) => (
            <motion.div
              key={m.label}
              initial={{ opacity: 0, y: 14 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, margin: "-60px" }}
              transition={{ duration: 0.5, delay: i * 0.08 }}
              className="bg-base-2/80 px-5 py-8 sm:px-6 sm:py-10 text-center sm:text-left"
            >
              <div className="font-display font-semibold tabular-nums text-4xl sm:text-5xl text-ink leading-none">
                <span
                  className={
                    m.label.includes("gecikme") || m.label.includes("sorgu")
                      ? "text-signal"
                      : ""
                  }
                >
                  <CountUp
                    value={m.value}
                    prefix={m.prefix}
                    suffix={m.suffix}
                    duration={1.8}
                  />
                </span>
              </div>
              <div className="mt-3 text-[12px] sm:text-[13px] text-muted">
                {m.label}
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  )
}
