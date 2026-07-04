"use client"

// =============================================================================
// TechStack — sade bir ızgara, hover'da her biri kendi rengiyle hafif parlar.
// =============================================================================

import { motion } from "motion/react"
import { TECH_STACK } from "@/lib/landing-data"

export function TechStack() {
  return (
    <section className="relative py-24 sm:py-28">
      <div className="mx-auto max-w-7xl px-5 sm:px-8">
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true, margin: "-80px" }}
          transition={{ duration: 0.5 }}
          className="max-w-2xl"
        >
          <span className="font-mono text-[11px] uppercase tracking-[0.25em] text-signal">
            {"// Teknoloji Yigini"}
          </span>
          <h2 className="mt-3 font-display text-3xl sm:text-4xl font-semibold tracking-tight text-ink">
            Doğru iş için doğru araç.
          </h2>
          <p className="mt-4 text-muted leading-relaxed">
            Her katman, çözdüğü probleme en uygun teknolojiyle inşa edildi —
            graf döngüleri için graf veritabanı, coğrafi hız için geo-spatial
            önbellek, istatistiksel aykırılık için denetimsiz model.
          </p>
        </motion.div>

        <div className="mt-12 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
          {TECH_STACK.map((t, i) => (
            <motion.div
              key={t.name}
              initial={{ opacity: 0, y: 14 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, margin: "-60px" }}
              transition={{ duration: 0.4, delay: i * 0.05 }}
              className="group relative rounded-lg border border-line bg-base-2/40 px-4 py-4 transition-colors hover:bg-base-2/70"
              style={
                {
                  "--glow": t.color,
                } as React.CSSProperties
              }
            >
              <div
                className="absolute inset-0 rounded-lg opacity-0 group-hover:opacity-100 transition-opacity duration-300 pointer-events-none"
                style={{ boxShadow: `inset 0 0 0 1px ${t.color}40, 0 0 28px -10px ${t.color}80` }}
              />
              <div className="relative">
                <div
                  className="w-2 h-2 rounded-full mb-3 transition-transform group-hover:scale-125"
                  style={{ background: t.color, boxShadow: `0 0 8px ${t.color}` }}
                />
                <div className="text-[14px] font-medium text-ink">{t.name}</div>
                <div className="text-[11px] font-mono text-muted uppercase tracking-wider mt-0.5">
                  {t.role}
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  )
}
