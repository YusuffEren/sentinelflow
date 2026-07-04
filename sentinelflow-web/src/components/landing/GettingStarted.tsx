"use client"

// =============================================================================
// GettingStarted — kurulum komutlarını gerçek bir terminal bileşeninde gösterir.
// Terminal bileşeni (typewriter + kopyala) yanında adım numaralı rehber.
// Numaralı adımlar burada kullanılır çünkü gerçekten sıralı bir süreç var.
// =============================================================================

import { motion } from "motion/react"
import { Terminal } from "./Terminal"
import { SETUP_STEPS } from "@/lib/landing-data"

export function GettingStarted() {
  return (
    <section id="kurulum" className="relative py-24 sm:py-32">
      <div className="mx-auto max-w-7xl px-5 sm:px-8">
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true, margin: "-80px" }}
          transition={{ duration: 0.5 }}
          className="max-w-2xl"
        >
          <span className="font-mono text-[11px] uppercase tracking-[0.25em] text-signal">
            {"// Baslarken"}
          </span>
          <h2 className="mt-3 font-display text-3xl sm:text-4xl font-semibold tracking-tight text-ink">
            Tek <span className="text-signal">docker-compose</span> komutu.
          </h2>
          <p className="mt-4 text-muted leading-relaxed">
            Kafka, Neo4j ve Redis saniyeler içinde ayağa kalkar. Ardından
            sentetik işlem üreticisini çalıştır — uyarılar dashboard&apos;a düşmeye
            başlar.
          </p>
        </motion.div>

        <div className="mt-12 grid grid-cols-1 lg:grid-cols-12 gap-8 lg:gap-10 items-start">
          {/* Sol: numaralı adımlar */}
          <div className="lg:col-span-5 space-y-1">
            {SETUP_STEPS.map((s, i) => (
              <motion.div
                key={i}
                initial={{ opacity: 0, x: -12 }}
                whileInView={{ opacity: 1, x: 0 }}
                viewport={{ once: true, margin: "-60px" }}
                transition={{ duration: 0.4, delay: i * 0.08 }}
                className="flex gap-4 rounded-lg px-3 py-3 hover:bg-base-2/40 transition-colors"
              >
                <span className="font-mono text-[13px] text-signal/70 tabular-nums flex-shrink-0 mt-0.5">
                  {String(i + 1).padStart(2, "0")}
                </span>
                <div className="min-w-0">
                  <div className="text-[13px] text-ink">{s.comment}</div>
                  <div className="mt-1 font-mono text-[11px] text-muted break-all">
                    {s.command}
                  </div>
                </div>
              </motion.div>
            ))}
          </div>

          {/* Sağ: terminal */}
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true, margin: "-60px" }}
            transition={{ duration: 0.5, delay: 0.1 }}
            className="lg:col-span-7"
          >
            <Terminal />
          </motion.div>
        </div>
      </div>
    </section>
  )
}
