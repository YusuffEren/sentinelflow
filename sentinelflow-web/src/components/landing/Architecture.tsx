"use client"

// =============================================================================
// Architecture — Generator → Kafka → 4 Dedektör → Neo4j/Redis → Dashboard
// akışını, kullanıcı aşağı kaydırdıkça adım adım aydınlanan bir diyagram olarak
// anlatır. Kutular view'a girince "ışığı yanar"; bağlantı çizgileri dash-offset
// ile akar.
// =============================================================================

import { motion } from "motion/react"
import { FLOW_STEPS, type FlowStep } from "@/lib/landing-data"
import { cn } from "@/lib/utils"

const KIND_STYLE: Record<
  FlowStep["kind"],
  { dot: string; border: string; glow: string; label: string }
> = {
  source: { dot: "bg-amber", border: "border-amber/30", glow: "shadow-[0_0_24px_-6px_rgba(255,176,32,0.5)]", label: "text-amber" },
  stream: { dot: "bg-signal", border: "border-signal/30", glow: "shadow-[0_0_24px_-6px_rgba(0,229,199,0.5)]", label: "text-signal" },
  detector: { dot: "bg-signal", border: "border-signal/30", glow: "shadow-[0_0_24px_-6px_rgba(0,229,199,0.45)]", label: "text-signal" },
  store: { dot: "bg-signal-soft", border: "border-signal/25", glow: "shadow-[0_0_24px_-6px_rgba(45,225,194,0.4)]", label: "text-signal-soft" },
  ui: { dot: "bg-alarm", border: "border-alarm/30", glow: "shadow-[0_0_24px_-6px_rgba(255,77,94,0.5)]", label: "text-alarm" },
}

function FlowBox({ step, delay }: { step: FlowStep; delay: number }) {
  const s = KIND_STYLE[step.kind]
  return (
    <motion.div
      initial={{ opacity: 0.2, y: 14 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true, margin: "-60px" }}
      transition={{ duration: 0.5, delay }}
      className={cn(
        "relative w-full rounded-lg border bg-base-2/70 backdrop-blur-sm px-3.5 py-3 transition-shadow duration-500",
        s.border,
      )}
    >
      <motion.div
        initial={{ opacity: 0 }}
        whileInView={{ opacity: 1 }}
        viewport={{ once: true, margin: "-60px" }}
        transition={{ duration: 0.5, delay: delay + 0.15 }}
        className={cn("absolute inset-0 rounded-lg pointer-events-none", s.glow)}
      />
      <div className="relative flex items-center gap-2.5">
        <span className={cn("w-1.5 h-1.5 rounded-full flex-shrink-0", s.dot)} />
        <div className="min-w-0">
          <div className="text-[13px] font-medium text-ink truncate">
            {step.label}
          </div>
          <div className="text-[10px] font-mono text-muted uppercase tracking-wider truncate">
            {step.sub}
          </div>
        </div>
      </div>
    </motion.div>
  )
}

// Bağlantı çizgisi — sürekli akıyor (flow-line)
function Connector({ vertical = false }: { vertical?: boolean }) {
  if (vertical) {
    return (
      <div className="flex justify-center py-1.5" aria-hidden>
        <svg width="2" height="22" className="overflow-visible">
          <line
            x1="1"
            y1="0"
            x2="1"
            y2="22"
            stroke="rgba(0,229,199,0.35)"
            strokeWidth="1.5"
            className="flow-line"
          />
        </svg>
      </div>
    )
  }
  return (
    <div className="flex items-center justify-center px-1.5" aria-hidden>
      <svg width="28" height="2" className="overflow-visible">
        <line
          x1="0"
          y1="1"
          x2="28"
          y2="1"
          stroke="rgba(0,229,199,0.35)"
          strokeWidth="1.5"
          className="flow-line"
        />
      </svg>
    </div>
  )
}

export function Architecture() {
  return (
    <section id="mimari" className="relative py-24 sm:py-32">
      <div className="mx-auto max-w-7xl px-5 sm:px-8">
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true, margin: "-80px" }}
          transition={{ duration: 0.5 }}
          className="max-w-2xl"
        >
          <span className="font-mono text-[11px] uppercase tracking-[0.25em] text-signal">
            {"// Mimari"}
          </span>
          <h2 className="mt-3 font-display text-3xl sm:text-4xl font-semibold tracking-tight text-ink">
            Bir işlem, beş aşamada
            <span className="text-signal"> taranır.</span>
          </h2>
          <p className="mt-4 text-muted leading-relaxed">
            Her işlem Kafka topic&apos;ine düştüğü anda dört paralel dedektör
            süzgecinden geçer; graf ve geo verileriyle zenginleştirilip SOC
            konsoluna saniyeler içinde düşer. Aşağı kaydırın — akış aydınlanır.
          </p>
        </motion.div>

        {/* Diyagram */}
        <div className="mt-12 rounded-2xl border border-line bg-base-2/30 p-5 sm:p-8">
          {/* Masaüstü: yatay akış */}
          <div className="hidden md:flex items-stretch justify-between gap-1">
            {FLOW_STEPS.map((group, gi) => (
              <div key={gi} className="flex items-center gap-1">
                <div className="flex flex-col gap-2.5 min-w-[140px]">
                  {group.map((step, si) => (
                    <FlowBox
                      key={step.id}
                      step={step}
                      delay={gi * 0.18 + si * 0.08}
                    />
                  ))}
                </div>
                {gi < FLOW_STEPS.length - 1 && <Connector />}
              </div>
            ))}
          </div>

          {/* Mobil: dikey akış */}
          <div className="md:hidden">
            {FLOW_STEPS.map((group, gi) => (
              <div key={gi}>
                <div className="flex flex-col gap-2.5">
                  {group.map((step, si) => (
                    <FlowBox
                      key={step.id}
                      step={step}
                      delay={gi * 0.12 + si * 0.06}
                    />
                  ))}
                </div>
                {gi < FLOW_STEPS.length - 1 && <Connector vertical />}
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}
