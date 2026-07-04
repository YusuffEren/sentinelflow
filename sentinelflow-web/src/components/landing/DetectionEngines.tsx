"use client"

// =============================================================================
// DetectionEngines — 4 sinyal kartı (sıra numarasıyla değil, ayrı kartlar).
// Her kart hover'da öne çıkar + o motorun tespit ettiği paterni mini animasyonla
// gösterir. Şiddet etiketi renk kodlu.
// =============================================================================

import { motion } from "motion/react"
import { ENGINES, SEVERITY_META, type DetectionEngine } from "@/lib/landing-data"

// --- Mini animasyonlar (SVG) -------------------------------------------------

function RingAnimation() {
  // 3 düğüm arasında dönen bir ok → dairesel aklama paterni
  const cx = [22, 78, 50]
  const cy = [30, 38, 78]
  return (
    <svg viewBox="0 0 100 100" className="w-full h-full">
      <defs>
        <marker id="ringArrow" markerWidth="6" markerHeight="6" refX="3" refY="3" orient="auto">
          <path d="M0,0 L6,3 L0,6 Z" fill="#ff4d5e" />
        </marker>
      </defs>
      {[0, 1, 2].map((i) => {
        const j = (i + 1) % 3
        return (
          <line
            key={i}
            x1={cx[i]}
            y1={cy[i]}
            x2={cx[j]}
            y2={cy[j]}
            stroke="rgba(255,77,94,0.25)"
            strokeWidth="1.5"
          />
        )
      })}
      {/* dönen ok */}
      <g>
        <animateTransform
          attributeName="transform"
          type="rotate"
          from="0 50 50"
          to="360 50 50"
          dur="3.5s"
          repeatCount="indefinite"
        />
        <circle cx="50" cy="50" r="14" fill="none" stroke="#ff4d5e" strokeWidth="1.2" strokeDasharray="3 4" opacity="0.6" />
        <path d="M50,36 L54,42 L46,42 Z" fill="#ff4d5e" />
      </g>
      {cx.map((x, i) => (
        <circle key={i} cx={x} cy={cy[i]} r="4.5" fill="#0e131c" stroke="#ff4d5e" strokeWidth="1.5" />
      ))}
    </svg>
  )
}

function TravelAnimation() {
  // iki nokta arasında imkansız çizgi + nabız
  return (
    <svg viewBox="0 0 100 100" className="w-full h-full">
      <circle cx="18" cy="50" r="4" fill="#00e5c7" />
      <circle cx="82" cy="50" r="4" fill="#ff4d5e" />
      <line x1="22" y1="50" x2="78" y2="50" stroke="rgba(0,229,199,0.2)" strokeWidth="1.5" />
      {/* akan imkansız sinyal */}
      <circle r="3" fill="#ffb020">
        <animate attributeName="cx" values="22;78;22" dur="2.2s" repeatCount="indefinite" />
        <animate attributeName="cy" values="50;50;50" dur="2.2s" repeatCount="indefinite" />
        <animate attributeName="opacity" values="0;1;0" dur="2.2s" repeatCount="indefinite" />
      </circle>
      <text x="50" y="34" textAnchor="middle" fontSize="8" fill="#ffb020" fontFamily="monospace">
        9.000 km/h
      </text>
      <text x="18" y="68" textAnchor="middle" fontSize="6.5" fill="#7d8aa0" fontFamily="monospace">IST</text>
      <text x="82" y="68" textAnchor="middle" fontSize="6.5" fill="#7d8aa0" fontFamily="monospace">BER</text>
    </svg>
  )
}

function NlpAnimation() {
  // kelimelerden biri bayraklanır (yanıp söner)
  const words = ["fatura", "kredı", "ödeme", "transfer", "geri"]
  const flagIdx = 1
  return (
    <svg viewBox="0 0 100 100" className="w-full h-full">
      {words.map((w, i) => {
        const x = 14 + (i % 3) * 30
        const y = 30 + Math.floor(i / 3) * 34
        const flagged = i === flagIdx
        return (
          <g key={w}>
            <rect
              x={x - 12}
              y={y - 9}
              width="24"
              height="14"
              rx="3"
              fill={flagged ? "rgba(255,77,94,0.12)" : "rgba(0,229,199,0.06)"}
              stroke={flagged ? "#ff4d5e" : "rgba(0,229,199,0.25)"}
              strokeWidth="1"
            >
              {flagged && (
                <animate
                  attributeName="stroke-opacity"
                  values="1;0.3;1"
                  dur="1.4s"
                  repeatCount="indefinite"
                />
              )}
            </rect>
            <text
              x={x}
              y={y + 1}
              textAnchor="middle"
              fontSize="6"
              fill={flagged ? "#ff4d5e" : "#7d8aa0"}
              fontFamily="monospace"
            >
              {w}
            </text>
          </g>
        )
      })}
      <text x="50" y="86" textAnchor="middle" fontSize="6" fill="#ffb020" fontFamily="monospace">
        match: loan_laundering
      </text>
    </svg>
  )
}

function AnomalyAnimation() {
  // dağılım içinde bir aykırı nokta yanıp söner
  const pts = [
    [25, 70], [32, 64], [28, 58], [40, 68], [36, 60],
    [44, 66], [30, 66], [38, 72], [34, 62], [42, 64],
    [46, 70], [50, 66], [78, 26], [26, 62], [48, 60],
  ]
  return (
    <svg viewBox="0 0 100 100" className="w-full h-full">
      {pts.map((p, i) => {
        const outlier = i === 11
        return (
          <circle
            key={i}
            cx={p[0]}
            cy={p[1]}
            r={outlier ? 3 : 1.8}
            fill={outlier ? "#ff4d5e" : "rgba(0,229,199,0.5)"}
          >
            {outlier && (
              <>
                <animate attributeName="r" values="3;5;3" dur="1.6s" repeatCount="indefinite" />
                <animate attributeName="opacity" values="1;0.4;1" dur="1.6s" repeatCount="indefinite" />
              </>
            )}
          </circle>
        )
      })}
      {/* eksen çizgileri */}
      <line x1="14" y1="80" x2="90" y2="80" stroke="rgba(125,138,160,0.2)" strokeWidth="0.8" />
      <line x1="14" y1="20" x2="14" y2="80" stroke="rgba(125,138,160,0.2)" strokeWidth="0.8" />
      <text x="78" y="20" textAnchor="middle" fontSize="6" fill="#ff4d5e" fontFamily="monospace">
        z=8.4
      </text>
    </svg>
  )
}

function EngineAnimation({ id }: { id: string }) {
  if (id === "circular-ring") return <RingAnimation />
  if (id === "impossible-travel") return <TravelAnimation />
  if (id === "nlp-blacklist") return <NlpAnimation />
  return <AnomalyAnimation />
}

// --- Kart --------------------------------------------------------------------

function EngineCard({ engine, index }: { engine: DetectionEngine; index: number }) {
  const sev = SEVERITY_META[engine.severity]
  return (
    <motion.article
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true, margin: "-60px" }}
      transition={{ duration: 0.5, delay: index * 0.08 }}
      whileHover={{ y: -4 }}
      className="group relative rounded-xl border border-line bg-base-2/50 backdrop-blur-sm overflow-hidden transition-colors hover:border-signal/30"
    >
      {/* hover parıltısı */}
      <div className="pointer-events-none absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-500 shadow-[0_0_40px_-12px_rgba(0,229,199,0.35)]" />

      <div className="relative p-5">
        {/* üst satır: kod + şiddet */}
        <div className="flex items-center justify-between">
          <span className="font-mono text-[10px] tracking-wider text-muted uppercase">
            {engine.code}
          </span>
          <span
            className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[10px] font-medium border"
            style={{
              color: sev.color,
              borderColor: `${sev.color}40`,
              background: `${sev.color}12`,
            }}
          >
            <span
              className="w-1.5 h-1.5 rounded-full"
              style={{ background: sev.color }}
            />
            {sev.label}
          </span>
        </div>

        {/* başlık + teknoloji */}
        <div className="mt-4 flex items-start justify-between gap-3">
          <div>
            <h3 className="font-display text-lg font-semibold text-ink">
              {engine.name}
            </h3>
            <p className="mt-1 text-[11px] font-mono text-signal uppercase tracking-wider">
              {engine.tech}
            </p>
          </div>
          {/* mini animasyon */}
          <div className="w-16 h-16 sm:w-20 sm:h-20 flex-shrink-0 rounded-lg border border-line bg-base/60 p-1">
            <EngineAnimation id={engine.id} />
          </div>
        </div>

        {/* açıklama */}
        <p className="mt-3 text-[13px] text-muted leading-relaxed">
          {engine.description}
        </p>

        {/* örnek */}
        <div className="mt-4 rounded-lg border border-line bg-base/60 p-3">
          <div className="text-[10px] font-mono uppercase tracking-wider text-muted/70">
            {"// ornek tespit"}
          </div>
          <div
            className="mt-1.5 font-mono text-[12px] break-all"
            style={{ color: sev.color }}
          >
            {engine.example}
          </div>
          <div className="mt-1 font-mono text-[11px] text-muted">
            {engine.exampleDetail}
          </div>
        </div>
      </div>
    </motion.article>
  )
}

export function DetectionEngines() {
  return (
    <section id="motorlar" className="relative py-24 sm:py-32">
      <div className="mx-auto max-w-7xl px-5 sm:px-8">
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true, margin: "-80px" }}
          transition={{ duration: 0.5 }}
          className="max-w-2xl"
        >
          <span className="font-mono text-[11px] uppercase tracking-[0.25em] text-signal">
            {"// Tespit Motorlari"}
          </span>
          <h2 className="mt-3 font-display text-3xl sm:text-4xl font-semibold tracking-tight text-ink">
            Dört bağımsız sinyal,
            <span className="text-alarm"> tek akış.</span>
          </h2>
          <p className="mt-4 text-muted leading-relaxed">
            Her motor farklı bir suç paternini farklı bir teknolojiyle arar —
            biri graf döngüsü bulurken diğeri coğrafi hız hesaplar. Birlikte
            çalışırlar; birbirlerini güçlendirirler.
          </p>
        </motion.div>

        <div className="mt-12 grid grid-cols-1 md:grid-cols-2 gap-4 sm:gap-5">
          {ENGINES.map((e, i) => (
            <EngineCard key={e.id} engine={e} index={i} />
          ))}
        </div>
      </div>
    </section>
  )
}
