"use client"

// =============================================================================
// Terminal — kurulum komutlarını yazılıyormuş gibi (typewriter) gösterir.
// Başlık çubuğunda kopyala + tekrar oynat. prefers-reduced-motion: anında.
// =============================================================================

import { useEffect, useRef, useState } from "react"
import { Check, Copy } from "lucide-react"
import { SETUP_STEPS } from "@/lib/landing-data"

const TYPE_SPEED = 26 // ms/karakter

export function Terminal() {
  const [doneStep, setDoneStep] = useState(0) // tamamlanan adım sayısı
  const [typed, setTyped] = useState("") // mevcut adımın yazılan metni
  const [copied, setCopied] = useState(false)
  const rafRef = useRef<number | null>(null)

  const allCmds = SETUP_STEPS.map((s) => s.command).join("\n")

  useEffect(() => {
    const prefersReduced =
      typeof window !== "undefined" &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches
    if (prefersReduced) {
      requestAnimationFrame(() => {
        setDoneStep(SETUP_STEPS.length)
        setTyped("")
      })
      return
    }

    let stepIdx = 0
    let charIdx = 0
    let last = performance.now()

    const step = SETUP_STEPS[stepIdx]
    if (!step) return

    const loop = (now: number) => {
      if (now - last < TYPE_SPEED) {
        rafRef.current = requestAnimationFrame(loop)
        return
      }
      last = now
      const cmd = SETUP_STEPS[stepIdx].command
      if (charIdx < cmd.length) {
        charIdx += 1
        setTyped(cmd.slice(0, charIdx))
        rafRef.current = requestAnimationFrame(loop)
      } else {
        // adım bitti → bir sonrakine geç (kısa duraklama)
        setDoneStep(stepIdx + 1)
        setTyped("")
        stepIdx += 1
        charIdx = 0
        if (stepIdx < SETUP_STEPS.length) {
          setTimeout(() => {
            rafRef.current = requestAnimationFrame(loop)
          }, 360)
        }
      }
    }
    rafRef.current = requestAnimationFrame(loop)

    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current)
    }
  }, [])

  const copyAll = async () => {
    try {
      await navigator.clipboard.writeText(allCmds)
      setCopied(true)
      setTimeout(() => setCopied(false), 1600)
    } catch {
      /* ignore */
    }
  }

  return (
    <div className="rounded-xl overflow-hidden border border-line bg-[#080b11] shadow-2xl shadow-black/40">
      {/* title bar */}
      <div className="flex items-center justify-between px-4 h-9 border-b border-line bg-base-2/80">
        <div className="flex items-center gap-2">
          <span className="w-2.5 h-2.5 rounded-full bg-alarm/70" />
          <span className="w-2.5 h-2.5 rounded-full bg-amber/70" />
          <span className="w-2.5 h-2.5 rounded-full bg-signal/70" />
          <span className="ml-3 text-[11px] font-mono text-muted">
            sentinelflow — bash
          </span>
        </div>
        <div className="flex items-center gap-1">
          <button
            onClick={copyAll}
            className="flex items-center gap-1.5 px-2 py-1 rounded text-[11px] text-muted hover:text-signal hover:bg-signal/5 transition-colors"
            title="Tüm komutları kopyala"
          >
            {copied ? (
              <Check className="w-3 h-3 text-signal" />
            ) : (
              <Copy className="w-3 h-3" />
            )}
            {copied ? "Kopyalandı" : "Kopyala"}
          </button>
        </div>
      </div>

      {/* body */}
      <div className="p-4 font-mono text-[13px] leading-relaxed min-h-[210px]">
        {SETUP_STEPS.map((s, i) => {
          const isDone = i < doneStep
          const isActive = i === doneStep
          const visibleCmd = isDone ? s.command : isActive ? typed : ""
          if (!isDone && !isActive) return null
          return (
            <div key={i} className="mb-3">
              {s.comment && (
                <div className="text-muted/60 text-[11px] mb-0.5">
                  # {s.comment}
                </div>
              )}
              <div className="flex items-start gap-2">
                <span className="text-signal/70 select-none">{s.prompt}</span>
                <span className="text-amber/80 select-none">$</span>
                <span className="text-ink break-all">
                  {visibleCmd}
                  {isActive && (
                    <span className="blink-caret text-signal ml-0.5">▋</span>
                  )}
                </span>
              </div>
              {isDone && (
                <div className="text-signal/60 text-[11px] mt-0.5">
                  ↳ {s.comment === "Sentetik işlem akışı başlar → alarmlar düşer" ? "streaming alerts…" : "done"}
                </div>
              )}
            </div>
          )
        })}
        {doneStep >= SETUP_STEPS.length && (
          <div className="text-signal text-[12px] mt-2 flex items-center gap-2">
            <span className="w-1.5 h-1.5 rounded-full bg-signal animate-subtle-pulse" />
            sistem aktif — dashboard: localhost:8501
          </div>
        )}
      </div>
    </div>
  )
}
