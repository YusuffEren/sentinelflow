"use client"

// =============================================================================
// BootSequence — "sistem başlatılıyor" giriş sekansı (~1.2sn).
// Üstte bir ilerleme çubuğu + durum satırları, sonra fade out.
// prefers-reduced-motion: hiç gösterilmez, içerik doğrudan görünür.
// =============================================================================

import { useEffect, useState } from "react"
import { AnimatePresence, motion } from "motion/react"

const LINES = [
  "kernel://sentinelflow",
  "kafka broker ........... OK",
  "neo4j graph ............ OK",
  "redis geo .............. OK",
  "detectors [4] .......... ARMED",
]

export function BootSequence({ onDone }: { onDone?: () => void }) {
  const [visible, setVisible] = useState(true)
  const [shown, setShown] = useState(0)

  useEffect(() => {
    const prefersReduced =
      typeof window !== "undefined" &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches
    if (prefersReduced) {
      requestAnimationFrame(() => {
        setVisible(false)
        onDone?.()
      })
      return
    }
    let i = 0
    const iv = setInterval(() => {
      i += 1
      setShown(i)
      if (i >= LINES.length) {
        clearInterval(iv)
        setTimeout(() => {
          setVisible(false)
          onDone?.()
        }, 320)
      }
    }, 200)
    return () => clearInterval(iv)
  }, [onDone])

  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          className="fixed inset-0 z-[100] bg-base flex items-center justify-center"
          initial={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.4, ease: "easeInOut" }}
        >
          <div className="w-full max-w-md px-6 font-mono text-xs">
            <div className="mb-4 text-signal tracking-[0.3em] uppercase">
              SentinelFlow
            </div>
            <div className="space-y-1.5 min-h-[7.5rem]">
              {LINES.slice(0, shown).map((l, i) => (
                <motion.div
                  key={l}
                  initial={{ opacity: 0, x: -6 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ duration: 0.18 }}
                  className="text-muted"
                >
                  <span className="text-signal/60">$</span> {l.split(" ...")[0]}
                  {l.includes("...") && (
                    <span className="text-signal/60"> ...</span>
                  )}
                  {l.includes("OK") && (
                    <span className="text-signal"> OK</span>
                  )}
                  {l.includes("ARMED") && (
                    <span className="text-alarm"> ARMED</span>
                  )}
                  {i === shown - 1 && (
                    <span className="blink-caret text-signal ml-1">_</span>
                  )}
                </motion.div>
              ))}
            </div>
            <div className="mt-6 h-px w-full bg-line overflow-hidden">
              <motion.div
                className="h-full bg-signal"
                initial={{ width: "0%" }}
                animate={{ width: "100%" }}
                transition={{ duration: 1.1, ease: "easeInOut" }}
              />
            </div>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  )
}
