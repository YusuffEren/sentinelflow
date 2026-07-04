"use client"

// =============================================================================
// Navbar — sabit üst bar. SOC hissi: canlı saat + durum noktası.
// Scroll'da zemini belirginleşir. Mobilde sade (logo + GitHub).
// =============================================================================

import { useEffect, useState } from "react"
import Link from "next/link"
import { motion } from "motion/react"
import { Github, ShieldHalf } from "lucide-react"
import { REPO_URL } from "@/lib/landing-data"
import { cn } from "@/lib/utils"

const NAV_LINKS = [
  { href: "#mimari", label: "Mimari" },
  { href: "#motorlar", label: "Motorlar" },
  { href: "#metrikler", label: "Metrikler" },
  { href: "#kurulum", label: "Kurulum" },
]

export function Navbar() {
  const [scrolled, setScrolled] = useState(false)
  const [time, setTime] = useState("")

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 24)
    onScroll()
    window.addEventListener("scroll", onScroll, { passive: true })
    return () => window.removeEventListener("scroll", onScroll)
  }, [])

  useEffect(() => {
    const tick = () =>
      setTime(
        new Date().toLocaleTimeString("en-GB", {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
        }),
      )
    tick()
    const iv = setInterval(tick, 1000)
    return () => clearInterval(iv)
  }, [])

  return (
    <motion.header
      initial={{ y: -24, opacity: 0 }}
      animate={{ y: 0, opacity: 1 }}
      transition={{ duration: 0.5, ease: "easeOut", delay: 0.1 }}
      className={cn(
        "fixed top-0 inset-x-0 z-50 transition-colors duration-300",
        scrolled
          ? "bg-base/80 backdrop-blur-md border-b border-line"
          : "bg-transparent border-b border-transparent",
      )}
    >
      <div className="mx-auto max-w-7xl px-5 sm:px-8 h-14 flex items-center justify-between">
        <Link href="/" className="flex items-center gap-2.5 group">
          <span className="relative grid place-items-center w-7 h-7 rounded-md border border-signal/30 bg-signal/5">
            <ShieldHalf className="w-3.5 h-3.5 text-signal" />
            <span className="absolute inset-0 rounded-md ring-1 ring-signal/0 group-hover:ring-signal/40 transition" />
          </span>
          <span className="font-display font-semibold tracking-tight text-ink text-[15px]">
            Sentinel<span className="text-signal">Flow</span>
          </span>
        </Link>

        <nav className="hidden md:flex items-center gap-1">
          {NAV_LINKS.map((l) => (
            <a
              key={l.href}
              href={l.href}
              className="px-3 py-1.5 text-[13px] text-muted hover:text-ink rounded-md transition-colors"
            >
              {l.label}
            </a>
          ))}
        </nav>

        <div className="flex items-center gap-3">
          <div className="hidden sm:flex items-center gap-2 px-2.5 py-1 rounded-md border border-line bg-base-2/60">
            <span className="w-1.5 h-1.5 rounded-full bg-signal animate-subtle-pulse" />
            <span className="text-[11px] text-muted font-mono tabular-nums">
              {time}
            </span>
          </div>
          <a
            href={REPO_URL}
            target="_blank"
            rel="noreferrer"
            className="flex items-center gap-2 px-3 py-1.5 rounded-md border border-line bg-base-2/60 text-[13px] text-ink hover:border-signal/40 hover:text-signal transition-colors"
          >
            <Github className="w-4 h-4" />
            <span className="hidden sm:inline">GitHub</span>
          </a>
        </div>
      </div>
    </motion.header>
  )
}
