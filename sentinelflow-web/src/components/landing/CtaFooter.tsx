"use client"

// =============================================================================
// CtaFooter — GitHub reposuna link, lisans, iletisim. Sayfanin kapanisi.
// =============================================================================

import { motion } from "motion/react"
import { Github, ArrowUpRight, ShieldHalf } from "lucide-react"
import Link from "next/link"
import { REPO_URL } from "@/lib/landing-data"

export function CtaFooter() {
  return (
    <footer className="relative border-t border-line">
      {/* CTA bandi */}
      <div className="relative overflow-hidden">
        <div className="absolute inset-0 pointer-events-none">
          <div className="absolute -top-24 left-1/2 -translate-x-1/2 w-[40rem] h-[40rem] rounded-full bg-signal/5 blur-3xl" />
        </div>
        <div className="relative mx-auto max-w-7xl px-5 sm:px-8 py-24 sm:py-32 text-center">
          <motion.div
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true, margin: "-80px" }}
            transition={{ duration: 0.5 }}
          >
            <span className="font-mono text-[11px] uppercase tracking-[0.25em] text-signal">
              {"// Hazir misin"}
            </span>
            <h2 className="mt-4 font-display text-3xl sm:text-5xl font-semibold tracking-tight text-ink">
              Dolandiricilik seni izlemeden,
              <br className="hidden sm:block" />
              <span className="text-signal"> sen onu izle.</span>
            </h2>
            <p className="mt-5 max-w-xl mx-auto text-muted leading-relaxed">
              Acik kaynakli. Tek komutla ayaga kalkar. Sahte veriyle bile gercek
              bir SOC hissi verir.
            </p>
            <div className="mt-8 flex flex-wrap items-center justify-center gap-3">
              <a
                href={REPO_URL}
                target="_blank"
                rel="noreferrer"
                className="group inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-signal text-[#04141a] font-medium hover:bg-signal-soft transition-colors"
              >
                <Github className="w-4 h-4" />
                GitHub
                <ArrowUpRight className="w-4 h-4 group-hover:translate-x-0.5 group-hover:-translate-y-0.5 transition-transform" />
              </a>
              <Link
                href="/dashboard"
                className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg border border-line bg-base-2/50 text-ink hover:border-signal/40 hover:text-signal transition-colors"
              >
                Dashboard &rarr;
              </Link>
            </div>
          </motion.div>
        </div>
      </div>

      {/* alt bilgi */}
      <div className="border-t border-line">
        <div className="mx-auto max-w-7xl px-5 sm:px-8 py-8 flex flex-col sm:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-2.5">
            <span className="grid place-items-center w-6 h-6 rounded-md border border-signal/30 bg-signal/5">
              <ShieldHalf className="w-3 h-3 text-signal" />
            </span>
            <span className="font-display font-semibold text-[14px] text-ink">
              Sentinel<span className="text-signal">Flow</span>
            </span>
            <span className="text-[11px] font-mono text-muted ml-2">
              real-time fraud detection
            </span>
          </div>

          <div className="flex items-center gap-5 text-[12px] text-muted">
            <a
              href={REPO_URL}
              target="_blank"
              rel="noreferrer"
              className="hover:text-signal transition-colors"
            >
              GitHub
            </a>
            <a
              href={`${REPO_URL}/blob/main/LICENSE`}
              target="_blank"
              rel="noreferrer"
              className="hover:text-signal transition-colors"
            >
              MIT Lisans
            </a>
            <a
              href={`${REPO_URL}/issues`}
              target="_blank"
              rel="noreferrer"
              className="hover:text-signal transition-colors"
            >
              Iletisim
            </a>
          </div>
        </div>
        <div className="mx-auto max-w-7xl px-5 sm:px-8 pb-8">
          <p className="text-center sm:text-left text-[11px] font-mono text-muted/60">
            &copy; {new Date().getFullYear()} YusuffEren &mdash; TEKNOFEST 2026
          </p>
        </div>
      </div>
    </footer>
  )
}
