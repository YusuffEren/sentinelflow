"use client"

// =============================================================================
// SentinelFlow — Landing (ana sayfa)
// =============================================================================
// Tek sayfalik, SOC camindan bakiyor hissi veren tanitim sitesi. Her bölüm
// kendi icinde animator; arka planda ince veri akisi ambiyansi. Tum animasyonlar
// prefers-reduced-motion ile kapanir. Mevcut canli dashboard /dashboard altinda
// korunur.
// =============================================================================

import { useState } from "react"
import { BootSequence } from "@/components/landing/BootSequence"
import { Navbar } from "@/components/landing/Navbar"
import { ParticleField } from "@/components/landing/ParticleField"
import { Hero } from "@/components/landing/Hero"
import { Architecture } from "@/components/landing/Architecture"
import { DetectionEngines } from "@/components/landing/DetectionEngines"
import { AlertFeedPreview } from "@/components/landing/AlertFeedPreview"
import { TechStack } from "@/components/landing/TechStack"
import { PerformanceMetrics } from "@/components/landing/PerformanceMetrics"
import { GettingStarted } from "@/components/landing/GettingStarted"
import { CtaFooter } from "@/components/landing/CtaFooter"

export default function Home() {
  const [booted, setBooted] = useState(false)

  return (
    <>
      <BootSequence onDone={() => setBooted(true)} />

      {/* Ambient veri akisi arka plani (cok hafif) */}
      <div className="fixed inset-0 -z-10 pointer-events-none">
        <ParticleField />
      </div>

      <Navbar />

      <main className={booted ? "" : "opacity-0 transition-opacity duration-500"}>
        <Hero />
        <Architecture />
        <DetectionEngines />
        <AlertFeedPreview />
        <TechStack />
        <PerformanceMetrics />
        <GettingStarted />
        <CtaFooter />
      </main>
    </>
  )
}
