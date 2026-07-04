"use client"

// =============================================================================
// ParticleField — sayfa geneli çok hafif "veri akışı" ambiyansı.
// İnce, yavaş kayan dikey çizgiler + birkaç parça. Opaklık düşük, dikkat
// dağıtmaz. prefers-reduced-motion ve mobilde devre dışı.
// =============================================================================

import { useEffect, useRef } from "react"

interface ParticleFieldProps {
  className?: string
  count?: number
}

export function ParticleField({ className, count = 26 }: ParticleFieldProps) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext("2d", { alpha: true })
    if (!ctx) return

    const prefersReduced =
      typeof window !== "undefined" &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches
    const isMobile = window.innerWidth < 768
    if (prefersReduced || isMobile) return

    const N = Math.min(count, 30)
    let W = 0
    let H = 0
    let dpr = 1

    type P = { x: number; y: number; vy: number; len: number; a: number }
    let parts: P[] = []

    const resize = () => {
      W = canvas.clientWidth
      H = canvas.clientHeight
      dpr = Math.min(window.devicePixelRatio || 1, 2)
      canvas.width = Math.floor(W * dpr)
      canvas.height = Math.floor(H * dpr)
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
      parts = Array.from({ length: N }, () => ({
        x: Math.random() * W,
        y: Math.random() * H,
        vy: 6 + Math.random() * 22,
        len: 20 + Math.random() * 60,
        a: 0.04 + Math.random() * 0.07,
      }))
    }
    resize()
    window.addEventListener("resize", resize)

    let raf = 0
    let last = performance.now()
    const draw = (now: number) => {
      const dt = Math.min((now - last) / 1000, 0.05)
      last = now
      ctx.clearRect(0, 0, W, H)
      ctx.lineWidth = 1
      for (const p of parts) {
        p.y += p.vy * dt
        if (p.y - p.len > H) {
          p.y = -p.len
          p.x = Math.random() * W
        }
        const grad = ctx.createLinearGradient(p.x, p.y - p.len, p.x, p.y)
        grad.addColorStop(0, "rgba(0, 229, 199, 0)")
        grad.addColorStop(1, `rgba(0, 229, 199, ${p.a})`)
        ctx.strokeStyle = grad
        ctx.beginPath()
        ctx.moveTo(p.x, p.y - p.len)
        ctx.lineTo(p.x, p.y)
        ctx.stroke()
      }
      raf = requestAnimationFrame(draw)
    }
    raf = requestAnimationFrame(draw)

    return () => {
      cancelAnimationFrame(raf)
      window.removeEventListener("resize", resize)
    }
  }, [count])

  return (
    <canvas
      ref={canvasRef}
      aria-hidden="true"
      className={className}
      style={{ width: "100%", height: "100%" }}
    />
  )
}
