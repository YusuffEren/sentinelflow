"use client"

// =============================================================================
// NetworkGraph — SentinelFlow imza ogusu
// =============================================================================
// Canli bir dolandiricilik grafi: dugumler (hesaplar) + kenarlar (transferler)
// hafifce oynar, kenarlar boyunca islem parcaciklari akar. Belli araliklarla
// bir "dairesel ring" tespit edilir -> o dongudeki dugum/kenarlar kirmiziya
// donup nabiz gibi atar. Dugumlerde IBAN etiketleri, ring tespitinde
// "RING DETECTED" rozeti gorunur.
// =============================================================================

import { useEffect, useRef } from "react"
import { COLORS } from "@/lib/landing-data"

interface Node {
  ax: number
  ay: number
  vx: number
  vy: number
  phase: number
  radius: number
  label: string
  isHub: boolean
}

interface Edge {
  a: number
  b: number
  phase: number
  weight: number
}

interface Ring {
  nodes: number[]
  bornAt: number
  duration: number
}

interface NetworkGraphProps {
  className?: string
  nodeCount?: number
  ringIntervalMs?: number
  interactive?: boolean
  showLabels?: boolean
}

const IBAN_PREFIX = "TR"

function genIban(seed: number): string {
  const n = (seed * 7919 + 13) % 1000000000
  return `${IBAN_PREFIX} ${String(n).slice(0, 2)} ${String(n).slice(2, 6)} ${String(n).slice(6, 9)}`
}

export function NetworkGraph({
  className,
  nodeCount = 14,
  ringIntervalMs = 5200,
  interactive = true,
  showLabels = true,
}: NetworkGraphProps) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const wrapRef = useRef<HTMLDivElement | null>(null)
  const pointer = useRef({ x: 0.5, y: 0.5, tx: 0.5, ty: 0.5, active: false })

  useEffect(() => {
    const canvas = canvasRef.current
    const wrap = wrapRef.current
    if (!canvas || !wrap) return
    const ctx = canvas.getContext("2d", { alpha: true })
    if (!ctx) return

    const prefersReduced =
      typeof window !== "undefined" &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches

    const isMobile = window.innerWidth < 768
    const N = isMobile ? Math.min(nodeCount, 9) : nodeCount

    let W = 0
    let H = 0
    let dpr = 1

    // --- Dugum yerlesimi (cember etrafinda + jit) ----------------------------
    const nodes: Node[] = []
    const cx = 0.5
    const cy = 0.5
    for (let i = 0; i < N; i++) {
      const angle = (i / N) * Math.PI * 2
      const radius = 0.28 + Math.random() * 0.12
      nodes.push({
        ax: cx + Math.cos(angle) * radius,
        ay: cy + Math.sin(angle) * radius * 0.82,
        vx: 0,
        vy: 0,
        phase: Math.random() * Math.PI * 2,
        radius: 3 + Math.random() * 2,
        label: genIban(i + 1),
        isHub: i % 4 === 0,
      })
    }
    // merkezde bir hub
    if (N > 6) {
      nodes.push({
        ax: cx,
        ay: cy,
        vx: 0,
        vy: 0,
        phase: 0,
        radius: 5,
        label: genIban(99),
        isHub: true,
      })
    }

    const totalN = nodes.length

    // --- Kenar uretimi (hub'a bagli + cember cevresi) -----------------------
    const edges: Edge[] = []
    const adj = new Set<string>()
    const addEdge = (a: number, b: number, w = 1) => {
      if (a === b) return
      const key = a < b ? `${a}-${b}` : `${b}-${a}`
      if (adj.has(key)) return
      adj.add(key)
      edges.push({ a, b, phase: Math.random(), weight: w })
    }
    // cember cevresi baglantilari
    for (let i = 0; i < N; i++) {
      addEdge(i, (i + 1) % N, 1)
    }
    // hub varsa merkez'e bagla
    if (totalN > N) {
      const hubIdx = totalN - 1
      for (let i = 0; i < N; i += 2) {
        addEdge(i, hubIdx, 2)
      }
    }
    // bir kac capraz baglanti
    for (let i = 0; i < Math.floor(N / 2); i++) {
      addEdge(i, (i + Math.floor(N / 2)) % N, 1)
    }

    // --- Ring takibi ---------------------------------------------------------
    const rings: Ring[] = []
    let nextRingAt = prefersReduced ? Infinity : performance.now() + 2800

    const spawnRing = () => {
      const len = 3 + Math.floor(Math.random() * 2)
      const start = Math.floor(Math.random() * N)
      const path = [start]
      let cur = start
      for (let i = 0; i < len - 1; i++) {
        const neighbors: number[] = []
        for (const e of edges) {
          if (e.a === cur && !path.includes(e.b)) neighbors.push(e.b)
          else if (e.b === cur && !path.includes(e.a)) neighbors.push(e.a)
        }
        if (neighbors.length === 0) break
        const next = neighbors[Math.floor(Math.random() * neighbors.length)]
        path.push(next)
        cur = next
      }
      if (path.length < 3) return
      rings.push({
        nodes: path,
        bornAt: performance.now(),
        duration: 4000,
      })
    }

    // --- Boyutlandirma -------------------------------------------------------
    const resize = () => {
      const rect = wrap.getBoundingClientRect()
      W = rect.width
      H = rect.height
      dpr = Math.min(window.devicePixelRatio || 1, 2)
      canvas.width = Math.floor(W * dpr)
      canvas.height = Math.floor(H * dpr)
      canvas.style.width = `${W}px`
      canvas.style.height = `${H}px`
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    }
    resize()
    const ro = new ResizeObserver(resize)
    ro.observe(wrap)

    // --- Pointer parallax + hover -------------------------------------------
    const onMove = (e: PointerEvent) => {
      const rect = wrap.getBoundingClientRect()
      pointer.current.tx = (e.clientX - rect.left) / rect.width
      pointer.current.ty = (e.clientY - rect.top) / rect.height
      pointer.current.active = true
    }
    const onLeave = () => {
      pointer.current.active = false
    }
    if (interactive && !prefersReduced) {
      wrap.addEventListener("pointermove", onMove, { passive: true })
      wrap.addEventListener("pointerleave", onLeave, { passive: true })
    }

    // --- Dugum ekran koordinati ---------------------------------------------
    const nodePos = (n: Node, t: number) => {
      const drift = prefersReduced ? 0 : 1
      const dx = Math.sin(t * 0.0005 + n.phase) * 10 * drift
      const dy = Math.cos(t * 0.0004 + n.phase * 1.3) * 8 * drift
      const px = (pointer.current.x - 0.5) * 14 * drift
      const py = (pointer.current.y - 0.5) * 14 * drift
      return {
        x: n.ax * W + dx + px,
        y: n.ay * H + dy + py,
      }
    }

    // --- Cizim ---------------------------------------------------------------
    let raf = 0
    const draw = (t: number) => {
      pointer.current.x += (pointer.current.tx - pointer.current.x) * 0.06
      pointer.current.y += (pointer.current.ty - pointer.current.y) * 0.06

      ctx.clearRect(0, 0, W, H)

      // ring zamanlamasi
      if (t >= nextRingAt) {
        spawnRing()
        nextRingAt = t + ringIntervalMs
      }
      for (let i = rings.length - 1; i >= 0; i--) {
        if (t - rings[i].bornAt > rings[i].duration) rings.splice(i, 1)
      }

      const ringNodes = new Set<number>()
      const ringEdges = new Set<string>()
      let pulse = 0
      for (const r of rings) {
        const age = (t - r.bornAt) / r.duration
        pulse = Math.max(pulse, Math.sin(age * Math.PI))
        for (const idx of r.nodes) ringNodes.add(idx)
        for (let i = 0; i < r.nodes.length; i++) {
          const a = r.nodes[i]
          const b = r.nodes[(i + 1) % r.nodes.length]
          const key = a < b ? `${a}-${b}` : `${b}-${a}`
          ringEdges.add(key)
        }
      }

      const pos = nodes.map((n) => nodePos(n, t))

      // --- kenarlar (alt katman: soluk) ---
      for (const e of edges) {
        const a = pos[e.a]
        const b = pos[e.b]
        const key = e.a < e.b ? `${e.a}-${e.b}` : `${e.b}-${e.a}`
        const isRing = ringEdges.has(key)
        if (isRing) {
          ctx.strokeStyle = `rgba(255, 77, 94, ${0.3 + pulse * 0.6})`
          ctx.lineWidth = 2 + pulse * 1.5
          ctx.shadowColor = COLORS.alarm
          ctx.shadowBlur = 14 * pulse
        } else {
          ctx.strokeStyle = "rgba(0, 229, 199, 0.1)"
          ctx.lineWidth = 0.8
          ctx.shadowBlur = 0
        }
        ctx.beginPath()
        ctx.moveTo(a.x, a.y)
        ctx.lineTo(b.x, b.y)
        ctx.stroke()
      }
      ctx.shadowBlur = 0

      // --- ring kapanis kenarlari ---
      for (const r of rings) {
        const age = (t - r.bornAt) / r.duration
        const p = Math.sin(age * Math.PI)
        const a = pos[r.nodes[r.nodes.length - 1]]
        const b = pos[r.nodes[0]]
        ctx.strokeStyle = `rgba(255, 77, 94, ${0.2 + p * 0.7})`
        ctx.lineWidth = 2 + p * 1.5
        ctx.shadowColor = COLORS.alarm
        ctx.shadowBlur = 16 * p
        ctx.beginPath()
        ctx.moveTo(a.x, a.y)
        ctx.lineTo(b.x, b.y)
        ctx.stroke()
      }
      ctx.shadowBlur = 0

      // --- akis parcaciklari ---
      if (!prefersReduced) {
        for (const e of edges) {
          const a = pos[e.a]
          const b = pos[e.b]
          const key = e.a < e.b ? `${e.a}-${e.b}` : `${e.b}-${e.a}`
          const isRing = ringEdges.has(key)
          const count = isRing ? 3 : 1
          for (let k = 0; k < count; k++) {
            const prog =
              ((t * 0.00016 + e.phase + k / count) % 1 + 1) % 1
            const x = a.x + (b.x - a.x) * prog
            const y = a.y + (b.y - a.y) * prog
            if (isRing) {
              ctx.fillStyle = `rgba(255, 77, 94, ${0.7 + pulse * 0.3})`
              ctx.shadowColor = COLORS.alarm
              ctx.shadowBlur = 6
            } else {
              ctx.fillStyle = "rgba(0, 229, 199, 0.6)"
              ctx.shadowBlur = 0
            }
            ctx.beginPath()
            ctx.arc(x, y, isRing ? 2.5 : 1.6, 0, Math.PI * 2)
            ctx.fill()
          }
        }
        ctx.shadowBlur = 0
      }

      // --- dugumler ---
      for (let i = 0; i < nodes.length; i++) {
        const n = nodes[i]
        const p = pos[i]
        const isRing = ringNodes.has(i)
        const baseR = n.isHub ? n.radius + 1.5 : n.radius
        const r = baseR + (isRing ? Math.sin(t * 0.007) * 2 + 2.5 : 0)

        // dis halka (glow)
        if (isRing) {
          const grad = ctx.createRadialGradient(p.x, p.y, r, p.x, p.y, r + 18)
          grad.addColorStop(0, `rgba(255, 77, 94, ${0.4 + pulse * 0.4})`)
          grad.addColorStop(1, "rgba(255, 77, 94, 0)")
          ctx.fillStyle = grad
          ctx.beginPath()
          ctx.arc(p.x, p.y, r + 18, 0, Math.PI * 2)
          ctx.fill()
        } else if (n.isHub) {
          const grad = ctx.createRadialGradient(p.x, p.y, r, p.x, p.y, r + 14)
          grad.addColorStop(0, "rgba(0, 229, 199, 0.25)")
          grad.addColorStop(1, "rgba(0, 229, 199, 0)")
          ctx.fillStyle = grad
          ctx.beginPath()
          ctx.arc(p.x, p.y, r + 14, 0, Math.PI * 2)
          ctx.fill()
        }

        // cekirdek
        if (isRing) {
          ctx.fillStyle = COLORS.alarm
          ctx.shadowColor = COLORS.alarm
          ctx.shadowBlur = 18 * (0.5 + pulse * 0.5)
        } else if (n.isHub) {
          ctx.fillStyle = COLORS.signal
          ctx.shadowColor = COLORS.signal
          ctx.shadowBlur = 12
        } else {
          ctx.fillStyle = "#1a2330"
          ctx.shadowBlur = 0
        }
        ctx.beginPath()
        ctx.arc(p.x, p.y, r, 0, Math.PI * 2)
        ctx.fill()

        // kenarlik
        ctx.shadowBlur = 0
        ctx.strokeStyle = isRing
          ? COLORS.alarm
          : n.isHub
            ? COLORS.signal
            : "rgba(0, 229, 199, 0.5)"
        ctx.lineWidth = isRing ? 2 : 1.2
        ctx.beginPath()
        ctx.arc(p.x, p.y, r, 0, Math.PI * 2)
        ctx.stroke()

        // ic nokta (hub ve ring icin)
        if (n.isHub || isRing) {
          ctx.fillStyle = "#fff"
          ctx.beginPath()
          ctx.arc(p.x, p.y, r * 0.35, 0, Math.PI * 2)
          ctx.fill()
        }
      }

      // --- etiketler (IBAN'lar) ---
      if (showLabels) {
        ctx.font = "500 9px 'JetBrains Mono', monospace"
        ctx.textAlign = "center"
        for (let i = 0; i < nodes.length; i++) {
          const n = nodes[i]
          const p = pos[i]
          const isRing = ringNodes.has(i)
          if (n.isHub || isRing) {
            const labelY = p.y - n.radius - (isRing ? 14 : 10)
            // arka plan
            const text = n.label
            const w = ctx.measureText(text).width
            ctx.fillStyle = isRing
              ? "rgba(255, 77, 94, 0.12)"
              : "rgba(0, 229, 199, 0.08)"
            ctx.fillRect(p.x - w / 2 - 4, labelY - 7, w + 8, 12)
            ctx.strokeStyle = isRing
              ? "rgba(255, 77, 94, 0.3)"
              : "rgba(0, 229, 199, 0.2)"
            ctx.lineWidth = 0.5
            ctx.strokeRect(p.x - w / 2 - 4, labelY - 7, w + 8, 12)
            ctx.fillStyle = isRing ? COLORS.alarm : COLORS.signal
            ctx.fillText(text, p.x, labelY + 1)
          }
        }
      }

      // --- ring rozeti ---
      if (rings.length > 0 && pulse > 0.3) {
        const r = rings[0]
        const center = r.nodes.reduce(
          (acc, idx) => ({ x: acc.x + pos[idx].x, y: acc.y + pos[idx].y }),
          { x: 0, y: 0 },
        )
        center.x /= r.nodes.length
        center.y /= r.nodes.length
        const text = "RING DETECTED"
        ctx.font = "600 9px 'JetBrains Mono', monospace"
        ctx.textAlign = "center"
        const w = ctx.measureText(text).width
        ctx.fillStyle = `rgba(255, 77, 94, ${0.15 + pulse * 0.2})`
        ctx.fillRect(center.x - w / 2 - 8, center.y - 8, w + 16, 16)
        ctx.strokeStyle = `rgba(255, 77, 94, ${0.4 + pulse * 0.4})`
        ctx.lineWidth = 1
        ctx.strokeRect(center.x - w / 2 - 8, center.y - 8, w + 16, 16)
        ctx.fillStyle = COLORS.alarm
        ctx.fillText(text, center.x, center.y + 3)
      }

      ctx.shadowBlur = 0
      raf = requestAnimationFrame(draw)
    }

    if (prefersReduced) {
      draw(performance.now())
    } else {
      raf = requestAnimationFrame(draw)
    }

    return () => {
      cancelAnimationFrame(raf)
      ro.disconnect()
      wrap.removeEventListener("pointermove", onMove)
      wrap.removeEventListener("pointerleave", onLeave)
    }
  }, [nodeCount, ringIntervalMs, interactive, showLabels])

  return (
    <div ref={wrapRef} className={className} aria-hidden="true">
      <canvas ref={canvasRef} className="block w-full h-full" />
    </div>
  )
}
