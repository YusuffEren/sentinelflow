"use client"

// =============================================================================
// AlertOverlay3D — 3D yüzen alarm bildirim kartı
// =============================================================================
// GlobeScene Canvas'ı İÇİNDE render edilir (kendi Canvas'ı yoktur), böylece
// ekstra WebGL context açılmaz. Parent (GlobeScene) bu bileşeni her yeni alarm
// için `key={alert_id}` ile yeniden mount eder; bu sayede bileşen başlangıçta
// görünür (visible=true) başlar ve setState-in-effect gerekmeden temiz bir
// pop-in/out animasyonu verir. @react-spring/three ile ölçek animasyonu;
// 6 sn sonra otomatik kaybolur ve parent'taki aktif alarmı temizler.

import { useEffect, useRef, useState } from "react"
import { useFrame } from "@react-three/fiber"
import { Html } from "@react-three/drei"
import { useSpring, animated } from "@react-spring/three"
import * as THREE from "three"
import { SEVERITY_COLOR, fraudLabel, type Alert3D } from "./types"

interface AlertOverlay3DProps {
  alert: Alert3D
  /** Kart kapandığında çağrılır — parent aktif alarmı temizlemeli. */
  onDismiss?: () => void
  /** Kartın küre merkezinden ne kadar uzakta duracağı (kamera yönünde). */
  distance?: number
  /** Ekranda kalma süresi (ms). */
  duration?: number
}

export function AlertOverlay3D({
  alert,
  onDismiss,
  distance = 3.4,
  duration = 6000,
}: AlertOverlay3DProps) {
  const groupRef = useRef<THREE.Group>(null)
  // Parent key ile her yeni alarmda yeniden mount eder → başlangıçta görünür.
  const [visible, setVisible] = useState(true)

  // Sadece zamanlayıcı kur; setState çağrıları callback içinde (kurala uyumlu).
  useEffect(() => {
    const hideTimer = window.setTimeout(() => setVisible(false), duration)
    const dismissTimer = window.setTimeout(
      () => onDismiss?.(),
      duration + 400,
    )
    return () => {
      window.clearTimeout(hideTimer)
      window.clearTimeout(dismissTimer)
    }
  }, [duration, onDismiss])

  // Pop-in (0→1) ve pop-out (1→0) ölçek animasyonu.
  const springs = useSpring({
    scale: visible ? 1 : 0,
    from: { scale: 0 },
    config: { tension: 240, friction: 22 },
  })

  // Kartı her karede kameranın önünde tut ve kameraya bakacak şekilde döndür.
  // Sabit dünya konumu kullansaydık, kamera küre etrafında dönerken kart arkada
  // kalırdı.
  useFrame((state) => {
    const group = groupRef.current
    if (!group) return
    const camPos = state.camera.position
    const dir = camPos.clone().normalize().multiplyScalar(distance)
    group.position.set(dir.x, dir.y, dir.z)
    group.lookAt(camPos)
  })

  const severityKey = String(alert.severity).toLowerCase()
  const color = SEVERITY_COLOR[severityKey] ?? "#00f0ff"
  const confidencePct = Math.round((alert.confidence ?? 0) * 100)
  const amount = new Intl.NumberFormat("tr-TR", {
    style: "currency",
    currency: alert.currency || "TRY",
    maximumFractionDigits: 0,
  }).format(alert.amount ?? 0)

  return (
    <animated.group
      ref={groupRef}
      scale={springs.scale}
      // useFrame içinde position ayarlandığı için burada sadece başlangıç değeri.
      position={[0, 0, distance]}
    >
      <Html
        transform
        distanceFactor={4}
        zIndexRange={[100, 0]}
        occlude={false}
        style={{ pointerEvents: "none", userSelect: "none" }}
      >
        <div
          style={{
            width: 260,
            padding: "14px 16px",
            borderRadius: 12,
            // Glass-morphism: yarı saydam arka plan + blur + neon kenar.
            background: "rgba(10, 10, 26, 0.72)",
            backdropFilter: "blur(12px)",
            WebkitBackdropFilter: "blur(12px)",
            border: `1px solid ${color}66`,
            boxShadow: `0 0 24px ${color}44, 0 8px 32px rgba(0,0,0,0.5)`,
            color: "#e5e7eb",
            fontFamily:
              "ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif",
            opacity: visible ? 1 : 0,
            transition: "opacity 300ms ease",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              marginBottom: 8,
            }}
          >
            <span
              style={{
                fontSize: 10,
                fontWeight: 700,
                letterSpacing: 1.5,
                textTransform: "uppercase",
                color,
                textShadow: `0 0 8px ${color}aa`,
              }}
            >
              {severityKey} alert
            </span>
            <span
              style={{
                fontSize: 10,
                color: "#71717a",
                fontFamily: "ui-monospace, monospace",
              }}
            >
              {confidencePct}% conf
            </span>
          </div>

          <div
            style={{
              fontSize: 14,
              fontWeight: 600,
              color: "#fafafa",
              marginBottom: 4,
            }}
          >
            {fraudLabel(alert.fraud_type)}
          </div>

          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              marginTop: 8,
            }}
          >
            <span
              style={{
                fontSize: 15,
                fontWeight: 700,
                color,
                fontFamily: "ui-monospace, monospace",
                textShadow: `0 0 10px ${color}88`,
              }}
            >
              {amount}
            </span>
            <span style={{ fontSize: 10, color: "#71717a" }}>
              {alert.sender_city || "—"} → {alert.receiver_city || "—"}
            </span>
          </div>

          {alert.description ? (
            <div
              style={{
                fontSize: 11,
                color: "#a1a1aa",
                marginTop: 8,
                lineHeight: 1.4,
                display: "-webkit-box",
                WebkitLineClamp: 2,
                WebkitBoxOrient: "vertical",
                overflow: "hidden",
              }}
            >
              {alert.description}
            </div>
          ) : null}
        </div>
      </Html>
    </animated.group>
  )
}