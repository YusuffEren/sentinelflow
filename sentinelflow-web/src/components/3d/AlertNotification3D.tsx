"use client"

import { useRef, useState } from "react"
import { Canvas, useFrame } from "@react-three/fiber"
import { Text } from "@react-three/drei"
import * as THREE from "three"

interface Alert {
  alert_id: string
  fraud_type: string
  severity: string
  description: string
  amount: number
  detected_at: string
}

function AlertCard({ alert, index, onDismiss }: { alert: Alert; index: number; onDismiss: () => void }) {
  const meshRef = useRef<THREE.Mesh>(null!)
  const [hovered, setHovered] = useState(false)
  const entryTime = useRef(Date.now())

  useFrame((state) => {
    if (!meshRef.current) return
    const elapsed = (Date.now() - entryTime.current) / 1000
    // Slide-in animation (first 0.5s)
    if (elapsed < 0.5) {
      const t = elapsed / 0.5
      const eased = 1 - Math.pow(1 - t, 3) // ease-out cubic
      meshRef.current.position.x = 8 * (1 - eased)
    } else {
      meshRef.current.position.x = 0
    }
    // Floating
    const floatY = Math.sin(state.clock.elapsedTime * 0.5 + index) * 0.05
    meshRef.current.position.y = floatY
  })

  const severityColor = alert.severity === "critical" ? "#ff3333"
    : alert.severity === "high" ? "#ff6600"
    : alert.severity === "medium" ? "#ffaa00"
    : "#00f0ff"

  return (
    <group
      position={[0, 6 - index * 1.5, 0]}
      onPointerOver={() => setHovered(true)}
      onPointerOut={() => setHovered(false)}
      onClick={onDismiss}
    >
      <mesh ref={meshRef} position={[8, 0, 0]}>
        <planeGeometry args={[3.2, 0.8]} />
        <meshBasicMaterial
          color={severityColor}
          transparent
          opacity={hovered ? 0.25 : 0.12}
          side={THREE.DoubleSide}
        />
      </mesh>
      <mesh position={[0, 0, 0.01]}>
        <planeGeometry args={[3.1, 0.7]} />
        <meshBasicMaterial color="#0a0a1a" transparent opacity={0.85} side={THREE.DoubleSide} />
      </mesh>
      <Text position={[-1.4, 0.2, 0.02]} fontSize={0.08} color={severityColor}>
        {alert.severity.toUpperCase()}
      </Text>
      <Text position={[-1.4, -0.1, 0.02]} fontSize={0.06} color="#8899bb">
        {alert.description.slice(0, 28)}
      </Text>
      <Text position={[1.2, 0.2, 0.02]} fontSize={0.07} color="#00f0ff">
        ${(alert.amount || 0).toLocaleString()}
      </Text>
    </group>
  )
}

interface AlertNotification3DProps {
  alerts: Alert[]
  onDismiss: (id: string) => void
}

export default function AlertNotification3D({ alerts, onDismiss }: AlertNotification3DProps) {
  const visible = alerts.slice(0, 4)
  if (visible.length === 0) return null

  return (
    <div className="fixed top-4 right-4 w-80 h-96 pointer-events-none z-50">
      <Canvas
        camera={{ position: [0, 5, 10], fov: 30 }}
        gl={{ alpha: true, antialias: true }}
        style={{ background: "transparent" }}
        dpr={[1, 2]}
      >
        <ambientLight intensity={0.5} />
        {visible.map((alert, i) => (
          <AlertCard
            key={alert.alert_id}
            alert={alert}
            index={i}
            onDismiss={() => onDismiss(alert.alert_id)}
          />
        ))}
      </Canvas>
    </div>
  )
}
