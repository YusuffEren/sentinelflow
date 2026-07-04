"use client"

import { useRef, useMemo } from "react"
import { Canvas, useFrame } from "@react-three/fiber"
import { Text } from "@react-three/drei"
import * as THREE from "three"
import type { RingDatum } from "./types"

function Rings({ data }: { data: RingDatum[] }) {
  const groupRef = useRef<THREE.Group>(null!)
  const total = data.reduce((s, d) => s + d.value, 0) || 1

  useFrame((state) => {
    if (groupRef.current) {
      groupRef.current.rotation.y = Math.sin(state.clock.elapsedTime * 0.2) * 0.3
    }
  })

  let accumulated = 0

  return (
    <group ref={groupRef}>
      {data.map((d, i) => {
        const fraction = d.value / total
        const arc = fraction * Math.PI * 2
        const startAngle = (accumulated / total) * Math.PI * 2
        accumulated += d.value

        const radius = 1.2 + i * 0.3
        const color = d.color || "#00f0ff"

        return (
          <RingSegment
            key={d.label}
            radius={radius}
            arc={arc}
            startAngle={startAngle}
            color={color}
            label={d.label}
            value={d.value}
          />
        )
      })}
      <Text position={[0, 0, 0]} fontSize={0.2} color="#8899bb" anchorX="center" anchorY="middle">
        {total}
      </Text>
    </group>
  )
}

function RingSegment({
  radius,
  arc,
  startAngle,
  color,
  label,
  value,
}: {
  radius: number
  arc: number
  startAngle: number
  color: string
  label: string
  value: number
}) {
  const meshRef = useRef<THREE.Mesh>(null!)
  const geometry = useMemo(() => {
    const shape = new THREE.Shape()
    const inner = 0.04
    shape.absarc(0, 0, radius, 0, arc, false)
    shape.absarc(0, 0, radius - inner, arc, 0, true)
    shape.closePath()
    return new THREE.ShapeGeometry(shape)
  }, [radius, arc])

  return (
    <group rotation={[0, 0, startAngle]}>
      <mesh ref={meshRef} geometry={geometry}>
        <meshStandardMaterial
          color={color}
          emissive={color}
          emissiveIntensity={0.3}
          roughness={0.4}
          metalness={0.7}
          side={THREE.DoubleSide}
        />
      </mesh>
    </group>
  )
}

interface RingChart3DProps {
  data: RingDatum[]
}

export default function RingChart3D({ data }: RingChart3DProps) {
  if (data.length === 0) {
    return (
      <div className="w-full h-full flex items-center justify-center">
        <span className="text-zinc-600 text-xs">No data yet</span>
      </div>
    )
  }

  return (
    <div className="w-full h-full" style={{ background: "#050510" }}>
      <Canvas
        camera={{ position: [0, 0, 4], fov: 50 }}
        gl={{ antialias: true, alpha: false }}
        dpr={[1, 2]}
      >
        <ambientLight intensity={0.6} />
        <Rings data={data} />
      </Canvas>
    </div>
  )
}
