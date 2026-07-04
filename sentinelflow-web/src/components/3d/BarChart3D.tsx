"use client"

import { useRef, useMemo } from "react"
import { Canvas, useFrame } from "@react-three/fiber"
import { Text } from "@react-three/drei"
import * as THREE from "three"
import type { BarDatum } from "./types"

function Bars({ data }: { data: BarDatum[] }) {
  const groupRef = useRef<THREE.Group>(null!)
  const barsRef = useRef<THREE.InstancedMesh>(null!)
  const count = data.length
  const maxVal = Math.max(...data.map((d) => d.value), 1)

  const { heights, colors } = useMemo(() => {
    const h = new Float32Array(count)
    const c = new Float32Array(count * 3)
    data.forEach((d, i) => {
      h[i] = (d.value / maxVal) * 3
      const t = i / Math.max(count - 1, 1)
      c[i * 3] = 0 + t * 1 // R: 0 → 1
      c[i * 3 + 1] = 0.9 - t * 0.6 // G: 0.9 → 0.3
      c[i * 3 + 2] = 1 - t * 0.8 // B: 1 → 0.2
    })
    return { heights: h, colors: c }
  }, [data])

  useFrame((state) => {
    if (!barsRef.current) return
    const dummy = new THREE.Object3D()
    const color = new THREE.Color()
    const time = state.clock.elapsedTime

    data.forEach((d, i) => {
      const h = heights[i]
      const spacing = 0.8
      const startX = -((count - 1) * spacing) / 2
      const animH = h * (0.6 + 0.4 * Math.sin(time * 0.5 + i * 0.5 + 1))
      const x = startX + i * spacing

      dummy.position.set(x, animH / 2, 0)
      dummy.scale.set(0.25, animH, 0.25)
      dummy.updateMatrix()
      barsRef.current.setMatrixAt(i, dummy.matrix)

      color.setRGB(colors[i * 3], colors[i * 3 + 1], colors[i * 3 + 2])
      barsRef.current.setColorAt(i, color)
    })
    barsRef.current.instanceMatrix.needsUpdate = true
    if (barsRef.current.instanceColor) barsRef.current.instanceColor.needsUpdate = true
  })

  return (
    <group ref={groupRef}>
      <instancedMesh ref={barsRef} args={[undefined, undefined, count]}>
        <boxGeometry args={[1, 1, 1]} />
        <meshStandardMaterial roughness={0.3} metalness={0.6} vertexColors />
      </instancedMesh>
      {data.map((d, i) => {
        const spacing = 0.8
        const startX = -((count - 1) * spacing) / 2
        return (
          <Text
            key={d.label}
            position={[startX + i * spacing, -0.3, 0]}
            fontSize={0.12}
            color="#556688"
            anchorX="center"
          >
            {d.label.slice(0, 4)}
          </Text>
        )
      })}
    </group>
  )
}

interface BarChart3DProps {
  data: BarDatum[]
}

export default function BarChart3D({ data }: BarChart3DProps) {
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
        camera={{ position: [0, 2, 4], fov: 50 }}
        gl={{ antialias: true, alpha: false }}
        dpr={[1, 2]}
      >
        <ambientLight intensity={0.4} />
        <directionalLight position={[5, 5, 5]} intensity={1} />
        <Bars data={data} />
        <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, -0.05, 0]}>
          <planeGeometry args={[6, 6]} />
          <meshBasicMaterial color="#050510" />
        </mesh>
      </Canvas>
    </div>
  )
}
