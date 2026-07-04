"use client"

import { useRef, useMemo, useState, useEffect } from "react"
import { Canvas, useFrame, useThree } from "@react-three/fiber"
import { OrbitControls, Html } from "@react-three/drei"
import * as THREE from "three"
import {
  CITIES,
  CITY_MAP,
  ALL_TRANSFERS,
  type TransferArc,
  type City,
} from "@/lib/geo-data"

const R = 5

function latLngToVec3(lat: number, lng: number, r: number): THREE.Vector3 {
  const phi = (90 - lat) * (Math.PI / 180)
  const theta = (lng + 180) * (Math.PI / 180)
  return new THREE.Vector3(
    -r * Math.sin(phi) * Math.cos(theta),
    r * Math.cos(phi),
    r * Math.sin(phi) * Math.sin(theta),
  )
}

function Globe() {
  const ref = useRef<THREE.Mesh>(null!)
  useFrame(() => {
    if (ref.current) ref.current.rotation.y += 0.0004
  })
  return (
    <mesh ref={ref}>
      <sphereGeometry args={[R, 96, 96]} />
      <meshStandardMaterial
        color="#0a1525"
        emissive="#001020"
        emissiveIntensity={0.4}
        roughness={0.7}
        metalness={0.1}
      />
    </mesh>
  )
}

function GridLines() {
  const geo = useMemo(() => {
    const s = new THREE.SphereGeometry(R + 0.02, 32, 20)
    return new THREE.EdgesGeometry(s)
  }, [])
  return (
    <lineSegments geometry={geo}>
      <lineBasicMaterial color="#00e5c7" opacity={0.06} transparent />
    </lineSegments>
  )
}

function Atmosphere() {
  return (
    <mesh scale={1.06}>
      <sphereGeometry args={[R, 48, 48]} />
      <meshBasicMaterial color="#00e5c7" transparent opacity={0.04} side={THREE.BackSide} />
    </mesh>
  )
}

function makeCurve(from: City, to: City) {
  const start = latLngToVec3(from.lat, from.lng, R)
  const end = latLngToVec3(to.lat, to.lng, R)
  const dist = start.distanceTo(end)
  const mid = start.clone().add(end).multiplyScalar(0.5)
  mid.normalize().multiplyScalar(R * (1 + dist * 0.08))
  return {
    curve: new THREE.QuadraticBezierCurve3(start, mid, end),
    mid,
    start,
    end,
  }
}

function TransferArcMesh({ arc, idx }: { arc: TransferArc; idx: number }) {
  const from = CITY_MAP[arc.from]
  const to = CITY_MAP[arc.to]
  const { curve } = useMemo(() => makeCurve(from, to), [from, to])

  const isImp = arc.kind === "impossible"
  const isSusp = arc.kind === "suspicious"
  const color = isImp ? "#ff4d5e" : isSusp ? "#ffb020" : "#00e5c7"
  const baseOp = isImp ? 0.8 : isSusp ? 0.55 : 0.2

  const lineObj = useMemo(() => {
    const pts = curve.getPoints(48)
    const g = new THREE.BufferGeometry().setFromPoints(pts)
    const m = new THREE.LineBasicMaterial({
      color: new THREE.Color(color),
      transparent: true,
      opacity: baseOp,
    })
    return new THREE.Line(g, m)
  }, [curve, color, baseOp])

  useFrame((state) => {
    const mat = lineObj.material as THREE.LineBasicMaterial
    if (isImp) {
      const p = 0.5 + Math.sin(state.clock.elapsedTime * 3 + idx) * 0.4
      mat.opacity = 0.35 + p * 0.55
    } else {
      mat.opacity = baseOp
    }
  })

  return (
    <>
      <primitive object={lineObj} />
      <FlowParticle curve={curve} color={color} offset={idx * 0.13} size={isImp ? 0.08 : 0.05} />
    </>
  )
}

function FlowParticle({
  curve,
  color,
  offset,
  size,
}: {
  curve: THREE.QuadraticBezierCurve3
  color: string
  offset: number
  size: number
}) {
  const ref = useRef<THREE.Mesh>(null!)
  useFrame((state) => {
    if (!ref.current) return
    const t = ((state.clock.elapsedTime * 0.22 + offset) % 1 + 1) % 1
    ref.current.position.copy(curve.getPoint(t))
  })
  return (
    <mesh ref={ref}>
      <sphereGeometry args={[size, 8, 8]} />
      <meshBasicMaterial color={color} />
    </mesh>
  )
}

function CityMarker({ city, zoom }: { city: City; zoom: number }) {
  const ref = useRef<THREE.Mesh>(null!)
  const pos = useMemo(() => latLngToVec3(city.lat, city.lng, R + 0.05), [city])
  const showLabel = zoom > 0.45

  useFrame((state) => {
    if (!ref.current) return
    const s = 1 + Math.sin(state.clock.elapsedTime * 2 + pos.x) * 0.15
    ref.current.scale.set(s, s, s)
  })

  return (
    <group position={pos}>
      <mesh ref={ref}>
        <sphereGeometry args={[0.07, 12, 12]} />
        <meshBasicMaterial color="#00e5c7" />
      </mesh>
      <mesh scale={2.4}>
        <sphereGeometry args={[0.07, 12, 12]} />
        <meshBasicMaterial color="#00e5c7" transparent opacity={0.12} />
      </mesh>
      {showLabel && (
        <Html center distanceFactor={8} position={[0, 0.28, 0]} style={{ pointerEvents: "none" }}>
          <div
            style={{
              fontFamily: "'JetBrains Mono', ui-monospace, monospace",
              fontSize: "11px",
              color: "#00e5c7",
              whiteSpace: "nowrap",
              textShadow: "0 0 6px rgba(0,229,199,0.6)",
              opacity: 0.9,
            }}
          >
            {city.code} - {city.name}
          </div>
        </Html>
      )}
    </group>
  )
}

function ImpossibleLabel({ arc, zoom }: { arc: TransferArc; zoom: number }) {
  const from = CITY_MAP[arc.from]
  const to = CITY_MAP[arc.to]
  const { mid } = useMemo(() => makeCurve(from, to), [from, to])
  if (zoom < 0.4) return null
  return (
    <Html center position={mid.toArray()} distanceFactor={6} style={{ pointerEvents: "none" }}>
      <div
        style={{
          fontFamily: "'JetBrains Mono', ui-monospace, monospace",
          fontSize: "9px",
          color: "#ff4d5e",
          whiteSpace: "nowrap",
          textShadow: "0 0 8px rgba(255,77,94,0.8)",
          background: "rgba(255,77,94,0.1)",
          border: "1px solid rgba(255,77,94,0.3)",
          padding: "2px 6px",
          borderRadius: "3px",
        }}
      >
        {arc.label}
      </div>
    </Html>
  )
}

function ZoomTracker({ onZoom }: { onZoom: (z: number) => void }) {
  const { camera } = useThree()
  useEffect(() => {
    const update = () => {
      const dist = camera.position.length()
      const z = Math.max(0, Math.min(1, (18 - dist) / 11))
      onZoom(z)
    }
    update()
  }, [camera, onZoom])
  useFrame(() => {
    const dist = camera.position.length()
    const z = Math.max(0, Math.min(1, (18 - dist) / 11))
    onZoom(z)
  })
  return null
}

export interface GlobeSceneProps {
  alerts?: never[]
  activeAlert?: null
  onAlertDismiss?: () => void
}

export default function GlobeScene() {
  const [zoom, setZoom] = useState(0)
  const prefersReduced =
    typeof window !== "undefined" &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches

  return (
    <div className="w-full h-full" style={{ background: "#050810" }}>
      <Canvas
        camera={{ position: [0, 2, 14], fov: 45, near: 0.1, far: 100 }}
        gl={{
          antialias: true,
          alpha: false,
          toneMapping: THREE.ACESFilmicToneMapping,
          toneMappingExposure: 1.2,
        }}
        dpr={[1, 2]}
      >
        <ambientLight intensity={0.3} color="#0a1a3a" />
        <pointLight position={[10, 10, 10]} intensity={1.2} color="#00e5c7" />
        <pointLight position={[-10, -10, 5]} intensity={0.6} color="#ff4d5e" />

        <Globe />
        <GridLines />
        <Atmosphere />

        {CITIES.map((c) => (
          <CityMarker key={c.id} city={c} zoom={zoom} />
        ))}

        {ALL_TRANSFERS.map((arc, i) => (
          <TransferArcMesh key={i} arc={arc} idx={i} />
        ))}

        {ALL_TRANSFERS.filter((a) => a.kind === "impossible").map((arc, i) => (
          <ImpossibleLabel key={i} arc={arc} zoom={zoom} />
        ))}

        <ZoomTracker onZoom={setZoom} />

        <OrbitControls
          enableDamping
          dampingFactor={0.05}
          minDistance={7}
          maxDistance={20}
          autoRotate={!prefersReduced}
          autoRotateSpeed={0.3}
          enablePan={false}
        />
      </Canvas>
    </div>
  )
}
