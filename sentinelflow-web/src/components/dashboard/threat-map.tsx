"use client"

import { useState, useEffect } from "react"
import dynamic from "next/dynamic"
import { Map } from "lucide-react"

const LeafletMap = dynamic(() => import("./leaflet-map"), { 
  ssr: false, 
  loading: () => (
    <div className="w-full h-full bg-zinc-950 flex items-center justify-center">
      <div className="text-zinc-600 text-sm">Loading map...</div>
    </div>
  ),
})

interface ThreatMapProps {
  alerts: any[]
}

export function ThreatMap({ alerts }: ThreatMapProps) {
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    setMounted(true)
  }, [])

  return (
    <div className="h-full flex flex-col bg-zinc-900 border border-zinc-800 rounded-lg overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800">
        <div className="flex items-center gap-2">
          <Map className="w-4 h-4 text-zinc-400" />
          <h3 className="text-sm font-medium text-zinc-100">Threat Map</h3>
        </div>
        <div className="flex items-center gap-4 text-[10px] text-zinc-500">
          <div className="flex items-center gap-1.5">
            <div className="w-2 h-2 rounded-full bg-blue-500" />
            <span>Source</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-2 h-2 rounded-full bg-red-500" />
            <span>Target</span>
          </div>
        </div>
      </div>

      {/* Map */}
      <div className="flex-1 bg-zinc-950">
        {mounted && <LeafletMap alerts={alerts} />}
      </div>
    </div>
  )
}
