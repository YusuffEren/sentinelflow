"use client"

import { useEffect, useRef, useState } from "react"
// import ForceGraph2D from "react-force-graph-2d" 
// We import dynamically to avoid SSR issues with canvas
import dynamic from "next/dynamic"
import { Card } from "@/components/ui/card"
import { Maximize2, RefreshCw } from "lucide-react"

const ForceGraph2D = dynamic(() => import("react-force-graph-2d"), {
    ssr: false,
    loading: () => <div className="flex items-center justify-center h-full text-slate-500">Loading Visualization...</div>
})

interface NetworkGraphProps {
    data: {
        nodes: any[]
        links: any[]
    }
}

export function NetworkGraph({ data }: NetworkGraphProps) {
    const fgRef = useRef<any>(null)
    const containerRef = useRef<HTMLDivElement>(null)
    const [dimensions, setDimensions] = useState({ width: 0, height: 0 })

    useEffect(() => {
        if (containerRef.current) {
            setDimensions({
                width: containerRef.current.clientWidth,
                height: containerRef.current.clientHeight
            })
        }

        // Resize observer could he added here
    }, [])

    return (
        <Card className="col-span-1 md:col-span-2 lg:col-span-2 h-[400px] bg-black/40 border-white/10 relative overflow-hidden flex flex-col">
            <div className="absolute top-4 left-4 z-10 flex gap-2">
                <h3 className="text-white font-semibold flex items-center gap-2">
                    Traffic Analysis
                    <span className="text-[10px] text-slate-500 font-normal px-2 py-0.5 rounded-full bg-white/5 border border-white/10">
                        Real-time
                    </span>
                </h3>
            </div>

            <div className="absolute top-4 right-4 z-10 flex gap-2">
                <button className="p-1.5 rounded-md bg-white/5 text-slate-400 hover:text-white hover:bg-white/10">
                    <RefreshCw className="h-4 w-4" />
                </button>
                <button className="p-1.5 rounded-md bg-white/5 text-slate-400 hover:text-white hover:bg-white/10">
                    <Maximize2 className="h-4 w-4" />
                </button>
            </div>

            <div ref={containerRef} className="flex-1 w-full h-full">
                {dimensions.width > 0 && (
                    <ForceGraph2D
                        ref={fgRef}
                        width={dimensions.width}
                        height={dimensions.height}
                        graphData={data}
                        nodeLabel="id"
                        nodeColor={(node: any) => {
                            if (node.group === 1) return "#ef4444" // Fraud Ring (Red)
                            if (node.group === 2) return "#3b82f6" // Sender (Blue)
                            return "#10b981" // Receiver (Green)
                        }}
                        nodeRelSize={6}
                        linkColor={(link: any) => link.color || "#334155"}
                        linkDirectionalArrowLength={3.5}
                        linkDirectionalArrowRelPos={1}
                        linkDirectionalParticles={2}
                        linkDirectionalParticleSpeed={0.005}
                        linkDirectionalParticleWidth={2}
                        backgroundColor="transparent"
                        d3VelocityDecay={0.3}
                        cooldownTicks={100}
                        onEngineStop={() => fgRef.current?.zoomToFit(400)}
                    />
                )}
            </div>
        </Card>
    )
}
