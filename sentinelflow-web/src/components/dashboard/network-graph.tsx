"use client"

import { useEffect, useRef, useState, useCallback } from "react"
import dynamic from "next/dynamic"
import { Card } from "@/components/ui/card"
import { Maximize2, RefreshCw, AlertTriangle, Network } from "lucide-react"
import { config, getApiUrl } from "@/lib/config"

const ForceGraph2D = dynamic(() => import("react-force-graph-2d"), {
    ssr: false,
    loading: () => (
        <div className="flex items-center justify-center h-full text-zinc-500">
            <Network className="w-8 h-8 animate-pulse" />
        </div>
    )
})

interface GraphNode {
    id: string
    label: string
    group: number
    amount_total?: number
    tx_count?: number
    is_fraud?: boolean
}

interface GraphEdge {
    source: string
    target: string
    amount: number
    color?: string
    is_fraud?: boolean
}

interface GraphData {
    nodes: GraphNode[]
    links: GraphEdge[]
    metadata?: Record<string, any>
}

interface NetworkGraphProps {
    data?: GraphData
    autoFetch?: boolean
    showFraudOnly?: boolean
}

export function NetworkGraph({ 
    data: externalData, 
    autoFetch = true,
    showFraudOnly = false 
}: NetworkGraphProps) {
    const fgRef = useRef<any>(null)
    const containerRef = useRef<HTMLDivElement>(null)
    const [dimensions, setDimensions] = useState({ width: 0, height: 0 })
    const [graphData, setGraphData] = useState<GraphData>({ nodes: [], links: [] })
    const [loading, setLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null)

    const fetchGraphData = useCallback(async () => {
        if (!autoFetch || externalData) return
        
        setLoading(true)
        setError(null)
        
        try {
            const params = new URLSearchParams({
                limit: "100",
                hours: "24",
                include_fraud_only: showFraudOnly.toString(),
            })
            
            const res = await fetch(getApiUrl(`${config.endpoints.graphNodes}/../data?${params}`))
            
            if (res.ok) {
                const data: GraphData = await res.json()
                setGraphData(data)
            } else {
                setError("API bağlantısı başarısız")
            }
        } catch (e) {
            console.error("Failed to fetch graph data", e)
            setError("Bağlantı hatası")
            setGraphData(generateMockData())
        } finally {
            setLoading(false)
        }
    }, [autoFetch, externalData, showFraudOnly])

    useEffect(() => {
        if (containerRef.current) {
            const updateDimensions = () => {
                if (containerRef.current) {
                    setDimensions({
                        width: containerRef.current.clientWidth,
                        height: containerRef.current.clientHeight
                    })
                }
            }
            
            updateDimensions()
            
            const observer = new ResizeObserver(updateDimensions)
            observer.observe(containerRef.current)
            
            return () => observer.disconnect()
        }
    }, [])

    useEffect(() => {
        if (externalData) {
            setGraphData(externalData)
        } else {
            fetchGraphData()
        }
    }, [externalData, fetchGraphData])

    const handleNodeClick = useCallback((node: Record<string, unknown>, _event: MouseEvent) => {
        const typedNode = node as unknown as GraphNode
        setSelectedNode(typedNode)
        if (fgRef.current && typeof node.x === 'number' && typeof node.y === 'number') {
            fgRef.current.centerAt(node.x, node.y, 1000)
            fgRef.current.zoom(2, 1000)
        }
    }, [])

    const displayData = externalData || graphData
    const fraudCount = displayData.nodes.filter(n => n.is_fraud).length

    return (
        <Card className="h-full bg-zinc-900/50 border-zinc-800 relative overflow-hidden flex flex-col">
            {/* Header */}
            <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800">
                <div className="flex items-center gap-3">
                    <Network className="w-5 h-5 text-blue-400" />
                    <h3 className="text-white font-semibold">İşlem Ağı</h3>
                    <span className="text-[10px] text-zinc-500 px-2 py-0.5 rounded-full bg-zinc-800 border border-zinc-700">
                        {displayData.nodes.length} düğüm
                    </span>
                    {fraudCount > 0 && (
                        <span className="text-[10px] text-red-400 px-2 py-0.5 rounded-full bg-red-500/10 border border-red-500/20 flex items-center gap-1">
                            <AlertTriangle className="w-3 h-3" />
                            {fraudCount} şüpheli
                        </span>
                    )}
                </div>

                <div className="flex items-center gap-2">
                    <button 
                        onClick={fetchGraphData}
                        disabled={loading}
                        className="p-1.5 rounded-md bg-zinc-800 text-zinc-400 hover:text-white hover:bg-zinc-700 transition-colors disabled:opacity-50"
                    >
                        <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
                    </button>
                    <button 
                        onClick={() => fgRef.current?.zoomToFit(400)}
                        className="p-1.5 rounded-md bg-zinc-800 text-zinc-400 hover:text-white hover:bg-zinc-700 transition-colors"
                    >
                        <Maximize2 className="h-4 w-4" />
                    </button>
                </div>
            </div>

            {/* Graph Container */}
            <div ref={containerRef} className="flex-1 w-full relative">
                {error && (
                    <div className="absolute top-2 left-2 right-2 z-10 px-3 py-2 bg-yellow-500/10 border border-yellow-500/20 rounded-lg text-yellow-400 text-xs">
                        {error} - Demo verisi gösteriliyor
                    </div>
                )}
                
                {dimensions.width > 0 && displayData.nodes.length > 0 && (
                    <ForceGraph2D
                        ref={fgRef}
                        width={dimensions.width}
                        height={dimensions.height}
                        graphData={displayData}
                        nodeLabel={(node: any) => `${node.label}\n${node.tx_count || 0} işlem`}
                        nodeColor={(node: any) => {
                            if (node.is_fraud || node.group === 1) return "#ef4444"
                            if (node.group === 2) return "#3b82f6"
                            return "#10b981"
                        }}
                        nodeRelSize={6}
                        nodeCanvasObjectMode={() => "after"}
                        nodeCanvasObject={(node: any, ctx, globalScale) => {
                            if (node.is_fraud) {
                                ctx.beginPath()
                                ctx.arc(node.x, node.y, 8, 0, 2 * Math.PI)
                                ctx.strokeStyle = "#ef4444"
                                ctx.lineWidth = 2 / globalScale
                                ctx.stroke()
                            }
                        }}
                        linkColor={(link: any) => link.is_fraud ? "#ef4444" : link.color || "#334155"}
                        linkWidth={(link: any) => link.is_fraud ? 2 : 1}
                        linkDirectionalArrowLength={3.5}
                        linkDirectionalArrowRelPos={1}
                        linkDirectionalParticles={(link: any) => link.is_fraud ? 4 : 2}
                        linkDirectionalParticleSpeed={0.005}
                        linkDirectionalParticleWidth={2}
                        linkDirectionalParticleColor={(link: any) => link.is_fraud ? "#ef4444" : "#3b82f6"}
                        backgroundColor="transparent"
                        d3VelocityDecay={0.3}
                        cooldownTicks={100}
                        onNodeClick={handleNodeClick}
                        onEngineStop={() => fgRef.current?.zoomToFit(400, 50)}
                    />
                )}
                
                {displayData.nodes.length === 0 && !loading && (
                    <div className="absolute inset-0 flex flex-col items-center justify-center text-zinc-500">
                        <Network className="w-12 h-12 mb-3 opacity-50" />
                        <p>Henüz işlem verisi yok</p>
                    </div>
                )}
            </div>

            {/* Selected Node Info */}
            {selectedNode && (
                <div className="absolute bottom-4 left-4 right-4 p-3 bg-zinc-900/95 border border-zinc-700 rounded-lg backdrop-blur-sm">
                    <div className="flex items-center justify-between">
                        <div>
                            <p className="text-white font-medium">{selectedNode.label}</p>
                            <p className="text-xs text-zinc-500">
                                {selectedNode.tx_count || 0} işlem • {(selectedNode.amount_total || 0).toLocaleString("tr-TR")} TRY
                            </p>
                        </div>
                        {selectedNode.is_fraud && (
                            <span className="px-2 py-1 text-xs bg-red-500/20 text-red-400 rounded border border-red-500/30">
                                Şüpheli
                            </span>
                        )}
                        <button 
                            onClick={() => setSelectedNode(null)}
                            className="text-zinc-500 hover:text-white"
                        >
                            ✕
                        </button>
                    </div>
                </div>
            )}

            {/* Legend */}
            <div className="absolute bottom-4 right-4 flex items-center gap-3 text-[10px] text-zinc-500">
                <span className="flex items-center gap-1">
                    <span className="w-2 h-2 rounded-full bg-red-500" />
                    Fraud
                </span>
                <span className="flex items-center gap-1">
                    <span className="w-2 h-2 rounded-full bg-blue-500" />
                    Gönderen
                </span>
                <span className="flex items-center gap-1">
                    <span className="w-2 h-2 rounded-full bg-emerald-500" />
                    Alıcı
                </span>
            </div>
        </Card>
    )
}

function generateMockData(): GraphData {
    const nodes: GraphNode[] = []
    const links: GraphEdge[] = []
    
    for (let i = 0; i < 30; i++) {
        const isFraud = Math.random() < 0.15
        nodes.push({
            id: `ACC${i.toString().padStart(4, "0")}`,
            label: `Hesap ${i}`,
            group: isFraud ? 1 : (Math.random() < 0.3 ? 2 : 0),
            amount_total: Math.random() * 100000,
            tx_count: Math.floor(Math.random() * 20) + 1,
            is_fraud: isFraud,
        })
    }
    
    for (let i = 0; i < 50; i++) {
        const sourceIdx = Math.floor(Math.random() * nodes.length)
        const targetIdx = Math.floor(Math.random() * nodes.length)
        
        if (sourceIdx === targetIdx) continue
        
        const isFraud = nodes[sourceIdx].is_fraud || nodes[targetIdx].is_fraud
        
        links.push({
            source: nodes[sourceIdx].id,
            target: nodes[targetIdx].id,
            amount: Math.random() * 50000,
            color: isFraud ? "#ef4444" : "#334155",
            is_fraud: isFraud,
        })
    }
    
    return { nodes, links, metadata: { is_mock: true } }
}
