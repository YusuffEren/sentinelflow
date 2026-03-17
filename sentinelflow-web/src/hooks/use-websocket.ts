"use client"

import { useEffect, useRef, useState, useCallback } from "react"
// Removed sonner for now

type Alert = {
    alert_id: string
    fraud_type: string
    severity: "low" | "medium" | "high" | "critical"
    confidence: number
    description: string
    detected_at: string
    amount: number
    sender_iban?: string
    receiver_iban?: string
}

type WebSocketStatus = "connecting" | "connected" | "disconnected" | "error"

export function useWebSocket(url: string = "ws://127.0.0.1:8000/ws/alerts") {
    const [status, setStatus] = useState<WebSocketStatus>("disconnected")
    const [alerts, setAlerts] = useState<Alert[]>([])
    const [isConnected, setIsConnected] = useState(false)
    const ws = useRef<WebSocket | null>(null)
    const reconnectTimeout = useRef<NodeJS.Timeout | undefined>(undefined)

    const connect = useCallback(() => {
        try {
            setStatus("connecting")
            ws.current = new WebSocket(url)

            ws.current.onopen = () => {
                setStatus("connected")
                setIsConnected(true)
                console.log("WebSocket Connected")
            }

            ws.current.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data)

                    if (data.type === "connection" || data.type === "pong") {
                        return
                    }

                    // Assume it's an alert if it has alert_id
                    if (data.alert_id) {
                        setAlerts((prev) => [data, ...prev].slice(0, 50)) // Keep last 50
                    }
                } catch (e) {
                    console.error("Failed to parse WebSocket message", e)
                }
            }

            ws.current.onclose = () => {
                setStatus("disconnected")
                setIsConnected(false)
                console.log("WebSocket Disconnected")
                // Reconnect after 3 seconds
                reconnectTimeout.current = setTimeout(connect, 3000)
            }

            ws.current.onerror = (error) => {
                console.error("WebSocket Error:", error)
                setStatus("error")
                ws.current?.close()
            }
        } catch (e) {
            console.error("WebSocket Connection Failed", e)
            setStatus("error")
        }
    }, [url])

    useEffect(() => {
        connect()

        // Heartbeat
        const pingInterval = setInterval(() => {
            if (ws.current?.readyState === WebSocket.OPEN) {
                ws.current.send("ping")
            }
        }, 10000)

        return () => {
            ws.current?.close()
            clearInterval(pingInterval)
            clearTimeout(reconnectTimeout.current)
        }
    }, [connect])

    return { status, isConnected, alerts }
}
