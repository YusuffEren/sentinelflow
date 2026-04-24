"use client"

import { useEffect, useRef, useState, useCallback } from "react"
import { config, getWsUrl } from "@/lib/config"

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

const DEFAULT_WS_URL = getWsUrl(config.endpoints.wsAlerts)

export function useWebSocket(url: string = DEFAULT_WS_URL) {
    const [status, setStatus] = useState<WebSocketStatus>("disconnected")
    const [alerts, setAlerts] = useState<Alert[]>([])
    const [isConnected, setIsConnected] = useState(false)
    const [reconnectAttempts, setReconnectAttempts] = useState(0)
    const ws = useRef<WebSocket | null>(null)
    const reconnectTimeout = useRef<NodeJS.Timeout | undefined>(undefined)
    const maxReconnectAttempts = 10

    const connect = useCallback(() => {
        if (reconnectAttempts >= maxReconnectAttempts) {
            console.error("Max reconnection attempts reached")
            setStatus("error")
            return
        }

        try {
            setStatus("connecting")
            ws.current = new WebSocket(url)

            ws.current.onopen = () => {
                setStatus("connected")
                setIsConnected(true)
                setReconnectAttempts(0)
                console.log("WebSocket Connected to:", url)
            }

            ws.current.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data)

                    if (data.type === "connection" || data.type === "pong") {
                        return
                    }

                    if (data.alert_id) {
                        setAlerts((prev) => [data, ...prev].slice(0, config.ui.maxAlertsInFeed))
                    }
                } catch (e) {
                    console.error("Failed to parse WebSocket message", e)
                }
            }

            ws.current.onclose = (event) => {
                setStatus("disconnected")
                setIsConnected(false)
                console.log("WebSocket Disconnected, code:", event.code)
                
                if (!event.wasClean) {
                    const delay = Math.min(
                        config.ui.wsReconnectDelay * Math.pow(2, reconnectAttempts),
                        30000
                    )
                    console.log(`Reconnecting in ${delay}ms (attempt ${reconnectAttempts + 1})`)
                    setReconnectAttempts((prev) => prev + 1)
                    reconnectTimeout.current = setTimeout(connect, delay)
                }
            }

            ws.current.onerror = (error) => {
                console.error("WebSocket Error:", error)
                setStatus("error")
            }
        } catch (e) {
            console.error("WebSocket Connection Failed", e)
            setStatus("error")
            const delay = config.ui.wsReconnectDelay * Math.pow(2, reconnectAttempts)
            setReconnectAttempts((prev) => prev + 1)
            reconnectTimeout.current = setTimeout(connect, delay)
        }
    }, [url, reconnectAttempts])

    useEffect(() => {
        connect()

        const pingInterval = setInterval(() => {
            if (ws.current?.readyState === WebSocket.OPEN) {
                ws.current.send("ping")
            }
        }, config.ui.wsPingInterval)

        return () => {
            if (ws.current) {
                ws.current.onclose = null
                ws.current.close()
            }
            clearInterval(pingInterval)
            clearTimeout(reconnectTimeout.current)
        }
    }, []) // eslint-disable-line react-hooks/exhaustive-deps

    const reconnect = useCallback(() => {
        setReconnectAttempts(0)
        if (ws.current) {
            ws.current.onclose = null
            ws.current.close()
        }
        connect()
    }, [connect])

    return { status, isConnected, alerts, reconnect, reconnectAttempts }
}
