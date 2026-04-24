"use client"

import { useState, useRef, useEffect } from "react"
import { Bot, Send, ChevronUp, ChevronDown, Sparkles } from "lucide-react"
import { config, getApiUrl } from "@/lib/config"

interface Message {
  role: "user" | "assistant"
  content: string
}

export function AiChat() {
  const [isExpanded, setIsExpanded] = useState(false)
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState("")
  const [isTyping, setIsTyping] = useState(false)
  const scrollRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }, [messages])

  const handleSend = async () => {
    if (!input.trim()) return

    const userMsg = input
    setMessages(prev => [...prev, { role: "user", content: userMsg }])
    setInput("")
    setIsTyping(true)
    setIsExpanded(true)

    try {
      const res = await fetch(getApiUrl(config.endpoints.chat), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message: userMsg,
          context: { amount: 150000, fraud_type: "whale_anomaly" }
        })
      })

      if (res.ok) {
        const data = await res.json()
        setMessages(prev => [...prev, { role: "assistant", content: data.response }])
      } else {
        setMessages(prev => [...prev, { role: "assistant", content: "AI servisine bağlanılamıyor. Lütfen tekrar deneyin." }])
      }
    } catch {
      setMessages(prev => [...prev, { role: "assistant", content: "Bağlantı hatası. Lütfen tekrar deneyin." }])
    } finally {
      setIsTyping(false)
    }
  }

  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-lg overflow-hidden transition-all duration-300">
      {/* Expanded Chat */}
      {isExpanded && (
        <div className="h-[300px] flex flex-col border-b border-zinc-800">
          {/* Header */}
          <div className="flex items-center justify-between px-4 py-2 border-b border-zinc-800 bg-zinc-900/50">
            <div className="flex items-center gap-2">
              <div className="w-6 h-6 rounded bg-indigo-500/10 border border-indigo-500/20 flex items-center justify-center">
                <Sparkles className="w-3 h-3 text-indigo-400" />
              </div>
              <span className="text-sm font-medium text-zinc-200">SentinelAI</span>
            </div>
            <button 
              onClick={() => setIsExpanded(false)}
              className="p-1 hover:bg-zinc-800 rounded transition-colors"
            >
              <ChevronDown className="w-4 h-4 text-zinc-500" />
            </button>
          </div>

          {/* Messages */}
          <div className="flex-1 overflow-y-auto p-4 space-y-3" ref={scrollRef}>
            {messages.length === 0 && (
              <div className="flex flex-col items-center justify-center h-full text-center">
                <Bot className="w-8 h-8 text-zinc-600 mb-2" />
                <p className="text-sm text-zinc-500">Ask about suspicious activities</p>
                <p className="text-xs text-zinc-600 mt-1">I can help analyze patterns and anomalies</p>
              </div>
            )}
            
            {messages.map((msg, i) => (
              <div 
                key={i} 
                className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
              >
                <div className={`
                  max-w-[80%] px-3 py-2 rounded-lg text-sm
                  ${msg.role === "user" 
                    ? "bg-indigo-500 text-white" 
                    : "bg-zinc-800 text-zinc-200 border border-zinc-700"
                  }
                `}>
                  {msg.content}
                </div>
              </div>
            ))}
            
            {isTyping && (
              <div className="flex justify-start">
                <div className="bg-zinc-800 border border-zinc-700 px-3 py-2 rounded-lg flex gap-1">
                  <span className="w-1.5 h-1.5 bg-zinc-500 rounded-full animate-bounce [animation-delay:-0.3s]" />
                  <span className="w-1.5 h-1.5 bg-zinc-500 rounded-full animate-bounce [animation-delay:-0.15s]" />
                  <span className="w-1.5 h-1.5 bg-zinc-500 rounded-full animate-bounce" />
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Input Bar */}
      <div className="p-3">
        <form
          onSubmit={(e) => { e.preventDefault(); handleSend() }}
          className="flex items-center gap-3"
        >
          <div className="flex items-center gap-2 text-zinc-500">
            <Bot className="w-4 h-4" />
          </div>
          
          <input
            type="text"
            placeholder="Ask SentinelAI..."
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onFocus={() => setIsExpanded(true)}
            className="flex-1 bg-transparent text-sm text-zinc-200 placeholder:text-zinc-600 focus:outline-none"
          />
          
          <div className="flex items-center gap-2">
            {!isExpanded && messages.length > 0 && (
              <button
                type="button"
                onClick={() => setIsExpanded(true)}
                className="p-1.5 hover:bg-zinc-800 rounded transition-colors"
              >
                <ChevronUp className="w-4 h-4 text-zinc-500" />
              </button>
            )}
            
            <button
              type="submit"
              disabled={!input.trim()}
              className="p-1.5 bg-indigo-500 hover:bg-indigo-400 disabled:bg-zinc-700 disabled:cursor-not-allowed rounded transition-colors"
            >
              <Send className="w-3.5 h-3.5 text-white" />
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
