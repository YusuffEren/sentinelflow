"use client"

import { useEffect, useRef, useState } from "react"
import { LucideIcon, TrendingUp, TrendingDown } from "lucide-react"

interface StatCardProps {
  title: string
  value: string | number
  icon: LucideIcon
  description?: string
  trend?: "up" | "down" | "neutral"
  trendValue?: string
  color?: "emerald" | "red" | "blue" | "amber" | "zinc"
}

// Animate number counting up
function useCountUp(end: number, duration: number = 800) {
  const [count, setCount] = useState(0)
  const countRef = useRef(0)
  
  useEffect(() => {
    const startTime = Date.now()
    const startValue = countRef.current
    
    const animate = () => {
      const now = Date.now()
      const progress = Math.min((now - startTime) / duration, 1)
      
      // Ease out cubic
      const easeOut = 1 - Math.pow(1 - progress, 3)
      const current = Math.floor(startValue + (end - startValue) * easeOut)
      
      setCount(current)
      countRef.current = current
      
      if (progress < 1) {
        requestAnimationFrame(animate)
      }
    }
    
    requestAnimationFrame(animate)
  }, [end, duration])
  
  return count
}

export function StatCard({
  title,
  value,
  icon: Icon,
  description,
  trend,
  trendValue,
  color = "zinc",
}: StatCardProps) {
  // Check if value is a placeholder
  const isPlaceholder = value === "—" || value === "-" || value === ""
  
  // Parse numeric value for animation
  const numericValue = typeof value === "string" 
    ? parseInt(value.replace(/[^0-9]/g, "")) || 0
    : value
  
  const animatedValue = useCountUp(isPlaceholder ? 0 : numericValue)
  
  // Format the animated value back to string if needed
  const displayValue = isPlaceholder 
    ? "—"
    : typeof value === "string" && value.includes("%")
    ? value // Keep percentage strings as-is
    : typeof value === "string" && value.includes("m")
    ? value // Keep time strings as-is
    : typeof value === "string" && value.includes(",")
    ? animatedValue.toLocaleString()
    : typeof value === "string" && !value.match(/^\d+$/)
    ? value // Keep non-numeric strings as-is
    : animatedValue.toLocaleString()

  const colorClasses = {
    emerald: "text-emerald-400",
    red: "text-red-400",
    blue: "text-blue-400",
    amber: "text-amber-400",
    zinc: "text-zinc-400",
  }

  const trendColors = {
    up: "text-emerald-400",
    down: "text-red-400",
    neutral: "text-zinc-500",
  }

  return (
    <div className="group relative bg-zinc-900 border border-zinc-800 rounded-lg p-5 transition-all duration-200 hover:bg-zinc-900/80 hover:border-zinc-700">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <span className="text-[11px] font-medium uppercase tracking-wider text-zinc-500">
          {title}
        </span>
        <Icon className={`w-4 h-4 ${colorClasses[color]}`} />
      </div>

      {/* Value */}
      <div className="flex items-baseline gap-2">
        <span className={`text-3xl font-semibold tracking-tight font-mono tabular-nums ${isPlaceholder ? "text-zinc-600" : "text-zinc-50"}`}>
          {displayValue}
        </span>
      </div>

      {/* Footer */}
      <div className="flex items-center gap-2 mt-3">
        {trend && trend !== "neutral" && (
          <div className={`flex items-center gap-1 ${trendColors[trend]}`}>
            {trend === "up" ? (
              <TrendingUp className="w-3 h-3" />
            ) : (
              <TrendingDown className="w-3 h-3" />
            )}
            {trendValue && (
              <span className="text-xs font-medium">{trendValue}</span>
            )}
          </div>
        )}
        {description && (
          <span className="text-xs text-zinc-500">{description}</span>
        )}
      </div>
    </div>
  )
}
