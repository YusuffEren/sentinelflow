"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { Shield, LogOut, User, ShieldAlert, FolderKanban, LayoutDashboard } from "lucide-react"
import { useAuth } from "@/contexts/auth-context"

export function Header({ isConnected = false }: { isConnected?: boolean }) {
  const { user, logout, isAuthenticated } = useAuth()
  const [time, setTime] = useState("")

  useEffect(() => {
    const updateTime = () => {
      const now = new Date()
      setTime(now.toLocaleTimeString("en-GB", { 
        hour: "2-digit", 
        minute: "2-digit",
        timeZoneName: "short"
      }))
    }
    
    updateTime()
    const interval = setInterval(updateTime, 1000)
    return () => clearInterval(interval)
  }, [])

  return (
    <header className="h-14 border-b border-zinc-800 bg-zinc-900/50 backdrop-blur-sm flex items-center justify-between px-6">
      {/* Logo & Nav */}
      <div className="flex items-center gap-6">
        <Link href="/" className="flex items-center gap-3">
          <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-indigo-500/10 border border-indigo-500/20">
            <Shield className="w-4 h-4 text-indigo-400" />
          </div>
          <span className="text-[15px] font-semibold tracking-tight text-zinc-100">
            SentinelFlow
          </span>
        </Link>
        
        {/* Navigation */}
        {isAuthenticated && (
          <nav className="flex items-center gap-1">
            <Link
              href="/"
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-zinc-400 hover:text-white hover:bg-zinc-800 rounded-lg transition-colors"
            >
              <LayoutDashboard className="w-4 h-4" />
              Dashboard
            </Link>
            <Link
              href="/alerts"
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-zinc-400 hover:text-white hover:bg-zinc-800 rounded-lg transition-colors"
            >
              <ShieldAlert className="w-4 h-4" />
              Alarmlar
            </Link>
            <Link
              href="/cases"
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm text-zinc-400 hover:text-white hover:bg-zinc-800 rounded-lg transition-colors"
            >
              <FolderKanban className="w-4 h-4" />
              Vakalar
            </Link>
          </nav>
        )}
      </div>

      {/* Right side */}
      <div className="flex items-center gap-4">
        {/* Status */}
        <div className="flex items-center gap-2">
          <div className={`w-1.5 h-1.5 rounded-full ${isConnected ? "bg-emerald-400 animate-subtle-pulse" : "bg-zinc-500"}`} />
          <span className="text-xs font-medium text-zinc-400">
            {isConnected ? "Online" : "Offline"}
          </span>
        </div>

        {/* Time */}
        <span className="text-xs font-mono text-zinc-500 tabular-nums">
          {time}
        </span>
        
        {/* User menu */}
        {isAuthenticated && user && (
          <div className="flex items-center gap-3 pl-3 border-l border-zinc-800">
            <div className="flex items-center gap-2">
              <div className="w-7 h-7 rounded-full bg-blue-500/20 flex items-center justify-center">
                <User className="w-3.5 h-3.5 text-blue-400" />
              </div>
              <div className="text-xs">
                <p className="text-zinc-300 font-medium">{user.full_name}</p>
                <p className="text-zinc-600">{user.role}</p>
              </div>
            </div>
            <button
              onClick={logout}
              className="p-1.5 text-zinc-500 hover:text-red-400 hover:bg-zinc-800 rounded-lg transition-colors"
              title="Çıkış"
            >
              <LogOut className="w-4 h-4" />
            </button>
          </div>
        )}
      </div>
    </header>
  )
}
