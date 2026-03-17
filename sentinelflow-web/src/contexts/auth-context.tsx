"use client"

import { createContext, useContext, useEffect, useState, ReactNode } from "react"
import { useRouter, usePathname } from "next/navigation"
import { User, getStoredUser, getToken, logout as authLogout, refreshTokens } from "@/lib/auth"

interface AuthContextType {
  user: User | null
  isLoading: boolean
  isAuthenticated: boolean
  logout: () => Promise<void>
  refreshUser: () => void
}

const AuthContext = createContext<AuthContextType | null>(null)

const PUBLIC_PATHS = ["/login", "/register"]

export function AuthProvider({ children }: { children: ReactNode }) {
  const router = useRouter()
  const pathname = usePathname()
  
  const [user, setUser] = useState<User | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  
  const isPublicPath = PUBLIC_PATHS.includes(pathname)
  
  useEffect(() => {
    const checkAuth = async () => {
      const token = getToken()
      const storedUser = getStoredUser()
      
      if (token && storedUser) {
        setUser(storedUser)
        setIsLoading(false)
        
        // Redirect from login page if already authenticated
        if (isPublicPath) {
          router.push("/")
        }
      } else if (token) {
        // Token exists but no user, try to refresh
        const newTokens = await refreshTokens()
        if (!newTokens && !isPublicPath) {
          router.push("/login")
        }
        setIsLoading(false)
      } else {
        // No token
        if (!isPublicPath) {
          router.push("/login")
        }
        setIsLoading(false)
      }
    }
    
    checkAuth()
  }, [pathname, isPublicPath, router])
  
  const logout = async () => {
    await authLogout()
    setUser(null)
    router.push("/login")
  }
  
  const refreshUser = () => {
    setUser(getStoredUser())
  }
  
  // Don't render anything while checking auth on protected pages
  if (isLoading && !isPublicPath) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-[#09090B]">
        <div className="w-8 h-8 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
      </div>
    )
  }
  
  return (
    <AuthContext.Provider
      value={{
        user,
        isLoading,
        isAuthenticated: !!user,
        logout,
        refreshUser,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider")
  }
  return context
}
