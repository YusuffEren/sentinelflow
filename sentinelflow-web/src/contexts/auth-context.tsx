"use client"

import { createContext, useContext, useEffect, useState, ReactNode, useCallback } from "react"
import { useRouter, usePathname } from "next/navigation"
import { 
  User, 
  getStoredUser, 
  getToken, 
  logout as authLogout, 
  refreshTokens,
  login as authLogin,
  fetchCurrentUser
} from "@/lib/auth"
import { config } from "@/lib/config"

interface AuthContextType {
  user: User | null
  isLoading: boolean
  isAuthenticated: boolean
  login: (username: string, password: string) => Promise<void>
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
  const authEnabled = config.features.enableAuth
  
  useEffect(() => {
    const checkAuth = async () => {
      if (!authEnabled) {
        setIsLoading(false)
        return
      }
      
      const token = getToken()
      const storedUser = getStoredUser()
      
      if (token && storedUser) {
        setUser(storedUser)
        setIsLoading(false)
        
        if (isPublicPath) {
          router.push("/")
        }
      } else if (token) {
        try {
          const newTokens = await refreshTokens()
          if (newTokens) {
            const currentUser = await fetchCurrentUser()
            setUser(currentUser)
          } else if (!isPublicPath) {
            router.push("/login")
          }
        } catch {
          if (!isPublicPath) {
            router.push("/login")
          }
        }
        setIsLoading(false)
      } else {
        if (!isPublicPath && authEnabled) {
          router.push("/login")
        }
        setIsLoading(false)
      }
    }
    
    checkAuth()
  }, [pathname, isPublicPath, router, authEnabled])
  
  const login = useCallback(async (username: string, password: string) => {
    const loggedInUser = await authLogin(username, password)
    setUser(loggedInUser)
    router.push("/")
  }, [router])
  
  const logout = useCallback(async () => {
    await authLogout()
    setUser(null)
    router.push("/login")
  }, [router])
  
  const refreshUser = useCallback(() => {
    setUser(getStoredUser())
  }, [])
  
  if (isLoading && !isPublicPath && authEnabled) {
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
        login,
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
