// =============================================================================
// SentinelFlow Authentication Library
// =============================================================================

import { config, getApiUrl } from "./config"

export interface User {
  user_id: string
  username: string
  email: string
  full_name: string
  role: "admin" | "analyst" | "viewer"
  is_active: boolean
  created_at: string
}

export interface AuthTokens {
  access_token: string
  refresh_token: string
  token_type: string
  expires_in: number
}

const TOKEN_KEY = "sentinelflow_token"
const REFRESH_TOKEN_KEY = "sentinelflow_refresh_token"
const USER_KEY = "sentinelflow_user"

export function getToken(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem(TOKEN_KEY)
}

export function getRefreshToken(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem(REFRESH_TOKEN_KEY)
}

export function setTokens(tokens: AuthTokens): void {
  localStorage.setItem(TOKEN_KEY, tokens.access_token)
  localStorage.setItem(REFRESH_TOKEN_KEY, tokens.refresh_token)
}

export function clearTokens(): void {
  localStorage.removeItem(TOKEN_KEY)
  localStorage.removeItem(REFRESH_TOKEN_KEY)
  localStorage.removeItem(USER_KEY)
}

export function getStoredUser(): User | null {
  if (typeof window === "undefined") return null
  const userJson = localStorage.getItem(USER_KEY)
  if (!userJson) return null
  try {
    return JSON.parse(userJson)
  } catch {
    return null
  }
}

export function setStoredUser(user: User): void {
  localStorage.setItem(USER_KEY, JSON.stringify(user))
}

export async function login(username: string, password: string): Promise<User> {
  const res = await fetch(getApiUrl(config.endpoints.login), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      username,
      password,
    }),
  })

  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: "Login failed" }))
    throw new Error(error.detail || "Login failed")
  }

  const tokens: AuthTokens = await res.json()
  setTokens(tokens)

  const user = await fetchCurrentUser()
  return user
}

export async function register(
  username: string,
  email: string,
  password: string,
  fullName: string
): Promise<User> {
  const res = await fetch(getApiUrl(config.endpoints.register), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      username,
      email,
      password,
      full_name: fullName,
    }),
  })

  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: "Registration failed" }))
    throw new Error(error.detail || "Registration failed")
  }

  return await login(username, password)
}

export async function fetchCurrentUser(): Promise<User> {
  const token = getToken()
  if (!token) {
    throw new Error("No token available")
  }

  const res = await fetch(getApiUrl(config.endpoints.me), {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  })

  if (!res.ok) {
    throw new Error("Failed to fetch user")
  }

  const user: User = await res.json()
  setStoredUser(user)
  return user
}

export async function refreshTokens(): Promise<AuthTokens | null> {
  const refreshToken = getRefreshToken()
  if (!refreshToken) {
    return null
  }

  try {
    const res = await fetch(getApiUrl(config.endpoints.refresh), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: refreshToken }),
    })

    if (!res.ok) {
      clearTokens()
      return null
    }

    const tokens: AuthTokens = await res.json()
    setTokens(tokens)
    return tokens
  } catch {
    clearTokens()
    return null
  }
}

export async function logout(): Promise<void> {
  clearTokens()
}

export function getAuthHeader(): Record<string, string> {
  const token = getToken()
  if (!token) return {}
  return { Authorization: `Bearer ${token}` }
}

export async function fetchWithAuth(
  endpoint: string,
  options: RequestInit = {}
): Promise<Response> {
  const headers = {
    ...options.headers,
    ...getAuthHeader(),
  }

  let res = await fetch(getApiUrl(endpoint), { ...options, headers })

  if (res.status === 401) {
    const newTokens = await refreshTokens()
    if (newTokens) {
      res = await fetch(getApiUrl(endpoint), {
        ...options,
        headers: {
          ...options.headers,
          Authorization: `Bearer ${newTokens.access_token}`,
        },
      })
    }
  }

  return res
}
