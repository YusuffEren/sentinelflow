// =============================================================================
// SentinelFlow Frontend Configuration
// =============================================================================
// Centralized configuration for API endpoints and environment settings

const getEnvVar = (key: string, defaultValue: string): string => {
  if (typeof window !== "undefined") {
    return (process.env[key] as string) || defaultValue
  }
  return process.env[key] || defaultValue
}

export const config = {
  // API Configuration
  api: {
    baseUrl: getEnvVar("NEXT_PUBLIC_API_URL", "http://127.0.0.1:8000"),
    wsUrl: getEnvVar("NEXT_PUBLIC_WS_URL", "ws://127.0.0.1:8000"),
  },

  // API Endpoints
  endpoints: {
    // System
    health: "/api/v1/system/health",
    stats: "/api/v1/system/stats",

    // Alerts
    alerts: "/api/v1/alerts",
    alertDetail: (id: string) => `/api/v1/alerts/${id}`,
    alertDismiss: (id: string) => `/api/v1/alerts/${id}/dismiss`,

    // Cases
    cases: "/api/v1/cases",
    caseDetail: (id: string) => `/api/v1/cases/${id}`,

    // Transactions
    transactions: "/api/v1/transactions",

    // Auth
    login: "/api/v1/auth/login",
    register: "/api/v1/auth/register",
    me: "/api/v1/auth/me",
    refresh: "/api/v1/auth/refresh",

    // ML
    predict: "/api/v1/ml/predict",
    modelInfo: "/api/v1/ml/info",
    explain: "/api/v1/ml/explain",

    // Chat
    chat: "/api/v1/chat",

    // Graph
    graphNodes: "/api/v1/graph/nodes",
    graphEdges: "/api/v1/graph/edges",

    // WebSocket
    wsAlerts: "/ws/alerts",
  },

  // Feature Flags
  features: {
    enableAiChat: getEnvVar("NEXT_PUBLIC_ENABLE_AI_CHAT", "true") === "true",
    enableNetworkGraph: getEnvVar("NEXT_PUBLIC_ENABLE_NETWORK_GRAPH", "true") === "true",
    enableAuth: getEnvVar("NEXT_PUBLIC_ENABLE_AUTH", "false") === "true",
  },

  // UI Settings
  ui: {
    alertRefreshInterval: 2000, // ms
    statsRefreshInterval: 2000, // ms
    wsReconnectDelay: 3000, // ms
    wsPingInterval: 10000, // ms
    maxAlertsInFeed: 50,
  },
} as const

// Helper functions
export function getApiUrl(endpoint: string): string {
  return `${config.api.baseUrl}${endpoint}`
}

export function getWsUrl(endpoint: string): string {
  return `${config.api.wsUrl}${endpoint}`
}

// Type exports
export type Config = typeof config
