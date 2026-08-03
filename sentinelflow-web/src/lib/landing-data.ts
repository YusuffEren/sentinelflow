// =============================================================================
// SentinelFlow — Landing sayfası statik veri katmanı
// =============================================================================
// Tüm metinler projenin gerçek işleviyle (circular ring, impossible travel,
// NLP blacklist, Isolation Forest) uyumlu, sahte ama inandırıcı uyarı/örneklerle.
// =============================================================================

export const REPO_URL = "https://github.com/YusuffEren/sentinelflow"

// --- Hızlı erişim rengi token'ları (JS tarafında kullanım için) ---------------
export const COLORS = {
  signal: "#00e5c7",
  signalSoft: "#2de1c2",
  alarm: "#ff4d5e",
  amber: "#ffb020",
  muted: "#7d8aa0",
  line: "#1b2330",
} as const

export type Severity = "critical" | "high" | "medium"

export const SEVERITY_META: Record<
  Severity,
  { label: string; color: string; ring: string }
> = {
  critical: { label: "Critical", color: "#ff4d5e", ring: "rgba(255,77,94,0.4)" },
  high: { label: "High", color: "#ffb020", ring: "rgba(255,176,32,0.4)" },
  medium: { label: "Medium", color: "#2de1c2", ring: "rgba(45,225,194,0.4)" },
}

// =============================================================================
// 4 Tespit Motoru
// =============================================================================
export interface DetectionEngine {
  id: string
  code: string
  name: string
  tech: string
  techNote: string
  severity: Severity
  description: string
  example: string
  exampleDetail: string
}

export const ENGINES: DetectionEngine[] = [
  {
    id: "circular-ring",
    code: "RING-01",
    name: "Circular Ring",
    tech: "Neo4j",
    techNote: "Cypher graph traversal",
    severity: "critical",
    description:
      "Para akışının başladığı hesaba geri döndüğü dairesel zincirleri (A → B → C → A) graf sorgularıyla milisaniyeler içinde yakalar.",
    example: "TR12 → TR47 → TR89 → TR12",
    exampleDetail:
      "₺480.000, 3 düğüm, 11 dakika — para aynı halkada 4 tur attı",
  },
  {
    id: "impossible-travel",
    code: "GEO-02",
    name: "Impossible Travel",
    tech: "Redis Geo",
    techNote: "Geospatial distance",
    severity: "high",
    description:
      "Aynı kullanıcıdan peş peşe gelen iki işlem arasındaki fiziksel mesafeyi ve zamanı ölçer; insanca aşılması mümkün olmayan hızları işaretler.",
    example: "İstanbul 12:00 → Berlin 12:10",
    exampleDetail: "Gerekli hız 9.000 km/h — İMKANSIZ",
  },
  {
    id: "nlp-blacklist",
    code: "NLP-03",
    name: "NLP Blacklist",
    tech: "scikit-learn",
    techNote: "Keyword & pattern NLP",
    severity: "medium",
    description:
      "İşlem açıklamalarındaki kuşkulu terimleri, kodlanmış kelimeleri ve maskeleme girişimlerini (farklı yazım, unicode) eşleştirir.",
    example: "\"kredı geri odeme\" → match: loan_laundering",
    exampleDetail: "3 anahtar kelime, güven 0.92",
  },
  {
    id: "ai-anomaly",
    code: "ML-04",
    name: "AI Anomaly",
    tech: "Isolation Forest",
    techNote: "Unsupervised outlier",
    severity: "high",
    description:
      "Etiketli veriye ihtiyaç duymadan, işlem tutarlarının istatistiksel dağılımındaki aykırı noktaları (uyumsuz davranış) izole eder.",
    example: "Tutar ₺184.500 — hesap ortalaması ₺2.300",
    exampleDetail: "z-score 8.4, anomali skoru 0.96",
  },
]

// =============================================================================
// Mimari akış adımları (scroll-triggered diyagram)
// =============================================================================
export interface FlowStep {
  id: string
  label: string
  sub: string
  kind: "source" | "stream" | "detector" | "store" | "ui"
}

export const FLOW_STEPS: FlowStep[][] = [
  [{ id: "gen", label: "Generator", sub: "Synthetic tx", kind: "source" }],
  [{ id: "kafka", label: "Kafka", sub: "Stream broker", kind: "stream" }],
  [
    { id: "ring", label: "Ring", sub: "Neo4j", kind: "detector" },
    { id: "geo", label: "Travel", sub: "Redis", kind: "detector" },
    { id: "nlp", label: "NLP", sub: "Blacklist", kind: "detector" },
    { id: "ml", label: "AI", sub: "IsolationForest", kind: "detector" },
  ],
  [
    { id: "neo", label: "Neo4j", sub: "Graph DB", kind: "store" },
    { id: "redis", label: "Redis", sub: "Cache / Geo", kind: "store" },
  ],
  [{ id: "ui", label: "Dashboard", sub: "SOC console", kind: "ui" }],
]

// =============================================================================
// Performans metrikleri (count-up)
// =============================================================================
export interface Metric {
  value: number
  suffix: string
  prefix?: string
  label: string
  decimals?: number
}

export const METRICS: Metric[] = [
  { value: 10000, suffix: "", label: "İşlem / sn hedefi" },
  { value: 100, suffix: "ms", prefix: "<", label: "Tespit gecikmesi" },
  { value: 50, suffix: "ms", prefix: "<", label: "Graf sorgu süresi" },
  { value: 4, suffix: "", label: "Aktif tespit motoru" },
]

// =============================================================================
// Teknoloji yığını
// =============================================================================
export interface TechItem {
  name: string
  role: string
  color: string
}

export const TECH_STACK: TechItem[] = [
  { name: "Apache Kafka", role: "Event streaming", color: "#e74c3c" },
  { name: "Neo4j", role: "Graph database", color: "#018bff" },
  { name: "Redis", role: "Cache / Geo-spatial", color: "#ff4438" },
  { name: "scikit-learn", role: "Isolation Forest", color: "#f89939" },
  { name: "Streamlit", role: "SOC dashboard", color: "#ff4b4b" },
  { name: "Docker", role: "Container runtime", color: "#2496ed" },
  { name: "Python", role: "Servis dili", color: "#ffd43b" },
  { name: "React / Next.js", role: "Bu site", color: "#00e5c7" },
]

// =============================================================================
// Kurulum komutları (terminal + kopyala)
// =============================================================================
export interface SetupStep {
  prompt: string
  command: string
  comment?: string
}

export const SETUP_STEPS: SetupStep[] = [
  {
    prompt: "~",
    command: "git clone https://github.com/YusuffEren/sentinelflow.git",
    comment: "Repoyu klonla",
  },
  {
    prompt: "~/sentinelflow",
    command: "cd sentinelflow && docker-compose up -d",
    comment: "Kafka, Neo4j, Redis — altyapı ayağa kalkar",
  },
  {
    prompt: "~/sentinelflow",
    command: "pip install -e \".[dev]\"",
    comment: "Python bağımlılıkları",
  },
  {
    prompt: "~/sentinelflow",
    command: "sentinelflow-generate --fraud-ratio 0.05",
    comment: "Sentetik işlem akışı başlar → alarmlar düşer",
  },
]

// =============================================================================
// Sahte alert feed (canlı gösterim hissi)
// =============================================================================
export interface FauxAlert {
  id: string
  ts: string
  engine: string
  severity: Severity
  detail: string
  amount: string
}

export const FAUX_ALERTS: FauxAlert[] = [
  { id: "ALERT-7f3a2c", ts: "14:02:31.482", engine: "circular_ring", severity: "critical", detail: "TR12→TR47→TR89→TR12", amount: "₺480.000" },
  { id: "ALERT-9b1e0d", ts: "14:02:29.118", engine: "impossible_travel", severity: "high", detail: "IST→BER 9.000 km/h", amount: "₺12.400" },
  { id: "ALERT-2c8a4f", ts: "14:02:27.901", engine: "ml_ensemble", severity: "high", detail: "z=8.4 score 0.96", amount: "₺184.500" },
  { id: "ALERT-4d6b11", ts: "14:02:25.337", engine: "blacklist_keyword", severity: "medium", detail: "match: loan_laundering", amount: "₺7.900" },
  { id: "ALERT-1a9f3e", ts: "14:02:23.005", engine: "circular_ring", severity: "critical", detail: "4-node ring detected", amount: "₺1.2M" },
  { id: "ALERT-8e2c77", ts: "14:02:20.664", engine: "impossible_travel", severity: "high", detail: "ANK→TYO 2dk", amount: "₺33.100" },
  { id: "ALERT-5b0a19", ts: "14:02:18.221", engine: "ml_ensemble", severity: "medium", detail: "amount outlier", amount: "₺96.250" },
  { id: "ALERT-3f7d44", ts: "14:02:15.880", engine: "blacklist_keyword", severity: "medium", detail: "unicode mask match", amount: "₺2.100" },
  { id: "ALERT-6c1b8a", ts: "14:02:13.447", engine: "circular_ring", severity: "high", detail: "3-node ring forming", amount: "₺58.000" },
  { id: "ALERT-0d4e2c", ts: "14:02:10.992", engine: "impossible_travel", severity: "medium", detail: "IZR→IST 4dk", amount: "₺4.700" },
]

// Ek uyarılar (feed'i canlı tutmak için döngüsel ek)
export const FAUX_ALERTS_EXTRA: FauxAlert[] = [
  { id: "ALERT-a1b2c3", ts: "14:02:08.531", engine: "ml_ensemble", severity: "high", detail: "z=6.1 score 0.91", amount: "₺142.000" },
  { id: "ALERT-d4e5f6", ts: "14:02:06.114", engine: "circular_ring", severity: "critical", detail: "5-node ring", amount: "₺870.000" },
  { id: "ALERT-g7h8i9", ts: "14:02:03.702", engine: "blacklist_keyword", severity: "medium", detail: "match: cash_out", amount: "₺1.450" },
  { id: "ALERT-j1k2l3", ts: "14:02:01.288", engine: "impossible_travel", severity: "high", detail: "BER→IST 6dk", amount: "₺21.800" },
]
