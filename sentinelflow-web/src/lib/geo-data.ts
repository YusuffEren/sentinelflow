// =============================================================================
// SentinelFlow — Coğrafi veri katmanı (demo + fallback)
// =============================================================================
// Gerçek şehir koordinatları, şehirler arası transferler ve imkansız seyahat
// rotaları. Globe sahnesini besler — backend kapalıyken bile anlamlı görünür.
// =============================================================================

export interface City {
  id: string
  name: string
  code: string
  lat: number
  lng: number
  region: "tr" | "eu" | "asia" | "us"
}

// Türkiye + Avrupa + global odaklı şehirler
export const CITIES: City[] = [
  { id: "ist", name: "İstanbul", code: "IST", lat: 41.0082, lng: 28.9784, region: "tr" },
  { id: "ank", name: "Ankara", code: "ANK", lat: 39.9334, lng: 32.8597, region: "tr" },
  { id: "izm", name: "İzmir", code: "IZM", lat: 38.4237, lng: 27.1428, region: "tr" },
  { id: "ant", name: "Antalya", code: "AYT", lat: 36.8969, lng: 30.7133, region: "tr" },
  { id: "ber", name: "Berlin", code: "BER", lat: 52.52, lng: 13.405, region: "eu" },
  { id: "lon", name: "Londra", code: "LON", lat: 51.5074, lng: -0.1278, region: "eu" },
  { id: "fra", name: "Frankfurt", code: "FRA", lat: 50.1109, lng: 8.6821, region: "eu" },
  { id: "ams", name: "Amsterdam", code: "AMS", lat: 52.3676, lng: 4.9041, region: "eu" },
  { id: "dxb", name: "Dubai", code: "DXB", lat: 25.2048, lng: 55.2708, region: "asia" },
  { id: "tyo", name: "Tokyo", code: "TYO", lat: 35.6762, lng: 139.6503, region: "asia" },
  { id: "nyc", name: "New York", code: "NYC", lat: 40.7128, lng: -74.006, region: "us" },
  { id: "sin", name: "Singapore", code: "SIN", lat: 1.3521, lng: 103.8198, region: "asia" },
]

export interface TransferArc {
  from: string // city id
  to: string // city id
  amount: number
  kind: "normal" | "impossible" | "suspicious"
  label?: string
}

// Normal transferler (şüphesiz)
export const NORMAL_TRANSFERS: TransferArc[] = [
  { from: "ist", to: "ber", amount: 42000, kind: "normal" },
  { from: "ank", to: "fra", amount: 18500, kind: "normal" },
  { from: "izm", to: "ams", amount: 8900, kind: "normal" },
  { from: "ist", to: "dxb", amount: 67000, kind: "normal" },
  { from: "ant", to: "lon", amount: 12300, kind: "normal" },
  { from: "ist", to: "ank", amount: 95000, kind: "normal" },
  { from: "ber", to: "ams", amount: 31000, kind: "normal" },
  { from: "fra", to: "nyc", amount: 78000, kind: "normal" },
  { from: "dxb", to: "sin", amount: 54000, kind: "normal" },
  { from: "lon", to: "nyc", amount: 102000, kind: "normal" },
]

// İmkansız seyahat rotaları (kırmızı)
export const IMPOSSIBLE_TRAVELS: TransferArc[] = [
  {
    from: "ist",
    to: "ber",
    amount: 12400,
    kind: "impossible",
    label: "12dk · 9.000 km/h",
  },
  {
    from: "ber",
    to: "nyc",
    amount: 33100,
    kind: "impossible",
    label: "6dk · 12.500 km/h",
  },
  {
    from: "ank",
    to: "tyo",
    amount: 21800,
    kind: "impossible",
    label: "8dk · 14.000 km/h",
  },
  {
    from: "dxb",
    to: "lon",
    amount: 47200,
    kind: "impossible",
    label: "4dk · 13.800 km/h",
  },
]

// Şüpheli ama imkansız değil (amber)
export const SUSPICIOUS_TRANSFERS: TransferArc[] = [
  { from: "ist", to: "sin", amount: 184500, kind: "suspicious", label: "tutar anomalisi" },
  { from: "fra", to: "dxb", amount: 142000, kind: "suspicious", label: "high-value" },
]

export const ALL_TRANSFERS: TransferArc[] = [
  ...NORMAL_TRANSFERS,
  ...IMPOSSIBLE_TRAVELS,
  ...SUSPICIOUS_TRANSFERS,
]

// Şehir ID -> City haritası
export const CITY_MAP: Record<string, City> = Object.fromEntries(
  CITIES.map((c) => [c.id, c]),
)
