// =============================================================================
// SentinelFlow 3D — Coğrafi yardımcı fonksiyonlar
// =============================================================================
// Lat/lng → 3D vektör dönüşümü, şehir koordinatları ve yay (arc) üretimi.
// leaflet-map.tsx ile aynı koordinat kaynağını kullanır, böylece 2D ve 3D
// haritalar tutarlı konumlar gösterir.

import * as THREE from "three"

// Şehir koordinatları (leaflet-map.tsx ile birebir uyumlu)
export const CITY_COORDS: Record<string, [number, number]> = {
  İstanbul: [41.0082, 28.9784],
  Istanbul: [41.0082, 28.9784],
  Ankara: [39.9334, 32.8597],
  İzmir: [38.4192, 27.1287],
  Izmir: [38.4192, 27.1287],
  Bursa: [40.1885, 29.061],
  Antalya: [36.8969, 30.7133],
  Adana: [37.0, 35.3213],
  Konya: [37.8746, 32.4932],
  Gaziantep: [37.0662, 37.3833],
  Mersin: [36.8121, 34.6415],
  Diyarbakır: [37.9144, 40.2306],
  Kayseri: [38.7312, 35.4787],
  Eskişehir: [39.7767, 30.5206],
  Trabzon: [41.0027, 39.7168],
  Samsun: [41.2867, 36.33],
  Denizli: [37.7765, 29.0864],
  Berlin: [52.52, 13.405],
  London: [51.5074, -0.1278],
  Paris: [48.8566, 2.3522],
  Dubai: [25.2048, 55.2708],
  Moscow: [55.7558, 37.6173],
  "New York": [40.7128, -74.006],
  Tokyo: [35.6762, 139.6503],
}

// Bilinmeyen şehir adlarını güvenli biçimde çözümler.
// Türkçe/İngilizce varyantlar ve case farklarını tolere eder;
// eşleşme yoksa İstanbul'a düşer (TR odaklı dolandırıcılık sistemi için makul varsayılan).
export function resolveCity(city?: string): string {
  if (!city) return "Istanbul"
  const key = city.trim()
  if (CITY_COORDS[key]) return key
  const found = Object.keys(CITY_COORDS).find(
    (k) => k.toLowerCase() === key.toLowerCase(),
  )
  return found ?? "Istanbul"
}

// Enlem/boylam → küre yüzeyinde 3D nokta.
// radius, küre yarıçapıdır; noktayı yüzeyin biraz üstüne koymak için
// çağıran tarafın radius'u hafifçe büyütmesi yeterli.
export function latLngToVector3(
  lat: number,
  lng: number,
  radius: number,
): THREE.Vector3 {
  const phi = ((90 - lat) * Math.PI) / 180
  const theta = ((lng + 180) * Math.PI) / 180
  const x = -radius * Math.sin(phi) * Math.cos(theta)
  const y = radius * Math.cos(phi)
  const z = radius * Math.sin(phi) * Math.sin(theta)
  return new THREE.Vector3(x, y, z)
}

// İki yüzey noktası arası, yüzeyden yükselen kavisli yay noktaları üretir.
// QuadraticBezier kullanır; kontrol noktası orta noktayı küre merkezinden
// uzaklaştırır, böylece yay küre yüzeyinin üstünde kavisli görünür.
export function createArcPoints(
  start: THREE.Vector3,
  end: THREE.Vector3,
  segments: number,
  surfaceRadius: number,
): THREE.Vector3[] {
  const mid = start.clone().add(end).multiplyScalar(0.5)
  const distance = start.distanceTo(end)
  // Yay yüksekliği mesafeyle orantılı; minimum bir yüzey üstü payı var.
  const arcHeight = surfaceRadius + distance * 0.45
  mid.normalize().multiplyScalar(arcHeight)
  const curve = new THREE.QuadraticBezierCurve3(start, mid, end)
  return curve.getPoints(segments)
}

// Deterministik seeded PRNG (mulberry32). Math.random yerine render içinde
// kullanılır — React'in "component must be pure" kuralına uyum için. Aynı seed
// her zaman aynı diziyi üretir, böylece 3D sahne re-render'da kararsız olmaz.
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0
  return function () {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
