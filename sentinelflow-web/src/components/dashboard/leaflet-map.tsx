"use client"

import { useEffect } from "react"
import { MapContainer, TileLayer, CircleMarker, Polyline, Popup } from "react-leaflet"
import L from "leaflet"
import "leaflet/dist/leaflet.css"

// City Coordinates
const CITY_COORDS: Record<string, [number, number]> = {
  "İstanbul": [41.0082, 28.9784], "Istanbul": [41.0082, 28.9784],
  "Ankara": [39.9334, 32.8597],
  "İzmir": [38.4192, 27.1287], "Izmir": [38.4192, 27.1287],
  "Bursa": [40.1885, 29.0610],
  "Antalya": [36.8969, 30.7133],
  "Adana": [37.0000, 35.3213],
  "Konya": [37.8746, 32.4932],
  "Gaziantep": [37.0662, 37.3833],
  "Mersin": [36.8121, 34.6415],
  "Diyarbakır": [37.9144, 40.2306],
  "Kayseri": [38.7312, 35.4787],
  "Eskişehir": [39.7767, 30.5206],
  "Trabzon": [41.0027, 39.7168],
  "Samsun": [41.2867, 36.3300],
  "Denizli": [37.7765, 29.0864],
  "Berlin": [52.5200, 13.4050],
  "London": [51.5074, -0.1278],
  "Paris": [48.8566, 2.3522],
  "Dubai": [25.2048, 55.2708],
  "Moscow": [55.7558, 37.6173],
  "New York": [40.7128, -74.0060],
  "Tokyo": [35.6762, 139.6503],
}

interface LeafletMapProps {
  alerts: any[]
}

export default function LeafletMap({ alerts }: LeafletMapProps) {
  useEffect(() => {
    // @ts-ignore
    delete L.Icon.Default.prototype._getIconUrl
    L.Icon.Default.mergeOptions({
      iconRetinaUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png",
      iconUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png",
      shadowUrl: "https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png",
    })
  }, [])

  const mapObjects = alerts.flatMap((alert) => {
    const senderCity = alert.sender_city || "Istanbul"
    const receiverCity = alert.receiver_city || "Ankara"
    const senderCoords = CITY_COORDS[senderCity]
    const receiverCoords = CITY_COORDS[receiverCity]

    if (!senderCoords || !receiverCoords) return []

    return [{
      id: alert.alert_id,
      sender: { city: senderCity, coords: senderCoords },
      receiver: { city: receiverCity, coords: receiverCoords },
      fraudType: alert.fraud_type,
      severity: alert.severity,
    }]
  })

  return (
    <MapContainer
      center={[39.0, 35.0]}
      zoom={5}
      scrollWheelZoom={false}
      className="w-full h-full"
      zoomControl={false}
      attributionControl={false}
    >
      {/* Dark minimal map tiles */}
      <TileLayer
        url="https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png"
      />

      {mapObjects.map((obj) => (
        <div key={obj.id}>
          {/* Sender */}
          <CircleMarker
            center={obj.sender.coords}
            radius={4}
            pathOptions={{ 
              color: "#3B82F6", 
              fillColor: "#3B82F6", 
              fillOpacity: 0.8,
              weight: 1,
            }}
          >
            <Popup>
              <div className="text-sm">
                <p className="font-medium text-zinc-100">{obj.sender.city}</p>
                <p className="text-zinc-400 text-xs">Source</p>
              </div>
            </Popup>
          </CircleMarker>

          {/* Receiver */}
          <CircleMarker
            center={obj.receiver.coords}
            radius={4}
            pathOptions={{ 
              color: "#EF4444", 
              fillColor: "#EF4444", 
              fillOpacity: 0.8,
              weight: 1,
            }}
          >
            <Popup>
              <div className="text-sm">
                <p className="font-medium text-zinc-100">{obj.receiver.city}</p>
                <p className="text-zinc-400 text-xs">Target</p>
              </div>
            </Popup>
          </CircleMarker>

          {/* Connection */}
          <Polyline
            positions={[obj.sender.coords, obj.receiver.coords]}
            pathOptions={{ 
              color: "#EF4444", 
              weight: 1, 
              opacity: 0.4, 
              dashArray: "4, 8",
            }}
          />
        </div>
      ))}
    </MapContainer>
  )
}
