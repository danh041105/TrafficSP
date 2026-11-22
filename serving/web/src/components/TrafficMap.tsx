import { useEffect, useMemo } from 'react'
import { CircleMarker, MapContainer, Polyline, TileLayer, Tooltip, useMap as useLeaflet } from 'react-leaflet'
import type { LatLngBoundsExpression, LatLngExpression } from 'leaflet'
import type { LinkFeatureCollection, LinkProps, WeatherCell } from '../api'
import { mph, nyTime, stateOf, wmo } from '../format'

interface Props {
  data?: LinkFeatureCollection
  weather?: WeatherCell[]
  showWeather: boolean
  selected?: string
  onSelect: (id: string) => void
}

type Shape = { p: LinkProps; line?: LatLngExpression[]; point?: [number, number] }

function toShapes(data?: LinkFeatureCollection): Shape[] {
  return (data?.features ?? []).map(f => {
    if (f.geometry.type === 'LineString' && f.geometry.coordinates.length > 1)
      return { p: f.properties, line: f.geometry.coordinates.map(([lon, lat]) => [lat, lon]) as LatLngExpression[] }
    if (f.geometry.type === 'Point') {
      const [lon, lat] = f.geometry.coordinates
      return { p: f.properties, point: [lat, lon] as [number, number] }
    }
    return { p: f.properties }
  }).filter(s => s.line || s.point)
}

const latlngs = (s: Shape) => (s.line ?? (s.point ? [s.point] : [])) as [number, number][]

function FitBounds({ shapes }: { shapes: Shape[] }) {
  const map = useLeaflet()
  const key = shapes.map(s => s.p.link_id).join(',')
  useEffect(() => {
    const pts = shapes.flatMap(latlngs)
    if (pts.length) map.fitBounds(pts as LatLngBoundsExpression, { padding: [20, 20] })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key])
  return null
}

function FlyToSelected({ shapes, selected }: { shapes: Shape[]; selected?: string }) {
  const map = useLeaflet()
  useEffect(() => {
    const s = shapes.find(x => x.p.link_id === selected)
    if (!s) return
    const pts = latlngs(s)
    if (pts.length === 1) map.flyTo(pts[0], 14, { duration: 0.8 })
    else map.flyToBounds(pts as LatLngBoundsExpression, { padding: [80, 80], maxZoom: 14, duration: 0.8 })
    // chỉ bay khi đổi đoạn được chọn, không bay lại mỗi lần refetch
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected])
  return null
}

function LinkTip({ p }: { p: LinkProps }) {
  const st = stateOf(p.traffic_state)
  return (
    <Tooltip sticky className="tip">
      <b>{p.link_name}</b><br />
      {p.borough} · {st.mark} {st.label}<br />
      <span className="mono">{mph(p.speed_mph)} mph</span> / thông thoáng {mph(p.free_flow_speed_mph)} mph<br />
      dữ liệu lúc {nyTime(p.event_time)}
    </Tooltip>
  )
}

export function TrafficMap({ data, weather, showWeather, selected, onSelect }: Props) {
  // vẽ đường trạng thái nặng hơn lên trên; đoạn được chọn lên trên cùng
  const shapes = useMemo(() => toShapes(data).sort((a, b) =>
    (Number(a.p.link_id === selected) - Number(b.p.link_id === selected)) ||
    (stateOf(a.p.traffic_state).rank - stateOf(b.p.traffic_state).rank)), [data, selected])

  return (
    <MapContainer center={[40.72, -73.93]} zoom={11} className="map" zoomControl={false} preferCanvas>
      <TileLayer
        url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; OpenStreetMap contributors'
        className="dark-tiles"
      />
      <FitBounds shapes={shapes} />
      <FlyToSelected shapes={shapes} selected={selected} />
      {shapes.filter(s => s.p.link_id === selected && s.line).map(s => (
        <Polyline key={`glow-${s.p.link_id}`} positions={s.line!} interactive={false}
          pathOptions={{ color: '#ffffff', weight: 18, opacity: 0.35, lineCap: 'round' }} />
      ))}
      {shapes.map(s => {
        const st = stateOf(s.p.traffic_state)
        const isSel = s.p.link_id === selected
        const measured = st.rank > 0
        const opacity = selected && !isSel ? 0.18 : measured ? 0.95 : 0.5
        const handlers = { click: () => onSelect(s.p.link_id) }
        return s.line ? (
          <Polyline key={`${s.p.link_id}-${isSel}`} positions={s.line} eventHandlers={handlers}
            pathOptions={{ color: st.color, weight: isSel ? 10 : 5, opacity, lineCap: 'round', dashArray: measured ? undefined : '2 8' }}>
            <LinkTip p={s.p} />
          </Polyline>
        ) : (
          <CircleMarker key={`${s.p.link_id}-${isSel}`} center={s.point!} radius={isSel ? 9 : 6} eventHandlers={handlers}
            pathOptions={{ color: '#0e1525', weight: 2, fillColor: st.color, fillOpacity: opacity }}>
            <LinkTip p={s.p} />
          </CircleMarker>
        )
      })}
      {showWeather && weather?.map(w => (
        <CircleMarker key={w.cell_id} center={[w.latitude, w.longitude]} radius={15} interactive={false}
          pathOptions={{ color: '#94a3b8', weight: 1, dashArray: '3 3', fillOpacity: 0.06 }}>
          <Tooltip permanent direction="center" className="wx-label">
            {wmo(w.weather_code).icon} {w.temperature_c == null ? '–' : Math.round(w.temperature_c)}°
          </Tooltip>
        </CircleMarker>
      ))}
    </MapContainer>
  )
}
