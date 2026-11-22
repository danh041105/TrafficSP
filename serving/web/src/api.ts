import { useQuery } from '@tanstack/react-query'

async function get<T>(path: string): Promise<T> {
  const r = await fetch(`/api${path}`)
  if (!r.ok) throw new Error(`${r.status} ${path}`)
  return r.json()
}

export interface LinkProps {
  link_id: string
  link_name: string
  borough: string
  free_flow_speed_mph: number | null
  cell_id: string | null
  length_km: number | null
  speed_mph: number | null
  travel_time_s: number | null
  speed_ratio: number | null
  traffic_state: string
  event_time: string | null
}
export interface LinkFeatureCollection {
  type: 'FeatureCollection'
  features: { type: 'Feature'; geometry: GeoJSON.LineString | GeoJSON.Point; properties: LinkProps }[]
}
export interface Kpi {
  links_total: number
  links_reporting: number
  city_avg_speed: number | null
  congested_links: number
  slow_links: number
  no_data_links: number
  clock: string | null
  source: string
}
export interface BoroughKpi { borough: string; avg_speed: number | null; congested: number; reporting: number; links: number }
export interface StateCount { traffic_state: string; links: number }
export interface WeatherCell {
  cell_id: string; latitude: number; longitude: number; interval_start: string
  temperature_c: number | null; precipitation_mm: number | null; wind_speed_kmh: number | null
  weather_code: number | null; weather_status: string
}
export interface LinkDetail extends LinkProps { weather: WeatherCell | null }
export interface Point { t: string; speed: number | null }
export interface ForecastPoint { t: string; horizon_min: number; speed: number; lower: number; upper: number; issued_at: string; model_name: string; model_version: string }
export interface HeatCell { dow: number; hour: number; speed: number }
export interface CityPoint { t: string; speed: number | null; hist: number | null }

const qs = (b?: string) => (b ? `?borough=${encodeURIComponent(b)}` : '')

export const useMap = () => useQuery({ queryKey: ['map'], queryFn: () => get<LinkFeatureCollection>('/map/current') })
export const useKpi = (b?: string) => useQuery({ queryKey: ['kpi', b], queryFn: () => get<Kpi>(`/kpi/now${qs(b)}`) })
export const useBoroughs = () => useQuery({ queryKey: ['boroughs'], queryFn: () => get<BoroughKpi[]>('/kpi/boroughs') })
export const useStates = (b?: string) => useQuery({ queryKey: ['states', b], queryFn: () => get<StateCount[]>(`/kpi/states${qs(b)}`) })
export const useTop = (b?: string) => useQuery({ queryKey: ['top', b], queryFn: () => get<LinkProps[]>(`/links/top-congested?limit=8${b ? `&borough=${encodeURIComponent(b)}` : ''}`) })
export const useWeather = () => useQuery({ queryKey: ['weather'], queryFn: () => get<WeatherCell[]>('/weather/current'), refetchInterval: 30_000 })
export const useCityToday = (b?: string) => useQuery({ queryKey: ['cityToday', b], queryFn: () => get<CityPoint[]>(`/city/today${qs(b)}`), refetchInterval: 30_000 })
export const useCityHeatmap = (b?: string) => useQuery({ queryKey: ['cityHeat', b], queryFn: () => get<HeatCell[]>(`/city/heatmap${qs(b)}`), refetchInterval: false, staleTime: Infinity })

export const useLinkDetail = (id?: string) => useQuery({ queryKey: ['link', id], queryFn: () => get<LinkDetail>(`/links/${id}`), enabled: !!id })
export const useLinkHistory = (id?: string) => useQuery({ queryKey: ['hist', id], queryFn: () => get<Point[]>(`/links/${id}/history?hours=24`), enabled: !!id, refetchInterval: 30_000 })
export const useLinkForecast = (id?: string) => useQuery({ queryKey: ['fc', id], queryFn: () => get<ForecastPoint[]>(`/links/${id}/forecast`), enabled: !!id, refetchInterval: 30_000 })
export const useLinkHeatmap = (id?: string) => useQuery({ queryKey: ['lheat', id], queryFn: () => get<HeatCell[]>(`/links/${id}/heatmap`), enabled: !!id, refetchInterval: false, staleTime: Infinity })
