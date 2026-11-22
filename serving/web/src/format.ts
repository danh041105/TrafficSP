// Trạng thái giao thông theo serving.v_traffic_latest (CONGESTED < 0,5 <= SLOW < 0,8 <= FREE_FLOW).
// Màu = status palette của skill dataviz (good / warning / critical), luôn đi kèm nhãn + ký hiệu, không dùng màu đơn độc.
export type TrafficState = 'FREE_FLOW' | 'SLOW' | 'CONGESTED' | 'STALE' | 'NO_VALID_DATA' | 'NO_BASELINE' | 'NO_DATA'

export const STATE: Record<TrafficState, { color: string; label: string; mark: string; rank: number }> = {
  FREE_FLOW: { color: '#0ca30c', label: 'Thông thoáng', mark: '●', rank: 1 },
  SLOW: { color: '#fab219', label: 'Chậm', mark: '▲', rank: 2 },
  CONGESTED: { color: '#d03b3b', label: 'Tắc nghẽn', mark: '■', rank: 3 },
  STALE: { color: '#64748b', label: 'Dữ liệu cũ', mark: '○', rank: 0 },
  NO_VALID_DATA: { color: '#64748b', label: 'Không hợp lệ', mark: '○', rank: 0 },
  NO_BASELINE: { color: '#64748b', label: 'Chưa có baseline', mark: '○', rank: 0 },
  NO_DATA: { color: '#475569', label: 'Chưa có dữ liệu', mark: '○', rank: 0 },
}
export const MEASURED: TrafficState[] = ['FREE_FLOW', 'SLOW', 'CONGESTED']
export const stateOf = (s?: string) => STATE[(s as TrafficState)] ?? STATE.NO_DATA

// Thang tuần tự một màu (xanh dương) cho heatmap: đậm hơn = chậm hơn.
export const SEQ_BLUE = ['#0d366b', '#1c5cab', '#2a78d6', '#5598e7', '#86b6ef', '#b7d3f6']

// Mã thời tiết WMO
export function wmo(code: number | null | undefined): { icon: string; label: string } {
  if (code == null) return { icon: '–', label: 'Không rõ' }
  if (code === 0) return { icon: '☀️', label: 'Trời quang' }
  if (code <= 2) return { icon: '🌤️', label: 'Ít mây' }
  if (code === 3) return { icon: '☁️', label: 'Nhiều mây' }
  if (code <= 48) return { icon: '🌫️', label: 'Sương mù' }
  if (code <= 57) return { icon: '🌦️', label: 'Mưa phùn' }
  if (code <= 67) return { icon: '🌧️', label: 'Mưa' }
  if (code <= 77) return { icon: '🌨️', label: 'Tuyết' }
  if (code <= 82) return { icon: '🌧️', label: 'Mưa rào' }
  if (code <= 86) return { icon: '🌨️', label: 'Mưa tuyết' }
  return { icon: '⛈️', label: 'Dông' }
}

const nyTimeFmt = new Intl.DateTimeFormat('en-GB', { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit' })
const nyDateFmt = new Intl.DateTimeFormat('vi-VN', { timeZone: 'America/New_York', weekday: 'short', day: '2-digit', month: '2-digit', year: 'numeric' })
export const nyTime = (iso?: string | Date | null) => (iso ? nyTimeFmt.format(new Date(iso)) : '--:--')
export const nyDate = (iso?: string | Date | null) => (iso ? nyDateFmt.format(new Date(iso)) : '')

export const mph = (v?: number | null) => (v == null ? '–' : v.toFixed(1))
export const DOW = ['T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'CN']
