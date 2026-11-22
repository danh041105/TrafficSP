import ReactECharts from 'echarts-for-react'
import type { BoroughKpi, CityPoint, ForecastPoint, HeatCell, Point } from '../api'
import { DOW, SEQ_BLUE, nyTime } from '../format'

// Màu series theo categorical palette (dark) của skill dataviz: slot 1 xanh = thực tế, slot 2 cam = dự báo.
const ACTUAL = '#3987e5'
const FORECAST = '#d95926'
const MUTED = '#94a3b8'

const AXIS = { axisLine: { lineStyle: { color: '#334155' } }, axisLabel: { color: '#94a3b8', fontSize: 10 }, splitLine: { lineStyle: { color: '#1e293b' } } }
const TOOLTIP = { backgroundColor: '#0f172a', borderColor: '#334155', textStyle: { color: '#e2e8f0', fontSize: 12 } }

export function LinkSpeedChart({ history, forecast, freeFlow }: { history: Point[]; forecast: ForecastPoint[]; freeFlow?: number | null }) {
  const last = history[history.length - 1]
  const fc = last ? [{ t: last.t, speed: last.speed, lower: last.speed, upper: last.speed }, ...forecast] : forecast
  const option = {
    grid: { left: 36, right: 12, top: 26, bottom: 24 },
    tooltip: { trigger: 'axis', ...TOOLTIP, valueFormatter: (v: number) => (v == null ? '–' : `${v.toFixed(1)} mph`) },
    legend: { top: 0, right: 0, textStyle: { color: '#94a3b8', fontSize: 11 }, itemHeight: 8, data: ['Thực tế', 'Dự báo'] },
    xAxis: { type: 'time', ...AXIS, axisLabel: { ...AXIS.axisLabel, formatter: (v: number) => nyTime(new Date(v)) } },
    yAxis: { type: 'value', min: 0, ...AXIS, name: 'mph', nameTextStyle: { color: '#64748b', fontSize: 10 } },
    series: [
      {
        name: 'Thực tế', type: 'line', showSymbol: false, connectNulls: false,
        data: history.map(p => [p.t, p.speed]),
        lineStyle: { color: ACTUAL, width: 2 }, itemStyle: { color: ACTUAL },
        markLine: freeFlow ? { silent: true, symbol: 'none', label: { color: '#64748b', fontSize: 10, formatter: 'thông thoáng' }, lineStyle: { color: '#475569', type: 'dashed' }, data: [{ yAxis: freeFlow }] } : undefined,
      },
      { name: 'band-low', type: 'line', stack: 'band', showSymbol: false, data: fc.map(p => [p.t, p.lower]), lineStyle: { opacity: 0 }, tooltip: { show: false } },
      { name: 'band', type: 'line', stack: 'band', showSymbol: false, data: fc.map(p => [p.t, p.upper != null && p.lower != null ? p.upper - p.lower : null]), lineStyle: { opacity: 0 }, areaStyle: { color: 'rgba(217,89,38,.16)' }, tooltip: { show: false } },
      { name: 'Dự báo', type: 'line', data: fc.map(p => [p.t, p.speed]), lineStyle: { color: FORECAST, width: 2, type: 'dashed' }, itemStyle: { color: FORECAST }, symbolSize: 8 },
    ],
  }
  return <ReactECharts option={option} style={{ height: '100%', width: '100%' }} notMerge />
}

export function Heatmap({ cells, title }: { cells: HeatCell[]; title: string }) {
  const vals = cells.map(c => c.speed)
  const option = {
    title: { text: title, left: 0, top: 0, textStyle: { color: '#94a3b8', fontSize: 11, fontWeight: 500 } },
    grid: { left: 30, right: 8, top: 22, bottom: 38 },
    tooltip: { ...TOOLTIP, formatter: (p: { value: [number, number, number] }) => `${DOW[p.value[1]]} ${p.value[0]}h: <b>${p.value[2]} mph</b>` },
    xAxis: { type: 'category', data: Array.from({ length: 24 }, (_, i) => i), ...AXIS, splitArea: { show: false }, axisLabel: { ...AXIS.axisLabel, interval: 2 } },
    yAxis: { type: 'category', data: DOW, ...AXIS, inverse: true },
    visualMap: {
      // một màu tuần tự: đậm = chậm (tắc), nhạt = nhanh
      min: vals.length ? Math.min(...vals) : 0, max: vals.length ? Math.max(...vals) : 1, calculable: false,
      orient: 'horizontal', left: 'center', bottom: 0, itemHeight: 120, itemWidth: 8,
      text: ['nhanh', 'chậm'], textStyle: { color: MUTED, fontSize: 10 },
      inRange: { color: SEQ_BLUE },
    },
    series: [{ type: 'heatmap', data: cells.map(c => [c.hour, c.dow - 1, c.speed]), itemStyle: { borderColor: '#0e1525', borderWidth: 2, borderRadius: 2 }, emphasis: { itemStyle: { borderColor: '#f0efec' } } }],
  }
  return <ReactECharts option={option} style={{ height: '100%', width: '100%' }} notMerge />
}

export function CityTodayChart({ data }: { data: CityPoint[] }) {
  const option = {
    grid: { left: 32, right: 10, top: 24, bottom: 22 },
    tooltip: { trigger: 'axis', ...TOOLTIP, valueFormatter: (v: number) => (v == null ? '–' : `${v.toFixed(1)} mph`) },
    legend: { top: 0, right: 0, textStyle: { color: '#94a3b8', fontSize: 11 }, itemHeight: 8 },
    xAxis: { type: 'time', ...AXIS, axisLabel: { ...AXIS.axisLabel, formatter: (v: number) => nyTime(new Date(v)) } },
    yAxis: { type: 'value', ...AXIS, scale: true },
    series: [
      { name: '24h qua', type: 'line', showSymbol: false, data: data.map(p => [p.t, p.speed]), lineStyle: { color: ACTUAL, width: 2 }, itemStyle: { color: ACTUAL } },
      { name: 'TB lịch sử 01–09/2025', type: 'line', showSymbol: false, step: 'end', data: data.map(p => [p.t, p.hist]), lineStyle: { color: MUTED, width: 2, type: 'dashed' }, itemStyle: { color: MUTED } },
    ],
  }
  return <ReactECharts option={option} style={{ height: '100%', width: '100%' }} notMerge />
}

export function BoroughBars({ data }: { data: BoroughKpi[] }) {
  const option = {
    grid: { left: 86, right: 34, top: 4, bottom: 4 },
    tooltip: { ...TOOLTIP, trigger: 'axis', axisPointer: { type: 'none' }, valueFormatter: (v: number) => (v == null ? '–' : `${v} mph`) },
    xAxis: { type: 'value', show: false, max: 65 },
    yAxis: { type: 'category', data: data.map(d => d.borough), inverse: true, axisLine: { show: false }, axisTick: { show: false }, axisLabel: { color: '#cbd5e1', fontSize: 11 } },
    series: [{
      type: 'bar', barWidth: 10, data: data.map(d => d.avg_speed),
      itemStyle: { borderRadius: [0, 4, 4, 0], color: ACTUAL },
      label: { show: true, position: 'right', color: '#e2e8f0', fontSize: 11, formatter: '{c}' },
      showBackground: true, backgroundStyle: { color: '#1e293b', borderRadius: [0, 4, 4, 0] },
    }],
  }
  return <ReactECharts option={option} style={{ height: '100%', width: '100%' }} notMerge />
}
