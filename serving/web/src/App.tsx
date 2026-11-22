import { useMemo, useState } from 'react'
import {
  useBoroughs, useCityHeatmap, useCityToday, useKpi, useLinkDetail, useLinkForecast,
  useLinkHeatmap, useLinkHistory, useMap, useStates, useTop, useWeather,
} from './api'
import { BoroughBars, CityTodayChart, Heatmap, LinkSpeedChart } from './components/Charts'
import { LinkSearch } from './components/LinkSearch'
import { TrafficMap } from './components/TrafficMap'
import { MEASURED, STATE, mph, nyDate, nyTime, stateOf, wmo } from './format'

const BOROUGHS = ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']

function StateBadge({ state }: { state: string }) {
  const st = stateOf(state)
  return <span className="badge" style={{ background: st.color }}>{st.mark} {st.label}</span>
}

export default function App() {
  const [borough, setBorough] = useState<string>()
  const [selected, setSelected] = useState<string>()
  const [showWeather, setShowWeather] = useState(true)

  const map = useMap()
  const kpi = useKpi(borough)
  const boroughs = useBoroughs()
  const states = useStates(borough)
  const top = useTop(borough)
  const weather = useWeather()
  const cityToday = useCityToday(borough)
  const cityHeat = useCityHeatmap(borough)

  const detail = useLinkDetail(selected)
  const hist = useLinkHistory(selected)
  const fc = useLinkForecast(selected)
  const linkHeat = useLinkHeatmap(selected)

  const mapData = useMemo(() => map.data && borough
    ? { ...map.data, features: map.data.features.filter(f => f.properties.borough === borough) }
    : map.data, [map.data, borough])

  const linkOptions = useMemo(() =>
    (mapData?.features ?? [])
      .map(f => ({ id: f.properties.link_id, label: `${f.properties.link_name} · ${f.properties.borough}` }))
      .sort((a, b) => a.label.localeCompare(b.label)),
  [mapData])

  const w = weather.data
  const avg = (k: 'temperature_c' | 'precipitation_mm' | 'wind_speed_kmh') => {
    const v = (w ?? []).map(c => c[k]).filter((x): x is number => x != null)
    return v.length ? v.reduce((a, b) => a + b, 0) / v.length : undefined
  }
  const domCode = w?.length ? Math.max(...w.map(c => c.weather_code ?? 0)) : undefined
  const clock = kpi.data?.clock
  const source = kpi.data?.source
  const apiDown = kpi.isError

  const stateRows = states.data ?? []
  const totalLinks = stateRows.reduce((s, r) => s + r.links, 0)
  const ordered = [...MEASURED, 'STALE', 'NO_VALID_DATA', 'NO_BASELINE', 'NO_DATA']
    .map(s => ({ s, n: stateRows.find(r => r.traffic_state === s)?.links ?? 0 }))

  return (
    <div className="app">
      <header>
        <div className="brand">
          <span className="logo">◆</span>
          <div>
            <h1>NYC Traffic & Weather</h1>
            <p>TrafficSP · Lambda serving layer</p>
          </div>
        </div>
        <div className="header-right">
          {source && <span className={`pill ${source === 'demo' ? 'demo' : 'live'}`} title={source === 'demo' ? 'ClickHouse + replay data/raw (chế độ demo)' : 'Trino · lakehouse.serving.*'}><i />{source === 'demo' ? 'DEMO' : 'TRINO'}</span>}
          {apiDown && <span className="pill stale"><i />API lỗi</span>}
          <span className="clock" title="Đồng hồ dữ liệu: thời điểm của bản ghi mới nhất (giờ New York)">
            <small>Giờ dữ liệu</small>
            <span className="mono">{nyTime(clock)}</span>
            <small>{nyDate(clock)}</small>
          </span>
          <LinkSearch options={linkOptions} selected={selected} onSelect={id => setSelected(id)} />
          <select value={borough ?? ''} onChange={e => { setBorough(e.target.value || undefined); setSelected(undefined) }}>
            <option value="">Tất cả quận</option>
            {BOROUGHS.map(b => <option key={b}>{b}</option>)}
          </select>
        </div>
      </header>

      <aside className="left">
        <div className="kpis">
          <div className="kpi"><label>Tốc độ TB</label><b>{mph(kpi.data?.city_avg_speed)}</b><small>mph</small></div>
          <div className="kpi"><label>{STATE.CONGESTED.mark} Tắc nghẽn</label><b>{kpi.data?.congested_links ?? '–'}</b><small>đoạn</small></div>
          <div className="kpi"><label>{STATE.SLOW.mark} Chậm</label><b>{kpi.data?.slow_links ?? '–'}</b><small>đoạn</small></div>
          <div className="kpi"><label>Đang báo cáo</label><b>{kpi.data?.links_reporting ?? '–'}</b><small>/ {kpi.data?.links_total ?? '–'}</small></div>
        </div>

        <section className="card">
          <h3>Trạng thái các đoạn</h3>
          <div className="stack">
            {ordered.filter(r => r.n > 0).map(r => (
              <span key={r.s} style={{ flex: r.n, background: stateOf(r.s).color }} title={`${stateOf(r.s).label}: ${r.n}`} />
            ))}
          </div>
          <ul className="legend">
            {ordered.filter(r => r.n > 0 || MEASURED.includes(r.s as never)).map(r => (
              <li key={r.s}><i style={{ background: stateOf(r.s).color }} />{stateOf(r.s).mark} {stateOf(r.s).label}<b>{r.n}</b></li>
            ))}
          </ul>
          <p className="muted tiny">Tổng {totalLinks} đoạn · tắc &lt; 50%, chậm &lt; 80% tốc độ thông thoáng</p>
        </section>

        <section className="card grow">
          <h3>Tốc độ TB theo quận <span className="muted">mph</span></h3>
          <div className="chart-sm">{boroughs.data && <BoroughBars data={boroughs.data} />}</div>
        </section>
      </aside>

      <main className="center">
        <TrafficMap data={mapData} weather={w} showWeather={showWeather} selected={selected} onSelect={setSelected} />
        <div className="map-overlay">
          <label className="toggle"><input type="checkbox" checked={showWeather} onChange={e => setShowWeather(e.target.checked)} /> Lớp thời tiết</label>
        </div>
        <div className="map-legend">
          {MEASURED.map(s => <span key={s}><i style={{ background: STATE[s].color }} />{STATE[s].mark} {STATE[s].label}</span>)}
          <span><i className="dashed" />Không có số đo</span>
        </div>
      </main>

      <aside className="right">
        <section className="card weather">
          <h3>Thời tiết NYC</h3>
          <div className="wx-main">
            <span className="wx-icon">{wmo(domCode).icon}</span>
            <div>
              <b className="wx-temp">{avg('temperature_c')?.toFixed(1) ?? '–'}°C</b>
              <p>{wmo(domCode).label}</p>
            </div>
          </div>
          <div className="wx-grid">
            <div><label>Lượng mưa</label><b>{avg('precipitation_mm')?.toFixed(1) ?? '–'}<small> mm</small></b></div>
            <div><label>Gió</label><b>{avg('wind_speed_kmh')?.toFixed(0) ?? '–'}<small> km/h</small></b></div>
            <div><label>Ô lưới</label><b>{w?.length ?? '–'}</b></div>
          </div>
          <p className="muted tiny">ECMWF IFS · khoảng {nyTime(w?.[0]?.interval_start)} · {w?.[0]?.weather_status === 'FINAL' ? 'đã chốt' : 'tạm tính'}</p>
        </section>

        <section className="card grow">
          <h3>Top đoạn chậm nhất <span className="muted">% tốc độ thông thoáng</span></h3>
          <ol className="top">
            {top.data?.map(t => (
              <li key={t.link_id} className={t.link_id === selected ? 'sel' : ''} onClick={() => setSelected(t.link_id)}>
                <i style={{ background: stateOf(t.traffic_state).color }} />
                <div>
                  <p>{t.link_name}</p>
                  <small>{t.borough} · {stateOf(t.traffic_state).label}</small>
                </div>
                <b className="mono">{t.speed_ratio == null ? '–' : `${Math.round(t.speed_ratio * 100)}%`}</b>
              </li>
            ))}
            {top.data?.length === 0 && <li className="empty">Chưa có đoạn nào có số đo.</li>}
          </ol>
        </section>
      </aside>

      <section className="bottom">
        {selected && detail.data ? (
          <>
            <div className="card link-info">
              <button className="close" onClick={() => setSelected(undefined)} aria-label="Đóng">×</button>
              <StateBadge state={detail.data.traffic_state} />
              <h2>{detail.data.link_name}</h2>
              <p className="muted">{detail.data.borough} · link {detail.data.link_id}{detail.data.length_km ? ` · ${detail.data.length_km.toFixed(2)} km` : ''}</p>
              <div className="stats">
                <div><label>Hiện tại</label><b>{mph(detail.data.speed_mph)}<small> mph</small></b></div>
                <div><label>Thông thoáng (P85)</label><b>{mph(detail.data.free_flow_speed_mph)}<small> mph</small></b></div>
                <div><label>Thời gian đi</label><b>{detail.data.travel_time_s == null ? '–' : Math.round(detail.data.travel_time_s / 60)}<small> phút</small></b></div>
                <div><label>Thời tiết</label><b>{wmo(detail.data.weather?.weather_code).icon} {detail.data.weather?.temperature_c?.toFixed(0) ?? '–'}°</b></div>
              </div>
              <p className="muted tiny">
                {fc.data?.[0]
                  ? <>Dự báo: {fc.data[0].model_name} {fc.data[0].model_version} · phát lúc {nyTime(fc.data[0].issued_at)}</>
                  : 'Chưa có dự báo (chờ view serving.v_traffic_forecast)'}
              </p>
            </div>
            <div className="card chart"><h3>Tốc độ 24h + dự báo <span className="muted">giờ New York</span></h3><div className="chart-body">{hist.data && <LinkSpeedChart history={hist.data} forecast={fc.data ?? []} freeFlow={detail.data.free_flow_speed_mph} />}</div></div>
            <div className="card chart"><div className="chart-body">{linkHeat.data && <Heatmap cells={linkHeat.data} title="Tốc độ TB theo thứ × giờ (đoạn này)" />}</div></div>
          </>
        ) : (
          <>
            <div className="card hint">
              <h2>Chọn một đoạn đường</h2>
              <p className="muted">Bấm vào đường trên bản đồ, gõ tên ở ô tìm kiếm hoặc chọn trong danh sách bên phải để xem tốc độ 24h, dự báo và mẫu tắc đường theo giờ.</p>
            </div>
            <div className="card chart"><h3>Tốc độ {borough ?? 'toàn mạng'} · 24h vs trung bình lịch sử</h3><div className="chart-body">{cityToday.data && <CityTodayChart data={cityToday.data} />}</div></div>
            <div className="card chart"><div className="chart-body">{cityHeat.data && <Heatmap cells={cityHeat.data} title={`Tốc độ TB theo thứ × giờ · ${borough ?? 'toàn mạng'}`} />}</div></div>
          </>
        )}
      </section>
    </div>
  )
}
