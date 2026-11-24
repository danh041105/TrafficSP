import { useEffect, useMemo, useRef, useState } from "react";
import maplibregl, { type GeoJSONSource, type Map as MLMap } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useApp } from "../App";
import { axisStyle, baseChart, Chart, INK, Loadable, StateLabel } from "../components/ui";
import { BOROUGH_ORDER, duration, nyHour, nyTime, num, STATE_META, stateMeta, useData } from "../lib";

type Props = { link_id: number; link_name: string; borough: string; free_flow_speed_mph: number | null; speed_mph: number | null;
  traffic_state: string; valid_age_min: number | null; last_valid_reading_utc: string | null; last_invalid_reason: string | null };
type Links = { type: "FeatureCollection"; features: { type: "Feature"; id: number; properties: Props; geometry: { type: "LineString"; coordinates: number[][] } }[];
  links_without_geometry: number[] };
type History = { link: Props; history: { t: string; avg_speed_mph: number | null; state: string | null; n_valid: number; layer: string }[]; hours: number };

const COLOR_EXPR: any = ["match", ["get", "traffic_state"],
  ...Object.entries(STATE_META).flatMap(([k, v]) => [k, v.color]), "#9aa0a6"];
const SOLID = ["FREE_FLOW", "SLOW", "CONGESTED", "STALE"];

export default function MapPage() {
  const { lane, params, go } = useApp();
  const { data, error, loading } = useData<Links>(`/api/links?lane=${lane}`, 60000);
  const [borough, setBorough] = useState("all");
  const [hidden, setHidden] = useState<Set<string>>(new Set());
  const selected = params.get("link") ? Number(params.get("link")) : null;
  const mapEl = useRef<HTMLDivElement>(null);
  const map = useRef<MLMap | null>(null);
  const [ready, setReady] = useState(false);

  // khởi tạo bản đồ một lần
  useEffect(() => {
    if (!mapEl.current) return;
    const m = new maplibregl.Map({
      container: mapEl.current,
      // Nền vector OpenFreeMap (miễn phí, không cần key; dữ liệu OpenStreetMap). Đoạn đường vẽ chồng lên bằng geometry thật.
      style: "https://tiles.openfreemap.org/styles/positron",
      center: [-73.93, 40.72], zoom: 10.2, attributionControl: { compact: true },
    });
    m.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    // Thêm lớp đoạn đường ngay khi style sẵn sàng ("style.load"), KHÔNG chờ "load": "load" đợi mọi tile nền tải xong,
    // một tile nền lỗi là các đoạn đường (dữ liệu thật) không bao giờ được vẽ.
    const init = () => {
      if (m.getSource("links")) return;
      m.addSource("links", { type: "geojson", data: { type: "FeatureCollection", features: [] }, promoteId: "link_id" });
      m.addLayer({ id: "links-casing", type: "line", source: "links", layout: { "line-cap": "round", "line-join": "round" },
        paint: { "line-color": "#ffffff", "line-width": ["interpolate", ["linear"], ["zoom"], 9, 4, 14, 10] } });
      m.addLayer({ id: "links-solid", type: "line", source: "links", layout: { "line-cap": "round", "line-join": "round" },
        filter: ["in", ["get", "traffic_state"], ["literal", SOLID]],
        paint: { "line-color": COLOR_EXPR, "line-width": ["interpolate", ["linear"], ["zoom"], 9, 2.5, 14, 7] } });
      m.addLayer({ id: "links-nodata", type: "line", source: "links", layout: { "line-cap": "butt" },
        filter: ["!", ["in", ["get", "traffic_state"], ["literal", SOLID]]],
        paint: { "line-color": "#80868c", "line-width": ["interpolate", ["linear"], ["zoom"], 9, 2, 14, 5], "line-dasharray": [1.5, 1.5] } });
      // Đoạn đang chọn: viền đậm bao quanh nét (line-gap-width = bề rộng nét), vẽ trên cùng để luôn thấy
      m.addLayer({ id: "links-selected", type: "line", source: "links", filter: ["==", ["get", "link_id"], -1],
        layout: { "line-cap": "round", "line-join": "round" },
        paint: { "line-color": INK.ink, "line-width": 2.5, "line-gap-width": ["interpolate", ["linear"], ["zoom"], 9, 3, 14, 8] } });
      for (const id of ["links-solid", "links-nodata"]) {
        m.on("click", id, (e) => { const f = e.features?.[0]; if (f) go("map", { link: String(f.properties?.link_id) }); });
        m.on("mouseenter", id, () => { m.getCanvas().style.cursor = "pointer"; });
        m.on("mouseleave", id, () => { m.getCanvas().style.cursor = ""; });
      }
      setReady(true);
    };
    if (m.isStyleLoaded()) init(); else m.once("style.load", init);
    m.on("error", (e) => console.warn("[map]", e?.error?.message ?? e));
    map.current = m;
    return () => { m.remove(); map.current = null; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const shown = useMemo(() => ({
    type: "FeatureCollection" as const,
    features: (data?.features ?? []).filter((f) => (borough === "all" || f.properties.borough === borough) && !hidden.has(f.properties.traffic_state)),
  }), [data, borough, hidden]);

  useEffect(() => {
    if (!ready || !map.current) return;
    (map.current.getSource("links") as GeoJSONSource).setData(shown as any);
  }, [ready, shown]);
  useEffect(() => {
    if (!ready || !map.current) return;
    map.current.setFilter("links-selected", ["==", ["get", "link_id"], selected ?? -1]);
  }, [ready, selected]);
  // phóng tới quận được chọn / đoạn được chọn
  useEffect(() => {
    if (!ready || !map.current || !shown.features.length) return;
    const target = selected ? shown.features.filter((f) => f.properties.link_id === selected) : borough !== "all" ? shown.features : [];
    if (!target.length) return;
    const b = new maplibregl.LngLatBounds();
    target.forEach((f) => f.geometry.coordinates.forEach((c) => b.extend(c as [number, number])));
    map.current.fitBounds(b, { padding: 80, maxZoom: selected ? 13.5 : 12, duration: 400 });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ready, borough, selected, data]);

  const counts = useMemo(() => {
    const c: Record<string, number> = {};
    (data?.features ?? []).filter((f) => borough === "all" || f.properties.borough === borough)
      .forEach((f) => { c[f.properties.traffic_state] = (c[f.properties.traffic_state] ?? 0) + 1; });
    return Object.entries(c).sort((a, b) => stateMeta(a[0]).order - stateMeta(b[0]).order);
  }, [data, borough]);

  const toggle = (s: string) => setHidden((h) => { const n = new Set(h); n.has(s) ? n.delete(s) : n.add(s); return n; });

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Bản đồ đoạn đường</h1>
        </div>
        <div className="controls">
          <label>Quận{" "}
            <select value={borough} onChange={(e) => setBorough(e.target.value)}>
              <option value="all">Tất cả</option>
              {BOROUGH_ORDER.map((b) => <option key={b} value={b}>{b}</option>)}
            </select>
          </label>
        </div>
      </div>
      <Loadable error={error}>
        <div className="map-wrap">
          <div ref={mapEl} className="map" aria-label="Bản đồ các đoạn đường New York" />
          <div className="map-legend" aria-label="Chú giải">
            {counts.map(([s, n]) => (
              <label key={s} style={{ display: "flex", gap: 6, alignItems: "center", cursor: "pointer" }}>
                <input type="checkbox" checked={!hidden.has(s)} onChange={() => toggle(s)} />
                <StateLabel state={s} /> <span className="muted">{n}</span>
              </label>
            ))}
            {loading && !data && <span className="muted">Đang tải đoạn đường…</span>}
            {data && data.links_without_geometry.length > 0 && <span className="muted">{data.links_without_geometry.length} đoạn thiếu hình học, không vẽ</span>}
          </div>
          <aside className="map-side" aria-label="Chi tiết đoạn đường">
            {selected ? <LinkDetail id={selected} onClose={() => go("map")} /> : (
              <div className="empty">Bấm vào một đoạn trên bản đồ để xem tốc độ, trạng thái và diễn biến 24 giờ.</div>)}
          </aside>
        </div>
      </Loadable>
    </>
  );
}

function LinkDetail({ id, onClose }: { id: number; onClose: () => void }) {
  const { lane } = useApp();
  const { data, error, loading } = useData<History>(`/api/links/${id}/history?lane=${lane}&hours=24`, 60000);
  const option = useMemo(() => {
    if (!data?.history.length) return null;
    const ff = data.link.free_flow_speed_mph;
    const marks = ff ? [
      { yAxis: ff, name: "thông thoáng", lineStyle: { color: INK.ink2, type: "dashed" } },
      { yAxis: ff * 0.8, name: "ngưỡng chậm", lineStyle: { color: "#c98500", type: "dotted" } },
      { yAxis: ff * 0.5, name: "ngưỡng tắc", lineStyle: { color: "#d03b3b", type: "dotted" } },
    ] : [];
    return {
      ...baseChart(),
      grid: { left: 40, right: 70, top: 16, bottom: 28 },
      xAxis: { type: "category", data: data.history.map((r) => nyHour(r.t)), ...axisStyle, axisLabel: { ...axisStyle.axisLabel, interval: 47 } },
      // trục Y luôn chứa mức thông thoáng để 3 đường tham chiếu không bị cắt
      yAxis: { type: "value", name: "mph", ...axisStyle, nameTextStyle: { color: INK.ink3, fontSize: 11 }, min: 0,
               max: (v: { max: number }) => Math.ceil(Math.max(v.max, (ff ?? 0) * 1.08)) },
      series: [{ type: "line", name: "Tốc độ 5 phút", showSymbol: false, connectNulls: false, lineStyle: { width: 2, color: "#2a78d6" },
        itemStyle: { color: "#2a78d6" }, data: data.history.map((r) => (r.n_valid > 0 ? r.avg_speed_mph : null)),
        markLine: { symbol: "none", silent: true, label: { position: "end", formatter: "{b}", color: INK.ink2, fontSize: 11 }, data: marks } }],
    };
  }, [data]);
  const p = data?.link;
  return (
    <>
      <header style={{ display: "flex", justifyContent: "space-between", padding: "12px 16px", borderBottom: "1px solid var(--rule)" }}>
        <strong style={{ fontSize: 16 }}>{p?.link_name ?? `Đoạn ${id}`}</strong>
        <button onClick={onClose} aria-label="Đóng chi tiết" style={{ border: 0, background: "none", cursor: "pointer", fontSize: 18 }}>×</button>
      </header>
      <div className="body" style={{ padding: "14px 16px" }}>
        <Loadable loading={loading && !data} error={error}>
          {p && (
            <>
              <dl className="kv">
                <dt>Trạng thái</dt><dd><StateLabel state={p.traffic_state} /></dd>
                <dt>Tốc độ gần nhất</dt><dd>{num(p.speed_mph)} mph</dd>
                <dt>Tốc độ thông thoáng</dt><dd>{num(p.free_flow_speed_mph)} mph</dd>
                <dt>Tỉ lệ</dt><dd>{p.speed_mph != null && p.free_flow_speed_mph ? `${num((p.speed_mph / p.free_flow_speed_mph) * 100, 0)}%` : "—"}</dd>
                <dt>Bản ghi hợp lệ cuối</dt><dd>{nyTime(p.last_valid_reading_utc)} <span className="muted">(cũ {duration(p.valid_age_min)})</span></dd>
                <dt>Quận</dt><dd>{p.borough}</dd>
                <dt>Mã link</dt><dd className="mono-id">{p.link_id}</dd>
              </dl>
              <h3 style={{ fontSize: 14, margin: "18px 0 4px" }}>Tốc độ 24 giờ trước giờ sự kiện</h3>
              {option ? <Chart option={option} height={230} label={`Tốc độ 5 phút của ${p.link_name} trong 24 giờ`} />
                : <div className="empty">Không có cửa sổ 5 phút nào trong 24 giờ trước giờ sự kiện.</div>}
            </>
          )}
        </Loadable>
      </div>
    </>
  );
}
