import { useMemo } from "react";
import { useApp } from "../App";
import { axisStyle, baseChart, Chart, Loadable, Section, StateLabel } from "../components/ui";
import { BOROUGH_ORDER, duration, int, nyHour, nyTime, num, stateMeta, useData, VALID_STATES } from "../lib";

type OverviewData = {
  clock: { event_clock_utc: string };
  kpi: { links: number; links_valid: number; avg_speed_mph: number | null; median_valid_age_min: number | null; max_valid_age_min: number | null };
  states: { state: string; n: number }[];
  borough: { borough: string; state: string; n: number; avg_speed_mph: number | null }[];
  trend: { t: string; state: string; n: number; avg_speed_mph: number | null }[];
  trend_hours: number;
  stale_after_min: number;
};
type LinksData = { features: { properties: { link_id: number; link_name: string; borough: string; speed_mph: number | null;
  free_flow_speed_mph: number | null; traffic_state: string } }[] };

export default function Overview() {
  const { lane, go } = useApp();
  const { data, error, loading } = useData<OverviewData>(`/api/overview?lane=${lane}&hours=6`, 60000);
  const links = useData<LinksData>(`/api/links?lane=${lane}`, 60000);

  const states = useMemo(() => [...(data?.states ?? [])].sort((a, b) => stateMeta(a.state).order - stateMeta(b.state).order), [data]);
  const total = states.reduce((s, x) => s + x.n, 0);

  const trendOption = useMemo(() => {
    if (!data) return null;
    const times = [...new Set(data.trend.map((r) => r.t))].sort();
    const series = ["CONGESTED", "SLOW", "FREE_FLOW"].map((st) => {
      const m = new Map(data.trend.filter((r) => r.state === st).map((r) => [r.t, r.n]));
      return { name: stateMeta(st).label, type: "bar", stack: "s", barCategoryGap: "8%", data: times.map((t) => m.get(t) ?? 0),
        itemStyle: { color: stateMeta(st).color }, emphasis: { focus: "series" } };
    });
    return {
      ...baseChart(),
      legend: { top: 0, left: 0, itemWidth: 10, itemHeight: 10, icon: "rect", textStyle: { color: "#4b5560" } },
      grid: { left: 40, right: 12, top: 30, bottom: 28 },
      xAxis: { type: "category", data: times.map(nyHour), ...axisStyle, axisLabel: { ...axisStyle.axisLabel, interval: 11 } },
      yAxis: { type: "value", name: "đoạn", nameTextStyle: { color: "#7a838c", fontSize: 11 }, ...axisStyle },
      series,
    };
  }, [data]);

  const boroughRows = useMemo(() => {
    if (!data) return [];
    return BOROUGH_ORDER.map((b) => {
      const rows = data.borough.filter((r) => r.borough === b);
      const n = (s: string) => rows.find((r) => r.state === s)?.n ?? 0;
      const valid = rows.filter((r) => VALID_STATES.has(r.state));
      const nValid = valid.reduce((s, r) => s + r.n, 0);
      const avg = nValid ? valid.reduce((s, r) => s + (r.avg_speed_mph ?? 0) * r.n, 0) / nValid : null;
      return { b, links: rows.reduce((s, r) => s + r.n, 0), valid: nValid, free: n("FREE_FLOW"), slow: n("SLOW"), cong: n("CONGESTED"), avg };
    });
  }, [data]);

  const worst = useMemo(() => (links.data?.features ?? [])
    .map((f) => f.properties)
    .filter((p) => p.traffic_state === "CONGESTED" || p.traffic_state === "SLOW")
    .map((p) => ({ ...p, ratio: p.speed_mph != null && p.free_flow_speed_mph ? p.speed_mph / p.free_flow_speed_mph : 1 }))
    .sort((a, b) => a.ratio - b.ratio).slice(0, 8), [links.data]);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Tổng quan mạng lưới</h1>
        </div>
      </div>

      <Loadable loading={loading && !data} error={error}>
        {data && (
          <>
            <div className="stats" role="list">
              <div className="stat" role="listitem"><div className="k">Đoạn đường theo dõi</div><div className="v">{int(data.kpi.links)}</div>
                <div className="d">bảng v_link_dim</div></div>
              <div className="stat" role="listitem"><div className="k">Có số liệu hợp lệ</div>
                <div className="v">{int(data.kpi.links_valid)}<small>/ {int(data.kpi.links)}</small></div>
                <div className="d">trong {data.stale_after_min} phút trước giờ sự kiện</div></div>
              <div className="stat" role="listitem"><div className="k">Tốc độ trung bình</div>
                <div className="v">{num(data.kpi.avg_speed_mph)}<small>mph</small></div>
                <div className="d">trung bình cộng các đoạn hợp lệ</div></div>
              <div className="stat" role="listitem"><div className="k">Độ trễ bản ghi</div>
                <div className="v">{duration(data.kpi.median_valid_age_min)}</div>
                <div className="d">trung vị; lâu nhất {duration(data.kpi.max_valid_age_min)}</div></div>
            </div>

            <Section title="Phân bố trạng thái" note={`${int(total)} đoạn, tại ${nyTime(data.clock.event_clock_utc)} giờ New York`}>
              <div className="dist" aria-hidden="true">
                {states.map((s) => (
                  <span key={s.state} title={`${stateMeta(s.state).label}: ${s.n}`} style={{
                    flex: s.n, background: stateMeta(s.state).hatch
                      ? `repeating-linear-gradient(135deg, ${stateMeta(s.state).color} 0 3px, #fff 3px 6px)` : stateMeta(s.state).color }} />
                ))}
              </div>
              <div className="dist-legend">
                {states.map((s) => <span key={s.state}><StateLabel state={s.state} /> <b>{s.n}</b></span>)}
              </div>
            </Section>

            <div className="grid-2">
              <Section title={`Số đoạn theo trạng thái, ${data.trend_hours} giờ gần nhất`} note="mỗi cột là một cửa sổ 5 phút">
                {trendOption && data.trend.length
                  ? <Chart option={trendOption} height={280} label="Số đoạn đường theo trạng thái qua các cửa sổ 5 phút" />
                  : <div className="empty">Không có cửa sổ 5 phút hợp lệ trong {data.trend_hours} giờ trước giờ sự kiện.</div>}
              </Section>
              <Section title="Đoạn chậm nhất so với thông thoáng" flush>
                <Loadable loading={links.loading && !links.data} error={links.error}
                  empty={links.data && !worst.length ? "Không có đoạn nào chậm hoặc tắc." : false}>
                  <table className="data">
                    <thead><tr><th>Đoạn đường</th><th className="num">mph</th><th className="num">So với thông thoáng</th></tr></thead>
                    <tbody>
                      {worst.map((w) => (
                        <tr key={w.link_id} className="clickable" onClick={() => go("map", { link: String(w.link_id) })}>
                          <td><StateLabel state={w.traffic_state} text={w.link_name} /><div className="muted">{w.borough}</div></td>
                          <td className="num">{num(w.speed_mph)}</td>
                          <td className="num">{num(w.ratio * 100, 0)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </Loadable>
              </Section>
            </div>

            <Section title="Theo quận" flush>
              <table className="data">
                <thead><tr><th>Quận</th><th className="num">Đoạn</th><th className="num">Hợp lệ</th><th className="num">Thông thoáng</th>
                  <th className="num">Chậm</th><th className="num">Tắc</th><th className="num">Tốc độ TB (mph)</th></tr></thead>
                <tbody>
                  {boroughRows.map((r) => (
                    <tr key={r.b}><td>{r.b}</td><td className="num">{r.links}</td><td className="num">{r.valid}</td>
                      <td className="num">{r.free}</td><td className="num">{r.slow}</td><td className="num">{r.cong}</td>
                      <td className="num">{num(r.avg)}</td></tr>
                  ))}
                </tbody>
              </table>
            </Section>
          </>
        )}
      </Loadable>
    </>
  );
}
