import { useMemo, useState } from "react";
import { useApp } from "../App";
import { axisStyle, baseChart, Chart, INK, Loadable, Section, SERIES, StateLabel, StatusText } from "../components/ui";
import { BOROUGH_ORDER, FORECAST_STATUS, int, nyHour, nyTime, num, useData, vnTime } from "../lib";

type Row = { link_id: number; link_name: string; borough: string; origin_utc: string; target_start_utc: string; target_end_utc: string;
  predicted_speed: number | null; predicted_traffic_state: string; free_flow_speed: number | null; speed_last_known_60: number | null;
  actual_speed: number | null; abs_error: number | null; model_version: string; current_speed: number | null; current_state: string;
  status: string; created_at_utc: string };
type Eval = { link_id: number; origin_utc: string; target_start_utc: string; target_end_utc: string; predicted_speed: number;
  actual_speed: number; abs_error: number; persistence_error: number | null };
type FC = { clock: { event_clock_utc: string; actual_available_utc: string }; latest: Row[]; last_evaluated: Eval[]; data_stopped: boolean;
  summary: { status: string; n: number; mae_model: number | null; mae_persistence: number | null }[];
  by_origin: { origin_utc: string; n_forecast: number; n_evaluated: number; mae_model: number | null; mae_persistence: number | null }[];
  models: string[]; hours: number };
type Hist = { predictions: { origin_utc: string; target_start_utc: string; predicted_speed: number | null; actual_speed: number | null;
  speed_last_known_60: number | null; status: string }[]; actual: { t: string; avg_speed_mph: number | null; n_valid: number }[] };

export default function Forecast() {
  const { lane, params, go } = useApp();
  const { data, error, loading } = useData<FC>(`/api/forecast?lane=${lane}&hours=24`, 60000);
  const [borough, setBorough] = useState("all");
  const [sort, setSort] = useState<"drop" | "name">("drop");
  const selected = params.get("link") ? Number(params.get("link")) : null;

  const rows = useMemo(() => {
    const r = (data?.latest ?? []).filter((x) => borough === "all" || x.borough === borough);
    const delta = (x: Row) => (x.predicted_speed != null && x.current_speed != null ? x.predicted_speed - x.current_speed : 999);
    return [...r].sort((a, b) => (sort === "drop" ? delta(a) - delta(b) : a.link_name.localeCompare(b.link_name)));
  }, [data, borough, sort]);

  const sum = useMemo(() => {
    const s = Object.fromEntries((data?.summary ?? []).map((x) => [x.status, x]));
    return s as Record<string, FC["summary"][number]>;
  }, [data]);
  const head = data?.latest.find((r) => r.predicted_speed != null) ?? data?.latest[0];
  const lastEval = useMemo(() => new Map((data?.last_evaluated ?? []).map((e) => [e.link_id, e])), [data]);
  const evaluated = sum.EVALUATED;

  const maeOption = useMemo(() => {
    const pts = (data?.by_origin ?? []).filter((r) => r.n_evaluated > 0);
    if (!pts.length) return null;
    return {
      ...baseChart(), legend: { top: 0, left: 0, icon: "rect", itemWidth: 10, itemHeight: 3 },
      grid: { left: 40, right: 12, top: 44, bottom: 28 },
      xAxis: { type: "category", data: pts.map((r) => nyHour(r.origin_utc)), ...axisStyle },
      yAxis: { type: "value", name: "mph", ...axisStyle },
      series: [
        { name: "LightGBM", type: "line", showSymbol: pts.length < 30, data: pts.map((r) => r.mae_model), lineStyle: { width: 2, color: SERIES[0] }, itemStyle: { color: SERIES[0] } },
        { name: "Giữ nguyên tốc độ cuối (persistence)", type: "line", showSymbol: pts.length < 30, data: pts.map((r) => r.mae_persistence), lineStyle: { width: 2, color: SERIES[1] }, itemStyle: { color: SERIES[1] } },
      ],
    };
  }, [data]);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Dự báo tốc độ sau 15 phút</h1>
        </div>
        <div className="controls">
          <label>Quận{" "}
            <select value={borough} onChange={(e) => setBorough(e.target.value)}>
              <option value="all">Tất cả</option>
              {BOROUGH_ORDER.map((b) => <option key={b} value={b}>{b}</option>)}
            </select>
          </label>
          <button aria-pressed={sort === "drop"} onClick={() => setSort("drop")}>Giảm tốc nhiều nhất</button>
          <button aria-pressed={sort === "name"} onClick={() => setSort("name")}>Theo tên</button>
        </div>
      </div>

      <Loadable loading={loading && !data} error={error}
        empty={data && !data.latest.length ? <><b>Chưa có dự báo nào.</b> Task suy luận trong Airflow chưa chạy hoặc chưa ghi bảng ml.traffic_forecast_15m.</> : false}>
        {data && head && (
          <>
            <div className="stats">
              <div className="stat"><div className="k">Gốc dự báo mới nhất</div><div className="v" style={{ fontSize: 20 }}>{nyTime(head.origin_utc)}</div>
                <div className="d">giờ New York; hệ thống ghi lúc {vnTime(head.created_at_utc)} giờ VN</div></div>
              <div className="stat"><div className="k">Cửa sổ đích</div><div className="v" style={{ fontSize: 20 }}>{nyHour(head.target_start_utc)}–{nyHour(head.target_end_utc)}</div>
                <div className="d">thực tế đã có tới {nyTime(data.clock.actual_available_utc)}</div></div>
              <div className="stat"><div className="k">Dự báo 24 giờ qua</div>
                <div className="v">{int((data.summary ?? []).reduce((s, x) => s + (x.status === "NO_FORECAST" ? 0 : x.n), 0))}</div>
                <div className="d">{Object.keys(FORECAST_STATUS).filter((k) => sum[k]).map((k) => `${FORECAST_STATUS[k].label.toLowerCase()} ${sum[k].n}`).join(", ")}</div></div>
              <div className="stat"><div className="k">MAE trên dự báo đã đối chiếu</div>
                <div className="v">{evaluated ? num(evaluated.mae_model, 2) : "—"}<small>mph</small></div>
                <div className="d">{evaluated ? `persistence ${num(evaluated.mae_persistence, 2)} mph, ${int(evaluated.n)} dự báo` : "chưa có dự báo nào tới giờ đích"}</div></div>
              <div className="stat"><div className="k">Mô hình</div><div className="v" style={{ fontSize: 15, overflowWrap: "anywhere" }}>{data.models.join(", ") || "—"}</div>
                <div className="d">phiên bản ghi trong từng dự báo</div></div>
            </div>

            {!evaluated && (
              <div className="section"><div className="body">
                <b>Chưa đối chiếu được dự báo nào.</b> Cửa sổ đích của gốc mới nhất ({nyHour(head.target_start_utc)}–{nyHour(head.target_end_utc)})
                nằm sau mốc dữ liệu thực tế đã có ({nyTime(data.clock.actual_available_utc)}). Số thực tế chỉ xuất hiện khi Speed Layer phát cửa sổ đó,
                tức sau khi dữ liệu phát lại vượt qua cửa sổ đích cộng watermark 10 phút.
              </div></div>
            )}

            <div className="grid-2">
              <Section title="Dự báo mới nhất theo đoạn" note={`${rows.length} đoạn`} flush>
                {data.data_stopped && <p className="table-note"><b>Nguồn dữ liệu đã ngừng</b>: số thực tế chỉ có tới {nyTime(data.clock.actual_available_utc)} giờ New York.</p>}
                <div className="scroll">
                  <table className="data">
                    <thead><tr><th>Đoạn đường</th><th className="num">Hiện tại</th><th className="num">Sau 15 phút</th><th>Trạng thái dự báo</th>
                      <th>Gốc mới nhất</th><th className="num">Đối chiếu gần nhất</th></tr></thead>
                    <tbody>
                      {rows.map((r) => (
                        <tr key={r.link_id} className={`clickable${selected === r.link_id ? " selected" : ""}`}
                          onClick={() => go("forecast", { link: String(r.link_id) })}>
                          <td>{r.link_name}<div className="muted">{r.borough}</div></td>
                          <td className="num">{num(r.current_speed)}</td>
                          <td className="num">{num(r.predicted_speed)}{r.predicted_speed != null && r.current_speed != null &&
                            <div className="muted">{r.predicted_speed - r.current_speed >= 0 ? "+" : "−"}{num(Math.abs(r.predicted_speed - r.current_speed))}</div>}</td>
                          <td><StateLabel state={r.predicted_traffic_state} /></td>
                          <td><StatusText status={r.status} /></td>
                          <td className="num"><EvalCell e={lastEval.get(r.link_id)} /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Section>
              <div style={{ display: "flex", flexDirection: "column", gap: 22 }}>
                <Section title="Thực tế và dự báo của một đoạn, 6 giờ">
                  {selected ? <LinkForecast id={selected} name={data.latest.find((r) => r.link_id === selected)?.link_name ?? String(selected)} />
                    : <div className="empty">Chọn một đoạn trong bảng để so tốc độ thực tế với các dự báo đã đưa ra cho đoạn đó.</div>}
                </Section>
                <Section title="Sai số theo gốc dự báo, 24 giờ" note="chỉ gồm dự báo đã đối chiếu">
                  {maeOption ? <Chart option={maeOption} height={220} label="MAE theo gốc dự báo, mô hình so với persistence" />
                    : <div className="empty">Chưa có gốc dự báo nào có cửa sổ đích đã có số thực tế.</div>}
                </Section>
              </div>
            </div>
          </>
        )}
      </Loadable>
    </>
  );
}

function EvalCell({ e }: { e?: Eval }) {
  if (!e) return <span className="muted">chưa có</span>;
  return (
    <>
      <span title={`Gốc ${nyTime(e.origin_utc)}, đích ${nyHour(e.target_start_utc)}–${nyHour(e.target_end_utc)} giờ New York`}>
        {num(e.predicted_speed)} → {num(e.actual_speed)}</span>
      <div className="muted">sai {num(e.abs_error)}{e.persistence_error != null ? ` (${num(e.persistence_error)})` : ""}</div>
    </>
  );
}

function LinkForecast({ id, name }: { id: number; name: string }) {
  const { lane } = useApp();
  const { data, error, loading } = useData<Hist>(`/api/forecast/${id}/history?lane=${lane}&hours=6`, 60000);
  const option = useMemo(() => {
    if (!data) return null;
    const pts = data.predictions.filter((p) => p.predicted_speed != null);
    if (!data.actual.length && !pts.length) return null;
    return {
      ...baseChart(), legend: { top: 0, left: 0, icon: "rect", itemWidth: 10, itemHeight: 3 },
      grid: { left: 40, right: 12, top: 44, bottom: 28 },
      tooltip: { ...baseChart().tooltip as object, trigger: "item" },
      xAxis: { type: "time", ...axisStyle, axisLabel: { ...axisStyle.axisLabel, formatter: (v: number) => nyHour(new Date(v).toISOString()) } },
      yAxis: { type: "value", name: "mph", ...axisStyle },
      series: [
        { name: "Thực tế (cửa sổ 5 phút)", type: "line", showSymbol: false, lineStyle: { width: 2, color: INK.ink2 }, itemStyle: { color: INK.ink2 },
          data: data.actual.map((a) => [a.t, a.n_valid > 0 ? a.avg_speed_mph : null]) },
        { name: "Dự báo, đặt tại giờ đích", type: "scatter", symbolSize: 8, itemStyle: { color: SERIES[0], borderColor: "#fff", borderWidth: 1.5 },
          data: pts.map((p) => [p.target_start_utc, p.predicted_speed]) },
      ],
    };
  }, [data]);
  return (
    <Loadable loading={loading && !data} error={error}>
      {option ? <Chart option={option} height={240} label={`Tốc độ thực tế và dự báo của ${name}`} />
        : <div className="empty">Không có dữ liệu thực tế hay dự báo cho {name} trong 24 giờ trước giờ sự kiện.</div>}
      {data && <p className="muted" style={{ fontSize: 12, margin: "6px 0 0" }}>
        {data.predictions.length} dự báo trong 6 giờ trước giờ sự kiện; {data.predictions.filter((p) => p.status === "EVALUATED").length} đã có số thực tế. Giờ theo New York.</p>}
    </Loadable>
  );
}
