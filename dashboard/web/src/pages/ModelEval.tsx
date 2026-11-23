import { useMemo } from "react";
import { axisStyle, baseChart, Chart, INK, Loadable, Section, SERIES } from "../components/ui";
import { int, num, pct, stateMeta, useData } from "../lib";

type M = { n?: number; mae: number; rmse: number; macro_f1?: number | null };
type ML = {
  version: string;
  model: null | Record<string, any>;
  final_test: Record<string, M & { mae_by_borough?: Record<string, number>; mae_by_actual_state?: Record<string, number>;
    mae_by_peak?: Record<string, number>; mae_by_rain?: Record<string, number> }>;
  classification: { confusion: { labels: string[]; matrix: number[][] }; per_class: Record<string, { precision: number; recall: number; f1: number; support: number }>;
    macro_f1: number; accuracy: number };
  folds: Record<string, Record<string, M>>;
  walk_forward: Record<string, { mae_mean: number; mae_std: number; rmse_mean: number; macro_f1_mean: number; folds: number }>;
  experiments: Record<string, { fold: string; model: string; n: number; mae: number; rmse: number; holdout_links?: number }[]>;
};

const MODEL_LABEL: Record<string, string> = { persistence: "Persistence (giữ tốc độ cuối)", seasonal_naive: "Seasonal naive (cùng giờ tuần trước)",
  lightgbm: "LightGBM", lightgbm_sliding: "LightGBM, cửa sổ trượt" };
const MODEL_ORDER = ["persistence", "seasonal_naive", "lightgbm", "lightgbm_sliding"];

const barChart = (cats: string[], vals: (number | null)[], name: string, color = SERIES[0], height?: number) => ({
  ...baseChart(), tooltip: { ...(baseChart().tooltip as object), trigger: "axis" },
  grid: { left: 150, right: 40, top: 8, bottom: 24, ...(height ? {} : {}) },
  xAxis: { type: "value", name, nameLocation: "end", ...axisStyle },
  yAxis: { type: "category", data: cats, inverse: true, ...axisStyle, axisLabel: { color: INK.ink2, fontSize: 12 } },
  series: [{ type: "bar", data: vals, barMaxWidth: 18, itemStyle: { color, borderRadius: [0, 4, 4, 0] },
    label: { show: true, position: "right", color: INK.ink2, fontSize: 11, formatter: (p: any) => num(p.value, 3) } }],
});

export default function ModelEval() {
  const { data, error, loading } = useData<ML>("/api/ml");

  const foldOption = useMemo(() => {
    if (!data) return null;
    const folds = Object.keys(data.folds).sort();
    const models = MODEL_ORDER.filter((m) => folds.some((f) => data.folds[f][m]));
    return {
      ...baseChart(), legend: { top: 0, left: 0, icon: "rect", itemWidth: 10, itemHeight: 3 },
      grid: { left: 40, right: 16, top: 56, bottom: 28 },
      xAxis: { type: "category", data: folds.map((f) => `tháng ${f.slice(-2)}`), ...axisStyle },
      yAxis: { type: "value", ...axisStyle, min: 0 },
      series: models.map((m, i) => ({ name: MODEL_LABEL[m] ?? m, type: "line", symbolSize: 8, data: folds.map((f) => data.folds[f][m]?.mae ?? null),
        lineStyle: { width: 2, color: SERIES[i] }, itemStyle: { color: SERIES[i], borderColor: "#fff", borderWidth: 1.5 } })),
    };
  }, [data]);

  const confOption = useMemo(() => {
    if (!data?.classification) return null;
    const { labels, matrix } = data.classification.confusion;
    const pts: { value: [number, number, number, number]; label: { color: string } }[] = [];
    matrix.forEach((row, i) => { const s = row.reduce((a, b) => a + b, 0); row.forEach((v, j) => {
      const r = s ? v / s : 0; pts.push({ value: [j, i, r, v], label: { color: r > 0.45 ? "#ffffff" : "#0d1b2e" } }); }); });
    const lab = labels.map((l) => stateMeta(l).label);
    return {
      ...baseChart(), tooltip: { trigger: "item", backgroundColor: "#fff", borderColor: INK.axis, textStyle: { color: INK.ink },
        formatter: (p: any) => `Thực tế ${lab[p.value[1]]}, dự báo ${lab[p.value[0]]}<br/>${pct(p.value[2])} (${int(p.value[3])} mẫu)` },
      grid: { left: 110, right: 16, top: 26, bottom: 10 },
      xAxis: { type: "category", data: lab, ...axisStyle, position: "top", splitLine: { show: false } },
      yAxis: { type: "category", data: lab, inverse: true, ...axisStyle, splitLine: { show: false } },
      visualMap: { show: false, min: 0, max: 1, dimension: 2, inRange: { color: ["#f3f7fd", "#9ec5f4", "#2a78d6", "#104281"] } },
      series: [{ type: "heatmap", data: pts, itemStyle: { borderColor: "#fff", borderWidth: 2 },
        label: { show: true, formatter: (p: any) => pct(p.value[2], 0), fontSize: 12 }, emphasis: { itemStyle: { borderColor: INK.ink, borderWidth: 1 } } }],
    };
  }, [data]);

  const exp = data?.experiments ?? {};
  const final = data?.final_test ?? {};
  const lgb = final.lightgbm;

  const ablationOption = useMemo(() => {
    const r = exp.ablation ?? [];
    const names: Record<string, string> = { traffic: "Chỉ lịch sử tốc độ", traffic_temporal: "+ thời gian", traffic_temporal_spatial: "+ không gian", all: "+ thời tiết (đầy đủ)" };
    return r.length ? barChart(r.map((x) => names[x.model] ?? x.model), r.map((x) => x.mae), "MAE") : null;
  }, [exp.ablation]);
  const stateOption = useMemo(() => {
    const s = lgb?.mae_by_actual_state; if (!s) return null;
    const keys = ["FREE_FLOW", "SLOW", "CONGESTED"].filter((k) => k in s);
    return barChart(keys.map((k) => stateMeta(k).label), keys.map((k) => s[k]), "MAE");
  }, [lgb]);
  const latencyOption = useMemo(() => {
    const r = exp.avail_lag ?? []; if (!r.length) return null;
    const lags = [...new Set(r.map((x) => x.model.split("_A")[1]))].map(Number).sort((a, b) => a - b);
    const models = ["persistence", "seasonal_naive", "lightgbm"];
    return {
      ...baseChart(), legend: { top: 0, left: 0, icon: "rect", itemWidth: 10, itemHeight: 3 },
      grid: { left: 40, right: 16, top: 56, bottom: 34 },
      xAxis: { type: "category", data: lags.map((a) => `${a} phút`), name: "độ trễ dữ liệu", nameLocation: "middle", nameGap: 24, ...axisStyle },
      yAxis: { type: "value", min: 0, ...axisStyle },
      series: models.map((m, i) => ({ name: MODEL_LABEL[m], type: "line", symbolSize: 8,
        data: lags.map((a) => r.find((x) => x.model === `${m}_A${a}`)?.mae ?? null),
        lineStyle: { width: 2, color: SERIES[i] }, itemStyle: { color: SERIES[i], borderColor: "#fff", borderWidth: 1.5 } })),
    };
  }, [exp.avail_lag]);

  const spatial = exp.spatial ?? [];
  const sp = (m: string) => spatial.find((x) => x.model === m);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Đánh giá mô hình dự báo</h1>
          <p>Số liệu của mô hình đang phục vụ, đọc từ artifact của lần huấn luyện (reports/ml/{data?.version ?? "…"}/metrics.json),
            không tính lại trên dashboard. Tập kiểm tra cuối là tháng 10 đến 12 năm 2025; walk-forward gồm các fold mở rộng từ tháng 5 tới tháng 9.</p>
        </div>
      </div>
      <Loadable loading={loading && !data} error={error}>
        {data && (
          <>
            <Section title="Tập kiểm tra cuối" note={lgb ? `${int(lgb.n)} mẫu đánh giá` : ""} flush>
              <table className="data">
                <thead><tr><th>Mô hình</th><th className="num">MAE (mph)</th><th className="num">RMSE (mph)</th><th className="num">Macro-F1 trạng thái</th>
                  <th className="num">MAE khi tắc</th></tr></thead>
                <tbody>
                  {MODEL_ORDER.filter((m) => final[m]).map((m) => (
                    <tr key={m}><td>{m === "lightgbm" ? <b>{MODEL_LABEL[m]}</b> : MODEL_LABEL[m]}</td>
                      <td className="num">{num(final[m].mae, 3)}</td><td className="num">{num(final[m].rmse, 3)}</td>
                      <td className="num">{num(final[m].macro_f1, 3)}</td>
                      <td className="num">{num(final[m].mae_by_actual_state?.CONGESTED, 3)}</td></tr>
                  ))}
                </tbody>
              </table>
            </Section>

            <div className="grid-2e">
              <Section title="Walk-forward theo fold" note="MAE (mph) trên từng tháng kiểm tra">
                {foldOption && <Chart option={foldOption} height={260} label="MAE theo fold walk-forward" />}
                <table className="data" style={{ marginTop: 8 }}>
                  <thead><tr><th>Mô hình</th><th className="num">MAE trung bình</th><th className="num">Độ lệch chuẩn</th><th className="num">Macro-F1</th></tr></thead>
                  <tbody>{MODEL_ORDER.filter((m) => data.walk_forward?.[m]).map((m) => (
                    <tr key={m}><td>{MODEL_LABEL[m]}</td><td className="num">{num(data.walk_forward[m].mae_mean, 3)}</td>
                      <td className="num">{num(data.walk_forward[m].mae_std, 3)}</td><td className="num">{num(data.walk_forward[m].macro_f1_mean, 3)}</td></tr>))}
                  </tbody>
                </table>
              </Section>
              <Section title="Ma trận nhầm lẫn trạng thái, LightGBM" note="hàng: thực tế, cột: dự báo; tỉ lệ theo hàng">
                {confOption && <Chart option={confOption} height={240} label="Ma trận nhầm lẫn ba trạng thái giao thông" />}
                <table className="data" style={{ marginTop: 8 }}>
                  <thead><tr><th>Trạng thái</th><th className="num">Precision</th><th className="num">Recall</th><th className="num">F1</th><th className="num">Số mẫu</th></tr></thead>
                  <tbody>{Object.entries(data.classification?.per_class ?? {}).map(([k, v]) => (
                    <tr key={k}><td>{stateMeta(k).label}</td><td className="num">{num(v.precision, 3)}</td><td className="num">{num(v.recall, 3)}</td>
                      <td className="num">{num(v.f1, 3)}</td><td className="num">{int(v.support)}</td></tr>))}
                  </tbody>
                </table>
                <p className="muted" style={{ fontSize: 12 }}>Accuracy {pct(data.classification?.accuracy)}; Macro-F1 {num(data.classification?.macro_f1, 3)}.</p>
              </Section>
            </div>

            <div className="grid-3">
              <Section title="MAE theo trạng thái thực tế">{stateOption && <Chart option={stateOption} height={170} label="MAE theo trạng thái thực tế" />}</Section>
              <Section title="Bỏ bớt nhóm đặc trưng" note="fold tháng 9">{ablationOption && <Chart option={ablationOption} height={170} label="MAE khi thêm dần các nhóm đặc trưng" />}</Section>
              <Section title="Độ nhạy theo độ trễ dữ liệu" note="MAE (mph), fold tháng 9">{latencyOption && <Chart option={latencyOption} height={200} label="MAE theo độ trễ sẵn có của dữ liệu" />}</Section>
            </div>

            <div className="grid-2e">
              <Section title="Đoạn đường chưa thấy khi huấn luyện (spatial holdout)" note={sp("with_link_id__holdout_links") ? `${sp("with_link_id__holdout_links")!.holdout_links} đoạn giữ lại, fold tháng 9` : ""} flush>
                <table className="data">
                  <thead><tr><th>Biến thể</th><th className="num">Đoạn đã thấy</th><th className="num">Đoạn giữ lại</th></tr></thead>
                  <tbody>
                    <tr><td>Có mã đoạn đường (link_id)</td><td className="num">{num(sp("with_link_id__seen_links")?.mae, 3)}</td><td className="num">{num(sp("with_link_id__holdout_links")?.mae, 3)}</td></tr>
                    <tr><td>Không có mã đoạn đường</td><td className="num">{num(sp("no_link_id__seen_links")?.mae, 3)}</td><td className="num">{num(sp("no_link_id__holdout_links")?.mae, 3)}</td></tr>
                  </tbody>
                </table>
                <p className="muted" style={{ fontSize: 12, padding: "0 16px" }}>Đơn vị: MAE (mph).</p>
              </Section>
              <Section title="Thời tiết và trọng số lớp" note="fold tháng 9, MAE (mph)" flush>
                <table className="data">
                  <thead><tr><th>Biến thể</th><th className="num">MAE</th><th className="num">RMSE</th></tr></thead>
                  <tbody>
                    {[...(exp.weather_lag ?? []), ...(exp.weighting ?? [])].map((x) => (
                      <tr key={x.model}><td>{({ no_weather: "Không dùng thời tiết", weather_lag_0min: "Thời tiết, trễ 0 phút", weather_lag_60min: "Thời tiết, trễ 60 phút",
                        weather_lag_180min: "Thời tiết, trễ 180 phút", unweighted: "Không trọng số lớp", class_weighted: "Trọng số theo lớp" } as Record<string, string>)[x.model] ?? x.model}</td>
                        <td className="num">{num(x.mae, 3)}</td><td className="num">{num(x.rmse, 3)}</td></tr>))}
                  </tbody>
                </table>
              </Section>
            </div>

            {data.model && (
              <Section title="Mô hình đang phục vụ" note={`models/ml/${data.model.model_version}/metadata.json`}>
                <dl className="kv">
                  <dt>Phiên bản</dt><dd className="mono-id">{data.model.model_version}</dd>
                  <dt>Dữ liệu huấn luyện</dt><dd>{String(data.model.train_start).slice(0, 10)} tới {String(data.model.train_boundary).slice(0, 10)}, {int(data.model.dataset_rows)} dòng</dd>
                  <dt>Đặc trưng</dt><dd>{data.model.n_features} đặc trưng, phiên bản {data.model.feature_version}</dd>
                  <dt>Tầm dự báo</dt><dd>{data.model.horizon_minutes} phút; dữ liệu sẵn có sau {data.model.availability_lag_minutes} phút; thời tiết trễ {data.model.weather_lag_minutes} phút</dd>
                  <dt>Tạo lúc</dt><dd>{data.model.created_at}</dd>
                </dl>
              </Section>
            )}
          </>
        )}
      </Loadable>
    </>
  );
}
